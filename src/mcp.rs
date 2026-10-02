use crate::executor::execute_batch;
use crate::toon::{BlendBatchRequest, ToolRequest, parse_raw, parse_script};
use crate::upstream::UpstreamRuntimeConfig;
use crate::{AppResult, BUILD_STAMP, SERVER_NAME, SERVER_VERSION};
use anyhow::{Context, anyhow, bail};
use serde_json::{Map, Value, json};
use std::env;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::fs;
use std::io::{self, BufRead, Write};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;

const PROTOCOL_VERSION: &str = "2025-06-18";
const TOOL_NAME: &str = "bravemag";

/// Serve a minimal MCP stdio endpoint with one tool named `bravemag`.
///
/// # Errors
///
/// Returns an error if stdin/stdout I/O fails while reading or writing JSON-RPC.
pub fn serve_stdio(config: &UpstreamRuntimeConfig) -> AppResult<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    let reload = ReloadGuard::from_env();

    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let request: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(err) => {
                write_message(
                    &mut stdout,
                    &jsonrpc_error(&Value::Null, -32700, &err.to_string()),
                )?;
                continue;
            }
        };

        if request.is_array() {
            write_message(
                &mut stdout,
                &jsonrpc_error(&Value::Null, -32600, "batch JSON-RPC is not supported"),
            )?;
            continue;
        }

        let Some(method) = request.get("method").and_then(Value::as_str) else {
            let id = request.get("id").cloned().unwrap_or(Value::Null);
            write_message(
                &mut stdout,
                &jsonrpc_error(&id, -32600, "request missing method"),
            )?;
            continue;
        };
        let id = request.get("id").cloned();
        let params = request.get("params").cloned().unwrap_or_else(|| json!({}));

        let response = match method {
            "initialize" => id.map(|id| jsonrpc_result(&id, &initialize_result())),
            "tools/list" => id.map(|id| jsonrpc_result(&id, &tools_list_result())),
            "tools/call" => id.map(|id| handle_tool_call(&id, &params, config)),
            "ping" => id.map(|id| jsonrpc_result(&id, &json!({}))),
            "notifications/initialized" | "notifications/cancelled" => None,
            _ => id.map(|id| jsonrpc_error(&id, -32601, &format!("unknown method `{method}`"))),
        };

        if let Some(response) = response {
            write_message(&mut stdout, &response)?;
        }
        if let Err(err) = reload.reexec_if_replaced() {
            eprintln!("bravemag reload check failed: {err}");
        }
    }

    Ok(())
}

fn handle_tool_call(id: &Value, params: &Value, config: &UpstreamRuntimeConfig) -> Value {
    match call_bravemag(params, config) {
        Ok(result) => jsonrpc_result(id, &tool_ok(&result)),
        Err(err) => jsonrpc_result(id, &tool_error(&err.to_string())),
    }
}

fn call_bravemag(params: &Value, config: &UpstreamRuntimeConfig) -> AppResult<Value> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("tools/call params missing `name`"))?;
    if name != TOOL_NAME {
        bail!("unknown tool `{name}`");
    }
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let request: ToolRequest =
        serde_json::from_value(arguments).context("invalid bravemag args")?;
    let batch = request_to_batch(&request)?;
    Ok(serde_json::to_value(execute_batch(config, &batch)?)?)
}

fn request_to_batch(request: &ToolRequest) -> AppResult<BlendBatchRequest> {
    if request.session_id.is_some() {
        bail!(
            "`session_id` is reserved; bravemag routes every seat through one serialized background workspace tab"
        );
    }
    if let Some(batch) = request.batch.clone() {
        ensure_has_steps(&batch)?;
        return Ok(batch);
    }
    if let Some(step) = request.call.clone() {
        return Ok(BlendBatchRequest {
            mode: Some("drip".to_string()),
            lane: step
                .lane
                .clone()
                .or_else(|| Some("live_mutate".to_string())),
            steps: vec![step],
            ..BlendBatchRequest::default()
        });
    }
    if let Some(raw) = request.raw.clone() {
        return parse_raw(&raw);
    }
    if let Some(script) = request.script.clone() {
        return parse_script(&script);
    }
    bail!("provide `script`, `raw`, `batch`, or `call`");
}

fn ensure_has_steps(batch: &BlendBatchRequest) -> AppResult<()> {
    if batch.steps.is_empty() {
        bail!("batch must contain at least one step");
    }
    Ok(())
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": {
            "tools": {}
        },
        "serverInfo": {
            "name": SERVER_NAME,
            "version": SERVER_VERSION,
            "buildStamp": BUILD_STAMP
        }
    })
}

fn tools_list_result() -> Value {
    json!({
        "tools": [{
            "name": TOOL_NAME,
            "description": "Run a paced browser batch through Browser Bridge (bbx). Each batch resolves the human's current active Brave tab, then pins all tab-bound steps to it for the batch. Use `bravemag.status` first when you need its exact title and URL; a matching current page must be reused instead of re-navigated. Method vocabulary is bbx (e.g. page.get_text, navigation.navigate, dom.query, input.click, tabs.list) — not Playwright. Tab activation, creation, and closing remain gateway-owned.",
            "inputSchema": bravemag_input_schema(),
            "annotations": {
                "title": "Bravemag Sequential Executor",
                "destructiveHint": true
            }
        }]
    })
}

fn bravemag_input_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "script": {
                "type": "string",
                "description": "TOON browser batch script."
            },
            "raw": {
                "type": "string",
                "description": "Line-oriented raw upstream tool calls."
            },
            "batch": batch_schema(),
            "call": step_schema(),
            "session_id": {
                "type": "string",
                "description": "Reserved. Bravemag uses one shared serialized workspace tab; `session_id` is refused."
            },
            "close": {
                "type": "boolean",
                "description": "Legacy compatibility flag; ignored by the BBX shared-workspace path."
            },
            "resume": {
                "type": "boolean",
                "description": "Legacy compatibility flag; ignored because the shared BBX tab id is validated automatically."
            },
            "sessionless": {
                "type": "boolean",
                "description": "Legacy compatibility flag; ignored because all calls use the shared background workspace."
            }
        }
    })
}

fn batch_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "mode": { "type": "string", "enum": ["drip", "atomic"] },
            "lane": { "type": "string", "enum": ["live_mutate", "live_read", "background"] },
            "step_delay_ms": { "type": "integer", "minimum": 0 },
            "continue_on_error": { "type": "boolean" },
            "max_parallel": { "type": "integer", "minimum": 1, "default": 1,
                "description": "Maximum concurrent consecutive read steps (BBX only). Mutations run alone." },
            "capture_after": { "type": "boolean" },
            "checkpoint_every": { "type": "integer", "minimum": 1 },
            "capture_limit_bytes": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum bytes returned in each step preview before bravemag truncates locally."
            },
            "steps": {
                "type": "array",
                "minItems": 1,
                "items": step_schema()
            }
        },
        "required": ["steps"]
    })
}

fn step_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "id": { "type": "string" },
            "tool": { "type": "string" },
            "args": {
                "type": "object",
                "additionalProperties": true
            },
            "lane": { "type": "string", "enum": ["live_mutate", "live_read", "background"] },
            "timeout_ms": { "type": "integer", "minimum": 1 },
            "mutates": { "type": "boolean" }
        },
        "required": ["tool"]
    })
}

fn tool_ok(value: &Value) -> Value {
    let mut structured = value.clone();
    if let Some(object) = structured.as_object_mut() {
        object.insert("build_stamp".to_string(), json!(BUILD_STAMP));
    }
    json!({
        "content": [{
            "type": "text",
            "text": render_tool_text(value)
        }],
        "structuredContent": structured,
        "isError": false
    })
}

fn render_tool_text(value: &Value) -> String {
    let step_count = value
        .get("step_count")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let elapsed_ms = value
        .get("elapsed_ms")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let mut out =
        format!("bravemag build_stamp={BUILD_STAMP} ok steps={step_count} elapsed_ms={elapsed_ms}");
    if let Some(results) = value.get("results").and_then(Value::as_array) {
        for result in results {
            let index = result
                .get("index")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let tool = result
                .get("tool")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let status = if result
                .get("success")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                "ok"
            } else {
                "error"
            };
            let _ = write!(out, "\n[{index}] {tool} {status}");
            if let Some(path) = result.get("artifact_path").and_then(Value::as_str) {
                let _ = write!(out, "\nartifact: {path}");
            }
            if let Some(preview) = result.get("preview").and_then(Value::as_str) {
                out.push('\n');
                out.push_str(&truncate_chars(preview, 260));
            }
            if let Some(error) = result.get("error").and_then(Value::as_str) {
                let _ = write!(out, "\nerror: {error}");
            }
        }
    }
    out
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut truncated = text.chars().take(max_chars).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn tool_error(message: &str) -> Value {
    let stamped = format!("bravemag build_stamp={BUILD_STAMP} error: {message}");
    json!({
        "content": [{
            "type": "text",
            "text": stamped
        }],
        "structuredContent": {
            "error": message,
            "build_stamp": BUILD_STAMP
        },
        "isError": true
    })
}

fn jsonrpc_result(id: &Value, result: &Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

fn jsonrpc_error(id: &Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message
        }
    })
}

fn write_message(mut writer: impl Write, value: &Value) -> AppResult<()> {
    serde_json::to_writer(&mut writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

#[derive(Debug, Clone)]
struct ReloadGuard {
    command_path: Option<PathBuf>,
    args: Vec<OsString>,
    launched: Option<FileId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileId {
    dev: u64,
    ino: u64,
}

impl ReloadGuard {
    fn from_env() -> Self {
        let mut args = env::args_os();
        let command_path = args.next().and_then(|arg| resolve_command_path(&arg));
        let launched = command_path.as_deref().and_then(file_id);
        Self {
            command_path,
            args: args.collect(),
            launched,
        }
    }

    fn reexec_if_replaced(&self) -> AppResult<()> {
        let (Some(path), Some(launched)) = (&self.command_path, self.launched) else {
            return Ok(());
        };
        let Some(current) = file_id(path) else {
            return Ok(());
        };
        if current == launched {
            return Ok(());
        }

        eprintln!(
            "bravemag binary changed at {}; re-execing build {}",
            path.display(),
            BUILD_STAMP
        );
        let err = Command::new(path).args(&self.args).exec();
        Err(err).with_context(|| format!("failed to exec {}", path.display()))
    }
}

fn resolve_command_path(command: &OsString) -> Option<PathBuf> {
    let path = PathBuf::from(command);
    if path.components().count() > 1 {
        return Some(path);
    }
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .map(|dir| dir.join(&path))
            .find(|candidate| candidate.is_file())
    })
}

#[cfg(unix)]
fn file_id(path: &Path) -> Option<FileId> {
    fs::metadata(path).ok().map(|meta| FileId {
        dev: meta.dev(),
        ino: meta.ino(),
    })
}

#[cfg(not(unix))]
fn file_id(_path: &Path) -> Option<FileId> {
    None
}

#[allow(dead_code)]
fn object() -> Value {
    Value::Object(Map::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_one_bravemag_tool() {
        let tools = tools_list_result();
        let list = tools
            .get("tools")
            .and_then(Value::as_array)
            .expect("tools array");

        assert_eq!(list.len(), 1);
        assert_eq!(
            list[0].get("name").and_then(Value::as_str),
            Some("bravemag")
        );
    }

    #[test]
    fn converts_single_call_to_batch() {
        let request = ToolRequest {
            call: Some(crate::toon::BlendStep {
                id: Some("inspect".to_string()),
                tool: "get_objects_summary".to_string(),
                args: json!({}),
                lane: Some("live_read".to_string()),
                timeout_ms: None,
                mutates: Some(false),
            }),
            ..ToolRequest::default()
        };

        let batch = request_to_batch(&request).unwrap();

        assert_eq!(batch.steps.len(), 1);
        assert_eq!(batch.lane.as_deref(), Some("live_read"));
    }

    #[test]
    fn rejects_explicit_session_id() {
        let request = ToolRequest {
            session_id: Some("abc".to_string()),
            raw: Some("browser_navigate {\"url\":\"about:blank\"}".to_string()),
            ..ToolRequest::default()
        };

        let err = request_to_batch(&request).unwrap_err().to_string();

        assert!(err.contains("`session_id` is reserved"));
    }

    #[test]
    fn resume_is_an_envelope_flag_not_a_batch_field() {
        // `resume` routes to the resident session in `call_bravemag`; the batch
        // parser itself only ever sees the script.
        let request = ToolRequest {
            resume: Some(true),
            raw: Some("browser_navigate {\"url\":\"about:blank\"}".to_string()),
            ..ToolRequest::default()
        };

        let batch = request_to_batch(&request).unwrap();

        assert_eq!(batch.steps.len(), 1);
        assert_eq!(batch.steps[0].tool, "browser_navigate");
    }
}
