use anyhow::{anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ToolRequest {
    #[serde(default)]
    pub call: Option<BlendStep>,
    #[serde(default)]
    pub batch: Option<BlendBatchRequest>,
    #[serde(default)]
    pub script: Option<String>,
    #[serde(default)]
    pub raw: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub close: Option<bool>,
    /// Resume an earlier session of the same seat after its page went cold
    /// (idle eviction or process restart). Playwright MCP persists tabs by
    /// name, so a resident upstream session reconnects to the same page.
    #[serde(default)]
    pub resume: Option<bool>,
    /// Explicitly opt the batch out of the resident session (one-shot fresh
    /// upstream session, nothing survives the call). Default (`None`) is the
    /// resident session; `false` means the same as `None`.
    #[serde(default)]
    pub sessionless: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct BlendBatchRequest {
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub lane: Option<String>,
    #[serde(default)]
    pub step_delay_ms: Option<u64>,
    #[serde(default)]
    pub continue_on_error: Option<bool>,
    #[serde(default)]
    pub max_parallel: Option<usize>,
    #[serde(default)]
    pub capture_after: Option<bool>,
    #[serde(default)]
    pub checkpoint_every: Option<usize>,
    #[serde(default)]
    pub capture_limit_bytes: Option<usize>,
    #[serde(default)]
    pub steps: Vec<BlendStep>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BlendStep {
    #[serde(default)]
    pub id: Option<String>,
    pub tool: String,
    #[serde(default = "empty_object")]
    pub args: Value,
    #[serde(default)]
    pub lane: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub mutates: Option<bool>,
}

/// `#[serde(default)]` on a `Value` field yields `Value::Null` when the field
/// is absent, but Playwright MCP rejects `null` arguments ("expected record,
/// received null"). A step with no `args` must default to an empty object.
fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

#[derive(Debug)]
struct StepBuilder {
    line_no: usize,
    step: BlendStep,
    pending_multiline: Option<PendingMultiline>,
}

#[derive(Debug)]
struct PendingMultiline {
    key: String,
    base_indent: usize,
    lines: Vec<String>,
}

/// Parse line-oriented RAW tool calls into a batch request.
///
/// # Errors
///
/// Returns an error when no tool lines are present or an inline args payload is
/// not a JSON/YAML object.
pub fn parse_raw(input: &str) -> anyhow::Result<BlendBatchRequest> {
    let mut batch = BlendBatchRequest {
        mode: Some("drip".to_string()),
        lane: Some("live_mutate".to_string()),
        ..BlendBatchRequest::default()
    };

    for (index, line) in input.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let (tool, args) = parse_raw_step(trimmed, index + 1)?;
        batch.steps.push(BlendStep {
            id: Some(format!("step-{}", batch.steps.len() + 1)),
            tool,
            args,
            lane: None,
            timeout_ms: None,
            mutates: None,
        });
    }

    if batch.steps.is_empty() {
        bail!("RAW request must contain at least one tool line");
    }
    Ok(batch)
}

fn parse_raw_step(line: &str, line_no: usize) -> anyhow::Result<(String, Value)> {
    let Some((tool, args_raw)) = line.split_once(' ') else {
        return Ok((line.to_string(), Value::Object(Map::new())));
    };
    let args = parse_args_value(args_raw.trim(), line_no)?;
    Ok((tool.to_string(), args))
}

/// Parse bravemag TOON into a batch request.
///
/// # Errors
///
/// Returns an error when the script shape is invalid, references unsupported
/// fields, places envelope fields inside the script, or provides non-object
/// step args.
pub fn parse_script(input: &str) -> anyhow::Result<BlendBatchRequest> {
    let mut batch = BlendBatchRequest::default();
    let mut current: Option<StepBuilder> = None;

    for (zero_index, line) in input.lines().enumerate() {
        let line_no = zero_index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        if let Some(builder) = current.as_mut() {
            if handle_pending_multiline(builder, line)? {
                continue;
            }
        }

        if let Some(rest) = trimmed.strip_prefix("job ") {
            if let Some(builder) = current.take() {
                batch.steps.push(finalize_step(builder)?);
            }
            current = Some(start_step(rest, line_no)?);
            continue;
        }

        if let Some(builder) = current.as_mut() {
            apply_step_field(builder, line, line_no)?;
        } else {
            apply_batch_field(&mut batch, trimmed, line_no)?;
        }
    }

    if let Some(builder) = current.take() {
        batch.steps.push(finalize_step(builder)?);
    }

    if batch.steps.is_empty() {
        bail!("TOON script must define at least one `job <name>:` block");
    }
    Ok(batch)
}

fn start_step(rest: &str, line_no: usize) -> anyhow::Result<StepBuilder> {
    let (name, inline) = rest
        .split_once(':')
        .ok_or_else(|| anyhow!("line {line_no}: job header must contain ':'"))?;
    let id = name.trim();
    if id.is_empty() {
        bail!("line {line_no}: job name must not be empty");
    }

    let mut step = BlendStep {
        id: Some(id.to_string()),
        tool: String::new(),
        args: Value::Object(Map::new()),
        lane: None,
        timeout_ms: None,
        mutates: None,
    };

    let inline = inline.trim();
    if !inline.is_empty() {
        step.tool = inline.to_string();
    }

    Ok(StepBuilder {
        line_no,
        step,
        pending_multiline: None,
    })
}

fn handle_pending_multiline(builder: &mut StepBuilder, line: &str) -> anyhow::Result<bool> {
    let Some(pending) = builder.pending_multiline.as_mut() else {
        return Ok(false);
    };
    let indent = leading_spaces(line);
    if indent >= pending.base_indent {
        pending.lines.push(line[pending.base_indent..].to_string());
        return Ok(true);
    }

    let pending = builder.pending_multiline.take().expect("checked above");
    apply_multiline_value(builder, pending)?;
    Ok(false)
}

fn apply_multiline_value(
    builder: &mut StepBuilder,
    pending: PendingMultiline,
) -> anyhow::Result<()> {
    let PendingMultiline { key, lines, .. } = pending;
    let value = lines.join("\n");
    if key == "args" {
        builder.step.args = parse_args_value(&value, builder.line_no)?;
    } else {
        let line_no = builder.line_no;
        bail!("line {line_no}: unsupported multiline field `{key}`");
    }
    Ok(())
}

fn finalize_step(mut builder: StepBuilder) -> anyhow::Result<BlendStep> {
    if let Some(pending) = builder.pending_multiline.take() {
        apply_multiline_value(&mut builder, pending)?;
    }
    if builder.step.tool.trim().is_empty() {
        let line_no = builder.line_no;
        bail!("line {line_no}: job needs `tool:` or inline tool name");
    }
    Ok(builder.step)
}

fn apply_batch_field(
    batch: &mut BlendBatchRequest,
    trimmed: &str,
    line_no: usize,
) -> anyhow::Result<()> {
    let (key, value) = parse_key_value(trimmed, line_no)?;
    match key {
        "mode" => batch.mode = Some(value.to_string()),
        "lane" => batch.lane = Some(value.to_string()),
        "step_delay_ms" => batch.step_delay_ms = Some(parse_u64(value, key, line_no)?),
        "continue_on_error" => batch.continue_on_error = Some(parse_bool(value, key, line_no)?),
        "max_parallel" => batch.max_parallel = Some(parse_usize(value, key, line_no)?),
        "capture_after" => batch.capture_after = Some(parse_bool(value, key, line_no)?),
        "checkpoint_every" => batch.checkpoint_every = Some(parse_usize(value, key, line_no)?),
        "capture_limit_bytes" => {
            batch.capture_limit_bytes = Some(parse_usize(value, key, line_no)?);
        }
        "session_id" | "close" | "resume" => bail!(
            "line {line_no}: `{key}` is an envelope field; pass it as top-level MCP params, not inside script"
        ),
        other => bail!("line {line_no}: unsupported batch field `{other}`"),
    }
    Ok(())
}

fn apply_step_field(builder: &mut StepBuilder, line: &str, line_no: usize) -> anyhow::Result<()> {
    let trimmed = line.trim();
    let (key, value) = parse_key_value(trimmed, line_no)?;
    if value == "|" {
        builder.pending_multiline = Some(PendingMultiline {
            key: key.to_string(),
            base_indent: leading_spaces(line) + 2,
            lines: Vec::new(),
        });
        return Ok(());
    }

    match key {
        "tool" => builder.step.tool = value.to_string(),
        "args" => builder.step.args = parse_args_value(value, line_no)?,
        "lane" => builder.step.lane = Some(value.to_string()),
        "timeout_ms" => builder.step.timeout_ms = Some(parse_u64(value, key, line_no)?),
        "mutates" => builder.step.mutates = Some(parse_bool(value, key, line_no)?),
        "id" => builder.step.id = Some(value.to_string()),
        "session_id" | "close" | "resume" => bail!(
            "line {line_no}: `{key}` is an envelope field; pass it as top-level MCP params, not inside script"
        ),
        other => bail!("line {line_no}: unsupported job field `{other}`"),
    }
    Ok(())
}

fn parse_key_value(input: &str, line_no: usize) -> anyhow::Result<(&str, &str)> {
    let (key, value) = input
        .split_once(':')
        .ok_or_else(|| anyhow!("line {line_no}: expected `key: value`"))?;
    let key = key.trim();
    if key.is_empty() {
        bail!("line {line_no}: field name must not be empty");
    }
    Ok((key, value.trim()))
}

fn parse_args_value(input: &str, line_no: usize) -> anyhow::Result<Value> {
    if input.is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    serde_json::from_str::<Value>(input)
        .or_else(|_| serde_yaml::from_str::<Value>(input))
        .map_err(|err| anyhow!("line {line_no}: args must parse as JSON/YAML object: {err}"))
        .and_then(|value| {
            if value.is_object() {
                Ok(value)
            } else {
                bail!("line {line_no}: args must be an object")
            }
        })
}

fn parse_bool(input: &str, key: &str, line_no: usize) -> anyhow::Result<bool> {
    input
        .parse::<bool>()
        .map_err(|_| anyhow!("line {line_no}: `{key}` must be true or false"))
}

fn parse_u64(input: &str, key: &str, line_no: usize) -> anyhow::Result<u64> {
    input
        .parse::<u64>()
        .map_err(|_| anyhow!("line {line_no}: `{key}` must be an integer"))
}

fn parse_usize(input: &str, key: &str, line_no: usize) -> anyhow::Result<usize> {
    input
        .parse::<usize>()
        .map_err(|_| anyhow!("line {line_no}: `{key}` must be an integer"))
}

fn leading_spaces(input: &str) -> usize {
    input.chars().take_while(|ch| *ch == ' ').count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_toon_batch_with_multiline_args() {
        let batch = parse_script(
            r#"
mode: drip
lane: live_mutate
step_delay_ms: 150
continue_on_error: false
capture_after: true
capture_limit_bytes: 4096

job inspect:
  tool: get_objects_summary
  lane: live_read
  mutates: false
  args: {}

job count:
  tool: browser_evaluate
  args: |
    {"function":"() => document.title"}
"#,
        )
        .unwrap();

        assert_eq!(batch.mode.as_deref(), Some("drip"));
        assert_eq!(batch.step_delay_ms, Some(150));
        assert_eq!(batch.capture_after, Some(true));
        assert_eq!(batch.capture_limit_bytes, Some(4096));
        assert_eq!(batch.steps.len(), 2);
        assert_eq!(batch.steps[0].id.as_deref(), Some("inspect"));
        assert_eq!(batch.steps[0].tool, "get_objects_summary");
        assert_eq!(batch.steps[0].lane.as_deref(), Some("live_read"));
        assert_eq!(batch.steps[0].mutates, Some(false));
        assert_eq!(batch.steps[1].tool, "browser_evaluate");
        assert!(batch.steps[1].args.get("function").is_some());
    }

    #[test]
    fn rejects_session_id_inside_script() {
        let err = parse_script(
            r"
session_id: work
job inspect:
  tool: get_objects_summary
",
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("`session_id` is an envelope field"));
    }

    #[test]
    fn rejects_resume_inside_script() {
        let err = parse_script(
            r"
resume: true
job inspect:
  tool: get_objects_summary
",
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("`resume` is an envelope field"));
    }

    #[test]
    fn parses_raw_tool_lines() {
        let batch = parse_raw(
            r#"
# comments ignored
get_objects_summary
browser_navigate {"url":"https://example.com"}
"#,
        )
        .unwrap();

        assert_eq!(batch.steps.len(), 2);
        assert_eq!(batch.steps[0].tool, "get_objects_summary");
        assert_eq!(batch.steps[1].tool, "browser_navigate");
    }
}
