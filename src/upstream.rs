use crate::AppResult;
use anyhow::{Context, anyhow, bail};
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::time::Duration;

const PROTOCOL_VERSION: &str = "2025-06-18";

/// Name of the persistent Playwright MCP tab bravemag drives. Written to
/// `document.title` by [`UpstreamSession::ensure_tab`] (via a tab-marking
/// evaluate) so the tab list — which shows the title — keeps a stable
/// needle across navigations, and `browser_tabs select` can find it by
/// substring after any upstream eviction or process restart.
pub const SESSION_TAB_NAME: &str = "bravemag-session";

/// Page the resident session tab boots on. Must be a real navigation so
/// the tab is durable; `about:blank` is a legitimate real page here.
const SESSION_BOOT_URL: &str = "about:blank";

#[derive(Debug, Clone)]
pub struct UpstreamClientConfig {
    pub endpoint: String,
    pub tool: String,
    pub args_json: String,
}

#[derive(Debug, Clone)]
pub struct UpstreamRuntimeConfig {
    pub endpoint: String,
    pub caller: String,
    pub audit_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct UpstreamProbeResult {
    pub server_name: String,
    pub server_version: String,
    pub tool_count: usize,
    pub sample_tools: Vec<String>,
    pub called_tool: String,
    pub call_status: String,
    pub call_preview: String,
}

impl UpstreamProbeResult {
    #[must_use]
    pub fn render_slim(&self) -> String {
        format!(
            "[bravemag upstream] server={} version={} tools={} sample={}\n[bravemag upstream call] tool={} status={}\n{}",
            self.server_name,
            self.server_version,
            self.tool_count,
            self.sample_tools.join(","),
            self.called_tool,
            self.call_status,
            self.call_preview
        )
    }
}

pub struct UpstreamSession {
    client: Client,
    endpoint: String,
    session_id: String,
    next_id: u64,
    tab_ready: bool,
}

impl Drop for UpstreamSession {
    fn drop(&mut self) {
        // A resident-session tab still hot at drop means the HTTP session —
        // and with it the upstream snapshot refs — is about to close under a
        // live page. Fail loudly: silent ref-death is how the old four-faces
        // bug presented.
        assert!(
            !self.tab_ready,
            "bravemag UpstreamSession dropped with a live resident tab; \
             hold the session for the page's whole lifetime (use execute_session)"
        );
    }
}

impl UpstreamSession {
    /// Initialize an MCP session and boot the resident tab on it.
    ///
    /// # Errors
    ///
    /// Returns an error when the initialize handshake succeeds but the tab
    /// boot (`browser_tabs new` + select) fails — a session without its page
    /// is useless, so fail fast.
    pub fn connect_with_tab(config: &UpstreamRuntimeConfig) -> AppResult<Self> {
        let mut session = Self::connect(config)?;
        session.boot_tab()?;
        Ok(session)
    }

    /// Re-select the resident bravemag tab on this upstream session, so
    /// subsequent calls land on the shared page even after the upstream
    /// session was re-created (idle eviction / process restart).
    ///
    /// The tab is found by scanning the tab list for [`SESSION_TAB_NAME`] in
    /// the title; the marker itself is re-stamped by `mark_tab` after every
    /// navigation (navigations reset `document.title`).
    ///
    /// # Errors
    ///
    /// Returns an error when the tab list call fails or no tab carries the
    /// marker.
    pub fn ensure_tab(&mut self) -> AppResult<()> {
        let tabs = self.call_tool("browser_tabs", &json!({"action": "list"}))?;
        let text = tabs
            .get("content")
            .and_then(Value::as_array)
            .and_then(|c| c.first())
            .and_then(|item| item.get("text"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let index = find_tab_index_by_marker(text, SESSION_TAB_NAME)
            .ok_or_else(|| anyhow!("no Brave tab carries the `{SESSION_TAB_NAME}` marker"))?;
        self.call_tool("browser_tabs", &json!({"action": "select", "index": index}))?;
        self.mark_tab()?;
        self.tab_ready = true;
        Ok(())
    }

    /// Cheap liveness check: does the upstream MCP session still answer?
    ///
    /// Deliberately leaves tab selection alone — the session's current tab
    /// is caller-owned state (a standalone `browser_tabs select` must
    /// survive into the next call), and the title marker is page-owned
    /// (SPAs rewrite `document.title` at will), so a missing marker on a
    /// live session means nothing. Only an unreachable session is stale.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream session no longer answers (HTTP
    /// failure or MCP session eviction).
    pub fn ping(&mut self) -> AppResult<()> {
        self.call_tool("browser_tabs", &json!({"action": "list"}))?;
        Ok(())
    }

    /// Re-stamp [`SESSION_TAB_NAME`] into `document.title` on the current
    /// tab. Called after every upstream navigation so the marker survives
    /// the title reset. Best-effort: a failed stamp leaves the tab findable
    /// only until the next navigation.
    fn mark_tab(&mut self) -> AppResult<()> {
        let script =
            format!("() => {{ document.title = {SESSION_TAB_NAME:?}; return document.title; }}");
        self.call_tool("browser_evaluate", &json!({"function": script}))?;
        Ok(())
    }

    /// Boot the resident tab on this upstream session: open a fresh tab,
    /// select it, and stamp the marker into its title. Exposed for the
    /// resident-session layer; not part of the plain one-shot connect path.
    ///
    /// `ensure_tab` is the wrong tool on a cold boot: it searches the tab
    /// list for the marker, but a freshly created tab carries no marker yet
    /// (the mark is what this routine stamps). So instead we create the tab,
    /// pick the highest index from the list (the new tab is appended), select
    /// it, and mark it.
    ///
    /// # Errors
    ///
    /// Returns an error when any of the upstream calls fail.
    pub fn boot_tab(&mut self) -> AppResult<()> {
        self.call_tool(
            "browser_tabs",
            &json!({"action": "new", "url": SESSION_BOOT_URL}),
        )?;
        let index = self.last_tab_index()?;
        self.call_tool("browser_tabs", &json!({"action": "select", "index": index}))?;
        self.mark_tab()?;
        self.tab_ready = true;
        Ok(())
    }

    /// Highest tab index in the upstream tab list — i.e. the most recently
    /// opened tab. Used by [`boot_tab`] to select the tab it just created.
    ///
    /// # Errors
    ///
    /// Returns an error when the tab list call fails or no index can be
    /// parsed from the list (the upstream tab-list shape changed).
    fn last_tab_index(&mut self) -> AppResult<u64> {
        let tabs = self.call_tool("browser_tabs", &json!({"action": "list"}))?;
        let text = tabs
            .get("content")
            .and_then(Value::as_array)
            .and_then(|c| c.first())
            .and_then(|item| item.get("text"))
            .and_then(Value::as_str)
            .unwrap_or("");
        all_tab_indices(text)
            .max()
            .ok_or_else(|| anyhow!("bravemag: tab list has no parseable indices"))
    }

    /// Give up the resident tab binding without tripping the Drop guard —
    /// used when the upstream session is being deliberately replaced (stale
    /// session eviction) rather than leaking a live page.
    pub fn release_tab(&mut self) {
        self.tab_ready = false;
    }
}

/// Connect to long-lived Playwright MCP HTTP, list tools, and call one tool.
///
/// # Errors
///
/// Returns an error when the HTTP MCP endpoint is unavailable, emits invalid
/// JSON-RPC/SSE, reports an MCP error, or the configured args are invalid JSON.
pub fn run_upstream_probe(config: UpstreamClientConfig) -> AppResult<UpstreamProbeResult> {
    let runtime = UpstreamRuntimeConfig {
        endpoint: config.endpoint,
        caller: "probe".to_string(),
        audit_path: PathBuf::from("/tmp/bravemag/audit.jsonl"),
    };
    let (mut client, init) = UpstreamSession::connect_with_info(&runtime)?;
    let tools = client.list_tools()?;
    let tool_names = extract_tool_names(&tools)?;
    let args = parse_args_object(&config.args_json)?;
    let call = client.call_tool(&config.tool, &args)?;

    Ok(UpstreamProbeResult {
        server_name: json_pointer_string(&init, "/serverInfo/name")
            .unwrap_or_else(|| "unknown".to_string()),
        server_version: json_pointer_string(&init, "/serverInfo/version")
            .unwrap_or_else(|| "unknown".to_string()),
        tool_count: tool_names.len(),
        sample_tools: tool_names.into_iter().take(8).collect(),
        called_tool: config.tool,
        call_status: call_status(&call),
        call_preview: compact_preview(&call, 700),
    })
}

impl UpstreamSession {
    /// Initialize an MCP session against the configured Playwright MCP URL.
    ///
    /// The session is bound to whatever page upstream considers current; it is
    /// NOT pinned to the resident bravemag tab. Use [`Self::connect_with_tab`]
    /// (one-shot-with-page) or the resident-session layer in `executor`
    /// whenever the caller needs a page that survives across calls.
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP endpoint cannot be reached or rejects the
    /// initialize handshake.
    pub fn connect(config: &UpstreamRuntimeConfig) -> AppResult<Self> {
        let (session, _init) = Self::connect_with_info(config)?;
        Ok(session)
    }

    /// Initialize an MCP session and return server info.
    ///
    /// Same page caveat as [`Self::connect`] — the session follows upstream's
    /// current page, not the resident bravemag tab. Kept for the probe path
    /// (which wants server info, not a durable page).
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP endpoint cannot be reached or rejects the
    /// initialize handshake.
    pub fn connect_with_info(config: &UpstreamRuntimeConfig) -> AppResult<(Self, Value)> {
        let client = Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .context("failed to build HTTP client")?;
        let mut session = Self {
            client,
            endpoint: config.endpoint.clone(),
            session_id: String::new(),
            next_id: 1,
            tab_ready: false,
        };
        let init = session.initialize()?;
        Ok((session, init))
    }

    fn initialize(&mut self) -> AppResult<Value> {
        let id = self.next_id;
        self.next_id += 1;
        let message = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "initialize",
            "params": initialize_params(),
        });
        let (result, session_id) = self.post_message(&message, None, id)?;
        self.session_id = session_id.ok_or_else(|| anyhow!("initialize missing mcp-session-id"))?;
        self.notify("notifications/initialized", &json!({}))?;
        Ok(result)
    }

    /// Return upstream MCP `tools/list`.
    ///
    /// # Errors
    ///
    /// Returns an error when upstream rejects the request or returns invalid
    /// JSON-RPC.
    pub fn list_tools(&mut self) -> AppResult<Value> {
        self.request("tools/list", &json!({}))
    }

    /// Call one upstream MCP tool.
    ///
    /// # Errors
    ///
    /// Returns an error when upstream rejects the request or returns invalid
    /// JSON-RPC.
    pub fn call_tool(&mut self, tool: &str, args: &Value) -> AppResult<Value> {
        self.request(
            "tools/call",
            &json!({
                "name": tool,
                "arguments": args,
            }),
        )
    }

    fn request(&mut self, method: &str, params: &Value) -> AppResult<Value> {
        let id = self.next_id;
        self.next_id += 1;
        let message = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let session_id = Some(self.session_id.as_str());
        let (result, _session_id) = self.post_message(&message, session_id, id)?;
        Ok(result)
    }

    fn notify(&mut self, method: &str, params: &Value) -> AppResult<()> {
        let response = self
            .client
            .post(&self.endpoint)
            .headers(Self::headers(Some(&self.session_id))?)
            .json(&json!({
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            }))
            .send()
            .with_context(|| format!("failed to POST notification to {}", self.endpoint))?;
        if !response.status().is_success() {
            bail!(
                "upstream notification `{method}` failed: {}",
                response.status()
            );
        }
        Ok(())
    }

    fn post_message(
        &self,
        message: &Value,
        session_id: Option<&str>,
        id: u64,
    ) -> AppResult<(Value, Option<String>)> {
        let response = self
            .client
            .post(&self.endpoint)
            .headers(Self::headers(session_id)?)
            .json(message)
            .send()
            .with_context(|| format!("failed to POST request to {}", self.endpoint))?;
        let status = response.status();
        let session_id = response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let body = response
            .text()
            .context("failed to read upstream response")?;
        if !status.is_success() {
            bail!("upstream HTTP {status}: {body}");
        }
        let envelope = parse_sse_or_json(&body)?;
        if envelope.get("id").and_then(Value::as_u64) != Some(id) {
            bail!("upstream response id mismatch for id {id}: {envelope}");
        }
        if let Some(error) = envelope.get("error") {
            bail!("upstream request failed: {error}");
        }
        let result = envelope
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow!("upstream response missing result"))?;
        Ok((result, session_id))
    }

    fn headers(session_id: Option<&str>) -> AppResult<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream"),
        );
        if let Some(session_id) = session_id {
            headers.insert(
                "mcp-session-id",
                HeaderValue::from_str(session_id).context("invalid MCP session id")?,
            );
        }
        Ok(headers)
    }
}

fn parse_sse_or_json(body: &str) -> AppResult<Value> {
    let trimmed = body.trim();
    if trimmed.starts_with('{') {
        return serde_json::from_str(trimmed).context("upstream emitted invalid JSON-RPC");
    }
    for line in trimmed.lines() {
        if let Some(data) = line.strip_prefix("data: ") {
            return serde_json::from_str(data).context("upstream emitted invalid SSE JSON-RPC");
        }
    }
    bail!("upstream response did not contain JSON-RPC data");
}

/// Scan a `browser_tabs list` blob for the line whose title carries
/// `marker`, and return that tab's index. The list renders entries as
/// `- N: [title](url)` (current tab annotated `(current)`), so we match the
/// marker against the whole line and lift the leading integer.
fn find_tab_index_by_marker(list_text: &str, marker: &str) -> Option<u64> {
    let needle = marker.to_lowercase();
    list_text
        .lines()
        .filter(|line| line.to_lowercase().contains(&needle))
        .find_map(first_index_on_line)
}

/// Every tab index parseable from a `browser_tabs list` blob. Used by
/// [`UpstreamSession::boot_tab`] to pick the most recently opened tab (the
/// highest index — new tabs are appended).
fn all_tab_indices(list_text: &str) -> impl Iterator<Item = u64> + '_ {
    list_text.lines().filter_map(first_index_on_line)
}

/// Lift the leading integer off a single tab-list line (`- N: ...`).
fn first_index_on_line(line: &str) -> Option<u64> {
    let start = line.find(|ch: char| ch.is_ascii_digit())?;
    let rest = &line[start..];
    let end = rest
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

fn initialize_params() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": {},
        "clientInfo": {
            "name": "bravemag",
            "version": crate::SERVER_VERSION,
        },
    })
}

fn extract_tool_names(tools_result: &Value) -> AppResult<Vec<String>> {
    let tools = tools_result
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("tools/list response missing tools array"))?;
    Ok(tools
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect())
}

fn parse_args_object(input: &str) -> AppResult<Value> {
    let value: Value = serde_json::from_str(input).context("--args-json must be valid JSON")?;
    if value.is_object() {
        Ok(value)
    } else {
        bail!("--args-json must be a JSON object");
    }
}

fn call_status(call_result: &Value) -> String {
    if call_result
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        "error".to_string()
    } else {
        "ok".to_string()
    }
}

#[must_use]
pub fn compact_preview(value: &Value, max_chars: usize) -> String {
    let mut text = serde_json::to_string(value).unwrap_or_else(|_| "<unrenderable>".to_string());
    text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.chars().count() <= max_chars {
        return text;
    }
    let mut truncated = text.chars().take(max_chars).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn json_pointer_string(value: &Value, pointer: &str) -> Option<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sse_jsonrpc_message() {
        let value = parse_sse_or_json(
            r#"event: message
data: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}
"#,
        )
        .unwrap();

        assert_eq!(
            value.pointer("/result/ok").and_then(Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn rejects_non_object_args() {
        let err = parse_args_object("[]").unwrap_err().to_string();
        assert!(err.contains("JSON object"));
    }

    #[test]
    fn truncates_preview() {
        let value = json!({"x": "abcdef"});
        let preview = compact_preview(&value, 6);
        assert_eq!(preview, "{\"x\":\"...");
    }
}
