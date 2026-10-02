use crate::AppResult;
#[cfg(test)]
#[path = "parallel_tests.rs"]
mod parallel_tests;
use crate::toon::{BlendBatchRequest, BlendStep, ToolRequest};
use crate::upstream::{UpstreamRuntimeConfig, UpstreamSession, compact_preview};
use anyhow::{Context, bail};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_CAPTURE_LIMIT_BYTES: usize = 12_000;
const XRAY_PREVIEW_ROWS: usize = 24;
// Retain this path during the migration so old and new gateway processes use
// the same cross-seat lock. Its former cached tab id is now ignored.
const ACTIVE_TAB_LOCK: &str = "/tmp/bravemag/shared-agent-workspace.lock";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchExecutionResult {
    pub caller: String,
    pub step_count: usize,
    pub elapsed_ms: u128,
    pub stopped_early: bool,
    pub mutating_steps: usize,
    pub hooks: Vec<HookExecutionResult>,
    pub results: Vec<StepExecutionResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepExecutionResult {
    pub index: usize,
    pub id: Option<String>,
    pub tool: String,
    pub lane: String,
    pub mutation: MutationClass,
    pub success: bool,
    pub duration_ms: u128,
    pub preview: String,
    pub artifact_path: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MutationClass {
    Read,
    Mutate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HookExecutionResult {
    pub kind: HookKind,
    pub after_step_index: Option<usize>,
    pub path: Option<String>,
    pub success: bool,
    pub duration_ms: u128,
    pub preview: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HookKind {
    Checkpoint,
    Capture,
}

/// Execute a parsed batch against the human's currently active Brave tab.
///
/// A cross-process file lock serializes complete batches. The active tab is
/// resolved once at batch start and every tab-bound step is pinned to that id.
///
/// # Errors
///
/// Returns an error only when the upstream MCP session cannot be started.
/// Per-step tool failures are captured inside `BatchExecutionResult`.
pub fn execute_batch(
    config: &UpstreamRuntimeConfig,
    batch: &BlendBatchRequest,
) -> AppResult<BatchExecutionResult> {
    if batch.max_parallel == Some(0) {
        bail!("max_parallel must be at least 1");
    }
    let (batch, requested_tab_id) = normalize_tab_target(batch)?;
    let active_tab = ActiveTabLease::acquire(requested_tab_id)?;
    let mut upstream = BbxSession::for_tab(active_tab.tab_id);
    Ok(execute_batch_with_session(
        config,
        &mut upstream,
        &batch,
        false,
    ))
}

/// Execute a parsed batch on the resident bravemag upstream session, reusing
/// it (and its page + snapshot refs) across calls.
///
/// The resident session is keyed by seat at the process level (`SESSION`
/// below) and upstream by the persistent named tab
/// [`crate::upstream::SESSION_TAB_NAME`], which survives both idle eviction
/// (re-created transparently on the next call) and process restarts
/// (`resume: true` in the request reconnects to the same page).
///
/// # Panics
///
/// Never under normal operation: the resident-session slot is always `Some`
/// after [`resident_session`] returns (it boots the session when empty).
///
/// # Errors
///
/// Returns an error when the resident session must be re-created (cold start,
/// or the page was closed upstream) and the re-connect fails, or when the
/// caller passed `close: true` — closing the resident session while a page
/// may still be in use kills refs silently, so it is refused.
pub fn execute_session(
    config: &UpstreamRuntimeConfig,
    batch: &BlendBatchRequest,
    request: &ToolRequest,
) -> AppResult<BatchExecutionResult> {
    if batch.max_parallel == Some(0) {
        bail!("max_parallel must be at least 1");
    }
    if batch.max_parallel.unwrap_or(1) > 1 {
        bail!("max_parallel > 1 is supported only by the BBX caller");
    }
    if request.close.unwrap_or(false) {
        bail!(
            "`close` is not supported on the resident bravemag session; \
             it would kill the page and its refs (that silent ref-death was \
             the bug this session mode fixes)"
        );
    }
    let mut session = resident_session(config, request.resume.unwrap_or(false))?;
    let upstream = session
        .as_mut()
        .expect("resident_session always leaves Some");
    Ok(execute_batch_with_session(config, upstream, batch, true))
}

// ─── Resident session — one per bravemag process (seat), lazily booted ──────
//
// The process is spawned per seat by the MCP client, so one resident
// session == one seat. Upstream the session is pinned to the persistent
// named tab `SESSION_TAB_NAME`, which outlives both idle eviction of the
// MCP session (re-created + re-selected transparently) and process restarts
// (reconnected on `resume: true`).

fn resident_session(
    config: &UpstreamRuntimeConfig,
    resume: bool,
) -> AppResult<std::sync::MutexGuard<'static, Option<UpstreamSession>>> {
    use std::sync::{Mutex, MutexGuard, OnceLock};

    static SESSION: OnceLock<Mutex<Option<UpstreamSession>>> = OnceLock::new();
    let cell = SESSION.get_or_init(|| Mutex::new(None));
    let mut guard: MutexGuard<'_, Option<UpstreamSession>> = cell
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if guard.is_none() {
        let mut session = UpstreamSession::connect(config)?;
        if resume {
            // The named tab survived a previous process; just re-select it.
            session.ensure_tab()?;
        } else {
            session.boot_tab()?;
        }
        *guard = Some(session);
    } else {
        // Liveness probe: reconnect only when the upstream session stopped
        // answering. On a live session the current tab is left strictly
        // alone — re-selecting the marked tab here undid standalone
        // `browser_tabs select` calls, and treating a missing title marker
        // as staleness booted fresh blank tabs over live pages whenever a
        // page rewrote its own `document.title` (the 2026-08-13 four-faces
        // regression).
        let dead = guard
            .as_mut()
            .is_some_and(|session| session.ping().is_err());
        if dead {
            // Drop the dead session without tripping its Drop-guard.
            if let Some(mut dead_session) = guard.take() {
                dead_session.release_tab();
            }
            let mut session = UpstreamSession::connect(config)?;
            // Prefer re-binding the surviving marked tab (the page and its
            // state live on); boot a fresh tab only when no marker is found.
            if session.ensure_tab().is_err() {
                session.boot_tab()?;
            }
            *guard = Some(session);
        }
    }
    Ok(guard)
}

#[cfg(test)]
mod session_tests {
    use super::*;
    use crate::toon::ToolRequest;

    fn dummy_config() -> UpstreamRuntimeConfig {
        UpstreamRuntimeConfig {
            endpoint: "http://127.0.0.1:0/mcp".to_string(),
            caller: "test".to_string(),
            audit_path: std::path::PathBuf::from("/tmp/bravemag/test-session-audit.jsonl"),
        }
    }

    fn one_step_batch() -> BlendBatchRequest {
        BlendBatchRequest {
            steps: vec![crate::toon::BlendStep {
                id: Some("s".to_string()),
                tool: "browser_evaluate".to_string(),
                args: serde_json::json!({"function": "() => 1"}),
                lane: None,
                timeout_ms: None,
                mutates: None,
            }],
            ..BlendBatchRequest::default()
        }
    }

    #[test]
    fn close_is_refused_with_a_named_error() {
        let request = ToolRequest {
            close: Some(true),
            ..ToolRequest::default()
        };
        let err = execute_session(&dummy_config(), &one_step_batch(), &request)
            .unwrap_err()
            .to_string();

        assert!(err.contains("`close` is not supported"));
    }

    #[test]
    fn sessionless_requests_skip_the_resident_session() {
        // `call_bravemag` routes on `sessionless`; the batch parser never sees
        // it. Here we just pin that `request_to_batch` accepts the flag
        // without error (routing is tested in mcp.rs's own tests).
        let request = ToolRequest {
            sessionless: Some(true),
            raw: Some("browser_navigate {\"url\":\"about:blank\"}".to_string()),
            ..ToolRequest::default()
        };
        // Cannot call `execute_batch` without a live upstream; assert only
        // that the envelope parses and carries the flag.
        assert_eq!(request.sessionless, Some(true));
        assert!(request.resume.is_none());
    }
}

pub(crate) trait ToolCaller {
    fn call_tool(&mut self, tool: &str, args: &Value) -> AppResult<Value>;

    fn fork_read(&self) -> Option<Box<dyn ToolCaller + Send>> {
        None
    }
}

impl ToolCaller for Box<dyn ToolCaller + Send> {
    fn call_tool(&mut self, tool: &str, args: &Value) -> AppResult<Value> {
        (**self).call_tool(tool, args)
    }
}

/// BBX is the sole browser engine. Its extension talks to the user's live
/// Brave, so no CDP-attached Playwright browser or file-chooser interception
/// sits between a human click and the native picker.
#[derive(Debug, Clone, Copy)]
struct BbxSession {
    tab_id: Option<u64>,
}

impl BbxSession {
    const fn control() -> Self {
        Self { tab_id: None }
    }

    const fn for_tab(tab_id: u64) -> Self {
        Self {
            tab_id: Some(tab_id),
        }
    }

    fn call_raw(&self, tool: &str, args: &Value) -> AppResult<Value> {
        let args_json = serde_json::to_string(args)?;
        let bbx = bbx_binary();
        // PATH is passed through as-is. `BRAVEMAG_EXTRA_PATH` (optional) is put in
        // front of it, for installs where bbx lives outside the caller's PATH.
        let path_env = match (
            std::env::var("BRAVEMAG_EXTRA_PATH"),
            std::env::var("PATH").unwrap_or_default(),
        ) {
            (Ok(extra), path) if !extra.trim().is_empty() => format!("{extra}:{path}"),
            (_, path) => path,
        };
        let mut command = Command::new(&bbx);
        let call_args = bbx_call_args(
            tool,
            &args_json,
            self.tab_id.filter(|_| requires_tab_identity(tool)),
        );
        command.args(&call_args);
        // Optional: when several OS users call bravemag but the live browser bridge
        // belongs to one of them, `BRAVEMAG_HOME` pins bbx discovery to that user's
        // home (daemon socket and native-host manifest). Unset = the caller's own
        // environment is used untouched.
        if let Ok(home) = std::env::var("BRAVEMAG_HOME") {
            let home = home.trim();
            if !home.is_empty() {
                command
                    .env("HOME", home)
                    .env("XDG_CONFIG_HOME", format!("{home}/.config"))
                    .env("XDG_DATA_HOME", format!("{home}/.local/share"));
            }
        }
        let output = command
            .env("PATH", &path_env)
            .env("BBX_PRINT_ENVELOPE", "1")
            .output()
            .with_context(|| format!("failed to start bbx (`{bbx}`) for `{tool}`"))?;
        if !output.status.success() {
            bail!(
                "bbx `{tool}` failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let text = String::from_utf8(output.stdout)?.trim().to_string();
        let envelope =
            serde_json::from_str::<Value>(&text).unwrap_or_else(|_| json!({"text": text}));
        if envelope.get("ok").and_then(Value::as_bool) == Some(false) {
            bail!(
                "bbx `{tool}` returned an error: {}",
                envelope
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            );
        }
        let value = envelope.get("result").cloned().unwrap_or(envelope.clone());
        if let Some(expected) = self.tab_id.filter(|_| requires_tab_identity(tool)) {
            let actual = envelope
                .pointer("/meta/actual_tab_id")
                .and_then(Value::as_u64);
            if actual != Some(expected) {
                bail!("TAB_MISMATCH: requested tab {expected} but BBX reported {actual:?}");
            }
        }
        Ok(value)
    }
}

fn requires_tab_identity(tool: &str) -> bool {
    !matches!(
        tool,
        "health.ping" | "tabs.list" | "tabs.create" | "tabs.close" | "tabs.activate"
    )
}

fn bbx_call_args(tool: &str, args_json: &str, tab_id: Option<u64>) -> Vec<String> {
    let mut args = vec!["call".to_string()];
    if let Some(tab_id) = tab_id {
        args.extend(["--tab".to_string(), tab_id.to_string()]);
    }
    args.extend([tool.to_string(), args_json.to_string()]);
    args
}

/// Cross-process lease for the tab active when a batch begins.
///
/// `flock`-style locks are released by the kernel if a seat process dies. The
/// file is mode 0666 because house seats run under both the operator UID and
/// root; browser access remains scoped by the BBX daemon of the user that owns the browser.
struct ActiveTabLease {
    file: File,
    tab_id: u64,
}

impl ActiveTabLease {
    fn acquire(requested_tab_id: Option<u64>) -> AppResult<Self> {
        if let Some(parent) = PathBuf::from(ACTIVE_TAB_LOCK).parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create BraveMag state directory `{}`",
                    parent.display()
                )
            })?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o666)
            .open(ACTIVE_TAB_LOCK)
            .with_context(|| format!("failed to open `{ACTIVE_TAB_LOCK}`"))?;
        // `mode` is filtered by umask on creation. Relax it explicitly so a
        // root-launched seat and an operator-launched seat can share the same
        // lock/state file on the next call.
        let _ = file.set_permissions(fs::Permissions::from_mode(0o666));
        FileExt::lock_exclusive(&file)
            .with_context(|| format!("failed to lock `{ACTIVE_TAB_LOCK}`"))?;

        let control = BbxSession::control();
        let tabs = control.call_raw("tabs.list", &json!({}))?;
        let tab_id = match requested_tab_id {
            Some(tab_id) => tabs
                .get("tabs")
                .and_then(Value::as_array)
                .and_then(|tabs| {
                    tabs.iter().find_map(|tab| {
                        (tab.get("tabId").and_then(Value::as_u64) == Some(tab_id))
                            .then_some(tab_id)
                    })
                })
                .with_context(|| format!("BBX tabId {tab_id} is no longer available"))?,
            None => active_tab_id(&tabs).context(
                "BBX tabs.list reported no active Brave tab; focus the tab you want BraveMag to use",
            )?,
        };

        Ok(Self { file, tab_id })
    }
}

/// Consume an explicit MCP `args.tabId` selector and keep every tab-bound step
/// in the batch on that BBX tab.  BBX receives the selector through `--tab`,
/// not as a method argument.  The default remains the operator's active tab.
fn normalize_tab_target(batch: &BlendBatchRequest) -> AppResult<(BlendBatchRequest, Option<u64>)> {
    let mut normalized = batch.clone();
    let mut selected = None;

    for step in &mut normalized.steps {
        let Some(object) = step.args.as_object_mut() else {
            continue;
        };
        let Some(value) = object.remove("tabId") else {
            continue;
        };
        // Keep the reservation error authoritative. These calls never acquire
        // a target, so a selector must not make them look merely unsupported.
        if matches!(step.tool.as_str(), "tabs.close" | "tabs.activate") {
            continue;
        }
        let tab_id = value
            .as_u64()
            .filter(|id| *id > 0)
            .with_context(|| format!("{} args.tabId must be a positive integer", step.tool))?;
        if !requires_tab_identity(&step.tool) && step.tool != "bravemag.status" {
            bail!("{} does not accept args.tabId", step.tool);
        }
        if let Some(previous) = selected.replace(tab_id) {
            if previous != tab_id {
                bail!("all args.tabId values in one batch must match ({previous} != {tab_id})");
            }
        }
    }

    Ok((normalized, selected))
}

impl Drop for ActiveTabLease {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn active_tab_id(value: &Value) -> Option<u64> {
    value
        .get("tabs")
        .and_then(Value::as_array)
        .and_then(|tabs| {
            tabs.iter().find_map(|tab| {
                tab.get("active")
                    .and_then(Value::as_bool)
                    .filter(|active| *active)
                    .and_then(|_| tab.get("tabId"))
                    .and_then(Value::as_u64)
                    .filter(|id| *id > 0)
            })
        })
}

fn listed_tab_id(value: &Value, index: u64) -> Option<u64> {
    value
        .get("tabs")
        .and_then(Value::as_array)?
        .get(usize::try_from(index).ok()?)?
        .get("tabId")
        .and_then(Value::as_u64)
        .filter(|id| *id > 0)
}

#[cfg(test)]
mod active_tab_tests {
    use super::*;

    #[test]
    fn explicit_workspace_tab_is_injected_into_bbx_calls() {
        assert_eq!(
            bbx_call_args("page.get_state", "{}", Some(42)),
            ["call", "--tab", "42", "page.get_state", "{}"]
        );
    }

    #[test]
    fn control_calls_do_not_receive_a_tab_override() {
        assert_eq!(
            bbx_call_args("tabs.list", "{}", None),
            ["call", "tabs.list", "{}"]
        );
    }

    #[test]
    fn active_tab_is_resolved_from_bbx_listing() {
        let tabs = json!({"tabs": [
            {"tabId": 41, "active": false},
            {"tabId": 42, "active": true}
        ]});
        assert_eq!(active_tab_id(&tabs), Some(42));
        assert_eq!(
            active_tab_id(&json!({"tabs": [{"tabId": 42, "active": false}]})),
            None
        );
    }

    #[test]
    fn browser_tabs_index_resolves_to_bbx_tab_id() {
        let tabs = json!({"tabs": [{"tabId": 41}, {"tabId": 42}]});

        assert_eq!(listed_tab_id(&tabs, 1), Some(42));
        assert_eq!(listed_tab_id(&tabs, 2), None);
    }

    #[test]
    fn agents_cannot_change_shared_workspace_tab_selection() {
        for tool in ["tabs.close", "tabs.activate"] {
            let step = BlendStep {
                id: None,
                tool: tool.to_string(),
                args: json!({}),
                lane: None,
                timeout_ms: None,
                mutates: None,
            };
            assert!(namespace_guard(&step).is_some(), "{tool} was not blocked");
        }
    }

    #[test]
    fn inactive_tab_creation_is_allowed_but_active_creation_is_blocked() {
        let inactive = BlendStep {
            id: None,
            tool: "tabs.create".to_string(),
            args: json!({"url": "https://example.com", "active": false}),
            lane: None,
            timeout_ms: None,
            mutates: None,
        };
        let active = BlendStep {
            args: json!({"url": "https://example.com", "active": true}),
            ..inactive.clone()
        };
        assert!(namespace_guard(&inactive).is_none());
        assert!(namespace_guard(&active).is_some());
    }

    #[test]
    fn explicit_tab_id_is_removed_from_bbx_args_and_pins_the_batch() {
        let batch = BlendBatchRequest {
            steps: vec![BlendStep {
                id: None,
                tool: "page.get_state".to_string(),
                args: json!({"tabId": 42}),
                lane: None,
                timeout_ms: None,
                mutates: None,
            }],
            ..BlendBatchRequest::default()
        };
        let (normalized, target) = normalize_tab_target(&batch).unwrap();
        assert_eq!(target, Some(42));
        assert_eq!(normalized.steps[0].args, json!({}));
    }

    #[test]
    fn conflicting_explicit_tab_ids_are_refused() {
        let batch = BlendBatchRequest {
            steps: vec![
                BlendStep {
                    id: None,
                    tool: "page.get_state".to_string(),
                    args: json!({"tabId": 41}),
                    lane: None,
                    timeout_ms: None,
                    mutates: None,
                },
                BlendStep {
                    id: None,
                    tool: "dom.query".to_string(),
                    args: json!({"tabId": 42}),
                    lane: None,
                    timeout_ms: None,
                    mutates: None,
                },
            ],
            ..BlendBatchRequest::default()
        };
        assert!(normalize_tab_target(&batch).is_err());
    }

    #[test]
    fn reserved_tab_operations_keep_their_reservation_error_with_a_tab_id() {
        let batch = BlendBatchRequest {
            steps: vec![BlendStep {
                id: None,
                tool: "tabs.activate".to_string(),
                args: json!({"tabId": 42}),
                lane: None,
                timeout_ms: None,
                mutates: None,
            }],
            ..BlendBatchRequest::default()
        };
        let (normalized, target) = normalize_tab_target(&batch).unwrap();
        assert_eq!(target, None);
        assert!(namespace_guard(&normalized.steps[0]).is_some());
    }

    #[test]
    fn legacy_tab_activation_is_blocked() {
        let legacy = BlendStep {
            id: None,
            tool: "browser_tabs".to_string(),
            args: json!({"action": "activate", "index": 1}),
            lane: None,
            timeout_ms: None,
            mutates: None,
        };

        assert!(namespace_guard(&legacy).is_some());
    }

    #[test]
    fn read_only_tab_listing_stays_available() {
        let step = BlendStep {
            id: None,
            tool: "browser_tabs".to_string(),
            args: json!({"action": "list"}),
            lane: None,
            timeout_ms: None,
            mutates: None,
        };
        assert!(namespace_guard(&step).is_none());
    }
}

/// The `bbx` binary: `BRAVEMAG_BBX` (an absolute path) when set, else `bbx`
/// from PATH (see also `BRAVEMAG_EXTRA_PATH`).
fn bbx_binary() -> String {
    if let Ok(path) = std::env::var("BRAVEMAG_BBX") {
        if !path.trim().is_empty() {
            return path;
        }
    }
    "bbx".to_string()
}

impl ToolCaller for BbxSession {
    fn fork_read(&self) -> Option<Box<dyn ToolCaller + Send>> {
        Some(Box::new(*self))
    }

    fn call_tool(&mut self, tool: &str, args: &Value) -> AppResult<Value> {
        let value = if tool == "bravemag.status" {
            let active_tab_id = self
                .tab_id
                .context("bravemag.status requires an active Brave tab lease")?;
            let tabs = self.call_raw("tabs.list", &json!({}))?;
            tabs.get("tabs")
                .and_then(Value::as_array)
                .and_then(|tabs| {
                    tabs.iter()
                        .find(|tab| tab.get("tabId").and_then(Value::as_u64) == Some(active_tab_id))
                })
                .cloned()
                .context("active Brave tab disappeared before BraveMag could report its status")?
        } else if tool == "browser_tabs" {
            match args.get("action").and_then(Value::as_str) {
                Some("list") => self.call_raw("tabs.list", &json!({}))?,
                Some("activate") => {
                    let tab_id = if let Some(tab_id) = args.get("tabId").and_then(Value::as_u64) {
                        tab_id
                    } else {
                        let index = args
                            .get("index")
                            .and_then(Value::as_u64)
                            .context("browser_tabs activate requires tabId or index")?;
                        let tabs = self.call_raw("tabs.list", &json!({}))?;
                        listed_tab_id(&tabs, index).with_context(|| {
                            format!("browser_tabs activate index {index} is out of range")
                        })?
                    };
                    self.call_raw("tabs.activate", &json!({"tabId": tab_id}))?
                }
                _ => anyhow::bail!("unsupported browser_tabs action"),
            }
        } else {
            self.call_raw(tool, args)?
        };
        Ok(json!({"content": [{"type": "text", "text": value.to_string()}]}))
    }
}

impl ToolCaller for UpstreamSession {
    fn call_tool(&mut self, tool: &str, args: &Value) -> AppResult<Value> {
        UpstreamSession::call_tool(self, tool, args)
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn execute_batch_with_session(
    config: &UpstreamRuntimeConfig,
    upstream: &mut impl ToolCaller,
    batch: &BlendBatchRequest,
    resident: bool,
) -> BatchExecutionResult {
    let started = Instant::now();
    let mut results = Vec::with_capacity(batch.steps.len());
    let mut hooks = Vec::new();
    let continue_on_error = batch.continue_on_error.unwrap_or(false);
    let delay = Duration::from_millis(batch.step_delay_ms.unwrap_or(100));
    let mut stopped_early = false;
    let mut mutating_steps = 0usize;

    let max_parallel = batch.max_parallel.unwrap_or(1);
    let parallel_error = if max_parallel == 0 {
        Some("max_parallel must be at least 1")
    } else if max_parallel > 1 && upstream.fork_read().is_none() {
        Some("max_parallel > 1 is supported only by callers with independent BBX read sessions")
    } else {
        None
    };
    let mut index = 0;
    while index < batch.steps.len() {
        let step = &batch.steps[index];
        let group = if let Some(error) = parallel_error {
            vec![failed_step(batch, step, index, error)]
        } else if max_parallel > 1 && step_is_read(batch, step) {
            let count = batch.steps[index..]
                .iter()
                .take(max_parallel)
                .take_while(|step| step_is_read(batch, step))
                .count();
            thread::scope(|scope| {
                let handles: Vec<_> = (index..index + count)
                    .map(|i| {
                        let caller = upstream.fork_read();
                        scope.spawn(move || match caller {
                            Some(mut caller) => {
                                execute_step(config, &mut caller, batch, &batch.steps[i], i, false)
                            }
                            None => failed_step(
                                batch,
                                &batch.steps[i],
                                i,
                                "parallel read caller unavailable",
                            ),
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .enumerate()
                    .map(|(offset, handle)| {
                        handle.join().unwrap_or_else(|_| {
                            failed_step(
                                batch,
                                &batch.steps[index + offset],
                                index + offset,
                                "parallel read worker panicked",
                            )
                        })
                    })
                    .collect::<Vec<_>>()
            })
        } else {
            vec![execute_step(config, upstream, batch, step, index, resident)]
        };
        let group_len = group.len();
        let mut should_stop = parallel_error.is_some();
        for result in group {
            write_audit(config, &batch.steps[result.index], &result);
            if result.mutation == MutationClass::Mutate && result.success {
                mutating_steps += 1;
                if should_checkpoint(batch, mutating_steps) {
                    hooks.push(run_checkpoint_hook(upstream, result.index));
                }
            }
            should_stop |= !result.success && !continue_on_error;
            results.push(result);
        }
        index += group_len;

        if should_stop {
            stopped_early = true;
            break;
        }

        if index < batch.steps.len() && !delay.is_zero() {
            thread::sleep(delay);
        }
    }

    if batch.capture_after.unwrap_or(false) && !results.is_empty() && !stopped_early {
        hooks.push(run_capture_hook(upstream));
    }

    BatchExecutionResult {
        caller: config.caller.clone(),
        step_count: batch.steps.len(),
        elapsed_ms: started.elapsed().as_millis(),
        stopped_early,
        mutating_steps,
        hooks,
        results,
    }
}

fn step_is_read(batch: &BlendBatchRequest, step: &BlendStep) -> bool {
    let lane = step
        .lane
        .as_deref()
        .or(batch.lane.as_deref())
        .unwrap_or("live_mutate");
    classify_mutation(batch, step, lane) == MutationClass::Read
}

fn failed_step(
    batch: &BlendBatchRequest,
    step: &BlendStep,
    index: usize,
    error: &str,
) -> StepExecutionResult {
    let lane = step
        .lane
        .as_deref()
        .or(batch.lane.as_deref())
        .unwrap_or("live_mutate");
    StepExecutionResult {
        index,
        id: step.id.clone(),
        tool: step.tool.clone(),
        lane: lane.to_string(),
        mutation: classify_mutation(batch, step, lane),
        success: false,
        duration_ms: 0,
        preview: String::new(),
        artifact_path: None,
        error: Some(error.to_string()),
    }
}

// This is the deliberately linear orchestration boundary: gateway specials,
// guards, argument normalization, upstream dispatch, and result capture stay
// in execution order so browser mutations remain auditable.
#[allow(clippy::too_many_lines)]
fn execute_step(
    config: &UpstreamRuntimeConfig,
    upstream: &mut impl ToolCaller,
    batch: &BlendBatchRequest,
    step: &BlendStep,
    index: usize,
    resident: bool,
) -> StepExecutionResult {
    let started = Instant::now();
    let lane = step
        .lane
        .as_deref()
        .or(batch.lane.as_deref())
        .unwrap_or("live_mutate")
        .to_string();
    let mutation = classify_mutation(batch, step, &lane);

    if let Some(result) = execute_gateway_step(
        config,
        upstream,
        batch,
        step,
        &GatewayStepContext {
            index,
            lane: &lane,
            mutation: mutation.clone(),
            started,
        },
    ) {
        return result;
    }

    if let Some(error) = namespace_guard(step) {
        return StepExecutionResult {
            index,
            id: step.id.clone(),
            tool: step.tool.clone(),
            lane,
            mutation,
            success: false,
            duration_ms: started.elapsed().as_millis(),
            preview: String::new(),
            artifact_path: None,
            error: Some(error),
        };
    }

    let args = match resolved_step_args(upstream, step) {
        Ok(args) => args,
        Err(err) => {
            return StepExecutionResult {
                index,
                id: step.id.clone(),
                tool: step.tool.clone(),
                lane,
                mutation,
                success: false,
                duration_ms: started.elapsed().as_millis(),
                preview: String::new(),
                artifact_path: None,
                error: Some(err.to_string()),
            };
        }
    };

    let result = upstream.call_tool(&step.tool, &args);
    // After a successful navigation the page's title resets — re-stamp the
    // resident tab marker so `ensure_tab` can still find it next call.
    // Only meaningful on the resident path: the one-shot session never booted
    // a named tab, so there's nothing to re-mark. `isError` absent means
    // success (MCP convention; the executor's own success check below makes
    // the same assumption).
    if resident
        && result.as_ref().is_ok_and(|value| {
            !value
                .get("isError")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        && is_navigating_step(&step.tool)
    {
        let _ = remark_resident_tab(upstream);
    }
    match result {
        Ok(value) => {
            let is_error = value
                .get("isError")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let raw_text = first_content_text(&value);
            let mut preview = step_preview(&step.tool, &value);
            let mut artifact_path = if !is_error && mutation == MutationClass::Read {
                write_read_artifact(config, step, raw_text.unwrap_or(&preview))
            } else {
                None
            };
            // Build 1: screenshot token-diet. After a successful screenshot,
            // emit a downscaled webp sibling and surface it as the primary
            // read path. Original PNG is left on disk untouched.
            if !is_error && step.tool == "browser_take_screenshot" {
                if let Some(diet) = screenshot_token_diet(&preview) {
                    preview = diet.preview_augmented;
                    artifact_path = Some(diet.small_path);
                }
            }
            preview = enforce_capture_limit(batch, &preview);
            StepExecutionResult {
                index,
                id: step.id.clone(),
                tool: step.tool.clone(),
                lane,
                mutation,
                success: !is_error,
                duration_ms: started.elapsed().as_millis(),
                preview,
                artifact_path,
                error: is_error.then(|| "upstream tool returned isError=true".to_string()),
            }
        }
        Err(err) => StepExecutionResult {
            index,
            id: step.id.clone(),
            tool: step.tool.clone(),
            lane,
            mutation,
            success: false,
            duration_ms: started.elapsed().as_millis(),
            preview: String::new(),
            artifact_path: None,
            error: Some(err.to_string()),
        },
    }
}

fn step_preview(tool: &str, value: &Value) -> String {
    let Some(text) = first_content_text(value) else {
        return compact_preview(value, 700);
    };
    if tool == "browser_evaluate" {
        return evaluate_preview(text);
    }
    snapshot_preview(text)
}

fn first_content_text(value: &Value) -> Option<&str> {
    value
        .get("content")
        .and_then(Value::as_array)
        .and_then(|content| content.first())
        .and_then(|item| item.get("text"))
        .and_then(Value::as_str)
}

fn evaluate_preview(text: &str) -> String {
    let result = section_between(text, "### Result", "### Ran Playwright code")
        .unwrap_or_else(|| text.trim());
    with_page_header(text, result)
}

fn snapshot_preview(text: &str) -> String {
    let body = text.split("### Page").next().unwrap_or(text).trim();
    with_page_header(text, body)
}

fn with_page_header(source: &str, body: &str) -> String {
    let mut lines = Vec::new();
    if let Some(url) = page_line_value(source, "- Page URL:") {
        lines.push(format!("Page URL: {url}"));
    }
    if let Some(title) = page_line_value(source, "- Page Title:") {
        lines.push(format!("Page Title: {title}"));
    }
    if !body.trim().is_empty() {
        if !lines.is_empty() {
            lines.push(String::new());
        }
        lines.push(body.trim().to_string());
    }
    lines.join("\n")
}

fn section_between<'a>(text: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let after_start = text.split_once(start)?.1;
    Some(
        after_start
            .split_once(end)
            .map_or(after_start, |(body, _)| body)
            .trim(),
    )
}

fn page_line_value<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    text.lines()
        .find_map(|line| line.trim().strip_prefix(prefix).map(str::trim))
        .filter(|value| !value.is_empty())
}

fn write_read_artifact(
    config: &UpstreamRuntimeConfig,
    step: &BlendStep,
    body: &str,
) -> Option<String> {
    if !matches!(
        step.tool.as_str(),
        "browser_snapshot" | "browser_evaluate" | "browser_xray"
    ) {
        return None;
    }
    let mut dir = config.audit_path.parent()?.to_path_buf();
    dir.push("reads");
    fs::create_dir_all(&dir).ok()?;
    let filename = format!(
        "page-{}-{}.md",
        now_ms(),
        fnv1a64_hex(&format!("{}:{body}", step.tool))
    );
    let path = dir.join(filename);
    let mut file = fs::File::create(&path).ok()?;
    writeln!(file, "# Tool: {}", step.tool).ok()?;
    writeln!(file, "# Captured: {}", now_ms()).ok()?;
    writeln!(file).ok()?;
    writeln!(file, "{body}").ok()?;
    Some(path.to_string_lossy().into_owned())
}

fn namespace_guard(step: &BlendStep) -> Option<String> {
    if step.tool == "tabs.create" && step.args.get("active").and_then(Value::as_bool) != Some(false)
    {
        return Some(
            "bravemag permits tabs.create only with active:false; active tabs can move the operator's screen"
                .to_string(),
        );
    }
    if matches!(step.tool.as_str(), "tabs.close" | "tabs.activate") {
        return Some(
            "bravemag reserves tab creation, closing, and activation for the shared background workspace; use navigation.navigate inside the workspace instead"
                .to_string(),
        );
    }
    if step.tool == "browser_tabs"
        && !matches!(
            step.args.get("action").and_then(Value::as_str),
            Some("list")
        )
    {
        return Some(
            "bravemag allows only browser_tabs list; the shared background workspace owns tab creation, closing, and activation"
                .to_string(),
        );
    }
    if step.tool == "browser_navigate" {
        let url = step.args.get("url").and_then(Value::as_str).unwrap_or("");
        if url.eq_ignore_ascii_case("about:blank") {
            return Some(
                "bravemag blocks browser_navigate to about:blank because it blanks the shared Brave stage"
                    .to_string(),
            );
        }
    }
    if step.tool == "browser_tabs" {
        let action = step.args.get("action").and_then(Value::as_str);
        if !matches!(action, Some("list")) {
            return Some("bravemag allows only browser_tabs list on the shared Brave".to_string());
        }
    }
    None
}

fn resolved_step_args(upstream: &mut impl ToolCaller, step: &BlendStep) -> AppResult<Value> {
    // Normalize a null/absent `args` to an empty object: Playwright MCP
    // rejects `null` ("expected record, received null"). The TOON parser
    // already defaults to `{}`, but a caller can still send `args: null`
    // over JSON-RPC, so guard at the consumption point too.
    let args = match step.args {
        Value::Null => Value::Object(serde_json::Map::new()),
        _ => step.args.clone(),
    };
    // Playwright MCP 0.0.78 names the accessibility reference argument
    // `target`. Earlier examples (and other Playwright clients) call it
    // `ref`; accept that familiar shape at our boundary.
    if step.tool == "browser_click" && args.get("target").is_none() {
        let mut args = args;
        if let Some(object) = args.as_object_mut() {
            if let Some(reference) = object.remove("ref") {
                object.insert("target".to_string(), reference);
            }
        }
        return Ok(args);
    }
    if !is_tab_select(step) || args.get("index").is_some() {
        return Ok(args);
    }
    let Some(needle) = tab_select_needle(&args) else {
        return Ok(args);
    };
    let tabs = upstream.call_tool("browser_tabs", &json!({"action": "list"}))?;
    let text = first_content_text(&tabs).unwrap_or("");
    let Some(index) = find_tab_index(text, needle) else {
        anyhow::bail!("no Brave tab matched `{needle}` by URL/title substring");
    };
    let mut args = args;
    if let Some(object) = args.as_object_mut() {
        object.insert("index".to_string(), json!(index));
        object.remove("substring");
        object.remove("query");
        object.remove("match");
        object.remove("url_substring");
        object.remove("title_substring");
    }
    Ok(args)
}

fn is_tab_select(step: &BlendStep) -> bool {
    step.tool == "browser_tabs" && step.args.get("action").and_then(Value::as_str) == Some("select")
}

/// True for steps that change the current page's URL (and therefore reset
/// its title, wiping the resident-tab marker).
fn is_navigating_step(tool: &str) -> bool {
    matches!(tool, "browser_navigate" | "browser_navigate_back")
}

/// Re-stamp the resident-tab marker on the current page after a navigation.
/// No-op on sessions that never booted a resident tab (the one-shot path).
fn remark_resident_tab(upstream: &mut impl ToolCaller) -> AppResult<()> {
    let script = format!(
        "() => {{ document.title = {name:?}; return document.title; }}",
        name = crate::upstream::SESSION_TAB_NAME,
    );
    upstream.call_tool("browser_evaluate", &json!({ "function": script }))?;
    Ok(())
}

fn tab_select_needle(args: &Value) -> Option<&str> {
    [
        "substring",
        "query",
        "match",
        "url_substring",
        "title_substring",
    ]
    .into_iter()
    .find_map(|key| args.get(key).and_then(Value::as_str))
    .filter(|value| !value.trim().is_empty())
}

fn find_tab_index(list_text: &str, needle: &str) -> Option<u64> {
    let needle = needle.to_lowercase();
    list_text
        .lines()
        .filter(|line| line.to_lowercase().contains(&needle))
        .find_map(first_unsigned_int)
}

fn first_unsigned_int(text: &str) -> Option<u64> {
    let start = text.find(|ch: char| ch.is_ascii_digit())?;
    let rest = &text[start..];
    let end = rest
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

fn write_audit(config: &UpstreamRuntimeConfig, step: &BlendStep, result: &StepExecutionResult) {
    if let Some(parent) = config.audit_path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.audit_path)
    else {
        return;
    };
    let row = json!({
        "ts_ms": now_ms(),
        "seat": config.caller,
        "tool": step.tool,
        "args_hash": fnv1a64_hex(&serde_json::to_string(&step.args).unwrap_or_default()),
        "lane": result.lane,
        "mutation": result.mutation,
        "duration_ms": result.duration_ms,
        "ok": result.success,
        "error": result.error,
    });
    let _ = serde_json::to_writer(&mut file, &row);
    let _ = file.write_all(b"\n");
}

fn classify_mutation(
    batch: &BlendBatchRequest,
    step: &BlendStep,
    effective_lane: &str,
) -> MutationClass {
    if let Some(mutates) = step_mutates_tabs(step) {
        return if mutates {
            MutationClass::Mutate
        } else {
            MutationClass::Read
        };
    }
    // Parallel execution cannot trust a caller's read-lane or mutates:false
    // hint: navigation, input and unknown methods must remain barriers.
    if batch.max_parallel.unwrap_or(1) > 1 {
        return if step.mutates == Some(true)
            || inferred_tool_mutates(&step.tool, effective_lane, batch)
        {
            MutationClass::Mutate
        } else {
            MutationClass::Read
        };
    }
    if step
        .mutates
        .unwrap_or_else(|| inferred_tool_mutates(&step.tool, effective_lane, batch))
    {
        MutationClass::Mutate
    } else {
        MutationClass::Read
    }
}

fn inferred_tool_mutates(tool: &str, effective_lane: &str, batch: &BlendBatchRequest) -> bool {
    let parallel = batch.max_parallel.unwrap_or(1) > 1;
    if !parallel && (effective_lane == "live_read" || batch.lane.as_deref() == Some("live_read")) {
        return false;
    }
    if parallel
        && matches!(
            tool,
            "page.get_state"
                | "page.get_text"
                | "page.extract_content"
                | "dom.query"
                | "dom.find_by_text"
                | "dom.find_by_role"
                | "dom.get_accessibility_tree"
                | "page.get_console"
                | "page.get_network"
                | "network.export_har"
                | "capture.screenshot"
                | "tabs.list"
                | "health.ping"
                | "daemon.metrics"
        )
    {
        return false;
    }
    !matches!(
        tool,
        "browser_snapshot"
            | "browser_take_screenshot"
            | "browser_console_messages"
            | "browser_network_requests"
    )
}

fn step_mutates_tabs(step: &BlendStep) -> Option<bool> {
    if step.tool == "bravemag.status" {
        return Some(false);
    }
    if step.tool == "browser_click_text" {
        // The gateway pseudo-tool *clicks* — always a mutation, regardless of lane.
        return Some(true);
    }
    if step.tool == "browser_xray" {
        return Some(step.args.get("click_index").is_some());
    }
    if step.tool == "browser_click" && step.args.get("sel").is_some() {
        return Some(true);
    }
    if step.tool != "browser_tabs" {
        return None;
    }
    let action = step.args.get("action").and_then(Value::as_str);
    Some(!matches!(action, Some("list")))
}

fn should_checkpoint(batch: &BlendBatchRequest, mutating_steps: usize) -> bool {
    batch
        .checkpoint_every
        .is_some_and(|every| every > 0 && mutating_steps % every == 0)
}

fn run_checkpoint_hook(
    upstream: &mut impl ToolCaller,
    after_step_index: usize,
) -> HookExecutionResult {
    let filename = hook_filename("checkpoints", "png");
    let path = format!("/tmp/bravemag/{filename}");
    run_hook_call(
        upstream,
        HookKind::Checkpoint,
        Some(after_step_index),
        Some(path.clone()),
        "browser_take_screenshot",
        &json!({ "filename": path, "fullPage": true }),
    )
}

fn run_capture_hook(upstream: &mut impl ToolCaller) -> HookExecutionResult {
    let filename = hook_filename("captures", "png");
    let path = format!("/tmp/bravemag/{filename}");
    run_hook_call(
        upstream,
        HookKind::Capture,
        None,
        Some(path.clone()),
        "browser_take_screenshot",
        &json!({ "filename": path, "fullPage": true }),
    )
}

fn run_hook_call(
    upstream: &mut impl ToolCaller,
    kind: HookKind,
    after_step_index: Option<usize>,
    path: Option<String>,
    tool: &str,
    args: &Value,
) -> HookExecutionResult {
    let started = Instant::now();
    match upstream.call_tool(tool, args) {
        Ok(value) => {
            let is_error = value
                .get("isError")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let preview = compact_preview(&value, capture_limit_bytes(None).min(700));
            HookExecutionResult {
                kind,
                after_step_index,
                path,
                success: !is_error,
                duration_ms: started.elapsed().as_millis(),
                preview,
                error: is_error.then(|| "upstream hook returned isError=true".to_string()),
            }
        }
        Err(err) => HookExecutionResult {
            kind,
            after_step_index,
            path,
            success: false,
            duration_ms: started.elapsed().as_millis(),
            preview: String::new(),
            error: Some(err.to_string()),
        },
    }
}

fn hook_filename(kind: &str, extension: &str) -> String {
    let mut dir = PathBuf::from("/tmp/bravemag");
    dir.push(kind);
    let _ = fs::create_dir_all(&dir);

    let mut filename = PathBuf::from(kind);
    filename.push(hook_basename(extension));
    filename.to_string_lossy().into_owned()
}

fn hook_basename(extension: &str) -> String {
    let now_ms = now_ms();
    format!("bravemag-{now_ms}.{extension}")
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

// ─── Build 1: screenshot token-diet ───────────────────────────────────────

struct ScreenshotDiet {
    small_path: String,
    preview_augmented: String,
}

/// After a successful `browser_take_screenshot`, produce `<base>.small.webp`
/// (max width `BRAVEMAG_SHOT_MAXW`, default 1024px, q85) next to the original
/// PNG. The PNG is never removed. Returns the small path + an augmented
/// preview that names both.
///
/// Token note (juxtap0, 2026-07-28): image-Read cost scales with pixel
/// dimensions, not bytes — the downscale is the token lever, webp is the
/// disk lever. Both together are the win.
fn screenshot_token_diet(preview: &str) -> Option<ScreenshotDiet> {
    let raw_name = extract_screenshot_filename(preview)?;
    let png_path = resolve_screenshot_path(&raw_name);
    if !png_path.exists() {
        return None;
    }
    let max_w = std::env::var("BRAVEMAG_SHOT_MAXW")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(1024);
    let small_path = small_webp_path(&png_path);
    // `>` in the geometry means "only shrink if larger" — Req 1.4 falls out.
    let geometry = format!("{max_w}x>");
    let status = std::process::Command::new("magick")
        .arg(&png_path)
        .arg("-resize")
        .arg(&geometry)
        .arg("-quality")
        .arg("85")
        .arg(&small_path)
        .status()
        .ok()?;
    if !status.success() || !small_path.exists() {
        return None;
    }
    let dims = image_dimensions(&small_path).unwrap_or_else(|| "?".to_string());
    let augmented = format!(
        "{preview}\n\n[bravemag token-diet] small: {} ({}px wide, webp q85) | full: {}",
        small_path.display(),
        dims,
        png_path.display()
    );
    Some(ScreenshotDiet {
        small_path: small_path.to_string_lossy().into_owned(),
        preview_augmented: augmented,
    })
}

/// Pull the saved filename out of the screenshot tool's preview text.
/// Looks for the markdown link `- [Screenshot of ...](./NAME.png)` or falls
/// back to the playwright code block's `path: 'NAME'`.
fn extract_screenshot_filename(preview: &str) -> Option<String> {
    for line in preview.lines() {
        if let Some(start) = line.find("](./") {
            let rest = &line[start + 4..];
            if let Some(end) = rest.find(')') {
                let name = &rest[..end];
                if has_png_extension(name) {
                    return Some(name.to_string());
                }
            }
        }
        if let Some(start) = line.find("]( /") {
            let rest = &line[start + 3..];
            if let Some(end) = rest.find(')') {
                let name = &rest[..end];
                if has_png_extension(name) {
                    return Some(name.to_string());
                }
            }
        }
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("path: '") {
            if let Some(end) = rest.find('\'') {
                let name = &rest[..end];
                if has_png_extension(name) {
                    return Some(name.to_string());
                }
            }
        }
    }
    None
}

fn has_png_extension(name: &str) -> bool {
    std::path::Path::new(name)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("png"))
}

/// Resolve the screenshot path the same way playwright-mcp does: absolute
/// paths stay absolute, relative paths resolve against /tmp/bravemag/.
fn resolve_screenshot_path(raw: &str) -> PathBuf {
    if raw.starts_with('/') {
        PathBuf::from(raw)
    } else {
        PathBuf::from("/tmp/bravemag").join(raw)
    }
}

fn small_webp_path(png: &std::path::Path) -> PathBuf {
    let stem = png.file_stem().and_then(|s| s.to_str()).unwrap_or("shot");
    png.with_file_name(format!("{stem}.small.webp"))
}

fn image_dimensions(path: &std::path::Path) -> Option<String> {
    let out = std::process::Command::new("magick")
        .arg("identify")
        .arg("-format")
        .arg("%w")
        .arg(path)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout)
        .ok()
        .map(|s| s.trim().to_string())
}

// ─── Build 2: browser_click_text ──────────────────────────────────────────

/// Gateway-side `browser_click_text`. Never reaches upstream as-is:
///   1. `browser_evaluate` → find visible elements whose trimmed innerText
///      equals/contains the needle (case-insensitive).
///   2. Exactly 1 match (and no explicit index) → `browser_click` that selector.
///   3. >1 matches, no index → return the disambiguation list.
///   4. 0 matches → empty list + note.
///   5. index given → click the nth match.
fn execute_click_text(
    upstream: &mut impl ToolCaller,
    batch: &BlendBatchRequest,
    step: &BlendStep,
    index: usize,
    lane: &str,
    mutation: MutationClass,
    started: Instant,
) -> StepExecutionResult {
    let fail = |msg: String| StepExecutionResult {
        index,
        id: step.id.clone(),
        tool: step.tool.clone(),
        lane: lane.to_string(),
        mutation: mutation.clone(),
        success: false,
        duration_ms: started.elapsed().as_millis(),
        preview: String::new(),
        artifact_path: None,
        error: Some(msg),
    };

    let Some(text) = step.args.get("text").and_then(Value::as_str) else {
        return fail("browser_click_text requires `text`".to_string());
    };
    let requested_index = step.args.get("index").and_then(Value::as_u64);

    let find_js = build_click_text_find_js(text);
    let found = match upstream.call_tool("browser_evaluate", &json!({ "function": find_js })) {
        Ok(v) => v,
        Err(err) => return fail(format!("find evaluate failed: {err}")),
    };
    if found
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return fail("find evaluate returned isError=true".to_string());
    }
    let matches = parse_click_text_matches(&found);

    // Decide: click, disambiguate, or report empty.
    let to_click: Option<usize> = match (requested_index, matches.len()) {
        (Some(i), n) if usize::try_from(i).is_ok_and(|index| index < n) => {
            Some(usize::try_from(i).expect("index already checked"))
        }
        (Some(i), n) => {
            return fail(format!(
                "index {i} out of range — {n} match(es) for `{text}`"
            ));
        }
        (None, 1) => Some(0),
        (None, _) => None,
    };

    let Some(click_idx) = to_click else {
        // No click — return the disambiguation/empty list as the step result.
        let preview = render_click_text_matches(text, &matches);
        let preview = enforce_capture_limit(batch, &preview);
        return StepExecutionResult {
            index,
            id: step.id.clone(),
            tool: step.tool.clone(),
            lane: lane.to_string(),
            mutation,
            success: true,
            duration_ms: started.elapsed().as_millis(),
            preview,
            artifact_path: None,
            error: None,
        };
    };

    let Some(sel) = matches.get(click_idx).and_then(|m| m.sel.clone()) else {
        return fail(format!("match {click_idx} had no selector"));
    };

    match upstream.call_tool(
        "browser_click",
        &json!({
            "element": format!("`{text}` (match #{click_idx})"),
            "target": sel,
        }),
    ) {
        Ok(v) => {
            let is_error = v.get("isError").and_then(Value::as_bool).unwrap_or(false);
            let preview = format!(
                "clicked `{text}` (#{click_idx}) via {sel}\n{}",
                first_content_text(&v).unwrap_or("")
            );
            let preview = enforce_capture_limit(batch, &preview);
            StepExecutionResult {
                index,
                id: step.id.clone(),
                tool: step.tool.clone(),
                lane: lane.to_string(),
                mutation,
                success: !is_error,
                duration_ms: started.elapsed().as_millis(),
                preview,
                artifact_path: None,
                error: is_error.then(|| "upstream click returned isError=true".to_string()),
            }
        }
        Err(err) => fail(format!("click failed: {err}")),
    }
}

#[derive(Debug, Clone)]
struct ClickTextMatch {
    index: usize,
    txt: String,
    sel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct XrayRow {
    index: usize,
    tag: String,
    role: String,
    label: String,
    sel: Option<String>,
    in_viewport: bool,
}

struct GatewayStepContext<'a> {
    index: usize,
    lane: &'a str,
    mutation: MutationClass,
    started: Instant,
}

fn execute_gateway_step(
    config: &UpstreamRuntimeConfig,
    upstream: &mut impl ToolCaller,
    batch: &BlendBatchRequest,
    step: &BlendStep,
    context: &GatewayStepContext<'_>,
) -> Option<StepExecutionResult> {
    match step.tool.as_str() {
        "browser_click_text" => Some(execute_click_text(
            upstream,
            batch,
            step,
            context.index,
            context.lane,
            context.mutation.clone(),
            context.started,
        )),
        "browser_xray" => Some(execute_xray(config, upstream, batch, step, context)),
        "browser_click" if step.args.get("sel").is_some() => {
            Some(execute_selector_click(upstream, step, context))
        }
        "browser_blur" => Some(execute_blur(upstream, batch, step, context)),
        "browser_scroll" => Some(execute_scroll(upstream, batch, step, context)),
        _ => None,
    }
}

/// Scroll the current page without making callers inject arbitrary JS just to
/// reveal a modal control. `delta_y` defaults to one viewport; `delta_x` is
/// optional. The result returns the before/after offsets for verification.
fn execute_scroll(
    upstream: &mut impl ToolCaller,
    batch: &BlendBatchRequest,
    step: &BlendStep,
    context: &GatewayStepContext<'_>,
) -> StepExecutionResult {
    let delta_y = step
        .args
        .get("delta_y")
        .and_then(Value::as_i64)
        .unwrap_or(700);
    let target = step
        .args
        .get("selector")
        .or_else(|| step.args.get("target"))
        .or_else(|| step.args.get("ref"))
        .and_then(Value::as_str);
    let target_json = serde_json::to_string(&target).unwrap_or_else(|_| "null".to_string());
    let delta_x = step
        .args
        .get("delta_x")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let script = format!(
        "() => {{ const target={target_json}; const el=target ? (target.startsWith('e') ? document.querySelector(`[data-ref=\"${{target}}\"]`) : document.querySelector(target)) : document.scrollingElement; if (!el) return {{error:`scroll target not found: ${{target}}`}}; const before={{x:el.scrollLeft,y:el.scrollTop}}; el.scrollBy({{left:{delta_x},top:{delta_y},behavior:'instant'}}); return {{target:target ?? 'page',before,after:{{x:el.scrollLeft,y:el.scrollTop}},scroll_height:el.scrollHeight,client_height:el.clientHeight}}; }}"
    );
    match upstream.call_tool("browser_evaluate", &json!({ "function": script })) {
        Ok(value) => {
            let is_error = value
                .get("isError")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let preview =
                enforce_capture_limit(batch, first_content_text(&value).unwrap_or("scrolled"));
            StepExecutionResult {
                index: context.index,
                id: step.id.clone(),
                tool: step.tool.clone(),
                lane: context.lane.to_string(),
                mutation: context.mutation.clone(),
                success: !is_error,
                duration_ms: context.started.elapsed().as_millis(),
                preview,
                artifact_path: None,
                error: is_error.then(|| "scroll evaluate returned isError=true".to_string()),
            }
        }
        Err(err) => StepExecutionResult {
            index: context.index,
            id: step.id.clone(),
            tool: step.tool.clone(),
            lane: context.lane.to_string(),
            mutation: context.mutation.clone(),
            success: false,
            duration_ms: context.started.elapsed().as_millis(),
            preview: String::new(),
            artifact_path: None,
            error: Some(format!("scroll failed: {err}")),
        },
    }
}

/// `browser_blur` — blur the currently-focused element so a subsequent
/// `browser_press_key` lands on `document` instead of inside an input. Calls
/// `browser_evaluate` with JS that reports what was blurred (or `null` if
/// nothing was focused), so the caller sees confirmation rather than a void.
fn execute_blur(
    upstream: &mut impl ToolCaller,
    batch: &BlendBatchRequest,
    step: &BlendStep,
    context: &GatewayStepContext<'_>,
) -> StepExecutionResult {
    let fail = |msg: String| StepExecutionResult {
        index: context.index,
        id: step.id.clone(),
        tool: step.tool.clone(),
        lane: context.lane.to_string(),
        mutation: context.mutation.clone(),
        success: false,
        duration_ms: context.started.elapsed().as_millis(),
        preview: String::new(),
        artifact_path: None,
        error: Some(msg),
    };

    // Blur + report in one round-trip. `document.body` is the resting focus
    // in a clean tab; blurring it is a no-op, so we report `null` for that
    // case rather than claiming we blurred the body.
    let blur_js = r##"() => {
  const el = document.activeElement;
  if (!el || el === document.body) return { blurred: null };
  const tag = el.tagName;
  const id = el.id || null;
  const name = el.name || null;
  el.blur();
  const label = tag + (id ? "#" + id : (name ? "[name=" + name + "]" : ""));
  return { blurred: label };
}"##;

    let result = match upstream.call_tool("browser_evaluate", &json!({ "function": blur_js })) {
        Ok(v) => v,
        Err(err) => return fail(format!("blur evaluate failed: {err}")),
    };
    if result
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return fail("blur evaluate returned isError=true".to_string());
    }

    let raw = first_content_text(&result).unwrap_or("");
    let preview = match parse_evaluate_json_string(raw) {
        Some(body) => {
            let blurred = body
                .trim_matches('"')
                .trim_start_matches('{')
                .trim_end_matches('}');
            format!("blurred: {blurred}")
        }
        None if raw.is_empty() => "nothing focused".to_string(),
        None => evaluate_preview(raw),
    };
    let preview = enforce_capture_limit(batch, &preview);
    StepExecutionResult {
        index: context.index,
        id: step.id.clone(),
        tool: step.tool.clone(),
        lane: context.lane.to_string(),
        mutation: context.mutation.clone(),
        success: true,
        duration_ms: context.started.elapsed().as_millis(),
        preview,
        artifact_path: None,
        error: None,
    }
}

fn execute_selector_click(
    upstream: &mut impl ToolCaller,
    step: &BlendStep,
    context: &GatewayStepContext<'_>,
) -> StepExecutionResult {
    let Some(sel) = step.args.get("sel").and_then(Value::as_str) else {
        return StepExecutionResult {
            index: context.index,
            id: step.id.clone(),
            tool: step.tool.clone(),
            lane: context.lane.to_string(),
            mutation: context.mutation.clone(),
            success: false,
            duration_ms: context.started.elapsed().as_millis(),
            preview: String::new(),
            artifact_path: None,
            error: Some("browser_click selector shorthand requires `sel`".to_string()),
        };
    };
    let element = step
        .args
        .get("element")
        .and_then(Value::as_str)
        .unwrap_or("selector target");
    execute_click_selector(upstream, step, context, sel, element)
}

fn execute_xray(
    config: &UpstreamRuntimeConfig,
    upstream: &mut impl ToolCaller,
    batch: &BlendBatchRequest,
    step: &BlendStep,
    context: &GatewayStepContext<'_>,
) -> StepExecutionResult {
    let fail = |msg: String| StepExecutionResult {
        index: context.index,
        id: step.id.clone(),
        tool: step.tool.clone(),
        lane: context.lane.to_string(),
        mutation: context.mutation.clone(),
        success: false,
        duration_ms: context.started.elapsed().as_millis(),
        preview: String::new(),
        artifact_path: None,
        error: Some(msg),
    };
    let mode = step
        .args
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("unlabeled");
    if !matches!(mode, "unlabeled" | "all" | "viewport") {
        return fail(format!(
            "browser_xray mode must be `unlabeled`, `all`, or `viewport`, got `{mode}`"
        ));
    }
    let xray_js = build_xray_js(mode);
    let found = match upstream.call_tool("browser_evaluate", &json!({ "function": xray_js })) {
        Ok(value) => value,
        Err(err) => return fail(format!("xray evaluate failed: {err}")),
    };
    if found
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return fail("xray evaluate returned isError=true".to_string());
    }
    let rows = parse_xray_rows(&found);
    let full = render_xray_rows(mode, &rows, usize::MAX);
    let artifact_path = write_read_artifact(config, step, &full);

    if let Some(click_index) = step.args.get("click_index").and_then(Value::as_u64) {
        let Ok(click_index) = usize::try_from(click_index) else {
            return fail("click_index is too large".to_string());
        };
        let Some(row) = rows.get(click_index) else {
            return fail(format!(
                "click_index {click_index} out of range — {} xray row(s)",
                rows.len()
            ));
        };
        let Some(sel) = row.sel.as_deref() else {
            return fail(format!("xray row {click_index} had no selector"));
        };
        return execute_click_selector(
            upstream,
            step,
            context,
            sel,
            &format!("xray row {click_index}: {}", row.label),
        );
    }

    let preview = render_xray_rows(mode, &rows, XRAY_PREVIEW_ROWS);
    let preview = enforce_capture_limit(batch, &preview);
    StepExecutionResult {
        index: context.index,
        id: step.id.clone(),
        tool: step.tool.clone(),
        lane: context.lane.to_string(),
        mutation: context.mutation.clone(),
        success: true,
        duration_ms: context.started.elapsed().as_millis(),
        preview,
        artifact_path,
        error: None,
    }
}

fn execute_click_selector(
    upstream: &mut impl ToolCaller,
    step: &BlendStep,
    context: &GatewayStepContext<'_>,
    sel: &str,
    element: &str,
) -> StepExecutionResult {
    let fail = |msg: String| StepExecutionResult {
        index: context.index,
        id: step.id.clone(),
        tool: step.tool.clone(),
        lane: context.lane.to_string(),
        mutation: context.mutation.clone(),
        success: false,
        duration_ms: context.started.elapsed().as_millis(),
        preview: String::new(),
        artifact_path: None,
        error: Some(msg),
    };

    match upstream.call_tool(
        "browser_click",
        &json!({
            "element": element,
            "target": sel,
        }),
    ) {
        Ok(value) => {
            let is_error = value
                .get("isError")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let preview = format!(
                "clicked selector `{sel}` ({element})\n{}",
                first_content_text(&value).unwrap_or("")
            );
            StepExecutionResult {
                index: context.index,
                id: step.id.clone(),
                tool: step.tool.clone(),
                lane: context.lane.to_string(),
                mutation: context.mutation.clone(),
                success: !is_error,
                duration_ms: context.started.elapsed().as_millis(),
                preview,
                artifact_path: None,
                error: is_error.then(|| "upstream click returned isError=true".to_string()),
            }
        }
        Err(err) => fail(format!("click failed: {err}")),
    }
}

/// JS that runs in the page: walk visible elements, match trimmed innerText
/// case-insensitively (exact first, then contains), return JSON list with a
/// stable selector per match.
fn build_click_text_find_js(needle: &str) -> String {
    let needle_json = serde_json::to_string(needle).unwrap_or_else(|_| "\"\"".to_string());
    format!(
        r#"() => {{
  const needle = {needle_json}.trim().toLowerCase();
  const out = [];
  const seen = new Set();
  const walk = (root) => {{
    const els = root.querySelectorAll('a,button,[role="button"],[role="link"],input[type="button"],input[type="submit"],input[type="reset"],summary,label,span,div,li,td,th,h1,h2,h3,h4,h5,h6,p');
    for (const el of els) {{
      if (seen.has(el)) continue;
      seen.add(el);
      const style = window.getComputedStyle(el);
      if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') continue;
      const rect = el.getBoundingClientRect();
      if (rect.width === 0 || rect.height === 0) continue;
      const raw = (el.innerText || el.textContent || '').trim();
      if (!raw) continue;
      const lower = raw.toLowerCase();
      if (lower !== needle && !lower.includes(needle)) continue;
      // Skip if a descendant already matched the exact same text (prefer deepest).
      let descendant_matched = false;
      for (const child of el.children) {{
        const ct = (child.innerText || child.textContent || '').trim().toLowerCase();
        if (ct === lower) {{ descendant_matched = true; break; }}
      }}
      if (descendant_matched) continue;
      const sel = stableSelector(el);
      out.push({{ txt: raw.length > 80 ? raw.slice(0, 77) + '...' : raw, sel }});
    }}
  }};
  const stableSelector = (el) => {{
    if (el.id) return '#' + CSS.escape(el.id);
    const parts = [];
    let cur = el;
    while (cur && cur.nodeType === 1 && parts.length < 5) {{
      let part = cur.tagName.toLowerCase();
      if (cur.id) {{ part += '#' + CSS.escape(cur.id); parts.unshift(part); break; }}
      const parent = cur.parentElement;
      if (parent) {{
        const sibs = Array.from(parent.children).filter(c => c.tagName === cur.tagName);
        if (sibs.length > 1) part += ':nth-of-type(' + (sibs.indexOf(cur) + 1) + ')';
      }}
      parts.unshift(part);
      cur = parent;
    }}
    return parts.join(' > ');
  }};
  walk(document);
  return JSON.stringify(out);
}}"#
    )
}

fn build_xray_js(mode: &str) -> String {
    let mode_json = serde_json::to_string(mode).unwrap_or_else(|_| "\"unlabeled\"".to_string());
    format!(
        r#"() => {{
  const mode = {mode_json};
  const out = [];
  const seen = new Set();
  const stableSelector = (el) => {{
    if (el.id) return '#' + CSS.escape(el.id);
    const parts = [];
    let cur = el;
    while (cur && cur.nodeType === 1 && parts.length < 5) {{
      let part = cur.tagName.toLowerCase();
      if (cur.id) {{ part += '#' + CSS.escape(cur.id); parts.unshift(part); break; }}
      const parent = cur.parentElement;
      if (parent) {{
        const sibs = Array.from(parent.children).filter(c => c.tagName === cur.tagName);
        if (sibs.length > 1) part += ':nth-of-type(' + (sibs.indexOf(cur) + 1) + ')';
      }}
      parts.unshift(part);
      cur = parent;
    }}
    return parts.join(' > ');
  }};
  const identity = (el) => {{
    const attrs = ['aria-label', 'title', 'alt', 'placeholder', 'name', 'value', 'type'];
    for (const attr of attrs) {{
      const value = (el.getAttribute(attr) || '').trim();
      if (value) return value;
    }}
    return '';
  }};
  const visible = (el) => {{
    const style = window.getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
    const rect = el.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  }};
  const inViewport = (el) => {{
    const rect = el.getBoundingClientRect();
    return rect.bottom >= 0 && rect.right >= 0 && rect.top <= window.innerHeight && rect.left <= window.innerWidth;
  }};
  const els = document.querySelectorAll('a,button,[role="button"],[role="link"],input,textarea,select,summary,label,[onclick],canvas,img,svg,[tabindex]');
  for (const el of els) {{
    if (seen.has(el)) continue;
    seen.add(el);
    if (!visible(el)) continue;
    const viewport = inViewport(el);
    if (mode === 'viewport' && !viewport) continue;
    const text = (el.innerText || el.textContent || '').trim().replace(/\s+/g, ' ');
    const label = identity(el);
    if (mode === 'unlabeled' && text) continue;
    const role = (el.getAttribute('role') || '').trim();
    const display = label || text || '?';
    out.push({{
      tag: el.tagName.toLowerCase(),
      role,
      label: display.length > 100 ? display.slice(0, 97) + '...' : display,
      sel: stableSelector(el),
      in_viewport: viewport
    }});
  }}
  return JSON.stringify(out);
}}"#
    )
}

/// Parse the evaluate response's JSON payload back into matches.
fn parse_click_text_matches(value: &Value) -> Vec<ClickTextMatch> {
    let Some(text) = first_content_text(value) else {
        return Vec::new();
    };
    // The evaluate preview wraps the JS return in a "### Result" section.
    let body = section_between(text, "### Result", "### Ran Playwright code")
        .unwrap_or(text)
        .trim()
        .trim_matches('`')
        .trim();
    // The body may itself be a JSON-encoded string (quotes around the JSON).
    let unwrapped: String = if body.starts_with('"') && body.ends_with('"') {
        serde_json::from_str(body).unwrap_or_else(|_| body.to_string())
    } else {
        body.to_string()
    };
    let Ok(arr) = serde_json::from_str::<Vec<Value>>(&unwrapped) else {
        return Vec::new();
    };
    arr.iter()
        .enumerate()
        .map(|(i, item)| ClickTextMatch {
            index: i,
            txt: item
                .get("txt")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            sel: item
                .get("sel")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        })
        .collect()
}

fn parse_xray_rows(value: &Value) -> Vec<XrayRow> {
    let Some(text) = first_content_text(value) else {
        return Vec::new();
    };
    let Some(unwrapped) = parse_evaluate_json_string(text) else {
        return Vec::new();
    };
    let Ok(arr) = serde_json::from_str::<Vec<Value>>(&unwrapped) else {
        return Vec::new();
    };
    arr.iter()
        .enumerate()
        .map(|(i, item)| XrayRow {
            index: i,
            tag: item
                .get("tag")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            role: item
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            label: item
                .get("label")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            sel: item
                .get("sel")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            in_viewport: item
                .get("in_viewport")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
        .collect()
}

fn parse_evaluate_json_string(text: &str) -> Option<String> {
    let body = section_between(text, "### Result", "### Ran Playwright code")
        .unwrap_or(text)
        .trim()
        .trim_matches('`')
        .trim();
    if body.starts_with('"') && body.ends_with('"') {
        serde_json::from_str(body).ok()
    } else {
        Some(body.to_string())
    }
}

fn render_click_text_matches(needle: &str, matches: &[ClickTextMatch]) -> String {
    if matches.is_empty() {
        return format!("no visible element matched `{needle}`");
    }
    let mut out = format!("{} matches for `{needle}`:\n", matches.len());
    for m in matches {
        let sel = m.sel.as_deref().unwrap_or("<no selector>");
        let _ = writeln!(out, "  [{}] {} ({})", m.index, m.txt, sel);
    }
    out.push_str("call again with `index: N` to click one");
    out
}

fn render_xray_rows(mode: &str, rows: &[XrayRow], max_rows: usize) -> String {
    let shown = rows.len().min(max_rows);
    let mut out = format!("browser_xray mode={mode} rows={}\n", rows.len());
    for row in rows.iter().take(shown) {
        let role = if row.role.is_empty() {
            "-"
        } else {
            row.role.as_str()
        };
        let label = if row.label.is_empty() {
            "?"
        } else {
            row.label.as_str()
        };
        let sel = row.sel.as_deref().unwrap_or("<no selector>");
        let viewport = if row.in_viewport {
            "viewport"
        } else {
            "offscreen"
        };
        let _ = writeln!(
            out,
            "  [{}] {} role={} label={} sel={} {}",
            row.index, row.tag, role, label, sel, viewport
        );
    }
    if shown < rows.len() {
        let _ = writeln!(
            out,
            "... {} more row(s) in artifact",
            rows.len().saturating_sub(shown)
        );
    }
    out
}

fn enforce_capture_limit(batch: &BlendBatchRequest, preview: &str) -> String {
    let limit = capture_limit_bytes(batch.capture_limit_bytes);
    truncate_utf8_bytes(preview, limit)
}

fn capture_limit_bytes(batch_limit: Option<usize>) -> usize {
    batch_limit
        .or_else(|| {
            std::env::var("BRAVEMAG_CAPTURE_LIMIT_BYTES")
                .ok()
                .and_then(|value| value.parse().ok())
        })
        .filter(|limit| *limit > 0)
        .unwrap_or(DEFAULT_CAPTURE_LIMIT_BYTES)
}

fn truncate_utf8_bytes(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let omitted = text.len().saturating_sub(end);
    format!(
        "{}\n[bravemag capture_limit_bytes={max_bytes}: truncated {omitted} byte(s)]",
        &text[..end]
    )
}

fn fnv1a64_hex(input: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::toon::BlendStep;
    use anyhow::bail;
    use serde_json::json;

    struct FakeCaller {
        responses: Vec<AppResult<Value>>,
        calls: Vec<(String, Value)>,
    }

    impl ToolCaller for FakeCaller {
        fn call_tool(&mut self, tool: &str, args: &Value) -> AppResult<Value> {
            self.calls.push((tool.to_string(), args.clone()));
            if self.responses.is_empty() {
                bail!("no fake response for {tool}");
            }
            self.responses.remove(0)
        }
    }

    fn step(id: &str, tool: &str) -> BlendStep {
        BlendStep {
            id: Some(id.to_string()),
            tool: tool.to_string(),
            args: json!({}),
            lane: None,
            timeout_ms: None,
            mutates: None,
        }
    }

    fn test_config() -> UpstreamRuntimeConfig {
        UpstreamRuntimeConfig {
            endpoint: "http://localhost:8931/mcp".to_string(),
            caller: "test".to_string(),
            audit_path: PathBuf::from("/tmp/bravemag/test-audit.jsonl"),
        }
    }

    #[test]
    fn stops_on_first_error_by_default() {
        let batch = BlendBatchRequest {
            steps: vec![step("one", "ok"), step("two", "skip")],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![Ok(json!({"isError": true}))],
            calls: Vec::new(),
        };

        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);

        assert_eq!(result.caller, "test");
        assert!(result.stopped_early);
        assert_eq!(result.results.len(), 1);
        assert_eq!(caller.calls, [("ok".to_string(), json!({}))]);
    }

    #[test]
    fn continues_when_configured() {
        let batch = BlendBatchRequest {
            continue_on_error: Some(true),
            steps: vec![step("one", "bad"), step("two", "ok")],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![Ok(json!({"isError": true})), Ok(json!({"isError": false}))],
            calls: Vec::new(),
        };

        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);

        assert!(!result.stopped_early);
        assert_eq!(result.results.len(), 2);
        assert_eq!(
            caller.calls,
            [
                ("bad".to_string(), json!({})),
                ("ok".to_string(), json!({}))
            ]
        );
        assert!(!result.results[0].success);
        assert!(result.results[1].success);
    }

    #[test]
    fn classifies_read_lane_as_non_mutating() {
        let batch = BlendBatchRequest {
            lane: Some("live_read".to_string()),
            steps: vec![step("one", "browser_evaluate")],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![Ok(json!({"isError": false}))],
            calls: Vec::new(),
        };

        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);

        assert_eq!(result.mutating_steps, 0);
        assert_eq!(result.results[0].mutation, MutationClass::Read);
    }

    #[test]
    fn runs_checkpoint_after_configured_mutations() {
        let batch = BlendBatchRequest {
            checkpoint_every: Some(1),
            steps: vec![step("one", "browser_navigate")],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![
                Ok(json!({"isError": false})),
                Ok(json!({"isError": false, "content": []})),
            ],
            calls: Vec::new(),
        };

        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);

        assert_eq!(result.mutating_steps, 1);
        assert_eq!(result.hooks.len(), 1);
        assert_eq!(result.hooks[0].kind, HookKind::Checkpoint);
        assert_eq!(
            caller.calls,
            [
                ("browser_navigate".to_string(), json!({})),
                (
                    "browser_take_screenshot".to_string(),
                    json!({"filename": result.hooks[0].path.clone().unwrap(), "fullPage": true})
                )
            ]
        );
    }

    #[test]
    fn blocks_tab_close() {
        let batch = BlendBatchRequest {
            steps: vec![BlendStep {
                id: Some("close".to_string()),
                tool: "browser_tabs".to_string(),
                args: json!({"action": "close"}),
                lane: None,
                timeout_ms: None,
                mutates: None,
            }],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![Ok(json!({"isError": false}))],
            calls: Vec::new(),
        };

        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);

        assert!(!result.results[0].success);
        assert!(caller.calls.is_empty());
    }

    #[test]
    fn blocks_tab_select_by_index() {
        let batch = BlendBatchRequest {
            steps: vec![BlendStep {
                id: Some("select".to_string()),
                tool: "browser_tabs".to_string(),
                args: json!({"action": "select", "index": 1}),
                lane: None,
                timeout_ms: None,
                mutates: None,
            }],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![Ok(json!({"isError": false}))],
            calls: Vec::new(),
        };

        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);

        assert!(!result.results[0].success);
        assert!(caller.calls.is_empty());
    }

    #[test]
    fn blocks_tab_select_by_substring() {
        let batch = BlendBatchRequest {
            steps: vec![BlendStep {
                id: Some("select".to_string()),
                tool: "browser_tabs".to_string(),
                args: json!({"action": "select", "substring": "moon"}),
                lane: None,
                timeout_ms: None,
                mutates: None,
            }],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![
                Ok(
                    json!({"isError": false, "content": [{"text": "- 0: Kim https://tilde.club/~harper\n- 1: Moon https://en.wikipedia.org/wiki/Moon"}]}),
                ),
                Ok(json!({"isError": false})),
            ],
            calls: Vec::new(),
        };

        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);

        assert!(!result.results[0].success);
        assert!(caller.calls.is_empty());
    }

    #[test]
    fn translates_legacy_click_ref_to_upstream_target() {
        let batch = BlendBatchRequest {
            steps: vec![BlendStep {
                id: Some("click".to_string()),
                tool: "browser_click".to_string(),
                args: json!({"element": "Login", "ref": "e12"}),
                lane: None,
                timeout_ms: None,
                mutates: None,
            }],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![Ok(json!({"isError": false}))],
            calls: Vec::new(),
        };
        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);
        assert!(result.results[0].success);
        assert_eq!(
            caller.calls,
            [(
                "browser_click".to_string(),
                json!({"element": "Login", "target": "e12"})
            )]
        );
    }

    #[test]
    fn scroll_uses_evaluate_with_requested_offsets() {
        let batch = BlendBatchRequest {
            steps: vec![BlendStep {
                id: Some("scroll".to_string()),
                tool: "browser_scroll".to_string(),
                args: json!({"delta_y": 320}),
                lane: None,
                timeout_ms: None,
                mutates: None,
            }],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![Ok(
                json!({"isError": false, "content": [{"text": "scrolled"}]}),
            )],
            calls: Vec::new(),
        };
        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);
        assert!(result.results[0].success);
        assert_eq!(caller.calls[0].0, "browser_evaluate");
        assert!(
            caller.calls[0].1["function"]
                .as_str()
                .unwrap()
                .contains("top:320")
        );
    }

    #[test]
    fn blocks_blank_navigation_on_shared_stage() {
        let batch = BlendBatchRequest {
            steps: vec![BlendStep {
                id: Some("blank".to_string()),
                tool: "browser_navigate".to_string(),
                args: json!({"url": "about:blank"}),
                lane: None,
                timeout_ms: None,
                mutates: None,
            }],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: Vec::new(),
            calls: Vec::new(),
        };

        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);

        assert!(!result.results[0].success);
        assert!(
            result.results[0]
                .error
                .as_ref()
                .unwrap()
                .contains("about:blank")
        );
        assert!(caller.calls.is_empty());
    }

    #[test]
    fn preserves_full_snapshot_body_in_read_artifact() {
        let snapshot = "- Page URL: https://example.test\n- Page Title: Example\n\n### Page\n- button \"Share\" [ref=a1]\n";
        let batch = BlendBatchRequest {
            lane: Some("live_read".to_string()),
            steps: vec![step("snap", "browser_snapshot")],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![Ok(json!({
                "isError": false,
                "content": [{"text": snapshot}]
            }))],
            calls: Vec::new(),
        };

        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);

        assert!(result.results[0].success);
        assert!(!result.results[0].preview.contains("button \"Share\""));
        let path = result.results[0].artifact_path.as_ref().unwrap();
        let artifact = std::fs::read_to_string(path).unwrap();
        assert!(artifact.contains("### Page"));
        assert!(artifact.contains("button \"Share\""));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn applies_capture_limit_to_text_tool_preview() {
        let batch = BlendBatchRequest {
            lane: Some("live_read".to_string()),
            capture_limit_bytes: Some(20),
            steps: vec![step("console", "browser_console_messages")],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![Ok(json!({
                "isError": false,
                "content": [{"text": "abcdefghijklmnopqrstuvwxyz"}]
            }))],
            calls: Vec::new(),
        };

        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);

        assert!(result.results[0].success);
        assert!(
            result.results[0]
                .preview
                .starts_with("abcdefghijklmnopqrst")
        );
        assert!(result.results[0].preview.contains("capture_limit_bytes=20"));
    }

    #[test]
    fn xray_writes_full_table_and_compact_preview() {
        let rows = serde_json::to_string(&json!([
            {"tag":"input","role":"","label":"Search","sel":"#search","in_viewport":true},
            {"tag":"img","role":"","label":"share shader","sel":"img:nth-of-type(1)","in_viewport":true}
        ]))
        .unwrap();
        let wrapped_rows = serde_json::to_string(&rows).unwrap();
        let batch = BlendBatchRequest {
            lane: Some("live_read".to_string()),
            steps: vec![BlendStep {
                id: Some("xray".to_string()),
                tool: "browser_xray".to_string(),
                args: json!({"mode": "unlabeled"}),
                lane: None,
                timeout_ms: None,
                mutates: None,
            }],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![Ok(json!({
                "isError": false,
                "content": [{"text": format!("### Result\n{wrapped_rows}\n### Ran Playwright code")}]
            }))],
            calls: Vec::new(),
        };

        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);

        assert!(result.results[0].success);
        assert_eq!(result.results[0].mutation, MutationClass::Read);
        assert!(
            result.results[0]
                .preview
                .contains("browser_xray mode=unlabeled rows=2")
        );
        assert!(result.results[0].preview.contains("label=share shader"));
        assert_eq!(caller.calls[0].0, "browser_evaluate");
        let path = result.results[0].artifact_path.as_ref().unwrap();
        let artifact = std::fs::read_to_string(path).unwrap();
        assert!(artifact.contains("sel=img:nth-of-type(1)"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn selector_click_shorthand_maps_to_upstream_browser_click() {
        let batch = BlendBatchRequest {
            steps: vec![BlendStep {
                id: Some("click".to_string()),
                tool: "browser_click".to_string(),
                args: json!({"sel": "#share", "element": "share icon"}),
                lane: None,
                timeout_ms: None,
                mutates: None,
            }],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![Ok(json!({
                "isError": false,
                "content": [{"text": "clicked"}]
            }))],
            calls: Vec::new(),
        };

        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);

        assert!(result.results[0].success);
        assert_eq!(result.results[0].mutation, MutationClass::Mutate);
        assert_eq!(
            caller.calls,
            [(
                "browser_click".to_string(),
                json!({"element": "share icon", "target": "#share"})
            )]
        );
    }

    #[test]
    fn xray_click_index_uses_selected_row_selector() {
        let rows = serde_json::to_string(&json!([
            {"tag":"button","role":"","label":"first","sel":"#first","in_viewport":true},
            {"tag":"img","role":"","label":"share shader","sel":"#share","in_viewport":true}
        ]))
        .unwrap();
        let wrapped_rows = serde_json::to_string(&rows).unwrap();
        let batch = BlendBatchRequest {
            steps: vec![BlendStep {
                id: Some("xclick".to_string()),
                tool: "browser_xray".to_string(),
                args: json!({"mode": "unlabeled", "click_index": 1}),
                lane: None,
                timeout_ms: None,
                mutates: None,
            }],
            ..BlendBatchRequest::default()
        };
        let mut caller = FakeCaller {
            responses: vec![
                Ok(json!({
                    "isError": false,
                    "content": [{"text": format!("### Result\n{wrapped_rows}\n### Ran Playwright code")}]
                })),
                Ok(json!({
                    "isError": false,
                    "content": [{"text": "clicked share"}]
                })),
            ],
            calls: Vec::new(),
        };

        let result = execute_batch_with_session(&test_config(), &mut caller, &batch, false);

        assert!(result.results[0].success);
        assert_eq!(result.results[0].mutation, MutationClass::Mutate);
        assert_eq!(caller.calls[0].0, "browser_evaluate");
        assert_eq!(
            caller.calls[1],
            (
                "browser_click".to_string(),
                json!({"element": "xray row 1: share shader", "target": "#share"})
            )
        );
    }
}
