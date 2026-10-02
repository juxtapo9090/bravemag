use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};

#[derive(Clone)]
struct Caller {
    barrier: Option<Arc<Barrier>>,
    events: Arc<Mutex<Vec<String>>>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    forkable: bool,
}

impl Caller {
    fn new(width: usize) -> Self {
        Self {
            barrier: (width > 1).then(|| Arc::new(Barrier::new(width))),
            events: Arc::default(),
            active: Arc::default(),
            peak: Arc::default(),
            forkable: true,
        }
    }
}

impl ToolCaller for Caller {
    fn fork_read(&self) -> Option<Box<dyn ToolCaller + Send>> {
        self.forkable
            .then(|| Box::new(self.clone()) as Box<dyn ToolCaller + Send>)
    }

    fn call_tool(&mut self, tool: &str, args: &Value) -> AppResult<Value> {
        let id = args["id"].as_str().unwrap();
        if tool == "dom.query" {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            if let Some(barrier) = &self.barrier {
                barrier.wait();
            }
            if id == "slow" {
                thread::sleep(Duration::from_millis(20));
            }
            self.events.lock().unwrap().push(id.to_string());
            self.active.fetch_sub(1, Ordering::SeqCst);
        } else {
            assert_eq!(
                self.active.load(Ordering::SeqCst),
                0,
                "mutation overlapped a read"
            );
            self.events.lock().unwrap().push(id.to_string());
        }
        if id == "fail" {
            bail!("test read failure");
        }
        Ok(json!({"content": [{"type": "text", "text": id}]}))
    }
}

fn batch(steps: &[(&str, &str)], parallel: Option<usize>) -> BlendBatchRequest {
    BlendBatchRequest {
        max_parallel: parallel,
        step_delay_ms: Some(0),
        steps: steps
            .iter()
            .map(|(tool, id)| BlendStep {
                id: Some((*id).to_string()),
                tool: (*tool).to_string(),
                args: json!({"id": id}),
                lane: Some("live_read".into()),
                timeout_ms: None,
                mutates: Some(false),
            })
            .collect(),
        ..BlendBatchRequest::default()
    }
}

fn config() -> UpstreamRuntimeConfig {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    UpstreamRuntimeConfig {
        endpoint: "http://127.0.0.1:0/mcp".into(),
        caller: "parallel-test".into(),
        audit_path: std::env::temp_dir().join(format!(
            "bravemag-parallel-test-{}-{}.jsonl",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )),
    }
}

#[test]
fn reads_overlap_but_mutations_barrier_and_results_and_audit_stay_ordered() {
    let config = config();
    let mut caller = Caller::new(2);
    let batch = batch(
        &[
            ("dom.query", "slow"),
            ("dom.query", "fast"),
            ("input.click", "click"),
            ("dom.query", "a"),
            ("dom.query", "b"),
        ],
        Some(2),
    );
    let result = execute_batch_with_session(&config, &mut caller, &batch, false);
    assert!(!result.stopped_early);
    assert_eq!(caller.peak.load(Ordering::SeqCst), 2);
    assert_eq!(
        result.results.iter().map(|r| r.index).collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4]
    );
    assert_eq!(result.results[0].preview, "slow");
    assert_eq!(result.mutating_steps, 1);
    let events = caller.events.lock().unwrap();
    assert_eq!(events[0], "fast");
    assert_eq!(events[2], "click");
    let audit = fs::read_to_string(&config.audit_path).unwrap();
    let rows: Vec<Value> = audit
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), 5);
    for (row, step) in rows.iter().zip(&batch.steps) {
        assert_eq!(
            row["args_hash"],
            fnv1a64_hex(&serde_json::to_string(&step.args).unwrap())
        );
    }
    fs::remove_file(config.audit_path).unwrap();
}

#[test]
fn failure_finishes_inflight_group_and_skips_later_steps_unless_continuing() {
    for continue_on_error in [false, true] {
        let config = config();
        let mut caller = Caller::new(3);
        let mut batch = batch(
            &[
                ("dom.query", "slow"),
                ("dom.query", "fail"),
                ("dom.query", "fast"),
                ("navigation.navigate", "navigate"),
            ],
            Some(3),
        );
        batch.continue_on_error = Some(continue_on_error);
        let result = execute_batch_with_session(&config, &mut caller, &batch, false);
        assert_eq!(result.stopped_early, !continue_on_error);
        assert_eq!(result.results.len(), if continue_on_error { 4 } else { 3 });
        assert!(result.results[0].success);
        assert!(!result.results[1].success);
        assert!(result.results[2].success);
        assert_eq!(caller.active.load(Ordering::SeqCst), 0);
        fs::remove_file(config.audit_path).unwrap();
    }
}

#[test]
fn serial_default_and_invalid_parallel_requests() {
    for parallel in [None, Some(1), Some(0), Some(2)] {
        let config = config();
        let mut caller = Caller::new(1);
        caller.forkable = false;
        let batch = batch(&[("dom.query", "first"), ("dom.query", "second")], parallel);
        let result = execute_batch_with_session(&config, &mut caller, &batch, false);
        let invalid = matches!(parallel, Some(0 | 2));
        assert_eq!(result.stopped_early, invalid);
        assert_eq!(
            caller.events.lock().unwrap().len(),
            if invalid { 0 } else { 2 }
        );
        if invalid {
            assert!(
                result.results[0]
                    .error
                    .as_deref()
                    .unwrap()
                    .contains("max_parallel")
            );
        }
        fs::remove_file(config.audit_path).unwrap();
    }
}

#[test]
fn parallel_classification_ignores_unsafe_read_hints() {
    for tool in [
        "input.click",
        "navigation.navigate",
        "navigation.back",
        "unknown.method",
        "browser_evaluate",
    ] {
        let batch = batch(&[(tool, "x")], Some(2));
        assert!(!step_is_read(&batch, &batch.steps[0]), "{tool}");
    }
    let mut batch = batch(&[("dom.query", "x")], Some(2));
    assert!(step_is_read(&batch, &batch.steps[0]));
    batch.steps[0].mutates = Some(true);
    assert!(!step_is_read(&batch, &batch.steps[0]));
    let caller = BbxSession::for_tab(42);
    assert!(caller.fork_read().is_some());
}

#[test]
fn json_and_toon_accept_max_parallel() {
    let json: BlendBatchRequest =
        serde_json::from_value(json!({"max_parallel": 3,"steps": []})).unwrap();
    assert_eq!(json.max_parallel, Some(3));
    let toon =
        crate::toon::parse_script("max_parallel: 3\njob read:\n  tool: dom.query\n").unwrap();
    assert_eq!(toon.max_parallel, Some(3));
}
