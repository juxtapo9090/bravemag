use crate::toon::ToolRequest;

#[must_use]
pub fn render_batch_preview(request: &ToolRequest) -> String {
    let Some(batch) = request.batch.as_ref() else {
        return "[bravemag] empty request".to_string();
    };

    let mut lines = Vec::new();
    lines.push(format!(
        "[bravemag] steps={} mode={} lane={} delay={}ms continue_on_error={} capture_after={}",
        batch.steps.len(),
        batch.mode.as_deref().unwrap_or("drip"),
        batch.lane.as_deref().unwrap_or("live_mutate"),
        batch.step_delay_ms.unwrap_or(100),
        batch.continue_on_error.unwrap_or(false),
        batch.capture_after.unwrap_or(false)
    ));

    for (index, step) in batch.steps.iter().enumerate() {
        lines.push(format!(
            "[bravemag #{}] id={} tool={} lane={} mutates={}",
            index + 1,
            step.id.as_deref().unwrap_or("-"),
            step.tool,
            step.lane
                .as_deref()
                .or(batch.lane.as_deref())
                .unwrap_or("live_mutate"),
            step.mutates.unwrap_or(true)
        ));
    }

    lines.join("\n")
}
