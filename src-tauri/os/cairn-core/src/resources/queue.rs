//! `cairn://p/<project>/queue`: the fleet's execution queue, read from a
//! project. The list is the workspace's, the same one Settings → Queue shows
//! (`capacity::queue_view`), because the fleet cap spans every database; the
//! project only says where an issue number in a write resolves. Its patch is
//! `mutations::dispatch::queue`.

use crate::models::{PinBy, PinKind};
use crate::orchestrator::Orchestrator;

pub(super) async fn read_project_queue(orch: &Orchestrator, project: &str) -> String {
    match crate::execution::gates::capacity::queue_view(orch).await {
        Ok(view) => render(project, &view, orch.services.clock.now()),
        Err(error) => format!("# Execution queue\n\nCould not read the queue: {error}"),
    }
}

/// `placed by judgment`, from the first pinned hold of an execution's census.
fn pin_phrase(queued: &crate::execution::gates::capacity::QueuedExecution) -> Option<String> {
    let pin = queued
        .census
        .as_ref()?
        .gates
        .iter()
        .find_map(|gate| gate.hold.as_ref().and_then(|hold| hold.pin()))?;
    let by = match &pin.by {
        PinBy::Operator => "the operator".to_string(),
        PinBy::Thread { thread } => thread.clone(),
    };
    Some(match pin.kind {
        PinKind::Placed => format!("placed by {by}"),
        PinKind::Frozen => format!("held at its place ahead of {by}'s move"),
    })
}

fn render(project: &str, view: &crate::execution::gates::capacity::QueueView, now: i64) -> String {
    let project = cairn_common::uri::canonical_project(project);
    let mut out = format!(
        "# Execution queue\n\nEvery execution the caps hold, across every project and thread in \
         the workspace, in the order the fleet releases them. Issue numbers in a write resolve \
         in {project}.\n\nFleet: {}/{} running",
        view.in_progress, view.limit
    );
    if view.waiting_for_machine > 0 {
        out.push_str(&format!(
            " ({} waiting for a machine)",
            view.waiting_for_machine
        ));
    }
    out.push_str(&format!(" · {} queued.\n\n", view.queued));
    if view.held.is_empty() {
        out.push_str("Nothing is queued.\n");
        return out;
    }
    for (index, queued) in view.held.iter().enumerate() {
        let mut parts = Vec::new();
        parts.push(match (&queued.project_key, queued.issue_number) {
            (Some(key), Some(number)) => {
                let key = cairn_common::uri::canonical_project(key);
                format!("[{key}/{number}](cairn://p/{key}/{number})")
            }
            _ => format!("execution {}", queued.execution_id),
        });
        parts.push(
            queued
                .thread_name
                .clone()
                .unwrap_or_else(|| "no thread".to_string()),
        );
        if let Some(title) = &queued.issue_title {
            parts.push(title.clone());
        }
        if let Some(census) = &queued.census {
            parts.push(census.summary.clone());
        }
        parts.push(format!(
            "held {}",
            crate::clock::format_elapsed((now - queued.held_since).max(0))
        ));
        if let Some(pin) = pin_phrase(queued) {
            parts.push(pin);
        }
        out.push_str(&format!("{}. {}\n", index + 1, parts.join(" · ")));
    }
    out
}
