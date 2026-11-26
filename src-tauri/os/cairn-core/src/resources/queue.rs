//! Presentation of the runner's single ordered queue.
use crate::execution::queue::{self, Activity, QueueItem};
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueRow {
    #[serde(flatten)]
    pub item: QueueItem,
    pub project_id: Option<String>,
    pub project_key: Option<String>,
    pub issue_number: Option<i64>,
    pub issue_title: Option<String>,
    pub thread_name: Option<String>,
    pub seq: Option<i64>,
    pub state: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueView {
    pub items: Vec<QueueRow>,
    pub global_cap: usize,
    pub thread_cap: usize,
}

/// Metadata and plain states are presentation only; membership, order and
/// admission always come from the queue authority.
pub async fn view(orch: &Orchestrator) -> Result<QueueView, String> {
    let state = queue::read(orch).await?;
    let mut items = Vec::with_capacity(state.items.len());
    for item in state.items {
        let mut row = QueueRow {
            state: plain_state(&item, None),
            item,
            project_id: None,
            project_key: None,
            issue_number: None,
            issue_title: None,
            thread_name: None,
            seq: None,
        };
        for db in orch.db.all_dbs().await {
            let id = row.item.execution_id.clone();
            let metadata = db
                .read(move |conn| {
                    let id = id.clone();
                    Box::pin(async move {
                        let mut rows = conn
                            .query(
                                "SELECT p.id, p.key, i.number, i.title, t.name, e.seq, i.id
                         FROM executions e JOIN issues i ON i.id = e.issue_id
                         JOIN projects p ON p.id = i.project_id
                         LEFT JOIN threads t ON t.id = i.parent_thread_id WHERE e.id = ?1",
                                [id.as_str()],
                            )
                            .await?;
                        let Some(meta) = rows.next().await? else {
                            return Ok(None);
                        };
                        let metadata = (
                            meta.text(0)?,
                            meta.text(1)?,
                            meta.i64(2)?,
                            meta.text(3)?,
                            meta.opt_text(4)?,
                            meta.i64(5)?,
                        );
                        let issue_id = meta.text(6)?;
                        drop(rows);
                        let mut waits = conn
                            .query(
                                "SELECT t.state,
                         CASE WHEN t.yield_reason = 'wait' THEN
                           (SELECT json_extract(aw.condition_json, '$.kind') FROM agent_waits aw
                             WHERE aw.job_id = j.id AND aw.state IN ('pending', 'resolving')
                             ORDER BY aw.created_at DESC LIMIT 1)
                         ELSE t.yield_reason END FROM jobs j
                         LEFT JOIN turns t ON t.id = j.current_turn_id
                         WHERE j.execution_id = ?1 AND j.status IN ('running', 'pending')",
                                [id.as_str()],
                            )
                            .await?;
                        let mut reasons = Vec::new();
                        let mut active = false;
                        while let Some(wait) = waits.next().await? {
                            let state = wait.opt_text(0)?;
                            if state.as_deref() == Some("running") {
                                active = true;
                            }
                            if state.as_deref() == Some("yielded") {
                                if let Some(reason) = wait.opt_text(1)? {
                                    if let Some(reason) = wait_label(&reason) {
                                        reasons.push(reason);
                                    }
                                }
                            }
                        }
                        drop(waits);
                        let mut waiting = if active {
                            None
                        } else {
                            reasons.into_iter().next()
                        };
                        if !active && waiting.is_none() {
                            let mut supply = conn.query(
                                "SELECT 1 FROM provider_limit_holds h JOIN jobs j ON j.id = h.job_id
                                 WHERE j.execution_id = ?1 AND h.state = 'active'
                                   AND h.boundary_source <> 'queue'
                                 LIMIT 1",
                                [id.as_str()],
                            ).await?;
                            if supply.next().await?.is_some() {
                                waiting = Some("provider limit");
                            }
                        }
                        if !active && waiting.is_none() {
                            let scope = crate::execution::approvals::ApprovalScope::Issues(vec![issue_id.clone()]);
                            if !crate::execution::approvals::pending_approvals_conn(conn, &scope).await?.is_empty() {
                                waiting = Some("review");
                            } else {
                                let mut prs = conn.query(
                                    "SELECT checks_status FROM merge_requests WHERE issue_id = ?1 AND status = 'open' ORDER BY opened_at DESC LIMIT 1",
                                    [issue_id.as_str()],
                                ).await?;
                                if prs.next().await?.is_some() {
                                    waiting = Some("review");
                                }
                            }
                        }
                        Ok(Some((metadata, waiting)))
                    })
                })
                .await
                .map_err(|error| error.to_string())?;
            if let Some(((project_id, key, number, title, thread, seq), waiting)) = metadata {
                row.project_id = Some(project_id);
                row.project_key = Some(key);
                row.issue_number = Some(number);
                row.issue_title = Some(title);
                row.thread_name = thread;
                row.seq = Some(seq);
                row.state = plain_state(&row.item, waiting);
                break;
            }
        }
        items.push(row);
    }
    Ok(QueueView {
        items,
        global_cap: state.global_cap,
        thread_cap: state.thread_cap,
    })
}

/// One plain state per row, from what the queue decided and what the entry
/// waits on: seated work runs or waits on something in flight, held work is
/// queued with its place in line, and dormant work says what it waits on.
fn plain_state(item: &QueueItem, waiting: Option<&str>) -> String {
    match (item.parked, item.runs, item.activity, waiting) {
        (true, ..) => "parked".into(),
        (false, true, Activity::Active, Some(waiting)) => format!("waiting on {waiting}"),
        (false, true, _, _) => "running".into(),
        (false, false, Activity::Wanting, _) => item.queued_label().to_lowercase(),
        (false, false, _, Some(waiting)) => format!("waiting on {waiting}"),
        (false, false, _, None) => "idle".into(),
    }
}

fn wait_label(reason: &str) -> Option<&'static str> {
    match reason {
        "queue" | "run_queue" => None,
        "user_input" | "permission" | "safety_pause" => Some("person"),
        "provider_limit" | "supply" => Some("provider limit"),
        "checks" | "check" => Some("check"),
        "review" | "artifact" => Some("review"),
        "dependencies" | "dependency_wait" => Some("task work"),
        "duration" => Some("timer"),
        "terminal" | "run_batch" | "mcp_continuation" => None,
        _ => None,
    }
}

pub(super) async fn read_project_queue(orch: &Orchestrator, _project: &str) -> String {
    match view(orch).await {
        Ok(view) => render(&view),
        Err(error) => format!("# Queue\n\nCould not read the queue: {error}"),
    }
}

pub(super) fn render(view: &QueueView) -> String {
    let running = view.items.iter().filter(|row| row.item.runs).count();
    let queued = view
        .items
        .iter()
        .filter(|row| !row.item.runs && !row.item.parked && row.item.activity == Activity::Wanting)
        .count();
    let mut out = format!(
        "# Queue\n\nRunning {running} of {}; per thread {}; {queued} queued\n\n",
        view.global_cap, view.thread_cap
    );
    for (index, row) in view
        .items
        .iter()
        .enumerate()
        .filter(|(_, row)| !row.item.parked)
    {
        out.push_str(&format!(
            "{}. {} · {} · {} · execution {}\n",
            row.item.position.unwrap_or(index + 1),
            issue_label(row),
            row.thread_name.as_deref().unwrap_or("no thread"),
            row.state,
            row.item.execution_id
        ));
    }
    out.push_str("\n## Parked\n\n");
    for row in view.items.iter().filter(|row| row.item.parked) {
        out.push_str(&format!(
            "- {} · {} · parked · execution {}\n",
            issue_label(row),
            row.thread_name.as_deref().unwrap_or("no thread"),
            row.item.execution_id
        ));
    }
    out
}

fn issue_label(row: &QueueRow) -> String {
    match (&row.project_key, row.issue_number) {
        (Some(key), Some(number)) => {
            let key = cairn_uri::canonical_project(key);
            format!(
                "[{key}/{number} — {}](cairn://p/{key}/{number})",
                row.issue_title.as_deref().unwrap_or("")
            )
        }
        _ => format!("issue {}", row.item.issue_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, position: Option<usize>, runs: bool, activity: Activity) -> QueueRow {
        let item = QueueItem {
            execution_id: id.into(),
            issue_id: format!("issue-{id}"),
            thread_id: Some("t".into()),
            position,
            runs,
            parked: position.is_none(),
            activity,
            in_line: (!runs && activity == Activity::Wanting).then_some(1),
        };
        QueueRow {
            state: plain_state(&item, (activity == Activity::Dormant).then_some("review")),
            item,
            project_id: Some("p".into()),
            project_key: Some("prj".into()),
            issue_number: Some(1),
            issue_title: Some("Issue".into()),
            thread_name: Some("alpha".into()),
            seq: Some(1),
        }
    }

    #[test]
    fn wait_states_are_plain_labels_not_queue_mechanisms() {
        for (reason, label) in [
            ("provider_limit", Some("provider limit")),
            ("checks", Some("check")),
            ("user_input", Some("person")),
            ("dependency_wait", Some("task work")),
            ("run_queue", None),
            ("run_batch", None),
            ("mcp_continuation", None),
        ] {
            assert_eq!(wait_label(reason), label);
        }
    }

    #[test]
    fn presentation_keeps_authority_order_and_says_what_each_entry_is_doing() {
        let view = QueueView {
            items: vec![
                row("a", Some(1), false, Activity::Dormant),
                row("b", Some(2), false, Activity::Wanting),
                row("c", Some(3), true, Activity::Active),
                row("d", Some(4), true, Activity::Wanting),
                row("e", None, false, Activity::Dormant),
            ],
            global_cap: 2,
            thread_cap: 1,
        };
        let text = render(&view);
        assert!(text.contains("Running 2 of 2; per thread 1; 1 queued"));
        assert!(text.contains("1. [prj/1 — Issue](cairn://p/prj/1) · alpha · waiting on review"));
        assert!(text.contains("queued · 1st in line · execution b"));
        assert!(text.contains("running · execution c"));
        assert!(text.find("execution a").unwrap() < text.find("execution b").unwrap());
        assert!(text.find("## Parked").unwrap() < text.find("execution e").unwrap());
        for old in [
            "running line",
            "focus slot",
            "fleet slot",
            "re-entry ahead",
            "Start now",
        ] {
            assert!(!text.contains(old));
        }
    }
}
