//! Attention delivery engine (push-queue model, docs/attention-redesign.md).
//!
//! Creates and renders attention pushes. Two responsibilities:
//!
//! 1. **Create pushes.** [`create_resolved_push`] turns a terminal `Resolved`
//!    fact into a passive `resolved:{issue}` push to the issue's watchers;
//!    [`create_catchup_push`] creates the passive `catchup:{child-job}` push at
//!    the user→child message moment, resolved at delivery against the parent's
//!    read cursor. Question and permission pushes are created at their own emit
//!    sites (where the producing node is known) through
//!    [`push_to_issue_watchers`].
//! 2. **Render pushes at resume time.** [`render_pushes_resolved`] resolves most
//!    drained push `content_ref`s to rendered resource content so a resumed agent
//!    acts without a round-trip read. Terminal `resolved:` pushes are rendered as
//!    concise confirmations instead of dumping the resolved issue body.

use turso::params;

use super::attention_push::{Boundary, Wake};
use super::Orchestrator;
use crate::orchestrator::{AttentionEvent, AttentionFact};
use crate::storage::{run_db_blocking, LocalDb, RowExt};

/// Create the passive `resolved:{issue}` push at a terminal-resolution emit.
///
/// The single creator of the resolved push, fed by every
/// `AttentionFact::Resolved` emit (the recompute terminal sweep, the work-turn
/// idle edge, the PR webhook, and `wake_for_issue`) through the
/// `emit_attention_event` funnel. Non-`Resolved` facts are ignored — question
/// and permission pushes are created at their own emit sites where the producing
/// node is known. Informational: passive, never wakes; supersede-by-key
/// collapses repeat emits to one undelivered row. Fire-and-forget.
pub fn create_resolved_push(orch: &Orchestrator, event: &AttentionEvent) {
    if !matches!(event.fact, AttentionFact::Resolved { .. }) {
        return;
    }
    let dbs = orch.db.clone();
    let issue_id = event.issue_id.clone();
    let issue_uri = event.issue_uri.clone();
    let result = run_db_blocking(move || async move {
        let db = crate::issues::crud::owning_db_for_issue(&dbs, &issue_id)
            .await
            .map_err(|e| e.to_string())?;
        let key = format!("resolved:{issue_uri}");
        // Resolved is issue-wide; there is no single producing node to exclude.
        push_to_issue_watchers(
            &db,
            &issue_uri,
            None,
            &issue_uri,
            Wake::Passive,
            Boundary::Event,
            &key,
        )
        .await
    });
    match result {
        Ok(_) => orch.notifier.emit_change("attention_pushes"),
        Err(e) => log::warn!("resolved push creation failed: {}", e),
    }
}

/// Push to every watcher of `issue_uri`, optionally excluding the producing
/// node. The shared creator for the question / permission / resolved push
/// sources (the review push has its own creator at the work-turn idle edge).
/// Supersede-by-key collapses repeats to one undelivered row per recipient; the
/// delivery layer drains, lazy-resolves, and stamps each push.
///
/// Returns the recipients that received a push, so a `wake`/`interrupt` caller
/// can wake each through `delivery::nudge_job_for_urgency` (CAIRN-1889). Passive
/// callers ignore the list.
pub(crate) async fn push_to_issue_watchers(
    db: &LocalDb,
    issue_uri: &str,
    exclude_job: Option<&str>,
    content_ref: &str,
    wake: Wake,
    boundary: Boundary,
    key: &str,
) -> Result<Vec<String>, String> {
    let watchers = subscriber_jobs_for_issue(db, issue_uri).await?;
    let mut pushed = Vec::new();
    for recipient in watchers {
        if Some(recipient.as_str()) == exclude_job {
            continue;
        }
        let (_, effective) =
            super::attention_push::push(db, &recipient, content_ref, wake, boundary, key)
                .await
                .map_err(|e| e.to_string())?;
        // Only hand rousing recipients back for nudging. A recipient that muted
        // this source gets a `Passive` ride-along row (created by the central
        // downgrade in `push`) and must NOT be woken (CAIRN-1900).
        if effective.wakes_idle() {
            pushed.push(recipient);
        }
    }
    Ok(pushed)
}

/// Distinct job ids with an active or muted issue subscription for `issue_uri`.
pub(crate) async fn subscriber_jobs_for_issue(
    db: &LocalDb,
    issue_uri: &str,
) -> Result<Vec<String>, String> {
    let issue_uri = issue_uri.to_string();
    db.read(|conn| {
        let issue_uri = issue_uri.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT DISTINCT job_id FROM wake_subscriptions
                     WHERE source_kind='issue' AND source_ref=?1 AND state != 'unsubscribed'",
                    params![issue_uri.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(row.text(0)?);
            }
            Ok(out)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Create the passive `catchup:{child-job}` push for the watching parent at the
/// user→child message moment (CAIRN-1894). Single trigger, definite recipient
/// (the watching parent), definite source (the addressed child node or sub-task's
/// chat).
///
/// The cursor is keyed by the child JOB id whose `{node|task}/chat` this renders,
/// so it counts exactly the transcript the parent is shown (one job's runs)
/// rather than the whole issue's sibling jobs and sub-task runs. The window's
/// `content_ref` is `{child_uri}/chat?offset={start}` with no end bound:
/// [`render_push_resolved`] reads it AT DELIVERY, so the rendered window spans
/// from `start` through whatever that job has accrued by the time the parent next
/// runs — turns in that gap are included for free, with no turn-end bump. `start`
/// is the parent's read cursor when it has looked before, else one turn of
/// lead-in. Because the cursor only advances on delivery, a second message before
/// delivery reuses the same start. Passive: it never wakes the idle parent; it
/// rides along on the parent's next run.
pub async fn create_catchup_push(
    db: &LocalDb,
    parent_job_id: &str,
    child_uri: &str,
) -> Result<(), String> {
    let Some(child_job_id) = job_id_for_child_uri(db, child_uri).await else {
        // The URI did not resolve to an agent job (a stale or not-yet-persisted
        // node/task, or a non-node/task URI); skip rather than mis-scope the
        // cursor.
        log::warn!("catch-up push: no job for {child_uri}");
        return Ok(());
    };
    let tail = count_job_chat_turns(db, &child_job_id).await;
    let start = match super::attention_push::read_cursor(db, parent_job_id, &child_job_id)
        .await
        .map_err(|e| e.to_string())?
    {
        Some(cursor) => cursor,
        None => (tail - 1).max(0),
    };
    let content_ref = format!("{child_uri}/chat?offset={start}");
    let key = format!("catchup:{child_job_id}");
    super::attention_push::push(
        db,
        parent_job_id,
        &content_ref,
        Wake::Passive,
        Boundary::Event,
        &key,
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// Resolve the agent job whose `{node|task}/chat` a catch-up push renders, from
/// the addressed child URI — the same job the chat resource resolves. A node URI
/// maps to its top-level job (issue + execution seq + node `uri_segment`); a task
/// URI maps to the addressed sub-task job (the task `uri_segment` under that node
/// job), so a user message directed at a sub-task still scopes catch-up to the
/// task's own chat. `None` for any other URI or an unresolved node/task.
async fn job_id_for_child_uri(db: &LocalDb, child_uri: &str) -> Option<String> {
    let (project, number, exec_seq, node_id, task_name) =
        match cairn_common::uri::parse_uri(child_uri)? {
            cairn_common::uri::CairnResource::Node {
                project,
                number,
                exec_seq,
                node_id,
            } => (project, number, exec_seq, node_id, None),
            cairn_common::uri::CairnResource::Task {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => (project, number, exec_seq, node_id, Some(task_name)),
            _ => return None,
        };
    let project = project.to_uppercase();
    let number = number as i64;
    let exec_seq = exec_seq as i64;
    db.read(|conn| {
        let project = project.clone();
        let node_id = node_id.clone();
        let task_name = task_name.clone();
        Box::pin(async move {
            // The top-level node job (the rendered job for a node URI, or the
            // task's parent for a task URI).
            let mut rows = conn
                .query(
                    "SELECT j.id FROM jobs j
                     JOIN executions e ON j.execution_id = e.id
                     JOIN issues i ON j.issue_id = i.id
                     JOIN projects p ON i.project_id = p.id
                     WHERE p.key=?1 AND i.number=?2 AND e.seq=?3
                       AND j.uri_segment=?4 AND j.parent_job_id IS NULL
                     LIMIT 1",
                    params![project.as_str(), number, exec_seq, node_id.as_str()],
                )
                .await?;
            let node_job_id = match rows.next().await? {
                Some(row) => row.text(0)?,
                None => return Ok::<Option<String>, crate::storage::DbError>(None),
            };
            // A node URI renders the node job itself; a task URI renders the
            // addressed sub-task job under it.
            let Some(task_name) = task_name else {
                return Ok(Some(node_job_id));
            };
            let mut task_rows = conn
                .query(
                    "SELECT id FROM jobs WHERE parent_job_id=?1 AND uri_segment=?2 LIMIT 1",
                    params![node_job_id.as_str(), task_name.as_str()],
                )
                .await?;
            match task_rows.next().await? {
                Some(row) => Ok(Some(row.text(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
    .ok()
    .flatten()
}

/// Distinct turns recorded across one job's runs — the job-scoped chat tail that
/// `{node}/chat` renders. Catch-up's window start when the parent has no prior
/// read cursor.
async fn count_job_chat_turns(db: &LocalDb, job_id: &str) -> i64 {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(DISTINCT e.turn_id) FROM events e
                     JOIN runs r ON e.run_id = r.id
                     WHERE r.job_id = ?1 AND e.turn_id IS NOT NULL",
                    params![job_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(row.i64(0)?),
                None => Ok(0),
            }
        })
    })
    .await
    .unwrap_or(0)
}

/// Resolve a single `cairn://` (or file/web) URI to rendered markdown via the
/// same `read_batch` path that backs `cairn read [uri]`. `run_id` is `None` so
/// the briefing never pollutes the agent's read-dedup state. Returns `None` on a
/// resolution failure (e.g. a fence suspension string, which is not an
/// envelope) or an empty body, so the caller can fall back to a bare pointer.
async fn resolve_uri_to_markdown(orch: &Orchestrator, uri: &str) -> Option<String> {
    let request = crate::mcp::types::McpCallbackRequest {
        cwd: String::new(),
        run_id: None,
        tool: "read_batch".to_string(),
        payload: serde_json::json!({ "paths": [uri] }),
        tool_use_id: None,
    };
    let cursors = std::sync::Mutex::new(std::collections::HashMap::new());
    let raw = crate::mcp::handlers::read::handle_read_batch(orch, &request, &cursors).await;
    let envelope: cairn_common::read::ReadBatchEnvelope = serde_json::from_str(&raw).ok()?;
    let text = envelope.text.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

/// Max characters of resolved push content inlined into a reminder/prompt
/// (CAIRN-1891). A full PR diff can be huge; cap to a useful rendered view and
/// keep the `content_ref` URI so the agent can follow it for the rest.
const PUSH_CONTENT_CAP: usize = 4000;

async fn resolved_issue_confirmation(orch: &Orchestrator, issue_uri: &str) -> Option<String> {
    let issue_uri = issue_uri.to_string();
    orch.db
        .local
        .read(|conn| {
            let issue_uri = issue_uri.clone();
            Box::pin(async move {
                let issue = crate::issues::relations::resolve_issue_uri(conn, &issue_uri).await?;
                let status = issue.map(|issue| issue.status);
                let message = match status {
                    Some(crate::models::IssueStatus::Merged) => "Issue Merged Successfully",
                    Some(crate::models::IssueStatus::Closed) => "Issue Closed Successfully",
                    _ => "Issue Resolved Successfully",
                };
                Ok(message.to_string())
            })
        })
        .await
        .ok()
}

/// Render a drained attention push with its referent content resolved inline
/// (CAIRN-1891), so the agent acts without a round-trip read. The header carries
/// the wake level and the `content_ref` URI; the body is the rendered resource
/// (the PR summary/diff, plan, question, or permission), capped. Resolution uses
/// the same in-process read that backs `cairn read {uri}` (and the briefing).
/// Terminal `resolved:` pushes are intentionally concise confirmations instead
/// of a full issue read. Falls back to the bare header line when resolution
/// yields nothing, so the agent still has the URI to follow.
pub async fn render_push_resolved(
    orch: &Orchestrator,
    push: &crate::orchestrator::attention_push::Push,
) -> String {
    let header = format!(
        "Attention update ({}): {}",
        push.wake.as_str(),
        push.content_ref
    );
    if push.key.starts_with("resolved:") {
        let body = resolved_issue_confirmation(orch, &push.content_ref)
            .await
            .unwrap_or_else(|| "Issue Resolved Successfully".to_string());
        return format!("{header}\n\n{body}");
    }

    // A `direct:` push carries frozen message content, not an idempotent
    // resolvable referent. Resolve it from the durable `messages` row by the
    // message id in the key (`direct:{message_id}`) rather than from
    // `content_ref` (which is the conversation surface the wake card links to).
    // Falls back to the header line if the row is missing (CAIRN-1900).
    if let Some(message_id) = push.key.strip_prefix("direct:") {
        return match crate::messages::db::get_message_by_id_async(&orch.db.local, message_id).await
        {
            Ok(Some(msg)) => {
                let body = crate::messages::render::render_direct_message(&msg);
                format!("{header}\n\n{body}")
            }
            _ => header,
        };
    }
    match resolve_uri_to_markdown(orch, &push.content_ref).await {
        Some(body) => {
            let capped = cap_push_content(&body, &push.content_ref);
            format!("{header}\n\n{capped}")
        }
        None => header,
    }
}

/// Resolve and render several pushes into one block (CAIRN-1891), or `None` when
/// the slice is empty so callers can fold it into an optional prompt section.
pub async fn render_pushes_resolved(
    orch: &Orchestrator,
    pushes: &[crate::orchestrator::attention_push::Push],
) -> Option<String> {
    if pushes.is_empty() {
        return None;
    }
    let mut blocks = Vec::with_capacity(pushes.len());
    for push in pushes {
        blocks.push(render_push_resolved(orch, push).await);
    }
    Some(blocks.join("\n\n"))
}

/// Cap resolved push content to [`PUSH_CONTENT_CAP`] characters on a char
/// boundary, appending a pointer to the full resource when truncated.
fn cap_push_content(body: &str, uri: &str) -> String {
    if body.chars().count() <= PUSH_CONTENT_CAP {
        return body.to_string();
    }
    let truncated: String = body.chars().take(PUSH_CONTENT_CAP).collect();
    format!("{truncated}\n\n… [truncated — read {uri} for the full content]")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalDb;

    const CHILD_URI: &str = "cairn://p/PROJ/2";

    #[test]
    fn cap_push_content_passes_short_content_through() {
        let body = "a short rendered body";
        assert_eq!(cap_push_content(body, "cairn://p/PROJ/2"), body);
    }

    #[test]
    fn cap_push_content_truncates_and_points_to_uri() {
        let body = "x".repeat(PUSH_CONTENT_CAP + 500);
        let uri = "cairn://p/PROJ/2/1/builder/pr";
        let capped = cap_push_content(&body, uri);
        assert!(capped.chars().count() < body.chars().count());
        assert!(capped.contains("truncated"));
        assert!(
            capped.contains(uri),
            "truncation must keep a pointer to the full resource"
        );
    }

    #[test]
    fn cap_push_content_respects_char_boundaries() {
        // A multi-byte char at the cap boundary must not panic the truncation.
        let body = "\u{1f600}".repeat(PUSH_CONTENT_CAP + 10);
        let capped = cap_push_content(&body, "cairn://p/PROJ/2");
        assert!(capped.contains("truncated"));
    }

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("attention-delivery.db").await
    }

    /// Parent issue + watcher job, child issue-1 with a child job + run, and a
    /// watcher subscription to the child issue.
    async fn seed(
        db: &LocalDb,
        sub_state: &str,
        fact_kinds_json: Option<&str>,
        until_kind: Option<&str>,
    ) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','PROJ','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('parent','p',1,'Parent','active','active','none',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('issue-1','p',2,'Child','active','active','none',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
              VALUES('watcher','p','parent','running','sess',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('exec-1','r','issue-1','p','running',1,1);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, uri_segment, status, current_session_id, created_at, updated_at)
              VALUES('child-job','p','issue-1','exec-1','builder','running','sess2',1,1);
            INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
              VALUES('run-1','p','child-job','issue-1',1,1);
            ",
        )
        .await
        .unwrap();
        let sub_state = sub_state.to_string();
        let fact_kinds_json = fact_kinds_json.map(str::to_string);
        let until_kind = until_kind.map(str::to_string);
        db.write(move |conn| {
            let sub_state = sub_state.clone();
            let fact_kinds_json = fact_kinds_json.clone();
            let until_kind = until_kind.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO wake_subscriptions
                       (id, job_id, source_kind, source_ref, fact_kinds_json, state,
                        mute_until_kind, mute_until_ref, created_by, created_at, updated_at, one_shot)
                     VALUES('sub-1','watcher','issue',?1,?2,?3,?4,NULL,'agent',1,1,0)",
                    params![CHILD_URI, fact_kinds_json.as_deref(), sub_state.as_str(), until_kind.as_deref()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    // ---- Push creators (CAIRN-1887) -----------------------------------------

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;
        use std::sync::Arc;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir).build()
    }

    #[tokio::test]
    async fn render_resolved_push_uses_concise_confirmation_not_issue_body() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        db.execute_script(
            "UPDATE issues
             SET status='merged', description='This long child issue description should not be inlined.'
             WHERE id='issue-1';",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);
        let push = crate::orchestrator::attention_push::Push {
            id: "push-1".into(),
            recipient: "watcher".into(),
            content_ref: CHILD_URI.into(),
            wake: Wake::Passive,
            boundary: Boundary::Event,
            key: format!("resolved:{CHILD_URI}"),
            created_at: 1,
            delivered_event_id: None,
        };

        let rendered = render_push_resolved(&orch, &push).await;

        assert!(rendered.contains("Attention update (passive): cairn://p/PROJ/2"));
        assert!(rendered.contains("Issue Merged Successfully"));
        assert!(!rendered.contains("Description"));
        assert!(!rendered.contains("This long child issue description"));
    }

    /// Subscribe `job_id` to the child issue (`CHILD_URI`).
    async fn add_issue_sub(db: &LocalDb, job_id: &str) {
        let job_id = job_id.to_string();
        db.write(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO wake_subscriptions
                       (id, job_id, source_kind, source_ref, state, created_by, created_at, updated_at, one_shot)
                     VALUES(?1, ?2, 'issue', ?3, 'active', 'agent', 1, 1, 0)",
                    params![format!("sub-{job_id}"), job_id.as_str(), CHILD_URI],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn push_to_issue_watchers_excludes_producing_node() {
        use crate::orchestrator::attention_push::list_pending;
        let db = migrated_db().await;
        // 'watcher' is subscribed to the child issue; subscribe the producing
        // node 'child-job' to the same issue so the exclusion is exercised.
        seed(&db, "active", None, None).await;
        add_issue_sub(&db, "child-job").await;

        push_to_issue_watchers(
            &db,
            CHILD_URI,
            Some("child-job"),
            "cairn://p/PROJ/2/1/planner/questions/q-1",
            Wake::Wake,
            Boundary::Event,
            "question:cairn://p/PROJ/2",
        )
        .await
        .unwrap();

        let watcher = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(watcher.len(), 1);
        assert_eq!(watcher[0].wake, Wake::Wake);
        assert_eq!(watcher[0].boundary, Boundary::Event);
        assert_eq!(watcher[0].key, "question:cairn://p/PROJ/2");
        assert_eq!(
            watcher[0].content_ref,
            "cairn://p/PROJ/2/1/planner/questions/q-1"
        );
        // The producing node never receives its own push.
        assert!(list_pending(&db, "child-job").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_resolved_push_pushes_passive_resolved_to_watcher() {
        use crate::models::{IssueAttention, IssueStatus};
        use crate::orchestrator::attention::{AttentionEvent, AttentionFact};
        use crate::orchestrator::attention_push::list_pending;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        let orch = test_orchestrator(db);

        super::create_resolved_push(
            &orch,
            &AttentionEvent {
                issue_id: "issue-1".into(),
                issue_uri: CHILD_URI.into(),
                fact: AttentionFact::Resolved {
                    final_status: IssueStatus::Merged,
                },
                attention: IssueAttention::None,
                status: IssueStatus::Merged,
                updated_at: 1,
            },
        );

        let watcher = list_pending(&orch.db.local, "watcher").await.unwrap();
        assert_eq!(watcher.len(), 1);
        // Resolved is informational: passive, rides along, never wakes.
        assert_eq!(watcher[0].wake, Wake::Passive);
        assert_eq!(watcher[0].key, format!("resolved:{CHILD_URI}"));
        assert_eq!(watcher[0].content_ref, CHILD_URI);
    }

    // ---- Catch-up push creator (CAIRN-1894) ----------------------------------

    /// Insert a chat event carrying `turn_id` on the child issue's run so
    /// `child_chat_turn_count` sees a distinct turn.
    async fn add_chat_turn(db: &LocalDb, turn_id: &str, seq: i64) {
        let turn_id = turn_id.to_string();
        db.write(move |conn| {
            let turn_id = turn_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO turns(id, session_id, run_id, sequence, state, created_at, updated_at)
                     VALUES(?1,'sess2','run-1',?2,'completed',1,1)",
                    params![turn_id.as_str(), seq],
                )
                .await?;
                conn.execute(
                    "INSERT INTO events(id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
                     VALUES(?1,'run-1',?2,?3,1,'assistant','{}',1)",
                    params![format!("ev-{turn_id}"), turn_id.as_str(), seq],
                )
                .await?;
                Ok::<(), crate::storage::DbError>(())
            })
        })
        .await
        .unwrap();
    }

    /// Undelivered catch-up pushes for a recipient.
    async fn pending_catchup(
        db: &LocalDb,
        recipient: &str,
    ) -> Vec<crate::orchestrator::attention_push::Push> {
        crate::orchestrator::attention_push::list_pending(db, recipient)
            .await
            .unwrap()
            .into_iter()
            .filter(|p| p.key.starts_with("catchup:"))
            .collect()
    }

    /// Insert a carrying event, stamp the pushes delivered, and advance their
    /// read cursors in one transaction — the real delivery seam.
    async fn deliver(db: &LocalDb, push_ids: &[String], event_id: &str, seq: i64) {
        let push_ids = push_ids.to_vec();
        let event_id = event_id.to_string();
        db.write(move |conn| {
            let push_ids = push_ids.clone();
            let event_id = event_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                     VALUES(?1,'run-1',?2,1,'system:message','{}',1)",
                    params![event_id.as_str(), seq],
                )
                .await?;
                crate::orchestrator::attention_push::stamp_delivered_conn(
                    conn, &push_ids, &event_id,
                )
                .await?;
                crate::orchestrator::attention_push::advance_read_cursors_conn(conn, &push_ids)
                    .await?;
                Ok::<(), crate::storage::DbError>(())
            })
        })
        .await
        .unwrap();
    }

    fn child_node_uri() -> String {
        format!("{CHILD_URI}/1/builder")
    }

    #[tokio::test]
    async fn child_turns_without_a_message_create_no_catchup() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        // The child does autonomous work — turns accrue — but no user→child
        // message fires. Catch-up is gated on the message moment, so none appears.
        add_chat_turn(&db, "t1", 1).await;
        add_chat_turn(&db, "t2", 2).await;
        assert!(
            pending_catchup(&db, "watcher").await.is_empty(),
            "autonomous child turns must not generate catch-up"
        );
    }

    #[tokio::test]
    async fn user_child_message_creates_one_passive_catchup_push() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await; // tail = 1
        create_catchup_push(&db, "watcher", &child_node_uri())
            .await
            .unwrap();
        let pushes = pending_catchup(&db, "watcher").await;
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].wake, Wake::Passive);
        assert_eq!(pushes[0].key, "catchup:child-job");
        // No prior cursor -> one turn of lead-in (tail - 1 = 0).
        assert!(pushes[0].content_ref.ends_with("/chat?offset=0"));
        assert!(
            !crate::orchestrator::attention_push::has_pending_waking_live(&db, "watcher")
                .await
                .unwrap(),
            "a passive catch-up push never wakes an idle parent"
        );
    }

    #[tokio::test]
    async fn delivered_window_spans_to_current_tail_and_advances_cursor() {
        use crate::orchestrator::attention_push::read_cursor;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await; // message moment: T0 = 1
        create_catchup_push(&db, "watcher", &child_node_uri())
            .await
            .unwrap();
        // The child works on before the parent resumes: T1 = 3.
        add_chat_turn(&db, "t2", 2).await;
        add_chat_turn(&db, "t3", 3).await;
        let pushes = pending_catchup(&db, "watcher").await;
        assert_eq!(pushes.len(), 1);
        // Window start frozen at creation (T0 - 1 = 0); the end is open and read
        // at delivery, so the gap turns are included for free.
        assert!(pushes[0].content_ref.ends_with("/chat?offset=0"));
        deliver(&db, std::slice::from_ref(&pushes[0].id), "carry-1", 100).await;
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            Some(3),
            "cursor advances to the child's tail at delivery"
        );
    }

    #[tokio::test]
    async fn second_message_after_delivery_opens_fresh_window_from_cursor() {
        use crate::orchestrator::attention_push::read_cursor;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        add_chat_turn(&db, "t2", 2).await;
        add_chat_turn(&db, "t3", 3).await; // T1 = 3
        create_catchup_push(&db, "watcher", &child_node_uri())
            .await
            .unwrap();
        let first = pending_catchup(&db, "watcher").await;
        deliver(&db, std::slice::from_ref(&first[0].id), "carry-1", 100).await;
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            Some(3)
        );

        // More child turns, then a SECOND user→child message.
        add_chat_turn(&db, "t4", 4).await;
        add_chat_turn(&db, "t5", 5).await; // tail = 5
        create_catchup_push(&db, "watcher", &child_node_uri())
            .await
            .unwrap();
        let second = pending_catchup(&db, "watcher").await;
        assert_eq!(
            second.len(),
            1,
            "the delivered row left the queue; a fresh undelivered row opens"
        );
        assert!(
            second[0].content_ref.ends_with("/chat?offset=3"),
            "fresh window starts at the advanced cursor (3), not the new tail"
        );
    }

    #[tokio::test]
    async fn rolled_back_delivery_leaves_cursor_and_redelivers() {
        use crate::orchestrator::attention_push::read_cursor;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        add_chat_turn(&db, "t2", 2).await;
        create_catchup_push(&db, "watcher", &child_node_uri())
            .await
            .unwrap();
        let id = pending_catchup(&db, "watcher").await[0].id.clone();

        // Deliver, then force the carrying transaction to roll back.
        let res = db
            .write(move |conn| {
                let id = id.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                         VALUES('carry-x','run-1',100,1,'system:message','{}',1)",
                        (),
                    )
                    .await?;
                    crate::orchestrator::attention_push::stamp_delivered_conn(
                        conn,
                        std::slice::from_ref(&id),
                        "carry-x",
                    )
                    .await?;
                    crate::orchestrator::attention_push::advance_read_cursors_conn(
                        conn,
                        std::slice::from_ref(&id),
                    )
                    .await?;
                    Err::<(), crate::storage::DbError>(crate::storage::DbError::Row(
                        "forced rollback".into(),
                    ))
                })
            })
            .await;
        assert!(res.is_err());

        // Event, stamp, and cursor advance roll back together: catch-up redelivers
        // against the OLD (absent) cursor.
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            None
        );
        assert_eq!(pending_catchup(&db, "watcher").await.len(), 1);
    }

    #[tokio::test]
    async fn cursor_ignores_sibling_and_subtask_turns_on_the_same_issue() {
        use crate::orchestrator::attention_push::read_cursor;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        // The addressed child node's job (child-job) has 2 chat turns.
        add_chat_turn(&db, "t1", 1).await;
        add_chat_turn(&db, "t2", 2).await;
        // A sub-task job on the SAME issue accrues its own turns on its own run.
        // node chat (job-scoped) never shows these, so the cursor must not count
        // them — issue-scoped counting (the bug) would have reported 4.
        db.execute_script(
            "INSERT INTO jobs(id, project_id, issue_id, parent_job_id, uri_segment, status, current_session_id, created_at, updated_at)
               VALUES('task-job','p','issue-1','child-job','explore','running','sess3',1,1);
             INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
               VALUES('run-task','p','task-job','issue-1',1,1);
             INSERT INTO turns(id, session_id, run_id, sequence, state, created_at, updated_at)
               VALUES('tk1','sess3','run-task',1,'completed',1,1);
             INSERT INTO turns(id, session_id, run_id, sequence, state, created_at, updated_at)
               VALUES('tk2','sess3','run-task',2,'completed',1,1);
             INSERT INTO events(id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
               VALUES('etk1','run-task','tk1',1,1,'assistant','{}',1);
             INSERT INTO events(id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
               VALUES('etk2','run-task','tk2',2,1,'assistant','{}',1);",
        )
        .await
        .unwrap();

        create_catchup_push(&db, "watcher", &child_node_uri())
            .await
            .unwrap();
        let pushes = pending_catchup(&db, "watcher").await;
        assert_eq!(pushes.len(), 1);
        deliver(&db, std::slice::from_ref(&pushes[0].id), "carry-1", 100).await;
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            Some(2),
            "cursor counts only the addressed job's turns, not the issue's sub-task runs"
        );
    }

    #[tokio::test]
    async fn task_targeted_message_scopes_catchup_to_the_subtask_job() {
        use crate::orchestrator::attention_push::read_cursor;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        // The node job (child-job) has 1 turn; the addressed sub-task job has 2.
        add_chat_turn(&db, "t1", 1).await;
        db.execute_script(
            "INSERT INTO jobs(id, project_id, issue_id, parent_job_id, uri_segment, status, current_session_id, created_at, updated_at)
               VALUES('task-job','p','issue-1','child-job','explore','running','sess3',1,1);
             INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
               VALUES('run-task','p','task-job','issue-1',1,1);
             INSERT INTO turns(id, session_id, run_id, sequence, state, created_at, updated_at)
               VALUES('tk1','sess3','run-task',1,'completed',1,1);
             INSERT INTO turns(id, session_id, run_id, sequence, state, created_at, updated_at)
               VALUES('tk2','sess3','run-task',2,'completed',1,1);
             INSERT INTO events(id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
               VALUES('etk1','run-task','tk1',1,1,'assistant','{}',1);
             INSERT INTO events(id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
               VALUES('etk2','run-task','tk2',2,1,'assistant','{}',1);",
        )
        .await
        .unwrap();

        // A user message directed at the sub-task URI must scope catch-up to the
        // task's own chat, not the parent node's.
        let task_uri = format!("{CHILD_URI}/1/builder/task/explore");
        create_catchup_push(&db, "watcher", &task_uri)
            .await
            .unwrap();
        let pushes = pending_catchup(&db, "watcher").await;
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].key, "catchup:task-job");
        // Renders the task chat; start = task tail - 1 = 1 (task has 2 turns).
        assert!(
            pushes[0]
                .content_ref
                .ends_with("/task/explore/chat?offset=1"),
            "task-targeted catch-up renders the sub-task chat: {}",
            pushes[0].content_ref
        );
        deliver(&db, std::slice::from_ref(&pushes[0].id), "carry-1", 100).await;
        assert_eq!(
            read_cursor(&db, "watcher", "task-job").await.unwrap(),
            Some(2),
            "cursor is scoped to the sub-task job's turns"
        );
    }
}
