//! `cairn://self-rebuild`: a self-rebuild's pending outcome.
//!
//! A self-rebuild replaces the running app, and with it every run bearer, so
//! the detached installer that performs the install and reports how it went
//! cannot speak for the run that started it. The run records a pending
//! outcome first (`cairn://self-rebuild` append, which only a run may do),
//! naming the issue or thread the outcome reports to. The installer then
//! delivers exactly one outcome to that record (`cairn://self-rebuild/{id}`
//! append). That delivery is the one write the machine credential's run-less
//! allowlist admits ([`is_self_rebuild_delivery`]): what it can do is bounded
//! by the record a run already made, and it does it once, because the claim
//! that marks a record delivered is a single conditional update.

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_uri::{build_self_rebuild_outcome_uri, parse_uri, CairnResource};

/// Where an outcome reports: an issue's channel or a durable thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReportTo {
    Issue { project: String, number: i32 },
    Thread { project: String, name: String },
}

impl ReportTo {
    pub(crate) fn parse(uri: &str) -> Result<Self, String> {
        match parse_uri(uri) {
            Some(CairnResource::Issue { project, number }) => Ok(Self::Issue { project, number }),
            Some(CairnResource::Thread {
                project,
                name,
                path,
            }) if path.is_empty() => Ok(Self::Thread { project, name }),
            _ => Err(format!(
                "reportTo must name an issue (cairn://p/PROJECT/N) or a durable thread (cairn://p/PROJECT/<name>); {uri} is neither"
            )),
        }
    }

    pub(crate) fn uri(&self) -> String {
        match self {
            Self::Issue { project, number } => CairnResource::Issue {
                project: project.clone(),
                number: *number,
            }
            .to_uri(),
            Self::Thread { project, name } => CairnResource::Thread {
                project: project.clone(),
                name: name.clone(),
                path: Vec::new(),
            }
            .to_uri(),
        }
    }
}

/// One recorded outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Outcome {
    pub id: String,
    pub report_to: String,
    pub run_id: String,
    pub created_at: i64,
    pub delivered_at: Option<i64>,
}

/// What claiming an outcome for delivery found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Claim {
    /// This claim marked the record delivered; the caller now owns delivery.
    Claimed(Outcome),
    Missing,
    AlreadyDelivered {
        at: i64,
    },
}

pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn instant(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| ms.to_string())
}

pub(crate) async fn record(
    db: &LocalDb,
    report_to: &str,
    run_id: &str,
    created_at: i64,
) -> Result<String, String> {
    let id = uuid::Uuid::new_v4().to_string();
    db.execute(
        "INSERT INTO self_rebuild_outcomes (id, report_to, run_id, created_at)
         VALUES (?1, ?2, ?3, ?4)",
        (id.as_str(), report_to, run_id, created_at),
    )
    .await
    .map_err(|error| format!("could not record the pending self-rebuild outcome: {error}"))?;
    Ok(id)
}

pub(crate) async fn load(db: &LocalDb, id: &str) -> Result<Option<Outcome>, String> {
    db.query_opt(
        "SELECT id, report_to, run_id, created_at, delivered_at
           FROM self_rebuild_outcomes WHERE id = ?1",
        (id.to_string(),),
        |row| {
            Ok(Outcome {
                id: row.text(0)?,
                report_to: row.text(1)?,
                run_id: row.text(2)?,
                created_at: row.i64(3)?,
                delivered_at: row.opt_i64(4)?,
            })
        },
    )
    .await
    .map_err(|error| error.to_string())
}

/// Mark the record delivered, only if no claim has yet. The update is the
/// whole claim, so of two concurrent deliveries exactly one changes the row.
pub(crate) async fn claim(db: &LocalDb, id: &str, at: i64) -> Result<Claim, String> {
    let changed = db
        .execute(
            "UPDATE self_rebuild_outcomes SET delivered_at = ?2
              WHERE id = ?1 AND delivered_at IS NULL",
            (id, at),
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(match (changed, load(db, id).await?) {
        (_, None) => Claim::Missing,
        (0, Some(outcome)) => Claim::AlreadyDelivered {
            at: outcome.delivered_at.unwrap_or_default(),
        },
        (_, Some(outcome)) => Claim::Claimed(outcome),
    })
}

/// Undo a claim whose message append failed, so the installer's next attempt
/// can still deliver. Only the claim stamped `at` is undone.
pub(crate) async fn release(db: &LocalDb, id: &str, at: i64) -> Result<(), String> {
    db.execute(
        "UPDATE self_rebuild_outcomes SET delivered_at = NULL
          WHERE id = ?1 AND delivered_at = ?2",
        (id, at),
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// Append `content` to the outcome's issue or thread. Works for a run-less
/// request: the message is attributed to the external sender.
pub(crate) async fn append_report(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    report_to: &ReportTo,
    content: &str,
) -> Result<String, String> {
    match report_to {
        ReportTo::Issue { project, number } => {
            crate::mcp::handlers::messages::append_project_or_issue_message(
                orch,
                crate::mcp::handlers::messages::MessageAuthor::Mcp(request),
                project,
                Some(*number),
                content,
            )
            .await
        }
        ReportTo::Thread { project, name } => {
            let thread_id = thread_id(orch, project, name).await?;
            crate::mcp::handlers::messages::append_thread_message(
                orch, request, project, &thread_id, content,
            )
            .await
        }
    }
}

/// The id of a durable thread, or why it cannot be reported to.
pub(crate) async fn thread_id(
    orch: &Orchestrator,
    project: &str,
    name: &str,
) -> Result<String, String> {
    let db = orch.db.for_project(project).await;
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(&db, project).await?;
    crate::threads::crud::by_name(&db, &project_id, name)
        .await
        .map(|thread| thread.id)
}

pub(crate) fn render_collection() -> String {
    "# Self-rebuild outcomes\n\nThe run that starts a self-rebuild records its pending outcome here, naming the issue or thread it reports to; the detached installer delivers it once at the `cairn://self-rebuild/<id>` the record returns.".to_string()
}

pub(crate) async fn render_outcome(db: &LocalDb, id: &str) -> String {
    let uri = build_self_rebuild_outcome_uri(id);
    match load(db, id).await {
        Err(error) => format!("{uri} could not be read: {error}"),
        Ok(None) => format!("No self-rebuild outcome is recorded at {uri}."),
        Ok(Some(outcome)) => {
            let delivered = match outcome.delivered_at {
                Some(at) => format!("yes, at {}", instant(at)),
                None => "no; appending content delivers it, once".to_string(),
            };
            format!(
                "# Self-rebuild outcome\n\n- uri: {uri}\n- reportTo: {}\n- recorded: {}\n- delivered: {delivered}",
                outcome.report_to,
                instant(outcome.created_at)
            )
        }
    }
}

pub(crate) fn delivered_refusal(id: &str, at: i64) -> String {
    format!(
        "Self-rebuild outcome {} was already delivered at {}; an outcome is delivered once, and nothing was written.",
        build_self_rebuild_outcome_uri(id),
        instant(at)
    )
}

pub(crate) fn missing_refusal(id: &str) -> String {
    format!(
        "No self-rebuild outcome is recorded at {}; nothing was written. A run records one with an append of {{reportTo}} to cairn://self-rebuild.",
        build_self_rebuild_outcome_uri(id)
    )
}

/// Whether `request` is a self-rebuild outcome delivery and nothing else:
/// a `write` of exactly one change that appends `{content}` to
/// `cairn://self-rebuild/{id}`, with no commit message or other batch option.
/// The run-less allowlist on the machine credential admits exactly this.
pub fn is_self_rebuild_delivery(request: &McpCallbackRequest) -> bool {
    matches!(
        crate::mcp::run_bearer::lone_content_append(request),
        Some(CairnResource::SelfRebuildOutcome { .. })
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn write(payload: Value) -> McpCallbackRequest {
        McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/".to_string(),
            run_id: None,
            tool: "write".to_string(),
            payload,
            tool_use_id: None,
            origin: None,
        }
    }

    fn delivery(target: &str) -> Value {
        json!({"changes": [{"target": target, "mode": "append", "payload": {"content": "done"}}]})
    }

    const OUTCOME: &str = "cairn://self-rebuild/5d2b7c1e-0000-4000-8000-000000000001";

    #[test]
    fn one_append_of_content_to_an_outcome_is_a_delivery() {
        assert!(is_self_rebuild_delivery(&write(delivery(OUTCOME))));
        // A typed client's unset options ride along as null or false.
        let mut typed = delivery(OUTCOME);
        typed["commit_msg"] = Value::Null;
        typed["branch"] = Value::Null;
        typed["preview"] = json!(false);
        assert!(is_self_rebuild_delivery(&write(typed)));
    }

    #[test]
    fn anything_wider_than_one_delivery_is_not() {
        let mut read = write(delivery(OUTCOME));
        read.tool = "read".into();
        assert!(!is_self_rebuild_delivery(&read));

        for target in [
            "cairn://self-rebuild",
            "cairn://p/cairn/devops/messages",
            "cairn://p/cairn/devops",
            "cairn:~/self-rebuild/x",
            "file:src/lib.rs",
        ] {
            assert!(
                !is_self_rebuild_delivery(&write(delivery(target))),
                "{target}"
            );
        }

        let mut committed = delivery(OUTCOME);
        committed["commit_msg"] = json!("land it");
        assert!(!is_self_rebuild_delivery(&write(committed)));

        let mut branched = delivery(OUTCOME);
        branched["branch"] = json!("main");
        assert!(!is_self_rebuild_delivery(&write(branched)));

        let mut patched = delivery(OUTCOME);
        patched["changes"][0]["mode"] = json!("patch");
        assert!(!is_self_rebuild_delivery(&write(patched)));

        let mut widened = delivery(OUTCOME);
        widened["changes"][0]["payload"]["escalate"] = json!(true);
        assert!(!is_self_rebuild_delivery(&write(widened)));

        let mut extra_key = delivery(OUTCOME);
        extra_key["changes"][0]["why"] = json!("x");
        assert!(!is_self_rebuild_delivery(&write(extra_key)));

        let two = json!({"changes": [
            {"target": OUTCOME, "mode": "append", "payload": {"content": "a"}},
            {"target": OUTCOME, "mode": "append", "payload": {"content": "b"}},
        ]});
        assert!(!is_self_rebuild_delivery(&write(two)));
        assert!(!is_self_rebuild_delivery(&write(json!({"changes": []}))));
        assert!(!is_self_rebuild_delivery(&write(json!([]))));
    }

    #[test]
    fn report_to_is_an_issue_or_a_durable_thread() {
        assert_eq!(
            ReportTo::parse("cairn://p/cairn/4702").unwrap(),
            ReportTo::Issue {
                project: "cairn".into(),
                number: 4702
            }
        );
        assert_eq!(
            ReportTo::parse("cairn://p/cairn/devops").unwrap().uri(),
            "cairn://p/cairn/devops"
        );
        for uri in [
            "cairn://p/cairn/devops/messages",
            "cairn://p/cairn/issues",
            "cairn://bug",
            "not a uri",
        ] {
            assert!(ReportTo::parse(uri).is_err(), "{uri}");
        }
    }
}
