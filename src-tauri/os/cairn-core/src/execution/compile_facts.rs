//! The compile fact: whether one tree builds, recorded once and read by every
//! consumer that would otherwise compile it again.
//!
//! The shape is cairn/5236's (testing owns what the fact means; this module
//! only stores and reads it). Writers: the landing compile gate, for every
//! merge result it judges, and any check lane whose output is a cargo build
//! failure. Readers: the landing gate's own reuse (a merge result whose tree
//! already has a `builds` row for the same command lands without a run), the
//! PR surface (a refused head renders as CONFLICTING until it moves), and the
//! `compiles` gate fact. The lane hold and red attribution are 5236's.

use std::sync::Arc;

use cairn_db::turso::{params, Connection};
use serde::Serialize;

use crate::execution::compile_signature::CompileFailure;
use crate::storage::{LocalDb, RowExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompileOutcome {
    Builds,
    DoesNotBuild,
    /// Nothing compiled the tree: no cell within budget, or placement refused.
    /// A refusal, never a pass.
    NotRun,
}

impl CompileOutcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Builds => "builds",
            Self::DoesNotBuild => "does-not-build",
            Self::NotRun => "not-run",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "builds" => Some(Self::Builds),
            "does-not-build" => Some(Self::DoesNotBuild),
            "not-run" => Some(Self::NotRun),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CompileSource {
    LandingGate,
    Lane(String),
    MainCadence,
}

impl CompileSource {
    pub(crate) fn label(&self) -> String {
        match self {
            Self::LandingGate => "landing-gate".to_string(),
            Self::Lane(check) => format!("lane:{check}"),
            Self::MainCadence => "main-cadence".to_string(),
        }
    }
}

/// One site of a build failure's signature. Attribution matches on `code` and
/// `path` and ignores `line`, which an unrelated edit above it moves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SignatureSite {
    pub code: Option<String>,
    pub path: Option<String>,
    pub line: Option<u32>,
}

pub(crate) fn signature(failure: &CompileFailure) -> Vec<SignatureSite> {
    failure
        .errors
        .iter()
        .map(|error| SignatureSite {
            code: error.code.clone(),
            path: error.path.clone(),
            line: error.line,
        })
        .collect()
}

#[derive(Debug, Clone)]
pub(crate) struct CompileFactWrite {
    pub project_id: String,
    pub tree_commit: String,
    pub tree_hash: String,
    pub base_commit: Option<String>,
    pub head_commit: Option<String>,
    pub merge_request_id: Option<String>,
    pub platform: Option<String>,
    pub executor_id: Option<String>,
    pub scope: String,
    pub check_name: Option<String>,
    pub outcome: CompileOutcome,
    pub failure: Option<CompileFailure>,
    pub diagnostic_tail: Option<String>,
    pub source: CompileSource,
    pub observation_id: Option<String>,
}

/// The stored diagnostic text is bounded: it is the tail a reader quotes, and
/// the signature carries every site.
const DIAGNOSTIC_TAIL_CHARS: usize = 16_000;

fn bounded(text: &str) -> String {
    if text.chars().count() <= DIAGNOSTIC_TAIL_CHARS {
        return text.to_string();
    }
    text.chars().take(DIAGNOSTIC_TAIL_CHARS).collect()
}

pub(crate) async fn record_compile_fact(
    db: &LocalDb,
    fact: CompileFactWrite,
) -> Result<String, String> {
    let id = uuid::Uuid::new_v4().to_string();
    let signature_json = fact
        .failure
        .as_ref()
        .map(|failure| serde_json::to_string(&signature(failure)).unwrap_or_default());
    let first = fact.failure.as_ref().and_then(|failure| failure.first_failed().cloned());
    let row_id = id.clone();
    db.write(move |conn| {
        let fact = fact.clone();
        let id = row_id.clone();
        let signature_json = signature_json.clone();
        let first = first.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO compile_facts (
                    id, project_id, tree_commit, tree_hash, base_commit, head_commit,
                    merge_request_id, platform, executor_id, scope, check_name, outcome,
                    signature_json, first_failing_crate, first_failing_target,
                    diagnostic_tail, source, observation_id, written_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                           ?15, ?16, ?17, ?18, ?19)",
                params![
                    id.as_str(),
                    fact.project_id.as_str(),
                    fact.tree_commit.as_str(),
                    fact.tree_hash.as_str(),
                    fact.base_commit.as_deref(),
                    fact.head_commit.as_deref(),
                    fact.merge_request_id.as_deref(),
                    fact.platform.as_deref(),
                    fact.executor_id.as_deref(),
                    fact.scope.as_str(),
                    fact.check_name.as_deref(),
                    fact.outcome.as_str(),
                    signature_json.as_deref(),
                    first.as_ref().map(|target| target.krate.as_str()),
                    first.as_ref().map(|target| target.target.as_str()),
                    fact.diagnostic_tail.as_deref().map(bounded),
                    fact.source.label(),
                    fact.observation_id.as_deref(),
                    chrono::Utc::now().timestamp_millis(),
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("Failed to record compile fact: {error}"))?;
    Ok(id)
}

/// [`record_compile_fact`] for a synchronous caller (the check composer, which
/// records its observations the same way).
pub(crate) fn record_compile_fact_blocking(
    db: Arc<LocalDb>,
    fact: CompileFactWrite,
) -> Result<String, String> {
    crate::execution::cache::run_checkpoint_cache_db(async move {
        record_compile_fact(&db, fact).await
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompileFact {
    pub id: String,
    pub tree_commit: String,
    pub base_commit: Option<String>,
    pub check_name: Option<String>,
    pub outcome: CompileOutcome,
    pub diagnostic_tail: Option<String>,
    pub source: String,
}

const FACT_COLUMNS: &str =
    "id, tree_commit, base_commit, check_name, outcome, diagnostic_tail, source";

fn fact_from_row(row: &cairn_db::turso::Row) -> Result<Option<CompileFact>, crate::storage::DbError> {
    let outcome = row.text(4)?;
    let Some(outcome) = CompileOutcome::parse(&outcome) else {
        return Ok(None);
    };
    Ok(Some(CompileFact {
        id: row.text(0)?,
        tree_commit: row.text(1)?,
        base_commit: row.opt_text(2)?,
        check_name: row.opt_text(3)?,
        outcome,
        diagnostic_tail: row.opt_text(5)?,
        source: row.text(6)?,
    }))
}

/// A `builds` row for this tree and this exact compile command, from any
/// source. Rule 2 of cairn/5224: a green fact for the same tree satisfies the
/// landing gate without a run.
pub(crate) async fn builds_fact_for_tree(
    db: &LocalDb,
    project_id: &str,
    tree_hash: &str,
    scope: &str,
) -> Result<Option<CompileFact>, String> {
    let keys = (
        project_id.to_string(),
        tree_hash.to_string(),
        scope.to_string(),
    );
    db.read(move |conn| {
        let keys = keys.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT {FACT_COLUMNS} FROM compile_facts
                          WHERE project_id = ?1 AND tree_hash = ?2 AND scope = ?3
                            AND outcome = 'builds'
                          ORDER BY written_at DESC
                          LIMIT 1"
                    ),
                    params![keys.0.as_str(), keys.1.as_str(), keys.2.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => fact_from_row(&row),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|error| format!("Failed to read compile facts: {error}"))
}

/// The newest fact about a tree that says whether it builds: a `not-run` row
/// says nothing either way, so it is passed over.
pub(crate) async fn verdict_for_tree(
    db: &LocalDb,
    project_id: &str,
    tree_hash: &str,
) -> Result<Option<CompileFact>, String> {
    let keys = (project_id.to_string(), tree_hash.to_string());
    db.read(move |conn| {
        let keys = keys.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT {FACT_COLUMNS} FROM compile_facts
                          WHERE project_id = ?1 AND tree_hash = ?2
                            AND outcome IN ('builds', 'does-not-build')
                          ORDER BY written_at DESC
                          LIMIT 1"
                    ),
                    params![keys.0.as_str(), keys.1.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => fact_from_row(&row),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|error| format!("Failed to read compile facts: {error}"))
}

/// The landing gate's newest judgement of a merge result built from this
/// branch head, when it was that the result does not compile. The head is
/// the key on purpose: the refusal stands exactly until the branch moves.
pub(crate) async fn landing_refusal_for_head(
    conn: &Connection,
    head_commit: &str,
) -> Option<CompileFact> {
    let mut rows = conn
        .query(
            &format!(
                "SELECT {FACT_COLUMNS} FROM compile_facts
                  WHERE head_commit = ?1 AND source = 'landing-gate'
                  ORDER BY written_at DESC
                  LIMIT 1"
            ),
            params![head_commit],
        )
        .await
        .ok()?;
    let row = rows.next().await.ok()??;
    fact_from_row(&row)
        .ok()
        .flatten()
        .filter(|fact| fact.outcome == CompileOutcome::DoesNotBuild)
}

/// How many diagnostic lines the PR surface quotes; the refusal itself and the
/// stored row carry the rest.
const RENDERED_REFUSAL_LINES: usize = 20;

/// The PR surface's Mergeable line for a head whose merge result the landing
/// gate refused, with the errors and the remedy.
pub(crate) fn render_landing_refusal(fact: &CompileFact, source: &str, target: &str) -> String {
    let base = fact
        .base_commit
        .as_deref()
        .map(|base| format!(" at {}", base.get(..12).unwrap_or(base)))
        .unwrap_or_default();
    let check = fact.check_name.as_deref().unwrap_or("the landing check");
    let tail = fact.diagnostic_tail.as_deref().unwrap_or_default();
    let mut lines: Vec<&str> = tail.lines().take(RENDERED_REFUSAL_LINES).collect();
    let more = tail.lines().count().saturating_sub(lines.len());
    let more_line = format!("- … {more} more lines in compile fact {}", fact.id);
    if more > 0 {
        lines.push(&more_line);
    }
    format!(
        "Mergeable: CONFLICTING (the merge result onto `{target}`{base} does not compile; \
         the landing gate refused it on `{check}`)\n\
         Rebase `{source}` onto current `{target}`, fix these on the branch, then merge \
         again:\n{}\n",
        lines.join("\n")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::compile_signature::parse_compile_failure;

    async fn db() -> LocalDb {
        let dir = tempfile::tempdir().unwrap();
        LocalDb::open_migrated_for_test(dir.keep().join("compile-facts.db"))
            .await
            .unwrap()
    }

    fn write(outcome: CompileOutcome, tree: &str, head: Option<&str>) -> CompileFactWrite {
        CompileFactWrite {
            project_id: "project".into(),
            tree_commit: format!("commit-{tree}"),
            tree_hash: tree.into(),
            base_commit: Some("base".into()),
            head_commit: head.map(str::to_string),
            merge_request_id: Some("mr".into()),
            platform: Some("linux".into()),
            executor_id: Some("fedora".into()),
            scope: "cargo check --workspace --tests --locked".into(),
            check_name: Some("landing-compile".into()),
            outcome,
            failure: None,
            diagnostic_tail: None,
            source: CompileSource::LandingGate,
            observation_id: None,
        }
    }

    #[tokio::test]
    async fn a_green_fact_is_found_by_tree_and_command_only() {
        let db = db().await;
        record_compile_fact(&db, write(CompileOutcome::Builds, "tree-a", None))
            .await
            .unwrap();
        let scope = "cargo check --workspace --tests --locked";
        assert!(builds_fact_for_tree(&db, "project", "tree-a", scope)
            .await
            .unwrap()
            .is_some());
        assert!(builds_fact_for_tree(&db, "project", "tree-b", scope)
            .await
            .unwrap()
            .is_none());
        assert!(builds_fact_for_tree(&db, "project", "tree-a", "cargo check -p one")
            .await
            .unwrap()
            .is_none());
    }

    /// A gate that could not run is not evidence either way, so it neither
    /// satisfies reuse nor hides the verdict before it.
    #[tokio::test]
    async fn a_not_run_row_is_never_a_verdict() {
        let db = db().await;
        record_compile_fact(&db, write(CompileOutcome::NotRun, "tree-a", None))
            .await
            .unwrap();
        assert!(verdict_for_tree(&db, "project", "tree-a").await.unwrap().is_none());
        record_compile_fact(&db, write(CompileOutcome::DoesNotBuild, "tree-a", None))
            .await
            .unwrap();
        record_compile_fact(&db, write(CompileOutcome::NotRun, "tree-a", None))
            .await
            .unwrap();
        let verdict = verdict_for_tree(&db, "project", "tree-a").await.unwrap().unwrap();
        assert_eq!(verdict.outcome, CompileOutcome::DoesNotBuild);
    }

    #[tokio::test]
    async fn a_red_landing_stands_for_its_head_until_a_later_judgement_of_it() {
        let db = db().await;
        let mut red = write(CompileOutcome::DoesNotBuild, "tree-a", Some("head-1"));
        red.failure = parse_compile_failure(
            "error[E0425]: cannot find function `jwt_payload` in this scope\n  --> os/cairn-models/src/auth.rs:12:5\nerror: could not compile `cairn-models` (lib) due to 1 previous error\n",
        );
        red.diagnostic_tail = red.failure.as_ref().map(CompileFailure::render);
        let id = record_compile_fact(&db, red).await.unwrap();
        let stored = db
            .query_opt_text(
                "SELECT signature_json || '|' || first_failing_crate || '|' || first_failing_target FROM compile_facts WHERE id = ?1",
                params![id.as_str()],
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored,
            r#"[{"code":"E0425","path":"os/cairn-models/src/auth.rs","line":12}]|cairn-models|lib"#
        );
        let refusal = db
            .read(|conn| Box::pin(async move { Ok(landing_refusal_for_head(conn, "head-1").await) }))
            .await
            .unwrap()
            .expect("the head's newest judgement is a red");
        let rendered = render_landing_refusal(&refusal, "agent/x", "main");
        assert!(
            rendered.starts_with("Mergeable: CONFLICTING (the merge result onto `main` at base does not compile"),
            "{rendered}"
        );
        assert!(rendered.contains("Rebase `agent/x` onto current `main`"), "{rendered}");
        assert!(
            rendered.contains("- os/cairn-models/src/auth.rs:12:5: error[E0425]"),
            "{rendered}"
        );

        record_compile_fact(&db, write(CompileOutcome::Builds, "tree-b", Some("head-1")))
            .await
            .unwrap();
        let cleared = db
            .read(|conn| Box::pin(async move { Ok(landing_refusal_for_head(conn, "head-1").await) }))
            .await
            .unwrap();
        assert!(cleared.is_none(), "a later green judgement of the head clears it");
    }
}
