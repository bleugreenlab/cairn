//! The `after:` gate predicate vocabulary: the fact sources that answer "is
//! this predicate true *now*?" for a held execution.
//!
//! ## Level resolution, not event sourcing
//!
//! Every fact source here is LEVEL-RESOLVABLE. [`GateFact::resolve`] answers by
//! querying current state; it never consumes an event. A satisfying transition
//! only prompts a re-evaluation that the next scheduler tick would have done
//! anyway. This is the whole design: an event-sourced gate strands an execution
//! forever when an event drops — a restart between a commit and its dispatch, a
//! transition committed on a replica this host never observed, a merge that
//! happened while we were offline — and "silently never starts" is exactly the
//! outcome the asymmetric-release ruling forbids. Level resolution costs one
//! tick of latency in the worst case instead.
//!
//! ## Three outcomes, not a bool
//!
//! [`GateOutcome`] separates `Refuted` (the fact reached a value that
//! contradicts the clause) from `NoEvidence` (nothing has borne on it yet).
//! Collapsing them would make a gate that is *quiet* indistinguishable from a
//! gate that is merely unsatisfied, and a quiet gate is invisible.
//!
//! ## Clause objects are the only authoring form
//!
//! A predicate is a [`TriggerClause`] — the same `BTreeMap<String, Value>` that
//! routes and thread definition graphs use — discriminated by its `fact` key.
//! The prose form ("after cairn/4724 merges") is [`GateFact::render`]'s output,
//! a RENDERING, not a grammar. Two predicate languages over one fact space
//! drift; this is ruled, not preferred.
//!
//! ## Extending it
//!
//! Because the clause is opaque and discriminated by `fact`, a new fact becomes
//! expressible by registering one [`GateFact`] with [`GateVocabulary::register`]
//! — no schema change, no migration of stored predicates. The `decision` and
//! `discussion` arms are deliberately left OPEN rather than stubbed: until those
//! entities exist, an unknown fact is refused BY NAME so an author learns the
//! vocabulary from the refusal.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use cairn_db::turso::params;
use serde_json::Value;

use crate::config::project_settings::ChecksContract;
use crate::execution::cache::{
    get_reusable_check_result, CheckResultCacheEntry, ReusableCheckLookup,
};
use crate::execution::check_identity::{
    verdict_environment_names, verdict_platforms, CHECK_RESULT_SCHEMA_VERSION,
};
use crate::execution::check_parsers::ParsedCheckResult;
use crate::execution::checks::{
    check_platform_identity, check_result_key, check_toolchain_identity,
    load_checks_contract_at_commit, CommitChecksContract,
};
use crate::execution::inputs::{ResolvedInputs, TreeBlobs, TreeSnapshot};
use crate::issues::relations::{canonicalize_issue_uri, resolve_issue_uri};
use crate::jj::JjEnv;
use crate::models::IssueStatus;
use crate::orchestrator::Orchestrator;
use crate::routes::{value_matches, FieldKind, TriggerClause};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

/// The terminal resolutions an issue can reach. An issue that has not reached
/// one of these bears no `issue_resolution` value at all, which is why an
/// omitted `resolution` field means "resolved, either way" rather than "any
/// status whatsoever".
pub(crate) const GATE_RESOLUTIONS: &[&str] = &["merged", "closed"];
/// Pull-request states. `open` is not terminal; the other two are.
pub(crate) const GATE_PR_STATES: &[&str] = &["open", "merged", "closed"];
/// The verdicts a check observation can carry.
pub(crate) const GATE_VERDICTS: &[&str] = &["passed", "failed"];

/// What the evidence says about one gate predicate right now.
///
/// Only `Satisfied` latches. `Refuted` and `NoEvidence` are both "not yet", and
/// they are kept apart because they render differently: a gate nothing has
/// borne on is quiet, a gate with contrary evidence is answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateOutcome {
    Satisfied,
    Refuted,
    NoEvidence,
}

impl GateOutcome {
    /// The stored form, for a gate row's `last_outcome`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Satisfied => "satisfied",
            Self::Refuted => "refuted",
            Self::NoEvidence => "no_evidence",
        }
    }

    /// The evidence-state phrase a census appends to [`GateFact::render`]. One
    /// home for these strings so a held execution reads the same everywhere it
    /// is shown.
    pub fn evidence_phrase(self) -> &'static str {
        match self {
            Self::Satisfied => "satisfied",
            Self::Refuted => "evidence says no",
            Self::NoEvidence => "no verdict yet",
        }
    }
}

/// What a resolver may read. Carries the orchestrator rather than a bare
/// connection because `check_verdict` has to reach the gated ref's repository to
/// compute the check identity its verdict must match, and a fact source that
/// could not do that would have to be answered by a hand-rolled query instead.
pub struct GateContext<'a> {
    orch: &'a Orchestrator,
    now: i64,
}

impl<'a> GateContext<'a> {
    pub fn new(orch: &'a Orchestrator, now: i64) -> Self {
        Self { orch, now }
    }

    /// Unix seconds this resolution is being taken as of.
    pub fn now(&self) -> i64 {
        self.now
    }

    pub(crate) fn orch(&self) -> &Orchestrator {
        self.orch
    }

    /// The database that owns the project `issue_uri` names — the private DB
    /// for a local project, its replica for a team project.
    ///
    /// Evidence is routed by the CLAUSE's project, not by the database the held
    /// execution lives in: a gate may name an issue in any project, and a team
    /// issue's rows exist only in its replica. Reading the private DB for it
    /// would find nothing and foreclose a live issue as nonexistent.
    pub(crate) async fn db_for_issue(&self, issue_uri: &str) -> Arc<LocalDb> {
        match cairn_common::uri::parse_uri(issue_uri) {
            Some(cairn_common::uri::CairnResource::Issue { project, .. }) => {
                self.orch.db.for_project(&project).await
            }
            _ => self.orch.db.local.clone(),
        }
    }
}

/// One fact source in the gate vocabulary.
///
/// Every source implements the same five operations, and that uniformity is
/// what keeps the vocabulary cheap to extend: adding a fact is writing one impl,
/// not threading a new case through the sweep, the census, and the scheduler.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait GateFact: Send + Sync + std::fmt::Debug {
    /// The `fact` key that discriminates this source's clauses.
    fn fact(&self) -> &'static str;

    /// Every field this source accepts, in declaration order. This is the ONE
    /// declaration: [`crate::routes::FactRegistry`] indexes gate sources from
    /// it, so the registry cannot describe a field the validator refuses.
    fn fields(&self) -> &'static [(&'static str, FieldKind)];

    /// A well-formed clause for this source, used to build the corrected object
    /// a refusal shows.
    fn example(&self) -> TriggerClause;

    /// Authoring-time refusal: unknown field, unparseable URI, malformed
    /// timestamp, value outside a closed set. Field membership is checked for
    /// every source by [`validate_fields`]; an impl adds only its value checks.
    fn validate(&self, clause: &TriggerClause) -> Result<(), String>;

    /// Is this predicate true right now? Answered by query, never by an event.
    async fn resolve(&self, ctx: &GateContext<'_>, clause: &TriggerClause)
        -> DbResult<GateOutcome>;

    /// `Some(reason)` when no future event can satisfy this predicate.
    async fn foreclosed(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<Option<String>>;

    /// The census phrase, without an evidence-state suffix.
    fn render(&self, clause: &TriggerClause) -> String;

    /// When this predicate could next become true on its own. Only `clock`
    /// answers; the scheduler folds `min(next_due_at)` into its sleep bound so a
    /// `not-before` releases on time rather than up to a tick late.
    fn next_due_at(&self, _clause: &TriggerClause) -> Option<i64> {
        None
    }
}

/// The registered gate fact sources, in declaration order.
///
/// Order is load-bearing exactly once: an unknown-fact refusal names the known
/// vocabulary, and it should read in the order the vocabulary is documented
/// rather than in hash order.
#[derive(Debug, Clone)]
pub struct GateVocabulary {
    sources: Vec<Arc<dyn GateFact>>,
}

impl Default for GateVocabulary {
    fn default() -> Self {
        Self {
            sources: vec![
                Arc::new(IssueResolutionFact),
                Arc::new(PrStateFact),
                Arc::new(ArtifactConfirmedFact),
                Arc::new(CheckVerdictFact),
                Arc::new(ClockFact),
                Arc::new(TenetVerdictFact),
                Arc::new(CompilesFact),
            ],
        }
    }
}

impl GateVocabulary {
    /// Add a fact source, or replace a same-named one in place. Registering is
    /// the WHOLE cost of a new predicate: stored predicates are opaque JSON
    /// discriminated by `fact`, so nothing is migrated and no column changes.
    pub fn register(&mut self, fact: Arc<dyn GateFact>) {
        match self
            .sources
            .iter_mut()
            .find(|existing| existing.fact() == fact.fact())
        {
            Some(slot) => *slot = fact,
            None => self.sources.push(fact),
        }
    }

    pub fn sources(&self) -> &[Arc<dyn GateFact>] {
        &self.sources
    }

    /// The known fact names, in declaration order.
    pub fn fact_names(&self) -> Vec<&'static str> {
        self.sources.iter().map(|source| source.fact()).collect()
    }

    /// The source a clause names, or a refusal that names the whole vocabulary.
    ///
    /// The by-name refusal is the open extension point's user interface: until
    /// `decision` and `discussion` have entities to resolve against, an author
    /// who reaches for one learns what does exist instead of getting silence.
    pub fn source_for(&self, clause: &TriggerClause) -> Result<&Arc<dyn GateFact>, String> {
        let Some(fact) = clause.get("fact").and_then(Value::as_str) else {
            return Err(format!(
                "a gate predicate needs a 'fact' key naming one of: {}. Corrected: {}",
                self.fact_names().join(", "),
                render_clause(&IssueResolutionFact.example())
            ));
        };
        self.sources
            .iter()
            .find(|source| source.fact() == fact)
            .ok_or_else(|| {
                format!(
                    "unknown gate fact '{fact}' \u{2014} known: {}",
                    self.fact_names().join(", ")
                )
            })
    }

    /// Refuse a malformed clause, showing the corrected object.
    ///
    /// A malformed clause is how an agent learns the shape, so the refusal
    /// carries a repaired version of what was actually written — the author's
    /// own good fields kept, the bad ones filled from the source's example —
    /// rather than a generic template.
    pub fn validate(&self, clause: &TriggerClause) -> Result<(), String> {
        let source = self.source_for(clause)?;
        validate_fields(source.as_ref(), clause)
            .and_then(|()| source.validate(clause))
            .map_err(|problem| {
                format!(
                    "{problem}. Corrected: {}",
                    render_clause(&correction(source.as_ref(), clause))
                )
            })
    }

    pub async fn resolve(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<GateOutcome> {
        self.source_for(clause)
            .map_err(DbError::Row)?
            .resolve(ctx, clause)
            .await
    }

    pub async fn foreclosed(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<Option<String>> {
        self.source_for(clause)
            .map_err(DbError::Row)?
            .foreclosed(ctx, clause)
            .await
    }

    pub fn render(&self, clause: &TriggerClause) -> String {
        match self.source_for(clause) {
            Ok(source) => source.render(clause),
            // An unrenderable clause is still a gate someone is held behind, so
            // it renders as itself rather than vanishing from the census.
            Err(_) => render_clause(clause),
        }
    }

    pub fn next_due_at(&self, clause: &TriggerClause) -> Option<i64> {
        self.source_for(clause).ok()?.next_due_at(clause)
    }
}

/// Field membership, shared by every source so `fields()` is the only place a
/// field is declared.
///
/// Gate clauses have no `presence` and no `Prefix` forms: those belong to
/// matching a live event, and a level-resolved fact has no stream to prefix.
/// They fall through to the unknown-field refusal, which is correct.
pub fn validate_fields(source: &dyn GateFact, clause: &TriggerClause) -> Result<(), String> {
    let known: Vec<&str> = source.fields().iter().map(|(name, _)| *name).collect();
    for key in clause.keys() {
        if key == "fact" {
            continue;
        }
        if !known.contains(&key.as_str()) {
            return Err(format!(
                "unknown field '{key}' for gate fact '{}' \u{2014} known: {}",
                source.fact(),
                known.join(", ")
            ));
        }
    }
    Ok(())
}

/// The repaired clause a refusal shows: the author's fields that this source
/// accepts, plus every example field they left out or got wrong.
fn correction(source: &dyn GateFact, clause: &TriggerClause) -> TriggerClause {
    let known: Vec<&str> = source.fields().iter().map(|(name, _)| *name).collect();
    let mut corrected: TriggerClause = clause
        .iter()
        .filter(|(key, _)| key.as_str() == "fact" || known.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    corrected.insert("fact".into(), Value::String(source.fact().into()));
    for (key, value) in source.example() {
        let repairable = {
            let mut probe = corrected.clone();
            probe.insert(key.clone(), value.clone());
            source.validate(&probe).is_ok()
        };
        if source.validate(&corrected).is_err() && repairable {
            corrected.insert(key, value);
        }
    }
    corrected
}

fn render_clause(clause: &TriggerClause) -> String {
    serde_json::to_string(clause).unwrap_or_else(|_| format!("{clause:?}"))
}

fn clause_str<'a>(clause: &'a TriggerClause, key: &str) -> Option<&'a str> {
    clause.get(key).and_then(Value::as_str)
}

/// Whether a scalar fact value satisfies a clause field, where an absent field
/// means "any". The array form goes through the shared membership matcher
/// unchanged, so `resolution: ["merged", "closed"]` means here exactly what it
/// means in a route trigger.
fn field_matches(clause: &TriggerClause, key: &str, actual: &str) -> Option<bool> {
    clause
        .get(key)
        .map(|expected| value_matches(&Value::String(actual.to_string()), expected))
}

/// How a clause field reads in prose: a scalar as itself, an array as
/// "a or b", an absent field as `any`.
fn field_phrase(clause: &TriggerClause, key: &str, any: &str) -> String {
    match clause.get(key) {
        None => any.to_string(),
        Some(Value::Array(values)) => {
            let rendered: Vec<String> = values.iter().map(scalar_phrase).collect();
            match rendered.split_last() {
                Some((last, [])) => last.clone(),
                Some((last, rest)) => format!("{} or {last}", rest.join(", ")),
                None => any.to_string(),
            }
        }
        Some(value) => scalar_phrase(value),
    }
}

fn scalar_phrase(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

/// Read and canonicalize the `issue` field every entity-scoped fact carries.
fn issue_uri(clause: &TriggerClause) -> Result<String, String> {
    let raw = clause_str(clause, "issue")
        .ok_or_else(|| "a gate on an issue needs an 'issue' field".to_string())?;
    canonicalize_issue_uri(raw)
}

/// The terminal resolution an issue has reached, or `None` while it is still
/// live. A live issue bears no `issue_resolution` value at all — that is what
/// makes `NoEvidence` the honest answer rather than `Refuted`.
fn terminal_resolution(status: &IssueStatus) -> Option<&'static str> {
    match status {
        IssueStatus::Merged => Some("merged"),
        IssueStatus::Closed => Some("closed"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// issue_resolution
// ---------------------------------------------------------------------------

/// `{fact: "issue_resolution", issue: "cairn://p/CAIRN/4724", resolution: "merged"}`
///
/// The fact bears a value only once the issue is RESOLVED, which is what makes
/// an omitted `resolution` mean "resolved either way" rather than the vacuous
/// "any status". A live issue yields `NoEvidence`.
#[derive(Debug, Clone, Copy)]
pub struct IssueResolutionFact;

const ISSUE_RESOLUTION_FIELDS: &[(&str, FieldKind)] = &[
    ("issue", FieldKind::Scalar),
    ("resolution", FieldKind::Scalar),
];

#[async_trait]
impl GateFact for IssueResolutionFact {
    fn fact(&self) -> &'static str {
        "issue_resolution"
    }

    fn fields(&self) -> &'static [(&'static str, FieldKind)] {
        ISSUE_RESOLUTION_FIELDS
    }

    fn example(&self) -> TriggerClause {
        BTreeMap::from([
            ("fact".into(), Value::String(self.fact().into())),
            ("issue".into(), Value::String("cairn://p/CAIRN/123".into())),
            ("resolution".into(), Value::String("merged".into())),
        ])
    }

    fn validate(&self, clause: &TriggerClause) -> Result<(), String> {
        issue_uri(clause)?;
        validate_enum(clause, "resolution", GATE_RESOLUTIONS)
    }

    async fn resolve(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<GateOutcome> {
        Ok(match issue_resolution_state(ctx, clause).await? {
            IssueResolutionState::Missing
            | IssueResolutionState::Unreachable
            | IssueResolutionState::Live => GateOutcome::NoEvidence,
            IssueResolutionState::Resolved { matched: true, .. } => GateOutcome::Satisfied,
            IssueResolutionState::Resolved { matched: false, .. } => GateOutcome::Refuted,
        })
    }

    async fn foreclosed(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<Option<String>> {
        let uri = issue_uri(clause).map_err(DbError::Row)?;
        Ok(match issue_resolution_state(ctx, clause).await? {
            // A resolution is a one-way door: an issue that closed without
            // merging can never become merged, so a gate on `merged` will never
            // be satisfied and the filing thread should hear about it now.
            IssueResolutionState::Resolved {
                matched: false,
                actual,
            } => Some(format!(
                "{uri} is {actual}; a resolution is terminal, so it can never become {}",
                field_phrase(clause, "resolution", "resolved")
            )),
            IssueResolutionState::Missing => Some(format!("{uri} no longer exists")),
            _ => None,
        })
    }

    fn render(&self, clause: &TriggerClause) -> String {
        format!(
            "issue {} {}",
            clause_str(clause, "issue").unwrap_or("?"),
            field_phrase(clause, "resolution", "resolved")
        )
    }
}

enum IssueResolutionState {
    /// The project is here and the issue is not: it was deleted, or never
    /// existed. Nothing can bring it back, so this forecloses.
    Missing,
    /// The issue's PROJECT is not in the database the clause routed to — a team
    /// replica not yet open, a route not yet loaded. That is this host not being
    /// able to see the issue, not the issue being gone, so it is silence and
    /// never a foreclosure.
    Unreachable,
    Live,
    Resolved {
        actual: &'static str,
        matched: bool,
    },
}

/// Resolve and foreclose ask the same question of the same row, so they ask it
/// once, in one place.
async fn issue_resolution_state(
    ctx: &GateContext<'_>,
    clause: &TriggerClause,
) -> DbResult<IssueResolutionState> {
    let uri = issue_uri(clause).map_err(DbError::Row)?;
    let clause = clause.clone();
    ctx.db_for_issue(&uri)
        .await
        .read(move |conn| {
            let uri = uri.clone();
            let clause = clause.clone();
            Box::pin(async move {
                let Some(issue) = resolve_issue_uri(conn, &uri).await? else {
                    return Ok(if project_present(conn, &uri).await? {
                        IssueResolutionState::Missing
                    } else {
                        IssueResolutionState::Unreachable
                    });
                };
                let Some(actual) = terminal_resolution(&issue.status) else {
                    return Ok(IssueResolutionState::Live);
                };
                Ok(IssueResolutionState::Resolved {
                    actual,
                    matched: field_matches(&clause, "resolution", actual).unwrap_or(true),
                })
            })
        })
        .await
}

/// Whether the issue a clause names can still bear evidence. The distinction is
/// the same one [`IssueResolutionState`] draws: an issue gone from a project
/// this database holds can never answer again, while a project this host cannot
/// see is silence, never a verdict.
enum TargetPresence {
    Present,
    Missing,
    Unreachable,
}

async fn target_presence(ctx: &GateContext<'_>, uri: &str) -> DbResult<TargetPresence> {
    let uri = uri.to_string();
    ctx.db_for_issue(&uri)
        .await
        .read(move |conn| {
            let uri = uri.clone();
            Box::pin(async move {
                if resolve_issue_uri(conn, &uri).await?.is_some() {
                    return Ok(TargetPresence::Present);
                }
                Ok(if project_present(conn, &uri).await? {
                    TargetPresence::Missing
                } else {
                    TargetPresence::Unreachable
                })
            })
        })
        .await
}

/// The foreclosure every issue-scoped fact shares: a gate whose target issue
/// was deleted can never be satisfied, whatever it was waiting to see there.
async fn deleted_target(ctx: &GateContext<'_>, clause: &TriggerClause) -> DbResult<Option<String>> {
    let uri = issue_uri(clause).map_err(DbError::Row)?;
    Ok(match target_presence(ctx, &uri).await? {
        TargetPresence::Missing => Some(format!("{uri} no longer exists")),
        TargetPresence::Present | TargetPresence::Unreachable => None,
    })
}

/// Whether the project `issue_uri` names has a row in this database.
async fn project_present(conn: &cairn_db::turso::Connection, issue_uri: &str) -> DbResult<bool> {
    let Some(cairn_common::uri::CairnResource::Issue { project, .. }) =
        cairn_common::uri::parse_uri(issue_uri)
    else {
        return Ok(false);
    };
    let mut rows = conn
        .query(
            "SELECT 1 FROM projects WHERE key = ?1 COLLATE NOCASE LIMIT 1",
            params![cairn_common::uri::canonical_project(&project)],
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

// ---------------------------------------------------------------------------
// pr_state
// ---------------------------------------------------------------------------

/// `{fact: "pr_state", issue: "cairn://p/CAIRN/4724", state: "merged"}`
#[derive(Debug, Clone, Copy)]
pub struct PrStateFact;

const PR_STATE_FIELDS: &[(&str, FieldKind)] =
    &[("issue", FieldKind::Scalar), ("state", FieldKind::Scalar)];

#[async_trait]
impl GateFact for PrStateFact {
    fn fact(&self) -> &'static str {
        "pr_state"
    }

    fn fields(&self) -> &'static [(&'static str, FieldKind)] {
        PR_STATE_FIELDS
    }

    fn example(&self) -> TriggerClause {
        BTreeMap::from([
            ("fact".into(), Value::String(self.fact().into())),
            ("issue".into(), Value::String("cairn://p/CAIRN/123".into())),
            ("state".into(), Value::String("merged".into())),
        ])
    }

    fn validate(&self, clause: &TriggerClause) -> Result<(), String> {
        issue_uri(clause)?;
        validate_enum(clause, "state", GATE_PR_STATES)
    }

    async fn resolve(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<GateOutcome> {
        let Some(status) = pr_status(ctx, clause).await? else {
            return Ok(GateOutcome::NoEvidence);
        };
        Ok(match field_matches(clause, "state", &status) {
            Some(true) | None => GateOutcome::Satisfied,
            // An open PR that was asked to be merged is not evidence against
            // the gate, only evidence it has not happened yet.
            Some(false) if !pr_status_is_terminal(&status) => GateOutcome::NoEvidence,
            Some(false) => GateOutcome::Refuted,
        })
    }

    async fn foreclosed(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<Option<String>> {
        // "No pull request" is silence only while the issue exists to open one.
        if let Some(reason) = deleted_target(ctx, clause).await? {
            return Ok(Some(reason));
        }
        let Some(status) = pr_status(ctx, clause).await? else {
            return Ok(None);
        };
        if pr_status_is_terminal(&status) && field_matches(clause, "state", &status) == Some(false)
        {
            return Ok(Some(format!(
                "the pull request on {} is {status}, which is terminal; it can never become {}",
                clause_str(clause, "issue").unwrap_or("?"),
                field_phrase(clause, "state", "resolved")
            )));
        }
        Ok(None)
    }

    fn render(&self, clause: &TriggerClause) -> String {
        format!(
            "pull request on {} {}",
            clause_str(clause, "issue").unwrap_or("?"),
            field_phrase(clause, "state", "in any state")
        )
    }
}

fn pr_status_is_terminal(status: &str) -> bool {
    matches!(status, "merged" | "closed")
}

async fn pr_status(ctx: &GateContext<'_>, clause: &TriggerClause) -> DbResult<Option<String>> {
    let uri = issue_uri(clause).map_err(DbError::Row)?;
    ctx.db_for_issue(&uri)
        .await
        .read(move |conn| {
            let uri = uri.clone();
            Box::pin(async move {
                let Some(issue) = resolve_issue_uri(conn, &uri).await? else {
                    return Ok(None);
                };
                let mut rows = conn
                    .query(
                        "SELECT status FROM merge_requests
                          WHERE issue_id = ?1
                          ORDER BY opened_at DESC, rowid DESC
                          LIMIT 1",
                        params![issue.issue_id.as_str()],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some(row.text(0)?)),
                    None => Ok(None),
                }
            })
        })
        .await
}

// ---------------------------------------------------------------------------
// artifact_confirmed
// ---------------------------------------------------------------------------

/// `{fact: "artifact_confirmed", issue: "cairn://p/CAIRN/4724", artifact: "plan"}`
///
/// Never refutes, and forecloses only when its issue is deleted: an artifact
/// that has not been confirmed can always be written and confirmed later, but
/// not on an issue that no longer exists.
#[derive(Debug, Clone, Copy)]
pub struct ArtifactConfirmedFact;

const ARTIFACT_CONFIRMED_FIELDS: &[(&str, FieldKind)] = &[
    ("issue", FieldKind::Scalar),
    ("artifact", FieldKind::Scalar),
];

#[async_trait]
impl GateFact for ArtifactConfirmedFact {
    fn fact(&self) -> &'static str {
        "artifact_confirmed"
    }

    fn fields(&self) -> &'static [(&'static str, FieldKind)] {
        ARTIFACT_CONFIRMED_FIELDS
    }

    fn example(&self) -> TriggerClause {
        BTreeMap::from([
            ("fact".into(), Value::String(self.fact().into())),
            ("issue".into(), Value::String("cairn://p/CAIRN/123".into())),
            ("artifact".into(), Value::String("plan".into())),
        ])
    }

    fn validate(&self, clause: &TriggerClause) -> Result<(), String> {
        issue_uri(clause)?;
        Ok(())
    }

    async fn resolve(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<GateOutcome> {
        let confirmed = confirmed_artifact_types(ctx, clause).await?;
        let satisfied = confirmed
            .iter()
            .any(|kind| field_matches(clause, "artifact", kind).unwrap_or(true));
        Ok(if satisfied {
            GateOutcome::Satisfied
        } else {
            GateOutcome::NoEvidence
        })
    }

    async fn foreclosed(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<Option<String>> {
        deleted_target(ctx, clause).await
    }

    fn render(&self, clause: &TriggerClause) -> String {
        format!(
            "{} confirmed on {}",
            field_phrase(clause, "artifact", "any artifact"),
            clause_str(clause, "issue").unwrap_or("?")
        )
    }
}

async fn confirmed_artifact_types(
    ctx: &GateContext<'_>,
    clause: &TriggerClause,
) -> DbResult<Vec<String>> {
    let uri = issue_uri(clause).map_err(DbError::Row)?;
    ctx.db_for_issue(&uri)
        .await
        .read(move |conn| {
            let uri = uri.clone();
            Box::pin(async move {
                let Some(issue) = resolve_issue_uri(conn, &uri).await? else {
                    return Ok(Vec::new());
                };
                let mut rows = conn
                    .query(
                        "SELECT DISTINCT a.artifact_type
                           FROM artifacts a
                           JOIN jobs j ON j.id = a.job_id
                          WHERE j.issue_id = ?1 AND a.confirmed = 1",
                        params![issue.issue_id.as_str()],
                    )
                    .await?;
                let mut kinds = Vec::new();
                while let Some(row) = rows.next().await? {
                    kinds.push(row.text(0)?);
                }
                Ok(kinds)
            })
        })
        .await
}

// ---------------------------------------------------------------------------
// clock
// ---------------------------------------------------------------------------

/// `{fact: "clock", notBefore: "2026-09-05T12:00:00Z"}`
///
/// This extends the shared vocabulary in place rather than forking a second
/// mechanism for "wait until": one fact source whose resolver reads the clock
/// and whose [`GateFact::next_due_at`] feeds the scheduler's sleep bound.
#[derive(Debug, Clone, Copy)]
pub struct ClockFact;

const CLOCK_FIELDS: &[(&str, FieldKind)] = &[("notBefore", FieldKind::Scalar)];

#[async_trait]
impl GateFact for ClockFact {
    fn fact(&self) -> &'static str {
        "clock"
    }

    fn fields(&self) -> &'static [(&'static str, FieldKind)] {
        CLOCK_FIELDS
    }

    fn example(&self) -> TriggerClause {
        BTreeMap::from([
            ("fact".into(), Value::String(self.fact().into())),
            (
                "notBefore".into(),
                Value::String("2026-09-05T12:00:00Z".into()),
            ),
        ])
    }

    fn validate(&self, clause: &TriggerClause) -> Result<(), String> {
        not_before(clause).map(|_| ())
    }

    async fn resolve(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<GateOutcome> {
        let at = not_before(clause).map_err(DbError::Row)?;
        Ok(if ctx.now() >= at {
            GateOutcome::Satisfied
        } else {
            GateOutcome::NoEvidence
        })
    }

    /// The clock NEVER forecloses. Every instant it waits for arrives.
    async fn foreclosed(
        &self,
        _ctx: &GateContext<'_>,
        _clause: &TriggerClause,
    ) -> DbResult<Option<String>> {
        Ok(None)
    }

    fn render(&self, clause: &TriggerClause) -> String {
        format!(
            "not before {}",
            clause_str(clause, "notBefore").unwrap_or("?")
        )
    }

    fn next_due_at(&self, clause: &TriggerClause) -> Option<i64> {
        not_before(clause).ok()
    }
}

fn not_before(clause: &TriggerClause) -> Result<i64, String> {
    let raw = clause_str(clause, "notBefore")
        .ok_or_else(|| "clock needs a 'notBefore' RFC 3339 timestamp".to_string())?;
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|at| at.timestamp())
        .map_err(|error| format!("notBefore '{raw}' is not an RFC 3339 timestamp: {error}"))
}

// ---------------------------------------------------------------------------
// tenet_verdict
// ---------------------------------------------------------------------------

/// What a tenet gate can wait for: the release policies a project can hold
/// (`crate::tenets::TenetRelease`).
pub(crate) const GATE_TENET_OUTCOMES: &[&str] = &["answered", "ratified"];

/// `{fact: "tenet_verdict", tenet: "cairn://p/CAIRN/tenets/12", outcome: "answered"}`
///
/// The operator's verdict on a tenet. The commissioning gate stamps this on a
/// thread's gated filing with `outcome` set to the project's release policy at
/// filing time, so the gate says what it waits for instead of reading a policy
/// that may have changed since.
///
/// `answered` is satisfied by an approval, including one later superseded:
/// the approval was given, and the filing it released was released on it.
/// `ratified` is never satisfied today. No verified authenticator path
/// exists, so the policy setter refuses it and no filing is stamped with it;
/// a clause written by hand with it simply waits.
#[derive(Debug, Clone, Copy)]
pub struct TenetVerdictFact;

const TENET_VERDICT_FIELDS: &[(&str, FieldKind)] =
    &[("tenet", FieldKind::Scalar), ("outcome", FieldKind::Scalar)];

#[async_trait]
impl GateFact for TenetVerdictFact {
    fn fact(&self) -> &'static str {
        crate::tenets::TENET_GATE_FACT
    }

    fn fields(&self) -> &'static [(&'static str, FieldKind)] {
        TENET_VERDICT_FIELDS
    }

    fn example(&self) -> TriggerClause {
        BTreeMap::from([
            ("fact".into(), Value::String(self.fact().into())),
            (
                "tenet".into(),
                Value::String("cairn://p/CAIRN/tenets/12".into()),
            ),
            ("outcome".into(), Value::String("answered".into())),
        ])
    }

    fn validate(&self, clause: &TriggerClause) -> Result<(), String> {
        tenet_address(clause)?;
        if clause.get("outcome").is_none() {
            return Err(
                "a tenet gate needs an 'outcome' naming what releases it: answered or ratified"
                    .into(),
            );
        }
        validate_enum(clause, "outcome", GATE_TENET_OUTCOMES)
    }

    async fn resolve(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<GateOutcome> {
        use crate::tenets::{TenetLookup, TenetStatus};
        let TenetLookup::Found(tenet) = tenet_lookup(ctx, clause).await? else {
            return Ok(GateOutcome::NoEvidence);
        };
        let answered = field_matches(clause, "outcome", "answered").unwrap_or(false);
        Ok(match tenet.status {
            TenetStatus::Approved | TenetStatus::Superseded if answered => GateOutcome::Satisfied,
            TenetStatus::Declined => GateOutcome::Refuted,
            _ => GateOutcome::NoEvidence,
        })
    }

    async fn foreclosed(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<Option<String>> {
        use crate::tenets::{TenetLookup, TenetStatus};
        let (_, number) = tenet_address(clause).map_err(DbError::Row)?;
        Ok(match tenet_lookup(ctx, clause).await? {
            // A verdict is given once, so a declined tenet is never approved.
            TenetLookup::Found(tenet) if tenet.status == TenetStatus::Declined => {
                Some(match tenet.comment.as_deref() {
                    Some(comment) => format!("tenet #{number} declined: {comment}"),
                    None => format!("tenet #{number} declined, with no comment"),
                })
            }
            TenetLookup::Found(tenet) if tenet.status == TenetStatus::Withdrawn => Some(format!(
                "tenet #{number} was withdrawn by the thread that proposed it"
            )),
            TenetLookup::Missing => Some(format!("tenet #{number} no longer exists")),
            TenetLookup::Found(_) | TenetLookup::Unreachable => None,
        })
    }

    fn render(&self, clause: &TriggerClause) -> String {
        let tenet = match tenet_address(clause) {
            Ok((_, number)) => format!("tenet #{number}"),
            Err(_) => "tenet ?".to_string(),
        };
        match clause_str(clause, "outcome") {
            Some("ratified") => format!("{tenet} ratified (verified authenticator)"),
            _ => format!("{tenet} answered (unverified desktop answer)"),
        }
    }
}

/// The project key and number a tenet URI names. A gate names a tenet by its
/// canonical number, never by a migrated ruling's slug alias.
pub(crate) fn parse_tenet_uri(raw: &str) -> Result<(String, i64), String> {
    match cairn_common::uri::parse_uri(raw.trim()) {
        Some(cairn_common::uri::CairnResource::ProjectTenet { project, key }) => key
            .parse::<i64>()
            .ok()
            .filter(|number| *number > 0)
            .map(|number| (project, number))
            .ok_or_else(|| {
                format!("tenet '{raw}' must name a tenet by number, like cairn://p/CAIRN/tenets/12")
            }),
        _ => Err(format!(
            "tenet '{raw}' is not a tenet URI like cairn://p/CAIRN/tenets/12"
        )),
    }
}

/// A tenet URI in its canonical stored form, so a gate row matches the tenet's
/// own address by string.
pub(crate) fn canonicalize_tenet_uri(raw: &str) -> Result<String, String> {
    let (project, number) = parse_tenet_uri(raw)?;
    Ok(crate::tenets::tenet_uri(&project, number))
}

fn tenet_address(clause: &TriggerClause) -> Result<(String, i64), String> {
    let raw = clause_str(clause, "tenet")
        .ok_or_else(|| "a tenet gate needs a 'tenet' field naming the tenet's URI".to_string())?;
    parse_tenet_uri(raw)
}

/// Read the tenet a clause names from the database that owns its project.
async fn tenet_lookup(
    ctx: &GateContext<'_>,
    clause: &TriggerClause,
) -> DbResult<crate::tenets::TenetLookup> {
    let (project, number) = tenet_address(clause).map_err(DbError::Row)?;
    ctx.orch()
        .db
        .for_project(&project)
        .await
        .read(move |conn| {
            let project = project.clone();
            Box::pin(async move { crate::tenets::lookup_conn(conn, &project, number).await })
        })
        .await
}

fn validate_enum(clause: &TriggerClause, key: &str, allowed: &[&str]) -> Result<(), String> {
    let values: Vec<&Value> = match clause.get(key) {
        None => return Ok(()),
        Some(Value::Array(values)) => values.iter().collect(),
        Some(value) => vec![value],
    };
    for value in values {
        if !value.as_str().is_some_and(|value| allowed.contains(&value)) {
            return Err(format!(
                "{key} {value} is not one of: {}",
                allowed.join(", ")
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// check_verdict
// ---------------------------------------------------------------------------

/// `{fact: "check_verdict", check: "rust-tests", issue: "cairn://p/CAIRN/4724", verdict: "passed"}`
///
/// A row saying `passed` is not a verdict. A gate keyed on `passed` alone fires
/// on a lie, and nothing is watching, because the gate's whole purpose is that
/// no thread turn is required. So the discriminators come from
/// [`get_reusable_check_result`] rather than a WHERE clause written here: it is
/// the codebase's only honest "is this a verdict" predicate, and duplicating it
/// guarantees drift.
///
/// Never forecloses: a red today says nothing about tomorrow's commit.
#[derive(Debug, Clone, Copy)]
pub struct CheckVerdictFact;

const CHECK_VERDICT_FIELDS: &[(&str, FieldKind)] = &[
    ("check", FieldKind::Scalar),
    ("issue", FieldKind::Scalar),
    ("verdict", FieldKind::Scalar),
];

#[async_trait]
impl GateFact for CheckVerdictFact {
    fn fact(&self) -> &'static str {
        "check_verdict"
    }

    fn fields(&self) -> &'static [(&'static str, FieldKind)] {
        CHECK_VERDICT_FIELDS
    }

    fn example(&self) -> TriggerClause {
        BTreeMap::from([
            ("fact".into(), Value::String(self.fact().into())),
            ("check".into(), Value::String("rust-tests".into())),
            ("issue".into(), Value::String("cairn://p/CAIRN/123".into())),
            ("verdict".into(), Value::String("passed".into())),
        ])
    }

    fn validate(&self, clause: &TriggerClause) -> Result<(), String> {
        issue_uri(clause)?;
        let check = clause_str(clause, "check").unwrap_or_default();
        if check.trim().is_empty() {
            return Err("check_verdict needs a 'check' field naming a configured check".into());
        }
        validate_enum(clause, "verdict", GATE_VERDICTS)
    }

    async fn resolve(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<GateOutcome> {
        let uri = issue_uri(clause).map_err(DbError::Row)?;
        let check = clause_str(clause, "check")
            .ok_or_else(|| DbError::Row("check_verdict needs a 'check' field".into()))?;
        // A coordinate we cannot resolve is an absence of evidence, not a
        // failure: an issue with no branch yet, a branch that does not resolve,
        // a commit whose contract does not declare the check. Level resolution
        // must be total, so the sweep is never poisoned by one gate.
        let Some(coordinate) = check_verdict_coordinate(ctx, &uri, check).await else {
            return Ok(GateOutcome::NoEvidence);
        };
        check_verdict_evidence(
            ctx.db_for_issue(&uri).await,
            &coordinate,
            clause.get("verdict"),
        )
        .map_err(DbError::internal)
    }

    /// A check verdict NEVER forecloses. A red is a fact about one tree; a later
    /// commit produces a different tree and can go green.
    async fn foreclosed(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<Option<String>> {
        // A verdict is produced on the issue's own work, which a deleted issue
        // will never have again.
        deleted_target(ctx, clause).await
    }

    fn render(&self, clause: &TriggerClause) -> String {
        format!(
            "check {} {} on {}",
            clause_str(clause, "check").unwrap_or("?"),
            field_phrase(clause, "verdict", "decided"),
            clause_str(clause, "issue").unwrap_or("?")
        )
    }
}

/// Everything the reuse guard needs to be asked about one check at one ref.
///
/// Split out from the resolver because the two halves fail for different
/// reasons and are worth testing apart: computing this needs the gated ref's
/// repository, while judging the stored evidence against it is pure database.
#[derive(Debug, Clone)]
pub(crate) struct CheckVerdictCoordinate {
    pub project_id: String,
    pub check_name: String,
    /// Computed from the GATED REF'S OWN check definition. This is how
    /// `defined_by_commit_sha` participates without being compared directly: a
    /// verdict produced under a sibling branch's definition of the check hashes
    /// differently and simply does not match.
    pub input_hash: String,
    pub verdict_platforms: Vec<String>,
    pub verdict_environment_hash: String,
}

/// Whether the store holds an authoritative verdict for this coordinate.
///
/// The reuse guard supplies `reusable`, `complete`, `failure_kind IS NULL`,
/// the declared platform set, `result_schema_version`, `runner_build_id`, and
/// `verdict_environment_hash`. Exactly ONE condition is added on top, and the
/// content identity is not it.
///
/// ## Why there is no whole-tree comparison
///
/// The plan asked for a match on `tree_hash` rather than `commit_sha`, so that
/// a rebase does not invalidate a verdict. `input_hash` already delivers that
/// purpose and more, and comparing `tree_hash` on top of it is wrong twice over.
///
/// `input_hash` IS the content identity: [`check_result_key`] hashes the
/// check's impact-matched tree entries, its command, platform, and toolchain,
/// falling back to whole-tree keying for a check that declares no inputs. It is
/// computed here from the GATED REF'S OWN definition, which is how
/// `defined_by_commit_sha` participates — a verdict produced under a sibling
/// branch's definition of the check hashes differently and cannot match.
/// Matching it survives a rebase, and also survives a commit that touched none
/// of the check's inputs, which is the whole reason input-hash keying exists.
///
/// `check_result_cache.tree_hash` would not have meant what it looks like it
/// means. The table's conflict key is
/// `(project_id, check_name, input_hash, environment_fingerprint, result_schema_version)`
/// — `tree_hash` is NOT in it, and every evaluation re-stamps the column so the
/// tree-keyed `/checks` listing still shows the check at the current head. So
/// the field records the most recent tree a verdict was STAMPED ONTO, not the
/// tree it was produced on. Comparing it would have held a gate forever the
/// first time an unrelated evaluation of the same inputs re-stamped the row
/// (the false negative), while a cache-hit re-stamp onto the gated tree would
/// have passed the comparison without proving anything (the false positive).
pub(crate) fn check_verdict_evidence(
    db: Arc<LocalDb>,
    coordinate: &CheckVerdictCoordinate,
    expected: Option<&Value>,
) -> Result<GateOutcome, String> {
    let Some(reused) = get_reusable_check_result(
        db,
        ReusableCheckLookup {
            project_id: &coordinate.project_id,
            check_name: &coordinate.check_name,
            input_hash: &coordinate.input_hash,
            verdict_platforms: &coordinate.verdict_platforms,
            executor_ids: None,
            implementation_identity: cairn_checkenv::implementation_identity(),
            verdict_environment_hash: &coordinate.verdict_environment_hash,
            result_schema_version: CHECK_RESULT_SCHEMA_VERSION as i64,
        },
    )?
    else {
        return Ok(GateOutcome::NoEvidence);
    };
    if selection_is_narrowed(&reused.entry) {
        return Ok(GateOutcome::NoEvidence);
    }
    let verdict = if reused.entry.passed {
        "passed"
    } else {
        "failed"
    };
    Ok(match expected {
        None if reused.entry.passed => GateOutcome::Satisfied,
        None => GateOutcome::Refuted,
        Some(expected) if value_matches(&Value::String(verdict.into()), expected) => {
            GateOutcome::Satisfied
        }
        Some(_) => GateOutcome::Refuted,
    })
}

/// Whether the stored parse ASSERTS the run covered less than the whole check.
///
/// A reusable green over a `vitest related` selection is honest, and it is not
/// "this check is green on this ref" — so `filtered` and `empty` are refused.
/// So is `fast`: the rust-tests fast tier deferred every test that ran past its
/// per-test budget, so its green never speaks for the whole suite.
///
/// It is refusal by ASSERTION rather than a requirement that the row claim
/// `full`, and the store is why. Only nextest reports a selection kind at all;
/// tsc records `unknown` and every non-parsed lane records no
/// `target_results_json` whatsoever. Across the whole result store there is not
/// one reusable PASSING row carrying `full` — every `full` row is a rust-tests
/// red. Requiring `full` would therefore have foreclosed all 28 lanes forever,
/// the same shape of error as the `EXISTS(check_test_results)` guard this
/// deliberately does not carry: a green with zero per-test rows is the ordinary
/// case (rust-fmt alone has thousands), and `reusable = 1` already encodes
/// non-empty selection where it is meaningful, since an empty selection forces
/// `reusable = 0`.
///
/// KNOWN RESIDUAL: toolchain drift. `toolchain_fingerprint` is recorded and not
/// enforced by the reuse guard (cairn/4727), so a verdict produced under a
/// different toolchain than the gated ref would run under still counts.
fn selection_is_narrowed(entry: &CheckResultCacheEntry) -> bool {
    entry
        .target_results_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<ParsedCheckResult>(json).ok())
        .is_some_and(|parsed| matches!(parsed.selection.as_str(), "filtered" | "empty" | "fast"))
}

/// Resolve the gated ref and compute the check identity a verdict on it must
/// have. `None` whenever the coordinate cannot be established, which the caller
/// reads as `NoEvidence`.
async fn check_verdict_coordinate(
    ctx: &GateContext<'_>,
    issue_uri: &str,
    check_name: &str,
) -> Option<CheckVerdictCoordinate> {
    let (project_id, repository, branch) = gated_ref(ctx, issue_uri).await.ok().flatten()?;
    let repository = PathBuf::from(repository);
    let jj_binary_path = ctx.orch().jj_binary_path.clone();
    let config_dir = ctx.orch().config_dir.clone();
    let coordinate_repository = {
        let jj_binary_path = jj_binary_path.clone();
        let config_dir = config_dir.clone();
        let repository = repository.clone();
        tokio::task::spawn_blocking(move || {
            let jj = JjEnv::resolve(&jj_binary_path, &config_dir);
            crate::jj::coordinate_repository(&jj, &config_dir, &repository)
        })
        .await
        .ok()?
        .ok()?
    };
    let commit = cairn_vcs::resolve_coordinate(&coordinate_repository, &branch)
        .await
        .ok()?;
    let CommitChecksContract {
        contract: ChecksContract {
            checks,
            extra_inputs,
        },
        ..
    } = load_checks_contract_at_commit(&repository, &commit).await?;
    let check = checks.get(check_name)?;

    let jj = JjEnv::resolve(&jj_binary_path, &config_dir);
    let tree_hash = crate::jj::logical_tree_hash(&jj, &repository, &commit).ok()?;
    let entries = crate::jj::tree_entries(&jj, &repository, &commit).ok();
    let blobs = TreeBlobs {
        jj: &jj,
        repository: &repository,
    };
    let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
    let inputs = ResolvedInputs::resolve(&checks, &extra_inputs, &snapshot);
    let input_hash = check_result_key(
        check,
        inputs.for_check(check_name),
        entries.as_deref(),
        &tree_hash,
        &check_platform_identity(),
        check_toolchain_identity(),
    );
    Some(CheckVerdictCoordinate {
        project_id,
        check_name: check_name.to_string(),
        input_hash,
        verdict_platforms: verdict_platforms(check),
        verdict_environment_hash: cairn_checkenv::verdict_environment_hash(
            verdict_environment_names(check),
            |name| std::env::var(name).ok(),
        ),
    })
}

/// The issue's project, repository, and working branch — the ref a check verdict
/// on that issue would be about. The newest branched job wins, which is the same
/// branch every other surface calls "the issue's branch".
async fn gated_ref(
    ctx: &GateContext<'_>,
    issue_uri: &str,
) -> DbResult<Option<(String, String, String)>> {
    let uri = issue_uri.to_string();
    ctx.db_for_issue(issue_uri)
        .await
        .read(move |conn| {
            let uri = uri.clone();
            Box::pin(async move {
                let Some(issue) = resolve_issue_uri(conn, &uri).await? else {
                    return Ok(None);
                };
                let mut rows = conn
                    .query(
                        "SELECT i.project_id, p.repo_path, j.branch
                           FROM issues i
                           JOIN projects p ON p.id = i.project_id
                           JOIN jobs j ON j.issue_id = i.id AND j.branch IS NOT NULL
                          WHERE i.id = ?1
                          ORDER BY j.created_at DESC, j.rowid DESC
                          LIMIT 1",
                        params![issue.issue_id.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                let (Some(repository), Some(branch)) = (row.opt_text(1)?, row.opt_text(2)?) else {
                    return Ok(None);
                };
                Ok(Some((row.text(0)?, repository, branch)))
            })
        })
        .await
}

/// `{fact: "compiles", project: "cairn", ref: "main"}`
///
/// Whether the tree a ref names now builds, read from the compile fact
/// (`execution::compile_facts`), so "main compiles" is a gate on that fact and
/// never on the issue number of whatever repair is in flight. A `builds` row
/// for the ref's current tree satisfies it and a `does-not-build` row refutes
/// it; a tree nothing has compiled, or only failed to compile, is quiet. The
/// landing compile gate writes a `builds` row for every merge result it lands,
/// so the default branch's tip carries one after every gated landing.
///
/// Never forecloses: a ref that does not build is repaired by its next commit.
#[derive(Debug, Clone, Copy)]
pub struct CompilesFact;

const COMPILES_FIELDS: &[(&str, FieldKind)] =
    &[("project", FieldKind::Scalar), ("ref", FieldKind::Scalar)];

/// The project key a `compiles` clause names, as a bare key or a
/// `cairn://p/KEY` URI.
fn compiles_project(clause: &TriggerClause) -> Result<String, String> {
    let raw = clause_str(clause, "project").unwrap_or_default().trim();
    let key = raw
        .strip_prefix("cairn://p/")
        .unwrap_or(raw)
        .trim_end_matches('/');
    if key.is_empty() || key.contains('/') {
        return Err(format!(
            "compiles needs a 'project' naming one project, as a key or cairn://p/KEY; got {raw:?}"
        ));
    }
    Ok(key.to_string())
}

#[async_trait]
impl GateFact for CompilesFact {
    fn fact(&self) -> &'static str {
        "compiles"
    }

    fn fields(&self) -> &'static [(&'static str, FieldKind)] {
        COMPILES_FIELDS
    }

    fn example(&self) -> TriggerClause {
        BTreeMap::from([
            ("fact".into(), Value::String(self.fact().into())),
            ("project".into(), Value::String("cairn".into())),
            ("ref".into(), Value::String("main".into())),
        ])
    }

    fn validate(&self, clause: &TriggerClause) -> Result<(), String> {
        compiles_project(clause)?;
        if clause_str(clause, "ref")
            .unwrap_or_default()
            .trim()
            .is_empty()
        {
            return Err("compiles needs a 'ref' naming a branch or commit".into());
        }
        Ok(())
    }

    async fn resolve(
        &self,
        ctx: &GateContext<'_>,
        clause: &TriggerClause,
    ) -> DbResult<GateOutcome> {
        let project = compiles_project(clause).map_err(DbError::Row)?;
        let reference = clause_str(clause, "ref")
            .ok_or_else(|| DbError::Row("compiles needs a 'ref' field".into()))?
            .trim()
            .to_string();
        let db = ctx.orch().db.for_project(&project).await;
        // An unknown project or a ref that does not resolve is an absence of
        // evidence, never a failure: level resolution must be total.
        let Some((project_id, tree_hash)) =
            compiles_coordinate(ctx, &db, &project, &reference).await
        else {
            return Ok(GateOutcome::NoEvidence);
        };
        let verdict =
            crate::execution::compile_facts::verdict_for_tree(&db, &project_id, &tree_hash)
                .await
                .map_err(DbError::internal)?;
        Ok(match verdict.map(|fact| fact.outcome) {
            Some(crate::execution::compile_facts::CompileOutcome::Builds) => GateOutcome::Satisfied,
            Some(_) => GateOutcome::Refuted,
            None => GateOutcome::NoEvidence,
        })
    }

    async fn foreclosed(
        &self,
        _ctx: &GateContext<'_>,
        _clause: &TriggerClause,
    ) -> DbResult<Option<String>> {
        Ok(None)
    }

    fn render(&self, clause: &TriggerClause) -> String {
        format!(
            "{} compiles in {}",
            clause_str(clause, "ref").unwrap_or("?"),
            clause_str(clause, "project").unwrap_or("?")
        )
    }
}

/// The project id and the git tree id of what `reference` names in it now.
async fn compiles_coordinate(
    ctx: &GateContext<'_>,
    db: &LocalDb,
    project_key: &str,
    reference: &str,
) -> Option<(String, String)> {
    let key = project_key.to_string();
    let (project_id, repository) = db
        .read(move |conn| {
            let key = key.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, repo_path FROM projects WHERE lower(key) = lower(?1) LIMIT 1",
                        params![key.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(match row.opt_text(1)? {
                    Some(repository) => Some((row.text(0)?, repository)),
                    None => None,
                })
            })
        })
        .await
        .ok()
        .flatten()?;
    let repository = PathBuf::from(repository);
    let jj_binary_path = ctx.orch().jj_binary_path.clone();
    let config_dir = ctx.orch().config_dir.clone();
    let coordinate_repository = {
        let jj_binary_path = jj_binary_path.clone();
        let config_dir = config_dir.clone();
        let repository = repository.clone();
        tokio::task::spawn_blocking(move || {
            let jj = JjEnv::resolve(&jj_binary_path, &config_dir);
            crate::jj::coordinate_repository(&jj, &config_dir, &repository)
        })
        .await
        .ok()?
        .ok()?
    };
    let commit = cairn_vcs::resolve_coordinate(&coordinate_repository, reference)
        .await
        .ok()?;
    let jj = JjEnv::resolve(&jj_binary_path, &config_dir);
    let tree_hash = crate::jj::logical_tree_hash(&jj, &repository, &commit).ok()?;
    Some((project_id, tree_hash))
}

/// A fact source that does not exist in the shipped vocabulary, standing in for
/// the deliberately open `decision` arm. Shared with the registry tests, which
/// assert that registering it moves the shared authoring surface too.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::execution::cache::{FreshCheckObservationWrite, SealedCheckCoordinate};

    /// A genuine reusable green in the rust-fmt SHAPE: no parse and no per-test
    /// rows at all. That population is the overwhelming majority of the result
    /// store, and getting it wrong is how a verdict guard forecloses every lane.
    pub(crate) fn green_observation(
        id: &str,
        project_id: &str,
        input: &str,
        tree: &str,
        environment: &str,
    ) -> FreshCheckObservationWrite {
        FreshCheckObservationWrite {
            sealed_coordinate: SealedCheckCoordinate::new(
                "commit-gated",
                "commit-gated",
                tree,
                input,
            ),
            id: id.to_string(),
            public_handle: format!("{id:0<24}"),
            infra_reset_scopes: Vec::new(),
            selection: None,
            deferred_count: 0,
            project_id: project_id.to_string(),
            commit_sha: "commit-gated".to_string(),
            defined_by_commit_sha: "commit-gated".to_string(),
            tree_hash: tree.to_string(),
            check_name: "rust-fmt".to_string(),
            input_hash: input.to_string(),
            environment_fingerprint: environment.to_string(),
            verdict_platform: Some("linux".to_string()),
            verdict_arch: Some("x86_64".to_string()),
            verdict_environment_hash: Some(environment.to_string()),
            exit_code: 0,
            verdict: "passed".to_string(),
            failure_kind: None,
            complete: true,
            reusable: true,
            non_reusable_reason: None,
            parser_version: 1,
            result_schema_version: CHECK_RESULT_SCHEMA_VERSION as i64,
            ran_at: 100,
            duration_ms: 50,
            job_id: None,
            run_id: None,
            cadence: "write".to_string(),
            executor_id: None,
            executor_device_id: None,
            executor_connection_generation: None,
            executor_cell_id: None,
            executor_lease_epoch: None,
            executor_started_at_unix_ms: None,
            executor_finished_at_unix_ms: None,
            runner_build_id: Some(cairn_checkenv::implementation_identity().to_string()),
            toolchain_fingerprint: Some("tools".to_string()),
            output_tail: "ok".to_string(),
            target_results_json: None,
            tests: Vec::new(),
        }
    }

    #[derive(Debug)]
    pub(crate) struct DecisionFact;

    const DECISION_FIELDS: &[(&str, FieldKind)] = &[
        ("decision", FieldKind::Scalar),
        ("state", FieldKind::Scalar),
    ];

    #[async_trait]
    impl GateFact for DecisionFact {
        fn fact(&self) -> &'static str {
            "decision"
        }

        fn fields(&self) -> &'static [(&'static str, FieldKind)] {
            DECISION_FIELDS
        }

        fn example(&self) -> TriggerClause {
            BTreeMap::from([
                ("fact".into(), Value::String("decision".into())),
                ("decision".into(), Value::from(12)),
                ("state".into(), Value::String("ratified".into())),
            ])
        }

        fn validate(&self, _clause: &TriggerClause) -> Result<(), String> {
            Ok(())
        }

        async fn resolve(
            &self,
            _ctx: &GateContext<'_>,
            clause: &TriggerClause,
        ) -> DbResult<GateOutcome> {
            Ok(match clause_str(clause, "state") {
                Some("ratified") => GateOutcome::Satisfied,
                _ => GateOutcome::NoEvidence,
            })
        }

        async fn foreclosed(
            &self,
            _ctx: &GateContext<'_>,
            _clause: &TriggerClause,
        ) -> DbResult<Option<String>> {
            Ok(None)
        }

        fn render(&self, clause: &TriggerClause) -> String {
            format!(
                "decision {} {}",
                clause
                    .get("decision")
                    .map(scalar_phrase)
                    .unwrap_or_default(),
                field_phrase(clause, "state", "decided")
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::DecisionFact;
    use super::*;
    use crate::db::DbState;
    use crate::execution::cache::{
        record_fresh_check_observation, FreshCheckObservationWrite, SealedCheckCoordinate,
    };
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{migrated_test_db, SearchIndex};

    const TREE: &str = "tree-gated";
    const INPUT: &str = "input-gated";
    const ENVIRONMENT: &str = "env-gated";

    fn clause(pairs: &[(&str, Value)]) -> TriggerClause {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    fn text(value: &str) -> Value {
        Value::String(value.to_string())
    }

    async fn gate_env(name: &str) -> (Orchestrator, Arc<LocalDb>) {
        let db = Arc::new(migrated_test_db(name).await);
        let root = tempfile::tempdir().unwrap().keep();
        let state = Arc::new(DbState::new(
            db.clone(),
            Arc::new(SearchIndex::open_or_create(root.join("search.db")).unwrap()),
        ));
        let orch = Orchestrator::builder(state, Arc::new(TestServicesBuilder::new().build()), root)
            .build();
        db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p1', 'default', 'Cairn', 'CAIRN', '/tmp/cairn', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES ('i1', 'p1', 123, 'Target', 'active', 'active', 'none', 1, 1);
            ",
        )
        .await
        .unwrap();
        (orch, db)
    }

    async fn set_issue_status(db: &LocalDb, status: &str) {
        db.execute(
            "UPDATE issues SET status = ?1 WHERE id = 'i1'",
            (status.to_string(),),
        )
        .await
        .unwrap();
    }

    fn issue_clause(fact: &str, extra: &[(&str, Value)]) -> TriggerClause {
        let mut pairs = vec![("fact", text(fact)), ("issue", text("cairn://p/CAIRN/123"))];
        pairs.extend(extra.iter().cloned());
        clause(&pairs)
    }

    // -- authoring refusals ------------------------------------------------

    #[test]
    fn an_unknown_fact_is_refused_by_name_and_names_the_whole_vocabulary() {
        let vocabulary = GateVocabulary::default();
        let error = vocabulary
            .validate(&clause(&[
                ("fact", text("decision")),
                ("decision", Value::from(12)),
                ("state", text("ratified")),
            ]))
            .unwrap_err();
        assert_eq!(
            error,
            "unknown gate fact 'decision' \u{2014} known: issue_resolution, pr_state, \
             artifact_confirmed, check_verdict, clock, tenet_verdict, compiles"
        );
    }

    #[test]
    fn every_refusal_shows_the_corrected_object() {
        let vocabulary = GateVocabulary::default();
        let cases = [
            // An unknown field.
            clause(&[
                ("fact", text("issue_resolution")),
                ("issue", text("cairn://p/CAIRN/123")),
                ("resolutions", text("merged")),
            ]),
            // An unparseable URI.
            clause(&[
                ("fact", text("issue_resolution")),
                ("issue", text("CAIRN-123")),
            ]),
            // A value outside the closed set.
            issue_clause("pr_state", &[("state", text("squashed"))]),
            // A malformed timestamp.
            clause(&[("fact", text("clock")), ("notBefore", text("tomorrow-ish"))]),
            // A missing required field.
            clause(&[("fact", text("clock"))]),
        ];
        for case in cases {
            let error = vocabulary.validate(&case).unwrap_err();
            let corrected = error
                .split_once("Corrected: ")
                .unwrap_or_else(|| panic!("{error} shows no corrected object"))
                .1;
            let corrected: TriggerClause = serde_json::from_str(corrected)
                .unwrap_or_else(|_| panic!("{error} shows unparseable JSON"));
            vocabulary.validate(&corrected).unwrap_or_else(|why| {
                panic!("the correction offered by {error} is itself refused: {why}")
            });
        }
    }

    #[test]
    fn a_correction_keeps_the_fields_the_author_got_right() {
        let error = GateVocabulary::default()
            .validate(&clause(&[
                ("fact", text("check_verdict")),
                ("check", text("rust-tests")),
                ("issue", text("not-a-uri")),
                ("verdict", text("passed")),
            ]))
            .unwrap_err();
        assert!(
            error.contains("\"check\":\"rust-tests\""),
            "the author's good field survives the correction: {error}"
        );
    }

    #[test]
    fn gate_clauses_have_no_presence_or_prefix_forms() {
        let vocabulary = GateVocabulary::default();
        for key in ["presence", "issuePrefix"] {
            let error = vocabulary
                .validate(&issue_clause("issue_resolution", &[(key, text("x"))]))
                .unwrap_err();
            assert!(error.contains("unknown field"), "{key}: {error}");
        }
    }

    #[test]
    fn the_documented_vocabulary_table_validates_as_written() {
        let vocabulary = GateVocabulary::default();
        for case in [
            issue_clause("issue_resolution", &[("resolution", text("merged"))]),
            issue_clause("issue_resolution", &[]),
            issue_clause(
                "issue_resolution",
                &[("resolution", Value::from(vec!["merged", "closed"]))],
            ),
            issue_clause("pr_state", &[("state", text("merged"))]),
            issue_clause("artifact_confirmed", &[("artifact", text("plan"))]),
            issue_clause(
                "check_verdict",
                &[("check", text("rust-tests")), ("verdict", text("passed"))],
            ),
            clause(&[
                ("fact", text("clock")),
                ("notBefore", text("2026-09-05T12:00:00Z")),
            ]),
            clause(&[
                ("fact", text("tenet_verdict")),
                ("tenet", text("cairn://p/CAIRN/tenets/12")),
                ("outcome", text("answered")),
            ]),
            clause(&[
                ("fact", text("compiles")),
                ("project", text("cairn")),
                ("ref", text("main")),
            ]),
            clause(&[
                ("fact", text("compiles")),
                ("project", text("cairn://p/cairn")),
                ("ref", text("main")),
            ]),
        ] {
            vocabulary
                .validate(&case)
                .unwrap_or_else(|why| panic!("{case:?} is refused: {why}"));
        }
    }

    // -- issue_resolution ---------------------------------------------------

    #[tokio::test]
    async fn issue_resolution_bears_no_value_until_the_issue_resolves() {
        let (orch, db) = gate_env("gate-issue-live.db").await;
        let ctx = GateContext::new(&orch, 0);
        let merged = issue_clause("issue_resolution", &[("resolution", text("merged"))]);

        for live in ["backlog", "active", "waiting", "complete", "failed"] {
            set_issue_status(&db, live).await;
            assert_eq!(
                IssueResolutionFact.resolve(&ctx, &merged).await.unwrap(),
                GateOutcome::NoEvidence,
                "{live} is not a resolution, so nothing bears on the gate yet"
            );
            assert_eq!(
                IssueResolutionFact.foreclosed(&ctx, &merged).await.unwrap(),
                None
            );
        }
    }

    #[tokio::test]
    async fn issue_resolution_satisfies_refutes_and_forecloses() {
        let (orch, db) = gate_env("gate-issue-resolved.db").await;
        let ctx = GateContext::new(&orch, 0);
        let merged = issue_clause("issue_resolution", &[("resolution", text("merged"))]);

        set_issue_status(&db, "merged").await;
        assert_eq!(
            IssueResolutionFact.resolve(&ctx, &merged).await.unwrap(),
            GateOutcome::Satisfied
        );
        assert_eq!(
            IssueResolutionFact.foreclosed(&ctx, &merged).await.unwrap(),
            None
        );

        set_issue_status(&db, "closed").await;
        assert_eq!(
            IssueResolutionFact.resolve(&ctx, &merged).await.unwrap(),
            GateOutcome::Refuted,
            "closed without merging is evidence against 'merged', not silence"
        );
        let reason = IssueResolutionFact
            .foreclosed(&ctx, &merged)
            .await
            .unwrap()
            .expect("a terminal resolution that does not match forecloses");
        assert!(reason.contains("closed"), "{reason}");
        assert!(reason.contains("merged"), "{reason}");
    }

    #[tokio::test]
    async fn issue_resolution_reuses_the_shared_membership_matcher() {
        let (orch, db) = gate_env("gate-issue-array.db").await;
        let ctx = GateContext::new(&orch, 0);
        let either = issue_clause(
            "issue_resolution",
            &[("resolution", Value::from(vec!["merged", "closed"]))],
        );
        let any = issue_clause("issue_resolution", &[]);

        for resolution in ["merged", "closed"] {
            set_issue_status(&db, resolution).await;
            assert_eq!(
                IssueResolutionFact.resolve(&ctx, &either).await.unwrap(),
                GateOutcome::Satisfied
            );
            assert_eq!(
                IssueResolutionFact.resolve(&ctx, &any).await.unwrap(),
                GateOutcome::Satisfied,
                "an omitted field means any, over the values this fact can bear"
            );
            assert_eq!(
                IssueResolutionFact.foreclosed(&ctx, &either).await.unwrap(),
                None
            );
        }
    }

    #[tokio::test]
    async fn a_missing_issue_forecloses_rather_than_waiting_forever() {
        let (orch, _db) = gate_env("gate-issue-missing.db").await;
        let ctx = GateContext::new(&orch, 0);
        let gone = clause(&[
            ("fact", text("issue_resolution")),
            ("issue", text("cairn://p/CAIRN/999")),
            ("resolution", text("merged")),
        ]);
        assert_eq!(
            IssueResolutionFact.resolve(&ctx, &gone).await.unwrap(),
            GateOutcome::NoEvidence
        );
        assert!(IssueResolutionFact
            .foreclosed(&ctx, &gone)
            .await
            .unwrap()
            .is_some());
    }

    // -- pr_state -----------------------------------------------------------

    async fn set_pr_status(db: &LocalDb, status: &str) {
        db.execute(
            "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                                        target_branch, status, opened_at, updated_at)
             VALUES ('mr1', 'j1', 'p1', 'i1', 'PR', 'feature', 'main', ?1, 1, 1)
             ON CONFLICT(id) DO UPDATE SET status = excluded.status",
            (status.to_string(),),
        )
        .await
        .unwrap();
    }

    /// A deleted target forecloses every issue-scoped fact, while "no pull
    /// request yet" and an unreachable project stay silent.
    #[tokio::test]
    async fn a_deleted_target_forecloses_where_no_pr_yet_and_an_unseen_project_do_not() {
        let (orch, db) = gate_env("gate-deleted-target.db").await;
        let ctx = GateContext::new(&orch, 0);
        let pr = issue_clause("pr_state", &[("state", text("merged"))]);
        let artifact = issue_clause("artifact_confirmed", &[("artifact", text("plan"))]);
        let check = issue_clause(
            "check_verdict",
            &[("check", text("rust-tests")), ("verdict", text("passed"))],
        );
        let unseen = clause(&[
            ("fact", text("pr_state")),
            ("issue", text("cairn://p/ELSEWHERE/1")),
            ("state", text("merged")),
        ]);

        assert_eq!(
            PrStateFact.foreclosed(&ctx, &pr).await.unwrap(),
            None,
            "no PR yet"
        );
        assert_eq!(
            ArtifactConfirmedFact
                .foreclosed(&ctx, &artifact)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            CheckVerdictFact.foreclosed(&ctx, &check).await.unwrap(),
            None
        );
        assert_eq!(
            PrStateFact.foreclosed(&ctx, &unseen).await.unwrap(),
            None,
            "a project this host cannot see is silence"
        );

        db.execute("DELETE FROM issues WHERE id = 'i1'", ())
            .await
            .unwrap();
        assert_eq!(
            PrStateFact.resolve(&ctx, &pr).await.unwrap(),
            GateOutcome::NoEvidence
        );
        let gone = Some("cairn://p/cairn/123 no longer exists".to_string());
        assert_eq!(PrStateFact.foreclosed(&ctx, &pr).await.unwrap(), gone);
        assert_eq!(
            ArtifactConfirmedFact
                .foreclosed(&ctx, &artifact)
                .await
                .unwrap(),
            gone
        );
        assert_eq!(
            CheckVerdictFact.foreclosed(&ctx, &check).await.unwrap(),
            gone
        );
        assert_eq!(PrStateFact.foreclosed(&ctx, &unseen).await.unwrap(), None);
    }

    #[tokio::test]
    async fn pr_state_separates_not_yet_from_evidence_against() {
        let (orch, db) = gate_env("gate-pr-state.db").await;
        let ctx = GateContext::new(&orch, 0);
        let merged = issue_clause("pr_state", &[("state", text("merged"))]);

        assert_eq!(
            PrStateFact.resolve(&ctx, &merged).await.unwrap(),
            GateOutcome::NoEvidence,
            "no pull request at all is silence"
        );

        set_pr_status(&db, "open").await;
        assert_eq!(
            PrStateFact.resolve(&ctx, &merged).await.unwrap(),
            GateOutcome::NoEvidence,
            "an open PR has not answered the question yet"
        );
        assert_eq!(PrStateFact.foreclosed(&ctx, &merged).await.unwrap(), None);

        set_pr_status(&db, "merged").await;
        assert_eq!(
            PrStateFact.resolve(&ctx, &merged).await.unwrap(),
            GateOutcome::Satisfied
        );

        set_pr_status(&db, "closed").await;
        assert_eq!(
            PrStateFact.resolve(&ctx, &merged).await.unwrap(),
            GateOutcome::Refuted
        );
        assert!(PrStateFact
            .foreclosed(&ctx, &merged)
            .await
            .unwrap()
            .is_some());
    }

    // -- artifact_confirmed -------------------------------------------------

    #[tokio::test]
    async fn artifact_confirmed_waits_and_never_forecloses_while_its_issue_exists() {
        let (orch, db) = gate_env("gate-artifact.db").await;
        let ctx = GateContext::new(&orch, 0);
        let plan = issue_clause("artifact_confirmed", &[("artifact", text("plan"))]);
        db.execute_script(
            "
            INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
             VALUES ('j1', 'p1', 'i1', 'complete', 1, 1);
            INSERT INTO artifacts(id, job_id, artifact_type, data, confirmed, created_at, updated_at)
             VALUES ('a1', 'j1', 'plan', '{}', 0, 1, 1);
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            ArtifactConfirmedFact.resolve(&ctx, &plan).await.unwrap(),
            GateOutcome::NoEvidence,
            "an unconfirmed plan is not yet, never no"
        );
        assert_eq!(
            ArtifactConfirmedFact.foreclosed(&ctx, &plan).await.unwrap(),
            None
        );

        db.execute("UPDATE artifacts SET confirmed = 1 WHERE id = 'a1'", ())
            .await
            .unwrap();
        assert_eq!(
            ArtifactConfirmedFact.resolve(&ctx, &plan).await.unwrap(),
            GateOutcome::Satisfied
        );
        assert_eq!(
            ArtifactConfirmedFact
                .resolve(
                    &ctx,
                    &issue_clause("artifact_confirmed", &[("artifact", text("create-pr"))])
                )
                .await
                .unwrap(),
            GateOutcome::NoEvidence,
            "a confirmed plan says nothing about a confirmed PR"
        );
    }

    // -- clock --------------------------------------------------------------

    #[tokio::test]
    async fn clock_releases_on_time_feeds_the_sleep_bound_and_never_forecloses() {
        let (orch, _db) = gate_env("gate-clock.db").await;
        let at = clause(&[
            ("fact", text("clock")),
            ("notBefore", text("2026-09-05T12:00:00Z")),
        ]);
        let due = 1_788_609_600;
        assert_eq!(ClockFact.next_due_at(&at), Some(due));

        let early = GateContext::new(&orch, due - 1);
        assert_eq!(
            ClockFact.resolve(&early, &at).await.unwrap(),
            GateOutcome::NoEvidence
        );
        assert_eq!(ClockFact.foreclosed(&early, &at).await.unwrap(), None);

        let arrived = GateContext::new(&orch, due);
        assert_eq!(
            ClockFact.resolve(&arrived, &at).await.unwrap(),
            GateOutcome::Satisfied
        );
        assert_eq!(
            ClockFact.foreclosed(&arrived, &at).await.unwrap(),
            None,
            "every instant the clock waits for arrives"
        );
    }

    #[test]
    fn only_clock_answers_next_due_at() {
        for source in GateVocabulary::default().sources() {
            let due = source.next_due_at(&source.example());
            assert_eq!(
                due.is_some(),
                source.fact() == "clock",
                "{} reported {due:?}",
                source.fact()
            );
        }
    }

    // -- check_verdict ------------------------------------------------------

    /// The default fixture is the rust-fmt SHAPE: a genuine reusable green that
    /// persists no parse and no per-test rows at all. That population is the
    /// overwhelming majority of the result store, and getting it wrong is how a
    /// verdict guard forecloses every lane.
    fn observation(id: &str, input: &str) -> FreshCheckObservationWrite {
        super::test_support::green_observation(id, "p1", input, TREE, ENVIRONMENT)
    }

    fn reseal(mut observation: FreshCheckObservationWrite) -> FreshCheckObservationWrite {
        observation.sealed_coordinate = SealedCheckCoordinate::new(
            observation.commit_sha.clone(),
            observation.defined_by_commit_sha.clone(),
            observation.tree_hash.clone(),
            observation.input_hash.clone(),
        );
        observation
    }

    fn coordinate(input: &str) -> CheckVerdictCoordinate {
        CheckVerdictCoordinate {
            project_id: "p1".to_string(),
            check_name: "rust-fmt".to_string(),
            input_hash: input.to_string(),
            verdict_platforms: vec!["linux".to_string()],
            verdict_environment_hash: ENVIRONMENT.to_string(),
        }
    }

    fn parse_json(selection: &str) -> String {
        format!(
            "{{\"schemaVersion\":1,\"complete\":true,\"selection\":\"{selection}\",\"tests\":[],\"undeclaredSkips\":0,\"parser\":\"nextest\",\"passed\":96,\"failed\":0,\"suiteFailures\":0,\"skipped\":0,\"failures\":[]}}"
        )
    }

    fn passed() -> Value {
        text("passed")
    }

    #[tokio::test]
    async fn a_lane_that_persists_no_per_test_rows_still_satisfies_a_check_gate() {
        // The regression test. An `EXISTS(check_test_results)` guard, or a
        // requirement that the row claim selection `full`, would have foreclosed
        // this shape — and with it every lane but nextest — permanently.
        let (_orch, db) = gate_env("gate-check-rustfmt.db").await;
        record_fresh_check_observation(db.clone(), observation("obs-fmt", INPUT)).unwrap();
        assert_eq!(
            check_verdict_evidence(db, &coordinate(INPUT), Some(&passed())).unwrap(),
            GateOutcome::Satisfied
        );
    }

    #[tokio::test]
    async fn an_unnarrowed_parse_still_satisfies_a_check_gate() {
        // tsc and every non-nextest parser record `unknown`; not one reusable
        // PASSING row in the whole store carries `full`.
        let (_orch, db) = gate_env("gate-check-unknown.db").await;
        for (id, input, selection) in [
            ("obs-unknown", "input-unknown", "unknown"),
            ("obs-full", "input-full", "full"),
        ] {
            let mut observed = observation(id, input);
            observed.target_results_json = Some(parse_json(selection));
            record_fresh_check_observation(db.clone(), reseal(observed)).unwrap();
            assert_eq!(
                check_verdict_evidence(db.clone(), &coordinate(input), Some(&passed())).unwrap(),
                GateOutcome::Satisfied,
                "a {selection} selection is not a narrowed one"
            );
        }
    }

    #[tokio::test]
    async fn a_passed_row_that_is_not_a_verdict_never_satisfies_a_check_gate() {
        let (_orch, db) = gate_env("gate-check-discipline.db").await;

        // Non-reusable.
        let mut not_reusable = observation("obs-not-reusable", "input-not-reusable");
        not_reusable.reusable = false;
        not_reusable.non_reusable_reason = Some("test selection was empty".to_string());
        record_fresh_check_observation(db.clone(), reseal(not_reusable)).unwrap();

        // Carries a failure kind.
        let mut abnormal = observation("obs-failure-kind", "input-failure-kind");
        abnormal.failure_kind = Some("timed_out".to_string());
        record_fresh_check_observation(db.clone(), reseal(abnormal)).unwrap();

        // Ran on a platform the project does not gate on.
        let mut elsewhere = observation("obs-platform", "input-platform");
        elsewhere.verdict_platform = Some("windows".to_string());
        record_fresh_check_observation(db.clone(), reseal(elsewhere)).unwrap();

        // Incomplete: the producer never enumerated its selected manifest.
        let mut incomplete = observation("obs-incomplete", "input-incomplete");
        incomplete.complete = false;
        record_fresh_check_observation(db.clone(), reseal(incomplete)).unwrap();

        // A narrowed, related-mode selection: honest, and not "green on this ref".
        let mut filtered = observation("obs-filtered", "input-filtered");
        filtered.target_results_json = Some(parse_json("filtered"));
        record_fresh_check_observation(db.clone(), reseal(filtered)).unwrap();

        // A fast-tier green: everything past the per-test budget was deferred.
        let mut fast = observation("obs-fast", "input-fast");
        fast.target_results_json = Some(parse_json("fast"));
        record_fresh_check_observation(db.clone(), reseal(fast)).unwrap();

        for input in [
            "input-not-reusable",
            "input-failure-kind",
            "input-platform",
            "input-incomplete",
            "input-filtered",
            "input-fast",
        ] {
            assert_eq!(
                check_verdict_evidence(db.clone(), &coordinate(input), Some(&passed())).unwrap(),
                GateOutcome::NoEvidence,
                "{input} is a passed row, not a verdict"
            );
        }
    }

    #[tokio::test]
    async fn a_rebase_does_not_invalidate_a_check_verdict() {
        let (_orch, db) = gate_env("gate-check-rebase.db").await;
        // A rebase moves the sha and leaves the content alone. The input hash is
        // content, not history, so the verdict still counts — which is the whole
        // point of "tree, not commit".
        let mut rebased = observation("obs-rebased", INPUT);
        rebased.commit_sha = "commit-rebased".to_string();
        rebased.defined_by_commit_sha = "commit-rebased".to_string();
        record_fresh_check_observation(db.clone(), reseal(rebased)).unwrap();
        assert_eq!(
            check_verdict_evidence(db.clone(), &coordinate(INPUT), Some(&passed())).unwrap(),
            GateOutcome::Satisfied
        );
    }

    #[tokio::test]
    async fn a_restamped_row_does_not_hold_the_gate_forever() {
        // The regression for the masked-verdict defect. `check_result_cache`'s
        // conflict key does not include `tree_hash`, and every evaluation
        // re-stamps the column, so ONE row carries whichever tree touched it
        // last. A guard that compared that column against the gated tree would
        // report NoEvidence forever the moment an unrelated evaluation of the
        // same inputs — a doc-only commit, exactly what input-hash keying exists
        // to serve — moved the stamp.
        let (_orch, db) = gate_env("gate-check-restamp.db").await;
        record_fresh_check_observation(db.clone(), observation("obs-at-gated-tree", INPUT))
            .unwrap();
        assert_eq!(
            check_verdict_evidence(db.clone(), &coordinate(INPUT), Some(&passed())).unwrap(),
            GateOutcome::Satisfied
        );

        // A later evaluation at a DIFFERENT whole tree, same input hash.
        let mut later = observation("obs-elsewhere", INPUT);
        later.tree_hash = "tree-doc-only-commit".to_string();
        later.commit_sha = "commit-doc-only".to_string();
        later.defined_by_commit_sha = "commit-doc-only".to_string();
        later.ran_at = 900;
        record_fresh_check_observation(db.clone(), reseal(later)).unwrap();

        let restamped = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT tree_hash, COUNT(*) FROM check_result_cache
                              WHERE input_hash = ?1 AND row_family = 'verdict'",
                            params![INPUT],
                        )
                        .await?;
                    let row = rows.next().await?.expect("the cache row survives");
                    Ok((row.text(0)?, row.i64(1)?))
                })
            })
            .await
            .unwrap();
        assert_eq!(
            restamped,
            ("tree-doc-only-commit".to_string(), 1),
            "one row, re-stamped onto the newer tree"
        );

        assert_eq!(
            check_verdict_evidence(db, &coordinate(INPUT), Some(&passed())).unwrap(),
            GateOutcome::Satisfied,
            "the verdict for these inputs is still authoritative after a re-stamp"
        );
    }

    #[tokio::test]
    async fn a_verdict_for_other_inputs_is_not_evidence_about_these() {
        // What actually discriminates content: the input hash, computed from the
        // gated ref's own check definition.
        let (_orch, db) = gate_env("gate-check-inputs.db").await;
        record_fresh_check_observation(db.clone(), observation("obs-other", "input-other"))
            .unwrap();
        assert_eq!(
            check_verdict_evidence(db, &coordinate(INPUT), Some(&passed())).unwrap(),
            GateOutcome::NoEvidence
        );
    }

    #[tokio::test]
    async fn a_red_verdict_refutes_a_green_gate_without_foreclosing_it() {
        let (orch, db) = gate_env("gate-check-red.db").await;
        let mut red = observation("obs-red", INPUT);
        red.exit_code = 1;
        red.verdict = "failed".to_string();
        record_fresh_check_observation(db.clone(), reseal(red)).unwrap();

        assert_eq!(
            check_verdict_evidence(db.clone(), &coordinate(INPUT), Some(&passed())).unwrap(),
            GateOutcome::Refuted
        );
        assert_eq!(
            check_verdict_evidence(db, &coordinate(INPUT), Some(&text("failed"))).unwrap(),
            GateOutcome::Satisfied
        );
        // A red is a fact about one tree. The next commit produces another.
        let ctx = GateContext::new(&orch, 0);
        let gate = issue_clause(
            "check_verdict",
            &[("check", text("rust-fmt")), ("verdict", text("passed"))],
        );
        assert_eq!(
            CheckVerdictFact.foreclosed(&ctx, &gate).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn an_unresolvable_gated_ref_is_silence_not_a_swept_failure() {
        let (orch, _db) = gate_env("gate-check-refless.db").await;
        let ctx = GateContext::new(&orch, 0);
        let gate = issue_clause(
            "check_verdict",
            &[("check", text("rust-fmt")), ("verdict", text("passed"))],
        );
        assert_eq!(
            CheckVerdictFact.resolve(&ctx, &gate).await.unwrap(),
            GateOutcome::NoEvidence,
            "an issue with no branch cannot poison the sweep"
        );
    }

    // -- extensibility ------------------------------------------------------

    #[tokio::test]
    async fn a_registered_fact_source_needs_no_migration() {
        let (orch, _db) = gate_env("gate-extensibility.db").await;
        let ctx = GateContext::new(&orch, 0);
        let predicate = clause(&[
            ("fact", text("decision")),
            ("decision", Value::from(12)),
            ("state", text("ratified")),
        ]);

        let mut vocabulary = GateVocabulary::default();
        assert!(vocabulary
            .validate(&predicate)
            .unwrap_err()
            .contains("unknown gate fact 'decision'"));

        vocabulary.register(Arc::new(DecisionFact));

        // A stored gate is an opaque canonicalized clause in one TEXT column, so
        // "storing it" is exactly this round trip: no schema change, no column,
        // no migration of predicates already written.
        let stored = serde_json::to_string(&predicate).unwrap();
        let loaded: TriggerClause = serde_json::from_str(&stored).unwrap();

        vocabulary.validate(&loaded).unwrap();
        assert_eq!(
            vocabulary.resolve(&ctx, &loaded).await.unwrap(),
            GateOutcome::Satisfied
        );
        assert_eq!(vocabulary.render(&loaded), "decision 12 ratified");
        assert_eq!(vocabulary.foreclosed(&ctx, &loaded).await.unwrap(), None);
        assert_eq!(vocabulary.next_due_at(&loaded), None);
    }

    #[test]
    fn registering_a_fact_twice_replaces_it_in_place() {
        let mut vocabulary = GateVocabulary::default();
        let before = vocabulary.fact_names();
        vocabulary.register(Arc::new(ClockFact));
        assert_eq!(vocabulary.fact_names(), before);
    }
}
