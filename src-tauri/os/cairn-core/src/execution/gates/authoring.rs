//! Authoring `after:` gates: turning a caller's clause objects into stored
//! predicates, and refusing what cannot be honoured.
//!
//! Three refusals live here, all at write time and before any row exists:
//!
//! - **Shape.** `after` is an array of clause OBJECTS. Prose (`"after cairn/4724
//!   merges"`) is the census rendering, never a grammar, so a string is refused
//!   with the clause object it should have been.
//! - **Vocabulary.** Every clause is validated through the shared
//!   [`crate::routes::FactRegistry`]'s gate vocabulary, whose refusals carry the
//!   corrected object.
//! - **Cycles.** A held execution on issue X gated on issue Y waits on Y; if Y's
//!   own held executions wait, transitively, on X, nothing in the cycle can ever
//!   start. Every unsatisfied clause that names an issue is an edge; a clause
//!   whose evidence is already in waits on nothing and is not.

use std::collections::{HashMap, HashSet};

use cairn_db::turso::Connection;
use serde_json::Value;

use super::vocabulary::{GateContext, GateOutcome, GateVocabulary};
use crate::issues::relations::{canonicalize_issue_uri, issue_uri_for_id};
use crate::routes::{FactRegistry, TriggerClause};
use crate::storage::{DbError, DbResult};

/// The facts whose clause names an issue the gated execution waits on: every
/// fact whose evidence that issue's own work produces (its resolution, its pull
/// request, an artifact its jobs confirm, a check verdict on its branch). Each
/// is a wait edge, so each can close a cycle.
///
/// `tenet_verdict` is deliberately absent here and from
/// [`SELF_PRODUCIBLE_FACTS`]: only the operator answers a tenet, so no
/// execution can produce that fact, its clause waits on no issue, and it can
/// close no wait cycle.
const ISSUE_EDGE_FACTS: &[&str] = &[
    "issue_resolution",
    "pr_state",
    "artifact_confirmed",
    "check_verdict",
];

/// The edge facts another execution of the SAME issue can satisfy ("build held
/// until this issue's plan is confirmed"). A self-edge on one of these is a
/// wait rather than a cycle while such a producer is live; a self-edge on any
/// other edge fact is always a cycle, since an issue cannot wait on its own
/// resolution or pull request.
const SELF_PRODUCIBLE_FACTS: &[&str] = &["artifact_confirmed", "check_verdict"];

/// Another execution on `?1` (other than `?2`) that can still produce evidence:
/// live, not finished, and not itself held, since a held execution produces
/// nothing.
const LIVE_PRODUCER_SQL: &str = concat!(
    "SELECT e.id FROM executions e WHERE e.issue_id = ?1 AND e.id != ?2 \
       AND e.status NOT IN ('complete', 'failed') AND ",
    cairn_db::live_execution_sql!(),
    " AND NOT (",
    cairn_db::gated_unreleased_sql!(),
    ") LIMIT 1"
);

/// The vocabulary `after:` clauses are validated and resolved through: the
/// shared registry's, so an author is offered exactly what a sweep can resolve.
pub fn gate_vocabulary() -> GateVocabulary {
    FactRegistry::default().gates().clone()
}

/// Parse an `after` payload value into clause objects.
///
/// `key` names the payload path in refusals (`payload.after`,
/// `payload.execution.after`). `null` is the same as absent.
pub fn parse_after(value: Option<&Value>, key: &str) -> Result<Vec<TriggerClause>, String> {
    let example = example_clause();
    let items = match value {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(Value::Array(items)) => items,
        Some(_) => {
            return Err(format!(
                "{key} must be an array of gate clause objects. Corrected: [{example}]"
            ))
        }
    };
    items
        .iter()
        .enumerate()
        .map(|(index, item)| match item {
            Value::Object(map) => Ok(map
                .iter()
                .map(|(field, value)| (field.clone(), value.clone()))
                .collect()),
            Value::String(prose) => Err(format!(
                "{key}[{index}] is prose (\"{prose}\"); clause objects are the only authoring \
                 form. Corrected: {example}"
            )),
            _ => Err(format!(
                "{key}[{index}] must be a gate clause object. Corrected: {example}"
            )),
        })
        .collect()
}

fn example_clause() -> String {
    gate_vocabulary()
        .sources()
        .first()
        .map(|source| serde_json::to_string(&source.example()).unwrap_or_default())
        .unwrap_or_default()
}

/// Validate every clause and return each one's canonical stored form, in
/// authored order with exact duplicates dropped.
///
/// Canonical means: the `issue` and `tenet` URIs canonicalized (so
/// `cairn://p/CAIRN/12` and `cairn://p/cairn/12` are one predicate, and cycle
/// detection, gate patching, and a tenet's lookup of the gates naming it
/// compare like with like) and keys in sorted order, which a `BTreeMap`
/// serializes to by construction.
pub fn canonical_predicates(clauses: &[TriggerClause], key: &str) -> Result<Vec<String>, String> {
    let vocabulary = gate_vocabulary();
    let mut seen = HashSet::new();
    let mut predicates = Vec::with_capacity(clauses.len());
    for (index, clause) in clauses.iter().enumerate() {
        vocabulary
            .validate(clause)
            .map_err(|error| format!("{key}[{index}]: {error}"))?;
        let mut canonical = clause.clone();
        if let Some(Value::String(issue)) = clause.get("issue") {
            let issue = canonicalize_issue_uri(issue)
                .map_err(|error| format!("{key}[{index}].issue: {error}"))?;
            canonical.insert("issue".into(), Value::String(issue));
        }
        if let Some(Value::String(tenet)) = clause.get("tenet") {
            let tenet = super::vocabulary::canonicalize_tenet_uri(tenet)
                .map_err(|error| format!("{key}[{index}].tenet: {error}"))?;
            canonical.insert("tenet".into(), Value::String(tenet));
        }
        let json = serde_json::to_string(&canonical)
            .map_err(|error| format!("{key}[{index}] is not serializable: {error}"))?;
        if seen.insert(json.clone()) {
            predicates.push(json);
        }
    }
    Ok(predicates)
}

/// The issue a stored predicate makes its execution wait on, when its fact is
/// one that names an issue.
fn waited_issue(predicate_json: &str) -> Option<String> {
    waited_edge(predicate_json).map(|(_, issue)| issue)
}

/// The fact and canonical issue of a wait edge.
fn waited_edge(predicate_json: &str) -> Option<(String, String)> {
    let clause: TriggerClause = serde_json::from_str(predicate_json).ok()?;
    let fact = clause.get("fact")?.as_str()?;
    if !ISSUE_EDGE_FACTS.contains(&fact) {
        return None;
    }
    let issue = canonicalize_issue_uri(clause.get("issue")?.as_str()?).ok()?;
    Some((fact.to_string(), issue))
}

/// The wait-edge predicates among `predicates` whose evidence already says yes.
///
/// A satisfied predicate waits on nothing: the creation-time sweep releases it
/// at once, so it cannot close a cycle and needs no producer. Resolved outside
/// the filing transaction, because evidence reads route to other databases; the
/// caller latches each satisfied gate in that transaction, so evidence that goes
/// away in between cannot strand the edge this observation let it skip.
pub async fn already_satisfied(
    orch: &crate::orchestrator::Orchestrator,
    predicates: &[String],
) -> HashSet<String> {
    let vocabulary = gate_vocabulary();
    let ctx = GateContext::new(orch, orch.services.clock.now());
    let mut satisfied = HashSet::new();
    for predicate in predicates {
        if waited_edge(predicate).is_none() {
            continue;
        }
        let Ok(clause) = serde_json::from_str::<TriggerClause>(predicate) else {
            continue;
        };
        if matches!(
            vocabulary.resolve(&ctx, &clause).await,
            Ok(GateOutcome::Satisfied)
        ) {
            satisfied.insert(predicate.clone());
        }
    }
    satisfied
}

fn self_wait_refusal(origin: &str) -> String {
    let issue = short_issue(origin);
    format!(
        "{issue} would wait on its own artifact or check verdict, but no other live execution \
         on {issue} could produce it (a superseded, finished, or held execution produces \
         nothing), so it would never start. Start or keep the producing execution first and \
         then file this one, or gate on a different issue."
    )
}

/// A path from `origin` back to itself through `edges`, entering through one of
/// `targets`, or `None` when the proposed edges close no cycle.
///
/// The returned path starts and ends at `origin`.
fn find_cycle(
    origin: &str,
    targets: &[String],
    edges: &HashMap<String, Vec<String>>,
) -> Option<Vec<String>> {
    let mut visited = HashSet::new();
    let mut stack: Vec<(String, Vec<String>)> = targets
        .iter()
        .rev()
        .map(|target| (target.clone(), vec![origin.to_string(), target.clone()]))
        .collect();
    while let Some((uri, path)) = stack.pop() {
        if uri == origin {
            return Some(path);
        }
        if !visited.insert(uri.clone()) {
            continue;
        }
        for next in edges.get(&uri).into_iter().flatten().rev() {
            let mut next_path = path.clone();
            next_path.push(next.clone());
            stack.push((next.clone(), next_path));
        }
    }
    None
}

/// `cairn://p/cairn/4736` as `cairn/4736`: the form a person writes and reads.
fn short_issue(uri: &str) -> &str {
    uri.strip_prefix("cairn://p/").unwrap_or(uri)
}

fn cycle_refusal(path: &[String]) -> String {
    let rendered: Vec<&str> = path.iter().map(|uri| short_issue(uri)).collect();
    format!(
        "{} \u{2014} an execution cannot wait on its own issue, directly or transitively.",
        rendered.join(" after ")
    )
}

/// Refuse `predicates` for a held execution on `issue_id` when they would close
/// a wait cycle, inside the caller's write transaction on `own_db`.
///
/// The edge set is every unsatisfied gate of every OTHER held execution
/// (`excluding` is the execution being patched, whose current gates the new
/// ones replace), across every database the walk reaches. A gate may name an
/// issue in any project, and a team project's held executions live only in its
/// replica, so each hop reads the database that owns the issue it lands on —
/// the same routing gate evidence uses. A cycle missed here would never
/// foreclose: it would hold every execution in it, silently, forever.
///
/// The filing's own database is read through `conn`, inside the transaction,
/// so two concurrent filings on ONE database cannot each pass and together
/// close a cycle. Other databases are read outside it (they are separate
/// stores; there is no transaction spanning them). RESIDUAL RACE: two filings
/// committing concurrently on DIFFERENT databases can still close a cycle
/// between them. That window is two writes on two replicas landing within one
/// check of each other, and it is accepted rather than locked against. A
/// project this host cannot see is a dead end for the walk, not an error.
pub async fn refuse_cycles_conn(
    conn: &Connection,
    dbs: &crate::db::DbState,
    own_db: &std::sync::Arc<crate::storage::LocalDb>,
    issue_id: &str,
    excluding: Option<&str>,
    predicates: &[String],
    satisfied: &HashSet<String>,
) -> DbResult<()> {
    // Satisfied evidence is no wait edge: the caller's fresh resolution, plus
    // whatever the execution being patched has already latched.
    let mut satisfied = satisfied.clone();
    if let Some(execution_id) = excluding {
        use crate::storage::RowExt;
        let mut rows = conn
            .query(
                "SELECT predicate_json FROM execution_gates
                  WHERE execution_id = ?1 AND satisfied_at IS NOT NULL",
                cairn_db::turso::params![execution_id],
            )
            .await?;
        while let Some(row) = rows.next().await? {
            satisfied.insert(row.text(0)?);
        }
    }
    let waits: Vec<(String, String)> = predicates
        .iter()
        .filter(|predicate| !satisfied.contains(*predicate))
        .filter_map(|p| waited_edge(p))
        .collect();
    if waits.is_empty() {
        return Ok(());
    }
    let origin = canonicalize_issue_uri(&issue_uri_for_id(conn, issue_id).await?)
        .map_err(DbError::Internal)?;

    let mut targets = Vec::with_capacity(waits.len());
    let mut waits_on_itself = false;
    for (fact, issue) in waits {
        if issue == origin && SELF_PRODUCIBLE_FACTS.contains(&fact.as_str()) {
            waits_on_itself = true;
        } else {
            targets.push(issue);
        }
    }
    if waits_on_itself
        && crate::storage::query_opt_text_conn(
            conn,
            LIVE_PRODUCER_SQL,
            cairn_db::turso::params![issue_id, excluding.unwrap_or("")],
        )
        .await?
        .is_none()
    {
        return Err(DbError::Internal(self_wait_refusal(&origin)));
    }
    if targets.is_empty() {
        return Ok(());
    }

    let mut edges = waiting_edges_conn(conn, excluding).await?;
    let mut loaded = vec![own_db.clone()];
    let mut frontier = targets.clone();
    let mut seen = HashSet::new();
    while let Some(uri) = frontier.pop() {
        if !seen.insert(uri.clone()) {
            continue;
        }
        if let Some(cairn_common::uri::CairnResource::Issue { project, .. }) =
            cairn_common::uri::parse_uri(&uri)
        {
            let db = dbs.for_project(&project).await;
            if !loaded
                .iter()
                .any(|known| std::sync::Arc::ptr_eq(known, &db))
            {
                let other = db
                    .read(|conn| Box::pin(async move { waiting_edges_conn(conn, None).await }))
                    .await?;
                for (waiting, targets) in other {
                    edges.entry(waiting).or_default().extend(targets);
                }
                loaded.push(db);
            }
        }
        frontier.extend(edges.get(&uri).into_iter().flatten().cloned());
    }

    match find_cycle(&origin, &targets, &edges) {
        Some(path) => Err(DbError::Internal(cycle_refusal(&path))),
        None => Ok(()),
    }
}

/// "Issue X waits on issue Y" for every unsatisfied gate of every held
/// execution in one database, keyed by X's canonical URI.
async fn waiting_edges_conn(
    conn: &Connection,
    excluding: Option<&str>,
) -> DbResult<HashMap<String, Vec<String>>> {
    let mut issue_uris: HashMap<String, Option<String>> = HashMap::new();
    let mut edges: HashMap<String, Vec<String>> = HashMap::new();
    for (waiting_issue_id, predicate_json) in
        cairn_workflow::gates::store::list_waiting_gate_predicates_conn(conn, excluding).await?
    {
        let Some(target) = waited_issue(&predicate_json) else {
            continue;
        };
        let waiting = match issue_uris.get(&waiting_issue_id) {
            Some(uri) => uri.clone(),
            None => {
                let uri = issue_uri_for_id(conn, &waiting_issue_id)
                    .await
                    .ok()
                    .and_then(|uri| canonicalize_issue_uri(&uri).ok());
                issue_uris.insert(waiting_issue_id.clone(), uri.clone());
                uri
            }
        };
        if let Some(waiting) = waiting {
            edges.entry(waiting).or_default().push(target);
        }
    }
    Ok(edges)
}

/// Where an execution stands with respect to its gates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoldState {
    pub has_gates: bool,
    pub released_at: Option<i64>,
    pub superseded: bool,
    /// The execution's issue has been merged or closed.
    pub issue_resolved: bool,
}

async fn hold_state_conn(conn: &Connection, execution_id: &str) -> DbResult<HoldState> {
    let mut rows = conn
        .query(
            "SELECT e.released_at, e.lifecycle = 'superseded',
                    EXISTS (SELECT 1 FROM execution_gates WHERE execution_id = ?1),
                    EXISTS (SELECT 1 FROM issues i WHERE i.id = e.issue_id
                              AND (i.merged_at IS NOT NULL OR i.closed_at IS NOT NULL))
               FROM executions e WHERE e.id = ?1",
            cairn_db::turso::params![execution_id],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Internal(format!("execution {execution_id} not found")))?;
    use crate::storage::RowExt;
    Ok(HoldState {
        released_at: row.opt_i64(0)?,
        superseded: row.i64(1)? != 0,
        has_gates: row.i64(2)? != 0,
        issue_resolved: row.i64(3)? != 0,
    })
}

pub async fn hold_state(
    db: &crate::storage::LocalDb,
    execution_id: &str,
) -> Result<HoldState, String> {
    let execution_id = execution_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move { hold_state_conn(conn, &execution_id).await })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Refuse a gate patch or early release on an execution that is not held,
/// saying why. `label` names the execution (`execution 2 on cairn://p/x/7`).
pub fn require_held(state: &HoldState, label: &str) -> Result<(), String> {
    if let Some(released_at) = state.released_at {
        let at = crate::clock::stamp(released_at).unwrap_or_else(|| released_at.to_string());
        return Err(format!(
            "{label} was released at {at}, so its gates can no longer change. Gates may be \
             added, removed, or released only while an execution is held: release is a one-way \
             door, and a gate added to a running execution would be a second, weaker hold with no \
             defined meaning."
        ));
    }
    if state.issue_resolved {
        return Err(format!(
            "{label} belongs to an issue that has already been merged or closed, so it will never \
             start: that work was resolved without it. Reopen the issue first if it should still \
             run."
        ));
    }
    if state.superseded {
        return Err(format!(
            "{label} was superseded by a later execution on its issue while it was held, so it \
             can never start. File a new execution with the gates you want instead."
        ));
    }
    if !state.has_gates {
        return Err(format!(
            "{label} was never held: it was filed without `after:` gates. Gates are set when an \
             execution is filed — append {{recipe, after:[...]}} to the issue's executions \
             collection."
        ));
    }
    Ok(())
}

/// Replace a held execution's gates with `gates` (canonical predicate JSON).
///
/// A predicate kept from the old list keeps its row — and so its latched
/// satisfaction, evidence, and foreclosure — rather than starting over.
/// Removing the last gate leaves nothing to wait on, so the execution is
/// released in the same transaction: a held execution must never be left
/// without the gates that hold it, because with no gates and no jobs the
/// projection derives `complete`.
pub async fn replace_gates(
    orch: &crate::orchestrator::Orchestrator,
    execution_id: &str,
    gates: &[String],
    label: &str,
) -> Result<String, String> {
    let satisfied = already_satisfied(orch, gates).await;
    replace_gates_observed(orch, execution_id, gates, label, satisfied).await
}

/// [`replace_gates`] with the satisfaction preflight already taken. Every gate
/// in `satisfied` is latched in the same transaction, because that observation
/// is what let its edge skip the cycle refusal.
pub(crate) async fn replace_gates_observed(
    orch: &crate::orchestrator::Orchestrator,
    execution_id: &str,
    gates: &[String],
    label: &str,
    satisfied: HashSet<String>,
) -> Result<String, String> {
    use cairn_workflow::gates::store;

    let db = crate::execution::routing::owning_db_for_execution(&orch.db, execution_id)
        .await
        .map_err(|error| error.to_string())?;
    let now = orch.services.clock.now();
    let (execution, gates_owned, label_owned) =
        (execution_id.to_string(), gates.to_vec(), label.to_string());
    let released = db
        .write(|conn| {
            let execution_id = execution.clone();
            let gates = gates_owned.clone();
            let label = label_owned.clone();
            let satisfied = satisfied.clone();
            let dbs = orch.db.clone();
            let own_db = db.clone();
            Box::pin(async move {
                require_held(&hold_state_conn(conn, &execution_id).await?, &label)
                    .map_err(DbError::Internal)?;
                // Write the execution row, which a release also writes, so a
                // release racing this patch conflicts with it instead of
                // committing against the gate list this patch is replacing.
                conn.execute(
                    "UPDATE executions SET released_at = NULL
                      WHERE id = ?1 AND released_at IS NULL",
                    cairn_db::turso::params![execution_id.as_str()],
                )
                .await?;
                let issue_id = crate::storage::query_opt_text_conn(
                    conn,
                    "SELECT issue_id FROM executions WHERE id = ?1",
                    cairn_db::turso::params![execution_id.as_str()],
                )
                .await?;
                if let Some(issue_id) = issue_id.as_deref() {
                    refuse_cycles_conn(
                        conn,
                        &dbs,
                        &own_db,
                        issue_id,
                        Some(&execution_id),
                        &gates,
                        &satisfied,
                    )
                    .await?;
                }

                let existing = store::list_gates_conn(conn, &execution_id).await?;
                for gate in &existing {
                    if !gates.contains(&gate.predicate_json) {
                        store::delete_gate_conn(conn, &gate.id).await?;
                    }
                }
                for (seq, predicate) in gates.iter().enumerate() {
                    let gate_id = match existing.iter().find(|g| &g.predicate_json == predicate) {
                        Some(gate) => gate.id.clone(),
                        None => store::insert_gates_conn(
                            conn,
                            &execution_id,
                            std::slice::from_ref(predicate),
                            now,
                        )
                        .await?
                        .remove(0),
                    };
                    store::set_gate_seq_conn(conn, &gate_id, seq as i64).await?;
                    if satisfied.contains(predicate) {
                        store::record_outcome_conn(conn, &gate_id, "satisfied", Some(now)).await?;
                    }
                }

                if gates.is_empty() {
                    return super::sweep::release_conn(
                        conn,
                        &execution_id,
                        now,
                        &super::sweep::ReleaseCause::GatesRemoved,
                    )
                    .await;
                }
                Ok(None)
            })
        })
        .await
        .map_err(|error| error.to_string())?;

    if let Some(released) = released {
        super::sweep::announce_release(orch, execution_id, released).await;
        return Ok(format!(
            "Removed every gate from {label}; with nothing left to wait on, it started now."
        ));
    }
    let evaluation = super::sweep::evaluate_execution(orch, execution_id).await?;
    if evaluation.released {
        return Ok(format!(
            "Replaced the gates on {label}; every one is already satisfied, so it started now."
        ));
    }
    Ok(format!(
        "Replaced the gates on held {label}: it now waits after {}.",
        evaluation.waiting.join("; after ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn clause(value: Value) -> TriggerClause {
        parse_after(Some(&json!([value])), "payload.after")
            .unwrap()
            .remove(0)
    }

    #[test]
    fn prose_is_refused_with_the_clause_object_it_should_have_been() {
        let error = parse_after(
            Some(&json!(["issue cairn://p/cairn/4724 merged"])),
            "payload.after",
        )
        .unwrap_err();
        assert!(
            error.starts_with("payload.after[0] is prose"),
            "got: {error}"
        );
        assert!(
            error.contains(r#"Corrected: {"fact":"issue_resolution""#),
            "the refusal must show the clause object: {error}"
        );

        let error = parse_after(Some(&json!({"fact": "clock"})), "payload.after").unwrap_err();
        assert!(error.contains("must be an array"), "got: {error}");
        assert_eq!(parse_after(Some(&Value::Null), "payload.after"), Ok(vec![]));
    }

    #[test]
    fn a_malformed_clause_is_refused_through_the_vocabulary_with_its_correction() {
        let error = canonical_predicates(
            &[clause(json!({"fact": "issue_resolution", "issue": "cairn://p/cairn/1", "resolution": "shipped"}))],
            "payload.after",
        )
        .unwrap_err();
        assert!(error.starts_with("payload.after[0]: "), "got: {error}");
        assert!(error.contains("Corrected: {"), "got: {error}");

        let error = canonical_predicates(
            &[clause(json!({"fact": "decision", "decision": 12}))],
            "payload.after",
        )
        .unwrap_err();
        assert!(
            error.contains("unknown gate fact 'decision'"),
            "got: {error}"
        );
    }

    #[test]
    fn canonical_predicates_normalize_the_issue_and_drop_exact_duplicates() {
        let predicates = canonical_predicates(
            &[
                clause(json!({"resolution": "merged", "issue": "cairn://p/CAIRN/7", "fact": "issue_resolution"})),
                clause(json!({"fact": "issue_resolution", "issue": "cairn://p/cairn/7", "resolution": "merged"})),
                clause(json!({"fact": "clock", "notBefore": "2026-09-05T12:00:00Z"})),
            ],
            "payload.after",
        )
        .unwrap();
        assert_eq!(
            predicates,
            vec![
                r#"{"fact":"issue_resolution","issue":"cairn://p/cairn/7","resolution":"merged"}"#
                    .to_string(),
                r#"{"fact":"clock","notBefore":"2026-09-05T12:00:00Z"}"#.to_string(),
            ]
        );
    }

    #[test]
    fn every_issue_naming_fact_forms_a_wait_edge_and_clock_does_not() {
        let nine = Some("cairn://p/cairn/9".to_string());
        assert_eq!(
            waited_issue(r#"{"fact":"pr_state","issue":"cairn://p/cairn/9","state":"merged"}"#),
            nine
        );
        assert_eq!(
            waited_issue(
                r#"{"fact":"artifact_confirmed","issue":"cairn://p/CAIRN/9","artifact":"plan"}"#
            ),
            nine,
            "an artifact is produced by that issue's jobs"
        );
        assert_eq!(
            waited_issue(
                r#"{"fact":"check_verdict","check":"lint","issue":"cairn://p/cairn/9","verdict":"passed"}"#
            ),
            nine,
            "a verdict is produced on that issue's branch"
        );
        assert_eq!(
            waited_issue(r#"{"fact":"clock","notBefore":"2026-09-05T12:00:00Z"}"#),
            None
        );
    }

    #[test]
    fn a_cycle_is_found_and_named_as_a_path() {
        let a = "cairn://p/cairn/4736".to_string();
        let b = "cairn://p/cairn/4737".to_string();
        let c = "cairn://p/cairn/4738".to_string();
        let edges = HashMap::from([(b.clone(), vec![c.clone()]), (c.clone(), vec![a.clone()])]);

        let path = find_cycle(&a, std::slice::from_ref(&b), &edges).unwrap();
        assert_eq!(
            cycle_refusal(&path),
            "cairn/4736 after cairn/4737 after cairn/4738 after cairn/4736 \u{2014} an execution \
             cannot wait on its own issue, directly or transitively."
        );
        assert_eq!(
            find_cycle(&a, std::slice::from_ref(&a), &HashMap::new()),
            Some(vec![a.clone(), a.clone()]),
            "waiting on its own issue directly is the shortest cycle"
        );
        assert_eq!(
            find_cycle(&c, std::slice::from_ref(&b), &HashMap::new()),
            None
        );
    }
}
