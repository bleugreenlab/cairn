//! Minting facts into the durable fact log, and restoring a row to the fact it
//! recorded.
//!
//! A producer that observes something routable calls [`mint`] (or
//! [`spawn_mint`] where it must not wait at all) instead of dispatching it. The
//! mint is one local insert: the row lands, the fact log worker is poked, and
//! the producer returns. Matching, thread wakes, and route dispatch all happen
//! later, in the worker, off the producer's path.
//!
//! A row holds only the registry's stored fields, bounded. Content fields are
//! declared `stored: false` and restored by [`hydrate`] from the canonical
//! record the row points at, so the log never becomes a second copy of the
//! transcript or of a file. The one exception is a source with no local
//! record at all (a relayed GitHub comment): its row stores the content once,
//! bounded only by the external record's own limit, because clipping it would
//! hand routes less than the producer saw. An `ingress` row stores no body:
//! its canonical local record is the `ingress_events` row it points at.

use std::collections::BTreeMap;

use serde_json::Value;

use super::{FactRegistry, Presence, RouteFact, SourceSpec};
use crate::orchestrator::Orchestrator;
use crate::storage::events::history::{EventFilter, HistoryReader, HotRow};
use crate::storage::{FactRow, NewFact, RowExt};

/// Longest stored scalar, matching the firing journal's snapshot bound.
const MAX_STORED_CHARS: usize = crate::storage::SNAPSHOT_CHARS;
/// Longest stored list.
const MAX_STORED_LIST: usize = 64;
/// Longest stored summary.
const MAX_SUMMARY_CHARS: usize = 280;

/// One fact to mint, as its producer decided it.
pub struct Mint<'a> {
    pub fact: RouteFact,
    /// A navigable pointer to the canonical record this fact is about.
    pub record_ref: String,
    /// The canonical record's raw key, which outlives any change to the
    /// resource grammar `record_ref` is written in.
    pub record_id: Option<String>,
    /// The producer's project, from its own context. `None` only for a fact
    /// that resolves to no project.
    pub project_id: Option<&'a str>,
    /// Operator presence when the fact was decided.
    pub presence: Presence,
    /// The job whose action caused this fact. Its own thread is never woken by it.
    pub actor_job_id: Option<&'a str>,
}

/// [`Mint`] with owned fields, for [`spawn_mint`]. `presence: None` resolves
/// operator presence inside the spawned task, so a producer that must not
/// await anything does not await that either.
pub struct MintOwned {
    pub fact: RouteFact,
    pub record_ref: String,
    pub record_id: Option<String>,
    pub project_id: Option<String>,
    pub presence: Option<Presence>,
    pub actor_job_id: Option<String>,
}

pub(crate) fn presence_str(presence: Presence) -> &'static str {
    match presence {
        Presence::Active => "active",
        Presence::Away => "away",
    }
}

pub(crate) fn presence_from_str(value: &str) -> Presence {
    match value {
        "active" => Presence::Active,
        _ => Presence::Away,
    }
}

/// The operator's single, provider-independent presence.
pub(crate) async fn operator_presence() -> Presence {
    if crate::channels::operator_presence_status().await.presence
        == crate::channels::OperatorPresence::Present
    {
        Presence::Active
    } else {
        Presence::Away
    }
}

fn bound_text(text: &str, limit: usize) -> String {
    let mut bounded: String = text.chars().take(limit).collect();
    if bounded.chars().count() < text.chars().count() {
        bounded.push('…');
    }
    bounded
}

fn bound_value(value: Value, max_chars: usize, max_list: usize) -> Value {
    match value {
        Value::String(text) => Value::String(bound_text(&text, max_chars)),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .take(max_list)
                .map(|item| match item {
                    Value::String(text) => Value::String(bound_text(&text, max_chars)),
                    other => other,
                })
                .collect(),
        ),
        other => other,
    }
}

/// The fields a row stores: registry fields marked stored, each bounded.
/// Anything the registry does not declare, and every hydrated field, is
/// dropped here, so no producer can widen what the log holds.
fn stored_fields(spec: &SourceSpec, fields: BTreeMap<String, Value>) -> BTreeMap<String, Value> {
    let max_chars = spec.max_stored_chars.unwrap_or(MAX_STORED_CHARS);
    let max_list = if spec.whole_lists {
        usize::MAX
    } else {
        MAX_STORED_LIST
    };
    fields
        .into_iter()
        .filter(|(name, _)| spec.fields.get(name.as_str()).is_some_and(|f| f.stored))
        .map(|(name, value)| (name, bound_value(value, max_chars, max_list)))
        .collect()
}

/// Insert one fact into the log. Returns the new row id, or `None` when the
/// source's dedupe rule recognised the fact as one already recorded.
pub async fn mint(orch: &Orchestrator, mint: Mint<'_>) -> Result<Option<i64>, String> {
    let registry = FactRegistry::default();
    let spec = registry
        .source(&mint.fact.source)
        .ok_or_else(|| format!("unknown fact source '{}'", mint.fact.source))?;
    let (origin_principal_json, origin_appearance_json) = match &mint.fact.origin {
        Some(origin) => {
            let (principal, appearance) = crate::issues::crud::encode_issue_authorship(origin)
                .map_err(|error| error.to_string())?;
            (Some(principal), Some(appearance))
        }
        None => (None, None),
    };
    let dedupe = spec.dedupe;
    // An outsider's content never reaches the summary: for an untrusted source
    // the producer's summary is ignored and composed from label fields.
    let summary = match spec.untrusted {
        Some(labels) => super::predicate::label_summary(labels, &mint.fact.fields),
        None => mint.fact.summary,
    };
    let fields_json = serde_json::to_string(&stored_fields(spec, mint.fact.fields))
        .map_err(|error| error.to_string())?;
    let new = NewFact {
        project_id: mint.project_id.map(str::to_owned),
        identity: cairn_common::uri::canonicalize_uri_identity(&mint.fact.identity),
        source: mint.fact.source,
        record_ref: mint.record_ref,
        record_id: mint.record_id,
        fields_json,
        summary: summary
            .as_deref()
            .map(str::trim)
            .filter(|summary| !summary.is_empty())
            .map(|summary| bound_text(summary, MAX_SUMMARY_CHARS)),
        presence: presence_str(mint.presence).to_string(),
        actor_job_id: mint.actor_job_id.map(str::to_owned),
        origin_principal_json,
        origin_appearance_json,
        // A fact minted while a route's sink runs is that route's output, so
        // the loop guard holds for every minter without each one threading it.
        route_provenance: mint
            .fact
            .route_provenance
            .or_else(super::current_provenance),
        minted_at: chrono::Utc::now().timestamp_millis(),
    };
    let id = crate::storage::insert_fact(&orch.db.local, new, dedupe)
        .await
        .map_err(|error| error.to_string())?;
    if id.is_some() {
        orch.fact_log.notify.notify_one();
    }
    Ok(id)
}

/// Mint without waiting for anything, not even the insert. The route
/// provenance of the calling task is captured before the spawn, because the
/// spawned task does not inherit task-locals.
pub fn spawn_mint(orch: &Orchestrator, mint: MintOwned) {
    spawn_mint_deferred(orch, async move { Ok(mint) });
}

/// [`spawn_mint`] for a producer whose fact needs reads of its own (the
/// writing job's project and node): `build` runs inside the spawned task, so
/// the producer waits on none of it.
pub fn spawn_mint_deferred<F>(orch: &Orchestrator, build: F)
where
    F: std::future::Future<Output = Result<MintOwned, String>> + Send + 'static,
{
    let provenance = super::current_provenance();
    let orch = orch.clone();
    tokio::spawn(async move {
        let mint = match build.await {
            Ok(mint) => mint,
            Err(error) => {
                log::warn!("could not build a fact to mint into the fact log: {error}");
                return;
            }
        };
        let mut fact = mint.fact;
        if fact.route_provenance.is_none() {
            fact.route_provenance = provenance;
        }
        let source = fact.source.clone();
        let presence = match mint.presence {
            Some(presence) => presence,
            None => operator_presence().await,
        };
        if let Err(error) = self::mint(
            &orch,
            Mint {
                fact,
                record_ref: mint.record_ref,
                record_id: mint.record_id,
                project_id: mint.project_id.as_deref(),
                presence,
                actor_job_id: mint.actor_job_id.as_deref(),
            },
        )
        .await
        {
            log::warn!("could not mint a {source} fact into the fact log: {error}");
        }
    });
}

/// A row restored to the fact it recorded.
pub struct Hydrated {
    pub fact: RouteFact,
    pub presence: Presence,
    /// When the canonical record says the fact happened, in milliseconds, for
    /// sources whose record carries a time the log row does not.
    pub occurred_at_ms: Option<i64>,
}

/// The row's stored fact, without hydrated fields.
pub fn stored_fact(row: &FactRow) -> Result<RouteFact, String> {
    let fields: BTreeMap<String, Value> =
        serde_json::from_str(&row.fields_json).map_err(|error| error.to_string())?;
    let origin = crate::issues::crud::decode_issue_authorship(
        row.origin_principal_json.clone(),
        row.origin_appearance_json.clone(),
    )
    .map_err(|error| error.to_string())?;
    Ok(RouteFact {
        source: row.source.clone(),
        identity: row.identity.clone(),
        fields,
        origin,
        summary: row.summary.clone(),
        route_provenance: row.route_provenance.clone(),
    })
}

fn thread_stream_hot_sql(guard_job: bool) -> String {
    let from = if guard_job {
        "events e JOIN runs r ON r.id = e.run_id WHERE e.rowid = ?1 AND r.job_id = ?2"
    } else {
        "events e WHERE e.rowid = ?1"
    };
    format!("SELECT {} FROM {from}", HotRow::columns(Some("e")))
}

/// Locate a hot fact directly before falling back to archived run membership.
async fn thread_stream_run_ids(
    db: &crate::storage::LocalDb,
    rowid: i64,
    job_id: Option<&str>,
) -> crate::storage::DbResult<Vec<String>> {
    if let Some(job_id) = job_id {
        let hot = db
            .query_opt(
                "SELECT e.run_id FROM events e JOIN runs r ON r.id = e.run_id
                 WHERE e.rowid = ?1 AND r.job_id = ?2",
                (rowid, job_id.to_owned()),
                |row| row.text(0),
            )
            .await?;
        if let Some(run_id) = hot {
            return Ok(vec![run_id]);
        }
        db.query_all(
            "SELECT id FROM runs WHERE job_id = ?1",
            (job_id.to_owned(),),
            |row| row.text(0),
        )
        .await
    } else {
        // Legacy facts need a durable run locator before their hot rows can
        // be evicted. This lookup reads no body and cannot locate evicted rows.
        db.query_all(
            "SELECT run_id FROM events WHERE rowid = ?1",
            (rowid,),
            |row| row.text(0),
        )
        .await
    }
}

/// Restore a row's hydrated fields from the canonical record it points at.
/// One arm per source that declares a hydrated field; every other source's row
/// already is its whole fact.
pub async fn hydrate(orch: &Orchestrator, row: &FactRow) -> Result<Hydrated, String> {
    let mut fact = stored_fact(row)?;
    let mut occurred_at_ms = None;
    if row.source == "thread_stream" {
        let rowid: i64 = row
            .record_id
            .as_deref()
            .and_then(|id| id.parse().ok())
            .ok_or_else(|| format!("thread_stream row {} has no transcript rowid", row.id))?;
        let project = fact
            .fields
            .get("project")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("thread_stream row {} has no project", row.id))?
            .to_owned();
        let db = orch.db.for_project(&project).await;
        let job_id = fact.fields.get("jobId").and_then(Value::as_str);
        let mut binds = vec![cairn_db::turso::Value::Integer(rowid)];
        if let Some(job_id) = job_id {
            binds.push(job_id.to_owned().into());
        }
        let selected = db
            .query_all(thread_stream_hot_sql(job_id.is_some()), binds, |row| {
                HotRow::from_row_at(row, 0)
            })
            .await
            .map_err(|error| error.to_string())?;
        let mut history = HistoryReader::new(&db);
        let mut event = None;
        if !selected.is_empty() {
            event = history
                .hydrate_selected_hot_rows(selected, EventFilter::default())
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .next()
                .map(|row| row.event);
        } else if !history.is_hot_only() {
            let run_ids = thread_stream_run_ids(&db, rowid, job_id)
                .await
                .map_err(|error| error.to_string())?;
            history
                .prime_runs(&run_ids)
                .await
                .map_err(|error| error.to_string())?;
            for run_id in run_ids {
                if let Some(found) = history
                    .run_event_by_rowid(&run_id, rowid)
                    .await
                    .map_err(|error| error.to_string())?
                {
                    event = Some(found.event);
                    break;
                }
            }
        }
        let event = event.ok_or_else(|| format!("transcript event {rowid} no longer exists"))?;
        let text = crate::channels::router::assistant_text(&event.data)
            .ok_or_else(|| format!("transcript event {rowid} carries no assistant text"))?;
        fact.fields.insert("text".into(), Value::String(text));
        // Transcript `created_at` is whole seconds.
        occurred_at_ms = Some(event.created_at.saturating_mul(1000));
    }
    if row.source == "github_comment" {
        let field = |name: &str| {
            fact.fields
                .get(name)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        let body = crate::github::relay::comment_context(
            &field("repository"),
            &field("kind"),
            &field("number"),
            &field("author"),
            &field("url"),
            &field("text"),
        );
        fact.fields.insert("body".into(), Value::String(body));
    }
    if row.source == "ingress" {
        let id: i64 = row
            .record_id
            .as_deref()
            .and_then(|id| id.parse().ok())
            .ok_or_else(|| format!("ingress row {} has no ingress_events id", row.id))?;
        let body = crate::ingress::event_body(&orch.db.local, id)
            .await?
            .ok_or_else(|| format!("ingress event {id} no longer exists"))?;
        fact.fields.insert("body".into(), Value::String(body));
    }
    if row.source == "channel_message" {
        let id = row
            .record_id
            .as_deref()
            .ok_or_else(|| format!("channel_message row {} has no inbound id", row.id))?;
        let inbound = crate::channels::ledger::get_inbound(&orch.db.local, id)
            .await?
            .ok_or_else(|| format!("channel inbound message {id} no longer exists"))?;
        fact.fields
            .insert("text".into(), Value::String(inbound.text));
        occurred_at_ms = Some(inbound.received_at);
    }
    Ok(Hydrated {
        fact,
        presence: presence_from_str(&row.presence),
        occurred_at_ms,
    })
}

/// Whether matching any of these clauses needs a field the row does not store.
pub(crate) fn clauses_need_hydration<'c>(
    spec: &SourceSpec,
    clauses: impl IntoIterator<Item = &'c super::TriggerClause>,
) -> bool {
    clauses.into_iter().any(|clause| {
        clause.keys().any(|key| {
            let field = key.strip_suffix("Prefix").unwrap_or(key);
            spec.is_hydrated(field)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::events::history::HistorySource;
    use crate::storage::history_pack::testing::{
        evict_all, publish_for_test, read_both, seed_fixture_execution,
    };
    use crate::storage::quarantine::{
        quarantine, QuarantineSet, HISTORY_PACK_SINK, TRANSCRIPT_EVENT_SINK, WITHHELD_PREFIX,
    };
    use crate::storage::{LocalDb, SearchIndex};
    use std::sync::Arc;

    fn stream_test_orchestrator(db: LocalDb) -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let config = root.join("config");
        std::fs::create_dir_all(config.join("agents")).unwrap();
        std::fs::create_dir_all(config.join("recipes")).unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(db), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config,
        )
        .build()
    }

    async fn stream_fact(db: &LocalDb, event_id: &str, job: Option<&str>) -> FactRow {
        let rowid = db
            .query_opt(
                "SELECT rowid FROM events WHERE id = ?1",
                (event_id.to_owned(),),
                |row| row.i64(0),
            )
            .await
            .unwrap()
            .unwrap();
        let mut fields = serde_json::json!({"project": "proj", "thread": "general"});
        if let Some(job) = job {
            fields["jobId"] = job.into();
        }
        FactRow {
            id: 1,
            project_id: Some("p".into()),
            source: "thread_stream".into(),
            identity: format!("cairn://p/proj/general:event:{rowid}"),
            record_ref: format!("cairn://p/proj/general:event:{rowid}"),
            record_id: Some(rowid.to_string()),
            fields_json: fields.to_string(),
            summary: None,
            presence: "away".into(),
            actor_job_id: None,
            origin_principal_json: None,
            origin_appearance_json: None,
            route_provenance: None,
            minted_at: 1,
            indexed_at: None,
            route_state: "pending".into(),
            route_note: None,
            wakes_json: None,
        }
    }

    struct StreamQuarantine(tempfile::TempDir);

    impl StreamQuarantine {
        fn new(sink: &str, event_id: &str) -> Self {
            let source = tempfile::tempdir().unwrap();
            quarantine().install_for(
                source.path(),
                QuarantineSet::from_entries([(
                    sink.to_owned(),
                    event_id.to_owned(),
                    "route-incident".to_owned(),
                )]),
            );
            Self(source)
        }
    }

    impl Drop for StreamQuarantine {
        fn drop(&mut self) {
            quarantine().install_for(self.0.path(), QuarantineSet::default());
        }
    }

    #[tokio::test]
    async fn thread_stream_hot_locator_selects_only_the_last_run() {
        let db = crate::storage::migrated_test_db("route-hydrate-last-run").await;
        seed_fixture_execution(&db, "route-hydrate-last-run").await;
        for number in 3..=50 {
            db.execute(
                "INSERT INTO runs(id, project_id, job_id, session_id, status, created_at, updated_at)
                 VALUES(?1, 'p', 'job-1', 'sess-2', 'exited', ?2, ?2)",
                (format!("run-{number}"), i64::from(number)),
            )
            .await
            .unwrap();
        }
        db.execute(
            "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
             VALUES('last-run-event', 'run-50', 'sess-2', 1, 50, 'assistant',
                    '{\"content\":\"last run prose\"}', 50)",
            (),
        )
        .await
        .unwrap();
        let fact = stream_fact(&db, "last-run-event", Some("job-1")).await;
        let rowid = fact.record_id.as_ref().unwrap().parse().unwrap();
        let selected = thread_stream_run_ids(&db, rowid, Some("job-1"))
            .await
            .unwrap();
        assert_eq!(selected, vec!["run-50"]);
        let orch = stream_test_orchestrator(db);
        for cataloged in [false, true] {
            if cataloged {
                publish_for_test(&orch.db.local, "exec-1").await;
            }
            let before = (
                orch.db.local.read_statement_count(),
                orch.db.local.read_checkout_count(),
            );
            assert_eq!(
                hydrate(&orch, &fact).await.unwrap().fact.fields["text"],
                "last run prose"
            );
            assert_eq!(
                (
                    orch.db.local.read_statement_count() - before.0,
                    orch.db.local.read_checkout_count() - before.1
                ),
                (1, 1),
                "50-run latest event, cataloged={cataloged}"
            );
        }
    }

    #[tokio::test]
    async fn thread_stream_hot_locator_plan_starts_at_the_rowid() {
        let db = crate::storage::migrated_test_db("route-hydrate-plan").await;
        for guarded in [false, true] {
            let mut binds = vec![1i64.into()];
            if guarded {
                binds.push("job".into());
            }
            let plan =
                crate::storage::query_plan(&db, &thread_stream_hot_sql(guarded), binds).await;
            assert!(
                plan.contains("SEARCH e") && plan.contains("rowid"),
                "{plan}"
            );
            assert!(
                !plan.contains("SCAN e") && !plan.contains("SCAN r"),
                "{plan}"
            );
            assert!(!plan.contains("TEMP B-TREE"), "{plan}");
        }
    }

    #[tokio::test]
    async fn thread_stream_hydration_matches_hot_forced_and_evicted_pack() {
        let db = crate::storage::migrated_test_db("route-hydrate-parity").await;
        let events = seed_fixture_execution(&db, "route-hydrate-parity").await;
        let first = stream_fact(&db, &events.restored, Some("job-1")).await;
        let second = stream_fact(&db, &events.second_run, Some("job-1")).await;
        publish_for_test(&db, "exec-1").await;
        let orch = stream_test_orchestrator(db);
        for (fact, text, occurred) in [
            (&first, "the restored body", 10_000),
            (&second, "second run", 20_000),
        ] {
            let (hot, pack) = read_both(&orch.db.local, || async {
                hydrate(&orch, fact).await.unwrap()
            })
            .await;
            assert_eq!(hot.fact.fields, pack.fact.fields);
            assert_eq!(hot.fact.fields["text"], text);
            assert_eq!(hot.occurred_at_ms, Some(occurred));
            assert_eq!(hot.occurred_at_ms, pack.occurred_at_ms);
        }
        evict_all(&orch.db.local).await;
        for (fact, text, occurred) in [
            (&first, "the restored body", 10_000),
            (&second, "second run", 20_000),
        ] {
            let hydrated = hydrate(&orch, fact).await.unwrap();
            assert_eq!(hydrated.fact.fields["text"], text);
            assert_eq!(hydrated.occurred_at_ms, Some(occurred));
        }
    }

    #[tokio::test]
    async fn thread_stream_hydration_withholds_hot_and_post_pack_transcript() {
        let db = crate::storage::migrated_test_db("route-hydrate-withheld").await;
        let events = seed_fixture_execution(&db, "route-hydrate-withheld").await;
        let fact = stream_fact(&db, &events.restored, Some("job-1")).await;
        publish_for_test(&db, "exec-1").await;
        let orch = stream_test_orchestrator(db);
        let _guard = StreamQuarantine::new(TRANSCRIPT_EVENT_SINK, &events.restored);
        let (hot, pack) = read_both(&orch.db.local, || async {
            hydrate(&orch, &fact).await.unwrap()
        })
        .await;
        assert_eq!(hot.fact.fields, pack.fact.fields);
        for hydrated in [hot, pack] {
            let text = hydrated.fact.fields["text"].as_str().unwrap();
            assert!(text.contains(WITHHELD_PREFIX), "{text}");
            assert!(!text.contains("the restored body"), "{text}");
            assert_eq!(hydrated.occurred_at_ms, Some(10_000));
        }
        evict_all(&orch.db.local).await;
        let hydrated = hydrate(&orch, &fact).await.unwrap();
        assert!(hydrated.fact.fields["text"]
            .as_str()
            .unwrap()
            .contains(WITHHELD_PREFIX));
    }

    #[tokio::test]
    async fn thread_stream_hydration_withholds_a_pack_copy_quarantined_after_packing() {
        let db = crate::storage::migrated_test_db("route-hydrate-pack-key").await;
        let events = seed_fixture_execution(&db, "route-hydrate-pack-key").await;
        let fact = stream_fact(&db, &events.second_run, Some("job-1")).await;
        publish_for_test(&db, "exec-1").await;
        let orch = stream_test_orchestrator(db);
        let _guard = StreamQuarantine::new(HISTORY_PACK_SINK, &events.second_run);
        orch.db
            .local
            .set_history_source_override(Some(HistorySource::Pack));
        let hydrated = hydrate(&orch, &fact).await.unwrap();
        let text = hydrated.fact.fields["text"].as_str().unwrap();
        assert!(text.contains(WITHHELD_PREFIX), "{text}");
        assert!(!text.contains("second run"), "{text}");
        assert_eq!(hydrated.occurred_at_ms, Some(20_000));
        orch.db.local.set_history_source_override(None);
    }

    #[tokio::test]
    async fn thread_stream_hydration_keeps_legacy_hot_facts_but_guards_known_job() {
        let db = crate::storage::migrated_test_db("route-hydrate-legacy").await;
        let events = seed_fixture_execution(&db, "route-hydrate-legacy").await;
        let legacy = stream_fact(&db, &events.restored, None).await;
        let wrong = stream_fact(&db, &events.restored, Some("different-job")).await;
        let orch = stream_test_orchestrator(db);
        let hydrated = hydrate(&orch, &legacy).await.unwrap();
        assert_eq!(hydrated.fact.fields["text"], "the restored body");
        assert_eq!(hydrated.occurred_at_ms, Some(10_000));
        assert!(hydrate(&orch, &wrong)
            .await
            .err()
            .unwrap()
            .contains("no longer exists"));
    }

    #[tokio::test]
    async fn thread_stream_hydration_reports_offline_catalog_without_hot_fallback() {
        let db = crate::storage::migrated_test_db("route-hydrate-offline").await;
        let events = seed_fixture_execution(&db, "route-hydrate-offline").await;
        let known = stream_fact(&db, &events.restored, Some("job-1")).await;
        let legacy = stream_fact(&db, &events.restored, None).await;
        publish_for_test(&db, "exec-1").await;
        db.execute("UPDATE history_tier SET evicted_at = 5", ())
            .await
            .unwrap();
        db.mark_history_evicted();
        let root = db.path().with_extension("archive");
        std::fs::rename(&root, root.with_extension("unplugged")).unwrap();
        let orch = stream_test_orchestrator(db);
        for fact in [&known, &legacy] {
            let error = hydrate(&orch, fact).await.err().unwrap();
            assert_eq!(error, format!("archive offline: {}", root.display()));
        }
    }

    #[test]
    fn stored_fields_drop_hydrated_and_unregistered_fields_and_bound_the_rest() {
        let registry = FactRegistry::default();
        let spec = registry.source("thread_stream").unwrap();
        let fields = BTreeMap::from([
            (
                "text".to_string(),
                Value::String("the whole utterance".into()),
            ),
            ("mystery".to_string(), Value::Bool(true)),
            (
                "context".to_string(),
                Value::String("x".repeat(MAX_STORED_CHARS + 5)),
            ),
        ]);
        let stored = stored_fields(spec, fields);
        assert_eq!(stored.keys().collect::<Vec<_>>(), vec!["context"]);
        assert_eq!(
            stored["context"].as_str().unwrap().chars().count(),
            MAX_STORED_CHARS + 1
        );

        let attention = registry.source("attention").unwrap();
        let labels = Value::Array((0..100).map(|n| Value::String(n.to_string())).collect());
        let stored = stored_fields(
            attention,
            BTreeMap::from([("label".to_string(), labels.clone())]),
        );
        assert_eq!(stored["label"].as_array().unwrap().len(), MAX_STORED_LIST);

        // A reference set the lane resolves or matches is never cut.
        for source in ["thread_stream", "post_created", "post_commented"] {
            let spec = registry.source(source).unwrap();
            let stored = stored_fields(
                spec,
                BTreeMap::from([("mentions".to_string(), labels.clone())]),
            );
            assert_eq!(
                stored["mentions"].as_array().unwrap().len(),
                100,
                "{source}"
            );
        }
    }
}
