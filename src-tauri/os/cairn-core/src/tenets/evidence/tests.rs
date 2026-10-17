//! Tests for tenet evidence: hooks, evaluation, and the contradiction wake.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};

use super::events::{self, NewEvent, PendingEvent};
use super::wake::{self, decayed};
use super::worker::{self, EvidenceDeps, Processed};
use super::*;
use crate::embeddings::InputType;
use crate::issues::crud;
use crate::responses::{ResponseDefinition, ResponseError, ResponseOutcome};
use crate::services::testing::CapturingEmitter;
use crate::services::{Clock, EventEmitter};
use crate::storage::{LocalDb, RowExt};
use crate::transitions::Resolution;

const A: &str = "device-a";
const B: &str = "device-b";
const DAY: i64 = 86_400;

struct FixedClock(i64);

impl Clock for FixedClock {
    fn now(&self) -> i64 {
        self.0
    }
    fn now_u64(&self) -> u64 {
        self.0 as u64
    }
}

async fn fixture(name: &str) -> Arc<LocalDb> {
    let db = crate::storage::migrated_test_db(name).await;
    db.execute_script(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('p', 'default', 'P', 'prj', '/tmp/p', 1, 1);
         INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
           VALUES ('t-design', 'p', 'design', 'active', 'none', 1, 1);
         INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
           VALUES ('t-other', 'p', 'other', 'active', 'none', 1, 1);
         INSERT INTO jobs (id, thread_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment)
           VALUES ('j-design', 't-design', 'p', 'thread', 'thread', 'idle', 1, 1, 'thread');
         INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
           VALUES ('run-design', 'p', 'j-design', 'live', 1, 1);
         INSERT INTO issues (id, project_id, number, title, description, status, created_at, updated_at)
           VALUES ('i', 'p', 7, 'Move uploads', 'Uploads go straight to S3 now.', 'active', 1, 1);
         INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
           VALUES ('i2', 'p', 8, 'Other work', 'active', 1, 1);",
    )
    .await
    .unwrap();
    Arc::new(db)
}

/// Insert an approved tenet proposed by `thread`, returning its id.
async fn approve(db: &LocalDb, number: i64, thread: &str, text: &str, rejected: &[&str]) -> String {
    let id = format!("tenet-{number}");
    let mut options = vec![text.to_string()];
    options.extend(rejected.iter().map(|option| option.to_string()));
    db.execute(
        "INSERT INTO tenets (id, project_id, number, proposing_thread_id, question, options_json,
             status, verdict, selected_option, answer_source, answered_at, origin,
             proposer_principal_json, created_at, updated_at)
         VALUES (?1, 'p', ?2, ?3, ?4, ?5, 'approved', 'option', 0, 'device', ?6, 'standing',
             '{\"kind\":\"agent\",\"node\":\"cairn://p/prj/design\",\"run_id\":null}', 1, 1)",
        cairn_db::turso::params![
            id.as_str(),
            number,
            thread,
            format!("How should we handle {number}?"),
            serde_json::to_string(&options).unwrap(),
            100 + number
        ],
    )
    .await
    .unwrap();
    id
}

async fn add_event(db: &LocalDb, key: &str, owner: &str, text: &str) -> PendingEvent {
    let event = NewEvent {
        project_id: "p".into(),
        source_uri: format!("cairn://p/prj/{key}"),
        source_kind: events::KIND_ISSUE,
        dedupe_key: format!("test:{key}"),
        owner_device_id: owner.into(),
        text: text.into(),
        created_at: 1,
    };
    let inserted = event.clone();
    db.write(move |conn| {
        let event = inserted.clone();
        Box::pin(async move { events::insert_event_conn(conn, &event).await })
    })
    .await
    .unwrap();
    PendingEvent {
        id: event.id(),
        project_id: event.project_id,
        source_uri: event.source_uri,
        owner_device_id: event.owner_device_id,
        text: event.text,
        attempts: 0,
    }
}

async fn count(db: &LocalDb, sql: &str) -> i64 {
    db.query_one(sql, (), |row| row.i64(0)).await.unwrap()
}

async fn event_row(db: &LocalDb, id: &str) -> (String, i64, i64, Option<String>) {
    db.query_one(
        "SELECT state, attempts, next_attempt_at, last_error FROM tenet_evidence_events WHERE id = ?1",
        (id.to_string(),),
        |row| Ok((row.text(0)?, row.i64(1)?, row.i64(2)?, row.opt_text(3)?)),
    )
    .await
    .unwrap()
}

#[derive(Clone, Copy)]
enum EmbedFailure {
    NoToken,
    Gateway,
}

/// Records every outside call. Texts embed to the vector of the first
/// registered needle they contain, or to an axis nothing else uses.
struct Fake {
    device: String,
    now: AtomicI64,
    dbs: Vec<Arc<LocalDb>>,
    needles: Vec<(String, Vec<f32>)>,
    failures: Mutex<HashMap<usize, EmbedFailure>>,
    embed_calls: Mutex<Vec<(Vec<String>, InputType)>>,
    verdicts: Mutex<Value>,
    judge_calls: Mutex<Vec<Value>>,
    nudges: Mutex<Vec<String>>,
    emitter: CapturingEmitter,
    /// Seconds the clock moves while the judge is out.
    judge_advance: AtomicI64,
    /// Another runner whose sweep runs while the judge is out.
    during_judge: Mutex<Option<Arc<Fake>>>,
}

impl Fake {
    fn new(device: &str, now: i64, db: &Arc<LocalDb>) -> Self {
        Self {
            device: device.into(),
            now: AtomicI64::new(now),
            dbs: vec![db.clone()],
            needles: Vec::new(),
            failures: Mutex::new(HashMap::new()),
            embed_calls: Mutex::new(Vec::new()),
            verdicts: Mutex::new(json!({"verdicts": []})),
            judge_calls: Mutex::new(Vec::new()),
            nudges: Mutex::new(Vec::new()),
            emitter: CapturingEmitter::new(),
            judge_advance: AtomicI64::new(0),
            during_judge: Mutex::new(None),
        }
    }

    fn needle(mut self, needle: &str, vector: &[f32]) -> Self {
        self.needles.push((needle.into(), vector.to_vec()));
        self
    }

    fn as_device(&self, device: &str) -> Self {
        let mut other = Fake::new(device, self.now.load(Ordering::SeqCst), &self.dbs[0]);
        other.needles = self.needles.clone();
        *other.verdicts.lock().unwrap() = self.verdicts.lock().unwrap().clone();
        other
    }

    fn set_verdicts(&self, verdicts: Value) {
        *self.verdicts.lock().unwrap() = json!({ "verdicts": verdicts });
    }

    fn fail_call(&self, index: usize, failure: EmbedFailure) {
        self.failures.lock().unwrap().insert(index, failure);
    }

    fn embeds(&self) -> usize {
        self.embed_calls.lock().unwrap().len()
    }

    fn judges(&self) -> Vec<Value> {
        self.judge_calls.lock().unwrap().clone()
    }

    fn vector(&self, text: &str) -> Vec<f32> {
        self.needles
            .iter()
            .find(|(needle, _)| text.contains(needle.as_str()))
            .map(|(_, vector)| vector.clone())
            .unwrap_or_else(|| vec![0.0, 0.0, 0.0, 1.0])
    }
}

#[async_trait]
impl EvidenceDeps for Fake {
    fn this_device(&self) -> String {
        self.device.clone()
    }
    fn now(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }
    async fn dbs(&self) -> Vec<Arc<LocalDb>> {
        self.dbs.clone()
    }
    async fn embed(
        &self,
        texts: Vec<String>,
        input: InputType,
    ) -> Result<Option<Vec<Vec<f32>>>, String> {
        let index = {
            let mut calls = self.embed_calls.lock().unwrap();
            calls.push((texts.clone(), input));
            calls.len() - 1
        };
        match self.failures.lock().unwrap().get(&index) {
            Some(EmbedFailure::NoToken) => return Ok(None),
            Some(EmbedFailure::Gateway) => return Err("embed gateway returned 503".into()),
            None => {}
        }
        Ok(Some(texts.iter().map(|text| self.vector(text)).collect()))
    }
    async fn judge(
        &self,
        _definition: &ResponseDefinition,
        args: &Value,
    ) -> Result<ResponseOutcome, ResponseError> {
        self.judge_calls.lock().unwrap().push(args.clone());
        let other = self.during_judge.lock().unwrap().take();
        if let Some(other) = other {
            wake::recheck_contradictions(other.as_ref()).await;
        }
        self.now
            .fetch_add(self.judge_advance.load(Ordering::SeqCst), Ordering::SeqCst);
        let verdicts = self.verdicts.lock().unwrap().clone();
        Ok(ResponseOutcome {
            seq: 1,
            text: verdicts.to_string(),
            parsed: Some(verdicts),
            model: "fake-sm".into(),
            backend: "fake".into(),
            latency_ms: 0,
            cost: None,
        })
    }
    fn nudge(&self, job_id: &str) {
        self.nudges.lock().unwrap().push(job_id.into());
    }
    fn emitter(&self) -> &dyn EventEmitter {
        &self.emitter
    }
}

// ── Hooks ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_resolution_inserts_one_event_per_resolution_generation() {
    let db = fixture("evidence-hook-dedupe.db").await;
    crud::resolve(&db, &FixedClock(500), "i", Resolution::Closed, A)
        .await
        .unwrap();
    crud::resolve(&db, &FixedClock(510), "i", Resolution::Closed, A)
        .await
        .unwrap();
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM tenet_evidence_events").await,
        1,
        "resolving twice concludes the same thing once"
    );

    crud::unresolve(&db, &FixedClock(520), "i").await.unwrap();
    crud::resolve(&db, &FixedClock(530), "i", Resolution::Closed, A)
        .await
        .unwrap();
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM tenet_evidence_events").await,
        2,
        "a reopened and re-closed issue is a new conclusion"
    );

    // `complete` never reaches `resolve_terminal`, and still snapshots.
    crud::complete(&db, &FixedClock(540), "i2", B)
        .await
        .unwrap();
    let owner = db
        .query_one(
            "SELECT owner_device_id, source_uri, text FROM tenet_evidence_events
             WHERE dedupe_key = 'issue:i2:merged:0'",
            (),
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
        )
        .await
        .unwrap();
    assert_eq!(owner.0, B);
    assert_eq!(owner.1, "cairn://p/prj/8");
    assert!(owner
        .2
        .starts_with("Issue cairn://p/prj/8 \u{2014} Other work\nResolution: merged"));

    let first = db
        .query_one(
            "SELECT owner_device_id, text FROM tenet_evidence_events WHERE dedupe_key = 'issue:i:closed:0'",
            (),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(first.0, A);
    assert_eq!(
        first.1,
        "Issue cairn://p/prj/7 \u{2014} Move uploads\nResolution: closed\n\nUploads go straight to S3 now."
    );
}

#[tokio::test]
async fn the_issue_event_carries_the_latest_pull_request_and_is_capped() {
    let db = fixture("evidence-hook-pr.db").await;
    let long = "x".repeat(EVENT_TEXT_CHARS * 2);
    db.execute(
        "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, body, source_branch,
             target_branch, opened_at, updated_at)
         VALUES ('mr', 'j-design', 'p', 'i', 'Direct uploads', ?1, 'a', 'main', 5, 5)",
        (long,),
    )
    .await
    .unwrap();
    crud::resolve(&db, &FixedClock(500), "i", Resolution::Merged, A)
        .await
        .unwrap();
    let text = db
        .query_one("SELECT text FROM tenet_evidence_events", (), |row| {
            row.text(0)
        })
        .await
        .unwrap();
    assert!(
        text.contains("\n\nPull request: Direct uploads\nxxx"),
        "{text}"
    );
    assert!(
        text.contains("\n\nUploads go straight to S3 now.\n\n"),
        "{text}"
    );
    assert_eq!(text.chars().count(), EVENT_TEXT_CHARS);
}

#[tokio::test]
async fn a_long_description_never_crowds_out_the_pull_request() {
    let db = fixture("evidence-hook-long-description.db").await;
    let plan = format!(
        "Plan: publish through the content store. {}",
        "x".repeat(EVENT_TEXT_CHARS * 2)
    );
    db.execute("UPDATE issues SET description = ?1 WHERE id = 'i'", (plan,))
        .await
        .unwrap();
    db.execute(
        "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, body, source_branch,
             target_branch, opened_at, updated_at)
         VALUES ('mr', 'j-design', 'p', 'i', 'Direct uploads', 'Adopted direct S3 uploads instead.',
             'a', 'main', 5, 5)",
        (),
    )
    .await
    .unwrap();
    crud::resolve(&db, &FixedClock(500), "i", Resolution::Merged, A)
        .await
        .unwrap();
    let text = db
        .query_one("SELECT text FROM tenet_evidence_events", (), |row| {
            row.text(0)
        })
        .await
        .unwrap();
    assert!(
        text.ends_with("\n\nPull request: Direct uploads\nAdopted direct S3 uploads instead."),
        "what was done survives the cap: {text}"
    );
    assert!(text.contains("Plan: publish through the content store."));
    assert_eq!(text.chars().count(), EVENT_TEXT_CHARS);
}

async fn seed_era(db: &LocalDb, status: &str) {
    db.execute_script(&format!(
        "INSERT INTO thread_eras (job_id, seq, start_block, end_block, started_at, ended_at, summary,
             summary_status, summary_model, source_tokens, minted_generation, created_at)
           VALUES ('j-design', 1, 0, 10, 100, 200, 'mechanical', '{status}', NULL, 900, 1, 300);
         INSERT INTO thread_era_children (job_id, era_seq, child_issue_id, child_issue_uri, child_title, final_status)
           VALUES ('j-design', 1, 'i', 'cairn://p/prj/7', 'Move uploads', 'merged');"
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn a_generated_era_summary_snapshots_one_event_and_a_lost_upgrade_none() {
    let db = fixture("evidence-era-hook.db").await;
    seed_era(&db, "fallback").await;
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM tenet_evidence_events").await,
        0,
        "minting a fallback era is not a conclusion"
    );
    let job = crate::threads::era_worker::EraSummaryJob {
        job_id: "j-design".into(),
        seq: 1,
    };
    let upgraded = crate::threads::era_worker::record_generated_summary(
        &db,
        &db,
        &job,
        "Settled on direct uploads.",
        "m",
        A,
        900,
    )
    .await
    .unwrap();
    assert!(upgraded);
    let (uri, kind, key, owner, text) = db
        .query_one(
            "SELECT source_uri, source_kind, dedupe_key, owner_device_id, text FROM tenet_evidence_events",
            (),
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?, row.text(3)?, row.text(4)?)),
        )
        .await
        .unwrap();
    assert_eq!(uri, "cairn://p/prj/design/eras/1");
    assert_eq!(kind, "era");
    assert_eq!(key, "era:j-design:1");
    assert_eq!(owner, A);
    assert_eq!(
        text,
        "Thread era cairn://p/prj/design/eras/1\nSummary: Settled on direct uploads.\nChildren:\n- Move uploads (merged) cairn://p/prj/7"
    );

    // The era is generated now, so a second upgrade loses its compare-and-swap.
    let again =
        crate::threads::era_worker::record_generated_summary(&db, &db, &job, "Again.", "m", A, 901)
            .await
            .unwrap();
    assert!(!again);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM tenet_evidence_events").await,
        1
    );
}

#[tokio::test]
async fn an_era_event_lands_in_the_project_routed_database_when_it_differs() {
    let era_db = fixture("evidence-era-routed-a.db").await;
    let project_db = fixture("evidence-era-routed-b.db").await;
    seed_era(&era_db, "fallback").await;
    let job = crate::threads::era_worker::EraSummaryJob {
        job_id: "j-design".into(),
        seq: 1,
    };
    crate::threads::era_worker::record_generated_summary(
        &era_db,
        &project_db,
        &job,
        "Summary.",
        "m",
        A,
        900,
    )
    .await
    .unwrap();
    assert_eq!(
        count(&era_db, "SELECT COUNT(*) FROM tenet_evidence_events").await,
        0
    );
    assert_eq!(
        count(&project_db, "SELECT COUNT(*) FROM tenet_evidence_events").await,
        1
    );
}

// ── Evaluation ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn an_event_with_no_approved_tenets_is_skipped_without_a_network_call() {
    let db = fixture("evidence-no-tenets.db").await;
    let event = add_event(&db, "1", A, "work").await;
    let fake = Fake::new(A, 1_000, &db);
    assert_eq!(
        worker::process(&fake, &db, &event).await,
        Processed::Skipped("no approved tenets")
    );
    assert_eq!(event_row(&db, &event.id).await.0, "skipped");
    assert_eq!(fake.embeds(), 0);
    assert!(fake.judges().is_empty());
}

#[tokio::test]
async fn unavailable_embeddings_back_off_then_fail_after_five_attempts() {
    let db = fixture("evidence-embed-unavailable.db").await;
    approve(&db, 1, "t-design", "Uploads go through the store", &[]).await;
    let mut event = add_event(&db, "1", A, "work").await;
    let fake = Fake::new(A, 1_000, &db);
    fake.fail_call(0, EmbedFailure::NoToken);
    assert!(matches!(
        worker::process(&fake, &db, &event).await,
        Processed::Failed(_)
    ));
    let (state, attempts, next, error) = event_row(&db, &event.id).await;
    assert_eq!((state.as_str(), attempts, next), ("pending", 1, 1_060));
    assert!(error.unwrap().contains("token"));
    assert!(fake.judges().is_empty());
    assert_eq!(count(&db, "SELECT COUNT(*) FROM tenet_evidence").await, 0);

    // A gateway error is a failed attempt too; the fifth gives up.
    for attempt in 1..5 {
        event.attempts = attempt;
        let fake = Fake::new(A, 2_000, &db);
        fake.fail_call(0, EmbedFailure::Gateway);
        worker::process(&fake, &db, &event).await;
    }
    let (state, attempts, _, error) = event_row(&db, &event.id).await;
    assert_eq!((state.as_str(), attempts), ("failed", 5));
    assert!(error.unwrap().contains("503"));
}

#[tokio::test]
async fn tenet_vectors_saved_before_a_failure_persist() {
    let db = fixture("evidence-embed-chunks.db").await;
    for number in 1..=(EMBED_CHUNK as i64 + 1) {
        approve(&db, number, "t-design", &format!("Rule {number}"), &[]).await;
    }
    let event = add_event(&db, "1", A, "work").await;
    let fake = Fake::new(A, 1_000, &db);
    fake.fail_call(1, EmbedFailure::Gateway);
    worker::process(&fake, &db, &event).await;
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM resource_embeddings").await,
        EMBED_CHUNK as i64,
        "the first chunk was saved before the second failed"
    );
    assert_eq!(event_row(&db, &event.id).await.1, 1);

    // The retry embeds only the tenet still missing, then the event.
    let mut event = event;
    event.attempts = 1;
    let fake = Fake::new(A, 2_000, &db);
    worker::process(&fake, &db, &event).await;
    let calls = fake.embed_calls.lock().unwrap().clone();
    assert_eq!(calls[0].0.len(), 1);
    assert_eq!(calls[0].1, InputType::SearchDocument);
    assert_eq!(calls[1].1, InputType::SearchQuery);
}

/// A unit vector at cosine `similarity` from the first axis.
fn at_similarity(similarity: f32) -> [f32; 4] {
    [similarity, (1.0 - similarity * similarity).sqrt(), 0.0, 0.0]
}

#[tokio::test]
async fn no_tenet_above_the_similarity_floor_skips_the_judge() {
    let db = fixture("evidence-knn-floor.db").await;
    approve(&db, 1, "t-design", "LEASE rule", &[]).await;
    let below = add_event(&db, "1", A, "BELOW work").await;
    let above = add_event(&db, "2", A, "ABOVE work").await;
    let fake = Fake::new(A, 1_000, &db)
        .needle("LEASE", &[1.0, 0.0, 0.0, 0.0])
        .needle("BELOW", &at_similarity(KNN_FLOOR - 0.005))
        .needle("ABOVE", &at_similarity(KNN_FLOOR + 0.005));
    assert_eq!(
        worker::process(&fake, &db, &below).await,
        Processed::Skipped("no tenet is near enough")
    );
    assert!(fake.judges().is_empty());
    assert_eq!(event_row(&db, &below.id).await.0, "skipped");

    assert_eq!(
        worker::process(&fake, &db, &above).await,
        Processed::Done(0)
    );
    assert_eq!(fake.judges().len(), 1, "just above the floor is judged");
}

#[tokio::test]
async fn only_the_five_nearest_tenets_are_judged() {
    let db = fixture("evidence-top-k.db").await;
    let mut fake = Fake::new(A, 1_000, &db).needle("WORK", &[1.0, 0.0, 0.0, 0.0]);
    for number in 1..=7 {
        approve(&db, number, "t-design", &format!("NEAR{number} rule"), &[]).await;
        let x = 1.0 - number as f32 * 0.05;
        fake = fake.needle(
            &format!("NEAR{number} "),
            &[x, (1.0 - x * x).sqrt(), 0.0, 0.0],
        );
    }
    let event = add_event(&db, "1", A, "WORK").await;
    worker::process(&fake, &db, &event).await;
    let judged = fake.judges();
    assert_eq!(judged.len(), 1);
    let tenets = judged[0]["tenets"].as_str().unwrap();
    assert!(tenets.contains("### Tenet 5\n"), "{tenets}");
    assert!(!tenets.contains("### Tenet 6"), "{tenets}");
    assert!(tenets.contains("Chosen: NEAR1 rule"));
    assert!(!tenets.contains("NEAR6") && !tenets.contains("NEAR7"));
}

#[tokio::test]
async fn only_confident_supports_and_contradicts_verdicts_are_stored() {
    let db = fixture("evidence-confidence.db").await;
    for (number, word) in [(1, "ALPHA"), (2, "BETA"), (3, "GAMMA")] {
        approve(&db, number, "t-design", &format!("{word} rule"), &["other"]).await;
    }
    let event = add_event(&db, "5012", A, "ALPHA BETA GAMMA work").await;
    let fake = Fake::new(A, 1_000, &db).needle("", &[1.0, 0.0, 0.0, 0.0]);
    fake.set_verdicts(json!([
        {"tenet": 1, "verdict": "contradicts", "probability": 0.69},
        {"tenet": 2, "verdict": "supports", "probability": 0.7},
        {"tenet": 2, "verdict": "contradicts", "probability": 0.99},
        {"tenet": 3, "verdict": "neither", "probability": 0.95},
        {"tenet": 9, "verdict": "contradicts", "probability": 0.99}
    ]));
    assert_eq!(
        worker::process(&fake, &db, &event).await,
        Processed::Done(1)
    );
    let stored = db
        .query_all(
            "SELECT e.tenet_id, e.sign, e.confidence, e.model, e.owner_device_id, e.evaluated_at, e.source_uri
             FROM tenet_evidence e",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.f64(2)?,
                    row.text(3)?,
                    row.text(4)?,
                    row.i64(5)?,
                    row.text(6)?,
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(stored.len(), 1);
    let row = &stored[0];
    assert_eq!(row.0, "tenet-2");
    assert_eq!(row.1, "supports");
    assert!((row.2 - 0.7).abs() < 1e-9);
    assert_eq!(
        (row.3.as_str(), row.4.as_str(), row.5),
        ("fake-sm", A, 1_000)
    );
    assert_eq!(row.6, "cairn://p/prj/5012");
    assert_eq!(event_row(&db, &event.id).await.0, "done");
    assert_eq!(fake.emitter.events_named("db-change").len(), 1);
}

#[tokio::test]
async fn only_the_owning_machine_evaluates_an_event() {
    let db = fixture("evidence-two-runners-events.db").await;
    approve(&db, 1, "t-design", "Rule", &[]).await;
    let event = add_event(&db, "1", A, "work").await;
    let base = Fake::new(A, 1_000, &db).needle("", &[1.0, 0.0, 0.0, 0.0]);

    let runner_b = base.as_device(B);
    assert!(worker::drain_due(&runner_b).await.is_empty());
    assert!(runner_b.judges().is_empty());
    assert_eq!(runner_b.embeds(), 0);
    assert_eq!(
        event_row(&db, &event.id).await,
        ("pending".into(), 0, 0, None)
    );

    let runner_a = base.as_device(A);
    assert_eq!(worker::drain_due(&runner_a).await.len(), 1);
    assert_eq!(runner_a.judges().len(), 1);
    assert_eq!(event_row(&db, &event.id).await.0, "done");
}

#[tokio::test]
async fn replayed_rows_with_derived_ids_upsert_instead_of_colliding() {
    let db = fixture("evidence-replay.db").await;
    let tenet = approve(&db, 1, "t-design", "Rule", &[]).await;
    let event = add_event(&db, "1", A, "work").await;
    // The same logical event, written again by another machine.
    add_event(&db, "1", B, "work").await;
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM tenet_evidence_events").await,
        1
    );
    let id = stable_id("tenet-evidence", &format!("{tenet}:{}", event.source_uri));
    for confidence in [0.8, 0.9] {
        // Turso Sync replays a remote row as an upsert on its primary key.
        db.execute(
            "INSERT INTO tenet_evidence (id, project_id, tenet_id, event_id, source_uri, sign,
                 confidence, model, owner_device_id, evaluated_at)
             VALUES (?1, 'p', ?2, ?3, ?4, 'contradicts', ?5, 'm', 'device-a', 10)
             ON CONFLICT(id) DO UPDATE SET confidence = excluded.confidence",
            cairn_db::turso::params![
                id.as_str(),
                tenet.as_str(),
                event.id.as_str(),
                event.source_uri.as_str(),
                confidence
            ],
        )
        .await
        .unwrap();
    }
    assert_eq!(count(&db, "SELECT COUNT(*) FROM tenet_evidence").await, 1);
    assert_eq!(stable_id("x", "y"), stable_id("x", "y"));
    assert_ne!(stable_id("x", "y"), stable_id("x", "z"));
}

#[tokio::test]
async fn the_read_projection_carries_evidence_newest_first() {
    let db = fixture("evidence-projection.db").await;
    let tenet = approve(&db, 1, "t-design", "Rule", &[]).await;
    contradiction(&db, &tenet, "cairn://p/prj/1", 0.8, 100, A).await;
    contradiction(&db, &tenet, "cairn://p/prj/2", 0.9, 300, A).await;
    let view = crate::tenets::get(&db, "p", 1).await.unwrap().unwrap();
    let sources: Vec<_> = view
        .evidence
        .iter()
        .map(|e| e.source_uri.as_str())
        .collect();
    assert_eq!(sources, ["cairn://p/prj/2", "cairn://p/prj/1"]);
    assert_eq!(view.evidence[0].sign, "contradicts");
    assert!((view.evidence[0].confidence - 0.9).abs() < 1e-9);
}

// ── The contradiction wake ──────────────────────────────────────────────────

async fn contradiction(
    db: &LocalDb,
    tenet: &str,
    source: &str,
    confidence: f64,
    at: i64,
    owner: &str,
) {
    let event = add_event(db, &format!("e-{source}"), owner, "work").await;
    db.execute(
        "INSERT INTO tenet_evidence (id, project_id, tenet_id, event_id, source_uri, sign,
             confidence, model, owner_device_id, evaluated_at)
         VALUES (?1, 'p', ?2, ?3, ?4, 'contradicts', ?5, 'm', ?6, ?7)",
        cairn_db::turso::params![
            stable_id("tenet-evidence", &format!("{tenet}:{source}")),
            tenet,
            event.id.as_str(),
            source,
            confidence,
            owner,
            at
        ],
    )
    .await
    .unwrap();
}

async fn messages(db: &LocalDb) -> Vec<String> {
    db.query_all(
        "SELECT content FROM messages WHERE recipient_run_id = 'run-design' ORDER BY created_at, id",
        (),
        |row| row.text(0),
    )
    .await
    .unwrap()
}

async fn alerted_at(db: &LocalDb, tenet: &str) -> Option<i64> {
    db.query_one(
        "SELECT contradiction_alerted_at FROM tenets WHERE id = ?1",
        (tenet.to_string(),),
        |row| row.opt_i64(0),
    )
    .await
    .unwrap()
}

/// The wake that consumed the contradiction from `source`, if any has.
async fn consumed_by(db: &LocalDb, source: &str) -> Option<String> {
    db.query_one(
        "SELECT consumed_by_wake FROM tenet_evidence WHERE source_uri = ?1",
        (source.to_string(),),
        |row| row.opt_text(0),
    )
    .await
    .unwrap()
}

/// Whether a wake message lists the contradiction from `cairn://p/prj/<source>`.
fn lists(message: &str, source: &str) -> bool {
    message.contains(&format!("- contradicts cairn://p/prj/{source} ("))
}

/// Bring the contradiction from `source`, and the event it cites, from one
/// replica's database into another, as Turso Sync applies a pulled row: an
/// upsert on its primary key, carrying the row exactly as its writer left it.
async fn replicate(from: &LocalDb, to: &LocalDb, source: &str) {
    let event = from
        .query_one(
            "SELECT ev.id, ev.project_id, ev.source_uri, ev.source_kind, ev.dedupe_key,
                    ev.owner_device_id, ev.text, ev.state, ev.created_at
             FROM tenet_evidence_events ev JOIN tenet_evidence e ON e.event_id = ev.id
             WHERE e.source_uri = ?1",
            (source.to_string(),),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.text(4)?,
                    row.text(5)?,
                    row.text(6)?,
                    row.text(7)?,
                    row.i64(8)?,
                ))
            },
        )
        .await
        .unwrap();
    to.execute(
        "INSERT INTO tenet_evidence_events (id, project_id, source_uri, source_kind, dedupe_key,
             owner_device_id, text, state, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(id) DO UPDATE SET state = excluded.state",
        cairn_db::turso::params![
            event.0, event.1, event.2, event.3, event.4, event.5, event.6, event.7, event.8
        ],
    )
    .await
    .unwrap();
    let evidence = from
        .query_one(
            "SELECT id, project_id, tenet_id, event_id, source_uri, sign, confidence, model,
                    owner_device_id, evaluated_at
             FROM tenet_evidence WHERE source_uri = ?1",
            (source.to_string(),),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.text(4)?,
                    row.text(5)?,
                    row.f64(6)?,
                    row.text(7)?,
                    row.text(8)?,
                    row.i64(9)?,
                ))
            },
        )
        .await
        .unwrap();
    to.execute(
        "INSERT INTO tenet_evidence (id, project_id, tenet_id, event_id, source_uri, sign,
             confidence, model, owner_device_id, evaluated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(id) DO UPDATE SET confidence = excluded.confidence",
        cairn_db::turso::params![
            evidence.0, evidence.1, evidence.2, evidence.3, evidence.4, evidence.5, evidence.6,
            evidence.7, evidence.8, evidence.9
        ],
    )
    .await
    .unwrap();
}

const T0: i64 = 1_760_000_000;

#[tokio::test]
async fn the_wake_fires_once_from_the_owner_of_the_newest_contradiction_after_it_settles() {
    let db = fixture("evidence-two-runners-wake.db").await;
    let tenet = approve(
        &db,
        12,
        "t-design",
        "Artifacts go through the store",
        &["Direct S3"],
    )
    .await;
    contradiction(&db, &tenet, "cairn://p/prj/5012", 0.86, T0, A).await;
    contradiction(&db, &tenet, "cairn://p/prj/5013", 0.78, T0 + 10, B).await;

    let a = Fake::new(A, T0 + 10 + WAKE_SETTLE_SECS - 1, &db);
    let b = a.as_device(B);
    assert_eq!(wake::recheck_contradictions(&a).await, 0);
    assert_eq!(wake::recheck_contradictions(&b).await, 0, "not settled yet");
    assert!(messages(&db).await.is_empty());

    let settled = T0 + 10 + WAKE_SETTLE_SECS;
    a.now.store(settled, Ordering::SeqCst);
    b.now.store(settled, Ordering::SeqCst);
    assert_eq!(
        wake::recheck_contradictions(&a).await,
        0,
        "A does not own the newest row"
    );
    assert_eq!(wake::recheck_contradictions(&b).await, 1);
    assert_eq!(wake::recheck_contradictions(&b).await, 0);
    assert_eq!(wake::recheck_contradictions(&a).await, 0);

    let sent = messages(&db).await;
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0],
        "Tenet #12 is contradicted by concluded work (weight 1.64, threshold 1.2).\n\n\
         #12 \u{2014} Artifacts go through the store\n  rejected: Direct S3\n\
         Contradicting evidence since approval:\n\
         - contradicts cairn://p/prj/5012 (0.86, 2025-10-09)\n\
         - contradicts cairn://p/prj/5013 (0.78, 2025-10-09)\n\
         Full record: cairn://p/prj/tenets/12\n\n\
         judge: supersede (propose a successor with supersedes:12) or correct course"
    );
    assert_eq!(
        alerted_at(&db, &tenet).await,
        Some(settled),
        "the last alert is when the wake fired"
    );
    let wake = consumed_by(&db, "cairn://p/prj/5012")
        .await
        .expect("consumed");
    assert_eq!(
        consumed_by(&db, "cairn://p/prj/5013").await.as_deref(),
        Some(wake.as_str()),
        "the wake consumes every row it counted"
    );
    assert_eq!(
        count(
            &db,
            &format!("SELECT COUNT(*) FROM messages WHERE id = '{wake}'")
        )
        .await,
        1,
        "rows are stamped with the wake's message id"
    );
    assert_eq!(*b.nudges.lock().unwrap(), ["j-design".to_string()]);
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM attention_pushes WHERE recipient = 'j-design' AND key = 'tenet-contradiction:tenet-12'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn a_second_wake_needs_contradictions_the_first_did_not_count() {
    let db = fixture("evidence-second-wake.db").await;
    let tenet = approve(&db, 1, "t-design", "Rule", &[]).await;
    contradiction(&db, &tenet, "cairn://p/prj/1", 0.9, T0, A).await;
    contradiction(&db, &tenet, "cairn://p/prj/2", 0.9, T0, A).await;
    let fake = Fake::new(A, T0 + DAY, &db);
    assert_eq!(wake::recheck_contradictions(&fake).await, 1);
    fake.now.store(T0 + 2 * DAY, Ordering::SeqCst);
    assert_eq!(wake::recheck_contradictions(&fake).await, 0, "nothing new");

    contradiction(&db, &tenet, "cairn://p/prj/3", 0.7, T0 + 2 * DAY, A).await;
    fake.now.store(T0 + 3 * DAY, Ordering::SeqCst);
    assert_eq!(
        wake::recheck_contradictions(&fake).await,
        0,
        "one new contradiction weighs less than the threshold"
    );
    contradiction(&db, &tenet, "cairn://p/prj/4", 0.9, T0 + 2 * DAY, A).await;
    assert_eq!(wake::recheck_contradictions(&fake).await, 1);
    // Both wakes can land in the same wall-clock second, where message order
    // falls back to their derived ids, so each is found by what it says.
    let sent = messages(&db).await;
    assert_eq!(sent.len(), 2);
    let first = sent
        .iter()
        .find(|message| message.contains("since approval:"))
        .unwrap_or_else(|| panic!("no first wake: {sent:?}"));
    assert!(lists(first, "1") && lists(first, "2"), "{first}");
    assert!(!lists(first, "3") && !lists(first, "4"), "{first}");
    // The second counts only what the first did not consume, and dates the
    // last alert by when the first fired (T0 + DAY, 2025-10-10).
    let second = sent
        .iter()
        .find(|message| message.contains("not counted by the last alert (2025-10-10):"))
        .unwrap_or_else(|| panic!("no second wake: {sent:?}"));
    assert!(lists(second, "3") && lists(second, "4"), "{second}");
    assert!(!lists(second, "1") && !lists(second, "2"), "{second}");
}

#[tokio::test]
async fn old_contradictions_decay_below_the_threshold() {
    let db = fixture("evidence-decay.db").await;
    let tenet = approve(&db, 1, "t-design", "Rule", &[]).await;
    contradiction(&db, &tenet, "cairn://p/prj/1", 1.0, T0, A).await;
    contradiction(&db, &tenet, "cairn://p/prj/2", 1.0, T0, A).await;
    let fake = Fake::new(A, T0 + 90 * DAY, &db);
    assert_eq!(wake::recheck_contradictions(&fake).await, 0);
    assert!((decayed(1.0, T0, T0 + 30 * DAY) - 0.5).abs() < 1e-9);
    assert_eq!(alerted_at(&db, &tenet).await, None);
    assert_eq!(consumed_by(&db, "cairn://p/prj/1").await, None);
}

#[tokio::test]
async fn a_superseded_tenet_never_wakes() {
    let db = fixture("evidence-superseded.db").await;
    let tenet = approve(&db, 1, "t-design", "Rule", &[]).await;
    contradiction(&db, &tenet, "cairn://p/prj/1", 0.9, T0, A).await;
    contradiction(&db, &tenet, "cairn://p/prj/2", 0.9, T0, A).await;
    db.execute("UPDATE tenets SET status = 'superseded'", ())
        .await
        .unwrap();
    let fake = Fake::new(A, T0 + DAY, &db);
    assert_eq!(wake::recheck_contradictions(&fake).await, 0);
    assert!(messages(&db).await.is_empty());
}

#[tokio::test]
async fn an_unreachable_thread_keeps_the_wake_owed_until_it_can_be_reached() {
    let db = fixture("evidence-unreachable.db").await;
    let tenet = approve(&db, 1, "t-design", "Rule", &[]).await;
    contradiction(&db, &tenet, "cairn://p/prj/1", 0.9, T0, A).await;
    contradiction(&db, &tenet, "cairn://p/prj/2", 0.9, T0, A).await;
    let fake = Fake::new(A, T0 + DAY, &db);

    db.execute(
        "UPDATE threads SET status = 'closed' WHERE id = 't-design'",
        (),
    )
    .await
    .unwrap();
    assert_eq!(wake::recheck_contradictions(&fake).await, 0);
    assert_eq!(
        consumed_by(&db, "cairn://p/prj/1").await,
        None,
        "a closed thread leaves every row unconsumed"
    );
    assert_eq!(alerted_at(&db, &tenet).await, None);

    db.execute(
        "UPDATE threads SET status = 'active' WHERE id = 't-design'",
        (),
    )
    .await
    .unwrap();
    db.execute("DELETE FROM runs WHERE id = 'run-design'", ())
        .await
        .unwrap();
    assert_eq!(wake::recheck_contradictions(&fake).await, 0);
    assert_eq!(
        consumed_by(&db, "cairn://p/prj/2").await,
        None,
        "no run leaves every row unconsumed"
    );

    db.execute(
        "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
         VALUES ('run-design', 'p', 'j-design', 'live', 2, 2)",
        (),
    )
    .await
    .unwrap();
    assert_eq!(wake::recheck_contradictions(&fake).await, 1);
    assert_eq!(alerted_at(&db, &tenet).await, Some(T0 + DAY));
    assert!(consumed_by(&db, "cairn://p/prj/1").await.is_some());
}

#[tokio::test]
async fn a_contradiction_that_lands_after_a_wake_counts_toward_the_next() {
    let db = fixture("evidence-late-contradiction.db").await;
    let tenet = approve(&db, 1, "t-design", "Rule", &[]).await;
    contradiction(&db, &tenet, "cairn://p/prj/1", 0.9, T0, B).await;
    contradiction(&db, &tenet, "cairn://p/prj/2", 0.9, T0, B).await;
    let event = add_event(&db, "late", A, "late work").await;

    // A starts judging at T0+200. While its judge is out, B's sweep fires the
    // wake over the rows it has, and A's clock moves to T0+300.
    let runner_b = Arc::new(Fake::new(B, T0 + 250, &db));
    let runner_a = Fake::new(A, T0 + 200, &db).needle("", &[1.0, 0.0, 0.0, 0.0]);
    runner_a.set_verdicts(json!([{"tenet": 1, "verdict": "contradicts", "probability": 0.9}]));
    runner_a.judge_advance.store(100, Ordering::SeqCst);
    *runner_a.during_judge.lock().unwrap() = Some(runner_b.clone());
    assert_eq!(
        worker::process(&runner_a, &db, &event).await,
        Processed::Done(1)
    );
    assert_eq!(
        messages(&db).await.len(),
        1,
        "B fired while A's judge was out"
    );

    let late_at = db
        .query_one(
            "SELECT evaluated_at FROM tenet_evidence WHERE source_uri = 'cairn://p/prj/late'",
            (),
            |row| row.i64(0),
        )
        .await
        .unwrap();
    assert_eq!(
        late_at,
        T0 + 300,
        "dated when it landed, not when judging began"
    );
    assert_eq!(
        consumed_by(&db, "cairn://p/prj/late").await,
        None,
        "B's wake did not count it"
    );

    contradiction(&db, &tenet, "cairn://p/prj/3", 0.9, T0 + 400, A).await;
    runner_a
        .now
        .store(T0 + 400 + WAKE_SETTLE_SECS, Ordering::SeqCst);
    assert_eq!(wake::recheck_contradictions(&runner_a).await, 1);
    // Both messages land in the same wall-clock second, so read them by
    // content rather than order.
    let sent = messages(&db).await;
    assert_eq!(sent.len(), 2);
    let counting_late: Vec<_> = sent
        .iter()
        .filter(|message| message.contains("- contradicts cairn://p/prj/late ("))
        .collect();
    assert_eq!(
        counting_late.len(),
        1,
        "the late contradiction is counted by the next wake: {sent:?}"
    );
    assert!(counting_late[0].contains("- contradicts cairn://p/prj/3 ("));
}

#[tokio::test]
async fn a_contradiction_replicated_in_after_a_wake_counts_toward_the_next_however_old() {
    // Two replicas of one team project, each with its own database view.
    let replica_a = fixture("evidence-delayed-replica-a.db").await;
    let replica_b = fixture("evidence-delayed-replica-b.db").await;
    let tenet = approve(&replica_a, 1, "t-design", "Rule", &[]).await;
    approve(&replica_b, 1, "t-design", "Rule", &[]).await;

    // A records contradictions at T0+100 and T0+110. B, offline, records one
    // at T0+105 that has not reached A.
    contradiction(&replica_a, &tenet, "cairn://p/prj/100", 0.9, T0 + 100, A).await;
    contradiction(&replica_a, &tenet, "cairn://p/prj/110", 0.9, T0 + 110, A).await;
    contradiction(&replica_b, &tenet, "cairn://p/prj/105", 0.9, T0 + 105, B).await;

    let fired_at = T0 + 110 + WAKE_SETTLE_SECS;
    let runner_a = Fake::new(A, fired_at, &replica_a);
    let runner_b = Fake::new(B, fired_at, &replica_b);
    assert_eq!(wake::recheck_contradictions(&runner_a).await, 1);
    assert_eq!(
        wake::recheck_contradictions(&runner_b).await,
        0,
        "B's one row weighs less than the threshold"
    );
    let first = messages(&replica_a).await;
    assert_eq!(first.len(), 1);
    assert!(
        lists(&first[0], "100") && lists(&first[0], "110"),
        "{first:?}"
    );
    assert!(!lists(&first[0], "105"), "{first:?}");

    // B's push lands after A's wake, carrying a row older than every row that
    // wake counted and older than the alert itself.
    replicate(&replica_b, &replica_a, "cairn://p/prj/105").await;
    assert!(alerted_at(&replica_a, &tenet).await.unwrap() > T0 + 105);
    assert_eq!(
        consumed_by(&replica_a, "cairn://p/prj/105").await,
        None,
        "no wake counted it, so it is still owed"
    );
    runner_a.now.store(fired_at + DAY, Ordering::SeqCst);
    assert_eq!(
        wake::recheck_contradictions(&runner_a).await,
        0,
        "alone it weighs less than the threshold, and B owns it"
    );

    // One more contradiction crosses the threshold, and the next wake lists
    // exactly the rows no wake has counted.
    contradiction(&replica_a, &tenet, "cairn://p/prj/300", 0.9, T0 + 300, A).await;
    runner_a
        .now
        .store(T0 + 300 + WAKE_SETTLE_SECS, Ordering::SeqCst);
    assert_eq!(wake::recheck_contradictions(&runner_a).await, 1);
    let sent = messages(&replica_a).await;
    assert_eq!(sent.len(), 2);
    let next = sent
        .iter()
        .find(|message| message.contains("not counted by the last alert (2025-10-09):"))
        .unwrap_or_else(|| panic!("no second wake: {sent:?}"));
    assert!(lists(next, "105") && lists(next, "300"), "{next}");
    assert!(!lists(next, "100") && !lists(next, "110"), "{next}");

    let first_wake = consumed_by(&replica_a, "cairn://p/prj/100").await.unwrap();
    let next_wake = consumed_by(&replica_a, "cairn://p/prj/105").await.unwrap();
    assert_ne!(first_wake, next_wake);
    assert_eq!(
        consumed_by(&replica_a, "cairn://p/prj/110")
            .await
            .as_deref(),
        Some(first_wake.as_str())
    );
    assert_eq!(
        consumed_by(&replica_a, "cairn://p/prj/300")
            .await
            .as_deref(),
        Some(next_wake.as_str())
    );
    assert_eq!(wake::recheck_contradictions(&runner_a).await, 0);
}

#[tokio::test]
async fn a_wake_whose_consumption_was_lost_is_restamped_not_resent() {
    let db = fixture("evidence-lost-consumption.db").await;
    let tenet = approve(&db, 1, "t-design", "Rule", &[]).await;
    contradiction(&db, &tenet, "cairn://p/prj/1", 0.9, T0, A).await;
    contradiction(&db, &tenet, "cairn://p/prj/2", 0.9, T0, A).await;
    let fake = Fake::new(A, T0 + DAY, &db);
    assert_eq!(wake::recheck_contradictions(&fake).await, 1);
    let wake = consumed_by(&db, "cairn://p/prj/1").await.unwrap();

    // A stale replay of the rows as first written clears their stamps.
    db.execute(
        "UPDATE tenet_evidence SET consumed_by_wake = NULL, consumed_at = NULL",
        (),
    )
    .await
    .unwrap();
    assert_eq!(
        wake::recheck_contradictions(&fake).await,
        0,
        "the same rows are the same wake, already sent"
    );
    assert_eq!(messages(&db).await.len(), 1);
    assert_eq!(fake.nudges.lock().unwrap().len(), 1);
    for source in ["cairn://p/prj/1", "cairn://p/prj/2"] {
        assert_eq!(
            consumed_by(&db, source).await.as_deref(),
            Some(wake.as_str())
        );
    }
}

// ── The judgement ───────────────────────────────────────────────────────────

#[test]
fn the_judge_definition_validates_and_its_example_conforms() {
    let definition = judge::definition();
    definition.validate().unwrap();
    assert_eq!(definition.examples.len(), 1);
    assert_eq!(judge::schema()["type"], "object");
    let rendered = definition
        .render(&json!({"event": "E", "tenets": "T"}))
        .unwrap();
    assert!(rendered.ends_with("## Concluded work\nE\n\n## Tenets\nT"));
}

#[test]
fn a_missing_verdict_reads_neither_and_the_first_entry_wins() {
    let outcome = ResponseOutcome {
        seq: 1,
        text: String::new(),
        parsed: Some(json!({"verdicts": [
            {"tenet": 2, "verdict": "supports", "probability": 0.8},
            {"tenet": 2, "verdict": "contradicts", "probability": 0.9},
            {"tenet": 0, "verdict": "contradicts", "probability": 0.9}
        ]})),
        model: "m".into(),
        backend: "b".into(),
        latency_ms: 0,
        cost: None,
    };
    let verdicts = judge::parse(&outcome, 2);
    assert_eq!(verdicts[0].sign, judge::Sign::Neither);
    assert_eq!(verdicts[1].sign, judge::Sign::Supports);
}

#[test]
fn an_arc_embeds_its_intent_and_working_prose() {
    let text = crate::embeddings::artifact_embed_text(
        "arc",
        &json!({"current_intent": "Ship direct uploads", "working": "Benchmarking the store"}),
    )
    .unwrap();
    assert!(text.contains("Ship direct uploads"));
    assert!(text.contains("Benchmarking the store"));
}
