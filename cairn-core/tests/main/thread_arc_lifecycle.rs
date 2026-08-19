//! End-to-end proof that a thread can create and keep its arc, driven through
//! the real `write` verb dispatcher (`handle_write`) against a thread that has
//! never run before.
//!
//! The arc is the only part of a thread's memory that survives rolling
//! compaction at full fidelity, so a thread that cannot create one has no
//! durable memory at all. A thread session job carries the `arc` preset as its
//! output contract, and this exercises that contract end to end:
//!
//! - a first-ever `create` of `arc` resolves its schema and stores v1 (the
//!   preset-registry failure this covers only ever reached a thread's FIRST
//!   write, which is why migrated threads never saw it);
//! - the arc is a living document: writing it auto-confirms and never arms the
//!   turn-ending handoff a terminal artifact arms;
//! - normal lifecycle patches append one ruling by `payload.ruling` (Cairn mints
//!   the slug) or edit one by slug; guarded migration coverage retains wholesale
//!   array replacement;
//! - ruling operations compose atomically with ordinary arc field updates;
//! - the schema is enforced, not merely resolvable.

use crate::common;
use crate::common::orchestrator;

use cairn_core::internal::storage::{DbError, LocalDb, RowExt};
use serde_json::{json, Value};
use std::sync::Arc;

const ARC_URI: &str = "cairn://p/thr/library/arc";

/// A project and a thread with no session job: the state every thread is in
/// before its first turn, and the only state in which the arc's contract is
/// resolved from scratch.
async fn seed(db: &LocalDb) {
    db.write(|conn| {
        Box::pin(async move {
            conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
            conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','thr','/tmp/p',1,1)", ()).await?;
            conn.execute("INSERT INTO threads (id, project_id, name, status, created_at, updated_at) VALUES ('t-1','p-1','library','active',1,1)", ()).await?;
            Ok::<_, DbError>(())
        })
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn self_referential_provenance_is_stored_byte_exact() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let created = common::change_resource(
        &orch,
        write_to(
            "create",
            json!({
                "current_intent": "Preserve authored provenance.",
                "rulings": [],
                "open_questions": []
            }),
        ),
    )
    .await;
    assert!(created.contains("version 1"), "got: {created}");

    let appended = common::change_resource(
        &orch,
        write_to(
            "patch",
            json!({"ruling": {
                "text": "canonical provenance remains canonical",
                "status": "accepted",
                "rationale": "stored artifact data is authored data",
                "provenance": ["cairn://p/thr/library"]
            }}),
        ),
    )
    .await;
    assert!(appended.contains("version 2"), "got: {appended}");

    let raw = latest_arc_raw(&db).await;
    assert!(
        raw.contains(r#""provenance":["cairn://p/thr/library"]"#),
        "canonical self-reference missing from raw stored data: {raw}"
    );
    assert!(
        !raw.contains("cairn:~/"),
        "storage must never rewrite canonical provenance: {raw}"
    );
}

/// The byte-exact stored data of the thread's latest arc version.
async fn latest_arc_raw(db: &LocalDb) -> String {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT data FROM artifacts WHERE output_name = 'arc' ORDER BY version DESC LIMIT 1",
                    (),
                )
                .await?;
            rows.next().await?.map(|row| row.text(0)).transpose()
        })
    })
    .await
    .unwrap()
    .expect("an arc version is stored")
}

fn write_to(mode: &str, payload: Value) -> Value {
    json!([{ "target": ARC_URI, "mode": mode, "payload": payload }])
}

/// The stored data of the thread's latest arc version.
async fn latest_arc(db: &LocalDb) -> Value {
    let raw = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT data FROM artifacts WHERE output_name = 'arc' \
                         ORDER BY version DESC LIMIT 1",
                        (),
                    )
                    .await?;
                rows.next().await?.map(|row| row.text(0)).transpose()
            })
        })
        .await
        .unwrap()
        .expect("an arc version is stored");
    serde_json::from_str(&raw).expect("stored arc data is JSON")
}

/// The session job's persisted output contract.
async fn arc_contract(db: &LocalDb) -> Option<String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT output_contract FROM jobs WHERE thread_id = 't-1' AND uri_segment = 'thread'",
                    (),
                )
                .await?;
            rows.next().await?.map(|row| row.opt_text(0)).transpose()
        })
    })
    .await
    .unwrap()
    .flatten()
}

fn ruling(text: &str, status: &str) -> Value {
    json!({
        "text": text,
        "status": status,
        "rationale": "argued in the terms it was actually argued",
        "provenance": ["cairn://p/thr/1"]
    })
}

fn slugs(arc: &Value) -> Vec<String> {
    arc["rulings"]
        .as_array()
        .expect("rulings is an array")
        .iter()
        .map(|ruling| ruling["slug"].as_str().expect("a minted slug").to_string())
        .collect()
}

#[tokio::test]
async fn ruling_operations_compose_with_arc_field_updates_in_one_version() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let created = common::change_resource(
        &orch,
        write_to(
            "create",
            json!({
                "current_intent": "Choose a pack identity.",
                "rulings": [ruling("packs resolve by content hash", "accepted")],
                "open_questions": []
            }),
        ),
    )
    .await;
    assert!(created.contains("version 1"), "got: {created}");

    let appended = common::change_resource(
        &orch,
        write_to(
            "patch",
            json!({
                "ruling": ruling("published packs are immutable", "accepted"),
                "current_intent": "Name packs without weakening immutability.",
                "open_questions": [{"question": "who owns aliases?", "provenance": ["cairn://p/thr/2"]}]
            }),
        ),
    )
    .await;
    assert!(
        appended.contains("version 2"),
        "one mixed write must land one version: {appended}"
    );
    let arc = latest_arc(&db).await;
    assert_eq!(
        arc["current_intent"],
        "Name packs without weakening immutability."
    );
    assert_eq!(arc["open_questions"].as_array().map(Vec::len), Some(1));
    assert_eq!(arc["rulings"].as_array().map(Vec::len), Some(2));
    let appended_slug = arc["rulings"][1]["slug"].as_str().expect("slug minted");

    let edited = common::change_resource(
        &orch,
        write_to(
            "patch",
            json!({
                "ruling_slug": appended_slug,
                "patch": {"status": "superseded", "rationale": "Aliases now carry this responsibility"},
                "current_intent": "Ship the alias model."
            }),
        ),
    )
    .await;
    assert!(
        edited.contains("version 3"),
        "one mixed edit must land one version: {edited}"
    );
    let arc = latest_arc(&db).await;
    assert_eq!(arc["current_intent"], "Ship the alias model.");
    assert_eq!(arc["rulings"][1]["status"], "superseded");

    let ambiguous = common::change_resource(
        &orch,
        write_to(
            "patch",
            json!({
                "ruling": ruling("a third decision", "accepted"),
                "rulings": []
            }),
        ),
    )
    .await;
    assert!(
        ambiguous.contains("both write the rulings array"),
        "got: {ambiguous}"
    );
    assert!(
        !ambiguous.contains("version 4"),
        "ambiguous writes must not store a version: {ambiguous}"
    );
    assert_eq!(
        latest_arc(&db).await,
        arc,
        "a refused write leaves v3 unchanged"
    );
}

#[tokio::test]
async fn a_new_thread_creates_and_keeps_its_arc() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    let orch = orchestrator(&temp, db.clone());

    // --- first-ever create: the schema resolves and v1 lands ---------------
    let created = common::change_resource(
        &orch,
        write_to(
            "create",
            json!({
                "current_intent": "Standing up the resource library.",
                "working": "reading the pack format",
                "rulings": [ruling("packs resolve by content hash", "accepted")],
                "open_questions": [{"question": "who owns pack GC?", "provenance": ["cairn://p/thr/2"]}]
            }),
        ),
    )
    .await;
    assert!(
        created.contains("version 1"),
        "a thread's first arc create must land v1, got: {created}"
    );
    assert!(
        !created.contains("Unknown preset schema"),
        "the arc's schema must resolve by name, got: {created}"
    );
    // A living document is never gated and never hands the turn back.
    assert!(
        !created.contains("awaiting user confirmation"),
        "the arc auto-confirms, got: {created}"
    );
    assert!(
        !created.contains("this turn now ends"),
        "writing the arc must never end the thread's turn, got: {created}"
    );
    assert_eq!(
        arc_contract(&db).await.as_deref(),
        Some(r#"{"schemaType":"arc"}"#),
        "the session job carries the arc contract by preset name"
    );

    // --- append one ruling without resending the array ---------------------
    let appended = common::change_resource(
        &orch,
        write_to(
            "patch",
            json!({"ruling": ruling("a pack is immutable once published", "accepted")}),
        ),
    )
    .await;
    assert!(appended.contains("version 2"), "got: {appended}");
    let arc = latest_arc(&db).await;
    let minted = slugs(&arc);
    assert_eq!(minted.len(), 2, "both rulings survive: {arc}");
    assert_eq!(
        arc["current_intent"], "Standing up the resource library.",
        "a ruling append leaves the rest of the arc alone: {arc}"
    );

    // --- guarded bulk migration, carrying the stable slugs -----------------
    let replaced = common::change_resource(
        &orch,
        write_to(
            "patch",
            json!({"rulings": [{
                "slug": minted[0],
                "text": "packs resolve by content hash",
                "status": "superseded",
                "rationale": "the catalog now addresses them by name",
                "provenance": ["cairn://p/thr/1", "cairn://p/thr/3"]
            }]}),
        ),
    )
    .await;
    assert!(replaced.contains("version 3"), "got: {replaced}");
    let arc = latest_arc(&db).await;
    assert_eq!(
        arc["rulings"].as_array().map(Vec::len),
        Some(1),
        "a provided array replaces the stored one wholesale: {arc}"
    );
    assert_eq!(arc["rulings"][0]["status"], "superseded");
    assert_eq!(arc["rulings"][0]["slug"], minted[0].as_str());

    // --- field merge: intent moves, decisions stay -------------------------
    let moved = common::change_resource(
        &orch,
        write_to(
            "patch",
            json!({"current_intent": "Packs are named, not hashed."}),
        ),
    )
    .await;
    assert!(moved.contains("version 4"), "got: {moved}");
    let arc = latest_arc(&db).await;
    assert_eq!(arc["current_intent"], "Packs are named, not hashed.");
    assert_eq!(
        arc["rulings"].as_array().map(Vec::len),
        Some(1),
        "a field merge keeps the fields it did not name: {arc}"
    );
    assert_eq!(arc["working"], "reading the pack format");

    // The read advertises the contract the write enforces. Without it the arc's
    // fields are guesswork from the role prompt alone, which is how a thread
    // ends up parking arc content somewhere that gets summarized away.
    let rendered = common::read_resource(&orch, ARC_URI).await;
    assert!(
        rendered.contains("Packs are named, not hashed."),
        "the arc's own data renders: {rendered}"
    );
    assert!(
        rendered.contains("`current_intent(str)`"),
        "the arc advertises its required field: {rendered}"
    );
    assert!(
        rendered.contains("append one ruling"),
        "the arc advertises its ruling actions: {rendered}"
    );
}

#[tokio::test]
async fn a_thread_minted_before_the_arc_had_a_contract_gets_one() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    // The pre-cutover shape: a live session job carrying no output contract, so
    // its arc writes were stored with nothing validating them and its arc read
    // had no fields to state.
    db.write(|conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs (id, thread_id, project_id, status, agent_config_id, node_name, uri_segment, created_at, updated_at) \
                 VALUES ('j-legacy','t-1','p-1','idle','thread','Thread','thread',1,1)",
                (),
            )
            .await?;
            Ok::<_, DbError>(())
        })
    })
    .await
    .unwrap();
    let orch = orchestrator(&temp, db.clone());

    let created = common::change_resource(
        &orch,
        write_to(
            "create",
            json!({"current_intent": "Carrying an older thread forward."}),
        ),
    )
    .await;
    assert!(created.contains("version 1"), "got: {created}");
    assert!(
        !created.contains("this turn now ends"),
        "a migrated thread's arc is a living doc too, got: {created}"
    );
    assert_eq!(
        arc_contract(&db).await.as_deref(),
        Some(r#"{"schemaType":"arc"}"#),
        "resolving the session backfills the arc contract onto the existing job"
    );

    // Backfilled, the same contract is enforced: a stray key is refused rather
    // than shallow-merged into the thread's durable memory.
    let stray = common::change_resource(
        &orch,
        write_to("patch", json!({"census": "where every child stands"})),
    )
    .await;
    assert!(stray.contains("has no field"), "got: {stray}");
}

#[tokio::test]
async fn the_arc_schema_is_enforced_not_merely_resolvable() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    let orch = orchestrator(&temp, db.clone());

    // A create missing the one required field is refused.
    let intentless = common::change_resource(
        &orch,
        write_to("create", json!({"working": "no direction declared"})),
    )
    .await;
    assert!(
        intentless.contains("current_intent"),
        "an arc without current_intent must be refused by name, got: {intentless}"
    );

    common::change_resource(
        &orch,
        write_to(
            "create",
            json!({"current_intent": "Standing up the resource library."}),
        ),
    )
    .await;

    // A key the arc has no field for would otherwise be shallow-merged into the
    // stored document verbatim.
    let stray = common::change_resource(
        &orch,
        write_to(
            "patch",
            json!({"chapters": "a table of contents it does not author"}),
        ),
    )
    .await;
    assert!(
        stray.contains("has no field"),
        "an undeclared arc field must be refused, got: {stray}"
    );

    // A ruling outside the decided vocabulary is refused too.
    let unstated = common::change_resource(
        &orch,
        write_to(
            "patch",
            json!({"ruling": ruling("executor placement stays lease-based", "maybe")}),
        ),
    )
    .await;
    assert!(
        !unstated.contains("version"),
        "a ruling with an unknown status must not be stored, got: {unstated}"
    );
}
