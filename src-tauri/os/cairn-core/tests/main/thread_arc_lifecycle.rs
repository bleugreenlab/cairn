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
//! - the artifact is PROSE ONLY. Rulings and open questions are rows in the
//!   thread's own database, so every surface that touches them here goes
//!   through `_cairn_rulings` / `_cairn_open_questions` rather than through an
//!   artifact field, and a write that still carries the retired keys is refused
//!   with the SQL that replaces it;
//! - the Markdown read projects those rows back beside the prose, byte-exact,
//!   while `format=json` returns the stored artifact data and is therefore
//!   prose only — and says so, rather than reading as though the decisions were
//!   lost;
//! - the schema is enforced, not merely resolvable — including on the session
//!   jobs live threads actually run on, which carry an execution (and often a
//!   recipe node) the thread acquired by taking turns and delegating.

use crate::common;
use crate::common::orchestrator;

use cairn_core::internal::storage::{DbError, LocalDb, RowExt};
use serde_json::{json, Value};
use std::sync::Arc;

fn arc_uri(thread: &str) -> String {
    format!("cairn://p/thr/{thread}/arc")
}

/// A project and a thread with no session job: the state every thread is in
/// before its first turn, and the only state in which the arc's contract is
/// resolved from scratch.
///
/// Each test gets its own thread id because the thread-database redirect is a
/// process-wide map keyed by that id; sharing one would let parallel tests open
/// each other's collections.
async fn seed(db: &LocalDb, temp: &tempfile::TempDir, thread_id: &str, thread_name: &str) {
    let (owned_id, owned_name) = (thread_id.to_string(), thread_name.to_string());
    db.write(move |conn| {
        let (thread_id, thread_name) = (owned_id.clone(), owned_name.clone());
        Box::pin(async move {
            conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
            conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','thr','/tmp/p',1,1)", ()).await?;
            conn.execute(
                "INSERT INTO threads (id, project_id, name, status, created_at, updated_at) VALUES (?1,'p-1',?2,'active',1,1)",
                cairn_db::turso::params![thread_id, thread_name],
            ).await?;
            Ok::<_, DbError>(())
        })
    })
    .await
    .unwrap();
    cairn_core::threads::db_store::use_test_path(
        thread_id,
        temp.path().join(format!("{thread_id}-store.db")),
    );
}

fn write_to(thread: &str, mode: &str, payload: Value) -> Value {
    json!([{ "target": arc_uri(thread), "mode": mode, "payload": payload }])
}

/// The stored data of the thread's latest arc version.
async fn latest_arc(db: &LocalDb) -> Value {
    serde_json::from_str(&latest_arc_raw(db).await).expect("stored arc data is JSON")
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

/// The session job's persisted output contract.
async fn arc_contract(db: &LocalDb, thread_id: &str) -> Option<String> {
    let thread_id = thread_id.to_string();
    db.read(move |conn| {
        let thread_id = thread_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT output_contract FROM jobs WHERE thread_id = ?1 AND uri_segment = 'thread'",
                    cairn_db::turso::params![thread_id],
                )
                .await?;
            rows.next().await?.map(|row| row.opt_text(0)).transpose()
        })
    })
    .await
    .unwrap()
    .flatten()
}

/// Write a ruling the way a thread actually writes one: parameterized SQL into
/// the thread's own database, never an artifact field.
async fn insert_ruling(thread_id: &str, slug: &str, text: &str, rationale: &str, provenance: &str) {
    let store = cairn_core::threads::db_store::open(thread_id)
        .await
        .expect("the thread database opens");
    let (slug, text, rationale, provenance) = (
        slug.to_string(),
        text.to_string(),
        rationale.to_string(),
        provenance.to_string(),
    );
    store
        .write(move |conn| {
            let (slug, text, rationale, provenance) = (
                slug.clone(),
                text.clone(),
                rationale.clone(),
                provenance.clone(),
            );
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO _cairn_rulings (slug, text, status, rationale, provenance) \
                     VALUES (?1, ?2, 'accepted', ?3, ?4)",
                    cairn_db::turso::params![slug, text, rationale, provenance],
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .expect("the ruling is stored");
}

#[tokio::test]
async fn a_new_thread_creates_and_keeps_its_arc() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db, &temp, "t-keeps", "library").await;
    let orch = orchestrator(&temp, db.clone());

    // --- first-ever create: the schema resolves and v1 lands ---------------
    let created = common::change_resource(
        &orch,
        write_to(
            "library",
            "create",
            json!({
                "current_intent": "Standing up the resource library.",
                "working": "reading the pack format"
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
        arc_contract(&db, "t-keeps").await.as_deref(),
        Some(r#"{"schemaType":"arc"}"#),
        "the session job carries the arc contract by preset name"
    );

    // --- field merge: intent moves, the fields it did not name stay --------
    let moved = common::change_resource(
        &orch,
        write_to(
            "library",
            "patch",
            json!({"current_intent": "Packs are named, not hashed."}),
        ),
    )
    .await;
    assert!(moved.contains("version 2"), "got: {moved}");
    let arc = latest_arc(&db).await;
    assert_eq!(arc["current_intent"], "Packs are named, not hashed.");
    assert_eq!(
        arc["working"], "reading the pack format",
        "a field merge keeps the fields it did not name: {arc}"
    );

    // The read advertises the contract the write enforces. Without it the arc's
    // fields are guesswork from the role prompt alone, which is how a thread
    // ends up parking arc content somewhere that gets summarized away.
    let rendered = common::read_resource(&orch, arc_uri("library")).await;
    assert!(
        rendered.contains("Packs are named, not hashed."),
        "the arc's own data renders: {rendered}"
    );
    assert!(
        rendered.contains("`current_intent(str)`"),
        "the arc advertises its required field: {rendered}"
    );
    assert!(
        rendered.contains("append one ruling") && rendered.contains("_cairn_rulings"),
        "the arc advertises where its rulings live: {rendered}"
    );
}

/// Rulings left the artifact in #3747. The keys that used to write them are the
/// ones an agent reaches for when a single ruling needs amending, so they must
/// fail loudly and say where the collections went — in BOTH modes. A `create`
/// carrying them is the hand-retyped wholesale replace, the shape most likely to
/// drop an unrelated rejection on its way through.
#[tokio::test]
async fn retired_ruling_keys_are_refused_with_the_sql_that_replaced_them() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db, &temp, "t-retired", "library").await;
    let orch = orchestrator(&temp, db.clone());

    common::change_resource(
        &orch,
        write_to(
            "library",
            "create",
            json!({"current_intent": "Standing up the resource library."}),
        ),
    )
    .await;

    // Both collections, in both modes. A `create` is the hand-retyped wholesale
    // replace; a `patch` is the incremental shape. Either way the refusal has to
    // name the table, because "this artifact has no field" tells a caller only
    // that its write bounced.
    for (mode, payload, table) in [
        (
            "create",
            json!({"current_intent": "x", "rulings": []}),
            "_cairn_rulings",
        ),
        ("patch", json!({"rulings": []}), "_cairn_rulings"),
        (
            "patch",
            json!({"ruling": {"text": "a decision"}}),
            "_cairn_rulings",
        ),
        (
            "patch",
            json!({"ruling_slug": "a-decision"}),
            "_cairn_rulings",
        ),
        (
            "create",
            json!({"current_intent": "x", "open_questions": []}),
            "_cairn_open_questions",
        ),
        (
            "patch",
            json!({"open_questions": [{"question": "who owns aliases?"}]}),
            "_cairn_open_questions",
        ),
        (
            "patch",
            json!({"open_question": {"question": "who owns aliases?"}}),
            "_cairn_open_questions",
        ),
    ] {
        let refused =
            common::change_resource(&orch, write_to("library", mode, payload.clone())).await;
        assert!(
            refused.contains(table) && refused.contains("cairn:~/db"),
            "a {mode} carrying {payload} must name {table} and the SQL that \
             replaced it, got: {refused}"
        );
        assert!(
            !refused.contains("version 2"),
            "a refused arc write must not store a version, got: {refused}"
        );
    }

    let arc = latest_arc(&db).await;
    assert_eq!(arc["current_intent"], "Standing up the resource library.");
}

/// A raw `format=json` artifact read returns the stored JSON and nothing else,
/// so a caller can parse the read result directly. The arc is the case most
/// tempting to exempt — its projection is prose only, because the collections
/// are rows rather than fields — and it is exactly where the guarantee has to
/// hold: this parses the surface a caller actually receives rather than
/// substring-checking it, because a body can carry the right characters and
/// still have stopped being JSON. Where the collections live is said on the
/// Markdown read and on the refusal, neither of which is machine-parsed.
#[tokio::test]
async fn the_json_projection_is_prose_only_and_still_reparses() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db, &temp, "t-json", "library").await;
    let orch = orchestrator(&temp, db.clone());

    common::change_resource(
        &orch,
        write_to(
            "library",
            "create",
            json!({"current_intent": "Standing up the resource library."}),
        ),
    )
    .await;
    insert_ruling(
        "t-json",
        "packs-resolve-by-name",
        "packs resolve by name, not by content hash",
        "the catalog addresses them by name everywhere else",
        "[\"cairn://p/thr/1\"]",
    )
    .await;

    let json_read =
        common::read_resource(&orch, format!("{}?format=json", arc_uri("library"))).await;
    let parsed: Value = serde_json::from_str(&json_read).unwrap_or_else(|error| {
        panic!("the raw projection must reparse as JSON ({error}), got: {json_read}")
    });
    assert_eq!(
        parsed["current_intent"],
        "Standing up the resource library."
    );
    assert!(
        parsed.get("rulings").is_none() && parsed.get("open_questions").is_none(),
        "the collections are rows, not artifact fields, so the stored-data \
         projection must not carry them: {parsed}"
    );

    // The Markdown read is the projection that carries them, and the one that
    // says where they live.
    let rendered = common::read_resource(&orch, arc_uri("library")).await;
    assert!(
        rendered.contains("packs resolve by name, not by content hash"),
        "the Markdown read projects the collections beside the prose: {rendered}"
    );
    assert!(
        rendered.contains("_cairn_rulings") && rendered.contains("_cairn_open_questions"),
        "the Markdown read advertises where both collections live: {rendered}"
    );
}

/// A ruling's rationale is the expensive half — the argument in the terms it was
/// actually argued — and a real one runs to several paragraphs. Provenance is
/// the other half, and it is written canonically because the arc outlives the
/// session that wrote it. Neither may be reflowed or rewritten on the way out.
#[tokio::test]
async fn a_multi_paragraph_rationale_and_canonical_provenance_render_byte_exact() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db, &temp, "t-fidelity", "library").await;
    let orch = orchestrator(&temp, db.clone());

    common::change_resource(
        &orch,
        write_to(
            "library",
            "create",
            json!({"current_intent": "Preserve authored reasoning."}),
        ),
    )
    .await;

    let rationale = "The builder followed the prescription and could not make the test fail.\n\
                     \nCause: the command reports the refusal on stderr and still exits 0, so the \
                     failure reached the wrapper looking like success.\n\nCOROLLARY: every caller \
                     that already discards that verdict became a new defect site.";
    insert_ruling(
        "t-fidelity",
        "retry-needs-a-representable-failure",
        "A retry wrapper is a no-op unless the failure it retries is representable",
        rationale,
        "[\"cairn://p/thr/library\"]",
    )
    .await;

    let rendered = common::read_resource(&orch, arc_uri("library")).await;
    assert!(
        rendered.contains(rationale),
        "a multi-paragraph rationale must render byte-exact, blank lines and \
         all: {rendered}"
    );
    // Asserted on the provenance line itself rather than on the whole read: the
    // affordance block quotes `cairn:~/db` in its own examples, which is correct
    // there and says nothing about how a stored URI was written.
    assert!(
        rendered.contains("- provenance: cairn://p/thr/library"),
        "provenance must render in the canonical form it was stored in, never \
         rewritten to a session-relative one: {rendered}"
    );
}

#[tokio::test]
async fn a_thread_minted_before_the_arc_had_a_contract_gets_one() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db, &temp, "t-legacy", "library").await;
    // The pre-cutover shape: a live session job carrying no output contract, so
    // its arc writes were stored with nothing validating them and its arc read
    // had no fields to state.
    db.write(|conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs (id, thread_id, project_id, status, agent_config_id, node_name, uri_segment, created_at, updated_at) \
                 VALUES ('j-legacy','t-legacy','p-1','idle','thread','Thread','thread',1,1)",
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
            "library",
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
        arc_contract(&db, "t-legacy").await.as_deref(),
        Some(r#"{"schemaType":"arc"}"#),
        "resolving the session backfills the arc contract onto the existing job"
    );

    // Backfilled, the same contract is enforced: a stray key is refused rather
    // than shallow-merged into the thread's durable memory.
    let stray = common::change_resource(
        &orch,
        write_to(
            "library",
            "patch",
            json!({"census": "where every child stands"}),
        ),
    )
    .await;
    assert!(stray.contains("has no field"), "got: {stray}");
}

#[tokio::test]
async fn the_arc_schema_is_enforced_not_merely_resolvable() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db, &temp, "t-schema", "library").await;
    let orch = orchestrator(&temp, db.clone());

    // A create missing the one required field is refused.
    let intentless = common::change_resource(
        &orch,
        write_to(
            "library",
            "create",
            json!({"working": "no direction declared"}),
        ),
    )
    .await;
    assert!(
        intentless.contains("current_intent"),
        "an arc without current_intent must be refused by name, got: {intentless}"
    );

    common::change_resource(
        &orch,
        write_to(
            "library",
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
            "library",
            "patch",
            json!({"chapters": "a table of contents it does not author"}),
        ),
    )
    .await;
    assert!(
        stray.contains("has no field"),
        "an undeclared arc field must be refused, got: {stray}"
    );
}

/// The machinery a live thread's session job has acquired by the time it writes
/// its arc. A thread that has taken a turn or delegated hosts a synthetic
/// delegation execution, and an older session also carries a `recipe_node_id`
/// that names no node in that execution's snapshot. Neither declares an
/// artifact; the arc contract is still the one on the job row.
#[derive(Clone, Copy, Debug)]
enum LiveSession {
    /// `execution_id` set, `recipe_node_id` NULL.
    Execution,
    /// `execution_id` and a dangling `recipe_node_id` both set.
    ExecutionAndRecipeNode,
}

/// Seed a thread's session job in a live shape, carrying the arc contract the
/// way `ensure_thread_session_conn` mints and backfills it.
async fn seed_live_session(db: &LocalDb, thread_id: &str, shape: LiveSession) {
    let thread_id = thread_id.to_string();
    db.write(move |conn| {
        let thread_id = thread_id.clone();
        Box::pin(async move {
            let job_id = format!("j-{thread_id}");
            let execution_id = format!("x-{thread_id}");
            let recipe_node_id = match shape {
                LiveSession::Execution => None,
                LiveSession::ExecutionAndRecipeNode => Some(format!("n-{thread_id}")),
            };
            conn.execute(
                "INSERT INTO executions (id, recipe_id, project_id, status, started_at, snapshot) \
                 VALUES (?1, ?2, 'p-1', 'running', 1, ?3)",
                cairn_db::turso::params![
                    execution_id.as_str(),
                    format!("delegation-{job_id}"),
                    json!({"recipe": {
                        "id": format!("delegation-{job_id}"),
                        "name": "Delegated Work",
                        "trigger": "manual",
                        "nodes": [],
                        "edges": []
                    }})
                    .to_string()
                ],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs (id, thread_id, project_id, status, agent_config_id, node_name, \
                 uri_segment, output_contract, execution_id, recipe_node_id, created_at, updated_at) \
                 VALUES (?1, ?2, 'p-1', 'idle', 'thread', 'thread', 'thread', \
                 '{\"schemaType\":\"arc\"}', ?3, ?4, 1, 1)",
                cairn_db::turso::params![
                    job_id.as_str(),
                    thread_id.as_str(),
                    execution_id.as_str(),
                    recipe_node_id
                ],
            )
            .await?;
            Ok::<_, DbError>(())
        })
    })
    .await
    .unwrap();
}

/// A `write` submitted with `preview:true`, the way an agent checks a write
/// before landing it.
async fn preview_write(
    orch: &cairn_core::internal::orchestrator::Orchestrator,
    changes: Value,
) -> String {
    let request = cairn_core::internal::mcp::types::McpCallbackRequest {
        thread_id: None,
        cwd: String::new(),
        run_id: None,
        tool: "write".to_string(),
        payload: json!({ "preview": true, "changes": changes }),
        tool_use_id: None,
    };
    cairn_core::internal::mcp::handlers::write::handle_write(orch, &request).await
}

/// A live thread's session job carries an execution, and often a recipe node,
/// that declare no artifact of their own. The arc contract on the job row must
/// still be the one enforced and advertised: resolving the recipe node first
/// found nothing, so a guessed key like `rulings_append` was stored verbatim as
/// a top-level field while the thread believed it had recorded a ruling, and
/// the read showed the generic artifact affordance instead of the arc's.
#[tokio::test]
async fn a_live_session_job_enforces_and_advertises_the_arc_contract() {
    for (thread_id, shape) in [
        ("t-live-exec", LiveSession::Execution),
        ("t-live-node", LiveSession::ExecutionAndRecipeNode),
    ] {
        let (temp, db) = common::migrated_db().await;
        let db = Arc::new(db);
        seed(&db, &temp, thread_id, "general").await;
        seed_live_session(&db, thread_id, shape).await;
        let orch = orchestrator(&temp, db.clone());

        // An unknown key on a create is refused, naming the accepted fields.
        let stray_create = common::change_resource(
            &orch,
            write_to(
                "general",
                "create",
                json!({"current_intent": "x", "chapters": "a table of contents"}),
            ),
        )
        .await;
        assert!(
            stray_create.contains("has no field `chapters`")
                && stray_create.contains("current_intent, working"),
            "{shape:?}: an unknown key on an arc create must be refused with the \
             accepted set named, got: {stray_create}"
        );

        let created = common::change_resource(
            &orch,
            write_to(
                "general",
                "create",
                json!({"current_intent": "Hold the general arc."}),
            ),
        )
        .await;
        assert!(created.contains("version 1"), "{shape:?}: got: {created}");
        assert!(
            !created.contains("this turn now ends"),
            "{shape:?}: the arc stays a living document, got: {created}"
        );

        // `old_string` is an operation only on a patch. On a create it is one
        // more key the caller sent, and must not switch the vocabulary check
        // off for the unknown key riding beside it.
        let text_shaped_create = common::change_resource(
            &orch,
            write_to(
                "general",
                "create",
                json!({
                    "current_intent": "replacement",
                    "old_string": "x",
                    "rulings_append": [{"text": "a decision"}]
                }),
            ),
        )
        .await;
        assert!(
            text_shaped_create.contains("has no field")
                && text_shaped_create.contains("`rulings_append`")
                && !text_shaped_create.contains("version 2"),
            "{shape:?}: a create carrying `old_string` and an unknown key must be \
             refused, got: {text_shaped_create}"
        );

        // The exact keys a live thread guessed, on a field-merge patch.
        for key in ["rulings_append", "rulings_update", "add_ruling"] {
            let stray = common::change_resource(
                &orch,
                write_to("general", "patch", json!({ key: [{"text": "a decision"}] })),
            )
            .await;
            assert!(
                stray.contains(&format!("has no field `{key}`"))
                    && stray.contains("current_intent, working"),
                "{shape:?}: `{key}` must be refused with the accepted set named, got: {stray}"
            );
            assert!(
                !stray.contains("version 2"),
                "{shape:?}: a refused arc write must not store a version, got: {stray}"
            );
        }

        // A preview answers what the write would: refused, not "would patch".
        let previewed = preview_write(
            &orch,
            write_to("general", "patch", json!({"probe_unknown_key": "x"})),
        )
        .await;
        assert!(
            previewed.contains("has no field `probe_unknown_key`")
                && !previewed.contains("Would patch"),
            "{shape:?}: a previewed unknown key must be refused, got: {previewed}"
        );
        let previewed_ok = preview_write(
            &orch,
            write_to("general", "patch", json!({"working": "reading"})),
        )
        .await;
        assert!(
            previewed_ok.contains("Would patch"),
            "{shape:?}: a well-formed preview still previews, got: {previewed_ok}"
        );

        // The retired collection keys keep their SQL-naming refusal ahead of
        // the generic one.
        let retired =
            common::change_resource(&orch, write_to("general", "patch", json!({"rulings": []})))
                .await;
        assert!(
            retired.contains("_cairn_rulings") && retired.contains("cairn:~/db"),
            "{shape:?}: got: {retired}"
        );

        let arc = latest_arc(&db).await;
        assert_eq!(
            arc,
            json!({"current_intent": "Hold the general arc."}),
            "{shape:?}: no refused key may reach the stored arc"
        );

        let rendered = common::read_resource(&orch, arc_uri("general")).await;
        assert!(
            rendered.contains("`current_intent(str)`")
                && rendered.contains("append one ruling")
                && rendered.contains("_cairn_rulings"),
            "{shape:?}: the read must render the arc-specific affordance, got: {rendered}"
        );
    }
}

/// Arcs written while enforcement was bypassed still carry the keys it would
/// have refused. The arc schema is closed, so a merge that inherited one could
/// never validate: turning enforcement on must not leave those arcs uneditable.
/// The next write sheds the fossil; the prior version keeps it.
#[tokio::test]
async fn an_arc_stored_with_a_fossil_key_stays_editable() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db, &temp, "t-fossil", "general").await;
    seed_live_session(&db, "t-fossil", LiveSession::ExecutionAndRecipeNode).await;
    db.write(|conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO artifacts (id, job_id, artifact_type, data, version, output_name, \
                 created_at, updated_at, confirmed) \
                 VALUES ('a-fossil', 'j-t-fossil', 'arc', \
                 '{\"current_intent\":\"Hold the arc.\",\"rulings_append\":null}', \
                 1, 'arc', 1, 1, 1)",
                (),
            )
            .await?;
            Ok::<_, DbError>(())
        })
    })
    .await
    .unwrap();
    let orch = orchestrator(&temp, db.clone());

    let patched = common::change_resource(
        &orch,
        write_to("general", "patch", json!({"working": "reading"})),
    )
    .await;
    assert!(
        patched.contains("version 2"),
        "a patch that sends only declared fields must land over a fossil, got: {patched}"
    );
    assert_eq!(
        latest_arc(&db).await,
        json!({"current_intent": "Hold the arc.", "working": "reading"}),
        "the inherited fossil is shed on the next write"
    );
}

/// How many jobs a thread owns, so a test can tell whether a preview started it.
async fn thread_job_count(db: &LocalDb, thread_id: &str) -> i64 {
    let thread_id = thread_id.to_string();
    db.read(move |conn| {
        let thread_id = thread_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM jobs WHERE thread_id = ?1",
                    cairn_db::turso::params![thread_id],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| row.get::<i64>(0))
                .transpose()
                .map(Option::unwrap_or_default)
                .map_err(Into::into)
        })
    })
    .await
    .unwrap()
}

/// A thread that has never run has no session job, and its first real write
/// mints one carrying the arc contract. A preview must neither start the thread
/// nor wave the write through unvalidated: it answers to the contract the write
/// would mint.
#[tokio::test]
async fn a_preview_on_a_never_run_thread_validates_without_starting_it() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db, &temp, "t-unminted", "library").await;
    let orch = orchestrator(&temp, db.clone());

    for (mode, payload, expected) in [
        (
            "patch",
            json!({"rulings_append": []}),
            "has no field `rulings_append`",
        ),
        (
            "create",
            json!({"current_intent": "x", "rulings": []}),
            "_cairn_rulings",
        ),
        (
            "create",
            json!({"working": "no direction"}),
            "current_intent",
        ),
        (
            "patch",
            json!({"old_string": "a", "new_string": "b"}),
            "requires an existing artifact",
        ),
    ] {
        let previewed = preview_write(&orch, write_to("library", mode, payload.clone())).await;
        assert!(
            previewed.contains(expected) && !previewed.contains("Would "),
            "a {mode} preview of {payload} on a never-run thread must be refused \
             with `{expected}`, got: {previewed}"
        );
    }

    let accepted = preview_write(
        &orch,
        write_to("library", "create", json!({"current_intent": "Begin."})),
    )
    .await;
    assert!(
        accepted.contains("Would create"),
        "a well-formed first arc still previews, got: {accepted}"
    );
    assert_eq!(
        thread_job_count(&db, "t-unminted").await,
        0,
        "a preview must not mint the thread's session job"
    );

    let missing = preview_write(
        &orch,
        write_to("absent", "create", json!({"current_intent": "x"})),
    )
    .await;
    assert!(
        !missing.contains("Would "),
        "a preview addressed to a thread that does not exist cannot succeed, got: {missing}"
    );
}

/// A session job minted before the arc had a contract carries none until its
/// next real write backfills it. The preview answers to the backfilled contract
/// without performing the backfill.
#[tokio::test]
async fn a_preview_on_a_pre_contract_session_answers_to_the_backfilled_contract() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db, &temp, "t-precontract", "library").await;
    db.write(|conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs (id, thread_id, project_id, status, agent_config_id, node_name, uri_segment, created_at, updated_at) \
                 VALUES ('j-precontract','t-precontract','p-1','idle','thread','Thread','thread',1,1)",
                (),
            )
            .await?;
            Ok::<_, DbError>(())
        })
    })
    .await
    .unwrap();
    let orch = orchestrator(&temp, db.clone());

    let previewed = preview_write(
        &orch,
        write_to("library", "patch", json!({"census": "every child"})),
    )
    .await;
    assert!(
        previewed.contains("has no field `census`"),
        "got: {previewed}"
    );
    assert_eq!(
        arc_contract(&db, "t-precontract").await,
        None,
        "a preview must not backfill the contract it previews against"
    );
}
