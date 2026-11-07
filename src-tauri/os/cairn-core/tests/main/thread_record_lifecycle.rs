//! End-to-end proof that a thread keeps its record in its own database, driven
//! through the real `write` and read dispatchers.
//!
//! A thread's authored record is the `intent` row and the `journal` of its
//! store (thread migration 0005), written through the row grammar a thread
//! already uses for its own tables. This exercises that end to end:
//!
//! - the starter tables are listed first and labelled cairn-owned, the intent
//!   row patches and stamps itself, it cannot be deleted, and DDL against
//!   either table is refused;
//! - a journal append takes `refs` as a plain array, a kind the CHECK does not
//!   know is refused, and patching a `resolution` retires an open row;
//! - a thread session keeps no artifacts: a write to `cairn:~/arc` or any other
//!   artifact name is refused with where the record lives, and the arc's old
//!   address reads as that refusal followed by its last version as history;
//! - the boot import moves an arc's intent into the intent row;
//! - SQL naming the retired `_cairn_rulings` table is refused with the tenets
//!   message on both surfaces, and a migrated ruling's rationale renders
//!   byte-exact on its tenet.

use crate::common;

use cairn_core::internal::storage::{DbError, LocalDb, RowExt};
use cairn_core::tenets::{AnswerSource, NewTenet, TenetOrigin, TenetStatus};
use serde_json::json;

const PROJECT: &str = "rec";
const THREAD: &str = "library";
const RULINGS_ARE_TENETS: &str = "rulings are project tenets now";

/// A project and a thread whose session job has a live run, so writes act in
/// the thread. Each test names its own run, and with it its own thread id and
/// store: the store redirect is a process-wide map keyed by thread id.
async fn fixture(
    run: &str,
) -> (
    tempfile::TempDir,
    std::sync::Arc<LocalDb>,
    cairn_core::internal::orchestrator::Orchestrator,
) {
    let (temp, db, orch, _repo) = common::project_resource_fixture(PROJECT).await;
    common::seed_authenticated_thread_run(&db, &format!("project-{PROJECT}"), THREAD, run).await;
    cairn_core::threads::db_store::use_test_path(
        &thread_id(run),
        temp.path().join(format!("{run}-store.db")),
    );
    (temp, db, orch)
}

fn thread_id(run: &str) -> String {
    format!("thread-{run}")
}

fn db_uri(path: &str) -> String {
    format!("cairn://p/{PROJECT}/{THREAD}/db{path}")
}

async fn store(run: &str) -> std::sync::Arc<LocalDb> {
    cairn_core::threads::db_store::open(&thread_id(run))
        .await
        .unwrap()
}

#[tokio::test]
async fn a_thread_keeps_its_intent_and_journal_through_the_row_grammar() {
    let run = "r-record";
    let (_temp, _db, orch) = fixture(run).await;
    store(run).await;

    let overview = common::read_resource(&orch, db_uri("")).await;
    let intent_at = overview.find("intent(").expect("intent is listed");
    let journal_at = overview.find("journal(").expect("journal is listed");
    assert!(intent_at < journal_at, "{overview}");
    assert!(
        overview.contains("cairn-owned shape, rows are yours"),
        "{overview}"
    );

    let patched = common::change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn:~/db/intent/1",
            "mode": "patch",
            "payload": {"current_intent": "Standing up the library.", "working": "reading packs"}
        }]),
        Some(run),
    )
    .await;
    assert!(
        patched.contains(&format!("Updated {}", db_uri("/intent/1"))),
        "{patched}"
    );
    let stamped = store(run)
        .await
        .query_one(
            "SELECT current_intent, updated_at FROM intent WHERE id = 1",
            (),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(stamped.0, "Standing up the library.");
    assert!(
        stamped.1.is_some_and(|at| at.ends_with('Z')),
        "a patch stamps updated_at"
    );

    let deleted = common::change_resource_as_run(
        &orch,
        json!([{"target": "cairn:~/db/intent/1", "mode": "delete"}]),
        Some(run),
    )
    .await;
    assert!(deleted.contains("intent is one row"), "{deleted}");

    let appended = common::change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn:~/db/journal",
            "mode": "append",
            "payload": {"kind": "watch", "note": "Lease renewal", "refs": ["cairn://p/rec/4"]}
        }]),
        Some(run),
    )
    .await;
    assert!(appended.contains(&db_uri("/journal/1")), "{appended}");

    let unknown = common::change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn:~/db/journal",
            "mode": "append",
            "payload": {"kind": "ruling", "note": "Not a kind"}
        }]),
        Some(run),
    )
    .await;
    assert!(!unknown.contains(&db_uri("/journal/2")), "{unknown}");

    let resolved = common::change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn:~/db/journal/1",
            "mode": "patch",
            "payload": {"resolution": "Renewed through 2027"}
        }]),
        Some(run),
    )
    .await;
    assert!(
        resolved.contains(&format!("Updated {}", db_uri("/journal/1"))),
        "{resolved}"
    );
    let row = store(run)
        .await
        .query_one(
            "SELECT refs, resolved_at FROM journal WHERE id = 1",
            (),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(row.0, r#"["cairn://p/rec/4"]"#);
    assert!(row.1.is_some(), "patching a resolution stamps resolved_at");

    let ddl = common::change_resource_as_run(
        &orch,
        json!([{"target": "cairn:~/db", "mode": "patch", "payload": {"sql": "DROP TABLE journal"}}]),
        Some(run),
    )
    .await;
    assert!(ddl.contains("app-contract schema"), "{ddl}");
}

#[tokio::test]
async fn a_thread_session_keeps_no_artifacts_and_its_arc_reads_as_history() {
    let run = "r-artifacts";
    let (_temp, db, orch) = fixture(run).await;
    db.execute(
        "INSERT INTO artifacts (id, job_id, artifact_type, output_name, data, version, created_at, updated_at, confirmed)
         VALUES ('a-old', ?1, 'arc', 'arc', '{\"current_intent\":\"The old direction\"}', 1, 3, 3, 1)",
        (format!("job-{run}"),),
    )
    .await
    .unwrap();

    for (target, lead) in [
        ("cairn:~/arc", "`cairn:~/arc` is retired."),
        ("cairn:~/plan", "A thread keeps no artifacts"),
    ] {
        let refused = common::change_resource_as_run(
            &orch,
            json!([{"target": target, "mode": "create", "payload": {"current_intent": "x"}}]),
            Some(run),
        )
        .await;
        assert!(refused.contains(lead), "{target}: {refused}");
        assert!(refused.contains("cairn:~/db/intent/1"), "{refused}");
    }
    assert_eq!(
        db.query_opt_i64("SELECT COUNT(*) FROM artifacts", ())
            .await
            .unwrap(),
        Some(1),
        "a refused write stores nothing"
    );

    let read = common::read_resource(&orch, format!("cairn://p/{PROJECT}/{THREAD}/arc")).await;
    assert!(read.starts_with("`cairn:~/arc` is retired."), "{read}");
    assert!(read.contains("The old direction"), "{read}");

    // The boot import moves that arc into the intent row nobody has written.
    let report = cairn_core::threads::intent_import::import_arc_intents(&orch.db).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.intents, 1);
    let intent = common::read_resource(&orch, db_uri("/intent/1")).await;
    assert!(intent.contains("The old direction"), "{intent}");
}

/// The retired table is refused by name, with where rulings went, on both
/// surfaces of the thread database — including the read, because an older
/// prompt still teaches `SELECT * FROM _cairn_rulings`.
#[tokio::test]
async fn sql_naming_the_retired_rulings_table_is_refused_on_write_and_read() {
    let run = "r-sql";
    let (_temp, _db, orch) = fixture(run).await;
    store(run).await;

    let written = common::change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn:~/db",
            "mode": "patch",
            "payload": {
                "sql": "INSERT INTO _cairn_rulings(slug,text,status,rationale,provenance) VALUES(?,?,?,?,?)",
                "params": ["a", "b", "accepted", "c", "[\"cairn://p/rec/1\"]"]
            }
        }]),
        Some(run),
    )
    .await;
    assert!(written.contains(RULINGS_ARE_TENETS), "got: {written}");

    let read = common::read_resource(
        &orch,
        db_uri("?sql=SELECT * FROM _cairn_rulings ORDER BY id"),
    )
    .await;
    assert!(read.contains(RULINGS_ARE_TENETS), "got: {read}");

    let questions = common::read_resource(
        &orch,
        db_uri("?sql=SELECT COUNT(*) AS n FROM journal WHERE kind = 'question'"),
    )
    .await;
    assert!(!questions.contains(RULINGS_ARE_TENETS), "got: {questions}");
}

/// A migrated ruling's rationale is the expensive half — the argument in the
/// terms it was actually argued — and a real one runs to several paragraphs. It
/// must survive the move into a tenet byte-exact, and the ruling's old slug must
/// still address it.
#[tokio::test]
async fn a_migrated_rulings_multi_paragraph_rationale_renders_byte_exact() {
    let run = "r-fidelity";
    let (_temp, db, orch) = fixture(run).await;

    let rationale = "The builder followed the prescription and could not make the test fail.\n\
                     \nCause: the command reports the refusal on stderr and still exits 0, so the \
                     failure reached the wrapper looking like success.\n\nCOROLLARY: every caller \
                     that already discards that verdict became a new defect site.";
    let tenet = NewTenet {
        project_id: format!("project-{PROJECT}"),
        proposing_thread_id: thread_id(run),
        slug: Some("retry-needs-a-representable-failure".into()),
        statement: "A retry wrapper is a no-op unless the failure it retries is representable"
            .into(),
        // The shape migration 0363 leaves: the rationale in its column, with the
        // copy an earlier import left in `legacy` still there.
        rationale: Some(rationale.to_string()),
        status: TenetStatus::Approved,
        verdict: Some(cairn_core::tenets::Verdict::Yes),
        comment: None,
        answered_by: None,
        answer_source: Some(AnswerSource::Migrated),
        answered_at: Some(1_700_000_000),
        supersedes: None,
        origin: TenetOrigin::MigratedRuling,
        legacy: Some(json!({
            "rationale": rationale,
            "provenance": [format!("cairn://p/{PROJECT}/{THREAD}")],
            "legacy_key": format!("{}:retry-needs-a-representable-failure", thread_id(run)),
            "original_slug": "retry-needs-a-representable-failure",
        })),
        proposer: cairn_common::identity::PrincipalRef::Agent {
            node: format!("cairn://p/{PROJECT}/{THREAD}"),
            run_id: None,
        },
        now: 100,
    };
    db.write(move |conn| {
        let tenet = tenet.clone();
        Box::pin(async move {
            cairn_core::tenets::insert_conn(conn, &tenet).await?;
            Ok::<_, DbError>(())
        })
    })
    .await
    .expect("the tenet is stored");

    let rendered = common::read_resource(
        &orch,
        format!("cairn://p/{PROJECT}/tenets/retry-needs-a-representable-failure"),
    )
    .await;
    assert!(
        rendered.contains("# Tenet #1"),
        "the old slug addresses the migrated tenet: {rendered}"
    );
    assert!(
        rendered.contains(&format!("## Rationale\n\n{rationale}\n\n")),
        "a migrated ruling's multi-paragraph rationale renders under Rationale \
         byte-exact, blank lines and all: {rendered}"
    );
    assert_eq!(
        rendered.matches("COROLLARY").count(),
        1,
        "the rationale renders once, not again under Migrated ruling: {rendered}"
    );
    assert!(!rendered.contains("- rationale:"), "{rendered}");
    assert!(rendered.contains("## Migrated ruling"), "{rendered}");
    assert!(!rendered.contains("legacy_key"), "{rendered}");
}
