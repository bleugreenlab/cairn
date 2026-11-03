use std::path::Path;
use std::sync::Arc;

use serde_json::{json, Value};

use super::data::{compose, validate, MAX_PAGE_BYTES};
use super::log::{list, record_retaining, truncate_detail, PageCall, DETAIL_MAX_BYTES};
use super::read::{envelope_or_error, gate_targets, PAGE_ROW_CAP};
use super::*;
use crate::db::DbState;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::models::CreateThread;
use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
use crate::services::testing::{CapturingEmitter, TestServicesBuilder};
use crate::storage::{LocalDb, RowExt, SearchIndex};

async fn seeded() -> (Orchestrator, Arc<CapturingEmitter>) {
    let local = LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("p.db"))
        .await
        .unwrap();
    local
        .execute_script(
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES ('p','w','P','prj','/tmp/p',1,1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('i','p',7,'Draft work','active',1,1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e','build','i','p','running',1,1);
             INSERT INTO jobs (id, project_id, issue_id, execution_id, node_name, uri_segment, agent_config_id, status, created_at, updated_at)
               VALUES ('node-job','p','i','e','Builder','builder','build','running',1,1);",
        )
        .await
        .unwrap();
    let search =
        Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
    let db = Arc::new(DbState::new(Arc::new(local), search));
    let emitter = Arc::new(CapturingEmitter::new());
    let orch = OrchestratorBuilder::new(
        db,
        Arc::new(
            TestServicesBuilder::new()
                .with_emitter(emitter.clone())
                .build(),
        ),
        tempfile::tempdir().unwrap().keep(),
    )
    .build();
    for name in ["roadmap", "devops"] {
        crate::threads::crud::create(
            &orch.db.local,
            CreateThread {
                project_id: "p".into(),
                name: Some(name.into()),
                jurisdiction: None,
                definition: None,
                migrated_from_number: None,
                model: None,
            },
        )
        .await
        .unwrap();
        // Each thread's own run, on its session: pages are written only by
        // their owner, and only through an authenticated run.
        let session = match session_jobs(&orch, name).await.first() {
            Some(session) => session.clone(),
            None => {
                orch.db
                    .local
                    .execute(
                        "INSERT INTO jobs (id, thread_id, project_id, node_name, agent_config_id,
                                           status, created_at, updated_at, uri_segment)
                         SELECT ?1, id, 'p', 'thread', 'thread', 'running', 1, 1, 'thread'
                         FROM threads WHERE name = ?2",
                        cairn_db::turso::params![format!("{name}-session"), name],
                    )
                    .await
                    .unwrap();
                format!("{name}-session")
            }
        };
        seed_run(&orch, &format!("{name}-run"), &session).await;
    }
    seed_run(&orch, "node-run", "node-job").await;
    (orch, emitter)
}

/// The run that owns `target`'s page: the thread's own, or the issue node's.
fn owner_run(target: &str) -> &'static str {
    if target.starts_with("cairn://p/prj/roadmap/") {
        "roadmap-run"
    } else if target.starts_with("cairn://p/prj/devops/") {
        "devops-run"
    } else {
        "node-run"
    }
}

fn request() -> McpCallbackRequest {
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: None,
        tool: "change".to_string(),
        payload: json!({}),
        tool_use_id: None,
        origin: None,
    }
}

/// Write `target` as its owner would.
async fn write(
    orch: &Orchestrator,
    target: &str,
    mode: ChangeMode,
    payload: Value,
) -> Result<String, String> {
    let item = ChangeItem {
        target: target.to_string(),
        mode,
        payload: Some(payload),
    };
    let request = McpCallbackRequest {
        run_id: Some(owner_run(target).to_string()),
        ..request()
    };
    crate::resources::mutations::dispatch_resource_change(orch, &request, 0, &item, false)
        .await
        .map(|change| change.summary)
        .map_err(|failure| failure.error)
}

async fn write_as(
    orch: &Orchestrator,
    run_id: &str,
    target: &str,
    payload: Value,
) -> Result<String, String> {
    let item = ChangeItem {
        target: target.to_string(),
        mode: ChangeMode::Create,
        payload: Some(payload),
    };
    let request = McpCallbackRequest {
        run_id: Some(run_id.to_string()),
        ..request()
    };
    crate::resources::mutations::dispatch_resource_change(orch, &request, 0, &item, false)
        .await
        .map(|change| change.summary)
        .map_err(|failure| failure.error)
}

async fn seed_run(orch: &Orchestrator, run_id: &str, job_id: &str) {
    orch.db
        .local
        .execute(
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
             VALUES (?1, 'p', ?2, 'live', 1, 1)",
            cairn_db::turso::params![run_id, job_id],
        )
        .await
        .unwrap();
}

pub(crate) async fn read(orch: &Orchestrator, uri: &str) -> String {
    crate::resources::read_cairn_resource(orch, &request(), uri).await
}

fn page(files: Value) -> Value {
    json!({ "title": "Board", "files": files })
}

fn index_js() -> Value {
    json!({ "index.js": "export default () => null;" })
}

async fn session_jobs(orch: &Orchestrator, thread: &str) -> Vec<String> {
    orch.db
        .local
        .query_all(
            format!(
                "SELECT j.id FROM jobs j JOIN threads t ON t.id = j.thread_id
                 WHERE t.name = ?1 AND {} ORDER BY j.created_at, j.rowid",
                crate::threads::SESSION_JOB_SHAPE
            ),
            cairn_db::turso::params![thread],
            |row| row.text(0),
        )
        .await
        .unwrap()
}

async fn actor_rows(orch: &Orchestrator, uri: &str) -> Vec<(String, String, String)> {
    orch.db
        .local
        .query_all(
            "SELECT verb, outcome, actor_json FROM page_calls WHERE page_uri = ?1
             ORDER BY created_at, rowid",
            cairn_db::turso::params![uri],
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
        )
        .await
        .unwrap()
}

// ---- Data rules ----

#[test]
fn a_valid_page_defaults_its_entry_and_passes() {
    let composed = compose(None, &page(index_js()), false, "index.js").unwrap();
    assert_eq!(composed["entry"], "index.js");
    validate(&composed).unwrap();
}

#[test]
fn the_entry_must_be_a_js_module_among_the_files() {
    let missing = compose(
        None,
        &json!({"title": "t", "files": {"app.js": ""}}),
        false,
        "index.js",
    )
    .unwrap();
    assert!(validate(&missing)
        .unwrap_err()
        .contains("not among the page's files"));

    let html = json!({"title": "t", "entry": "index.html", "files": {"index.html": ""}});
    assert!(validate(&html).unwrap_err().contains(".js or .mjs"));

    let mjs = json!({"title": "t", "entry": "main.mjs", "files": {"main.mjs": ""}});
    validate(&mjs).unwrap();
}

#[test]
fn file_paths_follow_the_segment_grammar_and_never_climb() {
    for bad in [
        "../x.js",
        "a/../b.js",
        "./a.js",
        "a//b.js",
        "/a.js",
        "a b.js",
        "a/b?.js",
    ] {
        let data = json!({"title": "t", "entry": "index.js", "files": {"index.js": "", bad: ""}});
        assert!(
            validate(&data).unwrap_err().contains("file path"),
            "{bad} should be refused"
        );
    }
    let nested = json!({"title": "t", "entry": "index.js", "files": {"index.js": "", "lib/chart.v2_x-y.js": ""}});
    validate(&nested).unwrap();
}

#[test]
fn files_are_capped_at_one_mebibyte_in_total() {
    let half = "x".repeat(MAX_PAGE_BYTES / 2);
    let at_cap =
        json!({"title": "t", "entry": "index.js", "files": {"index.js": half, "b.js": half}});
    validate(&at_cap).unwrap();
    let over = json!({"title": "t", "entry": "index.js", "files": {"index.js": half, "b.js": format!("{half}x")}});
    assert!(validate(&over).unwrap_err().contains("1 MiB"));
}

#[test]
fn the_schema_shape_is_enforced() {
    let no_title = json!({"entry": "index.js", "files": {"index.js": ""}});
    assert!(validate(&no_title)
        .unwrap_err()
        .contains("Page does not match"));
    let stray = json!({"title": "t", "entry": "index.js", "files": {"index.js": ""}, "html": "x"});
    assert!(validate(&stray).is_err());
    let non_text = json!({"title": "t", "entry": "index.js", "files": {"index.js": 3}});
    assert!(validate(&non_text).is_err());
}

#[test]
fn manifest_writes_name_only_the_owning_threads_tables() {
    let page = |writes: Value| {
        json!({"title": "t", "entry": "index.js", "files": {"index.js": ""},
               "manifest": {"writes": writes}})
    };
    validate(&page(json!(["cairn:~/db/attempts", "cairn:~/db/notes"]))).unwrap();
    validate(&page(json!([]))).unwrap();
    for (bad, why) in [
        ("cairn:~/db", "own thread's tables"),
        ("cairn:@/issues", "own thread's tables"),
        ("cairn://p/cairn/t/db/attempts", "own thread's tables"),
        ("file:x", "own thread's tables"),
        ("cairn:~/db/attempts/1", "not one row"),
        ("cairn:~/db/attempts?where=a=1", "no query"),
        ("cairn:~/db/*", "wildcard"),
        ("cairn:~/db/schema", "schema"),
        ("cairn:~/db/", "not a valid"),
    ] {
        let error = validate(&page(json!([bad]))).unwrap_err();
        assert!(error.contains(why), "{bad}: {error}");
        assert!(error.contains("cairn:~/db/<table>"), "{bad}: {error}");
    }
    let twice = validate(&page(json!(["cairn:~/db/a", "cairn:~/db/a"]))).unwrap_err();
    assert!(twice.contains("twice"), "{twice}");
    let many: Vec<String> = (0..33).map(|i| format!("cairn:~/db/t{i}")).collect();
    assert!(validate(&page(json!(many))).unwrap_err().contains("cap"));
    assert!(validate(&page(json!("cairn:~/db/a"))).is_err());

    let data = page(json!(["cairn:~/db/attempts"]));
    assert_eq!(
        super::data::write_scopes(&data),
        vec!["cairn:~/db/attempts".to_string()]
    );
}

/// A page reads only rows (tenet #1312): declaring network egress is refused
/// at write, and the refusal names the service pattern that replaces it.
#[tokio::test]
async fn a_page_declaring_manifest_network_is_refused_at_write_naming_services() {
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    let refused = write(
        &orch,
        uri,
        ChangeMode::Create,
        json!({"title": "Board", "files": index_js(),
               "manifest": {"network": {"hosts": ["api.github.com"]}}}),
    )
    .await
    .unwrap_err();
    assert!(
        refused.contains("manifest.network is retired")
            && refused.contains("manifest.services")
            && refused.contains("useRows"),
        "{refused}"
    );
    // Nothing was stored, so a later patch has nothing to compose onto.
    assert!(write(&orch, uri, ChangeMode::Patch, json!({"title": "x"}))
        .await
        .is_err());
}

#[test]
fn page_names_are_short_lowercase_segments() {
    for good in ["smoke", "release-board", "a1", "9"] {
        validate_page_name(good).unwrap();
    }
    for bad in ["", "-a", "Board", "a_b", "a.b", &"a".repeat(64)] {
        assert!(validate_page_name(bad).is_err(), "{bad}");
    }
}

#[test]
fn patch_merges_files_per_path_null_deletes_and_other_keys_replace() {
    let base = json!({"title": "Old", "description": "d", "entry": "index.js",
                      "files": {"index.js": "a", "chart.js": "b", "old.js": "c"}});
    let patched = compose(
        Some(&base),
        &json!({"title": "New", "description": null,
                "files": {"chart.js": "b2", "old.js": null, "new.js": "d"}}),
        true,
        "index.js",
    )
    .unwrap();
    assert_eq!(patched["title"], "New");
    assert!(patched.get("description").is_none());
    assert_eq!(
        patched["files"],
        json!({"index.js": "a", "chart.js": "b2", "new.js": "d"})
    );
    assert_eq!(patched["entry"], "index.js");

    assert!(compose(None, &json!({"files": {}}), true, "index.js")
        .unwrap_err()
        .contains("create"));
}

// ---- Store ----

/// A page reads with its owner's authority, so only the owner writes it.
#[tokio::test]
async fn only_the_owning_thread_or_the_drafts_own_node_writes_a_page() {
    let (orch, _) = seeded().await;
    let thread_page = "cairn://p/prj/roadmap/pages/board";

    let runless = crate::resources::mutations::dispatch_resource_change(
        &orch,
        &request(),
        0,
        &ChangeItem {
            target: thread_page.to_string(),
            mode: ChangeMode::Create,
            payload: Some(page(index_js())),
        },
        false,
    )
    .await
    .map(|change| change.summary)
    .expect_err("a write with no run identity is not taken for the operator");
    assert!(runless.error.contains("owning thread"), "{}", runless.error);

    let refused = write_as(&orch, "node-run", thread_page, page(index_js()))
        .await
        .expect_err("an issue node cannot write a thread's page");
    assert!(refused.contains("not this run's to write"), "{refused}");
    let other_thread = write_as(
        &orch,
        "roadmap-run",
        "cairn://p/prj/devops/pages/board",
        page(index_js()),
    )
    .await
    .expect_err("a thread cannot write another thread's page");
    assert!(
        other_thread.contains("not this run's to write"),
        "{other_thread}"
    );

    write_as(&orch, "roadmap-run", thread_page, page(index_js()))
        .await
        .expect("the owning thread writes its own page");

    let draft = "cairn://p/prj/7/1/builder/pages/sketch";
    write_as(&orch, "node-run", draft, page(index_js()))
        .await
        .expect("a node writes its own draft");
    write_as(&orch, "roadmap-run", draft, page(index_js()))
        .await
        .expect_err("a thread cannot write an issue node's draft");

    let unresolvable = write_as(&orch, "no-such-run", thread_page, page(index_js()))
        .await
        .expect_err("a run that does not resolve is refused, not treated as the operator");
    assert!(unresolvable.contains("resolve"), "{unresolvable}");
}

/// The thread pane asks its CURRENT session for a page by name; a page last
/// written under an earlier session must still resolve to its newest version.
#[tokio::test]
async fn a_named_lookup_on_the_current_session_finds_a_page_an_earlier_session_wrote() {
    let (orch, _) = seeded().await;
    write(
        &orch,
        "cairn://p/prj/roadmap/pages/board",
        ChangeMode::Create,
        page(json!({"index.js": "v1"})),
    )
    .await
    .unwrap();
    let first = session_jobs(&orch, "roadmap").await;
    orch.db
        .local
        .execute(
            "UPDATE jobs SET status = 'complete' WHERE id = ?1",
            cairn_db::turso::params![first[0].as_str()],
        )
        .await
        .unwrap();
    // Another page mints the next session without touching `board`.
    write(
        &orch,
        "cairn://p/prj/roadmap/pages/other",
        ChangeMode::Create,
        page(json!({"index.js": "x"})),
    )
    .await
    .unwrap();
    let sessions = session_jobs(&orch, "roadmap").await;
    assert_eq!(sessions.len(), 2);

    let found = named_artifact_for_job(&orch.db.local, &sessions[1], "pages/board")
        .await
        .unwrap()
        .expect("the current session resolves the thread's page");
    assert_eq!(found.job_id.as_deref(), Some(sessions[0].as_str()));
    assert_eq!(found.data["files"]["index.js"], "v1");

    assert!(
        named_artifact_for_job(&orch.db.local, "node-job", "pages/board")
            .await
            .unwrap()
            .is_none(),
        "a job that is not a thread session reads only its own chain"
    );
}

#[tokio::test]
async fn a_thread_page_continues_its_chain_across_a_new_session_job() {
    let (orch, emitter) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    write(
        &orch,
        uri,
        ChangeMode::Create,
        page(json!({"index.js": "v1", "util.js": "u"})),
    )
    .await
    .unwrap();
    let first_sessions = session_jobs(&orch, "roadmap").await;
    assert_eq!(first_sessions.len(), 1);
    assert!(
        emitter
            .events_named("db-change")
            .iter()
            .any(|event| event["table"] == "artifacts"),
        "a page write announces itself the way an artifact write does"
    );

    // The session ends; the next write mints a fresh one.
    orch.db
        .local
        .execute(
            "UPDATE jobs SET status = 'complete' WHERE id = ?1",
            cairn_db::turso::params![first_sessions[0].as_str()],
        )
        .await
        .unwrap();
    write(
        &orch,
        uri,
        ChangeMode::Patch,
        json!({"files": {"index.js": "v2", "util.js": null}}),
    )
    .await
    .unwrap();
    let sessions = session_jobs(&orch, "roadmap").await;
    assert_eq!(sessions.len(), 2, "the patch ran in a new session job");

    let rows: Vec<(String, i64, Option<String>, String)> = orch
        .db
        .local
        .query_all(
            "SELECT job_id, version, parent_version_id, id FROM artifacts
             WHERE output_name = 'pages/board' AND artifact_type = 'page' ORDER BY version",
            (),
            |row| Ok((row.text(0)?, row.i64(1)?, row.opt_text(2)?, row.text(3)?)),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, sessions[0]);
    assert_eq!(rows[1].0, sessions[1]);
    assert_eq!(rows[1].1, 2, "version 2, not a restart at 1");
    assert_eq!(rows[1].2.as_deref(), Some(rows[0].3.as_str()));

    let json: Value =
        serde_json::from_str(&read(&orch, &format!("{uri}?format=json")).await).unwrap();
    assert_eq!(json["files"], json!({"index.js": "v2"}));
    let first: Value =
        serde_json::from_str(&read(&orch, &format!("{uri}?format=json&version=1")).await).unwrap();
    assert_eq!(first["files"]["util.js"], "u");
    // The default read is the rendered page; its footer says which version it is,
    // whether or not this host can render it.
    let rendered = read(&orch, uri).await;
    assert!(rendered.contains("v2 of 2 · written"), "{rendered}");
    assert!(
        rendered.contains(&format!("Source: {uri}?format=json")),
        "{rendered}"
    );
}

#[tokio::test]
async fn an_invalid_write_stores_nothing_and_mints_no_session() {
    let (orch, _) = seeded().await;
    // Creating a thread establishes its session; close it so a write that got
    // as far as storing would have to mint a new one.
    let sessions = session_jobs(&orch, "devops").await;
    for session in &sessions {
        orch.db
            .local
            .execute(
                "UPDATE jobs SET status = 'complete' WHERE id = ?1",
                cairn_db::turso::params![session.as_str()],
            )
            .await
            .unwrap();
    }
    let error = write(
        &orch,
        "cairn://p/prj/devops/pages/board",
        ChangeMode::Create,
        json!({"title": "t", "files": {"app.js": ""}}),
    )
    .await
    .unwrap_err();
    assert!(error.contains("entry"), "{error}");
    assert_eq!(session_jobs(&orch, "devops").await, sessions);
    let stored = orch
        .db
        .local
        .query_opt_i64(
            "SELECT COUNT(*) FROM artifacts WHERE artifact_type = 'page'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(stored, Some(0));
}

#[tokio::test]
async fn a_draft_renders_on_its_node_and_only_promoted_thread_pages_are_indexed() {
    let (orch, _) = seeded().await;
    let draft = "cairn://p/prj/7/1/builder/pages/draft";
    let summary = write(
        &orch,
        draft,
        ChangeMode::Create,
        json!({"title": "Draft", "files": index_js(), "promoted": true}),
    )
    .await
    .unwrap();
    assert!(summary.contains("draft"), "{summary}");
    let rendered = read(&orch, draft).await;
    assert!(
        rendered.contains("draft on cairn://p/prj/7/1/builder"),
        "{rendered}"
    );
    assert!(read(&orch, "cairn://p/prj/7/1/builder/pages")
        .await
        .contains("[draft]"));

    write(
        &orch,
        "cairn://p/prj/devops/pages/release-board",
        ChangeMode::Create,
        json!({"title": "Release board", "description": "Versions and merges", "files": index_js(), "promoted": true}),
    )
    .await
    .unwrap();
    write(
        &orch,
        "cairn://p/prj/roadmap/pages/scratch",
        ChangeMode::Create,
        json!({"title": "Scratch", "files": index_js()}),
    )
    .await
    .unwrap();
    // Promoted once, then un-promoted: the newest version decides.
    write(
        &orch,
        "cairn://p/prj/roadmap/pages/retired",
        ChangeMode::Create,
        json!({"title": "Retired", "files": index_js(), "promoted": true}),
    )
    .await
    .unwrap();
    write(
        &orch,
        "cairn://p/prj/roadmap/pages/retired",
        ChangeMode::Patch,
        json!({"promoted": false}),
    )
    .await
    .unwrap();

    let pages = list_project_pages(&orch.db.local, "p", &PageIndexFilter::default())
        .await
        .unwrap();
    assert_eq!(pages.len(), 1, "{pages:?}");
    let board = &pages[0];
    assert_eq!(board.name, "release-board");
    assert_eq!(board.thread_name, "devops");
    assert_eq!(board.uri, "cairn://p/prj/devops/pages/release-board");
    assert_eq!(board.description.as_deref(), Some("Versions and merges"));
    let wire = serde_json::to_value(board).unwrap();
    for key in [
        "name",
        "title",
        "description",
        "threadName",
        "threadId",
        "version",
        "updatedAt",
        "uri",
    ] {
        assert!(wire.get(key).is_some(), "PageSummary carries {key}");
    }

    let none = list_project_pages(
        &orch.db.local,
        "p",
        &PageIndexFilter {
            thread: Some("roadmap".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(none.is_empty());
    let searched = list_project_pages(
        &orch.db.local,
        "p",
        &PageIndexFilter {
            search: Some("MERGES".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(searched.len(), 1);

    let index = read(&orch, "cairn://p/prj/pages").await;
    assert!(
        index.contains("release-board") && !index.contains("draft") && !index.contains("Retired"),
        "{index}"
    );
}

#[tokio::test]
async fn a_generic_artifact_write_cannot_create_a_page() {
    let (orch, _) = seeded().await;
    let error = write(
        &orch,
        "cairn://p/prj/7/1/builder/page",
        ChangeMode::Create,
        page(index_js()),
    )
    .await
    .unwrap_err();
    assert!(error.contains("cairn:~/pages/<name>"), "{error}");
}

// ---- Bridged reads ----

fn thread_page() -> PageAddress {
    PageAddress::parse("cairn://p/prj/roadmap/pages/board").unwrap()
}

#[test]
fn row_targets_expand_against_the_owner_and_carry_the_row_cap_and_json() {
    let targets: Vec<String> = gate_targets(
        &thread_page(),
        &[
            "cairn:~/db/attempts?where=problem=two-sum&order=-created_at".into(),
            "cairn:~/db/attempts/7".into(),
            "cairn://db/issues?limit=5000".into(),
            "cairn://p/prj/devops/db/notes?limit=12&format=json".into(),
            "cairn:~/db?sql=SELECT * FROM attempts WHERE code LIKE '%x%'".into(),
        ],
    )
    .unwrap()
    .into_iter()
    .map(|target| target.uri)
    .collect();
    assert_eq!(PAGE_ROW_CAP, 200);
    assert_eq!(
        targets,
        vec![
            "cairn://p/prj/roadmap/db/attempts?where=problem%3Dtwo-sum&order=-created_at&limit=200&format=json",
            "cairn://p/prj/roadmap/db/attempts/7?format=json",
            "cairn://db/issues?limit=200&format=json",
            "cairn://p/prj/devops/db/notes?limit=12&format=json",
            "cairn://p/prj/roadmap/db?limit=200&format=json&sql=SELECT * FROM attempts WHERE code LIKE '%x%'",
        ]
    );
    let draft = PageAddress::parse("cairn://p/prj/7/1/builder/pages/draft").unwrap();
    let refused = gate_targets(&draft, &["cairn:~/db/attempts".into()]).unwrap_err();
    assert!(
        refused[0].contains("a draft page has no thread database"),
        "{refused:?}"
    );
}

#[test]
fn the_gate_admits_only_rows_and_names_the_service_pattern() {
    for refused in [
        "file:src/lib.rs",
        "https://api.github.com/x",
        "http://api.github.com/x",
        "cairn://mcp/stripe/tools",
        "cairn://websearch?q=x",
        "cairn://posts",
        "cairn:~/pages",
        "cairn:@/issues?status=active",
        "cairn://p/prj/vcs/log?to=main&limit=5",
        "cairn://p/prj/7/comments?limit=500",
        "cairn://p/prj/attachments/1",
        "docs/readme.pdf",
    ] {
        let error = gate_targets(&thread_page(), &[refused.to_string()]).unwrap_err();
        assert_eq!(error.len(), 1, "{refused}");
        assert!(
            error[0].contains("a page reads only database rows")
                && error[0].contains("a service method"),
            "{refused}: {error:?}"
        );
    }
    for catalog in ["cairn:~/db", "cairn:~/db/schema", "cairn://db"] {
        let error = gate_targets(&thread_page(), &[catalog.to_string()]).unwrap_err();
        assert!(
            error[0].contains("catalog and schema"),
            "{catalog}: {error:?}"
        );
    }
    let unannounced = gate_targets(&thread_page(), &["cairn://db/page_calls".into()]).unwrap_err();
    assert!(
        unannounced[0].contains("not an application table a page reads")
            && unannounced[0].contains("issues, comments"),
        "{unannounced:?}"
    );
    let foreign =
        gate_targets(&thread_page(), &["cairn://p/other/roadmap/db/x".into()]).unwrap_err();
    assert!(foreign[0].contains("its own project"), "{foreign:?}");
    // One refused target refuses the batch.
    assert!(gate_targets(
        &thread_page(),
        &["cairn:~/db/attempts".into(), "cairn:~/pages".into()]
    )
    .is_err());
}

#[test]
fn a_read_that_would_suspend_on_a_fence_becomes_an_error() {
    let error = envelope_or_error(
        "Read suspended pending logical namespace approval; resume will continue once it is answered.",
    )
    .unwrap_err();
    assert!(
        error.contains("cannot wait on a permission answer"),
        "{error}"
    );
}

fn read_request(paths: &[&str], purpose: PageReadPurpose) -> PageReadRequest {
    PageReadRequest {
        project_id: "p".into(),
        page: "cairn://p/prj/roadmap/pages/board".into(),
        version: 1,
        paths: paths.iter().map(|p| p.to_string()).collect(),
        include_bodies: true,
        purpose,
        reader: PageReader::FrameViewer,
    }
}

#[tokio::test]
async fn page_read_answers_rows_as_the_owner_with_scopes_and_logs_each_call() {
    let (orch, _) = seeded_for_writes().await;
    let uri = WRITE_PAGE;
    let viewer = json!({"kind": "anonymous"});

    let response = page_read(
        &orch,
        &read_request(
            &[
                "cairn:~/db/attempts",
                "cairn://db/issues?where=status=active",
            ],
            PageReadPurpose::Read,
        ),
        &viewer,
    )
    .await
    .unwrap();
    assert_eq!(response["reminders"], json!([]));
    assert_eq!(
        response["scopes"],
        json!([
            {"store": "thread", "thread": "roadmap", "tables": ["attempts"]},
            {"store": "app", "tables": ["issues"]},
        ])
    );
    let envelope: cairn_common::read::ReadBatchEnvelope =
        serde_json::from_str(response["result"].as_str().unwrap()).unwrap();
    assert_eq!(
        envelope.segments[0].uri,
        "cairn://p/prj/roadmap/db/attempts?limit=200&format=json"
    );
    let bodies = envelope.bodies.unwrap();
    assert!(bodies[0].trim_start().starts_with('{'), "{}", bodies[0]);
    assert!(bodies[1].contains("\"Draft work\""), "{}", bodies[1]);

    // A subscription's refreshes are covered by its opening row.
    page_read(
        &orch,
        &read_request(&["cairn:~/db/attempts"], PageReadPurpose::SubscribeOpen),
        &viewer,
    )
    .await
    .unwrap();
    page_read(
        &orch,
        &read_request(&["cairn:~/db/attempts"], PageReadPurpose::SubscribeRefresh),
        &viewer,
    )
    .await
    .unwrap();

    let refused = page_read(
        &orch,
        &read_request(
            &["cairn:~/db/attempts", "https://api.github.com/x"],
            PageReadPurpose::Read,
        ),
        &viewer,
    )
    .await
    .unwrap_err();
    assert!(matches!(refused, PageCallError::Refused(_)), "{refused:?}");
    let file = page_read(
        &orch,
        &read_request(&["file:Cargo.toml"], PageReadPurpose::Read),
        &viewer,
    )
    .await
    .unwrap_err();
    assert!(file.to_string().contains("only database rows"), "{file}");

    let rows = actor_rows(&orch, uri).await;
    let shape: Vec<(&str, &str)> = rows
        .iter()
        .map(|(v, o, _)| (v.as_str(), o.as_str()))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("read", "ok"),
            ("subscribe", "ok"),
            ("read", "refused"),
            ("read", "refused")
        ]
    );
    assert!(rows
        .iter()
        .all(|(_, _, actor)| actor == r#"{"kind":"anonymous"}"#));

    let log = read(&orch, &format!("{uri}/log?outcome=refused")).await;
    assert!(
        log.contains("2 call(s)") && log.contains("only database rows"),
        "{log}"
    );

    let missing = page_read(
        &orch,
        &PageReadRequest {
            version: 9,
            ..read_request(&["cairn:~/db/attempts"], PageReadPurpose::Read)
        },
        &viewer,
    )
    .await
    .unwrap_err();
    assert!(matches!(missing, PageCallError::BadRequest(_)));
}

/// A SQL read follows exactly the tables its statement reads, derived from the
/// store's catalog; one over the application database that reaches a table
/// outside the announced set is refused, so it can never sit stale.
#[tokio::test]
async fn a_sql_read_is_scoped_to_the_tables_it_reads() {
    let (orch, _) = seeded_for_writes().await;
    let viewer = json!({"kind": "anonymous"});
    let response = page_read(
        &orch,
        &read_request(
            &[
                "cairn:~/db?sql=SELECT a.problem FROM attempts a",
                "cairn://db?sql=SELECT i.title, COUNT(c.id) FROM issues i LEFT JOIN comments c ON c.issue_id = i.id GROUP BY i.id",
            ],
            PageReadPurpose::Read,
        ),
        &viewer,
    )
    .await
    .unwrap();
    assert_eq!(
        response["scopes"],
        json!([
            {"store": "thread", "thread": "roadmap", "tables": ["attempts"]},
            {"store": "app", "tables": ["comments", "issues"]},
        ])
    );

    let refused = page_read(
        &orch,
        &read_request(
            &["cairn://db?sql=SELECT * FROM page_calls"],
            PageReadPurpose::Read,
        ),
        &viewer,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&refused, PageCallError::Refused(why) if why.contains("`page_calls` is not an application table a page reads")),
        "{refused:?}"
    );
}

/// Give the seeded issue a description far larger than an agent's read budget.
async fn seed_large_issue(orch: &Orchestrator) -> String {
    let description = (0..20_000)
        .map(|index| format!("word{index}"))
        .collect::<Vec<_>>()
        .join(" ");
    orch.db
        .local
        .execute(
            "UPDATE issues SET description = ?1 WHERE id = 'i'",
            cairn_db::turso::params![description.clone()],
        )
        .await
        .unwrap();
    description
}

#[tokio::test]
async fn a_page_read_carries_a_body_past_the_agent_budget_whole() {
    let (orch, _) = seeded().await;
    write(
        &orch,
        "cairn://p/prj/roadmap/pages/board",
        ChangeMode::Create,
        page(index_js()),
    )
    .await
    .unwrap();
    let description = seed_large_issue(&orch).await;

    // An agent's read of the same target is windowed under its context budget.
    let agent = crate::mcp::handlers::read::handle_read_batch(
        &orch,
        &McpCallbackRequest {
            tool: "read_batch".to_string(),
            payload: json!({"paths": ["cairn://p/prj/7"], "include_bodies": true}),
            ..request()
        },
        &std::sync::Mutex::new(std::collections::HashMap::new()),
    )
    .await;
    let agent: cairn_common::read::ReadBatchEnvelope = serde_json::from_str(&agent).unwrap();
    assert!(agent.segments[0].truncated);

    let response = page_read(
        &orch,
        &read_request(&["cairn://db/issues/i"], PageReadPurpose::Read),
        &json!({"kind": "anonymous"}),
    )
    .await
    .unwrap();
    let envelope: cairn_common::read::ReadBatchEnvelope =
        serde_json::from_str(response["result"].as_str().unwrap()).unwrap();
    let segment = &envelope.segments[0];
    assert_eq!(segment.kind, cairn_common::read::SegmentKind::Resource);
    assert!(!segment.truncated);
    assert!(!segment.char_continuation);
    assert!(envelope.text.len() > crate::storage::render::CLIENT_TOOL_RESULT_BYTE_CEILING);
    assert!(envelope.bodies.unwrap()[0].contains(&description));
}

#[tokio::test]
async fn a_page_read_refuses_a_target_past_its_byte_ceiling_rather_than_cutting_it() {
    let (orch, _) = seeded().await;
    seed_large_issue(&orch).await;

    let raw =
        super::read::dispatch_as_owner(&orch, &["cairn://p/prj/7".to_string()], true, 10_000).await;
    let envelope = envelope_or_error(&raw).unwrap();
    let segment = &envelope.segments[0];
    assert_eq!(segment.kind, cairn_common::read::SegmentKind::Error);
    assert!(!segment.truncated);
    let body = &envelope.bodies.unwrap()[0];
    assert!(body.contains("refused rather than cut"), "{body}");
    assert!(body.contains("10000-byte ceiling"), "{body}");
    assert!(!body.contains("word19999"), "{body}");
}

#[tokio::test]
async fn page_log_event_records_against_an_existing_version_only() {
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    write(&orch, uri, ChangeMode::Create, page(index_js()))
        .await
        .unwrap();
    let event = PageLogEvent {
        project_id: "p".into(),
        page: uri.into(),
        version: 1,
        verb: PageCallVerb::Diag,
        outcome: PageCallOutcome::Error,
        targets: Vec::new(),
        detail: "TypeError: x is undefined".into(),
    };
    page_log_event(&orch, &event, &json!({"kind": "anonymous"}))
        .await
        .unwrap();
    let log = read(&orch, &format!("{uri}/log?verb=diag")).await;
    assert!(log.contains("TypeError"), "{log}");

    let missing = page_log_event(
        &orch,
        &PageLogEvent {
            version: 2,
            ..event
        },
        &json!({"kind": "anonymous"}),
    )
    .await
    .unwrap_err();
    assert!(matches!(missing, PageCallError::BadRequest(_)));
}

#[tokio::test]
async fn the_log_keeps_the_newest_rows_per_page_and_truncates_detail() {
    let (orch, _) = seeded().await;
    let actor = json!({"kind": "anonymous"});
    for n in 0..5 {
        for uri in [
            "cairn://p/prj/roadmap/pages/a",
            "cairn://p/prj/roadmap/pages/b",
        ] {
            let detail = format!("call {n}");
            record_retaining(
                &orch.db.local,
                &PageCall {
                    project_id: "p",
                    page_artifact_id: "art",
                    page_uri: uri,
                    page_version: 1,
                    actor: &actor,
                    verb: PageCallVerb::Read,
                    targets: &[],
                    outcome: PageCallOutcome::Ok,
                    detail: &detail,
                },
                3,
            )
            .await
            .unwrap();
        }
    }
    for uri in [
        "cairn://p/prj/roadmap/pages/a",
        "cairn://p/prj/roadmap/pages/b",
    ] {
        let rows = list(&orch.db.local, uri, 50, None, None).await.unwrap();
        let details: Vec<&str> = rows.iter().map(|row| row.detail.as_str()).collect();
        assert_eq!(details, vec!["call 4", "call 3", "call 2"], "{uri}");
    }

    let long = "é".repeat(DETAIL_MAX_BYTES);
    let cut = truncate_detail(&long);
    assert!(cut.len() <= DETAIL_MAX_BYTES && cut.ends_with("[truncated]"));
    assert_eq!(truncate_detail("short"), "short");
}

// ---------------------------------------------------------------------------
// page_write
// ---------------------------------------------------------------------------

const WRITE_PAGE: &str = "cairn://p/prj/roadmap/pages/board";
pub(crate) const ATTEMPTS: &str = "cairn://p/prj/roadmap/db/attempts";

pub(crate) fn operator() -> PageOperator {
    use cairn_common::identity::{
        Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, CredentialRef,
        PrincipalRef, VerificationMethod, VerificationRecord, VerificationStatus,
        VerificationStrength,
    };
    let actor = PrincipalRef::Machine {
        device_id: "device-1".to_string(),
    };
    let verification = VerificationRecord::new(
        VerificationMethod::DesktopCredential,
        VerificationStatus::Verified,
        None,
        None,
        None,
        Some(CredentialRef::new("desktop_operator_credential").unwrap()),
        VerificationStrength::new("local_shared_secret").unwrap(),
        1,
    )
    .unwrap();
    let evidence = AppearanceEvidence::new(
        AppearanceTransport::AuthenticatedDesktop,
        Address::Desktop {
            device_id: "device-1".to_string(),
        },
        verification,
        1,
        None,
    )
    .unwrap();
    PageOperator {
        appearance: AppearanceSnapshot::new(actor.clone(), evidence, Vec::new(), None).unwrap(),
        actor,
        log_actor: json!({"kind": "desktop_operator", "deviceId": "device-1"}),
    }
}

/// The roadmap thread with an `attempts` table, and a page declaring it.
pub(crate) async fn seeded_for_writes() -> (Orchestrator, Arc<CapturingEmitter>) {
    let (orch, emitter) = seeded().await;
    write(
        &orch,
        "cairn://p/prj/roadmap/db",
        ChangeMode::Patch,
        json!({"sql": "CREATE TABLE attempts(id INTEGER PRIMARY KEY, problem TEXT NOT NULL, code TEXT, result TEXT, created_at INTEGER); CREATE TABLE other(id INTEGER PRIMARY KEY, x TEXT)"}),
    )
    .await
    .unwrap();
    write(
        &orch,
        WRITE_PAGE,
        ChangeMode::Create,
        json!({"title": "Board", "files": index_js(),
               "manifest": {"writes": ["cairn:~/db/attempts"]}}),
    )
    .await
    .unwrap();
    (orch, emitter)
}

fn write_request(input: Value) -> PageWriteRequest {
    PageWriteRequest {
        project_id: "p".into(),
        page: WRITE_PAGE.into(),
        version: 1,
        input,
    }
}

fn insert(table: &str, row: Value) -> Value {
    json!({"target": format!("cairn:~/db/{table}"), "mode": "append", "payload": row})
}

pub(crate) async fn attempt_count(orch: &Orchestrator) -> usize {
    let rows = read(orch, &format!("{ATTEMPTS}?format=json")).await;
    // The JSON document leads the read; its affordance block follows it.
    let parsed: Value = serde_json::Deserializer::from_str(&rows)
        .into_iter::<Value>()
        .next()
        .and_then(Result::ok)
        .unwrap_or(Value::Null);
    parsed
        .get("rows")
        .and_then(Value::as_array)
        .map_or_else(|| panic!("unexpected rows read: {rows}"), Vec::len)
}

async fn write_log(orch: &Orchestrator) -> Vec<(String, String, Value)> {
    orch.db
        .local
        .query_all(
            "SELECT outcome, targets_json, actor_json FROM page_calls
             WHERE page_uri = ?1 AND verb = 'write' ORDER BY created_at, rowid",
            cairn_db::turso::params![WRITE_PAGE],
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|(outcome, targets, actor)| (outcome, targets, serde_json::from_str(&actor).unwrap()))
        .collect()
}

#[tokio::test]
async fn a_page_write_inserts_a_row_attributed_to_the_operator_through_the_page() {
    let (orch, emitter) = seeded_for_writes().await;
    let response = page_write(
        &orch,
        &write_request(json!({"changes": [insert("attempts",
            json!({"problem": "two-sum", "code": "x", "result": "ok"}))]})),
        &operator(),
    )
    .await
    .unwrap();
    assert_eq!(response["reminders"], json!([]));
    let report: Value = serde_json::from_str(response["result"].as_str().unwrap()).unwrap();
    let row = report["applied"][0]["data"]["uri"]
        .as_str()
        .unwrap_or_else(|| panic!("no row uri: {report}"))
        .to_string();
    assert_eq!(row, format!("{ATTEMPTS}/1"));
    assert!(read(&orch, &row).await.contains("two-sum"));

    let changes: Vec<Value> = emitter
        .events_named("db-change")
        .into_iter()
        .filter(|event| event.to_string().contains(&row))
        .collect();
    assert_eq!(changes.len(), 1, "{changes:?}");

    let log = write_log(&orch).await;
    assert_eq!(log.len(), 1);
    let (outcome, targets, actor) = &log[0];
    assert_eq!(outcome, "ok");
    assert_eq!(targets, &json!([row]).to_string());
    assert_eq!(actor["kind"], "desktop_operator");
    assert_eq!(
        actor["instrument"],
        json!({"page": WRITE_PAGE, "version": 1, "owner": "cairn://p/prj/roadmap"})
    );
    let rendered = read(&orch, &format!("{WRITE_PAGE}/log")).await;
    assert!(
        rendered.contains("write") && rendered.contains(&row),
        "{rendered}"
    );

    let journal = orch
        .db
        .local
        .query_all(
            "SELECT outcome, mutation, summary, principal_json, actor_principal_json,
                    appearance_snapshot_json FROM authorization_events",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.opt_text(4)?,
                    row.opt_text(5)?,
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(journal.len(), 1, "{journal:?}");
    let (outcome, mutation, summary, principal, actor, appearance) = &journal[0];
    assert_eq!((outcome.as_str(), mutation.as_str()), ("direct", "create"));
    assert_eq!(summary, &row);
    let principal: Value = serde_json::from_str(principal).unwrap();
    assert_eq!(principal["run_id"], Value::Null);
    assert_eq!(
        principal["actor"],
        json!({"kind": "machine", "device_id": "device-1"})
    );
    assert_eq!(
        principal["instrument"],
        json!({"kind": "page", "page": WRITE_PAGE, "version": 1, "owner": "cairn://p/prj/roadmap"})
    );
    assert!(actor.as_deref().unwrap().contains("device-1"));
    assert!(appearance
        .as_deref()
        .unwrap()
        .contains("authenticated_desktop"));

    // patch and delete address the row the insert returned.
    for change in [
        json!({"target": "cairn:~/db/attempts/1", "mode": "patch", "payload": {"result": "fail"}}),
        json!({"target": "cairn:~/db/attempts/1", "mode": "delete"}),
    ] {
        let response = page_write(
            &orch,
            &write_request(json!({"changes": [change]})),
            &operator(),
        )
        .await
        .unwrap();
        let report: Value = serde_json::from_str(response["result"].as_str().unwrap()).unwrap();
        assert_eq!(report["applied"][0]["data"]["uri"], json!(row), "{report}");
    }
    assert_eq!(attempt_count(&orch).await, 0);
}

#[tokio::test]
async fn a_page_write_outside_its_manifest_is_refused_whole_and_logged() {
    let (orch, _) = seeded_for_writes().await;
    let good = insert("attempts", json!({"problem": "p"}));
    for (input, why) in [
        (
            json!({"changes": [insert("other", json!({"x": "1"}))]}),
            "manifest.writes",
        ),
        (
            json!({"changes": [{"target": "cairn:@/issues", "mode": "append", "payload": {"title": "t"}}]}),
            "manifest.writes",
        ),
        (
            json!({"changes": [{"target": "file:x", "mode": "create", "payload": {"content": ""}}]}),
            "file:",
        ),
        (
            json!({"changes": [{"target": "cairn:~/db", "mode": "patch", "payload": {"sql": "DELETE FROM attempts"}}]}),
            "manifest.writes",
        ),
        (
            json!({"changes": [{"target": "cairn:~/db/attempts", "mode": "patch", "payload": {"problem": "q"}}]}),
            "mode `patch`",
        ),
        (
            json!({"changes": [good.clone(), insert("other", json!({"x": "1"}))]}),
            "manifest.writes",
        ),
        (
            json!({"changes": [{"target": "cairn://p/prj/devops/db/attempts", "mode": "append", "payload": {}}]}),
            "manifest.writes",
        ),
        (
            json!({"changes": [good.clone()], "commit_msg": "x"}),
            "`commit_msg` is refused",
        ),
    ] {
        let refused = page_write(&orch, &write_request(input.clone()), &operator())
            .await
            .unwrap_err();
        let PageCallError::Refused(message) = &refused else {
            panic!("{input}: {refused:?}");
        };
        assert!(message.contains(why), "{input}: {message}");
        if why == "manifest.writes" {
            assert!(
                message.contains("board@v1's manifest.writes [cairn:~/db/attempts]"),
                "{message}"
            );
        }
    }
    assert_eq!(attempt_count(&orch).await, 0);
    let log = write_log(&orch).await;
    assert_eq!(log.len(), 8);
    assert!(log.iter().all(|(outcome, _, _)| outcome == "refused"));
}

#[tokio::test]
async fn a_page_appends_many_rows_and_deletes_the_rows_a_where_names() {
    let (orch, emitter) = seeded_for_writes().await;
    let response = page_write(
        &orch,
        &write_request(json!({"changes": [insert("attempts", json!({"rows": [
            {"problem": "a", "result": "ok"},
            {"problem": "b"},
            {"problem": "c"},
        ]}))]})),
        &operator(),
    )
    .await
    .unwrap();
    let report: Value = serde_json::from_str(response["result"].as_str().unwrap()).unwrap();
    assert_eq!(
        report["applied"][0]["data"]["uris"],
        json!([
            format!("{ATTEMPTS}/1"),
            format!("{ATTEMPTS}/2"),
            format!("{ATTEMPTS}/3")
        ]),
        "{report}"
    );
    assert_eq!(attempt_count(&orch).await, 3);
    // The call log names every row the bulk append wrote.
    let (_, targets, _) = write_log(&orch).await.pop().unwrap();
    assert_eq!(
        targets,
        json!([
            format!("{ATTEMPTS}/1"),
            format!("{ATTEMPTS}/2"),
            format!("{ATTEMPTS}/3")
        ])
        .to_string()
    );

    let before = emitter.events_named("db-change").len();
    let response = page_write(
        &orch,
        &write_request(json!({"changes": [
            {"target": "cairn:~/db/attempts?where=result=null", "mode": "delete"}
        ]})),
        &operator(),
    )
    .await
    .unwrap();
    let report: Value = serde_json::from_str(response["result"].as_str().unwrap()).unwrap();
    assert_eq!(
        report["applied"][0]["data"],
        json!({"count": 2}),
        "{report}"
    );
    assert_eq!(attempt_count(&orch).await, 1);
    let announced: Vec<Value> = emitter.events_named("db-change").split_off(before);
    assert!(
        announced
            .iter()
            .any(|event| event["dbTable"] == "attempts" && event["action"] == "delete"),
        "{announced:?}"
    );

    for (change, why) in [
        (
            json!({"target": "cairn:~/db/attempts?limit=1", "mode": "delete"}),
            "only query is where=",
        ),
        (
            json!({"target": "cairn:~/db/attempts?where=problem=a", "mode": "append", "payload": {"problem": "x"}}),
            "mode `append`",
        ),
        (
            json!({"target": "cairn:~/db/other?where=x=1", "mode": "delete"}),
            "manifest.writes",
        ),
    ] {
        let refused = page_write(
            &orch,
            &write_request(json!({"changes": [change.clone()]})),
            &operator(),
        )
        .await
        .unwrap_err();
        assert!(refused.to_string().contains(why), "{change}: {refused}");
    }
    assert_eq!(attempt_count(&orch).await, 1);
}

#[tokio::test]
async fn a_superseded_version_and_a_draft_cannot_write() {
    let (orch, _) = seeded_for_writes().await;
    write(
        &orch,
        WRITE_PAGE,
        ChangeMode::Patch,
        json!({"title": "Board 2"}),
    )
    .await
    .unwrap();
    let refused = page_write(
        &orch,
        &write_request(json!({"changes": [insert("attempts", json!({"problem": "p"}))]})),
        &operator(),
    )
    .await
    .unwrap_err();
    assert!(
        refused
            .to_string()
            .contains("was updated to v2; this frame is showing v1"),
        "{refused}"
    );
    assert_eq!(attempt_count(&orch).await, 0);

    let draft = write_as(
        &orch,
        "node-run",
        "cairn://p/prj/7/1/builder/pages/sketch",
        json!({"title": "Sketch", "files": index_js(),
               "manifest": {"writes": ["cairn:~/db/attempts"]}}),
    )
    .await
    .unwrap_err();
    assert!(draft.contains("a draft page cannot write"), "{draft}");
}

/// The write path's own floor, below the page gate: a request carrying a page
/// origin reaches nothing but its owner's rows, however it was built.
#[tokio::test]
async fn a_page_origin_reaches_only_its_owners_rows_in_the_write_path() {
    let (orch, _) = seeded_for_writes().await;
    let operator = operator();
    let origin = cairn_common::protocol::RowWriteOrigin {
        instrument: cairn_common::protocol::RowWriteInstrument::Page {
            page: WRITE_PAGE.to_string(),
            version: 1,
        },
        owner_thread_id: crate::threads::crud::list(&orch.db.local, "p")
            .await
            .unwrap()
            .into_iter()
            .find(|thread| thread.name == "roadmap")
            .unwrap()
            .id,
        owner: "cairn://p/prj/roadmap".to_string(),
        project_id: "p".to_string(),
        actor: operator.actor.clone(),
        appearance: operator.appearance.clone(),
    };
    let dispatch = |payload: Value| {
        let origin = origin.clone();
        let orch = &orch;
        async move {
            let request = McpCallbackRequest {
                tool: "write".to_string(),
                payload,
                origin: Some(cairn_common::protocol::DispatchOrigin::RowWrite(Box::new(
                    origin,
                ))),
                ..request()
            };
            let raw = crate::mcp::handlers::write::handle_write(orch, &request).await;
            serde_json::from_str::<Value>(&raw).unwrap_or_else(|_| panic!("{raw}"))
        }
    };
    for change in [
        // Operator-only: a tenet verdict, a settings section, a recipe.
        json!({"target": "cairn://p/prj/tenets/1", "mode": "patch", "payload": {"verdict": "approved"}}),
        json!({"target": "cairn://settings", "mode": "patch", "payload": {"backends": {}}}),
        json!({"target": "cairn://recipes/build", "mode": "patch", "payload": {"description": "x"}}),
        // Raw SQL, a deictic that would need a run, a file.
        json!({"target": "cairn://p/prj/roadmap/db", "mode": "patch", "payload": {"sql": "DELETE FROM attempts"}}),
        json!({"target": "cairn:~/db/attempts", "mode": "append", "payload": {"problem": "p"}}),
        json!({"target": "file:x", "mode": "create", "payload": {"content": ""}}),
        // Another thread's rows.
        json!({"target": "cairn://p/prj/devops/db/attempts", "mode": "append", "payload": {"problem": "p"}}),
    ] {
        let report = dispatch(json!({"changes": [change.clone()]})).await;
        assert_eq!(report["applied"], json!([]), "{change}: {report}");
        assert!(
            report["failures"].as_array().is_some_and(|f| !f.is_empty()),
            "{change}: {report}"
        );
    }
    let committed =
        dispatch(json!({"changes": [insert("attempts", json!({}))], "commit_msg": "x"})).await;
    assert_eq!(committed["applied"], json!([]), "{committed}");

    // And it does reach its owner's rows.
    let owned = dispatch(
        json!({"changes": [{"target": ATTEMPTS, "mode": "append", "payload": {"problem": "p"}}]}),
    )
    .await;
    assert_eq!(
        owned["applied"][0]["data"]["uri"],
        json!(format!("{ATTEMPTS}/1")),
        "{owned}"
    );
}

/// The instrument is the door's, never the caller's: a page write naming
/// another instrument, in its input or in a change, is refused whole, and an
/// admitted page write is journaled through the page.
#[tokio::test]
async fn a_page_write_cannot_name_its_own_instrument() {
    let (orch, _) = seeded_for_writes().await;
    for input in [
        json!({"changes": [insert("attempts", json!({"problem": "p"}))], "instrument": "browser"}),
        json!({"changes": [{"target": "cairn:~/db/attempts", "mode": "append",
                             "payload": {"problem": "p"}, "instrument": "db_browser"}]}),
    ] {
        let refused = page_write(&orch, &write_request(input.clone()), &operator())
            .await
            .unwrap_err();
        assert!(
            matches!(&refused, PageCallError::Refused(message) if message.contains("`instrument`")),
            "{input}: {refused:?}"
        );
    }
    assert_eq!(attempt_count(&orch).await, 0);
}

/// Without an origin, a run-less row write is still refused: the origin is
/// the only thing that lends a request owner standing, and JSON cannot carry
/// one (see `cairn_common::protocol`'s tests).
#[tokio::test]
async fn a_runless_row_write_without_an_origin_is_refused() {
    let (orch, _) = seeded_for_writes().await;
    let forged: McpCallbackRequest = serde_json::from_value(json!({
        "cwd": "",
        "tool": "write",
        "payload": {"changes": [{"target": ATTEMPTS, "mode": "append", "payload": {"problem": "p"}}]},
        "origin": {"RowWrite": {"instrument": {"Page": {"page": WRITE_PAGE}}}},
    }))
    .unwrap();
    assert!(forged.origin.is_none());
    let raw = crate::mcp::handlers::write::handle_write(&orch, &forged).await;
    let report: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(report["applied"], json!([]));
    assert!(
        report["failures"][0]["error"]
            .as_str()
            .unwrap()
            .contains("writable only by that thread"),
        "{report}"
    );
    assert_eq!(attempt_count(&orch).await, 0);
}

// ---- Services ----

#[test]
fn manifest_services_are_service_ids_declared_once() {
    let with = |services: Value| json!({"title": "Board", "files": index_js(), "manifest": {"services": services}});
    assert!(validate(&with(json!(["echo", "judge-2"]))).is_ok());
    for bad in [json!(["Echo"]), json!(["../x"]), json!(["echo", "echo"])] {
        assert!(validate(&with(bad.clone())).is_err(), "{bad}");
    }
    let many: Vec<String> = (0..17).map(|n| format!("s{n}")).collect();
    assert!(validate(&with(json!(many))).is_err());
}

#[tokio::test]
async fn a_page_reaches_only_the_services_its_manifest_declares_and_every_call_is_logged() {
    let (orch, _) = seeded().await;
    let (host, _) = crate::project_services::supervisor::tests::install_fakes(&orch);
    let declared = "cairn://p/prj/roadmap/pages/board";
    write(
        &orch,
        declared,
        ChangeMode::Create,
        json!({"title": "Board", "files": index_js(), "manifest": {"services": ["echo", "judge"]}}),
    )
    .await
    .unwrap();
    let plain = "cairn://p/prj/roadmap/pages/plain";
    write(&orch, plain, ChangeMode::Create, page(index_js()))
        .await
        .unwrap();
    let viewer = json!({"kind": "desktop_operator", "deviceId": "device-1"});
    let call = |page: &str, service: &str, params: Value| PageServiceRequest {
        project_id: "p".into(),
        page: page.into(),
        version: 1,
        service: service.into(),
        method: "echo".into(),
        params,
    };

    let answered = page_service(&orch, &call(declared, "echo", json!({"hi": 1})), &viewer)
        .await
        .unwrap();
    assert_eq!(answered["ok"], true, "{answered}");
    assert_eq!(answered["value"]["params"], json!({"hi": 1}));

    // The service sees a page caller: the owning thread as context, the page
    // as the instrument, and only the viewer's classification.
    let frame = &host.frames_of("call")[0];
    assert_eq!(frame["caller"]["kind"], "page");
    assert_eq!(frame["caller"]["actingFor"], "cairn://p/prj/roadmap");
    assert_eq!(frame["caller"]["instrument"]["page"], declared);
    assert_eq!(
        frame["caller"]["viewer"],
        json!({"kind": "desktop_operator"})
    );

    let undeclared = page_service(&orch, &call(plain, "echo", json!({})), &viewer)
        .await
        .unwrap();
    assert_eq!(undeclared["error"]["code"], "not_declared", "{undeclared}");
    assert!(undeclared["error"]["message"]
        .as_str()
        .unwrap()
        .contains("manifest.services"));
    let unknown = page_service(&orch, &call(declared, "judge", json!({})), &viewer)
        .await
        .unwrap();
    assert_eq!(unknown["error"]["code"], "unknown_service", "{unknown}");
    let oversized = page_service(
        &orch,
        &call(declared, "echo", json!("x".repeat(1024 * 1024 + 1))),
        &viewer,
    )
    .await
    .unwrap();
    assert_eq!(oversized["error"]["code"], "params_too_large");
    assert_eq!(
        host.frames_of("call").len(),
        1,
        "no refused call reached the service"
    );

    let shape = |rows: Vec<(String, String, String)>| {
        rows.into_iter()
            .map(|(verb, outcome, _)| (verb, outcome))
            .collect::<Vec<_>>()
    };
    let service_ok = ("service".to_string(), "ok".to_string());
    let service_refused = ("service".to_string(), "refused".to_string());
    assert_eq!(
        shape(actor_rows(&orch, declared).await),
        vec![service_ok, service_refused.clone(), service_refused.clone()]
    );
    assert_eq!(shape(actor_rows(&orch, plain).await), vec![service_refused]);
}

// ---- The reader's grants ----

/// A page reads only databases, and an agent run may read every database the
/// owner gate admits, so an agent reader narrows nothing; a reader whose
/// grants cannot be represented narrows everything.
#[test]
fn an_agent_reader_reads_every_database_the_owner_gate_admits() {
    let targets = gate_targets(
        &thread_page(),
        &["cairn:~/db/attempts".into(), "cairn://db/issues".into()],
    )
    .unwrap();
    for reader in [
        PageReader::FrameViewer,
        PageReader::Operator,
        PageReader::Agent(AgentReader {
            run_id: "r".into(),
            home: "cairn://p/other/9/1/builder".into(),
        }),
    ] {
        reader
            .narrow(&targets)
            .unwrap_or_else(|refused| panic!("{reader:?} narrowed {refused:?}"));
    }
    let refused = PageReader::Unrepresentable("no grants".into())
        .narrow(&targets)
        .unwrap_err();
    assert_eq!(refused.len(), 2, "{refused:?}");
}

/// The absence of a representable grant never selects the owner's authority.
#[tokio::test]
async fn a_reader_whose_grants_cannot_be_represented_is_refused_every_read() {
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    write(&orch, uri, ChangeMode::Create, page(index_js()))
        .await
        .unwrap();
    let reader = PageReader::for_request(
        &orch,
        &McpCallbackRequest {
            run_id: Some("no-such-run".into()),
            ..request()
        },
    )
    .await;
    assert!(
        matches!(reader, PageReader::Unrepresentable(_)),
        "{reader:?}"
    );
    let refused = page_read(
        &orch,
        &PageReadRequest {
            reader,
            ..read_request(&["cairn://db/issues"], PageReadPurpose::Read)
        },
        &json!({"kind": "anonymous"}),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&refused, PageCallError::Refused(why) if why.contains("does not resolve to an agent")),
        "{refused:?}"
    );
    let rows = actor_rows(&orch, uri).await;
    assert_eq!(
        rows.last().map(|(v, o, _)| (v.as_str(), o.as_str())),
        Some(("read", "refused"))
    );
}

#[tokio::test]
async fn an_agent_reader_is_recorded_as_the_viewer() {
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    write(&orch, uri, ChangeMode::Create, page(index_js()))
        .await
        .unwrap();
    let reader = PageReader::for_request(
        &orch,
        &McpCallbackRequest {
            run_id: Some("node-run".into()),
            ..request()
        },
    )
    .await;
    page_read(
        &orch,
        &PageReadRequest {
            reader,
            ..read_request(&["cairn://db/issues"], PageReadPurpose::Read)
        },
        &json!({"kind": "anonymous"}),
    )
    .await
    .unwrap();
    let log = read(&orch, &format!("{uri}/log")).await;
    assert!(
        log.contains("viewer: agent cairn://p/prj/7/1/builder"),
        "{log}"
    );
}

#[tokio::test]
async fn a_page_view_names_its_formats_and_reserves_screenshot() {
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    write(&orch, uri, ChangeMode::Create, page(index_js()))
        .await
        .unwrap();
    assert!(read(&orch, &format!("{uri}?format=yaml"))
        .await
        .contains("text (the rendered page, the default) or json"));
    assert!(read(&orch, &format!("{uri}?screenshot"))
        .await
        .contains("later slice"));
    let source: Value =
        serde_json::from_str(&read(&orch, &format!("{uri}?format=json")).await).unwrap();
    assert_eq!(source["files"], index_js());
}

// ---- Rendered reads (real bun, real confinement) ----

/// Whether this host can confine the render child. On macOS the only cause it
/// cannot is a test process already inside a Seatbelt profile, the declared
/// `nested-sandbox-exec` gap, recorded as a skip. Anywhere else a host that
/// cannot confine fails the test: a Linux lane without Landlock and seccomp is
/// broken, not exempt, and no declarable reason describes it.
fn renders_here(test: &str) -> bool {
    match cairn_sandbox::IsolatedChild::availability() {
        Ok(()) => true,
        Err(why) if cfg!(target_os = "macos") => {
            eprintln!("skipping {test}: {why}");
            cairn_common::skip_ledger::record_skip(test, "nested-sandbox-exec");
            false
        }
        Err(why) => panic!("{test} needs a host that can confine a rendered page: {why}"),
    }
}

fn agent_request() -> McpCallbackRequest {
    McpCallbackRequest {
        run_id: Some("node-run".into()),
        ..request()
    }
}

async fn read_as_agent(orch: &Orchestrator, uri: &str) -> String {
    crate::resources::read_cairn_resource(orch, &agent_request(), uri).await
}

async fn write_page_source(
    orch: &Orchestrator,
    uri: &str,
    mode: ChangeMode,
    title: &str,
    files: Value,
) {
    write(orch, uri, mode, json!({ "title": title, "files": files }))
        .await
        .unwrap();
}

const BOARD: &str = r#"import { html, Page, Section, Stat, DataTable, Freshness, useRows } from "@cairn/page-kit";
export default function Board() {
  const issues = useRows("issues", { where: { status: "active" }, order: "number" }, { store: "app" });
  return html`<${Page} title="Fixture board" actions=${html`<${Freshness} ...${issues} />`}>
    <${Section} title="Issues" chip=${String(issues.rows.length)}>
      <${Stat} label="Active" value=${issues.rows.length} detail="in the app database" />
      <${DataTable} columns=${["number", "title"]} rows=${issues.rows} />
    <//>
  <//>`;
}
"#;

#[tokio::test]
async fn a_page_renders_as_text_for_the_agent_that_reads_it() {
    if !renders_here("a_page_renders_as_text_for_the_agent_that_reads_it") {
        return;
    }
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    write_page_source(
        &orch,
        uri,
        ChangeMode::Create,
        "Board",
        json!({ "index.js": BOARD }),
    )
    .await;

    let body = read_as_agent(&orch, uri).await;
    assert!(body.contains("# Fixture board — live · updated"), "{body}");
    assert!(body.contains("## Issues · 1"), "{body}");
    assert!(
        body.contains("**Active**: 1 — in the app database"),
        "{body}"
    );
    assert!(body.contains("| number | title |"), "{body}");
    assert!(body.contains("Draft work"), "{body}");
    assert!(
        body.contains("v1 of 1") && body.contains("settled"),
        "{body}"
    );

    let log = read(&orch, &format!("{uri}/log")).await;
    assert!(log.contains("subscribe · ok"), "{log}");
    assert!(
        log.contains("viewer: agent cairn://p/prj/7/1/builder"),
        "{log}"
    );

    // The source is one format away, unchanged.
    let source: Value =
        serde_json::from_str(&read_as_agent(&orch, &format!("{uri}?format=json")).await).unwrap();
    assert_eq!(source["files"]["index.js"], BOARD);
}

#[tokio::test]
async fn a_version_renders_as_it_was() {
    if !renders_here("a_version_renders_as_it_was") {
        return;
    }
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    let titled = |title: &str| json!({ "index.js": format!("import {{ html, Page }} from \"@cairn/page-kit\";\nexport default () => html`<${{Page}} title=\"{title}\" />`;\n") });
    write_page_source(&orch, uri, ChangeMode::Create, "Board", titled("First")).await;
    write_page_source(&orch, uri, ChangeMode::Patch, "Board", titled("Second")).await;
    let first = read_as_agent(&orch, &format!("{uri}?version=1")).await;
    assert!(
        first.contains("# First") && first.contains("v1 of 2"),
        "{first}"
    );
    let latest = read_as_agent(&orch, uri).await;
    assert!(
        latest.contains("# Second") && latest.contains("v2 of 2"),
        "{latest}"
    );
}

#[tokio::test]
async fn a_page_that_throws_renders_its_failure_and_logs_it() {
    if !renders_here("a_page_that_throws_renders_its_failure_and_logs_it") {
        return;
    }
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    // A value the transpiler cannot fold away: folding a constant removes its
    // line, and the reported line is then the transpiled one.
    let source = "import { html, pageInfo } from \"@cairn/page-kit\";\nexport default function Broken() {\n  const rows = pageInfo().rows;\n  return html`<p>${rows.length}</p>`;\n}\n";
    write_page_source(
        &orch,
        uri,
        ChangeMode::Create,
        "Board",
        json!({ "index.js": source }),
    )
    .await;
    let body = read_as_agent(&orch, uri).await;
    assert!(body.contains("Page failed at index.js:4:"), "{body}");
    assert!(body.contains("failed after"), "{body}");
    let log = read(&orch, &format!("{uri}/log?verb=diag")).await;
    assert!(
        log.contains("diag · error") && log.contains("Page failed at index.js:4"),
        "{log}"
    );
}

/// A rendered read never writes: a page that writes on mount renders as a
/// failure naming the refused write, the table stays empty, and the log has
/// the refused write with the reading agent as its actor.
#[tokio::test]
async fn a_headless_render_of_a_page_that_writes_fails_with_the_write_refused() {
    if !renders_here("a_headless_render_of_a_page_that_writes_fails_with_the_write_refused") {
        return;
    }
    let (orch, _) = seeded_for_writes().await;
    let source = r#"import { html, Page, Text, insert, useEffect, useState } from "@cairn/page-kit";
export default function Recorder() {
  const [saved, setSaved] = useState("not yet");
  useEffect(() => {
    insert("attempts", { problem: "two-sum" }).then(setSaved, () => setSaved("refused, carrying on"));
  }, []);
  return html`<${Page} title="Recorder"><${Text}>${saved}<//><//>`;
}
"#;
    write(
        &orch,
        WRITE_PAGE,
        ChangeMode::Patch,
        json!({ "files": { "index.js": source } }),
    )
    .await
    .unwrap();

    let body = read_as_agent(&orch, WRITE_PAGE).await;
    assert!(
        body.contains(
            "Page failed: a headless render cannot write (write to cairn:~/db/attempts refused)"
        ),
        "{body}"
    );
    assert!(!body.contains("carrying on"), "{body}");
    assert_eq!(attempt_count(&orch).await, 0);
    let log = read(&orch, &format!("{WRITE_PAGE}/log?verb=write")).await;
    assert!(log.contains("write · refused"), "{log}");
    assert!(
        log.contains("viewer: agent cairn://p/prj/7/1/builder"),
        "{log}"
    );
}

/// Only the invoke handler's path builds a page-write origin: the headless
/// renderer, the read path, and every other module have no way to make a
/// request that writes as the operator: the one builder is `row_writes`, which
/// only the page and database-browser doors call.
#[test]
fn only_row_writes_builds_a_row_write_origin() {
    fn visit(dir: &std::path::Path, found: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, found);
            } else if path.extension().is_some_and(|ext| ext == "rs")
                && std::fs::read_to_string(&path)
                    .unwrap()
                    .contains("DispatchOrigin::RowWrite(")
            {
                found.push(path.display().to_string());
            }
        }
    }
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut found = Vec::new();
    visit(&src, &mut found);
    found.sort();
    let expected: Vec<String> = ["pages/tests.rs", "row_writes.rs"]
        .iter()
        .map(|file| src.join(file).display().to_string())
        .collect();
    assert_eq!(found, expected);
}

/// Answers every read except those naming `withheld`, which never answer.
struct Withholding {
    withheld: &'static str,
}

impl PageCallAnswerer for Withholding {
    fn answer<'a>(
        &'a self,
        orch: &'a Orchestrator,
        request: PageReadRequest,
        viewer: &'a Value,
    ) -> futures_util::future::BoxFuture<'a, Result<Value, PageCallError>> {
        Box::pin(async move {
            if request
                .paths
                .iter()
                .any(|path| path.contains(self.withheld))
            {
                std::future::pending::<()>().await;
            }
            page_read(orch, &request, viewer).await
        })
    }
}

async fn render_page(
    orch: &Orchestrator,
    uri: &str,
    settle: std::time::Duration,
    answerer: &dyn PageCallAnswerer,
) -> super::headless::RenderedPage {
    let address = PageAddress::parse(uri).unwrap();
    let (_, row) = super::store::load_page(&orch.db.local, &address, None)
        .await
        .unwrap()
        .unwrap();
    render(
        orch,
        RenderRequest {
            address: &address,
            project_id: "p",
            row: &row,
            reader: PageReader::Operator,
        },
        RenderLimits { settle },
        answerer,
    )
    .await
}

#[tokio::test]
async fn an_unsettled_read_renders_as_still_reading_at_the_deadline() {
    if !renders_here("an_unsettled_read_renders_as_still_reading_at_the_deadline") {
        return;
    }
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    let source = r#"import { html, Page, Section, Text, useRows } from "@cairn/page-kit";
function Issues() {
  const issues = useRows("issues", {}, { store: "app" });
  return html`<${Text}>${issues.loading ? "loading issues" : String(issues.rows.length)}<//>`;
}
export default function Board() {
  const threads = useRows("threads", {}, { store: "app" });
  return html`<${Page} title="Board">
    <${Section} title="Threads"><${Text}>${threads.loading ? "loading" : "threads read"}<//><//>
    <${Section} title="Issues"><${Issues} /><//>
  <//>`;
}
"#;
    write_page_source(
        &orch,
        uri,
        ChangeMode::Create,
        "Board",
        json!({ "index.js": source }),
    )
    .await;
    let started = std::time::Instant::now();
    let rendered = render_page(
        &orch,
        uri,
        std::time::Duration::from_millis(500),
        &Withholding {
            withheld: "cairn://db/issues",
        },
    )
    .await;
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert!(
        matches!(rendered.outcome, RenderOutcome::Deadline { .. }),
        "{:?}",
        rendered.outcome
    );
    let markdown = &rendered.markdown;
    let issues_at = markdown.find("## Issues").expect(markdown);
    assert!(
        markdown[issues_at..].contains("_still reading: `cairn://db/issues?"),
        "{markdown}"
    );
    assert!(markdown.contains("threads read"), "{markdown}");
}

#[tokio::test]
async fn a_page_that_loops_is_stopped_at_the_hard_bound() {
    if !renders_here("a_page_that_loops_is_stopped_at_the_hard_bound") {
        return;
    }
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    write_page_source(
        &orch,
        uri,
        ChangeMode::Create,
        "Board",
        json!({ "index.js": "export default function Spin() { for (;;) {} }\n" }),
    )
    .await;
    let started = std::time::Instant::now();
    let rendered = render_page(
        &orch,
        uri,
        std::time::Duration::from_millis(200),
        &super::headless::PageReadAnswerer,
    )
    .await;
    assert!(
        matches!(&rendered.outcome, RenderOutcome::Stopped { message } if message.contains("(stopped)")),
        "{:?}",
        rendered.outcome
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(15),
        "{:?}",
        started.elapsed()
    );
}

/// A one-shot read of anything but rows (here another page, which would also
/// start a render inside a render) is refused, and the rendered body lists the
/// refusal with the pattern that replaces it.
#[tokio::test]
async fn a_rendered_page_reading_anything_but_rows_lists_the_refusal() {
    if !renders_here("a_rendered_page_reading_anything_but_rows_lists_the_refusal") {
        return;
    }
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    let source = r#"import { html, Page, Text, cairn, useEffect, useState } from "@cairn/page-kit";
export default function Board() {
  const [said, setSaid] = useState("reading");
  useEffect(() => {
    cairn.read("cairn:~/pages/other").then(() => setSaid("read it"), () => setSaid("refused"));
  }, []);
  return html`<${Page} title="Board"><${Text}>${said}<//><//>`;
}
"#;
    write_page_source(
        &orch,
        uri,
        ChangeMode::Create,
        "Board",
        json!({ "index.js": source }),
    )
    .await;
    let body = read_as_agent(&orch, uri).await;
    assert!(
        body.contains("refused") && !body.contains("read it"),
        "{body}"
    );
    assert!(
        body.contains("### Refused reads") && body.contains("a page reads only database rows"),
        "{body}"
    );
}

/// The cutover: a page written against the retired `useRead` fails at mount
/// with an error that names the row hooks and the service pattern, rather than
/// the engine's bare missing-export message.
#[tokio::test]
async fn a_page_importing_the_retired_use_read_fails_naming_the_row_hooks() {
    if !renders_here("a_page_importing_the_retired_use_read_fails_naming_the_row_hooks") {
        return;
    }
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    let source = r#"import { html, Page, useRead } from "@cairn/page-kit";
export default function Board() {
  const issues = useRead("cairn:@/issues");
  return html`<${Page} title=${String(issues.data)} />`;
}
"#;
    write_page_source(
        &orch,
        uri,
        ChangeMode::Create,
        "Board",
        json!({ "index.js": source }),
    )
    .await;
    let body = read_as_agent(&orch, uri).await;
    assert!(
        body.contains("Page failed to load")
            && body.contains("`useRead` was retired from @cairn/page-kit")
            && body.contains("useRows")
            && body.contains("service"),
        "{body}"
    );
}

/// A page that probes the OS directly, past the host's lockdown: bun's own
/// module reads a file, connects over TCP and over a unix socket, and spawns a
/// process. It renders what each attempt got as one JSON line.
fn os_probe_page(canary: &Path, port: u16, socket: &str, bun: &Path) -> String {
    format!(
        r#"import {{ html, Page, Text, useEffect, useState }} from "@cairn/page-kit";
const B = import.meta.require("bun");
const attempts = {{}};
const refused = (error) => "refused: " + ((error && error.code) || String(error));
function connect(options) {{
  return new Promise((resolve) => {{
    const socket = {{ open(s) {{ s.end(); resolve("REACHED"); }}, data() {{}}, error(_s, error) {{ resolve(refused(error)); }}, connectError(_s, error) {{ resolve(refused(error)); }} }};
    try {{ B.connect({{ ...options, socket }}).catch((error) => resolve(refused(error))); }} catch (error) {{ resolve(refused(error)); }}
  }});
}}
async function probe() {{
  try {{ attempts.read = await B.file({canary:?}).text(); }} catch (error) {{ attempts.read = refused(error); }}
  attempts.tcp = await connect({{ hostname: "127.0.0.1", port: {port} }});
  attempts.unix = await connect({{ unix: {socket:?} }});
  try {{
    const child = B.spawnSync({{ cmd: [{bun:?}, "-e", "0"] }});
    attempts.spawn = child.success ? "REACHED" : "exited " + child.exitCode;
  }} catch (error) {{ attempts.spawn = refused(error); }}
  console.error("probe " + JSON.stringify(attempts));
}}
export default function Probe() {{
  const [done, setDone] = useState(false);
  useEffect(() => {{ probe().then(() => setDone(true)); }}, []);
  return html`<${{Page}} title="Probe"><${{Text}}>${{done ? JSON.stringify(attempts) : "probing"}}<//><//>`;
}}
"#
    )
}

/// The attempts an [`os_probe_page`] rendered, parsed from its JSON line.
fn probe_attempts(markdown: &str) -> Value {
    let line = markdown
        .lines()
        .find(|line| line.trim_start().starts_with("{\"read\""))
        .unwrap_or_else(|| panic!("no probe line in:\n{markdown}"));
    serde_json::from_str(line.trim()).unwrap_or_else(|error| panic!("{error}: {line}"))
}

/// Run the render host with no confinement at all and return what it rendered:
/// the negative control proving a probe CAN reach what the sandbox refuses.
async fn render_unconfined(files: Value) -> String {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let bundle = crate::runtime::page_render_bundle().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let mut child = tokio::process::Command::new(crate::env::resolve_sidecar("bun"))
        .args(["--smol", "--no-install"])
        .arg(bundle.join(crate::runtime::PAGE_RENDER_HOST))
        .env_clear()
        .env("HOME", scratch.path())
        .env("TMPDIR", scratch.path())
        .current_dir(scratch.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let init = json!({
        "page": {
            "uri": "cairn://p/prj/roadmap/pages/board",
            "name": "board",
            "version": 1,
            "entry": "index.js",
            "files": files,
            "home": { "thread": "roadmap", "project": "prj" },
        },
        "settleMs": 5000,
    });
    let mut stdin = child.stdin.take().unwrap();
    stdin
        .write_all(format!("{init}\n").as_bytes())
        .await
        .unwrap();
    let mut lines = tokio::io::BufReader::new(child.stdout.take().unwrap()).lines();
    let rendered = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            match message["type"].as_str() {
                Some("rendered") => {
                    return message["markdown"].as_str().unwrap_or_default().to_string()
                }
                Some("failed") => panic!("the unconfined render failed: {message}"),
                _ => {}
            }
        }
        panic!("the unconfined host exited without rendering");
    })
    .await
    .expect("the unconfined render finishes");
    drop(stdin);
    rendered
}

/// Connections waiting on a listener, accepted and counted without blocking.
fn pending_connections<S>(accept: impl Fn() -> std::io::Result<S>) -> usize {
    let mut count = 0;
    while accept().is_ok() {
        count += 1;
    }
    count
}

/// The confinement, measured from inside a page at the OS: a file read and a
/// spawn fail with an OS error, neither listener receives a connection, and
/// the canary never reaches the body. The same page with no confinement
/// reaches all four, so the test fails if the sandbox does nothing. The
/// connects are judged at the listeners because bun names any failed connect
/// ECONNREFUSED, even when the kernel refused the socket itself.
#[tokio::test]
async fn a_rendered_page_cannot_reach_the_network_or_read_outside_its_allow_list() {
    if !renders_here("a_rendered_page_cannot_reach_the_network_or_read_outside_its_allow_list") {
        return;
    }
    let outside = tempfile::tempdir().unwrap();
    let canary = outside.path().join("canary");
    std::fs::write(&canary, "CANARY-5514-SECRET").unwrap();
    let canary = canary.canonicalize().unwrap();
    let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = tcp.local_addr().unwrap().port();
    // A unix listener the unconfined control can reach. On Linux it is an
    // abstract socket: the worktree fence a check lane runs in refuses making
    // a socket file, and an abstract name needs none.
    #[cfg(target_os = "linux")]
    let (socket, _unix) = {
        use std::os::linux::net::SocketAddrExt;
        let name = format!("cairn-page-probe-{}", std::process::id());
        let address = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes()).unwrap();
        let listener = std::os::unix::net::UnixListener::bind_addr(&address).unwrap();
        (format!("\0{name}"), listener)
    };
    // Elsewhere a short path under /tmp: a socket path is capped near 104
    // bytes, which a check lane's temp directory exceeds.
    #[cfg(not(target_os = "linux"))]
    let short = tempfile::Builder::new()
        .prefix("pg")
        .tempdir_in("/tmp")
        .unwrap();
    #[cfg(not(target_os = "linux"))]
    let (socket, _unix) = {
        let path = short.path().canonicalize().unwrap().join("s.sock");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        (path.to_string_lossy().into_owned(), listener)
    };
    let bun = std::path::PathBuf::from(crate::env::resolve_sidecar("bun"))
        .canonicalize()
        .unwrap();
    let files = json!({ "index.js": os_probe_page(&canary, port, &socket, &bun) });
    tcp.set_nonblocking(true).unwrap();
    _unix.set_nonblocking(true).unwrap();
    let tcp_connections = || pending_connections(|| tcp.accept());
    let unix_connections = || pending_connections(|| _unix.accept());

    let started = std::time::Instant::now();
    let control = probe_attempts(&render_unconfined(files.clone()).await);
    eprintln!("unconfined control rendered in {:?}", started.elapsed());
    assert_eq!(
        control,
        json!({ "read": "CANARY-5514-SECRET", "tcp": "REACHED", "unix": "REACHED", "spawn": "REACHED" }),
        "unconfined, every probe must reach its target, or the confined run proves nothing"
    );
    assert!(
        tcp_connections() >= 1,
        "the control's TCP connect reached the listener"
    );
    assert!(
        unix_connections() >= 1,
        "the control's unix connect reached the listener"
    );

    let started = std::time::Instant::now();
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    write_page_source(&orch, uri, ChangeMode::Create, "Board", files).await;
    eprintln!("seeded in {:?}", started.elapsed());
    let started = std::time::Instant::now();
    let body = read_as_agent(&orch, uri).await;
    eprintln!("confined render in {:?}", started.elapsed());
    assert!(
        !body.contains("CANARY-5514-SECRET"),
        "the canary reached the body:\n{body}"
    );
    let attempts = probe_attempts(&body);
    for probe in ["read", "spawn"] {
        let got = attempts[probe].as_str().unwrap_or_default();
        assert!(
            ["refused: EPERM", "refused: EACCES"].contains(&got),
            "{probe} was not refused by the OS ({got}):\n{body}"
        );
    }
    for probe in ["tcp", "unix"] {
        let got = attempts[probe].as_str().unwrap_or_default();
        assert!(
            got.starts_with("refused: "),
            "{probe} connected ({got}):\n{body}"
        );
    }
    assert_eq!(
        tcp_connections(),
        0,
        "the confined page reached the TCP listener"
    );
    assert_eq!(
        unix_connections(),
        0,
        "the confined page reached the unix listener"
    );
    assert!(
        body.contains("### Diagnostics") && body.contains("probe "),
        "{body}"
    );
}

/// Answers every read, after the page has stopped listening, with a body far
/// larger than a pipe buffer.
struct LateAndLarge;

impl PageCallAnswerer for LateAndLarge {
    fn answer<'a>(
        &'a self,
        _orch: &'a Orchestrator,
        _request: PageReadRequest,
        _viewer: &'a Value,
    ) -> futures_util::future::BoxFuture<'a, Result<Value, PageCallError>> {
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            Ok(json!({ "result": "x".repeat(4 << 20), "reminders": [] }))
        })
    }
}

/// A page that blocks its own main thread without spending CPU never drains
/// its stdin; the render is still stopped at a bound instead of pinning the
/// runner on a write to the child.
#[tokio::test]
async fn a_page_that_stops_reading_its_results_is_stopped_not_waited_on() {
    if !renders_here("a_page_that_stops_reading_its_results_is_stopped_not_waited_on") {
        return;
    }
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    let source = r#"import { html, Page, Text, useEffect, useRows } from "@cairn/page-kit";
export default function Stuck() {
  const reads = Array.from({ length: 12 }, (_, i) => useRows("issues", { offset: i }, { store: "app" }));
  useEffect(() => { setTimeout(() => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0), 100); }, []);
  return html`<${Page} title="Stuck"><${Text}>${reads.filter((read) => read.loading).length}<//><//>`;
}
"#;
    write_page_source(
        &orch,
        uri,
        ChangeMode::Create,
        "Board",
        json!({ "index.js": source }),
    )
    .await;
    let started = std::time::Instant::now();
    let rendered = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        render_page(
            &orch,
            uri,
            std::time::Duration::from_millis(500),
            &LateAndLarge,
        ),
    )
    .await
    .expect("a render whose page stopped reading must not hang");
    assert!(
        matches!(&rendered.outcome, RenderOutcome::Stopped { message } if message.contains("(stopped)")),
        "{:?}",
        rendered.outcome
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(15),
        "{:?}",
        started.elapsed()
    );
}

/// A reader whose grants cannot be represented sees the refusal in the body,
/// never the owner's content.
#[tokio::test]
async fn an_unrepresentable_reader_sees_refusals_not_the_owners_content() {
    if !renders_here("an_unrepresentable_reader_sees_refusals_not_the_owners_content") {
        return;
    }
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    write_page_source(
        &orch,
        uri,
        ChangeMode::Create,
        "Board",
        json!({ "index.js": BOARD }),
    )
    .await;
    let body = crate::resources::read_cairn_resource(
        &orch,
        &McpCallbackRequest {
            run_id: Some("no-such-run".into()),
            ..request()
        },
        uri,
    )
    .await;
    assert!(
        !body.contains("Draft work"),
        "owner-authority content reached the reader:\n{body}"
    );
    assert!(body.contains("### Refused reads"), "{body}");
    assert!(body.contains("does not resolve to an agent"), "{body}");
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
#[tokio::test]
async fn a_host_without_confinement_refuses_rather_than_runs_unconfined() {
    let (orch, _) = seeded().await;
    let uri = "cairn://p/prj/roadmap/pages/board";
    write_page_source(
        &orch,
        uri,
        ChangeMode::Create,
        "Board",
        json!({ "index.js": BOARD }),
    )
    .await;
    let body = read_as_agent(&orch, uri).await;
    assert!(
        body.contains("Rendered reads need kernel confinement"),
        "{body}"
    );
    assert!(body.contains("not rendered"), "{body}");
}

/// cairn/5524, through the resource surface: a thread writes a service and
/// the write deploys it; a page on the same thread declares it by address and
/// renders its reply; another thread neither calls nor writes it; a patch of
/// `main.ts` is generation 2; and reading the service says whether it runs
/// and what it last said.
#[tokio::test]
async fn a_thread_writes_a_service_its_own_page_calls_and_a_patch_redeploys() {
    let (orch, _) = seeded().await;
    let (host, _) = crate::project_services::supervisor::tests::install_fakes(&orch);
    let service = "cairn://p/prj/roadmap/services/tutor";
    let written = write(
        &orch,
        service,
        ChangeMode::Create,
        json!({"title": "Tutor", "files": {"main.ts": "serve({methods: {echo: (p) => p}})"}}),
    )
    .await
    .unwrap();
    assert!(written.contains("version 1"), "{written}");
    assert!(
        written.contains("Version 1 is serving as generation 1"),
        "{written}"
    );
    let status = read(&orch, &format!("{service}/status")).await;
    assert!(status.contains("- State: running"), "{status}");
    assert!(status.contains("- Serving version: 1"), "{status}");

    let page_uri = "cairn://p/prj/roadmap/pages/lesson";
    write(
        &orch,
        page_uri,
        ChangeMode::Create,
        json!({"title": "Lesson", "files": index_js(), "manifest": {"services": [service]}}),
    )
    .await
    .unwrap();
    let viewer = json!({"kind": "desktop_operator"});
    let call = |page: &str| PageServiceRequest {
        project_id: "p".into(),
        page: page.into(),
        version: 1,
        service: service.into(),
        method: "echo".into(),
        params: json!({"hola": 1}),
    };
    let answered = page_service(&orch, &call(page_uri), &viewer).await.unwrap();
    assert_eq!(answered["ok"], true, "{answered}");
    assert_eq!(answered["value"]["params"], json!({"hola": 1}));
    assert_eq!(answered["value"]["generation"], 1);
    assert_eq!(
        host.frames_of("call")[0]["caller"]["actingFor"],
        "cairn://p/prj/roadmap"
    );

    // Another thread's page may name it, but the service answers its own
    // thread only; and only its own thread writes it.
    let foreign = "cairn://p/prj/devops/pages/peek";
    write(
        &orch,
        foreign,
        ChangeMode::Create,
        json!({"title": "Peek", "files": index_js(), "manifest": {"services": [service]}}),
    )
    .await
    .unwrap();
    let refused = page_service(&orch, &call(foreign), &viewer).await.unwrap();
    assert_eq!(refused["error"]["code"], "not_owner", "{refused}");
    let stolen = write_as(
        &orch,
        "devops-run",
        service,
        json!({"files": {"main.ts": "serve({})"}}),
    )
    .await
    .unwrap_err();
    assert!(stolen.contains("not this run's to write"), "{stolen}");

    let patched = write(
        &orch,
        service,
        ChangeMode::Patch,
        json!({"files": {"main.ts": "serve({methods: {echo: (p) => ({p})}})"}}),
    )
    .await
    .unwrap();
    assert!(
        patched.contains("Version 2 is serving as generation 2"),
        "{patched}"
    );
    let answered = page_service(&orch, &call(page_uri), &viewer).await.unwrap();
    assert_eq!(answered["value"]["generation"], 2, "{answered}");

    let body = read(&orch, service).await;
    assert!(body.contains("- Version: 2"), "{body}");
    assert!(body.contains("- Serving version: 2"), "{body}");
    assert!(body.contains("## Last said"), "{body}");
    assert!(body.contains("started generation 2"), "{body}");
    let index = read(&orch, "cairn://p/prj/roadmap/services").await;
    assert!(index.contains("[tutor]("), "{index}");
    let json_body = read(&orch, &format!("{service}?format=json")).await;
    assert!(json_body.trim_start().starts_with('{'), "{json_body}");
}

#[test]
fn manifest_services_name_project_ids_or_thread_service_uris() {
    let with = |services: Value| json!({"title": "Board", "files": index_js(), "manifest": {"services": services}});
    assert!(validate(&with(json!([
        "echo",
        "cairn://p/prj/roadmap/services/tutor"
    ])))
    .is_ok());
    for bad in [
        json!(["cairn://p/prj/roadmap/pages/tutor"]),
        json!(["cairn://p/prj/services/echo"]),
        json!(["cairn://p/prj/roadmap/services/Tutor"]),
        json!([
            "cairn://p/prj/roadmap/services/tutor",
            "cairn://p/PRJ/roadmap/services/tutor"
        ]),
    ] {
        assert!(validate(&with(bad.clone())).is_err(), "{bad}");
    }
}
