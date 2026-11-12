//! The camp resources' write path, as an agent's `write` reaches it: an
//! authenticated thread run creates a camp, adds members, renames, archives
//! and leaves, and posts to it.

use super::*;
use crate::db::DbState;
use crate::models::CreateThread;
use crate::orchestrator::OrchestratorBuilder;
use crate::services::testing::{CapturingEmitter, TestServicesBuilder};
use crate::storage::{LocalDb, SearchIndex};
use std::sync::Arc;

async fn orch_with_project() -> (Orchestrator, Arc<CapturingEmitter>) {
    let local = LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("t.db"))
        .await
        .unwrap();
    local
        .execute_script(
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES ('p','w','P','prj','/tmp/p',1,1);",
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
    (orch, emitter)
}

/// A thread with its session and a live run on it, as an agent holds one.
async fn thread_run(orch: &Orchestrator, name: &str) -> (String, String) {
    let thread_id = crate::threads::crud::create(
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
    .unwrap()
    .id;
    let session = crate::threads::ensure_thread_session(&orch.db.local, &thread_id)
        .await
        .unwrap();
    let run_id = format!("run-{name}");
    orch.db
        .local
        .execute(
            "INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at)
             VALUES (?1, 'p', ?2, 'live', 1, 1)",
            (run_id.clone(), session),
        )
        .await
        .unwrap();
    (thread_id, run_id)
}

async fn act(
    orch: &Orchestrator,
    run_id: Option<&str>,
    target: &str,
    mode: ChangeMode,
    payload: Option<serde_json::Value>,
) -> Result<String, String> {
    let request = McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: run_id.map(str::to_string),
        tool: "change".to_string(),
        payload: serde_json::json!({}),
        tool_use_id: None,
        origin: None,
    };
    let item = ChangeItem {
        target: target.into(),
        mode,
        payload,
    };
    dispatch_resource_change(orch, &request, 0, &item, false)
        .await
        .map(|change| change.summary)
        .map_err(|failure| failure.error)
}

async fn post(orch: &Orchestrator, run: &str, slug: &str, content: &str) -> Result<String, String> {
    act(
        orch,
        Some(run),
        &format!("cairn://p/prj/camps/{slug}/posts"),
        ChangeMode::Append,
        Some(serde_json::json!({ "content": content })),
    )
    .await
}

/// Waking receipts per thread for the camp's posts.
async fn woken(orch: &Orchestrator) -> std::collections::BTreeMap<String, i64> {
    orch.db
        .local
        .query_all(
            "SELECT recipient_id, COUNT(*) FROM delivery_receipts
             WHERE recipient_kind = 'thread' AND level = 'wake' GROUP BY recipient_id",
            (),
            |row| {
                Ok((
                    crate::storage::RowExt::text(row, 0)?,
                    crate::storage::RowExt::i64(row, 1)?,
                ))
            },
        )
        .await
        .unwrap()
        .into_iter()
        .collect()
}

async fn operator_unread(orch: &Orchestrator, slug: &str) -> i64 {
    crate::camps::view::list_summaries(&orch.db.local, Some(&orch.db.local), "p", "prj")
        .await
        .unwrap()
        .into_iter()
        .find(|camp| camp.slug == slug)
        .unwrap()
        .unread
}

/// A thread creates a camp with two threads and the operator; a post by any
/// member wakes every other thread member once, with the camp named in the
/// line, and the operator sees it as unread until the camp is marked read.
#[tokio::test]
async fn a_post_to_a_camp_wakes_every_other_member_once() {
    let (orch, _) = orch_with_project().await;
    let (a, a_run) = thread_run(&orch, "alpha").await;
    let (b, b_run) = thread_run(&orch, "beta").await;
    let (c, _) = thread_run(&orch, "gamma").await;
    let created = act(
        &orch,
        Some(&a_run),
        "cairn://p/prj/camps",
        ChangeMode::Append,
        Some(serde_json::json!({
            "slug": "x",
            "title": "The x effort",
            "members": ["cairn://p/prj/beta", "cairn://p/prj/gamma", "operator"],
        })),
    )
    .await
    .unwrap();
    assert_eq!(
        created,
        "Created camp cairn://p/prj/camps/x with 4 members."
    );

    post(&orch, &b_run, "x", "The interface is agreed.")
        .await
        .unwrap();
    crate::routes::drain_fact_log(&orch).await;
    let woken = woken(&orch).await;
    assert_eq!(woken.get(&a), Some(&1));
    assert_eq!(woken.get(&c), Some(&1));
    assert_eq!(
        woken.get(&b),
        None,
        "the poster is not woken by its own post"
    );
    let lines = orch
        .db
        .local
        .query_opt_i64(
            "SELECT COUNT(*) FROM messages WHERE content LIKE 'From camp x: %'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(lines, Some(2), "each woken member reads the camp's line");

    assert_eq!(operator_unread(&orch, "x").await, 1);
    let camp = orch.db.local.camp_by_slug("p", "x").await.unwrap().unwrap();
    orch.db
        .local
        .advance_camp_read_cursor("operator", &camp.id, i64::MAX, 10)
        .await
        .unwrap();
    assert_eq!(operator_unread(&orch, "x").await, 0);
}

/// Posts by two members inside one coalescing window wake a third member
/// once: the fact log folds a recipient's matched wakes per window, which is
/// what bounds a camp's reply cascade.
#[tokio::test]
async fn a_burst_of_posts_wakes_another_member_once() {
    let (orch, _) = orch_with_project().await;
    let (_, a_run) = thread_run(&orch, "alpha").await;
    let (_, b_run) = thread_run(&orch, "beta").await;
    let (c, _) = thread_run(&orch, "gamma").await;
    act(
        &orch,
        Some(&a_run),
        "cairn://p/prj/camps",
        ChangeMode::Append,
        Some(serde_json::json!({
            "slug": "x",
            "title": "X",
            "members": ["cairn://p/prj/beta", "cairn://p/prj/gamma"],
        })),
    )
    .await
    .unwrap();
    post(&orch, &a_run, "x", "First.").await.unwrap();
    post(&orch, &b_run, "x", "Second.").await.unwrap();
    crate::routes::drain_fact_log(&orch).await;
    let wakes = orch
        .db
        .local
        .query_opt_i64(
            "SELECT COUNT(*) FROM fact_log_wakes WHERE recipient_key = ?1",
            (format!("thread:{c}"),),
        )
        .await
        .unwrap();
    assert_eq!(wakes, Some(1), "one wake carries both posts");
}

/// Any member renames and archives a camp; an archived camp takes no posts
/// and delivers nothing, even for a post written before it was archived.
#[tokio::test]
async fn any_member_renames_and_archives_and_an_archived_camp_is_silent() {
    let (orch, _) = orch_with_project().await;
    let (_, a_run) = thread_run(&orch, "alpha").await;
    let (_, b_run) = thread_run(&orch, "beta").await;
    act(
        &orch,
        Some(&a_run),
        "cairn://p/prj/camps",
        ChangeMode::Append,
        Some(serde_json::json!({"slug": "x", "title": "X", "members": ["cairn://p/prj/beta"]})),
    )
    .await
    .unwrap();
    let renamed = act(
        &orch,
        Some(&b_run),
        "cairn://p/prj/camps/x",
        ChangeMode::Patch,
        Some(serde_json::json!({"title": "X, phase two"})),
    )
    .await
    .unwrap();
    assert_eq!(renamed, "Renamed camp cairn://p/prj/camps/x.");

    // Written before the archive, indexed after it.
    post(&orch, &a_run, "x", "Late news.").await.unwrap();
    let archived = act(
        &orch,
        Some(&b_run),
        "cairn://p/prj/camps/x",
        ChangeMode::Patch,
        Some(serde_json::json!({"status": "archived"})),
    )
    .await
    .unwrap();
    assert_eq!(archived, "Archived camp cairn://p/prj/camps/x.");
    crate::routes::drain_fact_log(&orch).await;
    assert!(
        woken(&orch).await.is_empty(),
        "an archived camp wakes no one"
    );

    let refused = post(&orch, &a_run, "x", "Anyone there?").await.unwrap_err();
    assert!(refused.contains("is archived"), "{refused}");
    let camp = orch.db.local.camp_by_slug("p", "x").await.unwrap().unwrap();
    assert_eq!(camp.title, "X, phase two");
}

/// A thread that is not a member cannot post to a camp or add others to it,
/// but may add itself; a member that closes leaves the member list with no
/// guard in the way.
#[tokio::test]
async fn a_non_member_is_refused_until_it_adds_itself() {
    let (orch, _) = orch_with_project().await;
    let (_, a_run) = thread_run(&orch, "alpha").await;
    let (d, d_run) = thread_run(&orch, "delta").await;
    thread_run(&orch, "epsilon").await;
    act(
        &orch,
        Some(&a_run),
        "cairn://p/prj/camps",
        ChangeMode::Append,
        Some(serde_json::json!({"slug": "x", "title": "X"})),
    )
    .await
    .unwrap();
    let refused = post(&orch, &d_run, "x", "Hello?").await.unwrap_err();
    assert!(refused.contains("is not a member of camp `x`"), "{refused}");
    assert!(
        refused.contains("cairn://p/prj/camps/x/members"),
        "{refused}"
    );
    let add_other = act(
        &orch,
        Some(&d_run),
        "cairn://p/prj/camps/x/members",
        ChangeMode::Append,
        Some(serde_json::json!({"member": "cairn://p/prj/epsilon"})),
    )
    .await
    .unwrap_err();
    assert!(add_other.contains("is not a member"), "{add_other}");
    let joined = act(
        &orch,
        Some(&d_run),
        "cairn://p/prj/camps/x/members",
        ChangeMode::Append,
        Some(serde_json::json!({"member": "cairn://p/prj/delta"})),
    )
    .await
    .unwrap();
    assert_eq!(
        joined,
        "Added cairn://p/prj/delta to camp cairn://p/prj/camps/x."
    );
    post(&orch, &d_run, "x", "Hello.").await.unwrap();

    crate::threads::crud::update(
        &orch.db.local,
        crate::models::UpdateThread {
            id: d.clone(),
            name: None,
            jurisdiction: None,
            definition: None,
            status: Some(crate::models::ThreadStatus::Closed),
            model: None,
        },
    )
    .await
    .expect("no camp holds a thread open");
    let camp = orch.db.local.camp_by_slug("p", "x").await.unwrap().unwrap();
    assert!(orch
        .db
        .local
        .camp_members(&camp)
        .await
        .unwrap()
        .iter()
        .all(|member| member.subject_key != d));
}

/// Base camp is every open thread's: it is never left, renamed or archived.
#[tokio::test]
async fn base_camp_is_not_left_renamed_or_archived() {
    let (orch, _) = orch_with_project().await;
    let (_, a_run) = thread_run(&orch, "alpha").await;
    crate::camps::ensure_project_base_camp(&orch.db.local, "p")
        .await
        .unwrap();
    for (target, mode, payload) in [
        ("cairn://p/prj/camps/base/members", ChangeMode::Delete, None),
        (
            "cairn://p/prj/camps/base",
            ChangeMode::Patch,
            Some(serde_json::json!({"title": "Elsewhere"})),
        ),
        (
            "cairn://p/prj/camps/base",
            ChangeMode::Patch,
            Some(serde_json::json!({"status": "archived"})),
        ),
    ] {
        let refused = act(&orch, Some(&a_run), target, mode, payload)
            .await
            .unwrap_err();
        assert!(refused.contains("base camp"), "{target}: {refused}");
    }
    post(&orch, &a_run, "base", "Open to every thread.")
        .await
        .unwrap();
}

/// No camp surface speaks the vocabulary cairn/7002 removed: not the camp
/// contracts' descriptions, labels or key notes, not the camp reads, and not
/// a seed's Camps section.
#[tokio::test]
async fn no_camp_surface_uses_the_removed_vocabulary() {
    const REMOVED: &[&str] = &[
        "pitch",
        "lead",
        "level",
        "reason",
        "description",
        "confirm",
        "strike",
        "struck",
        "roadmap",
    ];
    fn offending(text: &str) -> Vec<String> {
        let lower = text.to_lowercase();
        let mut found: Vec<String> = lower
            .split(|c: char| !c.is_alphanumeric())
            .filter(|word| REMOVED.iter().any(|stem| word.starts_with(stem)))
            .map(str::to_string)
            .collect();
        if lower.contains("joined issue") {
            found.push("joined issue".to_string());
        }
        found
    }

    let (orch, _) = orch_with_project().await;
    let (a, a_run) = thread_run(&orch, "alpha").await;
    thread_run(&orch, "beta").await;
    crate::camps::ensure_project_base_camp(&orch.db.local, "p")
        .await
        .unwrap();
    act(
        &orch,
        Some(&a_run),
        "cairn://p/prj/camps",
        ChangeMode::Append,
        Some(serde_json::json!({
            "slug": "x",
            "title": "The x effort",
            "members": ["cairn://p/prj/beta", "operator"],
        })),
    )
    .await
    .unwrap();
    post(&orch, &a_run, "x", "The interface is agreed.")
        .await
        .unwrap();
    crate::routes::drain_fact_log(&orch).await;

    let mut surfaces: Vec<(String, String)> = Vec::new();
    for kind in [
        cairn_uri::ResourceKind::ProjectCamps,
        cairn_uri::ResourceKind::ProjectCamp,
        cairn_uri::ResourceKind::ProjectCampMembers,
        cairn_uri::ResourceKind::ProjectCampPosts,
    ] {
        let contract = cairn_common::contract::contract_for(kind).unwrap();
        surfaces.push((format!("{kind:?} description"), contract.description.into()));
        for mutation in contract.mutations {
            surfaces.push((format!("{kind:?} label"), mutation.label.into()));
            for key in mutation.required.iter().chain(mutation.optional) {
                surfaces.push((format!("{kind:?} {}", key.key), key.note().into()));
            }
        }
    }
    let db = &orch.db.local;
    surfaces.push((
        "/camps".into(),
        crate::resources::camps::read_camps(&orch, db, "prj", &[]).await,
    ));
    for slug in ["base", "x"] {
        surfaces.push((
            format!("/camps/{slug}"),
            crate::resources::camps::read_camp(&orch, db, "prj", slug, &[]).await,
        ));
        surfaces.push((
            format!("/camps/{slug}/members"),
            crate::resources::camps::read_members(&orch, db, "prj", slug, &[]).await,
        ));
    }
    surfaces.push((
        "seed".into(),
        crate::resources::thread_seed::camp_lines(db, Some(&a), "prj").await,
    ));
    assert!(
        surfaces
            .iter()
            .any(|(_, text)| text.contains("From camp x") || text.contains("The x effort")),
        "the surfaces rendered: {surfaces:#?}"
    );
    for (surface, text) in &surfaces {
        let found = offending(text);
        assert!(found.is_empty(), "{surface} says {found:?}:\n{text}");
    }
}

/// A quiet camp keeps its notices however many a busy camp mints after them,
/// and each minted notice announces the camps projection so an open page
/// refreshes.
#[tokio::test]
async fn a_quiet_camps_notices_survive_a_busy_camp_and_each_notice_is_announced() {
    let (orch, emitter) = orch_with_project().await;
    let (_, a_run) = thread_run(&orch, "alpha").await;
    for slug in ["quiet", "busy"] {
        act(
            &orch,
            Some(&a_run),
            "cairn://p/prj/camps",
            ChangeMode::Append,
            Some(serde_json::json!({"slug": slug, "title": slug})),
        )
        .await
        .unwrap();
    }
    let notice = |slug: &'static str, n: usize| crate::camps::CampNotice {
        project_id: "p",
        project_key: "prj",
        camp_slug: slug,
        kind: "rebuild",
        identity: format!("{slug}:{n}"),
        summary: format!("{slug} notice {n}"),
        detail_uri: None,
        actor_job_id: None,
    };
    let before = emitter.events_named("db-change").len();
    crate::camps::notify_camp(&orch, notice("quiet", 0))
        .await
        .unwrap()
        .unwrap();
    assert!(
        emitter.events_named("db-change")[before..]
            .iter()
            .any(|event| event["table"] == "camps"),
        "a new notice announces the camps projection"
    );
    for n in 0..210 {
        crate::camps::notify_camp(&orch, notice("busy", n))
            .await
            .unwrap();
    }
    let view = |slug: &'static str| {
        let orch = &orch;
        async move {
            crate::camps::view::camp_view(&orch.db.local, Some(&orch.db.local), "p", "prj", slug)
                .await
                .unwrap()
                .unwrap()
        }
    };
    let quiet = view("quiet").await;
    assert_eq!(
        quiet
            .notices
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>(),
        ["quiet notice 0"]
    );
    assert_eq!(
        view("busy").await.notices.len(),
        crate::camps::view::NOTICE_LIMIT
    );
}
