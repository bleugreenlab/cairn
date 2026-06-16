//! Tests for server CRUD queries and project sync.

mod common;

use cairn_core::internal::services::RealClock;
use cairn_core::models::CreateProject;
use cairn_core::projects::crud as project_crud;
use cairn_core::remote_servers::{models::Server, queries, sync};

fn test_server(id: &str, name: &str) -> Server {
    let now = chrono::Utc::now().timestamp();
    Server {
        id: id.to_string(),
        name: name.to_string(),
        url: format!("http://{}.example.com:8080", name.to_lowercase()),
        org_id: None,
        status: "unknown".to_string(),
        version: None,
        error_message: None,
        excluded_project_ids: Vec::new(),
        last_seen_at: None,
        created_at: now,
        updated_at: now,
    }
}

#[tokio::test]
async fn create_and_get_server() {
    let (_temp, db) = common::migrated_db().await;
    let server = test_server("srv-1", "Prod");

    queries::create(&db, &server).await.unwrap();

    let fetched = queries::get(&db, "srv-1")
        .await
        .unwrap()
        .expect("server should exist");
    assert_eq!(fetched.name, "Prod");
    assert_eq!(fetched.status, "unknown");
}

#[tokio::test]
async fn get_nonexistent_returns_none() {
    let (_temp, db) = common::migrated_db().await;
    let result = queries::get(&db, "nope").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn list_servers_ordered_by_created_at_desc() {
    let (_temp, db) = common::migrated_db().await;

    let mut older = test_server("srv-old", "Older");
    older.created_at -= 100;
    let newer = test_server("srv-new", "Newer");

    queries::create(&db, &older).await.unwrap();
    queries::create(&db, &newer).await.unwrap();

    let servers = queries::list(&db).await.unwrap();
    assert_eq!(servers.len(), 2);
    assert_eq!(servers[0].name, "Newer");
    assert_eq!(servers[1].name, "Older");
}

#[tokio::test]
async fn update_status_changes_status_and_error_message() {
    let (_temp, db) = common::migrated_db().await;
    let server = test_server("srv-1", "Test");
    queries::create(&db, &server).await.unwrap();

    queries::update_status(&db, "srv-1", "error", Some("timeout"))
        .await
        .unwrap();

    let fetched = queries::get(&db, "srv-1").await.unwrap().unwrap();
    assert_eq!(fetched.status, "error");
    assert_eq!(fetched.error_message.as_deref(), Some("timeout"));
}

#[tokio::test]
async fn update_health_sets_version_and_last_seen() {
    let (_temp, db) = common::migrated_db().await;
    let server = test_server("srv-1", "Test");
    queries::create(&db, &server).await.unwrap();

    queries::update_health(&db, "srv-1", "connected", Some("1.2.3"), None)
        .await
        .unwrap();

    let fetched = queries::get(&db, "srv-1").await.unwrap().unwrap();
    assert_eq!(fetched.status, "connected");
    assert_eq!(fetched.version.as_deref(), Some("1.2.3"));
    assert!(fetched.last_seen_at.is_some());
    assert!(fetched.error_message.is_none());
}

#[tokio::test]
async fn update_name_only() {
    let (_temp, db) = common::migrated_db().await;
    let server = test_server("srv-1", "Old Name");
    queries::create(&db, &server).await.unwrap();

    queries::update(&db, "srv-1", Some("New Name"), None)
        .await
        .unwrap();

    let fetched = queries::get(&db, "srv-1").await.unwrap().unwrap();
    assert_eq!(fetched.name, "New Name");
}

#[tokio::test]
async fn update_excluded_project_ids() {
    let (_temp, db) = common::migrated_db().await;
    let server = test_server("srv-1", "Test");
    queries::create(&db, &server).await.unwrap();

    queries::update(&db, "srv-1", None, Some(r#"["proj-1","proj-2"]"#))
        .await
        .unwrap();

    let fetched = queries::get(&db, "srv-1").await.unwrap().unwrap();
    assert_eq!(fetched.excluded_project_ids, vec!["proj-1", "proj-2"]);
}

#[tokio::test]
async fn delete_server() {
    let (_temp, db) = common::migrated_db().await;
    let server = test_server("srv-1", "Test");
    queries::create(&db, &server).await.unwrap();

    queries::delete(&db, "srv-1").await.unwrap();

    let fetched = queries::get(&db, "srv-1").await.unwrap();
    assert!(fetched.is_none());
}

#[tokio::test]
async fn list_by_org_filters_by_org_id() {
    let (_temp, db) = common::migrated_db().await;

    let mut srv_org1 = test_server("srv-org1", "Org1 Server");
    srv_org1.org_id = Some("org-aaa".to_string());

    let mut srv_org2 = test_server("srv-org2", "Org2 Server");
    srv_org2.org_id = Some("org-bbb".to_string());

    let srv_no_org = test_server("srv-none", "No Org");

    queries::create(&db, &srv_org1).await.unwrap();
    queries::create(&db, &srv_org2).await.unwrap();
    queries::create(&db, &srv_no_org).await.unwrap();

    let org1_servers = queries::list_by_org(&db, "org-aaa").await.unwrap();
    assert_eq!(org1_servers.len(), 1);
    assert_eq!(org1_servers[0].name, "Org1 Server");
    assert_eq!(org1_servers[0].org_id.as_deref(), Some("org-aaa"));

    let org2_servers = queries::list_by_org(&db, "org-bbb").await.unwrap();
    assert_eq!(org2_servers.len(), 1);
    assert_eq!(org2_servers[0].name, "Org2 Server");

    let empty = queries::list_by_org(&db, "org-nonexistent").await.unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn delete_server_projects_removes_linked_projects() {
    let (_temp, db) = common::migrated_db().await;
    let clock = RealClock;

    let server = test_server("srv-1", "Test");
    queries::create(&db, &server).await.unwrap();

    project_crud::create_db(
        &db,
        &clock,
        &CreateProject {
            id: Some("p1".to_string()),
            name: "Project 1".to_string(),
            key: "P1".to_string(),
            repo_path: String::new(),
            remote_url: Some("http://example.com".to_string()),
            server_id: Some("srv-1".to_string()),
        },
    )
    .await
    .unwrap();

    project_crud::create_db(
        &db,
        &clock,
        &CreateProject {
            id: Some("p2".to_string()),
            name: "Project 2".to_string(),
            key: "P2".to_string(),
            repo_path: String::new(),
            remote_url: Some("http://example.com".to_string()),
            server_id: Some("srv-1".to_string()),
        },
    )
    .await
    .unwrap();

    project_crud::create_db(
        &db,
        &clock,
        &CreateProject {
            id: Some("p3".to_string()),
            name: "Local".to_string(),
            key: "LOC".to_string(),
            repo_path: "/tmp/local".to_string(),
            remote_url: None,
            server_id: None,
        },
    )
    .await
    .unwrap();

    queries::delete_server_projects(&db, "srv-1").await.unwrap();

    let projects = project_crud::list_db(&db).await.unwrap();
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].name, "Local");
}

#[tokio::test]
async fn sync_creates_new_project_bookmarks() {
    let (_temp, db) = common::migrated_db().await;
    let clock = RealClock;

    let server = test_server("srv-1", "Test");
    queries::create(&db, &server).await.unwrap();

    let remote_projects = vec![
        sync::RemoteProject {
            id: "rp-1".to_string(),
            name: "Remote Alpha".to_string(),
            key: "RA".to_string(),
        },
        sync::RemoteProject {
            id: "rp-2".to_string(),
            name: "Remote Beta".to_string(),
            key: "RB".to_string(),
        },
    ];

    let (created, removed) = sync::sync_projects(
        &db,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote_projects,
        &[],
    )
    .await
    .unwrap();

    assert_eq!(created, 2);
    assert_eq!(removed, 0);

    let projects = project_crud::list_db(&db).await.unwrap();
    assert_eq!(projects.len(), 2);
}

#[tokio::test]
async fn sync_respects_excluded_project_ids() {
    let (_temp, db) = common::migrated_db().await;
    let clock = RealClock;

    let server = test_server("srv-1", "Test");
    queries::create(&db, &server).await.unwrap();

    let remote_projects = vec![
        sync::RemoteProject {
            id: "rp-1".to_string(),
            name: "Included".to_string(),
            key: "INC".to_string(),
        },
        sync::RemoteProject {
            id: "rp-2".to_string(),
            name: "Excluded".to_string(),
            key: "EXC".to_string(),
        },
    ];

    let excluded = vec!["rp-2".to_string()];
    let (created, removed) = sync::sync_projects(
        &db,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote_projects,
        &excluded,
    )
    .await
    .unwrap();

    assert_eq!(created, 1);
    assert_eq!(removed, 0);

    let projects = project_crud::list_db(&db).await.unwrap();
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].name, "Included");
}

#[tokio::test]
async fn sync_removes_stale_bookmarks() {
    let (_temp, db) = common::migrated_db().await;
    let clock = RealClock;

    let server = test_server("srv-1", "Test");
    queries::create(&db, &server).await.unwrap();

    let remote_v1 = vec![
        sync::RemoteProject {
            id: "rp-1".to_string(),
            name: "Alpha".to_string(),
            key: "A".to_string(),
        },
        sync::RemoteProject {
            id: "rp-2".to_string(),
            name: "Beta".to_string(),
            key: "B".to_string(),
        },
    ];

    sync::sync_projects(
        &db,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote_v1,
        &[],
    )
    .await
    .unwrap();

    let remote_v2 = vec![sync::RemoteProject {
        id: "rp-1".to_string(),
        name: "Alpha".to_string(),
        key: "A".to_string(),
    }];

    let (created, removed) = sync::sync_projects(
        &db,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote_v2,
        &[],
    )
    .await
    .unwrap();

    assert_eq!(created, 0);
    assert_eq!(removed, 1);

    let projects = project_crud::list_db(&db).await.unwrap();
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].id, "rp-1");
}

#[tokio::test]
async fn sync_updates_project_name_when_changed() {
    let (_temp, db) = common::migrated_db().await;
    let clock = RealClock;

    let server = test_server("srv-1", "Test");
    queries::create(&db, &server).await.unwrap();

    let remote_v1 = vec![sync::RemoteProject {
        id: "rp-1".to_string(),
        name: "Old Name".to_string(),
        key: "ON".to_string(),
    }];

    sync::sync_projects(
        &db,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote_v1,
        &[],
    )
    .await
    .unwrap();

    let remote_v2 = vec![sync::RemoteProject {
        id: "rp-1".to_string(),
        name: "New Name".to_string(),
        key: "ON".to_string(),
    }];

    let (created, removed) = sync::sync_projects(
        &db,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote_v2,
        &[],
    )
    .await
    .unwrap();

    assert_eq!(created, 0);
    assert_eq!(removed, 0);

    let projects = project_crud::list_db(&db).await.unwrap();
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].name, "New Name");
}

#[tokio::test]
async fn sync_removes_bookmark_when_project_becomes_excluded() {
    let (_temp, db) = common::migrated_db().await;
    let clock = RealClock;

    let server = test_server("srv-1", "Test");
    queries::create(&db, &server).await.unwrap();

    let remote = vec![sync::RemoteProject {
        id: "rp-1".to_string(),
        name: "Project".to_string(),
        key: "P".to_string(),
    }];

    sync::sync_projects(
        &db,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote,
        &[],
    )
    .await
    .unwrap();

    assert_eq!(project_crud::list_db(&db).await.unwrap().len(), 1);

    let excluded = vec!["rp-1".to_string()];
    let (created, removed) = sync::sync_projects(
        &db,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote,
        &excluded,
    )
    .await
    .unwrap();

    assert_eq!(created, 0);
    assert_eq!(removed, 1);
    assert_eq!(project_crud::list_db(&db).await.unwrap().len(), 0);
}
