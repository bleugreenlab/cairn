//! Tests for server CRUD queries and project sync.

/// In-memory SQLite with FK enabled BEFORE migrations (matches unit test behavior).
/// `common::test_conn` enables FK after migrations, which leaves a stale
/// `artifacts_new` FK reference that breaks CASCADE deletes.
fn test_conn() -> diesel::SqliteConnection {
    use diesel::prelude::*;
    use diesel_migrations::MigrationHarness;

    let mut conn =
        diesel::SqliteConnection::establish(":memory:").expect("Failed to open in-memory database");
    diesel::sql_query("PRAGMA foreign_keys = ON")
        .execute(&mut conn)
        .expect("Failed to enable foreign keys");
    conn.run_pending_migrations(cairn_core::internal::db::MIGRATIONS)
        .expect("Failed to run Diesel migrations");
    conn
}

// ============================================================================
// Helper: create a DbServer for testing
// ============================================================================

fn test_db_server(id: &str, name: &str) -> cairn_core::remote_servers::models::DbServer {
    let now = chrono::Utc::now().timestamp() as i32;
    cairn_core::remote_servers::models::DbServer {
        id: id.to_string(),
        name: name.to_string(),
        url: format!("http://{}.example.com:8080", name.to_lowercase()),
        org_id: None,
        status: "unknown".to_string(),
        version: None,
        error_message: None,
        excluded_project_ids: None,
        last_seen_at: None,
        created_at: now,
        updated_at: now,
    }
}

// ============================================================================
// queries::create + queries::get
// ============================================================================

#[test]
fn create_and_get_server() {
    let mut conn = test_conn();
    let db_server = test_db_server("srv-1", "Prod");

    cairn_core::remote_servers::queries::create(&mut conn, &db_server).unwrap();

    let fetched = cairn_core::remote_servers::queries::get(&mut conn, "srv-1")
        .unwrap()
        .expect("server should exist");
    assert_eq!(fetched.name, "Prod");
    assert_eq!(fetched.status, "unknown");
}

#[test]
fn get_nonexistent_returns_none() {
    let mut conn = test_conn();
    let result = cairn_core::remote_servers::queries::get(&mut conn, "nope").unwrap();
    assert!(result.is_none());
}

// ============================================================================
// queries::list (ordered by created_at desc)
// ============================================================================

#[test]
fn list_servers_ordered_by_created_at_desc() {
    let mut conn = test_conn();

    let mut older = test_db_server("srv-old", "Older");
    older.created_at -= 100;
    let newer = test_db_server("srv-new", "Newer");

    cairn_core::remote_servers::queries::create(&mut conn, &older).unwrap();
    cairn_core::remote_servers::queries::create(&mut conn, &newer).unwrap();

    let servers = cairn_core::remote_servers::queries::list(&mut conn).unwrap();
    assert_eq!(servers.len(), 2);
    assert_eq!(servers[0].name, "Newer");
    assert_eq!(servers[1].name, "Older");
}

// ============================================================================
// queries::update_status
// ============================================================================

#[test]
fn update_status_changes_status_and_error_message() {
    let mut conn = test_conn();
    let db_server = test_db_server("srv-1", "Test");
    cairn_core::remote_servers::queries::create(&mut conn, &db_server).unwrap();

    cairn_core::remote_servers::queries::update_status(
        &mut conn,
        "srv-1",
        "error",
        Some("timeout"),
    )
    .unwrap();

    let fetched = cairn_core::remote_servers::queries::get(&mut conn, "srv-1")
        .unwrap()
        .unwrap();
    assert_eq!(fetched.status, "error");
    assert_eq!(fetched.error_message.as_deref(), Some("timeout"));
}

// ============================================================================
// queries::update_health
// ============================================================================

#[test]
fn update_health_sets_version_and_last_seen() {
    let mut conn = test_conn();
    let db_server = test_db_server("srv-1", "Test");
    cairn_core::remote_servers::queries::create(&mut conn, &db_server).unwrap();

    cairn_core::remote_servers::queries::update_health(
        &mut conn,
        "srv-1",
        "connected",
        Some("1.2.3"),
        None,
    )
    .unwrap();

    let fetched = cairn_core::remote_servers::queries::get(&mut conn, "srv-1")
        .unwrap()
        .unwrap();
    assert_eq!(fetched.status, "connected");
    assert_eq!(fetched.version.as_deref(), Some("1.2.3"));
    assert!(fetched.last_seen_at.is_some());
    assert!(fetched.error_message.is_none());
}

// ============================================================================
// queries::update (name, excluded_project_ids)
// ============================================================================

#[test]
fn update_name_only() {
    let mut conn = test_conn();
    let db_server = test_db_server("srv-1", "Old Name");
    cairn_core::remote_servers::queries::create(&mut conn, &db_server).unwrap();

    cairn_core::remote_servers::queries::update(&mut conn, "srv-1", Some("New Name"), None)
        .unwrap();

    let fetched = cairn_core::remote_servers::queries::get(&mut conn, "srv-1")
        .unwrap()
        .unwrap();
    assert_eq!(fetched.name, "New Name");
}

#[test]
fn update_excluded_project_ids() {
    let mut conn = test_conn();
    let db_server = test_db_server("srv-1", "Test");
    cairn_core::remote_servers::queries::create(&mut conn, &db_server).unwrap();

    cairn_core::remote_servers::queries::update(
        &mut conn,
        "srv-1",
        None,
        Some(r#"["proj-1","proj-2"]"#),
    )
    .unwrap();

    let fetched = cairn_core::remote_servers::queries::get(&mut conn, "srv-1")
        .unwrap()
        .unwrap();
    assert_eq!(fetched.excluded_project_ids, vec!["proj-1", "proj-2"]);
}

// ============================================================================
// queries::delete
// ============================================================================

#[test]
fn delete_server() {
    let mut conn = test_conn();
    let db_server = test_db_server("srv-1", "Test");
    cairn_core::remote_servers::queries::create(&mut conn, &db_server).unwrap();

    cairn_core::remote_servers::queries::delete(&mut conn, "srv-1").unwrap();

    let fetched = cairn_core::remote_servers::queries::get(&mut conn, "srv-1").unwrap();
    assert!(fetched.is_none());
}

// ============================================================================
// queries::list_by_org
// ============================================================================

#[test]
fn list_by_org_filters_by_org_id() {
    let mut conn = test_conn();

    let mut srv_org1 = test_db_server("srv-org1", "Org1 Server");
    srv_org1.org_id = Some("org-aaa".to_string());

    let mut srv_org2 = test_db_server("srv-org2", "Org2 Server");
    srv_org2.org_id = Some("org-bbb".to_string());

    let srv_no_org = test_db_server("srv-none", "No Org");

    cairn_core::remote_servers::queries::create(&mut conn, &srv_org1).unwrap();
    cairn_core::remote_servers::queries::create(&mut conn, &srv_org2).unwrap();
    cairn_core::remote_servers::queries::create(&mut conn, &srv_no_org).unwrap();

    let org1_servers =
        cairn_core::remote_servers::queries::list_by_org(&mut conn, "org-aaa").unwrap();
    assert_eq!(org1_servers.len(), 1);
    assert_eq!(org1_servers[0].name, "Org1 Server");
    assert_eq!(org1_servers[0].org_id.as_deref(), Some("org-aaa"));

    let org2_servers =
        cairn_core::remote_servers::queries::list_by_org(&mut conn, "org-bbb").unwrap();
    assert_eq!(org2_servers.len(), 1);
    assert_eq!(org2_servers[0].name, "Org2 Server");

    let empty =
        cairn_core::remote_servers::queries::list_by_org(&mut conn, "org-nonexistent").unwrap();
    assert!(empty.is_empty());
}

// ============================================================================
// queries::delete_server_projects
// ============================================================================

#[test]
fn delete_server_projects_removes_linked_projects() {
    let mut conn = test_conn();
    let clock = cairn_core::internal::services::RealClock;

    // Create server
    let db_server = test_db_server("srv-1", "Test");
    cairn_core::remote_servers::queries::create(&mut conn, &db_server).unwrap();

    // Create two projects linked to this server
    cairn_core::projects::crud::create_db(
        &mut conn,
        &clock,
        &cairn_core::models::CreateProject {
            id: Some("p1".to_string()),
            name: "Project 1".to_string(),
            key: "P1".to_string(),
            repo_path: String::new(),
            remote_url: Some("http://example.com".to_string()),
            server_id: Some("srv-1".to_string()),
        },
    )
    .unwrap();

    cairn_core::projects::crud::create_db(
        &mut conn,
        &clock,
        &cairn_core::models::CreateProject {
            id: Some("p2".to_string()),
            name: "Project 2".to_string(),
            key: "P2".to_string(),
            repo_path: String::new(),
            remote_url: Some("http://example.com".to_string()),
            server_id: Some("srv-1".to_string()),
        },
    )
    .unwrap();

    // Create an unlinked project
    cairn_core::projects::crud::create_db(
        &mut conn,
        &clock,
        &cairn_core::models::CreateProject {
            id: Some("p3".to_string()),
            name: "Local".to_string(),
            key: "LOC".to_string(),
            repo_path: "/tmp/local".to_string(),
            remote_url: None,
            server_id: None,
        },
    )
    .unwrap();

    // Delete server projects
    cairn_core::remote_servers::queries::delete_server_projects(&mut conn, "srv-1").unwrap();

    // Linked projects gone, local project remains
    let projects = cairn_core::projects::crud::list_db(&mut conn).unwrap();
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].name, "Local");
}

// ============================================================================
// sync::sync_projects — creates new bookmarks
// ============================================================================

#[test]
fn sync_creates_new_project_bookmarks() {
    let mut conn = test_conn();
    let clock = cairn_core::internal::services::RealClock;

    // Create server
    let db_server = test_db_server("srv-1", "Test");
    cairn_core::remote_servers::queries::create(&mut conn, &db_server).unwrap();

    let remote_projects = vec![
        cairn_core::remote_servers::sync::RemoteProject {
            id: "rp-1".to_string(),
            name: "Remote Alpha".to_string(),
            key: "RA".to_string(),
        },
        cairn_core::remote_servers::sync::RemoteProject {
            id: "rp-2".to_string(),
            name: "Remote Beta".to_string(),
            key: "RB".to_string(),
        },
    ];

    let (created, removed) = cairn_core::remote_servers::sync::sync_projects(
        &mut conn,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote_projects,
        &[],
    )
    .unwrap();

    assert_eq!(created, 2);
    assert_eq!(removed, 0);

    let projects = cairn_core::projects::crud::list_db(&mut conn).unwrap();
    assert_eq!(projects.len(), 2);
}

// ============================================================================
// sync::sync_projects — respects exclusions
// ============================================================================

#[test]
fn sync_respects_excluded_project_ids() {
    let mut conn = test_conn();
    let clock = cairn_core::internal::services::RealClock;

    let db_server = test_db_server("srv-1", "Test");
    cairn_core::remote_servers::queries::create(&mut conn, &db_server).unwrap();

    let remote_projects = vec![
        cairn_core::remote_servers::sync::RemoteProject {
            id: "rp-1".to_string(),
            name: "Included".to_string(),
            key: "INC".to_string(),
        },
        cairn_core::remote_servers::sync::RemoteProject {
            id: "rp-2".to_string(),
            name: "Excluded".to_string(),
            key: "EXC".to_string(),
        },
    ];

    let excluded = vec!["rp-2".to_string()];

    let (created, removed) = cairn_core::remote_servers::sync::sync_projects(
        &mut conn,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote_projects,
        &excluded,
    )
    .unwrap();

    assert_eq!(created, 1);
    assert_eq!(removed, 0);

    let projects = cairn_core::projects::crud::list_db(&mut conn).unwrap();
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].name, "Included");
}

// ============================================================================
// sync::sync_projects — removes stale bookmarks
// ============================================================================

#[test]
fn sync_removes_stale_bookmarks() {
    let mut conn = test_conn();
    let clock = cairn_core::internal::services::RealClock;

    let db_server = test_db_server("srv-1", "Test");
    cairn_core::remote_servers::queries::create(&mut conn, &db_server).unwrap();

    // First sync: create two projects
    let remote_v1 = vec![
        cairn_core::remote_servers::sync::RemoteProject {
            id: "rp-1".to_string(),
            name: "Alpha".to_string(),
            key: "A".to_string(),
        },
        cairn_core::remote_servers::sync::RemoteProject {
            id: "rp-2".to_string(),
            name: "Beta".to_string(),
            key: "B".to_string(),
        },
    ];

    cairn_core::remote_servers::sync::sync_projects(
        &mut conn,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote_v1,
        &[],
    )
    .unwrap();

    // Second sync: only one project remains on the remote
    let remote_v2 = vec![cairn_core::remote_servers::sync::RemoteProject {
        id: "rp-1".to_string(),
        name: "Alpha".to_string(),
        key: "A".to_string(),
    }];

    let (created, removed) = cairn_core::remote_servers::sync::sync_projects(
        &mut conn,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote_v2,
        &[],
    )
    .unwrap();

    assert_eq!(created, 0);
    assert_eq!(removed, 1);

    let projects = cairn_core::projects::crud::list_db(&mut conn).unwrap();
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].id, "rp-1");
}

// ============================================================================
// sync::sync_projects — updates project name if changed
// ============================================================================

#[test]
fn sync_updates_project_name_when_changed() {
    let mut conn = test_conn();
    let clock = cairn_core::internal::services::RealClock;

    let db_server = test_db_server("srv-1", "Test");
    cairn_core::remote_servers::queries::create(&mut conn, &db_server).unwrap();

    // Initial sync
    let remote_v1 = vec![cairn_core::remote_servers::sync::RemoteProject {
        id: "rp-1".to_string(),
        name: "Old Name".to_string(),
        key: "ON".to_string(),
    }];

    cairn_core::remote_servers::sync::sync_projects(
        &mut conn,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote_v1,
        &[],
    )
    .unwrap();

    // Re-sync with updated name
    let remote_v2 = vec![cairn_core::remote_servers::sync::RemoteProject {
        id: "rp-1".to_string(),
        name: "New Name".to_string(),
        key: "ON".to_string(),
    }];

    let (created, removed) = cairn_core::remote_servers::sync::sync_projects(
        &mut conn,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote_v2,
        &[],
    )
    .unwrap();

    assert_eq!(created, 0);
    assert_eq!(removed, 0);

    let projects = cairn_core::projects::crud::list_db(&mut conn).unwrap();
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].name, "New Name");
}

// ============================================================================
// sync::sync_projects — excluded existing bookmarks get removed
// ============================================================================

#[test]
fn sync_removes_bookmark_when_project_becomes_excluded() {
    let mut conn = test_conn();
    let clock = cairn_core::internal::services::RealClock;

    let db_server = test_db_server("srv-1", "Test");
    cairn_core::remote_servers::queries::create(&mut conn, &db_server).unwrap();

    // First sync: create project
    let remote = vec![cairn_core::remote_servers::sync::RemoteProject {
        id: "rp-1".to_string(),
        name: "Project".to_string(),
        key: "P".to_string(),
    }];

    cairn_core::remote_servers::sync::sync_projects(
        &mut conn,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote,
        &[],
    )
    .unwrap();

    assert_eq!(
        cairn_core::projects::crud::list_db(&mut conn)
            .unwrap()
            .len(),
        1
    );

    // Second sync: same remote list but project is now excluded
    let excluded = vec!["rp-1".to_string()];
    let (created, removed) = cairn_core::remote_servers::sync::sync_projects(
        &mut conn,
        &clock,
        "srv-1",
        "http://test.example.com:8080",
        &remote,
        &excluded,
    )
    .unwrap();

    // The excluded project is not in remote_ids, so the existing bookmark gets removed
    assert_eq!(created, 0);
    assert_eq!(removed, 1);
    assert_eq!(
        cairn_core::projects::crud::list_db(&mut conn)
            .unwrap()
            .len(),
        0
    );
}
