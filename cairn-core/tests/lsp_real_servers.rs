//! Gated end-to-end proof that the LSP engine drives a real language server.
//!
//! Spawns `rust-analyzer` against `tests/fixtures/lsp_rust/` through the actual
//! pool (`LspManager::get_or_spawn` → confined spawn → handshake → readiness
//! gating → query) and asserts definition/references/hover. This is the only
//! coverage that exercises the transport, handshake, and indexing-readiness path
//! against a live server rather than a scripted mock.
//!
//! Self-skips when the binary is absent, when no OS sandbox is available, and
//! when run nested inside a Cairn worktree fence (the confined real spawn cannot
//! nest). Unfenced CI with rust-analyzer installed runs it for real.

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use cairn_core::config::build_services::Templates;
use cairn_core::config::language_servers::LanguageServerConfig;
use cairn_core::internal::services::{sandbox, RealProcessSpawner};
use cairn_core::lsp::manager::LspManager;
use cairn_core::lsp::edit::plan_workspace_edit;
use cairn_core::lsp::queries::{
    compute_rename, resolve_rename_position, run_named_query, QueryOutcome, RenameTarget,
};
use cairn_core::lsp::{InstanceKey, LspOp};
use std::path::Path;

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lsp_rust")
}

fn rust_cfg() -> LanguageServerConfig {
    LanguageServerConfig {
        enabled: true,
        command: vec!["rust-analyzer".to_string()],
        extensions: vec!["rs".to_string()],
        root_markers: vec!["Cargo.toml".to_string()],
        container_separator: "::".to_string(),
        initialization_options: None,
        env: Default::default(),
    }
}

/// Whether a *functional* rust-analyzer is installed. A bare PATH lookup is not
/// enough: a rustup proxy shim resolves on PATH yet errors ("Unknown binary")
/// and exits when the `rust-analyzer` component is not installed in the
/// toolchain. The real binary's `--version` prints a `rust-analyzer ...` banner
/// to stdout; the shim error goes to stderr and leaves stdout empty, so probing
/// stdout distinguishes a working server from a stub.
fn rust_analyzer_works() -> bool {
    std::process::Command::new("rust-analyzer")
        .arg("--version")
        .output()
        .map(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).contains("rust-analyzer"))
        .unwrap_or(false)
}

fn templates(cache: &std::path::Path) -> Templates {
    Templates {
        home: PathBuf::from("/tmp"),
        cairn_home: cache.to_path_buf(),
        worktrees: PathBuf::from("/tmp"),
        worktree: None,
    }
}

#[test]
fn rust_analyzer_definition_references_hover() {
    if !rust_analyzer_works() {
        eprintln!("skipping: a functional rust-analyzer is not installed");
        return;
    }
    if !sandbox::is_available() {
        eprintln!("skipping: OS sandbox is unavailable on this host");
        return;
    }
    if common::skip_if_fenced("rust_analyzer_definition_references_hover") {
        return;
    }

    let root = fixture_root();
    let cache = tempfile::tempdir().unwrap();
    let spawner = RealProcessSpawner;
    let manager = LspManager::new();
    let key = InstanceKey::new("rust", root.clone());

    let instance = manager
        .get_or_spawn(
            &spawner,
            key,
            &rust_cfg(),
            &templates(cache.path()),
            cache.path(),
            vec![],
        )
        .expect("rust-analyzer should spawn and hand-shake");

    let file = root.join("src/lib.rs");
    instance
        .client
        .ensure_open(&file)
        .expect("didOpen should succeed");

    // Definition resolves the symbol back to its declaration site.
    let (def, _) = run_named_query(
        &instance.client,
        &root,
        LspOp::Definition,
        "build_widget",
        "::",
    )
    .expect("definition query");
    match def {
        QueryOutcome::Locations(hits) => assert!(
            hits.iter().any(|h| h.path.ends_with("lib.rs")),
            "definition should point into lib.rs, got {hits:?}"
        ),
        other => panic!("definition: expected locations, got {other:?}"),
    }

    // References include both call sites plus the declaration.
    let (refs, _) = run_named_query(
        &instance.client,
        &root,
        LspOp::References,
        "build_widget",
        "::",
    )
    .expect("references query");
    match refs {
        QueryOutcome::Locations(hits) => assert!(
            hits.len() >= 2,
            "references should find the declaration and call sites, got {hits:?}"
        ),
        other => panic!("references: expected locations, got {other:?}"),
    }

    // Hover renders the signature.
    let (hov, _) = run_named_query(&instance.client, &root, LspOp::Hover, "build_widget", "::")
        .expect("hover query");
    match hov {
        QueryOutcome::Hover(md) => assert!(
            md.contains("build_widget"),
            "hover should mention the symbol, got: {md}"
        ),
        other => panic!("hover: expected hover, got {other:?}"),
    }

    manager.stop_all();
}

/// Gated proof that the rename write op drives a real server end to end: resolve
/// `build_widget`, ask rust-analyzer for the `textDocument/rename` `WorkspaceEdit`,
/// and run it through the applier ([`plan_workspace_edit`]) to produce the
/// post-edit file contents. Non-destructive: the applier returns new content in
/// memory and never writes the fixture.
#[test]
fn rust_analyzer_rename_across_files() {
    if !rust_analyzer_works() {
        eprintln!("skipping: a functional rust-analyzer is not installed");
        return;
    }
    if !sandbox::is_available() {
        eprintln!("skipping: OS sandbox is unavailable on this host");
        return;
    }
    if common::skip_if_fenced("rust_analyzer_rename_across_files") {
        return;
    }

    let root = fixture_root();
    let cache = tempfile::tempdir().unwrap();
    let spawner = RealProcessSpawner;
    let manager = LspManager::new();
    let key = InstanceKey::new("rust", root.clone());

    let instance = manager
        .get_or_spawn(
            &spawner,
            key,
            &rust_cfg(),
            &templates(cache.path()),
            cache.path(),
            vec![],
        )
        .expect("rust-analyzer should spawn and hand-shake");

    let file = root.join("src/lib.rs");
    instance
        .client
        .ensure_open(&file)
        .expect("didOpen should succeed");

    // Resolve the symbol, then ask the server to compute the rename edit set.
    let (uri, position) =
        match resolve_rename_position(&instance.client, &root, "build_widget", "::")
            .expect("resolve rename position")
        {
            RenameTarget::Resolved { uri, position } => (uri, position),
            other => panic!("expected a single resolved symbol, got {other:?}"),
        };
    let edit = compute_rename(&instance.client, &uri, position, "assemble_widget")
        .expect("rename should produce a WorkspaceEdit");

    let translate = |p: &Path| p.starts_with(&root).then(|| p.to_path_buf());
    let file_edits = plan_workspace_edit(&edit, &translate).expect("applier should plan the edit");
    assert!(!file_edits.is_empty(), "rename should touch at least one file");

    let total_sites: usize = file_edits.iter().map(|fe| fe.site_count).sum();
    assert!(
        total_sites >= 2,
        "rename should rewrite the declaration and its call sites, got {file_edits:?}"
    );
    assert!(
        file_edits
            .iter()
            .filter_map(|fe| fe.new_content.as_deref())
            .any(|content| content.contains("assemble_widget")),
        "post-edit content should carry the new name, got {file_edits:?}"
    );

    manager.stop_all();
}

/// End-to-end through the actual `read` surface: a seeded node whose worktree is
/// the fixture crate, queried via `cairn://.../lsp` resource URIs and the
/// `file:...?lsp=` projection, all driving real rust-analyzer. Proves the URI
/// layer, resource dispatch, orchestrator routing, file projection, and honest
/// fallbacks compose into working answers — not just the engine in isolation.
#[tokio::test]
async fn read_surface_drives_real_rust_analyzer() {
    use cairn_core::internal::db::DbState;
    use cairn_core::internal::mcp::handlers::files::handle_read_file;
    use cairn_core::internal::mcp::types::McpCallbackRequest;
    use cairn_core::internal::orchestrator::Orchestrator;
    use cairn_core::internal::services::testing::TestServicesBuilder;
    use cairn_core::internal::services::RealProcessSpawner;
    use cairn_core::internal::storage::SearchIndex;
    use turso::params;

    if !rust_analyzer_works() {
        eprintln!("skipping: a functional rust-analyzer is not installed");
        return;
    }
    if !sandbox::is_available() {
        eprintln!("skipping: OS sandbox is unavailable on this host");
        return;
    }
    if common::skip_if_fenced("read_surface_drives_real_rust_analyzer") {
        return;
    }

    let fixture = fixture_root();
    let fixture_str = fixture.to_string_lossy().to_string();
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::insert_project_with_repo(&db, "LSPX", &fixture).await;

    // A real process spawner is required: the read path drives an actual
    // rust-analyzer, not the mock spawner `common::orchestrator` installs.
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db.clone(), search_index));
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_process(RealProcessSpawner)
            .build(),
    );
    let orch = Orchestrator::builder(db_state, services, temp.path().join("config")).build();

    // Seed an issue + execution + node job whose worktree is the fixture crate.
    db.execute(
        "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
         VALUES ('issue-lsp', ?1, 7, 'LSP', 'active', 1, 1)",
        params![project_id.as_str()],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
         VALUES ('exec-lsp', 'recipe', 'issue-lsp', ?1, 'running', 1, 1)",
        params![project_id.as_str()],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path)
         VALUES ('job-builder', 'exec-lsp', 'issue-lsp', ?1, 'Builder', 'running', 1, 1, 'builder', ?2)",
        params![project_id.as_str(), fixture_str.as_str()],
    )
    .await
    .unwrap();
    // An orphaned node whose worktree no longer exists on disk.
    db.execute(
        "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path)
         VALUES ('job-ghost', 'exec-lsp', 'issue-lsp', ?1, 'Ghost', 'running', 1, 1, 'ghost', '/no/such/dir/lsp-ghost')",
        params![project_id.as_str()],
    )
    .await
    .unwrap();

    // search discovers the symbol.
    let search =
        common::read_resource(&orch, "cairn://p/LSPX/7/1/builder/lsp?search=build_widget").await;
    assert!(
        search.contains("build_widget"),
        "search should surface build_widget, got:\n{search}"
    );

    // references finds the declaration plus call sites (>= 2 rows).
    let refs = common::read_resource(
        &orch,
        "cairn://p/LSPX/7/1/builder/lsp/build_widget?op=references",
    )
    .await;
    assert!(
        refs.matches("lib.rs:").count() >= 2,
        "references should find >= 2 sites, got:\n{refs}"
    );

    // definition resolves into lib.rs.
    let def = common::read_resource(
        &orch,
        "cairn://p/LSPX/7/1/builder/lsp/build_widget?op=definition",
    )
    .await;
    assert!(
        def.contains("lib.rs"),
        "definition should point into lib.rs, got:\n{def}"
    );

    // File projection through the file read handler (cwd = fixture).
    let file_request = McpCallbackRequest {
        cwd: fixture_str.clone(),
        run_id: None,
        tool: "read".to_string(),
        payload: serde_json::json!({
            "path": "file:src/lib.rs?lsp=references&symbol=build_widget"
        }),
        tool_use_id: None,
    };
    let file_proj = handle_read_file(&orch, &file_request).await;
    assert!(
        file_proj.matches("lib.rs:").count() >= 2,
        "file projection should find >= 2 reference sites, got:\n{file_proj}"
    );

    // Honest fallback: an extension with no configured server points at text search.
    let scratch = tempfile::tempdir().unwrap();
    std::fs::write(scratch.path().join("notes.xyz"), "build_widget\n").unwrap();
    let fallback_request = McpCallbackRequest {
        cwd: scratch.path().to_string_lossy().to_string(),
        run_id: None,
        tool: "read".to_string(),
        payload: serde_json::json!({
            "path": "file:notes.xyz?lsp=references&symbol=x"
        }),
        tool_use_id: None,
    };
    let fallback = handle_read_file(&orch, &fallback_request).await;
    assert!(
        fallback.to_lowercase().contains("text search"),
        "unknown extension should fall back to text search, got:\n{fallback}"
    );

    // Honest fallback: an orphaned node whose worktree is gone.
    let orphan = common::read_resource(&orch, "cairn://p/LSPX/7/1/ghost/lsp/build_widget").await;
    assert!(
        orphan.contains("instance unavailable"),
        "orphaned node should report instance unavailable, got:\n{orphan}"
    );

    orch.stop_lsp_services();
}
