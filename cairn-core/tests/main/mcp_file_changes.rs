use crate::common;
use std::path::Path;
use std::sync::Arc;

use cairn_core::internal::db::DbState;
use cairn_core::internal::jj::{self, JjEnv};
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::storage::{LocalDb, RowExt, SearchIndex};
use cairn_core::memories::db as memory_db;
use cairn_db::turso::params;
use serde_json::json;

// The change/write path seals and discards through the jj VCS seam, so a
// file-target test cwd must be a real `.jj` workspace over a shared store. These
// tests resolve `jj` and self-skip with a note when it is unavailable, mirroring
// `mcp::vcs`'s jj tests. Resource-only tests never touch the VCS and keep a
// plain cwd.

fn make_request(cwd: &str, payload: serde_json::Value) -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: cwd.to_string(),
        run_id: None,
        tool: "write".to_string(),
        payload,
        tool_use_id: None,
    }
}

fn make_preview_request(cwd: &str, payload: serde_json::Value) -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: cwd.to_string(),
        run_id: Some("run-preview".to_string()),
        tool: "write".to_string(),
        payload,
        tool_use_id: Some("toolu-preview".to_string()),
    }
}

fn parse_report(result: &str) -> serde_json::Value {
    serde_json::from_str(result)
        .unwrap_or_else(|_| panic!("Expected JSON ChangeReport, got: {}", result))
}

fn failure_count(report: &serde_json::Value) -> usize {
    report["failures"].as_array().map(Vec::len).unwrap_or(0)
}

fn assert_successful_change(report: &serde_json::Value, applied_count: usize) {
    assert_eq!(failure_count(report), 0, "{report:?}");
    assert_eq!(
        report["applied"].as_array().unwrap().len(),
        applied_count,
        "{report:?}"
    );
}

async fn change_report(
    orch: &Orchestrator,
    cwd: &str,
    payload: serde_json::Value,
) -> serde_json::Value {
    parse_report(&handle_write(orch, &make_request(cwd, payload)).await)
}

struct ChangeTestRepo {
    dir: tempfile::TempDir,
    _project: tempfile::TempDir,
    config: tempfile::TempDir,
    orch: Orchestrator,
}

impl ChangeTestRepo {
    /// Provision a jj-workspace-backed change test, or `None` to self-skip when
    /// jj is unavailable. The cwd (`dir`) is a real `.jj` workspace over a shared
    /// store backed by a throwaway project git repo.
    async fn try_new() -> Option<Self> {
        common::jj_bin()?;
        let dir = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        init_git_repo(project.path());
        let config = tempfile::tempdir().unwrap();
        let orch = orchestrator_with_config(config.path()).await;
        common::provision_jj_workspace(
            config.path(),
            project.path(),
            dir.path(),
            "agent/CHG-1-builder-0",
        );
        Some(Self {
            dir,
            _project: project,
            config,
            orch,
        })
    }

    /// Seal the current working copy into a base commit, so a later delete or
    /// edit operates against committed content rather than un-sealed `@` dirt.
    fn seal_base(&self, msg: &str) {
        let jj = JjEnv::resolve("jj", self.config.path());
        jj::seal(&jj, self.dir.path(), msg, None).unwrap();
    }

    /// Make this workspace OP-LOG stale via a sibling advance (the production
    /// data-loss shape: a later seal AND its restore are both blocked by
    /// staleness). See [`common::stale_sibling_advance`].
    fn make_stale_via_sibling(&self) {
        common::stale_sibling_advance(
            self.config.path(),
            self._project.path(),
            self.dir.path(),
            "agent/CHG-1-builder-0",
        );
    }

    fn cwd(&self) -> &str {
        self.dir.path().to_str().unwrap()
    }

    fn path(&self, path: &str) -> std::path::PathBuf {
        self.dir.path().join(path)
    }

    fn write(&self, path: &str, content: &str) {
        let path = self.path(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, content).unwrap();
    }

    fn read(&self, path: &str) -> String {
        std::fs::read_to_string(self.path(path)).unwrap()
    }

    async fn change_report(&self, payload: serde_json::Value) -> serde_json::Value {
        change_report(&self.orch, self.cwd(), payload).await
    }

    async fn preview_report(&self, payload: serde_json::Value) -> serde_json::Value {
        parse_report(&handle_write(&self.orch, &make_preview_request(self.cwd(), payload)).await)
    }
}

fn seed_bad_good_files(repo: &ChangeTestRepo) {
    repo.write("bad.rs", "old bad\n");
    repo.write("good.rs", "old good\n");
}

fn bad_good_patch_payload(commit_msg: &str, atomic: bool) -> serde_json::Value {
    let mut payload = json!({
        "changes": [
            { "target": "file:bad.rs", "mode": "patch", "payload": { "old_string": "missing", "new_string": "new bad" } },
            { "target": "file:good.rs", "mode": "patch", "payload": { "old_string": "old good", "new_string": "new good" } }
        ],
        "commit_msg": commit_msg
    });
    if atomic {
        payload["atomic"] = json!(true);
    }
    payload
}

async fn test_file_change_orchestrator() -> Orchestrator {
    let temp = tempfile::tempdir().unwrap();
    orchestrator_with_config(&temp.keep()).await
}

async fn orchestrator_with_config(config_dir: &Path) -> Orchestrator {
    let (_db_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let search_index = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    Orchestrator::builder(db_state, services, config_dir.to_path_buf()).build()
}

async fn insert_issue(db: &LocalDb, project_id: &str, number: i64, title: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let project_id = project_id.to_string();
    let title = title.to_string();
    db.execute(
        "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, 'active', 1, 1)",
        params![id.as_str(), project_id.as_str(), number, title.as_str()],
    )
    .await
    .unwrap();
    id
}

async fn insert_preview_event(db: &LocalDb, input: serde_json::Value) {
    let project_id = common::create_project(db, "PREVIEW").await;
    db.write(|conn| {
        let project_id = project_id.clone();
        let data = json!({
            "toolUses": [{
                "id": "toolu-preview",
                "name": "mcp__cairn__change",
                "input": input,
            }]
        })
        .to_string();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
                 VALUES ('job-preview', ?1, 'running', 'session-preview', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('session-preview', 'job-preview', 'active', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-preview', ?1, 'job-preview', 'live', 'session-preview', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
                 VALUES ('event-preview', 'run-preview', 'session-preview', 1, 1, 'assistant', ?1, 1)",
                params![data.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn count_rows(db: &LocalDb, sql: &'static str) -> i64 {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(sql, ()).await?;
            let row = rows.next().await?.expect("missing count row");
            row.i64(0)
        })
    })
    .await
    .unwrap()
}

fn init_git_repo(path: &std::path::Path) {
    for args in [
        vec!["init"],
        vec!["config", "user.email", "test@example.com"],
        vec!["config", "user.name", "Test User"],
        // Create an empty root commit so HEAD exists. The worktree==HEAD
        // invariant the change handler enforces needs a HEAD to reset to.
        vec!["commit", "--allow-empty", "-m", "initial"],
    ] {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(path)
            .output()
            .unwrap();
        assert!(output.status.success(), "git setup failed: {output:?}");
    }
}

#[tokio::test]
async fn change_patches_file_with_codex_patch_envelope() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lib.rs", "let x = 1;\n");

    let report = repo
        .change_report(json!({
            "changes": [{
                "target": "file:lib.rs",
                "mode": "patch",
                "payload": {
                    "diff": "*** Begin Patch
*** Update File: file:lib.rs
@@ -1,1 +1,1 @@
-let x = 1;
+let x = 2;
*** End Patch
"
                }
            }],
            "commit_msg": "apply codex patch"
        }))
        .await;

    assert_successful_change(&report, 1);
    assert_eq!(report["commit"]["status"], "committed", "{report:?}");
    assert_eq!(repo.read("lib.rs"), "let x = 2;\n");
}

#[tokio::test]
async fn change_unified_patch_adds_file_with_codex_envelope() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    let report = repo
        .change_report(
        json!({
            "changes": [{
                "target": "file:",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Add File: src/new.rs\n+pub fn new() {}\n*** End Patch\n" }
            }],
            "commit_msg": "add file"
        }),
    )
    .await;

    assert_successful_change(&report, 1);
    assert_eq!(
        report["applied"][0]["mode"].as_str().unwrap(),
        "unified_patch"
    );
    assert_eq!(repo.read("src/new.rs"), "pub fn new() {}");
}

#[tokio::test]
async fn change_unified_patch_updates_file_with_codex_envelope() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lib.rs", "let x = 1;\n");

    let report = repo
        .change_report(
        json!({
            "changes": [{
                "target": "file:lib.rs",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Update File: lib.rs\n@@ -1,1 +1,1 @@\n-let x = 1;\n+let x = 2;\n*** End Patch\n" }
            }],
            "commit_msg": "update file"
        }),
    )
    .await;

    assert_successful_change(&report, 1);
    assert_eq!(repo.read("lib.rs"), "let x = 2;\n");
}

#[tokio::test]
async fn change_unified_patch_deletes_file_with_codex_envelope() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("old.rs", "old();\n");
    // Seal old.rs into the base so deleting it is a real, committable change.
    repo.seal_base("seed old.rs");

    let report = repo
        .change_report(json!({
            "changes": [{
                "target": "file:old.rs",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Delete File: old.rs\n*** End Patch\n" }
            }],
            "commit_msg": "delete file"
        }))
        .await;

    assert_successful_change(&report, 1);
    assert!(!repo.path("old.rs").exists());
}

#[tokio::test]
async fn change_unified_patch_applies_multi_file_envelope() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lib.rs", "old();\n");
    repo.write("delete.rs", "delete();\n");

    let report = repo
        .change_report(
        json!({
            "changes": [{
                "target": "file:",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Add File: add.rs\n+add();\n*** Update File: lib.rs\n@@ -1,1 +1,1 @@\n-old();\n+new();\n*** Delete File: delete.rs\n*** End Patch\n" }
            }],
            "commit_msg": "multi-file envelope"
        }),
    )
    .await;

    assert_successful_change(&report, 3);
    assert_eq!(repo.read("add.rs"), "add();");
    assert_eq!(repo.read("lib.rs"), "new();\n");
    assert!(!repo.path("delete.rs").exists());
}

#[tokio::test]
async fn change_unified_patch_preview_hashes_expanded_targets() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lib.rs", "old();\n");
    let payload = json!({
        "changes": [{
            "target": "file:",
            "mode": "unified_patch",
            "payload": { "patch": "*** Begin Patch\n*** Add File: add.rs\n+add();\n*** Update File: lib.rs\n@@ -1,1 +1,1 @@\n-old();\n+new();\n*** End Patch\n" }
        }],
        "commit_msg": "add files",
        "preview": true
    });
    insert_preview_event(&repo.orch.db.local, payload.clone()).await;

    let report = repo.preview_report(payload).await;

    assert_successful_change(&report, 2);
    assert_eq!(repo.read("lib.rs"), "old();\n");
    assert!(!repo.path("add.rs").exists());
    let targets = report["target_hashes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hash| hash["target"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(targets, vec!["file:add.rs", "file:lib.rs"]);
}

#[tokio::test]
async fn change_unified_patch_applies_native_contextual_hunk() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lib.rs", "fn example() {\n    old();\n}\n\nfn other() {}\n");

    let report = repo
        .change_report(
        json!({
            "changes": [{
                "target": "file:lib.rs",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Update File: lib.rs\n@@ fn example() {\n fn example() {\n-    old();\n+    new();\n }\n*** End Patch\n" }
            }],
            "commit_msg": "native hunk"
        }),
    )
    .await;

    assert_successful_change(&report, 1);
    assert_eq!(
        repo.read("lib.rs"),
        "fn example() {\n    new();\n}\n\nfn other() {}\n"
    );
}

#[tokio::test]
async fn change_unified_patch_native_context_header_scopes_repeated_old_lines() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write(
        "lib.rs",
        "fn first() {\n    old();\n}\n\nfn second() {\n    old();\n}\n",
    );

    let report = repo
        .change_report(
        json!({
            "changes": [{
                "target": "file:lib.rs",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Update File: lib.rs\n@@ fn second() {\n-    old();\n+    new();\n*** End Patch\n" }
            }],
            "commit_msg": "scoped hunk"
        }),
    )
    .await;

    assert_successful_change(&report, 1);
    assert_eq!(
        repo.read("lib.rs"),
        "fn first() {\n    old();\n}\n\nfn second() {\n    new();\n}\n"
    );
}

#[tokio::test]
async fn change_unified_patch_native_add_only_context_header_inserts_after_anchor() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lib.rs", "fn first() {\n}\n\nfn second() {\n}\n");

    let report = repo
        .change_report(
        json!({
            "changes": [{
                "target": "file:lib.rs",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Update File: lib.rs\n@@ fn second() {\n+    inserted();\n*** End Patch\n" }
            }],
            "commit_msg": "insert after anchor"
        }),
    )
    .await;

    assert_successful_change(&report, 1);
    assert_eq!(
        repo.read("lib.rs"),
        "fn first() {\n}\n\nfn second() {\n    inserted();\n}\n"
    );
}

#[tokio::test]
async fn change_unified_patch_accepts_bare_native_header() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write(
        "README.md",
        "- [Algolia Autocomplete](https://www.algolia.com/doc/ui-libraries/autocomplete/introduction/what-is-autocomplete/) - the official Algolia Autocomplete documentation\n- [FlexSearch](https://github.com/nextapps-de/flexsearch) - the official FlexSearch documentation\n- [Zustand](https://docs.pmnd.rs/zustand/getting-started/introduction) - the official Zustand documentation\n",
    );

    let report = repo
        .change_report(
        json!({
            "changes": [{
                "target": "file:",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Update File: README.md\n@@\n - [Algolia Autocomplete](https://www.algolia.com/doc/ui-libraries/autocomplete/introduction/what-is-autocomplete/) - the official Algolia Autocomplete documentation\n - [FlexSearch](https://github.com/nextapps-de/flexsearch) - the official FlexSearch documentation\n-- [Zustand](https://docs.pmnd.rs/zustand/getting-started/introduction) - the official Zustand documentation\n+- [Zustand](https://docs.pmnd.rs/zustand/getting-started/introduction) - the official Zustand documentation\n+\n+## Unified Patch Test\n+\n+Temporary change to validate Cairn `change` with unified patch style in this throwaway worktree.\n*** End Patch" }
            }],
            "commit_msg": "bare native header"
        }),
    )
    .await;

    assert_successful_change(&report, 1);
    assert_eq!(report["commit"]["status"], "committed", "{report:?}");
    assert!(repo.read("README.md").contains("## Unified Patch Test"));
}

#[tokio::test]
async fn change_unified_patch_requires_commit_msg_for_file_targets() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().to_str().unwrap();
    let orch = test_file_change_orchestrator().await;

    let request = make_request(
        cwd,
        json!({
            "changes": [{
                "target": "file:",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Add File: new.rs\n+new();\n*** End Patch\n" }
            }]
        }),
    );

    let report = parse_report(&handle_write(&orch, &request).await);

    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert!(report["failures"][0]["error"]
        .as_str()
        .unwrap()
        .contains("commit_msg"));
    assert!(!dir.path().join("new.rs").exists());
}

#[tokio::test]
async fn change_unified_patch_rejects_single_file_target_mismatch() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    let report = repo
        .change_report(json!({
            "changes": [{
                "target": "file:expected.rs",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Add File: actual.rs\n+actual();\n*** End Patch\n" }
            }],
            "commit_msg": "target mismatch"
        }))
        .await;

    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert!(report["failures"][0]["error"]
        .as_str()
        .unwrap()
        .contains("envelope target path does not match change.target"));
    assert!(!repo.path("actual.rs").exists());
}

#[tokio::test]
async fn change_mixed_unified_patch_and_resource_batch_succeeds() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    common::create_project(&repo.orch.db.local, "MCP").await;

    let request = make_request(
        repo.cwd(),
        json!({
            "changes": [
                {
                    "target": "file:",
                    "mode": "unified_patch",
                    "payload": { "patch": "*** Begin Patch\n*** Add File: mixed.rs\n+mixed();\n*** End Patch\n" }
                },
                {
                    "target": "cairn://p/MCP/messages",
                    "mode": "append",
                    "payload": { "content": "Unified patch landed" }
                }
            ],
            "commit_msg": "mixed batch"
        }),
    );

    let report = parse_report(&handle_write(&repo.orch, &request).await);

    assert_eq!(report["applied"].as_array().unwrap().len(), 2);
    assert_eq!(failure_count(&report), 0, "{report:?}");
    assert_eq!(repo.read("mixed.rs"), "mixed();");
    assert_eq!(
        count_rows(
            &repo.orch.db.local,
            "SELECT COUNT(*) FROM messages WHERE channel_type = 'project' AND channel_id = 'MCP' AND content = 'Unified patch landed'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn change_default_non_atomic_applies_matching_file_items() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    seed_bad_good_files(&repo);

    let report = repo
        .change_report(bad_good_patch_payload("apply matching only", false))
        .await;

    assert_eq!(report["applied"].as_array().unwrap().len(), 1);
    assert_eq!(report["applied"][0]["index"], 1);
    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert_eq!(report["failures"][0]["index"], 0);
    assert_eq!(report["partial_success"], true);
    // The successful item still commits even though a sibling failed.
    assert_eq!(report["commit"]["status"], "committed", "{report:?}");
    assert_eq!(repo.read("bad.rs"), "old bad\n");
    assert_eq!(repo.read("good.rs"), "new good\n");
}

#[tokio::test]
async fn change_default_non_atomic_commits_only_applied_files() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    seed_bad_good_files(&repo);

    let report = repo
        .change_report(bad_good_patch_payload("commit good only", false))
        .await;

    assert_eq!(report["commit"]["status"], "committed");
    assert_eq!(failure_count(&report), 1, "{report:?}");
    // Only the applied file is sealed; the failed sibling is unchanged on disk
    // (and so absent from the commit).
    assert_eq!(repo.read("good.rs"), "new good\n");
    assert_eq!(repo.read("bad.rs"), "old bad\n");
}

#[tokio::test]
async fn change_default_non_atomic_chained_file_items_compose() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("other.rs", "old other\n");

    let report = repo
        .change_report(
        json!({
            "changes": [
                { "target": "file:chain.rs", "mode": "create", "payload": { "content": "first\n" } },
                { "target": "file:chain.rs", "mode": "patch", "payload": { "old_string": "first", "new_string": "second" } },
                { "target": "file:other.rs", "mode": "patch", "payload": { "old_string": "missing", "new_string": "new other" } }
            ],
            "commit_msg": "chain compose"
        }),
    )
    .await;

    assert_eq!(report["applied"].as_array().unwrap().len(), 2);
    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert_eq!(repo.read("chain.rs"), "second\n");
    assert_eq!(repo.read("other.rs"), "old other\n");
}

#[tokio::test]
async fn change_atomic_true_preserves_file_group_fail_fast() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    seed_bad_good_files(&repo);

    let report = repo
        .change_report(bad_good_patch_payload("atomic fail fast", true))
        .await;

    assert_eq!(report["applied"].as_array().unwrap().len(), 0);
    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert_eq!(report["transactional"], true);
    assert_eq!(repo.read("bad.rs"), "old bad\n");
    assert_eq!(repo.read("good.rs"), "old good\n");
}

#[tokio::test]
async fn change_atomic_promote_amend_rolls_back_decision() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("canon.md", "old canon\n");
    repo.seal_base("initial canon");
    let project_id = common::create_project(&repo.orch.db.local, "MCP").await;
    repo.orch
        .db
        .local
        .execute_script(&format!(
            "\
            INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)\
             VALUES ('issue-atomic-promote', '{project_id}', 1, 'Promote memory', 'active', 1, 1);\
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)\
             VALUES ('exec-atomic-promote', 'recipe', 'issue-atomic-promote', '{project_id}', 'running', 1, 1);\
            INSERT INTO jobs(id, execution_id, issue_id, project_id, recipe_node_id, node_name, uri_segment, status, created_at, updated_at)\
             VALUES ('job-atomic-promote', 'exec-atomic-promote', 'issue-atomic-promote', '{project_id}', 'builder', 'builder', 'builder', 'running', 1, 1);\
            INSERT INTO memories (id, name, project_id, content, status, scope, scope_value, job_id, node_seq, created_at, updated_at)\
             VALUES ('atomic-promote-memory', 'atomic promote', '{project_id}', 'promote me', 'claimed', 'project', '{project_id}', 'job-atomic-promote', 1, 1, 1);"
        ))
        .await
        .unwrap();

    let report = repo
        .change_report(json!({
            "changes": [
                {
                    "target": "file:canon.md",
                    "mode": "patch",
                    "payload": { "old_string": "old canon", "new_string": "new canon" }
                },
                {
                    "target": "cairn://p/MCP/1/1/builder/memories/1",
                    "mode": "patch",
                    "payload": { "action": "promote", "reason": "ready for canon" }
                }
            ],
            "commit_msg": "^",
            "atomic": true
        }))
        .await;

    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert_eq!(report["transactional"], true);
    let error = report["failures"][0]["error"].as_str().unwrap();
    assert!(
        error.contains("promote_memory cannot ride an amend"),
        "{error}"
    );

    let (status, triage_decision, reason, promoted_commit_sha) = repo
        .orch
        .db
        .local
        .query_one(
            "SELECT status, triage_decision, reason, promoted_commit_sha FROM memories WHERE id = 'atomic-promote-memory'",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    row.opt_text(1)?,
                    row.opt_text(2)?,
                    row.opt_text(3)?,
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(status, "claimed");
    assert!(triage_decision.is_none());
    assert!(reason.is_none());
    assert!(promoted_commit_sha.is_none());
}

#[tokio::test]
async fn node_memory_append_uses_target_node_and_scope_owner() {
    let orch = test_file_change_orchestrator().await;
    orch.db
        .local
        .execute(
            "INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
             VALUES ('workspace', 'default', 'Workspace', 'WS', '/tmp/workspace', 1, 1, 1)",
            (),
        )
        .await
        .unwrap();
    let project_id = common::create_project(&orch.db.local, "MCP").await;
    let issue_id = insert_issue(&orch.db.local, &project_id, 1, "Memory target").await;
    orch.db
        .local
        .execute_script(&format!(
            "\
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)\
             VALUES ('exec-memory-target', 'recipe', '{issue_id}', '{project_id}', 'running', 1, 1);\
            INSERT INTO jobs(id, execution_id, issue_id, project_id, recipe_node_id, node_name, uri_segment, status, created_at, updated_at)\
             VALUES ('job-builder', 'exec-memory-target', '{issue_id}', '{project_id}', 'builder', 'builder', 'builder', 'running', 1, 1);\
            INSERT INTO jobs(id, execution_id, issue_id, project_id, recipe_node_id, node_name, uri_segment, status, created_at, updated_at)\
             VALUES ('job-reviewer', 'exec-memory-target', '{issue_id}', '{project_id}', 'reviewer', 'reviewer', 'reviewer', 'running', 1, 1);"
        ))
        .await
        .unwrap();

    let request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/MCP/1/1/reviewer/memories",
                "mode": "append",
                "payload": {
                    "name": "workspace fact",
                    "content": "Workspace-scoped memory should live with workspace canon",
                    "scope": "workspace"
                }
            }]
        }),
    );

    let report = parse_report(&handle_write(&orch, &request).await);
    assert_successful_change(&report, 1);
    let summary = report["applied"][0]["summary"].as_str().unwrap();
    assert!(
        summary.contains("cairn://p/MCP/1/1/reviewer/memories/1"),
        "{summary}"
    );
    let resolved = memory_db::resolve_node_memory_id(&orch.db.local, "MCP", 1, 1, "reviewer", 1)
        .await
        .unwrap();
    let memory_id = resolved.expect("returned node URI resolves");
    let (job_id, project_id, scope, scope_value) = orch
        .db
        .local
        .query_one(
            "SELECT job_id, project_id, scope, scope_value FROM memories WHERE id = ?1",
            params![memory_id.as_str()],
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?, row.text(3)?)),
        )
        .await
        .unwrap();
    assert_eq!(job_id, "job-reviewer");
    assert_eq!(project_id, "workspace");
    assert_eq!(scope, "workspace");
    assert_eq!(scope_value, "workspace");
}

#[tokio::test]
async fn change_default_non_atomic_resource_batch_continues_after_failure() {
    let orch = test_file_change_orchestrator().await;
    common::create_project(&orch.db.local, "MCP").await;

    let request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [
                { "target": "cairn://p/MISSING/messages", "mode": "append", "payload": { "content": "Lost" } },
                { "target": "cairn://p/MCP/messages", "mode": "append", "payload": { "content": "Applied after failure" } }
            ]
        }),
    );

    let report = parse_report(&handle_write(&orch, &request).await);

    assert_eq!(report["applied"].as_array().unwrap().len(), 1);
    assert_eq!(report["applied"][0]["index"], 1);
    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert_eq!(report["failures"][0]["index"], 0);
    assert_eq!(
        count_rows(
            &orch.db.local,
            "SELECT COUNT(*) FROM messages WHERE channel_type = 'project' AND channel_id = 'MCP' AND content = 'Applied after failure'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn change_default_non_atomic_failures_are_reported_with_blocking_appends() {
    let orch = test_file_change_orchestrator().await;

    let request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [
                { "target": "cairn://p/MISSING/messages", "mode": "append", "payload": { "content": "Lost" } },
                { "target": "cairn://p/MCP/1/1/builder/tasks", "mode": "append", "payload": { "subagentType": "Explore", "description": "noop", "prompt": "noop" } }
            ]
        }),
    );

    let result = handle_write(&orch, &request).await;
    let report = parse_report(result.split("\n\n").next().unwrap());

    assert_eq!(report["applied"].as_array().unwrap().len(), 0);
    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert_eq!(report["failures"][0]["index"], 0);
    assert!(result.contains("No project found with key 'MISSING'"));
}

#[tokio::test]
async fn change_rejects_file_target_without_commit_msg() {
    // A file-target change with no commit_msg must be rejected before anything is
    // written — uncommitted worktree edits are lost if the worktree is cleaned up.
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().to_str().unwrap();
    let orch = test_file_change_orchestrator().await;

    let request = make_request(
        cwd,
        json!({
            "changes": [{
                "target": "file:new.rs",
                "mode": "create",
                "content": "let x = 1;\n"
            }]
        }),
    );

    let result = handle_write(&orch, &request).await;
    let report = parse_report(&result);

    assert_eq!(failure_count(&report), 1, "{report:?}");
    let error = report["failures"][0]["error"].as_str().unwrap();
    assert!(error.contains("commit_msg"), "{error}");
    assert_eq!(report["applied"].as_array().unwrap().len(), 0);
    // Nothing should have been written to disk.
    assert!(!dir.path().join("new.rs").exists());
}

#[tokio::test]
async fn change_allows_resource_only_batch_without_commit_msg() {
    // Resource-only batches never touch git, so they must still work without a
    // commit_msg.
    let orch = test_file_change_orchestrator().await;
    common::create_project(&orch.db.local, "MCP").await;

    let request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/MCP/issues",
                "mode": "append",
                "payload": { "title": "No commit msg needed" }
            }]
        }),
    );

    let result = handle_write(&orch, &request).await;
    let report = parse_report(&result);

    assert_eq!(failure_count(&report), 0, "{report:?}");
    assert_eq!(report["applied"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn change_rejects_malformed_diff_before_apply() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lib.rs", "let x = 1;\n");

    let report = repo
        .change_report(json!({
            "changes": [{
                "target": "file:lib.rs",
                "mode": "patch",
                "payload": {
                    "diff": "*** Begin Patch\n*** Update File: file:lib.rs\n-let x = 1;\n+let x = 2;\n*** End Patch\n"
                }
            }],
            "commit_msg": "malformed diff"
        }))
        .await;

    assert_eq!(failure_count(&report), 1);
    let error = report["failures"][0]["error"].as_str().unwrap();
    assert!(error.contains("Invalid diff"));
    assert!(error.contains("unified diff") || error.contains("single-file envelope"));
    assert!(!error.contains("No hunks found in diff"));
}

#[tokio::test]
async fn change_creates_issue_resource_without_nested_runtime_panic() {
    let orch = test_file_change_orchestrator().await;
    common::create_project(&orch.db.local, "MCP").await;

    let request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/MCP/issues",
                "mode": "append",
                "payload": {
                    "title": "Created through change",
                    "description": "Created by regression test"
                }
            }]
        }),
    );

    let result = handle_write(&orch, &request).await;
    let report = parse_report(&result);

    assert_eq!(report["applied"].as_array().unwrap().len(), 1);
    assert_eq!(failure_count(&report), 0, "{report:?}");
    assert!(report["applied"][0]["summary"]
        .as_str()
        .unwrap()
        .contains("Created issue MCP-"));
    assert_eq!(
        count_rows(
            &orch.db.local,
            "SELECT COUNT(*) FROM issues WHERE title = 'Created through change'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn change_appends_issue_comment_without_nested_runtime_panic() {
    let orch = test_file_change_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "MCP").await;
    insert_issue(&orch.db.local, &project_id, 1, "Comment target").await;

    let request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/MCP/1",
                "mode": "append",
                "payload": { "content": "Regression comment" }
            }]
        }),
    );

    let result = handle_write(&orch, &request).await;
    let report = parse_report(&result);

    assert_eq!(report["applied"].as_array().unwrap().len(), 1);
    assert_eq!(failure_count(&report), 0, "{report:?}");
    assert_eq!(
        report["applied"][0]["summary"].as_str().unwrap(),
        "Appended comment to issue MCP-1"
    );
    assert_eq!(
        count_rows(
            &orch.db.local,
            "SELECT COUNT(*) FROM comments c JOIN issues i ON c.issue_id = i.id WHERE i.number = 1 AND c.content = 'Regression comment'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn change_appends_project_and_issue_messages_without_nested_runtime_panic() {
    let orch = test_file_change_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "MCP").await;
    insert_issue(&orch.db.local, &project_id, 1, "Message target").await;

    let project_request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/MCP/messages",
                "mode": "append",
                "payload": { "content": "Project regression message" }
            }]
        }),
    );
    let issue_request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/MCP/1/messages",
                "mode": "append",
                "payload": { "content": "Issue regression message" }
            }]
        }),
    );

    let project_report = parse_report(&handle_write(&orch, &project_request).await);
    let issue_report = parse_report(&handle_write(&orch, &issue_request).await);

    assert_eq!(failure_count(&project_report), 0, "{project_report:?}");
    assert_eq!(failure_count(&issue_report), 0, "{issue_report:?}");
    assert_eq!(
        count_rows(
            &orch.db.local,
            "SELECT COUNT(*) FROM messages WHERE channel_type = 'project' AND channel_id = 'MCP' AND content = 'Project regression message'"
        )
        .await,
        1
    );
    assert_eq!(
        count_rows(
            &orch.db.local,
            "SELECT COUNT(*) FROM messages WHERE channel_type = 'issue' AND channel_id = 'MCP/1' AND content = 'Issue regression message'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn change_patches_issue_resource_without_nested_runtime_panic() {
    let orch = test_file_change_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "MCP").await;
    insert_issue(&orch.db.local, &project_id, 1, "Patch target").await;

    let request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/MCP/1",
                "mode": "patch",
                "payload": { "title": "Patched through change" }
            }]
        }),
    );

    let result = handle_write(&orch, &request).await;
    let report = parse_report(&result);

    assert_eq!(report["applied"].as_array().unwrap().len(), 1);
    assert_eq!(failure_count(&report), 0, "{report:?}");
    assert_eq!(
        report["applied"][0]["summary"].as_str().unwrap(),
        "Patched issue MCP-1"
    );
    assert_eq!(
        count_rows(
            &orch.db.local,
            "SELECT COUNT(*) FROM issues WHERE number = 1 AND title = 'Patched through change'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn stale_seal_recovers_or_reverts_multi_file_batch_cleanly() {
    // The direct regression for the reported multi-patch write loss. The base
    // advances mid-batch and rewrites this workspace's `@` out from under it, so
    // the seal hits "working copy is stale" AND the old restore would too. The
    // fix must either recover the batch onto the advanced base (Phase 2) or
    // cleanly revert (Phase 1) — never strand the applied edits uncommitted.
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!(
            "skipping stale_seal_recovers_or_reverts_multi_file_batch_cleanly: jj not resolvable"
        );
        return;
    };
    repo.seal_base("base");
    repo.make_stale_via_sibling();

    let report = repo
        .change_report(json!({
            "changes": [
                {"target": "file:alpha.rs", "mode": "create", "payload": {"content": "fn alpha() {}\n"}},
                {"target": "file:beta.rs", "mode": "create", "payload": {"content": "fn beta() {}\n"}},
                {"target": "file:gamma.rs", "mode": "create", "payload": {"content": "fn gamma() {}\n"}}
            ],
            "commit_msg": "add alpha beta gamma"
        }))
        .await;

    let committed = report["commit"]["status"] == "committed";
    if committed {
        // Phase 2: the batch landed on the advanced base. Its files are present,
        // and the sibling's file is too (recovery rebased onto the advanced tip).
        assert_eq!(
            failure_count(&report),
            0,
            "a recovered commit has no failures: {report:?}"
        );
        assert!(
            repo.path("alpha.rs").exists()
                && repo.path("beta.rs").exists()
                && repo.path("gamma.rs").exists(),
            "the recovered batch's files are present: {report:?}"
        );
        assert!(
            repo.path("sibling-advance.txt").exists(),
            "recovery rebased the batch onto the advanced sibling base"
        );
    } else {
        // Phase 1 fallback: a clean revert leaves no orphaned uncommitted dirt.
        assert!(
            !repo.path("alpha.rs").exists(),
            "a reverted batch must not strand its files on disk: {report:?}"
        );
    }

    // In BOTH outcomes the worktree ends clean and non-stale — equal to a real
    // commit, never orphaned dirt. `is_working_copy_dirty` errors if still stale,
    // so Ok(false) also asserts staleness was cleared.
    let jj = JjEnv::resolve("jj", repo.config.path());
    assert_eq!(
        jj::is_working_copy_dirty(&jj, repo.dir.path()),
        Ok(false),
        "worktree clean and non-stale after the batch"
    );
}
