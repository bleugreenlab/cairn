mod common;

use std::sync::Arc;

use cairn_core::internal::db::DbState;
use cairn_core::internal::mcp::handlers::files::handle_change;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::storage::{LocalDb, RowExt, SearchIndex};
use cairn_core::memories::db as memory_db;
use serde_json::json;
use turso::params;

fn make_request(cwd: &str, payload: serde_json::Value) -> McpCallbackRequest {
    McpCallbackRequest {
        cwd: cwd.to_string(),
        run_id: None,
        tool: "write".to_string(),
        payload,
        tool_use_id: None,
    }
}

fn make_preview_request(cwd: &str, payload: serde_json::Value) -> McpCallbackRequest {
    McpCallbackRequest {
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
    parse_report(&handle_change(orch, &make_request(cwd, payload)).await)
}

struct ChangeTestRepo {
    dir: tempfile::TempDir,
    orch: Orchestrator,
}

impl ChangeTestRepo {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        Self {
            dir,
            orch: test_file_change_orchestrator().await,
        }
    }

    fn cwd(&self) -> &str {
        self.dir.path().to_str().unwrap()
    }

    fn root(&self) -> &std::path::Path {
        self.dir.path()
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
        parse_report(&handle_change(&self.orch, &make_preview_request(self.cwd(), payload)).await)
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
    let (_db_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    Orchestrator::builder(db_state, services, temp.path().join("config")).build()
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
                "INSERT INTO runs(id, project_id, job_id, status, session_id, backend, created_at, updated_at, start_mode)
                 VALUES ('run-preview', ?1, 'job-preview', 'live', 'session-preview', 'codex', 1, 1, 'resume')",
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

fn git(path: &std::path::Path, args: &[&str]) -> std::process::Output {
    std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .output()
        .unwrap()
}

fn porcelain(path: &std::path::Path) -> String {
    let output = git(path, &["status", "--porcelain"]);
    assert!(output.status.success(), "git status failed: {output:?}");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// Drive `root` into a conflicted mid-merge state so `MERGE_HEAD` is present and
/// `is_repo_mid_transition` reports true.
fn start_merge_conflict(root: &std::path::Path) {
    std::fs::write(root.join("conflict.txt"), "base\n").unwrap();
    assert!(git(root, &["add", "-A"]).status.success());
    assert!(git(root, &["commit", "-m", "base"]).status.success());
    let base = String::from_utf8_lossy(&git(root, &["rev-parse", "--abbrev-ref", "HEAD"]).stdout)
        .trim()
        .to_string();
    assert!(git(root, &["checkout", "-b", "feature"]).status.success());
    std::fs::write(root.join("conflict.txt"), "feature\n").unwrap();
    git(root, &["add", "-A"]);
    git(root, &["commit", "-m", "feature"]);
    assert!(git(root, &["checkout", &base]).status.success());
    std::fs::write(root.join("conflict.txt"), "mainline\n").unwrap();
    git(root, &["add", "-A"]);
    git(root, &["commit", "-m", "mainline"]);
    let merge = git(root, &["merge", "feature"]);
    assert!(!merge.status.success(), "merge should conflict: {merge:?}");
}

#[tokio::test]
async fn change_patches_file_with_codex_patch_envelope() {
    let repo = ChangeTestRepo::new().await;
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
    let repo = ChangeTestRepo::new().await;

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
    let repo = ChangeTestRepo::new().await;
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
    let repo = ChangeTestRepo::new().await;
    repo.write("old.rs", "old();\n");
    // Track the file so deleting it is a real, committable change.
    git(repo.root(), &["add", "-A"]);
    git(repo.root(), &["commit", "-m", "seed old.rs"]);

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
    let repo = ChangeTestRepo::new().await;
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
    let repo = ChangeTestRepo::new().await;
    repo.write("lib.rs", "old();\n");
    let payload = json!({
        "changes": [{
            "target": "file:",
            "mode": "unified_patch",
            "payload": { "patch": "*** Begin Patch\n*** Add File: add.rs\n+add();\n*** Update File: lib.rs\n@@ -1,1 +1,1 @@\n-old();\n+new();\n*** End Patch\n" }
        }],
        "commit_msg": "NO_COMMIT",
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
    let repo = ChangeTestRepo::new().await;
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
    let repo = ChangeTestRepo::new().await;
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
    let repo = ChangeTestRepo::new().await;
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
    let repo = ChangeTestRepo::new().await;
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

    let report = parse_report(&handle_change(&orch, &request).await);

    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert!(report["failures"][0]["error"]
        .as_str()
        .unwrap()
        .contains("commit_msg"));
    assert!(!dir.path().join("new.rs").exists());
}

#[tokio::test]
async fn change_unified_patch_rejects_single_file_target_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().to_str().unwrap();
    let orch = test_file_change_orchestrator().await;

    let request = make_request(
        cwd,
        json!({
            "changes": [{
                "target": "file:expected.rs",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Add File: actual.rs\n+actual();\n*** End Patch\n" }
            }],
            "commit_msg": "NO_COMMIT"
        }),
    );

    let report = parse_report(&handle_change(&orch, &request).await);

    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert!(report["failures"][0]["error"]
        .as_str()
        .unwrap()
        .contains("envelope target path does not match change.target"));
    assert!(!dir.path().join("actual.rs").exists());
}

#[tokio::test]
async fn change_mixed_unified_patch_and_resource_batch_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    init_git_repo(dir.path());
    let cwd = dir.path().to_str().unwrap();
    let orch = test_file_change_orchestrator().await;
    common::create_project(&orch.db.local, "MCP").await;

    let request = make_request(
        cwd,
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

    let report = parse_report(&handle_change(&orch, &request).await);

    assert_eq!(report["applied"].as_array().unwrap().len(), 2);
    assert_eq!(failure_count(&report), 0, "{report:?}");
    assert_eq!(
        std::fs::read_to_string(dir.path().join("mixed.rs")).unwrap(),
        "mixed();"
    );
    assert_eq!(
        count_rows(
            &orch.db.local,
            "SELECT COUNT(*) FROM messages WHERE channel_type = 'project' AND channel_id = 'MCP' AND content = 'Unified patch landed'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn change_default_non_atomic_applies_matching_file_items() {
    let repo = ChangeTestRepo::new().await;
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
    let repo = ChangeTestRepo::new().await;
    seed_bad_good_files(&repo);

    let report = repo
        .change_report(bad_good_patch_payload("commit good only", false))
        .await;

    assert_eq!(report["commit"]["status"], "committed");
    assert_eq!(failure_count(&report), 1, "{report:?}");
    let show = std::process::Command::new("git")
        .args(["show", "--name-only", "--format="])
        .current_dir(repo.root())
        .output()
        .unwrap();
    assert!(show.status.success(), "git show failed: {show:?}");
    let names = String::from_utf8_lossy(&show.stdout);
    assert!(names.contains("good.rs"), "{names}");
    assert!(!names.contains("bad.rs"), "{names}");
}

#[tokio::test]
async fn change_default_non_atomic_chained_file_items_compose() {
    let repo = ChangeTestRepo::new().await;
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
    let repo = ChangeTestRepo::new().await;
    seed_bad_good_files(&repo);

    let report = repo
        .change_report(bad_good_patch_payload("NO_COMMIT", true))
        .await;

    assert_eq!(report["applied"].as_array().unwrap().len(), 0);
    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert_eq!(report["transactional"], true);
    assert_eq!(repo.read("bad.rs"), "old bad\n");
    assert_eq!(repo.read("good.rs"), "old good\n");
}

#[tokio::test]
async fn change_no_commit_rejected_outside_transition_restores_worktree() {
    // NO_COMMIT is only legitimate while resolving an in-progress merge or
    // rebase. Outside a transition the barrier rejects the batch and restores the
    // worktree to HEAD, so the applied-then-declined edits never linger on disk.
    let repo = ChangeTestRepo::new().await;

    let report = repo
        .change_report(json!({
            "changes": [{
                "target": "file:scratch.rs",
                "mode": "create",
                "payload": { "content": "let scratch = 1;\n" }
            }],
            "commit_msg": "NO_COMMIT"
        }))
        .await;

    assert_eq!(failure_count(&report), 1, "{report:?}");
    let error = report["failures"][0]["error"].as_str().unwrap();
    assert!(
        error.contains("NO_COMMIT is only valid while resolving an in-progress merge or rebase"),
        "{error}"
    );
    assert!(error.contains("restored to HEAD"), "{error}");
    assert_eq!(report["commit"]["status"], "failed", "{report:?}");
    // The worktree was restored: the edit is gone and the tree equals HEAD.
    assert!(!repo.path("scratch.rs").exists());
    assert!(
        porcelain(repo.root()).is_empty(),
        "worktree should equal HEAD after restore"
    );
}

#[tokio::test]
async fn change_no_commit_accepted_mid_merge_preserves_conflict_state() {
    // Mid-merge, NO_COMMIT is the legitimate escape: the edit applies, no commit
    // is made, and the in-progress merge state is left intact so the agent can
    // resolve the conflict across subsequent tool calls.
    let repo = ChangeTestRepo::new().await;
    start_merge_conflict(repo.root());
    assert!(
        repo.path(".git/MERGE_HEAD").exists(),
        "precondition: repo should be mid-merge"
    );

    let report = repo
        .change_report(json!({
            "changes": [{
                "target": "file:note.rs",
                "mode": "create",
                "payload": { "content": "resolved\n" }
            }],
            "commit_msg": "NO_COMMIT"
        }))
        .await;

    assert_successful_change(&report, 1);
    assert_eq!(report["commit"]["status"], "skipped", "{report:?}");
    assert_eq!(repo.read("note.rs"), "resolved\n");
    assert!(
        repo.path(".git/MERGE_HEAD").exists(),
        "merge state should survive a NO_COMMIT change"
    );
}

#[tokio::test]
async fn change_atomic_promote_amend_rolls_back_decision() {
    let repo = ChangeTestRepo::new().await;
    repo.write("canon.md", "old canon\n");
    let initial_commit = std::process::Command::new("git")
        .args(["add", "canon.md"])
        .current_dir(repo.root())
        .status()
        .unwrap();
    assert!(initial_commit.success());
    let initial_commit = std::process::Command::new("git")
        .args(["commit", "-m", "initial canon"])
        .current_dir(repo.root())
        .status()
        .unwrap();
    assert!(initial_commit.success());
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

    let report = parse_report(&handle_change(&orch, &request).await);
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

    let report = parse_report(&handle_change(&orch, &request).await);

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

    let result = handle_change(&orch, &request).await;
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

    let result = handle_change(&orch, &request).await;
    let report = parse_report(&result);

    assert_eq!(failure_count(&report), 1, "{report:?}");
    let error = report["failures"][0]["error"].as_str().unwrap();
    assert!(error.contains("commit_msg"), "{error}");
    assert!(error.contains("NO_COMMIT"), "{error}");
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

    let result = handle_change(&orch, &request).await;
    let report = parse_report(&result);

    assert_eq!(failure_count(&report), 0, "{report:?}");
    assert_eq!(report["applied"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn change_rejects_malformed_diff_before_apply() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().to_str().unwrap();
    std::fs::write(
        dir.path().join("lib.rs"),
        "let x = 1;
",
    )
    .unwrap();
    let orch = test_file_change_orchestrator().await;

    let request = make_request(
        cwd,
        json!({
            "changes": [{
                "target": "file:lib.rs",
                "mode": "patch",
                "payload": {
                    "diff": "*** Begin Patch
*** Update File: file:lib.rs
-let x = 1;
+let x = 2;
*** End Patch
"
                }
            }],
            "commit_msg": "NO_COMMIT"
        }),
    );

    let result = handle_change(&orch, &request).await;
    let report = parse_report(&result);

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

    let result = handle_change(&orch, &request).await;
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

    let result = handle_change(&orch, &request).await;
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

    let project_report = parse_report(&handle_change(&orch, &project_request).await);
    let issue_report = parse_report(&handle_change(&orch, &issue_request).await);

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

    let result = handle_change(&orch, &request).await;
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
