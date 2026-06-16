mod common;

use std::sync::Arc;

use cairn_core::internal::mcp::handlers::files::handle_read_file;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use common::orchestrator;
use serde_json::json;

async fn read(orch: &Orchestrator, cwd: &std::path::Path, path: String) -> String {
    handle_read_file(
        orch,
        &McpCallbackRequest {
            cwd: cwd.display().to_string(),
            run_id: None,
            tool: "read".to_string(),
            payload: json!({ "path": path }),
            tool_use_id: None,
        },
    )
    .await
}

#[tokio::test]
async fn grep_absolute_out_of_worktree_path_matches_plain_read_fence() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let orch = orchestrator(&temp, db);

    let worktree = temp.path().join("worktree");
    let outside = temp.path().join("outside");
    std::fs::create_dir_all(&worktree).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let file = outside.join("source.txt");
    std::fs::write(&file, "alpha\nneedle-parity\nomega\n").unwrap();

    let plain = read(&orch, &worktree, format!("file:{}", file.display())).await;
    assert!(plain.contains("needle-parity"), "plain read: {plain}");

    let grep = read(
        &orch,
        &worktree,
        format!(
            "file:{}?grep=needle-parity&output_mode=content",
            file.display()
        ),
    )
    .await;
    assert!(grep.contains("source.txt:2:needle-parity"), "grep: {grep}");
    assert!(
        !grep.contains("outside allowed directories"),
        "grep hit old allow-list: {grep}"
    );
}

#[tokio::test]
async fn grep_prunes_denylisted_descendants_from_broad_directory_walk() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);

    let config = temp.path().join("config");
    let parent = temp.path().join("outside-parent");
    let denied = parent.join("credentials");
    let public = parent.join("public");
    std::fs::create_dir_all(&denied).unwrap();
    std::fs::create_dir_all(&public).unwrap();
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join("settings.yaml"),
        format!("sandboxDenyRead:\n  - {}\n", denied.display()),
    )
    .unwrap();

    std::fs::write(denied.join("secret.txt"), "needle-prune secret\n").unwrap();
    std::fs::write(public.join("visible.txt"), "needle-prune visible\n").unwrap();

    let orch = orchestrator(&temp, db);
    let worktree = temp.path().join("worktree");
    std::fs::create_dir_all(&worktree).unwrap();

    let grep = read(
        &orch,
        &worktree,
        format!(
            "file:{}?grep=needle-prune&output_mode=content",
            parent.display()
        ),
    )
    .await;

    assert!(
        grep.contains("public/visible.txt:1:needle-prune visible"),
        "grep should include sibling match: {grep}"
    );
    assert!(
        !grep.contains("secret") && !grep.contains("credentials/secret.txt"),
        "grep surfaced denylisted descendant: {grep}"
    );
    assert!(
        !grep.contains("outside allowed directories"),
        "grep hit old allow-list: {grep}"
    );
}
