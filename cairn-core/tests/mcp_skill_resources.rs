mod common;

use std::path::Path;

use common::{
    change_resource as change, config_dir, project_resource_fixture, read_resource as read,
    resource_orchestrator_fixture,
};
use serde_json::json;

fn write_workspace_skill(config_dir: &Path, id: &str, description: &str, body: &str) {
    let dir = config_dir.join("skills").join(id);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("SKILL.md"),
        format!("---\nname: {id}\ndescription: {description}\n---\n\n{body}\n"),
    )
    .unwrap();
}

#[tokio::test]
async fn reads_skill_collection_root_and_subpaths() {
    let (temp, _db, orch) = resource_orchestrator_fixture().await;
    let cfg = config_dir(&temp);

    write_workspace_skill(&cfg, "testing", "Test patterns", "Run the tests.");
    // Add a references file to exercise directory listing + file content.
    let refs = cfg.join("skills/testing/references");
    std::fs::create_dir_all(&refs).unwrap();
    std::fs::write(refs.join("a.md"), "reference body").unwrap();

    // Collection lists the skill with a workspace link.
    let collection = read(&orch, "cairn://skills").await;
    assert!(collection.contains("testing"), "collection: {collection}");
    assert!(collection.contains("cairn://skills/testing"));
    assert!(collection.contains("[workspace]"));

    // Skill root serves the SKILL.md body inline and links the references dir,
    // but not a SKILL.md sub-resource (it lives here) nor absent scripts/assets.
    let root = read(&orch, "cairn://skills/testing").await;
    assert!(root.contains("Run the tests."));
    assert!(root.contains("cairn://skills/testing/references"));
    assert!(!root.contains("cairn://skills/testing/SKILL.md"));
    assert!(!root.contains("cairn://skills/testing/scripts"));

    // The SKILL.md sub-resource redirects to the skill root.
    let skill_md = read(&orch, "cairn://skills/testing/SKILL.md").await;
    assert!(skill_md.contains("skill root"), "skill_md: {skill_md}");
    assert!(skill_md.contains("cairn://skills/testing"));

    // references/ lists entries; nested file returns content.
    let refs_listing = read(&orch, "cairn://skills/testing/references").await;
    assert!(refs_listing.contains("a.md"));
    let refs_file = read(&orch, "cairn://skills/testing/references/a.md").await;
    assert!(
        refs_file.starts_with("reference body"),
        "refs_file: {refs_file}"
    );

    // Path traversal is rejected.
    let escape = read(&orch, "cairn://skills/testing/references/../../secrets").await;
    assert!(
        escape.contains("Invalid path segment") || escape.contains("escapes"),
        "escape: {escape}"
    );
}

#[tokio::test]
async fn create_patch_delete_workspace_skill_via_change() {
    let (temp, _db, orch) = resource_orchestrator_fixture().await;
    let cfg = config_dir(&temp);

    // Create a workspace skill on the contextual collection.
    let created = change(
        &orch,
        json!([{
            "target": "cairn://skills",
            "mode": "create",
            "payload": {
                "name": "deploy-helper",
                "description": "Helps with deploys",
                "prompt": "# Deploy\n\n## Steps\n\nDo the thing."
            }
        }]),
    )
    .await;
    assert!(created.contains("\"applied\""), "create: {created}");
    assert!(cfg.join("skills/deploy-helper/SKILL.md").exists());

    // Read it back through the resource graph.
    let root = read(&orch, "cairn://skills/deploy-helper").await;
    assert!(root.contains("Helps with deploys"));

    // Patch a section of the prompt.
    let patched = change(
        &orch,
        json!([{
            "target": "cairn://skills/deploy-helper",
            "mode": "patch",
            "payload": {
                "replaceSection": { "heading": "## Steps", "content": "Do it carefully." }
            }
        }]),
    )
    .await;
    assert!(patched.contains("\"applied\""), "patch: {patched}");
    let body = read(&orch, "cairn://skills/deploy-helper").await;
    assert!(body.contains("Do it carefully."));
    assert!(!body.contains("Do the thing."));

    // Mutating a package sub-path is rejected.
    let rejected = change(
        &orch,
        json!([{
            "target": "cairn://skills/deploy-helper/SKILL.md",
            "mode": "patch",
            "payload": { "description": "nope" }
        }]),
    )
    .await;
    assert!(rejected.contains("skill root"), "rejected: {rejected}");

    // Delete removes the directory.
    let deleted = change(
        &orch,
        json!([{
            "target": "cairn://skills/deploy-helper",
            "mode": "delete",
            "payload": { "reason": "superseded" }
        }]),
    )
    .await;
    assert!(deleted.contains("\"applied\""), "delete: {deleted}");
    assert!(!cfg.join("skills/deploy-helper").exists());
}

#[tokio::test]
async fn create_skill_rejects_duplicate_and_missing_fields() {
    let (temp, _db, orch) = resource_orchestrator_fixture().await;
    let cfg = config_dir(&temp);
    write_workspace_skill(&cfg, "existing", "Already here", "Body.");

    let duplicate = change(
        &orch,
        json!([{
            "target": "cairn://skills",
            "mode": "create",
            "payload": {
                "name": "existing",
                "description": "dup",
                "prompt": "x"
            }
        }]),
    )
    .await;
    assert!(
        duplicate.contains("already exists"),
        "duplicate: {duplicate}"
    );

    let missing = change(
        &orch,
        json!([{
            "target": "cairn://skills",
            "mode": "create",
            "payload": { "name": "incomplete" }
        }]),
    )
    .await;
    assert!(
        missing.contains("description") || missing.contains("prompt"),
        "missing: {missing}"
    );
}

#[tokio::test]
async fn explicit_project_mutation_does_not_bleed_into_workspace() {
    let (temp, _db, orch, _repo) = project_resource_fixture("PROJ").await;
    let cfg = config_dir(&temp);

    // A skill that exists ONLY at workspace scope.
    write_workspace_skill(&cfg, "shared", "Workspace shared skill", "Body.");

    // Reading it through an explicit project URI must not surface the workspace skill.
    let read_proj = read(&orch, "cairn://p/PROJ/skills/shared").await;
    assert!(
        read_proj.contains("Skill not found in project PROJ"),
        "read_proj: {read_proj}"
    );

    // Patching via the explicit project URI must be rejected, not silently rewrite workspace.
    let patched = change(
        &orch,
        json!([{
            "target": "cairn://p/PROJ/skills/shared",
            "mode": "patch",
            "payload": { "description": "hijacked" }
        }]),
    )
    .await;
    assert!(
        patched.contains("Skill not found in project PROJ"),
        "patched: {patched}"
    );
    let body = std::fs::read_to_string(cfg.join("skills/shared/SKILL.md")).unwrap();
    assert!(
        body.contains("Workspace shared skill"),
        "workspace untouched: {body}"
    );

    // Deleting via the explicit project URI must be rejected and leave workspace intact.
    let deleted = change(
        &orch,
        json!([{
            "target": "cairn://p/PROJ/skills/shared",
            "mode": "delete"
        }]),
    )
    .await;
    assert!(
        deleted.contains("Skill not found in project PROJ"),
        "deleted: {deleted}"
    );
    assert!(
        cfg.join("skills/shared/SKILL.md").exists(),
        "workspace skill must still exist"
    );
}

#[tokio::test]
async fn project_scoped_skill_create_read_patch_delete_roundtrip() {
    let (_temp, _db, orch, repo) = project_resource_fixture("PROJ").await;

    let skill_md = repo.join(".cairn/skills/builder-helper/SKILL.md");

    // Create a project-scoped skill.
    let created = change(
        &orch,
        json!([{
            "target": "cairn://p/PROJ/skills",
            "mode": "create",
            "payload": {
                "name": "builder-helper",
                "description": "Project helper",
                "prompt": "# Helper\n\n## Body\n\nOriginal."
            }
        }]),
    )
    .await;
    assert!(created.contains("\"applied\""), "create: {created}");
    assert!(skill_md.exists(), "project skill file should exist");

    // Read it back via the explicit project URI; the body is served inline and
    // package links must be project-scoped (no SKILL.md sub-resource).
    let root = read(&orch, "cairn://p/PROJ/skills/builder-helper").await;
    assert!(root.contains("Project helper"), "root: {root}");
    assert!(root.contains("Original."), "root body: {root}");
    assert!(!root.contains("cairn://p/PROJ/skills/builder-helper/SKILL.md"));
    assert!(root.contains("[project]"));

    // Patch a section.
    let patched = change(
        &orch,
        json!([{
            "target": "cairn://p/PROJ/skills/builder-helper",
            "mode": "patch",
            "payload": { "replaceSection": { "heading": "## Body", "content": "Updated." } }
        }]),
    )
    .await;
    assert!(patched.contains("\"applied\""), "patch: {patched}");
    let body = std::fs::read_to_string(&skill_md).unwrap();
    assert!(
        body.contains("Updated.") && !body.contains("Original."),
        "body: {body}"
    );

    // Delete it.
    let deleted = change(
        &orch,
        json!([{
            "target": "cairn://p/PROJ/skills/builder-helper",
            "mode": "delete"
        }]),
    )
    .await;
    assert!(deleted.contains("\"applied\""), "delete: {deleted}");
    assert!(!skill_md.exists(), "project skill file should be gone");
}
