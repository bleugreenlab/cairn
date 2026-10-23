use super::*;
use cairn_codec::testutil::{commit_all, init_repo, write_file};

fn selector(globs: &[&str]) -> InputSelector {
    InputSelector::from_globs(
        &globs
            .iter()
            .map(|glob| glob.to_string())
            .collect::<Vec<_>>(),
    )
}

/// The slice holds exactly the entries the cache key hashes: the same
/// selection over the same tree, so a slice check can see only what its
/// verdict is keyed on.
#[test]
fn the_slice_is_exactly_the_hashed_selection() {
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    write_file(repo.path(), "pins/codex-pin.json", b"{\"v\":1}\n");
    write_file(repo.path(), "scripts/check.ts", b"console.log('ok')\n");
    write_file(repo.path(), "src/unrelated.rs", b"fn main() {}\n");
    let commit = commit_all(repo.path(), "seed");
    let globs = selector(&["pins/**", "scripts/check.ts"]);

    let out = tempfile::tempdir().unwrap();
    let root = out.path().join("tree");
    let files = materialize(repo.path(), &commit, &globs, &root).unwrap();
    assert_eq!(files, 2);
    assert_eq!(
        std::fs::read(root.join("pins/codex-pin.json")).unwrap(),
        b"{\"v\":1}\n"
    );
    assert!(root.join("scripts/check.ts").is_file());
    assert!(!root.join("src").exists(), "an unselected path is absent");

    let store = ObjectStore::new(repo.path(), None).unwrap();
    let entries: Vec<(String, String)> = store
        .walk_commit(&commit, "", &ObjectReadLimits::default())
        .unwrap()
        .into_iter()
        .filter(|item| item.kind == TreeEntryKind::Blob)
        .map(|item| (item.path, item.oid))
        .collect();
    let hashed: Vec<&str> = selected_entries(&entries, &globs)
        .into_iter()
        .map(|(path, _)| path.as_str())
        .collect();
    assert_eq!(hashed, vec!["pins/codex-pin.json", "scripts/check.ts"]);
}

/// The cache key is computed over the production tree listing
/// (`jj::tree_entries`, `git ls-tree -r`), not over the gix walk the slice is
/// built from. The two listings must select the same files, or "the slice is
/// the key" holds only in the test.
#[test]
fn the_slice_matches_the_listing_the_key_is_hashed_from() {
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    write_file(repo.path(), "pins/a.json", b"{}\n");
    write_file(repo.path(), "pins/nested/b.json", b"[]\n");
    write_file(repo.path(), "scripts/check.ts", b"1\n");
    write_file(repo.path(), "other/c.txt", b"c\n");
    let commit = commit_all(repo.path(), "seed");
    let globs = selector(&["pins/**", "scripts/check.ts"]);

    let config = tempfile::tempdir().unwrap();
    let jj = crate::jj::environment("jj", config.path());
    let production = crate::jj::tree_entries(&jj, repo.path(), &commit).unwrap();
    let keyed: Vec<String> = selected_entries(&production, &globs)
        .into_iter()
        .map(|(path, _)| path.clone())
        .collect();

    let out = tempfile::tempdir().unwrap();
    let root = out.path().join("tree");
    materialize(repo.path(), &commit, &globs, &root).unwrap();
    let mut sliced: Vec<String> = walkdir(&root)
        .into_iter()
        .map(|path| {
            path.strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect();
    sliced.sort();
    assert_eq!(sliced, keyed);
    assert_eq!(keyed.len(), 3);
}

fn walkdir(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(walkdir(&path));
        } else {
            files.push(path);
        }
    }
    files
}

/// A check that reads a file it did not declare fails loudly, rather than
/// passing on a cache key that never covered that file.
#[test]
fn an_undeclared_input_is_absent_and_the_command_fails() {
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    write_file(repo.path(), "declared.txt", b"yes\n");
    write_file(repo.path(), "undeclared.txt", b"no\n");
    let commit = commit_all(repo.path(), "seed");
    let out = tempfile::tempdir().unwrap();
    let root = out.path().join("tree");
    materialize(repo.path(), &commit, &selector(&["declared.txt"]), &root).unwrap();
    let status = |script: &str| {
        std::process::Command::new("bash")
            .arg("-c")
            .arg(script)
            .current_dir(&root)
            .status()
            .unwrap()
            .success()
    };
    assert!(status("cat declared.txt"));
    assert!(!status("cat undeclared.txt"));
}

#[cfg(unix)]
#[test]
fn the_slice_keeps_executable_modes() {
    use std::os::unix::fs::PermissionsExt as _;
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    write_file(repo.path(), "run.sh", b"#!/bin/sh\necho hi\n");
    std::fs::set_permissions(
        repo.path().join("run.sh"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    let commit = commit_all(repo.path(), "seed");
    let out = tempfile::tempdir().unwrap();
    let root = out.path().join("tree");
    materialize(repo.path(), &commit, &selector(&["run.sh"]), &root).unwrap();
    let mode = std::fs::metadata(root.join("run.sh"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o111, 0o111);
}

#[test]
fn a_selection_over_the_file_bound_is_a_declaration_to_fix() {
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    for index in 0..=MAX_SLICE_FILES {
        write_file(repo.path(), &format!("many/{index}.txt"), b"x");
    }
    let commit = commit_all(repo.path(), "seed");
    let out = tempfile::tempdir().unwrap();
    assert!(matches!(
        materialize(
            repo.path(),
            &commit,
            &selector(&["many/**"]),
            &out.path().join("tree")
        ),
        Err(SliceError::TooLarge(_))
    ));
}

#[test]
fn an_unrunnable_tree_impact_declaration_is_a_per_check_config_error() {
    let check = |yaml: &str| -> crate::config::project_settings::CheckCommand {
        serde_yaml::from_str(yaml).unwrap()
    };
    assert!(impact_tree_config_error(&check("command: x\ntree: impact\nimpact: [a]\n")).is_none());
    assert!(impact_tree_config_error(&check("command: x\nimpact: [a]\n")).is_none());
    for invalid in [
        "command: x\ntree: impact\n",
        "command: x\ntree: impact\nimpact: []\n",
        "command: x\ntree: impact\nimpact: [a]\nscope: rust:cairn-core\n",
        "command: x\ntree: impact\nimpact: [a]\nfixes: true\n",
    ] {
        assert!(
            impact_tree_config_error(&check(invalid)).is_some(),
            "{invalid}"
        );
    }
    assert!(
        serde_yaml::from_str::<crate::config::project_settings::CheckCommand>(
            "command: x\ntree: none\n"
        )
        .is_err()
    );
}

#[test]
fn only_an_unpinned_or_same_platform_selector_admits_the_runner() {
    assert!(selector_admits_runner(None));
    let other_os = if std::env::consts::OS == "linux" {
        "macos"
    } else {
        "linux"
    };
    let pinned = |name: Option<&str>, os: Option<&str>| {
        let mut selector: ExecutorSelector = serde_json::from_value(serde_json::json!({})).unwrap();
        selector.name = name.map(str::to_string);
        selector.os = os.map(str::to_string);
        selector
    };
    assert!(selector_admits_runner(Some(&pinned(None, None))));
    assert!(selector_admits_runner(Some(&pinned(
        None,
        Some(std::env::consts::OS)
    ))));
    assert!(!selector_admits_runner(Some(&pinned(None, Some(other_os)))));
    assert!(!selector_admits_runner(Some(&pinned(Some("fedora"), None))));
}

/// The account a slice lane records names the runner as the colocated
/// executor, so its observation is the coordinator's own (environment and
/// toolchain from this process) rather than an unknown remote's.
#[test]
fn the_runner_account_is_the_colocated_executor() {
    let account = runner_account(1_000, 250);
    assert_eq!(account.executor_id, crate::fleet::COLOCATED_EXECUTOR_ID);
    assert_eq!(account.finished_at_unix_ms, 1_250);
    assert_eq!(
        account.verdict_platform.as_deref(),
        Some(std::env::consts::OS)
    );
    assert!(account.environment_fingerprint.is_empty());
}

fn lane_with_env(
    batch_env: Vec<(String, String)>,
    item_env: Vec<(String, String)>,
) -> (PlannedCheckBatchRequest, PlannedCheckBatchItem) {
    let item = PlannedCheckBatchItem {
        index: 0,
        name: "migrations".into(),
        input_hash: "hash".into(),
        resource_identity: CheckResourceIdentityInput::Checkpoint {
            name: "migrations".into(),
            command: "true".into(),
        },
        command: "true".into(),
        stream_id: "stream".into(),
        env: item_env,
        verdict_environment_names: Vec::new(),
        timeout_ms: 1,
        executor: None,
        verdict_platforms: Vec::new(),
        resource_class: crate::config::project_settings::CheckResourceClass::Shared,
        infra_scopes: crate::execution::cache::InfraScopes::head_only("job", "commit"),
        avoid_executor_ids: Vec::new(),
    };
    let batch = PlannedCheckBatchRequest {
        project_id: "project".into(),
        repository: "/repo".into(),
        store_dir: PathBuf::from("/store"),
        sealed_commit: "commit".into(),
        requesting_job_id: "job".into(),
        owner: cairn_common::executor_protocol::CellOwnerRef {
            project_id: "project".into(),
            project_key: None,
            issue_number: None,
            job_id: Some("job".into()),
            execution_seq: None,
            node_kind: None,
        },
        affinity_key: Some("job".into()),
        priority: crate::fleet::CellPriority::WriteCheck,
        env: batch_env,
        items: vec![item.clone()],
        run_context: None,
        mutation_policy: crate::fleet::MutationPolicy::AllowDelta,
        status_board: None,
        purpose: None,
    };
    (batch, item)
}

fn var(key: &str, value: &str) -> (String, String) {
    (key.to_string(), value.to_string())
}

/// A transport carried by both the batch and the item reaches the command
/// once, as a file path, holding the item's content. A surviving second copy
/// went to `execve` whole, and an executor base-content bundle alone is past
/// ARG_MAX.
#[test]
fn a_transport_on_both_batch_and_item_is_spilled_once_with_the_item_winning() {
    let scratch = tempfile::tempdir().unwrap();
    let (batch, item) = lane_with_env(
        vec![
            var("SHELL_VAR", "batch"),
            var("CAIRN_CHECK_BASE_CONTENT_CONTENT", "{\"from\":\"batch\"}"),
            var("CAIRN_CHECK_CHANGED_FILES_CONTENT", "batch.rs"),
        ],
        vec![
            var("SHELL_VAR", "item"),
            var("CAIRN_CHECK_BASE_CONTENT_CONTENT", "{\"from\":\"item\"}"),
            var("CAIRN_CHECK_CHANGED_FILES_CONTENT", "item.rs"),
        ],
    );
    let env = lane_env(&batch, &item, scratch.path()).unwrap();

    let named = |key: &str| -> Vec<&str> {
        env.iter()
            .filter(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
            .collect()
    };
    for (content_env, _) in CONTENT_TRANSPORTS {
        assert!(
            named(content_env).is_empty(),
            "{content_env} reached the env"
        );
    }
    let base = named("CAIRN_CHECK_BASE_CONTENT");
    assert_eq!(base.len(), 1, "exactly one base-content path: {base:?}");
    assert_eq!(
        std::fs::read_to_string(base[0]).unwrap(),
        "{\"from\":\"item\"}"
    );
    let changed = named("CAIRN_CHECK_CHANGED_FILES");
    assert_eq!(changed.len(), 1);
    assert_eq!(std::fs::read_to_string(changed[0]).unwrap(), "item.rs");
    assert_eq!(named("SHELL_VAR"), vec!["item"]);
}

/// The regression is caught by size, not by spawning: a bundle past 1 MiB on
/// both the batch and the item leaves a lane environment of paths and the
/// runner's own small variables.
#[test]
fn a_bundle_past_a_mebibyte_leaves_a_small_lane_env() {
    let scratch = tempfile::tempdir().unwrap();
    let bundle = "x".repeat(2 * 1024 * 1024);
    let (batch, item) = lane_with_env(
        vec![var("CAIRN_CHECK_BASE_CONTENT_CONTENT", &bundle)],
        vec![var("CAIRN_CHECK_BASE_CONTENT_CONTENT", &bundle)],
    );
    let env = lane_env(&batch, &item, scratch.path()).unwrap();
    let footprint = EnvFootprint::of(&env);
    assert!(
        footprint.bytes < 64 * 1024,
        "the lane env is {} bytes, largest {:?}",
        footprint.bytes,
        footprint.largest
    );
}

/// An `Argument list too long` names how large the lane's environment was and
/// which variable dominated it.
#[cfg(unix)]
#[test]
fn an_oversized_spawn_names_the_env_that_was_too_long() {
    let env = vec![
        var("SMALL", "1"),
        var("CAIRN_CHECK_BASE_CONTENT_CONTENT", &"x".repeat(4096)),
    ];
    let footprint = EnvFootprint::of(&env);
    assert_eq!(footprint.variables, 2);
    assert_eq!(footprint.bytes, (5 + 1 + 2) + (32 + 4096 + 2));
    let error = std::io::Error::from_raw_os_error(nix::errno::Errno::E2BIG as i32);
    let message = spawn_failure(&error, "bun run check", &footprint);
    assert!(
        message.contains("2 environment variables totalling 4138 bytes"),
        "{message}"
    );
    assert!(
        message.contains("`CAIRN_CHECK_BASE_CONTENT_CONTENT`, 4130 bytes"),
        "{message}"
    );
    assert!(message.contains("13-byte command"), "{message}");

    let other = std::io::Error::from_raw_os_error(nix::errno::Errno::ENOENT as i32);
    assert!(!spawn_failure(&other, "bun", &footprint).contains("ARG_MAX"));
}
