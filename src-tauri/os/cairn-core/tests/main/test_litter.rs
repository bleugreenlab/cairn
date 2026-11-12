//! A test process leaves nothing behind it.
//!
//! Each process seals its temp directory into a root of its own and removes that
//! root at exit (`cairn_common::test_seal`), and the Rust test runner deletes
//! the run's root after a kill. What a test puts outside it, in `/tmp`, survives
//! a killed process forever: a shared tmpfs `/tmp` once ran out of inodes on
//! per-process callback socket directories and per-test project repositories.

use std::collections::BTreeSet;
use std::path::Path;

use cairn_common::test_seal::SEALED_ENV;

/// Tests the child runs: one serves the executor callback socket, one
/// provisions a project repository for a file change.
const PROBES: [&str; 2] = [
    "effects_checkpoint_loop::rerun_rejects_non_checkpoint",
    "mcp_file_changes::change_rejects_malformed_diff_before_apply",
];

#[test]
fn a_test_process_leaves_nothing_in_its_temp_directory_or_in_tmp() {
    let temp = tempfile::tempdir().unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "--test-threads", "1"])
        .args(PROBES)
        // Unsealed, so the child seals itself under `temp` as a bare `cargo test`
        // would, and owns removing that root.
        .env_remove(SEALED_ENV)
        .env_remove("CAIRN_CELL_TMPDIR")
        .env("TMPDIR", temp.path())
        .env("TMP", temp.path())
        .env("TEMP", temp.path())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id();
    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains(&format!("{} passed", PROBES.len())),
        "the probe tests did not all run and pass:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let left: Vec<_> = std::fs::read_dir(temp.path())
        .unwrap()
        .flatten()
        .map(|entry| entry.file_name())
        .collect();
    assert!(left.is_empty(), "the child's temp directory kept {left:?}");

    let socket_dir = Path::new("/tmp").join(format!("cairn-test-cb-{pid}"));
    assert!(
        !socket_dir.exists(),
        "{} outlived its process",
        socket_dir.display()
    );
    let repos = repositories_in_tmp_with_origin_under(temp.path());
    assert!(
        repos.is_empty(),
        "project repositories outlived their process: {repos:?}"
    );
}

/// `/tmp` repositories whose `origin` lies under `root`: the child's own, told
/// apart from those of test processes running beside this one.
fn repositories_in_tmp_with_origin_under(root: &Path) -> BTreeSet<String> {
    let root = root.to_string_lossy().into_owned();
    std::fs::read_dir("/tmp")
        .unwrap()
        .flatten()
        .filter(|entry| {
            std::fs::read_to_string(entry.path().join(".git/config"))
                .is_ok_and(|config| config.contains(&root))
        })
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect()
}
