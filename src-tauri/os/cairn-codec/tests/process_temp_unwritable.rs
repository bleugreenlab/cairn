//! A full transfer (validate, install, prove the closure) succeeds while the
//! process's temp directory refuses every create, because the codec writes its
//! throwaway repositories only where its caller says.
//!
//! Its own test binary, so pointing `TMPDIR` at a read-only directory touches
//! no other test.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

use cairn_codec::transfer::{
    build_reachable_pack, install_pack, validate_pack, verify_commit_closure, PackLimits,
};

fn git(directory: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(directory)
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@example.com")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@example.com")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

#[test]
fn a_transfer_succeeds_while_process_temp_refuses_every_create() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir_all(source.join("nested")).unwrap();
    git(&source, &["init", "-q", "-b", "main"]);
    fs::write(source.join("nested/file"), b"payload").unwrap();
    git(&source, &["add", "."]);
    git(&source, &["commit", "-qm", "tip"]);
    let tip = git(&source, &["rev-parse", "HEAD"]);
    let (pack, _) = build_reachable_pack(&source, std::slice::from_ref(&tip), &[])
        .unwrap()
        .unwrap();

    let sealed = root.path().join("sealed-temp");
    fs::create_dir(&sealed).unwrap();
    fs::set_permissions(&sealed, fs::Permissions::from_mode(0o555)).unwrap();
    std::env::set_var("TMPDIR", &sealed);
    if tempfile::tempdir().is_ok() {
        // A privileged runner writes through the mode bits, so the condition
        // under test cannot be produced here.
        fs::set_permissions(&sealed, fs::Permissions::from_mode(0o755)).unwrap();
        eprintln!("skipped: this process can write to a read-only directory");
        return;
    }

    let scratch = root.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let validated = validate_pack(&pack, PackLimits::default(), &scratch).unwrap();
    let objects = root.path().join("receiver/objects");
    install_pack(&objects, &validated).unwrap();
    verify_commit_closure(&objects, &[], &[], &tip, &scratch).unwrap();
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);

    fs::set_permissions(&sealed, fs::Permissions::from_mode(0o755)).unwrap();
}
