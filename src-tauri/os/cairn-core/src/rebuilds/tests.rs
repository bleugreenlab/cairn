use super::*;
use serde_json::json;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{symlink, PermissionsExt};

fn root() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).unwrap();
    dir
}

fn phase(id: &str, event: &str) -> Value {
    json!({"event":event,"rebuild":id,"at":"2026-11-07T13:00:00Z","type":"phase","name":"build","state":"started"})
}

fn write_events(root: &Path, id: &str, events: &[Value]) {
    let path = root.join(format!("{id}.jsonl"));
    let text = events.iter().map(|v| format!("{v}\n")).collect::<String>();
    fs::write(&path, text).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
}

async fn db() -> LocalDb {
    crate::storage::migrated_test_db("rebuild-ingestion").await
}

#[tokio::test]
async fn two_boots_and_live_append_are_idempotent() {
    let dir = root();
    let db = db().await;
    write_events(dir.path(), "r1", &[phase("r1", "phase")]);
    assert_eq!(
        ingest_once(&db, dir.path(), ActorState::Loaded)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        ingest_once(&db, dir.path(), ActorState::Loaded)
            .await
            .unwrap(),
        0
    );
    let outcome = json!({"event":"outcome","rebuild":"r1","at":"2026-11-07T13:01:00Z","type":"outcome","outcome":"done","summary":"installed"});
    let mut file = OpenOptions::new()
        .append(true)
        .open(dir.path().join("r1.jsonl"))
        .unwrap();
    writeln!(file, "{outcome}").unwrap();
    assert_eq!(
        ingest_once(&db, dir.path(), ActorState::Absent)
            .await
            .unwrap(),
        1
    );
    let row = ledger::get(&db, "r1").await.unwrap().unwrap();
    assert_eq!(row.status, "done");
    assert_eq!(row.phases.len(), 1);
    assert_eq!(row.events.len(), 2);
    assert!(row.faults.is_empty());
    assert_eq!(
        ingest_once(&db, dir.path(), ActorState::Absent)
            .await
            .unwrap(),
        0
    );
}

/// A finished rebuild's bun copy goes once its journal holds the outcome, and
/// only that file: the rest of the directory and the journal are evidence. A
/// rebuild still in flight keeps the bun it is running from.
#[tokio::test]
async fn a_finished_rebuild_releases_its_bun_copy_and_keeps_its_evidence() {
    let dir = root();
    let db = db().await;
    let evidence = ["spec.json", "rebuild.js", "build.log", "actor.log"];
    for id in ["finished", "in-flight"] {
        let staged = dir.path().join(id);
        fs::create_dir(&staged).unwrap();
        for name in evidence.iter().chain([&ACTOR_RUNTIME]) {
            fs::write(staged.join(name), id).unwrap();
        }
    }
    let outcome = json!({"event":"outcome","rebuild":"finished","at":"2026-11-07T13:01:00Z","type":"outcome","outcome":"rolled_back","summary":"rolled back"});
    write_events(dir.path(), "finished", &[phase("finished", "e1"), outcome]);
    write_events(dir.path(), "in-flight", &[phase("in-flight", "e1")]);

    for _ in 0..2 {
        ingest_once(&db, dir.path(), ActorState::Loaded)
            .await
            .unwrap();
        assert!(!dir.path().join("finished").join(ACTOR_RUNTIME).exists());
        assert!(dir.path().join("in-flight").join(ACTOR_RUNTIME).is_file());
        for id in ["finished", "in-flight"] {
            assert!(dir.path().join(format!("{id}.jsonl")).is_file());
            for name in evidence {
                assert!(dir.path().join(id).join(name).is_file(), "{id}/{name}");
            }
        }
    }
    assert_eq!(
        ledger::get(&db, "finished").await.unwrap().unwrap().status,
        "rolled_back"
    );
}

/// Only a regular file named `bun` is removed: a symlink there is not followed.
#[tokio::test]
async fn releasing_bun_never_follows_a_symlink() {
    let dir = root();
    let db = db().await;
    let elsewhere = tempfile::tempdir().unwrap();
    let target = elsewhere.path().join("bun");
    fs::write(&target, "precious").unwrap();
    fs::create_dir(dir.path().join("r1")).unwrap();
    symlink(&target, dir.path().join("r1").join(ACTOR_RUNTIME)).unwrap();
    let outcome = json!({"event":"outcome","rebuild":"r1","at":"2026-11-07T13:01:00Z","type":"outcome","outcome":"done","summary":"installed"});
    write_events(dir.path(), "r1", &[outcome]);
    ingest_once(&db, dir.path(), ActorState::Absent)
        .await
        .unwrap();
    assert_eq!(fs::read_to_string(&target).unwrap(), "precious");
}

#[tokio::test]
async fn conflicting_bytes_unknown_types_and_malformed_lines_are_faults() {
    let dir = root();
    let db = db().await;
    let first = phase("r1", "e1");
    write_events(dir.path(), "r1", std::slice::from_ref(&first));
    ingest_once(&db, dir.path(), ActorState::Loaded)
        .await
        .unwrap();
    let conflict = json!({"event":"e1","rebuild":"r1","at":"2026-11-07T13:00:00Z","type":"outcome","outcome":"done","summary":"forged"});
    let unknown =
        json!({"event":"e2","rebuild":"r1","at":"2026-11-07T13:00:00Z","type":"surprise"});
    write_events(dir.path(), "r1", &[conflict, unknown]);
    OpenOptions::new()
        .append(true)
        .open(dir.path().join("r1.jsonl"))
        .unwrap()
        .write_all(b"{torn")
        .unwrap();
    ingest_once(&db, dir.path(), ActorState::Unknown)
        .await
        .unwrap();
    let row = ledger::get(&db, "r1").await.unwrap().unwrap();
    assert_eq!(row.events, vec![first]);
    assert_eq!(row.status, "acting");
    for name in ["event_id_conflict", "unknown_event_type", "malformed_line"] {
        assert!(
            row.faults.iter().any(|f| f.name == name),
            "{name}: {:?}",
            row.faults
        );
    }
    ingest_once(&db, dir.path(), ActorState::Unknown)
        .await
        .unwrap();
    assert_eq!(
        ledger::get(&db, "r1").await.unwrap().unwrap().faults,
        row.faults
    );
}

#[tokio::test]
async fn absent_actor_fails_last_phase_but_unknown_liveness_stays_acting() {
    let dir = root();
    let db = db().await;
    write_events(dir.path(), "r1", &[phase("r1", "e1")]);
    ingest_once(&db, dir.path(), ActorState::Unknown)
        .await
        .unwrap();
    assert_eq!(
        ledger::get(&db, "r1").await.unwrap().unwrap().status,
        "acting"
    );
    ingest_once(&db, dir.path(), ActorState::Absent)
        .await
        .unwrap();
    let row = ledger::get(&db, "r1").await.unwrap().unwrap();
    assert_eq!(row.status, "failed");
    assert_eq!(row.summary.as_deref(), Some("actor died during build"));
}

#[test]
fn unreadable_launchctl_is_not_death_evidence() {
    assert_eq!(job_state(Some(0), ""), ActorState::Loaded);
    assert_eq!(job_state(Some(113), "Bad request. Could not find service \"computer.cairn.rebuild\" in domain for user gui: 501"), ActorState::Absent);
    for (code, stderr) in [
        (None, ""),
        (Some(1), "Operation not permitted"),
        (Some(113), "Could not find domain"),
        (Some(113), "permission denied"),
    ] {
        assert_eq!(job_state(code, stderr), ActorState::Unknown);
    }
}

#[test]
fn inode_ownership_modes_symlinks_and_hardlinks_are_checked() {
    let dir = root();
    write_events(dir.path(), "r1", &[phase("r1", "e1")]);
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    let directory = open_directory(dir.path(), uid).unwrap().unwrap();
    assert!(read_journal(&directory, "r1.jsonl", uid).is_ok());
    assert_eq!(
        check_metadata(&directory, uid.wrapping_add(1), true, "root")
            .unwrap_err()
            .name,
        "foreign_owner"
    );
    let journal = File::open(dir.path().join("r1.jsonl")).unwrap();
    assert_eq!(
        check_metadata(&journal, uid.wrapping_add(1), false, "r1.jsonl")
            .unwrap_err()
            .name,
        "foreign_owner"
    );
    fs::set_permissions(
        dir.path().join("r1.jsonl"),
        fs::Permissions::from_mode(0o666),
    )
    .unwrap();
    assert_eq!(
        read_journal(&directory, "r1.jsonl", uid).unwrap_err().name,
        "unexpected_mode"
    );
    fs::set_permissions(
        dir.path().join("r1.jsonl"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    fs::hard_link(dir.path().join("r1.jsonl"), dir.path().join("linked.jsonl")).unwrap();
    assert_eq!(
        read_journal(&directory, "r1.jsonl", uid).unwrap_err().name,
        "hardlink_refused"
    );
    symlink(
        dir.path().join("r1.jsonl"),
        dir.path().join("symlink.jsonl"),
    )
    .unwrap();
    assert_eq!(
        read_journal(&directory, "symlink.jsonl", uid)
            .unwrap_err()
            .name,
        "symlink_refused"
    );
    let parent = root();
    symlink(dir.path(), parent.path().join("rebuilds")).unwrap();
    assert_eq!(
        open_directory(&parent.path().join("rebuilds"), uid)
            .unwrap_err()
            .name,
        "directory_symlink_refused"
    );
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o777)).unwrap();
    assert_eq!(
        open_directory(dir.path(), uid).unwrap_err().name,
        "unexpected_mode"
    );
}

#[tokio::test]
async fn inode_refusals_are_named_on_the_ledger_row() {
    let dir = root();
    let db = db().await;
    write_events(dir.path(), "r1", &[phase("r1", "e1")]);
    fs::set_permissions(
        dir.path().join("r1.jsonl"),
        fs::Permissions::from_mode(0o666),
    )
    .unwrap();
    ingest_once(&db, dir.path(), ActorState::Unknown)
        .await
        .unwrap();
    let row = ledger::get(&db, "r1").await.unwrap().unwrap();
    assert!(row.events.is_empty());
    assert_eq!(row.faults[0].name, "unexpected_mode");
    fs::remove_file(dir.path().join("r1.jsonl")).unwrap();
    let other = root();
    write_events(other.path(), "outside", &[phase("r1", "e1")]);
    symlink(
        other.path().join("outside.jsonl"),
        dir.path().join("r1.jsonl"),
    )
    .unwrap();
    ingest_once(&db, dir.path(), ActorState::Unknown)
        .await
        .unwrap();
    let row = ledger::get(&db, "r1").await.unwrap().unwrap();
    assert!(row.faults.iter().any(|f| f.name == "symlink_refused"));
}

#[test]
fn validates_all_outcomes_and_refuses_wrong_rebuild_or_malformed_envelopes() {
    for outcome in ["done", "failed", "inconclusive", "rolled_back", "cancelled"] {
        let value = json!({"event":"e1","rebuild":"r1","at":"2026-11-07T13:00:00Z","type":"outcome","outcome":outcome,"summary":"result"});
        assert!(validate_event(&value, "r1").is_ok());
    }
    let value = phase("other", "e1");
    assert_eq!(validate_event(&value, "r1"), Err("invalid_event_envelope"));
    assert_eq!(parse_journal(b"\n", "r1").1[0].name, "malformed_line");
    assert_eq!(parse_journal(b"\xff\n", "r1").1[0].name, "malformed_line");
}

#[tokio::test]
async fn changed_directory_mode_is_refused_on_existing_rows() {
    let dir = root();
    let db = db().await;
    write_events(dir.path(), "r1", &[phase("r1", "e1")]);
    ingest_once(&db, dir.path(), ActorState::Loaded)
        .await
        .unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o777)).unwrap();
    ingest_once(&db, dir.path(), ActorState::Absent)
        .await
        .unwrap();
    let row = ledger::get(&db, "r1").await.unwrap().unwrap();
    assert_eq!(
        row.status, "acting",
        "a refused directory is not evidence that its actor died"
    );
    assert!(row.faults.iter().any(|f| f.name == "unexpected_mode"));
}

#[test]
fn unexpected_phases_and_non_regular_files_are_refused() {
    let mut value = phase("r1", "e1");
    value["name"] = json!("arbitrary-privileged-step");
    assert_eq!(validate_event(&value, "r1"), Err("unexpected_phase"));
    let dir = root();
    fs::create_dir(dir.path().join("directory.jsonl")).unwrap();
    fs::set_permissions(
        dir.path().join("directory.jsonl"),
        fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    let directory = open_directory(dir.path(), uid).unwrap().unwrap();
    assert_eq!(
        read_journal(&directory, "directory.jsonl", uid)
            .unwrap_err()
            .name,
        "not_regular_file"
    );
}

#[test]
fn owner_only_umask_modes_are_valid_but_special_modes_are_not() {
    let dir = root();
    write_events(dir.path(), "r1", &[phase("r1", "e1")]);
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(
        dir.path().join("r1.jsonl"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    let directory = open_directory(dir.path(), uid).unwrap().unwrap();
    assert!(read_journal(&directory, "r1.jsonl", uid).is_ok());
    // Unprivileged macOS may silently clear setuid on chmod; the sticky bit
    // survives, so assert the kernel established the special-mode fixture.
    let path = dir.path().join("r1.jsonl");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o1644)).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().mode() & 0o7777, 0o1644);
    assert_eq!(
        read_journal(&directory, "r1.jsonl", uid).unwrap_err().name,
        "unexpected_mode"
    );
}

#[tokio::test]
async fn observes_actor_after_snapshot_so_launch_cannot_fail_a_new_journal() {
    let dir = root();
    let db = db().await;
    // The observer simulates an actor launching after the snapshot was taken.
    // That journal must not be seen with an earlier absent-job observation.
    ingest_with_observation(&db, dir.path(), || async {
        write_events(dir.path(), "r1", &[phase("r1", "e1")]);
        ActorState::Absent
    })
    .await
    .unwrap();
    assert!(ledger::get(&db, "r1").await.unwrap().is_none());
    ingest_with_observation(&db, dir.path(), || async { ActorState::Loaded })
        .await
        .unwrap();
    assert_eq!(
        ledger::get(&db, "r1").await.unwrap().unwrap().status,
        "acting"
    );
}

#[tokio::test]
async fn unreadable_malformed_or_missing_journals_do_not_prove_missing_outcomes() {
    let dir = root();
    let db = db().await;
    write_events(dir.path(), "r1", &[phase("r1", "e1")]);
    fs::set_permissions(
        dir.path().join("r1.jsonl"),
        fs::Permissions::from_mode(0o666),
    )
    .unwrap();
    ingest_once(&db, dir.path(), ActorState::Absent)
        .await
        .unwrap();
    let row = ledger::get(&db, "r1").await.unwrap().unwrap();
    assert_eq!(row.status, "acting");
    assert!(row.outcome.is_none());
    fs::set_permissions(
        dir.path().join("r1.jsonl"),
        fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    OpenOptions::new()
        .append(true)
        .open(dir.path().join("r1.jsonl"))
        .unwrap()
        .write_all(b"{torn")
        .unwrap();
    ingest_once(&db, dir.path(), ActorState::Absent)
        .await
        .unwrap();
    assert_eq!(
        ledger::get(&db, "r1").await.unwrap().unwrap().status,
        "acting"
    );
    fs::remove_file(dir.path().join("r1.jsonl")).unwrap();
    ingest_once(&db, dir.path(), ActorState::Absent)
        .await
        .unwrap();
    let row = ledger::get(&db, "r1").await.unwrap().unwrap();
    assert_eq!(row.status, "acting");
    assert!(row.faults.iter().any(|f| f.name == "journal_missing"));
}
