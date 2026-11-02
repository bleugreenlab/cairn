use std::sync::Arc;

use super::*;
use crate::agent_process::process::RunHandle;
use crate::node_scratch::owner::{self as scratch_owner, Replica, ScratchOwner};
use crate::services::testing::TestServicesBuilder;
use crate::storage::SearchIndex;

const HOUR_MS: u64 = 60 * 60 * 1_000;
const DAY_MS: u64 = 24 * HOUR_MS;

fn now() -> u64 {
    chrono::Utc::now().timestamp_millis() as u64
}

fn candidate(registered: &[&str], unregistered: Option<&str>) -> Candidate {
    Candidate {
        name: "entry".into(),
        path: PathBuf::from("entry"),
        registered: registered.iter().map(|job| job.to_string()).collect(),
        unregistered_job: unregistered.map(str::to_owned),
    }
}

#[test]
fn an_entry_is_ended_only_when_every_job_it_answers_to_has_ended() {
    let fates: HashMap<String, JobFate> = [
        (
            "merged",
            JobFate::Owner(OwnerFate::Ended("its issue merged")),
        ),
        ("live", JobFate::Owner(OwnerFate::Live)),
        (
            "unjudged",
            JobFate::Owner(OwnerFate::Unknown("replica not open".into())),
        ),
        ("missing", JobFate::Missing),
    ]
    .into_iter()
    .map(|(job, fate)| (job.to_owned(), fate))
    .collect();
    let none = HashSet::new();
    let ended = |owner: Owner| matches!(owner, Owner::Ended { .. });

    assert_eq!(
        classify(&candidate(&["merged"], None), &fates, &none),
        Owner::Ended {
            jobs: vec!["merged".into()],
            reason: "its issue merged"
        }
    );
    assert!(ended(classify(
        &candidate(&["merged", "missing"], None),
        &fates,
        &none
    )));
    assert!(
        !ended(classify(
            &candidate(&["merged", "live"], None),
            &fates,
            &none
        )),
        "one live job keeps a shared residence"
    );
    assert!(
        !ended(classify(
            &candidate(&["merged", "unjudged"], None),
            &fates,
            &none
        )),
        "a job whose database could not be asked is never read as ended"
    );
    let held = HashSet::from(["merged".to_owned()]);
    assert!(
        !ended(classify(&candidate(&["merged"], None), &fates, &held)),
        "a live process keeps its residence whatever its node's state"
    );

    assert_eq!(
        classify(&candidate(&[], Some("missing")), &fates, &none),
        Owner::Unowned,
        "a name that is no job's id is nobody's"
    );
    assert_eq!(
        classify(&candidate(&[], None), &fates, &none),
        Owner::Unowned
    );
    assert!(
        ended(classify(&candidate(&[], Some("merged")), &fates, &none)),
        "a residence created before its URI resolved still answers to its job"
    );
}

#[test]
fn measuring_counts_the_tree_and_never_follows_a_link_out_of_it() {
    let temp = tempfile::tempdir().unwrap();
    let tree = temp.path().join("tree");
    std::fs::create_dir_all(tree.join("sub")).unwrap();
    std::fs::write(tree.join("a"), [0_u8; 10]).unwrap();
    std::fs::write(tree.join("sub/b"), [0_u8; 20]).unwrap();
    let outside = temp.path().join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("big"), vec![1_u8; 8 * 1024 * 1024]).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, tree.join("link")).unwrap();

    let footprint = measure(&tree);
    assert_eq!(footprint.logical_bytes, 30);
    assert!(
        footprint.exclusive_bytes < 1024 * 1024,
        "the linked tree is not this one's: {footprint:?}"
    );
    assert!(footprint.newest_write_unix_ms > 0);
}

#[cfg(unix)]
#[test]
fn removing_a_link_entry_leaves_what_it_points_at() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("scratch");
    let target = temp.path().join("checkout");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(target.join("kept"), b"x").unwrap();
    std::os::unix::fs::symlink(&target, root.join("link")).unwrap();

    remove(&root, &root.join("link")).unwrap();

    assert!(std::fs::symlink_metadata(root.join("link")).is_err());
    assert!(target.join("kept").exists());
}

fn staged_entry(root: &Path, name: &str, owner: Owner, footprint: Footprint) -> Entry {
    let path = root.join(name);
    std::fs::create_dir_all(&path).unwrap();
    Entry {
        name: name.into(),
        path,
        owner,
        footprint: Some(footprint),
    }
}

fn footprint(exclusive_bytes: u64, newest_write_unix_ms: u64) -> Footprint {
    Footprint {
        logical_bytes: exclusive_bytes * 10,
        exclusive_bytes,
        newest_write_unix_ms,
    }
}

/// A tree of clones that is huge logically and frees little must not end the
/// pass early: credit is exclusive bytes, and the order is coldness.
#[test]
fn pressure_takes_cold_unowned_entries_coldest_first_until_their_exclusive_bytes_meet_the_ask() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let at = now();
    let mut entries = vec![
        staged_entry(
            root,
            "coldest",
            Owner::Unowned,
            footprint(60, at - 3 * DAY_MS),
        ),
        staged_entry(
            root,
            "colder",
            Owner::Unowned,
            footprint(50, at - 2 * DAY_MS),
        ),
        staged_entry(
            root,
            "cold",
            Owner::Unowned,
            footprint(1_000, at - 30 * HOUR_MS),
        ),
        staged_entry(
            root,
            "recent",
            Owner::Unowned,
            footprint(5_000, at - HOUR_MS),
        ),
        staged_entry(
            root,
            "live",
            Owner::Live {
                jobs: vec!["j".into()],
            },
            footprint(9_000, at - 9 * DAY_MS),
        ),
    ];
    let lines = Lines {
        pressure_free_bytes: 100,
        critical_free_bytes: 5,
    };

    let pass = pressure_pass(root, &mut entries, lines, at, &|| Some(10)).unwrap();

    assert_eq!(pass.band, StoragePressureBand::Pressure);
    assert_eq!(pass.asked_bytes, 90);
    assert_eq!(
        pass.removed
            .iter()
            .map(|removal| removal.name.as_str())
            .collect::<Vec<_>>(),
        ["coldest", "colder"]
    );
    assert_eq!(pass.credited_bytes, 110);
    assert!(!root.join("coldest").exists() && !root.join("colder").exists());
    for kept in ["cold", "recent", "live"] {
        assert!(root.join(kept).exists(), "{kept}");
    }
    assert_eq!(pass.kept_live.count, 1);
    assert_eq!(pass.kept_quiet.count, 1, "written an hour ago");
    assert_eq!(entries.len(), 3);

    let mut entries = vec![staged_entry(
        root,
        "above",
        Owner::Unowned,
        footprint(1, at - 9 * DAY_MS),
    )];
    assert!(
        pressure_pass(root, &mut entries, lines, at, &|| Some(101)).is_none(),
        "above the line nothing unowned is taken"
    );
    assert!(root.join("above").exists());
}

struct World {
    temp: tempfile::TempDir,
    orch: Orchestrator,
}

impl World {
    fn root(&self) -> PathBuf {
        self.temp.path().join("scratch")
    }

    /// A residence as session startup leaves one: registered and holding a file.
    fn residence(&self, job: &str, name: &str) {
        let root = self.root();
        std::fs::create_dir_all(root.join(SCRATCH_MAP_DIR)).unwrap();
        std::fs::write(root.join(SCRATCH_MAP_DIR).join(encode_component(job)), name).unwrap();
        std::fs::create_dir_all(root.join(name)).unwrap();
        std::fs::write(root.join(name).join("notes.md"), b"x").unwrap();
    }

    fn exists(&self, name: &str) -> bool {
        std::fs::symlink_metadata(self.root().join(name)).is_ok()
    }

    fn registered(&self, job: &str) -> bool {
        self.root()
            .join(SCRATCH_MAP_DIR)
            .join(encode_component(job))
            .exists()
    }

    async fn scratch(
        &self,
        job: &str,
        uri: &str,
        issue: Option<&str>,
        thread: Option<&str>,
    ) -> String {
        let scratch = scratch_owner::open_for_owner(
            &self.orch.db.local,
            &ScratchOwner {
                job_id: job.into(),
                uri: uri.into(),
                project_id: "p".into(),
                thread_id: thread.map(str::to_owned),
                issue_id: issue.map(str::to_owned),
            },
        )
        .await
        .unwrap();
        scratch_owner::record_replica(
            &self.orch.db.local,
            &Replica {
                scratch_id: scratch.id.clone(),
                executor_id: "remote-a".into(),
                synced_commit: None,
                synced_written_bytes: 0,
                dirty: false,
                refused_bytes: None,
            },
        )
        .await
        .unwrap();
        scratch.id
    }

    async fn closed(&self, scratch_id: &str) -> bool {
        scratch_owner::load(&self.orch.db.local, scratch_id)
            .await
            .unwrap()
            .unwrap()
            .closed_at
            .is_some()
    }

    async fn replicas(&self, scratch_id: &str) -> usize {
        scratch_owner::replicas(&self.orch.db.local, scratch_id)
            .await
            .unwrap()
            .len()
    }

    fn hold(&self, run: &str, job: &str) {
        self.orch
            .process_state
            .processes
            .lock()
            .unwrap()
            .register(run.into(), RunHandle::test_handle(None, Some(job)));
    }

    fn release(&self, run: &str) {
        self.orch
            .process_state
            .processes
            .lock()
            .unwrap()
            .remove(run);
    }
}

/// A project with a live issue, a merged one, a live thread, and a closed
/// thread, each with jobs.
async fn world() -> World {
    let temp = tempfile::tempdir().unwrap();
    let config_dir = temp.path().join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::create_dir_all(temp.path().join("scratch")).unwrap();
    let local = LocalDb::open_migrated_for_test(config_dir.join("db.turso.db"))
        .await
        .unwrap();
    local
        .execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'default', 'P', 'nsc', '/tmp/nsc', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
               VALUES ('live-issue', 'p', 1, 'Live', 'active', 'none', 1, 1),
                      ('done-issue', 'p', 2, 'Done', 'merged', 'none', 1, 1);
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
               VALUES ('e1', 'r', 'live-issue', 'p', 'running', 1, 1, '{}', 'manual'),
                      ('e2', 'r', 'done-issue', 'p', 'complete', 1, 1, '{}', 'manual');
             INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, uri_segment, created_at, updated_at)
               VALUES ('live-job', 'e1', 'live-issue', 'p', 'builder', 'running', 'builder', 1, 1),
                      ('done-job', 'e2', 'done-issue', 'p', 'builder', 'complete', 'builder', 1, 1);
             INSERT INTO jobs(id, execution_id, issue_id, project_id, parent_job_id, node_name, status, uri_segment, created_at, updated_at)
               VALUES ('done-task', 'e2', 'done-issue', 'p', 'done-job', 'review', 'complete', 'review', 1, 1);
             INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES ('open-thread', 'p', 'open', 'active', 'none', 1, 1),
                      ('closed-thread', 'p', 'shut', 'closed', 'none', 1, 1);
             INSERT INTO jobs(id, project_id, thread_id, node_name, status, uri_segment, created_at, updated_at)
               VALUES ('open-session', 'p', 'open-thread', 'Thread', 'running', 'thread', 1, 1),
                      ('closed-session', 'p', 'closed-thread', 'Thread', 'idle', 'thread', 1, 1),
                      ('closed-held', 'p', 'closed-thread', 'Design', 'running', 'design', 1, 1);",
        )
        .await
        .unwrap();
    let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
    let orch = Orchestrator::builder(
        Arc::new(DbState::new(Arc::new(local), search)),
        Arc::new(TestServicesBuilder::new().build()),
        config_dir,
    )
    .build();
    World { temp, orch }
}

const GIB: u64 = 1024 * 1024 * 1024;
/// Lines far above anything a fixture stages, so an ask under them is never met
/// before every eligible entry has been taken.
const LINES: Lines = Lines {
    pressure_free_bytes: 64 * GIB,
    critical_free_bytes: 16 * GIB,
};
const ABOVE: u64 = 128 * GIB;
const UNDER_PRESSURE: u64 = 32 * GIB;
const CRITICAL: u64 = GIB;

/// The runner-start pass for a resolved issue and a closed thread whose
/// teardown never reached the runner, under a Critical volume: every ended
/// node's residence and portable scratch go, the executors' replicas are
/// marked for removal, cold unowned entries go, and the live node's residence
/// survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_critical_pass_reclaims_everything_ended_and_unowned_and_keeps_the_live_node() {
    let world = world().await;
    world.residence("live-job", "nsc.1.1.builder");
    world.residence("done-job", "nsc.2.1.builder");
    world.residence("done-task", "nsc.2.1.builder.task.review");
    world.residence("closed-session", "nsc.shut");
    world.residence("closed-held", "nsc.shut.task.design");
    world.residence("open-session", "nsc.open");
    world.residence("gone-job", "nsc.9.1.builder");
    let root = world.root();
    std::fs::create_dir_all(root.join("triome-45-measure/data")).unwrap();
    std::fs::write(
        root.join("triome-45-measure/data/game.sqlite3"),
        [7_u8; 4096],
    )
    .unwrap();
    std::fs::write(root.join("proto.html"), b"<html>").unwrap();
    std::fs::create_dir_all(root.join(RESPONSE_SCRATCH_DIR).join("r")).unwrap();
    let done_scratch = world
        .scratch(
            "done-job",
            "cairn://p/nsc/2/1/builder",
            Some("done-issue"),
            None,
        )
        .await;
    let shut_scratch = world
        .scratch(
            "closed-session",
            "cairn://p/nsc/shut",
            None,
            Some("closed-thread"),
        )
        .await;
    let live_scratch = world
        .scratch(
            "live-job",
            "cairn://p/nsc/1/1/builder",
            Some("live-issue"),
            None,
        )
        .await;
    world.hold("run-held", "closed-held");

    // Two days on, so every unowned entry is past the critical quiet floor.
    tick(&world.orch, root.clone(), LINES, now() + 2 * DAY_MS, || {
        Some(CRITICAL)
    })
    .await;

    for live in ["nsc.1.1.builder", "nsc.open", RESPONSE_SCRATCH_DIR] {
        assert!(world.exists(live), "{live} must survive a Critical pass");
    }
    assert!(
        world.exists("nsc.shut.task.design"),
        "a residence a live process holds is never taken"
    );
    for ended in [
        "nsc.2.1.builder",
        "nsc.2.1.builder.task.review",
        "nsc.shut",
        "nsc.9.1.builder",
        "triome-45-measure",
        "proto.html",
    ] {
        assert!(!world.exists(ended), "{ended} must be reclaimed");
    }
    assert!(world.registered("live-job"));
    assert!(world.registered("closed-held"));
    assert!(!world.registered("done-job") && !world.registered("gone-job"));
    let leftovers = std::fs::read_dir(&root)
        .unwrap()
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(RECLAIMING_PREFIX)
        })
        .count();
    assert_eq!(leftovers, 0);

    assert!(world.closed(&done_scratch).await && world.closed(&shut_scratch).await);
    assert_eq!(
        world.replicas(&done_scratch).await,
        1,
        "the replica stays recorded until its machine acknowledges the delete"
    );
    assert!(!world.closed(&live_scratch).await);

    let report = world.orch.host_scratch.report();
    let pass = report.pressure.expect("a pass ran under the line");
    assert_eq!(pass.band, StoragePressureBand::Critical);
    assert_eq!(pass.kept_live.count, 3);
    let taken: Vec<&str> = pass
        .removed
        .iter()
        .map(|removal| removal.name.as_str())
        .collect();
    assert!(taken.contains(&"triome-45-measure") && taken.contains(&"proto.html"));
    assert!(report
        .removals
        .iter()
        .any(|removal| removal.name == "nsc.2.1.builder" && removal.why == "its issue merged"));
    assert!(report
        .removals
        .iter()
        .any(|removal| removal.name == "nsc.shut" && removal.why == "its thread closed"));
    let census = report.census.expect("a pass under pressure measures");
    assert!(census
        .entries
        .iter()
        .all(|entry| matches!(entry.owner, Owner::Live { .. })));

    // Once its process ends, the held residence of the closed thread goes too.
    world.release("run-held");
    tick(&world.orch, root, LINES, now(), || Some(ABOVE)).await;
    assert!(!world.exists("nsc.shut.task.design"));
    assert!(world.exists("nsc.1.1.builder"));
}

/// Above the pressure line nothing unowned is taken, and under it an unowned
/// workspace written to inside the quiet floor is kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unowned_workspace_in_use_is_left_alone() {
    let world = world().await;
    let root = world.root();
    std::fs::create_dir_all(root.join("measure")).unwrap();
    std::fs::write(root.join("measure/run.log"), b"still going").unwrap();

    tick(&world.orch, root.clone(), LINES, now(), || Some(ABOVE)).await;
    assert!(world.exists("measure"));
    let report = world.orch.host_scratch.report();
    assert!(report.pressure.is_none());
    let census = report.census.expect("the first pass measures");
    assert_eq!(census.entries.len(), 1);
    assert_eq!(census.entries[0].owner, Owner::Unowned);
    assert_eq!(census.entries[0].footprint.unwrap().logical_bytes, 11);

    tick(&world.orch, root, LINES, now(), || Some(UNDER_PRESSURE)).await;
    assert!(world.exists("measure"), "written an instant ago");
    let pass = world.orch.host_scratch.report().pressure.unwrap();
    assert_eq!(pass.band, StoragePressureBand::Pressure);
    assert!(pass.removed.is_empty());
    assert_eq!(pass.kept_quiet.count, 1);
}

/// Closing a thread removes the residences of its jobs that nothing holds and
/// closes its portable scratch, whose replicas are then marked for removal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_a_thread_removes_its_residences_and_closes_its_scratch() {
    let world = world().await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let session = format!("session-{suffix}");
    let held = format!("held-{suffix}");
    world
        .orch
        .db
        .local
        .execute_script(&format!(
            "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES ('thread-{suffix}', 'p', 'closing-{suffix}', 'active', 'none', 1, 1);
             INSERT INTO jobs(id, project_id, thread_id, node_name, status, uri_segment, created_at, updated_at)
               VALUES ('{session}', 'p', 'thread-{suffix}', 'Thread', 'idle', 'thread', 1, 1),
                      ('{held}', 'p', 'thread-{suffix}', 'Task', 'running', 'task', 1, 1);"
        ))
        .await
        .unwrap();
    let uri = format!("cairn://p/nsc/closing-{suffix}");
    let session_dir = crate::scratch::ensure_job_scratch_dir(&session, Some(&uri));
    let held_dir = crate::scratch::ensure_job_scratch_dir(&held, Some(&format!("{uri}/task/task")));
    let scratch = world
        .scratch(&session, &uri, None, Some(&format!("thread-{suffix}")))
        .await;
    world.hold("run-held", &held);

    crate::threads::crud::update_with_lifecycle(
        &world.orch,
        &world.orch.db.local,
        cairn_db::models::UpdateThread {
            id: format!("thread-{suffix}"),
            name: None,
            jurisdiction: None,
            definition: None,
            status: Some(cairn_db::models::ThreadStatus::Closed),
            model: None,
        },
    )
    .await
    .unwrap();

    assert!(!session_dir.exists(), "{}", session_dir.display());
    assert!(held_dir.exists(), "a live process's residence waits for it");
    assert!(world.closed(&scratch).await);
    assert_eq!(world.replicas(&scratch).await, 1);
    crate::scratch::remove_job_scratch_dir(&held);
}

/// Resolving an issue tears down its nodes' residences and closes their
/// portable scratch, leaving the replicas marked for removal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolving_an_issue_removes_its_residences_and_marks_its_replicas() {
    let world = world().await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let job = format!("job-{suffix}");
    world
        .orch
        .db
        .local
        .execute_script(&format!(
            "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
               VALUES ('issue-{suffix}', 'p', 3, 'Resolving', 'merged', 'none', 1, 1);
             INSERT INTO jobs(id, project_id, issue_id, node_name, status, uri_segment, created_at, updated_at)
               VALUES ('{job}', 'p', 'issue-{suffix}', 'builder', 'complete', 'builder', 1, 1);"
        ))
        .await
        .unwrap();
    let uri = format!("cairn://p/nsc-{suffix}/3/1/builder");
    let residence = crate::scratch::ensure_job_scratch_dir(&job, Some(&uri));
    std::fs::write(residence.join("notes.md"), b"x").unwrap();
    let scratch = world
        .scratch(&job, &uri, Some(&format!("issue-{suffix}")), None)
        .await;

    crate::execution::teardown::cleanup_issue_jobs(
        &world.orch,
        crate::execution::teardown::TeardownScope::Issue(format!("issue-{suffix}")),
        crate::execution::teardown::TeardownReason::Merged,
    )
    .await
    .unwrap();

    assert!(!residence.exists(), "{}", residence.display());
    assert!(world.closed(&scratch).await);
    assert_eq!(world.replicas(&scratch).await, 1);
}
