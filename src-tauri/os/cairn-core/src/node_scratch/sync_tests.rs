//! The scratch sync end to end, without execution cells: two replicas (two
//! machines' worth of `cairn_exec::node_scratch`, each with its own root and
//! object-channel credential) against the runner's own `scratch/*` router over
//! loopback HTTP, folding into the authority row and store.
//!
//! What a batch adds around this (placement, `$TMPDIR`, the run-result line)
//! is covered by the two-machine acceptance suite; this is the transfer, the
//! merge, and the ceiling, with nothing simulated.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cairn_common::executor_protocol::{
    ExecutorAdvertisement, ExecutorCapabilities, ExecutorIdentity, ExecutorMessage,
    NodeScratchGrant, NodeScratchReplica, ObjectChannelCredential, ScratchSyncReport,
    ScratchWriteBackOutcome, SCRATCH_STAGE_CREDENTIAL_REFUSED,
};
use cairn_exec::node_scratch::{self as replica, Replicas, ScratchTransport};

use super::owner::{self, NodeScratch, ScratchOwner};
use crate::db::DbState;
use crate::orchestrator::Orchestrator;
use crate::services::testing::TestServicesBuilder;
use crate::storage::{LocalDb, SearchIndex};

struct Machine {
    replicas: Replicas,
    transport: ScratchTransport,
    writer: &'static str,
}

struct World {
    _temp: tempfile::TempDir,
    orch: Orchestrator,
    base_url: String,
    scratch_id: String,
    a: Machine,
    b: Machine,
}

impl World {
    async fn scratch(&self) -> NodeScratch {
        owner::load(&self.orch.db.local, &self.scratch_id)
            .await
            .unwrap()
            .unwrap()
    }

    /// A grant as the runner would issue it now.
    async fn grant(&self) -> NodeScratchGrant {
        let scratch = self.scratch().await;
        let ceiling = owner::ceiling_bytes(&self.orch, &scratch.project_id).await;
        scratch.grant(ceiling)
    }
}

impl Machine {
    async fn prepare(&self, grant: &NodeScratchGrant) -> replica::Prepared {
        replica::prepare(&self.replicas, Some(&self.transport), grant, self.writer)
            .await
            .expect("prepare the scratch")
    }

    async fn finish(&self, prepared: replica::Prepared) -> ScratchSyncReport {
        replica::finish(&self.replicas, Some(&self.transport), prepared, self.writer).await
    }
}

/// A project whose repository carries `config` as its `.cairn/config.yaml`.
async fn world(config: Option<&str>) -> World {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(repo.join(".cairn")).unwrap();
    if let Some(config) = config {
        std::fs::write(repo.join(".cairn/config.yaml"), config).unwrap();
    }
    let config_dir = temp.path().join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    let local = LocalDb::open_migrated_for_test(config_dir.join("sync.db"))
        .await
        .unwrap();
    local
        .execute(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'P', 'nsc', ?1, 1, 1)",
            (repo.display().to_string(),),
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
    let scratch = owner::open_for_owner(
        &orch.db.local,
        &ScratchOwner {
            job_id: "job".into(),
            uri: "cairn://p/nsc/1/1/builder".into(),
            project_id: "p".into(),
            thread_id: None,
            issue_id: Some("issue".into()),
        },
    )
    .await
    .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = axum::Router::new().nest_service("/scratch", super::plane::router(orch.clone()));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let machine = |id: &'static str, writer: &'static str, root: PathBuf| {
        let (token, _) = orch
            .object_plane
            .issue_credential(
                id,
                &format!("{id}-device"),
                "runner",
                1,
                chrono::Utc::now().timestamp_millis() as u64,
            )
            .expect("a first generation is issued");
        orch.object_plane.scratch.authorize(id, 1, &scratch.id);
        Machine {
            replicas: Replicas::new(root),
            transport: ScratchTransport::new(format!("http://{address}"), token),
            writer,
        }
    };
    let a = machine("machine-a", "wa", temp.path().join("a/node-scratch"));
    let b = machine("machine-b", "wb", temp.path().join("b/node-scratch"));
    World {
        scratch_id: scratch.id,
        base_url: format!("http://{address}"),
        _temp: temp,
        orch,
        a,
        b,
    }
}

impl World {
    /// What one attach of `executor_id` leaves the runner holding: a fresh
    /// object-channel credential for that connection generation, and the
    /// scratch granted to it, as its advertisement grants a replica.
    fn attach(&self, executor_id: &str, generation: u64) -> ObjectChannelCredential {
        let (bearer_token, expires_at_unix_ms) = self
            .orch
            .object_plane
            .issue_credential(
                executor_id,
                &format!("{executor_id}-device"),
                "runner",
                generation,
                chrono::Utc::now().timestamp_millis() as u64,
            )
            .expect("an attach issues its generation");
        self.orch
            .object_plane
            .scratch
            .authorize(executor_id, generation, &self.scratch_id);
        ObjectChannelCredential {
            base_url: self.base_url.clone(),
            bearer_token,
            expires_at_unix_ms,
        }
    }

    /// What a runner restart does to one machine's connection: every token
    /// and grant it was issued is gone with the process.
    fn restart_runner(&self, executor_id: &str, generation: u64) {
        self.orch.object_plane.revoke(executor_id, generation);
    }

    /// A machine whose transport presents whatever `held` holds when each call
    /// goes out, as an executor presents the credential its link last set.
    fn live_machine(&self, held: Arc<Mutex<ObjectChannelCredential>>) -> Machine {
        Machine {
            replicas: Replicas::new(self._temp.path().join("c/node-scratch")),
            transport: ScratchTransport::live(cairn_tls::client().clone(), move || {
                Some(held.lock().unwrap().clone())
            }),
            writer: "wc",
        }
    }
}

fn applied(report: &ScratchSyncReport) -> &ScratchWriteBackOutcome {
    &report.write_back.as_ref().expect("a write-back").outcome
}

fn read(tree: &Path, path: &str) -> Option<String> {
    std::fs::read_to_string(tree.join(path)).ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_written_on_one_machine_is_read_on_another() {
    let world = world(None).await;
    let on_a = world.a.prepare(&world.grant().await).await;
    std::fs::create_dir_all(on_a.tree.join("extract/pilot")).unwrap();
    std::fs::write(on_a.tree.join("extract/pilot/rows.csv"), "id,value\n1,2\n").unwrap();
    let report = world.a.finish(on_a).await;
    assert!(
        matches!(applied(&report), ScratchWriteBackOutcome::Applied { conflicts, .. } if conflicts.is_empty()),
        "{report:?}"
    );
    assert!(report.write_back.as_ref().unwrap().bytes > 0);
    assert!(!report.dirty);

    let scratch = world.scratch().await;
    assert_eq!(scratch.head_commit, report.synced_commit);
    assert_eq!(scratch.head_executor_id.as_deref(), Some("machine-a"));
    assert_eq!(scratch.head_seq, 1);

    let on_b = world.b.prepare(&world.grant().await).await;
    assert_eq!(
        read(&on_b.tree, "extract/pilot/rows.csv").as_deref(),
        Some("id,value\n1,2\n"),
        "the second machine's first batch reads what the first machine wrote"
    );
    let report = world.b.finish(on_b).await;
    let materialized = report.materialize.expect("brought current");
    assert!(materialized.bytes > 0, "{materialized:?}");
    assert_eq!(materialized.to_commit, scratch.head_commit);
    assert!(materialized.deferred_reason.is_none());
    assert!(report.write_back.is_none(), "nothing changed on b");

    // Unchanged bytes never move twice: a again, already at the head.
    let again = world.a.prepare(&world.grant().await).await;
    let report = world.a.finish(again).await;
    assert!(report.materialize.is_none(), "{report:?}");
}

/// A file the runner puts (a host door's output) lands in the head as a
/// write-back would: a batch that ran meanwhile keeps its own writes beside
/// it, and the next machine reads both.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_the_runner_puts_is_read_on_a_machine_beside_a_concurrent_batchs_writes() {
    let world = world(None).await;
    let on_a = world.a.prepare(&world.grant().await).await;
    std::fs::write(on_a.tree.join("notes.md"), "mine\n").unwrap();

    let vectors = b"{\"vectors\":[[0.25,0.5]]}".to_vec();
    super::put::put(
        &world.orch,
        &world.scratch_id,
        vec![super::put::PutFile {
            path: "embeddings/run.json".into(),
            bytes: vectors.clone(),
        }],
        "models-embed",
    )
    .await
    .unwrap();
    let scratch = world.scratch().await;
    assert_eq!(scratch.head_seq, 1);
    assert_eq!(scratch.head_executor_id.as_deref(), Some("models-embed"));
    assert_eq!(scratch.written_bytes, vectors.len() as u64);

    let report = world.a.finish(on_a).await;
    assert!(
        matches!(applied(&report), ScratchWriteBackOutcome::Applied { conflicts, .. } if conflicts.is_empty()),
        "{report:?}"
    );
    let on_b = world.b.prepare(&world.grant().await).await;
    assert_eq!(
        std::fs::read(on_b.tree.join("embeddings/run.json")).unwrap(),
        vectors
    );
    assert_eq!(read(&on_b.tree, "notes.md").as_deref(), Some("mine\n"));
    world.b.finish(on_b).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_put_past_the_ceiling_is_refused_by_name_and_moves_nothing() {
    let world = world(Some("scratch:\n  ceilingMiB: 1\n")).await;
    let refusal = super::put::put(
        &world.orch,
        &world.scratch_id,
        vec![super::put::PutFile {
            path: "embeddings/big.npy".into(),
            bytes: vec![0u8; 2 * 1024 * 1024],
        }],
        "models-embed",
    )
    .await
    .unwrap_err();
    assert!(refusal.contains("cairn://p/nsc/1/1/builder"), "{refusal}");
    assert!(refusal.contains("past its ceiling"), "{refusal}");
    assert!(refusal.contains("`embeddings/big.npy`"), "{refusal}");
    let scratch = world.scratch().await;
    assert_eq!((scratch.head_commit, scratch.head_seq), (None, 0));
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writers_to_one_path_both_survive_and_the_conflict_is_named() {
    let world = world(None).await;
    let seed = world.a.prepare(&world.grant().await).await;
    std::fs::write(seed.tree.join("report.md"), "draft\n").unwrap();
    world.a.finish(seed).await;

    // Both machines start from the same head, as two task batches would.
    let grant = world.grant().await;
    let on_a = world.a.prepare(&grant).await;
    let on_b = world.b.prepare(&grant).await;
    std::fs::write(on_a.tree.join("report.md"), "from a\n").unwrap();
    std::fs::write(on_b.tree.join("report.md"), "from b\n").unwrap();
    std::fs::write(on_b.tree.join("only-b"), "b\n").unwrap();
    let first = world.a.finish(on_a).await;
    assert!(matches!(
        applied(&first),
        ScratchWriteBackOutcome::Applied { .. }
    ));
    let second = world.b.finish(on_b).await;
    let ScratchWriteBackOutcome::Applied { conflicts, .. } = applied(&second) else {
        panic!("the second writer folds in: {second:?}");
    };
    assert_eq!(conflicts.len(), 1, "{conflicts:?}");
    assert_eq!(conflicts[0].path.display(), "report.md");
    assert_eq!(conflicts[0].written_as.display(), "report.md~conflict-wb");
    assert!(!second.dirty, "b was brought to the merged head");

    for machine in [&world.a, &world.b] {
        let prepared = machine.prepare(&world.grant().await).await;
        assert_eq!(
            read(&prepared.tree, "report.md").as_deref(),
            Some("from a\n")
        );
        assert_eq!(
            read(&prepared.tree, "report.md~conflict-wb").as_deref(),
            Some("from b\n")
        );
        assert_eq!(read(&prepared.tree, "only-b").as_deref(), Some("b\n"));
        machine.finish(prepared).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_back_past_the_ceiling_is_refused_by_name_and_moves_nothing() {
    let world = world(Some("scratch:\n  ceilingMiB: 1\n")).await;
    let grant = world.grant().await;
    assert_eq!(grant.ceiling_bytes, 1024 * 1024);
    let seed = world.a.prepare(&grant).await;
    std::fs::write(seed.tree.join("small"), "kept\n").unwrap();
    world.a.finish(seed).await;
    let head = world.scratch().await.head_commit;

    let on_a = world.a.prepare(&world.grant().await).await;
    let big = vec![7u8; 2 * 1024 * 1024];
    std::fs::write(on_a.tree.join("export.bin"), &big).unwrap();
    let tree = on_a.tree.clone();
    let report = world.a.finish(on_a).await;
    let ScratchWriteBackOutcome::Refused(refusal) = applied(&report) else {
        panic!("past the ceiling must be refused: {report:?}");
    };
    assert_eq!(refusal.owner_uri, "cairn://p/nsc/1/1/builder");
    assert_eq!(refusal.ceiling_bytes, 1024 * 1024);
    assert_eq!(refusal.merged_bytes, big.len() as u64 + 5);
    assert_eq!(refusal.largest[0].path, "export.bin");
    assert!(report.dirty);
    assert_eq!(
        std::fs::read(tree.join("export.bin")).unwrap(),
        big,
        "a refusal leaves the files exactly where they were"
    );
    assert_eq!(world.scratch().await.head_commit, head, "nothing moved");
    let replicas = owner::replicas(&world.orch.db.local, &world.scratch_id)
        .await
        .unwrap();
    let a = replicas
        .iter()
        .find(|replica| replica.executor_id == "machine-a")
        .unwrap();
    assert!(a.dirty);
    assert_eq!(a.refused_bytes, Some(big.len() as u64 + 5));

    // Another machine meanwhile runs against the head as last synced.
    let on_b = world.b.prepare(&world.grant().await).await;
    assert!(!on_b.tree.join("export.bin").exists());
    assert_eq!(read(&on_b.tree, "small").as_deref(), Some("kept\n"));
    world.b.finish(on_b).await;

    // The next batch on a sees its files and is refused again by name...
    let again = world.a.prepare(&world.grant().await).await;
    assert!(again.tree.join("export.bin").exists());
    let report = world.a.finish(again).await;
    assert!(matches!(
        applied(&report),
        ScratchWriteBackOutcome::Refused(_)
    ));
    // ...until it shrinks the scratch, when it syncs.
    let shrunk = world.a.prepare(&world.grant().await).await;
    std::fs::remove_file(shrunk.tree.join("export.bin")).unwrap();
    std::fs::write(shrunk.tree.join("summary"), "2 MiB of sevens\n").unwrap();
    let report = world.a.finish(shrunk).await;
    assert!(
        matches!(applied(&report), ScratchWriteBackOutcome::Applied { .. }),
        "{report:?}"
    );
    assert!(!report.dirty);
    let on_b = world.b.prepare(&world.grant().await).await;
    assert_eq!(
        read(&on_b.tree, "summary").as_deref(),
        Some("2 MiB of sevens\n")
    );
    world.b.finish(on_b).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_is_never_moved_back_to_an_older_head() {
    let world = world(None).await;
    let first = world.a.prepare(&world.grant().await).await;
    std::fs::write(first.tree.join("state"), "one\n").unwrap();
    world.a.finish(first).await;
    // A grant built before a's next write-back lands...
    let stale = world.grant().await;
    let second = world.a.prepare(&stale).await;
    std::fs::write(second.tree.join("state"), "two\n").unwrap();
    world.a.finish(second).await;
    // ...must not take a back to the head it names.
    let late = world.a.prepare(&stale).await;
    assert_eq!(read(&late.tree, "state").as_deref(), Some("two\n"));
    let report = world.a.finish(late).await;
    assert!(report.materialize.is_none(), "{report:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_back_after_a_runner_restart_presents_the_credential_the_reattach_issued() {
    let world = world(None).await;
    let held = Arc::new(Mutex::new(world.attach("machine-c", 1)));
    let c = world.live_machine(held.clone());
    let on_c = c.prepare(&world.grant().await).await;
    std::fs::write(on_c.tree.join("result.txt"), "kept\n").unwrap();

    // The runner restarts while the batch runs, and the machine re-attaches
    // before the batch ends.
    world.restart_runner("machine-c", 1);
    *held.lock().unwrap() = world.attach("machine-c", 2);

    let report = c.finish(on_c).await;
    assert!(
        matches!(applied(&report), ScratchWriteBackOutcome::Applied { .. }),
        "{report:?}"
    );
    assert!(!report.dirty);
    let on_a = world.a.prepare(&world.grant().await).await;
    assert_eq!(read(&on_a.tree, "result.txt").as_deref(), Some("kept\n"));
    world.a.finish(on_a).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_credential_is_named_and_the_first_retry_after_reattach_lands() {
    let world = world(None).await;
    let held = Arc::new(Mutex::new(world.attach("machine-c", 1)));
    let c = world.live_machine(held.clone());
    let on_c = c.prepare(&world.grant().await).await;
    std::fs::write(on_c.tree.join("result.txt"), "owed\n").unwrap();

    // The runner restarts and the batch ends before the machine re-attaches.
    world.restart_runner("machine-c", 1);
    let report = c.finish(on_c).await;
    let ScratchWriteBackOutcome::Failed { stage, detail } = applied(&report) else {
        panic!("a refused credential fails the write-back: {report:?}");
    };
    assert_eq!(stage, SCRATCH_STAGE_CREDENTIAL_REFUSED, "{detail}");
    assert!(detail.contains("401"), "{detail}");
    assert!(report.dirty, "the writes stay owed on this machine");
    assert_eq!(world.scratch().await.head_commit, None, "nothing landed");

    // The re-attach brings a new credential, and the retry it triggers is
    // the one that lands.
    *held.lock().unwrap() = world.attach("machine-c", 2);
    assert_eq!(
        replica::retry_write_backs(&c.replicas, &c.transport, "resync").await,
        1
    );
    let scratch = world.scratch().await;
    assert_eq!(scratch.head_executor_id.as_deref(), Some("machine-c"));
    let on_a = world.a.prepare(&world.grant().await).await;
    assert_eq!(read(&on_a.tree, "result.txt").as_deref(), Some("owed\n"));
    world.a.finish(on_a).await;
    assert_eq!(
        replica::retry_write_backs(&c.replicas, &c.transport, "resync").await,
        0,
        "nothing is owed once it lands"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_refused_while_a_newer_credential_arrived_is_sent_once_more() {
    let world = world(None).await;
    let stale = world.attach("machine-c", 1);
    world.restart_runner("machine-c", 1);
    let fresh = world.attach("machine-c", 2);
    // The first call goes out with the credential the work began with; a
    // newer one has arrived by the time its 401 comes back.
    let reads = Arc::new(AtomicUsize::new(0));
    let c = Machine {
        replicas: Replicas::new(world._temp.path().join("c/node-scratch")),
        transport: ScratchTransport::live(cairn_tls::client().clone(), {
            let reads = reads.clone();
            move || {
                Some(if reads.fetch_add(1, Ordering::SeqCst) == 0 {
                    stale.clone()
                } else {
                    fresh.clone()
                })
            }
        }),
        writer: "wc",
    };
    let on_c = c.prepare(&world.grant().await).await;
    std::fs::write(on_c.tree.join("result.txt"), "renewed\n").unwrap();
    let report = c.finish(on_c).await;
    assert!(
        matches!(applied(&report), ScratchWriteBackOutcome::Applied { .. }),
        "{report:?}"
    );
    assert!(
        reads.load(Ordering::SeqCst) >= 2,
        "the credential was read again"
    );
}

fn advertisement(scratch_id: &str) -> ExecutorAdvertisement {
    let identity = ExecutorIdentity {
        device_id: "machine-d-device".into(),
        executor_id: "machine-d".into(),
        display_name: "machine-d".into(),
    };
    ExecutorAdvertisement {
        identity,
        capabilities: ExecutorCapabilities {
            os: "linux".into(),
            arch: "x86_64".into(),
            logical_cores: 4,
            concurrency_capacity: None,
            toolchains: Vec::new(),
            projects_served: Vec::new(),
            disk_budget_bytes: None,
            memory_budget_bytes: None,
            sandbox: None,
            toolchain_detection: None,
            build_prerequisites: None,
            containment: None,
            page_call_programs: None,
            github_credential: None,
            machine_services: Vec::new(),
            credential_floor_service_residents: false,
        },
        current_load: 0,
        observed_at_unix_ms: 0,
        liveness_observed_at_unix_ms: Some(0),
        facts_stale_refresh_blocked: None,
        last_refresh: None,
        node_scratches: vec![NodeScratchReplica {
            scratch_id: scratch_id.to_owned(),
            synced_commit: None,
            bytes: 5,
            dirty: true,
            in_use: false,
        }],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_attach_grants_the_scratches_it_advertises_before_it_returns() {
    let world = world(None).await;
    super::lifecycle::ensure_subscribed(&world.orch);
    let advertisement = advertisement(&world.scratch_id);
    let (sender, _frames) = tokio::sync::mpsc::unbounded_channel();
    let generation = world
        .orch
        .fleet
        .attach_advertised_executor(advertisement, Default::default(), sender, true, None, None)
        .unwrap();
    // Checked before this task yields once: the caller sends `Ready` as soon
    // as attach returns, and the executor retries what it owes the moment
    // Ready's credential arrives.
    assert!(world
        .orch
        .object_plane
        .scratch
        .authorizes("machine-d", generation, &world.scratch_id));
}

#[tokio::test(flavor = "multi_thread")]
async fn advertised_unknown_scratch_is_retained_but_known_closed_scratch_is_deleted() {
    let world = world(None).await;
    world
        .orch
        .db
        .local
        .execute(
            "UPDATE node_scratches SET closed_at = 1 WHERE id = ?1",
            (world.scratch_id.clone(),),
        )
        .await
        .unwrap();
    let mut advertisement = advertisement(&world.scratch_id);
    let mut unknown = advertisement.node_scratches[0].clone();
    unknown.scratch_id = "unknown-to-this-authority".into();
    // Closed comes last: receiving its delete proves the preceding unknown
    // advertisement was processed, without relying on a sleep for absence.
    advertisement.node_scratches.insert(0, unknown);
    super::lifecycle::ensure_subscribed(&world.orch);
    let (sender, mut frames) = tokio::sync::mpsc::unbounded_channel();
    world
        .orch
        .fleet
        .attach_advertised_executor(advertisement, Default::default(), sender, true, None, None)
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let ExecutorMessage::DeleteNodeScratch { scratch_id } = frames.recv().await.unwrap()
            {
                assert_eq!(scratch_id, world.scratch_id);
                break;
            }
        }
    })
    .await
    .expect("known closed scratch receives a deletion request");
    while let Ok(frame) = frames.try_recv() {
        assert!(!matches!(frame, ExecutorMessage::DeleteNodeScratch { .. }));
    }
}
