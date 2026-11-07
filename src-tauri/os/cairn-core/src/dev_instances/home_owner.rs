//! Who may touch a dev-instance home.
//!
//! A home (`~/.cairn-dev-<key>`) is keyed by the canonical branch, so every
//! launch of a branch reaches the same home whoever asks for it. That keying is
//! deliberate (docs/dev-instances.md, "Launch model"): it is what keeps a home,
//! its database, ports and build cache stable as commits land. Which checkout a
//! launch came from therefore says nothing about whose home it reaches. What
//! does is who launched it. A launch that reaches an existing home starts its
//! runner against that home's database, migrations included, and `--seed empty`
//! stops the home's runner and moves its database aside. So an agent's launch
//! may use a home only when the same job created it. The operator may use any.
//!
//! Who launched comes from the runner's own admission of the launch (the
//! operator credential, or the one-time ticket and the run behind it), never
//! from the launching shell. The rule is enforced here, in the runner, rather
//! than in `scripts/dev-instance-runtime.ts`: the runtime that runs is the one
//! at the commit being launched, so a check there could not protect a home
//! whose branch predates it, while the runner is always the current build.
//!
//! The creator record lives in the home and is written in the same rename that
//! creates the home, so no home this module creates is ever observable without
//! its record. A home with no record predates the rule (or was made by a local
//! `--here` launch, which only the operator can run), and is the operator's.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use cairn_common::paths::{dev_instance_slug, DEV_INSTANCE_HOME_PREFIX};
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};

use super::launch_ticket::Issuer;
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

/// The file inside an instance home naming who created it.
pub const CREATOR_RECORD: &str = "creator.json";

/// Who a launch is made by, as the runner admitted it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum LaunchActor {
    /// The operator credential, or a terminal no run created.
    Operator,
    /// A job: the run a ticket was issued to, or the run that created the
    /// terminal it was issued to. `node` is descriptive, for messages.
    #[serde(rename_all = "camelCase")]
    Job {
        job_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        node: Option<String>,
    },
}

/// Two actors are the same when they are both the operator, or the same job.
/// A job's node URI is how it is named, not who it is.
impl PartialEq for LaunchActor {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Operator, Self::Operator) => true,
            (Self::Job { job_id: left, .. }, Self::Job { job_id: right, .. }) => left == right,
            _ => false,
        }
    }
}

impl Eq for LaunchActor {}

impl LaunchActor {
    pub fn describe(&self) -> String {
        match self {
            Self::Operator => "the operator".to_string(),
            Self::Job {
                node: Some(node), ..
            } => format!("job {node}"),
            Self::Job { job_id, node: None } => format!("job {job_id}"),
        }
    }
}

/// Who a launch admitted with `issuer` is made by. `None` is the operator
/// credential. A ticket names a run, directly or as the creator of the terminal
/// it went to, and that run's job is the actor. A terminal no run created was
/// started by the app or `/api/invoke`, which is the operator. A run that names
/// no job cannot be attributed, and the launch is refused rather than treated
/// as the operator's.
pub async fn launch_actor(
    orch: &Orchestrator,
    issuer: Option<&Issuer>,
) -> Result<LaunchActor, String> {
    let run_id = match issuer {
        None => return Ok(LaunchActor::Operator),
        Some(Issuer::Run(run_id)) => run_id.clone(),
        Some(Issuer::Terminal(terminal)) => match &terminal.created_by_run_id {
            Some(run_id) => run_id.clone(),
            None => return Ok(LaunchActor::Operator),
        },
    };
    let owned = run_id.clone();
    let job_id = orch
        .db
        .local
        .read(move |conn| {
            let run_id = owned.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT job_id FROM runs WHERE id = ?1 LIMIT 1",
                        params![run_id.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                row.opt_text(0)
            })
        })
        .await
        .map_err(|error| {
            format!("Refused: could not read which job run {run_id} belongs to ({error}), so this launch cannot be attributed. Nothing was changed.")
        })?
        .map(|job_id| job_id.trim().to_string())
        .filter(|job_id| !job_id.is_empty())
        .ok_or_else(|| {
            format!("Refused: run {run_id} belongs to no job, so this launch cannot be attributed, and an unattributed launch from inside a run is not the operator's. Nothing was changed.")
        })?;
    let node = crate::messages::delivery::node_uri_for_job(&orch.db.local, &job_id).await;
    Ok(LaunchActor::Job { job_id, node })
}

/// The home a selector's instance lives in under `home_root`, the OS home.
/// Mirrors the runtime's `join(HOME, .cairn-dev-${slugify(branch)})`.
pub fn instance_home(home_root: &Path, selector: &str) -> Option<PathBuf> {
    dev_instance_slug(selector)
        .map(|key| home_root.join(format!("{DEV_INSTANCE_HOME_PREFIX}{key}")))
}

/// What an existing home says about who created it.
#[derive(Debug)]
enum Creator {
    Recorded(LaunchActor),
    Unrecorded,
    Unreadable(String),
}

fn read_creator(home: &Path) -> Creator {
    match std::fs::read(home.join(CREATOR_RECORD)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Creator::Unrecorded,
        Err(error) => Creator::Unreadable(error.to_string()),
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Creator::Recorded)
            .unwrap_or_else(|error| Creator::Unreadable(error.to_string())),
    }
}

fn creator_record(actor: &LaunchActor) -> Vec<u8> {
    let mut record = serde_json::to_value(actor).unwrap_or_default();
    let created = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    record["createdAtUnixMs"] = serde_json::Value::from(created);
    let mut bytes = serde_json::to_vec_pretty(&record).unwrap_or_default();
    bytes.push(b'\n');
    bytes
}

/// Create `home` with `actor`'s record already inside it: the record is written
/// into a staging directory beside the home, and the rename that publishes the
/// staging directory as the home is the creation. `Ok(false)` when another
/// launch created the home first; its record is then the one that counts.
fn create_with_record(home_root: &Path, home: &Path, actor: &LaunchActor) -> std::io::Result<bool> {
    let name = home
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    // A slug never contains '.', so no staging name can be read as an instance key.
    let staging = home_root.join(format!("{name}.claiming-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir(&staging)?;
    let published =
        std::fs::write(staging.join(CREATOR_RECORD), creator_record(actor)).and_then(|()| {
            match std::fs::rename(&staging, home) {
                Ok(()) => Ok(true),
                Err(_) if home.exists() => Ok(false),
                Err(error) => Err(error),
            }
        });
    if !matches!(published, Ok(true)) {
        let _ = std::fs::remove_dir_all(&staging);
    }
    published
}

/// Whether `actor` may use the existing `home`, judged before anything in it
/// is touched.
fn admit(
    home: &Path,
    selector: &str,
    commit_id: &str,
    actor: &LaunchActor,
    creator: Creator,
) -> Result<(), String> {
    if *actor == LaunchActor::Operator {
        return Ok(());
    }
    let created = match &creator {
        Creator::Recorded(creator) if creator == actor => return Ok(()),
        Creator::Recorded(creator) => format!("was created by {}", creator.describe()),
        Creator::Unrecorded => {
            "has no creator record, so it predates them and is the operator's".to_string()
        }
        Creator::Unreadable(why) => {
            format!("has a creator record that cannot be read ({why}), so it is treated as the operator's")
        }
    };
    let own_branch = "To run your own work, launch your own branch: `bun dev:instance --branch <your branch>`, or `bun dev:instance` from your own checkout.";
    let remedy = if selector == commit_id {
        own_branch.to_string()
    } else {
        format!(
            "To run this code in a home of your own, launch its commit instead: `bun dev:instance --branch {commit_id}` (a separate home, database and ports, built cold). {own_branch}"
        )
    };
    Err(format!(
        "Refused: the dev-instance home {} for '{selector}' {created}, and this launch was made by {}. An agent's launch never stops, reseeds, replaces or migrates a home another actor created, in any seed mode. Nothing was changed: no runner was stopped, no lease was taken, and nothing in that home was touched. {remedy}",
        home.display(),
        actor.describe(),
    ))
}

/// Claim the home `selector`'s instance lives in under `home_root` for `actor`,
/// before anything about the launch has changed. An absent home is created with
/// `actor`'s record; an existing one is admitted only for the operator or for
/// the job that created it. `commit_id` is what the launch resolved to, named
/// in a refusal's remedy.
pub fn claim_instance_home(
    home_root: &Path,
    selector: &str,
    commit_id: &str,
    actor: &LaunchActor,
) -> Result<PathBuf, String> {
    let home = instance_home(home_root, selector).ok_or_else(|| {
        format!("Refused: selector '{selector}' names no instance key. Nothing was changed.")
    })?;
    // Two rounds: if another launch publishes the home between our look and our
    // rename, the second look judges the record that launch wrote.
    for _ in 0..2 {
        match std::fs::symlink_metadata(&home) {
            Ok(_) => {
                admit(&home, selector, commit_id, actor, read_creator(&home))?;
                return Ok(home);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "Refused: could not read the dev-instance home {} ({error}). Nothing was changed.",
                    home.display()
                ));
            }
        }
        match create_with_record(home_root, &home, actor) {
            Ok(true) => return Ok(home),
            Ok(false) => continue,
            Err(error) => {
                return Err(format!(
                    "Refused: could not create the dev-instance home {} ({error}). Nothing was changed.",
                    home.display()
                ));
            }
        }
    }
    Err(format!(
        "Refused: the dev-instance home {} kept changing while this launch tried to claim it. Nothing was changed.",
        home.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::super::launch_ticket::TerminalIssuer;
    use super::super::{run_launch_session_from, DevInstanceResolution};
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use cairn_common::dev_instance_protocol::{
        DevInstanceLaunchEvent, DevInstanceLaunchFailure, DevInstanceLaunchRequest,
    };
    use cairn_common::executor_protocol::ResidentRelayId;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
    const SEEDS: [&str; 3] = ["copy", "onboarding", "empty"];

    fn job(id: &str) -> LaunchActor {
        LaunchActor::Job {
            job_id: id.into(),
            node: Some(format!("cairn://p/cairn/1/1/{id}")),
        }
    }

    /// Everything an operator's instance home and its neighbours hold, as bytes,
    /// so "unchanged" can be asserted rather than sampled.
    fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut files = BTreeMap::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(&directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    files.insert(path.clone(), Vec::new());
                    pending.push(path);
                } else {
                    files.insert(path.clone(), std::fs::read(&path).unwrap());
                }
            }
        }
        files
    }

    /// A HOME holding a populated `main` home, its registry entry, and, when
    /// `creator` is given, a creator record. `None` is a legacy home.
    fn home_root_with_main(creator: Option<&LaunchActor>) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join(".cairn-dev-main");
        match creator {
            Some(actor) => {
                claim_instance_home(root.path(), "main", COMMIT, actor).unwrap();
            }
            None => std::fs::create_dir(&home).unwrap(),
        }
        std::fs::write(home.join("cairn.db"), b"the operator's data").unwrap();
        std::fs::write(home.join("cairn.db-wal"), b"wal").unwrap();
        std::fs::write(
            root.path().join(".cairn-dev-instances.json"),
            b"{\n  \"main\": 1\n}\n",
        )
        .unwrap();
        root
    }

    async fn orchestrator() -> Orchestrator {
        let local =
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("t.db"))
                .await
                .unwrap();
        crate::project_services::testing::seed_project(&local).await;
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build()
    }

    /// A run of a job, each named `id`, as the runner records them.
    async fn run_of_job(orch: &Orchestrator, id: &str, number: i64) {
        let db = &orch.db.local;
        db.execute(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES(?1, 'p', ?2, 'I', 'active', 'active', 'none', 1, 1)",
            (id, number),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
             VALUES(?1, 'rec', ?1, 'p', 'running', 1, 1, '{}')",
            (id,),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO jobs(id, project_id, issue_id, execution_id, uri_segment, status, agent_config_id, created_at, updated_at)
             VALUES(?1, 'p', ?1, ?1, 'builder', 'running', 'build', 1, 1)",
            (id,),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO runs(id, issue_id, project_id, job_id, status, created_at, updated_at)
             VALUES(?1, ?1, 'p', ?1, 'running', 1, 1)",
            (id,),
        )
        .await
        .unwrap();
    }

    fn request(seed: &str) -> DevInstanceLaunchRequest {
        DevInstanceLaunchRequest {
            project_id: "p".into(),
            selector: None,
            checkout: None,
            source_terminal_session_id: None,
            seed: seed.into(),
            force_copy: false,
            env: Default::default(),
        }
    }

    fn resolved(selector: &str) -> DevInstanceResolution {
        DevInstanceResolution {
            project_id: "p".into(),
            repository_path: PathBuf::from("/repo"),
            selector: selector.into(),
            commit_id: COMMIT.into(),
            owner_ref: None,
        }
    }

    /// Every event a launch session emits for `actor`, run to completion
    /// against `home_root`. The test orchestrator has no executor, so a launch
    /// the home admits ends at `colocatedExecutorUnavailable`, and anything
    /// past the claim (the lease, the runtime and with it the runtime's
    /// stop-runner and backup paths) shows up as `acquiring` or `starting`.
    async fn launch_events(
        orch: &Orchestrator,
        home_root: &Path,
        seed: &str,
        selector: &str,
        actor: LaunchActor,
    ) -> Vec<DevInstanceLaunchEvent> {
        let (events, mut received) = mpsc::unbounded_channel();
        let (_controls, control_rx) = mpsc::unbounded_channel();
        run_launch_session_from(
            orch.clone(),
            request(seed),
            events,
            resolved(selector),
            actor,
            Some(home_root),
            control_rx,
        )
        .await;
        let mut seen = Vec::new();
        while let Ok(event) = received.try_recv() {
            seen.push(event);
        }
        seen
    }

    fn refusal(events: &[DevInstanceLaunchEvent]) -> String {
        assert!(
            !events.iter().any(|event| matches!(
                event,
                DevInstanceLaunchEvent::Acquiring | DevInstanceLaunchEvent::Starting
            )),
            "a refused launch takes no lease and starts no runtime: {events:?}"
        );
        match events {
            [DevInstanceLaunchEvent::Resolved { .. }, DevInstanceLaunchEvent::Failed {
                failure: DevInstanceLaunchFailure::InstanceHomeRefused { diagnostic },
            }] => diagnostic.clone(),
            other => panic!("expected a refusal right after resolution, got {other:?}"),
        }
    }

    fn admitted(events: &[DevInstanceLaunchEvent]) {
        assert!(
            matches!(
                events,
                [
                    DevInstanceLaunchEvent::Resolved { .. },
                    DevInstanceLaunchEvent::Failed {
                        failure: DevInstanceLaunchFailure::ColocatedExecutorUnavailable { .. },
                    }
                ]
            ),
            "the home admitted the launch, which went on to look for its executor: {events:?}"
        );
    }

    #[tokio::test]
    async fn an_operator_home_is_refused_to_an_agent_in_every_seed_mode_and_left_as_it_was() {
        let orch = orchestrator().await;
        let root = home_root_with_main(Some(&LaunchActor::Operator));
        let before = snapshot(root.path());
        for seed in SEEDS {
            let diagnostic =
                refusal(&launch_events(&orch, root.path(), seed, "main", job("job-a")).await);
            let home = root.path().join(".cairn-dev-main");
            for named in [
                home.display().to_string(),
                "was created by the operator".to_string(),
                "job cairn://p/cairn/1/1/job-a".to_string(),
                "Nothing was changed".to_string(),
                format!("bun dev:instance --branch {COMMIT}"),
            ] {
                assert!(diagnostic.contains(&named), "names {named}: {diagnostic}");
            }
            assert_eq!(
                snapshot(root.path()),
                before,
                "seed {seed}: the database, its sidecar, the creator record, the registry and the absent backups directory are all as they were"
            );
        }
    }

    #[tokio::test]
    async fn one_jobs_home_is_not_anothers() {
        let orch = orchestrator().await;
        let root = home_root_with_main(Some(&job("job-a")));
        let before = snapshot(root.path());
        let diagnostic =
            refusal(&launch_events(&orch, root.path(), "empty", "main", job("job-b")).await);
        assert!(
            diagnostic.contains("was created by job cairn://p/cairn/1/1/job-a"),
            "{diagnostic}"
        );
        assert_eq!(snapshot(root.path()), before);
    }

    /// A job's own home stays its own across turns, so its relaunch with
    /// `--seed empty` goes on to the runtime, whose backup-and-replace is
    /// `backupInstanceDatabase` in scripts/dev-instance-runtime.ts.
    #[tokio::test]
    async fn a_job_relaunches_the_home_it_created() {
        let orch = orchestrator().await;
        let root = tempfile::tempdir().unwrap();
        let branch = "agent/cairn-1-builder-0";
        admitted(&launch_events(&orch, root.path(), "empty", branch, job("job-a")).await);
        let home = root.path().join(".cairn-dev-agent-cairn-1-builder-0");
        let LaunchActor::Job { job_id, .. } = serde_json::from_slice::<LaunchActor>(
            &std::fs::read(home.join(CREATOR_RECORD)).unwrap(),
        )
        .unwrap() else {
            panic!("the home records the job that created it");
        };
        assert_eq!(job_id, "job-a");
        for seed in SEEDS {
            admitted(&launch_events(&orch, root.path(), seed, branch, job("job-a")).await);
        }
        // Another job asking for the same branch is refused, and the operator is not.
        refusal(&launch_events(&orch, root.path(), "empty", branch, job("job-b")).await);
        admitted(&launch_events(&orch, root.path(), "empty", branch, LaunchActor::Operator).await);
    }

    #[tokio::test]
    async fn a_home_without_a_record_is_the_operators() {
        let orch = orchestrator().await;
        let root = home_root_with_main(None);
        let before = snapshot(root.path());
        for seed in SEEDS {
            let diagnostic =
                refusal(&launch_events(&orch, root.path(), seed, "main", job("job-a")).await);
            assert!(diagnostic.contains("has no creator record"), "{diagnostic}");
        }
        assert_eq!(
            snapshot(root.path()),
            before,
            "no record is written by a refusal"
        );
        admitted(&launch_events(&orch, root.path(), "empty", "main", LaunchActor::Operator).await);
        assert_eq!(
            snapshot(root.path()),
            before,
            "the operator uses a legacy home as it is, without adding a record"
        );
    }

    #[tokio::test]
    async fn an_unreadable_record_is_read_as_the_operators() {
        let root = home_root_with_main(None);
        let home = root.path().join(".cairn-dev-main");
        std::fs::write(home.join(CREATOR_RECORD), b"{\"kind\":\"job\"").unwrap();
        let refused = claim_instance_home(root.path(), "main", COMMIT, &job("job-a")).unwrap_err();
        assert!(refused.contains("cannot be read"), "{refused}");
        assert!(claim_instance_home(root.path(), "main", COMMIT, &LaunchActor::Operator).is_ok());
    }

    /// The incident (cairn/5949, 11-05): a launch with no `--branch` from a clone
    /// whose branch is `main`, by a terminal an agent's run created, resolved to
    /// the `main` instance and, with `--seed empty`, moved the operator's
    /// `~/.cairn-dev-main` database aside.
    #[tokio::test]
    async fn an_agents_terminal_launching_main_from_a_clone_cannot_empty_the_operators_main() {
        let orch = orchestrator().await;
        run_of_job(&orch, "run-5949", 5949).await;
        let issuer = Issuer::Terminal(TerminalIssuer {
            relay_id: ResidentRelayId("relay".into()),
            uri: "cairn://p/proj2/terminal/devlaunch2".into(),
            created_by_run_id: Some("run-5949".into()),
        });
        let actor = launch_actor(&orch, Some(&issuer)).await.unwrap();
        assert_eq!(actor, job("run-5949"));

        let root = home_root_with_main(None);
        let before = snapshot(root.path());
        let diagnostic = refusal(&launch_events(&orch, root.path(), "empty", "main", actor).await);
        assert!(diagnostic.contains(".cairn-dev-main"), "{diagnostic}");
        assert_eq!(snapshot(root.path()), before);
        assert!(!root.path().join(".cairn-dev-main/backups").exists());
    }

    #[tokio::test]
    async fn the_actor_is_whoever_the_runner_admitted() {
        let orch = orchestrator().await;
        run_of_job(&orch, "run-a", 1).await;
        let terminal = |creator: Option<&str>| {
            Issuer::Terminal(TerminalIssuer {
                relay_id: ResidentRelayId("relay".into()),
                uri: "cairn://p/cairn/terminal/dev".into(),
                created_by_run_id: creator.map(str::to_string),
            })
        };
        assert_eq!(
            launch_actor(&orch, None).await.unwrap(),
            LaunchActor::Operator
        );
        assert_eq!(
            launch_actor(&orch, Some(&terminal(None))).await.unwrap(),
            LaunchActor::Operator,
            "a terminal no run created was started by the app or /api/invoke"
        );
        assert_eq!(
            launch_actor(&orch, Some(&Issuer::Run("run-a".into())))
                .await
                .unwrap(),
            job("run-a")
        );
        assert_eq!(
            launch_actor(&orch, Some(&terminal(Some("run-a"))))
                .await
                .unwrap(),
            job("run-a")
        );
        let refused = launch_actor(&orch, Some(&Issuer::Run("run-unknown".into())))
            .await
            .unwrap_err();
        assert!(
            refused.contains("run-unknown") && refused.contains("not the operator's"),
            "a run that names no job is refused, never taken for the operator: {refused}"
        );
    }

    #[test]
    fn a_new_home_is_published_with_its_record_and_leaves_no_staging_behind() {
        let root = tempfile::tempdir().unwrap();
        let home =
            claim_instance_home(root.path(), "Feature/Thing", COMMIT, &job("job-a")).unwrap();
        assert_eq!(home, root.path().join(".cairn-dev-feature-thing"));
        let entries: Vec<_> = std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(entries, [".cairn-dev-feature-thing"]);
        let inside: Vec<_> = std::fs::read_dir(&home)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(inside, [CREATOR_RECORD]);
        let record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(home.join(CREATOR_RECORD)).unwrap()).unwrap();
        assert_eq!(record["kind"], "job");
        assert_eq!(record["jobId"], "job-a");
        assert!(record["createdAtUnixMs"].as_u64().is_some());
    }

    #[test]
    fn a_refusal_for_a_commit_selector_does_not_offer_the_same_commit() {
        let root = tempfile::tempdir().unwrap();
        claim_instance_home(root.path(), COMMIT, COMMIT, &job("job-a")).unwrap();
        let refused = claim_instance_home(root.path(), COMMIT, COMMIT, &job("job-b")).unwrap_err();
        assert!(!refused.contains("launch its commit instead"), "{refused}");
        assert!(refused.contains("--branch <your branch>"), "{refused}");
    }
}
