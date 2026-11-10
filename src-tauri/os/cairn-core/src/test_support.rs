use crate::storage::LocalDb;

// Seal the cairn-core test binary away from every live Cairn home before any
// test runs; see `cairn_common::test_seal`.
ctor::declarative::ctor! {
    #[ctor(unsafe)]
    fn seal_test_process() {
        cairn_common::test_seal::seal_process();
    }
}

/// The seal this process runs inside, whether its constructor applied it or the
/// Rust test runner did.
fn seal_root() -> std::path::PathBuf {
    std::env::var_os(cairn_common::test_seal::SEALED_ENV)
        .map(std::path::PathBuf::from)
        .expect("the test process is sealed before main")
}

#[test]
fn the_test_process_runs_inside_its_seal() {
    use cairn_common::test_seal::is_within;
    let root = seal_root();
    let home = std::path::PathBuf::from(std::env::var_os("HOME").expect("HOME"));
    for (name, path) in [
        ("cairn home", cairn_common::paths::cairn_home()),
        ("HOME", home),
        ("temp dir", std::env::temp_dir()),
    ] {
        assert!(
            is_within(&root, &path),
            "{name} {} escapes the seal {}",
            path.display(),
            root.display()
        );
    }
    let ceilings = std::env::var_os("GIT_CEILING_DIRECTORIES").expect("git ceilings");
    assert!(
        std::env::split_paths(&ceilings).any(|ceiling| ceiling == root),
        "the seal root {} is not a git ceiling",
        root.display()
    );
}

/// A temp directory that is not a repository must not resolve to one above it.
/// Unsealed, with the temp directory a build slot's scratch under the live
/// home, this resolved the home's own `.git`, and a remote or commit made "in a
/// temp repo" landed in the operator's workspace repository.
#[test]
fn git_cannot_discover_a_repository_above_the_seal() {
    let dir = tempfile::tempdir().unwrap();
    let out = crate::env::git()
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(dir.path())
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "a bare temp directory resolved to the repository at {}",
        String::from_utf8_lossy(&out.stdout).trim()
    );
}

/// Log targets some test has asked to record, and the lines recorded for them.
static CAPTURED_TARGETS: std::sync::Mutex<Vec<&'static str>> = std::sync::Mutex::new(Vec::new());
static CAPTURED_LINES: std::sync::Mutex<Vec<(String, String)>> = std::sync::Mutex::new(Vec::new());

/// The one logger of the cairn-core test binary. `log` admits a single logger
/// per process, so every test that asserts on a log line records it here and
/// reads back only the lines it can attribute to itself.
struct CapturedLog;

impl log::Log for CapturedLog {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Info
            && CAPTURED_TARGETS
                .lock()
                .unwrap()
                .contains(&metadata.target())
    }
    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            CAPTURED_LINES
                .lock()
                .unwrap()
                .push((record.target().to_string(), record.args().to_string()));
        }
    }
    fn flush(&self) {}
}

/// Record every Info-or-louder line logged to `target` from now on.
pub(crate) fn capture_log(target: &'static str) {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        log::set_logger(&CapturedLog).expect("the cairn-core test binary installs no other logger");
        log::set_max_level(log::LevelFilter::Info);
    });
    let mut targets = CAPTURED_TARGETS.lock().unwrap();
    if !targets.contains(&target) {
        targets.push(target);
    }
}

/// The lines recorded for `target`, across the whole test binary.
pub(crate) fn captured_log(target: &str) -> Vec<String> {
    CAPTURED_LINES
        .lock()
        .unwrap()
        .iter()
        .filter(|(recorded, _)| recorded == target)
        .map(|(_, line)| line.clone())
        .collect()
}

/// The frozen snapshot of a shipping recipe — a Builder agent node that writes
/// `create-pr`, then the PR node — as an issue execution carries it.
///
/// Every production insert into `executions` freezes a snapshot, and the
/// advancement recompute and downstream-input resolution load it, so a fixture
/// execution without one is a row the product cannot reach. The PR node keeps
/// the topology derivation reading the builder as shipping work rather than a
/// standing (long-running) node.
pub(crate) fn shipping_execution_snapshot_json(issue_id: &str, project_id: &str) -> String {
    use crate::models::{
        AgentNodeConfig, ConfirmPolicy, ExecutionSnapshot, NodePosition, RecipeNode,
        RecipeNodeType, RecipeSnapshot, RecipeTrigger, SchemaConfig, TriggerContext, TriggerType,
    };
    let node = |id: &str, name: &str, node_type, agent_config| RecipeNode {
        id: id.to_string(),
        node_type,
        name: name.to_string(),
        position: NodePosition { x: 0.0, y: 0.0 },
        parent_id: None,
        trigger_config: None,
        agent_config,
        action_config: None,
        checkpoint_config: None,
        artifact_config: None,
        condition_config: None,
        decision_config: None,
        context_config: None,
    };
    let builder = node(
        "builder",
        "Builder",
        RecipeNodeType::Agent,
        Some(AgentNodeConfig {
            models: None,
            constraints: Default::default(),
            agent_config_id: Some("builder".to_string()),
            output_schema: Some(SchemaConfig {
                name: "create-pr".to_string(),
                schema: None,
                confirm_policy: ConfirmPolicy::Auto,
                tool_name: None,
                description: None,
            }),
            git_config: None,
        }),
    );
    let pr = node("pr", "PR", RecipeNodeType::Pr, None);
    serde_json::to_string(&ExecutionSnapshot {
        continuity: Default::default(),
        branch_target: Default::default(),
        recipe: RecipeSnapshot {
            id: "recipe".to_string(),
            name: "Recipe".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: vec![builder, pr],
            edges: vec![],
        },
        agents: std::collections::HashMap::new(),
        skills: std::collections::HashMap::new(),
        trigger_context: TriggerContext {
            issue_id: Some(issue_id.to_string()),
            project_id: project_id.to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
        inline_delegated_packets: vec![],
        created_at: 1,
    })
    .unwrap()
}

pub(crate) struct ThreadFixture {
    pub project_id: &'static str,
    pub thread_id: &'static str,
    pub session_job_id: &'static str,
    pub task_job_id: &'static str,
    pub child_issue_job_id: &'static str,
}

/// The production shape of a thread that has actually run: session job with a
/// synthetic host execution (seq NULL, issue_id NULL), children by
/// `issues.parent_thread_id`, spawned tasks carrying the inherited thread_id.
/// Mirrors thread session creation and host snapshot insertion; do not invent
/// ownership columns in individual tests.
pub(crate) async fn seed_thread_session(db: &LocalDb) -> ThreadFixture {
    let fixture = ThreadFixture {
        project_id: "thread-project",
        thread_id: "thread-id",
        session_job_id: "thread-session-job",
        task_job_id: "thread-task-job",
        child_issue_job_id: "thread-child-issue-job",
    };
    for sql in [
        "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('thread-workspace','Thread Workspace',1,1)",
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('thread-project','thread-workspace','Thread Project','thread-project','/tmp/thread-project',1,1)",
        "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at) VALUES ('thread-id','thread-project','design','active','none',1,1)",
        "INSERT INTO jobs (id, thread_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment) VALUES ('thread-session-job','thread-id','thread-project','Thread','thread','running',1,1,'thread')",
        "INSERT INTO executions (id, recipe_id, project_id, status, started_at) VALUES ('thread-host-execution','host-thread-session-job','thread-project','running',1)",
        "UPDATE jobs SET execution_id='thread-host-execution' WHERE id='thread-session-job'",
        "INSERT INTO jobs (id, parent_job_id, thread_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment) VALUES ('thread-task-job','thread-session-job','thread-id','thread-project','Task','build','running',1,1,'task')",
        "INSERT INTO issues (id, project_id, number, title, status, parent_thread_id, created_at, updated_at) VALUES ('thread-child-issue','thread-project',7,'Child issue','active','thread-id',1,1)",
        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('thread-child-execution','recipe','thread-child-issue','thread-project','running',1,1)",
        "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment) VALUES ('thread-child-issue-job','thread-child-execution','thread-child-issue','thread-project','Builder','build','running',1,1,'builder')",
    ] {
        db.execute(sql, ()).await.unwrap();
    }
    fixture
}
