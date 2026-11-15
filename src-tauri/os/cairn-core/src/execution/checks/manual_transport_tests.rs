use super::*;
use crate::jj::tests::{git, git_stdout, init_project, jj_bin};

const WIRE_FILE: &str = "src-tauri/cairn-exec/src/lib.rs";

fn commit(repo: &Path, files: &[(&str, &str)], message: &str) -> String {
    for (path, content) in files {
        let path = repo.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-q", "-m", message]);
    git_stdout(repo, &["rev-parse", "HEAD"])
}

#[test]
#[serial_test::serial(jj)]
fn commit_owned_policy_selects_new_paths_falls_back_and_fails_closed() {
    use crate::execution::check_identity::change_fingerprint;
    let Some(bin) = jj_bin() else { return };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path();
    init_project(repo);
    let jj = crate::jj::environment(&bin, home.path());
    let missing = git_stdout(repo, &["rev-parse", "HEAD"]);
    let changes = [
        GraphFileChange {
            path: "branch/wire_vocabulary.rs".into(),
            status: "modified".into(),
            previous_path: None,
            additions: 1,
            deletions: 0,
        },
        GraphFileChange {
            path: "src-tauri/os/cairn-common/src/wire_vocabulary.rs".into(),
            status: "added".into(),
            previous_path: None,
            additions: 1,
            deletions: 0,
        },
    ];
    let absent = executor_base_content_bundle(&jj, repo, &missing, &missing, &changes).unwrap_err();
    assert!(absent.contains(EXECUTOR_WIRE_POLICY_FILE), "{absent}");
    let base = commit(
        repo,
        &[
            (EXECUTOR_WIRE_POLICY_FILE, TEST_WIRE_POLICY),
            ("branch/wire_vocabulary.rs", "old wire"),
        ],
        "base policy",
    );
    let mut policy: serde_json::Value = serde_json::from_str(TEST_WIRE_POLICY).unwrap();
    policy["wireBehaviorPaths"].as_array_mut().unwrap().extend([
        "branch/".into(),
        "src-tauri/os/cairn-common/src/wire_vocabulary.rs".into(),
    ]);
    let head = commit(
        repo,
        &[(EXECUTOR_WIRE_POLICY_FILE, &policy.to_string())],
        "branch policy",
    );
    // The worktree is deliberately not the checked head.
    std::fs::write(repo.join(EXECUTOR_WIRE_POLICY_FILE), "not JSON").unwrap();
    let bundle: BTreeMap<String, Option<String>> = serde_json::from_str(
        &executor_base_content_bundle(&jj, repo, &base, &head, &changes).unwrap(),
    )
    .unwrap();
    assert_eq!(
        bundle["branch/wire_vocabulary.rs"].as_deref(),
        Some("old wire")
    );
    assert_eq!(
        bundle["src-tauri/os/cairn-common/src/wire_vocabulary.rs"],
        None
    );
    assert_eq!(bundle.len(), 3);
    let selected = executor_base_content_entries(&jj, repo, &base, &head, &changes).unwrap();
    let original = executor_base_content_entries(&jj, repo, &base, &base, &changes).unwrap();
    assert_ne!(
        change_fingerprint(&[], Some(&selected)),
        change_fingerprint(&[], Some(&original)),
        "a policy change must not reuse a key omitting the newly shipped base blobs"
    );
    // A head predating the file uses BASE's policy, not a compiled default.
    let fallback = executor_base_content_entries(&jj, repo, &head, &missing, &changes).unwrap();
    assert!(fallback
        .iter()
        .any(|(path, _)| path == "branch/wire_vocabulary.rs"));
    for malformed in ["not JSON", "{}", r#"{"protocolSource":42}"#] {
        let bad = commit(
            repo,
            &[(EXECUTOR_WIRE_POLICY_FILE, malformed)],
            "malformed policy",
        );
        let error = executor_base_content_bundle(&jj, repo, &base, &bad, &changes).unwrap_err();
        assert!(error.contains(EXECUTOR_WIRE_POLICY_FILE), "{error}");
    }
}

#[test]
#[serial_test::serial(jj)]
fn executor_bundle_ignores_replaced_base_blobs() {
    use crate::execution::check_identity::change_fingerprint;
    let Some(bin) = jj_bin() else { return };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path();
    init_project(repo);
    let jj = crate::jj::environment(&bin, home.path());
    let base = commit(
        repo,
        &[
            (EXECUTOR_WIRE_POLICY_FILE, TEST_WIRE_POLICY),
            (EXECUTOR_PROTOCOL_SOURCE, "original protocol"),
            ("replacement", "replacement protocol"),
        ],
        "base with policy",
    );
    let selected = executor_base_content_entries(&jj, repo, &base, &base, &[]).unwrap();
    let original_id = git_stdout(
        repo,
        &["rev-parse", &format!("{base}:{EXECUTOR_PROTOCOL_SOURCE}")],
    );
    let replacement_id = git_stdout(repo, &["rev-parse", &format!("{base}:replacement")]);
    git(repo, &["replace", &original_id, &replacement_id]);
    assert_eq!(
        git_stdout(repo, &["cat-file", "blob", &original_id]),
        "replacement protocol"
    );
    let after = executor_base_content_entries(&jj, repo, &base, &base, &[]).unwrap();
    assert_eq!(after, selected);
    assert_eq!(
        change_fingerprint(&[], Some(&after)),
        change_fingerprint(&[], Some(&selected)),
    );
    let bundle: BTreeMap<String, Option<String>> =
        serde_json::from_str(&executor_base_content_bundle(&jj, repo, &base, &base, &[]).unwrap())
            .unwrap();
    assert_eq!(
        bundle[EXECUTOR_PROTOCOL_SOURCE].as_deref(),
        Some("original protocol")
    );
    assert_eq!(bundle.len(), 1);
}

/// A protocol source at `version`, in the shape the ledger derives it from:
/// a heading for the version above the constant that reads it.
fn protocol(version: u32) -> String {
    format!(
        "/// Version {version} changes the test wire.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = {version};\n"
    )
}

fn value<'a>(env: &'a [(String, String)], key: &str) -> Option<&'a str> {
    env.iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

fn changed_paths(env: &[(String, String)]) -> BTreeSet<String> {
    value(env, CHANGED_FILES_CONTENT_ENV)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

/// A branch forked at protocol 40 that changes a wire file and bumps to 41
/// must be compared with 40 even after main independently reaches 41. The
/// target's tip would compare 41 with 41 and fail a correct change.
#[tokio::test]
#[serial_test::serial(jj)]
async fn explicit_revision_is_measured_from_its_fork_point_not_the_advanced_target() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path();
    init_project(repo);
    commit(
        repo,
        &[
            (EXECUTOR_PROTOCOL_SOURCE, &protocol(40)),
            (EXECUTOR_WIRE_POLICY_FILE, TEST_WIRE_POLICY),
        ],
        "protocol 40",
    );
    git(repo, &["checkout", "-q", "-b", "feature"]);
    let head = commit(
        repo,
        &[
            (WIRE_FILE, "pub fn wire() -> u32 { 41 }\n"),
            (EXECUTOR_PROTOCOL_SOURCE, &protocol(41)),
        ],
        "wire change with bump",
    );
    git(repo, &["checkout", "-q", "main"]);
    commit(
        repo,
        &[
            (EXECUTOR_PROTOCOL_SOURCE, &protocol(41)),
            ("unrelated.rs", "main moved on\n"),
        ],
        "main bumps independently",
    );

    let env = base_relative_transport(
        &bin,
        home.path(),
        repo,
        &head,
        "main",
        EXECUTOR_PROTOCOL_CHECK,
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        changed_paths(&env),
        BTreeSet::from([WIRE_FILE.to_string(), EXECUTOR_PROTOCOL_SOURCE.to_string()]),
        "main's later commit is not this revision's work"
    );
    let bundle: BTreeMap<String, Option<String>> =
        serde_json::from_str(value(&env, BASE_CONTENT_CONTENT_ENV).unwrap()).unwrap();
    assert_eq!(
        bundle[EXECUTOR_PROTOCOL_SOURCE].as_deref(),
        Some(protocol(40).as_str()),
        "base content must be the fork point's, not the advanced target's"
    );
    assert_eq!(
        bundle[WIRE_FILE], None,
        "the wire file is new at the fork point"
    );
}

/// A stacked child is measured against its parent's branch: the parent's work
/// is not the child's change.
#[tokio::test]
#[serial_test::serial(jj)]
async fn stacked_child_is_measured_against_its_parent_branch() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path();
    init_project(repo);
    git(repo, &["checkout", "-q", "-b", "agent/parent"]);
    commit(repo, &[("parent.rs", "parent work\n")], "parent");
    git(repo, &["checkout", "-q", "-b", "agent/child"]);
    let head = commit(repo, &[("child.rs", "child work\n")], "child");

    let env = base_relative_transport(
        &bin,
        home.path(),
        repo,
        &head,
        "agent/parent",
        "rust-lint",
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        changed_paths(&env),
        BTreeSet::from(["child.rs".to_string()])
    );
    assert_eq!(value(&env, BASE_CONTENT_CONTENT_ENV), None);
}

const LINT_CONFIG: &str = "checks:\n  lint:\n    command: bun run check:lint\n    when: review\n    impact:\n    - src/**\n";

/// A branch's lint lane carries its fork point's recorded finding set, read
/// back from the store under the input key the base's own row was recorded
/// at. Before any row records one, it carries the named absence instead.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_branch_lint_lane_carries_its_fork_points_recorded_findings() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path();
    init_project(repo);
    let base = commit(
        repo,
        &[
            (".cairn/config.yaml", LINT_CONFIG),
            ("src/Nav.tsx", "Date.now();\n"),
        ],
        "main with a lint error",
    );
    git(repo, &["checkout", "-q", "-b", "feature"]);
    let head = commit(repo, &[("src/Queue.tsx", "Date.now();\n")], "another");
    git(repo, &["checkout", "-q", "main"]);
    commit(repo, &[("src/later.tsx", "x\n")], "main moves on");

    let db = Arc::new(crate::storage::migrated_test_db("lint-baseline-transport.db").await);
    db.execute_script(
        "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-a', 'default', 'Project A', 'PA', '/tmp/project-a', 1, 1);",
    )
    .await
    .unwrap();
    let transport = || {
        base_relative_transport(
            &bin,
            home.path(),
            repo,
            &head,
            "main",
            "lint",
            Some((db.clone(), "project-a".to_string())),
        )
    };
    let baseline = |env: &[(String, String)]| -> serde_json::Value {
        serde_json::from_str(
            value(
                env,
                crate::execution::lint_baseline::LINT_BASELINE_CONTENT_ENV,
            )
            .expect("a branch lint lane carries its base"),
        )
        .unwrap()
    };

    let missing = baseline(&transport().await.unwrap());
    assert_eq!(missing["base"], "main");
    assert_eq!(
        missing["commit"],
        base.as_str(),
        "the fork point, not main's tip"
    );
    assert_eq!(missing["fingerprints"], serde_json::Value::Null);
    assert!(missing["missing"]
        .as_str()
        .unwrap()
        .contains("no recorded lint result"));

    let fingerprint =
        "eslint|react-hooks/purity|src/Nav.tsx|Error: Cannot call impure function during render";
    let document = serde_json::json!({
        "schemaVersion": 1, "parser": "eslint", "complete": true, "selection": "unknown",
        "passed": 0, "failed": 1, "skipped": 0, "failures": [],
        "lintFindings": { "tree": [fingerprint] }
    })
    .to_string();
    let insert = format!(
        "INSERT INTO check_result_cache(project_id, tree_hash, input_hash, check_name, exit_code, passed,
                 output_tail, duration_ms, ran_at, target_results_json)
             VALUES ('project-a', '{tree}', '{input}', 'lint', 1, 0, '', 1, 1, '{document}');",
        tree = missing["tree"].as_str().unwrap(),
        input = missing["inputHash"].as_str().unwrap(),
    );
    db.execute_script(&insert).await.unwrap();

    let found = baseline(&transport().await.unwrap());
    assert_eq!(found["fingerprints"], serde_json::json!([fingerprint]));
    assert!(found.get("missing").is_none());
}

/// A run of the base against itself (main's cadence, a `--branch main`
/// baseline) carries no baseline: its verdict is the whole tree.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_lint_run_of_the_base_itself_carries_no_baseline() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path();
    init_project(repo);
    let head = commit(repo, &[(".cairn/config.yaml", LINT_CONFIG)], "main");
    let db = Arc::new(crate::storage::migrated_test_db("lint-baseline-self.db").await);

    let env = base_relative_transport(
        &bin,
        home.path(),
        repo,
        &head,
        "main",
        "lint",
        Some((db, "project-a".to_string())),
    )
    .await
    .unwrap();
    assert_eq!(
        value(
            &env,
            crate::execution::lint_baseline::LINT_BASELINE_CONTENT_ENV
        ),
        None
    );
}

/// The key a branch lint run is reused and recorded under, and the baseline
/// its transport carries, from one resolved change.
async fn lint_lane(
    bin: &str,
    home: &Path,
    repo: &Path,
    head: &str,
    target: &str,
    db: Arc<LocalDb>,
) -> (String, serde_json::Value) {
    let change = resolve_base_relative_change(
        bin,
        home,
        repo,
        head,
        target,
        target,
        "lint",
        Some((db, "project-a".to_string())),
    )
    .await
    .unwrap();
    let lint: CheckCommand =
        serde_yaml::from_str("command: bun run check:lint\nwhen: review\nimpact:\n- src/**\n")
            .unwrap();
    let key = check_input_hash(
        &lint,
        &InputSelector::from_globs(&["src/**".to_string()]),
        None,
        "head-tree",
        CheckChange {
            name: "lint",
            basis: change.basis(),
        },
    );
    let env = tokio::task::spawn_blocking(move || base_relative_transport_for(&change, "lint"))
        .await
        .unwrap()
        .unwrap();
    let baseline = serde_json::from_str(
        value(
            &env,
            crate::execution::lint_baseline::LINT_BASELINE_CONTENT_ENV,
        )
        .expect("a branch lint lane carries its base"),
    )
    .unwrap();
    (key, baseline)
}

fn record_lint_tree_findings(baseline: &serde_json::Value, findings: &[&str]) -> String {
    let document = serde_json::json!({
        "schemaVersion": 1, "parser": "eslint", "complete": true, "selection": "unknown",
        "passed": 0, "failed": findings.len(), "skipped": 0, "failures": [],
        "lintFindings": { "tree": findings }
    });
    format!(
        "INSERT INTO check_result_cache(project_id, tree_hash, input_hash, check_name, exit_code, passed,
                 output_tail, duration_ms, ran_at, target_results_json)
             VALUES ('project-a', '{tree}', '{input}', 'lint', 1, 0, '', 1, 1, '{document}');",
        tree = baseline["tree"].as_str().unwrap(),
        input = baseline["inputHash"].as_str().unwrap(),
    )
}

/// cairn/6374: one head tree, measured against two bases, is two lint
/// questions once their recorded finding sets differ, and a base's set
/// arriving after a run recorded its absence is a new question too. The
/// key moves with exactly what the transport carries, so a lint row
/// recorded against one base's set is never reused for another.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_lint_lanes_key_moves_with_the_base_findings_it_carries() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path();
    init_project(repo);
    commit(
        repo,
        &[
            (".cairn/config.yaml", LINT_CONFIG),
            ("src/Nav.tsx", "Date.now();\n"),
        ],
        "main with a lint error",
    );
    git(repo, &["checkout", "-q", "-b", "agent/parent"]);
    commit(repo, &[("src/Nav.tsx", "fixed\n")], "parent fixes it");
    git(repo, &["checkout", "-q", "-b", "feature"]);
    let head = commit(repo, &[("src/Queue.tsx", "Date.now();\n")], "child");

    let db = Arc::new(crate::storage::migrated_test_db("lint-baseline-key.db").await);
    db.execute_script(
        "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-a', 'default', 'Project A', 'PA', '/tmp/project-a', 1, 1);",
    )
    .await
    .unwrap();
    let home = home.path();

    // Neither base has recorded its set: both lanes report no verdict.
    let (main_unrecorded, main_baseline) =
        lint_lane(&bin, home, repo, &head, "main", db.clone()).await;
    let (parent_unrecorded, parent_baseline) =
        lint_lane(&bin, home, repo, &head, "agent/parent", db.clone()).await;
    assert_eq!(main_baseline["fingerprints"], serde_json::Value::Null);
    assert_ne!(main_baseline["tree"], parent_baseline["tree"]);

    let finding =
        "eslint|react-hooks/purity|src/Queue.tsx|Error: Cannot call impure function during render";
    db.execute_script(record_lint_tree_findings(&main_baseline, &[finding]))
        .await
        .unwrap();
    db.execute_script(record_lint_tree_findings(&parent_baseline, &[]))
        .await
        .unwrap();

    let (main_recorded, main_baseline) =
        lint_lane(&bin, home, repo, &head, "main", db.clone()).await;
    let (parent_recorded, parent_baseline) =
        lint_lane(&bin, home, repo, &head, "agent/parent", db.clone()).await;
    assert_eq!(main_baseline["fingerprints"], serde_json::json!([finding]));
    assert_eq!(parent_baseline["fingerprints"], serde_json::json!([]));
    assert_ne!(
        main_recorded, parent_recorded,
        "the finding is the base's own against main and the branch's against its parent"
    );
    assert_ne!(
        main_unrecorded, main_recorded,
        "a set that arrives later is a new question"
    );
    assert_ne!(
        parent_unrecorded, parent_recorded,
        "a clean base is not an unrecorded one"
    );
    assert_eq!(
        lint_lane(&bin, home, repo, &head, "main", db.clone())
            .await
            .0,
        main_recorded,
        "the same base and set reuse"
    );
}

/// An integration target that does not resolve refuses the run; it never
/// degrades to another base.
#[tokio::test]
#[serial_test::serial(jj)]
async fn unresolvable_integration_target_refuses() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path();
    init_project(repo);
    let head = git_stdout(repo, &["rev-parse", "HEAD"]);

    let error = base_relative_transport(
        &bin,
        home.path(),
        repo,
        &head,
        "agent/vanished-parent",
        EXECUTOR_PROTOCOL_CHECK,
        None,
    )
    .await
    .unwrap_err();

    assert!(error.contains("agent/vanished-parent"), "{error}");
}

/// The run row names the job's own integration target, read from the
/// database it is handed: the owning one, which for a team job is its replica.
#[tokio::test]
async fn run_row_carries_a_stacked_childs_integration_target() {
    let db = crate::storage::migrated_test_db("manual-run-row.db").await;
    for statement in [
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
        "INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('proj','ws','P','p','/repo','main',1,1)",
        "INSERT INTO jobs(id, project_id, branch, base_branch, status, created_at, updated_at)
             VALUES ('child','proj','agent/child','agent/parent','running',1,1)",
        "INSERT INTO jobs(id, project_id, branch, status, created_at, updated_at)
             VALUES ('root','proj','agent/root','running',1,1)",
        "INSERT INTO runs(id, project_id, job_id, created_at, updated_at) VALUES ('run-child','proj','child',1,1)",
        "INSERT INTO runs(id, project_id, job_id, created_at, updated_at) VALUES ('run-root','proj','root',1,1)",
    ] {
        db.execute(statement, ()).await.unwrap();
    }

    let child = load_manual_run_row(&db, "run-child").await.unwrap();
    assert_eq!(child.integration_target, "agent/parent");
    assert_eq!(child.branch.as_deref(), Some("agent/child"));

    let root = load_manual_run_row(&db, "run-root").await.unwrap();
    assert_eq!(root.integration_target, "main");

    let missing = load_manual_run_row(&db, "run-absent").await.unwrap_err();
    assert!(missing.contains("run-absent"), "{missing}");
}
