use super::*;
use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
use crate::mcp::handlers::branch::BranchResolution;

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

/// A write prepares against the branch's tip, so the resolution it holds
/// after sealing still names the parent. The wave it fires has to plan, key,
/// and materialize the sealed commit, whose tree is the one carrying the
/// edit. The parent here already selects the check on its own changes, the
/// specimen shape: evaluating it yields a plausible verdict about a tree that
/// lacks the edit, recorded under the wrong commit.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_sealed_write_plans_its_checks_against_the_commit_it_sealed() {
    let Some(bin) = jj_bin() else {
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path().to_path_buf();
    init_project(&repo);
    let base = commit(
        &repo,
        &[
            (
                ".cairn/config.yaml",
                "checks:\n  fmt:\n    command: cargo fmt --check\n    impact:\n      - src/**\n    when: write\n",
            ),
            ("src/lib.rs", "fn before() {}\n"),
        ],
        "declare fmt",
    );
    git(&repo, &["checkout", "-q", "-b", "agent/feature"]);
    let parent = commit(&repo, &[("src/other.rs", "fn other() {}\n")], "parent");
    let sealed = commit(
        &repo,
        &[("src/lib.rs", "#[allow(dead_code)]\nfn before() {}\n")],
        "the write",
    );

    let jj = crate::jj::environment(&bin, home.path());
    let coordinates = {
        let (jj, home, repo) = (jj.clone(), home.path().to_path_buf(), repo.clone());
        tokio::task::spawn_blocking(move || crate::jj::coordinate_repository(&jj, &home, &repo))
            .await
            .unwrap()
            .unwrap()
    };
    let prepared_against = BranchResolution {
        project_id: "project".into(),
        repository_path: coordinates.clone(),
        object_repository_path: repo.clone(),
        rev: "agent/feature".into(),
        commit_id: parent.clone(),
        default_commit_id: base.clone(),
        default_branch: "main".into(),
    };

    let evaluated = WriteCheckTarget::Sealed {
        branch: &prepared_against,
        commit: &sealed,
    }
    .sealed_resolution()
    .expect("a sealed target resolves without the session");
    assert_eq!(evaluated.commit_id, sealed);
    assert_eq!(
        evaluated.rev, "agent/feature",
        "the branch coordinates carry"
    );
    assert!(WriteCheckTarget::CurrentHead.sealed_resolution().is_none());

    let db = Arc::new(crate::storage::migrated_test_db("write-wave-target.db").await);
    let plan_at = |head: &str| {
        let contract = checks_contract_at_commit(&repo, head).expect("fmt is declared");
        assert_eq!(contract.defined_by_commit, head);
        let planning = WriteWavePlanning {
            jj: jj.clone(),
            repository: coordinates.clone(),
            object_repository: repo.clone(),
            head: head.to_string(),
            base: base.clone(),
            contract: contract.contract,
            db: db.clone(),
            project_id: "project".into(),
            job_id: "job".into(),
        };
        async move {
            tokio::task::spawn_blocking(move || plan_write_wave(planning))
                .await
                .unwrap()
                .expect("fmt applies to a src change")
                .unwrap()
        }
    };

    let wave = plan_at(&evaluated.commit_id).await;
    assert_eq!(
        wave.sealed_commit, sealed,
        "the slot materializes, and the observations record, the sealed commit"
    );
    let names: Vec<&str> = wave
        .keyed
        .iter()
        .map(|(plan, _)| plan.name.as_str())
        .collect();
    assert_eq!(names, ["fmt"]);
    assert_eq!(
        wave.tree_hash,
        logical_tree_hash(&jj, &coordinates, &sealed).unwrap(),
        "the observation's tree is the sealed commit's"
    );
    let shipped = wave
        .transport
        .iter()
        .find(|(name, _)| name == CHANGED_FILES_CONTENT_ENV)
        .map(|(_, value)| value.lines().collect::<BTreeSet<_>>())
        .unwrap();
    assert!(shipped.contains("src/lib.rs"), "{shipped:?}");
    assert!(
        git_stdout(
            &repo,
            &["show", &format!("{}:src/lib.rs", wave.sealed_commit)]
        )
        .contains("#[allow(dead_code)]"),
        "the evaluated tree contains the edit"
    );

    // What the pre-write resolution would have evaluated: a real plan for
    // the same check, keyed to a different tree that lacks the edit.
    let stale = plan_at(&prepared_against.commit_id).await;
    assert_eq!(stale.sealed_commit, parent);
    assert_ne!(stale.tree_hash, wave.tree_hash);
    assert_ne!(
        stale.keyed[0].1, wave.keyed[0].1,
        "fmt's key moves with the edit"
    );
    assert!(
        !git_stdout(&repo, &["show", &format!("{parent}:src/lib.rs")])
            .contains("#[allow(dead_code)]")
    );
}

const MIGRATIONS_CONFIG: &str = "checks:\n  migrations:\n    command: bun run check:migrations\n    baseRelative: true\n    impact:\n      - src-tauri/turso_migrations/**\n  fmt:\n    command: cargo fmt --check\n    fixes: true\n    impact:\n      - src/**\n";
const MIGRATIONS: &str = "src-tauri/turso_migrations";

fn migration(name: &str) -> String {
    format!("{MIGRATIONS}/{name}")
}

fn rename(repo: &Path, from: &str, to: &str, message: &str) -> String {
    git(repo, &["mv", from, to]);
    commit(repo, &[], message)
}

fn coordinate(jj: &JjEnv, home: &Path, repo: &Path) -> PathBuf {
    crate::jj::coordinate_repository(jj, home, repo).unwrap()
}

fn wave_planning(
    jj: &JjEnv,
    coordinates: &Path,
    repo: &Path,
    db: &Arc<LocalDb>,
    base: &str,
    head: &str,
) -> WriteWavePlanning {
    WriteWavePlanning {
        jj: jj.clone(),
        repository: coordinates.to_path_buf(),
        object_repository: repo.to_path_buf(),
        head: head.to_string(),
        base: base.to_string(),
        contract: checks_contract_at_commit(repo, head)
            .expect("the head declares its checks")
            .contract,
        db: db.clone(),
        project_id: "project".into(),
        job_id: "job".into(),
    }
}

fn shipped(transport: &[(String, String)]) -> BTreeSet<String> {
    transport
        .iter()
        .find(|(name, _)| name == CHANGED_FILES_CONTENT_ENV)
        .map(|(_, value)| value.lines().map(str::to_string).collect())
        .expect("every wave ships its changed files")
}

fn key_of<'a>(wave: &'a [(CheckPlan, String)], name: &str) -> &'a str {
    wave.iter()
        .find(|(plan, _)| plan.name == name)
        .map(|(_, key)| key.as_str())
        .unwrap_or_else(|| panic!("{name} is planned"))
}

/// A pure rename (R100) ships its destination. When the source existed at
/// the base, the rename is also a deletion the change made, so the source
/// ships too.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_pure_rename_ships_its_destination_and_any_source_the_base_had() {
    let Some(bin) = jj_bin() else {
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path().to_path_buf();
    init_project(&repo);
    let base = commit(
        &repo,
        &[
            (".cairn/config.yaml", MIGRATIONS_CONFIG),
            (
                &migration("0001_a.sql"),
                "-- lineage: shared\nCREATE TABLE a(id);\n",
            ),
        ],
        "main",
    );
    git(&repo, &["checkout", "-q", "-b", "agent/feature"]);
    commit(
        &repo,
        &[(
            &migration("0002_x.sql"),
            "-- lineage: shared\nCREATE TABLE x(id);\n",
        )],
        "add a migration",
    );
    let renamed_own = rename(
        &repo,
        &migration("0002_x.sql"),
        &migration("NEXT_x.sql"),
        "the number is the landing's",
    );
    let renamed_base = rename(
        &repo,
        &migration("0001_a.sql"),
        &migration("NEXT_a.sql"),
        "and rename one the base had",
    );

    let jj = crate::jj::environment(&bin, home.path());
    let coordinates = {
        let (jj, home, repo) = (jj.clone(), home.path().to_path_buf(), repo.clone());
        tokio::task::spawn_blocking(move || coordinate(&jj, &home, &repo))
            .await
            .unwrap()
    };
    let db = Arc::new(crate::storage::migrated_test_db("rename-transport.db").await);
    for (head, expected) in [
        (renamed_own, vec![migration("NEXT_x.sql")]),
        (
            renamed_base,
            vec![
                migration("0001_a.sql"),
                migration("NEXT_a.sql"),
                migration("NEXT_x.sql"),
            ],
        ),
    ] {
        let planning = wave_planning(&jj, &coordinates, &repo, &db, &base, &head);
        let wave = tokio::task::spawn_blocking(move || plan_write_wave(planning))
            .await
            .unwrap()
            .expect("migrations applies to a migration change")
            .unwrap();
        assert_eq!(
            shipped(&wave.transport),
            expected.into_iter().collect::<BTreeSet<_>>(),
            "at {head}"
        );
    }
}

/// The cairn/5942 specimen. A wave planned at a head that added
/// `0002_x.sql` folds its formatter fix after the branch moved on to a head
/// that renamed it to `NEXT_x.sql`, so the fix lands on that newer head and
/// the wave re-verifies there. It must re-verify with the newer head's
/// change: the change it was planned with names `0002_x.sql`, and
/// check:migrations reads a `NEXT_` file missing from it as one this change
/// did not add. And the re-verified verdict must be keyed exactly as the
/// newer head's own wave keys it, so neither answers for the other wrongly.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_fix_landing_on_a_moved_head_re_verifies_with_that_heads_change() {
    let Some(bin) = jj_bin() else {
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path().to_path_buf();
    init_project(&repo);
    let base = commit(
        &repo,
        &[
            (".cairn/config.yaml", MIGRATIONS_CONFIG),
            ("src/lib.rs", "fn a() {}\n"),
        ],
        "main",
    );
    git(&repo, &["checkout", "-q", "-b", "agent/feature"]);
    let planned_at = commit(
        &repo,
        &[
            ("src/lib.rs", "fn a()  {}\n"),
            (
                &migration("0002_x.sql"),
                "-- lineage: shared\nCREATE TABLE x(id);\n",
            ),
        ],
        "the write the wave was planned at",
    );
    let moved_to = rename(
        &repo,
        &migration("0002_x.sql"),
        &migration("NEXT_x.sql"),
        "the next write, committed while that wave ran",
    );
    let fixed = commit(
        &repo,
        &[("src/lib.rs", "fn a() {}\nfn b() {}\n")],
        "fix: apply write-check changes",
    );

    let jj = crate::jj::environment(&bin, home.path());
    let coordinates = {
        let (jj, home, repo) = (jj.clone(), home.path().to_path_buf(), repo.clone());
        tokio::task::spawn_blocking(move || coordinate(&jj, &home, &repo))
            .await
            .unwrap()
    };
    let db = Arc::new(crate::storage::migrated_test_db("moved-head-fold.db").await);
    let plan = |head: &str| {
        let planning = wave_planning(&jj, &coordinates, &repo, &db, &base, head);
        async move {
            tokio::task::spawn_blocking(move || plan_write_wave(planning))
                .await
                .unwrap()
                .expect("both checks apply")
                .unwrap()
        }
    };
    let wave = plan(&planned_at).await;
    assert!(
        !shipped(&wave.transport).contains(&migration("NEXT_x.sql")),
        "the planned head never had the NEXT_ file"
    );

    let checks = checks_contract_at_commit(&repo, &fixed)
        .unwrap()
        .contract
        .checks;
    let replanning = FixReplanning {
        jj: jj.clone(),
        repository: coordinates.clone(),
        object_repository: repo.clone(),
        base: base.clone(),
        head: fixed.clone(),
        checks,
        extra_inputs: HashMap::new(),
        plans: wave.keyed.iter().map(|(plan, _)| plan.clone()).collect(),
        keys_before: wave.keyed.iter().map(|(_, key)| key.clone()).collect(),
        folded_by: wave
            .keyed
            .iter()
            .position(|(plan, _)| plan.name == "fmt")
            .unwrap(),
        fixed_paths: vec!["src/lib.rs".to_string()],
        fix_landed_on_tested_base: false,
        db: db.clone(),
        project_id: "project".into(),
    };
    let (rekeyed, invalidated, _, transport) =
        tokio::task::spawn_blocking(move || replan_onto_fix(replanning))
            .await
            .unwrap()
            .unwrap();

    assert_eq!(
        shipped(&transport),
        BTreeSet::from([migration("NEXT_x.sql"), "src/lib.rs".to_string()]),
        "the fixed commit's lanes carry the fixed commit's change"
    );
    let migrations = wave
        .keyed
        .iter()
        .position(|(plan, _)| plan.name == "migrations")
        .unwrap();
    assert!(
        invalidated.contains(&migrations),
        "the newer head's migration change invalidated the planned verdict"
    );
    let own_wave = plan(&fixed).await;
    assert_eq!(
        key_of(&rekeyed, "migrations"),
        key_of(&own_wave.keyed, "migrations"),
        "the re-verified verdict is keyed as the fixed commit's own wave keys it"
    );
    assert_eq!(shipped(&transport), shipped(&own_wave.transport));
    let _ = moved_to;
}
