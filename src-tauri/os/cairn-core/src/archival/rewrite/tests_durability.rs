use super::testsupport::*;
use super::*;

/// A rebase rewrites a branch's commits while the transcript keeps the ids it
/// recorded. A recorded commit that is no longer on any branch is packed
/// anyway: its write keeps that exact commit as its coordinate, a read of its
/// tree coordinatizes to it, and both reconstruct after every branch is deleted
/// and the rewritten commit is collected from the repository.
#[tokio::test]
async fn rewritten_commit_is_packed_and_survives_gc() {
    let fx = build_fixture();
    git(
        &fx.repo,
        &["checkout", "-q", "-b", "pre-rebase", &fx.anchor],
    );
    write_file(&fx.repo, "a.txt", b"REWRITTEN\n");
    write_file(&fx.repo, "c.txt", b"rewritten c\n");
    let rewritten = commit_all(&fx.repo, "pre-rebase work");
    git(&fx.repo, &["checkout", "-q", "work"]);
    let expected_diff = git(&fx.repo, &["show", "--format=", "--no-color", &rewritten]);

    let db = migrated_test_db("archival-rewritten.db").await;
    seed_chain(&db, fx.repo.to_str().unwrap(), false).await;
    let stored = rendered(&[("file:a.txt", b"REWRITTEN\n")]);
    let write = assistant_write("w1");
    insert_event(&db, "a-w1", "run", 1, 1, "assistant", &write).await;
    let result = write_result("w1", &short(&fx.repo, &rewritten));
    insert_event(&db, "e-w1", "run", 2, 2, "tool_result", &result).await;
    let read = assistant_read("r1", &["file:a.txt"]);
    insert_event(&db, "a-r1", "run", 3, 3, "assistant", &read).await;
    insert_event(
        &db,
        "e-r1",
        "run",
        4,
        4,
        "tool_result",
        &read_result("r1", &stored),
    )
    .await;

    let summary = archive(&db, Some(fx.range())).await.unwrap();
    assert_eq!(summary.gitcoord_write, 1);
    assert_eq!(summary.unresolved_write, 0);
    assert_eq!(
        summary.gitcoord_read, 1,
        "the replayed tracker sits at the rewritten commit"
    );
    assert_eq!(summary.mismatch_fallback, 0);
    assert_eq!(
        content_commit(&db, "a-w1").await.as_deref(),
        Some(rewritten.as_str())
    );
    assert_eq!(
        content_commit(&db, "e-r1").await.as_deref(),
        Some(rewritten.as_str())
    );

    git(&fx.repo, &["checkout", "-q", "main"]);
    git(&fx.repo, &["branch", "-D", "-q", "work", "pre-rebase"]);
    git(&fx.repo, &["reflog", "expire", "--expire=now", "--all"]);
    git(&fx.repo, &["gc", "-q", "--prune=now"]);
    let rewritten_present = Command::new("git")
        .current_dir(&fx.repo)
        .args(["cat-file", "-e", &format!("{rewritten}^{{commit}}")])
        .status()
        .unwrap()
        .success();
    assert!(
        !rewritten_present,
        "only the pack still holds the rewritten commit"
    );

    let reconstructed = reconstruct_events(&db, load_events(&db).await).await;
    let by_id: HashMap<&str, &Event> = reconstructed.iter().map(|e| (e.id.as_str(), e)).collect();
    assert_eq!(tool_result_of(by_id["e-r1"]), stored);
    let restored: Value = serde_json::from_str(&by_id["a-w1"].data).unwrap();
    let combined: String = restored["toolUses"][0]["input"]["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["payload"]["diff"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(combined, expected_diff.trim_start_matches('\n'));
}

/// A write whose recorded commit the repository does not know (collected
/// before archival, or sealed somewhere never exported) keeps its assistant
/// event verbatim as zstd, and the summary counts it.
#[tokio::test]
async fn unknown_write_commit_stays_zstd_and_is_counted() {
    let fx = build_fixture();
    let db = migrated_test_db("archival-unknown-commit.db").await;
    seed_chain(&db, fx.repo.to_str().unwrap(), false).await;
    let write = assistant_write("w1");
    insert_event(&db, "a-w1", "run", 1, 1, "assistant", &write).await;
    let result = write_result("w1", "0123456789abcdef0123456789abcdef01234567");
    insert_event(&db, "e-w1", "run", 2, 2, "tool_result", &result).await;

    let summary = archive(&db, Some(fx.range())).await.unwrap();
    assert_eq!(summary.gitcoord_write, 0);
    assert_eq!(summary.unresolved_write, 1);
    assert_eq!(storage_mode(&db, "a-w1").await.as_deref(), Some("zstd"));
    let reconstructed = reconstruct_events(&db, load_events(&db).await).await;
    let a_w1 = reconstructed.iter().find(|e| e.id == "a-w1").unwrap();
    assert_eq!(a_w1.data, write, "the write is kept verbatim");
}

/// The guarantee the pack exists for: once the branch is deleted and its
/// objects are garbage-collected, archived reads and writes still reconstruct,
/// and the node diff still renders from `execution_history`.
#[tokio::test]
async fn archive_survives_branch_deletion_and_gc() {
    let fx = build_fixture();
    let db = migrated_test_db("archival-survives-gc.db").await;
    seed_chain(&db, fx.repo.to_str().unwrap(), false).await;

    let stored_w1 = rendered(&[("file:a.txt", b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n")]);
    let stored_c = rendered(&[("file:c.txt", b"new file\n")]);
    let w1_short = short(&fx.repo, &fx.w1);
    insert_event(
        &db,
        "a-w1",
        "run",
        1,
        1,
        "assistant",
        &assistant_write("w1"),
    )
    .await;
    insert_event(
        &db,
        "e-w1",
        "run",
        2,
        2,
        "tool_result",
        &write_result("w1", &w1_short),
    )
    .await;
    let read_a = assistant_read("r1", &["file:a.txt"]);
    insert_event(&db, "a-r1", "run", 3, 3, "assistant", &read_a).await;
    insert_event(
        &db,
        "e-r1",
        "run",
        4,
        4,
        "tool_result",
        &read_result("r1", &stored_w1),
    )
    .await;
    let read_c = assistant_read("r2", &["file:c.txt"]);
    insert_event(&db, "a-r2", "run", 5, 5, "assistant", &read_c).await;
    insert_event(
        &db,
        "e-r2",
        "run",
        6,
        6,
        "tool_result",
        &read_result("r2", &stored_c),
    )
    .await;
    let expected_diff = git(&fx.repo, &["show", "--format=", "--no-color", &fx.w1]);

    let summary = archive(&db, Some(fx.range())).await.unwrap();
    assert_eq!(summary.gitcoord_read, 2);
    assert_eq!(summary.gitcoord_write, 1);
    assert!(has_history(&db).await);

    git(&fx.repo, &["checkout", "-q", "main"]);
    git(&fx.repo, &["branch", "-D", "-q", "work"]);
    git(&fx.repo, &["reflog", "expire", "--expire=now", "--all"]);
    git(&fx.repo, &["gc", "-q", "--prune=now"]);
    let w1_present = Command::new("git")
        .current_dir(&fx.repo)
        .args(["cat-file", "-e", &format!("{}^{{commit}}", fx.w1)])
        .status()
        .unwrap()
        .success();
    assert!(
        !w1_present,
        "the branch's commits are gone from the repository"
    );

    let reconstructed = reconstruct_events(&db, load_events(&db).await).await;
    let by_id: HashMap<&str, &Event> = reconstructed.iter().map(|e| (e.id.as_str(), e)).collect();
    assert_eq!(tool_result_of(by_id["e-r1"]), stored_w1);
    assert_eq!(tool_result_of(by_id["e-r2"]), stored_c);
    let write: Value = serde_json::from_str(&by_id["a-w1"].data).unwrap();
    let combined: String = write["toolUses"][0]["input"]["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["payload"]["diff"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(combined, expected_diff.trim_start_matches('\n'));

    // No live range resolves (the branch is gone and there is no jj binary), so
    // the node diff falls back to the archived history.
    let diff = crate::diff::node_base_tip_diff(&db, "job", "", Path::new("/nonexistent"))
        .await
        .unwrap()
        .expect("the archived history renders the node diff");
    let mut paths: Vec<&str> = diff.files.iter().map(|f| f.path.as_str()).collect();
    paths.sort();
    assert_eq!(paths, ["a.txt", "c.txt"]);
    assert_eq!(diff.commits_ahead, 2);
}

/// A second archival of an execution that already has history (a reopened
/// issue torn down again) never replaces the pack the first archival's
/// coordinates rely on.
#[tokio::test]
async fn rearchival_with_history_keeps_the_first_pack() {
    let fx = build_fixture();
    let db = migrated_test_db("archival-rearchive.db").await;
    seed_chain(&db, fx.repo.to_str().unwrap(), false).await;
    let stored = rendered(&[("file:dir/b.txt", b"unchanged-keep\n")]);
    insert_event(
        &db,
        "a-r1",
        "run",
        1,
        1,
        "assistant",
        &assistant_read("r1", &["file:dir/b.txt"]),
    )
    .await;
    insert_event(
        &db,
        "e-r1",
        "run",
        2,
        2,
        "tool_result",
        &read_result("r1", &stored),
    )
    .await;
    archive(&db, Some(fx.range())).await.unwrap();
    let first_tip = db
        .query_one(
            "SELECT tip_sha FROM execution_history WHERE execution_id = 'exec'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();

    // The reopened session reads again; the branch has moved on.
    write_file(&fx.repo, "later.txt", b"later\n");
    commit_all(&fx.repo, "later");
    insert_event(
        &db,
        "a-r2",
        "run",
        3,
        3,
        "assistant",
        &assistant_read("r2", &["file:dir/b.txt"]),
    )
    .await;
    insert_event(
        &db,
        "e-r2",
        "run",
        4,
        4,
        "tool_result",
        &read_result("r2", &stored),
    )
    .await;
    let summary = archive(&db, Some(fx.range())).await.unwrap();
    assert_eq!(summary.gitcoord_read, 0, "no range is used the second time");
    assert_eq!(summary.zstd, 2);
    let tip = db
        .query_one(
            "SELECT tip_sha FROM execution_history WHERE execution_id = 'exec'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert_eq!(tip, first_tip);
}

/// An execution spans one branch per node. A planner whose branch has no
/// commits of its own and a builder that committed share the execution: both
/// branches' objects are packed, the builder's write and reads are
/// coordinates, the planner's read resolves on its own branch, and
/// `execution_history` records the builder's range — so everything survives
/// deleting both branches.
#[tokio::test]
async fn execution_spanning_two_branches_archives_each_against_its_own() {
    let fx = build_fixture();
    git(&fx.repo, &["branch", "plan", &fx.anchor]);
    let db = migrated_test_db("archival-two-branches.db").await;
    seed_chain(&db, fx.repo.to_str().unwrap(), false).await;
    db.execute_script(
        "INSERT INTO jobs(id, execution_id, project_id, status, branch, created_at, updated_at)
           VALUES ('planjob','exec','proj','complete','plan',0,0);
         INSERT INTO runs(id, job_id, project_id, status, created_at, updated_at)
           VALUES ('planrun','planjob','proj','exited',0,0);",
    )
    .await
    .unwrap();

    let stored_plan = rendered(&[("file:a.txt", b"alpha\nbeta\ngamma\ndelta\n")]);
    let stored_c = rendered(&[("file:c.txt", b"new file\n")]);
    let read_plan = assistant_read("p1", &["file:a.txt"]);
    insert_event(&db, "a-p1", "planrun", 1, 1, "assistant", &read_plan).await;
    insert_event(
        &db,
        "e-p1",
        "planrun",
        2,
        2,
        "tool_result",
        &read_result("p1", &stored_plan),
    )
    .await;
    insert_event(
        &db,
        "a-w1",
        "run",
        3,
        3,
        "assistant",
        &assistant_write("w1"),
    )
    .await;
    let w1_short = short(&fx.repo, &fx.w1);
    insert_event(
        &db,
        "e-w1",
        "run",
        4,
        4,
        "tool_result",
        &write_result("w1", &w1_short),
    )
    .await;
    let read_c = assistant_read("r1", &["file:c.txt"]);
    insert_event(&db, "a-r1", "run", 5, 5, "assistant", &read_c).await;
    insert_event(
        &db,
        "e-r1",
        "run",
        6,
        6,
        "tool_result",
        &read_result("r1", &stored_c),
    )
    .await;

    // Teardown orders ranges by job age: the planner's branch comes first.
    let summary = archive_ranges(&db, vec![branch_range(&fx.repo, "plan"), fx.range()])
        .await
        .unwrap();
    assert_eq!(summary.gitcoord_write, 1);
    assert_eq!(summary.unresolved_write, 0);
    assert_eq!(summary.gitcoord_read, 2);
    assert_eq!(
        content_commit(&db, "e-p1").await.as_deref(),
        Some(fx.anchor.as_str())
    );
    assert_eq!(
        content_commit(&db, "e-r1").await.as_deref(),
        Some(fx.w1.as_str())
    );
    let (base, tip, has_pack) = db
        .query_one(
            "SELECT base_sha, tip_sha, pack IS NOT NULL FROM execution_history WHERE execution_id = 'exec'",
            (),
            |row| Ok((row.text(0)?, row.text(1)?, row.i64(2)? == 1)),
        )
        .await
        .unwrap();
    assert_eq!(base, fx.anchor, "history records the branch with commits");
    assert_eq!(tip, fx.drift);
    assert!(has_pack);

    git(&fx.repo, &["checkout", "-q", "main"]);
    git(&fx.repo, &["branch", "-D", "-q", "work", "plan"]);
    git(&fx.repo, &["reflog", "expire", "--expire=now", "--all"]);
    git(&fx.repo, &["gc", "-q", "--prune=now"]);
    let reconstructed = reconstruct_events(&db, load_events(&db).await).await;
    let by_id: HashMap<&str, &Event> = reconstructed.iter().map(|e| (e.id.as_str(), e)).collect();
    assert_eq!(tool_result_of(by_id["e-p1"]), stored_plan);
    assert_eq!(tool_result_of(by_id["e-r1"]), stored_c);
    let diff = crate::diff::node_base_tip_diff(&db, "job", "", Path::new("/nonexistent"))
        .await
        .unwrap()
        .expect("the builder's archived range renders its node diff");
    assert_eq!(diff.commits_ahead, 2);
}
