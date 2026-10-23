use super::*;
use jj_lib::git::{self, GitImportOptions};
use jj_lib::workspace::Workspace;
use std::process::Command;

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_AUTHOR_DATE", "2026-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2026-01-01T00:00:00Z")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn commit_all(dir: &Path, message: &str) -> String {
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-qm", message]);
    git(dir, &["rev-parse", "HEAD"])
}

fn lines(range: std::ops::Range<usize>, tag: &str) -> String {
    range.map(|n| format!("{tag} {n}\n")).collect()
}

/// The graph every test reads:
///
/// ```text
/// c0 ─ c1 ─ c2 ─ merge     (main)
///   ╲ f1 ─ f2 ╱            (feature)
/// ```
///
/// `c0` adds `a.txt` (ten lines) and `b.txt`; `c1` edits `b.txt`; `c2`
/// rewrites the first five lines of `a.txt`; `f1` and `f2` add and edit
/// `dir/c.txt`.
struct Fixture {
    dir: tempfile::TempDir,
    c0: String,
    c1: String,
    c2: String,
    f1: String,
    f2: String,
    merge: String,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    git(path, &["init", "-q", "-b", "main"]);
    git(path, &["config", "user.email", "test@cairn.local"]);
    git(path, &["config", "user.name", "Cairn Test"]);
    std::fs::write(path.join("a.txt"), lines(1..11, "original")).unwrap();
    std::fs::write(path.join("b.txt"), "b\n").unwrap();
    let c0 = commit_all(path, "c0: seed");
    std::fs::write(path.join("b.txt"), "b edited\n").unwrap();
    let c1 = commit_all(path, "c1: edit b");
    let rewritten = lines(1..6, "rewritten") + &lines(6..11, "original");
    std::fs::write(path.join("a.txt"), rewritten).unwrap();
    let c2 = commit_all(path, "c2: rewrite half of a\n\nbody line");
    git(path, &["checkout", "-q", "-b", "feature", &c0]);
    std::fs::create_dir_all(path.join("dir")).unwrap();
    std::fs::write(path.join("dir/c.txt"), "c\n").unwrap();
    let f1 = commit_all(path, "f1: add c");
    std::fs::write(path.join("dir/c.txt"), "c edited\n").unwrap();
    let f2 = commit_all(path, "f2: edit c");
    git(path, &["checkout", "-q", "main"]);
    git(
        path,
        &["merge", "-q", "--no-ff", "-m", "merge feature", "feature"],
    );
    let merge = git(path, &["rev-parse", "HEAD"]);
    init_store(path).await;
    Fixture {
        dir,
        c0,
        c1,
        c2,
        f1,
        f2,
        merge,
    }
}

async fn init_store(path: &Path) {
    let settings = crate::jj_settings(None).unwrap();
    let (_, repo) = Workspace::init_external_git(&settings, path, &path.join(".git"))
        .await
        .unwrap();
    let mut tx = repo.start_transaction();
    git::import_refs(
        tx.repo_mut(),
        &GitImportOptions {
            abandon_unreachable_commits: false,
            record_synthetic_predecessors: false,
            remote_auto_track_bookmarks: HashMap::new(),
        },
    )
    .await
    .unwrap();
    tx.commit("import fixture").await.unwrap();
}

const LIMIT: Duration = Duration::from_secs(30);

fn ids(page: &LogPage) -> Vec<&str> {
    page.entries.iter().map(|e| e.commit_id.as_str()).collect()
}

#[tokio::test]
async fn ancestry_reports_fork_point_containment_and_both_counts() {
    let f = fixture().await;
    let diverged = ancestry(f.dir.path(), &f.c2, &f.f2, 10_000, LIMIT)
        .await
        .unwrap();
    assert_eq!(diverged.merge_base.as_deref(), Some(f.c0.as_str()));
    assert!(!diverged.from_in_to && !diverged.to_in_from);
    assert_eq!(diverged.ahead.to_string(), "2", "f1 and f2");
    assert_eq!(diverged.behind.to_string(), "2", "c1 and c2");

    let merged = ancestry(f.dir.path(), &f.f2, &f.merge, 10_000, LIMIT)
        .await
        .unwrap();
    assert_eq!(merged.merge_base.as_deref(), Some(f.f2.as_str()));
    assert!(merged.from_in_to, "the merge contains the feature tip");
    assert!(!merged.to_in_from);
    assert_eq!(merged.ahead.count, 3, "c1, c2 and the merge");
    assert_eq!(merged.behind.count, 0);

    let capped = ancestry(f.dir.path(), &f.f2, &f.merge, 2, LIMIT)
        .await
        .unwrap();
    assert_eq!(capped.ahead.to_string(), "≥2");
    assert!(!capped.behind.capped);
}

#[tokio::test]
async fn an_unknown_commit_is_an_error_not_a_panic() {
    let f = fixture().await;
    let unknown = "0123456789abcdef0123456789abcdef01234567";
    assert_eq!(
        ancestry(f.dir.path(), unknown, &f.merge, 10, LIMIT).await,
        Err(HistoryError::UnknownCommit(unknown.to_string()))
    );
    assert_eq!(
        ancestry(f.dir.path(), "not-hex", &f.merge, 10, LIMIT).await,
        Err(HistoryError::UnknownCommit("not-hex".to_string()))
    );
}

#[tokio::test]
async fn log_walks_the_full_dag_newest_first_and_pages() {
    let f = fixture().await;
    let query = |from: Option<&str>, path: Option<&str>, limit, offset| LogQuery {
        to: f.merge.clone(),
        from: from.map(str::to_string),
        path: path.map(str::to_string),
        limit,
        offset,
    };
    let all = log(f.dir.path(), query(None, None, 30, 0), LIMIT)
        .await
        .unwrap();
    assert_eq!(all.entries.len(), 6, "every commit except jj's root");
    assert_eq!(all.entries[0].commit_id, f.merge);
    assert_eq!(all.entries[0].parents, vec![f.c2.clone(), f.f2.clone()]);
    assert_eq!(all.entries.last().unwrap().commit_id, f.c0);
    assert!(!all.more);

    let page = log(f.dir.path(), query(None, None, 2, 1), LIMIT)
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 2);
    assert_eq!(page.entries[0].commit_id, all.entries[1].commit_id);
    assert!(page.more);

    let range = log(f.dir.path(), query(Some(&f.c2), None, 30, 0), LIMIT)
        .await
        .unwrap();
    let mut range_ids = ids(&range);
    range_ids.sort();
    let mut expected = vec![f.merge.as_str(), f.f1.as_str(), f.f2.as_str()];
    expected.sort();
    assert_eq!(
        range_ids, expected,
        "c2..merge is the merge and the side branch"
    );
    let subject = all
        .entries
        .iter()
        .find(|entry| entry.commit_id == f.c2)
        .unwrap();
    assert_eq!(subject.subject(), "c2: rewrite half of a");
    assert_eq!(subject.author_name, "Cairn Test");
}

#[tokio::test]
async fn a_path_filter_skips_commits_that_do_not_touch_the_path() {
    let f = fixture().await;
    let query = |path: &str, from: Option<&str>| LogQuery {
        to: f.merge.clone(),
        from: from.map(str::to_string),
        path: Some(path.to_string()),
        limit: 30,
        offset: 0,
    };
    let a = log(f.dir.path(), query("a.txt", None), LIMIT)
        .await
        .unwrap();
    assert_eq!(ids(&a), vec![f.c2.as_str(), f.c0.as_str()]);
    let bounded = log(f.dir.path(), query("a.txt", Some(&f.c0)), LIMIT)
        .await
        .unwrap();
    assert_eq!(ids(&bounded), vec![f.c2.as_str()]);
    let directory = log(f.dir.path(), query("dir/", None), LIMIT).await.unwrap();
    assert_eq!(ids(&directory), vec![f.f2.as_str(), f.f1.as_str()]);
    let b = log(f.dir.path(), query("b.txt", None), LIMIT)
        .await
        .unwrap();
    assert_eq!(ids(&b), vec![f.c1.as_str(), f.c0.as_str()]);
}

#[tokio::test]
async fn annotate_attributes_a_half_rewritten_file_line_by_line() {
    let f = fixture().await;
    let blame = annotate(f.dir.path(), &f.merge, "a.txt", 1 << 20, LIMIT)
        .await
        .unwrap();
    assert_eq!(blame.lines.len(), 10);
    for (index, line) in blame.lines.iter().enumerate() {
        let number = index + 1;
        if number <= 5 {
            assert_eq!(line.commit_id, f.c2, "line {number}");
            assert_eq!(line.text, format!("rewritten {number}"));
        } else {
            assert_eq!(line.commit_id, f.c0, "line {number}");
            assert_eq!(line.text, format!("original {number}"));
        }
        assert_eq!(line.original_line, number);
    }
    assert_eq!(blame.commits.len(), 2);
    assert_eq!(blame.commits[&f.c2].subject(), "c2: rewrite half of a");
}

#[tokio::test]
async fn annotate_refuses_what_it_should_not_attribute() {
    let f = fixture().await;
    assert!(matches!(
        annotate(f.dir.path(), &f.merge, "missing.txt", 1 << 20, LIMIT).await,
        Err(HistoryError::PathAbsent { .. })
    ));
    assert!(matches!(
        annotate(f.dir.path(), &f.merge, "dir", 1 << 20, LIMIT).await,
        Err(HistoryError::Refused(_))
    ));
    assert!(matches!(
        annotate(f.dir.path(), &f.merge, "a.txt", 8, LIMIT).await,
        Err(HistoryError::Refused(_))
    ));
}

#[tokio::test]
async fn a_walk_past_its_deadline_times_out_instead_of_answering_partially() {
    let f = fixture().await;
    assert_eq!(
        annotate(f.dir.path(), &f.merge, "a.txt", 1 << 20, Duration::ZERO).await,
        Err(HistoryError::TimedOut {
            limit: Duration::ZERO
        })
    );
    let query = LogQuery {
        to: f.merge.clone(),
        from: None,
        path: None,
        limit: 30,
        offset: 0,
    };
    assert!(matches!(
        log(f.dir.path(), query, Duration::ZERO).await,
        Err(HistoryError::TimedOut { .. })
    ));
}

/// A path that never matches walks the whole range without yielding a single
/// entry, so the deadline must be consulted per commit walked, not per commit
/// returned; otherwise a typo'd path is an unbounded walk.
#[tokio::test]
async fn a_path_that_never_matches_is_still_bounded_by_the_deadline() {
    let f = fixture().await;
    let query = |offset| LogQuery {
        to: f.merge.clone(),
        from: None,
        path: Some("no/such/path".to_string()),
        limit: 30,
        offset,
    };
    assert_eq!(
        log(f.dir.path(), query(0), LIMIT).await.unwrap().entries,
        Vec::new()
    );
    assert!(matches!(
        log(f.dir.path(), query(0), Duration::ZERO).await,
        Err(HistoryError::TimedOut { .. })
    ));
    assert!(matches!(
        log(f.dir.path(), query(5), Duration::ZERO).await,
        Err(HistoryError::TimedOut { .. })
    ));
}
