use super::*;
use cairn_common::query::parse_query_params;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

fn query(op: VcsOp, raw: &str) -> Result<VcsQuery, String> {
    let params = if raw.is_empty() {
        Vec::new()
    } else {
        parse_query_params(raw).unwrap()
    };
    parse_query(op, &params)
}

#[test]
fn each_operation_accepts_only_its_own_keys() {
    assert!(query(VcsOp::Diff, "from=main&to=x&view=patch&glob=*.rs").is_ok());
    let error = query(VcsOp::Ancestry, "path=src").unwrap_err();
    assert!(error.contains("Unsupported query parameter 'path' for vcs/ancestry"));
    assert!(query(VcsOp::Log, "rev=main").is_err());
    assert!(query(VcsOp::Blame, "to=main&path=a").is_err());
}

#[test]
fn diff_queries_reject_contradictions_and_file_implies_patch() {
    assert_eq!(
        query(VcsOp::Diff, "file=src/lib.rs").unwrap().view,
        DiffView::Patch
    );
    assert!(query(VcsOp::Diff, "commit=abc&from=main").is_err());
    assert!(query(VcsOp::Diff, "commit=abc&base=from").is_err());
    assert!(query(VcsOp::Diff, "file=a&glob=*.rs").is_err());
    assert!(query(VcsOp::Diff, "view=summary&file=a").is_err());
    assert!(query(VcsOp::Diff, "view=commits&glob=*.rs").is_err());
    assert!(query(VcsOp::Diff, "base=sideways").is_err());
    assert_eq!(
        query(VcsOp::Diff, "base=from").unwrap().base,
        DiffBase::From
    );
    assert_eq!(query(VcsOp::Diff, "").unwrap().base, DiffBase::Fork);
}

#[test]
fn log_and_blame_validate_their_windows() {
    assert_eq!(query(VcsOp::Log, "").unwrap().limit, LOG_DEFAULT_LIMIT);
    assert!(query(VcsOp::Log, "limit=0").is_err());
    assert!(query(VcsOp::Log, &format!("limit={}", LOG_MAX_LIMIT + 1)).is_err());
    assert_eq!(query(VcsOp::Log, "limit=5&offset=10").unwrap().offset, 10);
    assert_eq!(
        query(VcsOp::Blame, "").unwrap_err(),
        "vcs/blame needs path=FILE"
    );
    assert_eq!(
        query(VcsOp::Blame, "path=a&lines=3-7").unwrap().lines,
        Some((3, 7))
    );
    assert!(query(VcsOp::Blame, "path=a&lines=7-3").is_err());
    assert!(query(VcsOp::Blame, "path=a&lines=0-3").is_err());
}

/// The cache is keyed by resolved commit ids: a repeat read is served without
/// touching the store, and a ref that moved resolves to a different id, so its
/// key differs and the answer is recomputed.
#[tokio::test]
async fn a_repeat_read_is_served_from_the_cache_and_a_moved_ref_misses() {
    let computed = AtomicUsize::new(0);
    let compute = |value: &'static str| {
        let computed = &computed;
        async move {
            computed.fetch_add(1, Ordering::SeqCst);
            Ok::<_, String>(value.to_string())
        }
    };
    let key = format!("test|{}|aaaa|bbbb", uuid::Uuid::new_v4());
    assert_eq!(cached(&key, compute("first")).await.unwrap(), "first");
    assert_eq!(cached(&key, compute("second")).await.unwrap(), "first");
    assert_eq!(computed.load(Ordering::SeqCst), 1);

    let moved = key.replace("bbbb", "cccc");
    assert_eq!(cached(&moved, compute("moved")).await.unwrap(), "moved");
    assert_eq!(computed.load(Ordering::SeqCst), 2);

    let failing = format!("test|{}|fail", uuid::Uuid::new_v4());
    assert!(
        cached::<String, _>(&failing, async { Err("boom".to_string()) })
            .await
            .is_err()
    );
    assert_eq!(
        cached(&failing, compute("retried")).await.unwrap(),
        "retried",
        "a failed answer is not cached"
    );
}

fn endpoint(rev: &str, commit: &str) -> Endpoint {
    Endpoint {
        requested: rev.to_string(),
        resolution: BranchResolution {
            project_id: "project".to_string(),
            repository_path: PathBuf::from("/store"),
            object_repository_path: PathBuf::from("/repo"),
            rev: rev.to_string(),
            commit_id: commit.to_string(),
            default_commit_id: commit.to_string(),
            default_branch: "main".to_string(),
        },
        label: format!("{rev}@{}", &commit[..8]),
    }
}

fn answer() -> DiffAnswer {
    DiffAnswer {
        base: "1111111111111111111111111111111111111111".to_string(),
        tip: "2222222222222222222222222222222222222222".to_string(),
        rows: vec![
            (
                "src/lib.rs".to_string(),
                "modified".to_string(),
                Some(2),
                Some(1),
                None,
            ),
            (
                "docs/a.md".to_string(),
                "added".to_string(),
                Some(5),
                Some(0),
                None,
            ),
        ],
        patch: "diff --git a/docs/a.md b/docs/a.md\nnew file mode 100644\n--- /dev/null\n+++ b/docs/a.md\n@@ -0,0 +1 @@\n+hello\ndiff --git a/src/lib.rs b/src/lib.rs\n--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1 +1 @@\n-old\n+new\n".to_string(),
    }
}

#[test]
fn the_diff_summary_names_both_endpoints_and_the_base_it_rendered_against() {
    let from = endpoint("main", "3333333333333333333333333333333333333333");
    let to = endpoint("agent/x", "2222222222222222222222222222222222222222");
    let header = DiffHeader {
        project: "cairn",
        from: Some(&from),
        to: &to,
        base: Some("merge-base"),
        commit_form: false,
    };
    let query = query(VcsOp::Diff, "").unwrap();
    let rendered = render_diff(&header, Some(answer()), None, &query, None);
    assert!(rendered.contains("- From: `main@33333333`"), "{rendered}");
    assert!(rendered.contains("- To: `agent/x@22222222`"));
    assert!(rendered.contains("- Against: merge-base `111111111111`"));
    assert!(rendered.contains("**2 files, +7 -1**"));

    let glob = GlobBuilder::new("src/**")
        .literal_separator(false)
        .build()
        .unwrap()
        .compile_matcher();
    let patch_query = parse_query(
        VcsOp::Diff,
        &parse_query_params("view=patch&glob=src/**").unwrap(),
    )
    .unwrap();
    let patch = render_diff(&header, Some(answer()), None, &patch_query, Some(&glob));
    assert!(patch.contains("+new"));
    assert!(!patch.contains("+hello"), "{patch}");

    let json_query = query_json();
    let json: serde_json::Value = serde_json::from_str(&render_diff(
        &header,
        Some(answer()),
        None,
        &json_query,
        None,
    ))
    .unwrap();
    assert_eq!(json["base"], "1111111111111111111111111111111111111111");
    assert_eq!(json["to"]["rev"], "agent/x");
    assert_eq!(json["files"].as_array().unwrap().len(), 2);
}

fn query_json() -> VcsQuery {
    parse_query(VcsOp::Diff, &parse_query_params("format=json").unwrap()).unwrap()
}

/// The production path of this family may not take the store lock, spawn a
/// subprocess, or drive the jj CLI. The one inherited exception, a conflicted
/// name repair, lives in the shared resolver (`branch.rs`), not here.
#[test]
fn production_vcs_path_has_no_lock_or_subprocess_edge() {
    let sources = [
        include_str!("../vcs.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap(),
        include_str!("../../../../cairn-vcs/src/history.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap(),
    ]
    .join("\n");
    for forbidden in [
        "acquire_store_lock(",
        "acquire_jj_store_lock",
        "Command::new(",
        "std::process::Command",
        "JjEnv",
        "materialize_",
    ] {
        assert!(
            !sources.contains(forbidden),
            "store-served vcs path acquired forbidden edge {forbidden}"
        );
    }
    assert!(sources.contains("resolve_for_project_read"));
    assert!(sources.contains("ObjectStore::new"));
    assert!(sources.contains("load_repo_at_head"));
}
