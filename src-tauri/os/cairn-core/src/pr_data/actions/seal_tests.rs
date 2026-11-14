//! A resolved pull request's create-pr record is sealed: the one gate refuses
//! writes to it with the route a follow-up takes, reads GitHub live before
//! writing to one the record calls open, and a late discovery restores what
//! resolved while keeping the post-resolution draft.

use super::create_pr::{prose_edit_seam, DeferredPrSync};
use super::resolution::POST_MERGE_DRAFT_CHAIN;
use crate::db::DbState;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::services::testing::{MockGitClient, MockHttpClient, TestServicesBuilder};
use crate::services::{HttpClient, HttpMethod, HttpResponse};
use crate::storage::{LocalDb, RowExt, SearchIndex};
use cairn_db::turso::params;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

const TEST_KEY: &str = include_str!("../../../tests/fixtures/test_rsa_key.pem");
const PR: i32 = 43;
const NODE: &str = "cairn://p/seal/1/1/builder";
const SNAPSHOT: &str = r#"{"recipe":{"id":"recipe-1","name":"R","description":null,"trigger":"manual","nodes":[],"edges":[]},"agents":{},"skills":{},"triggerContext":{"issueId":"issue-seal","projectId":"proj-seal","triggerType":"manual"},"createdAt":1}"#;

// ── A GitHub the test can still read after handing it over ──

type Fut<'a> = Pin<Box<dyn Future<Output = Result<HttpResponse, String>> + Send + 'a>>;

#[derive(Clone)]
struct GitHub(Arc<MockHttpClient>);

impl HttpClient for GitHub {
    fn get(&self, url: &str, headers: reqwest::header::HeaderMap) -> Fut<'_> {
        self.0.get(url, headers)
    }
    fn post(&self, url: &str, body: serde_json::Value, h: reqwest::header::HeaderMap) -> Fut<'_> {
        self.0.post(url, body, h)
    }
    fn put(&self, url: &str, body: serde_json::Value, h: reqwest::header::HeaderMap) -> Fut<'_> {
        self.0.put(url, body, h)
    }
    fn patch(&self, url: &str, body: serde_json::Value, h: reqwest::header::HeaderMap) -> Fut<'_> {
        self.0.patch(url, body, h)
    }
    fn delete(&self, url: &str, headers: reqwest::header::HeaderMap) -> Fut<'_> {
        self.0.delete(url, headers)
    }
}

impl GitHub {
    /// GETs of the pull request itself, not its reviews, files or listing.
    fn pull_fetches(&self) -> usize {
        let suffix = format!("/pulls/{PR}");
        self.0
            .requests()
            .iter()
            .filter(|r| r.method == HttpMethod::Get && r.url.ends_with(&suffix))
            .count()
    }

    fn listings(&self) -> usize {
        self.0
            .requests()
            .iter()
            .filter(|r| r.url.contains("/pulls?state=closed"))
            .count()
    }

    fn request_count(&self) -> usize {
        self.0.requests().len()
    }
}

fn json(status: u16, value: serde_json::Value) -> HttpResponse {
    HttpResponse::new(status, serde_json::to_vec(&value).unwrap())
}

/// The pull request as GitHub reports it.
struct Pull {
    merged_at: Option<&'static str>,
    title: &'static str,
    body: &'static str,
}

impl Pull {
    fn open() -> Self {
        Self {
            merged_at: None,
            title: "Merged title",
            body: "Merged body",
        }
    }

    fn merged_at(at: &'static str) -> Self {
        Self {
            merged_at: Some(at),
            ..Self::open()
        }
    }

    fn saying(self, title: &'static str, body: &'static str) -> Self {
        Self {
            title,
            body,
            ..self
        }
    }

    fn response(&self) -> HttpResponse {
        let merged = self.merged_at.is_some();
        json(
            200,
            serde_json::json!({
                "title": self.title, "body": self.body,
                "state": if merged { "closed" } else { "open" },
                "draft": false, "mergeable": true, "mergeable_state": "clean",
                "additions": 1, "deletions": 0, "merged": merged,
                "merge_commit_sha": merged.then_some("m".repeat(40)),
                "merged_at": self.merged_at, "closed_at": self.merged_at,
                "head": {"sha": "h".repeat(40)}
            }),
        )
    }
}

/// GitHub answering for PR #43 with `pulls` in order (the last repeats), and
/// listing `listed` as recently closed into `main`.
fn github(pulls: Vec<Pull>, listed: &[i32]) -> GitHub {
    let http = MockHttpClient::new()
        .respond_to(
            "access_tokens",
            json(
                201,
                serde_json::json!({"token": "ghs_test", "expires_at": "2099-01-01T00:00:00Z"}),
            ),
        )
        .respond_to("/pulls/43/reviews", json(200, serde_json::json!([])))
        .respond_to("/pulls/43/files", json(200, serde_json::json!([])))
        .respond_to(
            "/check-runs",
            json(200, serde_json::json!({"check_runs": []})),
        )
        .respond_to(
            "/pulls?state=closed",
            json(
                200,
                serde_json::Value::Array(
                    listed
                        .iter()
                        .map(|number| serde_json::json!({"number": number}))
                        .collect(),
                ),
            ),
        )
        .respond_to_sequence(
            "/pulls/43",
            pulls.iter().map(Pull::response).collect::<Vec<_>>(),
        );
    GitHub(Arc::new(http))
}

// ── One builder node whose create-pr artifact presents PR #43 ──

struct Fixture {
    db: Arc<LocalDb>,
    repo: tempfile::TempDir,
    cfg: tempfile::TempDir,
}

async fn fixture(name: &str) -> Fixture {
    let repo = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::storage::migrated_test_db(name).await);
    db.execute(
        "INSERT INTO github_app (id, app_id, private_key, installation_id) VALUES ('default', 123, ?1, 999)",
        params![TEST_KEY],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
         VALUES ('proj-seal', 'default', 'P', 'seal', ?1, 'main', 1, 1)",
        params![repo.path().to_str().unwrap()],
    )
    .await
    .unwrap();
    db.execute_script(format!(
        "INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at)
           VALUES ('issue-seal', 'proj-seal', 1, 'Issue', 'active', 'idle', 'none', 1, 1);
         INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
           VALUES ('exec-seal', 'recipe-1', 'issue-seal', 'proj-seal', 'running', 1, 1, '{SNAPSHOT}');
         INSERT INTO jobs (id, project_id, issue_id, execution_id, recipe_node_id, node_name, uri_segment,
                           status, branch, created_at, updated_at)
           VALUES ('job-seal', 'proj-seal', 'issue-seal', 'exec-seal', 'builder', 'builder', 'builder',
                   'complete', 'feature', 1, 1);
         INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, body, source_branch, target_branch,
                                     status, merge_method, opened_at, updated_at, is_local, github_pr_number, github_pr_url)
           VALUES ('mr-seal', 'job-seal', 'proj-seal', 'issue-seal', 'Merged title', 'Merged body', 'feature', 'main',
                   'open', 'squash', 1, 1, 0, {PR}, 'https://github.com/octo/widget/pull/{PR}');"
    ))
    .await
    .unwrap();
    Fixture {
        db,
        repo,
        cfg: tempfile::tempdir().unwrap(),
    }
}

impl Fixture {
    /// A fresh orchestrator (and so a fresh snapshot cache) talking to `github`.
    fn orch(&self, github: &GitHub) -> Orchestrator {
        let mut git = MockGitClient::new();
        git.expect_status()
            .times(0..)
            .returning(|_| Ok(String::new()));
        git.expect_run().times(0..).returning(|_, _| {
            Ok(crate::services::GitOutput {
                success: true,
                stdout: String::new(),
                stderr: String::new(),
            })
        });
        let root = self.cfg.path().join(uuid::Uuid::new_v4().to_string());
        let state = Arc::new(DbState::new(
            self.db.clone(),
            Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
        ));
        let services = TestServicesBuilder::new()
            .with_http(github.clone())
            .with_git(git)
            .build();
        Orchestrator::builder(state, Arc::new(services), root.join("config")).build()
    }

    async fn version(&self, version: i64, title: &str, body: &str, created_at: i64) {
        let data = serde_json::json!({"title": title, "body": body}).to_string();
        self.db
            .execute(
                "INSERT INTO artifacts (id, job_id, artifact_type, schema_version, data, version,
                                        parent_version_id, output_name, confirmed, created_at, updated_at)
                 VALUES (?1, 'job-seal', 'create-pr', 1, ?2, ?3, NULL, 'create-pr', 1, ?4, ?4)",
                params![format!("artifact-v{version}"), data, version, created_at],
            )
            .await
            .unwrap();
    }

    /// `(version, title, body)` of every version in `chain`, oldest first.
    async fn chain(&self, chain: &str) -> Vec<(i64, String, String)> {
        self.db
            .query_all(
                "SELECT version, data FROM artifacts WHERE job_id = 'job-seal' AND output_name = ?1
                  ORDER BY version",
                params![chain.to_string()],
                |row| Ok((row.i64(0)?, row.text(1)?)),
            )
            .await
            .unwrap()
            .into_iter()
            .map(|(version, data)| {
                let data: serde_json::Value = serde_json::from_str(&data).unwrap();
                (
                    version,
                    data["title"].as_str().unwrap_or_default().to_string(),
                    data["body"].as_str().unwrap_or_default().to_string(),
                )
            })
            .collect()
    }

    async fn row(&self, column: &str) -> Option<String> {
        self.db
            .query_opt_text(
                format!("SELECT CAST({column} AS TEXT) FROM merge_requests WHERE id = 'mr-seal'"),
                (),
            )
            .await
            .unwrap()
    }

    async fn resolve_row(&self, status: &str) {
        self.db
            .execute(
                "UPDATE merge_requests SET status = ?1 WHERE id = 'mr-seal'",
                params![status.to_string()],
            )
            .await
            .unwrap();
    }
}

fn write_request(mode: &str, payload: serde_json::Value) -> McpCallbackRequest {
    write_request_to("create-pr", mode, payload)
}

fn write_request_to(name: &str, mode: &str, payload: serde_json::Value) -> McpCallbackRequest {
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: std::env::temp_dir().to_string_lossy().to_string(),
        run_id: None,
        tool: "write".to_string(),
        payload: serde_json::json!({
            "changes": [{
                "target": format!("{NODE}/{name}"),
                "mode": mode,
                "payload": payload
            }]
        }),
        tool_use_id: None,
        origin: None,
    }
}

async fn write_create_pr(orch: &Orchestrator, mode: &str, payload: serde_json::Value) -> String {
    crate::mcp::handlers::write::handle_write(orch, &write_request(mode, payload)).await
}

fn assert_route(report: &str, outcome: &str, consequence: &str) {
    for needed in [
        outcome,
        "cairn://p/seal/1/executions",
        "continuity",
        NODE,
        consequence,
    ] {
        assert!(report.contains(needed), "missing {needed:?}: {report}");
    }
}

/// A recorded merge or close seals the record. Neither a create nor a body
/// patch lands, GitHub is not asked anything, and nothing reaches the pull
/// request; the refusal names the route a follow-up takes.
#[tokio::test(flavor = "current_thread")]
async fn a_patch_of_a_merged_prs_create_pr_body_is_refused_with_the_route() {
    for (status, mode, outcome) in [
        ("merged", "patch", "PR #43 merged"),
        ("merged", "create", "PR #43 merged"),
        ("closed", "patch", "PR #43 closed without merging"),
    ] {
        let fixture = fixture(&format!("seal-refused-{status}-{mode}.db")).await;
        fixture.version(1, "Merged title", "Merged body", 1).await;
        fixture.resolve_row(status).await;
        let github = github(vec![Pull::open()], &[]);
        let orch = fixture.orch(&github);
        let edits = prose_edit_seam::install(fixture.repo.path(), |_| Ok(()));

        let report = write_create_pr(
            &orch,
            mode,
            serde_json::json!({"title": "Second PR", "body": "A different change"}),
        )
        .await;

        assert_route(&report, outcome, "Nothing was written.");
        assert_eq!(
            fixture.chain("create-pr").await.len(),
            1,
            "{status}/{mode}: no version is added: {report}"
        );
        assert_eq!(
            github.request_count(),
            0,
            "{status}/{mode}: a recorded seal costs no GitHub call"
        );
        assert!(edits.probes().is_empty(), "{status}/{mode}: nothing edited");
    }
}

/// The record says open and a cached snapshot agrees, but GitHub says merged.
/// The write reads GitHub anyway, records the merge, and refuses.
#[tokio::test(flavor = "current_thread")]
async fn an_edit_of_a_stale_open_pr_refreshes_first_and_refuses_when_github_says_merged() {
    let fixture = fixture("seal-stale-open.db").await;
    fixture.version(1, "Merged title", "Merged body", 1).await;
    let github = github(
        vec![Pull::open(), Pull::merged_at("2020-01-01T00:00:00Z")],
        &[],
    );
    let orch = fixture.orch(&github);
    let edits = prose_edit_seam::install(fixture.repo.path(), |_| Ok(()));
    let cached = super::refresh::pr_snapshot_for_job(&orch, "job-seal", None)
        .await
        .unwrap();
    assert_eq!(cached.cache.state, crate::models::PrState::Open);
    assert_eq!(github.pull_fetches(), 1);

    let report = write_create_pr(&orch, "patch", serde_json::json!({"body": "Late edit"})).await;

    assert_route(&report, "PR #43 merged", "Nothing was written.");
    assert_eq!(
        github.pull_fetches(),
        2,
        "the gate read GitHub although a fresh open snapshot was cached"
    );
    assert_eq!(fixture.chain("create-pr").await.len(), 1, "{report}");
    assert_eq!(fixture.row("status").await.as_deref(), Some("merged"));
    assert!(edits.probes().is_empty());
}

/// The builder's terminal contract is the `pr` node's `create-pr`, so the
/// generic `cairn:~/artifact` alias resolves to the create-pr record. That
/// address skips the dispatch's own live read, and the write must read GitHub
/// itself before it stores a version and syncs prose into the pull request.
#[tokio::test(flavor = "current_thread")]
async fn a_create_pr_written_through_the_artifact_alias_reads_github_and_refuses_when_merged() {
    const PR_TERMINAL: &str = r#"{"recipe":{"id":"recipe-1","name":"R","description":null,"trigger":"manual","nodes":[{"id":"builder","nodeType":"agent","name":"builder","position":{"x":0,"y":0},"agentConfig":{"agentConfigId":"builder"}},{"id":"pr","nodeType":"pr","name":"pr","position":{"x":200,"y":0},"actionConfig":{"inputSchema":{"name":"create-pr","schema":{"type":"object","required":["title","body"],"properties":{"title":{"type":"string"},"body":{"type":"string"}},"additionalProperties":false}}}}],"edges":[{"id":"context","edgeType":"context","sourceNodeId":"builder","sourceHandle":"context-out","targetNodeId":"pr","targetHandle":"context-in"}]},"agents":{},"skills":{},"triggerContext":{"issueId":"issue-seal","projectId":"proj-seal","triggerType":"manual"},"createdAt":1}"#;
    let fixture = fixture("seal-alias.db").await;
    fixture
        .db
        .execute(
            "UPDATE executions SET snapshot = ?1 WHERE id = 'exec-seal'",
            params![PR_TERMINAL],
        )
        .await
        .unwrap();
    fixture.version(1, "Merged title", "Merged body", 1).await;
    let github = github(
        vec![Pull::open(), Pull::merged_at("2020-01-01T00:00:00Z")],
        &[],
    );
    let orch = fixture.orch(&github);
    let edits = prose_edit_seam::install(fixture.repo.path(), |_| Ok(()));
    super::refresh::pr_snapshot_for_job(&orch, "job-seal", None)
        .await
        .unwrap();

    let report = crate::mcp::handlers::write::handle_write(
        &orch,
        &write_request_to(
            "artifact",
            "patch",
            serde_json::json!({"body": "Late edit"}),
        ),
    )
    .await;

    assert_route(&report, "PR #43 merged", "Nothing was written.");
    assert_eq!(github.pull_fetches(), 2, "the write read GitHub: {report}");
    assert_eq!(fixture.chain("create-pr").await.len(), 1, "{report}");
    assert!(edits.probes().is_empty(), "{report}");
}

/// The gate fails closed when it cannot read GitHub, and says which cause
/// stopped it. With no installation to read with, nothing is asked of GitHub,
/// and the remedy is installing the app, not retrying.
#[tokio::test(flavor = "current_thread")]
async fn an_open_pr_with_no_installation_is_refused_naming_the_installation() {
    let fixture = fixture("seal-no-installation.db").await;
    fixture.version(1, "Merged title", "Merged body", 1).await;
    fixture
        .db
        .execute("DELETE FROM github_app", ())
        .await
        .unwrap();
    let github = github(vec![Pull::open()], &[]);
    let orch = fixture.orch(&github);
    let edits = prose_edit_seam::install(fixture.repo.path(), |_| Ok(()));

    let report = write_create_pr(&orch, "patch", serde_json::json!({"body": "An edit"})).await;

    for needed in [
        "Could not confirm PR #43 is still open: no GitHub App installation for octo",
        "Nothing was written.",
        "Remedy: install Cairn's GitHub App on octo",
    ] {
        assert!(report.contains(needed), "missing {needed:?}: {report}");
    }
    assert!(!report.contains("did not answer"), "{report}");
    assert_eq!(github.request_count(), 0, "nothing to read with: {report}");
    assert_eq!(fixture.chain("create-pr").await.len(), 1, "{report}");
    assert!(edits.probes().is_empty());
}

/// The other cause: the read was attempted and GitHub did not answer it, so the
/// remedy is a retry.
#[tokio::test(flavor = "current_thread")]
async fn an_open_pr_github_does_not_answer_for_is_refused_naming_the_read() {
    let fixture = fixture("seal-github-down.db").await;
    fixture.version(1, "Merged title", "Merged body", 1).await;
    let down = GitHub(Arc::new(
        MockHttpClient::new()
            .respond_to(
                "access_tokens",
                json(
                    201,
                    serde_json::json!({"token": "ghs_test", "expires_at": "2099-01-01T00:00:00Z"}),
                ),
            )
            .respond_to(
                "/pulls/43",
                json(502, serde_json::json!({"message": "Bad Gateway"})),
            ),
    ));
    let orch = fixture.orch(&down);
    let edits = prose_edit_seam::install(fixture.repo.path(), |_| Ok(()));

    let report = write_create_pr(&orch, "patch", serde_json::json!({"body": "An edit"})).await;

    for needed in [
        "Could not confirm PR #43 is still open: GitHub did not answer the read",
        "Nothing was written.",
        "Remedy: retry once GitHub answers.",
    ] {
        assert!(report.contains(needed), "missing {needed:?}: {report}");
    }
    assert!(!report.contains("installation for"), "{report}");
    assert_eq!(down.pull_fetches(), 1, "the read was attempted: {report}");
    assert_eq!(fixture.chain("create-pr").await.len(), 1, "{report}");
    assert!(edits.probes().is_empty());
}

/// The companion: GitHub confirms the pull request is open, so the edit lands,
/// and GitHub was still read first.
#[tokio::test(flavor = "current_thread")]
async fn an_edit_of_an_open_pr_reads_github_and_lands() {
    let fixture = fixture("seal-live-open.db").await;
    fixture.version(1, "Merged title", "Merged body", 1).await;
    let github = github(vec![Pull::open()], &[]);
    let orch = fixture.orch(&github);
    let _edits = prose_edit_seam::install(fixture.repo.path(), |_| Ok(()));
    super::refresh::pr_snapshot_for_job(&orch, "job-seal", None)
        .await
        .unwrap();

    // The sync after the store publishes the branch, which this fixture has no
    // store for, so the write reports the sync's failure; the gate is what is
    // under test, and it let the version through.
    let report = write_create_pr(&orch, "patch", serde_json::json!({"body": "An edit"})).await;

    assert_eq!(github.pull_fetches(), 2, "the gate read GitHub: {report}");
    let chain = fixture.chain("create-pr").await;
    assert_eq!(chain.len(), 2, "{report}");
    assert_eq!(chain[1].2, "An edit");
    assert_eq!(fixture.row("status").await.as_deref(), Some("open"));
}

/// A version stored under a held store lock is checked against the record
/// only, with no network call. Its deferred sync reads GitHub, finds the merge,
/// and edits nothing; the late discovery then restores the record.
#[tokio::test(flavor = "current_thread")]
async fn a_deferred_sync_refuses_when_the_pr_merged_meanwhile() {
    let fixture = fixture("seal-deferred.db").await;
    fixture.version(1, "Merged title", "Merged body", 1).await;
    let github = github(
        vec![Pull::merged_at("2020-01-01T00:00:00Z").saying("Someone's title", "Theirs")],
        &[],
    );
    let orch = fixture.orch(&github);
    let edits = prose_edit_seam::install(fixture.repo.path(), |_| Ok(()));

    let (_, deferral) = crate::mcp::handlers::comments_artifacts::write_artifact_change(
        &orch,
        &write_request("patch", serde_json::json!({})),
        "seal",
        1,
        1,
        "builder",
        None,
        Some("create-pr"),
        &serde_json::json!({"body": "Written under the lock"}),
        true,
        true,
        false,
    )
    .await
    .expect("the record says open, so the version stores under the lock");
    let deferral: DeferredPrSync = deferral.expect("its sync is deferred");
    assert_eq!(github.request_count(), 0, "no network call under the lock");

    let error = super::complete_deferred_pr_sync(&orch, &deferral, None)
        .await
        .expect_err("a merged pull request is not edited");

    assert_route(&error, "PR #43 merged", "The pull request was not edited.");
    assert!(edits.probes().is_empty(), "{error}");
    assert_eq!(fixture.row("status").await.as_deref(), Some("merged"));
    let chain = fixture.chain("create-pr").await;
    assert_eq!(
        (chain.len(), chain[2].2.as_str()),
        (3, "Merged body"),
        "the merged record is restored over the deferred version"
    );
    let drafts = fixture.chain(POST_MERGE_DRAFT_CHAIN).await;
    assert_eq!(drafts.len(), 1);
    assert_eq!(drafts[0].2, "Written under the lock");
}

/// GitHub merged at t1. The record was edited at t2 > t1, before anyone saw
/// the merge, and Cairn pushed that edit to GitHub. The discovery restores the
/// record as of t1, keeps the t2 version as a labelled draft, and puts the
/// merged prose back on GitHub. Someone else's prose on GitHub is left alone,
/// and a record with nothing after t1 is left as it is.
#[tokio::test(flavor = "current_thread")]
async fn a_late_merge_discovery_restores_the_merged_version_and_keeps_the_post_merge_draft() {
    // t0 = 100, t1 = 200, t2 = 300.
    const T1: &str = "1970-01-01T00:03:20Z";
    for (case, with_draft, github_prose) in [
        ("cairn-pushed", true, ("Second PR", "A different change")),
        ("someone-else", true, ("A teammate's title", "Their body")),
        ("no-draft", false, ("Merged title", "Merged body")),
    ] {
        let fixture = fixture(&format!("seal-restore-{case}.db")).await;
        fixture.version(1, "Merged title", "Merged body", 100).await;
        if with_draft {
            fixture
                .version(2, "Second PR", "A different change", 300)
                .await;
        }
        let github = github(
            vec![Pull::merged_at(T1).saying(github_prose.0, github_prose.1)],
            &[],
        );
        let orch = fixture.orch(&github);
        let edits = prose_edit_seam::install(fixture.repo.path(), |_| Ok(()));

        super::refresh::pr_snapshot_for_job(&orch, "job-seal", None)
            .await
            .unwrap();

        assert_eq!(fixture.row("status").await.as_deref(), Some("merged"));
        assert_eq!(
            fixture.row("merged_at").await.as_deref(),
            Some("200"),
            "{case}: merged_at is GitHub's merge time"
        );
        let chain = fixture.chain("create-pr").await;
        let drafts = fixture.chain(POST_MERGE_DRAFT_CHAIN).await;
        let probes = edits.probes();
        match case {
            "no-draft" => {
                assert_eq!(chain.len(), 1, "{case}: nothing to restore");
                assert!(drafts.is_empty(), "{case}");
                assert!(probes.is_empty(), "{case}");
            }
            _ => {
                assert_eq!(
                    chain.last().unwrap(),
                    &(3, "Merged title".to_string(), "Merged body".to_string()),
                    "{case}: v1 is restored as v3"
                );
                assert_eq!(
                    drafts,
                    vec![(1, "Second PR".to_string(), "A different change".to_string())],
                    "{case}: the post-merge version is kept as a labelled draft"
                );
                if case == "cairn-pushed" {
                    assert_eq!(probes.len(), 1, "{case}: {probes:?}");
                    assert_eq!(probes[0].pr_number, PR);
                    assert_eq!(probes[0].title, "Merged title");
                    assert_eq!(probes[0].body.as_deref(), Some("Merged body"));
                    assert_eq!(fixture.row("title").await.as_deref(), Some("Merged title"));
                } else {
                    assert!(probes.is_empty(), "{case}: their prose is theirs");
                }
            }
        }
    }
}

/// Origin's `main` advanced past the store with nobody reading PR #43. The
/// observer lists recently closed pull requests, finds #43, and its refresh
/// resolves the row on GitHub's word. A pull request the listing does not name
/// is not fetched.
#[tokio::test(flavor = "current_thread")]
async fn an_external_base_advance_observes_a_merge_without_a_reader() {
    for listed in [vec![41, PR], vec![41]] {
        let fixture = fixture(&format!("seal-observer-{}.db", listed.len())).await;
        let github = github(vec![Pull::merged_at("2020-01-01T00:00:00Z")], &listed);
        let orch = fixture.orch(&github);

        super::refresh::observe_external_resolutions(&orch, "proj-seal", "main").await;

        assert_eq!(github.listings(), 1, "one listing per advance");
        if listed.contains(&PR) {
            assert_eq!(fixture.row("status").await.as_deref(), Some("merged"));
            let surface = fixture
                .db
                .query_opt_text(
                    "SELECT surface FROM pr_resolution_attributions WHERE merge_request_id = 'mr-seal'",
                    (),
                )
                .await
                .unwrap();
            assert_eq!(surface.as_deref(), Some("github-refresh-observation"));
        } else {
            assert_eq!(
                github.pull_fetches(),
                0,
                "an unlisted pull request is not read"
            );
            assert_eq!(fixture.row("status").await.as_deref(), Some("open"));
        }
    }
}
