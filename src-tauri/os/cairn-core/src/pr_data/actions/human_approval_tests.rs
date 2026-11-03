use super::*;
use crate::api::{ApiConfig, RunnerCredential};
use crate::services::testing::MockHttpClient;
use crate::services::{HttpClient, HttpResponse};
use std::collections::VecDeque;
use std::sync::Arc;

const FIXTURE: &str = include_str!("../../../../../../fixtures/approvals/runner-routes-v1.json");
const SEED: &str = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
const ENROLLMENT: &str = "cae_fixture00000000000000000001";
const CHALLENGE: &str = "apr_1";

fn fixture() -> serde_json::Value {
    serde_json::from_str(FIXTURE).unwrap()
}

// ─── A transport that records the order of service calls among world steps ───

type Log = Arc<Mutex<Vec<String>>>;

struct Recorder {
    inner: MockHttpClient,
    log: Log,
}

impl Recorder {
    fn note(&self, method: &str, url: &str) {
        let path = url.split("/approvals/").nth(1).unwrap_or(url);
        self.log.lock().unwrap().push(format!("{method} {path}"));
    }
}

type Fut<'a> = Pin<Box<dyn Future<Output = Result<HttpResponse, String>> + Send + 'a>>;

impl HttpClient for Recorder {
    fn get(&self, url: &str, headers: reqwest::header::HeaderMap) -> Fut<'_> {
        self.note("GET", url);
        self.inner.get(url, headers)
    }
    fn post(
        &self,
        url: &str,
        body: serde_json::Value,
        headers: reqwest::header::HeaderMap,
    ) -> Fut<'_> {
        self.note("POST", url);
        self.inner.post(url, body, headers)
    }
    fn put(
        &self,
        url: &str,
        body: serde_json::Value,
        headers: reqwest::header::HeaderMap,
    ) -> Fut<'_> {
        self.inner.put(url, body, headers)
    }
    fn patch(
        &self,
        url: &str,
        body: serde_json::Value,
        headers: reqwest::header::HeaderMap,
    ) -> Fut<'_> {
        self.inner.patch(url, body, headers)
    }
    fn delete(&self, url: &str, headers: reqwest::header::HeaderMap) -> Fut<'_> {
        self.inner.delete(url, headers)
    }
}

// ─── A scripted landing ───

struct FakeWorld {
    observations: VecDeque<Result<Observation, String>>,
    execution: Option<Execution<()>>,
    shape: Result<LandedShape, String>,
    log: Log,
}

impl LandingWorld for FakeWorld {
    type Landing = ();

    async fn observe(&mut self) -> Result<Observation, String> {
        self.log.lock().unwrap().push("world observe".into());
        self.observations
            .pop_front()
            .unwrap_or_else(|| Err("observed too often".into()))
    }

    async fn execute(&mut self, _approved: &Observation) -> Execution<()> {
        self.log.lock().unwrap().push("world execute".into());
        self.execution.take().expect("executed once")
    }

    async fn landed_shape(&self, _commit: &str) -> Result<LandedShape, String> {
        self.log.lock().unwrap().push("world landed_shape".into());
        self.shape.clone()
    }

    fn route(&self) -> &'static str {
        "github"
    }
}

struct FakeClock(AtomicI64);

impl Clock for FakeClock {
    fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.0
            .fetch_add(duration.as_secs() as i64, Ordering::SeqCst);
        Box::pin(async {})
    }
}

// ─── Inputs ───

fn observation() -> Observation {
    Observation {
        repository_id: "123456789".into(),
        repository_name: "bleugreen/cairn".into(),
        target_ref: "refs/heads/main".into(),
        target_oid: "11".repeat(20),
        source_ref: "refs/heads/feature/zeta".into(),
        source_oid: "22".repeat(20),
        source_message: "Fix parser crash on empty input\n\nLonger body.\n".into(),
        source_commit_count: 3,
        merge_base_oid: Some("33".repeat(20)),
        candidate_oid: Some("22".repeat(20)),
        result_tree_oid: "66".repeat(20),
        strategy: ApprovalMergeStrategy::Squash,
        deletes_branch: true,
        checks: CheckGate {
            counts: ApprovalCheckCounts {
                passed: 4,
                failed: 0,
                pending: 0,
            },
            blocking: None,
        },
    }
}

fn context() -> IntentContext {
    IntentContext {
        installation_id: "installation-1".into(),
        project_id: "project".into(),
        merge_plan_id: "attempt-1".into(),
        initiating_request_id: "job".into(),
        audience: "cairn-runner:runner-1".into(),
    }
}

fn credential() -> RunnerCredential {
    use base64::Engine as _;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(br#"{"sub":"runner:runner-1","org_id":"org-fixture-1"}"#);
    RunnerCredential::parse(&format!("e30.{payload}.sig")).unwrap()
}

fn approved_digest() -> [u8; 32] {
    intent_digest(&build_intent(&observation(), &context()).unwrap()).unwrap()
}

fn hex_bytes(value: &str) -> Vec<u8> {
    (0..value.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).unwrap())
        .collect()
}

fn fixture_claims() -> serde_json::Value {
    use base64::Engine as _;
    let jwt = fixture()["assertion"]["jwt"].as_str().unwrap().to_string();
    serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(jwt.split('.').nth(1).unwrap())
            .unwrap(),
    )
    .unwrap()
}

/// The fixture's Human assertion, minted again for this challenge and intent.
fn assertion_for(digest: &[u8; 32]) -> String {
    let mut claims = fixture_claims();
    claims["challenge_id"] = CHALLENGE.into();
    claims["intent_digest_sha256"] = digest_hex(digest).into();
    let mut der = hex_bytes("302e020100300506032b657004220420");
    der.extend(hex_bytes(SEED));
    jsonwebtoken::encode(
        &jsonwebtoken::Header {
            typ: Some("cairn-approval+jwt".into()),
            ..jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA)
        },
        &claims,
        &jsonwebtoken::EncodingKey::from_ed_der(&der),
    )
    .unwrap()
}

fn respond(status: u16, body: serde_json::Value) -> HttpResponse {
    HttpResponse::new(status, body.to_string().into_bytes())
}

fn status_body(state: &str, assertion: Option<String>) -> serde_json::Value {
    let mut body = fixture()["responses"]["status"]["pending"].clone();
    body["challengeId"] = CHALLENGE.into();
    body["state"] = state.into();
    body["intentDigestSha256"] = digest_hex(&approved_digest()).into();
    body["serviceAssertion"] = assertion.into();
    body
}

/// The service, answering the way a phone that approves would make it.
struct Service {
    enrollment_state: &'static str,
    statuses: Vec<HttpResponse>,
    claim: HttpResponse,
}

impl Default for Service {
    fn default() -> Self {
        Self {
            enrollment_state: "live",
            statuses: vec![
                respond(200, status_body("pending", None)),
                respond(
                    200,
                    status_body(
                        "authorized_not_executed",
                        Some(assertion_for(&approved_digest())),
                    ),
                ),
            ],
            claim: respond(201, fixture()["responses"]["claim"]["claimed"].clone()),
        }
    }
}

impl Service {
    fn mock(self) -> MockHttpClient {
        let fixture = fixture();
        let responses = &fixture["responses"];
        let mut issue = responses["issue"].clone();
        issue["challengeId"] = CHALLENGE.into();
        issue["intentDigestSha256"] = digest_hex(&approved_digest()).into();
        let mut standing = responses["enrollment"]["live"].clone();
        standing["state"] = self.enrollment_state.into();
        standing["installationId"] = "installation-1".into();
        MockHttpClient::new()
            .respond_to("/claims", self.claim)
            .respond_to(
                "/completions",
                respond(201, responses["completion"]["recorded"].clone()),
            )
            .respond_to(
                "/invalidations",
                respond(201, responses["invalidation"]["stale"].clone()),
            )
            .respond_to("/runner/enrollments/", respond(200, standing))
            .respond_to(
                "jwks.json",
                respond(200, fixture["assertion"]["jwks"].clone()),
            )
            .respond_to_sequence(&format!("challenges/{CHALLENGE}"), self.statuses)
            .respond_to_sequence("/approvals/challenges", vec![respond(201, issue)])
    }
}

async fn journal_db() -> LocalDb {
    let dir = tempfile::tempdir().unwrap();
    let db = LocalDb::open_migrated_for_test(dir.keep().join("approval.db"))
        .await
        .unwrap();
    for statement in [
        "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('workspace', 'W', 1, 1)",
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
         VALUES ('project', 'workspace', 'P', 'project', '/tmp/repo', 1, 1)",
        "INSERT INTO merge_requests (id, job_id, project_id, title, source_branch, target_branch,
         opened_at, updated_at) VALUES ('mr', 'job', 'project', 'PR', 'feature/zeta', 'main', 1, 1)",
    ] {
        db.execute(statement, ()).await.unwrap();
    }
    db
}

fn attempt() -> MergeAttemptJournal<'static> {
    MergeAttemptJournal {
        attempt_id: "attempt-1",
        project_id: "project",
        merge_request_id: "mr",
        job_id: "job",
        target_ref: "main",
        source_ref: "feature/zeta",
        policy_json: r#"{"version":1,"state":"enabled","approval_enrollment_id":"cae_fixture00000000000000000001"}"#,
    }
}

type Row = (String, Option<String>, Option<String>);

async fn journal_rows(db: &LocalDb) -> Vec<Row> {
    db.read(|conn| {
        Box::pin(async move {
            use crate::storage::RowExt;
            let mut rows = conn
                .query(
                    "SELECT state, human_principal_json, detail FROM merge_attempt_events
                      WHERE attempt_id = 'attempt-1' ORDER BY sequence",
                    (),
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?));
            }
            Ok(out)
        })
    })
    .await
    .unwrap()
}

fn states(rows: &[Row]) -> Vec<&str> {
    rows.iter().map(|row| row.0.as_str()).collect()
}

struct Run {
    result: Result<ApprovedLanding<()>, String>,
    rows: Vec<Row>,
    log: Vec<String>,
    requests: Vec<crate::services::testing::RecordedRequest>,
}

impl Run {
    fn posted(&self, suffix: &str) -> Vec<&serde_json::Value> {
        self.requests
            .iter()
            .filter(|request| request.url.ends_with(suffix))
            .filter_map(|request| request.body.as_ref())
            .collect()
    }
    fn executed(&self) -> bool {
        self.log.iter().any(|entry| entry == "world execute")
    }
    fn asked(&self) -> bool {
        self.requests
            .iter()
            .any(|request| request.url.ends_with("/approvals/challenges"))
    }
}

async fn run(
    service: Service,
    observations: Vec<Result<Observation, String>>,
    execution: Execution<()>,
    shape: Result<LandedShape, String>,
    enrollment_id: &str,
) -> Run {
    let db = journal_db().await;
    let log: Log = Arc::default();
    let http = Recorder {
        inner: service.mock(),
        log: log.clone(),
    };
    let client = ApprovalsClient::new(
        &http,
        Default::default(),
        ApiConfig {
            base_url: "https://api.ceremony.test".into(),
        },
        credential(),
    );
    let journal = AttemptJournal::new(&db, attempt(), 1);
    let clock = FakeClock(AtomicI64::new(
        fixture()["assertion"]["expected"]["verifyAtEpochSeconds"]
            .as_i64()
            .unwrap(),
    ));
    let ceremony = Ceremony {
        client: &client,
        journal: &journal,
        clock: &clock,
        enrollment_id,
        project_id: "project",
        attempt_id: "attempt-1",
        job_id: "job",
        title: Some("Fix parser crash".into()),
        description: Some("Body\u{202e} with an override".into()),
    };
    let mut world = FakeWorld {
        observations: observations.into(),
        execution: Some(execution),
        shape,
        log: log.clone(),
    };
    let result = ceremony.run(&mut world).await;
    let rows = journal_rows(&db).await;
    let log = log.lock().unwrap().clone();
    Run {
        result,
        rows,
        log,
        requests: http.inner.requests(),
    }
}

fn landed() -> Execution<()> {
    Execution::Landed {
        commit: "77".repeat(20),
        approved_head: "22".repeat(20),
        landing: (),
    }
}

fn squash_shape() -> LandedShape {
    LandedShape {
        parents: vec!["11".repeat(20)],
        tree: "66".repeat(20),
    }
}

async fn twice(
    service: Service,
    execution: Execution<()>,
    shape: Result<LandedShape, String>,
) -> Run {
    run(
        service,
        vec![Ok(observation()), Ok(observation())],
        execution,
        shape,
        ENROLLMENT,
    )
    .await
}

// ─── The accepted merge ───

#[tokio::test]
async fn an_approved_merge_is_proven_before_it_is_reported_and_journals_the_human() {
    let run = twice(Service::default(), landed(), Ok(squash_shape())).await;
    let landed = run.result.as_ref().expect("the approved merge lands");
    assert!(landed.proven);
    assert_eq!(
        states(&run.rows),
        [
            "pending",
            "authorized_not_executed",
            "executing",
            "succeeded"
        ]
    );

    // Verify before the claim; prove before reporting.
    let position = |entry: &str| {
        run.log
            .iter()
            .position(|logged| logged.contains(entry))
            .unwrap_or_else(|| panic!("{entry} never happened: {:?}", run.log))
    };
    assert!(position("jwks.json") < position("/claims"));
    assert_eq!(run.log.iter().filter(|e| *e == "world observe").count(), 2);
    assert!(position("/claims") < position("world execute"));
    assert!(position("world landed_shape") < position("/completions"));

    let completions = run.posted("/completions");
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0]["outcome"], "succeeded");
    assert_eq!(completions[0]["evidence"]["landedCommit"], "77".repeat(20));
    // The claim and its report reuse the ids journaled before the claim.
    let ids: ExecutionIds = serde_json::from_str(run.rows[1].2.as_deref().unwrap()).unwrap();
    assert_eq!(run.posted("/claims")[0]["claimId"], ids.claim_id.as_str());
    assert_eq!(completions[0]["reportId"], ids.report_id.as_str());

    // The Human principal is exactly the assertion's.
    let principal: serde_json::Value =
        serde_json::from_str(run.rows[3].1.as_deref().unwrap()).unwrap();
    assert_eq!(principal, fixture_claims()["principal"]);
    assert!(
        run.rows[0].1.is_none(),
        "no Human before the phone answered"
    );
}

#[tokio::test]
async fn the_advisory_has_exactly_three_keys_and_unsafe_text_is_null() {
    let run = twice(Service::default(), landed(), Ok(squash_shape())).await;
    let issue = run
        .requests
        .iter()
        .find(|request| request.url.ends_with("/approvals/challenges"))
        .and_then(|request| request.body.clone())
        .unwrap();
    let advisory = issue["advisory"].as_object().unwrap();
    let mut keys: Vec<&String> = advisory.keys().collect();
    keys.sort();
    assert_eq!(keys, ["checks", "pullRequestSummary", "pullRequestTitle"]);
    assert_eq!(advisory["pullRequestTitle"], "Fix parser crash");
    assert!(
        advisory["pullRequestSummary"].is_null(),
        "a bidi override is not shown"
    );
    assert_eq!(
        advisory["checks"],
        serde_json::json!({"passed": 4, "failed": 0, "pending": 0})
    );
}

// ─── Stale before the claim ───

/// A named change to one bound field of the observed merge.
type ObservationEdit = (&'static str, fn(&mut Observation));

#[tokio::test]
async fn every_bound_field_that_changes_after_approval_is_stale_and_unclaimed() {
    let edits: Vec<ObservationEdit> = vec![
        ("repository rename", |o| {
            o.repository_name = "bleugreen/cairn-renamed".into()
        }),
        ("source ref", |o| {
            o.source_ref = "refs/heads/feature/other".into()
        }),
        ("subject", |o| {
            o.source_message = "Something else entirely\n".into()
        }),
        ("commit count", |o| o.source_commit_count = 4),
        ("target tip", |o| o.target_oid = "aa".repeat(20)),
        ("source tip", |o| {
            o.source_oid = "bb".repeat(20);
            o.candidate_oid = Some("bb".repeat(20));
        }),
        ("tree", |o| o.result_tree_oid = "cc".repeat(20)),
        ("merge base", |o| o.merge_base_oid = Some("dd".repeat(20))),
    ];
    for (label, edit) in edits {
        let mut moved = observation();
        edit(&mut moved);
        let run = run(
            Service::default(),
            vec![Ok(observation()), Ok(moved)],
            landed(),
            Ok(squash_shape()),
            ENROLLMENT,
        )
        .await;
        let refusal = run.result.as_ref().expect_err(label);
        assert!(
            refusal.contains("changed after you approved"),
            "{label}: {refusal}"
        );
        assert_eq!(run.posted("/invalidations").len(), 1, "{label}");
        assert!(run.posted("/claims").is_empty(), "{label}: no claim");
        assert!(!run.executed(), "{label}: no provider mutation");
        assert_eq!(states(&run.rows).last(), Some(&"stale"), "{label}");
    }
}

#[test]
fn the_subject_is_derived_from_the_message_read_now() {
    let mut moved = observation();
    // Same first line, different body: the same subject.
    moved.source_message = "Fix parser crash on empty input\r\nother body".into();
    assert_eq!(
        build_intent(&moved, &context())
            .unwrap()
            .source_commit_subject,
        derive_commit_subject(&observation().source_message)
    );
    moved.source_message = "Fix parser crash on empty input!".into();
    assert_ne!(
        intent_digest(&build_intent(&moved, &context()).unwrap()).unwrap(),
        approved_digest()
    );
}

#[test]
fn the_intent_is_identical_when_nothing_changed() {
    assert_eq!(
        intent_digest(&build_intent(&observation(), &context()).unwrap()).unwrap(),
        intent_digest(&build_intent(&observation(), &context()).unwrap()).unwrap()
    );
}

#[tokio::test]
async fn an_unobservable_revalidation_is_stale_not_a_guess() {
    let run = run(
        Service::default(),
        vec![Ok(observation()), Err("the repository moved".into())],
        landed(),
        Ok(squash_shape()),
        ENROLLMENT,
    )
    .await;
    assert!(run.result.is_err());
    assert_eq!(
        run.posted("/invalidations")[0]["observedIntentDigestSha256"],
        "00".repeat(32)
    );
    assert!(run.posted("/claims").is_empty());
    assert!(!run.executed());
}

// ─── Checks ───

#[tokio::test]
async fn failing_checks_mean_the_phone_is_never_asked() {
    let mut red = observation();
    red.checks.blocking = Some("the gate check `rust` has not passed".into());
    let run = run(
        Service::default(),
        vec![Ok(red)],
        landed(),
        Ok(squash_shape()),
        ENROLLMENT,
    )
    .await;
    assert!(run
        .result
        .as_ref()
        .unwrap_err()
        .contains("require passing checks"));
    assert!(!run.asked());
    assert_eq!(states(&run.rows), ["failed"]);
}

#[tokio::test]
async fn checks_that_go_red_after_approval_refuse_without_a_claim() {
    let mut red = observation();
    red.checks.blocking = Some("the gate check `rust` has not passed".into());
    let run = run(
        Service::default(),
        vec![Ok(observation()), Ok(red)],
        landed(),
        Ok(squash_shape()),
        ENROLLMENT,
    )
    .await;
    assert!(run.result.is_err());
    assert!(run.posted("/claims").is_empty());
    assert!(!run.executed());
}

fn lane(
    name: &str,
    policy: &str,
    state: NodeCheckState,
    verified_on: Option<&str>,
) -> NodeCheckStatus {
    NodeCheckStatus {
        job_id: "job".into(),
        request_id: None,
        name: name.into(),
        state,
        policy: policy.into(),
        when: "write".into(),
        cached: None,
        duration_ms: None,
        ran_at: None,
        passed: None,
        failed: None,
        skipped: None,
        deferred: None,
        parser: None,
        complete: None,
        selection: None,
        suite_failures: None,
        failure_names: Vec::new(),
        output_tail: None,
        failure_kind: None,
        suppressed_after: None,
        executor: None,
        verified_on: verified_on.map(|head| crate::execution::checks_status::VerifiedHead {
            head: head.into(),
            base: "base".into(),
        }),
        notice: None,
        declared_unverified: None,
        progress: None,
        base_attribution: None,
    }
}

/// A test runner's lane with its parsed result's facts.
fn runner_lane(
    state: NodeCheckState,
    selection: &str,
    complete: bool,
    passed: usize,
) -> NodeCheckStatus {
    let mut lane = lane("tests", "gate", state, None);
    lane.parser = Some("vitest".into());
    lane.selection = Some(selection.into());
    lane.complete = Some(complete);
    lane.passed = Some(passed);
    lane.failed = Some(0);
    lane.skipped = Some(0);
    lane
}

#[test]
fn gate_checks_require_every_gate_lane_passed_on_the_tip() {
    let tip = "22".repeat(20);
    let green = [
        lane("api", "gate", NodeCheckState::Passed, None),
        lane("lint", "advisory", NodeCheckState::Failed, None),
        lane("types", "advisory", NodeCheckState::Running, None),
    ];
    let gate = gate_checks(Some(&green), &tip);
    assert_eq!(gate.blocking, None);
    assert_eq!(
        gate.counts,
        ApprovalCheckCounts {
            passed: 1,
            failed: 1,
            pending: 1
        }
    );
    let declared = {
        let mut lane = runner_lane(NodeCheckState::Unverified, "full", true, 3);
        lane.declared_unverified = Some("ran on a stale checkout".into());
        lane
    };
    let died = {
        let mut lane = lane("api", "gate", NodeCheckState::Failed, None);
        lane.failure_kind = Some("infrastructure".into());
        lane
    };
    for blocking in [
        lane("api", "gate", NodeCheckState::Failed, None),
        lane("api", "gate", NodeCheckState::Pending, None),
        lane("api", "gate", NodeCheckState::Running, None),
        lane("api", "gate", NodeCheckState::NotApplicable, None),
        lane("api", "gate", NodeCheckState::Unsupported, None),
        runner_lane(NodeCheckState::Unverified, "empty", true, 0),
        runner_lane(NodeCheckState::Unverified, "full", false, 3),
        declared,
        died,
        lane(
            "api",
            "gate",
            NodeCheckState::Passed,
            Some("an-earlier-head"),
        ),
    ] {
        assert!(
            gate_checks(Some(std::slice::from_ref(&blocking)), &tip)
                .blocking
                .is_some(),
            "{blocking:?}"
        );
    }
    assert!(gate_checks(
        Some(&[lane("api", "gate", NodeCheckState::Passed, Some(&tip))]),
        &tip
    )
    .blocking
    .is_none());
    assert!(
        gate_checks(None, &tip).blocking.is_some(),
        "an unreadable ledger blocks"
    );
}

/// A script check records no structured result: its exit code is its verdict,
/// as every other checks surface and merge gate reads it, so its green on the
/// tip satisfies `require_checks`.
#[test]
fn a_script_gate_check_green_on_the_tip_satisfies_the_gate() {
    let tip = "22".repeat(20);
    let script = lane("smoke", "gate", NodeCheckState::Unverified, Some(&tip));
    let gate = gate_checks(Some(std::slice::from_ref(&script)), &tip);
    assert_eq!(gate.blocking, None);
    assert_eq!(
        gate.counts,
        ApprovalCheckCounts {
            passed: 1,
            failed: 0,
            pending: 0
        }
    );

    let stale = lane(
        "smoke",
        "gate",
        NodeCheckState::Unverified,
        Some("an-earlier-head"),
    );
    let blocking = gate_checks(Some(std::slice::from_ref(&stale)), &tip)
        .blocking
        .unwrap();
    assert!(
        blocking.contains("`smoke`") && blocking.contains("earlier commit"),
        "{blocking}"
    );
}

/// A lane that ended without a verdict blocks the gate and says why, and is
/// counted as neither passing nor failing on the phone.
#[test]
fn a_gate_lane_without_a_verdict_blocks_and_names_its_state() {
    let tip = "22".repeat(20);
    let mut died = lane("api", "gate", NodeCheckState::Failed, Some(&tip));
    died.failure_kind = Some("infrastructure".into());
    let gate = gate_checks(Some(std::slice::from_ref(&died)), &tip);
    assert_eq!(gate.counts, ApprovalCheckCounts::default());
    let blocking = gate.blocking.unwrap();
    assert!(
        blocking.contains("no verdict") && blocking.contains("died: infrastructure"),
        "{blocking}"
    );

    let red = lane("api", "gate", NodeCheckState::Failed, Some(&tip));
    let blocking = gate_checks(Some(std::slice::from_ref(&red)), &tip)
        .blocking
        .unwrap();
    assert!(blocking.contains("it failed"), "{blocking}");
}

// ─── Refusals before execution ───

#[tokio::test]
async fn a_retired_phone_is_never_asked() {
    for state in ["revoked", "reset"] {
        let run = run(
            Service {
                enrollment_state: state,
                ..Service::default()
            },
            vec![Ok(observation())],
            landed(),
            Ok(squash_shape()),
            ENROLLMENT,
        )
        .await;
        let refusal = run.result.as_ref().unwrap_err();
        assert!(
            refusal.contains("Choose a phone in project settings"),
            "{refusal}"
        );
        assert!(!run.asked());
        assert_eq!(states(&run.rows), ["policy_unavailable"]);
    }
}

#[tokio::test]
async fn a_rebase_is_refused_before_the_phone_is_asked() {
    let mut rebase = observation();
    rebase.strategy = ApprovalMergeStrategy::Rebase;
    let run = run(
        Service::default(),
        vec![Ok(rebase)],
        landed(),
        Ok(squash_shape()),
        ENROLLMENT,
    )
    .await;
    assert!(run
        .result
        .as_ref()
        .unwrap_err()
        .contains("Rebase merges can't be approved"));
    assert!(!run.asked());
}

#[tokio::test]
async fn denied_superseded_revoked_and_stale_answers_refuse_without_a_claim() {
    for state in ["denied", "superseded", "authorization_revoked", "stale"] {
        let run = run(
            Service {
                statuses: vec![respond(200, status_body(state, None))],
                ..Service::default()
            },
            vec![Ok(observation())],
            landed(),
            Ok(squash_shape()),
            ENROLLMENT,
        )
        .await;
        assert!(run.result.is_err(), "{state}");
        assert!(run.posted("/claims").is_empty(), "{state}");
        assert!(!run.executed(), "{state}");
        assert_eq!(states(&run.rows).last(), Some(&state), "{state}");
    }
}

#[tokio::test]
async fn an_approval_given_hours_later_is_still_claimed_once() {
    // Three hours of polling at the fake clock's pace, then the phone approves.
    let polls = (3 * 60 * 60 / POLL_INTERVAL.as_secs()) as usize;
    let mut statuses: Vec<HttpResponse> = (0..polls)
        .map(|_| respond(200, status_body("pending", None)))
        .collect();
    statuses.push(respond(
        200,
        status_body(
            "authorized_not_executed",
            Some(assertion_for(&approved_digest())),
        ),
    ));
    let run = twice(
        Service {
            statuses,
            ..Service::default()
        },
        landed(),
        Ok(squash_shape()),
    )
    .await;
    assert!(run.result.is_ok(), "{:?}", run.result.as_ref().err());
    assert_eq!(
        states(&run.rows),
        [
            "pending",
            "authorized_not_executed",
            "executing",
            "succeeded"
        ]
    );
    assert_eq!(run.posted("/claims").len(), 1);
}

#[tokio::test]
async fn a_status_read_the_service_refuses_ends_the_wait_unclaimed() {
    let run = run(
        Service {
            statuses: vec![respond(
                404,
                serde_json::json!({ "error": "no such challenge" }),
            )],
            ..Service::default()
        },
        vec![Ok(observation())],
        landed(),
        Ok(squash_shape()),
        ENROLLMENT,
    )
    .await;
    assert!(run
        .result
        .as_ref()
        .unwrap_err()
        .contains("stopped answering for this approval request"));
    assert_eq!(states(&run.rows).last(), Some(&"failed"));
    assert!(run.posted("/claims").is_empty());
    assert!(!run.executed());
}

#[tokio::test]
async fn a_newer_request_superseding_an_approval_before_the_claim_mutates_nothing() {
    // The phone approved, the runner verified and revalidated, and a newer
    // request for the same merge superseded this one before the claim landed.
    let run = twice(
        Service {
            statuses: vec![
                respond(
                    200,
                    status_body(
                        "authorized_not_executed",
                        Some(assertion_for(&approved_digest())),
                    ),
                ),
                respond(200, status_body("superseded", None)),
            ],
            claim: respond(409, fixture()["responses"]["claim"]["unavailable"].clone()),
            ..Service::default()
        },
        landed(),
        Ok(squash_shape()),
    )
    .await;
    assert!(run.result.is_err());
    assert!(!run.executed());
    assert_eq!(states(&run.rows).last(), Some(&"superseded"));
}

#[tokio::test]
async fn an_approval_from_another_phone_is_never_claimed() {
    let run = run(
        Service::default(),
        vec![Ok(observation()), Ok(observation())],
        landed(),
        Ok(squash_shape()),
        "cae_other",
    )
    .await;
    assert!(run.result.is_err());
    assert_eq!(states(&run.rows).last(), Some(&"invalid_signature"));
    assert!(run.posted("/claims").is_empty());
    assert!(!run.executed());
}

#[tokio::test]
async fn a_claim_the_service_refuses_mutates_nothing() {
    let run = twice(
        Service {
            claim: respond(409, fixture()["responses"]["claim"]["unavailable"].clone()),
            ..Service::default()
        },
        landed(),
        Ok(squash_shape()),
    )
    .await;
    assert!(run.result.is_err());
    assert!(!run.executed());
    assert!(run.posted("/completions").is_empty());
}

// ─── After the claim ───

#[tokio::test]
async fn a_ref_that_moves_after_the_claim_is_failed_with_no_mutation() {
    let run = twice(
        Service::default(),
        Execution::BoundStateChanged {
            observed: serde_json::json!({ "targetOid": "aa".repeat(20) }),
        },
        Ok(squash_shape()),
    )
    .await;
    assert!(run
        .result
        .as_ref()
        .unwrap_err()
        .contains("Nothing was merged"));
    let completion = run.posted("/completions");
    assert_eq!(completion[0]["outcome"], "failed");
    assert_eq!(completion[0]["evidence"]["mutationIssued"], false);
    assert_eq!(
        completion[0]["evidence"]["reason"],
        "bound_state_changed_after_claim"
    );
    assert!(run.posted("/invalidations").is_empty());
    assert_eq!(states(&run.rows).last(), Some(&"failed"));
}

#[tokio::test]
async fn a_provider_refusal_is_failed_and_an_unknown_outcome_is_never_succeeded() {
    let refused = twice(
        Service::default(),
        Execution::Refused("GitHub refused the merge (405)".into()),
        Ok(squash_shape()),
    )
    .await;
    assert_eq!(refused.posted("/completions")[0]["outcome"], "failed");
    assert_eq!(states(&refused.rows).last(), Some(&"failed"));

    let unknown = twice(
        Service::default(),
        Execution::Unknown {
            reason: "PUT request failed: timeout".into(),
            approved_head: Some("22".repeat(20)),
        },
        Ok(squash_shape()),
    )
    .await;
    assert_eq!(
        unknown.posted("/completions")[0]["outcome"],
        "indeterminate"
    );
    assert_eq!(states(&unknown.rows).last(), Some(&"indeterminate"));
    // The bound head is journaled so reconciliation can prove the merge later.
    let detail: serde_json::Value =
        serde_json::from_str(unknown.rows.last().unwrap().2.as_deref().unwrap()).unwrap();
    assert_eq!(detail["approvedHead"], "22".repeat(20));

    let unreadable = twice(
        Service::default(),
        landed(),
        Err("the commit is not readable yet".into()),
    )
    .await;
    assert!(!unreadable.result.as_ref().unwrap().proven);
    assert_eq!(
        unreadable.posted("/completions")[0]["outcome"],
        "indeterminate"
    );
    assert!(!unreadable
        .posted("/completions")
        .iter()
        .any(|body| body["outcome"] == "succeeded"));
}

#[tokio::test]
async fn a_landing_that_is_not_the_approved_shape_is_a_violation() {
    let run = twice(
        Service::default(),
        landed(),
        Ok(LandedShape {
            parents: vec!["aa".repeat(20)],
            tree: "66".repeat(20),
        }),
    )
    .await;
    assert!(run.result.is_err());
    let outcomes: Vec<&serde_json::Value> = run
        .posted("/completions")
        .iter()
        .map(|body| &body["outcome"])
        .collect();
    assert_eq!(outcomes, ["indeterminate", "violation"]);
    assert_eq!(states(&run.rows)[3..], ["indeterminate", "violation"]);
}

// ─── Proof ───

fn intent_with(strategy: ApprovalMergeStrategy) -> BaseMergeIntentV2 {
    let mut intent = build_intent(&observation(), &context()).unwrap();
    intent.merge_strategy = strategy;
    intent
}

fn shape(parents: &[&str], tree: &str) -> LandedShape {
    LandedShape {
        parents: parents.iter().map(|p| p.repeat(20)).collect(),
        tree: tree.repeat(20),
    }
}

fn mismatch(proof: Proof) -> bool {
    matches!(proof, Proof::Mismatch(_))
}

#[test]
fn a_squash_has_one_parent_the_approved_base_and_the_approved_tree() {
    let intent = intent_with(ApprovalMergeStrategy::Squash);
    assert_eq!(prove_landing(&intent, None, &squash_shape()), Proof::Proven);
    for wrong in [
        shape(&["11", "22"], "66"),
        shape(&["aa"], "66"),
        shape(&["11"], "aa"),
        shape(&[], "66"),
    ] {
        assert!(mismatch(prove_landing(&intent, None, &wrong)), "{wrong:?}");
    }
}

#[test]
fn a_merge_has_the_base_then_the_approved_head_and_the_approved_tree() {
    let intent = intent_with(ApprovalMergeStrategy::Merge);
    let head = "22".repeat(20);
    let good = shape(&["11", "22"], "66");
    assert_eq!(prove_landing(&intent, Some(&head), &good), Proof::Proven);
    assert!(
        matches!(prove_landing(&intent, None, &good), Proof::CannotProve(_)),
        "an unknown head proves nothing, and is not a mismatch"
    );
    for wrong in [
        shape(&["11", "bb"], "66"),
        shape(&["22", "11"], "66"),
        shape(&["11", "22"], "aa"),
        shape(&["11"], "66"),
    ] {
        assert!(
            mismatch(prove_landing(&intent, Some(&head), &wrong)),
            "{wrong:?}"
        );
    }
}

#[test]
fn the_store_merge_method_is_named_a_rebase_and_refused() {
    assert_eq!(
        approval_strategy("squash", false),
        Ok(ApprovalMergeStrategy::Squash)
    );
    assert_eq!(
        approval_strategy("merge", true),
        Ok(ApprovalMergeStrategy::Merge)
    );
    assert_eq!(
        approval_strategy("merge", false),
        Ok(ApprovalMergeStrategy::Rebase)
    );
    assert_eq!(
        approval_strategy("rebase", true),
        Ok(ApprovalMergeStrategy::Rebase)
    );
    assert!(approval_strategy("octopus", true).is_err());
    let mut observed = observation();
    observed.strategy = ApprovalMergeStrategy::Rebase;
    assert!(build_intent(&observed, &context()).is_err());
}

// ─── Reconciliation ───

#[test]
fn reconciliation_never_reads_absence_of_evidence_as_failure() {
    let intent = intent_with(ApprovalMergeStrategy::Squash);
    let landed = LandingEvidence::Landed {
        commit: "77".repeat(20),
        shape: squash_shape(),
    };
    let wrong = LandingEvidence::Landed {
        commit: "77".repeat(20),
        shape: shape(&["aa"], "66"),
    };
    let gone = LandingEvidence::DefinitelyNotLanded("closed unmerged".into());
    let unknown = LandingEvidence::Unknown("no answer".into());
    use CompletionOutcome::*;
    let executing = ApprovalOutcome::Executing;
    let indeterminate = ApprovalOutcome::Indeterminate;
    // A bound merge whose head was never recorded cannot be proven: it stays
    // unsettled rather than reading as a violation.
    let merge = intent_with(ApprovalMergeStrategy::Merge);
    let merged = LandingEvidence::Landed {
        commit: "77".repeat(20),
        shape: shape(&["11", "99"], "66"),
    };
    assert_eq!(
        reconcile(executing, &merge, None, &merged),
        Reconciliation::Report(vec![Indeterminate])
    );
    assert_eq!(
        reconcile(indeterminate, &merge, None, &merged),
        Reconciliation::Leave
    );
    assert_eq!(
        reconcile(indeterminate, &merge, Some(&"99".repeat(20)), &merged),
        Reconciliation::Report(vec![Succeeded])
    );
    for (latest, evidence, expected) in [
        (executing, &landed, Reconciliation::Report(vec![Succeeded])),
        (
            executing,
            &unknown,
            Reconciliation::Report(vec![Indeterminate]),
        ),
        (
            executing,
            &wrong,
            Reconciliation::Report(vec![Indeterminate, Violation]),
        ),
        (executing, &gone, Reconciliation::Report(vec![Failed])),
        (
            indeterminate,
            &landed,
            Reconciliation::Report(vec![Succeeded]),
        ),
        (indeterminate, &gone, Reconciliation::Report(vec![Failed])),
        (
            indeterminate,
            &wrong,
            Reconciliation::Report(vec![Violation]),
        ),
        (indeterminate, &unknown, Reconciliation::Leave),
    ] {
        assert_eq!(
            reconcile(latest, &intent, None, evidence),
            expected,
            "{latest:?} {evidence:?}"
        );
    }
}

/// A store history as a map from commit to shape.
fn history(commits: &[(&str, &[&str], &str)]) -> impl Fn(&str) -> Result<LandedShape, String> {
    let map: std::collections::HashMap<String, LandedShape> = commits
        .iter()
        .map(|(commit, parents, tree)| (commit.repeat(20), shape(parents, tree)))
        .collect();
    move |commit: &str| {
        map.get(commit)
            .cloned()
            .ok_or_else(|| format!("{commit} is unreadable"))
    }
}

#[test]
fn store_evidence_credits_only_this_attempts_fold() {
    let intent = intent_with(ApprovalMergeStrategy::Squash);
    // The attempt's own fold, with a later change on top.
    let ours = history(&[("a1", &["77"], "a2"), ("77", &["11"], "66")]);
    assert_eq!(
        store_evidence(&"a1".repeat(20), &intent, &ours),
        LandingEvidence::Landed {
            commit: "77".repeat(20),
            shape: shape(&["11"], "66")
        }
    );
    // Untouched: definitively not landed.
    assert!(matches!(
        store_evidence(&"11".repeat(20), &intent, &ours),
        LandingEvidence::DefinitelyNotLanded(_)
    ));
    // Another landing on the approved base (another tree) is not credited:
    // this attempt's fold can no longer happen.
    let theirs = history(&[("b1", &["11"], "bb")]);
    let evidence = store_evidence(&"b1".repeat(20), &intent, &theirs);
    assert!(
        matches!(evidence, LandingEvidence::DefinitelyNotLanded(_)),
        "{evidence:?}"
    );
    assert_eq!(
        reconcile(ApprovalOutcome::Indeterminate, &intent, None, &evidence),
        Reconciliation::Report(vec![CompletionOutcome::Failed]),
        "a later landing is never this attempt's violation"
    );
    // A history that no longer passes through the approved base.
    let elsewhere = history(&[("c1", &["c0"], "cc"), ("c0", &[], "c0")]);
    assert!(matches!(
        store_evidence(&"c1".repeat(20), &intent, &elsewhere),
        LandingEvidence::DefinitelyNotLanded(_)
    ));
    // A gap in what can be read is not evidence.
    let gap = history(&[("d1", &["d0"], "dd")]);
    assert!(matches!(
        store_evidence(&"d1".repeat(20), &intent, &gap),
        LandingEvidence::Unknown(_)
    ));
}

#[test]
fn github_attribution_credits_only_the_head_this_attempt_bound() {
    let approved = "22".repeat(20);
    let merged = |head: &str| PullRequestState {
        merged: true,
        closed: true,
        head: Some(head.repeat(20)),
        merge_commit: Some("77".repeat(20)),
    };
    assert_eq!(
        github_attribution(&merged("22"), Some(&approved)),
        Ok("77".repeat(20))
    );
    // A merge of another head, made after this attempt, is someone else's.
    assert!(matches!(
        github_attribution(&merged("bb"), Some(&approved)),
        Err(LandingEvidence::DefinitelyNotLanded(_))
    ));
    // With no recorded head nothing is attributed.
    assert!(matches!(
        github_attribution(&merged("22"), None),
        Err(LandingEvidence::Unknown(_))
    ));
    let open = PullRequestState {
        merged: false,
        closed: false,
        head: Some(approved.clone()),
        merge_commit: None,
    };
    assert!(matches!(
        github_attribution(&open, Some(&approved)),
        Err(LandingEvidence::Unknown(_))
    ));
    let closed = PullRequestState {
        closed: true,
        ..open
    };
    assert!(matches!(
        github_attribution(&closed, Some(&approved)),
        Err(LandingEvidence::DefinitelyNotLanded(_))
    ));
}

#[tokio::test]
async fn an_unconfirmed_claim_is_reported_failed_with_no_mutation() {
    let unreachable = HttpResponse::new(503, Vec::new());
    let run = twice(
        Service {
            claim: unreachable,
            ..Service::default()
        },
        landed(),
        Ok(squash_shape()),
    )
    .await;
    assert!(run.result.is_err());
    assert!(!run.executed());
    let completion = run.posted("/completions");
    assert_eq!(completion[0]["outcome"], "failed");
    assert_eq!(completion[0]["evidence"]["mutationIssued"], false);
    assert_eq!(states(&run.rows).last(), Some(&"failed"));
}

#[tokio::test]
async fn a_second_approved_merge_waits_for_an_unsettled_one() {
    let db = journal_db().await;
    assert_eq!(unsettled_attempt_for(&db, "mr").await.unwrap(), None);
    let journal = AttemptJournal::new(&db, attempt(), 1);
    journal
        .append(ApprovalOutcome::Executing, JournalEntry::default())
        .await
        .unwrap();
    assert_eq!(
        unsettled_attempt_for(&db, "mr").await.unwrap().as_deref(),
        Some("attempt-1")
    );
    journal
        .append(ApprovalOutcome::Succeeded, JournalEntry::default())
        .await
        .unwrap();
    assert_eq!(unsettled_attempt_for(&db, "mr").await.unwrap(), None);
}
