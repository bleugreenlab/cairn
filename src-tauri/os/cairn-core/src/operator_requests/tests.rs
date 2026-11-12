use super::*;
use crate::db::DbState;
use crate::orchestrator::OrchestratorBuilder;
use crate::services::testing::TestServicesBuilder;
use crate::storage::{LocalDb, SearchIndex};
use async_trait::async_trait;
use cairn_common::identity::{
    Address, AppearanceEvidence, CredentialRef, VerificationMethod, VerificationRecord,
    VerificationStatus, VerificationStrength,
};
use registry::ExecutorTarget;
use std::sync::Mutex;

fn commit(letter: char) -> String {
    letter.to_string().repeat(40)
}

/// Only this cfg(test) module registers simulated physical actors.
struct TestConsumer {
    kind: RequestKind,
    facts: Mutex<ActorFacts>,
    contexts: Mutex<Vec<AcceptedRequestContext>>,
    dispatch_error: Mutex<Option<String>>,
}
impl TestConsumer {
    fn new(kind: RequestKind) -> Arc<Self> {
        Arc::new(Self {
            kind,
            facts: Mutex::new(ActorFacts {
                document: json!({"source":"test-only physical evidence"}),
                running_version: Some("1.2.3".into()),
                main_commit: Some(commit('b')),
                candidate_commit: Some(commit('b')),
                base_commit: Some(commit('b')),
                policy_project_id: Some("project".into()),
                drain: (kind == RequestKind::LiftDrain).then(|| DrainAcceptance {
                    executor_id: "machine".into(),
                    drain_key: cairn_config::fleet::FleetConfig::drain_key("machine").to_string(),
                    owner: "operator".into(),
                    generation: 7,
                    machine_revision: 9,
                }),
            }),
            contexts: Mutex::new(Vec::new()),
            dispatch_error: Mutex::new(None),
        })
    }
    fn context(&self) -> AcceptedRequestContext {
        let contexts = self.contexts.lock().unwrap();
        let c = contexts
            .last()
            .expect("dispatch must receive a checked context");
        AcceptedRequestContext {
            event_id: c.event_id.clone(),
            claim_id: c.claim_id.clone(),
            acceptance: c.acceptance.clone(),
            adapter: c.adapter.clone(),
        }
    }
}

#[async_trait]
impl RequestConsumer for TestConsumer {
    fn kind(&self) -> &'static str {
        self.kind.as_str()
    }
    fn actor(&self) -> &'static str {
        registry::definition(self.kind()).unwrap().actor
    }
    async fn availability(&self, _: &Orchestrator) -> Result<(), String> {
        Ok(())
    }
    async fn facts(
        &self,
        _: &Orchestrator,
        _: Option<&str>,
        _: Option<&str>,
    ) -> Result<ActorFacts, String> {
        Ok(self.facts.lock().unwrap().clone())
    }
    async fn validate_snapshot(
        &self,
        _: &Orchestrator,
        a: &RequestAcceptance,
    ) -> Result<(), String> {
        if a.kind != self.kind || a.actor_id != self.actor() {
            return Err("wrong physical actor/kind".into());
        }
        if a.kind == RequestKind::LiftDrain && a.drain != self.facts.lock().unwrap().drain {
            return Err("drain generation changed".into());
        }
        Ok(())
    }
    async fn contains_commits(
        &self,
        _: &Orchestrator,
        candidate: &str,
        required: &[String],
    ) -> Result<(), String> {
        // A deterministic three-commit linear history: a <- b <- c.
        let history = [commit('a'), commit('b'), commit('c')];
        let tip = history
            .iter()
            .position(|c| c == candidate)
            .ok_or("unknown candidate")?;
        for required in required {
            if !history[..=tip].contains(required) {
                return Err(format!("uncovered commit {required}"));
            }
        }
        Ok(())
    }
    async fn executor_target(&self, _: &Orchestrator, uri: &str) -> Result<ExecutorTarget, String> {
        if uri != "cairn://executors/machine" {
            return Err("unknown executor".into());
        }
        Ok(ExecutorTarget {
            uri: uri.into(),
            executor_id: "machine".into(),
            drain_key: cairn_config::fleet::FleetConfig::drain_key("machine").to_string(),
        })
    }
    async fn verify_report(
        &self,
        _: &Orchestrator,
        _: &AcceptedRequestContext,
        report: ConsumerReport,
    ) -> Result<ConsumerReport, String> {
        if report.document.get("verified") != Some(&json!(true)) {
            return Err("decisive physical verification is missing".into());
        }
        Ok(report)
    }
    async fn dispatch(
        &self,
        orch: &Orchestrator,
        context: &AcceptedRequestContext,
    ) -> Result<(), String> {
        context.recheck(orch).await?;
        self.contexts.lock().unwrap().push(AcceptedRequestContext {
            event_id: context.event_id.clone(),
            claim_id: context.claim_id.clone(),
            acceptance: context.acceptance.clone(),
            adapter: context.adapter.clone(),
        });
        if let Some(error) = self.dispatch_error.lock().unwrap().clone() {
            return Err(error);
        }
        Ok(())
    }
}

async fn sql(orch: &Orchestrator, statement: &str) {
    orch.db.local.execute(statement, ()).await.unwrap();
}
async fn fixture() -> Orchestrator {
    let root = tempfile::tempdir().unwrap().keep();
    let local = LocalDb::open_migrated_for_test(root.join("requests.db"))
        .await
        .unwrap();
    let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
    let orch = OrchestratorBuilder::new(
        Arc::new(DbState::new(Arc::new(local), search)),
        Arc::new(TestServicesBuilder::new().build()),
        root,
    )
    .build();
    for statement in [
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('workspace','W',1,1)",
        "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('project','workspace','P','project','/tmp/requests',1,1)",
        "INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES('thread','project','devops',1,1)",
        "INSERT INTO issues(id,project_id,number,title,status,parent_thread_id,created_at,updated_at) VALUES('issue','project',1,'Request','active','thread',1,1)",
        "INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('execution','recipe','issue','project','running',1,1)",
        "INSERT INTO jobs(id,execution_id,issue_id,project_id,node_name,status,created_at,updated_at,uri_segment) VALUES('job','execution','issue','project','Builder','running',1,1,'builder')",
        "INSERT INTO jobs(id,execution_id,issue_id,project_id,parent_job_id,node_name,status,created_at,updated_at,uri_segment) VALUES('child','execution','issue','project','job','Child','running',1,1,'child')",
        "INSERT INTO jobs(id,project_id,thread_id,node_name,status,created_at,updated_at,uri_segment) VALUES('thread-job','project','thread','Thread','running',1,1,'thread')",
        "INSERT INTO jobs(id,project_id,agent_config_id,node_name,status,created_at,updated_at,uri_segment) VALUES('service-job','project','service','Service','running',1,1,'service')",
        "INSERT INTO runs(id,project_id,issue_id,job_id,status,created_at,updated_at,start_mode) VALUES('run','project','issue','job','live',1,1,'resume')",
        "INSERT INTO runs(id,project_id,issue_id,job_id,status,created_at,updated_at,start_mode) VALUES('child-run','project','issue','child','live',1,1,'resume')",
        "INSERT INTO runs(id,project_id,job_id,status,created_at,updated_at,start_mode) VALUES('thread-run','project','thread-job','live',1,1,'resume')",
        "INSERT INTO runs(id,project_id,job_id,status,created_at,updated_at,start_mode) VALUES('service-run','project','service-job','live',1,1,'resume')",
    ] { sql(&orch, statement).await; }
    orch
}
fn callback(run: &str) -> CallbackRequest {
    CallbackRequest {
        run_id: Some(run.into()),
        ..Default::default()
    }
}
fn input(kind: RequestKind, target: Option<&str>, rationale: &str) -> FileOperatorRequest {
    FileOperatorRequest {
        kind,
        target: target.map(str::to_string),
        rationale: rationale.into(),
        waiting: vec!["cairn://p/project/1".into()],
        requirements: Default::default(),
    }
}
fn register(orch: &Orchestrator, kind: RequestKind) -> Arc<TestConsumer> {
    let c = TestConsumer::new(kind);
    orch.operator_requests.register(c.clone()).unwrap();
    c
}
fn operator() -> (PrincipalRef, AppearanceSnapshot) {
    let principal = PrincipalRef::Machine {
        device_id: "test-device".into(),
    };
    let verification = VerificationRecord::new(
        VerificationMethod::DesktopCredential,
        VerificationStatus::Verified,
        None,
        None,
        None,
        Some(CredentialRef::new("desktop_operator_credential").unwrap()),
        VerificationStrength::new("local_shared_secret").unwrap(),
        1,
    )
    .unwrap();
    let evidence = AppearanceEvidence::new(
        AppearanceTransport::AuthenticatedDesktop,
        Address::Desktop {
            device_id: "test-device".into(),
        },
        verification,
        1,
        None,
    )
    .unwrap();
    let appearance = AppearanceSnapshot::new(principal.clone(), evidence, vec![], None).unwrap();
    (principal, appearance)
}
async fn accept_card(orch: &Orchestrator, r: &OperatorRequest) -> Result<OperatorRequest, String> {
    let (p, a) = operator();
    accept(
        orch,
        &r.id,
        r.revision,
        r.facts.as_ref().unwrap().version,
        &p,
        &a,
    )
    .await
}
async fn policy(orch: &Orchestrator, enabled: bool) {
    let value = if enabled {
        cairn_db::models::HumanBaseMergePolicy::enabled_v1("phone").encode()
    } else {
        cairn_db::models::HumanBaseMergePolicy::DISABLED_V1.encode()
    };
    orch.db
        .local
        .execute(
            "UPDATE projects SET human_base_merge_policy=?1 WHERE id='project'",
            (value,),
        )
        .await
        .unwrap();
}
fn report(outcome: RequestAttemptOutcome, verified: bool, settled: bool) -> ConsumerReport {
    ConsumerReport {
        outcome,
        document: json!({"verified":verified}),
        settled,
        built_commit: Some(commit('b')),
    }
}
async fn file_rebuild(orch: &Orchestrator, run: &str, rationale: &str) -> OperatorRequest {
    file(
        orch,
        &callback(run),
        input(RequestKind::Rebuild, None, rationale),
    )
    .await
    .unwrap()
    .request
}

#[tokio::test]
async fn production_unavailable_refuses_filing_and_acceptance_without_authorizing() {
    let mut orch = fixture().await;
    for kind in [
        RequestKind::Rebuild,
        RequestKind::BumpVersion,
        RequestKind::LiftDrain,
    ] {
        let e = file(&orch, &callback("run"), input(kind, Some("sm"), "need it"))
            .await
            .unwrap_err();
        assert!(
            e.contains(kind.as_str()) && e.contains("UNAVAILABLE"),
            "{e}"
        );
    }
    assert!(list(&orch, RequestFilter::default())
        .await
        .unwrap()
        .is_empty());
    // Persisted intent does not turn a missing production adapter into authority.
    register(&orch, RequestKind::Rebuild);
    let card = file_rebuild(&orch, "run", "persisted intent").await;
    orch.operator_requests = Arc::new(registry::RequestRegistry::default());
    let error = accept_card(&orch, &card).await.unwrap_err();
    assert!(
        error.contains("rebuild") && error.contains("UNAVAILABLE"),
        "{error}"
    );
    assert!(get(&orch, &card.id)
        .await
        .unwrap()
        .unwrap()
        .attempts
        .is_empty());
}

#[tokio::test]
async fn origins_are_authenticated_and_delegation_wakes_the_owning_thread() {
    let orch = fixture().await;
    register(&orch, RequestKind::Rebuild);
    for run in ["run", "child-run"] {
        let card = file_rebuild(&orch, run, run).await;
        let contribution = card.requesters.iter().find(|r| r.run_id == run).unwrap();
        assert_eq!(contribution.origin_uri, "cairn://p/project/1");
        assert_eq!(contribution.wake_destination, "cairn://p/project/devops");
        assert!(
            matches!(&contribution.principal, PrincipalRef::Agent { run_id: Some(id), .. } if id == run)
        );
    }
    let card = file_rebuild(&orch, "thread-run", "thread origin").await;
    let contribution = card
        .requesters
        .iter()
        .find(|r| r.run_id == "thread-run")
        .unwrap();
    assert_eq!(contribution.origin_uri, "cairn://p/project/devops");
    assert_eq!(contribution.wake_destination, contribution.origin_uri);
    sql(
        &orch,
        "UPDATE issues SET parent_thread_id=NULL WHERE id='issue'",
    )
    .await;
    let card = file_rebuild(&orch, "run", "parentless origin").await;
    let contribution = card
        .requesters
        .iter()
        .find(|r| r.rationale == "parentless origin")
        .unwrap();
    assert_eq!(contribution.wake_destination, "cairn://p/project/1");
    assert!(origin(&orch, &CallbackRequest::default()).await.is_err());
    assert!(origin(&orch, &callback("missing-run")).await.is_err());
    assert!(origin(&orch, &callback("service-run")).await.is_err());
}

#[tokio::test]
async fn rebuild_requirements_are_fixed_and_unlanded_commits_are_refused() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::Rebuild);
    let first = file_rebuild(&orch, "run", "bare main").await;
    assert_eq!(first.requesters[0].requirements.commits, vec![commit('b')]);
    let mut ask = input(RequestKind::Rebuild, None, "unlanded");
    ask.requirements.commits = vec![commit('c')];
    let error = file(&orch, &callback("thread-run"), ask).await.unwrap_err();
    assert!(
        error.contains("unlanded") && error.contains(&commit('c')),
        "{error}"
    );
    consumer.facts.lock().unwrap().main_commit = Some(commit('c'));
    assert_eq!(
        get(&orch, &first.id).await.unwrap().unwrap().requesters[0]
            .requirements
            .commits,
        vec![commit('b')]
    );
}

#[tokio::test]
async fn displayed_candidate_is_pinned_but_join_and_fact_refresh_invalidate_the_view() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::Rebuild);
    let shown = file_rebuild(&orch, "run", "shown b").await;
    {
        let mut f = consumer.facts.lock().unwrap();
        f.main_commit = Some(commit('c'));
        f.candidate_commit = Some(commit('c'));
    }
    let accepted = accept_card(&orch, &shown).await.unwrap();
    assert_eq!(
        accepted.attempts[0].acceptance.candidate_commit,
        Some(commit('b'))
    );
    let successor = file_rebuild(&orch, "thread-run", "late ask").await;
    assert_ne!(successor.id, shown.id);
    let joined = file_rebuild(&orch, "run", "another successor ask").await;
    assert_eq!(joined.id, successor.id);
    assert!(accept_card(&orch, &successor)
        .await
        .unwrap_err()
        .contains("stale"));
    refresh_facts(&orch, &joined.id).await.unwrap();
    assert!(accept_card(&orch, &joined)
        .await
        .unwrap_err()
        .contains("stale"));
    let current = get(&orch, &joined.id).await.unwrap().unwrap();
    assert!(accept_card(&orch, &current)
        .await
        .unwrap_err()
        .contains("unresolved"));
    dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
        .await
        .unwrap();
    consumer
        .context()
        .report(
            &orch,
            "complete-earlier",
            report(RequestAttemptOutcome::Done, true, true),
        )
        .await
        .unwrap();
    assert!(accept_card(&orch, &current).await.is_ok());
}

#[tokio::test]
async fn bump_request_refuses_human_only_base_merge_policy() {
    let orch = fixture().await;
    register(&orch, RequestKind::BumpVersion);
    policy(&orch, true).await;
    let ask = || input(RequestKind::BumpVersion, Some("sm"), "bump");
    let error = file(&orch, &callback("run"), ask()).await.unwrap_err();
    assert!(
        error.contains("bump_version")
            && error.contains("human_base_merge_policy")
            && error.contains("provably-human"),
        "{error}"
    );
    assert!(list(&orch, RequestFilter::default())
        .await
        .unwrap()
        .is_empty());
    policy(&orch, false).await;
    let card = file(&orch, &callback("run"), ask()).await.unwrap().request;
    policy(&orch, true).await;
    assert!(accept_card(&orch, &card)
        .await
        .unwrap_err()
        .contains("human_base_merge_policy"));
    assert!(get(&orch, &card.id)
        .await
        .unwrap()
        .unwrap()
        .attempts
        .is_empty());
    policy(&orch, false).await;
    let accepted = accept_card(&orch, &card).await.unwrap();
    policy(&orch, true).await;
    let event = &accepted.attempts[0].authorization_event_id;
    assert!(dispatch_accepted(&orch, event)
        .await
        .unwrap_err()
        .contains("human_base_merge_policy"));
    let reopened = get(&orch, &card.id).await.unwrap().unwrap();
    assert_eq!(reopened.state, RequestState::Open);
    assert_gate(&orch, &reopened, "accepted", true).await;
    assert_gate(&orch, &reopened, "done", false).await;
    policy(&orch, false).await;
    assert!(dispatch_accepted(&orch, event).await.is_err());
    let fresh = accept_card(&orch, &reopened).await.unwrap();
    assert!(fresh
        .attempts
        .iter()
        .any(|a| a.state == RequestState::Accepted && a.authorization_event_id != *event));
}

#[tokio::test]
async fn policy_enabled_after_claim_blocks_live_recheck_but_allows_failure_reporting() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::BumpVersion);
    let card = file(
        &orch,
        &callback("run"),
        input(RequestKind::BumpVersion, Some("sm"), "bump"),
    )
    .await
    .unwrap()
    .request;
    let accepted = accept_card(&orch, &card).await.unwrap();
    dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
        .await
        .unwrap();
    let context = consumer.context();
    policy(&orch, true).await;
    assert!(context
        .recheck(&orch)
        .await
        .unwrap_err()
        .contains("human_base_merge_policy"));
    context
        .report(
            &orch,
            "policy-refusal",
            report(RequestAttemptOutcome::Failed, true, true),
        )
        .await
        .unwrap();
    let failed = get(&orch, &card.id).await.unwrap().unwrap();
    assert_eq!(failed.state, RequestState::Open);
    assert!(failed.fulfilled_by_attempt.is_none());
    assert!(failed
        .requesters
        .iter()
        .all(|c| c.satisfied_by_attempt.is_none()));
    assert_eq!(
        failed.attempts[0].outcome,
        Some(RequestAttemptOutcome::Failed)
    );
}

#[tokio::test]
async fn bump_tiers_and_explicit_minimums_coalesce_without_reduction_and_stale_target_refuses() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::BumpVersion);
    let large = file(
        &orch,
        &callback("run"),
        input(RequestKind::BumpVersion, Some("lg"), "major"),
    )
    .await
    .unwrap();
    let smaller = file(
        &orch,
        &callback("thread-run"),
        input(RequestKind::BumpVersion, Some("1.2.9"), "explicit"),
    )
    .await
    .unwrap();
    assert!(!smaller.created);
    assert_eq!(large.request.id, smaller.request.id);
    assert_eq!(smaller.request.target.as_deref(), Some("2.0.0"));
    assert_eq!(
        smaller
            .request
            .requesters
            .iter()
            .map(|r| r.minimum_target.as_deref().unwrap())
            .collect::<std::collections::BTreeSet<_>>(),
        ["1.2.9", "2.0.0"].into_iter().collect()
    );
    consumer.facts.lock().unwrap().running_version = Some("2.0.0".into());
    let e = accept_card(&orch, &smaller.request).await.unwrap_err();
    assert!(
        e.contains("stale") && e.contains("No implicit extra bump"),
        "{e}"
    );
    assert_eq!(
        get(&orch, &large.request.id).await.unwrap().unwrap().state,
        RequestState::Open
    );
}

#[tokio::test]
async fn checked_lifecycle_rejects_duplicate_claim_missing_record_and_replayed_context() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::Rebuild);
    assert!(dispatch_accepted(&orch, "no-record").await.is_err());
    let card = file_rebuild(&orch, "run", "rebuild").await;
    let accepted = accept_card(&orch, &card).await.unwrap();
    assert_eq!(accepted.state, RequestState::Accepted);
    let event = &accepted.attempts[0].authorization_event_id;
    dispatch_accepted(&orch, event).await.unwrap();
    assert_eq!(consumer.contexts.lock().unwrap().len(), 1);
    assert!(dispatch_accepted(&orch, event).await.is_err());
    let context = consumer.context();
    context.recheck(&orch).await.unwrap();
    assert_eq!(
        get(&orch, &card.id).await.unwrap().unwrap().state,
        RequestState::Acting
    );
    assert!(context
        .report(
            &orch,
            "unverified",
            report(RequestAttemptOutcome::Done, false, true)
        )
        .await
        .is_err());
    let mut wrong_built = report(RequestAttemptOutcome::Done, true, true);
    wrong_built.built_commit = Some(commit('c'));
    assert!(context
        .report(&orch, "wrong-built", wrong_built)
        .await
        .is_err());
    assert!(context
        .report(
            &orch,
            "unsettled-success",
            report(RequestAttemptOutcome::Done, true, false)
        )
        .await
        .is_err());
    assert_eq!(
        get(&orch, &card.id).await.unwrap().unwrap().state,
        RequestState::Acting
    );
    let mut fabricated = consumer.context();
    fabricated.event_id = "no-record".into();
    assert!(fabricated.recheck(&orch).await.is_err());
    let mut wrong_commit = consumer.context();
    wrong_commit.acceptance.candidate_commit = Some(commit('c'));
    assert!(wrong_commit.recheck(&orch).await.is_err());
    let mut wrong_claim = consumer.context();
    wrong_claim.claim_id = "other-claim".into();
    assert!(wrong_claim.recheck(&orch).await.is_err());
    context
        .recheck_for(&orch, context.acceptance())
        .await
        .unwrap();
    for field in [
        "commit",
        "kind",
        "actor",
        "target",
        "install",
        "generation",
        "base",
        "membership",
        "facts",
    ] {
        let mut wrong = consumer.context();
        match field {
            "commit" => wrong.acceptance.candidate_commit = Some(commit('c')),
            "kind" => wrong.acceptance.kind = RequestKind::BumpVersion,
            "actor" => wrong.acceptance.actor_id = "other.actor".into(),
            "target" => wrong.acceptance.target = Some("2.0.0".into()),
            "install" => wrong.acceptance.install_id = "other-install".into(),
            "generation" => {
                wrong.acceptance.drain = TestConsumer::new(RequestKind::LiftDrain)
                    .facts
                    .lock()
                    .unwrap()
                    .drain
                    .clone()
            }
            "base" => wrong.acceptance.base_commit = Some(commit('c')),
            "membership" => wrong.acceptance.request_ids.push("other-request".into()),
            "facts" => wrong.acceptance.facts_version += 1,
            _ => unreachable!(),
        }
        assert!(
            context
                .recheck_for(&orch, wrong.acceptance())
                .await
                .unwrap_err()
                .contains("Intended privileged act"),
            "{field}"
        );
        assert!(wrong.recheck(&orch).await.is_err(), "{field}");
    }
    context
        .report(
            &orch,
            "verified-done",
            report(RequestAttemptOutcome::Done, true, true),
        )
        .await
        .unwrap();
    context
        .report(
            &orch,
            "verified-done",
            report(RequestAttemptOutcome::Done, true, true),
        )
        .await
        .unwrap();
    let done = get(&orch, &card.id).await.unwrap().unwrap();
    assert_eq!(done.state, RequestState::Done);
    assert_eq!(done.attempts[0].outcome, Some(RequestAttemptOutcome::Done));
    assert!(context.recheck(&orch).await.is_err());
    assert!(dispatch_accepted(&orch, event).await.is_err());
}

#[tokio::test]
async fn unsettled_failure_retains_the_fence_until_positive_settlement() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::Rebuild);
    let card = file_rebuild(&orch, "run", "first").await;
    let accepted = accept_card(&orch, &card).await.unwrap();
    dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
        .await
        .unwrap();
    let context = consumer.context();
    let successor = file_rebuild(&orch, "thread-run", "late").await;
    context
        .report(
            &orch,
            "unknown-liveness",
            report(RequestAttemptOutcome::Inconclusive, true, false),
        )
        .await
        .unwrap();
    let current = get(&orch, &successor.id).await.unwrap().unwrap();
    assert!(accept_card(&orch, &current).await.is_err());
    assert!(context.recheck(&orch).await.is_err());
    context
        .report(
            &orch,
            "positive-settlement",
            report(RequestAttemptOutcome::Failed, true, true),
        )
        .await
        .unwrap();
    let reopened = get(&orch, &card.id).await.unwrap().unwrap();
    assert_eq!(reopened.state, RequestState::Open);
    assert!(accept_card(&orch, &reopened).await.is_ok());
}

#[tokio::test]
async fn withdrawal_and_decline_do_not_grant_or_cancel_an_accepted_act() {
    let orch = fixture().await;
    register(&orch, RequestKind::Rebuild);
    let first = file_rebuild(&orch, "run", "issue asks").await;
    let joined = file_rebuild(&orch, "thread-run", "thread asks").await;
    assert_eq!(first.id, joined.id);
    let remaining = withdraw(&orch, &callback("run"), &first.id).await.unwrap();
    assert_eq!(remaining.state, RequestState::Open);
    assert_eq!(remaining.requesters.iter().filter(|r| r.active).count(), 1);
    let (p, a) = operator();
    assert!(
        decline(&orch, &remaining.id, remaining.revision, "  ", &p, &a)
            .await
            .is_err()
    );
    let declined = decline(&orch, &remaining.id, remaining.revision, "not now", &p, &a)
        .await
        .unwrap();
    assert_eq!(declined.state, RequestState::Declined);
    assert!(declined.attempts.is_empty());
    let next = file_rebuild(&orch, "run", "again").await;
    accept_card(&orch, &next).await.unwrap();
    assert!(withdraw(&orch, &callback("run"), &next.id).await.is_err());
}

#[tokio::test]
async fn lift_drain_freezes_generation_and_rechecks_it_before_dispatch() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::LiftDrain);
    let card = file(
        &orch,
        &callback("run"),
        input(
            RequestKind::LiftDrain,
            Some("cairn://executors/machine"),
            "resume",
        ),
    )
    .await
    .unwrap()
    .request;
    let accepted = accept_card(&orch, &card).await.unwrap();
    assert_eq!(
        accepted.attempts[0]
            .acceptance
            .drain
            .as_ref()
            .unwrap()
            .generation,
        7
    );
    consumer
        .facts
        .lock()
        .unwrap()
        .drain
        .as_mut()
        .unwrap()
        .generation = 8;
    assert!(
        dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
            .await
            .unwrap_err()
            .contains("generation")
    );
    assert!(consumer.contexts.lock().unwrap().is_empty());
}

#[test]
fn waiting_uris_reject_resources_and_noncanonical_addresses_and_deduplicate() {
    assert_eq!(
        waiting_uris(vec![
            "cairn://p/project/1".into(),
            "cairn://p/project/1".into()
        ])
        .unwrap(),
        vec!["cairn://p/project/1"]
    );
    for bad in [
        "cairn://requests/id",
        "cairn://p/PROJECT/1",
        "cairn://p/project/1/1/builder",
        "cairn://p/project/devops/messages",
    ] {
        assert!(waiting_uris(vec![bad.into()]).is_err(), "{bad}");
    }
    assert!(waiting_uris(vec!["cairn://p/project/1".into(); 101]).is_err());
}

#[tokio::test]
async fn bump_report_fulfills_only_proven_open_rebuild_contributions_without_fake_acceptance() {
    let orch = fixture().await;
    let rebuild = register(&orch, RequestKind::Rebuild);
    let bump = register(&orch, RequestKind::BumpVersion);
    rebuild.facts.lock().unwrap().main_commit = Some(commit('c'));
    let mut first = input(RequestKind::Rebuild, None, "covered a");
    first.requirements.commits = vec![commit('a')];
    let r = file(&orch, &callback("run"), first).await.unwrap().request;
    let mut second = input(RequestKind::Rebuild, None, "not covered c");
    second.requirements.commits = vec![commit('c')];
    file(&orch, &callback("thread-run"), second).await.unwrap();
    let b = file(
        &orch,
        &callback("run"),
        input(RequestKind::BumpVersion, Some("sm"), "bump and rebuild"),
    )
    .await
    .unwrap()
    .request;
    let accepted = accept_card(&orch, &b).await.unwrap();
    dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
        .await
        .unwrap();
    let context = bump.context();
    for _ in 0..2 {
        let mut verified = report(RequestAttemptOutcome::Done, true, true);
        verified.built_commit = Some(commit('b'));
        context
            .report(&orch, "bump-built-b", verified)
            .await
            .unwrap();
    }
    let mut conflicting = report(RequestAttemptOutcome::Done, true, true);
    conflicting.document = json!({"verified":true, "changedEvidence":true});
    assert!(context
        .report(&orch, "bump-built-b", conflicting)
        .await
        .is_err());
    let partial = get(&orch, &r.id).await.unwrap().unwrap();
    assert_eq!(partial.state, RequestState::Open);
    assert!(partial.attempts.is_empty());
    assert!(partial.current_attempt.is_none());
    assert_eq!(
        partial
            .requesters
            .iter()
            .find(|c| c.rationale == "covered a")
            .unwrap()
            .satisfied_by_attempt
            .as_deref(),
        Some(context.acceptance().attempt_id.as_str())
    );
    assert!(partial
        .requesters
        .iter()
        .find(|c| c.rationale == "not covered c")
        .unwrap()
        .satisfied_by_attempt
        .is_none());

    let next = file(
        &orch,
        &callback("run"),
        input(RequestKind::BumpVersion, Some("md"), "cover remaining"),
    )
    .await
    .unwrap()
    .request;
    let accepted = accept_card(&orch, &next).await.unwrap();
    dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
        .await
        .unwrap();
    let context = bump.context();
    let mut verified = report(RequestAttemptOutcome::Done, true, true);
    verified.built_commit = Some(commit('c'));
    context
        .report(&orch, "bump-built-c", verified)
        .await
        .unwrap();
    let fulfilled = get(&orch, &r.id).await.unwrap().unwrap();
    assert_eq!(fulfilled.state, RequestState::Done);
    assert_gate(&orch, &fulfilled, "done", true).await;
    assert_gate(&orch, &fulfilled, "accepted", false).await;
    assert!(fulfilled.attempts.is_empty());
    assert!(fulfilled.current_attempt.is_none());
    assert_eq!(
        fulfilled.fulfilled_by_attempt.as_deref(),
        Some(context.acceptance().attempt_id.as_str())
    );
}

#[tokio::test]
async fn settled_failure_preserves_attempt_and_joins_late_successor_before_fresh_acceptance() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::Rebuild);
    let first = file_rebuild(&orch, "run", "first").await;
    let accepted = accept_card(&orch, &first).await.unwrap();
    dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
        .await
        .unwrap();
    let old = consumer.context();
    let late = file_rebuild(&orch, "thread-run", "late").await;
    old.report(
        &orch,
        "settled-failure",
        report(RequestAttemptOutcome::Failed, true, true),
    )
    .await
    .unwrap();
    let reopened = get(&orch, &first.id).await.unwrap().unwrap();
    assert_eq!(reopened.state, RequestState::Open);
    assert!(reopened.member_request_ids.contains(&first.id));
    assert!(reopened.member_request_ids.contains(&late.id));
    assert_eq!(
        reopened.attempts[0].outcome,
        Some(RequestAttemptOutcome::Failed)
    );
    assert_gate(&orch, &reopened, "accepted", true).await;
    assert_gate(&orch, &reopened, "done", false).await;
    assert_gate(&orch, &late, "accepted", false).await;
    assert_gate(&orch, &late, "done", false).await;
    let fresh = accept_card(&orch, &reopened).await.unwrap();
    let attempt = fresh
        .attempts
        .iter()
        .find(|a| a.state == RequestState::Accepted)
        .unwrap();
    assert_ne!(attempt.id, old.acceptance().attempt_id);
    assert_eq!(attempt.acceptance.request_ids.len(), 2);
    assert_gate(&orch, &late, "accepted", true).await;
    assert!(old.recheck(&orch).await.is_err());
    assert!(reopened.fulfilled_by_attempt.is_none());
    assert!(reopened
        .requesters
        .iter()
        .all(|c| c.satisfied_by_attempt.is_none()));
}

#[tokio::test]
async fn request_done_by_its_own_attempt_records_that_attempt_as_its_fulfillment() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::Rebuild);
    let card = file_rebuild(&orch, "run", "first").await;
    file_rebuild(&orch, "thread-run", "second").await;
    let card = get(&orch, &card.id).await.unwrap().unwrap();
    assert_eq!(card.requesters.len(), 2);
    let accepted = accept_card(&orch, &card).await.unwrap();
    dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
        .await
        .unwrap();
    let context = consumer.context();
    context
        .report(
            &orch,
            "rebuild-done",
            report(RequestAttemptOutcome::Done, true, true),
        )
        .await
        .unwrap();
    let attempt = context.acceptance().attempt_id.clone();
    let done = get(&orch, &card.id).await.unwrap().unwrap();
    assert_eq!(done.state, RequestState::Done);
    assert_eq!(done.current_attempt.as_deref(), Some(attempt.as_str()));
    assert_eq!(done.fulfilled_by_attempt.as_deref(), Some(attempt.as_str()));
    assert!(done
        .requesters
        .iter()
        .filter(|c| c.active)
        .all(|c| c.satisfied_by_attempt.as_deref() == Some(attempt.as_str())));
}

async fn assert_gate(
    orch: &Orchestrator,
    request: &OperatorRequest,
    outcome: &str,
    satisfied: bool,
) {
    use crate::execution::gates::vocabulary::{GateContext, GateOutcome, GateVocabulary};
    let clause = std::collections::BTreeMap::from([
        ("fact".into(), json!("request")),
        ("request".into(), json!(request.uri)),
        ("outcome".into(), json!(outcome)),
    ]);
    let actual = GateVocabulary::default()
        .resolve(&GateContext::new(orch, now()), &clause)
        .await
        .unwrap();
    assert_eq!(
        actual == GateOutcome::Satisfied,
        satisfied,
        "{} {outcome}: {actual:?}",
        request.uri
    );
}

#[test]
fn release_tier_overflow_is_refused_instead_of_wrapping() {
    for (running, tier) in [
        (ReleaseVersion(1, 2, u64::MAX), "sm"),
        (ReleaseVersion(1, u64::MAX, 2), "md"),
        (ReleaseVersion(u64::MAX, 1, 2), "lg"),
    ] {
        assert!(running.resolve(tier).unwrap_err().contains("overflow"));
    }
}

#[tokio::test]
async fn registered_consumer_cannot_be_replaced_by_another_adapter() {
    let orch = fixture().await;
    let first = register(&orch, RequestKind::Rebuild);
    assert!(orch
        .operator_requests
        .register(TestConsumer::new(RequestKind::Rebuild))
        .unwrap_err()
        .contains("already"));
    let resolved = orch.operator_requests.consumer("rebuild").unwrap();
    let first: Arc<dyn RequestConsumer> = first;
    assert!(Arc::ptr_eq(&first, &resolved));
}

#[tokio::test]
async fn agent_appearance_and_mismatched_operator_cannot_accept_or_decline() {
    let orch = fixture().await;
    register(&orch, RequestKind::Rebuild);
    let card = file_rebuild(&orch, "run", "needs operator").await;
    let request = callback("run");
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, &request)
        .await
        .unwrap();
    let (principal, appearance) = agent.appearance(1).unwrap();
    assert!(accept(
        &orch,
        &card.id,
        card.revision,
        card.facts.as_ref().unwrap().version,
        &principal,
        &appearance
    )
    .await
    .is_err());
    assert!(decline(
        &orch,
        &card.id,
        card.revision,
        "agent decision",
        &principal,
        &appearance
    )
    .await
    .is_err());
    let (_, appearance) = operator();
    let other = PrincipalRef::Machine {
        device_id: "different-device".into(),
    };
    assert!(accept(
        &orch,
        &card.id,
        card.revision,
        card.facts.as_ref().unwrap().version,
        &other,
        &appearance
    )
    .await
    .is_err());
    let unchanged = get(&orch, &card.id).await.unwrap().unwrap();
    assert_eq!(unchanged.state, RequestState::Open);
    assert!(unchanged.attempts.is_empty());
}

#[tokio::test]
async fn failed_bump_leaves_linked_rebuild_open_and_successful_rebuild_does_not_fulfill_bump() {
    let orch = fixture().await;
    let rebuild = register(&orch, RequestKind::Rebuild);
    let bump = register(&orch, RequestKind::BumpVersion);
    let r = file_rebuild(&orch, "run", "open rebuild").await;
    let b = file(
        &orch,
        &callback("run"),
        input(RequestKind::BumpVersion, Some("sm"), "bump"),
    )
    .await
    .unwrap()
    .request;
    let accepted = accept_card(&orch, &b).await.unwrap();
    dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
        .await
        .unwrap();
    bump.context()
        .report(
            &orch,
            "bump-failed",
            report(RequestAttemptOutcome::Failed, true, true),
        )
        .await
        .unwrap();
    let open = get(&orch, &r.id).await.unwrap().unwrap();
    assert_eq!(open.state, RequestState::Open);
    assert!(open.fulfilled_by_attempt.is_none());
    assert!(open
        .requesters
        .iter()
        .all(|c| c.satisfied_by_attempt.is_none()));
    assert!(open.report.is_some());
    let accepted = accept_card(&orch, &open).await.unwrap();
    dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
        .await
        .unwrap();
    rebuild
        .context()
        .report(
            &orch,
            "rebuild-done",
            report(RequestAttemptOutcome::Done, true, true),
        )
        .await
        .unwrap();
    let still_bump = get(&orch, &b.id).await.unwrap().unwrap();
    assert_eq!(still_bump.state, RequestState::Open);
    assert!(still_bump.fulfilled_by_attempt.is_none());
    assert_eq!(
        still_bump.attempts[0].outcome,
        Some(RequestAttemptOutcome::Failed)
    );
}

#[tokio::test]
async fn malformed_requests_refuse_before_persisting_intent() {
    let orch = fixture().await;
    register(&orch, RequestKind::Rebuild);
    register(&orch, RequestKind::BumpVersion);
    for ask in [
        input(RequestKind::Rebuild, None, " "),
        input(RequestKind::Rebuild, Some("sm"), "invalid rebuild target"),
        input(RequestKind::BumpVersion, Some("1.2.3"), "not newer"),
        input(RequestKind::BumpVersion, Some("1.2.4-alpha"), "prerelease"),
    ] {
        assert!(file(&orch, &callback("run"), ask).await.is_err());
    }
    let mut wrong_requirements = input(RequestKind::BumpVersion, Some("sm"), "wrong requirements");
    wrong_requirements.requirements.commits.push(commit('a'));
    assert!(file(&orch, &callback("run"), wrong_requirements)
        .await
        .is_err());
    let mut wrong_waiting = input(RequestKind::Rebuild, None, "invalid wait");
    wrong_waiting.waiting.push("cairn://requests/other".into());
    assert!(file(&orch, &callback("run"), wrong_waiting).await.is_err());
    assert!(list(&orch, RequestFilter::default())
        .await
        .unwrap()
        .is_empty());
}

async fn dispatch_intent_pending(orch: &Orchestrator, event: &str) -> bool {
    store::pending_outbox(&orch.db.local, 200)
        .await
        .unwrap()
        .iter()
        .any(|i| {
            i.kind == "dispatch"
                && i.payload
                    .get("authorization_event_id")
                    .and_then(Value::as_str)
                    == Some(event)
        })
}

#[tokio::test]
async fn delivery_acknowledges_terminal_report_after_crash_without_redispatch() {
    for outcome in [RequestAttemptOutcome::Done, RequestAttemptOutcome::Failed] {
        let orch = fixture().await;
        sql(
            &orch,
            "UPDATE issues SET parent_thread_id=NULL WHERE id='issue'",
        )
        .await;
        let consumer = register(&orch, RequestKind::Rebuild);
        let card = file_rebuild(&orch, "run", "recover report-before-ack").await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        let event = &accepted.attempts[0].authorization_event_id;
        dispatch_accepted(&orch, event).await.unwrap();
        consumer
            .context()
            .report(&orch, "terminal-before-ack", report(outcome, true, true))
            .await
            .unwrap();
        assert!(dispatch_intent_pending(&orch, event).await);
        delivery::drain_once(&orch).await.unwrap();
        assert!(!dispatch_intent_pending(&orch, event).await);
        delivery::drain_once(&orch).await.unwrap();
        assert_eq!(consumer.contexts.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn delivery_keeps_claimed_or_unsettled_dispatch_intent_without_relaunch() {
    let orch = fixture().await;
    sql(
        &orch,
        "UPDATE issues SET parent_thread_id=NULL WHERE id='issue'",
    )
    .await;
    let consumer = register(&orch, RequestKind::Rebuild);
    let card = file_rebuild(&orch, "run", "unsettled claim").await;
    let accepted = accept_card(&orch, &card).await.unwrap();
    let event = &accepted.attempts[0].authorization_event_id;
    dispatch_accepted(&orch, event).await.unwrap();
    delivery::drain_once(&orch).await.unwrap();
    assert!(dispatch_intent_pending(&orch, event).await);
    assert_eq!(consumer.contexts.lock().unwrap().len(), 1);
    consumer
        .context()
        .report(
            &orch,
            "unsettled",
            report(RequestAttemptOutcome::Inconclusive, true, false),
        )
        .await
        .unwrap();
    delivery::drain_once(&orch).await.unwrap();
    assert!(dispatch_intent_pending(&orch, event).await);
    assert_eq!(consumer.contexts.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn identical_bare_rebuild_refiling_preserves_original_commit_after_main_advances() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::Rebuild);
    let first = file_rebuild(&orch, "run", "same bare ask").await;
    {
        let mut facts = consumer.facts.lock().unwrap();
        facts.main_commit = Some(commit('c'));
        facts.candidate_commit = Some(commit('c'));
    }
    let duplicate = file_rebuild(&orch, "run", "same bare ask").await;
    assert_eq!(first.id, duplicate.id);
    assert_eq!(duplicate.requesters.len(), 1);
    assert_eq!(
        duplicate.requesters[0].requirements.commits,
        vec![commit('b')]
    );
    let distinct = file_rebuild(&orch, "run", "new bare ask").await;
    assert_eq!(distinct.requesters.len(), 2);
    assert_eq!(
        distinct
            .requesters
            .iter()
            .find(|r| r.rationale == "new bare ask")
            .unwrap()
            .requirements
            .commits,
        vec![commit('c')]
    );
}

#[tokio::test]
async fn identical_tier_refiling_preserves_original_minimum_after_running_version_advances() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::BumpVersion);
    let first = file(
        &orch,
        &callback("run"),
        input(RequestKind::BumpVersion, Some("sm"), "same tier"),
    )
    .await
    .unwrap()
    .request;
    consumer.facts.lock().unwrap().running_version = Some("1.2.9".into());
    let duplicate = file(
        &orch,
        &callback("run"),
        input(RequestKind::BumpVersion, Some("sm"), "same tier"),
    )
    .await
    .unwrap()
    .request;
    assert_eq!(first.id, duplicate.id);
    assert_eq!(duplicate.requesters.len(), 1);
    assert_eq!(
        duplicate.requesters[0].minimum_target.as_deref(),
        Some("1.2.4")
    );
    assert_eq!(duplicate.target.as_deref(), Some("1.2.4"));
    let distinct = file(
        &orch,
        &callback("run"),
        input(
            RequestKind::BumpVersion,
            Some("sm"),
            "new tier contribution",
        ),
    )
    .await
    .unwrap()
    .request;
    assert_eq!(distinct.requesters.len(), 2);
    assert_eq!(distinct.target.as_deref(), Some("1.2.10"));
    assert_eq!(
        distinct
            .requesters
            .iter()
            .find(|r| r.rationale == "same tier")
            .unwrap()
            .minimum_target
            .as_deref(),
        Some("1.2.4")
    );
}

#[tokio::test]
async fn identical_explicit_refiling_returns_original_request_but_never_authorizes_stale_target() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::BumpVersion);
    let first = file(
        &orch,
        &callback("run"),
        input(RequestKind::BumpVersion, Some("1.2.4"), "same explicit ask"),
    )
    .await
    .unwrap()
    .request;
    consumer.facts.lock().unwrap().running_version = Some("1.2.9".into());
    let repeated = file(
        &orch,
        &callback("run"),
        input(RequestKind::BumpVersion, Some("1.2.4"), "same explicit ask"),
    )
    .await
    .unwrap();
    assert!(!repeated.created);
    assert_eq!(repeated.request.id, first.id);
    assert_eq!(repeated.request.revision, first.revision);
    assert_eq!(repeated.request.requesters, first.requesters);
    assert_eq!(repeated.request.target.as_deref(), Some("1.2.4"));
    assert!(accept_card(&orch, &repeated.request)
        .await
        .unwrap_err()
        .contains("stale"));
    assert!(get(&orch, &first.id)
        .await
        .unwrap()
        .unwrap()
        .attempts
        .is_empty());
}

#[tokio::test]
async fn consumer_failure_secrets_are_scrubbed_before_outbox_and_request_report_persistence() {
    use crate::security::{
        registry as secrets, SecretCategory, SecretId, SecretMaterial, REDACTED,
    };
    const SECRET: &str = "request-test-Zx91Qw82Lm-secret-value";
    let _guard = secrets::registry()
        .register(
            SecretId::new("operator-request-failure-test"),
            SecretCategory::ProviderKey,
            "test-only failed consumer",
            SecretMaterial::from_string(SECRET.into()),
        )
        .unwrap();
    let orch = fixture().await;
    sql(
        &orch,
        "UPDATE issues SET parent_thread_id=NULL WHERE id='issue'",
    )
    .await;
    let consumer = register(&orch, RequestKind::Rebuild);
    consumer.facts.lock().unwrap().document = json!({"diagnostic":SECRET});
    let card = file_rebuild(&orch, "run", &format!("request with credential {SECRET}")).await;
    let accepted = accept_card(&orch, &card).await.unwrap();
    let event = &accepted.attempts[0].authorization_event_id;
    let intent = store::pending_outbox(&orch.db.local, 200)
        .await
        .unwrap()
        .into_iter()
        .find(|i| {
            i.kind == "dispatch"
                && i.payload
                    .get("authorization_event_id")
                    .and_then(Value::as_str)
                    == Some(event)
        })
        .unwrap();
    *consumer.dispatch_error.lock().unwrap() = Some(format!("physical launch failed: {SECRET}"));
    delivery::drain_once(&orch).await.unwrap();
    let error = orch
        .db
        .local
        .query_opt_text(
            "SELECT last_error FROM request_outbox WHERE id=?1",
            (intent.id,),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(!error.contains(SECRET));
    assert!(error.contains(REDACTED));
    let failed = get(&orch, &card.id).await.unwrap().unwrap();
    assert_eq!(failed.state, RequestState::Failed);
    assert_eq!(
        failed.attempts[0].outcome,
        Some(RequestAttemptOutcome::Inconclusive)
    );
    assert!(failed
        .report
        .as_ref()
        .unwrap()
        .to_string()
        .contains(REDACTED));
    assert!(!serde_json::to_string(&failed).unwrap().contains(SECRET));
    assert_eq!(consumer.contexts.lock().unwrap().len(), 1);
    assert!(dispatch_intent_pending(&orch, event).await);
}

#[tokio::test]
async fn durable_context_recovery_requires_exact_registered_consumer_and_unresolved_claim() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::Rebuild);
    let card = file_rebuild(&orch, "run", "recover persisted claim").await;
    let accepted = accept_card(&orch, &card).await.unwrap();
    let event = accepted.attempts[0].authorization_event_id.clone();
    let intended = accepted.attempts[0].acceptance.clone();
    assert!(
        recover_claimed_request(&orch, &event, &intended, consumer.clone())
            .await
            .is_err()
    );
    dispatch_accepted(&orch, &event).await.unwrap();
    let original = consumer.context();
    drop(original);
    consumer.contexts.lock().unwrap().clear();
    assert!(
        recover_claimed_request(&orch, "no-record", &intended, consumer.clone())
            .await
            .is_err()
    );
    assert!(recover_claimed_request(
        &orch,
        &event,
        &intended,
        TestConsumer::new(RequestKind::Rebuild)
    )
    .await
    .is_err());
    assert!(recover_claimed_request(
        &orch,
        &event,
        &intended,
        TestConsumer::new(RequestKind::BumpVersion)
    )
    .await
    .is_err());
    for field in [
        "commit",
        "membership",
        "facts",
        "actor",
        "install",
        "target",
        "kind",
    ] {
        let mut wrong = intended.clone();
        match field {
            "commit" => wrong.candidate_commit = Some(commit('c')),
            "membership" => wrong.request_ids.push("other-request".into()),
            "facts" => wrong.facts_version += 1,
            "actor" => wrong.actor_id = "other.actor".into(),
            "install" => wrong.install_id = "other-install".into(),
            "target" => wrong.target = Some("2.0.0".into()),
            "kind" => wrong.kind = RequestKind::BumpVersion,
            _ => unreachable!(),
        }
        assert!(
            recover_claimed_request(&orch, &event, &wrong, consumer.clone())
                .await
                .is_err(),
            "{field}"
        );
    }
    let recovered = recover_claimed_request(&orch, &event, &intended, consumer.clone())
        .await
        .unwrap();
    recovered.recheck_for(&orch, &intended).await.unwrap();
    recovered
        .report(
            &orch,
            "recovered-done",
            report(RequestAttemptOutcome::Done, true, true),
        )
        .await
        .unwrap();
    assert_eq!(
        get(&orch, &card.id).await.unwrap().unwrap().state,
        RequestState::Done
    );
    drop(recovered);
    let terminal = recover_claimed_request(&orch, &event, &intended, consumer.clone())
        .await
        .unwrap();
    assert!(terminal.recheck_for(&orch, &intended).await.is_err());
    terminal
        .report(
            &orch,
            "recovered-done",
            report(RequestAttemptOutcome::Done, true, true),
        )
        .await
        .unwrap();
    assert!(terminal
        .report(
            &orch,
            "new-terminal-receipt",
            report(RequestAttemptOutcome::Done, true, true)
        )
        .await
        .is_err());
    let mut changed = report(RequestAttemptOutcome::Done, true, true);
    changed.document = json!({"verified":true, "changed":true});
    assert!(terminal
        .report(&orch, "recovered-done", changed)
        .await
        .is_err());
    assert!(
        consumer.contexts.lock().unwrap().is_empty(),
        "recovery never dispatches another physical actor"
    );
}

#[tokio::test]
async fn recovered_unsettled_failure_can_settle_but_cannot_restore_live_effects_or_bypass_policy() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::BumpVersion);
    let card = file(
        &orch,
        &callback("run"),
        input(RequestKind::BumpVersion, Some("sm"), "recover failed bump"),
    )
    .await
    .unwrap()
    .request;
    let accepted = accept_card(&orch, &card).await.unwrap();
    let event = accepted.attempts[0].authorization_event_id.clone();
    let intended = accepted.attempts[0].acceptance.clone();
    dispatch_accepted(&orch, &event).await.unwrap();
    let original = consumer.context();
    original
        .report(
            &orch,
            "unknown-consumer",
            report(RequestAttemptOutcome::Inconclusive, true, false),
        )
        .await
        .unwrap();
    drop(original);
    consumer.contexts.lock().unwrap().clear();
    policy(&orch, true).await;
    let recovered = recover_claimed_request(&orch, &event, &intended, consumer.clone())
        .await
        .unwrap();
    assert!(recovered.recheck_for(&orch, &intended).await.is_err());
    assert!(recovered
        .report(
            &orch,
            "forbidden-failed-to-done",
            report(RequestAttemptOutcome::Done, true, true)
        )
        .await
        .is_err());
    recovered
        .report(
            &orch,
            "recovered-positive-settlement",
            report(RequestAttemptOutcome::Failed, true, true),
        )
        .await
        .unwrap();
    let reopened = get(&orch, &card.id).await.unwrap().unwrap();
    assert_eq!(reopened.state, RequestState::Open);
    assert_gate(&orch, &reopened, "accepted", true).await;
    assert_gate(&orch, &reopened, "done", false).await;
    assert!(accept_card(&orch, &reopened)
        .await
        .unwrap_err()
        .contains("human_base_merge_policy"));
    drop(recovered);
    let terminal = recover_claimed_request(&orch, &event, &intended, consumer.clone())
        .await
        .unwrap();
    assert!(terminal.recheck_for(&orch, &intended).await.is_err());
    terminal
        .report(
            &orch,
            "recovered-positive-settlement",
            report(RequestAttemptOutcome::Failed, true, true),
        )
        .await
        .unwrap();
    assert!(terminal
        .report(
            &orch,
            "new-failure-receipt",
            report(RequestAttemptOutcome::Failed, true, true)
        )
        .await
        .is_err());
    let mut changed = report(RequestAttemptOutcome::Failed, true, true);
    changed.document = json!({"verified":true, "changed":true});
    assert!(terminal
        .report(&orch, "recovered-positive-settlement", changed)
        .await
        .is_err());
    assert!(consumer.contexts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn delegated_filing_reference_reaches_owning_thread_and_replay_does_not_retarget_new_run() {
    let orch = fixture().await;
    register(&orch, RequestKind::Rebuild);
    let card = file_rebuild(&orch, "child-run", "delegated request reference").await;
    let reference = store::pending_outbox(&orch.db.local, 200)
        .await
        .unwrap()
        .into_iter()
        .find(|i| i.kind == "reference" && i.request_id == card.id)
        .unwrap();
    delivery::drain_once(&orch).await.unwrap();
    let refs: Vec<(String, String)> = orch
        .db
        .local
        .query_all(
            "SELECT run_id,data FROM events WHERE event_type='request_reference' ORDER BY id"
                .to_string(),
            (),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].0, "thread-run");
    assert!(refs[0].1.contains(&card.uri));
    sql(&orch, "INSERT INTO runs(id,project_id,job_id,status,created_at,updated_at,start_mode) VALUES('new-thread-run','project','thread-job','live',2,2,'resume')").await;
    orch.db
        .local
        .execute(
            "UPDATE request_outbox SET delivered_at=NULL WHERE id=?1",
            (reference.id,),
        )
        .await
        .unwrap();
    delivery::drain_once(&orch).await.unwrap();
    let refs: Vec<String> = orch
        .db
        .local
        .query_all(
            "SELECT run_id FROM events WHERE event_type='request_reference' ORDER BY id"
                .to_string(),
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert_eq!(
        refs,
        vec!["thread-run"],
        "receipt replay preserves original destination across thread run rotation"
    );
}

#[tokio::test]
async fn explicit_refresh_replaces_actor_facts_without_join_and_invalidates_old_display() {
    let orch = fixture().await;
    let consumer = register(&orch, RequestKind::Rebuild);
    let shown = file_rebuild(&orch, "run", "refresh candidate").await;
    let shown_facts = shown.facts.as_ref().unwrap();
    {
        let mut facts = consumer.facts.lock().unwrap();
        facts.main_commit = Some(commit('c'));
        facts.candidate_commit = Some(commit('c'));
        facts.running_version = Some("1.2.9".into());
    }
    let unchanged = get(&orch, &shown.id).await.unwrap().unwrap();
    assert_eq!(unchanged.revision, shown.revision);
    assert_eq!(unchanged.facts.as_ref().unwrap(), shown_facts);
    let refreshed = refresh(&orch, &shown.id).await.unwrap().unwrap();
    assert_eq!(refreshed.revision, shown.revision);
    assert!(refreshed.facts.as_ref().unwrap().version > shown_facts.version);
    assert_eq!(
        refreshed.facts.as_ref().unwrap().document["runningVersion"],
        "1.2.9"
    );
    assert!(accept_card(&orch, &shown)
        .await
        .unwrap_err()
        .contains("stale"));
    let accepted = accept_card(&orch, &refreshed).await.unwrap();
    assert_eq!(
        accepted.attempts[0].acceptance.candidate_commit.as_deref(),
        Some(commit('c').as_str())
    );
    consumer.facts.lock().unwrap().candidate_commit = Some(commit('a'));
    let still_accepted = refresh(&orch, &accepted.id).await.unwrap().unwrap();
    assert_eq!(still_accepted.state, RequestState::Accepted);
    assert_eq!(still_accepted.facts, accepted.facts);
    assert_eq!(still_accepted.attempts, accepted.attempts);
}

/// The production consumer, installed exactly as the runner installs it, driven
/// through the real request primitive and a real settings file.
mod production_lift_drain {
    use super::*;
    use crate::fleet::lift_drain::LiftDrainConsumer;
    use cairn_config::drain::{DrainOwner, LiftApplication};
    use cairn_config::fleet::{RemoteExecutorConfig, RemotePlatform};

    const URI: &str = "cairn://executors/machine-a";
    const ID: &str = "machine-id";

    fn human(subject: &str) -> PrincipalRef {
        PrincipalRef::Human {
            issuer: "test".into(),
            subject: subject.into(),
            organization: None,
        }
    }

    fn remote(name: &str) -> RemoteExecutorConfig {
        RemoteExecutorConfig {
            host: "machine-a.local".into(),
            ssh_user: "dev".into(),
            platform: RemotePlatform::LinuxX86_64,
            binary_path: "/home/dev/.local/bin/cairn-executor".into(),
            cairn_home: "/home/dev/.cairn-executor".into(),
            executor_id: ID.into(),
            device_id: "machine-device".into(),
            display_name: name.into(),
            project_ids: vec![],
            bootstrapped_at_unix_ms: None,
            legacy_retired_at_unix_ms: None,
            legacy_tunnel_port: None,
            extra_ssh_args: vec![],
        }
    }

    async fn installed(with_thread: bool) -> (Orchestrator, Arc<LiftDrainConsumer>) {
        let orch = fixture().await;
        let consumer = LiftDrainConsumer::new();
        orch.operator_requests.register(consumer.clone()).unwrap();
        crate::fleet::management::mutate_fleet(&orch, |config| {
            config
                .remote_executors
                .insert(ID.into(), remote("machine-a"));
            config
                .drains
                .impose_operator(ID, &human("operator"), "maintenance", 1)?;
            if with_thread {
                config
                    .drains
                    .impose_thread(ID, "thread-a", &human("thread"), "hold", 2)?;
            }
            Ok(())
        })
        .unwrap();
        (orch, consumer)
    }

    fn standing(orch: &Orchestrator) -> Vec<DrainOwner> {
        cairn_config::settings::read_fleet_strict(&orch.config_dir)
            .unwrap()
            .drains
            .records(ID)
            .into_iter()
            .map(|record| record.owner)
            .collect()
    }

    async fn file_lift(orch: &Orchestrator) -> OperatorRequest {
        file(
            orch,
            &callback("run"),
            input(RequestKind::LiftDrain, Some(URI), "resume"),
        )
        .await
        .unwrap()
        .request
    }

    async fn state(orch: &Orchestrator, id: &str) -> OperatorRequest {
        get(orch, id).await.unwrap().unwrap()
    }

    fn accepted_attempt(request: &OperatorRequest) -> &cairn_common::requests::RequestAttempt {
        let id = request
            .current_attempt
            .as_ref()
            .expect("current accepted attempt");
        let attempt = request
            .attempts
            .iter()
            .find(|attempt| &attempt.id == id)
            .unwrap();
        assert_eq!(attempt.state, RequestState::Accepted);
        attempt
    }

    /// The claim and effect a crashed runner would have left: claimed, with the
    /// config application committed but nothing reported.
    async fn claim(orch: &Orchestrator, accepted: &OperatorRequest) -> LiftApplication {
        let attempt = &accepted.attempts[0];
        let drain = attempt.acceptance.drain.clone().unwrap();
        cairn_db::storage::requests::claim_request(
            &orch.db.local,
            &attempt.authorization_event_id,
            &attempt.acceptance,
            "claim-crash",
            now(),
        )
        .await
        .unwrap();
        LiftApplication {
            install_id: attempt.acceptance.install_id.clone(),
            workspace_id: attempt.acceptance.workspace_id.clone(),
            authorization_event_id: attempt.authorization_event_id.clone(),
            attempt_id: attempt.acceptance.attempt_id.clone(),
            claim_id: "claim-crash".into(),
            actor: "execution.fleet".into(),
            kind: "lift_drain".into(),
            executor_id: drain.executor_id,
            drain_key: drain.drain_key,
            accepted_owner: DrainOwner::Operator,
            operator_generation: drain.generation,
            machine_revision: drain.machine_revision,
            report_id: format!("lift-drain:{}", attempt.acceptance.attempt_id),
        }
    }

    #[tokio::test]
    async fn the_installed_adapter_makes_lift_drain_live_and_clears_only_the_operator_record() {
        let (orch, _) = installed(true).await;
        assert!(orch
            .operator_requests
            .unavailable_reason("lift_drain")
            .is_none());
        let card = file_lift(&orch).await;
        let facts = card.facts.as_ref().unwrap();
        assert_eq!(
            facts.document["document"]["owners"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let accepted = accept_card(&orch, &card).await.unwrap();
        dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
            .await
            .unwrap();
        assert_eq!(
            standing(&orch),
            vec![DrainOwner::Thread {
                thread_id: "thread-a".into()
            }]
        );
        assert!(
            orch.fleet.is_drained(ID),
            "another owner still closes admission"
        );
        let done = state(&orch, &card.id).await;
        assert_eq!(done.state, RequestState::Done);
        let body = serde_json::to_string(&done).unwrap();
        assert!(
            body.contains("CLOSED") && body.contains("thread:thread-a"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn filing_without_an_operator_record_refuses_by_name() {
        let (orch, _) = installed(false).await;
        crate::fleet::management::mutate_fleet(&orch, |config| {
            config.drains.clear_thread(ID, "none").map(|_| ())?;
            let revision = config.drains.machine_revision(ID);
            assert!(revision > 0);
            Ok(())
        })
        .unwrap();
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
            .await
            .unwrap();
        let error = file(
            &orch,
            &callback("run"),
            input(RequestKind::LiftDrain, Some(URI), "again"),
        )
        .await
        .unwrap_err();
        assert!(error.contains("no operator-owned drain"), "{error}");
    }

    #[tokio::test]
    async fn another_owners_change_spends_the_attempt_and_clears_nothing() {
        let (orch, _) = installed(false).await;
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        crate::fleet::management::mutate_fleet(&orch, |config| {
            config
                .drains
                .impose_thread(ID, "thread-b", &human("thread"), "new hold", 5)
                .map(|_| ())
        })
        .unwrap();
        dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
            .await
            .unwrap();
        assert_eq!(standing(&orch).len(), 2);
        assert!(standing(&orch).contains(&DrainOwner::Operator));
        let reopened = state(&orch, &card.id).await;
        assert_eq!(reopened.state, RequestState::Open);
        let again = accept_card(&orch, &reopened).await.unwrap();
        assert_ne!(
            accepted_attempt(&again).acceptance.attempt_id,
            accepted.attempts[0].acceptance.attempt_id
        );
    }

    #[tokio::test]
    async fn accepting_a_stale_card_is_refused_before_any_attempt_exists() {
        let (orch, _) = installed(false).await;
        let card = file_lift(&orch).await;
        crate::fleet::management::mutate_fleet(&orch, |config| {
            config
                .drains
                .impose_thread(ID, "thread-b", &human("thread"), "new hold", 5)
                .map(|_| ())
        })
        .unwrap();
        let error = accept_card(&orch, &card).await.unwrap_err();
        assert!(error.contains("drain state changed"), "{error}");
        assert!(state(&orch, &card.id).await.attempts.is_empty());
    }

    #[tokio::test]
    async fn a_rename_after_acceptance_still_lifts_the_same_machine() {
        let (orch, _) = installed(false).await;
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        crate::fleet::management::mutate_fleet(&orch, |config| {
            config
                .remote_executors
                .insert(ID.into(), remote("machine-renamed"));
            Ok(())
        })
        .unwrap();
        dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
            .await
            .unwrap();
        assert!(standing(&orch).is_empty());
    }

    #[tokio::test]
    async fn crash_before_the_claim_leaves_the_drain_and_recovery_is_a_no_op() {
        let (orch, consumer) = installed(false).await;
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        consumer.recover(&orch).await.unwrap();
        assert_eq!(standing(&orch), vec![DrainOwner::Operator]);
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Accepted);
        dispatch_accepted(&orch, &accepted.attempts[0].authorization_event_id)
            .await
            .unwrap();
        assert!(standing(&orch).is_empty());
    }

    #[tokio::test]
    async fn crash_after_claim_without_a_receipt_settles_failed_and_clears_nothing() {
        let (orch, consumer) = installed(false).await;
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        claim(&orch, &accepted).await;
        consumer.recover(&orch).await.unwrap();
        assert_eq!(standing(&orch), vec![DrainOwner::Operator]);
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Open);
        accept_card(&orch, &state(&orch, &card.id).await)
            .await
            .expect("fresh acceptance after a no-effect settlement");
    }

    #[tokio::test]
    async fn crash_after_atomic_replacement_before_publication_reconciles_the_receipt() {
        let (orch, consumer) = installed(false).await;
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        let application = claim(&orch, &accepted).await;
        // Committed to disk, never published to the live projection.
        cairn_config::settings::mutate_fleet(
            &orch.config_dir,
            |config| config.drains.apply_lift(application.clone()),
            |_| {},
        )
        .unwrap();
        assert!(
            orch.fleet.is_drained(ID),
            "live state still shows the old drain"
        );
        consumer.recover(&orch).await.unwrap();
        assert!(
            !orch.fleet.is_drained(ID),
            "recovery published the current config"
        );
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Done);
    }

    #[tokio::test]
    async fn crash_after_publication_before_report_never_clears_a_newer_drain() {
        let (orch, consumer) = installed(false).await;
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        let application = claim(&orch, &accepted).await;
        crate::fleet::management::mutate_fleet(&orch, |config| {
            config.drains.apply_lift(application.clone()).map(|_| ())
        })
        .unwrap();
        // The operator drains the machine again before the runner recovers.
        crate::fleet::management::mutate_fleet(&orch, |config| {
            config
                .drains
                .impose_operator(ID, &human("operator"), "second maintenance", 9)
                .map(|_| ())
        })
        .unwrap();
        consumer.recover(&orch).await.unwrap();
        assert_eq!(standing(&orch), vec![DrainOwner::Operator]);
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Done);
        // Replaying recovery after the report is a no-op.
        consumer.recover(&orch).await.unwrap();
        assert_eq!(standing(&orch), vec![DrainOwner::Operator]);
    }

    #[tokio::test]
    async fn real_atomic_replace_failure_has_no_effect_and_requires_fresh_acceptance() {
        let (orch, consumer) = installed(false).await;
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        let application = claim(&orch, &accepted).await;
        let path = cairn_config::settings::get_settings_path(&orch.config_dir);
        let backup = orch.config_dir.join("settings.backup");
        let bytes = std::fs::read(&path).unwrap();
        let before = cairn_config::settings::read_fleet_strict(&orch.config_dir).unwrap();

        // Exercise the production config/publication boundary after a real claim,
        // with a valid read and in-memory clear, but a failing atomic replacement.
        let result = crate::fleet::management::mutate_fleet(&orch, |config| {
            let receipt = config.drains.apply_lift(application.clone())?;
            std::fs::rename(&path, &backup).unwrap();
            std::fs::create_dir(&path).unwrap();
            Ok(receipt)
        });
        std::fs::remove_dir(&path).unwrap();
        std::fs::rename(&backup, &path).unwrap();
        let error = result.unwrap_err();
        assert!(error.contains("Failed to replace settings file"), "{error}");
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert_eq!(
            cairn_config::settings::read_fleet_strict(&orch.config_dir)
                .unwrap()
                .drains,
            before.drains
        );
        assert!(orch.fleet.is_drained(ID), "failed clear must not publish");
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Acting);

        consumer.recover(&orch).await.unwrap();
        let reopened = state(&orch, &card.id).await;
        assert_eq!(reopened.state, RequestState::Open);
        assert_eq!(
            reopened.attempts[0].outcome,
            Some(RequestAttemptOutcome::Failed)
        );
        assert!(serde_json::to_string(&reopened.attempts[0].report)
            .unwrap()
            .contains("no_effect"));
        assert_eq!(standing(&orch), vec![DrainOwner::Operator]);
        assert!(cairn_config::settings::read_fleet_strict(&orch.config_dir)
            .unwrap()
            .drains
            .lift_receipts()
            .is_empty());
        assert!(
            dispatch_accepted(&orch, &application.authorization_event_id)
                .await
                .is_err()
        );

        let fresh = accept_card(&orch, &reopened).await.unwrap();
        let attempt = accepted_attempt(&fresh);
        assert_ne!(attempt.id, application.attempt_id);
        dispatch_accepted(&orch, &attempt.authorization_event_id)
            .await
            .unwrap();
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Done);
        let committed = cairn_config::settings::read_fleet_strict(&orch.config_dir).unwrap();
        assert!(committed.drains.records(ID).is_empty());
        assert_eq!(committed.drains.lift_receipts().len(), 1);
        assert!(!committed
            .drains
            .lift_receipts()
            .contains_key(&application.attempt_id));
        assert!(!orch.fleet.is_drained(ID));
    }

    #[tokio::test]
    async fn dispatch_cannot_claim_and_recovery_cannot_settle_while_shared_exclusion_is_held() {
        let (orch, consumer) = installed(false).await;
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        let gate = consumer.exclusion().await.unwrap();
        let event = &accepted.attempts[0].authorization_event_id;
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(100),
            dispatch_accepted(&orch, event)
        )
        .await
        .is_err());
        let waiting = state(&orch, &card.id).await;
        assert_eq!(waiting.state, RequestState::Accepted);
        assert!(waiting.attempts[0].claim_id.is_none());

        // Model an active effect after claim, still holding the same exclusion.
        claim(&orch, &accepted).await;
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(100),
            consumer.recover(&orch)
        )
        .await
        .is_err());
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Acting);
        assert_eq!(standing(&orch), vec![DrainOwner::Operator]);
        drop(gate);
        consumer.recover(&orch).await.unwrap();
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Open);
    }

    #[tokio::test]
    async fn a_report_failure_after_committed_clear_remains_recoverable_without_another_effect() {
        let (orch, consumer) = installed(true).await;
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        let attempt = accepted_attempt(&accepted);
        // Fail the real success-report transaction, but leave the primitive's
        // dispatch-error report writable so it cannot hide a terminal downgrade.
        orch.db
            .local
            .execute(
                "CREATE TRIGGER refuse_lift_report BEFORE INSERT ON request_report_receipts \
             WHEN NEW.id LIKE 'lift-drain:%' BEGIN SELECT RAISE(ABORT, 'report unavailable'); END",
                (),
            )
            .await
            .unwrap();

        let dispatched = dispatch_accepted(&orch, &attempt.authorization_event_id).await;
        let committed = cairn_config::settings::read_fleet_strict(&orch.config_dir).unwrap();
        let receipt = committed.drains.lift_receipts()[&attempt.id].clone();
        assert_eq!(receipt.outcome, cairn_config::drain::LiftOutcome::Cleared);
        assert_eq!(
            standing(&orch),
            vec![DrainOwner::Thread {
                thread_id: "thread-a".into()
            }]
        );
        assert_eq!(
            state(&orch, &card.id).await.state,
            RequestState::Acting,
            "a committed effect must remain reconcilable when reporting fails"
        );
        dispatched.unwrap();
        let unresolved = store::unresolved_claims(&orch.db.local, RequestKind::LiftDrain)
            .await
            .unwrap();
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].acceptance.attempt_id, attempt.id);
        assert_eq!(unresolved[0].state, RequestState::Acting);

        orch.db
            .local
            .execute("DROP TRIGGER refuse_lift_report", ())
            .await
            .unwrap();
        crate::fleet::management::mutate_fleet(&orch, |config| {
            config
                .drains
                .impose_operator(ID, &human("operator"), "replacement", 3)?;
            Ok(())
        })
        .unwrap();
        let replacement = cairn_config::settings::read_fleet_strict(&orch.config_dir)
            .unwrap()
            .drains;
        consumer.recover(&orch).await.unwrap();
        consumer.recover(&orch).await.unwrap();
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Done);
        let after = cairn_config::settings::read_fleet_strict(&orch.config_dir).unwrap();
        assert_eq!(
            after.drains, replacement,
            "recovery reports evidence, never repeats the clear"
        );
        assert_eq!(after.drains.lift_receipts()[&attempt.id], receipt);
        assert!(
            store::unresolved_claims(&orch.db.local, RequestKind::LiftDrain)
                .await
                .unwrap()
                .is_empty()
        );
        let reports = orch
            .db
            .local
            .query_all(
                "SELECT id FROM request_report_receipts WHERE attempt_id=?1".to_string(),
                (attempt.id.clone(),),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(reports, vec![receipt.application.report_id]);
        let outbox = store::pending_outbox(&orch.db.local, 200).await.unwrap();
        assert_eq!(
            outbox
                .iter()
                .filter(|item| item.request_id == card.id
                    && item.kind == "changed"
                    && item.payload["state"] == "done")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn duplicate_dispatch_report_and_recovery_create_one_receipt_and_one_terminal_outbox_transition(
    ) {
        let (orch, consumer) = installed(false).await;
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        let event = &accepted.attempts[0].authorization_event_id;
        dispatch_accepted(&orch, event).await.unwrap();
        let context = recover_claimed_request(
            &orch,
            event,
            &accepted.attempts[0].acceptance,
            consumer.clone(),
        )
        .await
        .unwrap();
        let receipt = cairn_config::settings::read_fleet_strict(&orch.config_dir)
            .unwrap()
            .drains
            .lift_receipts()[&accepted.attempts[0].id]
            .clone();
        let before = store::pending_outbox(&orch.db.local, 200).await.unwrap();
        let terminal_changes = before
            .iter()
            .filter(|item| {
                item.request_id == card.id
                    && item.kind == "changed"
                    && item.payload["state"] == "done"
            })
            .count();
        assert_eq!(terminal_changes, 1);
        let outbox_ids = before
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>();

        assert!(dispatch_accepted(&orch, event).await.is_err());
        context
            .report(
                &orch,
                &receipt.application.report_id,
                report(RequestAttemptOutcome::Done, true, true),
            )
            .await
            .unwrap();
        consumer.recover(&orch).await.unwrap();
        consumer.recover(&orch).await.unwrap();
        let after = store::pending_outbox(&orch.db.local, 200).await.unwrap();
        assert_eq!(
            after.iter().map(|item| item.id.clone()).collect::<Vec<_>>(),
            outbox_ids
        );
        let reports = orch
            .db
            .local
            .query_all(
                "SELECT id FROM request_report_receipts WHERE attempt_id=?1".to_string(),
                (accepted.attempts[0].id.clone(),),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(reports, vec![receipt.application.report_id.clone()]);
        let current = cairn_config::settings::read_fleet_strict(&orch.config_dir).unwrap();
        assert_eq!(current.drains.lift_receipts().len(), 1);
        assert_eq!(
            current.drains.lift_receipts()[&accepted.attempts[0].id],
            receipt
        );
        assert_eq!(current.drains.machine_revision(ID), receipt.after_revision);
        delivery::drain_once(&orch).await.unwrap();
        assert!(!dispatch_intent_pending(&orch, event).await);
        delivery::drain_once(&orch).await.unwrap();
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Done);
    }

    #[tokio::test]
    async fn same_second_reconstructed_runner_recovers_receipt_and_no_receipt_claims() {
        for committed in [false, true] {
            let (orch, consumer) = installed(false).await;
            let card = file_lift(&orch).await;
            let accepted = accept_card(&orch, &card).await.unwrap();
            let application = claim(&orch, &accepted).await;
            let claim_second =
                store::get_attempt_by_event(&orch.db.local, &application.authorization_event_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .updated_at;
            if committed {
                // Crash boundary: receipt persisted, publication/report absent.
                cairn_config::settings::mutate_fleet(
                    &orch.config_dir,
                    |config| config.drains.apply_lift(application.clone()),
                    |_| {},
                )
                .unwrap();
            }
            let db = orch.db.clone();
            let config_dir = orch.config_dir.clone();
            drop(consumer);
            drop(orch);
            let restarted = OrchestratorBuilder::new(
                db,
                Arc::new(TestServicesBuilder::new().build()),
                config_dir,
            )
            .boot_at(claim_second)
            .build();
            assert_eq!(restarted.boot_at, claim_second);
            let consumer = LiftDrainConsumer::new();
            restarted
                .operator_requests
                .register(consumer.clone())
                .unwrap();
            let persisted = store::get_attempt_by_event(
                &restarted.db.local,
                &application.authorization_event_id,
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(persisted.updated_at, restarted.boot_at);
            assert_eq!(persisted.state, RequestState::Acting);

            consumer.recover(&restarted).await.unwrap();
            let recovered = state(&restarted, &card.id).await;
            let expected = if committed {
                RequestState::Done
            } else {
                RequestState::Open
            };
            assert_eq!(recovered.state, expected);
            assert_eq!(restarted.fleet.is_drained(ID), !committed);
            let config = cairn_config::settings::read_fleet_strict(&restarted.config_dir).unwrap();
            assert_eq!(config.drains.lift_receipts().len(), usize::from(committed));
            if !committed {
                assert_eq!(standing(&restarted), vec![DrainOwner::Operator]);
                assert_eq!(
                    recovered.attempts[0].outcome,
                    Some(RequestAttemptOutcome::Failed)
                );
                let fresh = accept_card(&restarted, &recovered).await.unwrap();
                let attempt = accepted_attempt(&fresh);
                assert_ne!(attempt.id, application.attempt_id);
                dispatch_accepted(&restarted, &attempt.authorization_event_id)
                    .await
                    .unwrap();
                assert_eq!(state(&restarted, &card.id).await.state, RequestState::Done);
            }
            consumer.recover(&restarted).await.unwrap();
            assert_eq!(state(&restarted, &card.id).await.state, RequestState::Done);
        }
    }

    #[tokio::test]
    async fn missing_machine_revision_in_attempt_or_authorization_journal_refuses_before_claim() {
        for location in ["journal", "attempt", "valid_control"] {
            let (orch, _) = installed(false).await;
            let card = file_lift(&orch).await;
            let accepted = accept_card(&orch, &card).await.unwrap();
            let original = &accepted.attempts[0];
            let mut envelope = original.acceptance.clone();
            envelope.cohort_id = format!("copied-cohort:{location}");
            envelope.attempt_id = format!("copied-attempt:{location}");
            let request_id = format!("copied-request:{location}");
            envelope.request_ids = vec![request_id.clone()];
            let event = format!("copied-event:{location}");
            let suffix = format!(":{location}");
            let valid = serde_json::to_string(&envelope).unwrap();
            let mut missing = serde_json::to_value(&envelope).unwrap();
            missing["drain"]
                .as_object_mut()
                .unwrap()
                .remove("machine_revision");
            let error = serde_json::from_value::<RequestAcceptance>(missing.clone()).unwrap_err();
            assert!(error.to_string().contains("machine_revision"), "{error}");
            let path = cairn_config::settings::get_settings_path(&orch.config_dir);
            let bytes = std::fs::read(&path).unwrap();

            // History is append-only, including attempt inputs and membership.
            // Copy a complete fixture cohort with coherent new identities; the
            // valid control must dispatch through the very same seeded shape.
            orch.db.local.execute(
                "INSERT INTO request_cohorts(id,workspace_id,coalescing_key,kind,state,revision,created_at,updated_at) \
                 SELECT ?1,workspace_id,coalescing_key,kind,state,revision,created_at,updated_at FROM request_cohorts WHERE id=?2",
                (envelope.cohort_id.clone(), original.acceptance.cohort_id.clone()),
            ).await.unwrap();
            orch.db.local.execute(
                "INSERT INTO requests(id,workspace_id,kind,target,target_identity,coalescing_key,cohort_id,state,revision,created_at,updated_at,current_attempt) \
                 SELECT ?1,workspace_id,kind,target,target_identity,coalescing_key,?2,state,revision,created_at,updated_at,?3 FROM requests WHERE id=?4",
                (request_id.clone(), envelope.cohort_id.clone(), envelope.attempt_id.clone(), card.id.clone()),
            ).await.unwrap();
            orch.db
                .local
                .execute(
                    "INSERT INTO request_cohort_members(cohort_id,request_id) VALUES(?1,?2)",
                    (envelope.cohort_id.clone(), request_id.clone()),
                )
                .await
                .unwrap();
            orch.db.local.execute(
                "INSERT INTO request_facts(cohort_id,version,revision,schema_version,observed_at,document_json) \
                 SELECT ?1,version,revision,schema_version,observed_at,document_json FROM request_facts WHERE cohort_id=?2",
                (envelope.cohort_id.clone(), original.acceptance.cohort_id.clone()),
            ).await.unwrap();
            orch.db.local.execute(
                "INSERT INTO request_requesters(id,request_id,origin_uri,run_id,principal_json,wake_destination,rationale,requirements_json,minimum_target,normalized_target_json,filing_key,active,satisfied_by_attempt,created_at) \
                 SELECT id || ?1,?2,origin_uri,run_id,principal_json,wake_destination,rationale,requirements_json,minimum_target,normalized_target_json,filing_key,active,satisfied_by_attempt,created_at FROM request_requesters WHERE request_id=?3",
                (suffix.clone(), request_id.clone(), card.id.clone()),
            ).await.unwrap();
            orch.db.local.execute(
                "INSERT INTO request_waiting(contribution_id,uri) SELECT contribution_id || ?1,uri FROM request_waiting \
                 WHERE contribution_id IN (SELECT id FROM request_requesters WHERE request_id=?2)",
                (suffix.clone(), card.id.clone()),
            ).await.unwrap();
            let uri = format!("cairn://requests/{request_id}");
            let scope = cairn_common::authorization::AuthorityScope::new(
                cairn_common::authorization::AuthorityPlace::Resource {
                    canonical_uri: uri.clone(),
                },
                cairn_common::authorization::AuthorityAction::Run,
            );
            let journal_json = if location == "journal" {
                missing.to_string()
            } else {
                valid.clone()
            };
            orch.db.local.execute(
                "INSERT INTO authorization_events(id,scope_key,place_kind,action,scope_json,mutation,summary,outcome,reason,principal_json,audience_json,workspace_id,run_id,request_uri,grant_id,decided_at,actor_principal_json,appearance_snapshot_json,request_acceptance_json) \
                 SELECT ?1,?2,place_kind,action,?3,mutation,summary,outcome,reason,principal_json,audience_json,workspace_id,run_id,?4,grant_id,decided_at,actor_principal_json,appearance_snapshot_json,?5 FROM authorization_events WHERE id=?6",
                (event.clone(), scope.shorthand(), serde_json::to_string(&scope).unwrap(), uri, journal_json, original.authorization_event_id.clone()),
            ).await.unwrap();
            let attempt_json = if location == "attempt" {
                missing.to_string()
            } else {
                valid
            };
            orch.db.local.execute(
                "INSERT INTO request_attempts(id,cohort_id,authorization_event_id,acceptance_json,state,updated_at) VALUES(?1,?2,?3,?4,'accepted',?5)",
                (envelope.attempt_id.clone(), envelope.cohort_id.clone(), event.clone(), attempt_json, original.updated_at),
            ).await.unwrap();
            let mut contributions = accepted.requesters.clone();
            for contribution in &mut contributions {
                contribution.id.push_str(&suffix);
                contribution.request_id = request_id.clone();
            }
            orch.db.local.execute(
                "INSERT INTO request_attempt_members(attempt_id,request_id,contributions_json) VALUES(?1,?2,?3)",
                (envelope.attempt_id.clone(), request_id.clone(), serde_json::to_string(&contributions).unwrap()),
            ).await.unwrap();

            let result = dispatch_accepted(&orch, &event).await;
            if location == "valid_control" {
                result.unwrap();
                assert_eq!(state(&orch, &request_id).await.state, RequestState::Done);
                assert!(standing(&orch).is_empty());
                continue;
            }
            let error = result.unwrap_err();
            assert!(error.contains("machine_revision"), "{location}: {error}");
            let unclaimed = orch
                .db
                .local
                .query_one(
                    "SELECT state,claim_id FROM request_attempts WHERE id=?1".to_string(),
                    (envelope.attempt_id.clone(),),
                    |row| Ok((row.text(0)?, row.opt_text(1)?)),
                )
                .await
                .unwrap();
            assert_eq!(unclaimed, ("accepted".into(), None), "{location}");
            let unchanged =
                store::get_attempt_by_event(&orch.db.local, &original.authorization_event_id)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(unchanged.state, RequestState::Accepted);
            assert!(unchanged.claim_id.is_none());
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            assert_eq!(standing(&orch), vec![DrainOwner::Operator]);
            assert!(cairn_config::settings::read_fleet_strict(&orch.config_dir)
                .unwrap()
                .drains
                .lift_receipts()
                .is_empty());
            dispatch_accepted(&orch, &original.authorization_event_id)
                .await
                .unwrap();
            assert_eq!(state(&orch, &card.id).await.state, RequestState::Done);
        }
    }

    #[tokio::test]
    async fn production_dispatch_refuses_forged_identity_claim_and_every_frozen_drain_restriction()
    {
        let (orch, consumer) = installed(false).await;
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        let event = &accepted.attempts[0].authorization_event_id;
        assert!(
            recover_claimed_request(
                &orch,
                event,
                &accepted.attempts[0].acceptance,
                consumer.clone(),
            )
            .await
            .is_err(),
            "an accepted but unclaimed act is not authority"
        );
        claim(&orch, &accepted).await;
        let context = recover_claimed_request(
            &orch,
            event,
            &accepted.attempts[0].acceptance,
            consumer.clone(),
        )
        .await
        .unwrap();
        context.recheck(&orch).await.unwrap();
        let path = cairn_config::settings::get_settings_path(&orch.config_dir);
        let bytes = std::fs::read(&path).unwrap();
        for field in [
            "event",
            "claim",
            "install",
            "workspace",
            "actor",
            "kind",
            "target",
            "identity",
            "executor",
            "key",
            "owner",
            "generation",
            "revision",
        ] {
            let mut forged = context.clone();
            match field {
                "event" => forged.event_id = "no-acceptance".into(),
                "claim" => forged.claim_id = "another-claim".into(),
                "install" => forged.acceptance.install_id = "another-install".into(),
                "workspace" => forged.acceptance.workspace_id = "another-workspace".into(),
                "actor" => forged.acceptance.actor_id = "another.actor".into(),
                "kind" => forged.acceptance.kind = RequestKind::Rebuild,
                "target" => forged.acceptance.target = Some("cairn://executors/other".into()),
                "identity" => forged.acceptance.target_identity = "other".into(),
                "executor" => {
                    forged.acceptance.drain.as_mut().unwrap().executor_id = "other".into()
                }
                "key" => forged.acceptance.drain.as_mut().unwrap().drain_key = "other".into(),
                "owner" => forged.acceptance.drain.as_mut().unwrap().owner = "thread:a".into(),
                "generation" => forged.acceptance.drain.as_mut().unwrap().generation += 1,
                "revision" => forged.acceptance.drain.as_mut().unwrap().machine_revision += 1,
                _ => unreachable!(),
            }
            assert!(consumer.dispatch(&orch, &forged).await.is_err(), "{field}");
            assert_eq!(std::fs::read(&path).unwrap(), bytes, "{field}");
            assert_eq!(
                state(&orch, &card.id).await.state,
                RequestState::Acting,
                "{field}"
            );
        }
        assert!(
            context
                .report(
                    &orch,
                    "forged-success",
                    report(RequestAttemptOutcome::Done, true, true),
                )
                .await
                .is_err(),
            "caller JSON cannot replace a missing config receipt"
        );
        assert_eq!(standing(&orch), vec![DrainOwner::Operator]);
        assert!(cairn_config::settings::read_fleet_strict(&orch.config_dir)
            .unwrap()
            .drains
            .lift_receipts()
            .is_empty());
        consumer.recover(&orch).await.unwrap();
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Open);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_cannot_settle_a_real_dispatch_paused_after_claim_before_effect() {
        let (orch, consumer) = installed(false).await;
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let config_dir = orch.config_dir.clone();
        let blocker = tokio::task::spawn_blocking(move || {
            cairn_config::settings::mutate_fleet(
                &config_dir,
                |_| {
                    entered_tx.send(()).unwrap();
                    release_rx
                        .recv_timeout(std::time::Duration::from_secs(15))
                        .unwrap();
                    Ok(())
                },
                |_| {},
            )
        });
        entered_rx.await.unwrap();
        let dispatched_orch = orch.clone();
        let event = accepted.attempts[0].authorization_event_id.clone();
        let dispatch =
            tokio::spawn(async move { dispatch_accepted(&dispatched_orch, &event).await });
        let acting = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if state(&orch, &card.id).await.state == RequestState::Acting {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        let recovery = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            consumer.recover(&orch),
        )
        .await;
        let during = state(&orch, &card.id).await;
        let still_drained = orch.fleet.is_drained(ID);
        release_tx.send(()).unwrap();
        blocker.await.unwrap().unwrap();
        dispatch.await.unwrap().unwrap();
        assert!(
            acting.is_ok(),
            "the real dispatch must reach its durable claim"
        );
        assert!(
            recovery.is_err(),
            "recovery cannot settle while the effect is in flight"
        );
        assert_eq!(during.state, RequestState::Acting);
        assert!(still_drained);
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Done);
        consumer.recover(&orch).await.unwrap();
        assert!(!orch.fleet.is_drained(ID));
        let reports = orch
            .db
            .local
            .query_all(
                "SELECT id FROM request_report_receipts WHERE attempt_id=?1".to_string(),
                (accepted.attempts[0].id.clone(),),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(reports.len(), 1);
    }

    #[tokio::test]
    async fn an_unreadable_settings_document_retains_the_claim_until_it_can_be_read() {
        let (orch, consumer) = installed(false).await;
        let card = file_lift(&orch).await;
        let accepted = accept_card(&orch, &card).await.unwrap();
        claim(&orch, &accepted).await;
        let path = cairn_config::settings::get_settings_path(&orch.config_dir);
        let good = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, "buildSlots: [").unwrap();
        assert!(consumer.recover(&orch).await.is_err());
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Acting);
        std::fs::write(&path, good).unwrap();
        consumer.recover(&orch).await.unwrap();
        assert_eq!(state(&orch, &card.id).await.state, RequestState::Open);
        assert_eq!(standing(&orch), vec![DrainOwner::Operator]);
    }
}
