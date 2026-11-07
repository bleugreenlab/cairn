//! Install-local operator requests: durable intent, not standing permission.
//!
//! The transport proves the operator; this service freezes the displayed act;
//! storage commits that acceptance and its authority journal together. Runner
//! components alone hold checked attempt references. Production adapters are
//! intentionally absent until their physical enforcement is implemented.

pub(crate) mod delivery;
pub mod registry;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use cairn_common::authorization::ActBinding;
use cairn_common::identity::{AppearanceSnapshot, AppearanceTransport, PrincipalRef};
use cairn_common::protocol::CallbackRequest;
use cairn_common::requests::*;
use cairn_common::uri::{parse_uri, CairnResource};
use cairn_db::storage::requests as store;
use serde_json::{json, Value};

use crate::orchestrator::Orchestrator;
use crate::security::Sanitizer;
use crate::storage::RowExt;
use registry::{ActorFacts, ConsumerReport, ReleaseVersion, RequestConsumer};

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn scrub_text(mut value: String) -> String {
    Sanitizer::exact().text_in_place(&mut value);
    value
}

fn scrub_document(mut value: Value) -> Value {
    Sanitizer::exact().json(&mut value);
    value
}

async fn decorate(orch: &Orchestrator, mut request: OperatorRequest) -> OperatorRequest {
    let available = consumer(orch, request.kind).await;
    request.unavailable_reason = match available {
        Err(error) => Some(error),
        Ok(adapter) => {
            let stale = request.state == RequestState::Open
                && request.facts.as_ref().is_none_or(|facts| {
                    facts.revision != request.revision || facts.schema_version != 1
                });
            let refresh_error = if stale {
                match refresh_facts_for(orch, &request, adapter.as_ref()).await {
                    Ok(()) => match store::get_request(&orch.db.local, &request.id).await {
                        Ok(Some(refreshed)) => {
                            request = refreshed;
                            None
                        }
                        Ok(None) => Some("Request disappeared while refreshing actor facts".into()),
                        Err(error) => Some(format!("Request facts could not be read: {error}")),
                    },
                    Err(error) => Some(format!("Actor facts are unavailable: {error}")),
                }
            } else {
                None
            };
            if refresh_error.is_some() {
                refresh_error
            } else if request.kind == RequestKind::BumpVersion
                && request.state == RequestState::Open
            {
                let project = request
                    .facts
                    .as_ref()
                    .and_then(|facts| facts.document.get("policyProjectId"))
                    .and_then(Value::as_str);
                bump_policy(orch, project).await.err()
            } else {
                None
            }
        }
    };
    request.unavailable_reason = request.unavailable_reason.map(scrub_text);
    request.available = request.unavailable_reason.is_none();
    request
}

pub async fn list(
    orch: &Orchestrator,
    filter: RequestFilter,
) -> Result<Vec<OperatorRequest>, String> {
    let requests =
        store::list_requests(&orch.db.local, crate::authorization::WORKSPACE_ID, &filter)
            .await
            .map_err(|error| error.to_string())?;
    let mut out = Vec::with_capacity(requests.len());
    for request in requests {
        out.push(decorate(orch, request).await);
    }
    Ok(out)
}

pub async fn get(orch: &Orchestrator, id: &str) -> Result<Option<OperatorRequest>, String> {
    match store::get_request(&orch.db.local, id)
        .await
        .map_err(|error| error.to_string())?
    {
        Some(request) => Ok(Some(decorate(orch, request).await)),
        None => Ok(None),
    }
}

/// Refresh asks the registered runner provider for new evidence. It accepts no
/// facts document from the caller, and ordinary reads do not continuously
/// publish new facts versions in response to their own invalidation events.
pub async fn refresh(orch: &Orchestrator, id: &str) -> Result<Option<OperatorRequest>, String> {
    if let Some(request) = store::get_request(&orch.db.local, id)
        .await
        .map_err(|error| error.to_string())?
    {
        if request.state == RequestState::Open {
            let adapter = consumer(orch, request.kind).await?;
            refresh_facts_for(orch, &request, adapter.as_ref()).await?;
        }
    }
    get(orch, id).await
}

async fn require_request(orch: &Orchestrator, id: &str) -> Result<OperatorRequest, String> {
    get(orch, id)
        .await?
        .ok_or_else(|| format!("Operator request {id} does not exist on this install"))
}

async fn consumer(
    orch: &Orchestrator,
    kind: RequestKind,
) -> Result<Arc<dyn RequestConsumer>, String> {
    let adapter = orch.operator_requests.consumer(kind.as_str())?;
    adapter.availability(orch).await.map_err(scrub_text)?;
    Ok(adapter)
}

/// Attribution is always resolved from a live run and its structural owners.
/// An issue's owning thread receives attention; a parentless issue remains an
/// explicit issue destination instead of disappearing.
async fn origin(
    orch: &Orchestrator,
    request: &CallbackRequest,
) -> Result<(String, String, String, PrincipalRef), String> {
    if request.row_write_origin().is_some() {
        return Err("Operator requests must be filed by an authenticated issue or thread run, not a page or service".into());
    }
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request).await?;
    let service_home = match parse_uri(&agent.home) {
        Some(CairnResource::ProjectService { .. }) => true,
        Some(CairnResource::Thread { path, .. }) => {
            path.first().is_some_and(|part| part == "services")
        }
        _ => false,
    };
    if service_home {
        return Err("Services cannot file operator requests as a thread".into());
    }
    let (ctx, db) = crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request).await?;
    if matches!(ctx.agent_config_id.as_deref(), Some("service" | "action")) {
        return Err("Services cannot file operator requests as a thread".into());
    }
    let job_id = agent.job_id.clone();
    let owners = db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "WITH RECURSIVE jobs_up(id,parent_job_id,issue_id,thread_id,depth) AS (
                    SELECT id,parent_job_id,issue_id,thread_id,0 FROM jobs WHERE id=?1
                    UNION ALL
                    SELECT j.id,j.parent_job_id,j.issue_id,j.thread_id,u.depth+1
                    FROM jobs j JOIN jobs_up u ON j.id=u.parent_job_id WHERE u.depth<100
                 ), issues_up(id,parent_issue_id,parent_thread_id,number,project_id,depth) AS (
                    SELECT id,parent_issue_id,parent_thread_id,number,project_id,0 FROM issues
                    WHERE id=(SELECT issue_id FROM jobs_up WHERE issue_id IS NOT NULL ORDER BY depth LIMIT 1)
                    UNION ALL
                    SELECT i.id,i.parent_issue_id,i.parent_thread_id,i.number,i.project_id,u.depth+1
                    FROM issues i JOIN issues_up u ON i.id=u.parent_issue_id WHERE u.depth<100
                 )
                 SELECT
                   (SELECT 'cairn://p/'||LOWER(p.key)||'/'||i.number
                    FROM issues_up i JOIN projects p ON p.id=i.project_id ORDER BY i.depth LIMIT 1),
                   (SELECT 'cairn://p/'||LOWER(p.key)||'/'||t.name
                    FROM threads t JOIN projects p ON p.id=t.project_id
                    WHERE t.id=COALESCE(
                      (SELECT thread_id FROM jobs_up WHERE thread_id IS NOT NULL ORDER BY depth LIMIT 1),
                      (SELECT parent_thread_id FROM issues_up WHERE parent_thread_id IS NOT NULL ORDER BY depth LIMIT 1)))",
                (job_id.as_str(),),
            ).await?;
            let row = rows.next().await?.ok_or_else(|| crate::storage::DbError::Row("Request origin is unavailable".into()))?;
            Ok((row.opt_text(0)?, row.opt_text(1)?))
        })
    }).await.map_err(|error| error.to_string())?;
    let (issue, thread): (Option<String>, Option<String>) = owners;
    let origin = issue.clone().or_else(|| thread.clone()).ok_or_else(|| {
        "Only an authenticated issue or thread run can file an operator request".to_string()
    })?;
    let destination = thread
        .or(issue)
        .expect("origin above proves a destination exists");
    let (principal, _) = agent.appearance(now().saturating_mul(1000))?;
    Ok((origin, destination, agent.run_id, principal))
}

fn waiting_uris(waiting: Vec<String>) -> Result<Vec<String>, String> {
    if waiting.len() > 100 {
        return Err("At most 100 waiting issue/thread URIs may be filed at once".into());
    }
    let mut canonical = Vec::new();
    for uri in waiting {
        let resource = parse_uri(&uri).ok_or_else(|| {
            format!("Invalid waiting URI {uri}; use a canonical issue or thread URI")
        })?;
        let allowed = match &resource {
            CairnResource::Issue { .. } => true,
            CairnResource::Thread { path, .. } => path.is_empty(),
            _ => false,
        };
        if !allowed {
            return Err(format!(
                "Waiting URI {uri} must name a canonical issue or thread"
            ));
        }
        if resource.to_uri() != uri {
            return Err(format!(
                "Waiting URI must be canonical: {}",
                resource.to_uri()
            ));
        }
        canonical.push(uri);
    }
    canonical.sort();
    canonical.dedup();
    Ok(canonical)
}

fn commit_id(commit: &str) -> bool {
    matches!(commit.len(), 40 | 64)
        && commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn required_text<'a>(value: &'a Option<String>, name: &str) -> Result<&'a str, String> {
    value
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("Actor facts are unavailable: missing {name}"))
}

async fn bump_policy(orch: &Orchestrator, project_id: Option<&str>) -> Result<(), String> {
    let project_id = project_id.filter(|id| !id.trim().is_empty())
        .ok_or_else(|| "bump_version is unavailable: the canonical target project's human_base_merge_policy cannot be identified".to_string())?;
    let db = crate::execution::routing::routing_db_for_id(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let policy =
        crate::projects::human_base_merge_policy::read_policy(&db, None, project_id).await?;
    if policy.state != "disabled" {
        return Err(format!("bump_version refuses human_base_merge_policy={}: desktop acceptance cannot supply provably-human base-merge approval. Use the project's approved human landing path, or wait for a stronger approval adapter; this request cannot bypass that policy.", policy.state));
    }
    Ok(())
}

async fn identical_open_filing(
    orch: &Orchestrator,
    kind: RequestKind,
    origin: &str,
    install: &str,
    rationale: &str,
    filing: &Value,
    waiting: &[String],
) -> Result<Option<String>, String> {
    let mut offset = 0;
    loop {
        let cards = store::list_requests(
            &orch.db.local,
            crate::authorization::WORKSPACE_ID,
            &RequestFilter {
                status: Some(RequestState::Open),
                kind: Some(kind),
                requester: Some(origin.into()),
                limit: Some(200),
                offset: Some(offset),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| error.to_string())?;
        let count = cards.len();
        for card in cards {
            if card.target_identity != install {
                continue;
            }
            for contribution in card.requesters {
                if contribution.active
                    && contribution.origin_uri == origin
                    && card.member_request_ids.contains(&contribution.request_id)
                    && contribution.rationale == rationale
                    && contribution.waiting.as_slice() == waiting
                    && contribution.normalized_target.get("filing") == Some(filing)
                {
                    // This read is the linearization point of an unchanged
                    // redelivery. A concurrent acceptance cannot gain any new
                    // obligation from it, and no later write reopens the act.
                    return Ok(Some(contribution.request_id));
                }
            }
        }
        if count < 200 {
            return Ok(None);
        }
        offset += 200;
    }
}

pub async fn file(
    orch: &Orchestrator,
    request: &CallbackRequest,
    input: FileOperatorRequest,
) -> Result<FiledOperatorRequest, String> {
    let adapter = consumer(orch, input.kind).await?;
    let (origin_uri, wake_destination, run_id, principal) = origin(orch, request).await?;
    if input.rationale.trim().is_empty() || input.rationale.len() > 65_536 {
        return Err(
            "An operator request requires a nonblank rationale of at most 65536 bytes".into(),
        );
    }
    let waiting = waiting_uris(input.waiting)?;
    let facts = adapter.facts(orch, None).await?;
    let install = orch.anon_device_manager.device_id();
    let mut explicit_requirements = input.requirements.clone();
    explicit_requirements.commits.sort();
    explicit_requirements.commits.dedup();
    let filing = json!({"target":input.target,"requirements":explicit_requirements});
    let rationale = scrub_text(input.rationale);
    if input.kind == RequestKind::BumpVersion {
        bump_policy(orch, facts.policy_project_id.as_deref()).await?;
    }
    if matches!(input.kind, RequestKind::Rebuild | RequestKind::BumpVersion) {
        if let Some(id) = identical_open_filing(
            orch,
            input.kind,
            &origin_uri,
            &install,
            &rationale,
            &filing,
            &waiting,
        )
        .await?
        {
            return Ok(FiledOperatorRequest {
                request: require_request(orch, &id).await?,
                created: false,
            });
        }
    }
    let mut requirements = input.requirements;
    let (target, identity, key, minimum, mut normalized) = match input.kind {
        RequestKind::Rebuild => {
            if input.target.is_some() {
                return Err(
                    "rebuild has no target; use requirements.commits for verification".into(),
                );
            }
            if requirements.commits.is_empty() {
                requirements
                    .commits
                    .push(required_text(&facts.main_commit, "main commit")?.to_string());
            }
            if requirements.commits.len() > 100
                || !requirements.commits.iter().all(|commit| commit_id(commit))
            {
                return Err("rebuild requirements.commits must contain at most 100 full lowercase commit ids already reachable on main".into());
            }
            requirements.commits.sort();
            requirements.commits.dedup();
            let main = required_text(&facts.main_commit, "main commit")?;
            adapter
                .contains_commits(orch, main, &requirements.commits)
                .await
                .map_err(|error| {
                    format!(
                        "rebuild refuses unlanded requirements [{}]: {error}",
                        requirements.commits.join(", ")
                    )
                })?;
            (
                None,
                install.clone(),
                format!("rebuild:{install}"),
                None,
                json!({"commits":requirements.commits}),
            )
        }
        RequestKind::BumpVersion => {
            if !requirements.commits.is_empty() {
                return Err("requirements.commits is supported only for rebuild".into());
            }
            bump_policy(orch, facts.policy_project_id.as_deref()).await?;
            let requested = input
                .target
                .as_deref()
                .ok_or("bump_version requires sm, md, lg or plain X.Y.Z")?;
            let running =
                ReleaseVersion::parse(required_text(&facts.running_version, "running version")?)?;
            let minimum = running.resolve(requested)?.to_string();
            (
                Some(minimum.clone()),
                install.clone(),
                format!("bump_version:{install}"),
                Some(minimum.clone()),
                json!({"requestedTarget":requested,"minimumVersion":minimum,"runningVersion":running.to_string(),"policyProjectId":facts.policy_project_id}),
            )
        }
        RequestKind::LiftDrain => {
            if !requirements.commits.is_empty() {
                return Err("requirements.commits is supported only for rebuild".into());
            }
            let uri = input
                .target
                .as_deref()
                .ok_or("lift_drain requires a canonical public executor URI")?;
            let parsed =
                parse_uri(uri).ok_or("lift_drain requires a canonical public executor URI")?;
            if !matches!(&parsed, CairnResource::Executor { .. }) || parsed.to_uri() != uri {
                return Err("lift_drain requires a canonical public executor URI".into());
            }
            let resolved = adapter.executor_target(orch, uri).await?;
            if resolved.uri != uri
                || resolved.executor_id.is_empty()
                || cairn_config::fleet::FleetConfig::drain_key(&resolved.executor_id)
                    != resolved.drain_key
            {
                return Err("lift_drain target must resolve to the canonical stable executor identity and FleetConfig drain key".into());
            }
            (
                Some(resolved.uri.clone()),
                resolved.executor_id.clone(),
                format!("lift_drain:{}", resolved.executor_id),
                None,
                json!({"executorUri":resolved.uri,"executorId":resolved.executor_id,"drainKey":resolved.drain_key}),
            )
        }
    };
    // Filing identity describes what was asked, not the moving source state
    // that resolved its fixed minimum. A redelivery must preserve that minimum.
    normalized["filing"] = filing;
    let contribution = RequestContribution {
        id: String::new(),
        request_id: String::new(),
        origin_uri,
        run_id,
        principal,
        wake_destination,
        rationale,
        waiting,
        requirements,
        minimum_target: minimum,
        normalized_target: normalized,
        active: true,
        satisfied_by_attempt: None,
        created_at: 0,
    };
    let mut filed = store::file_request(
        &orch.db.local,
        &store::NormalizedRequest {
            workspace_id: crate::authorization::WORKSPACE_ID.into(),
            kind: input.kind,
            target,
            target_identity: identity,
            coalescing_key: Some(key),
            contribution,
        },
        now(),
    )
    .await
    .map_err(|error| error.to_string())?;
    // A concurrent join invalidates this refresh rather than letting older
    // actor facts describe the newer card.
    filed.request = require_request(orch, &filed.request.id).await?;
    Ok(filed)
}

pub async fn withdraw(
    orch: &Orchestrator,
    request: &CallbackRequest,
    id: &str,
) -> Result<OperatorRequest, String> {
    let (origin, _, _, _) = origin(orch, request).await?;
    store::withdraw_request(&orch.db.local, id, &origin, now())
        .await
        .map_err(|error| error.to_string())?;
    require_request(orch, id).await
}

async fn refresh_facts_for(
    orch: &Orchestrator,
    request: &OperatorRequest,
    adapter: &dyn RequestConsumer,
) -> Result<(), String> {
    let facts = adapter.facts(orch, Some(&request.id)).await?;
    let document = scrub_document(serde_json::to_value(facts).map_err(|error| error.to_string())?);
    store::refresh_facts(
        &orch.db.local,
        &request.id,
        request.revision,
        1,
        document,
        now(),
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// Only runner code with a registered facts provider can publish actor facts.
#[allow(dead_code)] // Runner adapter API; production providers land with their consumers.
pub(crate) async fn refresh_facts(orch: &Orchestrator, id: &str) -> Result<(), String> {
    let request = store::get_request(&orch.db.local, id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or("Operator request does not exist")?;
    let adapter = consumer(orch, request.kind).await?;
    refresh_facts_for(orch, &request, adapter.as_ref()).await
}

/// This is defense in depth, not a credential verifier. Only the invoke
/// boundary can establish operator proof; a snapshot is immutable attribution,
/// never a bearer granting authority.
fn validate_operator(
    principal: &PrincipalRef,
    appearance: &AppearanceSnapshot,
) -> Result<(), String> {
    appearance.validate().map_err(|error| error.to_string())?;
    if appearance.principal() != principal
        || !matches!(
            appearance.evidence().transport,
            AppearanceTransport::AuthenticatedOperator | AppearanceTransport::AuthenticatedDesktop
        )
    {
        return Err("Operator request decisions require proven operator attribution".into());
    }
    Ok(())
}

pub async fn accept(
    orch: &Orchestrator,
    id: &str,
    revision: i64,
    facts_version: i64,
    principal: &PrincipalRef,
    appearance: &AppearanceSnapshot,
) -> Result<OperatorRequest, String> {
    let request = require_request(orch, id).await?;
    match registry::definition(request.kind.as_str())?.acceptance_policy {
        registry::AcceptancePolicy::OperatorCredential => validate_operator(principal, appearance)?,
    }
    let adapter = consumer(orch, request.kind).await?;
    if request.state != RequestState::Open {
        return Err("Only an open operator request can be accepted".into());
    }
    let shown = request
        .facts
        .as_ref()
        .ok_or("Actor facts are unavailable; refresh before accepting")?;
    if revision != request.revision || facts_version != shown.version || shown.revision != revision
    {
        return Err("Operator request view is stale; refresh the card before accepting".into());
    }
    if shown.schema_version != 1 {
        return Err("Actor facts schema is unsupported; refresh before accepting".into());
    }
    let facts: ActorFacts = serde_json::from_value(shown.document.clone())
        .map_err(|error| format!("Actor facts are unreadable: {error}"))?;
    let mut ids = request.member_request_ids.clone();
    ids.sort();
    ids.dedup();
    let envelope = RequestAcceptance {
        schema_version: 1,
        install_id: orch.anon_device_manager.device_id(),
        workspace_id: request.workspace_id.clone(),
        cohort_id: request.cohort_id.clone(),
        request_ids: ids,
        attempt_id: uuid::Uuid::new_v4().to_string(),
        actor_id: registry::definition(request.kind.as_str())?.actor.into(),
        kind: request.kind,
        target: request.target.clone(),
        target_identity: request.target_identity.clone(),
        binding: ActBinding::fresh(&registry::act_digest(
            request.kind,
            request.target.as_deref(),
        )),
        facts_version,
        revision,
        candidate_commit: facts.candidate_commit.clone(),
        base_commit: facts.base_commit.clone(),
        policy_project_id: facts.policy_project_id.clone(),
        drain: facts.drain.clone(),
        operator: principal.clone(),
        appearance: appearance.clone(),
        accepted_at: now(),
    };
    check_snapshot(orch, &request, &facts, &envelope, adapter.as_ref()).await?;
    store::accept_request(&orch.db.local, &envelope)
        .await
        .map_err(|error| error.to_string())?;
    require_request(orch, id).await
}

async fn check_snapshot(
    orch: &Orchestrator,
    request: &OperatorRequest,
    facts: &ActorFacts,
    acceptance: &RequestAcceptance,
    adapter: &dyn RequestConsumer,
) -> Result<(), String> {
    match request.kind {
        RequestKind::Rebuild => {
            let commit = required_text(&facts.candidate_commit, "displayed candidate commit")?;
            if !commit_id(commit) {
                return Err("Displayed rebuild candidate is not a full commit id".into());
            }
            let commits = request
                .requesters
                .iter()
                .filter(|entry| {
                    entry.active
                        && entry.satisfied_by_attempt.is_none()
                        && request.member_request_ids.contains(&entry.request_id)
                })
                .flat_map(|entry| entry.requirements.commits.iter().cloned())
                .collect::<Vec<_>>();
            adapter.contains_commits(orch, commit, &commits).await?;
        }
        RequestKind::BumpVersion => {
            bump_policy(orch, acceptance.policy_project_id.as_deref()).await?;
            let current = adapter.facts(orch, Some(&request.id)).await?;
            let running =
                ReleaseVersion::parse(required_text(&current.running_version, "running version")?)?;
            let effective =
                ReleaseVersion::parse(required_text(&request.target, "effective target")?)?;
            if effective <= running {
                return Err(format!("bump_version target {effective} is stale against running {running}; refresh facts and explicitly refresh the request. No implicit extra bump is authorized."));
            }
            let base = required_text(&facts.base_commit, "displayed base commit")?;
            if !commit_id(base) {
                return Err("Displayed bump base is not a full commit id".into());
            }
        }
        RequestKind::LiftDrain => {
            if acceptance.drain.is_none() {
                return Err("lift_drain requires the displayed exact owner and generation".into());
            }
        }
    }
    adapter.validate_snapshot(orch, acceptance).await
}

pub async fn decline(
    orch: &Orchestrator,
    id: &str,
    revision: i64,
    reason: &str,
    principal: &PrincipalRef,
    appearance: &AppearanceSnapshot,
) -> Result<OperatorRequest, String> {
    validate_operator(principal, appearance)?;
    if reason.trim().is_empty() {
        return Err("Declining an operator request requires a nonblank reason".into());
    }
    store::decline_request(
        &orch.db.local,
        id,
        revision,
        &scrub_text(reason.into()),
        principal,
        appearance,
        now(),
    )
    .await
    .map_err(|error| error.to_string())?;
    require_request(orch, id).await
}

/// A runner-owned checked reference, deliberately neither serializable nor
/// constructible outside this module. Rechecking is required before each live
/// privileged operation; owning this value does not freeze live policy.
#[derive(Clone)]
pub(crate) struct AcceptedRequestContext {
    event_id: String,
    claim_id: String,
    acceptance: RequestAcceptance,
    adapter: Arc<dyn RequestConsumer>,
}

impl AcceptedRequestContext {
    #[allow(dead_code)] // Runner adapter API, not an agent resource.
    pub(crate) fn acceptance(&self) -> &RequestAcceptance {
        &self.acceptance
    }

    pub(crate) async fn recheck(&self, orch: &Orchestrator) -> Result<(), String> {
        self.recheck_for(orch, &self.acceptance).await
    }

    /// Effect adapters compare their intended operation with the complete
    /// frozen envelope before touching a live privileged boundary. Comparing
    /// the whole typed record forbids silently dropping an optional restriction.
    pub(crate) async fn recheck_for(
        &self,
        orch: &Orchestrator,
        intended: &RequestAcceptance,
    ) -> Result<(), String> {
        if intended != &self.acceptance {
            return Err(
                "Intended privileged act differs from the frozen request acceptance".into(),
            );
        }
        if self.acceptance.install_id != orch.anon_device_manager.device_id() {
            return Err("Request acceptance belongs to another install".into());
        }
        if !self.acceptance.binding.names(&registry::act_digest(
            self.acceptance.kind,
            self.acceptance.target.as_deref(),
        )) {
            return Err("Request acceptance act binding does not match".into());
        }
        store::recheck_request(
            &orch.db.local,
            &self.event_id,
            &self.acceptance,
            &self.claim_id,
        )
        .await
        .map_err(|error| error.to_string())?;
        if self.acceptance.kind == RequestKind::BumpVersion {
            bump_policy(orch, self.acceptance.policy_project_id.as_deref()).await?;
        }
        self.adapter.availability(orch).await?;
        self.adapter.validate_snapshot(orch, &self.acceptance).await
    }

    #[allow(dead_code)] // Physical consumers supply decisive evidence through this API.
    pub(crate) async fn report(
        &self,
        orch: &Orchestrator,
        receipt_id: &str,
        report: ConsumerReport,
    ) -> Result<(), String> {
        // Reporting is effect-free reconciliation, not permission to keep
        // acting. A failed-but-unsettled actor must still be able to report
        // positive settlement after its live-operation context is fenced.
        let mut verified = self.adapter.verify_report(orch, self, report).await?;
        verified.document = scrub_document(
            json!({"evidence":verified.document,"builtCommit":verified.built_commit}),
        );
        let prior_coverage = store::report_receipt_coverage(
            &orch.db.local,
            &self.acceptance.attempt_id,
            &self.claim_id,
            receipt_id,
            &verified.outcome,
            &verified.document,
            verified.settled,
        )
        .await
        .map_err(|error| error.to_string())?;
        let replay = prior_coverage.is_some();
        let mut covered = prior_coverage.unwrap_or_default();
        if !replay && verified.outcome == RequestAttemptOutcome::Done {
            if !verified.settled {
                return Err("Success requires positive actor settlement".into());
            }
            match self.acceptance.kind {
                RequestKind::Rebuild => {
                    if verified.built_commit.as_ref() != self.acceptance.candidate_commit.as_ref() {
                        return Err("Successful rebuild evidence does not match the accepted candidate commit".into());
                    }
                }
                RequestKind::BumpVersion => {
                    let built =
                        required_text(&verified.built_commit, "verified built bump commit")?;
                    if !commit_id(built) {
                        return Err("Verified bump commit must be a full commit id".into());
                    }
                    let mut offset = 0;
                    loop {
                        let requests = store::list_requests(
                            &orch.db.local,
                            &self.acceptance.workspace_id,
                            &RequestFilter {
                                status: Some(RequestState::Open),
                                kind: Some(RequestKind::Rebuild),
                                limit: Some(200),
                                offset: Some(offset),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                        let count = requests.len();
                        for request in requests {
                            for contribution in request.requesters.iter().filter(|entry| {
                                entry.active && entry.satisfied_by_attempt.is_none()
                            }) {
                                if self
                                    .adapter
                                    .contains_commits(
                                        orch,
                                        built,
                                        &contribution.requirements.commits,
                                    )
                                    .await
                                    .is_ok()
                                {
                                    covered.push(contribution.id.clone());
                                }
                            }
                        }
                        if count < 200 {
                            break;
                        }
                        offset += 200;
                    }
                }
                RequestKind::LiftDrain => {}
            }
        }
        covered.sort();
        covered.dedup();
        store::report_request_with_fulfillment(
            &orch.db.local,
            &self.acceptance.attempt_id,
            &self.claim_id,
            receipt_id,
            verified.outcome,
            verified.document,
            verified.settled,
            &covered,
            now(),
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(())
    }
}

/// Called by the durable dispatch outbox only. A duplicate/replayed claim
/// refuses; physical reconciliation is the consumer's job, not a new launch.
pub(crate) async fn dispatch_accepted(orch: &Orchestrator, event_id: &str) -> Result<(), String> {
    let attempt = store::get_attempt_by_event(&orch.db.local, event_id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or("No request acceptance journal record exists for dispatch")?;
    let expected = attempt.acceptance;
    if expected.install_id != orch.anon_device_manager.device_id()
        || registry::definition(expected.kind.as_str())?.actor != expected.actor_id
        || !expected.binding.names(&registry::act_digest(
            expected.kind,
            expected.target.as_deref(),
        ))
    {
        return Err(
            "Request acceptance does not name this install, registered actor and exact act".into(),
        );
    }
    if attempt.state != RequestState::Accepted {
        return Err(
            "Request acceptance has already been claimed or spent; no dispatch is repeated".into(),
        );
    }
    let validation = async {
        let adapter = consumer(orch, expected.kind).await?;
        if expected.kind == RequestKind::BumpVersion {
            bump_policy(orch, expected.policy_project_id.as_deref()).await?;
        }
        adapter.validate_snapshot(orch, &expected).await?;
        Ok::<_, String>(adapter)
    }
    .await;
    let adapter = match validation {
        Ok(adapter) => adapter,
        Err(error) => {
            // No consumer claim or dispatch exists here. Positive knowledge
            // that no actor was launched permits settlement, not an old retry.
            store::fail_unclaimed_request(
                &orch.db.local,
                event_id,
                &expected,
                &format!("pre-dispatch:{}", expected.attempt_id),
                scrub_document(json!({"phase":"before_claim","error":error})),
                now(),
            )
            .await
            .map_err(|failure| format!("{error}; refusal could not be persisted: {failure}"))?;
            return Err(error);
        }
    };
    let claim_id = uuid::Uuid::new_v4().to_string();
    store::claim_request(&orch.db.local, event_id, &expected, &claim_id, now())
        .await
        .map_err(|error| error.to_string())?;
    let context = AcceptedRequestContext {
        event_id: event_id.into(),
        claim_id,
        acceptance: expected,
        adapter: adapter.clone(),
    };
    if let Err(error) = context.recheck(orch).await {
        store::report_request(
            &orch.db.local,
            &context.acceptance.attempt_id,
            &context.claim_id,
            &format!("before-dispatch:{}", context.acceptance.attempt_id),
            RequestAttemptOutcome::Failed,
            scrub_document(json!({"phase":"before_dispatch","error":error})),
            true,
            now(),
        )
        .await
        .map_err(|failure| format!("{error}; refusal could not be persisted: {failure}"))?;
        return Err(error);
    }
    if let Err(error) = adapter.dispatch(orch, &context).await {
        // Dispatch may have produced effects before returning an error. Keep
        // its physical fence until the consumer positively reconciles liveness.
        let recording = store::report_request(
            &orch.db.local,
            &context.acceptance.attempt_id,
            &context.claim_id,
            &format!("dispatch-error:{}", context.acceptance.attempt_id),
            RequestAttemptOutcome::Inconclusive,
            scrub_document(json!({"phase":"dispatch","error":error})),
            false,
            now(),
        )
        .await;
        return Err(match recording {
            Ok(_) => error,
            Err(failure) => format!("{error}; dispatch evidence could not be recorded: {failure}"),
        });
    }
    Ok(())
}

/// Reconstruct an effect-free reference after a runner restart. Possession of
/// event/claim ids is not authority: the caller must hold the exact registered
/// consumer instance, and the durable journal must match its whole intended act.
/// Live effects still require recheck_for; reporting settlement remains possible
/// when a failed actor is fenced or live policy now refuses the physical act.
/// A settled context permits only replay of its existing exact report receipt;
/// storage and live rechecks still reject every new outcome or physical effect.
#[allow(dead_code)] // Consumer recovery entry point, never a resource or invoke.
pub(crate) async fn recover_claimed_request(
    orch: &Orchestrator,
    event_id: &str,
    intended: &RequestAcceptance,
    adapter: Arc<dyn RequestConsumer>,
) -> Result<AcceptedRequestContext, String> {
    let registered = orch.operator_requests.consumer(intended.kind.as_str())?;
    if !Arc::ptr_eq(&registered, &adapter)
        || adapter.kind() != intended.kind.as_str()
        || adapter.actor() != intended.actor_id
        || registry::definition(intended.kind.as_str())?.actor != intended.actor_id
        || intended.install_id != orch.anon_device_manager.device_id()
        || !intended.binding.names(&registry::act_digest(
            intended.kind,
            intended.target.as_deref(),
        ))
    {
        return Err(
            "Recovery requires this install's registered consumer and exact accepted act".into(),
        );
    }
    let attempt = store::get_attempt_by_event(&orch.db.local, event_id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or("No durable accepted attempt exists for recovery")?;
    if attempt.acceptance != *intended {
        return Err("Recovery intended act differs from the immutable acceptance".into());
    }
    let claim_id = attempt
        .claim_id
        .filter(|claim| !claim.trim().is_empty())
        .ok_or("An unclaimed acceptance cannot be recovered as an acting context")?;
    match store::dispatch_disposition(&orch.db.local, event_id)
        .await
        .map_err(|error| error.to_string())?
    {
        store::DispatchDisposition::Unresolved | store::DispatchDisposition::Complete => {}
        store::DispatchDisposition::Dispatch => {
            return Err("An unclaimed acceptance cannot be recovered as a consumer context".into());
        }
    }
    Ok(AcceptedRequestContext {
        event_id: event_id.into(),
        claim_id,
        acceptance: intended.clone(),
        adapter,
    })
}
