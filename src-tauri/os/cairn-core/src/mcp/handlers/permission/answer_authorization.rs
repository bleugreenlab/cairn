//! Authority to answer is separate from the authority an allow would delegate.
use super::*;
use cairn_common::authorization::{
    AuthorityAction, AuthorityAudience, AuthorityPlace, AuthorityPrincipal, AuthorityScope,
};
use cairn_db::lineage::FilingParent;

/// Called before the first-answer claim and again when executing a recovered
/// winner. Persisted edges, not URI prefixes or payload claims, confer ancestry.
pub(super) async fn authorize(
    orch: &Orchestrator,
    db: &LocalDb,
    record: &PermissionRequestRecord,
    answer: &PermissionAnswer,
    check_effect: bool,
) -> Result<(), String> {
    if answer.operator_approval().is_some() {
        return Ok(());
    }
    let result = check_agent(orch, db, record, answer, check_effect).await;
    if let Err(reason) = result {
        let actor = answer.resolution_provenance()?.3;
        let decision_actor = answer
            .decision_attribution()
            .map(|(actor, _)| actor.clone());
        let event = cairn_db::storage::authority::NewAuthorizationEvent {
            scope: AuthorityScope::new(
                AuthorityPlace::Resource {
                    canonical_uri: permission_uri(db, &record.id).await?,
                },
                AuthorityAction::Write,
            ),
            mutation: "update".to_string(),
            summary: format!(
                "Refused {:?} of permission {} by {}: {reason}",
                answer.decision,
                record.id,
                actor.as_deref().unwrap_or("<unattributed>")
            ),
            outcome: "forbidden".to_string(),
            reason: "permission_answer_authority".to_string(),
            principal: AuthorityPrincipal {
                actor: decision_actor.clone(),
                ..Default::default()
            },
            audience: AuthorityAudience::workspace(crate::authorization::WORKSPACE_ID),
            run_id: Some(record.run_id.clone()),
            request_uri: Some(record.id.clone()),
            grant_id: None,
            request_acceptance: None,
            decision_actor,
            appearance_snapshot: answer
                .decision_attribution()
                .map(|(_, appearance)| appearance.clone()),
            decided_at: chrono::Utc::now().timestamp(),
        };
        cairn_db::storage::authority::append_event(&orch.db.local, event)
            .await
            .map_err(|error| {
                format!("Permission answer refused; could not journal refusal: {error}")
            })?;
        return Err(format!(
            "{OPERATOR_PENDING_REFUSAL} Only the requester's ancestor lineage or the operator may answer this permission. {reason} The request remains pending and answerable."
        ));
    }
    Ok(())
}

async fn permission_uri(db: &LocalDb, request_id: &str) -> Result<String, String> {
    let request_id = request_id.to_string();
    db.read(move |conn| {
        let request_id = request_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COALESCE(pr.job_id, r.job_id), pr.uri_segment
                 FROM permission_requests pr JOIN runs r ON r.id = pr.run_id
                 WHERE pr.id = ?1",
                    params![request_id.as_str()],
                )
                .await?;
            if let Some(row) = rows.next().await? {
                if let Some(job_id) = row.opt_text(0)? {
                    if let Some(home) =
                        cairn_db::lineage::home_uri_for_job_conn(conn, &job_id).await?
                    {
                        return Ok(match row.opt_text(1)? {
                            Some(segment) => format!("{home}/permissions/{segment}"),
                            None => format!("{home}/permissions"),
                        });
                    }
                }
            }
            // Old unowned prompts have no node address. The journal still names
            // their stored row in request_uri and the refusal summary.
            Ok("cairn://db".to_string())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn check_agent(
    orch: &Orchestrator,
    db: &LocalDb,
    record: &PermissionRequestRecord,
    answer: &PermissionAnswer,
    check_effect: bool,
) -> Result<(), String> {
    // A recovered resource winner carries the principal written by the
    // authenticated resource door. A channel's sender text is never such proof.
    let recovered;
    let principal = if let Some((principal, _)) = answer.decision_attribution() {
        principal
    } else if answer.provenance_resolution_id.is_some()
        && matches!(
            answer.answerer,
            Answerer::Surface(AnswerSurface::ResourcePatch)
        )
    {
        recovered = answer
            .provenance_actor
            .as_deref()
            .and_then(|actor| serde_json::from_str::<PrincipalRef>(actor).ok());
        recovered
            .as_ref()
            .ok_or("The stored answer has no authenticated agent identity.")?
    } else {
        return Err("This answer carries no authenticated ancestor run.".to_string());
    };
    let PrincipalRef::Agent {
        node,
        run_id: Some(run_id),
    } = principal
    else {
        return Err("This answer is not from an authenticated agent run.".to_string());
    };
    let agent = super::super::run_context::authenticated_agent_for_run(&orch.db, run_id).await?;
    if &agent.home != node {
        return Err("The answer's agent home does not match its stored run.".to_string());
    }
    if !is_ancestor(db, &record.id, &agent.job_id).await? {
        return Err("The answering run is not an ancestor of the requester.".to_string());
    }
    if check_effect
        && matches!(answer.decision, PermissionDecision::Allow)
        && !owns_effect(orch, record, &agent.run_id).await?
    {
        return Err(
            "An ancestor may allow only effects within its own effective grants.".to_string(),
        );
    }
    Ok(())
}

/// Whether `answering_job` is in the strict ancestor lineage of the job that
/// raised `request_id`. Job edges are persisted parentage; an issue's parent
/// edges count only where [`FilingParent::proof_sql`] proves them, checked at
/// every hop the walk crosses.
pub(crate) async fn is_ancestor(
    db: &LocalDb,
    request_id: &str,
    answering_job: &str,
) -> Result<bool, String> {
    let request_id = request_id.to_string();
    let answering_job = answering_job.to_string();
    let sql = format!(
        "WITH RECURSIVE
                 requester AS (
                     SELECT COALESCE(pr.job_id, r.job_id) AS job_id,
                            COALESCE(j.issue_id, e.issue_id) AS issue_id
                     FROM permission_requests pr JOIN runs r ON r.id = pr.run_id
                     JOIN jobs j ON j.id = COALESCE(pr.job_id, r.job_id)
                     LEFT JOIN executions e ON e.id = j.execution_id WHERE pr.id = ?1
                 ),
                 edges(kind, id, parent_kind, parent_id) AS (
                     SELECT 'job', id, 'job', parent_job_id FROM jobs WHERE parent_job_id IS NOT NULL
                     UNION ALL
                     SELECT 'job', j.id, 'issue', COALESCE(j.issue_id, e.issue_id)
                     FROM jobs j LEFT JOIN executions e ON e.id = j.execution_id
                     WHERE COALESCE(j.issue_id, e.issue_id) IS NOT NULL
                     UNION ALL
                     SELECT 'issue', i.id, 'issue', i.parent_issue_id FROM issues i
                     JOIN jobs j ON {issue_proof}
                     UNION ALL
                     SELECT 'issue', i.id, 'job', i.parent_job_id FROM issues i
                     JOIN jobs j ON {issue_proof}
                     UNION ALL
                     SELECT 'issue', i.id, 'thread', i.parent_thread_id FROM issues i
                     JOIN jobs j ON {thread_proof}
                 ),
                 lineage(kind, id) AS (
                     SELECT 'job', job_id FROM requester
                     UNION
                     SELECT e.parent_kind, e.parent_id FROM edges e
                     JOIN lineage l ON l.kind = e.kind AND l.id = e.id
                 )
                 SELECT 1 FROM jobs a, requester r
                 WHERE a.id = ?2 AND a.id != r.job_id AND (
                     EXISTS (SELECT 1 FROM lineage WHERE kind = 'job' AND id = a.id)
                     OR (a.parent_job_id IS NULL AND a.issue_id IS NOT NULL
                         AND a.issue_id != COALESCE(r.issue_id, '')
                         AND EXISTS (SELECT 1 FROM lineage WHERE kind = 'issue' AND id = a.issue_id))
                     OR (a.parent_job_id IS NULL AND a.uri_segment = 'thread'
                         AND EXISTS (SELECT 1 FROM lineage WHERE kind = 'thread' AND id = a.thread_id))
                 ) LIMIT 1",
        issue_proof = FilingParent::Issue.proof_sql(),
        thread_proof = FilingParent::Thread.proof_sql(),
    );
    db.read(move |conn| {
        let request_id = request_id.clone();
        let answering_job = answering_job.clone();
        let sql = sql.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    sql.as_str(),
                    params![request_id.as_str(), answering_job.as_str()],
                )
                .await?;
            Ok(rows.next().await?.is_some())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn owns_effect(
    orch: &Orchestrator,
    record: &PermissionRequestRecord,
    run_id: &str,
) -> Result<bool, String> {
    if let Some(detail) = parse_crossing_detail(&record.tool_input) {
        // The operator-only stored-prompt checks run before this check. A
        // parent cannot manufacture a grant by answering its child's prompt.
        if detail.names_no_path() {
            return Ok(false);
        }
        match fence_policy_for_run(orch, run_id).await? {
            Fence::Allow => return Ok(true),
            Fence::Deny => return Ok(false),
            Fence::Ask => {}
        }
        return Ok(detail.session_key().is_some_and(|key| {
            orch.session_allowed_crossings
                .lock()
                .is_ok_and(|allowed| allowed.contains(key))
        }));
    }
    let Some(tool) = permission_request_granted_tool_name(record) else {
        return Ok(false);
    };
    let db = crate::execution::routing::routing_db_for_id(&orch.db, run_id)
        .await
        .map_err(|error| error.to_string())?;
    let run_id = run_id.to_string();
    let config = db
        .read(move |conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT j.agent_config_id, e.snapshot, s.backend FROM runs r
                 JOIN jobs j ON j.id = r.job_id JOIN executions e ON e.id = j.execution_id
                 LEFT JOIN sessions s ON s.id = r.session_id
                 WHERE r.id = ?1",
                        params![run_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| Ok((row.opt_text(0)?, row.opt_text(1)?, row.opt_text(2)?)))
                    .transpose()
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    let Some((Some(agent_id), Some(snapshot), Some(backend))) = config else {
        return Ok(false);
    };
    let snapshot = crate::config::snapshot_migrate::load(&snapshot)?;
    let Some(agent) = snapshot.agents.get(&agent_id) else {
        return Ok(false);
    };
    let tools = crate::backends::backend_for_name(Some(&backend)).resolve_tools(
        &agent.tools,
        agent.disallowed_tools.as_deref().unwrap_or_default(),
    );
    // Only Codex consults the reusable approval set. It must not override
    // another backend's explicit exclusions or grant that backend native tools.
    let approved = backend == "codex"
        && orch
            .session_allowed_tools
            .lock()
            .is_ok_and(|allowed| allowed.contains(&tool));
    Ok((tools.allowed.contains(&tool) || approved) && !tools.disallowed.contains(&tool))
}
