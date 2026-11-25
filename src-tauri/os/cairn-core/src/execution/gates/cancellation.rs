//! Withdrawal is a held-only transition, never a provider stop.
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};
use cairn_common::protocol::CallbackRequest;
use cairn_db::turso::params;

/// Stable authenticated identity. Runs are deliberately not authorization keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Actor {
    pub kind: String,
    pub id: Option<String>,
}

/// Resolve authority only from the authenticated route; lookup failure is never
/// converted into machine or operator standing.
pub(crate) async fn request_actor(
    orch: &Orchestrator,
    request: &CallbackRequest,
) -> Result<Actor, String> {
    if request.is_desktop_operator() {
        return Ok(Actor {
            kind: "operator".into(),
            id: None,
        });
    }
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
        .await.map_err(|error| format!("Cannot authorize execution withdrawal: {error}. Use the verified operator or the execution's filing node/actual parent thread."))?;
    let (context, _) =
        crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request).await?;
    let direct_thread = matches!(
        cairn_uri::parse_uri(&agent.home),
        Some(cairn_uri::CairnResource::Thread { path, .. }) if path.is_empty()
    );
    Ok(match context.thread_id.filter(|_| direct_thread) {
        Some(id) => Actor {
            kind: "thread".into(),
            id: Some(id),
        },
        None => Actor {
            kind: "node".into(),
            id: Some(agent.job_id),
        },
    })
}

fn authorized(actor: &Actor, kind: Option<&str>, id: Option<&str>, parent: Option<&str>) -> bool {
    actor.kind == "operator"
        || (actor.id.is_some() && kind == Some(actor.kind.as_str()) && id == actor.id.as_deref())
        || (actor.kind == "thread" && actor.id.is_some() && actor.id.as_deref() == parent)
}

/// Validate against the same facts for preview and commit. Parent and filing
/// provenance are read inside the write transaction, not cached by the handler.
async fn validate_conn(
    conn: &TrackedConnection,
    execution: &str,
    actor: &Actor,
    label: &str,
) -> DbResult<String> {
    let mut rows = conn
        .query(
            "SELECT e.issue_id, e.lifecycle, e.released_at,
                e.filing_principal_kind, e.filing_principal_id, i.parent_thread_id,
                EXISTS(SELECT 1 FROM execution_gates g WHERE g.execution_id = e.id),
                EXISTS(SELECT 1 FROM jobs j WHERE j.execution_id = e.id),
                e.status
           FROM executions e JOIN issues i ON i.id = e.issue_id WHERE e.id = ?1",
            params![execution],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Internal(format!("{label} not found")))?;
    let issue = row.text(0)?;
    let lifecycle = row.text(1)?;
    let released = row.opt_i64(2)?.is_some();
    let kind = row.opt_text(3)?;
    let filer = row.opt_text(4)?;
    let parent = row.opt_text(5)?;
    let gates = row.i64(6)? != 0;
    let jobs = row.i64(7)? != 0;
    let status = row.text(8)?;
    drop(rows);
    if !authorized(actor, kind.as_deref(), filer.as_deref(), parent.as_deref()) {
        let legacy = if kind.is_none() {
            " This legacy execution has no trustworthy filing principal, so filer authority cannot be established; only its actual parent thread or the verified operator may withdraw it."
        } else {
            ""
        };
        return Err(DbError::Internal(format!(
            "Refusing to withdraw {label}: only its authenticated filing node/thread, its issue's actual parent thread, or the verified operator may cancel it.{legacy}"
        )));
    }
    if lifecycle == "withdrawn" {
        return Err(DbError::Internal(format!(
            "{label} is already withdrawn; nothing changed."
        )));
    }
    if lifecycle == "superseded" {
        return Err(DbError::Internal(format!(
            "{label} was superseded and cannot be withdrawn."
        )));
    }
    if released || jobs {
        let mut rows = conn.query(
            "SELECT id FROM jobs WHERE execution_id = ?1 ORDER BY parent_job_id IS NOT NULL, created_at, id",
            params![execution],
        ).await?;
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await? {
            ids.push(row.text(0)?);
        }
        drop(rows);
        let mut nodes = Vec::new();
        for id in ids {
            if let Some(uri) = crate::jobs::queries::home_uri_for_job_conn(conn, &id).await? {
                nodes.push(format!("{uri} PATCH {{action:\"stop\"}}"));
            }
        }
        let stop = if nodes.is_empty() {
            "Read the execution's jobs and PATCH the actual node URI with {action:\"stop\"}."
                .to_string()
        } else {
            nodes.join("; ")
        };
        return Err(DbError::Internal(format!(
            "{label} has released or started and cannot be withdrawn. {stop}. Stop parks a running node; it does not withdraw an execution."
        )));
    }
    if !matches!(lifecycle.as_str(), "pending" | "canonical" | "parallel")
        || matches!(
            status.as_str(),
            "complete" | "failed" | "cancelled" | "withdrawn" | "superseded"
        )
        || !gates
    {
        return Err(DbError::Internal(format!(
            "{label} is not live, held and unreleased. Only an execution still held by gates can be withdrawn."
        )));
    }
    Ok(issue)
}

pub(crate) async fn cancel(
    orch: &Orchestrator,
    db: &LocalDb,
    execution: &str,
    actor: Actor,
    label: &str,
    preview: bool,
) -> Result<String, String> {
    let execution_owned = execution.to_string();
    let label_owned = label.to_string();
    if preview {
        db.read(move |conn| {
            let (execution, actor, label) =
                (execution_owned.clone(), actor.clone(), label_owned.clone());
            Box::pin(async move {
                validate_conn(conn, &execution, &actor, &label)
                    .await
                    .map(|_| ())
            })
        })
        .await
        .map_err(|error| error.to_string())?;
        return Ok(format!(
            "Would withdraw {label}; remove its gates without starting or stopping any node."
        ));
    }
    let now = orch.services.clock.now();
    db.write(move |conn| {
        let (execution, actor, label) =
            (execution_owned.clone(), actor.clone(), label_owned.clone());
        Box::pin(async move { withdraw_conn(conn, &execution, &actor, &label, now).await })
    })
    .await
    .map_err(|error| error.to_string())?;
    let change = crate::notify::execution_db_change_for_id(db, execution, "update").await;
    let _ = orch.services.emitter.emit("db-change", change);
    let change =
        crate::notify::execution_gates_db_change_for_execution(db, execution, "delete").await;
    let _ = orch.services.emitter.emit("db-change", change);
    Ok(format!(
        "Withdrew {label}; removed every gate. No node was started or stopped."
    ))
}

async fn withdraw_conn(
    conn: &TrackedConnection,
    execution: &str,
    actor: &Actor,
    label: &str,
    now: i64,
) -> DbResult<()> {
    let issue = validate_conn(conn, execution, actor, label).await?;
    conn.execute(
        "UPDATE executions SET lifecycle = 'withdrawn', status = 'withdrawn', completed_at = ?2
          WHERE id = ?1 AND released_at IS NULL",
        params![execution, now],
    )
    .await?;
    conn.execute(
        "DELETE FROM execution_gates WHERE execution_id = ?1",
        params![execution],
    )
    .await?;
    conn.execute(
        "INSERT INTO execution_supersessions
         (id, issue_id, superseded_execution_id, successor_execution_id, reason, actor_kind, actor_ref, created_at)
         VALUES (?1, ?2, ?3, NULL, 'withdrawn', ?4, ?5, ?6)",
        params![uuid::Uuid::new_v4().to_string(), issue.as_str(), execution, actor.kind.as_str(), actor.id.as_deref(), now],
    ).await?;
    cairn_workflow::transitions::outcome::recompute_issue_status_conn(conn, &issue).await?;
    conn.on_commit(cairn_db::admission_hint::evidence_changed);
    Ok(())
}

/// Filing accepts run-less automation without granting it operator standing.
/// A request claiming an agent run must resolve or the filing fails closed.
pub(crate) async fn filing_actor(
    orch: &Orchestrator,
    request: &CallbackRequest,
) -> Result<Option<Actor>, String> {
    if request.is_desktop_operator() || request.run_id.is_some() {
        request_actor(orch, request).await.map(Some)
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::gates::sweep::tests::{count, env, execution, file};
    use serde_json::json;

    fn actor(kind: &str, id: &str) -> Actor {
        Actor {
            kind: kind.into(),
            id: Some(id.into()),
        }
    }

    #[test]
    fn authority_uses_only_stable_filer_actual_parent_and_operator() {
        assert!(authorized(
            &actor("node", "filer"),
            Some("node"),
            Some("filer"),
            Some("parent")
        ));
        assert!(authorized(
            &actor("thread", "filer"),
            Some("thread"),
            Some("filer"),
            Some("parent")
        ));
        assert!(authorized(
            &actor("thread", "parent"),
            None,
            None,
            Some("parent")
        ));
        assert!(authorized(&actor("operator", "verified"), None, None, None));
        for caller in [
            actor("node", "stranger"),
            actor("thread", "stranger"),
            actor("node", "parent"),
        ] {
            assert!(!authorized(
                &caller,
                Some("node"),
                Some("filer"),
                Some("parent")
            ));
            assert!(!authorized(&caller, None, None, Some("parent")));
        }
    }

    #[tokio::test]
    async fn withdrawal_is_atomic_audited_and_cannot_reappear_without_gates() {
        let env = env("cancellation.db").await;
        file(
            &env,
            1,
            json!([{"fact":"clock", "notBefore":"2099-01-01T00:00:00Z"}]),
        )
        .await
        .unwrap();
        let id = execution(&env, 1).await.0;
        env.db.execute(
            "UPDATE executions SET filing_principal_kind = 'node', filing_principal_id = 'coord' WHERE id = ?1",
            (id.clone(),),
        ).await.unwrap();
        let request = CallbackRequest {
            run_id: Some("run-coord".into()),
            ..Default::default()
        };
        let caller = request_actor(&env.orch, &request).await.unwrap();
        assert_eq!(caller, actor("node", "coord"));
        cancel(
            &env.orch,
            &env.db,
            &id,
            caller.clone(),
            "held execution",
            true,
        )
        .await
        .unwrap();
        assert_eq!(execution(&env, 1).await.1, "held");
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM execution_supersessions WHERE reason = 'withdrawn'"
            )
            .await,
            0
        );
        cancel(
            &env.orch,
            &env.db,
            &id,
            caller.clone(),
            "held execution",
            false,
        )
        .await
        .unwrap();
        assert!(
            cancel(&env.orch, &env.db, &id, caller, "held execution", false)
                .await
                .unwrap_err()
                .contains("already withdrawn")
        );
        let state = execution(&env, 1).await;
        assert_eq!(state.1, "withdrawn");
        assert!(state.2.is_none());
        assert_eq!(state.3, 0);
        assert_eq!(count(&env, "SELECT COUNT(*) FROM execution_gates").await, 0);
        let read_id = id.clone();
        env.db
            .write(move |conn| {
                let id = read_id.clone();
                Box::pin(async move {
                    cairn_workflow::transitions::outcome::recompute_execution_status_conn(
                        conn, &id,
                    )
                    .await?;
                    let censuses = super::super::census::census_for_executions_conn(
                        conn,
                        std::slice::from_ref(&id),
                    )
                    .await?;
                    let census = &censuses[&id];
                    assert!(!census.held);
                    assert!(census.summary.contains("withdrawn by node coord"));
                    assert!(census.summary.contains("never started"));
                    Ok::<_, DbError>(())
                })
            })
            .await
            .unwrap();
        assert_eq!(count(&env, "SELECT COUNT(*) FROM execution_supersessions WHERE reason = 'withdrawn' AND actor_kind = 'node' AND actor_ref = 'coord' AND successor_execution_id IS NULL").await, 1);
        assert_eq!(execution(&env, 1).await.1, "withdrawn");
        let state = super::super::authoring::hold_state(&env.db, &id)
            .await
            .unwrap();
        assert!(
            super::super::authoring::require_held(&state, "held execution")
                .unwrap_err()
                .contains("withdrawn")
        );
        let released = super::super::sweep::evaluate_execution(&env.orch, &id)
            .await
            .unwrap();
        assert!(!released.released);
        assert_eq!(execution(&env, 1).await.3, 0);
    }

    #[tokio::test]
    async fn identity_failures_legacy_and_current_parent_authority() {
        let env = env("cancellation-authority.db").await;
        for request in [
            CallbackRequest::default(),
            CallbackRequest {
                run_id: Some("missing".into()),
                ..Default::default()
            },
        ] {
            assert!(request_actor(&env.orch, &request).await.is_err());
        }
        file(
            &env,
            1,
            json!([{"fact":"clock", "notBefore":"2099-01-01T00:00:00Z"}]),
        )
        .await
        .unwrap();
        let id = execution(&env, 1).await.0;
        let refusal = cancel(
            &env.orch,
            &env.db,
            &id,
            actor("node", "coord"),
            "held",
            false,
        )
        .await
        .unwrap_err();
        assert!(refusal.contains("legacy") && refusal.contains("actual parent"));
        env.db.execute_script(
            "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at) VALUES('owner', 'p', 'owner', 'active', 'none', 1, 1);
             INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at) VALUES('old', 'p', 'old', 'active', 'none', 1, 1);
             UPDATE issues SET parent_thread_id = 'owner' WHERE id = 'i1';"
        ).await.unwrap();
        assert!(cancel(
            &env.orch,
            &env.db,
            &id,
            actor("thread", "old"),
            "held",
            false
        )
        .await
        .is_err());
        cancel(
            &env.orch,
            &env.db,
            &id,
            actor("thread", "owner"),
            "held",
            false,
        )
        .await
        .unwrap();
        assert_eq!(execution(&env, 1).await.1, "withdrawn");
    }
}

#[cfg(test)]
mod cancellation_api_tests {
    use crate::execution::gates::sweep::tests::{count, env, execution};
    use crate::mcp::types::{ChangeItem, ChangeMode};
    use cairn_common::protocol::{CallbackRequest, DispatchOrigin};
    use serde_json::json;

    #[tokio::test]
    async fn held_filing_preview_cancel_and_history_through_resource_api() {
        let env = env("cancel-resource-api.db").await;
        let caller = CallbackRequest {
            run_id: Some("run-coord".into()),
            ..Default::default()
        };
        let target = "cairn://p/prj/1/executions/1";
        let item = ChangeItem {
            target: "cairn://p/prj/1/executions".into(),
            mode: ChangeMode::Append,
            payload: Some(
                json!({"recipe":"memory-triage","after":[{"fact":"clock","notBefore":"2099-01-01T00:00:00Z"}]}),
            ),
        };
        crate::resources::mutations::dispatch_resource_change(&env.orch, &caller, 0, &item, false)
            .await
            .unwrap();
        let collection =
            crate::resources::read_cairn_resource(&env.orch, &caller, "cairn://p/prj/1/executions")
                .await;
        assert!(collection.contains(target), "{collection}");
        assert!(collection.contains("cancel:true"), "{collection}");
        assert_eq!(count(&env, "SELECT COUNT(*) FROM executions WHERE filing_principal_kind = 'node' AND filing_principal_id = 'coord'").await, 1);
        let item = ChangeItem {
            target: target.into(),
            mode: ChangeMode::Patch,
            payload: Some(json!({"cancel":true})),
        };
        let preview = crate::resources::mutations::dispatch_resource_change(
            &env.orch, &caller, 0, &item, true,
        )
        .await
        .unwrap();
        assert!(preview.summary.contains("Would withdraw"));
        assert_eq!(execution(&env, 1).await.1, "held");
        let forged = CallbackRequest {
            run_id: Some("absent".into()),
            ..Default::default()
        };
        assert!(crate::resources::mutations::dispatch_resource_change(
            &env.orch, &forged, 0, &item, false
        )
        .await
        .is_err());
        let mixed = ChangeItem {
            payload: Some(json!({"cancel":true, "after":[]})),
            ..item.clone()
        };
        assert!(crate::resources::mutations::dispatch_resource_change(
            &env.orch, &caller, 0, &mixed, false
        )
        .await
        .is_err());
        crate::resources::mutations::dispatch_resource_change(&env.orch, &caller, 0, &item, false)
            .await
            .unwrap();
        for uri in [target, "cairn://p/prj/1/executions"] {
            let text = crate::resources::read_cairn_resource(&env.orch, &caller, uri).await;
            assert!(text.contains("withdrawn by node coord"), "{text}");
            assert!(text.contains("never started"), "{text}");
        }
        let operator = CallbackRequest {
            origin: Some(DispatchOrigin::DesktopOperator),
            ..Default::default()
        };
        let result = crate::resources::mutations::dispatch_resource_change(
            &env.orch, &operator, 0, &item, false,
        )
        .await
        .unwrap_err();
        assert!(result.error.contains("already withdrawn"));
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM execution_supersessions WHERE reason = 'withdrawn'"
            )
            .await,
            1
        );
    }
}

#[cfg(test)]
mod authority_boundary_tests {
    use super::*;
    use crate::execution::gates::sweep::tests::{env, execution, file};
    use serde_json::json;

    #[tokio::test]
    async fn thread_tasks_are_nodes_and_resumed_thread_sessions_keep_thread_authority() {
        let env = env("cancellation-thread-tasks.db").await;
        env.db.execute_script(
            "INSERT INTO threads(id, project_id, name, created_at, updated_at) VALUES('t','p','owner',1,1);
             INSERT INTO jobs(id, thread_id, project_id, node_name, agent_config_id, status, uri_segment, created_at, updated_at)
               VALUES('session','t','p','thread','thread','running','thread',1,1);
             INSERT INTO jobs(id, thread_id, parent_job_id, project_id, node_name, agent_config_id, status, uri_segment, created_at, updated_at)
               VALUES('task','t','session','p','task','builder','running','child',1,1);
             INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at) VALUES('r-session','p','session','live',1,1);
             INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at) VALUES('r-resumed','p','session','live',2,2);
             INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at) VALUES('r-task','p','task','live',1,1);
             UPDATE issues SET parent_thread_id = 't' WHERE id = 'i1';"
        ).await.unwrap();
        let request = |id: &str| CallbackRequest {
            run_id: Some(id.into()),
            ..Default::default()
        };
        let session = request_actor(&env.orch, &request("r-session"))
            .await
            .unwrap();
        assert_eq!(session.kind, "thread");
        assert_eq!(session.id.as_deref(), Some("t"));
        assert_eq!(
            request_actor(&env.orch, &request("r-resumed"))
                .await
                .unwrap(),
            session
        );
        let task = request_actor(&env.orch, &request("r-task")).await.unwrap();
        assert_eq!(task.kind, "node");
        assert_eq!(task.id.as_deref(), Some("task"));
        file(
            &env,
            1,
            json!([{"fact":"clock","notBefore":"2099-01-01T00:00:00Z"}]),
        )
        .await
        .unwrap();
        let id = execution(&env, 1).await.0;
        assert!(cancel(&env.orch, &env.db, &id, task, "held", false)
            .await
            .is_err());
        cancel(&env.orch, &env.db, &id, session, "held", false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancellation_after_release_names_the_actual_stop_node() {
        let env = env("cancellation-released.db").await;
        file(&env, 1, json!([])).await.unwrap();
        let id = execution(&env, 1).await.0;
        let request = CallbackRequest {
            origin: Some(cairn_common::protocol::DispatchOrigin::DesktopOperator),
            ..Default::default()
        };
        let operator = request_actor(&env.orch, &request).await.unwrap();
        let error = cancel(&env.orch, &env.db, &id, operator, "execution", false)
            .await
            .unwrap_err();
        assert!(error.contains("cairn://p/prj/1/1/"), "{error}");
        assert!(error.contains(r#"PATCH {action:"stop"}"#), "{error}");
        assert!(error.contains("parks a running node"), "{error}");
    }
}

#[cfg(test)]
mod transition_order_tests {
    use super::*;
    use crate::execution::gates::sweep::tests::{count, env, execution, file};
    use crate::execution::gates::sweep::{self, ConnRelease, ReleaseCause};
    use serde_json::json;

    async fn release(db: &LocalDb, id: String) -> ConnRelease {
        db.write(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                sweep::release_conn(
                    conn,
                    &id,
                    100,
                    &ReleaseCause::Override {
                        principal: json!({"kind":"operator"}),
                    },
                )
                .await
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn only_one_of_withdrawal_and_release_can_win_after_stale_preflight() {
        let env = env("cancel-release-order.db").await;
        let operator = Actor {
            kind: "operator".into(),
            id: None,
        };
        let gate = json!([{"fact":"clock","notBefore":"2099-01-01T00:00:00Z"}]);
        file(&env, 1, gate.clone()).await.unwrap();
        let withdrawn = execution(&env, 1).await.0;
        // A sweeper has observed this held candidate before cancellation commits.
        assert!(
            super::super::authoring::hold_state(&env.db, &withdrawn)
                .await
                .unwrap()
                .has_gates
        );
        cancel(
            &env.orch,
            &env.db,
            &withdrawn,
            operator.clone(),
            "held",
            false,
        )
        .await
        .unwrap();
        assert!(matches!(
            release(&env.db, withdrawn).await,
            ConnRelease::NotHeld
        ));
        assert_eq!(execution(&env, 1).await.3, 0);

        file(&env, 2, gate).await.unwrap();
        let released = execution(&env, 2).await.0;
        cancel(
            &env.orch,
            &env.db,
            &released,
            operator.clone(),
            "held",
            true,
        )
        .await
        .unwrap();
        assert!(matches!(
            release(&env.db, released.clone()).await,
            ConnRelease::Released(_)
        ));
        assert!(
            cancel(&env.orch, &env.db, &released, operator, "held", false)
                .await
                .unwrap_err()
                .contains("cannot be withdrawn")
        );
        assert!(execution(&env, 2).await.3 > 0);
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM execution_supersessions WHERE reason='withdrawn'"
            )
            .await,
            1
        );
    }

    #[tokio::test]
    async fn replacement_and_withdrawal_preserve_exactly_the_committed_winner() {
        let env = env("cancel-replacement-order.db").await;
        let operator = Actor {
            kind: "operator".into(),
            id: None,
        };
        let gate = json!([{"fact":"clock","notBefore":"2099-01-01T00:00:00Z"}]);
        file(&env, 1, gate.clone()).await.unwrap();
        let old = execution(&env, 1).await.0;
        cancel(&env.orch, &env.db, &old, operator.clone(), "held", true)
            .await
            .unwrap();
        file(&env, 1, gate.clone()).await.unwrap();
        assert!(
            cancel(&env.orch, &env.db, &old, operator.clone(), "held", false)
                .await
                .unwrap_err()
                .contains("superseded")
        );
        let newest = execution(&env, 1).await.0;
        cancel(&env.orch, &env.db, &newest, operator, "held", false)
            .await
            .unwrap();
        file(&env, 1, gate).await.unwrap();
        assert_eq!(execution(&env, 1).await.1, "held");
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM executions WHERE lifecycle='pending' AND issue_id='i1'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM execution_supersessions WHERE reason='withdrawn'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM execution_supersessions WHERE reason='successor_created'"
            )
            .await,
            1
        );
    }
}
