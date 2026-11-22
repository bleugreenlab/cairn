//! Runner-owned identities joined to occupancy by physical request and attempt.
//! No executor-local paths or process-tree guesses participate in attribution.

use std::collections::HashMap;

use cairn_common::executor_protocol::ExecutorInspection;

use crate::execution::checks::requests;
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

pub(crate) type RunningAttributions = HashMap<(String, String), RunningAttribution>;

#[derive(Default)]
pub(crate) struct RunningAttribution {
    pub owner: Option<String>,
    pub project: Option<String>,
    pub commit: Option<String>,
    pub suites: Vec<String>,
    pub handles: Vec<String>,
}

impl RunningAttribution {
    pub(crate) fn render(&self) -> String {
        let mut parts = Vec::new();
        if let (Some(project), Some(commit)) = (&self.project, &self.commit) {
            let short = commit.get(..12).unwrap_or(commit);
            parts.extend(self.suites.iter().map(|suite| format!(
                "[{suite}@{short}](cairn://p/{project}/check-results/{commit}?suite={suite}&environment=current)"
            )));
        }
        if let Some(owner) = &self.owner {
            parts.push(format!("owner [{owner}]({owner})"));
        }
        if let Some(project) = &self.project {
            parts.extend(self.handles.iter().map(|handle| {
                format!(
                    "check [{handle}]({})",
                    requests::request_uri(project, handle)
                )
            }));
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!(" — {}", parts.join("; "))
        }
    }
}

pub(crate) async fn running_attributions(
    orch: &Orchestrator,
    executor: &ExecutorInspection,
) -> RunningAttributions {
    let mut result = RunningAttributions::new();
    let identities = executor
        .occupancy
        .executing_requests
        .iter()
        .map(|r| (r.request_id.clone(), r.attempt_id.clone()))
        .chain(
            executor
                .occupancy
                .finishing_requests
                .iter()
                .map(|r| (r.request_id.clone(), r.attempt_id.clone())),
        );
    for identity in identities {
        let request = orch.fleet.inspection_request(&identity);
        let claim = match orch.fleet.check_claim(&identity) {
            Some(claim) => Some(claim),
            None => crate::storage::check_lane_adoptions::lookup(
                &orch.db.local,
                &identity.0,
                &identity.1,
            )
            .await
            .unwrap_or_else(|error| {
                log::warn!("executor attribution cannot read grant: {error}");
                None
            }),
        };
        let project_id = request
            .as_ref()
            .map(|r| r.project_id.as_str())
            .or_else(|| claim.as_ref().map(|c| c.project_id.as_str()));
        let Some(project_id) = project_id else {
            continue;
        };
        let db = match requests::request_db(orch, project_id).await {
            Ok(db) => db,
            Err(error) => {
                log::warn!("executor attribution cannot read project {project_id}: {error}");
                continue;
            }
        };
        let project = db
            .query_opt(
                "SELECT key FROM projects WHERE id=?1",
                (project_id.to_string(),),
                |row| row.text(0),
            )
            .await
            .ok()
            .flatten();
        let job = request
            .as_ref()
            .and_then(|r| {
                r.requesting_job_id
                    .as_deref()
                    .or_else(|| r.owner.as_ref().and_then(|o| o.job_id.as_deref()))
            })
            .or_else(|| {
                claim.as_ref().and_then(|claim| {
                    claim
                        .recording_plans
                        .iter()
                        .find_map(|plan| plan.get("job_id").and_then(serde_json::Value::as_str))
                })
            });
        let owner = match job {
            Some(job) => crate::jobs::queries::home_uri_for_job(&db, job)
                .await
                .ok()
                .flatten(),
            None => None,
        };
        let mut attribution = RunningAttribution {
            owner,
            project,
            commit: claim.as_ref().map(|c| c.commit_sha.clone()),
            suites: claim
                .as_ref()
                .map(|c| c.check_names.clone())
                .unwrap_or_default(),
            handles: Vec::new(),
        };
        if claim.is_some() {
            match crate::storage::check_lane_adoptions::waiters(
                &orch.db.local,
                &identity.0,
                &identity.1,
            )
            .await
            {
                Ok(waiters) => {
                    for waiter in waiters.into_iter().filter(|w| {
                        w.consumer == "manual"
                            && matches!(
                                w.state.as_str(),
                                "running" | "joined" | "awaiting_adoption"
                            )
                    }) {
                        match requests::load_request(&db, project_id, &waiter.consumer_ref).await {
                            Ok(Some(row)) if row.is_open() => {
                                if attribution.owner.is_none() {
                                    attribution.owner = Some(row.requester_ref);
                                }
                                attribution.handles.push(row.id);
                            }
                            Ok(_) => {}
                            Err(error) => log::warn!("executor attribution: {error}"),
                        }
                    }
                }
                Err(error) => log::warn!("executor attribution cannot read consumers: {error}"),
            }
        }
        attribution.handles.sort();
        attribution.handles.dedup();
        result.insert(identity, attribution);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::check_lane_adoptions as store;
    use std::sync::Arc;

    #[tokio::test]
    async fn attribution_recovers_grant_owner_and_all_manual_handles_without_a_live_queue() {
        let db = crate::storage::migrated_test_db("executor-attribution.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','proj','/tmp',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('i','p',7,'I','active','active','none',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
               VALUES('e','r','i','p','running',1,1);
             INSERT INTO jobs(id,project_id,issue_id,execution_id,status,uri_segment,created_at,updated_at)
               VALUES('j','p','i','e','running','builder',1,1);",
        ).await.unwrap();
        for handle in ["handle-a", "handle-b"] {
            db.execute(
                "INSERT INTO check_requests(id,project_id,suite,subject_job_id,consumer,requester_ref,requester_job_id,
                  priority,state,accepted_at_ms,state_since_ms,updated_ms)
                 VALUES(?1,'p','rust-tests','j','requested','cairn://p/proj/7/1/builder','j','review_check','running',1,1,1)",
                (handle.to_string(),),
            ).await.unwrap();
        }
        let claim = store::Claim {
            request_id: "request".into(),
            attempt_id: "attempt".into(),
            project_id: "p".into(),
            check_names: vec!["rust-tests".into()],
            platform: "linux".into(),
            commit_sha: "aaaaaaaaaaaa1111".into(),
            input_hashes: vec!["input".into()],
            recording_plans: vec![serde_json::json!({"job_id":"j"})],
            consumer: "manual".into(),
            consumer_ref: "handle-a".into(),
            executor_id: "executor".into(),
            machine: "fedora".into(),
            granted_at_ms: 1,
            state: "running".into(),
            stopped_at_ms: None,
            evidence: None,
            observation_id: None,
            updated_ms: 1,
        };
        let waiters: Vec<_> = ["handle-a", "handle-b"]
            .into_iter()
            .map(|handle| store::Waiter {
                request_id: claim.request_id.clone(),
                attempt_id: claim.attempt_id.clone(),
                check_name: "rust-tests".into(),
                item_index: 0,
                consumer: "manual".into(),
                consumer_ref: handle.into(),
                state: "running".into(),
                observation_id: None,
                evidence: None,
                updated_ms: 1,
            })
            .collect();
        store::insert_grant(&db, &claim, &waiters).await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let search = Arc::new(
            crate::storage::SearchIndex::open_or_create(root.path().join("search")).unwrap(),
        );
        let orch = Orchestrator::builder(
            Arc::new(crate::db::DbState::new(Arc::new(db), search)),
            Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            root.path().to_path_buf(),
        )
        .build();
        let mut executor = super::super::tests::inspection("fedora", false);
        executor.occupancy = super::super::running_now_tests::running(None);
        let map = running_attributions(&orch, &executor).await;
        let row = &map[&("request".into(), "attempt".into())];
        assert_eq!(row.owner.as_deref(), Some("cairn://p/proj/7/1/builder"));
        assert_eq!(row.commit.as_deref(), Some("aaaaaaaaaaaa1111"));
        assert_eq!(row.handles, ["handle-a", "handle-b"]);
        let diagnostic = "running on [fedora](cairn://executors/fedora), slot-43 since 09:01:00Z";
        orch.db
            .local
            .execute(
                "UPDATE check_requests SET executor='fedora',diagnostic=?1 WHERE id='handle-a'",
                (diagnostic.to_string(),),
            )
            .await
            .unwrap();
        let requested = crate::resources::check_requests::requested_section(&orch, "proj", "j")
            .await
            .unwrap();
        assert!(requested.contains(diagnostic), "{requested}");
        store::transition_waiter(&orch.db.local, &waiters[0], "cancelled", None, None)
            .await
            .unwrap();
        let map = running_attributions(&orch, &executor).await;
        assert_eq!(
            map[&("request".into(), "attempt".into())].handles,
            ["handle-b"]
        );
        orch.db
            .local
            .execute(
                "UPDATE check_requests SET state='cancelled' WHERE id='handle-b'",
                (),
            )
            .await
            .unwrap();
        let map = running_attributions(&orch, &executor).await;
        assert!(map[&("request".into(), "attempt".into())]
            .handles
            .is_empty());
        executor.occupancy.executing_requests[0].attempt_id = "new-attempt".into();
        assert!(running_attributions(&orch, &executor).await.is_empty());
    }
}
