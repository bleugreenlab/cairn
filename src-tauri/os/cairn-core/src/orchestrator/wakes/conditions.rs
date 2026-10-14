use crate::orchestrator::Orchestrator;
pub use cairn_attention::wakes::conditions::*;
pub fn install_evaluator(orch: Orchestrator) {
    cairn_attention::wakes::conditions::install_evaluator(crate::attention_host::runtime(&orch))
}
pub async fn evaluate_thread_conditions(
    orch: &Orchestrator,
    thread_id: &str,
) -> Result<Vec<(String, ConditionOutcome, bool)>, String> {
    cairn_attention::wakes::conditions::evaluate_thread_conditions(
        &crate::attention_host::runtime(orch),
        thread_id,
    )
    .await
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::storage::RowExt;
    use cairn_db::turso::params;
    use std::sync::Arc;
    #[tokio::test]
    async fn committed_thread_write_notifies_evaluator_and_lands_push() {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let thread_id = format!("condition-integration-{}", uuid::Uuid::new_v4());
        let thread_store = tempfile::tempdir().unwrap();
        crate::threads::db_store::use_test_path(&thread_id, thread_store.path().join("thread.db"));
        crate::threads::db_store::open(&thread_id)
            .await
            .unwrap()
            .exclusive(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "CREATE TABLE findings(id INTEGER PRIMARY KEY, ready INTEGER)",
                        (),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        let local = crate::storage::migrated_test_db("wake-condition-integration.db").await;
        local
            .execute_script(&format!(
                "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','P','P','/tmp',1,1);
             INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES('{thread_id}','p','general','active','none',1,1);"
            ))
            .await
            .unwrap();
        let job_id = local
            .write({
                let thread_id = thread_id.clone();
                move |conn| {
                    let thread_id = thread_id.clone();
                    Box::pin(async move {
                        let job_id =
                            crate::threads::ensure_thread_session_conn(conn, &thread_id, None)
                                .await?;
                        conn.execute(
                        "INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at)
                         VALUES('condition-run','p',?1,'live',1,1)",
                        params![job_id.as_str()],
                    ).await?;
                        Ok(job_id)
                    })
                }
            })
            .await
            .unwrap();
        create_wake_condition(
            &local,
            &thread_id,
            "SELECT id FROM findings WHERE ready = 1",
            "findings ready",
            false,
            "agent",
        )
        .await
        .unwrap();

        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        let worktree = tempfile::tempdir().unwrap();
        let orch = OrchestratorBuilder::new(
            db.clone(),
            Arc::new(TestServicesBuilder::new().build()),
            worktree.path().to_path_buf(),
        )
        .build();
        install_evaluator(orch);

        crate::threads::db_store::apply_write(
            &thread_id,
            vec!["INSERT INTO findings(id, ready) VALUES(1, 1)".into()],
            vec![Vec::new()],
        )
        .await
        .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let count = db.local.read({
                    let job_id = job_id.clone();
                    move |conn| {
                        let job_id = job_id.clone();
                        Box::pin(async move {
                            let mut rows = conn.query(
                                "SELECT COUNT(*) FROM attention_pushes WHERE recipient = ?1 AND key LIKE 'condition:%'",
                                params![job_id.as_str()],
                            ).await?;
                            rows.next().await?.unwrap().i64(0)
                        })
                    }
                }).await.unwrap();
                if count == 1 { break; }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }).await.unwrap();

        let condition = list_wake_conditions(&db.local, &thread_id)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(condition.last_outcome.as_deref(), Some("matched"));
        crate::threads::db_store::remove(&thread_id).await.unwrap();
    }
}
