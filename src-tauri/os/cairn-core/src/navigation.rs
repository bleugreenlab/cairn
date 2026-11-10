//! Compact navigation reads over the databases this runner can currently read.
//! A database handle is an authority; project rows in a different scope are not.
use crate::db::DbState;
use crate::models::SearchFilters;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use cairn_uri::{build_issue_uri, build_thread_uri};
use serde::Serialize;

const NAVIGATION_METADATA_SQL: &str = "WITH metadata AS (
                SELECT 'issue' AS kind, i.id, p.id AS project_id, p.key AS project_key,
                       i.title, i.number, NULL AS name, i.updated_at,
                       'p/' || p.key || '/' || i.number AS uri
                  FROM issues i JOIN projects p ON p.id = i.project_id
                 WHERE p.id IN (SELECT value FROM json_each(?1))
                UNION ALL
                SELECT 'thread', t.id, p.id, p.key, t.name,
                       NULL, t.name, t.updated_at,
                       'p/' || p.key || '/' || t.name
                  FROM threads t JOIN projects p ON p.id = t.project_id
                 WHERE p.id IN (SELECT value FROM json_each(?1))
            ), matches AS (
                SELECT *, CASE
                    WHEN (?5 AND (lower(uri) = ?6 OR lower(project_key || '/' || COALESCE(name, number)) = ?6))
                      OR lower(name) = ?2 OR CAST(number AS TEXT) = ?2 OR lower(title) = ?2 THEN 3
                    WHEN substr(lower(title), 1, length(?2)) = ?2
                      OR substr(lower(name), 1, length(?2)) = ?2 THEN 2
                    WHEN instr(lower(title), ?2) > 0 OR instr(lower(name), ?2) > 0 OR instr(replace(lower(name), '-', ' '), ?2) > 0
                      OR (?5 AND instr(lower(uri), ?6) > 0)
                      OR (kind = 'issue' AND id IN (SELECT value FROM json_each(?3))) THEN 1
                    ELSE 0 END AS match_rank
                  FROM metadata
                 WHERE instr(lower(title), ?2) > 0 OR instr(lower(name), ?2) > 0 OR instr(replace(lower(name), '-', ' '), ?2) > 0
                    OR (?5 AND instr(lower(uri), ?6) > 0)
                    OR (kind = 'issue' AND id IN (SELECT value FROM json_each(?3)))
            )
            SELECT kind, id, project_id, project_key, title, number, name, updated_at, match_rank
              FROM matches ORDER BY match_rank DESC, updated_at DESC, project_id, kind, id LIMIT ?4";

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NavigationMetadataResult {
    pub kind: String,
    pub id: String,
    pub project_id: String,
    pub project_key: String,
    pub title: String,
    pub uri: String,
    pub issue_number: Option<i32>,
    pub thread_name: Option<String>,
    pub updated_at: i64,
    pub match_rank: i32,
    pub team_id: Option<String>,
}

/// Scope index retrieval BEFORE its result limit. Private workspace posts use
/// the empty project id; replicas must never claim that global namespace.
pub(crate) async fn project_authority(
    db: &LocalDb,
    team_id: Option<&str>,
) -> Result<Vec<String>, DbError> {
    match team_id {
        Some(team) => {
            db.query_all(
                "SELECT id FROM projects WHERE team_id = ?1 ORDER BY id",
                params![team],
                |row| row.text(0),
            )
            .await
        }
        None => {
            db.query_all(
                "SELECT p.id FROM projects p
             WHERE instr(p.id, '~') = 0
               AND NOT EXISTS (
                   SELECT 1 FROM project_routes r
                   WHERE lower(r.project_key) = lower(p.key) AND r.team_id IS NOT NULL
               )
             ORDER BY p.id",
                (),
                |row| row.text(0),
            )
            .await
        }
    }
}

/// Bounded metadata discovery, with no status or recency exclusion. The existing
/// title index contributes forgiving text matches; live SQL metadata supplies
/// identifiers, partial paths and thread fields absent from the content index.
pub async fn search_navigation_metadata(
    dbs: &DbState,
    query: &str,
    limit: Option<usize>,
) -> Result<Vec<NavigationMetadataResult>, String> {
    let query = query.trim().trim_start_matches('#');
    let limit = limit.unwrap_or(50).min(100);
    if query.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }
    if query.len() > 512 {
        return Err("Navigation query exceeds 512 bytes".into());
    }
    let mut out = Vec::new();
    for (team_id, db) in dbs.navigation_dbs().await {
        let row_team_id = team_id.clone();
        let result: Result<Vec<NavigationMetadataResult>, String> = async {
            let projects = project_authority(&db, team_id.as_deref())
                .await
                .map_err(|e| e.to_string())?;
            let mut ingest_projects = projects.clone();
            if team_id.is_none() {
                ingest_projects.push(String::new());
            }
            dbs.search_index
                .try_apply_pending_in_projects(&db, &ingest_projects)
                .await
                .map_err(|e| e.to_string())?;
            let indexed = dbs
                .search_index
                .search_in_projects(
                    query,
                    Some(SearchFilters {
                        title_only: true,
                        content_types: Some(vec!["issue".into()]),
                        limit: Some(100),
                        ..Default::default()
                    }),
                    &projects,
                )
                .map_err(|e| e.to_string())?;
            let ids = serde_json::to_string(&indexed.iter().map(|hit| &hit.id).collect::<Vec<_>>())
                .map_err(|e| e.to_string())?;
            let projects_json = serde_json::to_string(&projects).map_err(|e| e.to_string())?;
            let needle = query.to_lowercase();
            let address = needle.strip_prefix("cairn://").unwrap_or(&needle);
            let address_query = needle.contains('/') || needle.chars().all(|c| c.is_ascii_digit());
            let rows = db
                .query_all(
                    NAVIGATION_METADATA_SQL,
                    params![
                        projects_json.as_str(),
                        needle.as_str(),
                        ids.as_str(),
                        limit as i64,
                        address_query,
                        address
                    ],
                    move |row| {
                        let project_key = row.text(3)?;
                        let issue_number = row.opt_i64(5)?.map(|n| n as i32);
                        let thread_name = row.opt_text(6)?;
                        let uri = match (issue_number, thread_name.as_deref()) {
                            (Some(number), _) => build_issue_uri(&project_key, number),
                            (_, Some(name)) => build_thread_uri(&project_key, name),
                            _ => return Err(DbError::Row("Navigation row has no address".into())),
                        };
                        Ok(NavigationMetadataResult {
                            kind: row.text(0)?,
                            id: row.text(1)?,
                            project_id: row.text(2)?,
                            project_key,
                            title: row.text(4)?,
                            uri,
                            issue_number,
                            thread_name,
                            updated_at: row.i64(7)?,
                            match_rank: row.i64(8)? as i32,
                            team_id: row_team_id.clone(),
                        })
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
            Ok(rows)
        }
        .await;
        dbs.record_navigation_failure(
            "metadata",
            team_id.as_deref(),
            result.as_ref().err().cloned(),
        )
        .await;
        if let Ok(rows) = result {
            out.extend(rows);
        }
    }
    out.sort_by(|a, b| {
        b.match_rank
            .cmp(&a.match_rank)
            .then(b.updated_at.cmp(&a.updated_at))
            .then(a.team_id.cmp(&b.team_id))
            .then(a.project_id.cmp(&b.project_id))
            .then(a.kind.cmp(&b.kind))
            .then(a.id.cmp(&b.id))
    });
    out.truncate(limit);
    Ok(out)
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NavigationPassage {
    pub event_id: String,
}

/// Resolve an exact canonical passage without reading transcript bodies. A
/// project id is an authority selector, never a routing hint that can fall back.
pub async fn resolve_navigation_passage(
    dbs: &DbState,
    uri: &str,
    project_id: Option<&str>,
) -> Result<Option<NavigationPassage>, String> {
    use cairn_uri::{parse_uri, CairnResource};
    let parsed = parse_uri(uri).ok_or_else(|| "Invalid canonical passage URI".to_string())?;
    let parsed = cairn_uri::delegate_thread_descendant(parsed)?;
    let (project, number, exec_seq, node, task, turn, event) = match parsed {
        CairnResource::NodeChatTurn {
            project,
            number,
            exec_seq,
            node_id,
            turn_seq,
        } => (
            project,
            number,
            exec_seq,
            node_id,
            None,
            Some(turn_seq),
            None,
        ),
        CairnResource::TaskChatTurn {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            turn_seq,
        } => (
            project,
            number,
            exec_seq,
            node_id,
            Some(task_name),
            Some(turn_seq),
            None,
        ),
        CairnResource::NodeChatEvent {
            project,
            number,
            exec_seq,
            node_id,
            run_seq,
            event_seq,
        } => (
            project,
            number,
            exec_seq,
            node_id,
            None,
            None,
            Some((run_seq, event_seq)),
        ),
        CairnResource::TaskChatEvent {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            run_seq,
            event_seq,
        } => (
            project,
            number,
            exec_seq,
            node_id,
            Some(task_name),
            None,
            Some((run_seq, event_seq)),
        ),
        _ => return Err("URI does not address a supported transcript passage".into()),
    };
    let mut authority = None;
    for (team_id, db) in dbs.navigation_dbs().await {
        let projects = project_authority(&db, team_id.as_deref())
            .await
            .map_err(|e| e.to_string())?;
        let projects = serde_json::to_string(&projects).map_err(|e| e.to_string())?;
        let found = db
            .query_text(
                "SELECT id FROM projects WHERE lower(key) = lower(?1)
             AND id IN (SELECT value FROM json_each(?2)) AND (?3 IS NULL OR id = ?3) LIMIT 1",
                params![project.as_str(), projects.as_str(), project_id],
            )
            .await
            .map_err(|e| e.to_string())?;
        if found.is_some() {
            if authority.is_some() {
                return Err("Ambiguous project key; supply projectId".into());
            }
            authority = Some(db);
        }
    }
    let Some(db) = authority else {
        return Ok(None);
    };
    let job = crate::jobs::queries::job_id_for_node_coordinate(
        &db,
        &project,
        number,
        exec_seq,
        &node,
        task.as_deref(),
    )
    .await
    .map_err(|e| e.to_string())?;
    let Some(job) = job else {
        return Ok(None);
    };
    crate::resources::navigation_passage_event_id(&db, &job, turn, event, task.is_some())
        .await
        .map(|id| id.map(|event_id| NavigationPassage { event_id }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SearchIndex;
    use std::sync::Arc;

    #[tokio::test]
    async fn retained_private_outboxes_cannot_overwrite_or_delete_team_documents() {
        let dbs = state().await;
        seed(&dbs.local, true).await;
        let team = team_db("authoritative-ingestion").await;
        team.execute_script("UPDATE issues SET description = 'ownervocabulary' WHERE id = 'old';")
            .await
            .unwrap();
        dbs.register_team_db_for_test("team-a".into(), team).await;
        assert_eq!(
            crate::search::search_content_across(&dbs, "ownervocabulary", None, None)
                .await
                .unwrap()
                .len(),
            1
        );
        dbs.local
            .execute_script(
                "UPDATE issues SET description = 'retainedcopyvocabulary' WHERE id = 'old';",
            )
            .await
            .unwrap();
        assert!(
            crate::search::search_content_across(&dbs, "retainedcopyvocabulary", None, None)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            crate::search::search_content_across(&dbs, "ownervocabulary", None, None)
                .await
                .unwrap()
                .len(),
            1
        );
        dbs.local
            .execute_script("DELETE FROM issues WHERE id = 'old';")
            .await
            .unwrap();
        assert_eq!(
            crate::search::search_content_across(&dbs, "ownervocabulary", None, None)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    async fn state() -> DbState {
        let local = Arc::new(crate::storage::migrated_test_db("navigation").await);
        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        DbState::new(local, index)
    }

    async fn team_db(name: &str) -> Arc<LocalDb> {
        let db = LocalDb::open(
            tempfile::tempdir()
                .unwrap()
                .keep()
                .join(format!("{name}.db")),
        )
        .await
        .unwrap();
        crate::storage::MigrationRunner::new(crate::storage::TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db.execute_script(
            "INSERT INTO teams(id, name, created_at, updated_at) VALUES ('team-a', 'Team', 1, 1);
             INSERT INTO projects(id, team_id, name, key, repo_path, created_at, updated_at)
             VALUES ('team-a~00000000-0000-4000-8000-000000000001', 'team-a', 'Project', 'alpha', '/tmp/nav', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('old', 'team-a~00000000-0000-4000-8000-000000000001', 914, 'Ancient inference engine', 'closed', 1, 1),
                    ('new', 'team-a~00000000-0000-4000-8000-000000000001', 915, 'Recent incidental result', 'active', 200, 200);
             INSERT INTO threads(id, project_id, name, status, created_at, updated_at)
             VALUES ('th', 'team-a~00000000-0000-4000-8000-000000000001', 'stable-name', 'closed', 1, 1);",
        )
        .await
        .unwrap();
        Arc::new(db)
    }

    async fn seed(db: &LocalDb, team: bool) {
        let project = if team {
            "team-a~00000000-0000-4000-8000-000000000001"
        } else {
            "p"
        };
        db.execute_script(format!(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('{project}', 'default', 'Project', 'alpha', '/tmp/nav', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('old', '{project}', 914, 'Ancient inference engine', 'closed', 1, 1),
                    ('new', '{project}', 915, 'Recent incidental result', 'active', 200, 200);
             INSERT INTO threads(id, project_id, name, status, created_at, updated_at)
             VALUES ('th', '{project}', 'stable-name', 'closed', 1, 1);"
        ))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn metadata_finds_old_closed_titles_names_numbers_and_paths() {
        let dbs = state().await;
        seed(&dbs.local, false).await;
        for query in [
            "ancient",
            "inference eng",
            "914",
            "#914",
            "alpha/914",
            "cairn://p/alpha/914",
            "p/alpha/91",
        ] {
            let results = search_navigation_metadata(&dbs, query, None).await.unwrap();
            assert!(
                results.iter().any(|r| r.id == "old"),
                "missing old issue for {query}"
            );
        }
        for query in ["stable name", "stable-na", "alpha/stable"] {
            let results = search_navigation_metadata(&dbs, query, None).await.unwrap();
            assert!(
                results.iter().any(|r| r.id == "th"
                    && r.title == "stable-name"
                    && r.thread_name.as_deref() == Some("stable-name")),
                "missing thread for {query}"
            );
        }
        assert!(search_navigation_metadata(&dbs, "%_", None)
            .await
            .unwrap()
            .is_empty());
        assert!(search_navigation_metadata(&dbs, "", None)
            .await
            .unwrap()
            .is_empty());
        assert!(search_navigation_metadata(&dbs, "a", Some(0))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            search_navigation_metadata(&dbs, "a", Some(1))
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            search_navigation_metadata(&dbs, "914", Some(1))
                .await
                .unwrap()[0]
                .match_rank,
            3
        );
    }

    #[tokio::test]
    async fn metadata_does_not_match_scheme_and_retains_old_literal_and_stem_hits() {
        let dbs = state().await;
        seed(&dbs.local, false).await;
        dbs.local
            .execute_script(
                "UPDATE issues SET title = 'Old sailing boats' WHERE id = 'old';
             UPDATE issues SET title = 'Zulu' WHERE id = 'new';
             WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x<100)
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
             SELECT 'noise-'||x,'p',2000+x,'Zulu','active',1000+x,1000+x FROM seq;",
            )
            .await
            .unwrap();
        for query in ["ai", "c", "://"] {
            let rows = search_navigation_metadata(&dbs, query, None).await.unwrap();
            assert!(
                !rows.iter().any(|r| r.id == "new"),
                "scheme match for {query}"
            );
        }
        for query in ["ai", "sailing", "sail", "alpha/914", "cairn://p/alpha/914"] {
            let rows = search_navigation_metadata(&dbs, query, Some(1))
                .await
                .unwrap();
            assert_eq!(rows[0].id, "old", "old closed hit for {query}");
            assert!(
                rows[0].match_rank > 0,
                "trusted title index hit needs positive rank"
            );
        }
    }

    #[tokio::test]
    async fn metadata_exact_numbers_remain_distinct_across_projects() {
        let dbs = state().await;
        seed(&dbs.local, false).await;
        dbs.local
            .execute_script(
                "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES ('q','default','Other','beta','/tmp/beta',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
             VALUES ('other','q',914,'Entirely different','closed',1,1);",
            )
            .await
            .unwrap();
        let rows = search_navigation_metadata(&dbs, "914", None).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.match_rank == 3));
        assert_ne!(rows[0].uri, rows[1].uri);
        let rows = search_navigation_metadata(&dbs, "beta/914", None)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "other");
    }

    #[tokio::test]
    async fn index_authority_is_applied_before_top_docs_and_empty_means_empty() {
        let dbs = state().await;
        seed(&dbs.local, false).await;
        dbs.local.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('q', 'default', 'Other', 'other', '/tmp/other', 1, 1);
             INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
             VALUES ('unscoped-first', 'q', 1, 'ancient', 'ancient ancient ancient', 200, 200);"
        ).await.unwrap();
        dbs.search_index.apply_pending(&dbs.local).await.unwrap();
        let filter = Some(SearchFilters {
            limit: Some(1),
            ..Default::default()
        });
        assert_eq!(
            dbs.search_index.search("ancient", filter.clone()).unwrap()[0].project_id,
            "q",
            "fixture must put an unauthorized candidate before the allowed result"
        );
        assert!(dbs
            .search_index
            .search_in_projects("ancient", filter.clone(), &[])
            .unwrap()
            .is_empty());
        assert!(dbs
            .search_index
            .search_in_projects("ancient", filter.clone(), &["absent".into()])
            .unwrap()
            .is_empty());
        assert_eq!(
            dbs.search_index
                .search_in_projects("ancient", filter, &["p".into()])
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn passage_resolves_exact_turn_and_run_event_and_refuses_unknown_authority() {
        let dbs = state().await;
        seed(&dbs.local, false).await;
        dbs.local.execute_script(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('e', 'r', 'old', 'p', 'running', 1, 1);
             INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('j', 'e', 'old', 'p', 'Builder', 'builder', 'running', 1, 1);
             INSERT INTO jobs(id, execution_id, parent_job_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('task', 'e', 'j', 'old', 'p', 'Survey', 'survey', 'running', 1, 1);
             INSERT INTO runs(id, job_id, issue_id, project_id, session_id, created_at, updated_at)
             VALUES ('r1', 'j', 'old', 'p', 's1', 1, 1),
                    ('r2', 'j', 'old', 'p', 's2', 2, 2),
                    ('rt', 'task', 'old', 'p', 'st', 1, 1);
             INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t1', 's1', 'r1', 'j', 1, 'complete', 'initial', 1, 1),
                    ('t2', 's2', 'r2', 'j', 1, 'complete', 'initial', 2, 2),
                    ('tt', 'st', 'rt', 'task', 1, 'complete', 'initial', 1, 1);
             INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at, turn_id)
             VALUES ('event-first', 'r1', 's1', 3, 1, 'user', '{}', 1, 't1'),
                    ('event-second-session', 'r2', 's2', 7, 2, 'assistant', '{}', 2, 't2'),
                    ('event-task', 'rt', 'st', 2, 1, 'assistant', '{}', 1, 'tt');"
        ).await.unwrap();
        let turn = "cairn://p/alpha/914/1/builder/chat/turn/1";
        assert_eq!(
            resolve_navigation_passage(&dbs, turn, None)
                .await
                .unwrap()
                .unwrap()
                .event_id,
            "event-first"
        );
        assert_eq!(
            resolve_navigation_passage(&dbs, "cairn://p/alpha/914/1/builder/chat/2/7", Some("p"))
                .await
                .unwrap()
                .unwrap()
                .event_id,
            "event-second-session"
        );
        let task_turn = "cairn://p/alpha/914/1/builder/task/survey/chat/turn/1";
        assert_eq!(
            resolve_navigation_passage(&dbs, task_turn, None)
                .await
                .unwrap()
                .unwrap()
                .event_id,
            "event-task"
        );
        assert!(resolve_navigation_passage(&dbs, turn, Some("missing"))
            .await
            .unwrap()
            .is_none());
        assert!(resolve_navigation_passage(
            &dbs,
            "cairn://p/alpha/914/1/builder/chat/turn/99",
            None
        )
        .await
        .unwrap()
        .is_none());
        dbs.local.execute_script(
            "INSERT INTO runs(id, job_id, issue_id, project_id, session_id, created_at, updated_at)
             VALUES ('rt2', 'task', 'old', 'p', 'st2', 2, 2);"
        ).await.unwrap();
        assert!(resolve_navigation_passage(&dbs, task_turn, None)
            .await
            .unwrap_err()
            .contains("multiple sessions"));
        assert_eq!(
            resolve_navigation_passage(
                &dbs,
                "cairn://p/alpha/914/1/builder/task/survey/chat/1/2",
                None
            )
            .await
            .unwrap()
            .unwrap()
            .event_id,
            "event-task"
        );
    }

    #[tokio::test]
    async fn workflow_child_root_tasks_resolve_turn_and_run_passages() {
        let dbs = state().await;
        seed(&dbs.local, false).await;
        dbs.local.execute_script(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('e', 'r', 'old', 'p', 'running', 1, 1);
             INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('launcher', 'e', 'old', 'p', 'Launcher', 'launcher', 'running', 1, 1);
             INSERT INTO jobs(id, execution_id, parent_job_id, issue_id, project_id, agent_config_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('workflow', 'e', 'launcher', 'old', 'p', 'workflow', 'Workflow', 'subflow', 'running', 1, 1),
                    ('task', 'e', 'workflow', 'old', 'p', NULL, 'Survey', 'survey', 'running', 1, 1);
             INSERT INTO runs(id, job_id, issue_id, project_id, session_id, created_at, updated_at)
             VALUES ('rt', 'task', 'old', 'p', 'st', 1, 1);
             INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('tt', 'st', 'rt', 'task', 1, 'complete', 'initial', 1, 1);
             INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at, turn_id)
             VALUES ('workflow-task-event', 'rt', 'st', 2, 1, 'assistant', '{}', 1, 'tt');"
        ).await.unwrap();
        for suffix in ["turn/1", "1/2"] {
            let uri = format!("cairn://p/alpha/914/1/subflow/task/survey/chat/{suffix}");
            assert_eq!(
                resolve_navigation_passage(&dbs, &uri, Some("p"))
                    .await
                    .unwrap()
                    .unwrap()
                    .event_id,
                "workflow-task-event"
            );
        }
    }
}
