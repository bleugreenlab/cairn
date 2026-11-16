//! First-class thread resource rendering and migration-address resolution.
//! A thread's sub-path never reaches this module raw: read, write and `run`
//! first normalize it with `cairn_uri::delegate_thread_descendant`, which maps
//! it onto the node-family resource that answers for it.

use super::common::{lookup_project_by_key, read_resource};
use cairn_db::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

pub(super) async fn resolve_thread_id(
    db: &LocalDb,
    project_key: &str,
    name: &str,
) -> Result<Option<String>, String> {
    let project_key = project_key.to_string();
    let name = name.to_string();
    db.read(move |conn| Box::pin(async move {
        let mut rows = conn.query(
            "SELECT t.id FROM threads t JOIN projects p ON p.id = t.project_id WHERE LOWER(p.key) = LOWER(?1) AND t.name = ?2 LIMIT 1",
            params![project_key, name],
        ).await?;
        Ok(match rows.next().await? {
            Some(row) => Some(row.get::<String>(0)?),
            None => None,
        })
    })).await.map_err(|error| format!("Failed to resolve thread: {error}"))
}

/// Canonical text queue; callers supply the workspace focus bound.
pub(crate) async fn read_thread_queue(
    db: &LocalDb,
    project_key: &str,
    thread: &str,
    now: i64,
    limit: i64,
) -> String {
    let project_key = project_key.to_owned();
    let thread = thread.to_owned();
    read_resource(db, move |conn| {
        let (project_key, thread) = (project_key.clone(), thread.clone());
        Box::pin(async move {
            let project = match lookup_project_by_key(conn, &project_key).await {
                Ok(project) => project,
                Err(error) => return Ok(error),
            };
            let thread_id = match crate::storage::query_opt_text_conn(
                conn,
                "SELECT id FROM threads WHERE project_id = ?1 AND name = ?2",
                params![project.project_id.as_str(), thread.as_str()],
            )
            .await
            {
                Ok(Some(thread_id)) => thread_id,
                Ok(None) => return Ok(format!("Thread '{thread}' not found")),
                Err(error) => return Ok(format!("Failed to load thread '{thread}': {error}")),
            };
            let queue =
                match crate::execution::gates::reorder::thread_queue_conn(conn, &thread_id).await {
                    Ok(queue) => queue,
                    Err(error) => return Ok(format!("Failed to load {thread}'s queue: {error}")),
                };
            let standings = crate::execution::gates::census::focus_standings_conn(conn, &thread_id).await.map_err(|e| e.to_string())?;
            let mut rows = conn.query("SELECT id, number, title FROM issues WHERE parent_thread_id = ?1 ORDER BY number", params![thread_id.as_str()]).await.map_err(|e| e.to_string())?;
            let mut listed = Vec::new();
            while let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
                let id = row.text(0).map_err(|e| e.to_string())?;
                if let Some(standing) = standings.get(&id) {
                    listed.push((row.i64(1).map_err(|e| e.to_string())?, row.text(2).map_err(|e| e.to_string())?, standing.clone()));
                }
            }
            listed.sort_by_key(|(number, _, standing)| (standing.tier(), match standing {
                crate::execution::gates::census::FocusStanding::Queued { place, .. } => place.unwrap_or(usize::MAX),
                _ => *number as usize,
            }));
            let running = listed.iter().filter(|(_, _, s)| s.tier() == 0).count();
            let mut out = format!("# Queue — {thread}\n\nRunning {running}/{}\n\n", limit.max(1));
            for (heading, tiers) in [("Running", vec![0]), ("Queued", vec![1, 2]), ("Held by open-PR bound", vec![3]), ("Parked", vec![4]), ("Stopped", vec![5])] {
                out.push_str(&format!("## {heading}\n\n"));
                let mut count = 0;
                for (number, title, standing) in &listed {
                    if !tiers.contains(&standing.tier()) { continue; }
                    count += 1;
                    out.push_str(&format!("- [{number}](cairn://p/{project_key}/{number}) {title} · {}\n", standing.line(now)));
                }
                if count == 0 { out.push_str("None.\n"); }
                out.push('\n');
            }
            // Preserve the reorder's per-cap placement detail and operator provenance.
            if !queue.is_empty() { out.push_str(&render_thread_queue(&project_key, &thread, &queue, now)); }
            Ok(out)
        })
    })
    .await
    .unwrap_or_else(|error| error)
}

fn render_thread_queue(
    project_key: &str,
    thread: &str,
    queue: &[crate::execution::gates::reorder::ThreadQueued],
    now: i64,
) -> String {
    let mut out = format!("## Fresh release-order detail — {thread}\n\n");
    if queue.is_empty() {
        out.push_str("No cap-held executions.\n");
        return out;
    }
    for (index, queued) in queue.iter().enumerate() {
        let mut parts = vec![format!(
            "[{number}](cairn://p/{project_key}/{number}) {title}",
            number = queued.issue_number,
            title = queued.issue_title
        )];
        let places = crate::execution::gates::reorder::places_phrase(queued, " · ");
        if !places.is_empty() {
            parts.push(places);
        }
        parts.push(format!(
            "held {}",
            crate::clock::format_elapsed((now - queued.held_since).max(0))
        ));
        if let Some(by) = queued.placed_by() {
            parts.push(format!("placed ahead by {by}"));
        }
        out.push_str(&format!("{}. {}\n", index + 1, parts.join(" · ")));
    }
    out
}

pub(super) async fn resolve_migrated_thread_uri(
    db: &LocalDb,
    identity: &str,
) -> Result<Option<(String, String)>, String> {
    let parts: Vec<&str> = identity
        .strip_prefix("cairn://p/")
        .unwrap_or("")
        .split('/')
        .collect();
    if parts.len() < 2 {
        return Ok(None);
    }
    let Ok(_number) = parts[1].parse::<i64>() else {
        return Ok(None);
    };
    // The alias is a best-effort fallback: a project (or thread) that does not
    // resolve means "no alias here", never a new failure path. Erroring out
    // would preempt the canonical handling downstream — e.g. the contract
    // gate's "Unsupported resource mutation" rejection, which must win even
    // when the database knows nothing about the project in the URI.
    let alias = format!("cairn://p/{}/{}", parts[0], parts[1]);
    let lookup = alias.clone();
    let resolved = read_resource(db, move |conn| {
        Box::pin(async move {
            crate::threads::resolve_parent_thread_uri_conn(conn, &lookup)
                .await
                .map_err(|e| e.to_string())
        })
    })
    .await?;
    let Some((_, _, name)) = resolved else {
        return Ok(None);
    };
    let canonical = format!("cairn://p/{}/{}", parts[0], name);
    let mapped = match parts.as_slice() {
        [_, _, _, "thread", tail @ ..] if !tail.is_empty() => {
            format!("{canonical}/{}", tail.join("/"))
        }
        _ => canonical.clone(),
    };
    Ok(Some((mapped, canonical)))
}

pub(super) async fn read_project_threads(db: &LocalDb, project_key: &str) -> String {
    let project_key = project_key.to_owned();
    read_resource(db, move |conn| {
        Box::pin(async move {
            let project_key = project_key.as_str();
            let project = match lookup_project_by_key(conn, project_key).await {
                Ok(v) => v,
                Err(e) => return Ok(e),
            };
            // Active only. This collection is what an agent reads to find the threads it
            // can address, so a closed thread must not appear in it — while its own
            // canonical URI keeps resolving, transcript and children intact, so it can be
            // read and reopened.
            let mut rows = match conn.query("SELECT name, jurisdiction, status, attention, updated_at FROM threads WHERE project_id = ?1 AND status = 'active' ORDER BY name", params![project.project_id.as_str()]).await {
                Ok(v) => v, Err(e) => return Ok(format!("Failed to load project threads: {e}"))
            };
            let mut out = format!("# Threads — {}\n\n", project.project_key);
            let mut count = 0;
            while let Ok(Some(row)) = rows.next().await {
                let (Ok(name), Ok(jurisdiction), Ok(status), Ok(attention), Ok(updated_at)) = (
                    row.text(0),
                    row.opt_text(1),
                    row.text(2),
                    row.text(3),
                    row.i64(4),
                ) else {
                    continue;
                };
                count += 1;
                let last_activity = crate::clock::age(updated_at);
                out.push_str(&format!("- [{name}](cairn://p/{}/{name})\n  {}Status: {status}. Attention: {attention}. Last activity: {last_activity}.\n", project.project_key, jurisdiction.map(|v| format!("Jurisdiction: {v}. ")).unwrap_or_default()));
            }
            if count == 0 {
                out.push_str("No threads.\n");
            }
            Ok(out)
        })
    })
    .await
    .unwrap_or_else(|error| error)
}

/// What appending to a thread does to its session, said where a caller decides
/// to append. Thread messages are delivered as `DeliveryUrgency::Steer`: an
/// idle (or not yet started) session is woken and the message starts its turn;
/// a running turn receives it at its next tool or turn boundary.
fn thread_delivery_semantics(status: &str) -> &'static str {
    if status == "closed" {
        "Messaging: this thread is closed, so an append is refused until it is patched back to `status:\"active\"`."
    } else {
        "Messaging: append `{content}` to this URI to message the thread's session. An idle session starts a turn on the message at once (a session not yet started is started); a running turn receives it at its next tool call or turn boundary, without being interrupted."
    }
}

/// Render a thread's overview.
///
/// Only the overview: every addressable descendant is normalized onto its
/// node-family resource by [`cairn_uri::delegate_thread_descendant`] before dispatch
/// reaches here, so there is no sub-path interpretation left in this function.
///
/// `default_open_children` is the workspace's limit on a thread's open
/// children (the execution queue's focus cap, `maxOpenChildrenPerThread`).
pub(super) async fn read_thread(
    db: &LocalDb,
    project_key: &str,
    name: &str,
    path: &[String],
    default_open_children: i64,
) -> String {
    let project_key = project_key.to_owned();
    let name = name.to_owned();
    let path = path.to_vec();
    read_resource(db, move |conn| {
        Box::pin(async move {
            let project_key = project_key.as_str();
            let name = name.as_str();
            let path = path.as_slice();
            let project = match lookup_project_by_key(conn, project_key).await {
                Ok(v) => v,
                Err(e) => return Ok(e),
            };
            let mut rows = match conn.query("SELECT id, jurisdiction, status, attention, definition FROM threads WHERE project_id = ?1 AND name = ?2 LIMIT 1", params![project.project_id.as_str(), name]).await {
                Ok(v) => v, Err(e) => return Ok(format!("Failed to load thread: {e}"))
            };
            let Some(row) = rows.next().await.ok().flatten() else {
                return Ok(format!("Thread '{}' not found in {}", name, project.project_key));
            };
            let (Ok(id), Ok(jurisdiction), Ok(status), Ok(attention), Ok(definition)) = (
                row.text(0),
                row.opt_text(1),
                row.text(2),
                row.text(3),
                row.opt_text(4),
            ) else {
                return Ok("Failed to decode thread".into());
            };
            let mut jobs = match conn
                .query(
                    &format!(
                        "SELECT j.id, j.uri_segment, j.status FROM jobs j
                         WHERE j.thread_id = ?1 AND {}
                         ORDER BY j.created_at DESC, j.rowid DESC LIMIT 1",
                        crate::threads::SESSION_JOB_SHAPE
                    ),
                    params![id.as_str()],
                )
                .await
            {
                Ok(v) => v,
                Err(e) => return Ok(format!("Failed to load thread session: {e}")),
            };
            let session = jobs
                .next()
                .await
                .ok()
                .flatten()
                .and_then(|row| Some((row.text(0).ok()?, row.opt_text(1).ok()?, row.text(2).ok()?)));
            // Every addressable descendant was normalized onto its node-family resource
            // by `cairn_uri::delegate_thread_descendant` before dispatch reached here, so anything
            // still arriving with a path is a shape nothing answers for.
            if !path.is_empty() {
                return Ok(format!(
                    "Unknown thread session resource '{}'. Read {} for the thread overview.",
                    path.join("/"),
                    cairn_uri::build_thread_uri(&project.project_key, name)
                ));
            }
            let canonical = format!("cairn://p/{}/{}", project.project_key, name);
            let mut out = format!("# {name}\n\n");
            if let Some(jurisdiction) = jurisdiction {
                out.push_str(&format!("Jurisdiction: {jurisdiction}\n\n"));
            }
            out.push_str(&format!("Status: {status} · Attention: {attention}\n\n"));
            if let Some(definition) = definition {
                out.push_str(&format!("Definition: {definition}\n\n"));
            }
            let owner = crate::fleet::placement_default::DefaultOwner::Thread(id.clone());
            if let Ok(Some(default)) =
                crate::fleet::placement_default::load_conn(conn, &owner).await
            {
                out.push_str(&format!("{}\n\n", default.line()));
            }
            out.push_str(&format!(
                "- [intent]({canonical}/db/intent/1) · [journal]({canonical}/db/journal)\n"
            ));
            let read_through = match crate::threads::headlines::cursor_conn(conn, &id).await {
                Ok(cursor) => crate::threads::headlines::read_through_line(
                    cursor.as_ref(),
                    chrono::Utc::now().timestamp(),
                ),
                Err(error) => format!("How far the operator has read is unknown: {error}"),
            };
            out.push_str(&format!(
                "- [headlines]({canonical}/db/headlines) · {read_through}\n"
            ));
            match session {
                Some((job_id, segment, session_status)) => out.push_str(&format!(
                    "- Live session: {}{} ({})\n",
                    job_id,
                    segment.map(|v| format!(" / {v}")).unwrap_or_default(),
                    session_status
                )),
                None => out.push_str("- Live session: not started\n"),
            }
            out.push_str(&format!("\n{}\n", thread_delivery_semantics(&status)));
            let mut children = match conn
                .query(
                    "SELECT i.id, i.number, i.title, i.status
                       FROM issues i WHERE i.parent_thread_id = ?1 ORDER BY i.number",
                    params![id.as_str()],
                )
                .await
            {
                Ok(v) => v,
                Err(_) => return Ok(out),
            };
            let mut listed = Vec::new();
            while let Ok(Some(row)) = children.next().await {
                let (Ok(issue_id), Ok(number), Ok(title), Ok(status)) =
                    (row.text(0), row.i64(1), row.text(2), row.text(3))
                else {
                    continue;
                };
                listed.push((issue_id, number, title, status));
            }
            drop(children);
            let ids: Vec<String> = listed.iter().map(|(id, ..)| id.clone()).collect();
            let mut held = crate::execution::gates::census::gated_executions_for_issues_conn(
                conn, &ids,
            )
            .await
            .unwrap_or_default();

            out.push_str("\n## Children\n\n");
            let standings = crate::execution::gates::census::focus_standings_conn(conn, &id).await.map_err(|e| e.to_string())?;
            let limit = default_open_children.max(1);
            if !listed.is_empty() {
                let running = standings.values().filter(|s| s.tier() == 0).count();
                let queued = standings.values().filter(|s| matches!(s, crate::execution::gates::census::FocusStanding::Queued { .. } | crate::execution::gates::census::FocusStanding::HeldForPrs { .. })).count();
                let parked = standings.values().filter(|s| s.tier() == 4).count();
                out.push_str(&format!("Running {running}/{limit} · queued {queued} · parked {parked}\n\n"));
            }
            for (issue_id, number, title, status) in &listed {
                let standing_line = standings.get(issue_id).map(|s| format!(" · {}", s.line(chrono::Utc::now().timestamp()))).unwrap_or_default();
                let held_line = held
                    .remove(issue_id)
                    .and_then(|gated| gated.held.into_iter().next())
                    .map(|execution| format!(" · {}", execution.line))
                    .unwrap_or_default();
                let failed_line = if status == "failed" {
                    match cairn_attention::delivery::issue_failure_conn(conn, issue_id)
                        .await
                        .ok()
                        .flatten()
                    {
                        Some(failure) => format!(
                            " · Failed: {} — `{}`", failure.reason, failure.node_uri
                        ),
                        None => " · Failed".to_string(),
                    }
                } else {
                    String::new()
                };
                out.push_str(&format!(
                    "- [{number} — {title}](cairn://p/{}/{number}){failed_line}{standing_line}{held_line}\n",
                    project.project_key
                ));
            }
            if listed.is_empty() {
                out.push_str("None.\n");
            }
            Ok(out)
        })
    })
    .await
    .unwrap_or_else(|error| error)
}

#[cfg(test)]
mod children_tests {
    use crate::execution::gates::sweep::tests::{env, file};
    use serde_json::json;

    #[tokio::test]
    async fn failed_projection_children_name_failed_without_consuming_an_open_slot() {
        let env = env("thread-children-failed-projection.db").await;
        env.db
            .execute_script(
                "INSERT INTO threads(id, project_id, name, created_at, updated_at)
               VALUES('th', 'p', 'harness', 1, 1);
             UPDATE issues SET parent_thread_id = 'th', status = 'failed' WHERE id = 'i1';
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES('failed-exec','recipe','i1','p','failed',1,1);
             INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, uri_segment,
                              status, created_at, updated_at)
               VALUES('failed-job','failed-exec','i1','p','Builder','builder','failed',1,1);
             INSERT INTO runs(id, job_id, issue_id, status, error_message, created_at, updated_at)
               VALUES('failed-run','failed-job','i1','crashed','Launch refused',1,1);",
            )
            .await
            .unwrap();
        let page = super::read_thread(&env.db, "prj", "harness", &[], 4).await;
        assert!(page.contains("Running 0/4"), "{page}");
        assert!(page.contains("Failed"), "{page}");
        assert!(page.contains("Launch refused"), "{page}");
    }

    /// A thread's children read the queue's own line, and the open paragraph
    /// counts against the workspace limit.
    #[tokio::test]
    async fn the_children_section_reads_the_queued_line_and_the_open_count() {
        let env = env("thread-children-queue.db").await;
        let mut seed = String::from(
            "INSERT INTO threads(id, project_id, name, created_at, updated_at)
               VALUES('th', 'p', 'harness', 1, 1);",
        );
        for number in [100, 101, 102, 103, 105] {
            seed.push_str(&format!(
                "INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                                    parent_thread_id, created_at, updated_at)
                   VALUES('ci{number}', 'p', {number}, 'Child {number}', 'active', 'active',
                          'none', 'th', 1, 1);"
            ));
        }
        for number in 100..104 {
            seed.push_str(&format!(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                   VALUES('ce{number}', 'recipe', 'ci{number}', 'p', 'running', 1, 1);
                 INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status,
                                  created_at, updated_at)
                   VALUES('cj{number}', 'ce{number}', 'p', 'ci{number}', 'builder', 'running', 1, 1);"
            ));
        }
        env.db.execute_script(&seed).await.unwrap();
        file(&env, 105, json!([])).await.unwrap();
        // The sweep numbers its place in line.
        crate::execution::gates::sweep::sweep(&env.orch)
            .await
            .unwrap();

        let page = super::read_thread(&env.db, "prj", "harness", &[], 4).await;
        use crate::execution::gates::census::{
            focus_standings_conn, FocusParkCause, FocusStanding,
        };
        let standing = env
            .db
            .read(|conn| Box::pin(async move { focus_standings_conn(conn, "th").await }))
            .await
            .unwrap();
        for n in 100..104 {
            assert_eq!(
                standing.get(&format!("ci{n}")),
                Some(&FocusStanding::Running)
            );
        }
        assert!(matches!(
            standing.get("ci105"),
            Some(FocusStanding::Queued { reentry: false, .. })
        ));
        assert!(page.contains("Running 4/4"), "{page}");
        for (number, state, reason) in [
            (100, "interrupted", None),
            (101, "yielded", Some("wait")),
            (102, "yielded", Some("permission")),
        ] {
            let reason = reason
                .map(|r| format!("'{r}'"))
                .unwrap_or_else(|| "NULL".into());
            env.db.execute_script(format!(
                "INSERT INTO sessions(id, job_id, status, backend_id, created_at, updated_at)
                   VALUES('cs{number}', 'cj{number}', 'open', 'backend', 1, 1);
                 INSERT INTO runs(id,project_id,job_id,issue_id,status,session_id,created_at,updated_at)
                   VALUES('cr{number}','p','cj{number}','ci{number}','live','cs{number}',1,1);
                 INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,yield_reason,created_at,updated_at)
                   VALUES('ct{number}','cs{number}','cr{number}','cj{number}',1,'{state}',{reason},1,1);
                 UPDATE jobs SET current_turn_id='ct{number}' WHERE id='cj{number}';"
            )).await.unwrap();
        }
        env.db.execute_script(
            "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at,deadline_ms)
             VALUES('cw101','cj101','cr101','cs101','ct101','tool','{\"kind\":\"duration\"}','pending',1,1200001);"
        ).await.unwrap();
        let standing = env
            .db
            .read(|conn| Box::pin(async move { focus_standings_conn(conn, "th").await }))
            .await
            .unwrap();
        assert_eq!(standing.get("ci100"), Some(&FocusStanding::Stopped));
        assert!(matches!(
            standing.get("ci101"),
            Some(FocusStanding::Parked {
                cause: FocusParkCause::SleepingUntil(Some(1200001))
            })
        ));
        assert!(
            matches!(standing.get("ci102"), Some(FocusStanding::Parked { cause: FocusParkCause::OperatorAwaiting { kind, .. } }) if kind == "permission")
        );
        assert_eq!(standing.get("ci103"), Some(&FocusStanding::Running));
        env.db.execute_script(
            "UPDATE jobs SET status='pending' WHERE id IN ('cj100','cj101');
             INSERT INTO provider_limit_holds(id,job_id,backend,boundary_source,observed_text,state,created_at,updated_at,kind)
             VALUES('ch100','cj100','backend','thread_focus','focus full','active',5,5,'launch'),
                   ('ch101','cj101','backend','thread_open_prs','harness has 6/6 open PRs','active',6,6,'launch');
             UPDATE jobs SET status='complete' WHERE id='cj103';
             UPDATE executions SET status='complete' WHERE id='ce103';
             INSERT INTO merge_requests(id,job_id,project_id,issue_id,title,source_branch,target_branch,status,opened_at,updated_at,github_pr_number,checks_status)
             VALUES('cm103','cj103','p','ci103','PR','branch','main','open',1,1,5012,'SUCCESS');"
        ).await.unwrap();
        let standing = env
            .db
            .read(|conn| Box::pin(async move { focus_standings_conn(conn, "th").await }))
            .await
            .unwrap();
        assert!(matches!(
            standing.get("ci100"),
            Some(FocusStanding::Queued {
                reentry: true,
                place: Some(1),
                ..
            })
        ));
        assert!(matches!(
            standing.get("ci101"),
            Some(FocusStanding::HeldForPrs { .. })
        ));
        assert!(
            matches!(standing.get("ci103"), Some(FocusStanding::Parked { cause: FocusParkCause::PrAwaiting { number: Some(5012), lanes } }) if lanes == "merge")
        );
        env.db.execute_script(
            "UPDATE turns SET state='complete', yield_reason=NULL WHERE id='ct102';
             UPDATE jobs SET status='blocked' WHERE id='cj102';
             INSERT INTO artifacts(id,job_id,artifact_type,data,version,output_name,confirmed,created_at,updated_at)
             VALUES('ca102','cj102','plan','{}',1,'plan',0,500,500);"
        ).await.unwrap();
        let standing = env
            .db
            .read(|conn| Box::pin(async move { focus_standings_conn(conn, "th").await }))
            .await
            .unwrap();
        assert!(
            matches!(standing.get("ci102"), Some(FocusStanding::Parked { cause: FocusParkCause::PlanAwaiting { who, since: 500, .. } }) if who == "you")
        );
        // Keep all standing bands populated together, including a stopped
        // sibling beside a genuinely running job.
        env.db.execute_script(
            "INSERT INTO issues(id,project_id,number,title,status,progress,attention,parent_thread_id,created_at,updated_at)
             VALUES('ci106','p',106,'Running child','active','active','none','th',1,1),
                   ('ci107','p',107,'Stopped child','active','active','none','th',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
             VALUES('ce106','recipe','ci106','p','running',1,1),
                   ('ce107','recipe','ci107','p','running',1,1);
             INSERT INTO jobs(id,execution_id,project_id,issue_id,node_name,status,created_at,updated_at)
             VALUES('cj106','ce106','p','ci106','builder','running',1,1),
                   ('cj107','ce107','p','ci107','builder','running',1,1);
             INSERT INTO sessions(id,job_id,status,backend_id,created_at,updated_at)
             VALUES('cs107','cj107','open','backend',1,1);
             INSERT INTO runs(id,project_id,job_id,issue_id,status,session_id,created_at,updated_at)
             VALUES('cr107','p','cj107','ci107','live','cs107',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,created_at,updated_at)
             VALUES('ct107','cs107','cr107','cj107',1,'interrupted',1,1);
             UPDATE jobs SET current_turn_id='ct107' WHERE id='cj107';"
        ).await.unwrap();
        let standing = env
            .db
            .read(|conn| Box::pin(async move { focus_standings_conn(conn, "th").await }))
            .await
            .unwrap();
        assert_eq!(standing.get("ci106"), Some(&FocusStanding::Running));
        assert_eq!(standing.get("ci107"), Some(&FocusStanding::Stopped));
        let queue = super::read_thread_queue(&env.db, "prj", "harness", 1000, 4).await;
        let sections = [
            ("Running", vec![106]),
            ("Queued", vec![100, 105]),
            ("Held by open-PR bound", vec![101]),
            ("Parked", vec![102, 103]),
            ("Stopped", vec![107]),
        ];
        for (heading, numbers) in sections {
            let section = queue
                .split(&format!("## {heading}\n\n"))
                .nth(1)
                .unwrap_or_else(|| panic!("missing {heading}: {queue}"))
                .split("\n## ")
                .next()
                .unwrap();
            for number in numbers {
                assert!(
                    section.contains(&format!("cairn://p/prj/{number})")),
                    "{section}"
                );
                assert!(
                    section.contains(&standing[&format!("ci{number}")].line(1000)),
                    "{section}"
                );
            }
        }
        assert!(
            queue.find("cairn://p/prj/100)").unwrap() < queue.find("cairn://p/prj/105)").unwrap(),
            "{queue}"
        );
        let overview = super::read_thread(&env.db, "prj", "harness", &[], 4).await;
        assert!(
            overview.contains("Running 1/4 · queued 3 · parked 2"),
            "{overview}"
        );
        for (id, state) in &standing {
            let number = id.strip_prefix("ci").unwrap();
            let line = overview
                .lines()
                .find(|line| line.contains(&format!("cairn://p/prj/{number})")))
                .unwrap();
            let canonical = state.line(chrono::Utc::now().timestamp());
            assert!(line.contains(&canonical), "{line}");
        }
        super::super::thread_seed::assert_focus_text(&env.db, "th", &standing).await;

        // Three failed launches gate the re-entry: it leaves the band and
        // reads as stopped, needing the operator, with no place in line.
        env.db
            .execute_script(
                "UPDATE provider_limit_holds SET resume_gate='launch_failed',launch_attempts=3,updated_at=900 WHERE id='ch100';",
            )
            .await
            .unwrap();
        let standing = env
            .db
            .read(|conn| Box::pin(async move { focus_standings_conn(conn, "th").await }))
            .await
            .unwrap();
        assert!(
            matches!(standing.get("ci100"), Some(FocusStanding::NeedsOperator { reason, since: 900 }) if reason.contains("launch failed 3 times")),
            "{:?}",
            standing.get("ci100")
        );
        let queue = super::read_thread_queue(&env.db, "prj", "harness", 1000, 4).await;
        let stopped = queue
            .split("## Stopped\n\n")
            .nth(1)
            .unwrap()
            .split("\n## ")
            .next()
            .unwrap();
        assert!(
            stopped.contains("cairn://p/prj/100)")
                && stopped.contains(&standing["ci100"].line(1000)),
            "{queue}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fixture() -> LocalDb {
        let db = crate::storage::migrated_test_db("thread-resources.db").await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p-threads', 'default', 'Threads', 'thr', '/tmp/threads', 1, 1);
             INSERT INTO threads (id, project_id, name, jurisdiction, migrated_from_number, created_at, updated_at)
             VALUES ('t-design', 'p-threads', 'design-review', 'Own architecture decisions', 3404, 2, 3);
             INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, parent_thread_id, created_at, updated_at)
             VALUES ('i-child', 'p-threads', 12, 'Child issue', '', 'backlog', 'backlog', 'none', 0, 't-design', 4, 4);",
        )
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn collection_and_overview_render_first_class_thread_fields() {
        let db = fixture().await;
        let collection = read_project_threads(&db, "thr").await;
        assert!(collection.contains("[design-review](cairn://p/thr/design-review)"));
        assert!(collection.contains("Jurisdiction: Own architecture decisions"));
        assert!(collection.contains("Status: active"));
        assert!(collection.contains("Attention: none"));
        // A thread's last activity reads as an age with its absolute anchor
        // beside it, never as the bare `updated_at` epoch it is stored as.
        assert!(
            collection.contains("Last activity: ") && collection.contains(" ago ("),
            "{collection}"
        );
        assert!(
            !collection.contains("Last activity: 3."),
            "the stored epoch must not reach the surface: {collection}"
        );

        let overview = read_thread(&db, "thr", "design-review", &[], 4).await;
        // A thread's heading is its one identifier, the same string its URI
        // carries and its pane header shows.
        assert!(overview.contains("# design-review"));
        assert!(
            overview.contains("[intent](cairn://p/thr/design-review/db/intent/1) · [journal](cairn://p/thr/design-review/db/journal)"),
            "{overview}"
        );
        assert!(overview.contains("[12 — Child issue](cairn://p/thr/12)"));
        assert!(overview.contains("Live session: not started"));
        // The overview says what the seed says about the thread's reader.
        assert!(
            overview.contains(
                "- [headlines](cairn://p/thr/design-review/db/headlines) · Operator has never read your headlines\n"
            ),
            "{overview}"
        );
        let two_hours_ago = chrono::Utc::now().timestamp() - 2 * 3600 - 30;
        db.execute(
            "INSERT INTO headline_read_cursors (reader, thread_id, seen_through_id, seen_at, seen_on)
             VALUES ('operator', 't-design', 4, ?1, 'desktop')",
            (two_hours_ago,),
        )
        .await
        .unwrap();
        let overview = read_thread(&db, "thr", "design-review", &[], 4).await;
        assert!(
            overview.contains("· Operator has read through #4, 2h ago\n"),
            "{overview}"
        );
        // The page a caller appends to says what an append does to the session.
        assert!(
            overview.contains("An idle session starts a turn on the message at once"),
            "{overview}"
        );
        assert!(
            overview.contains("at its next tool call or turn boundary, without being interrupted"),
            "{overview}"
        );
    }

    /// The collection is an ACTIVE listing and the canonical URI is not. That
    /// asymmetry is the whole shape of dormancy: a closed thread stops being
    /// offered to agents while staying fully readable at its own address, which
    /// is what makes closing reversible from the outside.
    #[tokio::test]
    async fn the_collection_hides_a_closed_thread_that_its_own_uri_still_renders() {
        let db = fixture().await;
        db.execute_script(
            "INSERT INTO threads (id, project_id, name, jurisdiction, status, created_at, updated_at)
             VALUES ('t-retired', 'p-threads', 'retired-topic', 'Old ground', 'closed', 2, 3);
             INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, parent_thread_id, created_at, updated_at)
             VALUES ('i-retired-child', 'p-threads', 44, 'Still open child', '', 'backlog', 'backlog', 'none', 0, 't-retired', 4, 4);",
        )
        .await
        .unwrap();

        let collection = read_project_threads(&db, "thr").await;
        assert!(collection.contains("[design-review](cairn://p/thr/design-review)"));
        assert!(
            !collection.contains("retired-topic"),
            "a closed thread is not offered as somewhere to address: {collection}"
        );

        let overview = read_thread(&db, "thr", "retired-topic", &[], 4).await;
        assert!(overview.contains("# retired-topic"));
        assert!(overview.contains("Status: closed"));
        assert!(
            overview.contains("this thread is closed, so an append is refused"),
            "{overview}"
        );
        assert!(
            overview.contains("[44 — Still open child](cairn://p/thr/44)"),
            "closing routes attention, it does not disown children: {overview}"
        );
    }

    #[tokio::test]
    async fn migrated_number_resolves_only_when_no_issue_owns_it() {
        let db = fixture().await;
        assert_eq!(
            resolve_migrated_thread_uri(&db, "cairn://p/thr/3404")
                .await
                .unwrap(),
            Some((
                "cairn://p/thr/design-review".into(),
                "cairn://p/thr/design-review".into()
            ))
        );
        assert_eq!(
            resolve_migrated_thread_uri(&db, "cairn://p/thr/3404/1/thread/chat")
                .await
                .unwrap(),
            Some((
                "cairn://p/thr/design-review/chat".into(),
                "cairn://p/thr/design-review".into()
            ))
        );

        db.execute(
            "INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at)
             VALUES ('i-real', 'p-threads', 3404, 'Real issue', '', 'backlog', 'backlog', 'none', 0, 5, 5)",
            (),
        )
        .await
        .unwrap();
        assert_eq!(
            resolve_migrated_thread_uri(&db, "cairn://p/thr/3404")
                .await
                .unwrap(),
            None
        );
    }

    /// A thread's descendants are read by the ORDINARY node renderers, reached
    /// through the reserved coordinate the normalizer produces.
    ///
    /// Driving the node renderers directly is what this proves: no thread-specific
    /// read path is involved, and the URIs they render address the thread rather
    /// than a `0/0` placeholder.
    #[tokio::test]
    async fn thread_descendants_read_through_the_ordinary_node_renderers() {
        let db = fixture().await;
        let job_id = crate::threads::ensure_thread_session(&db, "t-design")
            .await
            .unwrap();
        db.execute(
            "INSERT INTO memories (id, name, project_id, content, scope, scope_value, job_id, node_seq, created_at, updated_at) VALUES ('m-thread', 'Thread fact', 'p-threads', 'Remember this detail', 'project', 'p-threads', ?1, 1, 6, 6)",
            (job_id.as_str(),),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO jobs (id, parent_job_id, project_id, status, node_name, uri_segment, created_at, updated_at) VALUES ('j-task', ?1, 'p-threads', 'complete', 'research', 'research', 7, 8)",
            (job_id.as_str(),),
        )
        .await
        .unwrap();

        // The session job is what a thread coordinate resolves to, and a task it
        // spawned resolves beneath it.
        assert_eq!(
            crate::jobs::queries::job_id_for_node_coordinate(
                &db,
                "thr",
                0,
                0,
                "design-review",
                None
            )
            .await
            .unwrap()
            .as_deref(),
            Some(job_id.as_str())
        );
        assert_eq!(
            crate::jobs::queries::job_id_for_node_coordinate(
                &db,
                "thr",
                0,
                0,
                "design-review",
                Some("research")
            )
            .await
            .unwrap()
            .as_deref(),
            Some("j-task")
        );

        // The node renderers, driven at the thread coordinate.
        let wakes = super::super::node::read_node_wakes(&db, "thr", 0, 0, "design-review").await;
        assert!(wakes.contains("# Wakes — design-review"), "{wakes}");
        // `ensure_thread_session` seeds the default system subscriptions so a
        // thread session hears messages from birth.
        assert!(wakes.contains("peer `*`"), "{wakes}");
        assert!(wakes.contains("user `*`"), "{wakes}");
        // The rendered address is the thread's, not a 0/0 placeholder.
        assert!(
            wakes.contains("cairn://p/thr/design-review/wakes"),
            "{wakes}"
        );
        assert!(!wakes.contains("/0/0/"), "{wakes}");

        let todos =
            super::super::node::read_job_todos(&db, "thr", 0, 0, "design-review", None).await;
        assert!(!todos.contains("/0/0/"), "{todos}");

        let tasks = super::super::node::read_node_tasks(
            &db,
            "thr",
            0,
            0,
            "design-review",
            &Default::default(),
        )
        .await;
        assert!(tasks.contains("research"), "{tasks}");
        assert!(!tasks.contains("/0/0/"), "{tasks}");

        let task = super::super::node::read_task(
            &db,
            &Default::default(),
            "thr",
            0,
            0,
            "design-review",
            "research",
        )
        .await;
        assert!(task.contains("research"), "{task}");
        assert!(!task.contains("/0/0/"), "{task}");

        let memories = super::super::memories::render_job_memories(
            &db,
            "design-review",
            "cairn://p/thr/design-review/memories",
            &job_id,
        )
        .await;
        assert!(
            memories.contains("cairn://p/thr/design-review/memories/1"),
            "{memories}"
        );
        assert_eq!(
            crate::memories::db::resolve_node_memory_id(&db, "thr", 0, 0, "design-review", 1)
                .await
                .unwrap()
                .as_deref(),
            Some("m-thread"),
            "a thread's memories are individually addressable"
        );
    }

    /// A read of a dormant thread degrades to its overview and, crucially, does
    /// not bring the session job into existence.
    #[tokio::test]
    async fn reading_a_thread_without_a_session_creates_nothing() {
        let db = fixture().await;
        async fn job_count(db: &LocalDb) -> i64 {
            db.query_opt("SELECT COUNT(*) FROM jobs", (), |row| row.i64(0))
                .await
                .unwrap()
                .unwrap()
        }
        assert_eq!(job_count(&db).await, 0);

        for body in [
            super::super::node::read_node_wakes(&db, "thr", 0, 0, "design-review").await,
            super::super::node::read_job_todos(&db, "thr", 0, 0, "design-review", None).await,
            super::super::node::read_node_tasks(
                &db,
                "thr",
                0,
                0,
                "design-review",
                &Default::default(),
            )
            .await,
        ] {
            assert!(body.contains("has no session yet"), "{body}");
            assert!(body.contains("cairn://p/thr/design-review"), "{body}");
        }

        assert_eq!(
            job_count(&db).await,
            0,
            "a read must never mint a thread's session job"
        );

        let overview = read_thread(&db, "thr", "design-review", &[], 4).await;
        assert!(overview.contains("Live session: not started"), "{overview}");
    }

    /// An era's rendered range derives from the thread's CURRENT coordinates
    /// rather than a value baked in at mint time -- the point of not
    /// persisting `range_uri` (CAIRN-4302 amendment): a rename never orphans
    /// it.
    #[tokio::test]
    async fn thread_eras_render_through_the_node_family_with_a_live_derived_range() {
        let db = fixture().await;
        let job_id = crate::threads::ensure_thread_session(&db, "t-design")
            .await
            .unwrap();
        db.execute(
            "INSERT INTO thread_eras (
                 job_id, seq, start_block, end_block, started_at, ended_at, summary,
                 summary_status, summary_model, minted_generation, created_at
             ) VALUES (?1, 1, 0, 2, 10, 20, 'Settled the initial layout question.', 'fallback', NULL, 1, 30)",
            (job_id.as_str(),),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO thread_era_children (job_id, era_seq, child_issue_id, child_issue_uri, child_title, final_status)
             VALUES (?1, 1, 'i-child', 'cairn://p/thr/12', 'Child issue', 'merged')",
            (job_id.as_str(),),
        )
        .await
        .unwrap();

        let collection = super::super::eras::render_node_eras(
            &db,
            "design-review",
            "cairn://p/thr/design-review/eras",
            "cairn://p/thr/design-review/chat",
            &job_id,
            50,
            0,
        )
        .await;
        assert!(collection.contains("1 era(s)"), "{collection}");
        assert!(
            collection.contains("cairn://p/thr/design-review/chat?offset=0&limit=3"),
            "the range is derived from the thread's live chat URI, not persisted: {collection}"
        );
        assert!(!collection.contains("/0/0/"), "{collection}");

        let member = super::super::eras::render_node_era(
            &db,
            "design-review",
            "cairn://p/thr/design-review/eras/1",
            "cairn://p/thr/design-review/chat",
            &job_id,
            1,
        )
        .await;
        assert!(
            member.contains("Settled the initial layout question."),
            "{member}"
        );
        assert!(member.contains("Child issue"), "{member}");
        assert!(
            member.contains("cairn://p/thr/design-review/chat?offset=0&limit=3"),
            "{member}"
        );
        assert!(!member.contains("/0/0/"), "{member}");
    }
}
