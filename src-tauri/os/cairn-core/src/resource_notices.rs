//! Authored resource context, private to this install, evaluated at the read boundary.
use cairn_db::turso::params;
use cairn_uri::{parse_uri, CairnResource};
use serde_json::Value;

use crate::execution::gates::ask_conditions::{assess_until, UntilStanding};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

fn pattern(project: &str) -> String {
    format!("cairn://p/{project}/*/*/*/rebase")
}

fn canonical(resource: &str) -> bool {
    !resource.contains('*') && parse_uri(resource).is_some_and(|parsed| parsed.to_uri() == resource)
}

fn validate_resource(project: &str, resource: &str) -> Result<(), String> {
    if parse_uri(resource)
        .and_then(|uri| uri.project_key().map(str::to_owned))
        .is_some_and(|key| key != project)
    {
        return Err("notice resource belongs to a different project".into());
    }
    if resource == pattern(project) || canonical(resource) {
        Ok(())
    } else {
        Err("resource must be a canonical cairn:// URI; the only pattern is cairn://p/{project}/*/*/*/rebase".into())
    }
}

fn matches(stored: &str, resource: &str) -> bool {
    if stored == resource {
        return true;
    }
    let parts: Vec<_> = stored.split('/').collect();
    let target: Vec<_> = resource.split('/').collect();
    parts.len() == 8
        && parts[2] == "p"
        && parts[4..7] == ["*", "*", "*"]
        && parts[7] == "rebase"
        && target.len() == parts.len()
        && parts[..4] == target[..4]
        && target[7] == "rebase"
        && canonical(resource)
}

async fn project_id(orch: &Orchestrator, project: &str) -> Result<String, String> {
    if project.is_empty() {
        return Ok(String::new());
    }
    if parse_uri(&format!("cairn://p/{project}"))
        .is_none_or(|uri| uri.to_uri() != format!("cairn://p/{project}"))
    {
        return Err("project must be a canonical project key".into());
    }
    orch.db
        .for_project(project)
        .await
        .query_opt(
            "SELECT id FROM projects WHERE key = ?1",
            (project.to_owned(),),
            |row| row.text(0),
        )
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("project {project} does not exist"))
}

/// Set or clear only this author's notice. Preview performs the same validation without writing.
pub(crate) async fn set(
    orch: &Orchestrator,
    project: &str,
    resource: &str,
    author: &str,
    payload: &Value,
    dry_run: bool,
) -> Result<String, String> {
    validate_resource(project, resource)?;
    let project_id = project_id(orch, project).await?;
    if author.trim().is_empty() {
        return Err("notice author must not be empty".into());
    }
    let object = payload
        .as_object()
        .ok_or("notice payload must be {text,refs,until}")?;
    if let Some(key) = object
        .keys()
        .find(|k| !["text", "refs", "until"].contains(&k.as_str()))
    {
        return Err(format!("unknown notice field: {key}"));
    }
    let text = match object.get("text") {
        Some(Value::Null) => None,
        Some(Value::String(text)) if !text.trim().is_empty() => Some(text.clone()),
        _ => return Err("notice text must be a nonempty string, or null to clear".into()),
    };
    let refs = match object.get("refs") {
        None => Vec::new(),
        Some(Value::Array(refs)) => refs
            .iter()
            .map(|reference| {
                let uri = reference
                    .as_str()
                    .ok_or("notice refs must be canonical issue URIs")?;
                if !matches!(parse_uri(uri), Some(CairnResource::Issue { .. })) || !canonical(uri) {
                    return Err("notice refs must be canonical issue URIs");
                }
                Ok(uri.to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err("notice refs must be an array of canonical issue URIs".into()),
    };
    let until = assess_until(orch, object.get("until"), "payload.until").await?;
    let expires = until.as_ref().map(|u| u.until_json());
    if dry_run {
        return Ok(format!(
            "Would {} notice on {resource}",
            if text.is_some() { "set" } else { "clear" }
        ));
    }
    if let Some(text) = text {
        orch.db.local.execute(
            "INSERT INTO resource_notices(id,project_id,resource_uri,author,text,refs,created_at,expires)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
             ON CONFLICT(project_id,resource_uri,author) DO UPDATE SET
             text=excluded.text,refs=excluded.refs,created_at=excluded.created_at,expires=excluded.expires,shown_count=0",
            params![uuid::Uuid::new_v4().to_string(), project_id, resource, author, text,
                serde_json::to_string(&refs).map_err(|e| e.to_string())?, orch.services.clock.now(), expires],
        ).await.map_err(|e| e.to_string())?;
        Ok(format!("Set notice on {resource}"))
    } else {
        orch.db.local.execute(
            "DELETE FROM resource_notices WHERE project_id=?1 AND resource_uri=?2 AND author=?3",
            params![project_id, resource, author],
        ).await.map_err(|e| e.to_string())?;
        Ok(format!("Cleared notice on {resource}"))
    }
}

struct Notice {
    id: String,
    resource: String,
    author: String,
    text: String,
    refs: String,
    expires: Option<String>,
    count: i64,
}

async fn render(
    orch: &Orchestrator,
    project: Option<&str>,
    resource: Option<&str>,
) -> Result<String, String> {
    let rows = if let Some(project) = project {
        let id = project_id(orch, project).await?;
        load(&orch.db.local, Some(id)).await?
    } else {
        load(&orch.db.local, None).await?
    };
    let mut output = String::new();
    for row in rows {
        if resource.is_some_and(|resource| !matches(&row.resource, resource)) {
            continue;
        }
        let until: Option<Value> = row
            .expires
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|e| format!("notice {} expiry: {e}", row.id))?;
        let assessed = assess_until(orch, until.as_ref(), "notice.until")
            .await
            .map_err(|e| format!("notice {} expiry: {e}", row.id))?;
        if assessed
            .as_ref()
            .is_some_and(|a| matches!(a.standing, UntilStanding::AlreadyTrue))
        {
            continue;
        }
        let refs: Vec<String> =
            serde_json::from_str(&row.refs).map_err(|e| format!("notice {} refs: {e}", row.id))?;
        let count = if resource.is_some() {
            orch.db
                .local
                .execute(
                    "UPDATE resource_notices SET shown_count=shown_count+1 WHERE id=?1",
                    (row.id.clone(),),
                )
                .await
                .map_err(|e| e.to_string())?;
            row.count + 1
        } else {
            row.count
        };
        output.push_str(&format!(
            "\n### Standing notice · {}\n{}\nResource: {}\nShown: {count}\n",
            row.author, row.text, row.resource
        ));
        if !refs.is_empty() {
            output.push_str(&format!("References: {}\n", refs.join(", ")));
        }
        if let Some(assessed) = assessed {
            output.push_str(&format!("Until: {}\n", assessed.rendered));
        }
    }
    Ok(output)
}

async fn load(db: &LocalDb, project_id: Option<String>) -> Result<Vec<Notice>, String> {
    db.query_all(
        "SELECT id,resource_uri,author,text,refs,expires,shown_count FROM resource_notices
         WHERE (?1 IS NULL OR project_id=?1 OR project_id='') ORDER BY created_at,id",
        (project_id,),
        |row| {
            Ok(Notice {
                id: row.text(0)?,
                resource: row.text(1)?,
                author: row.text(2)?,
                text: row.text(3)?,
                refs: row.text(4)?,
                expires: row.opt_text(5)?,
                count: row.get(6)?,
            })
        },
    )
    .await
    .map_err(|e| e.to_string())
}

fn display(result: Result<String, String>) -> String {
    result.unwrap_or_else(|error| format!("\n**Standing notice error:** {error}\n"))
}

/// Render exact notices and the single supported project rebase pattern.
pub(crate) async fn render_for(orch: &Orchestrator, resource: &str) -> String {
    if !canonical(resource) {
        return display(Err("notice read requires a canonical resource URI".into()));
    }
    display(render(orch, None, Some(resource)).await)
}

/// List live project and installation-global notices without counting a display.
pub(crate) async fn render_project(orch: &Orchestrator, project: &str) -> String {
    display(render(orch, Some(project), None).await)
}

pub(crate) async fn render_for_job(orch: &Orchestrator, db: &LocalDb, job_id: &str) -> String {
    match crate::jobs::queries::home_uri_for_job(db, job_id).await {
        Ok(Some(home)) => render_for(orch, &format!("{home}/rebase")).await,
        Ok(None) => String::new(),
        Err(error) => display(Err(error.to_string())),
    }
}

/// Attach the same live instruction a machine's page carries to every named
/// placement refusal. Called after placement releases its locks.
pub(crate) async fn annotate_refusal(
    orch: &Orchestrator,
    outcome: &mut cairn_common::executor_protocol::CellOutcome,
) {
    use cairn_common::executor_protocol::{CellOutcome, CellUnavailableReason};
    let CellOutcome::Unavailable {
        reason: CellUnavailableReason::NoMatchingExecutor { rejected },
        diagnostic,
    } = outcome
    else {
        return;
    };
    let names: std::collections::BTreeSet<_> = rejected
        .iter()
        .map(|machine| machine.executor_name.clone())
        .collect();
    for name in names {
        diagnostic.push_str(&render_for(orch, &format!("cairn://executors/{name}")).await);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::gates::sweep::tests::{env, resolve_issue};
    use serde_json::json;

    #[test]
    fn pattern_is_only_one_projects_node_rebases() {
        let pattern = pattern("prj");
        assert!(matches(&pattern, "cairn://p/prj/2/1/builder/rebase"));
        for other in [
            "cairn://p/other/2/1/builder/rebase",
            "cairn://p/prj/2/1/builder/tasks",
            "cairn://p/prj/2/1/builder/task/child/rebase",
            "cairn://p/prj/2/1/builder/rebase?limit=3",
        ] {
            assert!(!matches(&pattern, other), "{other}");
        }
        assert!(validate_resource("prj", "cairn://p/prj/*/*/*/rebase").is_ok());
        assert!(validate_resource("prj", "cairn://p/prj/*").is_err());
        assert!(validate_resource("prj", "cairn://p/other/2").is_err());
    }

    #[tokio::test]
    async fn authors_replace_clear_preview_and_live_expiry_counts() {
        let env = env("resource-notices.db").await;
        let orch = &env.orch;
        let target = "cairn://p/prj/2/1/builder/rebase";
        let pattern = pattern("prj");
        let value = json!({
            "text":"Wait for the fix",
            "refs":["cairn://p/prj/1"],
            "until":[{"fact":"issue_resolution","issue":"cairn://p/prj/1","resolution":"merged"}]
        });
        set(orch, "prj", &pattern, "alice", &value, true)
            .await
            .unwrap();
        assert!(render_project(orch, "prj").await.is_empty());
        set(orch, "prj", &pattern, "alice", &value, false)
            .await
            .unwrap();
        set(
            orch,
            "prj",
            &pattern,
            "bob",
            &json!({"text":"Another author"}),
            false,
        )
        .await
        .unwrap();
        set(
            orch,
            "prj",
            &pattern,
            "bob",
            &json!({"text":"Updated author"}),
            false,
        )
        .await
        .unwrap();
        assert_eq!(load(&orch.db.local, None).await.unwrap().len(), 2);
        let listing = render_project(orch, "prj").await;
        assert!(listing.contains("Wait for the fix"));
        assert!(listing.contains("Shown: 0"));
        let rendered = render_for(orch, target).await;
        assert!(rendered.contains("Wait for the fix"));
        assert!(rendered.contains("Updated author"));
        assert!(render_project(orch, "prj").await.contains("Shown: 1"));
        resolve_issue(&env, 1, "merged").await;
        let listing = render_project(orch, "prj").await;
        assert!(!listing.contains("Wait for the fix"));
        assert!(listing.contains("Updated author"));
        assert!(!render_for(orch, target).await.contains("Wait for the fix"));
        set(orch, "prj", &pattern, "bob", &json!({"text":null}), false)
            .await
            .unwrap();
        assert!(render_project(orch, "prj").await.is_empty());
        assert!(set(
            orch,
            "prj",
            target,
            "bob",
            &json!({"text":"bad", "refs":["cairn:~/plan"]}),
            false
        )
        .await
        .is_err());
        assert!(set(
            orch,
            "prj",
            target,
            "bob",
            &json!({"text":"bad", "until":[{"fact":"invented"}]}),
            false
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn global_notices_and_evaluation_errors_are_visible() {
        let env = env("resource-notices-global.db").await;
        let target = "cairn://executors";
        set(
            &env.orch,
            "",
            target,
            "operator",
            &json!({"text":"Fleet maintenance"}),
            false,
        )
        .await
        .unwrap();
        assert!(render_for(&env.orch, target)
            .await
            .contains("Fleet maintenance"));
        assert!(render_project(&env.orch, "prj")
            .await
            .contains("Fleet maintenance"));
        env.orch
            .db
            .local
            .execute(
                "UPDATE resource_notices SET expires='[{\"fact\":\"invented\"}]'",
                (),
            )
            .await
            .unwrap();
        assert!(render_for(&env.orch, target)
            .await
            .contains("Standing notice error"));
    }
}
