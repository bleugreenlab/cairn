use crate::{
    config::{responses as cfg, ConfigResult},
    mcp::{
        handlers::skills_resources::{current_run_project, project_path_by_key},
        types::McpCallbackRequest,
    },
    orchestrator::Orchestrator,
};
use cairn_common::uri::*;
use std::{collections::BTreeMap, path::PathBuf};
async fn history_scope(o: &Orchestrator, project: Option<&str>) -> Result<String, String> {
    let Some(project) = project else {
        return Ok("workspace".into());
    };
    let id =
        o.db.local
            .query_text(
                "SELECT id FROM projects WHERE UPPER(key) = UPPER(?1) LIMIT 1",
                (project.to_string(),),
            )
            .await
            .map_err(|error| format!("Failed to resolve response history project: {error}"))?
            .ok_or_else(|| format!("Project not found: {project}"))?;
    Ok(format!("project:{id}"))
}
async fn scope(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    p: Option<&str>,
) -> Result<(Option<String>, Option<PathBuf>), String> {
    if let Some(k) = p {
        Ok((
            Some(cairn_common::uri::canonical_project(k)),
            Some(project_path_by_key(o, k).await?),
        ))
    } else {
        Ok(match current_run_project(o, r).await {
            Some((k, p)) => (Some(k), p),
            None => (None, None),
        })
    }
}
pub(crate) async fn collection(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    p: Option<&str>,
) -> String {
    let (k, path) = match scope(o, r, p).await {
        Ok(x) => x,
        Err(e) => return e,
    };
    let list = match cfg::list_responses(&o.config_dir, path.as_deref()) {
        Ok(x) => x,
        Err(e) => return e,
    };
    let mut map = BTreeMap::new();
    let mut bad = vec![];
    for x in list {
        match x {
            ConfigResult::Ok(v) => {
                map.entry(v.id.clone()).or_insert(v);
            }
            ConfigResult::Err { path, error } => bad.push((path, error)),
        }
    }
    let mut out = format!(
        "# Responses — {} context\n\n{} response(s)\n\n",
        k.as_deref().unwrap_or("workspace"),
        map.len()
    );
    for x in map.values() {
        let scope_key = history_scope(
            o,
            if x.is_project_scoped {
                k.as_deref()
            } else {
                None
            },
        )
        .await
        .unwrap_or_else(|_| "workspace".into());
        let n = cairn_db::storage::count_response_invocations(&o.db.local, &scope_key, &x.id)
            .await
            .unwrap_or(0);
        let u = if x.is_project_scoped {
            k.as_deref()
                .map(|k| build_project_response_uri(k, &x.id))
                .unwrap_or_else(|| build_response_uri(&x.id))
        } else {
            build_response_uri(&x.id)
        };
        let backend = x.definition.backend.clone().or_else(|| {
            crate::config::presets::resolve_preset(
                x.definition.tier.as_deref().unwrap_or("sm"),
                &crate::config::presets::load_effective_presets(&o.config_dir, path.as_deref()),
            )
            .ok()
            .map(|preset| preset.backend)
        });
        let shape = backend
            .as_deref()
            .map(|backend| {
                format!(
                    "{:?}",
                    crate::backends::backend_for_name(Some(backend)).completion_shape()
                )
            })
            .unwrap_or_else(|| "Unavailable".into());
        let selection = x.definition.model.as_deref().map_or_else(
            || format!("tier {}", x.definition.tier.as_deref().unwrap_or("sm")),
            |model| {
                format!(
                    "{} / {}",
                    x.definition.backend.as_deref().unwrap_or("?"),
                    model
                )
            },
        );
        out.push_str(&format!(
            "- [{}]({}) [{}] — {} · {} · {} · {} recent invocation(s)\n",
            x.id,
            u,
            if x.is_project_scoped {
                "project"
            } else {
                "workspace"
            },
            x.definition.description,
            selection,
            shape,
            n
        ));
    }
    for (p, e) in bad {
        out.push_str(&format!("- invalid {} — {}\n", p.display(), e));
    }
    out
}
pub(crate) async fn member(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    id: &str,
    p: Option<&str>,
) -> String {
    let (k, path) = match scope(o, r, p).await {
        Ok(x) => x,
        Err(e) => return e,
    };
    let x = match cfg::get_response(&o.config_dir, id, path.as_deref()) {
        Ok(Some(x)) if p.is_none() || x.is_project_scoped => x,
        Ok(_) => return format!("Response not found: {id}"),
        Err(e) => return e,
    };
    let d = &x.definition;
    let h = if x.is_project_scoped {
        k.as_deref()
            .map(|k| build_project_response_history_uri(k, id))
            .unwrap_or_else(|| build_response_history_uri(id))
    } else {
        build_response_history_uri(id)
    };
    let variables = if d.variables.is_empty() {
        "none".to_string()
    } else {
        d.variables
            .iter()
            .map(|v| {
                format!(
                    "`{}`{}",
                    v.name,
                    if v.required { " (required)" } else { "" }
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let output = serde_json::to_string(&d.output).unwrap_or_else(|_| "text".into());
    let selection = d.model.as_deref().map_or_else(
        || format!("tier: {}", d.tier.as_deref().unwrap_or("sm")),
        |model| {
            format!(
                "model: {}\n- backend: {}",
                model,
                d.backend.as_deref().unwrap_or("?")
            )
        },
    );
    let mut out=format!("# Response {} — {}\n\n{}\n\n- {}\n- timeout: {}s\n- variables: {}\n- output: `{}`\n- few-shot examples: {}\n- history: [{}]({})\n\n## prompt\n\n{}\n\n## recent history\n\n",id,d.name,d.description,selection,d.timeout.as_secs(),variables,output,d.examples.len(),h,h,d.template);
    let scope_key = history_scope(
        o,
        if x.is_project_scoped {
            k.as_deref()
        } else {
            None
        },
    )
    .await
    .unwrap_or_else(|_| "workspace".into());
    match cairn_db::storage::list_response_invocations(&o.db.local, &scope_key, id, 5).await {
        Ok(v) if v.is_empty() => out.push_str("No invocations recorded.\n"),
        Ok(v) => {
            for q in v {
                out.push_str(&format!(
                    "- [#{}]({}/{}) · {} · {}ms\n",
                    q.seq,
                    h,
                    q.seq,
                    q.status,
                    q.latency_ms.unwrap_or(0)
                ))
            }
        }
        Err(e) => out.push_str(&e.to_string()),
    };
    out
}
pub(crate) async fn history(o: &Orchestrator, id: &str, project: Option<&str>) -> String {
    let scope_key = match history_scope(o, project).await {
        Ok(value) => value,
        Err(error) => return error,
    };
    match cairn_db::storage::list_response_invocations(&o.db.local, &scope_key, id, 200).await {
        Ok(v) => {
            let mut s = format!("# Response history — {}\n\n", id);
            for q in v {
                let entry_uri = project
                    .map(|project| build_project_response_history_entry_uri(project, id, q.seq))
                    .unwrap_or_else(|| build_response_history_entry_uri(id, q.seq));
                s.push_str(&format!(
                    "- [#{}]({}) · {} · {}\n",
                    q.seq, entry_uri, q.status, q.created_at
                ))
            }
            s
        }
        Err(e) => e.to_string(),
    }
}
pub(crate) async fn entry(o: &Orchestrator, id: &str, seq: i64, project: Option<&str>) -> String {
    let scope_key = match history_scope(o, project).await {
        Ok(value) => value,
        Err(error) => return error,
    };
    match cairn_db::storage::get_response_invocation(&o.db.local,&scope_key,id,seq).await{Ok(Some(q))=>format!("# Response history entry #{} — {}\n\n- status: {}\n- caller: {}\n- model: {}\n- backend: {}\n- latency: {}ms\n\n## rendered prompt\n\n{}\n\n## output\n\n{}\n",q.seq,q.response_id,q.status,q.caller_kind,q.model.unwrap_or_else(||"unknown".into()),q.backend.unwrap_or_else(||"unknown".into()),q.latency_ms.unwrap_or(0),q.rendered_prompt,q.output_text.unwrap_or_default()),Ok(None)=>format!("Response history entry not found: {id}/{seq}"),Err(e)=>e.to_string()}
}
