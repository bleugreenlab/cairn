use crate::{
    config::{routes as cfg, ConfigResult},
    mcp::{
        handlers::skills_resources::{current_run_project, project_path_by_key},
        types::McpCallbackRequest,
    },
    orchestrator::Orchestrator,
    routes::{RouteDefinition, RouteNodeConfig, RouteSink},
};
use cairn_common::{query::QueryParam, uri::*};
use std::path::PathBuf;
async fn scope(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    p: Option<&str>,
) -> Result<(Option<String>, Option<PathBuf>), String> {
    if let Some(k) = p {
        Ok((
            Some(k.to_uppercase()),
            Some(project_path_by_key(o, k).await?),
        ))
    } else {
        Ok(match current_run_project(o, r).await {
            Some((k, p)) => (Some(k), p),
            None => (None, None),
        })
    }
}
async fn scope_key(o: &Orchestrator, p: Option<&str>) -> Result<String, String> {
    let Some(p) = p else {
        return Ok("workspace".into());
    };
    let id =
        o.db.local
            .query_text(
                "SELECT id FROM projects WHERE UPPER(key)=UPPER(?1) LIMIT 1",
                (p.to_string(),),
            )
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("Project not found: {p}"))?;
    Ok(format!("project:{id}"))
}
fn sink_kind(s: &RouteSink) -> &'static str {
    match s {
        RouteSink::Channel { .. } => "channel",
        RouteSink::Message { .. } => "message",
        RouteSink::Issue { .. } => "issue",
        RouteSink::Label { .. } => "label",
    }
}
/// A route delivers to every sink it declares, so its rendered identity names
/// each one rather than pretending the first is the whole story.
fn sink_kinds(definition: &RouteDefinition) -> String {
    definition
        .sinks()
        .map(sink_kind)
        .collect::<Vec<_>>()
        .join(" + ")
}
fn uri(project: Option<&str>, id: &str) -> String {
    project
        .map(|p| build_project_route_uri(p, id))
        .unwrap_or_else(|| build_route_uri(id))
}
pub(crate) async fn collection(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    p: Option<&str>,
    params: &[QueryParam],
) -> String {
    let (k, path) = match scope(o, r, p).await {
        Ok(x) => x,
        Err(e) => return e,
    };
    let list = match if p.is_some() {
        cfg::list_project_routes(path.as_deref().expect("explicit project has a path"))
    } else {
        cfg::list_routes(&o.config_dir, path.as_deref())
    } {
        Ok(x) => x,
        Err(e) => return e,
    };
    if params
        .iter()
        .any(|q| q.key == "projection" && q.value == "graph")
    {
        return graph(o, k.as_deref(), list).await;
    }
    let mut out = format!(
        "# Routes — {} context\n\n",
        k.as_deref().unwrap_or("workspace")
    );
    for x in list {
        match x {
            ConfigResult::Ok(v) => {
                let project = if v.is_project_scoped {
                    k.as_deref()
                } else {
                    None
                };
                let count = match scope_key(o, project).await {
                    Ok(sk) => cairn_db::storage::count_route_firings(&o.db.local, &sk, &v.id)
                        .await
                        .unwrap_or(0),
                    Err(_) => 0,
                };
                out.push_str(&format!(
                    "- [{}]({}) [{}] — {} · {} · {} recent firing(s)\n",
                    v.id,
                    uri(project, &v.id),
                    if v.is_project_scoped {
                        "project"
                    } else {
                        "workspace"
                    },
                    v.definition.description,
                    sink_kinds(&v.definition),
                    count
                ));
            }
            ConfigResult::Err { path, error } => {
                out.push_str(&format!("- invalid {} — {}\n", path.display(), error))
            }
        }
    }
    out
}
async fn graph(
    o: &Orchestrator,
    project: Option<&str>,
    list: Vec<ConfigResult<cfg::FileRoute>>,
) -> String {
    let mut nodes = serde_json::Map::new();
    let mut edges = vec![];
    for x in list {
        let ConfigResult::Ok(v) = x else { continue };
        let sk = scope_key(o, if v.is_project_scoped { project } else { None })
            .await
            .unwrap_or_else(|_| "workspace".into());
        let count = cairn_db::storage::count_route_firings(&o.db.local, &sk, &v.id)
            .await
            .unwrap_or(0);
        // The route's own graph, prefixed so several routes share one picture,
        // hung off a shared fact node per source so the overview still reads as
        // "which facts drive which routes".
        let route_node = format!("route:{}", v.id);
        nodes.insert(
            route_node.clone(),
            serde_json::json!({"id":route_node,"kind":"route","label":v.definition.name}),
        );
        let scoped = |id: &str| format!("node:{}:{id}", v.id);
        for node in &v.definition.nodes {
            let label = if node.name.is_empty() {
                match &node.config {
                    RouteNodeConfig::Trigger { when } => when
                        .get("fact")
                        .and_then(|x| x.as_str())
                        .unwrap_or("trigger")
                        .to_string(),
                    RouteNodeConfig::Response { response, .. } => response.clone(),
                    RouteNodeConfig::Sink { sink } => sink_kind(sink).to_string(),
                }
            } else {
                node.name.clone()
            };
            let id = scoped(&node.id);
            nodes.insert(
                id.clone(),
                serde_json::json!({"id":id.clone(),"kind":node.config.type_name(),"label":label}),
            );
            if let RouteNodeConfig::Trigger { when } = &node.config {
                if let Some(f) = when.get("fact").and_then(|x| x.as_str()) {
                    nodes.insert(
                        format!("fact:{f}"),
                        serde_json::json!({"id":format!("fact:{f}"),"kind":"fact","label":f}),
                    );
                    edges.push(serde_json::json!({"route":v.id,"from":format!("fact:{f}"),"to":id,"recentFirings":count}));
                }
            }
        }
        for edge in &v.definition.edges {
            edges.push(serde_json::json!({"route":v.id,"from":scoped(&edge.from),"to":scoped(&edge.to),"recentFirings":count}));
        }
    }
    format!(
        "# Route graph\n\n{}\n",
        serde_json::to_string_pretty(
            &serde_json::json!({"nodes":nodes.into_values().collect::<Vec<_>>(),"edges":edges})
        )
        .unwrap()
    )
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
    let route = if p.is_some() {
        cfg::get_project_route(path.as_deref().expect("explicit project has a path"), id)
    } else {
        cfg::get_route(&o.config_dir, id, path.as_deref())
    };
    match route {
        Ok(Some(v)) => {
            let h = if v.is_project_scoped {
                build_project_route_history_uri(k.as_deref().unwrap(), id)
            } else {
                build_route_history_uri(id)
            };
            format!("# Route {} — {}\n\n{}\n\n- enabled: {}\n- sink: {}\n- triggers: {}\n- responses: {}\n- history: [{}]({})\n\n## definition\n\n{}\n",id,v.definition.name,v.definition.description,v.definition.enabled,sink_kinds(&v.definition),v.definition.triggers().count(),v.definition.responses().count(),h,h,serde_yaml::to_string(&v.definition).unwrap_or_default())
        }
        Ok(_) => format!("Route not found: {id}"),
        Err(e) => e,
    }
}
pub(crate) async fn history(o: &Orchestrator, id: &str, p: Option<&str>) -> String {
    if let Some(project) = p {
        let path = match project_path_by_key(o, project).await {
            Ok(path) => path,
            Err(error) => return error,
        };
        match cfg::get_project_route(&path, id) {
            Ok(Some(_)) => {}
            Ok(None) => return format!("Route not found: {id}"),
            Err(error) => return error,
        }
    }
    let sk = match scope_key(o, p).await {
        Ok(x) => x,
        Err(e) => return e,
    };
    match cairn_db::storage::list_route_firings(&o.db.local, &sk, id, 200).await {
        Ok(v) => {
            let mut s = format!("# Route history — {id}\n\n");
            for q in v {
                let u = p
                    .map(|p| build_project_route_history_entry_uri(p, id, q.seq))
                    .unwrap_or_else(|| build_route_history_entry_uri(id, q.seq));
                s.push_str(&format!(
                    "- [#{}]({}) · {} · {} · {}\n",
                    q.seq, u, q.status, q.trigger_source, q.created_at
                ));
            }
            s
        }
        Err(e) => e.to_string(),
    }
}
pub(crate) async fn entry(o: &Orchestrator, id: &str, seq: i64, p: Option<&str>) -> String {
    if let Some(project) = p {
        let path = match project_path_by_key(o, project).await {
            Ok(path) => path,
            Err(error) => return error,
        };
        match cfg::get_project_route(&path, id) {
            Ok(Some(_)) => {}
            Ok(None) => return format!("Route not found: {id}"),
            Err(error) => return error,
        }
    }
    let sk = match scope_key(o, p).await {
        Ok(x) => x,
        Err(e) => return e,
    };
    match cairn_db::storage::get_route_firing(&o.db.local,&sk,id,seq).await{Ok(Some(q))=>format!("# Route history entry #{} — {}\n\n- status: {}\n- trigger: {}\n- fact identity: {}\n- sink: {}\n- sink ref: {}\n- drop reason: {}\n- error: {}\n\n## transforms\n\n{}\n",q.seq,q.route_id,q.status,q.trigger_source,q.fact_identity,q.sink_kind,q.sink_ref.unwrap_or_default(),q.drop_reason.unwrap_or_default(),q.error.unwrap_or_default(),q.transforms_json.unwrap_or_else(||"[]".into())),Ok(None)=>format!("Route history entry not found: {id}/{seq}"),Err(e)=>e.to_string()}
}
