//! Reference checking for a route definition.
//!
//! `RouteDefinition::validate` answers whether a definition is well formed on
//! its own terms — a legal graph whose bindings resolve against what reaches
//! them. This answers the other half: whether the things it names actually
//! exist — the Response a response node invokes, the recipe an issue sink
//! starts, the project or issue a message sink addresses.
//!
//! Both authoring doors run it. A route saved from the settings canvas and a
//! route written through `cairn://` are the same object, so they are held to the
//! same standard; without this the editor would be the lenient door, saving
//! routes that fail only at fire time, when the operator is not watching.

use super::{ArgumentBinding, RouteDefinition, RouteGraph, RouteNodeConfig, RouteSink};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::{parse_uri, CairnResource};
use std::path::Path;

pub async fn validate_references(
    orch: &Orchestrator,
    route: &RouteDefinition,
    project_path: Option<&Path>,
) -> Result<(), String> {
    let graph = RouteGraph::new(route)?;
    for node in graph.nodes() {
        match &node.config {
            RouteNodeConfig::Response { response, args } => {
                let file = crate::config::responses::get_response(
                    &orch.config_dir,
                    response,
                    project_path,
                )?
                .ok_or_else(|| format!("Unknown response '{response}'"))?;
                let mut arguments = serde_json::Map::new();
                for (name, binding) in args {
                    // A field or an upstream node only has a value at fire time,
                    // and `validate` has already confirmed each one resolves;
                    // rendering here is about the Response's own required
                    // variables, so a placeholder stands in.
                    let value = match binding {
                        ArgumentBinding::Value { value } => value.clone(),
                        _ => serde_json::Value::String(String::new()),
                    };
                    arguments.insert(name.clone(), value);
                }
                file.definition
                    .render(&serde_json::Value::Object(arguments))
                    .map_err(|error| {
                        format!("Invalid arguments for response '{response}': {error}")
                    })?;
            }
            // Every sink's references are checked: a route is only as valid as
            // its least-resolvable delivery.
            RouteNodeConfig::Sink { sink } => {
                validate_sink_references(orch, sink, project_path).await?
            }
            RouteNodeConfig::Trigger { .. } => {}
        }
    }
    Ok(())
}

async fn validate_sink_references(
    orch: &Orchestrator,
    sink: &RouteSink,
    project_path: Option<&Path>,
) -> Result<(), String> {
    if let RouteSink::Issue {
        recipe: Some(recipe),
        ..
    } = sink
    {
        // A recipe is addressed by its file id, not its display name.
        if crate::config::recipes::get_recipe(&orch.config_dir, recipe, project_path)?.is_none() {
            return Err(format!("Unknown recipe '{recipe}'"));
        }
    }
    if let RouteSink::Message { target } = sink {
        let (project, number) = match parse_uri(target) {
            Some(CairnResource::Project { project }) => (project, None),
            Some(CairnResource::Issue { project, number }) => (project, Some(number)),
            _ => {
                return Err(format!(
                    "message target must resolve to a project or issue: {target}"
                ))
            }
        };
        let db = orch.db.for_project(&project).await;
        let found = if let Some(number) = number {
            db.query_text(
                "SELECT i.id FROM issues i JOIN projects p ON p.id=i.project_id WHERE UPPER(p.key)=UPPER(?1) AND i.number=?2 LIMIT 1",
                (project.clone(), number),
            )
            .await
        } else {
            db.query_text(
                "SELECT id FROM projects WHERE UPPER(key)=UPPER(?1) LIMIT 1",
                (project.clone(),),
            )
            .await
        }
        .map_err(|error| error.to_string())?;
        if found.is_none() {
            return Err(match number {
                Some(number) => format!("Issue not found: {}-{number}", project.to_uppercase()),
                None => format!("Project not found: {}", project.to_uppercase()),
            });
        }
    }
    Ok(())
}
