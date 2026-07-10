//! Parsing of cairn:// URIs into `CairnResource` / `CairnResourceUri`.

use super::build::canonical_project;
use super::types::{
    is_reserved_node_segment, CairnResource, CairnResourceUri, DEFAULT_BROWSER_SLUG, PROJECT_SCOPE,
};
use crate::query::parse_query_params;

fn parse_positive_i32(value: &str) -> Option<i32> {
    value.parse::<i32>().ok().filter(|value| *value > 0)
}

fn parse_non_negative_i32(value: &str) -> Option<i32> {
    value.parse::<i32>().ok().filter(|value| *value >= 0)
}

pub fn parse_resource_uri(uri: &str) -> Result<Option<CairnResourceUri>, String> {
    let (identity, raw_query) = match uri.split_once('?') {
        Some((identity, query)) => (identity, Some(query)),
        None => (uri, None),
    };
    let Some(resource) = parse_uri(identity) else {
        return Ok(None);
    };
    let params = raw_query
        .map(parse_query_params)
        .transpose()?
        .unwrap_or_default();
    Ok(Some(CairnResourceUri { resource, params }))
}

pub fn parse_uri(uri: &str) -> Option<CairnResource> {
    let uri = uri.strip_prefix("cairn://")?;

    // External MCP gateway family. Handled before the '?'-split and the generic
    // '/'-split so the external resource tail survives intact — it may contain
    // '/', '://', AND its own '?query', none of which are Cairn-side syntax
    // (the family advertises no read projections). A server name itself never
    // contains '?' or '/'.
    if uri == "mcp" {
        return Some(CairnResource::Mcp {
            server: None,
            resource: None,
        });
    }
    if let Some(rest) = uri.strip_prefix("mcp/") {
        let mut segs = rest.splitn(2, '/');
        let server = segs.next()?;
        if server.is_empty() || server.contains('?') {
            return None;
        }
        let resource = segs.next().filter(|s| !s.is_empty()).map(|s| s.to_string());
        return Some(CairnResource::Mcp {
            server: Some(server.to_string()),
            resource,
        });
    }

    let path = uri.split('?').next()?;
    if path.is_empty() {
        return None;
    }
    let parts: Vec<&str> = path.split('/').collect();
    if parts.iter().any(|part| part.is_empty()) {
        return None;
    }

    match parts.as_slice() {
        ["db"] => Some(CairnResource::Db),
        ["dev"] => Some(CairnResource::Dev),
        ["dev", "db"] => Some(CairnResource::DevDb),
        ["dev", "pid"] => Some(CairnResource::DevPid),
        ["logs"] => Some(CairnResource::Logs),
        ["bug"] => Some(CairnResource::Bug),
        ["help"] => Some(CairnResource::Help),
        ["websearch"] => Some(CairnResource::WebSearch),
        ["skills"] => Some(CairnResource::Skills),
        ["skills", skill_id, rest @ ..] => Some(CairnResource::Skill {
            skill_id: (*skill_id).to_string(),
            path: rest.iter().map(|segment| (*segment).to_string()).collect(),
        }),
        [PROJECT_SCOPE, project, "skills"] => Some(CairnResource::ProjectSkills {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "skills", skill_id, rest @ ..] => {
            Some(CairnResource::ProjectSkill {
                project: canonical_project(project),
                skill_id: (*skill_id).to_string(),
                path: rest.iter().map(|segment| (*segment).to_string()).collect(),
            })
        }
        [PROJECT_SCOPE, project, "references"] => Some(CairnResource::ProjectReferences {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "references", name] => Some(CairnResource::ProjectReference {
            project: canonical_project(project),
            name: (*name).to_string(),
        }),
        ["settings"] => Some(CairnResource::Settings),
        ["projects"] => Some(CairnResource::Projects),
        ["labels"] => Some(CairnResource::Labels),
        ["labels", label_id] => Some(CairnResource::Label {
            label_id: (*label_id).to_string(),
        }),
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "memories"] => {
            Some(CairnResource::NodeMemories {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "memories", memory_seq] => {
            Some(CairnResource::NodeMemory {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                memory_seq: parse_positive_i32(memory_seq)?,
            })
        }
        ["recipes"] => Some(CairnResource::Recipes),
        ["recipes", recipe_id] => Some(CairnResource::Recipe {
            recipe_id: (*recipe_id).to_string(),
        }),
        [PROJECT_SCOPE, project, "recipes"] => Some(CairnResource::ProjectRecipes {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "recipes", recipe_id] => Some(CairnResource::ProjectRecipe {
            project: canonical_project(project),
            recipe_id: (*recipe_id).to_string(),
        }),
        ["workflows"] => Some(CairnResource::Workflows),
        ["workflows", workflow_id] => Some(CairnResource::Workflow {
            workflow_id: (*workflow_id).to_string(),
        }),
        [PROJECT_SCOPE, project, "workflows"] => Some(CairnResource::ProjectWorkflows {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "workflows", workflow_id] => {
            Some(CairnResource::ProjectWorkflow {
                project: canonical_project(project),
                workflow_id: (*workflow_id).to_string(),
            })
        }
        ["agents"] => Some(CairnResource::Agents),
        ["agents", agent_id] => Some(CairnResource::Agent {
            agent_id: (*agent_id).to_string(),
        }),
        [PROJECT_SCOPE, project, "agents"] => Some(CairnResource::ProjectAgents {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "agents", agent_id] => Some(CairnResource::ProjectAgent {
            project: canonical_project(project),
            agent_id: (*agent_id).to_string(),
        }),
        ["actions"] => Some(CairnResource::Actions),
        ["actions", action_id] => Some(CairnResource::Action {
            action_id: (*action_id).to_string(),
        }),
        [PROJECT_SCOPE, project, "actions"] => Some(CairnResource::ProjectActions {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "actions", action_id] => Some(CairnResource::ProjectAction {
            project: canonical_project(project),
            action_id: (*action_id).to_string(),
        }),
        // Literal `symbols` segment(s); must precede the numeric issue arm below so a
        // project-scoped symbols URI is never misread as an issue id.
        [PROJECT_SCOPE, project, "symbols"] => Some(CairnResource::ProjectSymbols {
            project: canonical_project(project),
            symbol: None,
        }),
        [PROJECT_SCOPE, project, "symbols", symbol] => Some(CairnResource::ProjectSymbols {
            project: canonical_project(project),
            symbol: Some((*symbol).to_string()),
        }),
        [PROJECT_SCOPE, project] => Some(CairnResource::Project {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "issues"] => Some(CairnResource::ProjectIssues {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "messages"] => Some(CairnResource::ProjectMessages {
            project: canonical_project(project),
        }),
        // Must precede the `[PROJECT_SCOPE, project, number]` issue arm: a
        // literal `settings` segment is not a numeric issue id.
        [PROJECT_SCOPE, project, "settings"] => Some(CairnResource::ProjectSettings {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "terminal", slug] => Some(CairnResource::ProjectTerminal {
            project: canonical_project(project),
            slug: (*slug).to_string(),
        }),
        [PROJECT_SCOPE, project, "browser", slug] => Some(CairnResource::ProjectBrowser {
            project: canonical_project(project),
            slug: (*slug).to_string(),
        }),
        [PROJECT_SCOPE, project, "browser"] => Some(CairnResource::ProjectBrowser {
            project: canonical_project(project),
            slug: DEFAULT_BROWSER_SLUG.to_string(),
        }),
        [PROJECT_SCOPE, project, number] => Some(CairnResource::Issue {
            project: canonical_project(project),
            number: parse_positive_i32(number)?,
        }),
        [PROJECT_SCOPE, project, number, "changed"] => Some(CairnResource::Changed {
            project: canonical_project(project),
            number: parse_positive_i32(number)?,
        }),
        [PROJECT_SCOPE, project, number, "messages"] => Some(CairnResource::IssueMessages {
            project: canonical_project(project),
            number: parse_positive_i32(number)?,
        }),
        [PROJECT_SCOPE, project, number, "executions"] => Some(CairnResource::IssueExecutions {
            project: canonical_project(project),
            number: parse_positive_i32(number)?,
        }),
        [PROJECT_SCOPE, project, number, "comments"] => Some(CairnResource::IssueComments {
            project: canonical_project(project),
            number: parse_positive_i32(number)?,
        }),
        // Must precede the `[PROJECT_SCOPE, project, number, exec_seq, node_id]`
        // node arm: that arm binds `exec_seq`/`node_id` to any segments, so a
        // literal `comments` member URI would otherwise be misread as a node.
        [PROJECT_SCOPE, project, number, "comments", comment_seq] => {
            Some(CairnResource::IssueComment {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                comment_seq: parse_positive_i32(comment_seq)?,
            })
        }
        // MUST precede the Node arm: both are 5-segment shapes, but a literal
        // `executions` in the 4th position names a single execution snapshot,
        // not a node whose exec_seq happens to parse here.
        [PROJECT_SCOPE, project, number, "executions", exec_seq] => {
            Some(CairnResource::IssueExecution {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id] => Some(CairnResource::Node {
            project: canonical_project(project),
            number: parse_positive_i32(number)?,
            exec_seq: parse_positive_i32(exec_seq)?,
            node_id: (*node_id).to_string(),
        }),
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "chat"] => {
            Some(CairnResource::NodeChat {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "chat", "raw"] => {
            Some(CairnResource::NodeChatRaw {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "chat", "turn", turn_seq] => {
            Some(CairnResource::NodeChatTurn {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                turn_seq: parse_non_negative_i32(turn_seq)?,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "chat", run_seq, event_seq] => {
            Some(CairnResource::NodeChatEvent {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                run_seq: parse_positive_i32(run_seq)?,
                event_seq: parse_non_negative_i32(event_seq)?,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "artifact"] => {
            Some(CairnResource::NodeArtifact {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                name: None,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "diff"] => {
            Some(CairnResource::NodeDiff {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "symbols"] => {
            Some(CairnResource::NodeSymbols {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                symbol: None,
            })
        }
        // A `::`/`.`-qualified symbol stays one path segment, so a
        // container-qualified name (`Foo::bar`) passes through intact.
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "symbols", symbol] => {
            Some(CairnResource::NodeSymbols {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                symbol: Some((*symbol).to_string()),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "todos"] => {
            Some(CairnResource::JobTodos {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: None,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "tasks"] => {
            Some(CairnResource::NodeTasks {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "calls"] => {
            Some(CairnResource::NodeCalls {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "wakes"] => {
            Some(CairnResource::NodeWakes {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "checks"] => {
            Some(CairnResource::NodeChecks {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "questions"] => {
            Some(CairnResource::NodeQuestions {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "questions", segment] => {
            Some(CairnResource::NodeQuestion {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                segment: (*segment).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "permissions"] => {
            Some(CairnResource::NodePermissions {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "permissions", segment] => {
            Some(CairnResource::NodePermission {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                segment: (*segment).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "messages"] => {
            Some(CairnResource::NodeMessages {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "progress"] => {
            Some(CairnResource::NodeProgress {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "todos"] => {
            Some(CairnResource::JobTodos {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: Some((*task_name).to_string()),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "terminal", slug] => {
            Some(CairnResource::NodeTerminal {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                slug: (*slug).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "repl", slug] => {
            Some(CairnResource::NodeRepl {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                slug: (*slug).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "terminal", slug] => {
            Some(CairnResource::TaskTerminal {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
                slug: (*slug).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "browser", slug] => {
            Some(CairnResource::NodeBrowser {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                slug: (*slug).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "browser"] => {
            Some(CairnResource::NodeBrowser {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                slug: DEFAULT_BROWSER_SLUG.to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "browser", slug] => {
            Some(CairnResource::TaskBrowser {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
                slug: (*slug).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "browser"] => {
            Some(CairnResource::TaskBrowser {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
                slug: DEFAULT_BROWSER_SLUG.to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name] => {
            Some(CairnResource::Task {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "chat"] => {
            Some(CairnResource::TaskChat {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "chat", "raw"] => {
            Some(CairnResource::TaskChatRaw {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "chat", "turn", turn_seq] => {
            Some(CairnResource::TaskChatTurn {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
                turn_seq: parse_non_negative_i32(turn_seq)?,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "chat", run_seq, event_seq] => {
            Some(CairnResource::TaskChatEvent {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
                run_seq: parse_positive_i32(run_seq)?,
                event_seq: parse_non_negative_i32(event_seq)?,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "artifact"] => {
            Some(CairnResource::TaskArtifact {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
                name: None,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "messages"] => {
            Some(CairnResource::TaskMessages {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "permissions"] => {
            Some(CairnResource::TaskPermissions {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "permissions", segment] => {
            Some(CairnResource::TaskPermission {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
                segment: (*segment).to_string(),
            })
        }
        // Type-named artifact under a task: a trailing non-reserved segment.
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, name]
            if !is_reserved_node_segment(name) =>
        {
            Some(CairnResource::TaskArtifact {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
                name: Some((*name).to_string()),
            })
        }
        // Type-named artifact under a node: a trailing non-reserved segment
        // (`.../{node}/plan`). Reserved keywords are handled by the arms above.
        [PROJECT_SCOPE, project, number, exec_seq, node_id, name]
            if !is_reserved_node_segment(name) =>
        {
            Some(CairnResource::NodeArtifact {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                name: Some((*name).to_string()),
            })
        }
        _ => None,
    }
}
