//! Unified cairn:// URI scheme parser.
//!
//! URI hierarchy:
//! ```text
//! cairn://PROJECT                            # Project overview
//!   ├── /messages                            # Project-channel messages (paginated)
//!   ├── /NUMBER                              # Issue (includes comments, PR data)
//!   │   ├── /files                           # All files changed for this issue
//!   │   ├── /messages                        # Issue-channel messages (paginated)
//!   │   └── /NODE                            # Job (e.g., planner-1, includes PR data)
//!   │       ├── /chat                        # Job transcript
//!   │       ├── /chat/full                   # Full transcript (untruncated)
//!   │       ├── /artifact                    # Job output
//!   │       ├── /files                       # Files changed by this node
//!   │       ├── /terminal/SLUG               # Job-scoped terminal
//!   │       └── /task/NAME                   # Nested task
//!   │           ├── /chat
//!   │           ├── /chat/full
//!   │           └── /artifact
//!   ├── /terminal/SLUG                       # Project-scoped terminal
//!   └── /chat/NAME                           # Project chat (named)
//! ```
//!
//! All identifiers are human-readable (no UUIDs in URIs).

/// Parsed cairn:// resource URI
#[derive(Debug, Clone, PartialEq)]
pub enum CairnResource {
    // === Project-level resources ===
    /// Project overview: `cairn://PROJECT`
    Project { project: String },

    // === Issue-level resources ===
    /// Issue overview: `cairn://PROJECT/NUMBER`
    /// Includes inlined comments and PR data.
    Issue { project: String, number: i32 },

    // === Node-level resources ===
    /// Node summary: `cairn://PROJECT/NUMBER/EXEC/NODE`
    /// exec_seq is required for all node-scoped URIs.
    Node {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },

    /// Node chat transcript: `cairn://PROJECT/NUMBER/EXEC/NODE/chat`
    NodeChat {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },

    /// Full node chat transcript: `cairn://PROJECT/NUMBER/EXEC/NODE/chat/full`
    NodeChatFull {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },

    /// Single event in node chat: `cairn://PROJECT/NUMBER/EXEC/NODE/chat/RUN_SEQ/EVENT_SEQ`
    NodeChatEvent {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        run_seq: i32,
        event_seq: i32,
    },

    /// Node artifact: `cairn://PROJECT/NUMBER/EXEC/NODE/artifact`
    NodeArtifact {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },

    /// Node-scoped terminal: `cairn://PROJECT/NUMBER/EXEC/NODE/terminal/SLUG`
    NodeTerminal {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        slug: String,
    },

    // === Task-level resources (nested under nodes) ===
    /// Task chat: `cairn://PROJECT/NUMBER/EXEC/NODE/task/NAME/chat`
    TaskChat {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
    },

    /// Full task chat: `cairn://PROJECT/NUMBER/EXEC/NODE/task/NAME/chat/full`
    TaskChatFull {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
    },

    /// Single event in task chat: `cairn://PROJECT/NUMBER/EXEC/NODE/task/NAME/chat/RUN_SEQ/EVENT_SEQ`
    TaskChatEvent {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
        run_seq: i32,
        event_seq: i32,
    },

    /// Task artifact: `cairn://PROJECT/NUMBER/EXEC/NODE/task/NAME/artifact`
    TaskArtifact {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
    },

    // === Message resources ===
    /// Project messages: `cairn://PROJECT/messages`
    ProjectMessages { project: String },

    /// Issue messages: `cairn://PROJECT/NUMBER/messages`
    IssueMessages { project: String, number: i32 },

    // === Issue-level file changes ===
    /// All files changed for an issue: `cairn://PROJECT/NUMBER/files`
    Files { project: String, number: i32 },

    /// Files changed for a specific node: `cairn://PROJECT/NUMBER/EXEC/NODE/files`
    NodeFiles {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },

    // === Project-level resources ===
    /// Project-scoped terminal: `cairn://PROJECT/terminal/SLUG`
    ProjectTerminal { project: String, slug: String },

    /// Project chat session: `cairn://PROJECT/chat/NAME`
    ProjectChat { project: String, name: String },
}

impl CairnResource {
    /// Convert this resource to its canonical URI string.
    pub fn to_uri(&self) -> String {
        match self {
            // Project-level overview
            CairnResource::Project { project } => {
                format!("cairn://{}", project)
            }

            // Issue-level
            CairnResource::Issue { project, number } => {
                format!("cairn://{}/{}", project, number)
            }

            // Node-level (exec_seq is always required)
            CairnResource::Node {
                project,
                number,
                exec_seq,
                node_id,
            } => {
                format!("cairn://{}/{}/{}/{}", project, number, exec_seq, node_id)
            }
            CairnResource::NodeChat {
                project,
                number,
                exec_seq,
                node_id,
            } => {
                format!(
                    "cairn://{}/{}/{}/{}/chat",
                    project, number, exec_seq, node_id
                )
            }
            CairnResource::NodeChatFull {
                project,
                number,
                exec_seq,
                node_id,
            } => {
                format!(
                    "cairn://{}/{}/{}/{}/chat/full",
                    project, number, exec_seq, node_id
                )
            }
            CairnResource::NodeChatEvent {
                project,
                number,
                exec_seq,
                node_id,
                run_seq,
                event_seq,
            } => {
                format!(
                    "cairn://{}/{}/{}/{}/chat/{}/{}",
                    project, number, exec_seq, node_id, run_seq, event_seq
                )
            }
            CairnResource::NodeArtifact {
                project,
                number,
                exec_seq,
                node_id,
            } => {
                format!(
                    "cairn://{}/{}/{}/{}/artifact",
                    project, number, exec_seq, node_id
                )
            }
            CairnResource::NodeTerminal {
                project,
                number,
                exec_seq,
                node_id,
                slug,
            } => {
                format!(
                    "cairn://{}/{}/{}/{}/terminal/{}",
                    project, number, exec_seq, node_id, slug
                )
            }

            // Task-level (exec_seq is always required)
            CairnResource::TaskChat {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => {
                format!(
                    "cairn://{}/{}/{}/{}/task/{}/chat",
                    project, number, exec_seq, node_id, task_name
                )
            }
            CairnResource::TaskChatFull {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => {
                format!(
                    "cairn://{}/{}/{}/{}/task/{}/chat/full",
                    project, number, exec_seq, node_id, task_name
                )
            }
            CairnResource::TaskChatEvent {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                run_seq,
                event_seq,
            } => {
                format!(
                    "cairn://{}/{}/{}/{}/task/{}/chat/{}/{}",
                    project, number, exec_seq, node_id, task_name, run_seq, event_seq
                )
            }
            CairnResource::TaskArtifact {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => {
                format!(
                    "cairn://{}/{}/{}/{}/task/{}/artifact",
                    project, number, exec_seq, node_id, task_name
                )
            }

            // Messages
            CairnResource::ProjectMessages { project } => {
                format!("cairn://{}/messages", project)
            }
            CairnResource::IssueMessages { project, number } => {
                format!("cairn://{}/{}/messages", project, number)
            }

            // File changes
            CairnResource::Files { project, number } => {
                format!("cairn://{}/{}/files", project, number)
            }
            CairnResource::NodeFiles {
                project,
                number,
                exec_seq,
                node_id,
            } => {
                format!(
                    "cairn://{}/{}/{}/{}/files",
                    project, number, exec_seq, node_id
                )
            }

            // Project-level
            CairnResource::ProjectTerminal { project, slug } => {
                format!("cairn://{}/terminal/{}", project, slug)
            }
            CairnResource::ProjectChat { project, name } => {
                format!("cairn://{}/chat/{}", project, name)
            }
        }
    }

    /// Convert this resource to a frontend route path.
    ///
    /// Transformation rules:
    /// - `cairn://` → `/p/`
    /// - Project uppercase → lowercase
    /// - Issue number gets `/i/` prefix
    /// - exec_seq is always included for node-scoped routes
    pub fn to_route(&self) -> String {
        match self {
            // Project-level overview
            CairnResource::Project { project } => {
                format!("/p/{}", project.to_lowercase())
            }

            // Issue-level
            CairnResource::Issue { project, number } => {
                format!("/p/{}/i/{}", project.to_lowercase(), number)
            }

            // Node-level (exec_seq is always required)
            CairnResource::Node {
                project,
                number,
                exec_seq,
                node_id,
            } => {
                // Bare node routes redirect to chat
                format!(
                    "/p/{}/i/{}/{}/{}/chat",
                    project.to_lowercase(),
                    number,
                    exec_seq,
                    node_id
                )
            }
            CairnResource::NodeChat {
                project,
                number,
                exec_seq,
                node_id,
            } => {
                format!(
                    "/p/{}/i/{}/{}/{}/chat",
                    project.to_lowercase(),
                    number,
                    exec_seq,
                    node_id
                )
            }
            CairnResource::NodeChatFull {
                project,
                number,
                exec_seq,
                node_id,
            } => {
                format!(
                    "/p/{}/i/{}/{}/{}/chat/full",
                    project.to_lowercase(),
                    number,
                    exec_seq,
                    node_id
                )
            }
            CairnResource::NodeChatEvent {
                project,
                number,
                exec_seq,
                node_id,
                run_seq,
                event_seq,
            } => {
                format!(
                    "/p/{}/i/{}/{}/{}/chat/{}/{}",
                    project.to_lowercase(),
                    number,
                    exec_seq,
                    node_id,
                    run_seq,
                    event_seq
                )
            }
            CairnResource::NodeArtifact {
                project,
                number,
                exec_seq,
                node_id,
            } => {
                format!(
                    "/p/{}/i/{}/{}/{}/artifact",
                    project.to_lowercase(),
                    number,
                    exec_seq,
                    node_id
                )
            }
            CairnResource::NodeTerminal {
                project,
                number,
                exec_seq,
                node_id,
                slug,
            } => {
                format!(
                    "/p/{}/i/{}/{}/{}/terminal/{}",
                    project.to_lowercase(),
                    number,
                    exec_seq,
                    node_id,
                    slug
                )
            }

            // Task-level (exec_seq is always required)
            CairnResource::TaskChat {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => {
                format!(
                    "/p/{}/i/{}/{}/{}/task/{}/chat",
                    project.to_lowercase(),
                    number,
                    exec_seq,
                    node_id,
                    task_name
                )
            }
            CairnResource::TaskChatFull {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => {
                format!(
                    "/p/{}/i/{}/{}/{}/task/{}/chat/full",
                    project.to_lowercase(),
                    number,
                    exec_seq,
                    node_id,
                    task_name
                )
            }
            CairnResource::TaskChatEvent {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                run_seq,
                event_seq,
            } => {
                format!(
                    "/p/{}/i/{}/{}/{}/task/{}/chat/{}/{}",
                    project.to_lowercase(),
                    number,
                    exec_seq,
                    node_id,
                    task_name,
                    run_seq,
                    event_seq
                )
            }
            CairnResource::TaskArtifact {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => {
                format!(
                    "/p/{}/i/{}/{}/{}/task/{}/artifact",
                    project.to_lowercase(),
                    number,
                    exec_seq,
                    node_id,
                    task_name
                )
            }

            // Messages (route to issue/project view)
            CairnResource::ProjectMessages { project } => {
                format!("/p/{}", project.to_lowercase())
            }
            CairnResource::IssueMessages { project, number } => {
                format!("/p/{}/i/{}", project.to_lowercase(), number)
            }

            // File changes (route to issue view)
            CairnResource::Files { project, number } => {
                format!("/p/{}/i/{}", project.to_lowercase(), number)
            }
            CairnResource::NodeFiles {
                project,
                number,
                exec_seq,
                node_id,
                ..
            } => {
                format!(
                    "/p/{}/i/{}/{}/{}",
                    project.to_lowercase(),
                    number,
                    exec_seq,
                    node_id
                )
            }

            // Project-level
            CairnResource::ProjectTerminal { project, slug } => {
                format!("/p/{}/terminal/{}", project.to_lowercase(), slug)
            }
            CairnResource::ProjectChat { project, name } => {
                format!("/p/{}/chat/{}", project.to_lowercase(), name)
            }
        }
    }

    /// Get the project key for this resource.
    pub fn project(&self) -> &str {
        match self {
            CairnResource::Project { project }
            | CairnResource::Issue { project, .. }
            | CairnResource::Node { project, .. }
            | CairnResource::NodeChat { project, .. }
            | CairnResource::NodeChatFull { project, .. }
            | CairnResource::NodeChatEvent { project, .. }
            | CairnResource::NodeArtifact { project, .. }
            | CairnResource::NodeTerminal { project, .. }
            | CairnResource::TaskChat { project, .. }
            | CairnResource::TaskChatFull { project, .. }
            | CairnResource::TaskChatEvent { project, .. }
            | CairnResource::TaskArtifact { project, .. }
            | CairnResource::ProjectMessages { project, .. }
            | CairnResource::IssueMessages { project, .. }
            | CairnResource::Files { project, .. }
            | CairnResource::NodeFiles { project, .. }
            | CairnResource::ProjectTerminal { project, .. }
            | CairnResource::ProjectChat { project, .. } => project,
        }
    }

    /// Get the issue number if this resource is issue-scoped.
    pub fn issue_number(&self) -> Option<i32> {
        match self {
            CairnResource::Issue { number, .. }
            | CairnResource::Node { number, .. }
            | CairnResource::NodeChat { number, .. }
            | CairnResource::NodeChatFull { number, .. }
            | CairnResource::NodeChatEvent { number, .. }
            | CairnResource::NodeArtifact { number, .. }
            | CairnResource::NodeTerminal { number, .. }
            | CairnResource::TaskChat { number, .. }
            | CairnResource::TaskChatFull { number, .. }
            | CairnResource::TaskChatEvent { number, .. }
            | CairnResource::TaskArtifact { number, .. }
            | CairnResource::IssueMessages { number, .. }
            | CairnResource::Files { number, .. }
            | CairnResource::NodeFiles { number, .. } => Some(*number),

            CairnResource::Project { .. }
            | CairnResource::ProjectMessages { .. }
            | CairnResource::ProjectTerminal { .. }
            | CairnResource::ProjectChat { .. } => None,
        }
    }

    /// Get the node ID if this resource is node-scoped.
    pub fn node_id(&self) -> Option<&str> {
        match self {
            CairnResource::Node { node_id, .. }
            | CairnResource::NodeChat { node_id, .. }
            | CairnResource::NodeChatFull { node_id, .. }
            | CairnResource::NodeChatEvent { node_id, .. }
            | CairnResource::NodeArtifact { node_id, .. }
            | CairnResource::NodeTerminal { node_id, .. }
            | CairnResource::TaskChat { node_id, .. }
            | CairnResource::TaskChatFull { node_id, .. }
            | CairnResource::TaskChatEvent { node_id, .. }
            | CairnResource::TaskArtifact { node_id, .. }
            | CairnResource::NodeFiles { node_id, .. } => Some(node_id),

            _ => None,
        }
    }
}

/// Parse a cairn:// URI string into a CairnResource.
///
/// Returns `None` if the URI is invalid or doesn't match the expected format.
pub fn parse_uri(uri: &str) -> Option<CairnResource> {
    let uri = uri.strip_prefix("cairn://")?;

    // Strip query string if present
    let path = if let Some(idx) = uri.find('?') {
        &uri[..idx]
    } else {
        uri
    };

    let parts: Vec<&str> = path.split('/').collect();

    if parts.is_empty() {
        return None;
    }

    let project = parts[0].to_uppercase();

    match parts.as_slice() {
        // Empty path - invalid
        [] => None,

        // cairn://PROJECT - project overview
        [_project] => Some(CairnResource::Project { project }),

        // cairn://PROJECT/terminal/SLUG - project-scoped terminal
        [_project, "terminal", slug] => Some(CairnResource::ProjectTerminal {
            project,
            slug: slug.to_string(),
        }),

        // cairn://PROJECT/messages - project messages
        [_project, "messages"] => Some(CairnResource::ProjectMessages { project }),

        // cairn://PROJECT/chat/NAME - project chat session
        [_project, "chat", name] => Some(CairnResource::ProjectChat {
            project,
            name: name.to_string(),
        }),

        // cairn://PROJECT/NUMBER - issue overview
        [_project, number_str] => {
            let number = number_str.parse().ok()?;
            Some(CairnResource::Issue { project, number })
        }

        // cairn://PROJECT/NUMBER/... (issue-scoped resources)
        [_project, number_str, rest @ ..] => {
            let number = number_str.parse().ok()?;
            parse_issue_scoped(&project, number, rest)
        }
    }
}

/// Parse issue-scoped resources (everything after PROJECT/NUMBER)
///
/// Node-scoped URIs require exec_seq:
/// - `cairn://PROJECT/NUMBER/EXEC/NODE` - exec_seq is required
///
/// Old format URIs without exec_seq are rejected.
fn parse_issue_scoped(project: &str, number: i32, parts: &[&str]) -> Option<CairnResource> {
    let project = project.to_string();

    // cairn://PROJECT/NUMBER/files — issue-level file changes
    if parts == ["files"] {
        return Some(CairnResource::Files { project, number });
    }

    // cairn://PROJECT/NUMBER/messages — issue-level messages
    if parts == ["messages"] {
        return Some(CairnResource::IssueMessages { project, number });
    }

    // Node routes require exec_seq as the first part
    // Must have at least 2 parts: exec_seq and node_id
    if parts.len() < 2 {
        return None;
    }

    // First part must be a positive integer (exec_seq)
    let exec_seq: i32 = parts[0].parse::<i32>().ok().filter(|&seq| seq > 0)?;
    let node_parts = &parts[1..];

    match node_parts {
        // cairn://PROJECT/NUMBER/EXEC/NODE
        [node_id] => Some(CairnResource::Node {
            project,
            number,
            exec_seq,
            node_id: node_id.to_string(),
        }),

        // cairn://PROJECT/NUMBER/EXEC/NODE/chat
        [node_id, "chat"] => Some(CairnResource::NodeChat {
            project,
            number,
            exec_seq,
            node_id: node_id.to_string(),
        }),

        // cairn://PROJECT/NUMBER/EXEC/NODE/chat/full
        [node_id, "chat", "full"] => Some(CairnResource::NodeChatFull {
            project,
            number,
            exec_seq,
            node_id: node_id.to_string(),
        }),

        // cairn://PROJECT/NUMBER/EXEC/NODE/chat/RUN_SEQ/EVENT_SEQ
        [node_id, "chat", run_seq_str, event_seq_str] => {
            let run_seq = run_seq_str.parse().ok()?;
            let event_seq = event_seq_str.parse().ok()?;
            Some(CairnResource::NodeChatEvent {
                project,
                number,
                exec_seq,
                node_id: node_id.to_string(),
                run_seq,
                event_seq,
            })
        }

        // cairn://PROJECT/NUMBER/EXEC/NODE/artifact
        [node_id, "artifact"] => Some(CairnResource::NodeArtifact {
            project,
            number,
            exec_seq,
            node_id: node_id.to_string(),
        }),

        // cairn://PROJECT/NUMBER/EXEC/NODE/files
        [node_id, "files"] => Some(CairnResource::NodeFiles {
            project,
            number,
            exec_seq,
            node_id: node_id.to_string(),
        }),

        // cairn://PROJECT/NUMBER/EXEC/NODE/terminal/SLUG
        [node_id, "terminal", slug] => Some(CairnResource::NodeTerminal {
            project,
            number,
            exec_seq,
            node_id: node_id.to_string(),
            slug: slug.to_string(),
        }),

        // cairn://PROJECT/NUMBER/EXEC/NODE/task/NAME/chat
        [node_id, "task", task_name, "chat"] => Some(CairnResource::TaskChat {
            project,
            number,
            exec_seq,
            node_id: node_id.to_string(),
            task_name: task_name.to_string(),
        }),

        // cairn://PROJECT/NUMBER/EXEC/NODE/task/NAME/chat/full
        [node_id, "task", task_name, "chat", "full"] => Some(CairnResource::TaskChatFull {
            project,
            number,
            exec_seq,
            node_id: node_id.to_string(),
            task_name: task_name.to_string(),
        }),

        // cairn://PROJECT/NUMBER/EXEC/NODE/task/NAME/chat/RUN_SEQ/EVENT_SEQ
        [node_id, "task", task_name, "chat", run_seq_str, event_seq_str] => {
            let run_seq = run_seq_str.parse().ok()?;
            let event_seq = event_seq_str.parse().ok()?;
            Some(CairnResource::TaskChatEvent {
                project,
                number,
                exec_seq,
                node_id: node_id.to_string(),
                task_name: task_name.to_string(),
                run_seq,
                event_seq,
            })
        }

        // cairn://PROJECT/NUMBER/EXEC/NODE/task/NAME/artifact
        [node_id, "task", task_name, "artifact"] => Some(CairnResource::TaskArtifact {
            project,
            number,
            exec_seq,
            node_id: node_id.to_string(),
            task_name: task_name.to_string(),
        }),

        _ => None,
    }
}

/// Build a terminal URI for a job-scoped terminal.
///
/// Format: `cairn://PROJECT/NUMBER/EXEC/NODE/terminal/SLUG`
/// exec_seq is required for all node-scoped URIs.
pub fn build_node_terminal_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    slug: &str,
) -> String {
    CairnResource::NodeTerminal {
        project: project.to_uppercase(),
        number,
        exec_seq,
        node_id: node_id.to_string(),
        slug: slug.to_string(),
    }
    .to_uri()
}

/// Build a terminal URI for a project-scoped terminal.
///
/// Format: `cairn://PROJECT/terminal/SLUG`
pub fn build_project_terminal_uri(project: &str, slug: &str) -> String {
    CairnResource::ProjectTerminal {
        project: project.to_uppercase(),
        slug: slug.to_string(),
    }
    .to_uri()
}

#[cfg(test)]
mod tests {
    use super::*;

    // === Issue-level parsing ===

    #[test]
    fn test_parse_issue() {
        let result = parse_uri("cairn://CAIRN/123").unwrap();
        assert_eq!(
            result,
            CairnResource::Issue {
                project: "CAIRN".to_string(),
                number: 123,
            }
        );
    }

    #[test]
    fn test_parse_issue_lowercase_project() {
        let result = parse_uri("cairn://cairn/123").unwrap();
        assert_eq!(
            result,
            CairnResource::Issue {
                project: "CAIRN".to_string(),
                number: 123,
            }
        );
    }

    // === Node-level parsing (exec_seq required) ===

    #[test]
    fn test_parse_node() {
        let result = parse_uri("cairn://CAIRN/123/1/planner-1").unwrap();
        assert_eq!(
            result,
            CairnResource::Node {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "planner-1".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_node_chat() {
        let result = parse_uri("cairn://CAIRN/123/1/builder-1/chat").unwrap();
        assert_eq!(
            result,
            CairnResource::NodeChat {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_node_chat_full() {
        let result = parse_uri("cairn://CAIRN/123/1/builder-1/chat/full").unwrap();
        assert_eq!(
            result,
            CairnResource::NodeChatFull {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_node_chat_event() {
        let result = parse_uri("cairn://CAIRN/123/1/builder-1/chat/1/5").unwrap();
        assert_eq!(
            result,
            CairnResource::NodeChatEvent {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
                run_seq: 1,
                event_seq: 5,
            }
        );
    }

    #[test]
    fn test_parse_node_artifact() {
        let result = parse_uri("cairn://CAIRN/123/1/planner-1/artifact").unwrap();
        assert_eq!(
            result,
            CairnResource::NodeArtifact {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "planner-1".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_node_terminal() {
        let result = parse_uri("cairn://CAIRN/123/1/builder-1/terminal/dev-server").unwrap();
        assert_eq!(
            result,
            CairnResource::NodeTerminal {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
                slug: "dev-server".to_string(),
            }
        );
    }

    // === Task-level parsing (exec_seq required) ===

    #[test]
    fn test_parse_task_chat() {
        let result = parse_uri("cairn://CAIRN/123/1/builder-1/task/Explore/chat").unwrap();
        assert_eq!(
            result,
            CairnResource::TaskChat {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
                task_name: "Explore".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_task_chat_full() {
        let result = parse_uri("cairn://CAIRN/123/1/builder-1/task/Explore/chat/full").unwrap();
        assert_eq!(
            result,
            CairnResource::TaskChatFull {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
                task_name: "Explore".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_task_chat_event() {
        let result = parse_uri("cairn://CAIRN/123/1/builder-1/task/Explore/chat/2/10").unwrap();
        assert_eq!(
            result,
            CairnResource::TaskChatEvent {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
                task_name: "Explore".to_string(),
                run_seq: 2,
                event_seq: 10,
            }
        );
    }

    #[test]
    fn test_parse_task_artifact() {
        let result = parse_uri("cairn://CAIRN/123/1/builder-1/task/Explore/artifact").unwrap();
        assert_eq!(
            result,
            CairnResource::TaskArtifact {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
                task_name: "Explore".to_string(),
            }
        );
    }

    // === Old format URIs (without exec_seq) should fail ===

    #[test]
    fn test_parse_old_format_node_fails() {
        // Old format without exec_seq should return None
        assert!(parse_uri("cairn://CAIRN/123/planner-1").is_none());
        assert!(parse_uri("cairn://CAIRN/123/builder-1/chat").is_none());
        assert!(parse_uri("cairn://CAIRN/123/builder-1/artifact").is_none());
        assert!(parse_uri("cairn://CAIRN/123/builder-1/terminal/dev-server").is_none());
        assert!(parse_uri("cairn://CAIRN/123/builder-1/task/Explore/chat").is_none());
    }

    // === Project-level parsing ===

    #[test]
    fn test_parse_project_terminal() {
        let result = parse_uri("cairn://CAIRN/terminal/build").unwrap();
        assert_eq!(
            result,
            CairnResource::ProjectTerminal {
                project: "CAIRN".to_string(),
                slug: "build".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_project_chat() {
        let result = parse_uri("cairn://CAIRN/chat/api-design").unwrap();
        assert_eq!(
            result,
            CairnResource::ProjectChat {
                project: "CAIRN".to_string(),
                name: "api-design".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_issue_files() {
        let result = parse_uri("cairn://CAIRN/123/files").unwrap();
        assert_eq!(
            result,
            CairnResource::Files {
                project: "CAIRN".to_string(),
                number: 123,
            }
        );
    }

    #[test]
    fn test_parse_node_files() {
        let result = parse_uri("cairn://CAIRN/123/1/builder-1/files").unwrap();
        assert_eq!(
            result,
            CairnResource::NodeFiles {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
            }
        );
    }

    // === Invalid URIs ===

    #[test]
    fn test_parse_invalid_scheme() {
        assert!(parse_uri("issue://CAIRN/123").is_none());
        assert!(parse_uri("terminal://CAIRN/main/dev").is_none());
        assert!(parse_uri("file:///test.txt").is_none());
    }

    #[test]
    fn test_parse_invalid_number() {
        assert!(parse_uri("cairn://CAIRN/abc").is_none());
        assert!(parse_uri("cairn://CAIRN/").is_none());
    }

    #[test]
    fn test_parse_project() {
        let result = parse_uri("cairn://CAIRN").unwrap();
        assert_eq!(
            result,
            CairnResource::Project {
                project: "CAIRN".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_project_lowercase() {
        let result = parse_uri("cairn://cairn").unwrap();
        assert_eq!(
            result,
            CairnResource::Project {
                project: "CAIRN".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_with_query_string() {
        let result = parse_uri("cairn://CAIRN/123?foo=bar").unwrap();
        assert_eq!(
            result,
            CairnResource::Issue {
                project: "CAIRN".to_string(),
                number: 123,
            }
        );
    }

    // === Roundtrip tests ===

    #[test]
    fn test_roundtrip_all_variants() {
        let resources = vec![
            CairnResource::Project {
                project: "CAIRN".to_string(),
            },
            CairnResource::Issue {
                project: "CAIRN".to_string(),
                number: 123,
            },
            CairnResource::Node {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "planner-1".to_string(),
            },
            CairnResource::NodeChat {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
            },
            CairnResource::NodeChatFull {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
            },
            CairnResource::NodeChatEvent {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
                run_seq: 1,
                event_seq: 5,
            },
            CairnResource::NodeArtifact {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "planner-1".to_string(),
            },
            CairnResource::NodeTerminal {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
                slug: "dev-server".to_string(),
            },
            CairnResource::TaskChat {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
                task_name: "Explore".to_string(),
            },
            CairnResource::TaskChatFull {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
                task_name: "Explore".to_string(),
            },
            CairnResource::TaskChatEvent {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
                task_name: "Explore".to_string(),
                run_seq: 2,
                event_seq: 10,
            },
            CairnResource::TaskArtifact {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
                task_name: "Explore".to_string(),
            },
            CairnResource::ProjectMessages {
                project: "CAIRN".to_string(),
            },
            CairnResource::IssueMessages {
                project: "CAIRN".to_string(),
                number: 123,
            },
            CairnResource::Files {
                project: "CAIRN".to_string(),
                number: 123,
            },
            CairnResource::NodeFiles {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
            },
            CairnResource::ProjectTerminal {
                project: "CAIRN".to_string(),
                slug: "build".to_string(),
            },
            CairnResource::ProjectChat {
                project: "CAIRN".to_string(),
                name: "api-design".to_string(),
            },
        ];

        for resource in resources {
            let uri = resource.to_uri();
            let parsed = parse_uri(&uri).expect(&format!("Failed to parse: {}", uri));
            assert_eq!(resource, parsed, "Roundtrip failed for {}", uri);
        }
    }

    // === Route conversion tests ===

    #[test]
    fn test_to_route_project() {
        let resource = CairnResource::Project {
            project: "CAIRN".to_string(),
        };
        assert_eq!(resource.to_route(), "/p/cairn");
    }

    #[test]
    fn test_to_route_issue() {
        let resource = CairnResource::Issue {
            project: "CAIRN".to_string(),
            number: 123,
        };
        assert_eq!(resource.to_route(), "/p/cairn/i/123");
    }

    #[test]
    fn test_to_route_node_chat() {
        let resource = CairnResource::NodeChat {
            project: "CAIRN".to_string(),
            number: 123,
            exec_seq: 1,
            node_id: "planner-1".to_string(),
        };
        assert_eq!(resource.to_route(), "/p/cairn/i/123/1/planner-1/chat");
    }

    #[test]
    fn test_to_route_node_redirects_to_chat() {
        let resource = CairnResource::Node {
            project: "CAIRN".to_string(),
            number: 123,
            exec_seq: 1,
            node_id: "planner-1".to_string(),
        };
        // Bare node routes should redirect to chat
        assert_eq!(resource.to_route(), "/p/cairn/i/123/1/planner-1/chat");
    }

    #[test]
    fn test_to_route_node_terminal() {
        let resource = CairnResource::NodeTerminal {
            project: "CAIRN".to_string(),
            number: 123,
            exec_seq: 1,
            node_id: "builder-1".to_string(),
            slug: "dev-server".to_string(),
        };
        assert_eq!(
            resource.to_route(),
            "/p/cairn/i/123/1/builder-1/terminal/dev-server"
        );
    }

    #[test]
    fn test_to_route_task_chat() {
        let resource = CairnResource::TaskChat {
            project: "CAIRN".to_string(),
            number: 123,
            exec_seq: 1,
            node_id: "builder-1".to_string(),
            task_name: "Explore".to_string(),
        };
        assert_eq!(
            resource.to_route(),
            "/p/cairn/i/123/1/builder-1/task/Explore/chat"
        );
    }

    #[test]
    fn test_to_route_project_terminal() {
        let resource = CairnResource::ProjectTerminal {
            project: "CAIRN".to_string(),
            slug: "build".to_string(),
        };
        assert_eq!(resource.to_route(), "/p/cairn/terminal/build");
    }

    #[test]
    fn test_to_route_project_chat() {
        let resource = CairnResource::ProjectChat {
            project: "CAIRN".to_string(),
            name: "api-design".to_string(),
        };
        assert_eq!(resource.to_route(), "/p/cairn/chat/api-design");
    }

    // === Route tests with exec_seq (various values) ===

    #[test]
    fn test_to_route_node_chat_exec_seq_3() {
        let resource = CairnResource::NodeChat {
            project: "CAIRN".to_string(),
            number: 123,
            exec_seq: 3,
            node_id: "planner-1".to_string(),
        };
        assert_eq!(resource.to_route(), "/p/cairn/i/123/3/planner-1/chat");
    }

    #[test]
    fn test_to_route_node_terminal_exec_seq_5() {
        let resource = CairnResource::NodeTerminal {
            project: "CAIRN".to_string(),
            number: 123,
            exec_seq: 5,
            node_id: "builder-1".to_string(),
            slug: "dev-server".to_string(),
        };
        assert_eq!(
            resource.to_route(),
            "/p/cairn/i/123/5/builder-1/terminal/dev-server"
        );
    }

    #[test]
    fn test_to_route_node_artifact_exec_seq_2() {
        let resource = CairnResource::NodeArtifact {
            project: "CAIRN".to_string(),
            number: 123,
            exec_seq: 2,
            node_id: "planner-1".to_string(),
        };
        assert_eq!(resource.to_route(), "/p/cairn/i/123/2/planner-1/artifact");
    }

    // === Helper function tests ===

    #[test]
    fn test_build_node_terminal_uri() {
        let uri = build_node_terminal_uri("cairn", 123, 1, "builder-1", "dev-server");
        assert_eq!(uri, "cairn://CAIRN/123/1/builder-1/terminal/dev-server");
    }

    #[test]
    fn test_build_node_terminal_uri_exec_seq_5() {
        let uri = build_node_terminal_uri("cairn", 123, 5, "builder-1", "dev-server");
        assert_eq!(uri, "cairn://CAIRN/123/5/builder-1/terminal/dev-server");
    }

    #[test]
    fn test_build_project_terminal_uri() {
        let uri = build_project_terminal_uri("cairn", "build");
        assert_eq!(uri, "cairn://CAIRN/terminal/build");
    }

    #[test]
    fn test_accessor_methods() {
        let resource = CairnResource::NodeTerminal {
            project: "CAIRN".to_string(),
            number: 123,
            exec_seq: 1,
            node_id: "builder-1".to_string(),
            slug: "dev-server".to_string(),
        };

        assert_eq!(resource.project(), "CAIRN");
        assert_eq!(resource.issue_number(), Some(123));
        assert_eq!(resource.node_id(), Some("builder-1"));
    }

    #[test]
    fn test_accessor_methods_project_level() {
        let resource = CairnResource::ProjectTerminal {
            project: "CAIRN".to_string(),
            slug: "build".to_string(),
        };

        assert_eq!(resource.project(), "CAIRN");
        assert_eq!(resource.issue_number(), None);
        assert_eq!(resource.node_id(), None);
    }

    // === URI parsing tests with various exec_seq values ===

    #[test]
    fn test_parse_node_exec_seq_3() {
        let result = parse_uri("cairn://CAIRN/123/3/planner-1").unwrap();
        assert_eq!(
            result,
            CairnResource::Node {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 3,
                node_id: "planner-1".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_node_chat_exec_seq_5() {
        let result = parse_uri("cairn://CAIRN/123/5/builder-1/chat").unwrap();
        assert_eq!(
            result,
            CairnResource::NodeChat {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 5,
                node_id: "builder-1".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_node_terminal_exec_seq_2() {
        let result = parse_uri("cairn://CAIRN/123/2/builder-1/terminal/dev-server").unwrap();
        assert_eq!(
            result,
            CairnResource::NodeTerminal {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 2,
                node_id: "builder-1".to_string(),
                slug: "dev-server".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_node_artifact_exec_seq_7() {
        let result = parse_uri("cairn://CAIRN/123/7/planner-1/artifact").unwrap();
        assert_eq!(
            result,
            CairnResource::NodeArtifact {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 7,
                node_id: "planner-1".to_string(),
            }
        );
    }

    // === Invalid exec_seq values should fail ===

    #[test]
    fn test_exec_seq_zero_fails() {
        // exec_seq must be positive, so 0 should fail to parse
        assert!(parse_uri("cairn://CAIRN/123/0/chat").is_none());
    }

    #[test]
    fn test_exec_seq_negative_fails() {
        // exec_seq must be positive, so negative numbers should fail to parse
        assert!(parse_uri("cairn://CAIRN/123/-1/chat").is_none());
    }

    #[test]
    fn test_roundtrip_various_exec_seq() {
        let resources = vec![
            CairnResource::Node {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 3,
                node_id: "planner-1".to_string(),
            },
            CairnResource::NodeChat {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 5,
                node_id: "builder-1".to_string(),
            },
            CairnResource::NodeTerminal {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 2,
                node_id: "builder-1".to_string(),
                slug: "dev-server".to_string(),
            },
            CairnResource::TaskChat {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 1,
                node_id: "builder-1".to_string(),
                task_name: "Explore".to_string(),
            },
            CairnResource::NodeFiles {
                project: "CAIRN".to_string(),
                number: 123,
                exec_seq: 4,
                node_id: "builder-1".to_string(),
            },
        ];

        for resource in resources {
            let uri = resource.to_uri();
            let parsed = parse_uri(&uri).expect(&format!("Failed to parse: {}", uri));
            assert_eq!(resource, parsed, "Roundtrip failed for {}", uri);
        }
    }
}
