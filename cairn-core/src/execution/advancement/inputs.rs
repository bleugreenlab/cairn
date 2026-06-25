/// Resolved input from an upstream job's artifact.
#[derive(Debug, Clone)]
pub struct ResolvedInput {
    pub artifact_type: String,
    pub data: serde_json::Value,
}

/// Format resolved inputs as markdown for injection into agent prompt.
pub fn format_resolved_inputs(inputs: &[ResolvedInput]) -> String {
    if inputs.is_empty() {
        return String::new();
    }

    let mut sections = Vec::new();

    for input in inputs {
        let section = match input.artifact_type.as_str() {
            "trigger_context" => {
                let has_event = input.data.get("event").is_some();
                let mut parts = Vec::new();

                // Accumulated triggers: multiple source issues
                if let Some(issues_arr) = input.data.get("issues").and_then(|v| v.as_array()) {
                    let mut section = "## Source Issues\n\nThe following issues' jobs contributed to this batch:\n".to_string();
                    for issue in issues_arr {
                        let key = issue.get("key").and_then(|v| v.as_str()).unwrap_or("???");
                        let title = issue
                            .get("title")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Untitled");
                        let description = issue
                            .get("description")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        section.push_str(&format!("\n### {} — {}", key, title));
                        if !description.is_empty() {
                            section.push_str(&format!("\n\n{}", description));
                        }
                    }
                    parts.push(section);
                } else if let Some(issue) = input.data.get("issue") {
                    // Single event trigger: one source issue
                    let title = issue
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Untitled");
                    let description = issue
                        .get("description")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");

                    if has_event {
                        let mut section = format!(
                            "## Source Issue\n\nThe following issue's job has ended. This is the issue that was worked on:\n\n**{}**",
                            title
                        );
                        if !description.is_empty() {
                            section.push_str(&format!("\n\n{}", description));
                        }
                        parts.push(section);
                    } else if description.is_empty() {
                        parts.push(format!("# {}", title));
                    } else {
                        parts.push(format!("# {}\n\n{}", title, description));
                    }
                }

                if let Some(event) = input.data.get("event") {
                    parts.push(format!(
                        "## Trigger Event\n\n```json\n{}\n```",
                        serde_json::to_string_pretty(event).unwrap_or_default()
                    ));
                }

                if parts.is_empty() {
                    serde_json::to_string_pretty(&input.data).unwrap_or_default()
                } else {
                    parts.join("\n\n")
                }
            }
            "context" => {
                if let Some(content) = input.data.get("content").and_then(|v| v.as_str()) {
                    if let Some(title) = input.data.get("title").and_then(|v| v.as_str()) {
                        format!("## {}\n\n{}", title, content)
                    } else {
                        format!("## Context\n\n{}", content)
                    }
                } else {
                    input
                        .data
                        .get("content")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_default()
                }
            }
            "plan" => {
                if let Some(content) = input.data.get("content").and_then(|v| v.as_str()) {
                    if let Some(title) = input.data.get("title").and_then(|v| v.as_str()) {
                        format!("**{}**\n\n{}", title, content)
                    } else {
                        content.to_string()
                    }
                } else {
                    format!(
                        "```json\n{}\n```",
                        serde_json::to_string_pretty(&input.data).unwrap_or_default()
                    )
                }
            }
            "tasklist" => {
                let mut parts = Vec::new();
                if let Some(objective) = input.data.get("objective").and_then(|v| v.as_str()) {
                    parts.push(format!("**Objective:** {}", objective));
                }
                if let Some(reqs) = input.data.get("requirements").and_then(|v| v.as_array()) {
                    let items: Vec<String> = reqs
                        .iter()
                        .filter_map(|r| r.as_str())
                        .map(|r| format!("- {}", r))
                        .collect();
                    if !items.is_empty() {
                        parts.push(format!("**Requirements:**\n{}", items.join("\n")));
                    }
                }
                if let Some(tasks) = input.data.get("tasks").and_then(|v| v.as_array()) {
                    let items: Vec<String> = tasks
                        .iter()
                        .filter_map(|t| {
                            let id = t.get("id").and_then(|v| v.as_str())?;
                            let title = t.get("title").and_then(|v| v.as_str())?;
                            let deps = t
                                .get("dependencies")
                                .and_then(|v| v.as_array())
                                .map(|arr| {
                                    arr.iter()
                                        .filter_map(|d| d.as_str())
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                })
                                .unwrap_or_default();
                            if deps.is_empty() {
                                Some(format!("- **{}** ({})", title, id))
                            } else {
                                Some(format!("- **{}** ({}) — depends on: {}", title, id, deps))
                            }
                        })
                        .collect();
                    parts.push(format!("**Tasks:**\n{}", items.join("\n")));
                }
                parts.join("\n\n")
            }
            _ => {
                format!(
                    "**{}**\n\n```json\n{}\n```",
                    input.artifact_type,
                    serde_json::to_string_pretty(&input.data).unwrap_or_default()
                )
            }
        };
        sections.push(section);
    }

    sections.join("\n\n")
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_trigger_context_with_issue_only() {
        let inputs = vec![ResolvedInput {
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "issue": {
                    "id": "iss-1",
                    "title": "Fix the bug",
                    "description": "It crashes on startup",
                }
            }),
        }];
        let output = format_resolved_inputs(&inputs);
        assert!(output.contains("# Fix the bug"));
        assert!(output.contains("It crashes on startup"));
        assert!(!output.contains("Trigger Event"));
    }

    #[test]
    fn format_trigger_context_with_event_only() {
        let inputs = vec![ResolvedInput {
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "event": {
                    "jobId": "j1",
                    "status": "complete",
                    "projectId": "p1",
                }
            }),
        }];
        let output = format_resolved_inputs(&inputs);
        assert!(output.contains("## Trigger Event"));
        assert!(output.contains("\"status\": \"complete\""));
        // No issue heading (h1), only the event section (h2)
        assert!(!output.starts_with("# "));
        assert!(!output.contains("\n# "));
    }

    #[test]
    fn format_trigger_context_with_issue_and_event() {
        let inputs = vec![ResolvedInput {
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "issue": {
                    "id": "iss-1",
                    "title": "Deploy monitoring",
                    "description": null,
                },
                "event": {
                    "skillId": "deploy",
                    "skillName": "Deploy",
                }
            }),
        }];
        let output = format_resolved_inputs(&inputs);
        // Event-triggered: issue framed as source context, not open problem
        assert!(output.contains("## Source Issue"));
        assert!(output.contains("**Deploy monitoring**"));
        assert!(output.contains("## Trigger Event"));
        assert!(output.contains("\"skillId\": \"deploy\""));
    }

    #[test]
    fn format_trigger_context_empty_data_falls_through() {
        let inputs = vec![ResolvedInput {
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({}),
        }];
        let output = format_resolved_inputs(&inputs);
        // Falls through to JSON pretty-print of empty object
        assert!(output.contains("{}"));
    }

    #[test]
    fn format_trigger_context_issue_without_description() {
        let inputs = vec![ResolvedInput {
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "issue": {
                    "id": "iss-1",
                    "title": "Title only",
                    "description": null,
                }
            }),
        }];
        let output = format_resolved_inputs(&inputs);
        assert!(output.contains("# Title only"));
        // Should not have a blank line after title with no description
        assert!(!output.contains("# Title only\n\n\n"));
    }

    #[test]
    fn format_trigger_context_accumulated_multiple_issues() {
        let inputs = vec![ResolvedInput {
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "issues": [
                    {
                        "id": "iss-1",
                        "key": "PROJ-42",
                        "title": "Fix login",
                        "description": "Login page crashes",
                    },
                    {
                        "id": "iss-2",
                        "key": "PROJ-55",
                        "title": "Update deps",
                        "description": null,
                    }
                ],
                "event": {
                    "accumulated": true,
                    "groupKey": "build",
                    "threshold": 2,
                    "eventCount": 2,
                    "events": [],
                }
            }),
        }];
        let output = format_resolved_inputs(&inputs);
        assert!(output.contains("## Source Issues"));
        assert!(output.contains("### PROJ-42 — Fix login"));
        assert!(output.contains("Login page crashes"));
        assert!(output.contains("### PROJ-55 — Update deps"));
        assert!(output.contains("## Trigger Event"));
    }
}
