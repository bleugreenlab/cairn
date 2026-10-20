/// Resolved input from an upstream job's artifact.
#[derive(Debug, Clone)]
pub struct ResolvedInput {
    pub artifact_type: String,
    pub data: serde_json::Value,
    /// The stored artifact version this input resolved to. `None` for trigger
    /// context, static content, and transcript fallbacks: those have no version
    /// chain a later write could supersede.
    pub source: Option<ArtifactInputSource>,
}

/// Which stored artifact version a launch input resolved to, and how the
/// consumer receives it. This is the structural record of what a job was
/// launched with, so a later revision of the same artifact can be delivered to
/// it as a diff against exactly the version it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactInputSource {
    pub producer_job_id: String,
    /// The artifact's name, the key of its version chain on the producer.
    pub output_name: String,
    pub version: i64,
    /// The artifact's canonical resource URI: the producer's home URI plus the
    /// artifact name, the same address the write path reports.
    pub uri: String,
    pub delivery: InputDelivery,
}

/// How an artifact input reaches its consumer, decided by the port its context
/// edge lands on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputDelivery {
    /// `context-in`: the content is embedded in the launch prompt.
    Content,
    /// `context-ref`: only the URI and version are given; the consumer reads
    /// the current version itself.
    Reference,
}

/// The line set beside an embedded artifact naming where it lives and which
/// version the prompt holds. The text below it is a launch snapshot; the
/// artifact is what stays true.
fn authority_note(source: &ArtifactInputSource) -> String {
    format!(
        "> Embedded below: version {} of `{}`, as it stood when you launched. The artifact is authoritative. If it is revised while you work, the revision reaches you as a message carrying the changes, and it supersedes this copy.",
        source.version, source.uri
    )
}

/// A referenced artifact: its identity and the version current at launch, never
/// its content, so nothing in the prompt can be mistaken for the current text.
fn format_reference(input: &ResolvedInput, source: &ArtifactInputSource) -> String {
    let heading = input
        .data
        .get("title")
        .and_then(|v| v.as_str())
        .map(|title| format!("## Artifact of record `{}`: {title}", source.output_name))
        .unwrap_or_else(|| format!("## Artifact of record `{}`", source.output_name));
    format!(
        "{heading}\n\n`{}` (version {} when you launched). Read it there. Its current version is authoritative over any copy of it quoted elsewhere, including the text another node was launched with. If it is revised while you work, the revision reaches you as a message.",
        source.uri, source.version
    )
}

/// Format resolved inputs as markdown for injection into agent prompt.
pub fn format_resolved_inputs(inputs: &[ResolvedInput]) -> String {
    if inputs.is_empty() {
        return String::new();
    }

    let mut sections = Vec::new();

    for input in inputs {
        if let Some(source) = input
            .source
            .as_ref()
            .filter(|source| source.delivery == InputDelivery::Reference)
        {
            sections.push(format_reference(input, source));
            continue;
        }
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

                    let is_thread = issue.get("kind").and_then(|v| v.as_str()) == Some("thread");

                    if has_event {
                        let mut section = format!(
                            "## Source Issue\n\nThe following issue's job has ended. This is the issue that was worked on:\n\n**{}**",
                            title
                        );
                        if !description.is_empty() {
                            section.push_str(&format!("\n\n{}", description));
                        }
                        parts.push(section);
                    } else if is_thread && !description.is_empty() {
                        // A thread opens with what the user said, verbatim. Its
                        // title is a NAME for a place — usually derived from this
                        // very message — so leading with it as a heading states
                        // the message twice and dresses a conversation up as a
                        // work item. An issue keeps the heading: there the title
                        // is the objective, and the body is its detail.
                        parts.push(description.to_string());
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
        let section = match input.source.as_ref() {
            Some(source) => format!("{}\n\n{section}", authority_note(source)),
            None => section,
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
            source: None,
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
    fn a_thread_opens_with_the_message_itself() {
        // The thread's title is a name derived from this same message; rendering
        // it as a heading above the message states it twice and reads as a work
        // item rather than as something the user said.
        let inputs = vec![ResolvedInput {
            source: None,
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "issue": {
                    "id": "iss-1",
                    "title": "where should placement live",
                    "description": "where should placement live",
                    "kind": "thread",
                }
            }),
        }];
        let output = format_resolved_inputs(&inputs);
        assert_eq!(output, "where should placement live");
    }

    #[test]
    fn a_thread_with_nothing_said_still_names_itself() {
        let inputs = vec![ResolvedInput {
            source: None,
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "issue": {
                    "id": "iss-1",
                    "title": "Executor placement",
                    "description": null,
                    "kind": "thread",
                }
            }),
        }];
        assert_eq!(format_resolved_inputs(&inputs), "# Executor placement");
    }

    #[test]
    fn an_issue_keeps_its_title_heading() {
        // An issue's title is its objective and the body is the detail, so the
        // heading is carrying real information there.
        let inputs = vec![ResolvedInput {
            source: None,
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "issue": {
                    "id": "iss-1",
                    "title": "Fix the bug",
                    "description": "It crashes on startup",
                    "kind": "issue",
                }
            }),
        }];
        assert_eq!(
            format_resolved_inputs(&inputs),
            "# Fix the bug\n\nIt crashes on startup"
        );
    }

    #[test]
    fn format_trigger_context_with_event_only() {
        let inputs = vec![ResolvedInput {
            source: None,
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
            source: None,
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
            source: None,
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
            source: None,
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
            source: None,
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "issues": [
                    {
                        "id": "iss-1",
                        "key": "proj-42",
                        "title": "Fix login",
                        "description": "Login page crashes",
                    },
                    {
                        "id": "iss-2",
                        "key": "proj-55",
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
        assert!(output.contains("### proj-42 — Fix login"));
        assert!(output.contains("Login page crashes"));
        assert!(output.contains("### proj-55 — Update deps"));
        assert!(output.contains("## Trigger Event"));
    }

    fn plan_source(delivery: InputDelivery) -> ArtifactInputSource {
        ArtifactInputSource {
            producer_job_id: "j-planner".to_string(),
            output_name: "plan".to_string(),
            version: 2,
            uri: "cairn://p/cairn/5073/1/planner/plan".to_string(),
            delivery,
        }
    }

    #[test]
    fn an_embedded_artifact_names_its_uri_version_and_authority() {
        let inputs = vec![ResolvedInput {
            artifact_type: "plan".to_string(),
            data: serde_json::json!({"title": "Match colon forms", "content": "Use exact colon-prefixed Go forms."}),
            source: Some(plan_source(InputDelivery::Content)),
        }];
        let output = format_resolved_inputs(&inputs);
        assert!(
            output.contains("version 2 of `cairn://p/cairn/5073/1/planner/plan`"),
            "{output}"
        );
        assert!(output.contains("The artifact is authoritative"), "{output}");
        assert!(output.contains("reaches you as a message"), "{output}");
        // The note sits beside the text it qualifies, which is still embedded.
        assert!(output.contains("**Match colon forms**\n\nUse exact colon-prefixed Go forms."));
        assert!(output.find("authoritative") < output.find("**Match colon forms**"));
    }

    #[test]
    fn a_referenced_artifact_gives_its_identity_and_never_its_text() {
        let inputs = vec![ResolvedInput {
            artifact_type: "plan".to_string(),
            data: serde_json::json!({"title": "Match colon forms", "content": "SECRET PLAN BODY"}),
            source: Some(plan_source(InputDelivery::Reference)),
        }];
        let output = format_resolved_inputs(&inputs);
        assert!(
            output.contains("`cairn://p/cairn/5073/1/planner/plan` (version 2"),
            "{output}"
        );
        assert!(output.contains("Match colon forms"), "{output}");
        assert!(output.contains("authoritative over any copy"), "{output}");
        assert!(!output.contains("SECRET PLAN BODY"), "{output}");
    }

    #[test]
    fn an_input_without_a_stored_version_carries_no_authority_note() {
        let inputs = vec![ResolvedInput {
            artifact_type: "context".to_string(),
            data: serde_json::json!({"title": "Packet", "content": "static"}),
            source: None,
        }];
        assert_eq!(format_resolved_inputs(&inputs), "## Packet\n\nstatic");
    }
}
