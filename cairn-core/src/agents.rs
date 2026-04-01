//! Agent configuration import/export.
//!
//! This module handles parsing agent markdown files for import/export.
//! Supports Claude Code-compatible format.

use crate::models::{ApprovalPolicy, FilesystemScope};
use serde::{Deserialize, Serialize};

/// Agent frontmatter from markdown files (Claude Code compatible)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentFrontmatter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    pub description: String,
    /// Comma-separated list of tools (Claude Code format) or YAML array
    #[serde(deserialize_with = "deserialize_tools")]
    pub tools: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "model")]
    pub tier: Option<String>,
    /// Legacy field — kept for backward compat when reading old YAML files
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<ApprovalPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filesystem_scope: Option<FilesystemScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hooks: Option<serde_json::Value>,
    /// Tools to disallow (added to blocked list)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disallowed_tools: Option<Vec<String>>,
    /// Skills to inject into the prompt
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skills: Option<Vec<String>>,
    /// Preferred backend when multiple providers are available.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "backend", alias = "backendPreference")]
    pub backend_preference: Option<String>,
}

/// Custom deserializer that handles both comma-separated string and YAML array
fn deserialize_tools<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct ToolsVisitor;

    impl<'de> de::Visitor<'de> for ToolsVisitor {
        type Value = String;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a comma-separated string or array of strings")
        }

        fn visit_str<E>(self, value: &str) -> Result<String, E>
        where
            E: de::Error,
        {
            Ok(value.to_string())
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<String, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut tools = Vec::new();
            while let Some(tool) = seq.next_element::<String>()? {
                tools.push(tool);
            }
            Ok(tools.join(", "))
        }
    }

    deserializer.deserialize_any(ToolsVisitor)
}

/// Parsed agent with inference metadata
#[derive(Debug, Clone)]
pub struct ParsedAgent {
    pub id: String,
    pub id_generated: bool,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub tools: Vec<String>,
    pub tier: Option<String>,
    pub approval_policy: Option<ApprovalPolicy>,
    pub filesystem_scope: Option<FilesystemScope>,
    pub hooks: Option<serde_json::Value>,
    pub disallowed_tools: Option<Vec<String>>,
    pub skills: Option<Vec<String>>,
    pub backend_preference: Option<String>,
}

/// Generate a slug from a name (for ID generation)
fn slugify(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// Parse agent markdown file with YAML frontmatter
///
/// Supports Claude Code-compatible format:
/// ```markdown
/// ---
/// name: Agent Name
/// description: Description
/// tools: Read, Grep, Glob
/// model: sonnet
/// permissionMode: plan
/// ---
///
/// # Agent prompt content in markdown
/// ```
///
/// Also supports legacy Cairn format with YAML array tools.
pub fn parse_agent_markdown(content: &str) -> Result<ParsedAgent, String> {
    // Check for frontmatter delimiters
    if !content.starts_with("---\n") {
        return Err("Missing frontmatter start delimiter".to_string());
    }

    // Find the end of frontmatter
    let content_after_start = &content[4..]; // Skip first "---\n"
    let end_idx = content_after_start
        .find("\n---\n")
        .ok_or("Missing frontmatter end delimiter")?;

    let frontmatter_str = &content_after_start[..end_idx];
    let prompt = content_after_start[end_idx + 5..].trim().to_string(); // Skip "\n---\n"

    // Parse YAML frontmatter
    let frontmatter: AgentFrontmatter = serde_yaml::from_str(frontmatter_str)
        .map_err(|e| format!("Failed to parse frontmatter: {}", e))?;

    // Validate required fields
    if frontmatter.name.is_empty() {
        return Err("Agent name cannot be empty".to_string());
    }
    if frontmatter.description.is_empty() {
        return Err("Agent description cannot be empty".to_string());
    }
    if frontmatter.tools.trim().is_empty() {
        return Err("Agent must have at least one tool".to_string());
    }

    // Parse tools from comma-separated string
    let tools: Vec<String> = frontmatter
        .tools
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if tools.is_empty() {
        return Err("Agent must have at least one tool".to_string());
    }

    // Generate ID if not provided
    let (id, id_generated) = if let Some(id) = frontmatter.id {
        if id.is_empty() {
            return Err("Agent id cannot be empty if provided".to_string());
        }
        (id, false)
    } else {
        (slugify(&frontmatter.name), true)
    };

    // Prefer new fields; fall back to legacy permissionMode conversion
    let (approval_policy, filesystem_scope) =
        if frontmatter.approval_policy.is_some() || frontmatter.filesystem_scope.is_some() {
            (frontmatter.approval_policy, frontmatter.filesystem_scope)
        } else if frontmatter.permission_mode.is_some() {
            let (a, f) =
                crate::models::split_legacy_permission_mode(frontmatter.permission_mode.as_deref());
            (Some(a), Some(f))
        } else {
            (None, None)
        };

    Ok(ParsedAgent {
        id,
        id_generated,
        name: frontmatter.name,
        description: frontmatter.description,
        prompt,
        tools,
        tier: frontmatter.tier,
        approval_policy,
        filesystem_scope,
        hooks: frontmatter.hooks,
        disallowed_tools: frontmatter.disallowed_tools,
        skills: frontmatter.skills,
        backend_preference: frontmatter.backend_preference,
    })
}

/// Parameters for converting an agent to markdown
pub struct AgentExportData<'a> {
    #[allow(dead_code)]
    pub id: &'a str,
    pub name: &'a str,
    pub description: &'a str,
    pub tools: &'a [String],
    pub tier: Option<&'a str>,
    pub prompt: &'a str,
    pub approval_policy: Option<ApprovalPolicy>,
    pub filesystem_scope: Option<FilesystemScope>,
    pub disallowed_tools: Option<&'a [String]>,
    pub skills: Option<&'a [String]>,
    pub hooks: Option<&'a serde_json::Value>,
    pub backend_preference: Option<&'a str>,
}

/// Convert agent to Claude Code-compatible markdown format for export
pub fn agent_to_markdown(data: AgentExportData) -> String {
    let AgentExportData {
        id: _,
        name,
        description,
        tools,
        tier,
        prompt,
        approval_policy,
        filesystem_scope,
        disallowed_tools,
        skills,
        hooks,
        backend_preference,
    } = data;

    let mut frontmatter = format!(
        "---\nname: {}\ndescription: {}\ntools: {}\n",
        name,
        description,
        tools.join(", ")
    );

    if let Some(t) = tier {
        frontmatter.push_str(&format!("tier: {}\n", t));
    }

    if let Some(b) = backend_preference {
        frontmatter.push_str(&format!("backend: {}\n", b));
    }

    // Write approvalPolicy only when non-default (default is Ask)
    if let Some(policy) = approval_policy {
        if policy != ApprovalPolicy::Ask {
            let val = serde_json::to_string(&policy).unwrap_or_default();
            frontmatter.push_str(&format!("approvalPolicy: {}\n", val.trim_matches('"')));
        }
    }

    // Write filesystemScope only when non-default (default is CwdOnly)
    if let Some(mode) = filesystem_scope {
        if mode != FilesystemScope::CwdOnly {
            let val = serde_json::to_string(&mode).unwrap_or_default();
            frontmatter.push_str(&format!("filesystemScope: {}\n", val.trim_matches('"')));
        }
    }

    // Export disallowedTools if present
    if let Some(disallowed) = disallowed_tools {
        if !disallowed.is_empty() {
            frontmatter.push_str("disallowedTools:\n");
            for tool in disallowed {
                frontmatter.push_str(&format!("  - {}\n", tool));
            }
        }
    }

    // Export skills if present
    if let Some(skill_list) = skills {
        if !skill_list.is_empty() {
            frontmatter.push_str("skills:\n");
            for skill in skill_list {
                frontmatter.push_str(&format!("  - {}\n", skill));
            }
        }
    }

    // Export hooks if present (preserve original YAML structure)
    if let Some(hooks_val) = hooks {
        if let Ok(hooks_yaml) = serde_yaml::to_string(hooks_val) {
            // serde_yaml adds "---\n" header, skip it
            let hooks_content = hooks_yaml
                .trim_start_matches("---\n")
                .trim_start_matches("---");
            if !hooks_content.trim().is_empty() {
                frontmatter.push_str("hooks:\n");
                // Indent each line by 2 spaces
                for line in hooks_content.lines() {
                    if !line.is_empty() {
                        frontmatter.push_str(&format!("  {}\n", line));
                    }
                }
            }
        }
    }

    frontmatter.push_str("---\n\n");
    frontmatter.push_str(prompt);

    frontmatter
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_claude_code_format() {
        let content = r#"---
name: Test Agent
description: A test agent
tools: Read, Grep, Glob
model: sonnet
permissionMode: plan
---

# Test Prompt

This is the agent's system prompt.
"#;

        let result = parse_agent_markdown(content);
        assert!(result.is_ok());

        let agent = result.unwrap();
        assert_eq!(agent.name, "Test Agent");
        assert_eq!(agent.id, "test-agent");
        assert!(agent.id_generated);
        assert_eq!(agent.tools.len(), 3);
        assert_eq!(agent.tier, Some("sonnet".to_string()));
        assert!(agent.prompt.contains("# Test Prompt"));
    }

    #[test]
    fn test_parse_legacy_cairn_format() {
        let content = r#"---
id: test-agent
name: Test Agent
description: A test agent
runMode: plan
tools:
  - Read
  - Glob
model: sonnet
version: 0.5.0
---

# Test Prompt

This is the agent's system prompt.
"#;

        let result = parse_agent_markdown(content);
        assert!(result.is_ok());

        let agent = result.unwrap();
        assert_eq!(agent.id, "test-agent");
        assert!(!agent.id_generated);
        assert_eq!(agent.name, "Test Agent");
        assert_eq!(agent.tools.len(), 2);
        assert_eq!(agent.tier, Some("sonnet".to_string()));
    }

    #[test]
    fn test_tools_parsing() {
        let content = r#"---
name: Code Writer
description: Writes code
tools: Read, Write, Edit
---

Prompt
"#;

        let result = parse_agent_markdown(content);
        assert!(result.is_ok());

        let agent = result.unwrap();
        assert_eq!(agent.tools, vec!["Read", "Write", "Edit"]);
    }

    #[test]
    fn test_slugify() {
        assert_eq!(slugify("Test Agent"), "test-agent");
        assert_eq!(slugify("Code-Reviewer"), "code-reviewer");
        assert_eq!(slugify("DB Reader!"), "db-reader");
        assert_eq!(slugify("Multi   Space"), "multi-space");
    }

    #[test]
    fn test_parse_missing_frontmatter() {
        let content = "# Just a prompt\n\nNo frontmatter here";
        let result = parse_agent_markdown(content);
        assert!(result.is_err());
    }

    #[test]
    fn test_agent_to_markdown() {
        let tools = vec!["Read".to_string(), "Grep".to_string()];
        let markdown = agent_to_markdown(AgentExportData {
            id: "test-agent",
            name: "Test Agent",
            description: "A test agent",
            tools: &tools,
            tier: Some("sonnet"),
            prompt: "# Test Prompt\n\nThis is the prompt.",
            approval_policy: None,
            filesystem_scope: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            backend_preference: None,
        });

        assert!(markdown.contains("name: Test Agent"));
        assert!(markdown.contains("tools: Read, Grep"));
        assert!(markdown.contains("tier: sonnet"));
        assert!(!markdown.contains("permissionMode"));
        assert!(markdown.contains("# Test Prompt"));
        assert!(!markdown.contains("id:")); // ID not exported
    }

    #[test]
    fn test_agent_to_markdown_with_disallowed_tools_and_skills() {
        let tools = vec!["Read".to_string(), "Task".to_string()];
        let disallowed = vec!["Bash".to_string(), "WebFetch".to_string()];
        let skills = vec!["api-conventions".to_string(), "error-handling".to_string()];
        let markdown = agent_to_markdown(AgentExportData {
            id: "planner",
            name: "Planner",
            description: "Creates plans",
            tools: &tools,
            tier: None,
            prompt: "Plan stuff.",
            approval_policy: Some(ApprovalPolicy::AcceptAll),
            filesystem_scope: None,
            disallowed_tools: Some(&disallowed),
            skills: Some(&skills),
            hooks: None,
            backend_preference: None,
        });

        assert!(markdown.contains("disallowedTools:"));
        assert!(markdown.contains("  - Bash"));
        assert!(markdown.contains("  - WebFetch"));
        assert!(markdown.contains("skills:"));
        assert!(markdown.contains("  - api-conventions"));
        assert!(markdown.contains("  - error-handling"));
        assert!(markdown.contains("approvalPolicy: acceptAll"));
    }

    #[test]
    fn test_permission_mode_parsing() {
        let content = r#"---
name: Editor
description: Edits code
tools: Read
permissionMode: acceptEdits
---

Prompt
"#;

        let result = parse_agent_markdown(content);
        assert!(result.is_ok());

        let agent = result.unwrap();
        assert_eq!(agent.approval_policy, Some(ApprovalPolicy::AcceptAll));
        assert_eq!(agent.filesystem_scope, Some(FilesystemScope::CwdOnly));
    }

    #[test]
    fn test_disallowed_tools_parsing() {
        let content = r#"---
name: Planner
description: Creates plans
tools: Read, Task
disallowedTools:
  - Bash
  - WebFetch
---

Plan stuff.
"#;

        let result = parse_agent_markdown(content);
        assert!(result.is_ok());

        let agent = result.unwrap();
        assert_eq!(
            agent.disallowed_tools,
            Some(vec!["Bash".to_string(), "WebFetch".to_string()])
        );
    }

    #[test]
    fn test_skills_parsing() {
        let content = r#"---
name: Builder
description: Builds things
tools: Read, Write
skills:
  - api-conventions
  - error-handling
---

Build stuff.
"#;

        let result = parse_agent_markdown(content);
        assert!(result.is_ok());

        let agent = result.unwrap();
        assert_eq!(
            agent.skills,
            Some(vec![
                "api-conventions".to_string(),
                "error-handling".to_string()
            ])
        );
    }

    #[test]
    fn test_disallowed_tools_empty() {
        let content = r#"---
name: Explore
description: Reads code
tools: Read
---

Read stuff.
"#;

        let result = parse_agent_markdown(content);
        assert!(result.is_ok());

        let agent = result.unwrap();
        assert_eq!(agent.disallowed_tools, None);
        assert_eq!(agent.skills, None);
    }

    #[test]
    fn test_hooks_roundtrip() {
        let content = r#"---
name: Builder
description: Builds things
tools: Read, Write
hooks:
  PreToolUse:
    - matcher: "Bash"
      hooks:
        - type: command
          command: "./scripts/validate.sh"
---

Build stuff.
"#;

        let result = parse_agent_markdown(content);
        assert!(result.is_ok());

        let agent = result.unwrap();
        assert!(agent.hooks.is_some());

        // Export and verify hooks are preserved
        let markdown = agent_to_markdown(AgentExportData {
            id: "builder",
            name: &agent.name,
            description: &agent.description,
            tools: &agent.tools,
            tier: None,
            prompt: &agent.prompt,
            approval_policy: None,
            filesystem_scope: None,
            disallowed_tools: None,
            skills: None,
            hooks: agent.hooks.as_ref(),
            backend_preference: None,
        });

        assert!(markdown.contains("hooks:"));
        assert!(markdown.contains("PreToolUse"));
    }

    #[test]
    fn test_backend_preference_roundtrip() {
        let content = r#"---
name: Codex Builder
description: Builds with Codex
tools: Read, Write
backend: codex
---

Build with Codex.
"#;

        let agent = parse_agent_markdown(content).unwrap();
        assert_eq!(agent.backend_preference.as_deref(), Some("codex"));

        // Export and verify backend preference is preserved
        let markdown = agent_to_markdown(AgentExportData {
            id: "codex-builder",
            name: &agent.name,
            description: &agent.description,
            tools: &agent.tools,
            tier: None,
            prompt: &agent.prompt,
            approval_policy: None,
            filesystem_scope: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            backend_preference: agent.backend_preference.as_deref(),
        });

        assert!(markdown.contains("backend: codex"));
    }

    #[test]
    fn test_backend_preference_omitted_when_absent() {
        let markdown = agent_to_markdown(AgentExportData {
            id: "plain",
            name: "Plain",
            description: "No backend preference",
            tools: &["Read".to_string()],
            tier: Some("sonnet"),
            prompt: "Do stuff.",
            approval_policy: None,
            filesystem_scope: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            backend_preference: None,
        });

        assert!(!markdown.contains("backendPreference"));
        assert!(!markdown.contains("backend:"));
    }
}
