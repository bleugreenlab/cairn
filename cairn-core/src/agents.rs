//! Agent configuration import/export.
//!
//! This module handles parsing agent markdown files for import/export.
//! Supports Claude Code-compatible format.

use crate::models::{Fence, LegacyOnEscape, LegacySandbox};
use serde::{Deserialize, Serialize};

/// Agent frontmatter from markdown files (Claude Code compatible)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentFrontmatter {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    name: String,
    description: String,
    /// Comma-separated list of tools (Claude Code format) or YAML array.
    ///
    /// Optional. A missing or empty `tools` field means "the default surface":
    /// the three core verbs (`read`/`write`/`run`) are always ensured at runtime
    /// via `ensure_core_verbs`, so an empty list is a valid, runnable agent. The
    /// UI agent form has no tools editor and legitimately writes empty tools, so
    /// the loader must accept what the form writes rather than reject it.
    #[serde(default, deserialize_with = "deserialize_tools")]
    tools: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "model")]
    pub(crate) tier: Option<String>,
    /// Worktree fence behavior for sandbox escapes.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    fence: Option<Fence>,
    /// Legacy permission fields accepted on read and collapsed into `fence`.
    #[serde(default)]
    #[serde(skip_serializing)]
    sandbox: Option<LegacySandbox>,
    #[serde(default)]
    #[serde(skip_serializing)]
    on_escape: Option<LegacyOnEscape>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hooks: Option<serde_json::Value>,
    /// Tools to disallow (added to blocked list)
    #[serde(skip_serializing_if = "Option::is_none")]
    disallowed_tools: Option<Vec<String>>,
    /// Skills to inject into the prompt
    #[serde(skip_serializing_if = "Option::is_none")]
    skills: Option<Vec<String>>,
    /// Preferred backend when multiple providers are available.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "backend", alias = "backendPreference")]
    backend_preference: Option<String>,
    /// Optional lucide icon name (kebab-case, e.g. `hammer`) giving the agent a
    /// compact visual identity. Absent means no icon (a `Bot` fallback renders).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    icon: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    bundles: Vec<String>,
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

        // YAML null (`tools:` with no value) and absent fields resolve to the
        // empty list — a valid "default surface" agent, not a parse error.
        fn visit_unit<E>(self) -> Result<String, E>
        where
            E: de::Error,
        {
            Ok(String::new())
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
    pub fence: Option<Fence>,
    pub hooks: Option<serde_json::Value>,
    pub disallowed_tools: Option<Vec<String>>,
    pub skills: Option<Vec<String>>,
    pub backend_preference: Option<String>,
    pub icon: Option<String>,
    pub bundles: Vec<String>,
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
/// onEscape: allow
/// ---
///
/// # Agent prompt content in markdown
/// ```
///
/// Also supports legacy Cairn format with YAML array tools.
pub fn parse_agent_markdown(content: &str) -> Result<ParsedAgent, String> {
    let (frontmatter_str, prompt) = crate::markdown_frontmatter::split_yaml_frontmatter(content)?;

    // Parse YAML frontmatter
    let mut frontmatter: AgentFrontmatter = serde_yaml::from_str(frontmatter_str)
        .map_err(|e| format!("Failed to parse frontmatter: {}", e))?;
    crate::config::contextual_packages::normalize_bundles(&mut frontmatter.bundles)?;

    // Validate required fields
    if frontmatter.name.is_empty() {
        return Err("Agent name cannot be empty".to_string());
    }
    if frontmatter.description.is_empty() {
        return Err("Agent description cannot be empty".to_string());
    }

    // Parse tools from comma-separated string. An empty list is valid: it means
    // "the default surface" (the three core verbs are ensured at runtime). The UI
    // agent form has no tools editor and writes empty tools, so rejecting empty
    // here is exactly the save/load asymmetry that makes new agents invisible.
    let tools: Vec<String> = frontmatter
        .tools
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Generate ID if not provided
    let (id, id_generated) = if let Some(id) = frontmatter.id {
        if id.is_empty() {
            return Err("Agent id cannot be empty if provided".to_string());
        }
        (id, false)
    } else {
        (slugify(&frontmatter.name), true)
    };

    let fence = frontmatter.fence.or_else(|| {
        Some(Fence::from_legacy(
            frontmatter.sandbox,
            frontmatter.on_escape,
        ))
    });

    Ok(ParsedAgent {
        id,
        id_generated,
        name: frontmatter.name,
        description: frontmatter.description,
        prompt,
        tools,
        tier: frontmatter.tier,
        fence,
        hooks: frontmatter.hooks,
        disallowed_tools: frontmatter.disallowed_tools,
        skills: frontmatter.skills,
        backend_preference: frontmatter.backend_preference,
        icon: frontmatter.icon,
        bundles: frontmatter.bundles,
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
    pub fence: Option<Fence>,
    pub disallowed_tools: Option<&'a [String]>,
    pub skills: Option<&'a [String]>,
    pub hooks: Option<&'a serde_json::Value>,
    pub backend_preference: Option<&'a str>,
    /// Optional lucide icon name; an `icon:` line is emitted only when set.
    pub icon: Option<&'a str>,
    pub bundles: &'a [String],
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
        fence,
        disallowed_tools,
        skills,
        hooks,
        backend_preference,
        icon,
        bundles,
    } = data;

    let mut frontmatter = format!("---\nname: {}\ndescription: {}\n", name, description);

    // Only emit a `tools:` line when there are tools. An empty value serializes
    // to YAML null, which the loader would otherwise have to special-case;
    // omitting the line keeps the file clean and round-trips through the
    // optional, defaulted `tools` field.
    if !tools.is_empty() {
        frontmatter.push_str(&format!("tools: {}\n", tools.join(", ")));
    }

    if !bundles.is_empty() {
        frontmatter.push_str(&format!("bundles: [{}]\n", bundles.join(", ")));
    }

    if let Some(t) = tier {
        frontmatter.push_str(&format!("tier: {}\n", t));
    }

    if let Some(b) = backend_preference {
        frontmatter.push_str(&format!("backend: {}\n", b));
    }

    // Only emit an `icon:` line when set, so agents without an icon round-trip
    // through the optional, defaulted `icon` frontmatter field unchanged.
    if let Some(icon) = icon {
        frontmatter.push_str(&format!("icon: {}\n", icon));
    }

    // Write fence only when non-default (default is Ask)
    if let Some(f) = fence {
        if f != Fence::Ask {
            let val = serde_json::to_string(&f).unwrap_or_default();
            frontmatter.push_str(&format!("fence: {}\n", val.trim_matches('"')));
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
    fn empty_tools_roundtrips_through_writer_and_loader() {
        // Mirrors exactly what the Settings agent form writes: no tools (the form
        // has no tools editor and passes `tools: []`) plus a custom tier. This is
        // the save/load asymmetry from CAIRN-1656 — the written file must reload.
        let markdown = agent_to_markdown(AgentExportData {
            id: "my-agent",
            name: "My Agent",
            description: "desc",
            tools: &[],
            tier: Some("xl"),
            prompt: "prompt",
            fence: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            backend_preference: None,
            icon: None,
            bundles: &[],
        });
        assert!(
            !markdown.contains("tools:"),
            "empty tools should omit the line, got:\n{markdown}"
        );
        let parsed = parse_agent_markdown(&markdown).expect("empty-tools agent must reload");
        assert_eq!(parsed.name, "My Agent");
        assert!(parsed.tools.is_empty());
        assert_eq!(parsed.tier.as_deref(), Some("xl"));
    }

    #[test]
    fn explicit_null_tools_field_is_accepted() {
        // Pre-fix files written by the old writer carry a bare `tools:` line,
        // which YAML parses as null. The loader must recover them, not reject.
        let content =
            "---\nname: Legacy\ndescription: legacy agent\ntools: \ntier: xl\n---\n\nprompt";
        let parsed = parse_agent_markdown(content).expect("null tools must parse");
        assert_eq!(parsed.name, "Legacy");
        assert!(parsed.tools.is_empty());
    }

    #[test]
    fn missing_tools_field_is_accepted() {
        let content = "---\nname: NoTools\ndescription: no tools field\n---\n\nprompt";
        let parsed = parse_agent_markdown(content).expect("missing tools must parse");
        assert!(parsed.tools.is_empty());
    }

    #[test]
    fn parses_crlf_frontmatter() {
        let content = "---\r\nname: Windows Agent\r\ndescription: A bundled agent with CRLF line endings\r\ntools: Read, Grep\r\n---\r\n\r\nPrompt";
        let parsed = parse_agent_markdown(content).expect("CRLF frontmatter must parse");
        assert_eq!(parsed.name, "Windows Agent");
        assert_eq!(parsed.tools, vec!["Read", "Grep"]);
        assert_eq!(parsed.prompt, "Prompt");
    }

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
            fence: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            backend_preference: None,
            icon: None,
            bundles: &[],
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
            fence: Some(Fence::Allow),
            disallowed_tools: Some(&disallowed),
            skills: Some(&skills),
            hooks: None,
            backend_preference: None,
            icon: None,
            bundles: &[],
        });

        assert!(markdown.contains("disallowedTools:"));
        assert!(markdown.contains("  - Bash"));
        assert!(markdown.contains("  - WebFetch"));
        assert!(markdown.contains("skills:"));
        assert!(markdown.contains("  - api-conventions"));
        assert!(markdown.contains("  - error-handling"));
        assert!(markdown.contains("fence: allow"));
    }

    #[test]
    fn test_legacy_sandbox_on_escape_parsing() {
        let content = r#"---
name: Editor
description: Edits code
tools: Read
sandbox: full
onEscape: allow
---

Prompt
"#;

        let result = parse_agent_markdown(content);
        assert!(result.is_ok());

        let agent = result.unwrap();
        assert_eq!(agent.fence, Some(Fence::Allow));
    }

    #[test]
    fn test_legacy_permission_keys_are_ignored() {
        // Legacy permission frontmatter is no longer honored (migration removed).
        let content = r#"---
name: Old
description: Old agent
tools: Read
permissionMode: bypassPermissions
approvalPolicy: acceptAll
filesystemScope: fullAccess
---

Prompt
"#;

        let agent = parse_agent_markdown(content).unwrap();
        assert_eq!(agent.fence, Some(Fence::Ask));
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
            fence: None,
            disallowed_tools: None,
            skills: None,
            hooks: agent.hooks.as_ref(),
            backend_preference: None,
            icon: None,
            bundles: &[],
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
            fence: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            backend_preference: agent.backend_preference.as_deref(),
            icon: None,
            bundles: &[],
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
            fence: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            backend_preference: None,
            icon: None,
            bundles: &[],
        });

        assert!(!markdown.contains("backendPreference"));
        assert!(!markdown.contains("backend:"));
    }

    #[test]
    fn test_icon_roundtrip() {
        let content = r#"---
name: Builder
description: Builds things
tools: Read, Write
icon: hammer
---

Build stuff.
"#;

        let agent = parse_agent_markdown(content).unwrap();
        assert_eq!(agent.icon.as_deref(), Some("hammer"));

        // Export and verify the icon line is preserved, then re-parse.
        let markdown = agent_to_markdown(AgentExportData {
            id: "builder",
            name: &agent.name,
            description: &agent.description,
            tools: &agent.tools,
            tier: None,
            prompt: &agent.prompt,
            fence: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            backend_preference: None,
            icon: agent.icon.as_deref(),
            bundles: &agent.bundles,
        });

        assert!(markdown.contains("icon: hammer"));
        let reparsed = parse_agent_markdown(&markdown).unwrap();
        assert_eq!(reparsed.icon.as_deref(), Some("hammer"));
    }

    #[test]
    fn test_icon_omitted_when_absent() {
        // An agent with no icon must not emit an `icon:` line and must reload
        // with `icon == None` — no save/load asymmetry for the optional field.
        let markdown = agent_to_markdown(AgentExportData {
            id: "plain",
            name: "Plain",
            description: "No icon",
            tools: &["Read".to_string()],
            tier: None,
            prompt: "Do stuff.",
            fence: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            backend_preference: None,
            icon: None,
            bundles: &[],
        });

        assert!(!markdown.contains("icon:"));
        let parsed = parse_agent_markdown(&markdown).unwrap();
        assert!(parsed.icon.is_none());
    }
}
