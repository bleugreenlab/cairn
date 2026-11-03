//! Agent configuration import/export.
//!
//! This module handles parsing agent markdown files for import/export.
//! Supports Claude Code-compatible format.

use crate::allotment::{parse_allotment, retired_model_key, AllotmentEntry};
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
    /// The pools and `provider/model` pairs this agent selects within.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    models: Option<Vec<String>>,
    /// Retired model keys, read only so a file still naming one is refused
    /// with the instruction rather than run on the workspace default.
    #[serde(default, skip_serializing)]
    tier: Option<serde_yaml::Value>,
    #[serde(default, skip_serializing)]
    model: Option<serde_yaml::Value>,
    #[serde(default, skip_serializing, alias = "backendPreference")]
    backend: Option<serde_yaml::Value>,
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
    /// Optional lucide icon name (kebab-case, e.g. `hammer`) giving the agent a
    /// compact visual identity. Absent means no icon (a `Bot` fallback renders).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    icon: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    bundles: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seed_budget_tokens: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seed_eras: Option<i32>,
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
    /// Empty when the file names no `models`: the workspace default applies.
    pub models: Vec<AllotmentEntry>,
    pub fence: Option<Fence>,
    pub hooks: Option<serde_json::Value>,
    pub disallowed_tools: Option<Vec<String>>,
    pub skills: Option<Vec<String>>,
    pub icon: Option<String>,
    pub bundles: Vec<String>,
    pub seed_budget_tokens: Option<i32>,
    pub seed_eras: Option<i32>,
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
/// models: [md]
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
    crate::contextual_packages::normalize_bundles(&mut frontmatter.bundles)?;

    // Validate required fields
    if frontmatter.name.is_empty() {
        return Err("Agent name cannot be empty".to_string());
    }
    if frontmatter.description.is_empty() {
        return Err("Agent description cannot be empty".to_string());
    }
    for (key, present) in [
        ("tier", frontmatter.tier.is_some()),
        ("model", frontmatter.model.is_some()),
        ("backend", frontmatter.backend.is_some()),
    ] {
        if let Some(instruction) = present.then(|| retired_model_key(key)).flatten() {
            return Err(instruction);
        }
    }
    let models = match &frontmatter.models {
        Some(raw) => parse_allotment(raw)?,
        None => Vec::new(),
    };

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
        models,
        fence,
        hooks: frontmatter.hooks,
        disallowed_tools: frontmatter.disallowed_tools,
        skills: frontmatter.skills,
        icon: frontmatter.icon,
        bundles: frontmatter.bundles,
        seed_budget_tokens: frontmatter.seed_budget_tokens,
        seed_eras: frontmatter.seed_eras,
    })
}

/// Parameters for converting an agent to markdown
pub struct AgentExportData<'a> {
    #[allow(dead_code)]
    pub id: &'a str,
    pub name: &'a str,
    pub description: &'a str,
    pub tools: &'a [String],
    pub models: &'a [AllotmentEntry],
    pub prompt: &'a str,
    pub fence: Option<Fence>,
    pub disallowed_tools: Option<&'a [String]>,
    pub skills: Option<&'a [String]>,
    pub hooks: Option<&'a serde_json::Value>,
    /// Optional lucide icon name; an `icon:` line is emitted only when set.
    pub icon: Option<&'a str>,
    pub bundles: &'a [String],
    pub seed_budget_tokens: Option<i32>,
    pub seed_eras: Option<i32>,
}

/// Convert agent to Claude Code-compatible markdown format for export
pub fn agent_to_markdown(data: AgentExportData) -> String {
    let AgentExportData {
        id: _,
        name,
        description,
        tools,
        models,
        prompt,
        fence,
        disallowed_tools,
        skills,
        hooks,
        icon,
        bundles,
        seed_budget_tokens,
        seed_eras,
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

    if !models.is_empty() {
        let entries: Vec<String> = models.iter().map(ToString::to_string).collect();
        frontmatter.push_str(&format!("models: [{}]\n", entries.join(", ")));
    }
    if let Some(value) = seed_budget_tokens {
        frontmatter.push_str(&format!("seedBudgetTokens: {value}\n"));
    }
    if let Some(value) = seed_eras {
        frontmatter.push_str(&format!("seedEras: {value}\n"));
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

    /// An export with every optional field unset, for tests to override.
    fn export<'a>(name: &'a str, tools: &'a [String], prompt: &'a str) -> AgentExportData<'a> {
        AgentExportData {
            id: "agent",
            name,
            description: "desc",
            tools,
            models: &[],
            prompt,
            fence: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            icon: None,
            bundles: &[],
            seed_budget_tokens: None,
            seed_eras: None,
        }
    }

    #[test]
    fn empty_tools_roundtrips_through_writer_and_loader() {
        // Mirrors exactly what the Settings agent form writes: no tools (the form
        // has no tools editor and passes `tools: []`) plus an allotment. This is
        // the save/load asymmetry from CAIRN-1656 — the written file must reload.
        let models = vec![
            AllotmentEntry::Pool("lg".into()),
            AllotmentEntry::parse("claude/fable").unwrap(),
        ];
        let markdown = agent_to_markdown(AgentExportData {
            models: &models,
            ..export("My Agent", &[], "prompt")
        });
        assert!(
            !markdown.contains("tools:"),
            "empty tools should omit the line, got:\n{markdown}"
        );
        assert!(
            markdown.contains("models: [lg, claude/fable]"),
            "{markdown}"
        );
        let parsed = parse_agent_markdown(&markdown).expect("empty-tools agent must reload");
        assert_eq!(parsed.name, "My Agent");
        assert!(parsed.tools.is_empty());
        assert_eq!(parsed.models, models);
    }

    #[test]
    fn explicit_null_tools_field_is_accepted() {
        // Pre-fix files written by the old writer carry a bare `tools:` line,
        // which YAML parses as null. The loader must recover them, not reject.
        let content =
            "---\nname: Legacy\ndescription: legacy agent\ntools: \nmodels: [md]\n---\n\nprompt";
        let parsed = parse_agent_markdown(content).expect("null tools must parse");
        assert_eq!(parsed.name, "Legacy");
        assert!(parsed.tools.is_empty());
    }

    #[test]
    fn missing_tools_field_is_accepted() {
        let content = "---\nname: NoTools\ndescription: no tools field\n---\n\nprompt";
        let parsed = parse_agent_markdown(content).expect("missing tools must parse");
        assert!(parsed.tools.is_empty());
        assert!(parsed.models.is_empty(), "no models means the default");
    }

    #[test]
    fn parses_crlf_frontmatter() {
        let content = "---\r\nname: Windows Agent\r\ndescription: A bundled agent with CRLF line endings\r\ntools: Read, Grep\r\n---\r\n\r\nPrompt";
        let parsed = parse_agent_markdown(content).expect("CRLF frontmatter must parse");
        assert_eq!(parsed.name, "Windows Agent");
        assert_eq!(parsed.tools, vec!["Read", "Grep"]);
        assert_eq!(parsed.prompt, "Prompt");
    }

    /// A file still naming a retired model key is refused with the
    /// instruction, never run on the workspace default in silence.
    #[test]
    fn retired_model_keys_are_refused_with_the_instruction() {
        for (line, key) in [
            ("tier: lg", "`tier:`"),
            ("model: sonnet", "`model:`"),
            ("backend: codex", "`backend:`"),
            ("backendPreference: codex", "`backend:`"),
        ] {
            let content = format!("---\nname: Old\ndescription: old agent\n{line}\n---\n\nprompt");
            let error = parse_agent_markdown(&content).unwrap_err();
            assert!(error.contains(key), "{line}: {error}");
            assert!(error.contains("models: ["), "{line}: {error}");
            assert!(
                error.contains("docs/backends.md#model-selection"),
                "{error}"
            );
        }
    }

    #[test]
    fn an_empty_or_malformed_allotment_is_refused() {
        let empty = "---\nname: A\ndescription: d\nmodels: []\n---\n\np";
        assert!(parse_agent_markdown(empty)
            .unwrap_err()
            .contains("`models` is empty"));
        let malformed = "---\nname: A\ndescription: d\nmodels: [claude/]\n---\n\np";
        assert!(parse_agent_markdown(malformed)
            .unwrap_err()
            .contains("not provider/model"));
    }

    #[test]
    fn test_parse_claude_code_format() {
        let content = r#"---
name: Test Agent
description: A test agent
tools: Read, Grep, Glob
models: [sm, codex/gpt-5.6-luna]
permissionMode: plan
---

# Test Prompt

This is the agent's system prompt.
"#;

        let agent = parse_agent_markdown(content).unwrap();
        assert_eq!(agent.name, "Test Agent");
        assert_eq!(agent.id, "test-agent");
        assert!(agent.id_generated);
        assert_eq!(agent.tools.len(), 3);
        assert_eq!(
            agent.models,
            vec![
                AllotmentEntry::Pool("sm".into()),
                AllotmentEntry::parse("codex/gpt-5.6-luna").unwrap()
            ]
        );
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
version: 0.5.0
---

# Test Prompt

This is the agent's system prompt.
"#;

        let agent = parse_agent_markdown(content).unwrap();
        assert_eq!(agent.id, "test-agent");
        assert!(!agent.id_generated);
        assert_eq!(agent.name, "Test Agent");
        assert_eq!(agent.tools.len(), 2);
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

        let agent = parse_agent_markdown(content).unwrap();
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
        assert!(parse_agent_markdown(content).is_err());
    }

    #[test]
    fn test_agent_to_markdown() {
        let tools = vec!["Read".to_string(), "Grep".to_string()];
        let models = vec![AllotmentEntry::Pool("md".into())];
        let markdown = agent_to_markdown(AgentExportData {
            description: "A test agent",
            models: &models,
            ..export("Test Agent", &tools, "# Test Prompt\n\nThis is the prompt.")
        });

        assert!(markdown.contains("name: Test Agent"));
        assert!(markdown.contains("tools: Read, Grep"));
        assert!(markdown.contains("models: [md]"));
        assert!(!markdown.contains("tier:"));
        assert!(!markdown.contains("backend:"));
        assert!(!markdown.contains("permissionMode"));
        assert!(markdown.contains("# Test Prompt"));
        assert!(!markdown.contains("id:")); // ID not exported
    }

    #[test]
    fn no_models_line_without_an_allotment() {
        let markdown = agent_to_markdown(export("Plain", &[], "Do stuff."));
        assert!(!markdown.contains("models:"), "{markdown}");
    }

    #[test]
    fn test_agent_to_markdown_with_disallowed_tools_and_skills() {
        let tools = vec!["Read".to_string(), "Task".to_string()];
        let disallowed = vec!["Bash".to_string(), "WebFetch".to_string()];
        let skills = vec!["api-conventions".to_string(), "error-handling".to_string()];
        let markdown = agent_to_markdown(AgentExportData {
            description: "Creates plans",
            fence: Some(Fence::Allow),
            disallowed_tools: Some(&disallowed),
            skills: Some(&skills),
            ..export("Planner", &tools, "Plan stuff.")
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

        let agent = parse_agent_markdown(content).unwrap();
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

        let agent = parse_agent_markdown(content).unwrap();
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

        let agent = parse_agent_markdown(content).unwrap();
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

        let agent = parse_agent_markdown(content).unwrap();
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

        let agent = parse_agent_markdown(content).unwrap();
        assert!(agent.hooks.is_some());

        // Export and verify hooks are preserved
        let markdown = agent_to_markdown(AgentExportData {
            description: &agent.description,
            hooks: agent.hooks.as_ref(),
            ..export(&agent.name, &agent.tools, &agent.prompt)
        });

        assert!(markdown.contains("hooks:"));
        assert!(markdown.contains("PreToolUse"));
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
            description: &agent.description,
            icon: agent.icon.as_deref(),
            bundles: &agent.bundles,
            ..export(&agent.name, &agent.tools, &agent.prompt)
        });

        assert!(markdown.contains("icon: hammer"));
        let reparsed = parse_agent_markdown(&markdown).unwrap();
        assert_eq!(reparsed.icon.as_deref(), Some("hammer"));
    }

    #[test]
    fn test_icon_omitted_when_absent() {
        // An agent with no icon must not emit an `icon:` line and must reload
        // with `icon == None` — no save/load asymmetry for the optional field.
        let tools = ["Read".to_string()];
        let markdown = agent_to_markdown(export("Plain", &tools, "Do stuff."));

        assert!(!markdown.contains("icon:"));
        let parsed = parse_agent_markdown(&markdown).unwrap();
        assert!(parsed.icon.is_none());
    }
}
