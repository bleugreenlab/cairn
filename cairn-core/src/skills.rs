//! Skill configuration import/export.
//!
//! This module handles parsing skill markdown files for import/export.
//! Supports Claude Code-compatible SKILL.md format with YAML frontmatter.

use serde::{Deserialize, Serialize};

/// Skill frontmatter from markdown files (Claude Code compatible)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillFrontmatter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    pub description: String,
    /// Comma-separated list of tools (Claude Code format) or YAML array
    #[serde(
        default,
        deserialize_with = "deserialize_tools",
        skip_serializing_if = "Option::is_none"
    )]
    pub allowed_tools: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// Custom deserializer that handles both comma-separated string and YAML array
fn deserialize_tools<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct ToolsVisitor;

    impl<'de> de::Visitor<'de> for ToolsVisitor {
        type Value = Option<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a comma-separated string, array of strings, or null")
        }

        fn visit_none<E>(self) -> Result<Option<String>, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Option<String>, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_str<E>(self, value: &str) -> Result<Option<String>, E>
        where
            E: de::Error,
        {
            if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(value.to_string()))
            }
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Option<String>, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut tools = Vec::new();
            while let Some(tool) = seq.next_element::<String>()? {
                tools.push(tool);
            }
            if tools.is_empty() {
                Ok(None)
            } else {
                Ok(Some(tools.join(", ")))
            }
        }
    }

    deserializer.deserialize_any(ToolsVisitor)
}

/// Parsed skill with inference metadata
#[derive(Debug, Clone)]
pub struct ParsedSkill {
    pub id: String,
    pub id_generated: bool,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub allowed_tools: Option<Vec<String>>,
    pub model: Option<String>,
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

/// Parse skill markdown file with YAML frontmatter
///
/// Supports Claude Code-compatible format:
/// ```markdown
/// ---
/// name: Skill Name
/// description: Description
/// allowed-tools: Read, Grep, Glob
/// model: sonnet
/// ---
///
/// # Skill prompt content in markdown
/// ```
pub fn parse_skill_markdown(content: &str) -> Result<ParsedSkill, String> {
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
    let frontmatter: SkillFrontmatter = serde_yaml::from_str(frontmatter_str)
        .map_err(|e| format!("Failed to parse frontmatter: {}", e))?;

    // Validate required fields
    if frontmatter.name.is_empty() {
        return Err("Skill name cannot be empty".to_string());
    }
    if frontmatter.description.is_empty() {
        return Err("Skill description cannot be empty".to_string());
    }

    // Parse tools from comma-separated string if present
    let allowed_tools = frontmatter.allowed_tools.map(|tools_str| {
        tools_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });

    // Generate ID if not provided
    let (id, id_generated) = if let Some(id) = frontmatter.id {
        if id.is_empty() {
            return Err("Skill id cannot be empty if provided".to_string());
        }
        (id, false)
    } else {
        (slugify(&frontmatter.name), true)
    };

    Ok(ParsedSkill {
        id,
        id_generated,
        name: frontmatter.name,
        description: frontmatter.description,
        prompt,
        allowed_tools,
        model: frontmatter.model,
    })
}

/// Parameters for converting a skill to markdown
pub struct SkillExportData<'a> {
    pub name: &'a str,
    pub description: &'a str,
    pub allowed_tools: Option<&'a [String]>,
    pub model: Option<&'a str>,
    pub prompt: &'a str,
}

/// Convert skill to Claude Code-compatible markdown format for export
pub fn skill_to_markdown(data: SkillExportData) -> String {
    let SkillExportData {
        name,
        description,
        allowed_tools,
        model,
        prompt,
    } = data;

    let mut frontmatter = format!("---\nname: {}\ndescription: {}\n", name, description);

    if let Some(tools) = allowed_tools {
        if !tools.is_empty() {
            frontmatter.push_str(&format!("allowedTools: {}\n", tools.join(", ")));
        }
    }

    if let Some(m) = model {
        frontmatter.push_str(&format!("model: {}\n", m));
    }

    frontmatter.push_str("---\n\n");
    frontmatter.push_str(prompt);

    frontmatter
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_skill_markdown() {
        let content = r#"---
name: Code Review
description: Review code for quality and best practices
allowedTools: Read, Grep, Glob
model: sonnet
---

# Code Review

Review the code thoroughly, checking for:
- Bug risks
- Performance issues
- Security vulnerabilities
"#;

        let result = parse_skill_markdown(content);
        assert!(result.is_ok());

        let skill = result.unwrap();
        assert_eq!(skill.name, "Code Review");
        assert_eq!(skill.id, "code-review");
        assert!(skill.id_generated);
        assert_eq!(
            skill.allowed_tools,
            Some(vec![
                "Read".to_string(),
                "Grep".to_string(),
                "Glob".to_string()
            ])
        );
        assert_eq!(skill.model, Some("sonnet".to_string()));
        assert!(skill.prompt.contains("# Code Review"));
    }

    #[test]
    fn test_parse_skill_no_tools() {
        let content = r#"---
name: Simple Skill
description: A skill without tool restrictions
---

Do something simple.
"#;

        let result = parse_skill_markdown(content);
        assert!(result.is_ok());

        let skill = result.unwrap();
        assert_eq!(skill.name, "Simple Skill");
        assert!(skill.allowed_tools.is_none());
    }

    #[test]
    fn test_parse_skill_with_array_tools() {
        let content = r#"---
name: Array Tools
description: Skill with YAML array tools
allowedTools:
  - Read
  - Glob
---

Prompt here.
"#;

        let result = parse_skill_markdown(content);
        assert!(result.is_ok());

        let skill = result.unwrap();
        assert_eq!(
            skill.allowed_tools,
            Some(vec!["Read".to_string(), "Glob".to_string()])
        );
    }

    #[test]
    fn test_slugify() {
        assert_eq!(slugify("Code Review"), "code-review");
        assert_eq!(slugify("Security-Check"), "security-check");
        assert_eq!(slugify("DB Reader!"), "db-reader");
        assert_eq!(slugify("Multi   Space"), "multi-space");
    }

    #[test]
    fn test_parse_missing_frontmatter() {
        let content = "# Just a prompt\n\nNo frontmatter here";
        let result = parse_skill_markdown(content);
        assert!(result.is_err());
    }

    #[test]
    fn test_skill_to_markdown() {
        let tools = vec!["Read".to_string(), "Grep".to_string()];
        let markdown = skill_to_markdown(SkillExportData {
            name: "Test Skill",
            description: "A test skill",
            allowed_tools: Some(&tools),
            model: Some("sonnet"),
            prompt: "# Test Prompt\n\nThis is the prompt.",
        });

        assert!(markdown.contains("name: Test Skill"));
        assert!(markdown.contains("allowedTools: Read, Grep"));
        assert!(markdown.contains("model: sonnet"));
        assert!(markdown.contains("# Test Prompt"));
    }

    #[test]
    fn test_skill_to_markdown_no_optional_fields() {
        let markdown = skill_to_markdown(SkillExportData {
            name: "Minimal Skill",
            description: "Just the basics",
            allowed_tools: None,
            model: None,
            prompt: "Do the thing.",
        });

        assert!(markdown.contains("name: Minimal Skill"));
        assert!(!markdown.contains("allowedTools"));
        assert!(!markdown.contains("model"));
    }
}
