//! Skill configuration import/export.
//!
//! This module handles parsing skill markdown files for import/export.
//! Supports both the Agent Skills spec (agentskills.io) format and legacy Cairn format.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Skill frontmatter from markdown files.
///
/// Accepts both spec-compliant format (kebab-case, `allowed-tools`, `metadata` map)
/// and legacy Cairn format (camelCase `allowedTools`, top-level `model`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SkillFrontmatter {
    /// Spec: lowercase slug matching directory name. Legacy: display name.
    pub name: String,
    pub description: String,
    /// Spec: space-delimited string. Legacy: comma-separated string or YAML array.
    #[serde(
        default,
        alias = "allowedTools",
        deserialize_with = "deserialize_allowed_tools",
        skip_serializing_if = "Option::is_none"
    )]
    pub allowed_tools: Option<AllowedToolsValue>,
    /// Spec optional fields
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<String>,
    /// Spec: metadata map for extensions (display-name, etc.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    // Legacy fields (read but not written in spec mode):
    /// Legacy: explicit id field
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Legacy: top-level model field (tolerated on read, omitted on rewrite)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// Wrapper for allowed tools that handles multiple input formats.
/// Normalizes to a `Vec<String>` internally.
/// Serializes as a space-delimited string (spec format).
#[derive(Debug, Clone)]
pub struct AllowedToolsValue(pub Vec<String>);

impl Serialize for AllowedToolsValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize as space-delimited string (spec format)
        serializer.serialize_str(&self.0.join(" "))
    }
}

impl AllowedToolsValue {
    pub fn tools(&self) -> &[String] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Custom deserializer that handles three formats:
/// - Spec: space-delimited string ("Read Grep Glob")
/// - Legacy: comma-separated string ("Read, Grep, Glob")
/// - Legacy: YAML array (["Read", "Grep"])
fn deserialize_allowed_tools<'de, D>(deserializer: D) -> Result<Option<AllowedToolsValue>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct ToolsVisitor;

    impl<'de> de::Visitor<'de> for ToolsVisitor {
        type Value = Option<AllowedToolsValue>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str(
                "a space-delimited string, comma-separated string, array of strings, or null",
            )
        }

        fn visit_none<E>(self) -> Result<Option<AllowedToolsValue>, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Option<AllowedToolsValue>, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_str<E>(self, value: &str) -> Result<Option<AllowedToolsValue>, E>
        where
            E: de::Error,
        {
            if value.is_empty() {
                return Ok(None);
            }
            // Detect format: if contains comma, parse as comma-separated (legacy)
            // Otherwise parse as space-delimited (spec)
            let tools: Vec<String> = if value.contains(',') {
                value
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            } else {
                value.split_whitespace().map(|s| s.to_string()).collect()
            };
            if tools.is_empty() {
                Ok(None)
            } else {
                Ok(Some(AllowedToolsValue(tools)))
            }
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Option<AllowedToolsValue>, A::Error>
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
                Ok(Some(AllowedToolsValue(tools)))
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

/// Validate a skill name against the Agent Skills spec rules.
///
/// Rules: 1-64 chars, lowercase `[a-z0-9-]` only, no leading/trailing/consecutive hyphens.
pub fn validate_skill_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Skill name cannot be empty".to_string());
    }
    if name.len() > 64 {
        return Err("Skill name must be 64 characters or fewer".to_string());
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err("Skill name cannot start or end with a hyphen".to_string());
    }
    if name.contains("--") {
        return Err("Skill name cannot contain consecutive hyphens".to_string());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(
            "Skill name must contain only lowercase letters, digits, and hyphens".to_string(),
        );
    }
    Ok(())
}

/// Parse skill markdown file with YAML frontmatter.
///
/// Supports both spec-compliant format and legacy Cairn format:
/// ```markdown
/// ---
/// name: testing
/// description: Test structure and patterns
/// allowed-tools: Read Grep Glob
/// metadata:
///   model: sonnet
///   display-name: Testing
/// ---
///
/// # Skill prompt content
/// ```
pub fn parse_skill_markdown(content: &str) -> Result<ParsedSkill, String> {
    let (frontmatter_str, prompt) = crate::markdown_frontmatter::split_yaml_frontmatter(content)?;

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

    // Extract allowed tools
    let allowed_tools = frontmatter
        .allowed_tools
        .map(|atv| atv.0)
        .filter(|v| !v.is_empty());

    // Generate ID if not provided
    let (id, id_generated) = if let Some(id) = frontmatter.id {
        if id.is_empty() {
            return Err("Skill id cannot be empty if provided".to_string());
        }
        (id, false)
    } else {
        (slugify(&frontmatter.name), true)
    };

    // Resolve display name: metadata.display-name > frontmatter.name (if it looks like a display name)
    let display_name = frontmatter
        .metadata
        .as_ref()
        .and_then(|m| m.get("display-name").cloned())
        .unwrap_or(frontmatter.name);

    Ok(ParsedSkill {
        id,
        id_generated,
        name: display_name,
        description: frontmatter.description,
        prompt,
        allowed_tools,
    })
}

/// Parameters for converting a skill to markdown
pub struct SkillExportData<'a> {
    pub name: &'a str,
    pub description: &'a str,
    pub allowed_tools: Option<&'a [String]>,
    pub prompt: &'a str,
}

/// Convert skill to spec-compliant markdown format for export.
///
/// Writes spec frontmatter: `name` is slug, `allowed-tools` space-delimited,
/// `model` goes into `metadata.model`, display name into `metadata.display-name`.
pub fn skill_to_markdown(data: SkillExportData) -> String {
    let SkillExportData {
        name,
        description,
        allowed_tools,
        prompt,
    } = data;

    let mut frontmatter = format!("---\nname: {}\ndescription: {}\n", name, description);

    if let Some(tools) = allowed_tools {
        if !tools.is_empty() {
            frontmatter.push_str(&format!("allowed-tools: {}\n", tools.join(" ")));
        }
    }

    let slug = slugify(name);
    let needs_display_name = name != slug && titlecase(&slug) != name;

    if needs_display_name {
        frontmatter.push_str("metadata:\n");
        if needs_display_name {
            frontmatter.push_str(&format!("  display-name: {}\n", name));
        }
    }

    frontmatter.push_str("---\n\n");
    frontmatter.push_str(prompt);

    frontmatter
}

/// Parameters for spec-compliant export with slug ID separate from display name.
pub struct SkillExportDataSpec<'a> {
    /// Lowercase slug (directory/id name)
    pub slug: &'a str,
    /// Display name
    pub display_name: &'a str,
    pub description: &'a str,
    pub allowed_tools: Option<&'a [String]>,
    pub prompt: &'a str,
}

/// Convert skill to spec-compliant markdown with explicit slug.
pub fn skill_to_markdown_spec(data: SkillExportDataSpec) -> String {
    let SkillExportDataSpec {
        slug,
        display_name,
        description,
        allowed_tools,
        prompt,
    } = data;

    let mut frontmatter = format!("---\nname: {}\ndescription: {}\n", slug, description);

    if let Some(tools) = allowed_tools {
        if !tools.is_empty() {
            frontmatter.push_str(&format!("allowed-tools: {}\n", tools.join(" ")));
        }
    }

    let needs_display_name = display_name != slug && titlecase(slug) != display_name;

    if needs_display_name {
        frontmatter.push_str("metadata:\n");
        if needs_display_name {
            frontmatter.push_str(&format!("  display-name: {}\n", display_name));
        }
    }

    frontmatter.push_str("---\n\n");
    frontmatter.push_str(prompt);

    frontmatter
}

/// Titlecase a slug: "my-skill" → "My Skill"
pub fn titlecase_slug(slug: &str) -> String {
    titlecase(slug)
}

fn titlecase(slug: &str) -> String {
    slug.split('-')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => {
                    let mut s = c.to_uppercase().to_string();
                    s.extend(chars);
                    s
                }
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Replace a markdown section by heading, returning the new prompt.
///
/// Finds the heading line, replaces content until the next same-level heading or EOF.
pub fn replace_section_in_prompt(
    prompt: &str,
    heading: &str,
    new_content: &str,
) -> Result<String, String> {
    let heading_trimmed = heading.trim();

    // Determine heading level
    let level = heading_trimmed.chars().take_while(|&c| c == '#').count();
    if level == 0 {
        return Err("Heading must start with '#'".to_string());
    }
    let heading_prefix = "#".repeat(level);

    let lines: Vec<&str> = prompt.lines().collect();

    // Find the heading line
    let start_idx = lines
        .iter()
        .position(|line| {
            let trimmed = line.trim();
            trimmed == heading_trimmed
                || (trimmed.starts_with(&heading_prefix)
                    && !trimmed.starts_with(&format!("{}#", heading_prefix))
                    && trimmed[level..].trim() == heading_trimmed[level..].trim())
        })
        .ok_or_else(|| format!("Heading not found: {}", heading_trimmed))?;

    // Find end: next same-level-or-higher heading, or EOF
    let end_idx = lines
        .iter()
        .enumerate()
        .skip(start_idx + 1)
        .find(|(_, line)| {
            let trimmed = line.trim();
            if !trimmed.starts_with('#') {
                return false;
            }
            let line_level = trimmed.chars().take_while(|&c| c == '#').count();
            line_level <= level
        })
        .map(|(i, _)| i)
        .unwrap_or(lines.len());

    // Build result
    let mut result = Vec::new();
    result.extend_from_slice(&lines[..=start_idx]);
    result.push(""); // blank line after heading
    for line in new_content.lines() {
        result.push(line);
    }
    if end_idx < lines.len() {
        result.push(""); // blank line before next heading
        result.extend_from_slice(&lines[end_idx..]);
    }

    Ok(result.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_skill_markdown_legacy() {
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
        assert!(skill.prompt.contains("# Code Review"));
    }

    #[test]
    fn test_parse_spec_format() {
        let content = r#"---
name: testing
description: Test structure and patterns
allowed-tools: Read Grep Glob
metadata:
  model: sonnet
  display-name: Testing
---

# Testing

Test content here.
"#;

        let result = parse_skill_markdown(content).unwrap();
        assert_eq!(result.name, "Testing"); // from metadata.display-name
        assert_eq!(result.id, "testing"); // from slugified name
        assert_eq!(
            result.allowed_tools,
            Some(vec![
                "Read".to_string(),
                "Grep".to_string(),
                "Glob".to_string()
            ])
        );
    }

    #[test]
    fn test_parse_legacy_array_format() {
        let content = r#"---
name: Array Tools
description: Skill with YAML array tools
allowedTools:
  - Read
  - Glob
---

Prompt here.
"#;

        let result = parse_skill_markdown(content).unwrap();
        assert_eq!(
            result.allowed_tools,
            Some(vec!["Read".to_string(), "Glob".to_string()])
        );
    }

    #[test]
    fn parses_crlf_frontmatter() {
        let content = "---\r\nname: windows-skill\r\ndescription: A bundled skill with CRLF line endings\r\n---\r\n\r\nPrompt";
        let parsed = parse_skill_markdown(content).expect("CRLF frontmatter must parse");
        assert_eq!(parsed.id, "windows-skill");
        assert_eq!(parsed.name, "windows-skill");
        assert_eq!(parsed.prompt, "Prompt");
    }

    #[test]
    fn test_parse_metadata_display_name() {
        let content = r#"---
name: test
description: test
metadata:
  display-name: Test Skill
---

Prompt.
"#;
        let result = parse_skill_markdown(content).unwrap();
        assert_eq!(result.name, "Test Skill".to_string());
    }

    #[test]
    fn test_parse_legacy_model_is_tolerated() {
        let content = r#"---
name: test
description: test
model: sonnet
---

Prompt.
"#;
        let result = parse_skill_markdown(content).unwrap();
        assert_eq!(result.id, "test");
    }

    #[test]
    fn test_parse_metadata_display_name_overrides_name() {
        let content = r#"---
name: test
description: test
model: sonnet
metadata:
  display-name: Test Display
---

Prompt.
"#;
        let result = parse_skill_markdown(content).unwrap();
        assert_eq!(result.name, "Test Display".to_string());
    }

    #[test]
    fn test_parse_skill_no_tools() {
        let content = r#"---
name: Simple Skill
description: A skill without tool restrictions
---

Do something simple.
"#;

        let result = parse_skill_markdown(content).unwrap();
        assert_eq!(result.name, "Simple Skill");
        assert!(result.allowed_tools.is_none());
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
    fn test_export_spec_compliant() {
        let tools = vec!["Read".to_string(), "Grep".to_string()];
        let markdown = skill_to_markdown_spec(SkillExportDataSpec {
            slug: "testing",
            display_name: "Testing",
            description: "A test skill",
            allowed_tools: Some(&tools),
            prompt: "# Test Prompt\n\nThis is the prompt.",
        });

        assert!(markdown.contains("name: testing"));
        assert!(markdown.contains("allowed-tools: Read Grep"));
        // "Testing" is titlecase of "testing", so no display-name needed
        assert!(!markdown.contains("display-name"));
        assert!(markdown.contains("# Test Prompt"));
    }

    #[test]
    fn test_export_with_display_name() {
        let markdown = skill_to_markdown_spec(SkillExportDataSpec {
            slug: "code-review",
            display_name: "Code Review Pro",
            description: "Reviews code",
            allowed_tools: None,
            prompt: "Prompt.",
        });

        // "Code Review Pro" != titlecase("code-review") = "Code Review"
        assert!(markdown.contains("display-name: Code Review Pro"));
    }

    #[test]
    fn test_skill_to_markdown_compat() {
        // Legacy export function still works
        let tools = vec!["Read".to_string(), "Grep".to_string()];
        let markdown = skill_to_markdown(SkillExportData {
            name: "Test Skill",
            description: "A test skill",
            allowed_tools: Some(&tools),
            prompt: "# Test Prompt\n\nThis is the prompt.",
        });

        assert!(markdown.contains("name: Test Skill"));
        assert!(markdown.contains("allowed-tools: Read Grep"));
        assert!(markdown.contains("# Test Prompt"));
    }

    #[test]
    fn test_skill_to_markdown_no_optional_fields() {
        let markdown = skill_to_markdown(SkillExportData {
            name: "Minimal Skill",
            description: "Just the basics",
            allowed_tools: None,
            prompt: "Do the thing.",
        });

        assert!(markdown.contains("name: Minimal Skill"));
        assert!(!markdown.contains("allowed-tools"));
        assert!(!markdown.contains("metadata"));
    }

    #[test]
    fn test_validate_skill_name_valid() {
        assert!(validate_skill_name("testing").is_ok());
        assert!(validate_skill_name("code-review").is_ok());
        assert!(validate_skill_name("my-skill-123").is_ok());
        assert!(validate_skill_name("a").is_ok());
    }

    #[test]
    fn test_validate_skill_name_invalid() {
        assert!(validate_skill_name("").is_err());
        assert!(validate_skill_name("Code Review").is_err()); // uppercase
        assert!(validate_skill_name("-leading").is_err());
        assert!(validate_skill_name("trailing-").is_err());
        assert!(validate_skill_name("double--hyphen").is_err());
        assert!(validate_skill_name("under_score").is_err());
        let long_name = "a".repeat(65);
        assert!(validate_skill_name(&long_name).is_err());
    }

    #[test]
    fn test_titlecase() {
        assert_eq!(titlecase("testing"), "Testing");
        assert_eq!(titlecase("code-review"), "Code Review");
        assert_eq!(titlecase("my-cool-skill"), "My Cool Skill");
    }

    #[test]
    fn test_replace_section_basic() {
        let prompt =
            "# Intro\n\nSome intro text.\n\n## Details\n\nOld details.\n\n## Conclusion\n\nEnd.";
        let result = replace_section_in_prompt(prompt, "## Details", "New details here.").unwrap();
        assert!(result.contains("New details here."));
        assert!(!result.contains("Old details."));
        assert!(result.contains("## Conclusion"));
        assert!(result.contains("# Intro"));
    }

    #[test]
    fn test_replace_section_at_end() {
        let prompt = "# Intro\n\nText.\n\n## Last\n\nOld last.";
        let result = replace_section_in_prompt(prompt, "## Last", "New last.").unwrap();
        assert!(result.contains("New last."));
        assert!(!result.contains("Old last."));
    }

    #[test]
    fn test_replace_section_not_found() {
        let prompt = "# Intro\n\nText.";
        let result = replace_section_in_prompt(prompt, "## Missing", "Content");
        assert!(result.is_err());
    }

    #[test]
    fn test_replace_section_preserves_sub_headings() {
        // Replacing a ## heading should stop at the next ## (same level),
        // not consume ### sub-headings of a sibling section.
        let prompt = "## A\n\nA content.\n\n### A1\n\nSub content.\n\n## B\n\n### B1\n\nB sub.";
        let result = replace_section_in_prompt(prompt, "## A", "Replaced A.").unwrap();
        assert!(result.contains("Replaced A."));
        assert!(!result.contains("A content."));
        assert!(!result.contains("Sub content.")); // ### A1 is inside ## A's range
        assert!(result.contains("## B"));
        assert!(result.contains("### B1"));
        assert!(result.contains("B sub."));
    }

    #[test]
    fn test_replace_section_higher_level_heading_stops() {
        // Replacing ## should stop when it hits a # (higher level)
        let prompt = "## Section\n\nContent.\n\n# Top Level\n\nTop content.";
        let result = replace_section_in_prompt(prompt, "## Section", "New content.").unwrap();
        assert!(result.contains("New content."));
        assert!(result.contains("# Top Level"));
        assert!(result.contains("Top content."));
    }

    #[test]
    fn test_allowed_tools_value_serialize_roundtrip() {
        let atv = AllowedToolsValue(vec![
            "Read".to_string(),
            "Grep".to_string(),
            "Glob".to_string(),
        ]);
        let serialized = serde_json::to_string(&atv).unwrap();
        assert_eq!(serialized, r#""Read Grep Glob""#);
    }

    #[test]
    fn test_skill_to_markdown_emits_display_name() {
        // When name doesn't match titlecase(slug), display-name should be emitted
        let tools = vec!["Read".to_string()];
        let markdown = skill_to_markdown(SkillExportData {
            name: "TestNG Framework", // not titlecase of "testng-framework"
            description: "Testing tool",
            allowed_tools: Some(&tools),
            prompt: "Prompt.",
        });

        assert!(markdown.contains("display-name: TestNG Framework"));
    }

    #[test]
    fn test_parse_spec_display_name_fallback_to_titlecase() {
        // When no display-name in metadata, name should still be resolved
        let content = "---\nname: code-review\ndescription: Reviews code\n---\n\nPrompt.";
        let result = parse_skill_markdown(content).unwrap();
        // Without metadata.display-name, name comes from frontmatter.name directly
        assert_eq!(result.name, "code-review");
        assert_eq!(result.id, "code-review");
    }

    #[test]
    fn test_validate_skill_name_boundary_64_chars() {
        // Exactly 64 chars should be valid
        let name = "a".repeat(64);
        assert!(validate_skill_name(&name).is_ok());
    }
}
