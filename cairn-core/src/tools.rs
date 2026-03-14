//! Custom tool configuration parsing.
//!
//! This module handles parsing `.ts` tool files with named exports.
//! Tools are defined as TypeScript files with `export const name`, `export const description`,
//! optional `export const inputSchema`, and `export default async function`.

use regex::Regex;
use std::collections::HashSet;

/// Parsed tool with code and metadata
#[derive(Debug, Clone)]
pub struct ParsedTool {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
    pub code: String,
    pub required_tools: Vec<String>,
}

/// Parse a `.ts` tool file with named exports.
///
/// Extracts:
/// - `name` and `description` from `export const name = "..."` / `export const description = "..."`
/// - `inputSchema` via brace-matching + json5 parsing
/// - `code` is the entire file content (used for import-based execution)
/// - `required_tools` inferred by scanning for `mcp.\w+(` patterns
pub fn parse_tool_ts(content: &str) -> Result<ParsedTool, String> {
    let name = extract_string_export(content, "name")
        .ok_or_else(|| "Missing `export const name = \"...\"`".to_string())?;
    if name.is_empty() {
        return Err("Tool name cannot be empty".to_string());
    }

    let description = extract_string_export(content, "description")
        .ok_or_else(|| "Missing `export const description = \"...\"`".to_string())?;
    if description.is_empty() {
        return Err("Tool description cannot be empty".to_string());
    }

    let input_schema = extract_input_schema(content)?;
    let required_tools = infer_required_tools(content);

    Ok(ParsedTool {
        name,
        description,
        input_schema,
        code: content.to_string(),
        required_tools,
    })
}

/// Extract a string value from `export const <field> = "value"` or `export const <field> = 'value'`.
fn extract_string_export(content: &str, field: &str) -> Option<String> {
    let pattern = format!(
        r#"export\s+const\s+{}\s*=\s*["'](.*?)["']\s*;?"#,
        regex::escape(field)
    );
    let re = Regex::new(&pattern).ok()?;
    re.captures(content).map(|c| c[1].to_string())
}

/// Extract `inputSchema` via brace-matching + json5 parsing.
///
/// 1. Find `export const inputSchema = {`
/// 2. Brace-match to find the complete object
/// 3. Strip TypeScript syntax (`as const`, `satisfies X`)
/// 4. Parse with json5
/// 5. If not found, return default empty schema
fn extract_input_schema(content: &str) -> Result<serde_json::Value, String> {
    let re = Regex::new(r"export\s+const\s+inputSchema\s*=\s*").unwrap();
    let m = match re.find(content) {
        Some(m) => m,
        None => {
            // No inputSchema export — default empty schema
            return Ok(serde_json::json!({
                "type": "object",
                "properties": {},
                "required": []
            }));
        }
    };

    let after = &content[m.end()..];
    if !after.starts_with('{') {
        return Err("inputSchema must be an object literal".to_string());
    }

    // Brace-match to find the complete object
    let obj_str = brace_match(after)?;

    // Strip trailing TypeScript syntax: `as const`, `satisfies X`
    let clean = strip_ts_suffix(obj_str);

    // Parse with json5 (handles unquoted keys, trailing commas, etc.)
    let value: serde_json::Value =
        json5::from_str(clean).map_err(|e| format!("Failed to parse inputSchema: {}", e))?;

    Ok(value)
}

/// Match braces from the opening `{` to its closing `}`.
/// Returns the matched substring including both braces.
fn brace_match(s: &str) -> Result<&str, String> {
    if !s.starts_with('{') {
        return Err("Expected '{'".to_string());
    }

    let mut depth = 0i32;
    let mut in_string: Option<char> = None;
    let mut prev_char = '\0';

    for (i, ch) in s.char_indices() {
        // Handle string literals (skip brace counting inside strings)
        if let Some(quote) = in_string {
            if ch == quote && prev_char != '\\' {
                in_string = None;
            }
            prev_char = ch;
            continue;
        }

        match ch {
            '"' | '\'' | '`' => {
                in_string = Some(ch);
            }
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(&s[..=i]);
                }
            }
            _ => {}
        }
        prev_char = ch;
    }

    Err("Unmatched braces in inputSchema".to_string())
}

/// Strip trailing TypeScript type annotations after the object literal.
/// Handles `as const`, `satisfies SomeType`, etc.
fn strip_ts_suffix(s: &str) -> &str {
    // The brace_match already gives us just the `{...}` portion,
    // so this is mainly a safety measure.
    s.trim()
}

/// Infer which MCP tools the code requires by scanning for function call patterns.
///
/// Looks for `mcp.\w+(` patterns (the canonical form in .ts tools).
/// Also catches direct calls like `read(`, `bash(` for backwards compat.
fn infer_required_tools(code: &str) -> Vec<String> {
    let mut tools = HashSet::new();

    // MCP object calls: mcp.read(, mcp.write(, etc.
    let mcp_re = Regex::new(r"\bmcp\.\s*(\w+)\s*\(").unwrap();
    for cap in mcp_re.captures_iter(code) {
        tools.insert(cap[1].to_string());
    }

    // Direct function calls (for backwards compat / convenience aliases)
    let direct_re = Regex::new(r"\b(read|write|edit|bash|kill_shell|task|batch_tasks|skill|search|create_issue|update_issue|add_comment|ask_user)\s*\(").unwrap();
    for cap in direct_re.captures_iter(code) {
        tools.insert(cap[1].to_string());
    }

    let mut result: Vec<String> = tools.into_iter().collect();
    result.sort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tool_ts_basic() {
        let content = r#"
export const name = "Check Status";
export const description = "Check the status of issues";

export const inputSchema = {
  type: "object",
  properties: {
    issues: {
      type: "array",
      items: { type: "string" },
      description: "Issue identifiers",
    },
  },
  required: ["issues"],
};

export default async function ({ inputs, mcp }) {
  const results = await Promise.all(
    inputs.issues.map(id => mcp.read({ path: `cairn://${PROJECT_ID}/${id}` }))
  );
  return JSON.stringify(results, null, 2);
}
"#;

        let result = parse_tool_ts(content);
        assert!(result.is_ok(), "Parse failed: {:?}", result.err());

        let tool = result.unwrap();
        assert_eq!(tool.name, "Check Status");
        assert_eq!(tool.description, "Check the status of issues");
        assert!(tool.code.contains("inputs.issues"));
        assert!(tool.required_tools.contains(&"read".to_string()));

        // Check schema
        let schema = &tool.input_schema;
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["issues"].is_object());
        assert!(schema["required"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("issues")));
    }

    #[test]
    fn test_parse_tool_ts_no_inputs() {
        let content = r#"
export const name = "Hello World";
export const description = "Prints hello world";

export default async function () {
  return "Hello, world!";
}
"#;

        let result = parse_tool_ts(content);
        assert!(result.is_ok());

        let tool = result.unwrap();
        assert_eq!(tool.name, "Hello World");
        assert!(tool.required_tools.is_empty());
        assert_eq!(tool.input_schema["properties"], serde_json::json!({}));
    }

    #[test]
    fn test_parse_tool_ts_missing_name() {
        let content = r#"
export const description = "Test";
export default async function () {}
"#;
        let result = parse_tool_ts(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("name"));
    }

    #[test]
    fn test_parse_tool_ts_missing_description() {
        let content = r#"
export const name = "Test";
export default async function () {}
"#;
        let result = parse_tool_ts(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("description"));
    }

    #[test]
    fn test_parse_tool_ts_empty_name() {
        let content = r#"
export const name = "";
export const description = "Test";
export default async function () {}
"#;
        let result = parse_tool_ts(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("name cannot be empty"));
    }

    #[test]
    fn test_parse_tool_ts_single_quotes() {
        let content = r#"
export const name = 'My Tool';
export const description = 'Does things';

export default async function () {
  return "done";
}
"#;

        let result = parse_tool_ts(content);
        assert!(result.is_ok());
        let tool = result.unwrap();
        assert_eq!(tool.name, "My Tool");
        assert_eq!(tool.description, "Does things");
    }

    #[test]
    fn test_parse_tool_ts_with_as_const() {
        let content = r#"
export const name = "Typed Tool";
export const description = "Has as const";

export const inputSchema = {
  type: "object",
  properties: {
    query: { type: "string", description: "Search query" },
  },
  required: ["query"],
} as const;

export default async function ({ inputs, mcp }) {
  return await mcp.search({ query: inputs.query });
}
"#;

        let result = parse_tool_ts(content);
        assert!(result.is_ok(), "Parse failed: {:?}", result.err());
        let tool = result.unwrap();
        assert_eq!(tool.input_schema["properties"]["query"]["type"], "string");
    }

    #[test]
    fn test_extract_input_schema_nested_braces() {
        let content = r#"
export const name = "Nested";
export const description = "Has nested objects";

export const inputSchema = {
  type: "object",
  properties: {
    config: {
      type: "object",
      properties: {
        nested: { type: "string" }
      }
    }
  },
  required: [],
};

export default async function () {}
"#;

        let result = parse_tool_ts(content);
        assert!(result.is_ok(), "Parse failed: {:?}", result.err());
        let tool = result.unwrap();
        assert!(tool.input_schema["properties"]["config"]["properties"]["nested"].is_object());
    }

    #[test]
    fn test_brace_match_with_strings() {
        // Braces inside string literals should not affect matching
        let s = r#"{ key: "value with { and }", other: 1 }"#;
        let result = brace_match(s);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), s);
    }

    #[test]
    fn test_brace_match_unmatched() {
        let s = "{ open: true";
        let result = brace_match(s);
        assert!(result.is_err());
    }

    #[test]
    fn test_infer_required_tools() {
        let code = r#"
export default async function ({ inputs, mcp }) {
  const result = await mcp.read({ path: "file.txt" });
  await mcp.write({ file_path: "out.txt", content: "hello" });
  const data = await mcp.bash({ command: "ls" });
  await mcp.search({ query: "test" });
}
"#;

        let tools = infer_required_tools(code);
        assert!(tools.contains(&"read".to_string()));
        assert!(tools.contains(&"write".to_string()));
        assert!(tools.contains(&"bash".to_string()));
        assert!(tools.contains(&"search".to_string()));
    }

    #[test]
    fn test_infer_required_tools_no_matches() {
        let code = r#"
export default async function () {
  console.log("hello");
}
"#;
        let tools = infer_required_tools(code);
        assert!(tools.is_empty());
    }

    #[test]
    fn test_code_is_entire_file() {
        let content = r#"import { readFileSync } from "fs";

export const name = "File Reader";
export const description = "Reads files";

export default async function ({ inputs }) {
  return readFileSync(inputs.path, "utf-8");
}
"#;

        let tool = parse_tool_ts(content).unwrap();
        // Code should be the entire file content
        assert!(tool.code.starts_with("import"));
        assert!(tool.code.contains("export const name"));
        assert!(tool.code.contains("export default"));
    }
}
