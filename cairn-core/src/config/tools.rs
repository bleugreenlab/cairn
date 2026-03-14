//! Custom tool configuration file reading.
//!
//! Tools are stored as `.ts` files with named exports.
//! - Workspace-scoped: `~/.cairn/tools/`
//! - Project-scoped: `[project-dir]/.cairn/tools/`

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::tools::parse_tool_ts;

use super::{id_from_path, ConfigResult};

/// Tool loaded from a file
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileTool {
    pub id: String,
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
    pub code: String,
    pub required_tools: Vec<String>,
    /// Whether this tool is project-scoped (vs workspace-scoped)
    pub is_project_scoped: bool,
    /// Path to the source file
    pub file_path: PathBuf,
}

/// List all tools from files
///
/// Reads workspace-scoped tools from `~/.cairn/tools/`.
/// If project_path is Some, also includes tools from `[project_path]/.cairn/tools/`.
/// Project-scoped tools take precedence over workspace-scoped ones with the same ID.
pub fn list_tools(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<Vec<ConfigResult<FileTool>>, String> {
    let mut results = vec![];
    let mut seen_ids = std::collections::HashSet::new();

    // Read project-scoped tools first (they take precedence)
    if let Some(proj_path) = project_path {
        let proj_dir = proj_path.join(".cairn").join("tools");
        if proj_dir.exists() && proj_dir.is_dir() {
            for entry in std::fs::read_dir(&proj_dir)
                .map_err(|e| format!("Failed to read project tools directory: {}", e))?
            {
                let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
                let path = entry.path();

                if path.extension().and_then(|e| e.to_str()) != Some("ts") {
                    continue;
                }

                if let Some(id) = id_from_path(&path) {
                    seen_ids.insert(id);
                }
                results.push(load_tool_file(&path, true));
            }
        }
    }

    // Read workspace-scoped tools (skip if already loaded from project)
    let ws_dir = config_dir.join("tools");
    if ws_dir.exists() {
        for entry in std::fs::read_dir(&ws_dir)
            .map_err(|e| format!("Failed to read tools directory: {}", e))?
        {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let path = entry.path();

            // Skip directories and non-.ts files
            if path.is_dir() || path.extension().and_then(|e| e.to_str()) != Some("ts") {
                continue;
            }

            // Skip if project-scoped version exists
            if let Some(id) = id_from_path(&path) {
                if seen_ids.contains(&id) {
                    continue;
                }
            }

            results.push(load_tool_file(&path, false));
        }
    }

    Ok(results)
}

/// Get a specific tool by ID
///
/// Checks project-scoped first (if project_path provided), then workspace-scoped.
pub fn get_tool(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<Option<FileTool>, String> {
    // Try project-scoped first if project specified
    if let Some(proj_path) = project_path {
        let path = proj_path
            .join(".cairn")
            .join("tools")
            .join(format!("{}.ts", id));
        if path.exists() {
            return match load_tool_file(&path, true) {
                ConfigResult::Ok(tool) => Ok(Some(tool)),
                ConfigResult::Err { error, .. } => Err(error),
            };
        }
    }

    // Try workspace-scoped
    let path = config_dir.join("tools").join(format!("{}.ts", id));
    if path.exists() {
        return match load_tool_file(&path, false) {
            ConfigResult::Ok(tool) => Ok(Some(tool)),
            ConfigResult::Err { error, .. } => Err(error),
        };
    }

    Ok(None)
}

/// Load a single tool file
fn load_tool_file(path: &Path, is_project_scoped: bool) -> ConfigResult<FileTool> {
    let id = match id_from_path(path) {
        Some(id) => id,
        None => {
            return ConfigResult::Err {
                path: path.to_path_buf(),
                error: "Could not determine tool ID from filename".to_string(),
            }
        }
    };

    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            return ConfigResult::Err {
                path: path.to_path_buf(),
                error: format!("Failed to read file: {}", e),
            }
        }
    };

    match parse_tool_ts(&content) {
        Ok(parsed) => ConfigResult::Ok(FileTool {
            id,
            name: parsed.name,
            description: parsed.description,
            input_schema: parsed.input_schema,
            code: parsed.code,
            required_tools: parsed.required_tools,
            is_project_scoped,
            file_path: path.to_path_buf(),
        }),
        Err(e) => ConfigResult::Err {
            path: path.to_path_buf(),
            error: e,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_load_tool_file() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("check_status.ts");

        let content = r#"export const name = "Check Status";
export const description = "Check issue statuses";

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

export default async function ({ inputs, mcp, PROJECT_ID }) {
  const results = await Promise.all(
    inputs.issues.map(id => mcp.read({ path: `cairn://${PROJECT_ID}/${id}` }))
  );
  return JSON.stringify(results, null, 2);
}
"#;

        std::fs::write(&path, content).unwrap();

        let result = load_tool_file(&path, false);
        match result {
            ConfigResult::Ok(tool) => {
                assert_eq!(tool.id, "check_status");
                assert_eq!(tool.name, "Check Status");
                assert_eq!(tool.description, "Check issue statuses");
                assert!(!tool.is_project_scoped);
                assert!(tool.code.contains("inputs.issues"));
                assert!(tool.required_tools.contains(&"read".to_string()));
            }
            ConfigResult::Err { error, .. } => panic!("Failed to load: {}", error),
        }
    }

    #[test]
    fn test_project_scoped_precedence() {
        let temp = tempdir().unwrap();

        // Create workspace-level tool
        let ws_dir = temp.path().join("workspace").join("tools");
        std::fs::create_dir_all(&ws_dir).unwrap();
        std::fs::write(
            ws_dir.join("my_tool.ts"),
            "export const name = 'Workspace Tool';\nexport const description = 'From workspace';\nexport default async function () { return 'ws'; }\n",
        )
        .unwrap();

        // Create project-level tool with same ID
        let proj_dir = temp.path().join("project").join(".cairn").join("tools");
        std::fs::create_dir_all(&proj_dir).unwrap();
        std::fs::write(
            proj_dir.join("my_tool.ts"),
            "export const name = 'Project Tool';\nexport const description = 'From project';\nexport default async function () { return 'proj'; }\n",
        )
        .unwrap();

        // Load project tool directly
        let result = load_tool_file(&proj_dir.join("my_tool.ts"), true);
        match result {
            ConfigResult::Ok(tool) => {
                assert_eq!(tool.name, "Project Tool");
                assert!(tool.is_project_scoped);
            }
            ConfigResult::Err { error, .. } => panic!("Failed to load: {}", error),
        }
    }
}
