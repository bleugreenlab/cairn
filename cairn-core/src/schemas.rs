//! Output schema resolution and management.
//!
//! Preset schemas are loaded from a configurable directory.
//! Custom schemas are passed directly as JSON.

use std::path::{Path, PathBuf};

/// List of preset schema names
pub const PRESET_SCHEMAS: &[&str] = &[
    "document",
    "plan",
    "tasklist",
    "review",
    "checklist",
    "implementation",
    "return",
];

/// Check if a schema name is a preset
pub fn is_preset_schema(name: &str) -> bool {
    PRESET_SCHEMAS.contains(&name)
}

/// Load a preset schema from a schema directory.
///
/// `schema_dir` is host-specific:
/// - Tauri: `resource_dir/resources/schemas`
/// - cairn-server: a configured path or embedded
pub fn load_preset_schema(
    schema_dir: &Path,
    schema_name: &str,
) -> Result<serde_json::Value, String> {
    let schema_path = schema_dir.join(format!("{}.json", schema_name));

    if !schema_path.exists() {
        return Err(format!(
            "Preset schema '{}' not found at {:?}",
            schema_name, schema_path
        ));
    }

    let content = std::fs::read_to_string(&schema_path)
        .map_err(|e| format!("Failed to read schema file {:?}: {}", schema_path, e))?;

    serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse schema JSON from {:?}: {}", schema_path, e))
}

/// Resolve an output schema to its JSON Schema value.
/// - For preset names (e.g., "plan"), loads from the schema directory
/// - For custom schemas (already a JSON object), returns as-is
pub fn resolve_output_schema(
    schema_dir: Option<&Path>,
    output_schema: &crate::models::OutputSchema,
) -> Result<serde_json::Value, String> {
    match output_schema {
        crate::models::OutputSchema::Preset(name) => {
            if !is_preset_schema(name) {
                return Err(format!(
                    "Unknown preset schema '{}'. Valid presets: {:?}",
                    name, PRESET_SCHEMAS
                ));
            }
            let dir = schema_dir.ok_or_else(|| {
                format!(
                    "No schema directory configured for loading preset '{}'",
                    name
                )
            })?;
            load_preset_schema(dir, name)
        }
        crate::models::OutputSchema::Custom(schema) => Ok(schema.clone()),
    }
}

/// Write a schema to a temporary file for passing to cairn-mcp via --schema arg
pub fn write_schema_to_temp_file(schema: &serde_json::Value) -> Result<PathBuf, String> {
    let temp_dir = std::env::temp_dir();
    let file_name = format!("cairn-schema-{}.json", uuid::Uuid::new_v4());
    let temp_path = temp_dir.join(file_name);

    let content = serde_json::to_string_pretty(schema)
        .map_err(|e| format!("Failed to serialize schema: {}", e))?;

    std::fs::write(&temp_path, content)
        .map_err(|e| format!("Failed to write schema to temp file: {}", e))?;

    Ok(temp_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_preset_schema() {
        assert!(is_preset_schema("plan"));
        assert!(is_preset_schema("document"));
        assert!(is_preset_schema("review"));
        assert!(is_preset_schema("checklist"));
        assert!(is_preset_schema("implementation"));
        assert!(is_preset_schema("return"));
        assert!(!is_preset_schema("custom"));
        assert!(!is_preset_schema(""));
    }
}
