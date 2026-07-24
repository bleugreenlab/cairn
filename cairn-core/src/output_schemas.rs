//! Output schema resolution and management.
//!
//! Preset schemas are loaded from a configurable directory.
//! Custom schemas are passed directly as JSON.

use std::path::Path;

fn embedded_preset_schema(schema_name: &str) -> Option<&'static str> {
    match schema_name {
        "document" => Some(include_str!("../../../resources/schemas/document.json")),
        "plan" => Some(include_str!("../../../resources/schemas/plan.json")),
        "tasklist" => Some(include_str!("../../../resources/schemas/tasklist.json")),
        "review" => Some(include_str!("../../../resources/schemas/review.json")),
        "checklist" => Some(include_str!("../../../resources/schemas/checklist.json")),
        "implementation" => Some(include_str!(
            "../../../resources/schemas/implementation.json"
        )),
        "return" => Some(include_str!("../../../resources/schemas/return.json")),
        _ => None,
    }
}

/// List of preset schema names
pub(crate) const PRESET_SCHEMAS: &[&str] = &[
    "document",
    "plan",
    "tasklist",
    "review",
    "checklist",
    "implementation",
    "return",
];

/// Check if a schema name is a preset
pub(crate) fn is_preset_schema(name: &str) -> bool {
    PRESET_SCHEMAS.contains(&name)
}

/// Load a preset schema from a schema directory.
///
/// `schema_dir` is host-specific:
/// - Tauri: `resource_dir/resources/schemas`
/// - cairn-server: a configured path or embedded
fn load_preset_schema(schema_dir: &Path, schema_name: &str) -> Result<serde_json::Value, String> {
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

fn load_embedded_preset_schema(schema_name: &str) -> Result<serde_json::Value, String> {
    let content = embedded_preset_schema(schema_name)
        .ok_or_else(|| format!("Embedded preset schema '{}' is not available", schema_name))?;
    serde_json::from_str(content)
        .map_err(|e| format!("Failed to parse embedded schema '{}': {}", schema_name, e))
}

/// Resolve an output schema to its JSON Schema value.
/// - For preset names (e.g., "plan"), loads from the schema directory
/// - For custom schemas (already a JSON object), returns as-is
pub(crate) fn resolve_output_schema(
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
            match schema_dir {
                Some(dir) => {
                    load_preset_schema(dir, name).or_else(|_| load_embedded_preset_schema(name))
                }
                None => load_embedded_preset_schema(name),
            }
        }
        crate::models::OutputSchema::Custom(schema) => Ok(schema.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_preset_schema() {
        assert!(!is_preset_schema("dashboard"));
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
