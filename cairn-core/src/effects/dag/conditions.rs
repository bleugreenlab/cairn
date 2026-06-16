use super::*;

pub(super) fn extract_condition_spec(node: &DbRecipeNode) -> ConditionSpec {
    let config: Option<serde_json::Value> = node
        .config
        .as_ref()
        .and_then(|c| serde_json::from_str(c).ok());

    let condition_type = config
        .as_ref()
        .and_then(|c| c.get("conditionType").and_then(|v| v.as_str()))
        .unwrap_or("programmatic")
        .to_string();

    let expression = config
        .as_ref()
        .and_then(|c| c.get("expression").and_then(|v| v.as_str()))
        .map(|s| s.to_string());

    let question = config
        .as_ref()
        .and_then(|c| c.get("question").and_then(|v| v.as_str()))
        .map(|s| s.to_string());

    let ports = config
        .as_ref()
        .and_then(|c| c.get("ports").and_then(|v| v.as_array()))
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let error_handling = config
        .as_ref()
        .and_then(|c| c.get("errorHandling").and_then(|v| v.as_str()))
        .unwrap_or("use_default")
        .to_string();

    ConditionSpec {
        condition_type,
        expression,
        question,
        ports,
        error_handling,
    }
}
