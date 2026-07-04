//! Shared `#[cfg(test)]` helpers used across the module test suites.
use rmcp::model::CallToolResult;

use crate::schemas::RunInput;
use crate::server::CairnCmd;

pub(crate) fn create_test_mcp_with_home_uri(home_uri: Option<&str>) -> CairnCmd {
    CairnCmd::new_with_home_uri(
        "http://localhost:3847".to_string(),
        "/test/path".to_string(),
        None,
        None,
        vec![],
        home_uri.map(str::to_string),
    )
}

pub(crate) fn get_text(result: &CallToolResult) -> &str {
    result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_ref())
        .unwrap()
}

pub(crate) fn run_input(value: serde_json::Value) -> RunInput {
    serde_json::from_value(value).expect("valid RunInput")
}
