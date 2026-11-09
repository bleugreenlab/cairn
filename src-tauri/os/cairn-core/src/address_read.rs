//! The app's Read pane (`cairn_uri::Surface::Read`): the operator opening any
//! cairn:// address that has no dedicated view and seeing what the read verb
//! returns for it.
//!
//! It has no renderer of its own. [`read_address`] is a run-less read of one
//! address through the read verb's own door ([`crate::dispatch::dispatch_tool`]),
//! exactly as [`crate::db_browser::read_db`] is for database URIs, so the pane
//! shows the Markdown an agent's `read` gets, scrubbed at the same final
//! crossing. It widens nothing: the runner's machine secret already admits a
//! run-less read of every such address at `/api/mcp`, and the invoke route that
//! carries this door takes no weaker credential.
//!
//! Only cairn:// addresses the grammar resolves are read. A `file:` target, a
//! web page, or a string that names nothing is refused before dispatch, so the
//! door cannot become a general file or network reader.

use std::collections::HashMap;
use std::sync::Mutex;

use cairn_common::read::{ReadBatchEnvelope, SegmentKind};
use cairn_uri::{delegate_thread_descendant, parse_uri};
use serde::Serialize;
use serde_json::json;

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;

/// Why an address was not read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressReadError {
    /// Not a cairn:// address the grammar resolves.
    NotAnAddress(String),
    /// The read verb answered with something that is not a read envelope.
    Failed(String),
}

impl std::fmt::Display for AddressReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAnAddress(message) | Self::Failed(message) => f.write_str(message),
        }
    }
}

/// What the pane renders: the read's body and its affordance blocks, both the
/// Markdown an agent sees, and whether the read failed in place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressRead {
    pub uri: String,
    pub body: String,
    pub affordances: Vec<String>,
    pub failed: bool,
}

/// `uri` as an address the pane may read, or why it is not one.
fn gate(uri: &str) -> Result<&str, AddressReadError> {
    let target = uri.trim();
    let identity = target.split(['?', '#']).next().unwrap_or_default();
    let resolves = target.starts_with("cairn://")
        && parse_uri(identity)
            .and_then(|resource| delegate_thread_descendant(resource).ok())
            .is_some();
    if resolves {
        Ok(target)
    } else {
        Err(AddressReadError::NotAnAddress(format!(
            "{target}: the Read pane reads cairn:// addresses, and this one names nothing"
        )))
    }
}

/// Read one cairn:// address as a run-less read.
pub async fn read_address(orch: &Orchestrator, uri: &str) -> Result<AddressRead, AddressReadError> {
    let target = gate(uri)?;
    let request = McpCallbackRequest {
        tool: "read_batch".to_string(),
        payload: json!({ "paths": [target], "include_bodies": true }),
        ..Default::default()
    };
    let cursors = Mutex::new(HashMap::new());
    let output = crate::dispatch::dispatch_tool(orch, &request, &cursors)
        .await
        .into_inner();
    let envelope: ReadBatchEnvelope = serde_json::from_str(&output.content).map_err(|error| {
        AddressReadError::Failed(format!(
            "the read of {target} returned no envelope: {error}"
        ))
    })?;
    let failed = envelope
        .segments
        .first()
        .is_some_and(|segment| matches!(segment.kind, SegmentKind::Error | SegmentKind::Duplicate));
    let body = envelope
        .bodies
        .and_then(|bodies| bodies.into_iter().next())
        .unwrap_or(envelope.text);
    let affordances = envelope
        .affordances
        .into_iter()
        .map(|affordance| affordance.block)
        .filter(|block| !block.is_empty() && !body.contains(block.as_str()))
        .collect();
    Ok(AddressRead {
        uri: target.to_string(),
        body,
        affordances,
        failed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_resolving_cairn_addresses_pass_the_gate() {
        for uri in [
            "cairn://p/cairn/42",
            "cairn://p/cairn/42/1/builder/questions/q-1",
            "cairn://executors/bbs",
            " cairn://p/cairn/general/todos?limit=5 ",
        ] {
            assert!(gate(uri).is_ok(), "{uri}");
        }
        for uri in [
            "file:src/lib.rs",
            "https://example.com",
            "cairn://nope",
            "cairn://p/cairn/design-review/procedures",
            "",
        ] {
            assert!(
                matches!(gate(uri), Err(AddressReadError::NotAnAddress(_))),
                "{uri}"
            );
        }
    }
}
