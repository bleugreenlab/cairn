//! Cairn URI resolution and display-relativization for the MCP verbs: home/base
//! shorthand expansion and target validation.
use cairn_common::query::split_target_query;
use cairn_common::uri::parse_uri as parse_cairn_uri;

use crate::schemas::ChangeInput;
use crate::server::CairnCmd;

const CAIRN_URI_PREFIX: &str = "cairn://";
const CAIRN_HOME_PREFIX: &str = "cairn:~/";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolvedTarget {
    CairnUri(String),
    FileUri(String),
}

fn invalid_target_message(target: &str) -> String {
    format!(
        "Invalid target: expected cairn://... or file:...; use file:relative/path (worktree-relative), file:/absolute/path, or bare file: for the worktree root instead of '{target}'"
    )
}

fn unsupported_positional_shorthand_message(target: &str) -> String {
    format!(
        "Unsupported shorthand: positional Cairn URIs (`cairn:./...`, `cairn:../...`) are not supported; use canonical `cairn://p/...` or home-anchored `cairn:~/...` instead ({target})"
    )
}

impl CairnCmd {
    #[cfg(test)]
    fn resolve_target(&self, target: &str) -> Result<ResolvedTarget, String> {
        self.resolve_target_with(target)
    }

    /// Resolve a target without expanding `cairn:~/` client-side.
    ///
    /// The host derives home from the authenticated run on every request. Forwarding
    /// the shorthand raw keeps that resolution current when a thread is renamed and
    /// also lets one pooled `cairn-cmd` serve call threads with different homes.
    fn resolve_target_with(&self, target: &str) -> Result<ResolvedTarget, String> {
        if target == "cairn:~" || target.starts_with(CAIRN_HOME_PREFIX) {
            return Ok(ResolvedTarget::CairnUri(target.to_string()));
        }
        if target.starts_with(CAIRN_URI_PREFIX) {
            if parse_cairn_uri(target).is_none() {
                return Err(format!("Invalid cairn resource URI: {}", target));
            }
            return Ok(ResolvedTarget::CairnUri(target.to_string()));
        }

        if let Some(reference) = target.strip_prefix("cairn:") {
            if reference.starts_with("./") || reference.starts_with("../") {
                return Err(unsupported_positional_shorthand_message(target));
            }
        }

        // Forward the `file:` identity to core unchanged; core owns resolution
        // semantics (relative vs absolute, jailing, the file:~ hard cut).
        if target.starts_with("file:") {
            return Ok(ResolvedTarget::FileUri(target.to_string()));
        }

        Err(invalid_target_message(target))
    }

    pub(crate) fn rewrite_change_targets(
        &self,
        input: &ChangeInput,
    ) -> Result<ChangeInput, String> {
        let mut rewritten = input.clone();
        // Targets are guaranteed present by `validate_change_value`, which runs
        // before this; the Option handling keeps the lenient types honest.
        if let Some(changes) = rewritten.changes.as_mut() {
            for change in changes.iter_mut() {
                if let Some(target) = change.target.as_ref() {
                    let resolved = match self.resolve_target_with(target)? {
                        ResolvedTarget::CairnUri(uri) | ResolvedTarget::FileUri(uri) => uri,
                    };
                    change.target = Some(resolved);
                }
            }
        }
        Ok(rewritten)
    }

    pub(crate) fn resolve_read_target(&self, target: &str) -> Result<String, String> {
        let split = split_target_query(target)?;
        let resolved = match self.resolve_target_with(&split.identity)? {
            ResolvedTarget::CairnUri(uri) | ResolvedTarget::FileUri(uri) => uri,
        };

        Ok(match split.raw_query {
            Some(query) if !query.is_empty() => format!("{resolved}?{query}"),
            _ => resolved,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::ChangeItemInput;
    use crate::test_support::create_test_mcp_with_home_uri;

    #[test]
    fn home_relative_targets_pass_through_for_host_resolution() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));
        assert_eq!(
            mcp.resolve_read_target("cairn:~/diff").unwrap(),
            "cairn:~/diff"
        );
        assert_eq!(
            mcp.resolve_read_target("cairn:~/diff?limit=5").unwrap(),
            "cairn:~/diff?limit=5"
        );
    }

    #[test]
    fn canonical_and_file_targets_pass_through_unchanged() {
        let mcp = create_test_mcp_with_home_uri(None);
        assert_eq!(
            mcp.resolve_target("cairn://p/OTHER/9"),
            Ok(ResolvedTarget::CairnUri("cairn://p/OTHER/9".into()))
        );
        assert_eq!(
            mcp.resolve_target("file:src/lib.rs"),
            Ok(ResolvedTarget::FileUri("file:src/lib.rs".into()))
        );
    }

    #[test]
    fn positional_shorthand_and_bare_paths_are_rejected() {
        let mcp = create_test_mcp_with_home_uri(None);
        assert!(mcp
            .resolve_target("cairn:../artifact")
            .unwrap_err()
            .contains("Unsupported shorthand"));
        assert!(mcp
            .resolve_target("src/main.rs")
            .unwrap_err()
            .contains("expected cairn://... or file:..."));
    }

    #[test]
    fn rewrite_change_targets_only_rewrites_targets() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086"));
        let input = ChangeInput {
            changes: Some(vec![ChangeItemInput {
                target: Some("cairn:~/messages".into()),
                mode: Some("append".into()),
                payload: Some(serde_json::json!({"content":"cairn://p/CAIRN/1086"})),
            }]),
            commit_msg: None,
            preview: None,
            atomic: None,
            conflict_markers_reason: None,
        };
        let rewritten = mcp.rewrite_change_targets(&input).unwrap();
        let change = &rewritten.changes.as_ref().unwrap()[0];
        assert_eq!(change.target.as_deref(), Some("cairn:~/messages"));
        assert_eq!(change.payload, input.changes.as_ref().unwrap()[0].payload);
    }
}
