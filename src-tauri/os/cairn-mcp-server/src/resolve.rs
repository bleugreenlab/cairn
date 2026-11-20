//! Cairn URI resolution and display-relativization for the MCP verbs: home/base
//! shorthand expansion and target validation.
use cairn_common::query::split_target_query;
use cairn_common::uri_refusal::refusal_message;
use cairn_uri::UriGrammar;

use crate::schemas::ChangeInput;
use crate::server::CairnMcpServer;

const CAIRN_URI_PREFIX: &str = "cairn://";
const CAIRN_HOME_PREFIX: &str = "cairn:~/";
const CAIRN_PROJECT_PREFIX: &str = "cairn:@";

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

impl CairnMcpServer {
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
        self.resolve_target_against(target, UriGrammar::current())
    }

    /// [`Self::resolve_target_with`] against a given grammar, so a test can stand
    /// in for a runner older than the address it is handed.
    fn resolve_target_against(
        &self,
        target: &str,
        grammar: &UriGrammar,
    ) -> Result<ResolvedTarget, String> {
        if target == "cairn:~"
            || target.starts_with(CAIRN_HOME_PREFIX)
            || target.starts_with(CAIRN_PROJECT_PREFIX)
        {
            return Ok(ResolvedTarget::CairnUri(target.to_string()));
        }
        if target.starts_with(CAIRN_URI_PREFIX) {
            if let Err(refusal) = grammar.classify(target) {
                return Err(refusal_message(target, &refusal));
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
    use crate::test_support::test_server;

    #[test]
    fn home_relative_targets_pass_through_for_host_resolution() {
        let mcp = test_server();
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
    fn project_relative_targets_pass_through_for_host_resolution() {
        let mcp = test_server();
        for target in ["cairn:@", "cairn:@/issues?status=active", "cairn:@1086"] {
            assert_eq!(mcp.resolve_read_target(target).unwrap(), target);
        }
    }

    #[test]
    fn canonical_and_file_targets_pass_through_unchanged() {
        let mcp = test_server();
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
        let mcp = test_server();
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
    fn a_garbled_cairn_target_is_refused_naming_its_fault() {
        let mcp = test_server();
        assert_eq!(
            mcp.resolve_target("cairn://posts//1").unwrap_err(),
            "Malformed cairn address `cairn://posts//1`: segment 2 is empty (a doubled or \
             trailing `/`)."
        );
    }

    /// The specimen: a launcher newer than its runner asks for
    /// `cairn://dev/launch-ticket`. The runner says it lacks the family and
    /// who it is, not that the address is garbled.
    #[test]
    fn a_family_this_runner_lacks_is_refused_as_skew_naming_the_build() {
        let mcp = test_server();
        let older = UriGrammar::without(|resource| {
            matches!(
                resource,
                cairn_uri::CairnResource::Dev
                    | cairn_uri::CairnResource::DevDb
                    | cairn_uri::CairnResource::DevPid
                    | cairn_uri::CairnResource::DevLaunchTicket
            )
        });
        let message = mcp
            .resolve_target_against("cairn://dev/launch-ticket", &older)
            .unwrap_err();
        assert!(
            message.starts_with(&format!(
                "This runner ({}) has no `cairn://dev` resource.",
                cairn_common::uri_refusal::runner_identity()
            )),
            "{message}"
        );
        assert!(message.contains("rebuild or update it"), "{message}");
        assert!(message.contains("Known families: "), "{message}");
        assert!(message.contains("models"), "{message}");

        // The same address against this build resolves.
        assert!(mcp.resolve_target("cairn://dev/launch-ticket").is_ok());
    }

    #[test]
    fn a_near_miss_cairn_target_suggests_the_nearest_family() {
        let mcp = test_server();
        let message = mcp
            .rewrite_change_targets(&ChangeInput {
                changes: Some(vec![ChangeItemInput {
                    target: Some("cairn://modles/catalog".into()),
                    mode: Some("patch".into()),
                    payload: None,
                }]),
                commit_msg: None,
                preview: None,
                atomic: None,
                conflict_markers_reason: None,
            })
            .unwrap_err();
        assert!(
            message.contains("has no `cairn://modles` resource. Did you mean `cairn://models`?"),
            "{message}"
        );
    }

    #[test]
    fn rewrite_change_targets_only_rewrites_targets() {
        let mcp = test_server();
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
