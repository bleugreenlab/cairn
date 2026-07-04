//! Cairn URI resolution and display-relativization for the MCP verbs: home/base
//! shorthand expansion, target validation, and canonical-URI segment math.
use cairn_common::query::split_target_query;
use cairn_common::uri::parse_uri as parse_cairn_uri;

use crate::output::CallbackOutcome;
use crate::schemas::ChangeInput;
use crate::server::{CairnCmd, TerminalReadResult};

const CAIRN_URI_PREFIX: &str = "cairn://";
const CAIRN_HOME_PREFIX: &str = "cairn:~/";
const CAIRN_RESOURCE_ROOT_SEGMENTS: usize = 2;

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

fn find_cairn_uri_end(text: &str) -> usize {
    text.find(|c: char| {
        c.is_whitespace()
            || matches!(
                c,
                '<' | '>' | '"' | '\'' | '`' | '(' | ')' | '[' | ']' | '{' | '}'
            )
    })
    .unwrap_or(text.len())
}

fn split_uri_suffix(candidate: &str) -> (&str, &str) {
    let mut split_at = candidate.len();
    while split_at > 0 {
        let ch = candidate[..split_at].chars().next_back().unwrap();
        if matches!(ch, '.' | ',' | ':' | ';' | '!' | '?') {
            split_at -= ch.len_utf8();
        } else {
            break;
        }
    }
    (&candidate[..split_at], &candidate[split_at..])
}

impl CairnCmd {
    #[cfg(test)]
    fn current_base_uri(&self) -> Option<String> {
        self.base_uri.lock().unwrap().clone()
    }

    fn home_uri_string(&self) -> Option<String> {
        self.home_uri.as_ref().map(|uri| uri.to_string())
    }

    fn set_base_uri(&self, uri: String) {
        *self.base_uri.lock().unwrap() = Some(uri);
    }

    pub(crate) fn note_successful_resource_read(&self, uri: &str) {
        self.set_base_uri(uri.to_string());
    }

    fn relativize_cairn_uri_for_display(&self, uri: &str) -> String {
        let target_segments = match Self::canonical_uri_segments(uri) {
            Ok(segments) => segments,
            Err(_) => return uri.to_string(),
        };

        let mut best = uri.to_string();

        if let Some(home_uri) = self.home_uri_string() {
            if let Ok(home_segments) = Self::canonical_uri_segments(&home_uri) {
                if let Some(relative) =
                    Self::relative_display_from_home(&home_segments, &target_segments)
                {
                    if relative.len() < best.len() {
                        best = relative;
                    }
                }
            }
        }

        best
    }

    pub(crate) fn relativize_cairn_uris_in_text(&self, text: &str) -> String {
        let mut rendered = String::with_capacity(text.len());
        let mut remaining = text;

        while let Some(index) = remaining.find(CAIRN_URI_PREFIX) {
            rendered.push_str(&remaining[..index]);
            let candidate = &remaining[index..];
            let raw_end = find_cairn_uri_end(candidate);
            let raw_uri = &candidate[..raw_end];
            let (uri, suffix) = split_uri_suffix(raw_uri);

            if parse_cairn_uri(uri).is_some() {
                rendered.push_str(&self.relativize_cairn_uri_for_display(uri));
                rendered.push_str(suffix);
            } else {
                rendered.push_str(raw_uri);
            }

            remaining = &candidate[raw_end..];
        }

        rendered.push_str(remaining);
        rendered
    }

    fn resolve_target(&self, target: &str) -> Result<ResolvedTarget, String> {
        if target.starts_with(CAIRN_URI_PREFIX) {
            if parse_cairn_uri(target).is_none() {
                return Err(format!("Invalid cairn resource URI: {}", target));
            }
            return Ok(ResolvedTarget::CairnUri(target.to_string()));
        }

        if let Some(suffix) = target.strip_prefix(CAIRN_HOME_PREFIX) {
            let home_uri = self
                .home_uri
                .as_ref()
                .map(|uri| uri.as_ref().clone())
                .ok_or_else(|| {
                    format!(
                        "Cannot resolve home-relative Cairn URI without home_uri: {}",
                        target
                    )
                })?;
            let resolved = Self::resolve_uri_reference(&home_uri, suffix)?;
            return Ok(ResolvedTarget::CairnUri(resolved));
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
                    let resolved = match self.resolve_target(target)? {
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
        let resolved = match self.resolve_target(&split.identity)? {
            ResolvedTarget::CairnUri(uri) | ResolvedTarget::FileUri(uri) => uri,
        };

        Ok(match split.raw_query {
            Some(query) if !query.is_empty() => format!("{resolved}?{query}"),
            _ => resolved,
        })
    }

    pub(crate) fn should_update_base_uri_after_read(
        &self,
        tool_name: &str,
        response: &CallbackOutcome,
    ) -> bool {
        if !response.transport_ok || Self::resource_result_looks_like_error(&response.result) {
            return false;
        }

        if tool_name == "read_resource" {
            return serde_json::from_str::<TerminalReadResult>(&response.result).is_ok();
        }

        true
    }

    fn resource_result_looks_like_error(result: &str) -> bool {
        [
            "Error ",
            "Invalid ",
            "Database error:",
            "Issue ",
            "Project '",
            "Run ",
            "No jobs found for issue ",
            "Missing Authorization header",
            "Invalid token:",
            "Failed to parse request:",
        ]
        .iter()
        .any(|prefix| result.starts_with(prefix))
    }

    fn resolve_uri_reference(base_uri: &str, reference: &str) -> Result<String, String> {
        let mut segments = Self::canonical_uri_segments(base_uri)?;

        for segment in reference.split('/') {
            match segment {
                "" | "." => continue,
                ".." => {
                    if segments.len() <= CAIRN_RESOURCE_ROOT_SEGMENTS {
                        return Err(format!(
                            "Cairn URI traversal cannot escape above cairn://p/<project>: {} from {}",
                            reference, base_uri
                        ));
                    }
                    segments.pop();
                }
                _ => segments.push(segment.to_string()),
            }
        }

        let resolved = format!("{}{}", CAIRN_URI_PREFIX, segments.join("/"));
        if parse_cairn_uri(&resolved).is_none() {
            return Err(format!("Invalid Cairn shorthand target: {}", resolved));
        }

        Ok(resolved)
    }

    fn relative_display_from_home(home: &[String], target: &[String]) -> Option<String> {
        if target.len() < home.len() || home != &target[..home.len()] {
            return None;
        }

        let remainder = &target[home.len()..];
        let suffix = if remainder.is_empty() {
            "~/".to_string()
        } else {
            format!("~/{}", remainder.join("/"))
        };

        Some(format!("cairn:{}", suffix))
    }

    fn canonical_uri_segments(uri: &str) -> Result<Vec<String>, String> {
        if parse_cairn_uri(uri).is_none() {
            return Err(format!("Invalid cairn resource URI: {}", uri));
        }

        let identity = split_target_query(uri)?.identity;
        let stripped = identity
            .strip_prefix(CAIRN_URI_PREFIX)
            .ok_or_else(|| format!("Unknown resource scheme (expected cairn://): {}", uri))?;
        let segments = stripped
            .split('/')
            .filter(|segment| !segment.is_empty())
            .map(|segment| segment.to_string())
            .collect::<Vec<_>>();

        if segments.len() < CAIRN_RESOURCE_ROOT_SEGMENTS
            || segments.first().map(String::as_str) != Some("p")
        {
            return Err(format!("Invalid cairn resource URI: {}", uri));
        }

        Ok(segments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::ChangeItemInput;
    use crate::test_support::create_test_mcp_with_home_uri;

    #[test]
    fn test_base_uri_initializes_from_home_uri() {
        let home_uri = "cairn://p/CAIRN/1086/1/builder";
        let mcp = create_test_mcp_with_home_uri(Some(home_uri));

        assert_eq!(
            mcp.home_uri.as_ref().map(|uri| uri.as_str()),
            Some(home_uri)
        );
        assert_eq!(mcp.current_base_uri().as_deref(), Some(home_uri));
    }

    #[test]
    fn test_resolve_home_shorthand_with_home_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));

        assert_eq!(
            mcp.resolve_target("cairn:~/chat"),
            Ok(ResolvedTarget::CairnUri(
                "cairn://p/CAIRN/1086/1/builder/chat".to_string()
            ))
        );
    }

    #[test]
    fn test_resolve_home_shorthand_root_with_home_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));

        assert_eq!(
            mcp.resolve_target("cairn:~/"),
            Ok(ResolvedTarget::CairnUri(
                "cairn://p/CAIRN/1086/1/builder".to_string()
            ))
        );
    }

    #[test]
    fn test_resolve_home_shorthand_without_home_uri() {
        let mcp = create_test_mcp_with_home_uri(None);

        let err = mcp.resolve_target("cairn:~/messages").unwrap_err();
        assert!(err.contains("home_uri"));
    }

    #[test]
    fn test_resolve_target_rejects_positional_shorthands() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));

        let current_err = mcp.resolve_target("cairn:./messages").unwrap_err();
        assert!(current_err.contains("Unsupported shorthand"));

        let parent_err = mcp.resolve_target("cairn:../artifact").unwrap_err();
        assert!(parent_err.contains("Unsupported shorthand"));
    }

    #[test]
    fn test_relativize_cairn_uri_uses_home_relative_display() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));
        mcp.set_base_uri("cairn://p/CAIRN/1086".to_string());

        assert_eq!(
            mcp.relativize_cairn_uri_for_display("cairn://p/CAIRN/1086/1/builder/chat"),
            "cairn:~/chat"
        );
    }

    #[test]
    fn test_relativize_cairn_uri_keeps_cross_project_canonical() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));
        mcp.set_base_uri("cairn://p/CAIRN/1086".to_string());

        assert_eq!(
            mcp.relativize_cairn_uri_for_display("cairn://p/OTHER/99"),
            "cairn://p/OTHER/99"
        );
    }

    #[test]
    fn test_relativize_cairn_uris_in_text_updates_markdown_and_punctuation() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086"));
        let text =
            "See [messages](cairn://p/CAIRN/1086/messages), then cairn://p/CAIRN/1086/messages.";

        let rendered = mcp.relativize_cairn_uris_in_text(text);

        assert_eq!(
            rendered,
            "See [messages](cairn:~/messages), then cairn:~/messages."
        );
    }

    #[test]
    fn test_resolve_read_target_accepts_home_shorthand() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086"));

        assert_eq!(
            mcp.resolve_read_target("cairn:~/messages"),
            Ok("cairn://p/CAIRN/1086/messages".to_string())
        );
    }

    #[test]
    fn test_resolve_target_rejects_bare_filesystem_paths() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));

        let err = mcp.resolve_target("src/main.rs").unwrap_err();
        assert!(err.contains("expected cairn://... or file:..."));
    }

    #[test]
    fn test_resolve_target_forwards_file_targets_unchanged() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));

        // The CLI no longer normalizes file targets; core owns resolution.
        // Worktree root, relative, and absolute identities all forward verbatim.
        assert_eq!(
            mcp.resolve_target("file:"),
            Ok(ResolvedTarget::FileUri("file:".to_string()))
        );
        assert_eq!(
            mcp.resolve_target("file:src/lib.rs"),
            Ok(ResolvedTarget::FileUri("file:src/lib.rs".to_string()))
        );
        assert_eq!(
            mcp.resolve_target("file:/Users/x/abs.rs"),
            Ok(ResolvedTarget::FileUri("file:/Users/x/abs.rs".to_string()))
        );
    }

    #[test]
    fn test_successful_resource_read_updates_base_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));

        mcp.note_successful_resource_read("cairn://p/CAIRN/1086/messages");

        assert_eq!(
            mcp.current_base_uri().as_deref(),
            Some("cairn://p/CAIRN/1086/messages")
        );
    }

    #[test]
    fn test_failed_issue_read_does_not_update_base_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086"));
        let before = mcp.current_base_uri();
        let response = CallbackOutcome {
            result: "Issue CAIRN-999 not found".to_string(),
            transport_ok: true,
            ..Default::default()
        };

        assert!(!mcp.should_update_base_uri_after_read("read_issue_resource", &response));
        assert_eq!(mcp.current_base_uri(), before);
    }

    #[test]
    fn test_transport_failure_does_not_update_base_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086"));
        let before = mcp.current_base_uri();
        let response = CallbackOutcome {
            result: "Error calling Tauri: boom".to_string(),
            transport_ok: false,
            ..Default::default()
        };

        assert!(!mcp.should_update_base_uri_after_read("read_issue_resource", &response));
        assert_eq!(mcp.current_base_uri(), before);
    }

    #[test]
    fn test_terminal_parse_failure_does_not_update_base_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086"));
        let response = CallbackOutcome {
            result: "not-json".to_string(),
            transport_ok: true,
            ..Default::default()
        };

        assert!(!mcp.should_update_base_uri_after_read("read_resource", &response));
    }

    #[test]
    fn test_file_target_resolution_does_not_update_base_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));
        let before = mcp.current_base_uri();

        let resolved = mcp.resolve_target("file:Cargo.toml").unwrap();

        assert_eq!(
            resolved,
            ResolvedTarget::FileUri("file:Cargo.toml".to_string())
        );
        assert_eq!(mcp.current_base_uri(), before);
    }

    #[test]
    fn test_rewrite_change_targets_resolves_home_shorthand_without_mutating_base_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086"));
        let before = mcp.current_base_uri();
        let input = ChangeInput {
            changes: Some(vec![
                ChangeItemInput {
                    target: Some("cairn:~/messages".to_string()),
                    mode: Some("append".to_string()),
                    payload: Some(serde_json::json!({ "content": "hello" })),
                },
                ChangeItemInput {
                    target: Some("file:docs/notes.md".to_string()),
                    mode: Some("patch".to_string()),
                    payload: Some(serde_json::json!({ "old_string": "old", "new_string": "new" })),
                },
            ]),
            commit_msg: None,
            preview: None,
            atomic: None,
        };

        let rewritten = mcp.rewrite_change_targets(&input).unwrap();
        let changes = rewritten.changes.as_ref().unwrap();

        assert_eq!(
            changes[0].target.as_deref(),
            Some("cairn://p/CAIRN/1086/messages")
        );
        assert_eq!(changes[1].target.as_deref(), Some("file:docs/notes.md"));
        assert_eq!(mcp.current_base_uri(), before);
    }
}
