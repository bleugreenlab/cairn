//! Relative resolution over the canonical cairn:// URI grammar.
//!
//! A cursor-style client (the interactive shell) needs to join a typed fragment
//! onto a current location the way a filesystem joins a path onto a working
//! directory. That join is textual; the *judgement* of whether the result names
//! a resource stays with [`parse_uri`], the single authority. Nothing here
//! restates the URI grammar, so a resource family that ships tomorrow is
//! navigable the day it parses.
//!
//! The empty segment path is the workspace root, written [`ROOT_URI`]. It is the
//! one location that deliberately does not parse: no resource lives at
//! `cairn://`, but every canonical URI climbs to it.

use super::parse::parse_uri;

/// The workspace root: the empty segment path under the cairn scheme.
pub const ROOT_URI: &str = "cairn://";

/// The home-anchored shorthand, which the host resolves against the calling run
/// rather than the client resolving it textually.
const HOME_PREFIX: &str = "cairn:~";

/// The segments of a canonical `cairn://` URI, with any `?query` discarded.
///
/// The root yields an empty vector. A target that is not canonical — a `file:`
/// path, a home shorthand, an http URL — is an error rather than an empty path,
/// because silently treating it as the root would navigate somewhere the caller
/// never asked to go.
pub fn uri_segments(uri: &str) -> Result<Vec<String>, String> {
    let identity = uri.split('?').next().unwrap_or(uri);
    let rest = identity
        .strip_prefix(ROOT_URI)
        .ok_or_else(|| format!("not a canonical cairn:// URI: {uri}"))?;
    Ok(rest
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(str::to_string)
        .collect())
}

/// Join segments back into a canonical URI. An empty path is [`ROOT_URI`].
fn join(segments: &[String]) -> String {
    format!("{ROOT_URI}{}", segments.join("/"))
}

/// The closest enclosing URI that actually parses, or the root once the
/// segments run out. `None` only for the root itself, which has no ancestor.
///
/// Climbing rather than popping once is what makes navigation total: an issue
/// node lives at `cairn://p/cairn/4279/1/builder`, and the four-segment prefix
/// `cairn://p/cairn/4279/1` names nothing at all. A single pop would strand the
/// cursor at a location that cannot be read; climbing lands on the issue, which
/// is where a person means to go.
pub fn nearest_ancestor(uri: &str) -> Option<String> {
    let mut segments = uri_segments(uri).ok()?;
    if segments.is_empty() {
        return None;
    }
    loop {
        segments.pop();
        if segments.is_empty() {
            return Some(ROOT_URI.to_string());
        }
        let candidate = join(&segments);
        if parse_uri(&candidate).is_some() {
            return Some(candidate);
        }
    }
}

/// Walk `path`'s segments onto `base`, honoring `.` and `..`.
///
/// Only the final result is validated by the caller: intermediate joins are
/// routinely unparseable (`p/cairn/4279/1` on the way to a node), and rejecting
/// them would make the grammar's own shape unreachable.
fn walk(base: &[String], path: &str) -> Result<Vec<String>, String> {
    let mut segments = base.to_vec();
    for segment in path.split('/') {
        match segment {
            "" | "." => continue,
            ".." => match nearest_ancestor(&join(&segments)) {
                Some(ancestor) => segments = uri_segments(&ancestor)?,
                None => return Err(format!("already at {ROOT_URI}")),
            },
            other => segments.push(other.to_string()),
        }
    }
    Ok(segments)
}

/// Resolve `input` against the current location `base`, returning a target the
/// read path accepts.
///
/// `base` is a canonical URI or [`ROOT_URI`]. `input` may be absolute
/// (`cairn://…`), home-anchored (`cairn:~/…`, forwarded verbatim for the host to
/// resolve), root-absolute (`/posts`), or a plain segment path with `.` and `..`.
/// Any `?query` on the input rides through to the result unchanged, so the
/// scoping grammar a read already understands needs nothing new here.
pub fn resolve_relative(base: &str, input: &str) -> Result<String, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("empty navigation input".to_string());
    }
    // The host resolves home shorthand against the authenticated run; a client
    // that expanded it textually would answer with a stale home.
    if input == HOME_PREFIX || input.starts_with("cairn:~/") {
        return Ok(input.to_string());
    }

    let (identity, query) = match input.split_once('?') {
        Some((identity, query)) => (identity, Some(query)),
        None => (input, None),
    };

    let segments = if let Some(rest) = identity.strip_prefix(ROOT_URI) {
        walk(&[], rest)?
    } else if let Some(rest) = identity.strip_prefix('/') {
        walk(&[], rest)?
    } else if identity.contains(':') {
        return Err(format!("not a cairn target: {identity}"));
    } else {
        walk(&uri_segments(base)?, identity)?
    };

    let uri = join(&segments);
    if !segments.is_empty() && parse_uri(&uri).is_none() {
        return Err(format!("{uri} does not name a resource"));
    }
    Ok(match query {
        Some(query) if !query.is_empty() => format!("{uri}?{query}"),
        _ => uri,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The operator's sketch, keystroke for keystroke: each line is typed at the
    /// prompt the previous line left behind.
    #[test]
    fn the_operators_sketch_resolves_step_by_step() {
        let project = resolve_relative(ROOT_URI, "p/cairn").unwrap();
        assert_eq!(project, "cairn://p/cairn");
        let issue = resolve_relative(&project, "4279").unwrap();
        assert_eq!(issue, "cairn://p/cairn/4279");
        let node = resolve_relative(&issue, "1/builder").unwrap();
        assert_eq!(node, "cairn://p/cairn/4279/1/builder");
    }

    #[test]
    fn dot_dot_climbs_past_segment_prefixes_that_name_nothing() {
        // `cairn://p/cairn/4279/1` is not a resource, so a single pop would
        // strand the cursor; the climb lands on the issue.
        assert_eq!(
            resolve_relative("cairn://p/cairn/4279/1/builder", "..").unwrap(),
            "cairn://p/cairn/4279"
        );
        assert_eq!(
            resolve_relative("cairn://p/cairn/4279", "..").unwrap(),
            "cairn://p/cairn"
        );
        // `cairn://p` names nothing either, so the project climbs to the root.
        assert_eq!(resolve_relative("cairn://p/cairn", "..").unwrap(), ROOT_URI);
    }

    #[test]
    fn dot_dot_at_the_root_is_an_error_not_a_silent_no_op() {
        assert!(resolve_relative(ROOT_URI, "..").is_err());
    }

    #[test]
    fn dot_dot_composes_with_a_following_segment_path() {
        assert_eq!(
            resolve_relative("cairn://p/cairn/4279", "../4036").unwrap(),
            "cairn://p/cairn/4036"
        );
    }

    #[test]
    fn a_lone_dot_re_reads_the_current_location() {
        assert_eq!(
            resolve_relative("cairn://p/cairn/4279", ".").unwrap(),
            "cairn://p/cairn/4279"
        );
    }

    #[test]
    fn absolute_and_root_absolute_forms_ignore_the_cursor() {
        assert_eq!(
            resolve_relative("cairn://p/cairn/4279", "cairn://p/cairn/issues").unwrap(),
            "cairn://p/cairn/issues"
        );
        assert_eq!(
            resolve_relative("cairn://p/cairn/4279", "/posts").unwrap(),
            "cairn://posts"
        );
    }

    #[test]
    fn home_shorthand_is_forwarded_for_the_host_to_resolve() {
        assert_eq!(
            resolve_relative(ROOT_URI, "cairn:~/todos").unwrap(),
            "cairn:~/todos"
        );
    }

    #[test]
    fn a_query_string_rides_through_unchanged() {
        assert_eq!(
            resolve_relative("cairn://p/cairn", "4279?grep=Status").unwrap(),
            "cairn://p/cairn/4279?grep=Status"
        );
        assert_eq!(
            resolve_relative("cairn://p/cairn/4279", "..?limit=5").unwrap(),
            "cairn://p/cairn?limit=5"
        );
    }

    #[test]
    fn an_input_that_names_no_resource_is_rejected() {
        // The parser is the sole judge: the shell never decides validity itself,
        // which is also why a segment under a project resolves generously (it
        // may name a thread) while these do not resolve at all.
        let error = resolve_relative(ROOT_URI, "nope").unwrap_err();
        assert!(error.contains("does not name a resource"), "{error}");
        assert!(resolve_relative("cairn://p/cairn/4279", "zz/builder").is_err());
        assert!(resolve_relative("cairn://p/cairn/4279/1/builder", "a/b/c/d").is_err());
    }

    #[test]
    fn a_non_cairn_scheme_is_not_treated_as_a_segment_path() {
        assert!(resolve_relative(ROOT_URI, "file:src/lib.rs").is_err());
        assert!(resolve_relative(ROOT_URI, "https://example.com").is_err());
    }

    #[test]
    fn segments_round_trip_through_the_root() {
        assert_eq!(uri_segments(ROOT_URI).unwrap(), Vec::<String>::new());
        assert_eq!(
            uri_segments("cairn://p/cairn/4279?grep=x").unwrap(),
            ["p", "cairn", "4279"]
        );
        assert!(uri_segments("file:src/lib.rs").is_err());
        assert_eq!(nearest_ancestor(ROOT_URI), None);
    }
}
