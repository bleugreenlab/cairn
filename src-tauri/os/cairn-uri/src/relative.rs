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

/// The project-anchored shorthand, also resolved live by the host.
const PROJECT_PREFIX: &str = "cairn:@";

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
    if input == HOME_PREFIX
        || input.starts_with("cairn:~/")
        || input == PROJECT_PREFIX
        || input.starts_with(PROJECT_PREFIX)
    {
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
mod tests;
