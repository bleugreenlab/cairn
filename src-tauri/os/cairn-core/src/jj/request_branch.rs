//! The `request/` branch namespace: the one branch an accepted operator request
//! may own, for the length of one attempt.
//!
//! A request branch holds exactly one sealed commit whose parent is the tip of
//! the default branch it lands on. It is never prepared or advanced (base
//! preparation refuses the namespace by name), it lands only when that parent
//! is still the target's tip, and it ends one of two ways: retired once its
//! commit landed, or discarded unlanded, and then only when its head carries
//! the attempt's own trailer. No job and no merge request ever owns one.

use std::path::Path;

/// The namespace every operator request's branch lives under.
pub const REQUEST_BRANCH_PREFIX: &str = "request/";

/// The trailer naming the request attempt a sealed commit belongs to.
pub const REQUEST_ATTEMPT_TRAILER: &str = "Cairn-Request-Attempt";

/// Whether `branch` is in the request namespace.
pub fn is_request_branch(branch: &str) -> bool {
    branch.starts_with(REQUEST_BRANCH_PREFIX)
}

/// The branch request attempt `attempt_id` seals onto.
pub fn request_branch(attempt_id: &str) -> String {
    format!("{REQUEST_BRANCH_PREFIX}{attempt_id}")
}

/// The attempt a request branch belongs to: its non-empty suffix.
pub fn request_attempt_of(branch: &str) -> Option<&str> {
    branch
        .strip_prefix(REQUEST_BRANCH_PREFIX)
        .filter(|attempt| !attempt.is_empty())
}

/// The trailer line a request attempt's sealed commit carries.
pub fn request_attempt_trailer(attempt_id: &str) -> String {
    format!("{REQUEST_ATTEMPT_TRAILER}: {attempt_id}")
}

/// Whether `message` carries `attempt_id`'s trailer as a line of its own.
fn carries_attempt_trailer(message: &str, attempt_id: &str) -> bool {
    let trailer = request_attempt_trailer(attempt_id);
    message.lines().any(|line| line.trim() == trailer)
}

/// The full message of `commit`, read from the store's backing repository.
fn commit_message(store: &Path, commit: &str) -> Result<String, String> {
    let git_dir = super::store_git_dir(store)
        .ok_or_else(|| format!("resolve the backing repository of {}", store.display()))?;
    let output = crate::env::git()
        .arg(format!("--git-dir={}", git_dir.display()))
        .args(["show", "-s", "--format=%B", commit])
        .output()
        .map_err(|error| format!("read the message of {commit}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "read the message of {commit}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Remove a request branch whose commit never landed, if it still holds
/// exactly `expected_head`: the bookmark, and the git ref when it has one.
///
/// [`super::retire_bookmark_expecting`] alone retires a branch only as its
/// caller judged it, which for a landed branch is its landing. An unlanded
/// branch has no such proof, so this refuses everything but the one shape a
/// request attempt leaves: a name in [`REQUEST_BRANCH_PREFIX`] whose head
/// carries that attempt's own [`REQUEST_ATTEMPT_TRAILER`]. A branch that
/// moved since `expected_head` is kept and reported as refused. The caller
/// holds the branch reserved and the store lock.
pub fn discard_unlanded_bookmark_expecting(
    store: &Path,
    branch: &str,
    expected_head: &str,
) -> Result<super::Retirement, String> {
    let attempt = request_attempt_of(branch).ok_or_else(|| {
        format!(
            "refusing to discard `{branch}`: only a `{REQUEST_BRANCH_PREFIX}` branch is ever \
             discarded unlanded"
        )
    })?;
    let state = super::read_local_branch_state(store, branch)?;
    if state.bookmark.as_deref() != Some(expected_head)
        || state
            .git_ref
            .as_deref()
            .is_some_and(|git_ref| git_ref != expected_head)
    {
        return Ok(super::Retirement::Refused(format!(
            "`{branch}` is not at {expected_head} (bookmark {:?}, git ref {:?})",
            state.bookmark, state.git_ref
        )));
    }
    if !carries_attempt_trailer(&commit_message(store, expected_head)?, attempt) {
        return Err(format!(
            "refusing to discard `{branch}`: its head {expected_head} does not carry \
             `{}`, so it is not this attempt's sealed commit",
            request_attempt_trailer(attempt)
        ));
    }
    super::retire_bookmark_expecting(store, branch, &state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_namespace_names_its_attempt_and_nothing_else() {
        assert!(is_request_branch("request/att-1"));
        assert!(!is_request_branch("agent/request/att-1"));
        assert_eq!(request_attempt_of(&request_branch("att-1")), Some("att-1"));
        assert_eq!(request_attempt_of("request/"), None);
        assert_eq!(request_attempt_of("main"), None);
    }

    #[test]
    fn only_this_attempts_own_trailer_line_counts() {
        let message =
            "Bump version to 1.2.3\n\nCairn-Request-Act: abc\nCairn-Request-Attempt: att-1\n";
        assert!(carries_attempt_trailer(message, "att-1"));
        assert!(!carries_attempt_trailer(message, "att-12"));
        assert!(!carries_attempt_trailer(
            "mentions Cairn-Request-Attempt: att-1 inline",
            "att-1"
        ));
    }
}
