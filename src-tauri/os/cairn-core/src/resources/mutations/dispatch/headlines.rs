//! The headlines `seen` patch, refused on this surface to every caller.
//!
//! Moving the operator's read cursor says what he has been shown, so it is
//! his alone. A resource write carries no verified operator credential, and a
//! request with no run identity is indistinguishable from an agent shell that
//! dropped its own, so, as with tenet verdicts, the desktop's authenticated
//! invoke path is the one writer: it calls
//! [`crate::threads::headlines::mark_seen`] with the principal it verified.

use super::super::{build_failure, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode};
use cairn_uri::CairnResource;

pub(super) fn dispatch(
    index: usize,
    item: &ChangeItem,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    match (resource, item.mode) {
        (CairnResource::ProjectHeadlines { .. }, ChangeMode::Patch) => Err(build_failure(
            index,
            item,
            crate::threads::headlines::OPERATOR_ONLY_REFUSAL,
        )),
        _ => Ok(None),
    }
}
