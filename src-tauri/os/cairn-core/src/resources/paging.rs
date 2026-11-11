//! Who interprets a resource read's `offset` and `limit`.
//!
//! The read grammar gives every target a line window: `offset`/`limit` count
//! rendered lines, and a read the budget cuts hands out a `continue:` footer
//! that advances `offset` by the lines it showed. Some resources define the
//! same two keys as their own paging instead: `vcs/log`'s `offset` skips
//! commits, `/issues`' skips issues. One key cannot mean two things on one URI.
//! A footer that writes a line offset into a URI whose resource reads it as a
//! record offset resumes that many records further on, and everything between
//! is never delivered.
//!
//! [`paging`] is the one answer to "whose key is it" for each resource kind.
//! The read router dispatches on it (a renderer's own keys reach the renderer,
//! never the view), and the parity test below pins it to what each resource's
//! contract advertises, so a resource cannot start declaring `offset` or
//! `limit` without deciding which of the two meanings it takes.

use cairn_uri::ResourceKind;

/// How a resource kind's `offset`/`limit` query keys are interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Paging {
    /// The resource declares neither key; both are the universal line window.
    Undeclared,
    /// The resource declares the keys and means the universal line window over
    /// its rendered body (a log tail, a text attachment, a browser page).
    Lines,
    /// The renderer pages its own records with both keys. The view never
    /// line-windows the body, and a page the budget cuts continues in records.
    Records,
    /// The renderer caps its page with `limit` and has no `offset` of its own,
    /// so `offset` stays the universal line window over the capped page.
    Limit,
}

impl Paging {
    /// Whether `offset` is the renderer's record offset rather than a line
    /// window. Such an `offset` composes with `grep` like any other filter.
    pub(crate) fn renderer_owns_offset(self) -> bool {
        self == Paging::Records
    }
}

/// Who interprets `offset`/`limit` on a read of `kind`.
pub(crate) fn paging(kind: ResourceKind) -> Paging {
    use ResourceKind as K;
    match kind {
        K::Requests
        | K::Rebuilds
        | K::Db
        | K::DbTable
        | K::DevDb
        | K::ThreadDb
        | K::ThreadDbTable
        | K::ProjectIssues
        | K::ProjectCheckResults
        | K::ProjectVcsLog
        | K::ProjectEvents
        | K::WorkspaceEvents
        | K::NodeEras
        | K::NodeRebase => Paging::Records,
        K::Project
        | K::Changed
        | K::Posts
        | K::ProjectPosts
        | K::ProjectCampPosts
        | K::HomeFeed
        | K::ProjectTenets
        | K::ProjectPages
        | K::NodePageLog
        | K::ProjectHome
        | K::ProjectMessages
        | K::ChannelsAmbientFeedback
        | K::IssueMessages
        | K::NodeMessages
        | K::TaskMessages
        | K::NodeProgress
        | K::ProjectServiceLog
        | K::ThreadServiceLog => Paging::Limit,
        K::Logs
        | K::ProjectAttachment
        | K::NodeChatEvent
        | K::TaskChatEvent
        | K::NodeBrowser
        | K::TaskBrowser
        | K::ProjectBrowser => Paging::Lines,
        _ => Paging::Undeclared,
    }
}

/// One page of records a renderer paged itself with its own `offset`/`limit`,
/// with the counts the view needs to continue it in records rather than lines.
pub(crate) struct RecordPage {
    pub body: String,
    /// Plural noun for the header and footer (`"commits"`, `"eras"`).
    pub noun: &'static str,
    /// Records skipped before this page.
    pub offset: usize,
    /// The page size the renderer applied: the requested `limit`, or its
    /// default when none was requested.
    pub limit: usize,
    /// Records this page carries.
    pub shown: usize,
    /// Records in the whole filtered result, when the renderer can count them.
    pub total: Option<usize>,
    /// When `total` is `None`: whether the renderer saw records past this
    /// page. Ignored when the total is known.
    pub more: bool,
    /// When every record renders as exactly one body line after a fixed
    /// prelude, how many lines that prelude is. This is what lets a page the
    /// budget cuts be recounted in records; without it a cut page claims no
    /// count and continues from its own start under a smaller `limit`.
    pub prelude_lines: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::contract::RESOURCE_CONTRACTS;

    /// The classification and the advertised contract say the same thing for
    /// every kind: a kind that declares neither key is unclassified, a record
    /// pager declares both, and a limit-capped renderer declares `limit` and
    /// not `offset`. A new resource that starts declaring either key fails here
    /// until it is classified, which is the moment its continuation footer's
    /// meaning gets decided.
    #[test]
    fn paging_classification_matches_every_contracts_declared_keys() {
        let mismatches: Vec<String> = RESOURCE_CONTRACTS
            .iter()
            .filter_map(|contract| {
                let declares = |key: &str| {
                    contract
                        .read_projections
                        .iter()
                        .any(|projection| projection.key == key)
                };
                let (offset, limit) = (declares("offset"), declares("limit"));
                let paging = paging(contract.kind);
                let consistent = match paging {
                    Paging::Undeclared => !offset && !limit,
                    Paging::Lines => offset || limit,
                    Paging::Records => offset && limit,
                    Paging::Limit => limit && !offset,
                };
                (!consistent).then(|| {
                    format!(
                        "{:?} is classified {paging:?} but its contract declares offset={offset}, limit={limit}",
                        contract.kind
                    )
                })
            })
            .collect();
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }
}
