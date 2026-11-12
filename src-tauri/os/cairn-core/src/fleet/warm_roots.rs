//! The runner's copy of one machine's warm roots: the commits whose complete
//! object closure the machine holds, by repository.
//!
//! Set whole from the link's Hello and moved by each `WarmRootsChanged` the
//! executor sends after it. Placement reads it twice: whether the machine
//! already holds the exact commit a request names, and which roots it can
//! offer as haves when a sync onto it is priced.

use std::sync::Arc;

use cairn_common::executor_protocol::{
    RepositoryIdentity, WarmRepositoryRoots, WarmRootChange, WarmRootInventory,
};

/// One machine's warm roots as of `revision`. Every repository's commits are
/// sorted and deduplicated, so membership is a binary search and the haves a
/// sync is priced with need no sorting.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct WarmRootSet {
    revision: u64,
    /// A handful of repositories per machine, so a scan beats a map. Each
    /// list is shared so a change to one repository copies only the others'
    /// pointers.
    repositories: Vec<(RepositoryIdentity, Arc<[String]>)>,
}

/// What applying one change did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WarmRootApply {
    Applied,
    /// The change is no newer than the set: a frame an earlier link left
    /// queued behind the Hello that superseded it.
    Stale,
    /// A delta against a revision this set is not at. The set is unchanged.
    BaseMismatch {
        held: u64,
        base: u64,
    },
}

impl WarmRootSet {
    pub(crate) fn from_inventory(inventory: WarmRootInventory) -> Self {
        let mut set = Self {
            revision: inventory.revision,
            repositories: Vec::new(),
        };
        set.replace(inventory.repositories);
        set
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    /// The roots this machine holds for `repository`, sorted.
    pub(crate) fn commits(&self, repository: &RepositoryIdentity) -> &[String] {
        self.repositories
            .iter()
            .find(|(held, _)| held == repository)
            .map_or(&[][..], |(_, commits)| commits)
    }

    pub(crate) fn holds(&self, repository: &RepositoryIdentity, commit: &str) -> bool {
        self.commits(repository)
            .binary_search_by(|root| root.as_str().cmp(commit))
            .is_ok()
    }

    /// Move the set to `revision` by `change`, or say why it did not move.
    pub(crate) fn apply(&mut self, revision: u64, change: WarmRootChange) -> WarmRootApply {
        if revision <= self.revision {
            return WarmRootApply::Stale;
        }
        match change {
            WarmRootChange::Whole { repositories } => self.replace(repositories),
            WarmRootChange::Delta {
                base_revision,
                added,
                removed,
            } => {
                if base_revision != self.revision {
                    return WarmRootApply::BaseMismatch {
                        held: self.revision,
                        base: base_revision,
                    };
                }
                for group in removed {
                    // The wire does not promise order, so sort before searching.
                    let mut gone = group.commits;
                    gone.sort_unstable();
                    self.edit(group.repository, |held| {
                        held.retain(|commit| gone.binary_search(commit).is_err())
                    });
                }
                for group in added {
                    let commits = group.commits;
                    self.edit(group.repository, |held| held.extend(commits));
                }
            }
        }
        self.revision = revision;
        WarmRootApply::Applied
    }

    fn replace(&mut self, repositories: Vec<WarmRepositoryRoots>) {
        self.repositories.clear();
        for group in repositories {
            let commits = group.commits;
            self.edit(group.repository, |held| held.extend(commits));
        }
    }

    /// Rewrite one repository's list through `edit`, keeping it sorted and
    /// deduplicated and dropping it once empty.
    fn edit(&mut self, repository: RepositoryIdentity, edit: impl FnOnce(&mut Vec<String>)) {
        let position = self
            .repositories
            .iter()
            .position(|(held, _)| *held == repository);
        let mut commits =
            position.map_or_else(Vec::new, |index| self.repositories[index].1.to_vec());
        edit(&mut commits);
        commits.sort_unstable();
        commits.dedup();
        match (position, commits.is_empty()) {
            (Some(index), true) => {
                self.repositories.swap_remove(index);
            }
            (Some(index), false) => self.repositories[index].1 = commits.into(),
            (None, true) => {}
            (None, false) => self.repositories.push((repository, commits.into())),
        }
    }
}
