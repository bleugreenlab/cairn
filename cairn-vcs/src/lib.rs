//! Embedded version-control transactions quarantined from Cairn's core domain.
//!
//! The public boundary uses filesystem paths and hexadecimal object IDs only.
//! No jj-lib or gix type crosses into `cairn-core`, and this crate never imports
//! `cairn-core`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use futures::io::Cursor;
use futures::StreamExt as _;
use jj_lib::backend::{CommitId, CopyId, TreeValue};
use jj_lib::config::{ConfigLayer, ConfigSource, StackedConfig};
use jj_lib::local_working_copy::{LocalWorkingCopy, LocalWorkingCopyFactory};
use jj_lib::matchers::EverythingMatcher;
use jj_lib::merge::Merge;
use jj_lib::merged_tree_builder::MergedTreeBuilder;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::RefTarget;
use jj_lib::ref_name::{RefName, RemoteRefSymbol};
use jj_lib::repo::{ReadonlyRepo, Repo as _, StoreFactories};
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::revset::{SymbolResolver, SymbolResolverExtension};
use jj_lib::settings::UserSettings;
use jj_lib::workspace::{WorkingCopyFactories, Workspace};

/// Fallback identity for a transaction that carries no resolved project
/// identity.
///
/// This is the same identity the CLI-driven seal path writes into the managed jj
/// config, so a commit's provenance never depends on which jj driver produced
/// it. `cairn-core`'s `JjEnv` reads these constants rather than defining a
/// second copy.
pub const MANAGED_IDENTITY_NAME: &str = "Cairn Agent";
pub const MANAGED_IDENTITY_EMAIL: &str = "agent@cairn.local";

/// The identity stamped on every commit published through this crate, as BOTH
/// author and committer.
///
/// jj takes a new commit's author and committer, and a rewritten commit's
/// committer, straight from its `user.name`/`user.email` settings — which
/// `StackedConfig::with_defaults()` leaves as the EMPTY STRING, because jj's
/// built-in defaults expect a user config file layered on top. An empty
/// committer is not a valid Git signature: it is exported as the literal
/// `JJ_EMPTY_STRING` and `jj git push` refuses the commit outright ("Won't push
/// commit … since it has no author and/or committer set"), which makes the whole
/// branch unpushable. Every jj transaction here therefore layers a real identity
/// over those defaults instead of accepting them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationIdentity {
    pub name: String,
    pub email: String,
}

/// jj settings for one repository interaction: the built-in defaults with a real
/// identity layered over them.
///
/// Every `UserSettings` in this crate is built here, so the empty-identity
/// default can never be reintroduced by a new call path. A blank name or email
/// is coerced to the managed fallback rather than rejected — an unusable
/// identity must never cost an agent its sealed work, and the fallback is itself
/// a valid, pushable signature.
fn jj_settings(identity: Option<&PublicationIdentity>) -> Result<UserSettings, String> {
    let name = identity
        .map(|identity| identity.name.trim())
        .filter(|name| !name.is_empty())
        .unwrap_or(MANAGED_IDENTITY_NAME);
    let email = identity
        .map(|identity| identity.email.trim())
        .filter(|email| !email.is_empty())
        .unwrap_or(MANAGED_IDENTITY_EMAIL);
    let mut layer = ConfigLayer::empty(ConfigSource::Repo);
    for (key, value) in [
        ("user.name", name),
        ("user.email", email),
        // jj defaults the operation's user to the empty string too, which leaves
        // an embedded transaction anonymous in `jj op log` while every CLI
        // operation beside it names one — the difference that makes a
        // misattributed commit hard to trace back to the path that wrote it.
        ("operation.username", name),
    ] {
        layer
            .set_value(key, value)
            .map_err(|error| format!("set jj setting `{key}`: {error}"))?;
    }
    let mut config = StackedConfig::with_defaults();
    config.add_layer(layer);
    UserSettings::from_config(config).map_err(|error| format!("load jj settings: {error}"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinateResolutionError {
    Invalid(String),
    Absent {
        coordinate: String,
        diagnostic: String,
    },
    Ambiguous(String),
    /// The coordinate names a local bookmark that jj holds in its conflicted
    /// state: one name, several competing targets, because the local side and
    /// the backing git ref both moved off a common base.
    ///
    /// Distinguished from [`Self::Absent`] because it is REPAIRABLE and
    /// [`Self::Absent`] is not. jj reports it as an ordinary resolution failure
    /// (`Name \`main\` is conflicted`), which is how a repairable condition used
    /// to reach agents as "this branch does not exist" and strand them: every
    /// verb that resolved the name died, including the ones needed to diagnose
    /// it. Callers that can reach the store repair this and retry.
    Conflicted {
        coordinate: String,
        /// The competing commits, so a caller can choose among them (or a
        /// diagnostic can name them) without re-querying.
        targets: Vec<String>,
    },
    Repository(String),
}

/// One complete path override applied to the authoritative logical head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalTreeMutation {
    pub path: String,
    pub content: Option<Vec<u8>>,
}

/// Build and publish a tree without materializing it in a workspace. The path
/// mutations are ordered, so repeated paths have the same last-write-wins
/// semantics as an ordered MCP write batch.
pub fn publish_logical_mutations(
    repository_path: &Path,
    bookmark: &str,
    expected_head: &str,
    mutations: Vec<LogicalTreeMutation>,
    identity: Option<PublicationIdentity>,
    mode: PublicationMode,
) -> Result<LogicalHeadPublication, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("start logical-tree publication runtime: {error}"))?
        .block_on(publish_logical_head_inner(
            repository_path,
            bookmark,
            expected_head,
            ProposedTree::Mutations(mutations),
            identity,
            mode,
        ))
}

/// Revert one reachable, single-parent commit onto the current logical head.
///
/// The inverse is computed and published under the caller's canonical store
/// lock. Later edits on disjoint paths are preserved; a path changed to a third
/// value refuses the entire operation. Tree values are restored directly, so
/// file modes, symlinks, deletions, and binary contents retain their exact store
/// representation.
pub fn publish_logical_revert(
    repository_path: &Path,
    bookmark: &str,
    expected_head: &str,
    commit: &str,
    identity: Option<PublicationIdentity>,
    description: String,
) -> Result<LogicalHeadPublication, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("start logical-revert publication runtime: {error}"))?
        .block_on(publish_logical_head_inner(
            repository_path,
            bookmark,
            expected_head,
            ProposedTree::Revert(commit.to_string()),
            identity,
            PublicationMode::Child { description },
        ))
}

enum ProposedTree {
    DeltaCommit(String),
    Mutations(Vec<LogicalTreeMutation>),
    Revert(String),
}

/// Atomically publish a complete proposed tree at one runner-owned logical
/// bookmark. The caller serializes repository writers with Cairn's canonical
/// store lock; this boundary reloads the jj operation head and compares the
/// durable bookmark before writing any visible history.
pub fn publish_logical_head(
    repository_path: &Path,
    bookmark: &str,
    expected_head: &str,
    delta_commit: &str,
    identity: Option<PublicationIdentity>,
    mode: PublicationMode,
) -> Result<LogicalHeadPublication, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("start logical-head publication runtime: {error}"))?
        .block_on(publish_logical_head_inner(
            repository_path,
            bookmark,
            expected_head,
            ProposedTree::DeltaCommit(delta_commit.to_string()),
            identity,
            mode,
        ))
}

async fn publish_logical_head_inner(
    repository_path: &Path,
    bookmark: &str,
    expected_head: &str,
    proposed_tree: ProposedTree,
    identity: Option<PublicationIdentity>,
    mode: PublicationMode,
) -> Result<LogicalHeadPublication, String> {
    if bookmark.trim().is_empty() {
        return Err("logical-head bookmark must not be empty".to_string());
    }

    let expected_id = CommitId::try_from_hex(expected_head)
        .ok_or_else(|| "expected logical head is not a full hexadecimal object ID".to_string())?;
    let settings = jj_settings(identity.as_ref())?;
    let stores = StoreFactories::default();
    let mut working_copies: WorkingCopyFactories = HashMap::new();
    working_copies.insert(
        LocalWorkingCopy::name().to_string(),
        Box::new(LocalWorkingCopyFactory {}),
    );
    let workspace = Workspace::load(&settings, repository_path, &stores, &working_copies)
        .map_err(|error| format!("load logical-head repository: {error}"))?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .await
        .map_err(|error| format!("load logical-head operation: {error}"))?;
    let bookmark_name = RefName::new(bookmark);
    let target = repo.view().get_local_bookmark(bookmark_name);
    if target.has_conflict() {
        return Err(format!(
            "logical-head conflict: bookmark `{bookmark}` is conflicted"
        ));
    }
    let actual = target
        .as_normal()
        .ok_or_else(|| format!("logical-head conflict: bookmark `{bookmark}` is absent"))?;
    if actual != &expected_id {
        return Err(format!(
            "logical-head conflict: bookmark `{bookmark}` changed from {expected_head} to {}",
            actual.hex()
        ));
    }
    let head = repo
        .store()
        .get_commit(&expected_id)
        .map_err(|error| format!("read expected logical head: {error}"))?;
    let (tree, affected_paths) = match proposed_tree {
        ProposedTree::DeltaCommit(delta_commit) => {
            let delta_id = CommitId::try_from_hex(&delta_commit)
                .ok_or_else(|| "proposed delta is not a full hexadecimal object ID".to_string())?;
            let delta = repo
                .store()
                .get_commit(&delta_id)
                .map_err(|error| format!("read proposed logical-head tree: {error}"))?;
            if delta.parent_ids() != [expected_id.clone()] {
                return Err(format!(
                    "logical-head delta parent mismatch: expected {expected_head}, got {}",
                    delta
                        .parent_ids()
                        .iter()
                        .map(|id| id.hex())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            (delta.tree(), Vec::new())
        }
        ProposedTree::Mutations(mutations) => {
            let base_tree = head.tree();
            let mut builder = MergedTreeBuilder::new(base_tree.clone());
            for mutation in mutations {
                let path = RepoPathBuf::from_internal_string(mutation.path.clone())
                    .map_err(|error| format!("invalid logical-tree path: {error}"))?;
                if path.as_internal_file_string().is_empty() {
                    return Err("logical-tree mutation path must not be empty".to_string());
                }
                let value =
                    match mutation.content {
                        None => Merge::absent(),
                        Some(content) => {
                            let current = base_tree.path_value(&path).await.map_err(|error| {
                                format!("read logical-tree path `{}`: {error}", mutation.path)
                            })?;
                            let (executable, copy_id) = match current.as_resolved() {
                                Some(Some(TreeValue::File {
                                    executable,
                                    copy_id,
                                    ..
                                })) => (*executable, copy_id.clone()),
                                Some(None) => (false, CopyId::placeholder()),
                                Some(Some(_)) => {
                                    return Err(format!(
                                        "logical-tree path `{}` is not a regular file",
                                        mutation.path
                                    ));
                                }
                                // A complete file replacement is also an explicit
                                // conflict resolution. There is no single prior
                                // mode/copy identity to preserve, so use regular
                                // file defaults just as a newly created path does.
                                None => (false, CopyId::placeholder()),
                            };
                            let mut reader = Cursor::new(content);
                            let id = repo.store().write_file(&path, &mut reader).await.map_err(
                                |error| {
                                    format!("write logical-tree file `{}`: {error}", mutation.path)
                                },
                            )?;
                            Merge::resolved(Some(TreeValue::File {
                                id,
                                executable,
                                copy_id,
                            }))
                        }
                    };
                builder.set_or_remove(path, value);
            }
            let tree = builder
                .write_tree()
                .await
                .map_err(|error| format!("write proposed logical tree: {error}"))?;
            (tree, Vec::new())
        }
        ProposedTree::Revert(commit) => {
            if commit.len() != expected_id.hex().len() {
                return Err("revert commit is not a full hexadecimal object ID".to_string());
            }
            let commit_id = CommitId::try_from_hex(&commit)
                .ok_or_else(|| "revert commit is not a full hexadecimal object ID".to_string())?;
            let selected = repo
                .store()
                .get_commit(&commit_id)
                .map_err(|error| format!("read revert commit `{commit}`: {error}"))?;
            if !repo
                .index()
                .is_ancestor(&commit_id, &expected_id)
                .map_err(|error| format!("check revert commit reachability: {error}"))?
            {
                return Err(format!(
                    "revert commit `{commit}` is not reachable from logical head `{expected_head}`"
                ));
            }
            let [parent_id] = selected.parent_ids() else {
                return Err(format!(
                    "revert commit `{commit}` must have exactly one parent (found {})",
                    selected.parent_ids().len()
                ));
            };
            if selected.has_conflict() {
                return Err(format!("revert commit `{commit}` has tree conflicts"));
            }
            let parent = repo
                .store()
                .get_commit(parent_id)
                .map_err(|error| format!("read revert commit parent: {error}"))?;
            if parent.has_conflict() || head.has_conflict() {
                return Err(
                    "logical revert requires conflict-free parent, selected, and head trees"
                        .to_string(),
                );
            }

            let parent_tree = parent.tree();
            let selected_tree = selected.tree();
            let head_tree = head.tree();
            let mut builder = MergedTreeBuilder::new(head_tree.clone());
            let mut changed_paths = Vec::new();
            let mut overlapping_paths = Vec::new();
            let mut diff = parent_tree.diff_stream(&selected_tree, &EverythingMatcher);
            while let Some(entry) = diff.next().await {
                let values = entry
                    .values
                    .map_err(|error| format!("read revert tree difference: {error}"))?;
                let current = head_tree.path_value(&entry.path).await.map_err(|error| {
                    format!(
                        "read current value for revert path `{}`: {error}",
                        entry.path.as_internal_file_string()
                    )
                })?;
                if current == values.after {
                    builder.set_or_remove(entry.path.clone(), values.before);
                    changed_paths.push(entry.path.into_internal_string());
                } else if current != values.before {
                    overlapping_paths.push(entry.path.into_internal_string());
                }
            }
            overlapping_paths.sort();
            if !overlapping_paths.is_empty() {
                return Err(format!(
                    "logical revert overlaps later edits at: {}",
                    overlapping_paths.join(", ")
                ));
            }
            if changed_paths.is_empty() {
                return Err(format!(
                    "revert commit `{commit}` is already unapplied from logical head `{expected_head}`"
                ));
            }
            changed_paths.sort();
            let tree = builder
                .write_tree()
                .await
                .map_err(|error| format!("write proposed revert tree: {error}"))?;
            (tree, changed_paths)
        }
    };

    let mut amend_note = None;
    let mut tx = repo.start_transaction();
    let mut rewrote_head = false;
    let published = match mode {
        // Both signatures come from `settings` (jj's commit builder stamps the
        // author and committer of a new commit from the same identity), so no
        // per-field override is needed here.
        PublicationMode::Child { description } => tx
            .repo_mut()
            .new_commit(vec![expected_id.clone()], tree.clone())
            .set_description(description)
            .write()
            .await
            .map_err(|error| format!("write logical-head child commit: {error}"))?,
        PublicationMode::Amend => {
            let foreign = repo
                .view()
                .local_bookmarks_for_commit(&expected_id)
                .filter(|(name, target)| {
                    *name != bookmark_name && target.as_normal() == Some(&expected_id)
                })
                .map(|(name, _)| name.as_str().to_string())
                .collect::<Vec<_>>();
            if foreign.is_empty() {
                rewrote_head = true;
                tx.repo_mut()
                    .rewrite_commit(&head)
                    .set_tree(tree.clone())
                    .write()
                    .await
                    .map_err(|error| format!("rewrite logical-head commit: {error}"))?
            } else {
                let description = if head.description().trim().is_empty() {
                    "amend".to_string()
                } else {
                    head.description().to_string()
                };
                amend_note = Some(format!(
                    "amend converted to a new commit: the previous commit is shared with {}",
                    foreign.join(", ")
                ));
                tx.repo_mut()
                    .new_commit(vec![expected_id.clone()], tree)
                    .set_description(description)
                    .write()
                    .await
                    .map_err(|error| format!("write guarded logical-head amend: {error}"))?
            }
        }
    };
    if rewrote_head {
        tx.repo_mut()
            .rebase_descendants()
            .await
            .map_err(|error| format!("rebase logical-head descendants after amend: {error}"))?;
    }
    tx.repo_mut()
        .set_local_bookmark_target(bookmark_name, RefTarget::normal(published.id().clone()));
    tx.commit(format!("publish logical head `{bookmark}`"))
        .await
        .map_err(|error| format!("commit logical-head publication transaction: {error}"))?;
    Ok(LogicalHeadPublication {
        head: published.id().hex(),
        change_id: published.change_id().to_string(),
        amend_note,
        affected_paths,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationMode {
    Child {
        description: String,
    },
    /// Rewrite the bookmark's own commit in place. jj preserves a rewritten
    /// commit's author and re-stamps only its committer, so the identity still
    /// has to be supplied for this mode.
    Amend,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalHeadPublication {
    pub head: String,
    pub change_id: String,
    pub amend_note: Option<String>,
    /// Paths whose exact prior tree values were restored by a revert.
    /// Empty for ordinary delta and mutation publication.
    pub affected_paths: Vec<String>,
}

impl std::fmt::Display for CoordinateResolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(value) => write!(f, "invalid revision coordinate {value:?}"),
            Self::Absent {
                coordinate,
                diagnostic,
            } => {
                write!(
                    f,
                    "revision coordinate {coordinate:?} did not resolve: {diagnostic}"
                )
            }
            Self::Ambiguous(value) => write!(f, "revision coordinate {value:?} is ambiguous"),
            Self::Conflicted {
                coordinate,
                targets,
            } => write!(
                f,
                "revision coordinate {coordinate:?} has several competing targets ({})",
                targets.join(", ")
            ),
            Self::Repository(diagnostic) => write!(f, "load jj repository: {diagnostic}"),
        }
    }
}

impl std::error::Error for CoordinateResolutionError {}

/// Resolve one user coordinate against the repository's current operation head.
///
/// SymbolResolver implements jj's native exact local/remote bookmark and
/// unambiguous commit/change-ID prefix semantics. This path performs no command
/// execution and returns no jj-lib type.
pub async fn resolve_coordinate(
    repository_path: &Path,
    coordinate: &str,
) -> Result<String, CoordinateResolutionError> {
    let coordinate = coordinate.trim();
    if coordinate.is_empty() {
        return Err(CoordinateResolutionError::Invalid(coordinate.to_string()));
    }
    // jj-lib futures are not Send, so the resolution runs to completion on a
    // dedicated thread; the future this function returns stays Send.
    let repository_path = repository_path.to_path_buf();
    let coordinate = coordinate.to_string();
    tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| CoordinateResolutionError::Repository(error.to_string()))?
            .block_on(resolve_coordinate_inner(&repository_path, &coordinate))
    })
    .await
    .map_err(|error| CoordinateResolutionError::Repository(error.to_string()))?
}

async fn resolve_coordinate_inner(
    repository_path: &Path,
    coordinate: &str,
) -> Result<String, CoordinateResolutionError> {
    let repo = load_repo_at_head(repository_path).await?;
    resolve_symbol_at(&repo, coordinate).map(|id| id.hex())
}

/// Load a repository at its current operation head for read-only inspection.
///
/// Read-only, but built through the same settings seam as every mutating path
/// here so no call site in this crate loads jj's bare defaults.
async fn load_repo_at_head(
    repository_path: &Path,
) -> Result<Arc<ReadonlyRepo>, CoordinateResolutionError> {
    let settings = jj_settings(None).map_err(CoordinateResolutionError::Repository)?;
    let stores = StoreFactories::default();
    let mut working_copies: WorkingCopyFactories = HashMap::new();
    working_copies.insert(
        LocalWorkingCopy::name().to_string(),
        Box::new(LocalWorkingCopyFactory {}),
    );
    let workspace = Workspace::load(&settings, repository_path, &stores, &working_copies)
        .map_err(|error| CoordinateResolutionError::Repository(error.to_string()))?;
    workspace
        .repo_loader()
        .load_at_head()
        .await
        .map_err(|error| CoordinateResolutionError::Repository(error.to_string()))
}

/// Resolve one user coordinate against an already-loaded repository.
///
/// `name@remote` addresses a remote bookmark directly; everything else goes
/// through jj's own [`SymbolResolver`], which implements exact local bookmark
/// and unambiguous commit/change-ID prefix semantics.
///
/// A conflicted local bookmark name is probed structurally BEFORE the symbol
/// resolver runs, because the resolver reports it as a plain failure and the
/// difference matters: absence is final, a conflicted name is repairable.
fn resolve_symbol_at(
    repo: &ReadonlyRepo,
    coordinate: &str,
) -> Result<CommitId, CoordinateResolutionError> {
    if let Some(conflicted) = conflicted_local_bookmark(repo, coordinate) {
        return Err(conflicted);
    }
    if let Some((name, remote)) = coordinate.rsplit_once('@') {
        if !name.is_empty() && !remote.is_empty() {
            let remote_ref = repo.view().get_remote_bookmark(RemoteRefSymbol {
                name: name.as_ref(),
                remote: remote.as_ref(),
            });
            return match remote_ref.target.as_resolved() {
                Some(Some(id)) => Ok(id.clone()),
                Some(None) => Err(CoordinateResolutionError::Absent {
                    coordinate: coordinate.to_string(),
                    diagnostic: "remote bookmark is absent".to_string(),
                }),
                None => Err(CoordinateResolutionError::Ambiguous(coordinate.to_string())),
            };
        }
    }
    let extensions: &[Box<dyn SymbolResolverExtension>] = &[];
    let resolver = SymbolResolver::new(repo, extensions);
    resolver.resolve_symbol(repo, coordinate).map_err(|error| {
        let diagnostic = error.to_string();
        if diagnostic.to_ascii_lowercase().contains("ambiguous") {
            CoordinateResolutionError::Ambiguous(coordinate.to_string())
        } else {
            CoordinateResolutionError::Absent {
                coordinate: coordinate.to_string(),
                diagnostic,
            }
        }
    })
}

/// The conflicted-name error for `coordinate`, when it names a local bookmark
/// jj holds in that state.
///
/// The probe is a view lookup rather than an error-string match, so it stays
/// true across jj releases and cannot be confused with an unrelated failure that
/// happens to mention the word.
fn conflicted_local_bookmark(
    repo: &ReadonlyRepo,
    coordinate: &str,
) -> Option<CoordinateResolutionError> {
    let target = repo.view().get_local_bookmark(RefName::new(coordinate));
    if !target.has_conflict() {
        return None;
    }
    let mut targets = target.added_ids().map(|id| id.hex()).collect::<Vec<_>>();
    targets.sort();
    Some(CoordinateResolutionError::Conflicted {
        coordinate: coordinate.to_string(),
        targets,
    })
}

/// Both endpoints of a merge-base query plus their fork point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeBase {
    /// Resolved commit id of the left coordinate.
    pub left: String,
    /// Resolved commit id of the right coordinate.
    pub right: String,
    /// Best common ancestor of `left` and `right`.
    pub base: String,
}

/// Resolve two coordinates and their merge base in one repository load.
///
/// This is the canonical fork-point primitive. A branch's OWN work is the range
/// `base..left` — what `git diff right...left` renders — so any surface that
/// reports what a branch changed relative to its integration target computes
/// both endpoints here at read time rather than trusting a coordinate recorded
/// when the branch was cut. A recorded fork point goes stale the moment the
/// target advances beneath a live branch; the merge base does not.
///
/// Resolution walks jj's commit index, so it costs an operation-head load and no
/// subprocess. Criss-cross histories can have several best common ancestors; the
/// lowest hex id wins so a rendered diff is stable across reads instead of
/// varying with index iteration order.
pub async fn merge_base(
    repository_path: &Path,
    left: &str,
    right: &str,
) -> Result<MergeBase, CoordinateResolutionError> {
    let left = left.trim().to_string();
    let right = right.trim().to_string();
    if left.is_empty() {
        return Err(CoordinateResolutionError::Invalid(left));
    }
    if right.is_empty() {
        return Err(CoordinateResolutionError::Invalid(right));
    }
    // jj-lib futures are not Send, so the query runs to completion on a
    // dedicated thread; the future this function returns stays Send.
    let repository_path = repository_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| CoordinateResolutionError::Repository(error.to_string()))?
            .block_on(merge_base_inner(&repository_path, &left, &right))
    })
    .await
    .map_err(|error| CoordinateResolutionError::Repository(error.to_string()))?
}

async fn merge_base_inner(
    repository_path: &Path,
    left: &str,
    right: &str,
) -> Result<MergeBase, CoordinateResolutionError> {
    let repo = load_repo_at_head(repository_path).await?;
    let left_id = resolve_symbol_at(&repo, left)?;
    let right_id = resolve_symbol_at(&repo, right)?;
    let mut bases = repo
        .index()
        .common_ancestors(
            std::slice::from_ref(&left_id),
            std::slice::from_ref(&right_id),
        )
        .map_err(|error| CoordinateResolutionError::Repository(error.to_string()))?;
    // jj's virtual root commit is the ancestor of every commit in the index, so
    // genuinely unrelated histories "share" it. It is not a real Git object and
    // cannot be diffed against; treat it as no fork point at all rather than
    // handing a caller a coordinate that resolves nowhere.
    let root = repo.store().root_commit_id().clone();
    bases.retain(|id| *id != root);
    bases.sort_by_key(|id| id.hex());
    let base = bases
        .into_iter()
        .next()
        .ok_or_else(|| CoordinateResolutionError::Absent {
            coordinate: format!("{right}...{left}"),
            diagnostic: "the two coordinates share no common ancestor".to_string(),
        })?;
    Ok(MergeBase {
        left: left_id.hex(),
        right: right_id.hex(),
        base: base.hex(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::process::Command;

    fn command(program: &str, args: &[&str]) -> String {
        let output = Command::new(program).args(args).output().unwrap();
        assert!(
            output.status.success(),
            "{program} {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    fn fixture() -> (tempfile::TempDir, String, String, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        command("git", &["-C", path, "init", "-q"]);
        command(
            "git",
            &["-C", path, "config", "user.email", "test@cairn.local"],
        );
        command("git", &["-C", path, "config", "user.name", "Cairn Test"]);
        for index in 0..20 {
            std::fs::write(dir.path().join("value"), index.to_string()).unwrap();
            command("git", &["-C", path, "add", "value"]);
            command(
                "git",
                &["-C", path, "commit", "-qm", &format!("commit {index}")],
            );
        }
        command("git", &["-C", path, "remote", "add", "origin", path]);
        command(
            "git",
            &[
                "-C",
                path,
                "update-ref",
                "refs/remotes/origin/remote",
                "HEAD",
            ],
        );
        command("jj", &["git", "init", "--colocate", path]);
        command("jj", &["-R", path, "git", "fetch", "--remote", "origin"]);
        command(
            "jj",
            &["-R", path, "bookmark", "create", "feature", "-r", "@"],
        );
        let commit = command(
            "jj",
            &[
                "-R",
                path,
                "log",
                "-r",
                "feature",
                "--no-graph",
                "-T",
                "commit_id",
            ],
        );
        let change = command(
            "jj",
            &[
                "-R",
                path,
                "log",
                "-r",
                "feature",
                "--no-graph",
                "-T",
                "change_id",
            ],
        );
        let ids = command("git", &["-C", path, "rev-list", "--all"]);
        let mut ambiguous = None;
        for width in 1..=2 {
            let mut prefixes = std::collections::HashSet::new();
            for id in ids.lines() {
                let prefix = &id[..width];
                if !prefixes.insert(prefix.to_string()) {
                    ambiguous = Some(prefix.to_string());
                    break;
                }
            }
            if ambiguous.is_some() {
                break;
            }
        }
        (
            dir,
            commit,
            change,
            ambiguous.expect("fixture has an ambiguous commit prefix"),
        )
    }

    #[test]
    fn logical_head_child_is_atomic_and_does_not_move_the_working_copy() {
        let (dir, expected, old_change, _) = fixture();
        let delta = delta_commit(dir.path(), &expected, "logical child\n");
        let before_bytes = std::fs::read(dir.path().join("value")).unwrap();
        let result = publish_logical_head(
            dir.path(),
            "feature",
            &expected,
            &delta,
            Some(PublicationIdentity {
                name: "Logical Author".into(),
                email: "logical@cairn.local".into(),
            }),
            PublicationMode::Child {
                description: "logical child".into(),
            },
        )
        .unwrap();
        assert_ne!(result.head, expected);
        assert_ne!(result.change_id, old_change);
        assert_eq!(
            command(
                "jj",
                &[
                    "-R",
                    dir.path().to_str().unwrap(),
                    "log",
                    "-r",
                    "feature",
                    "--no-graph",
                    "-T",
                    "commit_id",
                    "--ignore-working-copy",
                ],
            ),
            result.head
        );
        // The COMMITTER is asserted alongside the author: jj-lib's built-in
        // defaults leave `user.name`/`user.email` empty, and a commit with an
        // empty committer is rendered as `JJ_EMPTY_STRING` on git export and
        // refused outright by `jj git push` ("no author and/or committer set"),
        // making the whole branch unpushable.
        assert_eq!(
            command(
                "git",
                &[
                    "-C",
                    dir.path().to_str().unwrap(),
                    "show",
                    "-s",
                    "--format=%P|%an|%ae|%cn|%ce",
                    &result.head
                ]
            ),
            format!(
                "{expected}|Logical Author|logical@cairn.local|Logical Author|logical@cairn.local"
            )
        );
        assert_eq!(
            std::fs::read(dir.path().join("value")).unwrap(),
            before_bytes
        );
        let stale = publish_logical_head(
            dir.path(),
            "feature",
            &expected,
            &delta,
            None,
            PublicationMode::Amend,
        )
        .unwrap_err();
        assert!(stale.contains("changed from"));
    }

    fn bookmark_commit(repository: &Path) -> String {
        command(
            "jj",
            &[
                "-R",
                repository.to_str().unwrap(),
                "log",
                "-r",
                "feature",
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
        )
    }

    /// A batch that straddles a base advance seals its delta against the base
    /// the runner declared, and by publication time the bookmark has moved past
    /// it. Neither the publication nor the obvious retry may launder that delta
    /// onto the new head: the first is refused because the bookmark is not what
    /// the delta was built against, and the retry that merely updates its
    /// expectation is refused because the delta is not a child of what it would
    /// be published onto. Either refusal leaves the bookmark exactly where the
    /// advance put it.
    #[test]
    fn a_delta_sealed_against_a_moved_base_is_refused_and_leaves_the_bookmark_alone() {
        let (dir, base, _, _) = fixture();
        let straddler = delta_commit(dir.path(), &base, "the straddling batch\n");

        // The advance lands on the branch while that batch is still running.
        let advance = delta_commit(dir.path(), &base, "a teammate's landed commit\n");
        let advanced = publish_logical_head(
            dir.path(),
            "feature",
            &base,
            &advance,
            None,
            PublicationMode::Child {
                description: "the advance".into(),
            },
        )
        .unwrap();
        assert_ne!(advanced.head, base);

        let stale = publish_logical_head(
            dir.path(),
            "feature",
            &base,
            &straddler,
            None,
            PublicationMode::Child {
                description: "the straddling batch".into(),
            },
        )
        .unwrap_err();
        assert!(stale.contains("changed from"), "{stale}");

        let retried = publish_logical_head(
            dir.path(),
            "feature",
            &advanced.head,
            &straddler,
            None,
            PublicationMode::Child {
                description: "the straddling batch".into(),
            },
        )
        .unwrap_err();
        assert!(retried.contains("delta parent mismatch"), "{retried}");

        assert_eq!(bookmark_commit(dir.path()), advanced.head);
    }

    #[test]
    fn logical_tree_mutations_publish_without_materializing() {
        let (dir, expected, _, _) = fixture();
        let before_bytes = std::fs::read(dir.path().join("value")).unwrap();
        let result = publish_logical_mutations(
            dir.path(),
            "feature",
            &expected,
            vec![
                LogicalTreeMutation {
                    path: "value".into(),
                    content: Some(b"tree native\n".to_vec()),
                },
                LogicalTreeMutation {
                    path: "created".into(),
                    content: Some(b"new\n".to_vec()),
                },
            ],
            None,
            PublicationMode::Child {
                description: "tree mutation".into(),
            },
        )
        .unwrap();
        // No resolved project identity: the commit still carries the managed
        // fallback on BOTH signatures rather than jj's empty default.
        assert_eq!(
            command(
                "git",
                &[
                    "-C",
                    dir.path().to_str().unwrap(),
                    "show",
                    "-s",
                    "--format=%an|%ae|%cn|%ce",
                    &result.head,
                ],
            ),
            format!(
                "{MANAGED_IDENTITY_NAME}|{MANAGED_IDENTITY_EMAIL}|\
                 {MANAGED_IDENTITY_NAME}|{MANAGED_IDENTITY_EMAIL}"
            )
        );
        assert_eq!(
            command(
                "git",
                &[
                    "-C",
                    dir.path().to_str().unwrap(),
                    "show",
                    &format!("{}:value", result.head),
                ],
            ),
            "tree native"
        );
        assert_eq!(
            command(
                "git",
                &[
                    "-C",
                    dir.path().to_str().unwrap(),
                    "show",
                    &format!("{}:created", result.head),
                ],
            ),
            "new"
        );
        assert_eq!(
            std::fs::read(dir.path().join("value")).unwrap(),
            before_bytes
        );
        assert!(!dir.path().join("created").exists());
    }

    fn publish_files(
        repository: &Path,
        expected_head: &str,
        files: &[(&str, Option<&[u8]>)],
        description: &str,
    ) -> LogicalHeadPublication {
        publish_logical_mutations(
            repository,
            "feature",
            expected_head,
            files
                .iter()
                .map(|(path, content)| LogicalTreeMutation {
                    path: (*path).to_string(),
                    content: content.map(<[u8]>::to_vec),
                })
                .collect(),
            None,
            PublicationMode::Child {
                description: description.to_string(),
            },
        )
        .unwrap()
    }

    fn commit_file(repository: &Path, commit: &str, path: &str) -> String {
        command(
            "git",
            &[
                "-C",
                repository.to_str().unwrap(),
                "show",
                &format!("{commit}:{path}"),
            ],
        )
    }

    #[test]
    fn logical_revert_round_trips_and_preserves_disjoint_later_work() {
        let (dir, base, _, _) = fixture();
        let a = publish_files(
            dir.path(),
            &base,
            &[("value", Some(b"from a\n")), ("added-by-a", Some(b"a\n"))],
            "A",
        );
        let b = publish_files(dir.path(), &a.head, &[("disjoint", Some(b"from b\n"))], "B");

        let reverted = publish_logical_revert(
            dir.path(),
            "feature",
            &b.head,
            &a.head,
            None,
            "revert A".into(),
        )
        .unwrap();
        assert_eq!(reverted.affected_paths, ["added-by-a", "value"]);
        assert_eq!(commit_file(dir.path(), &reverted.head, "value"), "19");
        assert_eq!(
            commit_file(dir.path(), &reverted.head, "disjoint"),
            "from b"
        );
        let missing = Command::new("git")
            .args([
                "-C",
                dir.path().to_str().unwrap(),
                "cat-file",
                "-e",
                &format!("{}:added-by-a", reverted.head),
            ])
            .status()
            .unwrap();
        assert!(!missing.success());
        assert_eq!(
            command(
                "git",
                &[
                    "-C",
                    dir.path().to_str().unwrap(),
                    "show",
                    "-s",
                    "--format=%P",
                    &reverted.head,
                ],
            ),
            b.head
        );

        let restored = publish_logical_revert(
            dir.path(),
            "feature",
            &reverted.head,
            &reverted.head,
            None,
            "revert the revert".into(),
        )
        .unwrap();
        assert_eq!(commit_file(dir.path(), &restored.head, "value"), "from a");
        assert_eq!(commit_file(dir.path(), &restored.head, "added-by-a"), "a");
        assert_eq!(
            commit_file(dir.path(), &restored.head, "disjoint"),
            "from b"
        );
    }

    #[test]
    fn logical_revert_refuses_sorted_overlaps_without_moving_the_bookmark() {
        let (dir, base, _, _) = fixture();
        let a = publish_files(
            dir.path(),
            &base,
            &[("value", Some(b"a\n")), ("z-path", Some(b"a\n"))],
            "A",
        );
        let b = publish_files(
            dir.path(),
            &a.head,
            &[("z-path", Some(b"b\n")), ("value", Some(b"b\n"))],
            "B",
        );

        let error = publish_logical_revert(
            dir.path(),
            "feature",
            &b.head,
            &a.head,
            None,
            "revert A".into(),
        )
        .unwrap_err();
        assert_eq!(
            error,
            "logical revert overlaps later edits at: value, z-path"
        );
        assert_eq!(bookmark_commit(dir.path()), b.head);
        assert_eq!(commit_file(dir.path(), &b.head, "value"), "b");
        assert_eq!(commit_file(dir.path(), &b.head, "z-path"), "b");
    }

    #[test]
    fn logical_revert_refuses_invalid_stale_and_already_unapplied_requests() {
        let (dir, base, _, _) = fixture();
        let a = publish_files(dir.path(), &base, &[("value", Some(b"a\n"))], "A");
        let malformed =
            publish_logical_revert(dir.path(), "feature", &a.head, "abc", None, "bad".into())
                .unwrap_err();
        assert_eq!(
            malformed,
            "revert commit is not a full hexadecimal object ID"
        );

        let reverted = publish_logical_revert(
            dir.path(),
            "feature",
            &a.head,
            &a.head,
            None,
            "revert A".into(),
        )
        .unwrap();
        let already = publish_logical_revert(
            dir.path(),
            "feature",
            &reverted.head,
            &a.head,
            None,
            "revert A again".into(),
        )
        .unwrap_err();
        assert!(already.contains("already unapplied"), "{already}");

        let stale = publish_logical_revert(
            dir.path(),
            "feature",
            &a.head,
            &a.head,
            None,
            "stale revert".into(),
        )
        .unwrap_err();
        assert!(stale.contains("changed from"), "{stale}");
        assert_eq!(bookmark_commit(dir.path()), reverted.head);
    }

    #[test]
    fn logical_head_amend_preserves_change_id_and_foreign_guard_creates_child() {
        let (dir, expected, expected_change, _) = fixture();
        let delta = delta_commit(dir.path(), &expected, "amended\n");
        let child_before = delta_commit(dir.path(), &expected, "stacked child\n");
        command(
            "git",
            &[
                "-C",
                dir.path().to_str().unwrap(),
                "update-ref",
                "refs/heads/child-seed",
                &child_before,
            ],
        );
        command(
            "jj",
            &[
                "-R",
                dir.path().to_str().unwrap(),
                "git",
                "import",
                "--ignore-working-copy",
            ],
        );
        let head_author = command(
            "git",
            &[
                "-C",
                dir.path().to_str().unwrap(),
                "show",
                "-s",
                "--format=%an|%ae",
                &expected,
            ],
        );
        let amended = publish_logical_head(
            dir.path(),
            "feature",
            &expected,
            &delta,
            Some(PublicationIdentity {
                name: "Amend Committer".into(),
                email: "amend@cairn.local".into(),
            }),
            PublicationMode::Amend,
        )
        .unwrap();
        assert_eq!(amended.change_id, expected_change);
        // A rewrite keeps the original author and re-stamps only the committer,
        // so the amend path needs the identity just as much as a child commit
        // does — without it the committer is empty and the branch stops pushing.
        assert_eq!(
            command(
                "git",
                &[
                    "-C",
                    dir.path().to_str().unwrap(),
                    "show",
                    "-s",
                    "--format=%an|%ae|%cn|%ce",
                    &amended.head,
                ],
            ),
            format!("{head_author}|Amend Committer|amend@cairn.local")
        );
        let child_after = command(
            "jj",
            &[
                "-R",
                dir.path().to_str().unwrap(),
                "log",
                "-r",
                "child-seed",
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
        );
        assert_ne!(child_after, child_before);
        assert_eq!(
            command(
                "git",
                &[
                    "-C",
                    dir.path().to_str().unwrap(),
                    "show",
                    "-s",
                    "--format=%P",
                    &child_after,
                ],
            ),
            amended.head
        );
        let guarded_delta = delta_commit(dir.path(), &amended.head, "guarded\n");
        command(
            "jj",
            &[
                "-R",
                dir.path().to_str().unwrap(),
                "bookmark",
                "create",
                "sibling",
                "-r",
                &amended.head,
                "--ignore-working-copy",
            ],
        );
        let guarded = publish_logical_head(
            dir.path(),
            "feature",
            &amended.head,
            &guarded_delta,
            Some(PublicationIdentity {
                name: "Guarded Committer".into(),
                email: "guarded@cairn.local".into(),
            }),
            PublicationMode::Amend,
        )
        .unwrap();
        assert_ne!(guarded.change_id, amended.change_id);
        // The foreign-bookmark guard writes a CHILD instead of rewriting, a
        // third commit-writing path that must also be signed.
        assert_eq!(
            command(
                "git",
                &[
                    "-C",
                    dir.path().to_str().unwrap(),
                    "show",
                    "-s",
                    "--format=%an|%ae|%cn|%ce",
                    &guarded.head,
                ],
            ),
            "Guarded Committer|guarded@cairn.local|Guarded Committer|guarded@cairn.local"
        );
        assert_eq!(
            guarded.amend_note.as_deref(),
            Some("amend converted to a new commit: the previous commit is shared with sibling")
        );
        assert_eq!(
            command(
                "jj",
                &[
                    "-R",
                    dir.path().to_str().unwrap(),
                    "log",
                    "-r",
                    "sibling",
                    "--no-graph",
                    "-T",
                    "commit_id",
                    "--ignore-working-copy",
                ],
            ),
            amended.head
        );
    }

    fn command_with_input(program: &str, args: &[&str], input: &str) -> String {
        let mut child = Command::new(program)
            .args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{program} {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    fn delta_commit(repo: &Path, parent: &str, value: &str) -> String {
        let path = repo.to_str().unwrap();
        let blob = command_with_input("git", &["-C", path, "hash-object", "-w", "--stdin"], value);
        let tree = command_with_input(
            "git",
            &["-C", path, "mktree"],
            &format!("100644 blob {blob}\tvalue\n"),
        );
        command_with_input(
            "git",
            &["-C", path, "commit-tree", &tree, "-p", parent],
            "delta\n",
        )
    }
    #[test]
    fn resolves_sha_change_prefix_and_bookmark_without_a_command_runner() {
        let (dir, commit, change, ambiguous) = fixture();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            assert_eq!(
                resolve_coordinate(dir.path(), &commit).await.unwrap(),
                commit
            );
            assert_eq!(
                resolve_coordinate(dir.path(), &change[..8]).await.unwrap(),
                commit
            );
            let started = std::time::Instant::now();
            assert_eq!(
                resolve_coordinate(dir.path(), "feature").await.unwrap(),
                commit
            );
            eprintln!("embedded bookmark resolution: {:?}", started.elapsed());
            let remote_commit = command(
                "git",
                &["-C", dir.path().to_str().unwrap(), "rev-parse", "HEAD"],
            );
            assert_eq!(
                resolve_coordinate(dir.path(), "main@origin").await.unwrap(),
                remote_commit
            );
            assert!(matches!(
                resolve_coordinate(dir.path(), "does-not-exist").await,
                Err(CoordinateResolutionError::Absent { .. })
            ));
            assert!(matches!(
                resolve_coordinate(dir.path(), &ambiguous).await,
                Err(CoordinateResolutionError::Ambiguous(_))
            ));
        });
    }

    /// A branch cut from `main` at a fork point, after which `main` advances
    /// underneath it with unrelated work — the shape that inflates any diff
    /// rendered from a fork point recorded when the branch was cut.
    fn advanced_base_fixture() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        command("git", &["-C", path, "init", "-q", "-b", "main"]);
        command(
            "git",
            &["-C", path, "config", "user.email", "test@cairn.local"],
        );
        command("git", &["-C", path, "config", "user.name", "Cairn Test"]);
        std::fs::write(dir.path().join("shared"), "fork point\n").unwrap();
        command("git", &["-C", path, "add", "shared"]);
        command("git", &["-C", path, "commit", "-qm", "fork point"]);

        command("git", &["-C", path, "checkout", "-q", "-b", "feature"]);
        std::fs::write(dir.path().join("branch-work"), "branch work\n").unwrap();
        command("git", &["-C", path, "add", "branch-work"]);
        command("git", &["-C", path, "commit", "-qm", "branch work"]);

        command("git", &["-C", path, "checkout", "-q", "main"]);
        for index in 0..3 {
            std::fs::write(dir.path().join("other"), format!("{index}\n")).unwrap();
            command("git", &["-C", path, "add", "other"]);
            command(
                "git",
                &["-C", path, "commit", "-qm", &format!("other work {index}")],
            );
        }
        command("jj", &["git", "init", "--colocate", path]);
        dir
    }

    fn git_rev(repo: &Path, rev: &str) -> String {
        command("git", &["-C", repo.to_str().unwrap(), "rev-parse", rev])
    }

    #[test]
    fn merge_base_is_the_fork_point_not_the_advanced_target() {
        let dir = advanced_base_fixture();
        let fork = git_rev(dir.path(), "feature^");
        let branch_head = git_rev(dir.path(), "feature");
        let target_head = git_rev(dir.path(), "main");
        assert_ne!(fork, target_head, "the fixture must advance the target");

        let resolved = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(merge_base(dir.path(), "feature", "main"))
            .unwrap();

        assert_eq!(resolved.base, fork);
        assert_eq!(resolved.left, branch_head);
        assert_eq!(resolved.right, target_head);
    }

    #[test]
    fn merge_base_follows_a_rebase_onto_the_advanced_target() {
        let dir = advanced_base_fixture();
        let path = dir.path().to_str().unwrap();
        let advanced = git_rev(dir.path(), "main");
        command("jj", &["-R", path, "rebase", "-b", "feature", "-d", "main"]);

        let resolved = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(merge_base(dir.path(), "feature", "main"))
            .unwrap();

        // After the rebase the fork point IS the advanced target, and the branch
        // head has been rewritten onto it.
        assert_eq!(resolved.base, advanced);
        assert_eq!(resolved.right, advanced);
        assert_ne!(resolved.left, advanced);
    }

    #[test]
    fn merge_base_reports_absence_for_unrelated_histories() {
        let dir = advanced_base_fixture();
        let path = dir.path().to_str().unwrap();
        command(
            "git",
            &["-C", path, "checkout", "-q", "--orphan", "stranger"],
        );
        std::fs::write(dir.path().join("stranger"), "unrelated\n").unwrap();
        command("git", &["-C", path, "add", "stranger"]);
        command("git", &["-C", path, "commit", "-qm", "unrelated root"]);
        command("jj", &["-R", path, "git", "import"]);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(matches!(
            runtime.block_on(merge_base(dir.path(), "stranger", "main")),
            Err(CoordinateResolutionError::Absent { .. })
        ));
        assert!(matches!(
            runtime.block_on(merge_base(dir.path(), "does-not-exist", "main")),
            Err(CoordinateResolutionError::Absent { .. })
        ));
    }

    /// A store whose local bookmark `agent/x` sits in jj's conflicted-name state,
    /// built the way the real one arises: the local side and the backing git ref
    /// both move off a common base between exports, and the next import records
    /// both as competing targets.
    ///
    /// The topology matches production — a non-colocated store whose git backend
    /// is the project's own `.git` — because a colocated repository auto-exports
    /// on every jj command and so never reaches this state.
    ///
    /// Returns the temp root, the store path, and the two competing commits.
    fn conflicted_bookmark_fixture() -> (tempfile::TempDir, std::path::PathBuf, String, String) {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("project");
        let store = dir.path().join("store");
        std::fs::create_dir_all(&project).unwrap();
        let path = project.to_str().unwrap();
        command("git", &["-C", path, "init", "-q", "-b", "main"]);
        command(
            "git",
            &["-C", path, "config", "user.email", "test@cairn.local"],
        );
        command("git", &["-C", path, "config", "user.name", "Cairn Test"]);
        std::fs::write(project.join("f.txt"), "base\n").unwrap();
        command("git", &["-C", path, "add", "-A"]);
        command("git", &["-C", path, "commit", "-qm", "base"]);
        let base = git_rev(&project, "HEAD");
        command("git", &["-C", path, "branch", "agent/x", &base]);
        let store_arg = store.to_str().unwrap();
        command("jj", &["git", "init", "--git-repo", path, store_arg]);

        // Two independent children of `base`, both made reachable so the store
        // imports them, neither yet claimed by `agent/x`.
        let local = sibling_commit(&project, &base, "l.txt", "local work");
        let git_side = sibling_commit(&project, &base, "o.txt", "moved outside jj");
        command(
            "git",
            &["-C", path, "update-ref", "refs/heads/keep-l", &local],
        );
        command(
            "git",
            &["-C", path, "update-ref", "refs/heads/keep-g", &git_side],
        );
        command(
            "jj",
            &["-R", store_arg, "--ignore-working-copy", "git", "import"],
        );

        // The local side advances inside jj and is deliberately not exported;
        // the git ref then advances outside jj. The next import sees both.
        command(
            "jj",
            &[
                "-R",
                store_arg,
                "--ignore-working-copy",
                "bookmark",
                "set",
                "agent/x",
                "-r",
                &local,
                "--allow-backwards",
            ],
        );
        command(
            "git",
            &["-C", path, "update-ref", "refs/heads/agent/x", &git_side],
        );
        command(
            "jj",
            &["-R", store_arg, "--ignore-working-copy", "git", "import"],
        );
        (dir, store, local, git_side)
    }

    /// A commit on top of `parent` that adds one file, left unreachable from the
    /// working tree so the caller decides which ref claims it.
    fn sibling_commit(project: &Path, parent: &str, file: &str, body: &str) -> String {
        let path = project.to_str().unwrap();
        std::fs::write(project.join(file), format!("{body}\n")).unwrap();
        command("git", &["-C", path, "add", "-A"]);
        let tree = command("git", &["-C", path, "write-tree"]);
        let commit = command(
            "git",
            &["-C", path, "commit-tree", &tree, "-p", parent, "-m", body],
        );
        command("git", &["-C", path, "reset", "-q", "--hard", parent]);
        commit
    }

    /// A conflicted name is a repairable condition, and the resolver has to say
    /// so. Reported as ordinary absence, it reached agents as "this branch does
    /// not exist" and took every verb that resolved the name down with it.
    #[test]
    fn a_conflicted_bookmark_name_resolves_as_conflicted_not_absent() {
        let (_dir, store, local, git_side) = conflicted_bookmark_fixture();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let error = runtime
            .block_on(resolve_coordinate(&store, "agent/x"))
            .expect_err("a conflicted name resolves to no single commit");

        let CoordinateResolutionError::Conflicted {
            coordinate,
            targets,
        } = &error
        else {
            panic!("expected a conflicted-name outcome, got {error:?}");
        };
        assert_eq!(coordinate, "agent/x");
        let mut expected = vec![local, git_side];
        expected.sort();
        assert_eq!(targets, &expected);
    }

    /// The competing commits stay individually addressable. This is what lets a
    /// read serve content while the name itself is unusable, and what lets a
    /// repair choose among the targets the error just named.
    #[test]
    fn a_conflicted_name_never_costs_its_commits_their_own_resolvability() {
        let (_dir, store, local, git_side) = conflicted_bookmark_fixture();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        for commit in [&local, &git_side] {
            assert_eq!(
                runtime
                    .block_on(resolve_coordinate(&store, commit))
                    .unwrap(),
                *commit
            );
        }
        // An unrelated name is untouched by the neighbouring conflict.
        assert!(runtime.block_on(resolve_coordinate(&store, "main")).is_ok());
    }

    #[test]
    fn rejects_an_empty_coordinate_before_loading_a_repository() {
        let result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(resolve_coordinate(Path::new("missing"), "  "));
        assert_eq!(
            result,
            Err(CoordinateResolutionError::Invalid(String::new()))
        );
    }
}
