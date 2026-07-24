//! Embedded version-control transactions quarantined from Cairn's core domain.
//!
//! The public boundary uses filesystem paths and hexadecimal object IDs only.
//! No jj-lib or gix type crosses into `cairn-core`, and this crate never imports
//! `cairn-core`.

use std::collections::HashMap;
use std::path::Path;

use futures::io::Cursor;
use jj_lib::backend::{CommitId, CopyId, Signature, TreeValue};
use jj_lib::config::StackedConfig;
use jj_lib::local_working_copy::{LocalWorkingCopy, LocalWorkingCopyFactory};
use jj_lib::merge::Merge;
use jj_lib::merged_tree_builder::MergedTreeBuilder;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::RefTarget;
use jj_lib::ref_name::{RefName, RemoteRefSymbol};
use jj_lib::repo::{Repo as _, StoreFactories};
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::revset::{SymbolResolver, SymbolResolverExtension};
use jj_lib::settings::UserSettings;
use jj_lib::workspace::{WorkingCopyFactories, Workspace};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinateResolutionError {
    Invalid(String),
    Absent {
        coordinate: String,
        diagnostic: String,
    },
    Ambiguous(String),
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
            mode,
        ))
}

enum ProposedTree {
    DeltaCommit(String),
    Mutations(Vec<LogicalTreeMutation>),
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
            mode,
        ))
}

async fn publish_logical_head_inner(
    repository_path: &Path,
    bookmark: &str,
    expected_head: &str,
    proposed_tree: ProposedTree,
    mode: PublicationMode,
) -> Result<LogicalHeadPublication, String> {
    if bookmark.trim().is_empty() {
        return Err("logical-head bookmark must not be empty".to_string());
    }

    let expected_id = CommitId::try_from_hex(expected_head)
        .ok_or_else(|| "expected logical head is not a full hexadecimal object ID".to_string())?;
    let settings = UserSettings::from_config(StackedConfig::with_defaults())
        .map_err(|error| format!("load logical-head publication settings: {error}"))?;
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
    let tree = match proposed_tree {
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
            delta.tree()
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
            builder
                .write_tree()
                .await
                .map_err(|error| format!("write proposed logical tree: {error}"))?
        }
    };

    let mut amend_note = None;
    let mut tx = repo.start_transaction();
    let mut rewrote_head = false;
    let published = match mode {
        PublicationMode::Child {
            description,
            author,
        } => {
            let mut builder = tx
                .repo_mut()
                .new_commit(vec![expected_id.clone()], tree.clone())
                .set_description(description);
            if let Some(author) = author {
                let mut signature = settings.signature();
                signature.name = author.name;
                signature.email = author.email;
                builder = builder.set_author(Signature { ..signature });
            }
            builder
                .write()
                .await
                .map_err(|error| format!("write logical-head child commit: {error}"))?
        }
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
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationAuthor {
    pub name: String,
    pub email: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationMode {
    Child {
        description: String,
        author: Option<PublicationAuthor>,
    },
    Amend,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalHeadPublication {
    pub head: String,
    pub change_id: String,
    pub amend_note: Option<String>,
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
    let settings = UserSettings::from_config(StackedConfig::with_defaults())
        .map_err(|error| CoordinateResolutionError::Repository(error.to_string()))?;
    let stores = StoreFactories::default();
    let mut working_copies: WorkingCopyFactories = HashMap::new();
    working_copies.insert(
        LocalWorkingCopy::name().to_string(),
        Box::new(LocalWorkingCopyFactory {}),
    );
    let workspace = Workspace::load(&settings, repository_path, &stores, &working_copies)
        .map_err(|error| CoordinateResolutionError::Repository(error.to_string()))?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .await
        .map_err(|error| CoordinateResolutionError::Repository(error.to_string()))?;
    if let Some((name, remote)) = coordinate.rsplit_once('@') {
        if !name.is_empty() && !remote.is_empty() {
            let remote_ref = repo.view().get_remote_bookmark(RemoteRefSymbol {
                name: name.as_ref(),
                remote: remote.as_ref(),
            });
            return match remote_ref.target.as_resolved() {
                Some(Some(id)) => Ok(id.hex()),
                Some(None) => Err(CoordinateResolutionError::Absent {
                    coordinate: coordinate.to_string(),
                    diagnostic: "remote bookmark is absent".to_string(),
                }),
                None => Err(CoordinateResolutionError::Ambiguous(coordinate.to_string())),
            };
        }
    }
    let extensions: &[Box<dyn SymbolResolverExtension>] = &[];
    let resolver = SymbolResolver::new(repo.as_ref(), extensions);
    resolver
        .resolve_symbol(repo.as_ref(), coordinate)
        .map(|id| id.hex())
        .map_err(|error| {
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
            PublicationMode::Child {
                description: "logical child".into(),
                author: Some(PublicationAuthor {
                    name: "Logical Author".into(),
                    email: "logical@cairn.local".into(),
                }),
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
        assert_eq!(
            command(
                "git",
                &[
                    "-C",
                    dir.path().to_str().unwrap(),
                    "show",
                    "-s",
                    "--format=%P|%an|%ae",
                    &result.head
                ]
            ),
            format!("{expected}|Logical Author|logical@cairn.local")
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
            PublicationMode::Amend,
        )
        .unwrap_err();
        assert!(stale.contains("changed from"));
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
            PublicationMode::Child {
                description: "tree mutation".into(),
                author: None,
            },
        )
        .unwrap();
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
        let amended = publish_logical_head(
            dir.path(),
            "feature",
            &expected,
            &delta,
            PublicationMode::Amend,
        )
        .unwrap();
        assert_eq!(amended.change_id, expected_change);
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
            PublicationMode::Amend,
        )
        .unwrap();
        assert_ne!(guarded.change_id, amended.change_id);
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
