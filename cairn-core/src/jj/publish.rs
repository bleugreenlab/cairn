//! Runner-only publication of validated executor trees into jj history.
//!
//! ## The publication ladder
//!
//! Advancing a managed branch is three rungs, not one, and they live in
//! different places:
//!
//! 1. **The jj bookmark.** [`cairn_vcs::publish_logical_head`] /
//!    [`cairn_vcs::publish_logical_mutations`] write a commit and move the
//!    bookmark inside one store transaction. That transaction is deliberately
//!    CLI-free and touches nothing but the jj operation log.
//! 2. **The git ref.** `refs/heads/<branch>` in the backing checkout is a
//!    separate ref that only `jj git export` writes. Everything outside jj —
//!    `git rev-parse`, a push, a child branch cut from the ref, GitHub's view of
//!    a PR head — reads that ref and nothing else.
//! 3. **`origin`.** A push, required while a remote PR is open on the branch.
//!
//! [`publish_logical_head_exported`] joins rungs 1 and 2 into one operation, and
//! is the ONLY sanctioned caller of `cairn_vcs::publish_logical_*`.
//! [`publish_branch_to_origin`] is rung 3. A publication that climbs only the
//! first rung looks entirely successful — it reports a real commit on a real
//! branch — while the git ref stays frozen at whatever it last held, so the
//! failure is invisible until something downstream reads the stale tip.

use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;

use super::export::export_bookmark_advance;
use super::merge::StoreBookmarkPushError;
use super::reconcile::{discover_origin_presence, OriginPresence};
use super::JjEnv;

/// The description carried by an integration commit. The object is
/// intermediate — [`cairn_vcs::publish_logical_head`] writes a fresh jj commit
/// from its tree with the publication identity and the batch's own message — so
/// this text never reaches history. `git commit-tree` still requires one.
const INTEGRATION_DESCRIPTION: &str = "cairn: integrate straddled delta onto the moved branch head";

/// The tree a batch proposes for its branch's logical head.
pub(crate) enum ProposedPublication {
    /// A validated delta commit whose tree becomes the published tree.
    DeltaCommit(String),
    /// Ordered path mutations applied to the current head's tree without
    /// materializing it in any workspace.
    Mutations(Vec<cairn_vcs::LogicalTreeMutation>),
    /// A reachable commit whose inverse is applied to the current head.
    Revert(String),
}

/// A logical-head publication together with the export leg that makes it
/// visible outside jj.
pub(crate) struct PublishedLogicalHead {
    /// The commit the transaction landed. Real and durable whatever `export`
    /// says: the store transaction committed before the export ran.
    pub landed: cairn_vcs::LogicalHeadPublication,
    /// The jj→git export. `Err` means the bookmark advanced but
    /// `refs/heads/<branch>` in the backing checkout did not, so the commit is
    /// sealed locally and unpublished. Callers MUST NOT report such a
    /// publication as committed; the whole point of carrying the result here
    /// rather than logging it is that a discarded export result is what made
    /// this class of failure invisible.
    pub export: Result<(), String>,
}

/// Publish a batch at a branch's logical head and export the advanced bookmark
/// into the backing checkout's `refs/heads/<branch>`.
///
/// The single sanctioned entry point for `cairn_vcs::publish_logical_*`. See the
/// module docs for why the two legs are one operation.
///
/// The export is wrapped here in cairn-core rather than inside the transaction
/// because [`super::export::export_bookmark_advance`] already resolves the
/// bookmark, exports, PROVES the git ref moved, detects and repairs a freeze,
/// re-verifies, and reattaches a checkout HEAD the export detached. cairn-vcs is
/// deliberately CLI-free, so reimplementing that against `jj_lib` there would be
/// a second implementation of hardened code.
///
/// Both legs run inside one blocking task under the caller's already-held store
/// lock: the export must observe exactly the bookmark this transaction wrote,
/// with no other writer in between.
pub(crate) async fn publish_logical_head_exported(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    expected_head: &str,
    proposed: ProposedPublication,
    identity: Option<cairn_vcs::PublicationIdentity>,
    mode: cairn_vcs::PublicationMode,
) -> Result<PublishedLogicalHead, String> {
    let jj = jj.clone();
    let store = store.to_path_buf();
    let branch = branch.to_string();
    let expected_head = expected_head.to_string();
    tokio::task::spawn_blocking(move || {
        let landed = match proposed {
            ProposedPublication::DeltaCommit(delta) => cairn_vcs::publish_logical_head(
                &store,
                &branch,
                &expected_head,
                &delta,
                identity,
                mode,
            ),
            ProposedPublication::Mutations(mutations) => cairn_vcs::publish_logical_mutations(
                &store,
                &branch,
                &expected_head,
                mutations,
                identity,
                mode,
            ),
            ProposedPublication::Revert(commit) => {
                let cairn_vcs::PublicationMode::Child { description } = mode else {
                    return Err("logical revert must publish a child commit".to_string());
                };
                cairn_vcs::publish_logical_revert(
                    &store,
                    &branch,
                    &expected_head,
                    &commit,
                    identity,
                    description,
                )
            }
        }?;
        let export = export_bookmark_advance(
            &jj,
            &store,
            true,
            &branch,
            &format!("publish `{branch}` at the logical head"),
        );
        Ok(PublishedLogicalHead { landed, export })
    })
    .await
    .map_err(|error| format!("logical-head publication worker failed: {error}"))?
}

/// Push an exported branch bookmark to `origin` — the ladder's third rung.
///
/// Deliberately NOT run under the store lock: this is network I/O, and the
/// bookmark it publishes is already durable in the store and in `refs/heads/*`.
/// Addresses the SHARED STORE rather than a workspace, because agent processes
/// live in scratch and own no jj workspace — a push routed through a workspace
/// marker has nothing to read and silently publishes nothing.
///
/// A project with no `origin` has nothing to publish and is not a failure;
/// anything else propagates.
pub(crate) async fn publish_branch_to_origin(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<(), StoreBookmarkPushError> {
    let jj = jj.clone();
    let store = store.to_path_buf();
    let branch = branch.to_string();
    tokio::task::spawn_blocking(move || {
        match super::merge::push_store_bookmark_classified(&jj, &store, &branch) {
            Ok(()) => Ok(()),
            Err(error) => match discover_origin_presence(&jj, &store) {
                OriginPresence::Absent => {
                    log::info!("publish `{branch}`: no `origin` remote; nothing to publish");
                    Ok(())
                }
                _ => Err(error),
            },
        }
    })
    .await
    .map_err(|error| {
        StoreBookmarkPushError::Failed(format!("origin publication worker failed: {error}"))
    })?
}

/// The ladder is a contract between three call sites that cannot see each other.
///
/// Its rungs were split across modules once already, and the split was invisible
/// precisely because each site looked complete on its own: the transaction
/// reported a real commit, so nothing downstream had a reason to ask whether the
/// branch ref moved. These are static guards over the sites' own source, in the
/// idiom of `run`'s failure-opening guard — they cost nothing and they fail on
/// the exact drift that produced the incident.
#[cfg(test)]
mod ladder_contract {
    use super::{publish_logical_head_exported, JjEnv, ProposedPublication};

    /// Every site that advances a managed branch, with its source.
    const PUBLICATION_SITES: &[(&str, &str)] = &[
        ("run", include_str!("../mcp/handlers/run/mod.rs")),
        (
            "write",
            include_str!("../mcp/handlers/write/file_mutations.rs"),
        ),
        ("memory triage", include_str!("../execution/actions.rs")),
    ];

    /// The verbs that commit on behalf of an agent, and therefore owe an origin
    /// push while a remote PR is open on the branch. Each carries every source
    /// file its ladder is spread across. The memory-triage ledger is excluded
    /// deliberately: it publishes onto a branch whose PR its own action opens
    /// afterwards.
    const AGENT_COMMIT_VERBS: &[(&str, &[&str])] = &[
        ("run", &[include_str!("../mcp/handlers/run/mod.rs")]),
        (
            "write",
            &[
                include_str!("../mcp/handlers/write/mod.rs"),
                include_str!("../mcp/handlers/write/file_mutations.rs"),
            ],
        ),
    ];

    const MANAGED_PUSH_TRIGGERS: &[(&str, &str)] = &[
        ("run", include_str!("../mcp/handlers/run/mod.rs")),
        ("write", include_str!("../mcp/handlers/write/mod.rs")),
        (
            "create-pr resave",
            include_str!("../pr_data/actions/create_pr.rs"),
        ),
        (
            "reconciler",
            include_str!("../orchestrator/base_advance.rs"),
        ),
    ];

    #[test]
    fn no_site_publishes_a_logical_head_without_exporting_it() {
        for (name, source) in PUBLICATION_SITES {
            assert!(
                source.contains("publish_logical_head_exported"),
                "{name} must publish through the sanctioned barrier"
            );
            for bare in [
                "cairn_vcs::publish_logical_head(",
                "cairn_vcs::publish_logical_mutations(",
            ] {
                assert!(
                    !source.contains(bare),
                    "{name} calls {bare} directly, which moves the jj bookmark and leaves the \
                     branch ref frozen. Publish through publish_logical_head_exported."
                );
            }
        }
    }

    #[test]
    fn both_commit_verbs_climb_the_same_ladder() {
        for (name, sources) in AGENT_COMMIT_VERBS {
            for rung in [
                "publication_requirement_for_managed_branch",
                "publish_managed_branch",
            ] {
                assert!(
                    sources.iter().any(|source| source.contains(rung)),
                    "{name} never reaches `{rung}`. Both commit verbs advance the same branches, \
                     so a verb missing a rung silently withholds publication for every commit \
                     made through it."
                );
            }
        }
    }

    #[tokio::test]
    async fn revert_publication_refuses_amend_before_touching_the_store() {
        let temp = tempfile::tempdir().unwrap();
        let jj = JjEnv::resolve("jj", temp.path());
        let error = publish_logical_head_exported(
            &jj,
            temp.path(),
            "feature",
            &"a".repeat(40),
            ProposedPublication::Revert("b".repeat(40)),
            None,
            cairn_vcs::PublicationMode::Amend,
        )
        .await
        .err()
        .expect("amend must be refused");
        assert_eq!(error, "logical revert must publish a child commit");
    }

    #[test]
    fn every_managed_push_trigger_uses_the_recovery_wrapper() {
        for (name, source) in MANAGED_PUSH_TRIGGERS {
            assert!(
                source.contains("publish_managed_branch"),
                "{name} bypasses the canonical stale-publication recovery wrapper"
            );
        }
    }
}

/// What reconciling a sealed delta with the branch head it must land on found.
///
/// A delta declares the commit it was built on as its parent, and the logical
/// head transaction refuses a parent the bookmark no longer holds. When the
/// bookmark moved between routing and publication — a sibling landed, or the
/// base-advance reconciler rebased the branch — that refusal strands the
/// batch's work with no route back into the branch. Integration answers the
/// question the refusal asks: what would this batch's changes look like sitting
/// on the head the branch actually holds?
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Integration {
    /// The bookmark still holds the commit the batch built on. The sealed delta
    /// publishes exactly as it is.
    Unmoved,
    /// The branch already carries the batch's content: merging changes nothing
    /// about the head's tree. Publishing would add an empty commit.
    AlreadyLanded,
    /// A commit parented at the current head whose tree carries both the
    /// advance and the batch's work.
    Commit(String),
    /// The advance and the batch changed the same region. Nothing was written
    /// to any ref; the batch's edits stay where they are.
    Conflicted { paths: Vec<String> },
}

/// Reconcile a sealed delta with the branch head it must land on.
///
/// A real three-way merge with `delta_base` as the *explicit* merge base. The
/// explicit base is load-bearing: after a rebase advance, `head` and
/// `delta_commit` share only the pre-rebase base, so git's own merge-base
/// computation answers a different question than the one being asked.
/// `delta_base` is what the batch actually built on, which is the definition of
/// the base for this merge.
///
/// A merge rather than a replay of the delta's own diff, because a replay
/// silently takes the batch's side wherever the advance also changed a path —
/// a lost update. The merge takes both sides where they are disjoint and
/// conflicts only where they genuinely diverge.
///
/// Fails closed: a merge that cannot be computed, or one that conflicts, writes
/// nothing to any ref and returns without proposing a commit.
pub(crate) fn integrate_delta_onto_head(
    repository: &Path,
    delta_base: &str,
    delta_commit: &str,
    head: &str,
) -> Result<Integration, String> {
    if head.eq_ignore_ascii_case(delta_base) {
        return Ok(Integration::Unmoved);
    }
    let output = crate::env::git()
        .args([
            "merge-tree",
            "--write-tree",
            "--no-messages",
            "-z",
            &format!("--merge-base={delta_base}"),
            head,
            delta_commit,
        ])
        .current_dir(repository)
        .output()
        .map_err(|error| format!("integrate delta {delta_commit} onto {head}: {error}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut sections = stdout.split('\0');
    let tree = sections.next().unwrap_or_default().trim().to_string();
    match output.status.code() {
        Some(0) => {}
        // Conflicted file info follows the tree, one NUL-terminated
        // `<mode> <object> <stage>\t<path>` record per conflicted stage, so one
        // path appears up to three times.
        Some(1) => {
            let mut paths = sections
                .filter_map(|entry| entry.split_once('\t'))
                .map(|(_, path)| path.to_string())
                .collect::<Vec<_>>();
            paths.sort();
            paths.dedup();
            return Ok(Integration::Conflicted { paths });
        }
        _ => {
            return Err(format!(
                "integrate delta {delta_commit} onto {head}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        }
    }
    if tree.is_empty() {
        return Err(format!(
            "integrate delta {delta_commit} onto {head}: the merge reported success without a tree"
        ));
    }
    if tree
        == git_capture(
            repository,
            &["rev-parse", &format!("{head}^{{tree}}")],
            "read the logical head tree for integration",
        )?
    {
        return Ok(Integration::AlreadyLanded);
    }
    let commit = git_capture_with_identity(
        repository,
        &[
            "commit-tree",
            &tree,
            "-p",
            head,
            "-m",
            INTEGRATION_DESCRIPTION,
        ],
        "write the integrated delta commit",
    )?;
    Ok(Integration::Commit(commit))
}

fn git_capture(repository: &Path, args: &[&str], context: &str) -> Result<String, String> {
    capture(
        crate::env::git().args(args).current_dir(repository),
        context,
    )
}

/// `git commit-tree` refuses to run without a configured identity, and the
/// repository's own config is the user's rather than Cairn's. The managed
/// identity is supplied explicitly, the same one every other Cairn-authored
/// object falls back to.
fn git_capture_with_identity(
    repository: &Path,
    args: &[&str],
    context: &str,
) -> Result<String, String> {
    capture(
        crate::env::git()
            .args(args)
            .current_dir(repository)
            .env("GIT_AUTHOR_NAME", cairn_vcs::MANAGED_IDENTITY_NAME)
            .env("GIT_AUTHOR_EMAIL", cairn_vcs::MANAGED_IDENTITY_EMAIL)
            .env("GIT_COMMITTER_NAME", cairn_vcs::MANAGED_IDENTITY_NAME)
            .env("GIT_COMMITTER_EMAIL", cairn_vcs::MANAGED_IDENTITY_EMAIL),
        context,
    )
}

fn capture(command: &mut std::process::Command, context: &str) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|error| format!("{context}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{context}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// A Git pack protected from pruning while a ref-less delta is being folded.
#[derive(Debug)]
pub struct DeltaObjectPin {
    keep_path: PathBuf,
    owned: bool,
}

impl Drop for DeltaObjectPin {
    fn drop(&mut self) {
        if !self.owned {
            return;
        }
        if let Err(error) = std::fs::remove_file(&self.keep_path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                log::warn!(
                    "failed to release delta object pin {}: {error}",
                    self.keep_path.display()
                );
            }
        }
    }
}

/// Refuse a commit whose tree root carries jj conflict scaffolding.
///
/// The publish boundary is the last place a poisoned tree can be stopped before
/// it becomes history. A tree that needs sidecars is not a resolved merge: its
/// top-level content silently holds one side of every conflicted file, and
/// committing it onward is how half a design lands on the default branch. Fails
/// closed — an unreadable tree here is "could not check", which on a publication
/// path is not "checked and fine".
fn refuse_conflict_scaffolding(repository: &Path, commit: &str) -> Result<(), String> {
    let output = crate::env::git()
        .args(["ls-tree", "--name-only", commit])
        .current_dir(repository)
        .output()
        .map_err(|error| {
            format!("inspect delta tree root {commit} for conflict scaffolding: {error}")
        })?;
    if !output.status.success() {
        return Err(format!(
            "inspect delta tree root {commit} for conflict scaffolding: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let entries = cairn_common::conflict_scaffolding::conflict_scaffolding_in_root_listing(
        &String::from_utf8_lossy(&output.stdout),
    );
    if entries.is_empty() {
        return Ok(());
    }
    log::error!(
        "publish refused in {}: delta commit {commit} carries jj conflict scaffolding ({}). The \
         branch this delta sits on needs repair in the store.",
        repository.display(),
        entries.join(", ")
    );
    Err(
        cairn_common::conflict_scaffolding::conflict_scaffolding_refusal(
            "publish", commit, &entries,
        ),
    )
}

/// Protect a validated delta from even immediate Git pruning until its tree is
/// committed into jj. Managed packs pass their installed path directly;
/// colocated loose objects are copied into a dedicated non-thin pack first.
///
/// Guarded first by [`refuse_conflict_scaffolding`]: a delta whose tree carries
/// jj conflict sidecars never gets pinned, so it never gets folded into history.
pub(crate) fn pin_validated_delta(
    repository: &Path,
    base_commit: &str,
    delta_commit: &str,
    installed_pack: Option<&Path>,
) -> Result<DeltaObjectPin, String> {
    refuse_conflict_scaffolding(repository, delta_commit)?;
    let pack_path = if let Some(pack_path) = installed_pack {
        pack_path.to_path_buf()
    } else {
        let output = crate::env::git()
            .args(["rev-parse", "--git-path", "objects"])
            .current_dir(repository)
            .output()
            .map_err(|error| format!("resolve delta pin object database: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "resolve delta pin object database: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        let objects = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
        let objects = if objects.is_absolute() {
            objects
        } else {
            repository.join(objects)
        };
        let prefix = objects.join("pack").join("cairn-delta-pin");
        std::fs::create_dir_all(prefix.parent().expect("pack prefix has parent"))
            .map_err(|error| format!("create delta pin pack directory: {error}"))?;
        let mut child = crate::env::git()
            .args(["pack-objects", "--revs"])
            .arg(&prefix)
            .current_dir(repository)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("start delta pin pack construction: {error}"))?;
        write!(
            child.stdin.take().expect("piped stdin"),
            "{delta_commit}\n^{base_commit}\n"
        )
        .map_err(|error| format!("write delta pin revision set: {error}"))?;
        let output = child
            .wait_with_output()
            .map_err(|error| format!("wait for delta pin pack construction: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "construct delta pin pack: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        let checksum = String::from_utf8_lossy(&output.stdout).trim().to_string();
        prefix.with_file_name(format!("cairn-delta-pin-{checksum}.pack"))
    };
    let keep_path = pack_path.with_extension("keep");
    let owned = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&keep_path)
    {
        Ok(mut file) => {
            if let Err(error) = file.write_all(b"Cairn validated delta publication\n") {
                let _ = std::fs::remove_file(&keep_path);
                return Err(format!(
                    "write delta object pin {}: {error}",
                    keep_path.display()
                ));
            }
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(error) => {
            return Err(format!(
                "create delta object pin {}: {error}",
                keep_path.display()
            ))
        }
    };
    Ok(DeltaObjectPin { keep_path, owned })
}
