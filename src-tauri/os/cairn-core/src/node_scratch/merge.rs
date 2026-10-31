//! The path-level merge a write-back is folded into the head with.
//!
//! Scratch files are data, so nothing here merges text. Three inputs meet: the
//! head (the authority's current tree), and for every path the batch changed,
//! what the batch's machine last synced (`base`) and what the batch left
//! (`new`). When the head is still that base the merge is a fast-forward;
//! otherwise it is decided path by path, and nothing is ever dropped:
//!
//! - changed only by this batch: applied;
//! - changed only in the head: kept (it is simply not in the change list);
//! - changed identically on both sides: nothing to do;
//! - changed differently on both sides: the head's version stays, and this
//!   batch's is written beside it as `<path>~conflict-<writer>`;
//! - deleted on one side and changed on the other: the change wins.

use std::collections::BTreeMap;

use cairn_common::executor_protocol::{
    ScratchBlob, ScratchChange, ScratchConflict, ScratchEntrySize, ScratchPath, ScratchRefusal,
    NODE_SCRATCH_MAX_FILE_BYTES,
};

/// A scratch tree, flattened: every file's path and blob.
pub type Tree = BTreeMap<Vec<u8>, ScratchBlob>;

/// How many of the largest files a refusal or warning names.
pub const LARGEST_NAMED: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergePlan {
    pub result: Tree,
    pub merged_bytes: u64,
    pub conflicts: Vec<ScratchConflict>,
    /// The blobs the result takes from this write-back, one per oid. The store
    /// must hold every one of them before the result is committed.
    pub incoming: Vec<(Vec<u8>, ScratchBlob)>,
    /// Whether the result differs from the head at all.
    pub changed: bool,
}

impl MergePlan {
    /// Bytes the result holds that the head did not: the growth this
    /// write-back adds to the authority's monotonic `written_bytes`.
    pub fn added_bytes(&self, head: &Tree) -> u64 {
        let present: std::collections::HashSet<&str> =
            head.values().map(|blob| blob.oid.as_str()).collect();
        let mut counted = std::collections::HashSet::new();
        self.incoming
            .iter()
            .filter(|(_, blob)| !present.contains(blob.oid.as_str()))
            .filter(|(_, blob)| counted.insert(blob.oid.clone()))
            .map(|(_, blob)| blob.size)
            .sum()
    }
}

fn same(left: Option<&ScratchBlob>, right: Option<&ScratchBlob>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.oid == right.oid && left.mode == right.mode,
        _ => false,
    }
}

fn conflict_name(path: &[u8], writer: &str, attempt: usize) -> Vec<u8> {
    let mut name = path.to_vec();
    name.extend_from_slice(b"~conflict-");
    name.extend_from_slice(writer.as_bytes());
    if attempt > 1 {
        name.extend_from_slice(format!("-{attempt}").as_bytes());
    }
    name
}

/// A free name beside `path` for `blob`, reusing one that already holds the
/// same content, so folding the same write-back twice adds nothing.
fn conflict_path(result: &Tree, path: &[u8], writer: &str, blob: &ScratchBlob) -> Vec<u8> {
    for attempt in 1.. {
        let candidate = conflict_name(path, writer, attempt);
        match result.get(&candidate) {
            None => return candidate,
            Some(existing) if same(Some(existing), Some(blob)) => return candidate,
            Some(_) => {}
        }
    }
    unreachable!("an unbounded search always finds a free name")
}

/// Every proper directory prefix of `path`: `a/b/c` yields `a` and `a/b`.
fn prefixes(path: &[u8]) -> impl Iterator<Item = &[u8]> {
    path.iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b'/')
        .map(move |(index, _)| &path[..index])
}

/// Where `path` would clash with the tree's shape, if anywhere: a prefix of it
/// is a file, or it is itself a directory of other files. Git cannot record
/// both, so the incoming entry is the one that moves.
fn relocation(result: &Tree, path: &[u8], writer: &str, blob: &ScratchBlob) -> Option<Vec<u8>> {
    if let Some(file) = prefixes(path).find(|prefix| result.contains_key(*prefix)) {
        let mut moved = conflict_path(result, file, writer, blob);
        moved.extend_from_slice(&path[file.len()..]);
        return Some(moved);
    }
    let mut directory = path.to_vec();
    directory.push(b'/');
    let is_directory = result
        .range(directory.clone()..)
        .next()
        .is_some_and(|(key, _)| key.starts_with(&directory));
    is_directory.then(|| conflict_path(result, path, writer, blob))
}

/// Fold `changes` into `head`. Pure: the store is consulted by the caller.
pub fn plan(head: &Tree, changes: &[ScratchChange], writer: &str) -> MergePlan {
    let mut result = head.clone();
    let mut incoming: Vec<Vec<u8>> = Vec::new();
    let mut conflicts = Vec::new();
    for change in changes {
        let path = &change.path.0;
        let in_head = head.get(path);
        let base = change.base.as_ref();
        let new = change.new.as_ref();
        if same(in_head, new) {
            continue;
        }
        if same(in_head, base) || in_head.is_none() {
            // Only this batch changed it, or the head deleted what this batch
            // changed: either way the batch's version is the one to keep.
            match new {
                Some(blob) => {
                    result.insert(path.clone(), blob.clone());
                    incoming.push(path.clone());
                }
                None => {
                    result.remove(path);
                }
            }
        } else if let Some(blob) = new {
            let written_as = conflict_path(&result, path, writer, blob);
            result.insert(written_as.clone(), blob.clone());
            incoming.push(written_as.clone());
            conflicts.push(ScratchConflict {
                path: ScratchPath(path.clone()),
                written_as: ScratchPath(written_as),
            });
        }
        // Otherwise the batch deleted a path the head changed: the change wins.
    }

    // A batch's own tree is self-consistent, so a file/directory clash can
    // only come from meeting the head's. Move the incoming side.
    let mut placed = Vec::with_capacity(incoming.len());
    for path in incoming {
        let Some(blob) = result.get(&path).cloned() else {
            continue;
        };
        let mut current = path.clone();
        let mut guard = 0;
        // Neither check reads `current`'s own entry (only its proper prefixes
        // and the paths beneath it), so it can stay in the tree while asking.
        loop {
            match relocation(&result, &current, writer, &blob) {
                Some(moved) if guard < 8 => {
                    result.remove(&current);
                    result.insert(moved.clone(), blob.clone());
                    current = moved;
                    guard += 1;
                }
                _ => break,
            }
        }
        if current != path {
            match conflicts
                .iter_mut()
                .find(|conflict| conflict.written_as.0 == path)
            {
                Some(conflict) => conflict.written_as = ScratchPath(current.clone()),
                None => conflicts.push(ScratchConflict {
                    path: ScratchPath(path.clone()),
                    written_as: ScratchPath(current.clone()),
                }),
            }
        }
        placed.push((current, blob));
    }
    let mut seen = std::collections::HashSet::new();
    placed.retain(|(_, blob)| seen.insert(blob.oid.clone()));

    let merged_bytes = result.values().map(|blob| blob.size).sum();
    let changed = result != *head;
    MergePlan {
        result,
        merged_bytes,
        conflicts,
        incoming: placed,
        changed,
    }
}

/// The largest files of a tree, largest first.
pub fn largest(tree: &Tree, count: usize) -> Vec<ScratchEntrySize> {
    let mut files: Vec<(&Vec<u8>, u64)> =
        tree.iter().map(|(path, blob)| (path, blob.size)).collect();
    files.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0)));
    files
        .into_iter()
        .take(count)
        .map(|(path, bytes)| ScratchEntrySize {
            path: String::from_utf8_lossy(path).into_owned(),
            bytes,
        })
        .collect()
}

/// The largest top-level directories of a tree by the bytes beneath them,
/// largest first. A test run's leftovers are many small files, so a refusal
/// that named only [`largest`] files would leave the agent to take a census.
pub fn largest_directories(tree: &Tree, count: usize) -> Vec<ScratchEntrySize> {
    let mut sums: BTreeMap<&[u8], u64> = BTreeMap::new();
    for (path, blob) in tree {
        if let Some(slash) = path.iter().position(|byte| *byte == b'/') {
            let sum = sums.entry(&path[..slash]).or_default();
            *sum = sum.saturating_add(blob.size);
        }
    }
    let mut directories: Vec<(&[u8], u64)> = sums.into_iter().collect();
    directories.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0)));
    directories
        .into_iter()
        .take(count)
        .map(|(path, bytes)| ScratchEntrySize {
            path: format!("{}/", String::from_utf8_lossy(path)),
            bytes,
        })
        .collect()
}

/// Whether the ceiling refuses this plan.
///
/// The check is against the MERGED size, because that is what the scratch
/// would hold. A write-back that does not grow the scratch is never refused,
/// so lowering the ceiling cannot trap a node that is already over it: it can
/// always shrink. A file over the transport's per-pack cap is refused at any
/// ceiling, because it could not be moved.
pub fn refusal(
    plan: &MergePlan,
    head_bytes: u64,
    ceiling_bytes: u64,
    owner_uri: &str,
) -> Option<ScratchRefusal> {
    let oversized: Vec<ScratchEntrySize> = plan
        .incoming
        .iter()
        .filter(|(_, blob)| blob.size > NODE_SCRATCH_MAX_FILE_BYTES)
        .map(|(path, blob)| ScratchEntrySize {
            path: String::from_utf8_lossy(path).into_owned(),
            bytes: blob.size,
        })
        .collect();
    let over = plan.merged_bytes > ceiling_bytes && plan.merged_bytes > head_bytes;
    (over || !oversized.is_empty()).then(|| ScratchRefusal {
        owner_uri: owner_uri.to_owned(),
        merged_bytes: plan.merged_bytes,
        ceiling_bytes,
        largest: largest(&plan.result, LARGEST_NAMED),
        largest_directories: largest_directories(&plan.result, LARGEST_NAMED),
        oversized,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(oid: &str, size: u64) -> ScratchBlob {
        ScratchBlob {
            oid: oid.repeat(40 / oid.len()),
            mode: 0o100644,
            size,
        }
    }

    fn tree(entries: &[(&str, ScratchBlob)]) -> Tree {
        entries
            .iter()
            .map(|(path, blob)| (path.as_bytes().to_vec(), blob.clone()))
            .collect()
    }

    fn change(path: &str, base: Option<ScratchBlob>, new: Option<ScratchBlob>) -> ScratchChange {
        ScratchChange {
            path: ScratchPath::from(path),
            base,
            new,
        }
    }

    #[test]
    fn a_head_still_at_the_base_fast_forwards_to_the_snapshot() {
        let head = tree(&[
            ("keep", blob("a", 1)),
            ("edit", blob("b", 2)),
            ("gone", blob("c", 3)),
        ]);
        let plan = plan(
            &head,
            &[
                change("edit", Some(blob("b", 2)), Some(blob("d", 4))),
                change("gone", Some(blob("c", 3)), None),
                change("new", None, Some(blob("e", 5))),
            ],
            "w1",
        );
        assert_eq!(
            plan.result,
            tree(&[
                ("keep", blob("a", 1)),
                ("edit", blob("d", 4)),
                ("new", blob("e", 5))
            ])
        );
        assert!(plan.conflicts.is_empty());
        assert_eq!(plan.merged_bytes, 10);
        assert!(plan.changed);
        assert_eq!(plan.added_bytes(&head), 9);
    }

    #[test]
    fn disjoint_writers_both_land() {
        // The head moved on (another writer added `theirs`) since this
        // machine synced; this batch added `mine`.
        let head = tree(&[("shared", blob("a", 1)), ("theirs", blob("b", 2))]);
        let plan = plan(&head, &[change("mine", None, Some(blob("c", 3)))], "w1");
        assert_eq!(
            plan.result,
            tree(&[
                ("shared", blob("a", 1)),
                ("theirs", blob("b", 2)),
                ("mine", blob("c", 3))
            ])
        );
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn a_path_changed_differently_on_both_sides_keeps_both_versions() {
        let head = tree(&[("report.csv", blob("b", 2))]);
        let plan = plan(
            &head,
            &[change("report.csv", Some(blob("a", 1)), Some(blob("c", 3)))],
            "run12345",
        );
        assert_eq!(
            plan.result,
            tree(&[
                ("report.csv", blob("b", 2)),
                ("report.csv~conflict-run12345", blob("c", 3)),
            ])
        );
        assert_eq!(
            plan.conflicts,
            vec![ScratchConflict {
                path: ScratchPath::from("report.csv"),
                written_as: ScratchPath::from("report.csv~conflict-run12345"),
            }]
        );
        // Folding the same write-back again adds nothing new.
        let again = super::plan(
            &plan.result,
            &[change("report.csv", Some(blob("a", 1)), Some(blob("c", 3)))],
            "run12345",
        );
        assert_eq!(again.result, plan.result);
        assert!(!again.changed);
    }

    #[test]
    fn identical_changes_on_both_sides_are_not_a_conflict() {
        let head = tree(&[("same", blob("c", 3))]);
        let plan = plan(
            &head,
            &[change("same", Some(blob("a", 1)), Some(blob("c", 3)))],
            "w",
        );
        assert!(plan.conflicts.is_empty());
        assert!(!plan.changed);
    }

    #[test]
    fn a_delete_against_a_change_keeps_the_change() {
        // This batch deleted what the head changed.
        let head = tree(&[("data", blob("b", 2))]);
        let kept = plan(&head, &[change("data", Some(blob("a", 1)), None)], "w");
        assert_eq!(kept.result, head);
        // The head deleted what this batch changed.
        let head = tree(&[]);
        let kept = plan(
            &head,
            &[change("data", Some(blob("a", 1)), Some(blob("c", 3)))],
            "w",
        );
        assert_eq!(kept.result, tree(&[("data", blob("c", 3))]));
        assert!(kept.conflicts.is_empty());
    }

    #[test]
    fn a_file_directory_clash_moves_the_incoming_side() {
        let head = tree(&[("out", blob("a", 1))]);
        let plan = plan(
            &head,
            &[change("out/part-1", None, Some(blob("b", 2)))],
            "w",
        );
        assert_eq!(
            plan.result,
            tree(&[
                ("out", blob("a", 1)),
                ("out~conflict-w/part-1", blob("b", 2))
            ])
        );
        assert_eq!(plan.conflicts.len(), 1);
    }

    #[test]
    fn the_ceiling_is_checked_against_the_merged_size() {
        // 6 bytes already in the head, 5 more incoming from this writer: the
        // merged scratch holds 11, past a ceiling of 10 that neither side
        // crosses alone.
        let head = tree(&[("theirs", blob("a", 6))]);
        let plan = plan(&head, &[change("mine", None, Some(blob("b", 5)))], "w");
        let refusal = refusal(&plan, 6, 10, "cairn://p/x/1/1/builder").expect("refused");
        assert_eq!(refusal.merged_bytes, 11);
        assert_eq!(refusal.ceiling_bytes, 10);
        assert_eq!(refusal.owner_uri, "cairn://p/x/1/1/builder");
        assert_eq!(refusal.largest[0].path, "theirs");
        assert!(super::refusal(&plan, 6, 11, "o").is_none());
    }

    #[test]
    fn a_refusal_names_the_directory_many_small_files_add_up_in() {
        let head = tree(&[
            ("export.tar", blob("a", 40)),
            ("kept/.tmpAbCdEf/one", blob("b", 10)),
            ("kept/.tmpAbCdEf/two", blob("c", 10)),
            ("kept/three", blob("d", 25)),
            ("notes/a.md", blob("e", 5)),
        ]);
        let plan = plan(
            &head,
            &[change("notes/b.md", None, Some(blob("f", 1)))],
            "w",
        );
        let refusal = refusal(&plan, 90, 10, "o").expect("refused");
        assert_eq!(refusal.largest[0].path, "export.tar");
        assert_eq!(
            refusal.largest_directories,
            vec![
                ScratchEntrySize {
                    path: "kept/".into(),
                    bytes: 45
                },
                ScratchEntrySize {
                    path: "notes/".into(),
                    bytes: 6
                },
            ]
        );
    }

    #[test]
    fn a_shrink_is_allowed_under_a_lowered_ceiling() {
        let head = tree(&[("big", blob("a", 100)), ("small", blob("b", 10))]);
        let plan = plan(&head, &[change("small", Some(blob("b", 10)), None)], "w");
        assert_eq!(plan.merged_bytes, 100);
        assert!(
            refusal(&plan, 110, 50, "o").is_none(),
            "a write-back that shrinks the scratch must never be refused"
        );
    }

    #[test]
    fn a_file_past_the_transport_cap_is_refused_at_any_ceiling() {
        let huge = NODE_SCRATCH_MAX_FILE_BYTES + 1;
        let plan = plan(
            &Tree::new(),
            &[change("export.db", None, Some(blob("a", huge)))],
            "w",
        );
        let refusal = refusal(&plan, 0, u64::MAX, "o").expect("refused");
        assert_eq!(refusal.oversized[0].path, "export.db");
    }
}
