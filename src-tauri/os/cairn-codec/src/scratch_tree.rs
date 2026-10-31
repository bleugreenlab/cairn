//! Git plumbing for node scratch trees, shared by the runner's store and every
//! executor's replica (cairn/5253).
//!
//! A node scratch is a directory tree stored as a Git tree. Each state is a
//! parentless commit with a fixed identity and date, so two machines that
//! record the same files record the same commit. Every invocation runs with
//! [`SCRATCH_GIT_CONFIG`], uses `-z` plumbing throughout so any file name
//! survives (this module never goes through [`crate::objects`], whose reader
//! rejects non-UTF-8 names), and goes through Git's CLI like the rest of
//! [`crate::transfer`].

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// The empty tree's object id, which every Git repository can name.
pub const EMPTY_TREE: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

/// Configuration every scratch Git invocation carries. Scratch files are data:
/// no line-ending conversion, no signing, and no automatic gc, which would
/// prune blobs only a replica's own index names.
pub const SCRATCH_GIT_CONFIG: [&str; 10] = [
    "-c",
    "core.autocrlf=false",
    "-c",
    "core.safecrlf=false",
    "-c",
    "commit.gpgSign=false",
    "-c",
    "i18n.commitEncoding=UTF-8",
    "-c",
    "gc.auto=0",
];

/// The identity and date of every scratch commit. Fixed, so a runner and an
/// executor that build a commit from the same tree build the same commit.
pub const SCRATCH_COMMIT_ENV: [(&str, &str); 6] = [
    ("GIT_AUTHOR_NAME", "Cairn node scratch"),
    ("GIT_AUTHOR_EMAIL", "scratch@cairn.invalid"),
    ("GIT_AUTHOR_DATE", "@1000000000 +0000"),
    ("GIT_COMMITTER_NAME", "Cairn node scratch"),
    ("GIT_COMMITTER_EMAIL", "scratch@cairn.invalid"),
    ("GIT_COMMITTER_DATE", "@1000000000 +0000"),
];

const SCRATCH_COMMIT_MESSAGE: &str = "node scratch";

/// One file of a scratch tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeFile {
    pub path: Vec<u8>,
    /// `0o100644`, `0o100755`, or `0o120000`.
    pub mode: u32,
    pub oid: String,
    pub size: u64,
}

/// One path that differs between two trees.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeChange {
    pub path: Vec<u8>,
    /// `(mode, oid)` before; `None` when the path was added.
    pub old: Option<(u32, String)>,
    /// `(mode, oid)` after; `None` when the path was deleted.
    pub new: Option<(u32, String)>,
}

/// A scratch repository: a bare object store, optionally paired with a work
/// tree and an index of its own.
#[derive(Clone, Debug)]
pub struct ScratchRepo {
    git_dir: PathBuf,
    work_tree: Option<PathBuf>,
    index: Option<PathBuf>,
}

impl ScratchRepo {
    pub fn bare(git_dir: impl Into<PathBuf>) -> Self {
        Self {
            git_dir: git_dir.into(),
            work_tree: None,
            index: None,
        }
    }

    /// A store whose index sits outside the work tree, so whatever runs in the
    /// work tree sees no `.git` at all.
    pub fn with_work_tree(
        git_dir: impl Into<PathBuf>,
        work_tree: impl Into<PathBuf>,
        index: impl Into<PathBuf>,
    ) -> Self {
        Self {
            git_dir: git_dir.into(),
            work_tree: Some(work_tree.into()),
            index: Some(index.into()),
        }
    }

    pub fn git_dir(&self) -> &Path {
        &self.git_dir
    }

    pub fn objects_dir(&self) -> PathBuf {
        self.git_dir.join("objects")
    }

    /// Create the bare store if it does not exist yet. Idempotent, and safe to
    /// call concurrently for one path.
    ///
    /// Two writers can reach a store that does not exist yet at the same time
    /// (two machines' first write-backs), and two `git init`s into one
    /// directory race on its config lock. So each caller builds a complete
    /// store aside and renames it into place: the rename is atomic, the store
    /// at `git_dir` is always whole, and a caller that loses finds the
    /// winner's and drops its own.
    pub fn ensure(&self) -> Result<(), String> {
        if self.is_initialized() {
            return Ok(());
        }
        let parent = self
            .git_dir
            .parent()
            .ok_or_else(|| format!("{} has no parent directory", self.git_dir.display()))?;
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
        let staging = tempfile::Builder::new()
            .prefix(".init-")
            .tempdir_in(parent)
            .map_err(|error| format!("stage a store in {}: {error}", parent.display()))?;
        let staged = staging.path().join("store.git");
        // host-default-branch: a bare store addressed by object id; its HEAD only
        // marks a finished create and is never resolved to a branch.
        let output = Command::new("git")
            .args(["init", "--bare", "--quiet"])
            .arg(&staged)
            .stdin(Stdio::null())
            .output()
            .map_err(|error| format!("spawn git init: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "git init {}: {}",
                staged.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        let staged_repo = ScratchRepo::bare(&staged);
        staged_repo.run(["config", "gc.auto", "0"], None)?;
        staged_repo.run(["config", "core.logAllRefUpdates", "false"], None)?;
        // Nothing ever initializes in place, so a directory here without a
        // HEAD is debris from an interrupted create, never a store in progress.
        if self.git_dir.exists() && !self.is_initialized() {
            let _ = std::fs::remove_dir_all(&self.git_dir);
        }
        match std::fs::rename(&staged, &self.git_dir) {
            Ok(()) => Ok(()),
            Err(_) if self.is_initialized() => Ok(()),
            Err(error) => Err(format!(
                "move a new store into {}: {error}",
                self.git_dir.display()
            )),
        }
    }

    fn is_initialized(&self) -> bool {
        self.git_dir.join("HEAD").is_file()
    }

    fn command(&self) -> Command {
        let mut command = Command::new("git");
        command
            .args(SCRATCH_GIT_CONFIG)
            .env("GIT_DIR", &self.git_dir)
            // A scratch store is never anyone's repository: no user or system
            // configuration decides how its bytes are recorded.
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env_remove("GIT_INDEX_FILE")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_OBJECT_DIRECTORY")
            .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
            .stdin(Stdio::null());
        if let Some(work_tree) = &self.work_tree {
            command
                .env("GIT_WORK_TREE", work_tree)
                .current_dir(work_tree);
        } else {
            command.current_dir(&self.git_dir);
        }
        if let Some(index) = &self.index {
            command.env("GIT_INDEX_FILE", index);
        }
        command
    }

    /// Run Git with optional stdin and return its stdout.
    pub fn run<I, S>(&self, args: I, stdin: Option<&[u8]>) -> Result<Vec<u8>, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.run_with(self.command(), args, stdin)
    }

    fn run_with<I, S>(
        &self,
        mut command: Command,
        args: I,
        stdin: Option<&[u8]>,
    ) -> Result<Vec<u8>, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args: Vec<_> = args
            .into_iter()
            .map(|arg| arg.as_ref().to_owned())
            .collect();
        command.args(&args);
        if stdin.is_some() {
            command.stdin(Stdio::piped());
        }
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("spawn git: {error}"))?;
        if let Some(input) = stdin {
            let mut pipe = child.stdin.take().expect("stdin is piped");
            let input = input.to_vec();
            // Written from its own thread, so a large input cannot deadlock
            // against Git filling its stdout.
            let writer = std::thread::spawn(move || pipe.write_all(&input));
            let output = child
                .wait_with_output()
                .map_err(|error| format!("wait for git: {error}"))?;
            writer
                .join()
                .map_err(|_| "git stdin writer panicked".to_string())?
                .map_err(|error| format!("write git stdin: {error}"))?;
            return finish(&args, output);
        }
        let output = child
            .wait_with_output()
            .map_err(|error| format!("wait for git: {error}"))?;
        finish(&args, output)
    }

    /// Whether `commit` names a commit this store holds.
    pub fn has_commit(&self, commit: &str) -> bool {
        valid_oid(commit)
            && self
                .run(["cat-file", "-e", &format!("{commit}^{{commit}}")], None)
                .is_ok()
    }

    /// Every file of a commit's tree, with its size. `None` is the empty tree.
    /// Every blob must be present: Git cannot size one it lacks.
    pub fn files(&self, commit: Option<&str>) -> Result<Vec<TreeFile>, String> {
        self.list(commit, true)
    }

    /// Every file of a commit's tree without sizes (each `size` is 0), which
    /// needs only the trees: what a replica lists to learn which blobs to ask
    /// for.
    pub fn file_ids(&self, commit: Option<&str>) -> Result<Vec<TreeFile>, String> {
        self.list(commit, false)
    }

    fn list(&self, commit: Option<&str>, sized: bool) -> Result<Vec<TreeFile>, String> {
        let Some(commit) = commit else {
            return Ok(Vec::new());
        };
        let listed = if sized {
            self.run(["ls-tree", "-r", "-l", "-z", "--full-tree", commit], None)?
        } else {
            self.run(["ls-tree", "-r", "-z", "--full-tree", commit], None)?
        };
        let fields_per_record = if sized { 4 } else { 3 };
        let mut files = Vec::new();
        for record in listed.split(|byte| *byte == 0).filter(|r| !r.is_empty()) {
            let tab = record
                .iter()
                .position(|byte| *byte == b'\t')
                .ok_or_else(|| "ls-tree record without a path".to_string())?;
            let meta = std::str::from_utf8(&record[..tab])
                .map_err(|_| "ls-tree metadata is not text".to_string())?;
            let fields: Vec<&str> = meta.split_whitespace().collect();
            if fields.len() != fields_per_record || fields[1] != "blob" {
                // Gitlinks never enter a scratch; anything else is not a file.
                continue;
            }
            files.push(TreeFile {
                path: record[tab + 1..].to_vec(),
                mode: u32::from_str_radix(fields[0], 8)
                    .map_err(|error| format!("ls-tree mode {}: {error}", fields[0]))?,
                oid: fields[2].to_owned(),
                size: if sized {
                    fields[3]
                        .parse()
                        .map_err(|error| format!("ls-tree size {}: {error}", fields[3]))?
                } else {
                    0
                },
            });
        }
        Ok(files)
    }

    /// The size of each object, or `None` for one this store lacks.
    pub fn sizes(&self, oids: &[String]) -> Result<Vec<Option<u64>>, String> {
        if oids.is_empty() {
            return Ok(Vec::new());
        }
        let mut input = String::new();
        for oid in oids {
            if !valid_oid(oid) {
                return Err(format!("not an object id: {oid:?}"));
            }
            input.push_str(oid);
            input.push('\n');
        }
        let output = self.run(
            ["cat-file", "--batch-check=%(objectname) %(objectsize)"],
            Some(input.as_bytes()),
        )?;
        let text = String::from_utf8_lossy(&output);
        let lines: Vec<&str> = text.lines().collect();
        if lines.len() != oids.len() {
            return Err("cat-file --batch-check answered a different number of objects".into());
        }
        Ok(lines
            .iter()
            .map(|line| {
                let mut fields = line.split_whitespace();
                let _oid = fields.next();
                fields.next().and_then(|size| size.parse().ok())
            })
            .collect())
    }

    /// The objects of `oids` this store lacks, in order.
    pub fn missing(&self, oids: &[String]) -> Result<Vec<String>, String> {
        Ok(oids
            .iter()
            .zip(self.sizes(oids)?)
            .filter(|(_, size)| size.is_none())
            .map(|(oid, _)| oid.clone())
            .collect())
    }

    /// Validate a pack and install it into this store.
    pub fn install_pack(&self, pack: &[u8]) -> Result<u64, String> {
        let validated =
            crate::transfer::validate_pack(pack, crate::transfer::PackLimits::default())?;
        crate::transfer::install_pack(&self.objects_dir(), &validated)?;
        Ok(validated.manifest.object_count)
    }

    /// A pack of exactly `oids`.
    pub fn pack_of(&self, oids: &[String]) -> Result<Vec<u8>, String> {
        crate::transfer::build_pack_of_objects(&self.git_dir, oids).map(|(pack, _)| pack)
    }

    /// The commit and trees of `want` that `have` does not already carry.
    pub fn skeleton_objects(&self, want: &str, have: Option<&str>) -> Result<Vec<String>, String> {
        if !valid_oid(want) {
            return Err(format!("not a commit id: {want:?}"));
        }
        let mut args = vec![
            "rev-list".to_owned(),
            "--objects".into(),
            "--no-object-names".into(),
            "--filter=blob:none".into(),
            want.to_owned(),
        ];
        if let Some(have) = have.filter(|have| *have != want && self.has_commit(have)) {
            args.push("--not".into());
            args.push(have.to_owned());
        }
        let listed = self.run(&args, None)?;
        Ok(String::from_utf8_lossy(&listed)
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .map(str::to_owned)
            .collect())
    }

    /// Write a tree from a flat list of files, verifying every blob is
    /// present. Uses a throwaway index, never the store's own.
    pub fn write_tree(&self, files: &[TreeFile]) -> Result<String, String> {
        if files.is_empty() {
            return self.ensure_empty_tree();
        }
        let scratch = tempfile::Builder::new()
            .prefix(".scratch-index-")
            .tempdir_in(&self.git_dir)
            .map_err(|error| format!("create a throwaway index: {error}"))?;
        let index = scratch.path().join("index");
        let mut input = Vec::new();
        for file in files {
            input.extend_from_slice(format!("{:o} {}\t", file.mode, file.oid).as_bytes());
            input.extend_from_slice(&file.path);
            input.push(0);
        }
        let mut command = self.command();
        command.env("GIT_INDEX_FILE", &index);
        self.run_with(
            command,
            ["update-index", "-z", "--index-info"],
            Some(&input),
        )?;
        let mut command = self.command();
        command.env("GIT_INDEX_FILE", &index);
        let tree = self.run_with(command, ["write-tree"], None)?;
        parse_oid(&tree)
    }

    /// Make sure the empty tree object exists here, and name it.
    pub fn ensure_empty_tree(&self) -> Result<String, String> {
        let tree = self.run(["mktree", "-z"], Some(b""))?;
        parse_oid(&tree)
    }

    /// The tree of a commit.
    pub fn tree_of(&self, commit: &str) -> Result<String, String> {
        parse_oid(&self.run(["rev-parse", &format!("{commit}^{{tree}}")], None)?)
    }

    /// The scratch commit of `tree`: parentless, with the fixed identity.
    pub fn commit(&self, tree: &str) -> Result<String, String> {
        let mut command = self.command();
        for (name, value) in SCRATCH_COMMIT_ENV {
            command.env(name, value);
        }
        let commit = self.run_with(
            command,
            ["commit-tree", tree, "-m", SCRATCH_COMMIT_MESSAGE],
            None,
        )?;
        parse_oid(&commit)
    }

    /// What changed between two trees, path by path, without rename detection.
    pub fn changes(&self, from_tree: &str, to_tree: &str) -> Result<Vec<TreeChange>, String> {
        let raw = self.run(
            [
                "diff-tree",
                "-r",
                "-z",
                "--raw",
                "--no-abbrev",
                "--no-renames",
                from_tree,
                to_tree,
            ],
            None,
        )?;
        let mut changes = Vec::new();
        let mut records = raw.split(|byte| *byte == 0);
        while let Some(meta) = records.next() {
            if meta.is_empty() {
                continue;
            }
            let path = records
                .next()
                .ok_or_else(|| "diff-tree record without a path".to_string())?;
            let meta = std::str::from_utf8(meta)
                .map_err(|_| "diff-tree metadata is not text".to_string())?;
            let fields: Vec<&str> = meta.trim_start_matches(':').split_whitespace().collect();
            if fields.len() < 5 {
                return Err(format!("unreadable diff-tree record {meta:?}"));
            }
            let side = |mode: &str, oid: &str| -> Result<Option<(u32, String)>, String> {
                let mode = u32::from_str_radix(mode, 8)
                    .map_err(|error| format!("diff-tree mode {mode}: {error}"))?;
                Ok((mode != 0 && mode != 0o160000).then(|| (mode, oid.to_owned())))
            };
            changes.push(TreeChange {
                path: path.to_vec(),
                old: side(fields[0], fields[2])?,
                new: side(fields[1], fields[3])?,
            });
        }
        Ok(changes)
    }

    /// Point `refs/heads/head` at `commit`, or remove it. The ref exists only
    /// so gc keeps the head's objects; the caller's authority decides it.
    pub fn set_head_ref(&self, commit: Option<&str>) -> Result<(), String> {
        match commit {
            Some(commit) => self.run(["update-ref", "refs/heads/head", commit], None),
            None => self.run(["update-ref", "-d", "refs/heads/head"], None),
        }
        .map(|_| ())
    }

    /// Bytes the store holds on disk and how many packs, from `count-objects`.
    pub fn footprint(&self) -> Result<(u64, u64), String> {
        let output = self.run(["count-objects", "-v"], None)?;
        let text = String::from_utf8_lossy(&output);
        let field = |name: &str| -> u64 {
            text.lines()
                .find_map(|line| line.strip_prefix(name))
                .and_then(|value| value.trim().parse().ok())
                .unwrap_or(0)
        };
        Ok((
            (field("size:") + field("size-pack:")) * 1024,
            field("packs:"),
        ))
    }
}

fn finish(args: &[std::ffi::OsString], output: std::process::Output) -> Result<Vec<u8>, String> {
    if output.status.success() {
        return Ok(output.stdout);
    }
    let verb = args
        .iter()
        .map(|arg| arg.to_string_lossy())
        .find(|arg| !arg.starts_with('-'))
        .unwrap_or_default()
        .into_owned();
    Err(format!(
        "git {verb}: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

fn parse_oid(output: &[u8]) -> Result<String, String> {
    let oid = String::from_utf8_lossy(output).trim().to_owned();
    if valid_oid(&oid) {
        Ok(oid)
    } else {
        Err(format!("git answered {oid:?} where an object id belongs"))
    }
}

pub fn valid_oid(oid: &str) -> bool {
    oid.len() == 40 && oid.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Index files by path, for merging and for diffing listings.
pub fn by_path(files: Vec<TreeFile>) -> BTreeMap<Vec<u8>, TreeFile> {
    files
        .into_iter()
        .map(|file| (file.path.clone(), file))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(repo: &ScratchRepo, bytes: &[u8]) -> String {
        parse_oid(
            &repo
                .run(["hash-object", "-w", "--stdin"], Some(bytes))
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn a_tree_round_trips_through_files_and_commits_deterministically() {
        let temp = tempfile::tempdir().unwrap();
        let one = ScratchRepo::bare(temp.path().join("one.git"));
        let two = ScratchRepo::bare(temp.path().join("two.git"));
        one.ensure().unwrap();
        two.ensure().unwrap();
        let mut files = Vec::new();
        for repo in [&one, &two] {
            let data = hash(repo, b"rows\n");
            let script = hash(repo, b"#!/bin/sh\n");
            let link = hash(repo, b"data.csv");
            files = vec![
                TreeFile {
                    path: b"data.csv".to_vec(),
                    mode: 0o100644,
                    oid: data,
                    size: 5,
                },
                TreeFile {
                    path: b"bin/run".to_vec(),
                    mode: 0o100755,
                    oid: script,
                    size: 10,
                },
                TreeFile {
                    path: b"latest".to_vec(),
                    mode: 0o120000,
                    oid: link,
                    size: 8,
                },
                TreeFile {
                    path: b"caf\xe9.bin".to_vec(),
                    mode: 0o100644,
                    oid: hash(repo, b"latin-1 name"),
                    size: 12,
                },
            ];
        }
        let commits: Vec<String> = [&one, &two]
            .iter()
            .map(|repo| repo.commit(&repo.write_tree(&files).unwrap()).unwrap())
            .collect();
        assert_eq!(
            commits[0], commits[1],
            "same files must make the same commit"
        );
        let mut listed = one.files(Some(&commits[0])).unwrap();
        listed.sort_by(|a, b| a.path.cmp(&b.path));
        let mut expected = files.clone();
        expected.sort_by(|a, b| a.path.cmp(&b.path));
        assert_eq!(listed, expected);
        assert!(one.has_commit(&commits[0]));
        assert_eq!(
            one.missing(&[files[0].oid.clone(), "f".repeat(40)])
                .unwrap(),
            vec!["f".repeat(40)]
        );
    }

    #[test]
    fn concurrent_creates_of_one_store_all_succeed_and_leave_one_store() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("project").join("store.git");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let creators: Vec<_> = (0..8)
            .map(|_| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    ScratchRepo::bare(path).ensure()
                })
            })
            .collect();
        for creator in creators {
            creator.join().unwrap().unwrap();
        }
        let repo = ScratchRepo::bare(&path);
        assert_eq!(
            repo.run(["config", "gc.auto"], None).unwrap(),
            b"0\n".to_vec()
        );
        let left: Vec<_> = std::fs::read_dir(temp.path().join("project"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(left, vec![std::ffi::OsString::from("store.git")]);
    }

    #[test]
    fn write_tree_refuses_a_missing_blob() {
        let temp = tempfile::tempdir().unwrap();
        let repo = ScratchRepo::bare(temp.path().join("store.git"));
        repo.ensure().unwrap();
        let error = repo
            .write_tree(&[TreeFile {
                path: b"absent".to_vec(),
                mode: 0o100644,
                oid: "e".repeat(40),
                size: 1,
            }])
            .unwrap_err();
        assert!(
            error.contains("write-tree") || error.contains("update-index"),
            "{error}"
        );
    }

    #[test]
    fn changes_and_skeletons_move_only_what_differs() {
        let temp = tempfile::tempdir().unwrap();
        let repo = ScratchRepo::bare(temp.path().join("store.git"));
        repo.ensure().unwrap();
        let kept = hash(&repo, b"kept");
        let old = hash(&repo, b"old");
        let new = hash(&repo, b"new");
        let first = repo
            .write_tree(&[
                TreeFile {
                    path: b"kept".to_vec(),
                    mode: 0o100644,
                    oid: kept.clone(),
                    size: 4,
                },
                TreeFile {
                    path: b"dir/edit".to_vec(),
                    mode: 0o100644,
                    oid: old.clone(),
                    size: 3,
                },
            ])
            .unwrap();
        let second = repo
            .write_tree(&[
                TreeFile {
                    path: b"kept".to_vec(),
                    mode: 0o100644,
                    oid: kept,
                    size: 4,
                },
                TreeFile {
                    path: b"dir/edit".to_vec(),
                    mode: 0o100644,
                    oid: new.clone(),
                    size: 3,
                },
                TreeFile {
                    path: b"added".to_vec(),
                    mode: 0o100755,
                    oid: old.clone(),
                    size: 3,
                },
            ])
            .unwrap();
        let mut changes = repo.changes(&first, &second).unwrap();
        changes.sort_by(|a, b| a.path.cmp(&b.path));
        assert_eq!(
            changes,
            vec![
                TreeChange {
                    path: b"added".to_vec(),
                    old: None,
                    new: Some((0o100755, old.clone()))
                },
                TreeChange {
                    path: b"dir/edit".to_vec(),
                    old: Some((0o100644, old)),
                    new: Some((0o100644, new)),
                },
            ]
        );
        let empty = repo.ensure_empty_tree().unwrap();
        assert_eq!(empty, EMPTY_TREE);
        let c1 = repo.commit(&first).unwrap();
        let c2 = repo.commit(&second).unwrap();
        let skeleton = repo.skeleton_objects(&c2, Some(&c1)).unwrap();
        assert!(skeleton.contains(&c2));
        assert!(skeleton.contains(&second));
        assert!(
            !skeleton.contains(&first),
            "a tree the have carries must not move"
        );
        assert_eq!(
            skeleton.len(),
            3,
            "commit, root tree, changed subtree: {skeleton:?}"
        );
    }
}
