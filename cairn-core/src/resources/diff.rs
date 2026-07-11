//! Layered node workspace diff resource.

use std::path::{Component, Path, PathBuf};

use cairn_common::query::QueryParam;
use globset::GlobBuilder;

use super::common::{connect_and_find_node_job, find_query_value};
use super::files::{
    dedupe_file_changes_by_path, load_job_file_changes, push_file_change_table, FileChangeRow,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{
    count_commits_ahead, list_range_commits, render_range_diff, render_range_file_diffs,
    ObjectStore, RowExt,
};

const FINDING_CAP: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiffView {
    Summary,
    Commits,
    Patch,
    Symbols,
    Check,
}

#[derive(Debug)]
struct DiffRequest<'a> {
    view: DiffView,
    file: Option<&'a str>,
    glob: Option<&'a str>,
}

#[derive(Debug)]
struct DiffCoords {
    worktree_path: Option<String>,
    execution_id: Option<String>,
    repo_path: String,
    base_branch: String,
    base_commit: Option<String>,
    branch: Option<String>,
}

#[derive(Debug)]
struct DisplayCommit {
    commit_id: String,
    change_id: Option<String>,
    description: String,
    author: String,
    timestamp: String,
    working_copy: bool,
}

#[derive(Debug, Default)]
struct CheckData {
    workspace_state: String,
    dirty_paths: Vec<String>,
    conflicted_files: Vec<String>,
    conflicted_commits: Vec<crate::jj::ConflictedCommit>,
    marker_findings: Vec<LineFinding>,
    marker_total: usize,
    whitespace_findings: Vec<LineFinding>,
    whitespace_total: usize,
}

enum DiffContentSource {
    Live {
        worktree: PathBuf,
    },
    Archived {
        repo_path: PathBuf,
        pack: Option<(Vec<u8>, Vec<u8>)>,
        tip_sha: String,
    },
    Unavailable {
        reason: String,
    },
}

enum DiffContentReader<'a> {
    Live {
        worktree: &'a Path,
    },
    Archived {
        store: Box<ObjectStore>,
        tip_sha: &'a str,
    },
    Unavailable {
        reason: String,
    },
}

impl DiffContentSource {
    fn reader(&self) -> DiffContentReader<'_> {
        match self {
            Self::Live { worktree } => DiffContentReader::Live { worktree },
            Self::Archived {
                repo_path,
                pack,
                tip_sha,
            } => match ObjectStore::new(repo_path, pack.clone()) {
                Ok(store) => DiffContentReader::Archived {
                    store: Box::new(store),
                    tip_sha,
                },
                Err(error) => DiffContentReader::Unavailable {
                    reason: format!("archived source store could not be opened: {error}"),
                },
            },
            Self::Unavailable { reason } => DiffContentReader::Unavailable {
                reason: reason.clone(),
            },
        }
    }
}

impl DiffContentReader<'_> {
    fn read_path(&self, path: &str) -> Result<Vec<u8>, String> {
        match self {
            Self::Live { worktree } => {
                let relative = Path::new(path);
                if relative.as_os_str().is_empty()
                    || relative.is_absolute()
                    || !relative
                        .components()
                        .all(|component| matches!(component, Component::Normal(_)))
                {
                    return Err("invalid non-relative changed path".to_string());
                }
                read_live_source_no_follow(worktree, relative)
            }
            Self::Archived { store, tip_sha } => store
                .resolve_path_at_commit(tip_sha, path)
                .map_err(|error| format!("archived source could not be resolved: {error}")),
            Self::Unavailable { reason } => Err(reason.clone()),
        }
    }
}

#[cfg(unix)]
fn read_live_source_no_follow(worktree: &Path, relative: &Path) -> Result<Vec<u8>, String> {
    use std::ffi::CString;
    use std::fs::File;
    use std::io::Read;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;

    fn owned_fd(fd: libc::c_int, context: &str) -> Result<OwnedFd, String> {
        if fd < 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ELOOP) {
                return Err("live source is a symlink and is not analyzed".to_string());
            }
            return Err(format!("live source could not open {context}: {error}"));
        }
        // SAFETY: `fd` is a newly opened descriptor and ownership transfers to
        // the returned `OwnedFd`, which closes it exactly once.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    let root = CString::new(worktree.as_os_str().as_bytes())
        .map_err(|_| "live worktree path contains a NUL byte".to_string())?;
    // SAFETY: `root` is a valid NUL-terminated path. The returned descriptor is
    // immediately wrapped in `OwnedFd`; `O_NOFOLLOW` rejects a symlink root.
    let root_fd = unsafe {
        libc::open(
            root.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    let mut directory = owned_fd(root_fd, "worktree directory")?;
    let components = relative
        .components()
        .map(|component| match component {
            Component::Normal(name) => CString::new(name.as_bytes())
                .map_err(|_| "live source path contains a NUL byte".to_string()),
            _ => Err("invalid non-relative changed path".to_string()),
        })
        .collect::<Result<Vec<_>, _>>()?;

    for (index, component) in components.iter().enumerate() {
        let final_component = index + 1 == components.len();
        let flags = if final_component {
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK
        } else {
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW
        };
        // SAFETY: `directory` remains open for this call, `component` is a valid
        // NUL-terminated single path component, and the result is immediately
        // transferred to `OwnedFd`. Traversal never re-resolves an inspected path.
        let fd = unsafe { libc::openat(directory.as_raw_fd(), component.as_ptr(), flags) };
        let opened = owned_fd(fd, "changed path component")?;
        if final_component {
            let mut file = File::from(opened);
            let metadata = file
                .metadata()
                .map_err(|error| format!("live source metadata is unavailable: {error}"))?;
            if !metadata.is_file() {
                return Err("live source is not a regular file".to_string());
            }
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)
                .map_err(|error| format!("live source could not be read: {error}"))?;
            return Ok(bytes);
        }
        directory = opened;
    }

    Err("invalid empty changed path".to_string())
}

#[cfg(not(unix))]
fn read_live_source_no_follow(_worktree: &Path, _relative: &Path) -> Result<Vec<u8>, String> {
    Err("atomic no-follow live source reads are unavailable on this platform".to_string())
}

struct DiffData {
    source_note: Option<String>,
    content_source: DiffContentSource,
    base_branch: String,
    base_commit: Option<String>,
    current_revision: Option<String>,
    branch: Option<String>,
    commits_ahead: Option<i32>,
    rows: Vec<FileChangeRow>,
    patch: Option<String>,
    commits: Option<Vec<DisplayCommit>>,
    check: CheckData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LineFinding {
    path: String,
    line: usize,
    detail: String,
}

pub(super) async fn read_node_diff(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    params: &[QueryParam],
) -> String {
    let request = match parse_diff_request(params) {
        Ok(request) => request,
        Err(error) => return error,
    };
    let db = orch.db.for_project(project).await;
    let (conn, job) = match connect_and_find_node_job(&db, project, number, exec_seq, node_id).await
    {
        Ok(found) => found,
        Err(error) => return error,
    };
    let coords = match load_diff_coords(&conn, &job.id).await {
        Ok(coords) => coords,
        Err(error) => return error,
    };

    let mut data = match load_live_data(orch, &coords).await {
        Some(data) => data,
        None => match load_archived_data(&conn, &coords, request.view == DiffView::Symbols).await {
            Some(data) => data,
            None => load_cache_data(&conn, &job.id, &coords).await,
        },
    };

    if let Some(pattern) = request.glob {
        let matcher = match GlobBuilder::new(pattern).literal_separator(false).build() {
            Ok(glob) => glob.compile_matcher(),
            Err(error) => return format!("Invalid glob '{pattern}': {error}"),
        };
        filter_diff_data_by_glob(&mut data, &matcher);
    }

    match request.view {
        DiffView::Summary => render_summary(project, number, exec_seq, node_id, &data),
        DiffView::Commits => render_commits(project, number, node_id, &data),
        DiffView::Patch => {
            let patch = data.patch.as_deref().unwrap_or_default();
            let scoped = if let Some(path) = request.file {
                filter_patch(patch, Some(path), None)
            } else {
                patch.to_string()
            };
            render_patch(
                project,
                number,
                node_id,
                &data,
                &scoped,
                request.file,
                request.glob,
            )
        }
        DiffView::Symbols => render_symbols(project, number, node_id, &data, request.glob),
        DiffView::Check => render_check(project, number, node_id, &data),
    }
}

fn parse_diff_request(params: &[QueryParam]) -> Result<DiffRequest<'_>, String> {
    if let Some(param) = params
        .iter()
        .find(|param| !matches!(param.key.as_str(), "view" | "file" | "glob"))
    {
        return Err(format!(
            "Unsupported query parameter '{}' for node diff",
            param.key
        ));
    }
    let file = find_query_value(params, "file").filter(|value| !value.is_empty());
    let glob = find_query_value(params, "glob").filter(|value| !value.is_empty());
    if file.is_some() && glob.is_some() {
        return Err("node diff accepts either file=PATH or glob=PATTERN, not both".to_string());
    }
    let view = match find_query_value(params, "view") {
        None if file.is_some() => DiffView::Patch,
        None => DiffView::Summary,
        Some("commits") => DiffView::Commits,
        Some("patch") => DiffView::Patch,
        Some("symbols") => DiffView::Symbols,
        Some("check") => DiffView::Check,
        Some(value) => {
            return Err(format!(
                "Invalid node diff view '{value}'. Expected commits, patch, symbols, or check."
            ));
        }
    };
    if file.is_some() && view != DiffView::Patch {
        return Err("file=PATH is only valid with view=patch".to_string());
    }
    if glob.is_some() && matches!(view, DiffView::Commits | DiffView::Check) {
        return Err(
            "glob=PATTERN is only valid with the summary, patch, or symbols view".to_string(),
        );
    }
    Ok(DiffRequest { view, file, glob })
}

async fn load_diff_coords(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Result<DiffCoords, String> {
    let mut rows = conn
        .query(
            "SELECT j.worktree_path, j.execution_id, p.repo_path,
                    COALESCE(j.base_branch, p.default_branch, 'main'), j.branch
             FROM jobs j JOIN projects p ON j.project_id = p.id
             WHERE j.id = ?1 LIMIT 1",
            (job_id,),
        )
        .await
        .map_err(|error| format!("Failed to load node diff coordinates: {error}"))?;
    let row = rows
        .next()
        .await
        .map_err(|error| format!("Failed to load node diff coordinates: {error}"))?
        .ok_or_else(|| "Node diff coordinates were not found".to_string())?;
    let worktree_path = row
        .opt_text(0)
        .ok()
        .flatten()
        .filter(|value| !value.is_empty());
    let execution_id = row
        .opt_text(1)
        .ok()
        .flatten()
        .filter(|value| !value.is_empty());
    let base_commit = match (&worktree_path, &execution_id) {
        (Some(worktree), Some(execution)) => {
            let mut anchors = conn
                .query(
                    "SELECT base_commit, pack_anchor FROM jobs
                     WHERE execution_id = ?1 AND worktree_path = ?2
                     ORDER BY created_at ASC LIMIT 1",
                    (execution.as_str(), worktree.as_str()),
                )
                .await
                .map_err(|error| format!("Failed to load node diff base anchor: {error}"))?;
            anchors
                .next()
                .await
                .map_err(|error| format!("Failed to load node diff base anchor: {error}"))?
                .and_then(|anchor| {
                    anchor
                        .opt_text(1)
                        .ok()
                        .flatten()
                        .or_else(|| anchor.opt_text(0).ok().flatten())
                })
                .filter(|value| !value.is_empty())
        }
        _ => None,
    };
    Ok(DiffCoords {
        worktree_path,
        execution_id,
        repo_path: row
            .text(2)
            .map_err(|error| format!("Invalid node diff repo path: {error}"))?,
        base_branch: row
            .text(3)
            .map_err(|error| format!("Invalid node diff base branch: {error}"))?,
        base_commit,
        branch: row
            .opt_text(4)
            .ok()
            .flatten()
            .filter(|value| !value.is_empty()),
    })
}

async fn load_live_data(orch: &Orchestrator, coords: &DiffCoords) -> Option<DiffData> {
    let worktree = PathBuf::from(coords.worktree_path.as_deref()?);
    if !worktree.exists() || !crate::jj::is_jj_dir(&worktree) {
        return None;
    }
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let fork = crate::jj::resolve_node_fork_point(
        &jj,
        &worktree,
        Some(&coords.base_branch),
        coords.base_commit.as_deref(),
    )?;
    let patch = crate::jj::node_range_patch(
        &jj,
        &worktree,
        Some(&coords.base_branch),
        coords.base_commit.as_deref(),
    )?;
    let rows = crate::jj::parse_git_patch(&patch)
        .into_iter()
        .map(|change| {
            (
                change.path,
                change.status,
                Some(change.additions),
                Some(change.deletions),
                change.previous_path,
            )
        })
        .collect::<Vec<_>>();
    let dirty_paths = crate::jj::working_copy_dirty_paths(&jj, &worktree).unwrap_or_default();
    let conflicted_files = crate::jj::conflicted_files(&jj, &worktree);
    let range = format!("{fork}..@");
    let conflicted_commits = crate::jj::conflicted_commits(&jj, &worktree, &range);
    let commits = crate::jj::range_commits(&jj, &worktree, &fork)
        .unwrap_or_default()
        .into_iter()
        .map(|commit| DisplayCommit {
            commit_id: commit.commit_id,
            change_id: Some(commit.change_id),
            description: commit.description,
            author: commit.author,
            timestamp: commit.timestamp,
            working_copy: commit.working_copy,
        })
        .collect::<Vec<_>>();
    let commits_ahead = commits.iter().filter(|commit| !commit.working_copy).count() as i32;
    let current_revision = crate::jj::head_commit(&jj, &worktree)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let (marker_findings, marker_total) = scan_workspace_markers(&worktree, &rows, FINDING_CAP);
    let (whitespace_findings, whitespace_total) =
        scan_added_lines(&patch, ScanKind::Whitespace, FINDING_CAP);
    Some(DiffData {
        source_note: None,
        content_source: DiffContentSource::Live {
            worktree: worktree.clone(),
        },
        base_branch: coords.base_branch.clone(),
        base_commit: Some(fork),
        current_revision,
        branch: coords.branch.clone(),
        commits_ahead: Some(commits_ahead),
        rows,
        patch: Some(patch),
        commits: Some(commits),
        check: CheckData {
            workspace_state: if dirty_paths.is_empty() {
                "clean".to_string()
            } else {
                format!("{} loose edited paths", dirty_paths.len())
            },
            dirty_paths,
            conflicted_files,
            conflicted_commits,
            marker_findings,
            marker_total,
            whitespace_findings,
            whitespace_total,
        },
    })
}

async fn load_archived_data(
    conn: &cairn_db::turso::Connection,
    coords: &DiffCoords,
    retain_content_source: bool,
) -> Option<DiffData> {
    let execution_id = coords.execution_id.as_deref()?;
    let mut rows = conn
        .query(
            "SELECT base_sha, tip_sha, pack, pack_idx
             FROM execution_history WHERE execution_id = ?1 LIMIT 1",
            (execution_id,),
        )
        .await
        .ok()?;
    let row = rows.next().await.ok()??;
    let base = row.text(0).ok()?;
    let tip = row.text(1).ok()?;
    let pack = match (
        row.opt_blob(2).ok().flatten(),
        row.opt_blob(3).ok().flatten(),
    ) {
        (Some(pack), Some(index)) => Some((pack, index)),
        _ => None,
    };
    let content_source =
        archived_content_source(&coords.repo_path, &tip, &pack, retain_content_source);
    let store = ObjectStore::new(Path::new(&coords.repo_path), pack).ok()?;
    let patch = render_range_diff(&store, &base, &tip).ok()?;
    let file_diffs = render_range_file_diffs(&store, &base, &tip).ok()?;
    let rows = file_diffs
        .into_iter()
        .map(|file| {
            (
                file.path,
                file.status,
                Some(file.additions as i32),
                Some(file.deletions as i32),
                file.previous_path,
            )
        })
        .collect::<Vec<_>>();
    let commits = list_range_commits(&store, &base, &tip)
        .unwrap_or_default()
        .into_iter()
        .map(|commit| DisplayCommit {
            commit_id: commit.sha,
            change_id: None,
            description: commit.summary,
            author: commit.author,
            timestamp: commit.timestamp.to_string(),
            working_copy: false,
        })
        .collect::<Vec<_>>();
    let (marker_findings, marker_total) = scan_added_lines(&patch, ScanKind::Marker, FINDING_CAP);
    let (whitespace_findings, whitespace_total) =
        scan_added_lines(&patch, ScanKind::Whitespace, FINDING_CAP);
    let commits_ahead = count_commits_ahead(&store, &base, &tip);
    Some(DiffData {
        source_note: Some(
            "Workspace torn down; rendered from archived execution history. Renames may appear as delete + add."
                .to_string(),
        ),
        content_source,
        base_branch: coords.base_branch.clone(),
        base_commit: Some(base.clone()),
        current_revision: Some(tip.clone()),
        branch: coords.branch.clone(),
        commits_ahead: Some(commits_ahead),
        rows,
        patch: Some(patch),
        commits: Some(commits),
        check: CheckData {
            workspace_state: "workspace torn down".to_string(),
            marker_findings,
            marker_total,
            whitespace_findings,
            whitespace_total,
            ..CheckData::default()
        },
    })
}

async fn load_cache_data(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    coords: &DiffCoords,
) -> DiffData {
    DiffData {
        source_note: Some(
            "Legacy cache fallback: patch, commit, revision, conflict, whitespace, and structural symbol views are unavailable because this execution has no archived history."
                .to_string(),
        ),
        content_source: DiffContentSource::Unavailable {
            reason: "legacy cache records changed paths and counts but no file content".to_string(),
        },
        base_branch: coords.base_branch.clone(),
        base_commit: coords.base_commit.clone(),
        current_revision: None,
        branch: coords.branch.clone(),
        commits_ahead: None,
        rows: load_job_file_changes(conn, job_id).await,
        patch: None,
        commits: None,
        check: CheckData {
            workspace_state: "unavailable (legacy cache only)".to_string(),
            ..CheckData::default()
        },
    }
}

fn render_summary(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    data: &DiffData,
) -> String {
    let rows = dedupe_file_changes_by_path(&data.rows);
    let additions: i32 = rows.iter().filter_map(|row| row.2).sum();
    let deletions: i32 = rows.iter().filter_map(|row| row.3).sum();
    let conflict_state = if !data.check.conflicted_files.is_empty() {
        format!("jj conflict in {}", data.check.conflicted_files.join(", "))
    } else if !data.check.conflicted_commits.is_empty() {
        format!(
            "jj conflict in {} commit(s)",
            data.check.conflicted_commits.len()
        )
    } else {
        "none".to_string()
    };
    let mut out = format!("# Workspace Diff - {project}-{number} / {node_id}\n\n");
    out.push_str(&format!(
        "- Base: `{}` at `{}`\n",
        data.base_branch,
        short(data.base_commit.as_deref())
    ));
    out.push_str(&format!(
        "- Current: `@- {}`",
        short(data.current_revision.as_deref())
    ));
    if let Some(branch) = &data.branch {
        out.push_str(&format!(" on bookmark `{branch}`"));
    }
    out.push('\n');
    out.push_str(&format!(
        "- Commits ahead: {}\n- Working copy: {}\n- Conflicts: {}\n",
        data.commits_ahead
            .map(|count| count.to_string())
            .unwrap_or_else(|| "unavailable".to_string()),
        data.check.workspace_state,
        conflict_state
    ));
    if let Some(note) = &data.source_note {
        out.push_str(&format!("\n> {note}\n"));
    }
    out.push_str(&format!(
        "\n**{} files, +{} -{}**\n\n",
        rows.len(),
        additions,
        deletions
    ));
    let diff_uri = format!("cairn://p/{project}/{number}/{exec_seq}/{node_id}/diff");
    if rows.is_empty() {
        out.push_str("No changes in the node range.\n");
    } else {
        push_file_change_table(&mut out, &rows);
        out.push_str(&format!(
            "\nUse `{diff_uri}?view=patch&file=PATH` for one file.\n"
        ));
    }
    out.push_str(&format!(
        "\n## Drill down\n\n- `{diff_uri}?view=commits`\n- `{diff_uri}?view=patch`\n- `{diff_uri}?view=symbols`\n- `{diff_uri}?view=check`\n"
    ));
    out
}

fn render_commits(project: &str, number: i32, node_id: &str, data: &DiffData) -> String {
    let mut out = format!("# Commits - {project}-{number} / {node_id}\n\n");
    let Some(commits) = &data.commits else {
        out.push_str(
            data.source_note
                .as_deref()
                .unwrap_or("Commit history is unavailable."),
        );
        return out;
    };
    if let Some(note) = &data.source_note {
        out.push_str(&format!("> {note}\n\n"));
    }
    if commits.is_empty() {
        out.push_str("No non-empty commits in the node range.\n");
        return out;
    }
    out.push_str("| Commit | Change | Description | Author | Timestamp |\n|---|---|---|---|---|\n");
    for commit in commits {
        let commit_label = if commit.working_copy {
            format!("{} @ (working copy)", short(Some(&commit.commit_id)))
        } else {
            short(Some(&commit.commit_id)).to_string()
        };
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} |\n",
            escape_table(&commit_label),
            commit
                .change_id
                .as_deref()
                .map(|id| format!("`{}`", escape_table(id)))
                .unwrap_or_else(|| "unavailable (archived Git object)".to_string()),
            escape_table(if commit.description.is_empty() {
                "(no description)"
            } else {
                &commit.description
            }),
            escape_table(&commit.author),
            escape_table(&commit.timestamp),
        ));
    }
    out
}

fn render_patch(
    project: &str,
    number: i32,
    node_id: &str,
    data: &DiffData,
    patch: &str,
    file: Option<&str>,
    glob: Option<&str>,
) -> String {
    let mut out = format!("# Patch - {project}-{number} / {node_id}");
    if let Some(file) = file {
        out.push_str(&format!(" / {file}"));
    } else if let Some(glob) = glob {
        out.push_str(&format!(" / glob `{glob}`"));
    }
    out.push_str("\n\n");
    if data.patch.is_none() {
        out.push_str(
            data.source_note
                .as_deref()
                .unwrap_or("Patch is unavailable."),
        );
    } else if patch.is_empty() {
        out.push_str("(no patch sections matched)\n");
    } else {
        out.push_str(patch);
    }
    out
}

fn archived_content_source(
    repo_path: &str,
    tip_sha: &str,
    pack: &Option<(Vec<u8>, Vec<u8>)>,
    retain: bool,
) -> DiffContentSource {
    if retain {
        DiffContentSource::Archived {
            repo_path: PathBuf::from(repo_path),
            pack: pack.clone(),
            tip_sha: tip_sha.to_string(),
        }
    } else {
        DiffContentSource::Unavailable {
            reason: "archived source was not requested for this projection".to_string(),
        }
    }
}

fn filter_diff_data_by_glob(data: &mut DiffData, matcher: &globset::GlobMatcher) {
    data.rows.retain(|row| {
        matcher.is_match(&row.0)
            || row
                .4
                .as_deref()
                .map(|path| matcher.is_match(path))
                .unwrap_or(false)
    });
    if let Some(patch) = data.patch.take() {
        data.patch = Some(filter_patch(&patch, None, Some(matcher)));
    }
}

fn render_symbols(
    project: &str,
    number: i32,
    node_id: &str,
    data: &DiffData,
    glob: Option<&str>,
) -> String {
    let mut out = format!("# Symbols - {project}-{number} / {node_id}");
    if let Some(glob) = glob {
        out.push_str(&format!(" / glob `{glob}`"));
    }
    out.push_str("\n\n");
    if let Some(note) = &data.source_note {
        out.push_str(&format!("> {note}\n\n"));
    }

    let rows = dedupe_file_changes_by_path(&data.rows);
    if rows.is_empty() {
        out.push_str("No changed files matched the symbols projection.\n");
        return out;
    }

    let reader = data.content_source.reader();
    let mut blocks = Vec::with_capacity(rows.len());
    let mut rendered_count = 0usize;
    for row in rows {
        let path = &row.0;
        let status = row.1.as_str();
        let result = if matches!(status, "removed" | "deleted") {
            "(no current outline: file was removed)".to_string()
        } else if crate::symbols::engine::lang_for_path(Path::new(path)).is_none() {
            "(no current outline: unsupported file grammar)".to_string()
        } else {
            match reader.read_path(path) {
                Err(reason) => format!("(no current outline: source unavailable: {reason})"),
                Ok(bytes) if bytes.contains(&0) => {
                    "(no current outline: binary content contains NUL bytes)".to_string()
                }
                Ok(bytes) => {
                    let source = String::from_utf8_lossy(&bytes);
                    let outline = crate::symbols::outline::outline_text(
                        &source,
                        crate::symbols::engine::lang_for_path(Path::new(path)),
                    );
                    if outline.is_empty() {
                        "(no current outline: supported file has no extractable outline entries)"
                            .to_string()
                    } else {
                        rendered_count += 1;
                        outline
                    }
                }
            }
        };
        blocks.push(format!("{path}\n{result}"));
    }

    if rendered_count == 0 {
        out.push_str(
            "No structural outline entries were rendered; file-specific reasons follow.\n\n",
        );
    }
    out.push_str(&blocks.join("\n\n"));
    out.push('\n');
    out
}

fn render_check(project: &str, number: i32, node_id: &str, data: &DiffData) -> String {
    let mut out = format!("# Diff Check - {project}-{number} / {node_id}\n\n");
    if let Some(note) = &data.source_note {
        out.push_str(&format!("> {note}\n\n"));
    }
    out.push_str(&format!("- Working copy: {}\n", data.check.workspace_state));
    if !data.check.dirty_paths.is_empty() {
        for path in &data.check.dirty_paths {
            out.push_str(&format!("  - `{path}`\n"));
        }
    }
    if data.check.conflicted_files.is_empty() && data.check.conflicted_commits.is_empty() {
        out.push_str("- Jujutsu conflicts: none\n");
    } else {
        out.push_str("- Jujutsu conflicts:\n");
        for path in &data.check.conflicted_files {
            out.push_str(&format!("  - `@`: `{path}`\n"));
        }
        for commit in &data.check.conflicted_commits {
            out.push_str(&format!(
                "  - `{}` / `{}` {}{}\n",
                commit.commit_id,
                commit.change_id,
                commit.description,
                if commit.files.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", commit.files.join(", "))
                }
            ));
        }
    }
    push_findings(
        &mut out,
        "Literal conflict markers",
        &data.check.marker_findings,
        data.check.marker_total,
    );
    push_findings(
        &mut out,
        "Trailing whitespace on added lines",
        &data.check.whitespace_findings,
        data.check.whitespace_total,
    );
    out
}

fn push_findings(out: &mut String, label: &str, findings: &[LineFinding], total: usize) {
    if total == 0 {
        out.push_str(&format!("- {label}: none\n"));
        return;
    }
    out.push_str(&format!("- {label}: {total}\n"));
    for finding in findings {
        out.push_str(&format!(
            "  - `{}:{}` {}\n",
            finding.path, finding.line, finding.detail
        ));
    }
    if total > findings.len() {
        out.push_str(&format!(
            "  - … {} more (capped at {FINDING_CAP})\n",
            total - findings.len()
        ));
    }
}

fn short(value: Option<&str>) -> &str {
    let value = value.unwrap_or("unavailable");
    value.get(..value.len().min(12)).unwrap_or(value)
}

fn escape_table(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

fn filter_patch(patch: &str, file: Option<&str>, glob: Option<&globset::GlobMatcher>) -> String {
    split_patch_sections(patch)
        .into_iter()
        .filter(|section| {
            let files = crate::jj::parse_git_patch(section);
            files.iter().any(|change| {
                let exact = file
                    .map(|path| {
                        change.path == path || change.previous_path.as_deref() == Some(path)
                    })
                    .unwrap_or(true);
                let glob_match = glob
                    .map(|matcher| {
                        matcher.is_match(&change.path)
                            || change
                                .previous_path
                                .as_deref()
                                .map(|path| matcher.is_match(path))
                                .unwrap_or(false)
                    })
                    .unwrap_or(true);
                exact && glob_match
            })
        })
        .collect()
}

fn split_patch_sections(patch: &str) -> Vec<&str> {
    let mut starts = patch
        .match_indices("diff --git ")
        .filter(|(index, _)| *index == 0 || patch.as_bytes().get(index - 1) == Some(&b'\n'))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if starts.is_empty() {
        return Vec::new();
    }
    starts.push(patch.len());
    starts
        .windows(2)
        .map(|window| &patch[window[0]..window[1]])
        .collect()
}

fn scan_workspace_markers(
    workspace: &Path,
    rows: &[FileChangeRow],
    cap: usize,
) -> (Vec<LineFinding>, usize) {
    let mut findings = Vec::new();
    let mut total = 0;
    for row in rows {
        let path = workspace.join(&row.0);
        let Ok(bytes) = std::fs::read(path) else {
            continue;
        };
        if bytes.contains(&0) {
            continue;
        }
        let text = String::from_utf8_lossy(&bytes);
        let (file_findings, file_total) =
            scan_conflict_markers(&row.0, &text, cap.saturating_sub(findings.len()));
        total += file_total;
        findings.extend(file_findings);
    }
    (findings, total)
}

fn scan_conflict_markers(path: &str, text: &str, cap: usize) -> (Vec<LineFinding>, usize) {
    let mut findings = Vec::new();
    let mut total = 0;
    for (index, line) in text.lines().enumerate() {
        if let Some(marker) = marker_at_line_start(line) {
            total += 1;
            if findings.len() < cap {
                findings.push(LineFinding {
                    path: path.to_string(),
                    line: index + 1,
                    detail: marker.to_string(),
                });
            }
        }
    }
    (findings, total)
}

fn marker_at_line_start(line: &str) -> Option<&'static str> {
    ["<<<<<<<", "=======", ">>>>>>>"]
        .into_iter()
        .find(|marker| line.starts_with(marker))
}

#[derive(Debug, Clone, Copy)]
enum ScanKind {
    Marker,
    Whitespace,
}

fn scan_added_lines(patch: &str, kind: ScanKind, cap: usize) -> (Vec<LineFinding>, usize) {
    let mut findings = Vec::new();
    let mut total = 0;
    for section in split_patch_sections(patch) {
        let path = crate::jj::parse_git_patch(section)
            .into_iter()
            .next()
            .map(|change| change.path)
            .unwrap_or_else(|| "unknown".to_string());
        let mut new_line = 0usize;
        let mut in_hunk = false;
        for line in section.lines() {
            if line.starts_with("@@ ") {
                new_line = parse_new_hunk_start(line).unwrap_or(0);
                in_hunk = true;
                continue;
            }
            if !in_hunk || line.starts_with("\\ No newline") {
                continue;
            }
            if let Some(content) = line.strip_prefix('+') {
                let detail = match kind {
                    ScanKind::Marker => marker_at_line_start(content).map(str::to_string),
                    ScanKind::Whitespace => (content.ends_with(' ') || content.ends_with('\t'))
                        .then(|| "trailing whitespace".to_string()),
                };
                if let Some(detail) = detail {
                    total += 1;
                    if findings.len() < cap {
                        findings.push(LineFinding {
                            path: path.clone(),
                            line: new_line,
                            detail,
                        });
                    }
                }
                new_line += 1;
            } else if line.starts_with('-') {
                // Deleted lines do not advance the new-file line number.
            } else {
                new_line += 1;
            }
        }
    }
    (findings, total)
}

fn parse_new_hunk_start(header: &str) -> Option<usize> {
    let plus = header
        .split_whitespace()
        .find(|part| part.starts_with('+'))?;
    plus.trim_start_matches('+').split(',').next()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_codec::packfile::build_execution_pack;
    use cairn_codec::testutil::{commit_all, git, init_repo, write_file};
    use cairn_common::query::parse_query_params;

    fn test_data(content_source: DiffContentSource, rows: Vec<FileChangeRow>) -> DiffData {
        DiffData {
            source_note: None,
            content_source,
            base_branch: "main".to_string(),
            base_commit: Some("0123456789abcdef".to_string()),
            current_revision: Some("fedcba9876543210".to_string()),
            branch: Some("agent/CAIRN-42-builder".to_string()),
            commits_ahead: Some(1),
            rows,
            patch: None,
            commits: None,
            check: CheckData {
                workspace_state: "clean".to_string(),
                ..CheckData::default()
            },
        }
    }

    fn changed(path: &str, status: &str, previous_path: Option<&str>) -> FileChangeRow {
        (
            path.to_string(),
            status.to_string(),
            Some(2),
            Some(1),
            previous_path.map(str::to_string),
        )
    }

    #[test]
    fn summary_links_stay_on_the_addressed_node() {
        let data = test_data(
            DiffContentSource::Unavailable {
                reason: "test source unavailable".to_string(),
            },
            vec![changed("src/lib.rs", "modified", None)],
        );

        let rendered = render_summary("CAIRN", 42, 7, "sibling", &data);
        let base = "cairn://p/CAIRN/42/7/sibling/diff";
        assert!(rendered.contains(&format!("`{base}?view=patch&file=PATH`")));
        assert!(rendered.contains(&format!("`{base}?view=commits`")));
        assert!(rendered.contains(&format!("`{base}?view=patch`")));
        assert!(rendered.contains(&format!("`{base}?view=symbols`")));
        assert!(rendered.contains(&format!("`{base}?view=check`")));
        assert!(!rendered.contains("cairn:~/diff"));
    }

    #[test]
    fn dispatch_rejects_unknown_params_and_file_implies_patch() {
        let params = parse_query_params("file=src/lib.rs").unwrap();
        assert_eq!(parse_diff_request(&params).unwrap().view, DiffView::Patch);
        let params = parse_query_params("bogus=1").unwrap();
        assert!(parse_diff_request(&params)
            .unwrap_err()
            .contains("Unsupported"));
    }

    #[test]
    fn dispatch_rejects_invalid_combinations() {
        let params = parse_query_params("view=check&file=x").unwrap();
        assert!(parse_diff_request(&params).is_err());
        let params = parse_query_params("view=symbols&file=src/lib.rs").unwrap();
        assert_eq!(
            parse_diff_request(&params).unwrap_err(),
            "file=PATH is only valid with view=patch"
        );
        let params = parse_query_params("file=x&glob=*.rs").unwrap();
        assert!(parse_diff_request(&params).is_err());
    }

    #[test]
    fn symbols_view_accepts_glob() {
        let params = parse_query_params("view=symbols").unwrap();
        assert_eq!(parse_diff_request(&params).unwrap().view, DiffView::Symbols);

        let params = parse_query_params("view=symbols&glob=src/**/*.rs").unwrap();
        let request = parse_diff_request(&params).unwrap();
        assert_eq!(request.view, DiffView::Symbols);
        assert_eq!(request.glob, Some("src/**/*.rs"));
    }

    #[test]
    fn symbols_render_only_changed_supported_sources() {
        let temp = tempfile::tempdir().unwrap();
        write_file(temp.path(), "src/changed.rs", b"pub fn changed() {}\n");
        write_file(
            temp.path(),
            "src/changed.ts",
            b"export class ChangedTs { run(): void {} }\n",
        );
        write_file(temp.path(), "src/unchanged.rs", b"pub fn unchanged() {}\n");
        let data = test_data(
            DiffContentSource::Live {
                worktree: temp.path().to_path_buf(),
            },
            vec![
                changed("src/changed.rs", "modified", None),
                changed("src/changed.ts", "added", None),
            ],
        );

        let rendered = render_symbols("CAIRN", 42, "builder", &data, None);
        assert!(rendered.contains("src/changed.rs\n1:pub fn changed()"));
        assert!(
            rendered.contains("src/changed.ts\n1:class ChangedTs"),
            "{rendered}"
        );
        assert!(!rendered.contains("unchanged"));
    }

    #[test]
    fn live_symbols_reject_parent_traversal_paths() {
        let temp = tempfile::tempdir().unwrap();
        let data = test_data(
            DiffContentSource::Live {
                worktree: temp.path().to_path_buf(),
            },
            vec![changed("../escape.rs", "modified", None)],
        );

        let rendered = render_symbols("CAIRN", 42, "builder", &data, None);
        assert!(rendered.contains("source unavailable: invalid non-relative changed path"));
    }

    #[cfg(unix)]
    #[test]
    fn live_symbols_reject_symlinks_that_escape_the_worktree() {
        use std::os::unix::fs::symlink;

        let worktree = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        write_file(outside.path(), "secret.rs", b"pub fn outside_secret() {}\n");
        symlink(
            outside.path().join("secret.rs"),
            worktree.path().join("leak.rs"),
        )
        .unwrap();
        let data = test_data(
            DiffContentSource::Live {
                worktree: worktree.path().to_path_buf(),
            },
            vec![changed("leak.rs", "modified", None)],
        );

        let rendered = render_symbols("CAIRN", 42, "builder", &data, None);
        assert!(rendered.contains("source unavailable: live source is a symlink"));
        assert!(!rendered.contains("outside_secret"));
    }

    #[cfg(unix)]
    #[test]
    fn live_source_no_follow_resists_concurrent_symlink_swaps() {
        use std::os::unix::fs::symlink;
        use std::sync::{Arc, Barrier};

        let worktree = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let path = worktree.path().join("leak.rs");
        let outside_path = outside.path().join("secret.rs");
        let safe = b"pub fn safe() {}\n";
        write_file(worktree.path(), "leak.rs", safe);
        write_file(outside.path(), "secret.rs", b"pub fn outside_secret() {}\n");

        let barrier = Arc::new(Barrier::new(2));
        let writer_barrier = Arc::clone(&barrier);
        let writer_path = path.clone();
        let writer_outside = outside_path.clone();
        let writer = std::thread::spawn(move || {
            writer_barrier.wait();
            for index in 0..5_000 {
                let candidate = writer_path.with_extension("candidate");
                let _ = std::fs::remove_file(&candidate);
                if index % 2 == 0 {
                    std::fs::write(&candidate, safe).unwrap();
                } else {
                    symlink(&writer_outside, &candidate).unwrap();
                }
                std::fs::rename(&candidate, &writer_path).unwrap();
            }
        });

        barrier.wait();
        let mut successful_reads = 0usize;
        for _ in 0..5_000 {
            match read_live_source_no_follow(worktree.path(), Path::new("leak.rs")) {
                Ok(bytes) => {
                    successful_reads += 1;
                    assert_eq!(bytes, safe, "a no-follow read escaped the worktree");
                }
                Err(_) => {
                    // A concurrently installed symlink is rejected atomically.
                }
            }
        }
        writer.join().unwrap();
        assert!(successful_reads > 0);
    }

    #[test]
    fn symbols_accounts_for_every_non_rendered_file() {
        let temp = tempfile::tempdir().unwrap();
        write_file(temp.path(), "binary.rs", b"pub fn binary() {}\0");
        write_file(temp.path(), "empty.rs", b"// no declarations\n");
        let data = test_data(
            DiffContentSource::Live {
                worktree: temp.path().to_path_buf(),
            },
            vec![
                changed("removed.rs", "removed", None),
                changed("notes.txt", "modified", None),
                changed("binary.rs", "modified", None),
                changed("missing.rs", "modified", None),
                changed("empty.rs", "modified", None),
            ],
        );

        let rendered = render_symbols("CAIRN", 42, "builder", &data, None);
        assert!(rendered.contains("removed.rs\n(no current outline: file was removed)"));
        assert!(rendered.contains("notes.txt\n(no current outline: unsupported file grammar)"));
        assert!(
            rendered.contains("binary.rs\n(no current outline: binary content contains NUL bytes)")
        );
        assert!(rendered.contains("missing.rs\n(no current outline: source unavailable:"));
        assert!(rendered.contains(
            "empty.rs\n(no current outline: supported file has no extractable outline entries)"
        ));
        assert!(rendered.contains("No structural outline entries were rendered"));
    }

    #[test]
    fn rename_glob_selects_previous_path_but_outlines_current_path() {
        let temp = tempfile::tempdir().unwrap();
        write_file(temp.path(), "src/new.rs", b"pub fn renamed() {}\n");
        let mut data = test_data(
            DiffContentSource::Live {
                worktree: temp.path().to_path_buf(),
            },
            vec![changed("src/new.rs", "renamed", Some("legacy/selected.rs"))],
        );
        let matcher = GlobBuilder::new("legacy/**/*.rs")
            .literal_separator(false)
            .build()
            .unwrap()
            .compile_matcher();
        filter_diff_data_by_glob(&mut data, &matcher);

        let rendered = render_symbols("CAIRN", 42, "builder", &data, Some("legacy/**/*.rs"));
        assert!(rendered.contains("src/new.rs\n1:pub fn renamed()"));
        assert!(!rendered.contains("legacy/selected.rs\n"));
    }

    #[test]
    fn archived_content_source_is_retained_only_when_requested() {
        let pack = Some((vec![1, 2, 3], vec![4, 5]));
        assert!(matches!(
            archived_content_source("/repo", "tip", &pack, false),
            DiffContentSource::Unavailable { .. }
        ));
        match archived_content_source("/repo", "tip", &pack, true) {
            DiffContentSource::Archived {
                repo_path,
                pack: retained,
                tip_sha,
            } => {
                assert_eq!(repo_path, PathBuf::from("/repo"));
                assert_eq!(retained, pack);
                assert_eq!(tip_sha, "tip");
            }
            _ => panic!("symbols view must retain archived source coordinates"),
        }
    }

    #[test]
    fn live_and_archived_sources_render_identical_outlines() {
        let origin = tempfile::tempdir().unwrap();
        init_repo(origin.path());
        write_file(origin.path(), "README.md", b"base\n");
        let anchor = commit_all(origin.path(), "base");

        let worktree = tempfile::tempdir().unwrap();
        git(
            origin.path(),
            &[
                "clone",
                "-q",
                origin.path().to_str().unwrap(),
                worktree.path().to_str().unwrap(),
            ],
        );
        git(worktree.path(), &["checkout", "-q", "-b", "work"]);
        write_file(worktree.path(), "src/lib.rs", b"pub struct Shared;\n");
        let tip = commit_all(worktree.path(), "add shared source");
        let (pack, index) = build_execution_pack(worktree.path(), &tip, &anchor, "main")
            .unwrap()
            .unwrap();
        let rows = vec![changed("src/lib.rs", "added", None)];
        let live = test_data(
            DiffContentSource::Live {
                worktree: worktree.path().to_path_buf(),
            },
            rows.clone(),
        );
        let archived = test_data(
            DiffContentSource::Archived {
                repo_path: origin.path().to_path_buf(),
                pack: Some((pack, index)),
                tip_sha: tip,
            },
            rows,
        );

        assert_eq!(
            render_symbols("CAIRN", 42, "builder", &live, None),
            render_symbols("CAIRN", 42, "builder", &archived, None)
        );
    }

    #[test]
    fn legacy_cache_symbols_degrade_without_changing_summary() {
        let rows = vec![changed("src/lib.rs", "modified", None)];
        let data = test_data(
            DiffContentSource::Unavailable {
                reason: "legacy cache records changed paths and counts but no file content"
                    .to_string(),
            },
            rows,
        );

        let symbols = render_symbols("CAIRN", 42, "builder", &data, None);
        assert!(symbols.contains("source unavailable: legacy cache records"));
        let summary = render_summary("CAIRN", 42, 7, "builder", &data);
        assert!(summary.contains("**1 files, +2 -1**"));
    }

    #[test]
    fn symbols_distinguish_empty_changed_selection() {
        let data = test_data(
            DiffContentSource::Unavailable {
                reason: "unused".to_string(),
            },
            Vec::new(),
        );
        let rendered = render_symbols("CAIRN", 42, "builder", &data, Some("src/**/*.rs"));
        assert!(rendered.contains("No changed files matched the symbols projection."));
        assert!(!rendered.contains("No structural outline entries were rendered"));
    }

    #[test]
    fn patch_splits_and_scopes_by_file() {
        let patch = "diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n-old\n+new\ndiff --git a/b.txt b/b.txt\n--- a/b.txt\n+++ b/b.txt\n@@ -0,0 +1 @@\n+fresh\n";
        assert_eq!(split_patch_sections(patch).len(), 2);
        let scoped = filter_patch(patch, Some("b.txt"), None);
        assert!(!scoped.contains("a/a.txt"));
        assert!(scoped.contains("b/b.txt"));
    }

    #[test]
    fn marker_scanner_only_matches_line_start_and_binary_is_skipped() {
        let (findings, total) = scan_conflict_markers(
            "x.txt",
            "<<<<<<< ours\nprefix =======\n=======\n>>>>>>> theirs\n",
            100,
        );
        assert_eq!(total, 3);
        assert_eq!(
            findings
                .iter()
                .map(|finding| finding.line)
                .collect::<Vec<_>>(),
            vec![1, 3, 4]
        );

        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("binary"), b"\0<<<<<<< nope").unwrap();
        let rows = vec![(
            "binary".to_string(),
            "modified".to_string(),
            None,
            None,
            None,
        )];
        assert_eq!(scan_workspace_markers(temp.path(), &rows, 100).1, 0);
    }

    #[test]
    fn whitespace_scanner_checks_added_lines_only() {
        let patch = "diff --git a/x b/x\n--- a/x\n+++ b/x\n@@ -10,2 +10,3 @@\n-old \n context\n+clean\n+bad \n";
        let (findings, total) = scan_added_lines(patch, ScanKind::Whitespace, 100);
        assert_eq!(total, 1);
        assert_eq!(findings[0].path, "x");
        assert_eq!(findings[0].line, 12);
    }
}
