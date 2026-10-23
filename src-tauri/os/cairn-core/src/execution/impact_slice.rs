//! `tree: impact` lanes: a check whose inputs are a bounded set of files runs
//! on the runner, with no cell and no placement, in a scratch directory holding
//! exactly the files its `impact` globs select at the commit under test.
//!
//! The command runs unchanged; only what it can see changes. The slice is built
//! from the same [`selected_entries`] the check's cache key hashes, read from
//! the project's Git object database through the gix [`ObjectStore`], so a slice
//! check sees precisely what its verdict is keyed on. A file it reads but did
//! not declare is simply absent, and the check fails loudly instead of passing
//! on a key that never covered it.
//!
//! Lanes enter here from [`super::checks::submit_planned_check_batch`] after the
//! batch's reservation, so suppression budgets, retries, and recording are the
//! cell path's own; only the execution differs. The result carries a
//! colocated-executor account, so its observation names the runner's machine
//! and environment exactly as a check the coordinator ran itself.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use cairn_codec::objects::{ObjectReadLimits, ObjectStore, TreeEntryKind};
use cairn_common::executor_protocol::{CellExecutionMeta, ExecutorSelector};
use tokio::sync::Semaphore;

use super::checks::{
    CheckExecutionFailure, PlannedCheckBatchItem, PlannedCheckBatchRequest, SubstrateFailureShape,
};
use super::inputs::{impact_tree_config_error, InputSelector};
use super::selection::selected_entries;
use super::wire::CheckResourceIdentityInput;
use crate::config::project_settings::CheckTree;
use crate::mcp::handlers::run::CheckExecResult;

/// A slice larger than this is a configuration mistake, not a slice.
pub(crate) const MAX_SLICE_FILES: usize = 5_000;
pub(crate) const MAX_SLICE_BYTES: u64 = 64 * 1024 * 1024;
/// Slice lanes running at once on the runner.
const RUNNER_LANES: usize = 4;
/// The bound a lane gets when its plan carries none.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10 * 60);
/// The output kept for parsing and the recorded tail.
const OUTPUT_TAIL_BYTES: usize = 256 * 1024;
/// The executor id this lane's account names: the runner's own machine.
const RUNNER_CELL_ID: &str = "impact-slice";

fn lanes() -> &'static Semaphore {
    static LANES: OnceLock<Semaphore> = OnceLock::new();
    LANES.get_or_init(|| Semaphore::new(RUNNER_LANES))
}

/// The `impact` globs of an item that runs on the runner, or `None` when it
/// takes its configured cell path.
///
/// A lane runs here only when it declared `tree: impact` validly, when the
/// runner's platform is one whose answer counts as this check's verdict, and
/// when nothing pins it to another machine. Anything else is unchanged.
pub(crate) fn runner_globs(item: &PlannedCheckBatchItem) -> Option<Vec<String>> {
    let CheckResourceIdentityInput::Configured { check, .. } = &item.resource_identity else {
        return None;
    };
    if check.tree != CheckTree::Impact || impact_tree_config_error(check).is_some() {
        return None;
    }
    if !item
        .verdict_platforms
        .iter()
        .any(|platform| platform == std::env::consts::OS)
    {
        return None;
    }
    if !selector_admits_runner(item.executor.as_ref()) {
        return None;
    }
    check.impact.clone()
}

/// Whether an executor selector leaves this machine eligible: no selector, or
/// one that names no machine and no other platform.
fn selector_admits_runner(selector: Option<&ExecutorSelector>) -> bool {
    let Some(selector) = selector else {
        return true;
    };
    selector.name.as_deref().is_none_or(str::is_empty)
        && selector
            .os
            .as_deref()
            .is_none_or(|os| os == std::env::consts::OS)
}

/// Run every slice lane of `batch`, concurrently up to the runner's lane
/// bound, and answer each by its index.
pub(crate) async fn run_lanes(
    batch: &PlannedCheckBatchRequest,
    lanes: Vec<(PlannedCheckBatchItem, Vec<String>)>,
) -> HashMap<usize, Result<CheckExecResult, CheckExecutionFailure>> {
    let runs = lanes.into_iter().map(|(item, globs)| async move {
        let index = item.index;
        let _permit = self::lanes().acquire().await;
        (index, run_lane(batch, &item, &globs).await)
    });
    futures_util::future::join_all(runs)
        .await
        .into_iter()
        .collect()
}

async fn run_lane(
    batch: &PlannedCheckBatchRequest,
    item: &PlannedCheckBatchItem,
    globs: &[String],
) -> Result<CheckExecResult, CheckExecutionFailure> {
    let scratch = slices_root().join(format!(
        "{}-{}",
        item.input_hash.get(..16).unwrap_or(&item.input_hash),
        uuid::Uuid::new_v4().simple()
    ));
    // Owns the scratch directory and, once spawned, the command's process
    // group: a lane abandoned mid-run (its batch cancelled, its caller gone)
    // still kills every process it started and leaves nothing on disk.
    let cleanup = LaneCleanup {
        scratch: scratch.clone(),
        group: std::sync::Mutex::new(None),
    };
    let root = scratch.join("tree");
    let repository = PathBuf::from(&batch.repository);
    let commit = batch.sealed_commit.clone();
    let globs = globs.to_vec();
    let slice_root = root.clone();
    let materialized = tokio::task::spawn_blocking(move || {
        materialize(
            &repository,
            &commit,
            &InputSelector::from_globs(&globs),
            &slice_root,
        )
    })
    .await
    .map_err(|error| SliceError::Read(format!("slice task failed: {error}")))
    .and_then(|result| result);
    let result = match materialized {
        Ok(files) => {
            let env = lane_env(batch, item, &scratch);
            match env {
                Ok(env) => spawn(&root, &scratch, item, env, files, &cleanup).await,
                Err(error) => Err(CheckExecutionFailure::substrate(
                    SubstrateFailureShape::Preparation,
                    error,
                )),
            }
        }
        Err(SliceError::TooLarge(reason)) => Err(CheckExecutionFailure::Process(format!(
            "`{}` declares `tree: impact`, but {reason}. An impact slice holds at most {} files \
             and {} MB; narrow the check's `impact` globs, or declare `tree: full` so it runs \
             in a cell.",
            item.name,
            MAX_SLICE_FILES,
            MAX_SLICE_BYTES / (1024 * 1024)
        ))),
        Err(SliceError::Read(reason)) => Err(CheckExecutionFailure::substrate(
            SubstrateFailureShape::Preparation,
            format!(
                "the impact slice for `{}` could not be built: {reason}",
                item.name
            ),
        )),
    };
    drop(cleanup);
    result
}

struct LaneCleanup {
    scratch: PathBuf,
    /// The command's process group, once it has one.
    group: std::sync::Mutex<Option<u32>>,
}

impl Drop for LaneCleanup {
    fn drop(&mut self) {
        // `kill_on_drop` reaches only the shell that leads the group; the tools
        // it started are the group's other members, and this is what ends them.
        #[cfg(unix)]
        if let Some(group) = self.group.lock().ok().and_then(|group| *group) {
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(group as i32),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
        let _ = std::fs::remove_dir_all(&self.scratch);
    }
}

fn slices_root() -> PathBuf {
    cairn_common::paths::cairn_home().join("check-slices")
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SliceError {
    /// The selection exceeds the slice bounds: a declaration to fix.
    TooLarge(String),
    /// The store could not produce the selection.
    Read(String),
}

/// Write the entries `selector` takes from `commit`'s tree under `root`,
/// keeping file modes and symlinks, and return how many were written.
pub(crate) fn materialize(
    repository: &Path,
    commit: &str,
    selector: &InputSelector,
    root: &Path,
) -> Result<usize, SliceError> {
    let store = ObjectStore::new(repository, None).map_err(SliceError::Read)?;
    let limits = ObjectReadLimits::new(
        256,
        2_000_000,
        MAX_SLICE_BYTES as usize,
        Duration::from_secs(60),
    );
    let items = store
        .walk_commit(commit, "", &limits)
        .map_err(|error| SliceError::Read(error.to_string()))?;
    let modes: HashMap<&str, u16> = items
        .iter()
        .filter(|item| item.kind == TreeEntryKind::Blob)
        .map(|item| (item.path.as_str(), item.mode))
        .collect();
    let entries: Vec<(String, String)> = items
        .iter()
        .filter(|item| item.kind == TreeEntryKind::Blob)
        .map(|item| (item.path.clone(), item.oid.clone()))
        .collect();
    let selected = selected_entries(&entries, selector);
    if selected.len() > MAX_SLICE_FILES {
        return Err(SliceError::TooLarge(format!(
            "its globs select {} files at this commit",
            selected.len()
        )));
    }
    std::fs::create_dir_all(root).map_err(|error| SliceError::Read(error.to_string()))?;
    let mut total: u64 = 0;
    for (path, oid) in &selected {
        let bytes = store
            .blob(oid, &limits)
            .map_err(|error| SliceError::Read(format!("{path}: {error}")))?;
        total += bytes.len() as u64;
        if total > MAX_SLICE_BYTES {
            return Err(SliceError::TooLarge(format!(
                "its globs select more than {} MB at this commit",
                MAX_SLICE_BYTES / (1024 * 1024)
            )));
        }
        let target = root.join(path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|error| SliceError::Read(error.to_string()))?;
        }
        write_entry(
            &target,
            &bytes,
            modes.get(path.as_str()).copied().unwrap_or(0o100644),
        )
        .map_err(|error| SliceError::Read(format!("{path}: {error}")))?;
    }
    Ok(selected.len())
}

fn write_entry(target: &Path, bytes: &[u8], mode: u16) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if mode == 0o120000 {
            use std::os::unix::ffi::OsStrExt as _;
            return std::os::unix::fs::symlink(std::ffi::OsStr::from_bytes(bytes), target);
        }
        std::fs::write(target, bytes)?;
        if mode == 0o100755 {
            std::fs::set_permissions(target, std::fs::Permissions::from_mode(0o755))?;
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = mode;
        std::fs::write(target, bytes)
    }
}

/// The content transports a check may carry, each paired with the variable that
/// names the file its content is written to.
const CONTENT_TRANSPORTS: [(&str, &str); 2] = [
    (
        "CAIRN_CHECK_CHANGED_FILES_CONTENT",
        "CAIRN_CHECK_CHANGED_FILES",
    ),
    (
        "CAIRN_CHECK_BASE_CONTENT_CONTENT",
        "CAIRN_CHECK_BASE_CONTENT",
    ),
];

/// The environment a cell lane would get: the batch's variables overlaid by the
/// item's own, with the `*_CONTENT` transports written to files beside (never
/// inside) the slice, exactly as the fleet does for a cell.
///
/// The overlay is by key, the item's value winning, so each variable reaches
/// the command once. A transport that survived as a second copy would go to
/// `execve` as an environment string, and an executor base-content bundle is
/// megabytes: on its own it exceeds `ARG_MAX`, and the lane cannot start.
fn lane_env(
    batch: &PlannedCheckBatchRequest,
    item: &PlannedCheckBatchItem,
    scratch: &Path,
) -> Result<Vec<(String, String)>, String> {
    let mut env: Vec<(String, String)> = Vec::new();
    for (key, value) in batch.env.iter().chain(&item.env) {
        overlay(&mut env, key, value.clone());
    }
    for (content_env, file_env) in CONTENT_TRANSPORTS {
        if let Some(position) = env.iter().position(|(key, _)| key == content_env) {
            let (_, contents) = env.remove(position);
            let path = scratch.join(file_env.to_ascii_lowercase());
            std::fs::write(&path, contents)
                .map_err(|error| format!("write {file_env} for the slice: {error}"))?;
            overlay(&mut env, file_env, path.to_string_lossy().into_owned());
        }
    }
    overlay(&mut env, "PATH", crate::env::agent_shell_path());
    overlay(
        &mut env,
        "CAIRN_HOME",
        cairn_common::paths::cairn_home()
            .to_string_lossy()
            .into_owned(),
    );
    overlay(&mut env, "TMPDIR", scratch.to_string_lossy().into_owned());
    // The slice lives under the Cairn home, which may itself be a repository;
    // a bare `git` in the command must find no repository, not that one.
    overlay(
        &mut env,
        "GIT_CEILING_DIRECTORIES",
        scratch.to_string_lossy().into_owned(),
    );
    Ok(env)
}

/// Set `key` in `env`, replacing every earlier value it held.
fn overlay(env: &mut Vec<(String, String)>, key: &str, value: String) {
    env.retain(|(existing, _)| existing != key);
    env.push((key.to_string(), value));
}

/// The size of the environment a lane sets, as `execve` receives it (each
/// variable is one `KEY=VALUE` string with its terminator).
#[derive(Debug, PartialEq, Eq)]
struct EnvFootprint {
    bytes: usize,
    variables: usize,
    /// The largest variable and its size.
    largest: Option<(String, usize)>,
}

impl EnvFootprint {
    fn of(env: &[(String, String)]) -> Self {
        let size = |(key, value): &(String, String)| key.len() + value.len() + 2;
        Self {
            bytes: env.iter().map(size).sum(),
            variables: env.len(),
            largest: env
                .iter()
                .max_by_key(|variable| size(variable))
                .map(|variable| (variable.0.clone(), size(variable))),
        }
    }
}

/// Why a lane could not start. When the kernel refused its arguments and
/// environment as too large, say how large and which variable dominates, so an
/// `os error 7` names what was too long.
fn spawn_failure(error: &std::io::Error, command: &str, footprint: &EnvFootprint) -> String {
    let mut message = format!("spawn the slice lane: {error}");
    #[cfg(unix)]
    if error.raw_os_error() == Some(nix::errno::Errno::E2BIG as i32) {
        message.push_str(&format!(
            ". The lane sets {} environment variables totalling {} bytes",
            footprint.variables, footprint.bytes
        ));
        if let Some((key, bytes)) = &footprint.largest {
            message.push_str(&format!(" (the largest is `{key}`, {bytes} bytes)"));
        }
        message.push_str(&format!(
            " and runs a {}-byte command; with the environment it inherits from the \
             runner, that exceeds the system's argument limit (ARG_MAX)",
            command.len()
        ));
    }
    #[cfg(not(unix))]
    let _ = (command, footprint);
    message
}

async fn spawn(
    root: &Path,
    scratch: &Path,
    item: &PlannedCheckBatchItem,
    env: Vec<(String, String)>,
    files: usize,
    cleanup: &LaneCleanup,
) -> Result<CheckExecResult, CheckExecutionFailure> {
    let output_path = scratch.join("output.log");
    let output = std::fs::File::create(&output_path).map_err(|error| {
        CheckExecutionFailure::substrate(
            SubstrateFailureShape::Preparation,
            format!("create the slice lane's output file: {error}"),
        )
    })?;
    let stderr = output.try_clone().map_err(|error| {
        CheckExecutionFailure::substrate(SubstrateFailureShape::Preparation, error.to_string())
    })?;
    let mut command = tokio::process::Command::new("bash");
    command
        .arg("-c")
        .arg(&item.command)
        .current_dir(root)
        .stdin(std::process::Stdio::null())
        .stdout(output)
        .stderr(stderr)
        .kill_on_drop(true);
    let footprint = EnvFootprint::of(&env);
    for (key, value) in env {
        command.env(key, value);
    }
    #[cfg(unix)]
    command.process_group(0);
    let timeout = if item.timeout_ms == 0 {
        DEFAULT_TIMEOUT
    } else {
        Duration::from_millis(item.timeout_ms)
    };
    let started_at = unix_ms();
    let started = Instant::now();
    let mut child = command.spawn().map_err(|error| {
        CheckExecutionFailure::substrate(
            SubstrateFailureShape::Preparation,
            spawn_failure(&error, &item.command, &footprint),
        )
    })?;
    if let Ok(mut group) = cleanup.group.lock() {
        *group = child.id();
    }
    let (exit_code, timed_out) = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) => (status.code(), false),
        Ok(Err(error)) => {
            return Err(CheckExecutionFailure::substrate(
                SubstrateFailureShape::Result,
                format!("wait for the slice lane: {error}"),
            ))
        }
        Err(_) => {
            kill_group(&mut child).await;
            (None, true)
        }
    };
    let duration_ms = started.elapsed().as_millis() as u64;
    let body = read_tail(&output_path);
    let header = format!("ran on runner: impact slice, {files} files\n");
    Ok(CheckExecResult {
        exit_code,
        output: header + &body,
        timed_out,
        duration_ms: Some(duration_ms as i64),
        provenance: Some(runner_account(started_at, duration_ms)),
        publication: None,
    })
}

async fn kill_group(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
    let _ = child.kill().await;
}

fn read_tail(path: &Path) -> String {
    let bytes = std::fs::read(path).unwrap_or_default();
    let start = bytes.len().saturating_sub(OUTPUT_TAIL_BYTES);
    String::from_utf8_lossy(&bytes[start..]).into_owned()
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

/// The account a slice lane records: the runner's own machine, as the
/// colocated executor. Environment, toolchain, and verdict-environment fields
/// are left for the recorder, which fills them from this process when the
/// executor is the coordinator itself.
fn runner_account(started_at: u64, duration_ms: u64) -> CellExecutionMeta {
    CellExecutionMeta {
        warmth: None,
        load_context: None,
        cell_origin: None,
        executor_id: crate::fleet::COLOCATED_EXECUTOR_ID.to_string(),
        executor_device_id: String::new(),
        executor_connection_generation: 0,
        cell_id: RUNNER_CELL_ID.to_string(),
        cell_epoch: 0,
        started_at_unix_ms: started_at,
        finished_at_unix_ms: started_at + duration_ms,
        duration_ms: Some(duration_ms),
        peak_rss_bytes: None,
        peak_physical_footprint_bytes: None,
        cpu: None,
        allocation: None,
        disk_delta_bytes: None,
        measurement_quality: None,
        environment_fingerprint: String::new(),
        verdict_platform: Some(std::env::consts::OS.to_string()),
        verdict_arch: Some(std::env::consts::ARCH.to_string()),
        verdict_environment_hash: None,
        sandbox: None,
        toolchain_fingerprint: None,
        containment: None,
        stop: None,
    }
}

#[cfg(test)]
mod tests;
