//! Windows service lifecycle, in the two shapes of [`WindowsAutostart`].
//!
//! The runner's HKCU Run value is the per-user, zero-elevation autostart store
//! that matches a desktop daemon: the current interactive user can write it at
//! medium integrity and Explorer launches it at logon. What the Run key cannot
//! do is supervise. It expresses no restart policy, so a daemon that dies stays
//! dead until the next logon — stated here rather than papered over, because
//! the launchd and systemd backends do restart and code reading only one of
//! them would assume parity.
//!
//! The executor's scheduled task is sequenced in [`super::windows_task`]; what
//! lives here is [`SystemTaskHost`], its real Task Scheduler and process table.

use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use serde::Deserialize;
use windows_sys::Win32::Foundation::{ERROR_FILE_NOT_FOUND, ERROR_SUCCESS};
use windows_sys::Win32::System::Registry::{
    RegCloseKey, RegCreateKeyW, RegDeleteValueW, RegOpenKeyExW, RegQueryValueExW, RegSetValueExW,
    HKEY, HKEY_CURRENT_USER, KEY_READ, KEY_SET_VALUE, REG_SZ,
};

use super::windows_task::{
    self, ExecutorTask, ScheduledTaskState, StartWait, TaskHost, TaskRefusal,
};
use super::{
    parse_run_value, split_command_line, tail_lines, windows_run_command, LegacyIdentity,
    RegisteredUnit, ServiceChange, ServiceSpec, TerminatedProcess, WindowsAutostart,
    RETIRED_EXECUTOR_RUN_VALUE,
};

const RUN_KEY_SUBPATH: &str = r"Software\Microsoft\Windows\CurrentVersion\Run";

/// `CREATE_NO_WINDOW`: the desktop and release daemons are GUI-subsystem
/// processes, so spawning console helpers should not allocate a visible console
/// window.
const CREATE_NO_WINDOW: u32 = 0x08000000;
/// `DETACHED_PROCESS`: the daemon spawned by `install`/`restart` must outlive the
/// short-lived service command process.
const DETACHED_PROCESS: u32 = 0x00000008;

/// The spec's scheduled task, when it registers as one.
fn executor_task(spec: &ServiceSpec) -> Option<ExecutorTask<'_>> {
    match &spec.windows {
        WindowsAutostart::ScheduledTask { name, config } => Some(ExecutorTask {
            name,
            executable: &spec.exe,
            config,
        }),
        WindowsAutostart::RunValue(_) => None,
    }
}

pub fn install(spec: &ServiceSpec) -> Result<ServiceChange, String> {
    if let Some(task) = executor_task(spec) {
        return windows_task::install(&task, &SystemTaskHost, StartWait::default())
            .map(|ended| ServiceChange { ended });
    }
    cleanup_legacy(spec);
    write_run_value(spec.windows.name(), &windows_run_command(spec))?;
    spawn_daemon(spec);
    Ok(ServiceChange::default())
}

pub fn uninstall(spec: &ServiceSpec) -> Result<ServiceChange, String> {
    if let Some(task) = executor_task(spec) {
        return windows_task::uninstall(&task, &SystemTaskHost)
            .map(|ended| ServiceChange { ended });
    }
    delete_run_value_named(spec.windows.name())?;
    if let Some(legacy) = spec.legacy.as_ref().and_then(|l| l.run_value.as_deref()) {
        let _ = delete_run_value_named(legacy);
    }
    Ok(ServiceChange::default())
}

pub fn unregister_from_within(spec: &ServiceSpec) -> Result<(), String> {
    match executor_task(spec) {
        Some(task) => windows_task::unregister_from_within(&task, &SystemTaskHost),
        None => uninstall(spec).map(|_| ()),
    }
}

/// A Run value has no supervisor, so there is no restart policy to suspend:
/// the caller ends the running process, and the value itself acts only at
/// logon. A scheduled task's instance is ended; its next tick starts it again.
pub fn stop(spec: &ServiceSpec) -> Result<(), String> {
    let Some(task) = executor_task(spec) else {
        return Ok(());
    };
    match SystemTaskHost.query(task.name)? {
        Some(state) if state.running() => SystemTaskHost.end(task.name).map_err(|refusal| {
            format!("end the scheduled task {}: {}", task.name, refusal.message)
        }),
        _ => Ok(()),
    }
}

/// Ensure the Run value points at this binary and spawn the current daemon. This
/// is the Windows adoption path after an app update: the new binary is launched
/// directly, since the Run key alone would not take effect until the next logon.
/// A scheduled task's install already ends any running instance and starts it
/// again.
pub fn restart(spec: &ServiceSpec) -> Result<(), String> {
    if executor_task(spec).is_some() {
        return install(spec).map(|_| ());
    }
    cleanup_legacy(spec);
    let name = spec.windows.name();
    let desired = windows_run_command(spec);
    if read_run_value(name).as_deref() != Some(desired.as_str()) {
        write_run_value(name, &desired)?;
    }
    spawn_daemon(spec);
    Ok(())
}

/// Refresh only a value that already describes THIS instance. HKCU is per user
/// while a machine can host several Cairn instances (`~/.cairn` vs
/// `~/.cairn-dev`), so a manually-started dev daemon must not rewrite a
/// production autostart value merely because its executable path differs. The
/// guard is therefore the argument tail: same arguments, stale program path.
///
/// A scheduled task is never refreshed from here: only an administrator can
/// register one, and its action names the stable installed path, which a
/// self-update renames in place. `service install` reports a task that has
/// drifted.
pub fn refresh_if_stale(spec: &ServiceSpec) {
    if executor_task(spec).is_some() {
        return;
    }
    let name = spec.windows.name();
    let Some(existing) = read_run_value(name) else {
        return;
    };
    let desired = windows_run_command(spec);
    if existing == desired {
        return;
    }
    if argument_tail(&existing) != argument_tail(&desired) {
        return;
    }
    match write_run_value(name, &desired) {
        Ok(()) => tracing::info!(
            "windows autostart value drifted; rewrote HKCU Run value {name} (applies at the next logon)"
        ),
        Err(e) => tracing::warn!("windows autostart refresh: rewrite {name} failed: {e}"),
    }
}

fn query_task(spec: &ServiceSpec) -> Option<Option<ScheduledTaskState>> {
    let task = executor_task(spec)?;
    match SystemTaskHost.query(task.name) {
        Ok(state) => Some(state),
        Err(error) => {
            tracing::warn!(task = task.name, %error, "query the scheduled task");
            Some(None)
        }
    }
}

/// For the executor's task: registered, and installed by `service install`
/// since the last uninstall. The task itself outlives an uninstall, so its
/// existence alone would read every uninstalled executor as installed.
pub fn is_installed(spec: &ServiceSpec) -> bool {
    match executor_task(spec) {
        Some(task) => {
            windows_task::executor_installed(task.config)
                && query_task(spec).is_some_and(|state| state.is_some())
        }
        None => read_run_value(spec.windows.name()).is_some(),
    }
}

pub fn is_loaded(spec: &ServiceSpec) -> bool {
    match executor_task(spec) {
        Some(task) => {
            windows_task::executor_installed(task.config)
                && query_task(spec).is_some_and(|state| state.is_some_and(|state| state.enabled))
        }
        None => is_installed(spec),
    }
}

pub fn location(_spec: &ServiceSpec) -> Option<PathBuf> {
    None
}

/// The Run key starts a process at logon and keeps no account of it; a
/// scheduled task's account is the scheduler's.
pub fn runtime(spec: &ServiceSpec) -> Option<super::ServiceRuntime> {
    query_task(spec)?.as_ref().map(windows_task::runtime_of)
}

/// What the executor's supervisor wrote of the daemon's stderr. A Run value's
/// daemon has its std handles nulled, so there is nothing to read.
pub fn recent_log(spec: &ServiceSpec, lines: usize) -> Option<String> {
    executor_task(spec)?;
    std::fs::read(spec.log_path("err"))
        .ok()
        .map(|bytes| tail_lines(&String::from_utf8_lossy(&bytes), lines))
}

/// Everything after the quoted program path, which is what identifies WHICH
/// instance a Run value describes.
fn argument_tail(value: &str) -> &str {
    match value
        .strip_prefix('"')
        .and_then(|rest| rest.find('"').map(|end| &rest[end + 1..]))
    {
        Some(tail) => tail.trim_start(),
        None => value,
    }
}

fn spawn_daemon(spec: &ServiceSpec) {
    let mut command = Command::new(&spec.exe);
    command.args(&spec.args);
    // The Run value itself carries no environment (see `windows_run_command`), so
    // this only shapes the process THIS install starts; a logon-time launch
    // depends on the arguments alone.
    for (key, value) in &spec.env {
        command.env(key, value);
    }
    // NUL stdio is load-bearing. `install`/`restart` are run by the desktop
    // under `std::process::Command::output()`, which reads the subprocess's
    // stdout/stderr pipes until EOF. With inherited (default) stdio this
    // detached daemon would inherit those pipe write-ends and hold them open
    // for its entire life, so the caller's `.output()` would block forever
    // waiting for an EOF that never comes — the "Starting Cairn…" spin. Cairn
    // daemons log through their internal JSONL logger, so nulling their std
    // handles loses nothing.
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .creation_flags(CREATE_NO_WINDOW | DETACHED_PROCESS);
    match command.spawn() {
        Ok(child) => {
            tracing::info!("spawned detached {} pid={}", spec.exe.display(), child.id());
            drop(child);
        }
        Err(e) => tracing::warn!("spawn detached {} failed: {e}", spec.exe.display()),
    }
}

fn cleanup_legacy(spec: &ServiceSpec) {
    if let Some(legacy) = spec.legacy.as_ref() {
        let _ = evict_legacy(legacy);
    }
}

/// The Run value registered under `identity`. The Run key keeps no account of
/// the process it started, so there is no runtime; the value itself is
/// returned as the command line that process was started with.
pub fn describe_unit(identity: &LegacyIdentity) -> Result<Option<RegisteredUnit>, String> {
    let Some(name) = identity.run_value.as_deref() else {
        return Ok(None);
    };
    let Some(value) = try_read_run_value(name)? else {
        return Ok(None);
    };
    Ok(Some(RegisteredUnit {
        name: name.to_string(),
        location: None,
        description: parse_run_value(&value),
        runtime: None,
        command_line: Some(value),
    }))
}

/// Delete the Run value. This stops nothing: what it started at logon keeps
/// running, which [`terminate_run_value_processes`] ends (cairn/4845).
pub fn evict_legacy(identity: &LegacyIdentity) -> Result<(), String> {
    match identity.run_value.as_deref() {
        Some(name) => delete_run_value_named(name),
        None => Ok(()),
    }
}

/// End every process a Run value launched, and what each started: same
/// executable, same argument tail. The argument tail is what tells one
/// instance from another ([`refresh_if_stale`]), so a process of the same
/// program running against a different home is not touched.
pub fn terminate_run_value_processes(command_line: &str) -> Result<Vec<TerminatedProcess>, String> {
    let program = parse_run_value(command_line)
        .program
        .ok_or_else(|| "the Run value names no program".to_string())?;
    let wanted_tail = argument_tail(command_line).trim().to_string();
    terminate_trees(&program, |line| argument_tail(line).trim() == wanted_tail)
}

/// Whether `line` runs its program with exactly one of `argument_sets`.
/// Compared without case, because every word that can differ is a Windows
/// path; the program word is the caller's to check, against the image.
fn runs_one_of(line: &str, argument_sets: &[Vec<String>]) -> bool {
    let words = split_command_line(line);
    let Some(arguments) = words.get(1..) else {
        return false;
    };
    argument_sets.iter().any(|set| {
        set.len() == arguments.len()
            && set
                .iter()
                .zip(arguments)
                .all(|(wanted, found)| wanted.eq_ignore_ascii_case(found))
    })
}

/// A process as `Win32_Process` lists it.
struct ListedProcess {
    pid: u32,
    parent: Option<u32>,
    executable: Option<String>,
    command_line: Option<String>,
}

/// This machine's processes, and the instant the listing was taken.
fn list_processes() -> Result<(Vec<ListedProcess>, u64), String> {
    let listed_at_ms = now_unix_ms();
    let stdout = powershell(
        "ConvertTo-Json -Compress -InputObject @(Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId,ExecutablePath,CommandLine)",
    )
    .map_err(|error| format!("enumerate processes: {error}"))?;
    let processes: Vec<serde_json::Value> =
        serde_json::from_str(&stdout).map_err(|error| format!("parse process list: {error}"))?;
    let number = |process: &serde_json::Value, name: &str| {
        process
            .get(name)
            .and_then(serde_json::Value::as_u64)
            .map(|value| value as u32)
    };
    let text = |process: &serde_json::Value, name: &str| {
        process
            .get(name)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    };
    let listed = processes
        .iter()
        .filter_map(|process| {
            Some(ListedProcess {
                pid: number(process, "ProcessId")?,
                parent: number(process, "ParentProcessId"),
                executable: text(process, "ExecutablePath"),
                command_line: text(process, "CommandLine"),
            })
        })
        .collect();
    Ok((listed, listed_at_ms))
}

/// The listed processes running `program` whose command line `is_root`
/// accepts.
fn matching_roots<'a>(
    processes: &'a [ListedProcess],
    program: &Path,
    is_root: impl Fn(&str) -> bool,
) -> Vec<&'a ListedProcess> {
    let program = program.to_string_lossy().to_lowercase();
    processes
        .iter()
        .filter(|process| {
            matches!((&process.executable, &process.command_line), (Some(executable), Some(line))
                if executable.to_lowercase() == program && is_root(line))
        })
        .collect()
}

/// End every process running `program` whose command line `is_root` accepts,
/// and everything each started.
///
/// A pid read from a process listing names nothing by the time it is acted
/// on, so each one is opened first and every check runs through the held
/// handle, which keeps the pid from being reused until it is closed: the
/// process must have existed when the listing was taken (its creation time is
/// no later than that), a root must still run `program`, and a descendant must
/// have been created no earlier than the parent it was listed under. It is
/// then terminated and waited on through the same handle, and reported with
/// its creation time as its start token.
fn terminate_trees(
    program: &Path,
    is_root: impl Fn(&str) -> bool,
) -> Result<Vec<TerminatedProcess>, String> {
    let (processes, listed_at_ms) = list_processes()?;
    let roots: Vec<u32> = matching_roots(&processes, program, is_root)
        .iter()
        .map(|process| process.pid)
        .collect();
    let program = program.to_string_lossy().to_lowercase();
    let mut ended = Vec::new();
    // (pid, the verified creation time of the parent it was listed under)
    let mut queue: std::collections::VecDeque<(u32, Option<u64>)> =
        roots.into_iter().map(|pid| (pid, None)).collect();
    let mut seen = std::collections::HashSet::new();
    while let Some((pid, parent_created)) = queue.pop_front() {
        if !seen.insert(pid) {
            continue;
        }
        let expected = parent_created.is_none().then_some(program.as_str());
        let Some(open) = OpenedProcess::open(pid)? else {
            continue;
        };
        let created = open.created_unix_ms()?;
        if created > listed_at_ms || parent_created.is_some_and(|parent| created < parent) {
            continue;
        }
        if let Some(expected) = expected {
            if open.image_path()?.to_lowercase() != expected {
                continue;
            }
        }
        // Children first in the listing order, through this verified parent.
        for child in processes
            .iter()
            .filter(|process| process.parent == Some(pid))
        {
            queue.push_back((child.pid, Some(created)));
        }
        open.terminate_and_wait()?;
        ended.push(TerminatedProcess {
            pid,
            start_token: created.to_string(),
            command_line: processes
                .iter()
                .find(|process| process.pid == pid)
                .and_then(|process| process.command_line.clone()),
        });
    }
    Ok(ended)
}

/// Run a PowerShell script and return its stdout, read as UTF-8.
fn powershell(script: &str) -> Result<String, String> {
    let output = Command::new("powershell")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            &format!("[Console]::OutputEncoding = [Text.Encoding]::UTF8; {script}"),
        ])
        .stdin(Stdio::null())
        .creation_flags(CREATE_NO_WINDOW)
        .output()
        .map_err(|error| format!("run powershell: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// A task name as a PowerShell single-quoted literal.
fn quoted(name: &str) -> String {
    format!("'{}'", name.replace('\'', "''"))
}

#[derive(Deserialize)]
struct OperationOutcome {
    ok: bool,
    #[serde(default)]
    code: Option<i64>,
    #[serde(default)]
    message: Option<String>,
}

/// Windows's Task Scheduler and process table, as this account sees them.
///
/// Every scheduler operation goes through the ScheduledTasks cmdlets rather
/// than `schtasks.exe`, because their refusals carry the HRESULT
/// (`error_Code` on the CIM error) where schtasks prints only localized text.
pub struct SystemTaskHost;

impl SystemTaskHost {
    fn operate(&self, cmdlet: &str, name: &str, extra: &str) -> Result<(), TaskRefusal> {
        let script = format!(
            "try {{ {cmdlet} -TaskPath '\\' -TaskName {name} -ErrorAction Stop {extra} | Out-Null; '{{\"ok\":true}}' }} \
             catch {{ $e = $_.Exception; $code = $null; \
             if ($e.ErrorData) {{ $p = $e.ErrorData.CimInstanceProperties['error_Code']; if ($p -and $null -ne $p.Value) {{ $code = [int64]$p.Value }} }}; \
             if ($null -eq $code) {{ $code = [int64]$e.HResult }}; \
             [pscustomobject]@{{ ok = $false; code = $code; message = $e.Message.Trim() }} | ConvertTo-Json -Compress }}",
            name = quoted(name)
        );
        let stdout = powershell(&script).map_err(|message| TaskRefusal {
            hresult: None,
            message,
        })?;
        let outcome: OperationOutcome =
            serde_json::from_str(stdout.trim()).map_err(|error| TaskRefusal {
                hresult: None,
                message: format!("read the outcome of {cmdlet}: {error}: {}", stdout.trim()),
            })?;
        if outcome.ok {
            Ok(())
        } else {
            Err(TaskRefusal {
                hresult: outcome.code.map(|code| code as u32),
                message: outcome
                    .message
                    .unwrap_or_else(|| format!("{cmdlet} failed")),
            })
        }
    }
}

impl TaskHost for SystemTaskHost {
    fn account(&self) -> String {
        std::env::var("USERNAME").unwrap_or_else(|_| "this account".into())
    }

    fn profile(&self) -> Option<String> {
        std::env::var("USERPROFILE")
            .ok()
            .filter(|profile| !profile.is_empty())
    }

    fn user_sid(&self) -> Result<String, String> {
        powershell("[Security.Principal.WindowsIdentity]::GetCurrent().User.Value")
            .map(|sid| sid.trim().to_string())
            .and_then(|sid| {
                if sid.starts_with("S-1-") {
                    Ok(sid)
                } else {
                    Err(format!("read this account's SID: got `{sid}`"))
                }
            })
    }

    fn query(&self, name: &str) -> Result<Option<ScheduledTaskState>, String> {
        let script = format!(
            "try {{ $t = Get-ScheduledTask -TaskPath '\\' -TaskName {name} -ErrorAction Stop }} \
             catch {{ if ($_.CategoryInfo.Category -eq 'ObjectNotFound') {{ 'null'; exit 0 }} else {{ throw }} }}; \
             $i = $t | Get-ScheduledTaskInfo; \
             $x = Export-ScheduledTask -TaskPath '\\' -TaskName {name}; \
             $u = [string]$t.Principal.UserId; \
             $sid = if ($u -match '^S-1-') {{ $u }} else {{ try {{ (New-Object Security.Principal.NTAccount $u).Translate([Security.Principal.SecurityIdentifier]).Value }} catch {{ $null }} }}; \
             $run = if ($i.LastRunTime) {{ $i.LastRunTime.ToUniversalTime().ToString('o') }} else {{ $null }}; \
             [pscustomobject]@{{ state = [int]$t.State; enabled = [bool]$t.Settings.Enabled; lastTaskResult = [int64]$i.LastTaskResult; lastRunTime = $run; userSid = $sid; xml = [string]$x }} | ConvertTo-Json -Compress",
            name = quoted(name)
        );
        let stdout = powershell(&script)
            .map_err(|error| format!("query the scheduled task {name}: {error}"))?;
        windows_task::parse_scheduled_task_query(&stdout)
    }

    fn run(&self, name: &str) -> Result<(), TaskRefusal> {
        self.operate("Start-ScheduledTask", name, "")
    }

    fn end(&self, name: &str) -> Result<(), TaskRefusal> {
        self.operate("Stop-ScheduledTask", name, "")
    }

    fn retired_run_value(&self) -> Result<Option<String>, String> {
        try_read_run_value(RETIRED_EXECUTOR_RUN_VALUE)
    }

    fn delete_retired_run_value(&self) -> Result<(), String> {
        delete_run_value_named(RETIRED_EXECUTOR_RUN_VALUE)
    }

    fn terminate_run_value_processes(
        &self,
        command_line: &str,
    ) -> Result<Vec<TerminatedProcess>, String> {
        terminate_run_value_processes(command_line)
    }

    fn terminate_instances(
        &self,
        program: &Path,
        argument_sets: &[Vec<String>],
    ) -> Result<Vec<TerminatedProcess>, String> {
        terminate_trees(program, |line| runs_one_of(line, argument_sets))
    }

    fn find_instances(
        &self,
        program: &Path,
        argument_sets: &[Vec<String>],
    ) -> Result<Vec<u32>, String> {
        let (processes, _) = list_processes()?;
        Ok(
            matching_roots(&processes, program, |line| runs_one_of(line, argument_sets))
                .iter()
                .map(|process| process.pid)
                .collect(),
        )
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// A process held open by handle, so its pid cannot be reused while held.
struct OpenedProcess(windows_sys::Win32::Foundation::HANDLE, u32);

impl OpenedProcess {
    /// `None` when no such process exists any more.
    fn open(pid: u32) -> Result<Option<Self>, String> {
        use windows_sys::Win32::Foundation::ERROR_INVALID_PARAMETER;
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE,
        };
        let handle = unsafe {
            OpenProcess(
                PROCESS_TERMINATE | PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            )
        };
        if handle.is_null() {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) {
                return Ok(None);
            }
            return Err(format!("open process {pid}: {error}"));
        }
        Ok(Some(Self(handle, pid)))
    }

    fn created_unix_ms(&self) -> Result<u64, String> {
        use windows_sys::Win32::Foundation::FILETIME;
        use windows_sys::Win32::System::Threading::GetProcessTimes;
        let zero = || FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let (mut created, mut exited, mut kernel, mut user) = (zero(), zero(), zero(), zero());
        if unsafe { GetProcessTimes(self.0, &mut created, &mut exited, &mut kernel, &mut user) }
            == 0
        {
            return Err(format!(
                "read the creation time of process {}: {}",
                self.1,
                std::io::Error::last_os_error()
            ));
        }
        // 100ns intervals since 1601-01-01.
        let ticks = (u64::from(created.dwHighDateTime) << 32) | u64::from(created.dwLowDateTime);
        Ok((ticks / 10_000).saturating_sub(11_644_473_600_000))
    }

    fn image_path(&self) -> Result<String, String> {
        use windows_sys::Win32::System::Threading::{
            QueryFullProcessImageNameW, PROCESS_NAME_WIN32,
        };
        let mut buffer = vec![0u16; 32_768];
        let mut size = buffer.len() as u32;
        if unsafe {
            QueryFullProcessImageNameW(self.0, PROCESS_NAME_WIN32, buffer.as_mut_ptr(), &mut size)
        } == 0
        {
            return Err(format!(
                "read the image of process {}: {}",
                self.1,
                std::io::Error::last_os_error()
            ));
        }
        Ok(String::from_utf16_lossy(&buffer[..size as usize]))
    }

    fn terminate_and_wait(&self) -> Result<(), String> {
        use windows_sys::Win32::Foundation::WAIT_OBJECT_0;
        use windows_sys::Win32::System::Threading::{TerminateProcess, WaitForSingleObject};
        if unsafe { TerminateProcess(self.0, 1) } == 0 {
            let error = std::io::Error::last_os_error();
            // Already exiting: the wait below decides.
            tracing::debug!(pid = self.1, %error, "TerminateProcess refused");
        }
        if unsafe { WaitForSingleObject(self.0, 5_000) } != WAIT_OBJECT_0 {
            return Err(format!(
                "process {} did not exit after it was terminated",
                self.1
            ));
        }
        Ok(())
    }
}

impl Drop for OpenedProcess {
    fn drop(&mut self) {
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
    }
}

fn write_run_value(name: &str, value: &str) -> Result<(), String> {
    let key = create_run_key()?;
    let _guard = RegKeyGuard(key);
    let wide = wide_null(value);
    let bytes = (wide.len() * std::mem::size_of::<u16>()) as u32;
    let status = unsafe {
        RegSetValueExW(
            key,
            wide_null(name).as_ptr(),
            0,
            REG_SZ,
            wide.as_ptr().cast::<u8>(),
            bytes,
        )
    };
    status_to_result(status, "set HKCU Run value")
}

fn read_run_value(name: &str) -> Option<String> {
    match try_read_run_value(name) {
        Ok(value) => value,
        Err(e) => {
            tracing::warn!("read HKCU Run value {name}: {e}");
            None
        }
    }
}

fn try_read_run_value(value_name: &str) -> Result<Option<String>, String> {
    let Some(key) = open_run_key_with_access(KEY_READ)? else {
        return Ok(None);
    };
    let _guard = RegKeyGuard(key);
    let name = wide_null(value_name);
    let mut ty = 0u32;
    let mut bytes = 0u32;
    let status = unsafe {
        RegQueryValueExW(
            key,
            name.as_ptr(),
            std::ptr::null_mut(),
            &mut ty,
            std::ptr::null_mut(),
            &mut bytes,
        )
    };
    if status == ERROR_FILE_NOT_FOUND {
        return Ok(None);
    }
    status_to_result(status, "query HKCU Run value size")?;
    if ty != REG_SZ || bytes == 0 {
        return Ok(None);
    }
    let mut buf = vec![0u16; bytes.div_ceil(2) as usize];
    let status = unsafe {
        RegQueryValueExW(
            key,
            name.as_ptr(),
            std::ptr::null_mut(),
            &mut ty,
            buf.as_mut_ptr().cast::<u8>(),
            &mut bytes,
        )
    };
    if status == ERROR_FILE_NOT_FOUND {
        return Ok(None);
    }
    status_to_result(status, "query HKCU Run value")?;
    let end = buf.iter().position(|unit| *unit == 0).unwrap_or(buf.len());
    Ok(Some(String::from_utf16_lossy(&buf[..end])))
}

fn delete_run_value_named(name: &str) -> Result<(), String> {
    let Some(key) = open_run_key_with_access(KEY_SET_VALUE)? else {
        return Ok(());
    };
    let _guard = RegKeyGuard(key);
    let status = unsafe { RegDeleteValueW(key, wide_null(name).as_ptr()) };
    if status == ERROR_FILE_NOT_FOUND {
        return Ok(());
    }
    status_to_result(status, "delete HKCU Run value")
}

fn create_run_key() -> Result<HKEY, String> {
    let mut key: HKEY = std::ptr::null_mut();
    let status = unsafe {
        RegCreateKeyW(
            HKEY_CURRENT_USER,
            wide_null(RUN_KEY_SUBPATH).as_ptr(),
            &mut key,
        )
    };
    status_to_result(status, "create HKCU Run key")?;
    Ok(key)
}

fn open_run_key_with_access(access: u32) -> Result<Option<HKEY>, String> {
    let mut key: HKEY = std::ptr::null_mut();
    let status = unsafe {
        RegOpenKeyExW(
            HKEY_CURRENT_USER,
            wide_null(RUN_KEY_SUBPATH).as_ptr(),
            0,
            access,
            &mut key,
        )
    };
    if status == ERROR_FILE_NOT_FOUND {
        return Ok(None);
    }
    status_to_result(status, "open HKCU Run key")?;
    Ok(Some(key))
}

fn status_to_result(status: u32, context: &str) -> Result<(), String> {
    if status == ERROR_SUCCESS {
        Ok(())
    } else {
        Err(format!(
            "{context}: {}",
            std::io::Error::from_raw_os_error(status as i32)
        ))
    }
}

fn wide_null(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

struct RegKeyGuard(HKEY);

impl Drop for RegKeyGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                RegCloseKey(self.0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{argument_tail, runs_one_of};

    #[test]
    fn an_instance_matches_its_own_config_and_never_another() {
        let sets = vec![
            vec![
                "supervise".into(),
                "--config".into(),
                r"C:\Users\cairn\.cairn-exec\executor.json".into(),
            ],
            vec![
                "run".into(),
                "--config".into(),
                r"C:\Users\cairn\.cairn-exec\executor.json".into(),
            ],
        ];
        assert!(runs_one_of(
            r#""C:\Users\cairn\.local\bin\cairn-exec.exe" supervise --config "C:\Users\cairn\.cairn-exec\executor.json""#,
            &sets
        ));
        // The supervisor spawns `run` with the path unquoted when it has no space.
        assert!(runs_one_of(
            r"C:\Users\cairn\.local\bin\cairn-exec.exe run --config c:\users\cairn\.cairn-exec\executor.json",
            &sets
        ));
        assert!(!runs_one_of(
            r#""C:\x\cairn-exec.exe" run --config "C:\Users\cairn\.cairn-exec-dev\executor.json""#,
            &sets
        ));
        assert!(!runs_one_of(r#""C:\x\cairn-exec.exe" run"#, &sets));
        assert!(!runs_one_of(
            r#""C:\x\cairn-exec.exe" service install --config "C:\Users\cairn\.cairn-exec\executor.json""#,
            &sets
        ));
    }

    #[test]
    fn argument_tail_identifies_the_instance_not_the_program_path() {
        assert_eq!(
            argument_tail("\"C:\\old\\cairn-orch.exe\" run --data-dir \"C:\\home\""),
            "run --data-dir \"C:\\home\""
        );
        // Same instance, stale program path: the tails match, so a refresh is
        // allowed to rewrite it.
        assert_eq!(
            argument_tail("\"C:\\old\\cairn-orch.exe\" run --data-dir \"C:\\home\""),
            argument_tail("\"C:\\new\\cairn-orch.exe\" run --data-dir \"C:\\home\"")
        );
        // A different home is a different instance and must be left alone.
        assert_ne!(
            argument_tail("\"C:\\a.exe\" run --data-dir \"C:\\home\""),
            argument_tail("\"C:\\a.exe\" run --data-dir \"C:\\home-dev\"")
        );
    }
}
