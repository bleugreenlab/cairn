//! HKCU Run-key autostart lifecycle (Windows). The Run key is the per-user,
//! zero-elevation autostart store that matches Cairn's daemon model: the current
//! interactive user can write it at medium integrity, Explorer launches it at
//! logon, and no Task Scheduler task-store ACL or principal serialization is
//! involved.
//!
//! What the Run key cannot do is supervise. It expresses no restart policy, so a
//! daemon that dies stays dead until the next logon — stated here rather than
//! papered over, because the launchd and systemd backends do restart and code
//! reading only one of them would assume parity.

use std::os::windows::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use windows_sys::Win32::Foundation::{ERROR_FILE_NOT_FOUND, ERROR_SUCCESS};
use windows_sys::Win32::System::Registry::{
    RegCloseKey, RegCreateKeyW, RegDeleteValueW, RegOpenKeyExW, RegQueryValueExW, RegSetValueExW,
    HKEY, HKEY_CURRENT_USER, KEY_READ, KEY_SET_VALUE, REG_SZ,
};

use super::{parse_run_value, windows_run_command, LegacyIdentity, RegisteredUnit, ServiceSpec};

const RUN_KEY_SUBPATH: &str = r"Software\Microsoft\Windows\CurrentVersion\Run";

/// `CREATE_NO_WINDOW`: the desktop and release daemons are GUI-subsystem
/// processes, so spawning console helpers should not allocate a visible console
/// window.
const CREATE_NO_WINDOW: u32 = 0x08000000;
/// `DETACHED_PROCESS`: the daemon spawned by `install`/`restart` must outlive the
/// short-lived service command process.
const DETACHED_PROCESS: u32 = 0x00000008;

pub fn install(spec: &ServiceSpec) -> Result<(), String> {
    cleanup_legacy(spec);
    write_run_value(&spec.run_value, &windows_run_command(spec))?;
    spawn_daemon(spec);
    Ok(())
}

pub fn uninstall(spec: &ServiceSpec) -> Result<(), String> {
    delete_run_value_named(&spec.run_value)?;
    if let Some(legacy) = spec.legacy.as_ref().and_then(|l| l.run_value.as_deref()) {
        let _ = delete_run_value_named(legacy);
    }
    Ok(())
}

/// Ensure the Run value points at this binary and spawn the current daemon. This
/// is the Windows adoption path after an app update: the new binary is launched
/// directly, since the Run key alone would not take effect until the next logon.
pub fn restart(spec: &ServiceSpec) -> Result<(), String> {
    cleanup_legacy(spec);
    let desired = windows_run_command(spec);
    if read_run_value(&spec.run_value).as_deref() != Some(desired.as_str()) {
        write_run_value(&spec.run_value, &desired)?;
    }
    spawn_daemon(spec);
    Ok(())
}

/// Refresh only a value that already describes THIS instance. HKCU is per user
/// while a machine can host several Cairn instances (`~/.cairn` vs
/// `~/.cairn-dev`), so a manually-started dev daemon must not rewrite a
/// production autostart value merely because its executable path differs. The
/// guard is therefore the argument tail: same arguments, stale program path.
pub fn refresh_if_stale(spec: &ServiceSpec) {
    let Some(existing) = read_run_value(&spec.run_value) else {
        return;
    };
    let desired = windows_run_command(spec);
    if existing == desired {
        return;
    }
    if argument_tail(&existing) != argument_tail(&desired) {
        return;
    }
    match write_run_value(&spec.run_value, &desired) {
        Ok(()) => tracing::info!(
            "windows autostart value drifted; rewrote HKCU Run value {} (applies at the next logon)",
            spec.run_value
        ),
        Err(e) => tracing::warn!(
            "windows autostart refresh: rewrite {} failed: {e}",
            spec.run_value
        ),
    }
}

pub fn is_installed(spec: &ServiceSpec) -> bool {
    read_run_value(&spec.run_value).is_some()
}

pub fn is_loaded(spec: &ServiceSpec) -> bool {
    is_installed(spec)
}

pub fn location(_spec: &ServiceSpec) -> Option<PathBuf> {
    None
}

/// The Run key starts a process at logon and keeps no account of it.
pub fn runtime(_spec: &ServiceSpec) -> Option<super::ServiceRuntime> {
    None
}

pub fn recent_log(_spec: &ServiceSpec, _lines: usize) -> Option<String> {
    None
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
///
/// A pid read from a process listing names nothing by the time it is acted
/// on, so each one is opened first and every check runs through the held
/// handle, which keeps the pid from being reused until it is closed: the
/// process must have existed when the listing was taken (its creation time is
/// no later than that), a root must still run the Run value's executable, and
/// a descendant must have been created no earlier than the parent it was
/// listed under. It is then terminated and waited on through the same handle,
/// and reported with its creation time as its start token.
pub fn terminate_run_value_processes(
    command_line: &str,
) -> Result<Vec<super::TerminatedProcess>, String> {
    let program = parse_run_value(command_line)
        .program
        .ok_or_else(|| "the Run value names no program".to_string())?;
    let listed_at_ms = now_unix_ms();
    let output = Command::new("powershell")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "ConvertTo-Json -Compress -InputObject @(Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId,ExecutablePath,CommandLine)",
        ])
        .stdin(Stdio::null())
        .creation_flags(CREATE_NO_WINDOW)
        .output()
        .map_err(|error| format!("enumerate processes: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "enumerate processes: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let processes: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("parse process list: {error}"))?;
    let field = |process: &serde_json::Value, name: &str| {
        process
            .get(name)
            .and_then(serde_json::Value::as_u64)
            .map(|value| value as u32)
    };
    let wanted_tail = argument_tail(command_line).trim().to_string();
    let program = program.to_string_lossy().to_lowercase();
    let roots: Vec<u32> = processes
        .iter()
        .filter(|process| {
            let executable = process.get("ExecutablePath").and_then(serde_json::Value::as_str);
            let line = process.get("CommandLine").and_then(serde_json::Value::as_str);
            matches!((executable, line), (Some(executable), Some(line))
                if executable.to_lowercase() == program && argument_tail(line).trim() == wanted_tail)
        })
        .filter_map(|process| field(process, "ProcessId"))
        .collect();
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
            .filter(|process| field(process, "ParentProcessId") == Some(pid))
            .filter_map(|process| field(process, "ProcessId"))
        {
            queue.push_back((child, Some(created)));
        }
        open.terminate_and_wait()?;
        ended.push(super::TerminatedProcess {
            pid,
            start_token: created.to_string(),
        });
    }
    Ok(ended)
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
    use super::argument_tail;

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
