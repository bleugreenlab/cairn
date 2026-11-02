//! The resident executor's Windows registration: one Task Scheduler task,
//! `CairnExecutor`, that keeps `cairn-exec supervise` running for the executor's
//! own account whether or not anyone is logged on.
//!
//! The division of labour is fixed by what Windows lets a non-administrator do
//! (measured on bgsrv-win, cairn/5836):
//!
//! - **An administrator registers the task, once per machine.** A task that
//!   runs without a logon needs a principal the executor account cannot
//!   register for itself: S4U is refused to a non-administrator by every API,
//!   and to an elevated administrator reached over ssh as well, and `Password`
//!   needs the account's password, which the account never has. So
//!   `scripts/setup-self-hosted-runner-windows.ps1 -ExecutorUser <name>`, run
//!   elevated, sets a fresh random password for the account and registers the
//!   task with it, so the password lives only in Task Scheduler's store. It
//!   registers from [`EXECUTOR_TASK_TEMPLATE`], the one committed artifact that
//!   this module and the script both read. Either way the account needs 'Log on
//!   as a batch job'.
//! - **The executor account operates it, and never removes it.** The template's
//!   security descriptor lets that account read, run and end the task, so
//!   `cairn-exec service install|uninstall` and the in-daemon self-uninstall run
//!   as the account, over ssh or from the daemon, with no elevation. No
//!   cairn-exec path deletes the task: the account could never register it
//!   again, so a delete in a failed enrolment's rollback would send the operator
//!   to an administrator before every retry. The registration is machine setup
//!   that the setup script creates and removes.
//!
//! Whether the executor runs is therefore decided by what the minute tick
//! finds, not by whether the task is registered. The supervisor starts the
//! daemon only while its configuration exists and an
//! [`EXECUTOR_INSTALLED_MARKER`] stands beside it, and otherwise exits 0 at
//! once. `service install` writes the marker and then starts the task;
//! `service uninstall` removes it before it ends the task; the self-uninstall
//! removes the home both live in. So a registration made by the setup script
//! starts nothing until the ceremony installs the executor (the tick cannot
//! race the ceremony's own checks), and once the binary is gone the tick
//! records a failed start and runs nothing. From cairn-exec's side the
//! executor's service is *installed* exactly while the task exists and the
//! marker stands, which is what a rollback proves false after an uninstall. Do
//! not add a delete back to make a stop stick: the marker and the binary's
//! absence are what make it stick.
//!
//! What the task expresses, and why:
//!
//! - A `TimeTrigger` repeating every minute, indefinitely, with
//!   `MultipleInstancesPolicy=IgnoreNew`. The tick is what brings the supervisor
//!   back at boot without a logon and after the supervisor itself died. There is
//!   deliberately no `BootTrigger` (unreliable for this principal) and no
//!   `RestartOnFailure`, which restarts a task only when it fails to *start*,
//!   never a daemon that ran and then exited.
//! - `Priority` 4. Task Scheduler's default of 7 is below-normal CPU, low I/O
//!   and low memory priority, and every build the executor runs would inherit it
//!   underneath the executor's own policy for lowering its children. The Windows
//!   twin of `ProcessType=Interactive` on the launchd plist.
//! - The action is `cairn-exec supervise --config <path>`: the supervisor
//!   restarts the daemon with the same policy launchd and systemd express, which
//!   the task itself cannot.
//!
//! Everything here except the real [`TaskHost`] is pure and compiled on every
//! platform, so the sequencing is tested on the lanes that have no Windows.

use std::path::Path;
use std::time::{Duration, Instant};

use serde::Deserialize;

use super::{xml_escape, ServiceRuntime, TerminatedProcess};

/// The task, as the setup script registers it. `@NAME@` placeholders are filled
/// by [`executor_task_xml`] here and by the script's own substitution there.
///
/// The security descriptor, `D:P(A;;FA;;;BA)(A;;FA;;;SY)(A;;FRFXSD;;;<SID>)`,
/// grants the executor account read, execute and delete while Administrators
/// and SYSTEM keep full control. Measured on bgsrv-win (cairn/5836), that
/// lets the account query and export the task's definition, start it, end it
/// (which ends the action and everything it started), and delete it; it
/// cannot change or disable it. cairn-exec never uses the delete.
/// Administrators, through the setup script, are the ones who remove it.
pub const EXECUTOR_TASK_TEMPLATE: &str = include_str!("executor-task.xml");

/// The placeholders [`EXECUTOR_TASK_TEMPLATE`] carries, in the order
/// [`executor_task_xml`] fills them.
pub const EXECUTOR_TASK_PLACEHOLDERS: [&str; 4] =
    ["USER_SID", "EXECUTABLE", "CONFIG", "WORKING_DIRECTORY"];

/// `Get-ScheduledTask`'s `State`.
pub const TASK_STATE_DISABLED: u32 = 1;
pub const TASK_STATE_READY: u32 = 3;
pub const TASK_STATE_RUNNING: u32 = 4;

/// `LastTaskResult` values with a meaning of their own.
pub const SCHED_S_TASK_RUNNING: u32 = 0x0004_1301;
pub const SCHED_S_TASK_HAS_NOT_RUN: u32 = 0x0004_1303;
pub const SCHED_S_TASK_TERMINATED: u32 = 0x0004_1306;
/// A tick that `IgnoreNew` turned away because an instance was already
/// running: "the operator or administrator has refused the request".
pub const TICK_IGNORED_WHILE_RUNNING: u32 = 0x8007_10E0;
/// The password Task Scheduler stores for the principal no longer logs on.
pub const ERROR_LOGON_FAILURE: u32 = 0x8007_052E;
/// The action's program is not there: the executor was uninstalled, or not
/// yet installed, and the registration is waiting for it.
pub const ACTION_FILE_NOT_FOUND: u32 = 0x8007_0002;
pub const ACTION_PATH_NOT_FOUND: u32 = 0x8007_0003;

/// The file, beside the executor's configuration, that `service install`
/// writes and `service uninstall` removes: the executor is installed, so its
/// supervisor may start it.
pub const EXECUTOR_INSTALLED_MARKER: &str = "exec.installed";

/// Whether `service install` has installed the executor configured at
/// `config` since the last uninstall.
pub fn executor_installed(config: &Path) -> bool {
    config
        .parent()
        .is_some_and(|home| home.join(EXECUTOR_INSTALLED_MARKER).is_file())
}

/// Whether the executor configured at `config` may be started: it is
/// installed and its configuration exists. The supervisor's rule, read before
/// every start.
pub fn executor_may_start(config: &Path) -> bool {
    config.is_file() && executor_installed(config)
}

/// The principal lacks the logon right its logon type needs: for S4U, 'Log on
/// as a batch job'.
pub const ERROR_LOGON_TYPE_NOT_GRANTED: u32 = 0x8007_0569;
pub const E_ACCESSDENIED: u32 = 0x8007_0005;

/// The task XML for the executor at `executable` running against `config`,
/// for the account `user_sid`.
///
/// The working directory is the binary's own directory, never the executor
/// home: Windows refuses to delete a directory any process holds as its
/// current directory, so a supervisor or daemon sitting in the home would
/// keep the self-uninstall from removing it. The setup script derives it the
/// same way, from the same string.
pub fn executor_task_xml(executable: &Path, config: &Path, user_sid: &str) -> String {
    let executable = executable.display().to_string();
    fill_placeholders(
        EXECUTOR_TASK_TEMPLATE,
        &[
            ("USER_SID", xml_escape(user_sid)),
            ("EXECUTABLE", xml_escape(&executable)),
            ("CONFIG", xml_escape(&config.display().to_string())),
            ("WORKING_DIRECTORY", xml_escape(directory_of(&executable))),
        ],
    )
}

/// Everything before the last path separator, either kind, so the answer is
/// the same for a Windows path whatever host computes it.
fn directory_of(path: &str) -> &str {
    path.rfind(['\\', '/']).map_or("", |at| &path[..at])
}

/// One pass over `template`, so a value that itself contains `@NAME@` is never
/// substituted again.
fn fill_placeholders(template: &str, values: &[(&str, String)]) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(at) = rest.find('@') {
        out.push_str(&rest[..at]);
        let after = &rest[at + 1..];
        match values
            .iter()
            .find(|(name, _)| after.starts_with(name) && after[name.len()..].starts_with('@'))
        {
            Some((name, value)) => {
                out.push_str(value);
                rest = &after[name.len() + 1..];
            }
            None => {
                out.push('@');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// The scheduler's account of the task, as the real host reads it through
/// `Get-ScheduledTask`, `Get-ScheduledTaskInfo` and `Export-ScheduledTask`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScheduledTaskState {
    pub state: u32,
    pub enabled: bool,
    pub last_task_result: u32,
    /// Changes with every run, which is how a start is told from the result of
    /// the run before it.
    pub last_run_time: Option<String>,
    /// The principal's SID, resolved from whatever form the scheduler reports
    /// it in.
    pub user_sid: Option<String>,
    /// The registered definition. `None` when it could not be exported.
    pub xml: Option<String>,
}

impl ScheduledTaskState {
    pub fn running(&self) -> bool {
        self.state == TASK_STATE_RUNNING
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTaskState {
    state: u32,
    enabled: bool,
    last_task_result: i64,
    #[serde(default)]
    last_run_time: Option<String>,
    #[serde(default)]
    user_sid: Option<String>,
    #[serde(default)]
    xml: Option<String>,
}

/// Read the real host's query output: one compressed JSON object, or `null`
/// when no such task exists. `LastTaskResult` is an unsigned HRESULT that
/// PowerShell may render either way.
pub fn parse_scheduled_task_query(json: &str) -> Result<Option<ScheduledTaskState>, String> {
    let raw: Option<RawTaskState> = serde_json::from_str(json.trim())
        .map_err(|error| format!("parse the scheduled task's state: {error}"))?;
    Ok(raw.map(|raw| ScheduledTaskState {
        state: raw.state,
        enabled: raw.enabled,
        last_task_result: raw.last_task_result as u32,
        last_run_time: raw.last_run_time.filter(|time| !time.trim().is_empty()),
        user_sid: raw.user_sid.filter(|sid| !sid.trim().is_empty()),
        xml: raw.xml.filter(|xml| !xml.trim().is_empty()),
    }))
}

/// The scheduler's account as a [`ServiceRuntime`]. The task's process is the
/// supervisor, not the daemon, so the pid and restart count are the
/// supervisor's to report; this is what the scheduler alone can say.
pub fn runtime_of(task: &ScheduledTaskState) -> ServiceRuntime {
    // Not failures: routine outcomes, and the registration waiting for an
    // executor that is not installed, which is a stop rather than a fault.
    let benign = [
        0,
        SCHED_S_TASK_RUNNING,
        SCHED_S_TASK_HAS_NOT_RUN,
        SCHED_S_TASK_TERMINATED,
        TICK_IGNORED_WHILE_RUNNING,
        ACTION_FILE_NOT_FOUND,
        ACTION_PATH_NOT_FOUND,
    ];
    ServiceRuntime {
        running: task.running(),
        pid: None,
        last_exit_status: (!task.running() && !benign.contains(&task.last_task_result))
            .then_some(task.last_task_result as i32),
        restarts: None,
    }
}

/// A field of the registered task that differs from what this executor needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskMismatch {
    pub field: &'static str,
    pub expected: String,
    pub found: String,
}

impl std::fmt::Display for TaskMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let shown = |value: &str| {
            if value.is_empty() {
                "(absent)".to_string()
            } else {
                format!("`{value}`")
            }
        };
        write!(
            f,
            "{} is {}, expected {}",
            self.field,
            shown(&self.found),
            shown(&self.expected)
        )
    }
}

/// How one field is read from a task definition.
enum Read {
    /// An element's text, or the schema default when the element is absent.
    Text {
        path: &'static str,
        default: &'static str,
        /// A path compared as Windows compares paths.
        fold_case: bool,
    },
    /// The names of an element's children, in order.
    Children(&'static str),
    /// Whether the element is present at all.
    Present(&'static str),
}

/// The fields that carry the task's shape. Not compared: the description, the
/// security descriptor (the scheduler does not export it; a wrong one shows as
/// an access refusal when the account operates the task), the trigger's start
/// boundary, and the task's own enabled flag, which is state rather than shape.
const TASK_FIELDS: &[(&str, Read)] = &[
    ("actions", Read::Children("Actions")),
    (
        "the action's program",
        Read::Text {
            path: "Actions/Exec/Command",
            default: "",
            fold_case: true,
        },
    ),
    (
        "the action's arguments",
        Read::Text {
            path: "Actions/Exec/Arguments",
            default: "",
            fold_case: true,
        },
    ),
    (
        "the action's working directory",
        Read::Text {
            path: "Actions/Exec/WorkingDirectory",
            default: "",
            fold_case: true,
        },
    ),
    (
        "the principal's logon type",
        Read::Text {
            path: "Principals/Principal/LogonType",
            default: "",
            fold_case: false,
        },
    ),
    (
        "the principal's run level",
        Read::Text {
            path: "Principals/Principal/RunLevel",
            default: "LeastPrivilege",
            fold_case: false,
        },
    ),
    ("triggers", Read::Children("Triggers")),
    (
        "the trigger's repetition interval",
        Read::Text {
            path: "Triggers/TimeTrigger/Repetition/Interval",
            default: "",
            fold_case: false,
        },
    ),
    (
        "the trigger's repetition duration",
        Read::Text {
            path: "Triggers/TimeTrigger/Repetition/Duration",
            default: "",
            fold_case: false,
        },
    ),
    (
        "the trigger's enabled flag",
        Read::Text {
            path: "Triggers/TimeTrigger/Enabled",
            default: "true",
            fold_case: false,
        },
    ),
    (
        "the multiple-instances policy",
        Read::Text {
            path: "Settings/MultipleInstancesPolicy",
            default: "IgnoreNew",
            fold_case: false,
        },
    ),
    (
        "restart on failure",
        Read::Present("Settings/RestartOnFailure"),
    ),
    (
        "the priority",
        Read::Text {
            path: "Settings/Priority",
            default: "7",
            fold_case: false,
        },
    ),
    (
        "the execution time limit",
        Read::Text {
            path: "Settings/ExecutionTimeLimit",
            default: "PT72H",
            fold_case: false,
        },
    ),
    (
        "start when available",
        Read::Text {
            path: "Settings/StartWhenAvailable",
            default: "false",
            fold_case: false,
        },
    ),
    (
        "disallow start on batteries",
        Read::Text {
            path: "Settings/DisallowStartIfOnBatteries",
            default: "true",
            fold_case: false,
        },
    ),
    (
        "stop when going on batteries",
        Read::Text {
            path: "Settings/StopIfGoingOnBatteries",
            default: "true",
            fold_case: false,
        },
    ),
    (
        "allow hard terminate",
        Read::Text {
            path: "Settings/AllowHardTerminate",
            default: "true",
            fold_case: false,
        },
    ),
    (
        "allow start on demand",
        Read::Text {
            path: "Settings/AllowStartOnDemand",
            default: "true",
            fold_case: false,
        },
    ),
    (
        "run only if network available",
        Read::Text {
            path: "Settings/RunOnlyIfNetworkAvailable",
            default: "false",
            fold_case: false,
        },
    ),
    (
        "stop on idle end",
        Read::Text {
            path: "Settings/IdleSettings/StopOnIdleEnd",
            default: "true",
            fold_case: false,
        },
    ),
];

/// Every element of a task definition as (path below the root, text).
struct TaskDocument(Vec<(String, String)>);

impl TaskDocument {
    fn parse(xml: &str) -> Result<Self, String> {
        use quick_xml::events::Event;
        let mut reader = quick_xml::Reader::from_str(xml);
        let mut stack: Vec<String> = Vec::new();
        let mut elements: Vec<(String, String)> = Vec::new();
        // Index into `elements` of each open element.
        let mut open: Vec<usize> = Vec::new();
        let error = |error: &dyn std::fmt::Display| format!("parse the task definition: {error}");
        loop {
            match reader.read_event().map_err(|e| error(&e))? {
                Event::Start(start) => {
                    stack.push(String::from_utf8_lossy(start.local_name().as_ref()).into_owned());
                    elements.push((stack[1..].join("/"), String::new()));
                    open.push(elements.len() - 1);
                }
                Event::Empty(empty) => {
                    stack.push(String::from_utf8_lossy(empty.local_name().as_ref()).into_owned());
                    elements.push((stack[1..].join("/"), String::new()));
                    stack.pop();
                }
                Event::End(_) => {
                    stack.pop();
                    open.pop();
                }
                Event::Text(text) => {
                    if let Some(&index) = open.last() {
                        elements[index]
                            .1
                            .push_str(&text.decode().map_err(|e| error(&e))?);
                    }
                }
                Event::CData(data) => {
                    if let Some(&index) = open.last() {
                        elements[index]
                            .1
                            .push_str(&data.decode().map_err(|e| error(&e))?);
                    }
                }
                Event::GeneralRef(reference) => {
                    let resolved = match reference.resolve_char_ref().map_err(|e| error(&e))? {
                        Some(ch) => ch.to_string(),
                        None => {
                            let name = reference.decode().map_err(|e| error(&e))?;
                            quick_xml::escape::resolve_predefined_entity(&name)
                                .ok_or_else(|| error(&format!("unknown entity &{name};")))?
                                .to_string()
                        }
                    };
                    if let Some(&index) = open.last() {
                        elements[index].1.push_str(&resolved);
                    }
                }
                Event::Eof => break,
                _ => {}
            }
        }
        if elements.is_empty() {
            return Err("parse the task definition: it has no root element".into());
        }
        Ok(Self(elements))
    }

    fn text(&self, path: &str) -> Option<String> {
        self.0
            .iter()
            .find(|(element, _)| element == path)
            .map(|(_, text)| text.trim().to_string())
    }

    fn children(&self, path: &str) -> String {
        let prefix = format!("{path}/");
        self.0
            .iter()
            .filter_map(|(element, _)| element.strip_prefix(&prefix))
            .filter(|rest| !rest.contains('/'))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn read(&self, read: &Read) -> String {
        match read {
            Read::Text { path, default, .. } => {
                self.text(path).unwrap_or_else(|| default.to_string())
            }
            Read::Children(path) => self.children(path),
            Read::Present(path) => {
                if self.text(path).is_some() {
                    "present".into()
                } else {
                    String::new()
                }
            }
        }
    }
}

/// Compare the task registered on this machine with the one this executor
/// needs, field by field. `registered_user_sid` is the principal as the host
/// resolved it, since the scheduler may report the account by name.
pub fn verify_executor_task(
    expected_xml: &str,
    registered_xml: &str,
    registered_user_sid: Option<&str>,
) -> Result<Vec<TaskMismatch>, String> {
    let expected = TaskDocument::parse(expected_xml)?;
    let registered = TaskDocument::parse(registered_xml)?;
    let mut mismatches = Vec::new();

    let wanted_sid = expected
        .text("Principals/Principal/UserId")
        .unwrap_or_default();
    let found_principal = registered_user_sid
        .map(str::to_string)
        .or_else(|| registered.text("Principals/Principal/UserId"))
        .unwrap_or_default();
    if !wanted_sid.eq_ignore_ascii_case(&found_principal) {
        mismatches.push(TaskMismatch {
            field: "the principal",
            expected: wanted_sid,
            found: found_principal,
        });
    }

    for (field, read) in TASK_FIELDS {
        let wanted = expected.read(read);
        let found = registered.read(read);
        let same = match read {
            Read::Text {
                fold_case: true, ..
            } => wanted.to_lowercase() == found.to_lowercase(),
            _ => wanted == found,
        };
        if !same {
            mismatches.push(TaskMismatch {
                field,
                expected: wanted,
                found,
            });
        }
    }
    Ok(mismatches)
}

/// The one command that registers the task correctly, with what to do after.
/// `overrides` are the script's path arguments for an enrollment that does
/// not use the default paths ([`setup_overrides`]).
pub fn setup_remedy(account: &str, overrides: &[String]) -> String {
    let mut command = format!(
        "powershell -ExecutionPolicy Bypass -File .\\scripts\\setup-self-hosted-runner-windows.ps1 -ExecutorUser {account}"
    );
    for argument in overrides {
        command.push(' ');
        command.push_str(argument);
    }
    format!(
        "from an elevated PowerShell on this machine, in a Cairn checkout, run `{command}`, then enroll \
         the machine again"
    )
}

/// The setup script's path arguments this executor needs: none when it lives
/// where the runner installs by default under the account's profile, and
/// `-ExecutorBinary`/`-ExecutorConfig` for whichever it overrides, so the
/// remedy registers the task this executor verifies against. With no profile
/// to compare against, both are named.
pub fn setup_overrides(task: &ExecutorTask<'_>, profile: Option<&str>) -> Vec<String> {
    let differs = |actual: &Path, default: Option<String>| {
        default.is_none_or(|default| !actual.display().to_string().eq_ignore_ascii_case(&default))
    };
    let profile = profile.map(|profile| profile.trim_end_matches(['\\', '/']).to_string());
    let mut overrides = Vec::new();
    if differs(
        task.executable,
        profile
            .as_ref()
            .map(|profile| format!("{profile}\\.local\\bin\\cairn-exec.exe")),
    ) {
        overrides.push(format!("-ExecutorBinary \"{}\"", task.executable.display()));
    }
    if differs(
        task.config,
        profile
            .as_ref()
            .map(|profile| format!("{profile}\\.cairn-exec\\executor.json")),
    ) {
        overrides.push(format!("-ExecutorConfig \"{}\"", task.config.display()));
    }
    overrides
}

/// A Task Scheduler operation the host refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRefusal {
    pub hresult: Option<u32>,
    pub message: String,
}

impl TaskRefusal {
    fn describe(&self, name: &str, operation: &str, account: &str, remedy: &str) -> String {
        if self.hresult == Some(E_ACCESSDENIED) {
            format!(
                "{account} may not {operation} the scheduled task {name}: a task by that name exists that \
                 this account cannot operate, registered by hand or by an older setup script. Remove it as \
                 an administrator (`schtasks /delete /tn {name} /f`), then {remedy}"
            )
        } else {
            format!("{operation} the scheduled task {name}: {}", self.message)
        }
    }
}

/// Where the executor's task and the processes it runs are kept. The real
/// host is Windows's Task Scheduler and process table; tests fake it.
pub trait TaskHost {
    /// This account, as the remedies name it.
    fn account(&self) -> String;
    /// This account's profile directory, which the runner's default paths
    /// live under.
    fn profile(&self) -> Option<String>;
    /// This account's SID.
    fn user_sid(&self) -> Result<String, String>;
    fn query(&self, name: &str) -> Result<Option<ScheduledTaskState>, String>;
    fn run(&self, name: &str) -> Result<(), TaskRefusal>;
    fn end(&self, name: &str) -> Result<(), TaskRefusal>;
    /// The command line of the Run value the executor registered before it
    /// was a scheduled task, if this account still has one.
    fn retired_run_value(&self) -> Result<Option<String>, String>;
    fn delete_retired_run_value(&self) -> Result<(), String>;
    /// End the processes that Run value started.
    fn terminate_run_value_processes(
        &self,
        command_line: &str,
    ) -> Result<Vec<TerminatedProcess>, String>;
    /// End every process running `program` with exactly one of `argument_sets`,
    /// and what each started.
    fn terminate_instances(
        &self,
        program: &Path,
        argument_sets: &[Vec<String>],
    ) -> Result<Vec<TerminatedProcess>, String>;
    /// The pids running `program` with one of `argument_sets`.
    fn find_instances(
        &self,
        program: &Path,
        argument_sets: &[Vec<String>],
    ) -> Result<Vec<u32>, String>;
    fn sleep(&self, duration: Duration);
}

/// The executor's task, as one machine's registration names it.
#[derive(Debug, Clone)]
pub struct ExecutorTask<'a> {
    pub name: &'a str,
    pub executable: &'a Path,
    pub config: &'a Path,
}

impl ExecutorTask<'_> {
    /// The command lines of this instance, and only this one: the config path
    /// is part of every match, so a second executor home on the machine, or an
    /// operator's copy run against another config, is never taken.
    fn instance_arguments(&self) -> Vec<Vec<String>> {
        let config = self.config.display().to_string();
        ["supervise", "run"]
            .iter()
            .map(|subcommand| vec![subcommand.to_string(), "--config".into(), config.clone()])
            .collect()
    }
}

impl ExecutorTask<'_> {
    fn installed_marker(&self) -> Option<std::path::PathBuf> {
        self.config
            .parent()
            .map(|home| home.join(EXECUTOR_INSTALLED_MARKER))
    }
}

/// How long `install` waits for the task to report its instance running.
#[derive(Debug, Clone, Copy)]
pub struct StartWait {
    pub poll: Duration,
    pub timeout: Duration,
}

impl Default for StartWait {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(500),
            timeout: Duration::from_secs(15),
        }
    }
}

/// Remove the Run value an older build registered, and end what it started,
/// so two generations never run against one home.
fn evict_retired_run_value(host: &dyn TaskHost) -> Result<Vec<TerminatedProcess>, String> {
    let Some(command_line) = host.retired_run_value()? else {
        return Ok(Vec::new());
    };
    host.delete_retired_run_value()?;
    host.terminate_run_value_processes(&command_line)
}

/// Confirm the registered task is the one this executor needs, then start its
/// instance through the scheduler and wait for it to be running. Never spawns
/// the daemon itself: a process started from an ssh session dies with it.
///
/// Returns the processes it ended along the way (a Run-value generation).
pub fn install(
    task: &ExecutorTask<'_>,
    host: &dyn TaskHost,
    wait: StartWait,
) -> Result<Vec<TerminatedProcess>, String> {
    let name = task.name;
    let account = host.account();
    let remedy = setup_remedy(&account, &setup_overrides(task, host.profile().as_deref()));
    let ended = evict_retired_run_value(host)?;

    let expected = executor_task_xml(task.executable, task.config, &host.user_sid()?);
    let Some(registered) = host.query(name)? else {
        return Err(format!(
            "no scheduled task named {name} is registered on this machine. Registering it needs an \
             administrator once per machine: {remedy}"
        ));
    };
    let definition = registered.xml.as_deref().ok_or_else(|| {
        format!(
            "the scheduled task {name} exists but its definition could not be read by {account}"
        )
    })?;
    let mismatches = verify_executor_task(&expected, definition, registered.user_sid.as_deref())?;
    if !mismatches.is_empty() {
        let listed = mismatches
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        return Err(format!(
            "the scheduled task {name} is not the one this executor runs: {listed}. To register it \
             again, {remedy}"
        ));
    }
    if !registered.enabled {
        return Err(format!(
            "the scheduled task {name} is disabled. To register it again, {remedy}"
        ));
    }
    // Written before the start, so the supervisor this start launches runs.
    let marker = task
        .installed_marker()
        .ok_or_else(|| format!("the config {} names no home", task.config.display()))?;
    std::fs::write(&marker, b"installed by cairn-exec service install\n").map_err(|error| {
        format!(
            "mark the executor installed at {}: {error}",
            marker.display()
        )
    })?;

    // An instance already running is a previous generation: ending the task
    // ends the supervisor, whose job takes the daemon and every cell command.
    if registered.running() {
        host.end(name)
            .map_err(|refusal| refusal.describe(name, "end", &account, &remedy))?;
        let deadline = Instant::now() + wait.timeout;
        while host.query(name)?.is_some_and(|task| task.running()) {
            if Instant::now() >= deadline {
                return Err(format!(
                    "the scheduled task {name} was ended but still reports its previous instance running"
                ));
            }
            host.sleep(wait.poll);
        }
    }

    let before = host.query(name)?.and_then(|task| task.last_run_time);
    host.run(name)
        .map_err(|refusal| refusal.describe(name, "start", &account, &remedy))?;
    let deadline = Instant::now() + wait.timeout;
    loop {
        let Some(now) = host.query(name)? else {
            return Err(format!(
                "the scheduled task {name} was removed while it was starting"
            ));
        };
        if now.running() {
            return Ok(ended);
        }
        if now.last_task_result == ERROR_LOGON_TYPE_NOT_GRANTED {
            return Err(format!(
                "the scheduled task {name} could not start: {account} lacks 'Log on as a batch job' \
                 (0x{ERROR_LOGON_TYPE_NOT_GRANTED:08X}). Granting it needs an administrator: {remedy}"
            ));
        }
        if now.last_task_result == ERROR_LOGON_FAILURE {
            return Err(format!(
                "the scheduled task {name} could not start: the password Task Scheduler stores for \
                 {account} no longer logs on (0x{ERROR_LOGON_FAILURE:08X}), so it was changed since the \
                 task was registered. Registering it again sets a new one: {remedy}"
            ));
        }
        let ran = now.last_run_time.is_some() && now.last_run_time != before;
        if ran
            && ![
                SCHED_S_TASK_RUNNING,
                SCHED_S_TASK_HAS_NOT_RUN,
                TICK_IGNORED_WHILE_RUNNING,
            ]
            .contains(&now.last_task_result)
        {
            return Err(format!(
                "the scheduled task {name} started and its action exited at once with 0x{:08X}; \
                 the supervisor's reason is in {}",
                now.last_task_result,
                task.config.with_file_name("exec.err.log").display()
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "the scheduled task {name} did not report its instance running within {}s (state {}, last result 0x{:08X})",
                wait.timeout.as_secs(),
                now.state,
                now.last_task_result
            ));
        }
        host.sleep(wait.poll);
    }
}

/// Stop the executor on this machine from outside it (the ceremony's
/// `service uninstall`): withdraw its installed marker, so the minute tick's
/// supervisor starts nothing; end the task, whose job takes the supervisor,
/// the daemon and every cell command; remove the retired Run value; end any
/// stray instance of this executor; and confirm none remains. The task stays
/// registered (see the module docs).
pub fn uninstall(
    task: &ExecutorTask<'_>,
    host: &dyn TaskHost,
) -> Result<Vec<TerminatedProcess>, String> {
    let name = task.name;
    let account = host.account();
    let remedy = setup_remedy(&account, &setup_overrides(task, host.profile().as_deref()));
    let mut errors = Vec::new();
    if let Some(marker) = task.installed_marker() {
        match std::fs::remove_file(&marker) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => errors.push(format!(
                "withdraw the installed marker {}: {error}",
                marker.display()
            )),
        }
    }
    if host.query(name)?.is_some_and(|state| state.running()) {
        if let Err(refusal) = host.end(name) {
            errors.push(refusal.describe(name, "end", &account, &remedy));
        }
    }
    let mut ended = match evict_retired_run_value(host) {
        Ok(ended) => ended,
        Err(error) => {
            errors.push(error);
            Vec::new()
        }
    };
    let instances = task.instance_arguments();
    match host.terminate_instances(task.executable, &instances) {
        Ok(strays) => ended.extend(strays),
        Err(error) => errors.push(error),
    }
    match host.find_instances(task.executable, &instances) {
        Ok(left) if left.is_empty() => {}
        Ok(left) => errors.push(format!(
            "executor processes are still running after the uninstall: {}",
            left.iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )),
        Err(error) => errors.push(error),
    }
    if errors.is_empty() {
        Ok(ended)
    } else {
        Err(errors.join("; "))
    }
}

/// The registration's part of the self-uninstall a revoked credential directs,
/// run inside the daemon the task supervises once the home and binary are
/// gone. The task is neither ended nor deleted: ending it would end the caller
/// through the supervisor's job before the daemon finished, and the account
/// could never register it again. With the configuration gone the supervisor
/// starts nothing (the daemon exits 0 after this, and the supervisor with it),
/// and with the binary gone the tick runs nothing. What is left to remove is
/// a Run value from before the task.
pub fn unregister_from_within(_task: &ExecutorTask<'_>, host: &dyn TaskHost) -> Result<(), String> {
    if host.retired_run_value()?.is_some() {
        host.delete_retired_run_value()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
