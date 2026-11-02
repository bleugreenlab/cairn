use std::cell::RefCell;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::*;

const SID: &str = "S-1-5-21-1475301750-3062682503-2792452579-1004";

fn exe() -> PathBuf {
    PathBuf::from(r"C:\Users\cairn\.local\bin\cairn-exec.exe")
}

fn config() -> PathBuf {
    PathBuf::from(r"C:\Users\cairn\.cairn-exec\executor.json")
}

fn home() -> PathBuf {
    PathBuf::from(r"C:\Users\cairn\.cairn-exec")
}

fn expected() -> String {
    executor_task_xml(&exe(), &config(), SID)
}

/// What `Export-ScheduledTask` returned for the spike tasks on bgsrv-win: the
/// principal as a SID, `RunLevel` omitted because LeastPrivilege is the
/// default, and a `URI` the scheduler adds.
fn exported() -> String {
    expected()
        .replace("      <RunLevel>LeastPrivilege</RunLevel>\n", "")
        .replace(
            "  <RegistrationInfo>\n",
            "  <RegistrationInfo>\n    <URI>\\CairnExecutor</URI>\n",
        )
}

fn task<'a>(exe: &'a Path, config: &'a Path) -> ExecutorTask<'a> {
    ExecutorTask {
        name: "CairnExecutor",
        executable: exe,
        config,
    }
}

#[test]
fn the_template_carries_exactly_the_placeholders_the_generator_fills() {
    let mut found: Vec<&str> = EXECUTOR_TASK_TEMPLATE
        .split('@')
        .skip(1)
        .step_by(2)
        .collect();
    found.sort_unstable();
    found.dedup();
    let mut wanted = EXECUTOR_TASK_PLACEHOLDERS.to_vec();
    wanted.sort_unstable();
    assert_eq!(found, wanted);
    assert!(!expected().contains('@'), "a placeholder was left unfilled");
}

/// The shape the plan and the bgsrv-win spike settled, read back through the
/// same parser the verifier uses.
#[test]
fn the_task_runs_the_supervisor_every_minute_as_the_account_at_normal_priority() {
    let document = TaskDocument::parse(&expected()).unwrap();
    let text = |path: &str| document.text(path).unwrap_or_default();
    assert_eq!(document.children("Triggers"), "TimeTrigger");
    assert_eq!(text("Triggers/TimeTrigger/Repetition/Interval"), "PT1M");
    assert_eq!(
        document.text("Triggers/TimeTrigger/Repetition/Duration"),
        None,
        "no Duration: the tick repeats indefinitely"
    );
    assert_eq!(text("Principals/Principal/UserId"), SID);
    assert_eq!(text("Principals/Principal/LogonType"), "Password");
    assert_eq!(text("Principals/Principal/RunLevel"), "LeastPrivilege");
    assert_eq!(text("Settings/MultipleInstancesPolicy"), "IgnoreNew");
    assert_eq!(text("Settings/ExecutionTimeLimit"), "PT0S");
    assert_eq!(text("Settings/Priority"), "4");
    assert_eq!(text("Settings/DisallowStartIfOnBatteries"), "false");
    assert_eq!(text("Settings/StopIfGoingOnBatteries"), "false");
    assert_eq!(text("Settings/StartWhenAvailable"), "true");
    assert_eq!(document.text("Settings/RestartOnFailure"), None);
    assert_eq!(text("Actions/Exec/Command"), exe().display().to_string());
    assert_eq!(
        text("Actions/Exec/Arguments"),
        format!("supervise --config \"{}\"", config().display())
    );
    assert_eq!(
        text("Actions/Exec/WorkingDirectory"),
        r"C:\Users\cairn\.local\bin",
        "the binary's directory: a process sitting in the home would keep the self-uninstall from removing it"
    );
    assert_eq!(
        text("RegistrationInfo/SecurityDescriptor"),
        format!("D:P(A;;FA;;;BA)(A;;FA;;;SY)(A;;FRFXSD;;;{SID})")
    );
}

#[test]
fn paths_are_escaped_into_the_xml_and_read_back_whole() {
    let odd = PathBuf::from(r"C:\Users\R&D <ops>\.cairn-exec\executor.json");
    let xml = executor_task_xml(&exe(), &odd, SID);
    assert!(xml.contains("R&amp;D &lt;ops&gt;"));
    let document = TaskDocument::parse(&xml).unwrap();
    assert_eq!(
        document.text("Actions/Exec/Arguments").unwrap(),
        format!("supervise --config \"{}\"", odd.display())
    );
    // A value that looks like a placeholder is never substituted again.
    let sneaky = PathBuf::from(r"C:\@USER_SID@\cairn-exec.exe");
    assert!(executor_task_xml(&sneaky, &config(), SID).contains(r"C:\@USER_SID@\"));
}

#[test]
fn the_task_as_the_scheduler_exports_it_matches() {
    assert_eq!(
        verify_executor_task(&expected(), &expected(), Some(SID)).unwrap(),
        []
    );
    assert_eq!(
        verify_executor_task(&expected(), &exported(), Some(SID)).unwrap(),
        []
    );
    // The principal reported by name, resolved by the host to the same SID.
    let by_name = exported().replace(&format!("<UserId>{SID}</UserId>"), "<UserId>cairn</UserId>");
    assert_eq!(
        verify_executor_task(&expected(), &by_name, Some(SID)).unwrap(),
        []
    );
    // Paths compare as Windows compares them.
    let shouted = exported().replace(r"C:\Users\cairn", r"c:\USERS\cairn");
    assert_eq!(
        verify_executor_task(&expected(), &shouted, Some(SID)).unwrap(),
        []
    );
}

fn fields(mismatches: &[TaskMismatch]) -> Vec<&'static str> {
    mismatches.iter().map(|mismatch| mismatch.field).collect()
}

/// The hand-made bgsrv-win task, and a task an older script or a person
/// registered: each difference is named with both values.
#[test]
fn every_field_that_carries_the_shape_is_named_when_it_differs() {
    let check =
        |registered: String| verify_executor_task(&expected(), &registered, Some(SID)).unwrap();

    let default_priority = check(exported().replace("    <Priority>4</Priority>\n", ""));
    assert_eq!(fields(&default_priority), ["the priority"]);
    assert_eq!(
        default_priority[0].to_string(),
        "the priority is `7`, expected `4`"
    );

    let run_key_command = check(exported().replace("supervise --config", "run --config"));
    assert_eq!(fields(&run_key_command), ["the action's arguments"]);

    let other_home = check(exported().replace(
        ".cairn-exec\\executor.json",
        ".cairn-exec-dev\\executor.json",
    ));
    assert_eq!(fields(&other_home), ["the action's arguments"]);

    let boot = check(exported().replace(
        "  </Triggers>",
        "    <BootTrigger>\n      <Enabled>true</Enabled>\n    </BootTrigger>\n  </Triggers>",
    ));
    assert_eq!(fields(&boot), ["triggers"]);
    assert_eq!(boot[0].found, "TimeTrigger, BootTrigger");

    let restart = check(exported().replace(
        "    <Priority>4</Priority>\n",
        "    <Priority>4</Priority>\n    <RestartOnFailure>\n      <Interval>PT1M</Interval>\n      <Count>255</Count>\n    </RestartOnFailure>\n",
    ));
    assert_eq!(fields(&restart), ["restart on failure"]);

    let bounded = check(exported().replace(
        "<Interval>PT1M</Interval>",
        "<Interval>PT1M</Interval>\n        <Duration>P1D</Duration>",
    ));
    assert_eq!(fields(&bounded), ["the trigger's repetition duration"]);

    let s4u = check(exported().replace(
        "<LogonType>Password</LogonType>",
        "<LogonType>S4U</LogonType>",
    ));
    assert_eq!(fields(&s4u), ["the principal's logon type"]);

    let parallel = check(exported().replace("IgnoreNew", "Parallel"));
    assert_eq!(fields(&parallel), ["the multiple-instances policy"]);

    let someone_else =
        verify_executor_task(&expected(), &exported(), Some("S-1-5-21-1-2-3-500")).unwrap();
    assert_eq!(fields(&someone_else), ["the principal"]);
}

#[test]
fn the_scheduler_query_reads_state_result_and_definition() {
    assert_eq!(parse_scheduled_task_query("null\r\n").unwrap(), None);
    let running = parse_scheduled_task_query(&format!(
        r#"{{"state":4,"enabled":true,"lastTaskResult":267009,"lastRunTime":"2026-11-02T05:40:01.0000000Z","userSid":"{SID}","xml":"<Task/>"}}"#
    ))
    .unwrap()
    .unwrap();
    assert!(running.running());
    assert_eq!(running.last_task_result, SCHED_S_TASK_RUNNING);
    assert_eq!(running.user_sid.as_deref(), Some(SID));
    // An HRESULT PowerShell rendered signed.
    let refused = parse_scheduled_task_query(
        r#"{"state":3,"enabled":true,"lastTaskResult":-2147023511,"lastRunTime":null,"userSid":null,"xml":""}"#,
    )
    .unwrap()
    .unwrap();
    assert_eq!(refused.last_task_result, ERROR_LOGON_TYPE_NOT_GRANTED);
    assert_eq!(refused.xml, None);
    assert_eq!(refused.last_run_time, None);
    // Unsigned, as Windows PowerShell 5.1 renders a UInt32.
    let unsigned =
        parse_scheduled_task_query(r#"{"state":1,"enabled":false,"lastTaskResult":2147943785}"#)
            .unwrap()
            .unwrap();
    assert_eq!(unsigned.last_task_result, ERROR_LOGON_TYPE_NOT_GRANTED);
    assert_eq!(unsigned.state, TASK_STATE_DISABLED);
}

#[test]
fn the_runtime_names_a_failure_and_nothing_routine() {
    let state = |state, last_task_result| ScheduledTaskState {
        state,
        enabled: true,
        last_task_result,
        ..ScheduledTaskState::default()
    };
    let running = runtime_of(&state(TASK_STATE_RUNNING, TICK_IGNORED_WHILE_RUNNING));
    assert!(running.running);
    assert_eq!(running.last_exit_status, None);
    for routine in [
        0,
        SCHED_S_TASK_HAS_NOT_RUN,
        SCHED_S_TASK_TERMINATED,
        TICK_IGNORED_WHILE_RUNNING,
    ] {
        assert_eq!(
            runtime_of(&state(TASK_STATE_READY, routine)).last_exit_status,
            None
        );
    }
    assert_eq!(
        runtime_of(&state(TASK_STATE_READY, ERROR_LOGON_TYPE_NOT_GRANTED)).last_exit_status,
        Some(ERROR_LOGON_TYPE_NOT_GRANTED as i32)
    );
    assert_eq!(
        runtime_of(&state(TASK_STATE_READY, 75)).last_exit_status,
        Some(75)
    );
}

/// A scripted host that records every call.
struct FakeHost {
    calls: RefCell<Vec<String>>,
    /// Successive answers to `query`; the last repeats.
    states: RefCell<VecDeque<Option<ScheduledTaskState>>>,
    run: Result<(), TaskRefusal>,
    end: Result<(), TaskRefusal>,
    run_value: Option<String>,
    left_running: Vec<u32>,
}

impl FakeHost {
    fn with(states: Vec<Option<ScheduledTaskState>>) -> Self {
        Self {
            calls: RefCell::new(Vec::new()),
            states: RefCell::new(states.into()),
            run: Ok(()),
            end: Ok(()),
            run_value: None,
            left_running: Vec::new(),
        }
    }

    fn call(&self, call: impl Into<String>) {
        self.calls.borrow_mut().push(call.into());
    }

    fn calls(&self) -> Vec<String> {
        self.calls
            .borrow()
            .iter()
            .filter(|call| *call != "query")
            .cloned()
            .collect()
    }
}

impl TaskHost for FakeHost {
    fn account(&self) -> String {
        "cairn".into()
    }
    fn profile(&self) -> Option<String> {
        Some(r"C:\Users\cairn".into())
    }
    fn user_sid(&self) -> Result<String, String> {
        Ok(SID.into())
    }
    fn query(&self, _name: &str) -> Result<Option<ScheduledTaskState>, String> {
        self.call("query");
        let mut states = self.states.borrow_mut();
        Ok(if states.len() > 1 {
            states.pop_front().unwrap()
        } else {
            states.front().cloned().flatten()
        })
    }
    fn run(&self, name: &str) -> Result<(), TaskRefusal> {
        self.call(format!("run {name}"));
        self.run.clone()
    }
    fn end(&self, name: &str) -> Result<(), TaskRefusal> {
        self.call(format!("end {name}"));
        self.end.clone()
    }
    fn retired_run_value(&self) -> Result<Option<String>, String> {
        Ok(self.run_value.clone())
    }
    fn delete_retired_run_value(&self) -> Result<(), String> {
        self.call("delete run value");
        Ok(())
    }
    fn terminate_run_value_processes(
        &self,
        command_line: &str,
    ) -> Result<Vec<TerminatedProcess>, String> {
        self.call(format!("terminate run value {command_line}"));
        Ok(vec![TerminatedProcess {
            pid: 41,
            start_token: "1".into(),
            command_line: Some(command_line.into()),
        }])
    }
    fn terminate_instances(
        &self,
        program: &Path,
        argument_sets: &[Vec<String>],
    ) -> Result<Vec<TerminatedProcess>, String> {
        let sets: Vec<String> = argument_sets.iter().map(|set| set.join(" ")).collect();
        self.call(format!(
            "terminate {} [{}]",
            program.display(),
            sets.join(" | ")
        ));
        Ok(Vec::new())
    }
    fn find_instances(
        &self,
        _program: &Path,
        _argument_sets: &[Vec<String>],
    ) -> Result<Vec<u32>, String> {
        self.call("find");
        Ok(self.left_running.clone())
    }
    fn sleep(&self, _duration: Duration) {}
}

/// A real executor home with its configuration, for the lifecycle: install
/// and uninstall read and write the installed marker beside the config.
struct Home {
    _dir: tempfile::TempDir,
    exe: PathBuf,
    config: PathBuf,
    home: PathBuf,
}

impl Home {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join(".cairn-exec");
        std::fs::create_dir(&home).unwrap();
        let config = home.join("executor.json");
        std::fs::write(&config, "{}").unwrap();
        Self {
            exe: dir.path().join("cairn-exec.exe"),
            _dir: dir,
            config,
            home,
        }
    }

    fn task(&self) -> ExecutorTask<'_> {
        task(&self.exe, &self.config)
    }

    fn marker(&self) -> PathBuf {
        self.home.join(EXECUTOR_INSTALLED_MARKER)
    }

    fn exported(&self) -> String {
        executor_task_xml(&self.exe, &self.config, SID)
            .replace("      <RunLevel>LeastPrivilege</RunLevel>\n", "")
    }

    fn registered(&self, state: u32) -> Option<ScheduledTaskState> {
        Some(ScheduledTaskState {
            state,
            enabled: true,
            last_task_result: SCHED_S_TASK_HAS_NOT_RUN,
            last_run_time: None,
            user_sid: Some(SID.into()),
            xml: Some(self.exported()),
        })
    }

    fn ran(&self, state: u32, last_task_result: u32) -> Option<ScheduledTaskState> {
        self.registered(state).map(|task| ScheduledTaskState {
            last_task_result,
            last_run_time: Some("2026-11-02T05:40:01Z".into()),
            ..task
        })
    }
}

fn quick() -> StartWait {
    StartWait {
        poll: Duration::ZERO,
        timeout: Duration::from_millis(50),
    }
}

const OLD_RUN_VALUE: &str = r#""C:\old\cairn-exec.exe" run --config "C:\h\executor.json""#;

#[test]
fn install_evicts_the_run_value_marks_the_executor_installed_and_starts_the_task() {
    let home = Home::new();
    let mut host = FakeHost::with(vec![
        home.registered(TASK_STATE_READY),
        home.registered(TASK_STATE_READY),
        home.ran(TASK_STATE_READY, SCHED_S_TASK_HAS_NOT_RUN),
        home.ran(TASK_STATE_RUNNING, SCHED_S_TASK_RUNNING),
    ]);
    host.run_value = Some(OLD_RUN_VALUE.into());

    let ended = install(&home.task(), &host, quick()).unwrap();

    assert_eq!(
        host.calls(),
        [
            "delete run value".to_string(),
            format!("terminate run value {OLD_RUN_VALUE}"),
            "run CairnExecutor".to_string(),
        ],
        "the daemon is started through the scheduler and never spawned"
    );
    assert_eq!(
        ended.iter().map(|process| process.pid).collect::<Vec<_>>(),
        [41]
    );
    assert!(executor_may_start(&home.config));
}

#[test]
fn install_replaces_a_running_generation_by_ending_the_task_first() {
    let home = Home::new();
    let host = FakeHost::with(vec![
        home.registered(TASK_STATE_RUNNING),
        home.registered(TASK_STATE_RUNNING),
        home.ran(TASK_STATE_READY, SCHED_S_TASK_TERMINATED),
        home.ran(TASK_STATE_READY, SCHED_S_TASK_TERMINATED),
        home.ran(TASK_STATE_RUNNING, SCHED_S_TASK_RUNNING),
    ]);
    install(&home.task(), &host, quick()).unwrap();
    assert_eq!(host.calls(), ["end CairnExecutor", "run CairnExecutor"]);
}

#[test]
fn install_without_the_task_names_the_administrator_step() {
    let home = Home::new();
    let host = FakeHost::with(vec![None]);
    let error = install(&home.task(), &host, quick()).unwrap_err();
    assert!(
        error.contains("no scheduled task named CairnExecutor"),
        "{error}"
    );
    assert!(
        error.contains("setup-self-hosted-runner-windows.ps1 -ExecutorUser cairn"),
        "{error}"
    );
    assert!(host.calls().is_empty(), "nothing is started or removed");
    assert!(!home.marker().exists());
}

#[test]
fn install_refuses_a_task_that_differs_naming_the_field_and_both_values() {
    let home = Home::new();
    let mut hand_made = home.registered(TASK_STATE_RUNNING).unwrap();
    hand_made.xml = Some(
        home.exported()
            .replace("supervise --config", "run --config")
            .replace("    <Priority>4</Priority>\n", ""),
    );
    let host = FakeHost::with(vec![Some(hand_made)]);
    let error = install(&home.task(), &host, quick()).unwrap_err();
    assert!(
        error.contains("the action's arguments is `run --config"),
        "{error}"
    );
    assert!(error.contains("expected `supervise --config"), "{error}");
    assert!(
        error.contains("the priority is `7`, expected `4`"),
        "{error}"
    );
    assert!(error.contains("-ExecutorUser cairn"), "{error}");
    assert!(
        host.calls().is_empty(),
        "a task that is not ours is never ended"
    );
    assert!(!home.marker().exists(), "and nothing is marked installed");
}

#[test]
fn install_refuses_a_disabled_task() {
    let home = Home::new();
    let mut disabled = home.registered(TASK_STATE_DISABLED).unwrap();
    disabled.enabled = false;
    let host = FakeHost::with(vec![Some(disabled)]);
    let error = install(&home.task(), &host, quick()).unwrap_err();
    assert!(error.contains("is disabled"), "{error}");
}

#[test]
fn install_reports_a_missing_batch_logon_right_with_the_remedy() {
    let home = Home::new();
    let host = FakeHost::with(vec![
        home.registered(TASK_STATE_READY),
        home.registered(TASK_STATE_READY),
        home.ran(TASK_STATE_READY, ERROR_LOGON_TYPE_NOT_GRANTED),
    ]);
    let error = install(&home.task(), &host, quick()).unwrap_err();
    assert!(
        error.contains("lacks 'Log on as a batch job' (0x80070569)"),
        "{error}"
    );
    assert!(error.contains("-ExecutorUser cairn"), "{error}");
}

#[test]
fn install_reports_a_stale_stored_password_and_an_action_that_exits() {
    let home = Home::new();
    let host = FakeHost::with(vec![
        home.registered(TASK_STATE_READY),
        home.registered(TASK_STATE_READY),
        home.ran(TASK_STATE_READY, ERROR_LOGON_FAILURE),
    ]);
    let error = install(&home.task(), &host, quick()).unwrap_err();
    assert!(error.contains("no longer logs on"), "{error}");

    let host = FakeHost::with(vec![
        home.registered(TASK_STATE_READY),
        home.registered(TASK_STATE_READY),
        home.ran(TASK_STATE_READY, 1),
    ]);
    let error = install(&home.task(), &host, quick()).unwrap_err();
    assert!(error.contains("exited at once with 0x00000001"), "{error}");
    assert!(error.contains("exec.err.log"), "{error}");
}

#[test]
fn install_names_a_task_this_account_may_not_operate() {
    let home = Home::new();
    let mut host = FakeHost::with(vec![home.registered(TASK_STATE_READY)]);
    host.run = Err(TaskRefusal {
        hresult: Some(E_ACCESSDENIED),
        message: "Access is denied.".into(),
    });
    let error = install(&home.task(), &host, quick()).unwrap_err();
    assert!(
        error.contains("a task by that name exists that this account cannot operate"),
        "{error}"
    );
    assert!(
        error.contains("schtasks /delete /tn CairnExecutor /f"),
        "{error}"
    );
    assert!(error.contains("-ExecutorUser cairn"), "{error}");
}

/// The race the marker closes: the setup script registers the task before the
/// ceremony runs, so a configuration alone must not start the executor.
#[test]
fn the_supervisor_starts_only_an_executor_service_install_installed() {
    let home = Home::new();
    assert!(
        !executor_may_start(&home.config),
        "configured but not yet installed: the tick starts nothing"
    );
    std::fs::write(home.marker(), "").unwrap();
    assert!(executor_may_start(&home.config));
    std::fs::remove_file(&home.config).unwrap();
    assert!(
        !executor_may_start(&home.config),
        "a self-uninstalled home starts nothing"
    );
}

#[test]
fn uninstall_withdraws_the_marker_ends_reaps_strays_by_config_and_verifies() {
    let home = Home::new();
    std::fs::write(home.marker(), "").unwrap();
    let mut host = FakeHost::with(vec![home.registered(TASK_STATE_RUNNING)]);
    host.run_value = Some(OLD_RUN_VALUE.into());
    let ended = uninstall(&home.task(), &host).unwrap();
    assert!(
        !executor_installed(&home.config) && !executor_may_start(&home.config),
        "the next tick's supervisor starts nothing, and status reads not installed"
    );
    assert_eq!(
        host.calls(),
        [
            "end CairnExecutor".to_string(),
            "delete run value".to_string(),
            format!("terminate run value {OLD_RUN_VALUE}"),
            format!(
                r"terminate {} [supervise --config {cfg} | run --config {cfg}]",
                home.exe.display(),
                cfg = home.config.display()
            ),
            "find".to_string(),
        ],
        "the task is ended and stays registered"
    );
    assert_eq!(ended.len(), 1);
}

#[test]
fn uninstall_of_a_stopped_task_ends_nothing_and_never_creates_a_home() {
    let (exe, config, home) = (exe(), config(), home());
    let host = FakeHost::with(vec![Some(ScheduledTaskState {
        state: TASK_STATE_READY,
        enabled: true,
        ..ScheduledTaskState::default()
    })]);
    uninstall(&task(&exe, &config), &host).unwrap();
    assert!(!host.calls().iter().any(|call| call.starts_with("end")));
    assert!(!home.exists());
}

#[test]
fn uninstall_names_what_survived_it() {
    let home = Home::new();
    let mut host = FakeHost::with(vec![home.registered(TASK_STATE_RUNNING)]);
    host.left_running = vec![4242];
    host.end = Err(TaskRefusal {
        hresult: Some(E_ACCESSDENIED),
        message: "Access is denied.".into(),
    });
    let error = uninstall(&home.task(), &host).unwrap_err();
    assert!(
        error.contains("cairn may not end the scheduled task CairnExecutor"),
        "{error}"
    );
    assert!(
        error.contains("still running after the uninstall: 4242"),
        "{error}"
    );
}

#[test]
fn the_self_uninstall_neither_ends_nor_removes_the_task() {
    let home = Home::new();
    let mut host = FakeHost::with(vec![home.registered(TASK_STATE_RUNNING)]);
    host.run_value = Some("\"C:\\old\\cairn-exec.exe\" run".into());
    unregister_from_within(&home.task(), &host).unwrap();
    assert_eq!(host.calls(), ["delete run value"]);
}

/// An enrollment that overrides the binary or home gets a remedy that
/// registers the task it verifies against, instead of the defaults again.
#[test]
fn the_remedy_names_the_paths_an_enrollment_overrides() {
    let (exe, config) = (exe(), config());
    assert!(
        setup_overrides(&task(&exe, &config), Some(r"C:\Users\cairn")).is_empty(),
        "the runner's defaults need no arguments"
    );
    assert!(
        setup_overrides(&task(&exe, &config), Some(r"c:\users\CAIRN\")).is_empty(),
        "compared as Windows compares paths"
    );

    let elsewhere = PathBuf::from(r"D:\cairn\bin\cairn-exec.exe");
    let other_home = PathBuf::from(r"D:\cairn\home\executor.json");
    assert_eq!(
        setup_overrides(&task(&elsewhere, &config), Some(r"C:\Users\cairn")),
        [r#"-ExecutorBinary "D:\cairn\bin\cairn-exec.exe""#]
    );
    let both = setup_overrides(&task(&elsewhere, &other_home), Some(r"C:\Users\cairn"));
    assert_eq!(
        both,
        [
            r#"-ExecutorBinary "D:\cairn\bin\cairn-exec.exe""#,
            r#"-ExecutorConfig "D:\cairn\home\executor.json""#
        ]
    );
    assert_eq!(
        setup_overrides(&task(&exe, &config), None).len(),
        2,
        "with no profile to compare, both are named"
    );
    assert!(setup_remedy("cairn", &both).contains(
        r#"-ExecutorUser cairn -ExecutorBinary "D:\cairn\bin\cairn-exec.exe" -ExecutorConfig "D:\cairn\home\executor.json"`"#
    ));
}

#[test]
fn install_on_overridden_paths_names_them_in_its_remedy() {
    let home = Home::new();
    let host = FakeHost::with(vec![None]);
    let error = install(&home.task(), &host, quick()).unwrap_err();
    assert!(error.contains("-ExecutorBinary \""), "{error}");
    assert!(error.contains("-ExecutorConfig \""), "{error}");
}

/// Measured on bgsrv-win: a registration whose program is gone records
/// 0x80070002 on every tick and runs nothing. That is the stopped state, not
/// a failure.
#[test]
fn a_registration_waiting_for_its_executor_is_not_a_failure() {
    let waiting = ScheduledTaskState {
        state: TASK_STATE_READY,
        enabled: true,
        last_task_result: ACTION_FILE_NOT_FOUND,
        ..ScheduledTaskState::default()
    };
    assert_eq!(runtime_of(&waiting).last_exit_status, None);
    assert!(!runtime_of(&waiting).running);
}
