//! Cross-platform coverage for the pure unit generators. They compile on every
//! host, so one machine can assert what all three service managers will be told.

use std::path::PathBuf;

use super::*;

fn spec() -> ServiceSpec {
    ServiceSpec {
        label: "computer.cairn.test".to_string(),
        unit_name: "cairn-test.service".to_string(),
        run_value: "CairnTest".to_string(),
        description: "Cairn test daemon".to_string(),
        exe: PathBuf::from("/opt/cairn/cairn-test"),
        args: vec!["run".to_string()],
        env: vec![("CAIRN_ENV".to_string(), "dev".to_string())],
        log_dir: PathBuf::from("/tmp/throwaway-home"),
        log_prefix: "test".to_string(),
        file_descriptor_limit: 8192,
        supervision: Supervision::always_on(),
        legacy: None,
    }
}

#[test]
fn plist_carries_program_arguments_env_and_logs() {
    let out = plist_contents(&spec());
    assert!(out.contains("<string>computer.cairn.test</string>"));
    assert!(out.contains("<string>/opt/cairn/cairn-test</string>"));
    assert!(out.contains("<string>run</string>"));
    assert!(out.contains("<key>RunAtLoad</key>"));
    assert!(out.contains("<key>CAIRN_ENV</key>"));
    assert!(out.contains("<string>dev</string>"));
    assert!(out.contains("/tmp/throwaway-home/test.out.log"));
    assert!(out.contains("/tmp/throwaway-home/test.err.log"));
    assert_eq!(out.matches("<integer>8192</integer>").count(), 2);
}

#[test]
fn plist_omits_env_block_when_unset() {
    let mut spec = spec();
    spec.env.clear();
    let out = plist_contents(&spec);
    assert!(!out.contains("EnvironmentVariables"));
    assert!(!out.contains("CAIRN_ENV"));
}

/// An always-on daemon is restarted whatever its exit status, and inherits
/// launchd's own throttle default rather than naming one.
#[test]
fn always_on_supervision_keeps_alive_unconditionally() {
    let out = plist_contents(&spec());
    assert!(out.contains("<key>KeepAlive</key>\n\t<true/>"));
    assert!(!out.contains("ThrottleInterval"));
    assert!(systemd_unit_contents(&spec()).contains("RestartSec=5"));
}

/// Both service managers express one cadence from one number. This is the guard
/// against the platforms drifting apart: a change to the delay has to move both
/// generated units or fail here.
#[test]
fn retry_on_failure_expresses_one_cadence_on_both_managers() {
    let mut spec = spec();
    spec.supervision = Supervision::retry_on_failure(60);

    let plist = plist_contents(&spec);
    // A clean exit is a decision and stays stopped…
    assert!(plist.contains("<key>SuccessfulExit</key>\n\t\t<false/>"));
    assert!(!plist.contains("<key>KeepAlive</key>\n\t<true/>"));
    // …and a failure waits the shared interval, overriding launchd's 10s default.
    assert!(plist.contains("<key>ThrottleInterval</key>\n\t<integer>60</integer>"));

    let unit = systemd_unit_contents(&spec);
    assert!(unit.contains("Restart=on-failure"));
    assert!(unit.contains("RestartSec=60"));
    // Never latch a refused daemon off: the fix lives in a config file on the
    // machine, and a stopped unit would require someone to log in to apply it.
    assert!(!unit.contains("RestartPreventExitStatus"));
}

#[test]
fn systemd_unit_has_execstart_env_and_install_target() {
    let out = systemd_unit_contents(&spec());
    assert!(out.contains("Description=Cairn test daemon"));
    assert!(out.contains("ExecStart=/opt/cairn/cairn-test run"));
    assert!(out.contains("Environment=\"CAIRN_ENV=dev\""));
    assert!(out.contains("LimitNOFILE=8192"));
    assert!(out.contains("WantedBy=default.target"));
}

/// A child the kernel OOM-kills is not the daemon's end, and a daemon that
/// keeps failing is retried rather than parked by systemd's start limit.
#[test]
fn systemd_unit_survives_a_childs_oom_kill_and_never_latches_off() {
    let unit = systemd_unit_contents(&spec());
    assert!(unit.contains("[Service]"));
    assert!(unit.contains("\nOOMPolicy=continue\n"));
    let (unit_section, _) = unit.split_once("[Service]").unwrap();
    assert!(
        unit_section.contains("\nStartLimitIntervalSec=0\n"),
        "the start limit is a [Unit] setting"
    );
}

/// Only a daemon that bounds its own exit names a short stop window; the rest
/// keep systemd's default.
#[test]
fn systemd_stop_timeout_is_written_only_when_the_spec_names_one() {
    assert!(!systemd_unit_contents(&spec()).contains("TimeoutStopSec"));
    let mut bounded = spec();
    bounded.supervision = Supervision::retry_on_failure(5).with_stop_timeout(15);
    assert!(systemd_unit_contents(&bounded).contains("\nTimeoutStopSec=15\n"));
}

const UNIT_CGROUP: &str =
    "0::/user.slice/user-1000.slice/user@1000.service/app.slice/cairn-test.service\n";

#[test]
fn the_units_own_cgroup_is_recognized_on_v2_and_v1() {
    assert!(cgroup_is_unit(UNIT_CGROUP, "cairn-test.service"));
    assert!(cgroup_is_unit(
        "12:memory:/user.slice\n1:name=systemd:/user.slice/user@1000.service/cairn-test.service\n",
        "cairn-test.service"
    ));
    // A login session's scope, or another unit, is not this unit.
    assert!(!cgroup_is_unit(
        "0::/user.slice/user-1000.slice/session-3.scope\n",
        "cairn-test.service"
    ));
    assert!(!cgroup_is_unit(
        "0::/user.slice/user-1000.slice/user@1000.service/app.slice/cairn-orch.service\n",
        "cairn-test.service"
    ));
    assert!(!cgroup_is_unit("", "cairn-test.service"));
}

#[test]
fn a_drifted_unit_is_rewritten_and_reloaded_only_by_its_own_process() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cairn-test.service");
    std::fs::write(&path, "[Service]\nRestartSec=60\n").unwrap();
    let spec = spec();

    // Another unit's process (a dev run, a colocated child) leaves it alone.
    let mut reloaded = false;
    let outcome = refresh_systemd_unit(
        &spec,
        Some("0::/user.slice/user-1000.slice/session-3.scope\n"),
        &path,
        || true,
        || {
            reloaded = true;
            Ok(())
        },
    );
    assert_eq!(outcome, Ok(SystemdRefresh::NotTheUnit));
    assert!(!reloaded);
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "[Service]\nRestartSec=60\n"
    );
    assert_eq!(
        refresh_systemd_unit(&spec, None, &path, || true, || Ok(())),
        Ok(SystemdRefresh::NotTheUnit)
    );

    // The unit's own process rewrites it and reloads, once.
    let mut reloads = 0;
    let outcome = refresh_systemd_unit(
        &spec,
        Some(UNIT_CGROUP),
        &path,
        || false,
        || {
            reloads += 1;
            Ok(())
        },
    );
    assert_eq!(outcome, Ok(SystemdRefresh::Rewritten));
    assert_eq!(reloads, 1);
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        systemd_unit_contents(&spec)
    );
    let outcome = refresh_systemd_unit(
        &spec,
        Some(UNIT_CGROUP),
        &path,
        || false,
        || {
            reloads += 1;
            Ok(())
        },
    );
    assert_eq!(outcome, Ok(SystemdRefresh::Current));
    assert_eq!(reloads, 1);

    // A current file whose earlier reload failed is reloaded now.
    let outcome = refresh_systemd_unit(
        &spec,
        Some(UNIT_CGROUP),
        &path,
        || true,
        || {
            reloads += 1;
            Ok(())
        },
    );
    assert_eq!(outcome, Ok(SystemdRefresh::Reloaded));
    assert_eq!(reloads, 2);

    // Nothing installed is nothing to refresh.
    assert_eq!(
        refresh_systemd_unit(
            &spec,
            Some(UNIT_CGROUP),
            &dir.path().join("absent.service"),
            || true,
            || Ok(())
        ),
        Ok(SystemdRefresh::NotInstalled)
    );
}

/// The headless runner's shape: its unit launches through a stable
/// `current/` symlink that activation swaps, while the running binary sees
/// itself at the versioned path the symlink resolves to. The refresh keeps
/// the installed program, so the unit never gets pinned to one version.
#[cfg(unix)]
#[test]
fn a_unit_launching_through_a_symlink_keeps_its_symlinked_program() {
    let dir = tempfile::tempdir().unwrap();
    let versioned = dir.path().join("versions").join("v1");
    std::fs::create_dir_all(&versioned).unwrap();
    std::fs::write(versioned.join("cairn-test"), "binary").unwrap();
    std::os::unix::fs::symlink(&versioned, dir.path().join("current")).unwrap();
    let stable = dir.path().join("current").join("cairn-test");

    let mut installed = spec();
    installed.exe = stable.clone();
    let path = dir.path().join("cairn-test.service");
    std::fs::write(&path, systemd_unit_contents(&installed)).unwrap();

    // What the running binary would build: its own resolved path.
    let mut running = spec();
    running.exe = std::fs::canonicalize(versioned.join("cairn-test")).unwrap();
    assert_eq!(
        refresh_systemd_unit(&running, Some(UNIT_CGROUP), &path, || false, || Ok(())),
        Ok(SystemdRefresh::Current),
        "the same binary through its symlink is not drift"
    );

    // A template drift is rewritten, and still through the symlink.
    std::fs::write(
        &path,
        systemd_unit_contents(&installed).replace("OOMPolicy=continue\n", ""),
    )
    .unwrap();
    assert_eq!(
        refresh_systemd_unit(&running, Some(UNIT_CGROUP), &path, || false, || Ok(())),
        Ok(SystemdRefresh::Rewritten)
    );
    let rewritten = std::fs::read_to_string(&path).unwrap();
    assert_eq!(rewritten, systemd_unit_contents(&installed));
    assert!(rewritten.contains(&format!("ExecStart={} run", stable.display())));

    // A unit naming some other binary is drift, and takes the running one.
    let mut elsewhere = spec();
    elsewhere.exe = dir.path().join("old").join("cairn-test");
    std::fs::write(&path, systemd_unit_contents(&elsewhere)).unwrap();
    assert_eq!(
        refresh_systemd_unit(&running, Some(UNIT_CGROUP), &path, || false, || Ok(())),
        Ok(SystemdRefresh::Rewritten)
    );
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        systemd_unit_contents(&running)
    );
}

#[test]
fn systemd_install_restarts_an_active_generation() {
    assert_eq!(
        systemd_install_steps("cairn-test.service"),
        vec![
            vec!["daemon-reload".to_string()],
            vec![
                "enable".to_string(),
                "--now".to_string(),
                "cairn-test.service".to_string()
            ],
            vec!["restart".to_string(), "cairn-test.service".to_string()],
        ]
    );
}

#[test]
fn windows_run_command_quotes_paths_but_not_flags() {
    let mut spec = spec();
    spec.args = vec![
        "run".to_string(),
        "--config".to_string(),
        "/tmp/throwaway-home/executor.json".to_string(),
    ];
    assert_eq!(
        windows_run_command(&spec),
        "\"/opt/cairn/cairn-test\" run --config \"/tmp/throwaway-home/executor.json\""
    );
    assert!(!windows_run_command(&spec).contains("cmd.exe"));
    assert!(!windows_run_command(&spec).contains("schtasks"));
}

#[test]
fn windows_run_command_without_args_is_just_the_program() {
    let mut spec = spec();
    spec.args.clear();
    assert_eq!(windows_run_command(&spec), "\"/opt/cairn/cairn-test\"");
}

#[test]
fn xml_escape_covers_all_predefined_entities() {
    assert_eq!(
        xml_escape("a & b < c > d \" e ' f"),
        "a &amp; b &lt; c &gt; d &quot; e &apos; f"
    );
    // Ampersand is escaped exactly once (not double-escaped by later passes).
    assert_eq!(xml_escape("<tag>"), "&lt;tag&gt;");
}

/// A launchd job that died at startup: not running, its exit code, and how
/// many times launchd spawned it. Nested blocks that reuse `state` are ignored.
#[test]
fn launchctl_print_reports_a_job_that_keeps_dying() {
    let output = "gui/501/computer.cairn.exec = {
	active count = 0
	path = /Users/dev/Library/LaunchAgents/computer.cairn.exec.plist
	type = LaunchAgent
	state = not running

	program = /Users/dev/.local/bin/cairn-exec
	arguments = {
		/Users/dev/.local/bin/cairn-exec
		run
	}

	event triggers = {
		state = running
	}

	runs = 4
	last exit code = 1
	spawn type = interactive (4)
}
";
    assert_eq!(
        parse_launchctl_print(output),
        ServiceRuntime {
            running: false,
            pid: None,
            last_exit_status: Some(1),
            restarts: Some(3),
        }
    );
}

#[test]
fn launchctl_print_reports_a_running_job_that_never_exited() {
    let output = "gui/501/computer.cairn.exec = {
	state = running
	runs = 1
	pid = 4242
	last exit code = (never exited)
}
";
    assert_eq!(
        parse_launchctl_print(output),
        ServiceRuntime {
            running: true,
            pid: Some(4242),
            last_exit_status: None,
            restarts: Some(0),
        }
    );
}

/// A unit between restarts is not running, and its last exit is reported only
/// because the main process has exited.
#[test]
fn systemctl_show_reports_a_unit_waiting_to_restart() {
    let output = "ActiveState=activating\nSubState=auto-restart\nMainPID=0\nExecMainCode=1\nExecMainStatus=78\nNRestarts=5\n";
    assert_eq!(
        parse_systemctl_show(output),
        ServiceRuntime {
            running: false,
            pid: None,
            last_exit_status: Some(78),
            restarts: Some(5),
        }
    );

    let running = "ActiveState=active\nSubState=running\nMainPID=900\nExecMainCode=0\nExecMainStatus=0\nNRestarts=0\n";
    assert_eq!(
        parse_systemctl_show(running),
        ServiceRuntime {
            running: true,
            pid: Some(900),
            last_exit_status: None,
            restarts: Some(0),
        }
    );
}

/// The legacy runner's spec as the pre-rename generator shaped it: the home in
/// `CAIRN_HOME` on launchd and systemd, in `--data-dir` on Windows.
fn legacy_runner_spec(windows: bool) -> ServiceSpec {
    ServiceSpec {
        label: PRE_RENAME_RUNNER_LABEL.to_string(),
        unit_name: PRE_RENAME_RUNNER_UNIT.to_string(),
        run_value: PRE_RENAME_RUNNER_RUN_VALUE.to_string(),
        exe: PathBuf::from(if windows {
            r"C:\Users\Dev User\AppData\Local\cairn\cairn-runner.exe"
        } else {
            "/usr/bin/cairn-runner"
        }),
        args: if windows {
            vec![
                "run".into(),
                "--data-dir".into(),
                r"C:\Users\Dev User\.cairn-executor".into(),
            ]
        } else {
            vec!["run".into()]
        },
        env: vec![
            ("CAIRN_ENV".into(), "prod".into()),
            ("CAIRN_HOME".into(), "/home/bleu/.cairn-executor".into()),
        ],
        ..spec()
    }
}

#[test]
fn a_generated_systemd_unit_reads_back_its_program_and_home() {
    let unit = systemd_unit_contents(&legacy_runner_spec(false));
    assert_eq!(
        parse_systemd_unit(&unit),
        UnitDescription {
            program: Some(PathBuf::from("/usr/bin/cairn-runner")),
            args: vec!["run".into()],
            cairn_home: Some(PathBuf::from("/home/bleu/.cairn-executor")),
        }
    );
}

#[test]
fn a_hand_written_systemd_unit_reads_back_too() {
    let unit = "[Service]\nExecStart=-\"/opt/cairn runner/bin/cairn-runner\" run --data-dir /srv/cairn\nEnvironment=CAIRN_ENV=prod OTHER=1\n";
    let description = parse_systemd_unit(unit);
    assert_eq!(
        description.program,
        Some(PathBuf::from("/opt/cairn runner/bin/cairn-runner"))
    );
    assert_eq!(description.cairn_home, Some(PathBuf::from("/srv/cairn")));
}

#[test]
fn a_launchd_plist_as_plutil_renders_it_reads_back_its_program_and_home() {
    let json = r#"{"Label":"computer.cairn.runner","ProgramArguments":["/Applications/cairn.app/Contents/MacOS/cairn-runner","run"],"EnvironmentVariables":{"CAIRN_ENV":"prod","CAIRN_HOME":"/Users/dev/.cairn-executor"},"RunAtLoad":true}"#;
    assert_eq!(
        parse_launchd_plist_json(json).unwrap(),
        UnitDescription {
            program: Some(PathBuf::from(
                "/Applications/cairn.app/Contents/MacOS/cairn-runner"
            )),
            args: vec!["run".into()],
            cairn_home: Some(PathBuf::from("/Users/dev/.cairn-executor")),
        }
    );
    // A unit that names no home leaves the default to the caller.
    let bare = r#"{"ProgramArguments":["/usr/local/bin/cairn-runner","run"]}"#;
    assert_eq!(parse_launchd_plist_json(bare).unwrap().cairn_home, None);
    assert!(parse_launchd_plist_json("not json").is_err());
}

#[test]
fn a_generated_run_value_reads_back_its_program_and_data_dir() {
    let value = windows_run_command(&legacy_runner_spec(true));
    assert_eq!(
        parse_run_value(&value),
        UnitDescription {
            program: Some(PathBuf::from(
                r"C:\Users\Dev User\AppData\Local\cairn\cairn-runner.exe"
            )),
            args: vec![
                "run".into(),
                "--data-dir".into(),
                r"C:\Users\Dev User\.cairn-executor".into()
            ],
            cairn_home: Some(PathBuf::from(r"C:\Users\Dev User\.cairn-executor")),
        }
    );
}

#[test]
fn the_runner_identities_are_distinct_on_every_platform() {
    let current = runner_identity();
    let legacy = pre_rename_runner_identity();
    assert_ne!(current.label, legacy.label);
    assert_ne!(current.unit_name, legacy.unit_name);
    assert_ne!(current.run_value, legacy.run_value);
    assert!(legacy.platform_name().is_some());
}

/// A logind stand-in: whether the user lingers, whether this session may turn
/// it on, and whether turning it on actually sticks.
struct FakeLinger {
    lingering: std::cell::Cell<bool>,
    enable: Result<(), String>,
    enable_sticks: bool,
    probe: Result<(), String>,
    enable_calls: std::cell::Cell<u32>,
}

impl FakeLinger {
    fn new(lingering: bool) -> Self {
        Self {
            lingering: std::cell::Cell::new(lingering),
            enable: Ok(()),
            enable_sticks: true,
            probe: Ok(()),
            enable_calls: std::cell::Cell::new(0),
        }
    }
}

impl Linger for FakeLinger {
    fn linger_enabled(&self, _user: &str) -> Result<bool, String> {
        self.probe.clone().map(|()| self.lingering.get())
    }
    fn enable_linger(&self, _user: &str) -> Result<(), String> {
        self.enable_calls.set(self.enable_calls.get() + 1);
        self.enable.clone()?;
        if self.enable_sticks {
            self.lingering.set(true);
        }
        Ok(())
    }
}

#[test]
fn a_lingering_user_is_left_alone() {
    let host = FakeLinger::new(true);
    assert_eq!(ensure_linger(&host, "dev"), Ok(()));
    assert_eq!(host.enable_calls.get(), 0);
}

#[test]
fn linger_this_session_may_enable_is_enabled() {
    let host = FakeLinger::new(false);
    assert_eq!(ensure_linger(&host, "dev"), Ok(()));
    assert_eq!(host.enable_calls.get(), 1);
    assert!(host.lingering.get());
}

#[test]
fn linger_polkit_refuses_is_a_refusal_naming_the_command() {
    let host = FakeLinger {
        enable: Err("Access denied".into()),
        ..FakeLinger::new(false)
    };
    let refusal = ensure_linger(&host, "dev").unwrap_err();
    assert_eq!(refusal.user, "dev");
    assert_eq!(refusal.cause, "Access denied");
    let message = refusal.to_string();
    assert!(message.contains("at logout"), "{message}");
    assert!(
        message.contains("\n  sudo loginctl enable-linger dev\n"),
        "{message}"
    );
}

#[test]
fn an_enable_logind_never_recorded_is_a_refusal() {
    let host = FakeLinger {
        enable_sticks: false,
        ..FakeLinger::new(false)
    };
    let refusal = ensure_linger(&host, "dev").unwrap_err();
    assert_eq!(refusal.cause, "loginctl still reports Linger=no");
}

#[test]
fn linger_that_cannot_be_read_is_a_refusal_without_an_enable() {
    let host = FakeLinger {
        probe: Err("loginctl is not available".into()),
        ..FakeLinger::new(false)
    };
    let refusal = ensure_linger(&host, "dev").unwrap_err();
    assert_eq!(refusal.cause, "loginctl is not available");
    assert_eq!(host.enable_calls.get(), 0);
}
