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
