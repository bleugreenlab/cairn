//! Why a confined macOS process cannot run `ps`, said where it fails.
//!
//! macOS will not let a process inside a Seatbelt profile execute a setuid
//! binary: the exec fails with `EPERM`, and the launching shell reports
//! `Operation not permitted` with exit status 126 (127 from zsh). Every macOS
//! agent spawn, and every branch-code check on a machine holding an operator
//! credential, runs inside a profile (see [`crate::operator_credential`]), so
//! `/bin/ps` and `/usr/bin/top`, both setuid root, never run there. That is an
//! accepted cost of the floor and not a rule a grant could lift: no profile
//! exception is known to reach past the platform's setuid refusal without
//! widening the floor itself. The refusal is therefore explained in the output
//! it lands in, not routed to a fence prompt or retried.
//!
//! Recognition is deliberately narrow, because `Operation not permitted` is also
//! what every ordinary refused write or read says. Only a launcher's own
//! exec-failure line naming one of these tools counts, and a bare tool name
//! (which is also how a shell reports a refused redirect into a file called
//! `ps`) counts only when the command ended with an exec-failure status. An
//! absolute tool path counts regardless of the final status: a later command
//! can succeed after the refused exec. Python's exact EPERM `PermissionError`
//! naming a quoted inspection tool also counts independently of the final status.

use crate::{Confinement, MacosProfile};

/// The setuid process-inspection tools the floor refuses: the bare name a
/// launcher may report, and the absolute path `sh` and `bash` report after
/// resolving `PATH`.
const SETUID_INSPECTION_TOOLS: [(&str, &str); 2] = [("ps", "/bin/ps"), ("top", "/usr/bin/top")];

/// Programs whose exec-failure line takes the shape `LABEL: [LINE: ]TOOL: Operation
/// not permitted`, or zsh's `zsh:LINE: operation not permitted: TOOL`.
/// `cairn-procedure` is the `$0` a run item's shell carries when the item passes
/// procedure arguments.
const LAUNCHERS: [&str; 10] = [
    "sh",
    "bash",
    "dash",
    "zsh",
    "env",
    "xargs",
    "nohup",
    "nice",
    "timeout",
    "cairn-procedure",
];

const EPERM_MESSAGE: &str = "Operation not permitted";

/// The diagnostic to append to a command's output when the macOS
/// Seatbelt profile this spawn ran under refused it a setuid process-inspection
/// tool, or `None` for every other outcome.
///
/// The command's own output and exit status are left exactly as they were; this
/// only says why, and what to use instead.
pub fn setuid_refusal_note(
    confinement: &Confinement,
    exit_code: Option<i32>,
    output: &str,
) -> Option<String> {
    let tool = refused_setuid_inspection_tool(exit_code, output)?;
    let profile = confinement.macos_profile()?;
    Some(refusal_note(tool, profile))
}

/// The cause first, as the kernel states it (any Seatbelt profile refuses
/// setuid execution), then why that profile is there, claiming the credential
/// floor only when the profile actually carries it.
fn refusal_note(tool: &str, profile: MacosProfile) -> String {
    let why_the_profile = if profile.carries_credential_floor {
        "Cairn cannot run this command without that profile on this machine: it carries the \
         operator credential floor, which keeps every Cairn home's `operator_auth_secret` \
         unreadable to agent and branch-code processes, so no fence answer changes this."
    } else {
        "The profile here is this command's worktree fence; this machine's profile carries no \
         operator credential rules."
    };
    format!(
        "Cairn: macOS refused to execute `{tool}` because this command runs inside a macOS \
         Seatbelt sandbox profile, and macOS never lets a sandboxed process execute a setuid \
         binary (`ps` and `top` are setuid). {why_the_profile} To find a process by name use \
         `pgrep` (for example `pgrep -fl NAME`), which is not setuid and works here."
    )
}

/// The canonical path of the setuid inspection tool a command's output
/// shows a launcher was refused, if it shows one.
fn refused_setuid_inspection_tool(exit_code: Option<i32>, output: &str) -> Option<&'static str> {
    let ended_on_exec_failure = matches!(exit_code, Some(126 | 127));
    output.lines().find_map(|line| {
        let (tool, unambiguous) = exec_refused_tool(line.trim())?;
        (unambiguous || ended_on_exec_failure).then_some(tool)
    })
}

/// The tool one exec-failure line names, and whether its evidence is independent
/// of the final status (an absolute launcher path or Python's EPERM exception).
fn exec_refused_tool(line: &str) -> Option<(&'static str, bool)> {
    if let Some(operand) = line.strip_prefix("PermissionError: [Errno 1] Operation not permitted: ")
    {
        let named = operand.strip_prefix('\'')?.strip_suffix('\'')?;
        return SETUID_INSPECTION_TOOLS
            .iter()
            .find_map(|(name, path)| (named == *name || named == *path).then_some((*path, true)));
    }
    let segments: Vec<&str> = line.split(": ").collect();
    let (label, rest) = segments.split_first()?;
    if !is_launcher(label) {
        return None;
    }
    let named = match rest {
        [message, tool] if message.eq_ignore_ascii_case(EPERM_MESSAGE) => *tool,
        [tool, message] if message.eq_ignore_ascii_case(EPERM_MESSAGE) => *tool,
        [marker, tool, message]
            if is_line_marker(marker) && message.eq_ignore_ascii_case(EPERM_MESSAGE) =>
        {
            *tool
        }
        _ => return None,
    };
    SETUID_INSPECTION_TOOLS
        .iter()
        .find_map(|(name, path)| match named {
            named if named == *path => Some((*path, true)),
            named if named == *name => Some((*path, false)),
            _ => None,
        })
}

/// `sh`, `/bin/bash`, `zsh:1`: a launcher's name, by path or with zsh's line.
fn is_launcher(label: &str) -> bool {
    let label = match label.rsplit_once(':') {
        Some((name, line)) if !line.is_empty() && line.bytes().all(|b| b.is_ascii_digit()) => name,
        _ => label,
    };
    let name = label.rsplit('/').next().unwrap_or(label);
    LAUNCHERS.contains(&name)
}

/// dash's `1` and bash's `line 1` between the launcher and the tool.
fn is_line_marker(segment: &str) -> bool {
    let number = segment.strip_prefix("line ").unwrap_or(segment);
    !number.is_empty() && number.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every shape measured on macOS 26 under the floor, each with the exit
    /// status its launcher produced.
    const REFUSALS: [(&str, i32, &str); 8] = [
        ("/bin/sh: /bin/ps: Operation not permitted", 126, "/bin/ps"),
        (
            "/bin/bash: /usr/bin/top: Operation not permitted",
            126,
            "/usr/bin/top",
        ),
        (
            "bash: line 3: /bin/ps: Operation not permitted",
            126,
            "/bin/ps",
        ),
        ("/bin/dash: 1: ps: Operation not permitted", 126, "/bin/ps"),
        ("zsh:1: operation not permitted: top", 127, "/usr/bin/top"),
        ("env: ps: Operation not permitted", 126, "/bin/ps"),
        ("xargs: ps: Operation not permitted", 126, "/bin/ps"),
        (
            "cairn-procedure: /bin/ps: Operation not permitted",
            126,
            "/bin/ps",
        ),
    ];

    #[test]
    fn every_launcher_shape_of_a_refused_inspection_tool_is_recognized() {
        for (line, code, tool) in REFUSALS {
            assert_eq!(
                refused_setuid_inspection_tool(Some(code), line),
                Some(tool),
                "{line}"
            );
        }
    }

    #[test]
    fn a_refusal_inside_other_output_is_still_found() {
        let output = "starting probe\n/bin/sh: /bin/ps: Operation not permitted\r\nprobe done";
        assert_eq!(
            refused_setuid_inspection_tool(Some(126), output),
            Some("/bin/ps")
        );
    }

    /// `ps aux | grep x` ends with grep's status, not the shell's 126. A tool
    /// named by its absolute path is the shell having resolved and executed it,
    /// so that still counts.
    #[test]
    fn a_pipeline_that_fails_downstream_of_the_refusal_still_counts_by_path() {
        assert_eq!(
            refused_setuid_inspection_tool(Some(1), "/bin/sh: /bin/ps: Operation not permitted"),
            Some("/bin/ps")
        );
    }

    #[test]
    fn recovered_diagnostic_script_still_reports_the_refused_exec_once() {
        let stderr = "/bin/sh: /bin/ps: Operation not permitted\n\
                      /bin/sh: /bin/ps: Operation not permitted\n\
                      /bin/sh: /bin/ps: Operation not permitted\n\
                      /bin/sh: /bin/ps: Operation not permitted\n\
                      /bin/sh: /usr/bin/top: Operation not permitted";
        let worktree = tempfile::tempdir().unwrap();
        let fenced = Confinement::Fenced(crate::SandboxPolicy::for_run(
            worktree.path(),
            &[],
            Vec::new(),
        ));
        for output in [stderr.to_string(), format!("{stderr}\nprobe done")] {
            for code in [Some(0), None] {
                assert_eq!(
                    refused_setuid_inspection_tool(code, &output),
                    Some("/bin/ps")
                );
                let note = setuid_refusal_note(&fenced, code, &output);
                assert_eq!(note.is_some(), cfg!(target_os = "macos"));
                if let Some(note) = note {
                    assert_eq!(note.matches("Cairn:").count(), 1);
                }
                assert!(setuid_refusal_note(&Confinement::Unconfined, code, &output).is_none());
            }
        }
    }

    #[test]
    fn python_eperm_inspection_refusals_are_explained_once() {
        let worktree = tempfile::tempdir().unwrap();
        let fenced = Confinement::Fenced(crate::SandboxPolicy::for_run(
            worktree.path(),
            &[],
            Vec::new(),
        ));
        for (operand, tool) in [
            ("ps", "/bin/ps"),
            ("top", "/usr/bin/top"),
            ("/bin/ps", "/bin/ps"),
            ("/usr/bin/top", "/usr/bin/top"),
        ] {
            let traceback = format!(
                "Traceback (most recent call last):\n  File \"/Users/dev/.local/share/uv/python/cpython-3.12.13-macos-aarch64-none/lib/python3.12/subprocess.py\", line 1955, in _execute_child\n    raise child_exception_type(errno_num, err_msg, err_filename)\nPermissionError: [Errno 1] Operation not permitted: '{operand}'"
            );
            for code in [Some(1), Some(0), None] {
                assert_eq!(refused_setuid_inspection_tool(code, &traceback), Some(tool));
                let note = setuid_refusal_note(&fenced, code, &traceback);
                assert_eq!(note.is_some(), cfg!(target_os = "macos"));
                if let Some(note) = note {
                    assert_eq!(note.matches("Cairn:").count(), 1);
                    assert!(note.contains(tool));
                }
            }
        }
        for ordinary in [
            "PermissionError: [Errno 13] Permission denied: '/Users/x/file'",
            "PermissionError: [Errno 1] Operation not permitted: '/Users/x/file'",
            "PermissionError: [Errno 13] Permission denied: 'ps'",
        ] {
            assert_eq!(refused_setuid_inspection_tool(Some(1), ordinary), None);
        }
    }

    #[test]
    fn ordinary_permission_failures_keep_their_own_meaning() {
        let unrelated: [(&str, Option<i32>); 11] = [
            // A refused write or read named by the tool that attempted it.
            ("touch: /System/x: Operation not permitted", Some(1)),
            ("cat: /bin/ps: Operation not permitted", Some(1)),
            (
                "cat: /Users/u/.cairn/operator_auth_secret: Operation not permitted",
                Some(1),
            ),
            // `ps` itself ran and reports something it could not do.
            ("ps: sysctl: Operation not permitted", Some(1)),
            // A refused redirect into a file named `ps` (bash's shape, exit 1).
            ("bash: line 1: ps: Operation not permitted", Some(1)),
            ("zsh:1: operation not permitted: ps", Some(1)),
            // A non-executable file is EACCES, not the setuid refusal.
            ("/bin/bash: /bin/ps: Permission denied", Some(126)),
            // Another program refused for another reason.
            (
                "/bin/sh: /bin/launchctl: Operation not permitted",
                Some(126),
            ),
            (
                "sandbox-exec: sandbox_apply: Operation not permitted",
                Some(71),
            ),
            // Bare names still need an exec-failure status, even after recovery.
            ("env: ps: Operation not permitted", Some(0)),
            ("zsh:1: operation not permitted: top", None),
        ];
        for (line, code) in unrelated {
            assert_eq!(
                refused_setuid_inspection_tool(code, line),
                None,
                "{line} ({code:?})"
            );
        }
    }

    #[test]
    fn an_unconfined_spawn_is_never_told_about_the_floor() {
        assert_eq!(
            setuid_refusal_note(
                &Confinement::Unconfined,
                Some(126),
                "/bin/sh: /bin/ps: Operation not permitted"
            ),
            None
        );
    }

    #[test]
    fn the_note_states_the_kernel_cause_and_names_the_floor_only_where_it_applies() {
        let floored = refusal_note(
            "/bin/ps",
            MacosProfile {
                carries_credential_floor: true,
            },
        );
        let fenced_only = refusal_note(
            "/bin/ps",
            MacosProfile {
                carries_credential_floor: false,
            },
        );
        for note in [&floored, &fenced_only] {
            assert!(note.contains("`/bin/ps`"), "{note}");
            assert!(note.contains("Seatbelt sandbox profile"), "{note}");
            assert!(note.contains("setuid"), "{note}");
            assert!(note.contains("`pgrep`"), "{note}");
        }
        assert!(floored.contains("operator credential floor"), "{floored}");
        assert!(
            floored.contains("cannot run this command without that profile"),
            "{floored}"
        );
        // A profile with no credential rules must not blame a credential.
        assert!(fenced_only.contains("worktree fence"), "{fenced_only}");
        assert!(
            !fenced_only.contains("operator credential floor"),
            "{fenced_only}"
        );
        assert!(
            !fenced_only.contains("operator_auth_secret"),
            "{fenced_only}"
        );
    }

    /// A fenced spawn always runs inside a Seatbelt profile on macOS and inside
    /// none anywhere else, so only macOS ever annotates.
    #[test]
    fn only_a_macos_profile_annotates() {
        let worktree = tempfile::tempdir().unwrap();
        let fenced = Confinement::Fenced(crate::SandboxPolicy::for_run(
            worktree.path(),
            &[],
            Vec::new(),
        ));
        let note = setuid_refusal_note(
            &fenced,
            Some(126),
            "/bin/sh: /bin/ps: Operation not permitted",
        );
        assert_eq!(note.is_some(), cfg!(target_os = "macos"), "{note:?}");
    }

    /// The shapes above are pinned against the real kernel: under the agent
    /// floor or a check lane's inherited profile the shell is already refused
    /// `ps`; in an unconfined developer shell the smallest profile is applied.
    /// Either way `pgrep` runs where `ps` cannot. `pgrep` leaves out its own
    /// ancestors, so the shell is never a match for itself; the script plants
    /// a background process, finds it by pid (its name may still be the
    /// shell's until it execs), then kills and reaps it. Nothing else running
    /// on the machine can change the answer.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_profiled_shell_is_refused_ps_but_not_pgrep_live() {
        let script = "/bin/sleep 30 & planted=$!; pgrep -U \"$(id -u)\" | grep -qx \"$planted\"; echo pgrep=$?; kill $planted; wait $planted 2>/dev/null; /bin/ps -p $$ -o pid=";
        let args = vec!["-c".to_string(), script.to_string()];
        let (program, args) = if crate::current_process_is_confined() {
            ("/bin/sh".to_string(), args)
        } else {
            crate::macos::wrap_argv_with_profile(
                "/bin/sh",
                &args,
                "(version 1)\n(allow default)\n".to_string(),
            )
        };
        let output = std::process::Command::new(program)
            .args(args)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(126), "{stdout}{stderr}");
        assert!(stdout.contains("pgrep=0"), "{stdout}{stderr}");
        assert_eq!(
            refused_setuid_inspection_tool(output.status.code(), &stderr),
            Some("/bin/ps"),
            "{stderr}"
        );
    }
}
