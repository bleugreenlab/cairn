use super::*;
use crate::execution::jobs::setup_progress::{emit as emit_setup, SetupSink};
use crate::services::ChildProcess;
use std::fmt;
use std::io::BufRead;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn get_shell_command() -> (&'static str, &'static str) {
    if cfg!(windows) {
        ("cmd.exe", "/c")
    } else {
        ("sh", "-c")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetupError {
    Cancelled,
    Spawn {
        command: String,
        message: String,
    },
    Failed {
        command: String,
        exit_code: Option<i32>,
        output: String,
    },
}

impl fmt::Display for SetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetupError::Cancelled => write!(f, "Setup cancelled"),
            SetupError::Spawn { command, message } => {
                write!(f, "Failed to execute setup command '{command}': {message}")
            }
            SetupError::Failed {
                command,
                exit_code,
                output,
            } => write!(
                f,
                "Setup command '{command}' failed with exit code {exit_code:?}\n{output}"
            ),
        }
    }
}

impl std::error::Error for SetupError {}

fn emit_reader_lines(
    mut reader: Box<dyn BufRead + Send>,
    sink: SetupSink,
    job_id: String,
    issue_id: Option<String>,
    kind: &'static str,
    command: String,
) -> String {
    let mut output = String::new();
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                let text = line.trim_end_matches(['\r', '\n']).to_string();
                output.push_str(&text);
                output.push('\n');
                emit_setup(
                    &sink,
                    &job_id,
                    issue_id.clone(),
                    kind,
                    Some("setup"),
                    Some(command.clone()),
                    Some(text),
                );
            }
            Err(e) => {
                let text = format!("[setup] failed to read {kind}: {e}");
                output.push_str(&text);
                output.push('\n');
                emit_setup(
                    &sink,
                    &job_id,
                    issue_id.clone(),
                    "stderr",
                    Some("setup"),
                    Some(command.clone()),
                    Some(text),
                );
                break;
            }
        }
    }
    output
}

/// Run setup commands in a worktree directory (injectable version).
/// Commands are executed sequentially in the worktree directory.
/// If any command fails, execution stops and returns an error.
pub fn run_setup_commands_with_process(
    process: &dyn ProcessSpawner,
    worktree_path: &Path,
    commands: &[String],
) -> Result<(), String> {
    let sink: SetupSink = Arc::new(|_| {});
    run_setup_commands_with_process_streaming(
        process,
        worktree_path,
        commands,
        &sink,
        "",
        None,
        &Arc::new(AtomicBool::new(false)),
        &Arc::new(Mutex::new(None)),
    )
    .map_err(|e| e.to_string())
}

#[allow(clippy::too_many_arguments)]
pub fn run_setup_commands_with_process_streaming(
    process: &dyn ProcessSpawner,
    worktree_path: &Path,
    commands: &[String],
    sink: &SetupSink,
    job_id: &str,
    issue_id: Option<String>,
    cancel: &Arc<AtomicBool>,
    child_slot: &Arc<Mutex<Option<Box<dyn ChildProcess>>>>,
) -> Result<(), SetupError> {
    if commands.is_empty() {
        return Ok(());
    }

    log::info!(
        "Running {} setup command(s) in {}",
        commands.len(),
        worktree_path.display()
    );

    let (shell, flag) = get_shell_command();

    for (idx, cmd) in commands.iter().enumerate() {
        if cancel.load(Ordering::SeqCst) {
            return Err(SetupError::Cancelled);
        }

        log::info!("Setup command {}/{}: {}", idx + 1, commands.len(), cmd);
        emit_setup(
            sink,
            job_id,
            issue_id.clone(),
            "status",
            Some("setup"),
            Some(cmd.clone()),
            Some(format!("[info] Running setup: {cmd}")),
        );

        let config = SpawnConfig::new(shell)
            .arg(flag)
            .arg(cmd)
            .cwd(&worktree_path.to_string_lossy());

        let mut child = process.spawn(config).map_err(|message| SetupError::Spawn {
            command: cmd.clone(),
            message,
        })?;

        let stdout = child.take_stdout();
        let stderr = child.take_stderr();
        {
            let mut slot = child_slot.lock().unwrap();
            *slot = Some(child);
        }

        let stderr_handle = stderr.map(|reader| {
            let sink = sink.clone();
            let job_id = job_id.to_string();
            let issue_id = issue_id.clone();
            let command = cmd.clone();
            thread::spawn(move || {
                emit_reader_lines(reader, sink, job_id, issue_id, "stderr", command)
            })
        });

        let mut output = String::new();
        if let Some(reader) = stdout {
            output.push_str(&emit_reader_lines(
                reader,
                sink.clone(),
                job_id.to_string(),
                issue_id.clone(),
                "stdout",
                cmd.clone(),
            ));
        }

        if let Some(handle) = stderr_handle {
            if let Ok(stderr_output) = handle.join() {
                output.push_str(&stderr_output);
            }
        }

        let status = loop {
            if cancel.load(Ordering::SeqCst) {
                if let Some(child) = child_slot.lock().unwrap().as_mut() {
                    let _ = child.kill();
                }
            }

            let maybe_status = {
                let mut slot = child_slot.lock().unwrap();
                match slot.as_mut() {
                    Some(child) => child.try_wait().map_err(|e| SetupError::Spawn {
                        command: cmd.clone(),
                        message: format!("Failed to wait for setup command: {e}"),
                    })?,
                    None => None,
                }
            };

            if let Some(status) = maybe_status {
                break status;
            }
            thread::sleep(Duration::from_millis(50));
        };

        {
            let mut slot = child_slot.lock().unwrap();
            *slot = None;
        }

        if cancel.load(Ordering::SeqCst) {
            return Err(SetupError::Cancelled);
        }

        if !status.success() {
            return Err(SetupError::Failed {
                command: cmd.clone(),
                exit_code: status.code(),
                output,
            });
        }
    }

    log::info!("All setup commands completed successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::jobs::setup_progress::{noop_sink, SetupProgress};
    use crate::services::testing::{MockChildProcess, MockProcessSpawner};

    fn capture_sink() -> (SetupSink, Arc<Mutex<Vec<SetupProgress>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink_events = events.clone();
        let sink: SetupSink = Arc::new(move |event| sink_events.lock().unwrap().push(event));
        (sink, events)
    }

    fn slot() -> Arc<Mutex<Option<Box<dyn ChildProcess>>>> {
        Arc::new(Mutex::new(None))
    }

    #[test]
    fn test_run_setup_commands_empty() {
        let mock = MockProcessSpawner::new();
        let result = run_setup_commands_with_process_streaming(
            &mock,
            Path::new("/tmp/test"),
            &[],
            &noop_sink(),
            "job-1",
            None,
            &Arc::new(AtomicBool::new(false)),
            &slot(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_setup_commands_single_success_streams_lines() {
        let mut mock = MockProcessSpawner::new();
        mock.expect_spawn()
            .withf(|config| {
                config.program == "sh"
                    && config.args == vec!["-c", "bun install"]
                    && config.cwd == Some("/tmp/worktree".to_string())
            })
            .times(1)
            .returning(|_| {
                let mut child = MockChildProcess::with_stdout(
                    1,
                    vec!["installed packages".to_string(), "done".to_string()],
                );
                child.set_exited();
                Ok(Box::new(child))
            });

        let (sink, events) = capture_sink();
        let result = run_setup_commands_with_process_streaming(
            &mock,
            Path::new("/tmp/worktree"),
            &["bun install".to_string()],
            &sink,
            "job-1",
            Some("issue-1".to_string()),
            &Arc::new(AtomicBool::new(false)),
            &slot(),
        );
        assert!(result.is_ok());
        let events = events.lock().unwrap();
        let lines: Vec<_> = events.iter().filter_map(|e| e.line.as_deref()).collect();
        assert_eq!(
            lines,
            vec![
                "[info] Running setup: bun install",
                "installed packages",
                "done"
            ]
        );
        assert_eq!(events[1].kind, "stdout");
    }

    #[test]
    fn test_run_setup_commands_multiple_success() {
        let mut mock = MockProcessSpawner::new();
        mock.expect_spawn().times(2).returning(|_| {
            let mut child = MockChildProcess::with_stdout(1, vec![]);
            child.set_exited();
            Ok(Box::new(child))
        });

        let commands = vec!["bun install".to_string(), "cargo build".to_string()];
        let result = run_setup_commands_with_process_streaming(
            &mock,
            Path::new("/tmp/worktree"),
            &commands,
            &noop_sink(),
            "job-1",
            None,
            &Arc::new(AtomicBool::new(false)),
            &slot(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_setup_commands_cancel_stops_before_next_command() {
        let mut mock = MockProcessSpawner::new();
        mock.expect_spawn().times(1).returning(|_| {
            let mut child = MockChildProcess::with_stdout(1, vec!["first".to_string()]);
            child.set_exited();
            Ok(Box::new(child))
        });

        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_after_first = cancel.clone();
        let sink: SetupSink = Arc::new(move |event| {
            if event.kind == "stdout" {
                cancel_after_first.store(true, Ordering::SeqCst);
            }
        });

        let commands = vec!["one".to_string(), "two".to_string()];
        let result = run_setup_commands_with_process_streaming(
            &mock,
            Path::new("/tmp/worktree"),
            &commands,
            &sink,
            "job-1",
            None,
            &cancel,
            &slot(),
        );
        assert_eq!(result, Err(SetupError::Cancelled));
    }

    #[test]
    fn test_run_setup_commands_first_fails_captures_output() {
        let mut mock = MockProcessSpawner::new();
        mock.expect_spawn().times(1).returning(|_| {
            Ok(Box::new(MockChildProcess::failing(
                1,
                "command not found",
                1,
            )))
        });

        let commands = vec!["bad-command".to_string(), "good-command".to_string()];
        let result = run_setup_commands_with_process_streaming(
            &mock,
            Path::new("/tmp/worktree"),
            &commands,
            &noop_sink(),
            "job-1",
            None,
            &Arc::new(AtomicBool::new(false)),
            &slot(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("bad-command"));
        assert!(err.contains("command not found"));
    }

    #[test]
    fn test_run_setup_commands_spawn_error() {
        let mut mock = MockProcessSpawner::new();
        mock.expect_spawn()
            .times(1)
            .returning(|_| Err("spawn failed".to_string()));

        let result = run_setup_commands_with_process_streaming(
            &mock,
            Path::new("/tmp/worktree"),
            &["echo hello".to_string()],
            &noop_sink(),
            "job-1",
            None,
            &Arc::new(AtomicBool::new(false)),
            &slot(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("spawn failed"));
    }

    #[cfg(unix)]
    #[test]
    fn test_cancel_kills_shell_descendant_and_returns_promptly() {
        use crate::services::RealProcessSpawner;
        use std::sync::mpsc;

        let temp = tempfile::tempdir().unwrap();
        let cancel = Arc::new(AtomicBool::new(false));
        let child_slot = slot();
        let (line_tx, line_rx) = mpsc::channel();
        let sink: SetupSink = Arc::new(move |event| {
            if event.kind == "stdout" && event.line.as_deref() == Some("child-started") {
                let _ = line_tx.send(());
            }
        });

        let cancel_for_thread = cancel.clone();
        let slot_for_thread = child_slot.clone();
        let path = temp.path().to_path_buf();
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let result = run_setup_commands_with_process_streaming(
                &RealProcessSpawner,
                &path,
                &["sleep 10 & echo child-started; wait".to_string()],
                &sink,
                "job-1",
                None,
                &cancel_for_thread,
                &slot_for_thread,
            );
            let _ = done_tx.send(result);
        });

        line_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("setup command should start child and stream first line");
        cancel.store(true, Ordering::SeqCst);
        if let Some(child) = child_slot.lock().unwrap().as_mut() {
            child.kill().unwrap();
        }

        let result = done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("cancelling setup should not wait for shell descendants");
        assert_eq!(result, Err(SetupError::Cancelled));
    }
}
