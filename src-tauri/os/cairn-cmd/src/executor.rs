//! First-class remote executor lifecycle commands.
//!
//! The CLI verifies key-based SSH authentication and can help an interactive
//! operator establish it. The runner remains the sole owner of enrollment,
//! installation, supervision, and teardown.

use crate::invoke_client::InvokeClient;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

#[derive(clap::Subcommand, Clone, Debug)]
pub(crate) enum ExecutorCommand {
    /// Enroll and attach an SSH-reachable executor.
    Add {
        /// SSH target in user@host form.
        target: String,
        #[arg(long)]
        binary_path: Option<String>,
        #[arg(long)]
        remote_home: Option<String>,
        #[arg(long)]
        executor_id: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long)]
        display_name: Option<String>,
        /// Restrict this executor to a project key. Repeat for multiple projects.
        #[arg(long = "project")]
        projects: Vec<String>,
        /// Extra argument passed to ssh. Repeat once per argument.
        #[arg(long = "ssh-arg", allow_hyphen_values = true)]
        extra_ssh_args: Vec<String>,
        /// What the machine is for. Omitted means general for a new machine
        /// and the role it already holds for one enrolled again.
        #[arg(long, value_parser = ["buildsOnly", "general", "inferenceOnly"])]
        role: Option<String>,
    },
    /// Tear down an executor and revoke its enrollment, by public name.
    Remove {
        name: String,
        /// Remove even though the machine still counts work on it. What the
        /// counts describe is stranded; the lanes behind them re-arm elsewhere.
        /// This is the way past counts a machine that has stopped answering
        /// will never clear on its own.
        #[arg(long)]
        confirm: bool,
    },
    /// Give an executor a different public name.
    ///
    /// The name is the address every placement request is written in, so this
    /// moves the configuration, the enrollment claim, and the running executor's
    /// own advertisement together.
    Rename { name: String, new_name: String },
    /// List configured remote executors and their live fleet status.
    List,
}

#[derive(Debug, PartialEq)]
enum SshProbeFailure {
    Authentication,
    WrongUser,
    HostKey,
    Unreachable,
    Other,
}

fn ssh_target(request: &AddRequest) -> String {
    format!("{}@{}", request.ssh_user, request.host)
}

fn ssh_copy_id_args(extra_ssh_args: &[String]) -> Vec<String> {
    let mut copy_args = Vec::new();
    let mut args = extra_ssh_args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-J" => {
                copy_args.push("-o".into());
                if let Some(jump_host) = args.next() {
                    copy_args.push(format!("ProxyJump={jump_host}"));
                }
            }
            "-4" => copy_args.extend(["-o".into(), "AddressFamily=inet".into()]),
            "-6" => copy_args.extend(["-o".into(), "AddressFamily=inet6".into()]),
            _ if arg.starts_with("-J") => {
                copy_args.extend(["-o".into(), format!("ProxyJump={}", &arg[2..])]);
            }
            _ => copy_args.push(arg.clone()),
        }
    }

    copy_args
}

fn shell_quote(argument: &str) -> String {
    if argument
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || "@%_+=:,./-".contains(character))
    {
        argument.to_owned()
    } else {
        format!("'{}'", argument.replace('\'', "'\\''"))
    }
}

fn ssh_copy_id_command(request: &AddRequest) -> String {
    std::iter::once("ssh-copy-id".to_string())
        .chain(
            ssh_copy_id_args(&request.extra_ssh_args)
                .into_iter()
                .map(|arg| shell_quote(&arg)),
        )
        .chain(std::iter::once(shell_quote(&ssh_target(request))))
        .collect::<Vec<_>>()
        .join(" ")
}

fn ssh_copy_id_guidance(request: &AddRequest) -> String {
    let target = ssh_target(request);
    let copy_command = ssh_copy_id_command(request);
    format!(
        "SSH key authentication is not ready for {target}. Run `{copy_command}`, then re-run `cairn executor add {target}`. Verify the SSH username if the copy command rejects it."
    )
}

fn classify_ssh_probe(output: &Output) -> SshProbeFailure {
    let diagnostic = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    )
    .to_ascii_lowercase();

    if diagnostic.contains("remote host identification has changed")
        || diagnostic.contains("host key verification failed")
        || diagnostic.contains("host key mismatch")
    {
        SshProbeFailure::HostKey
    } else if diagnostic.contains("invalid user")
        || diagnostic.contains("illegal user")
        || diagnostic.contains("unknown user")
    {
        SshProbeFailure::WrongUser
    } else if diagnostic.contains("connection timed out")
        || diagnostic.contains("operation timed out")
        || diagnostic.contains("connection refused")
        || diagnostic.contains("no route to host")
        || diagnostic.contains("network is unreachable")
        || diagnostic.contains("could not resolve hostname")
        || diagnostic.contains("name or service not known")
    {
        SshProbeFailure::Unreachable
    } else if diagnostic.contains("permission denied")
        || diagnostic.contains("authentication failed")
        || diagnostic.contains("no supported authentication methods")
    {
        SshProbeFailure::Authentication
    } else {
        SshProbeFailure::Other
    }
}

/// Read an authentication probe's outcome from ssh's exit status.
///
/// ssh exits 255 for its own failures (connect, host key, authentication) and
/// otherwise passes through the remote command's status. Any other status means
/// a shell ran as the user, which is all the probe asks — whether that shell
/// accepted the probe word is irrelevant, and a Windows host whose OpenSSH
/// default shell is cmd.exe rejects `true` with status 1.
fn ssh_probe_verdict(output: &Output) -> Result<(), SshProbeFailure> {
    match output.status.code() {
        Some(255) | None => Err(classify_ssh_probe(output)),
        Some(_) => Ok(()),
    }
}

fn run_ssh_probe(request: &AddRequest) -> Result<(), (SshProbeFailure, String)> {
    let target = ssh_target(request);
    let output = Command::new("ssh")
        .args(["-o", "BatchMode=yes", "-o", "ConnectTimeout=5"])
        .args(&request.extra_ssh_args)
        .arg("--")
        .arg(&target)
        .arg("true")
        .output()
        .map_err(|error| {
            (
                SshProbeFailure::Other,
                format!("could not run the SSH authentication probe: {error}"),
            )
        })?;
    ssh_probe_verdict(&output).map_err(|failure| {
        let diagnostic = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        (failure, diagnostic)
    })
}

fn local_public_key_exists() -> bool {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from);
    let Some(ssh_dir) = home.map(|home| home.join(".ssh")) else {
        return false;
    };
    std::fs::read_dir(ssh_dir).is_ok_and(|entries| {
        entries.flatten().any(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with("id_") && name.ends_with(".pub") && entry.path().is_file()
        })
    })
}

fn confirm(prompt: &str) -> Result<bool, String> {
    eprint!("{prompt} [Y/n] ");
    io::stderr().flush().map_err(|error| error.to_string())?;
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .map_err(|error| error.to_string())?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "" | "y" | "yes"
    ))
}

fn prepare_ssh_auth(request: &AddRequest) -> Result<(), String> {
    let Err((failure, diagnostic)) = run_ssh_probe(request) else {
        return Ok(());
    };
    let target = ssh_target(request);
    match failure {
        SshProbeFailure::HostKey => Err(format!(
            "SSH host-key verification failed for {target}. Resolve the host-key mismatch before enrollment. {diagnostic}"
        )),
        SshProbeFailure::WrongUser => Err(format!(
            "SSH reports that `{}` is not a valid user on {}. Correct the user in the enrollment target. {diagnostic}",
            request.ssh_user, request.host
        )),
        SshProbeFailure::Unreachable => Err(format!(
            "SSH could not reach {} within 5 seconds. Check its address, network, and SSH service. {diagnostic}",
            request.host
        )),
        SshProbeFailure::Other => Err(format!(
            "SSH probe failed for {target}. {diagnostic}"
        )),
        SshProbeFailure::Authentication => {
            if !(io::stdin().is_terminal() && io::stderr().is_terminal()) {
                return Err(ssh_copy_id_guidance(request));
            }

            eprintln!(
                "SSH reached {}, but key authentication for `{}` was rejected. If that account does not exist on the host, re-run this command with the correct username.",
                request.host, request.ssh_user
            );
            if !local_public_key_exists() {
                if !confirm("No local SSH public key was found. Generate an Ed25519 key now?")? {
                    return Err(format!(
                        "No SSH key was generated. Create one with `ssh-keygen -t ed25519`, then {}",
                        ssh_copy_id_guidance(request)
                    ));
                }
                let status = Command::new("ssh-keygen")
                    .args(["-t", "ed25519", "-N", ""])
                    .stdin(Stdio::inherit())
                    .stdout(Stdio::inherit())
                    .stderr(Stdio::inherit())
                    .status()
                    .map_err(|error| format!("could not run ssh-keygen: {error}"))?;
                if !status.success() {
                    return Err(format!("ssh-keygen exited with {status}"));
                }
            }

            let copy_args = ssh_copy_id_args(&request.extra_ssh_args);
            let copy_command = ssh_copy_id_command(request);
            eprintln!("Running `{copy_command}`…");
            let status = Command::new("ssh-copy-id")
                .args(&copy_args)
                .arg("--")
                .arg(&target)
                .stdin(Stdio::inherit())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .status()
                .map_err(|error| format!("could not run `{copy_command}`: {error}"))?;
            if !status.success() {
                return Err(format!(
                    "`{copy_command}` exited with {status}. Verify that `{}` is the correct remote user, then retry.",
                    request.ssh_user
                ));
            }
            run_ssh_probe(request).map_err(|(failure, diagnostic)| {
                format!(
                    "SSH authentication still failed after `{copy_command}` ({failure:?}). Verify the username and SSH options. {diagnostic}"
                )
            })
        }
    }
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct AddRequest {
    host: String,
    ssh_user: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    binary_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cairn_home: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    executor_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    project_keys: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    extra_ssh_args: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MutationResult {
    display_name: String,
    os: Option<String>,
    arch: Option<String>,
    attach_state: String,
    /// Present when a removal completed without reaching the host. Absent means
    /// nothing was left behind.
    unverified_remote_cleanup: Option<String>,
}

/// What starting an enrollment answers with. The SSH bootstrap behind it takes
/// minutes, so the runner hands back the operation to watch rather than holding
/// the request open.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EnrollmentStarted {
    operation_id: String,
    name: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct EnrollmentOperation {
    id: String,
    name: String,
    uri: String,
    phase: String,
    #[serde(default)]
    diagnostic: Option<String>,
    cleanup: String,
}

impl EnrollmentOperation {
    fn is_terminal(&self) -> bool {
        matches!(
            self.phase.as_str(),
            "ready" | "failed" | "retryRemoveRequired"
        )
    }
}

/// Follow an enrollment to its end, printing each phase the runner actually
/// reaches. This is the same operation record the app renders; the CLI is one
/// more reader of it, not a second implementation of waiting.
async fn follow_enrollment(
    client: &InvokeClient,
    started: &EnrollmentStarted,
) -> Result<String, String> {
    let mut last_phase = String::new();
    loop {
        let operations: Vec<EnrollmentOperation> = client
            .invoke("executor_enrollment_operations", json!({}))
            .await?;
        let Some(operation) = operations
            .into_iter()
            .find(|operation| operation.id == started.operation_id)
        else {
            return Err(format!(
                "the runner stopped reporting enrollment {} for {}",
                started.operation_id, started.name
            ));
        };
        if operation.phase != last_phase {
            eprintln!("{}: {}", operation.name, phase_label(&operation.phase));
            last_phase = operation.phase.clone();
        }
        if operation.is_terminal() {
            return match operation.phase.as_str() {
                "ready" => Ok(format!(
                    "Enrolled {}: ready ({})",
                    operation.name, operation.uri
                )),
                _ => Err(format!(
                    "{}{}",
                    operation
                        .diagnostic
                        .unwrap_or_else(|| "enrollment failed".into()),
                    if operation.cleanup == "incomplete" {
                        format!(
                            " Run `cairn executor remove {}` to clear what the rollback could not.",
                            operation.name
                        )
                    } else {
                        String::new()
                    }
                )),
            };
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// The runner's phase in the words it uses itself (`EnrollmentPhase::label`
/// in cairn-core, which this crate does not depend on). A phase this table
/// does not know is printed as it came: a newer runner's step is not a
/// failure.
fn phase_label(phase: &str) -> &str {
    match phase {
        "queued" => "waiting for another enrollment of this machine to finish",
        "awaitingRunnerListener" => "waiting for this runner's executor listener",
        "validating" => "validating the request",
        "probingHost" => "probing the host and its platform",
        "resolvingArtifact" => "resolving an executor build for it",
        "installingBinary" => "installing the executor binary",
        "persistingConfiguration" => "persisting the enrollment configuration",
        "grantingEnrollment" => "granting the enrollment",
        "verifyingReach" => "verifying the machine can reach this runner",
        "registeringService" => "asking the machine to register its executor service",
        "awaitingFirstDial" => "waiting for the machine to dial in",
        "ready" => "ready",
        "cleaningUp" => "rolling back after a failure",
        "failed" => "failed",
        "retryRemoveRequired" => "failed; rollback incomplete",
        other => other,
    }
}

/// Only a runner reached through the default local callback shares this shell's
/// SSH keys and agent; one reached through an explicit callback (an `ssh -L`
/// forward, an agent shell) may live on another machine entirely.
fn shares_callers_ssh_environment(explicit_callback: Option<&str>) -> bool {
    explicit_callback.is_none()
}

fn parse_target(target: &str) -> Result<(String, String), String> {
    let (user, host) = target
        .split_once('@')
        .ok_or_else(|| "target must be user@host".to_string())?;
    if user.is_empty()
        || host.is_empty()
        || host.contains('@')
        || user.starts_with('-')
        || host.starts_with('-')
    {
        return Err("target must be user@host".into());
    }
    Ok((user.to_owned(), host.to_owned()))
}

fn add_request(command: ExecutorCommand) -> Result<AddRequest, String> {
    let ExecutorCommand::Add {
        target,
        binary_path,
        remote_home,
        executor_id,
        device_id,
        display_name,
        projects,
        extra_ssh_args,
        role,
    } = command
    else {
        unreachable!()
    };
    let (ssh_user, host) = parse_target(&target)?;
    Ok(AddRequest {
        host,
        ssh_user,
        binary_path,
        cairn_home: remote_home,
        executor_id,
        device_id,
        display_name,
        project_keys: projects,
        extra_ssh_args,
        role,
    })
}

pub(crate) async fn run(command: ExecutorCommand) -> bool {
    let client = InvokeClient::from_environment();
    let result = match command {
        command @ ExecutorCommand::Add { .. } => {
            let request = match add_request(command) {
                Ok(request) => request,
                Err(error) => return emit_error("add", &error),
            };
            if shares_callers_ssh_environment(std::env::var("CAIRN_CALLBACK_URL").ok().as_deref()) {
                if let Err(error) = prepare_ssh_auth(&request) {
                    return emit_error("add", &error);
                }
            }
            match client
                .invoke::<EnrollmentStarted>(
                    "add_remote_executor",
                    serde_json::to_value(request).unwrap(),
                )
                .await
            {
                Ok(started) => follow_enrollment(&client, &started)
                    .await
                    .map_err(|error| ("add", error)),
                Err(error) => Err(("add", error)),
            }
        }
        ExecutorCommand::Remove { name, confirm } => {
            eprintln!("Stopping remote executor, verifying cleanup, and revoking enrollment…");
            client
                .invoke::<MutationResult>(
                    "remove_remote_executor",
                    json!({ "name": name, "confirm": confirm }),
                )
                .await
                .map(|result| format_mutation("Removed", &result))
                .map_err(|error| ("remove", error))
        }
        ExecutorCommand::Rename { name, new_name } => {
            eprintln!("Moving the executor's public name and restarting supervision…");
            client
                .invoke::<MutationResult>(
                    "rename_remote_executor",
                    json!({ "name": name, "newName": new_name }),
                )
                .await
                .map(|result| format_mutation("Renamed", &result))
                .map_err(|error| ("rename", error))
        }
        ExecutorCommand::List => list(&client).await.map_err(|error| ("list", error)),
    };
    match result {
        Ok(output) => {
            println!("{output}");
            true
        }
        Err((verb, error)) => emit_error(verb, &error),
    }
}

fn emit_error(verb: &str, error: &str) -> bool {
    eprintln!("cairn executor {verb}: {error}");
    false
}

fn format_mutation(action: &str, result: &MutationResult) -> String {
    let platform = match (&result.os, &result.arch) {
        (Some(os), Some(arch)) => format!(" ({os}/{arch})"),
        _ => String::new(),
    };
    let unreached = match &result.unverified_remote_cleanup {
        Some(reason) => format!(
            "\nIts host could not be reached ({reason}), so no remote cleanup ran there. The enrollment is revoked, so anything still running on it cannot reattach."
        ),
        None => String::new(),
    };
    format!(
        "{action} {}: {}{platform}{unreached}",
        result.display_name, result.attach_state
    )
}

async fn list(client: &InvokeClient) -> Result<String, String> {
    let config: Value = client.invoke("get_build_slots_config", json!({})).await?;
    let health: Value = client.invoke("get_substrate_health", json!({})).await?;
    Ok(format_list(&config, &health))
}

fn format_list(config: &Value, health: &Value) -> String {
    let Some(remotes) = config.get("remoteExecutors").and_then(Value::as_object) else {
        return "No remote executors configured.".into();
    };
    if remotes.is_empty() {
        return "No remote executors configured.".into();
    }
    let live = health
        .get("executors")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut rows = Vec::new();
    for remote in remotes.values() {
        let id = remote
            .get("executorId")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let name = remote
            .get("displayName")
            .and_then(Value::as_str)
            .unwrap_or(id);
        let target = format!(
            "{}@{}",
            remote.get("sshUser").and_then(Value::as_str).unwrap_or("?"),
            remote.get("host").and_then(Value::as_str).unwrap_or("?")
        );
        let attached = live.iter().find(|entry| {
            entry
                .pointer("/identity/executorId")
                .and_then(Value::as_str)
                == Some(id)
        });
        let status = attached
            .and_then(|entry| entry.get("status"))
            .and_then(Value::as_str)
            .unwrap_or("offline");
        let os = attached
            .and_then(|entry| entry.pointer("/advertisement/capabilities/os"))
            .and_then(Value::as_str)
            .unwrap_or("-");
        let arch = attached
            .and_then(|entry| entry.pointer("/advertisement/capabilities/arch"))
            .and_then(Value::as_str)
            .unwrap_or("-");
        rows.push(format!("{name}\t{target}\t{status}\t{os}/{arch}"));
    }
    rows.sort();
    format!("NAME\tSSH TARGET\tSTATUS\tPLATFORM\n{}", rows.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_target_and_builds_minimal_unrestricted_request() {
        let request = add_request(ExecutorCommand::Add {
            target: "dev@builder.local".into(),
            binary_path: None,
            remote_home: None,
            executor_id: None,
            device_id: None,
            display_name: None,
            projects: vec![],
            extra_ssh_args: vec![],
            role: None,
        })
        .unwrap();
        assert_eq!(request.host, "builder.local");
        assert_eq!(request.ssh_user, "dev");
        assert!(request.project_keys.is_empty());
        let json = serde_json::to_value(request).unwrap();
        assert!(json.get("binaryPath").is_none());
        assert!(
            json.get("role").is_none(),
            "an omitted role is left to the runner"
        );
    }

    #[test]
    fn role_is_parsed_with_its_three_spellings_and_sent_as_given() {
        use clap::Parser;
        #[derive(clap::Parser)]
        struct Cli {
            #[command(subcommand)]
            command: ExecutorCommand,
        }
        let cli = Cli::try_parse_from(["cairn", "add", "dev@sp1.local", "--role", "inferenceOnly"])
            .unwrap();
        let request = add_request(cli.command).unwrap();
        assert_eq!(
            serde_json::to_value(request).unwrap().get("role"),
            Some(&json!("inferenceOnly"))
        );
        let refused = Cli::try_parse_from(["cairn", "add", "dev@sp1.local", "--role", "gpu"])
            .err()
            .expect("an unknown role is refused")
            .to_string();
        assert!(
            refused.contains("buildsOnly") && refused.contains("inferenceOnly"),
            "{refused}"
        );
    }

    #[test]
    fn preserves_spaced_ssh_username_as_one_target_component() {
        let request = add_request(ExecutorCommand::Add {
            target: "dell workstation@192.168.1.18".into(),
            binary_path: None,
            remote_home: None,
            executor_id: None,
            device_id: None,
            display_name: None,
            projects: vec![],
            extra_ssh_args: vec!["-4".into()],
            role: None,
        })
        .unwrap();
        assert_eq!(request.ssh_user, "dell workstation");
        assert_eq!(request.host, "192.168.1.18");
    }

    #[test]
    fn mutation_confirmation_names_the_machine_by_its_public_name() {
        let result = MutationResult {
            display_name: "bglab-win".into(),
            os: Some("windows".into()),
            arch: Some("x86_64".into()),
            attach_state: "ready".into(),
            unverified_remote_cleanup: None,
        };

        assert_eq!(
            format_mutation("Added", &result),
            "Added bglab-win: ready (windows/x86_64)"
        );
    }

    /// A removal that never reached the host has to say so. The operator's next
    /// move depends on it: the enrollment is revoked either way, but only in
    /// this case is there possibly something still running on a machine Cairn
    /// can no longer talk to.
    #[test]
    fn a_removal_that_never_reached_the_host_says_what_was_left_behind() {
        let result = MutationResult {
            display_name: "bglab-win".into(),
            os: None,
            arch: None,
            attach_state: "removed".into(),
            unverified_remote_cleanup: Some("ssh timed out".into()),
        };

        let confirmation = format_mutation("Removed", &result);

        assert!(
            confirmation.starts_with("Removed bglab-win: removed"),
            "{confirmation}"
        );
        assert!(confirmation.contains("ssh timed out"), "{confirmation}");
        assert!(confirmation.contains("cannot reattach"), "{confirmation}");
    }

    fn ssh_output(code: u8, stderr: &str) -> Output {
        #[cfg(unix)]
        use std::os::unix::process::ExitStatusExt;
        #[cfg(windows)]
        use std::os::windows::process::ExitStatusExt;
        // A Unix raw wait status carries the exit code in its second byte.
        #[cfg(unix)]
        let raw = i32::from(code) << 8;
        #[cfg(windows)]
        let raw = u32::from(code);
        Output {
            status: std::process::ExitStatus::from_raw(raw),
            stdout: vec![],
            stderr: stderr.as_bytes().to_vec(),
        }
    }

    fn failed_ssh_output(stderr: &str) -> Output {
        ssh_output(255, stderr)
    }

    #[test]
    fn probe_passes_when_the_remote_shell_rejects_the_probe_word() {
        let cmd_exe = ssh_output(
            1,
            "'true' is not recognized as an internal or external command,\r\noperable program or batch file.",
        );
        assert_eq!(ssh_probe_verdict(&cmd_exe), Ok(()));
        assert_eq!(ssh_probe_verdict(&ssh_output(0, "")), Ok(()));
    }

    #[test]
    fn probe_fails_only_on_ssh_own_exit_status() {
        assert_eq!(
            ssh_probe_verdict(&failed_ssh_output("Permission denied (publickey).")),
            Err(SshProbeFailure::Authentication)
        );
        assert_eq!(
            ssh_probe_verdict(&failed_ssh_output("Host key verification failed.")),
            Err(SshProbeFailure::HostKey)
        );
        assert_eq!(
            ssh_probe_verdict(&failed_ssh_output(
                "ssh: connect to host bgsrv-win port 22: Connection refused"
            )),
            Err(SshProbeFailure::Unreachable)
        );
    }

    #[test]
    fn ssh_probe_failures_keep_actionable_stages_distinct() {
        assert_eq!(
            classify_ssh_probe(&failed_ssh_output("Connection timed out")),
            SshProbeFailure::Unreachable
        );
        assert_eq!(
            classify_ssh_probe(&failed_ssh_output(
                "REMOTE HOST IDENTIFICATION HAS CHANGED!"
            )),
            SshProbeFailure::HostKey
        );
        assert_eq!(
            classify_ssh_probe(&failed_ssh_output("Invalid user deploy")),
            SshProbeFailure::WrongUser
        );
        assert_eq!(
            classify_ssh_probe(&failed_ssh_output("Permission denied (publickey).")),
            SshProbeFailure::Authentication
        );
    }

    #[test]
    fn non_interactive_guidance_names_exact_copy_and_retry_commands() {
        let request = add_request(ExecutorCommand::Add {
            target: "dev@builder.local".into(),
            binary_path: None,
            remote_home: None,
            executor_id: None,
            device_id: None,
            display_name: None,
            projects: vec![],
            extra_ssh_args: vec![],
            role: None,
        })
        .unwrap();
        assert_eq!(ssh_copy_id_guidance(&request), "SSH key authentication is not ready for dev@builder.local. Run `ssh-copy-id dev@builder.local`, then re-run `cairn executor add dev@builder.local`. Verify the SSH username if the copy command rejects it.");
    }

    #[test]
    fn copy_id_uses_the_same_custom_ssh_endpoint() {
        let request = add_request(ExecutorCommand::Add {
            target: "dev@builder.local".into(),
            binary_path: None,
            remote_home: None,
            executor_id: None,
            device_id: None,
            display_name: None,
            projects: vec![],
            extra_ssh_args: vec!["-p".into(), "2222".into(), "-J".into(), "jump host".into()],
            role: None,
        })
        .unwrap();
        assert_eq!(
            ssh_copy_id_command(&request),
            "ssh-copy-id -p 2222 -o 'ProxyJump=jump host' dev@builder.local"
        );
    }

    #[test]
    fn rejects_targets_that_ssh_could_parse_as_options() {
        assert_eq!(
            parse_target("-oProxyCommand=payload@host"),
            Err("target must be user@host".into())
        );
        assert_eq!(
            parse_target("dev@-oProxyCommand=payload"),
            Err("target must be user@host".into())
        );
    }

    #[test]
    fn explicit_callbacks_do_not_claim_the_callers_ssh_environment() {
        assert!(shares_callers_ssh_environment(None));
        assert!(!shares_callers_ssh_environment(Some(
            "http://127.0.0.1:43850/api/mcp"
        )));
    }

    #[test]
    fn list_combines_configuration_with_live_health() {
        let config = json!({"remoteExecutors":{"linux":{"executorId":"linux","displayName":"bglab-ub","sshUser":"dev","host":"builder"}}});
        let health = json!({"executors":[{"identity":{"executorId":"linux"},"status":"online","advertisement":{"capabilities":{"os":"linux","arch":"x86_64"}}}]});
        let output = format_list(&config, &health);
        assert!(output.contains("bglab-ub\tdev@builder\tonline\tlinux/x86_64"));
    }
}
