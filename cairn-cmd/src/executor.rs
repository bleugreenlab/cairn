//! First-class remote executor lifecycle commands.
//!
//! This module deliberately contains no SSH or enrollment behavior. It is a
//! typed client for the runner's invoke surface, which remains the sole owner of
//! preflight, credentials, supervision, and teardown.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

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
        #[arg(long)]
        tunnel_port: Option<u16>,
        /// Extra argument passed to ssh. Repeat once per argument.
        #[arg(long = "ssh-arg", allow_hyphen_values = true)]
        extra_ssh_args: Vec<String>,
    },
    /// Tear down an executor and revoke its enrollment, by public name.
    Remove { name: String },
    /// Give an executor a different public name.
    ///
    /// The name is the address every placement request is written in, so this
    /// moves the configuration, the enrollment claim, and the running executor's
    /// own advertisement together.
    Rename { name: String, new_name: String },
    /// List configured remote executors and their live fleet status.
    List,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    tunnel_port: Option<u16>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    extra_ssh_args: Vec<String>,
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

fn phase_label(phase: &str) -> &str {
    match phase {
        "validating" => "validating the request",
        "probingHost" => "probing the host and its platform",
        "resolvingArtifact" => "resolving an executor build for it",
        "installingBinary" => "installing the executor binary",
        "persistingConfiguration" => "persisting the enrollment configuration",
        "grantingEnrollment" => "granting the enrollment",
        "startingSupervision" => "starting supervision",
        "awaitingReady" => "waiting for the executor to report Ready",
        "ready" => "ready",
        "cleaningUp" => "rolling back after a failure",
        "retryRemoveRequired" => "failed; rollback incomplete",
        _ => "failed",
    }
}

struct InvokeClient {
    base_url: String,
    token: Option<String>,
    http: reqwest::Client,
}

impl InvokeClient {
    fn from_environment() -> Self {
        let callback = std::env::var("CAIRN_CALLBACK_URL")
            .unwrap_or_else(|_| crate::cli::default_callback_url());
        let mut parsed = url::Url::parse(&callback).expect("callback URL is valid");
        parsed.set_path("");
        parsed.set_query(None);
        parsed.set_fragment(None);
        Self {
            base_url: parsed.as_str().trim_end_matches('/').to_string(),
            token: std::env::var("CAIRN_MCP_SECRET")
                .ok()
                .or_else(cairn_common::auth::load_local_mcp_token),
            http: reqwest::Client::new(),
        }
    }

    async fn invoke<T: DeserializeOwned>(&self, command: &str, args: Value) -> Result<T, String> {
        let mut request = self
            .http
            .post(format!("{}/api/invoke", self.base_url))
            .json(&json!({ "command": command, "args": args }));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.map_err(|error| error.to_string())?;
        let status = response.status();
        let body = response.text().await.map_err(|error| error.to_string())?;
        if !status.is_success() {
            return Err(invoke_error_text(&body));
        }
        serde_json::from_str(&body).map_err(|error| format!("invalid runner response: {error}"))
    }
}

fn invoke_error_text(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| value.get("error")?.as_str().map(str::to_owned))
        .unwrap_or_else(|| body.trim().to_owned())
}

fn parse_target(target: &str) -> Result<(String, String), String> {
    let (user, host) = target
        .split_once('@')
        .ok_or_else(|| "target must be user@host".to_string())?;
    if user.is_empty() || host.is_empty() || host.contains('@') {
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
        tunnel_port,
        extra_ssh_args,
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
        tunnel_port,
        extra_ssh_args,
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
        ExecutorCommand::Remove { name } => {
            eprintln!("Stopping remote executor, verifying cleanup, and revoking enrollment…");
            client
                .invoke::<MutationResult>("remove_remote_executor", json!({ "name": name }))
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
            tunnel_port: None,
            extra_ssh_args: vec![],
        })
        .unwrap();
        assert_eq!(request.host, "builder.local");
        assert_eq!(request.ssh_user, "dev");
        assert!(request.project_keys.is_empty());
        assert!(request.tunnel_port.is_none());
        let json = serde_json::to_value(request).unwrap();
        assert!(json.get("binaryPath").is_none());
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
            tunnel_port: None,
            extra_ssh_args: vec!["-4".into()],
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

    #[test]
    fn preserves_stage_specific_invoke_error_verbatim() {
        assert_eq!(
            invoke_error_text(
                r#"{"error":"remote prerequisite preflight failed: binary missing"}"#
            ),
            "remote prerequisite preflight failed: binary missing"
        );
    }

    #[test]
    fn list_combines_configuration_with_live_health() {
        let config = json!({"remoteExecutors":{"linux":{"executorId":"linux","displayName":"bglab-ub","sshUser":"dev","host":"builder"}}});
        let health = json!({"executors":[{"identity":{"executorId":"linux"},"status":"online","advertisement":{"capabilities":{"os":"linux","arch":"x86_64"}}}]});
        let output = format_list(&config, &health);
        assert!(output.contains("bglab-ub\tdev@builder\tonline\tlinux/x86_64"));
    }
}
