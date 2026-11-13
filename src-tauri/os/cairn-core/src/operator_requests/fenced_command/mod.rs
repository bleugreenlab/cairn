//! `fenced_command`: the operator accepts one exact command, and Cairn runs it
//! once on one executor, outside the requesting agent's path fence but under
//! the operator-credential floor.
//!
//! Everything the card shows and the act binds is fixed by Cairn, never taken
//! from the agent: the commit is the filing run's branch head, the executor's
//! stable and device identities come from the live registry, and the working
//! directory is resolved inside the checkout. The act is offered only on an
//! executor whose measured `OperatorCredentialRead` edge is `Refused` and
//! current, and the acceptance binds that exact measurement and connection,
//! because containment is measured at executor start and replayed rather than
//! re-probed.

use cairn_common::executor_protocol::{
    EdgeDisposition, EdgeId, EdgeRequirement, ExecutorHealthStatus, ExecutorInspection,
    MaximumDisposition, NodeScratchGrant, ProcessClass, WorkloadEnvelope,
};
use cairn_common::protocol::CallbackRequest;
use cairn_common::requests::{
    ExecutorFloorWitness, FencedCommandAcceptance, FencedCommandAct, FencedCommandTarget,
};
use cairn_common::run_contract::RUN_BATCH_CEILING_MS;
use serde_json::{json, Value};

use super::registry::ActorFacts;
use crate::orchestrator::Orchestrator;

pub(crate) mod consumer;
pub(crate) mod offer;
pub(crate) use offer::offer_for;

pub(crate) const KIND: &str = "fenced_command";
pub(crate) const ACTOR: &str = "execution.fenced_command";

/// The one value the fleet's operator-act path accepts. Its fields are private
/// to this module, so only the consumer of an accepted, rechecked request can
/// build one: an agent's `run` verb cannot reach an unfenced spawn by type.
pub(crate) struct OperatorAct {
    act: FencedCommandAct,
    /// The project's repository, as a run batch's `ColocatedPath` names it.
    repository: String,
    cell_request_id: String,
    cell_attempt_id: String,
    node_scratch: Option<NodeScratchGrant>,
}

impl OperatorAct {
    pub(crate) fn act(&self) -> &FencedCommandAct {
        &self.act
    }

    pub(crate) fn repository(&self) -> &str {
        &self.repository
    }

    pub(crate) fn cell_request_id(&self) -> &str {
        &self.cell_request_id
    }

    pub(crate) fn cell_attempt_id(&self) -> &str {
        &self.cell_attempt_id
    }

    pub(crate) fn node_scratch(&self) -> Option<&NodeScratchGrant> {
        self.node_scratch.as_ref()
    }
}

/// The card shows the command in full, so it is bounded where a person can
/// still read every byte of it.
const MAX_COMMAND_BYTES: usize = 16 * 1024;

/// What the operator is told the act can reach, rendered beside the command.
pub(crate) fn consequence(executor: &str) -> String {
    format!(
        "Runs once on {executor} with your account's access except Cairn's credentials. \
         The code at this commit was written by an agent, and so are any files under the \
         requesting agent's $TMPDIR, which are not shown here. Network is not restricted."
    )
}

/// The containment a `fenced_command` executor must prove: an agent process
/// there is measured unable to read Cairn's operator credential.
pub(crate) fn credential_floor_envelope() -> WorkloadEnvelope {
    WorkloadEnvelope {
        process_class: ProcessClass::BatchSpawn,
        edges: vec![EdgeRequirement {
            edge: EdgeId::OperatorCredentialRead,
            maximum_disposition: MaximumDisposition::Refused,
        }],
    }
}

/// One executor as the act binds it, read from the live registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutorFloor {
    pub executor_id: String,
    pub device_id: String,
    pub name: String,
    pub os: String,
    pub online: bool,
    pub witness: ExecutorFloorWitness,
}

/// Why an executor cannot take the act, phrased for the agent and the card.
fn floor_refusal(name: &str, os: &str, disposition: Option<&EdgeDisposition>) -> String {
    let measured = match disposition {
        None => "not measured".to_string(),
        Some(EdgeDisposition::Ambient { .. }) => {
            "ambient: an agent process there can read it".into()
        }
        Some(EdgeDisposition::Constrained { .. }) => "constrained, not refused".into(),
        Some(EdgeDisposition::Unmeasured { reason }) => format!("unmeasured ({reason})"),
        Some(EdgeDisposition::Refused { .. }) => {
            "refused, but the measurement is not current for this executor's build, boot or inputs"
                .into()
        }
    };
    format!(
        "fenced_command is unavailable on {name} ({os}): its operator-credential floor is {measured}. \
         The act runs outside the path fence and is offered only where an agent process is measured \
         unable to read Cairn's credentials, which today means macOS executors. For a path this work \
         needs every time, ask for a declared writable path (cairn/7081) instead."
    )
}

/// Read one executor's identity and credential floor from an inspection.
pub(crate) fn floor_of(inspection: &ExecutorInspection) -> Result<ExecutorFloor, String> {
    let health = &inspection.health;
    let capabilities = &health.advertisement.capabilities;
    let name = inspection.name.clone();
    let os = capabilities.os.clone();
    let containment = capabilities
        .containment
        .as_ref()
        .ok_or_else(|| floor_refusal(&name, &os, None))?;
    let observation = containment
        .observations
        .iter()
        .find(|observation| observation.edge == EdgeId::OperatorCredentialRead);
    let disposition = observation.map(|observation| &observation.disposition);
    containment
        .satisfies(&credential_floor_envelope())
        .map_err(|_| floor_refusal(&name, &os, disposition))?;
    let observation = observation.ok_or_else(|| floor_refusal(&name, &os, None))?;
    let scope = observation
        .disposition
        .positive_scope()
        .ok_or_else(|| floor_refusal(&name, &os, disposition))?;
    Ok(ExecutorFloor {
        executor_id: health.identity.executor_id.clone(),
        device_id: health.identity.device_id.clone(),
        name,
        os,
        online: health.status == ExecutorHealthStatus::Online,
        witness: ExecutorFloorWitness {
            connection_generation: health.connection_generation,
            executor_build_id: containment.executor_build_id.clone(),
            boot_generation: containment.boot_generation.clone(),
            floor_fingerprint: scope.invalidation_fingerprint.digest.clone(),
            floor_measured_at_unix_ms: observation.observed_at_unix_ms,
        },
    })
}

/// The live floor of the executor with this stable id. An executor that is
/// not connected has no current measurement, so it cannot take the act.
pub(crate) fn executor_floor(
    orch: &Orchestrator,
    executor_id: &str,
) -> Result<ExecutorFloor, String> {
    let inspection = orch
        .fleet
        .inspect_executors(crate::fleet::unix_time_ms())
        .into_iter()
        .find(|inspection| inspection.health.identity.executor_id == executor_id)
        .ok_or_else(|| {
            format!(
                "fenced_command is unavailable: executor {} is not connected, so its credential floor has no current measurement",
                crate::fleet::management::public_name(orch, executor_id)
            )
        })?;
    floor_of(&inspection)
}

/// Normalize a repository-relative directory, refusing anything that could
/// leave the checkout. The repository root is the empty string.
pub(crate) fn normalize_cwd(cwd: Option<&str>) -> Result<String, String> {
    let Some(raw) = cwd.map(str::trim).filter(|cwd| !cwd.is_empty()) else {
        return Ok(String::new());
    };
    let outside = || {
        format!("fenced_command cwd `{raw}` must be a directory inside the checkout, relative to the repository root")
    };
    if raw.starts_with('/') || raw.starts_with('\\') || raw.contains('\0') || raw.contains(':') {
        return Err(outside());
    }
    let mut parts = Vec::new();
    for part in raw.split(['/', '\\']) {
        match part {
            "" | "." => {}
            ".." => return Err(outside()),
            part => parts.push(part),
        }
    }
    Ok(parts.join("/"))
}

/// Parse and validate what the agent asked for. Pure: no registry or branch
/// state is consulted here.
pub(crate) fn parse_target(raw: Option<&Value>) -> Result<FencedCommandTarget, String> {
    let shape = "fenced_command requires target {executor, command, cwd?, timeoutMs?}";
    let raw = raw.ok_or(shape)?;
    let target: FencedCommandTarget =
        serde_json::from_value(raw.clone()).map_err(|error| format!("{shape}: {error}"))?;
    if target.command.trim().is_empty() {
        return Err("fenced_command requires a nonblank command".into());
    }
    if target.command.len() > MAX_COMMAND_BYTES || target.command.contains('\0') {
        return Err(format!(
            "fenced_command command must be at most {MAX_COMMAND_BYTES} bytes with no NUL; the operator reads every byte of it"
        ));
    }
    if target.executor.trim().is_empty() {
        return Err("fenced_command requires the executor's public name".into());
    }
    match target.timeout_ms {
        Some(0) => return Err("fenced_command timeoutMs must be positive".into()),
        Some(timeout) if timeout > u64::from(RUN_BATCH_CEILING_MS) => {
            return Err(format!(
                "fenced_command timeoutMs {timeout} exceeds the batch ceiling of {RUN_BATCH_CEILING_MS} ms"
            ))
        }
        _ => {}
    }
    Ok(target)
}

/// Freeze the act a filing names: the run's own branch head, the executor's
/// registry identity, and a cwd inside the checkout. Refuses before anything is
/// persisted when the executor's floor is not measured refused.
pub(crate) async fn freeze(
    orch: &Orchestrator,
    request: &CallbackRequest,
    raw: Option<&Value>,
) -> Result<FencedCommandAct, String> {
    let target = parse_target(raw)?;
    let cwd = normalize_cwd(target.cwd.as_deref())?;
    let executor_id = crate::fleet::management::resolve_executor_reference(orch, &target.executor)?;
    let floor = executor_floor(orch, &executor_id)?;
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request).await?;
    let resolution = crate::mcp::handlers::branch::resolve_current_for_read(orch, request).await?;
    Ok(FencedCommandAct {
        executor_id: floor.executor_id,
        device_id: floor.device_id,
        project_id: resolution.project_id,
        branch: resolution.rev,
        commit: resolution.commit_id,
        cwd,
        command: target.command,
        timeout_ms: target.timeout_ms.unwrap_or(u64::from(RUN_BATCH_CEILING_MS)),
        node: agent.home,
    })
}

/// The canonical stored target: one encoding, so the store, the digest and
/// the coalescing key all name the same bytes.
pub(crate) fn encode_act(act: &FencedCommandAct) -> Result<String, String> {
    serde_json::to_string(act)
        .map_err(|error| format!("fenced_command act is unencodable: {error}"))
}

pub(crate) fn decode_act(target: Option<&str>) -> Result<FencedCommandAct, String> {
    let target = target.ok_or("fenced_command request has no frozen act")?;
    let act: FencedCommandAct = serde_json::from_str(target)
        .map_err(|error| format!("fenced_command frozen act is unreadable: {error}"))?;
    if encode_act(&act)? != target {
        return Err("fenced_command frozen act is not in canonical form".into());
    }
    Ok(act)
}

/// Two filings coalesce exactly when their frozen acts are equal.
pub(crate) fn coalescing_key(encoded: &str) -> String {
    use sha2::Digest;
    format!("{KIND}:{:x}", sha2::Sha256::digest(encoded.as_bytes()))
}

/// The requesting node's open scratch, if it has one. Never creates one.
pub(crate) async fn node_scratch_now(
    orch: &Orchestrator,
    node: &str,
) -> Result<Option<crate::node_scratch::NodeScratch>, String> {
    crate::node_scratch::owner::open_by_uri(&orch.db.local, node)
        .await
        .map_err(|error| format!("the requesting node's $TMPDIR is unreadable: {error}"))
}

/// The facts the card renders, and the witness an acceptance freezes.
pub(crate) async fn facts(orch: &Orchestrator, target: Option<&str>) -> Result<ActorFacts, String> {
    let act = decode_act(target)?;
    let floor = executor_floor(orch, &act.executor_id)?;
    if floor.device_id != act.device_id {
        return Err(format!(
            "executor identity changed since filing: {} now presents a different device; file the request again",
            floor.name
        ));
    }
    let scratch = node_scratch_now(orch, &act.node).await?;
    let scratch_head = scratch
        .as_ref()
        .and_then(|scratch| scratch.head_commit.clone());
    let document = json!({
        "scratch": scratch.as_ref().map(|scratch| json!({
            "head": scratch.head_commit,
            "bytes": scratch.head_bytes,
        })),
        "executor": {
            "name": floor.name,
            "os": floor.os,
            "executorId": floor.executor_id,
            "deviceId": floor.device_id,
            "credentialFloor": "refused",
        },
        "commit": {
            "id": act.commit,
            "branch": act.branch,
            "diffUri": format!("{}/diff", act.node),
        },
        "cwd": act.cwd,
        "command": act.command,
        "timeoutMs": act.timeout_ms,
        "node": act.node,
        "consequence": consequence(&floor.name),
    });
    Ok(ActorFacts {
        document,
        running_version: None,
        main_commit: None,
        candidate_commit: None,
        base_commit: None,
        policy_project_id: None,
        drain: None,
        fenced_command: Some(FencedCommandAcceptance {
            act,
            witness: floor.witness,
            scratch_head,
        }),
    })
}

pub(crate) const SCRATCH_MOVED: &str = "the requesting node's $TMPDIR changed since this was accepted (a batch of that node wrote to it); nothing ran. Refresh the card and accept again to run what it holds now";

/// Whether the machine and scratch standing now are the ones accepted. The
/// branch is deliberately not compared: the cell checks out the frozen commit,
/// so what runs is what the operator reviewed however the branch has moved.
fn check_live(
    accepted: &FencedCommandAcceptance,
    floor: &ExecutorFloor,
    scratch_head: Option<&str>,
) -> Result<(), String> {
    if floor.device_id != accepted.act.device_id {
        return Err("executor identity changed since acceptance; nothing ran".into());
    }
    if floor.witness != accepted.witness {
        return Err(format!(
            "{}'s connection or credential-floor measurement changed since acceptance (generation, build, boot or fingerprint); nothing ran, and the operator must accept again",
            floor.name
        ));
    }
    if !floor.online {
        return Err(format!("{} is not online; nothing ran", floor.name));
    }
    if scratch_head != accepted.scratch_head.as_deref() {
        return Err(SCRATCH_MOVED.into());
    }
    Ok(())
}

/// The act and witness this request accepted, unchanged against the registry
/// and the node's scratch now. Every refusal here is before any effect.
pub(crate) async fn recheck_live(
    orch: &Orchestrator,
    accepted: &FencedCommandAcceptance,
) -> Result<ExecutorFloor, String> {
    let floor = executor_floor(orch, &accepted.act.executor_id)?;
    let scratch = node_scratch_now(orch, &accepted.act.node).await?;
    check_live(
        accepted,
        &floor,
        scratch
            .as_ref()
            .and_then(|scratch| scratch.head_commit.as_deref()),
    )?;
    Ok(floor)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accepted_act() -> FencedCommandAcceptance {
        FencedCommandAcceptance {
            act: act("make", "cairn://p/p/1/1/builder"),
            witness: ExecutorFloorWitness {
                connection_generation: 4,
                executor_build_id: "build-9".into(),
                boot_generation: "boot-1".into(),
                floor_fingerprint: "f".repeat(64),
                floor_measured_at_unix_ms: 1,
            },
            scratch_head: Some("c".repeat(40)),
        }
    }

    fn live(accepted: &FencedCommandAcceptance) -> ExecutorFloor {
        ExecutorFloor {
            executor_id: accepted.act.executor_id.clone(),
            device_id: accepted.act.device_id.clone(),
            name: "mac".into(),
            os: "macos".into(),
            online: true,
            witness: accepted.witness.clone(),
        }
    }

    #[test]
    fn the_claim_refuses_a_changed_device_measurement_connection_or_scratch() {
        let accepted = accepted_act();
        let head = "c".repeat(40);
        assert!(check_live(&accepted, &live(&accepted), Some(&head)).is_ok());

        let mut other_device = live(&accepted);
        other_device.device_id = "device-2".into();
        assert!(check_live(&accepted, &other_device, Some(&head))
            .unwrap_err()
            .contains("executor identity changed"));

        let mut reconnected = live(&accepted);
        reconnected.witness.connection_generation = 5;
        let mut remeasured = live(&accepted);
        remeasured.witness.floor_fingerprint = "0".repeat(64);
        let mut rebuilt = live(&accepted);
        rebuilt.witness.executor_build_id = "build-10".into();
        for moved in [reconnected, remeasured, rebuilt] {
            assert!(check_live(&accepted, &moved, Some(&head))
                .unwrap_err()
                .contains("must accept again"));
        }

        let mut offline = live(&accepted);
        offline.online = false;
        assert!(check_live(&accepted, &offline, Some(&head))
            .unwrap_err()
            .contains("not online"));

        assert_eq!(
            check_live(&accepted, &live(&accepted), Some(&"d".repeat(40))).unwrap_err(),
            SCRATCH_MOVED
        );
        assert_eq!(
            check_live(&accepted, &live(&accepted), None).unwrap_err(),
            SCRATCH_MOVED
        );
        let mut empty = accepted_act();
        empty.scratch_head = None;
        assert!(check_live(&empty, &live(&empty), None).is_ok());
    }

    #[test]
    fn the_scratch_head_is_part_of_what_an_acceptance_binds() {
        let accepted = accepted_act();
        let mut other = accepted_act();
        other.scratch_head = Some("d".repeat(40));
        let digest = |fenced: &FencedCommandAcceptance| {
            crate::operator_requests::registry::act_digest(
                cairn_common::requests::RequestKind::FencedCommand,
                Some("target"),
                Some(fenced),
            )
        };
        assert_ne!(digest(&accepted), digest(&other));
    }

    #[test]
    fn cwd_stays_inside_the_checkout() {
        assert_eq!(normalize_cwd(None).unwrap(), "");
        assert_eq!(normalize_cwd(Some(" . ")).unwrap(), "");
        assert_eq!(
            normalize_cwd(Some("src-tauri/./os/")).unwrap(),
            "src-tauri/os"
        );
        for outside in [
            "/tmp",
            "..",
            "a/../../b",
            "C:\\Users",
            "\\\\server\\share",
            "a/..",
        ] {
            assert!(normalize_cwd(Some(outside)).is_err(), "{outside}");
        }
    }

    #[test]
    fn the_target_is_typed_bounded_and_never_carries_a_commit() {
        let ok = parse_target(Some(&json!({"executor":"local","command":"ls"}))).unwrap();
        assert_eq!(ok.timeout_ms, None);
        for forged in ["commit", "executorId", "deviceId", "node", "branch"] {
            let mut raw = json!({"executor":"local","command":"ls"});
            raw[forged] = json!("forged");
            assert!(parse_target(Some(&raw)).is_err(), "{forged}");
        }
        assert!(parse_target(Some(&json!({"executor":"local","command":"  "}))).is_err());
        assert!(parse_target(Some(&json!("ls"))).is_err());
        assert!(parse_target(None).is_err());
        assert!(parse_target(Some(
            &json!({"executor":"local","command":"ls","timeoutMs":0})
        ))
        .is_err());
        let over = u64::from(RUN_BATCH_CEILING_MS) + 1;
        assert!(parse_target(Some(
            &json!({"executor":"local","command":"ls","timeoutMs":over})
        ))
        .is_err());
        let long = "x".repeat(MAX_COMMAND_BYTES + 1);
        assert!(parse_target(Some(&json!({"executor":"local","command":long}))).is_err());
    }

    fn act(command: &str, node: &str) -> FencedCommandAct {
        FencedCommandAct {
            executor_id: "exec-1".into(),
            device_id: "device-1".into(),
            project_id: "project".into(),
            branch: "agent/x".into(),
            commit: "a".repeat(40),
            cwd: String::new(),
            command: command.into(),
            timeout_ms: 1_000,
            node: node.into(),
        }
    }

    #[test]
    fn identical_acts_share_a_key_and_any_difference_including_the_node_opens_another() {
        let one = encode_act(&act("make", "cairn://p/p/1/1/builder")).unwrap();
        let same = encode_act(&act("make", "cairn://p/p/1/1/builder")).unwrap();
        let other_command = encode_act(&act("make install", "cairn://p/p/1/1/builder")).unwrap();
        let other_node = encode_act(&act("make", "cairn://p/p/2/1/builder")).unwrap();
        assert_eq!(coalescing_key(&one), coalescing_key(&same));
        assert_ne!(coalescing_key(&one), coalescing_key(&other_command));
        assert_ne!(coalescing_key(&one), coalescing_key(&other_node));
        assert_eq!(
            decode_act(Some(&one)).unwrap(),
            act("make", "cairn://p/p/1/1/builder")
        );
        let reordered = one.replacen("{\"executor_id\"", "{ \"executor_id\"", 1);
        assert!(decode_act(Some(&reordered)).is_err());
    }
}
