//! The durable record of every machine-service launch (cairn/6097, tenet 300).
//!
//! A machine service runs on one of two paths: a resident the supervisor
//! starts on a machine (`project_services::host`), or, for a
//! `placement: fleet` service, a stdio child the runner's MCP gateway spawns
//! on its host when a session first uses it. Both are recorded here in one
//! shape: which machine, which service, whether the program was the pack's
//! pinned artifact or an override and who chose it, the confinement it ran
//! under, and for the gateway the session it connected for. The table
//! (`machine_service_launches`) is the record; the latest launch is also kept
//! in memory so a synchronous rendering can name it.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use cairn_common::executor_protocol::ProcessSandboxMode;
use cairn_db::turso::params;

use crate::config::mcp_servers::FleetLaunchMark;
use crate::orchestrator::Orchestrator;
use crate::project_services::declaration::{LaunchSource, NativeLaunch};

/// Which of the two paths launched the program.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchPath {
    Resident,
    FleetGateway,
}

impl LaunchPath {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Resident => "resident",
            Self::FleetGateway => "fleet_gateway",
        }
    }
}

/// One launch, as recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineServiceLaunch {
    pub at_ms: u64,
    pub machine: String,
    pub service_key: String,
    pub path: LaunchPath,
    /// `artifact` or `override`.
    pub source: &'static str,
    pub program: String,
    /// The row's `SetBy` for the selection, as JSON; absent for an artifact
    /// no row entry chose, or a selection made before attribution existed.
    pub set_by_json: Option<String>,
    /// `unconfined` or `credential_floor`.
    pub confinement: &'static str,
    /// The session the gateway connected for.
    pub session_id: Option<String>,
}

impl MachineServiceLaunch {
    /// A resident the supervisor started for `service_key` on `machine`.
    pub(crate) fn resident(service_key: &str, machine: &str, native: &NativeLaunch) -> Self {
        Self {
            at_ms: crate::fleet::unix_time_ms(),
            machine: machine.to_string(),
            service_key: service_key.to_string(),
            path: LaunchPath::Resident,
            source: native.source.as_str(),
            program: native.program.clone(),
            set_by_json: native
                .set_by
                .as_ref()
                .and_then(|set_by| serde_json::to_string(set_by).ok()),
            confinement: confinement(native.sandbox_mode),
            session_id: None,
        }
    }

    /// A fleet entry the gateway spawned as `program` for `session`.
    pub fn gateway(mark: &FleetLaunchMark, program: &str, session: Option<&str>) -> Self {
        Self {
            at_ms: crate::fleet::unix_time_ms(),
            machine: mark.machine.clone(),
            service_key: mark.service_key.clone(),
            path: LaunchPath::FleetGateway,
            source: if mark.overridden {
                LaunchSource::Override.as_str()
            } else {
                LaunchSource::Artifact.as_str()
            },
            program: program.to_string(),
            set_by_json: mark.set_by_json.clone(),
            confinement: if mark.credential_floor {
                "credential_floor"
            } else {
                "unconfined"
            },
            session_id: session.map(str::to_string),
        }
    }

    /// The launch as one line: what ran, under what, chosen by whom, when.
    pub fn sentence(&self) -> String {
        let set_by = self
            .set_by_json
            .as_deref()
            .and_then(|json| serde_json::from_str::<cairn_config::fleet::SetBy>(json).ok());
        let at = chrono::DateTime::from_timestamp_millis(self.at_ms as i64)
            .map(|at| {
                at.with_timezone(&chrono::Local)
                    .format("%m-%d %H:%M")
                    .to_string()
            })
            .unwrap_or_default();
        let via = match self.path {
            LaunchPath::Resident => "as a resident".to_string(),
            LaunchPath::FleetGateway => match &self.session_id {
                Some(session) => format!("by the gateway for session {session}"),
                None => "by the gateway".to_string(),
            },
        };
        let chooser = match (self.source, set_by.as_ref()) {
            ("artifact", None) => "the pack's pinned artifact".to_string(),
            (_, set_by) => crate::fleet::management::describe_setter(set_by),
        };
        format!(
            "{at}: {} {} {via}, {} ({chooser})",
            self.source,
            self.program,
            self.confinement.replace('_', " "),
        )
    }
}

fn confinement(mode: ProcessSandboxMode) -> &'static str {
    match mode {
        ProcessSandboxMode::CredentialFloor => "credential_floor",
        _ => "unconfined",
    }
}

/// (service key, machine). A runner runs one orchestrator, so the cache is
/// that runner's.
type LatestKey = (String, String);

fn latest_launches() -> &'static Mutex<HashMap<LatestKey, MachineServiceLaunch>> {
    static LATEST: OnceLock<Mutex<HashMap<LatestKey, MachineServiceLaunch>>> = OnceLock::new();
    LATEST.get_or_init(Mutex::default)
}

/// Write `launch` to the record. A write that fails is logged at warn rather
/// than failing the launch it describes: the process is already running.
pub(crate) async fn record(orch: &Orchestrator, launch: MachineServiceLaunch) {
    let written = orch
        .db
        .local
        .execute(
            "INSERT INTO machine_service_launches
               (at_ms, machine, service_key, path, source, program, set_by_json, confinement, session_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                launch.at_ms as i64,
                launch.machine.as_str(),
                launch.service_key.as_str(),
                launch.path.as_str(),
                launch.source,
                launch.program.as_str(),
                launch.set_by_json.clone(),
                launch.confinement,
                launch.session_id.clone()
            ],
        )
        .await;
    if let Err(error) = written {
        log::warn!(
            "could not record the launch of {} on {}: {error}",
            launch.service_key,
            launch.machine
        );
    }
    latest_launches()
        .lock()
        .unwrap()
        .insert((launch.service_key.clone(), launch.machine.clone()), launch);
}

/// The gateway's half: record that it spawned a fleet entry for `session`.
pub async fn record_gateway_spawn(
    orch: &Orchestrator,
    mark: &FleetLaunchMark,
    program: &str,
    session: Option<&str>,
) {
    record(orch, MachineServiceLaunch::gateway(mark, program, session)).await;
}

/// The latest launch of `service_key` on `machine` since this runner started,
/// as one line.
pub(crate) fn latest_sentence(service_key: &str, machine: &str) -> Option<String> {
    latest_launches()
        .lock()
        .unwrap()
        .get(&(service_key.to_string(), machine.to_string()))
        .map(MachineServiceLaunch::sentence)
}

/// Every recorded launch of `service_key`, newest first.
#[cfg(test)]
pub(crate) async fn recorded(
    orch: &Orchestrator,
    service_key: &str,
) -> Vec<(String, String, String, Option<String>)> {
    use crate::storage::RowExt;
    orch.db
        .local
        .query_all(
            "SELECT path, source, confinement, session_id FROM machine_service_launches
              WHERE service_key = ?1 ORDER BY id DESC",
            params![service_key],
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?, row.opt_text(3)?)),
        )
        .await
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project_services::declaration::NativeLaunch;

    /// One row per resident start and per gateway spawn, with its source,
    /// confinement and, for the gateway, its session; the latest is named.
    #[tokio::test]
    async fn each_launch_is_recorded_on_its_path() {
        let orch = crate::orchestrator::models::tests::seeded_orchestrator().await;
        let set_by = cairn_config::fleet::SetBy {
            kind: cairn_config::fleet::SetByKind::Agent,
            principal: "cairn://p/cairn/1/1/builder".to_string(),
            node_uri: Some("cairn://p/cairn/1/1/builder".to_string()),
            approval: Some("grant-1".to_string()),
            at_unix_ms: 0,
        };
        record(
            &orch,
            MachineServiceLaunch::resident(
                "kit/launched",
                "lab",
                &NativeLaunch::overriding("/opt/probe".to_string(), vec![], Some(set_by.clone())),
            ),
        )
        .await;
        let mark = FleetLaunchMark {
            service_key: "kit/launched".to_string(),
            machine: "local".to_string(),
            overridden: false,
            credential_floor: false,
            set_by_json: None,
        };
        record_gateway_spawn(
            &orch,
            &mark,
            "/home/u/.cairn/tools/probe",
            Some("session-1"),
        )
        .await;
        assert_eq!(
            recorded(&orch, "kit/launched").await,
            [
                (
                    "fleet_gateway".to_string(),
                    "artifact".to_string(),
                    "unconfined".to_string(),
                    Some("session-1".to_string())
                ),
                (
                    "resident".to_string(),
                    "override".to_string(),
                    "credential_floor".to_string(),
                    None
                ),
            ]
        );
        let resident = latest_sentence("kit/launched", "lab").unwrap();
        assert!(
            resident.contains("override /opt/probe as a resident, credential floor"),
            "{resident}"
        );
        assert!(
            resident.contains(
                "set by agent cairn://p/cairn/1/1/builder, approved by the operator (grant-1)"
            ),
            "{resident}"
        );
        let gateway = latest_sentence("kit/launched", "local").unwrap();
        assert!(
            gateway.contains("by the gateway for session session-1"),
            "{gateway}"
        );
        assert!(gateway.contains("the pack's pinned artifact"), "{gateway}");
    }
}
