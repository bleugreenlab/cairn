//! Persisted fleet policy and executor declarations; no running fleet.
use cairn_common::executor_protocol::{
    normalize_executor_name, EXECUTOR_NAME_RULE, LOCAL_EXECUTOR_NAME,
};
pub use cairn_common::executor_protocol::{MachineRole, PlacementWorkClass};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
pub const COLOCATED_EXECUTOR_ID: &str = "colocated";
pub const DEFAULT_PLACEMENT_PROFILE: &str = "default";
pub const INTERACTIVE_PLACEMENT_PROFILE: &str = "interactive";
pub const LOW_POWER_PLACEMENT_PROFILE: &str = "low-power";
pub const MAX_PREFERENCE_DELAY_SECONDS: u64 = 60 * 60;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PlacementStance {
    LocalFirst,
    RemoteFirst,
    RemoteOnly,
    #[default]
    Any,
}

fn default_active_placement_profile() -> String {
    DEFAULT_PLACEMENT_PROFILE.to_string()
}

pub fn built_in_profile(name: &str) -> Option<&'static PlacementProfile> {
    use std::sync::OnceLock;
    static BUILT_INS: OnceLock<BTreeMap<String, PlacementProfile>> = OnceLock::new();
    BUILT_INS.get_or_init(built_in_placement_profiles).get(name)
}

fn is_built_in_profile(name: &str) -> bool {
    built_in_profile(name).is_some()
}

fn validate_custom_profile_name(name: &str) -> Result<(), String> {
    if name.trim().is_empty() || name.trim() != name {
        return Err(
            "placement profile name must be nonblank and have no surrounding whitespace".into(),
        );
    }

    if is_built_in_profile(name) {
        return Err(format!(
            "built-in placement profile {name} cannot be shadowed"
        ));
    }
    Ok(())
}

fn validate_profile_executor_id(
    executor_id: &str,
    known_executor_ids: &HashSet<&str>,
) -> Result<(), String> {
    if !known_executor_ids.contains(executor_id) {
        return Err(format!(
            "placement profile names unknown executor ID {executor_id}"
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlacementProfile {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub machine_priority: Vec<String>,
    pub routes: BTreeMap<PlacementWorkClass, PlacementStance>,
    #[serde(default)]
    pub max_preference_delay_seconds: u64,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub executor_policy_overrides:
        HashMap<String, cairn_common::executor_protocol::ExecutorRuntimePolicy>,
}

impl PlacementProfile {
    pub fn stance(&self, work_class: PlacementWorkClass) -> PlacementStance {
        self.routes
            .get(&work_class)
            .copied()
            .unwrap_or(PlacementStance::Any)
    }

    pub fn validate(&self, known_executor_ids: &HashSet<&str>) -> Result<(), String> {
        for class in placement_work_classes() {
            if !self.routes.contains_key(&class) {
                return Err(format!("placement profile is missing route {class:?}"));
            }
        }
        if self.routes.len() != placement_work_classes().len() {
            return Err("placement profile contains an unknown work-class route".into());
        }
        if self.max_preference_delay_seconds > MAX_PREFERENCE_DELAY_SECONDS {
            return Err(format!(
                "maxPreferenceDelaySeconds must not exceed {MAX_PREFERENCE_DELAY_SECONDS}"
            ));
        }
        let mut priority = HashSet::new();
        for executor_id in &self.machine_priority {
            validate_profile_executor_id(executor_id, known_executor_ids)?;
            if !priority.insert(executor_id) {
                return Err(format!(
                    "placement profile repeats machinePriority executor {executor_id}"
                ));
            }
        }
        for executor_id in self.executor_policy_overrides.keys() {
            validate_profile_executor_id(executor_id, known_executor_ids)?;
        }
        Ok(())
    }
}

fn placement_work_classes() -> [PlacementWorkClass; 5] {
    PlacementWorkClass::ALL
}

pub fn profile_with_routes(
    max_preference_delay_seconds: u64,
    route: impl Fn(PlacementWorkClass) -> PlacementStance,
) -> PlacementProfile {
    PlacementProfile {
        machine_priority: Vec::new(),
        routes: placement_work_classes()
            .into_iter()
            .map(|class| (class, route(class)))
            .collect(),
        max_preference_delay_seconds,
        executor_policy_overrides: HashMap::new(),
    }
}

pub fn built_in_placement_profiles() -> BTreeMap<String, PlacementProfile> {
    [
        (
            DEFAULT_PLACEMENT_PROFILE.to_string(),
            profile_with_routes(0, |_| PlacementStance::Any),
        ),
        (INTERACTIVE_PLACEMENT_PROFILE.to_string(), {
            let mut profile = profile_with_routes(30, |class| match class {
                PlacementWorkClass::ReviewChecks | PlacementWorkClass::WriteChecks => {
                    PlacementStance::RemoteFirst
                }
                PlacementWorkClass::AgentSessions => PlacementStance::LocalFirst,
                PlacementWorkClass::DevInstances | PlacementWorkClass::Services => {
                    PlacementStance::Any
                }
            });
            profile.executor_policy_overrides.insert(
                LOCAL_EXECUTOR_NAME.to_string(),
                cairn_common::executor_protocol::ExecutorRuntimePolicy {
                    cpu_admission: cairn_common::executor_protocol::CpuAdmissionPolicy {
                        entry_utilization: 0.75,
                        clear_utilization: 0.60,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            );
            profile
        }),
        (
            LOW_POWER_PLACEMENT_PROFILE.to_string(),
            profile_with_routes(120, |_| PlacementStance::RemoteFirst),
        ),
    ]
    .into_iter()
    .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FleetConfig {
    #[serde(default = "default_active_placement_profile")]
    pub active_placement_profile: String,
    /// Operator-authored profiles only. Code-owned built-ins are merged through
    /// [`FleetConfig::resolved_placement_profiles`] and never persisted here.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub placement_profiles: BTreeMap<String, PlacementProfile>,
    /// How long a caller with no tighter answer of its own is willing to wait
    /// for a machine that is merely busy.
    ///
    /// This is a wait horizon, not a queue budget. It is not a bound on one
    /// attempt — there are no attempts — and nothing about it is refreshed while
    /// a request waits: it is the requester's answer to "when does this result
    /// stop being wanted", carried onto the queue entry and honoured there.
    ///
    /// Read it through [`FleetConfig::capacity_wait_horizon_ms`], never
    /// directly: a value below [`MIN_CAPACITY_WAIT_HORIZON_SECONDS`] is not a
    /// policy anyone chose, and that accessor is where it is refused.
    ///
    /// The retired spelling `acquisitionDeadlineSeconds` is deliberately NOT
    /// aliased. It named a different quantity — twenty seconds of per-attempt
    /// queue budget that a ten-minute executor-side pause then quietly
    /// compensated for — and CAIRN-3268 changed the meaning by a factor of
    /// thirty. An alias migrates a spelling; it cannot migrate a semantics. So
    /// the retired key is ignored and the default applies, because inheriting
    /// the old number under the new name is how a fleet came to abandon every
    /// check after twenty seconds while its settings file looked deliberate
    /// (CAIRN-3429).
    #[serde(default = "default_capacity_wait_horizon_seconds")]
    pub capacity_wait_horizon_seconds: u64,
    #[serde(default = "default_timeout_seconds")]
    pub default_timeout_seconds: u64,
    /// Workspace fallback used when a machine has no complete runtime-policy
    /// override. Profile and machine-specific resolution can layer above this
    /// single baseline without putting policy into executor inventory.
    #[serde(default)]
    pub cpu_admission: cairn_common::executor_protocol::CpuAdmissionPolicy,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub executor_policies: HashMap<String, cairn_common::executor_protocol::ExecutorRuntimePolicy>,
    /// Runner-owned SSH executor declarations, keyed by stable executor ID.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub remote_executors: BTreeMap<String, RemoteExecutorConfig>,
    /// Stable operating-system identities learned during SSH enrollment, keyed
    /// by executor ID. Kept outside the public declaration because callers name
    /// a host; only the host itself can authoritatively identify it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub remote_host_identities: BTreeMap<String, String>,
    /// Which pack-declared machine services run on which machines: one resident
    /// per selected machine. Presence of a machine under a service is the whole
    /// selection; runtime state is derived live and never stored here.
    #[serde(default, skip_serializing_if = "MachineServicesConfig::is_empty")]
    pub machine_services: MachineServicesConfig,
    /// The retired per-machine desktop toggles, read only as input to their
    /// conversion into `machineServices.axon/axon`
    /// ([`FleetConfig::adopt_legacy_desktop_automation`]). Every typed read
    /// converts it through [`FleetConfig::healed`], so a snapshot written back
    /// by `set_fleet` carries the converted row rather than dropping it, and
    /// `skip_serializing` removes the key at the first write.
    #[serde(default, rename = "desktopAutomation", skip_serializing)]
    pub legacy_desktop_automation: Option<LegacyDesktopAutomation>,
    /// Where this runner listens for executors that dial in. Absent on a runner
    /// with no remote fleet, which binds nothing beyond loopback. A runner with
    /// enrolled machines writes it itself when it first brings the listener up,
    /// because every machine bootstrapped against it pins that port. Reading
    /// this never synthesizes one: a listener claimed here may not have bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_listener: Option<ExecutorListenerSettings>,
    /// Machines an operator has drained, by the key
    /// [`FleetConfig::drain_key`] gives them.
    ///
    /// Draining is an admission-refusal fact the RUNNER holds, not a setting on
    /// the machine: it says this runner will not place new work there. That is
    /// why it lives here rather than only in the executor's memory. A drain is
    /// reached for precisely when a machine is in no condition to agree to it,
    /// and a flag that died with the connection let a crushed machine be
    /// dogpiled again the moment it came back (CAIRN-4360).
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub drained_machines: BTreeSet<String>,
    /// What each machine is for, by the key [`FleetConfig::drain_key`] gives
    /// it. Only roles other than general are stored, so an absent entry IS
    /// general and a fleet that never set one serializes nothing here.
    ///
    /// Like a drain, a role is a fact the RUNNER holds and enforces in its own
    /// placement; the machine is never told. Unlike a drain it is not a
    /// refusal of everything: it says which classes of work this runner will
    /// offer the machine at all, derived from the role in
    /// [`MachineRole::accepts`]. This map is the one authority; the fleet's
    /// in-memory copy is rebuilt from it whenever it is written or loaded.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub machine_roles: BTreeMap<String, MachineRole>,
    /// Where the runner's own executor keeps its cells, managed objects,
    /// dependency catalog and external caches, when that is a volume apart
    /// from the home: an external drive the machine may be undocked from.
    /// Absent keeps them under the home. Set only through the build-root
    /// command, which validates the path and writes the marker, never through
    /// a whole-block settings write; read at runner start, so a change takes
    /// effect when the runner next starts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub colocated_build_root: Option<cairn_common::executor_paths::BuildRootDeclaration>,
}

/// The one segment rule both halves of a machine-service key follow.
fn is_machine_service_segment(segment: &str) -> bool {
    let mut characters = segment.chars();
    segment.len() <= 63
        && characters
            .next()
            .is_some_and(|character| character.is_ascii_lowercase())
        && characters.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
}

pub const MACHINE_SERVICE_KEY_RULE: &str =
    "a machine-service key is <pack>/<service>, each half matching ^[a-z][a-z0-9-]{0,62}$";

/// A pack-declared service, addressed as `<pack>/<service>`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MachineServiceKey {
    pub pack: String,
    pub service: String,
}

impl MachineServiceKey {
    pub fn new(pack: &str, service: &str) -> Result<Self, String> {
        format!("{pack}/{service}").parse()
    }
}

impl std::str::FromStr for MachineServiceKey {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, String> {
        match value.split_once('/') {
            Some((pack, service))
                if is_machine_service_segment(pack) && is_machine_service_segment(service) =>
            {
                Ok(Self {
                    pack: pack.to_string(),
                    service: service.to_string(),
                })
            }
            _ => Err(format!(
                "{value:?} is not a machine service: {MACHINE_SERVICE_KEY_RULE}"
            )),
        }
    }
}

impl TryFrom<String> for MachineServiceKey {
    type Error = String;
    fn try_from(value: String) -> Result<Self, String> {
        value.parse()
    }
}

impl From<MachineServiceKey> for String {
    fn from(key: MachineServiceKey) -> String {
        key.to_string()
    }
}

impl std::fmt::Display for MachineServiceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.pack, self.service)
    }
}

/// Every machine service this runner places, and on which machines.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct MachineServicesConfig(pub BTreeMap<MachineServiceKey, MachineServiceSelection>);

/// The machines one service is selected on, by public executor name.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MachineServiceSelection {
    #[serde(default)]
    pub machines: BTreeMap<String, MachineSelection>,
}

/// One machine's selection of a service. An empty selection runs the pack's
/// own artifact; an override runs the operator's binary instead and is shown
/// as such. There is deliberately no `enabled` flag: presence is the
/// selection, and a second way to say "off" is a second way to be wrong.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MachineSelection {
    #[serde(default, rename = "override", skip_serializing_if = "Option::is_none")]
    pub override_: Option<CommandOverride>,
}

impl MachineSelection {
    pub fn override_command(&self) -> Option<&str> {
        self.override_.as_ref().map(|value| value.command.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommandOverride {
    pub command: String,
}

/// Whether `path` is absolute on some platform a machine could run: POSIX
/// `/...`, a Windows drive path `C:\...`, or a UNC path. The runner cannot know
/// which platform a machine that has never attached runs, so it accepts the
/// absolute forms of each; a bare name is refused because it would be a PATH
/// lookup, which is the managed artifact's job.
pub fn is_absolute_command_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    path.starts_with('/')
        || path.starts_with(r"\\")
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'\\' | b'/'))
}

impl MachineServicesConfig {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// One machine's selection of one service, if it is selected there.
    pub fn selection(&self, key: &MachineServiceKey, machine: &str) -> Option<&MachineSelection> {
        let machine = normalize_executor_name(machine)?;
        self.0.get(key)?.machines.get(&machine)
    }

    /// Every service selected on `machine`, in key order.
    pub fn selected_on(&self, machine: &str) -> Vec<(&MachineServiceKey, &MachineSelection)> {
        let Some(machine) = normalize_executor_name(machine) else {
            return Vec::new();
        };
        self.0
            .iter()
            .filter_map(|(key, selection)| {
                selection.machines.get(&machine).map(|chosen| (key, chosen))
            })
            .collect()
    }

    /// Every machine `key` is selected on, in name order.
    pub fn machines_for(&self, key: &MachineServiceKey) -> Vec<(&str, &MachineSelection)> {
        self.0
            .get(key)
            .map(|selection| {
                selection
                    .machines
                    .iter()
                    .map(|(name, chosen)| (name.as_str(), chosen))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Select `key` on `machine`, replacing any previous selection there.
    /// Returns whether the stored row changed.
    pub fn select(
        &mut self,
        key: MachineServiceKey,
        machine: &str,
        selection: MachineSelection,
    ) -> Result<bool, String> {
        let machine = normalize_executor_name(machine)
            .ok_or_else(|| format!("{machine:?} has no public name: {EXECUTOR_NAME_RULE}"))?;
        validate_machine_selection(&key, &machine, &selection)?;
        let machines = &mut self.0.entry(key).or_default().machines;
        Ok(machines.insert(machine, selection.clone()) != Some(selection))
    }

    /// Remove `key` from `machine`. A service selected nowhere leaves the row
    /// entirely. Returns whether the stored row changed.
    pub fn unselect(&mut self, key: &MachineServiceKey, machine: &str) -> bool {
        let Some(machine) = normalize_executor_name(machine) else {
            return false;
        };
        let Some(selection) = self.0.get_mut(key) else {
            return false;
        };
        let removed = selection.machines.remove(&machine).is_some();
        if selection.machines.is_empty() {
            self.0.remove(key);
        }
        removed
    }

    /// Carry every selection a machine holds with it when it is renamed.
    ///
    /// The row is keyed by public name rather than by executor identity,
    /// because the colocated machine has no remote declaration to hang it off.
    /// That makes a rename a move: leaving the entry under the old name would
    /// silently unselect every service on the machine.
    pub fn rename_machine(&mut self, from: &str, to: &str) {
        let (Some(from), Some(to)) = (normalize_executor_name(from), normalize_executor_name(to))
        else {
            return;
        };
        if from == to {
            return;
        }
        for selection in self.0.values_mut() {
            if let Some(machine) = selection.machines.remove(&from) {
                selection.machines.insert(to.clone(), machine);
            }
        }
    }

    /// Drop a removed machine's selections along with it, so removal leaves no
    /// entry addressing a machine that no longer exists.
    pub fn forget_machine(&mut self, name: &str) {
        let Some(name) = normalize_executor_name(name) else {
            return;
        };
        for selection in self.0.values_mut() {
            selection.machines.remove(&name);
        }
        self.0.retain(|_, selection| !selection.machines.is_empty());
    }

    pub fn validate(&self) -> Result<(), String> {
        for (key, selection) in &self.0 {
            for (machine, chosen) in &selection.machines {
                if normalize_executor_name(machine).as_deref() != Some(machine.as_str()) {
                    return Err(format!(
                        "machineServices.{key} names machine {machine:?}, which is not a public executor name: {EXECUTOR_NAME_RULE}"
                    ));
                }
                validate_machine_selection(key, machine, chosen)?;
            }
        }
        Ok(())
    }
}

fn validate_machine_selection(
    key: &MachineServiceKey,
    machine: &str,
    selection: &MachineSelection,
) -> Result<(), String> {
    if let Some(command) = selection.override_command() {
        if command.trim() != command || !is_absolute_command_path(command) {
            return Err(format!(
                "machineServices.{key}.machines.{machine}.override.command must be an absolute path, not {command:?}: a bare name would be a PATH lookup, and the pack's managed artifact is what runs without an override"
            ));
        }
    }
    Ok(())
}

/// The service the retired `desktopAutomation` toggles selected.
pub const AXON_MACHINE_SERVICE: &str = "axon/axon";

/// `buildSlots.desktopAutomation` as it was last written. Deliberately not
/// `deny_unknown_fields`: it is conversion input, and refusing an odd key in it
/// would refuse the whole fleet block it sits in.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LegacyDesktopAutomation {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub machines: BTreeMap<String, LegacyMachineDesktopAutomation>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LegacyMachineDesktopAutomation {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub binary: Option<String>,
}

pub use cairn_common::executor_protocol::RemotePlatform;

/// The runner's executor-facing listener, as an operator configures it.
///
/// `bind` is a local socket address; `advertised_endpoints` is what a remote
/// machine should dial, which is deliberately a separate answer: a wildcard bind
/// says nothing about the names it is reachable at. Empty means the runner
/// advertises what it discovers on its own host; non-empty is an operator
/// override that replaces discovery. Either way each bootstrap verifies the
/// list from the machine being enrolled.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutorListenerSettings {
    pub bind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub advertised_endpoints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoteExecutorDeclaration {
    pub host: String,
    pub ssh_user: String,
    pub binary_path: Option<String>,
    pub cairn_home: Option<String>,
    pub executor_id: String,
    pub device_id: String,
    pub display_name: String,
    pub project_ids: Vec<String>,
    /// Accepted and dropped. See [`RemoteExecutorConfig::legacy_tunnel_port`].
    #[serde(default, rename = "tunnelPort", skip_serializing)]
    pub legacy_tunnel_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_ssh_args: Vec<String>,
    /// What the machine is for. Carried by the ceremony and persisted to
    /// [`FleetConfig::machine_roles`] in the same write as the machine's
    /// configuration, never to the machine itself.
    #[serde(default)]
    pub role: MachineRole,
}

impl RemoteExecutorDeclaration {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("host", self.host.as_str()),
            ("sshUser", self.ssh_user.as_str()),
            ("executorId", self.executor_id.as_str()),
            ("deviceId", self.device_id.as_str()),
            ("displayName", self.display_name.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("remote executor {name} must not be blank"));
            }
        }
        if self.host.starts_with('-') || self.ssh_user.starts_with('-') {
            return Err("remote executor host and sshUser must not begin with '-'".into());
        }
        if self.executor_id == COLOCATED_EXECUTOR_ID || !is_safe_executor_id(&self.executor_id) {
            return Err("remote executor executorId is unsafe".into());
        }
        validate_public_executor_name(&self.display_name)?;
        for (name, path) in [
            ("binaryPath", &self.binary_path),
            ("cairnHome", &self.cairn_home),
        ] {
            if path.as_deref().is_some_and(|value| value.trim().is_empty()) {
                return Err(format!("remote executor {name} must not be blank"));
            }
        }
        let mut projects = HashSet::new();
        for project_id in &self.project_ids {
            uuid::Uuid::parse_str(project_id)
                .map_err(|_| format!("remote executor project ID is not a UUID: {project_id}"))?;
            if !projects.insert(project_id) {
                return Err(format!("remote executor repeats project UUID {project_id}"));
            }
        }
        validate_extra_ssh_args(&self.extra_ssh_args)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoteExecutorConfig {
    pub host: String,
    pub ssh_user: String,
    #[serde(default)]
    pub platform: RemotePlatform,
    pub binary_path: String,
    pub cairn_home: String,
    pub executor_id: String,
    pub device_id: String,
    pub display_name: String,
    pub project_ids: Vec<String>,
    /// When this machine's bootstrap ceremony last completed, and therefore the
    /// durable claim that it runs a resident executor as its own OS service.
    ///
    /// An enrollment without it was written before the transport inversion: it
    /// has no resident service or listener-compatible credential. This durable
    /// record, unlike link state, is what lets startup identify that cutover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrapped_at_unix_ms: Option<u64>,
    /// When this runner last confirmed that the machine runs no pre-inversion
    /// executor installation: no pre-rename runner service left, and every
    /// legacy home's executor generation proven released. Machines bootstrapped
    /// before cairn/5002 lack it, which is how the runner's start pass knows to
    /// retire what their cutover left running; a ceremony stamps it too.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_retired_at_unix_ms: Option<u64>,
    /// The reverse-tunnel port a runner used to open back to itself, before the
    /// executor became the side that dials.
    ///
    /// Accepted and dropped rather than deleted. Both of these structs carry
    /// `deny_unknown_fields`, and `load_fleet` turns a parse error into
    /// `unwrap_or_default()` — so removing the key outright would give an
    /// operator whose settings file still names it a runner with no enrolled
    /// machines, no placement profiles and no drained set, silently. Keeping the
    /// field lets the document parse; `skip_serializing` means the next
    /// `set_fleet` write removes it for good.
    ///
    /// Deletable once load_fleet stops swallowing a parse failure into an empty
    /// fleet (cairn://posts/209) — until then this sink is what stands between a
    /// stale key and a runner that silently has no machines.
    #[serde(default, rename = "tunnelPort", skip_serializing)]
    pub legacy_tunnel_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_ssh_args: Vec<String>,
}

impl RemoteExecutorConfig {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("host", self.host.as_str()),
            ("sshUser", self.ssh_user.as_str()),
            ("executorId", self.executor_id.as_str()),
            ("deviceId", self.device_id.as_str()),
            ("displayName", self.display_name.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("remote executor {name} must not be blank"));
            }
        }
        if self.host.starts_with('-') || self.ssh_user.starts_with('-') {
            return Err("remote executor host and sshUser must not begin with '-'".into());
        }
        for (name, path) in [
            ("binaryPath", &self.binary_path),
            ("cairnHome", &self.cairn_home),
        ] {
            if path.trim().is_empty() || !self.platform.is_absolute(path) {
                return Err(format!("remote executor {name} must be an absolute path"));
            }
        }
        if self.executor_id == COLOCATED_EXECUTOR_ID {
            return Err("remote executor cannot reuse the colocated executor identity".into());
        }
        if !is_safe_executor_id(&self.executor_id) {
            return Err("remote executor executorId must start with an ASCII letter or digit and contain only ASCII letters, digits, '.', '_', or '-'".into());
        }
        validate_public_executor_name(&self.display_name)?;
        let mut project_ids = HashSet::new();
        for project_id in &self.project_ids {
            uuid::Uuid::parse_str(project_id)
                .map_err(|_| format!("remote executor project ID is not a UUID: {project_id}"))?;
            if !project_ids.insert(project_id) {
                return Err(format!("remote executor repeats project UUID {project_id}"));
            }
        }
        validate_extra_ssh_args(&self.extra_ssh_args)
    }
}

fn validate_extra_ssh_args(args: &[String]) -> Result<(), String> {
    for argument in args {
        if !matches!(argument.as_str(), "-4" | "-6") {
            return Err(format!(
                "remote executor extra SSH argument is not an allowed transport selector (-4 or -6): {argument}"
            ));
        }
    }
    Ok(())
}

/// Whether an operator-supplied label can serve as this machine's public
/// address.
///
/// A remote executor may not claim the reserved local name: agents read that
/// name as "the machine the runner is on", and a remote answering to it would
/// send work that must be local somewhere else entirely.
fn validate_public_executor_name(display_name: &str) -> Result<(), String> {
    let Some(name) = normalize_executor_name(display_name) else {
        return Err(format!(
            "remote executor displayName {display_name:?} yields no public name: {EXECUTOR_NAME_RULE}"
        ));
    };
    if name == LOCAL_EXECUTOR_NAME {
        return Err(format!(
            "remote executor displayName {display_name:?} claims the reserved name {LOCAL_EXECUTOR_NAME}, which addresses the runner's own executor"
        ));
    }
    Ok(())
}

fn is_safe_executor_id(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric())
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        })
}

impl FleetConfig {
    pub fn resolved_placement_profiles(&self) -> BTreeMap<String, PlacementProfile> {
        let mut profiles = built_in_placement_profiles();
        profiles.extend(self.placement_profiles.clone());
        profiles
    }

    pub fn active_placement_profile(&self) -> &PlacementProfile {
        self.placement_profiles
            .get(&self.active_placement_profile)
            .or_else(|| built_in_profile(&self.active_placement_profile))
            .expect("FleetConfig is healed at load and validated before save")
    }

    pub fn effective_executor_policy(
        &self,
        executor_id: &str,
    ) -> cairn_common::executor_protocol::ExecutorRuntimePolicy {
        self.active_placement_profile()
            .executor_policy_overrides
            .get(executor_id)
            .cloned()
            .unwrap_or_else(|| self.resolve_executor_policy(executor_id))
    }

    pub fn save_placement_profile(
        &mut self,
        name: String,
        profile: PlacementProfile,
    ) -> Result<(), String> {
        validate_custom_profile_name(&name)?;
        profile.validate(&self.known_profile_executor_ids())?;
        self.placement_profiles.insert(name, profile);
        Ok(())
    }

    pub fn delete_placement_profile(&mut self, name: &str) -> Result<PlacementProfile, String> {
        if is_built_in_profile(name) {
            return Err(format!(
                "built-in placement profile {name} cannot be deleted"
            ));
        }
        if self.active_placement_profile == name {
            return Err(format!("active placement profile {name} cannot be deleted"));
        }
        self.placement_profiles
            .remove(name)
            .ok_or_else(|| format!("placement profile {name} does not exist"))
    }

    pub fn activate_placement_profile(&mut self, name: &str) -> Result<PlacementProfile, String> {
        let profile = self
            .placement_profiles
            .get(name)
            .cloned()
            .or_else(|| built_in_profile(name).cloned())
            .ok_or_else(|| format!("placement profile {name} does not exist"))?;
        self.active_placement_profile = name.to_string();
        Ok(profile)
    }

    fn known_profile_executor_ids(&self) -> HashSet<&str> {
        std::iter::once(LOCAL_EXECUTOR_NAME)
            .chain(self.remote_executors.keys().map(String::as_str))
            .collect()
    }

    /// How a machine is named in durable fleet state.
    ///
    /// The colocated executor has no enrollment to hang a key off, so it is
    /// keyed by the name that addresses it; every other machine is keyed by its
    /// stable identity, which a rename does not move. This is the same key
    /// convention `executor_policies` uses, and drain must not invent a second
    /// one: two spellings of "this machine" is how a drain comes to be held for
    /// a machine nobody can clear it from.
    pub fn drain_key(executor_id: &str) -> &str {
        if executor_id == COLOCATED_EXECUTOR_ID {
            LOCAL_EXECUTOR_NAME
        } else {
            executor_id
        }
    }

    /// Whether this runner is holding a machine drained.
    pub fn is_machine_drained(&self, executor_id: &str) -> bool {
        self.drained_machines.contains(Self::drain_key(executor_id))
    }

    /// Record or clear a drain. Returns whether the stored state changed.
    pub fn set_machine_drained(&mut self, executor_id: &str, drained: bool) -> bool {
        let key = Self::drain_key(executor_id).to_string();
        if drained {
            self.drained_machines.insert(key)
        } else {
            self.drained_machines.remove(&key)
        }
    }

    /// Drop a removed machine's runner-held facts (its drain and its role)
    /// along with it, so a host that is enrolled again does not inherit a
    /// refusal or a purpose nobody asked for.
    pub fn forget_machine(&mut self, executor_id: &str) {
        let key = Self::drain_key(executor_id);
        self.drained_machines.remove(key);
        self.machine_roles.remove(key);
    }

    /// What this runner holds a machine to be for. Absent means general.
    pub fn machine_role(&self, executor_id: &str) -> MachineRole {
        self.machine_roles
            .get(Self::drain_key(executor_id))
            .copied()
            .unwrap_or_default()
    }

    /// Record a machine's role. General removes the entry, since absence is
    /// general. Returns whether the stored state changed.
    pub fn set_machine_role(&mut self, executor_id: &str, role: MachineRole) -> bool {
        let key = Self::drain_key(executor_id).to_string();
        if role == MachineRole::General {
            self.machine_roles.remove(&key).is_some()
        } else {
            self.machine_roles.insert(key, role) != Some(role)
        }
    }

    /// Take `roles` as this config's roles, keeping only those for machines it
    /// declares, so a write that removes a machine also drops its role.
    pub fn carry_machine_roles(&mut self, roles: BTreeMap<String, MachineRole>) {
        let known = self.known_profile_executor_ids();
        let kept: BTreeMap<String, MachineRole> = roles
            .into_iter()
            .filter(|(key, role)| known.contains(key.as_str()) && *role != MachineRole::General)
            .collect();
        self.machine_roles = kept;
    }

    /// Every machine with a role other than general, by executor identity.
    pub fn role_executor_ids(&self) -> Vec<(String, MachineRole)> {
        self.machine_roles
            .iter()
            .map(|(key, role)| (Self::executor_id_for_key(key), *role))
            .collect()
    }

    /// The executor identity a durable key names: the inverse of
    /// [`Self::drain_key`].
    fn executor_id_for_key(key: &str) -> String {
        if key == LOCAL_EXECUTOR_NAME {
            COLOCATED_EXECUTOR_ID.to_string()
        } else {
            key.to_string()
        }
    }

    /// Every machine this runner is holding drained, by executor identity.
    pub fn drained_executor_ids(&self) -> Vec<String> {
        self.drained_machines
            .iter()
            .map(|key| Self::executor_id_for_key(key))
            .collect()
    }

    pub fn resolve_executor_policy(
        &self,
        executor_id: &str,
    ) -> cairn_common::executor_protocol::ExecutorRuntimePolicy {
        self.executor_policies
            .get(executor_id)
            .cloned()
            .unwrap_or_else(|| cairn_common::executor_protocol::ExecutorRuntimePolicy {
                cpu_admission: self.cpu_admission,
                ..Default::default()
            })
    }

    /// Replace a wait horizon that is a leftover rather than a policy, at the
    /// one moment the config is read from disk.
    ///
    /// Healing on load rather than at each use, so that every consumer — the
    /// scheduler, the settings API, the Fleet editor the operator is looking at
    /// — sees one effective number. A value corrected only where it is spent
    /// would leave the operator reading 20 in a form whose save button then
    /// refuses it.
    ///
    /// Raising rather than refusing, because this is state that already exists
    /// on disk: refusing would take the fleet down over a number nobody chose,
    /// and honouring it would keep the fleet abandoning work in twenty seconds.
    /// The log line is how the operator learns their file says something they
    /// did not mean; [`Self::validate`] is what keeps a new one from being
    /// written, and the file itself heals on the next fleet-settings save.
    pub fn healed(mut self) -> Self {
        self.adopt_legacy_desktop_automation();
        if self.capacity_wait_horizon_seconds < MIN_CAPACITY_WAIT_HORIZON_SECONDS {
            log::warn!(
                "settings.yaml buildSlots.capacityWaitHorizonSeconds is {}, below the {MIN_CAPACITY_WAIT_HORIZON_SECONDS}s floor; \
                 using {}s instead. A value this small is usually a pre-cairn-3268 acquisitionDeadlineSeconds \
                 carried across the rename, where it meant a per-attempt queue budget rather than a total wait.",
                self.capacity_wait_horizon_seconds,
                default_capacity_wait_horizon_seconds(),
            );
            self.capacity_wait_horizon_seconds = default_capacity_wait_horizon_seconds();
        }
        self.placement_profiles
            .retain(|name, _| !is_built_in_profile(name));
        if !is_built_in_profile(&self.active_placement_profile)
            && !self
                .placement_profiles
                .contains_key(&self.active_placement_profile)
        {
            log::warn!(
                "settings.yaml names missing placement profile {}; using {DEFAULT_PLACEMENT_PROFILE}",
                self.active_placement_profile
            );
            self.active_placement_profile = DEFAULT_PLACEMENT_PROFILE.to_string();
        }
        self
    }

    /// Convert the retired `desktopAutomation` toggles into selections of
    /// [`AXON_MACHINE_SERVICE`], returning one note per thing that did not carry
    /// over exactly, or `None` when there was nothing to convert.
    ///
    /// A machine listed under `machines` whose effective `enabled` is true (its
    /// own value, else the global one) is selected; an explicit `false` is not.
    /// The global default never selects a machine that is not listed: it used to
    /// mean "every machine that has the binary", and read literally it would put
    /// a resident on every server in the fleet. An absolute `binary` becomes the
    /// machine's override; a bare name was a PATH lookup and is dropped, so the
    /// pack's managed artifact runs there instead. A selection the row already
    /// holds is kept, which makes the conversion idempotent.
    pub fn adopt_legacy_desktop_automation(&mut self) -> Option<Vec<String>> {
        let legacy = self.legacy_desktop_automation.take()?;
        let key: MachineServiceKey = AXON_MACHINE_SERVICE
            .parse()
            .expect("the axon key follows the key rule");
        let global = legacy.enabled.unwrap_or(true);
        let mut notes = Vec::new();
        for (listed, machine) in legacy.machines {
            if !machine.enabled.unwrap_or(global) {
                continue;
            }
            let Some(name) = normalize_executor_name(&listed) else {
                notes.push(format!(
                    "desktopAutomation.machines.{listed} names no public machine, so it was not converted"
                ));
                continue;
            };
            if self.machine_services.selection(&key, &name).is_some() {
                continue;
            }
            let override_ = match machine.binary {
                Some(binary) if is_absolute_command_path(&binary) => {
                    Some(CommandOverride { command: binary })
                }
                Some(binary) => {
                    notes.push(format!(
                        "{name}: dropped the bare desktop binary {binary:?}; the axon pack's managed artifact runs there now"
                    ));
                    None
                }
                None => None,
            };
            if let Err(error) =
                self.machine_services
                    .select(key.clone(), &name, MachineSelection { override_ })
            {
                notes.push(format!("{name}: not converted: {error}"));
            }
        }
        if global {
            notes.push(
                "desktopAutomation's global default selected every machine with the binary; machineServices selects only the machines listed"
                    .to_string(),
            );
        }
        Some(notes)
    }

    /// The horizon a caller with no tighter answer of its own waits on.
    pub fn capacity_wait_horizon_ms(&self) -> u64 {
        self.capacity_wait_horizon_seconds.saturating_mul(1_000)
    }

    pub fn validate(&self) -> Result<(), String> {
        self.cpu_admission.validate()?;
        self.machine_services.validate()?;
        for (executor_id, policy) in &self.executor_policies {
            policy
                .validate()
                .map_err(|error| format!("executor policy {executor_id}: {error}"))?;
        }
        if self.capacity_wait_horizon_seconds < MIN_CAPACITY_WAIT_HORIZON_SECONDS {
            return Err(format!(
                "capacityWaitHorizonSeconds must be at least {MIN_CAPACITY_WAIT_HORIZON_SECONDS}: \
                 a shorter horizon cannot outlast one ordinary unit of work finishing, so it refuses \
                 every request the machine is merely busy for"
            ));
        }
        let mut device_ids = HashSet::new();
        let mut names: HashMap<String, String> = HashMap::new();
        for (executor_id, remote) in &self.remote_executors {
            remote.validate()?;
            if executor_id != &remote.executor_id {
                return Err(format!(
                    "remote executor inventory key {executor_id} does not match executorId {}",
                    remote.executor_id
                ));
            }
            if !device_ids.insert(&remote.device_id) {
                return Err(format!(
                    "remote executors must not share device identity {}",
                    remote.device_id
                ));
            }
            let name = normalize_executor_name(&remote.display_name)
                .expect("validate() rejects a displayName with no public name");
            if let Some(existing) = names.insert(name.clone(), remote.executor_id.clone()) {
                return Err(format!(
                    "remote executors {existing} and {} both address the public name {name}; rename one with `cairn executor rename`",
                    remote.executor_id
                ));
            }
        }
        if !is_built_in_profile(&self.active_placement_profile)
            && !self
                .placement_profiles
                .contains_key(&self.active_placement_profile)
        {
            return Err(format!(
                "active placement profile {} does not exist",
                self.active_placement_profile
            ));
        }
        let known = self.known_profile_executor_ids();
        for (name, profile) in &self.placement_profiles {
            validate_custom_profile_name(name)?;
            profile.validate(&known)?;
        }
        for (key, role) in &self.machine_roles {
            if !known.contains(key.as_str()) {
                return Err(format!(
                    "machineRoles names unknown executor ID {key} (role {})",
                    role.as_str()
                ));
            }
        }
        Ok(())
    }
}

impl Default for FleetConfig {
    fn default() -> Self {
        Self {
            active_placement_profile: default_active_placement_profile(),
            placement_profiles: BTreeMap::new(),
            capacity_wait_horizon_seconds: default_capacity_wait_horizon_seconds(),
            default_timeout_seconds: default_timeout_seconds(),
            cpu_admission: Default::default(),
            executor_policies: HashMap::new(),
            remote_executors: BTreeMap::new(),
            remote_host_identities: BTreeMap::new(),
            machine_services: MachineServicesConfig::default(),
            legacy_desktop_automation: None,
            executor_listener: None,
            drained_machines: BTreeSet::new(),
            machine_roles: BTreeMap::new(),
            colocated_build_root: None,
        }
    }
}

fn default_capacity_wait_horizon_seconds() -> u64 {
    10 * 60
}

/// The shortest wait horizon that can be a policy rather than a leftover.
///
/// Below this the number stops describing patience and starts guaranteeing
/// refusal: the fleet's own placement, provisioning, and admission round trip
/// takes seconds on an idle machine, so a horizon under a minute cannot outlast
/// even one ordinary unit of work finishing. Every caller that reaches this
/// default is one with no tighter answer of its own — a terminal, a REPL, a dev
/// instance — and none of them means "give up before anything could plausibly
/// free".
///
/// It exists because a persisted twenty, carried across CAIRN-3268's rename
/// from a field that meant something else, is indistinguishable afterwards from
/// a number an operator typed. This floor is what tells them apart, and
/// [`FleetConfig::validate`] is what stops a new one being written.
pub const MIN_CAPACITY_WAIT_HORIZON_SECONDS: u64 = 60;
fn default_timeout_seconds() -> u64 {
    // Cold Rust builds in managed cells routinely cross ten minutes. Thirty
    // minutes clears that ordinary floor while keeping setup, residency, and
    // check infrastructure failures bounded and operator-configurable.
    30 * 60
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_machine_roles_block_is_every_machine_general() {
        let config: FleetConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(config.machine_roles.is_empty());
        assert_eq!(
            config.machine_role(COLOCATED_EXECUTOR_ID),
            MachineRole::General
        );
        assert_eq!(config.machine_role("sp1"), MachineRole::General);
        let serialized = serde_json::to_value(&config).unwrap();
        assert!(
            serialized.get("machineRoles").is_none(),
            "a fleet with no roles writes none"
        );
    }

    #[test]
    fn machine_roles_are_keyed_like_drains_and_general_is_absence() {
        let mut config = FleetConfig::default();
        assert!(config.set_machine_role(COLOCATED_EXECUTOR_ID, MachineRole::BuildsOnly));
        assert!(!config.set_machine_role(COLOCATED_EXECUTOR_ID, MachineRole::BuildsOnly));
        assert_eq!(
            config.machine_roles.get(LOCAL_EXECUTOR_NAME),
            Some(&MachineRole::BuildsOnly),
            "the colocated executor is keyed by the name that addresses it"
        );
        assert_eq!(
            config.role_executor_ids(),
            vec![(COLOCATED_EXECUTOR_ID.to_string(), MachineRole::BuildsOnly)]
        );
        assert!(config.set_machine_role(COLOCATED_EXECUTOR_ID, MachineRole::General));
        assert!(
            config.machine_roles.is_empty(),
            "general is stored as absence"
        );
        assert!(!config.set_machine_role(COLOCATED_EXECUTOR_ID, MachineRole::General));
    }

    #[test]
    fn forgetting_a_machine_drops_its_drain_and_its_role() {
        let mut config = FleetConfig::default();
        config.set_machine_drained("sp1", true);
        config.set_machine_role("sp1", MachineRole::InferenceOnly);
        config.set_machine_role(COLOCATED_EXECUTOR_ID, MachineRole::BuildsOnly);
        config.forget_machine("sp1");
        assert!(!config.is_machine_drained("sp1"));
        assert_eq!(config.machine_role("sp1"), MachineRole::General);
        assert_eq!(
            config.machine_role(COLOCATED_EXECUTOR_ID),
            MachineRole::BuildsOnly,
            "only the forgotten machine loses its facts"
        );
    }

    #[test]
    fn validate_refuses_a_role_for_an_unknown_machine() {
        let mut config = FleetConfig::default();
        config.set_machine_role(COLOCATED_EXECUTOR_ID, MachineRole::BuildsOnly);
        assert_eq!(config.validate(), Ok(()));
        config.set_machine_role("sp1", MachineRole::InferenceOnly);
        let error = config.validate().unwrap_err();
        assert!(error.contains("unknown executor ID sp1"), "{error}");
    }
    fn axon() -> MachineServiceKey {
        "axon/axon".parse().unwrap()
    }

    #[test]
    fn the_machine_services_row_round_trips_through_yaml() {
        let yaml = r#"
machineServices:
  axon/axon:
    machines:
      bgsrv-mac: {}
      local:
        override:
          command: /Applications/Axon.app/Contents/Resources/bin/axon
"#;
        let config: FleetConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.validate(), Ok(()));
        assert_eq!(
            config
                .machine_services
                .selection(&axon(), "local")
                .and_then(MachineSelection::override_command),
            Some("/Applications/Axon.app/Contents/Resources/bin/axon")
        );
        assert_eq!(
            config.machine_services.selection(&axon(), "bgsrv-mac"),
            Some(&MachineSelection::default())
        );
        let reparsed: FleetConfig =
            serde_yaml::from_str(&serde_yaml::to_string(&config).unwrap()).unwrap();
        assert_eq!(reparsed.machine_services, config.machine_services);
        let empty = serde_json::to_value(FleetConfig::default()).unwrap();
        assert!(
            empty.get("machineServices").is_none(),
            "a fleet with no selections writes no row"
        );
    }

    /// A selection is keyed by the name a machine answers to, so the two
    /// operations that change which names exist have to move it, for every
    /// service at once.
    #[test]
    fn selections_follow_a_machine_through_rename_and_removal() {
        let other: MachineServiceKey = "imsg/watch".parse().unwrap();
        let mut row = MachineServicesConfig::default();
        row.select(
            axon(),
            "BGLab Win",
            MachineSelection {
                override_: Some(CommandOverride {
                    command: r"C:\axon\axon-win.exe".into(),
                }),
            },
        )
        .unwrap();
        row.select(other.clone(), "bglab-win", MachineSelection::default())
            .unwrap();
        row.select(axon(), "local", MachineSelection::default())
            .unwrap();

        row.rename_machine("BGLab Win", "bglab-windows");
        assert_eq!(
            row.selection(&axon(), "bglab-windows")
                .and_then(MachineSelection::override_command),
            Some(r"C:\axon\axon-win.exe"),
            "a renamed machine keeps its override"
        );
        assert!(row.selection(&other, "bglab-windows").is_some());
        assert!(
            row.selected_on("bglab-win").is_empty(),
            "the old name is not left behind addressing nothing"
        );

        row.forget_machine("bglab-windows");
        assert!(row.selected_on("bglab-windows").is_empty());
        assert!(
            !row.0.contains_key(&other),
            "a service selected nowhere leaves the row"
        );
        assert_eq!(
            row.machines_for(&axon()),
            vec![("local", &MachineSelection::default())],
            "only the forgotten machine loses its selections"
        );
    }

    #[test]
    fn an_override_must_be_an_absolute_path() {
        let mut row = MachineServicesConfig::default();
        for bare in ["axon", "bin/axon", " /abs/with/space", ""] {
            let error = row
                .select(
                    axon(),
                    "local",
                    MachineSelection {
                        override_: Some(CommandOverride {
                            command: bare.into(),
                        }),
                    },
                )
                .unwrap_err();
            assert!(error.contains("must be an absolute path"), "{error}");
        }
        for absolute in [
            "/opt/axon",
            r"C:\Axon\axon.exe",
            "D:/axon.exe",
            r"\\share\axon.exe",
        ] {
            assert!(is_absolute_command_path(absolute), "{absolute}");
        }
        let yaml = "machineServices:\n  axon/axon:\n    machines:\n      local:\n        override: {command: axon}\n";
        let config: FleetConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().unwrap_err().contains("absolute path"));
    }

    #[test]
    fn an_enabled_key_and_a_malformed_service_key_are_refused() {
        let enabled =
            "machineServices:\n  axon/axon:\n    machines:\n      local: {enabled: true}\n";
        let error = serde_yaml::from_str::<FleetConfig>(enabled)
            .unwrap_err()
            .to_string();
        assert!(error.contains("enabled"), "{error}");
        let service_enabled = "machineServices:\n  axon/axon:\n    enabled: true\n";
        assert!(serde_yaml::from_str::<FleetConfig>(service_enabled).is_err());

        for malformed in [
            "axon",
            "Axon/axon",
            "axon/",
            "axon/axon/x",
            "1x/axon",
            "axon/a_b",
        ] {
            let yaml = format!("machineServices:\n  {malformed}:\n    machines: {{}}\n");
            let error = serde_yaml::from_str::<FleetConfig>(&yaml)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("not a machine service"),
                "{malformed}: {error}"
            );
        }
        assert!(MachineServiceKey::new(&"a".repeat(63), "b").is_ok());
        assert!(MachineServiceKey::new(&"a".repeat(64), "b").is_err());
    }

    /// The operator's settings, byte for byte as they stood before the row.
    const OPERATOR_DESKTOP_AUTOMATION: &str = r#"
desktopAutomation:
  enabled: true
  machines:
    local:
      binary: /Applications/Axon.app/Contents/Resources/bin/axon
    bgsrv-mac:
      enabled: true
executorListener:
  bind: 0.0.0.0:3846
drainedMachines:
- bgsrv-win
"#;

    #[test]
    fn the_operators_desktop_toggles_become_exactly_two_selections() {
        let mut config: FleetConfig = serde_yaml::from_str(OPERATOR_DESKTOP_AUTOMATION).unwrap();
        let notes = config.adopt_legacy_desktop_automation().unwrap();
        let expected: MachineServicesConfig = serde_yaml::from_str(
            "axon/axon:\n  machines:\n    bgsrv-mac: {}\n    local:\n      override:\n        command: /Applications/Axon.app/Contents/Resources/bin/axon\n",
        )
        .unwrap();
        assert_eq!(config.machine_services, expected);
        assert!(
            notes
                .iter()
                .any(|note| note.contains("only the machines listed")),
            "{notes:?}"
        );
        assert!(
            config.adopt_legacy_desktop_automation().is_none(),
            "the conversion runs once"
        );

        let written = serde_yaml::to_string(&config).unwrap();
        assert!(!written.contains("desktopAutomation"), "{written}");
        let reread: FleetConfig = serde_yaml::from_str::<FleetConfig>(&written)
            .unwrap()
            .healed();
        assert_eq!(
            reread.machine_services, expected,
            "rereading the written block changes nothing"
        );
        assert!(reread.is_machine_drained("bgsrv-win"));
    }

    #[test]
    fn every_typed_read_converts_so_a_snapshot_write_cannot_drop_the_row() {
        let healed = serde_yaml::from_str::<FleetConfig>(OPERATOR_DESKTOP_AUTOMATION)
            .unwrap()
            .healed();
        assert_eq!(healed.machine_services.machines_for(&axon()).len(), 2);
    }

    #[test]
    fn a_listed_machine_turned_on_is_selected_even_when_the_global_is_off() {
        let yaml = r#"
desktopAutomation:
  enabled: false
  machines:
    bglab-win:
      enabled: true
      binary: C:\Axon\axon-win.exe
    bglab-ub:
      binary: axon-linux
    bglab-mac:
      enabled: false
"#;
        let mut config: FleetConfig = serde_yaml::from_str(yaml).unwrap();
        let notes = config.adopt_legacy_desktop_automation().unwrap();
        assert_eq!(
            config.machine_services.machines_for(&axon()),
            vec![(
                "bglab-win",
                &MachineSelection {
                    override_: Some(CommandOverride {
                        command: r"C:\Axon\axon-win.exe".into()
                    })
                }
            )],
            "an inherited false and an explicit false both select nothing"
        );
        assert!(notes.is_empty(), "{notes:?}");
    }

    #[test]
    fn a_bare_binary_is_dropped_with_a_note_and_an_existing_selection_is_kept() {
        let yaml = r#"
machineServices:
  axon/axon:
    machines:
      bglab-ub:
        override: {command: /opt/axon/axon-linux}
desktopAutomation:
  machines:
    bglab-ub:
      binary: /usr/local/bin/other
    bgsrv-ub:
      binary: axon-linux
"#;
        let mut config: FleetConfig = serde_yaml::from_str(yaml).unwrap();
        let notes = config.adopt_legacy_desktop_automation().unwrap();
        assert_eq!(
            config
                .machine_services
                .selection(&axon(), "bglab-ub")
                .and_then(MachineSelection::override_command),
            Some("/opt/axon/axon-linux"),
            "the row's own selection wins"
        );
        assert_eq!(
            config.machine_services.selection(&axon(), "bgsrv-ub"),
            Some(&MachineSelection::default())
        );
        assert!(
            notes
                .iter()
                .any(|note| note.contains("bare desktop binary")),
            "{notes:?}"
        );
    }

    #[test]
    fn a_selection_under_an_unnormalized_machine_name_is_refused() {
        let yaml = "machineServices:\n  axon/axon:\n    machines:\n      BGLab Win: {}\n";
        let config: FleetConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config
            .validate()
            .unwrap_err()
            .contains("not a public executor name"));
    }
}
