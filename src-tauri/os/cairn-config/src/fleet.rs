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
    #[serde(default)]
    pub desktop_automation: DesktopAutomationConfig,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DesktopAutomationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub machines: HashMap<String, MachineDesktopAutomation>,
}

impl Default for DesktopAutomationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            machines: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MachineDesktopAutomation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDesktopAutomation {
    pub enabled: bool,
    pub binary: String,
}

fn default_true() -> bool {
    true
}

impl DesktopAutomationConfig {
    /// Carry a machine's desktop configuration with it when it is renamed.
    ///
    /// This map is keyed by public name rather than by executor identity,
    /// because the colocated machine has no remote declaration to hang it off.
    /// That makes a rename a move: leaving the entry under the old name would
    /// silently strip the machine of the binary it was configured with, and the
    /// only symptom would be desktop automation quietly resolving the platform
    /// default instead.
    pub fn rename_machine(&mut self, from: &str, to: &str) {
        let (Some(from), Some(to)) = (normalize_executor_name(from), normalize_executor_name(to))
        else {
            return;
        };
        if from == to {
            return;
        }
        if let Some(machine) = self.machines.remove(&from) {
            self.machines.insert(to, machine);
        }
    }

    /// Drop a removed machine's desktop configuration along with it, so removal
    /// leaves no entry addressing a machine that no longer exists.
    pub fn forget_machine(&mut self, name: &str) {
        if let Some(name) = normalize_executor_name(name) {
            self.machines.remove(&name);
        }
    }

    pub fn resolve(&self, machine_name: &str, os: &str) -> ResolvedDesktopAutomation {
        let machine =
            normalize_executor_name(machine_name).and_then(|name| self.machines.get(&name));
        ResolvedDesktopAutomation {
            enabled: machine
                .and_then(|config| config.enabled)
                .unwrap_or(self.enabled),
            binary: machine
                .and_then(|config| config.binary.clone())
                .unwrap_or_else(|| match os {
                    "windows" => "axon-win.exe".to_string(),
                    "linux" => "axon-linux".to_string(),
                    _ => "axon".to_string(),
                }),
        }
    }
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

    /// The horizon a caller with no tighter answer of its own waits on.
    pub fn capacity_wait_horizon_ms(&self) -> u64 {
        self.capacity_wait_horizon_seconds.saturating_mul(1_000)
    }

    pub fn validate(&self) -> Result<(), String> {
        self.cpu_admission.validate()?;
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
            desktop_automation: DesktopAutomationConfig::default(),
            executor_listener: None,
            drained_machines: BTreeSet::new(),
            machine_roles: BTreeMap::new(),
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
    /// A machine's desktop configuration is keyed by the name it answers to, so
    /// the two operations that change which names exist have to move it.
    #[test]
    fn desktop_configuration_follows_a_machine_through_rename_and_removal() {
        let mut config = DesktopAutomationConfig {
            enabled: true,
            machines: HashMap::from([(
                "bglab-win".to_string(),
                MachineDesktopAutomation {
                    enabled: None,
                    binary: Some(r"C:\axon\axon-win.exe".to_string()),
                },
            )]),
        };

        config.rename_machine("BGLab Win", "bglab-windows");
        assert_eq!(
            config.resolve("bglab-windows", "windows").binary,
            r"C:\axon\axon-win.exe",
            "a renamed machine keeps the binary it was configured with"
        );
        assert!(
            !config.machines.contains_key("bglab-win"),
            "the old name is not left behind addressing nothing"
        );

        config.forget_machine("bglab-windows");
        assert!(config.machines.is_empty());
        assert_eq!(
            config.resolve("bglab-windows", "windows").binary,
            "axon-win.exe",
            "a forgotten machine falls back to the platform default"
        );
    }
    #[test]
    fn desktop_automation_resolves_inheritance_binary_and_normalized_machine_name() {
        let mut config = DesktopAutomationConfig {
            enabled: false,
            ..Default::default()
        };
        config.machines.insert(
            "bglab-win".into(),
            MachineDesktopAutomation {
                enabled: Some(true),
                binary: Some(r"C:\\Axon\\axon.exe".into()),
            },
        );

        assert_eq!(
            config.resolve("BGLAB WIN", "windows"),
            ResolvedDesktopAutomation {
                enabled: true,
                binary: r"C:\\Axon\\axon.exe".into(),
            }
        );
        assert_eq!(
            DesktopAutomationConfig::default().resolve("local", "linux"),
            ResolvedDesktopAutomation {
                enabled: true,
                binary: "axon-linux".into(),
            }
        );
    }
}
