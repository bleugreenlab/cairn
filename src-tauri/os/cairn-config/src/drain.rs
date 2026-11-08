//! Runner-owned admission holds. Authentication belongs to callers; mutations
//! require already verified principals and run inside settings::mutate_fleet.
use std::collections::BTreeMap;

use cairn_common::identity::{PrincipalPosition, PrincipalRef};
use serde::{Deserialize, Serialize};

use crate::fleet::FleetConfig;

pub const LEGACY_DRAIN_REASON: &str = "imposed before ownership was recorded";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionEffect {
    /// Every new admission is refused, residency-pinned batches and terminals
    /// included. Operator, thread, legacy and removal-fence holds have this effect.
    RefuseAllNewAdmission,
    /// Only work that would create a new environment on the machine is refused:
    /// a batch or terminal of a job already resident there is admitted and runs
    /// on the outgoing build. The self-update drain has this effect, and alone it
    /// never sets the executor's own `DrainModeRequest` refusal.
    RefuseNewEnvironments,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeDrainKind {
    SelfUpdate,
    Quiesce,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeDrainContext {
    pub kind: RuntimeDrainKind,
    pub operation_id: String,
    pub turn_id: String,
    pub runner_boot_id: String,
    pub admission_effect: AdmissionEffect,
}

impl RuntimeDrainContext {
    fn validate(&self) -> Result<(), String> {
        nonempty(&self.operation_id, "runtime operation")?;
        nonempty(&self.turn_id, "runtime turn")?;
        nonempty(&self.runner_boot_id, "runner boot")?;
        // The self-update drain has exactly one effect; a record claiming another
        // would change what resident work may do and fails closed instead.
        if self.kind == RuntimeDrainKind::SelfUpdate
            && self.admission_effect != AdmissionEffect::RefuseNewEnvironments
        {
            return Err("a self-update drain refuses new environments only".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DrainOwner {
    Operator,
    Thread { thread_id: String },
    Runtime { context: RuntimeDrainContext },
    Legacy,
}

impl DrainOwner {
    fn key(&self) -> String {
        match self {
            Self::Operator => "operator".into(),
            Self::Thread { thread_id } => format!("thread:{thread_id}"),
            // A JSON tuple avoids collisions between punctuation in identifiers.
            Self::Runtime { context } => format!(
                "runtime:{}",
                serde_json::to_string(context).expect("context serializes")
            ),
            Self::Legacy => "legacy".into(),
        }
    }

    fn validate(&self) -> Result<(), String> {
        match self {
            Self::Thread { thread_id } => nonempty(thread_id, "thread ID"),
            Self::Runtime { context } => context.validate(),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DrainProvenance {
    Attributed {
        imposed_by: PrincipalRef,
        imposed_at_unix_ms: i64,
    },
    LegacyUnknown,
}

// Records decode at the durable boundary; callers cannot construct them through
// struct literals and insert them through an untyped mutation API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[non_exhaustive]
pub struct DrainRecord {
    pub owner: DrainOwner,
    pub reason: String,
    pub generation: u64,
    pub provenance: DrainProvenance,
}

impl DrainRecord {
    /// What this record refuses. Only a runtime record names its own effect;
    /// every other owner refuses all new admission.
    pub fn admission_effect(&self) -> AdmissionEffect {
        match &self.owner {
            DrainOwner::Runtime { context } => context.admission_effect.clone(),
            DrainOwner::Operator | DrainOwner::Thread { .. } | DrainOwner::Legacy => {
                AdmissionEffect::RefuseAllNewAdmission
            }
        }
    }

    fn validate(&self) -> Result<(), String> {
        self.owner.validate()?;
        nonempty(&self.reason, "drain reason")?;
        if self.generation == 0 {
            return Err("drain generation must be positive".into());
        }
        match (&self.owner, &self.provenance) {
            (DrainOwner::Legacy, DrainProvenance::LegacyUnknown)
                if self.reason == LEGACY_DRAIN_REASON && self.generation == 1 =>
            {
                Ok(())
            }
            (DrainOwner::Legacy, _) | (_, DrainProvenance::LegacyUnknown) => {
                Err("invalid legacy drain provenance".into())
            }
            (
                _,
                DrainProvenance::Attributed {
                    imposed_by,
                    imposed_at_unix_ms,
                },
            ) => {
                imposed_by
                    .validate_at(PrincipalPosition::AppearancePrincipal)
                    .map_err(|e| e.to_string())?;
                if *imposed_at_unix_ms < 0 {
                    return Err("drain timestamp must be nonnegative".into());
                }
                Ok(())
            }
        }
    }

    pub fn imposed_by(&self) -> Option<&PrincipalRef> {
        match &self.provenance {
            DrainProvenance::Attributed { imposed_by, .. } => Some(imposed_by),
            DrainProvenance::LegacyUnknown => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeDrainToken {
    pub executor_id: String,
    pub drain_key: String,
    pub context: RuntimeDrainContext,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FrozenRemoval {
    pub enrollment_device_id: String,
    pub executor_id: String,
    pub drain_key: String,
    pub machine_revision: u64,
    pub acting_principal: PrincipalRef,
    pub removed: Vec<DrainRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemovalApplication {
    pub install_id: String,
    pub operation_id: String,
    /// Reference to credential verification evidence, never credential material.
    pub provenance: String,
    pub frozen: FrozenRemoval,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemovalOutcome {
    EnrollmentRevoked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemovalReceipt {
    pub application: RemovalApplication,
    pub outcome: RemovalOutcome,
    pub before_revision: u64,
    pub after_revision: u64,
}

/// Frozen, checked authorization supplied by the registered request consumer.
/// Config does not accept or claim requests; this binds their decisive evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LiftApplication {
    pub install_id: String,
    pub workspace_id: String,
    pub authorization_event_id: String,
    pub attempt_id: String,
    pub claim_id: String,
    pub actor: String,
    pub kind: String,
    pub executor_id: String,
    pub drain_key: String,
    pub accepted_owner: DrainOwner,
    pub operator_generation: u64,
    pub machine_revision: u64,
    pub report_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiftOutcome {
    Cleared,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LiftReceipt {
    pub application: LiftApplication,
    pub before_revision: u64,
    pub after_revision: u64,
    pub outcome: LiftOutcome,
    /// Owner keys and generations still standing when this application
    /// committed (`owner@generation`), frozen so the report never depends on
    /// later live state.
    pub remaining_owners: Vec<String>,
    /// Deterministic report evidence, independent of later live state.
    pub report_body: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MachineDrains {
    machine_revision: u64,
    records: BTreeMap<String, DrainRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DrainStateWire {
    version: u32,
    machines: BTreeMap<String, MachineDrains>,
    lift_receipts: BTreeMap<String, LiftReceipt>,
    removal_receipts: BTreeMap<String, RemovalReceipt>,
}

/// Maps and receipts are immutable to callers. Replacements go through exact
/// typed operations, preserving retained counters even after enrollment removal.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "DrainStateWire", into = "DrainStateWire")]
pub struct DrainState {
    machines: BTreeMap<String, MachineDrains>,
    lift_receipts: BTreeMap<String, LiftReceipt>,
    removal_receipts: BTreeMap<String, RemovalReceipt>,
}

impl From<DrainState> for DrainStateWire {
    fn from(state: DrainState) -> Self {
        Self {
            version: 1,
            machines: state.machines,
            lift_receipts: state.lift_receipts,
            removal_receipts: state.removal_receipts,
        }
    }
}

impl TryFrom<DrainStateWire> for DrainState {
    type Error = String;
    fn try_from(wire: DrainStateWire) -> Result<Self, String> {
        if wire.version != 1 {
            return Err(format!("unsupported drain state version {}", wire.version));
        }
        let state = Self {
            machines: wire.machines,
            lift_receipts: wire.lift_receipts,
            removal_receipts: wire.removal_receipts,
        };
        state.validate()?;
        Ok(state)
    }
}

fn nonempty(value: &str, field: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("{field} must be nonempty"))
    } else {
        Ok(())
    }
}

fn identity(executor_id: &str, key: &str) -> Result<(), String> {
    nonempty(executor_id, "executor ID")?;
    if key != FleetConfig::drain_key(executor_id) {
        return Err("drain key does not match stable executor identity".into());
    }
    Ok(())
}

impl LiftApplication {
    fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("install", self.install_id.as_str()),
            ("workspace", &self.workspace_id),
            ("authorization event", &self.authorization_event_id),
            ("attempt", &self.attempt_id),
            ("claim", &self.claim_id),
            ("report", &self.report_id),
        ] {
            nonempty(value, field)?;
        }
        identity(&self.executor_id, &self.drain_key)?;
        if self.actor != "execution.fleet"
            || self.kind != "lift_drain"
            || self.accepted_owner != DrainOwner::Operator
        {
            return Err(
                "lift authorization must name execution.fleet/lift_drain and operator owner".into(),
            );
        }
        if self.operator_generation == 0 || self.operator_generation > self.machine_revision {
            return Err("invalid accepted drain generations".into());
        }
        Ok(())
    }
}

impl RemovalApplication {
    fn validate(&self) -> Result<(), String> {
        nonempty(&self.install_id, "install")?;
        nonempty(&self.operation_id, "removal operation")?;
        nonempty(&self.provenance, "credential verification provenance")?;
        nonempty(
            &self.frozen.enrollment_device_id,
            "frozen enrollment device identity",
        )?;
        identity(&self.frozen.executor_id, &self.frozen.drain_key)?;
        self.frozen
            .acting_principal
            .validate_at(PrincipalPosition::AppearancePrincipal)
            .map_err(|e| e.to_string())?;
        let mut previous = None;
        for record in &self.frozen.removed {
            let owner_key = record.owner.key();
            if previous.as_ref().is_some_and(|key| key >= &owner_key) {
                return Err("removal records must have unique owners in canonical order".into());
            }
            previous = Some(owner_key);
            record.validate()?;
            if record.generation > self.frozen.machine_revision {
                return Err("removal generation exceeds revision".into());
            }
            if !matches!(record.owner, DrainOwner::Legacy)
                && !(record.owner == DrainOwner::Operator
                    && record.imposed_by() == Some(&self.frozen.acting_principal))
            {
                return Err("removal cannot clear another owner's drain".into());
            }
        }
        Ok(())
    }
}

impl DrainState {
    pub fn validate(&self) -> Result<(), String> {
        for (key, machine) in &self.machines {
            nonempty(key, "drain machine key")?;
            if key == crate::fleet::COLOCATED_EXECUTOR_ID {
                return Err("colocated drain key must be local".into());
            }
            let mut generations = std::collections::BTreeSet::new();
            for (owner_key, record) in &machine.records {
                record.validate()?;
                if owner_key != &record.owner.key()
                    || record.generation > machine.machine_revision
                    || !generations.insert(record.generation)
                {
                    return Err("drain owner key or generation is inconsistent".into());
                }
            }
        }
        for (attempt, receipt) in &self.lift_receipts {
            receipt.application.validate()?;
            let key = &receipt.application.drain_key;
            if attempt != &receipt.application.attempt_id
                || self.revision_for_key(key) < receipt.after_revision
            {
                return Err("lift receipt index or retained revision is inconsistent".into());
            }
            let expected = if receipt.outcome == LiftOutcome::Cleared {
                receipt.before_revision.checked_add(1)
            } else {
                Some(receipt.before_revision)
            };
            if (receipt.outcome == LiftOutcome::Cleared
                && receipt.before_revision != receipt.application.machine_revision)
                || expected != Some(receipt.after_revision)
                || receipt.report_body
                    != lift_report_body(
                        &receipt.outcome,
                        receipt.after_revision,
                        &receipt.remaining_owners,
                    )
            {
                return Err("lift receipt outcome is inconsistent".into());
            }
        }
        for (operation, receipt) in &self.removal_receipts {
            receipt.application.validate()?;
            let frozen = &receipt.application.frozen;
            let expected = if frozen.removed.is_empty() {
                Some(receipt.before_revision)
            } else {
                receipt.before_revision.checked_add(1)
            };
            if operation != &receipt.application.operation_id
                || receipt.before_revision != frozen.machine_revision
                || expected != Some(receipt.after_revision)
                || self.revision_for_key(&frozen.drain_key) < receipt.after_revision
            {
                return Err("removal receipt index or revisions are inconsistent".into());
            }
        }
        Ok(())
    }

    /// Migration-only, reachable solely from the durable settings decoder.
    pub(crate) fn import_legacy(keys: Vec<String>) -> Result<Self, String> {
        let mut state = Self::default();
        for key in keys {
            nonempty(&key, "legacy drain key")?;
            let record = DrainRecord {
                owner: DrainOwner::Legacy,
                reason: LEGACY_DRAIN_REASON.into(),
                generation: 1,
                provenance: DrainProvenance::LegacyUnknown,
            };
            if state
                .machines
                .insert(
                    key,
                    MachineDrains {
                        machine_revision: 1,
                        records: [(record.owner.key(), record)].into(),
                    },
                )
                .is_some()
            {
                return Err("duplicate legacy drain key".into());
            }
        }
        state.validate()?;
        Ok(state)
    }

    fn revision_for_key(&self, key: &str) -> u64 {
        self.machines.get(key).map_or(0, |m| m.machine_revision)
    }

    pub fn machine_revision(&self, executor_id: &str) -> u64 {
        self.revision_for_key(FleetConfig::drain_key(executor_id))
    }

    pub fn records(&self, executor_id: &str) -> Vec<DrainRecord> {
        self.machines
            .get(FleetConfig::drain_key(executor_id))
            .map(|m| m.records.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn records_by_key(&self) -> Vec<(String, Vec<DrainRecord>)> {
        self.machines
            .iter()
            .filter(|(_, m)| !m.records.is_empty())
            .map(|(key, machine)| (key.clone(), machine.records.values().cloned().collect()))
            .collect()
    }

    pub fn lift_receipts(&self) -> &BTreeMap<String, LiftReceipt> {
        &self.lift_receipts
    }
    pub fn removal_receipts(&self) -> &BTreeMap<String, RemovalReceipt> {
        &self.removal_receipts
    }

    fn impose(
        &mut self,
        executor_id: &str,
        owner: DrainOwner,
        principal: &PrincipalRef,
        reason: &str,
        at: i64,
    ) -> Result<bool, String> {
        identity(executor_id, FleetConfig::drain_key(executor_id))?;
        owner.validate()?;
        principal
            .validate_at(PrincipalPosition::AppearancePrincipal)
            .map_err(|e| e.to_string())?;
        nonempty(reason, "drain reason")?;
        if at < 0 {
            return Err("drain timestamp must be nonnegative".into());
        }
        let key = FleetConfig::drain_key(executor_id);
        let owner_key = owner.key();
        if let Some(record) = self
            .machines
            .get(key)
            .and_then(|m| m.records.get(&owner_key))
        {
            if owner == DrainOwner::Operator && record.imposed_by() != Some(principal) {
                return Err(
                    "standing operator drain belongs to another principal; accept lift_drain first"
                        .into(),
                );
            }
            // The observation time of a retry is not a semantic replacement.
            if record.reason == reason && record.imposed_by() == Some(principal) {
                return Ok(false);
            }
        }
        let revision = self
            .revision_for_key(key)
            .checked_add(1)
            .ok_or("drain machine revision overflow")?;
        let record = DrainRecord {
            owner,
            reason: reason.into(),
            generation: revision,
            provenance: DrainProvenance::Attributed {
                imposed_by: principal.clone(),
                imposed_at_unix_ms: at,
            },
        };
        let machine = self.machines.entry(key.into()).or_default();
        machine.machine_revision = revision;
        machine.records.insert(owner_key, record);
        Ok(true)
    }

    pub fn impose_operator(
        &mut self,
        executor_id: &str,
        principal: &PrincipalRef,
        reason: &str,
        at: i64,
    ) -> Result<bool, String> {
        self.impose(executor_id, DrainOwner::Operator, principal, reason, at)
    }

    pub fn impose_thread(
        &mut self,
        executor_id: &str,
        thread_id: &str,
        principal: &PrincipalRef,
        reason: &str,
        at: i64,
    ) -> Result<bool, String> {
        self.impose(
            executor_id,
            DrainOwner::Thread {
                thread_id: thread_id.into(),
            },
            principal,
            reason,
            at,
        )
    }

    pub fn impose_runtime(
        &mut self,
        executor_id: &str,
        context: RuntimeDrainContext,
        principal: &PrincipalRef,
        reason: &str,
        at: i64,
    ) -> Result<RuntimeDrainToken, String> {
        self.impose(
            executor_id,
            DrainOwner::Runtime {
                context: context.clone(),
            },
            principal,
            reason,
            at,
        )?;
        let key = FleetConfig::drain_key(executor_id);
        let generation = self.machines[key].records[&DrainOwner::Runtime {
            context: context.clone(),
        }
        .key()]
            .generation;
        Ok(RuntimeDrainToken {
            executor_id: executor_id.into(),
            drain_key: key.into(),
            context,
            generation,
        })
    }

    fn clear_keys(&mut self, executor_id: &str, keys: &[String]) -> Result<bool, String> {
        nonempty(executor_id, "executor ID")?;
        let key = FleetConfig::drain_key(executor_id);
        let Some(machine) = self.machines.get_mut(key) else {
            return Ok(false);
        };
        if !keys.iter().any(|k| machine.records.contains_key(k)) {
            return Ok(false);
        }
        let revision = machine
            .machine_revision
            .checked_add(1)
            .ok_or("drain machine revision overflow")?;
        for key in keys {
            machine.records.remove(key);
        }
        machine.machine_revision = revision;
        Ok(true)
    }

    pub fn clear_thread(&mut self, executor_id: &str, thread_id: &str) -> Result<bool, String> {
        nonempty(thread_id, "thread ID")?;
        self.clear_keys(
            executor_id,
            &[
                DrainOwner::Thread {
                    thread_id: thread_id.into(),
                }
                .key(),
                DrainOwner::Legacy.key(),
            ],
        )
    }

    pub fn clear_legacy_operator(&mut self, executor_id: &str) -> Result<bool, String> {
        self.clear_keys(executor_id, &[DrainOwner::Legacy.key()])
    }

    pub fn release_runtime(&mut self, token: &RuntimeDrainToken) -> Result<bool, String> {
        identity(&token.executor_id, &token.drain_key)?;
        token.context.validate()?;
        let owner_key = DrainOwner::Runtime {
            context: token.context.clone(),
        }
        .key();
        if self
            .machines
            .get(&token.drain_key)
            .and_then(|m| m.records.get(&owner_key))
            .is_none_or(|r| r.generation != token.generation)
        {
            return Ok(false);
        }
        self.clear_keys(&token.executor_id, &[owner_key])
    }

    pub(crate) fn prepare_removal_records(
        &self,
        executor_id: &str,
        principal: &PrincipalRef,
        enrollment_device_id: &str,
    ) -> Result<FrozenRemoval, String> {
        nonempty(enrollment_device_id, "enrollment device identity")?;
        identity(executor_id, FleetConfig::drain_key(executor_id))?;
        principal
            .validate_at(PrincipalPosition::AppearancePrincipal)
            .map_err(|e| e.to_string())?;
        let removed = self.records(executor_id);
        for record in &removed {
            if matches!(record.owner, DrainOwner::Legacy) {
                continue;
            }
            if record.owner == DrainOwner::Operator && record.imposed_by() == Some(principal) {
                continue;
            }
            return Err(format!("removal blocked by {:?} generation {}; clear through its exact owner path (operator: accepted lift_drain)", record.owner, record.generation));
        }
        Ok(FrozenRemoval {
            enrollment_device_id: enrollment_device_id.into(),
            executor_id: executor_id.into(),
            drain_key: FleetConfig::drain_key(executor_id).into(),
            machine_revision: self.machine_revision(executor_id),
            acting_principal: principal.clone(),
            removed,
        })
    }

    pub(crate) fn apply_removal(
        &mut self,
        application: RemovalApplication,
    ) -> Result<RemovalReceipt, String> {
        application.validate()?;
        if let Some(receipt) = self.removal_receipts.get(&application.operation_id) {
            return if receipt.application == application {
                Ok(receipt.clone())
            } else {
                Err("removal operation identity collision".into())
            };
        }
        let frozen = &application.frozen;
        let current = self.prepare_removal_records(
            &frozen.executor_id,
            &frozen.acting_principal,
            &frozen.enrollment_device_id,
        )?;
        if &current != frozen {
            return Err("machine drain state changed during removal; no records cleared".into());
        }
        let before_revision = frozen.machine_revision;
        self.clear_keys(
            &frozen.executor_id,
            &frozen
                .removed
                .iter()
                .map(|r| r.owner.key())
                .collect::<Vec<_>>(),
        )?;
        let after_revision = self.machine_revision(&frozen.executor_id);
        let receipt = RemovalReceipt {
            application,
            outcome: RemovalOutcome::EnrollmentRevoked,
            before_revision,
            after_revision,
        };
        self.removal_receipts
            .insert(receipt.application.operation_id.clone(), receipt.clone());
        Ok(receipt)
    }

    pub fn apply_lift(&mut self, application: LiftApplication) -> Result<LiftReceipt, String> {
        application.validate()?;
        if let Some(receipt) = self.lift_receipts.get(&application.attempt_id) {
            return if receipt.application == application {
                Ok(receipt.clone())
            } else {
                Err("lift attempt identity collision".into())
            };
        }
        let before_revision = self.machine_revision(&application.executor_id);
        let operator = self
            .records(&application.executor_id)
            .into_iter()
            .find(|r| r.owner == DrainOwner::Operator);
        let outcome = if before_revision == application.machine_revision
            && operator
                .as_ref()
                .is_some_and(|r| r.generation == application.operator_generation)
        {
            self.clear_keys(&application.executor_id, &[DrainOwner::Operator.key()])?;
            LiftOutcome::Cleared
        } else {
            LiftOutcome::Stale
        };
        let after_revision = self.machine_revision(&application.executor_id);
        let remaining_owners: Vec<String> = self
            .records(&application.executor_id)
            .iter()
            .map(|record| format!("{}@{}", record.owner.key(), record.generation))
            .collect();
        let receipt = LiftReceipt {
            application,
            before_revision,
            after_revision,
            report_body: lift_report_body(&outcome, after_revision, &remaining_owners),
            remaining_owners,
            outcome,
        };
        self.lift_receipts
            .insert(receipt.application.attempt_id.clone(), receipt.clone());
        Ok(receipt)
    }
}

fn lift_report_body(outcome: &LiftOutcome, revision: u64, remaining: &[String]) -> String {
    let standing = if remaining.is_empty() {
        "no other durable drain record stands (a temporary removal fence or startup hold may still refuse admission)".to_string()
    } else {
        format!(
            "the machine stays CLOSED to new work: still held by {}",
            remaining.join(", ")
        )
    };
    match outcome {
        LiftOutcome::Cleared => format!("operator drain cleared at machine revision {revision}; {standing}"),
        LiftOutcome::Stale => format!("accepted drain state is stale at machine revision {revision}; no records cleared; {standing}; new acceptance required"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(subject: &str) -> PrincipalRef {
        PrincipalRef::Human {
            issuer: "test".into(),
            subject: subject.into(),
            organization: None,
        }
    }

    fn context(operation: &str) -> RuntimeDrainContext {
        RuntimeDrainContext {
            kind: RuntimeDrainKind::SelfUpdate,
            operation_id: operation.into(),
            turn_id: "turn".into(),
            runner_boot_id: "boot".into(),
            admission_effect: AdmissionEffect::RefuseNewEnvironments,
        }
    }

    #[test]
    fn a_self_update_record_carries_only_the_new_environment_effect() {
        let mut state = DrainState::default();
        let mut refuse_all = context("update");
        refuse_all.admission_effect = AdmissionEffect::RefuseAllNewAdmission;
        assert!(state
            .impose_runtime("remote", refuse_all, &principal("runner"), "update", 1)
            .is_err());
        let token = state
            .impose_runtime(
                "remote",
                context("update"),
                &principal("runner"),
                "update",
                1,
            )
            .unwrap();
        assert_eq!(
            state.records("remote")[0].admission_effect(),
            AdmissionEffect::RefuseNewEnvironments
        );
        assert_eq!(
            token.context.admission_effect,
            AdmissionEffect::RefuseNewEnvironments
        );
        state
            .impose_operator("remote", &principal("runner"), "maintenance", 2)
            .unwrap();
        let operator = state
            .records("remote")
            .into_iter()
            .find(|record| record.owner == DrainOwner::Operator)
            .unwrap();
        assert_eq!(
            operator.admission_effect(),
            AdmissionEffect::RefuseAllNewAdmission
        );
        let mut quiesce = context("quiesce");
        quiesce.kind = RuntimeDrainKind::Quiesce;
        quiesce.admission_effect = AdmissionEffect::RefuseAllNewAdmission;
        state
            .impose_runtime("remote", quiesce, &principal("runner"), "quiesce", 3)
            .unwrap();
        // An effect this build does not know never decodes into an open machine.
        let mut json = serde_json::to_value(&state).unwrap();
        let text = json
            .to_string()
            .replace("refuse_new_environments", "refuse_some");
        json = serde_json::from_str(&text).unwrap();
        assert!(serde_json::from_value::<DrainState>(json).is_err());
    }

    fn lift(state: &DrainState, attempt: &str) -> LiftApplication {
        LiftApplication {
            install_id: "install".into(),
            workspace_id: "workspace".into(),
            authorization_event_id: "authorization".into(),
            attempt_id: attempt.into(),
            claim_id: "claim".into(),
            actor: "execution.fleet".into(),
            kind: "lift_drain".into(),
            executor_id: "machine".into(),
            drain_key: "machine".into(),
            accepted_owner: DrainOwner::Operator,
            operator_generation: state
                .records("machine")
                .into_iter()
                .find(|r| r.owner == DrainOwner::Operator)
                .unwrap()
                .generation,
            machine_revision: state.machine_revision("machine"),
            report_id: "report".into(),
        }
    }

    #[test]
    fn independent_owners_and_identical_retries_have_exact_revisions() {
        let mut state = DrainState::default();
        let p = principal("a");
        assert!(state.impose_operator("machine", &p, "repair", 1).unwrap());
        assert!(!state.impose_operator("machine", &p, "repair", 99).unwrap());
        assert_eq!(state.machine_revision("machine"), 1);
        state
            .impose_thread("machine", "thread", &p, "investigate", 2)
            .unwrap();
        let runtime = state
            .impose_runtime("machine", context("update"), &p, "update", 3)
            .unwrap();
        assert_eq!(state.records("machine").len(), 3);
        assert!(state.clear_thread("machine", "thread").unwrap());
        assert_eq!(state.machine_revision("machine"), 4);
        assert_eq!(state.records("machine").len(), 2);
        assert!(
            state.release_runtime(&runtime).unwrap(),
            "other owner's revision must not invalidate token"
        );
        assert_eq!(state.machine_revision("machine"), 5);
        assert_eq!(state.records("machine")[0].owner, DrainOwner::Operator);
        assert!(!state.release_runtime(&runtime).unwrap());
        assert!(!state.clear_thread("machine", "thread").unwrap());
        assert_eq!(state.machine_revision("machine"), 5);
    }

    #[test]
    fn operator_cannot_be_taken_over_and_reason_replacement_is_new_generation() {
        let mut state = DrainState::default();
        state
            .impose_operator("machine", &principal("a"), "repair", 1)
            .unwrap();
        let before = state.clone();
        assert!(state
            .impose_operator("machine", &principal("b"), "repair", 2)
            .is_err());
        assert_eq!(state, before);
        state
            .impose_operator("machine", &principal("a"), "new reason", 2)
            .unwrap();
        assert_eq!(state.machine_revision("machine"), 2);
        assert_eq!(state.records("machine")[0].generation, 2);
    }

    #[test]
    fn runtime_replacement_and_wrong_identity_cannot_be_released_by_old_token() {
        let mut state = DrainState::default();
        let old = state
            .impose_runtime("machine", context("update"), &principal("a"), "update", 1)
            .unwrap();
        let new = state
            .impose_runtime(
                "machine",
                context("update"),
                &principal("a"),
                "new target",
                2,
            )
            .unwrap();
        assert!(!state.release_runtime(&old).unwrap());
        let mut wrong = new.clone();
        wrong.executor_id = "other".into();
        assert!(state.release_runtime(&wrong).is_err());
        assert_eq!(state.records("machine").len(), 1);
        assert!(state.release_runtime(&new).unwrap());
        let next = state
            .impose_runtime("machine", context("update"), &principal("a"), "update", 3)
            .unwrap();
        assert!(next.generation > new.generation);
        assert!(!state.release_runtime(&new).unwrap());
    }

    #[test]
    fn legacy_is_unknown_and_only_authenticated_clear_paths_remove_it() {
        let mut state = DrainState::import_legacy(vec!["local".into()]).unwrap();
        let record = state.records("colocated").pop().unwrap();
        assert_eq!(record.reason, LEGACY_DRAIN_REASON);
        assert_eq!(record.generation, 1);
        assert_eq!(record.provenance, DrainProvenance::LegacyUnknown);
        assert_eq!(record.imposed_by(), None);
        let token = state
            .impose_runtime(
                "colocated",
                context("update"),
                &principal("a"),
                "update",
                10,
            )
            .unwrap();
        state.release_runtime(&token).unwrap();
        assert_eq!(state.records("colocated").len(), 1);
        state
            .impose_thread("colocated", "t", &principal("a"), "test", 11)
            .unwrap();
        assert!(state.clear_thread("colocated", "t").unwrap());
        assert!(state.records("colocated").is_empty());
        assert_eq!(state.machine_revision("colocated"), 5);
        assert!(DrainState::import_legacy(vec!["local".into(), "local".into()]).is_err());
    }

    #[test]
    fn lift_replay_is_receipt_first_and_never_clears_replacement() {
        let mut state = DrainState::default();
        let p = principal("a");
        state.impose_operator("machine", &p, "repair", 1).unwrap();
        state
            .impose_thread("machine", "thread", &p, "investigate", 2)
            .unwrap();
        let application = lift(&state, "attempt");
        let receipt = state.apply_lift(application.clone()).unwrap();
        assert_eq!(receipt.outcome, LiftOutcome::Cleared);
        assert_eq!((receipt.before_revision, receipt.after_revision), (2, 3));
        assert_eq!(state.records("machine").len(), 1, "other owner remains");
        state
            .impose_operator("machine", &p, "replacement", 3)
            .unwrap();
        let before = state.clone();
        assert_eq!(state.apply_lift(application.clone()).unwrap(), receipt);
        assert_eq!(state, before);
        let mut collision = application;
        collision.claim_id = "another claim".into();
        assert!(state.apply_lift(collision).is_err());
        assert_eq!(state, before);
        state.validate().unwrap();
        let round_trip: DrainState =
            serde_json::from_value(serde_json::to_value(&state).unwrap()).unwrap();
        assert_eq!(round_trip, state);
    }

    #[test]
    fn another_owners_reversed_change_makes_lift_stale_and_records_outcome() {
        let mut state = DrainState::default();
        let p = principal("a");
        state.impose_operator("machine", &p, "repair", 1).unwrap();
        let application = lift(&state, "attempt");
        state.impose_thread("machine", "t", &p, "test", 2).unwrap();
        state.clear_thread("machine", "t").unwrap();
        let revision = state.machine_revision("machine");
        let receipt = state.apply_lift(application.clone()).unwrap();
        assert_eq!(receipt.outcome, LiftOutcome::Stale);
        assert_eq!(
            (receipt.before_revision, receipt.after_revision),
            (revision, revision)
        );
        assert_eq!(state.records("machine").len(), 1);
        assert_eq!(state.apply_lift(application).unwrap(), receipt);
        assert_eq!(state.machine_revision("machine"), revision);
    }

    #[test]
    fn removal_checks_principal_other_owners_and_racing_revision() {
        let mut state = DrainState::default();
        let p = principal("a");
        state.impose_operator("machine", &p, "repair", 1).unwrap();
        assert!(state
            .prepare_removal_records("machine", &principal("b"), "device")
            .is_err());
        let frozen = state
            .prepare_removal_records("machine", &p, "device")
            .unwrap();
        state.impose_thread("machine", "t", &p, "test", 2).unwrap();
        assert!(state
            .prepare_removal_records("machine", &p, "device")
            .is_err());
        state.clear_thread("machine", "t").unwrap();
        let application = RemovalApplication {
            install_id: "install".into(),
            operation_id: "remove".into(),
            provenance: "credential-proof".into(),
            frozen,
        };
        let before = state.clone();
        assert!(state.apply_removal(application).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn removal_own_and_legacy_clear_retains_receipt_and_counter_on_reenrollment() {
        let mut state = DrainState::import_legacy(vec!["machine".into()]).unwrap();
        let p = principal("a");
        state.impose_operator("machine", &p, "repair", 1).unwrap();
        let frozen = state
            .prepare_removal_records("machine", &p, "device")
            .unwrap();
        assert_eq!(frozen.removed.len(), 2);
        let application = RemovalApplication {
            install_id: "install".into(),
            operation_id: "remove".into(),
            provenance: "credential-proof".into(),
            frozen,
        };
        let receipt = state.apply_removal(application.clone()).unwrap();
        assert_eq!((receipt.before_revision, receipt.after_revision), (2, 3));
        assert!(state.records("machine").is_empty());
        assert!(
            state.lift_receipts().is_empty(),
            "removal never fabricates accepted evidence"
        );
        state
            .impose_operator("machine", &p, "replacement", 4)
            .unwrap();
        assert_eq!(state.apply_removal(application.clone()).unwrap(), receipt);
        assert_eq!(state.records("machine")[0].generation, 4);
        let mut collision = application;
        collision.install_id = "other".into();
        assert!(state.apply_removal(collision).is_err());
        state.validate().unwrap();
    }

    #[test]
    fn checked_overflow_refuses_imposition_and_clear_without_mutation() {
        let mut state = DrainState::default();
        state
            .impose_thread("machine", "t", &principal("a"), "test", 1)
            .unwrap();
        state.machines.get_mut("machine").unwrap().machine_revision = u64::MAX;
        let before = state.clone();
        assert!(state
            .impose_operator("machine", &principal("a"), "repair", 2)
            .is_err());
        assert!(state.clear_thread("machine", "t").is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn malformed_new_schema_and_unknown_effect_fail_closed() {
        let mut state = DrainState::default();
        state
            .impose_runtime("machine", context("update"), &principal("a"), "update", 1)
            .unwrap();
        let original = serde_json::to_value(&state).unwrap();
        let mut bad = original.clone();
        bad["version"] = serde_json::json!(2);
        assert!(serde_json::from_value::<DrainState>(bad).is_err());
        let mut bad = original.clone();
        bad["surprise"] = serde_json::json!(true);
        assert!(serde_json::from_value::<DrainState>(bad).is_err());
        let mut bad = original.clone();
        bad["machines"]["machine"]["machineRevision"] = serde_json::json!(0);
        assert!(serde_json::from_value::<DrainState>(bad).is_err());
        let mut bad = original;
        let record = bad["machines"]["machine"]["records"]
            .as_object_mut()
            .unwrap()
            .values_mut()
            .next()
            .unwrap();
        record["owner"]["context"]["admissionEffect"] = serde_json::json!("admit_pinned");
        assert!(serde_json::from_value::<DrainState>(bad).is_err());
        assert!(state
            .impose_thread("machine", "", &principal("a"), "test", 2)
            .is_err());
        assert!(state
            .impose_operator("machine", &principal("a"), " ", 2)
            .is_err());
        assert!(state
            .impose_operator("machine", &principal("a"), "test", -1)
            .is_err());
    }
}
