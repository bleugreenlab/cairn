//! The `lift_drain` consumer: the ordinary path that clears an operator-owned
//! drain. (The one other clear authority is credential-proven removal of the
//! acting operator's own record, inside the removal mutation in cairn-config.)
//!
//! FleetConfig stays the single durable drain authority. An accepted request
//! does not clear anything by itself: the clear is one compare-and-clear inside
//! the serialized fleet mutation, and its attempt-id receipt is persisted in
//! the same write, before any live publication. The request journal's result
//! is settled afterwards from that receipt, never from caller JSON, so a crash
//! at any point reconciles to the same outcome and can never clear a drain
//! imposed after the accepted one.
//!
//! Order of effects: (1) the primitive's DB claim, (2) config compare/clear
//! plus receipt, (3) checked report. Recovery after a restart runs the same
//! steps backwards: find the claim, read the receipt, report it.

use std::sync::Arc;

use async_trait::async_trait;
use cairn_common::requests::{
    DrainAcceptance, RequestAcceptance, RequestAttemptOutcome, RequestKind,
};
use cairn_config::drain::{DrainOwner, DrainRecord, LiftApplication, LiftOutcome, LiftReceipt};
use cairn_config::fleet::FleetConfig;
use cairn_db::storage::requests as store;
use cairn_uri::{parse_uri, CairnResource};
use serde_json::{json, Value};

use crate::operator_requests::registry::{
    ActorFacts, ConsumerReport, ExecutorTarget, RequestConsumer,
};
use crate::operator_requests::{self, AcceptedRequestContext};
use crate::orchestrator::Orchestrator;

const KIND: &str = "lift_drain";
const RECOVERY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

pub(crate) struct LiftDrainConsumer {
    /// Held by the primitive from before the DB claim through the report, and
    /// by every reconciliation pass: any claim a pass can see has no effect in
    /// flight, so no timestamp or boot heuristic is needed.
    gate: Arc<tokio::sync::Mutex<()>>,
}

/// Register the consumer once the fleet settings have loaded, then reconcile
/// claims a previous runner left unresolved. Call before the dispatch outbox
/// worker starts so no accepted lift is dispatched to an unregistered slot.
pub(crate) fn install(orch: &Orchestrator) -> Result<(), String> {
    let consumer = LiftDrainConsumer::new();
    orch.operator_requests.register(consumer.clone())?;
    let orch = orch.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(RECOVERY_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if !orch.fleet.drain_projection_initialized() {
                continue;
            }
            if let Err(error) = consumer.recover(&orch).await {
                tracing::warn!(%error, "lift_drain reconciliation retained unresolved claims");
            }
        }
    });
    Ok(())
}

fn report_id(attempt_id: &str) -> String {
    format!("lift-drain:{attempt_id}")
}

fn owner_label(owner: &DrainOwner) -> String {
    match owner {
        DrainOwner::Operator => "operator".into(),
        DrainOwner::Thread { thread_id } => format!("thread:{thread_id}"),
        DrainOwner::Runtime { context } => {
            format!("runtime:{:?}:{}", context.kind, context.operation_id)
        }
        DrainOwner::Legacy => "legacy".into(),
    }
}

fn record_fact(record: &DrainRecord) -> Value {
    json!({
        "owner": owner_label(&record.owner),
        "reason": record.reason,
        "generation": record.generation,
        "provenance": serde_json::to_value(&record.provenance).unwrap_or(Value::Null),
    })
}

fn frozen_drain(acceptance: &RequestAcceptance) -> Result<&DrainAcceptance, String> {
    let drain = acceptance
        .drain
        .as_ref()
        .ok_or("lift_drain acceptance has no drain snapshot")?;
    if drain.owner != "operator"
        || FleetConfig::drain_key(&drain.executor_id) != drain.drain_key
        || acceptance.target_identity != drain.executor_id
    {
        return Err(
            "lift_drain acceptance does not name one stable machine's operator drain".into(),
        );
    }
    Ok(drain)
}

fn application(
    context: &AcceptedRequestContext,
    acceptance: &RequestAcceptance,
    drain: &DrainAcceptance,
) -> LiftApplication {
    LiftApplication {
        install_id: acceptance.install_id.clone(),
        workspace_id: acceptance.workspace_id.clone(),
        authorization_event_id: context.event_id().into(),
        attempt_id: acceptance.attempt_id.clone(),
        claim_id: context.claim_id().into(),
        actor: acceptance.actor_id.clone(),
        kind: KIND.into(),
        executor_id: drain.executor_id.clone(),
        drain_key: drain.drain_key.clone(),
        accepted_owner: DrainOwner::Operator,
        operator_generation: drain.generation,
        machine_revision: drain.machine_revision,
        report_id: report_id(&acceptance.attempt_id),
    }
}

/// Decisive evidence for one attempt, read from the config document alone.
/// A present receipt must describe exactly this accepted act; an absent one
/// proves no effect only because the document read successfully.
fn evidence(
    config: &FleetConfig,
    context: &AcceptedRequestContext,
) -> Result<ConsumerReport, String> {
    let acceptance = context.acceptance();
    let drain = frozen_drain(acceptance)?;
    let Some(receipt) = config.drains.lift_receipts().get(&acceptance.attempt_id) else {
        return Ok(ConsumerReport {
            outcome: RequestAttemptOutcome::Failed,
            document: json!({
                "outcome": "no_effect",
                "detail": "no application receipt exists in FleetConfig; no drain was cleared; a fresh acceptance is required",
            }),
            settled: true,
            built_commit: None,
        });
    };
    if receipt.application != application(context, acceptance, drain) {
        return Err("lift application receipt does not describe this accepted act".into());
    }
    Ok(receipt_report(receipt))
}

fn receipt_report(receipt: &LiftReceipt) -> ConsumerReport {
    let (outcome, word) = match receipt.outcome {
        LiftOutcome::Cleared => (RequestAttemptOutcome::Done, "cleared"),
        LiftOutcome::Stale => (RequestAttemptOutcome::Failed, "stale"),
    };
    ConsumerReport {
        outcome,
        document: json!({
            "outcome": word,
            "executorId": receipt.application.executor_id,
            "operatorGeneration": receipt.application.operator_generation,
            "acceptedMachineRevision": receipt.application.machine_revision,
            "beforeRevision": receipt.before_revision,
            "afterRevision": receipt.after_revision,
            "body": receipt.report_body,
        }),
        settled: true,
        built_commit: None,
    }
}

async fn refresh_cards(orch: &Orchestrator, ids: &[String]) {
    for id in ids {
        if let Err(error) = operator_requests::refresh_facts(orch, id).await {
            tracing::debug!(request = %id, %error, "lift_drain card facts not refreshed");
        }
    }
}

impl LiftDrainConsumer {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Reconcile one claim from FleetConfig evidence. The current document is
    /// republished first, through the serialized mutation, so recovery after a
    /// crash between commit and publication converges the live projection on
    /// the present config (never a historical snapshot) before the report.
    async fn settle(
        &self,
        orch: &Orchestrator,
        context: &AcceptedRequestContext,
    ) -> Result<(), String> {
        super::management::mutate_fleet(orch, |_| Ok(()))?;
        let config = cairn_config::settings::read_fleet_strict(&orch.config_dir)?;
        let report = evidence(&config, context)?;
        let failed = report.outcome != RequestAttemptOutcome::Done;
        let acceptance = context.acceptance().clone();
        context
            .report(orch, &report_id(&acceptance.attempt_id), report)
            .await?;
        if failed {
            refresh_cards(orch, &acceptance.request_ids).await;
        }
        Ok(())
    }

    /// Settle every claim whose effect is not in flight: those a previous boot
    /// left behind and those a dispatch could not settle itself.
    pub(crate) async fn recover(&self, orch: &Orchestrator) -> Result<(), String> {
        let _gate = self.gate.lock().await;
        let attempts = store::unresolved_claims(&orch.db.local, RequestKind::LiftDrain)
            .await
            .map_err(|error| error.to_string())?;
        let mut first_error = None;
        for attempt in attempts {
            let result = async {
                let adapter = orch.operator_requests.consumer(KIND)?;
                let context = operator_requests::recover_claimed_request(
                    orch,
                    &attempt.authorization_event_id,
                    &attempt.acceptance,
                    adapter,
                )
                .await?;
                self.settle(orch, &context).await
            }
            .await;
            if let Err(error) = result {
                first_error.get_or_insert(format!(
                    "attempt {}: {error}",
                    attempt.acceptance.attempt_id
                ));
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

#[async_trait]
impl RequestConsumer for LiftDrainConsumer {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn actor(&self) -> &'static str {
        "execution.fleet"
    }

    async fn exclusion(&self) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        Some(self.gate.clone().lock_owned().await)
    }

    async fn availability(&self, orch: &Orchestrator) -> Result<(), String> {
        if orch.fleet.drain_projection_initialized() {
            return Ok(());
        }
        Err(match orch.fleet.drain_initialization_error() {
            Some(error) => {
                format!("lift_drain is unavailable: fleet settings failed to load: {error}")
            }
            None => "lift_drain is unavailable: fleet drain state is still loading".into(),
        })
    }

    async fn facts(
        &self,
        orch: &Orchestrator,
        request_id: Option<&str>,
        target: Option<&str>,
    ) -> Result<ActorFacts, String> {
        let uri = target.ok_or("lift_drain requires a canonical public executor URI")?;
        let machine = match request_id {
            // An existing request keeps its frozen stable identity; a rename
            // must never retarget it at whichever machine inherits the name.
            Some(id) => {
                let request = store::get_request(&orch.db.local, id)
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or("lift_drain request does not exist")?;
                ExecutorTarget {
                    uri: uri.into(),
                    drain_key: FleetConfig::drain_key(&request.target_identity).to_string(),
                    executor_id: request.target_identity,
                }
            }
            None => self.executor_target(orch, uri).await?,
        };
        let config = cairn_config::settings::read_fleet_strict(&orch.config_dir)?;
        let records = config.drains.records(&machine.executor_id);
        let operator = records
            .iter()
            .find(|record| record.owner == DrainOwner::Operator)
            .ok_or_else(|| {
                format!(
                    "no operator-owned drain stands on {uri}; thread, runtime and legacy drains are cleared by their own paths and need no lift_drain"
                )
            })?;
        let revision = config.drains.machine_revision(&machine.executor_id);
        Ok(ActorFacts {
            document: json!({
                "executor": {
                    "uri": machine.uri,
                    "id": machine.executor_id,
                    "drainKey": machine.drain_key,
                },
                "machineRevision": revision,
                "owners": records.iter().map(record_fact).collect::<Vec<_>>(),
            }),
            running_version: None,
            main_commit: None,
            candidate_commit: None,
            base_commit: None,
            policy_project_id: None,
            drain: Some(DrainAcceptance {
                executor_id: machine.executor_id,
                drain_key: machine.drain_key,
                owner: "operator".into(),
                generation: operator.generation,
                machine_revision: revision,
            }),
        })
    }

    /// Structural only. Whether the accepted drain still stands is decided
    /// atomically by the compare-and-clear, so a changed machine is a recorded
    /// stale outcome rather than a pre-dispatch refusal that loses the evidence.
    async fn validate_snapshot(
        &self,
        _: &Orchestrator,
        acceptance: &RequestAcceptance,
    ) -> Result<(), String> {
        if acceptance.kind != RequestKind::LiftDrain || acceptance.actor_id != self.actor() {
            return Err("wrong physical actor or kind for lift_drain".into());
        }
        frozen_drain(acceptance).map(|_| ())
    }

    async fn contains_commits(
        &self,
        _: &Orchestrator,
        _: &str,
        _: &[String],
    ) -> Result<(), String> {
        Err("lift_drain has no commit requirements".into())
    }

    async fn executor_target(
        &self,
        orch: &Orchestrator,
        uri: &str,
    ) -> Result<ExecutorTarget, String> {
        let Some(CairnResource::Executor { name }) = parse_uri(uri) else {
            return Err("lift_drain requires a canonical public executor URI".into());
        };
        let executor_id = super::management::resolve_executor_reference(orch, &name)?;
        let canonical = CairnResource::Executor {
            name: super::management::public_name(orch, &executor_id),
        }
        .to_uri();
        if canonical != uri {
            return Err(format!("use the canonical executor URI {canonical}"));
        }
        Ok(ExecutorTarget {
            uri: canonical,
            drain_key: FleetConfig::drain_key(&executor_id).to_string(),
            executor_id,
        })
    }

    async fn verify_report(
        &self,
        orch: &Orchestrator,
        context: &AcceptedRequestContext,
        report: ConsumerReport,
    ) -> Result<ConsumerReport, String> {
        let config = cairn_config::settings::read_fleet_strict(&orch.config_dir)?;
        let decisive = evidence(&config, context)?;
        if decisive.outcome != report.outcome {
            return Err("reported outcome contradicts the FleetConfig application evidence".into());
        }
        Ok(decisive)
    }

    async fn dispatch(
        &self,
        orch: &Orchestrator,
        context: &AcceptedRequestContext,
    ) -> Result<(), String> {
        // The primitive holds `exclusion` across this call.
        let acceptance = context.acceptance().clone();
        context.recheck_for(orch, &acceptance).await?;
        let drain = frozen_drain(&acceptance)?.clone();
        let intended = application(context, &acceptance, &drain);
        // An Err here always means no replacement happened: nothing is
        // cleared and nothing is published. The diagnostic is logged; the
        // settled report carries only the deterministic no-effect evidence.
        if let Err(error) = super::management::mutate_fleet(orch, |config| {
            config.drains.apply_lift(intended.clone())
        }) {
            tracing::warn!(attempt = %acceptance.attempt_id, %error, "lift_drain config application refused");
        }
        // A committed application receipt remains authoritative if reporting
        // fails. Returning an error would let the primitive terminally fail the
        // attempt before recovery can report that receipt's successful clear.
        if let Err(error) = self.settle(orch, context).await {
            tracing::warn!(attempt = %acceptance.attempt_id, %error, "lift_drain settlement deferred to receipt reconciliation");
        }
        Ok(())
    }
}
