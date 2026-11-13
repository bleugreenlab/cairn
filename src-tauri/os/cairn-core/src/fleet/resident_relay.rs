//! The runner's half of the resident-process relay: one context per terminal,
//! REPL, workflow runtime, or service process placed on a machine other than
//! the runner's.
//!
//! A resident process on a remote cannot reach the runner's loopback callback,
//! so its executor serves a local URL for that one process and forwards what
//! arrives there over the authenticated link, with the process's identity and
//! the credential it presented. The executor proves which process called; the
//! context here decides what that process may do. The two halves meet on the
//! relay id, a random routing name minted for one start of one process.
//!
//! A context lives as long as its process. It is registered before the start is
//! sent, bound to the executor link the start went out on, and learns the
//! process generation from the start's answer, because the executor assigns
//! generations. It is revoked when the process exits or is stopped, when its
//! residency is released, and when another process starts at its key. A link
//! that closes only *detaches* it: the executor re-announces its live relays as
//! the first frame of the next link ([`ResidentRelays::reattach`]), and a
//! context it does not announce is revoked then. Contexts are not persisted, so
//! a runner restart ends every one; a relayed call after that is refused as
//! unknown and its process has to be restarted (cairn/5562 follow-up A).
//!
//! Two kinds of authority, because the two kinds of resident authenticate
//! differently:
//!
//! - A terminal or REPL presents a per-process capability the runner minted and
//!   keeps only the SHA-256 of. It speaks for the run that created the process.
//! - A project service presents run bearers that change with every call. Each
//!   is bound to [`crate::mcp::run_bearer::relay_audience`] of the executor the
//!   service runs on, and the run it verifies to must belong to this
//!   incarnation of this service.
//!
//! Either way the request is then held to its run by
//! [`crate::mcp::run_bearer::bind_request_to_run`], the same rule the HTTP
//! callback applies to a bearer.

use std::collections::HashMap;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rand::RngCore;
use sha2::{Digest, Sha256};

use cairn_common::executor_protocol::{
    RelayAuthorization, RelayIdentity, ResidencyHolder, ResidentRelayGrant, ResidentRelayId,
};

use crate::mcp::types::McpCallbackRequest;
use crate::security::lease::LeaseAudience;

/// What a relayed resident process is allowed to present, and for whom.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResidentRelayAuthority {
    /// A terminal or REPL: the digest of the capability in its environment,
    /// and the run it speaks for. `None` for a process no run created, whose
    /// calls may name neither a run nor a thread.
    RunCapability {
        run_id: Option<String>,
        capability_digest: [u8; 32],
    },
    /// One incarnation of a project service.
    ServiceBearers(ServiceIncarnation),
}

/// Which service incarnation a bearer's run must belong to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServiceIncarnation {
    pub project_id: String,
    /// [`crate::project_services::ServiceOwner::key`]: empty for the project.
    pub owner_key: String,
    pub service: String,
    pub generation: u64,
}

/// A verified service bearer: its run and the incarnation the run belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedServiceRun {
    pub run_id: String,
    pub incarnation: ServiceIncarnation,
    /// The incarnation's own run, as opposed to one call's delegation. A call
    /// is retried on the next incarnation when the first fails, so a
    /// delegation may be presented by any incarnation of its service; a
    /// standing run only by its own.
    pub standing: bool,
}

struct ResidentRelayContext {
    executor_id: String,
    /// The connection generation of the link the process's start went out on,
    /// or of the link that re-announced it. `None` between links.
    binding: Option<u64>,
    holder: ResidencyHolder,
    process_key: String,
    /// Learned from the start's answer, or from the first call when that
    /// answer has not been processed yet.
    process_generation: Option<u64>,
    /// An exit observed before the generation was learned.
    exited_generation: Option<u64>,
    authority: ResidentRelayAuthority,
    /// Unregisters the capability from output scrubbing when the context
    /// ends. Held, never read.
    _capability: Option<crate::security::SecretGuard<'static>>,
}

/// One relayed call, as its executor described it.
pub struct ResidentRelayCall<'a> {
    pub relay_id: &'a ResidentRelayId,
    pub holder: &'a ResidencyHolder,
    pub process_key: &'a str,
    pub process_generation: u64,
    pub authorization: &'a RelayAuthorization,
}

/// Every live resident relay this runner holds.
#[derive(Default)]
pub(crate) struct ResidentRelays {
    contexts: HashMap<ResidentRelayId, ResidentRelayContext>,
}

fn random_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn digest(value: &str) -> [u8; 32] {
    Sha256::digest(value.as_bytes()).into()
}

fn register_capability(
    process_key: &str,
    capability: &str,
) -> Option<crate::security::SecretGuard<'static>> {
    use crate::security::{registry, SecretCategory, SecretId, SecretMaterial};
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let id = SecretId::new(format!(
        "resident-relay-capability:{process_key}:{sequence}"
    ));
    match registry().register(
        id.clone(),
        SecretCategory::CallbackCredential,
        "resident process relay capability",
        SecretMaterial::from_string(capability.to_string()),
    ) {
        Ok(guard) => Some(guard),
        Err(error) => {
            tracing::warn!(secret = %id, %error, "resident relay capability was not registered for scrubbing");
            None
        }
    }
}

/// A capability minted for a terminal or REPL, and the relay grant that
/// carries its calls.
pub(crate) struct MintedResidentRelay {
    pub grant: ResidentRelayGrant,
    /// The per-process capability, sent in the start's `CAIRN_MCP_SECRET` (the
    /// executor strips it there on unix and attaches it on the lineage socket);
    /// `None` for a service, which presents its run bearers instead.
    pub capability: Option<String>,
}

impl ResidentRelays {
    /// Open a context for a process about to start on `executor_id` at
    /// `process_key`, ending any context an earlier start left there.
    ///
    /// `run_id` is the run a terminal or REPL speaks for; `service` names the
    /// incarnation a service's bearers must belong to. Exactly one applies.
    pub(crate) fn register(
        &mut self,
        executor_id: &str,
        holder: &ResidencyHolder,
        process_key: &str,
        authority: RegisteredAuthority,
    ) -> MintedResidentRelay {
        self.revoke_process(holder, process_key);
        let relay_id = ResidentRelayId(random_token());
        let (authority, capability, guard) = match authority {
            RegisteredAuthority::Run { run_id } => {
                let capability = random_token();
                let guard = register_capability(process_key, &capability);
                (
                    ResidentRelayAuthority::RunCapability {
                        run_id,
                        capability_digest: digest(&capability),
                    },
                    Some(capability),
                    guard,
                )
            }
            RegisteredAuthority::Service(incarnation) => (
                ResidentRelayAuthority::ServiceBearers(incarnation),
                None,
                None,
            ),
        };
        self.contexts.insert(
            relay_id.clone(),
            ResidentRelayContext {
                executor_id: executor_id.to_string(),
                binding: None,
                holder: holder.clone(),
                process_key: process_key.to_string(),
                process_generation: None,
                exited_generation: None,
                authority,
                _capability: guard,
            },
        );
        MintedResidentRelay {
            grant: ResidentRelayGrant { relay_id },
            capability,
        }
    }

    /// Bind a context to the link its start is being sent on.
    pub(crate) fn bind(
        &mut self,
        relay_id: &ResidentRelayId,
        executor_id: &str,
        generation: u64,
    ) -> Result<(), String> {
        let context = self
            .contexts
            .get_mut(relay_id)
            .ok_or_else(|| "this process's callback relay was revoked before its start was sent; start it again".to_string())?;
        if context.executor_id != executor_id {
            let expected = context.executor_id.clone();
            self.contexts.remove(relay_id);
            return Err(format!(
                "this process's callback relay was opened for executor {expected}, but its environment is on {executor_id}; start it again"
            ));
        }
        context.binding = Some(generation);
        Ok(())
    }

    /// Record the generation the executor assigned the process's start.
    pub(crate) fn learn_generation(&mut self, relay_id: &ResidentRelayId, generation: u64) {
        let exited = match self.contexts.get_mut(relay_id) {
            Some(context) => {
                context.process_generation = Some(generation);
                context.exited_generation == Some(generation)
            }
            None => return,
        };
        if exited {
            self.contexts.remove(relay_id);
        }
    }

    pub(crate) fn revoke(&mut self, relay_id: &ResidentRelayId) {
        self.contexts.remove(relay_id);
    }

    /// End the context of whatever runs, or ran, at `process_key`.
    pub(crate) fn revoke_process(&mut self, holder: &ResidencyHolder, process_key: &str) {
        self.contexts.retain(|_, context| {
            !(context.holder == *holder && context.process_key == process_key)
        });
    }

    /// End every context in a released residency.
    pub(crate) fn revoke_holder(&mut self, holder: &ResidencyHolder) {
        self.contexts.retain(|_, context| context.holder != *holder);
    }

    /// A process exited: end its context.
    pub(crate) fn process_exited(
        &mut self,
        executor_id: &str,
        holder: &ResidencyHolder,
        process_key: &str,
        generation: u64,
    ) {
        self.contexts.retain(|_, context| {
            if context.executor_id != executor_id
                || context.holder != *holder
                || context.process_key != process_key
            {
                return true;
            }
            match context.process_generation {
                Some(known) => known != generation,
                // The start's answer has not been read yet. Kept, with the
                // exit remembered, so learning that generation ends it.
                None => {
                    context.exited_generation = Some(generation);
                    true
                }
            }
        });
    }

    /// The link these contexts were bound to closed. They wait for the
    /// executor's next link to re-announce them.
    pub(crate) fn detach(&mut self, executor_id: &str, generation: u64) {
        for context in self.contexts.values_mut() {
            if context.executor_id == executor_id && context.binding == Some(generation) {
                context.binding = None;
            }
        }
    }

    /// An executor opened link `generation` naming the relays it still serves.
    ///
    /// An announced relay this runner holds for that executor, at the same
    /// process, is bound to the new link. A held relay that was waiting for a
    /// link and was not announced is gone from the executor and is revoked.
    /// One already bound to this link was started on it after the executor
    /// composed the announcement, and is kept. An announced relay this runner
    /// does not hold is ignored: its calls are refused as unknown.
    pub(crate) fn reattach(
        &mut self,
        executor_id: &str,
        generation: u64,
        relays: &[RelayIdentity],
    ) {
        let mut announced = std::collections::HashSet::new();
        for relay in relays {
            let RelayIdentity::Resident {
                relay_id,
                holder,
                process_key,
                process_generation,
            } = relay
            else {
                continue;
            };
            let Some(context) = self.contexts.get_mut(relay_id) else {
                continue;
            };
            let same_process = context.executor_id == executor_id
                && context.holder == *holder
                && context.process_key == *process_key
                && context
                    .process_generation
                    .is_none_or(|known| known == *process_generation);
            if !same_process {
                continue;
            }
            context.process_generation = Some(*process_generation);
            context.binding = Some(generation);
            announced.insert(relay_id.clone());
        }
        self.contexts.retain(|relay_id, context| {
            context.executor_id != executor_id
                || context.binding == Some(generation)
                || announced.contains(relay_id)
        });
    }

    /// Decide one relayed call. On success `request.run_id` names the run the
    /// call is dispatched as.
    ///
    /// `verify_service` verifies a service's bearer against an audience and
    /// says which incarnation its run belongs to.
    pub(crate) fn authorize(
        &mut self,
        executor_id: &str,
        generation: u64,
        call: &ResidentRelayCall<'_>,
        request: &mut McpCallbackRequest,
        verify_service: &dyn Fn(&str, &LeaseAudience) -> Result<VerifiedServiceRun, String>,
    ) -> Result<(), String> {
        let context = self.contexts.get_mut(call.relay_id).ok_or_else(|| {
            "unknown resident relay: the process ended, or the runner restarted since it started; restart the terminal, REPL, or service".to_string()
        })?;
        if context.executor_id != executor_id {
            return Err("this resident relay belongs to a different executor".into());
        }
        match context.binding {
            Some(bound) if bound == generation => {}
            Some(_) => {
                return Err(
                    "this resident relay is bound to a different link of its executor".into(),
                )
            }
            None => return Err(
                "this resident relay has not been re-announced since its executor's link was lost"
                    .into(),
            ),
        }
        if context.holder != *call.holder || context.process_key != call.process_key {
            return Err("this resident relay belongs to a different process".into());
        }
        match context.process_generation {
            Some(known) if known != call.process_generation => {
                return Err(
                    "this resident relay belongs to an earlier start of this process".into(),
                )
            }
            Some(_) => {}
            None => context.process_generation = Some(call.process_generation),
        }
        let token = call
            .authorization
            .bearer_token()
            .ok_or_else(|| "a relayed call must present a `Bearer` credential".to_string())?;
        let run_id = match &context.authority {
            ResidentRelayAuthority::RunCapability {
                run_id,
                capability_digest,
            } => {
                if !crate::mcp::run_bearer::constant_time_eq(&digest(token), capability_digest) {
                    return Err(
                        "this credential is not the one this process was started with".into(),
                    );
                }
                run_id.clone()
            }
            ResidentRelayAuthority::ServiceBearers(incarnation) => {
                let audience = crate::mcp::run_bearer::relay_audience(executor_id);
                let verified = verify_service(token, &audience)?;
                let same_service = verified.incarnation.project_id == incarnation.project_id
                    && verified.incarnation.owner_key == incarnation.owner_key
                    && verified.incarnation.service == incarnation.service;
                let same_generation = verified.incarnation.generation == incarnation.generation;
                if !same_service || (verified.standing && !same_generation) {
                    return Err(format!(
                        "run {} does not belong to this incarnation of service `{}`",
                        verified.run_id, incarnation.service
                    ));
                }
                Some(verified.run_id)
            }
        };
        match run_id {
            Some(run_id) => crate::mcp::run_bearer::bind_request_to_run(request, &run_id),
            None => {
                if request.run_id.as_deref().is_some_and(|run| !run.is_empty()) {
                    return Err("this process was started for no run and cannot claim one".into());
                }
                crate::mcp::run_bearer::hold_to_run_less_process(request)?;
                request.origin = Some(
                    cairn_common::protocol::DispatchOrigin::RelayedRunLessProcess(
                        cairn_common::protocol::RelayedProcess {
                            relay_id: call.relay_id.clone(),
                            process_key: context.process_key.clone(),
                        },
                    ),
                );
                Ok(())
            }
        }
    }

    /// Whether the process behind this relay is, as far as this runner can tell,
    /// still running. A context waiting for its executor's next link, or holding
    /// an exit it has not matched to a generation yet, counts as ended.
    pub(crate) fn is_live(&self, relay_id: &ResidentRelayId) -> bool {
        self.contexts
            .get(relay_id)
            .is_some_and(|context| context.binding.is_some() && context.exited_generation.is_none())
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.contexts.len()
    }

    /// The relay this runner holds for the process at `process_key`, if any.
    #[cfg(test)]
    pub(crate) fn relay_of_process(
        &self,
        holder: &ResidencyHolder,
        process_key: &str,
    ) -> Option<ResidentRelayId> {
        self.contexts
            .iter()
            .find(|(_, context)| context.holder == *holder && context.process_key == process_key)
            .map(|(relay_id, _)| relay_id.clone())
    }
}

/// Who a new relay context will authorize.
pub(crate) enum RegisteredAuthority {
    Run { run_id: Option<String> },
    Service(ServiceIncarnation),
}

/// Authorize one relayed resident call against this orchestrator's contexts,
/// bearers, and service runs. On success `request.run_id` is the run the call
/// is dispatched as.
pub fn authorize_resident_relay(
    orch: &crate::orchestrator::Orchestrator,
    executor_id: &str,
    generation: u64,
    call: &ResidentRelayCall<'_>,
    request: &mut McpCallbackRequest,
) -> Result<(), String> {
    let verify_service = |token: &str, audience: &LeaseAudience| {
        let verified = orch
            .run_bearers
            .verify(token, audience)
            .map_err(|refusal| refusal.to_string())?;
        let run_id = verified
            .run_id()
            .ok_or_else(|| format!("{} is no project service's run", verified.scope.label()))?
            .to_string();
        let run = orch
            .project_services
            .runs
            .get(&run_id)
            .ok_or_else(|| format!("run {run_id} belongs to no live project service"))?;
        Ok(VerifiedServiceRun {
            run_id,
            standing: matches!(
                run.role,
                crate::project_services::authority::ServiceRunRole::Standing
            ),
            incarnation: ServiceIncarnation {
                project_id: run.project_id,
                owner_key: run.owner.key().to_string(),
                service: run.service,
                generation: run.generation,
            },
        })
    };
    orch.fleet
        .resident_relays()
        .authorize(executor_id, generation, call, request, &verify_service)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holder() -> ResidencyHolder {
        ResidencyHolder::Job {
            job_id: "job".into(),
        }
    }

    fn bearer(value: &str) -> RelayAuthorization {
        RelayAuthorization(format!("Bearer {value}"))
    }

    fn request(run_id: Option<&str>) -> McpCallbackRequest {
        McpCallbackRequest {
            cwd: "/".into(),
            run_id: run_id.map(str::to_string),
            tool: "read_batch".into(),
            payload: serde_json::json!({"paths": ["cairn:~"]}),
            ..McpCallbackRequest::default()
        }
    }

    fn no_services(_: &str, _: &LeaseAudience) -> Result<VerifiedServiceRun, String> {
        Err("no services here".into())
    }

    /// A terminal's relay, started and answered at generation 4 on link 7.
    fn started_terminal(relays: &mut ResidentRelays) -> (ResidentRelayId, String) {
        let minted = relays.register(
            "machine-b",
            &holder(),
            "shell",
            RegisteredAuthority::Run {
                run_id: Some("run-1".into()),
            },
        );
        let relay_id = minted.grant.relay_id;
        relays.bind(&relay_id, "machine-b", 7).unwrap();
        relays.learn_generation(&relay_id, 4);
        (relay_id, minted.capability.unwrap())
    }

    fn call<'a>(
        relay_id: &'a ResidentRelayId,
        holder: &'a ResidencyHolder,
        process_key: &'a str,
        process_generation: u64,
        authorization: &'a RelayAuthorization,
    ) -> ResidentRelayCall<'a> {
        ResidentRelayCall {
            relay_id,
            holder,
            process_key,
            process_generation,
            authorization,
        }
    }

    #[test]
    fn a_terminal_capability_speaks_for_its_run_on_its_link() {
        let mut relays = ResidentRelays::default();
        let (relay_id, capability) = started_terminal(&mut relays);
        let holder = holder();
        let auth = bearer(&capability);
        let mut absent = request(None);
        relays
            .authorize(
                "machine-b",
                7,
                &call(&relay_id, &holder, "shell", 4, &auth),
                &mut absent,
                &no_services,
            )
            .unwrap();
        assert_eq!(
            absent.run_id.as_deref(),
            Some("run-1"),
            "an absent run is filled"
        );
        let mut stated = request(Some("run-1"));
        relays
            .authorize(
                "machine-b",
                7,
                &call(&relay_id, &holder, "shell", 4, &auth),
                &mut stated,
                &no_services,
            )
            .unwrap();
    }

    #[test]
    fn every_mismatch_is_refused_by_name() {
        let mut relays = ResidentRelays::default();
        let (relay_id, capability) = started_terminal(&mut relays);
        let holder = holder();
        let other_holder = ResidencyHolder::Job {
            job_id: "other".into(),
        };
        let auth = bearer(&capability);
        let wrong = bearer("not-the-capability");
        let unknown = ResidentRelayId("unknown".into());
        let cases: Vec<(&str, &str, u64, ResidentRelayCall<'_>, McpCallbackRequest)> = vec![
            (
                "unknown relay",
                "machine-b",
                7,
                call(&unknown, &holder, "shell", 4, &auth),
                request(None),
            ),
            (
                "wrong capability",
                "machine-b",
                7,
                call(&relay_id, &holder, "shell", 4, &wrong),
                request(None),
            ),
            (
                "wrong executor",
                "machine-c",
                7,
                call(&relay_id, &holder, "shell", 4, &auth),
                request(None),
            ),
            (
                "stale link",
                "machine-b",
                6,
                call(&relay_id, &holder, "shell", 4, &auth),
                request(None),
            ),
            (
                "other process",
                "machine-b",
                7,
                call(&relay_id, &other_holder, "shell", 4, &auth),
                request(None),
            ),
            (
                "other key",
                "machine-b",
                7,
                call(&relay_id, &holder, "other", 4, &auth),
                request(None),
            ),
            (
                "earlier start",
                "machine-b",
                7,
                call(&relay_id, &holder, "shell", 3, &auth),
                request(None),
            ),
            (
                "foreign run",
                "machine-b",
                7,
                call(&relay_id, &holder, "shell", 4, &auth),
                request(Some("run-2")),
            ),
        ];
        for (case, executor, generation, call, mut request) in cases {
            assert!(
                relays
                    .authorize(executor, generation, &call, &mut request, &no_services)
                    .is_err(),
                "{case} must be refused"
            );
        }
        let mut threaded = request(None);
        threaded.thread_id = Some("thread".into());
        relays
            .authorize(
                "machine-b",
                7,
                &call(&relay_id, &holder, "shell", 4, &auth),
                &mut threaded,
                &no_services,
            )
            .unwrap();
        assert_eq!(threaded.run_id.as_deref(), Some("run-1"));
        assert_eq!(threaded.thread_id, None, "a thread cannot re-attribute it");
        let mut fine = request(None);
        assert!(
            relays
                .authorize(
                    "machine-b",
                    7,
                    &call(&relay_id, &holder, "shell", 4, &auth),
                    &mut fine,
                    &no_services
                )
                .is_ok(),
            "refusals change nothing about the context"
        );
    }

    #[test]
    fn an_exited_or_stopped_or_replaced_process_loses_its_relay() {
        let holder = holder();
        for end in ["exit", "stop", "release", "replace"] {
            let mut relays = ResidentRelays::default();
            let (relay_id, capability) = started_terminal(&mut relays);
            match end {
                "exit" => relays.process_exited("machine-b", &holder, "shell", 4),
                "stop" => relays.revoke_process(&holder, "shell"),
                "release" => relays.revoke_holder(&holder),
                _ => {
                    relays.register(
                        "machine-b",
                        &holder,
                        "shell",
                        RegisteredAuthority::Run {
                            run_id: Some("run-1".into()),
                        },
                    );
                }
            }
            let auth = bearer(&capability);
            let refusal = relays
                .authorize(
                    "machine-b",
                    7,
                    &call(&relay_id, &holder, "shell", 4, &auth),
                    &mut request(None),
                    &no_services,
                )
                .unwrap_err();
            assert!(
                refusal.contains("unknown resident relay"),
                "{end}: {refusal}"
            );
        }
    }

    #[test]
    fn an_exit_of_another_generation_does_not_end_a_relay_and_an_early_exit_does() {
        let holder = holder();
        let mut relays = ResidentRelays::default();
        let (_, _) = started_terminal(&mut relays);
        relays.process_exited("machine-b", &holder, "shell", 3);
        assert_eq!(relays.len(), 1);

        let mut relays = ResidentRelays::default();
        let minted = relays.register(
            "machine-b",
            &holder,
            "shell",
            RegisteredAuthority::Run { run_id: None },
        );
        relays.bind(&minted.grant.relay_id, "machine-b", 7).unwrap();
        relays.process_exited("machine-b", &holder, "shell", 1);
        assert_eq!(relays.len(), 1, "kept until its generation is known");
        relays.learn_generation(&minted.grant.relay_id, 1);
        assert_eq!(relays.len(), 0, "the exit it already saw ends it");
    }

    #[test]
    fn a_link_gap_detaches_and_reattachment_restores_or_revokes() {
        let holder = holder();
        let mut relays = ResidentRelays::default();
        let (kept, capability) = started_terminal(&mut relays);
        let minted = relays.register(
            "machine-b",
            &holder,
            "other",
            RegisteredAuthority::Run { run_id: None },
        );
        let dropped = minted.grant.relay_id;
        relays.bind(&dropped, "machine-b", 7).unwrap();
        relays.detach("machine-b", 7);

        let auth = bearer(&capability);
        let refusal = relays
            .authorize(
                "machine-b",
                7,
                &call(&kept, &holder, "shell", 4, &auth),
                &mut request(None),
                &no_services,
            )
            .unwrap_err();
        assert!(refusal.contains("re-announced"), "{refusal}");

        relays.reattach(
            "machine-b",
            8,
            &[
                RelayIdentity::Resident {
                    relay_id: kept.clone(),
                    holder: holder.clone(),
                    process_key: "shell".into(),
                    process_generation: 4,
                },
                RelayIdentity::Resident {
                    relay_id: ResidentRelayId("never-held".into()),
                    holder: holder.clone(),
                    process_key: "x".into(),
                    process_generation: 1,
                },
                RelayIdentity::Batch {
                    runner_context_id: "ctx".into(),
                    request_id: "r".into(),
                    attempt_id: "a".into(),
                },
            ],
        );
        relays
            .authorize(
                "machine-b",
                8,
                &call(&kept, &holder, "shell", 4, &auth),
                &mut request(None),
                &no_services,
            )
            .unwrap();
        assert_eq!(relays.len(), 1, "the relay it did not announce is revoked");
    }

    #[test]
    fn reattachment_keeps_a_relay_already_started_on_the_new_link_and_ignores_other_machines() {
        let holder = holder();
        let mut relays = ResidentRelays::default();
        let fresh = relays.register(
            "machine-b",
            &holder,
            "new",
            RegisteredAuthority::Run { run_id: None },
        );
        relays.bind(&fresh.grant.relay_id, "machine-b", 8).unwrap();
        let elsewhere = relays.register(
            "machine-c",
            &holder,
            "there",
            RegisteredAuthority::Run { run_id: None },
        );
        relays
            .bind(&elsewhere.grant.relay_id, "machine-c", 2)
            .unwrap();
        relays.detach("machine-c", 2);
        relays.reattach("machine-b", 8, &[]);
        assert_eq!(relays.len(), 2);
    }

    #[test]
    fn a_relay_bound_to_the_wrong_machine_is_refused_and_dropped() {
        let mut relays = ResidentRelays::default();
        let minted = relays.register(
            "machine-b",
            &holder(),
            "shell",
            RegisteredAuthority::Run { run_id: None },
        );
        assert!(relays.bind(&minted.grant.relay_id, "machine-c", 1).is_err());
        assert_eq!(relays.len(), 0);
    }

    #[test]
    fn a_runless_process_may_not_name_a_run() {
        let holder = holder();
        let mut relays = ResidentRelays::default();
        let minted = relays.register(
            "machine-b",
            &holder,
            "shell",
            RegisteredAuthority::Run { run_id: None },
        );
        let relay_id = minted.grant.relay_id;
        relays.bind(&relay_id, "machine-b", 1).unwrap();
        let auth = bearer(&minted.capability.unwrap());
        let mut fine = request(None);
        relays
            .authorize(
                "machine-b",
                1,
                &call(&relay_id, &holder, "shell", 1, &auth),
                &mut fine,
                &no_services,
            )
            .unwrap();
        assert_eq!(fine.run_id, None);
        assert!(
            fine.is_unverified_local(),
            "it is limited and labelled as any run-less caller is"
        );
        let named = fine.relayed_run_less_process().unwrap();
        assert_eq!(named.relay_id, relay_id);
        assert_eq!(named.process_key, "shell");
        assert!(relays
            .authorize(
                "machine-b",
                1,
                &call(&relay_id, &holder, "shell", 1, &auth),
                &mut request(Some("run")),
                &no_services
            )
            .is_err());
    }

    #[test]
    fn a_relay_stops_being_live_when_its_process_ends_or_its_link_is_lost() {
        let holder = holder();
        for end in ["exit", "stop", "release", "replace", "link", "early exit"] {
            let mut relays = ResidentRelays::default();
            let (relay_id, _) = started_terminal(&mut relays);
            assert!(relays.is_live(&relay_id), "{end}");
            match end {
                "exit" => relays.process_exited("machine-b", &holder, "shell", 4),
                "stop" => relays.revoke_process(&holder, "shell"),
                "release" => relays.revoke_holder(&holder),
                "replace" => {
                    relays.register(
                        "machine-b",
                        &holder,
                        "shell",
                        RegisteredAuthority::Run { run_id: None },
                    );
                }
                "link" => relays.detach("machine-b", 7),
                _ => {
                    relays
                        .contexts
                        .get_mut(&relay_id)
                        .unwrap()
                        .process_generation = None;
                    relays.process_exited("machine-b", &holder, "shell", 4);
                }
            }
            assert!(!relays.is_live(&relay_id), "{end}");
        }
    }

    fn incarnation(generation: u64) -> ServiceIncarnation {
        ServiceIncarnation {
            project_id: "p".into(),
            owner_key: String::new(),
            service: "echo".into(),
            generation,
        }
    }

    #[test]
    fn a_service_bearer_is_verified_at_its_executors_relay_audience_and_held_to_its_incarnation() {
        let holder = ResidencyHolder::Service {
            service_id: "project:p:echo".into(),
        };
        let mut relays = ResidentRelays::default();
        let minted = relays.register(
            "machine-b",
            &holder,
            "svc-2",
            RegisteredAuthority::Service(incarnation(2)),
        );
        assert!(
            minted.capability.is_none(),
            "a service presents its bearers, not a capability"
        );
        let relay_id = minted.grant.relay_id;
        relays.bind(&relay_id, "machine-b", 1).unwrap();
        relays.learn_generation(&relay_id, 1);
        let verify = |token: &str, audience: &LeaseAudience| {
            if *audience != crate::mcp::run_bearer::relay_audience("machine-b") {
                return Err("wrong audience".to_string());
            }
            let verified = |run_id: &str, incarnation, standing| {
                Ok(VerifiedServiceRun {
                    run_id: run_id.into(),
                    incarnation,
                    standing,
                })
            };
            match token {
                "standing" => verified("run-standing", incarnation(2), true),
                "delegated" => verified("run-call", incarnation(2), false),
                // A call retried on this incarnation after an earlier one
                // failed keeps the delegation minted for the earlier one.
                "retried" => verified("run-retried", incarnation(1), false),
                "previous" => verified("run-old", incarnation(1), true),
                "other" => verified(
                    "run-judge",
                    ServiceIncarnation {
                        service: "judge".into(),
                        ..incarnation(2)
                    },
                    false,
                ),
                _ => Err("expired".to_string()),
            }
        };
        for (token, run) in [
            ("standing", "run-standing"),
            ("delegated", "run-call"),
            ("retried", "run-retried"),
        ] {
            let auth = bearer(token);
            let mut request = request(None);
            relays
                .authorize(
                    "machine-b",
                    1,
                    &call(&relay_id, &holder, "svc-2", 1, &auth),
                    &mut request,
                    &verify,
                )
                .unwrap();
            assert_eq!(request.run_id.as_deref(), Some(run));
        }
        for token in ["previous", "other", "expired"] {
            let auth = bearer(token);
            assert!(
                relays
                    .authorize(
                        "machine-b",
                        1,
                        &call(&relay_id, &holder, "svc-2", 1, &auth),
                        &mut request(None),
                        &verify
                    )
                    .is_err(),
                "{token}"
            );
        }
    }
}
