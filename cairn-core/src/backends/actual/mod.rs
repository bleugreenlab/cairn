//! Actual Computer: user-owned inference clusters, reached either directly or
//! through Actual's end-to-end Private Relay.
//!
//! Actual serves OpenAI-compatible HTTP, so the transport underneath is the
//! shared [`openai_compat`](crate::backends::openai_compat) machinery. What is
//! Actual's own, and what this module keeps rather than flattening into a
//! generic endpoint, is the lifecycle around that transport: a target is a
//! device or a relay route, it can be reachable but holding no loaded model,
//! and its inventory is whatever that particular target can serve rather than
//! whatever Actual publishes.
//!
//! ## What is established, and what is not
//!
//! Probing the public relay edge (2026-08-16) establishes that
//! `POST /v1/chat/completions`, `POST /v1/responses`, `POST /v1/messages` and
//! `GET /v1/models` all exist and are credential-gated, and that the edge
//! distinguishes its answers cleanly: 401 for a real route reached without a
//! credential, 405 for a real route reached by the wrong method, and a bare
//! zero-length 404 for a route that is not there.
//!
//! `GET /v1/clusters` — which Actual's own agent guide documents as the way to
//! list clusters for `X-Cluster-ID` — is a zero-length 404 with and without a
//! bearer token. It is not served. So Cairn does not enumerate clusters, and
//! cluster pinning is an operator-supplied value carried on the target. Whether
//! one credential can route several clusters is likewise unestablished: the
//! relay rejects on credential before any cluster handling, returning identical
//! 401s with and without the header.
//!
//! Relay auth failure is `text/plain` — the literal token
//! `missing_or_malformed_credential`, not an OpenAI JSON error envelope — so
//! classification here reads the status code and treats the body as opaque.
//!
//! ## One protocol family, chosen rather than assumed
//!
//! Actual fronts three request families behind one base URL, so which one Cairn
//! speaks is an execution invariant rather than an implementation detail. Cairn
//! speaks exactly one: `POST /v1/chat/completions`. `/v1/responses` and
//! `/v1/messages` are known to exist and to be credential-gated, and nothing
//! beyond their existence is claimed or used.
//!
//! That single choice is safe here for a reason specific to Actual, and the
//! reason is worth stating because it does not generalize. A gateway that
//! proxies to heterogeneous upstream vendors has a per-model native family, and
//! sending a model to the wrong one can succeed with corrupted output rather
//! than fail. Actual is not that: its registry entries name HuggingFace weights
//! and quantizations with no serving protocol at all, because the three families
//! are Actual's own surfaces over whatever model that device has loaded. There
//! is no per-model upstream to mismatch, and no field in anything Cairn can read
//! that would name one.
//!
//! What that argument does *not* cover: whether a given model's chat template
//! emits reasoning markup into visible assistant content. That is a property of
//! the weights and the server's templating, not of the endpoint family, and it
//! cannot be established without an authorized device. It is therefore recorded
//! as unverified rather than assumed benign.

mod adapter;
// Public because the transport layer exposes target readiness as its own
// read-only command: reachability is a question the settings surface asks
// directly, not something the model catalog can answer.
pub mod models;

use crate::agent_process::process::BackendStdin;
use crate::backends::{
    AgentBackend, CompletionError, CompletionOutcome, CompletionRequest, CompletionShape,
    DiscoveredModel, ResolvedTools, SessionConfig,
};
use crate::identity::{ActualTargetKind, ApiProvider, ProviderAuth};
use crate::orchestrator::Orchestrator;
use std::sync::atomic::Ordering;

pub(crate) const ACTUAL_BACKEND_KEY: &str = "actual";
pub(crate) const ACTUAL_BACKEND_NAME: &str = "Actual";

/// The endpoint a local Actual instance serves on.
pub const DEFAULT_LOCAL_BASE_URL: &str = "http://127.0.0.1:8080";
/// Actual's Private Relay.
pub const DEFAULT_RELAY_BASE_URL: &str = "https://api.actual.inc";

#[derive(Debug, Clone, Copy)]
pub struct ActualBackend;

/// One configured Actual target, flattened out of its provider account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Target {
    pub(crate) account_id: String,
    pub(crate) label: String,
    pub(crate) kind: ActualTargetKind,
    pub(crate) base_url: String,
    pub(crate) api_key: Option<String>,
    pub(crate) cluster_id: Option<String>,
}

impl Target {
    /// The request headers this target needs.
    ///
    /// A local instance takes no per-request auth on loopback, so it gets no
    /// `Authorization` header at all rather than an empty one. `X-Cluster-ID`
    /// rides only when the operator pinned a cluster.
    /// Remove this target's own credential from text that came back over the
    /// wire, before that text is shown to anyone.
    ///
    /// Upstream bodies are surfaced deliberately — into the transcript as the
    /// provider's own sentence, and into the settings panel as a probe detail —
    /// because an opaque failure is worse than a verbose one. But a proxy or
    /// gateway that echoes the request in its error body would carry the `ac_`
    /// key into both, where it becomes durable. Redaction is exact rather than
    /// pattern-matched because the credential is known right here; guessing at
    /// what looks like a secret would both miss and over-redact.
    pub(crate) fn redact_credential(&self, text: String) -> String {
        match self.api_key.as_deref() {
            Some(key) if !key.is_empty() && text.contains(key) => {
                text.replace(key, "[redacted credential]")
            }
            _ => text,
        }
    }

    pub(crate) fn headers(&self) -> Vec<(String, String)> {
        let mut headers = vec![("content-type".to_string(), "application/json".to_string())];
        if let Some(api_key) = &self.api_key {
            headers.push(("authorization".to_string(), format!("Bearer {api_key}")));
        }
        if let Some(cluster_id) = &self.cluster_id {
            headers.push(("x-cluster-id".to_string(), cluster_id.clone()));
        }
        headers
    }

    /// `{base}/{path}` with exactly one separator.
    pub(crate) fn url(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }
}

/// Every configured Actual target in account-priority order.
pub(crate) fn targets_in_project(orch: &Orchestrator, project_id: Option<&str>) -> Vec<Target> {
    let Some(store) = orch.get_identity_store() else {
        return Vec::new();
    };
    store
        .accounts_for_provider(ApiProvider::Actual, project_id)
        .into_iter()
        .filter_map(|account| match &account.auth {
            ProviderAuth::ActualTarget {
                kind,
                base_url,
                api_key,
                cluster_id,
            } => Some(Target {
                account_id: account.id.clone(),
                label: account.label.clone(),
                kind: *kind,
                base_url: base_url.clone(),
                api_key: api_key.clone(),
                cluster_id: cluster_id.clone(),
            }),
            _ => None,
        })
        .collect()
}

/// The target a model should run on: the highest-priority one discovery saw
/// serving it.
///
/// When discovery has not run yet the highest-priority target is used instead,
/// which is what makes first launch possible before any catalog exists. That
/// fallback is honest here specifically because Actual fails loudly when it is
/// wrong: a target that cannot serve the model answers 503 (nothing loaded) or
/// an explicit model error, both of which reach the user as themselves rather
/// than as a silent wrong-model run.
pub(crate) fn target_for_model(
    orch: &Orchestrator,
    model: &str,
    project_id: Option<&str>,
) -> Result<Target, String> {
    let targets = targets_in_project(orch, project_id);
    if targets.is_empty() {
        return Err("No Actual target configured. Add a local instance or a Private Relay target under Settings, Providers, Actual."
            .to_string());
    }
    let serving = orch
        .get_model_catalog()
        .into_iter()
        .find(|catalog| catalog.backend == ACTUAL_BACKEND_KEY)
        .and_then(|catalog| {
            catalog
                .models
                .into_iter()
                .find(|entry| entry.model == model || entry.id == model)
        })
        .map(|entry| entry.serving_account_ids);
    select_priority_target(targets, serving.as_deref()).ok_or_else(|| {
        format!(
            "No configured Actual target serves {model}. Discovery reported this model on \
             Actual targets that are not configured here; add that target, or refresh the \
             model list if the model has moved."
        )
    })
}

/// Pick the target a model should run on.
///
/// The fallback to the highest-priority target applies only when the catalog
/// says *nothing* about this model — no entry, or an entry with no serving
/// targets — which is the cold-catalog case that makes a first launch possible
/// before discovery has ever run.
///
/// A non-empty serving list is an affirmative answer, and falling back through
/// it would contradict the catalog: with a project-scoped target set, a deleted
/// account, or serving targets outside this scope, the catalog is warm and
/// specific and none of what it named is available. Declining is the honest
/// result, and it fails before a request is sent rather than after a target that
/// was never going to serve the model answers.
fn select_priority_target(targets: Vec<Target>, serving: Option<&[String]>) -> Option<Target> {
    let Some(serving) = serving.filter(|ids| !ids.is_empty()) else {
        return targets.into_iter().next();
    };
    targets
        .into_iter()
        .find(|target| serving.contains(&target.account_id))
}

impl AgentBackend for ActualBackend {
    fn name(&self) -> &str {
        ACTUAL_BACKEND_NAME
    }

    fn is_available(&self) -> Result<(), String> {
        Ok(())
    }

    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        // Inventory is per-target, so it cannot be answered without the
        // configured targets; the orchestrator routes Actual through
        // `models::discover_catalog_blocking` instead.
        Err("Actual discovery requires configured targets".to_string())
    }

    fn response_completion_availability(
        &self,
        orch: &Orchestrator,
        project_id: Option<&str>,
    ) -> Result<(), String> {
        if targets_in_project(orch, project_id).is_empty() {
            return Err("needs a configured Actual instance or Private Relay target".to_string());
        }
        Ok(())
    }

    fn response_model_availability(
        &self,
        orch: &Orchestrator,
        project_id: Option<&str>,
        model: &str,
    ) -> Result<(), String> {
        self.response_completion_availability(orch, project_id)?;
        // Asking the router itself, rather than re-deriving the answer beside
        // it. Held apart, the two drifted: the router treats an absent catalog
        // entry as cold and falls back to the highest-priority target, so a
        // session could launch on a freshly configured target before discovery
        // had ever run, while this check read the same absence as "not loaded"
        // and refused the one-shot before a request was sent. Whether a target
        // exists for a model is one question, so it gets one answer.
        target_for_model(orch, model, project_id).map(|_| ())
    }

    fn complete(
        &self,
        request: CompletionRequest,
        orch: &Orchestrator,
    ) -> Result<CompletionOutcome, CompletionError> {
        let adapter =
            adapter::ActualAdapter::new(orch, &request.model, request.project_id.as_deref())
                .map_err(|_| CompletionError::BackendUnavailable)?;
        adapter.complete(request)
    }

    fn completion_shape(&self) -> CompletionShape {
        CompletionShape::InProcess
    }

    fn resolve_tools(&self, agent_tools: &[String], _agent_disallowed: &[String]) -> ResolvedTools {
        use crate::agent_process::toolkits;
        let mut allowed = toolkits::resolve_tools(agent_tools);
        toolkits::ensure_core_verbs(&mut allowed);
        allowed.retain(|tool| tool != "apply_patch");
        ResolvedTools {
            allowed,
            disallowed: Vec::new(),
        }
    }

    fn start_session(&self, config: SessionConfig, orch: &Orchestrator) -> Result<(), String> {
        let model = config
            .model
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let adapter = adapter::ActualAdapter::new(orch, &model, Some(&config.project_id))?;
        crate::backends::http_loop::start_session(config, orch, adapter)
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn supports_warm_processes(&self) -> bool {
        false
    }

    fn runtime_launch_capability(
        &self,
        _launch: &crate::backends::RuntimeLaunch,
    ) -> Result<crate::backends::RuntimeLaunchCapability, String> {
        Ok(crate::backends::stateless_http_runtime_capability(
            "actual-http",
        ))
    }

    fn call_batch_capability(&self) -> crate::backends::CallBatchCapability {
        crate::backends::CallBatchCapability {
            shape: crate::backends::CallBatchShape::InProcess,
            max_concurrency: None,
        }
    }

    fn send_user_message(
        &self,
        _: &mut dyn BackendStdin,
        _: &crate::agent_process::stdin::MessageContent,
        _: &str,
        _: Option<&str>,
        _: Option<&str>,
    ) -> Result<(), String> {
        Err("Actual HTTP turns do not keep a warm stdin; start a new run/turn".to_string())
    }

    fn send_interrupt(&self, stdin: &mut dyn BackendStdin) -> Result<(), String> {
        if let Some(stdin) = stdin
            .as_any_mut()
            .downcast_mut::<crate::backends::http_loop::HttpTurnStdin>()
        {
            stdin.cancel.store(true, Ordering::SeqCst);
            Ok(())
        } else {
            Err("Actual stdin unavailable".to_string())
        }
    }

    fn send_set_model(&self, _: &mut dyn BackendStdin, _: &str) -> Result<(), String> {
        Err("Actual model changes apply to the next HTTP turn".to_string())
    }

    fn send_set_permission_mode(&self, _: &mut dyn BackendStdin, _: &str) -> Result<(), String> {
        Err("Actual permission changes apply to the next HTTP turn".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(id: &str, kind: ActualTargetKind) -> Target {
        Target {
            account_id: id.to_string(),
            label: id.to_string(),
            kind,
            base_url: format!("http://{id}"),
            api_key: None,
            cluster_id: None,
        }
    }

    fn targets() -> Vec<Target> {
        vec![
            target("high", ActualTargetKind::Local),
            target("low", ActualTargetKind::Local),
        ]
    }

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn routing_picks_the_highest_priority_target_that_serves_the_model() {
        assert_eq!(
            select_priority_target(targets(), Some(&ids(&["high", "low"])))
                .unwrap()
                .account_id,
            "high"
        );
        // Priority is the configured order, not the order discovery reported.
        assert_eq!(
            select_priority_target(targets(), Some(&ids(&["low"])))
                .unwrap()
                .account_id,
            "low"
        );
    }

    #[test]
    fn routing_falls_back_to_the_first_target_only_when_the_catalog_is_cold() {
        // No entry, and an entry naming no serving targets, both mean discovery
        // has nothing to say. First launch has to be possible before it runs.
        for serving in [None, Some(ids(&[]).as_slice())] {
            assert_eq!(
                select_priority_target(targets(), serving)
                    .unwrap()
                    .account_id,
                "high"
            );
        }
        assert_eq!(select_priority_target(Vec::new(), None), None);
    }

    #[test]
    fn routing_declines_rather_than_sending_a_model_to_a_target_the_catalog_excluded() {
        // The catalog is warm and specific here: it named the targets serving
        // this model, and none of them is configured in this scope. Falling back
        // would send the request to a target discovery affirmatively left out.
        assert_eq!(
            select_priority_target(targets(), Some(ids(&["gone"]).as_slice())),
            None
        );
    }

    #[test]
    fn an_upstream_body_echoing_the_credential_is_redacted_before_anyone_sees_it() {
        let mut relay = target("relay", ActualTargetKind::Relay);
        relay.api_key = Some("ac_live_supersecret".to_string());
        // What a reflecting proxy actually returns: the request echoed back.
        let reflected = relay.redact_credential(
            "HTTP 502: upstream rejected {\"authorization\":\"Bearer ac_live_supersecret\"}"
                .to_string(),
        );
        assert!(
            !reflected.contains("ac_live_supersecret"),
            "credential survived redaction: {reflected}"
        );
        assert!(
            reflected.contains("[redacted credential]") && reflected.contains("HTTP 502"),
            "the failure itself must still be legible: {reflected}"
        );
        // A local target has no credential, and nothing else is touched.
        let local = target("local", ActualTargetKind::Local);
        assert_eq!(
            local.redact_credential("HTTP 503: no model loaded".to_string()),
            "HTTP 503: no model loaded"
        );
    }

    #[test]
    fn a_local_target_sends_no_authorization_header() {
        let headers = target("local", ActualTargetKind::Local).headers();
        assert!(
            !headers.iter().any(|(name, _)| name == "authorization"),
            "loopback takes no per-request auth: {headers:?}"
        );
        assert!(!headers.iter().any(|(name, _)| name == "x-cluster-id"));
    }

    #[test]
    fn a_relay_target_sends_bearer_auth_and_pins_only_when_asked() {
        let mut relay = target("relay", ActualTargetKind::Relay);
        relay.api_key = Some("ac_secret".to_string());
        let headers = relay.headers();
        assert!(headers.contains(&("authorization".to_string(), "Bearer ac_secret".to_string())));
        assert!(!headers.iter().any(|(name, _)| name == "x-cluster-id"));

        relay.cluster_id = Some("cluster-7".to_string());
        assert!(relay
            .headers()
            .contains(&("x-cluster-id".to_string(), "cluster-7".to_string())));
    }

    #[test]
    fn urls_join_with_exactly_one_separator() {
        let mut subject = target("t", ActualTargetKind::Local);
        subject.base_url = "http://127.0.0.1:8080/".to_string();
        assert_eq!(subject.url("/v1/models"), "http://127.0.0.1:8080/v1/models");
        subject.base_url = "http://127.0.0.1:8080".to_string();
        assert_eq!(subject.url("v1/models"), "http://127.0.0.1:8080/v1/models");
    }
}
