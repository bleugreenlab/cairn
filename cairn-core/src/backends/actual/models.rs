//! Per-target model discovery and readiness for Actual.
//!
//! The unit of truth is the target, not the vendor. Actual publishes a public
//! registry at `models.actual.inc`, but that is what Actual *offers*, not what
//! any device has downloaded or loaded, so it is never read here. Inventory
//! comes from asking each configured target what it can serve, and a target
//! that cannot answer reports why instead of borrowing someone else's list.

use super::Target;
use crate::backends::DiscoveredModel;
use crate::identity::ActualTargetKind;
use crate::orchestrator::Orchestrator;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;
use std::time::Duration;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

/// What one target has to say for itself right now.
///
/// These are kept apart deliberately. "Reachable but holding no loaded model"
/// and "cannot be reached at all" have completely different fixes -- load a
/// model versus start the device or check the relay -- and collapsing them into
/// one unhealthy state is what makes a provider panel useless at the moment it
/// matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetState {
    /// Answered with an inventory.
    Ready,
    /// Up, but holding no loaded model. Actual documents 503 for this, and the
    /// server hot-swaps models, so this is a moment rather than a defect.
    NoModelLoaded,
    /// Reached, and it refused the credential.
    Unauthorized,
    /// Not reachable at all.
    Unreachable,
    /// Reached, but it does not serve model discovery, so its inventory is
    /// unknown. Reported rather than papered over with the public registry.
    DiscoveryUnsupported,
}

impl TargetState {
    /// What the user should do about it, in their words rather than HTTP's.
    pub fn summary(self, kind: ActualTargetKind) -> &'static str {
        match (self, kind) {
            (TargetState::Ready, _) => "Ready",
            (TargetState::NoModelLoaded, _) => {
                "Reachable, but no model is loaded. Download and load a model on this device."
            }
            (TargetState::Unauthorized, ActualTargetKind::Relay) => {
                "Actual rejected this credential. Check the ac_ inference credential."
            }
            (TargetState::Unauthorized, ActualTargetKind::Local) => {
                "This instance asked for a credential. Loopback normally needs none."
            }
            (TargetState::Unreachable, ActualTargetKind::Local) => {
                "Cannot reach this instance. Check that Actual is running on this machine."
            }
            (TargetState::Unreachable, ActualTargetKind::Relay) => {
                "Cannot reach Actual Private Relay."
            }
            (TargetState::DiscoveryUnsupported, _) => {
                "Reachable, but it does not list models, so its inventory is unknown."
            }
        }
    }
}

/// One target's readiness, shaped for the settings surface.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActualTargetStatus {
    pub account_id: String,
    pub label: String,
    /// `local` or `relay`.
    pub kind: &'static str,
    pub base_url: String,
    /// The operator-supplied cluster pin, if any. Names a cluster; does not
    /// authenticate to one, so it is safe to show.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    pub state: TargetState,
    pub summary: String,
    /// The underlying transport detail, when there is one worth showing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Model ids this target reported it can serve.
    pub models: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ModelsResponse {
    #[serde(default)]
    data: Vec<ModelEntry>,
}

#[derive(Debug, Deserialize)]
struct ModelEntry {
    id: String,
    /// Taken only when the target actually reports it. Actual's registry
    /// publishes a context length per model, but that describes the published
    /// model rather than this deployment, so it is not substituted in here.
    #[serde(default, alias = "context_window")]
    context_length: Option<i64>,
}

/// Probe every configured target, in account-priority order.
pub fn probe_targets_blocking(
    orch: &Orchestrator,
    project_id: Option<&str>,
) -> Vec<ActualTargetStatus> {
    super::targets_in_project(orch, project_id)
        .into_iter()
        .map(|target| probe_target(&target))
        .collect()
}

/// Ask one target what it can serve.
pub(crate) fn probe_target(target: &Target) -> ActualTargetStatus {
    probe_target_with_models(target).0
}

/// One probe, both answers: the readiness a surface shows and the model
/// metadata the catalog needs. Kept together so building the catalog costs one
/// request per target rather than two.
fn probe_target_with_models(target: &Target) -> (ActualTargetStatus, Vec<(String, Option<i64>)>) {
    let (state, detail, models) = match fetch_models(target) {
        Ok(models) => (TargetState::Ready, None, models),
        // Redacted at the one chokepoint every probe detail passes through,
        // rather than at each classification site, so a body that echoes the
        // relay credential cannot reach the settings panel by any path.
        Err(failure) => (
            failure.state,
            failure
                .detail
                .map(|detail| target.redact_credential(detail)),
            Vec::new(),
        ),
    };
    let status = ActualTargetStatus {
        account_id: target.account_id.clone(),
        label: target.label.clone(),
        kind: target.kind.as_str(),
        base_url: target.base_url.clone(),
        cluster_id: target.cluster_id.clone(),
        state,
        summary: state.summary(target.kind).to_string(),
        detail,
        models: models.iter().map(|(id, _)| id.clone()).collect(),
    };
    (status, models)
}

#[derive(Debug)]
struct ProbeFailure {
    state: TargetState,
    detail: Option<String>,
}

/// `GET {target}/v1/models`, classified.
fn fetch_models(target: &Target) -> Result<Vec<(String, Option<i64>)>, ProbeFailure> {
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(DISCOVERY_TIMEOUT)
        .timeout(DISCOVERY_TIMEOUT)
        .build()
        .map_err(|error| ProbeFailure {
            state: TargetState::Unreachable,
            detail: Some(error_with_causes("client failed", &error)),
        })?;
    let mut request = client.get(target.url("/v1/models"));
    for (name, value) in target.headers() {
        request = request.header(name, value);
    }
    let response = request.send().map_err(|error| ProbeFailure {
        state: TargetState::Unreachable,
        detail: Some(error_with_causes("models request failed", &error)),
    })?;
    let status = response.status().as_u16();
    let body = response.text().unwrap_or_default();
    classify(status, &body)
}

/// Map an HTTP answer onto a readiness state.
///
/// The relay's error bodies are plain text (`missing_or_malformed_credential`),
/// not JSON envelopes, so nothing here parses the body to decide -- the status
/// decides, and the body is carried through as opaque evidence.
fn classify(status: u16, body: &str) -> Result<Vec<(String, Option<i64>)>, ProbeFailure> {
    match status {
        200 => {
            let parsed: ModelsResponse =
                serde_json::from_str(body).map_err(|error| ProbeFailure {
                    state: TargetState::DiscoveryUnsupported,
                    detail: Some(format!("models response was not a model list: {error}")),
                })?;
            Ok(parsed
                .data
                .into_iter()
                .map(|entry| (entry.id, entry.context_length))
                .collect())
        }
        503 => Err(ProbeFailure {
            state: TargetState::NoModelLoaded,
            detail: detail_of(body),
        }),
        401 | 403 => Err(ProbeFailure {
            state: TargetState::Unauthorized,
            detail: detail_of(body),
        }),
        404 | 405 | 501 => Err(ProbeFailure {
            state: TargetState::DiscoveryUnsupported,
            detail: detail_of(body),
        }),
        other => Err(ProbeFailure {
            state: TargetState::Unreachable,
            detail: Some(format!(
                "models returned HTTP {other}{}",
                detail_of(body)
                    .map(|detail| format!(": {detail}"))
                    .unwrap_or_default()
            )),
        }),
    }
}

/// Trim a body down to something worth putting in front of a person, without
/// letting a whole HTML error page through.
fn detail_of(body: &str) -> Option<String> {
    // Characters, not bytes. This body is untrusted and arbitrary: a multi-byte
    // character sitting across the cut is not an edge case but ordinary text in
    // most of the world, and slicing a `str` off a character boundary panics
    // rather than truncating. That panic would surface as "Actual target probe
    // panicked" for an error the target reported perfectly well, and can unwind
    // the discovery worker.
    const MAX: usize = 200;
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut rest = trimmed.chars();
    let clipped: String = rest.by_ref().take(MAX).collect();
    Some(if rest.next().is_some() {
        format!("{clipped}...")
    } else {
        clipped
    })
}

fn error_with_causes(context: &str, error: &dyn Error) -> String {
    let mut message = format!("{context}: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(&format!(": {cause}"));
        source = cause.source();
    }
    message
}

/// Build the Actual model catalog from every configured target.
pub(crate) fn discover_catalog_blocking(
    orch: &Orchestrator,
) -> (Vec<DiscoveredModel>, Option<String>) {
    let targets = super::targets_in_project(orch, None);
    if targets.is_empty() {
        return (
            vec![],
            Some("No Actual instance or Private Relay target configured".into()),
        );
    }
    let mut statuses = Vec::with_capacity(targets.len());
    let mut context_windows: BTreeMap<String, Option<i64>> = BTreeMap::new();
    for target in &targets {
        let (status, models) = probe_target_with_models(target);
        for (id, window) in models {
            // Targets arrive in priority order, so the highest-priority target
            // serving a model describes it.
            context_windows.entry(id).or_insert(window);
        }
        statuses.push(status);
    }
    let models = merge_targets(&statuses, &context_windows);
    let problems: Vec<String> = statuses
        .iter()
        .filter(|status| status.state != TargetState::Ready)
        .map(|status| format!("{}: {}", status.label, status.summary))
        .collect();
    let error = (!problems.is_empty()).then(|| problems.join("; "));
    (models, error)
}

/// Fold per-target inventories into one selectable list, remembering which
/// targets serve each model in priority order.
fn merge_targets(
    statuses: &[ActualTargetStatus],
    context_windows: &BTreeMap<String, Option<i64>>,
) -> Vec<DiscoveredModel> {
    let mut merged: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for status in statuses {
        for model in &status.models {
            merged
                .entry(model.clone())
                .or_default()
                .push(status.account_id.clone());
        }
    }
    merged
        .into_iter()
        // No per-model protocol family is claimed. Actual fronts three request
        // families, but they are its own surfaces over whichever weights a
        // device has loaded rather than routes to distinct upstreams, and
        // nothing Cairn can read names a family per model -- the public registry
        // records HuggingFace repositories and quantizations only. Cairn speaks
        // chat/completions to every Actual target, so there is no routing
        // decision here for this field to inform.
        .map(|(id, serving_account_ids)| DiscoveredModel {
            display_name: display_name_for(&id),
            model: id.clone(),
            description: None,
            hidden: false,
            is_default: false,
            default_reasoning_effort: None,
            supported_reasoning_efforts: vec![],
            context_window: context_windows.get(&id).copied().flatten(),
            canonical_slug: None,
            serving_account_ids,
            pricing: None,
            // Actual's model list does not state tool support either way, and
            // asserting it here would be a claim the wire never made.
            supported_parameters: vec![],
            router: false,
            architecture_modality: None,
            wire_protocol: None,
            id,
        })
        .collect()
}

/// Actual's registry ids pin an exact revision (`publisher/model@<sha>`). The
/// revision is what makes the id precise and is kept as the value; showing it
/// in a picker just buries the name, so only the display copy drops it.
fn display_name_for(id: &str) -> String {
    match id.split_once('@') {
        Some((name, _revision)) if !name.is_empty() => name.to_string(),
        _ => id.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(account: &str, models: &[&str]) -> ActualTargetStatus {
        ActualTargetStatus {
            account_id: account.into(),
            label: account.into(),
            kind: "local",
            base_url: format!("http://{account}"),
            cluster_id: None,
            state: TargetState::Ready,
            summary: "Ready".into(),
            detail: None,
            models: models.iter().map(|m| (*m).to_string()).collect(),
        }
    }

    #[test]
    fn a_model_list_becomes_inventory() {
        let body = r#"{"object":"list","data":[
            {"id":"google/gemma-4-e2b-it@abc123","object":"model","context_length":131072},
            {"id":"qwen/qwen3.8-27b@def456","object":"model"}
        ]}"#;
        let models = classify(200, body).unwrap();
        assert_eq!(models.len(), 2);
        assert_eq!(models[0].1, Some(131072));
        // Absent metadata stays absent rather than being filled from the public
        // registry, which describes the published model and not this device.
        assert_eq!(models[1].1, None);
    }

    #[test]
    fn a_503_means_no_model_loaded_not_a_dead_target() {
        let failure = classify(503, "no model loaded").unwrap_err();
        assert_eq!(failure.state, TargetState::NoModelLoaded);
        assert_eq!(failure.detail.as_deref(), Some("no model loaded"));
        assert!(failure
            .state
            .summary(ActualTargetKind::Local)
            .contains("no model is loaded"));
    }

    /// The relay answers plain text, not a JSON error envelope; classification
    /// must therefore rest on the status code alone.
    #[test]
    fn a_plain_text_401_classifies_without_parsing_the_body() {
        let failure = classify(401, "missing_or_malformed_credential").unwrap_err();
        assert_eq!(failure.state, TargetState::Unauthorized);
        assert_eq!(
            failure.detail.as_deref(),
            Some("missing_or_malformed_credential")
        );
        assert!(failure
            .state
            .summary(ActualTargetKind::Relay)
            .contains("rejected this credential"));
    }

    #[test]
    fn a_target_that_does_not_list_models_says_so_rather_than_looking_empty() {
        for status in [404, 405, 501] {
            assert_eq!(
                classify(status, "").unwrap_err().state,
                TargetState::DiscoveryUnsupported,
                "HTTP {status}"
            );
        }
        // A 200 that is not a model list is equally unknown, not empty.
        assert_eq!(
            classify(200, "<html>hi</html>").unwrap_err().state,
            TargetState::DiscoveryUnsupported
        );
    }

    #[test]
    fn an_unexpected_status_keeps_its_evidence() {
        let failure = classify(500, "boom").unwrap_err();
        assert_eq!(failure.state, TargetState::Unreachable);
        assert!(failure.detail.unwrap().contains("HTTP 500"));
    }

    #[test]
    fn long_error_bodies_are_bounded() {
        let detail = detail_of(&"x".repeat(500)).unwrap();
        assert!(detail.chars().count() < 250, "{}", detail.chars().count());
        assert!(detail.ends_with("..."));
        assert_eq!(detail_of("   "), None);
    }

    /// An upstream body is untrusted, arbitrary bytes. Clipping it by byte
    /// offset panics whenever a multi-byte character straddles the cut, which
    /// would turn an error the target reported perfectly well into a panicked
    /// probe -- or unwind the discovery worker.
    #[test]
    fn a_multibyte_character_across_the_limit_is_clipped_rather_than_panicking() {
        // 199 ASCII bytes then a two-byte character, so byte 200 lands inside it.
        let straddling = format!("{}\u{e9}{}", "x".repeat(199), "y".repeat(50));
        let detail = detail_of(&straddling).expect("a non-empty body has a detail");
        assert!(detail.ends_with("..."), "{detail}");
        assert!(detail.starts_with(&"x".repeat(199)), "{detail}");

        // Non-ASCII throughout, where every boundary is a multi-byte one.
        let cjk = "\u{6f22}".repeat(400);
        let detail = detail_of(&cjk).expect("a non-empty body has a detail");
        assert_eq!(
            detail.chars().count(),
            203,
            "200 characters plus the ellipsis"
        );

        // A body exactly at the limit is complete, so it gets no ellipsis.
        let exact = "\u{6f22}".repeat(200);
        assert_eq!(detail_of(&exact), Some(exact));
    }

    #[test]
    fn serving_targets_follow_configured_priority_and_merge() {
        let merged = merge_targets(
            &[
                status("high", &["shared", "only-high"]),
                status("low", &["shared"]),
            ],
            &BTreeMap::new(),
        );
        assert_eq!(merged.len(), 2);
        let shared = merged.iter().find(|m| m.model == "shared").unwrap();
        assert_eq!(shared.serving_account_ids, vec!["high", "low"]);
        let only = merged.iter().find(|m| m.model == "only-high").unwrap();
        assert_eq!(only.serving_account_ids, vec!["high"]);
    }

    #[test]
    fn a_target_that_is_not_ready_contributes_no_models() {
        let mut unloaded = status("cold", &[]);
        unloaded.state = TargetState::NoModelLoaded;
        assert!(merge_targets(&[unloaded], &BTreeMap::new()).is_empty());
    }

    #[test]
    fn tool_support_is_left_unstated_rather_than_guessed() {
        let merged = merge_targets(&[status("t", &["m"])], &BTreeMap::new());
        assert!(merged[0].supported_parameters.is_empty());
        assert!(merged[0].pricing.is_none());
    }

    #[test]
    fn display_names_drop_the_revision_but_values_keep_it() {
        let merged = merge_targets(
            &[status("t", &["google/gemma-4-e2b-it@905e84b5", "plain-id"])],
            &BTreeMap::new(),
        );
        let pinned = merged
            .iter()
            .find(|m| m.model == "google/gemma-4-e2b-it@905e84b5")
            .unwrap();
        assert_eq!(pinned.display_name, "google/gemma-4-e2b-it");
        assert_eq!(pinned.id, "google/gemma-4-e2b-it@905e84b5");
        let plain = merged.iter().find(|m| m.model == "plain-id").unwrap();
        assert_eq!(plain.display_name, "plain-id");
    }
}
