//! One served-API request, from an authenticated client's body to the
//! provider's reply.

use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use cairn_common::authorization::AuthorityDecision;
use cairn_db::storage::served_api::ServedApiClient;
use cairn_models::interface::relay::{RelayCall, RelayDoor, RelayError, Relayed};
use cairn_models::interface::{ModelScope, Models};
use serde_json::{Map, Value};

use super::requests::{Outcome, RequestLedger};
use super::resolve::{resolve, ServedRefusal};
use super::{clients, LOCAL_PROVIDER};
use crate::backends::launch::{relay_route, RelayLaunchError};
use crate::backends::relay::{RelayRefusal, RelayRequest, RelayWait};
use crate::orchestrator::Orchestrator;
use crate::storage::run_db_blocking;

/// A request from an authenticated client.
pub struct ServedRequest {
    pub door: RelayDoor,
    pub body: Map<String, Value>,
    pub client: ServedApiClient,
    /// Set when the client goes away.
    pub cancel: Arc<AtomicBool>,
}

/// What the request heard while it was being routed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServedEvent {
    /// It waits for a local model to load; the text says on what.
    Waiting(String),
}

/// The provider's answer, and the usage row to finish once its body ends.
pub struct ServedAnswer {
    pub relayed: Relayed,
    pub provider: String,
    pub sent_model: String,
    pub ledger: RequestLedger,
}

impl ServedAnswer {
    /// The usage row's id, which the client is told in `x-cairn-request`.
    pub fn request_id(&self) -> String {
        self.ledger.id().to_string()
    }

    /// Finish the usage row once the body has been relayed (or the client
    /// left). Blocking.
    pub fn finish(self, client_left: bool) {
        let ServedAnswer {
            relayed, ledger, ..
        } = self;
        let outcome = if client_left || relayed.body.cancelled() {
            Outcome::ClientClosed
        } else if (200..300).contains(&relayed.status) {
            Outcome::Answered
        } else {
            Outcome::UpstreamError
        };
        ledger.finish(relayed.status, outcome, Some(relayed.body.usage()), None);
    }
}

/// A request Cairn answers itself, in OpenAI's error shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedError {
    pub status: u16,
    /// OpenAI's error `type`.
    pub kind: &'static str,
    pub message: String,
    /// Seconds a client should wait before asking again.
    pub retry_after: Option<u64>,
    /// The usage row's id.
    pub request_id: String,
}

impl ServedError {
    fn new(status: u16, kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            kind,
            message: message.into(),
            retry_after: None,
            request_id: String::new(),
        }
    }
}

/// The first remote allow per client and grant this process has journaled.
/// Every denial is journaled; an allow only the first time, so a busy client
/// leaves one decision row per grant rather than one per request.
static ALLOWS_JOURNALED: Mutex<BTreeSet<(String, String)>> = Mutex::new(BTreeSet::new());

/// Who a load queue says is waiting.
pub fn requester(client: &ServedApiClient) -> String {
    format!("served API client {}", client.name)
}

/// Serve `request`: resolve its model, check the client may use the provider,
/// build the route (waiting for a local model to load, telling `on_event`
/// what it waits on), and relay. Blocking: run it off the async runtime.
pub fn serve_blocking(
    orch: &Orchestrator,
    request: ServedRequest,
    on_event: &mut (dyn FnMut(ServedEvent) + Send),
) -> Result<ServedAnswer, ServedError> {
    let ServedRequest {
        door,
        body,
        client,
        cancel,
    } = request;
    let stream = body.get("stream").and_then(Value::as_bool) == Some(true);
    let asked = body
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .map(str::to_string);
    let mut ledger = RequestLedger::begin(
        orch.db.local.clone(),
        door,
        Some(&client),
        stream,
        asked.clone(),
    );
    let refuse = |ledger: RequestLedger, error: ServedError| -> ServedError {
        let request_id = ledger.id().to_string();
        ledger.finish(
            error.status,
            Outcome::Refused,
            None,
            Some(error.message.clone()),
        );
        ServedError {
            request_id,
            ..error
        }
    };
    let Some(asked) = asked else {
        return Err(refuse(
            ledger,
            ServedError::new(400, "invalid_request_error", "request has no model"),
        ));
    };

    let settings = crate::config::settings::load_served_api(&orch.config_dir);
    let catalog = match orch.catalog(&ModelScope::workspace()) {
        Ok(catalog) => catalog,
        Err(error) => {
            return Err(refuse(
                ledger,
                ServedError::new(
                    503,
                    "model_unavailable",
                    format!("the model catalog is unavailable: {error}"),
                ),
            ))
        }
    };
    let db = orch.db.local.clone();
    let granted = {
        let db = db.clone();
        let client = client.clone();
        run_db_blocking(move || async move { clients::remote_grants(&db, &client).await })
            .unwrap_or_default()
    };
    let granted_providers: BTreeSet<String> = granted
        .iter()
        .filter_map(|grant| match &grant.scope.place {
            cairn_common::authorization::AuthorityPlace::ExternalAccount { provider, .. } => {
                Some(provider.clone())
            }
            _ => None,
        })
        .collect();
    let served = super::served_names_blocking(orch);
    let resolved = match resolve(&asked, door, &served, &catalog, |provider| {
        granted_providers.contains(provider)
    }) {
        Ok(resolved) => resolved,
        Err(refusal) => {
            let error = match &refusal {
                ServedRefusal::NotFound { model } => ServedError::new(
                    404,
                    "model_not_found",
                    format!("no provider serves {model}; GET /v1/models lists the model names"),
                ),
                ServedRefusal::Renamed { asked, real } => ServedError::new(
                    404,
                    "model_not_found",
                    format!("{asked} is not a model name; models go by their real names, so ask for {real}"),
                ),
                ServedRefusal::Unserved { model, detail } => ServedError::new(
                    400,
                    "invalid_request_error",
                    format!("{model} does not serve /v1/{}: {detail}", door.path()),
                ),
                ServedRefusal::RemoteNotGranted { model, provider } => {
                    ledger.provider(provider);
                    remote_refusal(model, provider, &client)
                }
            };
            if let ServedRefusal::RemoteNotGranted { provider, .. } = &refusal {
                journal_remote_denial(orch, &client, provider, "", &asked);
            }
            return Err(refuse(ledger, error));
        }
    };
    ledger.resolved(&resolved.model);

    let provider = resolved.offer.provider.clone();
    let backend = crate::backends::backend_for_name(Some(&provider));
    let started = Instant::now();
    let requester = requester(&client);
    let mut waiting = |said: &str| on_event(ServedEvent::Waiting(said.to_string()));
    let route = relay_route(
        orch,
        &provider,
        backend.as_ref(),
        RelayRequest {
            model: &resolved.offer.model,
            door,
            requester: &requester,
            wait: RelayWait {
                budget: settings.load_wait(),
                cancel: cancel.clone(),
                waiting: &mut waiting,
            },
        },
    );
    let wait_ms = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
    let route = match route {
        Ok(route) => route,
        Err(error) => {
            ledger.provider(&provider);
            let error = match error {
                RelayLaunchError::Unserved(refusal) => {
                    ServedError::new(400, "invalid_request_error", refusal.to_string())
                }
                RelayLaunchError::Refused(RelayRefusal::Unavailable(message)) => {
                    ServedError::new(503, "model_unavailable", message)
                }
                RelayLaunchError::Refused(RelayRefusal::WaitExpired(message)) => ServedError {
                    retry_after: Some(30),
                    ..ServedError::new(503, "model_unavailable", message)
                },
                RelayLaunchError::Refused(RelayRefusal::Cancelled(message)) => {
                    let request_id = ledger.id().to_string();
                    ledger.finish(499, Outcome::ClientClosed, None, Some(message.clone()));
                    return Err(ServedError {
                        request_id,
                        ..ServedError::new(499, "client_closed", message)
                    });
                }
            };
            return Err(refuse(ledger, error));
        }
    };

    let mut grant_id = None;
    if provider != LOCAL_PROVIDER {
        let account = route.account_id.clone().unwrap_or_default();
        let scope = clients::remote_scope(&provider, &account);
        let actor = clients::actor(db.clone(), &client);
        let decision = {
            let actor = actor.clone();
            let scope = scope.clone();
            run_db_blocking(move || async move {
                crate::authorization::served_api_use(&actor, &scope).await
            })
        };
        match decision {
            Ok(AuthorityDecision::AllowedByGrant {
                grant_id: id,
                reason,
            }) => {
                let first = ALLOWS_JOURNALED
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .insert((client.id.clone(), id.clone()));
                if first {
                    journal(
                        orch,
                        &client,
                        &scope,
                        &format!("served-API client {} uses {provider}", client.name),
                        AuthorityDecision::AllowedByGrant {
                            grant_id: id.clone(),
                            reason,
                        },
                    );
                }
                grant_id = Some(id);
            }
            Ok(_) => {
                journal_remote_denial(orch, &client, &provider, &account, &asked);
                ledger.provider(&provider);
                // A grant on an account the workspace no longer spends is not
                // "local-only": say which account it names.
                let stale = granted.iter().find_map(|grant| match &grant.scope.place {
                    cairn_common::authorization::AuthorityPlace::ExternalAccount {
                        provider: granted_provider,
                        account_id,
                    } if granted_provider == &provider => Some(account_id.clone()),
                    _ => None,
                });
                let error = match stale {
                    Some(old) => ServedError::new(
                        403,
                        "permission_denied",
                        format!(
                            "client {}'s remote grant names {provider} account {old}, but the workspace now spends account {account}; the operator re-grants remote use at cairn://models/clients/{}",
                            client.name, client.id
                        ),
                    ),
                    None => remote_refusal(&resolved.model, &provider, &client),
                };
                return Err(refuse(ledger, error));
            }
            Err(error) => {
                return Err(refuse(
                    ledger,
                    ServedError::new(503, "model_unavailable", error),
                ))
            }
        }
    }
    ledger.routed(&route, wait_ms, grant_id);

    let sent_model = route.sent_model.clone();
    match orch.models_runtime().relay.relay(RelayCall {
        door,
        route,
        body,
        cancel,
    }) {
        Ok(relayed) => {
            ledger.status(relayed.status);
            Ok(ServedAnswer {
                relayed,
                provider,
                sent_model,
                ledger,
            })
        }
        Err(RelayError::InvalidRequest(message)) => Err(refuse(
            ledger,
            ServedError::new(400, "invalid_request_error", message),
        )),
        Err(RelayError::Unreachable(message)) => {
            let request_id = ledger.id().to_string();
            ledger.finish(502, Outcome::UpstreamError, None, Some(message.clone()));
            Err(ServedError {
                request_id,
                ..ServedError::new(502, "upstream_error", message)
            })
        }
    }
}

fn remote_refusal(model: &str, provider: &str, client: &ServedApiClient) -> ServedError {
    ServedError::new(
        403,
        "permission_denied",
        format!(
            "{model} is served only by {provider}, a remote provider, and client {} is local-only; the operator grants remote use at cairn://models/clients/{}",
            client.name, client.id
        ),
    )
}

fn journal(
    orch: &Orchestrator,
    client: &ServedApiClient,
    scope: &cairn_common::authorization::AuthorityScope,
    summary: &str,
    decision: AuthorityDecision,
) {
    let Ok(appearance) = clients::appearance(client, chrono::Utc::now().timestamp()) else {
        return;
    };
    let actor = clients::actor(orch.db.local.clone(), client);
    let scope = scope.clone();
    let summary = summary.to_string();
    let _ = run_db_blocking(move || async move {
        crate::authorization::journal_served_api_decision(
            &actor,
            &scope,
            &summary,
            &decision,
            &appearance,
        )
        .await;
        Ok(())
    });
}

fn journal_remote_denial(
    orch: &Orchestrator,
    client: &ServedApiClient,
    provider: &str,
    account: &str,
    asked: &str,
) {
    journal(
        orch,
        client,
        &clients::remote_scope(provider, account),
        &format!(
            "served-API client {} asked {provider} for {asked} without a remote grant",
            client.name
        ),
        AuthorityDecision::ApprovalRequired(
            cairn_common::authorization::AuthorityReason::ServedApiRemoteUse,
        ),
    );
}
