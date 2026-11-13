//! Select options whose choices come from the provider's own credential.
//!
//! A provider may declare a select as `select: {from: …}`: its choices are what
//! a request made with the provider's credential returns (Cloudflare's
//! accounts, for one). Nothing here knows any provider; the request and the
//! paths that read it are pack data ([`cairn_packs::provider::http::ChoiceSource`]).
//!
//! One settle rule runs wherever a blank such option can be filled without the
//! operator: when a credential is connected or stored ([`settle`]), and when a
//! slot is about to use a provider whose required choice is blank
//! ([`fill_required`]), which is how a credential connected before the option
//! existed fills it with no reconnect. A stored value is never touched; one
//! choice is stored; several are left for the operator to pick; none, or a
//! failed request, is reported.
//!
//! The request goes out only through [`super::send`], so the credential's
//! `hosts` gate it before the first byte and on every redirect.
//!
//! A listing is bounded ([`LIST_DEADLINE`], headers and body together), and so
//! is the whole settle a connect waits for ([`CONNECT_SETTLE_DEADLINE`]): the
//! desktop's request to the runner gives up after 30 seconds, and a connect
//! whose token is already stored must not be reported as a failed
//! authorization because the provider was slow to list its accounts.

use std::path::Path;
use std::time::Duration;

use cairn_packs::provider::http;
use cairn_packs::provider::{CredentialKind, PackProvider, Slot};
use cairn_packs::provider_options::{self, Choice, ProviderOption};
use cairn_packs::server_config::NotReady;

use super::{Active, LIBRARY_LOCATION};

/// How long listing one option's choices may take, response body included.
pub const LIST_DEADLINE: Duration = Duration::from_secs(10);

/// How long a connect waits for everything its credential can settle. A slow
/// option is left blank and filled at its first use instead.
pub const CONNECT_SETTLE_DEADLINE: Duration = Duration::from_secs(15);

/// What settling one blank fetched-choices option came to.
#[derive(Debug, Clone, PartialEq)]
pub enum Settled {
    /// Exactly one choice, now stored.
    Stored(Choice),
    /// Several choices, sorted by label; the option stays blank for the
    /// operator to choose.
    Choose(Vec<Choice>),
    /// The credential reaches no choice.
    None,
    /// The request failed, or storing its one choice did.
    Failed(String),
}

/// One option's settlement.
#[derive(Debug, Clone, PartialEq)]
pub struct Settlement {
    pub slot: Slot,
    pub key: String,
    pub outcome: Settled,
}

/// The choices a dynamic select's request returns right now, sorted by label.
pub async fn fetch_choices(
    provider: &PackProvider,
    option: &ProviderOption,
) -> Result<Vec<Choice>, String> {
    request_choices(provider, option).await?.ok_or_else(|| {
        format!(
            "{} is not connected, so its {} choices cannot be listed",
            provider.provider.label, option.label
        )
    })
}

/// `Ok(None)` when the credential holds no usable value.
async fn request_choices(
    provider: &PackProvider,
    option: &ProviderOption,
) -> Result<Option<Vec<Choice>>, String> {
    let label = &provider.provider.label;
    let source = option
        .control
        .choice_source()
        .ok_or_else(|| format!("{label}'s option `{}` has no fetched choices", option.key))?;
    let credential = provider
        .credential
        .as_ref()
        .ok_or_else(|| format!("{label} names no credential"))?;
    let Some(token) =
        super::credential_value(&provider.pack, credential, "web provider choices").await?
    else {
        return Ok(None);
    };
    let prepared = http::prepare_choices(source, token.expose());
    let (status, body) =
        tokio::time::timeout(LIST_DEADLINE, list(&provider.pack, credential, &prepared))
            .await
            .map_err(|_| {
                format!(
                    "{label} could not list its {} choices: no answer within {} seconds",
                    option.label,
                    LIST_DEADLINE.as_secs()
                )
            })?
            .map_err(|e| format!("{label} could not list its {} choices: {e}", option.label))?;
    choices_from_response(label, option, status, &body).map(Some)
}

/// Send a listing and read its whole body.
async fn list(
    pack: &str,
    credential: &cairn_packs::provider::CredentialDecl,
    prepared: &http::PreparedRequest,
) -> Result<(u16, String), String> {
    #[cfg(test)]
    if let Ok(stand_in) = test_listing::LISTING.try_with(Clone::clone) {
        return stand_in(prepared.clone()).await;
    }
    let resp = super::send(pack, credential, prepared).await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.map_err(|e| e.to_string())?;
    Ok((status, body))
}

/// A stand-in for the provider's listing endpoint. Pack requests are HTTPS to
/// a host the credential lists, which no test server can be, so a test scopes
/// a stand-in over the one network step of a listing; everything before it
/// (the credential read, the prepared request) and after it (the deadline,
/// the response contract, the settle rule) is the real path.
#[cfg(test)]
pub(crate) mod test_listing {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use cairn_packs::provider::http::PreparedRequest;

    pub(crate) type Answer = Pin<Box<dyn Future<Output = Result<(u16, String), String>> + Send>>;
    pub(crate) type StandIn = Arc<dyn Fn(PreparedRequest) -> Answer + Send + Sync>;

    tokio::task_local! {
        pub(crate) static LISTING: StandIn;
    }
}

/// A choice response read into choices: non-2xx and a body of the wrong shape
/// are failures naming the provider and the option.
pub(crate) fn choices_from_response(
    label: &str,
    option: &ProviderOption,
    status: u16,
    body: &str,
) -> Result<Vec<Choice>, String> {
    let source = option
        .control
        .choice_source()
        .ok_or_else(|| format!("{label}'s option `{}` has no fetched choices", option.key))?;
    if !(200..300).contains(&status) {
        let snippet: String = body.trim().chars().take(200).collect();
        let detail = if snippet.is_empty() {
            String::new()
        } else {
            format!(" — {snippet}")
        };
        return Err(format!(
            "{label} could not list its {} choices: HTTP {status}{detail}",
            option.label
        ));
    }
    http::map_choices(source, body)
        .map_err(|e| format!("{label} returned an unexpected {} list: {e}", option.label))
}

/// The provider's fetched-choices options whose stored value is blank.
pub(crate) fn pending(config_dir: &Path, provider: &PackProvider) -> Vec<ProviderOption> {
    let stored = super::load_options(config_dir, provider.provider.slot, &provider.pack);
    provider
        .provider
        .options
        .iter()
        .filter(|option| option.control.choice_source().is_some() && is_blank(&stored, option))
        .cloned()
        .collect()
}

fn is_blank(stored: &super::Options, option: &ProviderOption) -> bool {
    stored
        .get(&option.key)
        .and_then(serde_yaml::Value::as_str)
        .is_none_or(|value| value.trim().is_empty())
}

/// Apply the settle rule to one fetched result. One choice is stored beside
/// the provider's other options, unless a value arrived meanwhile.
pub(crate) fn record(
    config_dir: &Path,
    provider: &PackProvider,
    option: &ProviderOption,
    result: Result<Vec<Choice>, String>,
) -> Settled {
    let pack = &provider.pack;
    let slot = provider.provider.slot;
    let outcome = match result {
        Err(error) => Settled::Failed(error),
        Ok(choices) if choices.is_empty() => Settled::None,
        Ok(mut choices) if choices.len() == 1 => {
            let choice = choices.remove(0);
            let stored = super::load_options(config_dir, slot, pack);
            if !is_blank(&stored, option) {
                return Settled::Choose(vec![choice]);
            }
            // Only usable values go back: a retired select value would fail
            // validation, and it already falls back to its default.
            let mut next = provider_options::usable_options(&provider.provider.options, &stored);
            next.insert(
                option.key.clone(),
                serde_yaml::Value::String(choice.value.clone()),
            );
            match super::upsert_options(config_dir, slot, pack, &next) {
                Ok(()) => Settled::Stored(choice),
                Err(error) => Settled::Failed(error),
            }
        }
        Ok(choices) => Settled::Choose(choices),
    };
    if let Settled::Failed(error) = &outcome {
        log::warn!(
            "web providers: could not settle `{pack}` {} option `{}`: {error}",
            slot.noun(),
            option.key
        );
    }
    outcome
}

/// Settle every blank fetched-choices option of one provider.
async fn settle_provider(config_dir: &Path, provider: &PackProvider) -> Vec<Settlement> {
    let mut out = Vec::new();
    for option in pending(config_dir, provider) {
        let result = fetch_choices(provider, &option).await;
        out.push(Settlement {
            slot: provider.provider.slot,
            key: option.key.clone(),
            outcome: record(config_dir, provider, &option, result),
        });
    }
    out
}

/// Every installed provider in `pack`, in any slot, that sends `credential`.
fn providers_using(config_dir: &Path, pack: &str, credential: &str) -> Vec<PackProvider> {
    Slot::ALL
        .into_iter()
        .filter_map(|slot| {
            super::installed_provider(config_dir, pack, slot)
                .ok()
                .flatten()
        })
        .filter(|provider| provider.provider.credential.as_deref() == Some(credential))
        .collect()
}

/// Settle the blank fetched-choices options of every provider that sends a
/// credential which was just connected or stored. Never fails: the credential
/// is already stored, and each option's outcome is reported.
pub async fn settle(config_dir: &Path, pack: &str, credential: &str) -> Vec<Settlement> {
    let mut out = Vec::new();
    for provider in providers_using(config_dir, pack, credential) {
        out.extend(settle_provider(config_dir, &provider).await);
    }
    out
}

/// [`settle`], within [`CONNECT_SETTLE_DEADLINE`]: what a connect or a stored
/// key waits for before it answers. Past the deadline the rest is left for the
/// first use to fill.
pub async fn settle_after_connect(config_dir: &Path, pack: &str, credential: &str) {
    if tokio::time::timeout(
        CONNECT_SETTLE_DEADLINE,
        settle(config_dir, pack, credential),
    )
    .await
    .is_err()
    {
        log::warn!(
            "web providers: settling `{pack}/{credential}` took longer than {} seconds; \
             what is left blank fills at first use",
            CONNECT_SETTLE_DEADLINE.as_secs()
        );
    }
}

/// What a connect still leaves undone: the first not-ready readiness, after
/// settling, among the pack's providers that send this credential.
pub fn still_needs(config_dir: &Path, pack: &str, credential: &str) -> Option<NotReady> {
    providers_using(config_dir, pack, credential)
        .iter()
        .find_map(|provider| super::readiness(config_dir, provider))
}

/// What a slot about to run a provider does about its required options.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Gap {
    /// Nothing required is blank; any blank fetched choice was just filled.
    Ready,
    /// A blank fetched choice whose credential holds nothing usable: the
    /// credential is what needs setting up.
    Credential(CredentialKind),
    /// A blank required text option, named by its label.
    Blank(String),
    /// A blank fetched choice with several candidates.
    Choose {
        option: String,
        choices: Vec<Choice>,
    },
    /// A blank fetched choice the credential reaches none of.
    Unreachable { option: String },
    /// Listing a blank fetched choice failed.
    Failed { option: String, error: String },
}

/// Fill the active provider's blank required fetched choices by the settle
/// rule, reloading its options after each store, and say what remains. This
/// runs inside a use of the credential its accessor already permits.
pub(crate) async fn fill_required(config_dir: &Path, active: &mut Active) -> Gap {
    loop {
        let provider = &active.provider;
        let Some(option) =
            provider_options::missing_required(&provider.provider.options, &active.options)
        else {
            return Gap::Ready;
        };
        if option.control.choice_source().is_none() {
            return Gap::Blank(option.label.clone());
        }
        let option = option.clone();
        let result = match request_choices(provider, &option).await {
            Ok(None) => {
                return Gap::Credential(
                    provider
                        .credential
                        .as_ref()
                        .map(|credential| credential.kind.clone())
                        .expect("a fetched choice names a credential"),
                )
            }
            Ok(Some(choices)) => Ok(choices),
            Err(error) => Err(error),
        };
        match record(config_dir, provider, &option, result) {
            Settled::Stored(_) => {
                active.options =
                    super::load_options(config_dir, provider.provider.slot, &provider.pack);
            }
            Settled::Choose(choices) => {
                return Gap::Choose {
                    option: option.label,
                    choices,
                }
            }
            Settled::None => {
                return Gap::Unreachable {
                    option: option.label,
                }
            }
            Settled::Failed(error) => {
                return Gap::Failed {
                    option: option.label,
                    error,
                }
            }
        }
    }
}

/// How many choices guidance names before summarising the rest.
const NAMED_CHOICES: usize = 5;

/// Guidance for a blank fetched choice: `Ok` text for a configuration gap
/// (several choices, none reachable), `Err` for a failed listing. `noun` is the
/// slot's verb phrase ("web fetch"). `None` for the gaps a slot words itself.
/// Choices are named by label only: a value is an identifier the operator
/// never needs to see.
pub(crate) fn choice_guidance(
    label: &str,
    noun: &str,
    gap: &Gap,
) -> Option<Result<String, String>> {
    match gap {
        Gap::Choose { option, choices } => {
            let mut names: Vec<&str> = choices
                .iter()
                .take(NAMED_CHOICES)
                .map(|choice| choice.label.as_str())
                .collect();
            let more = choices.len().saturating_sub(NAMED_CHOICES);
            let more = (more > 0).then(|| format!("{more} more"));
            if let Some(more) = &more {
                names.push(more);
            }
            Some(Ok(format!(
                "{label} {noun} needs its {option}, and the connected login reaches {}: {}. \
                 Choose one in {LIBRARY_LOCATION}.",
                choices.len(),
                names.join(", ")
            )))
        }
        Gap::Unreachable { option } => Some(Ok(format!(
            "The connected {label} login reaches no {option}. Connect a login that does in \
             {LIBRARY_LOCATION}."
        ))),
        Gap::Failed { option, error } => Some(Err(format!(
            "{label} {noun} needs its {option}, and listing the choices failed: {error}"
        ))),
        Gap::Ready | Gap::Credential(_) | Gap::Blank(_) => None,
    }
}
