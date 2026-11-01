//! Which offer serves the model an outside client named.
//!
//! Models go by their real names (tenet 1336). A local model's real name is
//! the model its engine loads (`deepseek-ai/DeepSeek-V4-Flash-Vision-Exp`); a
//! remote one's is the provider's id (`anthropic/claude-sonnet-4.5`), and
//! `provider/native-id` pins a provider. The upstream slug a local recipe
//! declares (the OpenRouter id the catalog merges on) also resolves, local
//! first. A tier is never a model name. Any other name for a local model (its
//! engine's served name, one bgllm answered to) is refused with a 404 that
//! names the real model, so a client still sending it fails visibly rather
//! than silently.
//!
//! Pure: the catalog, what Cairn's deployments serve, and what the client may
//! use are all handed in.

use cairn_inference::served::ServedModel;
use cairn_models::interface::relay::RelayDoor;
use cairn_models::interface::{Catalog, CatalogOffer, SelectedOffer};

use super::{relays_to, LOCAL_PROVIDER};

/// The names bgllm answered to and the upstream slug each stood for
/// (`deploy/bgllm.toml`). Kept only so a client still sending one is told the
/// real name; this table goes when bgllm's clients have moved.
const RETIRED_NAMES: &[(&str, &str)] = &[
    ("big", "deepseek/deepseek-v4-flash-vision-exp"),
    ("vision", "deepseek/deepseek-v4-flash-vision-exp"),
    ("dsv4-flash", "deepseek/deepseek-v4-flash-vision-exp"),
    ("fast", "qwen/qwen3.6-35b-a3b"),
    ("qwen36", "qwen/qwen3.6-35b-a3b"),
    ("embed", "qwen/qwen3-embedding-4b"),
];

/// The offer a request goes to, and the names it was reached by.
#[derive(Debug, Clone, PartialEq)]
pub struct Resolved {
    /// The name the client sent.
    pub asked: String,
    /// The model's name as the catalog knows it.
    pub model: String,
    pub offer: SelectedOffer,
}

/// Why a name reached no offer the client may use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServedRefusal {
    /// Nothing serves the name.
    NotFound { model: String },
    /// The name is an alias; `real` is the model's real name.
    Renamed { asked: String, real: String },
    /// Something serves the name, but not for this door's verb.
    Unserved { model: String, detail: String },
    /// Only a remote provider serves it, and the client holds no grant for
    /// that provider.
    RemoteNotGranted { model: String, provider: String },
}

/// `asked` without an `inference/` qualifier, which names Cairn's own
/// deployments.
fn unqualified(asked: &str) -> &str {
    asked
        .strip_prefix(LOCAL_PROVIDER)
        .and_then(|rest| rest.strip_prefix('/'))
        .unwrap_or(asked)
}

/// The deployment whose real name `asked` is, compared case-insensitively.
fn by_real_name<'a>(asked: &str, served: &'a [ServedModel]) -> Option<&'a ServedModel> {
    let asked = unqualified(asked);
    served
        .iter()
        .find(|model| model.id.eq_ignore_ascii_case(asked))
}

/// The real name another name stands for, when `asked` is one: an engine's
/// served name (a name for the engine, not the model), or a name bgllm
/// answered to.
fn renamed(asked: &str, served: &[ServedModel]) -> Option<String> {
    if by_real_name(asked, served).is_some() {
        return None;
    }
    let bare = unqualified(asked);
    if let Some(model) = served
        .iter()
        .find(|model| model.served_name.eq_ignore_ascii_case(bare))
    {
        return Some(model.id.clone());
    }
    let (_, slug) = RETIRED_NAMES
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(bare))?;
    Some(
        served
            .iter()
            .find(|model| model.upstream_id.as_deref() == Some(*slug))
            .map(|model| model.id.clone())
            .unwrap_or_else(|| slug.to_string()),
    )
}

/// Resolve `name` for `door`.
///
/// A deployment's real name reaches that deployment; any other name goes to
/// the catalog's exact names ([`Catalog::named_offers`]). Of the offers that
/// serve the door's verb, Cairn's own inference deployment wins whenever there
/// is one: it is never passed over for a remote provider, even unloaded, since
/// asking is what loads it. Otherwise the first remote offer `may_use` allows
/// is taken.
pub fn resolve(
    name: &str,
    door: RelayDoor,
    served: &[ServedModel],
    catalog: &Catalog,
    may_use: impl Fn(&str) -> bool,
) -> Result<Resolved, ServedRefusal> {
    let asked = name.trim().to_string();
    if let Some(real) = renamed(&asked, served) {
        return Err(ServedRefusal::Renamed { asked, real });
    }
    let model = by_real_name(&asked, served)
        .map(|model| model.id.clone())
        .unwrap_or_else(|| asked.clone());
    let offers: Vec<CatalogOffer> = catalog
        .named_offers(&model)
        .into_iter()
        .filter(|offer| relays_to(&offer.provider))
        .collect();
    if offers.is_empty() {
        return Err(ServedRefusal::NotFound { model: asked });
    }
    let verb = door.verb();
    let mut refusals = Vec::new();
    let mut admitted = |offer: &CatalogOffer| -> Option<SelectedOffer> {
        let provider = catalog.provider(&offer.provider)?;
        match provider.admit(&offer.model, verb) {
            Ok(served) => Some(SelectedOffer {
                provider: offer.provider.clone(),
                model: offer.model.clone(),
                served,
            }),
            Err(refusal) => {
                refusals.push(
                    refusal
                        .detail
                        .unwrap_or_else(|| format!("'{}' on {}", offer.model, offer.provider)),
                );
                None
            }
        }
    };
    let resolved = |offer: SelectedOffer| Resolved {
        asked: asked.clone(),
        model: model.clone(),
        offer,
    };

    let (local, remote): (Vec<&CatalogOffer>, Vec<&CatalogOffer>) = offers
        .iter()
        .partition(|offer| offer.provider == LOCAL_PROVIDER);
    if !local.is_empty() {
        return local
            .into_iter()
            .find_map(&mut admitted)
            .map(resolved)
            .ok_or_else(|| ServedRefusal::Unserved {
                model: asked.clone(),
                detail: refusals.join("; "),
            });
    }
    let serving: Vec<SelectedOffer> = remote.into_iter().filter_map(&mut admitted).collect();
    let Some(first) = serving.first() else {
        return Err(ServedRefusal::Unserved {
            model: asked,
            detail: refusals.join("; "),
        });
    };
    let first_provider = first.provider.clone();
    serving
        .into_iter()
        .find(|offer| may_use(&offer.provider))
        .map(resolved)
        .ok_or(ServedRefusal::RemoteNotGranted {
            model: asked,
            provider: first_provider,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::served_api::listing::tests::lab_served;
    use cairn_models::interface::CatalogProvider;
    use cairn_models::types::catalog::{
        claude_models, descriptor, DiscoveredModel, Modalities, Modality, ProviderModelCatalog,
        ServedVerb, Verb, Wire,
    };

    fn discovered(id: &str, slug: Option<&str>, embed: bool) -> DiscoveredModel {
        let mut model = claude_models().remove(0);
        model.id = id.into();
        model.model = id.into();
        model.display_name = id.into();
        model.canonical_slug = slug.map(str::to_string);
        model.serves = embed.then(|| {
            vec![ServedVerb {
                verb: Verb::Embed(
                    cairn_models::types::embedding::EmbeddingCapabilities::cairn_gateway(),
                ),
                wire: Wire::OpenAiEmbeddings,
            }]
        });
        model.modalities = Some(Modalities {
            input: vec![Modality::Text],
            output: vec![if embed {
                Modality::Embeddings
            } else {
                Modality::Text
            }],
        });
        model
    }

    fn provider(key: &str, models: Vec<DiscoveredModel>) -> CatalogProvider {
        CatalogProvider {
            descriptor: descriptor(key).expect("shipped").clone(),
            installed: true,
            models: Some(ProviderModelCatalog {
                backend: key.into(),
                models,
                options: Vec::new(),
                refreshed_at: Some(1),
                error: None,
            }),
            tiers: Vec::new(),
            serves: ServedVerb::conversation(Wire::OpenAiChatCompletions),
            unable: Vec::new(),
        }
    }

    const DSV4_SLUG: &str = "deepseek/deepseek-v4-flash-vision-exp";
    const DSV4: &str = "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp";
    const QWEN_EMBED: &str = "qwen/qwen3-embedding-4b";

    /// The lab as the catalog sees it: DeepSeek on the Sparks and the halo
    /// embedder under their real names, and OpenRouter serving the same
    /// DeepSeek under a dated canonical slug plus a model only it has, with a
    /// tier named `big` bound on it.
    fn lab() -> Catalog {
        let mut openrouter = provider(
            "openrouter",
            vec![
                discovered(
                    DSV4_SLUG,
                    Some("deepseek/deepseek-v4-flash-vision-exp-20261001"),
                    false,
                ),
                discovered("anthropic/claude-sonnet-4.5", None, false),
            ],
        );
        openrouter.tiers = vec![cairn_models::interface::TierBinding {
            tier: "big".into(),
            model: "anthropic/claude-sonnet-4.5".into(),
            options: Default::default(),
            is_tier_default: true,
        }];
        Catalog {
            providers: vec![
                provider(
                    LOCAL_PROVIDER,
                    vec![
                        discovered(DSV4, Some(DSV4_SLUG), false),
                        discovered(QWEN_EMBED, Some(QWEN_EMBED), true),
                    ],
                ),
                openrouter,
            ],
        }
    }

    fn chat(name: &str, remote: bool) -> Result<Resolved, ServedRefusal> {
        resolve(
            name,
            RelayDoor::ChatCompletions,
            &lab_served(),
            &lab(),
            |_| remote,
        )
    }

    #[test]
    fn a_local_model_answers_to_its_real_name_and_beats_remote() {
        for remote in [false, true] {
            let resolved = chat(DSV4, remote).unwrap();
            assert_eq!(resolved.asked, DSV4);
            assert_eq!(resolved.offer.provider, LOCAL_PROVIDER);
            assert_eq!(resolved.offer.model, DSV4);
            // The upstream slug groups the same offers, local first.
            let by_slug = chat(DSV4_SLUG, remote).unwrap();
            assert_eq!(by_slug.offer.provider, LOCAL_PROVIDER);
        }
    }

    #[test]
    fn a_served_name_is_refused_and_a_real_name_matches_in_any_case() {
        for name in [
            "deepseek-v4-flash-vision-exp",
            "inference/deepseek-v4-flash-vision-exp",
            "DeepSeek-V4-Flash-Vision-Exp",
        ] {
            assert_eq!(
                chat(name, true),
                Err(ServedRefusal::Renamed {
                    asked: name.into(),
                    real: DSV4.into()
                }),
                "{name}"
            );
        }
        let lower = chat(&DSV4.to_ascii_lowercase(), false).unwrap();
        assert_eq!(lower.offer.model, DSV4);
        let qualified = chat(&format!("inference/{DSV4}"), false).unwrap();
        assert_eq!(qualified.offer.provider, LOCAL_PROVIDER);
    }

    #[test]
    fn an_alias_is_refused_naming_the_real_model() {
        for alias in ["dsv4-flash", "big", "vision"] {
            assert_eq!(
                chat(alias, true),
                Err(ServedRefusal::Renamed {
                    asked: alias.into(),
                    real: DSV4.into()
                }),
                "{alias}"
            );
        }
        assert_eq!(
            resolve(
                "embed",
                RelayDoor::Embeddings,
                &lab_served(),
                &lab(),
                |_| true
            ),
            Err(ServedRefusal::Renamed {
                asked: "embed".into(),
                real: "qwen/qwen3-embedding-4b".into()
            })
        );
        // A retired name whose model Cairn does not run names its slug.
        assert_eq!(
            chat("qwen36", true),
            Err(ServedRefusal::Renamed {
                asked: "qwen36".into(),
                real: "qwen/qwen3.6-35b-a3b".into()
            })
        );
    }

    #[test]
    fn a_tier_name_is_not_a_model_name() {
        let served: Vec<ServedModel> = Vec::new();
        let catalog = lab();
        assert_eq!(
            resolve(
                "openrouter/big",
                RelayDoor::ChatCompletions,
                &served,
                &catalog,
                |_| true
            ),
            Err(ServedRefusal::NotFound {
                model: "openrouter/big".into()
            })
        );
    }

    #[test]
    fn a_remote_only_model_needs_the_grant() {
        assert_eq!(
            chat("anthropic/claude-sonnet-4.5", false),
            Err(ServedRefusal::RemoteNotGranted {
                model: "anthropic/claude-sonnet-4.5".into(),
                provider: "openrouter".into(),
            })
        );
        assert_eq!(
            chat("anthropic/claude-sonnet-4.5", true)
                .unwrap()
                .offer
                .provider,
            "openrouter"
        );
    }

    #[test]
    fn the_provider_qualifier_pins_a_remote_offer() {
        let pinned = format!("openrouter/{DSV4_SLUG}");
        assert_eq!(chat(&pinned, true).unwrap().offer.provider, "openrouter");
        assert!(matches!(
            chat(&pinned, false),
            Err(ServedRefusal::RemoteNotGranted { .. })
        ));
    }

    #[test]
    fn admission_refuses_a_door_whose_verb_the_model_does_not_serve() {
        let embedder = resolve(
            "qwen/qwen3-embedding-4b",
            RelayDoor::Embeddings,
            &lab_served(),
            &lab(),
            |_| false,
        )
        .unwrap();
        assert_eq!(embedder.offer.model, QWEN_EMBED);
        assert!(matches!(
            chat("qwen/qwen3-embedding-4b", true),
            Err(ServedRefusal::Unserved { .. })
        ));
        assert!(matches!(
            resolve(DSV4, RelayDoor::Embeddings, &lab_served(), &lab(), |_| true),
            Err(ServedRefusal::Unserved { .. })
        ));
    }

    #[test]
    fn an_unknown_name_is_not_found() {
        assert_eq!(
            chat("nothing/here", true),
            Err(ServedRefusal::NotFound {
                model: "nothing/here".into()
            })
        );
    }
}
