//! The workspace defaults that stand in for a model a caller did not name.
//!
//! Cairn's own doors pass the facet an explicit model and never let the
//! catalog choose (the embed door's rule, kept for every verb whose result
//! depends on which model produced it). A door that lets its caller omit the
//! model resolves the omission here first: the caller's model, else the
//! workspace default, else a refusal that names the setting to fill in.
//!
//! A default is checked when it is written ([`ModelSetting`]): a value no
//! installed provider serves for its verb is refused, naming the offers that
//! would serve it. Nothing is checked or rewritten when settings load, so a
//! default whose provider later goes away still loads; the verb's page says
//! it no longer resolves ([`Orchestrator::model_setting_standing`]).

use cairn_config::allotment::{model_ref, AllotmentEntry};
use cairn_models::interface::{fixes, Catalog, ModelScope, Models, OfferQuery};
use cairn_models::types::catalog::VerbKind;

use super::Orchestrator;
use crate::execution::model_selection::SelectionRefusal;
use crate::models::{Settings, UpdateSettings};

/// How many offers a refused default names as the ones that would serve.
const OFFERS_NAMED: usize = 5;

/// A workspace setting naming the model a verb runs on when its caller names
/// none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelSetting {
    /// `decisionModel`: a pool or `provider/model` decision nodes without
    /// their own models select within.
    Decision,
    Image,
    Speech,
    Embedding,
}

impl ModelSetting {
    pub const ALL: [Self; 4] = [Self::Decision, Self::Image, Self::Speech, Self::Embedding];

    /// The `cairn://settings` key.
    pub fn key(self) -> &'static str {
        match self {
            Self::Decision => "decisionModel",
            Self::Image => "imageModel",
            Self::Speech => "speechModel",
            Self::Embedding => "embeddingModel",
        }
    }

    /// The verb the setting's value must serve.
    pub fn verb(self) -> VerbKind {
        match self {
            Self::Decision => VerbKind::Decide,
            Self::Image => VerbKind::Image,
            Self::Speech => VerbKind::Speak,
            Self::Embedding => VerbKind::Embed,
        }
    }

    /// The value `settings` holds, when it holds a non-blank one.
    fn stored(self, settings: &Settings) -> Option<&str> {
        let value = match self {
            Self::Decision => &settings.decision_model,
            Self::Image => &settings.image_model,
            Self::Speech => &settings.speech_model,
            Self::Embedding => &settings.embedding_model,
        };
        non_blank(value.as_deref())
    }

    /// The value `input` sets, when it sets a non-blank one. Clearing and
    /// omitting are never checked.
    fn written(self, input: &UpdateSettings) -> Option<&str> {
        let value = match self {
            Self::Decision => &input.decision_model,
            Self::Image => &input.image_model,
            Self::Speech => &input.speech_model,
            Self::Embedding => &input.embedding_model,
        };
        non_blank(value.as_ref()?.as_deref())
    }
}

fn non_blank(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// A stored default and, when it does not resolve now, why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingStanding {
    pub value: Option<String>,
    pub refusal: Option<String>,
}

/// The offers serving `verb` now, as `provider/model`, those that look like
/// the model `value` spells first (the same native id on another provider, as
/// OpenRouter offers TypeSafe's `jev-1.14` as `typesafe/jev-1.14`): at most
/// [`OFFERS_NAMED`], then how many more. Empty when nothing serves `verb`.
fn serving_offers(catalog: &Catalog, verb: VerbKind, value: &str) -> String {
    let native = value.split_once('/').map_or(value, |(_, rest)| rest);
    let suffix = format!("/{native}");
    let (mut named, others): (Vec<_>, Vec<_>) = catalog
        .offers_serving(verb)
        .into_iter()
        .map(|offer| {
            let alike =
                offer.model == value || offer.model == native || offer.model.ends_with(&suffix);
            (alike, format!("{}/{}", offer.provider, offer.model))
        })
        .partition(|(alike, _)| *alike);
    named.extend(others);
    let named: Vec<String> = named.into_iter().map(|(_, name)| name).collect();
    let mut text = named
        .iter()
        .take(OFFERS_NAMED)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if named.len() > OFFERS_NAMED {
        text.push_str(&format!(" and {} more", named.len() - OFFERS_NAMED));
    }
    text
}

/// A verb whose workspace default fills in an omitted model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultedVerb {
    Image,
    Speak,
}

impl DefaultedVerb {
    /// The `cairn://settings` key that holds this verb's default.
    pub fn setting(self) -> &'static str {
        match self {
            Self::Image => "imageModel",
            Self::Speak => "speechModel",
        }
    }

    fn noun(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Speak => "speech",
        }
    }

    fn section(self) -> &'static str {
        match self {
            Self::Image => "Images",
            Self::Speak => "Speech",
        }
    }
}

/// The explicit model for a `verb` request: `requested` when it names one,
/// else `workspace_default`. A blank value names nothing.
pub fn explicit_model(
    verb: DefaultedVerb,
    requested: Option<&str>,
    workspace_default: Option<&str>,
) -> Result<String, String> {
    let named = |value: Option<&str>| {
        value
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    named(requested)
        .or_else(|| named(workspace_default))
        .ok_or_else(|| {
            format!(
                "no {noun} model was named and the workspace has no default: name one, or set `{setting}` (Settings → Models → {section})",
                noun = verb.noun(),
                setting = verb.setting(),
                section = verb.section(),
            )
        })
}

impl Orchestrator {
    /// [`explicit_model`] against this workspace's saved default for `verb`.
    pub fn explicit_model_for(
        &self,
        verb: DefaultedVerb,
        requested: Option<&str>,
    ) -> Result<String, String> {
        let settings = self.get_settings();
        let workspace_default = match verb {
            DefaultedVerb::Image => settings.image_model,
            DefaultedVerb::Speak => settings.speech_model,
        };
        explicit_model(verb, requested, workspace_default.as_deref())
    }

    /// Why `value` cannot serve `setting`'s verb in this workspace now, or
    /// `None` when it can.
    ///
    /// Asked the way the verb resolves the setting when it runs, so there is
    /// one answer: decide selects within it as an allotment (each candidate
    /// admitted by its provider, `CatalogProvider::admit`) and asks the
    /// catalog for the chosen offer; the other verbs hand it to the catalog's
    /// selector as an exact model. Supply running out (every account out of
    /// usage) is not a reason: it passes.
    pub(crate) fn model_setting_refusal(
        &self,
        setting: ModelSetting,
        value: &str,
    ) -> Option<String> {
        let catalog = match self.catalog(&ModelScope::default()) {
            Ok(catalog) => catalog,
            Err(error) => return Some(format!("the model catalog could not be read: {error}")),
        };
        let verb = setting.verb();
        let offer = |requested: &str| {
            catalog
                .offer_for(OfferQuery {
                    requested: Some(requested),
                    ..OfferQuery::new(verb)
                })
                .err()
                .map(|refusal| refusal.to_string())
        };
        if setting != ModelSetting::Decision {
            return offer(value);
        }
        let entry = match AllotmentEntry::parse(value) {
            Ok(entry) => entry,
            Err(error) => return Some(error),
        };
        match self.select_models(
            None,
            &[entry],
            verb,
            &Default::default(),
            Default::default(),
        ) {
            Ok(record) => offer(&model_ref(&record.chosen)),
            Err(SelectionRefusal::NoSupply(_)) => None,
            Err(refusal) => Some(refusal.to_string()),
        }
    }

    /// `setting`'s stored value and whether it resolves now: what the verb's
    /// page shows, so a default stranded by a removed provider or key is seen
    /// where an operator looks, not only in the logs of what it refused.
    pub(crate) fn model_setting_standing(&self, setting: ModelSetting) -> SettingStanding {
        let value = setting.stored(&self.get_settings()).map(str::to_string);
        let refusal = value
            .as_deref()
            .and_then(|value| self.model_setting_refusal(setting, value));
        SettingStanding { value, refusal }
    }

    /// Refuse an update that sets a model default no installed provider
    /// serves for its verb, naming why and the offers that would serve it.
    ///
    /// Only a value the update changes is asked about: one equal to what is
    /// stored is left alone, so a default stranded since it was saved never
    /// blocks saving anything else. A named model is never swapped for one
    /// that would serve; the operator picks.
    pub(crate) fn refuse_unserved_model_settings(
        &self,
        input: &UpdateSettings,
        before: &Settings,
    ) -> Result<(), String> {
        let mut problems = Vec::new();
        for setting in ModelSetting::ALL {
            let Some(value) = setting.written(input) else {
                continue;
            };
            if setting.stored(before) == Some(value) {
                continue;
            }
            let Some(why) = self.model_setting_refusal(setting, value) else {
                continue;
            };
            let verb = setting.verb();
            let offers = self
                .catalog(&ModelScope::default())
                .map(|catalog| serving_offers(&catalog, verb, value))
                .unwrap_or_default();
            let instead = if offers.is_empty() {
                format!(
                    "Nothing installed serves {} now. {}",
                    verb.as_str(),
                    fixes(verb)
                )
            } else {
                format!("Offers that serve {} now: {offers}.", verb.as_str())
            };
            problems.push(format!(
                "`{}` was not set to '{value}': {}. {instead}",
                setting.key(),
                why.trim_end().trim_end_matches('.')
            ));
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems.join("\n"))
        }
    }
}

#[cfg(test)]
impl Orchestrator {
    /// Store `input` as the settings file takes it, with no write-time check:
    /// a default saved before its provider went away, or one a test reads
    /// back without installing what serves it.
    pub(crate) fn store_settings_unchecked_for_test(&self, input: UpdateSettings) {
        crate::config::settings::update_settings(&self.config_dir, input, &self.provider_names())
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::models::UpdateSettings;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use cairn_models::subscription::testing::TempHomes;
    use std::sync::Arc;

    #[test]
    fn a_named_model_wins_over_the_default() {
        for verb in [DefaultedVerb::Image, DefaultedVerb::Speak] {
            assert_eq!(
                explicit_model(
                    verb,
                    Some(" openai/gpt-image-2 "),
                    Some("recraft/recraft-v4")
                ),
                Ok("openai/gpt-image-2".to_string())
            );
        }
    }

    #[test]
    fn an_omitted_or_blank_model_takes_the_default() {
        for requested in [None, Some(""), Some("   ")] {
            assert_eq!(
                explicit_model(DefaultedVerb::Image, requested, Some("recraft/recraft-v4")),
                Ok("recraft/recraft-v4".to_string())
            );
        }
    }

    #[test]
    fn nothing_named_and_no_default_is_refused_naming_the_setting() {
        for default in [None, Some(""), Some("  ")] {
            let image = explicit_model(DefaultedVerb::Image, None, default).unwrap_err();
            assert!(image.contains("no image model"), "{image}");
            assert!(image.contains("`imageModel`"), "{image}");
            assert!(image.contains("Settings → Models → Images"), "{image}");

            let speech = explicit_model(DefaultedVerb::Speak, Some(" "), default).unwrap_err();
            assert!(speech.contains("no speech model"), "{speech}");
            assert!(speech.contains("`speechModel`"), "{speech}");
        }
    }

    async fn test_orchestrator() -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let db = LocalDb::open(root.join("orch.db")).await.unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir.clone())
            .subscriptions(Arc::new(TempHomes::new(config_dir.join("claude-profiles"))))
            .build()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_saved_workspace_default_is_what_resolves() {
        let orch = test_orchestrator().await;
        let refused = orch
            .explicit_model_for(DefaultedVerb::Image, None)
            .unwrap_err();
        assert!(refused.contains("`imageModel`"), "{refused}");

        orch.store_settings_unchecked_for_test(UpdateSettings {
            image_model: Some(Some("codex/gpt-image-2".into())),
            speech_model: Some(Some("local-tts".into())),
            ..Default::default()
        });
        assert_eq!(
            orch.explicit_model_for(DefaultedVerb::Image, None),
            Ok("codex/gpt-image-2".to_string())
        );
        assert_eq!(
            orch.explicit_model_for(DefaultedVerb::Speak, Some("")),
            Ok("local-tts".to_string())
        );
        assert_eq!(
            orch.explicit_model_for(DefaultedVerb::Image, Some("openai/gpt-image-2")),
            Ok("openai/gpt-image-2".to_string())
        );

        orch.update_settings(UpdateSettings {
            image_model: Some(None),
            ..Default::default()
        })
        .unwrap();
        assert!(orch.explicit_model_for(DefaultedVerb::Image, None).is_err());
        assert_eq!(
            orch.explicit_model_for(DefaultedVerb::Speak, None),
            Ok("local-tts".to_string()),
            "clearing one default leaves the other"
        );
    }

    async fn read(orch: &Orchestrator, uri: &str) -> String {
        let request = crate::mcp::types::McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: None,
            tool: "read".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
            origin: None,
        };
        crate::resources::read_cairn_resource(orch, &request, uri).await
    }

    /// The workspace the incident ran in: TypeSafe's own backend is not
    /// installed, and OpenRouter, keyed, lists TypeSafe's decision models
    /// (`typesafe/jev-1.14` among them).
    async fn decision_workspace() -> Orchestrator {
        use crate::backends::typesafe::TYPESAFE_BACKEND_KEY;
        use crate::credentials::{ApiProvider, ProviderAuth};
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let enabled: Vec<String> = orch
            .enabled_providers()
            .into_iter()
            .filter(|key| key != TYPESAFE_BACKEND_KEY)
            .collect();
        assert!(enabled.iter().any(|key| key == "openrouter"));
        orch.update_settings(UpdateSettings {
            enabled_providers: Some(enabled),
            ..Default::default()
        })
        .unwrap();
        orch.add_account(
            ApiProvider::OpenRouter,
            "OpenRouter".into(),
            ProviderAuth::ApiKey {
                value: "sk-or-test".into(),
            },
            None,
        )
        .unwrap();
        orch.seed_model_catalog_for_test(cairn_models::types::catalog::ProviderModelCatalog {
            backend: "openrouter".into(),
            models: crate::backends::openrouter::decode_decision_models(include_str!(
                "../backends/openrouter/testdata/decisions_models.json"
            ))
            .unwrap(),
            options: Vec::new(),
            refreshed_at: Some(1_790_000_000),
            error: None,
        });
        orch
    }

    fn decision_model(value: Option<&str>) -> UpdateSettings {
        UpdateSettings {
            decision_model: Some(value.map(str::to_string)),
            ..Default::default()
        }
    }

    /// Removing `key` from the installed providers, as a removed key or
    /// provider does.
    fn uninstall(orch: &Orchestrator, key: &str) {
        let enabled = orch
            .enabled_providers()
            .into_iter()
            .filter(|entry| entry != key)
            .collect();
        orch.update_settings(UpdateSettings {
            enabled_providers: Some(enabled),
            ..Default::default()
        })
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_decision_model_nothing_installed_serves_is_refused_naming_what_does() {
        let orch = decision_workspace().await;
        let refused = orch
            .update_settings(decision_model(Some("typesafe/jev-1.14")))
            .unwrap_err();
        assert!(refused.contains("`decisionModel`"), "{refused}");
        assert!(refused.contains("typesafe is not installed"), "{refused}");
        assert!(
            refused.contains("Offers that serve decide now: openrouter/typesafe/jev-1.14"),
            "the same model's OpenRouter spelling comes first: {refused}"
        );
        assert_eq!(
            orch.get_settings().decision_model,
            None,
            "nothing was stored"
        );

        orch.update_settings(decision_model(Some(" openrouter/typesafe/jev-1.14 ")))
            .unwrap();
        assert_eq!(
            orch.get_settings().decision_model.as_deref(),
            Some("openrouter/typesafe/jev-1.14")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_decide_preview_and_page_show_the_configured_decision_model() {
        let orch = decision_workspace().await;
        let unset = read(&orch, "cairn://models/selection?verb=decide").await;
        assert!(
            unset.contains("No `decisionModel` is set, so the catalog's best decision offer"),
            "{unset}"
        );
        assert!(unset.contains("Chosen: `openrouter/"), "{unset}");
        let page = read(&orch, "cairn://models/decide").await;
        assert!(page.contains("Workspace default: none"), "{page}");

        orch.update_settings(decision_model(Some("openrouter/typesafe/jev-1.14")))
            .unwrap();
        let set = read(&orch, "cairn://models/selection?verb=decide").await;
        assert!(
            set.contains(
                "The workspace's `decisionModel`, [openrouter/typesafe/jev-1.14], for decide"
            ),
            "{set}"
        );
        assert!(
            set.contains("Chosen: `openrouter/typesafe/jev-1.14`"),
            "{set}"
        );
        let page = read(&orch, "cairn://models/decide").await;
        assert!(
            page.contains(
                "Workspace default: `openrouter/typesafe/jev-1.14` (`decisionModel`), which"
            ),
            "{page}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stored_decision_model_whose_provider_goes_away_still_loads_and_says_so() {
        let orch = decision_workspace().await;
        orch.update_settings(decision_model(Some("openrouter/typesafe/jev-1.14")))
            .unwrap();
        uninstall(&orch, "openrouter");

        assert_eq!(
            crate::config::settings::load_settings(&orch.config_dir)
                .decision_model
                .as_deref(),
            Some("openrouter/typesafe/jev-1.14"),
            "loading never checks or rewrites it"
        );
        let page = read(&orch, "cairn://models/decide").await;
        assert!(page.contains("does not resolve now"), "{page}");
        assert!(page.contains("openrouter is not installed"), "{page}");
        let preview = read(&orch, "cairn://models/selection?verb=decide").await;
        assert!(preview.contains("Refused: "), "{preview}");
        assert!(preview.contains("openrouter is not installed"), "{preview}");

        // Saving it unchanged, or anything else, is never blocked by it.
        orch.update_settings(decision_model(Some("openrouter/typesafe/jev-1.14")))
            .unwrap();
        orch.update_settings(UpdateSettings {
            max_thinking_tokens: Some(Some(2048)),
            ..Default::default()
        })
        .unwrap();
        orch.update_settings(decision_model(None)).unwrap();
        assert_eq!(orch.get_settings().decision_model, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_image_model_nothing_serves_is_refused_and_a_stored_one_survives_its_provider() {
        let (orch, _, key) = crate::orchestrator::image::tests::workspace().await;
        let refused = orch
            .update_settings(UpdateSettings {
                image_model: Some(Some("no-such-image-model".into())),
                ..Default::default()
            })
            .unwrap_err();
        assert!(refused.contains("`imageModel`"), "{refused}");
        assert!(refused.contains("no-such-image-model"), "{refused}");
        assert!(
            refused.contains("Offers that serve image now: "),
            "{refused}"
        );
        assert!(refused.contains(&format!("{key}/gpt-image-1")), "{refused}");
        assert_eq!(orch.get_settings().image_model, None);

        orch.update_settings(UpdateSettings {
            image_model: Some(Some("gpt-image-1".into())),
            ..Default::default()
        })
        .unwrap();
        uninstall(&orch, &key);
        assert_eq!(
            orch.get_settings().image_model.as_deref(),
            Some("gpt-image-1")
        );
        let page = read(&orch, "cairn://models/image").await;
        assert!(
            page.contains("Default: `gpt-image-1` (`imageModel`) does not resolve now"),
            "{page}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_speech_model_nothing_serves_is_refused_and_a_stored_one_survives_its_provider() {
        use crate::orchestrator::speak::tests::{workspace, Voicebox};
        let (orch, _, key) = workspace(Voicebox::new(&[b"x"], std::time::Duration::ZERO)).await;
        let refused = orch
            .update_settings(UpdateSettings {
                speech_model: Some(Some("no-such-voice-model".into())),
                ..Default::default()
            })
            .unwrap_err();
        assert!(refused.contains("`speechModel`"), "{refused}");
        assert!(refused.contains("no-such-voice-model"), "{refused}");
        assert!(
            refused.contains(&format!("Offers that serve speak now: {key}/tts-1")),
            "{refused}"
        );
        assert_eq!(orch.get_settings().speech_model, None);

        orch.update_settings(UpdateSettings {
            speech_model: Some(Some("tts-1".into())),
            ..Default::default()
        })
        .unwrap();
        uninstall(&orch, &key);
        assert_eq!(orch.get_settings().speech_model.as_deref(), Some("tts-1"));
        let page = read(&orch, "cairn://models/speak").await;
        assert!(
            page.contains("Default: `tts-1` (`speechModel`) does not resolve now"),
            "{page}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_embedding_model_nothing_serves_is_refused_and_a_stored_one_survives_its_provider() {
        let (orch, _, key) =
            crate::orchestrator::embed::tests::workspace_with_hub(Vec::new()).await;
        let refused = orch
            .update_settings(UpdateSettings {
                embedding_model: Some(Some("no-such-embedder".into())),
                ..Default::default()
            })
            .unwrap_err();
        assert!(refused.contains("`embeddingModel`"), "{refused}");
        assert!(refused.contains("no-such-embedder"), "{refused}");
        assert!(
            refused.contains(&format!(
                "Offers that serve embed now: {key}/qwen/qwen3-embedding-4b"
            )),
            "{refused}"
        );
        assert_eq!(orch.get_settings().embedding_model, None);

        orch.update_settings(UpdateSettings {
            embedding_model: Some(Some("qwen/qwen3-embedding-4b".into())),
            ..Default::default()
        })
        .unwrap();
        uninstall(&orch, &key);
        assert_eq!(
            orch.get_settings().embedding_model.as_deref(),
            Some("qwen/qwen3-embedding-4b")
        );
        assert!(orch
            .model_setting_refusal(ModelSetting::Embedding, "qwen/qwen3-embedding-4b")
            .is_some());
    }
}
