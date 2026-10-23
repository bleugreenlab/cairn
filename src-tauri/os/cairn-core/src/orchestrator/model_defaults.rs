//! The workspace defaults that stand in for a model a caller did not name.
//!
//! Cairn's own doors pass the facet an explicit model and never let the
//! catalog choose (the embed door's rule, kept for every verb whose result
//! depends on which model produced it). A door that lets its caller omit the
//! model resolves the omission here first: the caller's model, else the
//! workspace default, else a refusal that names the setting to fill in.

use super::Orchestrator;

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

        orch.update_settings(UpdateSettings {
            image_model: Some(Some("codex/gpt-image-2".into())),
            speech_model: Some(Some("local-tts".into())),
            ..Default::default()
        })
        .unwrap();
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
}
