use std::collections::BTreeMap;

use cairn_config::allotment::{
    parse_allotment, parse_model_ref, Level, ModelsConfig, Pool, PoolKind, Rung,
};
use cairn_models::interface::{Catalog, CatalogProvider};
use cairn_models::supply::SupplyInputs;
use cairn_models::types::catalog::{
    claude_models, descriptor, DiscoveredModel, DiscoveredReasoningEffort, ProviderModelCatalog,
    ServedVerb, VerbKind, Wire, OPENCODE_BACKEND_KEY,
};
use cairn_models::types::usage::{ProviderUsageScope, ProviderUsageWindow};

use super::*;

const NOW: i64 = 10_000_000;

fn discovered(id: &str, efforts: &[&str], maker: Option<&str>) -> DiscoveredModel {
    let mut model = claude_models().remove(0);
    model.id = id.into();
    model.model = id.into();
    model.maker = maker.map(str::to_string);
    model.supported_reasoning_efforts = efforts
        .iter()
        .map(|e| DiscoveredReasoningEffort {
            reasoning_effort: e.to_string(),
            description: None,
        })
        .collect();
    model
}

fn provider(key: &str, models: Vec<DiscoveredModel>) -> CatalogProvider {
    CatalogProvider {
        descriptor: descriptor(key).cloned().expect("shipped"),
        installed: true,
        models: Some(ProviderModelCatalog {
            backend: key.into(),
            models,
            options: Vec::new(),
            refreshed_at: Some(1),
            error: None,
        }),
        serves: ServedVerb::conversation(Wire::OpenAiChatCompletions),
        unable: Vec::new(),
    }
}

pub(crate) fn catalog() -> Catalog {
    Catalog {
        providers: vec![
            provider("claude", claude_models()),
            provider(
                "codex",
                vec![
                    discovered(
                        "gpt-5.6-sol",
                        &["low", "medium", "high", "xhigh", "ultra"],
                        None,
                    ),
                    discovered("gpt-5.6-terra", &["low", "medium", "high"], None),
                ],
            ),
            provider(
                OPENCODE_BACKEND_KEY,
                vec![discovered("glm-5.3", &["low", "high"], Some("z-ai"))],
            ),
        ],
    }
}

fn rung(model: &str, level: f64, effort: Option<f64>) -> Rung {
    Rung {
        model: parse_model_ref(model).unwrap(),
        level: Some(Level::set(level)),
        effort,
        effort_level: None,
        options: BTreeMap::new(),
    }
}

/// A ladder whose terra entry keeps `word` from the tier grid, no position.
pub(crate) fn models_keeping(word: &str) -> ModelsConfig {
    let mut models = models();
    models.ladder.push(Rung {
        model: parse_model_ref("codex/gpt-5.6-terra").unwrap(),
        level: Some(Level::set(0.4)),
        effort: None,
        effort_level: Some(word.to_string()),
        options: BTreeMap::new(),
    });
    models
}

/// An effort word kept from the tier grid is placed on the model's scale at
/// selection, once the catalog knows it; a word off that scale launches at
/// the provider's default, and the provenance says so.
#[test]
fn a_kept_effort_word_is_placed_on_the_scale_at_selection() {
    let catalog = catalog();
    let select_keeping = |word: &str| {
        let models = models_keeping(word);
        let ctx = SelectionContext {
            models: &models,
            catalog: &catalog,
            supply: SupplyInputs {
                store: None,
                project_id: None,
                assignments: &[],
                provider_windows: &no_windows,
                now: NOW,
            },
        };
        select(
            &ctx,
            &parse_allotment(&["codex/gpt-5.6-terra"]).unwrap(),
            VerbKind::Session,
            &ResolvedConstraints::default(),
            &TopOfAllotment,
        )
        .unwrap()
    };
    let placed = select_keeping("medium");
    assert_eq!(placed.extras.reasoning_effort.as_deref(), Some("medium"));
    assert!(
        placed.provenance.contains(" @ medium: "),
        "{}",
        placed.provenance
    );

    let off = select_keeping("ultra");
    assert_eq!(off.extras.reasoning_effort, None);
    assert!(
        off.provenance.ends_with(
            "; effort 'ultra' is not one of codex/gpt-5.6-terra's levels, so it launches at the provider's default"
        ),
        "{}",
        off.provenance
    );
}

fn band(id: &str, min: f64) -> Pool {
    Pool {
        id: id.into(),
        description: format!("{id} work"),
        kind: PoolKind::Band { min },
    }
}

pub(crate) fn models() -> ModelsConfig {
    ModelsConfig {
        pools: vec![
            band("sm", 0.0),
            band("md", 0.34),
            band("lg", 0.67),
            Pool {
                id: "myth".into(),
                description: "hand-picked".into(),
                kind: PoolKind::Set {
                    members: vec![parse_model_ref("claude/fable").unwrap()],
                },
            },
        ],
        ladder: vec![
            rung("claude/haiku", 0.17, None),
            rung("claude/sonnet", 0.5, Some(0.625)),
            rung("opencode-go/glm-5.3", 0.45, Some(0.75)),
            rung("claude/opus", 0.83, Some(0.9)),
            rung("codex/gpt-5.6-sol", 0.85, Some(0.5)),
        ],
        ..ModelsConfig::default()
    }
}

/// A Codex week with expiring allotment, as OpenCode-style windows: no
/// credential store, so each provider's windows come from the usage book.
pub(crate) fn windows(key: &str) -> Option<(Vec<ProviderUsageWindow>, i64)> {
    let week = |used: f64, hours: i64| ProviderUsageWindow {
        id: "week".into(),
        label: "Weekly".into(),
        scope: ProviderUsageScope::Weekly,
        scope_target: None,
        used_percent: used,
        remaining_percent: 100.0 - used,
        resets_at: Some(NOW + hours * 3600),
        reset_at_text: None,
        window_duration_mins: Some(7 * 24 * 60),
    };
    match key {
        "codex" => Some((vec![week(36.0, 14)], NOW)),
        "claude" => Some((vec![week(78.0, 120)], NOW)),
        _ => None,
    }
}

fn no_windows(_: &str) -> Option<(Vec<ProviderUsageWindow>, i64)> {
    None
}

pub(crate) fn run(
    allotment: &[&str],
    constraints: &ResolvedConstraints,
    provider_windows: cairn_models::supply::ProviderWindows<'_>,
) -> Result<SelectionRecord, SelectionRefusal> {
    run_entries(
        &parse_allotment(allotment).unwrap(),
        constraints,
        provider_windows,
    )
}

/// [`run`] over parsed entries, for callers that hold an allotment already.
pub(crate) fn run_entries(
    allotment: &[AllotmentEntry],
    constraints: &ResolvedConstraints,
    provider_windows: cairn_models::supply::ProviderWindows<'_>,
) -> Result<SelectionRecord, SelectionRefusal> {
    let models = models();
    let catalog = catalog();
    let ctx = SelectionContext {
        models: &models,
        catalog: &catalog,
        supply: SupplyInputs {
            store: None,
            project_id: None,
            assignments: &[],
            provider_windows,
            now: NOW,
        },
    };
    select(
        &ctx,
        allotment,
        VerbKind::Session,
        constraints,
        &TopOfAllotment,
    )
}

fn chosen(record: &SelectionRecord) -> String {
    model_ref(&record.chosen)
}

#[test]
fn the_top_of_the_allotment_runs_and_supply_orders_the_band() {
    let record = run(
        &["sm", "md", "lg"],
        &ResolvedConstraints::default(),
        &windows,
    )
    .unwrap();
    assert_eq!(record.band.as_deref(), Some("lg"));
    // Codex's expiring week beats Claude's headroom, despite opus' level.
    assert_eq!(chosen(&record), "codex/gpt-5.6-sol");
    assert_eq!(record.extras.reasoning_effort.as_deref(), Some("high"));
    assert_eq!(
        record.provenance,
        "lg = top of [sm, md, lg] → codex/gpt-5.6-sol @ high: OpenAI: Week 64% left, resets in 14h, ~61% would expire unused"
    );
    // Without usage, both are unpriced metered and the higher level wins.
    let flat = run(
        &["sm", "md", "lg"],
        &ResolvedConstraints::default(),
        &no_windows,
    )
    .unwrap();
    assert_eq!(chosen(&flat), "codex/gpt-5.6-sol");
    let small = run(&["sm", "md"], &ResolvedConstraints::default(), &windows).unwrap();
    assert_eq!(small.band.as_deref(), Some("md"));
    assert_eq!(chosen(&small), "claude/sonnet");
    assert_eq!(small.extras.reasoning_effort.as_deref(), Some("high"));
}

#[test]
fn a_model_off_the_ladder_sits_at_the_top() {
    let record = run(
        &["md", "myth"],
        &ResolvedConstraints::default(),
        &no_windows,
    )
    .unwrap();
    assert_eq!(record.band.as_deref(), Some("md"));
    let top: Vec<&str> = record
        .candidates
        .iter()
        .filter(|c| c.class.is_some())
        .map(|c| c.model.as_str())
        .collect();
    assert!(top.contains(&"claude/fable"), "{top:?}");
    // Off the ladder ranks ahead of laddered models at equal cost.
    assert_eq!(chosen(&record), "claude/fable");
}

#[test]
fn a_named_model_is_never_substituted() {
    let refused = run(
        &["openrouter/anthropic/claude-sonnet-4.5"],
        &ResolvedConstraints::default(),
        &no_windows,
    )
    .unwrap_err();
    assert!(
        refused.message.contains("openrouter is not installed"),
        "{refused}"
    );
    let alone = run(&["claude/opus"], &ResolvedConstraints::default(), &windows).unwrap();
    assert_eq!(chosen(&alone), "claude/opus");
    assert_eq!(alone.extras.reasoning_effort.as_deref(), Some("xhigh"));
}

fn builder(model: &str, band: Option<&str>, maker: Option<&str>) -> SelectionRecord {
    SelectionRecord {
        allotment: parse_allotment(&["sm", "md", "lg"]).unwrap(),
        requirement: TopOfAllotment
            .requirement(&models().expand(&parse_allotment(&["lg"]).unwrap()).unwrap()),
        band: band.map(str::to_string),
        candidates: Vec::new(),
        chosen: parse_model_ref(model).unwrap(),
        maker: maker.map(str::to_string),
        account: None,
        extras: RuntimeExtras::default(),
        relaxations: Vec::new(),
        provenance: String::new(),
        subject: SelectionSubject::default(),
    }
}

fn reviewing(record: SelectionRecord) -> ResolvedConstraints {
    ResolvedConstraints {
        same_band_as: Some(("builder-1".into(), record.clone())),
        different_maker_from: Some(("builder-1".into(), record)),
        fork_provider: None,
    }
}

#[test]
fn a_reviewer_takes_its_builders_band_from_another_maker() {
    let constraints = reviewing(builder("claude/opus", Some("lg"), Some("anthropic")));
    let record = run(&["sm", "md", "lg"], &constraints, &no_windows).unwrap();
    assert_eq!(chosen(&record), "codex/gpt-5.6-sol");
    assert!(record.relaxations.is_empty());
    assert!(
        record
            .provenance
            .ends_with("maker openai differs from builder-1's anthropic"),
        "{}",
        record.provenance
    );
}

#[test]
fn the_band_relaxes_up_then_down_but_never_the_maker() {
    // md's other-maker model is glm; drop it from the allotment by naming only
    // Anthropic's md model and lg: the reviewer must go up to lg.
    let up = reviewing(builder("claude/sonnet", Some("md"), Some("anthropic")));
    let record = run(&["claude/sonnet", "claude/haiku", "lg"], &up, &no_windows).unwrap();
    assert_eq!(chosen(&record), "codex/gpt-5.6-sol");
    assert!(
        record
            .relaxations
            .iter()
            .any(|r| r.starts_with("band relaxed md→lg")),
        "{:?}",
        record.relaxations
    );
    // A builder at lg whose band holds only Anthropic here: down to md's glm.
    let down = reviewing(builder("claude/opus", Some("lg"), Some("anthropic")));
    let record = run(&["md", "claude/opus"], &down, &no_windows).unwrap();
    assert_eq!(chosen(&record), "opencode-go/glm-5.3");
    assert!(
        record
            .relaxations
            .iter()
            .any(|r| r.starts_with("band relaxed lg→md")),
        "{:?}",
        record.relaxations
    );
}

/// With no other maker in any band, the maker rule gives way: the node
/// selects as if it were absent, keeping the band rules, and says so.
#[test]
fn no_other_maker_or_an_unknown_maker_selects_as_if_unconstrained() {
    let anthropic = reviewing(builder("claude/opus", Some("lg"), Some("anthropic")));
    let record = run(&["claude/opus", "claude/sonnet"], &anthropic, &no_windows).unwrap();
    assert_eq!(chosen(&record), "claude/opus");
    assert_eq!(record.band.as_deref(), Some("lg"));
    assert!(
        record.provenance.ends_with(
            "; no model from a maker other than anthropic in [claude/opus, claude/sonnet]; same maker as builder-1"
        ),
        "{}",
        record.provenance
    );
    assert!(
        record
            .candidates
            .iter()
            .all(|c| !c.note.starts_with("same maker")),
        "nothing is set aside for a rule that gave way: {:?}",
        record.candidates
    );

    // The band rule still holds: the builder's md, not the allotment's top.
    let md = reviewing(builder("claude/sonnet", Some("md"), Some("anthropic")));
    let record = run(&["claude/sonnet", "claude/opus"], &md, &no_windows).unwrap();
    assert_eq!(chosen(&record), "claude/sonnet");
    assert!(
        record.provenance.ends_with("; same maker as builder-1"),
        "{}",
        record.provenance
    );

    let unknown = reviewing(builder("custom:box/qwen", None, None));
    let record = run(&["lg"], &unknown, &no_windows).unwrap();
    assert!(
        record
            .provenance
            .contains("the maker of builder-1's custom:box/qwen is unknown"),
        "{}",
        record.provenance
    );
}

#[test]
fn the_record_round_trips_through_json() {
    let record = run(
        &["sm", "md", "lg"],
        &ResolvedConstraints::default(),
        &windows,
    )
    .unwrap();
    let json = serde_json::to_string(&record).unwrap();
    let back: SelectionRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(back, record);
}
