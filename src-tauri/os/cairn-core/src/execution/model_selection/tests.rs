use std::collections::BTreeMap;

use cairn_config::allotment::{
    parse_allotment, parse_model_ref, ExpandedAllotment, Level, LevelMap, ModelsConfig, Pool,
    PoolKind, Rung,
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
        alias_resolutions: Vec::new(),
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

fn rung(model: &str, level: f64) -> Rung {
    Rung {
        model: parse_model_ref(model).unwrap(),
        level: Some(Level::set(level)),
        options: BTreeMap::new(),
    }
}

/// A requirement that sets an effort on top of [`TopOfAllotment`]'s choice.
struct TopAtEffort(f64);

impl RequirementResolver for TopAtEffort {
    fn requirement(&self, allotment: &ExpandedAllotment) -> Requirement {
        Requirement {
            effort: Some(self.0),
            ..TopOfAllotment.requirement(allotment)
        }
    }
}

/// Effort is the requirement's alone: set, it is placed on the chosen
/// model's scale; unset, the launch runs at the provider's default and the
/// provenance says so.
#[test]
fn effort_comes_from_the_requirement_else_the_providers_default() {
    let catalog = catalog();
    let models = models();
    let select_with = |resolver: &dyn RequirementResolver| {
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
            &parse_allotment(&["codex/gpt-5.6-sol"]).unwrap(),
            VerbKind::Session,
            &ResolvedConstraints::default(),
            resolver,
        )
        .unwrap()
    };
    // 0.5 on sol's five-level scale lands on the third level.
    let placed = select_with(&TopAtEffort(0.5));
    assert_eq!(placed.extras.reasoning_effort.as_deref(), Some("high"));
    assert!(
        placed.provenance.contains(" @ high: "),
        "{}",
        placed.provenance
    );
    assert!(
        !placed.provenance.contains(PROVIDER_DEFAULT_EFFORT),
        "{}",
        placed.provenance
    );

    let default = select_with(&TopOfAllotment);
    assert_eq!(default.extras.reasoning_effort, None);
    assert!(
        default
            .provenance
            .ends_with(&format!("; {PROVIDER_DEFAULT_EFFORT}")),
        "{}",
        default.provenance
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
            rung("claude/haiku", 0.17),
            rung("claude/sonnet", 0.5),
            rung("opencode-go/glm-5.3", 0.45),
            rung("claude/opus", 0.83),
            rung("codex/gpt-5.6-sol", 0.85),
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
    assert_eq!(record.extras.reasoning_effort, None);
    assert_eq!(
        record.provenance,
        "lg = top of [sm, md, lg] → codex/gpt-5.6-sol: OpenAI: Week 64% left, resets in 14h, ~61% would expire unused; effort: provider default"
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

/// A named model with no ladder entry is placed where the shipped benchmarks
/// put it, instead of counting as the top; a stored level is never replaced.
#[test]
fn a_named_model_off_the_ladder_takes_its_benchmark_level() {
    let shipped = cairn_models::types::benchmarks::shipped();
    let placed = shipped
        .model("claude-opus-4.6")
        .expect("the shipped fit places claude-opus-4.6")
        .clone();
    let mut catalog = catalog();
    catalog.providers.push(provider(
        "openrouter",
        vec![discovered("anthropic/claude-opus-4.6", &[], None)],
    ));
    let mut models = models();
    // With no calibration rung the benchmarks place nothing: the named model
    // counts as the top, as a model off the ladder always has.
    {
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
        let record = select(
            &ctx,
            &parse_allotment(&["sm", "openrouter/anthropic/claude-opus-4.6"]).unwrap(),
            VerbKind::Session,
            &ResolvedConstraints::default(),
            &TopOfAllotment,
        )
        .unwrap();
        let opus = record
            .candidates
            .iter()
            .find(|c| c.model == "openrouter/anthropic/claude-opus-4.6")
            .unwrap();
        assert_eq!((opus.level, opus.level_source), (None, None));
    }
    // Two calibration rungs on the user's ladder set the scale.
    models.ladder.push(rung("openrouter/openai/gpt-5.5", 0.9));
    models
        .ladder
        .push(rung("openrouter/anthropic/claude-opus-4.5", 0.6));
    let point = |id: &str, level: f64| {
        let placed = shipped.model(id).expect("shipped");
        cairn_config::allotment::CalibrationPoint {
            at: placed.level,
            low: placed.low,
            high: placed.high,
            level,
        }
    };
    let map = LevelMap::fit([point("gpt-5.5", 0.9), point("claude-opus-4.5", 0.6)])
        .expect("gpt-5.5 and claude-opus-4.5 are distinguishable");
    let position = map.position(placed.level);
    let ctx = |models: &ModelsConfig| -> SelectionRecord {
        let ctx = SelectionContext {
            models,
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
            &parse_allotment(&["sm", "openrouter/anthropic/claude-opus-4.6"]).unwrap(),
            VerbKind::Session,
            &ResolvedConstraints::default(),
            &TopOfAllotment,
        )
        .unwrap()
    };
    let record = ctx(&models);
    let expected = models.band_of(position).unwrap().id;
    assert_eq!(record.band.as_deref(), Some(expected.as_str()));
    assert_eq!(chosen(&record), "openrouter/anthropic/claude-opus-4.6");
    let opus = &record.candidates[0];
    assert_eq!(opus.level, Some(position));
    assert_eq!(opus.level_source, Some(LevelSource::Benchmarks));
    let says = format!(
        "level from benchmarks {:.3} → {position:.2} via your ladder, anchor v1",
        placed.level
    );
    assert!(record.provenance.contains(&says), "{}", record.provenance);

    // A level the user set stands, whatever the benchmarks say.
    models
        .ladder
        .push(rung("openrouter/anthropic/claude-opus-4.6", 0.2));
    let held = ctx(&models);
    let opus = held
        .candidates
        .iter()
        .find(|c| c.model == "openrouter/anthropic/claude-opus-4.6")
        .unwrap();
    assert_eq!(opus.level, Some(0.2));
    assert_eq!(opus.level_source, Some(LevelSource::Set));
    assert!(
        !held.provenance.contains("benchmarks"),
        "{}",
        held.provenance
    );
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
        matches!(&refused, SelectionRefusal::Misconfigured { message } if message.contains("openrouter is not installed")),
        "{refused}"
    );
    let alone = run(&["claude/opus"], &ResolvedConstraints::default(), &windows).unwrap();
    assert_eq!(chosen(&alone), "claude/opus");
}

/// Every provider spent, each resetting at its own hour: Codex soonest.
fn exhausted(key: &str) -> Option<(Vec<ProviderUsageWindow>, i64)> {
    let hours = match key {
        "codex" => 14,
        "claude" => 120,
        _ => 50,
    };
    Some((
        vec![ProviderUsageWindow {
            id: "week".into(),
            label: "Weekly".into(),
            scope: ProviderUsageScope::Weekly,
            scope_target: None,
            used_percent: 100.0,
            remaining_percent: 0.0,
            resets_at: Some(NOW + hours * 3600),
            reset_at_text: None,
            window_duration_mins: Some(7 * 24 * 60),
        }],
        NOW,
    ))
}

#[test]
fn a_named_model_out_of_usage_waits_for_its_reset() {
    let refused = run(
        &["claude/opus"],
        &ResolvedConstraints::default(),
        &exhausted,
    )
    .unwrap_err();
    let SelectionRefusal::NoSupply(refusal) = refused else {
        panic!("out of usage is a supply refusal, not configuration: {refused}");
    };
    assert_eq!(refusal.subject, "claude/opus");
    assert_eq!(refusal.retry_at, Some(NOW + 120 * 3600));
    assert!(refusal.reason.contains("out of usage"), "{refusal}");
}

#[test]
fn an_allotment_with_every_candidate_out_waits_for_the_soonest_reset() {
    let refused = run(&["lg"], &ResolvedConstraints::default(), &exhausted).unwrap_err();
    let SelectionRefusal::NoSupply(refusal) = refused else {
        panic!("a band with every candidate out is a supply refusal: {refused}");
    };
    assert_eq!(refusal.subject, "[lg]");
    assert_eq!(refusal.retry_at, Some(NOW + 14 * 3600));
    assert!(refusal.reason.contains("can run now"), "{refusal}");
}

fn builder(model: &str, band: Option<&str>, maker: Option<&str>) -> SelectionRecord {
    SelectionRecord {
        allotment: parse_allotment(&["sm", "md", "lg"]).unwrap(),
        requirement: TopOfAllotment.requirement(
            &models()
                .expand(
                    &parse_allotment(&["lg"]).unwrap(),
                    &cairn_config::allotment::unplaced,
                )
                .unwrap(),
        ),
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
            .contains("; maker openai differs from builder-1's anthropic;"),
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
        record.provenance.contains(
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
        record.provenance.contains("; same maker as builder-1"),
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
