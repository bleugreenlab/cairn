//! The forecast's contract, golden ports of the bench, and the refit on a
//! seeded database. No test reaches the network: answerers are fakes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::{json, Value};

use super::applicability::maker;
use super::backfill::{created_by_result, filing_job, FirstJob};
use super::calibration::{
    conformal_shift, decide_table, monotone, quantile, Calibration, CalibrationBin,
};
use super::contract::*;
use super::fit::{self, FitLabel, PopulationIssue, RefitVerdict, FLOOR};
use super::question::{self, expected_score};
use super::store::{self, LabelRecord};
use super::*;
use crate::models::decision::OptionProbability;
use crate::storage::LocalDb;

// ---------------------------------------------------------------- fakes

#[derive(Clone, Copy)]
enum Behaviour {
    Score(f64),
    /// A score whose reply reported no cost.
    ScoreUncosted(f64),
    Fail,
    Slow,
    Refuse,
    /// A refusal the reply still charged for.
    RefusePaid(f64),
    Unanswerable,
}

struct FakeAnswerer {
    behaviour: Behaviour,
    calls: AtomicUsize,
    seen: Mutex<Vec<AnswerInput>>,
}

impl FakeAnswerer {
    fn new(behaviour: Behaviour) -> Arc<Self> {
        Arc::new(Self {
            behaviour,
            calls: AtomicUsize::new(0),
            seen: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

fn provenance() -> AnswerProvenance {
    AnswerProvenance {
        model: DECIDE_MODEL.to_string(),
        provider: DECIDE_PROVIDER.to_string(),
        measured_by: "native".to_string(),
        zdr_satisfied_by: "openrouter-zdr-routing".to_string(),
        cost_usd: Some(0.000064),
        latency_ms: 12,
    }
}

#[async_trait]
impl ScoreAnswerer for FakeAnswerer {
    fn version(&self) -> &str {
        ANSWERER_VERSION
    }

    async fn answer(
        &self,
        input: &AnswerInput,
        _deadline: Instant,
    ) -> Result<ScoreAnswer, AnswerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.seen.lock().unwrap().push(input.clone());
        match self.behaviour {
            Behaviour::Score(score) => Ok(ScoreAnswer {
                effort: score,
                scope: score,
                score,
                distributions: json!({}),
                provenance: provenance(),
            }),
            Behaviour::ScoreUncosted(score) => Ok(ScoreAnswer {
                effort: score,
                scope: score,
                score,
                distributions: json!({}),
                provenance: AnswerProvenance {
                    cost_usd: None,
                    ..provenance()
                },
            }),
            Behaviour::RefusePaid(cost) => Err(AnswerError::Refused {
                reason: "scope was refused".into(),
                cost_usd: Some(cost),
            }),
            Behaviour::Fail => Err(AnswerError::Failed("provider returned 502".into())),
            Behaviour::Slow => {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Err(AnswerError::Failed("unreachable".into()))
            }
            Behaviour::Refuse => Err(AnswerError::Refused {
                reason: "effort was refused".into(),
                cost_usd: None,
            }),
            Behaviour::Unanswerable => Err(AnswerError::Unanswerable(
                "typesafe/jev-1.14 on openrouter cannot keep zero data retention".into(),
            )),
        }
    }
}

#[derive(Default)]
struct MemoryStore {
    calibration: Option<Calibration>,
    labels: Mutex<Vec<LabelRecord>>,
    calibration_delay: Duration,
    label_delay: Duration,
}

#[async_trait]
impl ForecastStore for MemoryStore {
    async fn active_calibration(
        &self,
        answerer_version: &str,
        question_version: &str,
    ) -> Result<Option<Calibration>, String> {
        tokio::time::sleep(self.calibration_delay).await;
        Ok(self.calibration.clone().filter(|c| {
            c.answerer_version == answerer_version && c.question_version == question_version
        }))
    }

    async fn record_label(&self, label: LabelRecord) -> Result<(), String> {
        tokio::time::sleep(self.label_delay).await;
        self.labels.lock().unwrap().push(label);
        Ok(())
    }
}

fn fixture_calibration() -> Calibration {
    let bin = |i: f64, soft_basis| CalibrationBin {
        quantiles: [10.0 + i, 11.0 + i, 12.0 + i],
        soft_basis,
        heldout_coverage: Some(0.9),
        heldout_n: 20,
    };
    Calibration {
        version: "tfc-fixture".to_string(),
        answerer_version: ANSWERER_VERSION.to_string(),
        question_version: question::question_version().to_string(),
        as_of: 1_792_393_200,
        table_window: [FLOOR, 1_792_393_200],
        shift_window: [1_790_578_800, 1_792_393_200],
        fit_n: 1200,
        shift_n: 400,
        edges: vec![0.2, 0.4, 0.6, 0.8],
        bins: vec![
            bin(0.0, None),
            bin(1.0, Some(SoftBasis::BinHeldoutCoverageBelow085)),
            bin(2.0, None),
            bin(3.0, None),
            bin(4.0, Some(SoftBasis::TopScoreBin)),
        ],
        shifts: [0.1, 0.2],
        group: "anthropic".to_string(),
        large_actuals_coverage: Some(0.9),
    }
}

/// Labels are written off the caller's path; wait for `n` to land.
async fn labels_landed(store: &MemoryStore, n: usize) -> bool {
    for _ in 0..100 {
        if store.labels.lock().unwrap().len() >= n {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

fn request() -> ForecastRequest {
    ForecastRequest {
        issue: IssueRef {
            id: "issue-1".into(),
            uri: "cairn://p/prj/1".into(),
        },
        title: "Retry the upload when S3 times out".into(),
        description: "Uploads fail on the first timeout; retry twice with backoff.".into(),
        text_hash: "h".into(),
        filing_maker: Some("anthropic".into()),
        filing_model: Some("opus".into()),
        recipe: Some("build".into()),
        first_step_role: Some("Build".into()),
        snapshot_at: 1_792_400_000,
        deadline: Instant::now() + Duration::from_secs(5),
    }
}

fn forecaster_for(
    answerer: Arc<FakeAnswerer>,
    calibration: Option<Calibration>,
) -> (DecideScoreForecaster, Arc<MemoryStore>) {
    let store = Arc::new(MemoryStore {
        calibration,
        ..MemoryStore::default()
    });
    (DecideScoreForecaster::new(answerer, store.clone()), store)
}

fn unavailable(outcome: ForecastOutcome) -> Unavailable {
    match outcome {
        ForecastOutcome::Unavailable(unavailable) => unavailable,
        other => panic!("expected unavailable, got {other:?}"),
    }
}

// ---------------------------------------------------------------- contract

#[tokio::test]
async fn an_applicable_issue_gets_the_calibrated_p50_and_p90_with_provenance() {
    let answerer = FakeAnswerer::new(Behaviour::Score(0.5));
    let (forecaster, store) = forecaster_for(answerer.clone(), Some(fixture_calibration()));
    let ForecastOutcome::Available(forecast) = forecaster.forecast(request()).await else {
        panic!("expected a forecast");
    };
    // 0.5 exceeds two edges: bin 2, [12, 13, 14] shifted by [0.1, 0.2].
    assert_eq!(forecast.p50_tokens, 12f64.exp().round() as u64);
    assert_eq!(forecast.p90_tokens, 14.2f64.exp().round() as u64);
    assert!(forecast.p90_tokens >= forecast.p50_tokens);
    assert_eq!(forecast.target, TargetDef::current());
    assert_eq!(forecast.answerer.kind, ANSWERER_KIND_DECIDE_SCORE);
    assert_eq!(forecast.answerer.version, ANSWERER_VERSION);
    assert_eq!(
        forecast.answerer.question_version,
        question::question_version()
    );
    assert_eq!(forecast.answerer.zdr_satisfied_by, "openrouter-zdr-routing");
    assert_eq!(forecast.calibration.version, "tfc-fixture");
    assert_eq!(forecast.calibration.group, "anthropic");
    assert_eq!(
        forecast.flags,
        ApplicabilityFlags {
            p90_soft: false,
            p90_soft_basis: None
        }
    );
    assert_eq!(forecast.input_hash.len(), 64);
    assert_eq!(answerer.calls(), 1);
    assert!(
        labels_landed(&store, 1).await,
        "an answered filing stores its label"
    );
    let labels = store.labels.lock().unwrap();
    assert_eq!(labels[0].source, store::LabelSource::Filing);
    assert_eq!(labels[0].filing_maker.as_deref(), Some("anthropic"));
}

#[tokio::test]
async fn a_top_bin_score_carries_a_soft_p90() {
    let (forecaster, _) = forecaster_for(
        FakeAnswerer::new(Behaviour::Score(0.9)),
        Some(fixture_calibration()),
    );
    let ForecastOutcome::Available(forecast) = forecaster.forecast(request()).await else {
        panic!("expected a forecast");
    };
    assert_eq!(
        forecast.flags,
        ApplicabilityFlags {
            p90_soft: true,
            p90_soft_basis: Some(SoftBasis::TopScoreBin)
        }
    );
    let (forecaster, _) = forecaster_with(Behaviour::Score(0.3));
    let ForecastOutcome::Available(forecast) = forecaster.forecast(request()).await else {
        panic!("expected a forecast");
    };
    assert_eq!(
        forecast.flags.p90_soft_basis,
        Some(SoftBasis::BinHeldoutCoverageBelow085)
    );
}

fn forecaster_with(behaviour: Behaviour) -> (DecideScoreForecaster, Arc<FakeAnswerer>) {
    let answerer = FakeAnswerer::new(behaviour);
    let (forecaster, _) = forecaster_for(answerer.clone(), Some(fixture_calibration()));
    (forecaster, answerer)
}

#[tokio::test]
async fn no_active_calibration_is_retryable_and_spends_nothing() {
    let answerer = FakeAnswerer::new(Behaviour::Score(0.5));
    let (forecaster, _) = forecaster_for(answerer.clone(), None);
    let outcome = unavailable(forecaster.forecast(request()).await);
    assert_eq!(outcome.reason, UnavailableReason::NoActiveCalibration);
    assert!(outcome.retryable);
    assert_eq!(answerer.calls(), 0);

    // A calibration of another answerer version is not this one's.
    let mut other = fixture_calibration();
    other.answerer_version = "embedding_head.v1".into();
    let (forecaster, _) = forecaster_for(answerer.clone(), Some(other));
    let outcome = unavailable(forecaster.forecast(request()).await);
    assert_eq!(outcome.reason, UnavailableReason::NoActiveCalibration);
    assert_eq!(answerer.calls(), 0);
}

#[tokio::test]
async fn a_slow_answerer_is_cut_off_at_the_deadline() {
    let (forecaster, _) = forecaster_with(Behaviour::Slow);
    let started = Instant::now();
    let mut request = request();
    request.deadline = Instant::now() + Duration::from_millis(150);
    let outcome = unavailable(forecaster.forecast(request).await);
    assert_eq!(outcome.reason, UnavailableReason::DeadlineExceeded);
    assert!(outcome.retryable);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "returned within budget"
    );
}

#[tokio::test]
async fn a_slow_calibration_read_is_cut_off_at_the_deadline_without_a_model_call() {
    let answerer = FakeAnswerer::new(Behaviour::Score(0.5));
    let store = Arc::new(MemoryStore {
        calibration: Some(fixture_calibration()),
        calibration_delay: Duration::from_secs(30),
        ..MemoryStore::default()
    });
    let forecaster = DecideScoreForecaster::new(answerer.clone(), store);
    let mut request = request();
    request.deadline = Instant::now() + Duration::from_millis(150);
    let started = Instant::now();
    let outcome = unavailable(forecaster.forecast(request).await);
    assert_eq!(outcome.reason, UnavailableReason::DeadlineExceeded);
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(answerer.calls(), 0);
}

#[tokio::test]
async fn a_slow_label_store_never_holds_the_forecast_and_the_label_still_lands() {
    let answerer = FakeAnswerer::new(Behaviour::Score(0.5));
    let store = Arc::new(MemoryStore {
        calibration: Some(fixture_calibration()),
        label_delay: Duration::from_millis(1500),
        ..MemoryStore::default()
    });
    let forecaster = DecideScoreForecaster::new(answerer, store.clone());
    let mut request = request();
    request.deadline = Instant::now() + Duration::from_millis(500);
    let started = Instant::now();
    let outcome = forecaster.forecast(request).await;
    assert!(
        matches!(outcome, ForecastOutcome::Available(_)),
        "{outcome:?}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "returned within the deadline"
    );
    assert!(
        labels_landed(&store, 1).await,
        "the paid answer's label is kept"
    );
}

#[tokio::test]
async fn answerer_failures_map_to_their_reasons_and_retryability() {
    for (behaviour, reason, retryable) in [
        (Behaviour::Fail, UnavailableReason::AnswererFailed, true),
        (
            Behaviour::Unanswerable,
            UnavailableReason::AnswererUnanswerable,
            false,
        ),
        (Behaviour::Refuse, UnavailableReason::Refused, false),
    ] {
        let (forecaster, _) = forecaster_with(behaviour);
        let outcome = unavailable(forecaster.forecast(request()).await);
        assert_eq!(outcome.reason, reason);
        assert_eq!(outcome.retryable, retryable, "{reason:?}");
    }
}

#[tokio::test]
async fn out_of_scope_issues_are_not_applicable_without_a_model_call() {
    type Edit = Box<dyn Fn(&mut ForecastRequest)>;
    let cases: Vec<(Edit, NotApplicableReason)> = vec![
        (
            Box::new(|r| {
                r.filing_maker = None;
                r.filing_model = Some("gpt-6.1-sol".into());
            }),
            NotApplicableReason::MakerNotAnthropic,
        ),
        (
            Box::new(|r| {
                r.filing_maker = None;
                r.filing_model = None;
            }),
            NotApplicableReason::MakerUnknown,
        ),
        (
            Box::new(|r| r.first_step_role = Some("Coordinator".into())),
            NotApplicableReason::CoordinatorRole,
        ),
        (
            Box::new(|r| {
                r.title = "  ".into();
                r.description = String::new();
            }),
            NotApplicableReason::EmptyText,
        ),
    ];
    for (edit, reason) in cases {
        let (forecaster, answerer) = forecaster_with(Behaviour::Score(0.5));
        let mut request = request();
        edit(&mut request);
        match forecaster.forecast(request).await {
            ForecastOutcome::NotApplicable(refusal) => assert_eq!(refusal.reason, reason),
            other => panic!("expected not_applicable {reason:?}, got {other:?}"),
        }
        assert_eq!(answerer.calls(), 0, "{reason:?} asked the model");
    }
}

#[tokio::test]
async fn a_secret_in_the_description_never_reaches_the_answerer() {
    let (forecaster, answerer) = forecaster_with(Behaviour::Score(0.5));
    let mut request = request();
    request.description =
        "Deploy with sk-ant-api03-AbCdEfGh12345678_xyz and Bearer abcdefghijklmnop".into();
    let _ = forecaster.forecast(request).await;
    let seen = answerer.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert!(!seen[0].rendered.contains("AbCdEfGh12345678"));
    assert!(!seen[0].rendered.contains("abcdefghijklmnop"));
    assert!(seen[0].rendered.starts_with(question::preamble()));
}

#[test]
fn the_valid_outcomes_fixture_round_trips_and_covers_every_outcome() {
    let raw: Value = serde_json::from_str(VALID_OUTCOMES_FIXTURE).unwrap();
    let outcomes: Vec<ForecastOutcome> = serde_json::from_value(raw.clone()).unwrap();
    assert_eq!(serde_json::to_value(&outcomes).unwrap(), raw);
    assert!(matches!(outcomes[0], ForecastOutcome::Available(_)));
    assert!(matches!(outcomes[1], ForecastOutcome::Unavailable(_)));
    assert!(matches!(outcomes[2], ForecastOutcome::NotApplicable(_)));
}

#[test]
fn a_forecast_breaking_its_invariants_does_not_deserialize() {
    let mut raw: Value = serde_json::from_str(VALID_OUTCOMES_FIXTURE).unwrap();
    raw[0]["p90_tokens"] = json!(1);
    assert!(serde_json::from_value::<ForecastOutcome>(raw[0].clone()).is_err());
    let parts = ForecastParts {
        answerer: AnswererProvenance {
            kind: ANSWERER_KIND_DECIDE_SCORE.into(),
            version: ANSWERER_VERSION.into(),
            model: DECIDE_MODEL.into(),
            provider: DECIDE_PROVIDER.into(),
            question_version: question::question_version().into(),
            measured_by: "native".into(),
            zdr_satisfied_by: "openrouter-zdr-routing".into(),
            cost_usd: None,
            latency_ms: 1,
        },
        calibration: fixture_calibration().provenance(),
        flags: ApplicabilityFlags {
            p90_soft: false,
            p90_soft_basis: None,
        },
        input_hash: "x".into(),
    };
    assert!(Forecast::from_ln(f64::NAN, 10.0, parts.clone()).is_err());
    assert!(Forecast::from_ln(10.0, f64::INFINITY, parts.clone()).is_err());
    assert!(Forecast::from_ln(11.0, 10.0, parts.clone()).is_err());
    assert!(Forecast::from_ln(10.0, 11.0, parts).is_ok());
}

// ---------------------------------------------------------------- golden ports

#[test]
fn the_questions_are_exactly_the_ones_the_bench_measured() {
    // label_decide.ts's and score.py's QUESTION_VERSION for the bench's
    // questions/size.v1.json: the copy asks exactly what 6271 measured.
    assert_eq!(question::question_version(), "e5a0112ceee9");
    assert_eq!(
        question::render_input("Title: t\n\nDescription:\nd"),
        "Below is a software task exactly as it was filed for an AI coding agent to carry out. Answer each question from the text alone.\n\nTitle: t\n\nDescription:\nd"
    );
    let questions = question::decision_questions();
    assert_eq!(questions.len(), 2);
    assert_eq!(questions[0].name, "effort");
    assert_eq!(questions[1].name, "scope");
}

#[test]
fn the_filing_text_renders_as_features_py_does() {
    assert_eq!(
        question::render_filing_text("  Fix it ", "\n body \n"),
        "Title: Fix it\n\nDescription:\nbody"
    );
    assert_eq!(
        question::render_filing_text("t", "   "),
        "Title: t\n\nDescription:\n(empty)"
    );
    let long = "é".repeat(question::DESCRIPTION_CHAR_CAP + 3);
    let rendered = question::render_filing_text("t", &long);
    assert!(rendered.ends_with("\n[... truncated]"));
    assert_eq!(
        rendered.chars().filter(|c| *c == 'é').count(),
        question::DESCRIPTION_CHAR_CAP
    );
}

#[test]
fn the_expected_score_renormalises_over_answered_levels() {
    let p = |pairs: &[(&str, f64)]| -> Vec<OptionProbability> {
        pairs
            .iter()
            .map(|(value, probability)| OptionProbability {
                value: value.to_string(),
                probability: *probability,
            })
            .collect()
    };
    let full = p(&[
        ("0", 0.1),
        ("0.25", 0.2),
        ("0.5", 0.4),
        ("0.75", 0.2),
        ("1", 0.1),
    ]);
    assert!((expected_score(&full).unwrap() - 0.5).abs() < 1e-12);
    let partial = p(&[("0", 0.5), ("1", 0.25)]);
    assert!((expected_score(&partial).unwrap() - 1.0 / 3.0).abs() < 1e-12);
    assert_eq!(expected_score(&p(&[("x", 1.0)])), None);
    assert_eq!(expected_score(&[]), None);
}

/// Numbers produced by `bench/token-forecast/score.py` on the same pairs:
/// `[(((i*37)%41)/40, 14 + 2*s + ((i*17)%11)/5) for i in range(40)]`.
#[test]
fn the_fit_math_matches_score_py() {
    let pairs: Vec<(f64, f64)> = (0..40)
        .map(|i| {
            let s = ((i * 37) % 41) as f64 / 40.0;
            (s, 14.0 + 2.0 * s + ((i * 17) % 11) as f64 / 5.0)
        })
        .collect();
    let table = decide_table(&pairs).unwrap();
    assert_eq!(
        table.edges,
        vec![0.22000000000000003, 0.41500000000000004, 0.61, 0.805]
    );
    assert_eq!(
        table.bins,
        vec![
            [15.075000000000001, 16.13, 16.18],
            [15.75, 16.330000000000002, 16.38],
            [15.95, 16.53, 16.580000000000002],
            [16.15, 16.73, 16.779999999999998],
            [16.85, 17.01, 17.335],
        ]
    );
    let ys: Vec<f64> = pairs.iter().map(|(_, y)| *y).collect();
    assert_eq!(quantile(&ys, 0.9), Some(16.854999999999997));
    assert_eq!(quantile(&[1.0, 2.0, 4.0], 0.25), Some(1.5));
    let residuals = |j: usize| {
        pairs
            .iter()
            .map(|(s, y)| y - table.bins[table.bin_of(*s)][j])
            .collect::<Vec<_>>()
    };
    assert_eq!(
        conformal_shift(&residuals(2), 0.9),
        Some(0.07000000000000028)
    );
    assert_eq!(
        conformal_shift(&residuals(1), 0.8),
        Some(0.019999999999999574)
    );
    assert_eq!(
        conformal_shift(&(1..10).map(f64::from).collect::<Vec<_>>(), 0.9),
        Some(9.0)
    );
    assert_eq!(monotone([3.0, 2.0, 1.0]), [3.0, 3.0, 3.0]);
    // Bins under five issues take the pooled quantiles.
    let small = decide_table(&[(0.1, 1.0), (0.2, 2.0), (0.9, 3.0)]).unwrap();
    assert_eq!(
        small.edges,
        vec![
            0.14,
            0.18000000000000002,
            0.33999999999999997,
            0.6200000000000001
        ]
    );
    assert!(small.bins.iter().all(|b| *b == [2.0, 2.6, 2.8]));
}

#[test]
fn the_maker_rule_matches_extract_py() {
    for (model, want) in [
        ("opus", Some("anthropic")),
        ("claude-sonnet-4-5-20250929", Some("anthropic")),
        ("fable", Some("anthropic")),
        ("gpt-5.6-sol", Some("openai")),
        ("o3-mini", Some("openai")),
        ("~deepseek/deepseek-flash-latest", Some("deepseek")),
        ("dsv4-flash", Some("deepseek")),
        ("z-ai/glm-5.2", Some("zhipu")),
        ("glm-5.3", Some("zhipu")),
        ("moonshotai/kimi-k3", Some("moonshot")),
        ("anthropic", Some("anthropic")),
        ("custom:abc/whatever", Some("custom")),
        ("", None),
    ] {
        assert_eq!(maker(Some(model)).as_deref(), want, "{model}");
    }
    assert_eq!(maker(None), None);
}

fn job(id: &str, node: &str, started_at: Option<i64>) -> FirstJob {
    FirstJob {
        id: id.into(),
        recipe_node_id: Some(node.into()),
        created_at: 100,
        started_at,
        model: None,
        selection_maker: None,
        selection_model: None,
    }
}

/// `test_score.FilingMaker`: jobs of one execution share a creation second
/// and random ids, so the filing-time job comes from the recipe graph.
#[test]
fn the_filing_job_is_the_graph_root_not_the_first_by_id_or_time() {
    let nodes = json!([
        {"id": "trig", "nodeType": "trigger"}, {"id": "plan", "nodeType": "agent"},
        {"id": "art", "nodeType": "artifact"}, {"id": "build", "nodeType": "agent"},
        {"id": "delegated-x-trigger", "nodeType": "trigger"},
        {"id": "delegated-x-agent", "nodeType": "agent"}
    ]);
    let edges = json!([
        {"edgeType": "control", "sourceNodeId": "trig", "targetNodeId": "plan"},
        {"edgeType": "context", "sourceNodeId": "trig", "targetNodeId": "build"},
        {"edgeType": "control", "sourceNodeId": "plan", "targetNodeId": "build"},
        {"edgeType": "control", "sourceNodeId": "delegated-x-trigger", "targetNodeId": "delegated-x-agent"}
    ]);
    let jobs = vec![
        job("z-planner", "plan", Some(109)),
        job("a-builder", "build", Some(100)),
        job("0-delegate", "delegated-x-agent", Some(50)),
    ];
    let (found, method) = filing_job(Some(&nodes), Some(&edges), &jobs).unwrap();
    assert_eq!((found.id.as_str(), method), ("z-planner", "graph"));
    let jobs = vec![job("a", "x", None), job("b", "y", Some(5))];
    let (found, method) = filing_job(None, None, &jobs).unwrap();
    assert_eq!((found.id.as_str(), method), ("b", "launch_order"));
}

#[test]
fn a_write_reply_ties_creations_to_issue_numbers() {
    let reply = r#"Applied: {"applied":[{"index":0,"data":{"uri":"cairn://p/Cairn/7356"}},{"index":2,"summary":"Created issue cairn/12"}]} trailing"#;
    let tied = created_by_result(reply);
    assert_eq!(tied.get(&0), Some(&("cairn".to_string(), 7356)));
    assert_eq!(tied.get(&2), Some(&("cairn".to_string(), 12)));
    assert!(created_by_result("no json here").is_empty());
}

// ---------------------------------------------------------------- refit

const DAY: i64 = 86_400;

fn issue(id: usize, created_at: i64, resolved_at: Option<i64>, y: f64) -> PopulationIssue {
    PopulationIssue {
        issue_id: format!("i{id}"),
        project_key: Some("prj".into()),
        number: id as i64,
        created_at,
        resolved_at,
        open_executions: 0,
        last_bucket: None,
        primary_billable: y.exp().round() as i64,
        primary_maker: Some("anthropic".into()),
    }
}

/// `n` issues spread evenly over the 100 days before `as_of`, each resolved
/// a day after filing, with a score that tracks its tokens.
fn population(n: usize, as_of: i64) -> (Vec<PopulationIssue>, HashMap<String, FitLabel>) {
    let mut issues = Vec::new();
    let mut labels = HashMap::new();
    for i in 0..n {
        let created = as_of - 100 * DAY + (i as i64 * 99 * DAY) / n as i64;
        let score = ((i * 37) % 101) as f64 / 100.0;
        let y = 13.0 + 4.0 * score + ((i * 17) % 11) as f64 / 10.0;
        issues.push(issue(i, created, Some(created + DAY / 2), y));
        labels.insert(
            format!("i{i}"),
            FitLabel {
                score,
                filing_maker: Some("anthropic".into()),
                text_source: Some("event".into()),
            },
        );
    }
    (issues, labels)
}

#[test]
fn a_refit_is_reproducible_and_flags_the_top_bin_soft() {
    let as_of = FLOOR + 200 * DAY;
    let (issues, labels) = population(800, as_of);
    let first = fit::refit(&issues, &labels, as_of, as_of, ANSWERER_VERSION, "q");
    let again = fit::refit(&issues, &labels, as_of, as_of, ANSWERER_VERSION, "q");
    assert_eq!(first, again, "the same inputs give the same calibration");
    let RefitVerdict::Accepted(calibration) = first.verdict else {
        panic!("expected acceptance: {:?}", first.verdict);
    };
    assert!(calibration.version.starts_with("tfc-"));
    assert_eq!(calibration.fit_n, 800);
    assert_eq!(calibration.shift_window, [as_of - 21 * DAY, as_of]);
    assert_eq!(calibration.bins.len(), 5);
    assert_eq!(calibration.bins[4].soft_basis, Some(SoftBasis::TopScoreBin));
    // A different as_of is a different calibration.
    let later = fit::refit(
        &issues,
        &labels,
        as_of + 1,
        as_of + 1,
        ANSWERER_VERSION,
        "q",
    );
    let RefitVerdict::Accepted(later) = later.verdict else {
        panic!("expected acceptance");
    };
    assert_ne!(later.version, calibration.version);
}

#[test]
fn a_refit_with_too_few_issues_is_rejected_and_exclusions_are_counted() {
    let as_of = FLOOR + 200 * DAY;
    let (mut issues, mut labels) = population(120, as_of);
    issues.push(issue(9001, as_of - 5 * DAY, None, 15.0));
    issues.push(issue(9002, FLOOR - DAY, Some(FLOOR), 15.0));
    labels.get_mut("i3").unwrap().filing_maker = Some("openai".into());
    let refit = fit::refit(&issues, &labels, as_of, as_of, ANSWERER_VERSION, "q");
    let RefitVerdict::Rejected(rejected) = &refit.verdict else {
        panic!("expected rejection");
    };
    assert!(
        rejected.reason.contains("too few issues"),
        "{}",
        rejected.reason
    );
    assert_eq!(refit.exclusions.get("unresolved"), Some(&1));
    assert_eq!(refit.exclusions.get("before_floor"), Some(&1));
    assert_eq!(refit.exclusions.get("filing_maker_openai"), Some(&1));
}

#[test]
fn a_replay_scores_resolved_issues_and_counts_censored_misses() {
    let as_of = FLOOR + 200 * DAY;
    let (issues, labels) = population(800, as_of);
    let RefitVerdict::Accepted(calibration) =
        fit::refit(&issues, &labels, as_of, as_of, ANSWERER_VERSION, "q").verdict
    else {
        panic!("expected acceptance");
    };
    let mut window = Vec::new();
    let mut window_labels = labels.clone();
    for i in 0..100 {
        let score = ((i * 13) % 101) as f64 / 100.0;
        let y = 13.0 + 4.0 * score + ((i * 7) % 11) as f64 / 10.0;
        let mut item = issue(5000 + i, as_of + DAY, Some(as_of + 2 * DAY), y);
        if i < 3 {
            // Unresolved and already far past any p90.
            item.resolved_at = None;
            item.primary_billable = 30f64.exp() as i64;
        }
        window_labels.insert(
            item.issue_id.clone(),
            FitLabel {
                score,
                filing_maker: Some("anthropic".into()),
                text_source: None,
            },
        );
        window.push(item);
    }
    let report = fit::replay(
        &calibration,
        &window,
        &window_labels,
        as_of,
        as_of + 14 * DAY,
        as_of + 30 * DAY,
    );
    assert_eq!(report.n, 97);
    assert_eq!(report.censored_n, 3);
    assert_eq!(report.censored_misses, 3);
    let (p90, bar) = (
        report.p90_coverage.unwrap(),
        report.bar_p90_coverage.unwrap(),
    );
    assert!(
        (bar - p90 * 97.0 / 100.0).abs() < 1e-9,
        "censored misses join the denominator"
    );
    assert_eq!(report.largest_fifth_by_actuals.n, 19);
    assert_eq!(report.p90_soft.n + report.p90_unflagged.n, 97);
}

// ---------------------------------------------------------------- storage

async fn seeded(n: usize, as_of: i64) -> Arc<LocalDb> {
    seeded_with(n, as_of, true).await
}

async fn seeded_with(n: usize, as_of: i64, labelled: bool) -> Arc<LocalDb> {
    let db = crate::storage::migrated_test_db("token-forecast").await;
    let mut script = String::from(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('p', 'default', 'P', 'prj', '/tmp/p', 1, 1);\n",
    );
    let (issues, labels) = population(n, as_of);
    for (i, issue) in issues.iter().enumerate() {
        let id = &issue.issue_id;
        let resolved = issue.resolved_at.unwrap();
        script.push_str(&format!(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at, merged_at)
               VALUES ('{id}', 'p', {i}, 'Issue {i}', 'merged', {c}, {c}, {resolved});
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e{i}', 'build', '{id}', 'p', 'complete', {c}, 1);
             INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, model, created_at, updated_at)
               VALUES ('j{i}', 'e{i}', 'build', '{id}', 'p', 'complete', 'builder', 'builder', 'opus', {c}, {c});
             INSERT INTO token_rollup (id, bucket_start, project_id, run_id, job_id, model, backend, billable_tokens)
               VALUES ('t{i}', {c}, 'p', 'r{i}', 'j{i}', 'claude-opus-5-5', 'claude', {b});\n",
            c = issue.created_at,
            b = issue.primary_billable,
        ));
        let label = &labels[id];
        if !labelled {
            continue;
        }
        script.push_str(&format!(
            "INSERT INTO token_forecast_labels (issue_id, answerer_version, question_version, status, score, filing_maker, text_source, source, labeled_at)
               VALUES ('{id}', '{ANSWERER_VERSION}', '{q}', 'labelled', {s:?}, 'anthropic', 'event', 'backfill', {l});\n",
            q = question::question_version(),
            s = label.score,
            l = as_of - DAY,
        ));
    }
    db.execute_script(&script).await.unwrap();
    Arc::new(db)
}

#[tokio::test]
async fn the_population_reads_resolution_and_primary_tokens_from_the_database() {
    let as_of = FLOOR + 200 * DAY;
    let db = seeded(5, as_of).await;
    let issues = population::load(&db, FLOOR).await.unwrap();
    let (expected, _) = population(5, as_of);
    assert_eq!(issues.len(), 5);
    for (got, want) in issues.iter().zip(&expected) {
        assert_eq!(got.issue_id, want.issue_id);
        assert_eq!(got.resolved_at, want.resolved_at);
        assert_eq!(got.primary_billable, want.primary_billable);
        assert_eq!(got.primary_maker.as_deref(), Some("anthropic"));
        assert_eq!(got.last_bucket, Some(want.created_at));
    }
}

#[tokio::test]
async fn a_refit_from_the_database_activates_one_calibration_and_supersedes_the_last() {
    let as_of = FLOOR + 200 * DAY;
    let db = seeded(400, as_of).await;
    let dbs = vec![db.clone()];
    let refit = worker::refit_if_due(&dbs, &db, ANSWERER_VERSION, as_of)
        .await
        .unwrap()
        .expect("a refit is due when labels exist and none was attempted");
    let RefitVerdict::Accepted(first) = refit.verdict else {
        panic!("expected acceptance: {:?}", refit.verdict);
    };
    let active = store::active_calibration(&db, ANSWERER_VERSION, question::question_version())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active, first);
    assert!(
        worker::refit_if_due(&dbs, &db, ANSWERER_VERSION, as_of + DAY)
            .await
            .unwrap()
            .is_none(),
        "a fresh calibration is not refit"
    );
    // The same data and as_of give the same version.
    let again = worker::refit_now(&dbs, &db, ANSWERER_VERSION, as_of, as_of)
        .await
        .unwrap();
    let RefitVerdict::Accepted(again) = again.verdict else {
        panic!("expected acceptance");
    };
    assert_eq!(again.version, first.version);

    let week = as_of + worker::REFIT_EVERY_SECS;
    let refit = worker::refit_if_due(&dbs, &db, ANSWERER_VERSION, week)
        .await
        .unwrap()
        .unwrap();
    let RefitVerdict::Accepted(second) = refit.verdict else {
        panic!("expected acceptance");
    };
    assert_ne!(second.version, first.version);
    let history = store::refit_history(&db, ANSWERER_VERSION, question::question_version())
        .await
        .unwrap();
    let statuses: Vec<&str> = history
        .iter()
        .map(|(_, status, _, _)| status.as_str())
        .collect();
    assert_eq!(statuses, vec!["active", "superseded"]);
}

#[tokio::test]
async fn a_rejected_refit_is_recorded_and_retried_only_after_new_labels() {
    let as_of = FLOOR + 200 * DAY;
    let db = seeded(30, as_of).await;
    let dbs = vec![db.clone()];
    let refit = worker::refit_if_due(&dbs, &db, ANSWERER_VERSION, as_of)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(refit.verdict, RefitVerdict::Rejected(_)));
    assert!(
        store::active_calibration(&db, ANSWERER_VERSION, question::question_version())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        worker::refit_if_due(&dbs, &db, ANSWERER_VERSION, as_of + DAY)
            .await
            .unwrap()
            .is_none()
    );
    let history = store::refit_history(&db, ANSWERER_VERSION, question::question_version())
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].1, "rejected");
    assert!(history[0].3.is_empty() || history[0].3.values().all(|n| *n > 0));
}

#[tokio::test]
async fn the_database_store_serves_the_forecaster() {
    let as_of = FLOOR + 200 * DAY;
    let db = seeded(400, as_of).await;
    worker::refit_if_due(std::slice::from_ref(&db), &db, ANSWERER_VERSION, as_of)
        .await
        .unwrap();
    let clock: Arc<dyn crate::services::Clock> = Arc::new(crate::services::RealClock);
    let store = Arc::new(store::DbForecastStore::new(db.clone(), clock));
    let answerer = FakeAnswerer::new(Behaviour::Score(0.42));
    let forecaster = DecideScoreForecaster::new(answerer, store);
    let mut request = request();
    request.issue.id = "new-issue".into();
    let ForecastOutcome::Available(forecast) = forecaster.forecast(request).await else {
        panic!("expected a forecast");
    };
    assert!(forecast.p90_tokens >= forecast.p50_tokens);
    // The label is written off the caller's path.
    let mut labels = HashMap::new();
    for _ in 0..100 {
        labels = store::fit_labels(&db, ANSWERER_VERSION, question::question_version())
            .await
            .unwrap();
        if labels.contains_key("new-issue") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(labels["new-issue"].score, 0.42);
    assert_eq!(labels["new-issue"].text_source.as_deref(), Some("filing"));
}

// ---------------------------------------------------------------- backfill spend

fn tight_budget() -> backfill::Budget {
    backfill::Budget {
        calls: 50,
        examined: 500,
        daily_cap_usd: 0.0045,
        reserve_usd: 0.002,
        call_timeout: Duration::from_secs(5),
    }
}

#[tokio::test]
async fn paid_refusals_count_against_the_daily_cap_across_passes() {
    let as_of = FLOOR + 200 * DAY;
    let db = seeded_with(10, as_of, false).await;
    let dbs = vec![db.clone()];
    let answerer = FakeAnswerer::new(Behaviour::RefusePaid(0.001));
    let now = as_of + DAY;
    // Each refused call settles to $0.001; a fourth would pass $0.0045 once
    // its $0.002 reservation is counted.
    let first = backfill::pass(&dbs, &db, answerer.as_ref(), now, tight_budget()).await;
    assert_eq!(first.refused, 3, "{first:?}");
    assert_eq!(first.labelled, 0);
    assert!(
        first.stopped.as_deref().unwrap_or("").contains("daily cap"),
        "{first:?}"
    );
    // A later pass, as after a restart, reads the same ledger and makes no call.
    let second = backfill::pass(&dbs, &db, answerer.as_ref(), now + 60, tight_budget()).await;
    assert!(second.stopped.is_some(), "{second:?}");
    assert_eq!(answerer.calls(), 3);
    let spent = store::backfill_spend_since(&db, now - DAY).await.unwrap();
    assert!((spent - 0.003).abs() < 1e-9, "{spent}");
}

#[tokio::test]
async fn a_reply_without_a_cost_keeps_its_reservation() {
    let as_of = FLOOR + 200 * DAY;
    let db = seeded_with(10, as_of, false).await;
    let dbs = vec![db.clone()];
    let answerer = FakeAnswerer::new(Behaviour::ScoreUncosted(0.4));
    let now = as_of + DAY;
    let first = backfill::pass(&dbs, &db, answerer.as_ref(), now, tight_budget()).await;
    assert_eq!(first.labelled, 2, "{first:?}");
    let second = backfill::pass(&dbs, &db, answerer.as_ref(), now + 60, tight_budget()).await;
    assert_eq!(second.labelled, 0, "{second:?}");
    assert_eq!(answerer.calls(), 2);
    let spent = store::backfill_spend_since(&db, now - DAY).await.unwrap();
    assert!((spent - 0.004).abs() < 1e-9, "{spent}");
    // Labels landed with the filing maker the graph-less launch order gives.
    let labels = store::fit_labels(&db, ANSWERER_VERSION, question::question_version())
        .await
        .unwrap();
    assert_eq!(labels.len(), 2);
    assert!(labels
        .values()
        .all(|l| l.filing_maker.as_deref() == Some("anthropic")));
    assert!(labels
        .values()
        .all(|l| l.text_source.as_deref() == Some("current")));
}

#[test]
fn a_weak_largest_actuals_stratum_softens_every_bin() {
    let as_of = FLOOR + 200 * DAY;
    let (mut issues, labels) = population(800, as_of);
    // Make the shift window's largest 8% run far over what any bin says: few
    // enough that the p90 shift stays ordinary, enough to break the top fifth.
    let shift_from = as_of - 21 * DAY;
    let mut window: Vec<&mut PopulationIssue> = issues
        .iter_mut()
        .filter(|i| i.created_at >= shift_from)
        .collect();
    window.sort_by_key(|i| std::cmp::Reverse(i.primary_billable));
    let top = window.len() * 2 / 25;
    for issue in window.into_iter().take(top) {
        issue.primary_billable = 25f64.exp() as i64;
    }
    let RefitVerdict::Accepted(calibration) =
        fit::refit(&issues, &labels, as_of, as_of, ANSWERER_VERSION, "q").verdict
    else {
        panic!("expected acceptance");
    };
    assert!(calibration.large_actuals_coverage.unwrap() < fit::SOFT_COVERAGE);
    // The calibration-wide basis takes precedence, the top bin's included.
    assert!(calibration
        .bins
        .iter()
        .all(|b| b.soft_basis == Some(SoftBasis::LargestActualsHeldoutCoverageBelow085)));
}
