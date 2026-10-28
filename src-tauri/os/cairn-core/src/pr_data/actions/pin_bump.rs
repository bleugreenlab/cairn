//! The `pinBump` merge precondition.
//!
//! An upstream tool-version bump (a pin file such as a CLI's `*-pin.json`) may
//! land on the project base branch without a human only if the merge path itself
//! proves the change is exactly that bump and that the bump was verified at the
//! commit being landed. The precondition only ever ADDS a gate: every refusal
//! below stops a merge that would otherwise proceed, and an accepted bump still
//! passes every ordinary merge gate after it.
//!
//! [`evaluate`] is pure. Everything it reads is collected first by
//! [`collect_inputs`] (jj store reads and [`head_verdicts`]), and each collected
//! piece that can fail is carried as a `Result`, so a failure surfaces at the
//! step that needs it and the refusal order stays the one documented in
//! docs/checks.md.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::Path;

use cairn_db::models::{HumanBaseMergePolicyReadState, HumanBaseMergePolicyState};
use cairn_db::turso::params;

use crate::config::project_settings::{CheckCommand, PinBumpSpec};
use crate::execution::check_parsers::ParsedCheckResult;
use crate::execution::checks_status::{verdict_evidence, VerdictEvidence};
use crate::jj::GraphFileChange;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

use super::context::MergeMrContext;

/// Where the project config lives in every revision the gate reads.
const PROJECT_CONFIG_PATH: &str = ".cairn/config.yaml";

/// The `pinBumps` and `checks` sections of the merge TARGET's config.
#[derive(Debug, Clone, Default)]
pub(crate) struct BaseGateConfig {
    pub pin_bumps: BTreeMap<String, PinBumpSpec>,
    pub checks: HashMap<String, CheckCommand>,
}

/// One recorded verdict at a commit, as [`head_verdicts`] reads it.
#[derive(Debug, Clone)]
pub(crate) struct CollectedVerdict {
    pub check: String,
    pub platform: String,
    /// The commit the alias names. Only the head's own aliases count.
    pub commit_sha: String,
    pub observation_id: String,
    /// `fresh` (this commit ran it) or `cached` (an identical-input verdict
    /// was reused at this commit).
    pub disposition: String,
    pub passed: bool,
    pub failure_kind: Option<String>,
    pub parsed: Option<ParsedCheckResult>,
}

/// Everything [`evaluate`] decides from.
#[derive(Debug, Clone)]
pub(crate) struct PinBumpInputs {
    pub target_branch: String,
    pub base_branch: String,
    pub policy: HumanBaseMergePolicyReadState,
    /// The source bookmark's commit: the commit verified and the commit landed.
    pub head: Result<String, String>,
    /// The PR's own changes (merge base to head), with status.
    pub changed_files: Result<Vec<GraphFileChange>, String>,
    /// Where the head forked from the target, and the target's tip now. A bump
    /// is proven only against the base it was verified on, so the two must be
    /// the same commit.
    pub merge_base: Result<String, String>,
    pub target_tip: Result<String, String>,
    pub base_config: Result<BaseGateConfig, String>,
    /// The pin text at the target tip and at the head; `Ok(None)` when absent.
    pub base_pin: Result<Option<String>, String>,
    pub head_pin: Result<Option<String>, String>,
    pub verdicts: Result<Vec<CollectedVerdict>, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PinSide {
    Base,
    Head,
}

impl fmt::Display for PinSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Base => "base",
            Self::Head => "head",
        })
    }
}

/// Why a merge is not an admissible pin bump, in evaluation order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PinBumpRefusal {
    NotBaseLanding {
        target: String,
        base: String,
    },
    PolicyNotDisabled {
        state: String,
    },
    /// A fact the gate needs could not be read. Not a judgment about the bump.
    EvidenceUnavailable {
        what: &'static str,
        error: String,
    },
    NotPinOnly {
        files: Vec<String>,
    },
    NotOnCurrentBase {
        merge_base: String,
        target_tip: String,
    },
    NoSpec {
        path: String,
    },
    PinUnreadable {
        side: PinSide,
        error: String,
    },
    FrozenFieldChanged {
        field: String,
        base: String,
        head: String,
    },
    VersionNotForward {
        base: String,
        head: String,
    },
    VersionPrerelease {
        version: String,
    },
    VersionSkipped {
        version: String,
    },
    VerdictMissing {
        check: String,
        platform: String,
        head: String,
    },
    VerdictFailed {
        check: String,
        platform: String,
        head: String,
        observation_id: String,
    },
    VerdictUnverified {
        check: String,
        platform: String,
        head: String,
        reason: String,
    },
    VerdictNoEvidence {
        check: String,
        platform: String,
        head: String,
        failure_kind: String,
    },
    VerdictMisconfigured {
        check: String,
        reason: String,
    },
}

impl fmt::Display for PinBumpRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotBaseLanding { target, base } => write!(
                f,
                "not a base landing: the target `{target}` is not the project base branch `{base}`"
            ),
            Self::PolicyNotDisabled { state } => write!(
                f,
                "the provably-human base-merge policy is {state}, so no bump may land without a human"
            ),
            Self::EvidenceUnavailable { what, error } => {
                write!(f, "could not read {what}: {error}")
            }
            Self::NotPinOnly { files } => write!(
                f,
                "not a pin-only change: a bump modifies exactly one pin file and nothing else, but the change is [{}]",
                files.join(", ")
            ),
            Self::NotOnCurrentBase {
                merge_base,
                target_tip,
            } => write!(
                f,
                "not on the current base: the head forked from {merge_base}, but the target is at {target_tip}, so its verdicts never ran against the code it would land onto; let base-advance rebase it and its checks rerun"
            ),
            Self::NoSpec { path } => write!(
                f,
                "no spec: the base revision's `pinBumps` names no pin at `{path}`"
            ),
            Self::PinUnreadable { side, error } => {
                write!(f, "pin unreadable at the {side}: {error}")
            }
            Self::FrozenFieldChanged { field, base, head } => write!(
                f,
                "frozen field changed: `{field}` is {base} at the base and {head} at the head"
            ),
            Self::VersionNotForward { base, head } => write!(
                f,
                "version not forward: {head} does not advance past {base}"
            ),
            Self::VersionPrerelease { version } => write!(
                f,
                "pre-release: {version} is not a stable release, and a bump lands only stable ones"
            ),
            Self::VersionSkipped { version } => write!(
                f,
                "version skipped: the base pin's `skip` list holds back {version}"
            ),
            Self::VerdictMissing {
                check,
                platform,
                head,
            } => write!(
                f,
                "verdict missing: `{check}` on {platform} has no recorded verdict at {head} (pending, not run, or died before recording); record one with `cairn check run {check} --os {platform}` at that head"
            ),
            Self::VerdictFailed {
                check,
                platform,
                head,
                observation_id,
            } => write!(
                f,
                "verdict failed: `{check}` on {platform} failed at {head} (observation {observation_id})"
            ),
            Self::VerdictUnverified {
                check,
                platform,
                head,
                reason,
            } => write!(
                f,
                "verdict unverified: `{check}` on {platform} exited zero at {head} but {reason}"
            ),
            Self::VerdictNoEvidence {
                check,
                platform,
                head,
                failure_kind,
            } => write!(
                f,
                "verdict carries no evidence: `{check}` on {platform} at {head} ended as {failure_kind}, which is no verdict about the change"
            ),
            Self::VerdictMisconfigured { check, reason } => {
                write!(f, "verdict misconfigured: `{check}` {reason}")
            }
        }
    }
}

/// One verdict the accepted bump rests on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerdictCitation {
    pub check: String,
    pub platform: String,
    pub observation_id: String,
    pub disposition: String,
}

/// What an accepted bump proved, recorded on the merge journal's outcome row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PinBumpEvidence {
    pub tool: String,
    pub pin: String,
    pub from_version: String,
    pub to_version: String,
    pub head_commit: String,
    pub verdicts: Vec<VerdictCitation>,
}

impl fmt::Display for PinBumpEvidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "pinBump: {} `{}` {} -> {} at {}; verdicts:",
            self.tool, self.pin, self.from_version, self.to_version, self.head_commit
        )?;
        for citation in &self.verdicts {
            write!(
                f,
                " {}@{}={} ({})",
                citation.check, citation.platform, citation.observation_id, citation.disposition
            )?;
        }
        Ok(())
    }
}

/// One (check, platform) pair whose verdict the bump requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequiredVerdict {
    pub check: String,
    pub platform: String,
}

/// The changed file a bump consists of, when the change has the shape of one:
/// exactly one entry, and that entry a modification. An added, deleted, or
/// renamed pin is not a bump.
fn sole_modified_path(files: &[GraphFileChange]) -> Option<&str> {
    match files {
        [only] if only.status == "modified" => Some(only.path.as_str()),
        _ => None,
    }
}

fn spec_for_path<'a>(config: &'a BaseGateConfig, path: &str) -> Option<(&'a str, &'a PinBumpSpec)> {
    config
        .pin_bumps
        .iter()
        .find(|(_, spec)| spec.pin == path)
        .map(|(tool, spec)| (tool.as_str(), spec))
}

/// Every (check, platform) the spec requires: each of its `checks` on each of
/// its `platforms`. Or why a required check cannot be evidenced at all: the
/// spec names no platform, or a check does not trust a verdict from one of
/// them (its `verdictPlatforms`; absent means any platform, which is no
/// statement about the platforms a bump ships to).
fn required_verdicts(
    spec: &PinBumpSpec,
    checks: &HashMap<String, CheckCommand>,
) -> Result<Vec<RequiredVerdict>, PinBumpRefusal> {
    if spec.checks.is_empty() {
        return Err(PinBumpRefusal::VerdictMisconfigured {
            check: "(none)".to_string(),
            reason: "is all the spec names: a pinBumps entry must require at least one check"
                .to_string(),
        });
    }
    let mut platforms = spec.platforms.clone();
    platforms.sort();
    platforms.dedup();
    if platforms.is_empty() {
        return Err(PinBumpRefusal::VerdictMisconfigured {
            check: "(none)".to_string(),
            reason: "is proven nowhere: a pinBumps entry must name at least one platform"
                .to_string(),
        });
    }
    let mut required = Vec::new();
    for check in &spec.checks {
        let Some(command) = checks.get(check) else {
            return Err(PinBumpRefusal::VerdictMisconfigured {
                check: check.clone(),
                reason: "is not configured under `checks` at the base revision".to_string(),
            });
        };
        let trusted = command.verdict_platforms.as_deref().unwrap_or_default();
        if trusted.is_empty() {
            return Err(PinBumpRefusal::VerdictMisconfigured {
                check: check.clone(),
                reason: "declares no `verdictPlatforms`, so no platform's verdict can stand for the platforms a bump ships to".to_string(),
            });
        }
        if let Some(untrusted) = platforms
            .iter()
            .find(|platform| !trusted.contains(platform))
        {
            return Err(PinBumpRefusal::VerdictMisconfigured {
                check: check.clone(),
                reason: format!(
                    "does not trust a {untrusted} verdict (`verdictPlatforms` is [{}]), so it cannot prove a bump that must pass on {untrusted}",
                    trusted.join(", ")
                ),
            });
        }
        required.extend(platforms.iter().map(|platform| RequiredVerdict {
            check: check.clone(),
            platform: platform.clone(),
        }));
    }
    Ok(required)
}

/// Decide whether a merge is an admissible pin bump. See the module docs.
pub(crate) fn evaluate(inputs: &PinBumpInputs) -> Result<PinBumpEvidence, PinBumpRefusal> {
    // 1. Only a landing on the project base branch is a bump.
    if inputs.target_branch != inputs.base_branch {
        return Err(PinBumpRefusal::NotBaseLanding {
            target: inputs.target_branch.clone(),
            base: inputs.base_branch.clone(),
        });
    }

    // 2. The precondition reads the policy itself, independently of the
    //    ordinary policy gate, so approval machinery added there later can
    //    never silently open this path.
    match inputs.policy {
        HumanBaseMergePolicyReadState::Available { policy }
            if policy.state == HumanBaseMergePolicyState::Disabled => {}
        HumanBaseMergePolicyReadState::Available { .. } => {
            return Err(PinBumpRefusal::PolicyNotDisabled {
                state: "enabled".to_string(),
            })
        }
        HumanBaseMergePolicyReadState::Unavailable { reason } => {
            return Err(PinBumpRefusal::PolicyNotDisabled {
                state: format!("unavailable ({reason:?})"),
            })
        }
    }

    // 3. Exactly one modified file.
    let head = inputs
        .head
        .as_ref()
        .map_err(|error| PinBumpRefusal::EvidenceUnavailable {
            what: "the source branch head",
            error: error.clone(),
        })?;
    let files =
        inputs
            .changed_files
            .as_ref()
            .map_err(|error| PinBumpRefusal::EvidenceUnavailable {
                what: "the change's files",
                error: error.clone(),
            })?;
    let Some(path) = sole_modified_path(files) else {
        return Err(PinBumpRefusal::NotPinOnly {
            files: files
                .iter()
                .map(|file| match &file.previous_path {
                    Some(previous) => format!("{} {previous} -> {}", file.status, file.path),
                    None => format!("{} {}", file.status, file.path),
                })
                .collect(),
        });
    };

    // 4. The head carries the target tip, so the verdicts at the head ran
    //    against exactly the code the bump lands onto.
    let merge_base =
        inputs
            .merge_base
            .as_ref()
            .map_err(|error| PinBumpRefusal::EvidenceUnavailable {
                what: "the head's merge base with the target",
                error: error.clone(),
            })?;
    let target_tip =
        inputs
            .target_tip
            .as_ref()
            .map_err(|error| PinBumpRefusal::EvidenceUnavailable {
                what: "the target branch tip",
                error: error.clone(),
            })?;
    if merge_base != target_tip {
        return Err(PinBumpRefusal::NotOnCurrentBase {
            merge_base: merge_base.clone(),
            target_tip: target_tip.clone(),
        });
    }

    // 5. That file is a pin the BASE revision configures.
    let config =
        inputs
            .base_config
            .as_ref()
            .map_err(|error| PinBumpRefusal::EvidenceUnavailable {
                what: "the base revision's project config",
                error: error.clone(),
            })?;
    let Some((tool, spec)) = spec_for_path(config, path) else {
        return Err(PinBumpRefusal::NoSpec {
            path: path.to_string(),
        });
    };

    // 6. Both sides parse as pins.
    let base_pin = parse_pin(PinSide::Base, &inputs.base_pin)?;
    let head_pin = parse_pin(PinSide::Head, &inputs.head_pin)?;

    // 7. Frozen fields are untouched.
    for field in &spec.frozen {
        let before = base_pin.object.get(field);
        let after = head_pin.object.get(field);
        if before != after {
            let render = |value: Option<&serde_json::Value>| {
                value.map_or_else(|| "absent".to_string(), ToString::to_string)
            };
            return Err(PinBumpRefusal::FrozenFieldChanged {
                field: field.clone(),
                base: render(before),
                head: render(after),
            });
        }
    }

    // 8. The version moves forward, to a stable release.
    let forward = compare_versions(&head_pin.version, &base_pin.version).map_err(|error| {
        PinBumpRefusal::PinUnreadable {
            side: PinSide::Head,
            error,
        }
    })?;
    if forward != Ordering::Greater {
        return Err(PinBumpRefusal::VersionNotForward {
            base: base_pin.version.clone(),
            head: head_pin.version.clone(),
        });
    }

    if parse_version(&head_pin.version)
        .map_err(|error| PinBumpRefusal::PinUnreadable {
            side: PinSide::Head,
            error,
        })?
        .1
        .is_some()
    {
        return Err(PinBumpRefusal::VersionPrerelease {
            version: head_pin.version.clone(),
        });
    }

    // 9. The BASE pin's skip list is authoritative: a bump cannot unskip itself.
    if base_pin
        .skip
        .iter()
        .any(|skipped| skipped == &head_pin.version)
    {
        return Err(PinBumpRefusal::VersionSkipped {
            version: head_pin.version.clone(),
        });
    }

    // 10. Every required (check, platform) passed, verified, at this head.
    let required = required_verdicts(spec, &config.checks)?;
    let verdicts =
        inputs
            .verdicts
            .as_ref()
            .map_err(|error| PinBumpRefusal::EvidenceUnavailable {
                what: "the recorded check verdicts",
                error: error.clone(),
            })?;
    let mut citations = Vec::with_capacity(required.len());
    for RequiredVerdict { check, platform } in required {
        let Some(verdict) = verdicts.iter().find(|verdict| {
            verdict.check == check && verdict.platform == platform && &verdict.commit_sha == head
        }) else {
            return Err(PinBumpRefusal::VerdictMissing {
                check,
                platform,
                head: head.clone(),
            });
        };
        if let Some(failure_kind) = &verdict.failure_kind {
            return Err(PinBumpRefusal::VerdictNoEvidence {
                check,
                platform,
                head: head.clone(),
                failure_kind: failure_kind.clone(),
            });
        }
        match verdict_evidence(verdict.passed, verdict.parsed.as_ref()) {
            VerdictEvidence::Verified => citations.push(VerdictCitation {
                check,
                platform,
                observation_id: verdict.observation_id.clone(),
                disposition: verdict.disposition.clone(),
            }),
            VerdictEvidence::Failed => {
                return Err(PinBumpRefusal::VerdictFailed {
                    check,
                    platform,
                    head: head.clone(),
                    observation_id: verdict.observation_id.clone(),
                })
            }
            VerdictEvidence::Unverified(reason) => {
                return Err(PinBumpRefusal::VerdictUnverified {
                    check,
                    platform,
                    head: head.clone(),
                    reason: reason.to_string(),
                })
            }
        }
    }

    Ok(PinBumpEvidence {
        tool: tool.to_string(),
        pin: spec.pin.clone(),
        from_version: base_pin.version,
        to_version: head_pin.version,
        head_commit: head.clone(),
        verdicts: citations,
    })
}

struct Pin {
    object: serde_json::Map<String, serde_json::Value>,
    version: String,
    skip: Vec<String>,
}

fn parse_pin(side: PinSide, text: &Result<Option<String>, String>) -> Result<Pin, PinBumpRefusal> {
    let unreadable = |error: String| PinBumpRefusal::PinUnreadable { side, error };
    let text = text.as_ref().map_err(|error| unreadable(error.clone()))?;
    let Some(text) = text else {
        return Err(unreadable("the pin file is absent".to_string()));
    };
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|error| unreadable(format!("not JSON: {error}")))?;
    let serde_json::Value::Object(object) = value else {
        return Err(unreadable("the pin is not a JSON object".to_string()));
    };
    let version = object
        .get("version")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| unreadable("`version` is not a string".to_string()))?
        .to_string();
    parse_version(&version).map_err(unreadable)?;
    let skip = match object.get("skip") {
        None => Vec::new(),
        Some(serde_json::Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| unreadable(format!("`skip` holds a non-string {value}")))
            })
            .collect::<Result<_, _>>()?,
        Some(other) => return Err(unreadable(format!("`skip` is not an array: {other}"))),
    };
    Ok(Pin {
        object,
        version,
        skip,
    })
}

fn parse_version(version: &str) -> Result<(Vec<u64>, Option<&str>), String> {
    let (core, pre) = match version.split_once('-') {
        Some((core, pre)) => (core, Some(pre)),
        None => (version, None),
    };
    let parts = core
        .split('.')
        .map(|part| part.parse::<u64>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| format!("version {version:?} is not dotted-numeric"))?;
    Ok((parts, pre))
}

/// Order two pin versions: dot-separated numeric components (a missing
/// component counts as zero), then a release outranks any pre-release of the
/// same numbers, and two pre-release tails compare numerically aware. The
/// same rule as `compareVersions` in scripts/codex-pin.ts.
pub(crate) fn compare_versions(left: &str, right: &str) -> Result<Ordering, String> {
    let (left_parts, left_pre) = parse_version(left)?;
    let (right_parts, right_pre) = parse_version(right)?;
    for index in 0..left_parts.len().max(right_parts.len()) {
        let order = left_parts
            .get(index)
            .unwrap_or(&0)
            .cmp(right_parts.get(index).unwrap_or(&0));
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(match (left_pre, right_pre) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(left), Some(right)) => compare_numeric_aware(left, right),
    })
}

/// Compare two strings run by run, digit runs by value: `rc.10` follows
/// `rc.9`, and a digit run sorts before a letter run, as a numeric collation
/// does.
fn compare_numeric_aware(left: &str, right: &str) -> Ordering {
    fn runs(text: &str) -> Vec<&str> {
        let mut runs = Vec::new();
        let mut start = 0;
        let bytes = text.as_bytes();
        for index in 1..=bytes.len() {
            if index == bytes.len()
                || bytes[index].is_ascii_digit() != bytes[index - 1].is_ascii_digit()
            {
                runs.push(&text[start..index]);
                start = index;
            }
        }
        runs
    }
    let (left_runs, right_runs) = (runs(left), runs(right));
    for (a, b) in left_runs.iter().zip(right_runs.iter()) {
        let a_digits = a.as_bytes()[0].is_ascii_digit();
        let b_digits = b.as_bytes()[0].is_ascii_digit();
        let order = match (a_digits, b_digits) {
            (true, true) => {
                let (a, b) = (a.trim_start_matches('0'), b.trim_start_matches('0'));
                a.len().cmp(&b.len()).then_with(|| a.cmp(b))
            }
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase()),
        };
        if order != Ordering::Equal {
            return order;
        }
    }
    left_runs.len().cmp(&right_runs.len())
}

/// The newest verdict recorded AT `head_commit` for each required (check,
/// platform).
///
/// Reads commit aliases, so a `cached` alias counts: the engine records one
/// only when an identical-input verdict answered this exact commit. A row for
/// any other commit never counts, which is what makes a verdict on the PR's
/// parent "stale" rather than evidence. When several machines recorded a
/// verdict on one platform, the most recently evaluated decides, except that a
/// newer run that ended without a verdict never hides one (the rule
/// [`crate::execution::checks_status::select_lane_row`] states). The parsed
/// result that decides verified-vs-unverified lives on the verdict cache row
/// projected from the observation; when that row has since moved on to a newer
/// observation the verdict reads as having no structured result, which fails
/// closed.
pub(crate) async fn head_verdicts(
    db: &LocalDb,
    project_id: &str,
    head_commit: &str,
    required: &[RequiredVerdict],
) -> Result<Vec<CollectedVerdict>, String> {
    let mut checks: Vec<String> = required.iter().map(|r| r.check.clone()).collect();
    checks.sort();
    checks.dedup();
    let recorded = commit_verdicts(db, project_id, head_commit, &checks).await?;
    Ok(required
        .iter()
        .filter_map(|RequiredVerdict { check, platform }| {
            let mut newest_first = recorded
                .iter()
                .filter(|verdict| &verdict.check == check && &verdict.platform == platform);
            let newest = newest_first.clone().next();
            newest_first
                .find(|verdict| answers_the_lane(verdict))
                .or(newest)
                .cloned()
        })
        .collect())
}

fn answers_the_lane(verdict: &CollectedVerdict) -> bool {
    crate::execution::checks_status::row_existence(
        verdict.passed,
        verdict.failure_kind.as_deref(),
        None,
        verdict
            .parsed
            .as_ref()
            .map(ParsedCheckResult::verdict_facts)
            .as_ref(),
    )
    .answers_the_lane()
}

/// Every verdict recorded AT `commit` for each of `checks`, newest first within
/// a check, from any machine and any cadence (a lane, a manual whole-suite
/// run, a reuse aliased to this commit).
///
/// The one reader of "what was recorded at this commit": the `pinBump`
/// precondition takes the newest per platform from it. A row whose
/// observation names no platform reads with an empty one.
async fn commit_verdicts(
    db: &LocalDb,
    project_id: &str,
    commit: &str,
    checks: &[String],
) -> Result<Vec<CollectedVerdict>, String> {
    let keys = (project_id.to_string(), commit.to_string(), checks.to_vec());
    let sql = format!(
        "SELECT a.check_name, o.verdict_platform, a.commit_sha, o.id, a.disposition,
                o.verdict, o.failure_kind, c.target_results_json
           FROM check_result_commit_aliases a
           JOIN check_result_observations o ON o.id = a.source_observation_id
           LEFT JOIN check_result_cache c
             ON c.source_observation_id = o.id AND {}
          WHERE a.project_id = ?1 AND a.commit_sha = ?2 AND a.check_name = ?3
          ORDER BY a.evaluated_at DESC, o.ran_at DESC, o.rowid DESC",
        crate::execution::cache::verdict_row_predicate("c.row_family"),
    );
    db.read(|conn| {
        let keys = keys.clone();
        let sql = sql.clone();
        Box::pin(async move {
            let (project_id, commit, checks) = keys;
            let mut verdicts = Vec::new();
            for check in &checks {
                let mut rows = conn
                    .query(
                        &sql,
                        params![project_id.as_str(), commit.as_str(), check.as_str()],
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    verdicts.push(CollectedVerdict {
                        check: row.text(0)?,
                        platform: row.opt_text(1)?.unwrap_or_default(),
                        commit_sha: row.text(2)?,
                        observation_id: row.text(3)?,
                        disposition: row.text(4)?,
                        passed: row.text(5)? == "passed",
                        failure_kind: row.opt_text(6)?,
                        parsed: row
                            .opt_text(7)?
                            .and_then(|json| serde_json::from_str::<ParsedCheckResult>(&json).ok()),
                    });
                }
            }
            Ok(verdicts)
        })
    })
    .await
    .map_err(|error| format!("failed to read check verdicts: {error}"))
}

fn base_gate_config(text: Option<String>) -> Result<BaseGateConfig, String> {
    let Some(text) = text else {
        return Ok(BaseGateConfig::default());
    };
    let (settings, _) = crate::config::project_settings::parse_project_settings(&text)?;
    Ok(BaseGateConfig {
        pin_bumps: settings.pin_bumps.unwrap_or_default(),
        checks: settings.checks.unwrap_or_default(),
    })
}

/// Read everything [`evaluate`] needs from the shared jj store and the verdict
/// tables. Never fails: each fallible read is carried as a `Result` for the
/// evaluator to report at the step that needs it.
pub(crate) async fn collect_inputs(
    orch: &Orchestrator,
    db: &LocalDb,
    merge_context: &MergeMrContext,
    base_branch: &str,
    policy: HumanBaseMergePolicyReadState,
) -> PinBumpInputs {
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store =
        crate::jj::project_store_dir(&orch.config_dir, Path::new(&merge_context.mr.repo_path));
    let resolve = |branch: &str| {
        crate::jj::bookmark_commit_checked(&jj, &store, branch)?
            .ok_or_else(|| format!("the bookmark `{branch}` does not exist in the store"))
    };
    let head = resolve(&merge_context.source_branch);
    let target_tip = resolve(&merge_context.target_branch);

    let merge_base = match (&head, &target_tip) {
        (Ok(head), Ok(target_tip)) => crate::jj::revset_commit_checked(
            &jj,
            &store,
            &format!("heads(::{head} & ::{target_tip})"),
        )
        .and_then(|merge_base| {
            merge_base.ok_or_else(|| "the head and the target share no history".to_string())
        }),
        (Err(error), _) | (_, Err(error)) => Err(error.clone()),
    };
    let changed_files = match (&head, &merge_base) {
        (Ok(head), Ok(merge_base)) => {
            crate::jj::logical_changed_files(&jj, &store, merge_base, head)
                .map_err(|error| format!("jj could not diff {merge_base}..{head}: {error}"))
        }
        (Err(error), _) | (_, Err(error)) => Err(error.clone()),
    };
    let base_config = target_tip.clone().and_then(|tip| {
        crate::jj::text_at_revision(&jj, &store, &tip, PROJECT_CONFIG_PATH)
            .and_then(base_gate_config)
    });

    let matched = match (&changed_files, &base_config) {
        (Ok(files), Ok(config)) => sole_modified_path(files)
            .and_then(|path| spec_for_path(config, path))
            .map(|(_, spec)| (spec.clone(), required_verdicts(spec, &config.checks))),
        _ => None,
    };
    let read_pin = |revision: &Result<String, String>, path: &str| {
        revision
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|revision| crate::jj::text_at_revision(&jj, &store, revision, path))
    };
    let (base_pin, head_pin, required) = match &matched {
        Some((spec, required)) => (
            read_pin(&target_tip, &spec.pin),
            read_pin(&head, &spec.pin),
            required.clone().unwrap_or_default(),
        ),
        None => (Ok(None), Ok(None), Vec::new()),
    };
    let verdicts = match &head {
        Ok(head) if !required.is_empty() => {
            head_verdicts(db, &merge_context.project_id, head, &required).await
        }
        _ => Ok(Vec::new()),
    };

    PinBumpInputs {
        target_branch: merge_context.target_branch.clone(),
        base_branch: base_branch.to_string(),
        policy,
        head,
        changed_files,
        merge_base,
        target_tip,
        base_config,
        base_pin,
        head_pin,
        verdicts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

    const HEAD: &str = "head000000000000000000000000000000000000";
    const PARENT: &str = "parent0000000000000000000000000000000000";
    const TIP: &str = "tip0000000000000000000000000000000000000";
    const PIN: &str = "tools/codex-pin.json";

    const CONFIG: &str = r#"
checks:
  codex-pin:
    command: cargo nextest run pin
    verdictPlatforms: [macos, linux]
  codex-contract:
    command: cargo nextest run contract
    verdictPlatforms: [macos, linux]
pinBumps:
  codex:
    pin: tools/codex-pin.json
    frozen: [integration]
    checks: [codex-pin, codex-contract]
    platforms: [linux, macos]
  claude:
    pin: tools/claude-pin.json
    checks: [codex-pin]
    platforms: [linux, macos]
"#;

    fn config(yaml: &str) -> BaseGateConfig {
        base_gate_config(Some(yaml.to_string())).unwrap()
    }

    fn pin(version: &str, integration: i64, skip: &[&str]) -> Result<Option<String>, String> {
        Ok(Some(
            serde_json::json!({
                "integration": integration,
                "version": version,
                "skip": skip,
                "packages": {}
            })
            .to_string(),
        ))
    }

    fn change(path: &str, status: &str) -> GraphFileChange {
        GraphFileChange {
            path: path.to_string(),
            previous_path: None,
            status: status.to_string(),
            additions: 1,
            deletions: 1,
        }
    }

    fn nextest(passed: usize) -> ParsedCheckResult {
        serde_json::from_value(serde_json::json!({
            "parser": "nextest",
            "complete": true,
            "selection": "full",
            "passed": passed,
            "failed": 0,
            "skipped": 0,
            "failures": []
        }))
        .unwrap()
    }

    fn verdict(check: &str, platform: &str) -> CollectedVerdict {
        CollectedVerdict {
            check: check.to_string(),
            platform: platform.to_string(),
            commit_sha: HEAD.to_string(),
            observation_id: format!("obs-{check}-{platform}"),
            disposition: "fresh".to_string(),
            passed: true,
            failure_kind: None,
            parsed: Some(nextest(5)),
        }
    }

    fn all_green() -> Vec<CollectedVerdict> {
        ["codex-pin", "codex-contract"]
            .iter()
            .flat_map(|check| ["linux", "macos"].map(|platform| verdict(check, platform)))
            .collect()
    }

    fn inputs() -> PinBumpInputs {
        PinBumpInputs {
            target_branch: "main".to_string(),
            base_branch: "main".to_string(),
            policy: HumanBaseMergePolicyReadState::from_stored(
                r#"{"version":1,"state":"disabled"}"#,
            ),
            head: Ok(HEAD.to_string()),
            changed_files: Ok(vec![change(PIN, "modified")]),
            merge_base: Ok(TIP.to_string()),
            target_tip: Ok(TIP.to_string()),
            base_config: Ok(config(CONFIG)),
            base_pin: pin("0.163.0", 1, &[]),
            head_pin: pin("0.164.0", 1, &[]),
            verdicts: Ok(all_green()),
        }
    }

    fn refuse(inputs: PinBumpInputs) -> PinBumpRefusal {
        evaluate(&inputs).expect_err("the bump must be refused")
    }

    fn with_verdict(
        check: &str,
        platform: &str,
        edit: impl Fn(&mut CollectedVerdict),
    ) -> PinBumpInputs {
        let mut inputs = inputs();
        for verdict in inputs.verdicts.as_mut().unwrap() {
            if verdict.check == check && verdict.platform == platform {
                edit(verdict);
            }
        }
        inputs
    }

    #[test]
    fn pin_bump_accepts_a_verified_forward_bump_and_cites_every_verdict() {
        let evidence = evaluate(&inputs()).unwrap();
        assert_eq!(evidence.tool, "codex");
        assert_eq!(evidence.pin, PIN);
        assert_eq!(evidence.from_version, "0.163.0");
        assert_eq!(evidence.to_version, "0.164.0");
        assert_eq!(evidence.head_commit, HEAD);
        let cited: Vec<_> = evidence
            .verdicts
            .iter()
            .map(|citation| format!("{}@{}", citation.check, citation.platform))
            .collect();
        assert_eq!(
            cited,
            [
                "codex-pin@linux",
                "codex-pin@macos",
                "codex-contract@linux",
                "codex-contract@macos"
            ]
        );
        let detail = evidence.to_string();
        assert!(detail.contains("0.163.0 -> 0.164.0"), "{detail}");
        assert!(
            detail.contains("codex-contract@macos=obs-codex-contract-macos (fresh)"),
            "{detail}"
        );
    }

    #[test]
    fn pin_bump_refuses_a_non_base_target() {
        let mut inputs = inputs();
        inputs.target_branch = "agent/cairn-1-coordinator-0".to_string();
        // Also policy-enabled: the base-landing condition is evaluated first.
        inputs.policy =
            HumanBaseMergePolicyReadState::from_stored(r#"{"version":1,"state":"enabled"}"#);
        let refusal = refuse(inputs);
        assert!(
            matches!(refusal, PinBumpRefusal::NotBaseLanding { .. }),
            "{refusal:?}"
        );
        assert!(refusal.to_string().contains("not a base landing"));
    }

    #[test]
    fn pin_bump_refuses_when_the_human_merge_policy_is_enabled() {
        let mut inputs = inputs();
        inputs.policy =
            HumanBaseMergePolicyReadState::from_stored(r#"{"version":1,"state":"enabled"}"#);
        let refusal = refuse(inputs);
        assert_eq!(
            refusal,
            PinBumpRefusal::PolicyNotDisabled {
                state: "enabled".to_string()
            }
        );
    }

    #[test]
    fn pin_bump_refuses_when_the_human_merge_policy_is_unavailable() {
        let mut inputs = inputs();
        inputs.policy =
            HumanBaseMergePolicyReadState::from_stored(r#"{"version":2,"state":"disabled"}"#);
        let refusal = refuse(inputs);
        assert!(
            matches!(&refusal, PinBumpRefusal::PolicyNotDisabled { state } if state.starts_with("unavailable")),
            "{refusal:?}"
        );
    }

    #[test]
    fn pin_bump_refuses_a_foreign_file_alongside_the_pin() {
        let mut inputs = inputs();
        inputs.changed_files = Ok(vec![
            change(PIN, "modified"),
            change("src/lib.rs", "modified"),
        ]);
        let refusal = refuse(inputs);
        assert!(matches!(&refusal, PinBumpRefusal::NotPinOnly { files } if files.len() == 2));
        assert!(
            refusal.to_string().contains("modified src/lib.rs"),
            "{refusal}"
        );
    }

    #[test]
    fn pin_bump_refuses_two_pins_bumped_together() {
        let mut inputs = inputs();
        inputs.changed_files = Ok(vec![
            change(PIN, "modified"),
            change("tools/claude-pin.json", "modified"),
        ]);
        assert!(matches!(refuse(inputs), PinBumpRefusal::NotPinOnly { .. }));
    }

    #[test]
    fn pin_bump_refuses_an_added_pin() {
        let mut inputs = inputs();
        inputs.changed_files = Ok(vec![change(PIN, "added")]);
        let refusal = refuse(inputs);
        assert_eq!(
            refusal,
            PinBumpRefusal::NotPinOnly {
                files: vec![format!("added {PIN}")]
            }
        );
    }

    #[test]
    fn pin_bump_refuses_a_file_no_spec_names() {
        let mut inputs = inputs();
        inputs.changed_files = Ok(vec![change("tools/other-pin.json", "modified")]);
        assert_eq!(
            refuse(inputs),
            PinBumpRefusal::NoSpec {
                path: "tools/other-pin.json".to_string()
            }
        );
    }

    #[test]
    fn pin_bump_refuses_an_unreadable_head_pin() {
        let mut inputs = inputs();
        inputs.head_pin = Ok(Some(r#"{"version": 164}"#.to_string()));
        assert!(matches!(
            refuse(inputs),
            PinBumpRefusal::PinUnreadable {
                side: PinSide::Head,
                ..
            }
        ));
    }

    #[test]
    fn pin_bump_refuses_a_changed_frozen_integration() {
        let mut inputs = inputs();
        inputs.head_pin = pin("0.164.0", 2, &[]);
        assert_eq!(
            refuse(inputs),
            PinBumpRefusal::FrozenFieldChanged {
                field: "integration".to_string(),
                base: "1".to_string(),
                head: "2".to_string(),
            }
        );
    }

    #[test]
    fn pin_bump_refuses_a_downgrade() {
        let mut inputs = inputs();
        inputs.head_pin = pin("0.162.9", 1, &[]);
        assert!(matches!(
            refuse(inputs),
            PinBumpRefusal::VersionNotForward { .. }
        ));
    }

    #[test]
    fn pin_bump_refuses_an_equal_version() {
        let mut inputs = inputs();
        inputs.head_pin = pin("0.163.0", 1, &[]);
        assert_eq!(
            refuse(inputs),
            PinBumpRefusal::VersionNotForward {
                base: "0.163.0".to_string(),
                head: "0.163.0".to_string(),
            }
        );
    }

    #[test]
    fn pin_bump_refuses_a_version_the_base_skip_list_holds_back() {
        let mut inputs = inputs();
        inputs.base_pin = pin("0.163.0", 1, &["0.164.0"]);
        // The head clearing its own skip list changes nothing: base is authoritative.
        inputs.head_pin = pin("0.164.0", 1, &[]);
        assert_eq!(
            refuse(inputs),
            PinBumpRefusal::VersionSkipped {
                version: "0.164.0".to_string()
            }
        );
    }

    #[test]
    fn pin_bump_refuses_a_missing_macos_contract_verdict() {
        let mut inputs = inputs();
        inputs
            .verdicts
            .as_mut()
            .unwrap()
            .retain(|v| !(v.check == "codex-contract" && v.platform == "macos"));
        let refusal = refuse(inputs);
        assert_eq!(
            refusal,
            PinBumpRefusal::VerdictMissing {
                check: "codex-contract".to_string(),
                platform: "macos".to_string(),
                head: HEAD.to_string(),
            }
        );
        let text = refusal.to_string();
        assert!(text.contains("codex-contract") && text.contains("macos") && text.contains(HEAD));
    }

    #[test]
    fn pin_bump_refuses_a_red_linux_contract_verdict() {
        let refusal = refuse(with_verdict("codex-contract", "linux", |v| {
            v.passed = false
        }));
        assert!(
            matches!(&refusal, PinBumpRefusal::VerdictFailed { check, platform, .. }
                if check == "codex-contract" && platform == "linux"),
            "{refusal:?}"
        );
    }

    #[test]
    fn pin_bump_refuses_an_unverified_pin_check_verdict() {
        let refusal = refuse(with_verdict("codex-pin", "linux", |v| v.parsed = None));
        assert_eq!(
            refusal,
            PinBumpRefusal::VerdictUnverified {
                check: "codex-pin".to_string(),
                platform: "linux".to_string(),
                head: HEAD.to_string(),
                reason: "the command produced no structured coverage result".to_string(),
            }
        );
    }

    #[test]
    fn pin_bump_refuses_a_verdict_recorded_only_on_the_parent_commit() {
        let refusal = refuse(with_verdict("codex-pin", "linux", |v| {
            v.commit_sha = PARENT.to_string()
        }));
        assert!(
            matches!(&refusal, PinBumpRefusal::VerdictMissing { check, platform, .. }
                if check == "codex-pin" && platform == "linux"),
            "a stale verdict is no verdict: {refusal:?}"
        );
    }

    #[test]
    fn pin_bump_refuses_a_killed_lane_as_no_evidence() {
        let refusal = refuse(with_verdict("codex-contract", "macos", |v| {
            v.passed = false;
            v.failure_kind = Some("killed".to_string());
        }));
        assert_eq!(
            refusal,
            PinBumpRefusal::VerdictNoEvidence {
                check: "codex-contract".to_string(),
                platform: "macos".to_string(),
                head: HEAD.to_string(),
                failure_kind: "killed".to_string(),
            }
        );
    }

    #[test]
    fn pin_bump_refuses_a_required_check_without_verdict_platforms() {
        let mut inputs = inputs();
        inputs.base_config = Ok(config(&CONFIG.replace(
            "    command: cargo nextest run contract\n    verdictPlatforms: [macos, linux]\n",
            "    command: cargo nextest run contract\n",
        )));
        let refusal = refuse(inputs);
        assert!(
            matches!(&refusal, PinBumpRefusal::VerdictMisconfigured { check, .. } if check == "codex-contract"),
            "{refusal:?}"
        );
        assert!(refusal.to_string().contains("verdictPlatforms"));
    }

    #[test]
    fn pin_bump_refuses_a_head_behind_the_current_base() {
        let mut inputs = inputs();
        inputs.merge_base = Ok(PARENT.to_string());
        let refusal = refuse(inputs);
        assert_eq!(
            refusal,
            PinBumpRefusal::NotOnCurrentBase {
                merge_base: PARENT.to_string(),
                target_tip: TIP.to_string(),
            }
        );
        assert!(refusal.to_string().contains("not on the current base"));
    }

    #[test]
    fn pin_bump_refuses_a_pre_release_head() {
        let mut inputs = inputs();
        inputs.head_pin = pin("0.164.0-alpha.1", 1, &[]);
        assert_eq!(
            refuse(inputs),
            PinBumpRefusal::VersionPrerelease {
                version: "0.164.0-alpha.1".to_string(),
            }
        );
    }

    #[test]
    fn pin_bump_requires_only_the_spec_platforms_however_wide_the_trust_set() {
        // A check that also trusts Windows does not make a bump need a Windows
        // verdict: the spec's platforms decide what must be proven.
        let mut inputs = inputs();
        inputs.base_config = Ok(config(&CONFIG.replace(
            "verdictPlatforms: [macos, linux]",
            "verdictPlatforms: [macos, linux, windows]",
        )));
        let evidence = evaluate(&inputs).expect("linux and macos verdicts prove the bump");
        assert!(evidence
            .verdicts
            .iter()
            .all(|citation| citation.platform != "windows"));
    }

    #[test]
    fn pin_bump_refuses_a_spec_platform_the_check_does_not_trust() {
        let mut inputs = inputs();
        inputs.base_config = Ok(config(&CONFIG.replace(
            "    command: cargo nextest run contract\n    verdictPlatforms: [macos, linux]\n",
            "    command: cargo nextest run contract\n    verdictPlatforms: [linux]\n",
        )));
        let refusal = refuse(inputs);
        assert!(
            matches!(&refusal, PinBumpRefusal::VerdictMisconfigured { check, reason }
                if check == "codex-contract" && reason.contains("does not trust a macos verdict")),
            "{refusal:?}"
        );
    }

    #[test]
    fn pin_bump_missing_verdict_names_the_command_that_records_it() {
        let mut inputs = inputs();
        inputs
            .verdicts
            .as_mut()
            .unwrap()
            .retain(|v| !(v.check == "codex-pin" && v.platform == "macos"));
        let text = refuse(inputs).to_string();
        assert!(
            text.contains("cairn check run codex-pin --os macos"),
            "{text}"
        );
    }

    #[test]
    fn pin_bump_versions_order_like_the_script_twin() {
        let order = |a: &str, b: &str| compare_versions(a, b).unwrap();
        assert_eq!(order("0.164.0", "0.163.9"), Ordering::Greater);
        assert_eq!(order("0.10.0", "0.9.0"), Ordering::Greater);
        assert_eq!(order("1.0", "1.0.0"), Ordering::Equal);
        assert_eq!(order("1.0.0", "1.0.0-rc.1"), Ordering::Greater);
        assert_eq!(order("1.0.0-rc.10", "1.0.0-rc.9"), Ordering::Greater);
        assert_eq!(order("1.0.0-alpha", "1.0.0-beta"), Ordering::Less);
        assert!(compare_versions("1.x", "1.0").is_err());
    }

    async fn verdict_db() -> LocalDb {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.keep().join("pin-bump.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db.execute(
            "INSERT INTO workspaces (id, name, created_at, updated_at)
             VALUES ('workspace', 'Workspace', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project', 'workspace', 'Project', 'project', '/tmp/repo', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db
    }

    struct Observation<'a> {
        id: &'a str,
        commit: &'a str,
        check: &'a str,
        platform: &'a str,
        environment: &'a str,
        passed: bool,
        ran_at: i64,
    }

    async fn record(db: &LocalDb, observation: Observation<'_>) {
        record_with_failure_kind(db, observation, None).await;
    }

    /// Observations are immutable, so a death's kind is written with the row.
    async fn record_with_failure_kind(
        db: &LocalDb,
        observation: Observation<'_>,
        failure_kind: Option<&str>,
    ) {
        let verdict = if observation.passed {
            "passed"
        } else {
            "failed"
        };
        db.execute(
            "INSERT INTO check_result_observations (
                id, project_id, commit_sha, tree_hash, check_name, input_hash,
                environment_fingerprint, exit_code, verdict, complete, reusable,
                parser_version, result_schema_version, ran_at, duration_ms, cadence,
                output_tail, verdict_platform, failure_kind
             ) VALUES (?1, 'project', ?2, 'tree', ?3, 'input', ?4, ?5, ?6, 1, 1,
                       1, 1, ?7, 10, 'review', '', ?8, ?9)",
            params![
                observation.id,
                observation.commit,
                observation.check,
                observation.environment,
                i64::from(!observation.passed),
                verdict,
                observation.ran_at,
                observation.platform,
                failure_kind
            ],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO check_result_cache (
                project_id, tree_hash, input_hash, check_name, environment_fingerprint,
                result_schema_version, source_observation_id, exit_code, passed,
                output_tail, duration_ms, ran_at, target_results_json, verdict_platform
             ) VALUES ('project', 'tree', 'input', ?1, ?2, 1, ?3, ?4, ?5, '', 10, ?6, ?7, ?8)",
            params![
                observation.check,
                observation.environment,
                observation.id,
                i64::from(!observation.passed),
                i64::from(observation.passed),
                observation.ran_at,
                serde_json::to_string(&nextest(4)).unwrap(),
                observation.platform
            ],
        )
        .await
        .unwrap();
    }

    async fn alias(
        db: &LocalDb,
        commit: &str,
        check: &str,
        environment: &str,
        observation: &str,
        disposition: &str,
        evaluated_at: i64,
    ) {
        db.execute(
            "INSERT INTO check_result_commit_aliases (
                project_id, commit_sha, check_name, environment_fingerprint,
                result_schema_version, source_observation_id, tree_hash, input_hash,
                disposition, evaluated_at
             ) VALUES ('project', ?1, ?2, ?3, 1, ?4, 'tree', 'input', ?5, ?6)",
            params![
                commit,
                check,
                environment,
                observation,
                disposition,
                evaluated_at
            ],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn pin_bump_head_verdicts_read_only_the_head_and_let_the_newest_machine_decide() {
        let db = verdict_db().await;
        // Two Linux machines recorded codex-pin at the head; the newer one (red) decides.
        for (id, env, passed, at) in [
            ("obs-old", "env-a", true, 10),
            ("obs-new", "env-b", false, 20),
        ] {
            record(
                &db,
                Observation {
                    id,
                    commit: HEAD,
                    check: "codex-pin",
                    platform: "linux",
                    environment: env,
                    passed,
                    ran_at: at,
                },
            )
            .await;
            alias(&db, HEAD, "codex-pin", env, id, "fresh", at).await;
        }
        // A macOS verdict produced at another commit and reused at the head counts.
        record(
            &db,
            Observation {
                id: "obs-mac",
                commit: "elsewhere",
                check: "codex-pin",
                platform: "macos",
                environment: "env-mac",
                passed: true,
                ran_at: 5,
            },
        )
        .await;
        alias(&db, HEAD, "codex-pin", "env-mac", "obs-mac", "cached", 30).await;
        // codex-contract has a verdict only on the parent: stale, never evidence.
        record(
            &db,
            Observation {
                id: "obs-parent",
                commit: PARENT,
                check: "codex-contract",
                platform: "linux",
                environment: "env-a",
                passed: true,
                ran_at: 40,
            },
        )
        .await;
        alias(
            &db,
            PARENT,
            "codex-contract",
            "env-a",
            "obs-parent",
            "fresh",
            40,
        )
        .await;

        let required: Vec<RequiredVerdict> = ["codex-pin", "codex-contract"]
            .iter()
            .flat_map(|check| {
                ["linux", "macos"].map(|platform| RequiredVerdict {
                    check: check.to_string(),
                    platform: platform.to_string(),
                })
            })
            .collect();
        let verdicts = head_verdicts(&db, "project", HEAD, &required)
            .await
            .unwrap();
        let summary: Vec<_> = verdicts
            .iter()
            .map(|v| {
                (
                    v.check.as_str(),
                    v.platform.as_str(),
                    v.observation_id.as_str(),
                    v.disposition.as_str(),
                    v.passed,
                )
            })
            .collect();
        assert_eq!(
            summary,
            [
                ("codex-pin", "linux", "obs-new", "fresh", false),
                ("codex-pin", "macos", "obs-mac", "cached", true),
            ]
        );
        assert!(verdicts.iter().all(|v| v.commit_sha == HEAD));
        assert_eq!(
            verdict_evidence(verdicts[1].passed, verdicts[1].parsed.as_ref()),
            VerdictEvidence::Verified,
            "the cached verdict carries its structured result"
        );
    }

    /// A newer run on the same platform that died without a verdict does not
    /// stand in for the verdict an earlier run recorded at the head.
    #[tokio::test]
    async fn pin_bump_head_verdicts_keep_a_verdict_over_a_newer_run_without_one() {
        let db = verdict_db().await;
        for (id, env, passed, at, failure_kind) in [
            ("obs-pass", "env-a", true, 10, None),
            ("obs-infra", "env-b", false, 20, Some("infrastructure")),
        ] {
            record_with_failure_kind(
                &db,
                Observation {
                    id,
                    commit: HEAD,
                    check: "codex-pin",
                    platform: "linux",
                    environment: env,
                    passed,
                    ran_at: at,
                },
                failure_kind,
            )
            .await;
            alias(&db, HEAD, "codex-pin", env, id, "fresh", at).await;
        }

        let required = [RequiredVerdict {
            check: "codex-pin".to_string(),
            platform: "linux".to_string(),
        }];
        let verdicts = head_verdicts(&db, "project", HEAD, &required)
            .await
            .unwrap();
        assert_eq!(verdicts.len(), 1);
        assert_eq!(verdicts[0].observation_id, "obs-pass");
        assert!(verdicts[0].passed);
    }
}
