//! The pure half of a publication: which targets exist, which machines could
//! build each, what each leg runs, and what its outcome means for its row.
//! Nothing here touches the fleet, the ledger, or a clock.
use crate::execution::checks::CheckExecutionFailure;
use crate::execution::checks::SubstrateFailureShape;
use crate::fleet::KnownMachinePlatform;
use crate::mcp::handlers::run::CheckExecResult;
use serde_json::Value;

/// The prefix `scripts/publish-leg.ts` writes its result line with.
pub(crate) const LEG_RESULT_MARKER: &str = "CAIRN_LEG_RESULT ";
/// The prefix `scripts/plan-executor-publication.ts` writes its plan line with.
pub(crate) const PLAN_RESULT_MARKER: &str = "CAIRN_PLAN_RESULT ";
/// `publish-leg.ts`'s exit codes for the ledger states other than success
/// and failure (sysexits' EX_TEMPFAIL and EX_NOPERM).
pub(crate) const EXIT_DEFERRED: i32 = 75;
pub(crate) const EXIT_REFUSED: i32 = 77;

/// The row key of a leg that belongs to no one target.
pub(crate) const ALL_TARGETS: &str = "all";

/// The rust toolchain a build leg's machine must advertise.
const RUST: &str = "rust";

/// A published executor target and the machine that builds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TargetSpec {
    pub triple: &'static str,
    /// The OS of the machine that builds it, in the fleet's vocabulary.
    pub os: &'static str,
    /// The architecture of that machine. Both Linux targets build on x86_64:
    /// zig cross-links aarch64 there, as the workflow does today.
    pub builder_arch: &'static str,
}

/// `PUBLISHED_TARGETS` in `scripts/build-executor-manifest.ts`, in its order.
pub(crate) const EXECUTOR_TARGETS: [TargetSpec; 4] = [
    TargetSpec {
        triple: "aarch64-apple-darwin",
        os: "macos",
        builder_arch: "aarch64",
    },
    TargetSpec {
        triple: "aarch64-unknown-linux-gnu",
        os: "linux",
        builder_arch: "x86_64",
    },
    TargetSpec {
        triple: "x86_64-pc-windows-msvc",
        os: "windows",
        builder_arch: "x86_64",
    },
    TargetSpec {
        triple: "x86_64-unknown-linux-gnu",
        os: "linux",
        builder_arch: "x86_64",
    },
];

pub(crate) fn target_spec(triple: &str) -> Option<TargetSpec> {
    EXECUTOR_TARGETS
        .iter()
        .copied()
        .find(|spec| spec.triple == triple)
}

/// Why the Windows build leg never runs on a Cairn builder yet.
pub(crate) const WINDOWS_SIGNING_REFUSAL: &str = "the Windows executor is signed with Azure Trusted Signing through GitHub's OIDC credential (azure/login), which no Cairn builder can present; it stays with GitHub Actions until the Azure credential moves, and a Windows leg never falls back to an unsigned build";

/// The machines that could build one target, and the machines of its OS that
/// cannot and so are steered away from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Builders {
    pub eligible: Vec<String>,
    pub avoid: Vec<String>,
    /// Known machines on the target's OS, as `name (os/arch)`, for the reason
    /// a deferral names.
    pub same_os: Vec<String>,
}

/// Which known machines could build `spec`: its OS and builder architecture,
/// and the rust toolchain once attached (an enrolled machine that has not
/// attached counts, as placement counts it, because what it will advertise is
/// not known until it does).
pub(crate) fn builders_for(spec: TargetSpec, machines: &[KnownMachinePlatform]) -> Builders {
    let mut builders = Builders {
        eligible: Vec::new(),
        avoid: Vec::new(),
        same_os: Vec::new(),
    };
    for machine in machines.iter().filter(|machine| machine.os == spec.os) {
        builders.same_os.push(format!(
            "{} ({}/{})",
            machine.name, machine.os, machine.arch
        ));
        let has_rust = machine
            .toolchains
            .as_ref()
            .is_none_or(|toolchains| toolchains.iter().any(|toolchain| toolchain == RUST));
        if machine.arch == spec.builder_arch && has_rust {
            builders.eligible.push(machine.executor_id.clone());
        } else {
            builders.avoid.push(machine.executor_id.clone());
        }
    }
    builders.avoid.sort();
    builders.avoid.dedup();
    builders.same_os.sort();
    builders
}

/// Tenet 953's wording for a target no builder can take.
pub(crate) fn deferred_no_builder(triple: &str, why: &str) -> String {
    format!("deferred: no eligible builder for {triple}: {why}")
}

/// The deferral a target with no eligible builder gets, naming what was
/// wanted and what the fleet has instead.
pub(crate) fn no_builder_reason(spec: TargetSpec, builders: &Builders) -> String {
    let wanted = format!(
        "no machine the fleet knows of is {}/{} with the {RUST} toolchain",
        spec.os, spec.builder_arch
    );
    let have = if builders.same_os.is_empty() {
        format!("it knows no {} machine at all", spec.os)
    } else {
        format!(
            "its {} machines are {}",
            spec.os,
            builders.same_os.join(", ")
        )
    };
    deferred_no_builder(spec.triple, &format!("{wanted}; {have}"))
}

/// The targets the plan leg is told have a builder (`--eligible-targets`).
pub(crate) fn eligible_targets_csv(machines: &[KnownMachinePlatform]) -> String {
    EXECUTOR_TARGETS
        .iter()
        .filter(|spec| spec.os != "windows")
        .filter(|spec| !builders_for(**spec, machines).eligible.is_empty())
        .map(|spec| spec.triple)
        .collect::<Vec<_>>()
        .join(",")
}

/// The scratch directory a leg writes into on its builder. Ids and triples are
/// validated to opaque segments before they get here, so they need no quoting.
fn out_dir(publication: &str, target: &str) -> String {
    format!("\"${{TMPDIR:-/tmp}}/cairn-publication-{publication}-{target}\"")
}

/// The environment variable a leg's command reads the plan document from: a
/// placed batch carries environment, not files.
pub(crate) const PLAN_ENV: &str = "CAIRN_PUBLICATION_PLAN";

fn with_plan_file(publication: &str, target: &str) -> String {
    let out = out_dir(publication, target);
    format!("out={out}; mkdir -p \"$out\" && printf '%s' \"${PLAN_ENV}\" > \"$out/plan.json\"")
}

/// The plan leg: the pure planner over the live release and this checkout,
/// told which targets placement found a builder for.
pub(crate) fn plan_command(publication: &str, eligible_csv: &str, force: &[String]) -> String {
    let out = out_dir(publication, ALL_TARGETS);
    let force = if force.is_empty() {
        String::new()
    } else {
        format!(" --force-targets={}", force.join(","))
    };
    format!(
        "out={out}; mkdir -p \"$out\" && bun scripts/plan-executor-publication.ts --require-full-history --eligible-targets={eligible_csv}{force} --json-out \"$out/plan.json\""
    )
}

/// The build leg: build (or carry) the target's distribution, then assemble
/// and shape-check its per-target manifest, in one cell so the bytes never
/// leave it.
pub(crate) fn build_command(publication: &str, target: &str, commit: &str) -> String {
    let common =
        format!("--target {target} --publication {publication} --out \"$out\" --commit {commit}");
    format!(
        "{} && bun scripts/publish-leg.ts build {common} --plan \"$out/plan.json\" && bun scripts/publish-leg.ts manifest {common}",
        with_plan_file(publication, target)
    )
}

/// The verify leg: once the live per-target manifest names this commit, the
/// published bytes verify; until then it defers.
pub(crate) fn verify_command(publication: &str, target: &str, commit: &str) -> String {
    format!(
        "{} && bun scripts/publish-leg.ts verify --target {target} --publication {publication} --out \"$out\" --commit {commit} --plan \"$out/plan.json\"",
        with_plan_file(publication, target)
    )
}

/// A tool pin's verify leg (`kind: toolPin`).
pub(crate) fn tool_verify_command(tool: &str) -> String {
    format!("bun run scripts/tool-pin.ts {tool} verify")
}

/// Every line of `output` carrying `marker`, decoded, oldest first.
pub(crate) fn marked_documents(output: &str, marker: &str) -> Vec<Value> {
    output
        .lines()
        .filter_map(|line| line.trim_end().strip_prefix(marker))
        .filter_map(|json| serde_json::from_str(json).ok())
        .collect()
}

/// The planned targets a plan document says publish or defer, with the
/// planner's reason for each.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedTarget {
    pub triple: String,
    pub deferred: bool,
    pub reason: String,
}

/// The targets this publication has work for: those the plan publishes, and
/// those it defers. A target the live release already serves gets no row; the
/// plan document, kept verbatim, says why.
pub(crate) fn planned_targets(plan: &Value) -> Vec<PlannedTarget> {
    plan["plans"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|target| target["publish"] == true || target["deferred"] == true)
        .filter_map(|target| {
            Some(PlannedTarget {
                triple: target["target"].as_str()?.to_string(),
                deferred: target["deferred"] == true,
                reason: target["reason"].as_str().unwrap_or_default().to_string(),
            })
        })
        .collect()
}

/// What a leg's ending means for its row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LegVerdict {
    Succeeded,
    Failed(String),
    Deferred(String),
    Refused(String),
}

impl LegVerdict {
    pub(crate) fn state(&self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed(_) => "failed",
            Self::Deferred(_) => "deferred",
            Self::Refused(_) => "refused",
        }
    }

    pub(crate) fn reason(&self) -> Option<&str> {
        match self {
            Self::Succeeded => None,
            Self::Failed(reason) | Self::Deferred(reason) | Self::Refused(reason) => Some(reason),
        }
    }
}

fn last_line(output: &str) -> String {
    output
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with(LEG_RESULT_MARKER))
        .unwrap_or("no output")
        .chars()
        .take(400)
        .collect()
}

/// The verdict of a leg whose command ran to an end, from its exit code and
/// the result documents it printed (the last one wins).
pub(crate) fn completed_verdict(result: &CheckExecResult, documents: &[Value]) -> LegVerdict {
    let last = documents.last();
    let field = |key: &str| {
        last.and_then(|document| document[key].as_str())
            .map(str::to_string)
    };
    if result.timed_out {
        return LegVerdict::Failed("the leg timed out".into());
    }
    match result.exit_code {
        Some(0) => LegVerdict::Succeeded,
        Some(EXIT_DEFERRED) => LegVerdict::Deferred(
            field("deferred").unwrap_or_else(|| "deferred: the leg is waiting on a fact".into()),
        ),
        Some(EXIT_REFUSED) => LegVerdict::Refused(
            field("refused").unwrap_or_else(|| "the builder declined the leg".into()),
        ),
        code => {
            let code = code.map_or("no exit code (killed)".to_string(), |code| {
                format!("exit {code}")
            });
            let detail = field("error")
                .or_else(|| {
                    last.and_then(|document| document["verifier"]["detail"].as_str())
                        .filter(|_| {
                            last.is_some_and(|document| document["verifier"]["result"] == "fail")
                        })
                        .map(str::to_string)
                })
                .unwrap_or_else(|| last_line(&result.output));
            LegVerdict::Failed(format!("{code}: {detail}"))
        }
    }
}

/// The verdict of a leg that did not run to an end. A fleet with no machine
/// that could ever take it defers the leg (tenet 953: the publication ends with
/// nothing queued); a busy fleet never reaches here, because submission keeps
/// waiting on capacity.
pub(crate) fn failure_verdict(target: &str, failure: &CheckExecutionFailure) -> LegVerdict {
    match failure {
        CheckExecutionFailure::NotPlaceable(why) => {
            LegVerdict::Deferred(deferred_no_builder(target, why))
        }
        CheckExecutionFailure::Substrate(failure)
            if matches!(
                failure.shape(),
                SubstrateFailureShape::NoMachine
                    | SubstrateFailureShape::NoReachableMachine
                    | SubstrateFailureShape::Draining
            ) =>
        {
            LegVerdict::Deferred(deferred_no_builder(target, failure.diagnostic()))
        }
        CheckExecutionFailure::Substrate(failure) => LegVerdict::Failed(format!(
            "the substrate failed ({:?}): {}",
            failure.shape(),
            failure.diagnostic()
        )),
        CheckExecutionFailure::Process(error) => LegVerdict::Failed(error.clone()),
        CheckExecutionFailure::Suppressed(_) => LegVerdict::Failed(
            "submission declined: this leg's retry budget at this commit is spent".into(),
        ),
        CheckExecutionFailure::ConfigUnreadable(why) => {
            LegVerdict::Failed(format!("the project's execution policy does not load: {why}"))
        }
        CheckExecutionFailure::Cancelled(cause) => {
            LegVerdict::Failed(format!("cancelled: {cause:?}"))
        }
        CheckExecutionFailure::LeftRunning(_) => LegVerdict::Failed(
            "the runner stopped waiting while the builder still owned the leg; its outcome is unknown".into(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn machine(
        id: &str,
        os: &str,
        arch: &str,
        toolchains: Option<&[&str]>,
    ) -> KnownMachinePlatform {
        KnownMachinePlatform {
            executor_id: id.into(),
            name: id.into(),
            os: os.into(),
            arch: arch.into(),
            toolchains: toolchains.map(|t| t.iter().map(|s| s.to_string()).collect()),
        }
    }

    fn ran(exit: Option<i32>, output: &str) -> CheckExecResult {
        CheckExecResult {
            exit_code: exit,
            output: output.into(),
            timed_out: false,
            duration_ms: None,
            provenance: None,
            publication: None,
        }
    }

    #[test]
    fn linux_targets_build_on_x86_64_with_rust_and_steer_the_rest_away() {
        let fleet = [
            machine("fedora", "linux", "x86_64", Some(&["rust", "bun"])),
            machine("bginf-sp1", "linux", "aarch64", Some(&["rust"])),
            machine("bare", "linux", "x86_64", Some(&["bun"])),
            machine("bbs", "linux", "x86_64", None),
            machine("mac", "macos", "aarch64", Some(&["rust"])),
        ];
        let arm = builders_for(target_spec("aarch64-unknown-linux-gnu").unwrap(), &fleet);
        assert_eq!(arm.eligible, ["fedora", "bbs"]);
        assert_eq!(arm.avoid, ["bare", "bginf-sp1"]);
        let mac = builders_for(target_spec("aarch64-apple-darwin").unwrap(), &fleet);
        assert_eq!(mac.eligible, ["mac"]);
        assert!(mac.avoid.is_empty());
        assert_eq!(
            eligible_targets_csv(&fleet),
            "aarch64-apple-darwin,aarch64-unknown-linux-gnu,x86_64-unknown-linux-gnu"
        );
    }

    #[test]
    fn a_fleet_without_an_x86_64_linux_builder_defers_both_linux_targets_by_name() {
        let fleet = [machine("bginf-sp1", "linux", "aarch64", Some(&["rust"]))];
        assert_eq!(eligible_targets_csv(&fleet), "");
        let spec = target_spec("x86_64-unknown-linux-gnu").unwrap();
        let reason = no_builder_reason(spec, &builders_for(spec, &fleet));
        assert!(
            reason.starts_with("deferred: no eligible builder for x86_64-unknown-linux-gnu: "),
            "{reason}"
        );
        assert!(
            reason.contains("linux/x86_64") && reason.contains("bginf-sp1 (linux/aarch64)"),
            "{reason}"
        );
    }

    #[test]
    fn the_build_leg_is_release_build_work_and_the_others_are_not() {
        use cairn_common::executor_protocol::CellCommandClass;
        let commit = "a".repeat(40);
        let build = build_command("p1", "x86_64-unknown-linux-gnu", &commit);
        assert_eq!(
            CellCommandClass::classify(&build),
            CellCommandClass::ReleaseBuild
        );
        assert!(
            build.contains("publish-leg.ts manifest"),
            "the manifest is assembled in the build's cell"
        );
        for other in [
            verify_command("p1", "x86_64-unknown-linux-gnu", &commit),
            plan_command("p1", "", &[]),
            tool_verify_command("codex"),
        ] {
            assert_eq!(
                CellCommandClass::classify(&other),
                CellCommandClass::Other,
                "{other}"
            );
        }
        assert!(plan_command("p1", "a,b", &["c".into()])
            .contains("--eligible-targets=a,b --force-targets=c"));
    }

    #[test]
    fn the_plan_document_names_the_targets_with_work() {
        let plan = json!({"plans": [
            {"target": "x86_64-unknown-linux-gnu", "publish": true, "deferred": false, "reason": "build"},
            {"target": "aarch64-unknown-linux-gnu", "publish": false, "deferred": true, "reason": "no online runner"},
            {"target": "aarch64-apple-darwin", "publish": false, "deferred": false, "reason": "live"}
        ]});
        let output = format!("noise\n{PLAN_RESULT_MARKER}{plan}\n");
        let documents = marked_documents(&output, PLAN_RESULT_MARKER);
        assert_eq!(documents, std::slice::from_ref(&plan));
        let targets = planned_targets(&documents[0]);
        assert_eq!(
            targets
                .iter()
                .map(|t| (t.triple.as_str(), t.deferred))
                .collect::<Vec<_>>(),
            [
                ("x86_64-unknown-linux-gnu", false),
                ("aarch64-unknown-linux-gnu", true)
            ]
        );
    }

    #[test]
    fn exit_codes_and_result_documents_decide_the_row() {
        let deferred =
            json!({"deferred": "live per-target manifest for v171 does not name abc yet"});
        assert_eq!(
            completed_verdict(&ran(Some(75), ""), &[deferred]),
            LegVerdict::Deferred("live per-target manifest for v171 does not name abc yet".into())
        );
        assert_eq!(
            completed_verdict(&ran(Some(0), ""), &[]),
            LegVerdict::Succeeded
        );
        assert_eq!(
            completed_verdict(&ran(Some(77), ""), &[json!({"refused": "wrong host"})]),
            LegVerdict::Refused("wrong host".into())
        );
        let shape_fail = json!({"verifier": {"check": "manifest-shape", "result": "fail", "detail": "key x differs"}});
        assert_eq!(
            completed_verdict(&ran(Some(1), ""), &[shape_fail]),
            LegVerdict::Failed("exit 1: key x differs".into())
        );
        assert_eq!(
            completed_verdict(&ran(Some(101), "compiling\nerror: linker failed\n"), &[]),
            LegVerdict::Failed("exit 101: error: linker failed".into())
        );
    }

    #[test]
    fn a_fleet_that_can_never_place_the_leg_defers_it_and_other_failures_fail_it() {
        let not_placeable =
            CheckExecutionFailure::NotPlaceable("no machine matches os linux".into());
        assert_eq!(
            failure_verdict("x86_64-unknown-linux-gnu", &not_placeable),
            LegVerdict::Deferred("deferred: no eligible builder for x86_64-unknown-linux-gnu: no machine matches os linux".into())
        );
        let none =
            CheckExecutionFailure::substrate(SubstrateFailureShape::NoMachine, "nothing matches");
        assert_eq!(failure_verdict("t", &none).state(), "deferred");
        let lost = CheckExecutionFailure::substrate(
            SubstrateFailureShape::MachineUnreachable,
            "link lost",
        );
        assert_eq!(failure_verdict("t", &lost).state(), "failed");
    }
}
