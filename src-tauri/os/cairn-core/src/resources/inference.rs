//! Renderers for the cairn-inference facet's resources, `cairn://inference/*`.
//!
//! Each page renders an [`InferenceView`]: the stored inventory, the bginf
//! link row, the link's configuration and the enrolled executors. The pools
//! page computes the comparison against bginf from the last successful
//! `/state` the poller stored, which is the same snapshot the stored records
//! were imported from, and never hides or corrects a disagreement. A pool
//! under Cairn's control also shows the pool controller's state, and the
//! requests page lists the controller's queue, each computed by the same plan
//! the next tick writes.

use std::fmt::Write as _;

use cairn_common::query::QueryParam;
use cairn_inference::bginf::compare::{self as bginf_compare, CairnSide};
use cairn_inference::bginf::{MachineMap, PoolComparison, PoolState, State, Verdict};
use cairn_inference::pool_control::{self, Phase, RequestState};
use cairn_inference::{
    DeploymentSet, Engine, MachineName, MemoryPerMachine, Pool, Recipe, Requirements,
};

use super::common::find_query_value;
use crate::orchestrator::inference::{machine_map, InferenceView, LinkConfig, LinkRow};
use crate::orchestrator::inference_control::{holds_of, LoadRow};
use crate::orchestrator::pool_control::{effective_settings, ANSWERED_KEPT_MS};
use cairn_inference::control::{gib, Phase as DeploymentPhase};
use cairn_inference::{Deployment, DeploymentId};

const INDEX: &[(&str, &str)] = &[
    (
        "cairn://inference/recipes",
        "how each model is run: weights, engine, served name, and what the hardware must provide",
    ),
    (
        "cairn://inference/deployments",
        "recipes placed on named machines, with the API base and health URL derived from the head's address",
    ),
    (
        "cairn://inference/pools",
        "each pool's machines, pin and default, Cairn's resolution beside bginf's with a verdict, and the pool controller's state; patch it to take a pool under Cairn's control",
    ),
    (
        "cairn://inference/requests",
        "the pool controller's queue: append a model to ask for it, and read what each request waits on",
    ),
];

fn when_ms(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|at| at.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| ms.to_string())
}

fn age(seconds: i64) -> String {
    let seconds = seconds.max(0);
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3_600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h", seconds / 3_600)
    } else {
        format!("{}d", seconds / 86_400)
    }
}

fn age_since(view: &InferenceView, ms: i64) -> String {
    age((view.now_ms - ms) / 1_000)
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn set_label(set: &DeploymentSet) -> String {
    if set.is_empty() {
        "nothing (bginf `off`)".to_string()
    } else {
        set.iter()
            .map(|id| format!("`{id}`"))
            .collect::<Vec<_>>()
            .join(" + ")
    }
}

fn map_of(view: &InferenceView) -> MachineMap {
    match &view.config {
        LinkConfig::Configured(link) => machine_map(link),
        LinkConfig::Missing { .. } => MachineMap::default(),
    }
}

fn enrolment(view: &InferenceView, machine: &MachineName) -> &'static str {
    match view.executors.get(machine.as_str()) {
        Some(true) => "enrolled executor (attached)",
        Some(false) => "enrolled executor (not attached)",
        None => "not enrolled",
    }
}

/// The link's configuration and its last read, one fact per line.
fn link_status(view: &InferenceView) -> String {
    let mut out = String::new();
    match &view.config {
        LinkConfig::Configured(link) => {
            let _ = writeln!(
                out,
                "- bginf link: {} (read-only: GET /profiles and GET /state)",
                link.url
            );
        }
        LinkConfig::Missing { setting, account } => {
            let mut missing = Vec::new();
            if *setting {
                missing.push("the `inference.bginf` section of cairn://settings");
            }
            if *account {
                missing.push("a `bginf` account holding ctl's token (cairn://settings `accounts.add` with provider `bginf`)");
            }
            let _ = writeln!(
                out,
                "- bginf link: not configured; missing {}",
                missing.join(" and ")
            );
        }
    }
    match &view.link {
        None => out.push_str("- bginf has not been read yet\n"),
        Some(row) => {
            if let Some(error) = &row.last_error {
                let since = row
                    .failing_since
                    .or(row.last_attempt_at)
                    .unwrap_or(view.now_ms);
                let _ = writeln!(
                    out,
                    "- bginf unreachable since {} ({} ago): {error}",
                    when_ms(since),
                    age_since(view, since)
                );
            }
            match row.last_ok_at {
                Some(ok) if row.last_error.is_some() => {
                    let _ = writeln!(
                        out,
                        "- showing the last successful read, from {} ({} old)",
                        when_ms(ok),
                        age_since(view, ok)
                    );
                }
                Some(ok) => {
                    let _ = writeln!(
                        out,
                        "- bginf last read {} ({} ago)",
                        when_ms(ok),
                        age_since(view, ok)
                    );
                }
                None => out.push_str("- no successful read yet\n"),
            }
        }
    }
    out
}

fn reject_params(uri: &str, params: &[QueryParam], allowed: &[&str]) -> Option<String> {
    let unknown = params.iter().find(|p| !allowed.contains(&p.key.as_str()))?;
    Some(if allowed.is_empty() {
        format!("{uri} takes no query parameters (got '{}').", unknown.key)
    } else {
        format!(
            "Unsupported query parameter '{}' for {uri}; it reads {}.",
            unknown.key,
            allowed.join(", ")
        )
    })
}

/// `cairn://inference`: the link's status, the collections, and, under
/// `?format=yaml`, the inventory in Cairn's own record format.
pub(crate) fn render_index(view: &InferenceView, params: &[QueryParam]) -> String {
    const URI: &str = "cairn://inference";
    if let Some(error) = reject_params(URI, params, &["format"]) {
        return error;
    }
    match find_query_value(params, "format") {
        None => {}
        Some("yaml") => return view.inventory.to_yaml(),
        Some(other) => return format!("Unknown format='{other}' for {URI}; expected yaml."),
    }
    let mut out = String::from(
        "# cairn-inference\n\nWhich model each inference machine should run. Recipes say how a model is run, deployments place a recipe on named machines, and each pool resolves its pin over its default against the machines that are powered. Every record mirrors bginf and is read-only. A pool taken under Cairn's control is decided instead by the pool controller: a request loads its model, batches waiting for the pool unload an idle one, and an idle pool loads its default. The controller writes desired state; one deployment's page (`cairn://inference/deployments/<id>`) is the act side, starting or stopping it on its head and holding every machine of the gang away from batch work while it does. Cairn powers nothing.\n\n",
    );
    out.push_str(&link_status(view));
    if let Some(row) = &view.link {
        if let (Some(at), Some(report)) = (row.last_change_at, &row.last_change_report) {
            let _ = writeln!(
                out,
                "- last change {} ({} ago):",
                when_ms(at),
                age_since(view, at)
            );
            for line in report
                .lines()
                .iter()
                .filter(|l| !l.starts_with("warning: "))
            {
                let _ = writeln!(out, "  - {line}");
            }
        }
        if !row.last_warnings.is_empty() {
            out.push_str("- the last import refused or could not map:\n");
            for warning in &row.last_warnings {
                let _ = writeln!(out, "  - {warning}");
            }
        }
    }
    let _ = writeln!(
        out,
        "- records: {} recipes, {} deployments, {} pools\n",
        view.inventory.recipes.len(),
        view.inventory.deployments.len(),
        view.inventory.pools.len()
    );
    for (uri, what) in INDEX {
        let _ = writeln!(out, "- `{uri}` — {what}");
    }
    out
}

fn requirements_lines(out: &mut String, requirements: &Requirements) {
    out.push_str("- requirements:\n");
    let _ = writeln!(out, "  - machines: {}", requirements.machines);
    let accelerator = requirements.accelerator.map_or("unknown".to_string(), |a| {
        format!("{:?} on {:?}", a.family, a.runtime).to_lowercase()
    });
    let _ = writeln!(out, "  - accelerator: {accelerator}");
    let interconnect = requirements
        .interconnect
        .map_or("unknown".to_string(), |i| format!("{i:?}").to_lowercase());
    let _ = writeln!(out, "  - interconnect: {interconnect}");
    let gib = |bytes: u64| format!("{:.1} GiB", bytes as f64 / (1u64 << 30) as f64);
    let memory = match requirements.memory {
        None => "unknown".to_string(),
        Some(MemoryPerMachine::Unified { system }) => {
            format!("unified, {} of system RAM", gib(system.0))
        }
        Some(MemoryPerMachine::Discrete { vram, host }) => {
            format!(
                "discrete, {} of VRAM and {} of host RAM",
                gib(vram.0),
                gib(host.0)
            )
        }
    };
    let _ = writeln!(out, "  - memory per machine: {memory}");
}

fn source_label(source: cairn_inference::RecordSource) -> &'static str {
    match source {
        cairn_inference::RecordSource::Bginf => "bginf (mirrored, read-only)",
        cairn_inference::RecordSource::Cairn => "cairn",
    }
}

fn fenced(out: &mut String, label: &str, text: &str) {
    let _ = writeln!(out, "  - {label}:\n\n    ```sh");
    for line in text.lines() {
        let _ = writeln!(out, "    {line}");
    }
    out.push_str("    ```\n\n");
}

fn recipe_section(out: &mut String, recipe: &Recipe) {
    let _ = writeln!(out, "## {}\n", recipe.id);
    let _ = writeln!(out, "- source: {}", source_label(recipe.source));
    let _ = writeln!(out, "- model: {}", recipe.model.name);
    if recipe.model.weights.is_empty() {
        out.push_str("- weights: none pinned\n");
    } else {
        out.push_str("- weights:\n");
        for weight in &recipe.model.weights {
            let _ = writeln!(
                out,
                "  - {}@{} {} (sha256 {})",
                weight.repo, weight.revision, weight.path, weight.sha256
            );
        }
    }
    let _ = writeln!(out, "- served name: {}", recipe.served_name);
    let _ = writeln!(
        out,
        "- upstream id: {}",
        recipe.upstream_id.as_deref().unwrap_or("unknown")
    );
    let verbs = match (&recipe.verbs, recipe.source) {
        (Some(verbs), _) => verbs
            .iter()
            .map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        (None, cairn_inference::RecordSource::Bginf) => "unknown (imported from bginf)".to_string(),
        (None, _) => "unknown".to_string(),
    };
    let _ = writeln!(out, "- verbs: {verbs}");
    let _ = writeln!(out, "- engine: {}", recipe.engine.kind());
    match &recipe.engine {
        Engine::Container { image, args } => {
            let _ = writeln!(out, "  - image: {image}");
            if !args.is_empty() {
                let _ = writeln!(out, "  - args: {}", args.join(" "));
            }
        }
        Engine::Native {
            artifact,
            version,
            sha256,
            args,
        } => {
            let _ = writeln!(out, "  - artifact: {artifact} {version} (sha256 {sha256})");
            if !args.is_empty() {
                let _ = writeln!(out, "  - args: {}", args.join(" "));
            }
        }
        Engine::Command {
            start,
            stop,
            description,
        } => {
            if let Some(description) = description {
                let _ = writeln!(out, "  - description: {description}");
            }
            fenced(out, "start", start);
            match stop {
                Some(stop) => fenced(out, "stop", stop),
                None => out.push_str("  - stop: none\n"),
            }
        }
    }
    requirements_lines(out, &recipe.requirements);
    if !recipe.aliases.is_empty() {
        let _ = writeln!(out, "- aliases: {}", recipe.aliases.join(", "));
    }
    if let Some(notes) = &recipe.notes {
        let _ = writeln!(out, "- notes: {notes}");
    }
    out.push('\n');
}

pub(crate) fn render_recipes(view: &InferenceView, params: &[QueryParam]) -> String {
    if let Some(error) = reject_params("cairn://inference/recipes", params, &[]) {
        return error;
    }
    let mut out = String::from("# Inference recipes\n\n");
    if view.inventory.recipes.is_empty() {
        out.push_str("No recipes yet. Imported recipes appear once bginf has been read; see cairn://inference.\n");
        return out;
    }
    for recipe in view.inventory.recipes.values() {
        recipe_section(&mut out, recipe);
    }
    out
}

pub(crate) fn render_deployments(view: &InferenceView, params: &[QueryParam]) -> String {
    if let Some(error) = reject_params("cairn://inference/deployments", params, &[]) {
        return error;
    }
    let inventory = &view.inventory;
    let mut out = String::from("# Inference deployments\n\n");
    if inventory.deployments.is_empty() {
        out.push_str("No deployments yet. Imported deployments appear once bginf has been read; see cairn://inference.\n");
        return out;
    }
    for deployment in inventory.deployments.values() {
        let _ = writeln!(out, "## {}\n", deployment.id);
        let _ = writeln!(out, "- source: {}", source_label(deployment.source));
        let _ = writeln!(out, "- recipe: {}", deployment.recipe);
        let pool = inventory
            .pool_of_deployment(deployment)
            .map_or("none".to_string(), |p| p.id.to_string());
        let _ = writeln!(out, "- pool: {pool}");
        out.push_str("- machines:\n");
        for machine in &deployment.machines {
            let head = if *machine == deployment.head {
                "head; "
            } else {
                ""
            };
            let _ = writeln!(out, "  - `{machine}` ({head}{})", enrolment(view, machine));
        }
        match inventory.address_of(&deployment.head) {
            Some(address) => {
                let _ = writeln!(out, "- API base: {}", deployment.api_base(address));
                let _ = writeln!(out, "- health: {}", deployment.health_url(address));
            }
            None => {
                let _ = writeln!(
                    out,
                    "- API base and health: head address unknown (port {}, API path {}, health path {})",
                    deployment.api_port, deployment.api_path, deployment.health_path
                );
            }
        }
        let _ = writeln!(out, "- phase: {}", phase_summary(view, &deployment.id));
        let _ = writeln!(
            out,
            "- read or act on it: cairn://inference/deployments/{}",
            deployment.id
        );
        out.push('\n');
    }
    out
}

/// A duration in milliseconds, to the second.
fn duration(ms: u64) -> String {
    let s = ms / 1_000;
    if s < 60 {
        format!("{s}s")
    } else if s < 3_600 {
        format!("{}m {}s", s / 60, s % 60)
    } else {
        format!("{}h {}m", s / 3_600, (s % 3_600) / 60)
    }
}

fn at(view: &InferenceView, ms: i64) -> String {
    format!("{} ({} ago)", when_ms(ms), age_since(view, ms))
}

/// One deployment's phase as Cairn holds it, in one sentence.
fn phase_summary(view: &InferenceView, id: &DeploymentId) -> String {
    let Some(row) = view.deployment_control.states.get(id) else {
        return "never acted on by Cairn, so it is neither probed nor reserved (bginf may be running it)".into();
    };
    match &row.phase {
        DeploymentPhase::Stopped { since_ms } => format!("stopped since {}", at(view, *since_ms)),
        DeploymentPhase::Reserving {
            since_ms,
            waiting_on,
        } => format!(
            "reserving since {}: its machines are held and the start is not sent yet{}",
            at(view, *since_ms),
            waiting_on
                .as_deref()
                .map(|on| format!("; waiting on {on}"))
                .unwrap_or_default()
        ),
        DeploymentPhase::Starting { since_ms, .. } => format!(
            "starting since {}: the head is running the start command",
            at(view, *since_ms)
        ),
        DeploymentPhase::Healthy { since_ms, load_ms } => format!(
            "healthy since {}; {}",
            at(view, *since_ms),
            match load_ms {
                Some(ms) => format!("the load took {}", duration(*ms)),
                None => "found already answering, so no load was measured".into(),
            }
        ),
        DeploymentPhase::Stopping { since_ms } => format!(
            "stopping since {}: the head is running the stop command",
            at(view, *since_ms)
        ),
        DeploymentPhase::Failed { at_ms, reason } => {
            format!("failed {}: {reason}", at(view, *at_ms))
        }
    }
}

fn footprint_line(footprint: &std::collections::BTreeMap<String, u64>) -> String {
    footprint
        .iter()
        .map(|(machine, bytes)| format!("{machine} {}", gib(*bytes)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn load_line(view: &InferenceView, load: &LoadRow) -> String {
    let waited = load
        .sent_at
        .map(|sent| {
            format!(
                "; waited {} for its machines",
                duration((sent - load.requested_at).max(0) as u64)
            )
        })
        .unwrap_or_default();
    match (load.healthy, load.load_ms) {
        (true, Some(ms)) => format!(
            "- {}: healthy after {}{waited}; settled footprint {}",
            at(view, load.finished_at),
            duration(ms),
            load.footprint
                .as_ref()
                .map_or("not recorded yet".to_string(), footprint_line)
        ),
        _ => format!(
            "- {}: failed{waited}: {}",
            at(view, load.finished_at),
            load.reason.as_deref().unwrap_or("no reason recorded")
        ),
    }
}

fn machine_line(
    view: &InferenceView,
    deployment: &Deployment,
    held: &std::collections::HashMap<String, cairn_common::executor_protocol::InferenceHold>,
    machine: &cairn_inference::MachineName,
) -> String {
    let head = if *machine == deployment.head {
        "head; "
    } else {
        ""
    };
    let hold = match held.get(machine.as_str()) {
        Some(hold) if hold.deployment == deployment.id.as_str() => {
            "held away from batch work by this deployment".to_string()
        }
        Some(hold) => format!("held by deployment {} ({})", hold.deployment, hold.phase),
        None => "not held".to_string(),
    };
    let memory = match view.deployment_control.readings.get(machine.as_str()) {
        None => "no GPU reading (not attached)".to_string(),
        Some(reading) => match reading.gpu_allocated_bytes {
            Some(bytes) => format!("GPU-allocated now {}", gib(bytes)),
            None => "GPU memory unmeasured".to_string(),
        },
    };
    format!(
        "  - `{machine}` ({head}{}); {hold}; {memory}",
        enrolment(view, machine)
    )
}

/// `cairn://inference/deployments/<id>`: one deployment and what Cairn has
/// done to it.
pub(crate) fn render_deployment(view: &InferenceView, id: &str, params: &[QueryParam]) -> String {
    let uri = format!("cairn://inference/deployments/{id}");
    if let Some(error) = reject_params(&uri, params, &[]) {
        return error;
    }
    let id = DeploymentId::new(id);
    let Some(deployment) = view.inventory.deployments.get(&id) else {
        return format!(
            "No deployment `{id}`. cairn://inference/deployments lists every deployment.\n"
        );
    };
    let mut out = format!("# Inference deployment {id}\n\n");
    let _ = writeln!(out, "- source: {}", source_label(deployment.source));
    let _ = writeln!(out, "- recipe: {}", deployment.recipe);
    let pool = view
        .inventory
        .pool_of_deployment(deployment)
        .map_or("none".to_string(), |p| p.id.to_string());
    let _ = writeln!(out, "- pool: {pool}");
    let _ = writeln!(out, "- phase: {}", phase_summary(view, &id));
    let held = holds_of(&view.inventory, &view.deployment_control.states);
    out.push_str("- machines:\n");
    for machine in &deployment.machines {
        let _ = writeln!(out, "{}", machine_line(view, deployment, &held, machine));
    }
    match view.inventory.address_of(&deployment.head) {
        Some(address) => {
            let _ = writeln!(out, "- API base: {}", deployment.api_base(address));
            let _ = writeln!(out, "- health: {}", deployment.health_url(address));
        }
        None => out.push_str(
            "- health: the head's address is unknown, so Cairn cannot start or probe it\n",
        ),
    }
    let loads = view.deployment_control.loads.get(&id);
    match loads.and_then(|loads| {
        loads
            .iter()
            .find(|load| load.healthy && load.load_ms.is_some())
    }) {
        Some(load) => {
            let _ = writeln!(
                out,
                "- last measured load: {} to health, finished {}",
                duration(load.load_ms.unwrap_or_default()),
                at(view, load.finished_at)
            );
        }
        None => out.push_str("- last measured load: none yet\n"),
    }
    let footprint = crate::orchestrator::inference_control::last_footprint(loads);
    if footprint.is_empty() {
        out.push_str("- measured footprint: none yet, so a start checks no memory until one load has settled\n");
    } else {
        let _ = writeln!(
            out,
            "- measured footprint (GPU-allocated once settled): {}; a start waits until each machine has this much free",
            footprint_line(&footprint)
        );
    }
    if let Some(loads) = loads.filter(|loads| !loads.is_empty()) {
        out.push_str("\n## Recent loads\n\n");
        for load in loads.iter().take(10) {
            let _ = writeln!(out, "{}", load_line(view, load));
        }
    }
    let _ = write!(
        out,
        "\n## Acting on it\n\n- start: `write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"start\"}}}}]}})` reserves every machine above at once, then runs the recipe's start command on the head once each has room.\n- stop: the same with `action:\"stop\"` runs the recipe's stop command on the head and waits for the health check to stop answering.\n- While Cairn controls this deployment, bginf must not: pin bginf's pool `{pool}` to `off` or stop its ctl, or ctl's reconcile loop will start or stop it behind Cairn's back.\n"
    );
    out
}

fn pool_records(
    out: &mut String,
    view: &InferenceView,
    pool: &Pool,
    state: Option<&State>,
    map: &MachineMap,
) {
    out.push_str("- machines:\n");
    for machine in &pool.machines {
        let address = machine.address.as_deref().unwrap_or("address unknown");
        let power = match state {
            None => "power unknown".to_string(),
            Some(state) => match state.power.get(&map.bginf(&machine.name)) {
                None => "no outlet bginf manages, so always powered".to_string(),
                Some(power) => format!(
                    "bginf wants power {}{}",
                    power.desired,
                    power
                        .phase
                        .as_deref()
                        .map(|phase| format!(", phase {phase}"))
                        .unwrap_or_default()
                ),
            },
        };
        let _ = writeln!(
            out,
            "  - `{}`: {}; {address}; {power}",
            machine.name,
            enrolment(view, &machine.name)
        );
    }
    let pin = pool.pin.as_ref().map_or("none".to_string(), set_label);
    let _ = writeln!(out, "- pin (mirrored): {pin}");
    let _ = writeln!(out, "- default (mirrored): {}", set_label(&pool.default));
}

fn cairn_line(out: &mut String, cairn: &CairnSide) {
    let _ = writeln!(
        out,
        "- Cairn: desired {}; because {}; converged {}",
        set_label(&cairn.resolution.desired),
        cairn.resolution.reason,
        yes_no(cairn.converged)
    );
}

fn bginf_line(out: &mut String, bginf: &PoolState) {
    let running = bginf
        .running
        .as_ref()
        .map_or("nothing".to_string(), ToString::to_string);
    let _ = writeln!(
        out,
        "- bginf: desired `{}`; reason {:?}; running {running}; converged {}",
        bginf.desired,
        bginf.reason,
        yes_no(bginf.converged)
    );
}

fn verdict_line(verdict: &Verdict) -> String {
    match verdict {
        Verdict::Agree => "agree".to_string(),
        Verdict::Disagree(differences) => format!(
            "DISAGREE: {}",
            differences
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ")
        ),
    }
}

fn comparison_section(
    out: &mut String,
    view: &InferenceView,
    comparison: &PoolComparison,
    state: &State,
    map: &MachineMap,
) {
    let _ = writeln!(out, "## {}\n", comparison.pool);
    let _ = writeln!(out, "- verdict: {}", verdict_line(&comparison.verdict));
    let pool = view.inventory.pools.get(&comparison.pool);
    if let Some(pool) = pool {
        pool_records(out, view, pool, Some(state), map);
    }
    match &comparison.cairn {
        Some(cairn) => cairn_line(out, cairn),
        None => out.push_str("- Cairn: no record of this pool\n"),
    }
    match &comparison.bginf {
        Some(bginf) => bginf_line(out, bginf),
        None => out.push_str("- bginf: does not report this pool\n"),
    }
    if let Some(pool) = pool {
        control_lines(out, view, pool);
    }
    out.push('\n');
}

fn control_set(set: &DeploymentSet) -> String {
    if set.is_empty() {
        "nothing".to_string()
    } else {
        set.iter()
            .map(|id| format!("`{id}`"))
            .collect::<Vec<_>>()
            .join(" + ")
    }
}

fn phase_label(phase: Option<&Phase>) -> String {
    match phase {
        None | Some(Phase::Stopped) => "stopped".to_string(),
        Some(Phase::Starting) => "starting".to_string(),
        Some(Phase::Healthy) => "healthy".to_string(),
        Some(Phase::Stopping) => "stopping".to_string(),
        Some(Phase::Failed { reason, .. }) => format!("failed: {reason}"),
    }
}

fn plural(count: u32, one: &str, many: &str) -> String {
    if count == 1 {
        format!("1 {one}")
    } else {
        format!("{count} {many}")
    }
}

/// The pool controller's state for `pool`, or how to take control of it.
fn control_lines(out: &mut String, view: &InferenceView, pool: &Pool) {
    let control_view = &view.control;
    let Some(control) = control_view.controls.get(&pool.id) else {
        let _ = writeln!(
            out,
            "- Cairn's control: not taken, so bginf decides this pool; patch this resource with {{pool:\"{}\", controlled:true}} to take it",
            pool.id
        );
        return;
    };
    let settings = effective_settings(control);
    let settings_line = format!(
        "- settings: model idle window {}; pool idle window {}; minimum residency {}; load bound {}",
        settings.model_idle, settings.pool_idle, settings.min_residency, settings.load_bound
    );
    let Some(planned) = control_view.plans.get(&pool.id) else {
        out.push_str(
            "- Cairn's control: handed back, so bginf decides this pool; its settings are kept\n",
        );
        let _ = writeln!(out, "{settings_line}");
        return;
    };
    let now = control_view.now;
    let plan = &planned.plan;
    out.push_str("- Cairn's control: the pool controller decides this pool\n");
    let _ = writeln!(
        out,
        "- desired (written as desired state): {}; slot {}; {}",
        control_set(&plan.resolution.desired),
        plan.resolution.slot.as_str(),
        plan.resolution.reason.describe(now)
    );
    if pool.pin.as_ref().is_none_or(|pin| !pin.is_empty()) {
        let _ = writeln!(
            out,
            "- warning: bginf still drives this pool (its pin is {}, not off); pin bginf to nothing for this pool or stop it, or bginf and Cairn will fight over it",
            pool.pin.as_ref().map_or("unset".to_string(), set_label)
        );
    }
    match &plan.control.pin {
        None => out.push_str("- Cairn pin: none\n"),
        Some(pin) => {
            let _ = writeln!(
                out,
                "- Cairn pin: {} held by {} since {} ({} ago)",
                control_set(&pin.set),
                pin.holder,
                when_ms(pin.at),
                age_since(view, pin.at)
            );
        }
    }
    if plan.control.active.members.is_empty() {
        out.push_str("- controller's set: nothing\n");
    } else {
        out.push_str("- controller's set:\n");
        for (id, member) in &plan.control.active.members {
            let since = match (member.healthy_since, member.loading_since) {
                (Some(at), _) => format!("; healthy for {}", age((now - at) / 1_000)),
                (None, Some(at)) => format!("; loading for {}", age((now - at) / 1_000)),
                (None, None) => format!("; chosen {} ago", age((now - member.chosen_at) / 1_000)),
            };
            let _ = writeln!(
                out,
                "  - `{id}`: {}{since}",
                phase_label(control_view.phases.get(id))
            );
        }
    }
    let builds = planned.builds;
    let _ = writeln!(
        out,
        "- batch work: {} and {} running on the pool's machines; {} waiting that they refused; last batch work {} ago",
        plural(builds.running_turns, "agent turn", "agent turns"),
        plural(builds.running_other, "other batch", "other batches"),
        plural(builds.waiting, "batch", "batches"),
        age((now - plan.control.last_batch_work_at) / 1_000)
    );
    let _ = writeln!(out, "{settings_line}");
    let _ = writeln!(
        out,
        "- requests waiting: {} (cairn://inference/requests)",
        plan.waits.len()
    );
}

pub(crate) fn render_pools(view: &InferenceView, params: &[QueryParam]) -> String {
    if let Some(error) = reject_params("cairn://inference/pools", params, &[]) {
        return error;
    }
    let mut out = String::from("# Inference pools\n\n");
    out.push_str(&link_status(view));
    out.push('\n');
    let map = map_of(view);
    let state = view.link.as_ref().and_then(LinkRow::state);
    match &state {
        Some(state) => {
            for comparison in bginf_compare::compare(&view.inventory, state, &map) {
                comparison_section(&mut out, view, &comparison, state, &map);
            }
        }
        None if view.inventory.pools.is_empty() => {
            out.push_str("No pools yet. They appear once bginf has been read.\n");
        }
        None => {
            for pool in view.inventory.pools.values() {
                let _ = writeln!(out, "## {}\n", pool.id);
                out.push_str("- verdict: no bginf read to compare against\n");
                pool_records(&mut out, view, pool, None, &map);
                control_lines(&mut out, view, pool);
                out.push('\n');
            }
        }
    }
    out
}

/// `cairn://inference/requests`: every waiting request with what it waits on,
/// then the last week's answered ones, newest first.
pub(crate) fn render_requests(view: &InferenceView, params: &[QueryParam]) -> String {
    if let Some(error) = reject_params("cairn://inference/requests", params, &[]) {
        return error;
    }
    let control = &view.control;
    let mut out = String::from(
        "# Inference requests\n\nAppend {model} here to ask for a model in a pool under Cairn's control. A request names exactly one deployment and waits until it is healthy, fails to load, or is cancelled; it is never served by another model.\n\n",
    );
    let (waiting, answered): (Vec<_>, Vec<_>) = control
        .requests
        .iter()
        .partition(|r| r.request.state == RequestState::Waiting);
    let _ = writeln!(out, "## Waiting ({})\n", waiting.len());
    if waiting.is_empty() {
        out.push_str("Nothing is waiting.\n");
    }
    for stored in &waiting {
        let line = match control.wait_of(stored) {
            Some(wait) => {
                pool_control::describe_waiting(&stored.request, &stored.pool, wait, control.now)
            }
            None => format!(
                "{} on pool {} for {}: the pool is not under Cairn's control, so nothing decides it",
                stored.request.model, stored.pool, stored.request.requester
            ),
        };
        let _ = writeln!(out, "- #{} {line}", stored.request.id);
    }
    let _ = writeln!(
        out,
        "\n## Answered in the last {} ({})\n",
        age(ANSWERED_KEPT_MS / 1_000),
        answered.len()
    );
    if answered.is_empty() {
        out.push_str("None.\n");
    }
    for stored in answered.iter().rev() {
        let request = &stored.request;
        let outcome = match &request.state {
            RequestState::Ready { at } => format!("ready {} ago", age_since(view, *at)),
            RequestState::Failed { at, reason } => {
                format!("failed {} ago: {reason}", age_since(view, *at))
            }
            RequestState::Cancelled { at } => format!("cancelled {} ago", age_since(view, *at)),
            RequestState::Waiting => continue,
        };
        let _ = writeln!(
            out,
            "- #{} {} on pool {} for {}: {outcome}",
            request.id, request.model, stored.pool, request.requester
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_config::inference::BginfLinkSettings;
    use cairn_inference::bginf::{import, Profiles, Topology};
    use std::collections::BTreeMap;

    const NOW: i64 = 1_800_000_000_000;

    fn profiles() -> Profiles {
        serde_json::from_value(serde_json::json!({
            "dsv4f-tp2": {"id": "dsv4f-tp2", "nodes": ["sp1", "sp2"], "head": "sp1",
                "start": "./start.sh --port 8888", "stop": "./stop.sh", "engine": "vllm",
                "health": "http://192.0.2.40:8888/v1/models", "api_base": "http://192.0.2.40:8888/v1"},
            "qwen36-fast": {"id": "qwen36-fast", "nodes": ["sp1"], "head": "sp1",
                "start": "./start.sh --port 8001", "stop": "./stop.sh",
                "health": "http://192.0.2.40:8001/v1/models", "api_base": "http://192.0.2.40:8001/v1"},
            "qwen3-embed-4b": {"id": "qwen3-embed-4b", "nodes": ["halo"], "head": "halo",
                "start": "./start.sh --port 8090", "stop": "./stop.sh",
                "health": "http://192.0.2.43:8090/health", "api_base": "http://192.0.2.43:8090/v1"},
            "off": {"id": "off", "nodes": []}
        }))
        .unwrap()
    }

    fn state(sparks_reason: &str) -> serde_json::Value {
        serde_json::json!({
            "pools": {
                "sparks": {"desired": "dsv4f-tp2", "reason": sparks_reason, "pin": null,
                           "default": "dsv4f-tp2", "running": "dsv4f-tp2", "converged": true,
                           "nodes": ["sp1", "sp2"]},
                "halo": {"desired": "qwen3-embed-4b", "reason": "default", "pin": null,
                         "default": "qwen3-embed-4b", "running": "qwen3-embed-4b", "converged": true,
                         "nodes": ["halo"]}
            },
            "power": {"sp1": {"desired": "on", "phase": "on"}, "sp2": {"desired": "on", "phase": "on"}},
            "nodes": {
                "sp1": {"running": "dsv4f-tp2", "healthy": true, "stale": false},
                "sp2": {"running": "dsv4f-tp2", "healthy": true, "stale": false},
                "halo": {"running": "qwen3-embed-4b", "healthy": true, "stale": false}
            }
        })
    }

    fn link() -> BginfLinkSettings {
        BginfLinkSettings {
            url: "http://ctl.example:4100".into(),
            machines: BTreeMap::from([("halo".to_string(), "fedora".to_string())]),
        }
    }

    fn view(state_json: serde_json::Value, row: impl FnOnce(&mut LinkRow)) -> InferenceView {
        let parsed: State = serde_json::from_value(state_json.clone()).unwrap();
        let imported = import(&profiles(), Topology::Live(&parsed), &machine_map(&link()));
        let mut link_row = LinkRow {
            last_attempt_at: Some(NOW - 12_000),
            last_ok_at: Some(NOW - 12_000),
            state_json: Some(state_json.to_string()),
            ..LinkRow::default()
        };
        row(&mut link_row);
        InferenceView {
            config: LinkConfig::Configured(link()),
            link: Some(link_row),
            inventory: imported.inventory,
            executors: BTreeMap::from([("fedora".to_string(), true)]),
            control: Default::default(),
            deployment_control: Default::default(),
            now_ms: NOW,
        }
    }

    #[test]
    fn the_pools_page_agrees_pool_by_pool() {
        let page = render_pools(&view(state("default"), |_| {}), &[]);
        assert!(
            page.contains("- bginf last read 2027-01-15 07:59:48 UTC (12s ago)"),
            "{page}"
        );
        assert_eq!(page.matches("- verdict: agree").count(), 2, "{page}");
        assert!(page.contains("- `fedora`: enrolled executor (attached); 192.0.2.43; no outlet bginf manages, so always powered"), "{page}");
        assert!(
            page.contains("- `sp2`: not enrolled; address unknown; bginf wants power on, phase on"),
            "{page}"
        );
        assert!(
            page.contains("- Cairn: desired `dsv4f-tp2`; because default; converged yes"),
            "{page}"
        );
        assert!(page.contains("- bginf: desired `dsv4f-tp2`; reason \"default\"; running dsv4f-tp2; converged yes"), "{page}");
    }

    #[test]
    fn a_disagreement_is_shown_not_corrected() {
        let page = render_pools(&view(state("pin"), |_| {}), &[]);
        assert!(
            page.contains("- verdict: DISAGREE: reason: Cairn \"default\", bginf \"pin\""),
            "{page}"
        );
        assert!(
            page.contains("- Cairn: desired `dsv4f-tp2`; because default"),
            "{page}"
        );
    }

    #[test]
    fn an_unreachable_bginf_shows_the_last_read_with_its_age() {
        let page = render_pools(
            &view(state("default"), |row| {
                row.last_attempt_at = Some(NOW - 5_000);
                row.failing_since = Some(NOW - 95_000);
                row.last_error = Some("GET /state: connection refused".into());
                row.last_ok_at = Some(NOW - 125_000);
            }),
            &[],
        );
        assert!(page.contains("- bginf unreachable since 2027-01-15 07:58:25 UTC (1m ago): GET /state: connection refused"), "{page}");
        assert!(
            page.contains(
                "- showing the last successful read, from 2027-01-15 07:57:55 UTC (2m old)"
            ),
            "{page}"
        );
        assert_eq!(page.matches("- verdict: agree").count(), 2, "{page}");
    }

    #[test]
    fn an_unconfigured_link_names_what_is_missing() {
        let view = InferenceView {
            config: LinkConfig::Missing {
                setting: true,
                account: true,
            },
            link: None,
            inventory: Default::default(),
            executors: BTreeMap::new(),
            control: Default::default(),
            deployment_control: Default::default(),
            now_ms: NOW,
        };
        let page = render_pools(&view, &[]);
        assert!(page.contains("- bginf link: not configured; missing the `inference.bginf` section of cairn://settings and a `bginf` account"), "{page}");
        assert!(page.contains("No pools yet."), "{page}");
        let index = render_index(&view, &[]);
        assert!(index.contains("- `cairn://inference/pools`"), "{index}");
    }

    #[test]
    fn recipes_and_deployments_state_their_unknowns() {
        let view = view(state("default"), |_| {});
        let recipes = render_recipes(&view, &[]);
        assert!(
            recipes.contains("- verbs: unknown (imported from bginf)"),
            "{recipes}"
        );
        assert!(recipes.contains("  - accelerator: unknown"), "{recipes}");
        assert!(recipes.contains("    ./start.sh --port 8888"), "{recipes}");
        let deployments = render_deployments(&view, &[]);
        assert!(
            deployments.contains("- API base: http://192.0.2.43:8090/v1"),
            "{deployments}"
        );
        assert!(
            deployments.contains("  - `fedora` (head; enrolled executor (attached))"),
            "{deployments}"
        );
        assert!(
            deployments.contains("  - `sp2` (not enrolled)"),
            "{deployments}"
        );
    }

    /// The sparks pool under Cairn's control with `dsv4f-tp2` healthy and
    /// requested a minute ago, three batches waiting, and a request for
    /// `qwen36-fast` queued behind it.
    fn controlled_view() -> InferenceView {
        use crate::orchestrator::pool_control::{ControlView, PoolPlan, StoredRequest};
        use cairn_inference::pool_control::{
            BuildPressure, Observed, PoolControl, PoolSettings, Request,
        };
        let mut view = view(state("default"), |_| {});
        view.inventory.pools.get_mut(&"sparks".into()).unwrap().pin = Some(DeploymentSet::new());
        let pool = view.inventory.pools[&"sparks".into()].clone();
        let phases = BTreeMap::from([("dsv4f-tp2".into(), Phase::Healthy)]);
        let mut control = PoolControl::begin(
            &pool,
            &view.inventory.deployments,
            &phases,
            NOW - 30 * 60_000,
        );
        control.settings = PoolSettings {
            model_idle_secs: Some(600),
            ..PoolSettings::default()
        };
        let request = |id, deployment: &str, at, state| StoredRequest {
            pool: "sparks".into(),
            request: Request {
                id,
                deployment: deployment.into(),
                model: deployment.to_string(),
                requester: "cairn://p/cairn/9/1/builder".into(),
                at,
                state,
            },
        };
        let requests = vec![
            request(
                1,
                "dsv4f-tp2",
                NOW - 60_000,
                RequestState::Ready { at: NOW - 60_000 },
            ),
            request(2, "qwen36-fast", NOW - 30_000, RequestState::Waiting),
        ];
        let builds = BuildPressure {
            waiting: 3,
            ..BuildPressure::default()
        };
        let settings = effective_settings(&control);
        let mine: Vec<Request> = requests.iter().map(|r| r.request.clone()).collect();
        let plan = pool_control::plan(
            &pool,
            &view.inventory.deployments,
            &control,
            &settings,
            &mine,
            Observed {
                now: NOW,
                phases: &phases,
                builds,
            },
        );
        view.control = ControlView {
            now: NOW,
            controls: BTreeMap::from([("sparks".into(), control)]),
            plans: BTreeMap::from([(
                "sparks".into(),
                PoolPlan {
                    settings,
                    builds,
                    plan,
                },
            )]),
            phases,
            requests,
        };
        view
    }

    #[test]
    fn a_controlled_pool_shows_what_the_controller_decided_and_why() {
        let page = render_pools(&controlled_view(), &[]);
        assert!(
            page.contains("- Cairn's control: the pool controller decides this pool"),
            "{page}"
        );
        assert!(
            page.contains("- desired (written as desired state): `dsv4f-tp2`; slot default; adopted what was running when Cairn took control, 30m ago"),
            "{page}"
        );
        assert!(
            page.contains("  - `dsv4f-tp2`: healthy; healthy for 30m"),
            "{page}"
        );
        assert!(
            page.contains("- batch work: 0 agent turns and 0 other batches running on the pool's machines; 3 batches waiting that they refused; last batch work 0s ago"),
            "{page}"
        );
        assert!(
            page.contains("- settings: model idle window 10m (operator); pool idle window 15m (built-in; no load measured yet);"),
            "{page}"
        );
        assert!(
            !page.contains("warning: bginf"),
            "bginf is pinned off: {page}"
        );
        assert!(
            page.contains("- Cairn's control: not taken, so bginf decides this pool; patch this resource with {pool:\"halo\", controlled:true} to take it"),
            "{page}"
        );
    }

    #[test]
    fn the_requests_page_names_the_model_the_pool_and_the_wait() {
        let page = render_requests(&controlled_view(), &[]);
        assert!(page.contains("## Waiting (1)"), "{page}");
        assert!(
            page.contains("- #2 qwen36-fast on pool sparks for cairn://p/cairn/9/1/builder, waiting 30s: dsv4f-tp2 was requested within its idle window and keeps the pool, until in 9m"),
            "{page}"
        );
        assert!(page.contains("## Answered in the last 7d (1)"), "{page}");
        assert!(
            page.contains(
                "- #1 dsv4f-tp2 on pool sparks for cairn://p/cairn/9/1/builder: ready 1m ago"
            ),
            "{page}"
        );
    }

    #[test]
    fn a_deployment_page_shows_its_phase_holds_memory_and_loads() {
        use crate::orchestrator::inference_control::{MachineReading, StateRow};
        let mut view = view(state("default"), |_| {});
        let page = render_deployment(&view, "dsv4f-tp2", &[]);
        assert!(
            page.contains("- phase: never acted on by Cairn, so it is neither probed nor reserved"),
            "{page}"
        );
        assert!(page.contains("- last measured load: none yet"), "{page}");

        view.deployment_control.states.insert(
            "dsv4f-tp2".into(),
            StateRow {
                phase: DeploymentPhase::Healthy {
                    since_ms: NOW - 120_000,
                    load_ms: Some(312_000),
                },
                command_id: None,
                updated_at: NOW - 120_000,
            },
        );
        view.deployment_control.readings.insert(
            "sp1".into(),
            MachineReading {
                gpu_allocated_bytes: Some(100 << 30),
                free_bytes: Some(20 << 30),
                free_is_host_memory: true,
            },
        );
        view.deployment_control.loads.insert(
            "dsv4f-tp2".into(),
            vec![LoadRow {
                recipe: "dsv4f-tp2".into(),
                requested_at: NOW - 440_000,
                sent_at: Some(NOW - 432_000),
                finished_at: NOW - 120_000,
                healthy: true,
                load_ms: Some(312_000),
                reason: None,
                footprint: Some(BTreeMap::from([
                    ("sp1".to_string(), 100u64 << 30),
                    ("sp2".to_string(), 98u64 << 30),
                ])),
            }],
        );
        let page = render_deployment(&view, "dsv4f-tp2", &[]);
        assert!(
            page.contains(
                "- phase: healthy since 2027-01-15 07:58:00 UTC (2m ago); the load took 5m 12s"
            ),
            "{page}"
        );
        assert!(
            page.contains("  - `sp1` (head; not enrolled); held away from batch work by this deployment; GPU-allocated now 100.0 GiB"),
            "{page}"
        );
        assert!(
            page.contains("  - `sp2` (not enrolled); held away from batch work by this deployment; no GPU reading (not attached)"),
            "{page}"
        );
        assert!(
            page.contains(
                "- measured footprint (GPU-allocated once settled): sp1 100.0 GiB, sp2 98.0 GiB"
            ),
            "{page}"
        );
        assert!(
            page.contains("healthy after 5m 12s; waited 8s for its machines"),
            "{page}"
        );
        assert!(
            page.contains("pin bginf's pool `sparks` to `off`"),
            "{page}"
        );

        let list = render_deployments(&view, &[]);
        assert!(
            list.contains("- read or act on it: cairn://inference/deployments/dsv4f-tp2"),
            "{list}"
        );
        assert!(render_deployment(&view, "nope", &[]).starts_with("No deployment `nope`"));
    }

    #[test]
    fn the_index_exports_yaml_on_request() {
        let view = view(state("default"), |_| {});
        let format = QueryParam {
            key: "format".into(),
            value: "yaml".into(),
        };
        let yaml = render_index(&view, std::slice::from_ref(&format));
        assert_eq!(
            cairn_inference::Inventory::from_yaml(&yaml),
            Ok(view.inventory.clone())
        );
        let bad = QueryParam {
            key: "view".into(),
            value: "x".into(),
        };
        assert!(render_index(&view, &[bad]).starts_with("Unsupported query parameter"));
    }
}
