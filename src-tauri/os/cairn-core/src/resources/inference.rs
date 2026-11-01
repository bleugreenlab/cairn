//! Renderers for the cairn-inference facet's resources, `cairn://inference/*`.
//!
//! Each page renders an [`InferenceView`]: the stored inventory, what Cairn
//! has done to the deployments it acted on, the pool controller's state and
//! the enrolled executors. A pool under Cairn's control shows the
//! controller's state, and the requests page lists the controller's queue,
//! each computed by the same plan the next tick writes.

use std::fmt::Write as _;

use cairn_common::query::QueryParam;
use cairn_inference::pool_control::{self, RequestState};
use cairn_inference::{
    DeploymentSet, Engine, MachineName, MemoryPerMachine, Pool, Recipe, Requirements,
};

use super::common::find_query_value;
use crate::orchestrator::inference::InferenceView;
use crate::orchestrator::inference_control::{holds_of, LoadRow};
use crate::orchestrator::inference_verify::VerificationRow;
use crate::orchestrator::pool_control::ANSWERED_KEPT_MS;
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
        "each pool's machines, pin and default, and the pool controller's state; patch it to take a pool under Cairn's control",
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

fn set_label(set: &DeploymentSet) -> String {
    if set.is_empty() {
        "nothing".to_string()
    } else {
        set.iter()
            .map(|id| format!("`{id}`"))
            .collect::<Vec<_>>()
            .join(" + ")
    }
}

fn enrolment(view: &InferenceView, machine: &MachineName) -> &'static str {
    match view.executors.get(machine.as_str()) {
        Some(true) => "enrolled executor (attached)",
        Some(false) => "enrolled executor (not attached)",
        None => "not enrolled",
    }
}

/// What is left of bginf, and how to clear it, one fact per line.
fn retired_lines(view: &InferenceView) -> String {
    let mut out = String::new();
    let foreign = view.inventory.foreign_records();
    if !foreign.is_empty() {
        let _ = writeln!(
            out,
            "- {} records are still mirrored from bginf and read-only ({}); adopt them as Cairn's own, keeping their ids: `write({{changes:[{{target:\"cairn://inference\",mode:\"patch\",payload:{{adopt:\"bginf\"}}}}]}})`",
            foreign.len(),
            foreign.join(", ")
        );
    }
    if view.retired_link_setting {
        out.push_str("- settings.yaml still names the retired bginf link (`inference.bginf`), which nothing reads; writing the `inference` section of cairn://settings drops it\n");
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

/// `cairn://inference`: the record counts, the collections, and, under
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
        "# cairn-inference\n\nWhich model each inference machine should run. Recipes say how a model is run, deployments place a recipe on named machines, and pools are the sets of machines that share one desired state. Every record is Cairn's own and the operator writes it here: replace this resource with a whole inventory (the document `?format=yaml` reads), or patch it to add, replace or remove records; a write is validated as a whole and refused naming every problem. A pool taken under Cairn's control is decided by the pool controller: a request loads its model, batches waiting for the pool unload an idle one, and an idle pool loads its default. The controller writes desired state; one deployment's page (`cairn://inference/deployments/<id>`) is the act side, starting or stopping it on its head and holding every machine of the gang away from batch work while it does. Cairn powers nothing.\n\n",
    );
    out.push_str(&retired_lines(view));
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
        cairn_inference::RecordSource::Bginf => {
            "bginf (mirrored before bginf retired; read-only until adopted)"
        }
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

fn container_lines(out: &mut String, container: &cairn_inference::Container) {
    let _ = writeln!(out, "  - image: {}", container.image);
    let _ = writeln!(out, "  - server: {}", container.server.as_str());
    if let Some(entrypoint) = &container.entrypoint {
        let _ = writeln!(out, "  - entrypoint: {entrypoint:?}");
    }
    if !container.args.is_empty() {
        let _ = writeln!(out, "  - args: {}", container.args.join(" "));
    }
    for (key, value) in &container.env {
        let _ = writeln!(out, "  - env {key}={value}");
    }
    for (rank, env) in container.ranks.iter().enumerate() {
        let pairs: Vec<String> = env.iter().map(|(k, v)| format!("{k}={v}")).collect();
        let _ = writeln!(
            out,
            "  - rank {rank} env: {}",
            if pairs.is_empty() {
                "none".to_string()
            } else {
                pairs.join(" ")
            }
        );
    }
    for mount in &container.mounts {
        let from = match &mount.from {
            cairn_inference::MountSource::HfCache => "the Hugging Face cache".to_string(),
            cairn_inference::MountSource::Source { path } => format!("source {path}"),
        };
        let _ = writeln!(
            out,
            "  - mount: {from} at {}{}",
            mount.to,
            if mount.read_only { " (read-only)" } else { "" }
        );
    }
    let host = &container.host;
    let mut flags = Vec::new();
    if host.gpus {
        flags.push("gpus".to_string());
    }
    if host.ipc_host {
        flags.push("host ipc".to_string());
    }
    if host.network_host {
        flags.push("host network".to_string());
    }
    if let Some(size) = &host.shm_size {
        flags.push(format!("shm {size}"));
    }
    flags.extend(host.devices.iter().map(|d| format!("device {d}")));
    if !flags.is_empty() {
        let _ = writeln!(out, "  - host: {}", flags.join(", "));
    }
    if let Some(files) = &container.files {
        let _ = writeln!(out, "  - source files: {} at {}", files.url, files.commit);
    }
}

fn origin_line(out: &mut String, origin: &cairn_inference::RecipeOrigin) {
    match origin {
        cairn_inference::RecipeOrigin::Authored => out.push_str("- origin: authored\n"),
        cairn_inference::RecipeOrigin::Imported {
            url,
            commit,
            compose,
            service,
        } => {
            let _ = writeln!(
                out,
                "- origin: imported from {url} at {commit}, service {service} of {compose}"
            );
        }
        cairn_inference::RecipeOrigin::Generated {
            template,
            model_revision,
        } => {
            let _ = writeln!(
                out,
                "- origin: generated from template {template} for the model at {model_revision}"
            );
        }
    }
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
    let _ = writeln!(
        out,
        "- quantization: {}",
        recipe.quantization.as_deref().unwrap_or("unknown")
    );
    origin_line(out, &recipe.origin);
    let _ = writeln!(out, "- served name: {}", recipe.served_name);
    let _ = writeln!(
        out,
        "- upstream id: {}",
        recipe.upstream_id.as_deref().unwrap_or("unknown")
    );
    let verbs = match &recipe.verbs {
        Some(verbs) => verbs
            .iter()
            .map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        None => "unknown".to_string(),
    };
    let _ = writeln!(out, "- verbs: {verbs}");
    let _ = writeln!(out, "- engine: {}", recipe.engine.kind());
    match &recipe.engine {
        Engine::Container(container) => container_lines(out, container),
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
        out.push_str("No recipes yet. Write them by replacing or patching cairn://inference.\n");
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
        out.push_str(
            "No deployments yet. Write them by replacing or patching cairn://inference.\n",
        );
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
        return "never acted on by Cairn, so it is neither probed nor reserved; whatever answers there now was not started by Cairn".into();
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
            "- {}: healthy after {}{waited}; settled footprint {}{}",
            at(view, load.finished_at),
            duration(ms),
            load.footprint
                .as_ref()
                .map_or("not recorded yet".to_string(), footprint_line),
            load.reason
                .as_deref()
                .map(|late| format!("; its start script then ended badly: {late}"))
                .unwrap_or_default()
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
        "\n## Acting on it\n\n- start: `write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"start\"}}}}]}})` reserves every machine above at once, then runs the recipe's start command on the head once each has room.\n- stop: the same with `action:\"stop\"` runs the recipe's stop command on the head and waits for the health check to stop answering.\n- While it holds its machines, a write to cairn://inference may not remove it or move it to other machines; stop it first. A changed start or stop command takes effect at the next start or stop.\n"
    );
    out
}

fn pool_records(out: &mut String, view: &InferenceView, pool: &Pool) {
    let _ = writeln!(out, "- source: {}", source_label(pool.source));
    out.push_str("- machines:\n");
    for machine in &pool.machines {
        let address = machine.address.as_deref().unwrap_or("address unknown");
        let _ = writeln!(
            out,
            "  - `{}`: {}; {address}",
            machine.name,
            enrolment(view, &machine.name)
        );
    }
    let pin = pool.pin.as_ref().map_or("none".to_string(), set_label);
    let _ = writeln!(out, "- pin (record): {pin}");
    let _ = writeln!(out, "- default: {}", set_label(&pool.default));
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

/// A controller member's phase in a few words; the deployment's own page has
/// the rest.
fn phase_label(phase: Option<&DeploymentPhase>) -> String {
    match phase {
        None => "unknown (Cairn has not acted on it yet)".to_string(),
        Some(DeploymentPhase::Reserving {
            waiting_on: Some(on),
            ..
        }) => format!("waiting to fit: {on}"),
        Some(DeploymentPhase::Failed { reason, .. }) => format!("failed: {reason}"),
        Some(phase) => phase.label().to_string(),
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
            "- Cairn's control: not taken, so nothing decides this pool and its deployments start and stop only from their own pages; patch this resource with {{pool:\"{}\", controlled:true}} to take it",
            pool.id
        );
        return;
    };
    let settings = control_view
        .settings
        .get(&pool.id)
        .copied()
        .unwrap_or_else(|| control.settings.effective(None));
    let settings_line = format!(
        "- settings: model idle window {}; pool idle window {}; minimum residency {}; load bound {}",
        settings.model_idle, settings.pool_idle, settings.min_residency, settings.load_bound
    );
    let Some(planned) = control_view.plans.get(&pool.id) else {
        out.push_str(
            "- Cairn's control: handed back, so nothing decides this pool; its settings are kept\n",
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
    if view.inventory.pools.is_empty() {
        out.push_str("No pools yet. Write them by replacing or patching cairn://inference.\n");
        return out;
    }
    for pool in view.inventory.pools.values() {
        let _ = writeln!(out, "## {}\n", pool.id);
        pool_records(&mut out, view, pool);
        control_lines(&mut out, view, pool);
        out.push('\n');
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

/// One verification in one line: what it reached and what it measured.
fn verification_line(view: &InferenceView, row: &VerificationRow) -> String {
    let mut parts = vec![format!(
        "{} on pool {} ({}) {}",
        row.outcome,
        row.pool,
        row.machines.join(", "),
        at(view, row.started_at)
    )];
    if row.outcome != "passed" {
        parts.push(format!("stage {}", row.stage));
    }
    if let Some(ms) = row.load_ms {
        parts.push(format!("loaded in {}", duration(ms.max(0) as u64)));
    }
    if let Some(footprint) = &row.footprint {
        let per: Vec<String> = footprint
            .iter()
            .map(|(machine, bytes)| format!("{machine} {}", gib(*bytes)))
            .collect();
        parts.push(format!("memory {}", per.join(", ")));
    }
    if let Some(context) = row.context_tokens {
        parts.push(format!("context {context}"));
    }
    if let Some(kv) = row.kv_tokens {
        parts.push(format!("KV cache {kv} tokens"));
    }
    if let Some(ttft) = row.ttft_ms {
        parts.push(format!("TTFT {ttft} ms"));
    }
    if let Some(tps) = row.decode_tps {
        parts.push(format!("decode {tps:.1} tok/s"));
    }
    if let Some(tps) = row.prefill_tps {
        parts.push(format!("prefill {tps:.0} tok/s"));
    }
    if let Some(tps) = row.embed_tps {
        parts.push(format!("embeddings {tps:.0} tok/s"));
    }
    if let Some(version) = &row.engine_version {
        parts.push(format!("engine {version}"));
    }
    if let Some(reason) = &row.reason {
        parts.push(format!("reason: {reason}"));
    }
    if row.stale(&view.inventory) {
        parts.push("stale: the recipe has changed since".into());
    }
    format!("`{}`: {}", row.id, parts.join("; "))
}

/// The newest verification of `recipe` on each pool.
fn latest_per_pool<'a>(view: &'a InferenceView, recipe: &str) -> Vec<&'a VerificationRow> {
    let mut seen = std::collections::BTreeSet::new();
    view.verifications
        .iter()
        .filter(|row| row.recipe == recipe && seen.insert(row.pool.clone()))
        .collect()
}

fn model_line(model: &cairn_inference::Model) -> String {
    format!(
        "{} (revision {}{})",
        crate::orchestrator::inference_models::describe(model),
        model
            .revision
            .as_deref()
            .unwrap_or("unread: its facts are unknown"),
        model
            .read_at
            .map(|at| format!(", read {}", when_ms(at)))
            .unwrap_or_default()
    )
}

fn recipe_summary(recipe: &Recipe) -> String {
    let server = match &recipe.engine {
        Engine::Container(container) => container.server.as_str(),
        other => other.kind(),
    };
    format!(
        "{}, {server}, x{}, {}",
        recipe
            .quantization
            .as_deref()
            .unwrap_or("quantization unknown"),
        recipe.requirements.machines,
        recipe.origin.label()
    )
}

pub(crate) fn render_models(view: &InferenceView, params: &[QueryParam]) -> String {
    if let Some(error) = reject_params("cairn://inference/models", params, &[]) {
        return error;
    }
    let mut out = String::from("# Inference models\n\n");
    if view.inventory.models.is_empty() {
        out.push_str("No models yet. Register one by appending its Hugging Face name to cairn://inference/models.\n");
        return out;
    }
    for model in view.inventory.models.values() {
        let recipes: Vec<String> = view
            .inventory
            .recipes
            .values()
            .filter(|r| r.model.name == model.name)
            .map(|r| format!("`{}`", r.id))
            .collect();
        let _ = writeln!(
            out,
            "- `{}`: {}; recipes: {}",
            model.name,
            model_line(model),
            if recipes.is_empty() {
                "none".to_string()
            } else {
                recipes.join(", ")
            }
        );
    }
    out
}

pub(crate) fn render_model(view: &InferenceView, name: &str, params: &[QueryParam]) -> String {
    let uri = format!("cairn://inference/models/{name}");
    if let Some(error) = reject_params(&uri, params, &[]) {
        return error;
    }
    let Some(model) = view.inventory.models.get(name) else {
        return format!("There is no model {name}. cairn://inference/models lists them; append a Hugging Face name there to register one.");
    };
    let mut out = format!("# {}\n\n- {}\n", model.name, model_line(model));
    let facts = &model.facts;
    let unknown = |v: Option<String>| v.unwrap_or_else(|| "unknown".into());
    let _ = writeln!(out, "- model type: {}", unknown(facts.model_type.clone()));
    if let Some(modalities) = &facts.modalities {
        let list = |m: &[cairn_models::types::catalog::Modality]| {
            m.iter()
                .map(|m| m.as_str().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };
        let _ = writeln!(
            out,
            "- takes {} and gives {}",
            list(&modalities.input),
            list(&modalities.output)
        );
    }
    let _ = writeln!(
        out,
        "- KV geometry: {}",
        match facts.kv_elements_per_token() {
            Some(elements) => format!("{elements} elements a token across its layers"),
            None => "unknown, so verification clamps the context to the cache the engine measures"
                .into(),
        }
    );
    out.push_str("\n## Recipes\n\n");
    let recipes: Vec<&Recipe> = view
        .inventory
        .recipes
        .values()
        .filter(|r| r.model.name == model.name)
        .collect();
    if recipes.is_empty() {
        out.push_str("None yet: import one or generate candidates at cairn://inference/recipes.\n");
    }
    for recipe in recipes {
        let _ = writeln!(out, "- `{}`: {}", recipe.id, recipe_summary(recipe));
        for row in latest_per_pool(view, recipe.id.as_str()) {
            let _ = writeln!(out, "  - {}", verification_line(view, row));
        }
    }
    out
}

pub(crate) fn render_recipe(view: &InferenceView, id: &str, params: &[QueryParam]) -> String {
    let uri = format!("cairn://inference/recipes/{id}");
    if let Some(error) = reject_params(&uri, params, &[]) {
        return error;
    }
    let Some(recipe) = view
        .inventory
        .recipes
        .get(&cairn_inference::RecipeId::from(id))
    else {
        return format!("There is no recipe {id}. cairn://inference/recipes lists them.");
    };
    let mut out = String::new();
    recipe_section(&mut out, recipe);
    for deployment in view
        .inventory
        .deployments
        .values()
        .filter(|d| d.recipe == recipe.id)
    {
        let pool = view.inventory.pool_of_deployment(deployment);
        let machines: Vec<cairn_inference::render::MachineAt> = deployment
            .machines
            .iter()
            .map(|name| cairn_inference::render::MachineAt {
                name: name.clone(),
                address: pool.and_then(|p| p.address_of(name)).map(str::to_string),
                link: pool.and_then(|p| p.link_of(name)).map(str::to_string),
            })
            .collect();
        let _ = writeln!(out, "## As deployment {}\n", deployment.id);
        match cairn_inference::render::render(recipe, deployment, &machines) {
            Ok(rendered) => {
                fenced(&mut out, "start", &rendered.start);
                if let Some(stop) = &rendered.stop {
                    fenced(&mut out, "stop", stop);
                }
            }
            Err(refusal) => {
                let _ = writeln!(out, "- it cannot be rendered: {}\n", refusal.0);
            }
        }
    }
    out.push_str("## Verifications\n\n");
    let rows: Vec<&VerificationRow> = view
        .verifications
        .iter()
        .filter(|r| r.recipe == id)
        .collect();
    if rows.is_empty() {
        let _ = writeln!(
            out,
            "None yet. Verify it on a pool: patch {uri} {{verify:{{pool:\"<pool>\"}}}}"
        );
    }
    for row in rows {
        let _ = writeln!(out, "- {}", verification_line(view, row));
    }
    out
}

pub(crate) fn render_verifications(view: &InferenceView, params: &[QueryParam]) -> String {
    if let Some(error) = reject_params("cairn://inference/verifications", params, &[]) {
        return error;
    }
    let mut out = String::from("# Inference verifications\n\n");
    if view.verifications.is_empty() {
        out.push_str("None yet. Patch a recipe or a model with verify to load it on a pool and measure it.\n");
    }
    for row in &view.verifications {
        let _ = writeln!(
            out,
            "- recipe `{}`: {}",
            row.recipe,
            verification_line(view, row)
        );
    }
    out
}

pub(crate) fn render_verification(view: &InferenceView, id: &str, params: &[QueryParam]) -> String {
    if let Some(error) = reject_params(
        &format!("cairn://inference/verifications/{id}"),
        params,
        &[],
    ) {
        return error;
    }
    let Some(row) = view.verifications.iter().find(|r| r.id == id) else {
        return format!(
            "There is no verification {id}. cairn://inference/verifications lists them."
        );
    };
    let mut out = format!("# Verification {id}\n\n");
    let _ = writeln!(
        out,
        "- recipe: `{}` (cairn://inference/recipes/{})",
        row.recipe, row.recipe
    );
    let _ = writeln!(out, "- deployment: `{}`", row.deployment);
    let _ = writeln!(out, "- requested by: {}", row.requested_by);
    let _ = writeln!(out, "- stage: {}", row.stage);
    if let Some(finished) = row.finished_at {
        let _ = writeln!(out, "- finished: {}", at(view, finished));
    }
    let _ = writeln!(out, "- {}", verification_line(view, row));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_inference::{Inventory, RecordSource};
    use std::collections::BTreeMap;

    const NOW: i64 = 1_800_000_000_000;

    const INVENTORY: &str = r#"
models:
  - {name: dsv4f-tp2}
  - {name: qwen36-fast}
  - {name: qwen3-embed-4b}
recipes:
  - {id: dsv4f-tp2, model: {name: dsv4f-tp2}, servedName: dsv4f-tp2, engine: {kind: command, start: ./start.sh --port 8888, stop: ./stop.sh, description: vllm}, requirements: {machines: 2}}
  - {id: qwen36-fast, model: {name: qwen36-fast}, servedName: qwen36-fast, engine: {kind: command, start: ./start.sh --port 8001, stop: ./stop.sh}, requirements: {machines: 1}}
  - {id: qwen3-embed-4b, model: {name: qwen3-embed-4b}, servedName: qwen3-embed-4b, engine: {kind: command, start: ./start.sh --port 8090, stop: ./stop.sh}, requirements: {machines: 1}}
deployments:
  - {id: dsv4f-tp2, recipe: dsv4f-tp2, machines: [sp1, sp2], head: sp1, apiPort: 8888, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen36-fast, recipe: qwen36-fast, machines: [sp1], head: sp1, apiPort: 8001, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen3-embed-4b, recipe: qwen3-embed-4b, machines: [fedora], head: fedora, apiPort: 8090, apiPath: /v1, healthPath: /health}
pools:
  - {id: sparks, machines: [{name: sp1, address: 192.0.2.40}, {name: sp2}], default: [dsv4f-tp2]}
  - {id: halo, machines: [{name: fedora, address: 192.0.2.43}], default: [qwen3-embed-4b]}
"#;

    fn view() -> InferenceView {
        InferenceView {
            inventory: Inventory::from_yaml(INVENTORY).unwrap(),
            retired_link_setting: false,
            executors: BTreeMap::from([("fedora".to_string(), true)]),
            control: Default::default(),
            deployment_control: Default::default(),
            verifications: Vec::new(),
            now_ms: NOW,
        }
    }

    #[test]
    fn the_model_and_recipe_pages_show_facts_recipes_and_verifications() {
        let mut view = view();
        let recipe = view.inventory.recipes.values().next().unwrap().clone();
        let model = recipe.model.name.clone();
        let row = VerificationRow {
            id: "v1".into(),
            recipe: recipe.id.to_string(),
            recipe_digest: crate::orchestrator::inference_verify::digest(&recipe),
            pool: "sparks".into(),
            deployment: format!("verify-{}-sparks", recipe.id),
            machines: vec!["bginf-sp1".into()],
            requested_by: "operator".into(),
            started_at: NOW - 600_000,
            finished_at: Some(NOW - 60_000),
            outcome: "passed".into(),
            stage: "done".into(),
            reason: None,
            load_ms: Some(400_000),
            footprint: Some(BTreeMap::from([("bginf-sp1".to_string(), 100 << 30)])),
            context_tokens: Some(262_144),
            kv_tokens: Some(1_447_093),
            ttft_ms: Some(180),
            decode_tps: Some(41.5),
            prefill_tps: Some(4_200.0),
            embed_tps: None,
            engine_version: Some("0.32.0".into()),
        };
        view.verifications.push(row);
        let page = render_model(&view, &model, &[]);
        assert!(page.contains("unread: its facts are unknown"), "{page}");
        assert!(page.contains(&format!("- `{}`: ", recipe.id)), "{page}");
        assert!(page.contains("passed on pool sparks (bginf-sp1)"), "{page}");
        assert!(
            page.contains("loaded in 6m 40s; memory bginf-sp1 100.0 GiB"),
            "{page}"
        );
        assert!(page.contains("decode 41.5 tok/s"), "{page}");
        assert!(!page.contains("stale"), "{page}");

        let page = render_recipe(&view, recipe.id.as_str(), &[]);
        assert!(page.contains("## Verifications"), "{page}");
        assert!(page.contains("`v1`: passed"), "{page}");

        view.inventory.recipes.get_mut(&recipe.id).unwrap().notes = Some("changed".into());
        let page = render_verifications(&view, &[]);
        assert!(
            page.contains("stale: the recipe has changed since"),
            "{page}"
        );
        assert!(render_model(&view, "nobody/Nothing", &[])
            .starts_with("There is no model nobody/Nothing"));
    }

    #[test]
    fn the_pools_page_shows_each_pools_records() {
        let page = render_pools(&view(), &[]);
        assert!(
            page.contains("- `fedora`: enrolled executor (attached); 192.0.2.43\n"),
            "{page}"
        );
        assert!(
            page.contains("- `sp2`: not enrolled; address unknown\n"),
            "{page}"
        );
        assert!(page.contains("- default: `dsv4f-tp2`"), "{page}");
        assert!(page.contains("- pin (record): none"), "{page}");
        assert!(!page.contains("bginf"), "{page}");
    }

    #[test]
    fn an_empty_inventory_says_where_records_are_written() {
        let view = InferenceView {
            inventory: Default::default(),
            ..view()
        };
        let page = render_pools(&view, &[]);
        assert!(
            page.contains("No pools yet. Write them by replacing or patching cairn://inference."),
            "{page}"
        );
        let index = render_index(&view, &[]);
        assert!(index.contains("- `cairn://inference/pools`"), "{index}");
        assert!(index.contains("- records: 0 recipes"), "{index}");
    }

    #[test]
    fn the_index_says_what_is_left_of_bginf_until_it_is_cleared() {
        let mut view = view();
        assert!(!render_index(&view, &[]).contains("bginf"));
        view.inventory.pools.get_mut(&"halo".into()).unwrap().source = RecordSource::Bginf;
        view.retired_link_setting = true;
        let index = render_index(&view, &[]);
        assert!(
            index.contains("- 1 records are still mirrored from bginf and read-only (pool halo (bginf)); adopt them as Cairn's own"),
            "{index}"
        );
        assert!(index.contains("payload:{adopt:\"bginf\"}"), "{index}");
        assert!(
            index.contains("settings.yaml still names the retired bginf link"),
            "{index}"
        );
    }

    #[test]
    fn recipes_and_deployments_state_their_unknowns() {
        let view = view();
        let recipes = render_recipes(&view, &[]);
        assert!(recipes.contains("- verbs: unknown\n"), "{recipes}");
        assert!(recipes.contains("- source: cairn\n"), "{recipes}");
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
        let mut view = view();
        let pool = view.inventory.pools[&"sparks".into()].clone();
        let phases = BTreeMap::from([(
            "dsv4f-tp2".into(),
            DeploymentPhase::Healthy {
                since_ms: NOW - 30 * 60_000,
                load_ms: None,
            },
        )]);
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
        let settings = control.settings.effective(None);
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
            settings: BTreeMap::from([("sparks".into(), settings)]),
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
            page.contains("- Cairn's control: not taken, so nothing decides this pool and its deployments start and stop only from their own pages; patch this resource with {pool:\"halo\", controlled:true} to take it"),
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
        let mut view = view();
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
            page.contains(
                "a write to cairn://inference may not remove it or move it to other machines"
            ),
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
        let view = view();
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
