//! Renderers for the cairn-inference facet's resources, `cairn://inference/*`.
//!
//! Each page renders an [`InferenceView`]: the stored inventory, what Cairn
//! has done to the deployments it acted on, the enrolled executors, and what
//! placement holds and decides about local models ([`ModelsView`]): the pools
//! it detects, the residents on them, and the requests with what each waits
//! on, as the last pass left them.

use std::fmt::Write as _;

use cairn_common::query::QueryParam;
use cairn_inference::{Engine, InferenceProfile, MachineName, MemoryPerMachine, Requirements};

use super::common::find_query_value;
use crate::fleet::models::requests::RequestState;
use crate::fleet::models::store::ANSWERED_KEPT_MS;
use crate::fleet::models::{ModelsView, PoolView, ResidentView};
use crate::orchestrator::inference::InferenceView;
use crate::orchestrator::inference_control::{self, LoadRow};
use crate::orchestrator::inference_verify::VerificationRow;
use cairn_inference::control::{gib, Phase as DeploymentPhase};
use cairn_inference::{unknown_address, Addresses, Deployment, DeploymentId};

const INDEX: &[(&str, &str)] = &[
    (
        "cairn://inference/profiles",
        "how each model is run: weights, engine, served name, and what the hardware must provide",
    ),
    (
        "cairn://inference/deployments",
        "profiles placed on named machines, with the API base and health URL derived from the head's address",
    ),
    (
        "cairn://inference/pools",
        "the pools placement detects from the executors' links, the models resident on each, its warm model, idle window and pins; patch it to set them",
    ),
    (
        "cairn://inference/requests",
        "placement's model requests: append a model to ask for it, and read what each request waits on",
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

/// Where Cairn reaches `deployment`'s head and where it learned that, with
/// the URLs derived from it; or why it cannot reach the head.
fn head_lines(out: &mut String, view: &InferenceView, deployment: &Deployment) {
    match Addresses::new(&view.inventory, &view.network).from_orch(&deployment.head) {
        Some(head) => {
            let _ = writeln!(out, "- head address: {head}");
            let _ = writeln!(out, "- API base: {}", deployment.api_base(&head.address));
            let _ = writeln!(out, "- health: {}", deployment.health_url(&head.address));
        }
        None => {
            let _ = writeln!(
                out,
                "- API base and health: unknown, so Cairn cannot start or probe it (port {}, API path {}, health path {}): the head {}",
                deployment.api_port,
                deployment.api_path,
                deployment.health_path,
                unknown_address(&deployment.head)
            );
        }
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
        "# cairn-inference\n\nHow each local model is run and where. Profiles say how a model is run, deployments place a profile on named machines, and machines say where each answers. Every record is Cairn's own and the operator writes it here: replace this resource with a whole inventory (the document `?format=yaml` reads), or patch it to add, replace or remove records; a write is validated as a whole and refused naming every problem. Which model runs is placement's: it detects pools of machines joined by a fast link, holds a running model as a resident on one, loads a model a request asks for, evicts an idle one when other work is refused its room, and places a pool's warm model when the pool is idle. One deployment's page (`cairn://inference/deployments/<id>`) starts or stops it by hand. Cairn powers nothing.\n\n",
    );
    out.push_str(&retired_lines(view));
    let _ = writeln!(
        out,
        "- records: {} profiles, {} deployments, {} machines\n",
        view.inventory.profiles.len(),
        view.inventory.deployments.len(),
        view.inventory.machines.len()
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
            cairn_inference::MountSource::HfCache => {
                "the pinned repositories' directories of the Hugging Face cache".to_string()
            }
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

fn origin_line(out: &mut String, origin: &cairn_inference::ProfileOrigin) {
    match origin {
        cairn_inference::ProfileOrigin::Authored => out.push_str("- origin: authored\n"),
        cairn_inference::ProfileOrigin::Imported {
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
        cairn_inference::ProfileOrigin::Generated {
            run,
            model_revision,
            starting_template,
            sources,
            answers,
        } => {
            let _ = writeln!(
                out,
                "- origin: generated {}for the model at {model_revision}{}",
                match run {
                    Some(run) => format!("by {run} "),
                    None => String::new(),
                },
                starting_template
                    .as_deref()
                    .map(|template| format!(", starting from template {template}"))
                    .unwrap_or_default()
            );
            for source in sources {
                let _ = writeln!(out, "  - cited: {} ({})", source.url, source.title);
            }
            for answer in answers {
                let _ = writeln!(
                    out,
                    "  - the operator answered {}: {}",
                    answer.question, answer.answer
                );
            }
        }
    }
}

fn profile_section(out: &mut String, profile: &InferenceProfile) {
    let _ = writeln!(out, "## {}\n", profile.id);
    let _ = writeln!(out, "- source: {}", source_label(profile.source));
    let _ = writeln!(out, "- model: {}", profile.model.name);
    if profile.model.weights.is_empty() {
        out.push_str("- weights: none pinned\n");
    } else {
        out.push_str("- weights:\n");
        for weight in &profile.model.weights {
            let _ = writeln!(
                out,
                "  - {}@{} {} (sha256 {})",
                weight.repo, weight.revision, weight.path, weight.sha256
            );
        }
    }
    if !profile.model.config_files.is_empty() {
        out.push_str("- config files:\n");
        for file in &profile.model.config_files {
            let pin = match (&file.sha256, &file.git_blob) {
                (Some(sha256), _) => format!("sha256 {sha256}"),
                (None, Some(blob)) => format!("git blob {blob}"),
                (None, None) => "unpinned".into(),
            };
            let _ = writeln!(
                out,
                "  - {}@{} {} ({pin})",
                file.repo, file.revision, file.path
            );
        }
    }
    let _ = writeln!(
        out,
        "- quantization: {}",
        profile.quantization.as_deref().unwrap_or("unknown")
    );
    origin_line(out, &profile.origin);
    let _ = writeln!(out, "- served name: {}", profile.served_name);
    let _ = writeln!(
        out,
        "- upstream id: {}",
        profile.upstream_id.as_deref().unwrap_or("unknown")
    );
    let verbs = match &profile.verbs {
        Some(verbs) => verbs
            .iter()
            .map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        None => "unknown".to_string(),
    };
    let _ = writeln!(out, "- verbs: {verbs}");
    let _ = writeln!(out, "- engine: {}", profile.engine.kind());
    match &profile.engine {
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
    requirements_lines(out, &profile.requirements);
    if let Some(notes) = &profile.notes {
        let _ = writeln!(out, "- notes: {notes}");
    }
    out.push('\n');
}

pub(crate) fn render_profiles(view: &InferenceView, params: &[QueryParam]) -> String {
    if let Some(error) = reject_params("cairn://inference/profiles", params, &[]) {
        return error;
    }
    let mut out = String::from("# Inference profiles\n\n");
    if view.inventory.profiles.is_empty() {
        out.push_str("No profiles yet. Write them by replacing or patching cairn://inference.\n");
        return out;
    }
    for profile in view.inventory.profiles.values() {
        profile_section(&mut out, profile);
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
        let _ = writeln!(out, "- profile: {}", deployment.profile);
        let _ = writeln!(out, "- pool: {}", pool_line(view, &deployment.id));
        out.push_str("- machines:\n");
        for machine in &deployment.machines {
            let head = if *machine == deployment.head {
                "head; "
            } else {
                ""
            };
            let _ = writeln!(out, "  - `{machine}` ({head}{})", enrolment(view, machine));
        }
        head_lines(&mut out, view, deployment);
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

/// Where placement may run a deployment now, or why it may not.
fn pool_line(view: &InferenceView, id: &DeploymentId) -> String {
    match view.models.placement(id) {
        Some(placement) => match (&placement.pool, &placement.refused) {
            (Some(pool), _) => format!("`{pool}`"),
            (None, Some(why)) => format!("none: {why}"),
            (None, None) => "none".to_string(),
        },
        None => "not known to placement yet".to_string(),
    }
}

/// One deployment's phase as Cairn holds it, in one sentence.
fn phase_summary(view: &InferenceView, id: &DeploymentId) -> String {
    let Some(row) = view.deployment_control.states.get(id) else {
        return "never acted on by Cairn; it is probed once, and adopted as a resident if it answers".into();
    };
    match &row.phase {
        DeploymentPhase::Stopped { since_ms } => format!("stopped since {}", at(view, *since_ms)),
        DeploymentPhase::Reserving {
            since_ms,
            waiting_on,
        } => format!(
            "reserving since {}: placement holds its footprint's room and has not sent the start yet{}",
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

/// What a deployment needs on each machine and where that comes from: its
/// last measured load, else its profile's statement, else a prediction marked
/// as one; or why it has no need, which refuses its start.
fn need_line(
    view: &InferenceView,
    deployment: &Deployment,
    loads: Option<&Vec<LoadRow>>,
) -> String {
    use crate::orchestrator::inference_control::{need_of, NeedBasis};
    const RESERVES: &str = "placement reserves this on each machine, and a start goes once each has this much free, draining only the checks that cover a shortfall";
    let need = need_of(&view.inventory, deployment, loads, |machine| {
        view.deployment_control.readings.get(machine)?.total_bytes
    });
    let need = match need {
        Ok(need) => need,
        Err(why) => {
            return format!("- memory need: unknown, so a start is refused: {why}\n");
        }
    };
    let unreported: Vec<&str> = deployment
        .machines
        .iter()
        .map(|machine| machine.as_str())
        .filter(|machine| !need.per_machine.contains_key(*machine))
        .collect();
    let mut each = footprint_line(&need.per_machine);
    if !unreported.is_empty() {
        if !each.is_empty() {
            each.push_str(", ");
        }
        each.push_str(&format!(
            "unknown on {} until it reports its memory",
            unreported.join(" and ")
        ));
    }
    match &need.basis {
        NeedBasis::Measured => format!(
            "- measured footprint (GPU-allocated once settled): {each}; {RESERVES}\n"
        ),
        NeedBasis::Stated => format!(
            "- memory need, as its profile states (`requirements.memory`): {each}; {RESERVES}, and its first load's measured footprint replaces it\n"
        ),
        NeedBasis::Predicted(how) => format!(
            "- memory need, predicted because nothing is measured or stated: {each}, as {how}; {RESERVES}, and its first load's measured footprint replaces it\n"
        ),
    }
}

/// What a cold load's line says of it.
fn cold_note(load: &LoadRow) -> &'static str {
    if load.cold {
        ", cold: its start downloaded what it serves first"
    } else {
        ""
    }
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
            "- {}: healthy after {}{}{waited}; settled footprint {}{}",
            at(view, load.finished_at),
            duration(ms),
            cold_note(load),
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
    machine: &cairn_inference::MachineName,
) -> String {
    let head = if *machine == deployment.head {
        "head; "
    } else {
        ""
    };
    let resident = view
        .models
        .pools
        .iter()
        .flat_map(|pool| &pool.residents)
        .chain(&view.models.stranded)
        .find(|resident| resident.machines.contains(machine.as_str()));
    let hold = match resident {
        Some(resident) if resident.deployment == deployment.id => {
            match resident.footprint.get(machine.as_str()) {
                Some(bytes) => format!("resident here, reserving {}", gib(*bytes)),
                None => "resident here, footprint unmeasured".to_string(),
            }
        }
        Some(resident) => format!(
            "{} (deployment {}) is resident here",
            resident.model, resident.deployment
        ),
        None => "no model resident".to_string(),
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
    let _ = writeln!(out, "- profile: {}", deployment.profile);
    let _ = writeln!(out, "- pool: {}", pool_line(view, &id));
    let _ = writeln!(out, "- phase: {}", phase_summary(view, &id));
    if let Some(resident) = view.models.resident(&id) {
        let _ = writeln!(
            out,
            "- resident: {}; {}",
            resident.why_said,
            stays(resident, view.models.now)
        );
    }
    out.push_str("- machines:\n");
    for machine in &deployment.machines {
        let _ = writeln!(out, "{}", machine_line(view, deployment, machine));
    }
    head_lines(&mut out, view, deployment);
    let loads = view.deployment_control.loads.get(&id);
    let latest = loads.and_then(|loads| {
        loads
            .iter()
            .find(|load| load.healthy && load.load_ms.is_some())
    });
    match latest {
        Some(load) => {
            let _ = write!(
                out,
                "- last measured load: {} to health{}, finished {}",
                duration(load.load_ms.unwrap_or_default()),
                cold_note(load),
                at(view, load.finished_at)
            );
            let reference = loads.and_then(|loads| inference_control::reference_load(loads));
            match reference.filter(|reference| !std::ptr::eq(*reference, load)) {
                Some(warm) => {
                    let _ = writeln!(
                        out,
                        "; its pool's windows weigh its latest load from cache instead, {} to health, finished {}",
                        duration(warm.load_ms.unwrap_or_default()),
                        at(view, warm.finished_at)
                    );
                }
                None => out.push_str("; its pool's windows weigh this load for it\n"),
            }
        }
        None => out.push_str("- last measured load: none yet\n"),
    }
    out.push_str(&need_line(view, deployment, loads));
    if let Some(loads) = loads.filter(|loads| !loads.is_empty()) {
        out.push_str("\n## Recent loads\n\n");
        for load in loads.iter().take(10) {
            let _ = writeln!(out, "{}", load_line(view, load));
        }
    }
    let _ = write!(
        out,
        "\n## Acting on it\n\n- start: `write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"start\"}}}}]}})` makes it a resident: placement reserves its footprint on every machine above at once, then runs the profile's start command on the head once each has room. Refused while another model is resident on one of its machines.\n- stop: the same with `action:\"stop\"` lets it go: the profile's stop command runs on the head until the health check stops answering. Refused while it is pinned. A request for it acquires it again.\n- To load it on demand instead, append its model to cairn://inference/requests; to keep it loaded, pin it on cairn://inference/pools.\n- While its model occupies its machines, a write to cairn://inference may not remove it or move it to other machines; stop it first. A changed start or stop command takes effect at the next start or stop.\n"
    );
    out
}

/// A resident's phase in a few words; the deployment's own page has the rest.
fn phase_label(phase: Option<&DeploymentPhase>) -> String {
    match phase {
        None => "chosen; the start is being asked for".to_string(),
        Some(DeploymentPhase::Reserving {
            waiting_on: Some(on),
            ..
        }) => format!("waiting to fit: {on}"),
        Some(DeploymentPhase::Failed { reason, .. }) => format!("failed: {reason}"),
        Some(phase) => phase.label().to_string(),
    }
}

/// Why a resident keeps its machines, or that it may go.
fn stays(resident: &ResidentView, now: i64) -> String {
    match &resident.hold {
        Some(hold) => format!("stays: {}", hold.describe(now)),
        None => "idle: evicted when work is refused its room".to_string(),
    }
}

fn resident_line(out: &mut String, resident: &ResidentView, now: i64) {
    let held = if resident.footprint.is_empty() {
        "footprint unmeasured".to_string()
    } else {
        let room = footprint_line(&resident.footprint);
        if resident.unified {
            format!("reserving {room}")
        } else {
            format!("{room} of VRAM, none of the room batches use")
        }
    };
    let _ = writeln!(
        out,
        "  - {} (`{}`): {}; {held}; {}; {}",
        resident.model,
        resident.deployment,
        phase_label(resident.phase.as_ref()),
        resident.why_said,
        stays(resident, now)
    );
}

fn pool_section(out: &mut String, view: &InferenceView, pool: &PoolView) {
    let models = &view.models;
    let now = models.now;
    let _ = writeln!(out, "## {}\n", pool.key);
    match pool.rate_gbps {
        Some(rate) => {
            let _ = writeln!(out, "- link: {rate} Gb/s between its machines");
        }
        None => out.push_str("- link: none; a machine with no fast link is a pool of its own\n"),
    }
    out.push_str("- machines:\n");
    for member in &pool.members {
        let name = MachineName::from(member.machine.as_str());
        let address = Addresses::new(&view.inventory, &view.network)
            .from_orch(&name)
            .map_or("address unknown".to_string(), |at| at.to_string());
        let link = member
            .link_address
            .map(|ip| format!("; link address {ip}"))
            .unwrap_or_default();
        let _ = writeln!(
            out,
            "  - `{}`: {}; {address}{link}",
            member.machine,
            enrolment(view, &name)
        );
    }
    if pool.residents.is_empty() {
        out.push_str("- residents: none\n");
    } else {
        out.push_str("- residents:\n");
        for resident in &pool.residents {
            resident_line(out, resident, now);
        }
    }
    let _ = writeln!(
        out,
        "- warm model: {}",
        pool.warm_model.as_deref().unwrap_or("none")
    );
    let _ = writeln!(
        out,
        "- idle window: {}; minimum residency {}; load bound {}",
        pool.settings.idle, pool.settings.min_residency, pool.settings.load_bound
    );
    if let Some(at) = pool.idle_since {
        let _ = writeln!(
            out,
            "- last batch work on its machines: {} ago",
            age((now - at) / 1_000)
        );
    }
    let _ = writeln!(
        out,
        "- deployable here: {}",
        if pool.models.is_empty() {
            "nothing".to_string()
        } else {
            pool.models.join(", ")
        }
    );
    out.push('\n');
}

pub(crate) fn render_pools(view: &InferenceView, params: &[QueryParam]) -> String {
    const URI: &str = "cairn://inference/pools";
    if let Some(error) = reject_params(URI, params, &[]) {
        return error;
    }
    let models: &ModelsView = &view.models;
    let mut out = String::from(
        "# Inference pools\n\nPlacement detects a pool as machines joined by a fast link, from the links the executors report; a GPU machine with no such link is a pool of its own. A model runs as a resident on one pool: while it loads, its machines keep its footprint off-limits to batches; once healthy, batches run beside it as measured room allows. An idle resident (no request for its idle window, past its minimum residency, unpinned) is evicted when work is refused its room, and a pool with no batch work for its idle window places its warm model.\n\n",
    );
    if models.pools.is_empty() {
        out.push_str("No pools detected: no attached machine reports a GPU.\n\n");
    }
    for pool in &models.pools {
        pool_section(&mut out, view, pool);
    }
    if !models.stranded.is_empty() {
        out.push_str("## Residents on no detected pool\n\nTheir machines no longer form the pool they were placed on (a member detached); they are held as they are until they stop.\n\n");
        for resident in &models.stranded {
            resident_line(&mut out, resident, models.now);
        }
        out.push('\n');
    }
    let refused: Vec<_> = models
        .deployments
        .iter()
        .filter_map(|p| Some((p, p.refused.as_ref()?)))
        .collect();
    if !refused.is_empty() {
        out.push_str("## Deployments placement cannot run now\n\n");
        for (placement, why) in refused {
            let _ = writeln!(
                out,
                "- {} (`{}`): {why}",
                placement.model, placement.deployment
            );
        }
        out.push('\n');
    }
    if !models.departures.is_empty() {
        out.push_str("## Recently left\n\n");
        for departure in models.departures.iter().take(5) {
            let _ = writeln!(out, "- {}", departure.describe(models.now));
        }
        out.push('\n');
    }
    let _ = write!(
        out,
        "## Setting them\n\nPatch this resource with `{{pool, warm?, idleSecs?, pin?, release?, revoke?}}`, naming the pool by its key:\n- `warm`: the model, by its real name, placed when the pool is idle; `null` clears it.\n- `idleSecs`: the pool's idle window in seconds; `null` derives it from the pool's measured load again.\n- `pin`: a model to keep resident: a standing lease, never evicted for room, acquired at once if absent. Another holder's pin on its machines refuses it.\n- `release`: `true` releases your pins on the pool, or name the model; `revoke` does the same for anyone's pin.\n\ne.g. `write({{changes:[{{target:\"{URI}\",mode:\"patch\",payload:{{pool:\"bginf-sp1+bginf-sp2\",warm:\"deepseek-ai/DeepSeek-V4-Flash\"}}}}]}})`\n"
    );
    out
}

/// `cairn://inference/requests`: every waiting request with what it waits on,
/// then the last week's answered ones, newest first.
pub(crate) fn render_requests(view: &InferenceView, params: &[QueryParam]) -> String {
    if let Some(error) = reject_params("cairn://inference/requests", params, &[]) {
        return error;
    }
    let models = &view.models;
    let mut out = String::from(
        "# Inference requests\n\nAppend {model} here to ask for a model by its real name (and {pool} to narrow it to one pool). A request leases the model's resident, loading it if it is absent, and waits until it is healthy, fails to load, or is cancelled; it is never served by another model.\n\n",
    );
    let (waiting, answered): (Vec<_>, Vec<_>) = models
        .requests
        .iter()
        .partition(|r| r.request.state == RequestState::Waiting);
    let _ = writeln!(out, "## Waiting ({})\n", waiting.len());
    if waiting.is_empty() {
        out.push_str("Nothing is waiting.\n");
    }
    for request in &waiting {
        let _ = writeln!(out, "- #{} {}", request.request.id, request.said);
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
    for answered in answered.iter().rev() {
        let request = &answered.request;
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
            request.id, request.model, request.pool, request.requester
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
        parts.push("stale: the profile has changed since".into());
    }
    format!("`{}`: {}", row.id, parts.join("; "))
}

/// The newest verification of `profile` on each pool.
fn latest_per_pool<'a>(view: &'a InferenceView, profile: &str) -> Vec<&'a VerificationRow> {
    let mut seen = std::collections::BTreeSet::new();
    view.verifications
        .iter()
        .filter(|row| row.profile == profile && seen.insert(row.pool.clone()))
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

fn profile_summary(profile: &InferenceProfile) -> String {
    let server = match &profile.engine {
        Engine::Container(container) => container.server.as_str(),
        other => other.kind(),
    };
    format!(
        "{}, {server}, x{}, {}",
        profile
            .quantization
            .as_deref()
            .unwrap_or("quantization unknown"),
        profile.requirements.machines,
        profile.origin.label()
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
        let profiles: Vec<String> = view
            .inventory
            .profiles
            .values()
            .filter(|r| r.model.name == model.name)
            .map(|r| format!("`{}`", r.id))
            .collect();
        let _ = writeln!(
            out,
            "- `{}`: {}; profiles: {}",
            model.name,
            model_line(model),
            if profiles.is_empty() {
                "none".to_string()
            } else {
                profiles.join(", ")
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
    out.push_str("\n## Profiles\n\n");
    let profiles: Vec<&InferenceProfile> = view
        .inventory
        .profiles
        .values()
        .filter(|r| r.model.name == model.name)
        .collect();
    if profiles.is_empty() {
        out.push_str(
            "None yet: import one or generate candidates at cairn://inference/profiles.\n",
        );
    }
    for profile in profiles {
        let _ = writeln!(out, "- `{}`: {}", profile.id, profile_summary(profile));
        for row in latest_per_pool(view, profile.id.as_str()) {
            let _ = writeln!(out, "  - {}", verification_line(view, row));
        }
    }
    out
}

pub(crate) fn render_profile(view: &InferenceView, id: &str, params: &[QueryParam]) -> String {
    let uri = format!("cairn://inference/profiles/{id}");
    if let Some(error) = reject_params(&uri, params, &[]) {
        return error;
    }
    let Some(profile) = view
        .inventory
        .profiles
        .get(&cairn_inference::InferenceProfileId::from(id))
    else {
        return format!("There is no profile {id}. cairn://inference/profiles lists them.");
    };
    let mut out = String::new();
    profile_section(&mut out, profile);
    let addresses = Addresses::new(&view.inventory, &view.network);
    for deployment in view
        .inventory
        .deployments
        .values()
        .filter(|d| d.profile == profile.id)
    {
        let machines = addresses.machines_at(deployment);
        let _ = writeln!(out, "## As deployment {}\n", deployment.id);
        match cairn_inference::render::render(profile, deployment, &machines) {
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
        .filter(|r| r.profile == id)
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
        out.push_str("None yet. Patch a profile or a model with verify to load it on a pool and measure it.\n");
    }
    for row in &view.verifications {
        let _ = writeln!(
            out,
            "- profile `{}`: {}",
            row.profile,
            verification_line(view, row)
        );
    }
    out
}

pub(crate) fn render_verification(view: &InferenceView, id: &str, params: &[QueryParam]) -> String {
    let uri = format!("cairn://inference/verifications/{id}");
    if let Some(error) = reject_params(&uri, params, &["format"]) {
        return error;
    }
    let as_data = match find_query_value(params, "format") {
        None => false,
        Some("json") => true,
        Some(other) => return format!("Unknown format='{other}' for {uri}; expected json."),
    };
    let Some(row) = view.verifications.iter().find(|r| r.id == id) else {
        return format!(
            "There is no verification {id}. cairn://inference/verifications lists them."
        );
    };
    if as_data {
        return serde_json::to_string_pretty(row).unwrap_or_default();
    }
    let mut out = format!("# Verification {id}\n\n");
    let _ = writeln!(
        out,
        "- profile: `{}` (cairn://inference/profiles/{})",
        row.profile, row.profile
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
    use crate::fleet::models::requests::{Hold, HoldCause, Request, Wait};
    use crate::fleet::models::rules::Why;
    use crate::fleet::models::settings::EffectiveSettings;
    use crate::fleet::models::{DeploymentPlacement, RequestView};
    use crate::fleet::pools::PoolMember;
    use cairn_inference::{Inventory, Network, RecordSource};
    use std::collections::BTreeMap;

    const NOW: i64 = 1_800_000_000_000;

    const INVENTORY: &str = r#"
models:
  - {name: deepseek-ai/DeepSeek-V4-Flash}
  - {name: nvidia/Qwen3.6-35B}
  - {name: Qwen/Qwen3-Embedding-4B}
profiles:
  - {id: dsv4f-tp2, model: {name: deepseek-ai/DeepSeek-V4-Flash}, servedName: dsv4f-tp2, engine: {kind: command, start: ./start.sh --port 8888, stop: ./stop.sh, description: vllm}, requirements: {machines: 2}}
  - {id: qwen36-fast, model: {name: nvidia/Qwen3.6-35B}, servedName: qwen36-fast, engine: {kind: command, start: ./start.sh --port 8001, stop: ./stop.sh}, requirements: {machines: 1}}
  - {id: qwen3-embed-4b, model: {name: Qwen/Qwen3-Embedding-4B}, servedName: qwen3-embed-4b, engine: {kind: command, start: ./start.sh --port 8090, stop: ./stop.sh}, requirements: {machines: 1}}
deployments:
  - {id: dsv4f-tp2, profile: dsv4f-tp2, machines: [sp1, sp2], head: sp1, apiPort: 8888, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen36-fast, profile: qwen36-fast, machines: [sp1], head: sp1, apiPort: 8001, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen3-embed-4b, profile: qwen3-embed-4b, machines: [fedora], head: fedora, apiPort: 8090, apiPath: /v1, healthPath: /health}
machines:
  - {name: sp1, address: 192.0.2.40}
  - {name: fedora, address: 192.0.2.43}
"#;

    fn view() -> InferenceView {
        InferenceView {
            inventory: Inventory::from_yaml(INVENTORY).unwrap(),
            retired_link_setting: false,
            executors: BTreeMap::from([("fedora".to_string(), true)]),
            network: Network::default(),
            models: ModelsView {
                now: NOW,
                ..ModelsView::default()
            },
            deployment_control: Default::default(),
            verifications: Vec::new(),
            now_ms: NOW,
        }
    }

    fn member(machine: &str) -> PoolMember {
        PoolMember {
            machine: machine.into(),
            link_address: None,
        }
    }

    /// The Sparks joined at 200 Gb/s with DeepSeek healthy and requested a
    /// minute ago, a request for the Qwen model queued behind it, and halo's
    /// embedder unable to run.
    fn placed_view() -> InferenceView {
        let mut view = view();
        let deepseek = ResidentView {
            deployment: "dsv4f-tp2".into(),
            model: "deepseek-ai/DeepSeek-V4-Flash".into(),
            pool: "sp1+sp2".into(),
            machines: ["sp1".to_string(), "sp2".to_string()].into(),
            phase: Some(DeploymentPhase::Healthy {
                since_ms: NOW - 30 * 60_000,
                load_ms: None,
            }),
            why: Why::Adopted,
            why_said: "adopted as it was running when placement started, 30m ago".into(),
            chosen_at: NOW - 30 * 60_000,
            loading_since: None,
            healthy_since: Some(NOW - 30 * 60_000),
            hold: Some(Hold {
                deployment: "dsv4f-tp2".into(),
                model: "deepseek-ai/DeepSeek-V4-Flash".into(),
                because: HoldCause::RecentRequest,
                until: Some(NOW + 9 * 60_000),
            }),
            pin: None,
            footprint: BTreeMap::from([("sp1".into(), 100 << 30), ("sp2".into(), 98 << 30)]),
            unified: true,
        };
        let queued = Request {
            id: 2,
            pool: "sp1+sp2".into(),
            deployment: "qwen36-fast".into(),
            model: "nvidia/Qwen3.6-35B".into(),
            requester: "cairn://p/cairn/9/1/builder".into(),
            at: NOW - 30_000,
            state: RequestState::Waiting,
        };
        let wait = Wait::Held {
            by: vec![deepseek.hold.clone().unwrap()],
        };
        view.models = ModelsView {
            now: NOW,
            passed_at: NOW,
            pools: vec![PoolView {
                key: "sp1+sp2".into(),
                members: vec![member("sp1"), member("sp2")],
                rate_gbps: Some(200),
                settings: EffectiveSettings::derive(Some(600), None),
                warm_model: Some("deepseek-ai/DeepSeek-V4-Flash".into()),
                idle_override_secs: Some(600),
                idle_since: Some(NOW - 60_000),
                residents: vec![deepseek],
                models: vec![
                    "deepseek-ai/DeepSeek-V4-Flash".into(),
                    "nvidia/Qwen3.6-35B".into(),
                ],
            }],
            stranded: Vec::new(),
            deployments: vec![
                DeploymentPlacement {
                    deployment: "dsv4f-tp2".into(),
                    model: "deepseek-ai/DeepSeek-V4-Flash".into(),
                    pool: Some("sp1+sp2".into()),
                    refused: None,
                },
                DeploymentPlacement {
                    deployment: "qwen3-embed-4b".into(),
                    model: "Qwen/Qwen3-Embedding-4B".into(),
                    pool: None,
                    refused: Some(
                        "fedora is in no detected pool: it is not attached or reports no GPU"
                            .into(),
                    ),
                },
            ],
            requests: vec![
                RequestView {
                    request: Request {
                        id: 1,
                        deployment: "dsv4f-tp2".into(),
                        model: "deepseek-ai/DeepSeek-V4-Flash".into(),
                        at: NOW - 60_000,
                        state: RequestState::Ready { at: NOW - 60_000 },
                        ..queued.clone()
                    },
                    wait: None,
                    said: String::new(),
                },
                RequestView {
                    said: crate::fleet::models::requests::describe_waiting(&queued, &wait, NOW),
                    request: queued,
                    wait: Some(wait),
                },
            ],
            departures: Vec::new(),
        };
        view
    }

    #[test]
    fn the_model_and_profile_pages_show_facts_profiles_and_verifications() {
        let mut view = view();
        let profile = view.inventory.profiles.values().next().unwrap().clone();
        let model = profile.model.name.clone();
        let row = VerificationRow {
            id: "v1".into(),
            profile: profile.id.to_string(),
            profile_digest: crate::orchestrator::inference_verify::digest(&profile),
            pool: "sparks".into(),
            deployment: format!("verify-{}-sparks", profile.id),
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
        assert!(page.contains(&format!("- `{}`: ", profile.id)), "{page}");
        assert!(page.contains("passed on pool sparks (bginf-sp1)"), "{page}");
        assert!(
            page.contains("loaded in 6m 40s; memory bginf-sp1 100.0 GiB"),
            "{page}"
        );
        assert!(page.contains("decode 41.5 tok/s"), "{page}");
        assert!(!page.contains("stale"), "{page}");

        let page = render_profile(&view, profile.id.as_str(), &[]);
        assert!(page.contains("## Verifications"), "{page}");
        assert!(page.contains("`v1`: passed"), "{page}");

        view.inventory.profiles.get_mut(&profile.id).unwrap().notes = Some("changed".into());
        let page = render_verifications(&view, &[]);
        assert!(
            page.contains("stale: the profile has changed since"),
            "{page}"
        );
        assert!(render_model(&view, "nobody/Nothing", &[])
            .starts_with("There is no model nobody/Nothing"));
    }

    #[test]
    fn the_pools_page_shows_each_detected_pool_its_residents_and_settings() {
        let page = render_pools(&placed_view(), &[]);
        for line in [
            "## sp1+sp2\n",
            "- link: 200 Gb/s between its machines",
            "  - `sp1`: not enrolled; 192.0.2.40 (record)\n",
            "  - `sp2`: not enrolled; address unknown\n",
            "  - deepseek-ai/DeepSeek-V4-Flash (`dsv4f-tp2`): healthy; reserving sp1 100.0 GiB, sp2 98.0 GiB; adopted as it was running when placement started, 30m ago; stays: deepseek-ai/DeepSeek-V4-Flash was requested within its idle window and keeps its machines, until in 9m",
            "- warm model: deepseek-ai/DeepSeek-V4-Flash",
            "- idle window: 10m (operator); minimum residency 20m (built-in; no load measured yet); load bound 30m (built-in; no load measured yet)",
            "- last batch work on its machines: 1m ago",
            "## Deployments placement cannot run now",
            "- Qwen/Qwen3-Embedding-4B (`qwen3-embed-4b`): fedora is in no detected pool",
            "Patch this resource with `{pool, warm?, idleSecs?, pin?, release?, revoke?}`",
        ] {
            assert!(page.contains(line), "{line}\n---\n{page}");
        }
        assert!(!page.contains("controlled"), "{page}");
    }

    #[test]
    fn no_pool_says_why() {
        let page = render_pools(&view(), &[]);
        assert!(
            page.contains("No pools detected: no attached machine reports a GPU."),
            "{page}"
        );
        let index = render_index(&view(), &[]);
        assert!(index.contains("- `cairn://inference/pools`"), "{index}");
        assert!(
            index.contains("- records: 3 profiles, 3 deployments, 2 machines"),
            "{index}"
        );
    }

    #[test]
    fn the_index_says_what_is_left_of_bginf_until_it_is_cleared() {
        let mut view = view();
        assert!(!render_index(&view, &[]).contains("bginf"));
        view.inventory
            .machines
            .get_mut(&"fedora".into())
            .unwrap()
            .source = RecordSource::Bginf;
        view.retired_link_setting = true;
        let index = render_index(&view, &[]);
        assert!(
            index.contains("- 1 records are still mirrored from bginf and read-only (machine fedora (bginf)); adopt them as Cairn's own"),
            "{index}"
        );
        assert!(index.contains("payload:{adopt:\"bginf\"}"), "{index}");
        assert!(
            index.contains("settings.yaml still names the retired bginf link"),
            "{index}"
        );
    }

    #[test]
    fn the_requests_page_names_the_model_the_pool_and_the_wait() {
        let page = render_requests(&placed_view(), &[]);
        assert!(page.contains("## Waiting (1)"), "{page}");
        assert!(
            page.contains("- #2 nvidia/Qwen3.6-35B (deployment qwen36-fast) on pool sp1+sp2 for cairn://p/cairn/9/1/builder, waiting 30s: deepseek-ai/DeepSeek-V4-Flash was requested within its idle window and keeps its machines, until in 9m"),
            "{page}"
        );
        assert!(page.contains("## Answered in the last 7d (1)"), "{page}");
        assert!(
            page.contains("- #1 deepseek-ai/DeepSeek-V4-Flash on pool sp1+sp2 for cairn://p/cairn/9/1/builder: ready 1m ago"),
            "{page}"
        );
    }

    #[test]
    fn profiles_and_deployments_state_their_unknowns() {
        let view = view();
        let profiles = render_profiles(&view, &[]);
        assert!(profiles.contains("- verbs: unknown\n"), "{profiles}");
        assert!(profiles.contains("- source: cairn\n"), "{profiles}");
        assert!(profiles.contains("  - accelerator: unknown"), "{profiles}");
        assert!(
            profiles.contains("    ./start.sh --port 8888"),
            "{profiles}"
        );
        let deployments = render_deployments(&view, &[]);
        assert!(
            deployments.contains(
                "- head address: 192.0.2.43 (record)\n- API base: http://192.0.2.43:8090/v1"
            ),
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

    /// An attached executor with no machine record is reached where its
    /// connection came from, and the page says so; one neither recorded nor
    /// attached says what would give it an address.
    #[test]
    fn a_head_s_address_names_where_it_came_from() {
        let mut view = view();
        view.inventory.machines.clear();
        view.network
            .connections
            .insert("fedora".into(), "10.10.10.10".parse().unwrap());
        let deployments = render_deployments(&view, &[]);
        assert!(
            deployments.contains(
                "- head address: 10.10.10.10 (observed from its connection)\n- API base: http://10.10.10.10:8090/v1\n- health: http://10.10.10.10:8090/health\n"
            ),
            "{deployments}"
        );
        let page = render_deployment(&view, "qwen36-fast", &[]);
        assert!(
            page.contains("- API base and health: unknown, so Cairn cannot start or probe it (port 8001, API path /v1, health path /v1/models): the head sp1's address is unknown: no executor named sp1 is attached and it has no machine record; enrol it as an executor or add a machine record\n"),
            "{page}"
        );
    }

    #[test]
    fn a_deployment_page_shows_its_phase_residency_memory_and_loads() {
        use crate::orchestrator::inference_control::{MachineReading, StateRow};
        let mut view = placed_view();
        let page = render_deployment(&view, "qwen3-embed-4b", &[]);
        assert!(
            page.contains("- phase: never acted on by Cairn; it is probed once, and adopted as a resident if it answers"),
            "{page}"
        );
        assert!(
            page.contains("- pool: none: fedora is in no detected pool"),
            "{page}"
        );
        let page = render_deployment(&view, "dsv4f-tp2", &[]);
        assert!(page.contains("- last measured load: none yet"), "{page}");
        assert!(
            page.contains("- memory need, predicted because nothing is measured or stated: unknown on sp1 and sp2 until it reports its memory, as vLLM pre-allocates 0.90"),
            "{page}"
        );
        let id = cairn_inference::InferenceProfileId::from("dsv4f-tp2");
        let set_description = |view: &mut InferenceView, to: Option<&str>| {
            if let Some(cairn_inference::Engine::Command { description, .. }) =
                view.inventory.profiles.get_mut(&id).map(|p| &mut p.engine)
            {
                *description = to.map(str::to_string);
            }
        };
        let described = view.inventory.profiles[&id].engine.clone();
        set_description(&mut view, None);
        let page = render_deployment(&view, "dsv4f-tp2", &[]);
        assert!(
            page.contains(
                "- memory need: unknown, so a start is refused: its profile states no memory"
            ),
            "{page}"
        );
        if let Some(profile) = view.inventory.profiles.get_mut(&id) {
            profile.engine = described;
        }
        view.deployment_control.readings.insert(
            "sp1".into(),
            MachineReading {
                total_bytes: Some(122 << 30),
                ..MachineReading::default()
            },
        );
        let page = render_deployment(&view, "dsv4f-tp2", &[]);
        assert!(
            page.contains("- memory need, predicted because nothing is measured or stated: sp1 109.8 GiB, unknown on sp2 until it reports its memory, as vLLM pre-allocates 0.90 of each machine's memory, its default"),
            "{page}"
        );

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
                total_bytes: Some(122 << 30),
            },
        );
        view.deployment_control.loads.insert(
            "dsv4f-tp2".into(),
            vec![LoadRow {
                profile: "dsv4f-tp2".into(),
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
                cold: false,
            }],
        );
        let page = render_deployment(&view, "dsv4f-tp2", &[]);
        assert!(
            page.contains(
                "- phase: healthy since 2027-01-15 07:58:00 UTC (2m ago); the load took 5m 12s"
            ),
            "{page}"
        );
        assert!(page.contains("- pool: `sp1+sp2`"), "{page}");
        assert!(
            page.contains("- resident: adopted as it was running when placement started, 30m ago; stays: deepseek-ai/DeepSeek-V4-Flash was requested within its idle window"),
            "{page}"
        );
        assert!(
            page.contains("  - `sp1` (head; not enrolled); resident here, reserving 100.0 GiB; GPU-allocated now 100.0 GiB"),
            "{page}"
        );
        assert!(
            page.contains("  - `sp2` (not enrolled); resident here, reserving 98.0 GiB; no GPU reading (not attached)"),
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
            page.contains("- last measured load: 5m 12s to health, finished 2027-01-15 07:58:00 UTC (2m ago); its pool's windows weigh this load for it\n"),
            "{page}"
        );
        assert!(
            page.contains(
                "a write to cairn://inference may not remove it or move it to other machines"
            ),
            "{page}"
        );

        // A newer load that pulled its image first: the windows keep
        // weighing the load from cache.
        let mut pulled = view.deployment_control.loads[&DeploymentId::from("dsv4f-tp2")][0].clone();
        pulled.finished_at = NOW - 60_000;
        pulled.load_ms = Some(611_000);
        pulled.cold = true;
        view.deployment_control
            .loads
            .get_mut(&DeploymentId::from("dsv4f-tp2"))
            .unwrap()
            .insert(0, pulled);
        let page = render_deployment(&view, "dsv4f-tp2", &[]);
        assert!(
            page.contains("- last measured load: 10m 11s to health, cold: its start downloaded what it serves first, finished 2027-01-15 07:59:00 UTC (1m ago); its pool's windows weigh its latest load from cache instead, 5m 12s to health, finished 2027-01-15 07:58:00 UTC (2m ago)\n"),
            "{page}"
        );
        assert!(
            page.contains("healthy after 10m 11s, cold: its start downloaded what it serves first; waited 8s for its machines"),
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
