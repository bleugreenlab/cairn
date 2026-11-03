//! The profile generator's reads: the tools its agent and its workflow script
//! read facts through (a model's files, a pool's hardware, the fit, the
//! templates and a starting profile from one), and the generations
//! themselves. Each answers markdown, and `?format=json` the same answer as
//! data, which is what the workflow script parses.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use cairn_common::query::QueryParam;
use cairn_inference::fit::{templates, Template};
use cairn_inference::hf::{AuxiliaryFile, RepoFiles};
use cairn_inference::{control::gib, Model};
use serde::Serialize;

use super::common::find_query_value;
use crate::orchestrator::inference::load_inventory;
use crate::orchestrator::inference_generate::{
    self as generate, FitAnswer, GenerationView, PoolFacts, StatedHardware,
};
use crate::orchestrator::inference_models::Web;
use crate::orchestrator::Orchestrator;

fn reject(uri: &str, params: &[QueryParam], allowed: &[&str]) -> Option<String> {
    let unknown = params.iter().find(|p| !allowed.contains(&p.key.as_str()))?;
    Some(format!(
        "Unsupported query parameter '{}' for {uri}; it reads {}.",
        unknown.key,
        if allowed.is_empty() {
            "none".to_string()
        } else {
            allowed.join(", ")
        }
    ))
}

/// Whether the read asked for data, or why its format is not one.
fn json(uri: &str, params: &[QueryParam]) -> Result<bool, String> {
    match find_query_value(params, "format") {
        None => Ok(false),
        Some("json") => Ok(true),
        Some(other) => Err(format!(
            "Unknown format='{other}' for {uri}; expected json."
        )),
    }
}

fn data<T: Serialize>(value: &T) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|error| format!("{{\"error\":\"{error}\"}}"))
}

fn required<'a>(uri: &str, params: &'a [QueryParam], key: &str) -> Result<&'a str, String> {
    find_query_value(params, key)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{uri} needs ?{key}="))
}

async fn registered(orch: &Orchestrator, name: &str) -> Result<Model, String> {
    load_inventory(&orch.db.local)
        .await?
        .models
        .get(name)
        .cloned()
        .ok_or_else(|| {
            format!("model {name} is not registered; append it to cairn://inference/models first")
        })
}

/// The hardware a `?generation=` read states for machines that report none.
async fn stated(
    orch: &Orchestrator,
    params: &[QueryParam],
) -> Result<BTreeMap<String, StatedHardware>, String> {
    match find_query_value(params, "generation") {
        None => Ok(BTreeMap::new()),
        Some(id) => Ok(generate::get_generation(&orch.db.local, id)
            .await?
            .ok_or_else(|| format!("there is no generation {id}"))?
            .hardware),
    }
}

async fn files_of(model: &Model) -> Result<(RepoFiles, Vec<String>), String> {
    let hub = Web::new()?;
    generate::model_files(&hub, model).await
}

fn aux_line(out: &mut String, file: &AuxiliaryFile) {
    let _ = writeln!(
        out,
        "- `{}` in {}@{} ({}{})",
        file.file.path,
        file.repo,
        &file.revision[..file.revision.len().min(12)],
        gib(file.file.size),
        file.quantization
            .as_deref()
            .map(|q| format!(", {q}"))
            .unwrap_or_default()
    );
}

/// `cairn://inference/models/<name>/files`.
pub(crate) async fn render_model_files(
    orch: &Orchestrator,
    name: &str,
    params: &[QueryParam],
) -> String {
    let uri = format!("cairn://inference/models/{name}/files");
    if let Some(error) = reject(&uri, params, &["format"]) {
        return error;
    }
    let as_data = match json(&uri, params) {
        Ok(as_data) => as_data,
        Err(error) => return error,
    };
    let model = match registered(orch, name).await {
        Ok(model) => model,
        Err(error) => return error,
    };
    let (files, skipped) = match files_of(&model).await {
        Ok(read) => read,
        Err(error) => return format!("{name}'s files could not be read: {error}"),
    };
    if as_data {
        return data(&serde_json::json!({ "files": files, "unreadDerivatives": skipped }));
    }
    let mut out = format!(
        "# {name}: weight files\n\nIts own repository and the quantized derivatives its organisation or a trusted quantizer publishes, each at its current revision. A source key (`<repo>@<quantization>`) is what cairn://inference/fit and cairn://inference/templates/<id> take as `source`.\n\n## Weight sources\n\n"
    );
    if files.sources.is_empty() {
        out.push_str("None.\n");
    }
    for source in &files.sources {
        let _ = writeln!(
            out,
            "- `{}`: {:?}, {} in {} file(s) at revision {}",
            source.key,
            source.format,
            gib(source.bytes()),
            source.files.len(),
            source.revision
        );
        if source.files.len() > 1 {
            for file in &source.files {
                let _ = writeln!(out, "  - `{}` ({})", file.path, gib(file.size));
            }
        }
    }
    out.push_str("\n## Vision projectors\n\nllama.cpp loads one beside the weights with `--mmproj`; without one a profile takes text only.\n\n");
    if files.projectors.is_empty() {
        out.push_str("None.\n");
    }
    for file in &files.projectors {
        aux_line(&mut out, file);
    }
    out.push_str("\n## Draft heads\n\nSpeculative-decoding heads (multi-token prediction, DFlash). Never a weight source; deliberately unused.\n\n");
    if files.draft_heads.is_empty() {
        out.push_str("None.\n");
    }
    for file in &files.draft_heads {
        aux_line(&mut out, file);
    }
    if !files.other.is_empty() {
        out.push_str("\n## Other files\n\nImportance matrices and incomplete shard sets: not weights a profile can load.\n\n");
        for file in &files.other {
            aux_line(&mut out, file);
        }
    }
    if !skipped.is_empty() {
        let _ = writeln!(
            out,
            "\nDerivatives that could not be read: {}",
            skipped.join("; ")
        );
    }
    out
}

/// `cairn://inference/pools/<pool>/hardware`.
pub(crate) async fn render_pool_hardware(
    orch: &Orchestrator,
    pool: &str,
    params: &[QueryParam],
) -> String {
    let uri = format!("cairn://inference/pools/{pool}/hardware");
    if let Some(error) = reject(&uri, params, &["format", "generation"]) {
        return error;
    }
    let as_data = match json(&uri, params) {
        Ok(as_data) => as_data,
        Err(error) => return error,
    };
    let facts = match pool_facts(orch, pool, params).await {
        Ok(facts) => facts,
        Err(error) => return error,
    };
    if as_data {
        return data(&facts);
    }
    let mut out = format!(
        "# Pool {}: hardware\n\n- link: {}\n- driver and CUDA versions: not reported by executors\n\n",
        facts.pool,
        match (facts.linked, facts.link_gbps) {
            (true, Some(gbps)) => format!("{gbps} Gb/s between its machines, so a model may split across them"),
            (true, None) => "linked".into(),
            (false, _) => "none; a model runs on one machine".into(),
        }
    );
    for machine in &facts.machines {
        let _ = writeln!(
            out,
            "## {}\n\n- GPU: {} ({}){}\n- memory: {} {}, of which an engine may take {}\n- CPU: {:?}\n- address: {}{}\n{}",
            machine.name,
            if machine.gpu.is_empty() { "unnamed" } else { &machine.gpu },
            machine.family.as_str(),
            if machine.blackwell { ", Blackwell (FP4 kernels)" } else { "" },
            gib(machine.memory_bytes),
            machine.memory,
            gib(machine.usable_bytes),
            machine.arch,
            machine.address.as_deref().unwrap_or("unknown"),
            machine
                .link_address
                .as_deref()
                .map(|link| format!("; link address {link}"))
                .unwrap_or_default(),
            if machine.stated { "- stated by the operator, not reported\n" } else { "" },
        );
    }
    out
}

async fn pool_facts(
    orch: &Orchestrator,
    pool: &str,
    params: &[QueryParam],
) -> Result<PoolFacts, String> {
    let stated = stated(orch, params).await?;
    let (detected, hardware) = generate::hardware_of(orch, pool, &stated)?;
    let inventory = load_inventory(&orch.db.local).await?;
    Ok(generate::pool_facts(
        &detected,
        &hardware,
        &inventory,
        &orch.fleet.network(),
        &stated,
    ))
}

/// `cairn://inference/fit?model=&pool=&source=`.
pub(crate) async fn render_fit(orch: &Orchestrator, params: &[QueryParam]) -> String {
    const URI: &str = "cairn://inference/fit";
    if let Some(error) = reject(
        URI,
        params,
        &["model", "pool", "source", "generation", "format"],
    ) {
        return error;
    }
    let answer = async {
        let as_data = json(URI, params)?;
        let model = registered(orch, required(URI, params, "model")?).await?;
        let pool = required(URI, params, "pool")?;
        let stated = stated(orch, params).await?;
        let (detected, hardware) = generate::hardware_of(orch, pool, &stated)?;
        let (files, _) = files_of(&model).await?;
        let fits = generate::fit_sources(
            &model,
            &files,
            find_query_value(params, "source"),
            &hardware,
        )?;
        Ok::<_, String>((as_data, model, detected.key.0, fits))
    }
    .await;
    let (as_data, model, pool, fits) = match answer {
        Ok(answer) => answer,
        Err(error) => return error,
    };
    if as_data {
        return data(&serde_json::json!({ "model": model.name, "pool": pool, "fits": fits }));
    }
    let mut out = format!(
        "# Fit of {} on pool {pool}\n\nPredicted, not measured: verification decides.\n\n",
        model.name
    );
    for fit in &fits {
        fit_line(&mut out, fit);
    }
    out
}

fn fit_line(out: &mut String, fit: &FitAnswer) {
    match (&fit.predicted, &fit.why) {
        (Some(p), _) => {
            let _ = writeln!(
                out,
                "- `{}` ({} of weights): fits on {} machine(s), {} a machine, context {}{}",
                fit.source,
                gib(fit.weight_bytes),
                p.machines,
                gib(p.per_machine_bytes),
                p.context,
                match p.kv_tokens {
                    Some(tokens) => format!(" (KV cache for {tokens} tokens)"),
                    None => " (KV geometry unknown: verification clamps the context to the cache the engine measures)".into(),
                }
            );
        }
        (None, why) => {
            let _ = writeln!(
                out,
                "- `{}` ({} of weights): does not fit: {}",
                fit.source,
                gib(fit.weight_bytes),
                why.as_deref().unwrap_or("unknown")
            );
        }
    }
}

fn template_lines(out: &mut String, template: &Template) {
    let _ = writeln!(
        out,
        "## {}\n\n- server: {}\n- hardware: {} on {:?}{}\n- weights: {:?}\n- image: `{}`\n- command: `{}`\n",
        template.id,
        template.server.as_str(),
        template.family.as_str(),
        template.runtime,
        template
            .arch
            .map(|arch| format!(", {arch:?} only"))
            .unwrap_or_default(),
        template.formats,
        template.image,
        template.command.join(" ")
    );
}

/// `cairn://inference/templates`.
pub(crate) fn render_templates(params: &[QueryParam]) -> String {
    const URI: &str = "cairn://inference/templates";
    if let Some(error) = reject(URI, params, &["format"]) {
        return error;
    }
    match json(URI, params) {
        Err(error) => return error,
        Ok(true) => return data(&templates()),
        Ok(false) => {}
    }
    let mut out = String::from(
        "# Inference templates\n\nEngines Cairn ships as known-good starting points. Read `cairn://inference/templates/<id>?model=&pool=&source=` for one made into a whole profile.\n\n",
    );
    for template in templates() {
        template_lines(&mut out, template);
    }
    out
}

/// `cairn://inference/templates/<id>`, and with model, pool and source a
/// starting profile as Cairn's record YAML.
pub(crate) async fn render_template(
    orch: &Orchestrator,
    id: &str,
    params: &[QueryParam],
) -> String {
    let uri = format!("cairn://inference/templates/{id}");
    if let Some(error) = reject(
        &uri,
        params,
        &["model", "pool", "source", "projector", "generation"],
    ) {
        return error;
    }
    if find_query_value(params, "model").is_none() {
        return match templates().iter().find(|t| t.id == id) {
            Some(template) => {
                let mut out = String::from("# Inference template\n\n");
                template_lines(&mut out, template);
                out.push_str("Add `?model=<name>&pool=<pool>&source=<repo>@<quant>` for a starting profile.\n");
                out
            }
            None => format!("there is no template {id}; cairn://inference/templates lists them"),
        };
    }
    let profile = async {
        let model = registered(orch, required(&uri, params, "model")?).await?;
        let stated = stated(orch, params).await?;
        let (_, hardware) = generate::hardware_of(orch, required(&uri, params, "pool")?, &stated)?;
        let (files, _) = files_of(&model).await?;
        generate::starting_profile(
            id,
            &model,
            &files,
            required(&uri, params, "source")?,
            find_query_value(params, "projector"),
            &hardware,
        )
    }
    .await;
    match profile {
        Ok(profile) => match serde_yaml::to_string(&profile) {
            Ok(yaml) => format!(
                "# Starting profile from template {id}\n\nA starting point: keep it or change it, then write it as `generated.profile`. Its memory need is what the fit predicts; verification measures the real one.\n\n```yaml\n{yaml}```\n"
            ),
            Err(error) => format!("the starting profile did not serialize: {error}"),
        },
        Err(error) => error,
    }
}

fn generation_section(out: &mut String, view: &GenerationView) {
    let row = &view.row;
    let _ = writeln!(
        out,
        "## {}\n\n- model: {}\n- pool: {}\n- state: {}{}\n- run: {}",
        row.id,
        row.model,
        row.pool,
        view.status,
        view.phase
            .as_deref()
            .map(|phase| format!(" (phase {phase})"))
            .unwrap_or_default(),
        row.node_uri
    );
    if let Some(log) = &view.last_log {
        let _ = writeln!(out, "- latest: {log}");
    }
    if let Some(question) = &view.pending_question {
        let _ = writeln!(out, "- waiting on the operator: {}", question.uri);
    }
    if let Some(draft) = &row.draft_profile {
        let _ = writeln!(out, "- draft profile: cairn://inference/profiles/{draft}");
    }
    if !row.hardware.is_empty() {
        let _ = writeln!(
            out,
            "- hardware stated for: {}",
            row.hardware.keys().cloned().collect::<Vec<_>>().join(", ")
        );
    }
    if let Some(outcome) = &row.outcome {
        if let Some(why) = outcome.get("why").and_then(serde_json::Value::as_str) {
            let _ = writeln!(out, "- why: {why}");
        }
        if let Some(attempts) = outcome
            .get("attempts")
            .and_then(serde_json::Value::as_array)
        {
            for (n, attempt) in attempts.iter().enumerate() {
                let _ = writeln!(
                    out,
                    "- attempt {}: {}",
                    n + 1,
                    attempt
                        .get("reason")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("no reason recorded")
                );
            }
        }
    }
    out.push('\n');
}

/// `cairn://inference/generations`.
pub(crate) async fn render_generations(orch: &Orchestrator, params: &[QueryParam]) -> String {
    const URI: &str = "cairn://inference/generations";
    if let Some(error) = reject(URI, params, &["format"]) {
        return error;
    }
    let as_data = match json(URI, params) {
        Ok(as_data) => as_data,
        Err(error) => return error,
    };
    let views = match generate::generation_views(orch).await {
        Ok(views) => views,
        Err(error) => return format!("the generations could not be read: {error}"),
    };
    if as_data {
        return data(&views);
    }
    let mut out = String::from(
        "# Inference profile generations\n\nEach is an agent session that researched how to run a model on a pool, asked the operator, wrote a profile and had verification decide (tenet 1364). Started from Settings > Inference.\n\n",
    );
    if views.is_empty() {
        out.push_str("None yet.\n");
    }
    for view in &views {
        generation_section(&mut out, view);
    }
    out
}

/// `cairn://inference/generations/<id>`.
pub(crate) async fn render_generation(
    orch: &Orchestrator,
    id: &str,
    params: &[QueryParam],
) -> String {
    let uri = format!("cairn://inference/generations/{id}");
    if let Some(error) = reject(&uri, params, &["format"]) {
        return error;
    }
    let as_data = match json(&uri, params) {
        Ok(as_data) => as_data,
        Err(error) => return error,
    };
    let row = match generate::get_generation(&orch.db.local, id).await {
        Ok(Some(row)) => row,
        Ok(None) => return format!("there is no generation {id}"),
        Err(error) => return error,
    };
    let view = generate::generation_view(orch, row).await;
    if as_data {
        return data(&view);
    }
    let mut out = String::from("# Inference profile generation\n\n");
    generation_section(&mut out, &view);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_templates_list_and_their_data_agree() {
        let listed = render_templates(&[]);
        for template in templates() {
            assert!(listed.contains(&format!("## {}", template.id)), "{listed}");
        }
        let as_data = render_templates(&[QueryParam {
            key: "format".into(),
            value: "json".into(),
        }]);
        let parsed: serde_json::Value = serde_json::from_str(&as_data).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), templates().len());
        assert!(render_templates(&[QueryParam {
            key: "format".into(),
            value: "xml".into(),
        }])
        .starts_with("Unknown format"));
    }
}
