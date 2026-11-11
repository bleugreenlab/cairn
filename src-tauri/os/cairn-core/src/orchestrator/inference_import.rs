//! Importing a published serving profile: the fetching around the facet's
//! compose compiler (`cairn_inference::import`).
//!
//! The repository must be on GitHub: its ref resolves to a full commit, and
//! the compose and env files are read at that commit, so the profile pins
//! exactly what was compiled. An image named by tag is resolved to the digest
//! its registry serves now (ghcr.io, Docker Hub or nvcr.io, each asked
//! anonymously); a registry that will not answer leaves the import needing the
//! image in its spec. The weights are the registered model's own files, or a
//! named derivative's, pinned at a revision with every file's hash.

use cairn_inference::fit::{dtype_label, profile_id};
use cairn_inference::hf::{HfRepo, WeightFormat};
use cairn_inference::import::{self, Files, ImportError, ImportSpec, Missing};
use cairn_inference::profile::is_secret_name;
use cairn_inference::{
    Engine, InferenceProfile, Inventory, Model, ModelRef, ProfileOrigin, RecordSource,
    Requirements, Server, WeightFile,
};
use sha2::{Digest, Sha256};

use super::inference::{load_inventory, write_inventory, Authoring, Removals};
use super::inference_models::{read_repo, Hub};
use crate::storage::LocalDb;

/// `https://github.com/<owner>/<repo>` as `(owner, repo)`.
fn github_repo(url: &str) -> Result<(String, String), String> {
    let path = url
        .trim_end_matches('/')
        .trim_end_matches(".git")
        .strip_prefix("https://github.com/")
        .ok_or_else(|| {
            format!("{url} is not a GitHub repository URL (https://github.com/<owner>/<repo>); Cairn imports only from GitHub")
        })?;
    match path.split('/').collect::<Vec<_>>().as_slice() {
        [owner, repo] if !owner.is_empty() && !repo.is_empty() => {
            Ok((owner.to_string(), repo.to_string()))
        }
        _ => Err(format!("{url} does not name one repository")),
    }
}

/// The full commit sha a ref names.
pub async fn resolve_commit(hub: &dyn Hub, url: &str, reference: &str) -> Result<String, String> {
    let (owner, repo) = github_repo(url)?;
    let body = hub
        .fetch(
            format!("https://api.github.com/repos/{owner}/{repo}/commits/{reference}"),
            vec![("Accept", "application/vnd.github+json".into())],
        )
        .await?
        .ok_or_else(|| format!("{url} has no commit {reference}"))?;
    serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| value.get("sha")?.as_str().map(str::to_string))
        .ok_or_else(|| format!("GitHub's answer for {url}@{reference} names no commit"))
}

async fn read_file(hub: &dyn Hub, url: &str, commit: &str, path: &str) -> Result<String, String> {
    let (owner, repo) = github_repo(url)?;
    hub.get(format!(
        "https://raw.githubusercontent.com/{owner}/{repo}/{commit}/{path}"
    ))
    .await?
    .ok_or_else(|| format!("{url} has no {path} at {commit}"))
}

/// A registry's host, the repository within it, and the tag.
fn image_parts(image: &str) -> Option<(String, String, String)> {
    let (name, tag) = match image.rsplit_once(':') {
        Some((name, tag)) if !tag.contains('/') => (name, tag),
        _ => (image, "latest"),
    };
    let (host, repo) = match name.split_once('/') {
        Some((host, repo)) if host.contains('.') || host.contains(':') => {
            (host.to_string(), repo.to_string())
        }
        Some(_) => ("docker.io".to_string(), name.to_string()),
        None => ("docker.io".to_string(), format!("library/{name}")),
    };
    Some((host, repo, tag.to_string()))
}

const MANIFEST_TYPES: &str = "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json";

/// `image` pinned to the digest its registry serves for its tag now.
pub async fn pin_image(hub: &dyn Hub, image: &str) -> Result<String, String> {
    let (host, repo, tag) =
        image_parts(image).ok_or_else(|| format!("{image} is not an image name"))?;
    let (registry, token_url) = match host.as_str() {
        "docker.io" => (
            "registry-1.docker.io".to_string(),
            format!("https://auth.docker.io/token?service=registry.docker.io&scope=repository:{repo}:pull"),
        ),
        "ghcr.io" => (
            host.clone(),
            format!("https://ghcr.io/token?scope=repository:{repo}:pull"),
        ),
        "nvcr.io" => (
            host.clone(),
            format!("https://nvcr.io/proxy_auth?scope=repository:{repo}:pull"),
        ),
        other => {
            return Err(format!(
                "Cairn does not know how to ask registry {other} for a digest; give the image pinned by digest"
            ))
        }
    };
    let token = hub
        .get(token_url)
        .await?
        .and_then(|body| {
            let value: serde_json::Value = serde_json::from_str(&body).ok()?;
            value
                .get("token")
                .or_else(|| value.get("access_token"))?
                .as_str()
                .map(str::to_string)
        })
        .ok_or_else(|| format!("{host} gave no anonymous token for {repo}"))?;
    let manifest = hub
        .fetch(
            format!("https://{registry}/v2/{repo}/manifests/{tag}"),
            vec![
                ("Accept", MANIFEST_TYPES.to_string()),
                ("Authorization", format!("Bearer {token}")),
            ],
        )
        .await?
        .ok_or_else(|| format!("{host} has no {repo}:{tag}"))?;
    let digest = Sha256::digest(manifest.as_bytes());
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    let name = image.rsplit_once(':').map_or(image, |(name, _)| name);
    Ok(format!("{name}@sha256:{hex}"))
}

/// The weight files a profile of `server` pins from `repo`: every safetensors
/// file for vLLM, the one GGUF file for llama.cpp.
pub fn weight_files(
    repo: &HfRepo,
    server: Server,
    gguf_file: Option<&str>,
) -> Result<Vec<WeightFile>, String> {
    let pick = |format: WeightFormat| {
        repo.weights_of(format)
            .map(|file| WeightFile {
                repo: repo.repo.clone(),
                revision: repo.revision.clone(),
                path: file.path.clone(),
                sha256: file.sha256.clone(),
                size: Some(file.size),
            })
            .collect::<Vec<_>>()
    };
    let files = match server {
        Server::Vllm => pick(WeightFormat::Safetensors),
        Server::MlxLm => {
            return Err(
                "MLX runs natively on Apple silicon, and a compose service is a container".into(),
            )
        }
        Server::LlamaCpp => {
            let all = pick(WeightFormat::Gguf);
            match gguf_file {
                Some(path) => all.into_iter().filter(|w| w.path == path).collect(),
                None if all.len() == 1 => all,
                None => {
                    return Err(format!(
                        "{} holds {} GGUF files; name one as weights \"{}:<file>\"",
                        repo.repo,
                        all.len(),
                        repo.repo
                    ))
                }
            }
        }
    };
    if files.is_empty() {
        return Err(format!(
            "{} at {} has no weight files a {} profile can pin",
            repo.repo,
            repo.revision,
            server.as_str()
        ));
    }
    Ok(files)
}

/// Why an import was refused: the compile's account of the keys its spec
/// lacks and what it refused, or any other reason in words.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportRefusal {
    Compile(ImportError),
    Other(String),
}

impl From<String> for ImportRefusal {
    fn from(reason: String) -> Self {
        Self::Other(reason)
    }
}

impl std::fmt::Display for ImportRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compile(error) => error.fmt(f),
            Self::Other(reason) => f.write_str(reason),
        }
    }
}

/// How a compile treats a variable whose name says it holds a secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Secrets {
    /// As Compose does: its value comes from the spec, the env files or the
    /// default. What an import writes.
    Resolve,
    /// Never resolved: wherever the spec or an env file gives it a value, or
    /// the compose file reads it with no default, the compiler is handed the
    /// literal `${NAME}`, so the candidate carries the named reference and no
    /// value. What a preview shows.
    Reference,
}

fn reference(name: &str) -> String {
    format!("${{{name}}}")
}

/// A compose service compiled into a profile, with nothing written: the
/// spec at its resolved commit, the profile, what the compile said of the
/// compose file, and the secret-named variables kept as references.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// The spec as compiled: its commit the full sha the ref resolved to.
    pub spec: ImportSpec,
    pub profile: InferenceProfile,
    pub notes: Vec<String>,
    /// Every variable carried as `${NAME}`, by name. Empty under
    /// [`Secrets::Resolve`].
    pub references: Vec<String>,
}

/// Compile `spec` against the records in `inventory`: resolve its commit,
/// read its files there, compile its service with the facet's parser, pin
/// its image and weights. It holds no database, so it writes nothing; an
/// import persists the result, a preview shows it.
pub async fn resolve(
    inventory: &Inventory,
    hub: &dyn Hub,
    spec: &ImportSpec,
    secrets: Secrets,
) -> Result<Candidate, ImportRefusal> {
    let model: Model = inventory.models.get(&spec.model).cloned().ok_or_else(|| {
        format!(
            "model {} is not registered; append it to cairn://inference/models first",
            spec.model
        )
    })?;
    let mut spec = spec.clone();
    spec.commit = resolve_commit(hub, &spec.url, &spec.commit).await?;
    let mut files = Files {
        compose: read_file(hub, &spec.url, &spec.commit, &spec.compose).await?,
        env_files: Vec::new(),
    };
    for path in &spec.env_files {
        let text = read_file(hub, &spec.url, &spec.commit, path).await?;
        files.env_files.push((path.clone(), text));
    }

    // Under `Reference`, every secret-named variable with a value is handed
    // to the compiler as its own reference. The spec's env wins over every
    // env file and default, so no value of one reaches the compiled
    // container; the values are kept only to prove that below.
    let caller_spec = spec.clone();
    let mut references = std::collections::BTreeSet::new();
    let mut withheld: Vec<(String, String)> = Vec::new();
    if secrets == Secrets::Reference {
        let mut given = std::collections::BTreeMap::new();
        for (_, text) in &files.env_files {
            given.extend(import::parse_env_file(text));
        }
        given.extend(spec.env.clone());
        for (name, value) in given {
            if is_secret_name(&name) && !value.is_empty() && value != reference(&name) {
                spec.env.insert(name.clone(), reference(&name));
                references.insert(name.clone());
                withheld.push((name, value));
            }
        }
    }
    let mut compiled = match import::compile(&spec, &files) {
        // A secret the compose file reads with no default, and that nothing
        // sets, is the reference itself rather than a key the spec lacks.
        Err(error) if secrets == Secrets::Reference => {
            let unset: Vec<String> = error
                .missing
                .iter()
                .filter_map(|missing| missing.key.strip_prefix("env."))
                .filter(|name| is_secret_name(name) && !spec.env.contains_key(*name))
                .map(str::to_string)
                .collect();
            if unset.is_empty() {
                return Err(ImportRefusal::Compile(error));
            }
            for name in unset {
                spec.env.insert(name.clone(), reference(&name));
                references.insert(name);
            }
            import::compile(&spec, &files)
        }
        compiled => compiled,
    }
    .map_err(ImportRefusal::Compile)?;
    // The references were the compiler's input, not the caller's: the
    // candidate's spec is the one asked for, at its resolved commit.
    let spec = ImportSpec {
        commit: spec.commit,
        ..caller_spec
    };
    if let Some(tagged) = compiled.unpinned_image.take() {
        compiled.container.image = pin_image(hub, &tagged).await.map_err(|why| {
            ImportRefusal::Compile(ImportError {
                missing: vec![Missing {
                    key: "image".into(),
                    meaning: format!("{tagged} pinned by digest (name@sha256:<64 hex>): {why}"),
                }],
                refused: Vec::new(),
            })
        })?;
    }

    let (weights_repo, gguf_file) = match &spec.weights {
        Some(weights) => match weights.split_once(':') {
            Some((repo, file)) => (repo.to_string(), Some(file.to_string())),
            None => (weights.clone(), None),
        },
        None => (model.name.clone(), None),
    };
    let revision = (weights_repo == model.name)
        .then(|| model.revision.clone())
        .flatten();
    let read = read_repo(hub, &weights_repo, revision.as_deref()).await?;
    let server = compiled.container.server;
    let weights = weight_files(&read, server, gguf_file.as_deref())?;
    let quantization = match server {
        Server::LlamaCpp => weights.first().and_then(|w| {
            cairn_inference::hf::HfFile {
                path: w.path.clone(),
                sha256: w.sha256.clone(),
                size: 0,
            }
            .gguf_quantization()
        }),
        Server::Vllm | Server::MlxLm => read
            .facts
            .quantization
            .clone()
            .or_else(|| model.facts.dtype.as_deref().map(dtype_label)),
    };
    let id = spec.id.clone().unwrap_or_else(|| {
        profile_id(
            &model.name,
            quantization.as_deref(),
            server,
            None,
            compiled.machines,
        )
    });
    let profile = InferenceProfile {
        id: id.as_str().into(),
        source: RecordSource::Cairn,
        model: ModelRef {
            name: model.name.clone(),
            weights,
            // vLLM reads these beside the weights; a GGUF file holds them.
            config_files: match server {
                Server::Vllm | Server::MlxLm => read.config_files.clone(),
                Server::LlamaCpp => Vec::new(),
            },
        },
        served_name: model.name.clone(),
        upstream_id: None,
        verbs: model.facts.verbs.clone(),
        modalities: model.facts.modalities.clone(),
        quantization,
        origin: ProfileOrigin::Imported {
            url: spec.url.clone(),
            commit: spec.commit.clone(),
            compose: spec.compose.clone(),
            service: spec.service.clone(),
        },
        engine: Engine::Container(compiled.container),
        requirements: Requirements {
            machines: compiled.machines,
            accelerator: compiled.accelerator,
            interconnect: None,
            memory: None,
        },
        notes: None,
    };
    let mut notes = compiled.notes;
    notes.extend(references.iter().map(|name| {
        format!(
            "kept ${{{name}}} as a named reference: a preview never resolves or shows the value of a secret-named variable, and nothing resolves it at container start either, so the candidate is refused until that entry is removed or given a value that is not a secret (see its problems)"
        )
    }));
    if !withheld.is_empty() {
        let shown = format!(
            "{}\n{}",
            serde_json::to_string(&profile).unwrap_or_default(),
            notes.join("\n")
        );
        if let Some((name, _)) = withheld
            .iter()
            .find(|(_, value)| value.len() >= 4 && shown.contains(value.as_str()))
        {
            return Err(ImportRefusal::Other(format!(
                "the candidate would still carry the value given for {name}, a secret-named variable, through another variable or default, so the preview is refused rather than shown"
            )));
        }
    }
    Ok(Candidate {
        spec,
        profile,
        notes,
        references: references.into_iter().collect(),
    })
}

/// Write `candidate` into the records and say what was written.
pub async fn persist(db: &LocalDb, candidate: Candidate) -> Result<String, ImportRefusal> {
    let Candidate {
        spec,
        profile,
        notes,
        ..
    } = candidate;
    let id = profile.id.clone();
    let mut records = Inventory::default();
    records.insert_profile(profile);
    let wrote = write_inventory(
        db,
        &Authoring::Patch {
            upsert: Some(records.to_yaml()),
            remove: Removals::default(),
        },
    )
    .await?;
    let mut said = format!(
        "Imported profile {id} from {} at {}, service {} of {}.\n{wrote}",
        spec.url, spec.commit, spec.service, spec.compose
    );
    if !notes.is_empty() {
        said.push_str("\nFrom the compose file:\n- ");
        said.push_str(&notes.join("\n- "));
    }
    said.push_str(&format!(
        "\nVerify it on a pool: patch cairn://inference/profiles/{id} {{verify:{{pool:\"<pool>\"}}}}"
    ));
    Ok(said)
}

/// Import `spec`: compile its service, pin its image and weights, and write
/// the profile; or refuse with every key it needs.
pub async fn import(
    db: &LocalDb,
    hub: &dyn Hub,
    spec: &ImportSpec,
) -> Result<String, ImportRefusal> {
    let inventory = load_inventory(db).await?;
    let candidate = resolve(&inventory, hub, spec, Secrets::Resolve).await?;
    persist(db, candidate).await
}

/// Preview `spec`: compile it exactly as [`import`] does, with secret-named
/// variables kept as `${NAME}` references, and write nothing. Answers the
/// summary and `{preview, profile, source, notes, references, problems,
/// replaces}`: the candidate as JSON, which a generation submits through its
/// own `generated` door, citing the source commit.
pub async fn preview(
    db: &LocalDb,
    hub: &dyn Hub,
    spec: &ImportSpec,
) -> Result<(String, serde_json::Value), ImportRefusal> {
    let inventory = load_inventory(db).await?;
    let candidate = resolve(&inventory, hub, spec, Secrets::Reference).await?;
    let Candidate {
        spec,
        profile,
        notes,
        references,
    } = candidate;
    let id = profile.id.clone();
    let replaces = inventory.profiles.contains_key(&id);
    let problems: Vec<String> = profile.problems().iter().map(ToString::to_string).collect();
    let mut records = Inventory::default();
    records.insert_profile(profile.clone());
    let mut said = format!(
        "Preview of profile {id} from {} at {}, service {} of {}; nothing was written.",
        spec.url, spec.commit, spec.service, spec.compose
    );
    if replaces {
        said.push_str(&format!(
            "\nA profile {id} is already recorded; importing this would replace it."
        ));
    }
    if !notes.is_empty() {
        said.push_str("\nFrom the compose file:\n- ");
        said.push_str(&notes.join("\n- "));
    }
    if !problems.is_empty() {
        said.push_str("\nAs a profile it has these problems:\n- ");
        said.push_str(&problems.join("\n- "));
    }
    said.push_str(&format!("\nThe candidate:\n{}", records.to_yaml()));
    let data = serde_json::json!({
        "preview": true,
        "profile": profile,
        "source": {
            "url": spec.url,
            "commit": spec.commit,
            "compose": spec.compose,
            "service": spec.service,
        },
        "notes": notes,
        "references": references,
        "problems": problems,
        "replaces": replaces,
    });
    Ok((said, data))
}

#[cfg(test)]
mod tests {
    use super::super::inference_models::tests::FakeHub;
    use super::super::inference_models::HUGGING_FACE;
    use super::*;
    use crate::storage::RowExt;
    use std::collections::BTreeMap;

    const COMMIT: &str = "f3d76450e075d5f89698aed2e865655f0b7eeb1c";
    const REPO: &str = "https://github.com/MiaAI-Lab/DeepSeek-v4-Flash-DSpark-2x-DGX-Spark";

    fn hub() -> FakeHub {
        let mut hub = FakeHub::fixtures();
        let raw =
            "https://raw.githubusercontent.com/MiaAI-Lab/DeepSeek-v4-Flash-DSpark-2x-DGX-Spark";
        hub.bodies.insert(
            "https://api.github.com/repos/MiaAI-Lab/DeepSeek-v4-Flash-DSpark-2x-DGX-Spark/commits/f3d7645".into(),
            format!("{{\"sha\":\"{COMMIT}\"}}"),
        );
        hub.bodies.insert(
            format!("{raw}/{COMMIT}/docker-compose.dspark.yml"),
            include_str!(
                "../../../cairn-inference/tests/fixtures/import/docker-compose.dspark.yml"
            )
            .into(),
        );
        hub.bodies.insert(
            format!("{raw}/{COMMIT}/.env.dspark.example"),
            include_str!("../../../cairn-inference/tests/fixtures/import/env.dspark.example")
                .into(),
        );
        hub
    }

    fn pairs(list: &[(&str, &str)]) -> BTreeMap<String, String> {
        list.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn spec() -> ImportSpec {
        ImportSpec {
            url: REPO.into(),
            commit: "f3d7645".into(),
            compose: "docker-compose.dspark.yml".into(),
            service: "vllm-dspark".into(),
            env_files: vec![".env.dspark.example".into()],
            env: pairs(&[
                ("VLLM_HOST", "0.0.0.0"),
                ("VLLM_PORT", "{port}"),
                ("SERVED_MODEL_NAME", "{servedName}"),
                ("DSPARK_MODEL", "{model}"),
                ("DSPARK_REVISION", "{revision}"),
                ("MASTER_ADDR", "{head.link}"),
                ("NNODES", "{machines}"),
            ]),
            ranks: Some(vec![
                pairs(&[("NODE_RANK", "0"), ("VLLM_HOST_IP", "{self.link}")]),
                pairs(&[
                    ("NODE_RANK", "1"),
                    ("HEADLESS", "1"),
                    ("VLLM_HOST_IP", "{self.link}"),
                ]),
            ]),
            host_paths: BTreeMap::from([
                ("/cache/huggingface".into(), import::HostPath::HfCache),
                ("/tmp".into(), import::HostPath::Drop),
            ]),
            model: "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp".into(),
            weights: None,
            image: None,
            id: None,
        }
    }

    #[tokio::test]
    async fn the_dspark_profile_imports_pinned_and_says_how_to_verify() {
        let db = crate::storage::migrated_test_db("inference-import-dspark").await;
        let hub = hub();
        super::super::inference_models::register(
            &db,
            &hub,
            "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp",
            None,
        )
        .await
        .unwrap();
        let said = import(&db, &hub, &spec()).await.unwrap();
        assert!(
            said.starts_with(&format!(
                "Imported profile deepseek-v4-flash-vision-exp-fp8-vllm-x2 from {REPO} at {COMMIT}"
            )),
            "{said}"
        );
        assert!(said.contains("dropped `restart`"), "{said}");
        let inventory = load_inventory(&db).await.unwrap();
        let profile = &inventory.profiles[&"deepseek-v4-flash-vision-exp-fp8-vllm-x2".into()];
        assert_eq!(
            profile.served_name,
            "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp"
        );
        assert_eq!(profile.quantization.as_deref(), Some("fp8"));
        assert_eq!(profile.requirements.machines, 2);
        assert!(profile.model.weights.len() > 40);
        assert!(profile
            .model
            .weights
            .iter()
            .all(|w| w.revision == "6821d6ad3681a4b137b066b76094fa82ebd0a380"));
        assert!(
            matches!(&profile.origin, ProfileOrigin::Imported { commit, .. } if commit == COMMIT)
        );
        assert_eq!(profile.problems(), vec![]);
    }

    #[tokio::test]
    async fn an_import_names_what_it_lacks_and_needs_a_registered_model() {
        let db = crate::storage::migrated_test_db("inference-import-refusals").await;
        let hub = hub();
        let refusal = import(&db, &hub, &spec()).await.unwrap_err().to_string();
        assert!(refusal.contains("is not registered"), "{refusal}");
        super::super::inference_models::register(
            &db,
            &hub,
            "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp",
            None,
        )
        .await
        .unwrap();
        let bare = ImportSpec {
            ranks: None,
            ..spec()
        };
        let refusal = import(&db, &hub, &bare).await.unwrap_err();
        let ImportRefusal::Compile(needs) = &refusal else {
            panic!("a spec without ranks names the key it lacks: {refusal}");
        };
        assert_eq!(
            needs
                .missing
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["ranks"]
        );
        let refusal = refusal.to_string();
        assert!(
            refusal.contains("- ranks: one env map per machine"),
            "{refusal}"
        );
        let elsewhere = ImportSpec {
            url: "https://gitlab.com/a/b".into(),
            ..spec()
        };
        let refusal = import(&db, &hub, &elsewhere).await.unwrap_err().to_string();
        assert!(
            refusal.contains("Cairn imports only from GitHub"),
            "{refusal}"
        );
    }

    /// Every table's row count and the whole inventory: what a write of any
    /// kind would change.
    async fn snapshot(db: &LocalDb) -> (Vec<(String, i64)>, String) {
        let tables = db
            .query_all(
                "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name".to_string(),
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        let mut counts = Vec::new();
        for table in tables {
            let count = db
                .query_one(format!("SELECT COUNT(*) FROM \"{table}\""), (), |row| {
                    row.i64(0)
                })
                .await
                .unwrap();
            counts.push((table, count));
        }
        (counts, load_inventory(db).await.unwrap().to_yaml())
    }

    async fn registered(name: &str) -> (LocalDb, FakeHub) {
        let db = crate::storage::migrated_test_db(name).await;
        let hub = hub();
        super::super::inference_models::register(
            &db,
            &hub,
            "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp",
            None,
        )
        .await
        .unwrap();
        (db, hub)
    }

    #[tokio::test]
    async fn a_preview_answers_what_the_import_would_write_and_writes_nothing() {
        let (db, hub) = registered("inference-import-preview").await;
        let before = snapshot(&db).await;
        let (said, data) = preview(&db, &hub, &spec()).await.unwrap();
        assert_eq!(snapshot(&db).await, before, "a preview writes nothing");
        assert!(
            said.starts_with(&format!(
                "Preview of profile deepseek-v4-flash-vision-exp-fp8-vllm-x2 from {REPO} at {COMMIT}"
            )),
            "{said}"
        );
        assert!(said.contains("nothing was written"), "{said}");
        assert!(said.contains("dropped `restart`"), "{said}");
        assert_eq!(data["preview"], true);
        assert_eq!(
            data["source"]["commit"], COMMIT,
            "the ref resolved to its commit"
        );
        assert_eq!(data["source"]["url"], REPO);
        assert_eq!(data["references"], serde_json::json!([]));
        assert_eq!(data["replaces"], false);
        assert_eq!(data["problems"], serde_json::json!([]));

        import(&db, &hub, &spec()).await.unwrap();
        let inventory = load_inventory(&db).await.unwrap();
        let written = &inventory.profiles[&"deepseek-v4-flash-vision-exp-fp8-vllm-x2".into()];
        assert_eq!(
            data["profile"],
            serde_json::to_value(written).unwrap(),
            "with no secrets, the candidate is exactly the profile the import writes"
        );
        let (said, data) = preview(&db, &hub, &spec()).await.unwrap();
        assert_eq!(data["replaces"], true);
        assert!(said.contains("importing this would replace it"), "{said}");
    }

    /// The dspark service reading two secrets: `HF_TOKEN` with no default and
    /// `GITHUB_TOKEN` with an empty one, the latter set by an env file.
    fn secret_hub() -> FakeHub {
        let mut hub = hub();
        let raw =
            "https://raw.githubusercontent.com/MiaAI-Lab/DeepSeek-v4-Flash-DSpark-2x-DGX-Spark";
        let compose = include_str!(
            "../../../cairn-inference/tests/fixtures/import/docker-compose.dspark.yml"
        )
        .replace(
            "      HF_HOME: /cache/huggingface\n",
            "      HF_HOME: /cache/huggingface\n      HF_TOKEN: \"${HF_TOKEN}\"\n      GITHUB_TOKEN: \"${GITHUB_TOKEN:-}\"\n",
        );
        assert!(compose.contains("${HF_TOKEN}"));
        hub.bodies
            .insert(format!("{raw}/{COMMIT}/docker-compose.secret.yml"), compose);
        hub.bodies.insert(
            format!("{raw}/{COMMIT}/.env.secret"),
            "GITHUB_TOKEN=ghp_filesecret456789\n".into(),
        );
        hub
    }

    fn secret_spec() -> ImportSpec {
        ImportSpec {
            compose: "docker-compose.secret.yml".into(),
            env_files: vec![".env.dspark.example".into(), ".env.secret".into()],
            ..spec()
        }
    }

    fn container_env(profile: &serde_json::Value) -> std::collections::BTreeMap<String, String> {
        let profile: InferenceProfile = serde_json::from_value(profile.clone()).unwrap();
        let Engine::Container(container) = profile.engine else {
            panic!("an import compiles a container");
        };
        container.env
    }

    #[tokio::test]
    async fn a_preview_keeps_secrets_as_named_references_and_never_shows_their_values() {
        let db = crate::storage::migrated_test_db("inference-import-preview-secrets").await;
        let hub = secret_hub();
        super::super::inference_models::register(
            &db,
            &hub,
            "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp",
            None,
        )
        .await
        .unwrap();

        // An import still needs a secret the compose file reads unset...
        let refusal = import(&db, &hub, &secret_spec()).await.unwrap_err();
        let ImportRefusal::Compile(needs) = &refusal else {
            panic!("{refusal}");
        };
        assert!(
            needs.missing.iter().any(|m| m.key == "env.HF_TOKEN"),
            "{refusal}"
        );

        // ...where a preview carries it, and the env file's, as references.
        let before = snapshot(&db).await;
        let (said, data) = preview(&db, &hub, &secret_spec()).await.unwrap();
        assert_eq!(snapshot(&db).await, before, "a preview writes nothing");
        let env = container_env(&data["profile"]);
        assert_eq!(env["HF_TOKEN"], "${HF_TOKEN}");
        assert_eq!(env["GITHUB_TOKEN"], "${GITHUB_TOKEN}");
        assert_eq!(
            data["references"],
            serde_json::json!(["GITHUB_TOKEN", "HF_TOKEN"])
        );
        assert!(
            said.contains("kept ${HF_TOKEN} as a named reference"),
            "{said}"
        );
        assert!(
            data["notes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|note| note.as_str().unwrap().contains("${GITHUB_TOKEN}")),
            "{data}"
        );
        let shown = format!("{said}\n{data}");
        assert!(!shown.contains("ghp_filesecret456789"), "{shown}");

        // A secret the caller gives is not echoed either.
        let given = {
            let mut spec = secret_spec();
            spec.env
                .insert("HF_TOKEN".into(), "hf_specsecret123456".into());
            spec
        };
        let (said, data) = preview(&db, &hub, &given).await.unwrap();
        assert_eq!(snapshot(&db).await, before, "a preview writes nothing");
        assert_eq!(container_env(&data["profile"])["HF_TOKEN"], "${HF_TOKEN}");
        let shown = format!("{said}\n{data}");
        assert!(!shown.contains("hf_specsecret123456"), "{shown}");
        assert!(!shown.contains("ghp_filesecret456789"), "{shown}");

        // The import itself interpolates as it always has.
        import(&db, &hub, &given).await.unwrap();
        let inventory = load_inventory(&db).await.unwrap();
        let written = &inventory.profiles[&"deepseek-v4-flash-vision-exp-fp8-vllm-x2".into()];
        let env = container_env(&serde_json::to_value(written).unwrap());
        assert_eq!(env["HF_TOKEN"], "hf_specsecret123456");
        assert_eq!(env["GITHUB_TOKEN"], "ghp_filesecret456789");
    }

    #[tokio::test]
    async fn a_preview_that_would_carry_a_withheld_value_another_way_is_refused() {
        let db = crate::storage::migrated_test_db("inference-import-preview-leak").await;
        let hub = secret_hub();
        super::super::inference_models::register(
            &db,
            &hub,
            "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp",
            None,
        )
        .await
        .unwrap();
        let mut spec = secret_spec();
        spec.env
            .insert("HF_TOKEN".into(), "hf_specsecret123456".into());
        // The same value under a name that is not a secret's.
        spec.env
            .insert("HF_HUB_OFFLINE".into(), "hf_specsecret123456".into());
        let before = snapshot(&db).await;
        let refusal = preview(&db, &hub, &spec).await.unwrap_err().to_string();
        assert_eq!(snapshot(&db).await, before);
        assert!(
            refusal.contains("the value given for HF_TOKEN"),
            "{refusal}"
        );
        assert!(!refusal.contains("hf_specsecret123456"), "{refusal}");
    }

    #[tokio::test]
    async fn a_previewed_reference_cannot_be_written_until_its_entry_is_dropped() {
        let db = crate::storage::migrated_test_db("inference-import-preview-write").await;
        let hub = secret_hub();
        super::super::inference_models::register(
            &db,
            &hub,
            "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp",
            None,
        )
        .await
        .unwrap();
        let mut spec = secret_spec();
        spec.env
            .insert("HF_TOKEN".into(), "hf_specsecret123456".into());
        let (said, data) = preview(&db, &hub, &spec).await.unwrap();
        let problems: Vec<&str> = data["problems"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p.as_str().unwrap())
            .collect();
        assert!(
            problems
                .iter()
                .any(|p| p.contains("env HF_TOKEN reads ${HF_TOKEN}") && p.contains("on the host")),
            "{problems:?}"
        );
        assert!(
            problems
                .iter()
                .any(|p| p.contains("env GITHUB_TOKEN reads ${GITHUB_TOKEN}")),
            "{problems:?}"
        );
        assert!(
            said.contains("the candidate is refused until that entry is removed"),
            "{said}"
        );

        // The candidate as given is refused by the inventory's own
        // validation, which every write door (the generated one included)
        // runs, and the refusal names no value.
        let profile: InferenceProfile = serde_json::from_value(data["profile"].clone()).unwrap();
        let write = |profile: InferenceProfile| {
            let mut records = Inventory::default();
            records.insert_profile(profile);
            Authoring::Patch {
                upsert: Some(records.to_yaml()),
                remove: Removals::default(),
            }
        };
        let before = snapshot(&db).await;
        let refusal = write_inventory(&db, &write(profile.clone()))
            .await
            .unwrap_err();
        assert_eq!(snapshot(&db).await, before);
        assert!(refusal.contains("reads ${HF_TOKEN}"), "{refusal}");
        for value in ["hf_specsecret123456", "ghp_filesecret456789"] {
            assert!(!refusal.contains(value), "{refusal}");
            assert!(!format!("{said}\n{data}").contains(value));
        }

        // Without the secret entries it validates and writes.
        let mut dropped = profile;
        let Engine::Container(container) = &mut dropped.engine else {
            unreachable!()
        };
        container.env.remove("HF_TOKEN");
        container.env.remove("GITHUB_TOKEN");
        assert_eq!(dropped.problems(), vec![]);
        write_inventory(&db, &write(dropped)).await.unwrap();
    }

    #[tokio::test]
    async fn a_tagged_image_is_pinned_to_the_digest_of_its_manifest() {
        let mut hub = FakeHub::default();
        hub.bodies.insert(
            "https://ghcr.io/token?scope=repository:ggml-org/llama.cpp:pull".into(),
            r#"{"token":"t"}"#.into(),
        );
        hub.bodies.insert(
            "https://ghcr.io/v2/ggml-org/llama.cpp/manifests/server-vulkan".into(),
            "{}".into(),
        );
        let pinned = pin_image(&hub, "ghcr.io/ggml-org/llama.cpp:server-vulkan")
            .await
            .unwrap();
        assert_eq!(
            pinned,
            "ghcr.io/ggml-org/llama.cpp@sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        );
        assert_eq!(
            image_parts("vllm/vllm-openai:v0.12.0"),
            Some((
                "docker.io".into(),
                "vllm/vllm-openai".into(),
                "v0.12.0".into()
            ))
        );
        assert!(pin_image(&hub, "quay.io/x/y:1")
            .await
            .unwrap_err()
            .contains("give the image pinned by digest"));
        let _ = HUGGING_FACE;
    }

    #[test]
    fn profile_ids_name_the_model_quant_server_and_machines() {
        assert_eq!(
            profile_id(
                "Qwen/Qwen3-Embedding-4B",
                Some("q8_0"),
                Server::LlamaCpp,
                None,
                1
            ),
            "qwen3-embedding-4b-q8_0-llamacpp-x1"
        );
        assert_eq!(
            profile_id(
                "Qwen/Qwen3-Embedding-4B",
                Some("q8_0"),
                Server::LlamaCpp,
                Some(cairn_inference::AcceleratorRuntime::Metal),
                1
            ),
            "qwen3-embedding-4b-q8_0-llamacpp-metal-x1",
            "a native profile names its runtime"
        );
    }
}
