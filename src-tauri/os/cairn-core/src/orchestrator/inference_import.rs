//! Importing a published serving recipe: the fetching around the facet's
//! compose compiler (`cairn_inference::import`).
//!
//! The repository must be on GitHub: its ref resolves to a full commit, and
//! the compose and env files are read at that commit, so the recipe pins
//! exactly what was compiled. An image named by tag is resolved to the digest
//! its registry serves now (ghcr.io, Docker Hub or nvcr.io, each asked
//! anonymously); a registry that will not answer leaves the import needing the
//! image in its spec. The weights are the registered model's own files, or a
//! named derivative's, pinned at a revision with every file's hash.

use cairn_inference::generate::{dtype_label, recipe_id};
use cairn_inference::hf::{HfRepo, WeightFormat};
use cairn_inference::import::{self, Files, ImportError, ImportSpec, Missing};
use cairn_inference::{
    Engine, Inventory, Model, ModelRef, Recipe, RecipeOrigin, RecordSource, Requirements, Server,
    WeightFile,
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

/// The weight files a recipe of `server` pins from `repo`: every safetensors
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
            "{} at {} has no weight files a {} recipe can pin",
            repo.repo,
            repo.revision,
            server.as_str()
        ));
    }
    Ok(files)
}

fn refused(missing: Vec<Missing>, refused: Vec<String>) -> String {
    ImportError { missing, refused }.to_string()
}

/// Import `spec`: compile its service, pin its image and weights, and write
/// the recipe; or refuse with every key it needs.
pub async fn import(db: &LocalDb, hub: &dyn Hub, spec: &ImportSpec) -> Result<String, String> {
    let inventory = load_inventory(db).await?;
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
    let mut compiled = import::compile(&spec, &files).map_err(|error| error.to_string())?;
    if let Some(tagged) = compiled.unpinned_image.take() {
        compiled.container.image = pin_image(hub, &tagged).await.map_err(|why| {
            refused(
                vec![Missing {
                    key: "image".into(),
                    meaning: format!("{tagged} pinned by digest (name@sha256:<64 hex>): {why}"),
                }],
                Vec::new(),
            )
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
        Server::Vllm => read
            .facts
            .quantization
            .clone()
            .or_else(|| model.facts.dtype.as_deref().map(dtype_label)),
    };
    let id = spec.id.clone().unwrap_or_else(|| {
        recipe_id(
            &model.name,
            quantization.as_deref(),
            server,
            compiled.machines,
        )
    });
    let recipe = Recipe {
        id: id.as_str().into(),
        source: RecordSource::Cairn,
        model: ModelRef {
            name: model.name.clone(),
            weights,
        },
        served_name: model.name.clone(),
        upstream_id: None,
        verbs: model.facts.verbs.clone(),
        modalities: model.facts.modalities.clone(),
        quantization,
        origin: RecipeOrigin::Imported {
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
    let mut records = Inventory::default();
    records.insert_recipe(recipe);
    let wrote = write_inventory(
        db,
        &Authoring::Patch {
            upsert: Some(records.to_yaml()),
            remove: Removals::default(),
        },
    )
    .await?;
    let mut said = format!(
        "Imported recipe {id} from {} at {}, service {} of {}.\n{wrote}",
        spec.url, spec.commit, spec.service, spec.compose
    );
    if !compiled.notes.is_empty() {
        said.push_str("\nFrom the compose file:\n- ");
        said.push_str(&compiled.notes.join("\n- "));
    }
    said.push_str(&format!(
        "\nVerify it on a pool: patch cairn://inference/recipes/{id} {{verify:{{pool:\"<pool>\"}}}}"
    ));
    Ok(said)
}

#[cfg(test)]
mod tests {
    use super::super::inference_models::tests::FakeHub;
    use super::super::inference_models::HUGGING_FACE;
    use super::*;
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
    async fn the_dspark_recipe_imports_pinned_and_says_how_to_verify() {
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
                "Imported recipe deepseek-v4-flash-vision-exp-fp8-vllm-x2 from {REPO} at {COMMIT}"
            )),
            "{said}"
        );
        assert!(said.contains("dropped `restart`"), "{said}");
        let inventory = load_inventory(&db).await.unwrap();
        let recipe = &inventory.recipes[&"deepseek-v4-flash-vision-exp-fp8-vllm-x2".into()];
        assert_eq!(
            recipe.served_name,
            "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp"
        );
        assert_eq!(recipe.quantization.as_deref(), Some("fp8"));
        assert_eq!(recipe.requirements.machines, 2);
        assert!(recipe.model.weights.len() > 40);
        assert!(recipe
            .model
            .weights
            .iter()
            .all(|w| w.revision == "6821d6ad3681a4b137b066b76094fa82ebd0a380"));
        assert!(
            matches!(&recipe.origin, RecipeOrigin::Imported { commit, .. } if commit == COMMIT)
        );
        assert_eq!(recipe.problems(), vec![]);
    }

    #[tokio::test]
    async fn an_import_names_what_it_lacks_and_needs_a_registered_model() {
        let db = crate::storage::migrated_test_db("inference-import-refusals").await;
        let hub = hub();
        let refusal = import(&db, &hub, &spec()).await.unwrap_err();
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
        assert!(
            refusal.contains("- ranks: one env map per machine"),
            "{refusal}"
        );
        let elsewhere = ImportSpec {
            url: "https://gitlab.com/a/b".into(),
            ..spec()
        };
        let refusal = import(&db, &hub, &elsewhere).await.unwrap_err();
        assert!(
            refusal.contains("Cairn imports only from GitHub"),
            "{refusal}"
        );
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
    fn recipe_ids_name_the_model_quant_server_and_machines() {
        assert_eq!(
            recipe_id("Qwen/Qwen3-Embedding-4B", Some("q8_0"), Server::LlamaCpp, 1),
            "qwen3-embedding-4b-q8_0-llamacpp-x1"
        );
    }
}
