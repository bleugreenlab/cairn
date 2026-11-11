//! Models by name: reading a Hugging Face repository and registering the
//! model it is.
//!
//! The facet's `hf` module parses; this module fetches. A repository is read
//! at a revision (a ref such as `main` resolves to its sha), and the model it
//! is gets registered in the inventory through the same write every record
//! takes, so it is validated and refused the same way. A quantized derivative
//! (`nvidia/Qwen3.6-35B-A3B-NVFP4`) is not registered: its base is, and the
//! answer says so, because a quantized repository is the weights of one of the
//! base model's profiles. A gated repository is refused, naming the gate;
//! Hugging Face credentials are not something Cairn holds.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use cairn_inference::hf::{self, HfRepo};
use cairn_inference::{Inventory, Model, WeightFile};

use super::inference::{write_inventory, Authoring, Removals};
use crate::storage::LocalDb;

pub const HUGGING_FACE: &str = "https://huggingface.co";

/// Organisations whose quantized derivatives of a model are offered as its
/// weights, besides the model's own organisation: NVIDIA's FP4 and FP8
/// builds, llama.cpp's own GGUFs, and the MLX community's quantizations,
/// which are what Apple silicon runs under MLX.
const TRUSTED_QUANTIZERS: &[&str] = &["nvidia", "ggml-org", "mlx-community"];

pub type Fetched<'a> = Pin<Box<dyn Future<Output = Result<Option<String>, String>> + Send + 'a>>;

/// What reading the web needs (Hugging Face, GitHub, an image registry): a
/// GET with headers answering the body, `None` for a 404, or why it failed.
pub trait Hub: Send + Sync {
    fn fetch(&self, url: String, headers: Vec<(&'static str, String)>) -> Fetched<'_>;
    fn get(&self, url: String) -> Fetched<'_> {
        self.fetch(url, Vec::new())
    }
    fn base(&self) -> &str {
        HUGGING_FACE
    }
}

/// The live web, read anonymously.
pub struct Web {
    client: reqwest::Client,
}

impl Web {
    pub fn new() -> Result<Self, String> {
        cairn_tls::client_builder()
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(60))
            .user_agent("cairn-inference")
            .build()
            .map(|client| Self { client })
            .map_err(|error| format!("could not build an HTTP client: {error}"))
    }
}

impl Hub for Web {
    fn fetch(&self, url: String, headers: Vec<(&'static str, String)>) -> Fetched<'_> {
        Box::pin(async move {
            let mut request = self.client.get(&url);
            for (name, value) in headers {
                request = request.header(name, value);
            }
            let response = request
                .send()
                .await
                .map_err(|error| format!("{url}: {error}"))?;
            match response.status().as_u16() {
                404 => Ok(None),
                401 | 403 => Err(format!(
                    "{url} answered {}: it is private or gated, and Cairn reads it without credentials",
                    response.status()
                )),
                _ => {
                    let response = response
                        .error_for_status()
                        .map_err(|error| format!("{url}: {error}"))?;
                    response
                        .text()
                        .await
                        .map(Some)
                        .map_err(|error| format!("{url}: {error}"))
                }
            }
        })
    }
}

/// Read `repo` at `revision` (a sha or a ref; `main` when `None`).
pub async fn read_repo(
    hub: &dyn Hub,
    repo: &str,
    revision: Option<&str>,
) -> Result<HfRepo, String> {
    let revision = revision.unwrap_or("main");
    let base = hub.base().trim_end_matches('/');
    let api = hub
        .get(format!(
            "{base}/api/models/{repo}/revision/{revision}?blobs=true"
        ))
        .await?
        .ok_or_else(|| format!("Hugging Face has no repository {repo} at revision {revision}"))?;
    let sha = serde_json::from_str::<serde_json::Value>(&api)
        .ok()
        .and_then(|value| value.get("sha")?.as_str().map(str::to_string))
        .ok_or_else(|| format!("Hugging Face's answer for {repo} names no revision"))?;
    let config = hub
        .get(format!("{base}/{repo}/raw/{sha}/config.json"))
        .await?;
    let read = hf::parse(&api, config.as_deref())?;
    if read.gated {
        return Err(format!(
            "{repo} is gated on Hugging Face: downloading it needs its licence accepted with an account, and Cairn holds no Hugging Face credentials"
        ));
    }
    Ok(read)
}

/// The model a repository read is, and the derivative it was found through.
pub struct Resolved {
    pub model: HfRepo,
    /// Set when the named repository was a quantized derivative.
    pub derivative: Option<HfRepo>,
}

/// The model `repo` is: itself, or the base it quantizes.
pub async fn resolve(
    hub: &dyn Hub,
    repo: &str,
    revision: Option<&str>,
) -> Result<Resolved, String> {
    let read = read_repo(hub, repo, revision).await?;
    if !read.is_derivative() {
        return Ok(Resolved {
            model: read,
            derivative: None,
        });
    }
    let mut tried = Vec::new();
    for candidate in &read.base_candidates {
        match read_repo(hub, candidate, None).await {
            Ok(base) if !base.is_derivative() => {
                return Ok(Resolved {
                    model: base,
                    derivative: Some(read),
                })
            }
            Ok(_) => tried.push(format!("{candidate} is itself a quantization")),
            Err(why) => tried.push(why),
        }
    }
    Err(format!(
        "{repo} is a quantized derivative, but none of the models it may quantize could be read: {}",
        tried.join("; ")
    ))
}

fn now_ms() -> i64 {
    crate::fleet::unix_time_ms() as i64
}

fn model_of(read: &HfRepo, at: i64) -> Model {
    Model {
        name: read.repo.clone(),
        source: cairn_inference::RecordSource::Cairn,
        revision: Some(read.revision.clone()),
        facts: read.facts.clone(),
        read_at: Some(at),
    }
}

/// Register the model `repo` is, reading its facts, or refresh one already
/// registered; say what was registered.
pub async fn register(
    db: &LocalDb,
    hub: &dyn Hub,
    repo: &str,
    revision: Option<&str>,
) -> Result<String, String> {
    let resolved = resolve(hub, repo, revision).await?;
    let model = model_of(&resolved.model, now_ms());
    let mut records = Inventory::default();
    records.insert_model(model.clone());
    let wrote = write_inventory(
        db,
        &Authoring::Patch {
            upsert: Some(records.to_yaml()),
            remove: Removals::default(),
        },
    )
    .await?;
    let mut said = match &resolved.derivative {
        Some(derivative) => format!(
            "{repo} is a {} quantization of {}, so {} is the model and {repo}'s files are the weights of one of its profiles.\n",
            derivative.facts.quantization.as_deref().unwrap_or("quantized"),
            model.name,
            model.name
        ),
        None => String::new(),
    };
    said.push_str(&format!(
        "Model {} at revision {}: {}\n{wrote}",
        model.name,
        model.revision.as_deref().unwrap_or("unknown"),
        describe(&model)
    ));
    Ok(said)
}

/// One line of what a model's facts say, unknown facts named as such.
pub fn describe(model: &Model) -> String {
    let facts = &model.facts;
    let unknown = || "unknown".to_string();
    let params = facts
        .parameters
        .map(|p| format!("{:.1}B parameters", p as f64 / 1e9))
        .unwrap_or_else(|| "parameters unknown".into());
    let context = facts
        .context
        .map(|c| format!("{c} tokens of context"))
        .unwrap_or_else(|| "context unknown".into());
    let verbs = facts
        .verbs
        .as_ref()
        .map(|verbs| {
            verbs
                .iter()
                .map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(unknown);
    format!(
        "{}, {params}, {} weights{}, {context}, serves {verbs}",
        facts.architecture.clone().unwrap_or_else(unknown),
        facts.dtype.clone().unwrap_or_else(unknown),
        facts
            .quantization
            .as_ref()
            .map(|q| format!(" quantized {q}"))
            .unwrap_or_default()
    )
}

/// The weight sources a model can be run from: its own repository, and the
/// quantized derivatives its own organisation or a trusted quantizer
/// publishes, each read at its current revision. A derivative that cannot be
/// read is left out, with why.
///
/// Derivatives are listed per owner. A popular model has hundreds of
/// derivatives across Hugging Face, so one listing of all of them, cut at a
/// page, would leave out a trusted quantizer's at random.
pub async fn weight_sources(
    hub: &dyn Hub,
    model: &Model,
) -> Result<(Vec<HfRepo>, Vec<String>), String> {
    let own = read_repo(hub, &model.name, model.revision.as_deref()).await?;
    let mut sources = vec![own];
    let mut skipped = Vec::new();
    let base = hub.base().trim_end_matches('/');
    let org = model.name.split('/').next().unwrap_or_default();
    let mut candidates: Vec<String> = Vec::new();
    for owner in std::iter::once(org).chain(
        TRUSTED_QUANTIZERS
            .iter()
            .copied()
            .filter(|owner| *owner != org),
    ) {
        let listed = hub
            .get(format!(
                "{base}/api/models?author={owner}&filter=base_model:quantized:{}&limit=100",
                model.name
            ))
            .await?
            .unwrap_or_else(|| "[]".into());
        candidates.extend(
            serde_json::from_str::<Vec<serde_json::Value>>(&listed)
                .unwrap_or_default()
                .iter()
                .filter_map(|entry| {
                    entry
                        .get("id")
                        .or_else(|| entry.get("modelId"))?
                        .as_str()
                        .map(str::to_string)
                })
                // The listing is asked for one owner; take nobody else's.
                .filter(|id| id.split('/').next() == Some(owner)),
        );
    }
    // A GGUF repository often names the model's own base as its base, so the
    // filter misses it: look for it by name.
    let gguf = format!("{}-GGUF", model.name);
    if !candidates.contains(&gguf) {
        candidates.push(gguf);
    }
    for candidate in candidates {
        match read_repo(hub, &candidate, None).await {
            Ok(read) if read.base_candidates.contains(&model.name) => sources.push(read),
            Ok(_) => {}
            Err(why) if why.starts_with("Hugging Face has no repository") => {}
            Err(why) => skipped.push(why),
        }
    }
    Ok((sources, skipped))
}

/// One weight file as Hugging Face lists it at `revision` (a sha or a ref),
/// pinned: the revision resolved to its sha, and the file's sha256 and size
/// read rather than typed.
pub async fn weight_file(
    hub: &dyn Hub,
    repo: &str,
    revision: &str,
    path: &str,
) -> Result<WeightFile, String> {
    let read = read_repo(hub, repo, Some(revision).filter(|r| !r.is_empty())).await?;
    let file = read
        .weights
        .iter()
        .find(|file| file.path == path)
        .ok_or_else(|| {
            format!(
                "{repo} at {} has no weight file {path}",
                &read.revision[..read.revision.len().min(12)]
            )
        })?;
    Ok(WeightFile {
        repo: read.repo.clone(),
        revision: read.revision.clone(),
        path: file.path.clone(),
        sha256: file.sha256.clone(),
        size: Some(file.size),
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Hugging Face as the captured fixtures answer it.
    #[derive(Default)]
    pub(crate) struct FakeHub {
        pub(crate) bodies: BTreeMap<String, String>,
    }

    macro_rules! fixture {
        ($slug:literal) => {
            include_str!(concat!(
                "../../../cairn-inference/tests/fixtures/hf/",
                $slug,
                ".api.json"
            ))
        };
        ($slug:literal, config) => {
            include_str!(concat!(
                "../../../cairn-inference/tests/fixtures/hf/",
                $slug,
                ".config.json"
            ))
        };
    }

    fn revision_of(api: &str) -> String {
        serde_json::from_str::<serde_json::Value>(api).unwrap()["sha"]
            .as_str()
            .unwrap()
            .to_string()
    }

    impl FakeHub {
        pub(crate) fn add(&mut self, repo: &str, api: &str, config: Option<&str>) {
            let sha = revision_of(api);
            for revision in ["main".to_string(), sha.clone()] {
                self.bodies.insert(
                    format!("{HUGGING_FACE}/api/models/{repo}/revision/{revision}?blobs=true"),
                    api.to_string(),
                );
            }
            if let Some(config) = config {
                self.bodies.insert(
                    format!("{HUGGING_FACE}/{repo}/raw/{sha}/config.json"),
                    config.to_string(),
                );
            }
        }

        /// Every fixture repository.
        pub(crate) fn fixtures() -> Self {
            let mut hub = Self::default();
            hub.add(
                "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp",
                fixture!("deepseek-ai--DeepSeek-V4-Flash-Vision-Exp"),
                Some(fixture!(
                    "deepseek-ai--DeepSeek-V4-Flash-Vision-Exp",
                    config
                )),
            );
            hub.add(
                "Qwen/Qwen3.6-35B-A3B",
                fixture!("Qwen--Qwen3.6-35B-A3B"),
                Some(fixture!("Qwen--Qwen3.6-35B-A3B", config)),
            );
            hub.add(
                "nvidia/Qwen3.6-35B-A3B-NVFP4",
                fixture!("nvidia--Qwen3.6-35B-A3B-NVFP4"),
                Some(fixture!("nvidia--Qwen3.6-35B-A3B-NVFP4", config)),
            );
            hub.add(
                "Qwen/Qwen3-Embedding-4B",
                fixture!("Qwen--Qwen3-Embedding-4B"),
                Some(fixture!("Qwen--Qwen3-Embedding-4B", config)),
            );
            hub.add(
                "Qwen/Qwen3-Embedding-4B-GGUF",
                fixture!("Qwen--Qwen3-Embedding-4B-GGUF"),
                None,
            );
            hub.add(
                "mlx-community/Qwen3.6-35B-A3B-4bit",
                fixture!("mlx-community--Qwen3.6-35B-A3B-4bit"),
                Some(fixture!("mlx-community--Qwen3.6-35B-A3B-4bit", config)),
            );
            hub.add(
                "mlx-community/Qwen3-Embedding-4B-4bit-DWQ",
                fixture!("mlx-community--Qwen3-Embedding-4B-4bit-DWQ"),
                Some(fixture!(
                    "mlx-community--Qwen3-Embedding-4B-4bit-DWQ",
                    config
                )),
            );
            let listing = |owner: &str, model: &str| {
                format!("{HUGGING_FACE}/api/models?author={owner}&filter=base_model:quantized:{model}&limit=100")
            };
            hub.bodies.insert(
                listing("nvidia", "Qwen/Qwen3.6-35B-A3B"),
                r#"[{"id":"nvidia/Qwen3.6-35B-A3B-NVFP4"}]"#.into(),
            );
            hub.bodies.insert(
                listing("mlx-community", "Qwen/Qwen3.6-35B-A3B"),
                // An answer naming someone else's repository is not taken
                // as that owner's.
                r#"[{"id":"mlx-community/Qwen3.6-35B-A3B-4bit"},{"id":"someone/Qwen3.6-35B-A3B-Q2"}]"#.into(),
            );
            hub.bodies.insert(
                listing("mlx-community", "Qwen/Qwen3-Embedding-4B"),
                r#"[{"id":"mlx-community/Qwen3-Embedding-4B-4bit-DWQ"}]"#.into(),
            );
            hub
        }
    }

    impl Hub for FakeHub {
        fn fetch(&self, url: String, _headers: Vec<(&'static str, String)>) -> Fetched<'_> {
            let body = self.bodies.get(&url).cloned();
            Box::pin(async move { Ok(body) })
        }
    }

    #[tokio::test]
    async fn registering_a_derivative_registers_its_base_and_says_so() {
        let db = crate::storage::migrated_test_db("inference-models-derivative").await;
        let hub = FakeHub::fixtures();
        let said = register(&db, &hub, "nvidia/Qwen3.6-35B-A3B-NVFP4", None)
            .await
            .unwrap();
        assert!(
            said.starts_with(
                "nvidia/Qwen3.6-35B-A3B-NVFP4 is a nvfp4 quantization of Qwen/Qwen3.6-35B-A3B"
            ),
            "{said}"
        );
        assert!(said.contains("added model Qwen/Qwen3.6-35B-A3B"), "{said}");
        let inventory = super::super::inference::load_inventory(&db).await.unwrap();
        let model = &inventory.models["Qwen/Qwen3.6-35B-A3B"];
        assert_eq!(
            model.revision.as_deref(),
            Some("995ad96eacd98c81ed38be0c5b274b04031597b0")
        );
        assert_eq!(model.facts.kv_layers, Some(10));
        assert!(!inventory
            .models
            .contains_key("nvidia/Qwen3.6-35B-A3B-NVFP4"));
    }

    #[tokio::test]
    async fn the_gguf_repo_resolves_to_the_embedder_not_its_base() {
        let hub = FakeHub::fixtures();
        let resolved = resolve(&hub, "Qwen/Qwen3-Embedding-4B-GGUF", None)
            .await
            .unwrap();
        assert_eq!(resolved.model.repo, "Qwen/Qwen3-Embedding-4B");
    }

    #[tokio::test]
    async fn a_missing_or_gated_repo_is_refused_by_name() {
        let mut hub = FakeHub::fixtures();
        let refusal = read_repo(&hub, "nobody/Nothing", None).await.unwrap_err();
        assert_eq!(
            refusal,
            "Hugging Face has no repository nobody/Nothing at revision main"
        );
        hub.add(
            "meta-llama/Gated",
            r#"{"id":"meta-llama/Gated","sha":"0123456789abcdef0123456789abcdef01234567","gated":"manual","siblings":[]}"#,
            None,
        );
        let refusal = read_repo(&hub, "meta-llama/Gated", None).await.unwrap_err();
        assert!(refusal.contains("is gated"), "{refusal}");
    }

    #[tokio::test]
    async fn a_weight_file_is_read_back_pinned_to_its_revision() {
        let hub = FakeHub::fixtures();
        let repo = "Qwen/Qwen3-Embedding-4B-GGUF";
        let file = weight_file(&hub, repo, "main", "Qwen3-Embedding-4B-Q8_0.gguf")
            .await
            .unwrap();
        assert_eq!(
            file,
            WeightFile {
                repo: repo.into(),
                revision: "f4602530db1d980e16da9d7d3a70294cf5c190be".into(),
                path: "Qwen3-Embedding-4B-Q8_0.gguf".into(),
                sha256: "b60ae5ce2dd6a0b77f82cadf21def1f310a3e10cde380ad0081b07a9d416949d".into(),
                size: Some(4_279_660_224),
            }
        );
        let missing = weight_file(&hub, repo, "", "nope.gguf").await.unwrap_err();
        assert_eq!(
            missing,
            format!("{repo} at f4602530db1d has no weight file nope.gguf")
        );
    }

    #[tokio::test]
    async fn weight_sources_take_trusted_derivatives_and_the_gguf_by_name() {
        let hub = FakeHub::fixtures();
        let qwen = Model::named("Qwen/Qwen3.6-35B-A3B");
        let (sources, _) = weight_sources(&hub, &qwen).await.unwrap();
        let repos: Vec<&str> = sources.iter().map(|s| s.repo.as_str()).collect();
        assert_eq!(
            repos,
            vec![
                "Qwen/Qwen3.6-35B-A3B",
                "nvidia/Qwen3.6-35B-A3B-NVFP4",
                "mlx-community/Qwen3.6-35B-A3B-4bit"
            ],
            "someone/... is not a trusted quantizer"
        );
        assert!(sources[2].mlx, "the MLX community's weights are MLX's");
        let embedder = Model::named("Qwen/Qwen3-Embedding-4B");
        let (sources, _) = weight_sources(&hub, &embedder).await.unwrap();
        let repos: Vec<&str> = sources.iter().map(|s| s.repo.as_str()).collect();
        assert_eq!(
            repos,
            vec![
                "Qwen/Qwen3-Embedding-4B",
                "mlx-community/Qwen3-Embedding-4B-4bit-DWQ",
                "Qwen/Qwen3-Embedding-4B-GGUF"
            ]
        );
    }
}
