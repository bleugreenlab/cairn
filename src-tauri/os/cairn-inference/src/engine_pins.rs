//! The engines the shipped templates run, pinned outside the templates so a
//! new engine release reaches every install without an app release.
//!
//! `engine-pins.json` (beside this crate's `Cargo.toml`) is the one place an
//! engine's release is written: per template, a container image pinned by
//! digest with the tag it was read from, or a native artifact pinned by URL,
//! version and sha256 with the program in it. `templates.yaml` says how each
//! engine is run and on what hardware, and never which release.
//!
//! The file is compiled in as the fallback, published to the rolling
//! `engine-pins-v{ENGINE_PINS_SCHEMA}` release when a change to it merges, and
//! followed at run time by the orchestrator with the managed CLIs' rule
//! (`cairn_models::managed::follow`). Following decides what new profiles
//! start from (generation, import, the template list). It never changes an
//! existing profile: each profile embeds the exact image or artifact it was
//! verified with, so its measured speed, fit and quality stay that engine's,
//! and a deployment renders from its profile. [`EnginePins::lag`] reports a
//! profile whose engine is not the current pin, which the operator may
//! re-verify onto it.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::fit::{base_templates, Template};
use crate::profile::{is_pinned_image, Artifact, Engine, InferenceProfile, Native, ProfileOrigin};

/// The shape of `engine-pins.json` this build reads. A file for any other
/// schema is refused, and a change of shape publishes under a new release
/// tag, so a build never reads a file it cannot.
pub const ENGINE_PINS_SCHEMA: u32 = 1;

/// The pin file's name, in the repository and in the release.
pub const ENGINE_PINS_FILE: &str = "engine-pins.json";

/// The rolling release this build's engine pins are published under.
pub fn release_tag() -> String {
    format!("engine-pins-v{ENGINE_PINS_SCHEMA}")
}

/// `engine-pins.json` as written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EnginePinFile {
    pub schema: u32,
    /// By template id. An entry for a template this build does not ship is
    /// a newer build's, and is ignored.
    pub engines: BTreeMap<String, EnginePin>,
}

/// One template's engine release.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EnginePin {
    /// Why this release: what it is and what it lacks.
    pub reason: String,
    /// A container template's image, `name@sha256:<64 hex>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// The tag the image's digest was read from (`server-cuda`, `v0.32.0`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// A native template's artifact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<Artifact>,
    /// The serving program in the artifact: a path inside an archive, which
    /// names the release (`llama-b12283/llama-server`), or a wheel's console
    /// script.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program: Option<String>,
}

impl EnginePin {
    /// The release, as a person reads it: a native artifact's version, or a
    /// container's tag and the start of its digest.
    pub fn label(&self) -> String {
        match (&self.artifact, &self.image) {
            (Some(artifact), _) => artifact.version.clone(),
            (None, Some(image)) => format!(
                "{} ({})",
                self.tag.as_deref().unwrap_or("untagged"),
                short_digest(image)
            ),
            (None, None) => "unpinned".into(),
        }
    }
}

/// `sha256:` and the first 12 hex digits of an image's digest.
fn short_digest(image: &str) -> String {
    match image.rsplit_once("@sha256:") {
        Some((_, digest)) => format!("sha256:{}", &digest[..digest.len().min(12)]),
        None => image.to_string(),
    }
}

/// Engine pins validated against the templates this build ships, and those
/// templates with their engines filled in. Parsing is validating: a value of
/// this type pins every shipped template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "EnginePinFile", into = "EnginePinFile")]
pub struct EnginePins {
    file: EnginePinFile,
    templates: Vec<Template>,
}

impl TryFrom<EnginePinFile> for EnginePins {
    type Error = String;

    fn try_from(file: EnginePinFile) -> Result<Self, String> {
        if file.schema != ENGINE_PINS_SCHEMA {
            return Err(format!(
                "the engine pins are schema {}, and this build reads schema {ENGINE_PINS_SCHEMA}",
                file.schema
            ));
        }
        let mut templates = Vec::new();
        for base in base_templates() {
            let id = &base.id;
            let Some(pin) = file.engines.get(id) else {
                return Err(format!("the engine pins pin no engine for template {id}"));
            };
            let problem = |why: &str| Err(format!("the engine pin for template {id}: {why}"));
            if pin.reason.trim().is_empty() {
                return problem("reason is empty");
            }
            let command = match (&base.args, pin) {
                (
                    None,
                    EnginePin {
                        image: Some(image),
                        tag: Some(tag),
                        artifact: None,
                        program: None,
                        ..
                    },
                ) => {
                    if !is_pinned_image(image) {
                        return problem("image is not pinned by digest (name@sha256:<64 hex>)");
                    }
                    if tag.is_empty()
                        || tag
                            .chars()
                            .any(|c| c.is_whitespace() || c.is_control() || c == '@')
                    {
                        return problem("tag is not a tag name");
                    }
                    base.command.clone().unwrap_or_default()
                }
                (
                    Some(args),
                    EnginePin {
                        artifact: Some(artifact),
                        program: Some(program),
                        image: None,
                        tag: None,
                        ..
                    },
                ) => {
                    if let Some(why) = artifact.problem() {
                        return problem(why);
                    }
                    let native = Native {
                        server: base.server,
                        artifact: artifact.clone(),
                        program: program.clone(),
                        args: args.clone(),
                        env: BTreeMap::new(),
                    };
                    if let Some(why) = native.program_problem() {
                        return problem(why);
                    }
                    std::iter::once(program.clone())
                        .chain(args.iter().cloned())
                        .collect()
                }
                (None, _) => {
                    return problem("a container template's engine is an image and the tag it was read from, and nothing else")
                }
                (Some(_), _) => {
                    return problem("a native template's engine is an artifact and the program in it, and nothing else")
                }
            };
            templates.push(Template {
                id: id.clone(),
                server: base.server,
                family: base.family,
                runtime: base.runtime,
                arch: base.arch,
                image: pin.image.clone(),
                artifact: pin.artifact.clone(),
                formats: base.formats.clone(),
                command,
                notes: base.notes.clone(),
            });
        }
        Ok(Self { file, templates })
    }
}

impl From<EnginePins> for EnginePinFile {
    fn from(pins: EnginePins) -> Self {
        pins.file
    }
}

/// A profile whose engine is not its template's current pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PinLag {
    /// The template the profile runs the engine of.
    pub template: String,
    /// The release the profile runs (and was verified with, if it was).
    pub runs: String,
    /// The template's current pin.
    pub pinned: String,
}

impl EnginePins {
    /// Parse and validate a pin file's text.
    pub fn parse(text: &str) -> Result<Self, String> {
        let file: EnginePinFile = serde_json::from_str(text)
            .map_err(|error| format!("the engine pins do not parse: {error}"))?;
        Self::try_from(file)
    }

    /// The pins compiled into this build.
    pub fn embedded() -> &'static Self {
        static EMBEDDED: OnceLock<EnginePins> = OnceLock::new();
        EMBEDDED.get_or_init(|| {
            Self::parse(include_str!("../engine-pins.json"))
                .unwrap_or_else(|why| panic!("engine-pins.json: {why}"))
        })
    }

    /// The shipped templates, each running its pinned engine.
    pub fn templates(&self) -> &[Template] {
        &self.templates
    }

    pub fn template(&self, id: &str) -> Option<&Template> {
        self.templates.iter().find(|template| template.id == id)
    }

    pub fn pin(&self, template: &str) -> Option<&EnginePin> {
        self.file.engines.get(template)
    }

    /// The template whose engine `profile` runs: the one it was generated
    /// from, when that is the same kind of engine for the same server, else
    /// the one template running the same server the same way (natively, or
    /// from the same image repository on the profile's runtime). `None` when
    /// nothing, or more than one, matches.
    fn template_of(&self, profile: &InferenceProfile) -> Option<&Template> {
        let same_engine = |template: &&Template| match &profile.engine {
            Engine::Native(native) => {
                template.artifact.is_some() && template.server == native.server
            }
            Engine::Container(container) => {
                template.server == container.server
                    && template.image.as_deref().map(repository)
                        == Some(repository(&container.image))
            }
            Engine::Command { .. } => false,
        };
        if let ProfileOrigin::Generated {
            starting_template: Some(id),
            ..
        } = &profile.origin
        {
            if let Some(template) = self.template(id).filter(same_engine) {
                return Some(template);
            }
        }
        let runtime = profile.requirements.accelerator.map(|a| a.runtime);
        let mut matching = self
            .templates
            .iter()
            .filter(same_engine)
            .filter(|template| runtime.is_none_or(|runtime| template.runtime == runtime));
        let only = matching.next()?;
        matching.next().is_none().then_some(only)
    }

    /// Whether `profile` runs an engine other than its template's current
    /// pin, and which. Nothing moves a profile onto a new pin but the
    /// operator's re-verify (cairn-core's `inference_reverify`), which moves
    /// it only once a verification of the new engine passes.
    pub fn lag(&self, profile: &InferenceProfile) -> Option<PinLag> {
        let template = self.template_of(profile)?;
        let pin = self.pin(&template.id)?;
        let runs = match (&profile.engine, &pin.artifact, &pin.image) {
            (Engine::Native(native), Some(pinned), _) => {
                if native.artifact.sha256 == pinned.sha256 {
                    return None;
                }
                native.artifact.version.clone()
            }
            (Engine::Container(container), _, Some(pinned)) => {
                if container.image == *pinned {
                    return None;
                }
                short_digest(&container.image)
            }
            _ => return None,
        };
        Some(PinLag {
            template: template.id.clone(),
            runs,
            pinned: pin.label(),
        })
    }
}

/// An image reference's repository: everything before its digest or tag.
fn repository(image: &str) -> &str {
    let name = image.split_once('@').map_or(image, |(name, _)| name);
    match name.rsplit_once(':') {
        Some((repo, tag)) if !tag.contains('/') => repo,
        _ => name,
    }
}
