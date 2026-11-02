//! `run cairn://models/image` and `run cairn://models/speak`: the image and
//! speech doors as a run item answers them.
//!
//! The orchestrator chooses the model and calls the facet
//! ([`Orchestrator::draw_blocking`], [`Orchestrator::speak_blocking`]); this
//! module turns a run item's arguments into the facet's request and keeps
//! what comes back. The facet stores nothing, so the door keeps it: each
//! image and each clip becomes a durable attachment in the caller's own
//! conversation (its issue, else its thread, else its project), because a
//! run result is capped far below an image's size and a file left in the
//! caller's `$TMPDIR` would not reach a script already running in it.
//!
//! An image comes back as an image block already carrying its stored
//! address, so the agent sees the picture, the transcript cites it, and
//! settlement does not store it twice. A clip has no block to ride in, so its
//! address is in the reply. Either way the reply is the item's `data`, which
//! the SDKs return whole.

use std::collections::BTreeMap;
use std::time::Instant;

use base64::Engine;
use cairn_common::read::ImageBlock;
use cairn_common::uri::{parse_uri, CairnResource};
use cairn_models::interface::image::{
    Edit, ImageInput, ImageRequest, ImageShape, Provenance, Reference, WireExtras,
};
use cairn_models::interface::speak::{AudioFormat, SpeechRequest};
use cairn_models::types::catalog::Wire;
use cairn_models::types::image::{Orientation, Pixels, Ratio};
use cairn_models::types::usage::MeteredUsage;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::types::ItemOutcome;
use crate::attachments::{AttachmentProvenance, AttachmentScope, MAX_ATTACHMENT_BYTES};
use crate::mcp::handlers::RunContext;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

/// A stored image named by its address, as `references` and `edit` take it.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReferenceArg {
    image: String,
    #[serde(default)]
    label: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EditArg {
    image: String,
    #[serde(default)]
    mask: Option<String>,
}

/// `payload.args_json` of a `cairn://models/image` item.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ImageArgs {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    provider: Option<String>,
    prompt: String,
    #[serde(default)]
    count: Option<u32>,
    #[serde(default)]
    size: Option<Pixels>,
    #[serde(default)]
    aspect: Option<Ratio>,
    #[serde(default)]
    orientation: Option<Orientation>,
    #[serde(default)]
    references: Vec<ReferenceArg>,
    #[serde(default)]
    edit: Option<EditArg>,
    /// Keyed by wire name (`openai-images`, `openrouter-images`,
    /// `codex-app-server`).
    #[serde(default)]
    extras: BTreeMap<String, Map<String, Value>>,
}

/// `payload.args_json` of a `cairn://models/speak` item.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SpeakArgs {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    provider: Option<String>,
    text: String,
    voice: String,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    speed: Option<f32>,
    /// The provider's `instructions` speech parameter, a prompt for how the
    /// voice should sound. Named apart from the wire key so it is never read
    /// as an MCP server's `initialize` instructions.
    #[serde(default, rename = "instructions")]
    voice_style: Option<String>,
}

/// One stored image, as the image door's reply lists it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DrawnImage {
    pub uri: String,
    pub media_type: String,
    pub bytes: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revised_prompt: Option<String>,
}

/// What `run cairn://models/image` answers.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImageReply {
    pub images: Vec<DrawnImage>,
    pub provenance: Provenance,
    pub usage: MeteredUsage,
    pub latency_ms: u64,
}

/// What `run cairn://models/speak` answers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SpeakReply {
    pub uri: String,
    pub media_type: String,
    pub bytes: usize,
    /// The format the provider served (`mp3`, `pcm_24000`, ...); PCM is
    /// stored as WAV.
    pub format: String,
    pub provider: String,
    pub model: String,
    pub usage: MeteredUsage,
    pub latency_ms: u64,
}

/// Where a door keeps what it makes, and the database that holds it.
struct Keep {
    db: std::sync::Arc<LocalDb>,
    scope: AttachmentScope,
    project_key: String,
    project_id: String,
}

impl Keep {
    async fn for_run(
        orch: &Orchestrator,
        run_context: Option<&RunContext>,
        uri: &str,
    ) -> Result<Self, String> {
        let run = run_context.ok_or_else(|| {
            format!("{uri} keeps what it makes in the caller's conversation, so it needs a run: call it from an agent, or from a script inside one")
        })?;
        let db = crate::execution::routing::routing_db_for_id(&orch.db, &run.run_id)
            .await
            .map_err(|error| error.to_string())?;
        let scope = AttachmentScope::new(run.project_id.clone(), run.project_key.clone())
            .for_job(&db, &run.job_id)
            .await;
        Ok(Self {
            db,
            scope,
            project_key: run.project_key.clone(),
            project_id: run.project_id.clone(),
        })
    }

    /// The bytes of the stored image `uri` names, in this run's project.
    async fn image(&self, uri: &str) -> Result<ImageInput, String> {
        let (project, reference) = match parse_uri(uri) {
            Some(CairnResource::ProjectImage { project, reference })
            | Some(CairnResource::ProjectAttachment { project, reference }) => (project, reference),
            _ => {
                return Err(format!(
                    "'{uri}' is not a stored image; give a cairn://p/PROJECT/ISSUE/images/N address"
                ))
            }
        };
        if !project.eq_ignore_ascii_case(&self.project_key) {
            return Err(format!(
                "'{uri}' belongs to project {project}; this run can draw only from images in {}",
                self.project_key
            ));
        }
        let fetched = crate::attachments::fetch_attachment(
            &self.db,
            &self.project_id,
            &self.project_key,
            &reference,
        )
        .await
        .map_err(|error| format!("'{uri}': {error}"))?;
        if !fetched.media_type.starts_with("image/") {
            return Err(format!("'{uri}' is {}, not an image", fetched.media_type));
        }
        Ok(ImageInput {
            bytes: fetched.bytes,
            media_type: fetched.media_type,
        })
    }

    async fn store(
        &self,
        bytes: Vec<u8>,
        file_name: &str,
        media_type: &str,
    ) -> Result<crate::attachments::StoredAttachment, String> {
        crate::attachments::store_attachment_bytes(
            &self.db,
            &self.scope,
            bytes,
            file_name,
            Some(media_type),
            &AttachmentProvenance::default(),
        )
        .await
        .map_err(|error| error.to_string())
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn cost(usage: &MeteredUsage) -> String {
    usage
        .cost_usd
        .map(|cost| format!(" · ${cost:.4}"))
        .unwrap_or_default()
}

/// The image shape the arguments ask for: at most one of the three.
fn shape(args: &ImageArgs) -> Result<ImageShape, String> {
    match (args.size, args.aspect, args.orientation) {
        (None, None, None) => Ok(ImageShape::Auto),
        (Some(size), None, None) => Ok(ImageShape::Pixels(size)),
        (None, Some(aspect), None) => Ok(ImageShape::Aspect(aspect)),
        (None, None, Some(orientation)) => Ok(ImageShape::Orientation(orientation)),
        _ => Err("give at most one of size, aspect and orientation".to_string()),
    }
}

fn extras(fields: BTreeMap<String, Map<String, Value>>) -> Result<Vec<WireExtras>, String> {
    fields
        .into_iter()
        .map(|(wire, fields)| {
            let wire: Wire = serde_json::from_value(Value::String(wire.clone())).map_err(|_| {
                format!("extras are keyed by wire (openai-images, openrouter-images, codex-app-server), not '{wire}'")
            })?;
            Ok(WireExtras { wire, fields })
        })
        .collect()
}

fn extension(media_type: &str) -> &str {
    match media_type {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/webp" => "webp",
        "image/gif" => "gif",
        "image/svg+xml" => "svg",
        _ => "bin",
    }
}

/// Answer a `cairn://models/image` item. It runs to its own bound up to the
/// run ceiling, as a response item does; a draw abandoned at the bound ends
/// within the facet's own 180-second limit.
pub(super) async fn run_image(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    header: String,
    args: Value,
    timeout: Option<u32>,
) -> ItemOutcome {
    const URI: &str = "cairn://models/image";
    let args: ImageArgs = match serde_json::from_value(args) {
        Ok(args) => args,
        Err(error) => {
            return ItemOutcome::failed(
                header,
                format!("{URI} takes {{prompt, model?, provider?, count?, size?|aspect?|orientation?, references?, edit?, extras?}} in payload.args_json: {error}. Read {URI} for the shape."),
            )
        }
    };
    match draw(orch, run_context, args, timeout).await {
        Err(refusal) => ItemOutcome::failed(header, refusal),
        Ok((reply, blocks)) => ItemOutcome {
            header: format!(
                "{URI} [{} · {} · {:.1}s{}]",
                reply.provenance.provider,
                reply.provenance.model,
                reply.latency_ms as f64 / 1000.0,
                cost(&reply.usage)
            ),
            body: serde_json::to_string_pretty(&reply).unwrap_or_default(),
            succeeded: true,
            suspended: false,
            images: blocks,
            tracked_modifications: None,
            data: serde_json::to_value(&reply).ok(),
            whole: false,
        },
    }
}

async fn draw(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    args: ImageArgs,
    timeout: Option<u32>,
) -> Result<(ImageReply, Vec<ImageBlock>), String> {
    let started = Instant::now();
    let keep = Keep::for_run(orch, run_context, "cairn://models/image").await?;
    let shape = shape(&args)?;
    let mut references = Vec::new();
    for reference in &args.references {
        references.push(Reference {
            image: keep.image(&reference.image).await?,
            label: reference.label.clone(),
        });
    }
    let edit = match &args.edit {
        None => None,
        Some(edit) => Some(Edit {
            base: keep.image(&edit.image).await?,
            mask: match &edit.mask {
                Some(mask) => Some(keep.image(mask).await?),
                None => None,
            },
        }),
    };
    let request = ImageRequest {
        prompt: args.prompt,
        references,
        edit,
        count: args.count.unwrap_or(1),
        shape,
        extras: extras(args.extras)?,
    };
    let bound = super::response_item_bound(timeout);
    let limit = bound.limit;
    let project_id = keep.project_id.clone();
    let (model, provider) = (args.model, args.provider);
    let worker = orch.clone();
    let drawing = tokio::task::spawn_blocking(move || {
        worker.draw_blocking(
            Some(&project_id),
            model.as_deref(),
            provider.as_deref(),
            &request,
            limit,
        )
    });
    let (result, _) = match tokio::time::timeout(limit, drawing).await {
        Err(_) => {
            return Err(format!(
                "Drawing {} The call it abandoned ends within the facet's 180-second limit.",
                bound.describe(None)
            ))
        }
        Ok(Err(error)) => return Err(format!("the image task failed: {error}")),
        Ok(Ok(drawn)) => drawn?,
    };
    let mut images = Vec::new();
    let mut blocks = Vec::new();
    for (index, image) in result.images.into_iter().enumerate() {
        let data = base64::engine::general_purpose::STANDARD.encode(&image.bytes);
        let bytes = image.bytes.len();
        let name = format!("image-{}.{}", index + 1, extension(&image.media_type));
        let stored = keep.store(image.bytes, &name, &image.media_type).await?;
        blocks.push(ImageBlock::stored(
            image.media_type.clone(),
            data,
            stored.uri.clone(),
        ));
        images.push(DrawnImage {
            uri: stored.uri,
            media_type: image.media_type,
            bytes,
            size: image.size.map(|size| size.to_string()),
            revised_prompt: image.revised_prompt,
        });
    }
    Ok((
        ImageReply {
            images,
            provenance: result.provenance,
            usage: result.usage,
            latency_ms: elapsed_ms(started),
        },
        blocks,
    ))
}

/// Answer a `cairn://models/speak` item. It runs to its own bound up to the
/// run ceiling, as a response item does, and a clip still speaking at the
/// bound is cancelled upstream.
pub(super) async fn run_speak(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    header: String,
    args: Value,
    timeout: Option<u32>,
) -> ItemOutcome {
    const URI: &str = "cairn://models/speak";
    let args: SpeakArgs = match serde_json::from_value(args) {
        Ok(args) => args,
        Err(error) => {
            return ItemOutcome::failed(
                header,
                format!("{URI} takes {{text, voice, model?, provider?, format?, speed?, instructions?}} in payload.args_json: {error}. Read {URI} for the shape."),
            )
        }
    };
    match speak(orch, run_context, args, timeout).await {
        Err(refusal) => ItemOutcome::failed(header, refusal),
        Ok(reply) => ItemOutcome {
            header: format!(
                "{URI} [{} · {} · {} · {:.1}s{}]",
                reply.provider,
                reply.model,
                reply.format,
                reply.latency_ms as f64 / 1000.0,
                cost(&reply.usage)
            ),
            body: serde_json::to_string_pretty(&reply).unwrap_or_default(),
            succeeded: true,
            suspended: false,
            images: Vec::new(),
            tracked_modifications: None,
            data: serde_json::to_value(&reply).ok(),
            whole: false,
        },
    }
}

async fn speak(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    args: SpeakArgs,
    timeout: Option<u32>,
) -> Result<SpeakReply, String> {
    let started = Instant::now();
    let keep = Keep::for_run(orch, run_context, "cairn://models/speak").await?;
    let formats = match args.format.as_deref().map(str::trim) {
        None | Some("") => Vec::new(),
        Some(format) => vec![format.parse::<AudioFormat>()?],
    };
    let request = SpeechRequest {
        formats,
        speed: args.speed,
        instructions: args.voice_style,
        ..SpeechRequest::new(args.voice)
    };
    let bound = super::response_item_bound(timeout);
    let deadline = started + bound.limit;
    let project_id = keep.project_id.clone();
    let (model, provider, text) = (args.model, args.provider, args.text);
    let worker = orch.clone();
    // The blocking worker watches the same deadline, so it drops the stream
    // (cancelling the provider's clip) at the moment this wait gives up.
    let speaking = tokio::task::spawn_blocking(move || {
        worker.speak_blocking(
            Some(&project_id),
            model.as_deref(),
            provider.as_deref(),
            request,
            &text,
            crate::orchestrator::speak::ClipLimits {
                deadline,
                max_bytes: MAX_ATTACHMENT_BYTES,
            },
        )
    });
    let spoken = match speaking.await {
        Err(error) => return Err(format!("the speech task failed: {error}")),
        Ok(Err(refusal)) if Instant::now() >= deadline => {
            return Err(format!("Speaking {} ({refusal})", bound.describe(None)))
        }
        Ok(spoken) => spoken?,
    };
    let (extension, media_type) = spoken.file_type();
    let bytes = spoken.audio.len();
    let stored = keep
        .store(spoken.audio, &format!("speech.{extension}"), media_type)
        .await?;
    Ok(SpeakReply {
        uri: stored.uri,
        media_type: media_type.to_string(),
        bytes,
        format: spoken.format.to_string(),
        provider: spoken.offer.provider,
        model: spoken.offer.model,
        usage: spoken.usage,
        latency_ms: elapsed_ms(started),
    })
}

#[cfg(test)]
pub(super) mod tests {
    use std::time::Duration;

    use cairn_common::uri::{parse_uri, CairnResource};
    use serde_json::{json, Value};

    use super::super::types::ItemOutcome;
    use super::{DrawnImage, SpeakReply};
    use crate::models::UpdateSettings;
    use crate::orchestrator::Orchestrator;

    /// A project `prj` with issue 7 and a live run `r` on job `j` in it,
    /// whose node is `cairn://p/prj/7/1/builder`.
    pub(crate) async fn seed_run(orch: &Orchestrator) {
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',7,'T','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, branch) VALUES ('j','e','i','p','Builder','running',1,1,'builder','agent/test')",
            "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at) VALUES ('r','i','p','j','live',1,1)",
        ] {
            orch.db.local.execute(sql, ()).await.unwrap();
        }
    }

    /// One item at the dispatch arm, in run `r`'s context or in none. The
    /// run handler around it prepares the run's working tree, which a seeded
    /// project does not have; what the door itself does is all here.
    async fn item(orch: &Orchestrator, in_run: bool, door: &str, args: Value) -> ItemOutcome {
        let context = match in_run {
            true => Some(
                crate::mcp::handlers::run_context::lookup_run_by_id(&orch.db.local, "r")
                    .await
                    .unwrap(),
            ),
            false => None,
        };
        let header = door.to_string();
        match door {
            "cairn://models/image" => {
                super::run_image(orch, context.as_ref(), header, args, None).await
            }
            _ => super::run_speak(orch, context.as_ref(), header, args, None).await,
        }
    }

    async fn image(orch: &Orchestrator, in_run: bool, args: Value) -> ItemOutcome {
        item(orch, in_run, "cairn://models/image", args).await
    }

    /// The bytes a stored address names.
    async fn stored(orch: &Orchestrator, uri: &str) -> Vec<u8> {
        let reference = match parse_uri(uri) {
            Some(CairnResource::ProjectImage { reference, .. })
            | Some(CairnResource::ProjectAttachment { reference, .. }) => reference,
            other => panic!("'{uri}' is not a stored address: {other:?}"),
        };
        crate::attachments::fetch_attachment(&orch.db.local, "p", "prj", &reference)
            .await
            .unwrap()
            .bytes
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_drawn_image_is_seen_stored_in_the_issue_and_returned_whole() {
        let (orch, studio, key) = crate::orchestrator::image::tests::workspace().await;
        seed_run(&orch).await;
        let outcome = image(
            &orch,
            true,
            json!({"model": "gpt-image-1", "prompt": "a lighthouse"}),
        )
        .await;
        assert!(outcome.succeeded, "{}", outcome.body);
        let data = outcome.data.expect("the reply rides whole in data");
        let images: Vec<DrawnImage> = serde_json::from_value(data["images"].clone()).unwrap();
        assert_eq!(images.len(), 1);
        let uri = &images[0].uri;
        assert!(uri.starts_with("cairn://p/prj/7/images/"), "{uri}");
        assert_eq!(images[0].media_type, "image/png");
        assert_eq!(data["provenance"]["provider"], json!(key));
        assert_eq!(outcome.images.len(), 1, "the agent sees the picture");
        assert_eq!(
            outcome.images[0].uri.as_deref(),
            Some(uri.as_str()),
            "already addressed, so settlement cites it and stores nothing twice"
        );
        assert!(outcome.body.contains(uri.as_str()), "{}", outcome.body);
        assert!(outcome.header.contains(&key), "{}", outcome.header);
        assert_eq!(
            stored(&orch, uri).await,
            crate::orchestrator::image::tests::PNG
        );
        assert_eq!(studio.count(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_doors_refuse_a_caller_with_no_run_and_an_unset_default_by_name() {
        let (orch, studio, _) = crate::orchestrator::image::tests::workspace().await;
        seed_run(&orch).await;
        let refused = |outcome: ItemOutcome, expected: &str| {
            assert!(!outcome.succeeded, "{}", outcome.body);
            assert!(outcome.data.is_none());
            assert!(outcome.body.contains(expected), "{}", outcome.body);
        };
        refused(
            image(&orch, false, json!({"model": "gpt-image-1", "prompt": "x"})).await,
            "needs a run",
        );
        refused(
            image(&orch, true, json!({"prompt": "x"})).await,
            "`imageModel`",
        );
        refused(
            image(
                &orch,
                true,
                json!({"prompt": "x", "size": "1024x1024", "orientation": "square"}),
            )
            .await,
            "at most one of size",
        );
        refused(
            image(&orch, true, json!({"prompt": "x", "extras": {"fax": {}}})).await,
            "keyed by wire",
        );
        refused(
            image(&orch, true, json!({"prompt": "x", "promt": "y"})).await,
            "Read cairn://models/image for the shape",
        );
        refused(
            image(
                &orch,
                true,
                json!({"prompt": "x", "model": "gpt-image-1",
                       "references": [{"image": "cairn://p/other/1/images/1"}]}),
            )
            .await,
            "belongs to project other",
        );
        assert_eq!(studio.count(), 0, "nothing was drawn");

        orch.update_settings(UpdateSettings {
            image_model: Some(Some("gpt-image-1".into())),
            ..Default::default()
        })
        .unwrap();
        let outcome = image(&orch, true, json!({"prompt": "a lighthouse"})).await;
        assert!(outcome.succeeded, "{}", outcome.body);
        assert_eq!(studio.count(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_spoken_clip_is_stored_in_the_issue_and_named_in_the_reply() {
        let voicebox =
            crate::orchestrator::speak::tests::Voicebox::new(&[b"ID3a", b"bc"], Duration::ZERO);
        let (orch, _, key) = crate::orchestrator::speak::tests::workspace(voicebox).await;
        seed_run(&orch).await;
        let outcome = item(
            &orch,
            true,
            "cairn://models/speak",
            json!({"model": "tts-1", "text": "Hello there.", "voice": "alloy", "format": "mp3"}),
        )
        .await;
        assert!(outcome.succeeded, "{}", outcome.body);
        let reply: SpeakReply = serde_json::from_value(outcome.data.unwrap()).unwrap();
        assert!(
            reply.uri.starts_with("cairn://p/prj/7/attachments/"),
            "{}",
            reply.uri
        );
        assert_eq!(reply.provider, key);
        assert_eq!(reply.format, "mp3");
        assert_eq!(reply.media_type, "audio/mpeg");
        assert_eq!(reply.bytes, 6);
        assert_eq!(stored(&orch, &reply.uri).await, b"ID3abc");
        assert!(outcome.body.contains(&reply.uri), "{}", outcome.body);
        assert!(outcome.images.is_empty());

        let outcome = item(
            &orch,
            true,
            "cairn://models/speak",
            json!({"model": "tts-1", "text": "Hi.", "voice": "alloy", "format": "flac"}),
        )
        .await;
        assert!(!outcome.succeeded);
        assert!(
            outcome.body.contains("unknown audio encoding"),
            "{}",
            outcome.body
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_voice_style_under_the_wire_key_instructions_reaches_the_speech_request() {
        let voicebox = crate::orchestrator::speak::tests::Voicebox::new(&[b"ID3"], Duration::ZERO);
        let (orch, voicebox, _) =
            crate::orchestrator::speak::tests::offering(voicebox, &["gpt-4o-mini-tts"]).await;
        seed_run(&orch).await;
        let outcome = item(
            &orch,
            true,
            "cairn://models/speak",
            json!({"model": "gpt-4o-mini-tts", "text": "Hi.", "voice": "alloy", "format": "mp3",
                   "instructions": "Speak softly."}),
        )
        .await;
        assert!(outcome.succeeded, "{}", outcome.body);
        let asked = voicebox.asked.lock().unwrap();
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].3.as_deref(), Some("Speak softly."));
    }
}
