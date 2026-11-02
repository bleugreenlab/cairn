//! `run cairn://models/embed`: the embed door as a run item answers it.
//!
//! The orchestrator embeds ([`Orchestrator::embed_named`]); this module keeps
//! what comes back where the caller works next. The whole reply, with the
//! input texts, is put into the calling node's scratch as
//! `embeddings/<stamp>-<id>.json`, and the vectors beside it as a float32
//! `.npy` of shape `[N, dims]` ([`crate::node_scratch::put`]). The agent reads
//! one line of description and the two paths, and works on the vectors in its
//! next `run` without carrying them through its transcript. The item's `data`,
//! which the SDKs return, still carries every vector, with the paths.
//!
//! Paths are said relative to `$TMPDIR`, never as one machine's absolute
//! path: the scratch sits somewhere different on every machine, and the
//! node's next batch may run on any of them.
//!
//! A reply that is not saved (the caller has no node scratch, passed
//! `save: false`, or the scratch refused it) has no file to point at, so its
//! text is the whole reply, as `data` is.

use serde::{Deserialize, Serialize};

use super::types::ItemOutcome;
use crate::mcp::handlers::RunContext;
use crate::orchestrator::embed::{EmbedDoorRequest, EmbedOutcome, EmbedUsage};
use crate::orchestrator::Orchestrator;

const URI: &str = "cairn://models/embed";

/// The scratch directory saved replies go in.
const DIRECTORY: &str = "embeddings";

/// The writer a saved reply names in the node's next sync line.
const WRITER: &str = "models-embed";

/// Where the door saved a reply, relative to the node's `$TMPDIR`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SavedEmbeddings {
    /// The whole reply and the input texts.
    pub json: String,
    /// The vectors as float32, `[N, dims]`. Absent only when the vectors are
    /// not all `dims` wide, which no offer returns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub npy: Option<String>,
}

/// What the door answers in `data`: the facet's reply, and where it was saved.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EmbedReply<'a> {
    #[serde(flatten)]
    outcome: &'a EmbedOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    saved: Option<&'a SavedEmbeddings>,
}

/// The saved JSON: provenance first, so the head of the file says what it
/// is, then the texts and their vectors in input order.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EmbedFile<'a> {
    model: &'a str,
    dims: u32,
    provider: &'a str,
    usage: &'a EmbedUsage,
    #[serde(skip_serializing_if = "Option::is_none")]
    cost: Option<f64>,
    latency_ms: u64,
    input: &'a [String],
    vectors: &'a [Vec<f32>],
}

/// What became of saving a reply.
enum Kept {
    Saved(SavedEmbeddings),
    /// No node scratch to save into, or the caller passed `save: false`.
    Unsaved,
    Refused(String),
}

/// Answer a `cairn://models/embed` item, bounded by the item's timeout under
/// the host-item cap. A timeout releases the caller at once, and the call it
/// abandons sends no further request.
pub(super) async fn run_embed(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    header: String,
    args: serde_json::Value,
    timeout: Option<u32>,
) -> ItemOutcome {
    let request: EmbedDoorRequest = match serde_json::from_value(args) {
        Ok(request) => request,
        Err(error) => {
            return ItemOutcome::failed(
                header,
                format!(
                    "{URI} takes {{model, input, purpose?, width?, save?}} in payload.args_json: {error}. Read {URI} for the shape."
                ),
            )
        }
    };
    let (input, save) = (request.input.clone(), request.save);
    let bound = super::host_item_bound(timeout);
    let project_id = run_context.map(|context| context.project_id.as_str());
    let outcome =
        match tokio::time::timeout(bound.limit, orch.embed_named(project_id, request)).await {
            Err(_) => {
                return ItemOutcome::failed(
                    header,
                    format!(
                        "Embedding {} The call it abandoned sends no further request, and the one in flight ends within 60 s.",
                        bound.describe(None)
                    ),
                )
            }
            Ok(Err(refusal)) => return ItemOutcome::failed(header, refusal),
            Ok(Ok(outcome)) => outcome,
        };
    let kept = match save {
        true => keep(orch, run_context, &outcome, &input).await,
        false => Kept::Unsaved,
    };
    let saved = match &kept {
        Kept::Saved(saved) => Some(saved),
        _ => None,
    };
    let reply = EmbedReply {
        outcome: &outcome,
        saved,
    };
    let body = match &kept {
        Kept::Saved(saved) => format!("{}\n{}", describe(&outcome), where_saved(&outcome, saved)),
        Kept::Unsaved => serde_json::to_string(&reply).unwrap_or_default(),
        Kept::Refused(why) => format!(
            "Not saved to `$TMPDIR`: {why}.\n{}",
            serde_json::to_string(&reply).unwrap_or_default()
        ),
    };
    ItemOutcome {
        header: format!(
            "{URI} [{} · {} · {}×{} · {:.1}s]",
            outcome.provider,
            outcome.model,
            outcome.vectors.len(),
            outcome.dims,
            outcome.latency_ms as f64 / 1000.0
        ),
        body,
        succeeded: true,
        suspended: false,
        images: Vec::new(),
        tracked_modifications: None,
        data: serde_json::to_value(&reply).ok(),
        whole: false,
    }
}

/// Save `outcome` into the calling node's scratch.
async fn keep(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    outcome: &EmbedOutcome,
    input: &[String],
) -> Kept {
    let Some(run) = run_context else {
        return Kept::Unsaved;
    };
    let db = match crate::execution::routing::routing_db_for_id(&orch.db, &run.run_id).await {
        Ok(db) => db,
        Err(error) => return Kept::Refused(format!("the run's database is unavailable: {error}")),
    };
    crate::node_scratch::lifecycle::ensure_subscribed(orch);
    let scratch = match crate::node_scratch::resolve_scratch(orch, &db, &run.job_id).await {
        Ok(Some(resolved)) => resolved.scratch,
        Ok(None) => return Kept::Unsaved,
        Err(error) => return Kept::Refused(error),
    };
    let stem = format!(
        "{DIRECTORY}/{}-{}",
        chrono::Utc::now().format("%Y%m%d-%H%M%S"),
        &uuid::Uuid::new_v4().simple().to_string()[..6]
    );
    let file = EmbedFile {
        model: &outcome.model,
        dims: outcome.dims,
        provider: &outcome.provider,
        usage: &outcome.usage,
        cost: outcome.cost,
        latency_ms: outcome.latency_ms,
        input,
        vectors: &outcome.vectors,
    };
    let json = match serde_json::to_vec(&file) {
        Ok(json) => json,
        Err(error) => {
            return Kept::Refused(format!("the reply could not be written as JSON: {error}"))
        }
    };
    let saved = SavedEmbeddings {
        json: format!("{stem}.json"),
        npy: None,
    };
    let mut files = vec![crate::node_scratch::put::PutFile {
        path: saved.json.clone(),
        bytes: json,
    }];
    let npy = npy(&outcome.vectors, outcome.dims as usize).map(|bytes| {
        let path = format!("{stem}.npy");
        files.push(crate::node_scratch::put::PutFile {
            path: path.clone(),
            bytes,
        });
        path
    });
    match crate::node_scratch::put::put(orch, &scratch.id, files, WRITER).await {
        Ok(()) => Kept::Saved(SavedEmbeddings { npy, ..saved }),
        Err(refusal) => Kept::Refused(refusal),
    }
}

/// `N × D · model · provider · cost · latency`.
fn describe(outcome: &EmbedOutcome) -> String {
    format!(
        "{} × {} · {} · {} · {} · {:.1}s",
        outcome.vectors.len(),
        outcome.dims,
        outcome.model,
        outcome.provider,
        outcome
            .cost
            .map_or_else(|| "cost not reported".to_string(), usd),
        outcome.latency_ms as f64 / 1000.0
    )
}

fn where_saved(outcome: &EmbedOutcome, saved: &SavedEmbeddings) -> String {
    let json = format!(
        "$TMPDIR/{} (the whole reply, with the input texts)",
        saved.json
    );
    match &saved.npy {
        Some(npy) => format!(
            "Saved to {json} and $TMPDIR/{npy} (float32, {} × {}).",
            outcome.vectors.len(),
            outcome.dims
        ),
        None => format!("Saved to {json}."),
    }
}

/// A dollar amount to three significant figures, since an embedding's cost
/// is often a small fraction of a cent.
fn usd(cost: f64) -> String {
    let decimals = match cost > 0.0 {
        true => (2 - cost.log10().floor() as i32).clamp(2, 10) as usize,
        false => 2,
    };
    format!("${cost:.decimals$}")
}

/// `vectors` as a NumPy `.npy` (format 1.0): little-endian float32 of shape
/// `[N, dims]`, its data 64-byte aligned as NumPy writes it, so it loads
/// with `np.load` and as a `Float32Array` at the offset the header gives.
/// `None` when a vector is not `dims` wide.
fn npy(vectors: &[Vec<f32>], dims: usize) -> Option<Vec<u8>> {
    if vectors.iter().any(|vector| vector.len() != dims) {
        return None;
    }
    let mut header = format!(
        "{{'descr': '<f4', 'fortran_order': False, 'shape': ({}, {dims}), }}",
        vectors.len()
    );
    // The magic, version and length (10 bytes), the header, and its newline.
    let unpadded = 10 + header.len() + 1;
    header.push_str(&" ".repeat((64 - unpadded % 64) % 64));
    header.push('\n');
    let mut bytes = Vec::with_capacity(10 + header.len() + vectors.len() * dims * 4);
    bytes.extend_from_slice(b"\x93NUMPY\x01\x00");
    bytes.extend_from_slice(&u16::try_from(header.len()).ok()?.to_le_bytes());
    bytes.extend_from_slice(header.as_bytes());
    for value in vectors.iter().flatten() {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::super::types::ItemOutcome;
    use super::SavedEmbeddings;
    use crate::orchestrator::embed::EmbedOutcome;
    use crate::orchestrator::Orchestrator;

    /// Three 1536-wide vectors whose components each print with many digits,
    /// so a text holding any of them would show it.
    fn vector(seed: usize) -> Vec<f32> {
        (0..1536)
            .map(|i| ((seed * 1536 + i) as f32 * 0.618_034).sin() * 0.087_654_32)
            .collect()
    }

    async fn hub() -> Orchestrator {
        let data: Vec<Value> = (0..3)
            .map(|index| json!({"index": index, "embedding": vector(index)}))
            .collect();
        let (orch, _, _) = crate::orchestrator::embed::tests::workspace_with_hub_of(
            1536,
            vec![
                json!({"model": "qwen3-embedding-4b", "data": data, "usage": {"prompt_tokens": 3}}),
            ],
        )
        .await;
        super::super::media::tests::seed_run(&orch).await;
        orch
    }

    async fn embed(orch: &Orchestrator, in_run: bool, save: Option<bool>) -> ItemOutcome {
        let context = match in_run {
            true => Some(
                crate::mcp::handlers::run_context::lookup_run_by_id(&orch.db.local, "r")
                    .await
                    .unwrap(),
            ),
            false => None,
        };
        let mut args =
            json!({"model": "qwen/qwen3-embedding-4b", "input": ["one", "two", "three"]});
        if let Some(save) = save {
            args["save"] = json!(save);
        }
        super::run_embed(orch, context.as_ref(), super::URI.into(), args, None).await
    }

    /// The bytes at `path` in the head of run `r`'s node scratch, if any.
    async fn in_scratch(orch: &Orchestrator, path: &str) -> Option<Vec<u8>> {
        let scratch = crate::node_scratch::resolve_scratch(orch, &orch.db.local, "j")
            .await
            .unwrap()
            .expect("run r's job belongs to a node")
            .scratch;
        let repo =
            crate::node_scratch::store::open(&orch.config_dir, &scratch.project_id, &scratch.id)
                .unwrap();
        let file = repo
            .files(scratch.head_commit.as_deref())
            .unwrap()
            .into_iter()
            .find(|file| file.path == path.as_bytes())?;
        Some(repo.run(["cat-file", "blob", &file.oid], None).unwrap())
    }

    /// A `.npy` read back: its shape as the header declares it, and its data.
    fn read_npy(bytes: &[u8]) -> (String, Vec<f32>) {
        assert_eq!(&bytes[..8], b"\x93NUMPY\x01\x00");
        let offset = 10 + u16::from_le_bytes([bytes[8], bytes[9]]) as usize;
        assert_eq!(offset % 64, 0, "the data is 64-byte aligned");
        let header = std::str::from_utf8(&bytes[10..offset]).unwrap();
        assert!(header.contains("'descr': '<f4'"), "{header}");
        assert!(header.ends_with('\n'), "{header:?}");
        let shape = header[header.find("'shape': ").unwrap() + 9..]
            .split(')')
            .next()
            .unwrap()
            .to_string()
            + ")";
        let data = bytes[offset..]
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        (shape, data)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_saved_reply_is_a_description_and_paths_in_the_text_and_whole_in_the_scratch_and_data(
    ) {
        let orch = hub().await;
        let outcome = embed(&orch, true, None).await;
        assert!(outcome.succeeded, "{}", outcome.body);
        let expected: Vec<Vec<f32>> = (0..3).map(vector).collect();

        // Scripts see exactly the vectors they always did, and the paths.
        let data = outcome.data.clone().expect("the reply rides in data");
        let reply: EmbedOutcome = serde_json::from_value(data.clone()).unwrap();
        assert_eq!(reply.vectors, expected);
        assert_eq!(reply.dims, 1536);
        let saved: SavedEmbeddings = serde_json::from_value(data["saved"].clone()).unwrap();
        assert!(saved.json.starts_with("embeddings/"), "{saved:?}");
        let npy = saved.npy.clone().expect("a float32 copy beside the JSON");
        assert_eq!(npy, saved.json.replace(".json", ".npy"));

        // The agent reads a line of description and where the vectors are.
        let text = &outcome.body;
        assert!(
            text.starts_with(&format!(
                "3 × 1536 · qwen/qwen3-embedding-4b · {} · cost not reported · ",
                reply.provider
            )),
            "{text}"
        );
        assert!(text.contains(&format!("$TMPDIR/{}", saved.json)), "{text}");
        assert!(text.contains(&format!("$TMPDIR/{npy}")), "{text}");
        assert!(text.len() < 400, "{text}");
        assert!(!text.contains('['), "{text}");
        let shown = expected
            .iter()
            .flatten()
            .map(f32::to_string)
            // Long enough not to occur in a timestamp or a count by chance.
            .filter(|value| value.len() >= 8)
            .find(|value| text.contains(value.as_str()));
        assert_eq!(shown, None, "no vector component is in the text: {text}");

        // The paths exist, and hold exactly what data does.
        let json = in_scratch(&orch, &saved.json)
            .await
            .expect("the JSON is in the node's scratch");
        let file: Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(file["input"], json!(["one", "two", "three"]));
        assert_eq!(file["model"], json!("qwen/qwen3-embedding-4b"));
        assert_eq!(file["provider"], json!(reply.provider));
        assert_eq!(file["dims"], json!(1536));
        assert_eq!(file["usage"], json!({"inputTokens": 3}));
        let from_file: Vec<Vec<f32>> = serde_json::from_value(file["vectors"].clone()).unwrap();
        assert_eq!(from_file, reply.vectors);
        let (shape, flat) = read_npy(&in_scratch(&orch, &npy).await.expect("the .npy too"));
        assert_eq!(shape, "(3, 1536)");
        assert_eq!(flat, reply.vectors.concat());
    }

    /// With no file to point at, the text is the whole reply, as before.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unsaved_reply_is_whole_in_the_text() {
        let orch = hub().await;
        let declined = embed(&orch, true, Some(false)).await;
        assert!(declined.succeeded, "{}", declined.body);
        let text: EmbedOutcome = serde_json::from_str(&declined.body).unwrap();
        assert_eq!(text.vectors, (0..3).map(vector).collect::<Vec<_>>());
        assert!(declined.data.unwrap().get("saved").is_none());
        let scratch = crate::node_scratch::resolve_scratch(&orch, &orch.db.local, "j")
            .await
            .unwrap()
            .unwrap()
            .scratch;
        assert_eq!(scratch.head_commit, None, "save: false writes nothing");

        let orch = hub().await;
        let nowhere = embed(&orch, false, None).await;
        let text: EmbedOutcome = serde_json::from_str(&nowhere.body).unwrap();
        assert_eq!(text.vectors.len(), 3);
        assert!(nowhere.data.unwrap().get("saved").is_none());
    }

    #[test]
    fn a_cost_keeps_three_significant_figures() {
        assert_eq!(super::usd(0.000_012_34), "$0.0000123");
        assert_eq!(super::usd(0.5), "$0.500");
        assert_eq!(super::usd(12.0), "$12.00");
        assert_eq!(super::usd(0.0), "$0.00");
    }

    #[test]
    fn ragged_vectors_have_no_npy() {
        assert!(super::npy(&[vec![1.0, 2.0], vec![3.0]], 2).is_none());
        let (shape, flat) = read_npy(&super::npy(&[vec![1.0, 2.0]], 2).unwrap());
        assert_eq!((shape.as_str(), flat), ("(1, 2)", vec![1.0, 2.0]));
    }
}
