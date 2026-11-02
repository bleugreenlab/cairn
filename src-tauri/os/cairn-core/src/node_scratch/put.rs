//! Files the runner writes into a node's scratch itself.
//!
//! A door that runs on the host can make data its caller works on next (the
//! embed door's vectors). A put lands that data in the node's scratch the way
//! a machine's write-back does, with the runner as the writer: under the
//! scratch's write lock it folds the files into the head through the same
//! merge and ceiling, commits, and advances the head by compare-and-swap. The
//! node's next batch, terminal or REPL brings the files into `$TMPDIR` on
//! whatever machine it runs; work already running sees them as it sees
//! another machine's writes, from its next sync on.

use cairn_common::executor_protocol::{ScratchBlob, ScratchChange, ScratchPath, ScratchRefusal};

use super::{merge, owner, report, store};
use crate::orchestrator::Orchestrator;

/// A regular file's Git mode.
const FILE_MODE: u32 = 0o100644;

/// One file to put, at a `/`-separated path relative to the scratch root.
pub struct PutFile {
    pub path: String,
    pub bytes: Vec<u8>,
}

/// Put `files` into scratch `scratch_id`, refused by name past the project's
/// ceiling (a refusal moves nothing). `writer` names the runner-side writer:
/// the next batch's sync line names it as where the head came from, and a
/// conflict name ends with it, so it must be a single path segment.
pub async fn put(
    orch: &Orchestrator,
    scratch_id: &str,
    files: Vec<PutFile>,
    writer: &str,
) -> Result<(), String> {
    let lock = orch.object_plane.scratch.lock(scratch_id);
    let _held = lock.lock().await;
    let scratch = match owner::load(&orch.db.local, scratch_id).await {
        Ok(Some(scratch)) if scratch.closed_at.is_none() => scratch,
        Ok(Some(scratch)) => return Err(format!("the scratch of {} is closed", scratch.owner_uri)),
        Ok(None) => return Err(format!("there is no node scratch {scratch_id}")),
        Err(error) => return Err(format!("reading node scratch {scratch_id} failed: {error}")),
    };
    let ceiling = owner::ceiling_bytes(orch, &scratch.project_id).await;
    let config_dir = orch.config_dir.clone();
    let folding = {
        let scratch = scratch.clone();
        let writer = writer.to_owned();
        tokio::task::spawn_blocking(move || {
            let repo = store::open(&config_dir, &scratch.project_id, &scratch.id)?;
            let head = store::tree(&repo, scratch.head_commit.as_deref())?;
            let mut changes = Vec::with_capacity(files.len());
            for file in files {
                let path = file.path.into_bytes();
                changes.push(ScratchChange {
                    // The head's own entry as the base: only this put changes
                    // the path, so the merge applies it.
                    base: head.get(&path).cloned(),
                    new: Some(ScratchBlob {
                        oid: store::write_blob(&repo, &file.bytes)?,
                        mode: FILE_MODE,
                        size: file.bytes.len() as u64,
                    }),
                    path: ScratchPath(path),
                });
            }
            let plan = merge::plan(&head, &changes, &writer);
            if let Some(refusal) =
                merge::refusal(&plan, scratch.head_bytes, ceiling, &scratch.owner_uri)
            {
                return Err(refusal_text(&refusal));
            }
            let commit = match (&scratch.head_commit, plan.changed) {
                (Some(head), false) => head.clone(),
                _ => store::commit(&repo, &plan.result)?,
            };
            Ok((commit, plan.merged_bytes, plan.added_bytes(&head)))
        })
    };
    let (head, head_bytes, added) = folding
        .await
        .map_err(|error| format!("the scratch put failed: {error}"))??;
    if scratch.head_commit.as_ref() != Some(&head) {
        let advanced = owner::advance_head(
            &orch.db.local,
            &scratch.id,
            scratch.head_commit.as_deref(),
            &head,
            head_bytes,
            added,
            writer,
        )
        .await
        .map_err(|error| {
            format!(
                "advancing the scratch of {} failed: {error}",
                scratch.owner_uri
            )
        })?;
        if !advanced {
            // Only another runner process moves the head while this one
            // holds the lock.
            return Err(format!(
                "the scratch of {} moved during the put",
                scratch.owner_uri
            ));
        }
    }
    let config_dir = orch.config_dir.clone();
    let maintained = tokio::task::spawn_blocking(move || {
        let repo = store::open(&config_dir, &scratch.project_id, &scratch.id)?;
        store::maintain(&repo, Some(&head), head_bytes)
    })
    .await;
    if let Ok(Err(error)) | Err(error) = maintained.map_err(|error| error.to_string()) {
        log::warn!("maintain node scratch store {scratch_id}: {error}");
    }
    Ok(())
}

fn refusal_text(refusal: &ScratchRefusal) -> String {
    if !refusal.oversized.is_empty() {
        return format!(
            "{} is larger than one transfer can carry, which no ceiling admits",
            report::named(&refusal.oversized)
        );
    }
    let mut text = format!(
        "the scratch of {} would hold {}, past its ceiling of {}",
        refusal.owner_uri,
        report::bytes(refusal.merged_bytes),
        report::ceiling(refusal.ceiling_bytes)
    );
    if !refusal.largest.is_empty() {
        text.push_str(&format!("; largest: {}", report::named(&refusal.largest)));
    }
    text
}
