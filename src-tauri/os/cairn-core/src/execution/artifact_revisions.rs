//! Delivering a revised artifact to the jobs that were launched with an earlier
//! version of it.
//!
//! A job's launch prompt resolves each upstream artifact once, at launch
//! (`prepare_job`). A later write to the same artifact — most often a thread
//! patching a confirmed plan a minute after the builder started — would
//! otherwise never reach that job: a patch is a pure edit that re-gates and
//! restarts nothing. This module closes that gap in three parts:
//!
//! - At launch, [`record_launch_inputs`] stores which artifact version each
//!   input resolved to (`job_artifact_inputs`), from the structured resolution
//!   rather than the prompt text.
//! - On each artifact write, [`schedule_after_write`] notes the write in a
//!   per-artifact quiet-period debouncer held on the orchestrator, so a burst of
//!   patches settles before anything is sent.
//! - When the burst settles, [`deliver_artifact_revision`] sends every
//!   non-terminal consumer one queued system message carrying the net diff from
//!   the version it holds to the latest, then advances its record. A consumer
//!   whose held content already equals the latest receives nothing.
//!
//! Delivery rides the ordinary system-direct queue at `Steer` urgency, so a busy
//! consumer takes it at its next tool boundary, an idle one is woken, and a
//! self-suspended one receives it when its own wait resolves.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use cairn_db::turso::params;
use cairn_workflow::advancement::ResolvedInput;

use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};

/// How long an artifact must go unwritten before its revision is delivered. A
/// thread revising a plan in place lands several patches seconds apart (one
/// specimen: five versions in twelve seconds); waiting for the burst to end is
/// what makes the consumer receive one message with the net change.
pub(crate) const DEFAULT_QUIET_PERIOD: Duration = Duration::from_secs(15);

/// The longest a revision waits behind a burst that never goes quiet, so a
/// steadily patched artifact still reaches its consumers.
const MAX_COALESCE: Duration = Duration::from_secs(120);

// ---------------------------------------------------------------------------
// Launch record
// ---------------------------------------------------------------------------

/// Record the artifact version each of `job_id`'s inputs resolved to. Called
/// with the same resolution that composed the launch prompt, so the record is
/// exactly what the job holds. A relaunch re-resolves to the latest version and
/// overwrites the record with it.
pub(crate) async fn record_launch_inputs(
    db: &LocalDb,
    job_id: &str,
    inputs: &[ResolvedInput],
) -> Result<(), String> {
    let rows: Vec<(String, String, i64)> = inputs
        .iter()
        .filter_map(|input| input.source.as_ref())
        .map(|source| {
            (
                source.producer_job_id.clone(),
                source.output_name.clone(),
                source.version,
            )
        })
        .collect();
    if rows.is_empty() {
        return Ok(());
    }
    let job_id = job_id.to_string();
    let label = job_id.clone();
    let now = chrono::Utc::now().timestamp();
    db.write(move |conn| {
        let job_id = job_id.clone();
        let rows = rows.clone();
        Box::pin(async move {
            for (producer_job_id, output_name, version) in rows {
                conn.execute(
                    "INSERT INTO job_artifact_inputs
                       (job_id, producer_job_id, output_name, version, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT (job_id, producer_job_id, output_name)
                     DO UPDATE SET version = excluded.version,
                                   updated_at = excluded.updated_at",
                    params![
                        job_id.as_str(),
                        producer_job_id.as_str(),
                        output_name.as_str(),
                        version,
                        now
                    ],
                )
                .await?;
            }
            Ok::<_, DbError>(())
        })
    })
    .await
    .map_err(|e| format!("Failed to record launch inputs for job {label}: {e}"))
}

// ---------------------------------------------------------------------------
// Quiet-period coalescing
// ---------------------------------------------------------------------------

/// One artifact's version chain: the producer job and the artifact name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ArtifactKey {
    pub producer_job_id: String,
    pub output_name: String,
}

#[derive(Debug)]
struct PendingRevision {
    first_write: Instant,
    last_write: Instant,
    /// Who wrote during this burst, in first-write order, without repeats.
    writers: Vec<String>,
}

/// Per-artifact debouncer for revision delivery, held on the orchestrator. It
/// owns only the timing; what a consumer receives is always computed fresh from
/// the stored versions when the burst settles.
#[derive(Debug)]
pub struct RevisionDeliveries {
    quiet: Duration,
    pending: Mutex<HashMap<ArtifactKey, PendingRevision>>,
}

impl RevisionDeliveries {
    pub(crate) fn new(quiet: Duration) -> Self {
        Self {
            quiet,
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Note a write. Returns `true` when this write opens a new burst, in which
    /// case the caller starts the one waiter that will deliver it; a write into
    /// an open burst only extends it.
    fn note_write(&self, key: &ArtifactKey, writer: &str, now: Instant) -> bool {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        match pending.get_mut(key) {
            Some(burst) => {
                burst.last_write = now;
                if !burst.writers.iter().any(|w| w == writer) {
                    burst.writers.push(writer.to_string());
                }
                false
            }
            None => {
                pending.insert(
                    key.clone(),
                    PendingRevision {
                        first_write: now,
                        last_write: now,
                        writers: vec![writer.to_string()],
                    },
                );
                true
            }
        }
    }

    /// Close the burst if it has gone quiet (or hit the coalescing cap),
    /// returning its writers; otherwise how much longer to wait.
    fn take_if_settled(&self, key: &ArtifactKey, now: Instant) -> Result<Vec<String>, Duration> {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        let Some(burst) = pending.get(key) else {
            return Ok(Vec::new());
        };
        let due = (burst.last_write + self.quiet).min(burst.first_write + MAX_COALESCE);
        if now >= due {
            Ok(pending.remove(key).map(|b| b.writers).unwrap_or_default())
        } else {
            Err(due - now)
        }
    }
}

impl Default for RevisionDeliveries {
    fn default() -> Self {
        Self::new(DEFAULT_QUIET_PERIOD)
    }
}

/// After an artifact write lands, schedule delivery of the revision to any job
/// that was launched with an earlier version of it. A no-op when no job has
/// consumed this artifact, which is nearly every write.
pub(crate) async fn schedule_after_write(
    orch: &Orchestrator,
    db: &LocalDb,
    producer_job_id: &str,
    output_name: &str,
    writer_run_id: Option<&str>,
) {
    match has_consumers(db, producer_job_id, output_name).await {
        Ok(true) => {}
        Ok(false) => return,
        Err(error) => {
            log::warn!(
                "artifact revision: could not look up consumers of {producer_job_id}/{output_name}: {error}"
            );
            return;
        }
    }
    let writer = writer_label(db, writer_run_id).await;
    let key = ArtifactKey {
        producer_job_id: producer_job_id.to_string(),
        output_name: output_name.to_string(),
    };
    if !orch
        .artifact_revisions
        .note_write(&key, &writer, Instant::now())
    {
        return;
    }
    let orch = orch.clone();
    tokio::spawn(async move {
        let writers = loop {
            match orch
                .artifact_revisions
                .take_if_settled(&key, Instant::now())
            {
                Ok(writers) => break writers,
                Err(wait) => tokio::time::sleep(wait).await,
            }
        };
        if let Err(error) =
            deliver_artifact_revision(&orch, &key.producer_job_id, &key.output_name, &writers).await
        {
            log::warn!(
                "artifact revision: delivery for {}/{} failed: {error}",
                key.producer_job_id,
                key.output_name
            );
        }
    });
}

async fn has_consumers(
    db: &LocalDb,
    producer_job_id: &str,
    output_name: &str,
) -> Result<bool, String> {
    let producer_job_id = producer_job_id.to_string();
    let output_name = output_name.to_string();
    db.read(move |conn| {
        let producer_job_id = producer_job_id.clone();
        let output_name = output_name.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT 1 FROM job_artifact_inputs
                     WHERE producer_job_id = ?1 AND output_name = ?2 LIMIT 1",
                    params![producer_job_id.as_str(), output_name.as_str()],
                )
                .await?;
            Ok(rows.next().await?.is_some())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Who made a write, as the revision message names them: the writing node's
/// URI, or the operator when the write came from no agent run.
async fn writer_label(db: &LocalDb, run_id: Option<&str>) -> String {
    let Some(run_id) = run_id else {
        return "the operator".to_string();
    };
    let job_id = crate::messages::side_channel::job_id_for_run(db, run_id).await;
    let uri = match job_id {
        Some(job_id) => crate::jobs::queries::home_uri_for_job(db, &job_id)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    uri.unwrap_or_else(|| format!("run {run_id}"))
}

// ---------------------------------------------------------------------------
// Delivery
// ---------------------------------------------------------------------------

/// Deliver every revision a restart left behind. The coalescing timer lives in
/// memory, so a host that stops inside a quiet period loses the scheduled
/// delivery while the stored record still shows the consumer on an older
/// version. This pass finds every artifact with an unfinished consumer behind
/// its latest version, across every open database, and delivers it now.
/// Delivery is keyed by consumer and target version, so this cannot double-send
/// against a timer that also fires. Who wrote the revision was held only by the
/// lost timer, so a recovered message does not name a writer.
pub async fn deliver_pending_revisions(orch: &Orchestrator) -> usize {
    let mut artifacts: Vec<ArtifactKey> = Vec::new();
    for db in orch.db.all_dbs().await {
        let found = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT DISTINCT i.producer_job_id, i.output_name
                             FROM job_artifact_inputs i
                             JOIN jobs j ON j.id = i.job_id
                             WHERE j.status NOT IN ('complete', 'failed', 'cancelled')
                               AND i.version < (
                                   SELECT MAX(a.version) FROM artifacts a
                                   WHERE a.job_id = i.producer_job_id
                                     AND a.output_name = i.output_name)",
                            (),
                        )
                        .await?;
                    let mut found = Vec::new();
                    while let Some(row) = rows.next().await? {
                        found.push(ArtifactKey {
                            producer_job_id: row.text(0)?,
                            output_name: row.text(1)?,
                        });
                    }
                    Ok::<_, DbError>(found)
                })
            })
            .await;
        match found {
            Ok(found) => artifacts.extend(found),
            Err(error) => log::warn!("artifact revision: startup scan failed: {error}"),
        }
    }
    let mut delivered = 0;
    for key in artifacts {
        match deliver_artifact_revision(orch, &key.producer_job_id, &key.output_name, &[]).await {
            Ok(count) => delivered += count,
            Err(error) => log::warn!(
                "artifact revision: startup delivery for {}/{} failed: {error}",
                key.producer_job_id,
                key.output_name
            ),
        }
    }
    if delivered > 0 {
        log::info!("artifact revision: delivered {delivered} revision(s) a restart left pending");
    }
    delivered
}

struct Consumer {
    job_id: String,
    held_version: i64,
}

struct StoredVersion {
    version: i64,
    data: serde_json::Value,
    confirmed: bool,
}

/// Deliver the latest version of `producer_job_id`'s `output_name` artifact to
/// every non-terminal job holding an earlier one, as one message per job with
/// the net change. Returns how many messages were queued.
///
/// Safe to call repeatedly: a consumer already at the latest version is
/// skipped, the message is keyed by consumer and target version so a repeat
/// cannot queue it twice, and the record only advances from the version the
/// delivery was computed against.
pub async fn deliver_artifact_revision(
    orch: &Orchestrator,
    producer_job_id: &str,
    output_name: &str,
    writers: &[String],
) -> Result<usize, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, producer_job_id)
        .await
        .map_err(|e| e.to_string())?;
    let Some(latest) = load_version(&db, producer_job_id, output_name, None).await? else {
        return Ok(0);
    };
    let consumers = load_live_consumers(&db, producer_job_id, output_name).await?;
    if consumers.is_empty() {
        return Ok(0);
    }
    let uri = crate::jobs::queries::home_uri_for_job(&db, producer_job_id)
        .await
        .map_err(|e| e.to_string())?
        .map(|home| format!("{home}/{output_name}"))
        .unwrap_or_else(|| format!("{producer_job_id}/{output_name}"));
    let new_text = artifact_text(&latest.data);

    let mut delivered = 0;
    for consumer in consumers {
        if consumer.held_version >= latest.version {
            continue;
        }
        let held = load_version(
            &db,
            producer_job_id,
            output_name,
            Some(consumer.held_version),
        )
        .await?;
        let old_text = held
            .as_ref()
            .map(|v| artifact_text(&v.data))
            .unwrap_or_default();
        if held.is_some() && old_text == new_text {
            // Nothing the consumer holds has changed; it already has this
            // version's content, so record that and send nothing.
            advance_record(&db, &consumer, producer_job_id, output_name, latest.version).await?;
            continue;
        }
        let message = render_revision_message(&Revision {
            uri: &uri,
            output_name,
            from_version: consumer.held_version,
            to_version: latest.version,
            writers,
            confirmed: latest.confirmed,
            old_text: &old_text,
            new_text: &new_text,
        });
        let Some(run_id) = latest_run(&db, &consumer.job_id).await else {
            log::debug!(
                "artifact revision: consumer {} has no run to deliver to",
                consumer.job_id
            );
            continue;
        };
        let key = format!(
            "artifact-revision:{}:{producer_job_id}:{output_name}:v{}",
            consumer.job_id, latest.version
        );
        let queue_orch = orch.clone();
        tokio::task::spawn_blocking(move || {
            crate::messages::delivery::queue_system_direct_once(
                &queue_orch,
                &run_id,
                &message,
                DeliveryUrgency::Steer,
                &key,
            )
        })
        .await
        .map_err(|e| format!("revision delivery task failed: {e}"))??;
        advance_record(&db, &consumer, producer_job_id, output_name, latest.version).await?;
        delivered += 1;
        log::info!(
            "artifact revision: delivered {uri} v{} -> v{} to job {}",
            consumer.held_version,
            latest.version,
            consumer.job_id
        );
    }
    Ok(delivered)
}

async fn load_live_consumers(
    db: &LocalDb,
    producer_job_id: &str,
    output_name: &str,
) -> Result<Vec<Consumer>, String> {
    let producer_job_id = producer_job_id.to_string();
    let output_name = output_name.to_string();
    db.read(move |conn| {
        let producer_job_id = producer_job_id.clone();
        let output_name = output_name.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT i.job_id, i.version
                     FROM job_artifact_inputs i
                     JOIN jobs j ON j.id = i.job_id
                     WHERE i.producer_job_id = ?1 AND i.output_name = ?2
                       AND j.status NOT IN ('complete', 'failed', 'cancelled')
                     ORDER BY i.job_id",
                    params![producer_job_id.as_str(), output_name.as_str()],
                )
                .await?;
            let mut consumers = Vec::new();
            while let Some(row) = rows.next().await? {
                consumers.push(Consumer {
                    job_id: row.text(0)?,
                    held_version: row.i64(1)?,
                });
            }
            Ok(consumers)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// One stored version of the artifact: `version` when given, else the latest.
async fn load_version(
    db: &LocalDb,
    producer_job_id: &str,
    output_name: &str,
    version: Option<i64>,
) -> Result<Option<StoredVersion>, String> {
    let producer_job_id = producer_job_id.to_string();
    let output_name = output_name.to_string();
    db.read(move |conn| {
        let producer_job_id = producer_job_id.clone();
        let output_name = output_name.clone();
        Box::pin(async move {
            let mut rows = match version {
                Some(version) => {
                    conn.query(
                        "SELECT version, data, confirmed FROM artifacts
                         WHERE job_id = ?1 AND output_name = ?2 AND version = ?3
                         LIMIT 1",
                        params![producer_job_id.as_str(), output_name.as_str(), version],
                    )
                    .await?
                }
                None => {
                    conn.query(
                        "SELECT version, data, confirmed FROM artifacts
                         WHERE job_id = ?1 AND output_name = ?2
                         ORDER BY version DESC LIMIT 1",
                        params![producer_job_id.as_str(), output_name.as_str()],
                    )
                    .await?
                }
            };
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            Ok(Some(StoredVersion {
                version: row.i64(0)?,
                data: serde_json::from_str(&row.text(1)?).unwrap_or_default(),
                confirmed: row.i64(2)? != 0,
            }))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn advance_record(
    db: &LocalDb,
    consumer: &Consumer,
    producer_job_id: &str,
    output_name: &str,
    to_version: i64,
) -> Result<(), String> {
    db.execute(
        "UPDATE job_artifact_inputs SET version = ?1, updated_at = ?2
         WHERE job_id = ?3 AND producer_job_id = ?4 AND output_name = ?5 AND version = ?6",
        params![
            to_version,
            chrono::Utc::now().timestamp(),
            consumer.job_id.as_str(),
            producer_job_id,
            output_name,
            consumer.held_version
        ],
    )
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

async fn latest_run(db: &LocalDb, job_id: &str) -> Option<String> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM runs WHERE job_id = ?1 ORDER BY created_at DESC LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            crate::storage::next_opt_text(&mut rows, 0).await
        })
    })
    .await
    .ok()
    .flatten()
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// An artifact version as text for diffing: its title, summary, and content
/// first (the fields a reader takes in that order), then any other fields in
/// key order, each under its own heading so a hunk says which field moved.
pub(crate) fn artifact_text(data: &serde_json::Value) -> String {
    let Some(object) = data.as_object() else {
        return format!(
            "{}\n",
            serde_json::to_string_pretty(data).unwrap_or_default()
        );
    };
    const LEADING: [&str; 3] = ["title", "summary", "content"];
    let mut keys: Vec<&String> = object
        .keys()
        .filter(|key| LEADING.contains(&key.as_str()))
        .collect();
    keys.sort_by_key(|key| LEADING.iter().position(|lead| lead == key));
    let mut rest: Vec<&String> = object
        .keys()
        .filter(|key| !LEADING.contains(&key.as_str()))
        .collect();
    rest.sort();
    keys.extend(rest);

    let mut text = String::new();
    for key in keys {
        let value = &object[key.as_str()];
        let body = match value.as_str() {
            Some(s) => s.to_string(),
            None => serde_json::to_string_pretty(value).unwrap_or_default(),
        };
        text.push_str(&format!("## {key}\n\n{}\n\n", body.trim_end()));
    }
    text
}

pub(crate) struct Revision<'a> {
    pub uri: &'a str,
    pub output_name: &'a str,
    pub from_version: i64,
    pub to_version: i64,
    pub writers: &'a [String],
    pub confirmed: bool,
    pub old_text: &'a str,
    pub new_text: &'a str,
}

/// The message a consumer receives: which artifact moved, from which version
/// to which, who moved it, that the artifact outranks its launch copy, and the
/// net change — as a diff, or as the full new text when that is shorter.
pub(crate) fn render_revision_message(revision: &Revision<'_>) -> String {
    let writers = if revision.writers.is_empty() {
        String::new()
    } else {
        format!(" Revised by {}.", revision.writers.join(", "))
    };
    let confirmation = if revision.confirmed {
        ""
    } else {
        " This version is awaiting confirmation."
    };
    let mut message = format!(
        "The `{name}` artifact you were launched with has been revised: `{uri}` is now version {to}, and you were working from version {from}.{writers}{confirmation} The artifact is authoritative over the copy you were given at launch, so bring your work in line with version {to}.",
        name = revision.output_name,
        uri = revision.uri,
        to = revision.to_version,
        from = revision.from_version,
    );
    let diff = cairn_vcs::jj::create_patch_body(revision.old_text, revision.new_text);
    if diff.len() < revision.new_text.len() {
        message.push_str(&format!(
            "\n\nChanges from version {} to version {}:\n\n{}",
            revision.from_version,
            revision.to_version,
            fenced("diff", &diff)
        ));
    } else {
        message.push_str(&format!(
            "\n\nVersion {} in full:\n\n{}",
            revision.to_version,
            fenced("markdown", revision.new_text.trim_end())
        ));
    }
    message
}

/// Fence `body` with more backticks than any run inside it, so a plan's own
/// code fences cannot close the block early.
fn fenced(lang: &str, body: &str) -> String {
    let mut longest = 0;
    let mut run = 0;
    for c in body.chars() {
        if c == '`' {
            run += 1;
            longest = longest.max(run);
        } else {
            run = 0;
        }
    }
    let fence = "`".repeat(longest.max(2) + 1);
    format!("{fence}{lang}\n{body}\n{fence}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ArtifactKey {
        ArtifactKey {
            producer_job_id: "j-planner".to_string(),
            output_name: "plan".to_string(),
        }
    }

    #[test]
    fn a_burst_of_writes_settles_once_after_the_last_goes_quiet() {
        let deliveries = RevisionDeliveries::new(Duration::from_secs(15));
        let t0 = Instant::now();
        // 5073's shape: five versions twelve seconds apart in total.
        assert!(deliveries.note_write(&key(), "thread", t0));
        for offset in [3, 6, 9, 12] {
            assert!(
                !deliveries.note_write(&key(), "thread", t0 + Duration::from_secs(offset)),
                "a write inside an open burst starts no second waiter"
            );
        }
        // Fifteen seconds after the FIRST write is not quiet: the last was at 12.
        assert_eq!(
            deliveries.take_if_settled(&key(), t0 + Duration::from_secs(15)),
            Err(Duration::from_secs(12))
        );
        assert_eq!(
            deliveries.take_if_settled(&key(), t0 + Duration::from_secs(27)),
            Ok(vec!["thread".to_string()])
        );
        // The burst is closed; the next write opens a new one.
        assert!(deliveries.note_write(&key(), "thread", t0 + Duration::from_secs(40)));
    }

    #[test]
    fn a_burst_that_never_goes_quiet_still_settles_at_the_cap() {
        let deliveries = RevisionDeliveries::new(Duration::from_secs(15));
        let t0 = Instant::now();
        deliveries.note_write(&key(), "a", t0);
        let mut t = t0;
        while t < t0 + MAX_COALESCE {
            t += Duration::from_secs(10);
            deliveries.note_write(&key(), "b", t);
        }
        assert_eq!(
            deliveries.take_if_settled(&key(), t0 + MAX_COALESCE),
            Ok(vec!["a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn artifact_text_leads_with_title_summary_content() {
        let text = artifact_text(&serde_json::json!({
            "content": "body",
            "extra": {"k": 1},
            "title": "T",
        }));
        let title = text.find("## title").unwrap();
        let content = text.find("## content").unwrap();
        let extra = text.find("## extra").unwrap();
        assert!(title < content && content < extra, "{text}");
    }

    #[test]
    fn the_message_carries_both_versions_the_writer_and_the_diff() {
        // A plan-sized document: one changed line among many, so the diff is
        // the smaller rendering.
        let body = |rule: &str| {
            let mut lines: Vec<String> =
                (1..=40).map(|n| format!("step {n} of the plan")).collect();
            lines[20] = rule.to_string();
            lines.join("\n")
        };
        let old = artifact_text(
            &serde_json::json!({"title": "T", "content": body("match bare phrases")}),
        );
        let new = artifact_text(
            &serde_json::json!({"title": "T", "content": body("match exact colon-prefixed forms")}),
        );
        let message = render_revision_message(&Revision {
            uri: "cairn://p/cairn/5073/1/planner/plan",
            output_name: "plan",
            from_version: 2,
            to_version: 7,
            writers: &["cairn://p/cairn/thread".to_string()],
            confirmed: true,
            old_text: &old,
            new_text: &new,
        });
        assert!(
            message.contains("`cairn://p/cairn/5073/1/planner/plan` is now version 7"),
            "{message}"
        );
        assert!(message.contains("working from version 2"), "{message}");
        assert!(
            message.contains("Revised by cairn://p/cairn/thread."),
            "{message}"
        );
        assert!(message.contains("-match bare phrases"), "{message}");
        assert!(
            message.contains("+match exact colon-prefixed forms"),
            "{message}"
        );
        assert!(!message.contains("awaiting confirmation"), "{message}");
    }

    #[test]
    fn a_rewrite_larger_as_a_diff_is_sent_in_full() {
        let message = render_revision_message(&Revision {
            uri: "u",
            output_name: "plan",
            from_version: 1,
            to_version: 2,
            writers: &[],
            confirmed: false,
            old_text: "## content\n\nalpha\n",
            new_text: "## content\n\nbeta\n",
        });
        assert!(message.contains("Version 2 in full:"), "{message}");
        assert!(message.contains("awaiting confirmation"), "{message}");
        assert!(!message.contains("Revised by"), "{message}");
    }

    #[test]
    fn a_fence_outlasts_any_fence_inside_the_body() {
        let body = "before\n````rust\ncode\n````\nafter";
        let fenced = fenced("diff", body);
        assert!(fenced.starts_with("`````diff\n"), "{fenced}");
        assert!(fenced.ends_with("\n`````"), "{fenced}");
    }
}
