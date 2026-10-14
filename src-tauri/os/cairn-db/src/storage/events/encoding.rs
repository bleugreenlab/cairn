//! The single encode/decode contract over the archival `events` columns.
//!
//! The teardown writer (`crate::archival::rewrite`) and the read-path reader
//! ([`super::reconstruct`]) speak exactly one private contract over six event
//! columns. Holding it in one enum keeps the two sides from drifting: the writer
//! encodes a shape to columns and the reader decodes columns back to a shape, so
//! adding a fourth shape forces both halves to update together and a column
//! combination no shape produces is rejected at the boundary instead of
//! mis-reconstructed.
//!
//! | shape          | storage_mode | content_commit | data                        | data_blob              | codec   |
//! |----------------|--------------|----------------|-----------------------------|------------------------|---------|
//! | full           | full / NULL  | NULL           | original                    | NULL                   | none    |
//! | zstd           | zstd         | NULL           | label stub                  | zstd(original)         | zstd_v1 |
//! | gitcoord read  | gitcoord     | commit         | stub (pins tool_input.paths)| NULL                   | none    |
//! | gitcoord write | gitcoord     | batch commit   | label stub                  | zstd(stripped remainder)| zstd_v1 |
//! | hybrid read    | gitcoord     | batch commit   | skeleton stub + sections    | zstd(placeholder skel.) | zstd_v1 |
//! | blobbed        | gitcoord     | NULL           | blob refs + inline remainder| NULL                   | none    |
//!
//! A `gitcoord` row is content-addressed, in two coordinate systems: a git commit
//! (`content_commit` present) for transcript reads/writes, or content-hashed blob
//! references (`content_commit` NULL) for a `blobbed` row, whose constant parts
//! live once in `archival_blobs`. So `content_commit` first splits the `gitcoord`
//! mode into git-addressed vs hash-addressed. Within git-addressed, a `data_blob`
//! absent is a pure-file read; a `data_blob` present is either a committed write
//! or a mixed-target hybrid read, split by `content_render_sha`: a hybrid read
//! carries the drift sha over its full original bytes, a write never does.
//! [`decode`] is the only place that derives these, so neither the writer nor the
//! reader re-implements the rules.
//!
//! The `blobbed` shape is one storage mechanism shared by two consumers: an
//! assembled `system:prompt` and a near-constant `system:init`. Both inline their
//! per-run remainder and reference their constant parts by content hash; the
//! `archived` marker inside `data` (`"system_prompt"` vs `"system_init"`) selects
//! which reassembly contract [`super::reconstruct`] applies. The column encoding
//! is identical for both, so no new `storage_mode` (and thus no `events` table
//! rebuild against its CHECK constraint) is introduced to add the second consumer.

//! The `hybrid read` shape extends gitcoord reads to mixed batches: a composed
//! `read` whose reproducible `file:` sections are git-addressed by `content_commit`
//! (their indices listed in `data.sections`) while the non-file sections, footers,
//! and separators live verbatim in the zstd `data_blob` as a NUL-placeholder
//! skeleton. Reconstruction renders each indexed file section standalone and
//! splices it back into its placeholder; `content_render_sha` is the drift
//! tripwire over the full original bytes. It reuses the gitcoord columns whole, so
//! like the second `blobbed` consumer it needs no new `storage_mode`.

use super::{CODEC_NONE, CODEC_ZSTD_V1};

/// `data.archived` markers that select a `blobbed` row's reassembly contract on
/// the read path. Both consumers store their constant parts in `archival_blobs`
/// and inline their per-run remainder; only the reassembly differs.
pub const ARCHIVED_SYSTEM_PROMPT: &str = "system_prompt";
pub const ARCHIVED_SYSTEM_INIT: &str = "system_init";

/// The skeleton placeholder tag for a `system:init` row's deduped tool-set array.
/// Scalar varying fields tag themselves by their JSON path; the tool set is the
/// one structured field, so its tag is named explicitly and shared by the writer
/// and reader.
pub const INIT_TOOLS_TAG: &str = "raw.tools";

/// Wrap a tag in the NUL-delimited placeholder a `system:init` skeleton carries
/// in place of a varying value. A raw NUL byte can never appear in serialized
/// JSON, so a placeholder can never collide with skeleton content.
pub fn init_placeholder(tag: &str) -> String {
    format!("\0{tag}\0")
}

/// `storage_mode` column markers.
const MODE_FULL: &str = "full";
const MODE_ZSTD: &str = "zstd";
const MODE_GITCOORD: &str = "gitcoord";

/// One archived event's logical form: the shapes the storage contract admits.
/// Each carries exactly the fields its columns encode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchivedShape {
    /// Unarchived: `data` holds the original rendered bytes verbatim.
    Full { data: String },
    /// Compressed backstop: `data` is a label stub, the original lives zstd in
    /// `blob`.
    Zstd { data: String, blob: Vec<u8> },
    /// A read addressed by a git coordinate: `data` is a stub pinning
    /// `tool_input.paths`, the rendered result is regenerated from `commit`.
    GitcoordRead {
        commit: String,
        render_sha: String,
        data: String,
    },
    /// A committed write's assistant event: `data` is a label stub, the
    /// payload-stripped remainder lives zstd in `blob`, and the committed diff is
    /// regenerated from `commit`.
    GitcoordWrite {
        commit: String,
        data: String,
        blob: Vec<u8>,
    },
    /// A mixed-target read: the reproducible `file:` sections are git-addressed by
    /// `commit` (their indices listed in `data.sections`), and the rest of the
    /// composed result — non-file sections, separators, footers, affordances —
    /// lives verbatim in the zstd `blob` as a NUL-placeholder skeleton.
    /// `render_sha` is the drift tripwire over the full original bytes, and the
    /// discriminator from [`ArchivedShape::GitcoordWrite`] (which never sets it).
    HybridRead {
        commit: String,
        render_sha: String,
        data: String,
        blob: Vec<u8>,
    },
    /// A hash-addressed row whose constant parts live once in `archival_blobs`
    /// and whose per-run remainder is inlined in `data`. Two consumers share this
    /// shape, discriminated by the `archived` marker inside `data`:
    /// - `"system_prompt"`: an ordered segment map of content-hash references plus
    ///   an inlined dynamic tail (orientation).
    /// - `"system_init"`: a content-hashed constant skeleton plus the inlined
    ///   varying fields (session ids, cwd) and a deduped tool-set reference.
    ///
    /// `render_sha` is the sha-256 of the original reconstructed bytes, a drift
    /// tripwire never used for reconstruction.
    Blobbed { render_sha: String, data: String },
}

/// The six `events` columns the archival contract owns. Built from an [`Event`]
/// on the read path and bound straight into the writer's UPDATE.
///
/// [`Event`]: crate::models::Event
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventColumns {
    pub storage_mode: Option<String>,
    pub content_commit: Option<String>,
    pub content_render_sha: Option<String>,
    pub data: String,
    pub data_blob: Option<Vec<u8>>,
    pub codec: Option<String>,
}

impl ArchivedShape {
    /// Project a shape onto its column values. Total and infallible: every shape
    /// maps to exactly one valid column combination.
    pub fn encode(&self) -> EventColumns {
        match self {
            ArchivedShape::Full { data } => EventColumns {
                storage_mode: Some(MODE_FULL.to_string()),
                content_commit: None,
                content_render_sha: None,
                data: data.clone(),
                data_blob: None,
                codec: None,
            },
            ArchivedShape::Zstd { data, blob } => EventColumns {
                storage_mode: Some(MODE_ZSTD.to_string()),
                content_commit: None,
                content_render_sha: None,
                data: data.clone(),
                data_blob: Some(blob.clone()),
                codec: Some(CODEC_ZSTD_V1.to_string()),
            },
            ArchivedShape::GitcoordRead {
                commit,
                render_sha,
                data,
            } => EventColumns {
                storage_mode: Some(MODE_GITCOORD.to_string()),
                content_commit: Some(commit.clone()),
                content_render_sha: Some(render_sha.clone()),
                data: data.clone(),
                data_blob: None,
                codec: Some(CODEC_NONE.to_string()),
            },
            ArchivedShape::GitcoordWrite { commit, data, blob } => EventColumns {
                storage_mode: Some(MODE_GITCOORD.to_string()),
                content_commit: Some(commit.clone()),
                content_render_sha: None,
                data: data.clone(),
                data_blob: Some(blob.clone()),
                codec: Some(CODEC_ZSTD_V1.to_string()),
            },
            ArchivedShape::HybridRead {
                commit,
                render_sha,
                data,
                blob,
            } => EventColumns {
                storage_mode: Some(MODE_GITCOORD.to_string()),
                content_commit: Some(commit.clone()),
                content_render_sha: Some(render_sha.clone()),
                data: data.clone(),
                data_blob: Some(blob.clone()),
                codec: Some(CODEC_ZSTD_V1.to_string()),
            },
            ArchivedShape::Blobbed { render_sha, data } => EventColumns {
                storage_mode: Some(MODE_GITCOORD.to_string()),
                content_commit: None,
                content_render_sha: Some(render_sha.clone()),
                data: data.clone(),
                data_blob: None,
                codec: Some(CODEC_NONE.to_string()),
            },
        }
    }
}

/// A column combination that matches no archived shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError(String);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Classify a row's columns into its [`ArchivedShape`], rejecting combinations no
/// shape produces (a `gitcoord` row with no commit, a compressed row whose codec
/// disagrees with its blob, a `full` row carrying a blob, an unknown mode).
pub(crate) fn decode(cols: &EventColumns) -> Result<ArchivedShape, DecodeError> {
    // Global blob/codec invariant: a blob is always zstd_v1, its absence is always
    // uncompressed. This holds across every mode, so check it once up front.
    match (cols.data_blob.is_some(), cols.codec.as_deref()) {
        (true, Some(CODEC_ZSTD_V1)) => {}
        (true, other) => {
            return Err(DecodeError(format!(
                "compressed row has codec {other:?}, expected {CODEC_ZSTD_V1}"
            )))
        }
        (false, None | Some(CODEC_NONE)) => {}
        (false, Some(other)) => {
            return Err(DecodeError(format!("uncompressed row has codec {other:?}")))
        }
    }

    let mode = cols.storage_mode.as_deref().unwrap_or(MODE_FULL);
    match mode {
        MODE_FULL => {
            if cols.content_commit.is_some() {
                return Err(DecodeError("full row carries a content_commit".to_string()));
            }
            if cols.data_blob.is_some() {
                return Err(DecodeError("full row carries a data_blob".to_string()));
            }
            Ok(ArchivedShape::Full {
                data: cols.data.clone(),
            })
        }
        MODE_ZSTD => {
            if cols.content_commit.is_some() {
                return Err(DecodeError("zstd row carries a content_commit".to_string()));
            }
            let blob = cols
                .data_blob
                .clone()
                .ok_or_else(|| DecodeError("zstd row has no data_blob".to_string()))?;
            Ok(ArchivedShape::Zstd {
                data: cols.data.clone(),
                blob,
            })
        }
        MODE_GITCOORD => {
            // No commit means hash-addressed: a `blobbed` row whose constant parts
            // live in `archival_blobs` and whose per-run remainder is inlined in
            // `data` (the `archived` marker inside `data` selects the system-prompt
            // vs system-init reassembly). With a commit it is git-addressed: a
            // `data_blob` absent is a pure-file read; a `data_blob` present is a
            // committed write (no render_sha) or a mixed-target hybrid read
            // (render_sha present, the drift tripwire over the full original bytes).
            let Some(commit) = cols.content_commit.clone() else {
                return Ok(ArchivedShape::Blobbed {
                    render_sha: cols.content_render_sha.clone().unwrap_or_default(),
                    data: cols.data.clone(),
                });
            };
            match cols.data_blob.clone() {
                Some(blob) => match cols.content_render_sha.clone() {
                    Some(render_sha) => Ok(ArchivedShape::HybridRead {
                        commit,
                        render_sha,
                        data: cols.data.clone(),
                        blob,
                    }),
                    None => Ok(ArchivedShape::GitcoordWrite {
                        commit,
                        data: cols.data.clone(),
                        blob,
                    }),
                },
                None => Ok(ArchivedShape::GitcoordRead {
                    commit,
                    render_sha: cols.content_render_sha.clone().unwrap_or_default(),
                    data: cols.data.clone(),
                }),
            }
        }
        other => Err(DecodeError(format!("unknown storage_mode: {other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::compress;
    use crate::storage::event_fixture as fx;

    /// Every shape, built on real-anatomy fixture data, survives encode→decode
    /// unchanged — the property that keeps the writer and reader from drifting.
    #[test]
    fn every_shape_round_trips() {
        let shapes = vec![
            ArchivedShape::Full {
                data: fx::assistant_text("thinking out loud"),
            },
            ArchivedShape::Zstd {
                data: "{\"archived\":\"zstd\"}".to_string(),
                blob: compress(fx::user_text("zephyr needle").as_bytes()).unwrap(),
            },
            ArchivedShape::GitcoordRead {
                commit: "a".repeat(40),
                render_sha: "b".repeat(64),
                data: fx::read_stub("r1", &["file:a.txt"]),
            },
            ArchivedShape::GitcoordWrite {
                commit: "c".repeat(40),
                data: "{\"archived\":\"gitcoord_write\"}".to_string(),
                blob: compress(fx::assistant_write_remainder("w1").as_bytes()).unwrap(),
            },
            ArchivedShape::HybridRead {
                commit: "f".repeat(40),
                render_sha: "a".repeat(64),
                data: fx::hybrid_read_stub("h1", &["file:a.txt", "cairn://p/P/1"], &[0]),
                blob: compress(b"skeleton with a placeholder").unwrap(),
            },
            ArchivedShape::Blobbed {
                render_sha: "d".repeat(64),
                data: "{\"archived\":\"system_prompt\",\"segments\":[]}".to_string(),
            },
        ];
        for shape in shapes {
            let decoded = decode(&shape.encode()).expect("shape decodes");
            assert_eq!(decoded, shape, "shape must survive encode/decode");
        }
    }

    /// A pristine (never-archived) row decodes as `Full`: storage_mode and codec
    /// both NULL, no blob.
    #[test]
    fn null_columns_decode_as_full() {
        let cols = EventColumns {
            data: fx::user_text("hi").to_string(),
            ..Default::default()
        };
        assert_eq!(
            decode(&cols).unwrap(),
            ArchivedShape::Full {
                data: fx::user_text("hi")
            }
        );
    }

    /// A `gitcoord` row with no commit is no longer rejected: it is the
    /// hash-addressed `blobbed` shape (constant parts in `archival_blobs`, per-run
    /// remainder inline in `data`).
    #[test]
    fn gitcoord_without_commit_is_blobbed() {
        let decoded = decode(&EventColumns {
            storage_mode: Some(MODE_GITCOORD.to_string()),
            content_render_sha: Some("e".repeat(64)),
            data: "{\"segments\":[]}".to_string(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            decoded,
            ArchivedShape::Blobbed {
                render_sha: "e".repeat(64),
                data: "{\"segments\":[]}".to_string(),
            }
        );
    }

    /// A git-addressed row carrying both a commit and a blob is discriminated by
    /// `content_render_sha`: present → hybrid read, absent → write. This is the
    /// backward-compatibility guard — existing write rows set no render_sha and
    /// must keep decoding as `GitcoordWrite`.
    #[test]
    fn commit_and_blob_discriminate_hybrid_from_write_by_render_sha() {
        let blob = compress(b"skeleton").unwrap();
        let hybrid = decode(&EventColumns {
            storage_mode: Some(MODE_GITCOORD.to_string()),
            content_commit: Some("a".repeat(40)),
            content_render_sha: Some("b".repeat(64)),
            data: "{\"archived\":\"hybrid_read\"}".to_string(),
            data_blob: Some(blob.clone()),
            codec: Some(CODEC_ZSTD_V1.to_string()),
        })
        .unwrap();
        assert!(matches!(hybrid, ArchivedShape::HybridRead { .. }));

        let write = decode(&EventColumns {
            storage_mode: Some(MODE_GITCOORD.to_string()),
            content_commit: Some("a".repeat(40)),
            content_render_sha: None,
            data: "{}".to_string(),
            data_blob: Some(blob),
            codec: Some(CODEC_ZSTD_V1.to_string()),
        })
        .unwrap();
        assert!(matches!(write, ArchivedShape::GitcoordWrite { .. }));
    }

    #[test]
    fn invalid_combinations_are_rejected() {
        // zstd with no blob.
        assert!(decode(&EventColumns {
            storage_mode: Some(MODE_ZSTD.to_string()),
            ..Default::default()
        })
        .is_err());
        // blob present but codec says uncompressed.
        assert!(decode(&EventColumns {
            storage_mode: Some(MODE_ZSTD.to_string()),
            data_blob: Some(vec![1, 2, 3]),
            codec: Some(CODEC_NONE.to_string()),
            ..Default::default()
        })
        .is_err());
        // full row carrying a blob.
        assert!(decode(&EventColumns {
            storage_mode: Some(MODE_FULL.to_string()),
            data_blob: Some(compress(b"x").unwrap()),
            codec: Some(CODEC_ZSTD_V1.to_string()),
            ..Default::default()
        })
        .is_err());
        // unknown storage_mode.
        assert!(decode(&EventColumns {
            storage_mode: Some("sometime_future_shape".to_string()),
            ..Default::default()
        })
        .is_err());
    }
}
