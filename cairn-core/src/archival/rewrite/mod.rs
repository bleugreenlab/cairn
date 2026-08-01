//! Shared content-addressed event-shape encoding used by archival maintenance.
//!
//! This module contains no execution-teardown or workspace reader. The historical
//! maintenance pass classifies durable event rows and stores hash-addressed system
//! prompt/init segments or a byte-exact zstd fallback.

use std::collections::HashMap;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::models::Event;
use crate::storage::compress;
use crate::storage::events::encoding::{
    init_placeholder, ArchivedShape, ARCHIVED_SYSTEM_INIT, ARCHIVED_SYSTEM_PROMPT, INIT_TOOLS_TAG,
};
use crate::storage::events::reconstruct;

mod event_shape;
mod system_blob;

pub(crate) use event_shape::{build_tool_map, event_tool_use_id, normalize_tool_name, zstd_stub};
pub(crate) use system_blob::{build_system_init_shape, build_system_prompt_shape};

pub(crate) type SegmentBlob = (String, Vec<u8>);
pub(crate) type BlobbedShape = (ArchivedShape, Vec<SegmentBlob>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SystemBlobKind {
    Prompt,
    Init,
}

pub(crate) trait SystemBlobSink {
    fn push_blobbed(
        &mut self,
        event: &Event,
        kind: SystemBlobKind,
        shape: ArchivedShape,
        blobs: Vec<SegmentBlob>,
    );

    fn push_zstd(&mut self, event: &Event) -> Result<(), String>;
}

pub(crate) fn push_blobbed_or_zstd<S, F>(
    event: &Event,
    kind: SystemBlobKind,
    build: F,
    sink: &mut S,
) -> Result<(), String>
where
    S: SystemBlobSink,
    F: FnOnce(&Event) -> Result<Option<BlobbedShape>, String>,
{
    match build(event)? {
        Some((shape, blobs)) => sink.push_blobbed(event, kind, shape, blobs),
        None => sink.push_zstd(event)?,
    }
    Ok(())
}
