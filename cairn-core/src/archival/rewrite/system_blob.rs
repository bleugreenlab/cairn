use super::*;

#[cfg(test)]
pub(crate) fn classify_system_blob_columns_for_test(
    event: &Event,
    kind: SystemBlobKind,
) -> Result<
    (
        crate::storage::events::encoding::EventColumns,
        Vec<SegmentBlob>,
        ArchiveSummary,
    ),
    String,
> {
    let mut updates = Vec::new();
    let mut summary = ArchiveSummary::default();
    summary.bytes_before += event.data.len();
    let mut blobs = Vec::new();
    let mut sink = RewriteSystemBlobSink {
        updates: &mut updates,
        summary: &mut summary,
        blobs: &mut blobs,
    };
    match kind {
        SystemBlobKind::Prompt => {
            push_blobbed_or_zstd(event, kind, build_system_prompt_shape, &mut sink)?
        }
        SystemBlobKind::Init => {
            push_blobbed_or_zstd(event, kind, build_system_init_shape, &mut sink)?
        }
    }
    let update = updates
        .into_iter()
        .next()
        .ok_or_else(|| "system blob classifier produced no update".to_string())?;
    Ok((update.shape.encode(), blobs, summary))
}

/// Build the segmented shape and its segment blobs from an event's recorded
/// boundary map (`raw.segments`, an ordered list of `{kind, byteOffset,
/// byteLen}`). Returns `None` when the content or map is absent, a span is out of
/// range, or the reconstructed concatenation does not byte-match the full prompt
/// — every such case keeps the event whole (the caller falls to zstd).
pub(crate) fn build_system_prompt_shape(event: &Event) -> Result<Option<BlobbedShape>, String> {
    let Ok(value) = serde_json::from_str::<Value>(&event.data) else {
        return Ok(None);
    };
    let Some(content) = value.get("content").and_then(|v| v.as_str()) else {
        return Ok(None);
    };
    let Some(seg_map) = value
        .get("raw")
        .and_then(|raw| raw.get("segments"))
        .and_then(|segments| segments.as_array())
        .filter(|segments| !segments.is_empty())
    else {
        return Ok(None);
    };

    let mut archived_segments: Vec<Value> = Vec::with_capacity(seg_map.len());
    let mut blobs: Vec<SegmentBlob> = Vec::new();
    let mut rebuilt = String::with_capacity(content.len());

    for seg in seg_map {
        let kind = seg.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let offset = seg.get("byteOffset").and_then(|v| v.as_u64());
        let len = seg.get("byteLen").and_then(|v| v.as_u64());
        let (Some(offset), Some(len)) = (offset, len) else {
            return Ok(None);
        };
        let Some(slice) = (offset as usize)
            .checked_add(len as usize)
            .and_then(|end| content.get(offset as usize..end))
        else {
            return Ok(None);
        };
        rebuilt.push_str(slice);
        if kind == crate::orchestrator::session::SEGMENT_KIND_DYNAMIC {
            archived_segments.push(json!({ "kind": kind, "inline": slice }));
        } else {
            let hash = sha256_hex(slice.as_bytes());
            blobs.push((hash.clone(), compress(slice.as_bytes())?));
            archived_segments.push(json!({ "kind": kind, "hash": hash }));
        }
    }

    // Verify-then-discard: the recorded spans must tile the full prompt exactly,
    // or the boundary metadata is untrustworthy and the event stays whole.
    if rebuilt != content {
        return Ok(None);
    }

    let render_sha = sha256_hex(content.as_bytes());
    let mut map = value.as_object().cloned().unwrap_or_default();
    map.remove("content");
    // `raw.segments` (the {kind, byteOffset, byteLen} boundary map) and `raw.hash`
    // are consumed at archival time and redundant with the archived `segments`
    // list below; the byte offsets are meaningless once `content` is dropped, so
    // strip both from the stub.
    if let Some(Value::Object(raw)) = map.get_mut("raw") {
        raw.remove("segments");
        raw.remove("hash");
    }
    map.insert("archived".to_string(), json!(ARCHIVED_SYSTEM_PROMPT));
    map.insert("segments".to_string(), Value::Array(archived_segments));
    let data = Value::Object(map).to_string();

    Ok(Some((ArchivedShape::Blobbed { render_sha, data }, blobs)))
}

/// Locate the byte span of `value`'s serialized form in `data`, anchored at
/// `key_token` (e.g. `"\"sessionId\":"`). The bytes after the key are exactly
/// what serde wrote, so matching against [`serde_json::to_string`] of the parsed
/// value is escape-correct. Returns `Ok(None)` when the key is absent or the
/// following bytes do not match — an untrustworthy anchor the caller declines to
/// rewrite (keeping the event whole).
fn locate_value_span(
    data: &str,
    key_token: &str,
    value: &Value,
) -> Result<Option<(usize, usize)>, String> {
    let serialized = serde_json::to_string(value).map_err(|e| e.to_string())?;
    let Some(pos) = data.find(key_token) else {
        return Ok(None);
    };
    let start = pos + key_token.len();
    if !data[start..].starts_with(&serialized) {
        return Ok(None);
    }
    Ok(Some((start, start + serialized.len())))
}

/// Build the skeleton + blobs for a `system:init` event, or `None` to keep it
/// whole. Substitutes the varying spans (`sessionId`, `raw.session_id`,
/// `raw.uuid`, `raw.cwd`, `raw.tools`) with placeholders into a constant
/// skeleton, dedupes the sorted tool set into a shared blob, and verifies the
/// reconstruction is byte-exact before returning. Never re-serializes the event,
/// so it is agnostic to the recording app's struct version: a row whose field set
/// differs simply fails the verify and falls to zstd.
pub(crate) fn build_system_init_shape(event: &Event) -> Result<Option<BlobbedShape>, String> {
    let data = event.data.as_str();
    let Ok(value) = serde_json::from_str::<Value>(data) else {
        return Ok(None);
    };
    let Some(obj) = value.as_object() else {
        return Ok(None);
    };

    // (start, end, placeholder) spans to splice out of `data`, plus the `vars`
    // map (tag -> raw value) the stub inlines for the scalar fields.
    let mut spans: Vec<(usize, usize, String)> = Vec::new();
    let mut vars = serde_json::Map::new();
    let mut blobs: Vec<SegmentBlob> = Vec::new();

    let raw = obj.get("raw");
    // Each scalar varying field: (vars tag, key anchor, value). The tag doubles as
    // the placeholder body and the `vars` key. Absent or non-string fields are
    // skipped — a Codex init carries only `sessionId`.
    let scalars: [(&str, &str, Option<&Value>); 4] = [
        ("sessionId", "\"sessionId\":", obj.get("sessionId")),
        (
            "raw.session_id",
            "\"session_id\":",
            raw.and_then(|r| r.get("session_id")),
        ),
        ("raw.uuid", "\"uuid\":", raw.and_then(|r| r.get("uuid"))),
        ("raw.cwd", "\"cwd\":", raw.and_then(|r| r.get("cwd"))),
    ];
    for (tag, key_token, val) in scalars {
        let Some(val) = val else { continue };
        if !val.is_string() {
            continue;
        }
        let Some((start, end)) = locate_value_span(data, key_token, val)? else {
            return Ok(None);
        };
        spans.push((start, end, init_placeholder(tag)));
        vars.insert(tag.to_string(), val.clone());
    }

    // Tool set: dedupe the sorted unique names into a shared blob and keep only
    // the order permutation that restores this run's shuffled order.
    let mut toolset: Option<(String, Vec<usize>)> = None;
    let mut toolset_json: Option<String> = None;
    if let Some(tools_val) = raw.and_then(|r| r.get("tools")) {
        if let Some(arr) = tools_val.as_array() {
            if let Some(names) = arr
                .iter()
                .map(|t| t.as_str())
                .collect::<Option<Vec<&str>>>()
            {
                let Some((start, end)) = locate_value_span(data, "\"tools\":", tools_val)? else {
                    return Ok(None);
                };
                let mut sorted = names.clone();
                sorted.sort_unstable();
                sorted.dedup();
                let Some(order) = names
                    .iter()
                    .map(|n| sorted.binary_search(n).ok())
                    .collect::<Option<Vec<usize>>>()
                else {
                    return Ok(None);
                };
                let sorted_json = serde_json::to_string(&sorted).map_err(|e| e.to_string())?;
                let hash = sha256_hex(sorted_json.as_bytes());
                blobs.push((hash.clone(), compress(sorted_json.as_bytes())?));
                spans.push((start, end, init_placeholder(INIT_TOOLS_TAG)));
                toolset = Some((hash, order));
                toolset_json = Some(sorted_json);
            }
        }
    }

    // Splice placeholders over the located spans to form the constant skeleton.
    spans.sort_by_key(|(start, _, _)| *start);
    let mut skeleton = String::with_capacity(data.len());
    let mut cursor = 0usize;
    for (start, end, placeholder) in &spans {
        if *start < cursor {
            return Ok(None); // overlapping spans: untrustworthy, keep whole
        }
        skeleton.push_str(&data[cursor..*start]);
        skeleton.push_str(placeholder);
        cursor = *end;
    }
    skeleton.push_str(&data[cursor..]);
    let skeleton_hash = sha256_hex(skeleton.as_bytes());

    let mut stub = serde_json::Map::new();
    stub.insert("archived".to_string(), json!(ARCHIVED_SYSTEM_INIT));
    stub.insert("eventType".to_string(), json!(event.event_type));
    stub.insert("skeleton".to_string(), json!(skeleton_hash));
    if !vars.is_empty() {
        stub.insert("vars".to_string(), Value::Object(vars));
    }
    if let Some((hash, order)) = &toolset {
        stub.insert("toolset".to_string(), json!(hash));
        stub.insert("order".to_string(), json!(order));
    }
    let stub_data = Value::Object(stub).to_string();
    let render_sha = sha256_hex(data.as_bytes());

    // Verify-then-discard: reconstruct byte-exactly from the stub and the exact
    // blob texts the reader will load, and bail to zstd on any mismatch. Sharing
    // the reader's reassembly guarantees the verify exercises the real path.
    let mut verify_blobs: HashMap<String, String> = HashMap::new();
    verify_blobs.insert(skeleton_hash.clone(), skeleton.clone());
    if let (Some((hash, _)), Some(json)) = (&toolset, toolset_json) {
        verify_blobs.insert(hash.clone(), json);
    }
    if reconstruct::reassemble_system_init(&stub_data, &verify_blobs).as_deref() != Some(data) {
        return Ok(None);
    }

    blobs.push((skeleton_hash, compress(skeleton.as_bytes())?));
    Ok(Some((
        ArchivedShape::Blobbed {
            render_sha,
            data: stub_data,
        },
        blobs,
    )))
}
