//! The runner's own pass over output that came back from an executor.
//!
//! An executor already scrubs what it emits, so this is a second pass over the
//! same bytes — but it is not a redundant one, because the two registries hold
//! deliberately different sets. The executor registers the per-batch relay
//! capability it injects and nothing else; a remote machine is never told the
//! runner's own credentials, which is the CAIRN-3385 property. Meanwhile the
//! runner hands its `CAIRN_MCP_SECRET` to terminals, workflows, and host
//! processes, and the colocated executor is a child of this process. So the one
//! credential most likely to appear in a terminal's output is precisely the one
//! the executor cannot recognize. Each end scrubs for what it knows.
//!
//! Relayed output arrives in chunks whose boundaries fall wherever the
//! executor's coalescer put them, so this pass is streaming rather than
//! per-chunk: a value split across two frames is only contiguous if one carry
//! sees both halves. That makes an explicit end-of-stream flush mandatory. A
//! stream that is dropped without one loses whatever the scrubber was still
//! withholding, and that failure reads as a rendering glitch rather than a bug,
//! which is why every caller here drains at its terminal outcome.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cairn_common::executor_protocol::ResidentProcessStream;

use crate::security::StreamingScrubber;

/// Streaming scrubbers for relayed output, one per live stream.
#[derive(Default)]
pub(crate) struct RelayedOutput {
    streams: HashMap<String, StreamingScrubber>,
}

/// A guard shared between the callback that feeds it and the batch that drains
/// it. The batch holds its own handle, so a context revoked early — by a lost
/// link, or by the result arriving before the submitting future returns — still
/// gets flushed by the code that knows the batch is over.
pub(crate) type SharedRelayedOutput = Arc<Mutex<RelayedOutput>>;

/// Separator for composite stream keys. A unit separator cannot occur in an
/// executor id, a process key, or a stream name, so no two streams can collide
/// by spelling.
const KEY_SEPARATOR: char = '\u{1f}';

/// Every stream a resident process can produce.
///
/// The exit flush walks this list to drain a process whose streams it cannot
/// enumerate from the exit event itself, which carries no stream of its own. A
/// stream missing from it is never flushed, and an unflushed stream truncates
/// silently rather than failing — so the list is held closed by the compiler
/// through [`stream_index`], not by this comment.
const RESIDENT_STREAMS: [ResidentProcessStream; 3] = [
    ResidentProcessStream::Stdout,
    ResidentProcessStream::Stderr,
    ResidentProcessStream::Pty,
];

/// Where a stream sits in [`RESIDENT_STREAMS`], and how it is named in a key.
///
/// Exhaustive by construction: adding a variant to `ResidentProcessStream`
/// stops this compiling, which is the point. Whoever adds it is then forced to
/// place it in the flush list rather than discovering later that one stream's
/// tail quietly disappears.
///
/// It also keeps stream identity out of the `Debug` derive. Spelling the stream
/// into a key via `{stream:?}` would make two scrubbers distinct because of how
/// a derive happens to render — a rename would silently repartition live
/// streams, splicing one process's carry onto another's.
const fn stream_index(stream: ResidentProcessStream) -> usize {
    match stream {
        ResidentProcessStream::Stdout => 0,
        ResidentProcessStream::Stderr => 1,
        ResidentProcessStream::Pty => 2,
    }
}

impl RelayedOutput {
    pub(crate) fn shared() -> SharedRelayedOutput {
        Arc::new(Mutex::new(Self::default()))
    }

    fn stream(&mut self, key: &str) -> &mut StreamingScrubber {
        self.streams.entry(key.to_string()).or_default()
    }

    /// Scrub a chunk, returning the part that cannot still be part of a value
    /// arriving in the next one.
    pub(crate) fn settle_bytes(&mut self, key: &str, chunk: &[u8]) -> Vec<u8> {
        self.stream(key).push(chunk)
    }

    /// Text form of [`Self::settle_bytes`].
    ///
    /// The scrubber releases only on character boundaries, so the conversion is
    /// exact for a UTF-8 stream and lossy only for output that was never text.
    pub(crate) fn settle_text(&mut self, key: &str, chunk: &str) -> String {
        let settled = self.settle_bytes(key, chunk.as_bytes());
        String::from_utf8_lossy(&settled).into_owned()
    }

    /// Release one stream's withheld tail and forget the stream.
    pub(crate) fn finish_bytes(&mut self, key: &str) -> Vec<u8> {
        self.streams
            .remove(key)
            .map(|mut scrubber| scrubber.flush())
            .unwrap_or_default()
    }

    /// Release every stream's withheld tail, keyed as the caller keyed it.
    ///
    /// Empty tails are dropped rather than reported, so a caller can publish
    /// everything this returns without filtering.
    pub(crate) fn finish_all_text(&mut self) -> Vec<(String, String)> {
        std::mem::take(&mut self.streams)
            .into_iter()
            .filter_map(|(key, mut scrubber)| {
                let residue = scrubber.flush();
                (!residue.is_empty()).then(|| (key, String::from_utf8_lossy(&residue).into_owned()))
            })
            .collect()
    }

    /// Drop every stream belonging to one executor link.
    ///
    /// A link that dies mid-stream never delivers the exits that would have
    /// drained its processes, so without this the map keeps a scrubber per
    /// process that will never speak again.
    pub(crate) fn forget_executor(&mut self, executor_id: &str) {
        let prefix = format!("{executor_id}{KEY_SEPARATOR}");
        self.streams.retain(|key, _| !key.starts_with(&prefix));
    }
}

/// Identity of one resident process's stream, stable across its lifetime.
///
/// The generation is part of it because a process key is reused: a terminal that
/// exits and respawns under the same key is a different stream, and carrying the
/// old scrubber's withheld bytes into it would splice one process's output onto
/// another's.
pub(crate) fn resident_stream_key(
    executor_id: &str,
    holder_key: &str,
    process_key: &str,
    generation: u64,
    stream: ResidentProcessStream,
) -> String {
    let stream = stream_index(stream);
    format!(
        "{executor_id}{KEY_SEPARATOR}{holder_key}{KEY_SEPARATOR}{process_key}{KEY_SEPARATOR}{generation}{KEY_SEPARATOR}{stream}"
    )
}

/// Drain every stream of one resident process, in the order they are declared.
///
/// Called when the process exits, before the exit reaches its subscribers, so
/// the last bytes land in the scrollback the exit is about to seal.
pub(crate) fn finish_resident_process(
    output: &SharedRelayedOutput,
    executor_id: &str,
    holder_key: &str,
    process_key: &str,
    generation: u64,
) -> Vec<(ResidentProcessStream, Vec<u8>)> {
    let mut guard = output.lock().unwrap();
    RESIDENT_STREAMS
        .into_iter()
        .filter_map(|stream| {
            let key = resident_stream_key(executor_id, holder_key, process_key, generation, stream);
            let residue = guard.finish_bytes(&key);
            (!residue.is_empty()).then_some((stream, residue))
        })
        .collect()
}

/// Release what a finished batch's output scrubbers are still withholding.
///
/// The last bytes of a stream are exactly the ones a scrubber holds back, so a
/// batch that ends without draining loses its own tail. Every submission path
/// calls this at its terminal outcome, whatever that outcome was — completion,
/// failure, cancellation, a lost executor all end the same await — and the run
/// identity is passed in rather than looked up, because by this point the batch's
/// callback context has usually already been revoked.
pub(crate) fn flush_batch_output(
    orch: &crate::orchestrator::Orchestrator,
    output: &SharedRelayedOutput,
    run_context: Option<&crate::mcp::handlers::RunContext>,
) {
    let residue = output.lock().unwrap().finish_all_text();
    let Some(run_context) = run_context else {
        return;
    };
    for (stream_id, chunk) in residue {
        let _ = orch.services.emitter.emit(
            "run-output",
            serde_json::json!({
                "runId": run_context.run_id,
                "toolUseId": stream_id,
                "chunk": chunk,
                "stream": "stdout",
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::{registry, SecretCategory, SecretGuard, SecretId, SecretMaterial};
    use base64::Engine;

    /// Two credentials with the SHORTER one earlier in the stream. This is the
    /// shape that defeats a scan which takes whichever needle matches first:
    /// such a scan copies everything before its hit verbatim, so the shorter
    /// value sitting earlier goes out in the clear.
    const SHORT: &str = "shortAAA12345";
    const LONG: &str = "muchLongerSecretValue987654";

    fn register(id: &str, value: &str) -> SecretGuard<'static> {
        registry()
            .register(
                SecretId::new(id),
                SecretCategory::BatchCapability,
                "unit test",
                SecretMaterial::from_string(value.to_string()),
            )
            .expect("registerable")
    }

    /// Drive one stream through the guard, splitting it at `at`.
    fn relay(full: &str, at: usize) -> String {
        let mut output = RelayedOutput::default();
        let (left, right) = full.split_at(at);
        let mut out = output.settle_text("stream", left);
        out.push_str(&output.settle_text("stream", right));
        for (_, residue) in output.finish_all_text() {
            out.push_str(&residue);
        }
        out
    }

    /// The property, not the path: two registered values at different offsets
    /// and one of them in two forms, split at EVERY boundary. A single
    /// credential appearing once cannot fail on a scan loop no matter how the
    /// loop is written, which is how the first version of this shipped broken.
    #[test]
    fn every_registered_value_and_form_survives_every_chunk_split() {
        let _short = register("relay-short", SHORT);
        let _long = register("relay-long", LONG);
        let encoded = base64::engine::general_purpose::STANDARD.encode(SHORT);
        let full = format!("a {SHORT} b {LONG} c {encoded} d");
        for at in 0..=full.len() {
            if !full.is_char_boundary(at) {
                continue;
            }
            let out = relay(&full, at);
            assert!(!out.contains(SHORT), "leaked short at {at}: {out}");
            assert!(!out.contains(LONG), "leaked long at {at}: {out}");
            assert!(!out.contains(&encoded), "leaked encoded at {at}: {out}");
            assert!(
                !out.contains(encoded.trim_end_matches('=')),
                "leaked the unpadded encoding at {at}: {out}"
            );
            // Deliberately not one exact string. Unpadded base64 is a proper
            // prefix of the padded form, so a split landing inside the token
            // lets the unpadded needle match before the padding arrives, leaving
            // `==` behind the marker. What survives is padding, never credential
            // bytes — the assertions above hold at every split — so the property
            // is what gets pinned here and the cosmetic residue is recorded in
            // docs/secret-redaction.md rather than contorted away.
            let normalized = out.replace("[REDACTED]==", "[REDACTED]");
            assert_eq!(
                normalized, "a [REDACTED] b [REDACTED] c [REDACTED] d",
                "split {at}"
            );
        }
    }

    /// What the flush is actually for.
    ///
    /// A stream that ends on a run of alphabet bytes leaves the scrubber holding
    /// them: at that moment they are indistinguishable from the first half of a
    /// credential whose second half has not arrived, and only end of stream
    /// resolves it. A finalize path that skips the flush therefore does not leak
    /// — it TRUNCATES, silently, which is why the failure reads as a rendering
    /// glitch rather than as a bug, and why it earns a test of its own.
    #[test]
    fn a_finalize_path_that_skips_the_flush_truncates_the_stream() {
        let _guard = register("relay-tail", LONG);
        let partial = &LONG[..10];
        let mut output = RelayedOutput::default();
        let emitted = output.settle_text("stream", &format!("tail {partial}"));
        assert_eq!(emitted, "tail ", "an unsettled tail must not go out early");
        assert_eq!(
            output.finish_all_text(),
            vec![("stream".to_string(), partial.to_string())],
            "the flush owes the caller the bytes it withheld"
        );
    }

    /// And the flush still scrubs what it releases: a credential arriving as the
    /// very last bytes of a stream is redacted by the flush, not handed over raw.
    #[test]
    fn the_flush_scrubs_what_it_releases() {
        let _guard = register("relay-flush", LONG);
        let mut output = RelayedOutput::default();
        let mut out = String::new();
        for chunk in ["tail ", &LONG[..9], &LONG[9..]] {
            out.push_str(&output.settle_text("stream", chunk));
        }
        for (_, residue) in output.finish_all_text() {
            out.push_str(&residue);
        }
        assert_eq!(out, "tail [REDACTED]");
    }

    #[test]
    fn streams_are_scrubbed_independently_and_flushed_once() {
        let mut output = RelayedOutput::default();
        assert_eq!(output.settle_text("a", "first "), "first ");
        assert_eq!(output.settle_text("b", "second "), "second ");
        assert!(output.finish_all_text().is_empty(), "nothing was withheld");
        // Draining forgets the streams, so a second drain is a no-op rather than
        // a replay.
        assert!(output.finish_all_text().is_empty());
    }

    /// Each stream carries its own scrubber.
    ///
    /// Halves of a value that land on two different streams are two ordinary
    /// strings, and each stream must return its own bytes rather than borrow the
    /// other's carry. Sharing one scrubber across streams would splice output
    /// that was never adjacent, which corrupts interleaved stdout and stderr for
    /// the sake of a match that was never really there.
    #[test]
    fn each_stream_carries_its_own_scrubber() {
        let _guard = register("relay-split-streams", LONG);
        let mut output = RelayedOutput::default();
        let (head, tail) = LONG.split_at(8);
        // `head` begins the credential, so it waits; `tail` does not begin it,
        // so it settles at once. That asymmetry is the point: if the two streams
        // shared one carry, `tail` would complete `head` into a match that was
        // never contiguous in either stream.
        let mut emitted = vec![
            ("stdout".to_string(), output.settle_text("stdout", head)),
            ("stderr".to_string(), output.settle_text("stderr", tail)),
        ];
        for (stream, residue) in output.finish_all_text() {
            let slot = emitted
                .iter_mut()
                .find(|(name, _)| *name == stream)
                .expect("a stream it was fed");
            slot.1.push_str(&residue);
        }
        emitted.sort();
        assert_eq!(
            emitted,
            vec![
                ("stderr".to_string(), tail.to_string()),
                ("stdout".to_string(), head.to_string()),
            ],
            "each stream owes back exactly its own bytes"
        );
    }

    /// The compiler keeps `stream_index` exhaustive; this keeps the flush list
    /// and that index agreeing, so a new variant cannot be given an index and
    /// then left out of the walk.
    #[test]
    fn the_flush_list_holds_every_stream_exactly_once() {
        for (position, stream) in RESIDENT_STREAMS.into_iter().enumerate() {
            assert_eq!(stream_index(stream), position, "{stream:?} is misplaced");
        }
    }

    #[test]
    fn a_process_generation_does_not_inherit_the_previous_ones_stream() {
        let first = resident_stream_key("exec", "job:1", "main", 1, ResidentProcessStream::Pty);
        let second = resident_stream_key("exec", "job:1", "main", 2, ResidentProcessStream::Pty);
        assert_ne!(first, second);
    }

    #[test]
    fn a_lost_link_forgets_only_its_own_streams() {
        let mut output = RelayedOutput::default();
        let mine = resident_stream_key("gone", "job:1", "main", 1, ResidentProcessStream::Stdout);
        let theirs = resident_stream_key("live", "job:1", "main", 1, ResidentProcessStream::Stdout);
        output.settle_text(&mine, "x");
        output.settle_text(&theirs, "y");
        output.forget_executor("gone");
        assert!(!output.streams.contains_key(&mine));
        assert!(output.streams.contains_key(&theirs));
    }
}
