//! Bounded streaming scrubber.
//!
//! A credential written to a pipe does not arrive whole. `sk-live-…` can be
//! split across two reads, and scrubbing each read independently would emit the
//! halves untouched. The scrubber therefore withholds a suffix long enough that
//! any registered form spanning the boundary is still contiguous in the buffer
//! when the next chunk arrives.
//!
//! The withheld suffix is bounded: it never exceeds [`MAX_CARRY_BYTES`], so a
//! pathologically long registration cannot turn a stream into an unbounded
//! buffer. A form longer than the bound is matched only when it happens to land
//! inside one chunk, which is stated here rather than papered over.
//!
//! Withholding is also *conditional*, not unconditional, and that is what makes
//! the scrubber usable on a live stream. A value split across a chunk boundary
//! leaves its head as a suffix of what is held, and that head is a prefix of the
//! form it belongs to — so the only run that must wait is the longest suffix
//! that is a proper prefix of some registered form. Everything before it is
//! settled and goes out immediately. For output that does not begin to spell a
//! credential, which is essentially all of it, nothing waits at all: a terminal
//! echoes a keystroke the moment it arrives.
//!
//! An earlier version of this asked a weaker question — whether the trailing
//! bytes were drawn from the alphabet the registered forms are built from. That
//! is necessary but nowhere near sufficient, and it failed in exactly the way
//! this rule exists to prevent. The callback credential is registered as 32 raw
//! random bytes; between its percent and base64 forms the alphabet covers most
//! of printable ASCII, so a single echoed keystroke was withheld in four installs
//! out of five. The rule now tests what it actually means to test.
//!
//! Every consumer must call [`StreamingScrubber::flush`] at end of stream. A
//! finalize path that skips it truncates the withheld suffix, and that failure
//! reads as a rendering glitch rather than as a bug.

use std::sync::Arc;

use super::registry::{registry, Detections, RegistrySnapshot};

/// Ceiling on the withheld suffix, and therefore on the scrubber's memory.
pub const MAX_CARRY_BYTES: usize = 8 * 1024;

/// Longest run of bytes a structural look-behind could need. Reserved so a
/// future structural streaming mode does not change the buffering contract.
const STRUCTURAL_LOOKBEHIND: usize = 64;

/// Scrubs a byte stream of registered credentials across chunk boundaries.
pub struct StreamingScrubber {
    snapshot: Arc<RegistrySnapshot>,
    carry: Vec<u8>,
    carry_bound: usize,
    found: Detections,
}

impl Default for StreamingScrubber {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamingScrubber {
    /// Build against the process registry as it stands now.
    ///
    /// The snapshot is taken once: a stream is scrubbed against one consistent
    /// set of registered values, so a mid-stream registration does not change
    /// how much of the stream is already in flight.
    pub fn new() -> Self {
        Self::with_snapshot(registry().snapshot())
    }

    pub fn with_snapshot(snapshot: Arc<RegistrySnapshot>) -> Self {
        let carry_bound = if snapshot.is_empty() {
            0
        } else {
            // Safe to clamp: the floor is a compile-time constant well below
            // the ceiling, so the panicking `max < min` case cannot arise.
            const _: () = assert!(STRUCTURAL_LOOKBEHIND <= MAX_CARRY_BYTES);
            snapshot
                .max_needle_len()
                .saturating_sub(1)
                .clamp(STRUCTURAL_LOOKBEHIND, MAX_CARRY_BYTES)
        };
        Self {
            snapshot,
            carry: Vec::new(),
            carry_bound,
            found: Detections::new(),
        }
    }

    pub fn detections(&self) -> &Detections {
        &self.found
    }

    /// Bytes currently withheld awaiting more input.
    pub fn pending(&self) -> usize {
        self.carry.len()
    }

    /// Feed a chunk; get back the prefix that is safe to emit now.
    pub fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        if self.carry_bound == 0 {
            // Nothing registered: the scrubber is a pass-through and must not
            // add latency to output that has nothing to hide.
            return chunk.to_vec();
        }
        self.carry.extend_from_slice(chunk);
        if let Some(scrubbed) = self.snapshot.scrub_bytes(&self.carry, &mut self.found) {
            self.carry = scrubbed;
        }
        let split = utf8_boundary_at_or_before(&self.carry, self.settled_prefix());
        self.carry.drain(..split).collect()
    }

    /// How much of the carry cannot belong to a match still arriving.
    ///
    /// The withheld run is bounded twice over: by `carry_bound`, so a
    /// pathological registration cannot grow the buffer without limit, and by
    /// the forms themselves, since nothing longer than a form's own length can
    /// be a proper prefix of it.
    fn settled_prefix(&self) -> usize {
        let unsettled = self
            .snapshot
            .unsettled_suffix_len(&self.carry, self.carry_bound);
        self.carry.len() - unsettled
    }

    /// End of stream: emit everything still withheld.
    ///
    /// A terminal flush is where a credential that arrived as the very last
    /// bytes of a stream would otherwise escape, so the buffer is scrubbed once
    /// more before it is released.
    pub fn flush(&mut self) -> Vec<u8> {
        if let Some(scrubbed) = self.snapshot.scrub_bytes(&self.carry, &mut self.found) {
            self.carry = scrubbed;
        }
        std::mem::take(&mut self.carry)
    }
}

/// Largest index `<= at` that does not split a UTF-8 character.
///
/// Emitting a partial code point renders as a replacement character in the UI
/// even though the bytes are intact, so a split lands on a character boundary.
/// An incomplete trailing sequence is the only error a UTF-8 stream cut at an
/// arbitrary offset can produce; any other error means the data is not UTF-8 at
/// all — a PTY carrying binary — and the requested split is used as-is.
fn utf8_boundary_at_or_before(buf: &[u8], at: usize) -> usize {
    let at = at.min(buf.len());
    match std::str::from_utf8(&buf[..at]) {
        Ok(_) => at,
        Err(error) if error.error_len().is_none() => error.valid_up_to(),
        Err(_) => at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::registry::SecretRegistry;
    use crate::security::secret::{SecretCategory, SecretId, SecretMaterial};

    const SECRET: &str = "sk-live-Qa9Zm2Xp7Lr4";

    fn snapshot_with(values: &[&str]) -> Arc<RegistrySnapshot> {
        let registry = Box::leak(Box::new(SecretRegistry::new()));
        for (index, value) in values.iter().enumerate() {
            registry
                .register(
                    SecretId::new(format!("test-{index}")),
                    SecretCategory::CallbackCredential,
                    "unit test",
                    SecretMaterial::from_string((*value).to_string()),
                )
                .expect("registerable")
                .retain_for_process();
        }
        registry.snapshot()
    }

    fn drive(chunks: &[&[u8]], snapshot: Arc<RegistrySnapshot>) -> String {
        let mut scrubber = StreamingScrubber::with_snapshot(snapshot);
        let mut out = Vec::new();
        for chunk in chunks {
            out.extend(scrubber.push(chunk));
        }
        out.extend(scrubber.flush());
        String::from_utf8(out).expect("utf-8 out")
    }

    #[test]
    fn catches_the_secret_at_every_split_point() {
        let snapshot = snapshot_with(&[SECRET]);
        let full = format!("head {SECRET} tail");
        for split in 0..=full.len() {
            if !full.is_char_boundary(split) {
                continue;
            }
            let (left, right) = full.split_at(split);
            let out = drive(&[left.as_bytes(), right.as_bytes()], snapshot.clone());
            assert!(!out.contains(SECRET), "leaked at split {split}: {out}");
            assert_eq!(out, "head [REDACTED] tail", "split {split}");
        }
    }

    #[test]
    fn catches_a_secret_delivered_one_byte_at_a_time() {
        let snapshot = snapshot_with(&[SECRET]);
        let full = format!("a{SECRET}b");
        let chunks: Vec<&[u8]> = full.as_bytes().chunks(1).collect();
        assert_eq!(drive(&chunks, snapshot), "a[REDACTED]b");
    }

    #[test]
    fn catches_encoded_forms_across_splits() {
        use base64::Engine;
        let snapshot = snapshot_with(&[SECRET]);
        let encoded = base64::engine::general_purpose::STANDARD.encode(SECRET);
        let full = format!("x {encoded} y");
        for split in 1..full.len() {
            let (left, right) = full.split_at(split);
            let out = drive(&[left.as_bytes(), right.as_bytes()], snapshot.clone());
            assert!(!out.contains(&encoded), "leaked encoded at {split}");
        }
    }

    /// The streaming counterpart of the earliest-match regression: two secrets
    /// in one stream, split at every point including between the occurrences.
    #[test]
    fn two_secrets_split_across_a_chunk_boundary_are_both_caught() {
        let short = "shortAAA12345";
        let long = "muchLongerSecretValue987654";
        let snapshot = snapshot_with(&[short, long]);
        let full = format!("a {short} b {long} c");
        for split in 0..=full.len() {
            if !full.is_char_boundary(split) {
                continue;
            }
            let (left, right) = full.split_at(split);
            let out = drive(&[left.as_bytes(), right.as_bytes()], snapshot.clone());
            assert!(!out.contains(short), "leaked short at split {split}: {out}");
            assert!(!out.contains(long), "leaked long at split {split}: {out}");
            assert_eq!(out, "a [REDACTED] b [REDACTED] c", "split {split}");
        }
    }

    /// One credential streamed in both its raw and base64 forms.
    #[test]
    fn raw_and_encoded_forms_of_one_secret_are_both_caught_across_splits() {
        use base64::Engine;

        let snapshot = snapshot_with(&[SECRET]);
        let encoded = base64::engine::general_purpose::STANDARD.encode(SECRET);
        let full = format!("raw={SECRET} enc={encoded}");
        for split in 0..=full.len() {
            let (left, right) = full.split_at(split);
            let out = drive(&[left.as_bytes(), right.as_bytes()], snapshot.clone());
            assert!(!out.contains(SECRET), "leaked raw at split {split}: {out}");
            assert!(
                !out.contains(&encoded),
                "leaked encoded at split {split}: {out}"
            );
        }
    }

    #[test]
    fn catches_json_string_escaped_form_at_every_split_point() {
        let secret = "SYNTH-Q7\"m2Zx9\\line\n-RedTeam";
        let snapshot = snapshot_with(&[secret]);
        let encoded = serde_json::to_string(secret).unwrap();
        let escaped = &encoded[1..encoded.len() - 1];
        let full = format!("json={escaped};");
        for split in 0..=full.len() {
            let (left, right) = full.split_at(split);
            let out = drive(&[left.as_bytes(), right.as_bytes()], snapshot.clone());
            assert_eq!(out, "json=[REDACTED];", "split {split}: {out}");
            assert!(!out.contains(escaped), "leaked at split {split}: {out}");
        }
    }

    #[test]
    fn terminal_flush_releases_a_trailing_secret_redacted() {
        let snapshot = snapshot_with(&[SECRET]);
        let mut scrubber = StreamingScrubber::with_snapshot(snapshot);
        let emitted = scrubber.push(format!("tail {SECRET}").as_bytes());
        let flushed = scrubber.flush();
        let out = String::from_utf8([emitted, flushed].concat()).unwrap();
        assert_eq!(out, "tail [REDACTED]");
        assert_eq!(scrubber.pending(), 0);
    }

    #[test]
    fn never_emits_a_split_utf8_character() {
        let snapshot = snapshot_with(&[SECRET]);
        let text = "日本語".repeat(64);
        let mut scrubber = StreamingScrubber::with_snapshot(snapshot);
        let mut out = Vec::new();
        for chunk in text.as_bytes().chunks(7) {
            let emitted = scrubber.push(chunk);
            assert!(std::str::from_utf8(&emitted).is_ok(), "split a character");
            out.extend(emitted);
        }
        out.extend(scrubber.flush());
        assert_eq!(String::from_utf8(out).unwrap(), text);
    }

    #[test]
    fn an_empty_registry_is_a_pass_through() {
        let snapshot = snapshot_with(&[]);
        let mut scrubber = StreamingScrubber::with_snapshot(snapshot);
        assert_eq!(scrubber.push(b"anything at all"), b"anything at all");
        assert_eq!(scrubber.pending(), 0);
    }

    /// Output that does not begin to spell a credential leaves immediately.
    ///
    /// Asserted on `pending()` rather than on the text, because a scrubber that
    /// withholds and later releases produces the same final string either way —
    /// only the in-flight state tells the two apart, and the in-flight state is
    /// what a person watching a terminal actually sees.
    #[test]
    fn output_that_does_not_start_a_credential_is_released_immediately() {
        let snapshot = snapshot_with(&[SECRET]);
        let mut scrubber = StreamingScrubber::with_snapshot(snapshot);
        for chunk in ["user@host:~$ ", "building...\n", "\tstep\r\n", "ls -la"] {
            assert_eq!(scrubber.push(chunk.as_bytes()), chunk.as_bytes());
            assert_eq!(scrubber.pending(), 0, "withheld {chunk:?}");
        }
    }

    /// Regression for the release rule that shipped first, measured rather than
    /// reasoned about.
    ///
    /// The runner registers its callback and operator credentials as 32 *raw
    /// random bytes*, and the derived percent and base64 forms of such a value
    /// cover most of printable ASCII between them. A release rule that asks only
    /// "is this byte one a registered form contains" therefore answers yes almost
    /// always: measured over 200 simulated installs, a single echoed keystroke
    /// was withheld in 163 of them, and typing `git status` one character at a
    /// time left four characters withheld on average and all ten in the worst
    /// case — nothing appearing until Enter.
    ///
    /// Cairn terminals are interactive and PTY echo is what the user reads, so
    /// that is a broken terminal, which is how a security feature gets turned
    /// off. This drives the real fixture: random 32-byte credentials, keystrokes
    /// arriving one byte at a time.
    #[test]
    fn raw_random_credentials_do_not_stall_keystroke_echo() {
        let registry = Box::leak(Box::new(SecretRegistry::new()));
        for (index, seed) in [17u8, 211].into_iter().enumerate() {
            // Deterministic but spread across the byte range, which is what makes
            // the alphabet wide. A real credential is random; this reproduces the
            // property that matters without a random-number dependency.
            let value: Vec<u8> = (0..32u8)
                .map(|step| seed.wrapping_mul(step.wrapping_add(7)).wrapping_add(step))
                .collect();
            registry
                .register(
                    SecretId::new(format!("raw-{index}")),
                    SecretCategory::CallbackCredential,
                    "unit test",
                    SecretMaterial::from_bytes(&value),
                )
                .expect("registerable")
                .retain_for_process();
        }
        let mut scrubber = StreamingScrubber::with_snapshot(registry.snapshot());
        let mut echoed = Vec::new();
        for byte in b"git status" {
            echoed.extend(scrubber.push(&[*byte]));
        }
        assert_eq!(
            String::from_utf8(echoed).unwrap(),
            "git status",
            "a keystroke was withheld, so this terminal looks frozen"
        );
        assert_eq!(scrubber.pending(), 0);
    }

    /// The converse: a trailing run that is a proper prefix of a registered form
    /// is exactly the shape a value split across a chunk boundary has, so it
    /// waits.
    #[test]
    fn a_trailing_prefix_of_a_registered_form_is_withheld_until_it_settles() {
        let snapshot = snapshot_with(&[SECRET]);
        let mut scrubber = StreamingScrubber::with_snapshot(snapshot);
        let head = &SECRET.as_bytes()[..6];
        assert!(scrubber.push(head).is_empty(), "emitted a partial value");
        assert_eq!(scrubber.pending(), head.len());
        let emitted = scrubber.push(&SECRET.as_bytes()[6..]);
        let out = String::from_utf8([emitted, scrubber.flush()].concat()).unwrap();
        assert_eq!(out, "[REDACTED]");
    }

    /// Releasing settled bytes early must not release a partial character. A
    /// multi-byte character's continuation bytes are outside an ASCII needle's
    /// alphabet, so the settled prefix lands mid-character unless the UTF-8
    /// boundary check pulls it back.
    #[test]
    fn a_settled_prefix_never_ends_mid_character() {
        let snapshot = snapshot_with(&[SECRET]);
        let text = "日本語".repeat(8);
        let mut scrubber = StreamingScrubber::with_snapshot(snapshot);
        let mut out = Vec::new();
        for chunk in text.as_bytes().chunks(5) {
            let emitted = scrubber.push(chunk);
            assert!(std::str::from_utf8(&emitted).is_ok(), "split a character");
            out.extend(emitted);
        }
        out.extend(scrubber.flush());
        assert_eq!(String::from_utf8(out).unwrap(), text);
    }

    #[test]
    fn withheld_suffix_stays_bounded() {
        let snapshot = snapshot_with(&[SECRET]);
        let mut scrubber = StreamingScrubber::with_snapshot(snapshot);
        for _ in 0..64 {
            scrubber.push(&[b'x'; 4096]);
            assert!(scrubber.pending() <= MAX_CARRY_BYTES);
        }
    }
}
