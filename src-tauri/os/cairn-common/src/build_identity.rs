use std::io::Read;
use std::path::Path;

/// Content identity for an executable. Unlike the package semver, this changes
/// whenever a rebuilt sidecar's bytes change and remains stable for identical
/// binaries.
pub fn executable_build_id(path: &Path) -> Result<String, String> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| format!("open executable {}: {error}", path.display()))?;
    // Two independently-seeded word-at-a-time lanes give a compact deterministic
    // content fingerprint without making the low-level common crate depend on a
    // cryptography package. This is an equality identity, not a trust boundary.
    // Hashing u64 words instead of every byte matters at process startup: debug
    // runner binaries are large enough that the former byte loop delayed the
    // first health response by more than a second.
    let mut first = 0xcbf29ce484222325_u64;
    let mut second = 0x84222325cbf29ce4_u64;
    let total_len = file
        .metadata()
        .map_err(|error| format!("stat executable {}: {error}", path.display()))?
        .len();
    let mut remaining = total_len;
    let mut buffer = [0_u8; 64 * 1024];
    while remaining > 0 {
        let read = usize::try_from(remaining.min(buffer.len() as u64))
            .expect("bounded executable read length");
        file.read_exact(&mut buffer[..read])
            .map_err(|error| format!("read executable {}: {error}", path.display()))?;
        remaining -= read as u64;
        let bytes = &buffer[..read];
        let mut words = bytes.chunks_exact(8);
        for chunk in &mut words {
            let word = u64::from_le_bytes(chunk.try_into().expect("eight-byte chunk"));
            first = (first ^ word).wrapping_mul(0x100000001b3);
            first ^= first.rotate_right(29);
            second = (second ^ word.rotate_left(31) ^ first).wrapping_mul(0x9e3779b185ebca87);
            second ^= second.rotate_right(33);
        }
        let tail = words.remainder();
        if !tail.is_empty() {
            let mut padded = [0_u8; 8];
            padded[..tail.len()].copy_from_slice(tail);
            let word = u64::from_le_bytes(padded) ^ ((tail.len() as u64) << 56);
            first = (first ^ word).wrapping_mul(0x100000001b3);
            second = (second ^ word.rotate_left(31) ^ first).wrapping_mul(0x9e3779b185ebca87);
        }
    }
    first ^= total_len;
    second ^= total_len.rotate_left(32);
    Ok(format!("content-v2:{first:016x}{second:016x}"))
}

/// A value that changes every time this process starts.
///
/// Containment evidence is bound to it so a measurement cannot outlive the
/// process that took it. A restart re-probes; it does not inherit. The identity
/// is per-process rather than per-machine on purpose — a host reboot, an
/// executor restart, and a re-exec all invalidate a prior measurement equally,
/// and a process-scoped value is the one thing that reliably moves for all
/// three.
pub fn process_boot_generation() -> String {
    static GENERATION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    GENERATION
        .get_or_init(|| uuid::Uuid::new_v4().simple().to_string())
        .clone()
}

pub fn current_executable_build_id() -> Result<String, String> {
    let path = std::env::current_exe().map_err(|error| format!("resolve current exe: {error}"))?;
    executable_build_id(&path)
}

/// Delimiters of the version stamp `sidecar_version_stamp!` embeds in every
/// sidecar built from this workspace.
///
/// Content identity above answers "are these the same bytes"; the stamp answers
/// "which version produced these bytes", which no hash can, because the expected
/// hash exists only after a rebuild. Build tooling reads the stamp by scanning
/// the file (`scripts/verify-bundle-sidecars.ts`) rather than running the binary:
/// a release leg must be able to check a cross-compiled sidecar it cannot
/// execute, and a build step must never hand argv to a daemon that could read an
/// unrecognized flag as "start up and take ownership of shared state".
pub const SIDECAR_VERSION_STAMP_OPEN: &str = "[cairn-sidecar-version:";
pub const SIDECAR_VERSION_STAMP_CLOSE: &str = "]";

/// Longest version a stamp may carry, bounding how far past an opening
/// delimiter [`read_embedded_version`] looks for the closing one.
const MAX_STAMPED_VERSION_LENGTH: usize = 64;

/// The version stamped into a binary's bytes, or `None` when it carries no
/// readable stamp.
///
/// The Rust twin of `readEmbeddedVersion` in `scripts/lib/sidecars.ts`, and
/// fail-closed the same way: a stamp that cannot be found is never a pass.
/// Rust string literals are length-prefixed rather than terminated and land
/// packed together in read-only data, so the closing delimiter is what bounds
/// the version. Scanning continues past an occurrence that does not parse,
/// because the opening delimiter also appears on its own wherever a crate links
/// the constant above.
pub fn read_embedded_version(bytes: &[u8]) -> Option<&str> {
    let open = SIDECAR_VERSION_STAMP_OPEN.as_bytes();
    let close = SIDECAR_VERSION_STAMP_CLOSE.as_bytes()[0];
    let mut from = 0;
    while let Some(offset) = find_subslice(&bytes[from..], open) {
        let start = from + offset + open.len();
        let window_end = bytes.len().min(start + MAX_STAMPED_VERSION_LENGTH + 1);
        if let Some(length) = bytes[start..window_end].iter().position(|b| *b == close) {
            let candidate = &bytes[start..start + length];
            if is_stamped_version(candidate) {
                return std::str::from_utf8(candidate).ok();
            }
        }
        from = from + offset + 1;
    }
    None
}

/// First occurrence of `needle`, scanning for its first byte so a multi-hundred
/// megabyte binary is not compared window by window.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    let first = *needle.first()?;
    let mut from = 0;
    while let Some(offset) = haystack[from..].iter().position(|byte| *byte == first) {
        let at = from + offset;
        if haystack[at..].starts_with(needle) {
            return Some(at);
        }
        from = at + 1;
    }
    None
}

/// `MAJOR.MINOR.PATCH`, optionally followed by one `-` or `+` suffix of
/// `[0-9A-Za-z.-]` — the grammar `STAMP_PATTERN` in `scripts/lib/sidecars.ts`
/// accepts.
fn is_stamped_version(candidate: &[u8]) -> bool {
    let mut rest = candidate;
    for part in 0..3 {
        let digits = rest.iter().take_while(|b| b.is_ascii_digit()).count();
        if digits == 0 {
            return false;
        }
        rest = &rest[digits..];
        if part < 2 {
            match rest.split_first() {
                Some((b'.', tail)) => rest = tail,
                _ => return false,
            }
        }
    }
    match rest.split_first() {
        None => true,
        Some((b'-' | b'+', suffix)) => {
            !suffix.is_empty()
                && suffix
                    .iter()
                    .all(|b| b.is_ascii_alphanumeric() || *b == b'.' || *b == b'-')
        }
        Some(_) => false,
    }
}

/// Embed the invoking binary's package version where build tooling can find it
/// by reading the file. Invoke once at the crate root of every sidecar staged
/// into `src-tauri/binaries/`, then call `retain_sidecar_version_stamp()` from
/// that binary's entry point.
///
/// `env!` expands against the invoking crate, so each sidecar stamps its own
/// `CARGO_PKG_VERSION` — which every first-party crate inherits from
/// `[workspace.package]`, making the stamp the release version of record.
#[macro_export]
macro_rules! sidecar_version_stamp {
    () => {
        /// `#[used]` keeps the compiler from discarding the stamp before linking.
        /// The entry-point reference below is also required: MSVC's linker may
        /// discard an otherwise unreferenced data section despite `#[used]`.
        #[used]
        static CAIRN_SIDECAR_VERSION_STAMP: &str =
            concat!("[cairn-sidecar-version:", env!("CARGO_PKG_VERSION"), "]");

        // `build-cmd.ts` supplies this tracked input. Its value changing makes
        // Cargo rebuild stamped sidecars after a workspace version bump. The
        // build script's schema-keyed target namespace handles artifacts compiled
        // before this input existed. `option_env!` keeps ordinary cargo-only
        // development and test builds valid.
        #[used]
        static CAIRN_SIDECAR_BUILD_VERSION_INPUT: Option<&str> =
            option_env!("CAIRN_SIDECAR_BUILD_VERSION");

        /// Make the stamp reachable from the executable entry point so every
        /// supported linker retains its bytes. `black_box` prevents optimization
        /// from proving that loading the static has no observable effect.
        #[inline(always)]
        fn retain_sidecar_version_stamp() {
            std::hint::black_box(CAIRN_SIDECAR_VERSION_STAMP);
            std::hint::black_box(CAIRN_SIDECAR_BUILD_VERSION_INPUT);
        }
    };
}

#[cfg(test)]
mod stamp_tests {
    // Expand the stamp into this test binary. Build tooling reads these bytes out
    // of a linked artifact, so proving the macro's text is right proves nothing;
    // what has to hold is that the stamp survives compilation and linking.
    crate::sidecar_version_stamp!();

    #[test]
    fn stamp_survives_into_the_linked_binary() {
        retain_sidecar_version_stamp();
        let exe = std::env::current_exe().expect("resolve test executable");
        let bytes = std::fs::read(&exe).expect("read test executable");
        let expected = format!(
            "{}{}{}",
            super::SIDECAR_VERSION_STAMP_OPEN,
            env!("CARGO_PKG_VERSION"),
            super::SIDECAR_VERSION_STAMP_CLOSE
        );

        assert!(
            bytes
                .windows(expected.len())
                .any(|window| window == expected.as_bytes()),
            "{} carries no {expected}; dead-data elimination reached the stamp, and \
             scripts/verify-bundle-sidecars.ts can no longer establish what a staged \
             sidecar was built from",
            exe.display()
        );
        assert_eq!(
            super::read_embedded_version(&bytes),
            Some(env!("CARGO_PKG_VERSION")),
            "the Rust stamp reader must find the stamp the macro linked in"
        );
    }
}

/// Parity with `readEmbeddedVersion` in `scripts/lib/sidecars.ts`: the same
/// inputs, the same answers. Nothing else ties the two readers together.
///
/// Every well-formed stamp here is assembled at runtime. A complete stamp
/// written as a literal would be linked into this test binary ahead of the
/// macro's own, and `stamp_survives_into_the_linked_binary` would read it.
#[cfg(test)]
mod read_stamp_tests {
    use super::{read_embedded_version, SIDECAR_VERSION_STAMP_CLOSE, SIDECAR_VERSION_STAMP_OPEN};

    fn stamp(version: &str) -> Vec<u8> {
        format!("{SIDECAR_VERSION_STAMP_OPEN}{version}{SIDECAR_VERSION_STAMP_CLOSE}").into_bytes()
    }

    fn surround(before: &[u8], version: &str, after: &[u8]) -> Vec<u8> {
        [before, &stamp(version), after].concat()
    }

    #[test]
    fn reads_a_stamp_packed_among_other_literals() {
        let bytes = surround(b"\x00\x7fELFjunk", "0.12.33", b"next-literal");
        assert_eq!(read_embedded_version(&bytes), Some("0.12.33"));
    }

    #[test]
    fn an_unstamped_binary_has_no_version() {
        assert_eq!(read_embedded_version(b"plain bytes with no stamp"), None);
        assert_eq!(read_embedded_version(b""), None);
    }

    #[test]
    fn a_bare_delimiter_is_skipped_for_the_real_stamp_after_it() {
        // The opening delimiter linked on its own (as the constant), followed
        // by the real stamp later in the image.
        let bytes = surround(b"[cairn-sidecar-version:]]]padding", "1.2.3", b"");
        assert_eq!(read_embedded_version(&bytes), Some("1.2.3"));
    }

    #[test]
    fn malformed_occurrences_never_pass() {
        for bytes in [
            &b"[cairn-sidecar-version:1.2]"[..],
            b"[cairn-sidecar-version:v1.2.3]",
            b"[cairn-sidecar-version:1.2.3 ]",
            b"[cairn-sidecar-version:1.2.3-]",
            b"[cairn-sidecar-version:1.2.3",
        ] {
            assert_eq!(
                read_embedded_version(bytes),
                None,
                "{}",
                String::from_utf8_lossy(bytes)
            );
        }
    }

    #[test]
    fn prerelease_and_build_suffixes_are_versions() {
        for version in ["0.13.0-rc.1", "0.13.0+ci.7"] {
            assert_eq!(read_embedded_version(&stamp(version)), Some(version));
        }
    }

    #[test]
    fn a_version_past_the_length_bound_is_not_read() {
        let long = format!("[cairn-sidecar-version:1.2.3-{}]", "a".repeat(80));
        assert_eq!(read_embedded_version(long.as_bytes()), None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_identity_tracks_bytes_not_path_or_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        std::fs::write(&first, b"same bytes").unwrap();
        std::fs::write(&second, b"same bytes").unwrap();
        assert_eq!(
            executable_build_id(&first).unwrap(),
            executable_build_id(&second).unwrap()
        );

        std::fs::write(&second, b"different bytes").unwrap();
        assert_ne!(
            executable_build_id(&first).unwrap(),
            executable_build_id(&second).unwrap()
        );
        assert!(executable_build_id(&first)
            .unwrap()
            .starts_with("content-v2:"));
    }

    #[test]
    fn build_identity_tracks_each_word_and_tail() {
        let dir = tempfile::tempdir().unwrap();
        let baseline = dir.path().join("baseline");
        let changed_word = dir.path().join("changed-word");
        let changed_tail = dir.path().join("changed-tail");
        std::fs::write(&baseline, b"01234567tail").unwrap();
        std::fs::write(&changed_word, b"01234566tail").unwrap();
        std::fs::write(&changed_tail, b"01234567tall").unwrap();

        let baseline_id = executable_build_id(&baseline).unwrap();
        assert_ne!(baseline_id, executable_build_id(&changed_word).unwrap());
        assert_ne!(baseline_id, executable_build_id(&changed_tail).unwrap());
    }
}
