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
