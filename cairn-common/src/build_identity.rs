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
