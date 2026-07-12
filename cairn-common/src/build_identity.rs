use std::io::Read;
use std::path::Path;

/// Content identity for an executable. Unlike the package semver, this changes
/// whenever a rebuilt sidecar's bytes change and remains stable for identical
/// binaries.
pub fn executable_build_id(path: &Path) -> Result<String, String> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| format!("open executable {}: {error}", path.display()))?;
    // Two independently-seeded FNV-1a lanes give a compact deterministic
    // content fingerprint without making the low-level common crate depend on a
    // cryptography package. This is an equality identity, not a trust boundary.
    let mut first = 0xcbf29ce484222325_u64;
    let mut second = 0x84222325cbf29ce4_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("read executable {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        for byte in &buffer[..read] {
            first ^= u64::from(*byte);
            first = first.wrapping_mul(0x100000001b3);
            second ^= u64::from(*byte);
            second = second.wrapping_mul(0x100000001b3);
            second ^= first.rotate_left(17);
        }
    }
    Ok(format!("content-v1:{first:016x}{second:016x}"))
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
    }
}
