use std::path::{Path, PathBuf};

fn collect_inputs(root: &Path, directory: &Path, inputs: &mut Vec<PathBuf>) {
    // Cargo watches a directory's entry list as well as its contents. Registering
    // every traversed directory makes additions and removals rerun this script;
    // the per-file directives below cover edits to existing inputs.
    println!("cargo:rerun-if-changed={}", directory.display());
    let entries = std::fs::read_dir(directory).unwrap_or_else(|error| {
        panic!(
            "read check implementation source {}: {error}",
            directory.display()
        )
    });
    for entry in entries {
        let path = entry.expect("read source entry").path();
        let relative = path
            .strip_prefix(root)
            .expect("source beneath workspace root");
        if path.is_dir() {
            if !matches!(
                relative
                    .components()
                    .next()
                    .and_then(|part| part.as_os_str().to_str()),
                Some("target" | "binaries")
            ) {
                collect_inputs(root, &path, inputs);
            }
            continue;
        }
        let include = path
            .file_name()
            .is_some_and(|name| name == "Cargo.toml" || name == "Cargo.lock")
            || path.extension().is_some_and(|extension| extension == "rs");
        if include {
            inputs.push(path);
        }
    }
}

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x100000001b3);
    }
}

fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest
        .ancestors()
        .nth(2)
        .expect("cairn-common must remain beneath src-tauri/os")
        .to_path_buf();
    let mut inputs = Vec::new();
    collect_inputs(&workspace, &workspace, &mut inputs);
    inputs.sort();

    let mut hash = 0xcbf29ce484222325_u64;
    for path in inputs {
        println!("cargo:rerun-if-changed={}", path.display());
        let relative = path
            .strip_prefix(&workspace)
            .expect("source beneath workspace");
        hash_bytes(&mut hash, relative.to_string_lossy().as_bytes());
        hash_bytes(
            &mut hash,
            &std::fs::read(&path).unwrap_or_else(|error| {
                panic!(
                    "read check implementation input {}: {error}",
                    path.display()
                )
            }),
        );
    }
    println!("cargo:rustc-env=CAIRN_CHECK_IMPLEMENTATION_ID=source-v1:{hash:016x}");
}
