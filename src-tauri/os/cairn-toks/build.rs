//! Links toks from its pinned prebuilt release bundle.
//!
//! toks builds from source only with clang 21 and hand-written asm, so this
//! links the release's static library instead. A bundle exists for three
//! targets; on each the archive's sha256 is the trust boundary, checked on
//! every path the bytes can arrive by:
//!
//! 1. `TOKS_BUNDLE_DIR`, a directory holding the release archive under its
//!    upstream name (for offline machines and air-gapped builds);
//! 2. the host's content-addressed asset cache, `$CAIRN_ASSET_CACHE_DIR` or
//!    `~/.cache/cairn-assets`, keyed `<sha256>-<asset>` exactly as
//!    `scripts/lib/pinned-download.ts` keys it, so every build slot on one
//!    machine shares one download;
//! 3. a download from the GitHub release, which then fills the cache.
//!
//! A digest mismatch, or a machine that can reach none of the three, fails
//! the build with a message naming the override. Every other target gets no
//! link and no `cfg(toks_native)`, and the crate compiles its stub.
//!
//! Upgrading toks is deliberate: new digests here, then the fleet test
//! (`tests/fleet.rs`) rerun against every pinned tokenizer. Releases before
//! 0.3.2 were BUSL-licensed and must never be pinned.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};

const VERSION: &str = "0.3.2";

struct Bundle {
    platform: &'static str,
    sha256: &'static str,
}

const MACOS_ARM64: Bundle = Bundle {
    platform: "macos-arm64",
    sha256: "410f4981ba76a1b473640d0fcd9bb63b457b841abf4c0f2027826badc61fe82b",
};
const LINUX_X86_64: Bundle = Bundle {
    platform: "linux-x86_64",
    sha256: "e0780282bd8caabcee355e12404d2d3ead3ed5a52442fb73eb88ee2ba4b23b76",
};
const LINUX_ARM64: Bundle = Bundle {
    platform: "linux-arm64",
    sha256: "d273392667d6d80f66cf61a61e3129a7dfca16176b0be990752a0d08e4fd2fb8",
};

fn main() {
    println!("cargo:rustc-check-cfg=cfg(toks_native)");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=TOKS_BUNDLE_DIR");
    println!("cargo:rerun-if-env-changed=CAIRN_ASSET_CACHE_DIR");

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let Some(bundle) = bundle_for(
        &target_os,
        &env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default(),
        &env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default(),
    ) else {
        return;
    };

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("cargo sets OUT_DIR"));
    let root = prepare(&bundle, &out_dir);
    println!(
        "cargo:rustc-link-search=native={}",
        root.join("lib").display()
    );
    println!("cargo:rustc-link-lib=static=toks");
    if target_os == "linux" {
        // toks_par's worker pool; the core itself calls no thread function.
        println!("cargo:rustc-link-lib=pthread");
    }
    // `links` metadata: DEP_TOKS_ROOT for any dependent that needs the
    // bundle's LICENSE and NOTICE, which ship with every copy.
    println!("cargo:root={}", root.display());
    println!("cargo:rustc-cfg=toks_native");
}

fn bundle_for(os: &str, arch: &str, target_env: &str) -> Option<Bundle> {
    match (os, arch, target_env) {
        ("macos", "aarch64", _) => Some(MACOS_ARM64),
        ("linux", "x86_64", "gnu") => Some(LINUX_X86_64),
        ("linux", "aarch64", "gnu") => Some(LINUX_ARM64),
        _ => None,
    }
}

fn asset_name(bundle: &Bundle) -> String {
    format!("toks-{VERSION}-{}.tar.gz", bundle.platform)
}

fn url(bundle: &Bundle) -> String {
    format!(
        "https://github.com/actual-computer/toks/releases/download/v{VERSION}/{}",
        asset_name(bundle)
    )
}

/// The extracted bundle directory, verified. Extraction is skipped when a
/// previous build of this OUT_DIR already verified the same digest.
fn prepare(bundle: &Bundle, out_dir: &Path) -> PathBuf {
    let root = out_dir.join(format!("toks-{VERSION}-{}", bundle.platform));
    let stamp = out_dir.join("bundle.verified");
    if fs::read_to_string(&stamp).ok().as_deref() == Some(bundle.sha256)
        && root.join("lib/libtoks.a").is_file()
    {
        return root;
    }

    let bytes = obtain(bundle, out_dir);
    let archive = out_dir.join(asset_name(bundle));
    fs::write(&archive, &bytes).expect("stage the toks bundle");
    let _ = fs::remove_dir_all(&root);
    let status = Command::new("tar")
        .arg("-xzf")
        .arg(&archive)
        .arg("-C")
        .arg(out_dir)
        .status()
        .unwrap_or_else(|error| {
            panic!("cairn-toks: could not run tar to extract the toks bundle: {error}")
        });
    assert!(
        status.success(),
        "cairn-toks: tar failed extracting {}",
        archive.display()
    );

    // The archive digest already covers these bytes; checking them against
    // the bundle's own MANIFEST catches an extraction that went wrong.
    let manifest = fs::read_to_string(root.join("MANIFEST"))
        .expect("cairn-toks: the toks bundle carries a MANIFEST");
    for path in ["lib/libtoks.a", "include/toks.h", "LICENSE", "NOTICE"] {
        let expected = manifest
            .lines()
            .filter_map(|line| {
                let mut fields = line.split_whitespace();
                (fields.next() == Some("sha256")).then(|| (fields.next(), fields.next()))
            })
            .find_map(|(digest, named)| (named == Some(path)).then_some(digest).flatten())
            .unwrap_or_else(|| panic!("cairn-toks: the toks MANIFEST names no {path}"));
        let actual = sha256_hex(&fs::read(root.join(path)).expect("read an extracted bundle file"));
        assert_eq!(
            actual, expected,
            "cairn-toks: extracted {path} does not match the toks bundle MANIFEST"
        );
    }
    fs::write(&stamp, bundle.sha256).expect("stamp the verified toks bundle");
    root
}

fn obtain(bundle: &Bundle, out_dir: &Path) -> Vec<u8> {
    let asset = asset_name(bundle);
    if let Some(dir) = env::var_os("TOKS_BUNDLE_DIR") {
        let path = PathBuf::from(dir).join(&asset);
        let bytes = fs::read(&path).unwrap_or_else(|error| {
            panic!(
                "cairn-toks: TOKS_BUNDLE_DIR is set but {} is unreadable: {error}",
                path.display()
            )
        });
        verify(bundle, &bytes, &path.display().to_string());
        return bytes;
    }

    let cache_path = cache_dir().map(|dir| dir.join(format!("{}-{asset}", bundle.sha256)));
    if let Some(path) = &cache_path {
        if let Ok(bytes) = fs::read(path) {
            if sha256_hex(&bytes) == bundle.sha256 {
                return bytes;
            }
            // A corrupt entry is discarded rather than trusted.
            let _ = fs::remove_file(path);
        }
    }

    let partial = out_dir.join(format!("{asset}.download"));
    let downloaded = Command::new("curl")
        .args(["-fsSL", "--retry", "3", "-o"])
        .arg(&partial)
        .arg(url(bundle))
        .status();
    let bytes = match downloaded {
        Ok(status) if status.success() => fs::read(&partial).ok(),
        _ => None,
    }
    .unwrap_or_else(|| fail_offline(bundle, cache_path.as_deref()));
    let _ = fs::remove_file(&partial);
    verify(bundle, &bytes, &url(bundle));

    if let Some(path) = &cache_path {
        // A cache that cannot be written is not a build failure.
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let staging = path.with_extension(format!("{}.partial", std::process::id()));
        if fs::write(&staging, &bytes).is_ok() {
            let _ = fs::rename(&staging, path);
        }
    }
    bytes
}

fn cache_dir() -> Option<PathBuf> {
    if let Some(dir) = env::var_os("CAIRN_ASSET_CACHE_DIR") {
        return Some(PathBuf::from(dir));
    }
    env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .map(|home| PathBuf::from(home).join(".cache").join("cairn-assets"))
}

fn verify(bundle: &Bundle, bytes: &[u8], source: &str) {
    let actual = sha256_hex(bytes);
    assert!(
        actual == bundle.sha256,
        "cairn-toks: the toks {VERSION} bundle from {source} has sha256 {actual}, but the pinned \
         digest is {}. Refusing to link it.",
        bundle.sha256
    );
}

fn fail_offline(bundle: &Bundle, cache_path: Option<&Path>) -> ! {
    let cache = cache_path
        .map(|path| format!(" or place it in the asset cache as {}", path.display()))
        .unwrap_or_default();
    panic!(
        "cairn-toks: could not download the pinned toks {VERSION} bundle from {} \
         (offline, or curl is missing). Download it by hand (sha256 {}) and set \
         TOKS_BUNDLE_DIR to the directory holding {}{cache}.",
        url(bundle),
        bundle.sha256,
        asset_name(bundle),
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
