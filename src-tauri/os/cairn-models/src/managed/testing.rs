//! A managed CLI already in place, for tests that need a CLI that resolves
//! without an install.

use std::path::Path;
use std::sync::Arc;

use super::claude::CLAUDE;
use super::codex::CODEX;
use super::{
    Artifact, ManagedTool, ManagedTools, PinOrigin, RefusingSource, ToolPackage, ToolPin, ToolSpec,
    RELEASE_MARKER,
};

/// A package of `spec`'s shape for `version` whose CLI is the `/bin/sh`
/// script `script` (which must answer `--version` as the tool does): an
/// archive with its manifest for an unpacked tool, the bare script for an
/// executable one. Built for this host's target.
pub fn package_for(spec: &'static ToolSpec, version: &str, script: &str) -> Vec<u8> {
    let body = format!("#!/bin/sh\n{script}\n");
    match spec.artifact {
        Artifact::Executable { .. } => body.into_bytes(),
        Artifact::TarGzTree { manifest } => {
            let target = (spec.host_target)().unwrap_or(spec.supported_targets[0]);
            let entrypoint = format!("bin/{}", spec.name);
            let manifest_text = serde_json::json!({
                "version": version,
                "target": target,
                "entrypoint": entrypoint,
            })
            .to_string();
            let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
                Vec::new(),
                flate2::Compression::fast(),
            ));
            for (path, data, mode) in [
                (entrypoint.as_str(), body.as_bytes(), 0o755),
                (manifest, manifest_text.as_bytes(), 0o644),
            ] {
                let mut header = tar::Header::new_gnu();
                header.set_size(data.len() as u64);
                header.set_mode(mode);
                builder.append_data(&mut header, path, data).unwrap();
            }
            builder.into_inner().unwrap().finish().unwrap()
        }
    }
}

/// A valid pin for `spec` naming `package` (its digest and size) for every
/// target.
pub fn pin_for(spec: &'static ToolSpec, version: &str, package: &[u8]) -> ToolPin {
    use sha2::Digest;
    let sha256 = hex::encode(sha2::Sha256::digest(package));
    ToolPin {
        integration: spec.integration,
        version: version.to_string(),
        tag: (spec.check_tag)(&ToolPin {
            integration: spec.integration,
            version: version.to_string(),
            tag: None,
            reason: String::new(),
            skip: Vec::new(),
            packages: Default::default(),
        })
        .is_err()
        .then(|| format!("rust-v{version}")),
        reason: "test".to_string(),
        skip: Vec::new(),
        packages: spec
            .supported_targets
            .iter()
            .map(|target| {
                (
                    target.to_string(),
                    ToolPackage {
                        sha256: sha256.clone(),
                        size: package.len() as u64,
                    },
                )
            })
            .collect(),
    }
}

/// The managed CLIs with Claude planted under `dir` by
/// [`installed_release`] (running `script`) and Codex this host's test
/// default, which installs nothing.
pub fn tools_with_claude(dir: &Path, script: &str) -> ManagedTools {
    ManagedTools {
        codex: Arc::new(ManagedTool::for_host(&CODEX)),
        claude: Arc::new(installed_release(&CLAUDE, dir, script)),
    }
}

/// Lay out an active release of `spec`'s embedded pin under
/// `dir/tools/<name>`, as an install would leave it, whose entrypoint is the
/// `/bin/sh` script `script`, and return the [`ManagedTool`] that resolves
/// it. The tool refuses every pin fetch and download, so anything but
/// resolving the planted release fails.
pub fn installed_release(spec: &'static ToolSpec, dir: &Path, script: &str) -> ManagedTool {
    let target = (spec.host_target)().unwrap_or(spec.supported_targets[0]);
    let version = spec.embedded().version;
    let root = dir.join("tools").join(spec.name);
    let release = root.join(format!("{version}-{target}"));
    let (manifest, entrypoint) = match spec.artifact {
        Artifact::TarGzTree { manifest } => (manifest, format!("bin/{}", spec.name)),
        Artifact::Executable { file } => (RELEASE_MARKER, file(target).to_string()),
    };
    let path = release.join(&entrypoint);
    std::fs::create_dir_all(path.parent().expect("inside the release")).unwrap();
    std::fs::write(&path, format!("#!/bin/sh\n{script}\n")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    std::fs::write(
        release.join(manifest),
        serde_json::json!({
            "version": version,
            "target": target,
            "entrypoint": entrypoint,
            "sha256": "0".repeat(64),
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        root.join("current.json"),
        serde_json::json!({
            "version": version,
            "target": target,
            "integration": spec.integration,
            "pin_source": PinOrigin::Embedded,
        })
        .to_string(),
    )
    .unwrap();
    ManagedTool::new(
        spec,
        root,
        dir.join("bin"),
        Arc::new(RefusingSource),
        Arc::new(RefusingSource),
    )
    .with_target(Some(target))
}
