use super::VoiceModel;

/// Immutable upstream model identity. The digest is Hugging Face's LFS SHA-256,
/// not mutable repository metadata fetched during installation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelArtifact {
    pub model: VoiceModel,
    pub file_name: &'static str,
    pub url: &'static str,
    pub size_bytes: u64,
    pub sha256: &'static str,
}

pub const FAST_MODEL: ModelArtifact = ModelArtifact {
    model: VoiceModel::Fast,
    file_name: "ggml-base-q5_1.bin",
    url: "https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-base-q5_1.bin",
    size_bytes: 59_707_625,
    sha256: "422f1ae452ade6f30a004d7e5c6a43195e4433bc370bf23fac9cc591f01a8898",
};

pub const ACCURATE_MODEL: ModelArtifact = ModelArtifact {
    model: VoiceModel::Accurate,
    file_name: "ggml-large-v3-turbo-q5_0.bin",
    url: "https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-large-v3-turbo-q5_0.bin",
    size_bytes: 574_041_195,
    sha256: "394221709cd5ad1f40c46e6031ca61bce88931e6e088c188294c6d5a55ffa7e2",
};

/// NVIDIA Nemotron 3.5 ASR Streaming 0.6B, converted for parakeet.cpp. Pinned to
/// a repository revision rather than `main`, so the URL cannot start serving
/// other bytes; the digest would refuse them anyway.
pub const REALTIME_MODEL: ModelArtifact = ModelArtifact {
    model: VoiceModel::Realtime,
    file_name: "nemotron-3.5-asr-streaming-0.6b-q8_0.gguf",
    url: "https://huggingface.co/mudler/parakeet-cpp-gguf/resolve/9e421aa66101085dd3dabe9a0c621568ca7c5a3f/nemotron-3.5-asr-streaming-0.6b-q8_0.gguf",
    size_bytes: 983_696_512,
    sha256: "ba2f13eccd4a5245be728f77e6149bd6a4fdcdd133ff2e08ac6005bcef7a99f1",
};

pub fn model_catalog() -> &'static [ModelArtifact; 3] {
    &[FAST_MODEL, ACCURATE_MODEL, REALTIME_MODEL]
}

pub fn current_target() -> Option<&'static str> {
    if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        Some("aarch64-apple-darwin")
    } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        Some("x86_64-unknown-linux-gnu")
    } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
        Some("x86_64-pc-windows-msvc")
    } else {
        None
    }
}

pub fn engine_asset_name(version: &str, target: &str) -> String {
    let suffix = if target == "x86_64-pc-windows-msvc" {
        ".exe"
    } else {
        ""
    };
    format!("cairn-voice-v{version}-{target}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_asset_names_are_exact_and_versioned() {
        assert_eq!(
            engine_asset_name("1.2.3", "aarch64-apple-darwin"),
            "cairn-voice-v1.2.3-aarch64-apple-darwin"
        );
        assert_eq!(
            engine_asset_name("1.2.3", "x86_64-pc-windows-msvc"),
            "cairn-voice-v1.2.3-x86_64-pc-windows-msvc.exe"
        );
    }

    #[test]
    fn catalog_pins_sizes_and_sha256_digests() {
        for artifact in model_catalog() {
            assert!(artifact.size_bytes > 0);
            assert_eq!(artifact.sha256.len(), 64);
            assert!(artifact.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
    }

    /// Exactly these three, each under its own model. Adding, dropping, or
    /// re-pointing one is a deliberate edit to this list, not a side effect.
    #[test]
    fn the_catalog_is_exactly_the_three_pinned_models() {
        let entries: Vec<_> = model_catalog()
            .iter()
            .map(|artifact| (artifact.model, artifact.file_name, artifact.size_bytes))
            .collect();
        assert_eq!(
            entries,
            vec![
                (VoiceModel::Fast, "ggml-base-q5_1.bin", 59_707_625),
                (
                    VoiceModel::Accurate,
                    "ggml-large-v3-turbo-q5_0.bin",
                    574_041_195
                ),
                (
                    VoiceModel::Realtime,
                    "nemotron-3.5-asr-streaming-0.6b-q8_0.gguf",
                    983_696_512
                ),
            ]
        );
    }

    #[test]
    fn the_realtime_model_is_fetched_from_a_pinned_revision() {
        assert!(
            !REALTIME_MODEL.url.contains("/resolve/main/"),
            "a mutable branch URL: {}",
            REALTIME_MODEL.url
        );
        assert!(REALTIME_MODEL.url.ends_with(REALTIME_MODEL.file_name));
    }
}
