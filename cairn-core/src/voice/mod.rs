//! Desktop voice domain contract.
//!
//! This module owns voice intent and the immutable component catalog. Runtime
//! state is derived from verified managed files; it is never persisted as fact
//! in `settings.yaml`.

mod catalog;
mod service;

pub use service::{VoiceService, VoiceServiceHandle};
mod installer;
mod supervisor;

pub use catalog::{current_target, engine_asset_name, model_catalog, ModelArtifact};
pub use installer::{ComponentInstaller, InstallEvent, ManagedManifest};
pub use supervisor::{ProtocolCommand, ProtocolEvent, VoiceProcess};

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const PROTOCOL_VERSION: u32 = 1;
pub const SAMPLE_RATE: u32 = 16_000;
pub const CHANNELS: u32 = 1;
pub const CHUNK_ENCODING: &str = "f32le-base64";
pub const MAX_AUDIO_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VoiceModel {
    #[default]
    Fast,
    Accurate,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DictationMode {
    #[default]
    Hold,
    Toggle,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoicePreferences {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub preferred_file_model: VoiceModel,
    #[serde(default)]
    pub dictation_mode: DictationMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentState {
    Missing,
    Downloading,
    Installed,
    Corrupt,
    Incompatible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineState {
    Stopped,
    Starting,
    Ready,
    Busy,
    Restarting,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoiceStatus {
    pub supported: bool,
    pub enabled: bool,
    pub engine: ComponentState,
    pub engine_state: EngineState,
    pub fast_model: ComponentState,
    pub accurate_model: ComponentState,
    pub preferred_file_model: VoiceModel,
    pub dictation_mode: DictationMode,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    pub index: u32,
    pub start_ms: i64,
    pub end_ms: i64,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transcript {
    pub text: String,
    pub segments: Vec<Segment>,
    pub audio_duration_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamPartial {
    pub stream_id: String,
    pub revision: u64,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub text: String,
}

/// Every event the webview receives on the `voice-event` channel.
///
/// `rename_all` renames the variants; the fields inside them need
/// `rename_all_fields`, and without it a variant's payload arrives as
/// `operation_id` while the webview reads `operationId` — an event that is
/// delivered, parsed, and silently ignored. `Partial` escapes that because its
/// payload is a struct carrying its own attribute.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "type"
)]
pub enum VoiceEvent {
    DownloadStarted {
        operation_id: String,
        component: String,
        total_bytes: Option<u64>,
    },
    DownloadProgress {
        operation_id: String,
        component: String,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
    },
    DownloadFinished {
        operation_id: String,
        component: String,
    },
    EngineChanged {
        state: EngineState,
        detail: Option<String>,
    },
    TranscriptionProgress {
        request_id: String,
        stage: String,
        progress: f32,
    },
    Segment {
        request_id: String,
        segment: Segment,
    },
    Partial(StreamPartial),
    StreamTerminated {
        stream_id: String,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedComponent {
    pub kind: String,
    pub id: String,
    pub target: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
    pub path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum VoiceError {
    #[error("voice is not supported on this target")]
    UnsupportedTarget,
    #[error("voice component is not installed")]
    NotInstalled,
    #[error("voice component failed verification: {0}")]
    Corrupt(String),
    #[error("voice engine is incompatible: {0}; remove and re-download voice files")]
    Incompatible(String),
    #[error("voice engine is busy")]
    Busy,
    #[error("voice command queue is full; retry after pending audio is processed")]
    Backpressure,
    #[error("invalid voice input: {0}")]
    InvalidInput(String),
    #[error("voice request failed ({code}): {message}")]
    Request { code: String, message: String },
    #[error("voice transport failed: {0}")]
    Transport(String),
    #[error("voice installation failed: {0}")]
    Install(String),
}

pub type VoiceResult<T> = Result<T, VoiceError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preferences_default_to_disabled_fast_model_and_hold_dictation() {
        assert_eq!(
            VoicePreferences::default(),
            VoicePreferences {
                enabled: false,
                preferred_file_model: VoiceModel::Fast,
                dictation_mode: DictationMode::Hold,
            }
        );
    }

    /// The webview reduces download events by name and by camelCase field, and
    /// correlates one download through `operationId`. Renaming any of them stops
    /// the progress bar silently rather than failing a build.
    #[test]
    fn download_events_serialize_with_the_names_the_webview_reduces() {
        let started = serde_json::to_value(VoiceEvent::DownloadStarted {
            operation_id: "op-1".into(),
            component: "engine".into(),
            total_bytes: Some(9),
        })
        .unwrap();
        assert_eq!(started["type"], "downloadStarted");
        assert_eq!(started["operationId"], "op-1");
        assert_eq!(started["totalBytes"], 9);

        let progress = serde_json::to_value(VoiceEvent::DownloadProgress {
            operation_id: "op-1".into(),
            component: "model:fast".into(),
            downloaded_bytes: 4,
            total_bytes: Some(9),
        })
        .unwrap();
        assert_eq!(progress["type"], "downloadProgress");
        assert_eq!(progress["component"], "model:fast");
        assert_eq!(progress["downloadedBytes"], 4);

        let finished = serde_json::to_value(VoiceEvent::DownloadFinished {
            operation_id: "op-1".into(),
            component: "model:fast".into(),
        })
        .unwrap();
        assert_eq!(finished["type"], "downloadFinished");
        assert_eq!(finished["operationId"], "op-1");
    }

    /// Dictation matches a live session by `streamId`; a snake_case field here
    /// means a terminated stream is never noticed and the UI keeps listening.
    #[test]
    fn stream_events_serialize_with_the_names_dictation_matches_on() {
        let terminated = serde_json::to_value(VoiceEvent::StreamTerminated {
            stream_id: "s-1".into(),
            reason: "engine exited".into(),
        })
        .unwrap();
        assert_eq!(terminated["type"], "streamTerminated");
        assert_eq!(terminated["streamId"], "s-1");

        let partial = serde_json::to_value(VoiceEvent::Partial(StreamPartial {
            stream_id: "s-1".into(),
            revision: 2,
            window_start_ms: 0,
            window_end_ms: 500,
            text: "hello".into(),
        }))
        .unwrap();
        assert_eq!(partial["type"], "partial");
        assert_eq!(partial["streamId"], "s-1");
        assert_eq!(partial["windowEndMs"], 500);
    }

    #[test]
    fn legacy_preferences_default_dictation_mode_to_hold() {
        let preferences: VoicePreferences =
            serde_yaml::from_str("enabled: true\npreferredFileModel: accurate\n").unwrap();
        assert_eq!(preferences.dictation_mode, DictationMode::Hold);
    }
}
