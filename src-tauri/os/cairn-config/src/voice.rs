//! Persisted voice intent.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VoiceModel {
    #[default]
    Fast,
    Accurate,
    /// A streaming transducer (Nemotron): punctuated text that only ever
    /// appends as you speak. Opt-in; Fast stays the default.
    Realtime,
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
    /// The model live dictation opens a stream with. Defaults to `Fast`, which
    /// is the only one that keeps up with a speaker on typical hardware, and
    /// `serde(default)` so a settings file written before this existed still
    /// loads.
    #[serde(default)]
    pub preferred_dictation_model: VoiceModel,
    #[serde(default)]
    pub dictation_mode: DictationMode,
    /// The microphone dictation opens, named by the `deviceId` the webview's
    /// `enumerateDevices` reports. `None` means the system default, which is
    /// also what a device that has since been unplugged falls back to: a
    /// remembered id is a preference, never a requirement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_device_id: Option<String>,
}

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
                preferred_dictation_model: VoiceModel::Fast,
                dictation_mode: DictationMode::Hold,
                input_device_id: None,
            }
        );
    }
    #[test]
    fn legacy_preferences_default_dictation_mode_to_hold() {
        let preferences: VoicePreferences =
            serde_yaml::from_str("enabled: true\npreferredFileModel: accurate\n").unwrap();
        assert_eq!(preferences.dictation_mode, DictationMode::Hold);
        assert_eq!(preferences.input_device_id, None);
    }
    /// A settings file written before dictation had its own model must keep the
    /// behaviour it had: Fast for live speech, whatever was chosen for files.
    #[test]
    fn a_settings_file_without_a_dictation_model_still_dictates_with_fast() {
        let preferences: VoicePreferences =
            serde_yaml::from_str("enabled: true\npreferredFileModel: accurate\n").unwrap();
        assert_eq!(preferences.preferred_file_model, VoiceModel::Accurate);
        assert_eq!(preferences.preferred_dictation_model, VoiceModel::Fast);
    }
    /// The two choices are independent, and both survive a round trip.
    #[test]
    fn dictation_and_file_models_are_remembered_separately() {
        let split = VoicePreferences {
            enabled: true,
            preferred_file_model: VoiceModel::Accurate,
            preferred_dictation_model: VoiceModel::Fast,
            ..VoicePreferences::default()
        };
        let yaml = serde_yaml::to_string(&split).unwrap();
        assert!(yaml.contains("preferredDictationModel: fast"), "{yaml}");
        assert_eq!(
            serde_yaml::from_str::<VoicePreferences>(&yaml).unwrap(),
            split
        );
    }
    /// A Realtime choice persists under its own name, and a settings file that
    /// never heard of it still loads with Fast for both purposes.
    #[test]
    fn realtime_round_trips_without_moving_the_defaults() {
        let realtime = VoicePreferences {
            enabled: true,
            preferred_dictation_model: VoiceModel::Realtime,
            ..VoicePreferences::default()
        };
        let yaml = serde_yaml::to_string(&realtime).unwrap();
        assert!(yaml.contains("preferredDictationModel: realtime"), "{yaml}");
        assert_eq!(
            serde_yaml::from_str::<VoicePreferences>(&yaml).unwrap(),
            realtime
        );
        assert_eq!(
            VoicePreferences::default().preferred_dictation_model,
            VoiceModel::Fast
        );
        assert_eq!(
            VoicePreferences::default().preferred_file_model,
            VoiceModel::Fast
        );
    }
    /// The chosen microphone round-trips under the name the webview uses, and
    /// the system default is written as absence rather than as a sentinel id.
    #[test]
    fn an_input_device_round_trips_and_the_default_is_absence() {
        let chosen = VoicePreferences {
            enabled: true,
            input_device_id: Some("a1b2c3".into()),
            ..VoicePreferences::default()
        };
        let yaml = serde_yaml::to_string(&chosen).unwrap();
        assert!(yaml.contains("inputDeviceId: a1b2c3"), "{yaml}");
        assert_eq!(
            serde_yaml::from_str::<VoicePreferences>(&yaml).unwrap(),
            chosen
        );

        let default_device = serde_yaml::to_string(&VoicePreferences::default()).unwrap();
        assert!(
            !default_device.contains("inputDeviceId"),
            "{default_device}"
        );
    }
}
