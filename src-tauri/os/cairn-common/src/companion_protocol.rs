//! Wire contract for companion endpoints.
//!
//! Companion links are independent from executor links. In particular, this
//! module owns its version and framing, and a companion endpoint is never an
//! executor or an acting principal.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const COMPANION_PROTOCOL_VERSION: u32 = 1;
pub const COMPANION_FRAMING_VERSION: u32 = 1;

// IOS_WIRE_DESCRIPTION_BEGIN
/// Generator-facing description kept with the serde-owned companion contract.
/// Its tests below compare these names with real serialization, so drift fails
/// before another language can generate a parallel dialect.
pub struct CompanionProtocolWireDescription {
    pub enrollment_variants: &'static [&'static str],
    pub client_variants: &'static [&'static str],
    pub server_variants: &'static [&'static str],
    pub envelope_fields: &'static [&'static str],
}

pub const COMPANION_PROTOCOL_WIRE: CompanionProtocolWireDescription =
    CompanionProtocolWireDescription {
        enrollment_variants: &["grant", "credential"],
        client_variants: &["hello", "credentialAcknowledged", "snapshotRequested"],
        server_variants: &[
            "enrollmentAccepted",
            "enrollmentRejected",
            "protocolIncompatible",
            "snapshot",
            "delta",
            "heartbeat",
        ],
        envelope_fields: &["protocolVersion", "framingVersion", "payload"],
    };
// IOS_WIRE_DESCRIPTION_END

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum EnrollmentMaterial {
    Grant { grant: String },
    Credential { credential: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ClientMessage {
    Hello {
        protocol_version: u32,
        runner_binding: String,
        endpoint_id: String,
        device_id: String,
        enrollment: EnrollmentMaterial,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_version: Option<String>,
    },
    CredentialAcknowledged {
        credential: String,
    },
    SnapshotRequested {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_sequence: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ServerMessage {
    EnrollmentAccepted {
        endpoint_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        rotated_credential: Option<String>,
    },
    EnrollmentRejected {
        reason: String,
        #[serde(default)]
        retryable: bool,
    },
    ProtocolIncompatible {
        supported_protocol_version: u32,
    },
    Snapshot {
        sequence: u64,
        projection: Value,
    },
    Delta {
        sequence: u64,
        changes: Value,
    },
    Heartbeat {
        sent_at_unix_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sequence: Option<u64>,
    },
}

/// Encryption-ready frame. Metadata needed to select framing and keys remains
/// visible while the business message is wholly contained by `payload`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompanionEnvelope {
    pub protocol_version: u32,
    pub framing_version: u32,
    pub payload: EnvelopePayload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "encoding",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum EnvelopePayload {
    /// Transitional harness framing. Production business-message handling still
    /// decodes through the envelope rather than inspecting this JSON directly.
    UnsealedJson { message: Value },
    /// Reserved sealed framing; cryptographic processing is intentionally not
    /// specified by protocol v1.
    Sealed {
        algorithm: String,
        key_id: String,
        nonce: String,
        ciphertext: String,
    },
}

#[derive(Debug, Error)]
pub enum EnvelopeError {
    #[error("could not encode companion business message: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("could not decode companion business message: {0}")]
    Decode(#[source] serde_json::Error),
    #[error("sealed companion payload requires a cryptographic decoder")]
    SealedPayload,
}

impl CompanionEnvelope {
    pub fn unsealed<T: Serialize>(message: &T) -> Result<Self, EnvelopeError> {
        Ok(Self {
            protocol_version: COMPANION_PROTOCOL_VERSION,
            framing_version: COMPANION_FRAMING_VERSION,
            payload: EnvelopePayload::UnsealedJson {
                message: serde_json::to_value(message).map_err(EnvelopeError::Encode)?,
            },
        })
    }

    /// Decodes the business payload. Callers must separately enforce supported
    /// protocol and framing versions before acting on the returned message.
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, EnvelopeError> {
        match &self.payload {
            EnvelopePayload::UnsealedJson { message } => {
                serde_json::from_value(message.clone()).map_err(EnvelopeError::Decode)
            }
            EnvelopePayload::Sealed { .. } => Err(EnvelopeError::SealedPayload),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip<T>(value: &T)
    where
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let encoded = serde_json::to_value(value).unwrap();
        assert_eq!(*value, serde_json::from_value(encoded).unwrap());
    }

    #[test]
    fn every_client_variant_round_trips_with_stable_names() {
        let variants = [
            ClientMessage::Hello {
                protocol_version: 1,
                runner_binding: "runner-a".into(),
                endpoint_id: "endpoint-a".into(),
                device_id: "device-a".into(),
                enrollment: EnrollmentMaterial::Grant { grant: "g".into() },
                client_version: Some("test".into()),
            },
            ClientMessage::CredentialAcknowledged {
                credential: "c".into(),
            },
            ClientMessage::SnapshotRequested {
                last_sequence: Some(12),
            },
        ];
        let names = ["hello", "credentialAcknowledged", "snapshotRequested"];
        for (message, expected_name) in variants.iter().zip(names) {
            let encoded = serde_json::to_value(message).unwrap();
            assert_eq!(encoded["type"], expected_name);
            round_trip(message);
        }
    }

    #[test]
    fn every_server_variant_round_trips_with_stable_names() {
        let variants = [
            ServerMessage::EnrollmentAccepted {
                endpoint_id: "e".into(),
                rotated_credential: Some("c".into()),
            },
            ServerMessage::EnrollmentRejected {
                reason: "no".into(),
                retryable: true,
            },
            ServerMessage::ProtocolIncompatible {
                supported_protocol_version: 1,
            },
            ServerMessage::Snapshot {
                sequence: 4,
                projection: json!({"issues": []}),
            },
            ServerMessage::Delta {
                sequence: 5,
                changes: json!({"upsert": []}),
            },
            ServerMessage::Heartbeat {
                sent_at_unix_ms: 8,
                sequence: Some(5),
            },
        ];
        let names = [
            "enrollmentAccepted",
            "enrollmentRejected",
            "protocolIncompatible",
            "snapshot",
            "delta",
            "heartbeat",
        ];
        for (message, expected_name) in variants.iter().zip(names) {
            let encoded = serde_json::to_value(message).unwrap();
            assert_eq!(encoded["type"], expected_name);
            round_trip(message);
        }
    }

    #[test]
    fn additive_unknown_fields_are_ignored_at_every_layer() {
        let mut message = serde_json::to_value(ClientMessage::SnapshotRequested {
            last_sequence: None,
        })
        .unwrap();
        message
            .as_object_mut()
            .unwrap()
            .insert("futureMessageField".into(), json!(true));
        let mut envelope =
            serde_json::to_value(CompanionEnvelope::unsealed(&message).unwrap()).unwrap();
        envelope
            .as_object_mut()
            .unwrap()
            .insert("futureFrameField".into(), json!(true));
        envelope["payload"]
            .as_object_mut()
            .unwrap()
            .insert("futurePayloadField".into(), json!(true));
        let decoded: CompanionEnvelope = serde_json::from_value(envelope).unwrap();
        assert_eq!(
            decoded.decode::<ClientMessage>().unwrap(),
            ClientMessage::SnapshotRequested {
                last_sequence: None
            }
        );
    }

    #[test]
    fn additive_fields_default_and_optional_values_are_omitted() {
        let hello: ClientMessage = serde_json::from_value(json!({
            "type": "hello", "protocolVersion": 1, "runnerBinding": "r",
            "endpointId": "e", "deviceId": "d",
            "enrollment": {"type": "credential", "credential": "c"}
        }))
        .unwrap();
        assert!(matches!(
            hello,
            ClientMessage::Hello {
                client_version: None,
                ..
            }
        ));
        let request: ClientMessage =
            serde_json::from_value(json!({"type": "snapshotRequested"})).unwrap();
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({"type": "snapshotRequested"})
        );
        let rejection: ServerMessage =
            serde_json::from_value(json!({"type":"enrollmentRejected", "reason":"no"})).unwrap();
        assert_eq!(
            rejection,
            ServerMessage::EnrollmentRejected {
                reason: "no".into(),
                retryable: false
            }
        );
    }

    #[test]
    fn enrollment_material_names_are_stable() {
        assert_eq!(
            serde_json::to_value(EnrollmentMaterial::Grant { grant: "g".into() }).unwrap(),
            json!({"type":"grant", "grant":"g"})
        );
        assert_eq!(
            serde_json::to_value(EnrollmentMaterial::Credential {
                credential: "c".into()
            })
            .unwrap(),
            json!({"type":"credential", "credential":"c"})
        );
    }

    #[test]
    fn envelope_frames_and_decodes_business_messages() {
        let message = ClientMessage::SnapshotRequested {
            last_sequence: Some(41),
        };
        let envelope = CompanionEnvelope::unsealed(&message).unwrap();
        assert_eq!(envelope.protocol_version, COMPANION_PROTOCOL_VERSION);
        assert_eq!(envelope.framing_version, COMPANION_FRAMING_VERSION);
        assert_eq!(
            serde_json::to_value(&envelope).unwrap()["payload"]["encoding"],
            "unsealedJson"
        );
        assert_eq!(envelope.decode::<ClientMessage>().unwrap(), message);
    }

    #[test]
    fn sealed_payload_cannot_be_mistaken_for_plaintext() {
        let envelope = CompanionEnvelope {
            protocol_version: 1,
            framing_version: 1,
            payload: EnvelopePayload::Sealed {
                algorithm: "reserved".into(),
                key_id: "k".into(),
                nonce: "n".into(),
                ciphertext: "c".into(),
            },
        };
        assert!(matches!(
            envelope.decode::<ClientMessage>(),
            Err(EnvelopeError::SealedPayload)
        ));
        assert_eq!(
            serde_json::to_value(envelope).unwrap()["payload"]["encoding"],
            "sealed"
        );
    }

    #[test]
    fn protocol_version_is_a_module_local_contract() {
        let source = include_str!("companion_protocol.rs");
        assert!(source.contains("pub const COMPANION_PROTOCOL_VERSION: u32 = 1"));
        let forbidden = [
            ["executor", "_protocol"].concat(),
            ["EXECUTOR", "_PROTOCOL_VERSION"].concat(),
        ];
        for name in forbidden {
            assert!(
                !source.contains(&name),
                "companion protocol referenced {name}"
            );
        }
    }
}
