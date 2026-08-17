//! Validated identity references and immutable evidence of an actor's appearance.

pub mod display;

use crate::uri::{parse_uri, CairnResource};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use thiserror::Error;

const MAX_TEXT_LEN: usize = 512;
const MAX_CREDENTIAL_REF_LEN: usize = 256;
pub const MAX_DELEGATION_HOPS: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdentityValidationError {
    #[error("{field} must be nonblank")]
    Blank { field: &'static str },
    #[error("{field} exceeds its maximum length")]
    TooLong { field: &'static str },
    #[error("{field} contains non-printable metadata")]
    NonPrintable { field: &'static str },
    #[error("agent in {position:?} must carry a run id")]
    RunIdRequired { position: PrincipalPosition },
    #[error("node is not a canonical Cairn node URI")]
    InvalidNodeUri,
    #[error("timestamp {field} must be nonnegative")]
    NegativeTimestamp { field: &'static str },
    #[error("sequence must be nonnegative")]
    NegativeSequence,
    #[error("credential reference resembles credential material or an oracle label")]
    UnsafeCredentialRef,
    #[error("verification metadata is inconsistent with its method or status")]
    InvalidVerification,
    #[error("delegation exceeds {MAX_DELEGATION_HOPS} hops")]
    DelegationTooDeep,
    #[error("delegation chain is discontinuous")]
    DelegationDiscontinuous,
    #[error("delegation chain repeats a principal")]
    DelegationCycle,
    #[error("delegation terminal is missing or unexpected")]
    InvalidDelegationTerminal,
}
#[derive(Deserialize)]
struct DelegationHopWire {
    acting: PrincipalRef,
    represented: PrincipalRef,
    evidence: AppearanceEvidence,
    asserted_by: PrincipalRef,
    asserted_at: i64,
}
impl TryFrom<DelegationHopWire> for DelegationHop {
    type Error = IdentityValidationError;
    fn try_from(v: DelegationHopWire) -> Result<Self, Self::Error> {
        v.acting.validate_at(PrincipalPosition::DelegationActing)?;
        v.represented
            .validate_at(PrincipalPosition::DurableSubject)?;
        v.asserted_by
            .validate_at(PrincipalPosition::DelegationAsserting)?;
        if v.asserted_at < 0 {
            return Err(IdentityValidationError::NegativeTimestamp {
                field: "asserted_at",
            });
        }
        Ok(Self {
            acting: v.acting,
            represented: v.represented,
            evidence: v.evidence,
            asserted_by: v.asserted_by,
            asserted_at: v.asserted_at,
        })
    }
}
#[derive(Deserialize)]
struct AppearanceEvidenceWire {
    transport: AppearanceTransport,
    address: Address,
    verification: VerificationRecord,
    at: i64,
    sequence: Option<DurableSequence>,
}
impl TryFrom<AppearanceEvidenceWire> for AppearanceEvidence {
    type Error = IdentityValidationError;
    fn try_from(v: AppearanceEvidenceWire) -> Result<Self, Self::Error> {
        Self::new(v.transport, v.address, v.verification, v.at, v.sequence)
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum AddressWire {
    Channel {
        provider: String,
        conversation: String,
        sender: String,
        observed_alias: Option<String>,
    },
    Desktop {
        device_id: String,
    },
    Resource {
        node: String,
    },
    Invoke {
        origin: Option<String>,
    },
    None,
}
impl TryFrom<AddressWire> for Address {
    type Error = IdentityValidationError;
    fn try_from(value: AddressWire) -> Result<Self, Self::Error> {
        let address = match value {
            AddressWire::Channel {
                provider,
                conversation,
                sender,
                observed_alias,
            } => Self::Channel {
                provider,
                conversation,
                sender,
                observed_alias,
            },
            AddressWire::Desktop { device_id } => Self::Desktop { device_id },
            AddressWire::Resource { node } => Self::Resource { node },
            AddressWire::Invoke { origin } => Self::Invoke { origin },
            AddressWire::None => Self::None,
        };
        address.validate()?;
        Ok(address)
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum PrincipalRefWire {
    Human {
        issuer: String,
        subject: String,
        organization: Option<String>,
    },
    Agent {
        node: String,
        run_id: Option<String>,
    },
    Machine {
        device_id: String,
    },
    External {
        provider: String,
        namespace: String,
        id: String,
    },
}
impl TryFrom<PrincipalRefWire> for PrincipalRef {
    type Error = IdentityValidationError;
    fn try_from(value: PrincipalRefWire) -> Result<Self, Self::Error> {
        let principal = match value {
            PrincipalRefWire::Human {
                issuer,
                subject,
                organization,
            } => Self::Human {
                issuer,
                subject,
                organization,
            },
            PrincipalRefWire::Agent { node, run_id } => Self::Agent { node, run_id },
            PrincipalRefWire::Machine { device_id } => Self::Machine { device_id },
            PrincipalRefWire::External {
                provider,
                namespace,
                id,
            } => Self::External {
                provider,
                namespace,
                id,
            },
        };
        principal.validate_at(PrincipalPosition::DurableSubject)?;
        Ok(principal)
    }
}

fn metadata(value: &str, field: &'static str) -> Result<(), IdentityValidationError> {
    if value.trim().is_empty() {
        return Err(IdentityValidationError::Blank { field });
    }
    if value.len() > MAX_TEXT_LEN {
        return Err(IdentityValidationError::TooLong { field });
    }
    if !value.chars().all(|c| !c.is_control()) {
        return Err(IdentityValidationError::NonPrintable { field });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrincipalPosition {
    DurableSubject,
    DecisionActor,
    AppearancePrincipal,
    DelegationActing,
    DelegationRepresented,
    DelegationTerminal,
    DelegationAsserting,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    deny_unknown_fields,
    try_from = "PrincipalRefWire"
)]
pub enum PrincipalRef {
    Human {
        issuer: String,
        subject: String,
        organization: Option<String>,
    },
    Agent {
        node: String,
        run_id: Option<String>,
    },
    Machine {
        device_id: String,
    },
    External {
        provider: String,
        namespace: String,
        id: String,
    },
}

fn is_canonical_agent_home(node: &str) -> bool {
    match parse_uri(node) {
        Some(CairnResource::Node { .. } | CairnResource::Task { .. }) => true,
        Some(CairnResource::Thread { path, .. }) => {
            path.is_empty()
                || matches!(path.as_slice(), [task, name] if task == "task" && !name.is_empty())
        }
        _ => false,
    }
}

impl PrincipalRef {
    pub fn validate_at(&self, position: PrincipalPosition) -> Result<(), IdentityValidationError> {
        match self {
            Self::Human {
                issuer,
                subject,
                organization,
            } => {
                metadata(issuer, "issuer")?;
                metadata(subject, "subject")?;
                if let Some(v) = organization {
                    metadata(v, "organization")?;
                }
            }
            Self::Agent { node, run_id } => {
                metadata(node, "node")?;
                if !is_canonical_agent_home(node) {
                    return Err(IdentityValidationError::InvalidNodeUri);
                }
                if let Some(v) = run_id {
                    metadata(v, "run_id")?;
                }
                if run_id.is_none()
                    && !matches!(
                        position,
                        PrincipalPosition::DurableSubject | PrincipalPosition::DelegationTerminal
                    )
                {
                    return Err(IdentityValidationError::RunIdRequired { position });
                }
            }
            Self::Machine { device_id } => metadata(device_id, "device_id")?,
            Self::External {
                provider,
                namespace,
                id,
            } => {
                metadata(provider, "provider")?;
                metadata(namespace, "namespace")?;
                metadata(id, "id")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppearanceTransport {
    ResourcePatch,
    ChannelReply,
    RemoteIntent,
    NonOperatorInvoke,
    LocalInvoke,
    AuthenticatedOperator,
    AuthenticatedDesktop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    deny_unknown_fields,
    try_from = "AddressWire"
)]
pub enum Address {
    Channel {
        provider: String,
        conversation: String,
        sender: String,
        observed_alias: Option<String>,
    },
    Desktop {
        device_id: String,
    },
    Resource {
        node: String,
    },
    Invoke {
        origin: Option<String>,
    },
    None,
}

impl Address {
    pub fn validate(&self) -> Result<(), IdentityValidationError> {
        match self {
            Self::Channel {
                provider,
                conversation,
                sender,
                observed_alias,
            } => {
                metadata(provider, "provider")?;
                metadata(conversation, "conversation")?;
                metadata(sender, "sender")?;
                if let Some(v) = observed_alias {
                    metadata(v, "observed_alias")?;
                }
            }
            Self::Desktop { device_id } => metadata(device_id, "device_id")?,
            Self::Resource { node } => {
                metadata(node, "node")?;
                if !is_canonical_agent_home(node) {
                    return Err(IdentityValidationError::InvalidNodeUri);
                }
            }
            Self::Invoke { origin } => {
                if let Some(v) = origin {
                    metadata(v, "origin")?;
                }
            }
            Self::None => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationMethod {
    JwtOperator,
    DesktopCredential,
    ChannelAllowlist,
    NodeSession,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    Verified,
    None,
}

/// Descriptive evidence, deliberately not an ordered privilege scale.
///
/// ```compile_fail
/// use cairn_common::identity::VerificationStrength;
/// let weak = VerificationStrength::new("weak").unwrap();
/// let strong = VerificationStrength::new("strong").unwrap();
/// let _ = weak < strong;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct VerificationStrength(String);

impl VerificationStrength {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityValidationError> {
        let value = value.into();
        metadata(&value, "strength")?;
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for VerificationStrength {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct CredentialRef(String);

impl CredentialRef {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityValidationError> {
        let value = value.into();
        metadata(&value, "credential_ref")?;
        if value.len() > MAX_CREDENTIAL_REF_LEN {
            return Err(IdentityValidationError::TooLong {
                field: "credential_ref",
            });
        }
        let lower = value.to_ascii_lowercase();
        let secret_markers = [
            "bearer ",
            "token=",
            "token:",
            "secret=",
            "secret:",
            "password=",
            "password:",
            "private_key",
        ];
        let oracle_markers = ["fingerprint", "sha1:", "sha256:", "sha512:", "hash:"];
        let long_hex = value
            .split(|c: char| !c.is_ascii_hexdigit())
            .any(|part| part.len() >= 32);
        let jwt_shaped = value.split('.').count() == 3 && value.len() >= 32;
        if secret_markers.iter().any(|m| lower.contains(m))
            || oracle_markers.iter().any(|m| lower.contains(m))
            || long_hex
            || jwt_shaped
        {
            return Err(IdentityValidationError::UnsafeCredentialRef);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for CredentialRef {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "DurableSequenceWire")]
pub struct DurableSequence {
    seq_kind: String,
    seq: i64,
}

#[derive(Deserialize)]
struct DurableSequenceWire {
    seq_kind: String,
    seq: i64,
}
impl DurableSequence {
    pub fn new(seq_kind: impl Into<String>, seq: i64) -> Result<Self, IdentityValidationError> {
        let seq_kind = seq_kind.into();
        metadata(&seq_kind, "seq_kind")?;
        if seq < 0 {
            return Err(IdentityValidationError::NegativeSequence);
        }
        Ok(Self { seq_kind, seq })
    }
    pub fn seq_kind(&self) -> &str {
        &self.seq_kind
    }
    pub fn seq(&self) -> i64 {
        self.seq
    }
}
impl TryFrom<DurableSequenceWire> for DurableSequence {
    type Error = IdentityValidationError;
    fn try_from(v: DurableSequenceWire) -> Result<Self, Self::Error> {
        Self::new(v.seq_kind, v.seq)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "VerificationRecordWire")]
pub struct VerificationRecord {
    method: VerificationMethod,
    status: VerificationStatus,
    issuer: Option<String>,
    subject: Option<String>,
    session: Option<String>,
    credential_ref: Option<CredentialRef>,
    strength: VerificationStrength,
    verified_at: i64,
}
#[derive(Deserialize)]
struct VerificationRecordWire {
    method: VerificationMethod,
    status: VerificationStatus,
    issuer: Option<String>,
    subject: Option<String>,
    session: Option<String>,
    credential_ref: Option<CredentialRef>,
    strength: VerificationStrength,
    verified_at: i64,
}
impl VerificationRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        method: VerificationMethod,
        status: VerificationStatus,
        issuer: Option<String>,
        subject: Option<String>,
        session: Option<String>,
        credential_ref: Option<CredentialRef>,
        strength: VerificationStrength,
        verified_at: i64,
    ) -> Result<Self, IdentityValidationError> {
        for (v, field) in [
            (&issuer, "issuer"),
            (&subject, "subject"),
            (&session, "session"),
        ] {
            if let Some(v) = v {
                metadata(v, field)?;
            }
        }
        if verified_at < 0 {
            return Err(IdentityValidationError::NegativeTimestamp {
                field: "verified_at",
            });
        }
        let has_no_evidence =
            issuer.is_none() && subject.is_none() && session.is_none() && credential_ref.is_none();
        let method_fields_valid = match status {
            VerificationStatus::None => has_no_evidence,
            VerificationStatus::Verified => match method {
                VerificationMethod::JwtOperator => {
                    issuer.is_some()
                        && subject.is_some()
                        && session.is_none()
                        && credential_ref.is_none()
                }
                VerificationMethod::DesktopCredential => {
                    issuer.is_none()
                        && subject.is_none()
                        && session.is_none()
                        && credential_ref.is_some()
                }
                VerificationMethod::ChannelAllowlist => has_no_evidence,
                VerificationMethod::NodeSession => {
                    issuer.is_none()
                        && subject.is_none()
                        && session.is_some()
                        && credential_ref.is_none()
                }
            },
        };
        if !method_fields_valid {
            return Err(IdentityValidationError::InvalidVerification);
        }
        Ok(Self {
            method,
            status,
            issuer,
            subject,
            session,
            credential_ref,
            strength,
            verified_at,
        })
    }
    pub fn method(&self) -> VerificationMethod {
        self.method
    }
    pub fn status(&self) -> VerificationStatus {
        self.status
    }
    pub fn issuer(&self) -> Option<&str> {
        self.issuer.as_deref()
    }
    pub fn subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }
    pub fn session(&self) -> Option<&str> {
        self.session.as_deref()
    }
    pub fn credential_ref(&self) -> Option<&CredentialRef> {
        self.credential_ref.as_ref()
    }
    pub fn strength(&self) -> &VerificationStrength {
        &self.strength
    }
    pub fn verified_at(&self) -> i64 {
        self.verified_at
    }
}
impl TryFrom<VerificationRecordWire> for VerificationRecord {
    type Error = IdentityValidationError;
    fn try_from(v: VerificationRecordWire) -> Result<Self, Self::Error> {
        Self::new(
            v.method,
            v.status,
            v.issuer,
            v.subject,
            v.session,
            v.credential_ref,
            v.strength,
            v.verified_at,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "AppearanceEvidenceWire")]
pub struct AppearanceEvidence {
    pub transport: AppearanceTransport,
    pub address: Address,
    pub verification: VerificationRecord,
    pub at: i64,
    pub sequence: Option<DurableSequence>,
}
impl AppearanceEvidence {
    pub fn new(
        transport: AppearanceTransport,
        address: Address,
        verification: VerificationRecord,
        at: i64,
        sequence: Option<DurableSequence>,
    ) -> Result<Self, IdentityValidationError> {
        address.validate()?;
        if at < 0 {
            return Err(IdentityValidationError::NegativeTimestamp { field: "at" });
        }
        Ok(Self {
            transport,
            address,
            verification,
            at,
            sequence,
        })
    }
    pub fn validate(&self) -> Result<(), IdentityValidationError> {
        self.address.validate()?;
        if self.at < 0 {
            Err(IdentityValidationError::NegativeTimestamp { field: "at" })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "DelegationHopWire")]
pub struct DelegationHop {
    pub acting: PrincipalRef,
    pub represented: PrincipalRef,
    pub evidence: AppearanceEvidence,
    pub asserted_by: PrincipalRef,
    pub asserted_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "AppearanceSnapshotWire")]
pub struct AppearanceSnapshot {
    principal: PrincipalRef,
    #[serde(flatten)]
    evidence: AppearanceEvidence,
    delegation: Vec<DelegationHop>,
    terminal_represented: Option<PrincipalRef>,
}
#[derive(Deserialize)]
struct AppearanceSnapshotWire {
    principal: PrincipalRef,
    #[serde(flatten)]
    evidence: AppearanceEvidence,
    #[serde(default)]
    delegation: Vec<DelegationHop>,
    terminal_represented: Option<PrincipalRef>,
}

impl AppearanceSnapshot {
    pub fn new(
        principal: PrincipalRef,
        evidence: AppearanceEvidence,
        delegation: Vec<DelegationHop>,
        terminal_represented: Option<PrincipalRef>,
    ) -> Result<Self, IdentityValidationError> {
        let value = Self {
            principal,
            evidence,
            delegation,
            terminal_represented,
        };
        value.validate()?;
        Ok(value)
    }
    pub fn principal(&self) -> &PrincipalRef {
        &self.principal
    }
    pub fn evidence(&self) -> &AppearanceEvidence {
        &self.evidence
    }
    pub fn delegation(&self) -> &[DelegationHop] {
        &self.delegation
    }
    pub fn terminal_represented(&self) -> Option<&PrincipalRef> {
        self.terminal_represented.as_ref()
    }
    pub fn validate(&self) -> Result<(), IdentityValidationError> {
        self.principal
            .validate_at(PrincipalPosition::AppearancePrincipal)?;
        self.evidence.validate()?;
        validate_delegation_chain(
            &self.principal,
            &self.delegation,
            self.terminal_represented.as_ref(),
        )
    }
}
impl TryFrom<AppearanceSnapshotWire> for AppearanceSnapshot {
    type Error = IdentityValidationError;
    fn try_from(v: AppearanceSnapshotWire) -> Result<Self, Self::Error> {
        Self::new(
            v.principal,
            v.evidence,
            v.delegation,
            v.terminal_represented,
        )
    }
}

pub fn validate_delegation_chain(
    principal: &PrincipalRef,
    hops: &[DelegationHop],
    terminal: Option<&PrincipalRef>,
) -> Result<(), IdentityValidationError> {
    if hops.len() > MAX_DELEGATION_HOPS {
        return Err(IdentityValidationError::DelegationTooDeep);
    }
    if hops.is_empty() {
        return if terminal.is_none() {
            Ok(())
        } else {
            Err(IdentityValidationError::InvalidDelegationTerminal)
        };
    }
    if hops.first().map(|h| &h.acting) != Some(principal)
        || terminal != hops.last().map(|h| &h.represented)
    {
        return Err(IdentityValidationError::InvalidDelegationTerminal);
    }
    let mut seen = HashSet::new();
    seen.insert(principal);
    for (index, hop) in hops.iter().enumerate() {
        hop.acting
            .validate_at(PrincipalPosition::DelegationActing)?;
        let represented_position = if index + 1 == hops.len() {
            PrincipalPosition::DelegationTerminal
        } else {
            PrincipalPosition::DelegationRepresented
        };
        hop.represented.validate_at(represented_position)?;
        hop.asserted_by
            .validate_at(PrincipalPosition::DelegationAsserting)?;
        hop.evidence.validate()?;
        if hop.asserted_at < 0 {
            return Err(IdentityValidationError::NegativeTimestamp {
                field: "asserted_at",
            });
        }
        if index > 0 && hops[index - 1].represented != hop.acting {
            return Err(IdentityValidationError::DelegationDiscontinuous);
        }
        if !seen.insert(&hop.represented) {
            return Err(IdentityValidationError::DelegationCycle);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn node(run: Option<&str>) -> PrincipalRef {
        PrincipalRef::Agent {
            node: "cairn://p/demo/1/1/builder".into(),
            run_id: run.map(str::to_owned),
        }
    }
    fn human(id: &str) -> PrincipalRef {
        PrincipalRef::Human {
            issuer: "https://issuer.example".into(),
            subject: id.into(),
            organization: Some("org".into()),
        }
    }
    fn verification() -> VerificationRecord {
        VerificationRecord::new(
            VerificationMethod::NodeSession,
            VerificationStatus::Verified,
            None,
            None,
            Some("session-1".into()),
            None,
            VerificationStrength::new("session-bound").unwrap(),
            10,
        )
        .unwrap()
    }
    fn evidence() -> AppearanceEvidence {
        AppearanceEvidence::new(
            AppearanceTransport::ResourcePatch,
            Address::Resource {
                node: "cairn://p/demo/1/1/builder".into(),
            },
            verification(),
            11,
            Some(DurableSequence::new("event", 2).unwrap()),
        )
        .unwrap()
    }
    fn hop(acting: PrincipalRef, represented: PrincipalRef) -> DelegationHop {
        DelegationHop {
            acting,
            represented,
            evidence: evidence(),
            asserted_by: node(Some("assert-run")),
            asserted_at: 12,
        }
    }

    #[test]
    fn principal_variants_round_trip() {
        for value in [
            human("u"),
            node(Some("r")),
            PrincipalRef::Machine {
                device_id: "d".into(),
            },
            PrincipalRef::External {
                provider: "mail".into(),
                namespace: "tenant".into(),
                id: "x".into(),
            },
        ] {
            assert_eq!(
                serde_json::from_str::<PrincipalRef>(&serde_json::to_string(&value).unwrap())
                    .unwrap(),
                value
            );
        }
    }
    #[test]
    fn addresses_round_trip() {
        for value in [
            Address::Channel {
                provider: "imessage".into(),
                conversation: "c".into(),
                sender: "s".into(),
                observed_alias: Some("a".into()),
            },
            Address::Desktop {
                device_id: "d".into(),
            },
            Address::Resource {
                node: "cairn://p/demo/1/1/builder".into(),
            },
            Address::Invoke {
                origin: Some("loopback".into()),
            },
            Address::None,
        ] {
            let decoded: Address =
                serde_json::from_str(&serde_json::to_string(&value).unwrap()).unwrap();
            assert_eq!(decoded, value);
            decoded.validate().unwrap();
        }
    }
    #[test]
    fn closed_enums_round_trip_and_reject_unknown_transport() {
        for v in [
            AppearanceTransport::ResourcePatch,
            AppearanceTransport::ChannelReply,
            AppearanceTransport::RemoteIntent,
            AppearanceTransport::NonOperatorInvoke,
            AppearanceTransport::LocalInvoke,
            AppearanceTransport::AuthenticatedOperator,
            AppearanceTransport::AuthenticatedDesktop,
        ] {
            assert_eq!(
                serde_json::from_str::<AppearanceTransport>(&serde_json::to_string(&v).unwrap())
                    .unwrap(),
                v
            );
        }
        for v in [
            VerificationMethod::JwtOperator,
            VerificationMethod::DesktopCredential,
            VerificationMethod::ChannelAllowlist,
            VerificationMethod::NodeSession,
        ] {
            assert_eq!(
                serde_json::from_str::<VerificationMethod>(&serde_json::to_string(&v).unwrap())
                    .unwrap(),
                v
            );
        }
        for v in [VerificationStatus::Verified, VerificationStatus::None] {
            assert_eq!(
                serde_json::from_str::<VerificationStatus>(&serde_json::to_string(&v).unwrap())
                    .unwrap(),
                v
            );
        }
        assert!(serde_json::from_str::<AppearanceTransport>("\"carrier_pigeon\"").is_err());
    }
    #[test]
    fn none_status_is_independent_of_verification_method() {
        for method in [
            VerificationMethod::JwtOperator,
            VerificationMethod::DesktopCredential,
            VerificationMethod::ChannelAllowlist,
            VerificationMethod::NodeSession,
        ] {
            let record = VerificationRecord::new(
                method,
                VerificationStatus::None,
                None,
                None,
                None,
                None,
                VerificationStrength::new("none").unwrap(),
                1,
            )
            .unwrap();
            assert_eq!(record.method(), method);
            assert_eq!(record.status(), VerificationStatus::None);
        }
    }

    #[test]
    fn sequence_validates_and_round_trips() {
        let v = DurableSequence::new("message", 0).unwrap();
        assert_eq!(
            serde_json::from_str::<DurableSequence>(&serde_json::to_string(&v).unwrap()).unwrap(),
            v
        );
        assert!(DurableSequence::new(" ", -1).is_err());
        assert!(
            serde_json::from_value::<DurableSequence>(json!({"seq_kind":"x","seq":-1})).is_err()
        );
    }
    #[test]
    fn positions_require_run_anchored_agents_except_durable_or_terminal() {
        let v = node(None);
        assert!(v.validate_at(PrincipalPosition::DurableSubject).is_ok());
        assert!(v.validate_at(PrincipalPosition::DelegationTerminal).is_ok());
        for p in [
            PrincipalPosition::DecisionActor,
            PrincipalPosition::AppearancePrincipal,
            PrincipalPosition::DelegationActing,
            PrincipalPosition::DelegationRepresented,
            PrincipalPosition::DelegationAsserting,
        ] {
            assert!(matches!(
                v.validate_at(p),
                Err(IdentityValidationError::RunIdRequired { .. })
            ));
        }
    }
    #[test]
    fn agent_home_uses_the_closed_canonical_resource_family() {
        for home in [
            "cairn://p/demo/1/1/builder",
            "cairn://p/demo/1/1/builder/task/explore",
            "cairn://p/demo/general",
            "cairn://p/demo/general/task/explore",
        ] {
            assert!(
                PrincipalRef::Agent {
                    node: home.into(),
                    run_id: Some("r".into()),
                }
                .validate_at(PrincipalPosition::AppearancePrincipal)
                .is_ok(),
                "{home}"
            );
            assert!(Address::Resource { node: home.into() }.validate().is_ok());
        }
        for not_home in [
            "cairn://p/demo/1/1/builder/chat",
            "cairn://p/demo/general/chat",
            "cairn://p/demo/1",
            "not-a-uri",
        ] {
            assert!(
                PrincipalRef::Agent {
                    node: not_home.into(),
                    run_id: Some("r".into()),
                }
                .validate_at(PrincipalPosition::AppearancePrincipal)
                .is_err(),
                "{not_home}"
            );
            assert!(Address::Resource {
                node: not_home.into()
            }
            .validate()
            .is_err());
        }
    }
    #[test]
    fn credential_refs_reject_secrets_and_oracles() {
        assert!(CredentialRef::new("desktop-key-slot-2").is_ok());
        for bad in [
            " ",
            "token=abc",
            "Bearer abc",
            "fingerprint:abcd",
            "sha256:abcd",
            "0123456789abcdef0123456789abcdef",
        ] {
            assert!(CredentialRef::new(bad).is_err(), "{bad}");
        }
        assert!(serde_json::from_str::<CredentialRef>("\"token:abc\"").is_err());
    }
    #[test]
    fn verification_enforces_method_fields_and_deserialization() {
        let jwt = VerificationRecord::new(
            VerificationMethod::JwtOperator,
            VerificationStatus::Verified,
            Some("iss".into()),
            Some("sub".into()),
            None,
            None,
            VerificationStrength::new("mfa").unwrap(),
            1,
        )
        .unwrap();
        assert_eq!(
            serde_json::from_str::<VerificationRecord>(&serde_json::to_string(&jwt).unwrap())
                .unwrap(),
            jwt
        );
        assert!(VerificationRecord::new(
            VerificationMethod::JwtOperator,
            VerificationStatus::Verified,
            None,
            None,
            None,
            None,
            VerificationStrength::new("x").unwrap(),
            1
        )
        .is_err());
        assert!(serde_json::from_value::<VerificationRecord>(json!({"method":"desktop_credential","status":"verified","issuer":null,"subject":null,"session":null,"credential_ref":"token:abc","strength":"device","verified_at":1})).is_err());
    }
    #[test]
    fn complete_snapshot_round_trips_without_secret_material() {
        let actor = node(Some("r"));
        let terminal = node(None);
        let snapshot = AppearanceSnapshot::new(
            actor.clone(),
            evidence(),
            vec![hop(actor, terminal.clone())],
            Some(terminal),
        )
        .unwrap();
        let encoded = serde_json::to_string(&snapshot).unwrap();
        assert!(!encoded.contains("token="));
        assert_eq!(
            serde_json::from_str::<AppearanceSnapshot>(&encoded).unwrap(),
            snapshot
        );
    }
    #[test]
    fn delegation_rejects_missing_mismatched_and_unexpected_terminals() {
        let actor = node(Some("r"));
        let terminal = human("terminal");
        let hops = vec![hop(actor.clone(), terminal.clone())];
        assert!(AppearanceSnapshot::new(actor.clone(), evidence(), hops.clone(), None).is_err());
        assert!(AppearanceSnapshot::new(actor, evidence(), hops, Some(human("other"))).is_err());
        assert!(
            AppearanceSnapshot::new(human("plain"), evidence(), vec![], Some(terminal)).is_err()
        );
    }
    #[test]
    fn delegation_rejects_discontinuity_cycles_and_five_hops() {
        let actor = node(Some("r"));
        let a = human("a");
        let b = human("b");
        let broken = vec![hop(actor.clone(), a.clone()), hop(b.clone(), human("end"))];
        assert!(matches!(
            AppearanceSnapshot::new(actor.clone(), evidence(), broken, Some(human("end"))),
            Err(IdentityValidationError::DelegationDiscontinuous)
        ));
        let cyclic = vec![hop(actor.clone(), a.clone()), hop(a.clone(), actor.clone())];
        assert!(matches!(
            AppearanceSnapshot::new(actor.clone(), evidence(), cyclic, Some(actor.clone())),
            Err(IdentityValidationError::DelegationCycle)
        ));
        let mut hops = Vec::new();
        let mut current = actor.clone();
        for i in 0..5 {
            let next = human(&format!("h{i}"));
            hops.push(hop(current, next.clone()));
            current = next;
        }
        assert!(matches!(
            AppearanceSnapshot::new(actor, evidence(), hops, Some(current)),
            Err(IdentityValidationError::DelegationTooDeep)
        ));
    }
    #[test]
    fn deserialization_revalidates_snapshot_and_timestamps() {
        let value = json!({"principal":{"kind":"agent","node":"cairn://p/demo/1/1/builder","run_id":null},"transport":"resource_patch","address":{"kind":"none"},"verification":{"method":"channel_allowlist","status":"verified","issuer":null,"subject":null,"session":null,"credential_ref":null,"strength":"allowlisted","verified_at":1},"at":1,"sequence":null,"delegation":[],"terminal_represented":null});
        assert!(serde_json::from_value::<AppearanceSnapshot>(value).is_err());
        assert!(AppearanceEvidence::new(
            AppearanceTransport::LocalInvoke,
            Address::None,
            verification(),
            -1,
            None
        )
        .is_err());
        assert!(
            serde_json::from_value::<PrincipalRef>(json!({"kind":"machine","device_id":" "}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<Address>(json!({"kind":"resource","node":"not-a-uri"}))
                .is_err()
        );
        assert!(serde_json::from_value::<AppearanceEvidence>(json!({"transport":"local_invoke","address":{"kind":"none"},"verification":{"method":"channel_allowlist","status":"verified","issuer":null,"subject":null,"session":null,"credential_ref":null,"strength":"allowlisted","verified_at":1},"at":-1,"sequence":null})).is_err());
    }
}
