//! Act-agnostic approval contracts and the first bound act: a base merge.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::identity::{
    Address, AppearanceSnapshot, AppearanceTransport, PrincipalRef, VerificationMethod,
    VerificationStatus,
};

const BASE_MERGE_DOMAIN_V2: &[u8] = b"cairn.approval.base_merge.v2\0";
const PHONE_ASSERTION_DOMAIN_V1: &[u8] = b"cairn.approval.phone_assertion.v1\0";
/// Enrollment-lifecycle domains. The binding is what App Attest attests; the
/// possession and succession domains wrap those same bytes so the two approval-
/// key signatures over one enrollment can never be interchanged, and so neither
/// can be replayed as a merge-ceremony signature.
const ENROLLMENT_BINDING_DOMAIN_V1: &[u8] = b"cairn.approval.enrollment_binding.v1\0";
const KEY_POSSESSION_DOMAIN_V1: &[u8] = b"cairn.approval.key_possession.v1\0";
const ENROLLMENT_SUCCESSION_DOMAIN_V1: &[u8] = b"cairn.approval.enrollment_succession.v1\0";
const ENROLLMENT_REVOCATION_DOMAIN_V1: &[u8] = b"cairn.approval.enrollment_revocation.v1\0";
const MAX_FIELD_LEN: usize = 4096;
const MAX_BRANCH_NAME_LEN: usize = 255;
const MAX_REPOSITORY_NAME_LEN: usize = 200;
const MAX_COMMIT_SUBJECT_LEN: usize = 300;
const REFS_HEADS: &str = "refs/heads/";

#[path = "approvals_display_ranges.rs"]
mod display_ranges;
pub use display_ranges::DISPLAY_TABLE_UNICODE_VERSION;
use display_ranges::FORBIDDEN_DISPLAY_RANGES;
/// The order of the P-256 base point. A scalar outside `1..n` is not a signature
/// component, whatever else it decodes to.
const P256_ORDER: [u8; 32] = [
    0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xbc, 0xe6, 0xfa, 0xad, 0xa7, 0x17, 0x9e, 0x84, 0xf3, 0xb9, 0xca, 0xc2, 0xfc, 0x63, 0x25, 0x51,
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApprovalIntent {
    BaseMergeV2(BaseMergeIntentV2),
}

/// Claims carried by the separately signed approvals-service assertion.
///
/// Signature, issuer, audience, and time validation happen in the consuming
/// runner before this semantic validation. This type then proves the service
/// constructed canonical Human appearance evidence rather than accepting a
/// client-defined identity dialect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceApprovalAssertion {
    pub r#type: String,
    pub iss: String,
    pub aud: String,
    pub sub: String,
    pub jti: String,
    pub iat: i64,
    pub exp: i64,
    pub challenge_id: String,
    pub enrollment_id: String,
    pub intent_digest_sha256: String,
    pub phone_assertion_hash_sha256: String,
    pub principal: PrincipalRef,
    pub appearance: AppearanceSnapshot,
    pub residual_trust: String,
}

impl ServiceApprovalAssertion {
    pub fn validate(
        &self,
        expected_issuer: &str,
        expected_audience: &str,
        expected_organization: &str,
        expected_challenge_id: &str,
        expected_intent_digest: &[u8; 32],
    ) -> Result<(), ApprovalContractError> {
        if self.r#type != "cairn_human_approval"
            || self.iss != expected_issuer
            || self.aud != expected_audience
            || self.challenge_id != expected_challenge_id
        {
            return Err(ApprovalContractError::InvalidServiceAssertion);
        }
        for (name, value) in [
            ("sub", self.sub.as_str()),
            ("jti", self.jti.as_str()),
            ("enrollment_id", self.enrollment_id.as_str()),
            (
                "phone_assertion_hash_sha256",
                self.phone_assertion_hash_sha256.as_str(),
            ),
        ] {
            validate_text(name, value)?;
        }
        if self.iat < 0
            || self.exp <= self.iat
            || self.intent_digest_sha256 != hex_digest(expected_intent_digest)
            || !is_lower_sha256(&self.phone_assertion_hash_sha256)
            || self.residual_trust != "attested_app_implementation_and_enrollment_contract"
            || self.appearance.principal() != &self.principal
            || !self.appearance.delegation().is_empty()
            || self.appearance.terminal_represented().is_some()
        {
            return Err(ApprovalContractError::InvalidServiceAssertion);
        }
        let PrincipalRef::Human {
            issuer,
            subject,
            organization,
        } = &self.principal
        else {
            return Err(ApprovalContractError::InvalidServiceAssertion);
        };
        let evidence = self.appearance.evidence();
        let Address::Companion { .. } = &evidence.address else {
            return Err(ApprovalContractError::InvalidServiceAssertion);
        };
        let verification = &evidence.verification;
        if issuer != expected_issuer
            || subject != &self.sub
            || organization.as_deref() != Some(expected_organization)
            || evidence.transport != AppearanceTransport::CompanionApp
            || evidence.at != self.iat
            || evidence.sequence.is_some()
            || verification.method() != VerificationMethod::SecureEnclaveUserPresence
            || verification.status() != VerificationStatus::Verified
            || verification.issuer() != Some(expected_issuer)
            || verification.subject() != Some(self.sub.as_str())
            || verification.verified_at() != self.iat
            || verification.strength().as_str() != "service-verified-phone-approval"
            || verification.credential_ref().map(|value| value.as_str())
                != Some(format!("companion-enrollment/{}", self.enrollment_id).as_str())
        {
            return Err(ApprovalContractError::InvalidServiceAssertion);
        }
        Ok(())
    }
}

impl PhoneApprovalAssertion {
    pub fn validate(&self) -> Result<(), ApprovalContractError> {
        validate_text("challenge_id", &self.challenge_id)?;
        if !is_lower_sha256(&self.challenge_nonce) {
            return Err(ApprovalContractError::InvalidChallengeNonce);
        }
        validate_text("approval_key_id", &self.approval_key_id)?;
        validate_text("ceremony_id", &self.ceremony_id)?;
        parse_canonical_p256_der_signature(&self.signature)?;
        Ok(())
    }
}

/// Decode an ECDSA P-256 signature, accepting only its canonical DER encoding.
///
/// ASN.1 DER admits several byte strings for one scalar pair, and a verifier
/// that tolerates them lets one approval be presented under many distinct
/// encodings — which is exactly what the service's exactly-once index on the
/// phone-assertion hash exists to prevent. This is the Rust half of the same
/// rule `api/src/lib/approvals.ts` applies, and the shared fixture vectors prove
/// the two halves agree byte for byte.
pub fn parse_canonical_p256_der_signature(
    der: &[u8],
) -> Result<([u8; 32], [u8; 32]), ApprovalContractError> {
    if der.len() < 8 || der.len() > 72 || der[0] != 0x30 || usize::from(der[1]) != der.len() - 2 {
        return Err(ApprovalContractError::InvalidSignatureEncoding);
    }
    let (r, after_r) = read_der_scalar(der, 2)?;
    let (s, after_s) = read_der_scalar(der, after_r)?;
    if after_s != der.len() {
        return Err(ApprovalContractError::InvalidSignatureEncoding);
    }
    Ok((r, s))
}

fn read_der_scalar(
    bytes: &[u8],
    offset: usize,
) -> Result<([u8; 32], usize), ApprovalContractError> {
    let invalid = || ApprovalContractError::InvalidSignatureEncoding;
    if bytes.get(offset) != Some(&0x02) {
        return Err(invalid());
    }
    let len = usize::from(*bytes.get(offset + 1).ok_or_else(invalid)?);
    if len == 0 || len > 33 || offset + 2 + len > bytes.len() {
        return Err(invalid());
    }
    let value = &bytes[offset + 2..offset + 2 + len];
    // A leading high bit would make this a negative INTEGER, and a redundant
    // leading zero would be a second encoding of the same magnitude.
    if value[0] & 0x80 != 0 {
        return Err(invalid());
    }
    if value[0] == 0 && len > 1 && value[1] & 0x80 == 0 {
        return Err(invalid());
    }
    let trimmed = if value[0] == 0 { &value[1..] } else { value };
    if trimmed.len() > 32 {
        return Err(invalid());
    }
    let mut scalar = [0u8; 32];
    scalar[32 - trimmed.len()..].copy_from_slice(trimmed);
    if scalar == [0u8; 32] || scalar >= P256_ORDER {
        return Err(invalid());
    }
    Ok((scalar, offset + 2 + len))
}

impl ApprovalChallenge {
    pub fn validate(&self) -> Result<(), ApprovalContractError> {
        validate_text("challenge_id", &self.challenge_id)?;
        validate_text("nonce", &self.nonce)?;
        validate_text("policy_id", &self.policy_id)?;
        validate_text("audience", &self.audience)?;
        if self.issued_at < 0 || self.expires_at <= self.issued_at {
            return Err(ApprovalContractError::InvalidChallengeWindow);
        }
        Ok(())
    }
}

impl ApprovalIntent {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ApprovalContractError> {
        match self {
            Self::BaseMergeV2(intent) => intent.canonical_bytes(),
        }
    }

    pub fn digest_sha256(&self) -> Result<[u8; 32], ApprovalContractError> {
        Ok(Sha256::digest(self.canonical_bytes()?).into())
    }
}

/// One base merge, described so a person can recognise it and a runner can
/// re-derive every byte of it.
///
/// `repository_name`, `source_ref`, `source_commit_subject`, and
/// `source_commit_count` exist so the phone can say "Merge feature/zeta into
/// main" from signed bytes alone. The runner reads each from the provider and
/// Git, never from the request, and reads them again immediately before
/// executing; a mismatch is the same `stale_ref` outcome as a moved branch,
/// because it is the same fact: the act shown is no longer the act available.
///
/// What the runner attests differs by field. `repository_name` and
/// `source_commit_count` are provider and Git facts. `source_ref` is the name
/// the requester gave the branch, and `source_commit_subject` is text the
/// commit's author wrote: the runner attests only that the commit at
/// `source_oid` carries it. Binding prevents either from being swapped; it does
/// not make the subject a true description of the change, which is why the phone
/// shows it as quoted author text and never in the act's headline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaseMergeIntentV2 {
    pub installation_id: String,
    pub project_id: String,
    pub repository_id: String,
    pub repository_name: String,
    pub target_ref: String,
    pub target_oid: String,
    pub source_ref: String,
    pub source_oid: String,
    pub source_commit_subject: String,
    pub source_commit_count: u32,
    pub merge_base_oid: Option<String>,
    pub candidate_oid: Option<String>,
    pub merge_strategy: ApprovalMergeStrategy,
    pub provider_flags: Vec<ApprovalProviderFlag>,
    pub action: ApprovalMergeAction,
    pub policy_id: String,
    pub policy_version: u32,
    pub merge_plan_id: String,
    pub initiating_request_id: String,
    pub result_commit_oid: Option<String>,
    pub result_tree_oid: Option<String>,
    pub audience: String,
}

impl BaseMergeIntentV2 {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ApprovalContractError> {
        self.validate()?;
        let mut out = Vec::with_capacity(640);
        out.extend_from_slice(BASE_MERGE_DOMAIN_V2);
        put_text(&mut out, &self.installation_id)?;
        put_text(&mut out, &self.project_id)?;
        put_text(&mut out, &self.repository_id)?;
        put_text(&mut out, &self.repository_name)?;
        put_text(&mut out, &self.target_ref)?;
        put_text(&mut out, &self.target_oid)?;
        put_text(&mut out, &self.source_ref)?;
        put_text(&mut out, &self.source_oid)?;
        put_text(&mut out, &self.source_commit_subject)?;
        out.extend_from_slice(&self.source_commit_count.to_be_bytes());
        put_optional_text(&mut out, self.merge_base_oid.as_deref())?;
        put_optional_text(&mut out, self.candidate_oid.as_deref())?;
        put_text(&mut out, self.merge_strategy.as_str())?;
        let flags = u32::try_from(self.provider_flags.len())
            .map_err(|_| ApprovalContractError::LengthOverflow)?;
        out.extend_from_slice(&flags.to_be_bytes());
        for flag in &self.provider_flags {
            put_text(&mut out, flag.as_str())?;
        }
        put_text(&mut out, self.action.as_str())?;
        put_text(&mut out, &self.policy_id)?;
        out.extend_from_slice(&self.policy_version.to_be_bytes());
        put_text(&mut out, &self.merge_plan_id)?;
        put_text(&mut out, &self.initiating_request_id)?;
        put_optional_text(&mut out, self.result_commit_oid.as_deref())?;
        put_optional_text(&mut out, self.result_tree_oid.as_deref())?;
        put_text(&mut out, &self.audience)?;
        Ok(out)
    }

    pub fn validate(&self) -> Result<(), ApprovalContractError> {
        for (name, value) in [
            ("installation_id", self.installation_id.as_str()),
            ("project_id", self.project_id.as_str()),
            ("repository_id", self.repository_id.as_str()),
            ("target_oid", self.target_oid.as_str()),
            ("source_oid", self.source_oid.as_str()),
            ("policy_id", self.policy_id.as_str()),
            ("merge_plan_id", self.merge_plan_id.as_str()),
            ("initiating_request_id", self.initiating_request_id.as_str()),
            ("audience", self.audience.as_str()),
        ] {
            validate_text(name, value)?;
        }
        if !is_repository_name(&self.repository_name) {
            return Err(ApprovalContractError::InvalidRepositoryName);
        }
        if !is_safe_head_ref(&self.target_ref) {
            return Err(ApprovalContractError::NonCanonicalRef("target_ref"));
        }
        if !is_safe_head_ref(&self.source_ref) {
            return Err(ApprovalContractError::NonCanonicalRef("source_ref"));
        }
        if self.source_ref == self.target_ref {
            return Err(ApprovalContractError::SourceIsTarget);
        }
        validate_display_text(
            "source_commit_subject",
            &self.source_commit_subject,
            MAX_COMMIT_SUBJECT_LEN,
            false,
        )?;
        if self.source_commit_count == 0 {
            return Err(ApprovalContractError::InvalidCommitCount);
        }
        for (name, oid) in [
            ("target_oid", Some(self.target_oid.as_str())),
            ("source_oid", Some(self.source_oid.as_str())),
            ("merge_base_oid", self.merge_base_oid.as_deref()),
            ("candidate_oid", self.candidate_oid.as_deref()),
            ("result_commit_oid", self.result_commit_oid.as_deref()),
            ("result_tree_oid", self.result_tree_oid.as_deref()),
        ] {
            if let Some(oid) = oid {
                validate_oid(name, oid)?;
            }
        }
        if self
            .provider_flags
            .windows(2)
            .any(|pair| pair[0].as_str() >= pair[1].as_str())
        {
            return Err(ApprovalContractError::NonCanonicalProviderFlags);
        }
        Ok(())
    }
}

/// The commit label a producer signs, derived from a commit message.
///
/// The runner calls this when it issues an intent and again when it revalidates
/// one, so both sides derive the same label from the same commit. It keeps the
/// first line (without a CRLF's carriage return) and turns tabs into spaces.
/// Every other run of code points the display-text rule refuses becomes one
/// visible U+FFFD rather than vanishing: deleting a right-to-left override would
/// show logical order where the provider shows the text reversed, and deleting a
/// zero-width space would join words with no sign that anything was there. It
/// then trims and truncates on a character boundary to the 300-byte bound with a
/// trailing ellipsis. A commit with no visible first line gets a fixed label,
/// because an empty field is not encodable.
pub fn derive_commit_subject(message: &str) -> String {
    const ELLIPSIS: char = '…';
    let first_line = message.split('\n').next().unwrap_or_default();
    let first_line = first_line.strip_suffix('\r').unwrap_or(first_line);
    let mut cleaned = String::with_capacity(first_line.len());
    let mut hiding = false;
    for c in first_line.chars() {
        if c == '\t' {
            cleaned.push(' ');
            hiding = false;
        } else if is_forbidden_display_char(c, false) {
            if !hiding {
                cleaned.push(char::REPLACEMENT_CHARACTER);
            }
            hiding = true;
        } else {
            cleaned.push(c);
            hiding = false;
        }
    }
    let trimmed = cleaned.trim_matches(is_edge_whitespace);
    if !trimmed.chars().any(|c| !is_display_exempt(c)) {
        return "(no commit message)".to_string();
    }
    if trimmed.len() <= MAX_COMMIT_SUBJECT_LEN {
        return trimmed.to_string();
    }
    let budget = MAX_COMMIT_SUBJECT_LEN - ELLIPSIS.len_utf8();
    let mut end = 0;
    for (index, c) in trimmed.char_indices() {
        if index + c.len_utf8() > budget {
            break;
        }
        end = index + c.len_utf8();
    }
    let mut subject = trimmed[..end]
        .trim_end_matches(is_edge_whitespace)
        .to_string();
    subject.push(ELLIPSIS);
    subject
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Approve,
    Deny,
}

// PHONE_CONTRACT_VOCABULARY_BEGIN
/// What the service can authoritatively say about one approval act.
///
/// Closed and exhaustive: a client that cannot name an outcome cannot render an
/// honest screen for it. `AlreadyConsumed` is distinct from `Accepted` because
/// exactly-once consumption has visibly happened; `Reset` is distinct from
/// `Revoked` because no authenticator survives and the recovery differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhoneApprovalOutcome {
    Pending,
    Accepted,
    AlreadyConsumed,
    Denied,
    Expired,
    StaleRef,
    Revoked,
    Reset,
    InvalidSignature,
    WrongAudience,
}

/// How far the approved act got, on an axis of its own.
///
/// `NetworkUncertain` is the honest name for a provider mutation the service
/// cannot yet prove happened or did not; it is never collapsed into failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhoneApprovalExecutionState {
    NotStarted,
    Executing,
    Succeeded,
    Failed,
    NetworkUncertain,
    Violation,
}

/// Whether an enrollment request starts a lineage or destroys one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalEnrollmentPurpose {
    Enroll,
    Reset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppAttestEnvironment {
    Production,
    Development,
}

/// The one Secure Enclave access policy this contract admits, named rather than
/// described so no caller can widen it in passing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalKeyAccessControlProfile {
    UserPresencePrivateKeyUsage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalEnrollmentRetirement {
    Revoked,
    Reset,
}

/// How the provider combines the source branch into the target.
///
/// Closed so the phone can describe every value in words; an unknown strategy
/// is a ceremony the app could not honestly render, so it fails decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalMergeStrategy {
    Merge,
    Squash,
    Rebase,
}

/// Provider-side options the merge is performed with. Declared in canonical
/// (byte-sorted) order, which is the order the digest requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalProviderFlag {
    DeleteBranch,
    RequireChecks,
}

/// The act a base-merge intent authorizes. One value today; a second is a
/// contract change the phone must learn to describe before it can appear.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalMergeAction {
    MergePullRequest,
}
// PHONE_CONTRACT_VOCABULARY_END

impl ApprovalMergeStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Merge => "merge",
            Self::Squash => "squash",
            Self::Rebase => "rebase",
        }
    }
}

impl ApprovalProviderFlag {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DeleteBranch => "delete_branch",
            Self::RequireChecks => "require_checks",
        }
    }
}

impl ApprovalMergeAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MergePullRequest => "merge_pull_request",
        }
    }
}

/// Everything an enrollment or reset request binds, in one canonical encoding.
///
/// These bytes are the App Attest client data, so a valid attestation proves the
/// attested app instance saw this exact approval public key, this challenge,
/// and — for a reset — this exact lineage being destroyed. Succession evidence is
/// deliberately NOT a field: it is a proof about this binding, not part of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalEnrollmentBindingV1 {
    pub purpose: ApprovalEnrollmentPurpose,
    pub attestation_challenge: String,
    pub org_id: String,
    pub user_id: String,
    pub installation_id: String,
    pub app_instance_id: String,
    pub app_attest_key_id: String,
    pub app_attest_environment: AppAttestEnvironment,
    pub access_control_profile: ApprovalKeyAccessControlProfile,
    pub approval_public_key_spki: String,
    pub retired_lineage_id: Option<String>,
}

impl ApprovalEnrollmentBindingV1 {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ApprovalContractError> {
        // A reset names the lineage it retires and an enroll names none. Letting
        // the two shapes overlap would let one attestation serve both.
        if (self.purpose == ApprovalEnrollmentPurpose::Reset) != self.retired_lineage_id.is_some() {
            return Err(ApprovalContractError::InvalidEnrollmentBinding);
        }
        for (name, value) in [
            ("attestation_challenge", self.attestation_challenge.as_str()),
            ("org_id", self.org_id.as_str()),
            ("user_id", self.user_id.as_str()),
            ("installation_id", self.installation_id.as_str()),
            ("app_instance_id", self.app_instance_id.as_str()),
            ("app_attest_key_id", self.app_attest_key_id.as_str()),
            (
                "approval_public_key_spki",
                self.approval_public_key_spki.as_str(),
            ),
        ] {
            validate_text(name, value)?;
        }
        let mut out = Vec::with_capacity(512);
        out.extend_from_slice(ENROLLMENT_BINDING_DOMAIN_V1);
        put_text(
            &mut out,
            match self.purpose {
                ApprovalEnrollmentPurpose::Enroll => "enroll",
                ApprovalEnrollmentPurpose::Reset => "reset",
            },
        )?;
        put_text(&mut out, &self.attestation_challenge)?;
        put_text(&mut out, &self.org_id)?;
        put_text(&mut out, &self.user_id)?;
        put_text(&mut out, &self.installation_id)?;
        put_text(&mut out, &self.app_instance_id)?;
        put_text(&mut out, &self.app_attest_key_id)?;
        put_text(
            &mut out,
            match self.app_attest_environment {
                AppAttestEnvironment::Production => "production",
                AppAttestEnvironment::Development => "development",
            },
        )?;
        put_text(&mut out, "user_presence_private_key_usage")?;
        put_text(&mut out, &self.approval_public_key_spki)?;
        put_optional_text(&mut out, self.retired_lineage_id.as_deref())?;
        Ok(out)
    }

    /// What the newly enrolled approval key signs to prove possession.
    pub fn key_possession_bytes(&self) -> Result<Vec<u8>, ApprovalContractError> {
        let mut out = KEY_POSSESSION_DOMAIN_V1.to_vec();
        out.extend_from_slice(&self.canonical_bytes()?);
        Ok(out)
    }

    /// What a surviving approval key signs to admit a successor into its lineage.
    pub fn succession_bytes(&self) -> Result<Vec<u8>, ApprovalContractError> {
        let mut out = ENROLLMENT_SUCCESSION_DOMAIN_V1.to_vec();
        out.extend_from_slice(&self.canonical_bytes()?);
        Ok(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalRevocationBindingV1 {
    pub attestation_challenge: String,
    pub org_id: String,
    pub user_id: String,
    pub enrollment_id: String,
    pub reason: ApprovalEnrollmentRetirement,
}

impl ApprovalRevocationBindingV1 {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ApprovalContractError> {
        for (name, value) in [
            ("attestation_challenge", self.attestation_challenge.as_str()),
            ("org_id", self.org_id.as_str()),
            ("user_id", self.user_id.as_str()),
            ("enrollment_id", self.enrollment_id.as_str()),
        ] {
            validate_text(name, value)?;
        }
        let mut out = Vec::with_capacity(256);
        out.extend_from_slice(ENROLLMENT_REVOCATION_DOMAIN_V1);
        put_text(&mut out, &self.attestation_challenge)?;
        put_text(&mut out, &self.org_id)?;
        put_text(&mut out, &self.user_id)?;
        put_text(&mut out, &self.enrollment_id)?;
        put_text(
            &mut out,
            match self.reason {
                ApprovalEnrollmentRetirement::Revoked => "revoked",
                ApprovalEnrollmentRetirement::Reset => "reset",
            },
        )?;
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalSignatureAlgorithm {
    P256EcdsaSha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalSignatureFormat {
    Asn1Der,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalChallenge {
    pub challenge_id: String,
    pub nonce: String,
    pub intent_digest_sha256: [u8; 32],
    pub issued_at: i64,
    pub expires_at: i64,
    pub policy_id: String,
    pub policy_version: u32,
    pub audience: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhoneApprovalAssertion {
    pub challenge_id: String,
    pub challenge_nonce: String,
    pub intent_digest_sha256: [u8; 32],
    pub decision: ApprovalDecision,
    pub approval_key_id: String,
    pub ceremony_id: String,
    pub algorithm: ApprovalSignatureAlgorithm,
    pub signature_format: ApprovalSignatureFormat,
    pub signature: Vec<u8>,
}

impl PhoneApprovalAssertion {
    pub fn canonical_signing_bytes(
        &self,
        intent: &ApprovalIntent,
    ) -> Result<Vec<u8>, ApprovalContractError> {
        self.validate()?;
        let mut out = Vec::new();
        out.extend_from_slice(PHONE_ASSERTION_DOMAIN_V1);
        put_text(&mut out, &self.challenge_id)?;
        put_text(&mut out, &self.challenge_nonce)?;
        put_text(&mut out, &hex_digest(&self.intent_digest_sha256))?;
        put_text(
            &mut out,
            match self.decision {
                ApprovalDecision::Approve => "approve",
                ApprovalDecision::Deny => "deny",
            },
        )?;
        put_text(&mut out, &self.approval_key_id)?;
        put_text(&mut out, &self.ceremony_id)?;
        put_text(&mut out, "p256_ecdsa_sha256_digest")?;
        put_text(&mut out, "asn1_der")?;
        out.extend_from_slice(&intent.canonical_bytes()?);
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalOutcome {
    PolicyDisabled,
    PolicyUnavailable,
    Pending,
    Denied,
    Expired,
    Stale,
    Consumed,
    InvalidSignature,
    WrongAudience,
    /// An authorization nobody claimed, killed because the approval key that
    /// produced it was retired. Terminal, and reachable only from
    /// `AuthorizedNotExecuted`: an execution in flight or already finished keeps
    /// its own history.
    AuthorizationRevoked,
    AuthorizedNotExecuted,
    Executing,
    Succeeded,
    Failed,
    Indeterminate,
    Violation,
}

impl ApprovalOutcome {
    pub fn can_transition_to(self, next: Self) -> bool {
        use ApprovalOutcome::*;
        matches!(
            (self, next),
            (
                Pending,
                Denied | Expired | Stale | InvalidSignature | WrongAudience | AuthorizedNotExecuted
            ) | (
                AuthorizedNotExecuted,
                Executing | Stale | AuthorizationRevoked
            ) | (Executing, Succeeded | Failed | Indeterminate)
                | (Indeterminate, Succeeded | Failed | Violation)
        )
    }

    pub fn is_terminal(self) -> bool {
        !matches!(
            self,
            Self::Pending | Self::AuthorizedNotExecuted | Self::Executing | Self::Indeterminate
        )
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ApprovalContractError {
    #[error("{0} must not be blank")]
    Blank(&'static str),
    #[error("{0} exceeds the canonical field bound")]
    TooLong(&'static str),
    #[error("{0} contains control characters")]
    NonPrintable(&'static str),
    #[error("{0} is not a canonical Git object id")]
    InvalidOid(&'static str),
    #[error("{0} must be refs/heads/ followed by a safe branch name")]
    NonCanonicalRef(&'static str),
    #[error("source_ref must name a different branch than target_ref")]
    SourceIsTarget,
    #[error("repository_name must be an ASCII owner/name path")]
    InvalidRepositoryName,
    #[error("{0} is not display-safe text")]
    UnsafeDisplayText(&'static str),
    #[error("source_commit_count must be at least 1")]
    InvalidCommitCount,
    #[error("provider_flags must be sorted and unique")]
    NonCanonicalProviderFlags,
    #[error("too many values to encode canonically")]
    LengthOverflow,
    #[error("challenge nonce must be lowercase 32-byte hex")]
    InvalidChallengeNonce,
    #[error("challenge expiry must be after a non-negative issue time")]
    InvalidChallengeWindow,
    #[error("signature must be bounded canonical ASN.1 DER")]
    InvalidSignatureEncoding,
    #[error("service assertion does not match the canonical Human approval contract")]
    InvalidServiceAssertion,
    #[error("enrollment binding does not match the canonical enrollment contract")]
    InvalidEnrollmentBinding,
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn hex_digest(value: &[u8; 32]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn validate_text(name: &'static str, value: &str) -> Result<(), ApprovalContractError> {
    if value.trim().is_empty() {
        return Err(ApprovalContractError::Blank(name));
    }
    if value.len() > MAX_FIELD_LEN {
        return Err(ApprovalContractError::TooLong(name));
    }
    if value.chars().any(char::is_control) {
        return Err(ApprovalContractError::NonPrintable(name));
    }
    Ok(())
}

fn validate_oid(name: &'static str, value: &str) -> Result<(), ApprovalContractError> {
    if !matches!(value.len(), 40 | 64)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(ApprovalContractError::InvalidOid(name));
    }
    Ok(())
}

/// `refs/heads/` followed by a branch name a person can read unambiguously.
///
/// A strict subset of `git check-ref-format`: printable ASCII only, so no two
/// distinct names can look alike, and none of the sequences Git itself refuses.
pub fn is_safe_head_ref(value: &str) -> bool {
    let Some(name) = value.strip_prefix(REFS_HEADS) else {
        return false;
    };
    !name.is_empty()
        && name.len() <= MAX_BRANCH_NAME_LEN
        && name
            .bytes()
            .all(|byte| (0x21..=0x7e).contains(&byte) && !b"~^:?*[\\".contains(&byte))
        && !name.contains("..")
        && !name.contains("@{")
        && !name.contains("//")
        && !name.starts_with(['/', '-'])
        && !name.ends_with(['/', '.'])
        && name != "@"
        && name
            .split('/')
            .all(|component| !component.starts_with('.') && !component.ends_with(".lock"))
}

fn is_repository_name(value: &str) -> bool {
    let segment = |part: &str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    };
    value.len() <= MAX_REPOSITORY_NAME_LEN
        && value.split('/').count() >= 2
        && value.split('/').all(segment)
}

/// Whitespace that may not open or close display text. Spelled out rather than
/// borrowed from a standard library so all three implementations agree exactly.
fn is_edge_whitespace(c: char) -> bool {
    matches!(
        c,
        '\n' | ' ' | '\u{00A0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200A}' | '\u{202F}' | '\u{205F}' | '\u{3000}'
    )
}

/// Code points that render as nothing or reorder what surrounds them, so a
/// label containing one could show a person something other than its bytes:
/// controls, plus the generated Default_Ignorable table, which leaves out the
/// three code points emoji need.
fn is_forbidden_display_char(c: char, allow_newline: bool) -> bool {
    let point = u32::from(c);
    match c {
        '\n' => !allow_newline,
        '\u{0000}'..='\u{001F}' | '\u{007F}'..='\u{009F}' => true,
        _ => FORBIDDEN_DISPLAY_RANGES
            .iter()
            .any(|&(low, high)| (low..=high).contains(&point)),
    }
}

/// The invisible code points the rule admits because emoji need them: the
/// zero-width joiner and the text and emoji presentation selectors. They may
/// appear, but never alone.
fn is_display_exempt(c: char) -> bool {
    matches!(c, '\u{200D}' | '\u{FE0E}' | '\u{FE0F}')
}

/// The rule for any text the phone renders as a fact: bounded in UTF-8 bytes,
/// no edge whitespace, no control, invisible, or bidirectional-format code
/// point, and at least one character that is not an admitted invisible one.
pub fn validate_display_text(
    name: &'static str,
    value: &str,
    max_bytes: usize,
    allow_newline: bool,
) -> Result<(), ApprovalContractError> {
    if value.is_empty()
        || value.len() > max_bytes
        || value.starts_with(is_edge_whitespace)
        || value.ends_with(is_edge_whitespace)
        || value
            .chars()
            .any(|c| is_forbidden_display_char(c, allow_newline))
        || value.chars().all(is_display_exempt)
    {
        return Err(ApprovalContractError::UnsafeDisplayText(name));
    }
    Ok(())
}

fn put_text(out: &mut Vec<u8>, value: &str) -> Result<(), ApprovalContractError> {
    let len = u32::try_from(value.len()).map_err(|_| ApprovalContractError::LengthOverflow)?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_optional_text(out: &mut Vec<u8>, value: Option<&str>) -> Result<(), ApprovalContractError> {
    match value {
        Some(value) => {
            out.push(1);
            put_text(out, value)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent() -> BaseMergeIntentV2 {
        BaseMergeIntentV2 {
            installation_id: "installation-1".into(),
            project_id: "project-1".into(),
            repository_id: "repository-1".into(),
            repository_name: "bleugreen/cairn".into(),
            target_ref: "refs/heads/main".into(),
            target_oid: "11".repeat(20),
            source_ref: "refs/heads/feature/zeta".into(),
            source_oid: "22".repeat(20),
            source_commit_subject: "Fix parser crash on empty input".into(),
            source_commit_count: 3,
            merge_base_oid: Some("33".repeat(20)),
            candidate_oid: Some("44".repeat(20)),
            merge_strategy: ApprovalMergeStrategy::Squash,
            provider_flags: vec![
                ApprovalProviderFlag::DeleteBranch,
                ApprovalProviderFlag::RequireChecks,
            ],
            action: ApprovalMergeAction::MergePullRequest,
            policy_id: "human-base-merge".into(),
            policy_version: 1,
            merge_plan_id: "plan-1".into(),
            initiating_request_id: "request-1".into(),
            result_commit_oid: Some("55".repeat(20)),
            result_tree_oid: Some("66".repeat(20)),
            audience: "cairn-runner:runner-1".into(),
        }
    }

    #[test]
    fn canonical_encoding_is_domain_separated_and_deterministic() {
        let approval = ApprovalIntent::BaseMergeV2(intent());
        let first = approval.canonical_bytes().unwrap();
        assert!(first.starts_with(BASE_MERGE_DOMAIN_V2));
        assert_eq!(first, approval.canonical_bytes().unwrap());
        let digest = approval.digest_sha256().unwrap();
        assert_eq!(digest, Sha256::digest(first).as_slice());
        assert_eq!(
            digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "a54a99f0b6c670a011d3596d2986748e7dd116ae03dd705f8583a2172cc459b3"
        );
        // The serde discriminator is the kind the service stores and filters on.
        assert_eq!(
            serde_json::to_value(&approval).unwrap()["kind"],
            "base_merge_v2"
        );
    }

    #[test]
    fn every_bound_field_changes_the_digest() {
        let original = intent();
        let baseline = ApprovalIntent::BaseMergeV2(original.clone())
            .digest_sha256()
            .unwrap();
        let mut variants: Vec<(&str, BaseMergeIntentV2)> = Vec::new();
        macro_rules! changed {
            ($field:ident, $value:expr) => {{
                let mut value = original.clone();
                value.$field = $value;
                variants.push((stringify!($field), value));
            }};
        }
        changed!(installation_id, "installation-2".into());
        changed!(project_id, "project-2".into());
        changed!(repository_id, "repository-2".into());
        changed!(repository_name, "bleugreen/other".into());
        changed!(target_ref, "refs/heads/trunk".into());
        changed!(target_oid, "aa".repeat(20));
        changed!(source_ref, "refs/heads/feature/eta".into());
        changed!(source_oid, "bb".repeat(20));
        changed!(
            source_commit_subject,
            "Fix parser crash on empty file".into()
        );
        changed!(source_commit_count, 4);
        changed!(merge_base_oid, None);
        changed!(candidate_oid, None);
        changed!(merge_strategy, ApprovalMergeStrategy::Merge);
        changed!(provider_flags, vec![ApprovalProviderFlag::RequireChecks]);
        // `action` has exactly one admissible value, so no variant exists to
        // compare. Its bytes are still bound, which the assertion below checks.
        changed!(policy_id, "other-policy".into());
        changed!(policy_version, 2);
        changed!(merge_plan_id, "plan-2".into());
        changed!(initiating_request_id, "request-2".into());
        changed!(result_commit_oid, None);
        changed!(result_tree_oid, None);
        changed!(audience, "cairn-runner:runner-2".into());
        // Every field but the single-valued `action` has a variant.
        assert_eq!(variants.len(), 21);
        let canonical = original.canonical_bytes().unwrap();
        assert!(canonical
            .windows(b"merge_pull_request".len())
            .any(|window| window == b"merge_pull_request"));
        for (field, variant) in variants {
            assert_ne!(
                ApprovalIntent::BaseMergeV2(variant)
                    .digest_sha256()
                    .unwrap(),
                baseline,
                "{field}"
            );
        }
    }

    #[test]
    fn one_shot_attempts_cannot_reenter_execution() {
        use ApprovalOutcome::*;
        assert!(Pending.can_transition_to(AuthorizedNotExecuted));
        assert!(AuthorizedNotExecuted.can_transition_to(Executing));
        assert!(AuthorizedNotExecuted.can_transition_to(AuthorizationRevoked));
        // Retiring a key never rewrites an execution that already began.
        assert!(!Executing.can_transition_to(AuthorizationRevoked));
        assert!(!Succeeded.can_transition_to(AuthorizationRevoked));
        assert!(Executing.can_transition_to(Succeeded));
        assert!(Executing.can_transition_to(Failed));
        assert!(Executing.can_transition_to(Indeterminate));
        assert!(Indeterminate.can_transition_to(Succeeded));
        assert!(!Indeterminate.can_transition_to(Executing));

        for terminal in [
            PolicyDisabled,
            PolicyUnavailable,
            Denied,
            Expired,
            Stale,
            Consumed,
            InvalidSignature,
            WrongAudience,
            AuthorizationRevoked,
            Succeeded,
            Failed,
            Violation,
        ] {
            assert!(terminal.is_terminal(), "{terminal:?}");
            assert!(!terminal.can_transition_to(Executing), "{terminal:?}");
            assert!(
                !terminal.can_transition_to(AuthorizedNotExecuted),
                "{terminal:?}"
            );
        }
        assert!(!Failed.can_transition_to(Pending));
    }

    #[test]
    fn challenge_and_phone_assertion_validation_fail_closed() {
        let challenge = ApprovalChallenge {
            challenge_id: "challenge-1".into(),
            nonce: "nonce-1".into(),
            intent_digest_sha256: [7; 32],
            issued_at: 10,
            expires_at: 20,
            policy_id: "human-base-merge".into(),
            policy_version: 1,
            audience: "cairn-runner:runner-1".into(),
        };
        challenge.validate().unwrap();
        let mut invalid = challenge;
        invalid.expires_at = invalid.issued_at;
        assert_eq!(
            invalid.validate(),
            Err(ApprovalContractError::InvalidChallengeWindow)
        );

        let assertion = PhoneApprovalAssertion {
            challenge_id: "challenge-1".into(),
            challenge_nonce: "77".repeat(32),
            intent_digest_sha256: [7; 32],
            decision: ApprovalDecision::Approve,
            approval_key_id: "approval-key-1".into(),
            ceremony_id: "ceremony-1".into(),
            algorithm: ApprovalSignatureAlgorithm::P256EcdsaSha256Digest,
            signature_format: ApprovalSignatureFormat::Asn1Der,
            // Canonical DER for r = s = 1. Smallest shape the contract accepts.
            signature: vec![0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01],
        };
        assertion.validate().unwrap();
        let intent = ApprovalIntent::BaseMergeV2(intent());
        let baseline = assertion.canonical_signing_bytes(&intent).unwrap();
        let mut changed = assertion.clone();
        changed.challenge_id = "challenge-2".into();
        assert_ne!(changed.canonical_signing_bytes(&intent).unwrap(), baseline);
        let mut changed = assertion.clone();
        changed.challenge_nonce = "88".repeat(32);
        assert_ne!(changed.canonical_signing_bytes(&intent).unwrap(), baseline);
        let mut changed = assertion.clone();
        changed.decision = ApprovalDecision::Deny;
        assert_ne!(changed.canonical_signing_bytes(&intent).unwrap(), baseline);
        let mut changed = assertion.clone();
        changed.ceremony_id = "ceremony-2".into();
        assert_ne!(changed.canonical_signing_bytes(&intent).unwrap(), baseline);

        let mut invalid_nonce = assertion.clone();
        invalid_nonce.challenge_nonce = "not-a-nonce".into();
        assert_eq!(
            invalid_nonce.validate(),
            Err(ApprovalContractError::InvalidChallengeNonce)
        );

        let mut raw_signature = assertion;
        raw_signature.signature = vec![0; 64];
        assert_eq!(
            raw_signature.validate(),
            Err(ApprovalContractError::InvalidSignatureEncoding)
        );
    }

    #[test]
    fn service_assertion_accepts_only_canonical_human_appearance() {
        let digest = ApprovalIntent::BaseMergeV2(intent())
            .digest_sha256()
            .unwrap();
        let value = serde_json::json!({
            "type": "cairn_human_approval",
            "iss": "https://api.cairn.test/approvals",
            "aud": "cairn-runner:runner-1",
            "sub": "user-1",
            "jti": "asa-1",
            "iat": 10,
            "exp": 20,
            "challenge_id": "challenge-1",
            "enrollment_id": "enrollment-1",
            "intent_digest_sha256": hex_digest(&digest),
            "phone_assertion_hash_sha256": "ab".repeat(32),
            "principal": {
                "kind": "human",
                "issuer": "https://api.cairn.test/approvals",
                "subject": "user-1",
                "organization": "org-1"
            },
            "appearance": {
                "principal": {
                    "kind": "human",
                    "issuer": "https://api.cairn.test/approvals",
                    "subject": "user-1",
                    "organization": "org-1"
                },
                "transport": "companion_app",
                "address": {
                    "kind": "companion",
                    "service": "cairn-approvals",
                    "installation_id": "installation-1",
                    "app_instance_id": "app-1"
                },
                "verification": {
                    "method": "secure_enclave_user_presence",
                    "status": "verified",
                    "issuer": "https://api.cairn.test/approvals",
                    "subject": "user-1",
                    "session": "ceremony-1",
                    "credential_ref": "companion-enrollment/enrollment-1",
                    "strength": "service-verified-phone-approval",
                    "verified_at": 10
                },
                "at": 10,
                "sequence": null,
                "delegation": [],
                "terminal_represented": null
            },
            "residual_trust": "attested_app_implementation_and_enrollment_contract"
        });
        let assertion: ServiceApprovalAssertion = serde_json::from_value(value.clone()).unwrap();
        assertion
            .validate(
                "https://api.cairn.test/approvals",
                "cairn-runner:runner-1",
                "org-1",
                "challenge-1",
                &digest,
            )
            .unwrap();

        for field in ["transport", "verification", "principal"] {
            let mut altered = value.clone();
            altered["appearance"][field] = serde_json::Value::Null;
            assert!(
                serde_json::from_value::<ServiceApprovalAssertion>(altered).is_err(),
                "accepted malformed {field}"
            );
        }
        let mut altered = assertion;
        altered.residual_trust = "direct_hardware_attestation".into();
        assert_eq!(
            altered.validate(
                "https://api.cairn.test/approvals",
                "cairn-runner:runner-1",
                "org-1",
                "challenge-1",
                &digest,
            ),
            Err(ApprovalContractError::InvalidServiceAssertion)
        );
    }

    #[test]
    fn rejects_ambiguous_or_noncanonical_inputs() {
        let mut value = intent();
        value.provider_flags.reverse();
        assert_eq!(
            value.canonical_bytes(),
            Err(ApprovalContractError::NonCanonicalProviderFlags)
        );
        let mut value = intent();
        value.target_oid = "ABC".into();
        assert_eq!(
            value.canonical_bytes(),
            Err(ApprovalContractError::InvalidOid("target_oid"))
        );

        for bad in [
            "main",
            "refs/heads/",
            "refs/tags/v1",
            "refs/heads/feature zeta",
            "refs/heads/feature..zeta",
            "refs/heads/feature@{1}",
            "refs/heads/feature//zeta",
            "refs/heads//zeta",
            "refs/heads/-zeta",
            "refs/heads/zeta/",
            "refs/heads/zeta.",
            "refs/heads/zeta.lock",
            "refs/heads/zeta.lock/x",
            "refs/heads/feature/.hidden",
            "refs/heads/@",
            "refs/heads/fe~ature",
            "refs/heads/fe^ature",
            "refs/heads/fe:ature",
            "refs/heads/fe?ature",
            "refs/heads/fe*ature",
            "refs/heads/fe[ature",
            "refs/heads/fe\\ature",
            "refs/heads/féature",
        ] {
            let mut value = intent();
            value.source_ref = bad.into();
            assert_eq!(
                value.canonical_bytes(),
                Err(ApprovalContractError::NonCanonicalRef("source_ref")),
                "{bad}"
            );
            let mut value = intent();
            value.target_ref = bad.into();
            assert_eq!(
                value.canonical_bytes(),
                Err(ApprovalContractError::NonCanonicalRef("target_ref")),
                "{bad}"
            );
        }
        let mut long = intent();
        long.source_ref = format!("refs/heads/{}", "a".repeat(256));
        assert!(long.canonical_bytes().is_err());
        for good in [
            "refs/heads/feature/zeta",
            "refs/heads/release-1.2",
            "refs/heads/a@b",
        ] {
            let mut value = intent();
            value.source_ref = good.into();
            value.canonical_bytes().unwrap();
        }

        let mut value = intent();
        value.source_ref = value.target_ref.clone();
        assert_eq!(
            value.canonical_bytes(),
            Err(ApprovalContractError::SourceIsTarget)
        );

        let mut value = intent();
        value.source_commit_count = 0;
        assert_eq!(
            value.canonical_bytes(),
            Err(ApprovalContractError::InvalidCommitCount)
        );

        for bad in [
            "cairn",
            "bleugreen/",
            "/cairn",
            "bleu green/cairn",
            "bleugreen/caïrn",
        ] {
            let mut value = intent();
            value.repository_name = bad.into();
            assert_eq!(
                value.canonical_bytes(),
                Err(ApprovalContractError::InvalidRepositoryName),
                "{bad}"
            );
        }
        let mut value = intent();
        value.repository_name = "group/subgroup/project".into();
        value.canonical_bytes().unwrap();

        for bad in [
            "",
            " leading",
            "trailing ",
            "tab\tinside",
            "line\nbreak",
            "rtl \u{202E}override",
            "zero\u{200B}width",
            "soft\u{00AD}hyphen",
            "bom\u{FEFF}",
            "tag\u{E0041}",
            "c1\u{0085}",
            "hangul filler\u{3164}",
            "Fix\u{115F}parser",
            "Fix\u{034F}parser",
            "Fix\u{FE00}parser",
            "Fix\u{E0100}parser",
            "Fix\u{2065}parser",
            "\u{200D}",
            "\u{FE0F}",
        ] {
            let mut value = intent();
            value.source_commit_subject = bad.into();
            assert_eq!(
                value.canonical_bytes(),
                Err(ApprovalContractError::UnsafeDisplayText(
                    "source_commit_subject"
                )),
                "{bad:?}"
            );
        }
        let mut value = intent();
        value.source_commit_subject = "Add family emoji 👨\u{200D}👩\u{200D}👧 support".into();
        value.canonical_bytes().unwrap();
        value.source_commit_subject = "x".repeat(301);
        assert!(value.canonical_bytes().is_err());
    }

    #[test]
    fn unknown_vocabulary_fails_decoding() {
        let valid = serde_json::to_value(intent()).unwrap();
        serde_json::from_value::<BaseMergeIntentV2>(valid.clone()).unwrap();
        for (field, value) in [
            ("merge_strategy", serde_json::json!("fast_forward")),
            ("provider_flags", serde_json::json!(["auto_merge"])),
            ("action", serde_json::json!("merge_base")),
        ] {
            let mut altered = valid.clone();
            altered[field] = value;
            assert!(
                serde_json::from_value::<BaseMergeIntentV2>(altered).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn derived_commit_subjects_are_always_display_safe() {
        assert_eq!(
            derive_commit_subject("Fix parser crash\n\nLonger body"),
            "Fix parser crash"
        );
        assert_eq!(
            derive_commit_subject("  Fix\tparser\u{202E} crash \r\nbody"),
            "Fix parser\u{FFFD} crash"
        );
        assert_eq!(derive_commit_subject("Fix parser\r\nbody"), "Fix parser");
        // A run of hidden code points leaves one visible mark, not a silent join.
        assert_eq!(
            derive_commit_subject("Fix\u{200B}\u{200C}\u{2060}parser"),
            "Fix\u{FFFD}parser"
        );
        assert_eq!(
            derive_commit_subject("Fix parser\u{3164}"),
            "Fix parser\u{FFFD}"
        );
        assert_eq!(
            derive_commit_subject("Fix\u{1160}\u{FFA0}parser"),
            "Fix\u{FFFD}parser"
        );
        assert_eq!(derive_commit_subject(" \u{200B}\n"), "\u{FFFD}");
        assert_eq!(derive_commit_subject(""), "(no commit message)");
        assert_eq!(
            derive_commit_subject("\u{200D}\u{FE0F}"),
            "(no commit message)"
        );
        let long = format!("{}ééé", "a".repeat(296));
        let subject = derive_commit_subject(&long);
        assert!(subject.len() <= MAX_COMMIT_SUBJECT_LEN, "{}", subject.len());
        assert!(subject.ends_with('…'));
        assert!(subject.starts_with(&"a".repeat(296)));
        let spaced = format!("{} tail", "b".repeat(296));
        assert_eq!(
            derive_commit_subject(&spaced),
            format!("{}…", "b".repeat(296))
        );
        for message in [
            "Add family emoji 👨\u{200D}👩\u{200D}👧 support",
            "\u{FEFF}\u{2066}isolated\u{2069}",
            &"漢".repeat(200),
            "\u{0007}bell",
            "Fix parser\u{3164}",
            "\u{034F}\u{FE00}\u{E0100}",
            "Ship it \u{2764}\u{FE0F}",
        ] {
            let subject = derive_commit_subject(message);
            validate_display_text("subject", &subject, MAX_COMMIT_SUBJECT_LEN, false)
                .unwrap_or_else(|error| panic!("{message:?} -> {subject:?}: {error}"));
        }
    }
}
