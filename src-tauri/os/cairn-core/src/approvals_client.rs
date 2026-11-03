//! The runner's client for the approvals service: the half of the phone
//! approval ceremony that issues a challenge for a base merge, waits for the
//! phone, claims the authorization, and reports what happened.
//!
//! Every response is decoded with unknown fields refused, against the shapes
//! `fixtures/approvals/runner-routes-v1.json` records; the service's own tests
//! assert it produces exactly those shapes. The canonical contract types in
//! `cairn_common::approvals` stay canonical: a wire DTO converts into them and
//! their `validate` runs.
//!
//! The signed Human assertion is verified here, by this runner, before anything
//! is claimed. The service row saying "authorized" is not enough on its own:
//! the assertion's signature, issuer, audience, organization, challenge, digest,
//! and enrollment are what make the journaled attribution trustworthy.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cairn_common::approvals::{
    ApprovalChallenge, ApprovalOutcome, BaseMergeIntentV2, ServiceApprovalAssertion,
};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use serde::{Deserialize, Serialize};

use crate::api::{ApiConfig, RunnerCredential};
use crate::services::{HttpClient, HttpResponse};

/// The `typ` header every Human approval assertion carries.
pub const ASSERTION_TYP: &str = "cairn-approval+jwt";

/// Why a call to the service did not produce the answer asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalsError {
    /// No definitive answer: the request did not complete, the service failed
    /// (5xx), or the answer did not decode. The act's state is unknown.
    Unreachable(String),
    /// The service answered and declined.
    Refused { status: u16, message: String },
}

impl std::fmt::Display for ApprovalsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable(detail) => {
                write!(f, "the approvals service is unreachable ({detail})")
            }
            Self::Refused { status, message } => {
                write!(f, "the approvals service refused ({status}): {message}")
            }
        }
    }
}

/// Display-only context for the phone. Never digested and never signed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalAdvisory {
    pub pull_request_title: Option<String>,
    pub pull_request_summary: Option<String>,
    pub checks: Option<ApprovalCheckCounts>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalCheckCounts {
    pub passed: u32,
    pub failed: u32,
    pub pending: u32,
}

/// A challenge the service issued, in canonical form, with what the runner
/// needs to verify the assertion that may follow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedChallenge {
    pub challenge: ApprovalChallenge,
    /// The `iss` the Human assertion for this challenge must carry.
    pub assertion_issuer: String,
}

/// The issuing runner's view of a challenge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChallengeStatus {
    pub challenge_id: String,
    pub state: ApprovalOutcome,
    pub intent_digest_sha256: String,
    pub service_assertion: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnrollmentState {
    Live,
    Revoked,
    Reset,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EnrollmentStanding {
    pub enrollment_id: String,
    pub installation_id: String,
    pub user_id: String,
    pub state: EnrollmentState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EnrollmentSummary {
    pub enrollment_id: String,
    pub installation_id: String,
    pub user_id: String,
    pub user_name: String,
    pub device_label: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// This call consumed the authorization.
    Claimed,
    /// An earlier call with the same claim id already consumed it.
    AlreadyClaimed,
    /// Nothing to consume: not authorized, superseded, or claimed by another id.
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionOutcome {
    Succeeded,
    Failed,
    Indeterminate,
    Violation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReportOutcome {
    Recorded,
    AlreadyRecorded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidationOutcome {
    Stale,
    AlreadyStale,
    Unavailable,
}

// ─── Wire shapes ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IssueDto {
    challenge_id: String,
    nonce: String,
    intent_digest_sha256: String,
    issued_at: String,
    policy_id: String,
    policy_version: u32,
    audience: String,
    #[allow(dead_code)]
    advisory: Option<serde_json::Value>,
    assertion_issuer: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StatusDto {
    challenge_id: String,
    state: ApprovalOutcome,
    intent_digest_sha256: String,
    service_assertion: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnrollmentsDto {
    enrollments: Vec<EnrollmentSummary>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClaimDto {
    status: String,
    #[allow(dead_code)]
    service_assertion_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReportDto {
    status: String,
    #[allow(dead_code)]
    outcome: CompletionOutcomeDto,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CompletionOutcomeDto {
    Succeeded,
    Failed,
    Indeterminate,
    Violation,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InvalidationDto {
    status: String,
}

#[derive(Deserialize)]
struct ErrorDto {
    error: String,
}

fn rfc3339_seconds(field: &str, value: &str) -> Result<i64, ApprovalsError> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|instant| instant.timestamp())
        .map_err(|_| ApprovalsError::Unreachable(format!("{field} is not an RFC 3339 time")))
}

fn digest_bytes(value: &str) -> Result<[u8; 32], ApprovalsError> {
    let invalid = || ApprovalsError::Unreachable("intent digest is not SHA-256 hex".to_string());
    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(invalid());
    }
    let mut out = [0u8; 32];
    for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
        let pair = std::str::from_utf8(chunk).map_err(|_| invalid())?;
        out[index] = u8::from_str_radix(pair, 16).map_err(|_| invalid())?;
    }
    Ok(out)
}

/// Lowercase hex of a SHA-256 digest.
pub fn digest_hex(digest: &[u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// An identifier that can sit in a URL path as it is.
fn path_id(value: &str) -> Result<&str, ApprovalsError> {
    let safe = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if safe {
        Ok(value)
    } else {
        Err(ApprovalsError::Refused {
            status: 0,
            message: format!("{value:?} is not an approvals-service identifier"),
        })
    }
}

fn decode<T: serde::de::DeserializeOwned>(
    what: &str,
    response: &HttpResponse,
) -> Result<T, ApprovalsError> {
    serde_json::from_slice(&response.body)
        .map_err(|error| ApprovalsError::Unreachable(format!("{what} did not decode: {error}")))
}

/// The approval service's signing keys, per API base, as last fetched.
///
/// Owned by whoever outlives the clients that verify against it (the
/// orchestrator, in a runner), so a verification after the first skips the
/// key-set fetch until a signature fails.
#[derive(Debug, Default)]
pub struct ApprovalKeys(Mutex<HashMap<String, Vec<String>>>);

impl ApprovalKeys {
    fn get(&self, base: &str) -> Option<Vec<String>> {
        self.0.lock().ok().and_then(|keys| keys.get(base).cloned())
    }

    fn insert(&self, base: String, keys: Vec<String>) {
        if let Ok(mut cache) = self.0.lock() {
            cache.insert(base, keys);
        }
    }
}

/// The runner's approvals-service client.
pub struct ApprovalsClient<'a> {
    http: &'a dyn HttpClient,
    keys: Arc<ApprovalKeys>,
    api: ApiConfig,
    credential: RunnerCredential,
}

impl<'a> ApprovalsClient<'a> {
    pub fn new(
        http: &'a dyn HttpClient,
        keys: Arc<ApprovalKeys>,
        api: ApiConfig,
        credential: RunnerCredential,
    ) -> Self {
        Self {
            http,
            keys,
            api,
            credential,
        }
    }

    pub fn credential(&self) -> &RunnerCredential {
        &self.credential
    }

    fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Ok(value) = HeaderValue::from_str(&format!("Bearer {}", self.credential.token())) {
            headers.insert(AUTHORIZATION, value);
        }
        headers
    }

    /// Send, and separate "the service answered" from "we do not know".
    async fn send(
        &self,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<HttpResponse, ApprovalsError> {
        let url = self.api.approvals_url(path);
        let response = match body {
            Some(body) => self.http.post(&url, body, self.headers()).await,
            None => self.http.get(&url, self.headers()).await,
        }
        .map_err(ApprovalsError::Unreachable)?;
        if response.status >= 500 || response.status < 200 || (300..400).contains(&response.status)
        {
            return Err(ApprovalsError::Unreachable(format!(
                "status {}",
                response.status
            )));
        }
        Ok(response)
    }

    fn refused(response: &HttpResponse) -> ApprovalsError {
        let message = serde_json::from_slice::<ErrorDto>(&response.body)
            .map(|body| body.error)
            .unwrap_or_else(|_| String::from_utf8_lossy(&response.body).into_owned());
        ApprovalsError::Refused {
            status: response.status,
            message,
        }
    }

    /// Ask the phone. A 409 `no_live_enrollment` is the service saying no phone
    /// in this organization can accept the intent's installation.
    pub async fn issue(
        &self,
        intent: &BaseMergeIntentV2,
        advisory: &ApprovalAdvisory,
    ) -> Result<IssuedChallenge, ApprovalsError> {
        let body = serde_json::json!({ "intent": intent, "advisory": advisory });
        let response = self.send("challenges", Some(body)).await?;
        if response.status != 201 {
            return Err(Self::refused(&response));
        }
        let dto: IssueDto = decode("the issued challenge", &response)?;
        let challenge = ApprovalChallenge {
            challenge_id: dto.challenge_id,
            nonce: dto.nonce,
            intent_digest_sha256: digest_bytes(&dto.intent_digest_sha256)?,
            issued_at: rfc3339_seconds("issuedAt", &dto.issued_at)?,
            policy_id: dto.policy_id,
            policy_version: dto.policy_version,
            audience: dto.audience,
        };
        challenge
            .validate()
            .map_err(|error| ApprovalsError::Unreachable(error.to_string()))?;
        Ok(IssuedChallenge {
            challenge,
            assertion_issuer: dto.assertion_issuer,
        })
    }

    pub async fn status(&self, challenge_id: &str) -> Result<ChallengeStatus, ApprovalsError> {
        let path = format!("challenges/{}", path_id(challenge_id)?);
        let response = self.send(&path, None).await?;
        if response.status != 200 {
            return Err(Self::refused(&response));
        }
        let dto: StatusDto = decode("the challenge status", &response)?;
        Ok(ChallengeStatus {
            challenge_id: dto.challenge_id,
            state: dto.state,
            intent_digest_sha256: dto.intent_digest_sha256,
            service_assertion: dto.service_assertion,
        })
    }

    pub async fn claim(
        &self,
        challenge_id: &str,
        intent_digest: &str,
        claim_id: &str,
    ) -> Result<ClaimOutcome, ApprovalsError> {
        let path = format!("challenges/{}/claims", path_id(challenge_id)?);
        let body = serde_json::json!({ "intentDigestSha256": intent_digest, "claimId": claim_id });
        let response = self.send(&path, Some(body)).await?;
        match response.status {
            201 | 200 => {
                let dto: ClaimDto = decode("the claim", &response)?;
                match dto.status.as_str() {
                    "claimed" => Ok(ClaimOutcome::Claimed),
                    "already_claimed" => Ok(ClaimOutcome::AlreadyClaimed),
                    other => Err(ApprovalsError::Unreachable(format!(
                        "unknown claim status {other:?}"
                    ))),
                }
            }
            409 => Ok(ClaimOutcome::Unavailable),
            _ => Err(Self::refused(&response)),
        }
    }

    pub async fn complete(
        &self,
        challenge_id: &str,
        intent_digest: &str,
        claim_id: &str,
        report_id: &str,
        outcome: CompletionOutcome,
        evidence: serde_json::Value,
    ) -> Result<ReportOutcome, ApprovalsError> {
        let path = format!("challenges/{}/completions", path_id(challenge_id)?);
        let body = serde_json::json!({
            "intentDigestSha256": intent_digest,
            "claimId": claim_id,
            "reportId": report_id,
            "outcome": outcome,
            "evidence": evidence,
        });
        let response = self.send(&path, Some(body)).await?;
        match response.status {
            201 | 200 => {
                let dto: ReportDto = decode("the completion", &response)?;
                match dto.status.as_str() {
                    "recorded" => Ok(ReportOutcome::Recorded),
                    "already_recorded" => Ok(ReportOutcome::AlreadyRecorded),
                    other => Err(ApprovalsError::Unreachable(format!(
                        "unknown completion status {other:?}"
                    ))),
                }
            }
            409 => Ok(ReportOutcome::Unavailable),
            _ => Err(Self::refused(&response)),
        }
    }

    /// Report that the approved intent no longer describes reality, so the
    /// phone shows `stale_ref`.
    pub async fn invalidate(
        &self,
        challenge_id: &str,
        intent_digest: &str,
        observed_digest: &str,
    ) -> Result<InvalidationOutcome, ApprovalsError> {
        let path = format!("challenges/{}/invalidations", path_id(challenge_id)?);
        let body = serde_json::json!({
            "intentDigestSha256": intent_digest,
            "observedIntentDigestSha256": observed_digest,
        });
        let response = self.send(&path, Some(body)).await?;
        match response.status {
            201 | 200 => {
                let dto: InvalidationDto = decode("the invalidation", &response)?;
                match dto.status.as_str() {
                    "stale" => Ok(InvalidationOutcome::Stale),
                    "already_stale" => Ok(InvalidationOutcome::AlreadyStale),
                    other => Err(ApprovalsError::Unreachable(format!(
                        "unknown invalidation status {other:?}"
                    ))),
                }
            }
            409 => Ok(InvalidationOutcome::Unavailable),
            _ => Err(Self::refused(&response)),
        }
    }

    /// One enrollment's standing, or `None` when the organization has no such
    /// enrollment.
    pub async fn enrollment(
        &self,
        enrollment_id: &str,
    ) -> Result<Option<EnrollmentStanding>, ApprovalsError> {
        let path = format!("runner/enrollments/{}", path_id(enrollment_id)?);
        let response = self.send(&path, None).await?;
        match response.status {
            200 => decode("the enrollment", &response).map(Some),
            404 => Ok(None),
            _ => Err(Self::refused(&response)),
        }
    }

    /// The organization's live approval phones.
    pub async fn enrollments(&self) -> Result<Vec<EnrollmentSummary>, ApprovalsError> {
        let response = self.send("runner/enrollments", None).await?;
        if response.status != 200 {
            return Err(Self::refused(&response));
        }
        decode::<EnrollmentsDto>("the enrollment list", &response).map(|dto| dto.enrollments)
    }

    /// The Ed25519 public keys (`x`, base64url) the API signs with.
    pub async fn jwks(&self) -> Result<Vec<String>, ApprovalsError> {
        let response = self
            .http
            .get(&self.api.jwks_url(), HeaderMap::new())
            .await
            .map_err(ApprovalsError::Unreachable)?;
        if response.status != 200 {
            return Err(ApprovalsError::Unreachable(format!(
                "the key set answered {}",
                response.status
            )));
        }
        #[derive(Deserialize)]
        struct Jwks {
            keys: Vec<Jwk>,
        }
        #[derive(Deserialize)]
        struct Jwk {
            kty: String,
            crv: Option<String>,
            x: Option<String>,
        }
        let jwks: Jwks = decode("the key set", &response)?;
        let keys: Vec<String> = jwks
            .keys
            .into_iter()
            .filter(|key| key.kty == "OKP" && key.crv.as_deref() == Some("Ed25519"))
            .filter_map(|key| key.x)
            .collect();
        if keys.is_empty() {
            return Err(ApprovalsError::Unreachable(
                "the key set holds no Ed25519 key".to_string(),
            ));
        }
        Ok(keys)
    }

    /// [`verify_service_assertion`] against this deployment's keys: cached per
    /// API base, and fetched again once if verification fails, in case the
    /// service rotated its key.
    pub async fn verify_assertion(
        &self,
        token: &str,
        expected: &AssertionExpectation<'_>,
        now: i64,
    ) -> Result<ServiceApprovalAssertion, AssertionRejection> {
        let base = self.api.base_url().to_string();
        if let Some(keys) = self.keys.get(&base) {
            match verify_service_assertion(token, &keys, expected, now) {
                Err(AssertionRejection::InvalidSignature(_)) => {}
                verdict => return verdict,
            }
        }
        let keys = self
            .jwks()
            .await
            .map_err(|error| AssertionRejection::KeysUnavailable(error.to_string()))?;
        self.keys.insert(base, keys.clone());
        verify_service_assertion(token, &keys, expected, now)
    }
}

/// Everything a Human approval assertion must match for this runner to act on
/// it.
#[derive(Debug, Clone)]
pub struct AssertionExpectation<'a> {
    pub issuer: &'a str,
    pub audience: &'a str,
    pub organization: &'a str,
    pub challenge_id: &'a str,
    pub intent_digest: &'a [u8; 32],
    /// The enrollment the project's policy selected. An approval from any other
    /// phone in the organization is not this project's approval.
    pub enrollment_id: &'a str,
}

/// Why an assertion was not accepted, in the journal's vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssertionRejection {
    /// The signature, header, issue time, claims, digest, or enrollment is wrong.
    InvalidSignature(String),
    /// It was minted for another issuer, audience, or organization.
    WrongAudience(String),
    /// The key set could not be read, so nothing could be verified.
    KeysUnavailable(String),
}

impl AssertionRejection {
    /// The `merge_attempt_events` state this rejection is journaled as.
    pub fn journal_state(&self) -> ApprovalOutcome {
        match self {
            Self::WrongAudience(_) => ApprovalOutcome::WrongAudience,
            Self::InvalidSignature(_) => ApprovalOutcome::InvalidSignature,
            // Nothing was verified either way: the keys could not be read.
            Self::KeysUnavailable(_) => ApprovalOutcome::Failed,
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            Self::InvalidSignature(detail)
            | Self::WrongAudience(detail)
            | Self::KeysUnavailable(detail) => detail,
        }
    }
}

/// Verify a `cairn_human_approval` assertion completely: the EdDSA signature
/// under one of `keys`, the `cairn-approval+jwt` type, an issue time not in
/// the future at `now`, the canonical claims
/// ([`ServiceApprovalAssertion::validate`]), and that the approving enrollment
/// is the one the policy selected. The assertion has no expiry to judge: it is
/// attribution evidence, and the claim that follows spends it exactly once.
pub fn verify_service_assertion(
    token: &str,
    keys: &[String],
    expected: &AssertionExpectation<'_>,
    now: i64,
) -> Result<ServiceApprovalAssertion, AssertionRejection> {
    use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
    let invalid = |detail: &str| AssertionRejection::InvalidSignature(detail.to_string());
    let header = decode_header(token).map_err(|_| invalid("the assertion is not a JWT"))?;
    if header.alg != Algorithm::EdDSA {
        return Err(invalid("the assertion is not signed with EdDSA"));
    }
    if header.typ.as_deref() != Some(ASSERTION_TYP) {
        return Err(invalid("the assertion is not a cairn-approval+jwt"));
    }
    let mut validation = Validation::new(Algorithm::EdDSA);
    // Issue time, issuer, and audience are judged below against `now` and the
    // canonical claims, so each failure is named rather than folded into one.
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.validate_aud = false;
    validation.required_spec_claims.clear();
    let claims = keys
        .iter()
        .filter_map(|x| DecodingKey::from_ed_components(x).ok())
        .find_map(|key| decode::<ServiceApprovalAssertion>(token, &key, &validation).ok())
        .map(|data| data.claims)
        .ok_or_else(|| {
            invalid("the assertion does not verify as a canonical Human approval under these keys")
        })?;
    if claims.iat > now + 60 {
        return Err(invalid("the assertion claims to be issued in the future"));
    }
    if claims.iss != expected.issuer || claims.aud != expected.audience {
        return Err(AssertionRejection::WrongAudience(
            "the assertion was minted for another issuer or audience".to_string(),
        ));
    }
    if let cairn_common::identity::PrincipalRef::Human { organization, .. } = &claims.principal {
        if organization.as_deref() != Some(expected.organization) {
            return Err(AssertionRejection::WrongAudience(
                "the assertion names another organization".to_string(),
            ));
        }
    }
    claims
        .validate(
            expected.issuer,
            expected.audience,
            expected.organization,
            expected.challenge_id,
            expected.intent_digest,
        )
        .map_err(|_| invalid("the assertion does not match this challenge's intent"))?;
    if claims.enrollment_id != expected.enrollment_id {
        return Err(invalid(
            "the assertion was approved on a phone other than the one this project selected",
        ));
    }
    Ok(claims)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::MockHttpClient;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

    const FIXTURE: &str = include_str!("../../../../fixtures/approvals/runner-routes-v1.json");
    /// RFC 8032's first test seed, the one the fixture is signed with.
    const SEED: &str = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";

    fn fixture() -> serde_json::Value {
        serde_json::from_str(FIXTURE).unwrap()
    }

    fn credential() -> RunnerCredential {
        use base64::Engine as _;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"sub":"runner:runner-1","org_id":"org-fixture-1"}"#);
        RunnerCredential::parse(&format!("e30.{payload}.sig")).unwrap()
    }

    fn api() -> ApiConfig {
        ApiConfig {
            base_url: "https://api.fixture.cairn.test".to_string(),
        }
    }

    fn respond(status: u16, body: &serde_json::Value) -> HttpResponse {
        HttpResponse::new(status, body.to_string().into_bytes())
    }

    fn keys() -> Vec<String> {
        vec![fixture()["assertion"]["jwks"]["keys"][0]["x"]
            .as_str()
            .unwrap()
            .to_string()]
    }

    fn digest() -> [u8; 32] {
        digest_bytes(fixture()["intentDigestSha256"].as_str().unwrap()).unwrap()
    }

    fn expectation<'a>(
        fixture: &'a serde_json::Value,
        digest: &'a [u8; 32],
    ) -> AssertionExpectation<'a> {
        let expected = &fixture["assertion"]["expected"];
        AssertionExpectation {
            issuer: expected["issuer"].as_str().unwrap(),
            audience: expected["audience"].as_str().unwrap(),
            organization: expected["organization"].as_str().unwrap(),
            challenge_id: expected["challengeId"].as_str().unwrap(),
            intent_digest: digest,
            enrollment_id: expected["enrollmentId"].as_str().unwrap(),
        }
    }

    fn verify_at(fixture: &serde_json::Value) -> i64 {
        fixture["assertion"]["expected"]["verifyAtEpochSeconds"]
            .as_i64()
            .unwrap()
    }

    /// The fixture's claims, re-signed with the fixture key after `edit`.
    fn resign(edit: impl FnOnce(&mut serde_json::Value), header: Header) -> String {
        use base64::Engine as _;
        let jwt = fixture()["assertion"]["jwt"].as_str().unwrap().to_string();
        let payload = jwt.split('.').nth(1).unwrap();
        let mut claims: serde_json::Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(payload)
                .unwrap(),
        )
        .unwrap();
        edit(&mut claims);
        let mut der = hex_bytes("302e020100300506032b657004220420");
        der.extend(hex_bytes(SEED));
        encode(&header, &claims, &EncodingKey::from_ed_der(&der)).unwrap()
    }

    fn hex_bytes(value: &str) -> Vec<u8> {
        (0..value.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&value[i..i + 2], 16).unwrap())
            .collect()
    }

    fn approval_header() -> Header {
        Header {
            typ: Some(ASSERTION_TYP.to_string()),
            ..Header::new(Algorithm::EdDSA)
        }
    }

    #[test]
    fn the_fixture_assertion_verifies_and_carries_the_human_attribution() {
        let fixture = fixture();
        let digest = digest();
        let jwt = fixture["assertion"]["jwt"].as_str().unwrap();
        let claims = verify_service_assertion(
            jwt,
            &keys(),
            &expectation(&fixture, &digest),
            verify_at(&fixture),
        )
        .unwrap();
        assert_eq!(claims.jti, fixture["assertion"]["expected"]["jti"]);
        assert!(matches!(
            claims.principal,
            cairn_common::identity::PrincipalRef::Human { .. }
        ));
        assert_eq!(claims.appearance.principal(), &claims.principal);
    }

    #[test]
    fn the_fixture_intent_digest_is_the_canonical_digest() {
        let fixture = fixture();
        let intent: BaseMergeIntentV2 = serde_json::from_value(fixture["intent"].clone()).unwrap();
        let digest = cairn_common::approvals::ApprovalIntent::BaseMergeV2(intent)
            .digest_sha256()
            .unwrap();
        assert_eq!(digest_hex(&digest), fixture["intentDigestSha256"]);
    }

    #[test]
    fn every_mismatch_is_rejected_under_its_journal_state() {
        let fixture = fixture();
        let digest = digest();
        let now = verify_at(&fixture);
        let expect = expectation(&fixture, &digest);
        let check = |token: &str, state: ApprovalOutcome| {
            let rejection = verify_service_assertion(token, &keys(), &expect, now).unwrap_err();
            assert_eq!(rejection.journal_state(), state, "{rejection:?}");
        };
        // The control: re-signing unchanged claims still verifies.
        verify_service_assertion(&resign(|_| {}, approval_header()), &keys(), &expect, now)
            .unwrap();
        // Elapsed time is not a reason to refuse: a day after issue it verifies.
        verify_service_assertion(
            fixture["assertion"]["jwt"].as_str().unwrap(),
            &keys(),
            &expect,
            now + 86_400,
        )
        .unwrap();

        // Bad signature: one byte of the signature flipped.
        let jwt = fixture["assertion"]["jwt"].as_str().unwrap();
        let mut forged = jwt.to_string();
        let last = forged.pop().unwrap();
        forged.push(if last == 'A' { 'B' } else { 'A' });
        check(&forged, ApprovalOutcome::InvalidSignature);
        // Wrong typ.
        check(
            &resign(|_| {}, Header::new(Algorithm::EdDSA)),
            ApprovalOutcome::InvalidSignature,
        );
        // An algorithm other than EdDSA.
        let hs = encode(
            &Header {
                typ: Some(ASSERTION_TYP.to_string()),
                ..Header::new(Algorithm::HS256)
            },
            &serde_json::json!({}),
            &EncodingKey::from_secret(b"secret"),
        )
        .unwrap();
        check(&hs, ApprovalOutcome::InvalidSignature);
        // Issued in the future.
        check(
            &resign(
                |claims| claims["iat"] = (now + 61).into(),
                approval_header(),
            ),
            ApprovalOutcome::InvalidSignature,
        );
        // A lifetime is not part of this contract, so a claim naming one is refused.
        check(
            &resign(
                |claims| claims["exp"] = (now + 300).into(),
                approval_header(),
            ),
            ApprovalOutcome::InvalidSignature,
        );
        // Wrong audience, issuer, organization.
        check(
            &resign(
                |claims| claims["aud"] = "cairn-runner:other".into(),
                approval_header(),
            ),
            ApprovalOutcome::WrongAudience,
        );
        check(
            &resign(
                |claims| claims["iss"] = "https://evil.test/approvals".into(),
                approval_header(),
            ),
            ApprovalOutcome::WrongAudience,
        );
        check(
            &resign(
                |claims| {
                    claims["principal"]["organization"] = "org-other".into();
                    claims["appearance"]["principal"]["organization"] = "org-other".into();
                },
                approval_header(),
            ),
            ApprovalOutcome::WrongAudience,
        );
        // Wrong digest.
        check(
            &resign(
                |claims| claims["intent_digest_sha256"] = "00".repeat(32).into(),
                approval_header(),
            ),
            ApprovalOutcome::InvalidSignature,
        );
        // Approved on a phone other than the selected one.
        let other = AssertionExpectation {
            enrollment_id: "cae_other",
            ..expect.clone()
        };
        let rejection = verify_service_assertion(jwt, &keys(), &other, now).unwrap_err();
        assert_eq!(rejection.journal_state(), ApprovalOutcome::InvalidSignature);
    }

    #[tokio::test]
    async fn every_recorded_route_response_decodes_into_the_canonical_types() {
        let fixture = fixture();
        let responses = &fixture["responses"];
        let mut http = MockHttpClient::new()
            .respond_to(
                "/approvals/challenges/apr_fixture000000000000000000001/claims",
                respond(201, &responses["claim"]["claimed"]),
            )
            .respond_to(
                "/completions",
                respond(201, &responses["completion"]["recorded"]),
            )
            .respond_to(
                "/invalidations",
                respond(201, &responses["invalidation"]["stale"]),
            )
            .respond_to(
                "/runner/enrollments/cae_",
                respond(200, &responses["enrollment"]["live"]),
            )
            .respond_to(
                "/runner/enrollments",
                respond(200, &responses["enrollments"]),
            )
            .respond_to(
                "/approvals/challenges/apr_",
                respond(200, &responses["status"]["pending"]),
            )
            .respond_to("/approvals/challenges", respond(201, &responses["issue"]));
        let client = ApprovalsClient::new(&http, Arc::default(), api(), credential());
        let intent: BaseMergeIntentV2 = serde_json::from_value(fixture["intent"].clone()).unwrap();
        let issued = client
            .issue(&intent, &ApprovalAdvisory::default())
            .await
            .unwrap();
        assert_eq!(issued.challenge.intent_digest_sha256, digest());
        assert_eq!(
            issued.assertion_issuer,
            fixture["assertion"]["expected"]["issuer"]
        );
        let id = issued.challenge.challenge_id.clone();
        assert_eq!(
            client.status(&id).await.unwrap().state,
            ApprovalOutcome::Pending
        );
        assert_eq!(
            client.claim(&id, "aa", "claim-1").await.unwrap(),
            ClaimOutcome::Claimed
        );
        assert_eq!(
            client
                .complete(
                    &id,
                    "aa",
                    "c",
                    "r",
                    CompletionOutcome::Succeeded,
                    serde_json::json!({})
                )
                .await
                .unwrap(),
            ReportOutcome::Recorded
        );
        assert_eq!(
            client.invalidate(&id, "aa", "bb").await.unwrap(),
            InvalidationOutcome::Stale
        );
        assert_eq!(
            client
                .enrollment("cae_fixture00000000000000000001")
                .await
                .unwrap()
                .unwrap()
                .state,
            EnrollmentState::Live
        );
        assert_eq!(client.enrollments().await.unwrap().len(), 1);

        // Every recorded status and standing decodes.
        for (_, body) in responses["status"].as_object().unwrap() {
            serde_json::from_value::<StatusDto>(body.clone()).unwrap();
        }
        for (_, body) in responses["enrollment"].as_object().unwrap() {
            serde_json::from_value::<EnrollmentStanding>(body.clone()).unwrap();
        }
        serde_json::from_value::<ClaimDto>(responses["claim"]["alreadyClaimed"].clone()).unwrap();
        serde_json::from_value::<ReportDto>(responses["completion"]["alreadyRecorded"].clone())
            .unwrap();
        serde_json::from_value::<InvalidationDto>(
            responses["invalidation"]["alreadyStale"].clone(),
        )
        .unwrap();

        // Every request carried the runner's bearer.
        let bearer = format!("Bearer {}", credential().token());
        assert!(http
            .requests()
            .iter()
            .all(|request| request.authorization() == Some(bearer.as_str())));

        // A field the service added that this runner does not know is refused.
        let mut extra = responses["status"]["pending"].clone();
        extra["surprise"] = true.into();
        http = MockHttpClient::new().respond_to("/approvals/challenges/", respond(200, &extra));
        let client = ApprovalsClient::new(&http, Arc::default(), api(), credential());
        assert!(matches!(
            client.status(&id).await,
            Err(ApprovalsError::Unreachable(_))
        ));
    }

    #[tokio::test]
    async fn refusals_are_told_apart_from_not_knowing() {
        let fixture = fixture();
        let responses = &fixture["responses"];
        let http = MockHttpClient::new()
            .respond_to("/claims", respond(409, &responses["claim"]["unavailable"]))
            .respond_to("/completions", HttpResponse::new(503, Vec::new()))
            .respond_to("/challenges", respond(409, &responses["issueRefused"]));
        let client = ApprovalsClient::new(&http, Arc::default(), api(), credential());
        assert_eq!(
            client.claim("apr_1", "aa", "c").await.unwrap(),
            ClaimOutcome::Unavailable
        );
        assert!(matches!(
            client
                .complete(
                    "apr_1",
                    "aa",
                    "c",
                    "r",
                    CompletionOutcome::Failed,
                    serde_json::json!({})
                )
                .await,
            Err(ApprovalsError::Unreachable(_))
        ));
        let intent: BaseMergeIntentV2 = serde_json::from_value(fixture["intent"].clone()).unwrap();
        assert_eq!(
            client.issue(&intent, &ApprovalAdvisory::default()).await,
            Err(ApprovalsError::Refused {
                status: 409,
                message: "no_live_enrollment".to_string()
            })
        );
        // An identifier that would change the path is never sent.
        assert!(client.status("../tokens").await.is_err());
    }
}
