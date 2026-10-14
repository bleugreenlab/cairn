//! Rust's half of the cross-language approvals contract.
//!
//! One checked-in fixture is the point: `bun run approvals:fixtures:generate`
//! writes it from the TypeScript service's canonical encoders, this test reads
//! the same file through Rust's, and the downstream Swift client reads it too.
//! If either side drifts, it fails here rather than surfacing much later as a
//! phone signature that verifies on one side and not the other.

use base64::Engine as _;
use cairn_common::approvals::{
    parse_canonical_p256_der_signature, ApprovalDecision, ApprovalEnrollmentBindingV1,
    ApprovalIntent, ApprovalRevocationBindingV1, ApprovalSignatureAlgorithm,
    ApprovalSignatureFormat, BaseMergeIntentV1, PhoneApprovalAssertion,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

const CONTRACT_FIXTURE: &str =
    include_str!("../../../../fixtures/approvals/phone-contract-v1.json");

fn fixture() -> Value {
    serde_json::from_str(CONTRACT_FIXTURE).expect("contract fixture is valid JSON")
}

fn base64_bytes(value: &Value) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(value.as_str().expect("base64 string"))
        .expect("canonical base64")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn expect_str(value: &Value) -> &str {
    value.as_str().expect("string")
}

#[test]
fn base_merge_intent_bytes_match_the_service() {
    let fixture = fixture();
    let recorded = &fixture["baseMergeIntent"];
    let intent: BaseMergeIntentV1 = serde_json::from_value(recorded["intent"].clone()).unwrap();

    let canonical = intent.canonical_bytes().unwrap();
    assert_eq!(canonical, base64_bytes(&recorded["canonicalBytesBase64"]));
    assert_eq!(
        canonical.len() as u64,
        recorded["canonicalBytesLength"].as_u64().unwrap()
    );
    assert_eq!(
        hex(&ApprovalIntent::BaseMergeV1(intent).digest_sha256().unwrap()),
        expect_str(&recorded["digestSha256"])
    );
}

#[test]
fn phone_ceremony_signing_bytes_match_the_service() {
    let fixture = fixture();
    let intent: BaseMergeIntentV1 =
        serde_json::from_value(fixture["baseMergeIntent"]["intent"].clone()).unwrap();
    let recorded = &fixture["phoneAssertion"];
    let assertion_json = &recorded["assertion"];

    let mut digest = [0u8; 32];
    let digest_hex = expect_str(&assertion_json["intent_digest_sha256"]);
    for (index, slot) in digest.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&digest_hex[index * 2..index * 2 + 2], 16).unwrap();
    }

    let assertion = PhoneApprovalAssertion {
        challenge_id: expect_str(&assertion_json["challenge_id"]).to_string(),
        challenge_nonce: expect_str(&assertion_json["challenge_nonce"]).to_string(),
        intent_digest_sha256: digest,
        decision: ApprovalDecision::Approve,
        approval_key_id: expect_str(&assertion_json["approval_key_id"]).to_string(),
        ceremony_id: expect_str(&assertion_json["ceremony_id"]).to_string(),
        algorithm: ApprovalSignatureAlgorithm::P256EcdsaSha256Digest,
        signature_format: ApprovalSignatureFormat::Asn1Der,
        signature: base64_bytes(&assertion_json["signature"]),
    };

    let signing = assertion
        .canonical_signing_bytes(&ApprovalIntent::BaseMergeV1(intent))
        .unwrap();
    assert_eq!(signing, base64_bytes(&recorded["signingBytesBase64"]));
    assert_eq!(
        signing.len() as u64,
        recorded["signingBytesLength"].as_u64().unwrap()
    );
    assert_eq!(
        sha256_hex(&signing),
        expect_str(&recorded["signingBytesSha256"])
    );
    // The recorded signature is also a canonical DER encoding by this side's rules.
    assertion.validate().unwrap();
}

#[test]
fn enrollment_and_revocation_bindings_match_the_service() {
    let fixture = fixture();

    for key in ["enrollmentBinding", "resetBinding"] {
        let recorded = &fixture[key];
        let binding: ApprovalEnrollmentBindingV1 =
            serde_json::from_value(recorded["binding"].clone()).unwrap();
        let canonical = binding.canonical_bytes().unwrap();
        assert_eq!(
            canonical,
            base64_bytes(&recorded["canonicalBytesBase64"]),
            "{key}"
        );
        assert_eq!(
            sha256_hex(&canonical),
            expect_str(&recorded["canonicalBytesSha256"]),
            "{key}"
        );
    }

    // The two approval-key proofs wrap the same binding under distinct domains,
    // so neither can ever be presented as the other.
    let enrollment = &fixture["enrollmentBinding"];
    let binding: ApprovalEnrollmentBindingV1 =
        serde_json::from_value(enrollment["binding"].clone()).unwrap();
    let possession = binding.key_possession_bytes().unwrap();
    let succession = binding.succession_bytes().unwrap();
    assert_eq!(
        possession,
        base64_bytes(&enrollment["keyPossessionBytesBase64"])
    );
    assert_eq!(
        succession,
        base64_bytes(&enrollment["successionBytesBase64"])
    );
    assert_ne!(possession, succession);
    assert_eq!(
        sha256_hex(&possession),
        expect_str(&enrollment["keyPossessionBytesSha256"])
    );
    assert_eq!(
        sha256_hex(&succession),
        expect_str(&enrollment["successionBytesSha256"])
    );

    let revocation = &fixture["revocationBinding"];
    let revocation_binding: ApprovalRevocationBindingV1 =
        serde_json::from_value(revocation["binding"].clone()).unwrap();
    let canonical = revocation_binding.canonical_bytes().unwrap();
    assert_eq!(canonical, base64_bytes(&revocation["canonicalBytesBase64"]));
    assert_eq!(
        sha256_hex(&canonical),
        expect_str(&revocation["canonicalBytesSha256"])
    );
}

#[test]
fn a_reset_binding_never_decodes_as_an_enrollment() {
    let fixture = fixture();
    let mut value = fixture["resetBinding"]["binding"].clone();
    value["purpose"] = Value::String("enroll".into());
    let binding: ApprovalEnrollmentBindingV1 = serde_json::from_value(value).unwrap();
    // Still naming a retired lineage, so the shape is refused rather than
    // silently encoding as an ordinary enrollment.
    assert!(binding.canonical_bytes().is_err());
}

#[test]
fn strict_decoding_refuses_unknown_and_unrepresentable_fields() {
    let fixture = fixture();
    let mut value = fixture["enrollmentBinding"]["binding"].clone();
    value["advisory_title"] = Value::String("Merge it".into());
    assert!(serde_json::from_value::<ApprovalEnrollmentBindingV1>(value).is_err());

    let mut value = fixture["enrollmentBinding"]["binding"].clone();
    value["access_control_profile"] = Value::String("device_passcode".into());
    assert!(serde_json::from_value::<ApprovalEnrollmentBindingV1>(value).is_err());

    let mut value = fixture["baseMergeIntent"]["intent"].clone();
    value.as_object_mut().unwrap().remove("audience");
    assert!(serde_json::from_value::<BaseMergeIntentV1>(value).is_err());
}

#[test]
fn der_vectors_decide_the_same_way_on_both_sides() {
    let fixture = fixture();
    let vectors = fixture["p256Vectors"]["vectors"].as_array().unwrap();
    assert!(
        vectors.len() >= 5,
        "the fixture must carry negative vectors too"
    );

    for vector in vectors {
        let label = expect_str(&vector["label"]);
        let der = base64_bytes(&vector["signatureDerBase64"]);
        let decoded = parse_canonical_p256_der_signature(&der);
        let accepted = vector["accepted"].as_bool().unwrap();
        assert_eq!(
            decoded.is_ok(),
            accepted,
            "{label}: {}",
            expect_str(&vector["reason"])
        );
        if let Ok((r, s)) = decoded {
            assert_eq!(hex(&r), expect_str(&vector["r"]), "{label} r");
            assert_eq!(hex(&s), expect_str(&vector["s"]), "{label} s");
        }
    }
}
