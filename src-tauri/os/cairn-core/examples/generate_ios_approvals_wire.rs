use std::{env, fs, path::PathBuf};

fn main() {
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()));
    let approvals = fs::read_to_string(root.join("src-tauri/os/cairn-common/src/approvals.rs"))
        .expect("read canonical approvals owner");
    let identity = fs::read_to_string(root.join("src-tauri/os/cairn-common/src/identity.rs"))
        .expect("read canonical identity owner");

    for marker in [
        "cairn.approval.base_merge.v2",
        "cairn.approval.phone_assertion.v1",
        "cairn.approval.enrollment_binding.v1",
        "cairn.approval.key_possession.v1",
        "cairn.approval.enrollment_succession.v1",
        "cairn.approval.enrollment_revocation.v1",
    ] {
        assert!(
            approvals.contains(marker),
            "canonical approval marker missing: {marker}"
        );
    }
    assert!(
        identity.contains("SecureEnclaveUserPresence"),
        "identity verification vocabulary drifted"
    );
}
