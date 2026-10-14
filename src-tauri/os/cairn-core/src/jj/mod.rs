//! Repository operations with Cairn's checkout repair policy.
pub use cairn_vcs::jj::*;

// The main checkout can carry persistent tracked lockfile churn while the Dev
// terminal or `bun dev:instance` runs from the project checkout. It is generated
// by Cargo's two-workspace lockfile behavior and is safe to discard when a merge
// advances the default branch. Keep this list deliberately tiny: every other
// tracked change should stop or skip checkout mutation before reconcile can
// hard-reset it.
pub(crate) const CHECKOUT_REPAIR_POLICY: cairn_vcs::checkout::CheckoutRepairPolicy =
    cairn_vcs::checkout::CheckoutRepairPolicy {
        regenerable_paths: &["src-tauri/Cargo.lock"],
    };

/// Resolve a driver with the application's checkout repair policy.
pub fn environment(bundled_bin: &str, config_dir: &std::path::Path) -> JjEnv {
    JjEnv::resolve(bundled_bin, config_dir).with_checkout_repair_policy(CHECKOUT_REPAIR_POLICY)
}

/// Use a previously snapshotted executable with the same repair policy.
pub fn environment_with_binary(bin: impl Into<String>, config_dir: &std::path::Path) -> JjEnv {
    JjEnv::with_binary(bin, config_dir).with_checkout_repair_policy(CHECKOUT_REPAIR_POLICY)
}

#[cfg(test)]
pub(crate) use cairn_vcs::jj::testing as tests;
#[cfg(test)]
mod checkout_policy_tests;
#[cfg(test)]
mod patience_contract;
#[cfg(test)]
mod publication_contract;
