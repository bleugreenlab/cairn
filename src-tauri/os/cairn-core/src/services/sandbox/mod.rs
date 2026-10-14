//! Canonical OS-level filesystem sandbox, owned by the lower `cairn-sandbox` crate.

pub use cairn_sandbox::*;

pub(crate) fn sandbox_applies(fence: crate::models::Fence) -> bool {
    matches!(
        fence,
        crate::models::Fence::Ask | crate::models::Fence::Deny
    )
}
