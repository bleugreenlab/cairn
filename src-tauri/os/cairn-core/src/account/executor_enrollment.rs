//! Executor-only adapter over the kind-scoped device enrollment rail.
//!
//! Keeping this boundary lets executor transport callers remain explicitly
//! executor-specific while the shared persistence implementation requires a
//! device kind for every lookup and mutation.

use super::device_enrollment::{self, DeviceKind};
pub use super::device_enrollment::{
    AcceptOutcome, BearerSubject, RecoverySource, UnrecognizedBearer,
};
use crate::storage::LocalDb;
use cairn_common::executor_protocol::{
    EnrollmentRejectionReason, ExecutorEnrollmentIdentity, ExecutorIdentity,
};

pub async fn create_grant(
    db: &LocalDb,
    runner: &str,
    endpoint: Option<&str>,
    device: Option<&str>,
    ttl: i64,
) -> Result<String, String> {
    device_enrollment::create_grant(db, DeviceKind::Executor, runner, endpoint, device, ttl).await
}
pub async fn token_may_open_handshake(db: &LocalDb, raw: &str) -> bool {
    device_enrollment::token_may_open_handshake(db, DeviceKind::Executor, raw).await
}
pub async fn token_is_known(db: &LocalDb, raw: &str) -> bool {
    device_enrollment::token_is_known(db, DeviceKind::Executor, raw).await
}
pub async fn describe_unrecognized_bearer(db: &LocalDb, raw: &str) -> UnrecognizedBearer {
    device_enrollment::describe_unrecognized_bearer(db, DeviceKind::Executor, raw).await
}
pub async fn create_recovery_grant(
    db: &LocalDb,
    runner: &str,
    endpoint: &str,
    device: &str,
    ttl: i64,
) -> Result<Option<String>, String> {
    device_enrollment::create_recovery_grant(
        db,
        DeviceKind::Executor,
        runner,
        endpoint,
        device,
        ttl,
    )
    .await
}
pub async fn accept(
    db: &LocalDb,
    bearer: &str,
    enrollment: &ExecutorEnrollmentIdentity,
    identity: &ExecutorIdentity,
    runner: &str,
) -> Result<AcceptOutcome, EnrollmentRejectionReason> {
    device_enrollment::accept(
        db,
        DeviceKind::Executor,
        bearer,
        enrollment,
        identity,
        runner,
    )
    .await
}
pub async fn rotate_credential(
    db: &LocalDb,
    current: &str,
    identity: &ExecutorIdentity,
    runner: &str,
) -> Result<Option<(String, i64)>, EnrollmentRejectionReason> {
    device_enrollment::rotate_credential(db, DeviceKind::Executor, current, identity, runner).await
}
pub async fn confirm_rotated_credential(
    db: &LocalDb,
    credential: &str,
    identity: &ExecutorIdentity,
    runner: &str,
) -> Result<bool, String> {
    device_enrollment::confirm_rotated_credential(
        db,
        DeviceKind::Executor,
        credential,
        identity,
        runner,
    )
    .await
}
pub async fn rename(db: &LocalDb, endpoint: &str, new_name: &str) -> Result<(), String> {
    device_enrollment::rename(db, endpoint, new_name).await
}
pub async fn revoke(
    db: &LocalDb,
    endpoint: &str,
    device: &str,
    runner: &str,
) -> Result<bool, String> {
    device_enrollment::revoke(db, DeviceKind::Executor, endpoint, device, runner).await
}
