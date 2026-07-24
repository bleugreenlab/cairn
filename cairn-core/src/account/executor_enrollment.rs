//! Durable, runner-scoped executor enrollment credentials; raw bearers never enter synced state.

use crate::storage::{LocalDb, RowExt};
use base64::Engine;
use cairn_common::executor_protocol::{
    EnrollmentRejectionReason, ExecutorEnrollmentIdentity, ExecutorIdentity,
};
use cairn_db::turso::params;
use rand::RngCore;
use sha2::{Digest, Sha256};

const CREDENTIAL_LIFETIME_SECONDS: i64 = 7 * 24 * 60 * 60;
const CREDENTIAL_ROTATE_BEFORE_SECONDS: i64 = 24 * 60 * 60;

// Only hashes cross the persistence boundary; raw bearer material is returned once.
fn hash(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
fn token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

pub async fn create_grant(
    db: &LocalDb,
    runner_device_id: &str,
    expected_executor_id: Option<&str>,
    expected_device_id: Option<&str>,
    ttl_seconds: i64,
) -> Result<String, String> {
    let raw = token();
    let token_hash = hash(&raw);
    let runner = runner_device_id.to_string();
    let executor = expected_executor_id.map(str::to_owned);
    let device = expected_device_id.map(str::to_owned);
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| { let token_hash=token_hash.clone(); let runner=runner.clone(); let executor=executor.clone(); let device=device.clone(); Box::pin(async move {
        conn.execute("INSERT INTO executor_enrollment_grants (token_hash,runner_device_id,expected_executor_id,expected_device_id,expires_at,created_at) VALUES (?1,?2,?3,?4,?5,?6)", params![token_hash,runner,executor,device,now+ttl_seconds,now]).await?; Ok(())
    })}).await.map_err(|e| e.to_string())?;
    Ok(raw)
}

pub async fn token_is_known(db: &LocalDb, raw: &str) -> bool {
    let digest = hash(raw);
    db.read(|conn| { let digest=digest.clone(); Box::pin(async move {
        let mut rows=conn.query("SELECT 1 FROM executor_enrollment_grants WHERE token_hash=?1 AND consumed_at IS NULL AND expires_at>?2 UNION ALL SELECT 1 FROM executor_enrollments WHERE revoked_at IS NULL AND ((credential_hash=?1 AND expires_at>?2) OR (previous_credential_hash=?1 AND previous_expires_at>?2)) LIMIT 1", params![digest, chrono::Utc::now().timestamp()]).await?;
        Ok(rows.next().await?.is_some())
    })}).await.unwrap_or(false)
}

pub async fn accept(
    db: &LocalDb,
    bearer: &str,
    enrollment: &ExecutorEnrollmentIdentity,
    identity: &ExecutorIdentity,
    runner_device_id: &str,
) -> Result<Option<String>, EnrollmentRejectionReason> {
    match enrollment {
        ExecutorEnrollmentIdentity::Colocated => Ok(None),
        ExecutorEnrollmentIdentity::Grant {
            token,
            expected_runner_device_id,
        } => {
            if token != bearer || expected_runner_device_id != runner_device_id {
                return Err(EnrollmentRejectionReason::RunnerIdentityMismatch);
            }
            consume_grant(db, token, identity, runner_device_id)
                .await
                .map(Some)
        }
        ExecutorEnrollmentIdentity::Credential {
            credential,
            expected_runner_device_id,
        } => {
            if credential != bearer || expected_runner_device_id != runner_device_id {
                return Err(EnrollmentRejectionReason::RunnerIdentityMismatch);
            }
            validate_credential(db, credential, identity, runner_device_id).await?;
            Ok(None)
        }
    }
}

async fn consume_grant(
    db: &LocalDb,
    raw: &str,
    identity: &ExecutorIdentity,
    runner_device_id: &str,
) -> Result<String, EnrollmentRejectionReason> {
    let digest = hash(raw);
    let now = chrono::Utc::now().timestamp();
    let binding = db.read(|conn| { let digest=digest.clone(); Box::pin(async move {
        let mut rows=conn.query("SELECT runner_device_id,expected_executor_id,expected_device_id,expires_at FROM executor_enrollment_grants WHERE token_hash=?1 AND consumed_at IS NULL", (digest.as_str(),)).await?;
        let Some(row)=rows.next().await? else { return Ok(None) };
        Ok(Some((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?, row.i64(3)?)))
    })}).await.map_err(|_| EnrollmentRejectionReason::Unenrolled)?;
    let Some((runner, executor, device, expires)) = binding else {
        return Err(EnrollmentRejectionReason::Unenrolled);
    };
    if expires <= now {
        return Err(EnrollmentRejectionReason::Expired);
    }
    if runner != runner_device_id
        || executor
            .as_ref()
            .is_some_and(|v| v != &identity.executor_id)
        || device.as_ref().is_some_and(|v| v != &identity.device_id)
    {
        return Err(EnrollmentRejectionReason::IdentityMismatch);
    }
    let credential = token();
    let credential_hash = hash(&credential);
    let identity = identity.clone();
    let runner = runner_device_id.to_string();
    let credential_expires = now + CREDENTIAL_LIFETIME_SECONDS;
    let consumed = db.write(|conn| { let digest=digest.clone(); let credential_hash=credential_hash.clone(); let identity=identity.clone(); let runner=runner.clone(); Box::pin(async move {
        let changed = conn.execute("UPDATE executor_enrollment_grants SET consumed_at=?2 WHERE token_hash=?1 AND consumed_at IS NULL", params![digest,now]).await?;
        if changed != 1 { return Ok(false); }
        conn.execute("INSERT INTO executor_enrollments (executor_id,device_id,runner_device_id,credential_hash,enrolled_at,expires_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?5) ON CONFLICT(executor_id) DO UPDATE SET device_id=excluded.device_id,runner_device_id=excluded.runner_device_id,credential_hash=excluded.credential_hash,enrolled_at=excluded.enrolled_at,expires_at=excluded.expires_at,revoked_at=NULL,updated_at=excluded.updated_at", params![identity.executor_id,identity.device_id,runner,credential_hash,now,credential_expires]).await?; Ok(true)
    })}).await.map_err(|_| EnrollmentRejectionReason::Unenrolled)?;
    if !consumed {
        return Err(EnrollmentRejectionReason::Unenrolled);
    }
    Ok(credential)
}

async fn validate_credential(
    db: &LocalDb,
    raw: &str,
    identity: &ExecutorIdentity,
    runner: &str,
) -> Result<(), EnrollmentRejectionReason> {
    let digest = hash(raw);
    let executor = identity.executor_id.clone();
    let device = identity.device_id.clone();
    let runner = runner.to_string();
    let now = chrono::Utc::now().timestamp();
    let state = db.read(|conn| Box::pin(async move {
        let mut rows=conn.query("SELECT revoked_at,CASE WHEN credential_hash=?1 THEN expires_at ELSE previous_expires_at END FROM executor_enrollments WHERE (credential_hash=?1 OR previous_credential_hash=?1) AND executor_id=?2 AND device_id=?3 AND runner_device_id=?4", params![digest,executor,device,runner]).await?;
        let Some(row) = rows.next().await? else { return Ok(None) };
        Ok(Some((row.opt_i64(0)?, row.i64(1)?)))
    })).await.map_err(|_| EnrollmentRejectionReason::Unenrolled)?
        .ok_or(EnrollmentRejectionReason::IdentityMismatch)?;
    if state.0.is_some() {
        Err(EnrollmentRejectionReason::Revoked)
    } else if state.1 <= now {
        Err(EnrollmentRejectionReason::Expired)
    } else {
        Ok(())
    }
}

pub async fn rotate_credential(
    db: &LocalDb,
    current: &str,
    identity: &ExecutorIdentity,
    runner_device_id: &str,
) -> Result<Option<(String, i64)>, EnrollmentRejectionReason> {
    let current_hash = hash(current);
    let replacement = token();
    let replacement_hash = hash(&replacement);
    let now = chrono::Utc::now().timestamp();
    let expires_at = now + CREDENTIAL_LIFETIME_SECONDS;
    let rotate_by = now + CREDENTIAL_ROTATE_BEFORE_SECONDS;
    let executor = identity.executor_id.clone();
    let device = identity.device_id.clone();
    let runner = runner_device_id.to_string();
    let changed = db.write(|conn| {
        let replacement_hash = replacement_hash.clone();
        let current_hash = current_hash.clone();
        let executor = executor.clone();
        let device = device.clone();
        let runner = runner.clone();
        Box::pin(async move {
            let changed = conn.execute("UPDATE executor_enrollments SET previous_credential_hash=credential_hash,previous_expires_at=expires_at,credential_hash=?1,expires_at=?2,updated_at=?3 WHERE credential_hash=?4 AND previous_credential_hash IS NULL AND executor_id=?5 AND device_id=?6 AND runner_device_id=?7 AND revoked_at IS NULL AND expires_at>?3 AND expires_at<=?8", params![replacement_hash.clone(),expires_at,now,current_hash.clone(),executor.clone(),device.clone(),runner.clone(),rotate_by]).await?;
            if changed == 1 {
                return Ok(changed);
            }
            // A reconnect may present the bounded previous credential after the
            // active replacement was lost in transit. Since raw active secrets
            // are never stored, replace that undelivered active hash with a new
            // one while preserving the same previous credential and expiry.
            let recovered = conn.execute("UPDATE executor_enrollments SET credential_hash=?1,expires_at=?2,updated_at=?3 WHERE previous_credential_hash=?4 AND previous_expires_at>?3 AND executor_id=?5 AND device_id=?6 AND runner_device_id=?7 AND revoked_at IS NULL", params![replacement_hash,expires_at,now,current_hash,executor,device,runner]).await?;
            Ok(recovered)
        })
    }).await.map_err(|_| EnrollmentRejectionReason::Unenrolled)?;
    Ok((changed == 1).then_some((replacement, expires_at)))
}

pub async fn confirm_rotated_credential(
    db: &LocalDb,
    credential: &str,
    identity: &ExecutorIdentity,
    runner_device_id: &str,
) -> Result<bool, String> {
    let credential_hash = hash(credential);
    let executor = identity.executor_id.clone();
    let device = identity.device_id.clone();
    let runner = runner_device_id.to_string();
    db.write(|conn| {
        let credential_hash = credential_hash.clone();
        let executor = executor.clone();
        let device = device.clone();
        let runner = runner.clone();
        Box::pin(async move {
            let changed = conn.execute("UPDATE executor_enrollments SET previous_credential_hash=NULL,previous_expires_at=NULL WHERE credential_hash=?1 AND executor_id=?2 AND device_id=?3 AND runner_device_id=?4 AND revoked_at IS NULL", params![credential_hash,executor,device,runner]).await?;
            Ok(changed == 1)
        })
    }).await.map_err(|error| error.to_string())
}

pub async fn revoke(
    db: &LocalDb,
    executor_id: &str,
    device_id: &str,
    runner_device_id: &str,
) -> Result<bool, String> {
    let now = chrono::Utc::now().timestamp();
    let executor = executor_id.to_string();
    let device = device_id.to_string();
    let runner = runner_device_id.to_string();
    db.write(|conn| {
        let executor = executor.clone();
        let device = device.clone();
        let runner = runner.clone();
        Box::pin(async move {
            let changed = conn.execute("UPDATE executor_enrollments SET revoked_at=?1,updated_at=?1 WHERE executor_id=?2 AND device_id=?3 AND runner_device_id=?4 AND revoked_at IS NULL", params![now,executor,device,runner]).await?;
            Ok(changed == 1)
        })
    }).await.map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

    async fn db() -> LocalDb {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = LocalDb::open(dir.join("private.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    #[tokio::test]
    async fn grant_is_one_time_and_credential_reconnects() {
        let db = db().await;
        let grant = create_grant(&db, "runner", Some("executor"), Some("device"), 60)
            .await
            .unwrap();
        assert!(token_is_known(&db, &grant).await);
        let identity = ExecutorIdentity {
            device_id: "device".into(),
            executor_id: "executor".into(),
            display_name: "Executor".into(),
        };
        let credential = accept(
            &db,
            &grant,
            &ExecutorEnrollmentIdentity::Grant {
                token: grant.clone(),
                expected_runner_device_id: "runner".into(),
            },
            &identity,
            "runner",
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!token_is_known(&db, &grant).await);
        assert!(token_is_known(&db, &credential).await);
        accept(
            &db,
            &credential,
            &ExecutorEnrollmentIdentity::Credential {
                credential: credential.clone(),
                expected_runner_device_id: "runner".into(),
            },
            &identity,
            "runner",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn expired_credential_is_unknown_and_rejected() {
        let db = db().await;
        let identity = ExecutorIdentity {
            device_id: "device".into(),
            executor_id: "executor".into(),
            display_name: "Executor".into(),
        };
        let grant = create_grant(&db, "runner", Some("executor"), Some("device"), 60)
            .await
            .unwrap();
        let credential = accept(
            &db,
            &grant,
            &ExecutorEnrollmentIdentity::Grant {
                token: grant.clone(),
                expected_runner_device_id: "runner".into(),
            },
            &identity,
            "runner",
        )
        .await
        .unwrap()
        .unwrap();
        let digest = hash(&credential);
        db.execute(
            "UPDATE executor_enrollments SET expires_at=0 WHERE credential_hash=?1",
            (digest.as_str(),),
        )
        .await
        .unwrap();
        assert!(!token_is_known(&db, &credential).await);
        assert_eq!(
            accept(
                &db,
                &credential,
                &ExecutorEnrollmentIdentity::Credential {
                    credential: credential.clone(),
                    expected_runner_device_id: "runner".into()
                },
                &identity,
                "runner"
            )
            .await,
            Err(EnrollmentRejectionReason::Expired)
        );
    }

    #[tokio::test]
    async fn rotation_is_compare_and_swap_and_loses_to_revoke() {
        let db = db().await;
        let identity = ExecutorIdentity {
            device_id: "device".into(),
            executor_id: "executor".into(),
            display_name: "Executor".into(),
        };
        let grant = create_grant(&db, "runner", Some("executor"), Some("device"), 60)
            .await
            .unwrap();
        let credential = accept(
            &db,
            &grant,
            &ExecutorEnrollmentIdentity::Grant {
                token: grant.clone(),
                expected_runner_device_id: "runner".into(),
            },
            &identity,
            "runner",
        )
        .await
        .unwrap()
        .unwrap();
        let digest = hash(&credential);
        let now = chrono::Utc::now().timestamp();
        db.execute(
            "UPDATE executor_enrollments SET expires_at=?1 WHERE credential_hash=?2",
            params![now + 60, digest],
        )
        .await
        .unwrap();
        let (lost_replacement, _) = rotate_credential(&db, &credential, &identity, "runner")
            .await
            .unwrap()
            .unwrap();
        assert!(token_is_known(&db, &credential).await);
        // Simulate disconnect before delivery: reconnecting with the previous
        // credential mints a new recoverable active secret.
        let (replacement, _) = rotate_credential(&db, &credential, &identity, "runner")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(replacement, lost_replacement);
        assert!(!token_is_known(&db, &lost_replacement).await);
        assert!(
            confirm_rotated_credential(&db, &replacement, &identity, "runner")
                .await
                .unwrap()
        );
        assert!(!token_is_known(&db, &credential).await);
        assert!(rotate_credential(&db, &credential, &identity, "runner")
            .await
            .unwrap()
            .is_none());
        assert!(revoke(&db, "executor", "device", "runner").await.unwrap());
        assert!(rotate_credential(&db, &replacement, &identity, "runner")
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            accept(
                &db,
                &replacement,
                &ExecutorEnrollmentIdentity::Credential {
                    credential: replacement.clone(),
                    expected_runner_device_id: "runner".into()
                },
                &identity,
                "runner"
            )
            .await,
            Err(EnrollmentRejectionReason::Revoked)
        );
    }

    #[tokio::test]
    async fn grant_binding_rejects_identity_mismatch() {
        let db = db().await;
        let grant = create_grant(&db, "runner", Some("expected"), None, 60)
            .await
            .unwrap();
        let identity = ExecutorIdentity {
            device_id: "device".into(),
            executor_id: "other".into(),
            display_name: "Other".into(),
        };
        assert_eq!(
            accept(
                &db,
                &grant,
                &ExecutorEnrollmentIdentity::Grant {
                    token: grant.clone(),
                    expected_runner_device_id: "runner".into()
                },
                &identity,
                "runner"
            )
            .await,
            Err(EnrollmentRejectionReason::IdentityMismatch)
        );
    }
}
