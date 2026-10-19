//! Durable, runner-scoped device enrollment credentials; raw bearers never enter synced state.

use crate::storage::{LocalDb, RowExt};
use base64::Engine;
use cairn_common::executor_protocol::{
    normalize_executor_name, EnrollmentRejectionReason, ExecutorEnrollmentIdentity,
    ExecutorIdentity,
};
use cairn_db::turso::params;
use rand::RngCore;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeviceKind {
    Executor,
    Companion,
}

/// Whether a bearer may reach the identity-bearing handshake. This deliberately
/// does not authenticate an enrollment: the protocol layer still validates
/// identity, revocation, expiry recovery, and supersession before attachment.
pub async fn token_may_open_handshake(db: &LocalDb, kind: DeviceKind, raw: &str) -> bool {
    let kind = kind.as_str();
    let digest = hash(raw);
    db.read(|conn| { let digest=digest.clone(); Box::pin(async move {
        let mut rows=conn.query("SELECT 1 FROM executor_enrollment_grants WHERE token_hash=?1 AND device_kind=?2 AND consumed_at IS NULL AND expires_at>?3 UNION ALL SELECT 1 FROM executor_enrollments WHERE device_kind=?2 AND (credential_hash=?1 OR previous_credential_hash=?1 OR superseded_credential_hash=?1) LIMIT 1", params![digest, kind, chrono::Utc::now().timestamp()]).await?;
        Ok(rows.next().await?.is_some())
    })}).await.unwrap_or(false)
}

/// What a bearer that failed admission turned out to be. Admission answers only
/// yes or no; an operator reading the refusal needs to know whether a known
/// machine is presenting the wrong one of its own secrets or a stranger is
/// knocking. Classifies only — nothing here admits anything.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnrecognizedBearer {
    /// A one-time grant that was already spent. The machine that spent it was
    /// issued a credential in the same transaction, so a daemon presenting this
    /// is replaying its grant instead of the credential the grant bought.
    ConsumedGrant {
        consumed_at: i64,
        subject: BearerSubject,
    },
    /// A grant nobody used before it expired.
    ExpiredGrant {
        expires_at: i64,
        subject: BearerSubject,
    },
    /// A credential this runner issued that is revoked, expired, or superseded.
    InactiveCredential {
        revoked: bool,
        subject: BearerSubject,
    },
    /// Matches no grant or credential this runner has issued.
    Unknown,
}

/// The enrolled machine a grant or credential belongs to, as far as the
/// runner's records name it. A grant minted for a machine the runner has not
/// enrolled yet names nobody.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BearerSubject {
    pub enrolled_endpoint_id: Option<String>,
    pub executor_name: Option<String>,
}

impl std::fmt::Display for BearerSubject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.executor_name, &self.enrolled_endpoint_id) {
            (Some(name), Some(id)) => write!(f, "{name} ({id})"),
            (None, Some(id)) => write!(f, "{id}"),
            (Some(name), None) => write!(f, "{name}"),
            (None, None) => write!(f, "an unnamed machine"),
        }
    }
}

fn utc(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|at| at.to_rfc3339())
        .unwrap_or_else(|| timestamp.to_string())
}

impl std::fmt::Display for UnrecognizedBearer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConsumedGrant {
                consumed_at,
                subject,
            } => write!(
                f,
                "bearer is an enrollment grant for {subject} already spent at {}; that machine should present the credential the grant bought (restarting its daemon reloads it)",
                utc(*consumed_at)
            ),
            Self::ExpiredGrant {
                expires_at,
                subject,
            } => write!(
                f,
                "bearer is an enrollment grant for {subject} that expired unused at {}",
                utc(*expires_at)
            ),
            Self::InactiveCredential { revoked, subject } => write!(
                f,
                "bearer is a {} credential for {subject}",
                if *revoked {
                    "revoked"
                } else {
                    "expired or superseded"
                }
            ),
            Self::Unknown => {
                write!(f, "bearer matches no grant or credential this runner issued")
            }
        }
    }
}

pub async fn describe_unrecognized_bearer(
    db: &LocalDb,
    kind: DeviceKind,
    raw: &str,
) -> UnrecognizedBearer {
    let kind = kind.as_str();
    let digest = hash(raw);
    let now = chrono::Utc::now().timestamp();
    db.read(|conn| { let digest=digest.clone(); Box::pin(async move {
        let mut rows=conn.query("SELECT g.consumed_at,g.expires_at,g.expected_enrolled_endpoint_id,e.executor_name FROM executor_enrollment_grants g LEFT JOIN executor_enrollments e ON e.device_kind=g.device_kind AND e.enrolled_endpoint_id=g.expected_enrolled_endpoint_id WHERE g.token_hash=?1 AND g.device_kind=?2", params![digest.clone(), kind]).await?;
        if let Some(row)=rows.next().await? {
            let subject=BearerSubject{enrolled_endpoint_id:row.opt_text(2)?,executor_name:row.opt_text(3)?};
            let expires_at=row.i64(1)?;
            match row.opt_i64(0)? {
                Some(consumed_at) => return Ok(UnrecognizedBearer::ConsumedGrant{consumed_at,subject}),
                None if expires_at<=now => return Ok(UnrecognizedBearer::ExpiredGrant{expires_at,subject}),
                // Still live: admission raced its consumption or expiry.
                None => {}
            }
        }
        let mut rows=conn.query("SELECT enrolled_endpoint_id,executor_name,revoked_at FROM executor_enrollments WHERE device_kind=?2 AND (credential_hash=?1 OR previous_credential_hash=?1 OR superseded_credential_hash=?1) LIMIT 1", params![digest, kind]).await?;
        if let Some(row)=rows.next().await? {
            return Ok(UnrecognizedBearer::InactiveCredential{revoked:row.opt_i64(2)?.is_some(),subject:BearerSubject{enrolled_endpoint_id:Some(row.text(0)?),executor_name:row.opt_text(1)?}});
        }
        Ok(UnrecognizedBearer::Unknown)
    })}).await.unwrap_or(UnrecognizedBearer::Unknown)
}

pub async fn recover_binding_credential(
    db: &LocalDb,
    kind: DeviceKind,
    presented: &str,
    binding: &DeviceBinding,
    runner_device_id: &str,
) -> Result<Option<(String, RecoverySource)>, EnrollmentRejectionReason> {
    let kind = kind.as_str();
    let presented_hash = hash(presented);
    let replacement = token();
    let replacement_hash = hash(&replacement);
    let now = chrono::Utc::now().timestamp();
    let expires_at = now + CREDENTIAL_LIFETIME_SECONDS;
    let endpoint = binding.enrolled_endpoint_id.clone();
    let device = binding.device_id.clone();
    let runner = runner_device_id.to_string();
    let source = db.write(|conn| {
        let replacement_hash = replacement_hash.clone();
        let presented_hash = presented_hash.clone();
        let endpoint = endpoint.clone();
        let device = device.clone();
        let runner = runner.clone();
        Box::pin(async move {
            // The SET expressions all read the pre-update row. In particular,
            // COALESCE classifies an older unconfirmed predecessor as superseded
            // before the current credential takes its place in the previous slot.
            let changed = conn.execute("UPDATE executor_enrollments SET superseded_credential_hash=COALESCE(previous_credential_hash,superseded_credential_hash),superseded_at=CASE WHEN previous_credential_hash IS NOT NULL THEN ?3 ELSE superseded_at END,previous_credential_hash=credential_hash,previous_expires_at=expires_at,credential_hash=?1,expires_at=?2,updated_at=?3 WHERE credential_hash=?4 AND device_kind=?8 AND enrolled_endpoint_id=?5 AND device_id=?6 AND runner_device_id=?7 AND revoked_at IS NULL", params![replacement_hash.clone(),expires_at,now,presented_hash.clone(),endpoint.clone(),device.clone(),runner.clone(),kind]).await?;
            if changed == 1 {
                return Ok(Some(RecoverySource::Current));
            }
            // A previous credential means the active replacement was never
            // confirmed received. Discard that replacement, but keep the
            // predecessor acceptable until this new delivery is acknowledged.
            let changed = conn.execute("UPDATE executor_enrollments SET credential_hash=?1,expires_at=?2,updated_at=?3 WHERE previous_credential_hash=?4 AND device_kind=?8 AND enrolled_endpoint_id=?5 AND device_id=?6 AND runner_device_id=?7 AND revoked_at IS NULL", params![replacement_hash,expires_at,now,presented_hash,endpoint,device,runner,kind]).await?;
            Ok((changed == 1).then_some(RecoverySource::Previous))
        })
    }).await.map_err(|_| EnrollmentRejectionReason::Unenrolled)?;
    Ok(source.map(|source| (replacement, source)))
}

impl DeviceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Executor => "executor",
            Self::Companion => "companion",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceBinding {
    pub enrolled_endpoint_id: String,
    pub device_id: String,
}

const CREDENTIAL_LIFETIME_SECONDS: i64 = 7 * 24 * 60 * 60;
// Rotate once a credential is past half its life. A narrower window makes
// refresh a lottery: a machine only rotates if it happens to be connected
// during it, and a fleet of laptops loses that lottery routinely.
const CREDENTIAL_ROTATE_BEFORE_SECONDS: i64 = CREDENTIAL_LIFETIME_SECONDS / 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoverySource {
    Current,
    Previous,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcceptOutcome {
    Reattached,
    Enrolled(String),
    Recovered(String, RecoverySource),
}

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
    kind: DeviceKind,
    runner_device_id: &str,
    expected_enrolled_endpoint_id: Option<&str>,
    expected_device_id: Option<&str>,
    ttl_seconds: i64,
) -> Result<String, String> {
    let kind = kind.as_str();
    let raw = token();
    let token_hash = hash(&raw);
    let runner = runner_device_id.to_string();
    let endpoint = expected_enrolled_endpoint_id.map(str::to_owned);
    let device = expected_device_id.map(str::to_owned);
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| { let token_hash=token_hash.clone(); let runner=runner.clone(); let endpoint=endpoint.clone(); let device=device.clone(); Box::pin(async move {
        conn.execute("INSERT INTO executor_enrollment_grants (token_hash,device_kind,runner_device_id,expected_enrolled_endpoint_id,expected_device_id,expires_at,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7)", params![token_hash,kind,runner,endpoint,device,now+ttl_seconds,now]).await?; Ok(())
    })}).await.map_err(|e| e.to_string())?;
    Ok(raw)
}

pub async fn token_is_known(db: &LocalDb, kind: DeviceKind, raw: &str) -> bool {
    let kind = kind.as_str();
    let digest = hash(raw);
    db.read(|conn| { let digest=digest.clone(); Box::pin(async move {
        let mut rows=conn.query("SELECT 1 FROM executor_enrollment_grants WHERE token_hash=?1 AND device_kind=?2 AND consumed_at IS NULL AND expires_at>?3 UNION ALL SELECT 1 FROM executor_enrollments WHERE device_kind=?2 AND revoked_at IS NULL AND ((credential_hash=?1 AND expires_at>?3) OR (previous_credential_hash=?1 AND previous_expires_at>?3)) LIMIT 1", params![digest, kind, chrono::Utc::now().timestamp()]).await?;
        Ok(rows.next().await?.is_some())
    })}).await.unwrap_or(false)
}

/// Mint a recovery grant only while the exact runner-owned machine remains
/// actively enrolled. The authority check and grant insertion share one write
/// transaction; `revoke` invalidates any grant inserted before its own write, so
/// revocation wins regardless of which operation reaches the database first.
pub async fn create_recovery_grant(
    db: &LocalDb,
    kind: DeviceKind,
    runner_device_id: &str,
    enrolled_endpoint_id: &str,
    device_id: &str,
    ttl_seconds: i64,
) -> Result<Option<String>, String> {
    let kind = kind.as_str();
    let raw = token();
    let token_hash = hash(&raw);
    let runner = runner_device_id.to_string();
    let endpoint = enrolled_endpoint_id.to_string();
    let device = device_id.to_string();
    let now = chrono::Utc::now().timestamp();
    let created = db.write(|conn| {
        let token_hash = token_hash.clone();
        let runner = runner.clone();
        let endpoint = endpoint.clone();
        let device = device.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT 1 FROM executor_enrollments WHERE device_kind=?4 AND runner_device_id=?1 AND enrolled_endpoint_id=?2 AND device_id=?3 AND revoked_at IS NULL LIMIT 1",
                params![runner.clone(), endpoint.clone(), device.clone(), kind],
            ).await?;
            if rows.next().await?.is_none() {
                return Ok(false);
            }
            drop(rows);
            conn.execute(
                "INSERT INTO executor_enrollment_grants (token_hash,device_kind,runner_device_id,expected_enrolled_endpoint_id,expected_device_id,expires_at,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![token_hash, kind, runner, endpoint, device, now + ttl_seconds, now],
            ).await?;
            Ok(true)
        })
    }).await.map_err(|error| error.to_string())?;
    Ok(created.then_some(raw))
}

pub async fn accept(
    db: &LocalDb,
    kind: DeviceKind,
    bearer: &str,
    enrollment: &ExecutorEnrollmentIdentity,
    identity: &ExecutorIdentity,
    runner_device_id: &str,
) -> Result<AcceptOutcome, EnrollmentRejectionReason> {
    match enrollment {
        // Which launch a colocated Hello is, the nonce, is the transport's
        // admission question; the bearer already proved it is this runner's.
        ExecutorEnrollmentIdentity::Colocated { .. } => Ok(AcceptOutcome::Reattached),
        ExecutorEnrollmentIdentity::Grant {
            token,
            expected_runner_device_id,
        } => {
            if token != bearer || expected_runner_device_id != runner_device_id {
                return Err(EnrollmentRejectionReason::RunnerIdentityMismatch);
            }
            accept_grant_with_name(
                db,
                kind,
                token,
                &DeviceBinding {
                    enrolled_endpoint_id: identity.executor_id.clone(),
                    device_id: identity.device_id.clone(),
                },
                runner_device_id,
                Some(&identity.display_name),
            )
            .await
            .map(AcceptOutcome::Enrolled)
        }
        ExecutorEnrollmentIdentity::Credential {
            credential,
            expected_runner_device_id,
        } => {
            if credential != bearer || expected_runner_device_id != runner_device_id {
                return Err(EnrollmentRejectionReason::RunnerIdentityMismatch);
            }
            let binding = DeviceBinding {
                enrolled_endpoint_id: identity.executor_id.clone(),
                device_id: identity.device_id.clone(),
            };
            let recovered =
                match validate_binding_credential(db, kind, credential, &binding, runner_device_id)
                    .await
                {
                    Ok(()) => None,
                    Err(EnrollmentRejectionReason::Expired) => {
                        match recover_binding_credential(
                            db,
                            kind,
                            credential,
                            &binding,
                            runner_device_id,
                        )
                        .await?
                        {
                            Some(recovered) => Some(recovered),
                            None => {
                                return Err(
                                    match validate_binding_credential(
                                        db,
                                        kind,
                                        credential,
                                        &binding,
                                        runner_device_id,
                                    )
                                    .await
                                    {
                                        Err(reason) => reason,
                                        Ok(()) => EnrollmentRejectionReason::Unenrolled,
                                    },
                                );
                            }
                        }
                    }
                    Err(reason) => return Err(reason),
                };
            claim_executor_name(db, identity).await?;
            Ok(match recovered {
                Some((credential, source)) => AcceptOutcome::Recovered(credential, source),
                None => AcceptOutcome::Reattached,
            })
        }
    }
}

/// Bind this executor's public name to its enrollment, or confirm it still
/// holds it.
///
/// A public name is an address: every placement request is written in one, so a
/// second machine answering to a name already in use would route work to the
/// wrong host with nothing to see. The read of the current holder and the write
/// of the claim share one transaction, so two executors presenting the same name
/// at once cannot both win.
///
/// A reconnecting executor that has changed its advertised label simply moves
/// its own claim — the row is keyed by executor identity, and an operator
/// renaming a machine is exactly that case.
async fn claim_executor_name(
    db: &LocalDb,
    identity: &ExecutorIdentity,
) -> Result<(), EnrollmentRejectionReason> {
    let Some(name) = normalize_executor_name(&identity.display_name) else {
        return Err(EnrollmentRejectionReason::MalformedAdvertisement);
    };
    let endpoint = identity.executor_id.clone();
    let holder = db
        .write(|conn| {
            let name = name.clone();
            let endpoint = endpoint.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT enrolled_endpoint_id FROM executor_enrollments WHERE executor_name=?1 AND revoked_at IS NULL AND enrolled_endpoint_id<>?2",
                        params![name.clone(), endpoint.clone()],
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    return Ok(Some(row.text(0)?));
                }
                drop(rows);
                conn.execute(
                    "UPDATE executor_enrollments SET executor_name=?1, updated_at=?3 WHERE enrolled_endpoint_id=?2 AND revoked_at IS NULL",
                    params![name, endpoint, chrono::Utc::now().timestamp()],
                )
                .await?;
                Ok(None)
            })
        })
        .await
        .map_err(|_| EnrollmentRejectionReason::Unenrolled)?;
    match holder {
        Some(holder) => Err(EnrollmentRejectionReason::NameConflict { name, holder }),
        None => Ok(()),
    }
}

async fn accept_grant_with_name(
    db: &LocalDb,
    kind: DeviceKind,
    raw: &str,
    binding: &DeviceBinding,
    runner_device_id: &str,
    executor_display_name: Option<&str>,
) -> Result<String, EnrollmentRejectionReason> {
    let kind = kind.as_str();
    let digest = hash(raw);
    let now = chrono::Utc::now().timestamp();
    let grant_binding = db.read(|conn| { let digest=digest.clone(); Box::pin(async move {
        let mut rows=conn.query("SELECT runner_device_id,expected_enrolled_endpoint_id,expected_device_id,expires_at FROM executor_enrollment_grants WHERE token_hash=?1 AND device_kind=?2 AND consumed_at IS NULL", params![digest, kind]).await?;
        let Some(row)=rows.next().await? else { return Ok(None) };
        Ok(Some((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?, row.i64(3)?)))
    })}).await.map_err(|_| EnrollmentRejectionReason::Unenrolled)?;
    let Some((runner, endpoint, device, expires)) = grant_binding else {
        return Err(EnrollmentRejectionReason::Unenrolled);
    };
    if expires <= now {
        return Err(EnrollmentRejectionReason::Expired);
    }
    if runner != runner_device_id
        || endpoint
            .as_ref()
            .is_some_and(|v| v != &binding.enrolled_endpoint_id)
        || device.as_ref().is_some_and(|v| v != &binding.device_id)
    {
        return Err(EnrollmentRejectionReason::IdentityMismatch);
    }
    // The public name is settled in the SAME transaction that spends the grant
    // and writes the enrollment. Checking it afterwards would burn a one-time
    // grant and leave a nameless active enrollment behind for a machine that was
    // refused — an executor that can never attach and an operator who cannot
    // retry without minting a new grant.
    let name = match kind {
        "executor" => Some(
            normalize_executor_name(executor_display_name.unwrap_or_default())
                .ok_or(EnrollmentRejectionReason::MalformedAdvertisement)?,
        ),
        "companion" => None,
        _ => unreachable!(),
    };
    let credential = token();
    let credential_hash = hash(&credential);
    let binding = binding.clone();
    let runner = runner_device_id.to_string();
    let credential_expires = now + CREDENTIAL_LIFETIME_SECONDS;
    let outcome = db.write(|conn| { let digest=digest.clone(); let credential_hash=credential_hash.clone(); let binding=binding.clone(); let runner=runner.clone(); let name=name.clone(); Box::pin(async move {
        // Read the holder before anything is written, so the refusal path
        // commits an empty transaction rather than a half-finished enrollment.
        if let Some(name) = name.as_ref() {
            let mut rows = conn.query("SELECT enrolled_endpoint_id FROM executor_enrollments WHERE device_kind='executor' AND executor_name=?1 AND revoked_at IS NULL AND enrolled_endpoint_id<>?2", params![name.clone(), binding.enrolled_endpoint_id.clone()]).await?;
            if let Some(row) = rows.next().await? {
                return Ok(GrantOutcome::NameHeldBy(row.text(0)?));
            }
        }
        let changed = conn.execute("UPDATE executor_enrollment_grants SET consumed_at=?3 WHERE token_hash=?1 AND device_kind=?2 AND consumed_at IS NULL", params![digest,kind,now]).await?;
        if changed != 1 { return Ok(GrantOutcome::GrantLost); }
        conn.execute("INSERT INTO executor_enrollments (device_kind,enrolled_endpoint_id,device_id,runner_device_id,credential_hash,enrolled_at,expires_at,updated_at,executor_name) VALUES (?1,?2,?3,?4,?5,?6,?7,?6,?8) ON CONFLICT(device_kind,enrolled_endpoint_id) DO UPDATE SET device_id=excluded.device_id,runner_device_id=excluded.runner_device_id,credential_hash=excluded.credential_hash,enrolled_at=excluded.enrolled_at,expires_at=excluded.expires_at,revoked_at=NULL,updated_at=excluded.updated_at,executor_name=excluded.executor_name", params![kind,binding.enrolled_endpoint_id,binding.device_id,runner,credential_hash,now,credential_expires,name]).await?; Ok(GrantOutcome::Enrolled)
    })}).await.map_err(|_| EnrollmentRejectionReason::Unenrolled)?;
    match outcome {
        GrantOutcome::Enrolled => Ok(credential),
        GrantOutcome::GrantLost => Err(EnrollmentRejectionReason::Unenrolled),
        GrantOutcome::NameHeldBy(holder) => Err(EnrollmentRejectionReason::NameConflict {
            name: name.expect("name conflict only applies to executors"),
            holder,
        }),
    }
}

pub async fn accept_grant(
    db: &LocalDb,
    kind: DeviceKind,
    raw: &str,
    binding: &DeviceBinding,
    runner_device_id: &str,
) -> Result<String, EnrollmentRejectionReason> {
    accept_grant_with_name(db, kind, raw, binding, runner_device_id, None).await
}

/// What the one transaction that spends a grant decided.
enum GrantOutcome {
    Enrolled,
    /// The grant was consumed by someone else between the read and the write.
    GrantLost,
    /// Another live machine already answers to this public name.
    NameHeldBy(String),
}

pub async fn validate_binding_credential(
    db: &LocalDb,
    kind: DeviceKind,
    raw: &str,
    binding: &DeviceBinding,
    runner: &str,
) -> Result<(), EnrollmentRejectionReason> {
    let kind = kind.as_str();
    let digest = hash(raw);
    let endpoint = binding.enrolled_endpoint_id.clone();
    let device = binding.device_id.clone();
    let runner = runner.to_string();
    let now = chrono::Utc::now().timestamp();
    let state = db.read(|conn| Box::pin(async move {
        let mut rows=conn.query("SELECT revoked_at,credential_hash,previous_credential_hash,superseded_credential_hash,expires_at,previous_expires_at FROM executor_enrollments WHERE (credential_hash=?1 OR previous_credential_hash=?1 OR superseded_credential_hash=?1) AND device_kind=?5 AND enrolled_endpoint_id=?2 AND device_id=?3 AND runner_device_id=?4", params![digest.clone(),endpoint,device,runner,kind]).await?;
        let Some(row) = rows.next().await? else { return Ok(None) };
        let current_hash = row.text(1)?;
        let previous_hash = row.opt_text(2)?;
        let superseded_hash = row.opt_text(3)?;
        let expiry = if current_hash == digest {
            Some(row.i64(4)?)
        } else if previous_hash.as_deref() == Some(digest.as_str()) {
            row.opt_i64(5)?
        } else {
            None
        };
        Ok(Some((
            row.opt_i64(0)?,
            superseded_hash.as_deref() == Some(digest.as_str()),
            expiry,
        )))
    })).await.map_err(|_| EnrollmentRejectionReason::Unenrolled)?
        .ok_or(EnrollmentRejectionReason::IdentityMismatch)?;
    if state.0.is_some() {
        Err(EnrollmentRejectionReason::Revoked)
    } else if state.1 {
        Err(EnrollmentRejectionReason::Superseded)
    } else if state.2.is_some_and(|expires| expires <= now) {
        Err(EnrollmentRejectionReason::Expired)
    } else {
        Ok(())
    }
}

pub async fn rotate_binding_credential(
    db: &LocalDb,
    kind: DeviceKind,
    current: &str,
    binding: &DeviceBinding,
    runner_device_id: &str,
) -> Result<Option<(String, i64)>, EnrollmentRejectionReason> {
    let kind = kind.as_str();
    let current_hash = hash(current);
    let replacement = token();
    let replacement_hash = hash(&replacement);
    let now = chrono::Utc::now().timestamp();
    let expires_at = now + CREDENTIAL_LIFETIME_SECONDS;
    let rotate_by = now + CREDENTIAL_ROTATE_BEFORE_SECONDS;
    let endpoint = binding.enrolled_endpoint_id.clone();
    let device = binding.device_id.clone();
    let runner = runner_device_id.to_string();
    let changed = db.write(|conn| {
        let replacement_hash = replacement_hash.clone();
        let current_hash = current_hash.clone();
        let endpoint = endpoint.clone();
        let device = device.clone();
        let runner = runner.clone();
        Box::pin(async move {
            // SQLite evaluates every SET expression against the pre-update row,
            // so the COALESCE retains the stale previous hash before that slot is
            // replaced by the current credential below.
            let changed = conn.execute("UPDATE executor_enrollments SET superseded_credential_hash=COALESCE(previous_credential_hash,superseded_credential_hash),superseded_at=CASE WHEN previous_credential_hash IS NOT NULL THEN ?3 ELSE superseded_at END,previous_credential_hash=credential_hash,previous_expires_at=expires_at,credential_hash=?1,expires_at=?2,updated_at=?3 WHERE credential_hash=?4 AND (previous_credential_hash IS NULL OR previous_expires_at<=?3) AND device_kind=?9 AND enrolled_endpoint_id=?5 AND device_id=?6 AND runner_device_id=?7 AND revoked_at IS NULL AND expires_at>?3 AND expires_at<=?8", params![replacement_hash.clone(),expires_at,now,current_hash.clone(),endpoint.clone(),device.clone(),runner.clone(),rotate_by,kind]).await?;
            if changed == 1 {
                return Ok(changed);
            }
            // A reconnect may present the bounded previous credential after the
            // active replacement was lost in transit. Since raw active secrets
            // are never stored, replace that undelivered active hash with a new
            // one while preserving the same previous credential and expiry.
            let recovered = conn.execute("UPDATE executor_enrollments SET credential_hash=?1,expires_at=?2,updated_at=?3 WHERE previous_credential_hash=?4 AND previous_expires_at>?3 AND device_kind=?8 AND enrolled_endpoint_id=?5 AND device_id=?6 AND runner_device_id=?7 AND revoked_at IS NULL", params![replacement_hash,expires_at,now,current_hash,endpoint,device,runner,kind]).await?;
            Ok(recovered)
        })
    }).await.map_err(|_| EnrollmentRejectionReason::Unenrolled)?;
    Ok((changed == 1).then_some((replacement, expires_at)))
}

pub async fn confirm_binding_credential(
    db: &LocalDb,
    kind: DeviceKind,
    credential: &str,
    binding: &DeviceBinding,
    runner_device_id: &str,
) -> Result<bool, String> {
    let kind = kind.as_str();
    let credential_hash = hash(credential);
    let endpoint = binding.enrolled_endpoint_id.clone();
    let device = binding.device_id.clone();
    let runner = runner_device_id.to_string();
    db.write(|conn| {
        let credential_hash = credential_hash.clone();
        let endpoint = endpoint.clone();
        let device = device.clone();
        let runner = runner.clone();
        Box::pin(async move {
            let changed = conn.execute("UPDATE executor_enrollments SET superseded_credential_hash=COALESCE(previous_credential_hash,superseded_credential_hash),superseded_at=CASE WHEN previous_credential_hash IS NOT NULL THEN unixepoch() ELSE superseded_at END,previous_credential_hash=NULL,previous_expires_at=NULL WHERE credential_hash=?1 AND device_kind=?5 AND enrolled_endpoint_id=?2 AND device_id=?3 AND runner_device_id=?4 AND revoked_at IS NULL", params![credential_hash,endpoint,device,runner,kind]).await?;
            Ok(changed == 1)
        })
    }).await.map_err(|error| error.to_string())
}

pub async fn rotate_credential(
    db: &LocalDb,
    kind: DeviceKind,
    current: &str,
    identity: &ExecutorIdentity,
    runner_device_id: &str,
) -> Result<Option<(String, i64)>, EnrollmentRejectionReason> {
    rotate_binding_credential(
        db,
        kind,
        current,
        &DeviceBinding {
            enrolled_endpoint_id: identity.executor_id.clone(),
            device_id: identity.device_id.clone(),
        },
        runner_device_id,
    )
    .await
}

pub async fn confirm_rotated_credential(
    db: &LocalDb,
    kind: DeviceKind,
    credential: &str,
    identity: &ExecutorIdentity,
    runner_device_id: &str,
) -> Result<bool, String> {
    confirm_binding_credential(
        db,
        kind,
        credential,
        &DeviceBinding {
            enrolled_endpoint_id: identity.executor_id.clone(),
            device_id: identity.device_id.clone(),
        },
        runner_device_id,
    )
    .await
}

/// Move an enrolled machine's public-name claim.
///
/// An operator rename has to land here as well as in configuration: the claim is
/// what makes a name an exclusive address, so leaving it on the old value would
/// have the machine's own reconnection refused as a second claimant of a name it
/// no longer uses.
pub async fn rename(
    db: &LocalDb,
    enrolled_endpoint_id: &str,
    new_name: &str,
) -> Result<(), String> {
    let name = normalize_executor_name(new_name)
        .ok_or_else(|| format!("{new_name:?} is not a usable executor name"))?;
    let endpoint = enrolled_endpoint_id.to_string();
    let holder = db
        .write(|conn| {
            let name = name.clone();
            let endpoint = endpoint.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT enrolled_endpoint_id FROM executor_enrollments WHERE executor_name=?1 AND revoked_at IS NULL AND enrolled_endpoint_id<>?2",
                        params![name.clone(), endpoint.clone()],
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    return Ok(Some(row.text(0)?));
                }
                drop(rows);
                conn.execute(
                    "UPDATE executor_enrollments SET executor_name=?1, updated_at=?3 WHERE enrolled_endpoint_id=?2 AND revoked_at IS NULL",
                    params![name, endpoint, chrono::Utc::now().timestamp()],
                )
                .await?;
                Ok(None)
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    match holder {
        Some(holder) => Err(format!(
            "the public name {name} already addresses enrolled executor {holder}"
        )),
        None => Ok(()),
    }
}

pub async fn revoke(
    db: &LocalDb,
    kind: DeviceKind,
    enrolled_endpoint_id: &str,
    device_id: &str,
    runner_device_id: &str,
) -> Result<bool, String> {
    let kind = kind.as_str();
    let now = chrono::Utc::now().timestamp();
    let endpoint = enrolled_endpoint_id.to_string();
    let device = device_id.to_string();
    let runner = runner_device_id.to_string();
    db.write(|conn| {
        let endpoint = endpoint.clone();
        let device = device.clone();
        let runner = runner.clone();
        Box::pin(async move {
            let changed = conn.execute("UPDATE executor_enrollments SET revoked_at=?1,updated_at=?1 WHERE device_kind=?5 AND enrolled_endpoint_id=?2 AND device_id=?3 AND runner_device_id=?4 AND revoked_at IS NULL", params![now,endpoint.clone(),device.clone(),runner.clone(),kind]).await?;
            if changed == 1 {
                conn.execute(
                    "DELETE FROM executor_enrollment_grants WHERE device_kind=?4 AND runner_device_id=?1 AND expected_enrolled_endpoint_id=?2 AND expected_device_id=?3 AND consumed_at IS NULL",
                    params![runner, endpoint, device, kind],
                ).await?;
            }
            Ok(changed == 1)
        })
    }).await.map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn db() -> LocalDb {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = LocalDb::open_migrated_for_test(dir.join("private.db"))
            .await
            .unwrap();
        db
    }

    fn enrolled(outcome: AcceptOutcome) -> String {
        match outcome {
            AcceptOutcome::Enrolled(credential) => credential,
            other => panic!("expected enrollment, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_refused_bearer_is_named_by_what_it_is() {
        let db = db().await;
        let grant = create_grant(
            &db,
            DeviceKind::Executor,
            "runner",
            Some("executor"),
            Some("device"),
            60,
        )
        .await
        .unwrap();
        let identity = ExecutorIdentity {
            device_id: "device".into(),
            executor_id: "executor".into(),
            display_name: "Executor".into(),
        };
        let credential = enrolled(
            accept(
                &db,
                DeviceKind::Executor,
                &grant,
                &ExecutorEnrollmentIdentity::Grant {
                    token: grant.clone(),
                    expected_runner_device_id: "runner".into(),
                },
                &identity,
                "runner",
            )
            .await
            .unwrap(),
        );

        // The spent grant is refused at the handshake and named as a replay
        // by the machine it enrolled, not as a stranger.
        assert!(!token_may_open_handshake(&db, DeviceKind::Executor, &grant).await);
        let UnrecognizedBearer::ConsumedGrant { subject, .. } =
            describe_unrecognized_bearer(&db, DeviceKind::Executor, &grant).await
        else {
            panic!("a spent grant is classified as consumed");
        };
        assert_eq!(subject.enrolled_endpoint_id.as_deref(), Some("executor"));
        assert!(subject.executor_name.is_some());

        let stale = create_grant(&db, DeviceKind::Executor, "runner", None, None, -1)
            .await
            .unwrap();
        assert!(matches!(
            describe_unrecognized_bearer(&db, DeviceKind::Executor, &stale).await,
            UnrecognizedBearer::ExpiredGrant {
                subject: BearerSubject {
                    enrolled_endpoint_id: None,
                    executor_name: None
                },
                ..
            }
        ));

        assert!(
            revoke(&db, DeviceKind::Executor, "executor", "device", "runner")
                .await
                .unwrap()
        );
        assert!(!token_is_known(&db, DeviceKind::Executor, &credential).await);
        assert!(matches!(
            describe_unrecognized_bearer(&db, DeviceKind::Executor, &credential).await,
            UnrecognizedBearer::InactiveCredential { revoked: true, .. }
        ));

        assert_eq!(
            describe_unrecognized_bearer(&db, DeviceKind::Executor, "never-issued").await,
            UnrecognizedBearer::Unknown
        );
        // Kinds never answer for each other.
        assert_eq!(
            describe_unrecognized_bearer(&db, DeviceKind::Companion, &grant).await,
            UnrecognizedBearer::Unknown
        );
    }

    #[tokio::test]
    async fn grant_is_one_time_and_credential_reconnects() {
        let db = db().await;
        let grant = create_grant(
            &db,
            DeviceKind::Executor,
            "runner",
            Some("executor"),
            Some("device"),
            60,
        )
        .await
        .unwrap();
        assert!(token_is_known(&db, DeviceKind::Executor, &grant).await);
        let identity = ExecutorIdentity {
            device_id: "device".into(),
            executor_id: "executor".into(),
            display_name: "Executor".into(),
        };
        let credential = enrolled(
            accept(
                &db,
                DeviceKind::Executor,
                &grant,
                &ExecutorEnrollmentIdentity::Grant {
                    token: grant.clone(),
                    expected_runner_device_id: "runner".into(),
                },
                &identity,
                "runner",
            )
            .await
            .unwrap(),
        );
        assert!(!token_is_known(&db, DeviceKind::Executor, &grant).await);
        assert!(token_is_known(&db, DeviceKind::Executor, &credential).await);
        accept(
            &db,
            DeviceKind::Executor,
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
    async fn a_lapsed_credential_is_re_granted_for_an_active_enrollment() {
        let db = db().await;
        let identity = ExecutorIdentity {
            device_id: "device".into(),
            executor_id: "executor".into(),
            display_name: "Executor".into(),
        };
        let grant = create_grant(
            &db,
            DeviceKind::Executor,
            "runner",
            Some("executor"),
            Some("device"),
            60,
        )
        .await
        .unwrap();
        let credential = enrolled(
            accept(
                &db,
                DeviceKind::Executor,
                &grant,
                &ExecutorEnrollmentIdentity::Grant {
                    token: grant.clone(),
                    expected_runner_device_id: "runner".into(),
                },
                &identity,
                "runner",
            )
            .await
            .unwrap(),
        );
        let digest = hash(&credential);
        db.execute(
            "UPDATE executor_enrollments SET expires_at=0 WHERE credential_hash=?1",
            (digest.as_str(),),
        )
        .await
        .unwrap();
        assert!(!token_is_known(&db, DeviceKind::Executor, &credential).await);
        let outcome = accept(
            &db,
            DeviceKind::Executor,
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
        let AcceptOutcome::Recovered(replacement, RecoverySource::Current) = outcome else {
            panic!("expected current-credential recovery, got {outcome:?}");
        };
        assert_ne!(replacement, credential);
        let rows = db.query_all(
            "SELECT credential_hash,previous_credential_hash FROM executor_enrollments WHERE enrolled_endpoint_id='executor'",
            (),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        ).await.unwrap();
        assert_eq!(rows, vec![(hash(&replacement), Some(hash(&credential)))]);
    }

    async fn accept_credential(
        db: &LocalDb,
        credential: &str,
        identity: &ExecutorIdentity,
    ) -> Result<AcceptOutcome, EnrollmentRejectionReason> {
        accept(
            db,
            DeviceKind::Executor,
            credential,
            &ExecutorEnrollmentIdentity::Credential {
                credential: credential.into(),
                expected_runner_device_id: "runner".into(),
            },
            identity,
            "runner",
        )
        .await
    }

    async fn expire(db: &LocalDb, credential: &str) {
        db.execute(
            "UPDATE executor_enrollments SET expires_at=0 WHERE credential_hash=?1",
            (hash(credential),),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_lapsed_credential_is_refused_for_a_revoked_enrollment() {
        let db = db().await;
        let credential = enroll(&db, "remote-a", "fedora").await.unwrap();
        expire(&db, &credential).await;
        revoke(
            &db,
            DeviceKind::Executor,
            "remote-a",
            "remote-a-device",
            "runner",
        )
        .await
        .unwrap();

        assert_eq!(
            accept_credential(
                &db,
                &credential,
                &ExecutorIdentity {
                    device_id: "remote-a-device".into(),
                    executor_id: "remote-a".into(),
                    display_name: "fedora".into(),
                },
            )
            .await,
            Err(EnrollmentRejectionReason::Revoked)
        );
    }

    #[tokio::test]
    async fn a_superseded_credential_is_refused_by_name() {
        let db = db().await;
        let identity = ExecutorIdentity {
            device_id: "remote-a-device".into(),
            executor_id: "remote-a".into(),
            display_name: "fedora".into(),
        };
        let original = enroll(&db, "remote-a", "fedora").await.unwrap();
        let now = chrono::Utc::now().timestamp();
        db.execute(
            "UPDATE executor_enrollments SET expires_at=?1 WHERE credential_hash=?2",
            params![now + 60, hash(&original)],
        )
        .await
        .unwrap();
        let (current, _) =
            rotate_credential(&db, DeviceKind::Executor, &original, &identity, "runner")
                .await
                .unwrap()
                .unwrap();
        assert!(confirm_rotated_credential(
            &db,
            DeviceKind::Executor,
            &current,
            &identity,
            "runner",
        )
        .await
        .unwrap());

        assert_eq!(
            accept_credential(&db, &original, &identity).await,
            Err(EnrollmentRejectionReason::Superseded)
        );
        let slots = db
            .query_all(
                "SELECT previous_credential_hash,superseded_credential_hash,superseded_at FROM executor_enrollments WHERE enrolled_endpoint_id='remote-a'",
                (),
                |row| Ok((row.opt_text(0)?, row.opt_text(1)?, row.opt_i64(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].0, None);
        assert_eq!(slots[0].1.as_deref(), Some(hash(&original).as_str()));
        assert!(slots[0].2.is_some());
    }

    #[tokio::test]
    async fn an_unknown_credential_does_not_claim_supersession() {
        let db = db().await;
        let identity = ExecutorIdentity {
            device_id: "remote-a-device".into(),
            executor_id: "remote-a".into(),
            display_name: "fedora".into(),
        };
        enroll(&db, "remote-a", "fedora").await.unwrap();
        assert_eq!(
            accept_credential(&db, "never-issued", &identity).await,
            Err(EnrollmentRejectionReason::IdentityMismatch)
        );
    }

    #[tokio::test]
    async fn a_lapsed_credential_from_another_machine_is_refused() {
        let db = db().await;
        let credential = enroll(&db, "remote-a", "fedora").await.unwrap();
        expire(&db, &credential).await;
        let other = ExecutorIdentity {
            device_id: "remote-b-device".into(),
            executor_id: "remote-b".into(),
            display_name: "other".into(),
        };
        assert_eq!(
            accept_credential(&db, &credential, &other).await,
            Err(EnrollmentRejectionReason::IdentityMismatch)
        );
    }

    #[tokio::test]
    async fn a_recovery_that_was_never_acknowledged_recovers_again() {
        let db = db().await;
        let identity = ExecutorIdentity {
            device_id: "remote-a-device".into(),
            executor_id: "remote-a".into(),
            display_name: "fedora".into(),
        };
        let lapsed = enroll(&db, "remote-a", "fedora").await.unwrap();
        expire(&db, &lapsed).await;
        let AcceptOutcome::Recovered(first, RecoverySource::Current) =
            accept_credential(&db, &lapsed, &identity).await.unwrap()
        else {
            panic!("first recovery did not use the current slot");
        };
        let AcceptOutcome::Recovered(second, RecoverySource::Previous) =
            accept_credential(&db, &lapsed, &identity).await.unwrap()
        else {
            panic!("retry did not recover through the unacknowledged previous slot");
        };
        assert_ne!(first, second);
        assert!(!token_may_open_handshake(&db, DeviceKind::Executor, &first).await);
        assert!(token_may_open_handshake(&db, DeviceKind::Executor, &lapsed).await);
    }

    #[tokio::test]
    async fn an_expired_unconfirmed_previous_does_not_block_rotation() {
        let db = db().await;
        let identity = ExecutorIdentity {
            device_id: "remote-a-device".into(),
            executor_id: "remote-a".into(),
            display_name: "fedora".into(),
        };
        let original = enroll(&db, "remote-a", "fedora").await.unwrap();
        let now = chrono::Utc::now().timestamp();
        db.execute(
            "UPDATE executor_enrollments SET expires_at=?1 WHERE credential_hash=?2",
            params![now + 60, hash(&original)],
        )
        .await
        .unwrap();
        let (current, _) =
            rotate_credential(&db, DeviceKind::Executor, &original, &identity, "runner")
                .await
                .unwrap()
                .unwrap();
        db.execute(
            "UPDATE executor_enrollments SET previous_expires_at=0,expires_at=?1 WHERE credential_hash=?2",
            params![now + 60, hash(&current)],
        )
        .await
        .unwrap();

        let (replacement, _) =
            rotate_credential(&db, DeviceKind::Executor, &current, &identity, "runner")
                .await
                .unwrap()
                .expect("an expired previous slot must not block the current credential");
        let slots = db
            .query_all(
                "SELECT credential_hash,previous_credential_hash,superseded_credential_hash FROM executor_enrollments WHERE enrolled_endpoint_id='remote-a'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            slots,
            vec![(
                hash(&replacement),
                Some(hash(&current)),
                Some(hash(&original))
            )]
        );
    }

    #[tokio::test]
    async fn acknowledging_a_credential_with_no_rotation_in_flight_is_accepted() {
        let db = db().await;
        let identity = ExecutorIdentity {
            device_id: "remote-a-device".into(),
            executor_id: "remote-a".into(),
            display_name: "fedora".into(),
        };
        let credential = enroll(&db, "remote-a", "fedora").await.unwrap();
        assert!(confirm_rotated_credential(
            &db,
            DeviceKind::Executor,
            &credential,
            &identity,
            "runner",
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn a_connected_machine_rotates_once_past_half_its_credential_life() {
        let db = db().await;
        let identity = ExecutorIdentity {
            device_id: "remote-a-device".into(),
            executor_id: "remote-a".into(),
            display_name: "fedora".into(),
        };
        let credential = enroll(&db, "remote-a", "fedora").await.unwrap();
        let now = chrono::Utc::now().timestamp();
        // Fedora last connected 29 hours before expiry: five hours before the old
        // 24-hour window, but well inside half of a seven-day credential life.
        let fedora_time_to_expiry = 29 * 60 * 60;
        assert!(fedora_time_to_expiry > 24 * 60 * 60);
        assert!(fedora_time_to_expiry <= CREDENTIAL_LIFETIME_SECONDS / 2);
        db.execute(
            "UPDATE executor_enrollments SET expires_at=?1 WHERE credential_hash=?2",
            params![now + fedora_time_to_expiry, hash(&credential)],
        )
        .await
        .unwrap();
        assert!(
            rotate_credential(&db, DeviceKind::Executor, &credential, &identity, "runner",)
                .await
                .unwrap()
                .is_some()
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
        let grant = create_grant(
            &db,
            DeviceKind::Executor,
            "runner",
            Some("executor"),
            Some("device"),
            60,
        )
        .await
        .unwrap();
        let credential = enrolled(
            accept(
                &db,
                DeviceKind::Executor,
                &grant,
                &ExecutorEnrollmentIdentity::Grant {
                    token: grant.clone(),
                    expected_runner_device_id: "runner".into(),
                },
                &identity,
                "runner",
            )
            .await
            .unwrap(),
        );
        let digest = hash(&credential);
        let now = chrono::Utc::now().timestamp();
        db.execute(
            "UPDATE executor_enrollments SET expires_at=?1 WHERE credential_hash=?2",
            params![now + 60, digest],
        )
        .await
        .unwrap();
        let (lost_replacement, _) =
            rotate_credential(&db, DeviceKind::Executor, &credential, &identity, "runner")
                .await
                .unwrap()
                .unwrap();
        assert!(token_is_known(&db, DeviceKind::Executor, &credential).await);
        // Simulate disconnect before delivery: reconnecting with the previous
        // credential mints a new recoverable active secret.
        let (replacement, _) =
            rotate_credential(&db, DeviceKind::Executor, &credential, &identity, "runner")
                .await
                .unwrap()
                .unwrap();
        assert_ne!(replacement, lost_replacement);
        assert!(!token_is_known(&db, DeviceKind::Executor, &lost_replacement).await);
        assert!(confirm_rotated_credential(
            &db,
            DeviceKind::Executor,
            &replacement,
            &identity,
            "runner"
        )
        .await
        .unwrap());
        assert!(!token_is_known(&db, DeviceKind::Executor, &credential).await);
        assert!(
            rotate_credential(&db, DeviceKind::Executor, &credential, &identity, "runner")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            revoke(&db, DeviceKind::Executor, "executor", "device", "runner")
                .await
                .unwrap()
        );
        assert!(
            rotate_credential(&db, DeviceKind::Executor, &replacement, &identity, "runner")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            accept(
                &db,
                DeviceKind::Executor,
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

    async fn enroll(
        db: &LocalDb,
        enrolled_endpoint_id: &str,
        display_name: &str,
    ) -> Result<String, EnrollmentRejectionReason> {
        let grant = create_grant(
            db,
            DeviceKind::Executor,
            "runner",
            Some(enrolled_endpoint_id),
            None,
            60,
        )
        .await
        .unwrap();
        let identity = ExecutorIdentity {
            device_id: format!("{enrolled_endpoint_id}-device"),
            executor_id: enrolled_endpoint_id.into(),
            display_name: display_name.into(),
        };
        accept(
            db,
            DeviceKind::Executor,
            &grant,
            &ExecutorEnrollmentIdentity::Grant {
                token: grant.clone(),
                expected_runner_device_id: "runner".into(),
            },
            &identity,
            "runner",
        )
        .await
        .map(enrolled)
    }

    async fn enrollment_count(db: &LocalDb, enrolled_endpoint_id: &str) -> i64 {
        let endpoint = enrolled_endpoint_id.to_string();
        db.read(|conn| {
            let endpoint = endpoint.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT COUNT(*) FROM executor_enrollments WHERE enrolled_endpoint_id=?1",
                        (endpoint.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .expect("COUNT always returns a row")
                    .i64(0)
            })
        })
        .await
        .unwrap()
    }

    async fn claimed_name(db: &LocalDb, enrolled_endpoint_id: &str) -> Option<String> {
        let endpoint = enrolled_endpoint_id.to_string();
        db.read(|conn| {
            let endpoint = endpoint.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT executor_name FROM executor_enrollments WHERE enrolled_endpoint_id=?1",
                        (endpoint.as_str(),),
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                row.opt_text(0)
            })
        })
        .await
        .unwrap()
    }

    /// The claim records the normalized address, not the operator's label: the
    /// label is what a person typed, and the address is what placement and
    /// `cairn://executors` both speak.
    #[tokio::test]
    async fn enrollment_claims_the_normalized_public_name() {
        let db = db().await;
        enroll(&db, "remote-a", "BGLab UB").await.unwrap();
        assert_eq!(
            claimed_name(&db, "remote-a").await.as_deref(),
            Some("bglab-ub")
        );
    }

    /// A second machine cannot take over an address that already routes work.
    /// The refusal names the address and the machine holding it, because an
    /// operator has to know which of the two to rename.
    ///
    /// The refusal must also cost nothing. A grant is one-time and an enrollment
    /// row is what lets a machine attach at all, so spending the first or
    /// creating the second on the way to saying "no" would leave the operator
    /// unable to retry and the fleet holding an enrollment for a machine it
    /// refused.
    #[tokio::test]
    async fn a_second_machine_cannot_claim_a_name_already_in_use() {
        let db = db().await;
        enroll(&db, "remote-a", "bglab-ub").await.unwrap();

        let grant = create_grant(
            &db,
            DeviceKind::Executor,
            "runner",
            Some("remote-b"),
            None,
            60,
        )
        .await
        .unwrap();
        let present = || ExecutorEnrollmentIdentity::Grant {
            token: grant.clone(),
            expected_runner_device_id: "runner".into(),
        };
        let identity = ExecutorIdentity {
            device_id: "remote-b-device".into(),
            executor_id: "remote-b".into(),
            display_name: "BGLab-UB".into(),
        };
        let rejection = accept(
            &db,
            DeviceKind::Executor,
            &grant,
            &present(),
            &identity,
            "runner",
        )
        .await
        .unwrap_err();

        assert_eq!(
            rejection,
            EnrollmentRejectionReason::NameConflict {
                name: "bglab-ub".into(),
                holder: "remote-a".into(),
            }
        );
        let diagnostic = rejection
            .diagnostic()
            .expect("a name conflict is actionable");
        assert!(diagnostic.contains("bglab-ub"), "{diagnostic}");
        assert!(diagnostic.contains("remote-a"), "{diagnostic}");
        assert!(diagnostic.contains("cairn executor rename"), "{diagnostic}");

        // Nothing was spent, and nothing was created.
        assert!(
            token_is_known(&db, DeviceKind::Executor, &grant).await,
            "a refused enrollment must leave its one-time grant unspent"
        );
        assert_eq!(
            enrollment_count(&db, "remote-b").await,
            0,
            "a refused enrollment must leave no row behind"
        );

        // So the operator retries with the same grant once the name is free.
        let renamed = ExecutorIdentity {
            display_name: "bglab-mac".into(),
            ..identity
        };
        accept(
            &db,
            DeviceKind::Executor,
            &grant,
            &present(),
            &renamed,
            "runner",
        )
        .await
        .expect("the unspent grant still enrolls under a free name");
        assert_eq!(
            claimed_name(&db, "remote-b").await.as_deref(),
            Some("bglab-mac")
        );
    }

    /// Reconnecting under a name you already hold is not a conflict with
    /// yourself, and a machine whose label changed moves its own claim rather
    /// than being locked out of the address it is about to use.
    #[tokio::test]
    async fn a_machine_keeps_and_may_move_its_own_claim() {
        let db = db().await;
        let credential = enroll(&db, "remote-a", "bglab-ub").await.unwrap();
        let identity = ExecutorIdentity {
            device_id: "remote-a-device".into(),
            executor_id: "remote-a".into(),
            display_name: "bglab-ub".into(),
        };
        accept(
            &db,
            DeviceKind::Executor,
            &credential,
            &ExecutorEnrollmentIdentity::Credential {
                credential: credential.clone(),
                expected_runner_device_id: "runner".into(),
            },
            &identity,
            "runner",
        )
        .await
        .expect("a machine reconnecting under its own name is not an impostor");

        let moved = ExecutorIdentity {
            display_name: "bglab-ubuntu".into(),
            ..identity
        };
        accept(
            &db,
            DeviceKind::Executor,
            &credential,
            &ExecutorEnrollmentIdentity::Credential {
                credential: credential.clone(),
                expected_runner_device_id: "runner".into(),
            },
            &moved,
            "runner",
        )
        .await
        .expect("a renamed machine moves its own claim");
        assert_eq!(
            claimed_name(&db, "remote-a").await.as_deref(),
            Some("bglab-ubuntu")
        );
    }

    #[tokio::test]
    async fn revocation_wins_over_an_outstanding_recovery_grant() {
        let db = db().await;
        enroll(&db, "remote-a", "bglab-ub").await.unwrap();

        assert!(create_recovery_grant(
            &db,
            DeviceKind::Executor,
            "other-runner",
            "remote-a",
            "remote-a-device",
            60,
        )
        .await
        .unwrap()
        .is_none());
        let recovery = create_recovery_grant(
            &db,
            DeviceKind::Executor,
            "runner",
            "remote-a",
            "remote-a-device",
            60,
        )
        .await
        .unwrap()
        .expect("the active bound enrollment authorizes recovery");

        revoke(
            &db,
            DeviceKind::Executor,
            "remote-a",
            "remote-a-device",
            "runner",
        )
        .await
        .unwrap();
        assert!(
            !token_is_known(&db, DeviceKind::Executor, &recovery).await,
            "revocation invalidates a recovery grant that has not been consumed"
        );
        assert!(create_recovery_grant(
            &db,
            DeviceKind::Executor,
            "runner",
            "remote-a",
            "remote-a-device",
            60,
        )
        .await
        .unwrap()
        .is_none());
    }

    /// A revoked machine releases its address for whatever replaces it, which is
    /// what makes `cairn executor remove` followed by a re-add work.
    #[tokio::test]
    async fn revoking_an_enrollment_releases_its_public_name() {
        let db = db().await;
        enroll(&db, "remote-a", "bglab-ub").await.unwrap();
        assert!(revoke(
            &db,
            DeviceKind::Executor,
            "remote-a",
            "remote-a-device",
            "runner"
        )
        .await
        .unwrap());
        enroll(&db, "remote-b", "bglab-ub")
            .await
            .expect("a released name is claimable");
    }

    /// An operator rename moves the durable claim. Left behind, the machine's
    /// own reconnection under the new label would be refused as a second
    /// claimant of a name nothing uses.
    #[tokio::test]
    async fn an_operator_rename_moves_the_claim_and_refuses_a_taken_name() {
        let db = db().await;
        enroll(&db, "remote-a", "bglab-ub").await.unwrap();
        enroll(&db, "remote-b", "bglab-mac").await.unwrap();

        rename(&db, "remote-a", "BGLab Ubuntu").await.unwrap();
        assert_eq!(
            claimed_name(&db, "remote-a").await.as_deref(),
            Some("bglab-ubuntu")
        );

        let taken = rename(&db, "remote-a", "bglab-mac").await.unwrap_err();
        assert!(taken.contains("bglab-mac"), "{taken}");
        assert!(taken.contains("remote-b"), "{taken}");
        assert_eq!(
            claimed_name(&db, "remote-a").await.as_deref(),
            Some("bglab-ubuntu"),
            "a refused rename leaves the previous claim intact"
        );

        assert!(rename(&db, "remote-a", "---").await.is_err());
    }

    /// A label carrying nothing that can be part of an address is not an
    /// enrollment the runner can route to, so it is refused at the door rather
    /// than admitted as a machine no request can name.
    #[tokio::test]
    async fn an_unaddressable_label_is_refused_at_enrollment() {
        let db = db().await;
        assert_eq!(
            enroll(&db, "remote-a", "   ").await.unwrap_err(),
            EnrollmentRejectionReason::MalformedAdvertisement
        );
    }

    #[tokio::test]
    async fn companion_credentials_are_kind_isolated_and_identity_neutral() {
        let db = db().await;
        let binding = DeviceBinding {
            enrolled_endpoint_id: "endpoint".into(),
            device_id: "phone".into(),
        };
        let grant = create_grant(
            &db,
            DeviceKind::Companion,
            "runner",
            Some("endpoint"),
            Some("phone"),
            60,
        )
        .await
        .unwrap();

        assert!(token_is_known(&db, DeviceKind::Companion, &grant).await);
        assert!(!token_is_known(&db, DeviceKind::Executor, &grant).await);
        let credential = accept_grant(&db, DeviceKind::Companion, &grant, &binding, "runner")
            .await
            .unwrap();
        assert!(!token_is_known(&db, DeviceKind::Companion, &grant).await);
        assert!(validate_binding_credential(
            &db,
            DeviceKind::Companion,
            &credential,
            &binding,
            "runner",
        )
        .await
        .is_ok());
        assert!(validate_binding_credential(
            &db,
            DeviceKind::Executor,
            &credential,
            &binding,
            "runner",
        )
        .await
        .is_err());

        let rows = db
            .query_all(
                "SELECT executor_name FROM executor_enrollments WHERE device_kind='companion'",
                (),
                |row| row.opt_text(0),
            )
            .await
            .unwrap();
        assert_eq!(rows, vec![None]);
    }

    #[tokio::test]
    async fn grant_binding_rejects_identity_mismatch() {
        let db = db().await;
        let grant = create_grant(
            &db,
            DeviceKind::Executor,
            "runner",
            Some("expected"),
            None,
            60,
        )
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
                DeviceKind::Executor,
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
