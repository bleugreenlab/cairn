//! Durable persistence for incidents, their affected-record inventory, and the
//! action journal that makes a response reconstructable afterwards.
//!
//! Every column here is non-secret by construction. The inventory holds a sink
//! name, a locator, and a count; the journal holds an action name, an actor, and
//! a detail string built from counts and sink labels. Nothing in this module
//! accepts record content, so there is no parameter through which matched
//! plaintext could reach a row — which is the point, since these rows are
//! themselves a durable store that a future disclosure would have to remediate.

use cairn_db::storage::{DbResult, LocalDb, RowExt};

use super::inventory::AffectedRecord;
use super::sink::Disposition;

/// How a disclosure came to light.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveredVia {
    /// A live crossing detected the credential in output it was about to serve.
    Crossing,
    /// An operator declared it — a provider alerted them, or they found it
    /// somewhere Cairn cannot see.
    Declared,
}

impl DiscoveredVia {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Crossing => "crossing",
            Self::Declared => "declared",
        }
    }
}

/// Where a response has got to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncidentStatus {
    /// Recorded, nothing done yet.
    Open,
    /// Every reachable store has been scanned.
    Inventoried,
    /// Every affected record is withheld, purged, or rebuilt, and authority is
    /// revoked. Not the same as resolved: the credential may still be live at
    /// its provider.
    ///
    /// Only ever set when *nothing* the response found is still being served.
    /// An incident that reads `contained` while a credential is still returned
    /// from some ungated store would be the most damaging summary this
    /// subsystem could produce, because a believable "contained" is exactly
    /// what stops an operator looking further.
    Contained,
    /// The automatic response finished, but records remain readable in stores
    /// with no read gate. Containment now needs a person: edit or delete the
    /// records the incident names, and rotate the credential.
    ActionRequired,
    /// An operator has confirmed the credential was rotated.
    Resolved,
}

impl IncidentStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Inventoried => "inventoried",
            Self::Contained => "contained",
            Self::ActionRequired => "action_required",
            Self::Resolved => "resolved",
        }
    }

    pub fn parse(value: &str) -> Self {
        match value {
            "inventoried" => Self::Inventoried,
            "contained" => Self::Contained,
            "action_required" => Self::ActionRequired,
            "resolved" => Self::Resolved,
            _ => Self::Open,
        }
    }
}

/// One recorded disclosure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncidentRecord {
    pub id: String,
    pub secret_id: String,
    pub category: Option<String>,
    pub discovered_via: DiscoveredVia,
    pub crossing: Option<String>,
    pub note: Option<String>,
    pub status: IncidentStatus,
    pub leases_revoked: i64,
    pub grants_revoked: i64,
    pub rotation_required: bool,
    pub rotation_confirmed_at: Option<i64>,
    pub discovered_at: i64,
    pub updated_at: i64,
}

/// Open an incident. Returns its id.
#[allow(clippy::too_many_arguments)]
pub async fn open_incident(
    db: &LocalDb,
    secret_id: &str,
    category: Option<&str>,
    discovered_via: DiscoveredVia,
    crossing: Option<&str>,
    note: Option<&str>,
    at: i64,
) -> DbResult<String> {
    let id = uuid::Uuid::new_v4().to_string();
    db.execute(
        "INSERT INTO disclosure_incidents \
         (id, secret_id, category, discovered_via, crossing, note, status, discovered_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'open', ?7, ?7)",
        (
            id.as_str(),
            secret_id,
            category,
            discovered_via.as_str(),
            crossing,
            note,
            at,
        ),
    )
    .await?;
    Ok(id)
}

/// Move an incident to a new status.
pub async fn set_status(
    db: &LocalDb,
    incident_id: &str,
    status: IncidentStatus,
    at: i64,
) -> DbResult<()> {
    db.execute(
        "UPDATE disclosure_incidents SET status = ?1, updated_at = ?2 WHERE id = ?3",
        (status.as_str(), at, incident_id),
    )
    .await?;
    Ok(())
}

/// Record what revocation took.
pub async fn record_revocation(
    db: &LocalDb,
    incident_id: &str,
    leases: usize,
    grants: usize,
    at: i64,
) -> DbResult<()> {
    db.execute(
        "UPDATE disclosure_incidents SET leases_revoked = ?1, grants_revoked = ?2, updated_at = ?3 \
         WHERE id = ?4",
        (leases as i64, grants as i64, at, incident_id),
    )
    .await?;
    Ok(())
}

/// Mark the credential rotated, which is the only thing that resolves an
/// incident. Deliberately an operator assertion rather than something inferred:
/// nothing here can observe a third party's key store.
pub async fn confirm_rotation(
    db: &LocalDb,
    incident_id: &str,
    actor: &str,
    at: i64,
) -> DbResult<bool> {
    let changed = db
        .execute(
            "UPDATE disclosure_incidents \
             SET rotation_required = 0, rotation_confirmed_at = ?1, status = 'resolved', \
                 updated_at = ?1 \
             WHERE id = ?2 AND rotation_confirmed_at IS NULL",
            (at, incident_id),
        )
        .await?;
    if changed > 0 {
        journal(db, incident_id, "rotation_confirmed", actor, None, at).await?;
    }
    Ok(changed > 0)
}

/// Record one affected record and its disposition.
pub async fn record_affected(
    db: &LocalDb,
    incident_id: &str,
    record: &AffectedRecord,
    disposition: Disposition,
    settled: bool,
    at: i64,
) -> DbResult<()> {
    db.execute(
        "INSERT INTO disclosure_affected_records \
         (id, incident_id, sink, locator, record_class, occurrences, disposition, found_at, settled_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        (
            uuid::Uuid::new_v4().to_string(),
            incident_id,
            record.sink.as_str(),
            record.locator.as_str(),
            record.sink.record_class().as_str(),
            record.occurrences as i64,
            disposition.as_str(),
            at,
            settled.then_some(at),
        ),
    )
    .await?;
    Ok(())
}

/// Append one step to an incident's action journal.
///
/// `detail` carries counts and sink names. It is the one free-text column in
/// this module, and every caller builds it from labels and numbers — never from
/// a record's content.
pub async fn journal(
    db: &LocalDb,
    incident_id: &str,
    action: &str,
    actor: &str,
    detail: Option<&str>,
    at: i64,
) -> DbResult<()> {
    let incident = incident_id.to_string();
    let next: i64 = db
        .read(move |conn| {
            let incident = incident.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT COALESCE(MAX(seq), 0) + 1 FROM disclosure_actions \
                         WHERE incident_id = ?1",
                        (incident,),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => DbResult::Ok(row.i64(0)?),
                    None => DbResult::Ok(1),
                }
            })
        })
        .await?;

    db.execute(
        "INSERT INTO disclosure_actions (id, incident_id, seq, action, actor, detail, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        (
            uuid::Uuid::new_v4().to_string(),
            incident_id,
            next,
            action,
            actor,
            detail,
            at,
        ),
    )
    .await?;
    Ok(())
}

/// One journal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionRecord {
    pub seq: i64,
    pub action: String,
    pub actor: String,
    pub detail: Option<String>,
    pub created_at: i64,
}

/// Every incident, newest first.
pub async fn list_incidents(db: &LocalDb, limit: i64) -> DbResult<Vec<IncidentRecord>> {
    db.read(move |conn| {
        Box::pin(async move {
            let mut out = Vec::new();
            let mut rows = conn
                .query(
                    "SELECT id, secret_id, category, discovered_via, crossing, note, status, \
                            leases_revoked, grants_revoked, rotation_required, \
                            rotation_confirmed_at, discovered_at, updated_at \
                     FROM disclosure_incidents ORDER BY discovered_at DESC LIMIT ?1",
                    (limit,),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                out.push(IncidentRecord {
                    id: row.text(0)?,
                    secret_id: row.text(1)?,
                    category: row.opt_text(2)?,
                    discovered_via: if row.text(3)? == "declared" {
                        DiscoveredVia::Declared
                    } else {
                        DiscoveredVia::Crossing
                    },
                    crossing: row.opt_text(4)?,
                    note: row.opt_text(5)?,
                    status: IncidentStatus::parse(&row.text(6)?),
                    leases_revoked: row.i64(7)?,
                    grants_revoked: row.i64(8)?,
                    rotation_required: row.i64(9)? != 0,
                    rotation_confirmed_at: row.opt_i64(10)?,
                    discovered_at: row.i64(11)?,
                    updated_at: row.i64(12)?,
                });
            }
            DbResult::Ok(out)
        })
    })
    .await
}

/// One incident by id.
pub async fn get_incident(db: &LocalDb, id: &str) -> DbResult<Option<IncidentRecord>> {
    let wanted = id.to_string();
    Ok(list_incidents(db, 10_000)
        .await?
        .into_iter()
        .find(|incident| incident.id == wanted))
}

/// The affected records recorded for an incident, grouped-friendly order.
pub async fn affected_for(
    db: &LocalDb,
    incident_id: &str,
) -> DbResult<Vec<(String, String, i64, String)>> {
    let incident = incident_id.to_string();
    db.read(move |conn| {
        let incident = incident.clone();
        Box::pin(async move {
            let mut out = Vec::new();
            let mut rows = conn
                .query(
                    "SELECT sink, locator, occurrences, disposition FROM disclosure_affected_records \
                     WHERE incident_id = ?1 ORDER BY sink, locator",
                    (incident,),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, row.text(1)?, row.i64(2)?, row.text(3)?));
            }
            DbResult::Ok(out)
        })
    })
    .await
}

/// An incident's action journal, in order.
pub async fn actions_for(db: &LocalDb, incident_id: &str) -> DbResult<Vec<ActionRecord>> {
    let incident = incident_id.to_string();
    db.read(move |conn| {
        let incident = incident.clone();
        Box::pin(async move {
            let mut out = Vec::new();
            let mut rows = conn
                .query(
                    "SELECT seq, action, actor, detail, created_at FROM disclosure_actions \
                     WHERE incident_id = ?1 ORDER BY seq",
                    (incident,),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                out.push(ActionRecord {
                    seq: row.i64(0)?,
                    action: row.text(1)?,
                    actor: row.text(2)?,
                    detail: row.opt_text(3)?,
                    created_at: row.i64(4)?,
                });
            }
            DbResult::Ok(out)
        })
    })
    .await
}
