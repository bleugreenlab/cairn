//! Responding to a credential that is already loose (CAIRN-3828).
//!
//! Everything else in [`crate::security`] is about stopping a credential from
//! reaching a record. This module is about the records that already have one.
//!
//! # Why this is a separate problem
//!
//! The crossings guarantee that output leaving a boundary *now* is scrubbed
//! against what is registered *now*. Neither half of that reaches backwards. A
//! record written before its credential was registered was written in the clear;
//! a log rotated away by an older build was never scrubbed at all; and a
//! detection at a live crossing is simultaneously a success (this reader did not
//! get it) and evidence of a failure (something produced it, and other things
//! have been producing it).
//!
//! So a detection is not just a block. It is a signal that a credential is loose
//! in stores nobody has looked at, and the response to it is a *procedure*, not
//! a log line.
//!
//! # The order is forced, not chosen
//!
//! 1. **Revoke.** [`revoke`]. The only step that reduces harm — quarantine does
//!    nothing about a copy someone already took. It runs first because the
//!    inventory is slow and every second of it is a second the credential is
//!    still being handed out.
//! 2. **Inventory.** [`inventory`]. Scan every reachable store. This must happen
//!    while the credential is still registered, because matching is by
//!    registered value.
//! 3. **Quarantine, purge, rebuild.** [`sink`] decides which of the three each
//!    store gets, from one question: is there a cleaner copy upstream?
//! 4. **Rotate.** [`rotation`]. Last, because rotating unregisters the value and
//!    makes every remaining copy invisible to a scan while leaving it exactly as
//!    readable on disk.
//!
//! Steps 1 and 4 look like they should be adjacent — both are about the
//! credential rather than the records — and putting them together is the natural
//! mistake. They cannot be. Revocation preserves the registration, which the
//! inventory needs; rotation destroys it. The inventory has to happen in
//! between.
//!
//! # Two prohibitions
//!
//! **Never persist matched plaintext.** An incident report wants to quote what
//! it found, and doing so would copy the credential into a new durable store on
//! the day someone decided the old ones were too dangerous to keep. The rule is
//! structural rather than remembered: [`inventory::count_occurrences`] is the
//! only function that sees record content and it returns a `usize`, and
//! [`inventory::AffectedRecord`] has no field a span could go in.
//!
//! **Never silently rewrite authored state.** A transcript, an issue body, a
//! terminal's scrollback, and an archival blob are the account of what happened.
//! Editing them to make a security problem disappear falsifies the record and
//! destroys the evidence an operator needs. So a source record is *withheld*,
//! not corrected: the row stays exactly as it was and the read path substitutes
//! a notice. Repair exists — [`sink::Disposition::Repaired`] — but no automatic
//! path produces it.
//!
//! # What a response does not do
//!
//! It does not reach the provider, another machine's replica, or anything that
//! has left this host. Those are named in [`sink::Reach::Manual`] and reported
//! *as part of the incident*, because an operator who is told "these three
//! stores are yours to handle, for these reasons" has a complete inventory,
//! and one who is told nothing has a false one.

pub mod inventory;
pub mod rebuild;
pub mod revoke;
pub mod rotation;
pub mod sink;
pub mod store;

use cairn_common::security::{registry, SecretCategory, SecretId};
use cairn_db::storage::{quarantine, DbResult, LocalDb};

pub use inventory::{AffectedRecord, Inventory, InventoryRoots};
pub use revoke::Revoked;
pub use rotation::{rotation_hook, RotationHook};
pub use sink::{Disposition, Gate, Reach, RecordClass, SinkKind, ALL_SINKS};
pub use store::{DiscoveredVia, IncidentRecord, IncidentStatus};

/// Recorded as the actor for steps the system took on its own.
const SYSTEM_ACTOR: &str = "system";

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// A credential known to be loose, and how that became known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disclosure {
    /// The registry identity of the credential. Names the producer, never the
    /// value.
    pub secret_id: SecretId,
    pub category: Option<SecretCategory>,
    pub discovered_via: DiscoveredVia,
    /// The crossing that caught it, when one did.
    pub crossing: Option<String>,
    /// An operator's own words about a declared disclosure. Never a quoted
    /// match: this is what they typed, not what was found.
    pub note: Option<String>,
}

impl Disclosure {
    /// A disclosure a live crossing detected.
    pub fn from_crossing(
        secret_id: SecretId,
        category: Option<SecretCategory>,
        crossing: &str,
    ) -> Self {
        Self {
            secret_id,
            category,
            discovered_via: DiscoveredVia::Crossing,
            crossing: Some(crossing.to_string()),
            note: None,
        }
    }

    /// A disclosure an operator declared — a provider alerted them, or they
    /// found the credential somewhere Cairn cannot see.
    pub fn declared(secret_id: SecretId, note: Option<String>) -> Self {
        Self {
            secret_id,
            category: None,
            discovered_via: DiscoveredVia::Declared,
            crossing: None,
            note,
        }
    }
}

/// Everything a response did, for the operator's report.
#[derive(Debug, Clone)]
pub struct Response {
    pub incident_id: String,
    pub revoked: Revoked,
    pub inventory: Inventory,
    /// Source records withheld from serving.
    pub quarantined: usize,
    /// Source records found in a store with no read gate. Named in the incident
    /// and still being served: containment for these is the operator's move,
    /// and reporting them as anything else would be a false claim of safety.
    pub reported: usize,
    /// Ephemeral rows deleted.
    pub purged: usize,
    /// Derived rows dropped so they regenerate from the withheld sources.
    pub invalidated: usize,
    pub rotation: RotationHook,
}

impl Response {
    /// Whether anything at all was found in a durable store.
    pub fn found_nothing(&self) -> bool {
        self.inventory.records.is_empty()
    }

    /// Whether records remain readable despite the response.
    ///
    /// The question an operator actually has after reading a response, and the
    /// reason [`Self::reported`] is not folded into [`Self::quarantined`].
    pub fn leaves_records_served(&self) -> bool {
        self.reported > 0
    }
}

/// Respond to a disclosure: revoke, inventory, contain, and report what rotation
/// still requires.
///
/// See the module docs for why the steps are in this order. The response is
/// recorded as it goes rather than at the end, so an interrupted run leaves a
/// readable incident naming what it had done — a half-finished response an
/// operator can see is recoverable, and one that vanished is not.
pub async fn respond(db: &LocalDb, disclosure: &Disclosure) -> DbResult<Response> {
    respond_in(db, disclosure, &InventoryRoots::host()).await
}

/// [`respond`], with the file-backed stores rooted at `roots` — for an operator
/// scanning a restored log directory, and for tests that must not read the
/// machine's own logs.
pub async fn respond_in(
    db: &LocalDb,
    disclosure: &Disclosure,
    roots: &InventoryRoots,
) -> DbResult<Response> {
    let at = now();
    let secret_id = &disclosure.secret_id;

    let incident_id = store::open_incident(
        db,
        secret_id.as_str(),
        disclosure.category.map(|category| category.as_str()),
        disclosure.discovered_via,
        disclosure.crossing.as_deref(),
        disclosure.note.as_deref(),
        at,
    )
    .await?;

    // 1. Revoke first. See the module docs: the inventory is slow, and until
    //    this runs the credential is still being handed out on request.
    let revoked = revoke::revoke_authority(db, secret_id).await;
    store::record_revocation(db, &incident_id, revoked.leases, revoked.grants, at).await?;
    store::journal(
        db,
        &incident_id,
        "revoke",
        SYSTEM_ACTOR,
        Some(&format!(
            "{} lease(s), {} grant(s) across scopes: {}",
            revoked.leases,
            revoked.grants,
            if revoked.scopes.is_empty() {
                "none recorded".to_string()
            } else {
                revoked.scopes.join(", ")
            }
        )),
        at,
    )
    .await?;

    // 2. Inventory, while the credential is still registered.
    let snapshot = registry().snapshot();
    let inventory = inventory::take_inventory_in(db, &snapshot, secret_id, roots).await?;
    store::set_status(db, &incident_id, IncidentStatus::Inventoried, at).await?;
    store::journal(
        db,
        &incident_id,
        "inventory",
        SYSTEM_ACTOR,
        Some(&format!(
            "{} record(s), {} occurrence(s), across: {}",
            inventory.records.len(),
            inventory.total_occurrences(),
            sink_list(&inventory)
        )),
        at,
    )
    .await?;

    // 3. Contain. Each record's disposition falls out of its store's class, so
    //    there is no branch here that could decide to rewrite a source record.
    //
    //    A source record is only recorded as quarantined when its store has a
    //    read gate that will actually withhold it. Without one, writing a
    //    quarantine row would produce a durable claim of containment that no
    //    read path honours, so the record is `Reported` instead: named, counted,
    //    and left for the operator.
    let mut quarantined = 0;
    let mut reported = 0;
    for record in &inventory.records {
        let mut disposition = record.sink.record_class().disposition();
        if disposition == Disposition::Quarantined && record.sink.gate() != Gate::Withholds {
            disposition = Disposition::Reported;
        }
        match disposition {
            Disposition::Quarantined => {
                quarantine::quarantine_record(
                    db,
                    record.sink.as_str(),
                    &record.locator,
                    &incident_id,
                    at,
                )
                .await?;
                quarantined += 1;
            }
            Disposition::Reported => reported += 1,
            _ => {}
        }
        store::record_affected(db, &incident_id, record, disposition, true, at).await?;
    }

    let purged = rebuild::purge_ephemeral(db, &inventory.records).await;
    // Rebuilt after quarantine, which is what makes the rebuild sanitized: the
    // sources it regenerates from now answer with a withholding notice.
    let rebuilt = rebuild::rebuild_derived(db, &inventory.records).await;

    store::journal(
        db,
        &incident_id,
        "contain",
        SYSTEM_ACTOR,
        Some(&format!(
            "{quarantined} withheld, {reported} reported but still served, {purged} purged, \
             {} derived row(s) invalidated ({})",
            rebuilt.invalidated,
            if rebuilt.sinks.is_empty() {
                "none".to_string()
            } else {
                rebuilt.sinks.join(", ")
            }
        )),
        at,
    )
    .await?;
    // `Contained` is a claim that nothing found is still readable, so it is
    // only made when nothing is. A record sitting in a store with no read gate
    // leaves the incident at `ActionRequired`: the automatic part finished and
    // a person still has to finish the rest. Summarising that as "contained"
    // would undo, at the incident level, exactly the per-record honesty the
    // disposition split exists to provide — and the summary is what an operator
    // reads first.
    let status = if reported > 0 {
        IncidentStatus::ActionRequired
    } else {
        IncidentStatus::Contained
    };
    store::set_status(db, &incident_id, status, at).await?;

    // 4. Rotation is the operator's, and the incident stays unresolved until
    //    they confirm it. Nothing here can observe a third party's key store.
    let rotation = rotation::rotation_hook(secret_id, disclosure.category);
    store::journal(
        db,
        &incident_id,
        "rotation_required",
        SYSTEM_ACTOR,
        Some(&format!(
            "provider {}, configured at {}",
            rotation.provider, rotation.configured_at
        )),
        at,
    )
    .await?;

    log::warn!(
        "disclosure incident {incident_id} for {secret_id}: revoked {} lease(s) and {} grant(s); \
         withheld {quarantined} record(s), {reported} still served in ungated stores, purged \
         {purged}, invalidated {}; rotation required at {}",
        revoked.leases,
        revoked.grants,
        rebuilt.invalidated,
        rotation.provider,
    );

    Ok(Response {
        incident_id,
        revoked,
        inventory,
        quarantined,
        reported,
        purged,
        invalidated: rebuilt.invalidated,
        rotation,
    })
}

/// Release a withheld record back to serving. An operator decision, journaled.
pub async fn release(
    db: &LocalDb,
    incident_id: &str,
    sink: SinkKind,
    locator: &str,
    actor: &str,
) -> DbResult<bool> {
    let at = now();
    let released = quarantine::release_record(db, sink.as_str(), locator, actor, at).await?;
    if released {
        store::journal(
            db,
            incident_id,
            "release",
            actor,
            Some(&format!("{sink} {locator}")),
            at,
        )
        .await?;
    }
    Ok(released)
}

/// Record that an operator rotated the credential, resolving the incident.
pub async fn confirm_rotation(db: &LocalDb, incident_id: &str, actor: &str) -> DbResult<bool> {
    store::confirm_rotation(db, incident_id, actor, now()).await
}

/// Load the quarantine set at startup, so the read gate is armed before the
/// first record is served.
///
/// Without this a restart serves every withheld record until something happens
/// to write to the quarantine table — the failure would look like the feature
/// working, right up until it mattered.
pub async fn arm_quarantine(db: &LocalDb) -> DbResult<usize> {
    let count = quarantine::quarantine().refresh(db).await?;
    if count > 0 {
        log::info!("quarantine armed: {count} record(s) withheld from serving");
    }
    Ok(count)
}

fn sink_list(inventory: &Inventory) -> String {
    let sinks = inventory.affected_sinks();
    if sinks.is_empty() {
        return "no store".to_string();
    }
    sinks
        .iter()
        .map(|sink| sink.label())
        .collect::<Vec<_>>()
        .join(", ")
}
