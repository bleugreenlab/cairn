//! Withholding records that are known to carry a disclosed credential.
//!
//! This module is mechanism, not policy. It answers exactly one question on the
//! read path — *is this record withheld?* — and it owns the durable table that
//! answers it. Which stores exist, how they are classified, and what a response
//! to a disclosure does are `cairn_core::security::remediation`'s business.
//!
//! # Why quarantine cannot be scrubbing
//!
//! The obvious implementation is to scrub a served record against the secret
//! registry on the way out, the way every other crossing does. That is wrong
//! here, and the reason is worth stating because the two look interchangeable.
//!
//! The registry is **process-local and rebuilt from live credentials**. After a
//! restart it is empty until each producer re-registers, and a credential that
//! has been *rotated* — which is the whole point of responding to a disclosure —
//! is never registered again by anyone. So a scrub-on-read gate would stop
//! redacting precisely when the disclosure is oldest and the operator most
//! believes it was handled. It fails open, silently, on the schedule that
//! matters most.
//!
//! The quarantine set is durable and names records rather than values, so it
//! keeps working across a restart, across a rotation, and across a build that no
//! longer has the credential at all. It costs a strictly coarser answer — the
//! whole record is withheld, not just the matched span — and that is the correct
//! trade for a store of records already known to be dirty.
//!
//! # The read path pays almost nothing
//!
//! Quarantine is rare and usually empty, so the set is a process-local snapshot
//! behind an atomic swap, exactly like [`crate`]'s sibling registry snapshot in
//! `cairn_common::security`. A read that touches no quarantined record costs one
//! `Arc` clone and an `is_empty` check; there is no per-row database round trip.
//! The snapshot is the union of every open database's withheld set — see
//! [`Quarantine`] for why it cannot be one flat set.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};

use crate::storage::{DbResult, LocalDb, RowExt};

/// The sink name under which transcript event rows are quarantined.
///
/// Declared here because this crate's read path matches on it, but the taxonomy
/// it belongs to lives in `cairn_core::security::remediation::SinkKind`. A test
/// there pins the two spellings together, so this constant cannot drift into a
/// second definition of the same sink.
pub const TRANSCRIPT_EVENT_SINK: &str = "transcript_event";

/// The sink name under which archival segment blobs are quarantined, keyed by
/// content hash.
pub const ARCHIVAL_BLOB_SINK: &str = "archival_blob";

/// What a reader sees in place of a withheld record.
///
/// Deliberately shaped like the archival reconstruction stub next door: a
/// labeled substitution inside an otherwise intact record, so a transcript keeps
/// rendering and the gap is legible rather than a mystery. It names the incident
/// so an operator reading the transcript can go find out what happened.
pub const WITHHELD_PREFIX: &str = "content withheld: quarantined by disclosure incident";

/// Render the withholding notice for one incident.
pub fn withheld_notice(incident_id: &str) -> String {
    format!(
        "[{WITHHELD_PREFIX} {incident_id}. The stored record carries a credential that was \
         disclosed; it is retained for forensics and withheld from serving. See \
         `cairn://grants?view=incidents`.]"
    )
}

/// The set of currently withheld records, as one immutable snapshot.
#[derive(Debug, Default)]
pub struct QuarantineSet {
    /// `(sink, locator)` addressing the record, mapped to the incident that
    /// withheld it. One map rather than a membership set beside an attribution
    /// list: the gate asks both questions about the same record in the same
    /// breath, so splitting them bought a linear scan for nothing.
    withheld: HashMap<(String, String), String>,
}

impl QuarantineSet {
    /// Build a set from `(sink, locator, incident_id)` triples.
    pub fn from_entries(rows: impl IntoIterator<Item = (String, String, String)>) -> Self {
        Self {
            withheld: rows
                .into_iter()
                .map(|(sink, locator, incident)| ((sink, locator), incident))
                .collect(),
        }
    }

    /// Nothing is withheld. The overwhelmingly common case, and the read path's
    /// early out.
    pub fn is_empty(&self) -> bool {
        self.withheld.is_empty()
    }

    /// How many records this set withholds.
    pub fn len(&self) -> usize {
        self.withheld.len()
    }

    /// The incident withholding this record, or `None` if it is servable.
    pub fn withheld_by(&self, sink: &str, locator: &str) -> Option<&str> {
        self.withheld
            .get(&(sink.to_string(), locator.to_string()))
            .map(String::as_str)
    }

    /// Fold `other`'s entries into this set.
    fn absorb(&mut self, other: &QuarantineSet) {
        for (address, incident) in &other.withheld {
            self.withheld.insert(address.clone(), incident.clone());
        }
    }
}

/// Process-local holder for the withheld records of every open database.
///
/// # Why this is keyed per database rather than held as one set
///
/// The holder is process-global but its data is per database: a refresh reads
/// one `quarantined_records` table, which knows only about itself. Installing
/// that result wholesale therefore makes the holder last-writer-wins, so
/// refreshing database B silently discards everything database A was
/// withholding — no error, no log line, just records quietly served again.
/// Keying by source means a refresh replaces only its own contribution.
///
/// Today exactly one database carries these tables. The migration is
/// private-only (see its header: an inventory of where a credential sits must
/// not be replicated), so a team replica has no quarantine of its own and the
/// union normally has one member. The keying is not speculation about that
/// changing — it is what makes a process-global holder correct rather than
/// accidentally correct, and a process that opens several databases (every test
/// binary, and any host that opens a replica) is already the plural case.
///
/// The gate reads the union, which is also the safe direction: a locator
/// withheld by one database and absent from another costs nothing in the
/// database that never had it, whereas the reverse would serve a disclosed
/// credential. That union is why a quarantine recorded privately still withholds
/// the record wherever it is read from — withholding is keyed by address, not by
/// which database answered.
#[derive(Debug)]
pub struct Quarantine {
    state: RwLock<QuarantineState>,
}

#[derive(Debug, Default)]
struct QuarantineState {
    /// Each open database's own withheld set, keyed by its file path.
    per_database: BTreeMap<PathBuf, Arc<QuarantineSet>>,
    /// Their union, recomputed on every install so the read path never merges.
    merged: Arc<QuarantineSet>,
}

impl Default for Quarantine {
    fn default() -> Self {
        Self::new()
    }
}

impl Quarantine {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(QuarantineState::default()),
        }
    }

    /// The union of every database's withheld records. Cheap enough for a
    /// per-read call: it clones one `Arc`.
    pub fn snapshot(&self) -> Arc<QuarantineSet> {
        self.state
            .read()
            .expect("quarantine snapshot lock poisoned")
            .merged
            .clone()
    }

    /// Replace what `source` contributes, leaving every other database's
    /// contribution intact.
    pub fn install_for(&self, source: &Path, set: QuarantineSet) {
        let mut state = self
            .state
            .write()
            .expect("quarantine snapshot lock poisoned");
        state
            .per_database
            .insert(source.to_path_buf(), Arc::new(set));

        // One database is the overwhelmingly common case; keep its own Arc
        // rather than copying it into an identical union.
        state.merged = if state.per_database.len() == 1 {
            state
                .per_database
                .values()
                .next()
                .expect("one source")
                .clone()
        } else {
            let mut union = QuarantineSet::default();
            for set in state.per_database.values() {
                union.absorb(set);
            }
            Arc::new(union)
        };
    }

    /// Reload `db`'s contribution from its durable table.
    ///
    /// Called at startup and after every quarantine or release, which is what
    /// makes the in-memory set trustworthy without the read path querying.
    pub async fn refresh(&self, db: &LocalDb) -> DbResult<usize> {
        let set = load_quarantine_set(db).await?;
        let count = set.len();
        self.install_for(db.path(), set);
        Ok(count)
    }
}

/// The process's quarantine.
pub fn quarantine() -> &'static Quarantine {
    static QUARANTINE: OnceLock<Quarantine> = OnceLock::new();
    QUARANTINE.get_or_init(Quarantine::new)
}

/// One withheld record, for the operator inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantinedRecord {
    pub sink: String,
    pub locator: String,
    pub incident_id: String,
    pub quarantined_at: i64,
    pub released_at: Option<i64>,
    pub released_by: Option<String>,
}

/// Load the live (unreleased) quarantine set from the database.
pub async fn load_quarantine_set(db: &LocalDb) -> DbResult<QuarantineSet> {
    let rows: Vec<(String, String, String)> = db
        .read(|conn| {
            Box::pin(async move {
                let mut out = Vec::new();
                let mut rows = conn
                    .query(
                        "SELECT sink, locator, incident_id FROM quarantined_records \
                         WHERE released_at IS NULL",
                        (),
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    out.push((row.text(0)?, row.text(1)?, row.text(2)?));
                }
                DbResult::Ok(out)
            })
        })
        .await?;
    Ok(QuarantineSet::from_entries(rows))
}

/// Withhold a record, and refresh the process snapshot so the gate sees it.
///
/// Idempotent: re-quarantining a record already withheld by the same incident
/// leaves it withheld. Re-quarantining one that was released re-withholds it,
/// because a second incident finding the same record is new evidence.
pub async fn quarantine_record(
    db: &LocalDb,
    sink: &str,
    locator: &str,
    incident_id: &str,
    at: i64,
) -> DbResult<()> {
    db.execute(
        "INSERT INTO quarantined_records (sink, locator, incident_id, quarantined_at) \
         VALUES (?1, ?2, ?3, ?4) \
         ON CONFLICT(sink, locator) DO UPDATE SET \
           incident_id = excluded.incident_id, \
           quarantined_at = excluded.quarantined_at, \
           released_at = NULL, \
           released_by = NULL",
        (sink, locator, incident_id, at),
    )
    .await?;
    quarantine().refresh(db).await?;
    Ok(())
}

/// Release a record back to serving.
///
/// Deliberately requires an actor: releasing is an operator decision that the
/// record is safe to serve again — because it was repaired, or because the
/// credential it carries is dead and the history is worth more than the residue.
/// The row is kept, released rather than deleted, so the audit trail survives.
pub async fn release_record(
    db: &LocalDb,
    sink: &str,
    locator: &str,
    released_by: &str,
    at: i64,
) -> DbResult<bool> {
    let changed = db
        .execute(
            "UPDATE quarantined_records SET released_at = ?1, released_by = ?2 \
             WHERE sink = ?3 AND locator = ?4 AND released_at IS NULL",
            (at, released_by, sink, locator),
        )
        .await?;
    quarantine().refresh(db).await?;
    Ok(changed > 0)
}

/// Every quarantine row, withheld and released alike, newest first.
pub async fn list_quarantined(db: &LocalDb) -> DbResult<Vec<QuarantinedRecord>> {
    db.read(|conn| {
        Box::pin(async move {
            let mut out = Vec::new();
            let mut rows = conn
                .query(
                    "SELECT sink, locator, incident_id, quarantined_at, released_at, released_by \
                     FROM quarantined_records ORDER BY quarantined_at DESC",
                    (),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                out.push(QuarantinedRecord {
                    sink: row.text(0)?,
                    locator: row.text(1)?,
                    incident_id: row.text(2)?,
                    quarantined_at: row.i64(3)?,
                    released_at: row.opt_i64(4)?,
                    released_by: row.opt_text(5)?,
                });
            }
            DbResult::Ok(out)
        })
    })
    .await
}

/// Substitute the withholding notice into every quarantined event in `events`.
///
/// The substituted record keeps only the fields transcript reconstruction needs
/// to place it: the event type, the session and parent-tool linkage, the tool
/// use it pairs with, the tool's name, and whether it errored. Everything that
/// can carry free text is dropped and `content` becomes the notice.
///
/// Dropping rather than redacting is what makes this fail closed. A field this
/// code does not know about — one added to the transcript event next year — is
/// absent from the rebuilt object rather than passed through, so a new
/// text-bearing field cannot quietly become a hole in the quarantine. The cost
/// is that a withheld record renders as a stub, which is the intent.
///
/// Identity fields are preserved for the same reason the transcript crossing
/// excludes them from redaction: they are Cairn-minted or protocol-fixed, and
/// blanking one turns a withheld event into a broken transcript.
pub fn withhold_quarantined(events: &mut [crate::models::Event]) {
    let set = quarantine().snapshot();
    if set.is_empty() {
        return;
    }
    for event in events.iter_mut() {
        let Some(incident) = set.withheld_by(TRANSCRIPT_EVENT_SINK, &event.id) else {
            continue;
        };
        event.data = withheld_event_data(&event.data, incident);
    }
}

/// Rebuild one event's `data` as a withholding stub. See
/// [`withhold_quarantined`] for why this drops rather than redacts.
fn withheld_event_data(data: &str, incident_id: &str) -> String {
    // Keys are the camelCase serialization of `TranscriptEvent`.
    const CARRIED: [&str; 5] = [
        "eventType",
        "sessionId",
        "parentToolUseId",
        "toolUseId",
        "toolName",
    ];

    let mut out = serde_json::Map::new();
    if let Ok(serde_json::Value::Object(original)) = serde_json::from_str(data) {
        for key in CARRIED {
            if let Some(value) = original.get(key) {
                out.insert(key.to_string(), value.clone());
            }
        }
        if let Some(is_error) = original.get("isError") {
            out.insert("isError".to_string(), is_error.clone());
        }
    }
    out.entry("isError")
        .or_insert(serde_json::Value::Bool(false));
    out.insert(
        "content".to_string(),
        serde_json::Value::String(withheld_notice(incident_id)),
    );
    // A record we cannot serialize is a record we must not serve, so the
    // fallback is the notice alone rather than the original bytes.
    serde_json::to_string(&serde_json::Value::Object(out))
        .unwrap_or_else(|_| format!("{{\"content\":{:?}}}", withheld_notice(incident_id)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_set_withholds_nothing() {
        let set = QuarantineSet::default();
        assert!(set.is_empty());
        assert_eq!(set.withheld_by(TRANSCRIPT_EVENT_SINK, "event-1"), None);
    }

    #[test]
    fn a_withheld_record_names_its_incident() {
        let set = QuarantineSet::from_entries([(
            TRANSCRIPT_EVENT_SINK.to_string(),
            "event-1".to_string(),
            "incident-9".to_string(),
        )]);
        assert!(!set.is_empty());
        assert_eq!(
            set.withheld_by(TRANSCRIPT_EVENT_SINK, "event-1"),
            Some("incident-9")
        );
        // A different record in the same sink, and the same locator in a
        // different sink, are both servable: the pair is the key.
        assert_eq!(set.withheld_by(TRANSCRIPT_EVENT_SINK, "event-2"), None);
        assert_eq!(set.withheld_by("process_log", "event-1"), None);
    }

    #[test]
    fn one_database_never_clobbers_another() {
        // The bug this keying exists to prevent: the holder is process-global
        // while each refresh reads one database's table, so installing a result
        // wholesale let whichever database refreshed last silently un-withhold
        // every record the others were holding.
        let quarantine = Quarantine::new();
        let private = Path::new("/tmp/private.db");
        let team = Path::new("/tmp/team.db");

        quarantine.install_for(
            private,
            QuarantineSet::from_entries([(
                TRANSCRIPT_EVENT_SINK.to_string(),
                "event-private".to_string(),
                "incident-p".to_string(),
            )]),
        );
        quarantine.install_for(
            team,
            QuarantineSet::from_entries([(
                TRANSCRIPT_EVENT_SINK.to_string(),
                "event-team".to_string(),
                "incident-t".to_string(),
            )]),
        );

        let set = quarantine.snapshot();
        assert_eq!(
            set.withheld_by(TRANSCRIPT_EVENT_SINK, "event-private"),
            Some("incident-p"),
            "arming the team replica un-withheld the private database's record"
        );
        assert_eq!(
            set.withheld_by(TRANSCRIPT_EVENT_SINK, "event-team"),
            Some("incident-t")
        );

        // Releasing everything in one database leaves the other's intact.
        quarantine.install_for(team, QuarantineSet::default());
        let set = quarantine.snapshot();
        assert_eq!(
            set.withheld_by(TRANSCRIPT_EVENT_SINK, "event-private"),
            Some("incident-p")
        );
        assert_eq!(set.withheld_by(TRANSCRIPT_EVENT_SINK, "event-team"), None);
    }

    #[test]
    fn the_notice_names_the_incident_and_never_the_value() {
        let notice = withheld_notice("incident-9");
        assert!(notice.contains("incident-9"));
        assert!(notice.starts_with(&format!("[{WITHHELD_PREFIX}")));
    }

    #[test]
    fn withholding_keeps_linkage_and_drops_every_text_field() {
        let data = serde_json::json!({
            "eventType": "tool_result",
            "sessionId": "session-1",
            "parentToolUseId": "parent-1",
            "toolUseId": "use-1",
            "toolName": "Bash",
            "isError": true,
            "content": "export TOKEN=ghp_realcredentialvalue",
            "toolResult": "ghp_realcredentialvalue",
            "thinking": "the key is ghp_realcredentialvalue",
            "toolInput": {"command": "echo ghp_realcredentialvalue"},
            "raw": {"echo": "ghp_realcredentialvalue"},
        })
        .to_string();

        let withheld = withheld_event_data(&data, "incident-9");

        // Not one byte of the value survives, in any field.
        assert!(!withheld.contains("ghp_realcredentialvalue"));
        // Linkage survives, so the transcript still reconstructs.
        let parsed: serde_json::Value = serde_json::from_str(&withheld).unwrap();
        assert_eq!(parsed["eventType"], "tool_result");
        assert_eq!(parsed["sessionId"], "session-1");
        assert_eq!(parsed["parentToolUseId"], "parent-1");
        assert_eq!(parsed["toolUseId"], "use-1");
        assert_eq!(parsed["toolName"], "Bash");
        assert_eq!(parsed["isError"], true);
        // Every text-bearing field is gone rather than emptied.
        for dropped in ["toolResult", "thinking", "toolInput", "raw"] {
            assert!(parsed.get(dropped).is_none(), "{dropped} survived");
        }
        assert!(parsed["content"].as_str().unwrap().contains("incident-9"));
    }

    #[test]
    fn an_unknown_field_is_dropped_rather_than_carried() {
        // The fail-closed property: a text-bearing field this code has never
        // heard of must not survive withholding just because it is unrecognized.
        let data = serde_json::json!({
            "eventType": "assistant",
            "fieldInventedNextYear": "ghp_realcredentialvalue",
        })
        .to_string();
        let withheld = withheld_event_data(&data, "incident-9");
        assert!(!withheld.contains("ghp_realcredentialvalue"));
        assert!(!withheld.contains("fieldInventedNextYear"));
    }

    #[test]
    fn unparseable_data_is_replaced_rather_than_passed_through() {
        let withheld = withheld_event_data("ghp_realcredentialvalue not json", "incident-9");
        assert!(!withheld.contains("ghp_realcredentialvalue"));
        assert!(withheld.contains("incident-9"));
    }
}
