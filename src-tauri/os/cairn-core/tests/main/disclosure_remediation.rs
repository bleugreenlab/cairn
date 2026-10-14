//! CAIRN-3828: a seeded disclosure, driven through the whole response.
//!
//! The acceptance bar for the issue, as one test each: the inventory finds every
//! affected store, quarantine actually blocks serving, authority is revoked,
//! derived records are rebuilt from withheld sources, and the incident report
//! never carries the credential it is about.
//!
//! Each test registers a credential unique to it, because tests share this
//! binary and the registry is process-wide.

use crate::common;

use cairn_core::internal::security::remediation::{
    self, sink::Reach, Disclosure, Gate, InventoryRoots, RecordClass, SinkKind, ALL_SINKS,
};
use cairn_core::internal::security::{
    registry, SecretCategory, SecretGuard, SecretId, SecretMaterial,
};
use cairn_db::storage::{quarantine, LocalDb};

/// A value that clears the registry's length and variety thresholds and is
/// distinctive enough that finding it anywhere is unambiguous.
fn credential(tag: &str) -> String {
    format!("sk-live-{tag}-Qa9Zm2Xp7Lr4Kt8Wd3Nv")
}

/// Empty log and scratch roots.
///
/// Load-bearing rather than tidy: the default roots are this machine's real
/// `~/.cairn/logs` and scratch tree, so a test that took them would scan the
/// developer's own log history — slowly, and with results that differ per
/// machine.
fn empty_roots(temp: &tempfile::TempDir) -> InventoryRoots {
    let logs = temp.path().join("logs");
    let scratch = temp.path().join("scratch");
    std::fs::create_dir_all(&logs).unwrap();
    std::fs::create_dir_all(&scratch).unwrap();
    InventoryRoots {
        log_dir: logs,
        scratch_root: scratch,
    }
}

fn register(id: &str, value: &str) -> SecretGuard<'static> {
    registry()
        .register(
            SecretId::new(id),
            SecretCategory::ProviderKey,
            "disclosure remediation test",
            SecretMaterial::from_string(value.to_string()),
        )
        .expect("test credential is registerable")
}

/// Seed one transcript event whose `data` carries `value` in the clear.
///
/// Written straight to the table on purpose: this is the historical record the
/// issue exists for, one written before the credential was ever registered, so
/// it must not go through the sanitizing write path that would clean it.
async fn seed_event(db: &LocalDb, event_id: &str, value: &str) {
    let now = chrono::Utc::now().timestamp();
    let run_id = format!("run-for-{event_id}");
    db.execute(
        "INSERT INTO runs (id, status, created_at, updated_at) VALUES (?1, 'completed', ?2, ?2)",
        (run_id.as_str(), now),
    )
    .await
    .expect("seed run");
    let data = serde_json::json!({
        "eventType": "tool_result",
        "toolUseId": "use-1",
        "toolResult": format!("connecting with {value}"),
        "isError": false,
    })
    .to_string();
    db.execute(
        "INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at, \
         storage_mode) VALUES (?1, ?2, 1, ?3, 'tool_result', ?4, ?3, 'full')",
        (event_id, run_id.as_str(), now, data.as_str()),
    )
    .await
    .expect("seed event");
}

async fn read_event_data(db: &LocalDb, event_id: &str) -> String {
    let id = event_id.to_string();
    let columns = cairn_db::storage::events::columns::EVENT_COLUMNS;
    let sql = format!("SELECT {columns} FROM events WHERE id = ?1");
    let events = db
        .read(move |conn| {
            let id = id.clone();
            let sql = sql.clone();
            Box::pin(async move {
                let mut out = Vec::new();
                let mut rows = conn.query(&sql, (id,)).await?;
                while let Some(row) = rows.next().await? {
                    out.push(cairn_db::storage::events::columns::event_from_row(&row)?);
                }
                cairn_db::storage::DbResult::Ok(out)
            })
        })
        .await
        .expect("read events");
    // The canonical read funnel every event consumer goes through.
    let reconstructed = cairn_db::storage::reconstruct_events(db, events).await;
    reconstructed[0].data.clone()
}

#[tokio::test]
async fn a_seeded_disclosure_is_inventoried_contained_and_reported() {
    let (_temp, db) = common::migrated_db().await;
    let value = credential("inventory");
    let _guard = register("web-provider:test:INVENTORY", &value);

    seed_event(&db, "event-dirty", &value).await;
    // A second event with nothing to find, so the inventory has to discriminate
    // rather than sweep everything into the incident.
    seed_event(&db, "event-clean", "nothing sensitive here").await;

    // A historical log file with the credential in the clear — the residual this
    // issue exists for, written before anything registered the value.
    let roots = empty_roots(&_temp);
    std::fs::write(
        roots.log_dir.join("cairn-runner.2026-01-01.jsonl"),
        format!("{{\"message\":\"resolved {value}\"}}\n"),
    )
    .unwrap();

    let response = remediation::respond_in(
        &db,
        &Disclosure::from_crossing(
            SecretId::new("web-provider:test:INVENTORY"),
            Some(SecretCategory::ProviderKey),
            "process_output",
        ),
        &roots,
    )
    .await
    .expect("response completes");

    // It found the dirty record and only the dirty record.
    let events: Vec<_> = response
        .inventory
        .records
        .iter()
        .filter(|record| record.sink == SinkKind::TranscriptEvent)
        .collect();
    assert_eq!(events.len(), 1, "expected exactly the seeded dirty event");
    assert_eq!(events[0].locator, "event-dirty");
    assert_eq!(events[0].occurrences, 1);

    // And the log file on disk, which no crossing could ever have covered.
    let logs: Vec<_> = response
        .inventory
        .records
        .iter()
        .filter(|record| record.sink == SinkKind::ProcessLog)
        .collect();
    assert_eq!(logs.len(), 1, "the historical log file was not inventoried");
    assert!(logs[0].locator.ends_with("cairn-runner.2026-01-01.jsonl"));

    // Every store this build cannot reach is named, with a reason. An operator
    // reading the incident learns what was NOT looked at, which is the whole
    // difference between a complete inventory and a falsely reassuring one.
    let manual_sinks: Vec<SinkKind> = response
        .inventory
        .manual
        .iter()
        .map(|(sink, _)| *sink)
        .collect();
    for sink in ALL_SINKS {
        if matches!(sink.reach(), Reach::Manual(_)) {
            assert!(
                manual_sinks.contains(sink),
                "{sink} is unreachable but the incident does not tell the operator so"
            );
        }
    }

    // Rotation is still outstanding: revoking a lease does not end a credential
    // the provider still accepts.
    assert_eq!(response.rotation.provider, "web-provider");
    assert!(!response.rotation.revocation_suffices);
}

#[tokio::test]
async fn a_quarantined_event_stops_being_served() {
    let (_temp, db) = common::migrated_db().await;
    let value = credential("serving");
    let _guard = register("web-provider:test:SERVING", &value);

    seed_event(&db, "event-served", &value).await;

    // Before: the read funnel hands back the credential in the clear. This is
    // the hazard, asserted rather than assumed.
    let before = read_event_data(&db, "event-served").await;
    assert!(
        before.contains(&value),
        "the seeded record should start out dirty, or this test proves nothing"
    );

    remediation::respond_in(
        &db,
        &Disclosure::declared(SecretId::new("web-provider:test:SERVING"), None),
        &empty_roots(&_temp),
    )
    .await
    .expect("response completes");

    // After: the same funnel withholds it.
    let after = read_event_data(&db, "event-served").await;
    assert!(!after.contains(&value), "the credential is still served");
    assert!(after.contains(quarantine::WITHHELD_PREFIX));

    // The row itself is untouched. Withholding is a read-path decision; the
    // record of what happened is not rewritten to make a problem go away.
    let stored: String = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT data FROM events WHERE id = 'event-served'", ())
                    .await?;
                let row = rows.next().await?.expect("row");
                cairn_db::storage::RowExt::text(&row, 0)
            })
        })
        .await
        .unwrap();
    assert!(
        stored.contains(&value),
        "the stored row was rewritten; quarantine must withhold, not edit"
    );
}

#[tokio::test]
async fn quarantine_survives_a_restart_and_a_rotation() {
    // The property that rules out scrubbing-on-read as the implementation. The
    // registry is process-local and a rotated credential is never registered
    // again, so a gate that matched on registered values would fail open
    // precisely when the disclosure is oldest.
    let (_temp, db) = common::migrated_db().await;
    let value = credential("restart");
    let guard = register("web-provider:test:RESTART", &value);

    seed_event(&db, "event-restart", &value).await;
    remediation::respond_in(
        &db,
        &Disclosure::declared(SecretId::new("web-provider:test:RESTART"), None),
        &empty_roots(&_temp),
    )
    .await
    .expect("response completes");

    // Rotate: the credential is unregistered and never comes back.
    drop(guard);
    // Restart: this database's contribution to the process-local set is
    // emptied, then armed from the database the way startup does it.
    quarantine::quarantine().install_for(db.path(), quarantine::QuarantineSet::default());
    remediation::arm_quarantine(&db).await.expect("armed");

    let after = read_event_data(&db, "event-restart").await;
    assert!(
        !after.contains(&value),
        "a rotated credential's record went back to being served after a restart"
    );
}

#[tokio::test]
async fn the_incident_report_never_carries_the_credential() {
    // The prohibition that matters most: remediating a disclosure must not
    // create a fresh durable copy of it. Every column of every table this
    // subsystem writes is checked, not just the ones that looked risky.
    let (_temp, db) = common::migrated_db().await;
    let value = credential("report");
    let _guard = register("web-provider:test:REPORT", &value);

    seed_event(&db, "event-report", &value).await;
    let response = remediation::respond_in(
        &db,
        &Disclosure::from_crossing(
            SecretId::new("web-provider:test:REPORT"),
            Some(SecretCategory::ProviderKey),
            "external_tool",
        ),
        &empty_roots(&_temp),
    )
    .await
    .expect("response completes");

    for table in [
        "disclosure_incidents",
        "disclosure_affected_records",
        "disclosure_actions",
        "quarantined_records",
    ] {
        let dumped = dump_table(&db, table).await;
        assert!(
            !dumped.contains(&value),
            "{table} carries the disclosed credential; the remediation would need remediating"
        );
    }

    // And the in-memory report an operator reads is clean too.
    assert!(!format!("{response:?}").contains(&value));
}

#[tokio::test]
async fn every_affected_record_gets_the_disposition_its_store_earns() {
    let (_temp, db) = common::migrated_db().await;
    let value = credential("disposition");
    let _guard = register("web-provider:test:DISPOSITION", &value);

    seed_event(&db, "event-disposition", &value).await;
    let response = remediation::respond_in(
        &db,
        &Disclosure::declared(SecretId::new("web-provider:test:DISPOSITION"), None),
        &empty_roots(&_temp),
    )
    .await
    .expect("response completes");

    let recorded = remediation::store::affected_for(&db, &response.incident_id)
        .await
        .expect("affected records");
    assert!(!recorded.is_empty());
    for (sink, _locator, _occurrences, disposition) in recorded {
        let kind = ALL_SINKS
            .iter()
            .find(|candidate| candidate.as_str() == sink)
            .unwrap_or_else(|| panic!("{sink} is not in the sink taxonomy"));
        // A source record is withheld, never edited. This is the "do not
        // silently rewrite authored state" rule, checked on the durable record
        // rather than on the code path that wrote it.
        if kind.record_class() == RecordClass::Source {
            // A source record is withheld, never edited — but only where a read
            // gate exists to withhold it. Where none does, the honest record is
            // `reported`: found, named, and still served.
            let expected = match kind.gate() {
                Gate::Withholds => "quarantined",
                Gate::Reports(_) => "reported",
            };
            assert_eq!(
                disposition, expected,
                "{sink} is a source store but its record was {disposition}"
            );
        }
    }
}

/// The gap this taxonomy exists to state out loud: a store with no read gate is
/// found and named, and its records keep being served. Asserted on serving
/// behaviour rather than on a label, because a label is exactly what would
/// otherwise paper over it.
#[tokio::test]
async fn a_store_with_no_read_gate_is_reported_rather_than_claimed_contained() {
    let (_temp, db) = common::migrated_db().await;
    let value = credential("ungated");
    let _guard = register("web-provider:test:UNGATED", &value);

    db.execute(
        "INSERT INTO messages (id, channel_type, channel_id, sender_name, content, created_at) \
         VALUES ('msg-ungated', 'project', 'proj-1', 'system', ?1, 1)",
        (format!("the provider replied with {value}").as_str(),),
    )
    .await
    .expect("seed message");

    let before = outbox_ops(&db, "messages", "msg-ungated").await;

    let response = remediation::respond_in(
        &db,
        &Disclosure::declared(SecretId::new("web-provider:test:UNGATED"), None),
        &empty_roots(&_temp),
    )
    .await
    .expect("response completes");

    // Found: the operator is told exactly which record carries it.
    let messages: Vec<_> = response
        .inventory
        .records
        .iter()
        .filter(|record| record.sink == SinkKind::Message)
        .collect();
    assert_eq!(messages.len(), 1, "the message was not inventoried");
    assert_eq!(messages[0].locator, "msg-ungated");

    // Not claimed contained: no quarantine row, and the response says how many
    // records it is leaving readable.
    let recorded = remediation::store::affected_for(&db, &response.incident_id)
        .await
        .expect("affected records");
    let message_row = recorded
        .iter()
        .find(|(sink, ..)| sink == SinkKind::Message.as_str())
        .expect("message recorded");
    assert_eq!(
        message_row.3, "reported",
        "an ungated store must not be recorded as contained"
    );
    assert!(response.reported >= 1);
    assert!(response.leaves_records_served());

    // The incident summary must not claim containment either. An operator reads
    // the status before they read the record table, so a "contained" here would
    // undo the per-record honesty above.
    let status = remediation::store::get_incident(&db, &response.incident_id)
        .await
        .expect("incident")
        .expect("present")
        .status;
    assert_eq!(
        status,
        remediation::IncidentStatus::ActionRequired,
        "an incident with a record still being served must not read as contained"
    );

    // The derived copy is EVICTED, not reindexed. Reindexing re-reads the
    // source, and this source still returns the credential, so an upsert would
    // write it straight back into the full-text index — taking a stale copy of
    // the disclosure and refreshing it.
    let queued = outbox_ops(&db, "messages", "msg-ungated").await;
    assert_eq!(
        queued[before.len()..],
        ["delete".to_string()],
        "an ungated source must have its indexed copy deleted, never reindexed from plaintext"
    );

    // And the record really is still served, which is the fact the label now
    // tells the truth about.
    let served = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT content FROM messages WHERE id = 'msg-ungated'", ())
                    .await?;
                let row = rows.next().await?.expect("row");
                cairn_db::storage::RowExt::text(&row, 0)
            })
        })
        .await
        .unwrap();
    assert!(served.contains(&value));
}

/// A gated source is the other half of the rule: its read gate answers with the
/// withholding notice, so reindexing it genuinely produces a clean document and
/// the indexed copy is rebuilt rather than dropped.
#[tokio::test]
async fn a_gated_source_is_reindexed_rather_than_evicted() {
    let (_temp, db) = common::migrated_db().await;
    let value = credential("gatedindex");
    let _guard = register("web-provider:test:GATEDINDEX", &value);

    seed_event(&db, "event-gatedindex", &value).await;
    let before = outbox_ops(&db, "events", "event-gatedindex").await;

    remediation::respond_in(
        &db,
        &Disclosure::declared(SecretId::new("web-provider:test:GATEDINDEX"), None),
        &empty_roots(&_temp),
    )
    .await
    .expect("response completes");

    let queued = outbox_ops(&db, "events", "event-gatedindex").await;
    assert_eq!(
        queued[before.len()..],
        ["upsert".to_string()],
        "a gated source's indexed copy is rebuilt through its read gate, not dropped"
    );
}

/// An archived event is reconstructed from its compressed blob, and that
/// regeneration overwrites whatever `data` held. A gate that ran before
/// reconstruction would be silently undone for exactly these rows.
#[tokio::test]
async fn a_quarantined_archived_event_is_withheld_after_reconstruction() {
    let (_temp, db) = common::migrated_db().await;
    let value = credential("archived");
    let _guard = register("web-provider:test:ARCHIVED", &value);

    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO runs (id, status, created_at, updated_at) VALUES ('run-arch', 'completed', \
         ?1, ?1)",
        (now,),
    )
    .await
    .expect("seed run");

    let data = serde_json::json!({
        "eventType": "tool_result",
        "toolUseId": "use-arch",
        "toolResult": format!("connecting with {value}"),
        "isError": false,
    })
    .to_string();
    let blob = cairn_db::storage::compress(data.as_bytes()).expect("compress");

    // The archived shape: `data` is an empty object and the real bytes live in
    // `data_blob`, exactly as teardown leaves a cold row.
    db.execute(
        "INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at, \
         storage_mode, data_blob, codec) VALUES ('event-arch', 'run-arch', 1, 1, \
         'tool_result', '{}', 1, 'zstd', ?1, ?2)",
        (blob, cairn_db::storage::CODEC_ZSTD_V1),
    )
    .await
    .expect("seed archived event");

    // It really does reconstruct to the credential before remediation.
    assert!(read_event_data(&db, "event-arch").await.contains(&value));

    remediation::respond_in(
        &db,
        &Disclosure::declared(SecretId::new("web-provider:test:ARCHIVED"), None),
        &empty_roots(&_temp),
    )
    .await
    .expect("response completes");

    let after = read_event_data(&db, "event-arch").await;
    assert!(
        !after.contains(&value),
        "an archived event regenerated its credential past the quarantine gate"
    );
    assert!(after.contains(quarantine::WITHHELD_PREFIX));
}

#[tokio::test]
async fn the_response_is_journaled_in_order() {
    let (_temp, db) = common::migrated_db().await;
    let value = credential("audit");
    let _guard = register("web-provider:test:AUDIT", &value);

    seed_event(&db, "event-audit", &value).await;
    let response = remediation::respond_in(
        &db,
        &Disclosure::declared(SecretId::new("web-provider:test:AUDIT"), None),
        &empty_roots(&_temp),
    )
    .await
    .expect("response completes");

    let actions = remediation::store::actions_for(&db, &response.incident_id)
        .await
        .expect("journal");
    let names: Vec<&str> = actions.iter().map(|entry| entry.action.as_str()).collect();
    // Revocation precedes the inventory, and the inventory precedes containment.
    // The order is the mechanism, so the journal pins it.
    assert_eq!(
        names,
        vec!["revoke", "inventory", "contain", "rotation_required"]
    );
    for (index, entry) in actions.iter().enumerate() {
        assert_eq!(entry.seq, index as i64 + 1);
    }

    // Resolving takes an operator saying the credential was rotated. Nothing
    // here can observe a third party's key store, so nothing here infers it.
    let incident = remediation::store::get_incident(&db, &response.incident_id)
        .await
        .expect("incident")
        .expect("present");
    assert!(incident.rotation_required);
    assert_eq!(incident.status, remediation::IncidentStatus::Contained);

    assert!(
        remediation::confirm_rotation(&db, &response.incident_id, "operator")
            .await
            .expect("confirmable")
    );
    let resolved = remediation::store::get_incident(&db, &response.incident_id)
        .await
        .expect("incident")
        .expect("present")
        .status;
    assert_eq!(resolved, remediation::IncidentStatus::Resolved);
}

#[tokio::test]
async fn a_released_record_is_served_again() {
    let (_temp, db) = common::migrated_db().await;
    let value = credential("release");
    let _guard = register("web-provider:test:RELEASE", &value);

    seed_event(&db, "event-release", &value).await;
    let response = remediation::respond_in(
        &db,
        &Disclosure::declared(SecretId::new("web-provider:test:RELEASE"), None),
        &empty_roots(&_temp),
    )
    .await
    .expect("response completes");

    assert!(!read_event_data(&db, "event-release").await.contains(&value));

    // An operator can decide the history is worth more than the residue — for a
    // credential that is now dead, it usually is — and that decision is theirs,
    // recorded, and reversible.
    assert!(remediation::release(
        &db,
        &response.incident_id,
        SinkKind::TranscriptEvent,
        "event-release",
        "operator",
    )
    .await
    .expect("release"));

    assert!(read_event_data(&db, "event-release").await.contains(&value));
}

/// The search-outbox ops queued for one record, oldest first.
///
/// Read before and after a response so a test asserts on what the *response*
/// enqueued. Seeding a row fires the ordinary insert trigger, so a bare
/// after-the-fact count would pass just as happily if the response enqueued
/// nothing at all.
async fn outbox_ops(
    db: &LocalDb,
    source_table: &'static str,
    source_id: &'static str,
) -> Vec<String> {
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT op FROM search_outbox WHERE source_table = ?1 AND source_id = ?2 \
                     ORDER BY created_at, id",
                    (source_table, source_id),
                )
                .await?;
            let mut ops = Vec::new();
            while let Some(row) = rows.next().await? {
                ops.push(cairn_db::storage::RowExt::text(&row, 0)?);
            }
            cairn_db::storage::DbResult::Ok(ops)
        })
    })
    .await
    .unwrap()
}

async fn dump_table(db: &LocalDb, table: &'static str) -> String {
    db.read(move |conn| {
        Box::pin(async move {
            let mut out = String::new();
            let mut rows = conn.query(&format!("SELECT * FROM {table}"), ()).await?;
            while let Some(row) = rows.next().await? {
                let mut index = 0;
                while let Ok(value) = row.get_value(index) {
                    out.push_str(&format!("{value:?}"));
                    index += 1;
                }
            }
            cairn_db::storage::DbResult::Ok(out)
        })
    })
    .await
    .expect("dump")
}
