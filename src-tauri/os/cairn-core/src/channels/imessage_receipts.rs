//! iMessage read receipts for messages Cairn sent, read straight from the
//! Messages database.
//!
//! On the Mac that sends, a message Cairn sent is an `is_from_me = 1` row of
//! `chat.db`'s `message` table, and the recipient's read receipt fills in its
//! `date_read`. imsg reports `is_read`/`date_read` only for inbound rows, so
//! this reads the column itself: one read-only `sqlite3` query run where imsg
//! runs, through the same local or placed launcher and the same Full Disk
//! Access, and nothing else. When imsg reports `date_read` for its own sends
//! this module and its fixture are deleted in favour of imsg's output.
//!
//! A receipt is a nicety, never a dependency: an error or an empty answer
//! leaves the outbound row's status and attempts alone and never holds a
//! message back from `delivered`.

use crate::operator::{record_seen, MessageFilter, SeenVia};
use crate::orchestrator::Orchestrator;

/// Seconds from the Unix epoch to Apple's reference date, 2001-01-01.
const APPLE_EPOCH_OFFSET_SECS: i64 = 978_307_200;

/// A provider guid that can be interpolated into the query: imsg's guids are
/// uppercase UUIDs. Anything else is dropped rather than quoted.
pub fn valid_guid(guid: &str) -> bool {
    guid.len() == 36
        && guid
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
}

/// The query for `guids`' receipts, or `None` when no guid survives
/// validation.
pub fn receipt_query(guids: &[String]) -> Option<String> {
    let list = guids
        .iter()
        .filter(|guid| valid_guid(guid))
        .map(|guid| format!("'{guid}'"))
        .collect::<Vec<_>>();
    if list.is_empty() {
        return None;
    }
    Some(format!(
        "SELECT guid, date_read FROM message WHERE is_from_me = 1 AND date_read > 0 AND guid IN ({})",
        list.join(", ")
    ))
}

/// The shell that runs `sql` against the Messages database of whichever
/// machine runs it: `$HOME` expands there, and `sql` stays one argument.
pub fn query_command(sql: &str) -> Vec<String> {
    vec![
        "-c".to_string(),
        r#"exec sqlite3 -readonly -json "$HOME/Library/Messages/chat.db" "$1""#.to_string(),
        "sh".to_string(),
        sql.to_string(),
    ]
}

/// Apple's `date_read` as epoch milliseconds. Current databases store
/// nanoseconds since 2001; older ones stored seconds.
pub fn apple_to_unix_ms(date_read: i64) -> i64 {
    let since_2001_ms = if date_read > 100_000_000_000 {
        date_read / 1_000_000
    } else {
        date_read * 1_000
    };
    since_2001_ms + APPLE_EPOCH_OFFSET_SECS * 1_000
}

/// `sqlite3 -json` output as `(guid, read_at_ms)`. No rows prints nothing.
pub fn parse(stdout: &str) -> Result<Vec<(String, i64)>, String> {
    #[derive(serde::Deserialize)]
    struct Row {
        guid: String,
        date_read: i64,
    }
    if stdout.trim().is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<Row> = serde_json::from_str(stdout)
        .map_err(|error| format!("unreadable chat.db receipt output: {error}"))?;
    Ok(rows
        .into_iter()
        .filter(|row| row.date_read > 0)
        .map(|row| (row.guid, apple_to_unix_ms(row.date_read)))
        .collect())
}

/// Stamp each receipt on its outbound row (once) and mark its message seen.
/// `unread` is `(binding_ref, provider_guid)` of the rows asked about.
/// Returns the message ids that turned seen.
pub async fn apply(
    orch: &Orchestrator,
    channel: &str,
    unread: &[(String, String)],
    receipts: &[(String, i64)],
) -> Result<Vec<String>, String> {
    let mut seen = Vec::new();
    for (guid, read_at) in receipts {
        let Some((binding_ref, _)) = unread.iter().find(|(_, unread)| unread == guid) else {
            continue;
        };
        if !super::ledger::mark_read(&orch.db.local, channel, guid, *read_at).await? {
            continue;
        }
        let Some(id) = super::operator_message::message_id(binding_ref) else {
            continue;
        };
        let moved = record_seen(
            orch,
            MessageFilter::Ids(vec![id.to_string()]),
            SeenVia::Receipt,
            read_at / 1_000,
        )
        .await?;
        seen.extend(moved.into_iter().map(|message| message.id));
    }
    Ok(seen)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GUID: &str = "8F0D3C55-2E3B-4E7B-9C61-0F6A1B2C3D4E";

    #[test]
    fn only_uuid_guids_reach_the_query() {
        assert!(valid_guid(GUID));
        for bad in [
            "",
            "x' OR 1=1 --",
            "8F0D3C55-2E3B-4E7B-9C61-0F6A1B2C3D4",
            "p:0/8F0D3C55-2E3B-4E7B-9C61-0F6A1B2C",
        ] {
            assert!(!valid_guid(bad), "{bad}");
        }
        assert_eq!(receipt_query(&["x'--".to_string()]), None);
        let sql = receipt_query(&[GUID.to_string(), "nope".to_string()]).unwrap();
        assert!(sql.ends_with(&format!("guid IN ('{GUID}')")), "{sql}");
    }

    /// The query text, run against a database with the Messages columns it
    /// reads, returns only Cairn's own read sends among those asked about.
    #[tokio::test]
    async fn the_query_reads_only_read_sends_it_asked_about() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::storage::LocalDb::open(dir.path().join("chat.db"))
            .await
            .unwrap();
        db.execute_batch(
            "CREATE TABLE message (ROWID INTEGER PRIMARY KEY, guid TEXT, is_from_me INTEGER, date_read INTEGER);",
        )
        .await
        .unwrap();
        let other = "11111111-2222-3333-4444-555555555555";
        let inbound = "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE";
        let unasked = "99999999-8888-7777-6666-555555555555";
        db.execute_script(format!(
            "INSERT INTO message (guid, is_from_me, date_read) VALUES
               ('{GUID}', 1, 784000000000000000),
               ('{other}', 1, 0),
               ('{inbound}', 0, 784000000000000000),
               ('{unasked}', 1, 784000000000000000);"
        ))
        .await
        .unwrap();
        let sql =
            receipt_query(&[GUID.to_string(), other.to_string(), inbound.to_string()]).unwrap();
        let rows = db
            .query_all(sql, (), |row| {
                use crate::storage::RowExt;
                Ok((row.text(0)?, row.i64(1)?))
            })
            .await
            .unwrap();
        assert_eq!(rows, vec![(GUID.to_string(), 784000000000000000)]);
    }

    /// `sqlite3 -json` as it prints a receipt, and as it prints no rows.
    #[test]
    fn the_parser_reads_sqlite_json_and_apple_time() {
        let fixture = include_str!("../../tests/fixtures/chatdb-receipts-sqlite3.json");
        let receipts = parse(fixture).unwrap();
        // 784000000000000000 ns after 2001-01-01 is 2025-11-05T01:46:40Z.
        assert_eq!(receipts, vec![(GUID.to_string(), 1_762_307_200_000)]);
        assert_eq!(parse("").unwrap(), Vec::new());
        assert!(parse("not json").is_err());
        assert_eq!(apple_to_unix_ms(1), 978_307_201_000);
    }
}
