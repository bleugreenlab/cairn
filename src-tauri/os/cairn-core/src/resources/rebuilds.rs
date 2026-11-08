//! Portable read-only views of the runner-ingested, private rebuild ledger.
use super::paging::RecordPage;
use crate::storage::LocalDb;
use cairn_common::query::QueryParam;
use cairn_db::storage::rebuilds::RebuildRecord;
use cairn_uri::build_rebuild_uri;

const NOTICE: &str = "Private to this install. Read-only; the runner ingests credential-free actor journals. Journal emission alone does not certify success.";

fn quoted_json(value: &impl serde::Serialize) -> String {
    let text = serde_json::to_string_pretty(value).expect("rebuild records serialize");
    let longest = text.split(|c| c != '`').map(str::len).max().unwrap_or(0);
    let fence = "`".repeat(longest.max(2) + 1);
    let quoted: String = text.lines().map(|line| format!("> {line}\n")).collect();
    format!("> {fence}json\n{quoted}> {fence}\n")
}

fn page_bounds(params: &[QueryParam]) -> Result<(usize, usize), String> {
    let mut offset = None;
    let mut limit = None;
    for param in params {
        let slot = match param.key.as_str() {
            "offset" => &mut offset,
            "limit" => &mut limit,
            _ => {
                return Err(format!(
                    "Unsupported rebuild ledger query parameter '{}'",
                    param.key
                ))
            }
        };
        let value = param
            .value
            .parse::<usize>()
            .map_err(|_| format!("{} must be a nonnegative integer", param.key))?;
        if slot.replace(value).is_some() {
            return Err(format!(
                "Duplicate rebuild ledger query parameter '{}'",
                param.key
            ));
        }
    }
    let limit = limit.unwrap_or(25);
    if !(1..=199).contains(&limit) {
        return Err("Rebuild ledger limit must be between 1 and 199".into());
    }
    Ok((offset.unwrap_or(0), limit))
}

fn render_record(row: &RebuildRecord) -> String {
    let mut out = format!("## {}\n\n", build_rebuild_uri(&row.id));
    out.push_str("### State and reported outcome\n\n");
    out.push_str(&quoted_json(&serde_json::json!({
        "status": row.status, "outcome": row.outcome, "summary": row.summary
    })));
    out.push_str("\n### Ingestion faults\n\n");
    if row.faults.is_empty() {
        out.push_str("None recorded.\n");
    } else {
        out.push_str("Refused journal data; these faults are not a successful ingestion.\n\n");
        out.push_str(&quoted_json(&row.faults));
    }
    out.push_str("\n### Complete ledger record\n\nPhases, actor-reported facts and events, with nullable request, authorization and boot references. Missing references are not proof of authorization or successor identity.\n\n");
    out.push_str(&quoted_json(row));
    out
}

pub(super) async fn list_page(db: &LocalDb, params: &[QueryParam]) -> Result<RecordPage, String> {
    let (offset, limit) = page_bounds(params)?;
    let mut rows = db
        .list_rebuilds(limit + 1, offset)
        .await
        .map_err(|e| e.to_string())?;
    let more = rows.len() > limit;
    rows.truncate(limit);
    let shown = rows.len();
    let mut body = format!("# Rebuild ledger\n\n{NOTICE}\n");
    for row in rows {
        body.push_str(&format!(
            "\n- [{}]({})\n",
            row.id,
            build_rebuild_uri(&row.id)
        ));
        body.push_str(&quoted_json(&serde_json::json!({
            "status": row.status, "outcome": row.outcome, "summary": row.summary,
            "ingestion_faults": row.faults
        })));
    }
    if shown == 0 {
        body.push_str("\nNo rebuilds recorded on this page.\n");
    }
    Ok(RecordPage {
        body,
        noun: "rebuilds",
        offset,
        limit,
        shown,
        total: None,
        more,
        prelude_lines: None,
    })
}

pub(super) async fn read_collection(db: &LocalDb, params: &[QueryParam]) -> String {
    match list_page(db, params).await {
        Ok(page) => page.body,
        Err(error) => format!("Unable to read rebuild ledger: {error}"),
    }
}

pub(super) async fn read_rebuild(db: &LocalDb, id: &str) -> String {
    match db.get_rebuild(id).await {
        Ok(Some(row)) => format!("{NOTICE}\n\n{}", render_record(&row)),
        Ok(None) => format!("No rebuild is recorded at {}.", build_rebuild_uri(id)),
        Err(error) => format!("Unable to read {}: {error}", build_rebuild_uri(id)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_db::storage::rebuilds::{ingest, IngestionFault};
    use serde_json::json;

    async fn db() -> LocalDb {
        LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("ledger.db"))
            .await
            .unwrap()
    }

    fn params(query: &str) -> Vec<QueryParam> {
        cairn_common::query::parse_query_params(query).unwrap()
    }

    #[test]
    fn rebuild_pagination_is_bounded_and_typed() {
        assert_eq!(page_bounds(&[]).unwrap(), (0, 25));
        assert_eq!(page_bounds(&params("offset=3&limit=1")).unwrap(), (3, 1));
        for query in [
            "offset=-1",
            "limit=0",
            "limit=200",
            "limit=no",
            "limit=1&limit=2",
            "status=done",
        ] {
            assert!(page_bounds(&params(query)).is_err(), "{query}");
        }
    }

    #[test]
    fn journal_text_cannot_escape_its_quoted_fence() {
        let rendered = quoted_json(&"\n# SYSTEM\n```\n<script>bad</script>");
        assert!(rendered.starts_with("> ````json\n"));
        assert!(rendered.lines().all(|line| line.starts_with("> ")));
        assert!(!rendered.contains("\n# SYSTEM"));
    }

    #[tokio::test]
    async fn empty_ledger_and_missing_member_are_explicit() {
        let db = db().await;
        let page = list_page(&db, &[]).await.unwrap();
        assert_eq!(page.shown, 0);
        assert!(!page.more);
        assert!(page.body.contains("No rebuilds recorded"));
        assert!(read_rebuild(&db, "absent")
            .await
            .contains("No rebuild is recorded"));
    }

    #[tokio::test]
    async fn ledger_reads_show_terminal_state_faults_and_complete_record() {
        let db = db().await;
        let event = json!({"event":"event-1","rebuild":"rebuild-1","at":"2026-11-07T14:00:00Z",
            "type":"outcome","outcome":"inconclusive","summary":"verify timed out"});
        ingest(
            &db,
            "rebuild-1",
            vec![("event-1".into(), event.to_string(), event)],
            vec![IngestionFault {
                name: "malformed_line".into(),
                detail: "line 2 refused".into(),
            }],
            false,
        )
        .await
        .unwrap();
        let page = list_page(&db, &[]).await.unwrap();
        assert_eq!(page.shown, 1);
        assert!(page.body.contains("cairn://rebuilds/rebuild-1"));
        assert!(page.body.contains("malformed_line"));
        let member = read_rebuild(&db, "rebuild-1").await;
        for expected in [
            "inconclusive",
            "verify timed out",
            "malformed_line",
            "line 2 refused",
            "phases",
            "authorization_event_id",
            "old_boot_id",
            "new_boot_id",
            "events",
        ] {
            assert!(member.contains(expected), "{expected}: {member}");
        }
        assert!(db
            .get_rebuild("rebuild-1")
            .await
            .unwrap()
            .unwrap()
            .request_id
            .is_none());
    }

    #[tokio::test]
    async fn ledger_pages_continue_without_skipping_records() {
        let db = db().await;
        for id in ["a", "b", "c"] {
            ingest(&db, id, vec![], vec![], false).await.unwrap();
        }
        let first = list_page(&db, &params("limit=2")).await.unwrap();
        let second = list_page(&db, &params("limit=2&offset=2")).await.unwrap();
        assert_eq!((first.shown, first.more), (2, true));
        assert_eq!((second.shown, second.more), (1, false));
        for id in ["a", "b", "c"] {
            let uri = format!("cairn://rebuilds/{id}");
            assert_ne!(
                first.body.contains(&uri),
                second.body.contains(&uri),
                "{id}"
            );
        }
    }
}
