//! Diagnostic probe: per-event storage breakdown for a (copied) Cairn
//! database. Prints stub/blob/decompressed sizes and a content head per
//! event so archival byte totals can be attributed to specific events.
//! Copy the full MVCC file set ({.db, -wal, -log}) of an idle instance
//! before pointing this at it; never open a live database.
//!
//! Usage: cargo run -p cairn-core --example dump_event_sizes -- /path/to/db

use turso::{Builder, Value};

fn text(v: &Value) -> String {
    match v {
        Value::Text(t) => t.clone(),
        Value::Null => "-".into(),
        Value::Integer(i) => i.to_string(),
        _ => "?".into(),
    }
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Integer(i) => *i,
        _ => 0,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let db_path = std::env::args()
        .nth(1)
        .expect("usage: dump_event_sizes <db>");
    let db = Builder::new_local(&db_path).build().await.unwrap();
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query(
            "SELECT substr(run_id,1,8), event_type, coalesce(storage_mode,'full'),\n                    length(data), coalesce(length(data_blob),0), data, data_blob\n             FROM events ORDER BY created_at ASC, sequence ASC",
            (),
        )
        .await
        .unwrap();

    let (mut total_stub, mut total_blob, mut total_orig) = (0i64, 0i64, 0i64);
    println!(
        "{:<9} {:<14} {:<9} {:>7} {:>7} {:>8}  {}",
        "run", "type", "mode", "stub", "blob", "orig", "head"
    );
    while let Some(row) = rows.next().await.unwrap() {
        let run = text(&row.get_value(0).unwrap());
        let etype = text(&row.get_value(1).unwrap());
        let mode = text(&row.get_value(2).unwrap());
        let stub_len = int(&row.get_value(3).unwrap());
        let blob_len = int(&row.get_value(4).unwrap());
        let (orig, head) = match row.get_value(6).unwrap() {
            Value::Blob(b) => match zstd::stream::decode_all(&b[..]) {
                Ok(d) => {
                    let h: String = String::from_utf8_lossy(&d).chars().take(110).collect();
                    (d.len() as i64, h)
                }
                Err(_) => (0, "<decompress failed>".into()),
            },
            _ => {
                let d = text(&row.get_value(5).unwrap());
                (0, d.chars().take(110).collect())
            }
        };
        total_stub += stub_len;
        total_blob += blob_len;
        total_orig += orig;
        if mode == "gitcoord" && std::env::args().nth(2).as_deref() == Some("--full-gitcoord") {
            println!(
                "--- full gitcoord stub ({etype}, {stub_len}B): {}",
                text(&row.get_value(5).unwrap())
            );
        }
        if std::env::args().nth(2).as_deref() == Some("--full-type")
            && std::env::args().nth(3).as_deref() == Some(etype.as_str())
        {
            let body = if head == "<decompress failed>" || orig == 0 {
                text(&row.get_value(5).unwrap())
            } else if let Value::Blob(b) = row.get_value(6).unwrap() {
                String::from_utf8_lossy(&zstd::stream::decode_all(&b[..]).unwrap()).into_owned()
            } else {
                text(&row.get_value(5).unwrap())
            };
            println!("--- full {etype} ({} B): {body}", body.len());
        }
        println!(
            "{:<9} {:<14} {:<9} {:>7} {:>7} {:>8}  {}",
            run,
            etype,
            mode,
            stub_len,
            blob_len,
            orig,
            head.replace('\n', "\\n")
        );
    }
    println!(
        "totals: stub={total_stub} blob={total_blob} stored={} orig_decompressed={total_orig}",
        total_stub + total_blob
    );
}
