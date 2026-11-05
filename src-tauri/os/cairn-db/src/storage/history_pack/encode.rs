//! The one JSON encoding of a stored row, shared by the project export and the
//! history pack.
//!
//! An `events` row leaves the database as JSON in two places: the export's
//! `tables/events.jsonl` and a history pack's `events` section. Both build the
//! object here, in two halves that match when each half's inputs exist:
//! [`event_columns_object`] runs while the row is in hand (inside the page's
//! read), and [`finish_event_object`] once reconstruction has decided what the
//! row's `data` is (after the read has released). The same stored row therefore
//! encodes to the same bytes in either place, which is what lets a pack row be
//! checked against an export row and a pack-sourced read against a hot one.
//!
//! [`stored_value`] is the typed inverse of [`json_value`], driven by the
//! column's declared type: a `BLOB` column was written as base64 text and
//! decodes back to bytes.

use base64::Engine;
use serde_json::{json, Map, Value as Json};
use turso::Value;

use crate::models::Event;
use crate::storage::events::Reconstruction;

/// The two `events` columns whose exported value is decided by reconstruction
/// rather than read from the row: `data` becomes the reconstructed text and
/// `data_blob` is null.
pub const RECONSTRUCTED_COLUMNS: [&str; 2] = ["data", "data_blob"];

/// One stored value as JSON. A blob is base64 text; a non-finite real, which
/// JSON cannot carry, is null.
pub fn json_value(value: &Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Integer(n) => Json::from(*n),
        Value::Real(n) => serde_json::Number::from_f64(*n)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::Text(text) => Json::String(text.clone()),
        Value::Blob(bytes) => Json::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
    }
}

/// The stored value a [`json_value`] encoding came from, given the column's
/// declared type. A `BLOB` column's base64 text decodes back to bytes (text
/// that is not base64 stays text, which is what the column would have held had
/// it been written as text). A number is an integer when it was one and a real
/// otherwise, so a real holding a whole number stays real.
pub fn stored_value(value: &Json, decl_type: &str) -> Value {
    match value {
        Json::Null => Value::Null,
        Json::Bool(flag) => Value::Integer(i64::from(*flag)),
        Json::Number(number) => match number.as_i64() {
            Some(integer) => Value::Integer(integer),
            None => Value::Real(number.as_f64().unwrap_or_default()),
        },
        Json::String(text) if decl_type.eq_ignore_ascii_case("BLOB") => {
            match base64::engine::general_purpose::STANDARD.decode(text) {
                Ok(bytes) => Value::Blob(bytes),
                Err(_) => Value::Text(text.clone()),
            }
        }
        Json::String(text) => Value::Text(text.clone()),
        other => Value::Text(other.to_string()),
    }
}

/// A fetched `events` row as its output object, every column but
/// [`RECONSTRUCTED_COLUMNS`], keyed by column name. `values` is index-aligned
/// with `columns`.
pub fn event_columns_object(columns: &[String], values: &[Value]) -> Map<String, Json> {
    let mut object = Map::with_capacity(columns.len() + 4);
    for (name, value) in columns.iter().zip(values) {
        if !RECONSTRUCTED_COLUMNS.contains(&name.as_str()) {
            object.insert(name.clone(), json_value(value));
        }
    }
    object
}

/// Complete an [`event_columns_object`] with what reconstruction made of the
/// row: `data` is the reconstructed text, `data_blob` is null, `reconstruction`
/// labels the outcome, and a row reconstruction could not resolve also carries
/// `unresolved: true` and its archival columns exactly as `stored`, so whatever
/// the reader could not resolve stays in the record for someone who can.
///
/// A withheld row carries no stored form: the label wins over the outcome
/// underneath it, so a quarantined credential is never copied out.
pub fn finish_event_object(
    object: &mut Map<String, Json>,
    stored: &Event,
    reconstructed_data: String,
    outcome: Reconstruction,
) {
    object.insert("data".to_string(), Json::String(reconstructed_data));
    object.insert("data_blob".to_string(), Json::Null);
    object.insert(
        "reconstruction".to_string(),
        Json::String(outcome_label(outcome).to_string()),
    );
    if outcome.is_unresolved() {
        object.insert("unresolved".to_string(), Json::Bool(true));
        object.insert("stored".to_string(), stored_form(stored));
    }
}

/// The label [`finish_event_object`] writes for an outcome.
pub fn outcome_label(outcome: Reconstruction) -> &'static str {
    match outcome {
        Reconstruction::Stored => "stored_plain",
        Reconstruction::Restored => "restored",
        Reconstruction::Degraded => "degraded",
        Reconstruction::Undecodable => "undecodable",
        Reconstruction::Withheld => "withheld",
    }
}

/// The outcome an [`outcome_label`] names.
pub fn outcome_from_label(label: &str) -> Option<Reconstruction> {
    Some(match label {
        "stored_plain" => Reconstruction::Stored,
        "restored" => Reconstruction::Restored,
        "degraded" => Reconstruction::Degraded,
        "undecodable" => Reconstruction::Undecodable,
        "withheld" => Reconstruction::Withheld,
        _ => return None,
    })
}

/// The archival columns of an unresolved row exactly as stored.
pub fn stored_form(event: &Event) -> Json {
    json!({
        "storage_mode": event.storage_mode,
        "content_commit": event.content_commit,
        "content_render_sha": event.content_render_sha,
        "codec": event.codec,
        "data": event.data,
        "data_blob": event
            .data_blob
            .as_ref()
            .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_value_type_round_trips_through_its_declared_type() {
        let cases = [
            (Value::Null, "TEXT"),
            (Value::Integer(-7), "INTEGER"),
            (Value::Integer(i64::MAX), "INTEGER"),
            (Value::Real(2.5), "REAL"),
            (Value::Real(3.0), "REAL"),
            (Value::Text("héllo\n\"q\"".to_string()), "TEXT"),
            (Value::Blob(vec![0, 1, 2, 255, 254]), "BLOB"),
            (Value::Blob(Vec::new()), "BLOB"),
        ];
        for (value, decl) in cases {
            let encoded = json_value(&value);
            // Through text, as a pack line carries it.
            let reparsed: Json = serde_json::from_str(&encoded.to_string()).unwrap();
            assert_eq!(stored_value(&reparsed, decl), value, "{decl} {encoded}");
        }
    }

    #[test]
    fn a_non_finite_real_encodes_as_null() {
        assert_eq!(json_value(&Value::Real(f64::NAN)), Json::Null);
        assert_eq!(json_value(&Value::Real(f64::INFINITY)), Json::Null);
    }

    #[test]
    fn text_in_a_blob_column_that_is_not_base64_stays_text() {
        assert_eq!(
            stored_value(&Json::String("not base64!".into()), "BLOB"),
            Value::Text("not base64!".into())
        );
    }

    #[test]
    fn every_label_names_its_outcome() {
        for outcome in [
            Reconstruction::Stored,
            Reconstruction::Restored,
            Reconstruction::Degraded,
            Reconstruction::Undecodable,
            Reconstruction::Withheld,
        ] {
            assert_eq!(outcome_from_label(outcome_label(outcome)), Some(outcome));
        }
    }
}
