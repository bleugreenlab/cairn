//! The operator's row writes into a thread's database, made through an
//! instrument: a page's frame (`pages::write`) or the app's database browser
//! (`db_browser`).
//!
//! Both are one authority class (cairn/5517, cairn/5914): a verified operator
//! writing rows of exactly one thread's tables through the row grammar
//! (`append` to a table, `patch` or `delete` of a row or of a `where=`-scoped
//! set of rows), with no SQL, no commit, and no approval boundary. They differ
//! only in which tables an instrument may name (a page's manifest; the one
//! thread the browser is showing), so each gate is its caller's. Everything
//! else lives here, once: reading a change as a row write, the dispatch that
//! carries the typed origin (the only place one is built outside tests), and
//! the journal that attributes each row.

use cairn_common::authorization::AuthorityMutation;
use cairn_common::protocol::{DispatchOrigin, RowWriteOrigin};
use cairn_common::uri::{parse_uri, CairnResource};
use serde_json::{Map, Value};

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;

/// One change as its instrument sent it, every key past `target`, `mode` and
/// `payload` refused.
pub(crate) struct Change<'a> {
    pub object: &'a Map<String, Value>,
    pub target: String,
    pub mode: String,
}

/// `change` read as `{target, mode, payload}`, or the refusal. `noun` names the
/// instrument ("a page").
pub(crate) fn read_change<'a>(change: &'a Value, noun: &str) -> Result<Change<'a>, String> {
    let object = change
        .as_object()
        .ok_or_else(|| "each change is an object {target, mode, payload}".to_string())?;
    if let Some(key) = object
        .keys()
        .find(|key| !matches!(key.as_str(), "target" | "mode" | "payload"))
    {
        return Err(format!(
            "{noun}'s change carries target, mode and payload only, not `{key}`"
        ));
    }
    let text = |key: &str| {
        object
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string()
    };
    Ok(Change {
        object,
        target: text("target"),
        mode: text("mode"),
    })
}

/// A thread-database table or row that a canonical write target names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RowTarget {
    /// The project segment, as the URI spelled it.
    pub project: String,
    pub thread: String,
    pub table: String,
    /// One row (`.../<table>/<pk>`) rather than the table.
    pub row: bool,
    /// The table, scoped by `where=` (a collection patch or delete).
    pub scoped: bool,
}

impl RowTarget {
    /// What `mode` does to this target, or `None` when the row grammar has no
    /// such write: `append` to a table, `patch` or `delete` of a row or of a
    /// `where=`-scoped table.
    pub(crate) fn mutation(&self, mode: &str) -> Option<AuthorityMutation> {
        match (self.row, self.scoped, mode) {
            (false, false, "append") => Some(AuthorityMutation::Create),
            (true, false, "patch") | (false, true, "patch") => Some(AuthorityMutation::Update),
            (true, false, "delete") | (false, true, "delete") => Some(AuthorityMutation::Delete),
            _ => None,
        }
    }

    /// The canonical collection URI the change writes into.
    pub(crate) fn collection(&self) -> String {
        cairn_common::uri::build_db_table_uri(
            &format!(
                "{}/db",
                cairn_common::uri::build_thread_uri(&self.project, &self.thread)
            ),
            &self.table,
        )
    }
}

/// A canonical, home-resolved `target` as a thread-database row target.
/// `Ok(None)` when it names anything else; a refusal for a fragment, or for
/// any query but the `where=` that scopes a collection patch or delete (the
/// write path parses and enforces the `where=` itself).
pub(crate) fn row_target(resolved: &str, target: &str) -> Result<Option<RowTarget>, String> {
    if resolved.contains('#') {
        return Err(format!("{target}: a write target carries no fragment"));
    }
    let scoped = match resolved.split_once('?') {
        None => false,
        Some((_, query)) => {
            let only_where = cairn_common::query::parse_query_params(query)
                .is_ok_and(|params| params.iter().all(|param| param.key == "where"));
            if !only_where {
                return Err(format!(
                    "{target}: a write target's only query is where=, scoping a patch or delete of a table's rows"
                ));
            }
            true
        }
    };
    let resource = parse_uri(resolved)
        .and_then(|parsed| crate::resources::delegate_thread_descendant(parsed).ok());
    Ok(match resource {
        Some(CairnResource::ThreadDbTable {
            project,
            name,
            table,
        }) => Some(RowTarget {
            project,
            thread: name,
            table,
            row: false,
            scoped,
        }),
        Some(CairnResource::ThreadDbRow {
            project,
            name,
            table,
            ..
        }) => Some(RowTarget {
            project,
            thread: name,
            table,
            row: true,
            scoped,
        }),
        _ => None,
    })
}

/// One gated change, canonical and ready to dispatch.
pub(crate) struct GatedChange {
    pub item: Value,
    pub mutation: AuthorityMutation,
    /// The canonical row collection the change writes into.
    pub collection: String,
}

impl GatedChange {
    /// `change` with its target replaced by the canonical `resolved`.
    pub(crate) fn new(
        change: &Change<'_>,
        resolved: String,
        target: &RowTarget,
        mutation: AuthorityMutation,
    ) -> Self {
        let mut item = change.object.clone();
        item.insert("target".to_string(), Value::String(resolved));
        Self {
            item: Value::Object(item),
            mutation,
            collection: target.collection(),
        }
    }

    pub(crate) fn target(&self) -> Option<&str> {
        self.item.get("target").and_then(Value::as_str)
    }
}

/// Why an instrument's write input was not taken.
#[derive(Debug)]
pub(crate) enum InputError {
    /// The input is malformed.
    BadRequest(String),
    /// The input carries a key no instrument may send.
    Refused(String),
}

/// The write input's changes and atomic flag, refusing every other key.
/// `command` names the door (`page_write`), `noun` the instrument ("a page").
pub(crate) fn split_input(
    input: &Value,
    command: &str,
    noun: &str,
) -> Result<(Vec<Value>, Option<bool>), InputError> {
    let object = input.as_object().ok_or_else(|| {
        InputError::BadRequest(format!(
            "{command}'s input is an object {{changes, atomic?}}"
        ))
    })?;
    let extra: Vec<&str> = object
        .keys()
        .map(String::as_str)
        .filter(|key| !matches!(*key, "changes" | "atomic"))
        .collect();
    if !extra.is_empty() {
        return Err(InputError::Refused(format!(
            "a write through {noun} carries only changes and atomic; {} refused",
            extra
                .iter()
                .map(|key| format!("`{key}` is"))
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    let changes = object
        .get("changes")
        .and_then(Value::as_array)
        .filter(|changes| !changes.is_empty())
        .cloned()
        .ok_or_else(|| {
            InputError::BadRequest(format!("{command} requires a non-empty `changes` array"))
        })?;
    let atomic = match object.get("atomic") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(atomic)) => Some(*atomic),
        Some(_) => {
            return Err(InputError::BadRequest("`atomic` is a boolean".to_string()));
        }
    };
    Ok((changes, atomic))
}

/// Dispatch gated changes through the write path as `origin`, returning the
/// write path's raw change report. The write path confines a row-write origin
/// to row writes in its one thread and never lets it cross an approval
/// boundary, whatever the gate above it admitted.
pub(crate) async fn dispatch(
    orch: &Orchestrator,
    origin: &RowWriteOrigin,
    gated: &[GatedChange],
    atomic: Option<bool>,
) -> String {
    let mut payload = serde_json::json!({
        "changes": gated.iter().map(|change| change.item.clone()).collect::<Vec<_>>(),
    });
    if let Some(atomic) = atomic {
        payload["atomic"] = Value::Bool(atomic);
    }
    let request = McpCallbackRequest {
        tool: "write".to_string(),
        payload,
        origin: Some(DispatchOrigin::RowWrite(Box::new(origin.clone()))),
        ..Default::default()
    };
    crate::mcp::handlers::write::handle_write(orch, &request).await
}

/// The URIs a change report names as written: each applied change's row URI
/// (every row's, for a bulk append), or its target when it names none.
pub(crate) fn written_uris(report: &Value) -> Vec<String> {
    report
        .get("applied")
        .and_then(Value::as_array)
        .map(|applied| {
            applied
                .iter()
                .flat_map(|change| {
                    let mut uris: Vec<String> = change
                        .pointer("/data/uris")
                        .and_then(Value::as_array)
                        .map(|uris| {
                            uris.iter()
                                .filter_map(Value::as_str)
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default();
                    if uris.is_empty() {
                        uris.extend(
                            change
                                .pointer("/data/uri")
                                .or_else(|| change.get("target"))
                                .and_then(Value::as_str)
                                .map(str::to_string),
                        );
                    }
                    uris
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Each failure a change report carries, as `target: error`.
pub(crate) fn failures(report: &Value) -> Vec<String> {
    report
        .get("failures")
        .and_then(Value::as_array)
        .map(|failures| {
            failures
                .iter()
                .map(|failure| {
                    format!(
                        "{}: {}",
                        failure
                            .get("target")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                        failure
                            .get("error")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Whether a change report asks for an answer: unreachable past a row gate,
/// and closed if it is ever reached, since an instrument has no one to answer.
pub(crate) fn is_blocking(report: &Value) -> bool {
    report
        .get("blocking")
        .is_some_and(|blocking| !blocking.is_null())
}

/// Journal every applied change as `origin`'s actor, through its instrument:
/// one event per row, matched to its gated change by index.
pub(crate) async fn journal(
    orch: &Orchestrator,
    origin: &RowWriteOrigin,
    report: &Value,
    gated: &[GatedChange],
) {
    let entries: Vec<crate::authorization::InstrumentedWrite> = report
        .get("applied")
        .and_then(Value::as_array)
        .map(|applied| {
            applied
                .iter()
                .filter_map(|change| {
                    let index = usize::try_from(change.get("index")?.as_u64()?).ok()?;
                    let gated = gated.get(index)?;
                    let summary = change
                        .pointer("/data/uri")
                        .or_else(|| change.get("target"))
                        .and_then(Value::as_str)?
                        .to_string();
                    Some(crate::authorization::InstrumentedWrite {
                        mutation: gated.mutation,
                        collection: gated.collection.clone(),
                        summary,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    if entries.is_empty() {
        return;
    }
    let authority = crate::authorization::row_write_actor(orch, origin);
    crate::authorization::journal_instrumented_write(&authority, &origin.appearance, &entries)
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_target_is_a_thread_table_or_row_with_at_most_a_where() {
        let table = row_target("cairn://p/prj/roadmap/db/attempts", "t")
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                table.thread.as_str(),
                table.table.as_str(),
                table.row,
                table.scoped
            ),
            ("roadmap", "attempts", false, false)
        );
        assert_eq!(table.mutation("append"), Some(AuthorityMutation::Create));
        assert_eq!(
            table.mutation("patch"),
            None,
            "an unscoped table patch is SQL"
        );
        assert_eq!(table.collection(), "cairn://p/prj/roadmap/db/attempts");

        let row = row_target("cairn://p/prj/roadmap/db/attempts/7", "t")
            .unwrap()
            .unwrap();
        assert!(row.row);
        assert_eq!(row.mutation("patch"), Some(AuthorityMutation::Update));
        assert_eq!(row.mutation("delete"), Some(AuthorityMutation::Delete));
        assert_eq!(row.mutation("append"), None);

        let scoped = row_target("cairn://p/prj/roadmap/db/attempts?where=result=null", "t")
            .unwrap()
            .unwrap();
        assert!(scoped.scoped);
        assert_eq!(scoped.mutation("delete"), Some(AuthorityMutation::Delete));

        for other in [
            "cairn://p/prj/roadmap/db",
            "cairn://db/issues",
            "cairn://db/issues/1",
            "cairn://p/prj/tenets/1",
        ] {
            assert_eq!(row_target(other, other).unwrap(), None, "{other}");
        }
        assert!(row_target("cairn://p/prj/roadmap/db/attempts?limit=1", "t")
            .unwrap_err()
            .contains("only query is where="));
        assert!(row_target("cairn://p/prj/roadmap/db/attempts#x", "t")
            .unwrap_err()
            .contains("no fragment"));
    }

    #[test]
    fn an_input_carries_changes_and_atomic_only() {
        let (changes, atomic) = split_input(
            &serde_json::json!({"changes": [{}], "atomic": true}),
            "write_db_rows",
            "the database browser",
        )
        .unwrap();
        assert_eq!((changes.len(), atomic), (1, Some(true)));
        let refused = split_input(
            &serde_json::json!({"changes": [{}], "instrument": "page"}),
            "write_db_rows",
            "the database browser",
        )
        .unwrap_err();
        assert!(
            matches!(&refused, InputError::Refused(message) if message.contains("`instrument` is refused")),
            "{refused:?}"
        );
        assert!(matches!(
            split_input(&serde_json::json!({"changes": []}), "write_db_rows", "x"),
            Err(InputError::BadRequest(_))
        ));
    }
}
