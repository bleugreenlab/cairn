//! The short numbers that address check requests, check observations,
//! messages, operator messages, operator requests and run batches.
//!
//! Each of those rows keeps its UUID or hex storage id as its primary key and
//! protocol correlation value, and takes a `number` in its insert's own
//! statement (the `address_counters` triggers). The number is the only id a
//! canonical address renders. A legacy address naming the storage id still
//! resolves to the same row, and its reply names the mapping on one
//! `Adjusted:` line ([`legacy_adjustment`]).

use cairn_common::adjustment::Adjustment;

use super::{LocalDb, RowExt};

/// A resource kind addressed by a short number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Numbered {
    /// `cairn://p/{project}/check-requests/{n}`, numbered per project.
    CheckRequest,
    /// `cairn://p/{project}/check-observations/{n}`, numbered per project.
    CheckObservation,
    /// `cairn://p/{project}/messages/{n}`, numbered per database.
    Message,
    /// `cairn://operator/messages/{n}`, numbered per install.
    OperatorMessage,
    /// `cairn://requests/{n}`, numbered per install.
    OperatorRequest,
    /// `.../batches/{n}`, numbered per calling job (node or task).
    RunBatch,
}

impl Numbered {
    fn table(self) -> &'static str {
        match self {
            Self::CheckRequest => "check_requests",
            Self::CheckObservation => "check_result_observations",
            Self::Message => "messages",
            Self::OperatorMessage => "operator_messages",
            Self::OperatorRequest => "requests",
            Self::RunBatch => "run_batch_requests",
        }
    }

    fn id_column(self) -> &'static str {
        match self {
            Self::RunBatch => "request_id",
            _ => "id",
        }
    }

    /// The column a number is unique within, or `None` for one count per
    /// database.
    fn scope_column(self) -> Option<&'static str> {
        match self {
            Self::CheckRequest | Self::CheckObservation => Some("project_id"),
            Self::RunBatch => Some("job_id"),
            Self::Message | Self::OperatorMessage | Self::OperatorRequest => None,
        }
    }

    /// What the kind is called in a reply.
    pub fn noun(self) -> &'static str {
        match self {
            Self::CheckRequest => "check request",
            Self::CheckObservation => "check observation",
            Self::Message => "message",
            Self::OperatorMessage => "operator message",
            Self::OperatorRequest => "operator request",
            Self::RunBatch => "run batch",
        }
    }
}

/// The number an address segment spells: plain ASCII digits, no sign, no
/// leading zero, at least 1.
pub fn parse_number(segment: &str) -> Option<u64> {
    let digits = !segment.is_empty()
        && segment.len() <= 19
        && segment.bytes().all(|byte| byte.is_ascii_digit())
        && !segment.starts_with('0');
    digits.then(|| segment.parse().ok()).flatten()
}

/// One addressed row: its storage id, its number, and whether the address
/// named it by the legacy storage id rather than the number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub id: String,
    pub number: u64,
    pub legacy: bool,
}

fn scope_clause(kind: Numbered) -> String {
    kind.scope_column()
        .map(|column| format!(" AND {column} = ?2"))
        .unwrap_or_default()
}

/// The storage id of the row a number names within `scope`. Two runners on
/// one team replica can mint the same number before a pull, so the earliest
/// row wins a tie.
pub async fn id_for_number(
    db: &LocalDb,
    kind: Numbered,
    scope: Option<&str>,
    number: u64,
) -> Result<Option<String>, String> {
    let sql = format!(
        "SELECT {} FROM {} WHERE number = ?1{} ORDER BY rowid LIMIT 1",
        kind.id_column(),
        kind.table(),
        scope_clause(kind)
    );
    let number = i64::try_from(number).map_err(|error| error.to_string())?;
    let result = match (kind.scope_column(), scope) {
        (Some(_), Some(scope)) => db.query_opt_text(sql, (number, scope.to_string())).await,
        (Some(_), None) => return Ok(None),
        (None, _) => db.query_opt_text(sql, (number,)).await,
    };
    result.map_err(|error| format!("look up {} {number}: {error}", kind.noun()))
}

/// The number a stored row took, or `None` when no row has that id (in
/// `scope`, for a scoped kind).
pub async fn number_of(
    db: &LocalDb,
    kind: Numbered,
    scope: Option<&str>,
    id: &str,
) -> Result<Option<u64>, String> {
    let map = |row: &turso::Row| row.opt_i64(0);
    // A storage id is unique on its own; the scope only narrows a lookup that
    // must stay inside one project or job.
    let result = match (kind.scope_column(), scope) {
        (Some(_), Some(scope)) => {
            let sql = format!(
                "SELECT number FROM {} WHERE {} = ?1{}",
                kind.table(),
                kind.id_column(),
                scope_clause(kind)
            );
            db.query_opt(sql, (id.to_string(), scope.to_string()), map)
                .await
        }
        _ => {
            let sql = format!(
                "SELECT number FROM {} WHERE {} = ?1",
                kind.table(),
                kind.id_column()
            );
            db.query_opt(sql, (id.to_string(),), map).await
        }
    };
    result
        .map(|number| {
            number
                .flatten()
                .and_then(|number| u64::try_from(number).ok())
        })
        .map_err(|error| format!("read the number of {} {id}: {error}", kind.noun()))
}

/// Resolve an address segment: a number within `scope`, or a legacy storage
/// id. `None` when neither names a row.
pub async fn resolve(
    db: &LocalDb,
    kind: Numbered,
    scope: Option<&str>,
    segment: &str,
) -> Result<Option<Resolved>, String> {
    if let Some(number) = parse_number(segment) {
        return Ok(id_for_number(db, kind, scope, number)
            .await?
            .map(|id| Resolved {
                id,
                number,
                legacy: false,
            }));
    }
    Ok(number_of(db, kind, scope, segment)
        .await?
        .map(|number| Resolved {
            id: segment.to_string(),
            number,
            legacy: true,
        }))
}

/// The one line a reply carries when its address named a row by the legacy
/// storage id: the address as given, and the numbered address it is cited by.
pub fn legacy_adjustment(received: &str, canonical: &str) -> Adjustment {
    Adjustment::new(
        "address",
        format!("`{received}`"),
        format!("`{canonical}`"),
        "addressed by its number; the storage-id link still resolves to the same row",
    )
}

/// `body` with the legacy mapping on its first line when `resolved` came from
/// a storage id, unchanged otherwise.
pub fn with_legacy_line(
    resolved: &Resolved,
    received: &str,
    canonical: &str,
    body: String,
) -> String {
    if resolved.legacy {
        format!("{}\n\n{body}", legacy_adjustment(received, canonical))
    } else {
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_number_is_plain_positive_digits() {
        assert_eq!(parse_number("12"), Some(12));
        assert_eq!(parse_number("1"), Some(1));
        for not in [
            "",
            "0",
            "012",
            "+1",
            "-1",
            "1a",
            "2ceba732",
            "99999999999999999999",
        ] {
            assert_eq!(parse_number(not), None, "{not}");
        }
    }

    #[tokio::test]
    async fn a_segment_resolves_by_number_or_by_legacy_id_within_its_scope() {
        let db = crate::storage::migrated_test_db("address-numbers").await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','default','P','p','/tmp/p',1,1), ('q','default','Q','q','/tmp/q',1,1);
             INSERT INTO check_requests(id, project_id, suite, consumer, requester_ref, priority, state,
                                        accepted_at_ms, state_since_ms, updated_ms)
               VALUES('2ceba732-1436-45ad-aca2-7ecac587b852','p','s','requested','operator','review_check','recorded',1,1,1),
                     ('other','q','s','requested','operator','review_check','recorded',2,2,2);",
        )
        .await
        .unwrap();
        let uuid = "2ceba732-1436-45ad-aca2-7ecac587b852";
        assert_eq!(
            resolve(&db, Numbered::CheckRequest, Some("p"), "1")
                .await
                .unwrap(),
            Some(Resolved {
                id: uuid.into(),
                number: 1,
                legacy: false
            })
        );
        assert_eq!(
            resolve(&db, Numbered::CheckRequest, Some("p"), uuid)
                .await
                .unwrap(),
            Some(Resolved {
                id: uuid.into(),
                number: 1,
                legacy: true
            })
        );
        // Each project counts its own; a number or id from another is not found.
        assert_eq!(
            resolve(&db, Numbered::CheckRequest, Some("q"), "1")
                .await
                .unwrap()
                .map(|r| r.id),
            Some("other".to_string())
        );
        assert_eq!(
            resolve(&db, Numbered::CheckRequest, Some("q"), uuid)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            resolve(&db, Numbered::CheckRequest, Some("p"), "9")
                .await
                .unwrap(),
            None
        );
        // Unscoped, a storage id still names its row: what a filing reads back.
        assert_eq!(
            number_of(&db, Numbered::CheckRequest, None, uuid)
                .await
                .unwrap(),
            Some(1)
        );

        let resolved = resolve(&db, Numbered::CheckRequest, Some("p"), uuid)
            .await
            .unwrap()
            .unwrap();
        let body = with_legacy_line(
            &resolved,
            &format!("cairn://p/p/check-requests/{uuid}"),
            "cairn://p/p/check-requests/1",
            "# Check request 1".into(),
        );
        let mut lines = body.lines();
        assert_eq!(
            lines.next(),
            Some(
                "Adjusted: `address` `cairn://p/p/check-requests/2ceba732-1436-45ad-aca2-7ecac587b852` \u{2192} `cairn://p/p/check-requests/1` (addressed by its number; the storage-id link still resolves to the same row)."
            )
        );
        assert_eq!(body.matches("Adjusted:").count(), 1);
    }
}
