//! Comment operations.

use crate::error::CairnError;
use crate::models::{Comment, CommentSource, CreateComment};
use crate::services::Clock;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use turso::params;
use uuid::Uuid;

const COMMENT_COLUMNS: &str = "id, issue_id, content, source, created_at, seq";

fn comment_from_row(row: &turso::Row) -> DbResult<Comment> {
    Ok(Comment {
        id: row.text(0)?,
        issue_id: row.text(1)?,
        content: row.text(2)?,
        source: row.text(3)?.parse().unwrap_or(CommentSource::User),
        created_at: row.i64(4)?,
        seq: row.i64(5)?,
    })
}

/// Next 1-based per-issue comment sequence. Computed inside the caller's write
/// transaction so the local single-writer DB assigns gap-free, stable seqs.
pub(crate) async fn next_issue_comment_seq(
    conn: &turso::Connection,
    issue_id: &str,
) -> DbResult<i64> {
    let mut rows = conn
        .query(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM comments WHERE issue_id = ?1",
            params![issue_id],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row("comment seq query returned no row".to_string()))?;
    row.i64(0)
}

/// Resolve a per-issue comment `seq` to its stable comment id, if present.
pub async fn id_for_issue_seq(
    db: &LocalDb,
    issue_id: &str,
    seq: i64,
) -> Result<Option<String>, CairnError> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM comments WHERE issue_id = ?1 AND seq = ?2 LIMIT 1",
                    params![issue_id.as_str(), seq],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(row.text(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(CairnError::from)
}

async fn load_conn(conn: &turso::Connection, id: &str) -> DbResult<Comment> {
    let sql = format!("SELECT {COMMENT_COLUMNS} FROM comments WHERE id = ?1");
    let mut rows = conn.query(&sql, params![id]).await?;
    rows.next()
        .await?
        .map(|row| comment_from_row(&row))
        .transpose()?
        .ok_or_else(|| DbError::Row(format!("comment not found: {id}")))
}

pub async fn list(db: &LocalDb, issue_id: &str) -> Result<Vec<Comment>, CairnError> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let sql = format!(
                "SELECT {COMMENT_COLUMNS}
                 FROM comments
                 WHERE issue_id = ?1
                 ORDER BY created_at ASC"
            );
            let mut rows = conn.query(&sql, params![issue_id.as_str()]).await?;
            let mut comments = Vec::new();
            while let Some(row) = rows.next().await? {
                comments.push(comment_from_row(&row)?);
            }
            Ok(comments)
        })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn create(
    db: &LocalDb,
    clock: &dyn Clock,
    input: CreateComment,
) -> Result<Comment, CairnError> {
    let CreateComment {
        issue_id,
        content,
        source,
    } = input;
    let id = Uuid::new_v4().to_string();
    let created_at = clock.now();
    let source_text = source.to_string();

    db.write(|conn| {
        let id = id.clone();
        let issue_id = issue_id.clone();
        let content = content.clone();
        let source = source.clone();
        let source_text = source_text.clone();
        Box::pin(async move {
            let seq = next_issue_comment_seq(conn, &issue_id).await?;
            conn.execute(
                "INSERT INTO comments (id, issue_id, content, source, created_at, seq)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    id.as_str(),
                    issue_id.as_str(),
                    content.as_str(),
                    source_text.as_str(),
                    created_at,
                    seq
                ],
            )
            .await?;

            Ok(Comment {
                id,
                issue_id,
                content,
                source,
                created_at,
                seq,
            })
        })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn update(db: &LocalDb, id: &str, content: &str) -> Result<Comment, CairnError> {
    let id = id.to_string();
    let content = content.to_string();
    db.write(|conn| {
        let id = id.clone();
        let content = content.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE comments SET content = ?1 WHERE id = ?2",
                params![content.as_str(), id.as_str()],
            )
            .await?;
            load_conn(conn, &id).await
        })
    })
    .await
    .map_err(|error| match error {
        DbError::Row(message) if message.starts_with("comment not found: ") => {
            CairnError::NotFound {
                entity: "comment",
                id,
            }
        }
        error => CairnError::from(error),
    })
}

pub async fn delete(db: &LocalDb, id: &str) -> Result<(), CairnError> {
    let id = id.to_string();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute("DELETE FROM comments WHERE id = ?1", params![id.as_str()])
                .await?;
            Ok(())
        })
    })
    .await
    .map_err(CairnError::from)
}
