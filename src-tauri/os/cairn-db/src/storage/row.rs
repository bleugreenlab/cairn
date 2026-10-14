use turso::{params::IntoParams, Connection, Row, Rows, Value};

use super::{DbError, DbResult};

pub trait FromDbRow: Sized {
    fn from_row(row: &Row) -> DbResult<Self>;
}

pub trait RowExt {
    fn text(&self, idx: usize) -> DbResult<String>;
    fn opt_text(&self, idx: usize) -> DbResult<Option<String>>;
    fn i64(&self, idx: usize) -> DbResult<i64>;
    fn opt_i64(&self, idx: usize) -> DbResult<Option<i64>>;
    fn f64(&self, idx: usize) -> DbResult<f64>;
    fn opt_f64(&self, idx: usize) -> DbResult<Option<f64>>;
    fn blob(&self, idx: usize) -> DbResult<Vec<u8>>;
    fn opt_blob(&self, idx: usize) -> DbResult<Option<Vec<u8>>>;
}

impl RowExt for Row {
    fn text(&self, idx: usize) -> DbResult<String> {
        match self.get_value(idx)? {
            Value::Text(value) => Ok(value),
            Value::Null => Err(DbError::Row(format!("column {idx} is NULL, expected TEXT"))),
            other => Err(DbError::Row(format!(
                "column {idx} has value {other:?}, expected TEXT"
            ))),
        }
    }

    fn opt_text(&self, idx: usize) -> DbResult<Option<String>> {
        match self.get_value(idx)? {
            Value::Text(value) => Ok(Some(value)),
            Value::Null => Ok(None),
            other => Err(DbError::Row(format!(
                "column {idx} has value {other:?}, expected TEXT or NULL"
            ))),
        }
    }

    fn i64(&self, idx: usize) -> DbResult<i64> {
        match self.get_value(idx)? {
            Value::Integer(value) => Ok(value),
            Value::Null => Err(DbError::Row(format!(
                "column {idx} is NULL, expected INTEGER"
            ))),
            other => Err(DbError::Row(format!(
                "column {idx} has value {other:?}, expected INTEGER"
            ))),
        }
    }

    fn opt_i64(&self, idx: usize) -> DbResult<Option<i64>> {
        match self.get_value(idx)? {
            Value::Integer(value) => Ok(Some(value)),
            Value::Null => Ok(None),
            other => Err(DbError::Row(format!(
                "column {idx} has value {other:?}, expected INTEGER or NULL"
            ))),
        }
    }

    fn f64(&self, idx: usize) -> DbResult<f64> {
        match self.get_value(idx)? {
            Value::Real(value) => Ok(value),
            Value::Integer(value) => Ok(value as f64),
            Value::Null => Err(DbError::Row(format!("column {idx} is NULL, expected REAL"))),
            other => Err(DbError::Row(format!(
                "column {idx} has value {other:?}, expected REAL"
            ))),
        }
    }

    fn opt_f64(&self, idx: usize) -> DbResult<Option<f64>> {
        match self.get_value(idx)? {
            Value::Real(value) => Ok(Some(value)),
            Value::Integer(value) => Ok(Some(value as f64)),
            Value::Null => Ok(None),
            other => Err(DbError::Row(format!(
                "column {idx} has value {other:?}, expected REAL or NULL"
            ))),
        }
    }

    fn blob(&self, idx: usize) -> DbResult<Vec<u8>> {
        match self.get_value(idx)? {
            Value::Blob(value) => Ok(value),
            Value::Null => Err(DbError::Row(format!("column {idx} is NULL, expected BLOB"))),
            other => Err(DbError::Row(format!(
                "column {idx} has value {other:?}, expected BLOB"
            ))),
        }
    }

    fn opt_blob(&self, idx: usize) -> DbResult<Option<Vec<u8>>> {
        match self.get_value(idx)? {
            Value::Blob(value) => Ok(Some(value)),
            Value::Null => Ok(None),
            other => Err(DbError::Row(format!(
                "column {idx} has value {other:?}, expected BLOB or NULL"
            ))),
        }
    }
}

pub async fn next_text(rows: &mut Rows, idx: usize) -> DbResult<Option<String>> {
    rows.next().await?.map(|row| row.text(idx)).transpose()
}

pub async fn next_opt_text(rows: &mut Rows, idx: usize) -> DbResult<Option<String>> {
    rows.next()
        .await?
        .map(|row| row.opt_text(idx))
        .transpose()
        .map(Option::flatten)
}

pub async fn next_i64(rows: &mut Rows, idx: usize) -> DbResult<Option<i64>> {
    rows.next().await?.map(|row| row.i64(idx)).transpose()
}

pub async fn query_text_conn<P>(conn: &Connection, sql: &str, params: P) -> DbResult<Option<String>>
where
    P: IntoParams,
{
    let mut rows = conn.query(sql, params).await?;
    next_text(&mut rows, 0).await
}

pub async fn query_opt_text_conn<P>(
    conn: &Connection,
    sql: &str,
    params: P,
) -> DbResult<Option<String>>
where
    P: IntoParams,
{
    let mut rows = conn.query(sql, params).await?;
    next_opt_text(&mut rows, 0).await
}

pub async fn query_opt_i64_conn<P>(conn: &Connection, sql: &str, params: P) -> DbResult<Option<i64>>
where
    P: IntoParams,
{
    let mut rows = conn.query(sql, params).await?;
    rows.next()
        .await?
        .map(|row| row.opt_i64(0))
        .transpose()
        .map(Option::flatten)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::storage::{DbError, LocalDb};

    use super::*;

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let path = temp.keep().join("cairn-row-ext-test.db");
        LocalDb::open(path).await.unwrap()
    }

    #[tokio::test]
    async fn f64_reads_real_and_sqlite_integer_numbers() {
        let db = test_db().await;

        let real = db
            .query_one("SELECT 1.25", (), |row| row.f64(0))
            .await
            .unwrap();
        assert_eq!(real, 1.25);

        let integer = db
            .query_one("SELECT 2", (), |row| row.f64(0))
            .await
            .unwrap();
        assert_eq!(integer, 2.0);
    }

    #[tokio::test]
    async fn f64_rejects_null_and_wrong_types() {
        let db = test_db().await;

        let null = db
            .query_one("SELECT NULL", (), |row| row.f64(0))
            .await
            .unwrap_err();
        assert!(
            matches!(null, DbError::Row(message) if message == "column 0 is NULL, expected REAL")
        );

        let text = db
            .query_one("SELECT 'nope'", (), |row| row.f64(0))
            .await
            .unwrap_err();
        assert!(
            matches!(text, DbError::Row(message) if message == "column 0 has value Text(\"nope\"), expected REAL")
        );
    }

    #[tokio::test]
    async fn opt_f64_reads_real_integer_and_null() {
        let db = test_db().await;

        let values = db
            .query_one("SELECT 1.5, 2, NULL", (), |row| {
                Ok((row.opt_f64(0)?, row.opt_f64(1)?, row.opt_f64(2)?))
            })
            .await
            .unwrap();
        assert_eq!(values, (Some(1.5), Some(2.0), None));
    }

    #[tokio::test]
    async fn blob_reads_bytes() {
        let db = test_db().await;

        let bytes = db
            .query_one("SELECT x'0102ff'", (), |row| row.blob(0))
            .await
            .unwrap();
        assert_eq!(bytes, vec![1, 2, 255]);
    }

    #[tokio::test]
    async fn blob_rejects_null_and_wrong_types() {
        let db = test_db().await;

        let null = db
            .query_one("SELECT NULL", (), |row| row.blob(0))
            .await
            .unwrap_err();
        assert!(
            matches!(null, DbError::Row(message) if message == "column 0 is NULL, expected BLOB")
        );

        let text = db
            .query_one("SELECT 'nope'", (), |row| row.blob(0))
            .await
            .unwrap_err();
        assert!(
            matches!(text, DbError::Row(message) if message == "column 0 has value Text(\"nope\"), expected BLOB")
        );
    }

    #[tokio::test]
    async fn opt_blob_reads_bytes_and_null() {
        let db = test_db().await;

        let values = db
            .query_one("SELECT x'0102', NULL", (), |row| {
                Ok((row.opt_blob(0)?, row.opt_blob(1)?))
            })
            .await
            .unwrap();
        assert_eq!(values, (Some(vec![1, 2]), None));
    }

    #[tokio::test]
    async fn next_scalar_helpers_map_first_row() {
        let db = test_db().await;
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query("SELECT 'x', NULL, 42", ()).await?;
                assert_eq!(next_text(&mut rows, 0).await?, Some("x".to_string()));

                let mut rows = conn.query("SELECT 'x', NULL, 42", ()).await?;
                assert_eq!(next_opt_text(&mut rows, 1).await?, None);

                let mut rows = conn.query("SELECT 'x', NULL, 42", ()).await?;
                assert_eq!(next_i64(&mut rows, 2).await?, Some(42));
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn connection_scalar_helpers_query_first_column() {
        let db = test_db().await;
        db.read(|conn| {
            Box::pin(async move {
                assert_eq!(
                    query_text_conn(conn, "SELECT 'text'", ()).await?,
                    Some("text".to_string())
                );
                assert_eq!(query_opt_text_conn(conn, "SELECT NULL", ()).await?, None);
                assert_eq!(query_opt_i64_conn(conn, "SELECT 99", ()).await?, Some(99));
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
