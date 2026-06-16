//! Account table operations.

use turso::params;

use crate::storage::{LocalDb, RowExt};

use super::connection::{AccountConnection, DbAccount};

/// Get the current account connection, if any.
pub async fn get(db: &LocalDb) -> Result<Option<AccountConnection>, String> {
    db.query_opt(
        "SELECT user_id, email, name, device_id, plan, jwt_encrypted,
                jwt_expires_at, org_memberships, connected_at, updated_at
         FROM account
         LIMIT 1",
        (),
        |row| DbAccount::from_row(row).map(AccountConnection::from),
    )
    .await
    .map_err(|e| format!("Failed to get account: {e}"))
}

/// Upsert the account connection.
pub async fn upsert(db: &LocalDb, acct: &DbAccount) -> Result<(), String> {
    let acct = acct.clone();
    db.write(|conn| {
        let acct = acct.clone();
        Box::pin(async move {
            conn.execute("DELETE FROM account", ()).await?;
            conn.execute(
                "INSERT INTO account (
                    user_id, email, name, device_id, plan, jwt_encrypted,
                    jwt_expires_at, org_memberships, connected_at, updated_at
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    acct.user_id.as_str(),
                    acct.email.as_str(),
                    acct.name.as_str(),
                    acct.device_id.as_str(),
                    acct.plan.as_str(),
                    acct.jwt_encrypted.as_deref(),
                    acct.jwt_expires_at.map(i64::from),
                    acct.org_memberships.as_deref(),
                    i64::from(acct.connected_at),
                    i64::from(acct.updated_at)
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to upsert account: {e}"))
}

/// Delete the account connection.
pub async fn delete(db: &LocalDb) -> Result<(), String> {
    db.execute("DELETE FROM account", ())
        .await
        .map(|_| ())
        .map_err(|e| format!("Failed to delete account: {e}"))
}

/// Update the JWT for the account.
pub async fn update_jwt(db: &LocalDb, encrypted_jwt: &str, expires_at: i64) -> Result<(), String> {
    let encrypted_jwt = encrypted_jwt.to_string();
    db.execute(
        "UPDATE account SET jwt_encrypted = ?1, jwt_expires_at = ?2",
        params![encrypted_jwt.as_str(), expires_at],
    )
    .await
    .map(|_| ())
    .map_err(|e| format!("Failed to update account JWT: {e}"))
}

/// Get the encrypted JWT and expiry, if present.
pub async fn get_jwt_data(db: &LocalDb) -> Result<Option<(String, Option<i64>)>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT jwt_encrypted, jwt_expires_at
                     FROM account
                     LIMIT 1",
                    (),
                )
                .await?;

            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let Some(jwt) = row.opt_text(0)? else {
                return Ok(None);
            };
            Ok(Some((jwt, row.opt_i64(1)?)))
        })
    })
    .await
    .map_err(|e| format!("Failed to get account JWT data: {e}"))
}

/// Update the account plan.
pub async fn update_plan(db: &LocalDb, plan: &str) -> Result<(), String> {
    let plan = plan.to_string();
    db.execute("UPDATE account SET plan = ?1", (plan.as_str(),))
        .await
        .map(|_| ())
        .map_err(|e| format!("Failed to update account plan: {e}"))
}
