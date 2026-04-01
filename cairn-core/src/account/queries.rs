//! Database CRUD operations for the `account` table.

use diesel::prelude::*;

use crate::schema::account;

use super::connection::{AccountConnection, DbAccount};

/// Get the current account connection, if any.
pub fn get(conn: &mut SqliteConnection) -> Result<Option<AccountConnection>, String> {
    account::table
        .first::<DbAccount>(conn)
        .optional()
        .map(|opt| opt.map(AccountConnection::from))
        .map_err(|e| format!("Failed to get account: {}", e))
}

/// Upsert the account connection.
pub fn upsert(conn: &mut SqliteConnection, acct: &DbAccount) -> Result<(), String> {
    diesel::replace_into(account::table)
        .values(acct)
        .execute(conn)
        .map(|_| ())
        .map_err(|e| format!("Failed to upsert account: {}", e))
}

/// Delete the account connection.
pub fn delete(conn: &mut SqliteConnection) -> Result<(), String> {
    diesel::delete(account::table)
        .execute(conn)
        .map(|_| ())
        .map_err(|e| format!("Failed to delete account: {}", e))
}

/// Update the JWT for the account.
pub fn update_jwt(
    conn: &mut SqliteConnection,
    encrypted_jwt: &str,
    expires_at: i64,
) -> Result<(), String> {
    // Account table has at most one row; update all rows
    diesel::update(account::table)
        .set((
            account::jwt_encrypted.eq(encrypted_jwt),
            account::jwt_expires_at.eq(expires_at as i32),
        ))
        .execute(conn)
        .map(|_| ())
        .map_err(|e| format!("Failed to update account JWT: {}", e))
}

/// Get the encrypted JWT and expiry, if present.
pub fn get_jwt_data(conn: &mut SqliteConnection) -> Result<Option<(String, Option<i64>)>, String> {
    let row = account::table
        .select((account::jwt_encrypted, account::jwt_expires_at))
        .first::<(Option<String>, Option<i32>)>(conn)
        .optional()
        .map_err(|e| format!("Failed to get account JWT data: {}", e))?;

    Ok(row.and_then(|(enc, exp)| enc.map(|jwt| (jwt, exp.map(|v| v as i64)))))
}

/// Update the account plan.
pub fn update_plan(conn: &mut SqliteConnection, plan: &str) -> Result<(), String> {
    diesel::update(account::table)
        .set(account::plan.eq(plan))
        .execute(conn)
        .map(|_| ())
        .map_err(|e| format!("Failed to update account plan: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::connection::DbAccount;
    use crate::test_utils::test_diesel_conn;

    fn test_account() -> DbAccount {
        DbAccount {
            user_id: "user-123".to_string(),
            email: "test@example.com".to_string(),
            name: "Test User".to_string(),
            device_id: "device-abc".to_string(),
            plan: "free".to_string(),
            jwt_encrypted: Some("encrypted-jwt-data".to_string()),
            jwt_expires_at: Some(1999999999),
            org_memberships: None,
            connected_at: 1000000,
            updated_at: 1000000,
        }
    }

    #[test]
    fn get_returns_none_when_empty() {
        let mut conn = test_diesel_conn();
        let result = get(&mut conn).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn upsert_then_get() {
        let mut conn = test_diesel_conn();
        let acct = test_account();

        upsert(&mut conn, &acct).unwrap();

        let result = get(&mut conn).unwrap().expect("should have account");
        assert_eq!(result.user_id, "user-123");
        assert_eq!(result.email, "test@example.com");
        assert_eq!(result.name, "Test User");
        assert_eq!(result.device_id, "device-abc");
        assert_eq!(result.plan, "free");
    }

    #[test]
    fn upsert_replaces_existing() {
        let mut conn = test_diesel_conn();
        let mut acct = test_account();
        upsert(&mut conn, &acct).unwrap();

        acct.email = "updated@example.com".to_string();
        acct.plan = "pro".to_string();
        upsert(&mut conn, &acct).unwrap();

        let result = get(&mut conn).unwrap().expect("should have account");
        assert_eq!(result.email, "updated@example.com");
        assert_eq!(result.plan, "pro");
    }

    #[test]
    fn delete_removes_account() {
        let mut conn = test_diesel_conn();
        upsert(&mut conn, &test_account()).unwrap();

        delete(&mut conn).unwrap();

        let result = get(&mut conn).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn delete_on_empty_is_ok() {
        let mut conn = test_diesel_conn();
        let result = delete(&mut conn);
        assert!(result.is_ok());
    }

    #[test]
    fn update_jwt_changes_token_and_expiry() {
        let mut conn = test_diesel_conn();
        upsert(&mut conn, &test_account()).unwrap();

        update_jwt(&mut conn, "new-encrypted-jwt", 1234567890).unwrap();

        let (jwt, exp) = get_jwt_data(&mut conn).unwrap().expect("should have jwt");
        assert_eq!(jwt, "new-encrypted-jwt");
        assert_eq!(exp, Some(1234567890));
    }

    #[test]
    fn get_jwt_data_returns_none_when_empty() {
        let mut conn = test_diesel_conn();
        let result = get_jwt_data(&mut conn).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn get_jwt_data_returns_none_when_jwt_is_null() {
        let mut conn = test_diesel_conn();
        let mut acct = test_account();
        acct.jwt_encrypted = None;
        upsert(&mut conn, &acct).unwrap();

        let result = get_jwt_data(&mut conn).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn update_plan_changes_plan() {
        let mut conn = test_diesel_conn();
        upsert(&mut conn, &test_account()).unwrap();

        update_plan(&mut conn, "pro").unwrap();

        let result = get(&mut conn).unwrap().expect("should have account");
        assert_eq!(result.plan, "pro");
    }

    #[test]
    fn update_plan_on_empty_is_ok() {
        let mut conn = test_diesel_conn();
        // No account — update is a no-op, not an error
        let result = update_plan(&mut conn, "pro");
        assert!(result.is_ok());
    }
}
