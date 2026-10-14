//! Account connection types.

use crate::storage::{DbError, RowExt};
pub use cairn_common::desktop_protocol::{AccountConnection, OrgMembership};

#[derive(Debug, Clone)]
pub struct DbAccount {
    pub user_id: String,
    pub email: String,
    pub name: String,
    pub device_id: String,
    pub plan: String,
    pub jwt_encrypted: Option<String>,
    pub jwt_expires_at: Option<i32>,
    pub org_memberships: Option<String>, // JSON
    pub connected_at: i32,
    pub updated_at: i32,
}

impl DbAccount {
    pub(crate) fn from_row(row: &cairn_db::turso::Row) -> Result<Self, DbError> {
        Ok(Self {
            user_id: row.text(0)?,
            email: row.text(1)?,
            name: row.text(2)?,
            device_id: row.text(3)?,
            plan: row.text(4)?,
            jwt_encrypted: row.opt_text(5)?,
            jwt_expires_at: row.opt_i64(6)?.map(|value| value as i32),
            org_memberships: row.opt_text(7)?,
            connected_at: row.i64(8)? as i32,
            updated_at: row.i64(9)? as i32,
        })
    }
}

/// Canonicalize a plan value to the current vocabulary.
///
/// `POST /tokens/device` now emits only `"team"` or `"free"` (CAIRN-2519); the
/// retired `"pro"`/`"remote"` values may still sit in a local `account` row
/// written before that change, or arrive from a malformed/legacy callback URL.
/// Fold anything that is not `"team"` down to `"free"` so a stale row or bad
/// callback renders as the Free plan instead of surfacing a dead label. Applied
/// at both the write boundary (`AccountManager::connect_with_jwt`) and the read
/// boundary (`AccountConnection::from`) so a retired literal is never stored,
/// returned, emitted, or displayed — there is a single local row and no
/// migration ceremony is warranted.
pub(crate) fn canonical_plan(plan: &str) -> String {
    match plan {
        "team" => "team".to_string(),
        _ => "free".to_string(),
    }
}

impl From<DbAccount> for AccountConnection {
    fn from(db: DbAccount) -> Self {
        let memberships: Vec<OrgMembership> = db
            .org_memberships
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        AccountConnection {
            user_id: db.user_id,
            email: db.email,
            name: db.name,
            device_id: db.device_id,
            plan: canonical_plan(&db.plan),
            org_memberships: memberships,
            connected_at: db.connected_at as i64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_db_account_basic_fields() {
        let db = DbAccount {
            user_id: "u1".to_string(),
            email: "a@b.com".to_string(),
            name: "Alice".to_string(),
            device_id: "d1".to_string(),
            plan: "team".to_string(),
            jwt_encrypted: Some("enc".to_string()),
            jwt_expires_at: Some(9999),
            org_memberships: None,
            connected_at: 12345,
            updated_at: 12345,
        };

        let conn = AccountConnection::from(db);
        assert_eq!(conn.user_id, "u1");
        assert_eq!(conn.email, "a@b.com");
        assert_eq!(conn.plan, "team");
        assert_eq!(conn.connected_at, 12345);
        assert!(conn.org_memberships.is_empty());
    }

    #[test]
    fn from_db_account_normalizes_retired_plan_values() {
        // Rows written before CAIRN-2519 may still hold the retired `pro`/`remote`
        // plan strings. Reading them for display must fold to `free` rather than
        // surface a dead label.
        for stale in ["pro", "remote", "anything-unknown"] {
            let db = DbAccount {
                user_id: "u1".to_string(),
                email: "a@b.com".to_string(),
                name: "Alice".to_string(),
                device_id: "d1".to_string(),
                plan: stale.to_string(),
                jwt_encrypted: None,
                jwt_expires_at: None,
                org_memberships: None,
                connected_at: 1,
                updated_at: 1,
            };
            assert_eq!(AccountConnection::from(db).plan, "free", "{stale} -> free");
        }
    }

    #[test]
    fn from_db_account_parses_org_memberships() {
        // OrgMembership uses #[serde(rename_all = "camelCase")]
        let orgs = serde_json::json!([
            {"orgId": "org-1", "orgName": "Acme", "role": "admin"},
            {"orgId": "org-2", "orgName": "Beta", "role": "member"},
        ]);

        let db = DbAccount {
            user_id: "u1".to_string(),
            email: "a@b.com".to_string(),
            name: "Alice".to_string(),
            device_id: "d1".to_string(),
            plan: "team".to_string(),
            jwt_encrypted: None,
            jwt_expires_at: None,
            org_memberships: Some(orgs.to_string()),
            connected_at: 1000,
            updated_at: 1000,
        };

        let conn = AccountConnection::from(db);
        assert_eq!(conn.org_memberships.len(), 2);
        assert_eq!(conn.org_memberships[0].org_id, "org-1");
        assert_eq!(conn.org_memberships[0].org_name, "Acme");
        assert_eq!(conn.org_memberships[1].role, "member");
    }

    #[test]
    fn from_db_account_invalid_json_gives_empty_memberships() {
        let db = DbAccount {
            user_id: "u1".to_string(),
            email: "a@b.com".to_string(),
            name: "Alice".to_string(),
            device_id: "d1".to_string(),
            plan: "free".to_string(),
            jwt_encrypted: None,
            jwt_expires_at: None,
            org_memberships: Some("not valid json".to_string()),
            connected_at: 1000,
            updated_at: 1000,
        };

        let conn = AccountConnection::from(db);
        assert!(conn.org_memberships.is_empty());
    }
}
