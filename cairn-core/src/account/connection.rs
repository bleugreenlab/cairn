//! Account connection types.

use crate::storage::{DbError, RowExt};
use serde::{Deserialize, Serialize};

/// Represents the desktop's connection to cairn.computer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountConnection {
    pub user_id: String,
    pub email: String,
    pub(crate) name: String,
    pub(crate) device_id: String,
    pub plan: String, // "team" | "free"
    pub org_memberships: Vec<OrgMembership>,
    pub(crate) connected_at: i64,
}

/// An organization the user belongs to.
///
/// Serializes as camelCase (`orgId`/`orgName`) — the shape the desktop frontend
/// and the persisted account row round-trip. Deserialization additionally
/// accepts the api's snake_case wire shape (`org_id`/`org_name`) via aliases, so
/// the `orgs` JSON returned by `POST /tokens/device` flows verbatim through the
/// `cairn://auth-callback` deep link into this DTO without an intermediate
/// transform. Without these aliases the snake_case input fails to deserialize
/// and org memberships are silently dropped, hiding the create-into-team
/// selector for users who genuinely belong to a team.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrgMembership {
    #[serde(alias = "org_id")]
    pub org_id: String,
    #[serde(alias = "org_name")]
    pub(crate) org_name: String,
    pub(crate) role: String,
}

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
    fn org_membership_deserializes_api_snake_case_wire_shape() {
        // The exact shape returned by `POST /tokens/device` (api/src/routes/tokens.ts):
        // snake_case `org_id`/`org_name`/`role`. This is the JSON that travels
        // verbatim through the `cairn://auth-callback` `orgs` query param into
        // `AccountManager::connect_with_jwt`. Regression for the connect path
        // dropping every membership because the DTO only accepted camelCase.
        let wire = r#"[{"org_id":"org-1","org_name":"Acme Team","role":"owner"},{"org_id":"org-2","org_name":"Beta","role":"member"}]"#;

        let memberships: Vec<OrgMembership> =
            serde_json::from_str(wire).expect("api snake_case orgs must deserialize");

        assert_eq!(
            memberships.len(),
            2,
            "snake_case wire shape must populate memberships"
        );
        assert_eq!(memberships[0].org_id, "org-1");
        assert_eq!(memberships[0].org_name, "Acme Team");
        assert_eq!(memberships[0].role, "owner");
        assert_eq!(memberships[1].org_id, "org-2");
        assert_eq!(memberships[1].org_name, "Beta");
        assert_eq!(memberships[1].role, "member");
    }

    #[test]
    fn org_membership_serializes_camel_case_for_frontend_and_db_roundtrip() {
        // The desktop frontend and the persisted `org_memberships` JSON expect
        // camelCase; deserialization must be tolerant but serialization must stay
        // canonical so the DB re-parse and the `account-connected` event payload
        // keep working.
        let membership = OrgMembership {
            org_id: "org-1".to_string(),
            org_name: "Acme Team".to_string(),
            role: "owner".to_string(),
        };
        let json = serde_json::to_value(&membership).unwrap();
        assert_eq!(json["orgId"], "org-1");
        assert_eq!(json["orgName"], "Acme Team");
        assert_eq!(json["role"], "owner");
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
