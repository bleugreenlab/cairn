use serde::{Deserialize, Serialize};

/// Represents the desktop's connection to cairn.computer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountConnection {
    pub user_id: String,
    pub email: String,
    pub name: String,
    pub device_id: String,
    pub plan: String, // "team" | "free"
    pub org_memberships: Vec<OrgMembership>,
    pub connected_at: i64,
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
    pub org_name: String,
    pub role: String,
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
