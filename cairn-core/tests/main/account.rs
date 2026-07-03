use crate::common;

use cairn_core::account::{queries, DbAccount};

fn account(user_id: &str, plan: &str) -> DbAccount {
    DbAccount {
        user_id: user_id.to_string(),
        email: format!("{user_id}@example.com"),
        name: "Test User".to_string(),
        device_id: "device-1".to_string(),
        plan: plan.to_string(),
        jwt_encrypted: None,
        jwt_expires_at: None,
        org_memberships: Some(
            serde_json::json!([
                {"orgId": "org-1", "orgName": "Acme", "role": "admin"}
            ])
            .to_string(),
        ),
        connected_at: 100,
        updated_at: 100,
    }
}

#[tokio::test]
async fn account_upsert_replaces_current_connection() {
    let (_temp, db) = common::migrated_db().await;

    queries::upsert(&db, &account("user-1", "free"))
        .await
        .unwrap();
    queries::upsert(&db, &account("user-2", "pro"))
        .await
        .unwrap();

    let loaded = queries::get(&db).await.unwrap().unwrap();
    assert_eq!(loaded.user_id, "user-2");
    assert_eq!(loaded.email, "user-2@example.com");
    assert_eq!(loaded.plan, "pro");
    assert_eq!(loaded.org_memberships.len(), 1);
    assert_eq!(loaded.org_memberships[0].org_id, "org-1");
}

#[tokio::test]
async fn account_jwt_plan_and_delete_roundtrip() {
    let (_temp, db) = common::migrated_db().await;

    queries::upsert(&db, &account("user-1", "free"))
        .await
        .unwrap();
    queries::update_jwt(&db, "encrypted-token", 1234)
        .await
        .unwrap();
    queries::update_plan(&db, "remote").await.unwrap();

    assert_eq!(
        queries::get_jwt_data(&db).await.unwrap(),
        Some(("encrypted-token".to_string(), Some(1234)))
    );
    assert_eq!(
        queries::get(&db).await.unwrap().unwrap().plan.as_str(),
        "remote"
    );

    queries::delete(&db).await.unwrap();
    assert!(queries::get(&db).await.unwrap().is_none());
    assert!(queries::get_jwt_data(&db).await.unwrap().is_none());
}
