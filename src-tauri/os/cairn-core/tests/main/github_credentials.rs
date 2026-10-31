use crate::common;

use cairn_core::github::credentials;
use cairn_db::turso::params;

/// Read a single raw text column from the `default` github_app row.
async fn raw_column(
    db: &cairn_core::internal::storage::LocalDb,
    column: &'static str,
) -> Option<String> {
    use cairn_core::internal::storage::RowExt;
    let sql = format!("SELECT {column} FROM github_app WHERE id = 'default'");
    db.read(move |conn| {
        let sql = sql.clone();
        Box::pin(async move {
            let mut rows = conn.query(&sql, ()).await?;
            match rows.next().await? {
                Some(row) => row.opt_text(0),
                None => Ok(None),
            }
        })
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn github_credentials_default_empty_when_unconfigured() {
    let (_temp, db) = common::migrated_db().await;

    let creds = credentials::get_github_credentials(&db).await.unwrap();
    assert!(creds.app_id.is_none());
    assert!(creds.private_key.is_none());
    assert!(creds.installation_id.is_none());
}

#[tokio::test]
async fn github_credentials_use_owner_installation_before_default() {
    let (_temp, db) = common::migrated_db().await;

    db.execute_script(
        "
        INSERT INTO github_app(
            id, app_id, app_name, app_slug, private_key, webhook_secret, installation_id
         )
         VALUES ('default', 42, 'Cairn', 'cairn', 'PRIVATE', 'secret', 100);
        INSERT INTO github_installations(
            id, account_login, account_type, installation_id, created_at, updated_at
         )
         VALUES ('inst-1', 'bleugreenlab', 'Organization', 200, 1, 1);
        ",
    )
    .await
    .unwrap();

    let creds = credentials::get_github_credentials(&db).await.unwrap();
    assert_eq!(creds.app_id, Some(42));
    assert_eq!(creds.app_name.as_deref(), Some("Cairn"));

    assert_eq!(
        credentials::get_installation_for_owner(&db, "bleugreenlab")
            .await
            .unwrap(),
        Some(200)
    );
    assert_eq!(
        credentials::installation_identity(&db, "bleugreenlab")
            .await
            .unwrap(),
        (42, 200)
    );
    assert_eq!(
        credentials::installation_identity(&db, "fallback-owner")
            .await
            .unwrap(),
        (42, 100)
    );
}

#[tokio::test]
async fn github_credentials_report_missing_required_fields() {
    let (_temp, db) = common::migrated_db().await;

    db.write(|conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO github_app(id, app_id, private_key, installation_id)
                 VALUES ('default', ?1, ?2, ?3)",
                params![Option::<i64>::None, Some("PRIVATE"), Some(1_i64)],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    let err = credentials::installation_identity(&db, "owner")
        .await
        .unwrap_err();
    assert!(err.contains("GitHub App ID not configured"));
}

#[tokio::test]
async fn at_rest_fields_roundtrip_through_store() {
    let (_temp, db) = common::migrated_db().await;

    let pem = "-----BEGIN RSA PRIVATE KEY-----\nMIIabc\n-----END RSA PRIVATE KEY-----";
    credentials::update_github_credentials(&db, |creds| {
        creds.app_id = Some(42);
        creds.private_key = Some(pem.to_string());
        creds.webhook_secret = Some("whsec_top_secret".to_string());
    })
    .await
    .unwrap();

    // The stored columns must be ciphertext, not the plaintext we wrote.
    let stored_pk = raw_column(&db, "private_key").await.unwrap();
    assert_ne!(stored_pk, pem);
    assert!(!stored_pk.contains("BEGIN RSA"));
    let stored_ws = raw_column(&db, "webhook_secret").await.unwrap();
    assert_ne!(stored_ws, "whsec_top_secret");

    // Reading back returns the decrypted plaintext.
    let creds = credentials::get_github_credentials(&db).await.unwrap();
    assert_eq!(creds.private_key.as_deref(), Some(pem));
    assert_eq!(creds.webhook_secret.as_deref(), Some("whsec_top_secret"));
}

#[tokio::test]
async fn legacy_plaintext_values_are_read_then_reencrypted() {
    let (_temp, db) = common::migrated_db().await;

    // Simulate an existing install: a plaintext PEM and relay secret written
    // directly to the rows, the way older builds stored them.
    let pem = "-----BEGIN PRIVATE KEY-----\nlegacy\n-----END PRIVATE KEY-----";
    db.write(|conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO github_app(id, app_id, private_key) VALUES ('default', 7, ?1)",
                params![pem],
            )
            .await?;
            conn.execute(
                "INSERT INTO relay_channel(id, channel_id, secret) VALUES ('default', 'ch', ?1)",
                params!["legacy-relay-secret"],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    // Plaintext is read transparently.
    let creds = credentials::get_github_credentials(&db).await.unwrap();
    assert_eq!(creds.private_key.as_deref(), Some(pem));
    let channel = cairn_core::ingress::channel::get_relay_channel(&db)
        .await
        .unwrap();
    assert_eq!(channel.secret.as_deref(), Some("legacy-relay-secret"));

    // A write (even an unrelated field) migrates the plaintext to ciphertext.
    credentials::update_github_credentials(&db, |creds| {
        creds.app_name = Some("Cairn".to_string());
    })
    .await
    .unwrap();
    cairn_core::ingress::channel::update_relay_channel(&db, |channel| {
        channel.health_state = Some("healthy".to_string());
    })
    .await
    .unwrap();

    let stored_pk = raw_column(&db, "private_key").await.unwrap();
    assert_ne!(stored_pk, pem);
    assert!(!stored_pk.contains("BEGIN PRIVATE KEY"));
    let stored_rs = db
        .query_opt_text("SELECT secret FROM relay_channel WHERE id = 'default'", ())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(stored_rs, "legacy-relay-secret");

    // And it still decrypts back to the original values.
    let creds = credentials::get_github_credentials(&db).await.unwrap();
    assert_eq!(creds.private_key.as_deref(), Some(pem));
    let channel = cairn_core::ingress::channel::get_relay_channel(&db)
        .await
        .unwrap();
    assert_eq!(channel.secret.as_deref(), Some("legacy-relay-secret"));
}
