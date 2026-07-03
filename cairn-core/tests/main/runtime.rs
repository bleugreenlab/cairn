use std::sync::Arc;
use std::time::Duration;

use cairn_core::internal::storage::{DbError, RowExt, SearchIndex};

use crate::common::{migrated_db, query_i64};

#[tokio::test]
async fn mvcc_allows_read_and_write_overlap() {
    let (_temp, db) = migrated_db().await;
    db.execute(
        "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
         VALUES ('project-1', 'default', 'Project', 'PROJ', '/tmp/project', 1, 1)",
        (),
    )
    .await
    .unwrap();

    let db = Arc::new(db);
    let reader_db = Arc::clone(&db);
    let reader = tokio::spawn(async move {
        reader_db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query("SELECT COUNT(*) FROM projects", ()).await?;
                    let row = rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::Row("missing project count".to_string()))?;
                    let count = row.i64(0)?;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(count)
                })
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    db.execute(
        "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
         VALUES ('issue-1', 'project-1', 1, 'Concurrent write', 'while reader is open', 2, 2)",
        (),
    )
    .await
    .unwrap();

    assert_eq!(reader.await.unwrap().unwrap(), 1);
    assert_eq!(
        query_i64(&db, "SELECT COUNT(*) FROM issues WHERE id = 'issue-1'")
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn search_outbox_feeds_tantivy_index() {
    let (temp, db) = migrated_db().await;
    db.execute_script(
        "
        INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
         VALUES ('project-1', 'default', 'Project', 'PROJ', '/tmp/project', 1, 1);
        INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
         VALUES ('issue-1', 'project-1', 1, 'Needle title', 'needle body', 2, 2);
        INSERT INTO comments(id, issue_id, content, source, created_at)
         VALUES ('comment-1', 'issue-1', 'needle comment', 'user', 3);
        INSERT INTO messages(id, channel_type, channel_id, sender_name, content, created_at)
         VALUES ('message-1', 'issue', 'issue-1', 'tester', 'needle message', 4);
        ",
    )
    .await
    .unwrap();

    let index = SearchIndex::open_or_create(temp.path().join("search")).unwrap();
    assert_eq!(index.apply_pending(&db).await.unwrap(), 3);

    let hits = index.search("needle", None).unwrap();
    let hit_ids = hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>();
    assert!(hit_ids.contains(&"issue-1"));
    assert!(hit_ids.contains(&"comment-1"));
    assert!(hit_ids.contains(&"message-1"));
    assert_eq!(
        query_i64(
            &db,
            "SELECT COUNT(*) FROM search_outbox WHERE status = 'pending'"
        )
        .await
        .unwrap(),
        0
    );
}
