//! The structural references a fact row's `mentions` field holds.
//!
//! One vocabulary for every source that carries `mentions`, so one predicate
//! shape (`mentions: <uri>`) matches a post, a comment, and a thread's own
//! utterance alike. A reference is structural or it is nothing: there is no
//! prose or keyword reading here.

use cairn_common::uri::CairnResource;

use crate::storage::LocalDb;

/// Every structural reference in `text`, canonical and deduplicated, in the
/// order first written.
///
/// Two shapes count. Any candidate [`cairn_common::uri::scan_uris`] hands to
/// the one URI parser and gets back a resource, so prose about "cairn://" that
/// does not parse names nothing. And a project-qualified thread name
/// (`project/thread`), which is how people name a thread in prose, included
/// only when that thread exists: `src/lib` parses as a thread URI too, and a
/// path is not a mention. A bare thread name is ordinary language and never a
/// reference.
///
/// Only that a thread exists is checked here. Which job a mention reaches is
/// decided at delivery, so the row never freezes a session that may rotate.
pub(crate) async fn structural_mentions(db: &LocalDb, text: &str) -> Vec<String> {
    let mut mentions: Vec<String> = Vec::new();
    for found in cairn_common::uri::scan_uris(text) {
        let uri = found.resource.to_uri();
        if !mentions.contains(&uri) {
            mentions.push(uri);
        }
    }
    for candidate in text.split(|character: char| {
        !character.is_ascii_alphanumeric() && !matches!(character, '-' | '/')
    }) {
        if candidate.split('/').count() != 2 {
            continue;
        }
        let Some(resource @ CairnResource::Thread { .. }) =
            cairn_common::uri::parse_uri(&format!("cairn://p/{candidate}"))
        else {
            continue;
        };
        let uri = resource.to_uri();
        if mentions.contains(&uri) || !thread_exists(db, &resource).await {
            continue;
        }
        mentions.push(uri);
    }
    mentions
}

async fn thread_exists(db: &LocalDb, thread: &CairnResource) -> bool {
    let CairnResource::Thread { project, name, .. } = thread else {
        return false;
    };
    db.query_opt_i64(
        "SELECT 1 FROM threads t JOIN projects p ON p.id = t.project_id
          WHERE p.key = ?1 AND t.name = ?2 LIMIT 1",
        (project.clone(), name.clone()),
    )
    .await
    .ok()
    .flatten()
    .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn db() -> LocalDb {
        let db = crate::storage::migrated_test_db("structural-mentions.db").await;
        db.execute_batch(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','Cairn','cairn','/tmp/cairn',1,1);
             INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('t','p','harness','active',1,1);",
        )
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn mentions_are_parsed_uris_and_existing_qualified_threads_only() {
        let db = db().await;
        let text = "See cairn://p/cairn/42 and cairn://posts/7, ask cairn/harness, \
                    not harness alone, not src/lib, not cairn://nonsense here, \
                    and cairn://p/cairn/42 again.";
        assert_eq!(
            structural_mentions(&db, text).await,
            vec![
                "cairn://p/cairn/42",
                "cairn://posts/7",
                "cairn://p/cairn/harness"
            ]
        );
        assert!(
            structural_mentions(&db, "harness and cairn:// prose")
                .await
                .is_empty(),
            "a bare thread name and unparseable cairn:// prose name nothing"
        );
    }
}
