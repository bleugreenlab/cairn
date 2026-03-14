//! Comment operations.

use crate::diesel_models::{DbComment, NewComment};
use crate::models::{Comment, CommentSource, CreateComment};
use crate::schema::comments;
use crate::services::Clock;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use uuid::Uuid;

/// Convert DbComment to Comment model.
pub fn db_comment_to_comment(db: DbComment) -> Comment {
    Comment {
        id: db.id,
        issue_id: db.issue_id,
        content: db.content,
        source: db.source.parse().unwrap_or(CommentSource::User),
        created_at: db.created_at as i64,
    }
}

/// List all comments for an issue, oldest first.
pub fn list(conn: &mut SqliteConnection, issue_id: &str) -> Result<Vec<Comment>, String> {
    let db_comments: Vec<DbComment> = comments::table
        .filter(comments::issue_id.eq(issue_id))
        .order(comments::created_at.asc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    Ok(db_comments.into_iter().map(db_comment_to_comment).collect())
}

/// Create a new comment.
pub fn create(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    input: CreateComment,
) -> Result<Comment, String> {
    let now = clock.now() as i32;
    let id = Uuid::new_v4().to_string();

    let new_comment = NewComment {
        id: &id,
        issue_id: &input.issue_id,
        content: &input.content,
        source: &input.source.to_string(),
        created_at: now,
    };

    diesel::insert_into(comments::table)
        .values(&new_comment)
        .execute(conn)
        .map_err(|e| e.to_string())?;

    Ok(Comment {
        id,
        issue_id: input.issue_id,
        content: input.content,
        source: input.source,
        created_at: now as i64,
    })
}

/// Update a comment's content. Returns the updated comment.
pub fn update(conn: &mut SqliteConnection, id: &str, content: &str) -> Result<Comment, String> {
    let db_comment: DbComment = comments::table
        .find(id)
        .first(conn)
        .map_err(|e| format!("Comment not found: {}", e))?;

    diesel::update(comments::table.find(id))
        .set(comments::content.eq(content))
        .execute(conn)
        .map_err(|e| e.to_string())?;

    Ok(Comment {
        id: id.to_string(),
        issue_id: db_comment.issue_id,
        content: content.to_string(),
        source: db_comment.source.parse().unwrap_or(CommentSource::User),
        created_at: db_comment.created_at as i64,
    })
}

/// Delete a comment.
pub fn delete(conn: &mut SqliteConnection, id: &str) -> Result<(), String> {
    diesel::delete(comments::table.find(id))
        .execute(conn)
        .map_err(|e| e.to_string())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::NewComment;
    use crate::services::testing::MockClock;
    use crate::test_utils::{create_test_project, test_diesel_conn};

    fn create_test_issue(conn: &mut SqliteConnection, project_id: &str) -> String {
        use crate::services::RealClock;
        crate::issues::crud::create(
            conn,
            &RealClock,
            crate::models::CreateIssue {
                project_id: project_id.to_string(),
                title: "Test Issue".to_string(),
                description: None,
                model: None,
                skills: None,
            },
        )
        .unwrap()
        .id
    }

    #[test]
    fn test_create_comment_with_clock() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let issue_id = create_test_issue(&mut conn, &project_id);

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700004000);

        let comment = create(
            &mut conn,
            &mock_clock,
            CreateComment {
                issue_id: issue_id.clone(),
                content: "Test comment".to_string(),
                source: CommentSource::User,
            },
        )
        .unwrap();

        assert_eq!(comment.content, "Test comment");
        assert_eq!(comment.created_at, 1700004000);
        assert_eq!(comment.source, CommentSource::User);
    }

    #[test]
    fn test_create_comment_agent_source() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let issue_id = create_test_issue(&mut conn, &project_id);

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700005000);

        let comment = create(
            &mut conn,
            &mock_clock,
            CreateComment {
                issue_id: issue_id.clone(),
                content: "Agent comment".to_string(),
                source: CommentSource::Agent,
            },
        )
        .unwrap();

        assert_eq!(comment.source, CommentSource::Agent);

        let db_comment: DbComment = comments::table.find(&comment.id).first(&mut conn).unwrap();
        assert_eq!(db_comment.source, "agent");
    }

    #[test]
    fn test_list_comments_ordered() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let issue_id = create_test_issue(&mut conn, &project_id);

        let c1 = NewComment {
            id: "c1",
            issue_id: &issue_id,
            content: "First",
            source: "user",
            created_at: 100,
        };
        let c2 = NewComment {
            id: "c2",
            issue_id: &issue_id,
            content: "Second",
            source: "agent",
            created_at: 200,
        };
        let c3 = NewComment {
            id: "c3",
            issue_id: &issue_id,
            content: "Third",
            source: "user",
            created_at: 150,
        };

        diesel::insert_into(comments::table)
            .values(&c1)
            .execute(&mut conn)
            .unwrap();
        diesel::insert_into(comments::table)
            .values(&c2)
            .execute(&mut conn)
            .unwrap();
        diesel::insert_into(comments::table)
            .values(&c3)
            .execute(&mut conn)
            .unwrap();

        let result = list(&mut conn, &issue_id).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].content, "First");
        assert_eq!(result[1].content, "Third");
        assert_eq!(result[2].content, "Second");
    }

    #[test]
    fn test_list_comments_empty() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let issue_id = create_test_issue(&mut conn, &project_id);

        let comments = list(&mut conn, &issue_id).unwrap();
        assert!(comments.is_empty());
    }
}
