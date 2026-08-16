//! Home-bound feed cursors: the unread projection, the issuance a read records,
//! and the acknowledgement that advances a position.
//!
//! The position is server-owned end to end. A read returns an opaque token; the
//! only thing an acknowledgement may carry is that token, and the only place it
//! can move the position to is the highest post id the server recorded having
//! shown on that token. There is no shape of caller input that names a post id,
//! so a caller cannot skip unread posts by asking to.

use super::posts::{map_post, visible_to_project, POST_COLUMNS};
use super::{DbError, DbResult, LocalDb, RowExt};
use crate::models::Post;
use turso::params;

/// Default page size for a feed read, and the ceiling a caller may raise it to.
pub const FEED_PAGE_DEFAULT: usize = 20;
pub const FEED_PAGE_MAX: usize = 100;

/// Said when a token is presented against a home that has issued nothing, or
/// nothing that is still outstanding.
const NOT_OUTSTANDING: &str = "That acknowledgement token is not this home's outstanding feed issuance: it was never issued here, or a later feed read has superseded it. Nothing moved. Read the feed again to be issued a current token.";

/// Which kind of durable home a cursor belongs to.
///
/// A thread's reading position belongs to the THREAD, not to the session job
/// currently holding it: sessions are replaced, compacted, and renamed, and the
/// position must survive all three. Everything else — an execution node, a
/// sub-agent task under either kind of parent — is durable at its job, so each
/// gets a cursor of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedHomeKind {
    Thread,
    Job,
}

impl FeedHomeKind {
    /// The `feed_cursors.home_kind` discriminant. The closed set lives in this
    /// enum rather than in a column CHECK, so an unrepresentable kind cannot be
    /// written at all.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Thread => "thread",
            Self::Job => "job",
        }
    }
}

/// One durable home, resolved: the `(kind, id)` its cursor is keyed by plus the
/// project whose scoped posts it may see. Workspace posts are visible to every
/// home; a project's posts are visible only to homes inside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedHome {
    pub kind: FeedHomeKind,
    pub id: String,
    pub project_id: String,
}

/// One page of unread posts and the token that acknowledges exactly it.
#[derive(Debug, Clone)]
pub struct FeedPage {
    /// The contiguous unread prefix, oldest first. Never skips an older unread
    /// post to show a newer one.
    pub posts: Vec<Post>,
    /// The position this page was read from: every post shown has a greater id.
    pub acknowledged_through: i64,
    /// Unread posts still queued behind this page.
    pub remaining_unread: i64,
    /// The acknowledgement token. `None` exactly when nothing was shown — an
    /// empty feed issues nothing, so there is no token that could advance a
    /// position past posts no one saw.
    pub token: Option<String>,
}

/// What an acknowledgement did. Every outcome is honest about movement:
/// only `Advanced` moved the position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedAck {
    Advanced { from: i64, to: i64 },
    AlreadyAcknowledged { at: i64 },
    Rejected(String),
}

impl LocalDb {
    /// Read `home`'s contiguous unread prefix and record the issuance that
    /// acknowledges it.
    ///
    /// The page and the issuance are written in one transaction, so a token
    /// only ever exists alongside the server's record of what it showed. A
    /// crash between this read and its acknowledgement replays the identical
    /// rows under a fresh token: delivery is at-least-once by construction.
    pub async fn issue_feed_page(&self, home: &FeedHome, limit: usize) -> DbResult<FeedPage> {
        let limit = i64::try_from(limit.clamp(1, FEED_PAGE_MAX)).unwrap_or(FEED_PAGE_MAX as i64);
        let kind = home.kind.as_str();
        let id = home.id.clone();
        let project_id = home.project_id.clone();
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        self.write(move |conn| {
            let (id, project_id, nonce) = (id.clone(), project_id.clone(), nonce.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT acknowledged_post_id FROM feed_cursors
                         WHERE home_kind = ?1 AND home_id = ?2",
                        params![kind, id.as_str()],
                    )
                    .await?;
                let position = match rows.next().await? {
                    Some(row) => row.i64(0)?,
                    None => 0,
                };
                drop(rows);

                // Unread is a position question AND a jurisdiction question: the
                // second half is the corpus predicate every posts surface shares,
                // not a rule the feed keeps for itself.
                let page_sql = format!(
                    "SELECT {POST_COLUMNS} FROM posts
                     WHERE id > ?1 AND {}
                     ORDER BY id ASC LIMIT ?3",
                    visible_to_project(2)
                );
                let mut rows = conn
                    .query(&page_sql, params![position, project_id.as_str(), limit])
                    .await?;
                let mut posts = Vec::new();
                while let Some(row) = rows.next().await? {
                    posts.push(map_post(&row)?);
                }
                drop(rows);

                // Nothing shown means nothing to acknowledge. Minting a token
                // here would advertise an advancement that covers no post.
                let Some(issued_through) = posts.last().map(|post| post.id) else {
                    return Ok(FeedPage {
                        posts,
                        acknowledged_through: position,
                        remaining_unread: 0,
                        token: None,
                    });
                };

                // The backlog count must stand on the same predicate as the page
                // it reports behind, or a home is told about posts it will never
                // be shown.
                let mut rows = conn
                    .query(
                        &format!(
                            "SELECT COUNT(*) FROM posts WHERE id > ?1 AND {}",
                            visible_to_project(2)
                        ),
                        params![issued_through, project_id.as_str()],
                    )
                    .await?;
                let remaining_unread = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::internal("feed backlog count returned no row"))?
                    .i64(0)?;
                drop(rows);

                // The issuance supersedes any earlier outstanding one, which is
                // what makes an unacknowledged token from a previous read stale.
                // The position itself is untouched: reading never acknowledges.
                conn.execute(
                    "INSERT INTO feed_cursors(
                         home_kind, home_id, acknowledged_post_id,
                         last_issued_nonce, last_issued_through, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, unixepoch())
                     ON CONFLICT(home_kind, home_id) DO UPDATE SET
                         last_issued_nonce = excluded.last_issued_nonce,
                         last_issued_through = excluded.last_issued_through,
                         updated_at = excluded.updated_at",
                    params![kind, id.as_str(), position, nonce.as_str(), issued_through],
                )
                .await?;

                Ok(FeedPage {
                    posts,
                    acknowledged_through: position,
                    remaining_unread,
                    token: Some(nonce),
                })
            })
        })
        .await
    }

    /// Acknowledge `token` against `home`, advancing the position at most to
    /// what the server recorded that token having shown.
    ///
    /// Only the addressed home's row is ever read or written, so a token minted
    /// for one home can never move another's. A token that already advanced
    /// this position is honoured again as a no-op success, which is what makes
    /// retrying an acknowledgement whose reply was lost safe.
    pub async fn acknowledge_feed(&self, home: &FeedHome, token: &str) -> DbResult<FeedAck> {
        let kind = home.kind.as_str();
        let id = home.id.clone();
        let token = token.to_string();
        self.write(move |conn| {
            let (id, token) = (id.clone(), token.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT acknowledged_post_id, last_issued_nonce,
                                last_issued_through, acknowledged_nonce
                         FROM feed_cursors WHERE home_kind = ?1 AND home_id = ?2",
                        params![kind, id.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(FeedAck::Rejected(NOT_OUTSTANDING.to_string()));
                };
                let position = row.i64(0)?;
                let issued_nonce = row.opt_text(1)?;
                let issued_through = row.opt_i64(2)?;
                let acknowledged_nonce = row.opt_text(3)?;
                drop(rows);

                // Replay of the token that last advanced this position. Checked
                // before the outstanding issuance so a retry that raced a fresh
                // read still reads as the success it already was.
                if acknowledged_nonce.as_deref() == Some(token.as_str()) {
                    return Ok(FeedAck::AlreadyAcknowledged { at: position });
                }

                let (Some(outstanding), Some(through)) = (issued_nonce, issued_through) else {
                    return Ok(FeedAck::Rejected(NOT_OUTSTANDING.to_string()));
                };
                if outstanding != token {
                    return Ok(FeedAck::Rejected(NOT_OUTSTANDING.to_string()));
                }

                // Monotonic, and bounded by what this token actually showed:
                // posts that arrived after the issuance stay unread.
                let advanced = position.max(through);
                conn.execute(
                    "UPDATE feed_cursors
                     SET acknowledged_post_id = ?3, acknowledged_nonce = ?4,
                         updated_at = unixepoch()
                     WHERE home_kind = ?1 AND home_id = ?2",
                    params![kind, id.as_str(), advanced, token.as_str()],
                )
                .await?;
                Ok(FeedAck::Advanced {
                    from: position,
                    to: advanced,
                })
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::CreatePost;
    use cairn_common::identity::{
        Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, PrincipalRef,
        VerificationMethod, VerificationRecord, VerificationStatus, VerificationStrength,
    };

    const PROJECT: &str = "project-a";
    const OTHER_PROJECT: &str = "project-b";

    fn attribution() -> (PrincipalRef, AppearanceSnapshot) {
        let actor = PrincipalRef::Human {
            issuer: "https://identity.example".to_string(),
            subject: "author".to_string(),
            organization: None,
        };
        let verification = VerificationRecord::new(
            VerificationMethod::JwtOperator,
            VerificationStatus::Verified,
            Some("https://identity.example".to_string()),
            Some("author".to_string()),
            None,
            None,
            VerificationStrength::new("strong").unwrap(),
            900,
        )
        .unwrap();
        let evidence = AppearanceEvidence::new(
            AppearanceTransport::AuthenticatedOperator,
            Address::Invoke { origin: None },
            verification,
            900,
            None,
        )
        .unwrap();
        let snapshot = AppearanceSnapshot::new(actor.clone(), evidence, vec![], None).unwrap();
        (actor, snapshot)
    }

    async fn fixture(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces(id, name, created_at, updated_at)
                     VALUES ('workspace-1', 'Workspace', 1, 1)",
                    (),
                )
                .await?;
                for key in [PROJECT, OTHER_PROJECT] {
                    conn.execute(
                        "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                         VALUES (?1, 'workspace-1', ?1, ?1, '/tmp/project', 1, 1)",
                        params![key],
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
        db
    }

    /// A post at workspace scope (`None`) or scoped to one project.
    async fn post(db: &LocalDb, project_id: Option<&str>, content: &str) -> i64 {
        let (author, appearance) = attribution();
        db.create_post(CreatePost {
            project_id: project_id.map(str::to_string),
            title: None,
            content: content.to_string(),
            author,
            appearance,
        })
        .await
        .unwrap()
        .id
    }

    fn thread_home(id: &str) -> FeedHome {
        FeedHome {
            kind: FeedHomeKind::Thread,
            id: id.to_string(),
            project_id: PROJECT.to_string(),
        }
    }

    fn job_home(id: &str) -> FeedHome {
        FeedHome {
            kind: FeedHomeKind::Job,
            id: id.to_string(),
            project_id: PROJECT.to_string(),
        }
    }

    fn ids(page: &FeedPage) -> Vec<i64> {
        page.posts.iter().map(|post| post.id).collect()
    }

    async fn position(db: &LocalDb, home: &FeedHome) -> i64 {
        db.query_opt(
            "SELECT acknowledged_post_id FROM feed_cursors WHERE home_kind = ?1 AND home_id = ?2",
            (home.kind.as_str().to_string(), home.id.clone()),
            |row| row.i64(0),
        )
        .await
        .unwrap()
        .unwrap_or(0)
    }

    /// A thread and a job addressed from the same project are different homes,
    /// and a task's job is a home like any other: acknowledging one moves only
    /// that one.
    #[tokio::test]
    async fn each_durable_home_holds_its_own_position() {
        let db = fixture("feed-independent-homes.db").await;
        post(&db, None, "first").await;
        post(&db, None, "second").await;
        let thread = thread_home("thread-row-1");
        let node = job_home("job-node");
        let task = job_home("job-task");

        let thread_page = db.issue_feed_page(&thread, 10).await.unwrap();
        let node_page = db.issue_feed_page(&node, 10).await.unwrap();
        let task_page = db.issue_feed_page(&task, 10).await.unwrap();
        assert_eq!(ids(&thread_page), vec![1, 2]);
        assert_eq!(ids(&node_page), vec![1, 2]);
        assert_eq!(ids(&task_page), vec![1, 2]);
        assert_ne!(thread_page.token, node_page.token);
        assert_ne!(node_page.token, task_page.token);

        db.acknowledge_feed(&thread, thread_page.token.as_deref().unwrap())
            .await
            .unwrap();
        assert!(db
            .issue_feed_page(&thread, 10)
            .await
            .unwrap()
            .posts
            .is_empty());
        assert_eq!(
            ids(&db.issue_feed_page(&node, 10).await.unwrap()),
            vec![1, 2]
        );
        assert_eq!(
            ids(&db.issue_feed_page(&task, 10).await.unwrap()),
            vec![1, 2]
        );
    }

    /// Workspace posts reach every home; a project's posts reach only homes
    /// inside it. Scope is enforced in the query, not after rendering.
    #[tokio::test]
    async fn scope_admits_workspace_and_the_home_s_own_project_only() {
        let db = fixture("feed-scope.db").await;
        let workspace = post(&db, None, "everyone").await;
        let own = post(&db, Some(PROJECT), "our project").await;
        post(&db, Some(OTHER_PROJECT), "not ours").await;

        let page = db.issue_feed_page(&thread_home("t"), 10).await.unwrap();
        assert_eq!(ids(&page), vec![workspace, own]);
        assert_eq!(page.remaining_unread, 0);
    }

    /// Repeated read+ack past the page size delivers every post exactly once, in
    /// id order, with no older post skipped to show a newer one.
    #[tokio::test]
    async fn every_post_arrives_exactly_once_oldest_first_across_pages() {
        let db = fixture("feed-paging.db").await;
        for index in 0..5 {
            post(&db, None, &format!("post {index}")).await;
        }
        let home = thread_home("t");

        let mut seen = Vec::new();
        let mut remaining_seen = Vec::new();
        loop {
            let page = db.issue_feed_page(&home, 2).await.unwrap();
            let Some(token) = page.token.clone() else {
                assert!(page.posts.is_empty());
                break;
            };
            assert!(page.posts.len() <= 2, "a page never exceeds its limit");
            remaining_seen.push(page.remaining_unread);
            seen.extend(ids(&page));
            db.acknowledge_feed(&home, &token).await.unwrap();
        }
        assert_eq!(seen, vec![1, 2, 3, 4, 5]);
        assert_eq!(remaining_seen, vec![3, 1, 0]);
    }

    /// Crash-before-ack: the next read replays the identical rows. The token is
    /// fresh, and the superseded one no longer moves anything.
    #[tokio::test]
    async fn reading_twice_without_acknowledging_replays_the_page_under_a_fresh_token() {
        let db = fixture("feed-replay.db").await;
        post(&db, None, "first").await;
        post(&db, None, "second").await;
        let home = thread_home("t");

        let first = db.issue_feed_page(&home, 10).await.unwrap();
        let second = db.issue_feed_page(&home, 10).await.unwrap();
        assert_eq!(ids(&first), ids(&second));
        assert_ne!(first.token, second.token);
        assert_eq!(first.acknowledged_through, second.acknowledged_through);

        let stale = first.token.unwrap();
        assert!(matches!(
            db.acknowledge_feed(&home, &stale).await.unwrap(),
            FeedAck::Rejected(_)
        ));
        assert_eq!(position(&db, &home).await, 0);

        assert_eq!(
            db.acknowledge_feed(&home, second.token.as_deref().unwrap())
                .await
                .unwrap(),
            FeedAck::Advanced { from: 0, to: 2 }
        );
    }

    /// A token is bound to the home that issued it, and to nothing else. A
    /// fabricated token is the same rejection: neither moves a position.
    #[tokio::test]
    async fn a_token_moves_only_the_home_it_was_issued_for() {
        let db = fixture("feed-cross-home.db").await;
        post(&db, None, "first").await;
        let thread = thread_home("t");
        let node = job_home("j");

        let thread_page = db.issue_feed_page(&thread, 10).await.unwrap();
        db.issue_feed_page(&node, 10).await.unwrap();
        let borrowed = thread_page.token.unwrap();

        assert!(matches!(
            db.acknowledge_feed(&node, &borrowed).await.unwrap(),
            FeedAck::Rejected(_)
        ));
        assert_eq!(position(&db, &node).await, 0);

        assert!(matches!(
            db.acknowledge_feed(&thread, "not-a-token-anyone-issued")
                .await
                .unwrap(),
            FeedAck::Rejected(_)
        ));
        assert_eq!(position(&db, &thread).await, 0);

        // The genuine token still works: a rejection consumed nothing.
        assert_eq!(
            db.acknowledge_feed(&thread, &borrowed).await.unwrap(),
            FeedAck::Advanced { from: 0, to: 1 }
        );
    }

    /// An acknowledgement is bounded by what its page showed, even when higher
    /// posts exist by the time it arrives.
    #[tokio::test]
    async fn acknowledgement_stops_at_what_its_page_showed() {
        let db = fixture("feed-bounded.db").await;
        post(&db, None, "first").await;
        post(&db, None, "second").await;
        let home = thread_home("t");

        let page = db.issue_feed_page(&home, 10).await.unwrap();
        assert_eq!(ids(&page), vec![1, 2]);

        post(&db, None, "third").await;
        post(&db, None, "fourth").await;

        assert_eq!(
            db.acknowledge_feed(&home, page.token.as_deref().unwrap())
                .await
                .unwrap(),
            FeedAck::Advanced { from: 0, to: 2 },
            "posts that arrived after the issuance stay unread"
        );
        assert_eq!(
            ids(&db.issue_feed_page(&home, 10).await.unwrap()),
            vec![3, 4]
        );
    }

    /// Replaying an acknowledgement whose reply was lost is a no-op success —
    /// distinguishable from a stale issuance, and still not a way to move.
    #[tokio::test]
    async fn replaying_a_successful_acknowledgement_is_an_idempotent_success() {
        let db = fixture("feed-idempotent.db").await;
        post(&db, None, "first").await;
        post(&db, None, "second").await;
        let home = thread_home("t");

        let page = db.issue_feed_page(&home, 10).await.unwrap();
        let token = page.token.unwrap();
        assert_eq!(
            db.acknowledge_feed(&home, &token).await.unwrap(),
            FeedAck::Advanced { from: 0, to: 2 }
        );
        assert_eq!(
            db.acknowledge_feed(&home, &token).await.unwrap(),
            FeedAck::AlreadyAcknowledged { at: 2 }
        );

        // Still a replay after a later read has taken the outstanding slot, and
        // still no movement past what it originally showed.
        post(&db, None, "third").await;
        db.issue_feed_page(&home, 10).await.unwrap();
        assert_eq!(
            db.acknowledge_feed(&home, &token).await.unwrap(),
            FeedAck::AlreadyAcknowledged { at: 2 }
        );
        assert_eq!(position(&db, &home).await, 2);
    }

    /// An empty feed mints nothing. There is no token, no cursor row, and
    /// nothing an acknowledgement could advance.
    #[tokio::test]
    async fn an_empty_feed_issues_no_advancement() {
        let db = fixture("feed-empty.db").await;
        let home = thread_home("t");

        let page = db.issue_feed_page(&home, 10).await.unwrap();
        assert!(page.posts.is_empty());
        assert_eq!(page.token, None);
        assert_eq!(page.remaining_unread, 0);
        assert_eq!(
            db.query_opt("SELECT COUNT(*) FROM feed_cursors", (), |row| row.i64(0))
                .await
                .unwrap(),
            Some(0),
            "reading an empty feed records no issuance"
        );
        assert!(matches!(
            db.acknowledge_feed(&home, "anything").await.unwrap(),
            FeedAck::Rejected(_)
        ));
    }

    /// The page size is bounded on both ends, so neither a zero nor an enormous
    /// limit produces a page the server did not intend.
    #[tokio::test]
    async fn the_page_size_is_clamped_at_both_ends() {
        let db = fixture("feed-limits.db").await;
        for index in 0..3 {
            post(&db, None, &format!("post {index}")).await;
        }
        let home = thread_home("t");
        assert_eq!(ids(&db.issue_feed_page(&home, 0).await.unwrap()), vec![1]);
        assert_eq!(
            ids(&db.issue_feed_page(&home, usize::MAX).await.unwrap()),
            vec![1, 2, 3]
        );
    }
}
