//! Who a post, its comments, and the references written into them actually
//! reach.
//!
//! These exercise [`super::posts`] against a migrated database rather than the
//! mutation dispatch above it, because every question here — jurisdiction,
//! late home resolution, dedup, mute — is settled in the routing layer and the
//! dispatch layer only forwards its verdict.

use cairn_common::identity::{
    Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, PrincipalRef,
    VerificationMethod, VerificationRecord, VerificationStatus, VerificationStrength,
};

use super::posts::{
    post_push_source, record_new_post_attention, record_post_comment_attention,
    NEW_POST_PUSH_PREFIX, POST_COMMENT_PUSH_PREFIX, POST_MENTION_PUSH_PREFIX,
};
use super::store::{mute, subscribe, unsubscribe_matching};
use super::types::SOURCE_KIND_POST;
use crate::models::{Post, PostComment};
use crate::orchestrator::attention_push::{list_pending, Wake};
use crate::storage::LocalDb;

async fn migrated_db() -> LocalDb {
    crate::storage::migrated_test_db("wake-posts.db").await
}

/// Two projects, each with a node home, plus a second home in the first and a
/// thread there.
///
/// Both projects are seeded on every test so a jurisdiction assertion always
/// has a genuine "elsewhere" to exclude rather than an empty one. The second
/// alpha home exists so a test can post *into* alpha from a node that is not
/// the alpha subscriber, since an author never wakes itself.
async fn seed(db: &LocalDb) {
    db.execute_script(
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES('proj-a','w','Alpha','alpha','/tmp/a',1,1),
                 ('proj-b','w','Beta','beta','/tmp/b',1,1);
         INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
           VALUES('issue-a','proj-a',1,'A','active','active','none',1,1),
                 ('issue-a2','proj-a',2,'A2','active','active','none',1,1),
                 ('issue-b','proj-b',1,'B','active','active','none',1,1);
         INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
           VALUES('exec-a','r','issue-a','proj-a','running',1,1),
                 ('exec-a2','r','issue-a2','proj-a','running',1,1),
                 ('exec-b','r','issue-b','proj-b','running',1,1);
         INSERT INTO jobs(id, execution_id, issue_id, project_id, status, node_name, uri_segment, created_at, updated_at)
           VALUES('job-a','exec-a','issue-a','proj-a','running','builder','builder',1,1),
                 ('job-a2','exec-a2','issue-a2','proj-a','running','builder','builder',1,1),
                 ('job-b','exec-b','issue-b','proj-b','running','builder','builder',1,1);
         INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
           VALUES('thread-a','proj-a','general','active','none',1,1);",
    )
    .await
    .unwrap();
}

/// A thread session job, minted directly so a test can control its ordering and
/// stand a *second* one up to represent a rotation.
async fn seed_thread_session(db: &LocalDb, job_id: &str, created_at: i64) {
    db.execute_script(&format!(
        "INSERT INTO jobs(id, thread_id, project_id, status, node_name, uri_segment, created_at, updated_at)
           VALUES('{job_id}','thread-a','proj-a','running','thread','thread',{created_at},{created_at});"
    ))
    .await
    .unwrap();
}

fn agent(node: &str) -> PrincipalRef {
    PrincipalRef::Agent {
        node: node.to_string(),
        run_id: Some("run-1".to_string()),
    }
}

/// The provenance a post carries. Its content is irrelevant to routing — only
/// [`Post::author`] is consulted — so this is the shortest snapshot that
/// validates.
fn appearance(principal: &PrincipalRef) -> AppearanceSnapshot {
    let verification = VerificationRecord::new(
        VerificationMethod::NodeSession,
        VerificationStatus::Verified,
        None,
        None,
        Some("run-1".to_string()),
        None,
        VerificationStrength::new("session-bound").unwrap(),
        1,
    )
    .unwrap();
    let evidence = AppearanceEvidence::new(
        AppearanceTransport::ResourcePatch,
        Address::Resource {
            node: "cairn://p/alpha/1/1/builder".to_string(),
        },
        verification,
        1,
        None,
    )
    .unwrap();
    AppearanceSnapshot::new(principal.clone(), evidence, vec![], None).unwrap()
}

fn post(id: i64, project_id: Option<&str>, author: &str, content: &str) -> Post {
    let author = agent(author);
    Post {
        id,
        project_id: project_id.map(str::to_string),
        title: None,
        content: content.to_string(),
        appearance: appearance(&author),
        author,
        author_display: None,
        created_at: 1,
    }
}

fn comment(id: i64, post_id: i64, author: &str, content: &str) -> PostComment {
    let author = agent(author);
    PostComment {
        id,
        post_id,
        content: content.to_string(),
        appearance: appearance(&author),
        author,
        author_display: None,
        created_at: 2,
    }
}

async fn watch_posts(db: &LocalDb, job_id: &str) {
    subscribe(db, job_id, SOURCE_KIND_POST, None, None, "agent")
        .await
        .unwrap();
}

/// The keys of a recipient's pending pushes, paired with the wake level each
/// row was created at — the two facts that distinguish "was told" from "was
/// roused".
async fn pending(db: &LocalDb, job_id: &str) -> Vec<(String, Wake)> {
    list_pending(db, job_id)
        .await
        .unwrap()
        .into_iter()
        .map(|push| (push.key, push.wake))
        .collect()
}

/// Each post push key must be recognized by both surfaces that read one: the
/// central mute consultation, which needs the `(source, fact)` the prefix stands
/// for, and the headline renderer that turns a drained push into a sentence.
///
/// This is where the two vocabularies meet. A prefix missing from either side
/// fails silently — an unmuteable wake, or a push that renders as the generic
/// "Attention update" — so both are asserted against the constants the routing
/// actually writes rather than against restated literals.
#[test]
fn every_post_push_prefix_is_muteable_and_renders_a_headline() {
    for prefix in [
        NEW_POST_PUSH_PREFIX,
        POST_COMMENT_PUSH_PREFIX,
        POST_MENTION_PUSH_PREFIX,
    ] {
        let (source_kind, fact_kind) =
            post_push_source(prefix).unwrap_or_else(|| panic!("{prefix} has no wake source"));
        assert_eq!(source_kind, SOURCE_KIND_POST);
        assert!(
            !fact_kind.is_empty(),
            "{prefix} must name the fact a Posts subscription can scope to"
        );

        let (kind, headline) = crate::orchestrator::attention_push::push_kind_headline(prefix);
        assert_eq!(
            kind, "post",
            "{prefix} renders under the post wake-card kind"
        );
        assert_ne!(
            headline, "Attention update",
            "{prefix} falls through to the generic headline"
        );
    }

    // A prefix that is not a post push is left alone, so nothing else is
    // accidentally routed through the Posts mute axis.
    assert!(post_push_source("direct").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn an_active_posts_subscriber_receives_a_new_post() {
    let db = migrated_db().await;
    seed(&db).await;
    watch_posts(&db, "job-a").await;

    let attention =
        record_new_post_attention(&db, &post(1, None, "cairn://p/beta/1/1/builder", "hi"))
            .await
            .unwrap();

    assert_eq!(attention.recorded, vec!["job-a".to_string()]);
    assert_eq!(
        attention.woken,
        vec!["job-a".to_string()],
        "an unmuted subscriber is roused, not merely told"
    );
    assert_eq!(attention.failed, 0);
    assert_eq!(
        pending(&db, "job-a").await,
        vec![("post:cairn://posts/1".to_string(), Wake::Wake)]
    );
}

/// A node that never elected to watch Posts is not a recipient, and an
/// unsubscribe genuinely removes one — the feed is opt-in on both edges.
#[tokio::test(flavor = "current_thread")]
async fn only_subscribed_nodes_receive_the_feed() {
    let db = migrated_db().await;
    seed(&db).await;
    watch_posts(&db, "job-a").await;

    let attention =
        record_new_post_attention(&db, &post(1, None, "cairn://p/beta/1/1/builder", "hi"))
            .await
            .unwrap();
    assert_eq!(attention.recorded, vec!["job-a".to_string()]);
    assert!(
        pending(&db, "job-b").await.is_empty(),
        "a node that never subscribed receives nothing"
    );

    unsubscribe_matching(&db, "job-a", SOURCE_KIND_POST, None, "agent")
        .await
        .unwrap();
    let after = record_new_post_attention(&db, &post(2, None, "cairn://p/beta/1/1/builder", "hi"))
        .await
        .unwrap();
    assert!(
        after.recorded.is_empty(),
        "unsubscribing stops the feed: {after:?}"
    );
}

/// Jurisdiction is mandatory and is applied at delivery, not at subscribe time:
/// the same two standing subscriptions receive differently depending on the
/// scope of the post being routed.
#[tokio::test(flavor = "current_thread")]
async fn a_project_scoped_post_reaches_only_that_projects_subscribers() {
    let db = migrated_db().await;
    seed(&db).await;
    watch_posts(&db, "job-a").await;
    watch_posts(&db, "job-b").await;

    let scoped = record_new_post_attention(
        &db,
        &post(1, Some("proj-a"), "cairn://p/alpha/2/1/builder", "scoped"),
    )
    .await
    .unwrap();
    assert_eq!(
        scoped.recorded,
        vec!["job-a".to_string()],
        "a project-scoped post excludes a subscriber homed in another project"
    );

    let workspace = record_new_post_attention(
        &db,
        &post(2, None, "cairn://p/alpha/2/1/builder", "everyone"),
    )
    .await
    .unwrap();
    let mut reached = workspace.recorded.clone();
    reached.sort();
    assert_eq!(
        reached,
        vec!["job-a".to_string(), "job-b".to_string()],
        "a workspace-wide post reaches both"
    );
}

/// Mute governs how loudly a subscriber is told, never whether it is a
/// recipient. The row is still written — as `Passive`, the existing suppressed
/// ride-along — so the catch-up digest has something to accumulate and no
/// scheduler is involved.
#[tokio::test(flavor = "current_thread")]
async fn a_muted_posts_subscription_accumulates_passively() {
    let db = migrated_db().await;
    seed(&db).await;
    watch_posts(&db, "job-a").await;
    mute(
        &db,
        "job-a",
        SOURCE_KIND_POST,
        None,
        None,
        None,
        None,
        "agent",
    )
    .await
    .unwrap();

    let attention = record_new_post_attention(
        &db,
        &post(1, None, "cairn://p/beta/1/1/builder", "quiet news"),
    )
    .await
    .unwrap();

    assert_eq!(
        attention.recorded,
        vec!["job-a".to_string()],
        "a muted subscriber is still a recipient"
    );
    assert!(
        attention.woken.is_empty(),
        "a muted subscriber is not roused: {attention:?}"
    );
    assert_eq!(
        pending(&db, "job-a").await,
        vec![("post:cairn://posts/1".to_string(), Wake::Passive)],
        "the push is created as the existing passive ride-along"
    );
}

/// The author's home is resolved fresh at delivery, so a thread that rotated to
/// a new session after posting receives the comment on the session it is on
/// now — not the one that happened to be live when the post was written.
#[tokio::test(flavor = "current_thread")]
async fn a_comment_reaches_the_post_authors_newest_thread_session() {
    let db = migrated_db().await;
    seed(&db).await;
    seed_thread_session(&db, "session-old", 10).await;

    let authored = post(1, None, "cairn://p/alpha/general", "from a thread");
    seed_thread_session(&db, "session-new", 20).await;

    let attention = record_post_comment_attention(
        &db,
        &authored,
        &comment(1, 1, "cairn://p/beta/1/1/builder", "reply"),
    )
    .await
    .unwrap();

    assert_eq!(
        attention.recorded,
        vec!["session-new".to_string()],
        "the comment follows the thread's rotation"
    );
    assert!(
        pending(&db, "session-old").await.is_empty(),
        "the retired session is not woken"
    );
    assert_eq!(
        pending(&db, "session-new").await,
        vec![("post-comment:cairn://posts/1".to_string(), Wake::Wake)]
    );
}

/// A comment is not a feed event. Everyone watching Posts hears about the post;
/// only the author hears about the reply.
#[tokio::test(flavor = "current_thread")]
async fn a_comment_reaches_the_author_and_not_the_feed() {
    let db = migrated_db().await;
    seed(&db).await;
    watch_posts(&db, "job-b").await;

    let authored = post(1, None, "cairn://p/alpha/1/1/builder", "mine");
    let attention = record_post_comment_attention(
        &db,
        &authored,
        &comment(1, 1, "cairn://p/beta/1/1/builder", "reply"),
    )
    .await
    .unwrap();

    assert_eq!(attention.recorded, vec!["job-a".to_string()]);
    assert!(
        pending(&db, "job-b").await.is_empty(),
        "a Posts subscriber is not a comment recipient"
    );
}

/// Commenting on your own post does not wake you, and neither does posting to a
/// feed you yourself watch.
#[tokio::test(flavor = "current_thread")]
async fn an_author_is_not_woken_by_their_own_writing() {
    let db = migrated_db().await;
    seed(&db).await;
    watch_posts(&db, "job-a").await;

    let own = record_new_post_attention(&db, &post(1, None, "cairn://p/alpha/1/1/builder", "mine"))
        .await
        .unwrap();
    assert!(own.recorded.is_empty(), "posting does not wake the poster");

    let authored = post(2, None, "cairn://p/alpha/1/1/builder", "mine");
    let self_reply = record_post_comment_attention(
        &db,
        &authored,
        &comment(1, 2, "cairn://p/alpha/1/1/builder", "and another thing"),
    )
    .await
    .unwrap();
    assert!(
        self_reply.recorded.is_empty(),
        "replying to yourself wakes nobody: {self_reply:?}"
    );
}

/// A thread URI in a post's body reaches the thread's newest session, by the
/// same late resolution the author route uses.
#[tokio::test(flavor = "current_thread")]
async fn a_thread_mention_wakes_the_threads_newest_session() {
    let db = migrated_db().await;
    seed(&db).await;
    seed_thread_session(&db, "session-old", 10).await;
    seed_thread_session(&db, "session-new", 20).await;

    let attention = record_new_post_attention(
        &db,
        &post(
            1,
            None,
            "cairn://p/beta/1/1/builder",
            "worth a look, cairn://p/alpha/general",
        ),
    )
    .await
    .unwrap();

    assert_eq!(attention.recorded, vec!["session-new".to_string()]);
    assert_eq!(
        pending(&db, "session-new").await,
        vec![("post-mention:cairn://posts/1".to_string(), Wake::Wake)]
    );
}

/// An issue reference routes through the canonical issue-watcher set, and a
/// node reference routes to that node's current job — the two other reference
/// classes, distinguished from each other and from citations.
#[tokio::test(flavor = "current_thread")]
async fn issue_and_node_references_route_through_existing_machinery() {
    let db = migrated_db().await;
    seed(&db).await;

    let node_ref = record_new_post_attention(
        &db,
        &post(
            1,
            None,
            "cairn://p/beta/1/1/builder",
            "see cairn://p/alpha/1/1/builder",
        ),
    )
    .await
    .unwrap();
    assert_eq!(node_ref.recorded, vec!["job-a".to_string()]);

    let issue_ref = record_new_post_attention(
        &db,
        &post(
            2,
            None,
            "cairn://p/beta/1/1/builder",
            "see cairn://p/alpha/1",
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        issue_ref.recorded,
        super::child::watcher_jobs_for_issue(&db, "cairn://p/alpha/1")
            .await
            .unwrap(),
        "an issue reference reaches exactly that issue's watchers"
    );

    // A sub-resource of a node is a citation of where to look, not a
    // destination: it says nothing about who to tell.
    let citation = record_new_post_attention(
        &db,
        &post(
            3,
            None,
            "cairn://p/beta/1/1/builder",
            "cairn://p/alpha/1/1/builder/diff",
        ),
    )
    .await
    .unwrap();
    assert!(
        citation.recorded.is_empty(),
        "a diff link is a citation, not a wake: {citation:?}"
    );
}

/// Mentions work in comments exactly as they do in posts, and the mentioned
/// destination is reached alongside the author rather than instead of them.
#[tokio::test(flavor = "current_thread")]
async fn a_comment_mention_routes_alongside_the_author() {
    let db = migrated_db().await;
    seed(&db).await;

    let authored = post(1, None, "cairn://p/alpha/1/1/builder", "mine");
    let attention = record_post_comment_attention(
        &db,
        &authored,
        &comment(
            1,
            1,
            "cairn://p/beta/1/1/builder",
            "asking cairn://p/beta/1/1/builder's neighbour: cairn://p/alpha/1/1/builder",
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        attention.recorded,
        vec!["job-a".to_string()],
        "the mentioned node is the author here, and is woken exactly once"
    );
    assert_eq!(
        pending(&db, "job-a").await.len(),
        1,
        "qualifying twice produces one push"
    );
}

/// Text that merely looks like a reference is not one. Nothing that fails to
/// parse as a canonical resource routes, and a raw job id — the one identifier
/// a caller might hope to smuggle in — is not addressable at all.
#[tokio::test(flavor = "current_thread")]
async fn malformed_and_unaddressable_references_route_nowhere() {
    let db = migrated_db().await;
    seed(&db).await;

    let attention = record_new_post_attention(
        &db,
        &post(
            1,
            None,
            "cairn://p/beta/1/1/builder",
            "cairn:// cairn://p/ cairn://p//1 cairn://p/alpha/notanumber \
             job-a job:job-a cairn://job/job-a cairn://p/nosuchproject/general",
        ),
    )
    .await
    .unwrap();

    assert!(
        attention.recorded.is_empty(),
        "malformed and unresolvable references route nowhere: {attention:?}"
    );
    assert_eq!(attention.failed, 0);
}

/// The same destination named repeatedly, and a destination that qualifies
/// through two different routes at once, each owe exactly one wake.
#[tokio::test(flavor = "current_thread")]
async fn repeated_and_overlapping_routes_deliver_once() {
    let db = migrated_db().await;
    seed(&db).await;
    // job-a qualifies twice over: it watches the feed AND is named in the body,
    // which itself names it three times.
    watch_posts(&db, "job-a").await;

    let attention = record_new_post_attention(
        &db,
        &post(
            1,
            None,
            "cairn://p/beta/1/1/builder",
            "cairn://p/alpha/1/1/builder and cairn://p/alpha/1/1/builder, plus \
             cairn://p/ALPHA/1/1/builder",
        ),
    )
    .await
    .unwrap();

    assert_eq!(attention.recorded, vec!["job-a".to_string()]);
    assert_eq!(
        pending(&db, "job-a").await,
        vec![("post-mention:cairn://posts/1".to_string(), Wake::Wake)],
        "the more specific reason claims the recipient, and claims it once"
    );
}

/// Jurisdiction binds the mention route too. A project-scoped post cannot reach
/// out of its project by naming a destination there explicitly.
#[tokio::test(flavor = "current_thread")]
async fn a_scoped_post_cannot_mention_its_way_out_of_its_project() {
    let db = migrated_db().await;
    seed(&db).await;

    let attention = record_new_post_attention(
        &db,
        &post(
            1,
            Some("proj-a"),
            "cairn://p/alpha/1/1/builder",
            "hey cairn://p/beta/1/1/builder",
        ),
    )
    .await
    .unwrap();

    assert!(
        attention.recorded.is_empty(),
        "a scoped post's mention of an out-of-scope home routes nowhere: {attention:?}"
    );
}

/// A second post before the first is drained supersedes in place on the shared
/// `(recipient, key)` — one pending row per post, not a stack per event.
#[tokio::test(flavor = "current_thread")]
async fn a_repeat_comment_supersedes_rather_than_stacks() {
    let db = migrated_db().await;
    seed(&db).await;

    let authored = post(1, None, "cairn://p/alpha/1/1/builder", "mine");
    for id in 1..=2 {
        record_post_comment_attention(
            &db,
            &authored,
            &comment(id, 1, "cairn://p/beta/1/1/builder", "reply"),
        )
        .await
        .unwrap();
    }

    assert_eq!(
        pending(&db, "job-a").await,
        vec![("post-comment:cairn://posts/1".to_string(), Wake::Wake)],
        "a second comment refreshes the pending row instead of stacking"
    );
}

/// A human-authored post has no session for a comment to wake, and saying so
/// costs nothing — the comment still commits, and routing reports zero
/// recipients rather than an error.
#[tokio::test(flavor = "current_thread")]
async fn a_non_agent_author_has_no_home_to_wake() {
    let db = migrated_db().await;
    seed(&db).await;

    let author = PrincipalRef::Human {
        issuer: "api".to_string(),
        subject: "operator".to_string(),
        organization: None,
    };
    let authored = Post {
        id: 1,
        project_id: None,
        title: None,
        content: "from a person".to_string(),
        appearance: appearance(&author),
        author,
        author_display: None,
        created_at: 1,
    };

    let attention = record_post_comment_attention(
        &db,
        &authored,
        &comment(1, 1, "cairn://p/beta/1/1/builder", "reply"),
    )
    .await
    .unwrap();

    assert!(attention.recorded.is_empty());
    assert_eq!(attention.failed, 0);
}
