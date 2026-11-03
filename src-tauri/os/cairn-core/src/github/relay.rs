//! GitHub App webhooks delivered over the install's relay channel.
//!
//! [`crate::ingress::sync`] reads the channel and opens each event. A webhook
//! reaches [`process_event`] one of two ways: through the App's own endpoint
//! (`/e/:id`, signature-verified by the api), via [`crate::ingress::consume`],
//! or as a legacy `/hook` event with no endpoint, via [`handle_github_event`].
//! A mention of the App becomes a route fact, and a branch push reaches the
//! base-branch sync.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::{
    github::credentials,
    ingress::sync::{EventOutcome, RelayEvent},
    orchestrator::Orchestrator,
    projects::remote::find_project_by_remote_full_name,
    routes::RouteFact,
};

#[derive(Debug, PartialEq)]
pub enum TriggerDecision {
    Ignore,
    Comment(CommentTrigger),
}

#[derive(Debug, PartialEq)]
pub struct CommentTrigger {
    pub comment_id: i64,
    pub repository: String,
    pub number: i64,
    pub kind: &'static str,
    pub author: String,
    pub url: String,
    pub body: String,
}

/// Maps only newly-created issue comments that explicitly address this app.
/// GitHub uses `issue_comment` for both issue and pull-request conversation.
pub fn trigger_for_event(event_type: &str, payload: &Value, app_slug: &str) -> TriggerDecision {
    if event_type != "issue_comment" || payload["action"].as_str() != Some("created") {
        return TriggerDecision::Ignore;
    }
    let Some(body) = payload["comment"]["body"].as_str() else {
        return TriggerDecision::Ignore;
    };
    if !contains_mention(body, app_slug) {
        return TriggerDecision::Ignore;
    }
    let author = payload["comment"]["user"]["login"]
        .as_str()
        .unwrap_or_default();
    let author_type = payload["comment"]["user"]["type"]
        .as_str()
        .unwrap_or_default();
    if author.is_empty()
        || author_type.eq_ignore_ascii_case("bot")
        || author.eq_ignore_ascii_case(app_slug)
        || author.eq_ignore_ascii_case(&format!("{app_slug}[bot]"))
    {
        return TriggerDecision::Ignore;
    }
    let Some(repository) = payload["repository"]["full_name"].as_str() else {
        return TriggerDecision::Ignore;
    };
    let Some(number) = payload["issue"]["number"].as_i64() else {
        return TriggerDecision::Ignore;
    };
    let Some(comment_id) = payload["comment"]["id"].as_i64() else {
        return TriggerDecision::Ignore;
    };
    TriggerDecision::Comment(CommentTrigger {
        comment_id,
        repository: repository.to_owned(),
        number,
        kind: if payload["issue"].get("pull_request").is_some() {
            "pull_request"
        } else {
            "issue"
        },
        author: author.to_owned(),
        url: payload["comment"]["html_url"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        body: body.to_owned(),
    })
}

fn contains_mention(body: &str, app_slug: &str) -> bool {
    let mention = format!("@{}", app_slug.to_ascii_lowercase());
    body.split(|character: char| {
        !(character.is_ascii_alphanumeric()
            || character == '-'
            || character == '_'
            || character == '@')
    })
    .any(|word| word.to_ascii_lowercase() == mention)
}

/// The issue description a mention becomes: where the comment came from, then
/// the comment itself. The fact log rebuilds a comment fact's `body` with this
/// from its stored fields, so the two can never disagree.
pub(crate) fn comment_context(
    repository: &str,
    kind: &str,
    number: &str,
    author: &str,
    url: &str,
    text: &str,
) -> String {
    format!("Repository: {repository}\nGitHub {kind}: #{number}\nAuthor: @{author}\nURL: {url}\n\n{text}")
}

pub(crate) fn comment_fact(
    comment: &CommentTrigger,
    project: &str,
    origin: crate::issues::crud::IssueAuthorship,
) -> RouteFact {
    let title = format!(
        "GitHub {} #{} mention from @{}",
        comment.kind.replace('_', " "),
        comment.number,
        comment.author
    );
    let context = comment_context(
        &comment.repository,
        comment.kind,
        &comment.number.to_string(),
        &comment.author,
        &comment.url,
        &comment.body,
    );
    RouteFact {
        source: "github_comment".into(),
        identity: format!("github-comment:{}", comment.comment_id),
        fields: BTreeMap::from([
            ("project".into(), json!(project)),
            ("repository".into(), json!(comment.repository)),
            ("number".into(), json!(comment.number.to_string())),
            ("kind".into(), json!(comment.kind)),
            ("author".into(), json!(comment.author)),
            ("url".into(), json!(comment.url)),
            ("title".into(), json!(title)),
            ("body".into(), json!(context)),
            ("text".into(), json!(comment.body)),
        ]),
        origin: Some(origin),
        summary: Some(title),
        route_provenance: None,
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProcessEventOutcome {
    Ignored,
    UnmappedMention,
    Routed,
    /// A push to one of a repository's branches, handed to the base-branch
    /// sync in the background, which decides whether the branch is one it
    /// tracks.
    BranchPush,
}

impl ProcessEventOutcome {
    /// How an `ingress_events` row names the outcome.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Ignored => "ignored",
            Self::UnmappedMention => "unmapped_mention",
            Self::Routed => "routed",
            Self::BranchPush => "push",
        }
    }
}

/// The `push` fields the base-branch sync needs, when this is a push that left
/// a branch (not a tag, not a deletion) at a new commit.
fn branch_push(event_type: &str, payload: &Value) -> Option<BranchPush> {
    if event_type != "push" || payload["deleted"].as_bool() == Some(true) {
        return None;
    }
    let push = BranchPush {
        repository: payload["repository"]["full_name"].as_str()?.to_string(),
        git_ref: payload["ref"].as_str()?.to_string(),
        default_branch: payload["repository"]["default_branch"]
            .as_str()?
            .to_string(),
        after: payload["after"].as_str()?.to_string(),
    };
    push.branch().is_some().then_some(push)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BranchPush {
    repository: String,
    git_ref: String,
    default_branch: String,
    after: String,
}

impl BranchPush {
    fn branch(&self) -> Option<&str> {
        self.git_ref
            .strip_prefix("refs/heads/")
            .filter(|branch| !branch.is_empty())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum BranchPushOutcome {
    /// A push to the repository's default branch, with what the checkout pull
    /// made of it.
    Default(crate::projects::remote::PushRemoteSyncOutcome),
    NoMatchingProject,
    /// A branch nothing live is cut from: the store's own, and none of the
    /// sync's business. Cairn's own pushes of agent branches land here.
    Untracked,
    /// A tracked base branch; the sync was told where origin now points.
    Observed {
        project_id: String,
    },
}

/// Map a branch push onto its project and tell the base-branch sync where
/// origin now points, when the branch is one it tracks. A default-branch push
/// also fast-forwards the operator's checkout when that is safe.
async fn handle_branch_push(
    orch: &Orchestrator,
    push: &BranchPush,
) -> Result<BranchPushOutcome, String> {
    let Some(branch) = push.branch() else {
        return Ok(BranchPushOutcome::Untracked);
    };
    let source = crate::orchestrator::base_branch_sync::Source::Push;
    let observation = orch.base_branch_sync.observe();
    if crate::projects::remote::is_default_branch_push(&push.git_ref, &push.default_branch) {
        let outcome = crate::projects::remote::pull_project_on_default_branch_push(
            &orch.db.local,
            orch.services.git.as_ref(),
            &push.repository,
            &push.git_ref,
            &push.default_branch,
        )
        .await
        .map_err(|error| error.to_string())?;
        if let crate::projects::remote::PushRemoteSyncOutcome::Pulled {
            project_id,
            default_branch,
        } = &outcome
        {
            crate::orchestrator::base_branch_sync::observe_remote_tip(
                orch,
                project_id,
                default_branch,
                &push.after,
                source,
                observation,
            )
            .await;
        }
        return Ok(BranchPushOutcome::Default(outcome));
    }
    let Some(project) = crate::projects::remote::find_project_by_remote_full_name(
        &orch.db.local,
        orch.services.git.as_ref(),
        &push.repository,
    )
    .await
    .map_err(|error| error.to_string())?
    else {
        return Ok(BranchPushOutcome::NoMatchingProject);
    };
    let default_branch = project
        .default_branch
        .clone()
        .unwrap_or_else(|| push.default_branch.clone());
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &project.id)
        .await
        .map_err(|error| error.to_string())?;
    let tracked = crate::orchestrator::base_advance::load_tracked_base_branches(
        &db,
        &project.id,
        &default_branch,
    )
    .await?;
    if !tracked.iter().any(|tracked| tracked == branch) {
        return Ok(BranchPushOutcome::Untracked);
    }
    crate::orchestrator::base_branch_sync::observe_remote_tip(
        orch,
        &project.id,
        branch,
        &push.after,
        source,
        observation,
    )
    .await;
    Ok(BranchPushOutcome::Observed {
        project_id: project.id,
    })
}

/// Handle one App webhook. `origin` is who a mention's fact is from: the
/// verified GitHub sender for an endpoint delivery, this machine for a legacy
/// `/hook` one nothing verified.
pub async fn process_event(
    orch: &Orchestrator,
    event_id: i64,
    event_type: &str,
    payload: &Value,
    app_slug: &str,
    origin: crate::issues::crud::IssueAuthorship,
) -> Result<ProcessEventOutcome, String> {
    if let Some(push) = branch_push(event_type, payload) {
        // Like minting, the relay's obligation ends at handing the push off:
        // the checkout pull and store sync run detached, so a slow fetch can
        // never hold the relay cursor.
        let orch = orch.clone();
        tokio::spawn(async move {
            match handle_branch_push(&orch, &push).await {
                Ok(outcome) => log::debug!(
                    "GitHub relay event {event_id}: push of {} to {} at {}: {outcome:?}",
                    push.git_ref,
                    push.repository,
                    push.after
                ),
                Err(error) => log::warn!(
                    "GitHub relay event {event_id}: push of {} to {} failed: {error}",
                    push.git_ref,
                    push.repository
                ),
            }
        });
        return Ok(ProcessEventOutcome::BranchPush);
    }
    let TriggerDecision::Comment(comment) = trigger_for_event(event_type, payload, app_slug) else {
        log::debug!(
            "Ignoring GitHub relay event {event_id}: type={event_type}, action={}",
            payload["action"].as_str().unwrap_or("<none>")
        );
        return Ok(ProcessEventOutcome::Ignored);
    };
    let Some(project) = find_project_by_remote_full_name(
        &orch.db.local,
        orch.services.git.as_ref(),
        &comment.repository,
    )
    .await
    .map_err(|error| error.to_string())?
    else {
        let now = chrono::Utc::now().to_rfc3339();
        log::info!(
            "Dropping GitHub app mention from relay event {event_id}: type={event_type}, action={}, repository={} maps to no Cairn project",
            payload["action"].as_str().unwrap_or("<none>"),
            comment.repository
        );
        credentials::update_github_credentials(&orch.db.local, |creds| {
            creds.relay_unmapped_mention_count =
                creds.relay_unmapped_mention_count.saturating_add(1);
            creds.relay_last_unmapped_event_id = Some(event_id.to_string());
            creds.relay_last_unmapped_repository = Some(comment.repository.clone());
            creds.relay_last_unmapped_at = Some(now);
        })
        .await?;
        return Ok(ProcessEventOutcome::UnmappedMention);
    };
    // Minting is the relay's whole obligation: routes run later, from the
    // fact log, so a route that fails or hangs cannot stall the relay cursor.
    // A failed insert still propagates and the event is retried; the source's
    // dedupe absorbs the retry.
    crate::routes::mint(
        orch,
        crate::routes::Mint {
            fact: comment_fact(&comment, &project.key, origin),
            record_ref: comment.url.clone(),
            record_id: Some(comment.comment_id.to_string()),
            project_id: Some(&project.id),
            presence: crate::routes::log::operator_presence().await,
            actor_job_id: None,
        },
    )
    .await?;
    Ok(ProcessEventOutcome::Routed)
}

/// Handle a GitHub App webhook the relay delivered. `opened` is its opened
/// payload (`Ok(None)` for the relay's plaintext skip marker), and `app_slug`
/// is `None` when no App is connected on this install.
pub(crate) async fn handle_github_event(
    orch: &Orchestrator,
    event: &RelayEvent,
    opened: Result<Option<Value>, String>,
    app_slug: Option<&str>,
) -> EventOutcome {
    let (payload, decrypted) = match opened {
        Ok(Some(payload)) => (payload, true),
        Ok(None) => (event.payload.clone(), false),
        Err(error) => {
            log::warn!(
                "Skipping malformed GitHub relay event {}: {error}",
                event.id
            );
            return EventOutcome::Skipped {
                reason: "relay_event_malformed",
                detail: error,
            };
        }
    };
    let Some(app_slug) = app_slug else {
        if !decrypted {
            return EventOutcome::PassedOver;
        }
        log::warn!(
            "Skipping GitHub relay event {} ({}): no GitHub App is connected",
            event.id,
            event.event_type
        );
        return EventOutcome::Skipped {
            reason: "github_not_connected",
            detail: "no GitHub App is connected on this install".into(),
        };
    };
    let processed = match crate::routes::installation_machine_origin(orch) {
        Ok(origin) => {
            process_event(
                orch,
                event.id,
                &event.event_type,
                &payload,
                app_slug,
                origin,
            )
            .await
        }
        Err(error) => Err(error),
    };
    match processed {
        Ok(_) if decrypted => EventOutcome::Delivered,
        Ok(_) => EventOutcome::PassedOver,
        Err(error) => {
            log::warn!(
                "Skipping GitHub relay event {} ({}): {error}",
                event.id,
                event.event_type
            );
            EventOutcome::Skipped {
                reason: "relay_event_processing_failed",
                detail: error,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::migrated_test_db;
    use std::sync::Arc;

    async fn test_orchestrator(name: &str) -> (Orchestrator, Arc<crate::storage::LocalDb>) {
        use crate::db::DbState;
        use crate::services::testing::{RecordingProcessSpawner, TestServicesBuilder};
        use crate::storage::SearchIndex;

        let temp = tempfile::tempdir().unwrap().keep();
        let db = Arc::new(migrated_test_db(name).await);
        let search = Arc::new(SearchIndex::open_or_create(temp.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(
                TestServicesBuilder::new()
                    .with_process(RecordingProcessSpawner::new().clone())
                    .build(),
            ),
            temp.join("config"),
        )
        .build();
        (orch, db)
    }

    fn machine(orch: &Orchestrator) -> crate::issues::crud::IssueAuthorship {
        crate::routes::installation_machine_origin(orch).unwrap()
    }

    fn comment(author: &str, author_type: &str, body: &str) -> Value {
        json!({
            "action": "created",
            "repository": {"full_name": "cairn/cairn"},
            "issue": {"number": 42, "pull_request": {"url": "https://api.github.test/pulls/42"}},
            "comment": {"id": 99, "body": body, "html_url": "https://github.test/cairn/cairn/pull/42#issuecomment-99", "user": {"login": author, "type": author_type}}
        })
    }

    #[tokio::test]
    async fn process_event_reports_ignored_non_mentions() {
        let (orch, _) = test_orchestrator("relay-ignored-event.db").await;

        let outcome = process_event(
            &orch,
            100,
            "issue_comment",
            &comment("alice", "User", "hello"),
            "cairn-app",
            machine(&orch),
        )
        .await
        .unwrap();

        assert_eq!(outcome, ProcessEventOutcome::Ignored);
    }

    #[tokio::test]
    async fn process_event_records_mentions_for_unmapped_repositories() {
        let (orch, db) = test_orchestrator("relay-unmapped-mention.db").await;
        let mut payload = comment("alice", "User", "@cairn-app please investigate");
        payload["repository"]["full_name"] = json!("unknown/repository");

        let outcome = process_event(
            &orch,
            101,
            "issue_comment",
            &payload,
            "cairn-app",
            machine(&orch),
        )
        .await
        .unwrap();

        assert_eq!(outcome, ProcessEventOutcome::UnmappedMention);
        let credentials = credentials::get_github_credentials(&db).await.unwrap();
        assert_eq!(credentials.relay_unmapped_mention_count, 1);
        assert_eq!(
            credentials.relay_last_unmapped_event_id.as_deref(),
            Some("101")
        );
        assert_eq!(
            credentials.relay_last_unmapped_repository.as_deref(),
            Some("unknown/repository")
        );
        assert!(credentials.relay_last_unmapped_at.is_some());
    }

    fn push(git_ref: &str, repository: &str) -> Value {
        json!({
            "ref": git_ref,
            "after": "9e77a2750000000000000000000000000000000b",
            "deleted": false,
            "repository": {"full_name": repository, "default_branch": "main"}
        })
    }

    #[test]
    fn a_push_that_leaves_a_branch_at_a_new_commit_is_a_branch_push() {
        let push_event = branch_push("push", &push("refs/heads/main", "acme/repo")).unwrap();
        assert_eq!(push_event.after, "9e77a2750000000000000000000000000000000b");
        assert_eq!(push_event.repository, "acme/repo");
        assert_eq!(push_event.branch(), Some("main"));
        assert_eq!(
            branch_push("push", &push("refs/heads/agent/p-1-builder-0", "acme/repo"))
                .unwrap()
                .branch(),
            Some("agent/p-1-builder-0")
        );
        assert_eq!(
            branch_push("push", &push("refs/tags/v1", "acme/repo")),
            None
        );
        let mut deleted = push("refs/heads/main", "acme/repo");
        deleted["deleted"] = json!(true);
        assert_eq!(branch_push("push", &deleted), None);
        assert_eq!(
            branch_push("issue_comment", &push("refs/heads/main", "acme/repo")),
            None
        );
    }

    #[tokio::test]
    async fn a_default_branch_push_for_a_mapped_project_reaches_the_sync() {
        use crate::services::testing::MockGitClient;

        let db = Arc::new(migrated_test_db("relay-default-push.db").await);
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('proj', 'default', 'Project', 'PROJ', '/repos/proj', 'main', 1, 1)",
            (),
        )
        .await
        .unwrap();
        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .returning(|_| Ok("git@github.com:acme/repo.git".into()));
        // A dirty operator checkout is left alone; the store sync still runs.
        git.expect_status()
            .returning(|_| Ok(" M src/lib.rs".into()));
        git.expect_fetch_origin_branch()
            .returning(|_, _| Err("git fetch origin failed: offline".into()));
        let temp = tempfile::tempdir().unwrap().keep();
        let orch = Orchestrator::builder(
            Arc::new(crate::db::DbState::new(
                db.clone(),
                Arc::new(crate::storage::SearchIndex::open_or_create(temp.join("search")).unwrap()),
            )),
            Arc::new(
                crate::services::testing::TestServicesBuilder::new()
                    .with_git(git)
                    .build(),
            ),
            temp.join("config"),
        )
        .build();

        let mapped = branch_push("push", &push("refs/heads/main", "acme/repo")).unwrap();
        let outcome = handle_branch_push(&orch, &mapped).await.unwrap();
        assert!(matches!(
            outcome,
            BranchPushOutcome::Default(crate::projects::remote::PushRemoteSyncOutcome::Pulled { ref project_id, .. }) if project_id == "proj"
        ));
        let state = orch.base_branch_sync.snapshot("proj", "main").unwrap();
        assert_eq!(
            state.remote_tip.as_deref(),
            Some("9e77a2750000000000000000000000000000000b")
        );
        assert_eq!(
            state.remote_tip_source,
            Some(crate::orchestrator::base_branch_sync::Source::Push)
        );

        let unmapped = branch_push("push", &push("refs/heads/main", "other/repo")).unwrap();
        assert_eq!(
            handle_branch_push(&orch, &unmapped).await.unwrap(),
            BranchPushOutcome::Default(
                crate::projects::remote::PushRemoteSyncOutcome::NoMatchingProject
            )
        );

        // An agent branch nothing is cut from is the store's own business.
        let untracked = branch_push("push", &push("refs/heads/agent/solo", "acme/repo")).unwrap();
        assert_eq!(
            handle_branch_push(&orch, &untracked).await.unwrap(),
            BranchPushOutcome::Untracked
        );
        assert_eq!(orch.base_branch_sync.snapshot("proj", "agent/solo"), None);

        // A parent's integration branch with a live child is tracked: the
        // push reaches the sync for that branch.
        db.execute(
            "INSERT INTO jobs (id, project_id, status, branch, base_branch, created_at, updated_at)
             VALUES ('child', 'proj', 'running', 'agent/child', 'agent/parent', 1, 1)",
            (),
        )
        .await
        .unwrap();
        let parent = branch_push("push", &push("refs/heads/agent/parent", "acme/repo")).unwrap();
        assert_eq!(
            handle_branch_push(&orch, &parent).await.unwrap(),
            BranchPushOutcome::Observed {
                project_id: "proj".into()
            }
        );
        assert_eq!(
            orch.base_branch_sync
                .snapshot("proj", "agent/parent")
                .unwrap()
                .remote_tip
                .as_deref(),
            Some("9e77a2750000000000000000000000000000000b")
        );
    }

    /// A default-branch push through the App's endpoint reaches the
    /// base-branch sync, and its row says so without keeping the body.
    #[tokio::test]
    async fn a_default_branch_push_through_the_app_endpoint_reaches_the_sync() {
        use crate::services::testing::MockGitClient;

        let temp = tempfile::tempdir().unwrap();
        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .returning(|_| Ok("git@github.com:acme/repo.git".into()));
        git.expect_status()
            .returning(|_| Ok(" M src/lib.rs".into()));
        git.expect_fetch_origin_branch()
            .returning(|_, _| Err("git fetch origin failed: offline".into()));
        let (orch, db) = crate::routes::fixture::orchestrator_with(
            temp.path(),
            &[],
            crate::services::testing::TestServicesBuilder::new().with_git(git),
        )
        .await;
        db.execute(
            "UPDATE projects SET default_branch = 'main' WHERE id = 'p'",
            (),
        )
        .await
        .unwrap();
        crate::ingress::consume::tests::github_app_endpoint(&db).await;

        let consumed = crate::ingress::consume::consume(
            &orch,
            crate::ingress::consume::tests::app_event(
                60,
                "push",
                push("refs/heads/main", "acme/repo"),
            ),
            Some("cairn-app"),
        )
        .await;

        assert!(
            matches!(
                consumed,
                crate::ingress::consume::Consumed::Delivered { .. }
            ),
            "{consumed:?}"
        );
        let row = crate::ingress::recent_events(&db, "src_app", 1)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(row.note.as_deref(), Some("github:push"));
        assert_eq!(row.body, None);
        let observed = async {
            loop {
                if let Some(state) = orch.base_branch_sync.snapshot("p", "main") {
                    return state;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };
        let state = tokio::time::timeout(std::time::Duration::from_secs(10), observed)
            .await
            .expect("the push reached the base-branch sync");
        assert_eq!(
            state.remote_tip.as_deref(),
            Some("9e77a2750000000000000000000000000000000b")
        );
    }

    #[tokio::test]
    async fn process_event_hands_a_default_branch_push_off_and_still_routes_mentions() {
        let (orch, _) = test_orchestrator("relay-push-outcome.db").await;
        let outcome = process_event(
            &orch,
            102,
            "push",
            &push("refs/heads/main", "acme/repo"),
            "cairn-app",
            machine(&orch),
        )
        .await
        .unwrap();
        assert_eq!(outcome, ProcessEventOutcome::BranchPush);
        let outcome = process_event(
            &orch,
            103,
            "push",
            &push("refs/tags/v1", "acme/repo"),
            "cairn-app",
            machine(&orch),
        )
        .await
        .unwrap();
        assert_eq!(outcome, ProcessEventOutcome::Ignored);
    }

    #[test]
    fn maps_an_explicit_app_mention_with_actionable_context() {
        let TriggerDecision::Comment(trigger) = trigger_for_event(
            "issue_comment",
            &comment("alice", "User", "@cairn-app please investigate"),
            "cairn-app",
        ) else {
            panic!("expected comment trigger")
        };
        assert_eq!(trigger.repository, "cairn/cairn");
        assert_eq!(trigger.number, 42);
        assert_eq!(trigger.kind, "pull_request");
        assert_eq!(trigger.author, "alice");
        assert_eq!(trigger.comment_id, 99);
    }

    #[test]
    fn ignores_non_mentions_and_bot_authored_comments() {
        assert_eq!(
            trigger_for_event(
                "issue_comment",
                &comment("alice", "User", "hello"),
                "cairn-app"
            ),
            TriggerDecision::Ignore
        );
        assert_eq!(
            trigger_for_event(
                "issue_comment",
                &comment("dependabot[bot]", "Bot", "@cairn-app hello"),
                "cairn-app"
            ),
            TriggerDecision::Ignore
        );
        assert_eq!(
            trigger_for_event(
                "issue_comment",
                &comment("cairn-app[bot]", "User", "@cairn-app hello"),
                "cairn-app"
            ),
            TriggerDecision::Ignore
        );
    }

    #[test]
    fn the_relay_skip_marker_is_not_a_mention() {
        assert_eq!(
            trigger_for_event(
                "cairn.relay_skipped",
                &json!({ "error": "payload_exceeds_hard_ceiling" }),
                "cairn-app"
            ),
            TriggerDecision::Ignore
        );
    }
}
