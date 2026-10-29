use std::collections::{HashMap, HashSet};

use cairn_db::turso::{params, Row};
use sha2::{Digest, Sha256};

use crate::execution::check_parsers::{VerdictFacts, VERDICT_FACTS_SQL};
use crate::storage::{DbResult, LocalDb, RowExt};

pub const CARD_RENDER_VERSION: u8 = 1;

pub use cairn_channels::state_card::{
    AgentCardState, AgentPhase, CheckState, CheckVerdict, PullRequestState, TodoProgress,
};

fn card_check_verdict(passed: bool, parsed: Option<&VerdictFacts<'_>>) -> CheckVerdict {
    match crate::execution::checks_status::check_state_from_facts(passed, parsed) {
        crate::execution::checks_status::NodeCheckState::Passed => CheckVerdict::Passed,
        crate::execution::checks_status::NodeCheckState::Unverified => CheckVerdict::Unverified,
        _ => CheckVerdict::Failed,
    }
}

pub fn fingerprint(state: &AgentCardState) -> Result<String, serde_json::Error> {
    let mut digest = Sha256::new();
    digest.update([CARD_RENDER_VERSION]);
    digest.update(serde_json::to_vec(state)?);
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(test)]
thread_local! {
    /// Job ids this thread derived card state for, in order.
    static DERIVED_JOBS: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Derive a card from current project-database truth without touching a worktree or VCS.
pub async fn derive_agent_state(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<AgentCardState>, String> {
    #[cfg(test)]
    DERIVED_JOBS.with(|jobs| jobs.borrow_mut().push(job_id.to_string()));
    let job_id = job_id.to_string();
    let checks_sql = format!("SELECT check_name, passed, {VERDICT_FACTS_SQL} FROM check_result_cache c WHERE job_id=?1 AND NOT EXISTS (SELECT 1 FROM check_result_cache n WHERE n.job_id=c.job_id AND n.check_name=c.check_name AND (n.ran_at>c.ran_at OR (n.ran_at=c.ran_at AND n.tree_hash>c.tree_hash) OR (n.ran_at=c.ran_at AND n.tree_hash=c.tree_hash AND n.input_hash>c.input_hash))) ORDER BY check_name");
    db.read(|conn| Box::pin(async move {
        let Some(base) = conn.query("SELECT node_name, status, started_at, completed_at,
             (SELECT COUNT(*) FROM todos WHERE job_id=j.id),
             (SELECT COUNT(*) FROM todos WHERE job_id=j.id AND status='completed'),
             (SELECT active_form FROM todos WHERE job_id=j.id AND status='in_progress' ORDER BY position LIMIT 1),
             (EXISTS(SELECT 1 FROM prompts WHERE job_id=j.id AND response IS NULL AND withdrawn_at IS NULL)
              OR EXISTS(SELECT 1 FROM permission_requests pr JOIN runs r ON r.id=pr.run_id WHERE r.job_id=j.id AND pr.status='pending'))
             FROM jobs j WHERE j.id=?1 AND j.parent_job_id IS NULL AND j.status <> 'pending' AND j.uri_segment IS NOT NULL",
             params![job_id.as_str()]).await?.next().await? else { return Ok(None); };
        let subject_uri = crate::jobs::queries::home_uri_for_job_conn(conn, &job_id).await?
            .ok_or_else(|| crate::storage::DbError::Row("cardable job has no home URI".into()))?;
        let status = base.text(1)?;
        let waiting = base.i64(7)? != 0;
        let phase = match status.as_str() {
            "completed" | "complete" | "succeeded" => AgentPhase::Succeeded,
            "failed" => AgentPhase::Failed,
            "stopped" | "cancelled" | "canceled" => AgentPhase::Stopped,
            _ if waiting => AgentPhase::WaitingForUser,
            _ => AgentPhase::Running,
        };
        let pull_request = conn.query("SELECT github_pr_number, COALESCE(github_state,status), COALESCE(additions,0), COALESCE(deletions,0), github_review FROM merge_requests WHERE job_id=?1 ORDER BY updated_at DESC LIMIT 1", params![job_id.as_str()]).await?.next().await?.map(|r| -> DbResult<_> { Ok(PullRequestState { number:r.opt_i64(0)?.unwrap_or(0), status:r.text(1)?, additions:r.i64(2)?, deletions:r.i64(3)?, review_status:r.opt_text(4)? }) }).transpose()?;
        let mut rows = conn.query(&checks_sql, params![job_id.as_str()]).await?;
        let mut checks = Vec::new();
        while let Some(row) = rows.next().await? {
            let facts = row
                .opt_text(2)?
                .and_then(|projection| VerdictFacts::from_projection(&projection));
            let verdict = card_check_verdict(row.i64(1)? != 0, facts.as_ref());
            checks.push(CheckState { name: row.text(0)?, verdict });
        }
        Ok(Some(AgentCardState { subject_uri, agent_name: base.opt_text(0)?.unwrap_or_else(|| "agent".into()), phase, started_at: base.opt_i64(2)?.unwrap_or(0), completed_at: base.opt_i64(3)?, active_todo: base.opt_text(6)?, todos: TodoProgress { total: base.i64(4)? as u32, completed: base.i64(5)? as u32 }, pull_request, checks }))
    })).await.map_err(|e| e.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewStateCard<'a> {
    pub provider: &'a str,
    pub conversation: &'a str,
    pub subject_uri: &'a str,
    pub target_uri: &'a str,
    pub project_key: &'a str,
    pub job_id: &'a str,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateCardRecord {
    pub provider: String,
    pub conversation: String,
    pub subject_uri: String,
    pub target_uri: String,
    pub project_key: String,
    pub job_id: String,
    pub provider_guid: Option<String>,
    pub pinned: bool,
    pub pin_attempted_at: Option<i64>,
    pub pin_error: Option<String>,
    pub desired_fingerprint: Option<String>,
    pub rendered_fingerprint: Option<String>,
    pub status: String,
    pub attempts: i64,
    pub next_attempt_at: i64,
    pub last_error: Option<String>,
    pub last_edit_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

const COLUMNS: &str = "provider,conversation,subject_uri,target_uri,project_key,job_id,provider_guid,pinned,pin_attempted_at,pin_error,desired_fingerprint,rendered_fingerprint,status,attempts,next_attempt_at,last_error,last_edit_at,created_at,updated_at";
impl StateCardRecord {
    fn from_row(r: &Row) -> DbResult<Self> {
        Ok(Self {
            provider: r.text(0)?,
            conversation: r.text(1)?,
            subject_uri: r.text(2)?,
            target_uri: r.text(3)?,
            project_key: r.text(4)?,
            job_id: r.text(5)?,
            provider_guid: r.opt_text(6)?,
            pinned: r.i64(7)? != 0,
            pin_attempted_at: r.opt_i64(8)?,
            pin_error: r.opt_text(9)?,
            desired_fingerprint: r.opt_text(10)?,
            rendered_fingerprint: r.opt_text(11)?,
            status: r.text(12)?,
            attempts: r.i64(13)?,
            next_attempt_at: r.i64(14)?,
            last_error: r.opt_text(15)?,
            last_edit_at: r.opt_i64(16)?,
            created_at: r.i64(17)?,
            updated_at: r.i64(18)?,
        })
    }
}

pub async fn ensure(db: &LocalDb, card: &NewStateCard<'_>) -> Result<bool, String> {
    let conversation = super::bindings::canonical_conversation(card.provider, card.conversation)?;
    db.execute("INSERT OR IGNORE INTO channel_state_card(provider,conversation,subject_uri,target_uri,project_key,job_id,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?7)",params![card.provider,conversation,card.subject_uri,card.target_uri,card.project_key,card.job_id,card.created_at]).await.map(|n|n==1).map_err(|e|e.to_string())
}
pub async fn list_due(
    db: &LocalDb,
    provider: &str,
    now: i64,
    limit: i64,
) -> Result<Vec<StateCardRecord>, String> {
    db.query_all(format!("SELECT {COLUMNS} FROM channel_state_card WHERE provider=?1 AND status IN ('pending','live') AND next_attempt_at<=?2 ORDER BY updated_at LIMIT ?3"),params![provider,now,limit],StateCardRecord::from_row).await.map_err(|e|e.to_string())
}
pub async fn set_desired(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    subject_uri: &str,
    value: &str,
    now: i64,
) -> Result<bool, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    db.execute("UPDATE channel_state_card SET desired_fingerprint=?4,updated_at=?5 WHERE provider=?1 AND conversation=?2 AND subject_uri=?3",params![provider,conversation,subject_uri,value,now]).await.map(|n|n==1).map_err(|e|e.to_string())
}
pub async fn mark_posted(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    subject_uri: &str,
    guid: &str,
    now: i64,
) -> Result<bool, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    db.execute("UPDATE channel_state_card SET provider_guid=?4,status='live',attempts=0,next_attempt_at=0,last_error=NULL,updated_at=?5 WHERE provider=?1 AND conversation=?2 AND subject_uri=?3",params![provider,conversation,subject_uri,guid,now]).await.map(|n|n==1).map_err(|e|e.to_string())
}
pub async fn mark_rendered(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    subject_uri: &str,
    fingerprint: &str,
    settled: bool,
    now: i64,
) -> Result<bool, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    db.execute("UPDATE channel_state_card SET rendered_fingerprint=?4,status=CASE WHEN ?5 THEN 'settled' ELSE 'live' END,attempts=0,next_attempt_at=0,last_error=NULL,last_edit_at=?6,updated_at=?6 WHERE provider=?1 AND conversation=?2 AND subject_uri=?3",params![provider,conversation,subject_uri,fingerprint,settled as i64,now]).await.map(|n|n==1).map_err(|e|e.to_string())
}
pub async fn mark_pin_attempt(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    subject_uri: &str,
    error: Option<&str>,
    now: i64,
) -> Result<bool, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    db.execute("UPDATE channel_state_card SET pinned=CASE WHEN ?4 IS NULL THEN 1 ELSE 0 END,pin_attempted_at=?5,pin_error=?4,updated_at=?5 WHERE provider=?1 AND conversation=?2 AND subject_uri=?3",params![provider,conversation,subject_uri,error,now]).await.map(|n|n==1).map_err(|e|e.to_string())
}

/// One conversation's unrecovered pin failure, for operator-facing health.
pub struct PinFailure {
    pub conversation: String,
    pub target_uri: String,
    pub error: String,
}

/// The provider's most recent pin failure that no later successful pin in the
/// same conversation has recovered, ignoring attempts older than `since`.
///
/// The window matters because a card is pinned at most once: without it, a
/// permission gap the operator had already fixed would describe the bridge
/// forever whenever the affected threads had no further cards to pin.
pub async fn latest_pin_failure(
    db: &LocalDb,
    provider: &str,
    since: i64,
) -> Result<Option<PinFailure>, String> {
    db.query_opt(
        "SELECT failed.conversation,failed.target_uri,failed.pin_error
         FROM channel_state_card failed
         WHERE failed.provider=?1 AND failed.pin_error IS NOT NULL
           AND failed.pin_attempted_at>=?2
           AND NOT EXISTS (
             SELECT 1 FROM channel_state_card recovered
             WHERE recovered.provider=failed.provider
               AND recovered.conversation=failed.conversation
               AND recovered.pinned=1
               AND recovered.pin_attempted_at>failed.pin_attempted_at
           )
         ORDER BY failed.pin_attempted_at DESC,failed.updated_at DESC LIMIT 1",
        params![provider, since],
        |row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?)),
    )
    .await
    .map(|row| {
        row.and_then(|(conversation, target_uri, error)| {
            error.map(|error| PinFailure {
                conversation,
                target_uri,
                error,
            })
        })
    })
    .map_err(|error| error.to_string())
}
pub async fn mark_failure(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    subject_uri: &str,
    error: &str,
    next_attempt_at: i64,
    now: i64,
) -> Result<bool, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    db.execute("UPDATE channel_state_card SET attempts=attempts+1,status=CASE WHEN attempts+1>=?6 THEN 'failed' ELSE status END,next_attempt_at=?5,last_error=?4,updated_at=?7 WHERE provider=?1 AND conversation=?2 AND subject_uri=?3",params![provider,conversation,subject_uri,error,next_attempt_at,MAX_ATTEMPTS,now]).await.map(|n|n==1).map_err(|e|e.to_string())
}

/// The jobs that already have a card, keyed by conversation.
async fn carded_jobs(
    db: &LocalDb,
    provider: &str,
) -> Result<HashMap<String, HashSet<String>>, String> {
    let rows = db
        .query_all(
            "SELECT conversation, job_id FROM channel_state_card WHERE provider=?1",
            params![provider],
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut carded: HashMap<String, HashSet<String>> = HashMap::new();
    for (conversation, job_id) in rows {
        carded.entry(conversation).or_default().insert(job_id);
    }
    Ok(carded)
}

const RECONCILE_BATCH: i64 = 50;
/// A card's budget for posting and editing. Pinning is deliberately outside it:
/// a pin is decoration, and a card that cannot be pinned must still be able to
/// spend every one of these attempts keeping its content current.
const MAX_ATTEMPTS: i64 = 8;

/// Converges Discord cards for one project database. The local database owns
/// bindings and retry state; the project database remains the source of agent truth.
pub async fn reconcile_discord_cards(
    project_db: &LocalDb,
    ledger_db: &LocalDb,
    provider: &dyn super::ChannelProvider,
    now: i64,
) -> Result<(), String> {
    // Discovery only creates cards, and `ensure` ignores one that exists, so a
    // job that already has a card in a conversation has nothing to discover
    // there; keeping its card current is the due pass's work.
    let carded = carded_jobs(ledger_db, "discord").await?;
    for binding in super::ledger::list_active_bindings(ledger_db)
        .await?
        .into_iter()
        .filter(|binding| binding.provider == "discord")
    {
        let Some((project_key, issue_number)) = issue_target(&binding.target_uri) else {
            continue;
        };
        let job_ids = project_db
            .query_all(
                "SELECT j.id FROM jobs j
                 JOIN issues i ON i.id=j.issue_id
                 JOIN projects p ON p.id=i.project_id
                 WHERE i.number=?1 AND lower(p.key)=lower(?2)
                   AND j.parent_job_id IS NULL
                   AND j.status <> 'pending' AND j.uri_segment IS NOT NULL
                 ORDER BY j.created_at",
                params![issue_number, project_key],
                |row| row.text(0),
            )
            .await
            .map_err(|error| error.to_string())?;
        let already_carded = carded.get(&binding.conversation);
        for job_id in job_ids {
            if already_carded.is_some_and(|jobs| jobs.contains(&job_id)) {
                continue;
            }
            let Some(state) = derive_agent_state(project_db, &job_id).await? else {
                continue;
            };
            ensure(
                ledger_db,
                &NewStateCard {
                    provider: "discord",
                    conversation: &binding.conversation,
                    subject_uri: &state.subject_uri,
                    target_uri: &binding.target_uri,
                    project_key,
                    job_id: &job_id,
                    created_at: now,
                },
            )
            .await?;
        }
    }

    for record in list_due(ledger_db, "discord", now, RECONCILE_BATCH).await? {
        let Some(state) = derive_agent_state(project_db, &record.job_id).await? else {
            continue;
        };
        let desired = fingerprint(&state).map_err(|error| error.to_string())?;
        set_desired(
            ledger_db,
            "discord",
            &record.conversation,
            &record.subject_uri,
            &desired,
            now,
        )
        .await?;
        if let Err(error) = reconcile_one(ledger_db, provider, &record, state, &desired, now).await
        {
            let delay = 5_i64.saturating_mul(1_i64 << record.attempts.min(6));
            mark_failure(
                ledger_db,
                "discord",
                &record.conversation,
                &record.subject_uri,
                &error,
                now.saturating_add(delay),
                now,
            )
            .await?;
        }
    }
    Ok(())
}

async fn reconcile_one(
    db: &LocalDb,
    provider: &dyn super::ChannelProvider,
    record: &StateCardRecord,
    state: AgentCardState,
    desired: &str,
    now: i64,
) -> Result<(), String> {
    let settled = matches!(
        state.phase,
        AgentPhase::Succeeded | AgentPhase::Failed | AgentPhase::Stopped
    );
    // Converged means the content matches and the one-time pin question has
    // been answered — not that the pin succeeded. A card whose pin failed is
    // as finished as one whose pin worked; only a card that has never been
    // attempted still has provider work waiting for it here.
    if record.rendered_fingerprint.as_deref() == Some(desired) && record.pin_attempted_at.is_some()
    {
        if settled && record.status != "settled" {
            mark_rendered(
                db,
                "discord",
                &record.conversation,
                &record.subject_uri,
                desired,
                true,
                now,
            )
            .await?;
        }
        return Ok(());
    }

    let message = super::StateCardMessage {
        conversation: record.conversation.clone(),
        state,
    };
    let (guid, rendered_by_post) = match record.provider_guid.clone() {
        Some(guid) => (guid, false),
        None => match provider
            .find_state_card(&record.conversation, &record.subject_uri)
            .await?
        {
            Some(found) => (found.provider_guid, false),
            None => (
                provider.post_state_card(&message).await?.provider_guid,
                true,
            ),
        },
    };
    if record.provider_guid.is_none() {
        mark_posted(
            db,
            "discord",
            &record.conversation,
            &record.subject_uri,
            &guid,
            now,
        )
        .await?;
    }
    if record.rendered_fingerprint.as_deref() != Some(desired) {
        if !rendered_by_post {
            provider.edit_state_card(&guid, &message).await?;
        }
        mark_rendered(
            db,
            "discord",
            &record.conversation,
            &record.subject_uri,
            desired,
            settled,
            now,
        )
        .await?;
    }
    if record.pin_attempted_at.is_none() {
        let pin_error = provider
            .pin_state_card(&record.conversation, &guid)
            .await
            .err();
        mark_pin_attempt(
            db,
            "discord",
            &record.conversation,
            &record.subject_uri,
            pin_error.as_deref(),
            now,
        )
        .await?;
    }
    Ok(())
}

fn issue_target(target: &str) -> Option<(&str, i64)> {
    let mut parts = target.strip_prefix("cairn://p/")?.split('/');
    let project = parts.next()?;
    let issue = parts.next()?.parse().ok()?;
    parts.next().is_none().then_some((project, issue))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn card_does_not_launder_evidence_free_successes_into_passes() {
        assert_eq!(card_check_verdict(true, None), CheckVerdict::Unverified);

        let empty = crate::execution::check_parsers::ParsedCheckResult {
            schema_version: 1,
            complete: true,
            selection: "empty".into(),
            tests: vec![],
            undeclared_skips: 0,
            parser: "nextest".into(),
            passed: 0,
            failed: 0,
            suite_failures: 0,
            deferred: 0,
            report: None,
            skipped: 0,
            failures: vec![],
        };
        assert_eq!(
            card_check_verdict(true, Some(&empty.verdict_facts())),
            CheckVerdict::Unverified
        );
    }

    #[derive(Default)]
    struct RecordingProvider {
        posts: Mutex<usize>,
        edits: Mutex<usize>,
        pins: Mutex<usize>,
        recover: bool,
        fail_edit: Mutex<bool>,
        fail_pin: Mutex<bool>,
    }

    #[async_trait::async_trait]
    impl super::super::ChannelProvider for RecordingProvider {
        fn capabilities(&self) -> super::super::ChannelCapabilities {
            super::super::ChannelCapabilities::default()
        }

        async fn send(
            &self,
            _message: &super::super::OutboundMessage,
        ) -> Result<super::super::SentIds, String> {
            unreachable!("state-card reconciliation never appends through send")
        }

        fn subscribe(&self) -> tokio::sync::mpsc::Receiver<super::super::InboundEvent> {
            tokio::sync::mpsc::channel(1).1
        }

        fn health(&self) -> super::super::ChannelHealth {
            super::super::ChannelHealth::Ready
        }

        async fn post_state_card(
            &self,
            _message: &super::super::StateCardMessage,
        ) -> Result<super::super::PostedCard, String> {
            *self.posts.lock().unwrap() += 1;
            Ok(super::super::PostedCard {
                provider_guid: "discord:1/2/99".into(),
            })
        }

        async fn edit_state_card(
            &self,
            _provider_guid: &str,
            _message: &super::super::StateCardMessage,
        ) -> Result<(), String> {
            if *self.fail_edit.lock().unwrap() {
                return Err("rate limited".into());
            }
            *self.edits.lock().unwrap() += 1;
            Ok(())
        }

        async fn find_state_card(
            &self,
            _conversation: &str,
            _marker: &str,
        ) -> Result<Option<super::super::PostedCard>, String> {
            Ok(self.recover.then(|| super::super::PostedCard {
                provider_guid: "discord:1/2/98".into(),
            }))
        }

        async fn pin_state_card(&self, _conversation: &str, _guid: &str) -> Result<(), String> {
            *self.pins.lock().unwrap() += 1;
            if *self.fail_pin.lock().unwrap() {
                Err("could not pin Discord message: Missing Permissions".into())
            } else {
                Ok(())
            }
        }
    }

    async fn seeded_reconcile_db(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','Workspace',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','Cairn','cairn','/tmp/cairn',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i','p',7,'Card','active',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('e','build','i','p','running',1,1);
             INSERT INTO jobs(id,execution_id,issue_id,project_id,status,uri_segment,node_name,started_at,created_at,updated_at) VALUES('j','e','i','p','running','builder','Builder',1,1,1);
             INSERT INTO channel_conversation_binding(provider,conversation,target_uri,binding_kind,message_classes,followed_at,selected_at) VALUES('discord','discord:1/2','cairn://p/cairn/7','structural',7,1,1);",
        )
        .await
        .unwrap();
        db
    }

    #[test]
    fn fingerprint_is_stable_and_sensitive() {
        let mut state = AgentCardState {
            subject_uri: "cairn://p/c/1/1/builder".into(),
            agent_name: "builder".into(),
            phase: AgentPhase::Running,
            started_at: 1,
            completed_at: None,
            active_todo: None,
            todos: TodoProgress {
                completed: 0,
                total: 0,
            },
            pull_request: None,
            checks: vec![],
        };
        let first = fingerprint(&state).unwrap();
        assert_eq!(first, fingerprint(&state).unwrap());
        assert_eq!(first.len(), 64);
        state.todos.total = 1;
        assert_ne!(first, fingerprint(&state).unwrap());
    }
    #[tokio::test]
    async fn production_reconcile_posts_pins_and_then_edits_one_card() {
        let db = seeded_reconcile_db("state-card-production.db").await;
        let provider = RecordingProvider::default();

        reconcile_discord_cards(&db, &db, &provider, 10)
            .await
            .unwrap();
        assert_eq!(*provider.posts.lock().unwrap(), 1);
        assert_eq!(*provider.edits.lock().unwrap(), 0);
        assert_eq!(*provider.pins.lock().unwrap(), 1);

        db.execute(
            "UPDATE jobs SET status='complete', completed_at=20 WHERE id='j'",
            (),
        )
        .await
        .unwrap();
        reconcile_discord_cards(&db, &db, &provider, 20)
            .await
            .unwrap();
        assert_eq!(*provider.posts.lock().unwrap(), 1);
        assert_eq!(*provider.edits.lock().unwrap(), 1);
        let card = list_due(&db, "discord", 20, 10).await.unwrap();
        assert!(
            card.is_empty(),
            "terminal cards settle out of the due queue"
        );
    }

    #[tokio::test]
    async fn production_reconcile_recovers_by_marker_and_records_edit_failure() {
        let db = seeded_reconcile_db("state-card-recovery.db").await;
        let provider = RecordingProvider {
            recover: true,
            ..Default::default()
        };
        *provider.fail_edit.lock().unwrap() = true;

        reconcile_discord_cards(&db, &db, &provider, 10)
            .await
            .unwrap();
        assert_eq!(*provider.posts.lock().unwrap(), 0);
        assert_eq!(*provider.pins.lock().unwrap(), 0);
        let card = list_due(&db, "discord", 20, 10).await.unwrap().remove(0);
        assert_eq!(card.provider_guid.as_deref(), Some("discord:1/2/98"));
        assert_eq!(card.attempts, 1);
        assert_eq!(card.last_error.as_deref(), Some("rate limited"));
        assert!(card.next_attempt_at > 10);
    }

    #[tokio::test]
    async fn pin_failure_does_not_fail_or_retry_a_settled_card() {
        let db = seeded_reconcile_db("state-card-pin-failure.db").await;
        db.execute(
            "UPDATE jobs SET status='complete', completed_at=9 WHERE id='j'",
            (),
        )
        .await
        .unwrap();
        let provider = RecordingProvider::default();
        *provider.fail_pin.lock().unwrap() = true;

        reconcile_discord_cards(&db, &db, &provider, 10)
            .await
            .unwrap();

        let card = db
            .query_one(
                format!("SELECT {COLUMNS} FROM channel_state_card"),
                (),
                StateCardRecord::from_row,
            )
            .await
            .unwrap();
        assert_eq!(card.status, "settled");
        assert!(card.rendered_fingerprint.is_some());
        assert!(!card.pinned);
        assert_eq!(card.pin_attempted_at, Some(10));
        assert_eq!(
            card.pin_error.as_deref(),
            Some("could not pin Discord message: Missing Permissions")
        );
        assert_eq!(card.attempts, 0, "pinning has no delivery retry budget");
        assert!(card.last_error.is_none());
        assert!(list_due(&db, "discord", 20, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_live_cards_failed_pin_is_never_reworked_or_retried() {
        let db = seeded_reconcile_db("state-card-pin-live.db").await;
        let provider = RecordingProvider::default();
        *provider.fail_pin.lock().unwrap() = true;

        reconcile_discord_cards(&db, &db, &provider, 10)
            .await
            .unwrap();
        assert_eq!(*provider.posts.lock().unwrap(), 1);
        assert_eq!(*provider.pins.lock().unwrap(), 1);

        // A live card is re-examined every sweep by design: its agent's state
        // can change under it. What must not survive the failed pin is provider
        // work — an unpinned card is converged once its content matches, or it
        // would re-walk the post/edit/pin path on every sweep for the rest of
        // the run.
        reconcile_discord_cards(&db, &db, &provider, 20)
            .await
            .unwrap();
        reconcile_discord_cards(&db, &db, &provider, 30)
            .await
            .unwrap();
        assert_eq!(*provider.posts.lock().unwrap(), 1);
        assert_eq!(*provider.edits.lock().unwrap(), 0);
        assert_eq!(
            *provider.pins.lock().unwrap(),
            1,
            "the single pin attempt is never retried"
        );

        let card = db
            .query_one(
                format!("SELECT {COLUMNS} FROM channel_state_card"),
                (),
                StateCardRecord::from_row,
            )
            .await
            .unwrap();
        assert_eq!(card.status, "live");
        assert_eq!(
            card.pin_attempted_at,
            Some(10),
            "the attempt keeps the date it was actually made"
        );
        assert_eq!(card.attempts, 0);

        // The whole point of the issue: the card is still live-edited, and still
        // settles, having never been pinned.
        db.execute(
            "UPDATE jobs SET status='complete', completed_at=40 WHERE id='j'",
            (),
        )
        .await
        .unwrap();
        reconcile_discord_cards(&db, &db, &provider, 50)
            .await
            .unwrap();
        assert_eq!(*provider.edits.lock().unwrap(), 1);
        assert_eq!(*provider.pins.lock().unwrap(), 1);
        assert!(
            list_due(&db, "discord", 60, 10).await.unwrap().is_empty(),
            "an unpinned card still leaves the due queue when its agent finishes"
        );
    }

    #[tokio::test]
    async fn a_pin_failure_is_reported_until_a_later_pin_in_that_channel_succeeds() {
        let db = crate::storage::migrated_test_db("state-card-pin-health.db").await;
        // A success that predates the failure, and a success in a different
        // channel, are both irrelevant to whether this channel is repaired.
        db.execute(
            "INSERT INTO channel_state_card(provider, conversation, subject_uri, target_uri,
                 project_key, job_id, pinned, pin_attempted_at, pin_error, status,
                 created_at, updated_at)
             VALUES
               ('discord','discord:1/42','s1','cairn://p/cairn/1','cairn','j1',
                1,100,NULL,'settled',100,100),
               ('discord','discord:1/42','s2','cairn://p/cairn/2','cairn','j2',
                0,500,'could not pin Discord message: Missing Permissions','settled',500,500),
               ('discord','discord:1/99','s4','cairn://p/cairn/4','cairn','j4',
                1,900,NULL,'settled',900,900)",
            (),
        )
        .await
        .unwrap();

        let failure = latest_pin_failure(&db, "discord", 0)
            .await
            .unwrap()
            .expect("an unrecovered failure describes the bridge");
        assert_eq!(failure.conversation, "discord:1/42");
        assert_eq!(failure.target_uri, "cairn://p/cairn/2");

        assert!(
            latest_pin_failure(&db, "discord", 600)
                .await
                .unwrap()
                .is_none(),
            "a failure older than the reporting window no longer describes the bridge"
        );

        db.execute(
            "INSERT INTO channel_state_card(provider, conversation, subject_uri, target_uri,
                 project_key, job_id, pinned, pin_attempted_at, pin_error, status,
                 created_at, updated_at)
             VALUES('discord','discord:1/42','s3','cairn://p/cairn/3','cairn','j3',
                    1,900,NULL,'settled',900,900)",
            (),
        )
        .await
        .unwrap();
        assert!(
            latest_pin_failure(&db, "discord", 0)
                .await
                .unwrap()
                .is_none(),
            "a later pin in the same channel proves the permission gap is closed"
        );
    }

    #[tokio::test]
    async fn ledger_canonicalizes_conversations() {
        let db = crate::storage::migrated_test_db("state-card-ledger.db").await;
        let mut card = NewStateCard {
            provider: "discord",
            conversation: "discord:1/2",
            subject_uri: "cairn://p/c/1/1/builder",
            target_uri: "cairn://p/c/1",
            project_key: "c",
            job_id: "j",
            created_at: 1,
        };
        assert!(ensure(&db, &card).await.unwrap());
        assert_eq!(
            list_due(&db, "discord", 1, 10).await.unwrap()[0].conversation,
            "discord:1/2"
        );
        card.conversation = "discord:123";
        assert!(ensure(&db, &card).await.is_err());
    }

    fn take_derived_jobs() -> Vec<String> {
        DERIVED_JOBS.with(|jobs| std::mem::take(&mut *jobs.borrow_mut()))
    }

    #[tokio::test]
    async fn discovery_derives_only_for_jobs_without_a_card() {
        let db = seeded_reconcile_db("state-card-discovery-skip.db").await;
        // Three more bound issues whose finished jobs already have cards, each
        // with a check row a derivation would have read.
        let mut script = String::new();
        for n in 8..11 {
            script.push_str(&format!(
                "INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i{n}','p',{n},'Card','active',1,1);
                 INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('e{n}','build','i{n}','p','completed',1,1);
                 INSERT INTO jobs(id,execution_id,issue_id,project_id,status,uri_segment,node_name,started_at,completed_at,created_at,updated_at) VALUES('j{n}','e{n}','i{n}','p','complete','builder','Builder',1,2,1,1);
                 INSERT INTO check_result_cache(project_id,tree_hash,input_hash,check_name,exit_code,passed,output_tail,duration_ms,ran_at,job_id,target_results_json) VALUES('p','t{n}','in{n}','typecheck',0,1,'',1,1,'j{n}','{{\"parser\":\"tsc\",\"passed\":0,\"failed\":0,\"skipped\":0,\"failures\":[]}}');
                 INSERT INTO channel_conversation_binding(provider,conversation,target_uri,binding_kind,message_classes,followed_at,selected_at) VALUES('discord','discord:1/{n}','cairn://p/cairn/{n}','structural',7,1,1);
                 INSERT INTO channel_state_card(provider,conversation,subject_uri,target_uri,project_key,job_id,status,created_at,updated_at) VALUES('discord','discord:1/{n}','cairn://p/cairn/{n}/1/builder','cairn://p/cairn/{n}','cairn','j{n}','settled',1,1);"
            ));
        }
        db.execute_script(&script).await.unwrap();
        let provider = RecordingProvider::default();
        take_derived_jobs();

        reconcile_discord_cards(&db, &db, &provider, 10)
            .await
            .unwrap();
        assert_eq!(
            take_derived_jobs(),
            ["j", "j"],
            "only the uncarded job is derived: once to create its card, once by the due pass that posts it"
        );
        let cards = db
            .query_all(
                "SELECT job_id FROM channel_state_card ORDER BY job_id",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(cards, ["j", "j10", "j8", "j9"]);
        assert_eq!(*provider.posts.lock().unwrap(), 1);

        reconcile_discord_cards(&db, &db, &provider, 20)
            .await
            .unwrap();
        assert_eq!(
            take_derived_jobs(),
            ["j"],
            "once carded, a live job is derived only by the due pass"
        );
    }

    /// The card reads verdict fields projected in SQL; the board reads the full
    /// parse. Both must classify every shape of result identically.
    #[tokio::test]
    async fn projected_verdict_facts_classify_like_the_full_parse() {
        use crate::execution::check_parsers::{CheckTestResult, ParsedCheckResult};
        use crate::execution::checks_status::{
            verdict_evidence, verdict_evidence_from_facts, UnverifiedReason, VerdictEvidence,
        };

        let result = |parser: &str, selection: &str, complete: bool, passed: usize, skipped| {
            let parsed = ParsedCheckResult {
                schema_version: 1,
                complete,
                selection: selection.into(),
                tests: (0..passed)
                    .map(|i| CheckTestResult {
                        id: format!("cairn-core::module::tests::test_number_{i}"),
                        status: "passed".into(),
                        duration_ms: Some(12),
                        attempts: 1,
                        retried: false,
                        flaky: false,
                        failure_message: None,
                        skip_reason: None,
                        skip_declaration: None,
                        skip_declaration_source: None,
                    })
                    .collect(),
                undeclared_skips: 0,
                parser: parser.into(),
                passed,
                failed: 0,
                suite_failures: 0,
                deferred: 0,
                report: None,
                skipped,
                failures: vec![],
            };
            Some(serde_json::to_string(&parsed).unwrap())
        };
        let raw = |json: &str| Some(json.to_string());
        let unverified = VerdictEvidence::Unverified;
        let cases = [
            (
                "a-verified",
                true,
                result("nextest", "full", true, 2_000, 0),
                VerdictEvidence::Verified,
            ),
            (
                "b-empty-selection",
                true,
                result("nextest", "empty", true, 0, 0),
                unverified(UnverifiedReason::EmptySelection),
            ),
            (
                "c-all-skipped",
                true,
                result("nextest", "full", true, 0, 4),
                unverified(UnverifiedReason::AllSkipped(4)),
            ),
            (
                "d-incomplete-enumeration",
                true,
                result("vitest", "full", false, 3, 0),
                unverified(UnverifiedReason::IncompleteEnumeration),
            ),
            (
                "e-no-tests-ran",
                true,
                result("nextest", "full", true, 0, 0),
                unverified(UnverifiedReason::NoTestsRan),
            ),
            (
                "f-incomplete-lint",
                true,
                result("rust-lint", "unknown", false, 0, 0),
                unverified(UnverifiedReason::IncompleteLint),
            ),
            (
                "g-typecheck",
                true,
                result("tsc", "unknown", false, 0, 0),
                VerdictEvidence::Verified,
            ),
            (
                "h-failed-exit",
                false,
                result("nextest", "full", true, 5, 0),
                VerdictEvidence::Failed,
            ),
            (
                "i-serde-defaults",
                true,
                raw(r#"{"parser":"nextest","passed":2,"failed":0,"skipped":0,"failures":[]}"#),
                unverified(UnverifiedReason::IncompleteEnumeration),
            ),
            (
                "j-missing-count",
                true,
                raw(r#"{"parser":"nextest","passed":2,"failed":0,"failures":[]}"#),
                unverified(UnverifiedReason::NoStructuredResult),
            ),
            (
                "k-malformed",
                true,
                raw("{not json"),
                unverified(UnverifiedReason::NoStructuredResult),
            ),
            (
                "l-absent",
                true,
                None,
                unverified(UnverifiedReason::NoStructuredResult),
            ),
        ];

        let db = seeded_reconcile_db("state-card-verdict-projection.db").await;
        for (name, passed, json, _) in &cases {
            db.execute(
                "INSERT INTO check_result_cache(project_id,tree_hash,input_hash,check_name,exit_code,passed,output_tail,duration_ms,ran_at,job_id,target_results_json) VALUES('p','tree',?1,?1,?2,?3,'',1,1,'j',?4)",
                params![*name, i64::from(!*passed), i64::from(*passed), json.clone()],
            )
            .await
            .unwrap();
        }

        for (name, passed, json, expected) in &cases {
            let full = json
                .as_deref()
                .and_then(|json| serde_json::from_str::<ParsedCheckResult>(json).ok());
            assert_eq!(
                verdict_evidence(*passed, full.as_ref()),
                *expected,
                "{name}: full parse"
            );
            let projection = db
                .query_one(
                    format!(
                        "SELECT {VERDICT_FACTS_SQL} FROM check_result_cache WHERE check_name=?1"
                    ),
                    params![*name],
                    |row| row.opt_text(0),
                )
                .await
                .unwrap();
            let facts = projection
                .as_deref()
                .and_then(VerdictFacts::from_projection);
            assert_eq!(
                verdict_evidence_from_facts(*passed, facts.as_ref()),
                *expected,
                "{name}: projection {projection:?}"
            );
        }

        let card = derive_agent_state(&db, "j").await.unwrap().unwrap();
        let card_checks: Vec<_> = card
            .checks
            .iter()
            .map(|check| (check.name.as_str(), check.verdict))
            .collect();
        let expected: Vec<_> = cases
            .iter()
            .map(|(name, _, _, evidence)| {
                let verdict = match evidence {
                    VerdictEvidence::Failed => CheckVerdict::Failed,
                    VerdictEvidence::Verified => CheckVerdict::Passed,
                    VerdictEvidence::Unverified(_) => CheckVerdict::Unverified,
                };
                (*name, verdict)
            })
            .collect();
        assert_eq!(card_checks, expected);
    }
}
