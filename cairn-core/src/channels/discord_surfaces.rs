//! Durable Discord container lifecycle.
//!
//! Domain mutations only need to ensure or update desired rows and wake a
//! reconciler. This module is the sole owner of remote container mutations.

use super::discord::DiscordApi;
use cairn_db::turso::{params, Row};
use std::sync::Arc;

use crate::storage::{LocalDb, RowExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SurfaceKind {
    ProjectCategory,
    ThreadChannel,
    UnmanagedHome,
    IssueThread,
}

/// Materialize the durable rows needed for an issue's first Discord-carried event.
///
/// Callers must invoke this only after message-class eligibility is established.
/// The returned issue row may still be pending; delivery must wait for its structural
/// binding instead of falling back to another Discord destination.
pub async fn ensure_issue_surface(
    db: &LocalDb,
    guild_id: u64,
    project_key: &str,
    issue_target: &str,
    parent_thread_target: Option<&str>,
    now: i64,
) -> Result<DiscordSurface, String> {
    let category = ensure_surface(
        db,
        guild_id,
        SurfaceKind::ProjectCategory,
        project_key,
        None,
        None,
        now,
    )
    .await?;
    let parent = match parent_thread_target {
        Some(target) => {
            ensure_surface(
                db,
                guild_id,
                SurfaceKind::ThreadChannel,
                project_key,
                Some(target),
                Some(category.id),
                now,
            )
            .await?
        }
        None => {
            ensure_surface(
                db,
                guild_id,
                SurfaceKind::UnmanagedHome,
                project_key,
                None,
                Some(category.id),
                now,
            )
            .await?
        }
    };
    ensure_surface(
        db,
        guild_id,
        SurfaceKind::IssueThread,
        project_key,
        Some(issue_target),
        Some(parent.id),
        now,
    )
    .await
}

pub async fn request_issue_lock(
    db: &LocalDb,
    guild_id: u64,
    target_uri: &str,
    now: i64,
) -> Result<bool, String> {
    let target = canonical_target(target_uri)?;
    db.execute(
        "UPDATE discord_surface SET desired_state = 'locked', next_attempt_at = 0, updated_at = ?3
         WHERE guild_id = ?1 AND surface_kind = 'issue_thread' AND target_uri = ?2
           AND desired_state = 'active'",
        params![guild_id.to_string(), target, now],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    };

    #[derive(Default)]
    struct FakeDiscordApi {
        created: Mutex<Vec<String>>,
        locks: Mutex<Vec<u64>>,
        seeds: AtomicUsize,
        fail_seed: AtomicBool,
        ambiguous_thread_create: AtomicBool,
        wakes: Mutex<Vec<u64>>,
        archived: AtomicBool,
    }

    #[async_trait]
    impl DiscordApi for FakeDiscordApi {
        async fn inspect_guild_permissions(
            &self,
            _guild_id: u64,
        ) -> Result<super::super::discord::DiscordGuildPermissions, String> {
            Ok(super::super::discord::DiscordGuildPermissions {
                manage_channels: true,
                manage_threads: true,
                send_messages_in_threads: true,
            })
        }

        async fn inspect_channel(
            &self,
            channel_id: u64,
        ) -> Result<super::super::discord::DiscordRemoteChannel, String> {
            Ok(super::super::discord::DiscordRemoteChannel {
                channel_id,
                parent_id: matches!(channel_id, 102 | 200).then_some(101),
                topic: None,
                archived: self.archived.load(Ordering::SeqCst),
                locked: false,
            })
        }
        async fn find_channel_by_marker(
            &self,
            _guild_id: u64,
            _marker: &str,
        ) -> Result<Option<super::super::discord::DiscordRemoteChannel>, String> {
            Ok(None)
        }
        async fn create_category(
            &self,
            _guild_id: u64,
            name: &str,
            _marker: &str,
        ) -> Result<u64, String> {
            self.created.lock().unwrap().push(name.into());
            Ok(100)
        }
        async fn create_text_channel(
            &self,
            _guild_id: u64,
            _parent_id: u64,
            name: &str,
            _marker: &str,
        ) -> Result<u64, String> {
            self.created.lock().unwrap().push(name.into());
            Ok(101)
        }
        async fn send_message(&self, _channel_id: u64, _body: &str) -> Result<u64, String> {
            self.seeds.fetch_add(1, Ordering::SeqCst);
            if self.fail_seed.swap(false, Ordering::SeqCst) {
                Err("seed failed".into())
            } else {
                Ok(200)
            }
        }
        async fn create_public_thread(
            &self,
            _channel_id: u64,
            _seed_message_id: u64,
            name: &str,
        ) -> Result<u64, String> {
            self.created.lock().unwrap().push(name.into());
            if self.ambiguous_thread_create.swap(false, Ordering::SeqCst) {
                Err("connection lost after create".into())
            } else {
                Ok(102)
            }
        }
        async fn edit_message(
            &self,
            _channel_id: u64,
            _message_id: u64,
            _body: &str,
        ) -> Result<(), String> {
            Ok(())
        }
        async fn set_thread_archived(
            &self,
            _channel_id: u64,
            _archived: bool,
        ) -> Result<(), String> {
            self.wakes.lock().unwrap().push(_channel_id);
            self.archived.store(false, Ordering::SeqCst);
            Ok(())
        }
        async fn lock_thread(&self, channel_id: u64) -> Result<(), String> {
            self.locks.lock().unwrap().push(channel_id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn duplicate_ensure_creates_one_remote_surface_and_lock_is_monotonic() {
        let db = Arc::new(crate::storage::migrated_test_db("discord-surface-lifecycle.db").await);
        let first = ensure_surface(
            &db,
            1,
            SurfaceKind::ProjectCategory,
            "cairn",
            None,
            None,
            10,
        )
        .await
        .unwrap();
        let duplicate = ensure_surface(
            &db,
            1,
            SurfaceKind::ProjectCategory,
            "cairn",
            None,
            None,
            11,
        )
        .await
        .unwrap();
        assert_eq!(first.id, duplicate.id);

        let api = Arc::new(FakeDiscordApi::default());
        let reconciler = DiscordSurfaceReconciler::new(db.clone(), api.clone());
        assert_eq!(reconciler.reconcile_due(12).await.unwrap(), 1);
        assert_eq!(&*api.created.lock().unwrap(), &["cairn"]);
        assert_eq!(reconciler.reconcile_due(13).await.unwrap(), 0);

        let thread = ensure_surface(
            &db,
            1,
            SurfaceKind::IssueThread,
            "cairn",
            Some("cairn://p/cairn/general"),
            Some(first.id),
            13,
        )
        .await
        .unwrap();
        assert_eq!(reconciler.reconcile_due(13).await.unwrap(), 1);
        assert_eq!(
            super::super::ledger::lookup_conversation_target(&db, "discord", "discord:1/102")
                .await
                .unwrap(),
            Some("cairn://p/cairn/general".into())
        );

        assert!(request_lock(&db, thread.id, 14).await.unwrap());
        assert!(!request_lock(&db, first.id, 15).await.unwrap());
        assert_eq!(reconciler.reconcile_due(16).await.unwrap(), 1);
        assert_eq!(&*api.locks.lock().unwrap(), &[102]);
        assert_eq!(reconciler.reconcile_due(17).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn unmanaged_home_is_unique_and_relative_targets_are_rejected() {
        let db = crate::storage::migrated_test_db("discord-surface-identity.db").await;
        let category = ensure_surface(&db, 1, SurfaceKind::ProjectCategory, "cairn", None, None, 9)
            .await
            .unwrap();
        let first = ensure_surface(
            &db,
            1,
            SurfaceKind::UnmanagedHome,
            "cairn",
            None,
            Some(category.id),
            10,
        )
        .await
        .unwrap();
        let duplicate = ensure_surface(
            &db,
            1,
            SurfaceKind::UnmanagedHome,
            "cairn",
            None,
            Some(category.id),
            11,
        )
        .await
        .unwrap();
        assert_eq!(first.id, duplicate.id);
        assert!(ensure_surface(
            &db,
            1,
            SurfaceKind::IssueThread,
            "cairn",
            Some("cairn:~/1"),
            Some(first.id),
            12
        )
        .await
        .unwrap_err()
        .contains("home-relative"));
    }

    #[tokio::test]
    async fn lazy_issue_parent_selection_is_canonical_and_concurrent_safe() {
        let db = crate::storage::migrated_test_db("discord-lazy-parents.db").await;
        let managed = ensure_issue_surface(
            &db,
            9,
            "cairn",
            "cairn://p/cairn/41",
            Some("cairn://p/cairn/general"),
            1,
        )
        .await
        .unwrap();
        let duplicate = ensure_issue_surface(
            &db,
            9,
            "cairn",
            "cairn://p/cairn/41",
            Some("cairn://p/cairn/general"),
            2,
        )
        .await
        .unwrap();
        assert_eq!(managed.id, duplicate.id);
        let unmanaged = ensure_issue_surface(&db, 9, "cairn", "cairn://p/cairn/42", None, 3)
            .await
            .unwrap();
        let parent_kinds = db.query_all(
            "SELECT surface_kind FROM discord_surface WHERE id IN (?1, ?2) ORDER BY surface_kind",
            params![managed.parent_surface_id.unwrap(), unmanaged.parent_surface_id.unwrap()],
            |row| row.text(0),
        ).await.unwrap();
        assert_eq!(parent_kinds, vec!["thread_channel", "unmanaged_home"]);
    }

    #[tokio::test]
    async fn seed_is_persisted_before_create_and_ambiguous_success_converges() {
        let db = Arc::new(crate::storage::migrated_test_db("discord-seed-recovery.db").await);
        let category = ensure_surface(&db, 1, SurfaceKind::ProjectCategory, "cairn", None, None, 1)
            .await
            .unwrap();
        let parent = ensure_surface(
            &db,
            1,
            SurfaceKind::UnmanagedHome,
            "cairn",
            None,
            Some(category.id),
            1,
        )
        .await
        .unwrap();
        let api = Arc::new(FakeDiscordApi::default());
        let reconciler = DiscordSurfaceReconciler::new(db.clone(), api.clone());
        reconciler.reconcile_due(1).await.unwrap();
        reconciler.reconcile_due(2).await.unwrap();
        let issue = ensure_surface(
            &db,
            1,
            SurfaceKind::IssueThread,
            "cairn",
            Some("cairn://p/cairn/7"),
            Some(parent.id),
            3,
        )
        .await
        .unwrap();
        api.ambiguous_thread_create.store(true, Ordering::SeqCst);
        reconciler.reconcile_due(3).await.unwrap();
        let recovered = db
            .query_one(
                &format!("SELECT {COLUMNS} FROM discord_surface WHERE id=?1"),
                (issue.id,),
                DiscordSurface::from_row,
            )
            .await
            .unwrap();
        assert_eq!(recovered.seed_message_id, Some(200));
        assert_eq!(recovered.remote_channel_id, Some(200));
        assert_eq!(api.seeds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn archived_issue_wakes_but_locked_issue_never_wakes() {
        let db = Arc::new(crate::storage::migrated_test_db("discord-archive-lock.db").await);
        let category = ensure_surface(&db, 1, SurfaceKind::ProjectCategory, "cairn", None, None, 1)
            .await
            .unwrap();
        let parent = ensure_surface(
            &db,
            1,
            SurfaceKind::UnmanagedHome,
            "cairn",
            None,
            Some(category.id),
            1,
        )
        .await
        .unwrap();
        let api = Arc::new(FakeDiscordApi::default());
        let reconciler = DiscordSurfaceReconciler::new(db.clone(), api.clone());
        reconciler.reconcile_due(1).await.unwrap();
        reconciler.reconcile_due(2).await.unwrap();
        let issue = ensure_surface(
            &db,
            1,
            SurfaceKind::IssueThread,
            "cairn",
            Some("cairn://p/cairn/8"),
            Some(parent.id),
            3,
        )
        .await
        .unwrap();
        reconciler.reconcile_due(3).await.unwrap();
        db.execute(
            "UPDATE discord_surface SET observed_state='archived' WHERE id=?1",
            (issue.id,),
        )
        .await
        .unwrap();
        reconciler.reconcile_due(4).await.unwrap();
        assert_eq!(&*api.wakes.lock().unwrap(), &[102]);
        request_lock(&db, issue.id, 5).await.unwrap();
        db.execute(
            "UPDATE discord_surface SET observed_state='archived' WHERE id=?1",
            (issue.id,),
        )
        .await
        .unwrap();
        reconciler.reconcile_due(5).await.unwrap();
        assert_eq!(&*api.wakes.lock().unwrap(), &[102]);
        assert_eq!(&*api.locks.lock().unwrap(), &[102]);
    }

    #[tokio::test]
    async fn active_surface_repairs_binding_in_separate_database() {
        let project_db =
            Arc::new(crate::storage::migrated_test_db("discord-project-binding-repair.db").await);
        let binding_db =
            Arc::new(crate::storage::migrated_test_db("discord-local-binding-repair.db").await);
        let category = ensure_surface(
            &project_db,
            1,
            SurfaceKind::ProjectCategory,
            "cairn",
            None,
            None,
            1,
        )
        .await
        .unwrap();
        let home = ensure_surface(
            &project_db,
            1,
            SurfaceKind::UnmanagedHome,
            "cairn",
            None,
            Some(category.id),
            1,
        )
        .await
        .unwrap();
        let issue = ensure_surface(
            &project_db,
            1,
            SurfaceKind::IssueThread,
            "cairn",
            Some("cairn://p/cairn/9"),
            Some(home.id),
            1,
        )
        .await
        .unwrap();
        let api = Arc::new(FakeDiscordApi::default());
        let reconciler =
            DiscordSurfaceReconciler::with_binding_db(project_db, binding_db.clone(), api);
        reconciler.reconcile_due(1).await.unwrap();
        reconciler.reconcile_due(2).await.unwrap();
        reconciler.reconcile_due(3).await.unwrap();
        assert_eq!(
            super::super::ledger::lookup_conversation_target(
                &binding_db,
                "discord",
                "discord:1/102"
            )
            .await
            .unwrap()
            .as_deref(),
            Some("cairn://p/cairn/9")
        );

        binding_db.execute(
            "DELETE FROM channel_conversation_binding WHERE provider='discord' AND conversation='discord:1/102'",
            (),
        ).await.unwrap();
        assert_eq!(reconciler.reconcile_due(4).await.unwrap(), 0);
        assert_eq!(
            super::super::ledger::lookup_conversation_target(
                &binding_db,
                "discord",
                "discord:1/102"
            )
            .await
            .unwrap()
            .as_deref(),
            Some("cairn://p/cairn/9")
        );
        let _ = issue;
    }

    #[tokio::test]
    async fn pre_send_inspection_wakes_an_archived_thread_before_posting() {
        let api = FakeDiscordApi::default();
        api.archived.store(true, Ordering::SeqCst);
        super::super::discord::ensure_channel_sendable(&api, 102)
            .await
            .unwrap();
        assert_eq!(&*api.wakes.lock().unwrap(), &[102]);
        assert!(!api.archived.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn locked_creating_issue_recovers_persisted_seed_after_crash() {
        let db = Arc::new(crate::storage::migrated_test_db("discord-lock-crash-recovery.db").await);
        let category = ensure_surface(&db, 1, SurfaceKind::ProjectCategory, "cairn", None, None, 1)
            .await
            .unwrap();
        let home = ensure_surface(
            &db,
            1,
            SurfaceKind::UnmanagedHome,
            "cairn",
            None,
            Some(category.id),
            1,
        )
        .await
        .unwrap();
        let api = Arc::new(FakeDiscordApi::default());
        let reconciler = DiscordSurfaceReconciler::new(db.clone(), api.clone());
        reconciler.reconcile_due(1).await.unwrap();
        reconciler.reconcile_due(2).await.unwrap();
        let issue = ensure_surface(
            &db,
            1,
            SurfaceKind::IssueThread,
            "cairn",
            Some("cairn://p/cairn/10"),
            Some(home.id),
            3,
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE discord_surface SET desired_state='locked', observed_state='creating', seed_message_id='200', remote_channel_id=NULL WHERE id=?1",
            (issue.id,),
        ).await.unwrap();

        reconciler.reconcile_due(3).await.unwrap();
        assert_eq!(&*api.locks.lock().unwrap(), &[200]);
        let recovered = db
            .query_one(
                &format!("SELECT {COLUMNS} FROM discord_surface WHERE id=?1"),
                (issue.id,),
                DiscordSurface::from_row,
            )
            .await
            .unwrap();
        assert_eq!(recovered.remote_channel_id, Some(200));
        assert_eq!(recovered.observed_state, "locked");
    }

    #[tokio::test]
    async fn one_row_error_does_not_starve_later_due_surfaces() {
        let db = Arc::new(crate::storage::migrated_test_db("discord-row-isolation.db").await);
        let category = ensure_surface(&db, 1, SurfaceKind::ProjectCategory, "cairn", None, None, 1)
            .await
            .unwrap();
        let home = ensure_surface(
            &db,
            1,
            SurfaceKind::UnmanagedHome,
            "cairn",
            None,
            Some(category.id),
            1,
        )
        .await
        .unwrap();
        let api = Arc::new(FakeDiscordApi::default());
        let reconciler = DiscordSurfaceReconciler::new(db.clone(), api);
        reconciler.reconcile_due(1).await.unwrap();
        reconciler.reconcile_due(2).await.unwrap();
        let broken = ensure_surface(
            &db,
            1,
            SurfaceKind::IssueThread,
            "cairn",
            Some("cairn://p/cairn/11"),
            Some(home.id),
            3,
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE discord_surface SET desired_state='locked', observed_state='creating', seed_message_id='999' WHERE id=?1",
            (broken.id,),
        ).await.unwrap();
        let later = ensure_surface(&db, 2, SurfaceKind::ProjectCategory, "later", None, None, 3)
            .await
            .unwrap();

        assert!(reconciler.reconcile_due(3).await.is_err());
        let state = db
            .query_one(
                "SELECT observed_state FROM discord_surface WHERE id=?1",
                (later.id,),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(state, "active");
    }

    #[tokio::test]
    async fn terminal_issue_lock_targets_only_the_matching_issue_surface() {
        let db = crate::storage::migrated_test_db("discord-issue-lock.db").await;
        let category = ensure_surface(&db, 7, SurfaceKind::ProjectCategory, "cairn", None, None, 1)
            .await
            .unwrap();
        let home = ensure_surface(
            &db,
            7,
            SurfaceKind::UnmanagedHome,
            "cairn",
            None,
            Some(category.id),
            2,
        )
        .await
        .unwrap();
        let issue = ensure_surface(
            &db,
            7,
            SurfaceKind::IssueThread,
            "cairn",
            Some("cairn://p/cairn/42"),
            Some(home.id),
            3,
        )
        .await
        .unwrap();

        assert!(request_issue_lock(&db, 7, "cairn://p/cairn/42", 4)
            .await
            .unwrap());
        assert!(!request_issue_lock(&db, 7, "cairn://p/cairn/41", 5)
            .await
            .unwrap());
        let due = list_due(&db, 5).await.unwrap();
        let locked = due.into_iter().find(|row| row.id == issue.id).unwrap();
        assert_eq!(locked.desired_state, "locked");
    }
}

impl SurfaceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::ProjectCategory => "project_category",
            Self::ThreadChannel => "thread_channel",
            Self::UnmanagedHome => "unmanaged_home",
            Self::IssueThread => "issue_thread",
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "project_category" => Ok(Self::ProjectCategory),
            "thread_channel" => Ok(Self::ThreadChannel),
            "unmanaged_home" => Ok(Self::UnmanagedHome),
            "issue_thread" => Ok(Self::IssueThread),
            other => Err(format!("unknown Discord surface kind {other:?}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscordSurface {
    pub id: i64,
    pub guild_id: u64,
    pub kind: SurfaceKind,
    pub project_key: String,
    pub target_uri: Option<String>,
    pub parent_surface_id: Option<i64>,
    pub remote_channel_id: Option<u64>,
    pub seed_message_id: Option<u64>,
    pub desired_state: String,
    pub observed_state: String,
    pub attempt_count: i64,
    pub next_attempt_at: i64,
    pub last_error: Option<String>,
    pub updated_at: i64,
}

impl DiscordSurface {
    fn from_row(row: &Row) -> cairn_db::storage::DbResult<Self> {
        let guild_id = row.text(1)?.parse().map_err(|_| {
            cairn_db::storage::DbError::internal("invalid discord_surface guild_id")
        })?;
        let kind =
            SurfaceKind::parse(&row.text(2)?).map_err(cairn_db::storage::DbError::internal)?;
        let remote_channel_id = row
            .opt_text(6)?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| cairn_db::storage::DbError::internal("invalid remote channel ID"))?;
        let seed_message_id = row
            .opt_text(7)?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| cairn_db::storage::DbError::internal("invalid seed message ID"))?;
        Ok(Self {
            id: row.i64(0)?,
            guild_id,
            kind,
            project_key: row.text(3)?,
            target_uri: row.opt_text(4)?,
            parent_surface_id: row.opt_i64(5)?,
            remote_channel_id,
            seed_message_id,
            desired_state: row.text(8)?,
            observed_state: row.text(9)?,
            attempt_count: row.i64(10)?,
            next_attempt_at: row.i64(11)?,
            last_error: row.opt_text(12)?,
            updated_at: row.i64(13)?,
        })
    }
}

const COLUMNS: &str = "id, guild_id, surface_kind, project_key, target_uri, parent_surface_id, remote_channel_id, seed_message_id, desired_state, observed_state, attempt_count, next_attempt_at, last_error, updated_at";

pub async fn ensure_surface(
    db: &LocalDb,
    guild_id: u64,
    kind: SurfaceKind,
    project_key: &str,
    target_uri: Option<&str>,
    parent_surface_id: Option<i64>,
    now: i64,
) -> Result<DiscordSurface, String> {
    let target_uri = target_uri.map(canonical_target).transpose()?;
    let guild = guild_id.to_string();
    let kind_name = kind.as_str().to_string();
    let project = project_key.to_ascii_lowercase();
    db.execute(
        "INSERT OR IGNORE INTO discord_surface
         (guild_id, surface_kind, project_key, target_uri, parent_surface_id, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            guild.clone(),
            kind_name.clone(),
            project.clone(),
            target_uri.clone(),
            parent_surface_id,
            now
        ],
    )
    .await
    .map_err(|error| error.to_string())?;

    let row = if let Some(target_uri) = target_uri {
        db.query_one(
            &format!("SELECT {COLUMNS} FROM discord_surface WHERE guild_id = ?1 AND surface_kind = ?2 AND target_uri = ?3"),
            params![guild, kind_name, target_uri],
            DiscordSurface::from_row,
        ).await
    } else {
        db.query_one(
            &format!("SELECT {COLUMNS} FROM discord_surface WHERE guild_id = ?1 AND surface_kind = ?2 AND project_key = ?3 AND target_uri IS NULL"),
            params![guild, kind_name, project],
            DiscordSurface::from_row,
        ).await
    };
    row.map_err(|error| error.to_string())
}

fn canonical_target(uri: &str) -> Result<String, String> {
    let uri = cairn_common::uri::canonicalize_uri_identity(uri);
    if uri == "cairn:~" || uri.starts_with("cairn:~/") {
        Err("home-relative Discord target must be resolved before persistence".into())
    } else {
        Ok(uri)
    }
}

pub async fn request_lock(db: &LocalDb, id: i64, now: i64) -> Result<bool, String> {
    db.execute(
        "UPDATE discord_surface SET desired_state = 'locked', next_attempt_at = 0, updated_at = ?2
         WHERE id = ?1 AND surface_kind = 'issue_thread' AND desired_state = 'active'",
        params![id, now],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

pub async fn list_due(db: &LocalDb, now: i64) -> Result<Vec<DiscordSurface>, String> {
    db.query_all(
        format!(
            "SELECT {COLUMNS} FROM discord_surface
             WHERE next_attempt_at <= ?1 AND (
               (desired_state = 'active' AND observed_state IN ('absent','creating','archived','failed'))
               OR (desired_state = 'locked' AND observed_state != 'locked')
             ) ORDER BY id"
        ),
        (now,),
        DiscordSurface::from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

pub struct DiscordSurfaceReconciler {
    db: Arc<LocalDb>,
    binding_db: Arc<LocalDb>,
    api: Arc<dyn DiscordApi>,
}

impl DiscordSurfaceReconciler {
    pub fn new(db: Arc<LocalDb>, api: Arc<dyn DiscordApi>) -> Self {
        Self {
            binding_db: db.clone(),
            db,
            api,
        }
    }

    pub fn with_binding_db(
        db: Arc<LocalDb>,
        binding_db: Arc<LocalDb>,
        api: Arc<dyn DiscordApi>,
    ) -> Self {
        Self {
            db,
            binding_db,
            api,
        }
    }

    pub async fn reconcile_due(&self, now: i64) -> Result<usize, String> {
        let rows = list_due(&self.db, now).await?;
        let mut first_error = self.reconcile_active_bindings(now).await.err();
        for row in &rows {
            if let Err(error) = self.reconcile(row, now).await {
                log::warn!("could not reconcile Discord surface {}: {error}", row.id);
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(rows.len())
    }

    async fn reconcile_active_bindings(&self, now: i64) -> Result<(), String> {
        let rows = self.db.query_all(
            format!("SELECT {COLUMNS} FROM discord_surface WHERE observed_state = 'active' AND target_uri IS NOT NULL AND remote_channel_id IS NOT NULL ORDER BY id"),
            (),
            DiscordSurface::from_row,
        ).await.map_err(|error| error.to_string())?;
        let mut first_error = None;
        for row in rows {
            if let Err(error) = self.install_binding(&row, now).await {
                log::warn!(
                    "could not install Discord surface binding {}: {error}",
                    row.id
                );
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn reconcile(&self, row: &DiscordSurface, now: i64) -> Result<(), String> {
        if row.desired_state == "locked" {
            return self.reconcile_lock(row, now).await;
        }
        if row.observed_state == "archived" {
            if row.kind != SurfaceKind::IssueThread {
                return self
                    .mark_failed(row.id, now, "only issue threads may be unarchived")
                    .await;
            }
            let remote = required_remote(row)?;
            return self
                .finish_remote(
                    row,
                    now,
                    self.api.set_thread_archived(remote, false).await,
                    "active",
                )
                .await;
        }
        if row.observed_state == "failed" || row.observed_state == "creating" {
            if let Some(found) = self
                .api
                .find_channel_by_marker(row.guild_id, &marker(row))
                .await?
            {
                return self
                    .mark_active(row.id, found.channel_id, row.seed_message_id, now)
                    .await;
            }
        }
        if !self
            .claim(row.id, &["absent", "failed", "creating"], "creating", now)
            .await?
        {
            return Ok(());
        }
        let result = self.create(row).await;
        match result {
            Ok((remote, seed)) => self.mark_active(row.id, remote, seed, now).await,
            Err(error) => {
                self.mark_failed(row.id, now, &error).await?;
                Ok(())
            }
        }
    }

    async fn create(&self, row: &DiscordSurface) -> Result<(u64, Option<u64>), String> {
        let name = surface_name(row);
        match row.kind {
            SurfaceKind::ProjectCategory => self
                .api
                .create_category(row.guild_id, &name, &marker(row))
                .await
                .map(|id| (id, None)),
            SurfaceKind::ThreadChannel | SurfaceKind::UnmanagedHome => {
                let parent = self.parent_remote(row).await?;
                self.api
                    .create_text_channel(row.guild_id, parent, &name, &marker(row))
                    .await
                    .map(|id| (id, None))
            }
            SurfaceKind::IssueThread => {
                let parent = self.parent_remote(row).await?;
                let seed = match row.seed_message_id {
                    Some(id) => id,
                    None => {
                        let id = self
                            .api
                            .send_message(parent, &format!("Creating {name}"))
                            .await?;
                        // The seed is the remote idempotency key. Persist it before
                        // thread creation so every retry addresses the same object.
                        self.persist_seed(row.id, id).await?;
                        id
                    }
                };
                match self.api.create_public_thread(parent, seed, &name).await {
                    Ok(id) => Ok((id, Some(seed))),
                    Err(create_error) => {
                        // Discord threads created from messages share the starter
                        // message's snowflake. An ambiguous success is therefore
                        // recoverable by inspecting that stable id.
                        match self.api.inspect_channel(seed).await {
                            Ok(remote) if remote.parent_id == Some(parent) => {
                                Ok((remote.channel_id, Some(seed)))
                            }
                            _ => Err(create_error),
                        }
                    }
                }
            }
        }
    }

    async fn persist_seed(&self, id: i64, seed: u64) -> Result<(), String> {
        self.db
            .execute(
                "UPDATE discord_surface SET seed_message_id = COALESCE(seed_message_id, ?2) WHERE id = ?1",
                params![id, seed.to_string()],
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn parent_remote(&self, row: &DiscordSurface) -> Result<u64, String> {
        let parent = row
            .parent_surface_id
            .ok_or_else(|| "Discord surface is missing its parent".to_string())?;
        self.db.query_one(
            "SELECT remote_channel_id FROM discord_surface WHERE id = ?1 AND observed_state = 'active'",
            (parent,),
            |row| row.text(0),
        ).await.map_err(|_| "Discord surface parent is not active".to_string())?
            .parse().map_err(|_| "Discord surface parent has an invalid remote ID".to_string())
    }

    async fn reconcile_lock(&self, row: &DiscordSurface, now: i64) -> Result<(), String> {
        if row.observed_state == "locked" {
            return Ok(());
        }
        let remote = match row.remote_channel_id {
            Some(remote) => remote,
            None => match self.recover_remote(row).await? {
                Some(remote) => {
                    self.persist_remote(row.id, remote, now).await?;
                    remote
                }
                None if row.seed_message_id.is_none() => {
                    self.db.execute(
                        "UPDATE discord_surface SET observed_state = 'locked', last_error = NULL, next_attempt_at = 0, updated_at = ?2 WHERE id = ?1",
                        params![row.id, now],
                    ).await.map_err(|error| error.to_string())?;
                    return Ok(());
                }
                None => {
                    return Err(
                        "could not recover Discord thread created from persisted seed".into(),
                    )
                }
            },
        };
        if !self
            .claim(
                row.id,
                &["active", "archived", "creating", "failed"],
                "locking",
                now,
            )
            .await?
        {
            return Ok(());
        }
        self.finish_remote(row, now, self.api.lock_thread(remote).await, "locked")
            .await
    }

    async fn recover_remote(&self, row: &DiscordSurface) -> Result<Option<u64>, String> {
        if let Some(found) = self
            .api
            .find_channel_by_marker(row.guild_id, &marker(row))
            .await?
        {
            return Ok(Some(found.channel_id));
        }
        let Some(seed) = row.seed_message_id else {
            return Ok(None);
        };
        let remote = self.api.inspect_channel(seed).await?;
        let expected_parent = self.parent_remote(row).await?;
        (remote.parent_id == Some(expected_parent))
            .then_some(remote.channel_id)
            .ok_or_else(|| "recovered Discord thread has the wrong parent".to_string())
            .map(Some)
    }

    async fn persist_remote(&self, id: i64, remote: u64, now: i64) -> Result<(), String> {
        self.db
            .execute(
                "UPDATE discord_surface SET remote_channel_id = ?2, updated_at = ?3 WHERE id = ?1",
                params![id, remote.to_string(), now],
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn finish_remote(
        &self,
        row: &DiscordSurface,
        now: i64,
        result: Result<(), String>,
        state: &str,
    ) -> Result<(), String> {
        match result {
            Ok(()) => {
                self.db.execute(
                    "UPDATE discord_surface SET observed_state = ?2, last_error = NULL, next_attempt_at = 0, updated_at = ?3 WHERE id = ?1",
                    params![row.id, state.to_string(), now],
                ).await.map(|_| ()).map_err(|error| error.to_string())
            }
            Err(error) => self.mark_failed(row.id, now, &error).await,
        }
    }

    async fn claim(&self, id: i64, from: &[&str], to: &str, now: i64) -> Result<bool, String> {
        let placeholders = std::iter::repeat_n("?", from.len())
            .enumerate()
            .map(|(index, _)| format!("?{}", index + 4))
            .collect::<Vec<_>>()
            .join(",");
        let mut values: Vec<cairn_db::turso::Value> =
            vec![id.into(), to.to_string().into(), now.into()];
        values.extend(from.iter().map(|value| (*value).to_string().into()));
        self.db.execute(
            &format!("UPDATE discord_surface SET observed_state = ?2, updated_at = ?3 WHERE id = ?1 AND observed_state IN ({placeholders})"),
            values,
        ).await.map(|changed| changed == 1).map_err(|error| error.to_string())
    }

    async fn mark_active(
        &self,
        id: i64,
        remote: u64,
        seed: Option<u64>,
        now: i64,
    ) -> Result<(), String> {
        self.db.execute(
            "UPDATE discord_surface SET remote_channel_id = ?2, seed_message_id = COALESCE(seed_message_id, ?3), observed_state = 'active', attempt_count = 0, next_attempt_at = 0, last_error = NULL, updated_at = ?4 WHERE id = ?1",
            params![id, remote.to_string(), seed.map(|id| id.to_string()), now],
        ).await.map_err(|error| error.to_string())?;

        let row = self
            .db
            .query_one(
                &format!("SELECT {COLUMNS} FROM discord_surface WHERE id = ?1"),
                (id,),
                DiscordSurface::from_row,
            )
            .await
            .map_err(|error| error.to_string())?;
        self.install_binding(&row, now).await
    }

    async fn install_binding(&self, row: &DiscordSurface, now: i64) -> Result<(), String> {
        let (Some(target), Some(remote)) = (row.target_uri.as_deref(), row.remote_channel_id)
        else {
            return Ok(());
        };
        super::ledger::bind_target(
            &self.binding_db,
            "discord",
            &format!("discord:{}/{}", row.guild_id, remote),
            target,
            super::bindings::BindingKind::Structural,
            super::bindings::MESSAGE_CLASSES_ALL,
            now,
            0,
        )
        .await
        .map(|_| ())
    }

    async fn mark_failed(&self, id: i64, now: i64, error: &str) -> Result<(), String> {
        self.db.execute(
            "UPDATE discord_surface SET observed_state = 'failed', attempt_count = attempt_count + 1, next_attempt_at = ?2 + MIN(3600, 1 << MIN(attempt_count, 11)), last_error = ?3, updated_at = ?2 WHERE id = ?1",
            params![id, now, error.to_string()],
        ).await.map(|_| ()).map_err(|error| error.to_string())
    }
}

fn required_remote(row: &DiscordSurface) -> Result<u64, String> {
    row.remote_channel_id
        .ok_or_else(|| "Discord surface has no remote channel".into())
}

fn marker(row: &DiscordSurface) -> String {
    format!(
        "cairn:surface:{}:{}",
        row.kind.as_str(),
        row.target_uri.as_deref().unwrap_or(&row.project_key)
    )
}

fn surface_name(row: &DiscordSurface) -> String {
    let source = row
        .target_uri
        .as_deref()
        .and_then(|uri| uri.rsplit('/').next())
        .unwrap_or(&row.project_key);
    let mut name = source
        .to_ascii_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    name.truncate(80);
    if name.is_empty() {
        "cairn".into()
    } else {
        name
    }
}
