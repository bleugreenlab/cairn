//! The default project: whose general thread takes unaddressed phone text that
//! neither a native reply-to nor a recent operator message claims.
//!
//! It is the operator's `channels.defaultProject` setting when set, otherwise
//! the workspace's oldest project, read when the text arrives so a workspace
//! that never set it still has an answer. A default project without an open
//! general thread is refused by name rather than falling through elsewhere.

use crate::orchestrator::Orchestrator;
use crate::projects::crud::GENERAL_THREAD_NAME;
use crate::storage::RowExt;

/// Where the default project came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultProjectSource {
    /// The operator's `channels.defaultProject` setting.
    Setting,
    /// No setting: the workspace's oldest project.
    Oldest,
}

impl DefaultProjectSource {
    /// The word `cairn://channels` renders it as.
    pub fn word(self) -> &'static str {
        match self {
            Self::Setting => "set",
            Self::Oldest => "oldest project",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultProject {
    /// The canonical project key.
    pub key: String,
    pub source: DefaultProjectSource,
}

/// The default project: the setting, else the oldest project across every
/// database by `(created_at, key)`. `None` only when the workspace has no
/// project at all.
pub async fn resolve(orch: &Orchestrator) -> Result<Option<DefaultProject>, String> {
    let configured = crate::config::settings::load_settings(&orch.config_dir)
        .channels
        .default_project
        .map(|key| cairn_uri::canonical_project(key.trim()))
        .filter(|key| !key.is_empty());
    if let Some(key) = configured {
        return Ok(Some(DefaultProject {
            key,
            source: DefaultProjectSource::Setting,
        }));
    }
    let mut oldest: Option<(i64, String)> = None;
    for db in orch.db.all_dbs().await {
        let projects = db
            .query_all("SELECT key, created_at FROM projects", (), |row| {
                Ok((row.text(0)?, row.i64(1)?))
            })
            .await
            .map_err(|error| error.to_string())?;
        for (key, created_at) in projects {
            let candidate = (created_at, cairn_uri::canonical_project(&key));
            if oldest.as_ref().is_none_or(|current| candidate < *current) {
                oldest = Some(candidate);
            }
        }
    }
    Ok(oldest.map(|(_, key)| DefaultProject {
        key,
        source: DefaultProjectSource::Oldest,
    }))
}

/// Whether `project` has a general thread that is not closed.
pub async fn open_general_thread(orch: &Orchestrator, project: &str) -> Result<bool, String> {
    let project = cairn_uri::canonical_project(project);
    let db = orch.db.for_project(&project).await;
    db.query_opt_i64(
        "SELECT 1 FROM threads t JOIN projects p ON p.id = t.project_id
          WHERE p.key = ?1 AND t.name = ?2 AND LOWER(t.status) != 'closed'
          LIMIT 1",
        (project, GENERAL_THREAD_NAME),
    )
    .await
    .map(|found| found.is_some())
    .map_err(|error| error.to_string())
}

/// `cairn://p/{project}/general`.
pub fn general_thread_uri(project: &str) -> String {
    cairn_uri::build_thread_uri(&cairn_uri::canonical_project(project), GENERAL_THREAD_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Projects `young` (created at 20) and `old` (at 10); `old`'s general
    /// thread is closed and `young`'s is active. `settings` is the settings
    /// file's text.
    async fn orchestrator(settings: &str) -> Orchestrator {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::{LocalDb, SearchIndex};
        use std::sync::Arc;
        let local =
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("t.db"))
                .await
                .unwrap();
        local
            .execute_script(
                "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
                 INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at)
                 VALUES ('p-young','w','Young','young','/tmp/young',20,20),
                        ('p-old','w','Old','old','/tmp/old',10,10);
                 INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
                 VALUES ('t-young', 'p-young', 'general', 'active', 1, 1),
                        ('t-old', 'p-old', 'general', 'closed', 1, 1);",
            )
            .await
            .unwrap();
        let config_dir = tempfile::tempdir().unwrap().keep();
        std::fs::write(
            crate::config::settings::get_settings_path(&config_dir),
            settings,
        )
        .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir,
        )
        .build()
    }

    #[tokio::test]
    async fn the_setting_wins_over_the_oldest_project() {
        let orch = orchestrator("channels:\n  defaultProject: YOUNG\n").await;
        assert_eq!(
            resolve(&orch).await.unwrap(),
            Some(DefaultProject {
                key: "young".into(),
                source: DefaultProjectSource::Setting,
            })
        );
        assert_eq!(general_thread_uri("YOUNG"), "cairn://p/young/general");
    }

    #[tokio::test]
    async fn unset_resolves_to_the_oldest_project() {
        let orch = orchestrator("channels: {}\n").await;
        assert_eq!(
            resolve(&orch).await.unwrap(),
            Some(DefaultProject {
                key: "old".into(),
                source: DefaultProjectSource::Oldest,
            })
        );
    }

    #[tokio::test]
    async fn a_closed_general_thread_is_not_open() {
        let orch = orchestrator("channels: {}\n").await;
        assert!(!open_general_thread(&orch, "old").await.unwrap());
        assert!(open_general_thread(&orch, "Young").await.unwrap());
        assert!(!open_general_thread(&orch, "missing").await.unwrap());
    }
}
