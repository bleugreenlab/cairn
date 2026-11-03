//! Machine capabilities a project's work turned out to need.
//!
//! A project can declare what it needs of a machine (`requires:` in its
//! `.cairn/config.yaml`), but it rarely knows until a machine without the
//! capability fails its work. A check whose git dependency fetch failed for
//! want of a credential is that failure: the classifier stores it as
//! infrastructure and names the capability it implies, and the runner records
//! it here, so later placement requires the capability whether or not the
//! project declares it. Placement still gates only on a machine that measured
//! itself without the capability; an unmeasured machine is not refused.
//!
//! The record is sticky. Its only effect is to keep the project off machines
//! that cannot fetch its dependencies, and `cairn://executors` shows it with
//! its evidence.

use crate::storage::{LocalDb, RowExt};
use cairn_common::executor_protocol::ProjectCapability;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

/// One capability a project was seen to need, and the run that showed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LearnedRequirement {
    pub project_id: String,
    pub capability: ProjectCapability,
    /// The repository the failing fetch named, when it named one.
    pub evidence_repository: Option<String>,
    /// The machine the most recent such failure ran on.
    pub executor_id: String,
    /// The check observation that most recently showed it.
    pub observation_id: String,
    pub first_learned_at_ms: u64,
    pub last_seen_at_ms: u64,
}

/// The runner's learned requirements: memory placement reads synchronously,
/// written through to the database once the store attaches.
#[derive(Clone, Default)]
pub(crate) struct LearnedRequirements {
    by_project: Arc<Mutex<HashMap<String, BTreeMap<ProjectCapability, LearnedRequirement>>>>,
    db: Arc<OnceLock<Arc<LocalDb>>>,
}

impl LearnedRequirements {
    /// The capabilities `project_id` has been learned to need.
    pub(crate) fn capabilities(&self, project_id: &str) -> Vec<ProjectCapability> {
        self.by_project
            .lock()
            .unwrap()
            .get(project_id)
            .map(|learned| learned.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Every learned requirement, by project then capability.
    pub(crate) fn all(&self) -> Vec<LearnedRequirement> {
        let by_project = self.by_project.lock().unwrap();
        let mut all: Vec<LearnedRequirement> = by_project
            .values()
            .flat_map(|learned| learned.values().cloned())
            .collect();
        all.sort_by(|a, b| (&a.project_id, a.capability).cmp(&(&b.project_id, b.capability)));
        all
    }

    /// Adopt what the database holds and write every later learning through
    /// to it. A requirement learned in memory before this read keeps its
    /// newer evidence.
    pub(crate) async fn attach(&self, db: Arc<LocalDb>) -> Result<(), String> {
        let rows = load_all(&db).await?;
        {
            let mut by_project = self.by_project.lock().unwrap();
            for row in rows {
                by_project
                    .entry(row.project_id.clone())
                    .or_default()
                    .entry(row.capability)
                    .or_insert(row);
            }
        }
        let _ = self.db.set(db.clone());
        // Whatever memory learned before the store attached, it now holds.
        let pending = self.all();
        for requirement in &pending {
            record(&db, requirement).await?;
        }
        Ok(())
    }

    /// Record that `project_id` needs `capability`, as `observation_id` on
    /// `executor_id` showed. Placement reads it at once; the database write
    /// follows when the store is attached.
    pub(crate) async fn learn(
        &self,
        project_id: &str,
        capability: ProjectCapability,
        evidence_repository: Option<String>,
        executor_id: &str,
        observation_id: &str,
        now_ms: u64,
    ) -> Result<(), String> {
        let requirement = {
            let mut by_project = self.by_project.lock().unwrap();
            let entry = by_project
                .entry(project_id.to_string())
                .or_default()
                .entry(capability)
                .or_insert_with(|| LearnedRequirement {
                    project_id: project_id.to_string(),
                    capability,
                    evidence_repository: None,
                    executor_id: String::new(),
                    observation_id: String::new(),
                    first_learned_at_ms: now_ms,
                    last_seen_at_ms: now_ms,
                });
            if evidence_repository.is_some() {
                entry.evidence_repository = evidence_repository;
            }
            entry.executor_id = executor_id.to_string();
            entry.observation_id = observation_id.to_string();
            entry.last_seen_at_ms = now_ms;
            entry.clone()
        };
        match self.db.get() {
            Some(db) => record(db, &requirement).await,
            None => Ok(()),
        }
    }
}

/// Every stored requirement this build knows the name of. A row naming a
/// capability only a newer build knows is left in the table and skipped here.
pub(crate) async fn load_all(db: &LocalDb) -> Result<Vec<LearnedRequirement>, String> {
    let rows = db
        .query_all(
            "SELECT project_id, requirement, evidence_repository, executor_id, observation_id,
                    first_learned_at_ms, last_seen_at_ms
               FROM project_learned_requirements",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.opt_text(2)?,
                    row.text(3)?,
                    row.text(4)?,
                    row.i64(5)?,
                    row.i64(6)?,
                ))
            },
        )
        .await
        .map_err(|error| format!("load learned project requirements: {error}"))?;
    Ok(rows
        .into_iter()
        .filter_map(
            |(
                project_id,
                requirement,
                evidence_repository,
                executor_id,
                observation_id,
                first,
                last,
            )| {
                Some(LearnedRequirement {
                    project_id,
                    capability: ProjectCapability::parse(&requirement)?,
                    evidence_repository,
                    executor_id,
                    observation_id,
                    first_learned_at_ms: u64::try_from(first).unwrap_or_default(),
                    last_seen_at_ms: u64::try_from(last).unwrap_or_default(),
                })
            },
        )
        .collect())
}

/// Insert a requirement, or refresh its evidence. The first learning's time
/// is kept, and a refresh that names no repository keeps the one on record.
pub(crate) async fn record(db: &LocalDb, requirement: &LearnedRequirement) -> Result<(), String> {
    db.execute(
        "INSERT INTO project_learned_requirements
              (project_id, requirement, evidence_repository, executor_id, observation_id,
               first_learned_at_ms, last_seen_at_ms)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(project_id, requirement) DO UPDATE SET
              evidence_repository = COALESCE(excluded.evidence_repository,
                                             project_learned_requirements.evidence_repository),
              executor_id = excluded.executor_id,
              observation_id = excluded.observation_id,
              last_seen_at_ms = excluded.last_seen_at_ms",
        (
            requirement.project_id.as_str(),
            requirement.capability.as_str(),
            requirement.evidence_repository.as_deref(),
            requirement.executor_id.as_str(),
            requirement.observation_id.as_str(),
            i64::try_from(requirement.first_learned_at_ms).unwrap_or(i64::MAX),
            i64::try_from(requirement.last_seen_at_ms).unwrap_or(i64::MAX),
        ),
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("record learned project requirement: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TM: &str = "2856b070-e753-428b-90d0-584cda7994d4";
    const TM_CARDS: &str = "https://github.com/bleugreen/tm-cards";

    #[tokio::test]
    async fn a_learned_requirement_survives_rebuilding_from_the_database() {
        let db = Arc::new(crate::storage::migrated_test_db("learned-requirements.db").await);
        let learned = LearnedRequirements::default();
        // Learned before the store attached: memory holds it, and the attach
        // writes it through.
        learned
            .learn(
                TM,
                ProjectCapability::GithubCredential,
                Some(TM_CARDS.into()),
                "bginf-sp1",
                "96e4e69d",
                1_000,
            )
            .await
            .unwrap();
        assert_eq!(
            learned.capabilities(TM),
            [ProjectCapability::GithubCredential]
        );
        learned.attach(db.clone()).await.unwrap();
        // A later failure that names no repository refreshes the evidence and
        // keeps the repository and the first time on record.
        learned
            .learn(
                TM,
                ProjectCapability::GithubCredential,
                None,
                "bginf-sp2",
                "5de3ed3d",
                2_000,
            )
            .await
            .unwrap();

        let rebuilt = LearnedRequirements::default();
        assert!(rebuilt.capabilities(TM).is_empty());
        rebuilt.attach(db.clone()).await.unwrap();
        assert_eq!(
            rebuilt.capabilities(TM),
            [ProjectCapability::GithubCredential]
        );
        assert!(rebuilt.capabilities("other").is_empty());
        assert_eq!(
            rebuilt.all(),
            vec![LearnedRequirement {
                project_id: TM.into(),
                capability: ProjectCapability::GithubCredential,
                evidence_repository: Some(TM_CARDS.into()),
                executor_id: "bginf-sp2".into(),
                observation_id: "5de3ed3d".into(),
                first_learned_at_ms: 1_000,
                last_seen_at_ms: 2_000,
            }]
        );

        db.execute(
            "INSERT INTO project_learned_requirements VALUES ('p', 'gpu', NULL, 'm', 'o', 1, 1)",
            (),
        )
        .await
        .unwrap();
        assert_eq!(
            load_all(&db).await.unwrap().len(),
            1,
            "a capability this build does not know is skipped, not an error"
        );
    }
}
