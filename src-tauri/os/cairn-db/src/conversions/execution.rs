//! Database record conversion into domain values.
use cairn_models::*;

/// Convert DbExecution to Execution
impl TryFrom<crate::db_records::DbExecution> for Execution {
    type Error = String;

    fn try_from(db: crate::db_records::DbExecution) -> Result<Self, Self::Error> {
        let status: ExecutionStatus = db
            .status
            .parse()
            .map_err(|e: String| format!("Invalid execution status: {}", e))?;

        // Reconstruct Initiator from nullable columns (all-or-nothing).
        let initiator = match (db.initiator_sub, db.initiator_org_id) {
            (Some(sub), Some(org_id)) => Some(Initiator { sub, org_id }),
            _ => None,
        };

        let triggered_by: TriggerType = db.triggered_by.parse().unwrap_or_default();

        Ok(Execution {
            id: db.id,
            recipe_id: db.recipe_id,
            issue_id: db.issue_id,
            project_id: db.project_id,
            status,
            started_at: db.started_at as i64,
            completed_at: db.completed_at.map(|t| t as i64),
            seq: db.seq,
            initiator,
            triggered_by,
            runner_device_id: db.runner_device_id,
            lifecycle: db.lifecycle.parse().unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::db_records::DbExecution;

    fn base_db_execution() -> DbExecution {
        DbExecution {
            id: "exec-1".to_string(),
            recipe_id: "recipe-1".to_string(),
            issue_id: None,
            project_id: Some("proj-1".to_string()),
            status: "running".to_string(),
            started_at: 1000,
            completed_at: None,
            snapshot: None,
            seq: Some(1),
            initiator_sub: None,
            initiator_org_id: None,
            triggered_by: "manual".to_string(),
            runner_device_id: None,
            lifecycle: "canonical".to_string(),
        }
    }

    #[test]
    fn try_from_db_execution_all_initiator_columns_present() {
        let db = DbExecution {
            initiator_sub: Some("user-123".to_string()),
            initiator_org_id: Some("org-456".to_string()),
            ..base_db_execution()
        };
        let exec: super::Execution = db.try_into().unwrap();
        let initiator = exec.initiator.expect("should have initiator");
        assert_eq!(initiator.sub, "user-123");
        assert_eq!(initiator.org_id, "org-456");
    }

    #[test]
    fn try_from_db_execution_no_initiator_columns() {
        let db = base_db_execution();
        let exec: super::Execution = db.try_into().unwrap();
        assert!(exec.initiator.is_none());
    }

    #[test]
    fn try_from_db_execution_sub_without_org_id_is_none() {
        // Only sub present, org_id missing → None (all-or-nothing)
        let db = DbExecution {
            initiator_sub: Some("user-123".to_string()),
            initiator_org_id: None,
            ..base_db_execution()
        };
        let exec: super::Execution = db.try_into().unwrap();
        assert!(
            exec.initiator.is_none(),
            "sub without org_id should produce None"
        );
    }

    #[test]
    fn try_from_db_execution_org_id_without_sub_is_none() {
        let db = DbExecution {
            initiator_sub: None,
            initiator_org_id: Some("org-456".to_string()),
            ..base_db_execution()
        };
        let exec: super::Execution = db.try_into().unwrap();
        assert!(
            exec.initiator.is_none(),
            "org_id without sub should produce None"
        );
    }

    #[test]
    fn try_from_db_execution_empty_org_id_is_valid_initiator() {
        // Empty string org_id (personal account) is valid — it's Some(""), not None
        let db = DbExecution {
            initiator_sub: Some("user-123".to_string()),
            initiator_org_id: Some("".to_string()),
            ..base_db_execution()
        };
        let exec: super::Execution = db.try_into().unwrap();
        let initiator = exec
            .initiator
            .expect("empty org_id should still produce Some");
        assert_eq!(initiator.org_id, "");
    }
}
