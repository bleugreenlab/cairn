//! Database record conversion into domain values.
use cairn_models::*;

impl From<crate::db_records::DbTurn> for Turn {
    fn from(db: crate::db_records::DbTurn) -> Self {
        Turn {
            id: db.id,
            session_id: db.session_id,
            run_id: db.run_id,
            job_id: db.job_id,
            sequence: db.sequence,
            predecessor_id: db.predecessor_id,
            state: db.state.parse().unwrap_or(TurnState::Pending),
            yield_reason: db.yield_reason.and_then(|r| r.parse().ok()),
            end_reason: db.end_reason.and_then(|r| r.parse().ok()),
            start_reason: db.start_reason.parse().unwrap_or(TurnStartReason::Initial),
            created_at: db.created_at as i64,
            started_at: db.started_at.map(|t| t as i64),
            ended_at: db.ended_at.map(|t| t as i64),
            updated_at: db.updated_at as i64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn db_turn_conversion_unknown_state_falls_back() {
        let db = crate::db_records::DbTurn {
            id: "t1".into(),
            session_id: "s1".into(),
            run_id: None,
            job_id: None,
            sequence: 1,
            predecessor_id: None,
            state: "GARBAGE".into(),
            yield_reason: None,
            end_reason: None,
            start_reason: "initial".into(),
            created_at: 0,
            started_at: None,
            ended_at: None,
            updated_at: 0,
        };
        let turn: Turn = db.into();
        assert_eq!(turn.state, TurnState::Pending);
    }

    #[test]
    fn db_turn_conversion_unknown_start_reason_falls_back() {
        let db = crate::db_records::DbTurn {
            id: "t1".into(),
            session_id: "s1".into(),
            run_id: None,
            job_id: None,
            sequence: 1,
            predecessor_id: None,
            state: "running".into(),
            yield_reason: None,
            end_reason: None,
            start_reason: "GARBAGE".into(),
            created_at: 0,
            started_at: None,
            ended_at: None,
            updated_at: 0,
        };
        let turn: Turn = db.into();
        assert_eq!(turn.start_reason, TurnStartReason::Initial);
    }

    #[test]
    fn db_turn_conversion_unknown_yield_reason_becomes_none() {
        let db = crate::db_records::DbTurn {
            id: "t1".into(),
            session_id: "s1".into(),
            run_id: None,
            job_id: None,
            sequence: 1,
            predecessor_id: None,
            state: "yielded".into(),
            yield_reason: Some("GARBAGE".into()),
            end_reason: None,
            start_reason: "initial".into(),
            created_at: 0,
            started_at: None,
            ended_at: None,
            updated_at: 0,
        };
        let turn: Turn = db.into();
        assert!(turn.yield_reason.is_none());
    }
}
