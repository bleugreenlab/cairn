//! Database record conversion into domain values.
use cairn_domain::*;

impl From<crate::db_records::DbPermissionRequest> for PermissionRequest {
    fn from(db: crate::db_records::DbPermissionRequest) -> Self {
        PermissionRequest {
            id: db.id,
            run_id: db.run_id,
            tool_use_id: db.tool_use_id,
            tool_name: db.tool_name,
            tool_input: serde_json::from_str(&db.tool_input).unwrap_or(serde_json::Value::Null),
            status: db.status.parse().unwrap_or(PermissionStatus::Pending),
            response: db.response.and_then(|r| serde_json::from_str(&r).ok()),
            created_at: db.created_at as i64,
            responded_at: db.responded_at.map(|t| t as i64),
            resolution: None,
        }
    }
}

impl From<crate::db_records::DbTodo> for TodoItem {
    fn from(db: crate::db_records::DbTodo) -> Self {
        TodoItem {
            id: db.todo_id,
            content: db.content,
            status: db.status,
            priority: db.priority,
            active_form: db.active_form,
        }
    }
}
