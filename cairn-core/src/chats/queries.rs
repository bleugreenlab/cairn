use crate::diesel_models::DbChat;
use crate::models::Chat;
use crate::schema::chats;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

/// Get the most recent chat for a project (if any)
pub fn get_project_chat(
    conn: &mut SqliteConnection,
    project_id: &str,
) -> Result<Option<Chat>, String> {
    let db_chat: Option<DbChat> = chats::table
        .filter(chats::project_id.eq(project_id))
        .order(chats::created_at.desc())
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to get project chat: {}", e))?;
    Ok(db_chat.map(Chat::from))
}

/// List all chat sessions for a project
pub fn list_project_chats(
    conn: &mut SqliteConnection,
    project_id: &str,
) -> Result<Vec<Chat>, String> {
    let db_chats: Vec<DbChat> = chats::table
        .filter(chats::project_id.eq(project_id))
        .order(chats::created_at.desc())
        .load(conn)
        .map_err(|e| format!("Failed to list project chats: {}", e))?;
    Ok(db_chats.into_iter().map(Chat::from).collect())
}
