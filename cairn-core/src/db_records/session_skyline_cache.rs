#[derive(Debug, Clone)]
pub struct DbSessionSkylineCache {
    pub session_id: String,
    pub event_count: i32,
    pub latest_event_created_at: i32,
    pub vibe_count: i32,
    pub latest_vibe_created_at: i32,
    pub bars_json: String,
    pub updated_at: i32,
}

#[derive(Debug)]
pub struct NewSessionSkylineCache<'a> {
    pub session_id: &'a str,
    pub event_count: i32,
    pub latest_event_created_at: i32,
    pub vibe_count: i32,
    pub latest_vibe_created_at: i32,
    pub bars_json: &'a str,
    pub updated_at: i32,
}
