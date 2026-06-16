//! Trigger accumulator state models for database records.

#[derive(Debug)]
pub struct DbAccumulatorState {
    pub id: String,
    pub recipe_id: String,
    pub group_key: String,
    pub scope_key: String,
    pub events: String,
    pub event_count: i32,
    pub seen_event_ids: String,
    pub first_event_at: i32,
    pub last_event_at: i32,
    pub created_at: i32,
}

#[derive(Debug)]
pub struct NewAccumulatorState<'a> {
    pub id: &'a str,
    pub recipe_id: &'a str,
    pub group_key: &'a str,
    pub scope_key: &'a str,
    pub events: &'a str,
    pub event_count: i32,
    pub seen_event_ids: &'a str,
    pub first_event_at: i32,
    pub last_event_at: i32,
    pub created_at: i32,
}
