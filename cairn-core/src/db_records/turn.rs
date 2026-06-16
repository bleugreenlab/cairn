//! Turn models for database records

#[derive(Debug, Clone)]
pub struct DbTurn {
    pub id: String,
    pub session_id: String,
    pub run_id: Option<String>,
    pub job_id: Option<String>,
    pub sequence: i32,
    pub predecessor_id: Option<String>,
    pub state: String,
    pub yield_reason: Option<String>,
    pub start_reason: String,
    pub created_at: i32,
    pub started_at: Option<i32>,
    pub ended_at: Option<i32>,
    pub updated_at: i32,
}

#[derive(Debug)]
pub struct NewTurn<'a> {
    pub id: &'a str,
    pub session_id: &'a str,
    pub run_id: Option<&'a str>,
    pub job_id: Option<&'a str>,
    pub sequence: i32,
    pub predecessor_id: Option<&'a str>,
    pub state: &'a str,
    pub yield_reason: Option<&'a str>,
    pub start_reason: &'a str,
    pub created_at: i32,
    pub started_at: Option<i32>,
    pub ended_at: Option<i32>,
    pub updated_at: i32,
}

#[derive(Debug, Default)]
pub struct UpdateTurnChangeset<'a> {
    pub run_id: Option<Option<&'a str>>,
    pub state: Option<&'a str>,
    pub yield_reason: Option<Option<&'a str>>,
    pub started_at: Option<Option<i32>>,
    pub ended_at: Option<Option<i32>>,
    pub updated_at: Option<i32>,
}
