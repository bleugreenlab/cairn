//! Comment models for database records

#[derive(Debug)]
pub struct DbComment {
    pub id: String,
    pub issue_id: String,
    pub content: String,
    pub source: String,
    pub created_at: i32,
}

#[derive(Debug)]
pub struct NewComment<'a> {
    pub id: &'a str,
    pub issue_id: &'a str,
    pub content: &'a str,
    pub source: &'a str,
    pub created_at: i32,
}
