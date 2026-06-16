//! Doc reference models for database records

#[derive(Debug)]
pub struct DbDocReference {
    pub id: String,
    pub issue_id: String,
    pub doc_path: String,
    pub created_at: i32,
}

#[derive(Debug)]
pub struct NewDocReference<'a> {
    pub id: &'a str,
    pub issue_id: &'a str,
    pub doc_path: &'a str,
    pub created_at: i32,
}
