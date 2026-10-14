//! Todo models for database records

#[derive(Debug)]
pub struct DbTodo {
    pub id: String,
    pub job_id: String,
    pub todo_id: String,
    pub content: String,
    pub status: String,
    pub priority: Option<String>,
    pub active_form: Option<String>,
    pub position: i32,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug)]
pub struct NewTodo<'a> {
    pub id: &'a str,
    pub job_id: &'a str,
    pub todo_id: &'a str,
    pub content: &'a str,
    pub status: &'a str,
    pub priority: Option<&'a str>,
    pub active_form: Option<&'a str>,
    pub position: i32,
    pub created_at: i32,
    pub updated_at: i32,
}
