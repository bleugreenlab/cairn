//! Artifact and artifact content models for database records

// ============================================================================
// Artifact models
// ============================================================================

#[derive(Debug, Clone)]
pub struct DbArtifact {
    pub id: String,
    pub job_id: Option<String>,
    pub artifact_type: String,
    pub schema_version: i32,
    pub data: String, // JSON
    pub version: i32,
    pub parent_version_id: Option<String>,
    pub output_name: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
    pub seen_at: Option<i32>,
    /// Durable resolution fact for the job-status projection: an approval
    /// checkpoint's artifact derives `Complete` once confirmed, `Blocked` while not.
    pub confirmed: bool,
}

#[derive(Debug)]
pub struct NewArtifact<'a> {
    pub id: &'a str,
    pub job_id: Option<&'a str>,
    pub artifact_type: &'a str,
    pub schema_version: i32,
    pub data: &'a str,
    pub version: i32,
    pub parent_version_id: Option<&'a str>,
    pub output_name: Option<&'a str>,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug, Default)]
pub struct UpdateArtifactChangeset<'a> {
    pub data: Option<&'a str>,
    pub version: Option<i32>,
    pub updated_at: Option<i32>,
}

// ============================================================================
// Artifact Content models (execution-time artifact data)
// ============================================================================

#[derive(Debug, Clone)]
pub struct DbArtifactContent {
    pub id: String,
    pub artifact_node_id: String,
    pub execution_id: String,
    pub job_id: Option<String>,
    pub data: String, // JSON content
    pub version: i32,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug)]
pub struct NewArtifactContent<'a> {
    pub id: &'a str,
    pub artifact_node_id: &'a str,
    pub execution_id: &'a str,
    pub job_id: Option<&'a str>,
    pub data: &'a str,
    pub version: i32,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug, Default)]
pub struct UpdateArtifactContentChangeset<'a> {
    pub data: Option<&'a str>,
    pub version: Option<i32>,
    pub updated_at: Option<i32>,
}
