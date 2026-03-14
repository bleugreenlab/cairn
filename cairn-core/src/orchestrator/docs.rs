//! Orchestrator doc operations.

use crate::docs;
use crate::models::{DocContent, DocFile, DocReference};

use super::Orchestrator;

impl Orchestrator {
    /// List documentation files for a project.
    pub fn list_docs(&self, project_id: &str) -> Result<Vec<DocFile>, String> {
        let mut conn = self.db.conn.lock().map_err(|e| e.to_string())?;
        let repo_path = crate::config::get_project_path(&mut conn, project_id)?;
        let roots = docs::get_doc_roots(&mut conn, project_id)?;
        drop(conn);

        docs::scan_docs(&repo_path, &roots)
    }

    /// Read a documentation file's content.
    pub fn read_doc(&self, project_id: &str, doc_path: &str) -> Result<DocContent, String> {
        let mut conn = self.db.conn.lock().map_err(|e| e.to_string())?;
        let repo_path = crate::config::get_project_path(&mut conn, project_id)?;
        drop(conn);

        docs::read_doc(&repo_path, doc_path)
    }

    /// Write a documentation file and git commit.
    pub fn write_doc(&self, project_id: &str, doc_path: &str, content: &str) -> Result<(), String> {
        let mut conn = self.db.conn.lock().map_err(|e| e.to_string())?;
        let repo_path = crate::config::get_project_path(&mut conn, project_id)?;
        drop(conn);

        docs::write_doc(&repo_path, doc_path, content)?;

        // Git add + commit
        let commit_msg = format!("Update {} via Cairn", doc_path);
        let output = std::process::Command::new("git")
            .current_dir(&repo_path)
            .args(["add", doc_path])
            .output()
            .map_err(|e| format!("Failed to git add: {}", e))?;
        if !output.status.success() {
            return Err(format!(
                "git add failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        let output = std::process::Command::new("git")
            .current_dir(&repo_path)
            .args(["commit", "-m", &commit_msg])
            .output()
            .map_err(|e| format!("Failed to git commit: {}", e))?;
        if !output.status.success() {
            return Err(format!(
                "git commit failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        let _ = self.services.emitter.emit_empty("doc-changed");

        Ok(())
    }

    /// Attach a doc reference to an issue.
    pub fn attach_doc(&self, issue_id: &str, doc_path: &str) -> Result<DocReference, String> {
        let mut conn = self.db.conn.lock().map_err(|e| e.to_string())?;
        docs::attach_doc(&mut conn, issue_id, doc_path)
    }

    /// Detach a doc reference.
    pub fn detach_doc(&self, reference_id: &str) -> Result<(), String> {
        let mut conn = self.db.conn.lock().map_err(|e| e.to_string())?;
        docs::detach_doc(&mut conn, reference_id)
    }

    /// List doc references for an issue.
    pub fn list_doc_references(&self, issue_id: &str) -> Result<Vec<DocReference>, String> {
        let mut conn = self.db.conn.lock().map_err(|e| e.to_string())?;
        docs::list_doc_references(&mut conn, issue_id)
    }
}
