//! Web reads, routed through typed services.
//!
//! `read` detects `http(s)://` targets and forwards them here as a `read_web`
//! callback. Remote `.pdf` URLs route to the typed PDF service
//! ([`super::pdf`]); everything else routes to the typed web-fetch service
//! ([`super::fetch_web`]).
//!
//! Local files never read through here, PDFs included: a `file:` target is an
//! ordinary file read (`read/file.rs`), which owns the protected-path,
//! deny-read and logical-root checks. This handler refuses one rather than
//! open a second path to disk that would skip them.
//!
//! Running through cairn-core (rather than directly in `cairn-cmd`) keeps PATH
//! and MCP resolution correct in the signed app and gives `cairn-server`
//! headless parity via the shared dispatcher. Fetch / extraction failures
//! surface as clean guidance text — never a panic.

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ReadWebPayload {
    /// An `http(s)://…` URL.
    target: String,
}

/// Handle a `read_web` callback. Thin String entry kept for the legacy dispatch
/// route; the batch path calls [`read_web_markdown`] so it can window the
/// markdown and surface a failure as an `Error`-kind segment.
pub(crate) async fn handle_read_web(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    match read_web_markdown(orch, request).await {
        Ok(markdown) => markdown,
        Err(message) => message,
    }
}

/// Convert a URL to markdown via the active typed service. `Ok` is the raw
/// markdown; `Err` is a clean guidance message.
pub(crate) async fn read_web_markdown(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<String, String> {
    let payload: ReadWebPayload = super::parse_payload(request)?;
    let target = payload.target.as_str();

    if !(target.starts_with("http://") || target.starts_with("https://")) {
        return Err(format!(
            "`{target}` is not an http(s) URL. Read local files, PDFs included, as a `file:` target through `read`."
        ));
    }

    if super::pdf::is_pdf_url(target) {
        super::pdf::read_pdf_url_markdown(orch, target).await
    } else {
        super::fetch_web::read_fetch_markdown(orch, target).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_orch() -> Orchestrator {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::{LocalDb, SearchIndex};
        use std::sync::Arc;

        let local =
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("t.db"))
                .await
                .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build()
    }

    /// A remote `.pdf` URL still routes to the PDF service and comes back as
    /// the document's text.
    #[tokio::test]
    async fn remote_pdf_url_still_extracts_through_the_pdf_service() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let pdf = super::super::pdf::text_pdf("remote-pdf-text");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/pdf\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                pdf.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&pdf).await.unwrap();
        });

        let orch = test_orch().await;
        let request = McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: tempfile::tempdir().unwrap().keep().display().to_string(),
            run_id: None,
            tool: "read_web".to_string(),
            payload: serde_json::json!({ "target": format!("http://{address}/paper.pdf") }),
            tool_use_id: None,
            origin: None,
        };
        let markdown = read_web_markdown(&orch, &request).await.unwrap();
        assert!(markdown.contains("remote-pdf-text"), "{markdown}");
        server.await.unwrap();
    }

    /// The legacy `read_web` route must not be a way around the file-read
    /// checks: a local target is refused before any path is resolved or read.
    #[tokio::test]
    async fn local_targets_are_refused_before_anything_is_read() {
        let orch = test_orch().await;
        let worktree = tempfile::tempdir().unwrap();
        std::fs::write(
            worktree.path().join("doc.pdf"),
            super::super::pdf::text_pdf("local-pdf-secret"),
        )
        .unwrap();

        for target in [
            "file:doc.pdf".to_string(),
            format!("file:{}", worktree.path().join("doc.pdf").display()),
            worktree.path().join("doc.pdf").display().to_string(),
        ] {
            let request = McpCallbackRequest {
                answer_bound_ms: None,
                thread_id: None,
                cwd: worktree.path().display().to_string(),
                run_id: None,
                tool: "read_web".to_string(),
                payload: serde_json::json!({ "target": target }),
                tool_use_id: None,
                origin: None,
            };
            let error = read_web_markdown(&orch, &request).await.unwrap_err();
            assert!(error.contains("not an http(s) URL"), "{target}: {error}");
            assert!(!error.contains("local-pdf-secret"), "{target}: {error}");
        }
    }
}
