//! Typed PDF extraction.
//!
//! `.pdf` reads (a local `file:` PDF or a remote `.pdf` URL) route here from
//! [`super::web`]. The active provider (`config::pdf`) decides how the bytes
//! become text:
//!
//! - **Local** (the default): a pure-Rust extractor that reads the bytes (from
//!   disk or over HTTP) and pulls out the text. No auth.
//! - **bmd**: routes remote PDF URLs through bmd's `fetch` tool (the same MCP
//!   path as bmd web fetch). bmd's HTTP fetch cannot read local bytes, so a
//!   local PDF file falls back to the local extractor.

use crate::config::pdf::{self, PdfProviderId};
use crate::orchestrator::Orchestrator;

/// Whether the target is a PDF: a local `file:` target (only PDFs reach the web
/// handler as files) or a URL whose path ends in `.pdf` (ignoring any query).
pub(crate) fn is_pdf_target(target: &str, is_file: bool) -> bool {
    is_file
        || target
            .split('?')
            .next()
            .unwrap_or(target)
            .to_lowercase()
            .ends_with(".pdf")
}

/// Extract a PDF (local file or remote URL) to text via the active PDF provider.
pub(crate) async fn read_pdf_markdown(
    orch: &Orchestrator,
    resolved_target: &str,
    original_target: &str,
    is_file: bool,
) -> Result<String, String> {
    match pdf::resolve_active_pdf(&orch.config_dir) {
        PdfProviderId::Local => local_extract(resolved_target, original_target, is_file).await,
        PdfProviderId::Bmd => {
            if is_file {
                // bmd's HTTP fetch cannot read local bytes; fall back to local.
                local_extract(resolved_target, original_target, is_file).await
            } else {
                super::fetch_web::bmd_fetch(orch, original_target).await
            }
        }
    }
}

/// Pure-Rust local extraction: read the bytes (from disk or over HTTP) and pull
/// out the text.
async fn local_extract(
    resolved_target: &str,
    original_target: &str,
    is_file: bool,
) -> Result<String, String> {
    let bytes = if is_file {
        std::fs::read(resolved_target)
            .map_err(|e| format!("Failed to read `{original_target}`: {e}"))?
    } else {
        let resp = reqwest::get(resolved_target)
            .await
            .map_err(|e| format!("Failed to download `{original_target}`: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "Download of `{original_target}` failed: HTTP {}",
                resp.status().as_u16()
            ));
        }
        resp.bytes()
            .await
            .map_err(|e| format!("Failed to read `{original_target}`: {e}"))?
            .to_vec()
    };
    pdf_extract::extract_text_from_mem(&bytes)
        .map_err(|e| format!("Failed to extract text from `{original_target}`: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_pdf_target_detects_files_and_urls() {
        assert!(is_pdf_target("file:docs/design.pdf", true));
        assert!(is_pdf_target("https://example.com/a.pdf", false));
        assert!(is_pdf_target("https://example.com/a.PDF", false));
        assert!(is_pdf_target("https://example.com/a.pdf?x=1", false));
        assert!(!is_pdf_target("https://example.com/page", false));
        assert!(!is_pdf_target("https://example.com/notpdf.html", false));
    }
}
