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

    /// A small, well-formed PDF whose only object is `depth` nested arrays. The
    /// xref offsets are exact so the parser reaches the object body rather than
    /// bailing on a broken cross-reference table.
    fn deeply_nested_array_pdf(depth: usize) -> Vec<u8> {
        let mut pdf = b"%PDF-1.7\n".to_vec();
        let object_offset = pdf.len();
        pdf.extend_from_slice(b"1 0 obj\n");
        pdf.extend(std::iter::repeat_n(b'[', depth));
        pdf.extend(std::iter::repeat_n(b']', depth));
        pdf.extend_from_slice(b"\nendobj\n");
        let xref_offset = pdf.len();
        pdf.extend_from_slice(
            format!(
                "xref\n0 2\n0000000000 65535 f \n{object_offset:010} 00000 n \n\
                 trailer\n<< /Size 2 /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n"
            )
            .as_bytes(),
        );
        pdf
    }

    /// GHSA-6fj4-rcr3-r7rr / RUSTSEC-2026-0187: lopdf <= 0.41 recursed without
    /// bound on nested arrays, so a ~40 KB input like this one overflowed the
    /// stack and aborted the whole process (uncatchable by `catch_unwind`).
    /// Extraction runs in the orchestrator process, so reaching the assertion at
    /// all — with either verdict — is the property under test.
    #[test]
    fn deeply_nested_pdf_returns_instead_of_overflowing_the_stack() {
        let bytes = deeply_nested_array_pdf(20_000);
        // A PDF with no page tree may be rejected or read as empty; either is
        // fine, but it must not yield text.
        if let Ok(text) = pdf_extract::extract_text_from_mem(&bytes) {
            assert!(text.trim().is_empty(), "unexpected text: {text:?}");
        }
    }
}
