//! Typed PDF extraction.
//!
//! Two callers, one extractor:
//!
//! - A **local** `file:` PDF is an ordinary file read (`read/file.rs`). It is
//!   gated, resolved and read exactly like any other `file:` target, and only
//!   its finished bytes pass through [`file_text_bytes`] to become text. There
//!   is deliberately no PDF-specific path to disk here: a second one would need
//!   its own copy of the protected-path, deny-read and logical-root checks.
//! - A **remote** `.pdf` URL routes here from [`super::web`], and the active
//!   provider (`config::pdf`) decides how it becomes text. **Local** (the
//!   default) downloads the bytes and extracts them in-process; **bmd** routes
//!   the URL through bmd's `fetch` tool on the `bmd` MCP server.

use std::borrow::Cow;
use std::path::Path;

use crate::config::pdf::{self, PdfProviderId};
use crate::orchestrator::Orchestrator;

/// Whether a URL's path ends in `.pdf` (ignoring any query).
pub(crate) fn is_pdf_url(url: &str) -> bool {
    url.split('?')
        .next()
        .unwrap_or(url)
        .to_lowercase()
        .ends_with(".pdf")
}

/// Whether a file path names a PDF, by extension.
pub(crate) fn is_pdf_path(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("pdf"))
}

/// The bytes a file read renders for a file it has already read: a PDF's
/// extracted text, or the bytes unchanged for any other file.
pub(crate) fn file_text_bytes<'a>(path: &Path, bytes: &'a [u8]) -> Result<Cow<'a, [u8]>, String> {
    if !is_pdf_path(path) {
        return Ok(Cow::Borrowed(bytes));
    }
    extract_text(bytes).map(|text| Cow::Owned(text.into_bytes()))
}

/// Extract the text of PDF bytes already in hand (a file read, an attachment).
pub(crate) fn extract_text(bytes: &[u8]) -> Result<String, String> {
    pdf_extract::extract_text_from_mem(bytes)
        .map_err(|error| format!("Failed to extract text from PDF: {error}"))
}

/// Extract a remote PDF URL to text via the active PDF provider.
pub(crate) async fn read_pdf_url_markdown(
    orch: &Orchestrator,
    url: &str,
) -> Result<String, String> {
    match pdf::resolve_active_pdf(&orch.config_dir) {
        PdfProviderId::Local => download_and_extract(url).await,
        PdfProviderId::Bmd => bmd_pdf(orch, url).await,
    }
}

/// bmd's `fetch` tool through the host MCP gateway, reusing the OAuth
/// connection of the `bmd` MCP server. Removed when the PDF slot moves to a
/// pack provider, as web fetch did.
async fn bmd_pdf(orch: &Orchestrator, source: &str) -> Result<String, String> {
    use crate::config::mcp_servers::McpServerConfigExt;
    let Some(gateway) = orch.mcp_gateway() else {
        return Err(
            "The bmd PDF provider needs the MCP gateway, which is not available in this host."
                .to_string(),
        );
    };
    let servers = crate::config::mcp_servers::load_workspace_mcp_servers(&orch.config_dir);
    let Some(config) = servers.get("bmd") else {
        return Ok(
            "The bmd PDF provider is not configured. Install and connect bmd in the Library, \
             or switch the PDF provider to Local."
                .to_string(),
        );
    };
    let credential_key = crate::config::secrets::credential_key("bmd", None);
    let expanded = config.brokered(&credential_key, "bmd pdf")?;
    gateway
        .call_tool(
            "cairn-pdf",
            &credential_key,
            &expanded,
            "fetch",
            serde_json::json!({ "source": source }),
            Some(120_000),
        )
        .await
        .map(|result| result.text)
}

/// Pure-Rust extraction of a remote PDF: download the bytes and pull out the text.
async fn download_and_extract(url: &str) -> Result<String, String> {
    let resp = cairn_tls::client()
        .get(url)
        .send()
        .await
        .map_err(|e| format!("Failed to download `{url}`: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!(
            "Download of `{url}` failed: HTTP {}",
            resp.status().as_u16()
        ));
    }
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("Failed to read `{url}`: {e}"))?;
    pdf_extract::extract_text_from_mem(&bytes)
        .map_err(|e| format!("Failed to extract text from `{url}`: {e}"))
}

/// A minimal, well-formed one-page PDF whose page shows `text` in Helvetica.
/// The xref offsets are exact so extractors parse it as a real document.
#[cfg(test)]
pub(crate) fn text_pdf(text: &str) -> Vec<u8> {
    let content = format!("BT /F1 12 Tf 72 712 Td ({text}) Tj ET");
    let objects = [
        "<< /Type /Catalog /Pages 2 0 R >>".to_string(),
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_string(),
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] \
         /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>"
            .to_string(),
        format!(
            "<< /Length {} >>\nstream\n{content}\nendstream",
            content.len()
        ),
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"
            .to_string(),
    ];
    let mut pdf = b"%PDF-1.4\n".to_vec();
    let mut offsets = Vec::with_capacity(objects.len());
    for (index, body) in objects.iter().enumerate() {
        offsets.push(pdf.len());
        pdf.extend_from_slice(format!("{} 0 obj\n{body}\nendobj\n", index + 1).as_bytes());
    }
    let xref_offset = pdf.len();
    let mut xref = format!("xref\n0 {}\n0000000000 65535 f \n", objects.len() + 1);
    for offset in offsets {
        xref.push_str(&format!("{offset:010} 00000 n \n"));
    }
    xref.push_str(&format!(
        "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n",
        objects.len() + 1
    ));
    pdf.extend_from_slice(xref.as_bytes());
    pdf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_pdf_url_detects_pdf_paths() {
        assert!(is_pdf_url("https://example.com/a.pdf"));
        assert!(is_pdf_url("https://example.com/a.PDF"));
        assert!(is_pdf_url("https://example.com/a.pdf?x=1"));
        assert!(!is_pdf_url("https://example.com/page"));
        assert!(!is_pdf_url("https://example.com/notpdf.html"));
    }

    #[test]
    fn file_text_bytes_extracts_pdfs_and_passes_other_files_through() {
        let pdf = text_pdf("needle-in-pdf");
        let text = file_text_bytes(Path::new("docs/a.PDF"), &pdf).unwrap();
        assert!(
            String::from_utf8_lossy(&text).contains("needle-in-pdf"),
            "{}",
            String::from_utf8_lossy(&text)
        );

        let plain = file_text_bytes(Path::new("notes.txt"), b"plain").unwrap();
        assert!(matches!(plain, Cow::Borrowed(b"plain")));

        let error = file_text_bytes(Path::new("fake.pdf"), b"not a pdf").unwrap_err();
        assert!(
            error.starts_with("Failed to extract text from PDF"),
            "{error}"
        );
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
