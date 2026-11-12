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
//!   `pdf` slot's provider ([`crate::web_providers`]) decides how it becomes
//!   text. **Local** (the zero-config default) downloads the bytes and
//!   extracts them in-process; a pack's `providers/pdf.yaml` MCP binding
//!   instead hands the URL to a tool on the pack's MCP server.

use std::borrow::Cow;
use std::path::Path;

use cairn_packs::provider::http::Inputs;
use cairn_packs::provider::{Adapter, Slot};

use crate::orchestrator::Orchestrator;
use crate::web_providers::{self, McpOutcome, Resolved, LIBRARY_LOCATION};

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

/// Extract a remote PDF URL to text via the `pdf` slot's provider: the
/// built-in local extractor unless the scalar names an installed pack, whose
/// MCP binding then answers. Nothing here names a provider.
pub(crate) async fn read_pdf_url_markdown(
    orch: &Orchestrator,
    url: &str,
) -> Result<String, String> {
    match web_providers::resolve(&orch.config_dir, Slot::Pdf) {
        Resolved::Unconfigured => download_and_extract(url).await,
        Resolved::NotInstalled { name } => Ok(format!(
            "The PDF provider `{name}` is selected but not installed. Install it from \
             {LIBRARY_LOCATION}, or select Local there."
        )),
        Resolved::Invalid { name, error } => Err(format!(
            "The PDF provider `{name}` is installed, but its declaration does not load: \
             {error}. Update or reinstall it from {LIBRARY_LOCATION}."
        )),
        Resolved::Provider(active) => pack_pdf(orch, &active, url).await,
    }
}

/// A pack's PDF provider: a required option left blank is guidance, then its
/// MCP binding runs. The parser admits no other adapter for this slot.
async fn pack_pdf(
    orch: &Orchestrator,
    active: &web_providers::Active,
    url: &str,
) -> Result<String, String> {
    let provider = &active.provider;
    let label = provider.provider.label.as_str();
    if let Some(option) =
        cairn_packs::provider_options::missing_required(&provider.provider.options, &active.options)
    {
        return Ok(format!(
            "{label} PDF extraction needs its {}. Set it in {LIBRARY_LOCATION}.",
            option.label
        ));
    }
    let Adapter::Mcp(binding) = &provider.provider.adapter else {
        return Err(format!(
            "The {label} PDF provider is not an MCP binding, which is the only form the PDF slot runs"
        ));
    };
    let inputs = Inputs {
        query: None,
        target: Some(url),
        credential: "",
        options: &active.options,
    };
    match web_providers::mcp_call(orch, provider, binding, &inputs).await? {
        McpOutcome::Text(text) => Ok(text),
        McpOutcome::NotConnected => Ok(format!(
            "{label} PDF extraction needs its MCP server `{}` connected. Connect it in \
             {LIBRARY_LOCATION}.",
            binding.server
        )),
    }
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
