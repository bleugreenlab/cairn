//! Cairn Pages: small sandboxed apps a thread owns.
//!
//! A page is a versioned `page` artifact stored as `pages/<name>`. Its entry
//! module default-exports a React component built from `@cairn/page-kit`; the
//! frontend renders it in a sandboxed frame whose only I/O is a postMessage
//! bridge back to the host, and the host carries each bridged read here.
//!
//! This module is the one home of the backend half:
//!
//! - [`data`]: what a page's data may be (the schema preset plus the rules a
//!   JSON Schema cannot state) and how a `patch` composes onto the stored page.
//! - [`store`]: where a page's versions live. On a thread the version chain is
//!   per `(thread, name)` across every session job the thread has had, so a
//!   fresh session continues a page rather than restarting it at version 1. On
//!   an issue node a page is a draft in that node's own job chain.
//! - [`index`]: the project's Pages index (promoted thread pages only).
//! - [`log`]: the `page_calls` audit trail of what open frames did.
//! - [`read`]: `page_read`, a bridged read evaluated with the owning thread's
//!   authority, never the viewer's, intersected with a reading agent's grants.
//! - [`headless`]: the rendered read, which runs a page in a confined bun
//!   process with the kit's text projection and answers its reads with
//!   `page_read`.
//! - [`write`]: `page_write`, a bridged row write made as the verified
//!   operator, through the page, to the tables its manifest declares.
//! - [`render`]: the page resources as agents read them.
//! - [`service`]: `page_service`, a call to a service the page's manifest
//!   declares (a project's, or its own thread's), executing as the service
//!   intersected with the page.
//!
//! Page writes do not go through the generic artifact path: a page has no
//! terminal contract, so it arms no gate, interrupts no run, and advances no
//! DAG. The generic path refuses to create a `page` artifact for the same
//! reason.

mod data;
mod headless;
mod index;
mod log;
mod read;
mod render;
mod service;
mod store;
mod write;

#[cfg(test)]
mod tests;

pub(crate) use data::{compose, validate_file_path};
pub use data::{validate_page_name, MAX_PAGE_BYTES, PAGE_ARTIFACT_TYPE};

/// How a page's relative imports become `page:` specifiers: one classic script
/// that both the frame's shell document and the headless renderer run, so the
/// two loaders cannot disagree about what a page's import means.
pub const PAGE_IMPORTS_JS: &str = include_str!("page_imports.js");
#[cfg(test)]
pub(crate) use headless::{render, PageCallAnswerer, RenderLimits, RenderOutcome, RenderRequest};
pub use index::{list_project_pages, PageIndexFilter, PageSummary};
pub use log::{PageCallOutcome, PageCallVerb};
pub use read::{
    page_log_event, page_read, AgentReader, PageCallError, PageLogEvent, PageReadPurpose,
    PageReadRequest, PageReader,
};
pub(crate) use render::{read_page, read_page_log, read_pages, read_project_pages};
pub use service::{page_service, PageServiceRequest};
pub use store::named_artifact_for_job;
pub(crate) use store::{write_page, PageAddress, PageWriter};
pub use write::{page_write, PageOperator, PageWriteRequest};
