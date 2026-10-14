//! Durable attachments: ingest, addressing, resolution, and retirement.
//!
//! An attachment has two identities and they are not the same thing.
//!
//! Its *storage identity* is the sha256 of its bytes. That is the content-store
//! key, and it is what makes dedup across members and integrity verification on
//! fetch work.
//!
//! Its *address* is what a person and an agent read and write: a short ordinal
//! scoped to the conversation the object entered through
//! (`cairn://p/KEY/{issue}/attachments/{n}`). A hash is a terrible address — it
//! says nothing about where the object came from and puts 64 characters of noise
//! in front of every reader of a transcript. [`mint_reference`] allocates the
//! address; an `attachments` row is the mapping between the two.
//!
//! Images are not a separate system. `images/{n}` and `attachments/{n}` are two
//! spellings of one row, differing only in that the first refuses a row whose
//! measured kind is not an image, so every address minted before this module
//! existed keeps resolving to exactly the row it always did.
//!
//! The hash form is never minted, but it is burned into transcripts and message
//! bodies that cannot be rewritten, so [`resolve_content_hash`] accepts it
//! forever as a permalink.

use crate::storage::{content_store::content_hash, LocalDb};
use base64::Engine;
use cairn_common::uri::{
    build_project_attachment_uri, build_project_image_uri, AttachmentRef, AttachmentScopeRef,
};
use cairn_db::storage::RowExt;

/// The ceiling on one durable ingest, matching what the web composer already
/// enforced on an upload. Above it the composer refuses by name with a stated
/// reason; there is deliberately no host-path fallback, because a path that
/// names nothing on the executor is the defect this module exists to close.
pub const MAX_ATTACHMENT_BYTES: usize = 25 * 1024 * 1024;

/// The `scope_key` of the project scope.
///
/// A sentinel rather than NULL for the reason `image_refs` used `0`: SQLite does
/// not compare NULLs as equal in a primary key, so a nullable namespace column
/// would admit duplicate project-scoped ordinals.
const PROJECT_SCOPE_KEY: &str = "";

/// What an attachment is, as measured from its bytes rather than as claimed.
///
/// This is the one job `media_type` cannot do for a row migrated from
/// `image_refs`, whose bytes were not reachable from SQL at migration time: it
/// decides authoritatively whether `images/{n}` may resolve the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentKind {
    Image,
    File,
}

impl AttachmentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::File => "file",
        }
    }

    fn from_str(value: &str) -> Self {
        match value {
            "image" => Self::Image,
            _ => Self::File,
        }
    }

    fn of(media_type: &str) -> Self {
        if media_type.starts_with("image/") {
            Self::Image
        } else {
            Self::File
        }
    }
}

/// Where a newly minted attachment reference is anchored, with the thread's id
/// as well as its name.
///
/// The URI carries a thread's stable *name*; the record carries its *id*, which
/// is what makes a thread's attachments cascade away with the thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachmentAnchor {
    Issue(i32),
    Thread { id: String, name: String },
    Project,
}

impl AttachmentAnchor {
    fn scope(&self) -> &'static str {
        match self {
            Self::Issue(_) => "issue",
            Self::Thread { .. } => "thread",
            Self::Project => "project",
        }
    }

    fn scope_key(&self) -> String {
        match self {
            Self::Issue(number) => number.to_string(),
            Self::Thread { id, .. } => id.clone(),
            Self::Project => PROJECT_SCOPE_KEY.to_string(),
        }
    }

    fn issue_number(&self) -> Option<i32> {
        match self {
            Self::Issue(number) => Some(*number),
            _ => None,
        }
    }

    fn thread_id(&self) -> Option<String> {
        match self {
            Self::Thread { id, .. } => Some(id.clone()),
            _ => None,
        }
    }

    /// The addressable form of this anchor.
    pub fn as_scope_ref(&self) -> AttachmentScopeRef {
        match self {
            Self::Issue(number) => AttachmentScopeRef::Issue(*number),
            Self::Thread { name, .. } => AttachmentScopeRef::Thread(name.clone()),
            Self::Project => AttachmentScopeRef::Project,
        }
    }
}

/// The project an attachment is being minted into, plus its anchor within it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentScope {
    pub project_id: String,
    pub project_key: String,
    pub anchor: AttachmentAnchor,
}

impl AttachmentScope {
    pub fn new(project_id: impl Into<String>, project_key: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            project_key: project_key.into(),
            anchor: AttachmentAnchor::Project,
        }
    }

    pub fn in_issue(mut self, number: Option<i32>) -> Self {
        self.anchor = match number.filter(|value| *value > 0) {
            Some(number) => AttachmentAnchor::Issue(number),
            None => AttachmentAnchor::Project,
        };
        self
    }

    /// Anchor this to the conversation a job belongs to.
    ///
    /// A chat composer knows which job it addresses, not which issue or thread:
    /// both are database facts, so they are resolved here rather than
    /// reconstructed by the caller from a display string. An issue wins over a
    /// thread because a job on an issue is a job inside that issue's world; a
    /// job with neither leaves the anchor project-wide, which is the honest
    /// answer rather than a guess.
    pub async fn for_job(mut self, db: &LocalDb, job_id: &str) -> Self {
        let anchored = db
            .query_opt(
                "SELECT issues.number, threads.id, threads.name
                 FROM jobs
                 LEFT JOIN issues ON jobs.issue_id = issues.id
                 LEFT JOIN threads ON jobs.thread_id = threads.id
                 WHERE jobs.id = ?1
                 LIMIT 1",
                (job_id.to_string(),),
                |row| Ok((row.opt_i64(0)?, row.opt_text(1)?, row.opt_text(2)?)),
            )
            .await
            .unwrap_or_default();
        self.anchor = match anchored {
            Some((Some(number), _, _)) if number > 0 => AttachmentAnchor::Issue(number as i32),
            Some((_, Some(id), Some(name))) => AttachmentAnchor::Thread { id, name },
            _ => AttachmentAnchor::Project,
        };
        self
    }
}

/// Immutable provenance recorded with an act of attaching.
///
/// `author_principal_json` and `appearance_snapshot_json` are the existing
/// principal/appearance vocabulary, serialized; the schema's triggers hold them
/// to being present or absent together and never rewritten. Absent is honest for
/// an ingest path that has no authenticated identity to record, and is not the
/// same fact as an unattributed upload.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AttachmentProvenance {
    pub origin_node_uri: Option<String>,
    pub origin_message_id: Option<String>,
    pub author_principal_json: Option<String>,
    pub appearance_snapshot_json: Option<String>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AttachmentError {
    #[error("attachment data is empty")]
    Empty,
    #[error("attachment base64 is malformed: {0}")]
    MalformedBase64(String),
    #[error("attachment exceeds the 25 MiB limit ({size} bytes)")]
    Oversized { size: usize },
    #[error("unsupported image format")]
    UnsupportedImage,
    #[error("attachment {hash} was not found")]
    Missing { hash: String },
    /// A public URI with no reference row. Either it was never minted, or it
    /// belongs to a project this database does not own.
    #[error("no attachment is registered at {uri}")]
    Unregistered { uri: String },
    /// The row exists but is not an image, so the `images/{n}` spelling of its
    /// address cannot render it. Names the address that can.
    #[error("{uri} is not an image ({media_type}); read it at {attachment_uri}")]
    NotAnImage {
        uri: String,
        media_type: String,
        attachment_uri: String,
    },
    #[error("attachment {uri} was retired and no longer resolves")]
    Retired { uri: String },
    #[error("attachment {hash} failed its content hash integrity check")]
    Corrupt { hash: String },
    #[error("content store {operation} failed: {message}")]
    Store {
        operation: &'static str,
        message: String,
    },
    #[error("attachment reference {operation} failed: {message}")]
    Reference {
        operation: &'static str,
        message: String,
    },
}

/// Sanitize a client-supplied filename into a safe single path component:
/// basename only (directory parts dropped), with any character that is not
/// alphanumeric / `-` / `_` / `.` mapped to `_`. A name that comes out empty or
/// all dots (`.`, `..`, …) becomes `"attachment"` — a bare dot name would
/// otherwise be a traversal component.
///
/// This is the filename gate for every ingest path, and materialization applies
/// it again on the executor side before deriving a destination.
pub fn sanitize_attachment_name(file_name: &str) -> String {
    let basename = std::path::Path::new(file_name)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let sanitized: String = basename
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() || sanitized.chars().all(|c| c == '.') {
        "attachment".to_string()
    } else {
        sanitized
    }
}

/// The media type a byte string actually carries.
///
/// Magic numbers decide first and alone: a `.txt` holding PNG bytes is an image,
/// and no filename can talk this function out of it. The extension is consulted
/// only to choose among candidates the bytes are already compatible with — which
/// text-shaped type a text-shaped payload is — and `application/octet-stream` is
/// the floor rather than a guess.
pub fn measure_media_type(bytes: &[u8], file_name: &str) -> String {
    if let Some(sniffed) = sniff_binary_media_type(bytes) {
        return sniffed.to_string();
    }
    if looks_like_text(bytes) {
        return text_media_type_for(file_name).to_string();
    }
    "application/octet-stream".to_string()
}

/// Media types a byte signature identifies outright.
fn sniff_binary_media_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        Some("image/jpeg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else if bytes.starts_with(b"%PDF-") {
        Some("application/pdf")
    } else if bytes.starts_with(b"PK\x03\x04") {
        Some("application/zip")
    } else if bytes.starts_with(b"\x1f\x8b") {
        Some("application/gzip")
    } else {
        None
    }
}

/// Whether these bytes are text: valid UTF-8 with no NUL and no C0 control
/// characters other than the whitespace a text file legitimately contains.
fn looks_like_text(bytes: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return false;
    };
    !text
        .chars()
        .any(|c| c.is_control() && !matches!(c, '\t' | '\n' | '\r'))
}

/// Which text-shaped type a text-shaped payload is. The extension is a tiebreak
/// among candidates the bytes already admit, never an override of them.
fn text_media_type_for(file_name: &str) -> &'static str {
    let extension = file_name
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase())
        .unwrap_or_default();
    match extension.as_str() {
        "json" => "application/json",
        "csv" => "text/csv",
        "tsv" => "text/tab-separated-values",
        "xml" | "svg" => "application/xml",
        "yaml" | "yml" => "application/yaml",
        "html" | "htm" => "text/html",
        "md" | "markdown" => "text/markdown",
        "toml" => "text/plain",
        _ => "text/plain",
    }
}

/// Whether a media type's bytes are worth rendering as text.
pub fn is_text_media_type(media_type: &str) -> bool {
    media_type.starts_with("text/")
        || matches!(
            media_type,
            "application/json" | "application/xml" | "application/yaml"
        )
}

/// One durable attachment, as recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentRow {
    pub scope: AttachmentScopeRef,
    pub ordinal: i32,
    pub content_hash: String,
    pub kind: AttachmentKind,
    /// `None` on a row migrated from `image_refs`: NOT YET MEASURED rather than
    /// absent. The fetch path measures and backfills on first read.
    pub media_type: Option<String>,
    pub declared_media_type: Option<String>,
    pub file_name: String,
    pub byte_size: Option<i64>,
    pub origin_node_uri: Option<String>,
    pub author_principal_json: Option<String>,
    pub created_at: i64,
    pub retired_at: Option<i64>,
}

impl AttachmentRow {
    /// The general address of this row.
    pub fn attachment_uri(&self, project_key: &str) -> String {
        build_project_attachment_uri(project_key, &self.reference())
    }

    /// The address this row is canonically spelled at: an image renders through
    /// `images/{n}`, everything else through `attachments/{n}`.
    pub fn canonical_uri(&self, project_key: &str) -> String {
        match self.kind {
            AttachmentKind::Image => build_project_image_uri(project_key, &self.reference()),
            AttachmentKind::File => self.attachment_uri(project_key),
        }
    }

    fn reference(&self) -> AttachmentRef {
        AttachmentRef::Scoped {
            scope: self.scope.clone(),
            ordinal: self.ordinal,
        }
    }
}

const ATTACHMENT_COLUMNS: &str = "ordinal, content_hash, kind, media_type, declared_media_type,
     file_name, byte_size, origin_node_uri, author_principal_json, created_at, retired_at";

fn map_attachment_row(
    scope: &AttachmentScopeRef,
    row: &cairn_db::turso::Row,
) -> cairn_db::storage::DbResult<AttachmentRow> {
    Ok(AttachmentRow {
        scope: scope.clone(),
        ordinal: row.i64(0)? as i32,
        content_hash: row.text(1)?,
        kind: AttachmentKind::from_str(&row.text(2)?),
        media_type: row.opt_text(3)?,
        declared_media_type: row.opt_text(4)?,
        file_name: row.text(5)?,
        byte_size: row.opt_i64(6)?,
        origin_node_uri: row.opt_text(7)?,
        author_principal_json: row.opt_text(8)?,
        created_at: row.i64(9)?,
        retired_at: row.opt_i64(10)?,
    })
}

/// The `(scope, scope_key)` pair addressing a scope, resolving a thread's name
/// to the id the rows are keyed by.
///
/// A thread name that names no thread yields `None`, which every caller reports
/// as an unregistered address rather than as an empty collection: the two are
/// different facts.
async fn scope_key_of(
    db: &LocalDb,
    project_id: &str,
    scope: &AttachmentScopeRef,
) -> Result<Option<(&'static str, String)>, AttachmentError> {
    Ok(match scope {
        AttachmentScopeRef::Issue(number) => Some(("issue", number.to_string())),
        AttachmentScopeRef::Project => Some(("project", PROJECT_SCOPE_KEY.to_string())),
        AttachmentScopeRef::Thread(name) => db
            .query_text(
                "SELECT id FROM threads WHERE project_id = ?1 AND name = ?2 LIMIT 1",
                (project_id.to_string(), name.clone()),
            )
            .await
            .map_err(|error| AttachmentError::Reference {
                operation: "resolve scope",
                message: error.to_string(),
            })?
            .map(|id| ("thread", id)),
    })
}

/// Allocate the next ordinal in `scope` and record the attachment against it.
///
/// The read of the current maximum and the insert share one write transaction,
/// so two concurrent uploads into the same scope cannot mint the same ordinal;
/// the primary key is the backstop if they somehow did.
///
/// Identical bytes uploaded twice mint two references to one object. That is
/// deliberate: a reference records an act of attaching, and the dedup that
/// matters already happened in the content store.
#[allow(clippy::too_many_arguments)]
async fn mint_reference(
    db: &LocalDb,
    scope: &AttachmentScope,
    hash: &str,
    kind: AttachmentKind,
    media_type: &str,
    declared_media_type: Option<&str>,
    file_name: &str,
    byte_size: usize,
    provenance: &AttachmentProvenance,
) -> Result<i32, AttachmentError> {
    let project_id = scope.project_id.clone();
    let scope_name = scope.anchor.scope();
    let scope_key = scope.anchor.scope_key();
    let issue_number = scope.anchor.issue_number();
    let thread_id = scope.anchor.thread_id();
    let hash = hash.to_string();
    let kind = kind.as_str();
    let media_type = media_type.to_string();
    let declared_media_type = declared_media_type.map(str::to_string);
    let file_name = file_name.to_string();
    let byte_size = byte_size as i64;
    let provenance = provenance.clone();

    db.write(move |conn| {
        let project_id = project_id.clone();
        let scope_key = scope_key.clone();
        let thread_id = thread_id.clone();
        let hash = hash.clone();
        let media_type = media_type.clone();
        let declared_media_type = declared_media_type.clone();
        let file_name = file_name.clone();
        let provenance = provenance.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COALESCE(MAX(ordinal), 0) FROM attachments
                     WHERE project_id = ?1 AND scope = ?2 AND scope_key = ?3",
                    (project_id.clone(), scope_name, scope_key.clone()),
                )
                .await?;
            let previous = match rows.next().await? {
                Some(row) => row.get::<i64>(0).unwrap_or(0),
                None => 0,
            };
            let ordinal = previous + 1;
            conn.execute(
                "INSERT INTO attachments
                     (project_id, scope, scope_key, issue_number, thread_id, ordinal,
                      content_hash, kind, media_type, declared_media_type, file_name,
                      byte_size, origin_node_uri, origin_message_id,
                      author_principal_json, appearance_snapshot_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                         unixepoch())",
                cairn_db::turso::params![
                    project_id,
                    scope_name,
                    scope_key,
                    issue_number,
                    thread_id,
                    ordinal,
                    hash,
                    kind,
                    media_type,
                    declared_media_type,
                    file_name,
                    byte_size,
                    provenance.origin_node_uri,
                    provenance.origin_message_id,
                    provenance.author_principal_json,
                    provenance.appearance_snapshot_json,
                ],
            )
            .await?;
            Ok(ordinal as i32)
        })
    })
    .await
    .map_err(|error| AttachmentError::Reference {
        operation: "mint",
        message: error.to_string(),
    })
}

/// What one act of attaching produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAttachment {
    /// The address to put in front of a reader: `images/{n}` for an image,
    /// `attachments/{n}` for everything else.
    pub uri: String,
    pub ordinal: i32,
    pub file_name: String,
    pub media_type: String,
    pub kind: AttachmentKind,
    pub byte_size: usize,
}

/// Ingest `bytes` and return the durable address for them.
pub async fn store_attachment_bytes(
    db: &LocalDb,
    scope: &AttachmentScope,
    bytes: Vec<u8>,
    file_name: &str,
    declared_media_type: Option<&str>,
    provenance: &AttachmentProvenance,
) -> Result<StoredAttachment, AttachmentError> {
    if bytes.is_empty() {
        return Err(AttachmentError::Empty);
    }
    if bytes.len() > MAX_ATTACHMENT_BYTES {
        return Err(AttachmentError::Oversized { size: bytes.len() });
    }
    let file_name = sanitize_attachment_name(file_name);
    let media_type = measure_media_type(&bytes, &file_name);
    let kind = AttachmentKind::of(&media_type);
    let byte_size = bytes.len();

    let hash = content_hash(&bytes);
    db.content_store()
        .put(&hash, &bytes)
        .await
        .map_err(|message| AttachmentError::Store {
            operation: "write",
            message,
        })?;
    let ordinal = mint_reference(
        db,
        scope,
        &hash,
        kind,
        &media_type,
        declared_media_type,
        &file_name,
        byte_size,
        provenance,
    )
    .await?;

    let reference = AttachmentRef::Scoped {
        scope: scope.anchor.as_scope_ref(),
        ordinal,
    };
    let uri = match kind {
        AttachmentKind::Image => build_project_image_uri(&scope.project_key, &reference),
        AttachmentKind::File => build_project_attachment_uri(&scope.project_key, &reference),
    };
    Ok(StoredAttachment {
        uri,
        ordinal,
        file_name,
        media_type,
        kind,
        byte_size,
    })
}

/// Ingest base64-encoded bytes.
pub async fn store_attachment_base64(
    db: &LocalDb,
    scope: &AttachmentScope,
    base64_data: &str,
    file_name: &str,
    declared_media_type: Option<&str>,
    provenance: &AttachmentProvenance,
) -> Result<StoredAttachment, AttachmentError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(base64_data)
        .map_err(|e| AttachmentError::MalformedBase64(e.to_string()))?;
    store_attachment_bytes(db, scope, bytes, file_name, declared_media_type, provenance).await
}

/// Ingest base64 bytes that the caller intends as an image, and return the
/// `images/{n}` address for them.
///
/// Bytes that are not an image are refused rather than silently stored as a
/// file, because the caller's contract is an image URI: a paste target, an issue
/// description, a message body. The measurement decides, not the claim.
pub async fn store_image_base64(
    db: &LocalDb,
    scope: &AttachmentScope,
    base64_data: &str,
) -> Result<String, AttachmentError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(base64_data)
        .map_err(|e| AttachmentError::MalformedBase64(e.to_string()))?;
    if bytes.is_empty() {
        return Err(AttachmentError::Empty);
    }
    if !measure_media_type(&bytes, "image").starts_with("image/") {
        return Err(AttachmentError::UnsupportedImage);
    }
    store_attachment_bytes(
        db,
        scope,
        bytes,
        "image",
        None,
        &AttachmentProvenance::default(),
    )
    .await
    .map(|stored| stored.uri)
}

/// The record a scoped address names. `Ok(None)` is impossible — an address that
/// resolves nothing is [`AttachmentError::Unregistered`], because "no such
/// attachment" and "an empty attachment" are different facts.
pub async fn resolve_attachment(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    scope: &AttachmentScopeRef,
    ordinal: i32,
) -> Result<AttachmentRow, AttachmentError> {
    let unregistered = || AttachmentError::Unregistered {
        uri: build_project_attachment_uri(
            project_key,
            &AttachmentRef::Scoped {
                scope: scope.clone(),
                ordinal,
            },
        ),
    };
    let Some((scope_name, scope_key)) = scope_key_of(db, project_id, scope).await? else {
        return Err(unregistered());
    };
    let scope_for_row = scope.clone();
    db.query_opt(
        format!(
            "SELECT {ATTACHMENT_COLUMNS} FROM attachments
             WHERE project_id = ?1 AND scope = ?2 AND scope_key = ?3 AND ordinal = ?4
             LIMIT 1"
        ),
        (
            project_id.to_string(),
            scope_name,
            scope_key,
            ordinal as i64,
        ),
        move |row| map_attachment_row(&scope_for_row, row),
    )
    .await
    .map_err(|error| AttachmentError::Reference {
        operation: "resolve",
        message: error.to_string(),
    })?
    .ok_or_else(unregistered)
}

/// The content-store key a public reference addresses.
///
/// A hash reference IS the key — that is the permalink form, and it resolves
/// without a lookup so an object referenced from a transcript older than the
/// reference table still renders.
pub async fn resolve_content_hash(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    reference: &AttachmentRef,
) -> Result<String, AttachmentError> {
    match reference {
        AttachmentRef::Hash(hash) => Ok(hash.clone()),
        AttachmentRef::Scoped { scope, ordinal } => {
            resolve_attachment(db, project_id, project_key, scope, *ordinal)
                .await
                .map(|row| row.content_hash)
        }
    }
}

/// Fetch and integrity-check the bytes behind a content hash. Corruption is
/// treated as absence rather than served.
pub async fn fetch_bytes(db: &LocalDb, hash: &str) -> Result<Vec<u8>, AttachmentError> {
    let bytes = db
        .content_store()
        .get(hash)
        .await
        .map_err(|message| AttachmentError::Store {
            operation: "read",
            message,
        })?
        .ok_or_else(|| AttachmentError::Missing {
            hash: hash.to_string(),
        })?;
    if content_hash(&bytes) != hash {
        return Err(AttachmentError::Corrupt {
            hash: hash.to_string(),
        });
    }
    Ok(bytes)
}

/// An attachment's bytes together with the media type they actually carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedAttachment {
    pub bytes: Vec<u8>,
    pub media_type: String,
    pub file_name: String,
    pub row: Option<AttachmentRow>,
}

/// Resolve a public reference all the way to its bytes.
///
/// A row whose `media_type` was never measured — every row carried over from
/// `image_refs` — is measured here from the fetched bytes and backfilled
/// best-effort, so a migrated row behaves identically to a freshly minted one
/// from the first read onward. A failed backfill costs nothing: the next read
/// measures again.
pub async fn fetch_attachment(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    reference: &AttachmentRef,
) -> Result<FetchedAttachment, AttachmentError> {
    let row = match reference {
        AttachmentRef::Hash(_) => None,
        AttachmentRef::Scoped { scope, ordinal } => {
            let row = resolve_attachment(db, project_id, project_key, scope, *ordinal).await?;
            if row.retired_at.is_some() {
                return Err(AttachmentError::Retired {
                    uri: row.attachment_uri(project_key),
                });
            }
            Some(row)
        }
    };
    let hash = match (&row, reference) {
        (Some(row), _) => row.content_hash.clone(),
        (None, AttachmentRef::Hash(hash)) => hash.clone(),
        (None, AttachmentRef::Scoped { .. }) => unreachable!("a scoped reference resolved a row"),
    };
    let bytes = fetch_bytes(db, &hash).await?;
    let file_name = row
        .as_ref()
        .map(|row| row.file_name.clone())
        .unwrap_or_else(|| "attachment".to_string());

    let media_type = match row.as_ref().and_then(|row| row.media_type.clone()) {
        Some(media_type) => media_type,
        None => {
            let measured = measure_media_type(&bytes, &file_name);
            if let Some(row) = row.as_ref() {
                backfill_measurement(db, project_id, row, &measured, bytes.len()).await;
            }
            measured
        }
    };
    Ok(FetchedAttachment {
        bytes,
        media_type,
        file_name,
        row,
    })
}

/// Record what a first read measured about a row that predates measurement.
async fn backfill_measurement(
    db: &LocalDb,
    project_id: &str,
    row: &AttachmentRow,
    media_type: &str,
    byte_size: usize,
) {
    let Ok(Some((scope_name, scope_key))) = scope_key_of(db, project_id, &row.scope).await else {
        return;
    };
    if let Err(error) = db
        .execute(
            "UPDATE attachments SET media_type = ?5, byte_size = ?6
             WHERE project_id = ?1 AND scope = ?2 AND scope_key = ?3 AND ordinal = ?4
               AND media_type IS NULL",
            cairn_db::turso::params![
                project_id.to_string(),
                scope_name,
                scope_key,
                row.ordinal as i64,
                media_type.to_string(),
                byte_size as i64,
            ],
        )
        .await
    {
        log::debug!("attachment measurement backfill did not land: {error}");
    }
}

/// The media type an address carries, when that is knowable without fetching
/// the bytes.
///
/// `Ok(None)` means "not yet measured": a hash permalink, which has no record at
/// all, or a row carried over from `image_refs`, whose bytes SQL could not see
/// at migration time. A caller that needs the answer for such an address has to
/// fetch, which measures and backfills it once and for all.
pub async fn peek_media_type(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    reference: &AttachmentRef,
) -> Result<Option<String>, AttachmentError> {
    match reference {
        AttachmentRef::Hash(_) => Ok(None),
        AttachmentRef::Scoped { scope, ordinal } => {
            let row = resolve_attachment(db, project_id, project_key, scope, *ordinal).await?;
            if row.retired_at.is_some() {
                return Err(AttachmentError::Retired {
                    uri: row.attachment_uri(project_key),
                });
            }
            Ok(row.media_type)
        }
    }
}

/// An image's bytes, resolved through an `images/{n}` address.
///
/// An ordinal that exists but does not hold an image is refused by name rather
/// than rendered as a broken image block, and the refusal carries the
/// `attachments/{n}` address that does resolve it.
pub async fn fetch_image(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    reference: &AttachmentRef,
) -> Result<FetchedAttachment, AttachmentError> {
    let fetched = fetch_attachment(db, project_id, project_key, reference).await?;
    if !fetched.media_type.starts_with("image/") {
        return Err(AttachmentError::NotAnImage {
            uri: build_project_image_uri(project_key, reference),
            media_type: fetched.media_type,
            attachment_uri: build_project_attachment_uri(project_key, reference),
        });
    }
    Ok(fetched)
}

/// Every attachment minted in one scope, in ordinal order, retired rows
/// included.
///
/// This is what makes an ordinal a navigable index rather than decoration: an
/// address ending in `5` says four siblings exist, and this enumerates them so
/// the inference is checkable instead of a guess. Ordinals are allocated as
/// `MAX + 1` and rows are never renumbered, so the Nth address is stable for as
/// long as the reference lives — which is why a retired row is listed as retired
/// rather than omitted.
pub async fn list_attachments(
    db: &LocalDb,
    project_id: &str,
    scope: &AttachmentScopeRef,
) -> Result<Vec<AttachmentRow>, AttachmentError> {
    let Some((scope_name, scope_key)) = scope_key_of(db, project_id, scope).await? else {
        return Ok(Vec::new());
    };
    let scope_for_row = scope.clone();
    db.query_all(
        format!(
            "SELECT {ATTACHMENT_COLUMNS} FROM attachments
             WHERE project_id = ?1 AND scope = ?2 AND scope_key = ?3
             ORDER BY ordinal"
        ),
        (project_id.to_string(), scope_name, scope_key),
        move |row| map_attachment_row(&scope_for_row, row),
    )
    .await
    .map_err(|error| AttachmentError::Reference {
        operation: "list",
        message: error.to_string(),
    })
}

/// Retire a reference: its address stops resolving and the reference is marked
/// spent.
///
/// This does NOT delete the object. Nothing in this module deletes from the
/// content store, and that restraint is the correct engineering call rather than
/// caution for its own sake: the private database's `cas_cache` holds a mixture
/// of objects whose authoritative home is that database and read-through cached
/// copies of objects whose home is a per-team broker shared with every other
/// member, so a reference inventory computed inside one database is not
/// authoritative over either. Physical reclamation is blocked on cairn/4648's
/// global inventory.
pub async fn retire_attachment(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    scope: &AttachmentScopeRef,
    ordinal: i32,
) -> Result<AttachmentRow, AttachmentError> {
    let row = resolve_attachment(db, project_id, project_key, scope, ordinal).await?;
    if row.retired_at.is_some() {
        return Ok(row);
    }
    let Some((scope_name, scope_key)) = scope_key_of(db, project_id, scope).await? else {
        return Err(AttachmentError::Unregistered {
            uri: row.attachment_uri(project_key),
        });
    };
    db.execute(
        "UPDATE attachments SET retired_at = unixepoch()
         WHERE project_id = ?1 AND scope = ?2 AND scope_key = ?3 AND ordinal = ?4",
        (
            project_id.to_string(),
            scope_name,
            scope_key,
            ordinal as i64,
        ),
    )
    .await
    .map_err(|error| AttachmentError::Reference {
        operation: "retire",
        message: error.to_string(),
    })?;
    resolve_attachment(db, project_id, project_key, scope, ordinal).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
    use cairn_common::uri::{parse_uri, CairnResource};
    use std::sync::Arc;

    async fn seeded_db() -> (tempfile::TempDir, LocalDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p','w','P','cairn','/tmp/p',1,1)",
            "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
             VALUES ('t1','p','harness','active','none',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        (dir, db)
    }

    fn png() -> Vec<u8> {
        b"\x89PNG\r\n\x1a\nfixture".to_vec()
    }

    fn scope(anchor: AttachmentAnchor) -> AttachmentScope {
        AttachmentScope {
            project_id: "p".into(),
            project_key: "cairn".into(),
            anchor,
        }
    }

    fn thread_anchor() -> AttachmentAnchor {
        AttachmentAnchor::Thread {
            id: "t1".into(),
            name: "harness".into(),
        }
    }

    async fn store(
        db: &LocalDb,
        anchor: AttachmentAnchor,
        bytes: Vec<u8>,
        name: &str,
    ) -> StoredAttachment {
        store_attachment_bytes(
            db,
            &scope(anchor),
            bytes,
            name,
            None,
            &AttachmentProvenance::default(),
        )
        .await
        .unwrap()
    }

    fn reference_of(uri: &str) -> AttachmentRef {
        match parse_uri(uri) {
            Some(CairnResource::ProjectImage { reference, .. })
            | Some(CairnResource::ProjectAttachment { reference, .. }) => reference,
            other => panic!("{uri} did not parse as an attachment: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_minted_image_keeps_its_issue_scoped_image_address() {
        let (_dir, db) = seeded_db().await;
        let stored = store(&db, AttachmentAnchor::Issue(3242), png(), "shot.png").await;
        assert_eq!(stored.uri, "cairn://p/cairn/3242/images/1");
        assert_eq!(stored.kind, AttachmentKind::Image);
        assert_eq!(stored.media_type, "image/png");
        assert!(!stored.uri.contains(&content_hash(&png())));
    }

    #[tokio::test]
    async fn a_minted_file_is_addressed_as_an_attachment() {
        let (_dir, db) = seeded_db().await;
        let stored = store(
            &db,
            AttachmentAnchor::Issue(4647),
            b"a,b\n1,2\n".to_vec(),
            "data.csv",
        )
        .await;
        assert_eq!(stored.uri, "cairn://p/cairn/4647/attachments/1");
        assert_eq!(stored.kind, AttachmentKind::File);
        assert_eq!(stored.media_type, "text/csv");
        assert_eq!(stored.byte_size, 8);
    }

    /// Images and files share one sequence per scope, and the three scopes count
    /// independently.
    #[tokio::test]
    async fn ordinals_advance_per_scope_independently() {
        let (_dir, db) = seeded_db().await;
        let first = store(&db, AttachmentAnchor::Issue(7), png(), "a.png").await;
        let second = store(&db, AttachmentAnchor::Issue(7), png(), "b.png").await;
        let file = store(&db, AttachmentAnchor::Issue(7), b"notes".to_vec(), "n.txt").await;
        let other_issue = store(&db, AttachmentAnchor::Issue(8), png(), "a.png").await;
        let thread = store(&db, thread_anchor(), png(), "a.png").await;
        let projectwide = store(&db, AttachmentAnchor::Project, png(), "a.png").await;

        assert_eq!(first.uri, "cairn://p/cairn/7/images/1");
        // Identical bytes mint a SECOND reference to the one deduped object: a
        // reference records an act of attaching, not a distinct object.
        assert_eq!(second.uri, "cairn://p/cairn/7/images/2");
        // One sequence, whatever the kind.
        assert_eq!(file.uri, "cairn://p/cairn/7/attachments/3");
        assert_eq!(other_issue.uri, "cairn://p/cairn/8/images/1");
        assert_eq!(thread.uri, "cairn://p/cairn/harness/images/1");
        assert_eq!(projectwide.uri, "cairn://p/cairn/images/1");
    }

    #[tokio::test]
    async fn every_minted_form_resolves_back_to_the_bytes() {
        let (_dir, db) = seeded_db().await;
        for anchor in [
            AttachmentAnchor::Issue(3242),
            thread_anchor(),
            AttachmentAnchor::Project,
        ] {
            let stored = store(&db, anchor, png(), "a.png").await;
            let fetched = fetch_attachment(&db, "p", "cairn", &reference_of(&stored.uri))
                .await
                .unwrap();
            assert_eq!(fetched.bytes, png());
            assert_eq!(fetched.media_type, "image/png");
        }
    }

    /// Transcripts and message bodies written before scoped addresses existed
    /// carry the hash form and cannot be rewritten, so it resolves with no
    /// reference row.
    #[tokio::test]
    async fn a_legacy_hash_uri_still_resolves_as_a_permalink() {
        let (_dir, db) = seeded_db().await;
        store(&db, AttachmentAnchor::Issue(1), png(), "a.png").await;
        let reference = AttachmentRef::Hash(content_hash(&png()));
        assert_eq!(
            fetch_attachment(&db, "p", "cairn", &reference)
                .await
                .unwrap()
                .bytes,
            png()
        );
    }

    #[tokio::test]
    async fn an_unminted_reference_is_reported_not_silently_empty() {
        let (_dir, db) = seeded_db().await;
        assert!(matches!(
            resolve_attachment(&db, "p", "cairn", &AttachmentScopeRef::Project, 4).await,
            Err(AttachmentError::Unregistered { .. })
        ));
        // A thread that does not exist is unregistered, not an empty scope.
        assert!(matches!(
            resolve_attachment(
                &db,
                "p",
                "cairn",
                &AttachmentScopeRef::Thread("absent".into()),
                1
            )
            .await,
            Err(AttachmentError::Unregistered { .. })
        ));
    }

    #[tokio::test]
    async fn identical_bytes_are_stored_once_with_distinct_records() {
        let (_dir, db) = seeded_db().await;
        let first = store_attachment_bytes(
            &db,
            &scope(AttachmentAnchor::Issue(1)),
            png(),
            "a.png",
            None,
            &AttachmentProvenance {
                origin_node_uri: Some("cairn://p/cairn/1/1/builder".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let second = store_attachment_bytes(
            &db,
            &scope(AttachmentAnchor::Issue(1)),
            png(),
            "a.png",
            None,
            &AttachmentProvenance {
                origin_node_uri: Some("cairn://p/cairn/1/1/planner".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_ne!(first.uri, second.uri);

        let blobs = db
            .query_all("SELECT COUNT(*) FROM cas_cache", (), |r| {
                Ok(r.get::<i64>(0)?)
            })
            .await
            .unwrap();
        let refs = db
            .query_all("SELECT COUNT(*) FROM attachments", (), |r| {
                Ok(r.get::<i64>(0)?)
            })
            .await
            .unwrap();
        assert_eq!(blobs, vec![1], "content addressing deduplicates bytes");
        assert_eq!(refs, vec![2], "each attach act keeps its own address");

        let origins = list_attachments(&db, "p", &AttachmentScopeRef::Issue(1))
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.origin_node_uri)
            .collect::<Vec<_>>();
        assert_eq!(
            origins,
            vec![
                Some("cairn://p/cairn/1/1/builder".to_string()),
                Some("cairn://p/cairn/1/1/planner".to_string()),
            ],
            "each record keeps its own provenance"
        );
    }

    /// The ordinal is only a navigable index if the collection it indexes is
    /// real: dense from 1, in order, and per-scope.
    #[tokio::test]
    async fn a_scope_enumerates_its_attachments_densely_from_one() {
        let (_dir, db) = seeded_db().await;
        for _ in 0..3 {
            store(&db, AttachmentAnchor::Issue(444), png(), "a.png").await;
        }
        store(&db, thread_anchor(), png(), "a.png").await;
        store(&db, AttachmentAnchor::Project, png(), "a.png").await;

        let issue = list_attachments(&db, "p", &AttachmentScopeRef::Issue(444))
            .await
            .unwrap();
        assert_eq!(
            issue.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "an issue's attachments enumerate densely from 1, in order"
        );
        for (scope, expected) in [
            (AttachmentScopeRef::Thread("harness".into()), vec![1]),
            (AttachmentScopeRef::Project, vec![1]),
        ] {
            let rows = list_attachments(&db, "p", &scope).await.unwrap();
            assert_eq!(
                rows.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
                expected,
                "{scope:?} counts separately from every other scope"
            );
        }
        assert!(
            list_attachments(&db, "p", &AttachmentScopeRef::Issue(999))
                .await
                .unwrap()
                .is_empty(),
            "a scope with no attachments enumerates empty rather than erroring"
        );
    }

    /// An agent may construct a sibling's address instead of looking it up,
    /// which is only safe while the Nth address keeps naming the Nth object.
    #[tokio::test]
    async fn a_constructed_sibling_address_resolves_to_that_sibling() {
        let (_dir, db) = seeded_db().await;
        store(&db, AttachmentAnchor::Issue(444), png(), "a.png").await;
        let csv = b"x,y\n1,2\n".to_vec();
        store(&db, AttachmentAnchor::Issue(444), csv.clone(), "d.csv").await;

        // Built by hand from the collection URI, never returned by a lookup.
        let constructed = format!(
            "{}/2",
            cairn_common::uri::build_project_attachments_uri(
                "cairn",
                &AttachmentScopeRef::Issue(444)
            )
        );
        assert_eq!(
            fetch_attachment(&db, "p", "cairn", &reference_of(&constructed))
                .await
                .unwrap()
                .bytes,
            csv
        );
    }

    #[tokio::test]
    async fn an_unregistered_reference_preserves_the_lowercase_project_key() {
        let (_dir, db) = seeded_db().await;
        let error = resolve_attachment(&db, "p", "cairn", &AttachmentScopeRef::Issue(3242), 1)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "no attachment is registered at cairn://p/cairn/3242/attachments/1"
        );
    }

    /// The `images/{n}` spelling is a claim about the row's kind, so a row that
    /// is not an image is refused by name rather than rendered as a broken
    /// image — and the refusal carries the address that does resolve it.
    #[tokio::test]
    async fn an_image_address_refuses_a_non_image_and_names_the_one_that_works() {
        let (_dir, db) = seeded_db().await;
        store(
            &db,
            AttachmentAnchor::Issue(5),
            b"plain notes".to_vec(),
            "n.txt",
        )
        .await;
        let reference = AttachmentRef::Scoped {
            scope: AttachmentScopeRef::Issue(5),
            ordinal: 1,
        };
        let error = fetch_image(&db, "p", "cairn", &reference)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "cairn://p/cairn/5/images/1 is not an image (text/plain); \
             read it at cairn://p/cairn/5/attachments/1"
        );
    }

    #[tokio::test]
    async fn ingest_enforces_its_bounds_and_sanitizes_its_names() {
        let (_dir, db) = seeded_db().await;
        let oversized = store_attachment_bytes(
            &db,
            &scope(AttachmentAnchor::Project),
            vec![0; MAX_ATTACHMENT_BYTES + 1],
            "huge.bin",
            None,
            &AttachmentProvenance::default(),
        )
        .await;
        assert_eq!(
            oversized,
            Err(AttachmentError::Oversized {
                size: MAX_ATTACHMENT_BYTES + 1
            })
        );
        assert_eq!(
            store_attachment_bytes(
                &db,
                &scope(AttachmentAnchor::Project),
                Vec::new(),
                "empty.txt",
                None,
                &AttachmentProvenance::default(),
            )
            .await,
            Err(AttachmentError::Empty)
        );

        let escaped = store(
            &db,
            AttachmentAnchor::Project,
            b"root:x:0:0".to_vec(),
            "../../etc/passwd",
        )
        .await;
        assert_eq!(escaped.file_name, "passwd");
    }

    /// The declared type is recorded and never consulted: what the bytes are is
    /// what the bytes say.
    #[test]
    fn media_type_is_measured_from_bytes_not_claimed_by_a_name() {
        assert_eq!(measure_media_type(&png(), "notes.txt"), "image/png");
        assert_eq!(
            measure_media_type(b"%PDF-1.7\n", "anything"),
            "application/pdf"
        );
        assert_eq!(measure_media_type(b"a,b\n1,2\n", "data.csv"), "text/csv");
        assert_eq!(
            measure_media_type(b"{\"a\":1}", "x.json"),
            "application/json"
        );
        assert_eq!(measure_media_type(b"hello", "x.unknown"), "text/plain");
        assert_eq!(
            measure_media_type(&[0u8, 1, 2, 3, 200], "x.txt"),
            "application/octet-stream"
        );
    }

    #[test]
    fn filenames_cannot_escape_a_single_path_component() {
        assert_eq!(sanitize_attachment_name("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_attachment_name(".."), "attachment");
        assert_eq!(sanitize_attachment_name(""), "attachment");
        assert_eq!(sanitize_attachment_name("a b;rm -rf.txt"), "a_b_rm_-rf.txt");
    }

    /// The scope discriminator and its typed projections cannot disagree.
    #[tokio::test]
    async fn the_scope_check_rejects_every_mismatched_combination() {
        let (_dir, db) = seeded_db().await;
        for sql in [
            // A thread-scoped row carrying an issue number.
            "INSERT INTO attachments (project_id, scope, scope_key, issue_number, thread_id,
                 ordinal, content_hash, kind, file_name, created_at)
             VALUES ('p','thread','t1',7,'t1',1,'h','file','f',1)",
            // An issue-scoped row whose key disagrees with its number.
            "INSERT INTO attachments (project_id, scope, scope_key, issue_number,
                 ordinal, content_hash, kind, file_name, created_at)
             VALUES ('p','issue','9',7,1,'h','file','f',1)",
            // A project-scoped row with a non-empty key.
            "INSERT INTO attachments (project_id, scope, scope_key,
                 ordinal, content_hash, kind, file_name, created_at)
             VALUES ('p','project','7',1,'h','file','f',1)",
            // A scope the discriminator does not admit.
            "INSERT INTO attachments (project_id, scope, scope_key,
                 ordinal, content_hash, kind, file_name, created_at)
             VALUES ('p','node','',1,'h','file','f',1)",
            // A kind the discriminator does not admit.
            "INSERT INTO attachments (project_id, scope, scope_key,
                 ordinal, content_hash, kind, file_name, created_at)
             VALUES ('p','project','',1,'h','video','f',1)",
        ] {
            assert!(db.execute(sql, ()).await.is_err(), "accepted: {sql}");
        }
    }

    #[tokio::test]
    async fn attribution_is_paired_and_immutable() {
        let (_dir, db) = seeded_db().await;
        assert!(
            db.execute(
                "INSERT INTO attachments (project_id, scope, scope_key, ordinal, content_hash,
                     kind, file_name, created_at, author_principal_json)
                 VALUES ('p','project','',1,'h','file','f',1,'principal')",
                (),
            )
            .await
            .is_err(),
            "a principal without an appearance is refused"
        );
        db.execute(
            "INSERT INTO attachments (project_id, scope, scope_key, ordinal, content_hash,
                 kind, file_name, created_at, author_principal_json, appearance_snapshot_json)
             VALUES ('p','project','',1,'h','file','f',1,'principal','appearance')",
            (),
        )
        .await
        .unwrap();
        for update in [
            "UPDATE attachments SET author_principal_json='changed',
                appearance_snapshot_json='changed' WHERE ordinal = 1",
            "UPDATE attachments SET author_principal_json=NULL,
                appearance_snapshot_json=NULL WHERE ordinal = 1",
        ] {
            assert!(db.execute(update, ()).await.is_err(), "accepted: {update}");
        }
    }

    /// A thread's attachments are the thread's: they go when it goes, and
    /// nothing else moves.
    #[tokio::test]
    async fn deleting_a_thread_takes_only_its_own_attachments() {
        let (_dir, db) = seeded_db().await;
        store(&db, thread_anchor(), png(), "a.png").await;
        store(&db, AttachmentAnchor::Issue(3), png(), "a.png").await;
        store(&db, AttachmentAnchor::Project, png(), "a.png").await;

        db.execute("DELETE FROM threads WHERE id = 't1'", ())
            .await
            .unwrap();

        assert!(
            list_attachments(&db, "p", &AttachmentScopeRef::Thread("harness".into()))
                .await
                .unwrap()
                .is_empty()
        );
        for scope in [AttachmentScopeRef::Issue(3), AttachmentScopeRef::Project] {
            assert_eq!(
                list_attachments(&db, "p", &scope).await.unwrap().len(),
                1,
                "{scope:?} is untouched by a thread deletion"
            );
        }
    }

    /// Every image address minted before this table existed still resolves to
    /// the same object, still as an image.
    #[tokio::test]
    async fn addresses_minted_against_image_refs_still_resolve() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("db")).await.unwrap();
        let before = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.version() != "0227")
            .copied()
            .collect::<Vec<_>>();
        MigrationRunner::new(before).run(&db).await.unwrap();
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p','w','P','cairn','/tmp/p',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        let hash = content_hash(&png());
        db.content_store().put(&hash, &png()).await.unwrap();
        db.execute(
            "INSERT INTO image_refs (project_id, issue_number, ordinal, content_hash, created_at)
             VALUES ('p', 3242, 1, ?1, 5), ('p', 3242, 2, ?1, 6), ('p', 0, 1, ?1, 7)",
            (hash.clone(),),
        )
        .await
        .unwrap();

        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        for uri in [
            "cairn://p/cairn/3242/images/1",
            "cairn://p/cairn/3242/images/2",
            "cairn://p/cairn/images/1",
        ] {
            let fetched = fetch_image(&db, "p", "cairn", &reference_of(uri))
                .await
                .unwrap_or_else(|error| panic!("{uri} stopped resolving: {error}"));
            assert_eq!(fetched.bytes, png(), "{uri}");
            assert_eq!(fetched.media_type, "image/png", "{uri}");
        }

        // The first read measured what SQL could not see at migration time, and
        // recorded it so later reads do not measure again.
        let measured = db
            .query_text(
                "SELECT media_type FROM attachments
                 WHERE project_id='p' AND scope='issue' AND scope_key='3242' AND ordinal=1",
                (),
            )
            .await
            .unwrap();
        assert_eq!(measured.as_deref(), Some("image/png"));
    }

    /// The reason reclamation is out of scope, made concrete: two databases can
    /// reference ONE object through a shared store, so deleting a project — which
    /// cascades away every reference it held — must leave the object alone. A
    /// sweep driven by one database's reference count would have deleted bytes
    /// the other database still resolves, and unrecoverably.
    #[tokio::test]
    async fn an_object_survives_a_project_whose_every_reference_is_gone() {
        use cairn_db::storage::{InMemoryContentStore, TeamReplicaContext};

        let shared = Arc::new(InMemoryContentStore::new());
        let mut dbs = Vec::new();
        let mut dirs = Vec::new();
        for _ in 0..2 {
            let dir = tempfile::tempdir().unwrap();
            let mut db = LocalDb::open(dir.path().join("db")).await.unwrap();
            MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
                .run(&db)
                .await
                .unwrap();
            db.set_team_context(
                TeamReplicaContext {
                    team_id: cairn_db::storage::TeamId::from("team"),
                    private_db: None,
                },
                shared.clone(),
            );
            for sql in [
                "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
                "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('p','w','P','cairn','/tmp/p',1,1)",
            ] {
                db.execute(sql, ()).await.unwrap();
            }
            dbs.push(db);
            dirs.push(dir);
        }

        // The same bytes attached in each database: one object, two references,
        // and neither database can see the other's.
        for db in &dbs {
            store(db, AttachmentAnchor::Issue(1), png(), "a.png").await;
        }
        let hash = content_hash(&png());
        assert!(shared.contains(&hash).await);

        // Delete one project outright. Its references cascade away.
        dbs[0]
            .execute("DELETE FROM projects WHERE id = 'p'", ())
            .await
            .unwrap();
        assert!(
            list_attachments(&dbs[0], "p", &AttachmentScopeRef::Issue(1))
                .await
                .unwrap()
                .is_empty(),
            "the deleted project's references are gone"
        );

        // The object is untouched, and the surviving database still resolves it.
        assert!(shared.contains(&hash).await);
        assert_eq!(fetch_bytes(&dbs[1], &hash).await.unwrap(), png());
        let reference = AttachmentRef::Scoped {
            scope: AttachmentScopeRef::Issue(1),
            ordinal: 1,
        };
        assert_eq!(
            fetch_attachment(&dbs[1], "p", "cairn", &reference)
                .await
                .unwrap()
                .bytes,
            png()
        );
    }

    /// Retirement spends the reference. It does not, and must not, touch the
    /// object: reclamation needs an inventory no single database can compute.
    #[tokio::test]
    async fn retiring_a_reference_stops_the_address_and_keeps_the_object() {
        let (_dir, db) = seeded_db().await;
        let stored = store(&db, AttachmentAnchor::Issue(1), png(), "a.png").await;
        let reference = reference_of(&stored.uri);

        let retired = retire_attachment(&db, "p", "cairn", &AttachmentScopeRef::Issue(1), 1)
            .await
            .unwrap();
        assert!(retired.retired_at.is_some());
        assert!(matches!(
            fetch_attachment(&db, "p", "cairn", &reference).await,
            Err(AttachmentError::Retired { .. })
        ));

        // The record is still enumerable, and the bytes are still there.
        let rows = list_attachments(&db, "p", &AttachmentScopeRef::Issue(1))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "a retired reference is listed, not erased");
        assert_eq!(
            fetch_bytes(&db, &content_hash(&png())).await.unwrap(),
            png(),
            "retirement leaves the object exactly where it was"
        );
    }
}
