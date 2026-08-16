//! Stored images: validation, minting a public reference, and fetching bytes.
//!
//! A stored image has two identities and they are not the same thing.
//!
//! Its *storage identity* is the sha256 of its bytes. That is the content-store
//! key, and it is what makes dedup across members and integrity verification on
//! fetch work. It stays exactly as it was.
//!
//! Its *address* is what a person and an agent read and write: a short ordinal
//! scoped to where the image entered the system
//! (`cairn://p/KEY/{issue}/images/{n}`). A hash is a terrible address — it says
//! nothing about where the image came from and puts 64 characters of noise in
//! front of every reader of a transcript. [`mint_image_uri`] allocates the
//! address; an `image_refs` row is the mapping between the two.
//!
//! Both address forms resolve. The hash form is never minted again, but it is
//! burned into transcripts and message bodies that cannot be rewritten, so
//! [`resolve_image_hash`] accepts it forever as a permalink.

use crate::storage::{content_store::content_hash, LocalDb};
use base64::Engine;
use cairn_common::uri::{build_project_image_uri, ImageRef};
pub const MAX_IMAGE_BYTES: usize = 5 * 1024 * 1024;

/// The project-scoped fallback's stand-in issue number.
///
/// Issue numbers are always positive, so 0 cannot collide with a real one. A
/// sentinel rather than NULL is what lets the `image_refs` primary key enforce
/// per-scope ordinal uniqueness: SQLite does not compare NULLs as equal.
const NO_ISSUE: i32 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredImage {
    pub bytes: Vec<u8>,
    pub mime_type: &'static str,
}

/// Where a newly minted image reference is anchored.
///
/// `issue_number` is `Some` for the overwhelmingly common case — a paste into a
/// chat or an issue, or an agent reading an image mid-run, all happen inside one
/// issue's world. It is `None` only where there genuinely is no issue yet, such
/// as a paste into the create-issue dialog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageScope {
    pub project_id: String,
    pub project_key: String,
    pub issue_number: Option<i32>,
}

impl ImageScope {
    pub fn new(project_id: impl Into<String>, project_key: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            project_key: project_key.into(),
            issue_number: None,
        }
    }

    pub fn in_issue(mut self, number: Option<i32>) -> Self {
        self.issue_number = number.filter(|value| *value > 0);
        self
    }

    /// Scope this to the issue a job belongs to.
    ///
    /// A chat composer knows which job it addresses, not which issue: the issue
    /// is a database fact, so it is resolved here rather than reconstructed by
    /// the caller from a display string. A job with no issue (or an id that
    /// resolves to nothing) leaves the scope project-wide, which is the honest
    /// answer rather than a guess.
    pub async fn in_issue_of_job(self, db: &LocalDb, job_id: &str) -> Self {
        let number = db
            .query_opt_i64(
                "SELECT issues.number FROM jobs
                 JOIN issues ON jobs.issue_id = issues.id
                 WHERE jobs.id = ?1
                 LIMIT 1",
                (job_id.to_string(),),
            )
            .await
            .unwrap_or_default()
            .map(|value| value as i32);
        self.in_issue(number)
    }

    fn scope_key(&self) -> i32 {
        self.issue_number.unwrap_or(NO_ISSUE)
    }

    fn reference(&self, ordinal: i32) -> ImageRef {
        match self.issue_number {
            Some(number) => ImageRef::Issue { number, ordinal },
            None => ImageRef::Project { ordinal },
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ImageError {
    #[error("image data is empty")]
    Empty,
    #[error("image base64 is malformed: {0}")]
    MalformedBase64(String),
    #[error("image exceeds the 5 MiB limit ({size} bytes)")]
    Oversized { size: usize },
    #[error("unsupported image format")]
    Unsupported,
    #[error("image {hash} was not found")]
    Missing { hash: String },
    /// A public URI with no reference row. Either it was never minted, or it
    /// belongs to a project this database does not own.
    #[error("no stored image is registered at {uri}")]
    Unregistered { uri: String },
    #[error("image {hash} failed its content hash integrity check")]
    Corrupt { hash: String },
    #[error("content store {operation} failed: {message}")]
    Store {
        operation: &'static str,
        message: String,
    },
    #[error("image reference {operation} failed: {message}")]
    Reference {
        operation: &'static str,
        message: String,
    },
}

pub fn detect_image_mime(b: &[u8]) -> Option<&'static str> {
    if b.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if b.starts_with(b"\xff\xd8\xff") {
        Some("image/jpeg")
    } else if b.starts_with(b"GIF87a") || b.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if b.len() >= 12 && &b[..4] == b"RIFF" && &b[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}

fn validate(bytes: Vec<u8>) -> Result<StoredImage, ImageError> {
    if bytes.is_empty() {
        return Err(ImageError::Empty);
    }
    if bytes.len() > MAX_IMAGE_BYTES {
        return Err(ImageError::Oversized { size: bytes.len() });
    }
    let mime_type = detect_image_mime(&bytes).ok_or(ImageError::Unsupported)?;
    Ok(StoredImage { bytes, mime_type })
}

/// Allocate the next ordinal in `scope` and record it against `hash`.
///
/// The read of the current maximum and the insert share one write transaction,
/// so two concurrent pastes into the same issue cannot mint the same ordinal;
/// the primary key is the backstop if they somehow did.
///
/// Identical bytes pasted twice mint two references to one blob. That is
/// deliberate: a reference records an act of attaching an image, and the dedup
/// that matters already happened in the content store.
async fn mint_reference(db: &LocalDb, scope: &ImageScope, hash: &str) -> Result<i32, ImageError> {
    let project_id = scope.project_id.clone();
    let scope_key = scope.scope_key();
    let hash = hash.to_string();
    db.write(move |conn| {
        let project_id = project_id.clone();
        let hash = hash.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COALESCE(MAX(ordinal), 0) FROM image_refs
                     WHERE project_id = ?1 AND issue_number = ?2",
                    (project_id.clone(), scope_key as i64),
                )
                .await?;
            let previous = match rows.next().await? {
                Some(row) => row.get::<i64>(0).unwrap_or(0),
                None => 0,
            };
            let ordinal = previous + 1;
            conn.execute(
                "INSERT INTO image_refs
                     (project_id, issue_number, ordinal, content_hash, created_at)
                 VALUES (?1, ?2, ?3, ?4, unixepoch())",
                (project_id, scope_key as i64, ordinal, hash),
            )
            .await?;
            Ok(ordinal as i32)
        })
    })
    .await
    .map_err(|error| ImageError::Reference {
        operation: "mint",
        message: error.to_string(),
    })
}

/// Store `bytes` and return the public URI that addresses them.
pub async fn store_image_bytes(
    db: &LocalDb,
    scope: &ImageScope,
    bytes: Vec<u8>,
) -> Result<String, ImageError> {
    let image = validate(bytes)?;
    let hash = content_hash(&image.bytes);
    db.content_store()
        .put(&hash, &image.bytes)
        .await
        .map_err(|message| ImageError::Store {
            operation: "write",
            message,
        })?;
    let ordinal = mint_reference(db, scope, &hash).await?;
    Ok(build_project_image_uri(
        &scope.project_key,
        &scope.reference(ordinal),
    ))
}

pub async fn store_image(
    db: &LocalDb,
    scope: &ImageScope,
    base64_data: &str,
) -> Result<String, ImageError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(base64_data)
        .map_err(|e| ImageError::MalformedBase64(e.to_string()))?;
    store_image_bytes(db, scope, bytes).await
}

/// The content-store key a public image reference addresses.
///
/// A hash reference IS the key — that is the permalink form, and it resolves
/// without a lookup so an image referenced from a transcript older than the
/// reference table still renders. A friendly reference is resolved through
/// `image_refs`.
pub async fn resolve_image_hash(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    reference: &ImageRef,
) -> Result<String, ImageError> {
    let (issue_number, ordinal) = match reference {
        ImageRef::Hash(hash) => return Ok(hash.clone()),
        ImageRef::Issue { number, ordinal } => (*number, *ordinal),
        ImageRef::Project { ordinal } => (NO_ISSUE, *ordinal),
    };
    db.query_text(
        "SELECT content_hash FROM image_refs
         WHERE project_id = ?1 AND issue_number = ?2 AND ordinal = ?3
         LIMIT 1",
        (project_id.to_string(), issue_number as i64, ordinal as i64),
    )
    .await
    .map_err(|error| ImageError::Reference {
        operation: "resolve",
        message: error.to_string(),
    })?
    .ok_or_else(|| ImageError::Unregistered {
        uri: build_project_image_uri(project_key, reference),
    })
}

pub async fn fetch_image(db: &LocalDb, hash: &str) -> Result<StoredImage, ImageError> {
    let bytes = db
        .content_store()
        .get(hash)
        .await
        .map_err(|message| ImageError::Store {
            operation: "read",
            message,
        })?
        .ok_or_else(|| ImageError::Missing {
            hash: hash.to_string(),
        })?;
    if content_hash(&bytes) != hash {
        return Err(ImageError::Corrupt {
            hash: hash.to_string(),
        });
    }
    validate(bytes)
}

/// One row of an image scope's enumeration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRefRow {
    pub ordinal: i32,
    pub created_at: i64,
}

/// Every image minted in one scope, in ordinal order.
///
/// This is what makes an ordinal a navigable index rather than decoration: an
/// address ending in `5` says four siblings exist, and this enumerates them so
/// the inference is checkable instead of a guess. Ordinals are allocated as
/// `MAX + 1` and rows are never renumbered, so the Nth address is stable for as
/// long as the reference lives.
pub async fn list_image_refs(
    db: &LocalDb,
    project_id: &str,
    issue_number: Option<i32>,
) -> Result<Vec<ImageRefRow>, ImageError> {
    db.query_all(
        "SELECT ordinal, created_at FROM image_refs
         WHERE project_id = ?1 AND issue_number = ?2
         ORDER BY ordinal",
        (
            project_id.to_string(),
            issue_number.unwrap_or(NO_ISSUE) as i64,
        ),
        |row| {
            Ok(ImageRefRow {
                ordinal: row.get::<i64>(0)? as i32,
                created_at: row.get::<i64>(1)?,
            })
        },
    )
    .await
    .map_err(|error| ImageError::Reference {
        operation: "list",
        message: error.to_string(),
    })
}

/// Resolve a public image reference all the way to its bytes.
pub async fn fetch_image_by_reference(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    reference: &ImageRef,
) -> Result<StoredImage, ImageError> {
    let hash = resolve_image_hash(db, project_id, project_key, reference).await?;
    fetch_image(db, &hash).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};

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
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        (dir, db)
    }

    fn png() -> Vec<u8> {
        b"\x89PNG\r\n\x1a\nfixture".to_vec()
    }

    fn scope(issue: Option<i32>) -> ImageScope {
        ImageScope::new("p", "cairn").in_issue(issue)
    }

    #[tokio::test]
    async fn a_minted_uri_carries_its_issue_and_no_hash() {
        let (_dir, db) = seeded_db().await;
        let uri = store_image_bytes(&db, &scope(Some(3242)), png())
            .await
            .unwrap();
        assert_eq!(uri, "cairn://p/cairn/3242/images/1");
        assert!(!uri.contains(&content_hash(&png())));
    }

    #[tokio::test]
    async fn ordinals_advance_per_scope_independently() {
        let (_dir, db) = seeded_db().await;
        let first = store_image_bytes(&db, &scope(Some(7)), png())
            .await
            .unwrap();
        let second = store_image_bytes(&db, &scope(Some(7)), png())
            .await
            .unwrap();
        let other_issue = store_image_bytes(&db, &scope(Some(8)), png())
            .await
            .unwrap();
        let projectwide = store_image_bytes(&db, &scope(None), png()).await.unwrap();

        assert_eq!(first, "cairn://p/cairn/7/images/1");
        // Identical bytes mint a SECOND reference to the one deduped blob:
        // a reference records an act of attaching, not a distinct object.
        assert_eq!(second, "cairn://p/cairn/7/images/2");
        assert_eq!(other_issue, "cairn://p/cairn/8/images/1");
        assert_eq!(projectwide, "cairn://p/cairn/images/1");
    }

    #[tokio::test]
    async fn every_minted_form_resolves_back_to_the_bytes() {
        let (_dir, db) = seeded_db().await;
        for issue in [Some(3242), None] {
            let uri = store_image_bytes(&db, &scope(issue), png()).await.unwrap();
            let reference = match cairn_common::uri::parse_uri(&uri) {
                Some(cairn_common::uri::CairnResource::ProjectImage { reference, .. }) => reference,
                other => panic!("{uri} did not parse as an image: {other:?}"),
            };
            let image = fetch_image_by_reference(&db, "p", "cairn", &reference)
                .await
                .unwrap();
            assert_eq!(image.bytes, png());
            assert_eq!(image.mime_type, "image/png");
        }
    }

    #[tokio::test]
    async fn a_legacy_hash_uri_still_resolves_as_a_permalink() {
        // Transcripts and message bodies written before this change carry the
        // hash form and cannot be rewritten, so it resolves with no reference row.
        let (_dir, db) = seeded_db().await;
        store_image_bytes(&db, &scope(Some(1)), png())
            .await
            .unwrap();
        let reference = ImageRef::Hash(content_hash(&png()));
        assert_eq!(
            fetch_image_by_reference(&db, "p", "cairn", &reference)
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
            resolve_image_hash(&db, "p", "cairn", &ImageRef::Project { ordinal: 4 }).await,
            Err(ImageError::Unregistered { .. })
        ));
    }

    #[tokio::test]
    async fn identical_bytes_are_stored_once() {
        let (_dir, db) = seeded_db().await;
        store_image_bytes(&db, &scope(Some(1)), png())
            .await
            .unwrap();
        store_image_bytes(&db, &scope(Some(1)), png())
            .await
            .unwrap();
        let blobs = db
            .query_all("SELECT COUNT(*) FROM cas_cache", (), |r| {
                Ok(r.get::<i64>(0)?)
            })
            .await
            .unwrap();
        let refs = db
            .query_all("SELECT COUNT(*) FROM image_refs", (), |r| {
                Ok(r.get::<i64>(0)?)
            })
            .await
            .unwrap();
        assert_eq!(
            blobs,
            vec![1],
            "content addressing still deduplicates bytes"
        );
        assert_eq!(refs, vec![2], "each attach act keeps its own address");
    }

    // The ordinal is only a navigable index if the collection it indexes is
    // real: dense from 1, in order, and per-scope.
    #[tokio::test]
    async fn a_scope_enumerates_its_images_densely_from_one() {
        let (_dir, db) = seeded_db().await;
        for _ in 0..3 {
            store_image_bytes(&db, &scope(Some(444)), png())
                .await
                .unwrap();
        }
        store_image_bytes(&db, &scope(None), png()).await.unwrap();

        let issue = list_image_refs(&db, "p", Some(444)).await.unwrap();
        assert_eq!(
            issue.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "an issue's images enumerate densely from 1, in order"
        );
        let project = list_image_refs(&db, "p", None).await.unwrap();
        assert_eq!(
            project.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
            vec![1],
            "the project scope counts separately from any issue's"
        );
        assert!(
            list_image_refs(&db, "p", Some(999))
                .await
                .unwrap()
                .is_empty(),
            "a scope with no images enumerates empty rather than erroring"
        );
    }

    // An agent may construct a sibling's address instead of looking it up, which
    // is only safe while the Nth address keeps naming the Nth image.
    #[tokio::test]
    async fn a_constructed_sibling_address_resolves_to_that_sibling() {
        let (_dir, db) = seeded_db().await;
        let first = store_image_bytes(&db, &scope(Some(444)), png())
            .await
            .unwrap();
        let gif: Vec<u8> = b"GIF89a-second".to_vec();
        let second = store_image_bytes(&db, &scope(Some(444)), gif.clone())
            .await
            .unwrap();

        assert_eq!(first, "cairn://p/cairn/444/images/1");
        assert_eq!(second, "cairn://p/cairn/444/images/2");

        // Built by hand from the collection URI, never returned by a lookup.
        let constructed = format!(
            "{}/2",
            cairn_common::uri::build_project_images_uri("cairn", Some(444))
        );
        let Some(cairn_common::uri::CairnResource::ProjectImage { reference, .. }) =
            cairn_common::uri::parse_uri(&constructed)
        else {
            panic!("a constructed sibling address must parse: {constructed}");
        };
        assert_eq!(
            fetch_image_by_reference(&db, "p", "cairn", &reference)
                .await
                .unwrap()
                .bytes,
            gif
        );
    }

    #[tokio::test]
    async fn an_unregistered_reference_preserves_the_lowercase_project_key() {
        let (_dir, db) = seeded_db().await;
        let error = resolve_image_hash(
            &db,
            "p",
            "cairn",
            &ImageRef::Issue {
                number: 3242,
                ordinal: 1,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "no stored image is registered at cairn://p/cairn/3242/images/1"
        );
    }

    #[test]
    fn validation_enforces_type_and_size() {
        assert_eq!(
            validate(vec![0; MAX_IMAGE_BYTES + 1]),
            Err(ImageError::Oversized {
                size: MAX_IMAGE_BYTES + 1
            })
        );
        assert_eq!(validate(b"no".to_vec()), Err(ImageError::Unsupported));
    }
}
