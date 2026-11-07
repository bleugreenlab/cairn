//! Where a page's versions live, and the write that appends one.

use cairn_db::turso::params;
use cairn_uri::{build_node_page_uri, build_node_uri, CairnResource, NodeAddress};
use serde_json::Value;

use super::data::{
    compose, output_name, validate, validate_page_name, DEFAULT_ENTRY, PAGE_ARTIFACT_TYPE,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

/// A page's coordinates: its owner's node coordinate (the reserved `(0, 0,
/// thread)` one for a thread) and its name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PageAddress {
    pub project: String,
    pub number: i32,
    pub exec_seq: i32,
    pub node_id: String,
    pub name: String,
}

impl PageAddress {
    /// The page a `NodePage` or `NodePageLog` resource names.
    pub(crate) fn from_resource(resource: &CairnResource) -> Option<Self> {
        match resource {
            CairnResource::NodePage {
                project,
                number,
                exec_seq,
                node_id,
                name,
            }
            | CairnResource::NodePageLog {
                project,
                number,
                exec_seq,
                node_id,
                name,
            } => Some(Self {
                project: project.clone(),
                number: *number,
                exec_seq: *exec_seq,
                node_id: node_id.clone(),
                name: name.clone(),
            }),
            _ => None,
        }
    }

    /// Parse a canonical page URI, through the same thread resolver the read
    /// and write dispatchers use.
    pub(crate) fn parse(uri: &str) -> Result<Self, String> {
        let refusal =
            || format!("`{uri}` is not a canonical page URI (cairn://p/PROJECT/THREAD/pages/NAME)");
        let resource = cairn_uri::parse_uri(uri.trim()).ok_or_else(refusal)?;
        let resource = cairn_uri::delegate_thread_descendant(resource)?;
        match resource {
            CairnResource::NodePage { .. } => Self::from_resource(&resource).ok_or_else(refusal),
            _ => Err(refusal()),
        }
    }

    pub(crate) fn uri(&self) -> String {
        build_node_page_uri(
            &self.project,
            self.number,
            self.exec_seq,
            &self.node_id,
            &self.name,
        )
    }

    /// The owner's canonical home: the thread, or the issue node of a draft.
    pub(crate) fn home_uri(&self) -> String {
        build_node_uri(&self.project, self.number, self.exec_seq, &self.node_id)
    }

    pub(crate) fn thread_name(&self) -> Option<&str> {
        match NodeAddress::new(self.number, self.exec_seq, &self.node_id) {
            NodeAddress::Thread { name } => Some(name),
            NodeAddress::Node { .. } => None,
        }
    }
}

/// Who owns a page's version chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PageOwner {
    /// Every session job the thread has had: a page outlives its sessions.
    Thread { thread_id: String },
    /// One issue node's job: a draft.
    Node { job_id: String },
}

impl PageOwner {
    /// The `WHERE` clause selecting the owner's rows, bound to `?1`. A thread's
    /// rows are those under its SESSION jobs; its tasks carry its id too, but a
    /// page is only ever stored under the session.
    fn scope(&self) -> (String, String) {
        match self {
            Self::Thread { thread_id } => (
                format!("j.thread_id = ?1 AND {}", crate::threads::SESSION_JOB_SHAPE),
                thread_id.clone(),
            ),
            Self::Node { job_id } => ("a.job_id = ?1".to_string(), job_id.clone()),
        }
    }
}

/// One stored version of a page.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PageRow {
    pub id: String,
    pub job_id: String,
    pub version: i32,
    pub parent_version_id: Option<String>,
    pub data: Value,
    pub created_at: i64,
    pub updated_at: i64,
}

const ROW_COLUMNS: &str =
    "a.id, a.job_id, a.version, a.parent_version_id, a.data, a.created_at, a.updated_at";

fn page_row(row: &cairn_db::turso::Row) -> DbResult<PageRow> {
    let data = row.text(4)?;
    Ok(PageRow {
        id: row.text(0)?,
        job_id: row.text(1)?,
        version: row.i64(2)? as i32,
        parent_version_id: row.opt_text(3)?,
        data: serde_json::from_str(&data)
            .map_err(|error| DbError::Row(format!("stored page data is not JSON: {error}")))?,
        created_at: row.i64(5)?,
        updated_at: row.i64(6)?,
    })
}

/// The owner of the page at `address`, or why there is none.
async fn resolve_owner_conn(
    conn: &cairn_db::storage::TrackedConnection,
    address: &PageAddress,
) -> DbResult<Result<PageOwner, String>> {
    match address.thread_name() {
        Some(thread) => {
            match crate::threads::thread_id_for_coordinate_conn(conn, &address.project, thread)
                .await
            {
                Ok(thread_id) => Ok(Ok(PageOwner::Thread { thread_id })),
                Err(DbError::Internal(message)) => Ok(Err(message)),
                Err(error) => Err(error),
            }
        }
        None => Ok(crate::jobs::queries::job_id_for_node_coordinate_conn(
            conn,
            &address.project,
            address.number,
            address.exec_seq,
            &address.node_id,
            None,
        )
        .await?
        .map(|job_id| PageOwner::Node { job_id })
        .ok_or_else(|| format!("No node found at {}", address.home_uri()))),
    }
}

/// The newest version of `name` in the owner's chain, or exactly `version`.
async fn page_row_conn(
    conn: &cairn_db::storage::TrackedConnection,
    owner: &PageOwner,
    name: &str,
    version: Option<i32>,
) -> DbResult<Option<PageRow>> {
    let (clause, scope) = owner.scope();
    let output = output_name(name);
    let sql = format!(
        "SELECT {ROW_COLUMNS} FROM artifacts a JOIN jobs j ON j.id = a.job_id
         WHERE {clause} AND a.artifact_type = '{PAGE_ARTIFACT_TYPE}' AND a.output_name = ?2 {}
         ORDER BY a.version DESC, a.created_at DESC LIMIT 1",
        if version.is_some() {
            "AND a.version = ?3"
        } else {
            ""
        }
    );
    let mut rows = match version {
        Some(version) => conn.query(&sql, params![scope, output, version]).await?,
        None => conn.query(&sql, params![scope, output]).await?,
    };
    rows.next().await?.map(|row| page_row(&row)).transpose()
}

/// The page at `address` (its newest version, or `version`), with its owner.
/// `Ok(None)` when the owner exists and holds no such page.
pub(crate) async fn load_page(
    db: &LocalDb,
    address: &PageAddress,
    version: Option<i32>,
) -> Result<Option<(PageOwner, PageRow)>, String> {
    let address = address.clone();
    db.read(move |conn| {
        let address = address.clone();
        Box::pin(async move {
            let owner = match resolve_owner_conn(conn, &address).await? {
                Ok(owner) => owner,
                Err(message) => return Ok(Err(message)),
            };
            let row = page_row_conn(conn, &owner, &address.name, version).await?;
            Ok(Ok(row.map(|row| (owner, row))))
        })
    })
    .await
    .map_err(|error| format!("Failed to load page: {error}"))?
}

/// A job's named artifact, with a thread's pages resolved across its sessions.
///
/// The app addresses a named artifact by `(job, output name)`. A thread page's
/// chain spans every session job the thread has had, while the thread pane
/// roots its tab strip at the current session, so it asks the current session
/// for a page whose newest version may sit under an earlier one. A `pages/`
/// name on a thread session job therefore resolves through the thread's chain;
/// every other name, and every other job, is the job's own chain.
pub async fn named_artifact_for_job(
    db: &LocalDb,
    job_id: &str,
    output_name: &str,
) -> Result<Option<crate::models::Artifact>, String> {
    if let Some(name) = output_name.strip_prefix("pages/") {
        let (job, name) = (job_id.to_string(), name.to_string());
        let newest = db
            .read(move |conn| {
                let (job, name) = (job.clone(), name.clone());
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            &format!(
                                "SELECT j.thread_id FROM jobs j
                                 WHERE j.id = ?1 AND j.thread_id IS NOT NULL
                                   AND {}",
                                crate::threads::SESSION_JOB_SHAPE
                            ),
                            params![job.as_str()],
                        )
                        .await?;
                    let Some(thread_id) = rows.next().await?.map(|row| row.text(0)).transpose()?
                    else {
                        return Ok(None);
                    };
                    let owner = PageOwner::Thread { thread_id };
                    Ok(page_row_conn(conn, &owner, &name, None)
                        .await?
                        .map(|row| row.id))
                })
            })
            .await
            .map_err(|error| format!("Failed to resolve the thread's page: {error}"))?;
        if let Some(artifact_id) = newest {
            return crate::artifacts::queries::get(db, &artifact_id)
                .await
                .map(Some);
        }
    }
    crate::artifacts::queries::get_named(db, job_id, output_name).await
}

/// The newest version of every page the owner at `address`'s coordinate holds,
/// by name. `address.name` is ignored.
pub(crate) async fn list_owner_pages(
    db: &LocalDb,
    address: &PageAddress,
) -> Result<Vec<(String, PageRow)>, String> {
    let address = address.clone();
    db.read(move |conn| {
        let address = address.clone();
        Box::pin(async move {
            let owner = match resolve_owner_conn(conn, &address).await? {
                Ok(owner) => owner,
                Err(message) => return Ok(Err(message)),
            };
            let (clause, scope) = owner.scope();
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT {ROW_COLUMNS}, a.output_name FROM artifacts a
                         JOIN jobs j ON j.id = a.job_id
                         WHERE {clause} AND a.artifact_type = '{PAGE_ARTIFACT_TYPE}'
                           AND a.output_name LIKE 'pages/%'
                         ORDER BY a.output_name, a.version DESC, a.created_at DESC"
                    ),
                    params![scope],
                )
                .await?;
            let mut pages: Vec<(String, PageRow)> = Vec::new();
            while let Some(row) = rows.next().await? {
                let output = row.text(7)?;
                let name = output.trim_start_matches("pages/").to_string();
                if pages.last().is_some_and(|(last, _)| *last == name) {
                    continue;
                }
                pages.push((name, page_row(&row)?));
            }
            Ok(Ok(pages))
        })
    })
    .await
    .map_err(|error| format!("Failed to load pages: {error}"))?
}

/// Who is writing a page: an authenticated run, by the job it belongs to.
///
/// There is no run-less writer. A request without a run identity cannot be
/// told apart from an agent shell that dropped its own, and a page is
/// agent-authored by definition; an operator path, if one is ever wanted, has
/// to come from a verified credential instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PageWriter {
    pub job_id: String,
}

/// Whether `writer` may write a page `owner` holds.
///
/// A page reads with its owner's authority, so whoever writes it chooses what
/// that authority reads and which declared hosts it may reach. Only the owner
/// writes it: a thread's own jobs (its sessions and their tasks all carry its
/// id), or, for a draft, the issue node's own job and the tasks it spawned.
async fn may_write_conn(
    conn: &cairn_db::storage::TrackedConnection,
    owner: &PageOwner,
    writer: &PageWriter,
) -> DbResult<bool> {
    let job_id = &writer.job_id;
    let (sql, owner_key) = match owner {
        PageOwner::Thread { thread_id } => (
            "SELECT 1 FROM jobs WHERE id = ?1 AND thread_id = ?2",
            thread_id,
        ),
        PageOwner::Node { job_id: node } => (
            "SELECT 1 FROM jobs WHERE id = ?1 AND (id = ?2 OR parent_job_id = ?2)",
            node,
        ),
    };
    let mut rows = conn
        .query(sql, params![job_id.as_str(), owner_key.as_str()])
        .await?;
    Ok(rows.next().await?.is_some())
}

fn not_the_owner(address: &PageAddress) -> String {
    format!(
        "{} is not this run's to write. A page reads with its owner's authority, so only its \
         owning thread (or, for a draft, its own issue node) writes it. Send the page's source to \
         the owner and let it write the page.",
        address.uri()
    )
}

/// A draft has no thread database of its own, and no owning thread has
/// adopted it, so it declares no writes.
fn refuse_draft_writes(address: &PageAddress, page: &Value) -> Result<(), String> {
    if address.thread_name().is_none() && !super::data::write_scopes(page).is_empty() {
        return Err(
            "a draft page cannot write; its thread adopts it first. Remove manifest.writes from the draft"
                .to_string(),
        );
    }
    Ok(())
}

struct StoredPage {
    row: PageRow,
    draft: bool,
}

/// Compose, validate, and append the next version, all inside one write
/// transaction so the version number and the patch base are the same read.
/// A refusal returns before a thread session is minted, so an invalid write
/// leaves nothing behind.
async fn store_conn(
    conn: &cairn_db::storage::TrackedConnection,
    address: &PageAddress,
    writer: &PageWriter,
    payload: &Value,
    is_patch: bool,
) -> DbResult<Result<StoredPage, String>> {
    let owner = match resolve_owner_conn(conn, address).await? {
        Ok(owner) => owner,
        Err(message) => return Ok(Err(message)),
    };
    if !may_write_conn(conn, &owner, writer).await? {
        return Ok(Err(not_the_owner(address)));
    }
    let latest = page_row_conn(conn, &owner, &address.name, None).await?;
    let page = match compose(
        latest.as_ref().map(|row| &row.data),
        payload,
        is_patch,
        DEFAULT_ENTRY,
    )
    .and_then(|page| validate(&page).map(|()| page))
    .and_then(|page| refuse_draft_writes(address, &page).map(|()| page))
    {
        Ok(page) => page,
        Err(message) => return Ok(Err(message)),
    };
    let (job_id, draft) = match &owner {
        PageOwner::Thread { thread_id } => (
            crate::threads::ensure_thread_session_conn(conn, thread_id, None).await?,
            false,
        ),
        PageOwner::Node { job_id } => (job_id.clone(), true),
    };
    let version = latest.as_ref().map_or(1, |row| row.version + 1);
    let parent_version_id = latest.map(|row| row.id);
    let now = chrono::Utc::now().timestamp();
    let id = cairn_common::ids::mint_child(&job_id);
    let data = serde_json::to_string(&page)
        .map_err(|error| DbError::Internal(format!("page data does not serialize: {error}")))?;
    conn.execute(
        "INSERT INTO artifacts (
             id, job_id, artifact_type, schema_version, data, version,
             parent_version_id, output_name, confirmed, created_at, updated_at
         ) VALUES (?1, ?2, ?3, 1, ?4, ?5, ?6, ?7, 1, ?8, ?8)",
        params![
            id.as_str(),
            job_id.as_str(),
            PAGE_ARTIFACT_TYPE,
            data.as_str(),
            version,
            parent_version_id.as_deref(),
            output_name(&address.name).as_str(),
            now
        ],
    )
    .await?;
    Ok(Ok(StoredPage {
        row: PageRow {
            id,
            job_id,
            version,
            parent_version_id,
            data: page,
            created_at: now,
            updated_at: now,
        },
        draft,
    }))
}

/// Write (`create`) or revise (`patch`) the page at `address`, appending a
/// version. A dry run composes and validates against the stored page and
/// writes nothing.
pub(crate) async fn write_page(
    orch: &Orchestrator,
    address: &PageAddress,
    writer: &PageWriter,
    payload: &Value,
    is_patch: bool,
    dry_run: bool,
) -> Result<String, String> {
    validate_page_name(&address.name)?;
    let db = orch.db.for_project(&address.project).await;
    let uri = address.uri();
    if dry_run {
        // A preview answers what the write would: an owner check it skipped
        // would make "would write" the evidence for a write that is refused.
        let (address_in, writer_in) = (address.clone(), writer.clone());
        let allowed = db
            .read(move |conn| {
                let (address, writer) = (address_in.clone(), writer_in.clone());
                Box::pin(async move {
                    Ok(match resolve_owner_conn(conn, &address).await? {
                        Ok(owner) => Ok(may_write_conn(conn, &owner, &writer).await?),
                        Err(message) => Err(message),
                    })
                })
            })
            .await
            .map_err(|error| format!("Failed to resolve the page's owner: {error}"))??;
        if !allowed {
            return Err(not_the_owner(address));
        }
        let base = load_page(&db, address, None)
            .await?
            .map(|(_, row)| row.data);
        let page = compose(base.as_ref(), payload, is_patch, DEFAULT_ENTRY)?;
        validate(&page)?;
        refuse_draft_writes(address, &page)?;
        let verb = if is_patch { "patch" } else { "write" };
        return Ok(format!("Would {verb} page {uri}"));
    }

    let (address_in, writer_in, payload_in) = (address.clone(), writer.clone(), payload.clone());
    let stored = db
        .write(move |conn| {
            let (address, writer, payload) =
                (address_in.clone(), writer_in.clone(), payload_in.clone());
            Box::pin(async move { store_conn(conn, &address, &writer, &payload, is_patch).await })
        })
        .await
        .map_err(|error| format!("Failed to store page: {error}"))??;

    let row = &stored.row;
    let artifact = crate::models::Artifact {
        id: row.id.clone(),
        job_id: Some(row.job_id.clone()),
        artifact_type: PAGE_ARTIFACT_TYPE.to_string(),
        schema_version: 1,
        data: row.data.clone(),
        version: row.version,
        parent_version_id: row.parent_version_id.clone(),
        output_name: Some(output_name(&address.name)),
        created_at: row.created_at,
        updated_at: row.updated_at,
        seen_at: None,
        confirmed: true,
    };
    orch.notifier.artifact(&db, &artifact).await;
    log::info!(
        "Page written: uri={uri}, job_id={}, version={}",
        row.job_id,
        row.version
    );

    let verb = if is_patch { "patched" } else { "wrote" };
    let mut summary = format!(
        "Page {verb} ({uri}, version {}). An open facet remounts on this version; runtime errors and refusals its frames report land at {uri}/log.",
        row.version
    );
    if stored.draft {
        summary.push_str(
            " This page is a draft on an issue node: it renders on the node and is never listed in the project's Pages index.",
        );
    }
    Ok(summary)
}
