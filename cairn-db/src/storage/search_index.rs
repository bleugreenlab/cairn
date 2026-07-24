use std::ops::Bound;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

use crate::models::{SearchContentType, SearchFilters};
use serde_json::Value as JsonValue;
use tantivy::collector::TopDocs;
use tantivy::query::{
    BooleanQuery, BoostQuery, ConstScoreQuery, FuzzyTermQuery, Occur, Query, RangeQuery, TermQuery,
};
use tantivy::schema::{
    Field, IndexRecordOption, NumericOptions, Schema, TantivyDocument, Value, STORED, STRING, TEXT,
};
use tantivy::snippet::SnippetGenerator;
use tantivy::tokenizer::TokenStream;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, Term};

use super::{DbError, DbResult, LocalDb, RowExt};

const INDEX_WRITER_MEMORY_BUDGET: usize = 50_000_000;

/// Multiplier applied to title-field matches so a hit in the title outranks the
/// same term appearing only in the body.
const TITLE_BOOST: tantivy::Score = 2.0;

/// Ceiling of the additive recency term (in BM25 score points). Kept well below
/// a single point so relevance differences dominate and recency only breaks
/// ties between near-equal scores.
const RECENCY_WEIGHT: f32 = 0.5;

/// Recency half-life in seconds (~30 days): a document this old contributes half
/// the recency bonus of a brand-new one.
const RECENCY_HALF_LIFE_SECS: f64 = 60.0 * 60.0 * 24.0 * 30.0;

#[derive(Debug, Clone)]
pub struct SearchIndexHit {
    pub id: String,
    pub content_type: SearchContentType,
    pub project_id: String,
    pub issue_id: Option<String>,
    pub job_id: Option<String>,
    /// Author-role facet (see `SearchFilters::role`); empty when not applicable.
    role: String,
    pub title: String,
    pub snippet: String,
    pub rank: f64,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
struct SearchDocument {
    source_id: String,
    content_type: SearchContentType,
    project_id: String,
    issue_id: Option<String>,
    job_id: Option<String>,
    role: String,
    title: String,
    body: String,
    created_at: i64,
}

#[derive(Debug, Clone)]
struct SearchOutboxEntry {
    id: String,
    source_table: String,
    source_id: String,
    content_type: SearchContentType,
    op: SearchOutboxOp,
}

struct SearchRebuildSnapshot {
    documents: Vec<SearchDocument>,
    pending_outbox_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchOutboxOp {
    Upsert,
    Delete,
}

#[derive(Debug, Clone, Copy)]
struct SearchFields {
    source_key: Field,
    source_id: Field,
    content_type: Field,
    project_id: Field,
    issue_id: Field,
    job_id: Field,
    role: Field,
    title: Field,
    body: Field,
    created_at: Field,
}

pub struct SearchIndex {
    index: Index,
    reader: IndexReader,
    writer: Mutex<IndexWriter<TantivyDocument>>,
    fields: SearchFields,
    needs_rebuild: AtomicBool,
}

/// Open the index writer, tolerating a directory write lock that a just-dropped
/// writer is still releasing.
///
/// Tantivy guards each index with an on-disk `.tantivy-writer.lock`. Dropping an
/// `IndexWriter` releases that lock, but its background merge thread can keep it
/// held for a few milliseconds after the owning `SearchIndex` is dropped.
/// Opening a fresh writer on the same directory immediately afterward (a reopen,
/// or two indices over one directory under the parallel test suite) then races
/// and returns `LockFailure(LockBusy)`. Retry briefly before surfacing it.
fn open_writer_with_retry(index: &Index) -> DbResult<IndexWriter<TantivyDocument>> {
    const MAX_RETRIES: u32 = 12;
    let mut attempt = 0u32;
    loop {
        match index.writer(INDEX_WRITER_MEMORY_BUDGET) {
            Ok(writer) => return Ok(writer),
            Err(tantivy::TantivyError::LockFailure(_, _)) if attempt < MAX_RETRIES => {
                attempt += 1;
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

impl SearchIndex {
    pub fn open_or_create(path: impl AsRef<Path>) -> DbResult<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;

        let schema = search_schema();
        let existed = path.join("meta.json").exists();
        let (index, needs_rebuild) = if existed {
            match Index::open_in_dir(path) {
                Ok(index) if search_fields(&index.schema()).is_ok() => (index, false),
                Err(_) => {
                    std::fs::remove_dir_all(path)?;
                    std::fs::create_dir_all(path)?;
                    (Index::create_in_dir(path, schema)?, true)
                }
                Ok(_) => {
                    std::fs::remove_dir_all(path)?;
                    std::fs::create_dir_all(path)?;
                    (Index::create_in_dir(path, schema)?, true)
                }
            }
        } else {
            (Index::create_in_dir(path, schema)?, true)
        };
        let fields = search_fields(&index.schema())?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let writer = open_writer_with_retry(&index)?;

        Ok(Self {
            index,
            reader,
            writer: Mutex::new(writer),
            fields,
            needs_rebuild: AtomicBool::new(needs_rebuild),
        })
    }

    pub fn needs_rebuild(&self) -> bool {
        self.needs_rebuild.load(Ordering::SeqCst)
    }

    pub async fn rebuild(&self, db: &LocalDb) -> DbResult<usize> {
        let mut snapshot = load_rebuild_snapshot(db).await?;
        // Events are indexed separately so archived rows reconstruct from git
        // coordinates before their text is indexed (a SQL json_extract over a
        // gitcoord/zstd stub would index nothing). Live `full` events pass
        // through reconstruction untouched.
        snapshot.documents.extend(load_event_documents(db).await?);
        let indexed_count = snapshot.documents.len();

        {
            let mut writer = self.writer.lock().map_err(|error| {
                DbError::Search(format!("search writer lock poisoned: {error}"))
            })?;
            writer.delete_all_documents()?;
            for document in snapshot.documents {
                writer.add_document(self.tantivy_document(document))?;
            }
            writer.commit()?;
        }

        self.reader.reload()?;
        mark_applied(db, &snapshot.pending_outbox_ids).await?;
        self.needs_rebuild.store(false, Ordering::SeqCst);
        Ok(indexed_count)
    }

    /// Rebuilds the index from the source rows of EVERY supplied database.
    ///
    /// Like [`rebuild`], but collects documents from all databases BEFORE the
    /// single `delete_all_documents`, so rebuilding never drops already-applied
    /// rows that live in another open database (e.g. a team replica). Documents
    /// are URI-keyed and URIs are project-encoded, so one index serves all
    /// databases without collision.
    pub async fn rebuild_many(&self, dbs: &[std::sync::Arc<LocalDb>]) -> DbResult<usize> {
        let mut all_documents = Vec::new();
        // (database index, that database's drained outbox ids) — marked applied
        // only after the rebuild commits.
        let mut pending: Vec<(usize, Vec<String>)> = Vec::new();
        for (idx, db) in dbs.iter().enumerate() {
            let mut snapshot = load_rebuild_snapshot(db).await?;
            snapshot.documents.extend(load_event_documents(db).await?);
            pending.push((idx, snapshot.pending_outbox_ids));
            all_documents.extend(snapshot.documents);
        }
        let indexed_count = all_documents.len();

        {
            let mut writer = self.writer.lock().map_err(|error| {
                DbError::Search(format!("search writer lock poisoned: {error}"))
            })?;
            writer.delete_all_documents()?;
            for document in all_documents {
                writer.add_document(self.tantivy_document(document))?;
            }
            writer.commit()?;
        }

        self.reader.reload()?;
        for (idx, outbox_ids) in pending {
            mark_applied(&dbs[idx], &outbox_ids).await?;
        }
        self.needs_rebuild.store(false, Ordering::SeqCst);
        Ok(indexed_count)
    }

    pub async fn apply_pending(&self, db: &LocalDb) -> DbResult<usize> {
        let entries = self.pending_entries(db).await?;
        if entries.is_empty() {
            return Ok(0);
        }

        let mut prepared = Vec::with_capacity(entries.len());
        let mut skipped_ids: Vec<String> = Vec::new();
        for entry in entries {
            let document = if entry.op == SearchOutboxOp::Upsert {
                if matches!(entry.content_type, SearchContentType::Event)
                    && self.event_is_archived(db, &entry.source_id).await?
                {
                    // An archived event's inline row is a gitcoord/zstd stub:
                    // the SQL source query would read the stub and index
                    // nothing, so the event reconstructs first (as in rebuild).
                    // Reconstruction yields the original text, so re-adding
                    // replaces an already-indexed document with identical
                    // content — and indexes events whose pending upsert had not
                    // yet applied when the row was archived (insert → teardown
                    // or backfill archival → outbox drain).
                    let document = load_archived_event_document(db, &entry.source_id).await?;
                    if document.is_none() {
                        // No reconstructable document: keep whatever the index
                        // already holds rather than delete without re-adding.
                        skipped_ids.push(entry.id);
                        continue;
                    }
                    document
                } else {
                    self.load_document(db, &entry).await?
                }
            } else {
                None
            };
            prepared.push((entry, document));
        }

        let mut applied_ids: Vec<String> =
            prepared.iter().map(|(entry, _)| entry.id.clone()).collect();
        applied_ids.extend(skipped_ids);

        {
            let mut writer = self.writer.lock().map_err(|error| {
                DbError::Search(format!("search writer lock poisoned: {error}"))
            })?;

            for (entry, document) in prepared {
                let source_key = source_key(&entry.content_type, &entry.source_id);
                writer.delete_term(Term::from_field_text(self.fields.source_key, &source_key));

                if let Some(document) = document {
                    writer.add_document(self.tantivy_document(document))?;
                }
            }

            writer.commit()?;
        }

        self.reader.reload()?;
        mark_applied(db, &applied_ids).await?;
        Ok(applied_ids.len())
    }

    pub fn search(
        &self,
        query: &str,
        filters: Option<SearchFilters>,
    ) -> DbResult<Vec<SearchIndexHit>> {
        let query = query.trim();
        if query.is_empty() {
            return Ok(Vec::new());
        }

        let filters = filters.unwrap_or_default();
        let limit = filters.limit.unwrap_or(50).min(100);
        if limit == 0
            || filters
                .content_types
                .as_ref()
                .is_some_and(|content_types| content_types.is_empty())
        {
            return Ok(Vec::new());
        }

        // `in=title` restricts matching to the title field alone; otherwise the
        // query matches over both title and body.
        let query_fields: Vec<Field> = if filters.title_only {
            vec![self.fields.title]
        } else {
            vec![self.fields.title, self.fields.body]
        };
        let Some(text_query) = self.build_text_query(query, &query_fields)? else {
            return Ok(Vec::new());
        };
        let snippet_query = text_query.box_clone();
        let search_query = self.filtered_query(text_query, &filters);
        let searcher = self.reader.searcher();
        let mut snippet_generator =
            SnippetGenerator::create(&searcher, snippet_query.as_ref(), self.fields.body)?;
        snippet_generator.set_max_num_chars(150);

        // BM25 relevance dominates; a small recency term (bounded well under a
        // single point) tips ties toward newer documents without reordering
        // meaningfully different scores. `created_at` is the existing FAST field.
        let now_secs = chrono::Utc::now().timestamp();
        let collector = TopDocs::with_limit(limit).tweak_score(
            move |segment_reader: &tantivy::SegmentReader| {
                let created = segment_reader.fast_fields().i64("created_at").ok();
                move |doc: tantivy::DocId, original_score: tantivy::Score| {
                    let created_at = created
                        .as_ref()
                        .and_then(|column| column.first(doc))
                        .unwrap_or(0);
                    original_score + recency_bonus(now_secs, created_at)
                }
            },
        );
        let top_docs = searcher.search(search_query.as_ref(), &collector)?;

        let mut hits = Vec::new();
        for (rank, address) in top_docs {
            let doc = searcher.doc::<TantivyDocument>(address)?;
            let hit = self.hit_from_doc(&doc, rank as f64, &snippet_generator)?;
            if hit_matches_filters(&hit, &filters) {
                hits.push(hit);
                if hits.len() >= limit {
                    break;
                }
            }
        }

        Ok(hits)
    }

    /// Tokenize the query with the index's default analyzer, dropping empties.
    fn tokenize(&self, query: &str) -> DbResult<Vec<String>> {
        let mut analyzer = self.index.tokenizers().get("default").ok_or_else(|| {
            DbError::Search("default search tokenizer not registered".to_string())
        })?;
        let mut stream = analyzer.token_stream(query);
        let mut tokens = Vec::new();
        while stream.advance() {
            let token = stream.token();
            if !token.text.is_empty() {
                tokens.push(token.text.clone());
            }
        }
        Ok(tokens)
    }

    /// Build the scored text query.
    ///
    /// Every token becomes a `Must` clause, so a multi-word query requires all
    /// of its words to match somewhere (AND across tokens) — the fix for the old
    /// OR-of-everything semantics that made multi-word queries noisier. Within a
    /// token, each field contributes a forgiving `Should` of an exact term plus
    /// — only on the trailing token, the word the user is still typing — a
    /// prefix-fuzzy term. That is classic search-as-you-type without fuzzing
    /// words the user already finished, the biggest source of fuzz noise.
    /// Title-field clauses are boosted so title matches outrank body matches.
    fn build_text_query(&self, query: &str, fields: &[Field]) -> DbResult<Option<Box<dyn Query>>> {
        let tokens = self.tokenize(query)?;
        let Some(last_index) = tokens.len().checked_sub(1) else {
            return Ok(None);
        };
        let clauses: Vec<(Occur, Box<dyn Query>)> = tokens
            .iter()
            .enumerate()
            .map(|(index, token)| {
                (
                    Occur::Must,
                    self.token_clause(token, index == last_index, fields),
                )
            })
            .collect();
        Ok(Some(Box::new(BooleanQuery::new(clauses))))
    }

    /// A single token's cross-field `Should` group (see `build_text_query`).
    fn token_clause(&self, token: &str, is_last: bool, fields: &[Field]) -> Box<dyn Query> {
        let mut inner: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        for &field in fields {
            let boost_title = field == self.fields.title;
            let exact: Box<dyn Query> = Box::new(TermQuery::new(
                Term::from_field_text(field, token),
                IndexRecordOption::WithFreqs,
            ));
            inner.push((Occur::Should, boost_if(exact, boost_title)));

            if is_last {
                let prefix: Box<dyn Query> = Box::new(FuzzyTermQuery::new_prefix(
                    Term::from_field_text(field, token),
                    0,
                    true,
                ));
                inner.push((Occur::Should, boost_if(prefix, boost_title)));
            }
        }
        Box::new(BooleanQuery::new(inner))
    }

    fn filtered_query(
        &self,
        text_query: Box<dyn Query>,
        filters: &SearchFilters,
    ) -> Box<dyn Query> {
        let mut clauses = vec![(Occur::Must, text_query)];

        if let Some(ref project_id) = filters.project_id {
            clauses.push((
                Occur::Must,
                self.exact_filter_query(self.fields.project_id, project_id),
            ));
        }

        if let Some(ref issue_id) = filters.issue_id {
            clauses.push((
                Occur::Must,
                self.exact_filter_query(self.fields.issue_id, issue_id),
            ));
        }

        if let Some(ref content_types) = filters.content_types {
            let type_clauses = content_types
                .iter()
                .map(|content_type| {
                    (
                        Occur::Should,
                        self.exact_filter_query(self.fields.content_type, content_type),
                    )
                })
                .collect();
            clauses.push((Occur::Must, Box::new(BooleanQuery::new(type_clauses))));
        }

        if let Some(ref role) = filters.role {
            clauses.push((Occur::Must, self.exact_filter_query(self.fields.role, role)));
        }

        if let Some(since) = filters.since {
            let since_query = RangeQuery::new(
                Bound::Included(Term::from_field_i64(self.fields.created_at, since)),
                Bound::Unbounded,
            );
            clauses.push((
                Occur::Must,
                Box::new(ConstScoreQuery::new(Box::new(since_query), 0.0)),
            ));
        }

        if clauses.len() == 1 {
            clauses.pop().expect("text query must be present").1
        } else {
            Box::new(BooleanQuery::new(clauses))
        }
    }

    fn exact_filter_query(&self, field: Field, value: &str) -> Box<dyn Query> {
        Box::new(ConstScoreQuery::new(
            Box::new(TermQuery::new(
                Term::from_field_text(field, value),
                IndexRecordOption::Basic,
            )),
            0.0,
        ))
    }

    async fn pending_entries(&self, db: &LocalDb) -> DbResult<Vec<SearchOutboxEntry>> {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, source_table, source_id, content_type, op
                         FROM search_outbox
                         WHERE status = 'pending'
                         ORDER BY created_at, id",
                        (),
                    )
                    .await?;

                let mut entries = Vec::new();
                while let Some(row) = rows.next().await? {
                    entries.push(SearchOutboxEntry {
                        id: row.text(0)?,
                        source_table: row.text(1)?,
                        source_id: row.text(2)?,
                        content_type: row
                            .text(3)?
                            .parse::<SearchContentType>()
                            .map_err(DbError::Search)?,
                        op: parse_outbox_op(&row.text(4)?)?,
                    });
                }
                Ok(entries)
            })
        })
        .await
    }

    /// Whether an event row has been rewritten to a git coordinate / zstd stub.
    /// Archived events must reconstruct before indexing — their inline `data`
    /// holds no searchable text (see `apply_pending`).
    async fn event_is_archived(&self, db: &LocalDb, event_id: &str) -> DbResult<bool> {
        let event_id = event_id.to_string();
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT storage_mode FROM events WHERE id = ?1 LIMIT 1",
                        (event_id.as_str(),),
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(false);
                };
                Ok(matches!(
                    row.opt_text(0)?.as_deref(),
                    Some("gitcoord") | Some("zstd")
                ))
            })
        })
        .await
    }

    async fn load_document(
        &self,
        db: &LocalDb,
        entry: &SearchOutboxEntry,
    ) -> DbResult<Option<SearchDocument>> {
        let Some(sql) = source_query(&entry.source_table) else {
            return Ok(None);
        };

        let source_id = entry.source_id.clone();
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(sql, (source_id.as_str(),)).await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                row_to_search_document(&row)
            })
        })
        .await
    }

    fn tantivy_document(&self, document: SearchDocument) -> TantivyDocument {
        let mut doc = TantivyDocument::new();
        doc.add_text(
            self.fields.source_key,
            source_key(&document.content_type, &document.source_id),
        );
        doc.add_text(self.fields.source_id, document.source_id);
        doc.add_text(self.fields.content_type, document.content_type.to_string());
        doc.add_text(self.fields.project_id, document.project_id);
        doc.add_text(self.fields.issue_id, document.issue_id.unwrap_or_default());
        doc.add_text(self.fields.job_id, document.job_id.unwrap_or_default());
        doc.add_text(self.fields.role, document.role);
        doc.add_text(self.fields.title, document.title);
        doc.add_text(self.fields.body, document.body);
        doc.add_i64(self.fields.created_at, document.created_at);
        doc
    }

    fn hit_from_doc(
        &self,
        doc: &TantivyDocument,
        rank: f64,
        snippet_generator: &SnippetGenerator,
    ) -> DbResult<SearchIndexHit> {
        let content_type: SearchContentType = doc_text(doc, self.fields.content_type)?
            .parse::<SearchContentType>()
            .map_err(DbError::Search)?;
        let snippet = snippet_generator
            .snippet_from_doc(doc)
            .to_html()
            .replace("<b>", "<mark>")
            .replace("</b>", "</mark>");
        let fallback = doc_text(doc, self.fields.body)?;

        Ok(SearchIndexHit {
            id: doc_text(doc, self.fields.source_id)?,
            content_type,
            project_id: doc_text(doc, self.fields.project_id)?,
            issue_id: empty_to_none(doc_text(doc, self.fields.issue_id)?),
            job_id: empty_to_none(doc_text(doc, self.fields.job_id)?),
            role: doc_text(doc, self.fields.role)?,
            title: doc_text(doc, self.fields.title)?,
            snippet: if snippet.is_empty() {
                fallback.chars().take(150).collect()
            } else {
                snippet
            },
            rank,
            created_at: doc_i64(doc, self.fields.created_at)?,
        })
    }
}

fn search_schema() -> Schema {
    let mut builder = Schema::builder();
    let stored_string = STRING | STORED;
    builder.add_text_field("source_key", stored_string.clone());
    builder.add_text_field("source_id", stored_string.clone());
    builder.add_text_field("content_type", stored_string.clone());
    builder.add_text_field("project_id", stored_string.clone());
    builder.add_text_field("issue_id", stored_string.clone());
    builder.add_text_field("job_id", stored_string.clone());
    builder.add_text_field("role", stored_string);
    builder.add_text_field("title", TEXT | STORED);
    builder.add_text_field("body", TEXT | STORED);
    builder.add_i64_field(
        "created_at",
        NumericOptions::default().set_stored().set_fast(),
    );

    builder.build()
}

fn search_fields(schema: &Schema) -> DbResult<SearchFields> {
    Ok(SearchFields {
        source_key: schema.get_field("source_key")?,
        source_id: schema.get_field("source_id")?,
        content_type: schema.get_field("content_type")?,
        project_id: schema.get_field("project_id")?,
        issue_id: schema.get_field("issue_id")?,
        job_id: schema.get_field("job_id")?,
        role: schema.get_field("role")?,
        title: schema.get_field("title")?,
        body: schema.get_field("body")?,
        created_at: schema.get_field("created_at")?,
    })
}

fn source_query(source_table: &str) -> Option<&'static str> {
    match source_table {
        "issues" => Some(
            "SELECT id, 'issue', project_id, id, NULL, title, COALESCE(description, ''), created_at
             FROM issues
             WHERE id = ?1",
        ),
        "comments" => Some(
            "SELECT c.id, 'comment', i.project_id, c.issue_id, NULL,
                    CASE WHEN c.source = 'user' THEN 'User Comment' ELSE 'Agent Comment' END,
                    c.content,
                    c.created_at
             FROM comments c
             JOIN issues i ON i.id = c.issue_id
             WHERE c.id = ?1",
        ),
        "artifacts" => Some(
            "SELECT a.id, 'artifact', j.project_id, j.issue_id, a.job_id, a.artifact_type,
                    COALESCE(
                        json_extract(a.data, '$.content'),
                        json_extract(a.data, '$.title'),
                        json_extract(a.data, '$.summary'),
                        json_extract(a.data, '$.body'),
                        ''
                    ),
                    a.created_at
             FROM artifacts a
             JOIN jobs j ON j.id = a.job_id
             WHERE a.id = ?1 AND a.job_id IS NOT NULL",
        ),
        "events" => Some(
            "SELECT e.id, 'event', r.project_id, r.issue_id, r.job_id, e.event_type,
                    json_extract(e.data, '$.content'),
                    e.created_at
             FROM events e
             JOIN runs r ON r.id = e.run_id
             WHERE e.id = ?1
               AND e.event_type IN ('assistant', 'text', 'tool_result', 'user')
               AND json_extract(e.data, '$.content') IS NOT NULL",
        ),
        "messages" => Some(
            "SELECT m.id, 'message',
                    CASE
                        WHEN m.channel_type = 'project' THEN m.channel_id
                        WHEN m.channel_type = 'issue' THEN i.project_id
                        ELSE NULL
                    END,
                    CASE WHEN m.channel_type = 'issue' THEN m.channel_id ELSE NULL END,
                    NULL,
                    m.sender_name,
                    m.content,
                    m.created_at
             FROM messages m
             LEFT JOIN issues i ON m.channel_type = 'issue' AND i.id = m.channel_id
             WHERE m.id = ?1",
        ),
        _ => None,
    }
}

/// Rebuild sources that are safe to read straight from SQL. Events are handled
/// separately by `load_event_documents` so archived rows reconstruct first.
fn rebuild_source_queries() -> [&'static str; 4] {
    [
        "SELECT id, 'issue', project_id, id, NULL, title, COALESCE(description, ''), created_at
         FROM issues",
        "SELECT c.id, 'comment', i.project_id, c.issue_id, NULL,
                CASE WHEN c.source = 'user' THEN 'User Comment' ELSE 'Agent Comment' END,
                c.content,
                c.created_at
         FROM comments c
         JOIN issues i ON i.id = c.issue_id",
        "SELECT a.id, 'artifact', j.project_id, j.issue_id, a.job_id, a.artifact_type,
                COALESCE(
                    json_extract(a.data, '$.content'),
                    json_extract(a.data, '$.title'),
                    json_extract(a.data, '$.summary'),
                    json_extract(a.data, '$.body'),
                    ''
                ),
                a.created_at
         FROM artifacts a
         JOIN jobs j ON j.id = a.job_id
         WHERE a.job_id IS NOT NULL",
        "SELECT m.id, 'message',
                CASE
                    WHEN m.channel_type = 'project' THEN m.channel_id
                    WHEN m.channel_type = 'issue' THEN i.project_id
                    ELSE NULL
                END,
                CASE WHEN m.channel_type = 'issue' THEN m.channel_id ELSE NULL END,
                NULL,
                m.sender_name,
                m.content,
                m.created_at
         FROM messages m
         LEFT JOIN issues i ON m.channel_type = 'issue' AND i.id = m.channel_id",
    ]
}

/// Build search documents for indexable events, reconstructing archived rows
/// from git coordinates first so their text is indexed (not their stub). Loads
/// all `assistant`/`user`/`tool_result` events (plus legacy `text`),
/// reconstructs in one pass, then keeps
/// those whose reconstructed `data` carries a `content` string — mirroring the
/// live `json_extract(data,'$.content') IS NOT NULL` filter.
async fn load_event_documents(db: &LocalDb) -> DbResult<Vec<SearchDocument>> {
    use crate::models::Event;

    // Project EVENT_COLUMNS in a subquery so `e.*` stays unambiguous against the
    // joined `runs` row (both tables have an `id`), while preserving the exact
    // column order `event_from_row` expects.
    let columns = crate::storage::events::columns::EVENT_COLUMNS;
    let sql = format!(
        "SELECT e.*, r.project_id, r.issue_id, r.job_id
         FROM (SELECT {columns} FROM events WHERE event_type IN ('assistant', 'text', 'tool_result', 'user')) e
         JOIN runs r ON r.id = e.run_id"
    );

    type EventRow = (Event, Option<String>, Option<String>, Option<String>);
    let rows: Vec<EventRow> = db
        .read(move |conn| {
            Box::pin(async move {
                let mut out: Vec<EventRow> = Vec::new();
                let mut rows = conn.query(&sql, ()).await?;
                while let Some(row) = rows.next().await? {
                    let event = crate::storage::events::columns::event_from_row(&row)?;
                    // r.project_id/issue_id/job_id ride just past EVENT_COLUMNS;
                    // key off EVENT_COLUMN_COUNT so adding an event column shifts
                    // them in lockstep instead of silently reading the wrong slot.
                    let project_id =
                        row.opt_text(crate::storage::events::columns::EVENT_COLUMN_COUNT)?;
                    let issue_id =
                        row.opt_text(crate::storage::events::columns::EVENT_COLUMN_COUNT + 1)?;
                    let job_id =
                        row.opt_text(crate::storage::events::columns::EVENT_COLUMN_COUNT + 2)?;
                    out.push((event, project_id, issue_id, job_id));
                }
                Ok(out)
            })
        })
        .await?;

    // (project_id, issue_id, job_id) carried alongside each event across
    // reconstruction so the documents can be rebuilt afterwards.
    type EventMeta = (Option<String>, Option<String>, Option<String>);
    let (events, meta): (Vec<Event>, Vec<EventMeta>) = rows
        .into_iter()
        .map(|(event, project_id, issue_id, job_id)| (event, (project_id, issue_id, job_id)))
        .unzip();
    let events = super::reconstruct_events(db, events).await;

    let mut documents = Vec::new();
    for (event, (project_id, issue_id, job_id)) in events.into_iter().zip(meta) {
        if let Some(document) = event_search_document(event, project_id, issue_id, job_id) {
            documents.push(document);
        }
    }
    Ok(documents)
}

/// Build the search document for a (reconstructed) event row: `None` when the
/// event has no owning project or no extractable `content` text.
fn event_search_document(
    event: crate::models::Event,
    project_id: Option<String>,
    issue_id: Option<String>,
    job_id: Option<String>,
) -> Option<SearchDocument> {
    let project_id = project_id?;
    let body = serde_json::from_str::<JsonValue>(&event.data)
        .ok()
        .and_then(|value| {
            value
                .get("content")
                .and_then(|content| content.as_str())
                .map(str::to_string)
        })?;
    let role = derive_role(&SearchContentType::Event, &event.event_type);
    Some(SearchDocument {
        source_id: event.id,
        content_type: SearchContentType::Event,
        project_id,
        issue_id,
        job_id,
        role,
        title: event.event_type,
        body: normalize_search_body(&body),
        created_at: event.created_at,
    })
}

/// Load and reconstruct a single event row for incremental indexing of an
/// archived event (see `apply_pending`). Mirrors `load_event_documents`,
/// scoped to one id.
async fn load_archived_event_document(
    db: &LocalDb,
    event_id: &str,
) -> DbResult<Option<SearchDocument>> {
    use crate::models::Event;

    let columns = crate::storage::events::columns::EVENT_COLUMNS;
    let sql = format!(
        "SELECT e.*, r.project_id, r.issue_id, r.job_id
         FROM (SELECT {columns} FROM events
               WHERE id = ?1 AND event_type IN ('assistant', 'text', 'tool_result', 'user')) e
         JOIN runs r ON r.id = e.run_id"
    );

    let id = event_id.to_string();
    type EventRow = (Event, Option<String>, Option<String>, Option<String>);
    let row: Option<EventRow> = db
        .read(move |conn| {
            Box::pin(async move {
                let mut rows = conn.query(&sql, (id.as_str(),)).await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                let event = crate::storage::events::columns::event_from_row(&row)?;
                Ok(Some((
                    event,
                    row.opt_text(crate::storage::events::columns::EVENT_COLUMN_COUNT)?,
                    row.opt_text(crate::storage::events::columns::EVENT_COLUMN_COUNT + 1)?,
                    row.opt_text(crate::storage::events::columns::EVENT_COLUMN_COUNT + 2)?,
                )))
            })
        })
        .await?;

    let Some((event, project_id, issue_id, job_id)) = row else {
        return Ok(None);
    };
    let Some(event) = super::reconstruct_events(db, vec![event]).await.pop() else {
        return Ok(None);
    };
    Ok(event_search_document(event, project_id, issue_id, job_id))
}

async fn load_rebuild_snapshot(db: &LocalDb) -> DbResult<SearchRebuildSnapshot> {
    db.read(|conn| {
        Box::pin(async move {
            let mut documents = Vec::new();
            for sql in rebuild_source_queries() {
                let mut rows = conn.query(sql, ()).await?;
                while let Some(row) = rows.next().await? {
                    if let Some(document) = row_to_search_document(&row)? {
                        documents.push(document);
                    }
                }
            }

            let mut pending_outbox_ids = Vec::new();
            let mut rows = conn
                .query("SELECT id FROM search_outbox WHERE status = 'pending'", ())
                .await?;
            while let Some(row) = rows.next().await? {
                pending_outbox_ids.push(row.text(0)?);
            }

            Ok(SearchRebuildSnapshot {
                documents,
                pending_outbox_ids,
            })
        })
    })
    .await
}

fn row_to_search_document(row: &turso::Row) -> DbResult<Option<SearchDocument>> {
    let Some(project_id) = row.opt_text(2)? else {
        return Ok(None);
    };
    let Some(body) = row.opt_text(6)? else {
        return Ok(None);
    };

    let content_type = row
        .text(1)?
        .parse::<SearchContentType>()
        .map_err(DbError::Search)?;
    let title = row.text(5)?;
    let role = derive_role(&content_type, &title);
    Ok(Some(SearchDocument {
        source_id: row.text(0)?,
        content_type,
        project_id,
        issue_id: row.opt_text(3)?,
        job_id: row.opt_text(4)?,
        role,
        title,
        body: normalize_search_body(&body),
        created_at: row.i64(7)?,
    }))
}

fn normalize_search_body(body: &str) -> String {
    if let Ok(value) = serde_json::from_str::<JsonValue>(body) {
        if let Some(text) = value.as_str() {
            return text.to_string();
        }
    }
    body.to_string()
}

async fn mark_applied(db: &LocalDb, ids: &[String]) -> DbResult<()> {
    let ids = ids.to_vec();
    db.write(|conn| {
        let ids = ids.clone();
        Box::pin(async move {
            for id in ids {
                conn.execute(
                    "UPDATE search_outbox SET status = 'applied' WHERE id = ?1",
                    (id.as_str(),),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

fn parse_outbox_op(value: &str) -> DbResult<SearchOutboxOp> {
    match value {
        "upsert" => Ok(SearchOutboxOp::Upsert),
        "delete" => Ok(SearchOutboxOp::Delete),
        other => Err(DbError::Search(format!(
            "unknown search outbox op: {other}"
        ))),
    }
}

fn source_key(content_type: &SearchContentType, source_id: &str) -> String {
    format!("{content_type}:{source_id}")
}

/// Wrap a query in a title `BoostQuery` when `apply` is set, else pass it
/// through unchanged.
fn boost_if(query: Box<dyn Query>, apply: bool) -> Box<dyn Query> {
    if apply {
        Box::new(BoostQuery::new(query, TITLE_BOOST))
    } else {
        query
    }
}

/// Small additive recency bonus in `(0, RECENCY_WEIGHT]`, decaying with document
/// age. `created_at` units vary by source table (seconds vs milliseconds), so a
/// value that is clearly milliseconds is normalized down to seconds first.
fn recency_bonus(now_secs: i64, created_at: i64) -> f32 {
    let created_secs = if created_at > 1_000_000_000_000 {
        created_at / 1000
    } else {
        created_at
    };
    let age = (now_secs - created_secs).max(0) as f64;
    (RECENCY_WEIGHT as f64 / (1.0 + age / RECENCY_HALF_LIFE_SECS)) as f32
}

/// Derive the author-role facet from content type and the document title, which
/// carries the event type (events) or the author label (comments). Empty for
/// content types without a meaningful role axis.
fn derive_role(content_type: &SearchContentType, title: &str) -> String {
    match content_type {
        SearchContentType::Event => match title {
            // Current backends emit assistant text as `assistant`; `text` is the
            // legacy event type kept for old stored transcripts.
            "assistant" | "text" => "assistant",
            "user" => "user",
            "tool_result" => "tool",
            _ => "",
        },
        SearchContentType::Comment => match title {
            "User Comment" => "user",
            "Agent Comment" => "agent",
            _ => "",
        },
        _ => "",
    }
    .to_string()
}

fn empty_to_none(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn doc_text(doc: &TantivyDocument, field: Field) -> DbResult<String> {
    doc.get_first(field)
        .and_then(|value| value.as_str().map(ToString::to_string))
        .ok_or_else(|| DbError::Search(format!("missing stored text field {field:?}")))
}

fn doc_i64(doc: &TantivyDocument, field: Field) -> DbResult<i64> {
    doc.get_first(field)
        .and_then(|value| value.as_i64())
        .ok_or_else(|| DbError::Search(format!("missing stored i64 field {field:?}")))
}

fn hit_matches_filters(hit: &SearchIndexHit, filters: &SearchFilters) -> bool {
    if let Some(ref project_id) = filters.project_id {
        if &hit.project_id != project_id {
            return false;
        }
    }

    if let Some(ref issue_id) = filters.issue_id {
        if hit.issue_id.as_ref() != Some(issue_id) {
            return false;
        }
    }

    if let Some(ref content_types) = filters.content_types {
        if !content_types.contains(&hit.content_type.to_string()) {
            return false;
        }
    }

    if let Some(ref role) = filters.role {
        if &hit.role != role {
            return false;
        }
    }

    if let Some(since) = filters.since {
        if hit.created_at < since {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use turso::params;

    use super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

    async fn migrated_db() -> DbResult<LocalDb> {
        let temp = tempdir()?;
        let path = temp.keep().join("cairn-search-index.db");
        let db = LocalDb::open(path).await?;
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await?;
        Ok(db)
    }

    async fn insert_workspace_and_project(db: &LocalDb, project_id: &str) -> DbResult<()> {
        let project_id = project_id.to_string();
        db.write(|conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT OR IGNORE INTO workspaces(id, name, created_at, updated_at)
                     VALUES ('workspace-1', 'Workspace', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES (?1, 'workspace-1', ?1, ?1, '/tmp/project', 1, 1)",
                    params![project_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
    }

    #[tokio::test]
    async fn search_treats_query_syntax_as_plain_text() {
        let db = migrated_db().await.unwrap();
        insert_workspace_and_project(&db, "project-1")
            .await
            .unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-1', 'project-1', 1, 'Plain search', 'quote token and title:fieldlike marker live here', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        assert_eq!(index.apply_pending(&db).await.unwrap(), 1);

        let quote_hits = index.search("\"quote", None).unwrap();
        assert_eq!(quote_hits.len(), 1);
        assert_eq!(quote_hits[0].id, "issue-1");

        let fieldlike_hits = index.search("title:fieldlike", None).unwrap();
        assert_eq!(fieldlike_hits.len(), 1);
        assert_eq!(fieldlike_hits[0].id, "issue-1");
    }

    #[tokio::test]
    async fn search_matches_token_prefixes() {
        let db = migrated_db().await.unwrap();
        insert_workspace_and_project(&db, "project-1")
            .await
            .unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-1', 'project-1', 1, 'Testing flows', 'body text', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-2', 'project-1', 2, 'Other issue', 'contains searchable body term', 2, 2)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        assert_eq!(index.apply_pending(&db).await.unwrap(), 2);

        let title_hits = index.search("tes", None).unwrap();
        assert_eq!(title_hits.len(), 1);
        assert_eq!(title_hits[0].id, "issue-1");

        let body_hits = index.search("search", None).unwrap();
        assert_eq!(body_hits.len(), 1);
        assert_eq!(body_hits[0].id, "issue-2");
    }

    #[tokio::test]
    async fn search_applies_filters_inside_tantivy_query() {
        let db = migrated_db().await.unwrap();
        insert_workspace_and_project(&db, "project-other")
            .await
            .unwrap();
        insert_workspace_and_project(&db, "project-target")
            .await
            .unwrap();
        db.write(|conn| {
            Box::pin(async move {
                for number in 1..=20 {
                    let id = format!("issue-other-{number}");
                    conn.execute(
                        "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                         VALUES (?1, 'project-other', ?2, 'sharedterm sharedterm', 'sharedterm body', ?2, ?2)",
                        params![id.as_str(), number],
                    )
                    .await?;
                }
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-target', 'project-target', 1, 'Target issue', 'sharedterm target body', 100, 100)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        assert_eq!(index.apply_pending(&db).await.unwrap(), 21);

        let hits = index
            .search(
                "sharedterm",
                Some(SearchFilters {
                    project_id: Some("project-target".to_string()),
                    issue_id: None,
                    content_types: Some(vec!["issue".to_string()]),
                    role: None,
                    title_only: false,
                    since: None,
                    limit: Some(1),
                }),
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "issue-target");
        assert_eq!(hits[0].project_id, "project-target");
    }

    #[tokio::test]
    async fn applies_search_outbox_to_tantivy_index() {
        let db = migrated_db().await.unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces(id, name, created_at, updated_at)
                     VALUES ('workspace-1', 'Workspace', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('project-1', 'workspace-1', 'Project', 'PROJ', '/tmp/project', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-1', 'project-1', 1, 'Turso migration', 'mvcc search body', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO comments(id, issue_id, content, source, created_at)
                     VALUES ('comment-1', 'issue-1', 'comment mentions tantivy', 'user', 2)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO messages(id, channel_type, channel_id, sender_name, content, created_at)
                     VALUES ('message-1', 'issue', 'issue-1', 'agent', 'message mentions concurrency', 3)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        assert_eq!(index.apply_pending(&db).await.unwrap(), 3);

        let issue_hits = index.search("mvcc", None).unwrap();
        assert_eq!(issue_hits.len(), 1);
        assert_eq!(issue_hits[0].id, "issue-1");
        assert_eq!(issue_hits[0].content_type, SearchContentType::Issue);
        assert!(issue_hits[0].snippet.contains("<mark>mvcc</mark>"));

        let comment_hits = index.search("tantivy", None).unwrap();
        assert_eq!(comment_hits.len(), 1);
        assert_eq!(comment_hits[0].id, "comment-1");
        assert_eq!(comment_hits[0].issue_id.as_deref(), Some("issue-1"));

        let message_hits = index
            .search(
                "concurrency",
                Some(SearchFilters {
                    project_id: Some("project-1".to_string()),
                    issue_id: Some("issue-1".to_string()),
                    content_types: Some(vec!["message".to_string()]),
                    role: None,
                    title_only: false,
                    since: None,
                    limit: Some(10),
                }),
            )
            .unwrap();
        assert_eq!(message_hits.len(), 1);
        assert_eq!(message_hits[0].id, "message-1");
        assert_eq!(message_hits[0].project_id, "project-1");

        let pending_count = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*) FROM search_outbox WHERE status = 'pending'",
                            (),
                        )
                        .await?;
                    let row = rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::Row("missing pending count".to_string()))?;
                    row.i64(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(pending_count, 0);
    }

    /// Seed workspace/project/run plus one `user` event archived to zstd — its
    /// inline `data` is a stub; the searchable text lives compressed in
    /// `data_blob`.
    async fn seed_archived_zstd_event(db: &LocalDb, content: &str) {
        let blob =
            crate::storage::compress(format!("{{\"content\":\"{content}\"}}").as_bytes()).unwrap();
        db.write(move |conn| {
            let blob = blob.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('proj','ws','p','PROJ','/tmp/p',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs(id, project_id, status, created_at, updated_at)
                     VALUES ('run','proj','exited',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, storage_mode, data_blob, codec)
                     VALUES ('ev','run',1,1,'user','{\"_archived\":true}',1,'zstd',?1,'zstd_v1')",
                    params![blob],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[test]
    fn read_tool_results_are_not_content_indexed() {
        // A reconstructed gitcoord/hybrid read carries its rendered bytes under
        // `toolResult`, never `content`; the event search body is extracted from
        // `$.content` only (`event_search_document` / `rebuild_source_queries`),
        // so a read is never indexed by search. This is the invariant that keeps
        // the startup search refresh non-corrupting even though it reconstructs
        // archived reads before the mcp render seam is registered: whether the
        // read reconstructs to real bytes or a coordinate stub, its `toolResult`
        // never reaches the index and `apply_pending` skips it identically. If
        // read tool_results ever become searchable, the renderer-registration
        // ordering (`init_database` / `OrchestratorBuilder::build`) becomes
        // load-bearing and this test should change deliberately.
        let data = r#"{"eventType":"tool_result","toolUseId":"t1","toolName":"read","toolInput":{"paths":["file:a.txt"]},"toolResult":"=== file:a.txt ===\nALPHA\nbeta"}"#;
        let event = crate::storage::events::reconstruct_fixture::make_event(
            "read-ev",
            "tool_result",
            data,
            None,
            None,
            None,
            None,
        );
        assert!(event_search_document(event, Some("proj".to_string()), None, None).is_none());
    }

    #[tokio::test]
    async fn rebuild_indexes_reconstructed_archived_events() {
        let db = migrated_db().await.unwrap();
        seed_archived_zstd_event(&db, "zephyr archived needle").await;

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        index.rebuild(&db).await.unwrap();

        // The reconstructed (decompressed) text is searchable even though the
        // inline row is a stub.
        let hits = index.search("zephyr", None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "ev");
    }

    async fn insert_pending_event_upsert(db: &LocalDb, outbox_id: &str) {
        let outbox_id = outbox_id.to_string();
        db.write(move |conn| {
            let outbox_id = outbox_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
                     VALUES (?1,'events','ev','event','upsert','pending',1)",
                    params![outbox_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn incremental_reindexes_archived_event_from_reconstruction() {
        let db = migrated_db().await.unwrap();
        seed_archived_zstd_event(&db, "zephyr archived needle").await;

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        index.rebuild(&db).await.unwrap();
        assert_eq!(index.search("zephyr", None).unwrap().len(), 1);

        // A later UPDATE to the archived row enqueues an outbox upsert. The
        // incremental path reconstructs the event and re-adds the same text;
        // the document must not degrade to the inline stub or be deleted.
        insert_pending_event_upsert(&db, "ob1").await;

        index.apply_pending(&db).await.unwrap();
        assert_eq!(index.search("zephyr", None).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn incremental_indexes_event_archived_before_first_apply() {
        let db = migrated_db().await.unwrap();
        // The row is already archived when its original insert upsert drains:
        // the event landed, teardown (or backfill) archived it, and only then
        // does apply_pending run. The document must still be indexed — marking
        // the entry applied without indexing would drop the event from search
        // until a full rebuild.
        seed_archived_zstd_event(&db, "zephyr archived needle").await;
        insert_pending_event_upsert(&db, "ob1").await;

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        assert_eq!(index.apply_pending(&db).await.unwrap(), 1);

        let hits = index.search("zephyr", None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "ev");
    }

    #[tokio::test]
    async fn search_index_updates_and_deletes_are_idempotent() {
        let db = migrated_db().await.unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces(id, name, created_at, updated_at)
                     VALUES ('workspace-1', 'Workspace', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('project-1', 'workspace-1', 'Project', 'PROJ', '/tmp/project', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-1', 'project-1', 1, 'Original title', 'oldword body', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        assert_eq!(index.apply_pending(&db).await.unwrap(), 1);
        assert_eq!(index.search("oldword", None).unwrap().len(), 1);

        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "UPDATE issues
                     SET title = 'Updated title', description = 'newword body', updated_at = 2
                     WHERE id = 'issue-1'",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        assert_eq!(index.apply_pending(&db).await.unwrap(), 1);
        assert!(index.search("oldword", None).unwrap().is_empty());
        assert_eq!(index.search("newword", None).unwrap().len(), 1);

        db.write(|conn| {
            Box::pin(async move {
                conn.execute("DELETE FROM issues WHERE id = 'issue-1'", ())
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        assert_eq!(index.apply_pending(&db).await.unwrap(), 1);
        assert!(index.search("newword", None).unwrap().is_empty());
        assert_eq!(index.apply_pending(&db).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn rebuild_indexes_source_rows_even_when_outbox_is_already_applied() {
        let db = migrated_db().await.unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces(id, name, created_at, updated_at)
                     VALUES ('workspace-1', 'Workspace', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('project-1', 'workspace-1', 'Project', 'PROJ', '/tmp/project', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-1', 'project-1', 1, 'Rebuildable issue', 'rebuildable body', 1, 1)",
                    (),
                )
                .await?;
                conn.execute("UPDATE search_outbox SET status = 'applied'", ())
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        assert!(index.needs_rebuild());
        assert!(index.search("rebuildable", None).unwrap().is_empty());

        assert_eq!(index.rebuild(&db).await.unwrap(), 1);
        assert!(!index.needs_rebuild());

        let hits = index.search("rebuildable", None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "issue-1");
        assert_eq!(
            db.read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*) FROM search_outbox WHERE status = 'pending'",
                            (),
                        )
                        .await?;
                    let row = rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::Row("missing pending count".to_string()))?;
                    row.i64(0)
                })
            })
            .await
            .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn search_index_reopens_committed_documents() {
        let db = migrated_db().await.unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces(id, name, created_at, updated_at)
                     VALUES ('workspace-1', 'Workspace', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('project-1', 'workspace-1', 'Project', 'PROJ', '/tmp/project', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-1', 'project-1', 1, 'Persistent index', 'reopenable body', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        {
            let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
            assert!(index.needs_rebuild());
            assert_eq!(index.apply_pending(&db).await.unwrap(), 1);
            assert_eq!(index.search("reopenable", None).unwrap().len(), 1);
        }

        let reopened = SearchIndex::open_or_create(index_dir.path()).unwrap();
        assert!(!reopened.needs_rebuild());
        let hits = reopened.search("reopenable", None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "issue-1");
    }

    #[tokio::test]
    async fn rebuild_many_keeps_every_database_when_outboxes_are_already_applied() {
        // Two databases, each with an applied-outbox issue. A single-DB rebuild
        // clears the whole index and reloads from one DB, dropping the other's
        // already-applied row. rebuild_many collects from all DBs before the
        // single clear, so both survive (the cross-DB no-drop guarantee).
        let db1 = migrated_db().await.unwrap();
        let db2 = migrated_db().await.unwrap();
        db1.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('w', 'W', 1, 1)", ()).await?;
                conn.execute("INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p', 'w', 'P', 'PROJ', '/tmp/p', 1, 1)", ()).await?;
                conn.execute("INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at) VALUES ('issue-a', 'p', 1, 'Title', 'alpha body', 1, 1)", ()).await?;
                conn.execute("UPDATE search_outbox SET status = 'applied'", ()).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        db2.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('w', 'W', 1, 1)", ()).await?;
                conn.execute("INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p', 'w', 'P', 'PROJ', '/tmp/p', 1, 1)", ()).await?;
                conn.execute("INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at) VALUES ('issue-b', 'p', 1, 'Title', 'bravo body', 1, 1)", ()).await?;
                conn.execute("UPDATE search_outbox SET status = 'applied'", ()).await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        assert!(index.needs_rebuild());

        let dbs = vec![std::sync::Arc::new(db1), std::sync::Arc::new(db2)];
        assert_eq!(index.rebuild_many(&dbs).await.unwrap(), 2);
        assert_eq!(
            index.search("alpha", None).unwrap().len(),
            1,
            "first database's already-applied row must survive the rebuild"
        );
        assert_eq!(
            index.search("bravo", None).unwrap().len(),
            1,
            "second database's already-applied row must survive the rebuild"
        );
    }

    #[tokio::test]
    async fn multi_token_query_requires_all_tokens() {
        let db = migrated_db().await.unwrap();
        insert_workspace_and_project(&db, "project-1")
            .await
            .unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-both', 'project-1', 1, 'Retry backoff logic', 'body', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-one', 'project-1', 2, 'Retry only', 'unrelated body', 2, 2)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        assert_eq!(index.apply_pending(&db).await.unwrap(), 2);

        // Only issue-both contains both words. The old OR-of-everything query
        // would also return issue-one (it has "retry").
        let hits = index.search("retry backoff", None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "issue-both");
    }

    #[tokio::test]
    async fn title_match_outranks_body_match() {
        let db = migrated_db().await.unwrap();
        insert_workspace_and_project(&db, "project-1")
            .await
            .unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-title', 'project-1', 1, 'Widget calibration', 'unrelated filler text', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-body', 'project-1', 2, 'Unrelated heading', 'the widget appears in the body only', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        assert_eq!(index.apply_pending(&db).await.unwrap(), 2);

        // Equal created_at (recency neutral); the title boost floats the
        // title match above the body-only match.
        let hits = index.search("widget", None).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, "issue-title", "title match must rank first");
    }

    #[tokio::test]
    async fn newer_document_outranks_older_on_equal_text_score() {
        let db = migrated_db().await.unwrap();
        insert_workspace_and_project(&db, "project-1")
            .await
            .unwrap();
        // Identical title+body → identical BM25. created_at near now so the
        // recency term separates them (older → smaller bonus).
        let now = chrono::Utc::now().timestamp();
        let newer = now;
        let older = now - 60 * 60 * 24 * 60; // 60 days: older than one half-life
        db.write(move |conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-older', 'project-1', 1, 'Identical subject', 'identical body text', ?1, ?1)",
                    params![older],
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-newer', 'project-1', 2, 'Identical subject', 'identical body text', ?1, ?1)",
                    params![newer],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        assert_eq!(index.apply_pending(&db).await.unwrap(), 2);

        let hits = index.search("identical subject", None).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(
            hits[0].id, "issue-newer",
            "newer document must rank first on equal text score"
        );
    }

    async fn seed_two_inline_events(db: &LocalDb) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('proj','ws','p','PROJ','/tmp/p',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs(id, project_id, status, created_at, updated_at)
                     VALUES ('run','proj','exited',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                     VALUES ('ev-assistant','run',1,1,'assistant','{\"content\":\"sharednoun assistant\"}',1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                     VALUES ('ev-user','run',2,2,'user','{\"content\":\"sharednoun user\"}',2)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn role_filter_narrows_events_by_author() {
        let db = migrated_db().await.unwrap();
        seed_two_inline_events(&db).await;

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        index.rebuild(&db).await.unwrap();

        // Unfiltered, both events match.
        assert_eq!(index.search("sharednoun", None).unwrap().len(), 2);

        // A `text` event derives role=assistant.
        let assistant = index
            .search(
                "sharednoun",
                Some(SearchFilters {
                    role: Some("assistant".to_string()),
                    ..Default::default()
                }),
            )
            .unwrap();
        assert_eq!(assistant.len(), 1);
        assert_eq!(assistant[0].id, "ev-assistant");

        // A `user` event derives role=user.
        let user = index
            .search(
                "sharednoun",
                Some(SearchFilters {
                    role: Some("user".to_string()),
                    ..Default::default()
                }),
            )
            .unwrap();
        assert_eq!(user.len(), 1);
        assert_eq!(user[0].id, "ev-user");
    }

    #[tokio::test]
    async fn title_only_filter_matches_title_field_alone() {
        let db = migrated_db().await.unwrap();
        insert_workspace_and_project(&db, "project-1")
            .await
            .unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-title', 'project-1', 1, 'Parser rewrite', 'unrelated body', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                     VALUES ('issue-body', 'project-1', 2, 'Unrelated heading', 'the parser lives only in the body', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        assert_eq!(index.apply_pending(&db).await.unwrap(), 2);

        // Default search matches title and body.
        assert_eq!(index.search("parser", None).unwrap().len(), 2);

        // title_only restricts matching to the title field.
        let hits = index
            .search(
                "parser",
                Some(SearchFilters {
                    title_only: true,
                    ..Default::default()
                }),
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "issue-title");
    }

    #[tokio::test]
    async fn rebuild_indexes_role_for_archived_events() {
        let db = migrated_db().await.unwrap();
        // Seeds one `user` event archived to zstd (inline row is a stub).
        seed_archived_zstd_event(&db, "zephyr archived needle").await;

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        index.rebuild(&db).await.unwrap();

        // The reconstructed archived event carries role=user (its event type).
        let user = index
            .search(
                "zephyr",
                Some(SearchFilters {
                    role: Some("user".to_string()),
                    ..Default::default()
                }),
            )
            .unwrap();
        assert_eq!(user.len(), 1);
        assert_eq!(user[0].id, "ev");

        // It must not surface under a different role.
        assert!(index
            .search(
                "zephyr",
                Some(SearchFilters {
                    role: Some("assistant".to_string()),
                    ..Default::default()
                }),
            )
            .unwrap()
            .is_empty());
    }
}
