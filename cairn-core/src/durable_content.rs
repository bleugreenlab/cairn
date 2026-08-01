use std::{future::Future, pin::Pin, sync::OnceLock};

use regex::Regex;
use serde_json::Value;

use crate::{images, mcp::types::McpCallbackRequest, orchestrator::Orchestrator, storage::LocalDb};

const MATERIALIZATION_READ_CAP: u64 = 32 * 1024 * 1024;

type ReadFuture<'a> = Pin<Box<dyn Future<Output = Option<Vec<u8>>> + Send + 'a>>;
type StoreFuture<'a> = Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>>;

trait DurableImageAuthority: Sync {
    fn read<'a>(&'a self, path: &'a str) -> ReadFuture<'a>;
    fn store<'a>(&'a self, bytes: Vec<u8>) -> StoreFuture<'a>;
}

struct RunAuthority<'a> {
    orch: &'a Orchestrator,
    db: std::sync::Arc<LocalDb>,
    run_id: String,
    job_id: String,
    project_id: String,
    /// Where images this run promotes are addressed from — the run's issue when
    /// it has one, so a promoted image's URI names where it came from.
    scope: images::ImageScope,
    head: tokio::sync::OnceCell<String>,
}

impl RunAuthority<'_> {
    /// The coordinate this run's cells are materialized at, resolved live from
    /// the store.
    ///
    /// Run placement resolves a slot's coordinate exactly this way, so a
    /// materialization selected here and the run that produced it agree by
    /// construction rather than by coincidence. The recorded `jobs.base_commit`
    /// row is the coordinate the branch was cut at and does not track a base
    /// advance; selecting a residency by it asks for a coordinate the run has
    /// already moved off, and the read silently finds nothing.
    ///
    /// Resolution is deferred to the first candidate because normalization runs
    /// over every message and the overwhelming majority carry no image path at
    /// all. One normalization pass resolves one coordinate.
    async fn head(&self) -> Option<&str> {
        self.head
            .get_or_try_init(|| async {
                let request = McpCallbackRequest {
                    run_id: Some(self.run_id.clone()),
                    ..Default::default()
                };
                crate::mcp::handlers::branch::resolve_current_for_read(self.orch, &request)
                    .await
                    .map(|resolution| resolution.commit_id)
            })
            .await
            .map_err(|error: String| {
                log::debug!(
                    "durable image read for run {}: logical head unresolvable ({error})",
                    self.run_id
                );
            })
            .ok()
            .map(String::as_str)
    }
}

impl DurableImageAuthority for RunAuthority<'_> {
    fn read<'a>(&'a self, path: &'a str) -> ReadFuture<'a> {
        Box::pin(async move {
            let base_commit = self.head().await?;
            let repository = cairn_common::executor_protocol::RepositoryIdentity {
                project_id: self.project_id.to_string(),
                repository_id: self.project_id.to_string(),
                object_format: cairn_common::executor_protocol::GitObjectFormat::Sha1,
            };
            let candidate = self
                .orch
                .fleet
                .select_materialization_read_candidate(
                    &self.run_id,
                    &self.job_id,
                    &self.project_id,
                    &repository,
                    base_commit,
                )
                .ok()?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            match self
                .orch
                .fleet
                .read_resident_materialization(
                    &candidate.executor_id,
                    candidate.generation,
                    cairn_common::executor_protocol::MaterializationReadRequest {
                        fence: candidate.fence,
                        cell_id: candidate.cell_id,
                        project_id: self.project_id.to_string(),
                        repository,
                        base_commit: base_commit.to_string(),
                        materialization_generation: candidate.materialization_generation,
                        path: path.to_string(),
                        deadline_unix_ms: now.saturating_add(30_000),
                        byte_cap: MATERIALIZATION_READ_CAP,
                    },
                )
                .await
            {
                cairn_common::executor_protocol::MaterializationReadResult::Bytes { bytes } => {
                    Some(bytes)
                }
                cairn_common::executor_protocol::MaterializationReadResult::Failed { .. } => None,
            }
        })
    }

    fn store<'a>(&'a self, bytes: Vec<u8>) -> StoreFuture<'a> {
        Box::pin(async move {
            images::store_image_bytes(&self.db, &self.scope, bytes)
                .await
                .map_err(|error| format!("Failed to make durable image content: {error}"))
        })
    }
}

fn candidate_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r#"(?i)(?:/|\.\.?/)?[^\s<>\"'()\[\]]+\.(?:png|jpe?g|gif|webp)"#).unwrap()
    })
}

async fn normalize_text_with(
    authority: &dyn DurableImageAuthority,
    input: &str,
) -> Result<String, String> {
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    for found in candidate_regex().find_iter(input) {
        output.push_str(&input[cursor..found.start()]);
        let path = found.as_str();
        if path.starts_with("http://")
            || path.starts_with("https://")
            || path.starts_with("cairn://")
        {
            output.push_str(path);
        } else if let Some(bytes) = authority.read(path).await {
            if images::detect_image_mime(&bytes).is_some() {
                if bytes.len() > images::MAX_IMAGE_BYTES {
                    return Err(format!(
                        "Durable image exceeds the 5 MiB ({} byte) limit",
                        images::MAX_IMAGE_BYTES
                    ));
                }
                let uri = authority.store(bytes).await?;
                output.push_str(&uri);
            } else {
                output.push_str(path);
            }
        } else {
            output.push_str(path);
        }
        cursor = found.end();
    }
    output.push_str(&input[cursor..]);
    Ok(output)
}

fn normalize_json_with<'a>(
    authority: &'a dyn DurableImageAuthority,
    value: &'a mut Value,
) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
    Box::pin(async move {
        match value {
            Value::String(text) => *text = normalize_text_with(authority, text).await?,
            Value::Array(values) => {
                for value in values {
                    normalize_json_with(authority, value).await?;
                }
            }
            Value::Object(values) => {
                for value in values.values_mut() {
                    normalize_json_with(authority, value).await?;
                }
            }
            _ => {}
        }
        Ok(())
    })
}

async fn authority_for_request<'a>(
    orch: &'a Orchestrator,
    request: &'a McpCallbackRequest,
    project_key: &'a str,
) -> Result<Option<RunAuthority<'a>>, String> {
    let Some(run_id) = request.run_id.as_deref() else {
        return Ok(None);
    };
    let (run, db) = crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request).await?;
    if !run.project_key.eq_ignore_ascii_case(project_key) {
        return Ok(None);
    }
    Ok(Some(RunAuthority {
        orch,
        db,
        run_id: run_id.to_string(),
        job_id: run.job_id,
        scope: images::ImageScope::new(run.project_id.clone(), project_key)
            .in_issue(run.issue_number),
        project_id: run.project_id,
        head: tokio::sync::OnceCell::new(),
    }))
}

async fn authority_for_run<'a>(
    orch: &'a Orchestrator,
    run_id: &str,
) -> Result<Option<RunAuthority<'a>>, String> {
    let db = crate::execution::routing::owning_db_for_run(&orch.db, run_id)
        .await
        .map_err(|error| error.to_string())?;
    let run = crate::mcp::handlers::run_context::lookup_run_by_id(&db, run_id).await?;
    Ok(Some(RunAuthority {
        orch,
        db,
        run_id: run_id.to_string(),
        job_id: run.job_id,
        scope: images::ImageScope::new(run.project_id.clone(), run.project_key)
            .in_issue(run.issue_number),
        project_id: run.project_id,
        head: tokio::sync::OnceCell::new(),
    }))
}

pub(crate) async fn normalize_text(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_key: &str,
    input: &str,
) -> Result<String, String> {
    let Some(authority) = authority_for_request(orch, request, project_key).await? else {
        return Ok(input.to_string());
    };
    normalize_text_with(&authority, input).await
}

pub(crate) async fn normalize_json(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_key: &str,
    value: &mut Value,
) -> Result<(), String> {
    let Some(authority) = authority_for_request(orch, request, project_key).await? else {
        return Ok(());
    };
    normalize_json_with(&authority, value).await
}

pub(crate) async fn normalize_json_for_run(
    orch: &Orchestrator,
    run_id: &str,
    value: &mut Value,
) -> Result<(), String> {
    let Some(authority) = authority_for_run(orch, run_id).await? else {
        return Ok(());
    };
    normalize_json_with(&authority, value).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Fake {
        reads: std::collections::HashMap<String, Option<Vec<u8>>>,
        fail_store: bool,
        stored: Mutex<Vec<Vec<u8>>>,
    }
    impl DurableImageAuthority for Fake {
        fn read<'a>(&'a self, path: &'a str) -> ReadFuture<'a> {
            Box::pin(async move { self.reads.get(path).cloned().flatten() })
        }
        fn store<'a>(&'a self, bytes: Vec<u8>) -> StoreFuture<'a> {
            Box::pin(async move {
                if self.fail_store {
                    Err("store failed".into())
                } else {
                    self.stored.lock().unwrap().push(bytes);
                    Ok("cairn://p/CAIRN/images/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into())
                }
            })
        }
    }
    fn png() -> Vec<u8> {
        b"\x89PNG\r\n\x1a\nimage".to_vec()
    }
    fn fake(entries: &[(&str, Option<Vec<u8>>)]) -> Fake {
        Fake {
            reads: entries
                .iter()
                .map(|(p, b)| (p.to_string(), b.clone()))
                .collect(),
            fail_store: false,
            stored: Mutex::new(Vec::new()),
        }
    }

    #[tokio::test]
    async fn preserves_non_promotable_and_remote_references_byte_for_byte() {
        let input = "missing.png reclaimed.jpg note.gif https://example.com/remote.webp";
        let authority = fake(&[
            ("missing.png", None),
            ("reclaimed.jpg", None),
            ("note.gif", Some(b"prose".to_vec())),
        ]);
        assert_eq!(normalize_text_with(&authority, input).await.unwrap(), input);
        assert!(authority.stored.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn promotes_supported_images_and_nested_json_strings() {
        let authority = fake(&[("plot.png", Some(png())), ("nested.webp", Some(png()))]);
        let mut value = serde_json::json!({"body":"![plot](plot.png)","nested":[{"path":"nested.webp"}],"number":3});
        normalize_json_with(&authority, &mut value).await.unwrap();
        assert!(value["body"]
            .as_str()
            .unwrap()
            .contains("cairn://p/CAIRN/images/"));
        assert!(value["nested"][0]["path"]
            .as_str()
            .unwrap()
            .starts_with("cairn://"));
        assert_eq!(authority.stored.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn store_failure_is_fail_closed_after_image_classification() {
        let authority = Fake {
            fail_store: true,
            ..fake(&[("plot.png", Some(png()))])
        };
        assert_eq!(
            normalize_text_with(&authority, "plot.png")
                .await
                .unwrap_err(),
            "store failed"
        );
    }

    #[tokio::test]
    async fn oversized_supported_image_is_fail_closed() {
        let mut bytes = png();
        bytes.resize(images::MAX_IMAGE_BYTES + 1, 0);
        let authority = fake(&[("plot.png", Some(bytes))]);
        let error = normalize_text_with(&authority, "plot.png")
            .await
            .unwrap_err();
        assert!(error.contains("5 MiB"));
    }
}
