//! Reads of services: a project's (`cairn://p/<project>/services`,
//! `/services/<id>`, `/status`, `/log`) and a thread's
//! (`cairn://p/<project>/<thread>/services`, `/services/<name>`, `/status`,
//! `/log`). The declaration comes from the owner's service source (the base
//! tip, or the thread's newest version); the state comes from this runner's
//! supervisor.

use cairn_common::query::QueryParam;
use cairn_common::uri::{
    build_project_service_log_uri, build_project_service_status_uri, build_project_service_uri,
    build_thread_service_log_uri, build_thread_service_status_uri, build_thread_services_uri,
};

use crate::orchestrator::Orchestrator;
use crate::project_services::declaration::ServiceListing;
use crate::project_services::log::{self, LogKind, LogRow};
use crate::project_services::supervisor::{self, ServiceRef, ServiceStatus};
use crate::project_services::{
    project_id_for_key, schedule, thread_store, ServiceOwner, CONFINEMENT_NOTE,
};

const DEFAULT_LOG_LIMIT: usize = 100;
const MAX_LOG_LIMIT: usize = 1_000;
/// The newest log lines a service read shows: what it last said.
const LAST_SAID: usize = 8;

fn short(hash: &str) -> &str {
    &hash[..hash.len().min(12)]
}

fn executor_of(listing: &ServiceListing) -> String {
    match &listing.declaration {
        Ok(declaration) => match &declaration.executor {
            None => "any".to_string(),
            Some(selector) => selector
                .name
                .clone()
                .or_else(|| selector.os.clone())
                .unwrap_or_else(|| "any".to_string()),
        },
        Err(_) => "-".to_string(),
    }
}

fn state_word(status: Option<&ServiceStatus>) -> &'static str {
    status.map_or("stopped", |status| status.state.as_str())
}

fn cell(text: &str) -> String {
    text.replace('|', "\\|").replace('\n', " ")
}

/// The service a URI names, resolved to its project and owner.
async fn resolve(
    orch: &Orchestrator,
    project: &str,
    thread: Option<&str>,
    service: &str,
) -> Result<ServiceRef, String> {
    let project_key = cairn_common::uri::canonical_project(project);
    let (project_id, owner) = match thread {
        None => (
            project_id_for_key(orch, project).await?,
            ServiceOwner::Project,
        ),
        Some(thread) => {
            let db = orch.db.for_project(project).await;
            thread_store::resolve_owner(&db, &project_key, thread).await?
        }
    };
    Ok(ServiceRef {
        project_id,
        project_key,
        owner,
        service: service.to_string(),
    })
}

async fn owning_db(
    orch: &Orchestrator,
    target: &ServiceRef,
) -> Result<std::sync::Arc<crate::storage::LocalDb>, String> {
    crate::execution::routing::owning_db_for_project(&orch.db, &target.project_id)
        .await
        .map_err(|error| error.to_string())
}

fn status_uri(target: &ServiceRef) -> String {
    match &target.owner {
        ServiceOwner::Project => {
            build_project_service_status_uri(&target.project_key, &target.service)
        }
        ServiceOwner::Thread { name, .. } => {
            build_thread_service_status_uri(&target.project_key, name, &target.service)
        }
    }
}

fn log_uri(target: &ServiceRef) -> String {
    match &target.owner {
        ServiceOwner::Project => {
            build_project_service_log_uri(&target.project_key, &target.service)
        }
        ServiceOwner::Thread { name, .. } => {
            build_thread_service_log_uri(&target.project_key, name, &target.service)
        }
    }
}

pub(crate) async fn read_index(orch: &Orchestrator, project: &str) -> String {
    let project_id = match project_id_for_key(orch, project).await {
        Ok(id) => id,
        Err(error) => return error,
    };
    let (revision, listings) =
        match supervisor::declarations(orch, &project_id, &ServiceOwner::Project).await {
            Ok(found) => found,
            Err(error) => return format!("Could not read the services of {project}: {error}"),
        };
    let mut out = format!(
        "# Services — {project}\n\nDeclared under `.cairn/services/` at base {}. {} service(s). \
         A thread's own services are at that thread's `/services`.\n\n",
        short(&revision),
        listings.len()
    );
    if listings.is_empty() {
        out.push_str("No services are declared.\n");
        return out;
    }
    out.push_str("| Service | State | Start | Executor | Generation | Source | Uptime | Restarts | Last error |\n");
    out.push_str("|---|---|---|---|---|---|---|---|---|\n");
    for listing in &listings {
        let status = orch.project_services.supervisor.status(
            &project_id,
            &ServiceOwner::Project,
            &listing.id,
        );
        let start = listing
            .declaration
            .as_ref()
            .map(|declaration| declaration.start.as_str())
            .unwrap_or("-");
        let error = match (&listing.declaration, status.as_ref()) {
            (Err(error), _) => error.clone(),
            (Ok(_), Some(status)) => status.last_error.clone().unwrap_or_default(),
            _ => String::new(),
        };
        out.push_str(&format!(
            "| [{id}]({uri}) | {state} | {start} | {executor} | {generation} | {source} | {uptime} | {restarts} | {error} |\n",
            id = listing.id,
            uri = build_project_service_uri(project, &listing.id),
            state = if listing.declaration.is_err() { "invalid" } else { state_word(status.as_ref()) },
            executor = cell(&executor_of(listing)),
            generation = status.as_ref().and_then(|s| s.generation).map_or("-".into(), |g| g.to_string()),
            source = status.as_ref().and_then(|s| s.source_hash.clone()).map_or("-".into(), |h| short(&h).to_string()),
            uptime = status.as_ref().and_then(|s| s.uptime_secs).map_or("-".into(), |s| format!("{s}s")),
            restarts = status.as_ref().map_or(0, |s| s.restarts),
            error = cell(&error),
        ));
    }
    out
}

/// The services a thread has written, newest version of each.
pub(crate) async fn read_thread_index(orch: &Orchestrator, project: &str, thread: &str) -> String {
    let project_key = cairn_common::uri::canonical_project(project);
    let db = orch.db.for_project(project).await;
    let (project_id, owner) = match thread_store::resolve_owner(&db, &project_key, thread).await {
        Ok(found) => found,
        Err(error) => return error,
    };
    let thread_id = owner.thread_id().unwrap_or_default().to_string();
    let rows = match thread_store::list_latest(&db, &thread_id).await {
        Ok(rows) => rows,
        Err(error) => return error,
    };
    let mut out = format!(
        "# Services — {}\n\n{} service(s) this thread has written. Write one at `{}/<name>`: a page is \
         the interface, a service is the process behind it, and the thread database is the state.\n\n",
        cairn_common::uri::build_thread_uri(&project_key, thread),
        rows.len(),
        build_thread_services_uri(&project_key, thread)
    );
    if rows.is_empty() {
        out.push_str("No services yet.\n");
        return out;
    }
    out.push_str("| Service | Title | State | Version | Serving | Generation | Uptime | Restarts | Last error |\n");
    out.push_str("|---|---|---|---|---|---|---|---|---|\n");
    for row in &rows {
        let status = orch
            .project_services
            .supervisor
            .status(&project_id, &owner, &row.name);
        let title = row
            .data
            .get("title")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(&row.name);
        let serving = serving_version(&db, &owner, &row.name, status.as_ref()).await;
        out.push_str(&format!(
            "| [{name}]({uri}) | {title} | {state} | {version} | {serving} | {generation} | {uptime} | {restarts} | {error} |\n",
            name = row.name,
            uri = owner.home(&project_key, &row.name),
            title = cell(title),
            state = state_word(status.as_ref()),
            version = row.version,
            serving = serving.map_or("-".into(), |v| v.to_string()),
            generation = status.as_ref().and_then(|s| s.generation).map_or("-".into(), |g| g.to_string()),
            uptime = status.as_ref().and_then(|s| s.uptime_secs).map_or("-".into(), |s| format!("{s}s")),
            restarts = status.as_ref().map_or(0, |s| s.restarts),
            error = cell(status.as_ref().and_then(|s| s.last_error.as_deref()).unwrap_or("")),
        ));
    }
    out
}

/// The newest stored version whose package is the one the service serves.
async fn serving_version(
    db: &crate::storage::LocalDb,
    owner: &ServiceOwner,
    service: &str,
    status: Option<&ServiceStatus>,
) -> Option<i64> {
    let thread_id = owner.thread_id()?;
    let status = status?;
    status.generation?;
    let hash = status.source_hash.as_deref()?;
    thread_store::versions(db, thread_id, service)
        .await
        .ok()?
        .into_iter()
        .find(|(_, source)| source == hash)
        .map(|(version, _)| version)
}

fn status_block(status: Option<&ServiceStatus>, serving: Option<i64>) -> String {
    let Some(status) = status else {
        return "- State: stopped (not started by this runner)\n".to_string();
    };
    let mut out = format!("- State: {}", status.state.as_str());
    if status.sticky_stop {
        out.push_str(" (stopped explicitly; calls are refused until a start or restart)");
    }
    out.push('\n');
    if let Some(version) = serving {
        out.push_str(&format!("- Serving version: {version}\n"));
    }
    if let Some(generation) = status.generation {
        out.push_str(&format!("- Generation: {generation}\n"));
    }
    if let Some(run) = &status.run_id {
        out.push_str(&format!("- Run: {run}\n"));
    }
    if let Some(executor) = &status.executor {
        out.push_str(&format!("- Executor: {executor}\n"));
    }
    if let Some(hash) = &status.source_hash {
        out.push_str(&format!("- Source: {}\n", short(hash)));
    }
    if let Some(uptime) = status.uptime_secs {
        out.push_str(&format!("- Uptime: {uptime}s\n"));
    }
    out.push_str(&format!("- Pending calls: {}\n", status.pending_calls));
    out.push_str(&format!("- Restarts: {}\n", status.restarts));
    if let Some(wait) = status.backoff_secs {
        out.push_str(&format!("- Restarting in: {wait}s\n"));
    }
    if let Some(error) = &status.last_error {
        out.push_str(&format!("- Last error: {error}\n"));
    }
    out
}

fn utc_seconds(at: i64) -> String {
    chrono::DateTime::from_timestamp(at, 0)
        .map(|at| at.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| at.to_string())
}

/// The idle stop and the schedule: when each scheduled method next runs and
/// what its last run did. Read from the persisted cadence, so it holds across
/// runner restarts and for a service with no process.
async fn schedule_block(
    orch: &Orchestrator,
    target: &ServiceRef,
    status: Option<&ServiceStatus>,
) -> String {
    let mut out = String::new();
    if let Some(idle) = status.and_then(|s| s.idle_stop_after) {
        out.push_str(&format!(
            "- Idle stop: after {} with no call (the next call or scheduled tick starts it)\n",
            schedule::every_text(idle.as_millis() as i64)
        ));
    }
    let rows = match owning_db(orch, target).await {
        Ok(db) => schedule::list(&db, &target.project_id, &target.owner, &target.service)
            .await
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    };
    if rows.is_empty() {
        return out;
    }
    let now = chrono::Utc::now().timestamp();
    out.push_str(
        "\n## Schedule\n\nCalled by the runner as this service's owner, with no turn.\n\n\
         | Method | Every | Next due | Last run | Outcome | Failures in a row |\n\
         |---|---|---|---|---|---|\n",
    );
    for row in &rows {
        let next = row
            .due_at(now)
            .map(|_| "due now".to_string())
            .or_else(|| row.next_due_at(now).map(utc_seconds))
            .unwrap_or_else(|| "-".into());
        out.push_str(&format!(
            "| {} | {} | {next} | {} | {} | {} |\n",
            cell(&row.method),
            schedule::every_text(row.every_ms),
            row.last_run_at.map_or("never".into(), utc_seconds),
            row.last_status.as_deref().unwrap_or("-"),
            row.consecutive_failures,
        ));
    }
    for row in rows
        .iter()
        .filter(|row| row.last_status.as_deref() != Some("ok"))
    {
        if let Some(error) = &row.last_error {
            out.push_str(&format!("\n`{}` last: {error}\n", row.method));
        }
    }
    out
}

fn log_line(row: &LogRow) -> String {
    let when = chrono::DateTime::from_timestamp_millis(row.created_at)
        .map(|at| at.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
        .unwrap_or_default();
    format!("- {when} [{}] g{} {}\n", row.kind, row.generation, row.text)
}

/// The status, recent calls, newest log lines, and links every service read
/// ends with, whoever owns it.
async fn live_sections(
    orch: &Orchestrator,
    target: &ServiceRef,
    status: Option<&ServiceStatus>,
    serving: Option<i64>,
) -> String {
    let mut out = String::from("## Status\n\n");
    out.push_str(&status_block(status, serving));
    out.push_str(&schedule_block(orch, target, status).await);
    out.push_str("\n## Recent calls\n\n");
    match status
        .map(|s| &s.recent_calls)
        .filter(|calls| !calls.is_empty())
    {
        None => out.push_str("None since this runner started.\n"),
        Some(calls) => {
            out.push_str("| When | Method | Caller | Outcome | ms |\n|---|---|---|---|---|\n");
            for call in calls.iter().rev() {
                let when = chrono::DateTime::from_timestamp_millis(call.at_ms)
                    .map(|at| at.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_default();
                out.push_str(&format!(
                    "| {when} | {} | {} {} | {} | {} |\n",
                    cell(&call.method),
                    call.caller_kind,
                    cell(&call.acting_for),
                    cell(&call.outcome),
                    call.millis
                ));
            }
        }
    }
    out.push_str("\n## Last said\n\n");
    let newest = match owning_db(orch, target).await {
        Ok(db) => log::list(
            &db,
            &target.project_id,
            target.owner.key(),
            &target.service,
            None,
            None,
            LAST_SAID,
        )
        .await
        .unwrap_or_default(),
        Err(_) => Vec::new(),
    };
    if newest.is_empty() {
        out.push_str("Nothing logged yet.\n");
    }
    for row in &newest {
        out.push_str(&log_line(row));
    }
    out.push_str(&format!(
        "\n- [status]({})\n- [log]({})\n",
        status_uri(target),
        log_uri(target)
    ));
    out
}

pub(crate) async fn read_service(orch: &Orchestrator, project: &str, service: &str) -> String {
    let target = match resolve(orch, project, None, service).await {
        Ok(target) => target,
        Err(error) => return error,
    };
    let declared = supervisor::declarations(orch, &target.project_id, &target.owner)
        .await
        .map(|(revision, listings)| {
            (
                revision,
                listings.into_iter().find(|listing| listing.id == service),
            )
        });
    let status = target.status(orch);
    let mut out = format!("# Service `{service}` — {project}\n\n");
    match &declared {
        Ok((revision, Some(listing))) => match &listing.declaration {
            Ok(declaration) => {
                out.push_str(&declaration.name);
                if !declaration.description.is_empty() {
                    out.push_str(&format!(": {}", declaration.description));
                }
                out.push_str(&format!(
                    "\n\n## Declaration\n\n`.cairn/services/{service}/service.yaml` at base {}:\n\n```yaml\n{}\n```\n\n",
                    short(revision),
                    declaration.manifest_text.trim_end()
                ));
            }
            Err(error) => out.push_str(&format!(
                "The declaration at base {} is invalid: {error}\n\n",
                short(revision)
            )),
        },
        Ok((revision, None)) => out.push_str(&format!(
            "Project {project} declares no service `{service}` at base {}.\n\n",
            short(revision)
        )),
        Err(error) => out.push_str(&format!("Could not read the declaration: {error}\n\n")),
    }
    out.push_str(&live_sections(orch, &target, status.as_ref(), None).await);
    out
}

/// A thread's service: what the thread wrote, which version is serving, and
/// what the process last said. `?format=json` answers the stored package.
pub(crate) async fn read_thread_service(
    orch: &Orchestrator,
    project: &str,
    thread: &str,
    service: &str,
    params: &[QueryParam],
) -> String {
    let mut format_json = false;
    let mut version = None;
    for param in params {
        match param.key.as_str() {
            "format" if param.value == "json" => format_json = true,
            "format" => return format!("`format` must be json, not `{}`", param.value),
            "version" => match param.value.parse::<i64>() {
                Ok(value) if value > 0 => version = Some(value),
                _ => {
                    return format!(
                        "`version` must be a positive integer, not `{}`",
                        param.value
                    )
                }
            },
            other => {
                return format!("Query parameter `{other}` is not supported on a thread service")
            }
        }
    }
    let target = match resolve(orch, project, Some(thread), service).await {
        Ok(target) => target,
        Err(error) => return error,
    };
    let uri = target.owner.home(&target.project_key, service);
    let db = match owning_db(orch, &target).await {
        Ok(db) => db,
        Err(error) => return error,
    };
    let thread_id = target.owner.thread_id().unwrap_or_default().to_string();
    let row = match thread_store::load(&db, &thread_id, service, version).await {
        Ok(row) => row,
        Err(error) => return error,
    };
    if format_json {
        return match &row {
            Some(row) => serde_json::to_string_pretty(&row.data).unwrap_or_else(|_| "null".into()),
            None => "null".to_string(),
        };
    }
    let status = target.status(orch);
    let serving = serving_version(&db, &target.owner, service, status.as_ref()).await;
    let mut out = format!("# Service `{service}` — {uri}\n\n");
    match &row {
        None => out.push_str(&format!(
            "Thread {thread} has written no service `{service}`{}. Write it with mode `create`: a \
             `files` object whose entry (default main.ts) calls `serve({{methods}})` from \
             `@cairn/sdk/service`. The write deploys it.\n\n",
            version.map_or(String::new(), |v| format!(" at version {v}"))
        )),
        Some(row) => {
            let text = |key: &str| row.data.get(key).and_then(serde_json::Value::as_str);
            out.push_str(text("title").unwrap_or(service));
            if let Some(description) = text("description").filter(|d| !d.is_empty()) {
                out.push_str(&format!(": {description}"));
            }
            out.push_str(&format!("\n\n## Package\n\n- Version: {}", row.version));
            if version.is_some() {
                out.push_str(" (an earlier version, by request)");
            }
            out.push_str(&format!(
                "\n- Source: {}\n- Entry: {}\n",
                short(&row.source_hash),
                text("entry").unwrap_or(crate::project_services::declaration::DEFAULT_ENTRY)
            ));
            if let Some(files) = row.data.get("files").and_then(serde_json::Value::as_object) {
                out.push_str("- Files:");
                for (path, content) in files {
                    let bytes = content.as_str().map_or(0, str::len);
                    out.push_str(&format!(" `{path}` ({bytes} B)"));
                }
                out.push('\n');
            }
            match row.data.get("manifest") {
                Some(manifest) if manifest.as_object().is_some_and(|m| !m.is_empty()) => {
                    out.push_str(&format!("- Manifest: `{manifest}`\n"));
                }
                _ => out.push_str(
                    "- Manifest: defaults (on_demand with a 5m idle stop, 256 MB, 8 concurrent \
                     calls, 30 s per call, no schedule)\n",
                ),
            }
            match (serving, status.as_ref().and_then(|s| s.generation)) {
                (Some(serving), _) if serving == row.version => {}
                (Some(serving), _) => out.push_str(&format!(
                    "\nVersion {serving} is serving, not version {}: read the log for why the newer \
                     one did not start.\n",
                    row.version
                )),
                (None, Some(_)) => out.push_str(
                    "\nThe serving generation runs a package no stored version matches.\n",
                ),
                (None, None) => {}
            }
            out.push('\n');
        }
    }
    out.push_str(&live_sections(orch, &target, status.as_ref(), serving).await);
    out
}

pub(crate) async fn read_status(
    orch: &Orchestrator,
    project: &str,
    thread: Option<&str>,
    service: &str,
) -> String {
    let target = match resolve(orch, project, thread, service).await {
        Ok(target) => target,
        Err(error) => return error,
    };
    let status = target.status(orch);
    let serving = match owning_db(orch, &target).await {
        Ok(db) => serving_version(&db, &target.owner, service, status.as_ref()).await,
        Err(_) => None,
    };
    format!(
        "# Status — service `{service}`\n\n{}{}\n{CONFINEMENT_NOTE}\n",
        status_block(status.as_ref(), serving),
        schedule_block(orch, &target, status.as_ref()).await
    )
}

pub(crate) async fn read_log(
    orch: &Orchestrator,
    project: &str,
    thread: Option<&str>,
    service: &str,
    params: &[QueryParam],
) -> String {
    let mut since = None;
    let mut limit = DEFAULT_LOG_LIMIT;
    let mut kind = None;
    for param in params {
        match param.key.as_str() {
            "since" => match param.value.parse::<i64>() {
                Ok(value) => since = Some(value),
                Err(_) => {
                    return format!("`since` must be unix milliseconds, not `{}`", param.value)
                }
            },
            "limit" => match param.value.parse::<usize>() {
                Ok(value) if value > 0 => limit = value.min(MAX_LOG_LIMIT),
                _ => return format!("`limit` must be a positive integer, not `{}`", param.value),
            },
            "kind" => match LogKind::parse(&param.value) {
                Some(value) => kind = Some(value),
                None => {
                    return format!(
                        "`kind` must be lifecycle, stderr, or call, not `{}`",
                        param.value
                    )
                }
            },
            other => return format!("Query parameter `{other}` is not supported on a service log"),
        }
    }
    let target = match resolve(orch, project, thread, service).await {
        Ok(target) => target,
        Err(error) => return error,
    };
    let db = match owning_db(orch, &target).await {
        Ok(db) => db,
        Err(error) => return error,
    };
    let rows = match log::list(
        &db,
        &target.project_id,
        target.owner.key(),
        service,
        since,
        kind,
        limit,
    )
    .await
    {
        Ok(rows) => rows,
        Err(error) => return error,
    };
    let mut out = format!(
        "# Log — service `{service}`\n\n{} row(s), oldest first.\n\n",
        rows.len()
    );
    for row in &rows {
        out.push_str(&log_line(row));
    }
    if let Some(last) = rows.last() {
        out.push_str(&format!("\nNewer rows: `?since={}`\n", last.created_at + 1));
    }
    out
}
