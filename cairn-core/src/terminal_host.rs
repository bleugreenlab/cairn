//! Shared PTY and persisted terminal host logic for Cairn hosts.

use crate::db::DbState;
use crate::mcp::handlers::slug::slugify;
use crate::orchestrator::Orchestrator;
use crate::services::{
    apply_shell_integration, get_default_shell, integration_dir, CommandState, Osc133Parser,
    PortableTerminalChild, PtySession, PtyState, Services, TerminalOutputWatcher, MAX_BUFFER_SIZE,
};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use portable_pty::{CommandBuilder, PtySize};
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use turso::params;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyDataPayload {
    pub session_id: String,
    pub data: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyExitPayload {
    pub session_id: String,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyCommandStatePayload {
    pub session_id: String,
    pub busy: bool,
    pub exit_code: Option<i32>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyCommandSnapshot {
    pub busy: bool,
    pub exit_code: Option<i32>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobTerminal {
    pub id: String,
    pub job_id: Option<String>,
    pub project_id: Option<String>,
    pub run_id: Option<String>,
    pub session_id: String,
    pub command: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub status: String,
    pub exit_code: Option<i32>,
    pub created_at: i64,
    pub exited_at: Option<i64>,
    pub slug: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActiveTerminal {
    pub id: String,
    pub session_id: String,
    pub command: String,
    pub job_id: Option<String>,
    pub issue_id: Option<String>,
    pub issue_number: Option<i32>,
    pub project_id: String,
    pub slug: Option<String>,
    pub node_name: Option<String>,
    pub exec_seq: Option<i32>,
    pub project_key: String,
}

fn terminal_from_row(row: &turso::Row) -> DbResult<JobTerminal> {
    Ok(JobTerminal {
        id: row.text(0)?,
        job_id: row.opt_text(1)?,
        project_id: row.opt_text(2)?,
        run_id: row.opt_text(3)?,
        session_id: row.text(4)?,
        command: row.text(5)?,
        title: row.opt_text(6)?,
        description: row.opt_text(7)?,
        status: row.text(8)?,
        exit_code: row.opt_i64(9)?.map(|value| value as i32),
        created_at: row.i64(10)?,
        exited_at: row.opt_i64(11)?,
        slug: row.opt_text(12)?,
    })
}

const TERMINAL_SELECT: &str = "
    SELECT id, job_id, project_id, run_id, session_id, command, title,
           description, status, exit_code, created_at, exited_at, slug
    FROM job_terminals
";

struct StartedTerminalSession {
    reader: Box<dyn Read + Send>,
    session_id: String,
    terminal_id: String,
    output_buffer: Arc<Mutex<VecDeque<u8>>>,
    command_state: Arc<Mutex<CommandState>>,
    output_watchers: Arc<Mutex<Vec<TerminalOutputWatcher>>>,
    shell_path: String,
    created_at: i64,
}

fn remove_terminal_session(
    state: &PtyState,
    session_id: &str,
) -> Result<Option<Arc<Mutex<PtySession>>>, String> {
    let mut sessions = state.sessions.lock().map_err(|e| e.to_string())?;
    Ok(sessions.remove(session_id))
}

fn start_terminal_session(
    state: &PtyState,
    services: &Services,
    cwd: &str,
) -> Result<StartedTerminalSession, String> {
    let size = PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    };
    let pair = services.pty_factory.create_pty(size)?;
    let shell_path = get_default_shell();
    let mut cmd = CommandBuilder::new(&shell_path);
    cmd.cwd(cwd);
    for (key, value) in std::env::vars() {
        cmd.env(key, value);
    }
    cmd.env("PATH", crate::env::get_user_path());
    cmd.env("TERM", "xterm-256color");
    apply_shell_integration(&mut cmd, &shell_path, &integration_dir());
    let components = pair.spawn_and_split(cmd)?;
    let session_id = Uuid::new_v4().to_string();
    let terminal_id = Uuid::new_v4().to_string();
    let output_buffer = Arc::new(Mutex::new(VecDeque::with_capacity(MAX_BUFFER_SIZE)));
    let command_state = Arc::new(Mutex::new(CommandState::default()));
    let output_watchers = Arc::new(Mutex::new(Vec::new()));
    let session = PtySession {
        master: Some(components.master),
        writer: Some(components.writer),
        child: Box::new(PortableTerminalChild::new(components.child)),
        output_buffer: Some(output_buffer.clone()),
        is_agent_spawned: false,
        last_output_at: None,
        command_state: Some(command_state.clone()),
        output_watchers: Some(output_watchers.clone()),
    };
    state
        .sessions
        .lock()
        .map_err(|e| e.to_string())?
        .insert(session_id.clone(), Arc::new(Mutex::new(session)));
    Ok(StartedTerminalSession {
        reader: components.reader,
        session_id,
        terminal_id,
        output_buffer,
        command_state,
        output_watchers,
        shell_path,
        created_at: chrono::Utc::now().timestamp(),
    })
}

#[allow(clippy::too_many_arguments)]
fn emit_reader_events(
    services: Arc<Services>,
    mut reader: Box<dyn Read + Send>,
    session_id: String,
    output_buffer: Arc<Mutex<VecDeque<u8>>>,
    command_state: Arc<Mutex<CommandState>>,
    output_watchers: Option<Arc<Mutex<Vec<TerminalOutputWatcher>>>>,
    orch: Option<Orchestrator>,
    on_exit: impl FnOnce() + Send + 'static,
) {
    thread::spawn(move || {
        let mut parser = Osc133Parser::new();
        let mut buf = [0u8; 4096];
        let mut on_exit = Some(on_exit);
        loop {
            match reader.read(&mut buf) {
                Ok(0) => {
                    let _ = services.emitter.emit(
                        "pty-exit",
                        serde_json::to_value(PtyExitPayload {
                            session_id: session_id.clone(),
                            exit_code: None,
                        })
                        .unwrap_or_default(),
                    );
                    if let Some(on_exit) = on_exit.take() {
                        on_exit();
                    }
                    break;
                }
                Ok(n) => {
                    let raw = String::from_utf8_lossy(&buf[..n]);
                    let (clean, events) = parser.feed(&raw);
                    if !clean.is_empty() {
                        let _ = services.emitter.emit(
                            "pty-data",
                            serde_json::to_value(PtyDataPayload {
                                session_id: session_id.clone(),
                                data: clean.clone(),
                            })
                            .unwrap_or_default(),
                        );
                    }
                    for event in events {
                        let (busy, exit_code, duration_ms) = match command_state.lock() {
                            Ok(mut guard) => guard.apply(event),
                            Err(_) => continue,
                        };
                        let _ = services.emitter.emit(
                            "pty-command-state",
                            serde_json::to_value(PtyCommandStatePayload {
                                session_id: session_id.clone(),
                                busy,
                                exit_code,
                                duration_ms,
                            })
                            .unwrap_or_default(),
                        );
                    }
                    if !clean.is_empty() {
                        if let (Some(orch), Some(watchers)) =
                            (orch.as_ref(), output_watchers.as_ref())
                        {
                            crate::orchestrator::wakes::scan_and_route_terminal_output(
                                orch, watchers, &clean,
                            );
                        }
                        let mut guard = output_buffer.lock().unwrap();
                        crate::services::update_output_buffer(
                            &mut guard,
                            clean.as_bytes(),
                            MAX_BUFFER_SIZE,
                        );
                    }
                }
                Err(error) => {
                    log::error!("PTY read error: {error}");
                    let _ = services.emitter.emit(
                        "pty-exit",
                        serde_json::to_value(PtyExitPayload {
                            session_id: session_id.clone(),
                            exit_code: None,
                        })
                        .unwrap_or_default(),
                    );
                    if let Some(on_exit) = on_exit.take() {
                        on_exit();
                    }
                    break;
                }
            }
        }
    });
}

pub fn create_pty(
    orch: &Orchestrator,
    cwd: String,
    cols: u16,
    rows: u16,
    shell: Option<String>,
) -> Result<String, String> {
    let size = PtySize {
        rows,
        cols,
        pixel_width: 0,
        pixel_height: 0,
    };
    let pair = orch.services.pty_factory.create_pty(size)?;
    let shell_path = shell.unwrap_or_else(get_default_shell);
    let mut cmd = CommandBuilder::new(&shell_path);
    cmd.cwd(&cwd);
    for (key, value) in std::env::vars() {
        cmd.env(key, value);
    }
    apply_shell_integration(&mut cmd, &shell_path, &integration_dir());
    let components = pair.spawn_and_split(cmd)?;
    let session_id = Uuid::new_v4().to_string();
    let output_buffer = Arc::new(Mutex::new(VecDeque::with_capacity(MAX_BUFFER_SIZE)));
    let command_state = Arc::new(Mutex::new(CommandState::default()));
    let session = PtySession {
        master: Some(components.master),
        writer: Some(components.writer),
        child: Box::new(PortableTerminalChild::new(components.child)),
        output_buffer: Some(output_buffer.clone()),
        is_agent_spawned: false,
        last_output_at: None,
        command_state: Some(command_state.clone()),
        output_watchers: None,
    };
    orch.pty_state
        .sessions
        .lock()
        .map_err(|e| e.to_string())?
        .insert(session_id.clone(), Arc::new(Mutex::new(session)));
    emit_reader_events(
        orch.services.clone(),
        components.reader,
        session_id.clone(),
        output_buffer,
        command_state,
        None,
        None,
        || {},
    );
    log::info!("Created PTY session {session_id} in {cwd}");
    Ok(session_id)
}

pub fn write_pty(orch: &Orchestrator, session_id: String, data: String) -> Result<(), String> {
    let sessions = orch.pty_state.sessions.lock().map_err(|e| e.to_string())?;
    let session = sessions.get(&session_id).ok_or("Session not found")?;
    let mut session = session.lock().map_err(|e| e.to_string())?;
    let writer = session
        .writer
        .as_mut()
        .ok_or("Terminal does not accept input")?;
    writer
        .write_all(data.as_bytes())
        .map_err(|e| e.to_string())?;
    writer.flush().map_err(|e| e.to_string())
}

pub fn resize_pty(
    orch: &Orchestrator,
    session_id: String,
    cols: u16,
    rows: u16,
) -> Result<(), String> {
    let sessions = orch.pty_state.sessions.lock().map_err(|e| e.to_string())?;
    let session = sessions.get(&session_id).ok_or("Session not found")?;
    let session = session.lock().map_err(|e| e.to_string())?;
    if let Some(master) = session.master.as_ref() {
        master
            .resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub fn close_pty(orch: &Orchestrator, session_id: String) -> Result<(), String> {
    if let Some(session_arc) = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| e.to_string())?
        .remove(&session_id)
    {
        if let Ok(mut session) = session_arc.lock() {
            let _ = session.child.kill();
            let _ = session.child.wait();
        }
        log::info!("Closed PTY session {session_id}");
    }
    Ok(())
}

pub fn check_pty_session_exists(orch: &Orchestrator, session_id: String) -> Result<bool, String> {
    Ok(orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| e.to_string())?
        .contains_key(&session_id))
}

pub fn get_pty_buffer(orch: &Orchestrator, session_id: String) -> Result<String, String> {
    let sessions = orch.pty_state.sessions.lock().map_err(|e| e.to_string())?;
    let session = sessions.get(&session_id).ok_or("Session not found")?;
    let session = session.lock().map_err(|e| e.to_string())?;
    match &session.output_buffer {
        Some(buffer) => {
            let bytes: Vec<u8> = buffer
                .lock()
                .map_err(|e| e.to_string())?
                .iter()
                .copied()
                .collect();
            Ok(String::from_utf8_lossy(&bytes).to_string())
        }
        None => Ok(String::new()),
    }
}

pub fn get_pty_command_states(
    orch: &Orchestrator,
    session_ids: Vec<String>,
) -> Result<HashMap<String, PtyCommandSnapshot>, String> {
    let sessions = orch.pty_state.sessions.lock().map_err(|e| e.to_string())?;
    let mut states = HashMap::new();
    for session_id in session_ids {
        let snapshot = sessions
            .get(&session_id)
            .and_then(|session| session.lock().ok())
            .and_then(|session| {
                session
                    .command_state
                    .as_ref()
                    .and_then(|state| state.lock().ok())
                    .map(|state| PtyCommandSnapshot {
                        busy: state.busy,
                        exit_code: state.last_exit_code,
                        duration_ms: state.last_duration_ms,
                    })
            })
            .unwrap_or(PtyCommandSnapshot {
                busy: false,
                exit_code: None,
                duration_ms: None,
            });
        states.insert(session_id, snapshot);
    }
    Ok(states)
}

async fn insert_terminal(db: Arc<LocalDb>, terminal: JobTerminal) -> Result<(), String> {
    db.write(|conn| {
        let terminal = terminal.clone();
        Box::pin(async move {
            conn.execute(
                "
                INSERT INTO job_terminals (
                    id, job_id, project_id, run_id, session_id, command, title,
                    description, status, exit_code, created_at, exited_at, slug
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                ",
                params![
                    terminal.id.as_str(),
                    terminal.job_id.as_deref(),
                    terminal.project_id.as_deref(),
                    terminal.run_id.as_deref(),
                    terminal.session_id.as_str(),
                    terminal.command.as_str(),
                    terminal.title.as_deref(),
                    terminal.description.as_deref(),
                    terminal.status.as_str(),
                    terminal.exit_code,
                    terminal.created_at,
                    terminal.exited_at,
                    terminal.slug.as_deref()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn delete_terminal(db: Arc<LocalDb>, terminal_id: String) -> Result<(), String> {
    db.write(|conn| {
        let terminal_id = terminal_id.clone();
        Box::pin(async move {
            conn.execute(
                "DELETE FROM job_terminals WHERE id = ?1",
                params![terminal_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_job_terminals(db: Arc<LocalDb>, job_id: String) -> Result<Vec<JobTerminal>, String> {
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!("{TERMINAL_SELECT} WHERE job_id = ?1 ORDER BY created_at ASC"),
                    params![job_id.as_str()],
                )
                .await?;
            let mut terminals = Vec::new();
            while let Some(row) = rows.next().await? {
                terminals.push(terminal_from_row(&row)?);
            }
            Ok(terminals)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn project_repo_path(db: Arc<LocalDb>, project_id: String) -> Result<String, String> {
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT repo_path FROM projects WHERE id = ?1 LIMIT 1",
                    params![project_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| row.text(0))
                .transpose()?
                .ok_or_else(|| DbError::Row(format!("Project not found: {project_id}")))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn slug_exists(
    conn: &turso::Connection,
    job_id: Option<&str>,
    project_id: Option<&str>,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = if let Some(job_id) = job_id {
        conn.query(
            "SELECT COUNT(*) FROM job_terminals WHERE job_id = ?1 AND slug = ?2",
            params![job_id, slug],
        )
        .await?
    } else if let Some(project_id) = project_id {
        conn.query("SELECT COUNT(*) FROM job_terminals WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2", params![project_id, slug]).await?
    } else {
        return Ok(false);
    };
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row("missing slug count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

async fn generate_slug_from_title(
    db: Arc<LocalDb>,
    job_id: Option<String>,
    project_id: Option<String>,
    title: String,
) -> Result<String, String> {
    let base_slug = slugify(&title);
    db.read(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let base_slug = base_slug.clone();
        Box::pin(async move {
            let mut counter = 1;
            let mut candidate = base_slug.clone();
            loop {
                if !slug_exists(conn, job_id.as_deref(), project_id.as_deref(), &candidate).await? {
                    return Ok(candidate);
                }
                counter += 1;
                candidate = format!("{base_slug}-{counter}");
            }
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn create_job_terminal(
    orch: &Orchestrator,
    job_id: String,
    cwd: String,
    title: String,
    initial_command: Option<String>,
) -> Result<JobTerminal, String> {
    let started = start_terminal_session(&orch.pty_state, &orch.services, &cwd)?;
    let command = initial_command
        .clone()
        .unwrap_or_else(|| started.shell_path.clone());
    let slug = generate_slug_from_title(
        orch.db.local.clone(),
        Some(job_id.clone()),
        None,
        title.clone(),
    )
    .await?;
    let terminal = JobTerminal {
        id: started.terminal_id.clone(),
        job_id: Some(job_id.clone()),
        project_id: None,
        run_id: None,
        session_id: started.session_id.clone(),
        command,
        title: Some(title.clone()),
        description: None,
        status: "running".to_string(),
        exit_code: None,
        created_at: started.created_at,
        exited_at: None,
        slug: Some(slug.clone()),
    };
    insert_terminal(orch.db.local.clone(), terminal.clone()).await?;
    // Surface the new terminal to connected UIs. The desktop's parallel
    // create path emits its own insert; this covers the runner invoke surface,
    // which mirrors commands through here.
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table":"job_terminals","action":"insert"}),
    );
    hydrate_watchers(orch, &job_id, &slug, &started.output_watchers).await;
    let cleanup_orch = orch.clone();
    let sid = started.session_id.clone();
    let tid = started.terminal_id.clone();
    emit_reader_events(
        orch.services.clone(),
        started.reader,
        started.session_id.clone(),
        started.output_buffer,
        started.command_state,
        Some(started.output_watchers),
        Some(orch.clone()),
        move || {
            let _ = remove_terminal_session(&cleanup_orch.pty_state, &sid);
            let _ = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .and_then(|runtime| {
                    runtime.block_on(delete_terminal(cleanup_orch.db.local.clone(), tid))
                });
            let _ = cleanup_orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table":"job_terminals","action":"delete"}),
            );
        },
    );
    send_initial_command(
        &orch.pty_state,
        &started.session_id,
        initial_command.as_deref(),
    )?;
    Ok(terminal)
}

async fn hydrate_watchers(
    orch: &Orchestrator,
    job_id: &str,
    slug: &str,
    output_watchers: &Arc<Mutex<Vec<TerminalOutputWatcher>>>,
) {
    if let Ok(persisted) =
        crate::orchestrator::wakes::list_terminal_output_watchers_for_job_terminal(
            &orch.db.local,
            job_id,
            slug,
        )
        .await
    {
        if let Ok(mut guard) = output_watchers.lock() {
            for (subscription_id, watcher_job_id, phrase, terminal_uri) in persisted {
                guard.push(TerminalOutputWatcher {
                    subscription_id,
                    job_id: watcher_job_id,
                    phrase,
                    carry: String::new(),
                    terminal_uri,
                });
            }
        }
    }
}

fn send_initial_command(
    state: &PtyState,
    session_id: &str,
    initial_command: Option<&str>,
) -> Result<(), String> {
    if let Some(cmd_str) = initial_command {
        if let Some(session) = state
            .sessions
            .lock()
            .map_err(|e| e.to_string())?
            .get(session_id)
        {
            if let Ok(mut session) = session.lock() {
                if let Some(writer) = session.writer.as_mut() {
                    writer
                        .write_all(format!("{cmd_str}\n").as_bytes())
                        .map_err(|e| e.to_string())?;
                    writer.flush().map_err(|e| e.to_string())?;
                }
            }
        }
    }
    Ok(())
}

pub async fn get_job_terminals(db: &DbState, job_id: String) -> Result<Vec<JobTerminal>, String> {
    load_job_terminals(db.local.clone(), job_id).await
}

async fn terminal_close_info(
    db: Arc<LocalDb>,
    terminal_id: String,
) -> Result<(String, Option<String>), String> {
    db.read(|conn| {
        let terminal_id = terminal_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT session_id, run_id FROM job_terminals WHERE id = ?1 LIMIT 1",
                    params![terminal_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Err(DbError::Row(format!("Terminal not found: {terminal_id}")));
            };
            Ok((row.text(0)?, row.opt_text(1)?))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn close_job_terminal(orch: &Orchestrator, terminal_id: String) -> Result<(), String> {
    let (session_id, run_id) =
        terminal_close_info(orch.db.local.clone(), terminal_id.clone()).await?;
    let is_tracked = orch
        .pty_state
        .sessions
        .lock()
        .ok()
        .and_then(|sessions| sessions.get(&session_id).cloned())
        .and_then(|arc| arc.lock().ok().map(|session| session.is_agent_spawned))
        .unwrap_or(run_id.is_some());
    if is_tracked {
        crate::mcp::handlers::terminal::finalize_terminal_by_session_id(orch, &session_id)?;
        return Ok(());
    }
    if let Some(session_arc) = remove_terminal_session(&orch.pty_state, &session_id)? {
        if let Ok(mut session) = session_arc.lock() {
            let _ = session.child.kill();
            let _ = session.child.wait();
        }
    }
    delete_terminal(orch.db.local.clone(), terminal_id).await?;
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table":"job_terminals","action":"delete"}),
    );
    Ok(())
}

pub async fn get_running_terminals(db: &DbState) -> Result<Vec<ActiveTerminal>, String> {
    db.local.read(|conn| Box::pin(async move {
        let mut all = Vec::new();
        let mut rows = conn.query("SELECT jt.id, jt.session_id, jt.command, jt.job_id, j.issue_id, i.number, i.project_id, jt.slug, j.node_name, e.seq, p.key, jt.created_at FROM job_terminals jt JOIN jobs j ON j.id = jt.job_id JOIN issues i ON i.id = j.issue_id JOIN projects p ON p.id = i.project_id LEFT JOIN executions e ON e.id = j.execution_id WHERE jt.status = 'running' AND jt.job_id IS NOT NULL ORDER BY jt.created_at DESC", ()).await?;
        while let Some(row) = rows.next().await? {
            all.push((row.i64(11)?, ActiveTerminal { id: row.text(0)?, session_id: row.text(1)?, command: row.text(2)?, job_id: row.opt_text(3)?, issue_id: row.opt_text(4)?, issue_number: row.opt_i64(5)?.map(|v| v as i32), project_id: row.text(6)?, slug: row.opt_text(7)?, node_name: row.opt_text(8)?, exec_seq: row.opt_i64(9)?.map(|v| v as i32), project_key: row.text(10)? }));
        }
        let mut rows = conn.query("SELECT jt.id, jt.session_id, jt.command, p.id, p.key, jt.slug, jt.created_at FROM job_terminals jt JOIN projects p ON p.id = jt.project_id WHERE jt.status = 'running' AND jt.project_id IS NOT NULL AND jt.job_id IS NULL ORDER BY jt.created_at DESC", ()).await?;
        while let Some(row) = rows.next().await? {
            all.push((row.i64(6)?, ActiveTerminal { id: row.text(0)?, session_id: row.text(1)?, command: row.text(2)?, job_id: None, issue_id: None, issue_number: None, project_id: row.text(3)?, slug: row.opt_text(5)?, node_name: None, exec_seq: None, project_key: row.text(4)? }));
        }
        all.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(all.into_iter().map(|(_, terminal)| terminal).collect())
    })).await.map_err(|error| error.to_string())
}

pub async fn create_project_terminal(
    orch: &Orchestrator,
    project_id: String,
    title: String,
    initial_command: Option<String>,
) -> Result<JobTerminal, String> {
    let repo_path = project_repo_path(orch.db.local.clone(), project_id.clone()).await?;
    let started = start_terminal_session(&orch.pty_state, &orch.services, &repo_path)?;
    let command = initial_command
        .clone()
        .unwrap_or_else(|| started.shell_path.clone());
    let slug = generate_slug_from_title(
        orch.db.local.clone(),
        None,
        Some(project_id.clone()),
        title.clone(),
    )
    .await?;
    let terminal = JobTerminal {
        id: started.terminal_id.clone(),
        job_id: None,
        project_id: Some(project_id),
        run_id: None,
        session_id: started.session_id.clone(),
        command,
        title: Some(title),
        description: None,
        status: "running".to_string(),
        exit_code: None,
        created_at: started.created_at,
        exited_at: None,
        slug: Some(slug),
    };
    insert_terminal(orch.db.local.clone(), terminal.clone()).await?;
    // Surface the new terminal to connected UIs (see create_job_terminal).
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table":"job_terminals","action":"insert"}),
    );
    emit_reader_events(
        orch.services.clone(),
        started.reader,
        started.session_id.clone(),
        started.output_buffer,
        started.command_state,
        Some(started.output_watchers),
        Some(orch.clone()),
        || {},
    );
    send_initial_command(
        &orch.pty_state,
        &started.session_id,
        initial_command.as_deref(),
    )?;
    Ok(terminal)
}

pub async fn open_project_terminal(
    orch: &Orchestrator,
    project_id: String,
) -> Result<JobTerminal, String> {
    let sessions: HashSet<String> = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| e.to_string())?
        .keys()
        .cloned()
        .collect();
    for terminal in
        load_running_project_terminals(orch.db.local.clone(), project_id.clone()).await?
    {
        if sessions.contains(&terminal.session_id) {
            return Ok(terminal);
        }
    }
    create_project_terminal(orch, project_id, "Terminal".to_string(), None).await
}

async fn load_running_project_terminals(
    db: Arc<LocalDb>,
    project_id: String,
) -> Result<Vec<JobTerminal>, String> {
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn.query(&format!("{TERMINAL_SELECT} WHERE project_id = ?1 AND status = 'running' ORDER BY created_at DESC"), params![project_id.as_str()]).await?;
            let mut terminals = Vec::new();
            while let Some(row) = rows.next().await? { terminals.push(terminal_from_row(&row)?); }
            Ok(terminals)
        })
    }).await.map_err(|error| error.to_string())
}

pub async fn get_project_terminals(
    orch: &Orchestrator,
    project_id: String,
) -> Result<Vec<JobTerminal>, String> {
    let sessions: HashSet<String> = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| e.to_string())?
        .keys()
        .cloned()
        .collect();
    orch.db
        .local
        .write(|conn| {
            let project_id = project_id.clone();
            let sessions = sessions.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        &format!(
                            "{TERMINAL_SELECT} WHERE project_id = ?1 ORDER BY created_at DESC"
                        ),
                        params![project_id.as_str()],
                    )
                    .await?;
                let mut terminals = Vec::new();
                while let Some(row) = rows.next().await? {
                    terminals.push(terminal_from_row(&row)?);
                }
                let mut result = Vec::new();
                for terminal in terminals {
                    if terminal.status == "running" && !sessions.contains(&terminal.session_id) {
                        conn.execute(
                            "DELETE FROM job_terminals WHERE id = ?1",
                            params![terminal.id.as_str()],
                        )
                        .await?;
                    } else {
                        result.push(terminal);
                    }
                }
                Ok(result)
            })
        })
        .await
        .map_err(|error| error.to_string())
}
