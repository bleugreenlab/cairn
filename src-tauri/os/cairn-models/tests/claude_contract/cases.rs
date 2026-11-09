//! The contract cases. Each drives Cairn's own Claude client code
//! (`LocalSessionHost`, `ClaudeCli::open` and `spawn_config`,
//! `ClaudeControl`, the `claude_stream` parser and predicates, and the
//! `session::events::claude` mapping) against the real pinned binary, and
//! returns a [`CaseFailure`] naming itself. Where Cairn branches on the CLI's
//! output, the case asserts the wire predicate Cairn branches with.

use std::path::Path;
use std::time::{Duration, Instant};

use cairn_domain::Model;
use cairn_models::claude::api_key::API_KEY_ENV;
use cairn_models::claude::tokens::OAUTH_TOKEN_ENV;
use cairn_models::interface::session::SecretEnv;
use cairn_models::managed::claude::CLAUDE;
use cairn_models::managed::{parse_cli_version, RELEASE_MARKER};
use cairn_models::session::claude::{API_KEY_FD_ENV, CLAUDE_HARDENING_ENV, OAUTH_TOKEN_FD_ENV};
use cairn_models::session::events::claude::{credential_event, map};
use cairn_models::session::events::{SessionEvent, TurnStatus};
use cairn_models::session::mcp::{McpServer, McpServers, McpTransport};
use cairn_models::wire::claude_stream::init::{builtin_tools, missing_core_verbs};
use cairn_models::wire::claude_stream::{
    extract_tool_start, message_start_usage, stderr_failure, terminal_provider_depletion,
    ClaudeEvent, ControlResponseInner, ProviderDepletion, StderrFailure, StreamEventInner,
    StreamEvidence,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::env::{
    fixture, CaseFailure, CaseResult, Check, ContractEnv, Credential, Frame, Launch,
    LaunchEnvironment, Mcp, Session, ANCESTOR_CANARY, ANCESTOR_SKILL_CANARY, API_KEY, MCP_BEARER,
    OPERATOR_CANARY, OPERATOR_SKILL_CANARY,
};
use crate::fake_messages::{
    Mode, FAKE_TEXT, FAKE_THINKING, MCP_TOOL_TEXT, STRUCTURED_INPUT, STRUCTURED_SCHEMA,
    TOOL_USE_ID, USAGE_CACHE_CREATION, USAGE_CACHE_READ, USAGE_INPUT, USAGE_OUTPUT,
};

/// Tools the CLI may offer the model beside MCP tools under `--tools ""`:
/// its deferred-tool placeholder, and the `--json-schema` answer channel.
/// Anything else bare is a built-in, which `--tools ""` must keep off.
const TOOL_EXEMPTIONS: [&str; 2] = ["DeferredToolPlaceholder", "StructuredOutput"];

/// Hosts the CLI's updater reaches for.
const UPDATE_HOSTS: [&str; 2] = ["downloads.claude.ai", "storage.googleapis.com"];

fn launch(c: Check, env: &ContractEnv, launch: Launch) -> Result<Session, CaseFailure> {
    c.setup(env.launch(launch), "launching through ClaudeCli::open")
}

fn init_of(
    c: Check,
    session: &Session,
) -> Result<(Frame, cairn_models::wire::claude_stream::Init), CaseFailure> {
    session.init().ok_or_else(|| {
        c.fail(format!(
            "no system/init event parsed as one\n{}",
            session.report()
        ))
    })
}

fn sha256(path: &Path) -> Result<String, String> {
    let bytes = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

/// `<cli> --version` under the launch hardening, as the install probe runs it.
fn printed_version(cli: &Path) -> Result<String, String> {
    let output = std::process::Command::new(cli)
        .arg("--version")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", std::env::temp_dir())
        .envs(CLAUDE_HARDENING_ENV.iter().copied())
        .output()
        .map_err(|error| error.to_string())?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn managed_install_of_pinned_binary(env: &ContractEnv) -> CaseResult {
    let c = Check("managed_install_of_pinned_binary");
    let pin = CLAUDE.embedded();
    let managed = c.setup(env.fresh_managed(), "staging a fresh install root")?;
    let target = c.setup(
        managed.target().ok_or("no Claude Code build for this host"),
        "naming the host target",
    )?;
    let resolved = c.ok(
        managed.ensure(),
        "installing the staged pinned binary through ManagedTool::ensure",
    )?;
    let release = managed.root().join(format!("{}-{target}", pin.version));
    c.that(resolved.path == release.join("claude"), || {
        format!(
            "installed at {}, not {}",
            resolved.path.display(),
            release.join("claude").display()
        )
    })?;
    c.that(resolved.version == pin.version, || {
        format!(
            "installed {} but the pin is {}",
            resolved.version, pin.version
        )
    })?;
    let marker: Value = c.ok(
        std::fs::read(release.join(RELEASE_MARKER))
            .map_err(|error| error.to_string())
            .and_then(|bytes| serde_json::from_slice(&bytes).map_err(|error| error.to_string())),
        "reading the release marker",
    )?;
    let package = c.setup(
        pin.packages
            .get(target)
            .ok_or("the pin names no package for this host"),
        "reading the pin",
    )?;
    c.that(
        marker["version"] == pin.version.as_str()
            && marker["target"] == target
            && marker["entrypoint"] == "claude"
            && marker["sha256"] == package.sha256.as_str(),
        || format!("the release marker {marker} does not match the pin"),
    )?;
    let installed = c.ok(sha256(&resolved.path), "hashing the installed binary")?;
    c.that(installed == package.sha256, || {
        format!(
            "the installed binary's sha256 is {installed}, the pin's {}",
            package.sha256
        )
    })?;
    let printed = c.ok(
        printed_version(&resolved.path),
        "running the installed claude --version",
    )?;
    c.that(
        parse_cli_version(&printed) == Some(pin.version.as_str()),
        || {
            format!(
                "`claude --version` printed {printed:?}, not {}",
                pin.version
            )
        },
    )
}

pub fn init_reports_pinned_version_session_and_model(env: &ContractEnv) -> CaseResult {
    let c = Check("init_reports_pinned_version_session_and_model");
    let pin = CLAUDE.embedded();
    let mut session = launch(c, env, Launch::new().model(Model::OPUS))?;
    session.ok_turn(c, "Say hello.")?;
    let (frame, init) = init_of(c, &session)?;
    c.that(
        init.claude_code_version.as_deref() == Some(pin.version.as_str()),
        || {
            format!(
                "init reports claude_code_version {:?}, the pin is {}",
                init.claude_code_version, pin.version
            )
        },
    )?;
    c.that(
        init.session_id.as_deref() == Some(session.session_id.as_str()),
        || {
            format!(
                "init reports session {:?}, Cairn launched --session-id {}",
                init.session_id, session.session_id
            )
        },
    )?;
    let started = frame.event.as_ref().map(map).unwrap_or_default();
    let Some(SessionEvent::Started {
        backend_session_id,
        model,
    }) = started.first()
    else {
        return Err(c.fail(format!("init mapped to {started:?}, not Started")));
    };
    c.that(
        backend_session_id.as_deref() == Some(session.session_id.as_str()),
        || format!("Started carries session {backend_session_id:?}"),
    )?;
    c.that(model.is_some() && *model == init.model, || {
        format!("Started carries model {model:?}, init {:?}", init.model)
    })?;
    let spawned = env.tap().spawned();
    let asked = spawned.last().and_then(|argv| {
        argv.iter()
            .position(|arg| arg == "--model")
            .and_then(|index| argv.get(index + 1).cloned())
    });
    c.that(asked.as_deref() == Some("opus[1m]"), || {
        format!("Cairn launched --model {asked:?}, not its opus[1m] alias")
    })?;
    c.that(
        model.as_deref().is_some_and(|model| model.contains("opus")),
        || format!("the CLI resolved Cairn's opus[1m] to {model:?}"),
    )
}

pub fn init_declares_cairn_core_verbs(env: &ContractEnv) -> CaseResult {
    let c = Check("init_declares_cairn_core_verbs");
    let mut session = launch(c, env, Launch::new())?;
    session.ok_turn(c, "Say hello.")?;
    let (_, init) = init_of(c, &session)?;
    let missing = missing_core_verbs(&init);
    c.that(missing.is_empty(), || {
        format!(
            "init declares {:?}, missing Cairn's {missing:?}",
            init.tools
        )
    })?;
    let status = init.mcp_server_statuses("cairn");
    c.that(status == [Some("connected")], || {
        format!("init reports Cairn's server as {status:?}")
    })?;
    let calls = env.mcp.calls();
    c.that(
        calls
            .iter()
            .any(|(method, bearer)| method == "tools/list" && bearer == MCP_BEARER),
        || format!("the CLI never listed Cairn's tools with the run bearer: {calls:?}"),
    )
}

pub fn tools_empty_offers_only_mcp_tools(env: &ContractEnv) -> CaseResult {
    let c = Check("tools_empty_offers_only_mcp_tools");
    let mut session = launch(c, env, Launch::new())?;
    session.ok_turn(c, "Say hello.")?;
    let (frame, init) = init_of(c, &session)?;
    env.record_init("tools_none", &frame.raw);
    let builtins = builtin_tools(&init);
    c.that(builtins.is_empty(), || {
        format!("under --tools \"\" init declares built-ins {builtins:?}")
    })?;
    let requests = env.fake.inference();
    c.that(!requests.is_empty(), || {
        "no inference request reached the fake".to_string()
    })?;
    for request in requests {
        let offered: Vec<String> = request
            .tools()
            .into_iter()
            .filter(|tool| !tool.starts_with("mcp__") && !TOOL_EXEMPTIONS.contains(&tool.as_str()))
            .collect();
        c.that(offered.is_empty(), || {
            format!("the model was offered built-ins {offered:?} (exempt: {TOOL_EXEMPTIONS:?})")
        })?;
    }
    Ok(())
}

pub fn setting_sources_user_loads_no_project_or_operator_config(env: &ContractEnv) -> CaseResult {
    let c = Check("setting_sources_user_loads_no_project_or_operator_config");
    let mut session = launch(c, env, Launch::new())?;
    session.ok_turn(c, "Say hello.")?;
    let (frame, init) = init_of(c, &session)?;
    // Hooks run as the turn starts; give a straggler a moment to land.
    std::thread::sleep(Duration::from_secs(2));
    let raw = frame.raw.to_string();
    for (what, canary) in [
        ("the planted project skill", "planted"),
        ("the operator's skill", "operator-skill"),
    ] {
        c.that(!raw.contains(canary), || {
            format!("init lists {what}: {raw}")
        })?;
    }
    for (what, canary) in [
        ("the project ancestor's CLAUDE.md", ANCESTOR_CANARY),
        ("the project skill", ANCESTOR_SKILL_CANARY),
        ("the operator's CLAUDE.md", OPERATOR_CANARY),
        ("the operator's skill", OPERATOR_SKILL_CANARY),
    ] {
        c.that(!env.fake.sent_to_the_model(canary), || {
            format!("{what} was sent to the model")
        })?;
    }
    let servers: Vec<&str> = init
        .mcp_servers
        .iter()
        .map(|server| server.name.as_str())
        .collect();
    c.that(servers == ["cairn"], || {
        format!("init attached {servers:?}; only Cairn's server is configured")
    })?;
    let markers = markers_in(&env.markers);
    c.that(markers.is_empty(), || {
        format!("project or operator hooks, statusLine or MCP servers ran: {markers:?}")
    })
}

fn markers_in(markers: &Path) -> Vec<String> {
    std::fs::read_dir(markers)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

pub fn normal_turn_streams_and_completes(env: &ContractEnv) -> CaseResult {
    let c = Check("normal_turn_streams_and_completes");
    env.fake.queue([Mode::Text]);
    let mut session = launch(c, env, Launch::new())?;
    let result = session.ok_turn(c, "Say hello.")?;

    let mapped: Vec<SessionEvent> = session.events().flat_map(map).collect();
    let text: String = mapped
        .iter()
        .filter_map(|event| match event {
            SessionEvent::TextDelta { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    c.that(text.contains(FAKE_TEXT), || {
        format!("the text deltas read {text:?}, not {FAKE_TEXT:?}")
    })?;
    let thinking: String = mapped
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ThinkingDelta { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    c.that(thinking.contains(FAKE_THINKING), || {
        format!("the thinking deltas read {thinking:?}, not {FAKE_THINKING:?}")
    })?;

    let started = session.events().find_map(|event| match event {
        ClaudeEvent::StreamEvent {
            inner: StreamEventInner::MessageStart { message },
            ..
        } => message_start_usage(message.as_ref()),
        _ => None,
    });
    let started = c.some(started, "a message_start carrying usage")?;
    c.that(
        started.input_tokens == USAGE_INPUT
            && started.cache_read_input_tokens == Some(USAGE_CACHE_READ)
            && started.cache_creation_input_tokens == Some(USAGE_CACHE_CREATION),
        || format!("message_start usage read as {started:?}"),
    )?;
    let delta_output = session.events().find_map(|event| match event {
        ClaudeEvent::StreamEvent {
            inner: StreamEventInner::MessageDelta { usage, .. },
            ..
        } => usage
            .as_ref()
            .and_then(|usage| usage["output_tokens"].as_u64()),
        _ => None,
    });
    c.that(delta_output == Some(u64::from(USAGE_OUTPUT)), || {
        format!("message_delta reported output_tokens {delta_output:?}")
    })?;

    let Some(ClaudeEvent::Result {
        usage,
        total_cost_usd,
        ..
    }) = &result.event
    else {
        return Err(c.fail(format!("the result did not parse: {}", result.raw)));
    };
    let usage = c.some(usage.clone(), "usage on the result")?;
    c.that(
        usage.input_tokens == USAGE_INPUT
            && usage.output_tokens == USAGE_OUTPUT
            && usage.cache_read_input_tokens == Some(USAGE_CACHE_READ)
            && usage.cache_creation_input_tokens == Some(USAGE_CACHE_CREATION),
        || {
            format!(
                "the result's usage read as {usage:?} from {}",
                result.raw["usage"]
            )
        },
    )?;
    c.that(
        usage
            .output_tokens_details
            .as_ref()
            .is_some_and(|details| details.thinking_tokens.is_some()),
        || {
            format!(
                "the result carries no output_tokens_details.thinking_tokens: {}",
                result.raw["usage"]
            )
        },
    )?;
    c.that(total_cost_usd.is_some(), || {
        "the result carries no total_cost_usd".to_string()
    })?;
    let complete = result.event.as_ref().map(map).unwrap_or_default();
    c.that(
        matches!(
            complete.first(),
            Some(SessionEvent::TurnComplete {
                status: TurnStatus::Completed,
                usage: Some(_),
                cost_usd: Some(_),
                ..
            })
        ),
        || format!("the result mapped to {complete:?}"),
    )
}

pub fn result_is_error_is_authoritative(env: &ContractEnv) -> CaseResult {
    let c = Check("result_is_error_is_authoritative");
    env.fake.queue([Mode::Http {
        status: 500,
        rate_limit_headers: false,
        unified_status: "",
    }]);
    let mut session = launch(c, env, Launch::new().retries(0))?;
    let result = session.run_turn(c, "Say hello.")?;
    eprintln!("a 500 ends the turn with subtype {:?}", result.subtype());
    c.that(
        matches!(
            result.event,
            Some(ClaudeEvent::Result { is_error: true, .. })
        ),
        || format!("the failed turn's result is not is_error: {}", result.raw),
    )?;
    let complete = result.event.as_ref().map(map).unwrap_or_default();
    c.that(
        matches!(
            complete.first(),
            Some(SessionEvent::TurnComplete {
                status: TurnStatus::Failed,
                error: Some(_),
                ..
            })
        ),
        || format!("Cairn read the failed turn as {complete:?}"),
    )
}

/// The depletion the pending assistant error and the terminal result of a
/// turn classify to, as the reader folds them.
fn depletion(session: &Session) -> Option<ProviderDepletion> {
    let mut pending = None;
    for event in session.events() {
        if let Some(found) = event.provider_depletion() {
            pending = Some(found);
        }
        if let ClaudeEvent::Result { is_error, data, .. } = event {
            return terminal_provider_depletion(*is_error, data, pending);
        }
    }
    None
}

pub fn api_error_429_is_rolling_window(env: &ContractEnv) -> CaseResult {
    let c = Check("api_error_429_is_rolling_window");
    env.fake.queue([Mode::Http {
        status: 429,
        rate_limit_headers: false,
        unified_status: "",
    }]);
    let mut session = launch(
        c,
        env,
        Launch::new()
            .credential(Credential::ApiKey(API_KEY))
            .retries(0),
    )?;
    session.run_turn(c, "Say hello.")?;
    let assistant = session.events().find_map(ClaudeEvent::provider_depletion);
    c.that(
        assistant == Some(ProviderDepletion::RollingWindow { resets_at: None }),
        || {
            format!(
                "the CLI's 429 assistant error classified as {assistant:?}\n{}",
                session.report()
            )
        },
    )?;
    let terminal = depletion(&session);
    c.that(
        terminal == Some(ProviderDepletion::RollingWindow { resets_at: None }),
        || format!("the 429 turn ended as {terminal:?}\n{}", session.report()),
    )
}

pub fn api_retry_401_then_pushed_token_rescues_turn(env: &ContractEnv) -> CaseResult {
    let c = Check("api_retry_401_then_pushed_token_rescues_turn");
    let mut session = launch(
        c,
        env,
        Launch::new().credential(Credential::OAuth("at-OLD")),
    )?;
    session.ok_turn(c, "turn one")?;
    let bearer = || {
        env.fake
            .inference()
            .last()
            .map(|served| served.bearer.clone())
    };
    c.that(bearer().as_deref() == Some("at-OLD"), || {
        format!("turn one carried {:?}", bearer())
    })?;
    holds_no_secret(
        c,
        &session,
        env,
        &["at-OLD"],
        (OAUTH_TOKEN_ENV, OAUTH_TOKEN_FD_ENV),
    )?;

    // Between turns: the old token is refused, and the pushed one is what
    // the next turn sends.
    env.fake.refuse("at-OLD");
    push(c, &mut session, "at-NEW", 2)?;
    session.ok_turn(c, "turn two")?;
    c.that(bearer().as_deref() == Some("at-NEW"), || {
        format!("turn two carried {:?}", bearer())
    })?;

    // Mid-turn: the current token is refused, the CLI reports the 401 and
    // waits, and a token pushed then carries the same turn's retry.
    env.fake.refuse("at-NEW");
    c.ok(session.turn("turn three"), "sending turn three")?;
    let rejected = session.until(Duration::from_secs(60), |frame| {
        frame
            .event
            .as_ref()
            .and_then(credential_event)
            .is_some_and(|event| matches!(event, SessionEvent::CredentialRejected))
    });
    c.some(
        rejected,
        "a system/api_retry for the 401 read as CredentialRejected",
    )?;
    c.ok(
        session
            .control
            .update_credential(&SecretEnv::new().with(OAUTH_TOKEN_ENV, "at-NEWER"), 3),
        "pushing generation 3",
    )?;
    let third = session.result(c)?;
    c.that(
        matches!(
            third.event,
            Some(ClaudeEvent::Result {
                is_error: false,
                ..
            })
        ),
        || format!("the rescued turn failed: {}", third.raw),
    )?;
    c.that(bearer().as_deref() == Some("at-NEWER"), || {
        format!("the rescued turn carried {:?}", bearer())
    })?;
    holds_no_secret(
        c,
        &session,
        env,
        &["at-OLD", "at-NEW", "at-NEWER"],
        (OAUTH_TOKEN_ENV, OAUTH_TOKEN_FD_ENV),
    )?;
    no_token_fetched(c, env)
}

/// Push `token` as `generation` and wait for the CLI's acknowledgement, read
/// as Cairn reads it.
fn push(c: Check, session: &mut Session, token: &str, generation: u64) -> CaseResult {
    c.ok(
        session
            .control
            .update_credential(&SecretEnv::new().with(OAUTH_TOKEN_ENV, token), generation),
        "pushing a credential through ClaudeControl::update_credential",
    )?;
    let acknowledged = session.until(Duration::from_secs(30), |frame| {
        frame.event.as_ref().and_then(credential_event).is_some_and(|event| {
            matches!(event, SessionEvent::CredentialAcknowledged { generation: seen } if seen == generation)
        })
    });
    c.some(
        acknowledged,
        &format!("an acknowledgement of generation {generation}"),
    )
    .map(|_| ())
}

/// The CLI's launch environment holds none of `secrets` and does not name
/// the credential's variable; it does name the descriptor variable. The
/// fake's address, which the launch does put there, is the control that the
/// environment was read at all.
fn holds_no_secret(
    c: Check,
    session: &Session,
    env: &ContractEnv,
    secrets: &[&str],
    (credential_env, descriptor_env): (&str, &str),
) -> CaseResult {
    let pid = c.some(session.pid(), "the CLI's pid")?;
    let launched = LaunchEnvironment::of(pid);
    c.that(launched.holds(&env.fake.base_url()), || {
        "the launch environment reader read nothing".to_string()
    })?;
    for secret in secrets {
        c.that(!launched.holds(secret), || {
            "a credential is in the CLI's launch environment".to_string()
        })?;
    }
    c.that(!launched.names().any(|name| name == credential_env), || {
        format!("the CLI's launch environment names {credential_env}")
    })?;
    let told = launched.names().any(|name| name == descriptor_env);
    c.that(told, || {
        format!("the CLI was not told which descriptor holds its credential ({descriptor_env})")
    })
}

/// Nothing asked for a token: no request reached an OAuth path, through the
/// fake or the trap, and the CLI wrote no credential into its home.
fn no_token_fetched(c: Check, env: &ContractEnv) -> CaseResult {
    let paths: Vec<String> = env
        .fake
        .served()
        .into_iter()
        .map(|served| served.target)
        .collect();
    c.that(!paths.iter().any(|path| path.contains("oauth")), || {
        format!("the CLI called a token endpoint: {paths:?}")
    })?;
    let egress = env.fake.egress();
    c.that(
        !egress.iter().any(|target| {
            ["platform.claude.com", "console.anthropic.com", "claude.ai"]
                .iter()
                .any(|host| target.contains(host))
        }),
        || format!("the CLI reached for a token endpoint: {egress:?}"),
    )?;
    let written: Vec<String> = std::fs::read_dir(&env.config)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .filter(|name| name.contains("credentials"))
                .collect()
        })
        .unwrap_or_default();
    c.that(written.is_empty(), || {
        format!("the CLI wrote {written:?} into its home")
    })
}

pub fn rate_limit_event_shape_parses(env: &ContractEnv) -> CaseResult {
    let c = Check("rate_limit_event_shape_parses");
    env.fake.queue([Mode::RateLimitHeaders]);
    let mut session = launch(c, env, Launch::new())?;
    session.ok_turn(c, "Say hello.")?;
    let info = session.events().find_map(|event| match event {
        ClaudeEvent::RateLimitEvent { rate_limit_info } => Some(rate_limit_info.clone()),
        _ => None,
    });
    let info = c.some(
        info,
        &format!("a rate_limit_event under OAuth\n{}", session.report()),
    )?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default();
    c.that(info.status == "allowed_warning", || {
        format!("rate_limit_info.status read as {:?}", info.status)
    })?;
    c.that(info.resets_at.is_some_and(|reset| reset > now), || {
        format!("rate_limit_info.resetsAt read as {:?}", info.resets_at)
    })?;
    c.that(info.rate_limit_type.as_deref() == Some("five_hour"), || {
        format!(
            "rate_limit_info.rateLimitType read as {:?}",
            info.rate_limit_type
        )
    })?;
    c.that(!info.is_blocking(), || {
        "allowed_warning read as blocking".to_string()
    })?;
    let mapped: Vec<SessionEvent> = session.events().flat_map(map).collect();
    c.that(
        mapped
            .iter()
            .any(|event| matches!(event, SessionEvent::RateLimit { info } if info.status == "allowed_warning")),
        || "the rate_limit_event mapped to no RateLimit event".to_string(),
    )
}

pub fn rate_limit_rejected_is_blocking(env: &ContractEnv) -> CaseResult {
    let c = Check("rate_limit_rejected_is_blocking");
    env.fake.queue([Mode::Http {
        status: 429,
        rate_limit_headers: true,
        unified_status: "rejected",
    }]);
    let mut session = launch(c, env, Launch::new().retries(0))?;
    session.run_turn(c, "Say hello.")?;
    let info = session.events().find_map(|event| match event {
        ClaudeEvent::RateLimitEvent { rate_limit_info } => Some(rate_limit_info.clone()),
        _ => None,
    });
    let info = c.some(
        info,
        &format!(
            "a rate_limit_event for the rejected 429\n{}",
            session.report()
        ),
    )?;
    c.that(info.is_blocking(), || {
        format!(
            "the rejected 429's rate_limit_event (status {:?}) is not blocking",
            info.status
        )
    })?;
    let mut evidence = StreamEvidence::default();
    for event in session.events() {
        evidence.observe(event);
    }
    c.that(evidence.saw_blocking_rate_limit, || {
        "the stream evidence saw no blocking rate limit".to_string()
    })?;
    let terminal = depletion(&session);
    c.that(
        terminal == Some(ProviderDepletion::RollingWindow { resets_at: None }),
        || format!("the rejected 429 ended the turn as {terminal:?}"),
    )
}

/// Wait for the `control_response` answering the last `subtype` request
/// Cairn sent, read through the parser's request-id normalization.
fn acknowledgement(
    c: Check,
    session: &mut Session,
    subtype: &str,
) -> Result<ControlResponseInner, CaseFailure> {
    let id = c.some(
        session.tap().last_control_request(subtype),
        &format!("Cairn's {subtype} control request on stdin"),
    )?;
    let answer = session.until(Duration::from_secs(30), |frame| {
        frame
            .event
            .as_ref()
            .and_then(ClaudeEvent::control_request_id)
            == Some(id.as_str())
    });
    let answer = c.some(
        answer,
        &format!(
            "a control_response for {subtype} request {id}\n{}",
            session.report()
        ),
    )?;
    match answer.event {
        Some(ClaudeEvent::ControlResponse { response, .. }) => Ok(response),
        _ => Err(c.fail(format!(
            "the {subtype} answer is not a control_response: {}",
            answer.raw
        ))),
    }
}

pub fn interrupt_is_acknowledged_and_session_survives(env: &ContractEnv) -> CaseResult {
    let c = Check("interrupt_is_acknowledged_and_session_survives");
    env.fake.queue([Mode::Slow]);
    let mut session = launch(c, env, Launch::new())?;
    let pid = session.pid();
    c.ok(session.turn("Count slowly."), "sending the slow turn")?;
    let streaming = session.until(Duration::from_secs(60), |frame| {
        frame
            .event
            .as_ref()
            .map(map)
            .unwrap_or_default()
            .iter()
            .any(|event| matches!(event, SessionEvent::TextDelta { .. }))
    });
    c.some(streaming, "the slow turn streaming")?;
    c.ok(
        session.control.interrupt(),
        "interrupting through ClaudeControl",
    )?;
    let response = acknowledgement(c, &mut session, "interrupt")?;
    c.that(
        matches!(response, ControlResponseInner::Success { .. }),
        || format!("the interrupt was answered {response:?}"),
    )?;
    let result = session.result(c)?;
    c.that(
        matches!(
            result.event,
            Some(ClaudeEvent::Result { is_error: true, .. })
        ) && result.raw["terminal_reason"] == "aborted_streaming",
        || format!("the interrupted turn ended {}", result.raw),
    )?;
    let next = session.ok_turn(c, "Again, quickly.")?;
    c.that(next.event.is_some(), || {
        "the next turn's result did not parse".to_string()
    })?;
    c.that(session.alive() && session.pid() == pid, || {
        "the CLI did not survive the interrupt".to_string()
    })
}

pub fn set_permission_mode_and_set_model_acknowledged(env: &ContractEnv) -> CaseResult {
    let c = Check("set_permission_mode_and_set_model_acknowledged");
    let mut session = launch(c, env, Launch::new())?;
    session.ok_turn(c, "Say hello.")?;
    c.ok(
        session.control.set_permission_mode("acceptEdits"),
        "setting the permission mode",
    )?;
    let mode = acknowledgement(c, &mut session, "set_permission_mode")?;
    c.that(matches!(mode, ControlResponseInner::Success { .. }), || {
        format!("set_permission_mode acceptEdits was answered {mode:?}")
    })?;
    c.ok(
        session.control.set_model(Model::SONNET),
        "setting the model",
    )?;
    let model = acknowledgement(c, &mut session, "set_model")?;
    c.that(
        matches!(model, ControlResponseInner::Success { .. }),
        || format!("set_model {} was answered {model:?}", Model::SONNET),
    )?;
    c.ok(
        session.control.set_permission_mode("bogusMode"),
        "setting an invalid mode",
    )?;
    let refused = acknowledgement(c, &mut session, "set_permission_mode")?;
    c.that(
        matches!(&refused, ControlResponseInner::Error { message: Some(_) }),
        || format!("an invalid mode was answered {refused:?}"),
    )?;
    // A refused control request is logged, never fatal: the session serves on.
    session.ok_turn(c, "Still there?").map(|_| ())
}

pub fn mcp_tool_round_trip(env: &ContractEnv) -> CaseResult {
    let c = Check("mcp_tool_round_trip");
    env.fake.queue([Mode::Tool {
        name: "mcp__cairn__read",
        args: r#"{"paths":["file:README.md"]}"#,
    }]);
    let mut session = launch(c, env, Launch::new())?;
    session.ok_turn(c, "Read the README.")?;

    let started = session.events().find_map(|event| match event {
        ClaudeEvent::StreamEvent {
            inner:
                StreamEventInner::ContentBlockStart {
                    content_block: Some(block),
                    ..
                },
            ..
        } => extract_tool_start(block),
        _ => None,
    });
    let (id, name) = c.some(started, "a content_block_start opening the tool call")?;
    c.that(
        id.as_deref() == Some(TOOL_USE_ID) && name == "mcp__cairn__read",
        || format!("the tool call opened as {id:?} {name:?}"),
    )?;
    let mapped: Vec<SessionEvent> = session.events().flat_map(map).collect();
    c.that(
        mapped.iter().any(|event| {
            matches!(event, SessionEvent::ToolCall { id, name, .. } if id == TOOL_USE_ID && name == "mcp__cairn__read")
        }),
        || "no ToolCall mapped from the assistant message".to_string(),
    )?;
    let result = mapped.iter().find_map(|event| match event {
        SessionEvent::ToolResult {
            tool_use_id,
            content,
            is_error,
        } if tool_use_id == TOOL_USE_ID => Some((content.clone(), *is_error)),
        _ => None,
    });
    let (content, is_error) = c.some(
        result,
        &format!("a ToolResult for {TOOL_USE_ID}\n{}", session.report()),
    )?;
    c.that(content.contains(MCP_TOOL_TEXT) && !is_error, || {
        format!("the tool result read as {content:?} (is_error {is_error})")
    })?;
    let call = env
        .mcp
        .bodies()
        .into_iter()
        .find(|body| body["method"] == "tools/call");
    let call = c.some(call, "a tools/call on Cairn's MCP server")?;
    c.that(call["params"]["name"] == "read", || {
        format!("the CLI called {call}")
    })?;
    let token = &call["params"]["_meta"]["progressToken"];
    c.that(token.is_string() || token.is_number(), || {
        format!("the CLI sent no progress token: {call}")
    })?;
    c.that(
        env.fake.inference().iter().any(|served| {
            served
                .tool_results()
                .iter()
                .any(|block| block["tool_use_id"] == TOOL_USE_ID)
        }),
        || "no request carried the tool result back to the model".to_string(),
    )
}

pub fn structured_output_survives_tools_empty(env: &ContractEnv) -> CaseResult {
    let c = Check("structured_output_survives_tools_empty");
    env.fake.queue([Mode::StructuredOutput]);
    let schema: Value = serde_json::from_str(STRUCTURED_SCHEMA).unwrap();
    let expected: Value = serde_json::from_str(STRUCTURED_INPUT).unwrap();
    let mut session = launch(
        c,
        env,
        Launch::new().one_shot("What is the answer?").schema(schema),
    )?;
    let result = session.result(c)?;
    let (frame, init) = init_of(c, &session)?;
    env.record_init("json_schema", &frame.raw);
    let builtins = builtin_tools(&init);
    c.that(builtins.is_empty(), || {
        format!("the schema call declares built-ins {builtins:?}")
    })?;
    c.that(result.raw["structured_output"] == expected, || {
        format!(
            "the result carries structured_output {}",
            result.raw["structured_output"]
        )
    })?;
    let complete = result.event.as_ref().map(map).unwrap_or_default();
    c.that(
        matches!(complete.first(), Some(SessionEvent::TurnComplete { structured: Some(value), .. }) if *value == expected),
        || format!("the result mapped to {complete:?}"),
    )?;
    for request in env.fake.inference() {
        let offered: Vec<String> = request
            .tools()
            .into_iter()
            .filter(|tool| !TOOL_EXEMPTIONS.contains(&tool.as_str()))
            .collect();
        c.that(offered.is_empty(), || {
            format!("the schema call offered the model {offered:?}")
        })?;
    }
    Ok(())
}

/// The last inference request sent `needle` to the model.
fn last_request_holds(env: &ContractEnv, needle: &str) -> bool {
    env.fake
        .inference()
        .last()
        .is_some_and(|served| served.body.to_string().contains(needle))
}

pub fn resume_existing_session(env: &ContractEnv) -> CaseResult {
    let c = Check("resume_existing_session");
    const CANARY: &str = "RESUME-CANARY-first-turn";
    let mut first = launch(c, env, Launch::new())?;
    let id = first.session_id.clone();
    first.ok_turn(c, CANARY)?;
    first.close();

    let mut resumed = launch(c, env, Launch::new().resume(&id))?;
    resumed.ok_turn(c, "second turn")?;
    let (_, init) = init_of(c, &resumed)?;
    c.that(init.session_id.as_deref() == Some(id.as_str()), || {
        format!(
            "the resumed session runs as {:?}, not {id}",
            init.session_id
        )
    })?;
    c.that(last_request_holds(env, CANARY), || {
        "the resumed turn did not carry the first turn's history".to_string()
    })
}

pub fn fork_session_creates_new_id(env: &ContractEnv) -> CaseResult {
    let c = Check("fork_session_creates_new_id");
    const CANARY: &str = "FORK-CANARY-source-turn";
    let mut source = launch(c, env, Launch::new())?;
    let id = source.session_id.clone();
    source.ok_turn(c, CANARY)?;
    source.close();

    let fork_id = uuid::Uuid::new_v4().to_string();
    let mut fork = launch(c, env, Launch::new().fork(&id, &fork_id))?;
    fork.ok_turn(c, "forked turn")?;
    let (_, init) = init_of(c, &fork)?;
    c.that(init.session_id.as_deref() == Some(fork_id.as_str()), || {
        format!(
            "the fork runs as {:?}, not the --session-id {fork_id} Cairn gave it",
            init.session_id
        )
    })?;
    c.that(last_request_holds(env, CANARY), || {
        "the fork did not carry its source's history".to_string()
    })
}

/// The committed transcript an older CLI wrote, and the canaries it holds.
const OLDER_TRANSCRIPT: &str = "claude_session-2.1.302.jsonl";
const OLDER_USER_CANARY: &str = "OLDER-CLI-CANARY-user-turn";
const OLDER_ASSISTANT_CANARY: &str = "OLDER-CLI-CANARY-assistant-turn";

pub fn resume_transcript_from_an_older_cli(env: &ContractEnv) -> CaseResult {
    let c = Check("resume_transcript_from_an_older_cli");
    let transcript = c.setup(
        std::fs::read_to_string(fixture(OLDER_TRANSCRIPT)),
        "reading the older CLI's transcript fixture",
    )?;
    let id = transcript
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|line| line["sessionId"].as_str().map(str::to_string));
    let id = c.setup(id.ok_or("no sessionId"), "reading the fixture's session id")?;

    // The CLI keeps a cwd's conversations under a directory it names for the
    // cwd; a session the pinned CLI starts here shows which.
    let mut probe = launch(c, env, Launch::new())?;
    let probe_id = probe.session_id.clone();
    probe.ok_turn(c, "probe")?;
    probe.close();
    let projects = env.config.join("projects");
    let slug_dir = std::fs::read_dir(&projects)
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .find(|dir| dir.join(format!("{probe_id}.jsonl")).is_file());
    let slug_dir = c.setup(
        slug_dir.ok_or("the pinned CLI kept no transcript"),
        "finding where the pinned CLI keeps this cwd's transcripts",
    )?;
    c.setup(
        std::fs::write(slug_dir.join(format!("{id}.jsonl")), &transcript),
        "planting the older transcript",
    )?;

    let mut resumed = launch(c, env, Launch::new().resume(&id))?;
    resumed.ok_turn(c, "Continue from where we were.")?;
    let (_, init) = init_of(c, &resumed)?;
    c.that(init.session_id.as_deref() == Some(id.as_str()), || {
        format!(
            "the older session resumed as {:?}, not {id}",
            init.session_id
        )
    })?;
    c.that(
        last_request_holds(env, OLDER_USER_CANARY)
            && last_request_holds(env, OLDER_ASSISTANT_CANARY),
        || "the resumed turn did not carry the older CLI's user and assistant history".to_string(),
    )
}

/// Read stderr until a line proves `failure` through Cairn's predicate.
fn stderr_proves(session: &mut Session, failure: StderrFailure) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if session
            .stderr()
            .iter()
            .any(|line| stderr_failure(line) == Some(failure))
        {
            return true;
        }
        session.drain(Duration::from_millis(200));
    }
    false
}

pub fn resume_unknown_is_session_unresolvable(env: &ContractEnv) -> CaseResult {
    let c = Check("resume_unknown_is_session_unresolvable");
    let unknown = uuid::Uuid::new_v4().to_string();
    let mut session = launch(c, env, Launch::new().resume(&unknown))?;
    // The CLI may exit before it reads this; a refused write is expected.
    let _ = session.turn("Hello?");
    c.that(
        stderr_proves(&mut session, StderrFailure::SessionUnresolvable),
        || {
            format!(
                "no stderr line read as SessionUnresolvable\n{}",
                session.report()
            )
        },
    )?;
    let status = session.wait_exit(Duration::from_secs(30));
    c.that(status.is_some_and(|status| !status.success()), || {
        format!("resuming an unknown session exited {status:?}")
    })
}

pub fn session_id_in_use_is_classified(env: &ContractEnv) -> CaseResult {
    let c = Check("session_id_in_use_is_classified");
    let mut first = launch(c, env, Launch::new())?;
    let id = first.session_id.clone();
    first.ok_turn(c, "Create the conversation.")?;
    first.close();
    let mut again = launch(c, env, Launch::new().session(&id))?;
    let _ = again.turn("Hello again?");
    c.that(
        stderr_proves(&mut again, StderrFailure::SessionIdInUse),
        || format!("no stderr line read as SessionIdInUse\n{}", again.report()),
    )
}

/// Records, for whatever runs it, whether its environment names or holds a
/// token and what it reads from descriptor 3. Run as `probe.sh <dir> <who>`.
const PROBE: &str = r#"dir=$1; tag=$2-$$
env > "$dir/$tag.env"
if (exec 9<&3) 2>/dev/null; then
  cat <&3 > "$dir/$tag.fd3"; echo open > "$dir/$tag.state"
else
  : > "$dir/$tag.fd3"; echo closed > "$dir/$tag.state"
fi
"#;

/// Hooks and a stdio MCP server (which is how `cairn-cmd` runs) that each
/// run [`PROBE`], recording what they inherited.
struct Probes {
    dir: std::path::PathBuf,
    settings: String,
    servers: McpServers,
}

impl Probes {
    fn new(env: &ContractEnv, name: &str) -> Result<Self, String> {
        let dir = env.dir().join(name);
        std::fs::create_dir_all(&dir).map_err(|error| error.to_string())?;
        let probe = env.dir().join("probe.sh");
        std::fs::write(&probe, PROBE).map_err(|error| error.to_string())?;
        let (probe, out) = (probe.display().to_string(), dir.display().to_string());
        let hook = |event: &str| json!([{"hooks": [{"type": "command", "command": format!("/bin/sh {probe} {out} hook-{event}")}]}]);
        let settings = json!({"hooks": {
            "SessionStart": hook("SessionStart"),
            "UserPromptSubmit": hook("UserPromptSubmit"),
            "Stop": hook("Stop"),
        }})
        .to_string();
        let servers = McpServers::new(vec![McpServer {
            name: "probe".to_string(),
            transport: McpTransport::Stdio {
                command: "/bin/sh".to_string(),
                args: vec![probe, out, "mcp".to_string()],
                env: Default::default(),
                inherit_env: Vec::new(),
            },
            required_tools: Vec::new(),
        }])
        .map_err(|error| error.to_string())?;
        Ok(Self {
            dir,
            settings,
            servers,
        })
    }

    fn states(&self, prefix: &str) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(&self.dir)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|entry| entry.path())
                    .filter(|path| {
                        let name = path.file_name().unwrap().to_string_lossy();
                        name.starts_with(prefix) && name.ends_with(".state")
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Every hook and the MCP server ran, and none of them inherited
    /// `secret` or a variable named `credential_env`, or read `secret` from
    /// descriptor 3. With `descriptor_closed`, none holds descriptor 3.
    fn none_saw(
        &self,
        c: Check,
        secret: &str,
        credential_env: &str,
        descriptor_closed: bool,
    ) -> CaseResult {
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.states("hook-").is_empty() || self.states("mcp-").is_empty() {
            c.that(Instant::now() < deadline, || {
                "the hooks and the MCP server never ran".to_string()
            })?;
            std::thread::sleep(Duration::from_millis(100));
        }
        for state in self.states("hook-").into_iter().chain(self.states("mcp-")) {
            let who = state.file_stem().unwrap().to_string_lossy().into_owned();
            let inherited =
                std::fs::read_to_string(state.with_extension("env")).unwrap_or_default();
            c.that(!inherited.contains(secret), || {
                format!("{who}: the credential is in its environment")
            })?;
            c.that(
                !inherited
                    .lines()
                    .any(|line| line.starts_with(&format!("{credential_env}="))),
                || format!("{who}: its environment names {credential_env}"),
            )?;
            let fd3 = std::fs::read(state.with_extension("fd3")).unwrap_or_default();
            c.that(
                !fd3.windows(secret.len())
                    .any(|window| window == secret.as_bytes()),
                || format!("{who}: read the credential from descriptor 3"),
            )?;
            if descriptor_closed {
                let descriptor = std::fs::read_to_string(&state).unwrap_or_default();
                c.that(descriptor.trim() == "closed", || {
                    format!("{who}: holds descriptor 3")
                })?;
            }
        }
        Ok(())
    }
}

/// The file under `dir` that holds `needle`, if any. Symlinks are not followed.
fn file_holding(dir: &Path, needle: &[u8]) -> Option<std::path::PathBuf> {
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if kind.is_dir() {
            if let Some(found) = file_holding(&path, needle) {
                return Some(found);
            }
        } else if kind.is_file()
            && std::fs::read(&path)
                .is_ok_and(|bytes| bytes.windows(needle.len()).any(|window| window == needle))
        {
            return Some(path);
        }
    }
    None
}

/// Neither the key nor the suffix the CLI keeps of an approved key is in any
/// file the case's CLIs could write.
fn no_file_holds_the_key(c: Check, env: &ContractEnv) -> CaseResult {
    let suffix = &API_KEY[API_KEY.len() - 20..];
    for needle in [API_KEY, suffix] {
        let found = file_holding(env.dir(), needle.as_bytes());
        c.that(found.is_none(), || {
            format!("{found:?} holds the API key or its suffix")
        })?;
    }
    Ok(())
}

pub fn credential_by_descriptor_and_operator_home_never_loads(env: &ContractEnv) -> CaseResult {
    let c = Check("credential_by_descriptor_and_operator_home_never_loads");

    // A managed account's token: nothing the CLI starts sees it.
    const TOKEN: &str = "at-DESCRIPTOR";
    let probes = c.setup(Probes::new(env, "probes-oauth"), "writing the probes")?;
    let mut session = launch(
        c,
        env,
        Launch::new()
            .credential(Credential::OAuth(TOKEN))
            .mcp(Mcp::Servers(probes.servers.clone()))
            .hook_settings(probes.settings.clone()),
    )?;
    session.ok_turn(c, "turn one")?;
    let bearer = env
        .fake
        .inference()
        .last()
        .map(|served| served.bearer.clone());
    c.that(bearer.as_deref() == Some(TOKEN), || {
        format!("the turn carried {bearer:?}")
    })?;
    holds_no_secret(
        c,
        &session,
        env,
        &[TOKEN],
        (OAUTH_TOKEN_ENV, OAUTH_TOKEN_FD_ENV),
    )?;
    probes.none_saw(c, TOKEN, OAUTH_TOKEN_ENV, false)?;
    drop(session);

    // An API key, interactively over two turns: read once, by descriptor.
    let probes = c.setup(Probes::new(env, "probes-key"), "writing the probes")?;
    let mut session = launch(
        c,
        env,
        Launch::new()
            .credential(Credential::ApiKey(API_KEY))
            .mcp(Mcp::Servers(probes.servers.clone()))
            .hook_settings(probes.settings.clone()),
    )?;
    let before = env.fake.inference().len();
    for turn in ["turn one", "turn two"] {
        session.ok_turn(c, turn)?;
    }
    let keyed: Vec<_> = env.fake.inference().into_iter().skip(before).collect();
    c.that(keyed.len() >= 2, || {
        format!("{} inference requests", keyed.len())
    })?;
    c.that(keyed.iter().all(|served| served.api_key == API_KEY), || {
        "an inference request carried a different API key".to_string()
    })?;
    c.that(keyed.iter().all(|served| served.bearer.is_empty()), || {
        "an API-key request carried a bearer token".to_string()
    })?;
    holds_no_secret(c, &session, env, &[API_KEY], (API_KEY_ENV, API_KEY_FD_ENV))?;
    probes.none_saw(c, API_KEY, API_KEY_ENV, true)?;
    drop(session);

    // And in print mode, as a one-shot completion runs.
    let mut session = launch(
        c,
        env,
        Launch::new()
            .credential(Credential::ApiKey(API_KEY))
            .one_shot("what is the answer?"),
    )?;
    let result = session.result(c)?;
    c.that(
        matches!(
            result.event,
            Some(ClaudeEvent::Result {
                is_error: false,
                ..
            })
        ),
        || format!("the one-shot failed: {}", result.raw),
    )?;
    let key = env
        .fake
        .inference()
        .last()
        .map(|served| served.api_key.clone());
    c.that(key.as_deref() == Some(API_KEY), || {
        "the one-shot carried another key".to_string()
    })?;
    no_token_fetched(c, env)?;
    no_file_holds_the_key(c, env)?;
    drop(session);

    // The operator's `~/.claude` loads into no launch: no hook or statusLine
    // ran and their CLAUDE.md was not sent...
    let markers = markers_in(&env.markers);
    c.that(markers.is_empty(), || {
        format!("operator or project hooks ran: {markers:?}")
    })?;
    c.that(!env.fake.sent_to_the_model(OPERATOR_CANARY), || {
        "the operator's CLAUDE.md was sent to the model".to_string()
    })?;
    // ...while the control, a launch naming that `~/.claude` as its home,
    // shows the fixture is live.
    let mut control = launch(
        c,
        env,
        Launch::new()
            .credential(Credential::ApiKey(API_KEY))
            .config(env.home.join(".claude")),
    )?;
    control.ok_turn(c, "control turn")?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !markers_in(&env.markers)
        .iter()
        .any(|marker| marker == "operator-UserPromptSubmit")
    {
        c.that(Instant::now() < deadline, || {
            format!(
                "the control's operator hooks never ran: {:?}",
                markers_in(&env.markers)
            )
        })?;
        std::thread::sleep(Duration::from_millis(100));
    }
    c.that(env.fake.sent_to_the_model(OPERATOR_CANARY), || {
        "the control never sent the operator's CLAUDE.md".to_string()
    })
}

pub fn no_unhandled_cli_control_requests(env: &ContractEnv) -> CaseResult {
    let c = Check("no_unhandled_cli_control_requests");
    env.fake.queue([Mode::Tool {
        name: "mcp__cairn__read",
        args: "{}",
    }]);
    let mut session = launch(c, env, Launch::new())?;
    session.ok_turn(c, "Read something.")?;
    session.ok_turn(c, "And again.")?;
    session.drain(Duration::from_secs(2));
    let requests: Vec<String> = session
        .frames
        .iter()
        .filter(|frame| frame.kind() == "control_request")
        .map(|frame| frame.raw.to_string())
        .collect();
    c.that(requests.is_empty(), || {
        format!("the CLI sent control requests Cairn never answers: {requests:?}")
    })
}

pub fn update_is_refused_under_launch_env(env: &ContractEnv) -> CaseResult {
    let c = Check("update_is_refused_under_launch_env");
    let pin = CLAUDE.embedded();
    // A release of its own, so nothing an update could do reaches the
    // install the other cases share.
    let managed = c.setup(env.fresh_managed(), "staging a fresh install root")?;
    let cli = c
        .setup(managed.ensure(), "installing the pinned binary")?
        .path;
    let before = c.setup(sha256(&cli), "hashing the installed binary")?;
    let run = c.setup(
        env.run_cli(&cli, &["update"], Duration::from_secs(120)),
        "running `claude update` under the launch environment",
    )?;
    eprintln!(
        "`claude update` exited {:?}\nstdout: {}\nstderr: {}",
        run.status, run.stdout, run.stderr
    );

    // A live session, kept past the window the updater checks in.
    let mut session = launch(c, env, Launch::new())?;
    session.ok_turn(c, "Say hello.")?;
    session.drain(Duration::from_secs(15));

    let egress = env.fake.egress();
    let updates: Vec<&String> = egress
        .iter()
        .filter(|target| UPDATE_HOSTS.iter().any(|host| target.contains(host)))
        .collect();
    c.that(updates.is_empty(), || {
        format!("the CLI reached for an update: {updates:?} (all egress {egress:?})")
    })?;
    let after = c.ok(sha256(&cli), "hashing the binary after the update")?;
    c.that(after == before, || {
        "`claude update` changed the managed binary".to_string()
    })?;
    let printed = c.ok(
        printed_version(&cli),
        "running claude --version after the update",
    )?;
    c.that(
        parse_cli_version(&printed) == Some(pin.version.as_str()),
        || format!("after the update `claude --version` printed {printed:?}"),
    )?;
    let local = env.home.join(".local");
    c.that(!local.exists(), || {
        format!("the CLI wrote under {}", local.display())
    })
}

pub fn no_egress(env: &ContractEnv) -> CaseResult {
    let c = Check("no_egress");
    env.fake.queue([Mode::Tool {
        name: "mcp__cairn__read",
        args: "{}",
    }]);
    let mut session = launch(c, env, Launch::new())?;
    session.ok_turn(c, "Read something.")?;
    // Measured on 2.1.311: without the hardening, a session reaches
    // api.anthropic.com at startup and again 10 to 12 s later.
    session.drain(Duration::from_secs(14));
    let pid = c.some(session.pid(), "the CLI's pid")?;
    let launched = LaunchEnvironment::of(pid);
    for (name, value) in CLAUDE_HARDENING_ENV {
        c.that(launched.has(name, value), || {
            format!("the launch environment does not carry {name}={value}")
        })?;
    }
    let egress = env.fake.egress();
    c.that(egress.is_empty(), || {
        format!("the CLI tried to reach the network beside its model endpoint: {egress:?}")
    })
}

pub fn planted_cwd_config_isolation(env: &ContractEnv, file: &str) -> CaseResult {
    let c = Check("planted_cwd_config_isolation");
    let dot = env.work.join(".claude");
    c.setup(
        std::fs::create_dir_all(&dot),
        "creating planted cwd settings",
    )?;
    let marker = env.markers.join("cwd-executed");
    let command = format!("touch '{}'", marker.display());
    let hooks = [
        "SessionStart",
        "UserPromptSubmit",
        "PreToolUse",
        "PostToolUse",
        "Stop",
    ];
    let settings = json!({
        "hooks": hooks.into_iter().map(|event| (event.to_string(),
            json!([{"matcher":"*", "hooks":[{"type":"command", "command":command}]}])
        )).collect::<serde_json::Map<String, Value>>(),
        "permissions": {"allow": ["Bash(*)"]},
        "enabledMcpjsonServers": ["planted"],
        "enableAllProjectMcpServers": true
    });
    let planted = if file == ".mcp.json" {
        json!({"mcpServers":{"planted":{"type":"stdio","command":"/bin/sh","args":["-c",command]}}})
    } else {
        settings
    };
    c.setup(
        std::fs::write(env.work.join(file), planted.to_string()),
        "planting cwd config",
    )?;
    env.fake.queue([
        Mode::Tool {
            name: "mcp__cairn__read",
            args: r#"{"paths":["file:README.md"]}"#,
        },
        Mode::Text,
    ]);
    let mut session = launch(c, env, Launch::new())?;
    session.ok_turn(c, "Read the README.")?;
    let (_, init) = init_of(c, &session)?;
    c.that(init.mcp_servers.iter().all(|s| s.name == "cairn"), || {
        format!("{file}: unexpected MCP servers")
    })?;
    c.that(builtin_tools(&init).is_empty(), || {
        format!("{file}: built-in tools enabled")
    })?;
    let requests = env.fake.inference();
    c.that(!requests.is_empty(), || "no inference request".to_string())?;
    for request in requests {
        let unexpected: Vec<_> = request
            .tools()
            .into_iter()
            .filter(|tool| {
                !tool.starts_with("mcp__cairn__") && !TOOL_EXEMPTIONS.contains(&tool.as_str())
            })
            .collect();
        c.that(unexpected.is_empty(), || {
            format!("{file}: unexpected tools {unexpected:?}")
        })?;
    }
    c.that(
        env.mcp.bodies().iter().any(|b| b["method"] == "tools/call"),
        || "no tool call exercised hook triggers".to_string(),
    )?;
    session.close();
    let markers = markers_in(&env.markers);
    c.that(markers.is_empty(), || {
        format!("{file}: planted process executed: {markers:?}")
    })
}

/// The account profile the CLI keeps under `oauthAccount` in its home's
/// `.claude.json`, each identity field planted with its own canary.
pub const EMAIL_CANARY: &str = "canary-email-5c1e@contract.invalid";
pub const DISPLAY_NAME_CANARY: &str = "CANARY-display-name-93ab";
pub const ORGANIZATION_CANARY: &str = "CANARY-organization-d07f";
const IDENTITY_CANARIES: [(&str, &str); 3] = [
    ("emailAddress", EMAIL_CANARY),
    ("displayName", DISPLAY_NAME_CANARY),
    ("organizationName", ORGANIZATION_CANARY),
];

/// How the CLI introduces the email: a `# userEmail` context block reading
/// "The user's email address is …", in the first user message.
const EMAIL_PARAGRAPH: [&str; 2] = ["# userEmail", "user's email address"];

/// How a case's home holds the account profile.
#[derive(Clone, Copy, Debug)]
pub enum Identity {
    /// Every identity field holds its canary.
    Planted,
    /// Every identity field is the empty string.
    Blanked,
    /// There is no `oauthAccount` at all.
    Absent,
}

fn planted_cli_state(identity: Identity) -> Value {
    let mut root = json!({
        "numStartups": 3,
        "userID": "contract-user-id-0000",
        "hasCompletedOnboarding": true,
    });
    if !matches!(identity, Identity::Absent) {
        let mut profile = json!({
            "accountUuid": "00000000-0000-4000-8000-00000000c0de",
            "organizationUuid": "00000000-0000-4000-8000-0000000000a1",
            "profileFetchedAt": 1_762_000_000_000i64,
        });
        for (field, canary) in IDENTITY_CANARIES {
            profile[field] = json!(match identity {
                Identity::Blanked => "",
                _ => canary,
            });
        }
        root["oauthAccount"] = profile;
    }
    root
}

/// What one turn on a home planted with an account profile came to.
struct IdentityTurn {
    /// Which canaries any request sent the model.
    sent: Vec<&'static str>,
    paragraph: bool,
    egress: Vec<String>,
    /// Requests to the fake's own host that could fetch a profile.
    profile_routes: Vec<String>,
    /// The profile as the case planted it, and as the CLI left it.
    before: Value,
    after: Value,
}

impl IdentityTurn {
    fn profile_unchanged(&self) -> bool {
        self.before.get("oauthAccount") == self.after.get("oauthAccount")
    }
}

/// Plant `identity` in the case's home, let `prepare` do to the home what a
/// launch does, then run one turn and drain long enough for the CLI's
/// delayed startup traffic (see [`no_egress`]).
fn identity_turn(
    c: Check,
    env: &ContractEnv,
    identity: Identity,
    prepare: impl FnOnce(&Path) -> Result<(), String>,
) -> Result<IdentityTurn, CaseFailure> {
    let file = env.config.join(".claude.json");
    let planted = serde_json::to_string_pretty(&planted_cli_state(identity)).unwrap();
    c.setup(std::fs::write(&file, &planted), "planting .claude.json")?;
    c.setup(prepare(&env.config), "preparing the home for launch")?;
    let read = |what| {
        c.setup(
            std::fs::read_to_string(&file)
                .map_err(|e| e.to_string())
                .and_then(|raw| serde_json::from_str::<Value>(&raw).map_err(|e| e.to_string())),
            what,
        )
    };
    let before = read("reading the prepared .claude.json")?;
    env.fake.queue([Mode::Text]);
    let mut session = launch(c, env, Launch::new())?;
    session.ok_turn(c, "Say hello.")?;
    session.drain(Duration::from_secs(14));
    session.close();
    let after = read("reading .claude.json after the turn")?;
    let bodies: Vec<String> = env
        .fake
        .inference()
        .iter()
        .map(|served| served.body.to_string())
        .collect();
    Ok(IdentityTurn {
        sent: IDENTITY_CANARIES
            .iter()
            .filter(|(_, canary)| env.fake.sent_to_the_model(canary))
            .map(|(field, _)| *field)
            .collect(),
        paragraph: bodies
            .iter()
            .any(|body| EMAIL_PARAGRAPH.iter().any(|words| body.contains(words))),
        egress: env.fake.egress(),
        profile_routes: env
            .fake
            .served()
            .iter()
            .filter(|served| !served.is_inference())
            .map(|served| format!("{} {}", served.method, served.target))
            .filter(|route| {
                let route = route.to_ascii_lowercase();
                ["oauth", "profile", "account"]
                    .iter()
                    .any(|word| route.contains(word))
            })
            .collect(),
        before,
        after,
    })
}

/// What the pinned CLI does with the account profile in its home, as measured
/// for cairn/6338 (2.1.311, macOS):
///
/// | profile | turn | sent to the model | email paragraph | profile fetched | profile rewritten |
/// |---|---|---|---|---|---|
/// | planted | completes | `emailAddress` only | yes | no | no |
/// | blanked | completes | nothing | no | no | no (stays blank) |
/// | absent | completes | nothing | no | no | no (not recreated) |
///
/// The CLI rewrites `.claude.json` at every launch with its own bookkeeping
/// (`firstStartTime`, `userID`, `machineID`, migrations, plugin usage), but
/// carries `oauthAccount` through as it found it. So removing the email
/// before launch holds for the whole session. A red here means a CLI bump
/// changed one of those facts: re-measure before trusting
/// `cairn_models::claude::home`.
pub fn account_identity_probe(env: &ContractEnv, identity: Identity) -> CaseResult {
    let c = Check("account_identity_probe");
    let turn = identity_turn(c, env, identity, |_| Ok(()))?;
    let sent = &turn.sent;
    match identity {
        Identity::Planted => {
            c.that(sent == &["emailAddress"] && turn.paragraph, || {
                format!(
                    "planted: the CLI sent {sent:?} (paragraph: {}), not the email paragraph alone",
                    turn.paragraph
                )
            })?;
        }
        Identity::Blanked | Identity::Absent => {
            c.that(sent.is_empty() && !turn.paragraph, || {
                format!(
                    "{identity:?}: the CLI sent {sent:?} (paragraph: {})",
                    turn.paragraph
                )
            })?;
        }
    }
    c.that(
        turn.egress.is_empty() && turn.profile_routes.is_empty(),
        || {
            format!(
                "{identity:?}: the CLI reached for a profile: egress {:?}, routes {:?}",
                turn.egress, turn.profile_routes
            )
        },
    )?;
    c.that(turn.profile_unchanged(), || {
        format!(
            "{identity:?}: the CLI rewrote oauthAccount from {} to {}",
            turn.before["oauthAccount"], turn.after["oauthAccount"]
        )
    })
}

/// The gate on `cairn_models::claude::home`: a home planted with the whole
/// account profile, prepared the way every managed launch prepares it, sends
/// the model none of its identity, and the CLI does not write the email back.
/// `prepare` is false only for the gate negative, which proves the case sees
/// the paragraph when it is there.
pub fn account_email_never_reaches_the_model(env: &ContractEnv, prepare: bool) -> CaseResult {
    let c = Check("account_email_never_reaches_the_model");
    let turn = identity_turn(c, env, Identity::Planted, |home| {
        if prepare {
            cairn_models::claude::home::strip_account_identity(home)?;
        }
        Ok(())
    })?;
    let sent = &turn.sent;
    c.that(sent.is_empty() && !turn.paragraph, || {
        format!(
            "the model was sent the account's {sent:?} (email paragraph: {})",
            turn.paragraph
        )
    })?;
    let profile = &turn.after["oauthAccount"];
    c.that(profile.get("emailAddress").is_none(), || {
        format!("the CLI wrote the email back into its home: {profile}")
    })
}
