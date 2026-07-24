//! Run-item resolution: classify each item into a shell command, inline code, a
//! skill-script spec, or a proxied MCP `tools/call`.

use super::types::{McpCallSpec, RunItem, RunSpec};
use crate::config::mcp_servers::McpServerConfig;
use crate::mcp::handlers::{skills_resources, unwrap_shell_launcher, RunContext};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;

/// Resolve a single run item into a header + executable spec, or a per-item
/// error message that will be reported inline (and counts as a failure).
pub(super) async fn resolve_run_item(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    run_context: Option<&RunContext>,
    item: &RunItem,
) -> (String, Result<RunSpec, String>) {
    // A `repl` key routes inline code into a live REPL session by slug — a
    // distinct spec from a fresh-process code item. Classify it first so the
    // exactly-one-of command/target/code check below doesn't misread it.
    if let Some(slug) = item.repl.as_deref() {
        return resolve_repl_send(item, slug);
    }

    // `interpreter` is only meaningful for inline `code`; a stray one on a
    // command/target item is a mistake worth naming rather than silently ignoring.
    if item.interpreter.is_some() && item.code.is_none() {
        return (
            "<invalid item>".to_string(),
            Err("Run item has `interpreter` but no `code`; `interpreter` is only valid with inline `code`".to_string()),
        );
    }

    // Exactly one of `command` / `target` / `code`.
    let present: Vec<&str> = [
        item.command.as_deref().map(|_| "command"),
        item.target.as_deref().map(|_| "target"),
        item.code.as_deref().map(|_| "code"),
    ]
    .into_iter()
    .flatten()
    .collect();
    match present.as_slice() {
        [] => (
            "<invalid item>".to_string(),
            Err("Run item has none of `command`, `target`, or `code`; provide exactly one".to_string()),
        ),
        [first, second, ..] => (
            "<invalid item>".to_string(),
            Err(format!(
                "Run item has both `{first}` and `{second}`; provide exactly one of `command`, `target`, or `code`"
            )),
        ),
        ["command"] => {
            let command = item.command.as_deref().unwrap_or_default();
            // Unwrap launcher forms (e.g. `bash -lc '...'`) like the old path did.
            let unwrapped = unwrap_shell_launcher(command);
            let command = if command.trim() != unwrapped {
                unwrapped
            } else {
                command.to_string()
            };
            let header = item_header(item, &command);
            (
                header,
                Ok(RunSpec::Shell {
                    command,
                    timeout: item.timeout,
                }),
            )
        }
        ["target"] => {
            let target = item.target.as_deref().unwrap_or_default();
            let header = item_header(item, target);
            // Branch on the target URI family: MCP gateway tool calls are
            // proxied RPC; everything else is a skill-script process exec.
            match cairn_common::uri::parse_uri(target) {
                Some(CairnResource::Mcp { server, resource }) => {
                    let spec = resolve_mcp_call(orch, run_context, server, resource, item).await;
                    (header, spec)
                }
                _ => {
                    let spec = resolve_script_spec(orch, request, target, item).await;
                    (header, spec)
                }
            }
        }
        ["code"] => resolve_code_spec(item),
        _ => unreachable!("present holds only command/target/code"),
    }
}

fn resolve_matlab_spec(
    code: &str,
    matlab_binary: Option<std::path::PathBuf>,
) -> Result<(String, Vec<String>, Option<String>), String> {
    let program = matlab_binary.ok_or_else(|| {
        "MATLAB was not found on the agent PATH or in /Applications/MATLAB_R*.app/bin/matlab. Install MATLAB there or add its bin directory to PATH, then read cairn://skills/matlab for setup guidance."
            .to_string()
    })?;
    Ok((
        program.to_string_lossy().into_owned(),
        vec!["-batch".to_string(), code.to_string()],
        None,
    ))
}

/// Resolve PATH first so explicit user configuration wins. A Finder-launched
/// macOS app often lacks MATLAB on PATH, so fall back to the newest release in
/// the standard application directory.
fn discover_matlab_binary() -> Option<std::path::PathBuf> {
    if let Ok(path) = crate::env::find_binary_on_agent_path("matlab") {
        return Some(path.into());
    }
    discover_macos_matlab_in(std::path::Path::new("/Applications"))
}

#[cfg(target_os = "macos")]
fn discover_macos_matlab_in(applications: &std::path::Path) -> Option<std::path::PathBuf> {
    let mut candidates = std::fs::read_dir(applications)
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            (name.starts_with("MATLAB_R") && name.ends_with(".app"))
                .then(|| entry.path().join("bin/matlab"))
        })
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.pop()
}

#[cfg(not(target_os = "macos"))]
fn discover_macos_matlab_in(_applications: &std::path::Path) -> Option<std::path::PathBuf> {
    None
}

fn item_header(item: &RunItem, fallback: &str) -> String {
    item.description
        .as_deref()
        .map(str::trim)
        .filter(|description| !description.is_empty())
        .unwrap_or(fallback)
        .to_string()
}

/// Resolve a `repl`-keyed item into a header + `ReplSend` spec. `repl` requires
/// inline `code` + an `interpreter` matching a supported REPL language, and
/// rejects `command`/`target`/`payload`. The interpreter is validated to parse
/// here; the send path fails closed if it does not match the target session's
/// actual language.
fn resolve_repl_send(item: &RunItem, slug: &str) -> (String, Result<RunSpec, String>) {
    let code = item.code.as_deref().unwrap_or_default();
    let header = item_header(item, &first_line_header(code));
    let slug = slug.trim();
    if slug.is_empty() {
        return (
            header,
            Err("Run item `repl` must be a non-empty REPL slug".to_string()),
        );
    }
    if item.command.is_some() || item.target.is_some() {
        return (
            header,
            Err("Run item has `repl` with `command` or `target`; a REPL send takes inline `code` only".to_string()),
        );
    }
    if item.code.is_none() {
        return (
            header,
            Err(
                "Run item has `repl` but no `code`; a REPL send evaluates inline `code`"
                    .to_string(),
            ),
        );
    }
    if item.payload.is_some() {
        return (
            header,
            Err("Run item has `repl` and `payload`; a REPL send takes no payload".to_string()),
        );
    }
    let interpreter = match item.interpreter.as_deref() {
        Some(i) => i,
        None => return (
            header,
            Err(
                "Run item has `repl` but no `interpreter`; set it to the REPL's language (python | typescript)"
                    .to_string(),
            ),
        ),
    };
    match crate::mcp::handlers::repl::ReplLang::parse(interpreter) {
        Some(lang) => (
            header,
            Ok(RunSpec::ReplSend {
                slug: slug.to_string(),
                code: code.to_string(),
                timeout: item.timeout,
                lang,
            }),
        ),
        None => (
            header,
            Err(format!(
                "Run item `repl` interpreter '{interpreter}' is not a supported REPL language; use python (py) | typescript (ts)"
            )),
        ),
    }
}

/// Resolve an inline-code run item into a header + `Script` spec.
///
/// Inline code reuses `RunSpec::Script` so `process.rs` runs it through the exact
/// same spawn/env/fence/timeout path as a skill script — the only new work is
/// mapping the language-named interpreter to an executable spec and deriving a
/// header. TypeScript/JavaScript pass the code as a single argv argument
/// (`bun -e <code>`); python delegates to [`resolve_python_spec`], which routes
/// through `uv run -` (code on stdin) when uv resolves and falls back to
/// `python3 -c <code>` otherwise. No shell (so no quoting), no temp file (no
/// lifecycle). Because `execute_process` injects the callback env, inline
/// TypeScript gets zero-config `@cairn/sdk` from the worktree `node_modules`.
fn resolve_code_spec(item: &RunItem) -> (String, Result<RunSpec, String>) {
    let code = item.code.as_deref().unwrap_or_default();
    let header = item_header(item, &first_line_header(code));

    // `payload` (args / args_json) is meaningless for inline code — reject it so
    // the item kinds stay cleanly separated rather than silently dropping it.
    if item.payload.is_some() {
        return (
            header,
            Err("Run item has both `code` and `payload`; inline code takes no payload".to_string()),
        );
    }

    let interpreter = match item.interpreter.as_deref() {
        Some(i) => i,
        None => {
            return (
                header,
                Err("Run item has `code` but no `interpreter`; set `interpreter` to one of: typescript (ts), javascript (js), python (py), matlab".to_string()),
            )
        }
    };

    let (program, args, stdin) = match interpreter.trim().to_ascii_lowercase().as_str() {
        // bun runs TypeScript and JavaScript identically; the ts/js split only
        // matters to the presentation-layer syntax highlighter.
        "typescript" | "ts" | "javascript" | "js" => (
            "bun".to_string(),
            vec!["-e".to_string(), code.to_string()],
            None,
        ),
        // python routes through `uv run -` when uv resolves, else `python3 -c`.
        // The env probe is isolated from the pure ladder so the decision logic
        // stays hermetically testable.
        "python" | "py" => resolve_python_spec(code, uv_on_agent_path()),
        "matlab" => match resolve_matlab_spec(code, discover_matlab_binary()) {
            Ok(spec) => spec,
            Err(error) => return (header, Err(error)),
        },
        other => {
            return (
                header,
                Err(format!(
                    "Run item has an unknown `interpreter` '{other}'; accepted values: typescript (ts), javascript (js), python (py), matlab"
                )),
            )
        }
    };

    (
        header,
        Ok(RunSpec::Script {
            program,
            args,
            timeout: item.timeout,
            stdin,
        }),
    )
}

/// Choose the `(program, args, stdin)` for an inline python item. Pure: the env
/// probe is threaded in as `uv_available` so the four-behavior ladder stays
/// hermetic and env-independent.
///
/// When uv resolves, delegate to `uv run -` and hand the code to it on **stdin**.
/// uv itself does the project detection and PEP 723 inline-metadata parsing, so a
/// `# /// script` dependency block, a surrounding `pyproject.toml`'s deps, or
/// plain stdlib code all run correctly with no mode branching here. Stdin is
/// required, not merely convenient: `uv run -c` never parses PEP 723 metadata,
/// and feeding stdin keeps `cwd` at the worktree so uv's project detection sees
/// the real project (a temp script in `$TMPDIR` would sit outside the worktree
/// and defeat that).
///
/// When uv is absent, fall back byte-for-byte to today's `python3 -c <code>`.
/// This fallback is the ladder's ONLY silent downgrade and is debug-logged at the
/// [`uv_on_agent_path`] probe; a dependency-resolution failure is NOT a fallback
/// — uv's nonzero exit surfaces its real error as the item's failed output.
fn resolve_python_spec(code: &str, uv_available: bool) -> (String, Vec<String>, Option<String>) {
    if uv_available {
        (
            "uv".to_string(),
            vec!["run".to_string(), "-".to_string()],
            Some(code.to_string()),
        )
    } else {
        (
            "python3".to_string(),
            vec!["-c".to_string(), code.to_string()],
            None,
        )
    }
}

/// Probe whether `uv` resolves on the agent shell PATH (which prepends the
/// host-owned `<cairn_home>/bin` shim dir, where a sidecar install may place
/// `uv` — a location `get_user_path` does not include). Logs at debug when the
/// probe misses so the `python3 -c` fallback, the ladder's one silent downgrade,
/// is visible in logs.
fn uv_on_agent_path() -> bool {
    match crate::env::find_binary_on_agent_path("uv") {
        Ok(_) => true,
        Err(_) => {
            log::debug!("uv not found on agent PATH; inline python falls back to `python3 -c`");
            false
        }
    }
}

/// Header for an inline-code item lacking a `description`: its first non-blank
/// source line, truncated for the composed-output `=== <header> ===` label.
fn first_line_header(code: &str) -> String {
    const MAX: usize = 80;
    let line = code
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("");
    if line.chars().count() > MAX {
        let truncated: String = line.chars().take(MAX).collect();
        format!("{truncated}\u{2026}")
    } else {
        line.to_string()
    }
}

/// Resolve a `cairn://mcp/<server>/<tool>` target into an `McpCall` spec.
async fn resolve_mcp_call(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    server: Option<String>,
    resource: Option<String>,
    item: &RunItem,
) -> Result<RunSpec, String> {
    let server = server.ok_or_else(|| {
        "MCP run target must name a server and tool: cairn://mcp/<server>/<tool>".to_string()
    })?;
    let tool = resource
        .ok_or_else(|| format!("MCP run target must name a tool: cairn://mcp/{server}/<tool>"))?;

    let (config, credential_key) = resolve_mcp_server_config(orch, run_context, &server).await?;

    // Named-argument object passed through to the server's tools/call. Default
    // to an empty object; reject non-object shapes with a clear message rather
    // than duplicating the server's own inputSchema validation.
    let args = match item.payload.as_ref().and_then(|p| p.args_json.clone()) {
        None | Some(serde_json::Value::Null) => serde_json::json!({}),
        Some(v @ serde_json::Value::Object(_)) => v,
        Some(_) => {
            return Err(format!(
                "MCP tool args (payload.args_json) for '{server}/{tool}' must be a JSON object of named arguments"
            ))
        }
    };

    Ok(RunSpec::McpCall(Box::new(McpCallSpec {
        credential_key,
        tool,
        args,
        config,
        timeout: item.timeout,
    })))
}

/// Resolve the env-expanded config for a named MCP server from workspace +
/// project settings (project overlays workspace).
async fn resolve_mcp_server_config(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    server: &str,
) -> Result<(McpServerConfig, String), String> {
    // Resolve the run's project path for the project-level overlay; fall back to
    // workspace-only servers when there is no active run context.
    let project_path = match run_context {
        Some(ctx) => crate::config::get_project_path(&orch.db.local, &ctx.project_id)
            .await
            .ok(),
        None => None,
    };

    let project_servers = project_path
        .as_deref()
        .map(crate::config::mcp_servers::load_project_mcp_servers)
        .unwrap_or_default();
    let servers =
        crate::config::mcp_servers::resolve_mcp_servers(&orch.config_dir, project_path.as_deref());

    match servers.get(server) {
        Some(cfg) => {
            let project_scope = project_path
                .as_deref()
                .filter(|_| project_servers.contains_key(server));
            let credential_key = crate::config::secrets::credential_key(server, project_scope);
            Ok((cfg.expanded(&credential_key), credential_key))
        }
        None => {
            let mut names: Vec<&str> = servers.keys().map(|s| s.as_str()).collect();
            names.sort_unstable();
            let configured = if names.is_empty() {
                "(none configured)".to_string()
            } else {
                names.join(", ")
            };
            Err(format!(
                "Unknown MCP server '{server}'. Configured servers: {configured}. \
                 Add it under `mcpServers` in ~/.cairn/settings.yaml or the project's .cairn/config.yaml."
            ))
        }
    }
}

/// Resolve a `cairn://skills/<id>/scripts/<name>` target to a Script spec.
async fn resolve_script_spec(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    target: &str,
    item: &RunItem,
) -> Result<RunSpec, String> {
    let resource = cairn_common::uri::parse_uri(target)
        .ok_or_else(|| format!("Invalid run target URI: {target}"))?;
    let script_path = skills_resources::resolve_skill_script_path(orch, request, &resource).await?;
    let (program, mut args) = resolve_interpreter(&script_path)?;
    args.push(script_path.to_string_lossy().to_string());
    if let Some(payload) = &item.payload {
        args.extend(payload.args.iter().cloned());
    }
    Ok(RunSpec::Script {
        program,
        args,
        timeout: item.timeout,
        stdin: None,
    })
}

/// Determine the interpreter for a script: shebang first, then extension.
/// Returns (program, prefix_args) where the script path is appended by caller.
fn resolve_interpreter(script_path: &std::path::Path) -> Result<(String, Vec<String>), String> {
    if let Ok(content) = std::fs::read_to_string(script_path) {
        if let Some(first) = content.lines().next() {
            if let Some(rest) = first.strip_prefix("#!") {
                let toks: Vec<&str> = rest.split_whitespace().collect();
                if let Some((&head, tail)) = toks.split_first() {
                    // `/usr/bin/env python3` -> program is the next token.
                    if head.ends_with("env") {
                        if let Some((&prog, env_tail)) = tail.split_first() {
                            return Ok((
                                prog.to_string(),
                                env_tail.iter().map(|s| s.to_string()).collect(),
                            ));
                        }
                    } else {
                        return Ok((
                            head.to_string(),
                            tail.iter().map(|s| s.to_string()).collect(),
                        ));
                    }
                }
            }
        }
    }

    let ext = script_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let program = match ext {
        "sh" => "bash",
        "py" => "python3",
        "js" => "node",
        "ts" => "bun",
        "rb" => "ruby",
        _ => {
            return Err(format!(
            "Cannot determine interpreter for script '{}': no shebang and unrecognized extension",
            script_path.display()
        ))
        }
    };
    Ok((program.to_string(), Vec::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_run_command_unwrap_uses_semantic_inner_command() {
        assert_eq!(
            unwrap_shell_launcher(r#"/bin/zsh -lc "sed -n '120,520p' src/app.tsx""#),
            "sed -n '120,520p' src/app.tsx"
        );
        assert_eq!(
            unwrap_shell_launcher("bash -lc 'git status --short'"),
            "git status --short"
        );
    }
    #[test]
    fn resolve_interpreter_prefers_env_shebang() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("s");
        std::fs::write(&p, "#!/usr/bin/env python3\nprint(1)\n").unwrap();
        let (prog, args) = resolve_interpreter(&p).unwrap();
        assert_eq!(prog, "python3");
        assert!(args.is_empty());
    }

    #[test]
    fn resolve_interpreter_uses_absolute_shebang_with_args() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("s");
        std::fs::write(&p, "#!/bin/bash -e\necho hi\n").unwrap();
        let (prog, args) = resolve_interpreter(&p).unwrap();
        assert_eq!(prog, "/bin/bash");
        assert_eq!(args, vec!["-e".to_string()]);
    }

    #[test]
    fn resolve_interpreter_extension_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("script.py");
        std::fs::write(&p, "print(1)\n").unwrap();
        let (prog, _args) = resolve_interpreter(&p).unwrap();
        assert_eq!(prog, "python3");
    }

    #[test]
    fn resolve_interpreter_unknown_extension_errors() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("script.xyz");
        std::fs::write(&p, "data\n").unwrap();
        assert!(resolve_interpreter(&p).is_err());
    }

    fn code_item(code: &str, interpreter: Option<&str>) -> RunItem {
        RunItem {
            command: None,
            description: None,
            timeout: None,
            target: None,
            payload: None,
            code: Some(code.to_string()),
            interpreter: interpreter.map(str::to_string),
            background: None,
            repl: None,
            wait_for: None,
        }
    }

    #[test]
    fn run_item_header_prefers_a_nonempty_description() {
        let mut item = code_item("echo hidden", Some("ts"));
        item.description = Some("Measure runner response".to_string());
        assert_eq!(item_header(&item, "echo hidden"), "Measure runner response");

        item.description = Some("  ".to_string());
        assert_eq!(item_header(&item, "echo visible"), "echo visible");
    }

    fn script(spec: Result<RunSpec, String>) -> (String, Vec<String>) {
        match spec {
            Ok(RunSpec::Script { program, args, .. }) => (program, args),
            _ => panic!("expected a Script spec"),
        }
    }

    // `RunSpec` is intentionally not `Debug`, so extract the error by match
    // rather than `unwrap_err` (which needs `T: Debug`).
    fn err(spec: Result<RunSpec, String>) -> String {
        match spec {
            Err(e) => e,
            Ok(_) => panic!("expected an error spec"),
        }
    }

    #[test]
    fn resolve_code_spec_maps_ts_and_js_aliases_to_bun_eval() {
        for interp in ["typescript", "ts", "javascript", "js"] {
            let (_h, spec) = resolve_code_spec(&code_item("console.log(1)", Some(interp)));
            let (program, args) = script(spec);
            assert_eq!(program, "bun", "interpreter {interp}");
            assert_eq!(
                args,
                vec!["-e".to_string(), "console.log(1)".to_string()],
                "interpreter {interp}"
            );
        }
    }

    // The python ladder is pinned at the pure decision point (`resolve_python_spec`)
    // rather than through `resolve_code_spec`, which probes the environment for uv.
    // This keeps the four-behavior contract hermetic and env-independent.
    #[test]
    fn resolve_python_spec_falls_back_to_python3_c_without_uv() {
        let (program, args, stdin) = resolve_python_spec("print(1)", false);
        assert_eq!(program, "python3");
        assert_eq!(args, vec!["-c".to_string(), "print(1)".to_string()]);
        assert_eq!(stdin, None, "the python3 fallback feeds no stdin");
    }

    #[test]
    fn resolve_python_spec_uses_uv_run_stdin_when_available() {
        let (program, args, stdin) = resolve_python_spec("print(1)", true);
        assert_eq!(program, "uv");
        assert_eq!(
            args,
            vec!["run".to_string(), "-".to_string()],
            "uv reads the script from stdin, not from an argv flag"
        );
        assert_eq!(
            stdin.as_deref(),
            Some("print(1)"),
            "the code must be delivered on stdin so uv parses PEP 723 metadata"
        );
    }

    #[test]
    fn resolve_matlab_spec_uses_direct_batch_argv() {
        let (program, args, stdin) = resolve_matlab_spec(
            "disp('hello world')",
            Some(std::path::PathBuf::from("/opt/matlab/bin/matlab")),
        )
        .unwrap();
        assert_eq!(program, "/opt/matlab/bin/matlab");
        assert_eq!(args, vec!["-batch", "disp('hello world')"]);
        assert_eq!(stdin, None);
    }

    #[test]
    fn resolve_matlab_spec_missing_binary_is_actionable() {
        let error = resolve_matlab_spec("disp(1)", None).unwrap_err();
        assert!(error.contains("/Applications/MATLAB_R*.app/bin/matlab"));
        assert!(error.contains("PATH"));
        assert!(error.contains("cairn://skills/matlab"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn matlab_application_fallback_picks_newest_release() {
        let dir = tempfile::tempdir().unwrap();
        for release in ["MATLAB_R2024b.app", "MATLAB_R2025a.app"] {
            let bin = dir.path().join(release).join("bin");
            std::fs::create_dir_all(&bin).unwrap();
            std::fs::write(bin.join("matlab"), "").unwrap();
        }
        assert_eq!(
            discover_macos_matlab_in(dir.path()).unwrap(),
            dir.path().join("MATLAB_R2025a.app/bin/matlab")
        );
    }

    #[test]
    fn resolve_code_spec_missing_interpreter_errors() {
        let (_h, spec) = resolve_code_spec(&code_item("print(1)", None));
        let err = err(spec);
        assert!(err.contains("interpreter"), "got: {err}");
    }

    #[test]
    fn resolve_code_spec_unknown_interpreter_names_accepted_set() {
        let (_h, spec) = resolve_code_spec(&code_item("puts 1", Some("ruby")));
        let err = err(spec);
        assert!(
            err.contains("typescript") && err.contains("python") && err.contains("matlab"),
            "the error must name the accepted set: {err}"
        );
    }

    #[test]
    fn resolve_code_spec_rejects_payload() {
        let mut item = code_item("print(1)", Some("python"));
        item.payload = Some(super::super::types::RunItemPayload::default());
        let (_h, spec) = resolve_code_spec(&item);
        let err = err(spec);
        assert!(err.contains("payload"), "got: {err}");
    }

    #[test]
    fn resolve_code_spec_header_uses_first_nonblank_line() {
        let (header, _spec) = resolve_code_spec(&code_item(
            "\n\n  const x = 1;\n  console.log(x);\n",
            Some("ts"),
        ));
        assert_eq!(header, "const x = 1;");
    }

    #[test]
    fn resolve_code_spec_header_prefers_description() {
        let mut item = code_item("console.log(1)", Some("ts"));
        item.description = Some("greet the world".to_string());
        let (header, _spec) = resolve_code_spec(&item);
        assert_eq!(header, "greet the world");
    }

    #[test]
    fn first_line_header_truncates_long_lines() {
        let long = "x".repeat(200);
        let header = first_line_header(&long);
        assert!(
            header.chars().count() <= 81,
            "got {} chars",
            header.chars().count()
        );
        assert!(header.ends_with('\u{2026}'));
    }
}
