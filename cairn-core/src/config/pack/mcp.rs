//! A pack's MCP server definitions, and the pack layer they form.
//!
//! MCP servers are config keys in a YAML mapping, not discovered files, so the
//! per-file git ownership oracle does not apply to them. Instead a pack's
//! servers become a *layer beneath* the user's `settings.yaml` in the existing
//! resolution chain: pack files → workspace settings → project config. A user
//! edit (disabling a server, pinning an absolute command path) writes one entry
//! into `settings.yaml` that shadows the pack default — the same pack-owned /
//! user-forked model the file-backed resources use, with no new concept.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use crate::config::mcp_servers::McpServerConfig;

/// The `mcpServers` envelope shared by Cairn's `mcp.yaml` and Claude Code's
/// `.mcp.json`. `serde_yaml` is a superset of JSON, so one parse reads both.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct McpFile {
    #[serde(default)]
    mcp_servers: HashMap<String, McpServerConfig>,
}

/// Parse a pack's MCP definition file.
///
/// A `secrets:` declaration is dropped here, because it is the one field in
/// `McpServerConfig` whose effect is not confined to the server that declares
/// it. Declaring a `${VAR}` a credential registers whatever it resolves to as a
/// process-global scrub target for the life of the process, and a registered
/// value does not merely get redacted — every model-authored write mentioning it
/// is *refused*, behind a deliberately generic refusal the agent cannot diagnose
/// and will retry into.
///
/// A pack shipping `args: ["${HOME}/state"]` with `secrets: [HOME]` would
/// therefore register the user's home directory and lock the agent out of
/// writing any path under it, with no way back short of editing the pack file
/// and restarting. `PATH`, `PWD`, and `USER` are equally available. That is
/// exactly the failure the declared-secret design exists to prevent (see
/// `security::broker`), arriving through content the user did not author.
///
/// Pack content is already treated as untrusted here — a pack server stays inert
/// until its references resolve — so the declaration is refused on the same
/// terms rather than gated. A user who wants a pack server's environment
/// variable treated as a credential forks the entry into their own
/// `settings.yaml`, which is where the field is honored.
pub fn parse_pack_mcp_file(path: &Path) -> Result<HashMap<String, McpServerConfig>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read pack MCP file {path:?}: {e}"))?;
    let parsed: McpFile = serde_yaml::from_str(&text)
        .map_err(|e| format!("Failed to parse pack MCP file {path:?}: {e}"))?;
    let mut servers = parsed.mcp_servers;
    for (name, config) in servers.iter_mut() {
        // Destructured exhaustively, not field-accessed, on purpose — the same
        // technique `fingerprint_mcp_config` uses, for a related reason.
        //
        // Every other field's effect is confined to the server that declares
        // it: a bad `command` fails that server's spawn, a bad `url` fails its
        // connect, `enabled: false` hides it. `secrets` is the only one whose
        // effect reaches the rest of the system, which is why it alone is
        // refused from an untrusted layer.
        //
        // A field added to `McpServerConfig` later must be a compile error HERE
        // rather than silently inheriting "packs may set this". If you are
        // reading this because the destructure stopped compiling: decide whether
        // the new field's effect stays inside this server, and either bind it to
        // `_` with a note saying so or refuse it alongside `secrets`.
        let McpServerConfig {
            transport: _,
            command: _,
            args: _,
            env: _,
            cwd: _,
            url: _,
            headers: _,
            enabled: _,
            oauth: _,
            secrets,
            agent_plugin_runtime: _,
        } = config;
        if secrets.is_empty() {
            continue;
        }
        log::warn!(
            "Ignoring `secrets: {secrets:?}` on MCP server `{name}` from pack file {path:?}: a \
             declared-secret list is honored only from user-authored settings. Fork the \
             server into settings.yaml to declare it."
        );
        secrets.clear();
    }
    Ok(servers)
}

/// One pack-layer server: the definition plus the pack that supplied it.
#[derive(Debug, Clone, PartialEq)]
pub struct PackMcpServer {
    pub pack_id: String,
    pub config: McpServerConfig,
}

/// The MCP layer contributed by every installed pack, keyed by server name.
///
/// Two packs declaring the same server name is a collision the install path
/// refuses, so a later pack winning here is a safety net rather than a policy.
pub fn load_pack_mcp_servers(config_dir: &Path) -> BTreeMap<String, PackMcpServer> {
    let mut layer = BTreeMap::new();
    for lock in super::lock::installed_packs(config_dir) {
        let path = super::lock::mcp_path(config_dir, &lock.id);
        if !path.is_file() {
            continue;
        }
        match parse_pack_mcp_file(&path) {
            Ok(servers) => {
                for (name, config) in servers {
                    let mut config = config;
                    // A server the user removed stays out of every layer,
                    // without them having had to uninstall the pack around it.
                    if lock.is_removed(super::PackItemKind::Mcp, &name) {
                        continue;
                    }
                    let plugin_root = super::lock::pack_dir(config_dir, &lock.id).join("source");
                    if plugin_root.join("plugin.json").is_file() {
                        config.agent_plugin_runtime =
                            Some(crate::config::mcp_servers::AgentPluginRuntime {
                                root: plugin_root,
                                data: config_dir.join("plugin-data").join(&lock.id),
                            });
                    }
                    layer.insert(
                        name,
                        PackMcpServer {
                            pack_id: lock.id.clone(),
                            config,
                        },
                    );
                }
            }
            Err(error) => log::warn!("Skipping pack `{}` MCP layer: {error}", lock.id),
        }
    }
    layer
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pack cannot declare a `${VAR}` a credential.
    ///
    /// The rest of the config is honored — packs legitimately ship servers — but
    /// `secrets:` registers a process-global scrub target that refuses every
    /// later model-authored write mentioning the resolved value. Pointed at
    /// `HOME`, `PATH`, or `USER` by content the user never wrote, that is an
    /// undiagnosable agent lockout with no recovery short of editing the pack
    /// file and restarting.
    #[test]
    fn a_pack_cannot_declare_a_variable_to_be_a_credential() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcp.yaml");
        std::fs::write(
            &path,
            "mcpServers:\n  \
             helper:\n    \
               command: helper\n    \
               args: [\"${HOME}/state\"]\n    \
               secrets: [HOME]\n",
        )
        .unwrap();

        let servers = parse_pack_mcp_file(&path).expect("pack mcp file parses");
        let helper = servers.get("helper").expect("the server is still supplied");
        assert!(
            helper.secrets.is_empty(),
            "a pack must not be able to declare a credential"
        );
        // Everything else about the server survives: this refuses one field, it
        // does not reject the pack's server.
        assert_eq!(helper.command.as_deref(), Some("helper"));
        assert_eq!(helper.args, vec!["${HOME}/state".to_string()]);
    }

    #[test]
    fn an_ordinary_pack_server_parses_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcp.yaml");
        std::fs::write(
            &path,
            "mcpServers:\n  linear:\n    type: http\n    url: https://mcp.example.com/mcp\n",
        )
        .unwrap();

        let servers = parse_pack_mcp_file(&path).expect("pack mcp file parses");
        let linear = servers.get("linear").expect("server present");
        assert_eq!(linear.transport, "http");
        assert_eq!(linear.url.as_deref(), Some("https://mcp.example.com/mcp"));
    }
}
