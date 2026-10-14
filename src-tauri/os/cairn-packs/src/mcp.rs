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

use serde::{Deserialize, Serialize};

use crate::server_config::McpServerConfig;

/// The `mcpServers` envelope shared by Cairn's `mcp.yaml` and Claude Code's
/// `.mcp.json`, plus Cairn's own `setup` block. `serde_yaml` is a superset of
/// JSON, so one parse reads both; a `.mcp.json` simply carries no `setup`.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct McpFile {
    #[serde(default)]
    mcp_servers: HashMap<String, McpServerConfig>,
    #[serde(default)]
    setup: BTreeMap<String, McpVarSetup>,
}

/// What kind of value a pack is asking for.
///
/// This decides two things: the field the settings surface offers (a file
/// picker or a masked input), and — through the host credential store — which
/// keychain namespace the supplied value is written to, which is what decides
/// whether it is ever treated as a credential. A `path` value resolves as
/// configuration and is never registered for scrubbing; a `secret` value is
/// registered like any other stored credential. See
/// the host configuration-value store for why that declaration lives
/// in the store rather than being inferred from the value at read time.
///
/// A pack is therefore making a security-relevant claim here, so it is not
/// taken on trust alone: `store_pack_mcp_value` refuses a `path` value that is
/// not an absolute path, which is what keeps a mislabelled variable from
/// carrying a token into the unprotected namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum McpVarKind {
    /// A path to something the user installed, offered with a file picker.
    Path,
    /// A token, key, or password, offered masked. The default: showing a secret
    /// in the clear is the worse way to be wrong.
    #[default]
    Secret,
}

/// How a pack describes one `${VAR}` its servers need, so the settings surface
/// can ask for it in the user's terms.
///
/// Without this, a server gated on `${MATLAB_MCP_SERVER}` can only be explained
/// by naming the variable, which tells a user nothing about what to supply or
/// where to get it. Every field is optional: a pack that declares nothing still
/// works, it is just explained less well.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpVarSetup {
    #[serde(default)]
    pub kind: McpVarKind,
    /// What the value IS, in the user's words: "MATLAB MCP server executable".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// One sentence of additional orientation, when the label needs it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Where to obtain the thing. Rendered as a link, so it is restricted to
    /// `http`/`https` at parse time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub help_url: Option<String>,
    /// The link's text: "Get it from MathWorks".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub help_label: Option<String>,
}

/// One `${VAR}` a server still needs, paired with how its pack described it.
///
/// The shape every surface renders: the readiness verdict names variables, and
/// this is that name plus the copy that makes it actionable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpVarRequirement {
    pub var: String,
    #[serde(flatten)]
    pub setup: McpVarSetup,
}

/// A parsed pack MCP file: its servers and the setup copy for their variables.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PackMcpFile {
    pub servers: HashMap<String, McpServerConfig>,
    pub setup: BTreeMap<String, McpVarSetup>,
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
pub fn parse_pack_mcp_file(path: &Path) -> Result<PackMcpFile, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read pack MCP file {path:?}: {e}"))?;
    let parsed: McpFile = serde_yaml::from_str(&text)
        .map_err(|e| format!("Failed to parse pack MCP file {path:?}: {e}"))?;
    let mut servers = parsed.mcp_servers;
    let mut setup = parsed.setup;
    // A help link is the one part of the setup block that is a capability
    // rather than prose: the user clicks it and their browser opens it. Pack
    // content is untrusted here, so anything that is not an ordinary web
    // address is dropped rather than handed to the system opener.
    for (var, entry) in setup.iter_mut() {
        let Some(url) = entry.help_url.as_deref() else {
            continue;
        };
        if !url.starts_with("https://") && !url.starts_with("http://") {
            log::warn!(
                "Ignoring `helpUrl` for `{var}` in pack MCP file {path:?}: only http and https \
                 links are offered."
            );
            entry.help_url = None;
        }
    }
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
    Ok(PackMcpFile { servers, setup })
}

/// One pack-layer server: the definition, the pack that supplied it, and the
/// setup copy that pack declared for the `${VAR}`s the definition references.
#[derive(Debug, Clone, PartialEq)]
pub struct PackMcpServer {
    pub pack_id: String,
    pub config: McpServerConfig,
    /// The pack's whole `setup` block, keyed by variable name. Carried per
    /// server rather than looked up later, so a surface holding an entry can
    /// explain what it needs without reaching back into the pack layer.
    pub setup: BTreeMap<String, McpVarSetup>,
}

impl PackMcpServer {
    /// How to ask for each of `vars`, in the order given. A variable the pack
    /// said nothing about still yields a requirement — the surface can name it
    /// and take a value, it just cannot say more than that.
    pub fn requirements(&self, vars: &[String]) -> Vec<McpVarRequirement> {
        vars.iter()
            .map(|var| McpVarRequirement {
                var: var.clone(),
                setup: self.setup.get(var).cloned().unwrap_or_default(),
            })
            .collect()
    }
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
            Ok(file) => {
                for (name, config) in file.servers {
                    let mut config = config;
                    // A server the user removed stays out of every layer,
                    // without them having had to uninstall the pack around it.
                    if lock.is_removed(super::PackItemKind::Mcp, &name) {
                        continue;
                    }
                    let plugin_root = super::lock::pack_dir(config_dir, &lock.id).join("source");
                    if plugin_root.join("plugin.json").is_file() {
                        config.agent_plugin_runtime =
                            Some(crate::server_config::AgentPluginRuntime {
                                root: plugin_root,
                                data: config_dir.join("plugin-data").join(&lock.id),
                            });
                    }
                    layer.insert(
                        name,
                        PackMcpServer {
                            pack_id: lock.id.clone(),
                            config,
                            setup: file.setup.clone(),
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

        let servers = parse_pack_mcp_file(&path)
            .expect("pack mcp file parses")
            .servers;
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

        let servers = parse_pack_mcp_file(&path)
            .expect("pack mcp file parses")
            .servers;
        let linear = servers.get("linear").expect("server present");
        assert_eq!(linear.transport, "http");
        assert_eq!(linear.url.as_deref(), Some("https://mcp.example.com/mcp"));
    }

    /// The setup block is what turns "set a value for ${VAR}" into a sentence a
    /// user can act on, so it has to survive the parse intact.
    #[test]
    fn a_setup_block_describes_the_variables_a_server_needs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcp.yaml");
        std::fs::write(
            &path,
            "mcpServers:\n  \
               matlab:\n    \
                 command: ${MATLAB_MCP_SERVER}\n\
             setup:\n  \
               MATLAB_MCP_SERVER:\n    \
                 kind: path\n    \
                 label: MATLAB MCP server executable\n    \
                 helpUrl: https://www.mathworks.com/help\n    \
                 helpLabel: Get it from MathWorks\n",
        )
        .unwrap();

        let file = parse_pack_mcp_file(&path).expect("pack mcp file parses");
        let entry = &file.setup["MATLAB_MCP_SERVER"];
        assert_eq!(entry.kind, McpVarKind::Path);
        assert_eq!(entry.label.as_deref(), Some("MATLAB MCP server executable"));
        assert_eq!(entry.help_label.as_deref(), Some("Get it from MathWorks"));

        // A variable nobody described still yields a requirement, so the
        // surface can take a value for it either way.
        let server = PackMcpServer {
            pack_id: "matlab".into(),
            config: file.servers["matlab"].clone(),
            setup: file.setup.clone(),
        };
        let required =
            server.requirements(&["MATLAB_MCP_SERVER".to_string(), "UNDESCRIBED".to_string()]);
        assert_eq!(required[0].setup.kind, McpVarKind::Path);
        assert_eq!(required[1].var, "UNDESCRIBED");
        assert_eq!(required[1].setup.kind, McpVarKind::Secret);
        assert!(required[1].setup.label.is_none());
    }

    /// A help link is opened in the user's browser on a click, so a pack cannot
    /// point it anywhere but the web.
    #[test]
    fn a_non_web_help_link_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcp.yaml");
        std::fs::write(
            &path,
            "mcpServers:\n  s:\n    command: ${V}\n\
             setup:\n  V:\n    label: A value\n    helpUrl: \"javascript:alert(1)\"\n",
        )
        .unwrap();

        let file = parse_pack_mcp_file(&path).expect("pack mcp file parses");
        assert!(file.setup["V"].help_url.is_none());
        // The rest of the description survives: one bad field is refused, not
        // the whole block.
        assert_eq!(file.setup["V"].label.as_deref(), Some("A value"));
    }
}
