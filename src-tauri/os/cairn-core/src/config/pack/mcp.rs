//! Pack credential storage belongs to the runner.
pub use cairn_packs::mcp::*;
use std::path::Path;
/// Store the value a gated pack server's `${VAR}` is waiting for.
///
/// One entry point, because *where* the value is written is its security
/// declaration and that decision must not be made twice. The providing pack's
/// `setup` block names the kind, so the routing is the pack author's stated
/// intent resolved here rather than a claim carried in from the caller.
///
/// Only a variable an installed pack's server actually references can be set
/// this way. A user-authored server in `settings.yaml` is never gated and has
/// its own editor; letting this reach one would be a second way to configure
/// the same thing with different semantics.
///
/// A `path` value must be absolute. That is a boundary check on shape, not on
/// filesystem state — the executable may legitimately not be installed yet, and
/// the declaration has to mean the same thing before and after it appears. It
/// is also what keeps a pack from routing a credential into the unprotected
/// namespace by mislabelling its variable: whatever a manifest claims, only
/// path-shaped values can land there.
pub fn store_pack_mcp_value(
    config_dir: &Path,
    server: &str,
    var: &str,
    value: &str,
) -> Result<(), String> {
    let layer = load_pack_mcp_servers(config_dir);
    let entry = layer
        .get(server)
        .ok_or_else(|| format!("No installed pack supplies an MCP server named `{server}`"))?;
    if !entry.config.referenced_vars().contains(var) {
        return Err(format!(
            "MCP server `{server}` does not reference ${{{var}}}"
        ));
    }

    let value = value.trim();
    if value.is_empty() {
        return Err("A value is required".to_string());
    }

    let key = crate::config::secrets::credential_key(server, None);
    match entry.setup.get(var).map(|s| s.kind).unwrap_or_default() {
        McpVarKind::Path => {
            if !Path::new(value).is_absolute() {
                return Err(format!(
                    "`{var}` takes a full path starting from the root of the filesystem, \
                     like /usr/local/bin/{server}-mcp-server"
                ));
            }
            crate::config::secrets::set_config_value(&key, var, value)?;
            // The two namespaces must never both answer for one variable: the
            // secret one is read first, so a leftover there would shadow this
            // write and resolve as a credential.
            crate::config::secrets::delete_secret(&key, var)?;
        }
        McpVarKind::Secret => {
            crate::config::secrets::set_secret(&key, var, value)?;
            crate::config::secrets::delete_config_value(&key, var)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    const MATLAB_PACK: &str = "mcpServers:\n  \
                                store-matlab:\n    \
                                  command: ${STORE_MATLAB_BIN}\n    \
                                  env:\n      \
                                    TOKEN: ${STORE_MATLAB_TOKEN}\n\
                              setup:\n  \
                                STORE_MATLAB_BIN:\n    \
                                  kind: path\n";
    /// Install a pack in `config_dir` holding `mcp` as its MCP file.
    fn install_pack(config_dir: &Path, id: &str, mcp: &str) {
        let dir = config_dir.join("packs").join(id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("pack.yaml"),
            format!(
                "cairnVersion: 1\nid: {id}\nname: {id}\nversion: 1.0.0\n\
                 installedAt: now\ncontentHash: h\nsource:\n  kind: bundled\n"
            ),
        )
        .unwrap();
        std::fs::write(dir.join("mcp.yaml"), mcp).unwrap();
    }

    /// The declaration decides the store, and the store is what decides whether
    /// the value is ever treated as a credential.
    // These tests share `store-matlab`'s entries in the process-wide test
    // keychain, so one test's stored value would otherwise race another's
    // "nothing stored" assertion.
    #[test]
    #[serial_test::serial(store_matlab_keychain)]
    fn a_supplied_value_lands_in_the_namespace_its_declaration_names() {
        crate::config::secrets::mock_keychain::install();
        let ws = tempfile::tempdir().unwrap();
        install_pack(ws.path(), "matlab", MATLAB_PACK);

        store_pack_mcp_value(
            ws.path(),
            "store-matlab",
            "STORE_MATLAB_BIN",
            "/opt/mcp/matlab",
        )
        .expect("a path is accepted");
        assert_eq!(
            crate::config::secrets::get_config_value("store-matlab", "STORE_MATLAB_BIN").as_deref(),
            Some("/opt/mcp/matlab")
        );
        assert!(!crate::config::secrets::has_secret(
            "store-matlab",
            "STORE_MATLAB_BIN"
        ));

        // An undeclared variable defaults to secret, which is the safe way to
        // be wrong: a value treated as a credential that isn't costs a mask.
        store_pack_mcp_value(
            ws.path(),
            "store-matlab",
            "STORE_MATLAB_TOKEN",
            "tok-Zx91Qw82",
        )
        .expect("a secret is accepted");
        assert!(crate::config::secrets::has_secret(
            "store-matlab",
            "STORE_MATLAB_TOKEN"
        ));
        assert!(
            crate::config::secrets::get_config_value("store-matlab", "STORE_MATLAB_TOKEN")
                .is_none()
        );
    }

    /// Whatever a manifest claims, only path-shaped values can reach the
    /// unprotected namespace — so mislabelling a variable cannot be used to
    /// strip scrubbing from a token.
    ///
    /// Shape, not existence: the executable may legitimately not be installed
    /// yet, and the declaration has to mean the same thing before and after it
    /// appears.
    #[test]
    #[serial_test::serial(store_matlab_keychain)]
    fn a_path_variable_refuses_anything_that_is_not_a_path() {
        crate::config::secrets::mock_keychain::install();
        let ws = tempfile::tempdir().unwrap();
        install_pack(ws.path(), "matlab", MATLAB_PACK);
        // Start from nothing stored, whatever an earlier test left behind.
        crate::config::secrets::delete_config_value("store-matlab", "STORE_MATLAB_BIN").unwrap();

        let error = store_pack_mcp_value(
            ws.path(),
            "store-matlab",
            "STORE_MATLAB_BIN",
            "lin_api_Zx91Qw",
        )
        .expect_err("a bare token is not a path");
        assert!(error.contains("full path"), "{error}");
        assert!(
            crate::config::secrets::get_config_value("store-matlab", "STORE_MATLAB_BIN").is_none()
        );

        // A path to something not yet installed is accepted: waiting for the
        // download is the normal order of events.
        store_pack_mcp_value(
            ws.path(),
            "store-matlab",
            "STORE_MATLAB_BIN",
            "/opt/not/installed/yet",
        )
        .expect("a path need not exist yet");
    }

    /// The affordance reaches pack-supplied servers and their own variables,
    /// and nothing else. A user-authored server is never gated and has its own
    /// editor; a second way in would be a second set of semantics.
    #[test]
    fn only_a_pack_servers_own_variables_can_be_supplied_this_way() {
        crate::config::secrets::mock_keychain::install();
        let ws = tempfile::tempdir().unwrap();
        install_pack(ws.path(), "matlab", MATLAB_PACK);

        let error = store_pack_mcp_value(ws.path(), "not-a-pack-server", "ANYTHING", "/tmp/x")
            .expect_err("no pack supplies that server");
        assert!(error.contains("No installed pack"), "{error}");

        let error = store_pack_mcp_value(ws.path(), "store-matlab", "UNREFERENCED", "/tmp/x")
            .expect_err("the server does not reference that variable");
        assert!(error.contains("does not reference"), "{error}");
    }

    /// Re-supplying a variable must not leave the previous answer behind in the
    /// other namespace, where the secret-first read order would resurrect it.
    #[test]
    #[serial_test::serial(store_matlab_keychain)]
    fn re_supplying_a_variable_clears_the_other_namespace() {
        crate::config::secrets::mock_keychain::install();
        let ws = tempfile::tempdir().unwrap();
        install_pack(ws.path(), "matlab", MATLAB_PACK);

        // A stale secret under the same name — from an earlier install, or an
        // earlier version of the pack that called this variable a secret.
        crate::config::secrets::set_secret("store-matlab", "STORE_MATLAB_BIN", "old-Zx91Qw82")
            .unwrap();
        store_pack_mcp_value(
            ws.path(),
            "store-matlab",
            "STORE_MATLAB_BIN",
            "/opt/mcp/matlab",
        )
        .unwrap();

        assert!(
            !crate::config::secrets::has_secret("store-matlab", "STORE_MATLAB_BIN"),
            "the stale secret would shadow the value just supplied"
        );
    }
}
