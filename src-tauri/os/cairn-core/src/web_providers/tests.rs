use std::path::{Path, PathBuf};

use cairn_packs::provider::http::{self, Inputs, PreparedRequest};
use tempfile::TempDir;

use super::*;
use crate::config::secrets::{self, mock_keychain};
use crate::services::{RealFileSystem, RealGitClient};

/// The app's real resource directory: `src-tauri`, holding `packs/`.
fn app_resources() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn write(path: &Path, text: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
}

/// A resource dir holding one search provider pack named `id`, whose
/// credential var is `var`. Each test uses its own id, because the mock
/// keychain is shared by every test in the process.
fn fixture_pack(resources: &Path, id: &str, var: &str) {
    let root = resources.join("packs").join(id);
    write(
        &root.join("cairn-pack.yaml"),
        &format!("cairnVersion: 1\nid: {id}\nname: Fixture {id}\nversion: 1.0.0\ndescription: d\ndefault: false\n"),
    );
    write(
        &root.join("credentials/api-key.yaml"),
        &format!("label: Key\nkind: secret\nvar: {var}\nhosts: [api.{id}.test]\naccessors: [provider:web-search]\n"),
    );
    write(
        &root.join("providers/web-search.yaml"),
        &format!(
            "label: Fixture\nprovides: web-search\ncredential: api-key\noptions:\n  - {{ key: count, label: Count, number: {{ min: 1, max: 9, integer: true }}, default: 3 }}\nadapter:\n  http:\n    request:\n      method: GET\n      url: https://api.{id}.test/?q=${{query|query}}&n=${{options.count|query}}\n      headers: {{ X-Key: \"${{credential}}\" }}\n    response:\n      results: results\n      fields: {{ url: [url] }}\n"
        ),
    );
}

fn settings(config: &Path, text: &str) {
    write(&crate::config::settings::get_settings_path(config), text);
}

fn install(resources: &Path, config: &Path, id: &str) {
    cairn_packs::sync_one_pack(&RealGitClient, &RealFileSystem, resources, config, id).unwrap();
}

fn provider_of(resolved: Resolved) -> Box<Active> {
    match resolved {
        Resolved::Provider(active) => active,
        other => panic!("expected a provider, got {other:?}"),
    }
}

#[test]
fn resolution_follows_the_scalar_and_what_is_installed() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let (resources, config) = (temp.path().join("res"), temp.path().join("home"));
    fixture_pack(&resources, "wp-resolve", "WP_RESOLVE_KEY");

    assert!(matches!(
        resolve(&config, Slot::WebSearch),
        Resolved::Unconfigured
    ));
    settings(
        &config,
        "activeWebSearch: wp-resolve\nactiveWebFetch: regular\n",
    );
    assert!(matches!(
        resolve(&config, Slot::WebSearch),
        Resolved::NotInstalled { name } if name == "wp-resolve"
    ));
    assert!(matches!(
        resolve(&config, Slot::WebFetch),
        Resolved::Unconfigured
    ));

    install(&resources, &config, "wp-resolve");
    assert_eq!(
        set_active(&config, Slot::WebSearch, Some("ghost")).unwrap_err(),
        "`ghost` is not an installed web-search provider"
    );
    set_active(&config, Slot::WebSearch, Some("wp-resolve")).unwrap();
    let active = provider_of(resolve(&config, Slot::WebSearch));
    assert_eq!(active.provider.pack, "wp-resolve");
    assert!(matches!(
        resolve(&config, Slot::WebFetch),
        Resolved::Unconfigured
    ));

    set_active(&config, Slot::WebSearch, None).unwrap();
    assert!(matches!(
        resolve(&config, Slot::WebSearch),
        Resolved::Unconfigured
    ));
}

#[test]
fn readiness_is_needs_key_until_the_credential_is_stored() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let (resources, config) = (temp.path().join("res"), temp.path().join("home"));
    fixture_pack(&resources, "wp-ready", "WP_READY_KEY");
    install(&resources, &config, "wp-ready");
    let provider = installed_provider(&config, "wp-ready", Slot::WebSearch)
        .unwrap()
        .unwrap();

    assert_eq!(
        readiness(&provider),
        Some(NotReady::MissingVars {
            vars: vec!["WP_READY_KEY".to_string()]
        })
    );
    assert!(store_credential(&config, "wp-ready", "api-key", "  ").is_err());
    assert!(store_credential(&config, "wp-ready", "token", "v").is_err());
    store_credential(&config, "wp-ready", "api-key", "wp-Lr28").unwrap();
    assert_eq!(readiness(&provider), None);
    assert!(secrets::has_secret("pack:wp-ready/api-key", "WP_READY_KEY").unwrap());

    delete_credential(&config, "wp-ready", "api-key").unwrap();
    assert!(matches!(
        readiness(&provider),
        Some(NotReady::MissingVars { .. })
    ));
}

#[test]
fn options_are_validated_against_the_pack_and_stored_by_pack_id() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let (resources, config) = (temp.path().join("res"), temp.path().join("home"));
    fixture_pack(&resources, "wp-options", "WP_OPTIONS_KEY");
    install(&resources, &config, "wp-options");

    let bad = HashMap::from([("count".to_string(), serde_yaml::Value::from(99))]);
    assert!(upsert_options(&config, Slot::WebSearch, "wp-options", &bad).is_err());
    let good = HashMap::from([("count".to_string(), serde_yaml::Value::from(7))]);
    upsert_options(&config, Slot::WebSearch, "wp-options", &good).unwrap();
    let raw = std::fs::read_to_string(crate::config::settings::get_settings_path(&config)).unwrap();
    assert!(
        raw.contains("webSearch:\n  wp-options:\n    count: 7"),
        "{raw}"
    );
}

#[test]
fn a_credential_reaches_only_its_declared_hosts() {
    let hosts = vec!["api.example.test".to_string()];
    let at = |url: &str| may_reach(&hosts, &url::Url::parse(url).unwrap());
    assert!(at("https://api.example.test/search"));
    assert!(at("https://API.example.test/search"));
    assert!(!at("http://api.example.test/search"), "never in the clear");
    assert!(!at("https://api.example.test.evil.test/"));
    assert!(!at("https://evil.test/?h=api.example.test"));
}

#[tokio::test]
async fn send_refuses_an_undeclared_host_before_anything_leaves() {
    let temp = TempDir::new().unwrap();
    let resources = temp.path().join("res");
    fixture_pack(&resources, "wp-send", "WP_SEND_KEY");
    let provider = cairn_packs::provider::load_pack_provider(
        &resources.join("packs/wp-send"),
        "wp-send",
        Slot::WebSearch,
    )
    .unwrap()
    .unwrap();
    let prepared = PreparedRequest {
        method: "GET".into(),
        url: "https://elsewhere.test/".into(),
        headers: vec![("X-Key".into(), "k".into())],
        body: None,
    };
    let error = send(&provider, &prepared).await.unwrap_err();
    assert!(
        error.contains("refusing to send credential `wp-send/api-key` to `elsewhere.test`"),
        "{error}"
    );
}

/// The upgrade acceptance, against the packs the app really ships: a workspace
/// that searched through Tavily and fetched through Jina keeps both, with no
/// click, its keys move to the pack namespace, a stored key for an unselected
/// provider is kept, and a second boot changes nothing.
#[test]
fn a_configured_workspace_keeps_working_after_upgrade() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let config = temp.path().join("home");
    settings(
        &config,
        "activeWebSearch: tavily\nactiveWebFetch: jina\nwebSearch:\n  tavily:\n    searchDepth: advanced\n",
    );
    secrets::set_secret("tavily", "TAVILY_API_KEY", "tv-Mn31Qa").unwrap();
    secrets::set_secret("jina", "JINA_API_KEY", "jn-Hc72Rb").unwrap();
    secrets::set_secret("exa", "EXA_API_KEY", "ex-Bd04Ws").unwrap();

    let report = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &app_resources());
    assert_eq!(report.installed, vec!["tavily", "jina"]);
    for moved in [
        "tavily/TAVILY_API_KEY",
        "jina/JINA_API_KEY",
        "exa/EXA_API_KEY",
    ] {
        assert!(report.keys_moved.contains(&moved.to_string()), "{report:?}");
    }
    for (owner, var) in [
        ("tavily", "TAVILY_API_KEY"),
        ("jina", "JINA_API_KEY"),
        ("exa", "EXA_API_KEY"),
    ] {
        assert!(
            !secrets::has_secret(owner, var).unwrap(),
            "{owner}/{var} still stored"
        );
    }
    assert!(cairn_packs::lock::read_lock(&config, "exa").is_none());
    assert!(credential_present("exa", "api-key", "EXA_API_KEY").unwrap());

    let search = provider_of(resolve(&config, Slot::WebSearch));
    assert_eq!(readiness(&search.provider), None);
    let key =
        crate::security::broker::pack_credential("tavily", "api-key", "TAVILY_API_KEY", "test")
            .unwrap()
            .unwrap();
    let prepared = http::prepare(
        &search.provider.provider,
        &Inputs {
            query: Some("rust async"),
            target: None,
            credential: key.expose(),
            options: &search.options,
        },
    );
    assert!(prepared
        .headers
        .contains(&("Authorization".to_string(), "Bearer tv-Mn31Qa".to_string())));
    let body: serde_json::Value = serde_json::from_str(prepared.body.as_deref().unwrap()).unwrap();
    assert_eq!(body["search_depth"], "advanced");

    let fetch = provider_of(resolve(&config, Slot::WebFetch));
    assert_eq!(
        fetch.provider.credential,
        search_credential_of(&config, "jina")
    );
    assert_eq!(readiness(&fetch.provider), None);

    let again = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &app_resources());
    assert!(!again.changed(), "{again:?}");
}

fn search_credential_of(config: &Path, pack: &str) -> cairn_packs::provider::CredentialDecl {
    installed_provider(config, pack, Slot::WebSearch)
        .unwrap()
        .unwrap()
        .credential
}

#[test]
fn an_uninstalled_pack_is_not_reinstalled() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let (resources, config) = (temp.path().join("res"), temp.path().join("home"));
    fixture_pack(&resources, "wp-gone", "WP_GONE_KEY");
    settings(&config, "activeWebSearch: wp-gone\n");
    cairn_packs::lock::record_uninstall(&RealFileSystem, &config, "wp-gone").unwrap();

    let report = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &resources);
    assert!(report.installed.is_empty());
    assert!(cairn_packs::lock::read_lock(&config, "wp-gone").is_none());
    assert!(
        report.skipped.iter().any(|s| s.starts_with("wp-gone:")),
        "{report:?}"
    );
}

/// A slot scalar is writable with the session token, so it must never become
/// a way to install a pack that runs a machine service.
#[test]
fn a_pack_carrying_a_machine_service_is_never_installed_by_the_migration() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let (resources, config) = (temp.path().join("res"), temp.path().join("home"));
    fixture_pack(&resources, "wp-service", "WP_SERVICE_KEY");
    write(
        &resources.join("packs/wp-service/services/helper/service.yaml"),
        "name: Helper\n",
    );
    settings(&config, "activeWebSearch: wp-service\n");

    let report = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &resources);
    assert!(report.installed.is_empty());
    assert!(cairn_packs::lock::read_lock(&config, "wp-service").is_none());
    assert_eq!(
        active_name(&config, Slot::WebSearch).as_deref(),
        Some("wp-service"),
        "the scalar is left alone"
    );
    assert!(report
        .skipped
        .contains(&"wp-service: carries a machine service".to_string()));
}

/// `{pack}/{VAR}` is also the account a workspace MCP server named `{pack}`
/// reads `${VAR}` from, so it is left where it is when such a server uses it.
#[test]
fn a_key_an_mcp_server_of_the_same_name_uses_stays_put() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let (resources, config) = (temp.path().join("res"), temp.path().join("home"));
    fixture_pack(&resources, "wp-mcp", "WP_MCP_KEY");
    settings(
        &config,
        "mcpServers:\n  wp-mcp:\n    type: http\n    url: https://mcp.wp-mcp.test/mcp\n    headers:\n      Authorization: \"Bearer ${WP_MCP_KEY}\"\n",
    );
    secrets::set_secret("wp-mcp", "WP_MCP_KEY", "mcp-Yt55").unwrap();

    let report = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &resources);
    assert!(report.keys_moved.is_empty(), "{report:?}");
    assert!(secrets::has_secret("wp-mcp", "WP_MCP_KEY").unwrap());
    assert!(!credential_present("wp-mcp", "api-key", "WP_MCP_KEY").unwrap());
}
