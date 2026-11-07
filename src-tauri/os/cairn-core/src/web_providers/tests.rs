use std::path::{Path, PathBuf};

use cairn_packs::provider::http::{self, Inputs, PreparedRequest};
use tempfile::TempDir;

use super::*;
use crate::config::secrets::{self, mock_keychain};
use crate::mcp::oauth::store::{self as oauth_store, StoredAuth};
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

fn secret_present(pack: &str, credential: &str, var: &str) -> bool {
    secrets::has_secret(&secrets::pack_credential_key(pack, credential), var).unwrap()
}

fn prepare(active: &Active, target: Option<&str>, credential: &str) -> PreparedRequest {
    http::prepare(
        &active.provider.provider,
        active.provider.provider.http().unwrap(),
        &Inputs {
            query: Some("rust async"),
            target,
            credential,
            options: &active.options,
        },
    )
}

fn stored_auth(token: &str) -> StoredAuth {
    StoredAuth {
        issuer: Some("https://dash.cloudflare.com".into()),
        resource: String::new(),
        authorization_endpoint: "https://dash.cloudflare.com/oauth2/auth".into(),
        token_endpoint: "https://dash.cloudflare.com/oauth2/token".into(),
        registration_endpoint: None,
        client_id: "93e9beed884bec7080022cbf0f2d25ee".into(),
        client_secret: None,
        access_token: token.into(),
        refresh_token: None,
        expires_at: Some(chrono::Utc::now().timestamp() + 3600),
        scopes: vec!["browser-rendering.write".into()],
        needs_scopes: Vec::new(),
        rejected_at: None,
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

/// The fetch slot's built-in is selectable by name and clears the scalar, and
/// the slot view offers it beside the installed providers.
#[test]
fn regular_is_the_fetch_slot_builtin() {
    let temp = TempDir::new().unwrap();
    let config = temp.path().join("home");
    settings(&config, "activeWebFetch: ghost\n");
    set_active(&config, Slot::WebFetch, Some("regular")).unwrap();
    assert_eq!(active_name(&config, Slot::WebFetch), None);
    let view = slot_view(&config, Slot::WebFetch);
    let builtin = view.builtin.unwrap();
    assert_eq!((builtin.id, builtin.label), ("regular", "Regular"));
    assert!(slot_view(&config, Slot::WebSearch).builtin.is_none());
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
        readiness(&config, &provider),
        Some(NotReady::MissingVars {
            vars: vec!["WP_READY_KEY".to_string()]
        })
    );
    assert!(store_credential(&config, "wp-ready", "api-key", "  ").is_err());
    assert!(store_credential(&config, "wp-ready", "token", "v").is_err());
    store_credential(&config, "wp-ready", "api-key", "wp-Lr28").unwrap();
    assert_eq!(readiness(&config, &provider), None);
    assert!(secrets::has_secret("pack:wp-ready/api-key", "WP_READY_KEY").unwrap());

    delete_credential(&config, "wp-ready", "api-key").unwrap();
    assert!(matches!(
        readiness(&config, &provider),
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
    let error = send(
        &provider.pack,
        provider.credential.as_ref().unwrap(),
        &prepared,
    )
    .await
    .unwrap_err();
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
    assert!(secret_present("exa", "api-key", "EXA_API_KEY"));

    let search = provider_of(resolve(&config, Slot::WebSearch));
    assert_eq!(readiness(&config, &search.provider), None);
    let key =
        crate::security::broker::pack_credential("tavily", "api-key", "TAVILY_API_KEY", "test")
            .unwrap()
            .unwrap();
    let prepared = prepare(&search, None, key.expose());
    assert!(prepared
        .headers
        .contains(&("Authorization".to_string(), "Bearer tv-Mn31Qa".to_string())));
    let body: serde_json::Value = serde_json::from_str(prepared.body.as_deref().unwrap()).unwrap();
    assert_eq!(body["search_depth"], "advanced");

    let fetch = provider_of(resolve(&config, Slot::WebFetch));
    assert_eq!(
        fetch.provider.credential,
        installed_provider(&config, "jina", Slot::WebSearch)
            .unwrap()
            .unwrap()
            .credential
    );
    assert_eq!(readiness(&config, &fetch.provider), None);

    let again = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &app_resources());
    assert!(!again.changed(), "{again:?}");
}

/// `activeWebFetch: firecrawl` with a stored key keeps fetching after upgrade
/// with no click: the pack installs, the key moves, and the stored
/// `onlyMainContent` still reaches the request.
#[test]
fn a_firecrawl_workspace_keeps_fetching_after_upgrade() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let config = temp.path().join("home");
    settings(
        &config,
        "activeWebFetch: firecrawl\nwebFetch:\n  firecrawl:\n    onlyMainContent: false\n",
    );
    secrets::set_secret("firecrawl", "FIRECRAWL_API_KEY", "fc-Wq58Ej").unwrap();

    let report = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &app_resources());
    assert!(
        report.installed.contains(&"firecrawl".to_string()),
        "{report:?}"
    );
    assert!(report
        .keys_moved
        .contains(&"firecrawl/FIRECRAWL_API_KEY".to_string()));
    assert!(!secrets::has_secret("firecrawl", "FIRECRAWL_API_KEY").unwrap());

    let fetch = provider_of(resolve(&config, Slot::WebFetch));
    assert_eq!(readiness(&config, &fetch.provider), None);
    let key = crate::security::broker::pack_credential(
        "firecrawl",
        "api-key",
        "FIRECRAWL_API_KEY",
        "test",
    )
    .unwrap()
    .unwrap();
    let prepared = prepare(&fetch, Some("https://example.com/"), key.expose());
    assert!(prepared
        .headers
        .contains(&("Authorization".to_string(), "Bearer fc-Wq58Ej".to_string())));
    let body: serde_json::Value = serde_json::from_str(prepared.body.as_deref().unwrap()).unwrap();
    assert_eq!(body["onlyMainContent"], false);
    assert_eq!(body["url"], "https://example.com/");

    let again = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &app_resources());
    assert!(!again.changed(), "{again:?}");
}

/// A connected Cloudflare Browser Run keeps fetching after upgrade with no
/// click: the pack installs, the OAuth record moves from `web-fetch/cloudflare`
/// to the credential's account with its pending entry gone, a stored
/// `waitUntil: load` falls back to the declared default, and a second boot is
/// a no-op.
#[test]
fn a_connected_cloudflare_keeps_fetching_after_upgrade() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let config = temp.path().join("home");
    settings(
        &config,
        "activeWebFetch: cloudflare\nwebFetch:\n  cloudflare:\n    accountId: account-123\n    browser: chromium\n    waitUntil: load\n",
    );
    oauth_store::save("web-fetch/cloudflare", &stored_auth("cf-Lz40Hd")).unwrap();
    oauth_store::save_pending(
        "web-fetch/cloudflare/pending",
        &oauth_store::PendingAuthorization {
            state: "s".into(),
            code_verifier: "v".into(),
            created_at: 0,
        },
    )
    .unwrap();

    let report = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &app_resources());
    assert!(
        report.installed.contains(&"cloudflare".to_string()),
        "{report:?}"
    );
    assert_eq!(report.oauth_moved, vec!["web-fetch/cloudflare"]);
    assert!(oauth_store::load("web-fetch/cloudflare").unwrap().is_none());
    assert!(oauth_store::take_pending("web-fetch/cloudflare/pending")
        .unwrap()
        .is_none());
    assert_eq!(
        oauth_store::load(&oauth::account("cloudflare", "browser-run"))
            .unwrap()
            .unwrap()
            .access_token,
        "cf-Lz40Hd"
    );

    let fetch = provider_of(resolve(&config, Slot::WebFetch));
    assert_eq!(readiness(&config, &fetch.provider), None);
    let prepared = prepare(&fetch, Some("https://example.com/"), "cf-Lz40Hd");
    assert_eq!(
        prepared.url,
        "https://api.cloudflare.com/client/v4/accounts/account-123/browser-rendering/markdown?browser=chromium"
    );
    let body: serde_json::Value = serde_json::from_str(prepared.body.as_deref().unwrap()).unwrap();
    assert_eq!(body["gotoOptions"]["waitUntil"], "domcontentloaded");

    let again = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &app_resources());
    assert!(!again.changed(), "{again:?}");

    // The Library shows the retired `load` as the default it now means, so a
    // form that resubmits every option with an unrelated edit still saves.
    let view = slot_view(&config, Slot::WebFetch);
    let cloudflare = view
        .providers
        .iter()
        .find(|provider| provider.pack == "cloudflare")
        .unwrap();
    assert!(
        !cloudflare.saved_options.contains_key("waitUntil"),
        "{:?}",
        cloudflare.saved_options
    );
    let mut resubmitted = cloudflare.saved_options.clone();
    resubmitted.insert("accountId".into(), serde_yaml::Value::from("account-456"));
    resubmitted.insert("browser".into(), serde_yaml::Value::from("kitesurf"));
    resubmitted.insert(
        "waitUntil".into(),
        serde_yaml::Value::from("domcontentloaded"),
    );
    upsert_options(&config, Slot::WebFetch, "cloudflare", &resubmitted).unwrap();
    upsert_options(
        &config,
        Slot::WebFetch,
        "cloudflare",
        &HashMap::from([(
            "accountId".to_string(),
            serde_yaml::Value::from("account-123"),
        )]),
    )
    .unwrap();

    // The views carry presence only: no token in anything the Library or an
    // agent reads.
    let views = serde_json::to_string(&slot_views(&config)).unwrap();
    assert!(!views.contains("cf-Lz40Hd"), "{views}");
    assert!(views.contains("\"kind\":\"oauth\""), "{views}");

    // A blank required option is "needs setup", naming it.
    upsert_options(
        &config,
        Slot::WebFetch,
        "cloudflare",
        &HashMap::from([("accountId".to_string(), serde_yaml::Value::from(""))]),
    )
    .unwrap();
    let fetch = provider_of(resolve(&config, Slot::WebFetch));
    assert_eq!(
        readiness(&config, &fetch.provider),
        Some(NotReady::NeedsSetup {
            missing: vec!["Account ID".into()]
        })
    );

    // Signing out removes the token; the credential then needs auth.
    delete_credential(&config, "cloudflare", "browser-run").unwrap();
    assert!(
        oauth_store::load(&oauth::account("cloudflare", "browser-run"))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        credential_readiness("cloudflare", fetch.provider.credential.as_ref().unwrap()),
        Some(NotReady::NeedsAuth)
    );
    assert!(store_credential(&config, "cloudflare", "browser-run", "pasted").is_err());

    // The relay finds its credential; one nobody claims is refused by name.
    assert_eq!(
        oauth::resolve_relay(&config, "cloudflare").unwrap(),
        ("cloudflare".to_string(), "browser-run".to_string())
    );
    let error = oauth::resolve_relay(&config, "nobody").unwrap_err();
    assert!(error.contains("`nobody`"), "{error}");
}

/// Authorizing an `oauth` credential sends exactly what its declaration says:
/// a public PKCE S256 client, the relay redirect, and every declared scope.
/// Cloudflare refuses an authorize with no scope and any scope its client does
/// not list, so the scopes are pinned in their dotted form.
#[tokio::test]
async fn authorizing_an_oauth_credential_follows_its_declaration() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let config = temp.path().join("home");
    install(&app_resources(), &config, "cloudflare");

    let started = oauth::begin(&config, "cloudflare", "browser-run", None).unwrap();
    assert_eq!(
        started.redirect_uri,
        "https://cairn.computer/oauth/cloudflare/callback"
    );
    let url = url::Url::parse(&started.url).unwrap();
    let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
    assert_eq!(
        url.as_str().split('?').next().unwrap(),
        "https://dash.cloudflare.com/oauth2/auth"
    );
    assert_eq!(query["client_id"], "93e9beed884bec7080022cbf0f2d25ee");
    assert_eq!(query["redirect_uri"], started.redirect_uri);
    assert_eq!(query["response_type"], "code");
    assert_eq!(query["code_challenge_method"], "S256");
    assert_eq!(
        query["scope"].split(' ').collect::<Vec<_>>(),
        vec!["browser-rendering.write", "account-settings.read"]
    );
    assert!(!query.contains_key("resource"));
    assert!(!query.contains_key("client_secret"));

    // The pending `state` decides completion: a mismatch is refused before
    // any exchange, and the pending record is consumed either way.
    let error = oauth::complete(
        &config,
        "cloudflare",
        "browser-run",
        "code",
        "not-the-state",
        &started.redirect_uri,
    )
    .await
    .unwrap_err();
    assert!(error.contains("state mismatch"), "{error}");
    let error = oauth::complete(
        &config,
        "cloudflare",
        "browser-run",
        "code",
        &query["state"],
        &started.redirect_uri,
    )
    .await
    .unwrap_err();
    assert!(error.contains("No authorization"), "{error}");

    // A secret credential is not authorized this way.
    install(&app_resources(), &config, "jina");
    assert!(oauth::begin(&config, "jina", "api-key", None).is_err());
}

/// A workspace that already had the `cloudflare` connector pack installed at
/// its old version gains Browser Run from the ordinary boot sync of bundled
/// packs, with no click, so the migration needs no step of its own for it.
#[test]
fn an_installed_old_cloudflare_gains_browser_run_at_the_boot_sync() {
    let temp = TempDir::new().unwrap();
    let (old, config) = (temp.path().join("old"), temp.path().join("home"));
    let shipped = app_resources().join("packs/cloudflare");
    for file in ["cairn-pack.yaml", "mcp.yaml"] {
        write(
            &old.join("packs/cloudflare").join(file),
            &std::fs::read_to_string(shipped.join(file))
                .unwrap()
                .replace("version: 1.1.0", "version: 1.0.0"),
        );
    }
    install(&old, &config, "cloudflare");
    assert!(installed_provider(&config, "cloudflare", Slot::WebFetch)
        .unwrap()
        .is_none());

    cairn_packs::sync_workspace_packs(
        &RealGitClient,
        &RealFileSystem,
        &app_resources(),
        &config,
        "test",
    )
    .unwrap();
    let provider = installed_provider(&config, "cloudflare", Slot::WebFetch)
        .unwrap()
        .expect("the boot sync installs the newly shipped provider");
    assert_eq!(provider.provider.label, "Cloudflare Browser Run");
}

/// The last tool call a [`MockGateway`] saw: server, tool, arguments, timeout.
type RecordedCall = (String, String, serde_json::Value, Option<u32>);

#[derive(Default)]
struct MockGateway {
    last: std::sync::Mutex<Option<RecordedCall>>,
}

#[async_trait::async_trait]
impl crate::mcp::gateway::McpGateway for MockGateway {
    async fn list_tools(
        &self,
        _: &str,
        _: &str,
        _: &crate::security::BrokeredMcpConfig,
    ) -> Result<crate::mcp::gateway::McpToolCatalog, String> {
        Ok(Default::default())
    }
    async fn list_resources(
        &self,
        _: &str,
        _: &str,
        _: &crate::security::BrokeredMcpConfig,
    ) -> Result<Vec<crate::mcp::gateway::McpResourceDef>, String> {
        Ok(vec![])
    }
    async fn read_resource(
        &self,
        _: &str,
        _: &str,
        _: &crate::security::BrokeredMcpConfig,
        _: &str,
    ) -> Result<String, String> {
        Ok(String::new())
    }
    async fn call_tool(
        &self,
        session: &str,
        _server: &str,
        _config: &crate::security::BrokeredMcpConfig,
        tool: &str,
        args: serde_json::Value,
        timeout: Option<u32>,
    ) -> Result<crate::mcp::gateway::McpToolCallResult, String> {
        *self.last.lock().unwrap() = Some((session.into(), tool.into(), args, timeout));
        Ok(crate::mcp::gateway::McpToolCallResult::from_text(
            "# bmd markdown",
        ))
    }
    async fn close_session(&self, _: &str) {}
}

/// `activeWebFetch: bmd` installs the bmd pack. Its server needs auth until
/// connected, a settings server of the same name shadows it (and so keeps an
/// existing connection), and the binding calls `fetch` with `{source}`.
#[tokio::test]
async fn a_bmd_workspace_binds_the_bmd_server_fetch_tool() {
    use crate::config::mcp_servers::McpServerConfigExt;
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let config = temp.path().join("home");
    settings(&config, "activeWebFetch: bmd\n");

    let report = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &app_resources());
    assert!(report.installed.contains(&"bmd".to_string()), "{report:?}");
    let fetch = provider_of(resolve(&config, Slot::WebFetch));
    assert_eq!(fetch.provider.credential, None);
    assert_eq!(
        readiness(&config, &fetch.provider),
        Some(NotReady::NeedsAuth),
        "a pack server with no connection is not ready"
    );

    settings(
        &config,
        "activeWebFetch: bmd\nmcpServers:\n  bmd:\n    type: http\n    url: https://api.become.md/mcp\n    oauth: {}\n",
    );
    assert_eq!(
        readiness(&config, &fetch.provider),
        None,
        "a server the user authored is the layer above the pack's"
    );

    let Adapter::Mcp(binding) = &fetch.provider.provider.adapter else {
        panic!("bmd is not an MCP binding");
    };
    let gateway = MockGateway::default();
    let authored: crate::config::mcp_servers::McpServerConfig =
        serde_json::from_value(serde_json::json!({"type": "http", "url": "https://bmd.test/mcp"}))
            .unwrap();
    let brokered = authored.brokered("bmd", "test").unwrap();
    let text = call_binding(
        &gateway,
        &brokered,
        "bmd",
        &fetch.provider,
        binding,
        &Inputs {
            query: None,
            target: Some("https://example.com/a?b=c"),
            credential: "",
            options: &fetch.options,
        },
    )
    .await
    .unwrap();
    assert_eq!(text, "# bmd markdown");
    let (session, tool, args, timeout) = gateway.last.lock().unwrap().clone().unwrap();
    assert_eq!(session, "cairn-web-fetch");
    assert_eq!(tool, "fetch");
    assert_eq!(
        args,
        serde_json::json!({"source": "https://example.com/a?b=c"})
    );
    assert_eq!(timeout, Some(120_000));
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
    assert!(!secret_present("wp-mcp", "api-key", "WP_MCP_KEY"));
}

/// An orchestrator over a scratch database whose config dir is `config`.
async fn orchestrator(config: &Path) -> crate::orchestrator::Orchestrator {
    use std::sync::Arc;
    let local = crate::storage::LocalDb::open_migrated_for_test(
        tempfile::tempdir().unwrap().keep().join("web-providers.db"),
    )
    .await
    .unwrap();
    let search = Arc::new(
        crate::storage::SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap(),
    );
    crate::orchestrator::OrchestratorBuilder::new(
        Arc::new(crate::db::DbState::new(Arc::new(local), search)),
        Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
        config.to_path_buf(),
    )
    .build()
}

fn read_request() -> crate::mcp::types::McpCallbackRequest {
    crate::mcp::types::McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: String::new(),
        run_id: None,
        tool: "read".to_string(),
        payload: serde_json::json!({}),
        tool_use_id: None,
        origin: None,
    }
}

/// `cairn://websearch` is windowed like any read, and a provider failure is a
/// failed read whether or not a grep rides on it, never a zero-match result.
/// The fixture's `.test` host cannot resolve, so its send fails at once.
#[tokio::test]
async fn a_web_search_read_windows_its_body_and_a_failure_stays_failed_under_grep() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let config = temp.path().join("home");
    std::fs::create_dir_all(&config).unwrap();
    let orch = orchestrator(&config).await;
    let request = read_request();
    let read =
        |uri: &'static str| crate::resources::produce_cairn_resource(&orch, &request, uri);

    let windowed = read("cairn://websearch?q=rust&offset=10&limit=5").await;
    assert!(!windowed.failed, "{}", windowed.content);
    assert!(
        windowed.content.contains("No web-search provider"),
        "{}",
        windowed.content
    );
    assert_eq!((windowed.offset, windowed.limit), (Some(10), Some(5)));

    let resources = temp.path().join("res");
    fixture_pack(&resources, "wp-read", "WP_READ_KEY");
    install(&resources, &config, "wp-read");
    settings(&config, "activeWebSearch: wp-read\n");
    store_credential(&config, "wp-read", "api-key", "wr-Qz51Ve").unwrap();
    for uri in [
        "cairn://websearch?q=rust",
        "cairn://websearch?q=rust&grep=anything",
    ] {
        let failed = read(uri).await;
        assert!(failed.failed, "{uri}: {}", failed.content);
        assert!(
            failed.content.contains("Web search via Fixture failed"),
            "{uri}: {}",
            failed.content
        );
    }
}

/// A search provider with a required option left blank is a configuration
/// gap: guidance naming the option, and no credentialed request goes out
/// (which, to this fixture's unresolvable host, would be an `Err`).
#[tokio::test]
async fn a_search_with_a_blank_required_option_sends_nothing() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let (resources, config) = (temp.path().join("res"), temp.path().join("home"));
    std::fs::create_dir_all(&config).unwrap();
    fixture_pack(&resources, "wp-required", "WP_REQUIRED_KEY");
    let provider = resources.join("packs/wp-required/providers/web-search.yaml");
    let text = std::fs::read_to_string(&provider).unwrap().replace(
        "options:\n",
        "options:\n  - { key: account, label: Account, text: { placeholder: id }, default: \"\", required: true }\n",
    );
    write(&provider, &text);
    install(&resources, &config, "wp-required");
    settings(&config, "activeWebSearch: wp-required\n");
    store_credential(&config, "wp-required", "api-key", "rq-Lm62Kp").unwrap();
    let orch = orchestrator(&config).await;

    let guidance = crate::mcp::handlers::search_web::search_web(&orch, "rust")
        .await
        .unwrap();
    assert!(guidance.contains("needs its Account"), "{guidance}");
    assert!(guidance.contains(LIBRARY_LOCATION), "{guidance}");
}
