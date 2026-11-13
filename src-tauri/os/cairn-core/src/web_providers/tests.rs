use std::path::{Path, PathBuf};

use cairn_packs::provider::http::{self, Inputs, PreparedRequest};
use tempfile::TempDir;

use super::choices::Settled;
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

/// A resource dir holding one PDF provider pack named `id`: an MCP server of
/// the same name, and a binding that hands the URL to its `extract` tool.
fn fixture_pdf_pack(resources: &Path, id: &str) {
    let root = resources.join("packs").join(id);
    write(
        &root.join("cairn-pack.yaml"),
        &format!("cairnVersion: 1\nid: {id}\nname: Fixture {id}\nversion: 1.0.0\ndescription: d\ndefault: false\n"),
    );
    write(
        &root.join("mcp.yaml"),
        &format!("mcpServers:\n  {id}:\n    type: http\n    url: https://mcp.{id}.test/mcp\n"),
    );
    write(
        &root.join("providers/pdf.yaml"),
        &format!(
            "label: Fixture\nprovides: pdf\nadapter:\n  mcp:\n    server: {id}\n    tool: extract\n    arguments: {{ source: \"${{target}}\" }}\n    result: text\n"
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

    // An account is never stored blank; one left unset is "needs setup",
    // naming it.
    assert!(upsert_options(
        &config,
        Slot::WebFetch,
        "cloudflare",
        &HashMap::from([("accountId".to_string(), serde_yaml::Value::from(""))]),
    )
    .is_err());
    upsert_options(
        &config,
        Slot::WebFetch,
        "cloudflare",
        &HashMap::from([("browser".to_string(), serde_yaml::Value::from("kitesurf"))]),
    )
    .unwrap();
    let fetch = provider_of(resolve(&config, Slot::WebFetch));
    assert_eq!(
        readiness(&config, &fetch.provider),
        Some(NotReady::NeedsSetup {
            missing: vec!["Cloudflare account".into()]
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

/// The text every [`MockGateway`] tool call returns.
const MOCK_TEXT: &str = "# bmd markdown";

#[derive(Default)]
struct MockGateway {
    last: std::sync::Mutex<Option<RecordedCall>>,
    /// The server name and URL the last call was handed: which definition of
    /// a server won shows here.
    endpoint: std::sync::Mutex<Option<(String, Option<String>)>>,
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
        server: &str,
        config: &crate::security::BrokeredMcpConfig,
        tool: &str,
        args: serde_json::Value,
        timeout: Option<u32>,
    ) -> Result<crate::mcp::gateway::McpToolCallResult, String> {
        *self.last.lock().unwrap() = Some((session.into(), tool.into(), args, timeout));
        *self.endpoint.lock().unwrap() =
            Some((server.into(), config.resolved_for_connect().url.clone()));
        Ok(crate::mcp::gateway::McpToolCallResult::from_text(MOCK_TEXT))
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

/// `activePdf: bmd` keeps working after upgrade with no click: the migration
/// installs the bundled pack that provides the slot, and the binding calls the
/// same `fetch` tool with `{source}` the compiled provider did. The built-in
/// `local` extractor stays selectable by name and installs nothing.
#[tokio::test]
async fn an_active_pdf_bmd_workspace_binds_the_bmd_server_fetch_tool() {
    use crate::config::mcp_servers::McpServerConfigExt;
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let config = temp.path().join("home");
    settings(&config, "activePdf: bmd\n");

    let report = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &app_resources());
    assert!(report.installed.contains(&"bmd".to_string()), "{report:?}");
    let pdf = provider_of(resolve(&config, Slot::Pdf));
    let Adapter::Mcp(binding) = &pdf.provider.provider.adapter else {
        panic!("bmd's pdf provider is not an MCP binding");
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
        &pdf.provider,
        binding,
        &Inputs {
            query: None,
            target: Some("https://example.com/paper.pdf"),
            credential: "",
            options: &pdf.options,
        },
    )
    .await
    .unwrap();
    assert_eq!(text, "# bmd markdown");
    let (session, tool, args, timeout) = gateway.last.lock().unwrap().clone().unwrap();
    assert_eq!(session, "cairn-pdf");
    assert_eq!(tool, "fetch");
    assert_eq!(
        args,
        serde_json::json!({"source": "https://example.com/paper.pdf"})
    );
    assert_eq!(timeout, Some(120_000));

    settings(&config, "activePdf: local\n");
    assert!(matches!(
        resolve(&config, Slot::Pdf),
        Resolved::Unconfigured
    ));
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
/// a way to install a pack that runs a machine service, whichever slot names
/// it.
#[test]
fn a_pack_carrying_a_machine_service_is_never_installed_by_the_migration() {
    mock_keychain::install();
    for (slot, id) in [
        (Slot::WebSearch, "wp-service"),
        (Slot::Pdf, "wp-pdf-service"),
    ] {
        let temp = TempDir::new().unwrap();
        let (resources, config) = (temp.path().join("res"), temp.path().join("home"));
        if slot == Slot::Pdf {
            fixture_pdf_pack(&resources, id);
        } else {
            fixture_pack(&resources, id, "WP_SERVICE_KEY");
        }
        // The migration skips silently past a pack whose provider does not
        // load, so the fixture must be one it would otherwise install.
        assert!(
            matches!(
                cairn_packs::provider::load_pack_provider(
                    &resources.join("packs").join(id),
                    id,
                    slot
                ),
                Ok(Some(_))
            ),
            "{id}: the fixture's {slot:?} provider must load"
        );
        write(
            &resources
                .join("packs")
                .join(id)
                .join("services/helper/service.yaml"),
            "name: Helper\n",
        );
        settings(&config, &format!("{}: {id}\n", scalar_key(slot)));

        let report = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &resources);
        assert!(report.installed.is_empty(), "{id}: {report:?}");
        assert!(cairn_packs::lock::read_lock(&config, id).is_none(), "{id}");
        assert_eq!(
            active_name(&config, slot).as_deref(),
            Some(id),
            "the scalar is left alone"
        );
        assert!(
            report
                .skipped
                .contains(&format!("{id}: carries a machine service")),
            "{id}: {report:?}"
        );
    }
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
    let read = |uri: &'static str| crate::resources::produce_cairn_resource(&orch, &request, uri);

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

/// A resource dir holding one fetch provider pack named `id` whose required
/// `account` is a select fetched with its secret credential (var `var`) from
/// `api.<id>.test`, which cannot resolve.
fn choices_pack(resources: &Path, id: &str, var: &str) {
    let root = resources.join("packs").join(id);
    write(
        &root.join("cairn-pack.yaml"),
        &format!("cairnVersion: 1\nid: {id}\nname: Fixture {id}\nversion: 1.0.0\ndescription: d\ndefault: false\n"),
    );
    write(
        &root.join("credentials/api-key.yaml"),
        &format!("label: Key\nkind: secret\nvar: {var}\nhosts: [api.{id}.test]\naccessors: [provider:web-fetch]\n"),
    );
    write(
        &root.join("providers/web-fetch.yaml"),
        &r#"label: Fixture
provides: web-fetch
credential: api-key
options:
  - key: account
    label: Account
    required: true
    select:
      from:
        request: { method: GET, url: "https://api.ID.test/accounts", headers: { X-Key: "${credential}" } }
        items: result
        value: id
        label: name
  - { key: mode, label: Mode, select: [[a, A], [b, B]], default: a }
adapter:
  http:
    request:
      method: GET
      url: https://api.ID.test/accounts/${options.account|query}/render?u=${target|query}&m=${options.mode|query}
      headers: { X-Key: "${credential}" }
    response:
      markdown: body
"#
        .replace("ID", id),
    );
}

fn accounts(items: &str) -> String {
    format!(r#"{{"success":true,"result":[{items}]}}"#)
}

/// The settle rule, against a real installed declaration: several choices are
/// left for the operator, sorted; none and a failure are reported; one is
/// stored beside the other options; and a stored value is never replaced.
#[test]
fn a_fetched_choice_settles_by_one_rule() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let (resources, config) = (temp.path().join("res"), temp.path().join("home"));
    choices_pack(&resources, "wp-settle", "WP_SETTLE_KEY");
    install(&resources, &config, "wp-settle");
    settings(
        &config,
        "activeWebFetch: wp-settle\nwebFetch:\n  wp-settle:\n    mode: b\n",
    );
    let provider = installed_provider(&config, "wp-settle", Slot::WebFetch)
        .unwrap()
        .unwrap();
    let pending = choices::pending(&config, &provider);
    assert_eq!(
        pending.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
        ["account"]
    );
    let option = &pending[0];
    let respond =
        |status: u16, body: &str| choices::choices_from_response("Fixture", option, status, body);
    let record = |result| choices::record(&config, &provider, option, result);

    let two = respond(
        200,
        &accounts(r#"{"id":"z9","name":"Zeta"},{"id":"a1","name":"Acme"}"#),
    );
    let Settled::Choose(choices) = record(two) else {
        panic!("two choices are the operator's");
    };
    assert_eq!(
        choices.iter().map(|c| c.label.as_str()).collect::<Vec<_>>(),
        ["Acme", "Zeta"]
    );
    assert_eq!(record(respond(200, &accounts(""))), Settled::None);
    for failure in [
        respond(403, r#"{"success":false}"#),
        respond(200, "not json"),
    ] {
        assert!(matches!(record(failure), Settled::Failed(_)));
    }
    assert_eq!(
        choices::pending(&config, &provider).len(),
        1,
        "nothing stored"
    );

    let one = respond(200, &accounts(r#"{"id":"acct-1","name":"Acme"}"#));
    assert!(matches!(record(one), Settled::Stored(_)));
    let stored = load_options(&config, Slot::WebFetch, "wp-settle");
    assert_eq!(stored["account"], serde_yaml::Value::from("acct-1"));
    assert_eq!(stored["mode"], serde_yaml::Value::from("b"));
    assert!(choices::pending(&config, &provider).is_empty());

    let other = respond(200, &accounts(r#"{"id":"acct-2","name":"Other"}"#));
    record(other);
    assert_eq!(
        load_options(&config, Slot::WebFetch, "wp-settle")["account"],
        serde_yaml::Value::from("acct-1"),
        "a stored value is never replaced"
    );

    let fetch = provider_of(resolve(&config, Slot::WebFetch));
    assert_eq!(
        readiness(&config, &fetch.provider),
        Some(NotReady::MissingVars {
            vars: vec!["WP_SETTLE_KEY".into()]
        })
    );
    let prepared = prepare(&fetch, Some("https://example.com/"), "k");
    assert!(
        prepared
            .url
            .starts_with("https://api.wp-settle.test/accounts/acct-1/render?"),
        "{}",
        prepared.url
    );
}

/// At use, a blank fetched choice is the credential's gap while the credential
/// holds nothing, and once it does the listing really goes out through `send`:
/// this fixture's host cannot resolve, so it fails, and the read fails naming
/// the option rather than sending a request with no account in it.
#[tokio::test]
async fn a_fetch_fills_its_blank_choice_from_the_credential_or_says_why_not() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let (resources, config) = (temp.path().join("res"), temp.path().join("home"));
    std::fs::create_dir_all(&config).unwrap();
    choices_pack(&resources, "wp-fill", "WP_FILL_KEY");
    install(&resources, &config, "wp-fill");
    settings(&config, "activeWebFetch: wp-fill\n");

    let mut active = provider_of(resolve(&config, Slot::WebFetch));
    assert!(matches!(
        choices::fill_required(&config, &mut active).await,
        choices::Gap::Credential(CredentialKind::Secret { .. })
    ));

    store_credential(&config, "wp-fill", "api-key", "wf-Hx20Qa").unwrap();
    let gap = choices::fill_required(&config, &mut active).await;
    let choices::Gap::Failed { option, error } = &gap else {
        panic!("an unreachable listing is a failure, got {gap:?}");
    };
    assert_eq!(option, "Account");
    assert!(
        error.contains("Fixture could not list its Account choices"),
        "{error}"
    );
    assert!(!error.contains("wf-Hx20Qa"), "{error}");

    let orch = orchestrator(&config).await;
    let read = crate::mcp::handlers::fetch_web::read_fetch_markdown(&orch, "https://example.com/")
        .await
        .unwrap_err();
    assert!(
        read.starts_with("Fixture web fetch needs its Account, and listing the choices failed"),
        "{read}"
    );
}

/// A copy of the shipped Cloudflare pack under its own id, so the keychain
/// account its token is stored under belongs to this test alone.
fn cloudflare_copy(resources: &Path, id: &str) {
    let shipped = app_resources().join("packs/cloudflare");
    for rel in [
        "cairn-pack.yaml",
        "mcp.yaml",
        "credentials/browser-run.yaml",
        "providers/web-fetch.yaml",
    ] {
        let text = std::fs::read_to_string(shipped.join(rel)).unwrap();
        let text = text.replace("id: cloudflare\n", &format!("id: {id}\n"));
        write(&resources.join("packs").join(id).join(rel), &text);
    }
}

/// A listing stand-in that records each request and answers with `answer`.
fn listing(
    seen: &std::sync::Arc<std::sync::Mutex<Vec<PreparedRequest>>>,
    answer: impl Fn() -> choices::test_listing::Answer + Send + Sync + 'static,
) -> choices::test_listing::StandIn {
    let seen = seen.clone();
    std::sync::Arc::new(move |request| {
        seen.lock().unwrap().push(request);
        answer()
    })
}

fn answered(status: u16, body: &'static str) -> choices::test_listing::Answer {
    Box::pin(async move { Ok((status, body.to_string())) })
}

/// Connect's tail as `complete` runs it once the token is stored: the bounded
/// settle, then what the connect reports it still needs.
async fn connect_settles(
    config: &Path,
    pack: &str,
    stand_in: choices::test_listing::StandIn,
) -> Option<NotReady> {
    choices::test_listing::LISTING
        .scope(
            stand_in,
            choices::settle_after_connect(config, pack, "browser-run"),
        )
        .await;
    choices::still_needs(config, pack, "browser-run")
}

/// Against the shipped Cloudflare declaration with a stored login: a refused
/// listing, a malformed one, and one that never answers each leave the login
/// connected and still needing its account, within the connect's bound; one
/// account is stored, and the Browser Run request then names it; and a login
/// connected before the option existed is filled at its first fetch.
#[tokio::test(start_paused = true)]
async fn a_connected_cloudflare_login_settles_its_account_or_says_what_it_still_needs() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let resources = temp.path().join("res");
    cloudflare_copy(&resources, "wp-cf");
    oauth_store::save(
        &oauth::account("wp-cf", "browser-run"),
        &stored_auth("cf-Qm71Tz"),
    )
    .unwrap();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let needs_account = Some(NotReady::NeedsSetup {
        missing: vec!["Cloudflare account".into()],
    });

    let config = temp.path().join("home");
    install(&resources, &config, "wp-cf");
    settings(&config, "activeWebFetch: wp-cf\n");
    for stand_in in [
        listing(&seen, || {
            answered(
                403,
                r#"{"success":false,"errors":[{"message":"Authentication error"}]}"#,
            )
        }),
        listing(&seen, || answered(200, "<html>not json</html>")),
    ] {
        assert_eq!(
            connect_settles(&config, "wp-cf", stand_in).await,
            needs_account
        );
    }

    let started = tokio::time::Instant::now();
    let stalled = listing(&seen, || Box::pin(std::future::pending()));
    assert_eq!(
        connect_settles(&config, "wp-cf", stalled.clone()).await,
        needs_account
    );
    let waited = started.elapsed();
    assert!(
        waited >= choices::LIST_DEADLINE && waited < choices::CONNECT_SETTLE_DEADLINE,
        "{waited:?}"
    );
    let mut active = provider_of(resolve(&config, Slot::WebFetch));
    let gap = choices::test_listing::LISTING
        .scope(stalled, choices::fill_required(&config, &mut active))
        .await;
    let choices::Gap::Failed { error, .. } = &gap else {
        panic!("a listing that never answers is a failure, got {gap:?}");
    };
    assert!(error.contains("no answer within 10 seconds"), "{error}");

    let one = || {
        answered(
            200,
            r#"{"success":true,"result":[{"id":"acct-77","name":"Acme"}]}"#,
        )
    };
    assert_eq!(
        connect_settles(&config, "wp-cf", listing(&seen, one)).await,
        None
    );
    let request = seen.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        (request.method.as_str(), request.url.as_str()),
        (
            "GET",
            "https://api.cloudflare.com/client/v4/accounts?per_page=50"
        )
    );
    assert!(request
        .headers
        .contains(&("Authorization".into(), "Bearer cf-Qm71Tz".into())));
    let fetch = provider_of(resolve(&config, Slot::WebFetch));
    let account_url =
        "https://api.cloudflare.com/client/v4/accounts/acct-77/browser-rendering/markdown?";
    let prepared = prepare(&fetch, Some("https://example.com/"), "cf-Qm71Tz");
    assert!(prepared.url.starts_with(account_url), "{}", prepared.url);

    // A login connected before the option existed: the first fetch fills it.
    let earlier = temp.path().join("earlier");
    install(&resources, &earlier, "wp-cf");
    settings(&earlier, "activeWebFetch: wp-cf\n");
    let mut active = provider_of(resolve(&earlier, Slot::WebFetch));
    let gap = choices::test_listing::LISTING
        .scope(
            listing(&seen, one),
            choices::fill_required(&earlier, &mut active),
        )
        .await;
    assert_eq!(gap, choices::Gap::Ready);
    let prepared = prepare(&active, Some("https://example.com/"), "cf-Qm71Tz");
    assert!(prepared.url.starts_with(account_url), "{}", prepared.url);
}

/// Reading a pack credential's value is a security decision: each site is a
/// place the value leaves the keychain. Today the two slots read it to send
/// their request, and the choice listing reads it to send its own.
#[test]
fn a_pack_credential_value_is_read_only_by_the_slots_and_their_choices() {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    walk(&src, &mut files);
    let mut found: Vec<String> = files
        .iter()
        .filter(|path| !path.ends_with("web_providers/tests.rs"))
        .filter(|path| {
            std::fs::read_to_string(path).unwrap().lines().any(|line| {
                line.contains("credential_value(") && !line.contains("fn credential_value(")
            })
        })
        .map(|path| {
            path.strip_prefix(&src)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect();
    found.sort();
    assert_eq!(
        found,
        [
            "mcp/handlers/fetch_web.rs",
            "mcp/handlers/search_web.rs",
            "web_providers/choices.rs",
        ],
        "the set of places that read a pack credential's value changed. Each is a place the \
         value leaves the keychain, so adding one is a security decision: confirm it sends the \
         value only through `web_providers::send` and never into a response, a log line or a \
         spawn environment, then update this list."
    );
}

/// Read `target` the way `read` does for an `http(s)://` target.
async fn read_web(
    orch: &crate::orchestrator::Orchestrator,
    target: &str,
) -> Result<String, String> {
    let request = crate::mcp::types::McpCallbackRequest {
        tool: "read_web".to_string(),
        payload: serde_json::json!({ "target": target }),
        ..read_request()
    };
    crate::mcp::handlers::web::read_web_markdown(orch, &request).await
}

/// A workspace whose `activePdf: bmd` the migration has carried onto the
/// bundled bmd pack, with a recording gateway installed on its orchestrator.
async fn bmd_pdf_workspace(
    temp: &TempDir,
) -> (
    PathBuf,
    crate::orchestrator::Orchestrator,
    std::sync::Arc<MockGateway>,
) {
    let config = temp.path().join("home");
    settings(&config, "activePdf: bmd\n");
    let report = migrate_workspace(&RealGitClient, &RealFileSystem, &config, &app_resources());
    assert!(report.installed.contains(&"bmd".to_string()), "{report:?}");
    let orch = orchestrator(&config).await;
    let gateway = std::sync::Arc::new(MockGateway::default());
    assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
    (config, orch, gateway)
}

/// A remote PDF read with `activePdf: bmd` goes through the bmd pack's binding
/// on the orchestrator's (sanitizing) gateway, and the reader gets exactly what
/// the compiled bmd provider returned for the same call. A `bmd` server the
/// user authored in settings is the one connected, not the pack's.
#[tokio::test]
async fn a_remote_pdf_read_goes_through_the_bmd_pack_and_returns_its_text_verbatim() {
    use crate::config::mcp_servers::McpServerConfigExt;
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let (config, orch, gateway) = bmd_pdf_workspace(&temp).await;
    settings(
        &config,
        "activePdf: bmd\nmcpServers:\n  bmd:\n    type: http\n    url: https://bmd.workspace.test/mcp\n",
    );
    let url = "https://example.com/paper.pdf?rev=2";

    let text = read_web(&orch, url).await.unwrap();

    let (session, tool, args, timeout) = gateway.last.lock().unwrap().clone().unwrap();
    assert_eq!(session, "cairn-pdf");
    assert_eq!(tool, "fetch");
    assert_eq!(args, serde_json::json!({ "source": url }));
    assert_eq!(timeout, Some(120_000));
    assert_eq!(
        gateway.endpoint.lock().unwrap().clone(),
        Some((
            "bmd".to_string(),
            Some("https://bmd.workspace.test/mcp".to_string())
        )),
        "the settings server shadows the pack's https://api.become.md/mcp"
    );

    // The compiled provider's call: the `bmd` key, session `cairn-pdf`, tool
    // `fetch` with `{source}`, 120 s, and `result.text`, through the same
    // installed gateway, so sanitization applies to both sides alike.
    let authored: crate::config::mcp_servers::McpServerConfig = serde_json::from_value(
        serde_json::json!({ "type": "http", "url": "https://bmd.workspace.test/mcp" }),
    )
    .unwrap();
    let legacy = orch
        .mcp_gateway()
        .unwrap()
        .call_tool(
            "cairn-pdf",
            "bmd",
            &authored.brokered("bmd", "bmd PDF").unwrap(),
            "fetch",
            serde_json::json!({ "source": url }),
            Some(120_000),
        )
        .await
        .unwrap()
        .text;
    assert_eq!(text, legacy);
    assert_eq!(text, MOCK_TEXT);
}

/// The pack's own bmd server needs OAuth, so until it is connected a PDF read
/// is guidance naming the server, and nothing reaches the gateway.
#[tokio::test]
async fn a_remote_pdf_read_with_bmd_unconnected_is_guidance() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let (_config, orch, gateway) = bmd_pdf_workspace(&temp).await;

    let guidance = read_web(&orch, "https://example.com/paper.pdf")
        .await
        .unwrap();
    assert_eq!(
        guidance,
        format!(
            "bmd PDF extraction needs its MCP server `bmd` connected. Connect it in {LIBRARY_LOCATION}."
        )
    );
    assert!(gateway.last.lock().unwrap().is_none());
}

/// A selected pack that is not installed is guidance, not a quiet fall back to
/// the local extractor: a lost installation stays visible. The `.test` host
/// cannot resolve, so a local download would have been an `Err`.
#[tokio::test]
async fn a_remote_pdf_read_with_its_pack_missing_is_guidance_not_local() {
    mock_keychain::install();
    let temp = TempDir::new().unwrap();
    let config = temp.path().join("home");
    settings(&config, "activePdf: wp-pdf-gone\n");
    let orch = orchestrator(&config).await;
    let gateway = std::sync::Arc::new(MockGateway::default());
    assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());

    let guidance = read_web(&orch, "https://pdf.wp-pdf-gone.test/paper.pdf")
        .await
        .unwrap();
    assert_eq!(
        guidance,
        format!(
            "The PDF provider `wp-pdf-gone` is selected but not installed. Install it from \
             {LIBRARY_LOCATION}, or select Local there."
        )
    );
    assert!(gateway.last.lock().unwrap().is_none());
}
