//! The process's one TLS client trust configuration.
//!
//! Every outbound TLS connection Cairn makes (async and blocking reqwest
//! clients and WebSocket dials) verifies servers against the platform's own
//! root store. Loading that store is a platform call, not a file read: on
//! macOS `rustls_native_certs::load_native_certs` is
//! `SecTrustSettingsCopyCertificates`, an XPC round trip to `trustd` per trust
//! domain, and on a loaded host `trustd` answers in seconds to minutes. reqwest
//! and tokio-tungstenite each load the store again on every client build or
//! TLS dial unless they are handed a finished config, and when that build runs
//! inside an async task, the runtime worker polling it is gone for as long as
//! `trustd` takes. Enough of them at once starve the runtime.
//!
//! So the store is loaded exactly once per process, here, and every client is
//! built on configs derived from it:
//!
//! - [`client`]: the shared async client, with no client-level timeout (set
//!   `RequestBuilder::timeout` per request). Use it unless a setting only a
//!   builder has (redirect policy, connect timeout, user agent) is needed.
//! - [`client_builder`] and [`blocking_client_builder`]: a reqwest builder that
//!   already carries the shared config. A client built from one should itself
//!   be kept, not rebuilt per call: a build still reads the system proxy
//!   configuration and, for a blocking client, starts a thread.
//! - [`websocket_config`]: the config for tungstenite's `Connector::Rustls`.
//!
//! Hosts call [`preload`] at startup, which loads the store on its own
//! `cairn-tls-roots` thread. Async code that may run before that finishes
//! awaits [`ready`], which waits for the loader without occupying the worker.
//! A synchronous accessor reached before the load completes waits for it (or
//! performs it, if nothing started one); either way the store is loaded once.

use std::panic::Location;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Once, OnceLock};
use std::time::{Duration, Instant};

use rustls::{ClientConfig, RootCertStore};
use tokio::sync::Notify;

/// The thread [`preload`] loads the store on, so a process sample names it.
pub const LOADER_THREAD: &str = "cairn-tls-roots";

struct Trust {
    roots: Arc<RootCertStore>,
    http: ClientConfig,
    websocket: Arc<ClientConfig>,
    report: LoadReport,
}

/// How the process's one root-store load went.
#[derive(Debug, Clone)]
pub struct LoadReport {
    /// The thread that performed the load, when it was named.
    pub thread: Option<String>,
    /// Root certificates accepted into the store.
    pub certificates: usize,
    /// How long the platform took to answer.
    pub elapsed: Duration,
}

static TRUST: OnceLock<Trust> = OnceLock::new();
static LOADS: AtomicUsize = AtomicUsize::new(0);
static LOADER: Once = Once::new();
static LOADED: Notify = Notify::const_new();
static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

/// The loaded trust state, loading or waiting for it if it is not there yet.
///
/// A synchronous accessor that arrives before the load completes holds its
/// thread for the rest of the load: on a runtime worker, that is the stall
/// this crate exists to remove. Each such arrival logs its caller once, so
/// the first boot's log names any path that can still reach the store cold.
fn trust_for(caller: &'static Location<'static>) -> &'static Trust {
    if let Some(trust) = TRUST.get() {
        return trust;
    }
    let thread = std::thread::current();
    if thread.name() != Some(LOADER_THREAD) {
        log::warn!(
            "{caller} reached the TLS trust config on thread {} before the platform root store finished loading; the thread waits for the load (await cairn_tls::ready() first)",
            thread.name().unwrap_or("<unnamed>")
        );
    }
    TRUST.get_or_init(load)
}

fn load() -> Trust {
    LOADS.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let loaded = rustls_native_certs::load_native_certs();
    if !loaded.errors.is_empty() {
        log::warn!(
            "some platform root certificates could not be loaded: {:?}",
            loaded.errors
        );
    }
    let mut roots = RootCertStore::empty();
    let (certificates, unparsable) = roots.add_parsable_certificates(loaded.certs);
    let elapsed = started.elapsed();
    if certificates == 0 {
        log::error!(
            "the platform root store yielded no usable certificates, so no TLS server can be verified"
        );
    } else {
        log::info!(
            "loaded {certificates} platform root certificates in {} ms ({unparsable} unparsable)",
            elapsed.as_millis()
        );
    }
    let roots = Arc::new(roots);
    Trust {
        // reqwest advertises exactly this pair when it builds its own config
        // with HTTP/2 compiled in; a preconfigured config must carry it itself.
        http: config(&roots, vec![b"h2".to_vec(), b"http/1.1".to_vec()]),
        // A WebSocket upgrade is HTTP/1.1 only: offering h2 would let a server
        // pick a protocol tungstenite cannot speak.
        websocket: Arc::new(config(&roots, Vec::new())),
        roots,
        report: LoadReport {
            thread: std::thread::current().name().map(str::to_owned),
            certificates,
            elapsed,
        },
    }
}

fn config(roots: &Arc<RootCertStore>, alpn: Vec<Vec<u8>>) -> ClientConfig {
    let mut config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("ring supports rustls' default protocol versions")
            .with_root_certificates(Arc::clone(roots))
            .with_no_client_auth();
    config.alpn_protocols = alpn;
    config
}

/// Start loading the root store on the [`LOADER_THREAD`], once per process.
/// Returns at once; later calls do nothing.
pub fn preload() {
    LOADER.call_once(|| {
        let spawned = std::thread::Builder::new()
            .name(LOADER_THREAD.to_owned())
            .spawn(|| {
                trust_for(Location::caller());
                LOADED.notify_waiters();
            });
        if let Err(error) = spawned {
            // Without the thread nothing would ever wake `ready()`, so load
            // here instead: slower for this caller, never a hang.
            log::warn!("could not start the {LOADER_THREAD} thread ({error}); loading inline");
            trust_for(Location::caller());
            LOADED.notify_waiters();
        }
    });
}

/// Resolve once the root store is loaded, starting the load if nothing has.
/// The waiting task yields; the load itself runs on the [`LOADER_THREAD`].
/// Call this before the first client use in async code that can run early.
pub async fn ready() {
    loop {
        // Register before checking, so a load that finishes in between is
        // still seen as a notification.
        let mut loaded = pin!(LOADED.notified());
        loaded.as_mut().enable();
        if TRUST.get().is_some() {
            return;
        }
        preload();
        loaded.await;
    }
}

/// The platform root store.
#[track_caller]
pub fn root_store() -> Arc<RootCertStore> {
    Arc::clone(&trust_for(Location::caller()).roots)
}

/// The client config for HTTP: the platform roots, offering h2 and http/1.1.
/// reqwest takes its config by value, so this is a copy sharing the roots.
#[track_caller]
pub fn http_config() -> ClientConfig {
    trust_for(Location::caller()).http.clone()
}

/// The client config for a `wss://` dial: the platform roots, no ALPN.
#[track_caller]
pub fn websocket_config() -> Arc<ClientConfig> {
    Arc::clone(&trust_for(Location::caller()).websocket)
}

/// An async reqwest builder that carries [`http_config`].
#[track_caller]
pub fn client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder().use_preconfigured_tls(http_config())
}

/// A blocking reqwest builder that carries [`http_config`].
#[track_caller]
pub fn blocking_client_builder() -> reqwest::blocking::ClientBuilder {
    reqwest::blocking::Client::builder().use_preconfigured_tls(http_config())
}

/// The process's shared async client: reqwest's defaults on [`http_config`],
/// with no client-level timeout. A `Client` is a handle onto one connection
/// pool; clone it freely.
///
/// # Panics
///
/// If reqwest cannot build a client at all, as `reqwest::Client::new` does.
#[track_caller]
pub fn client() -> &'static reqwest::Client {
    let caller = Location::caller();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .use_preconfigured_tls(trust_for(caller).http.clone())
            .build()
            .expect("reqwest builds a client on the shared TLS config")
    })
}

/// The load's report, once it has happened.
pub fn load_report() -> Option<&'static LoadReport> {
    TRUST.get().map(|trust| &trust.report)
}

/// How many times this process has loaded the platform root store: at most
/// one, which is the point.
pub fn root_store_loads() -> usize {
    LOADS.load(Ordering::Relaxed)
}
