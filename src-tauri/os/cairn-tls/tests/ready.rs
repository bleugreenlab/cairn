//! `ready()` loads the store on the loader thread, never on the runtime
//! worker awaiting it. Its own test binary, so nothing has loaded it yet.

#[test]
fn ready_loads_on_the_loader_thread_not_a_runtime_worker() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("tokio-runtime-worker")
        .build()
        .unwrap();
    assert!(cairn_tls::load_report().is_none());
    runtime.block_on(async {
        tokio::spawn(cairn_tls::ready()).await.unwrap();
    });
    let report = cairn_tls::load_report().expect("ready() resolves after the load");
    assert_eq!(report.thread.as_deref(), Some(cairn_tls::LOADER_THREAD));
    assert_eq!(cairn_tls::root_store_loads(), 1);

    // Already loaded: resolves at once, and loads nothing again.
    runtime.block_on(cairn_tls::ready());
    assert_eq!(cairn_tls::root_store_loads(), 1);
}
