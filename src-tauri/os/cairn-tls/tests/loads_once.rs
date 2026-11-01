//! However many clients are built, of whatever kind, the platform root store
//! is loaded once. Its own test binary, so the count starts at zero.

#[test]
fn every_client_kind_shares_one_root_store_load() {
    let first = cairn_tls::client_builder().build().unwrap();
    let second = cairn_tls::client_builder().build().unwrap();
    let blocking = cairn_tls::blocking_client_builder().build().unwrap();
    let shared = cairn_tls::client();
    assert!(std::ptr::eq(shared, cairn_tls::client()));
    let websocket = cairn_tls::websocket_config();
    let http = cairn_tls::http_config();
    drop((first, second, blocking));

    assert_eq!(cairn_tls::root_store_loads(), 1);
    assert!(std::sync::Arc::ptr_eq(
        &websocket,
        &cairn_tls::websocket_config()
    ));
    assert_eq!(
        http.alpn_protocols,
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    );
    assert!(websocket.alpn_protocols.is_empty());
}
