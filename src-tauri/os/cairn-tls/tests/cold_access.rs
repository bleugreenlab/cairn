//! A synchronous accessor reached before the root store has loaded says so,
//! naming its caller, because that thread now waits out the whole load. Its own
//! test binary, so nothing has loaded the store yet.

use std::sync::Mutex;

static WARNINGS: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct Capture;

impl log::Log for Capture {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Warn
    }
    fn log(&self, record: &log::Record) {
        if record.level() == log::Level::Warn {
            WARNINGS.lock().unwrap().push(record.args().to_string());
        }
    }
    fn flush(&self) {}
}

#[test]
fn a_cold_synchronous_access_warns_with_its_caller_and_a_warm_one_is_silent() {
    log::set_logger(&Capture).unwrap();
    log::set_max_level(log::LevelFilter::Warn);

    let _shared = cairn_tls::client();
    let warnings = WARNINGS.lock().unwrap().clone();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(
        warnings[0].contains("tests/cold_access.rs"),
        "the warning names the external caller, not a line in cairn-tls: {}",
        warnings[0]
    );

    let _config = cairn_tls::http_config();
    let _builder = cairn_tls::client_builder();
    assert_eq!(
        WARNINGS.lock().unwrap().len(),
        1,
        "warm accesses are silent"
    );
    assert_eq!(cairn_tls::root_store_loads(), 1);
}
