//! Export one project from a Cairn database file, outside the running app.
//!
//! The same export `write cairn://p/PROJECT {action:"export", path}` performs
//! (`cairn_core::projects::export`), driven against a database file instead of
//! the live instance. Use it when the app is quit, or against a copy-on-write
//! clone of the live file set (`cp -c cairn.db cairn.db-log cairn.db-wal DIR/`
//! on APFS), which costs no space and never opens the live file from a second
//! process.
//!
//! Private projects only: a team project's rows live in its team replica, which
//! only the running app has open and routed.
//!
//! Usage:
//!   cargo run -p cairn-core --example export_project --features internal-api -- \
//!     <path-to-cairn.db> <project-key> <absolute-destination-dir>

use std::path::PathBuf;
use std::sync::Arc;

use cairn_core::internal::mcp::handlers::read::register_archived_file_renderer;
use cairn_core::internal::storage::LocalDb;
use cairn_core::projects::export::export_project;

/// The export reports progress per table through `log`; print it.
struct Stderr;

impl log::Log for Stderr {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Info
    }
    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("{} {}", record.level(), record.args());
        }
    }
    fn flush(&self) {}
}

#[tokio::main]
async fn main() {
    let _ = log::set_logger(&Stderr).map(|()| log::set_max_level(log::LevelFilter::Info));
    let args: Vec<String> = std::env::args().collect();
    let [_, db_path, project_key, destination] = args.as_slice() else {
        eprintln!(
            "usage: export_project <path-to-cairn.db> <project-key> <absolute-destination-dir>"
        );
        std::process::exit(2);
    };
    // Without the renderer every archived read reconstructs to a coordinate
    // stub; the app registers it at startup, so this must too.
    register_archived_file_renderer();
    let db = match LocalDb::open(db_path).await {
        Ok(db) => Arc::new(db),
        Err(error) => {
            eprintln!("cannot open {db_path}: {error}");
            std::process::exit(1);
        }
    };
    match export_project(
        db.clone(),
        db,
        project_key.clone(),
        PathBuf::from(destination),
    )
    .await
    {
        Ok(report) => println!("{}", report.summary(project_key)),
        Err(error) => {
            eprintln!("export failed: {error}");
            std::process::exit(1);
        }
    }
}
