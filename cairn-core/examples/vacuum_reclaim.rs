//! Offline archival reclamation (CAIRN-1556): shrink a Cairn Turso database by
//! returning freelist pages to the OS via `VACUUM INTO`.
//!
//! The archival backfill (CAIRN-1555) compresses historical event data and frees
//! pages onto the freelist, but the database *file* never shrinks on its own:
//! freed pages are reused for new writes, so the file plateaus at its
//! high-water mark. This is a one-time, offline cleanup of that high-water mark.
//!
//! Why offline rather than in-app: with the Cairn app fully quit the database is
//! quiescent, so there is no write-loss window and no need for a crash-safe
//! startup swap. The operator quitting the app is the quiescence guarantee; a
//! retained backup is the recovery mechanism.
//!
//! What it does, all while the app is closed:
//!   1. `VACUUM INTO` a staged compacted image (no checkpoint — in-place VACUUM
//!      corrupts the migrated MVCC schema; see docs/database.md).
//!   2. Open the staged image and require `PRAGMA integrity_check` = `ok`.
//!   3. Move the live three-file set aside to a `.vacuum-backup` set.
//!   4. Move the staged set into the live location.
//!
//! The original is preserved as the backup until you confirm a clean app open;
//! nothing is deleted automatically. If the process dies mid-move, inspect the
//! `.vacuum-staged` / `.vacuum-backup` / live sets and restore the backup.
//!
//! Usage (the app MUST be fully quit first):
//!   cargo run -p cairn-core --example vacuum_reclaim --features internal-api -- <path-to-cairn.turso.db>
//!
//! Prod DB path:
//!   ~/Library/Application Support/com.cairn.desktop/cairn.turso.db
//! Dev DB path:
//!   ~/Library/Application Support/com.cairn.desktop/cairn-dev.turso.db
//!
//! Exit code 0 = reclaimed; non-zero = aborted (live DB left untouched).

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Instant;

use cairn_core::internal::storage::{db_set_paths, db_set_size, move_db_set, LocalDb, RowExt};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let Some(db_arg) = args.get(1) else {
        eprintln!("usage: vacuum_reclaim <path-to-cairn.turso.db>");
        eprintln!("  Quit the Cairn app first — the database must be quiescent.");
        std::process::exit(2);
    };

    match run(Path::new(db_arg)).await {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("ABORTED: {e}");
            std::process::exit(1);
        }
    }
}

async fn run(live: &Path) -> Result<(), String> {
    let live = live.to_path_buf();
    let staged = with_suffix(&live, ".vacuum-staged");
    let backup = with_suffix(&live, ".vacuum-backup");

    if !live.exists() {
        return Err(format!("live database not found: {}", live.display()));
    }

    // A leftover staged or backup set means a prior run was interrupted. Refuse
    // rather than guess which state is authoritative.
    if let Some(member) = first_existing_member(&staged) {
        return Err(format!(
            "a staged set already exists ({}); a prior run may have been interrupted. \
             Inspect and remove {}* before retrying.",
            member.display(),
            staged.display()
        ));
    }
    if let Some(member) = first_existing_member(&backup) {
        return Err(format!(
            "a backup set already exists ({}); a prior run may have been interrupted. \
             If the live DB opens cleanly, remove {}*; otherwise restore it over the live set first.",
            member.display(),
            backup.display()
        ));
    }

    let before = db_set_size(&live);
    println!("live set:  {}  ({})", live.display(), human(before));

    let start = Instant::now();
    println!("VACUUM INTO {} ...", staged.display());
    {
        let db = LocalDb::open(&live)
            .await
            .map_err(|e| format!("open live database: {e}"))?;
        db.vacuum_into(&staged)
            .await
            .map_err(|e| format!("VACUUM INTO: {e}"))?;
    }

    // Validate the compacted image before touching the live set.
    {
        let staged_db = LocalDb::open(&staged)
            .await
            .map_err(|e| format!("open staged image: {e}"))?;
        let result = integrity_check(&staged_db)
            .await
            .map_err(|e| format!("integrity_check: {e}"))?;
        if result != "ok" {
            // Leave the live set untouched; clean up the staged image we created.
            remove_db_set(&staged);
            return Err(format!(
                "staged integrity_check returned {result:?}; live database left untouched"
            ));
        }
    }
    println!("staged integrity_check: ok");
    let after = db_set_size(&staged);

    // Swap: live -> backup, then staged -> live. Each moves the whole set.
    move_db_set(&live, &backup).map_err(|e| format!("move live -> backup: {e}"))?;
    move_db_set(&staged, &live).map_err(|e| {
        format!(
            "move staged -> live: {e}. The original is preserved at {}*; \
             restore it over {} to recover.",
            backup.display(),
            live.display()
        )
    })?;

    let freed = before.saturating_sub(after);
    println!();
    println!("reclaimed in {:.1}s", start.elapsed().as_secs_f64());
    println!("  before: {}", human(before));
    println!("  after:  {}", human(after));
    println!("  freed:  {} ({:.1}%)", human(freed), percent(freed, before));
    println!();
    println!("The original three-file set is preserved at {}*", backup.display());
    println!("Open the Cairn app. If it works normally, delete the backup:");
    println!("  rm {}*", backup.display());
    println!("If it does NOT open, restore the backup over the live set:");
    for (b, l) in db_set_paths(&backup).iter().zip(db_set_paths(&live).iter()) {
        println!("  mv {} {}", b.display(), l.display());
    }
    Ok(())
}

/// First member of `base`'s three-file set that exists on disk, if any.
fn first_existing_member(base: &Path) -> Option<PathBuf> {
    db_set_paths(base).into_iter().find(|p| p.exists())
}

/// Remove every member of `base`'s three-file set that exists. Best-effort:
/// used only to clean up a staged image we just created when validation fails.
fn remove_db_set(base: &Path) {
    for member in db_set_paths(base) {
        let _ = std::fs::remove_file(member);
    }
}

/// Append `suffix` to the file name of `base` (e.g. `<db>` -> `<db>.vacuum-staged`).
fn with_suffix(base: &Path, suffix: &str) -> PathBuf {
    let mut name = base.as_os_str().to_owned();
    name.push(OsString::from(suffix));
    PathBuf::from(name)
}

async fn integrity_check(db: &LocalDb) -> Result<String, String> {
    let conn = db.connect().await.map_err(|e| e.to_string())?;
    let mut rows = conn
        .query("PRAGMA integrity_check", ())
        .await
        .map_err(|e| e.to_string())?;
    let row = rows
        .next()
        .await
        .map_err(|e| e.to_string())?
        .ok_or("integrity_check returned no rows")?;
    row.text(0).map_err(|e| e.to_string())
}

fn human(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

fn percent(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        (part as f64 / whole as f64) * 100.0
    }
}
