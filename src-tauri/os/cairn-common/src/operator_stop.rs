//! A person's durable request that the local orchestrator stay stopped.
//!
//! The orchestrator is supervised as always-on from every direction: launchd's
//! `RunAtLoad` + `KeepAlive`, a systemd unit wanted by `default.target`, a
//! Windows Run value read at logon, and a desktop whose startup repair reinstalls
//! any service that is not answering. Each of those reads "not running" as a
//! fault to correct, so a stop that lives only in the service manager's
//! transient state is undone by the next app launch or login, and the work it
//! was meant to halt resumes silently.
//!
//! This marker is the one record that a person asked for the stop. It lives in
//! the Cairn home beside the database it protects, is written BEFORE the service
//! is unloaded (so a stop interrupted halfway still leaves what the operator
//! asked for), and is removed only by an explicit start. Every path that can
//! start the orchestrator consults it: `cairn-orch install` and `restart`
//! refuse, the daemon re-applies the stop when something launches it anyway, the
//! desktop's repair paths leave a stopped orchestrator alone and offer Start, and
//! the `cairn` CLI names the stop instead of reporting a connection failure.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// The marker's file name inside the Cairn home.
pub const MARKER_FILENAME: &str = "orch-stopped.json";

/// How a person lifts the stop, named in every refusal.
pub const HOW_TO_START: &str = "choose Start Cairn in the Cairn app, or run `cairn-orch start`";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorStop {
    /// When the stop was requested, in Unix milliseconds. Zero when the marker
    /// exists but its contents could not be read.
    pub at_unix_ms: u64,
    /// The surface the person used, as a phrase that completes "stopped it
    /// via ...", e.g. "the Cairn app's quit dialog".
    pub via: String,
}

impl OperatorStop {
    pub fn now(via: impl Into<String>) -> Self {
        Self {
            at_unix_ms: unix_time_ms(),
            via: via.into(),
        }
    }

    /// One sentence for a refusal: what state the orchestrator is in, who put
    /// it there and when, and who can lift it.
    pub fn refusal(&self, now_unix_ms: u64) -> String {
        format!(
            "The Cairn orchestrator is stopped: a person stopped it {} via {}. Nothing starts it again until someone does so explicitly: {HOW_TO_START}.",
            self.age(now_unix_ms),
            self.via,
        )
    }

    fn age(&self, now_unix_ms: u64) -> String {
        if self.at_unix_ms == 0 {
            return "at an unrecorded time".to_string();
        }
        let seconds = now_unix_ms.saturating_sub(self.at_unix_ms) / 1_000;
        let (count, unit) = match seconds {
            0..=59 => return "just now".to_string(),
            60..=3_599 => (seconds / 60, "minute"),
            3_600..=172_799 => (seconds / 3_600, "hour"),
            _ => (seconds / 86_400, "day"),
        };
        let plural = if count == 1 { "" } else { "s" };
        format!("{count} {unit}{plural} ago")
    }
}

pub fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

pub fn marker_path(data_dir: &Path) -> PathBuf {
    data_dir.join(MARKER_FILENAME)
}

/// Record the stop. Written to a sibling file and renamed into place, so a
/// reader never sees a half-written marker.
pub fn record(data_dir: &Path, stop: &OperatorStop) -> Result<(), String> {
    let path = marker_path(data_dir);
    let staging = data_dir.join(format!("{MARKER_FILENAME}.tmp"));
    let encoded = serde_json::to_vec_pretty(stop)
        .map_err(|error| format!("encode operator stop: {error}"))?;
    std::fs::create_dir_all(data_dir)
        .map_err(|error| format!("create {}: {error}", data_dir.display()))?;
    std::fs::write(&staging, encoded)
        .map_err(|error| format!("write {}: {error}", staging.display()))?;
    std::fs::rename(&staging, &path).map_err(|error| format!("record {}: {error}", path.display()))
}

/// The standing stop, if any.
///
/// Fails closed: a marker that exists but cannot be read or parsed still means
/// a person asked for the stop, so it reads as stopped at an unrecorded time.
/// Treating it as absent would let a corrupt write resurrect the orchestrator,
/// which is the one outcome this marker exists to prevent.
pub fn read(data_dir: &Path) -> Option<OperatorStop> {
    let path = marker_path(data_dir);
    match std::fs::read(&path) {
        Ok(bytes) => Some(
            serde_json::from_slice::<OperatorStop>(&bytes).unwrap_or_else(|_| OperatorStop {
                at_unix_ms: 0,
                via: format!("a stop record at {} that could not be read", path.display()),
            }),
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => Some(OperatorStop {
            at_unix_ms: 0,
            via: format!("a stop record at {} that could not be read", path.display()),
        }),
    }
}

/// Lift the stop. Only an explicit start calls this.
pub fn clear(data_dir: &Path) -> Result<(), String> {
    let path = marker_path(data_dir);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("remove {}: {error}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_recorded_stop_stands_until_it_is_cleared() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read(dir.path()), None);

        let stop = OperatorStop::now("the Cairn app's quit dialog");
        record(dir.path(), &stop).unwrap();
        assert_eq!(read(dir.path()), Some(stop.clone()));
        // Reading never consumes it: every start path must see the same stop.
        assert_eq!(read(dir.path()), Some(stop));

        clear(dir.path()).unwrap();
        assert_eq!(read(dir.path()), None);
        clear(dir.path()).expect("clearing an absent stop is a no-op");
    }

    /// A corrupt marker must not resurrect the orchestrator.
    #[test]
    fn an_unreadable_marker_still_reads_as_stopped() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(marker_path(dir.path()), b"{ not json").unwrap();

        let stop = read(dir.path()).expect("a present marker is a stop");
        assert_eq!(stop.at_unix_ms, 0);
        assert!(stop
            .refusal(unix_time_ms())
            .contains("at an unrecorded time"));
    }

    #[test]
    fn the_refusal_names_the_state_who_set_it_and_how_to_lift_it() {
        let stop = OperatorStop {
            at_unix_ms: 1_000_000,
            via: "the Cairn app's quit dialog".to_string(),
        };
        let refusal = stop.refusal(1_000_000 + 2 * 3_600_000 + 5_000);
        assert!(refusal.contains("is stopped"), "{refusal}");
        assert!(
            refusal.contains("a person stopped it 2 hours ago"),
            "{refusal}"
        );
        assert!(
            refusal.contains("via the Cairn app's quit dialog"),
            "{refusal}"
        );
        assert!(refusal.contains("cairn-orch start"), "{refusal}");
    }

    #[test]
    fn ages_read_naturally() {
        let stop = |seconds_ago: u64| {
            OperatorStop {
                at_unix_ms: 10_000_000_000,
                via: String::new(),
            }
            .age(10_000_000_000 + seconds_ago * 1_000)
        };
        assert_eq!(stop(5), "just now");
        assert_eq!(stop(60), "1 minute ago");
        assert_eq!(stop(59 * 60), "59 minutes ago");
        assert_eq!(stop(3_600), "1 hour ago");
        assert_eq!(stop(47 * 3_600), "47 hours ago");
        assert_eq!(stop(3 * 86_400), "3 days ago");
    }
}
