//! The serde vocabulary that cairn-common's base modules and the executor wire
//! both speak. It imports nothing from this crate. Every item is part of the
//! executor protocol, and the executor-protocol check watches this file. It
//! holds vocabulary only (types, constants, and `describe_executor_bound`);
//! behaviour does not move here without the confirmation of the execution and
//! devops threads.

use serde::{Deserialize, Serialize};

/// Environment names read directly by the independently installed executor.
/// Runner launch and probe builders use these same constants so one side
/// cannot silently rename an input without the other.
pub const EXECUTOR_RUNNER_URL_ENV: &str = "CAIRN_EXECUTOR_RUNNER_URL";
pub const EXECUTOR_AUTH_TOKEN_ENV: &str = "CAIRN_MCP_AUTH_TOKEN";
pub const EXECUTOR_CAIRN_HOME_ENV: &str = "CAIRN_HOME";
pub const EXECUTOR_ID_ENV: &str = "CAIRN_EXECUTOR_ID";
pub const EXECUTOR_DEVICE_ID_ENV: &str = "CAIRN_EXECUTOR_DEVICE_ID";
pub const EXECUTOR_DISPLAY_NAME_ENV: &str = "CAIRN_EXECUTOR_DISPLAY_NAME";
pub const EXECUTOR_PROJECTS_ENV: &str = "CAIRN_EXECUTOR_PROJECTS";
pub const EXECUTOR_ENROLLMENT_TOKEN_ENV: &str = "CAIRN_EXECUTOR_ENROLLMENT_TOKEN";
pub const EXECUTOR_RUNNER_DEVICE_ID_ENV: &str = "CAIRN_EXECUTOR_RUNNER_DEVICE_ID";
/// The value a supervisor hands one launch of an executor, so that launch can
/// later be told apart from any other process running the same binary. A
/// colocated executor presents it in its enrollment; the runner admits only the
/// one it last issued.
pub const EXECUTOR_SUPERVISION_NONCE_ENV: &str = "CAIRN_EXECUTOR_SUPERVISION_NONCE";

/// The bounds a service manager enforces on an executor, as the kernel
/// reports them. Each is `None` when it is unbounded or could not be read.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceBounds {
    /// The unit cgroup's `memory.max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_max_bytes: Option<u64>,
    /// Its `memory.swap.max`; zero means the unit never swaps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swap_max_bytes: Option<u64>,
    /// Its `pids.max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tasks_max: Option<u64>,
    /// The watchdog interval systemd armed for the daemon.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watchdog_secs: Option<u64>,
    /// The machine's own memory, which the memory bound sits below. The
    /// machine reading says the same where it is the host's; this one is the
    /// host's whatever that reading measures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_memory_bytes: Option<u64>,
}

impl ServiceBounds {
    /// Whether any bound was read. The host's memory is context, not a bound.
    pub fn is_empty(&self) -> bool {
        self.memory_max_bytes.is_none()
            && self.swap_max_bytes.is_none()
            && self.tasks_max.is_none()
            && self.watchdog_secs.is_none()
    }

    /// `memory 11.25 GiB of 15.4 GiB (swap 0), tasks 18714, watchdog 180 s`:
    /// each bound that was read.
    pub fn describe(&self) -> String {
        const GIB: f64 = (1u64 << 30) as f64;
        let mut parts = Vec::new();
        if let Some(max) = self.memory_max_bytes {
            let mut memory = format!("memory {:.2} GiB", max as f64 / GIB);
            if let Some(host) = self.host_memory_bytes {
                memory.push_str(&format!(" of {:.1} GiB", host as f64 / GIB));
            }
            match self.swap_max_bytes {
                Some(0) => memory.push_str(" (swap 0)"),
                Some(swap) => memory.push_str(&format!(" (swap {:.2} GiB)", swap as f64 / GIB)),
                None => {}
            }
            parts.push(memory);
        }
        if let Some(tasks) = self.tasks_max {
            parts.push(format!("tasks {tasks}"));
        }
        if let Some(secs) = self.watchdog_secs {
            parts.push(format!("watchdog {secs} s"));
        }
        parts.join(", ")
    }
}

/// The executor-bound line a page shows for a machine on `os` that reported
/// `bounds`. A macOS or Windows executor has no service-level bound to
/// report, and says why rather than nothing.
pub fn describe_executor_bound(os: &str, bounds: Option<&ServiceBounds>) -> String {
    match bounds {
        Some(bounds) if !bounds.is_empty() => bounds.describe(),
        _ if os.eq_ignore_ascii_case("macos") => {
            "no service-level bound: launchd has no memory accounting; admission is the only bound"
                .to_string()
        }
        _ if os.eq_ignore_ascii_case("windows") => {
            "no service-level bound; admission only".to_string()
        }
        _ => "no unit bound (unbounded install or not under systemd)".to_string(),
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "camelCase")]
pub enum CellPriority {
    ReviewCheck,
    WriteCheck,
    AgentInteractive,
}

/// The runner's routing name for one resident process's relay.
///
/// Random and minted fresh for every start, so it names one incarnation of one
/// process and nothing else. It is not the credential the process presents,
/// but it is the path that process's calls arrive on, so it is kept out of
/// `Debug` output and the logs built from it.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResidentRelayId(pub String);

impl std::fmt::Debug for ResidentRelayId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ResidentRelayId(<redacted>)")
    }
}

impl ResidentRelayId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Decoded payload bytes per file, independent of its JSON wire representation.
pub const MAX_RESIDENT_RUNTIME_ASSET_BYTES: usize = 4 * 1024 * 1024;
/// Decoded payload bytes per collection. Full launch-frame admission is an
/// additional requirement; a collection within this cap may not fit the link.
pub const MAX_RESIDENT_RUNTIME_ASSETS_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_RESIDENT_RUNTIME_ASSETS: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidentRuntimeAsset {
    pub path: String,
    pub data: Vec<u8>,
}

/// What an admission request is asking the fleet for: a cell to run a command
/// batch in, or a cell to hold as a residency.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CellAdmissionKind {
    #[default]
    Command,
    Residency,
}

/// Why a measurement is absent.
///
/// A consumer renders a named gap from one of these rather than inventing a
/// meaning for a null. The codes are stable because both the operator panel and
/// the placement seam branch on them: "this platform cannot answer" and "this
/// platform tried and failed" are different facts about the same machine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum MeasurementGap {
    /// No implementation exists for this reading on this platform.
    UnsupportedPlatform,
    /// The platform API exists, was called, and failed.
    SamplingFailed,
    /// The reading requires a permission this executor does not hold.
    PermissionDenied,
    /// No attempt has completed yet. This is the state a freshly started
    /// executor is honestly in, and it is what a defaulted measurement means.
    NotSampled,
    /// The reading is of the build volume, and that volume is not mounted.
    BuildVolumeAbsent,
}

impl MeasurementGap {
    /// Why there is no value, in the words every surface uses. One home for the
    /// vocabulary, so a placement rejection and the operator panel cannot come
    /// to describe the same gap differently.
    pub fn describe(self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "this platform has no reading",
            Self::SamplingFailed => "sampling failed",
            Self::PermissionDenied => "permission denied",
            Self::NotSampled => "not sampled yet",
            Self::BuildVolumeAbsent => "the build volume is not mounted",
        }
    }
}

/// The outcome of one attempt to take a reading: a value, or a named reason
/// there is none.
///
/// Deliberately not `Option<T>`. A null cannot say whether the platform has no
/// implementation, refused the call, or has simply not been asked yet, and the
/// three lead to different actions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum MeasurementReading<T> {
    Measured {
        value: T,
    },
    Unavailable {
        reason: MeasurementGap,
        /// Bounded diagnostic text where the reason alone is not enough to act
        /// on — an OS error string, say. Never a path on the executor's disk.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}

/// One machine reading, carrying when it was taken.
///
/// The timestamp belongs to the *attempt*, not to the heartbeat that ships it:
/// a fresh beat cannot freshen a cached fact, and a failed refresh publishes an
/// unavailable reading stamped for the failure rather than resending the last
/// good value under a new time. Consumers compute age from
/// `measured_at_unix_ms` and never from the beat.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Measurement<T> {
    pub measured_at_unix_ms: u64,
    pub reading: MeasurementReading<T>,
}

impl<T> Default for Measurement<T> {
    fn default() -> Self {
        Self {
            measured_at_unix_ms: 0,
            reading: MeasurementReading::Unavailable {
                reason: MeasurementGap::NotSampled,
                detail: None,
            },
        }
    }
}

impl<T> Measurement<T> {
    pub fn measured(measured_at_unix_ms: u64, value: T) -> Self {
        Self {
            measured_at_unix_ms,
            reading: MeasurementReading::Measured { value },
        }
    }

    pub fn unavailable(measured_at_unix_ms: u64, reason: MeasurementGap) -> Self {
        Self {
            measured_at_unix_ms,
            reading: MeasurementReading::Unavailable {
                reason,
                detail: None,
            },
        }
    }

    pub fn unavailable_with(
        measured_at_unix_ms: u64,
        reason: MeasurementGap,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            measured_at_unix_ms,
            reading: MeasurementReading::Unavailable {
                reason,
                detail: Some(detail.into()),
            },
        }
    }

    pub fn value(&self) -> Option<&T> {
        match &self.reading {
            MeasurementReading::Measured { value } => Some(value),
            MeasurementReading::Unavailable { .. } => None,
        }
    }

    pub fn gap(&self) -> Option<MeasurementGap> {
        match &self.reading {
            MeasurementReading::Measured { .. } => None,
            MeasurementReading::Unavailable { reason, .. } => Some(*reason),
        }
    }

    /// How long ago this reading was taken, relative to a capture instant.
    /// Distinct from heartbeat age, which describes the link rather than the
    /// fact.
    pub fn age_ms(&self, now_unix_ms: u64) -> u64 {
        now_unix_ms.saturating_sub(self.measured_at_unix_ms)
    }
}
