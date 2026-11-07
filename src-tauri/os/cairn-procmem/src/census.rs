use crate::identity::KernelProcess;
use std::sync::Arc;

/// Identity and optional instruments from one kernel observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CensusRow {
    pub process: KernelProcess,
    pub resident_bytes: Option<u64>,
    /// What the process privately costs, which RSS overstates by counting
    /// shared pages: `ri_phys_footprint` on macOS, `Pss` from `smaps_rollup` on
    /// Linux. `None` where the platform cannot say.
    pub physical_footprint_bytes: Option<u64>,
    pub cpu_ms: Option<u64>,
    pub uninterruptible: bool,
    /// Physical storage bytes, not pipe or page-cache traffic.
    pub storage_io_bytes: Option<u64>,
}

/// Reused machine-sized buffers; rows and unreadable PIDs are sorted before folding.
#[derive(Default)]
pub struct Census {
    pub rows: Vec<CensusRow>,
    pub unreadable: Vec<u32>,
    pub storage_wanted: Vec<u32>,
    /// Pids whose footprint a watch wants. Linux pays a page-table walk per
    /// read, so it reads only these and their direct children (a child that
    /// joined this tick); macOS reads every row, the cost being one syscall.
    pub footprint_wanted: Vec<u32>,
    /// Unverified parent hints, never identities. These can prove a gap, not membership.
    pub unverified_parents: Vec<(u32, u32)>,
    /// An enumeration gap is not evidence that an absent known member departed.
    pub enumeration_incomplete: bool,
}

impl Census {
    pub fn clear(&mut self) {
        self.rows.clear();
        self.unreadable.clear();
        self.storage_wanted.clear();
        self.footprint_wanted.clear();
        self.unverified_parents.clear();
        self.enumeration_incomplete = false;
    }

    pub fn wants_storage(&self, pid: u32) -> bool {
        self.storage_wanted.binary_search(&pid).is_ok()
    }

    pub fn wants_footprint(&self, pid: u32) -> bool {
        self.footprint_wanted.binary_search(&pid).is_ok()
    }

    pub fn row(&self, pid: u32) -> Option<&CensusRow> {
        self.rows
            .binary_search_by_key(&pid, |row| row.process.pid)
            .ok()
            .map(|index| &self.rows[index])
    }
}

pub type CensusFn = Arc<dyn Fn(&mut Census) -> bool + Send + Sync>;
