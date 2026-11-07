//! Deterministic census data shared by consumer parity tests, not a second walker.
use crate::{Census, CensusRow, KernelProcess, StartKey, TickClocks};

#[derive(Clone)]
pub struct ParityStep {
    pub name: &'static str,
    pub success: bool,
    pub rows: Vec<CensusRow>,
    pub unreadable: Vec<u32>,
    pub enumeration_incomplete: bool,
    pub clocks: TickClocks,
    /// Expected sticky root members, pinned independently of either adapter.
    pub root_members: Vec<u32>,
    /// RSS floor, readable count, known count, incomplete. None means no reading.
    pub root_memory: Option<(u64, usize, usize, bool)>,
}

impl ParityStep {
    pub fn census(&self, out: &mut Census) -> bool {
        if !self.success {
            return false;
        }
        out.rows.extend(self.rows.iter().copied());
        out.unreadable.extend(self.unreadable.iter().copied());
        out.enumeration_incomplete = self.enumeration_incomplete;
        true
    }
}

fn row(
    pid: u32,
    parent: u32,
    group: u32,
    start: u64,
    rss: u64,
    cpu: u64,
    storage: u64,
) -> CensusRow {
    CensusRow {
        process: KernelProcess {
            pid,
            parent_pid: parent,
            process_group_id: group,
            session_id: Some(group),
            start: StartKey(start, 0),
        },
        resident_bytes: Some(rss),
        physical_footprint_bytes: None,
        cpu_ms: Some(cpu),
        storage_io_bytes: Some(storage),
        uninterruptible: false,
    }
}

/// Same ordered machine observations for executor and runner adapters. The root
/// is PID 10/start 1, the overlapping child watch is PID 11/start 2. Root reuse
/// must not capture its replacement, and gaps cannot erase exited CPU/storage.
pub fn parity_census_steps() -> Vec<ParityStep> {
    let root = row(10, 1, 10, 1, 100, 10, 100);
    let child = row(11, 10, 11, 2, 200, 20, 200);
    let mut detached = child;
    detached.process.parent_pid = 1;
    let mut group = row(12, 1, 10, 3, 50, 5, 50);
    group.process.session_id = Some(12);
    let mut session = row(13, 1, 13, 4, 25, 2, 25);
    session.process.session_id = Some(10);
    let mut steps = vec![];
    let mut push = |name,
                    rows: Vec<CensusRow>,
                    unreadable,
                    enumeration_incomplete,
                    success,
                    members,
                    memory| {
        let index = steps.len() as u64;
        steps.push(ParityStep {
            name,
            success,
            rows,
            unreadable,
            enumeration_incomplete,
            clocks: TickClocks {
                monotonic_ms: 1_000 + index * 5_000,
                unix_ms: 50_000 + index * 5_000,
            },
            root_members: members,
            root_memory: memory,
        });
    };
    push(
        "initial",
        vec![root, child],
        vec![],
        false,
        true,
        vec![10, 11],
        Some((300, 2, 2, false)),
    );
    push(
        "detached_group_and_session",
        vec![root, detached, group, session],
        vec![],
        false,
        true,
        vec![10, 11, 12, 13],
        Some((375, 4, 4, false)),
    );
    let mut rss_gap = detached;
    rss_gap.resident_bytes = None;
    rss_gap.cpu_ms = Some(40);
    rss_gap.storage_io_bytes = Some(400);
    push(
        "rss_only_gap",
        vec![root, rss_gap, group, session],
        vec![],
        false,
        true,
        vec![10, 11, 12, 13],
        Some((175, 3, 4, true)),
    );
    let mut cpu_gap = detached;
    cpu_gap.cpu_ms = None;
    cpu_gap.storage_io_bytes = Some(450);
    push(
        "cpu_only_gap",
        vec![root, cpu_gap, group, session],
        vec![],
        false,
        true,
        vec![10, 11, 12, 13],
        Some((375, 4, 4, false)),
    );
    let mut storage_gap = detached;
    storage_gap.cpu_ms = Some(50);
    storage_gap.storage_io_bytes = None;
    push(
        "requested_storage_only_gap",
        vec![root, storage_gap, group, session],
        vec![],
        false,
        true,
        vec![10, 11, 12, 13],
        Some((375, 4, 4, false)),
    );
    push(
        "unreadable_identity",
        vec![root, group, session],
        vec![11],
        false,
        true,
        vec![10, 11, 12, 13],
        Some((175, 3, 4, true)),
    );
    push(
        "partial_enumeration",
        vec![root, group, session],
        vec![],
        true,
        true,
        vec![10, 11, 12, 13],
        Some((175, 3, 4, true)),
    );
    push(
        "failed_census",
        vec![],
        vec![],
        false,
        false,
        vec![10, 11, 12, 13],
        Some((175, 3, 4, true)),
    );
    let mut recovered = detached;
    recovered.cpu_ms = Some(100);
    recovered.storage_io_bytes = Some(1_000);
    push(
        "recovery",
        vec![root, recovered, group, session],
        vec![],
        false,
        true,
        vec![10, 11, 12, 13],
        Some((375, 4, 4, false)),
    );
    let reused = row(10, 1, 10, 99, 999, 999, 999);
    push(
        "leader_reuse_with_survivors",
        vec![reused, recovered, group, session],
        vec![],
        false,
        true,
        vec![11, 12, 13],
        Some((275, 3, 3, false)),
    );
    push(
        "survivors_departed",
        vec![reused],
        vec![],
        false,
        true,
        vec![],
        None,
    );
    steps
}
