//! Memory probe: per-process resident memory and system-wide memory
//! availability, used to size the warm-process pool against real headroom
//! rather than an arbitrary count (CAIRN-2543).
//!
//! Direct `libc` FFI, following the `services/reaper.rs` precedent (which
//! deliberately rejected pulling in a system-info crate). macOS reads task and
//! host statistics through mach; Linux reads `/proc` (and cgroup v2 limits when
//! running memory-capped in a container). Every other platform returns `None`,
//! which makes the GC fall back to its count cap.
//!
//! It also reads a whole process tree ([`tree_members`]): a leader identified by
//! its pid and kernel start time, plus every live descendant, each identified
//! the same way while the walk reads it. The executor measures its trees by a
//! different route (`cairn-exec`'s `ProcessTreeSampler`); the two follow the
//! same rules (a recycled pid confers no membership, and a reading with an
//! unreadable member is `incomplete`, never a smaller number) and are to be
//! merged into one crate both import.

use cairn_common::process_identity::ProcessAnchor;

/// A snapshot of system-wide memory in bytes.
pub use cairn_common::memory::SystemMemory;

/// Whether [`tree_members`] can read a process tree on this platform.
pub const TREE_READS_SUPPORTED: bool = cfg!(any(target_os = "macos", target_os = "linux"));

/// The most processes one tree reading walks. A tree past it is read as
/// incomplete rather than walked without bound.
const TREE_MEMBER_LIMIT: usize = 4096;

/// One process found in a tree walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeMember {
    /// The pid and the start time read during this walk.
    pub anchor: ProcessAnchor,
    /// Resident bytes, or `None` when the process is present but could not be
    /// read (permission refused, short read).
    pub resident_bytes: Option<u64>,
    /// Whether this member's children could be listed. When they could not,
    /// the tree below it is unknown.
    pub children_listed: bool,
}

/// The resident size of one process tree, leader plus descendants.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeResident {
    /// Sum over the members that could be read.
    pub resident_bytes: u64,
    /// Every member present when the walk read it, the leader included.
    pub members: u32,
    /// Members present but unreadable, or whose children could not be listed.
    pub unreadable: u32,
}

impl TreeResident {
    pub fn of(members: &[TreeMember]) -> Self {
        members.iter().fold(Self::default(), |mut total, member| {
            total.members += 1;
            match member.resident_bytes {
                Some(bytes) => total.resident_bytes = total.resident_bytes.saturating_add(bytes),
                None => total.unreadable += 1,
            }
            if member.resident_bytes.is_some() && !member.children_listed {
                total.unreadable += 1;
            }
            total
        })
    }

    /// A tree with a member it could not read is incomplete: its total is a
    /// floor under the tree's size, and is shown as one, never as the size.
    pub fn incomplete(&self) -> bool {
        self.unreadable > 0
    }
}

/// A source of memory measurements. Abstracted behind a trait so the GC's
/// budget policy is unit-testable with a stub, without spawning real processes.
pub trait MemoryProbe: Send + Sync {
    /// Resident set size of `pid` in bytes, or `None` when it cannot be
    /// measured (process gone, not inspectable, or unsupported platform).
    fn process_rss_bytes(&self, pid: u32) -> Option<u64>;

    /// System-wide memory, or `None` when it cannot be measured. `None` makes
    /// the GC size the pool by count instead of by headroom.
    fn system_memory(&self) -> Option<SystemMemory>;

    /// The resident size of `leader`'s process tree, or `None` when the leader
    /// is gone, its pid now names another process, or this platform cannot
    /// read trees.
    fn process_tree_resident(&self, leader: &ProcessAnchor) -> Option<TreeResident>;

    /// Whether [`MemoryProbe::process_tree_resident`] can answer at all.
    fn reads_trees(&self) -> bool;
}

/// Production probe backed by OS calls.
pub struct OsMemoryProbe;

impl MemoryProbe for OsMemoryProbe {
    fn process_rss_bytes(&self, pid: u32) -> Option<u64> {
        #[cfg(target_os = "macos")]
        {
            process_rss_macos(pid)
        }
        #[cfg(target_os = "linux")]
        {
            process_rss_linux(pid)
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let _ = pid;
            None
        }
    }

    fn system_memory(&self) -> Option<SystemMemory> {
        cairn_common::memory::system_memory()
    }

    fn process_tree_resident(&self, leader: &ProcessAnchor) -> Option<TreeResident> {
        tree_members(leader).map(|members| TreeResident::of(&members))
    }

    fn reads_trees(&self) -> bool {
        TREE_READS_SUPPORTED
    }
}

/// What one read of a single process said.
#[cfg_attr(not(any(target_os = "macos", target_os = "linux")), allow(dead_code))]
enum MemberRead {
    Live {
        parent_pid: u32,
        start_token: String,
        resident_bytes: Option<u64>,
    },
    /// No such process, or a zombie: it holds no memory and is not counted.
    Departed,
    /// Present, but its identity could not be read.
    Unreadable,
}

/// Every live process in `leader`'s tree: the leader, then its descendants.
///
/// `None` when the leader has exited or its pid names a process with another
/// start time (a recycled pid confers nothing). A descendant is counted only
/// while it is still the child of the member it was listed under, so a pid
/// recycled between listing and reading is not mistaken for a member.
pub fn tree_members(leader: &ProcessAnchor) -> Option<Vec<TreeMember>> {
    if !TREE_READS_SUPPORTED {
        return None;
    }
    let MemberRead::Live {
        start_token,
        resident_bytes,
        ..
    } = read_member(leader.pid)
    else {
        return None;
    };
    if start_token != leader.start_token {
        return None;
    }
    let mut members = vec![TreeMember {
        anchor: leader.clone(),
        resident_bytes,
        children_listed: false,
    }];
    let mut seen = std::collections::HashSet::from([leader.pid]);
    let mut next = 0;
    while next < members.len() {
        let parent = members[next].anchor.pid;
        let children = list_children(parent);
        members[next].children_listed = children.is_some();
        next += 1;
        for pid in children.unwrap_or_default() {
            if !seen.insert(pid) {
                continue;
            }
            if members.len() >= TREE_MEMBER_LIMIT {
                // Counted as an unreadable member so the reading says it is
                // a floor.
                members.push(TreeMember {
                    anchor: ProcessAnchor {
                        pid,
                        start_token: String::new(),
                    },
                    resident_bytes: None,
                    children_listed: false,
                });
                return Some(members);
            }
            match read_member(pid) {
                MemberRead::Live {
                    parent_pid,
                    start_token,
                    resident_bytes,
                } if parent_pid == parent => members.push(TreeMember {
                    anchor: ProcessAnchor { pid, start_token },
                    resident_bytes,
                    children_listed: false,
                }),
                MemberRead::Live { .. } | MemberRead::Departed => {}
                MemberRead::Unreadable => members.push(TreeMember {
                    anchor: ProcessAnchor {
                        pid,
                        start_token: String::new(),
                    },
                    resident_bytes: None,
                    children_listed: false,
                }),
            }
        }
    }
    Some(members)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn read_member(_pid: u32) -> MemberRead {
    MemberRead::Departed
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn list_children(_pid: u32) -> Option<Vec<u32>> {
    None
}

// ============================================================================
// macOS
// ============================================================================

/// RSS via `proc_pidinfo(PROC_PIDTASKINFO)`. `None` when the process is gone
/// (ESRCH), not inspectable (EPERM), or the call returns a short read.
#[cfg(target_os = "macos")]
fn process_rss_macos(pid: u32) -> Option<u64> {
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    let ret = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDTASKINFO,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            size,
        )
    };
    // proc_pidinfo returns bytes written; a short read means the struct is not
    // reliably populated, and <= 0 is an error.
    if ret < size {
        return None;
    }
    Some(info.pti_resident_size)
}

/// `proc_listpids` selector for "children of pid N", from the macOS SDK's
/// `sys/proc_info.h`. The `libc` crate does not export it.
#[cfg(target_os = "macos")]
const PROC_PPID_ONLY: u32 = 6;

/// One member's identity, then its size. A size read that fails is a gap only
/// if the same process is still there afterwards: one that exited between the
/// two reads has departed, so a tree whose short-lived children come and go
/// is not reported as hiding something.
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn read_member(pid: u32) -> MemberRead {
    let (parent_pid, start_token) = match read_identity(pid) {
        Ok(identity) => identity,
        Err(read) => return read,
    };
    #[cfg(target_os = "macos")]
    let resident_bytes = process_rss_macos(pid);
    #[cfg(target_os = "linux")]
    let resident_bytes = process_rss_linux(pid);
    if resident_bytes.is_none()
        && !matches!(read_identity(pid), Ok((_, again)) if again == start_token)
    {
        return MemberRead::Departed;
    }
    MemberRead::Live {
        parent_pid,
        start_token,
        resident_bytes,
    }
}

/// A live process's parent and start token, or why there is none.
#[cfg(target_os = "macos")]
fn read_identity(pid: u32) -> Result<(u32, String), MemberRead> {
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    let read = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDTBSDINFO,
            0,
            (&mut info as *mut libc::proc_bsdinfo).cast(),
            size,
        )
    };
    if read != size {
        return Err(
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                MemberRead::Departed
            } else {
                MemberRead::Unreadable
            },
        );
    }
    if info.pbi_status == libc::SZOMB {
        return Err(MemberRead::Departed);
    }
    // The token `process_identity::process_start_token` renders.
    Ok((
        info.pbi_ppid,
        format!("{}:{}", info.pbi_start_tvsec, info.pbi_start_tvusec),
    ))
}

#[cfg(target_os = "macos")]
fn list_children(pid: u32) -> Option<Vec<u32>> {
    let pid_size = std::mem::size_of::<libc::c_int>();
    let needed = unsafe { libc::proc_listpids(PROC_PPID_ONLY, pid, std::ptr::null_mut(), 0) };
    if needed < 0 {
        return None;
    }
    // Room for children forked between the size query and the listing.
    let mut pids = vec![0 as libc::c_int; needed as usize / pid_size + 16];
    let filled = unsafe {
        libc::proc_listpids(
            PROC_PPID_ONLY,
            pid,
            pids.as_mut_ptr().cast(),
            (pids.len() * pid_size) as libc::c_int,
        )
    };
    if filled < 0 {
        return None;
    }
    pids.truncate(filled as usize / pid_size);
    Some(
        pids.into_iter()
            .filter(|child| *child > 0)
            .map(|child| child as u32)
            .collect(),
    )
}

// ============================================================================
// Linux
// ============================================================================

/// RSS via `VmRSS` in `/proc/<pid>/status` (reported in kB).
#[cfg(target_os = "linux")]
fn process_rss_linux(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Whether a failed read means the process (or thread) is gone rather than
/// hidden: procfs answers `ENOENT` or `ESRCH` for one that exited mid-read.
#[cfg(target_os = "linux")]
fn departed(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
}

/// A live process's parent and start token, or why there is none.
#[cfg(target_os = "linux")]
fn read_identity(pid: u32) -> Result<(u32, String), MemberRead> {
    let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if departed(&error) => return Err(MemberRead::Departed),
        Err(_) => return Err(MemberRead::Unreadable),
    };
    // The comm field may contain spaces and parentheses, so fields are counted
    // from after its final ')': index 0 is the state, 1 the parent, 19 the
    // start time (the token `process_identity::process_start_token` renders).
    let Some(fields) = stat.rsplit_once(')').map(|(_, rest)| rest) else {
        return Err(MemberRead::Unreadable);
    };
    let fields: Vec<&str> = fields.split_whitespace().collect();
    let (Some(state), Some(parent), Some(start)) = (fields.first(), fields.get(1), fields.get(19))
    else {
        return Err(MemberRead::Unreadable);
    };
    if *state == "Z" || *state == "X" {
        return Err(MemberRead::Departed);
    }
    let Ok(parent_pid) = parent.parse() else {
        return Err(MemberRead::Unreadable);
    };
    Ok((parent_pid, (*start).to_owned()))
}

/// Children of every thread of `pid`, from `/proc/<pid>/task/<tid>/children`.
/// `None` when the task directory cannot be read, or when a kernel built
/// without `CONFIG_PROC_CHILDREN` publishes no children file.
#[cfg(target_os = "linux")]
fn list_children(pid: u32) -> Option<Vec<u32>> {
    let tasks = match std::fs::read_dir(format!("/proc/{pid}/task")) {
        Ok(tasks) => tasks,
        // It exited after it was read: it has no children left to count.
        Err(error) if departed(&error) => return Some(Vec::new()),
        Err(_) => return None,
    };
    let mut children = Vec::new();
    let mut listed = false;
    for task in tasks.flatten() {
        match std::fs::read_to_string(task.path().join("children")) {
            Ok(list) => {
                listed = true;
                children.extend(
                    list.split_whitespace()
                        .filter_map(|child| child.parse::<u32>().ok()),
                );
            }
            // A thread that exited mid-walk has no children to report.
            Err(error) if departed(&error) => {}
            Err(_) => return None,
        }
    }
    listed.then_some(children)
}

// ============================================================================
// Test stub
// ============================================================================

/// Deterministic probe for unit tests: canned system memory, per-pid RSS, and
/// process trees keyed by leader pid.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone, Default)]
pub struct StubMemoryProbe {
    pub system: Option<SystemMemory>,
    pub rss: std::collections::HashMap<u32, u64>,
    /// Leader pid to (its current start token, each member's resident bytes,
    /// `None` for an unreadable member). The leader is the first member.
    pub trees: std::collections::HashMap<u32, (String, Vec<Option<u64>>)>,
    /// Whether this stub reads trees; on by default.
    pub no_tree_reads: bool,
    /// How many tree reads this probe has answered.
    pub tree_reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(any(test, feature = "test-utils"))]
impl StubMemoryProbe {
    /// A probe reporting the given system memory (or `None` to force the GC's
    /// count-based fallback path) and no measurable per-process RSS.
    pub fn new(system: Option<SystemMemory>) -> Self {
        Self {
            system,
            ..Self::default()
        }
    }

    /// Register a measurable RSS for `pid`.
    pub fn with_rss(mut self, pid: u32, bytes: u64) -> Self {
        self.rss.insert(pid, bytes);
        self
    }

    /// Register a tree under leader `pid` whose current start token is `start`.
    pub fn with_tree(mut self, pid: u32, start: &str, members: Vec<Option<u64>>) -> Self {
        self.trees.insert(pid, (start.to_string(), members));
        self
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl MemoryProbe for StubMemoryProbe {
    fn process_rss_bytes(&self, pid: u32) -> Option<u64> {
        self.rss.get(&pid).copied()
    }

    fn system_memory(&self) -> Option<SystemMemory> {
        self.system
    }

    fn process_tree_resident(&self, leader: &ProcessAnchor) -> Option<TreeResident> {
        self.tree_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (start, members) = self.trees.get(&leader.pid)?;
        if *start != leader.start_token {
            return None;
        }
        let members: Vec<TreeMember> = members
            .iter()
            .enumerate()
            .map(|(index, bytes)| TreeMember {
                anchor: ProcessAnchor {
                    pid: leader.pid + index as u32,
                    start_token: start.clone(),
                },
                resident_bytes: *bytes,
                children_listed: true,
            })
            .collect();
        Some(TreeResident::of(&members))
    }

    fn reads_trees(&self) -> bool {
        !self.no_tree_reads
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The real probe can measure this test process and the host on the
    /// platforms it supports. Mirrors `reaper.rs`'s real-process integration
    /// test: a smoke check that the FFI is wired correctly.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn os_probe_measures_self_and_system() {
        let probe = OsMemoryProbe;
        let rss = probe.process_rss_bytes(std::process::id());
        assert!(
            rss.map(|b| b > 0).unwrap_or(false),
            "self RSS should be a positive measurement, got {rss:?}"
        );
        let sys = probe.system_memory().expect("system memory measurable");
        assert!(sys.total > 0, "total memory should be positive");
        assert!(sys.available > 0, "available memory should be positive");
        assert!(
            sys.available <= sys.total,
            "available ({}) must not exceed total ({})",
            sys.available,
            sys.total
        );
    }

    #[test]
    fn stub_probe_reports_canned_values() {
        let probe = StubMemoryProbe::new(Some(SystemMemory {
            total: 16,
            available: 8,
        }))
        .with_rss(42, 1234);
        assert_eq!(probe.process_rss_bytes(42), Some(1234));
        assert_eq!(probe.process_rss_bytes(99), None);
        assert_eq!(probe.system_memory().map(|s| s.available), Some(8));
    }

    fn anchor(pid: u32, start: &str) -> ProcessAnchor {
        ProcessAnchor {
            pid,
            start_token: start.into(),
        }
    }

    /// A real tree: a shell leader with two background sleeps. The reading is
    /// the leader plus its descendants, complete, and nonzero.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn os_probe_reads_the_leader_and_every_descendant() {
        let mut child = std::process::Command::new("sh")
            .args(["-c", "sleep 30 & sleep 30 & wait"])
            .spawn()
            .unwrap();
        let pid = child.id();
        let start = cairn_common::process_identity::process_start_token(pid).unwrap();
        let leader = anchor(pid, &start);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let reading = loop {
            let reading = OsMemoryProbe.process_tree_resident(&leader).unwrap();
            if reading.members >= 3 || std::time::Instant::now() > deadline {
                break reading;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        let _ = child.kill();
        let _ = child.wait();
        assert!(reading.members >= 3, "leader and both sleeps: {reading:?}");
        assert!(!reading.incomplete(), "{reading:?}");
        assert!(reading.resident_bytes > 0, "{reading:?}");
        // The pid under another start time is not this tree.
        assert_eq!(
            OsMemoryProbe.process_tree_resident(&anchor(std::process::id(), "0:0")),
            None
        );
    }

    #[test]
    fn a_recycled_leader_pid_gives_no_reading() {
        let probe = StubMemoryProbe::default().with_tree(7, "new", vec![Some(10), Some(20)]);
        assert_eq!(probe.process_tree_resident(&anchor(7, "old")), None);
        assert_eq!(
            probe.process_tree_resident(&anchor(7, "new")),
            Some(TreeResident {
                resident_bytes: 30,
                members: 2,
                unreadable: 0
            })
        );
    }

    #[test]
    fn an_unreadable_member_makes_the_reading_incomplete_not_smaller() {
        let probe = StubMemoryProbe::default().with_tree(7, "s", vec![Some(10), None, Some(5)]);
        let reading = probe.process_tree_resident(&anchor(7, "s")).unwrap();
        assert!(reading.incomplete());
        assert_eq!((reading.members, reading.unreadable), (3, 1));
        assert_eq!(
            reading.resident_bytes, 15,
            "a floor over the readable members"
        );
    }

    #[test]
    fn a_member_whose_children_are_unlisted_is_a_gap() {
        let member = |bytes, children_listed| TreeMember {
            anchor: anchor(1, "s"),
            resident_bytes: Some(bytes),
            children_listed,
        };
        let reading = TreeResident::of(&[member(4, true), member(6, false)]);
        assert_eq!(reading.resident_bytes, 10);
        assert!(reading.incomplete());
    }
}
