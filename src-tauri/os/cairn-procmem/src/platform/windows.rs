//! Windows process observations use query-only handles. Toolhelp ancestry is
//! accepted only for a process created before the snapshot; the membership fold
//! must additionally require the verified parent's creation time <= the child's.
//! Without a caller-owned tree job, a child born and orphaned between ticks can
//! escape discovery. Terminal sessions and console groups are not tree proofs.
use crate::{
    census::{Census, CensusRow},
    identity::{KernelProcess, KernelRead, StartKey},
};
use std::{
    io,
    mem::{size_of, zeroed},
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle},
    ptr::null_mut,
    sync::Mutex,
};
use windows_sys::Win32::{
    Foundation::{
        DuplicateHandle, GetLastError, ERROR_INVALID_PARAMETER, ERROR_MORE_DATA,
        ERROR_NO_MORE_FILES, FILETIME, HANDLE, INVALID_HANDLE_VALUE,
    },
    System::{
        Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, Process32FirstW, Process32NextW, PROCESSENTRY32W,
            TH32CS_SNAPPROCESS,
        },
        JobObjects::{IsProcessInJob, JobObjectBasicProcessIdList, QueryInformationJobObject},
        ProcessStatus::{K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS},
        SystemInformation::GetSystemTimeAsFileTime,
        SystemServices::JOB_OBJECT_QUERY,
        Threading::{
            GetCurrentProcess, GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
        },
    },
};

fn owned(handle: HANDLE) -> io::Result<OwnedHandle> {
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: the successful Win32 creation call returned an owned real handle.
        Ok(unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) })
    }
}

fn open(pid: u32) -> Result<OwnedHandle, KernelRead> {
    // PID zero is Idle, not a missing process. OpenProcess documents INVALID_PARAMETER
    // for it as well as nonexistent PIDs.
    if pid == 0 {
        return Err(KernelRead::Unreadable);
    }
    owned(unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) })
        .map_err(|error| classify_open_error(error.raw_os_error().unwrap_or_default() as u32))
}

fn classify_open_error(error: u32) -> KernelRead {
    if error == ERROR_INVALID_PARAMETER {
        KernelRead::Gone
    } else {
        KernelRead::Unreadable
    }
}

fn filetime(value: FILETIME) -> u64 {
    (u64::from(value.dwHighDateTime) << 32) | u64::from(value.dwLowDateTime)
}

struct Times {
    start: StartKey,
    exited: bool,
    cpu_ms: u64,
}

fn times(handle: &OwnedHandle) -> Option<Times> {
    let (mut creation, mut exit, mut kernel, mut user): (FILETIME, FILETIME, FILETIME, FILETIME) =
        unsafe { zeroed() };
    if unsafe {
        GetProcessTimes(
            handle.as_raw_handle(),
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        )
    } == 0
    {
        return None;
    }
    Some(Times {
        start: StartKey(filetime(creation), 0),
        exited: filetime(exit) != 0,
        cpu_ms: filetime(kernel).saturating_add(filetime(user)) / 10_000,
    })
}

fn rss(handle: &OwnedHandle) -> Option<u64> {
    let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { zeroed() };
    counters.cb = size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
    (unsafe {
        K32GetProcessMemoryInfo(
            handle.as_raw_handle(),
            &mut counters,
            size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        )
    } != 0)
        .then_some(counters.WorkingSetSize as u64)
}

fn process(pid: u32, parent_pid: u32, start: StartKey) -> KernelProcess {
    KernelProcess {
        pid,
        parent_pid,
        process_group_id: 0,
        session_id: None,
        start,
    }
}

#[cfg(test)]
thread_local! { static SNAPSHOT_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

struct Snapshot {
    handle: OwnedHandle,
    before: u64,
}

impl Snapshot {
    fn new() -> io::Result<Self> {
        #[cfg(test)]
        SNAPSHOT_READS.with(|reads| reads.set(reads.get() + 1));
        let mut before: FILETIME = unsafe { zeroed() };
        unsafe { GetSystemTimeAsFileTime(&mut before) };
        let handle = owned(unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) })?;
        Ok(Self {
            handle,
            before: filetime(before),
        })
    }

    /// False means the snapshot could not be read to its end.
    fn each(&self, mut visit: impl FnMut(&PROCESSENTRY32W)) -> bool {
        let mut entry: PROCESSENTRY32W = unsafe { zeroed() };
        entry.dwSize = size_of::<PROCESSENTRY32W>() as u32;
        let mut ok = unsafe { Process32FirstW(self.handle.as_raw_handle(), &mut entry) };
        while ok != 0 {
            visit(&entry);
            ok = unsafe { Process32NextW(self.handle.as_raw_handle(), &mut entry) };
        }
        unsafe { GetLastError() == ERROR_NO_MORE_FILES }
    }
}

fn row(handle: &OwnedHandle, pid: u32, parent: u32, observed: &Times) -> CensusRow {
    CensusRow {
        process: process(pid, parent, observed.start),
        resident_bytes: rss(handle),
        physical_footprint_bytes: None,
        cpu_ms: Some(observed.cpu_ms),
        uninterruptible: false,
        // GetProcessIoCounters includes logical/cache/pipe I/O, not physical storage.
        storage_io_bytes: None,
    }
}

fn normalize(out: &mut Census) {
    out.rows.sort_unstable_by_key(|row| row.process.pid);
    out.rows.dedup_by_key(|row| row.process.pid);
    out.unreadable.sort_unstable();
    out.unreadable.dedup();
}

/// Appends one machine snapshot. Callers clear reusable census buffers per tick.
pub fn census(out: &mut Census) -> bool {
    let snapshot = match Snapshot::new() {
        Ok(snapshot) => snapshot,
        Err(_) => {
            out.enumeration_incomplete = true;
            return false;
        }
    };
    let complete = snapshot.each(|entry| {
        let pid = entry.th32ProcessID;
        match open(pid) {
            Ok(handle) => match times(&handle) {
                Some(observed) if observed.exited => {}
                Some(observed) if observed.start.0 <= snapshot.before => {
                    out.rows
                        .push(row(&handle, pid, entry.th32ParentProcessID, &observed));
                }
                // A replacement PID must not inherit the old snapshot's ancestry.
                _ => {
                    out.unreadable.push(pid);
                    out.unverified_parents
                        .push((pid, entry.th32ParentProcessID));
                }
            },
            Err(KernelRead::Gone) => {}
            Err(_) => {
                out.unreadable.push(pid);
                out.unverified_parents
                    .push((pid, entry.th32ParentProcessID));
            }
        }
    });
    out.enumeration_incomplete |= !complete;
    normalize(out);
    // Even a partially read snapshot provides evidence; never claim absent members
    // departed when enumeration_incomplete is set.
    complete || !out.rows.is_empty() || !out.unreadable.is_empty()
}

/// A held handle fences both identity and metrics even if the process exits.
pub fn read_kernel_state(pid: u32) -> KernelRead {
    let handle = match open(pid) {
        Ok(handle) => handle,
        Err(state) => return state,
    };
    let Some(observed) = times(&handle) else {
        return KernelRead::Unreadable;
    };
    if observed.exited {
        return KernelRead::Gone;
    }
    let Ok(snapshot) = Snapshot::new() else {
        return KernelRead::Unreadable;
    };
    let mut parent = None;
    let complete = snapshot.each(|entry| {
        if entry.th32ProcessID == pid {
            parent = Some(entry.th32ParentProcessID);
        }
    });
    match parent {
        Some(parent) => KernelRead::Live(process(pid, parent, observed.start)),
        None if complete => {
            // It can have exited while taking the snapshot; only the held handle
            // can distinguish that from an unreadable observation.
            if times(&handle).is_some_and(|times| times.exited) {
                KernelRead::Gone
            } else {
                KernelRead::Unreadable
            }
        }
        None => KernelRead::Unreadable,
    }
}

/// Windows has no Unix zombie state; a caller holding its exited child handle
/// can still capture the creation token until it releases that handle.
pub fn read_unreaped_child(pid: u32) -> Result<KernelProcess, String> {
    let handle = open(pid).map_err(|state| format!("cannot open process {pid}: {state:?}"))?;
    let observed = times(&handle).ok_or_else(|| format!("cannot read process {pid} times"))?;
    // Parent metadata is not needed for identity capture. An exited child is no
    // longer in Toolhelp, but its handle still supplies its original creation time.
    let mut parent = 0;
    if let Ok(snapshot) = Snapshot::new() {
        snapshot.each(|entry| {
            if entry.th32ProcessID == pid {
                parent = entry.th32ParentProcessID;
            }
        });
    }
    Ok(process(pid, parent, observed.start))
}

/// Creation identity from one held query handle, including an exited child
/// whose process object is retained by its parent. Token capture needs no
/// Toolhelp parent metadata and must not enumerate the machine.
pub fn read_start_token(pid: u32) -> Option<String> {
    let handle = open(pid).ok()?;
    Some(times(&handle)?.start.token())
}

pub fn process_rss_bytes(pid: u32) -> Option<u64> {
    let handle = open(pid).ok()?;
    let observed = times(&handle)?;
    if observed.exited {
        None
    } else {
        rss(&handle)
    }
}

/// Read-only capability for one caller-owned process-tree job. Never inferred
/// from the supervisor's current job, and never used to assign or kill processes.
pub struct TreeJob {
    handle: OwnedHandle,
    buffer: Mutex<Vec<usize>>,
}

impl TreeJob {
    /// Duplicates a live caller handle with query rights only.
    ///
    /// # Safety
    /// The handle must be a valid job handle for the duration of this call.
    /// The caller must guarantee this job is scoped exclusively to the watched
    /// tree, not a supervisor-wide job containing unrelated watched processes.
    pub unsafe fn from_raw_handle(handle: RawHandle) -> io::Result<Self> {
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a real tree-scoped job handle is required",
            ));
        }
        let current = unsafe { GetCurrentProcess() };
        let mut duplicate = null_mut();
        if unsafe {
            DuplicateHandle(
                current,
                handle,
                current,
                &mut duplicate,
                JOB_OBJECT_QUERY,
                0,
                0,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            handle: owned(duplicate)?,
            buffer: Mutex::new(Vec::new()),
        })
    }

    /// Appends verified job rows to a dedicated tree census. The caller merges
    /// these identities into this watch's membership, never every other watch.
    /// Unknown candidate PIDs and enumeration gaps are incomplete evidence.
    pub fn census(&self, out: &mut Census) -> bool {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let (pids, complete) = query_job_pids(&mut buffer, 16, |buffer, bytes| {
            let ok = unsafe {
                QueryInformationJobObject(
                    self.handle.as_raw_handle(),
                    JobObjectBasicProcessIdList,
                    buffer.as_mut_ptr().cast(),
                    bytes,
                    null_mut(),
                )
            };
            if ok == 0 {
                Err(unsafe { GetLastError() })
            } else {
                Ok(())
            }
        });
        out.enumeration_incomplete |= !complete;
        for pid in pids {
            let handle = match open(pid) {
                Ok(handle) => handle,
                Err(KernelRead::Gone) => continue,
                Err(_) => {
                    out.unreadable.push(pid);
                    continue;
                }
            };
            let mut in_job = 0;
            if unsafe {
                IsProcessInJob(
                    handle.as_raw_handle(),
                    self.handle.as_raw_handle(),
                    &mut in_job,
                )
            } == 0
            {
                out.unreadable.push(pid);
                continue;
            }
            if in_job == 0 {
                continue;
            }
            match times(&handle) {
                Some(observed) if observed.exited => {}
                Some(observed) => {
                    // Job membership is proved on this held process handle.
                    // Parent metadata comes from the one shared machine census,
                    // not another machine snapshot for every scoped watch.
                    out.rows.push(row(&handle, pid, 0, &observed));
                }
                None => out.unreadable.push(pid),
            }
        }
        normalize(out);
        complete
    }
}

// The PID list starts with two DWORD counts, then ULONG_PTR entries. usize
// storage gives the required alignment on both Windows architectures.
fn query_job_pids(
    buffer: &mut Vec<usize>,
    initial_capacity: usize,
    mut query: impl FnMut(&mut [usize], u32) -> Result<(), u32>,
) -> (Vec<u32>, bool) {
    const MAX_PIDS: usize = 1 << 20;
    let mut capacity = initial_capacity
        .max(buffer.len().saturating_sub(2))
        .clamp(1, MAX_PIDS);
    let mut pids = Vec::new();
    for _ in 0..8 {
        let bytes = 8 + capacity * size_of::<usize>();
        buffer.resize(bytes.div_ceil(size_of::<usize>()), 0);
        buffer.fill(0);
        let result = query(buffer, bytes as u32);
        if result.is_err_and(|error| error != ERROR_MORE_DATA) {
            pids.sort_unstable();
            pids.dedup();
            return (pids, false);
        }
        let header = buffer.as_ptr().cast::<u32>();
        let assigned = unsafe { *header } as usize;
        let listed = unsafe { *header.add(1) } as usize;
        // Never trust a count to form a slice outside the supplied buffer.
        if listed > capacity {
            return (pids, false);
        }
        let entries = unsafe {
            std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>().add(8).cast::<usize>(), listed)
        };
        pids.extend(entries.iter().filter_map(|pid| u32::try_from(*pid).ok()));
        if result.is_ok() && assigned == listed {
            pids.sort_unstable();
            pids.dedup();
            return (pids, true);
        }
        if capacity == MAX_PIDS {
            break;
        }
        capacity = assigned.max(capacity.saturating_mul(2)).min(MAX_PIDS);
    }
    pids.sort_unstable();
    pids.dedup();
    (pids, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{BufRead, BufReader, Write},
        process::{Child, Command, Stdio},
        time::{Duration, Instant},
    };
    use windows_sys::Win32::System::{
        JobObjects::{AssignProcessToJobObject, CreateJobObjectW},
        Threading::GetProcessHandleCount,
    };

    #[test]
    fn child_fixture() {
        if std::env::var_os("CAIRN_PROCMEM_WINDOWS_FIXTURE").is_some() {
            run_fixture_child();
        } else {
            let pid = std::process::id();
            assert!(read_start_token(pid)
                .and_then(|token| StartKey::parse(&token))
                .is_some());
            assert!(process_rss_bytes(pid).is_some_and(|bytes| bytes > 0));
        }
    }

    fn run_fixture_child() {
        let mut resident = vec![0u8; 32 * 1024 * 1024];
        for byte in resident.iter_mut().step_by(4096) {
            *byte = 1;
        }
        let until = Instant::now() + Duration::from_millis(40);
        while Instant::now() < until {
            std::hint::black_box(&resident);
        }
        println!("CAIRN_PROCMEM_READY");
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        std::hint::black_box(resident);
    }

    struct Fixture(Child);
    impl Fixture {
        fn spawn() -> Self {
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "platform::windows::tests::child_fixture",
                    "--nocapture",
                ])
                .env("CAIRN_PROCMEM_WINDOWS_FIXTURE", "1")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let mut stdout = BufReader::new(child.stdout.take().unwrap());
            let mut line = String::new();
            loop {
                assert_ne!(
                    stdout.read_line(&mut line).unwrap(),
                    0,
                    "fixture exited before readiness: {line}"
                );
                if line.contains("CAIRN_PROCMEM_READY") {
                    break;
                }
                line.clear();
            }
            // Keep the pipe open for libtest's final result output.
            child.stdout = Some(stdout.into_inner());
            Self(child)
        }

        fn finish(&mut self) {
            drop(self.0.stdin.take());
            assert!(self.0.wait().unwrap().success());
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn start_token_capture_reads_one_handle_without_enumerating_the_machine() {
        let pid = std::process::id();
        let expected = times(&open(pid).expect("own query handle"))
            .expect("own creation time")
            .start
            .token();
        let before = SNAPSHOT_READS.with(|reads| reads.get());
        assert_eq!(crate::platform::read_start_token(pid), Some(expected));
        assert_eq!(
            SNAPSHOT_READS.with(|reads| reads.get()),
            before,
            "identity capture must not take a machine snapshot"
        );
    }

    #[test]
    fn native_parent_child_identity_rss_cpu_and_exit() {
        let parent = read_unreaped_child(std::process::id()).unwrap();
        let mut child = Fixture::spawn();
        let pid = child.0.id();
        let KernelRead::Live(identity) = read_kernel_state(pid) else {
            panic!("live child unreadable")
        };
        assert_eq!(identity.parent_pid, parent.pid);
        assert!(parent.start <= identity.start);
        assert_eq!(identity.process_group_id, 0);
        assert_eq!(identity.session_id, None);
        assert_eq!(read_unreaped_child(pid).unwrap().start, identity.start);
        assert_eq!(
            crate::platform::read_start_token(pid),
            Some(identity.start.token())
        );
        assert_eq!(
            StartKey::parse(&identity.start.token()),
            Some(identity.start)
        );
        assert!(!identity.start.token().contains(':'));
        assert!(process_rss_bytes(pid).unwrap() > 16 * 1024 * 1024);
        let mut census_data = Census::default();
        assert!(census(&mut census_data));
        let observation = census_data
            .row(pid)
            .expect("child missing from native census");
        assert_eq!(observation.process, identity);
        assert!(observation.resident_bytes.unwrap() > 16 * 1024 * 1024);
        assert!(observation.cpu_ms.unwrap() > 0);
        assert_eq!(observation.storage_io_bytes, None);
        assert!(!observation.uninterruptible);
        child.finish();
        assert_eq!(read_kernel_state(pid), KernelRead::Gone);
        assert_eq!(read_unreaped_child(pid).unwrap().start, identity.start);
        assert_eq!(
            crate::platform::read_start_token(pid),
            Some(identity.start.token())
        );
        assert_eq!(process_rss_bytes(pid), None);
    }

    #[test]
    fn native_shared_sampler_measures_verified_parent_child() {
        use crate::{
            BatchObserver, ProcessTreeSampler, SamplingStatus, TickBatch, TickClocks, WatchOptions,
        };
        use std::sync::Arc;
        struct Observer;
        impl BatchObserver for Observer {
            fn tick(&self, _: &TickBatch<'_>) {}
        }
        let mut child = Fixture::spawn();
        let parent = read_unreaped_child(std::process::id()).unwrap().identity();
        let child_identity = read_unreaped_child(child.0.id()).unwrap().identity();
        let sampler = ProcessTreeSampler::with_source(
            Duration::from_secs(1),
            Arc::new(Observer),
            Arc::new(census),
            Arc::new(|| TickClocks {
                monotonic_ms: 1,
                unix_ms: 1,
            }),
            false,
        );
        let watch = sampler.watch(parent, WatchOptions::default()).unwrap();
        let mut out = Census::default();
        sampler.sample_once(&mut out);
        let SamplingStatus::Measured(reading) = watch.latest() else {
            panic!("native parent tree was not measured")
        };
        assert!(reading.resident_bytes > 16 * 1024 * 1024);
        assert!(reading.members_read >= 2);
        assert!(!reading.incomplete);
        assert!(watch.members().contains(&child_identity));
        assert!(watch.members().contains(&parent));
        assert_eq!(watch.members(), sampler.tree_members(parent));
        let wrong = crate::ProcessIdentity {
            start_time: StartKey(child_identity.start_time.0 + 1, 0),
            ..child_identity
        };
        let unverified = sampler.watch(wrong, WatchOptions::default()).unwrap();
        sampler.sample_once(&mut out);
        assert_eq!(unverified.latest(), SamplingStatus::GoneOrReused);
        assert!(unverified.members().is_empty());
        child.finish();
        sampler.sample_once(&mut out);
        assert!(!watch.members().contains(&child_identity));
    }

    #[test]
    fn unknown_is_not_departure() {
        use windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED;
        assert_eq!(
            classify_open_error(ERROR_ACCESS_DENIED),
            KernelRead::Unreadable
        );
        assert_eq!(
            classify_open_error(ERROR_INVALID_PARAMETER),
            KernelRead::Gone
        );
        assert_eq!(classify_open_error(123456), KernelRead::Unreadable);
        assert_eq!(read_kernel_state(0), KernelRead::Unreadable);
    }

    fn put_list(buffer: &mut [usize], assigned: u32, pids: &[usize]) {
        let header = buffer.as_mut_ptr().cast::<u32>();
        unsafe {
            *header = assigned;
            *header.add(1) = pids.len() as u32;
            std::ptr::copy_nonoverlapping(
                pids.as_ptr(),
                buffer.as_mut_ptr().cast::<u8>().add(8).cast::<usize>(),
                pids.len(),
            );
        }
    }

    #[test]
    fn job_buffer_retries_and_preserves_partial_evidence() {
        let mut buffer = Vec::new();
        let mut calls = 0;
        let (pids, complete) = query_job_pids(&mut buffer, 1, |buffer, bytes| {
            calls += 1;
            if calls == 1 {
                assert_eq!(bytes as usize, 8 + size_of::<usize>());
                put_list(buffer, 3, &[11]);
                Err(ERROR_MORE_DATA)
            } else {
                assert!(bytes as usize >= 8 + 3 * size_of::<usize>());
                put_list(buffer, 3, &[11, 12, 13]);
                Ok(())
            }
        });
        assert!(complete);
        assert_eq!(calls, 2);
        assert_eq!(pids, vec![11, 12, 13]);
        let mut calls = 0;
        let (pids, complete) = query_job_pids(&mut Vec::new(), 1, |buffer, _| {
            calls += 1;
            put_list(buffer, 1_000_000, &[42]);
            Err(ERROR_MORE_DATA)
        });
        assert!(!complete);
        assert!(calls <= 8);
        assert_eq!(pids, vec![42]);
        let (pids, complete) = query_job_pids(&mut Vec::new(), 1, |_, _| Err(5));
        assert!(!complete);
        assert!(pids.is_empty());
    }

    #[test]
    fn job_buffer_rejects_invalid_count() {
        let (pids, complete) = query_job_pids(&mut Vec::new(), 1, |buffer, _| {
            let header = buffer.as_mut_ptr().cast::<u32>();
            unsafe {
                *header = 2;
                *header.add(1) = 2;
            }
            Ok(())
        });
        assert!(!complete);
        assert!(pids.is_empty());
    }

    #[test]
    fn native_tree_job_query_duplication_and_handle_release() {
        let job = owned(unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) }).unwrap();
        let mut child_a = Fixture::spawn();
        let mut child_b = Fixture::spawn();
        for child in [&child_a, &child_b] {
            assert_ne!(
                unsafe { AssignProcessToJobObject(job.as_raw_handle(), child.0.as_raw_handle()) },
                0,
                "assign native fixture to dedicated nested tree job: {}",
                io::Error::last_os_error()
            );
        }
        let mut buffer = Vec::new();
        let mut queries = 0;
        let (pids, complete) = query_job_pids(&mut buffer, 1, |buffer, bytes| {
            queries += 1;
            if unsafe {
                QueryInformationJobObject(
                    job.as_raw_handle(),
                    JobObjectBasicProcessIdList,
                    buffer.as_mut_ptr().cast(),
                    bytes,
                    null_mut(),
                )
            } == 0
            {
                Err(unsafe { GetLastError() })
            } else {
                Ok(())
            }
        });
        assert!(complete);
        assert!(
            queries >= 2,
            "one PID buffer must retry for two native members"
        );
        assert!(pids.contains(&child_a.0.id()));
        assert!(pids.contains(&child_b.0.id()));
        let capability = unsafe { TreeJob::from_raw_handle(job.as_raw_handle()) }.unwrap();
        drop(job);
        let mut out = Census::default();
        assert!(capability.census(&mut out));
        assert!(!out.enumeration_incomplete);
        assert!(out.unreadable.is_empty());
        assert_eq!(out.rows.len(), 2);
        for child in [&child_a, &child_b] {
            let member = out.row(child.0.id()).unwrap();
            assert_eq!(
                member.process.start,
                read_unreaped_child(child.0.id()).unwrap().start
            );
            assert!(member.resident_bytes.unwrap() > 0);
        }
        fn handle_count() -> u32 {
            let mut count = 0;
            assert_ne!(
                unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) },
                0
            );
            count
        }
        let before = handle_count();
        for _ in 0..64 {
            let duplicate =
                unsafe { TreeJob::from_raw_handle(capability.handle.as_raw_handle()) }.unwrap();
            let mut out = Census::default();
            assert!(duplicate.census(&mut out));
            assert!(matches!(
                read_kernel_state(child_a.0.id()),
                KernelRead::Live(_)
            ));
            assert!(process_rss_bytes(child_a.0.id()).is_some());
        }
        assert_eq!(
            handle_count(),
            before,
            "query/snapshot/process/job handles leaked"
        );
        child_a.finish();
        child_b.finish();
        let mut ended = Census::default();
        assert!(capability.census(&mut ended));
        assert!(ended.rows.is_empty());
        assert!(unsafe { TreeJob::from_raw_handle(null_mut()) }.is_err());
    }
}
