//! Who is calling, told by the kernel: a Unix socket's peer, identified by its
//! process lineage.
//!
//! A credential in a process's launch environment is readable by every process
//! of the same user (`KERN_PROCARGS2` on macOS, `/proc/<pid>/environ` on
//! Linux), so a callback that authenticates by one is answerable by anyone who
//! reads it. A Unix socket needs no credential: the kernel reports which
//! process connected, and a server that recorded the processes it spawned can
//! walk the caller's parent chain to one of them. That spawned process is a
//! *root*, and the root decides what the caller speaks for. The socket's path
//! is not secret; the lineage check is the access control.
//!
//! The rules, which the security thread set as conditions C1 to C4 (cairn/5949,
//! cairn/6153):
//!
//! - **An identity that cannot be recycled (C1).** A pid alone names nothing
//!   once its process is reaped. On macOS a process is its `p_uniqueid`, and
//!   each child records its parent's as `p_puniqueid`, so every parent link is
//!   checked against it. On Linux a process is its pid and start time, and a
//!   parent may not have started after its child. The peer is identified again
//!   after the walk, and a peer that changed is refused.
//! - **Registered before it can be answered for (C2).** The spawner opens a
//!   [`PendingRoot`] before it spawns and records the root from the unreaped
//!   child. A caller whose walk finds no root while an earlier registration is
//!   still open waits for it, bounded, then walks once more. So a child whose
//!   first act is to connect is admitted, and nothing is granted before its
//!   root is recorded.
//! - **Fail closed (C3).** A walk that reaches pid 1, an ancestor it cannot
//!   read, a broken parent link or its depth limit, without finding a root, is
//!   refused. A daemonized or reparented process loses access by design.
//! - **The root decides (C4).** What a root maps to is the server's business;
//!   this module only answers which root, if any, a peer descends from.
//!
//! What a lineage check cannot prove is integrity: a same-user process that
//! can inject code into a registered lineage (Linux with
//! `kernel.yama.ptrace_scope` at 0) can make it connect. And attestation is per
//! connection, so a process that passes its connected socket to another
//! (`SCM_RIGHTS`) lends its own authority, as handing over a bearer would.
//! Both are named residuals in `docs/identity.md`.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// The variable naming the callback socket a process reaches its server on.
/// Not a secret: the path grants nothing without a recorded lineage.
pub const CALLBACK_SOCKET_ENV: &str = "CAIRN_CALLBACK_SOCKET";

/// How far up a parent chain a walk goes before it gives up.
const MAX_DEPTH: usize = 64;

/// How long a rootless caller waits for registrations already in flight.
pub const DEFAULT_PENDING_WAIT: Duration = Duration::from_secs(5);

/// One process lifetime: a pid together with what the kernel never repeats
/// for it (macOS `p_uniqueid`; Linux start time in clock ticks).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LineageId {
    pub pid: u32,
    pub key: u64,
}

/// What the kernel says about one process, for a walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcessRecord {
    id: LineageId,
    parent_pid: u32,
    /// macOS: the parent's `p_uniqueid` as the child recorded it.
    parent_key: Option<u64>,
    /// macOS: the pid version an audit token carries.
    pid_version: Option<i32>,
}

impl ProcessRecord {
    /// Whether `parent` is the process this one was born to (macOS), or at
    /// least is no younger than it (Linux), so a recycled parent pid is never
    /// walked through.
    fn is_child_of(&self, parent: &ProcessRecord) -> bool {
        if self.parent_pid != parent.id.pid {
            return false;
        }
        match self.parent_key {
            Some(recorded) => recorded == parent.id.key,
            None => parent.id.key <= self.id.key,
        }
    }
}

/// Why a caller was refused. Every message names pids and reasons, never a
/// value, since these reach logs and the caller.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    #[error("the socket's peer could not be identified: {0}")]
    PeerUnreadable(String),
    #[error("the socket's peer is no longer the process that connected")]
    PeerChanged,
    #[error(
        "the caller descends from no process Cairn started (its ancestry ends at pid {reached}); \
         a daemonized or reparented process has no run to speak for"
    )]
    NoRoot { reached: u32 },
    #[error("ancestor pid {pid} of the caller could not be read: {reason}")]
    AncestorUnreadable { pid: u32, reason: String },
    #[error("pid {pid}'s recorded parent is no longer the process at that pid")]
    BrokenLink { pid: u32 },
    #[error("the caller's ancestry is deeper than {MAX_DEPTH} processes")]
    DepthLimit,
    #[error(
        "the caller descends from no recorded process, and a registration still in flight did \
         not finish within {0:?}"
    )]
    PendingTimeout(Duration),
    #[error("process lineage cannot be attested on this platform")]
    Unsupported,
}

impl Refusal {
    /// Whether a registration still in flight could change this answer.
    fn awaits_registration(&self) -> bool {
        matches!(
            self,
            Self::NoRoot { .. } | Self::AncestorUnreadable { .. } | Self::DepthLimit
        )
    }
}

/// The process at the other end of a Unix socket, as the kernel reported it
/// when it connected.
#[derive(Debug)]
pub struct Peer {
    pid: u32,
    #[cfg(target_os = "macos")]
    pid_version: i32,
    #[cfg(target_os = "linux")]
    pidfd: std::os::fd::OwnedFd,
}

impl Peer {
    /// The peer of the connected socket `socket`. Read at once on accept: the
    /// macOS audit token and the Linux pidfd pin the process that connected.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    pub fn of(socket: std::os::fd::BorrowedFd<'_>) -> Result<Self, Refusal> {
        platform::peer(socket)
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    pub fn of(_socket: std::os::fd::BorrowedFd<'_>) -> Result<Self, Refusal> {
        Err(Refusal::Unsupported)
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// The peer's record now, refused unless it is still the process that
    /// connected.
    fn record(&self) -> Result<ProcessRecord, Refusal> {
        #[cfg(target_os = "macos")]
        {
            let record = platform::read_record(self.pid).map_err(|_| Refusal::PeerChanged)?;
            if record.pid_version != Some(self.pid_version) {
                return Err(Refusal::PeerChanged);
            }
            Ok(record)
        }
        #[cfg(target_os = "linux")]
        {
            let record = platform::read_record(self.pid).map_err(|_| Refusal::PeerChanged)?;
            // Read after the record: a live pidfd means the record was this
            // process's, since the pid cannot have been reused while it lives.
            if !platform::pidfd_alive(&self.pidfd) {
                return Err(Refusal::PeerChanged);
            }
            Ok(record)
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            Err(Refusal::Unsupported)
        }
    }
}

/// The identity of `pid` as a root is recorded: refused for a process that
/// is not this process's own live child.
pub fn child_identity(pid: u32) -> Result<LineageId, String> {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        let record = platform::read_record(pid)?;
        if record.parent_pid != std::process::id() {
            return Err(format!(
                "pid {pid} is not this process's child (its parent is pid {})",
                record.parent_pid
            ));
        }
        Ok(record.id)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = pid;
        Err(Refusal::Unsupported.to_string())
    }
}

/// Walk from `start` up its parents to the first process `lookup` names.
fn walk<R>(
    start: ProcessRecord,
    mut lookup: impl FnMut(&LineageId) -> Option<R>,
) -> Result<R, Refusal> {
    let mut node = start;
    for _ in 0..MAX_DEPTH {
        if let Some(root) = lookup(&node.id) {
            return Ok(root);
        }
        if node.parent_pid <= 1 {
            return Err(Refusal::NoRoot {
                reached: node.parent_pid,
            });
        }
        let parent = read_ancestor(node.parent_pid)?;
        if !node.is_child_of(&parent) {
            return Err(Refusal::BrokenLink { pid: node.id.pid });
        }
        node = parent;
    }
    Err(Refusal::DepthLimit)
}

fn read_ancestor(pid: u32) -> Result<ProcessRecord, Refusal> {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        platform::read_record(pid).map_err(|reason| Refusal::AncestorUnreadable { pid, reason })
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = pid;
        Err(Refusal::Unsupported)
    }
}

struct State<R> {
    roots: HashMap<LineageId, (u64, R)>,
    /// Registrations opened and not yet closed, by sequence number.
    open: BTreeSet<u64>,
    next: u64,
}

struct Inner<R> {
    state: Mutex<State<R>>,
    settled: Condvar,
}

/// The processes one server spawned and answers for, each mapped to what its
/// descendants speak for. One map serves the runner and every executor.
pub struct LineageRoots<R> {
    inner: Arc<Inner<R>>,
}

impl<R> Clone for LineageRoots<R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<R: Clone + Send + 'static> Default for LineageRoots<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: Clone + Send + 'static> LineageRoots<R> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    roots: HashMap::new(),
                    open: BTreeSet::new(),
                    next: 0,
                }),
                settled: Condvar::new(),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State<R>> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Open a registration, before spawning the process it will record. It
    /// closes when recorded, or when dropped on any failure path, so a spawn
    /// error, a panic or a cancelled future never leaves it open.
    pub fn pending(&self) -> PendingRoot<R> {
        let mut state = self.lock();
        let seq = state.next;
        state.next += 1;
        state.open.insert(seq);
        PendingRoot {
            roots: self.clone(),
            seq: Some(seq),
        }
    }

    fn close(&self, seq: u64) {
        self.lock().open.remove(&seq);
        self.inner.settled.notify_all();
    }

    fn lookup(&self, id: &LineageId) -> Option<R> {
        self.lock().roots.get(id).map(|(_, root)| root.clone())
    }

    /// How many roots are recorded.
    pub fn len(&self) -> usize {
        self.lock().roots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Which root `peer` descends from, and what it maps to.
    ///
    /// Blocking: a caller whose first walk finds no root waits up to `wait` for
    /// registrations opened before it began, then walks once more. Expiry is a
    /// refusal. Run it off an async executor's worker threads.
    pub fn attest(&self, peer: &Peer, wait: Duration) -> Result<R, Refusal> {
        self.attest_observed(peer, wait, |_| {})
    }

    /// [`Self::attest`], calling `step` with each process the walk visits.
    fn attest_observed(
        &self,
        peer: &Peer,
        wait: Duration,
        mut step: impl FnMut(&LineageId),
    ) -> Result<R, Refusal> {
        let (horizon, in_flight) = {
            let state = self.lock();
            (state.next, !state.open.is_empty())
        };
        let first = peer.record().and_then(|record| {
            walk(record, |id| {
                step(id);
                self.lookup(id)
            })
        });
        let root = match first {
            Ok(root) => root,
            Err(refusal) if !refusal.awaits_registration() => return Err(refusal),
            // Nothing was in flight when the walk began, so every root it
            // could descend from was already recorded.
            Err(refusal) if !in_flight => return Err(refusal),
            Err(_) => {
                if !self.await_registrations_before(horizon, wait) {
                    return Err(Refusal::PendingTimeout(wait));
                }
                walk(peer.record()?, |id| {
                    step(id);
                    self.lookup(id)
                })?
            }
        };
        // The walk took time; the peer it answered for must still be there.
        peer.record()?;
        Ok(root)
    }

    /// Wait until no registration opened before `horizon` is still open.
    fn await_registrations_before(&self, horizon: u64, wait: Duration) -> bool {
        let deadline = Instant::now() + wait;
        let mut state = self.lock();
        while state.open.range(..horizon).next().is_some() {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            state = self
                .inner
                .settled
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        true
    }
}

/// A registration opened before a spawn. Record the spawned child with
/// [`Self::record`]; dropping it unrecorded closes it as failed.
pub struct PendingRoot<R: Clone + Send + 'static> {
    roots: LineageRoots<R>,
    seq: Option<u64>,
}

impl<R: Clone + Send + 'static> PendingRoot<R> {
    /// Record `pid`, this process's unreaped child, as a root mapping to
    /// `root`. The identity is read from the child itself; a child that has
    /// already exited, or is not this process's, is refused, and the
    /// registration closes as failed so its descendants are refused too.
    pub fn record(mut self, pid: u32, root: R) -> Result<RootGuard<R>, String> {
        let id = child_identity(pid);
        let seq = self.seq.take().expect("a pending root is recorded once");
        let guard = id.map(|id| {
            self.roots.lock().roots.insert(id, (seq, root));
            RootGuard {
                roots: self.roots.clone(),
                id,
                seq,
            }
        });
        self.roots.close(seq);
        guard
    }
}

impl<R: Clone + Send + 'static> Drop for PendingRoot<R> {
    fn drop(&mut self) {
        if let Some(seq) = self.seq.take() {
            self.roots.close(seq);
        }
    }
}

/// A recorded root. Dropping it removes the root, after which its
/// descendants are refused.
pub struct RootGuard<R: Clone + Send + 'static> {
    roots: LineageRoots<R>,
    id: LineageId,
    seq: u64,
}

impl<R: Clone + Send + 'static> RootGuard<R> {
    pub fn id(&self) -> LineageId {
        self.id
    }
}

impl<R: Clone + Send + 'static> std::fmt::Debug for RootGuard<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RootGuard").field("id", &self.id).finish()
    }
}

impl<R: Clone + Send + 'static> Drop for RootGuard<R> {
    fn drop(&mut self) {
        let mut state = self.roots.lock();
        // A guard removes only its own entry, never a later root that a
        // recycled identity could in principle have put under the same key.
        if state
            .roots
            .get(&self.id)
            .is_some_and(|(seq, _)| *seq == self.seq)
        {
            state.roots.remove(&self.id);
        }
    }
}

/// Where a server named `name` listens under the Cairn home `home`: a short
/// path in an owner-only directory, since macOS caps a socket path at 104
/// bytes. A home too long for that gets a directory under `/tmp` named for the
/// user and the home instead.
pub fn socket_path(home: &Path, name: &str) -> PathBuf {
    // sun_path holds 104 bytes on macOS, 108 on Linux, NUL included.
    const LIMIT: usize = 103;
    let preferred = home.join("sock").join(format!("{name}.sock"));
    if preferred.as_os_str().len() <= LIMIT {
        return preferred;
    }
    use sha2::Digest as _;
    let digest = sha2::Sha256::digest(home.as_os_str().as_encoded_bytes());
    #[cfg(unix)]
    let user = unsafe { libc::getuid() };
    #[cfg(not(unix))]
    let user = 0;
    PathBuf::from(format!("/tmp/cairn-{user}"))
        .join(format!("{}-{name}.sock", hex::encode(&digest[..6])))
}

/// Bind a listener at `path`: its directory created owner-only (and refused
/// if another user owns it), a stale socket from a previous process removed,
/// anything else at the path left alone and refused.
#[cfg(unix)]
pub fn bind(path: &Path) -> Result<std::os::unix::net::UnixListener, String> {
    use std::os::unix::fs::{
        DirBuilderExt as _, FileTypeExt as _, MetadataExt as _, PermissionsExt as _,
    };

    let dir = path
        .parent()
        .ok_or_else(|| format!("{} has no directory", path.display()))?;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .map_err(|error| format!("create {}: {error}", dir.display()))?;
    let meta = std::fs::symlink_metadata(dir)
        .map_err(|error| format!("inspect {}: {error}", dir.display()))?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::getuid() } {
        return Err(format!(
            "{} is not a directory this user owns, so no callback socket is placed there",
            dir.display()
        ));
    }
    if meta.mode() & 0o077 != 0 {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .map_err(|error| format!("restrict {}: {error}", dir.display()))?;
    }
    match std::fs::symlink_metadata(path) {
        Ok(existing) if existing.file_type().is_socket() => std::fs::remove_file(path)
            .map_err(|error| format!("remove stale {}: {error}", path.display()))?,
        Ok(_) => {
            return Err(format!(
                "{} exists and is not a socket; it is left alone",
                path.display()
            ))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("inspect {}: {error}", path.display())),
    }
    std::os::unix::net::UnixListener::bind(path)
        .map_err(|error| format!("bind {}: {error}", path.display()))
}

#[cfg(target_os = "macos")]
mod platform {
    use std::os::fd::{AsRawFd, BorrowedFd};

    use super::{Peer, ProcessRecord, Refusal};

    /// `struct proc_uniqidentifierinfo`, from the SDK's `sys/proc_info.h`.
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct UniqIdentifierInfo {
        uuid: [u8; 16],
        unique_id: u64,
        parent_unique_id: u64,
        id_version: i32,
        orig_parent_id_version: i32,
        reserved: [u64; 2],
    }

    /// `struct proc_bsdinfowithuniqid`: the parent pid and both unique ids in
    /// one read, so they describe one moment.
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct BsdInfoWithUniqId {
        bsd: libc::proc_bsdinfo,
        uniq: UniqIdentifierInfo,
    }

    const PROC_PIDT_BSDINFOWITHUNIQID: libc::c_int = 18;
    const SOL_LOCAL: libc::c_int = 0;
    const LOCAL_PEERTOKEN: libc::c_int = 0x006;

    /// A live process's record. A zombie is not readable this way, so an
    /// exited process is never a link or a root.
    pub(super) fn read_record(pid: u32) -> Result<ProcessRecord, String> {
        let mut info: BsdInfoWithUniqId = unsafe { std::mem::zeroed() };
        let size = std::mem::size_of::<BsdInfoWithUniqId>() as libc::c_int;
        // SAFETY: the kernel fills a caller-owned struct of the size it is told.
        let read = unsafe {
            libc::proc_pidinfo(
                pid as libc::c_int,
                PROC_PIDT_BSDINFOWITHUNIQID,
                0,
                (&mut info as *mut BsdInfoWithUniqId).cast(),
                size,
            )
        };
        if read != size {
            return Err(format!("proc_pidinfo: {}", std::io::Error::last_os_error()));
        }
        Ok(ProcessRecord {
            id: super::LineageId {
                pid,
                key: info.uniq.unique_id,
            },
            parent_pid: info.bsd.pbi_ppid,
            parent_key: Some(info.uniq.parent_unique_id),
            pid_version: Some(info.uniq.id_version),
        })
    }

    pub(super) fn peer(socket: BorrowedFd<'_>) -> Result<Peer, Refusal> {
        let mut token = [0u32; 8];
        let mut len = std::mem::size_of_val(&token) as libc::socklen_t;
        // SAFETY: `token` is a caller-owned `audit_token_t`-sized buffer.
        let status = unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                SOL_LOCAL,
                LOCAL_PEERTOKEN,
                token.as_mut_ptr().cast(),
                &mut len,
            )
        };
        if status != 0 || len as usize != std::mem::size_of_val(&token) {
            return Err(Refusal::PeerUnreadable(format!(
                "LOCAL_PEERTOKEN: {}",
                std::io::Error::last_os_error()
            )));
        }
        // `audit_token_to_pid` and `audit_token_to_pidversion`.
        Ok(Peer {
            pid: token[5],
            pid_version: token[7] as i32,
        })
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};

    use super::{Peer, ProcessRecord, Refusal};

    /// `SO_PEERPIDFD`, Linux 6.5: a pidfd for the process that connected.
    const SO_PEERPIDFD: libc::c_int = 77;

    pub(super) fn read_record(pid: u32) -> Result<ProcessRecord, String> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .map_err(|error| format!("read /proc/{pid}/stat: {error}"))?;
        parse_stat(pid, &stat).ok_or_else(|| format!("/proc/{pid}/stat is malformed or a zombie's"))
    }

    /// Fields counted from after the command's final `)`, which may itself
    /// hold spaces and parentheses: 0 the state, 1 the parent, 19 the start
    /// time. A zombie has no live lineage and is no record.
    pub(super) fn parse_stat(pid: u32, stat: &str) -> Option<ProcessRecord> {
        let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
        if *fields.first()? == "Z" {
            return None;
        }
        Some(ProcessRecord {
            id: super::LineageId {
                pid,
                key: fields.get(19)?.parse().ok()?,
            },
            parent_pid: fields.get(1)?.parse().ok()?,
            parent_key: None,
            pid_version: None,
        })
    }

    pub(super) fn peer(socket: BorrowedFd<'_>) -> Result<Peer, Refusal> {
        let fd = socket.as_raw_fd();
        let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        // SAFETY: `cred` is a caller-owned `ucred`.
        let status = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut cred as *mut libc::ucred).cast(),
                &mut len,
            )
        };
        if status != 0 || cred.pid <= 0 {
            return Err(Refusal::PeerUnreadable(format!(
                "SO_PEERCRED: {}",
                std::io::Error::last_os_error()
            )));
        }
        let pid = cred.pid as u32;
        let mut pidfd: libc::c_int = -1;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: `pidfd` is a caller-owned int.
        let status = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                SO_PEERPIDFD,
                (&mut pidfd as *mut libc::c_int).cast(),
                &mut len,
            )
        };
        let pidfd = if status == 0 && pidfd >= 0 {
            pidfd
        } else {
            // Before 6.5 the pidfd is opened by pid, a moment after connect;
            // a peer that exited and was recycled in that moment is the one
            // gap, named in `docs/identity.md`.
            // SAFETY: `pidfd_open` takes a pid and flags and returns a new fd.
            let opened = unsafe { libc::syscall(libc::SYS_pidfd_open, cred.pid, 0) };
            if opened < 0 {
                return Err(Refusal::PeerUnreadable(format!(
                    "pidfd_open: {}",
                    std::io::Error::last_os_error()
                )));
            }
            opened as libc::c_int
        };
        // SAFETY: a fresh descriptor nothing else owns.
        let pidfd = unsafe { OwnedFd::from_raw_fd(pidfd) };
        Ok(Peer { pid, pidfd })
    }

    /// Whether the process `pidfd` pins is still running.
    pub(super) fn pidfd_alive(pidfd: &OwnedFd) -> bool {
        // SAFETY: signal 0 only checks; the pidfd is open.
        let sent = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                pidfd.as_raw_fd(),
                0,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        sent == 0
    }
}

#[cfg(all(test, any(target_os = "macos", target_os = "linux")))]
mod tests {
    use std::os::fd::AsFd;
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::time::Duration;

    use super::*;

    struct Socket {
        _dir: tempfile::TempDir,
        path: PathBuf,
        listener: UnixListener,
    }

    fn socket() -> Socket {
        // A short directory: a test temp dir can exceed macOS's 104 bytes.
        let dir = tempfile::Builder::new()
            .prefix("ln")
            .tempdir_in("/tmp")
            .unwrap();
        let path = dir.path().join("s");
        let listener = bind(&path).unwrap();
        Socket {
            _dir: dir,
            path,
            listener,
        }
    }

    /// What a forked process does: only async-signal-safe calls.
    #[derive(Clone, Copy)]
    enum Act {
        /// Connect, then hold the connection until the server hangs up.
        Connect,
        /// Fork a grandchild that sleeps, connects and holds; exit at once.
        OrphanThenConnect,
        /// Fork a grandchild that connects at once and holds; then wait for it.
        ParentOfConnector,
    }

    fn connect_and_hold(path: &std::ffi::CStr) -> ! {
        unsafe {
            let fd = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0);
            let mut addr: libc::sockaddr_un = std::mem::zeroed();
            addr.sun_family = libc::AF_UNIX as _;
            for (slot, byte) in addr.sun_path.iter_mut().zip(path.to_bytes()) {
                *slot = *byte as libc::c_char;
            }
            let len = std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t;
            if libc::connect(fd, (&addr as *const libc::sockaddr_un).cast(), len) != 0 {
                libc::_exit(3);
            }
            let mut byte = 0u8;
            libc::read(fd, (&mut byte as *mut u8).cast(), 1);
            libc::_exit(0);
        }
    }

    /// Fork a child that performs `act` against `path`. Answers its pid.
    fn fork(act: Act, path: &Path) -> u32 {
        let path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        // SAFETY: the child calls only async-signal-safe functions.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid > 0 {
            return pid as u32;
        }
        match act {
            Act::Connect => connect_and_hold(&path),
            Act::OrphanThenConnect => unsafe {
                if libc::fork() == 0 {
                    libc::usleep(300_000);
                    connect_and_hold(&path);
                }
                libc::_exit(0);
            },
            Act::ParentOfConnector => unsafe {
                let grandchild = libc::fork();
                if grandchild == 0 {
                    connect_and_hold(&path);
                }
                let mut status = 0;
                libc::waitpid(grandchild, &mut status, 0);
                libc::_exit(0);
            },
        }
    }

    fn reap(pid: u32) {
        let mut status = 0;
        unsafe { libc::waitpid(pid as libc::pid_t, &mut status, 0) };
    }

    fn accept(socket: &Socket) -> (UnixStream, Peer) {
        let (stream, _) = socket.listener.accept().unwrap();
        let peer = Peer::of(stream.as_fd()).unwrap();
        (stream, peer)
    }

    const SHORT: Duration = Duration::from_millis(200);

    /// C1: a root recorded under an identity that is not the process at that
    /// pid now answers for nothing.
    #[test]
    fn a_root_recorded_with_a_mismatched_start_identity_is_refused() {
        let socket = socket();
        let roots = LineageRoots::<&str>::new();
        let child = fork(Act::ParentOfConnector, &socket.path);
        let real = child_identity(child).unwrap();
        roots.lock().roots.insert(
            LineageId {
                pid: real.pid,
                key: real.key.wrapping_add(1),
            },
            (u64::MAX, "stale"),
        );
        let (stream, peer) = accept(&socket);
        let refused = roots.attest(&peer, SHORT).unwrap_err();
        assert!(matches!(refused, Refusal::NoRoot { .. }), "{refused}");
        drop(stream);
        reap(child);
    }

    /// C1: a peer that is gone by the end of the walk is refused, even though
    /// the walk itself found its root.
    #[test]
    fn a_peer_that_changes_during_the_walk_is_refused() {
        let socket = socket();
        let roots = LineageRoots::<&str>::new();
        let pending = roots.pending();
        let child = fork(Act::Connect, &socket.path);
        let _root = pending.record(child, "run-a").unwrap();
        let (stream, peer) = accept(&socket);
        let mut stream = Some(stream);
        let refused = roots
            .attest_observed(&peer, SHORT, |_| {
                // The connector exits and is reaped mid-walk.
                if let Some(stream) = stream.take() {
                    drop(stream);
                    reap(child);
                }
            })
            .unwrap_err();
        assert_eq!(refused, Refusal::PeerChanged);
    }

    /// C2: a child whose very first act is to connect is admitted, though its
    /// root is recorded only afterwards.
    #[test]
    fn a_child_that_connects_before_its_root_is_recorded_is_admitted() {
        let socket = socket();
        let roots = LineageRoots::<&str>::new();
        let pending = roots.pending();
        let child = fork(Act::Connect, &socket.path);
        let (stream, peer) = accept(&socket);
        let recorder = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            pending.record(child, "run-a").unwrap()
        });
        assert_eq!(roots.attest(&peer, DEFAULT_PENDING_WAIT), Ok("run-a"));
        let _root = recorder.join().unwrap();
        drop(stream);
        reap(child);
    }

    /// Condition 1: the wait for a registration is bounded, and its expiry is
    /// a refusal, never an admission.
    #[test]
    fn a_registration_that_never_finishes_ends_in_refusal() {
        let socket = socket();
        let roots = LineageRoots::<&str>::new();
        let _stuck = roots.pending();
        let child = fork(Act::Connect, &socket.path);
        let (stream, peer) = accept(&socket);
        let started = Instant::now();
        assert_eq!(
            roots.attest(&peer, SHORT),
            Err(Refusal::PendingTimeout(SHORT))
        );
        assert!(started.elapsed() >= SHORT);
        drop(stream);
        reap(child);
    }

    /// Condition 3: a registration dropped on a failure path closes, so a
    /// rootless caller is refused at once rather than after the bound.
    #[test]
    fn a_dropped_registration_closes_and_never_delays_a_refusal() {
        let socket = socket();
        let roots = LineageRoots::<&str>::new();
        let failed = roots.pending();
        let panicked = std::thread::spawn({
            let roots = roots.clone();
            move || {
                let _held = roots.pending();
                panic!("the spawner panicked mid-spawn");
            }
        });
        assert!(panicked.join().is_err());
        drop(failed);
        let child = fork(Act::Connect, &socket.path);
        let (stream, peer) = accept(&socket);
        let started = Instant::now();
        let refused = roots.attest(&peer, Duration::from_secs(30)).unwrap_err();
        assert!(matches!(refused, Refusal::NoRoot { .. }), "{refused}");
        assert!(started.elapsed() < Duration::from_secs(5));
        drop(stream);
        reap(child);
    }

    /// C3 and condition 2: a child that forks and exits before its root is
    /// recorded leaves no root, and its orphaned grandchild is refused.
    #[test]
    fn an_orphan_whose_root_exited_before_recording_is_refused() {
        let socket = socket();
        let roots = LineageRoots::<&str>::new();
        let pending = roots.pending();
        let child = fork(Act::OrphanThenConnect, &socket.path);
        // Let the child exit (a zombie until reaped) before recording.
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            pending.record(child, "run-a").is_err(),
            "a zombie root was recorded"
        );
        reap(child);
        let (stream, peer) = accept(&socket);
        let refused = roots.attest(&peer, SHORT).unwrap_err();
        assert!(
            matches!(refused, Refusal::NoRoot { .. } | Refusal::BrokenLink { .. }),
            "{refused}"
        );
        drop(stream);
    }

    /// C3: a double-forked process whose recorded parent has exited has been
    /// reparented, and its root no longer answers for it.
    #[test]
    fn a_reparented_descendant_of_a_recorded_root_is_refused() {
        let socket = socket();
        let roots = LineageRoots::<&str>::new();
        let pending = roots.pending();
        let child = fork(Act::OrphanThenConnect, &socket.path);
        let _root = pending.record(child, "run-a");
        reap(child);
        let (stream, peer) = accept(&socket);
        let refused = roots.attest(&peer, SHORT).unwrap_err();
        assert!(
            matches!(refused, Refusal::NoRoot { .. } | Refusal::BrokenLink { .. }),
            "{refused}"
        );
        drop(stream);
    }

    /// C4 and C6: with roots for two runs, a descendant of one is attested as
    /// that run and never the other, and a process outside both gets nothing.
    #[test]
    fn a_descendant_is_attested_as_its_own_root_and_an_outsider_as_none() {
        let socket = socket();
        let roots = LineageRoots::<&str>::new();
        let (pending_a, pending_b) = (roots.pending(), roots.pending());
        let a = fork(Act::ParentOfConnector, &socket.path);
        let _root_a = pending_a.record(a, "run-a").unwrap();
        let (stream_a, peer_a) = accept(&socket);
        let b = fork(Act::ParentOfConnector, &socket.path);
        let _root_b = pending_b.record(b, "run-b").unwrap();
        let (stream_b, peer_b) = accept(&socket);
        assert_eq!(roots.attest(&peer_a, SHORT), Ok("run-a"));
        assert_eq!(roots.attest(&peer_b, SHORT), Ok("run-b"));

        let outsider = UnixStream::connect(&socket.path).unwrap();
        let (server_side, _) = socket.listener.accept().unwrap();
        let peer = Peer::of(server_side.as_fd()).unwrap();
        assert_eq!(peer.pid(), std::process::id());
        // Under an operator's terminal the walk can meet a root-owned
        // ancestor (`login`) it may not read before it reaches pid 1.
        assert!(matches!(
            roots.attest(&peer, SHORT),
            Err(Refusal::NoRoot { .. } | Refusal::AncestorUnreadable { .. })
        ));
        drop((stream_a, stream_b, outsider));
        reap(a);
        reap(b);
    }

    /// A dropped root answers for nothing more.
    #[test]
    fn a_removed_root_admits_nothing() {
        let socket = socket();
        let roots = LineageRoots::<&str>::new();
        let pending = roots.pending();
        let child = fork(Act::Connect, &socket.path);
        let root = pending.record(child, "run-a").unwrap();
        let (stream, peer) = accept(&socket);
        drop(root);
        assert!(roots.is_empty());
        assert!(matches!(
            roots.attest(&peer, SHORT),
            Err(Refusal::NoRoot { .. })
        ));
        drop(stream);
        reap(child);
    }

    #[test]
    fn only_a_child_of_this_process_can_be_recorded() {
        let roots = LineageRoots::<&str>::new();
        assert!(roots.pending().record(1, "init").is_err());
        assert!(roots.pending().record(std::process::id(), "self").is_err());
    }

    #[test]
    fn a_socket_path_fits_the_kernel_limit_whatever_the_home() {
        let short = socket_path(Path::new("/Users/u/.cairn"), "runner");
        assert_eq!(short, Path::new("/Users/u/.cairn/sock/runner.sock"));
        let long_home = PathBuf::from(format!("/Users/u/{}", "x".repeat(120)));
        let long = socket_path(&long_home, "runner");
        assert!(long.as_os_str().len() <= 103, "{}", long.display());
        assert_ne!(long, socket_path(&long_home.join("y"), "runner"));
    }

    #[test]
    fn binding_replaces_a_stale_socket_and_leaves_anything_else() {
        let dir = tempfile::Builder::new()
            .prefix("lb")
            .tempdir_in("/tmp")
            .unwrap();
        let path = dir.path().join("sock").join("r.sock");
        drop(bind(&path).unwrap());
        let _again = bind(&path).unwrap();
        let file = dir.path().join("sock").join("f");
        std::fs::write(&file, b"x").unwrap();
        assert!(bind(&file).is_err());
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_stat_line_with_parentheses_in_its_command_parses() {
        let stat = "42 (a (b) c) S 7 42 42 0 -1 4194304 0 0 0 0 0 0 0 0 20 0 1 0 98765 0 0";
        let record = platform::parse_stat(42, stat).unwrap();
        assert_eq!(record.parent_pid, 7);
        assert_eq!(record.id.key, 98765);
        assert!(platform::parse_stat(42, &stat.replace(") S", ") Z")).is_none());
    }
}
