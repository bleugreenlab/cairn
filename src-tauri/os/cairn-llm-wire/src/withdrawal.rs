//! Calls that the owner of the endpoint they reach can cut off.
//!
//! Cairn stops one of its own inference deployments by withdrawing it: no new
//! call is routed there, and every call already in flight is cut off, its
//! connection closed, before the server is asked to exit. A server whose
//! graceful shutdown waits for its open requests (oMLX, through uvicorn, with
//! no timeout) would otherwise hold the stop for as long as its longest
//! generation, and such a server aborts a generation as soon as its client's
//! connection closes.
//!
//! A blocking reqwest call cannot be interrupted, because its request runs on
//! a runtime its caller cannot reach. So a call that may be cut off is sent by
//! [`send`] on the async client, on this module's runtime, and its blocking
//! caller reads the reply through a channel. Withdrawing drops the in-flight
//! future, which closes the connection. A call sent with no [`Withdrawal`]
//! takes the same path and is simply never cut off.

use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use reqwest::header::HeaderMap;
use reqwest::StatusCode;
use tokio::sync::{mpsc, oneshot, Notify};

/// What a cut-off call's reader is told, and the error a withdrawn send is.
const WITHDRAWN: &str = "the deployment is being stopped, so the call was cut off";

/// How many chunks of a reply wait for a slow reader before the connection
/// is read no further.
const BUFFERED_CHUNKS: usize = 16;

/// A flag that can be awaited.
#[derive(Default)]
struct Signal {
    set: AtomicBool,
    notify: Notify,
}

impl Signal {
    fn set(&self) {
        if !self.set.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    fn is_set(&self) -> bool {
        self.set.load(Ordering::SeqCst)
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Registered before the flag is read, so a set in between wakes it.
            notified.as_mut().enable();
            if self.is_set() {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Default)]
struct Inner {
    withdrawn: Signal,
    in_flight: AtomicUsize,
}

/// One endpoint's withdrawal, shared by every clone. Its owner withdraws it
/// once; from then on a call on it is refused before it is sent, and one in
/// flight is cut off.
#[derive(Clone, Default)]
pub struct Withdrawal(Arc<Inner>);

impl Withdrawal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Cut off every call on this withdrawal, and refuse every later one.
    pub fn withdraw(&self) {
        self.0.withdrawn.set();
    }

    pub fn is_withdrawn(&self) -> bool {
        self.0.withdrawn.is_set()
    }

    /// How many calls on it hold a connection or are opening one. A call
    /// that was cut off stops counting once its connection is closed, before
    /// its reader has heard.
    pub fn in_flight(&self) -> usize {
        self.0.in_flight.load(Ordering::SeqCst)
    }
}

/// Two withdrawals are equal when they are the same one.
impl PartialEq for Withdrawal {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Withdrawal {}

impl std::fmt::Debug for Withdrawal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Withdrawal")
            .field("withdrawn", &self.is_withdrawn())
            .field("in_flight", &self.in_flight())
            .finish()
    }
}

/// One call counted in its withdrawal's [`Withdrawal::in_flight`] while it
/// lives.
struct InFlight(Withdrawal);

impl InFlight {
    fn new(withdrawal: Withdrawal) -> Self {
        withdrawal.0.in_flight.fetch_add(1, Ordering::SeqCst);
        Self(withdrawal)
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0 .0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("cairn-wire-http")
            .enable_all()
            .build()
            .expect("the wire's HTTP runtime starts")
    })
}

/// Why [`send`] has no reply.
#[derive(Debug)]
pub enum SendError {
    /// The withdrawal was withdrawn before the reply's head arrived.
    Withdrawn,
    Request(reqwest::Error),
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Withdrawn => f.write_str(WITHDRAWN),
            Self::Request(error) => error.fmt(f),
        }
    }
}

/// A reply whose connection belongs to a task on this module's runtime. Its
/// body is read as a blocking [`Read`]; dropping it closes the connection.
pub struct Response {
    status: StatusCode,
    headers: HeaderMap,
    chunks: mpsc::Receiver<io::Result<Vec<u8>>>,
    chunk: Vec<u8>,
    read: usize,
    closed: Arc<Signal>,
}

impl Response {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// The whole body as text, any invalid UTF-8 replaced.
    pub fn text(mut self) -> io::Result<String> {
        let mut bytes = Vec::new();
        self.read_to_end(&mut bytes)?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}

impl Read for Response {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.read == self.chunk.len() {
            match self.chunks.blocking_recv() {
                Some(Ok(chunk)) => {
                    self.chunk = chunk;
                    self.read = 0;
                }
                Some(Err(error)) => return Err(error),
                None => return Ok(0),
            }
        }
        let count = buf.len().min(self.chunk.len() - self.read);
        buf[..count].copy_from_slice(&self.chunk[self.read..self.read + count]);
        self.read += count;
        Ok(count)
    }
}

impl Drop for Response {
    fn drop(&mut self) {
        self.closed.set();
    }
}

fn io_error(error: reqwest::Error) -> io::Error {
    let kind = if error.is_timeout() {
        io::ErrorKind::TimedOut
    } else {
        io::ErrorKind::Other
    };
    io::Error::new(kind, error)
}

/// Send `request` (built on an async client) and wait for its reply's head,
/// blocking. `withdrawal`, when given, cuts the call off: before the head,
/// as [`SendError::Withdrawn`]; while the body is read, as a read error of
/// kind [`io::ErrorKind::ConnectionAborted`]. Either way the connection is
/// closed at once. Must not be called from async code.
pub fn send(
    request: reqwest::RequestBuilder,
    withdrawal: Option<&Withdrawal>,
) -> Result<Response, SendError> {
    let withdrawal = withdrawal.cloned().unwrap_or_default();
    if withdrawal.is_withdrawn() {
        return Err(SendError::Withdrawn);
    }
    let closed = Arc::new(Signal::default());
    let (head_sender, head) = oneshot::channel();
    let (chunk_sender, chunks) = mpsc::channel(BUFFERED_CHUNKS);
    let held = InFlight::new(withdrawal.clone());
    let task_closed = Arc::clone(&closed);
    runtime().spawn(async move {
        let withdrawn = &withdrawal.0.withdrawn;
        let sent = tokio::select! {
            sent = request.send() => sent,
            () = withdrawn.wait() => {
                drop(held);
                let _ = head_sender.send(Err(SendError::Withdrawn));
                return;
            }
        };
        let mut response = match sent {
            Ok(response) => response,
            Err(error) => {
                drop(held);
                let _ = head_sender.send(Err(SendError::Request(error)));
                return;
            }
        };
        let head = (response.status(), response.headers().clone());
        if head_sender.send(Ok(head)).is_err() {
            return;
        }
        loop {
            let chunk = tokio::select! {
                chunk = response.chunk() => chunk,
                () = withdrawn.wait() => break,
                () = task_closed.wait() => return,
            };
            let item = match chunk {
                Ok(Some(chunk)) => Ok(chunk.to_vec()),
                Ok(None) => return,
                Err(error) => Err(io_error(error)),
            };
            let failed = item.is_err();
            tokio::select! {
                delivered = chunk_sender.send(item) => {
                    if delivered.is_err() || failed {
                        return;
                    }
                }
                () = withdrawn.wait() => break,
                () = task_closed.wait() => return,
            }
        }
        // Withdrawn: close the connection and stop counting the call before
        // its reader, which may be slow to read, is told.
        drop(response);
        drop(held);
        let _ = chunk_sender
            .send(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                WITHDRAWN,
            )))
            .await;
    });
    let (status, headers) = match head.blocking_recv() {
        Ok(head) => head?,
        // The task ended without a word, which only the runtime shutting down
        // does: nothing will come.
        Err(_) => return Err(SendError::Withdrawn),
    };
    Ok(Response {
        status,
        headers,
        chunks,
        chunk: Vec::new(),
        read: 0,
        closed,
    })
}
