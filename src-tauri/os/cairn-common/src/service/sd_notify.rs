//! The systemd watchdog, from the supervised daemon's side.
//!
//! A unit with `WatchdogSec=` expects its main process to send `WATCHDOG=1`
//! on the socket systemd names in `NOTIFY_SOCKET` more often than that
//! interval, and ends the process when it stops. This is the whole of the
//! protocol a watchdog needs, so it is written here rather than taken as a
//! dependency: read the socket and interval once, send one datagram.

use std::sync::OnceLock;
use std::time::Duration;

const NOTIFY_SOCKET: &str = "NOTIFY_SOCKET";
const WATCHDOG_PID: &str = "WATCHDOG_PID";
const WATCHDOG_USEC: &str = "WATCHDOG_USEC";

/// The watchdog systemd armed for this process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemdWatchdog {
    socket: String,
    interval: Duration,
}

impl SystemdWatchdog {
    /// How long systemd waits for a notification before ending the process.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Tell systemd this process is alive.
    pub fn notify(&self) -> std::io::Result<()> {
        send(&self.socket, b"WATCHDOG=1")
    }
}

/// The watchdog `get` describes for the process `own_pid`: `None` when no
/// socket is named, no interval is set, or the interval is for another
/// process (systemd sets `WATCHDOG_PID` to the one it watches).
pub fn watchdog_from_env(
    get: impl Fn(&str) -> Option<String>,
    own_pid: u32,
) -> Option<SystemdWatchdog> {
    let socket = get(NOTIFY_SOCKET).filter(|socket| !socket.is_empty())?;
    if let Some(pid) = get(WATCHDOG_PID) {
        if pid.trim().parse::<u32>().ok()? != own_pid {
            return None;
        }
    }
    let usec = get(WATCHDOG_USEC)?.trim().parse::<u64>().ok()?;
    (usec > 0).then(|| SystemdWatchdog {
        socket,
        interval: Duration::from_micros(usec),
    })
}

static WATCHDOG: OnceLock<Option<SystemdWatchdog>> = OnceLock::new();

/// Read the watchdog from this process's environment, once, and remove its
/// variables so that nothing this process spawns inherits them. Every
/// command a daemon runs inherits its environment, and a systemd-aware child
/// (a test daemon, a nested executor) would otherwise try to notify a socket
/// that accepts only the main process. Call it first thing in `main`, before
/// any other thread can read the environment.
pub fn take_watchdog_env() -> Option<&'static SystemdWatchdog> {
    WATCHDOG
        .get_or_init(|| {
            let watchdog = watchdog_from_env(|name| std::env::var(name).ok(), std::process::id());
            for name in [NOTIFY_SOCKET, WATCHDOG_PID, WATCHDOG_USEC] {
                std::env::remove_var(name);
            }
            watchdog
        })
        .as_ref()
}

/// The watchdog [`take_watchdog_env`] read, or `None` when it found none or
/// was never called.
pub fn watchdog() -> Option<&'static SystemdWatchdog> {
    WATCHDOG.get().and_then(Option::as_ref)
}

/// Send `message` to the notify socket `socket`: a filesystem path, or an
/// abstract socket written with a leading `@`.
#[cfg(unix)]
fn send(socket: &str, message: &[u8]) -> std::io::Result<()> {
    use std::os::unix::net::UnixDatagram;

    let datagram = UnixDatagram::unbound()?;
    match socket.strip_prefix('@') {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        Some(name) => {
            #[cfg(target_os = "android")]
            use std::os::android::net::SocketAddrExt;
            #[cfg(target_os = "linux")]
            use std::os::linux::net::SocketAddrExt;
            let address = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes())?;
            datagram.send_to_addr(message, &address)?;
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        Some(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "abstract notify sockets exist only on Linux",
            ))
        }
        None => {
            datagram.send_to(message, socket)?;
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn send(_socket: &str, _message: &[u8]) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "systemd notification needs a Unix socket",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        }
    }

    #[test]
    fn the_watchdog_is_read_only_for_its_own_process() {
        let armed = [
            (NOTIFY_SOCKET, "/run/user/1000/systemd/notify"),
            (WATCHDOG_PID, "4242"),
            (WATCHDOG_USEC, "180000000"),
        ];
        assert_eq!(
            watchdog_from_env(env(&armed), 4242),
            Some(SystemdWatchdog {
                socket: "/run/user/1000/systemd/notify".into(),
                interval: Duration::from_secs(180),
            })
        );
        assert_eq!(
            watchdog_from_env(env(&armed), 7),
            None,
            "another process's watchdog"
        );
        // systemd may leave the pid out; the interval is then this process's.
        assert!(watchdog_from_env(env(&[armed[0], armed[2]]), 7).is_some());
        assert_eq!(watchdog_from_env(env(&[armed[0], armed[1]]), 4242), None);
        assert_eq!(watchdog_from_env(env(&[armed[1], armed[2]]), 4242), None);
        assert_eq!(
            watchdog_from_env(env(&[armed[0], (WATCHDOG_USEC, "0")]), 4242),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_notification_reaches_a_path_socket() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notify");
        let listener = std::os::unix::net::UnixDatagram::bind(&path).unwrap();
        let watchdog = SystemdWatchdog {
            socket: path.display().to_string(),
            interval: Duration::from_secs(180),
        };
        watchdog.notify().unwrap();
        let mut buffer = [0_u8; 64];
        let read = listener.recv(&mut buffer).unwrap();
        assert_eq!(&buffer[..read], b"WATCHDOG=1");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_notification_reaches_an_abstract_socket() {
        use std::os::linux::net::SocketAddrExt;
        let name = format!("cairn-sd-notify-test-{}", std::process::id());
        let address = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes()).unwrap();
        let listener = std::os::unix::net::UnixDatagram::bind_addr(&address).unwrap();
        let watchdog = SystemdWatchdog {
            socket: format!("@{name}"),
            interval: Duration::from_secs(180),
        };
        watchdog.notify().unwrap();
        let mut buffer = [0_u8; 64];
        let read = listener.recv(&mut buffer).unwrap();
        assert_eq!(&buffer[..read], b"WATCHDOG=1");
    }
}
