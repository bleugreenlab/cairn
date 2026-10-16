//! The executor link's kernel-level give-up, set on both ends' sockets.
//!
//! [`crate::executor_protocol::EXECUTOR_LINK_TRANSPORT_GIVE_UP_MS`] states the
//! contract and why; this is the one place it becomes socket options, so the
//! executor's dialed socket and the runner's accepted one cannot drift apart.

use std::io;
use std::net::IpAddr;
use std::time::Duration;

use crate::executor_protocol::{
    EXECUTOR_LINK_KEEPALIVE_COUNT, EXECUTOR_LINK_KEEPALIVE_IDLE_MS,
    EXECUTOR_LINK_KEEPALIVE_INTERVAL_MS, EXECUTOR_LINK_TRANSPORT_GIVE_UP_MS,
};

/// Whether an address belongs to a Tailscale tailnet: its IPv4 range is the
/// carrier-grade NAT block `100.64.0.0/10`, its IPv6 range `fd7a:115c:a1e0::/48`.
/// Traffic to such an address rides the tunnel, whatever lies underneath it.
pub fn is_tunnel_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(v4) => {
            let [first, second, ..] = v4.octets();
            first == 100 && (second & 0xc0) == 64
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_tunnel_address(IpAddr::V4(v4));
            }
            let segments = v6.segments();
            segments[0] == 0xfd7a && segments[1] == 0x115c && segments[2] == 0xa1e0
        }
    }
}

/// The transport options an executor link's socket carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkGiveUp {
    pub keepalive_idle: Duration,
    pub keepalive_interval: Duration,
    pub keepalive_count: u32,
    /// The bound on unacknowledged data, where the platform has one.
    pub user_timeout: Option<Duration>,
}

impl LinkGiveUp {
    /// What the protocol constants say this platform's sockets should carry.
    pub fn contract() -> Self {
        Self {
            keepalive_idle: Duration::from_millis(EXECUTOR_LINK_KEEPALIVE_IDLE_MS),
            keepalive_interval: Duration::from_millis(EXECUTOR_LINK_KEEPALIVE_INTERVAL_MS),
            keepalive_count: EXECUTOR_LINK_KEEPALIVE_COUNT,
            user_timeout: if cfg!(any(target_os = "linux", target_os = "android")) {
                Some(Duration::from_millis(EXECUTOR_LINK_TRANSPORT_GIVE_UP_MS))
            } else {
                None
            },
        }
    }
}

/// Set the link's give-up on a connected socket. A failure leaves the kernel's
/// default in force; callers log it and keep the connection.
pub fn apply_give_up(stream: &tokio::net::TcpStream) -> io::Result<()> {
    let contract = LinkGiveUp::contract();
    let socket = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new().with_time(contract.keepalive_idle);
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    let keepalive = keepalive
        .with_interval(contract.keepalive_interval)
        .with_retries(contract.keepalive_count);
    socket.set_tcp_keepalive(&keepalive)?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    socket.set_tcp_user_timeout(contract.user_timeout)?;
    Ok(())
}

/// Read back what a socket carries, in the shape of [`LinkGiveUp::contract`].
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn read_give_up(stream: &tokio::net::TcpStream) -> io::Result<Option<LinkGiveUp>> {
    let socket = socket2::SockRef::from(stream);
    if !socket.keepalive()? {
        return Ok(None);
    }
    Ok(Some(LinkGiveUp {
        keepalive_idle: socket.tcp_keepalive_time()?,
        keepalive_interval: socket.tcp_keepalive_interval()?,
        keepalive_count: socket.tcp_keepalive_retries()?,
        #[cfg(target_os = "linux")]
        user_timeout: socket.tcp_user_timeout()?,
        #[cfg(not(target_os = "linux"))]
        user_timeout: None,
    }))
}

#[cfg(test)]
mod classification_tests {
    use super::*;

    #[test]
    fn tailnet_addresses_are_tunnel_addresses_and_nothing_else_is() {
        for tunnel in [
            "100.64.0.1",
            "100.75.184.96",
            "100.127.255.254",
            "fd7a:115c:a1e0::1",
            "fd7a:115c:a1e0:ab12:4843:cd96:6263:b860",
            "::ffff:100.99.188.99",
        ] {
            assert!(is_tunnel_address(tunnel.parse().unwrap()), "{tunnel}");
        }
        for direct in [
            "100.63.255.255",
            "100.128.0.1",
            "10.10.10.51",
            "192.168.1.4",
            "127.0.0.1",
            "fd7a:115c:a1e1::1",
            "fe80::1",
            "::1",
        ] {
            assert!(!is_tunnel_address(direct.parse().unwrap()), "{direct}");
        }
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_socket_carries_the_stated_give_up_after_it_is_applied() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dialed = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        assert_eq!(read_give_up(&dialed).unwrap(), None);
        apply_give_up(&dialed).unwrap();
        assert_eq!(read_give_up(&dialed).unwrap(), Some(LinkGiveUp::contract()));
    }
}
