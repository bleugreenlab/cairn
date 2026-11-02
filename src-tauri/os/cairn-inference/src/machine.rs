//! Machines, named the way Cairn names executors, and where to reach them.

use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};

use crate::deployment::Deployment;
use crate::inventory::Inventory;
use crate::record::RecordSource;
use crate::render::MachineAt;

crate::string_id!(
    /// A machine by its Cairn executor name, whether or not that executor is
    /// enrolled yet: the Sparks are `bginf-sp1` and `bginf-sp2`, and the halo
    /// box is `fedora`.
    MachineName
);

/// A machine's network address, written by the operator. An enrolled
/// executor needs none: its address is observed from its connection to the
/// orch (see [`Addresses`]), and a record only overrides that. A machine that
/// is not an enrolled executor is reachable only through its record.
///
/// Which machines form a pool is not recorded: placement detects it from the
/// links the executors report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineRecord {
    pub name: MachineName,
    #[serde(default)]
    pub source: RecordSource,
    pub address: String,
}

/// What the fleet observes now of how its attached executors are reached,
/// each by its public name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Network {
    /// Where each attached executor's connection to the orch came from. Only
    /// the orch's own executor, which shares its machine, is loopback; the host
    /// leaves out a remote executor whose connection arrived on loopback (a
    /// forwarded port), since loopback names the orch's machine, not that one.
    pub connections: BTreeMap<String, IpAddr>,
    /// Each pooled machine's address on the fast link joining its pool.
    pub links: BTreeMap<String, IpAddr>,
}

/// Where a resolved address came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressSource {
    /// The operator's machine record.
    Record,
    /// The executor's own connection to the orch.
    Connection,
    /// The machine's address on the fast link joining its pool.
    Link,
}

impl AddressSource {
    pub fn describe(self) -> &'static str {
        match self {
            Self::Record => "record",
            Self::Connection => "observed from its connection",
            Self::Link => "its address on the pool's fast link",
        }
    }
}

/// A machine's address and where Cairn learned it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Address {
    pub address: String,
    pub source: AddressSource,
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.address, self.source.describe())
    }
}

/// Why `machine` has no address, and what gives it one.
pub fn unknown_address(machine: &MachineName) -> String {
    format!(
        "{machine}'s address is unknown: no executor named {machine} is attached and it has no machine record; enrol it as an executor or add a machine record"
    )
}

/// The one answer to where a machine is reached: its machine record, else
/// what the fleet observes of it.
///
/// Two vantage points need an address, and they differ:
///
/// - **From the orch** ([`Self::from_orch`]): a deployment's API base, its
///   health URL (which the head itself polls) and its metrics. A record wins;
///   otherwise the address the executor's connection came from, a route the
///   orch and that machine already share. Loopback is kept: it is the orch's
///   own machine, where loopback is right both for the orch and for the head
///   polling itself.
/// - **From a gang's head** ([`Self::from_head`]): where the head reaches a
///   peer over ssh. A record wins; otherwise the peer's address on the fast
///   link joining the pool, which the head shares by construction; otherwise
///   its observed connection address, unless that is loopback, which names the
///   orch's machine only to the orch.
///
/// A machine that is neither recorded nor attached has no address.
pub struct Addresses<'a> {
    inventory: &'a Inventory,
    network: &'a Network,
}

impl<'a> Addresses<'a> {
    pub fn new(inventory: &'a Inventory, network: &'a Network) -> Self {
        Self { inventory, network }
    }

    fn record(&self, machine: &MachineName) -> Option<Address> {
        self.inventory.machines.get(machine).map(|record| Address {
            address: record.address.clone(),
            source: AddressSource::Record,
        })
    }

    fn connection(&self, machine: &MachineName) -> Option<IpAddr> {
        self.network.connections.get(machine.as_str()).copied()
    }

    /// Where the orch reaches `machine`.
    pub fn from_orch(&self, machine: &MachineName) -> Option<Address> {
        self.record(machine).or_else(|| {
            self.connection(machine).map(|ip| Address {
                address: ip.to_string(),
                source: AddressSource::Connection,
            })
        })
    }

    /// Where a gang's head reaches `machine`, one of its peers.
    pub fn from_head(&self, machine: &MachineName) -> Option<Address> {
        let link = || {
            self.network.links.get(machine.as_str()).map(|ip| Address {
                address: ip.to_string(),
                source: AddressSource::Link,
            })
        };
        let connection = || {
            self.connection(machine)
                .filter(|ip| !ip.is_loopback())
                .map(|ip| Address {
                    address: ip.to_string(),
                    source: AddressSource::Connection,
                })
        };
        self.record(machine).or_else(link).or_else(connection)
    }

    /// `deployment`'s machines as its renderer reaches them from the head.
    pub fn machines_at(&self, deployment: &Deployment) -> Vec<MachineAt> {
        deployment
            .machines
            .iter()
            .map(|name| MachineAt {
                name: name.clone(),
                address: self.from_head(name).map(|at| at.address),
                link: self
                    .network
                    .links
                    .get(name.as_str())
                    .map(ToString::to_string),
            })
            .collect()
    }

    /// Why a gang of `head` and `peers` cannot be started and probed, naming
    /// every machine with no address it needs; `None` when each has one.
    pub fn unreachable<'m>(
        &self,
        head: &MachineName,
        peers: impl IntoIterator<Item = &'m MachineName>,
    ) -> Option<String> {
        let mut problems = Vec::new();
        if self.from_orch(head).is_none() {
            problems.push(unknown_address(head));
        }
        for peer in peers.into_iter().filter(|peer| *peer != head) {
            if self.from_head(peer).is_some() {
                continue;
            }
            problems.push(match self.connection(peer) {
                Some(_) => format!(
                    "{peer} is the orch's own machine, which its connection names only as loopback, so the head {head} cannot reach it; add a machine record with its network address"
                ),
                None => unknown_address(peer),
            });
        }
        (!problems.is_empty()).then(|| problems.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    fn inventory(records: &[(&str, &str)]) -> Inventory {
        let mut inventory = Inventory::default();
        for (name, address) in records {
            inventory.insert_machine(MachineRecord {
                name: (*name).into(),
                source: RecordSource::Cairn,
                address: (*address).into(),
            });
        }
        inventory
    }

    fn network(connections: &[(&str, IpAddr)], links: &[(&str, IpAddr)]) -> Network {
        let by_name = |pairs: &[(&str, IpAddr)]| {
            pairs
                .iter()
                .map(|(name, ip)| ((*name).to_string(), *ip))
                .collect()
        };
        Network {
            connections: by_name(connections),
            links: by_name(links),
        }
    }

    fn at(address: &str, source: AddressSource) -> Option<Address> {
        Some(Address {
            address: address.into(),
            source,
        })
    }

    #[test]
    fn an_enrolled_executor_with_no_record_is_reached_where_it_connected_from() {
        let (records, fleet) = (inventory(&[]), network(&[("bbs", ip("10.10.10.10"))], &[]));
        let addresses = Addresses::new(&records, &fleet);
        let bbs = MachineName::from("bbs");
        assert_eq!(
            addresses.from_orch(&bbs),
            at("10.10.10.10", AddressSource::Connection)
        );
        assert_eq!(
            addresses.from_head(&bbs),
            at("10.10.10.10", AddressSource::Connection)
        );
        assert_eq!(addresses.unreachable(&bbs, [&bbs]), None);
        assert_eq!(
            addresses.from_orch(&bbs).unwrap().to_string(),
            "10.10.10.10 (observed from its connection)"
        );
    }

    #[test]
    fn a_record_overrides_what_the_fleet_observes() {
        let records = inventory(&[("bbs", "bbs.lan")]);
        let fleet = network(
            &[("bbs", ip("100.64.0.7"))],
            &[("bbs", ip("192.168.100.1"))],
        );
        let addresses = Addresses::new(&records, &fleet);
        let bbs = MachineName::from("bbs");
        assert_eq!(
            addresses.from_orch(&bbs),
            at("bbs.lan", AddressSource::Record)
        );
        assert_eq!(
            addresses.from_head(&bbs),
            at("bbs.lan", AddressSource::Record)
        );
    }

    #[test]
    fn a_machine_that_is_not_an_enrolled_executor_needs_a_record() {
        let fleet = Network::default();
        let bare = inventory(&[]);
        let halo = MachineName::from("fedora");
        let addresses = Addresses::new(&bare, &fleet);
        assert_eq!(addresses.from_orch(&halo), None);
        assert_eq!(addresses.from_head(&halo), None);
        assert_eq!(
            addresses.unreachable(&halo, [&halo]),
            Some("fedora's address is unknown: no executor named fedora is attached and it has no machine record; enrol it as an executor or add a machine record".into())
        );

        let recorded = inventory(&[("fedora", "10.20.20.43")]);
        let addresses = Addresses::new(&recorded, &fleet);
        assert_eq!(
            addresses.from_orch(&halo),
            at("10.20.20.43", AddressSource::Record)
        );
        assert_eq!(addresses.unreachable(&halo, [&halo]), None);
    }

    /// The orch's own machine is loopback: right for the orch and for that
    /// machine probing itself, never for another machine reaching it.
    #[test]
    fn loopback_serves_the_orch_but_not_a_gang_s_head() {
        let records = inventory(&[]);
        let fleet = network(
            &[
                ("local", IpAddr::V4(Ipv4Addr::LOCALHOST)),
                ("mac2", IpAddr::V6(Ipv6Addr::LOCALHOST)),
                ("spark", ip("10.0.0.2")),
            ],
            &[],
        );
        let addresses = Addresses::new(&records, &fleet);
        let (local, mac2, spark) = (
            MachineName::from("local"),
            MachineName::from("mac2"),
            MachineName::from("spark"),
        );
        assert_eq!(
            addresses.from_orch(&local),
            at("127.0.0.1", AddressSource::Connection)
        );
        assert_eq!(addresses.from_head(&local), None);
        assert_eq!(addresses.from_head(&mac2), None);
        // The orch's machine heads a gang: its peer was seen from the orch,
        // which is the head, so the observed address serves.
        assert_eq!(addresses.unreachable(&local, [&local, &spark]), None);
        // The orch's machine as a peer cannot be reached from another head.
        let refusal = addresses.unreachable(&spark, [&spark, &local]).unwrap();
        assert!(
            refusal.starts_with("local is the orch's own machine"),
            "{refusal}"
        );
    }

    /// A gang's head reaches its peers on the fast link joining the pool, and
    /// the orch reaches the head where its connection came from.
    #[test]
    fn a_gang_s_peers_are_reached_on_the_pool_link() {
        let records = inventory(&[]);
        let fleet = network(
            &[("sp1", ip("10.20.20.40")), ("sp2", ip("10.20.20.41"))],
            &[("sp1", ip("192.168.200.1")), ("sp2", ip("192.168.200.2"))],
        );
        let addresses = Addresses::new(&records, &fleet);
        let (sp1, sp2) = (MachineName::from("sp1"), MachineName::from("sp2"));
        assert_eq!(
            addresses.from_orch(&sp1),
            at("10.20.20.40", AddressSource::Connection)
        );
        assert_eq!(
            addresses.from_head(&sp2),
            at("192.168.200.2", AddressSource::Link)
        );
        let deployment = Deployment {
            id: "gang".into(),
            source: RecordSource::Cairn,
            profile: "gang".into(),
            head: sp1.clone(),
            machines: vec![sp1.clone(), sp2.clone()],
            api_port: 8888,
            api_path: "/v1".into(),
            health_path: "/v1/models".into(),
        };
        assert_eq!(
            addresses.machines_at(&deployment)[1],
            MachineAt {
                name: sp2,
                address: Some("192.168.200.2".into()),
                link: Some("192.168.200.2".into()),
            }
        );
    }
}
