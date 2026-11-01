//! Model pools: the GPU machines one model can be split across, derived from
//! what each machine reports about its own RDMA ports.
//!
//! Nothing here is configured. A pool is what the machines' reports say is
//! physically joined, so a machine that is cabled, addressed and reporting is
//! in a pool, and one that is not stays on its own. The rule leans toward
//! caution: a link needs both of its ends to report it, so a machine that
//! reports nothing (an older executor, or a reader that failed) can never be
//! pulled into a pool by a neighbour's claim.
//!
//! Placement reads the result through [`Fleet::detected_pools`]; the executors
//! resource renders it.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};

use cairn_common::executor_protocol::HostHealth;
use serde::{Deserialize, Serialize};

/// The rate, in gigabits per second, at or above which an active RDMA link
/// joins two machines into one pool. It is the floor at which splitting one
/// model's tensors across machines is worth it: below it, the traffic between
/// the halves costs more than the second machine's memory and compute give
/// back. One fleet constant until a recipe needs a different number.
pub const FAST_LINK_FLOOR_GBPS: u32 = 100;

/// A pool's name: its members' machine names, sorted and joined with `+`
/// (`bginf-sp1+bginf-sp2`). A pool of one is named by its machine.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PoolKey(pub String);

impl std::fmt::Display for PoolKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolMember {
    /// The machine's public name, the one a placement request accepts.
    pub machine: String,
    /// This member's address on the subnet that joins it to the pool: the
    /// lowest such subnet when several do, so every member of a directly
    /// cabled pair names the same one. `None` in a pool of one.
    pub link_address: Option<IpAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pool {
    pub key: PoolKey,
    /// Sorted by machine name.
    pub members: Vec<PoolMember>,
    /// The slowest link joining the members, in gigabits per second. `None`
    /// for a pool of one.
    pub rate_gbps: Option<u32>,
}

/// An IPv4 subnet, ordered by network address and then prefix length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Subnet {
    network: u32,
    prefix: u32,
}

/// One fast, active port's place on a subnet.
#[derive(Debug, Clone, Copy)]
struct Endpoint {
    subnet: Subnet,
    address: Ipv4Addr,
    rate_gbps: u32,
}

/// What joins two machines: the best of the links between them, and the
/// lowest subnet any of those links is on.
#[derive(Debug, Clone, Copy)]
struct Join {
    rate_gbps: u32,
    subnet: Subnet,
}

/// `10.0.1.1/24` as an address and its subnet. Anything else is not an
/// address this derivation can match, and is skipped.
fn parse_cidr(text: &str) -> Option<(Ipv4Addr, Subnet)> {
    let (address, prefix) = text.split_once('/')?;
    let address: Ipv4Addr = address.parse().ok()?;
    let prefix: u32 = prefix.parse().ok()?;
    if prefix > 32 {
        return None;
    }
    let mask = u32::MAX.checked_shl(32 - prefix).unwrap_or(0);
    Some((
        address,
        Subnet {
            network: u32::from(address) & mask,
            prefix,
        },
    ))
}

/// Every address on a port fast enough to join a pool and active now. A
/// reading that is a gap has none: the machine stays on its own.
fn fast_endpoints(health: &HostHealth) -> Vec<Endpoint> {
    health
        .interconnects
        .value()
        .into_iter()
        .flatten()
        .filter(|port| port.active && port.rate_gbps >= FAST_LINK_FLOOR_GBPS)
        .flat_map(|port| {
            port.addresses.iter().filter_map(|text| {
                let (address, subnet) = parse_cidr(text)?;
                Some(Endpoint {
                    subnet,
                    address,
                    rate_gbps: port.rate_gbps,
                })
            })
        })
        .collect()
}

fn join_of(a: &[Endpoint], b: &[Endpoint]) -> Option<Join> {
    let mut join: Option<Join> = None;
    for left in a {
        for right in b
            .iter()
            .filter(|right| right.subnet == left.subnet && right.address != left.address)
        {
            let rate_gbps = left.rate_gbps.min(right.rate_gbps);
            join = Some(match join {
                None => Join {
                    rate_gbps,
                    subnet: left.subnet,
                },
                Some(seen) => Join {
                    rate_gbps: seen.rate_gbps.max(rate_gbps),
                    subnet: seen.subnet.min(left.subnet),
                },
            });
        }
    }
    join
}

fn root(parent: &mut [usize], mut node: usize) -> usize {
    while parent[node] != node {
        parent[node] = parent[parent[node]];
        node = parent[node];
    }
    node
}

/// The model pools `machines` form, sorted by key.
///
/// A machine is a candidate when it reports at least one GPU. Two candidates
/// are joined when both report an active RDMA port of at least
/// [`FAST_LINK_FLOOR_GBPS`] with addresses on one IPv4 subnet. A pool is a
/// connected component of those joins, and a candidate with none is a pool of
/// one. A machine reporting no GPU is in no pool. Where a name appears twice,
/// its last report is the one read.
pub fn derive_pools<'a>(
    machines: impl IntoIterator<Item = (&'a str, &'a HostHealth)>,
) -> Vec<Pool> {
    let candidates: BTreeMap<&str, Vec<Endpoint>> = machines
        .into_iter()
        .filter(|(_, health)| health.gpus.value().is_some_and(|gpus| !gpus.is_empty()))
        .map(|(name, health)| (name, fast_endpoints(health)))
        .collect();
    let names: Vec<&str> = candidates.keys().copied().collect();
    let endpoints: Vec<&[Endpoint]> = candidates.values().map(Vec::as_slice).collect();

    let mut joins: Vec<(usize, usize, Join)> = Vec::new();
    for left in 0..names.len() {
        for right in left + 1..names.len() {
            if let Some(join) = join_of(endpoints[left], endpoints[right]) {
                joins.push((left, right, join));
            }
        }
    }
    let mut parent: Vec<usize> = (0..names.len()).collect();
    for &(left, right, _) in &joins {
        let (left, right) = (root(&mut parent, left), root(&mut parent, right));
        parent[left.max(right)] = left.min(right);
    }
    let mut components: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for machine in 0..names.len() {
        let component = root(&mut parent, machine);
        components.entry(component).or_default().push(machine);
    }

    let mut pools: Vec<Pool> = components
        .into_values()
        .map(|members| {
            let inside: Vec<&(usize, usize, Join)> = joins
                .iter()
                .filter(|(left, _, _)| members.contains(left))
                .collect();
            let rate_gbps = inside.iter().map(|(_, _, join)| join.rate_gbps).min();
            Pool {
                key: PoolKey(
                    members
                        .iter()
                        .map(|&machine| names[machine])
                        .collect::<Vec<_>>()
                        .join("+"),
                ),
                members: members
                    .iter()
                    .map(|&machine| PoolMember {
                        machine: names[machine].to_string(),
                        link_address: inside
                            .iter()
                            .filter(|(left, right, _)| *left == machine || *right == machine)
                            .map(|(_, _, join)| join.subnet)
                            .min()
                            .and_then(|subnet| {
                                endpoints[machine]
                                    .iter()
                                    .filter(|endpoint| endpoint.subnet == subnet)
                                    .map(|endpoint| endpoint.address)
                                    .min()
                            })
                            .map(IpAddr::V4),
                    })
                    .collect(),
                rate_gbps,
            }
        })
        .collect();
    pools.sort_by(|a, b| a.key.cmp(&b.key));
    pools
}

impl super::Fleet {
    /// The model pools the attached machines' latest reports derive, by each
    /// machine's public name. Cached state only: nothing is probed.
    pub fn detected_pools(&self) -> Vec<Pool> {
        let connections = self.lock_connections();
        let named: Vec<(String, &HostHealth)> = connections
            .values()
            .map(|entry| (super::executor_public_name(entry), &entry.health.host))
            .collect();
        derive_pools(named.iter().map(|(name, host)| (name.as_str(), *host)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::{
        GpuDevice, GpuMemory, GpuVendor, Interconnect, InterconnectKind, Measurement,
        MeasurementGap, MeasurementReading,
    };

    fn gb10() -> GpuDevice {
        GpuDevice {
            index: 0,
            vendor: GpuVendor::Nvidia,
            model: "NVIDIA GB10".into(),
            uuid: None,
            cores: None,
            memory: MeasurementReading::Measured {
                value: GpuMemory::Discrete {
                    used_bytes: 0,
                    total_bytes: 1,
                },
            },
            utilization_millis: MeasurementReading::Measured { value: 0 },
        }
    }

    fn port(rate_gbps: u32, active: bool, address: &str) -> Interconnect {
        Interconnect {
            kind: InterconnectKind::Roce,
            device: "rocep1s0f1".into(),
            port: 1,
            active,
            rate_gbps,
            netdev: Some("enp1s0f1np1".into()),
            addresses: vec![address.into()],
        }
    }

    /// A machine with one GPU and exactly `ports`.
    fn gpu_machine(ports: Vec<Interconnect>) -> HostHealth {
        HostHealth {
            gpus: Measurement::measured(1, vec![gb10()]),
            interconnects: Measurement::measured(1, ports),
            ..HostHealth::default()
        }
    }

    /// A Spark as it reads: four 200 Gb/s RoCE ports, each its own
    /// point-to-point /24 to its twin, this machine at `.host`.
    fn spark(host: u8) -> HostHealth {
        gpu_machine(
            [3, 1, 4, 2]
                .into_iter()
                .map(|subnet| port(200, true, &format!("10.0.{subnet}.{host}/24")))
                .collect(),
        )
    }

    fn alone(name: &str) -> Pool {
        Pool {
            key: PoolKey(name.into()),
            members: vec![PoolMember {
                machine: name.into(),
                link_address: None,
            }],
            rate_gbps: None,
        }
    }

    fn member(machine: &str, address: &str) -> PoolMember {
        PoolMember {
            machine: machine.into(),
            link_address: Some(address.parse().unwrap()),
        }
    }

    /// The fleet the gate for cairn/5728 reads: the Sparks join over their
    /// cables at the lowest shared subnet; bbs reports no RDMA port and fedora
    /// an executor too old to report any; the Mac has no GPU worth a pool.
    #[test]
    fn the_sparks_pair_and_every_other_gpu_machine_stands_alone() {
        let (sp1, sp2) = (spark(1), spark(2));
        let bbs = gpu_machine(Vec::new());
        let fedora = HostHealth {
            gpus: Measurement::measured(1, vec![gb10()]),
            ..HostHealth::default()
        };
        let no_gpu = HostHealth {
            gpus: Measurement::measured(1, Vec::new()),
            ..HostHealth::default()
        };
        let pools = derive_pools([
            ("fedora", &fedora),
            ("bginf-sp2", &sp2),
            ("bgsrv-ub", &no_gpu),
            ("bbs", &bbs),
            ("bginf-sp1", &sp1),
        ]);
        assert_eq!(
            pools,
            vec![
                alone("bbs"),
                Pool {
                    key: PoolKey("bginf-sp1+bginf-sp2".into()),
                    members: vec![
                        member("bginf-sp1", "10.0.1.1"),
                        member("bginf-sp2", "10.0.1.2"),
                    ],
                    rate_gbps: Some(200),
                },
                alone("fedora"),
            ]
        );
    }

    /// One side's report never forms a pool: a twin whose executor has not
    /// reported (or whose reader failed) keeps both machines on their own.
    #[test]
    fn a_pair_forms_only_from_both_reports() {
        let sp1 = spark(1);
        for silent in [
            Measurement::default(),
            Measurement::unavailable(1, MeasurementGap::SamplingFailed),
        ] {
            let sp2 = HostHealth {
                interconnects: silent,
                ..spark(2)
            };
            assert_eq!(
                derive_pools([("bginf-sp1", &sp1), ("bginf-sp2", &sp2)]),
                vec![alone("bginf-sp1"), alone("bginf-sp2")]
            );
        }
    }

    #[test]
    fn a_link_below_the_floor_or_down_does_not_join() {
        let slow = |host: &str| gpu_machine(vec![port(56, true, &format!("10.0.1.{host}/24"))]);
        let (a, b) = (slow("1"), slow("2"));
        assert_eq!(
            derive_pools([("a", &a), ("b", &b)]),
            vec![alone("a"), alone("b")]
        );

        let up = gpu_machine(vec![port(200, true, "10.0.1.1/24")]);
        let down = gpu_machine(vec![port(200, false, "10.0.1.2/24")]);
        assert_eq!(
            derive_pools([("a", &up), ("b", &down)]),
            vec![alone("a"), alone("b")]
        );
    }

    #[test]
    fn ports_on_different_subnets_do_not_join() {
        let a = gpu_machine(vec![port(200, true, "10.0.1.1/24")]);
        let b = gpu_machine(vec![port(200, true, "10.0.2.2/24")]);
        assert_eq!(
            derive_pools([("a", &a), ("b", &b)]),
            vec![alone("a"), alone("b")]
        );
    }

    /// A machine with a fast port and no GPU is in no pool, and so cannot
    /// stand in one with a GPU machine on its subnet.
    #[test]
    fn a_machine_without_a_gpu_forms_no_pool() {
        let gpu = gpu_machine(vec![port(200, true, "10.0.1.1/24")]);
        let storage = HostHealth {
            gpus: Measurement::measured(1, Vec::new()),
            ..gpu_machine(vec![port(200, true, "10.0.1.2/24")])
        };
        let unmeasured = HostHealth {
            gpus: Measurement::default(),
            ..gpu_machine(vec![port(200, true, "10.0.1.3/24")])
        };
        assert_eq!(
            derive_pools([("gpu", &gpu), ("storage", &storage), ("old", &unmeasured)]),
            vec![alone("gpu")]
        );
    }

    #[test]
    fn three_machines_on_one_switch_subnet_form_one_pool() {
        let machines: Vec<(String, HostHealth)> = ["c", "a", "b"]
            .into_iter()
            .enumerate()
            .map(|(host, name)| {
                (
                    name.to_string(),
                    gpu_machine(vec![port(400, true, &format!("172.16.0.{}/16", host + 10))]),
                )
            })
            .collect();
        assert_eq!(
            derive_pools(machines.iter().map(|(name, host)| (name.as_str(), host))),
            vec![Pool {
                key: PoolKey("a+b+c".into()),
                members: vec![
                    member("a", "172.16.0.11"),
                    member("b", "172.16.0.12"),
                    member("c", "172.16.0.10"),
                ],
                rate_gbps: Some(400),
            }]
        );
    }

    /// A chain joins end to end through its middle, and the pool runs at the
    /// pace of its slowest link.
    #[test]
    fn a_chain_is_one_pool_at_its_slowest_link() {
        let a = gpu_machine(vec![port(400, true, "10.0.1.1/24")]);
        let b = gpu_machine(vec![
            port(400, true, "10.0.1.2/24"),
            port(100, true, "10.0.2.2/24"),
        ]);
        let c = gpu_machine(vec![port(200, true, "10.0.2.3/24")]);
        let pools = derive_pools([("a", &a), ("b", &b), ("c", &c)]);
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].key, PoolKey("a+b+c".into()));
        assert_eq!(pools[0].rate_gbps, Some(100));
        assert_eq!(
            pools[0].members,
            vec![
                member("a", "10.0.1.1"),
                member("b", "10.0.1.2"),
                member("c", "10.0.2.3"),
            ]
        );
    }

    /// The fleet derives from each attached machine's posted health, under the
    /// name a placement request uses.
    #[test]
    fn the_fleet_derives_pools_from_its_attached_machines() {
        let fleet = super::super::Fleet::default();
        fleet.seed_host_for_test("bginf-sp1", spark(1));
        fleet.seed_host_for_test("bginf-sp2", spark(2));
        fleet.seed_host_for_test("bbs", gpu_machine(Vec::new()));
        let pools = fleet.detected_pools();
        assert_eq!(
            pools
                .iter()
                .map(|pool| pool.key.to_string())
                .collect::<Vec<_>>(),
            vec!["bbs", "bginf-sp1+bginf-sp2"]
        );
    }

    #[test]
    fn a_pool_serialises_as_the_fold_reads_it() {
        let (sp1, sp2) = (spark(1), spark(2));
        let pools = derive_pools([("bginf-sp1", &sp1), ("bginf-sp2", &sp2)]);
        assert_eq!(
            serde_json::to_value(&pools[0]).unwrap(),
            serde_json::json!({
                "key": "bginf-sp1+bginf-sp2",
                "members": [
                    {"machine": "bginf-sp1", "linkAddress": "10.0.1.1"},
                    {"machine": "bginf-sp2", "linkAddress": "10.0.1.2"},
                ],
                "rateGbps": 200,
            })
        );
    }
}
