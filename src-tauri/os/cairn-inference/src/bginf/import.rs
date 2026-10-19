//! bginf's profiles, pools, pins and defaults as Cairn records.
//!
//! Each profile that names nodes becomes a command recipe and a deployment
//! with the same id; a profile with no nodes (`off`) becomes no record, and a
//! pin or default naming it becomes the empty set. Every record is a
//! [`RecordSource::Bginf`] mirror.

use std::collections::{BTreeMap, BTreeSet};

use crate::deployment::{Deployment, DeploymentId};
use crate::inventory::Inventory;
use crate::machine::MachineName;
use crate::pool::{DeploymentSet, Pool, PoolMachine, RecordSource};
use crate::recipe::{Engine, ModelRef, Recipe, Requirements};
use crate::resolve::Slot;

use super::files::BginfFiles;
use super::wire::{Profile, Profiles, State};

/// bginf node names to Cairn machine names. A node absent from the map keeps
/// its name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MachineMap {
    to_cairn: BTreeMap<String, MachineName>,
}

impl MachineMap {
    pub fn new(pairs: impl IntoIterator<Item = (String, String)>) -> MachineMap {
        MachineMap {
            to_cairn: pairs
                .into_iter()
                .map(|(node, machine)| (node, MachineName::new(machine)))
                .collect(),
        }
    }

    /// The Cairn machine for a bginf node.
    pub fn cairn(&self, node: &str) -> MachineName {
        self.to_cairn
            .get(node)
            .cloned()
            .unwrap_or_else(|| MachineName::new(node))
    }

    /// The bginf node for a Cairn machine.
    pub fn bginf(&self, machine: &MachineName) -> String {
        self.to_cairn
            .iter()
            .find(|(_, m)| *m == machine)
            .map(|(node, _)| node.clone())
            .unwrap_or_else(|| machine.to_string())
    }
}

/// A pool's pin and default as bginf names them: profile ids.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolSlots {
    pub pin: Option<String>,
    pub default: Option<String>,
}

/// Where pools, addresses, pins and defaults come from.
pub enum Topology<'a> {
    /// The live ctl: pools and slots from `/state`, and each machine's address
    /// from the `api_base` host of the deployments it heads (so a machine that
    /// heads nothing has no known address).
    Live(&'a State),
    /// bginf's files: pools and addresses from `nodes.yaml`. The files hold no
    /// pins or defaults, so the caller supplies them by pool.
    Files {
        files: &'a BginfFiles,
        slots: &'a BTreeMap<String, PoolSlots>,
    },
}

/// An import's records and what it refused, in words.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Imported {
    pub inventory: Inventory,
    pub warnings: Vec<String>,
}

struct HttpUrl {
    host: String,
    port: u16,
    path: String,
}

fn parse_http_url(url: &str) -> Result<HttpUrl, String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("{url} is not an http:// URL"))?;
    let (authority, path) = match rest.find('/') {
        Some(at) => (&rest[..at], &rest[at..]),
        None => (rest, ""),
    };
    let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
        let (host, after) = bracketed
            .split_once(']')
            .ok_or_else(|| format!("{url} has an unclosed ["))?;
        (host, after.strip_prefix(':'))
    } else {
        match authority.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (authority, None),
        }
    };
    if host.is_empty() {
        return Err(format!("{url} has no host"));
    }
    let port = match port {
        Some(port) => port
            .parse::<u16>()
            .map_err(|_| format!("{url} has an invalid port"))?,
        None => 80,
    };
    Ok(HttpUrl {
        host: host.to_string(),
        port,
        path: path.to_string(),
    })
}

/// A profile's recipe, deployment and the API host its head serves on.
fn profile_records(
    id: &str,
    profile: &Profile,
    map: &MachineMap,
) -> Result<(Recipe, Deployment, String), String> {
    let head = profile
        .head
        .as_deref()
        .ok_or_else(|| format!("profile {id} names no head; skipped"))?;
    let start = profile
        .start
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| format!("profile {id} has no start command; skipped"))?;
    let api_base = profile
        .api_base
        .as_deref()
        .ok_or_else(|| format!("profile {id} has no api_base; skipped"))?;
    let api =
        parse_http_url(api_base).map_err(|err| format!("profile {id} api_base: {err}; skipped"))?;
    let health = profile
        .health
        .as_deref()
        .ok_or_else(|| format!("profile {id} has no health URL; skipped"))?;
    let health =
        parse_http_url(health).map_err(|err| format!("profile {id} health: {err}; skipped"))?;
    if (health.host.as_str(), health.port) != (api.host.as_str(), api.port) {
        return Err(format!(
            "profile {id} health is served at {}:{} but api_base at {}:{}; skipped",
            health.host, health.port, api.host, api.port
        ));
    }
    let served_name = profile
        .served_model
        .clone()
        .unwrap_or_else(|| id.to_string());
    let recipe = Recipe {
        id: id.into(),
        source: RecordSource::Bginf,
        model: ModelRef {
            name: profile.model.clone().unwrap_or_else(|| served_name.clone()),
            weights: Vec::new(),
        },
        served_name,
        upstream_id: profile.catalog_id.clone(),
        verbs: None,
        engine: Engine::Command {
            start: start.to_string(),
            stop: profile.stop.clone(),
            description: profile.engine.clone(),
        },
        requirements: Requirements {
            machines: profile.nodes.len() as u32,
            accelerator: None,
            interconnect: None,
            memory: None,
        },
        aliases: profile.aliases.clone(),
        notes: profile.notes.clone(),
    };
    let deployment = Deployment {
        id: id.into(),
        source: RecordSource::Bginf,
        recipe: id.into(),
        machines: profile.nodes.iter().map(|n| map.cairn(n)).collect(),
        head: map.cairn(head),
        api_port: api.port,
        api_path: api.path,
        health_path: health.path,
    };
    Ok((recipe, deployment, api.host))
}

/// One pool before its slots are known: id, nodes in order, and the source's
/// pin and default.
struct PoolDraft {
    id: String,
    machines: Vec<MachineName>,
    slots: PoolSlots,
}

/// Import bginf's configuration.
pub fn import(profiles: &Profiles, topology: Topology<'_>, map: &MachineMap) -> Imported {
    let mut warnings = Vec::new();
    let mut inventory = Inventory::default();
    let mut empty_profiles = BTreeSet::new();
    let mut api_hosts: Vec<(MachineName, DeploymentId, String)> = Vec::new();

    for (id, profile) in profiles {
        if profile.nodes.is_empty() {
            empty_profiles.insert(id.clone());
            continue;
        }
        match profile_records(id, profile, map) {
            Ok((recipe, deployment, host)) => {
                api_hosts.push((deployment.head.clone(), deployment.id.clone(), host));
                inventory.insert_recipe(recipe);
                inventory.insert_deployment(deployment);
            }
            Err(warning) => warnings.push(warning),
        }
    }

    // Pools and addresses.
    let mut drafts: Vec<PoolDraft> = Vec::new();
    let mut addresses: BTreeMap<MachineName, String> = BTreeMap::new();
    match &topology {
        Topology::Live(state) => {
            for (id, pool) in &state.pools {
                drafts.push(PoolDraft {
                    id: id.clone(),
                    machines: pool.nodes.iter().map(|n| map.cairn(n)).collect(),
                    slots: PoolSlots {
                        pin: pool.pin.clone(),
                        default: pool.default.clone(),
                    },
                });
            }
            for (machine, deployment, host) in &api_hosts {
                match addresses.get(machine) {
                    Some(known) if known != host => warnings.push(format!(
                        "machine {machine}: deployment {deployment} serves at {host}, but another deployment it heads serves at {known}; kept {known}"
                    )),
                    Some(_) => {}
                    None => {
                        addresses.insert(machine.clone(), host.clone());
                    }
                }
            }
        }
        Topology::Files { files, slots } => {
            for (node, entry) in &files.nodes {
                let machine = map.cairn(node);
                let pool = entry.pool_name();
                match drafts.iter_mut().find(|d| d.id == pool) {
                    Some(draft) => draft.machines.push(machine.clone()),
                    None => drafts.push(PoolDraft {
                        id: pool.to_string(),
                        machines: vec![machine.clone()],
                        slots: slots.get(pool).cloned().unwrap_or_default(),
                    }),
                }
                if let Some(ip) = &entry.ip {
                    addresses.insert(machine, ip.clone());
                }
            }
            for (machine, deployment, host) in &api_hosts {
                if let Some(ip) = addresses.get(machine).filter(|ip| *ip != host) {
                    warnings.push(format!(
                        "deployment {deployment} serves at {host}, but nodes.yaml gives its head {machine} the address {ip}"
                    ));
                }
            }
            for pool in slots.keys() {
                if !drafts.iter().any(|d| &d.id == pool) {
                    warnings.push(format!("pin or default supplied for unknown pool {pool}"));
                }
            }
        }
    }

    for draft in &drafts {
        inventory.insert_pool(Pool {
            id: draft.id.as_str().into(),
            source: RecordSource::Bginf,
            machines: draft
                .machines
                .iter()
                .map(|name| PoolMachine {
                    name: name.clone(),
                    address: addresses.get(name).cloned(),
                })
                .collect(),
            default: DeploymentSet::new(),
            pin: None,
        });
    }

    // Refuse what does not fit (a profile spanning pools, a machine count
    // unlike its node list), then drop the recipes left without a deployment.
    warnings.extend(inventory.prune_invalid().iter().map(ToString::to_string));
    let placed: BTreeSet<_> = inventory
        .deployments
        .values()
        .map(|d| d.recipe.clone())
        .collect();
    inventory.recipes.retain(|id, _| placed.contains(id));

    // Pins and defaults, against the deployments that survived.
    for draft in &drafts {
        let pool_id = draft.id.as_str().into();
        let fits_pool = |id: &DeploymentId| {
            inventory
                .deployments
                .get(id)
                .is_some_and(|d| d.machines.iter().all(|m| draft.machines.contains(m)))
        };
        let mut convert = |slot: Slot, name: Option<&String>| -> Option<DeploymentSet> {
            let name = name?;
            if empty_profiles.contains(name) {
                return Some(DeploymentSet::new());
            }
            let id = DeploymentId::from(name.as_str());
            if fits_pool(&id) {
                return Some(BTreeSet::from([id]));
            }
            warnings.push(format!(
                "pool {} {} names profile {name}, which has no deployment in the pool; {}",
                draft.id,
                slot.as_str(),
                match slot {
                    Slot::Pin => "treated as no pin, as bginf does",
                    Slot::Default => "treated as an empty default",
                }
            ));
            None
        };
        let pin = convert(Slot::Pin, draft.slots.pin.as_ref());
        let default = convert(Slot::Default, draft.slots.default.as_ref()).unwrap_or_default();
        if let Some(pool) = inventory.pools.get_mut(&pool_id) {
            pool.pin = pin;
            pool.default = default;
        }
    }
    warnings.extend(inventory.prune_invalid().iter().map(ToString::to_string));

    Imported {
        inventory,
        warnings,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::PoolId;

    /// bginf's `ctl/test_ctl.py` fixture, as profiles and nodes.
    fn test_files() -> BginfFiles {
        let nodes = "
sp1: { ip: 192.0.2.40, pool: sparks, outlet: switch.sp1, power_sensor: sensor.sp1 }
sp2: { ip: 192.0.2.41, pool: sparks, outlet: switch.sp2, power_sensor: sensor.sp2 }
halo: { ip: 192.0.2.43, pool: halo }
";
        let profile = |id: &str, nodes: &str, ip: &str, port: u16| {
            format!(
                "id: {id}\nnodes: {nodes}\nhead: {head}\nstart: ./start.sh\nstop: ./stop.sh\nhealth: http://{ip}:{port}/v1/models\napi_base: http://{ip}:{port}/v1\n",
                head = nodes.trim_matches(['[', ']']).split(',').next().unwrap().trim(),
            )
        };
        let profiles = [
            profile("dsv4f-tp2", "[sp1, sp2]", "192.0.2.40", 8888),
            profile("qwen36-fast", "[sp1]", "192.0.2.40", 8001),
            profile("qwen3-embed-4b", "[halo]", "192.0.2.43", 8090),
            "id: \"off\"\nnodes: []\n".to_string(),
            profile("straddle", "[sp1, halo]", "192.0.2.40", 9000),
        ];
        let named: Vec<(String, &str)> = profiles
            .iter()
            .enumerate()
            .map(|(i, text)| (format!("p{i}.yaml"), text.as_str()))
            .collect();
        let named: Vec<(&str, &str)> = named.iter().map(|(n, t)| (n.as_str(), *t)).collect();
        BginfFiles::parse(nodes, &named).unwrap()
    }

    fn halo_is_fedora() -> MachineMap {
        MachineMap::new([("halo".to_string(), "fedora".to_string())])
    }

    fn slots(pin: Option<&str>, default: Option<&str>) -> PoolSlots {
        PoolSlots {
            pin: pin.map(String::from),
            default: default.map(String::from),
        }
    }

    /// bginf `test_pools_resolve_independently`, import half: the defaults
    /// map into their pools, halo's deployment is headed on fedora, and
    /// `straddle` is refused.
    #[test]
    fn the_test_fixture_imports_with_halo_as_fedora_and_straddle_refused() {
        let files = test_files();
        let slots = BTreeMap::from([
            ("sparks".to_string(), slots(None, Some("dsv4f-tp2"))),
            ("halo".to_string(), slots(None, Some("qwen3-embed-4b"))),
        ]);
        let imported = import(
            &files.profiles,
            Topology::Files {
                files: &files,
                slots: &slots,
            },
            &halo_is_fedora(),
        );
        let inventory = &imported.inventory;
        assert_eq!(inventory.validate(), Ok(()));
        assert_eq!(
            inventory
                .deployments
                .keys()
                .map(|d| d.as_str())
                .collect::<Vec<_>>(),
            vec!["dsv4f-tp2", "qwen3-embed-4b", "qwen36-fast"]
        );
        assert!(!inventory.recipes.contains_key(&"straddle".into()));
        assert!(
            imported
                .warnings
                .iter()
                .any(|w| w.contains("straddle spans pools")),
            "{:?}",
            imported.warnings
        );
        let embed = &inventory.deployments[&DeploymentId::from("qwen3-embed-4b")];
        assert_eq!(embed.head, MachineName::from("fedora"));
        assert_eq!(inventory.address_of(&"fedora".into()), Some("192.0.2.43"));
        assert_eq!(
            embed.api_base(inventory.address_of(&embed.head).unwrap()),
            "http://192.0.2.43:8090/v1"
        );
        let halo = &inventory.pools[&PoolId::from("halo")];
        assert_eq!(halo.default, BTreeSet::from(["qwen3-embed-4b".into()]));
        // An empty-node profile has no pool, because it has no record.
        assert!(!inventory.deployments.contains_key(&"off".into()));
    }

    #[test]
    fn an_off_pin_is_pinned_to_nothing_and_an_unknown_pin_is_no_pin() {
        let files = test_files();
        let slots = BTreeMap::from([
            ("sparks".to_string(), slots(Some("off"), Some("dsv4f-tp2"))),
            ("halo".to_string(), slots(Some("nope"), Some("off"))),
        ]);
        let imported = import(
            &files.profiles,
            Topology::Files {
                files: &files,
                slots: &slots,
            },
            &MachineMap::default(),
        );
        let pools = &imported.inventory.pools;
        assert_eq!(pools[&PoolId::from("sparks")].pin, Some(BTreeSet::new()));
        assert_eq!(pools[&PoolId::from("halo")].pin, None);
        assert_eq!(pools[&PoolId::from("halo")].default, BTreeSet::new());
        assert!(imported
            .warnings
            .iter()
            .any(|w| w.contains("pool halo pin names profile nope")));
    }

    #[test]
    fn a_pin_from_another_pool_is_no_pin_as_in_bginf() {
        let files = test_files();
        let slots = BTreeMap::from([("halo".to_string(), slots(Some("qwen36-fast"), None))]);
        let imported = import(
            &files.profiles,
            Topology::Files {
                files: &files,
                slots: &slots,
            },
            &MachineMap::default(),
        );
        assert_eq!(imported.inventory.pools[&PoolId::from("halo")].pin, None);
    }

    #[test]
    fn a_health_url_on_another_host_refuses_the_profile() {
        let mut files = test_files();
        files.profiles.get_mut("qwen36-fast").unwrap().health =
            Some("http://192.0.2.41:8001/v1/models".into());
        let imported = import(
            &files.profiles,
            Topology::Files {
                files: &files,
                slots: &BTreeMap::new(),
            },
            &MachineMap::default(),
        );
        assert!(!imported
            .inventory
            .deployments
            .contains_key(&"qwen36-fast".into()));
        assert!(imported
            .warnings
            .iter()
            .any(|w| w.starts_with("profile qwen36-fast health is served at 192.0.2.41:8001")));
    }

    #[test]
    fn live_addresses_come_from_the_heads_api_base() {
        let files = test_files();
        let state: State = serde_json::from_value(serde_json::json!({
            "pools": {
                "sparks": {"desired": "dsv4f-tp2", "reason": "default", "pin": null,
                           "default": "dsv4f-tp2", "running": null, "converged": false,
                           "nodes": ["sp1", "sp2"]},
                "halo": {"desired": "qwen3-embed-4b", "reason": "default", "pin": "off",
                         "default": "qwen3-embed-4b", "running": null, "converged": false,
                         "nodes": ["halo"]}
            }
        }))
        .unwrap();
        let imported = import(&files.profiles, Topology::Live(&state), &halo_is_fedora());
        let inventory = &imported.inventory;
        assert_eq!(inventory.address_of(&"sp1".into()), Some("192.0.2.40"));
        assert_eq!(
            inventory.address_of(&"sp2".into()),
            None,
            "sp2 heads nothing"
        );
        assert_eq!(inventory.address_of(&"fedora".into()), Some("192.0.2.43"));
        assert_eq!(
            inventory.pools[&PoolId::from("halo")].pin,
            Some(BTreeSet::new())
        );
        assert_eq!(
            inventory.pools[&PoolId::from("sparks")].default,
            BTreeSet::from(["dsv4f-tp2".into()])
        );
    }

    #[test]
    fn urls_parse_into_host_port_and_path() {
        let url = parse_http_url("http://192.0.2.40:8888/v1").unwrap();
        assert_eq!(
            (url.host.as_str(), url.port, url.path.as_str()),
            ("192.0.2.40", 8888, "/v1")
        );
        let url = parse_http_url("http://host.example").unwrap();
        assert_eq!(
            (url.host.as_str(), url.port, url.path.as_str()),
            ("host.example", 80, "")
        );
        let url = parse_http_url("http://[2001:db8::1]:8001/health").unwrap();
        assert_eq!((url.host.as_str(), url.port), ("2001:db8::1", 8001));
        assert!(parse_http_url("https://host/v1").is_err());
        assert!(parse_http_url("http://host:port/v1").is_err());
    }
}
