//! Which machine serves a machine-service MCP call (`cairn://mcp/axon/<tool>`).
//!
//! A machine service is one MCP server that several machines may each run, and
//! each machine's catalog is its own: capability differs by platform. A call is
//! routed to exactly one machine, chosen by the batch's call-level `executor`
//! selector (`{name}` or `{os}`), and needs none when exactly one machine serves
//! the server. Several serving machines and no selector is refused rather than
//! guessed: the call drives a screen, and the wrong screen is not a retry.
//!
//! The route is settled for the whole batch before any item runs, so a refusal
//! runs nothing, and every refused item is named in that one refusal.
//!
//! A node's placement default never routes these calls. A default says where an
//! agent's tree-bound work runs; which desktop a call drives is the call's own
//! choice.

use super::types::{McpRoute, RunSpec};
use crate::config::machine_services::MachineCandidate;
use crate::project_services::supervisor::machine::{InstanceState, MachineServiceInstance};
use cairn_common::executor_protocol::{executor_names_match, ExecutorSelector};
use cairn_config::fleet::MachineServiceKey;
use std::collections::BTreeMap;

/// Whether `instance` takes a call: its resident speaks MCP and is ready or
/// degraded. Degraded routes on purpose: a slow probe or a crash backoff may
/// still answer, and a call that fails says so itself. Every other state names
/// itself in the refusal instead.
pub(crate) fn serves(instance: &MachineServiceInstance) -> bool {
    instance.serves_mcp
        && matches!(
            instance.state,
            InstanceState::Ready | InstanceState::Degraded { .. }
        )
}

/// Whether `instance` takes a call that names its machine. Every serving
/// machine does, and so does one waiting for its operator: its resident is up
/// (the state is reached only once it serves) and has merely reported that it
/// lacks a grant, so it can still answer, most usefully with the request for
/// that grant (Axon's `permit`). It never counts as serving for a call that does
/// not name it, because it is not ready to do the work.
pub(crate) fn takes_named_call(instance: &MachineServiceInstance) -> bool {
    serves(instance)
        || (instance.serves_mcp
            && matches!(instance.state, InstanceState::WaitingForOperator { .. }))
}

/// The note a refusal adds when a machine waits for its operator: it can still
/// be named, which is how the grant it waits on gets asked for.
fn waiting_note<'a>(
    server: &str,
    instances: impl IntoIterator<Item = &'a MachineServiceInstance>,
) -> String {
    let waiting: Vec<&str> = instances
        .into_iter()
        .filter(|instance| takes_named_call(instance) && !serves(instance))
        .map(|instance| instance.machine.as_str())
        .collect();
    match waiting.first() {
        None => String::new(),
        Some(first) => format!(
            " A machine waiting for its operator still takes a call that names it, for example \
             to ask for the grant it waits on: {}.",
            example(server, "permit", first)
        ),
    }
}

/// The `executor.os` vocabulary for a target triple.
fn os_of_target(target: &str) -> Option<&'static str> {
    if target.contains("apple-darwin") {
        Some("macos")
    } else if target.contains("windows") {
        Some("windows")
    } else if target.contains("linux") {
        Some("linux")
    } else {
        None
    }
}

/// The platform `machine` runs, when anything names its target.
pub(crate) fn machine_os(machine: &str, machines: &[MachineCandidate]) -> Option<&'static str> {
    machines
        .iter()
        .find(|candidate| executor_names_match(&candidate.name, machine))
        .and_then(|candidate| candidate.target.as_deref())
        .and_then(os_of_target)
}

/// `local (macos, ready)`: a machine as a refusal names it.
fn describe(instance: &MachineServiceInstance, machines: &[MachineCandidate]) -> String {
    match machine_os(&instance.machine, machines) {
        Some(os) => format!("{} ({os}, {})", instance.machine, instance.state.sentence()),
        None => format!("{} ({})", instance.machine, instance.state.sentence()),
    }
}

fn listing<'a>(
    instances: impl IntoIterator<Item = &'a MachineServiceInstance>,
    machines: &[MachineCandidate],
) -> String {
    instances
        .into_iter()
        .map(|instance| describe(instance, machines))
        .collect::<Vec<_>>()
        .join(", ")
}

fn example(server: &str, tool: &str, machine: &str) -> String {
    format!(
        "run({{executor:{{name:\"{machine}\"}}, commands:[{{target:\"cairn://mcp/{server}/{tool}\", payload:{{args_json:{{}}}}}}]}})"
    )
}

/// The selector keys that place tree-bound work and say nothing about which
/// machine serves a call, when the selector carries one.
fn placement_only_key(selector: &ExecutorSelector) -> Option<&'static str> {
    if !selector.required_toolchains.is_empty() {
        Some("requiredToolchains")
    } else if selector.requires_macos_nested_sandbox_write {
        Some("requiresMacosNestedSandboxWrite")
    } else if selector.containment.is_some() {
        Some("containment")
    } else if !selector.page_call_programs.is_empty() {
        Some("pageCallPrograms")
    } else {
        None
    }
}

/// The one instance of `server` that takes a call to `tool`, or the refusal.
pub(crate) fn route<'a>(
    server: &str,
    tool: &str,
    key: &MachineServiceKey,
    instances: &'a [MachineServiceInstance],
    machines: &[MachineCandidate],
    selector: Option<&ExecutorSelector>,
) -> Result<&'a MachineServiceInstance, String> {
    let selector = selector.filter(|selector| !selector.is_empty());
    if let Some(selector) = selector {
        if let Some(key_name) = placement_only_key(selector) {
            return Err(format!(
                "`executor.{key_name}` places tree-bound work and does not choose which machine \
                 serves `{server}`; a machine-service call takes `executor:{{name}}` or \
                 `executor:{{os}}`."
            ));
        }
        if selector.name.is_some() && selector.os.is_some() {
            return Err(
                "an executor selector states name or os, never both: naming a machine already \
                 settles its platform"
                    .to_string(),
            );
        }
    }
    if instances.is_empty() {
        return Err(format!(
            "`{server}` is not installed on any machine. The operator selects it per machine \
             from Settings → Library (`cairn://packs/{pack}`).",
            pack = key.pack
        ));
    }

    let chosen = match (
        selector.and_then(|selector| selector.name.as_deref()),
        selector.and_then(|selector| selector.os.as_deref()),
    ) {
        (Some(name), _) => {
            let Some(named) = instances
                .iter()
                .find(|instance| executor_names_match(&instance.machine, name))
            else {
                let serving: Vec<&MachineServiceInstance> = instances
                    .iter()
                    .filter(|instance| serves(instance))
                    .collect();
                let on = if serving.is_empty() {
                    format!(
                        "no machine serves it now; it is selected on {}",
                        listing(instances, machines)
                    )
                } else {
                    format!("it serves on {}", listing(serving, machines))
                };
                return Err(format!("`{server}` is not served by `{name}`; {on}."));
            };
            if !takes_named_call(named) {
                return Err(format!(
                    "{} on `{}` is {}.",
                    named.key,
                    named.machine,
                    named.state.sentence()
                ));
            }
            named
        }
        (None, os) => {
            let candidates: Vec<&MachineServiceInstance> = match os {
                Some(os) => instances
                    .iter()
                    .filter(|instance| {
                        machine_os(&instance.machine, machines)
                            .is_some_and(|machine_os| machine_os.eq_ignore_ascii_case(os))
                    })
                    .collect(),
                None => instances.iter().collect(),
            };
            let serving: Vec<&MachineServiceInstance> = candidates
                .iter()
                .copied()
                .filter(|instance| serves(instance))
                .collect();
            match (serving.as_slice(), os) {
                ([only], _) => *only,
                ([], Some(os)) if candidates.is_empty() => {
                    return Err(format!(
                        "`{server}` is selected on no {os} machine; it is selected on {}.",
                        listing(instances, machines)
                    ))
                }
                ([], Some(os)) => {
                    return Err(format!(
                        "`{server}` is served by no {os} machine now: {}.{}",
                        listing(candidates.iter().copied(), machines),
                        waiting_note(server, candidates.iter().copied())
                    ))
                }
                ([], None) => {
                    return Err(format!(
                        "`{server}` is served by no machine now: {}.{}",
                        listing(instances, machines),
                        waiting_note(server, instances)
                    ))
                }
                ([first, ..], _) => {
                    let narrowed = os.map(|os| format!(" {os}")).unwrap_or_default();
                    let mut refusal = format!(
                        "`{server}` is served by several{narrowed} machines, and a call drives \
                         exactly one: {}. Name one with the batch's `executor`, for example \
                         {}. A node's placement default does not choose this.",
                        listing(serving.iter().copied(), machines),
                        example(server, tool, &first.machine)
                    );
                    let idle: Vec<&MachineServiceInstance> = instances
                        .iter()
                        .filter(|instance| !serves(instance))
                        .collect();
                    if !idle.is_empty() {
                        refusal.push_str(&format!(
                            " Selected but not serving: {}.",
                            listing(idle, machines)
                        ));
                    }
                    return Err(refusal);
                }
            }
        }
    };

    // An empty catalog is no evidence: a probe that failed and one that has
    // not run yet both leave one. Only a listing that came back can refute.
    if !chosen.catalog.is_empty() && !chosen.catalog.iter().any(|item| item.name == tool) {
        let mut tools: Vec<&str> = chosen
            .catalog
            .iter()
            .map(|item| item.name.as_str())
            .collect();
        tools.sort_unstable();
        return Err(format!(
            "`{}` does not advertise `{tool}` for `{server}`; its tools are: {}. Read \
             cairn://mcp/{server} for each machine's catalog.",
            chosen.machine,
            tools.join(", ")
        ));
    }
    Ok(chosen)
}

/// Route every machine-service item in a batch, before any item runs: set each
/// one's machine and name it in the item's header, or refuse the whole batch,
/// naming every item that could not be routed.
///
/// `instances_for` reads a service's instance set; it is asked once per key.
pub(crate) fn route_batch(
    resolved: &mut [(String, Result<RunSpec, String>)],
    selector: Option<&ExecutorSelector>,
    instances_for: impl Fn(&MachineServiceKey) -> Vec<MachineServiceInstance>,
    machines: &[MachineCandidate],
) -> Result<(), String> {
    let mut sets: BTreeMap<MachineServiceKey, Vec<MachineServiceInstance>> = BTreeMap::new();
    let mut refusals = Vec::new();
    for (index, (header, spec)) in resolved.iter_mut().enumerate() {
        let Ok(RunSpec::McpCall(call)) = spec else {
            continue;
        };
        let McpRoute::MachineService { key, machine } = &mut call.route else {
            continue;
        };
        let instances = sets
            .entry(key.clone())
            .or_insert_with(|| instances_for(key));
        match route(&call.server, &call.tool, key, instances, machines, selector) {
            Ok(chosen) => {
                *header = format!("{header} on {}", chosen.machine);
                *machine = Some(chosen.machine.clone());
            }
            Err(refusal) => refusals.push(format!("commands[{index}] ({header}): {refusal}")),
        }
    }
    if refusals.is_empty() {
        Ok(())
    } else {
        Err(refusals.join("\n"))
    }
}

/// Why a batch's call-level `executor` cannot apply, when it is not a
/// tree-bound batch: it chooses a machine only for machine-service calls, so
/// every other host item is named.
pub(crate) fn host_selector_refusal(
    resolved: &[(String, Result<RunSpec, String>)],
) -> Option<String> {
    let offending: Vec<String> = resolved
        .iter()
        .enumerate()
        .filter(|(_, (_, spec))| match spec {
            Ok(RunSpec::McpCall(call)) => !matches!(call.route, McpRoute::MachineService { .. }),
            Ok(_) => true,
            Err(_) => false,
        })
        .map(|(index, (header, _))| format!("commands[{index}] ({header})"))
        .collect();
    (!offending.is_empty()).then(|| {
        format!(
            "The executor option places tree-bound shell or script batches, and chooses which \
             machine serves a machine-service MCP call such as `cairn://mcp/axon/<tool>`. These \
             items run on the host and take no machine: {}. Split them into a run call without \
             executor.",
            offending.join(", ")
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::gateway::McpToolDef;
    use crate::mcp::handlers::run::types::McpCallSpec;

    fn key() -> MachineServiceKey {
        "axon/axon".parse().unwrap()
    }

    fn tool(name: &str) -> McpToolDef {
        McpToolDef {
            name: name.into(),
            description: None,
            input_schema: serde_json::json!({}),
        }
    }

    fn instance(machine: &str, state: InstanceState, tools: &[&str]) -> MachineServiceInstance {
        MachineServiceInstance {
            key: key(),
            machine: machine.into(),
            state,
            provenance: "installed 0.3.6 (pack)".into(),
            installed_version: Some("0.3.6".into()),
            override_command: None,
            catalog: tools.iter().map(|name| tool(name)).collect(),
            probed_at_ms: None,
            serves_mcp: true,
            facade: None,
            fleet: false,
            launch: None,
        }
    }

    fn waiting() -> InstanceState {
        InstanceState::WaitingForOperator {
            reason: "accessibility-permission-denied".into(),
        }
    }

    fn machines() -> Vec<MachineCandidate> {
        [
            ("local", "aarch64-apple-darwin"),
            ("bgsrv-mac", "aarch64-apple-darwin"),
            ("bglab-ub", "x86_64-unknown-linux-gnu"),
        ]
        .into_iter()
        .map(|(name, target)| MachineCandidate {
            name: name.into(),
            target: Some(target.into()),
            attached: true,
        })
        .collect()
    }

    fn named(name: &str) -> ExecutorSelector {
        ExecutorSelector {
            name: Some(name.into()),
            ..ExecutorSelector::default()
        }
    }

    fn on_os(os: &str) -> ExecutorSelector {
        ExecutorSelector {
            os: Some(os.into()),
            ..ExecutorSelector::default()
        }
    }

    fn routed<'a>(
        instances: &'a [MachineServiceInstance],
        selector: Option<&ExecutorSelector>,
    ) -> Result<&'a str, String> {
        route("axon", "look", &key(), instances, &machines(), selector)
            .map(|instance| instance.machine.as_str())
    }

    #[test]
    fn one_serving_machine_takes_a_call_with_no_selector() {
        let instances = [
            instance("local", InstanceState::Ready, &["look"]),
            instance("bgsrv-mac", waiting(), &[]),
        ];
        assert_eq!(routed(&instances, None), Ok("local"));
        // A degraded machine is in the routing set.
        let degraded = [instance(
            "local",
            InstanceState::Degraded {
                reason: "probe timed out".into(),
            },
            &[],
        )];
        assert_eq!(routed(&degraded, None), Ok("local"));
    }

    #[test]
    fn several_serving_machines_and_no_selector_are_refused_naming_each() {
        let instances = [
            instance("local", InstanceState::Ready, &["look"]),
            instance("bgsrv-mac", InstanceState::Ready, &["look"]),
            instance("bglab-ub", waiting(), &[]),
        ];
        let refusal = routed(&instances, None).unwrap_err();
        assert!(
            refusal.contains("local (macos, ready), bgsrv-mac (macos, ready)"),
            "{refusal}"
        );
        assert!(
            refusal.contains(
                "run({executor:{name:\"local\"}, commands:[{target:\"cairn://mcp/axon/look\", payload:{args_json:{}}}]})"
            ),
            "{refusal}"
        );
        assert!(
            refusal.contains("A node's placement default does not choose this"),
            "{refusal}"
        );
        assert!(
            refusal.contains("Selected but not serving: bglab-ub (linux, waiting for the operator"),
            "{refusal}"
        );
    }

    #[test]
    fn no_serving_machine_names_each_machine_with_its_state() {
        let instances = [
            instance("local", waiting(), &[]),
            instance("bgsrv-mac", InstanceState::Starting, &[]),
        ];
        let refusal = routed(&instances, None).unwrap_err();
        assert!(refusal.contains("served by no machine now"), "{refusal}");
        assert!(
            refusal
                .contains("local (macos, waiting for the operator: Accessibility is not granted"),
            "{refusal}"
        );
        assert!(refusal.contains("bgsrv-mac (macos, starting)"), "{refusal}");
        assert!(
            refusal.contains(
                "still takes a call that names it, for example to ask for the grant it waits on: run({executor:{name:\"local\"}, commands:[{target:\"cairn://mcp/axon/permit\""
            ),
            "{refusal}"
        );
    }

    #[test]
    fn a_name_that_does_not_serve_is_refused_naming_the_machines_that_do() {
        let instances = [
            instance("local", InstanceState::Ready, &["look"]),
            instance("bgsrv-mac", waiting(), &[]),
        ];
        let unselected = routed(&instances, Some(&named("bglab-ub"))).unwrap_err();
        assert_eq!(
            unselected,
            "`axon` is not served by `bglab-ub`; it serves on local (macos, ready)."
        );
        // A machine waiting for its operator takes a call that names it (the
        // request for its grant), but one that is merely starting does not.
        assert_eq!(
            routed(&instances, Some(&named("BGSRV-Mac"))),
            Ok("bgsrv-mac")
        );
        let starting = [instance("bgsrv-mac", InstanceState::Starting, &[])];
        assert_eq!(
            routed(&starting, Some(&named("bgsrv-mac"))).unwrap_err(),
            "axon/axon on `bgsrv-mac` is starting."
        );
        assert_eq!(
            routed(&instances, Some(&named("local"))),
            Ok("local"),
            "a named serving machine is taken"
        );
    }

    #[test]
    fn an_os_selector_narrows_then_takes_one_or_refuses() {
        let instances = [
            instance("local", InstanceState::Ready, &["look"]),
            instance("bgsrv-mac", InstanceState::Ready, &["look"]),
            instance("bglab-ub", InstanceState::Ready, &["look"]),
        ];
        assert_eq!(routed(&instances, Some(&on_os("Linux"))), Ok("bglab-ub"));
        let several = routed(&instances, Some(&on_os("macos"))).unwrap_err();
        assert!(
            several.contains("served by several macos machines"),
            "{several}"
        );
        let none = routed(&instances, Some(&on_os("windows"))).unwrap_err();
        assert!(none.contains("selected on no windows machine"), "{none}");
        assert!(none.contains("bglab-ub (linux, ready)"), "{none}");
    }

    #[test]
    fn no_instance_at_all_points_at_the_library() {
        let refusal = routed(&[], None).unwrap_err();
        assert!(
            refusal.contains("not installed on any machine"),
            "{refusal}"
        );
        assert!(refusal.contains("`cairn://packs/axon`"), "{refusal}");
    }

    #[test]
    fn a_placement_only_key_is_refused_by_name() {
        let instances = [instance("local", InstanceState::Ready, &["look"])];
        let selector = ExecutorSelector {
            required_toolchains: vec!["bun".into()],
            ..ExecutorSelector::default()
        };
        let refusal = routed(&instances, Some(&selector)).unwrap_err();
        assert!(
            refusal.contains("`executor.requiredToolchains`"),
            "{refusal}"
        );
        assert!(refusal.contains("`executor:{name}`"), "{refusal}");
    }

    #[test]
    fn only_a_catalog_that_came_back_refutes_a_tool() {
        let listed = [instance("local", InstanceState::Ready, &["click", "look"])];
        let refusal = route("axon", "permit", &key(), &listed, &machines(), None).unwrap_err();
        assert_eq!(
            refusal,
            "`local` does not advertise `permit` for `axon`; its tools are: click, look. Read cairn://mcp/axon for each machine's catalog."
        );
        let unprobed = [instance("local", InstanceState::Ready, &[])];
        assert!(route("axon", "permit", &key(), &unprobed, &machines(), None).is_ok());
    }

    fn axon_item(tool: &str) -> (String, Result<RunSpec, String>) {
        (
            format!("cairn://mcp/axon/{tool}"),
            Ok(RunSpec::McpCall(Box::new(McpCallSpec {
                server: "axon".into(),
                tool: tool.into(),
                args: serde_json::json!({}),
                timeout: None,
                route: McpRoute::MachineService {
                    key: key(),
                    machine: None,
                },
            }))),
        )
    }

    fn routed_machine(item: &(String, Result<RunSpec, String>)) -> Option<&str> {
        match &item.1 {
            Ok(RunSpec::McpCall(call)) => match &call.route {
                McpRoute::MachineService { machine, .. } => machine.as_deref(),
                McpRoute::Gateway { .. } => None,
            },
            _ => None,
        }
    }

    /// Every refused item is named in the one refusal, and the instance set is
    /// read once per service however many items name it.
    #[test]
    fn a_batch_refusal_names_every_item_that_could_not_be_routed() {
        let reads = std::cell::Cell::new(0);
        let instances_for = |_: &MachineServiceKey| {
            reads.set(reads.get() + 1);
            vec![
                instance("local", InstanceState::Ready, &["look", "click"]),
                instance("bgsrv-mac", InstanceState::Ready, &["look", "click"]),
            ]
        };
        let mut batch = vec![
            axon_item("look"),
            ("echo".into(), Err("bad item".into())),
            axon_item("click"),
        ];
        let refusal = route_batch(&mut batch, None, instances_for, &machines()).unwrap_err();
        assert_eq!(reads.get(), 1);
        let lines: Vec<&str> = refusal.lines().collect();
        assert_eq!(lines.len(), 2, "{refusal}");
        assert!(
            lines[0].starts_with("commands[0] (cairn://mcp/axon/look): "),
            "{refusal}"
        );
        assert!(
            lines[1].starts_with("commands[2] (cairn://mcp/axon/click): "),
            "{refusal}"
        );
        assert_eq!(routed_machine(&batch[0]), None, "nothing was routed");
    }

    #[test]
    fn a_routed_batch_sets_each_items_machine_and_names_it_in_the_header() {
        let instances_for = |_: &MachineServiceKey| {
            vec![
                instance("local", InstanceState::Ready, &[]),
                instance("bgsrv-mac", InstanceState::Ready, &[]),
            ]
        };
        let mut batch = vec![axon_item("look"), axon_item("click")];
        route_batch(
            &mut batch,
            Some(&named("bgsrv-mac")),
            instances_for,
            &machines(),
        )
        .unwrap();
        for item in &batch {
            assert_eq!(routed_machine(item), Some("bgsrv-mac"));
            assert!(item.0.ends_with(" on bgsrv-mac"), "{}", item.0);
        }
    }

    #[test]
    fn the_selector_on_a_host_batch_is_refused_naming_each_item_it_cannot_place() {
        let gateway = (
            "cairn://mcp/github/search".to_string(),
            Ok(RunSpec::McpCall(Box::new(McpCallSpec {
                server: "github".into(),
                tool: "search".into(),
                args: serde_json::json!({}),
                timeout: None,
                route: McpRoute::Gateway {
                    credential_key: "github".into(),
                    config: Box::new(serde_json::from_value(serde_json::json!({})).unwrap()),
                },
            }))),
        );
        assert_eq!(host_selector_refusal(&[axon_item("look")]), None);
        let refusal = host_selector_refusal(&[axon_item("look"), gateway]).unwrap();
        assert!(
            refusal.contains("take no machine: commands[1] (cairn://mcp/github/search)"),
            "{refusal}"
        );
    }
}
