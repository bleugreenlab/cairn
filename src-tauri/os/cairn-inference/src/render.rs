//! Rendering a profile into the scripts its head runs.
//!
//! The executor never sees an engine: it runs a start script, polls a health
//! URL, and runs a stop script (`DeploymentOperation::{Start, Stop}`). So a
//! container profile is rendered here, orch-side, into those scripts, and the
//! wire is unchanged.
//!
//! Only the head acts. For a profile spanning machines the head's start script
//! reaches each peer over `ssh -o BatchMode=yes` and starts its rank there,
//! peers first and the head (rank 0) last, so the head's rendezvous finds its
//! peers waiting. On every machine it checks out the profile's pinned source
//! files, fetches and verifies pinned GGUF weights where the server cannot pin
//! a revision itself, removes any stale container of the same name, and runs
//! `cairn-<deployment>-r<rank>` detached with `--restart no`: what runs after
//! a reboot is placement's decision, not the container runtime's. The head then
//! waits for its own health URL, and if its container exits first it prints
//! the engine's error lines and fails, so the executor's report quotes them.
//!
//! Stop and the failed-start cleanup remove every rank's container, which is
//! idempotent.
//!
//! Every value placed in a script is single-quoted by [`quote`], so no profile
//! field reaches the shell as syntax. An argument or env value may span lines
//! (a published compose command often does); a peer's script travels in a
//! heredoc whose delimiter is lengthened until no line of the script equals
//! it. Names, paths, images and addresses may not hold a newline.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::control::Refusal;
use crate::deployment::Deployment;
use crate::inventory::ValidationError;
use crate::machine::MachineName;
use crate::profile::{
    placeholders, secret_references, AcceleratorFamily, AcceleratorRuntime, Container, Engine,
    InferenceProfile, MountSource, Server, WeightFile,
};

/// What the head runs for one deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rendered {
    pub start: String,
    /// `None` for a command profile that names no stop command.
    pub stop: Option<String>,
    /// What runs after a failed start, on the head.
    pub cleanup: Option<String>,
}

/// One machine of a deployment, as the renderer reaches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineAt {
    pub name: MachineName,
    /// Where the head reaches it over ssh. Needed for every peer; the head's
    /// own is used only for its health URL.
    pub address: Option<String>,
    /// Its address on the pool's fast link, which `{head.link}` and
    /// `{self.link}` fill in.
    pub link: Option<String>,
}

/// Single-quote `value` for a POSIX shell.
pub fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

/// A container's name for one rank of a deployment: what docker and podman
/// accept, the deployment id with anything else made a dash.
pub fn container_name(deployment: &str, rank: usize) -> String {
    let id: String = deployment
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    format!("cairn-{id}-r{rank}")
}

/// Where the renderer checks out a profile's pinned source files, under the
/// executor account's home.
const SOURCES: &str = "$HOME/.cairn/inference/sources";

/// A repository's directory within a Hugging Face cache: everything the hub
/// library keeps of it (`blobs/`, `refs/`, `snapshots/`), and all a container
/// is given of the cache.
pub fn cache_dir(repo: &str) -> String {
    format!("hub/models--{}", repo.replace('/', "--"))
}

fn cached_path(cache: &str, repo: &str, revision: &str, path: &str) -> String {
    format!(
        "{}/{}/snapshots/{revision}/{path}",
        cache.trim_end_matches('/'),
        cache_dir(repo)
    )
}

/// The in-container path of a weight file, in a Hugging Face cache mounted at
/// `cache`.
pub fn cached_weight_path(cache: &str, weight: &WeightFile) -> String {
    cached_path(cache, &weight.repo, &weight.revision, &weight.path)
}

/// Render `profile` placed as `deployment`, its machines given in the
/// deployment's order.
pub fn render(
    profile: &InferenceProfile,
    deployment: &Deployment,
    machines: &[MachineAt],
) -> Result<Rendered, Refusal> {
    for (index, weight) in profile.model.weights.iter().enumerate() {
        if let Some(problem) = weight.problem() {
            return Err(Refusal(format!(
                "profile {} weight file {index}: {problem}",
                profile.id
            )));
        }
    }
    for (index, file) in profile.model.config_files.iter().enumerate() {
        if let Some(problem) = file.problem() {
            return Err(Refusal(format!(
                "profile {} config file {index}: {problem}",
                profile.id
            )));
        }
    }
    match &profile.engine {
        Engine::Command { start, stop, .. } => Ok(Rendered {
            start: start.clone(),
            stop: stop.clone(),
            cleanup: stop.clone(),
        }),
        Engine::Native { .. } => Err(Refusal(format!(
            "profile {} uses a native engine, which Cairn cannot start yet",
            profile.id
        ))),
        Engine::Container(container) => {
            if container.server == Server::Vllm
                && mounts_cache(container)
                && profile.model.config_files.is_empty()
            {
                return Err(Refusal(format!(
                    "profile {} runs vLLM on the pinned weights alone: it pins none of its repository's configuration, tokenizer and chat-template files (model.configFiles), and its container reaches only the pinned files, read-only, so vLLM could not load it. Generate or import the profile again, which pins them",
                    profile.id
                )));
            }
            let ranks = Ranks::of(profile, deployment, machines)?;
            let start = start_script(profile, container, deployment, &ranks)?;
            let stop = stop_script(&ranks, deployment);
            Ok(Rendered {
                start,
                stop: Some(stop.clone()),
                cleanup: Some(stop),
            })
        }
    }
}

/// The deployment's machines in rank order: the head first, then the rest in
/// the deployment's order.
struct Ranks<'a> {
    machines: Vec<&'a MachineAt>,
}

impl<'a> Ranks<'a> {
    fn of(
        profile: &InferenceProfile,
        deployment: &Deployment,
        machines: &'a [MachineAt],
    ) -> Result<Self, Refusal> {
        let find = |name: &MachineName| {
            machines.iter().find(|m| &m.name == name).ok_or_else(|| {
                Refusal(format!(
                    "machine {name} of deployment {} was not given to the renderer",
                    deployment.id
                ))
            })
        };
        let mut ordered = vec![find(&deployment.head)?];
        for name in deployment
            .machines
            .iter()
            .filter(|m| **m != deployment.head)
        {
            ordered.push(find(name)?);
        }
        if ordered.len() != profile.requirements.machines as usize {
            return Err(Refusal(format!(
                "profile {} runs on {} machines but deployment {} places it on {}",
                profile.id,
                profile.requirements.machines,
                deployment.id,
                ordered.len()
            )));
        }
        for peer in &ordered[1..] {
            if peer.address.is_none() {
                return Err(Refusal(format!(
                    "machine {}'s address is unknown, so the head cannot reach it over ssh: it has no address on the pool's link, no attached executor of that name and no machine record",
                    peer.name
                )));
            }
        }
        Ok(Self { machines: ordered })
    }
}

/// The values placeholders fill in for one rank.
fn values(
    profile: &InferenceProfile,
    container: &Container,
    deployment: &Deployment,
    ranks: &Ranks<'_>,
    rank: usize,
) -> BTreeMap<&'static str, Option<String>> {
    let (model, revision) = profile.model.weights_source();
    let weights = container
        .mounts
        .iter()
        .find(|mount| matches!(mount.from, MountSource::HfCache))
        .zip(profile.model.weights.first())
        .map(|(mount, weight)| cached_weight_path(&mount.to, weight));
    BTreeMap::from([
        ("port", Some(deployment.api_port.to_string())),
        ("servedName", Some(profile.served_name.clone())),
        ("model", Some(model.to_string())),
        ("revision", revision.map(str::to_string)),
        ("rank", Some(rank.to_string())),
        ("machines", Some(ranks.machines.len().to_string())),
        ("head.link", ranks.machines[0].link.clone()),
        ("self.link", ranks.machines[rank].link.clone()),
        ("weights", weights),
    ])
}

fn why_unfilled(name: &str, ranks: &Ranks<'_>, rank: usize) -> String {
    match name {
        "revision" => "the profile pins no weight file".into(),
        "head.link" => format!(
            "the head {} has no link address: it reports no fast link joining it to a pool",
            ranks.machines[0].name
        ),
        "self.link" => format!(
            "machine {} has no link address: it reports no fast link joining it to a pool",
            ranks.machines[rank].name
        ),
        "weights" => "the profile mounts no Hugging Face cache or pins no weight file".into(),
        _ => "it has no value".into(),
    }
}

/// `text` with every placeholder filled in.
fn fill(
    profile: &InferenceProfile,
    text: &str,
    values: &BTreeMap<&'static str, Option<String>>,
    ranks: &Ranks<'_>,
    rank: usize,
) -> Result<String, Refusal> {
    let mut out = text.to_string();
    for name in placeholders(text) {
        let value = match values.get(name) {
            Some(Some(value)) => value,
            Some(None) => {
                return Err(Refusal(format!(
                    "profile {} uses {{{name}}}, which cannot be filled in: {}",
                    profile.id,
                    why_unfilled(name, ranks, rank)
                )))
            }
            None => {
                return Err(Refusal(format!(
                    "profile {} uses unknown placeholder {{{name}}}",
                    profile.id
                )))
            }
        };
        out = out.replace(&format!("{{{name}}}"), value);
    }
    Ok(out)
}

fn no_newline(profile: &InferenceProfile, value: &str) -> Result<(), Refusal> {
    if value.contains('\n') {
        return Err(Refusal(format!(
            "profile {} has a value holding a newline, which Cairn does not put in a script: {value:?}",
            profile.id
        )));
    }
    Ok(())
}

fn mounts_cache(container: &Container) -> bool {
    container
        .mounts
        .iter()
        .any(|mount| mount.from == MountSource::HfCache)
}

/// `hf_get <url> <file>`: download from Hugging Face on the host, with the
/// machine's token when it has one (`$HF_TOKEN`, else the cache's `token`
/// file), so a gated repository's files arrive without the container ever
/// seeing the token. The header travels through a file descriptor, never
/// argv, and curl drops it on a redirect to another host.
const HF_GET: &str = r#"hf_get() { if [ -z "${HF_TOKEN:-}" ] && [ -s "$hf/token" ]; then HF_TOKEN="$(cat "$hf/token")"; fi; if [ -n "${HF_TOKEN:-}" ]; then curl -fsSL --retry 3 -H @<(printf 'Authorization: Bearer %s\n' "$HF_TOKEN") -o "$2" "$1"; else curl -fsSL --retry 3 -o "$2" "$1"; fi; }"#;

const RUNTIME: &str = r#"if command -v docker >/dev/null 2>&1; then rt=docker; elif command -v podman >/dev/null 2>&1; then rt=podman; else echo "neither docker nor podman is installed on $(hostname)" >&2; exit 1; fi"#;

/// The script one rank runs on its own machine.
fn rank_script(
    profile: &InferenceProfile,
    container: &Container,
    deployment: &Deployment,
    ranks: &Ranks<'_>,
    rank: usize,
) -> Result<String, Refusal> {
    let values = values(profile, container, deployment, ranks, rank);
    let fill = |text: &str| fill(profile, text, &values, ranks, rank);
    let name = container_name(deployment.id.as_str(), rank);
    let mut out = String::new();
    out.push_str("set -euo pipefail\n");
    out.push_str(RUNTIME);
    out.push('\n');
    let cache = mounts_cache(container);
    if cache {
        out.push_str("hf=\"${HF_HOME:-$HOME/.cache/huggingface}\"\n");
        out.push_str(HF_GET);
        out.push('\n');
    }

    if let Some(files) = &container.files {
        no_newline(profile, &files.url)?;
        let _ = writeln!(out, "src=\"{SOURCES}/{}\"", files.commit);
        let _ = writeln!(
            out,
            "if [ ! -d \"$src/.git\" ]; then rm -rf \"$src.part\"; mkdir -p \"$src.part\"; git -C \"$src.part\" init -q; git -C \"$src.part\" fetch -q --depth 1 {} {}; git -C \"$src.part\" checkout -q FETCH_HEAD; mv \"$src.part\" \"$src\"; fi",
            quote(&files.url),
            quote(&files.commit)
        );
    }

    // Cairn fetches every pinned file before the container starts, since
    // the container reaches the cache only read-only; each is verified
    // against its pin and lands whole or not at all.
    if cache {
        let fetch = |out: &mut String, repo: &str, revision: &str, path: &str, verify: String| {
            let url = format!("https://huggingface.co/{repo}/resolve/{revision}/{path}");
            let _ = writeln!(
                out,
                "f=\"$hf\"/{}\nif [ ! -f \"$f\" ]; then mkdir -p \"$(dirname \"$f\")\"; hf_get {} \"$f.part\"; {verify}; mv \"$f.part\" \"$f\"; fi",
                quote(cached_path("", repo, revision, path).trim_start_matches('/')),
                quote(&url),
            );
        };
        for file in &profile.model.config_files {
            let verify = match (&file.sha256, &file.git_blob) {
                (Some(sha256), _) => {
                    format!("echo {}\"  $f.part\" | sha256sum -c --quiet -", quote(sha256))
                }
                (None, Some(blob)) => format!(
                    "[ \"$({{ printf 'blob %d\\0' \"$(wc -c <\"$f.part\")\"; cat \"$f.part\"; }} | sha1sum | cut -d ' ' -f 1)\" = {} ] || {{ echo \"$f.part does not match its pinned git blob\" >&2; exit 1; }}",
                    quote(blob)
                ),
                (None, None) => unreachable!("checked by ConfigFile::problem"),
            };
            fetch(&mut out, &file.repo, &file.revision, &file.path, verify);
        }
        for weight in &profile.model.weights {
            let verify = format!(
                "echo {}\"  $f.part\" | sha256sum -c --quiet -",
                quote(&weight.sha256)
            );
            fetch(
                &mut out,
                &weight.repo,
                &weight.revision,
                &weight.path,
                verify,
            );
        }
        for repo in profile.model.repos() {
            let _ = writeln!(out, "mkdir -p \"$hf\"/{}", quote(&cache_dir(repo)));
        }
    }

    let _ = writeln!(
        out,
        "\"$rt\" rm -f {} >/dev/null 2>&1 || true",
        quote(&name)
    );

    let mut flags: Vec<String> = vec![
        "-d".into(),
        "--restart".into(),
        "no".into(),
        "--name".into(),
        quote(&name),
    ];
    let host = &container.host;
    if host.ipc_host {
        flags.extend(["--ipc".into(), "host".into()]);
    }
    if host.network_host {
        flags.extend(["--network".into(), "host".into()]);
    } else {
        let port = deployment.api_port;
        flags.extend(["-p".into(), format!("{port}:{port}")]);
    }
    if let Some(size) = &host.shm_size {
        no_newline(profile, size)?;
        flags.extend(["--shm-size".into(), quote(size)]);
    }
    let mut runtime_flags = String::new();
    if host.gpus {
        let accelerator = profile.requirements.accelerator.ok_or_else(|| {
            Refusal(format!(
                "profile {} gives its container the GPUs but states no accelerator, so Cairn cannot tell how",
                profile.id
            ))
        })?;
        match accelerator.family {
            AcceleratorFamily::Nvidia => {
                runtime_flags.push_str(
                    "if [ \"$rt\" = docker ]; then gpu=(--gpus all); else gpu=(--device nvidia.com/gpu=all); fi\n",
                );
                flags.push("\"${gpu[@]}\"".into());
            }
            AcceleratorFamily::Amd => {
                flags.extend(["--device".into(), "/dev/dri".into()]);
                if accelerator.runtime == AcceleratorRuntime::Rocm {
                    // ROCm's documented container surface: the compute
                    // interface beside the render nodes, the video group, and
                    // no seccomp filter, since the default profiles refuse the
                    // NUMA memory-policy calls (mbind, set_mempolicy) the HSA
                    // runtime makes. Podman refuses any group beside
                    // keep-groups, which already carries the account's own
                    // video and render membership into a rootless container.
                    flags.extend([
                        "--device".into(),
                        "/dev/kfd".into(),
                        "--security-opt".into(),
                        "seccomp=unconfined".into(),
                    ]);
                    runtime_flags.push_str(
                        "if [ \"$rt\" = docker ]; then gpu=(--group-add video); else gpu=(); fi\n",
                    );
                    flags.push("\"${gpu[@]}\"".into());
                }
            }
            AcceleratorFamily::Apple => {
                return Err(Refusal(format!(
                    "profile {} runs a container on Apple hardware, which has no GPU passthrough",
                    profile.id
                )))
            }
        }
    }
    for device in &host.devices {
        no_newline(profile, device)?;
        flags.extend(["--device".into(), quote(device)]);
    }
    for (name, limit) in &host.ulimits {
        no_newline(profile, name)?;
        no_newline(profile, limit)?;
        flags.extend(["--ulimit".into(), quote(&format!("{name}={limit}"))]);
    }
    runtime_flags.push_str(
        "if [ \"$rt\" = podman ]; then extra=(--security-opt label=disable --group-add keep-groups); else extra=(); fi\n",
    );
    flags.push("\"${extra[@]}\"".into());
    for mount in &container.mounts {
        no_newline(profile, &mount.to)?;
        let host_side = match &mount.from {
            // Each pinned repository's own directory, read-only, at its
            // place under `to`: never the cache itself, which holds the
            // token and every other model's weights.
            MountSource::HfCache => {
                for repo in profile.model.repos() {
                    let dir = cache_dir(repo);
                    flags.extend([
                        "-v".into(),
                        format!(
                            "\"$hf\"/{}:{}",
                            quote(&dir),
                            quote(&format!("{}/{dir}:ro", mount.to.trim_end_matches('/')))
                        ),
                    ]);
                }
                continue;
            }
            MountSource::Source { path } => {
                no_newline(profile, path)?;
                let path = path.trim_start_matches("./");
                if path.is_empty() || path == "." {
                    "\"$src\"".to_string()
                } else {
                    format!("\"$src\"/{}", quote(path))
                }
            }
        };
        let ro = if mount.read_only { ":ro" } else { "" };
        flags.extend([
            "-v".into(),
            format!("{host_side}:{}", quote(&format!("{}{ro}", mount.to))),
        ]);
    }
    let mut env = container.env.clone();
    if let Some(rank_env) = container.ranks.get(rank) {
        env.extend(rank_env.clone());
    }
    for (key, value) in &env {
        no_newline(profile, key)?;
        // Validation refuses this too; a record written before it did must
        // still not start a container holding a reference as its credential.
        if let Some(reference) = secret_references(value).first() {
            let from_rank = container
                .ranks
                .get(rank)
                .is_some_and(|env| env.contains_key(key));
            return Err(Refusal(
                ValidationError::UnresolvedSecret {
                    profile: profile.id.clone(),
                    key: key.clone(),
                    rank: from_rank.then_some(rank),
                    reference: reference.to_string(),
                }
                .to_string(),
            ));
        }
        flags.extend(["-e".into(), quote(&format!("{key}={}", fill(value)?))]);
    }
    if cache {
        // Every file is already there and the mount is read-only: the
        // server must not try to fetch or write anything into it. Last, so
        // the profile's env cannot undo it.
        flags.extend(["-e".into(), quote("HF_HUB_OFFLINE=1")]);
    }
    let mut command: Vec<String> = Vec::new();
    match &container.entrypoint {
        None => {}
        Some(entrypoint) => match entrypoint.split_first() {
            None => flags.extend(["--entrypoint".into(), "''".into()]),
            Some((first, rest)) => {
                no_newline(profile, first)?;
                flags.extend(["--entrypoint".into(), quote(first)]);
                for part in rest {
                    command.push(quote(&fill(part)?));
                }
            }
        },
    }
    no_newline(profile, &container.image)?;
    for arg in &container.args {
        command.push(quote(&fill(arg)?));
    }
    out.push_str(&runtime_flags);
    let _ = writeln!(
        out,
        "\"$rt\" run {} {}{}",
        flags.join(" "),
        quote(&container.image),
        command
            .iter()
            .map(|part| format!(" {part}"))
            .collect::<String>()
    );
    Ok(out)
}

/// How many seconds the head's start waits between health checks.
const HEALTH_POLL_SECS: u32 = 5;

fn start_script(
    profile: &InferenceProfile,
    container: &Container,
    deployment: &Deployment,
    ranks: &Ranks<'_>,
) -> Result<String, Refusal> {
    let mut out = format!(
        "#!/usr/bin/env bash\n# Rendered by Cairn from profile {} for deployment {}.\nset -euo pipefail\n",
        profile.id, deployment.id
    );
    for (rank, peer) in ranks.machines.iter().enumerate().skip(1) {
        let script = rank_script(profile, container, deployment, ranks, rank)?;
        let address = peer.address.as_deref().expect("checked by Ranks::of");
        no_newline(profile, address)?;
        let mut delimiter = format!("CAIRN_RANK_{rank}");
        while script.lines().any(|line| line == delimiter) {
            delimiter.push('_');
        }
        let _ = writeln!(
            out,
            "# rank {rank} on {}\nssh -o BatchMode=yes {} 'bash -ls' <<'{delimiter}'\n{script}{delimiter}",
            peer.name,
            quote(address)
        );
    }
    let _ = writeln!(out, "# rank 0 on {}", ranks.machines[0].name);
    out.push_str(&rank_script(profile, container, deployment, ranks, 0)?);
    let name = quote(&container_name(deployment.id.as_str(), 0));
    no_newline(profile, &deployment.health_path)?;
    let health = quote(&format!(
        "http://127.0.0.1:{}{}",
        deployment.api_port, deployment.health_path
    ));
    let _ = writeln!(
        out,
        "while true; do\n  if curl -fsS -o /dev/null --max-time 5 {health}; then exit 0; fi\n  if [ \"$(\"$rt\" inspect -f '{{{{.State.Running}}}}' {name} 2>/dev/null)\" != true ]; then\n    echo \"container {} exited before it was healthy; its last errors:\"\n    \"$rt\" logs --tail 400 {name} 2>&1 | grep -E 'Error|error|Exception' | tail -n 6 || \"$rt\" logs --tail 10 {name} 2>&1\n    exit 1\n  fi\n  sleep {HEALTH_POLL_SECS}\ndone",
        container_name(deployment.id.as_str(), 0)
    );
    Ok(out)
}

fn stop_script(ranks: &Ranks<'_>, deployment: &Deployment) -> String {
    let mut out = format!(
        "#!/usr/bin/env bash\n# Rendered by Cairn for deployment {}: remove every rank's container.\nset -uo pipefail\n{RUNTIME}\n",
        deployment.id
    );
    let _ = writeln!(
        out,
        "\"$rt\" rm -f {} >/dev/null 2>&1 || true",
        quote(&container_name(deployment.id.as_str(), 0))
    );
    for (rank, peer) in ranks.machines.iter().enumerate().skip(1) {
        let address = peer.address.as_deref().unwrap_or_default();
        let remote = format!(
            "{RUNTIME}; \"$rt\" rm -f {} >/dev/null 2>&1 || true",
            quote(&container_name(deployment.id.as_str(), rank))
        );
        let _ = writeln!(
            out,
            "ssh -o BatchMode=yes {} {} || echo {}",
            quote(address),
            quote(&format!("bash -lc {}", quote(&remote))),
            quote(&format!(
                "could not reach {} to remove rank {rank}",
                peer.name
            ))
        );
    }
    out.push_str("exit 0\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::{
        Accelerator, ConfigFile, HostOptions, ModelRef, Mount, ProfileOrigin, Requirements,
        SourcePin,
    };
    use crate::record::RecordSource;

    const SHA: &str = "a83948492cf13df455170fb42885f5ef4db54fefe0feff0f841ecbff464ac9d8";

    fn weight(repo: &str, path: &str) -> WeightFile {
        WeightFile {
            repo: repo.into(),
            revision: "6821d6ad3681a4b137b066b76094fa82ebd0a380".into(),
            path: path.into(),
            sha256: "0".repeat(64),
            size: Some(1 << 30),
        }
    }

    fn deployment(id: &str, machines: &[&str], port: u16) -> Deployment {
        Deployment {
            id: id.into(),
            source: RecordSource::Cairn,
            profile: id.into(),
            machines: machines.iter().map(|m| (*m).into()).collect(),
            head: machines[0].into(),
            api_port: port,
            api_path: "/v1".into(),
            health_path: "/v1/models".into(),
        }
    }

    fn sparks() -> Vec<MachineAt> {
        vec![
            MachineAt {
                name: "bginf-sp1".into(),
                address: Some("10.20.20.40".into()),
                link: Some("10.0.1.1".into()),
            },
            MachineAt {
                name: "bginf-sp2".into(),
                address: Some("10.20.20.41".into()),
                link: Some("10.0.1.2".into()),
            },
        ]
    }

    pub(crate) fn dspark() -> InferenceProfile {
        let rank = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>()
        };
        InferenceProfile {
            id: "deepseek-v4-flash-vision-exp-dspark-x2".into(),
            source: RecordSource::Cairn,
            model: ModelRef {
                name: "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp".into(),
                weights: vec![weight(
                    "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp",
                    "model-00001-of-00049.safetensors",
                )],
                config_files: vec![
                    ConfigFile {
                        repo: "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp".into(),
                        revision: "6821d6ad3681a4b137b066b76094fa82ebd0a380".into(),
                        path: "config.json".into(),
                        sha256: None,
                        git_blob: Some("8b4b87fc69023e7a224eb6563753aaf3223d8b98".into()),
                        size: Some(727),
                    },
                    ConfigFile {
                        repo: "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp".into(),
                        revision: "6821d6ad3681a4b137b066b76094fa82ebd0a380".into(),
                        path: "tokenizer.json".into(),
                        sha256: Some(SHA.into()),
                        git_blob: None,
                        size: Some(11_422_947),
                    },
                ],
            },
            served_name: "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp".into(),
            upstream_id: None,
            verbs: None,
            modalities: None,
            quantization: Some("fp8".into()),
            origin: ProfileOrigin::Authored,
            engine: Engine::Container(Container {
                image: format!("ghcr.io/anemll/dspark-vllm-gx10@sha256:{SHA}"),
                server: Server::Vllm,
                entrypoint: Some(vec![]),
                args: vec![
                    "bash".into(),
                    "-c".into(),
                    "/opt/patches/apply.sh && exec vllm serve {model} --revision {revision} --served-model-name {servedName} --port {port} --tensor-parallel-size 2 --nnodes {machines} --node-rank {rank} --master-addr {head.link} --speculative-config '{\"method\":\"dspark\",\"num_speculative_tokens\":5}'".into(),
                ],
                env: rank(&[("NCCL_CUMEM_ENABLE", "0"), ("VLLM_HOST_IP", "{self.link}")]),
                mounts: vec![
                    Mount {
                        from: MountSource::Source {
                            path: "./patches".into(),
                        },
                        to: "/opt/patches".into(),
                        read_only: true,
                    },
                    Mount {
                        from: MountSource::HfCache,
                        to: "/root/.cache/huggingface".into(),
                        read_only: false,
                    },
                ],
                host: HostOptions {
                    ipc_host: true,
                    network_host: true,
                    shm_size: Some("64gb".into()),
                    gpus: true,
                    devices: vec![],
                    ulimits: BTreeMap::from([("memlock".to_string(), "-1:-1".to_string())]),
                },
                files: Some(SourcePin {
                    url: "https://github.com/MiaAI-Lab/DeepSeek-v4-Flash-DSpark-2x-DGX-Spark".into(),
                    commit: "f3d76450f3d76450f3d76450f3d76450f3d76450".into(),
                }),
                ranks: vec![rank(&[]), rank(&[("HEADLESS", "1")])],
            }),
            requirements: Requirements {
                machines: 2,
                accelerator: Some(Accelerator {
                    family: AcceleratorFamily::Nvidia,
                    runtime: AcceleratorRuntime::Cuda,
                }),
                interconnect: None,
                memory: None,
            },
            notes: None,
        }
    }

    fn embedder() -> InferenceProfile {
        let mut profile = dspark();
        profile.id = "qwen3-embedding-4b-q8_0-llamacpp-x1".into();
        profile.model = ModelRef {
            name: "Qwen/Qwen3-Embedding-4B".into(),
            weights: vec![weight(
                "Qwen/Qwen3-Embedding-4B-GGUF",
                "Qwen3-Embedding-4B-Q8_0.gguf",
            )],
            config_files: vec![],
        };
        profile.served_name = "Qwen/Qwen3-Embedding-4B".into();
        profile.requirements.machines = 1;
        profile.requirements.accelerator = Some(Accelerator {
            family: AcceleratorFamily::Amd,
            runtime: AcceleratorRuntime::Vulkan,
        });
        profile.engine = Engine::Container(Container {
            image: format!("ghcr.io/ggml-org/llama.cpp@sha256:{SHA}"),
            server: Server::LlamaCpp,
            entrypoint: None,
            args: vec![
                "-m".into(),
                "{weights}".into(),
                "--alias".into(),
                "{servedName}".into(),
                "--host".into(),
                "0.0.0.0".into(),
                "--port".into(),
                "{port}".into(),
                "--embeddings".into(),
                "--pooling".into(),
                "last".into(),
            ],
            env: BTreeMap::new(),
            mounts: vec![Mount {
                from: MountSource::HfCache,
                to: "/models".into(),
                read_only: true,
            }],
            host: HostOptions {
                gpus: true,
                ..HostOptions::default()
            },
            files: None,
            ranks: vec![],
        });
        profile
    }

    /// Compare against a stored rendering; `CAIRN_UPDATE_SNAPSHOTS=1`
    /// rewrites it.
    fn snapshot(name: &str, actual: &str) {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/render")
            .join(name);
        if std::env::var_os("CAIRN_UPDATE_SNAPSHOTS").is_some() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, actual).unwrap();
            return;
        }
        let expected = std::fs::read_to_string(&path).unwrap_or_else(|_| {
            panic!(
                "no snapshot at {}; run with CAIRN_UPDATE_SNAPSHOTS=1",
                path.display()
            )
        });
        assert_eq!(actual, expected, "rendering of {name} changed");
    }

    #[test]
    fn the_dspark_gang_renders_peers_first_and_the_head_last() {
        let profile = dspark();
        let rendered = render(
            &profile,
            &deployment("dsv4f", &["bginf-sp1", "bginf-sp2"], 8888),
            &sparks(),
        )
        .unwrap();
        snapshot("dspark-start.sh", &rendered.start);
        snapshot("dspark-stop.sh", rendered.stop.as_ref().unwrap());
        assert_eq!(rendered.cleanup, rendered.stop);

        let start = &rendered.start;
        let peer = start.find("# rank 1 on bginf-sp2").unwrap();
        let head = start.find("# rank 0 on bginf-sp1").unwrap();
        assert!(peer < head, "the peer starts before the head");
        assert!(start.contains("ssh -o BatchMode=yes '10.20.20.41' 'bash -ls' <<'CAIRN_RANK_1'"));
        assert!(
            start.contains("'VLLM_HOST_IP=10.0.1.2'"),
            "the peer's own link"
        );
        assert!(
            start.contains("'VLLM_HOST_IP=10.0.1.1'"),
            "the head's own link"
        );
        assert!(start.contains("'HEADLESS=1'"));
        assert!(start.contains("--node-rank 1 --master-addr 10.0.1.1"));
        assert!(start.contains("--restart no"));
        assert!(!start.contains("unless-stopped"));
        assert!(start.contains(r#"'{"'\''"method"'\''"#) || start.contains("method"));
        let stop = rendered.stop.unwrap();
        assert!(stop.contains("'cairn-dsv4f-r0'") && stop.contains("cairn-dsv4f-r1"));
    }

    #[test]
    fn a_hostile_weight_path_produces_no_script() {
        let mut profile = embedder();
        profile.model.weights[0].path = "../../../../../../tmp/cairn-hostile.gguf".into();
        let result = render(
            &profile,
            &deployment("embed", &["fedora"], 8090),
            &[MachineAt {
                name: "fedora".into(),
                address: None,
                link: None,
            }],
        );
        assert!(
            result.is_err(),
            "unsafe host download script was rendered: {result:?}"
        );
    }

    #[test]
    fn hostile_weight_locations_are_refused_by_inventory_and_render() {
        use crate::inventory::{Inventory, ValidationError, YamlError};

        for (field, value) in [
            ("repo", "../model"),
            ("repo", "owner/../model"),
            ("repo", "/owner/model"),
            ("repo", "owner/model?download=1"),
            ("repo", "owner\\model"),
            ("repo", "owner/model\n"),
            ("revision", ".."),
            ("revision", "../main"),
            ("revision", "/main"),
            ("revision", "refs/pr/1"),
            ("revision", "main?download=1"),
            ("revision", "main\\other"),
            ("revision", "main\r"),
            ("path", "../../../../../../tmp/cairn-hostile.gguf"),
            ("path", "Q4_K_M/../model.gguf"),
            ("path", ".."),
            ("path", "model%2f..%2fother.gguf"),
            ("path", "model.gguf?download=1"),
            ("path", "model.gguf#fragment"),
            ("path", "C:/tmp/model.gguf"),
            ("path", "/tmp/cairn-hostile.gguf"),
            ("path", "C:\\tmp\\model.gguf"),
            ("path", "model\u{0000}.gguf"),
            ("path", "model\t.gguf"),
            ("path", "model\u{007f}.gguf"),
        ] {
            let mut profile = embedder();
            let weight = &mut profile.model.weights[0];
            match field {
                "repo" => weight.repo = value.into(),
                "revision" => weight.revision = value.into(),
                "path" => weight.path = value.into(),
                _ => unreachable!(),
            }
            let mut inventory = Inventory::default();
            inventory.insert_model(crate::model::Model::named(&profile.model.name));
            inventory
                .profiles
                .insert(profile.id.clone(), profile.clone());
            let errors = inventory
                .validate()
                .expect_err("unsafe inventory must be refused");
            assert!(
                errors.iter().any(|error| matches!(
                    error, ValidationError::BadWeightFile { index: 0, problem, .. }
                        if problem.contains(field)
                )),
                "{field}={value:?}: {errors:?}"
            );
            assert!(matches!(
                Inventory::from_yaml(&inventory.to_yaml()),
                Err(YamlError::Invalid(errors)) if errors.iter().any(|error|
                    matches!(error, ValidationError::BadWeightFile { .. }))
            ));
            let rendered = render(
                &profile,
                &deployment("embed", &["fedora"], 8090),
                &[MachineAt {
                    name: "fedora".into(),
                    address: None,
                    link: None,
                }],
            );
            assert!(
                rendered.is_err(),
                "no script may be produced for {field}={value:?}"
            );
            assert!(rendered.unwrap_err().0.contains(field));
        }
    }

    #[test]
    fn real_weight_names_and_nested_gguf_shards_validate_and_render() {
        for (repo, revision, path) in [
            (
                "Qwen/Qwen3-Embedding-4B",
                "5cf2132abc99cad020ac570b19d031efec650f2b",
                "model-00001-of-00002.safetensors",
            ),
            (
                "Qwen/Qwen3-Embedding-4B-GGUF",
                "f4602530db1d980e16da9d7d3a70294cf5c190be",
                "Qwen3-Embedding-4B-Q8_0.gguf",
            ),
            (
                "unsloth/DeepSeek-R1-GGUF",
                "main",
                "DeepSeek-R1-Q4_K_M/DeepSeek-R1-Q4_K_M-00001-of-00009.gguf",
            ),
            ("owner_1/Model.v2", "v1.2-rc_1", "nested/model..v2.gguf"),
        ] {
            let mut profile = embedder();
            let weight = &mut profile.model.weights[0];
            weight.repo = repo.into();
            weight.revision = revision.into();
            weight.path = path.into();
            let mut inventory = crate::inventory::Inventory::default();
            inventory.insert_model(crate::model::Model::named(&profile.model.name));
            inventory.insert_profile(profile.clone());
            assert_eq!(inventory.validate(), Ok(()));
            assert!(crate::inventory::Inventory::from_yaml(&inventory.to_yaml()).is_ok());
            let rendered = render(
                &profile,
                &deployment("embed", &["fedora"], 8090),
                &[MachineAt {
                    name: "fedora".into(),
                    address: None,
                    link: None,
                }],
            )
            .unwrap();
            assert!(rendered.start.contains(&format!(
                "https://huggingface.co/{repo}/resolve/{revision}/{path}"
            )));
            assert!(rendered
                .start
                .contains(&cached_weight_path("/models", &profile.model.weights[0])));
        }
    }

    #[test]
    fn a_single_machine_llama_cpp_fetches_and_verifies_its_gguf() {
        let profile = embedder();
        let rendered = render(
            &profile,
            &deployment("embed", &["fedora"], 8090),
            &[MachineAt {
                name: "fedora".into(),
                address: Some("10.20.20.43".into()),
                link: None,
            }],
        )
        .unwrap();
        snapshot("llamacpp-start.sh", &rendered.start);
        let start = &rendered.start;
        assert!(!start.contains("ssh "));
        assert!(start.contains("sha256sum -c"));
        assert!(start.contains("'/models/hub/models--Qwen--Qwen3-Embedding-4B-GGUF/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/Qwen3-Embedding-4B-Q8_0.gguf'"));
        assert!(start.contains("--device /dev/dri"));
        assert!(
            start.contains("-p 8090:8090"),
            "no host network: the port is published"
        );
    }

    /// Every `-v` a script's container runs pass, as `(host side, container
    /// side)`.
    fn volumes(script: &str) -> Vec<(String, String)> {
        script
            .lines()
            .filter(|line| line.starts_with("\"$rt\" run "))
            .flat_map(|line| {
                let words: Vec<&str> = line.split(' ').collect();
                words
                    .windows(2)
                    .filter(|pair| pair[0] == "-v")
                    .map(|pair| {
                        let (host, container) = pair[1].split_once(":'").unwrap();
                        (
                            host.to_string(),
                            container.trim_end_matches('\'').to_string(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn fedora() -> Vec<MachineAt> {
        vec![MachineAt {
            name: "fedora".into(),
            address: None,
            link: None,
        }]
    }

    #[test]
    fn a_container_reaches_only_its_pinned_repositories_read_only() {
        let gang = render(
            &dspark(),
            &deployment("d", &["bginf-sp1", "bginf-sp2"], 8888),
            &sparks(),
        )
        .unwrap()
        .start;
        let embed = render(
            &embedder(),
            &deployment("embed", &["fedora"], 8090),
            &fedora(),
        )
        .unwrap()
        .start;
        for (script, repo, to) in [
            (
                &gang,
                "deepseek-ai--DeepSeek-V4-Flash-Vision-Exp",
                "/root/.cache/huggingface",
            ),
            (&embed, "Qwen--Qwen3-Embedding-4B-GGUF", "/models"),
        ] {
            let cache: Vec<(String, String)> = volumes(script)
                .into_iter()
                .filter(|(host, _)| host.starts_with("\"$hf\""))
                .collect();
            assert!(!cache.is_empty(), "{script}");
            for (host, container) in &cache {
                assert_eq!(host, &format!("\"$hf\"/'hub/models--{repo}'"), "{script}");
                assert_eq!(container, &format!("{to}/hub/models--{repo}:ro"));
            }
            assert!(
                !script.contains("-v \"$hf\":"),
                "the cache itself, and its token, is never mounted"
            );
            assert!(
                script.contains("'HF_HUB_OFFLINE=1' "),
                "the server fetches nothing into a read-only mount"
            );
        }
        assert!(
            gang.contains("hf_get 'https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash-Vision-Exp/resolve/6821d6ad3681a4b137b066b76094fa82ebd0a380/config.json'"),
            "vLLM's config files are fetched before it starts"
        );
        assert!(gang.contains("= '8b4b87fc69023e7a224eb6563753aaf3223d8b98' ]"));
        assert!(
            gang.contains("hf_get 'https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash-Vision-Exp/resolve/6821d6ad3681a4b137b066b76094fa82ebd0a380/model-00001-of-00049.safetensors'"),
            "and so are its weights"
        );
    }

    #[test]
    fn a_writable_cache_mount_reads_and_renders_read_only() {
        let mount: Mount = serde_yaml::from_str(
            "from: {kind: hfCache}\nto: /root/.cache/huggingface\nreadOnly: false\n",
        )
        .unwrap();
        assert!(mount.read_only);
        let source: Mount =
            serde_yaml::from_str("from: {kind: source, path: ./patches}\nto: /opt/patches\n")
                .unwrap();
        assert!(!source.read_only, "a source mount keeps what it says");

        let mut profile = embedder();
        if let Engine::Container(container) = &mut profile.engine {
            container.mounts[0].read_only = false;
        }
        let start = render(&profile, &deployment("embed", &["fedora"], 8090), &fedora())
            .unwrap()
            .start;
        assert!(volumes(&start).iter().all(|(_, to)| to.ends_with(":ro")));
    }

    #[test]
    fn a_projector_from_another_repository_is_mounted_beside_the_weights() {
        let mut profile = embedder();
        profile.model.weights.push(weight(
            "ggml-org/Qwen3-Embedding-4B-mmproj",
            "mmproj-f16.gguf",
        ));
        let start = render(&profile, &deployment("embed", &["fedora"], 8090), &fedora())
            .unwrap()
            .start;
        let containers: Vec<String> = volumes(&start).into_iter().map(|(_, to)| to).collect();
        assert_eq!(
            containers,
            vec![
                "/models/hub/models--Qwen--Qwen3-Embedding-4B-GGUF:ro",
                "/models/hub/models--ggml-org--Qwen3-Embedding-4B-mmproj:ro",
            ]
        );
        assert!(
            start.contains("mkdir -p \"$hf\"/'hub/models--ggml-org--Qwen3-Embedding-4B-mmproj'")
        );
    }

    #[test]
    fn vllm_without_its_config_files_is_refused() {
        let mut profile = dspark();
        profile.model.config_files.clear();
        let refusal = render(
            &profile,
            &deployment("d", &["bginf-sp1", "bginf-sp2"], 8888),
            &sparks(),
        )
        .unwrap_err();
        assert!(refusal.0.contains("model.configFiles"), "{}", refusal.0);

        profile = dspark();
        profile.model.config_files[0].git_blob = Some("not-a-sha".into());
        let refusal = render(
            &profile,
            &deployment("d", &["bginf-sp1", "bginf-sp2"], 8888),
            &sparks(),
        )
        .unwrap_err();
        assert!(
            refusal.0.contains("config file 0: gitBlob"),
            "{}",
            refusal.0
        );
    }

    #[test]
    fn docker_and_podman_get_their_own_gpu_flags() {
        let rendered = render(
            &dspark(),
            &deployment("d", &["bginf-sp1", "bginf-sp2"], 8888),
            &sparks(),
        )
        .unwrap();
        assert!(rendered.start.contains(
            "if [ \"$rt\" = docker ]; then gpu=(--gpus all); else gpu=(--device nvidia.com/gpu=all); fi"
        ));
        assert!(rendered.start.contains("--security-opt label=disable"));
    }

    #[test]
    fn an_env_reading_a_secret_nothing_fills_is_refused_not_passed_as_a_credential() {
        let fedora = [MachineAt {
            name: "fedora".into(),
            address: None,
            link: None,
        }];
        let mut profile = embedder();
        if let Engine::Container(container) = &mut profile.engine {
            container
                .env
                .insert("HF_TOKEN".into(), "${HF_TOKEN}".into());
        }
        let refusal = render(&profile, &deployment("embed", &["fedora"], 8090), &fedora)
            .unwrap_err()
            .0;
        assert!(
            refusal.contains("env HF_TOKEN reads ${HF_TOKEN}"),
            "{refusal}"
        );

        let mut gang = dspark();
        if let Engine::Container(container) = &mut gang.engine {
            container.ranks[1].insert("PEER_TOKEN".into(), "$PEER_TOKEN".into());
        }
        let refusal = render(
            &gang,
            &deployment("d", &["bginf-sp1", "bginf-sp2"], 8888),
            &sparks(),
        )
        .unwrap_err()
        .0;
        assert!(
            refusal.contains("rank 1 env PEER_TOKEN reads ${PEER_TOKEN}"),
            "{refusal}"
        );

        // Escaped and non-secret dollar text still renders, as written.
        let mut literal = embedder();
        if let Engine::Container(container) = &mut literal.engine {
            container
                .env
                .insert("BANNER".into(), "$${HF_TOKEN} costs $5".into());
            container.env.insert("HF_HOME".into(), "${HF_HOME}".into());
        }
        let start = render(&literal, &deployment("embed", &["fedora"], 8090), &fedora)
            .unwrap()
            .start;
        assert!(start.contains("'BANNER=$${HF_TOKEN} costs $5'"), "{start}");
    }

    #[test]
    fn hostile_values_stay_quoted_and_newlines_are_refused() {
        assert_eq!(quote("a'b"), r#"'a'\''b'"#);
        assert_eq!(quote("$(rm -rf /)"), "'$(rm -rf /)'");
        let mut profile = embedder();
        profile.served_name = "x'; rm -rf / #".into();
        let rendered = render(
            &profile,
            &deployment("embed", &["fedora"], 8090),
            &[MachineAt {
                name: "fedora".into(),
                address: None,
                link: None,
            }],
        )
        .unwrap();
        assert!(rendered.start.contains(r#"'x'\''; rm -rf / #'"#));

        let mut gang = dspark();
        gang.served_name = "x\nCAIRN_RANK_1\ny".into();
        let start = render(
            &gang,
            &deployment("d", &["bginf-sp1", "bginf-sp2"], 8888),
            &sparks(),
        )
        .unwrap()
        .start;
        assert!(
            start.contains("<<'CAIRN_RANK_1_'\n"),
            "the delimiter grew past the value's line"
        );
        assert!(start.contains("\nCAIRN_RANK_1_\n"));

        let mut pathy = embedder();
        if let Engine::Container(container) = &mut pathy.engine {
            container.mounts[0].to = "/models\nrm -rf /".into();
        }
        let refusal = render(
            &pathy,
            &deployment("embed", &["fedora"], 8090),
            &[MachineAt {
                name: "fedora".into(),
                address: None,
                link: None,
            }],
        )
        .unwrap_err();
        assert!(refusal.0.contains("newline"), "{}", refusal.0);
    }

    #[test]
    fn an_unfillable_placeholder_or_an_unreachable_peer_is_refused() {
        let mut machines = sparks();
        machines[1].link = None;
        let refusal = render(
            &dspark(),
            &deployment("d", &["bginf-sp1", "bginf-sp2"], 8888),
            &machines,
        )
        .unwrap_err();
        assert_eq!(
            refusal.0,
            "profile deepseek-v4-flash-vision-exp-dspark-x2 uses {self.link}, which cannot be filled in: machine bginf-sp2 has no link address: it reports no fast link joining it to a pool"
        );
        machines[1].address = None;
        let refusal = render(
            &dspark(),
            &deployment("d", &["bginf-sp1", "bginf-sp2"], 8888),
            &machines,
        )
        .unwrap_err();
        assert!(
            refusal.0.contains("cannot reach it over ssh"),
            "{}",
            refusal.0
        );
    }

    #[test]
    fn a_command_profile_passes_through_and_native_is_refused() {
        let profile = crate::fixture::command_profile("r", 1);
        let rendered = render(
            &profile,
            &deployment("r", &["sp1"], 8000),
            &[MachineAt {
                name: "sp1".into(),
                address: None,
                link: None,
            }],
        )
        .unwrap();
        assert_eq!(rendered.start, "./start.sh");
        assert_eq!(rendered.stop.as_deref(), Some("./stop.sh"));
        let mut native = profile;
        native.engine = Engine::Native {
            artifact: "a".into(),
            version: "1".into(),
            sha256: "0".repeat(64),
            args: vec![],
        };
        assert!(render(&native, &deployment("r", &["sp1"], 8000), &[])
            .unwrap_err()
            .0
            .contains("native engine"));
    }
}
