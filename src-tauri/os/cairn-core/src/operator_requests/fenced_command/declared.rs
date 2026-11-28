//! Project-declared approved commands (`approvedCommands` in
//! `.cairn/config.yaml`), as `fenced_command` resolves, freezes, rechecks and
//! lands them.
//!
//! A declaration is executable text from the repository, so it is read only at
//! the default branch's tip: changing it means landing a change on that
//! branch, under the project's own landing path. The act binds the commit it
//! was read from and the digest of the declaration, and every recheck reads
//! the tip again. A declaration that changed since the card was shown is a
//! different act and is refused.
//!
//! A `lands` act runs at the tip standing when it is claimed (a card may wait
//! for hours while the branch moves), so what it binds is the declaration and
//! each param's resolution (for a release version, `sm` from `0.13.8` to
//! `0.13.9`), never one commit's bytes. Its sealed commit carries
//! `Cairn-Request-Act: <sha256 of the encoded act>`, and that trailer on the
//! default branch is how every later path adopts the commit instead of running
//! the command a second time.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cairn_common::executor_protocol::ExecutorInspection;
use cairn_common::protocol::CallbackRequest;
use cairn_common::requests::{
    DeclaredAct, DeclaredCommandTarget, DeclaredLands, DeclaredParam, DeclaredVersionRead,
    FencedCommandAct, RequestKind,
};
use cairn_common::run_contract::RUN_BATCH_CEILING_MS;
use cairn_db::turso::params;

use super::ExecutorFloor;
use crate::config::project_settings::{
    parse_project_settings, ApprovedCommandParam, ApprovedCommandSpec, ApprovedCommandThen,
};
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

const PROJECT_CONFIG_PATH: &str = ".cairn/config.yaml";

/// The trailer naming the act a declared command's sealed commit performs.
pub(crate) const REQUEST_ACT_TRAILER: &str = "Cairn-Request-Act";

pub(crate) const DECLARATION_CHANGED: &str = "the declaration changed since this card was shown; nothing ran. Refresh the card and accept again to run what main declares now";

/// Refuses a declared command that lands a commit on a project whose
/// `human_base_merge_policy` is anything but `disabled`: a desktop acceptance
/// cannot supply the provably-human base-merge approval that policy demands.
pub(crate) async fn landing_policy(
    orch: &Orchestrator,
    project_id: &str,
    command_name: &str,
) -> Result<(), String> {
    if project_id.trim().is_empty() {
        return Err(format!("Declared command '{command_name}' is unavailable: the canonical target project's human_base_merge_policy cannot be identified"));
    }
    let db = crate::execution::routing::routing_db_for_id(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let policy =
        crate::projects::human_base_merge_policy::read_policy(&db, None, project_id).await?;
    if policy.state != "disabled" {
        return Err(format!("Declared command '{command_name}' refuses human_base_merge_policy={}: desktop acceptance cannot supply provably-human base-merge approval. Use the project's approved human landing path, or wait for a stronger approval adapter; this request cannot bypass that policy.", policy.state));
    }
    Ok(())
}

/// Plain release versions only: a parameter cannot smuggle prerelease or build
/// metadata into the command it is substituted into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ReleaseVersion(pub u64, pub u64, pub u64);

impl ReleaseVersion {
    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        let invalid = || {
            format!("Invalid release version '{value}': use sm, md, lg or plain X.Y.Z (no prerelease/build metadata)")
        };
        let parts: Vec<_> = value.split('.').collect();
        if parts.len() != 3 {
            return Err(invalid());
        }
        let mut numbers = [0; 3];
        for (index, part) in parts.iter().enumerate() {
            if part.is_empty()
                || !part.bytes().all(|byte| byte.is_ascii_digit())
                || (part.len() > 1 && part.starts_with('0'))
            {
                return Err(invalid());
            }
            numbers[index] = part.parse().map_err(|_| invalid())?;
        }
        Ok(Self(numbers[0], numbers[1], numbers[2]))
    }

    /// Resolves `sm`, `md` or `lg` against `self` as the current version, or
    /// accepts a plain `X.Y.Z` strictly greater than it.
    pub(crate) fn resolve(self, requested: &str) -> Result<Self, String> {
        let overflow = || "release version component overflows".to_string();
        match requested {
            "sm" => Ok(Self(
                self.0,
                self.1,
                self.2.checked_add(1).ok_or_else(overflow)?,
            )),
            "md" => Ok(Self(self.0, self.1.checked_add(1).ok_or_else(overflow)?, 0)),
            "lg" => Ok(Self(self.0.checked_add(1).ok_or_else(overflow)?, 0, 0)),
            explicit => {
                let target = Self::parse(explicit)?;
                if target <= self {
                    return Err(format!(
                        "release version {target} must be greater than current version {self}"
                    ));
                }
                Ok(target)
            }
        }
    }
}

impl std::fmt::Display for ReleaseVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}.{}", self.0, self.1, self.2)
    }
}

fn short(commit: &str) -> &str {
    &commit[..commit.len().min(12)]
}

/// A project's default branch as its store holds it.
#[derive(Debug, Clone)]
pub(crate) struct DefaultTip {
    pub(crate) store: PathBuf,
    pub(crate) branch: String,
    pub(crate) tip: String,
}

pub(crate) async fn default_tip(
    orch: &Orchestrator,
    project_id: &str,
) -> Result<DefaultTip, String> {
    let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| format!("the project's database is unavailable: {error}"))?;
    let (repo_path, stored_default) = db
        .query_opt(
            "SELECT repo_path, default_branch FROM projects WHERE id = ?1",
            params![project_id],
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .map_err(|error| format!("the project is unreadable: {error}"))?
        .ok_or_else(|| format!("project {project_id} is gone"))?;
    let branch = crate::config::project_settings::effective_default_branch(
        Some(Path::new(&repo_path)),
        stored_default.as_deref(),
    );
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let tip = crate::jj::bookmark_commit_checked(&jj, &store, &branch)?
        .ok_or_else(|| format!("the default branch `{branch}` is not in the project's store"))?;
    Ok(DefaultTip { store, branch, tip })
}

/// The declaration `name` at `commit`.
fn declaration_at(
    jj: &crate::jj::JjEnv,
    store: &Path,
    commit: &str,
    name: &str,
) -> Result<ApprovedCommandSpec, String> {
    let absent = || {
        format!(
            "main@{} declares no approved command `{name}` (approvedCommands in {PROJECT_CONFIG_PATH})",
            short(commit)
        )
    };
    let text =
        crate::jj::text_at_revision(jj, store, commit, PROJECT_CONFIG_PATH)?.ok_or_else(absent)?;
    let (settings, _) = parse_project_settings(&text)
        .map_err(|error| format!("main@{}'s project config: {error}", short(commit)))?;
    settings
        .approved_commands
        .and_then(|mut commands| commands.remove(name))
        .ok_or_else(absent)
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    format!("{:x}", sha2::Sha256::digest(bytes))
}

/// SHA-256 of the declaration's canonical JSON encoding.
pub(crate) fn declaration_digest(spec: &ApprovedCommandSpec) -> Result<String, String> {
    let encoded = serde_json::to_string(spec)
        .map_err(|error| format!("the declaration is unencodable: {error}"))?;
    Ok(sha256_hex(encoded.as_bytes()))
}

/// The string at top-level `key` of the JSON file `path` at `commit`.
fn json_key_at(
    jj: &crate::jj::JjEnv,
    store: &Path,
    commit: &str,
    path: &str,
    key: &str,
) -> Result<String, String> {
    let text = crate::jj::text_at_revision(jj, store, commit, path)?
        .ok_or_else(|| format!("{path} does not exist at {}", short(commit)))?;
    let value: serde_json::Value = serde_json::from_str(&text)
        .map_err(|error| format!("{path} at {} is not JSON: {error}", short(commit)))?;
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("{path} at {} has no string `{key}`", short(commit)))
}

/// A declaration resolved against what one filing asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Resolved {
    pub(crate) command: String,
    pub(crate) timeout_ms: u64,
    pub(crate) declaration: DeclaredAct,
}

fn expand(text: &str, values: &BTreeMap<&str, String>) -> String {
    // Every placeholder is closed and declared (config validation), and every
    // value is a validated literal with no braces, so one pass per name is
    // exact.
    values.iter().fold(text.to_string(), |text, (name, value)| {
        text.replace(&format!("{{{name}}}"), value)
    })
}

/// Resolve declaration `name` against the params a filing supplied. Pure:
/// `read_current(path, jsonKey)` reads a release version's current value at
/// `defined_by_commit`. Only validated literals reach the command (a release
/// version's digits and dots, or a declared choice), so no shell syntax does.
pub(crate) fn resolve(
    name: &str,
    spec: &ApprovedCommandSpec,
    requested: &BTreeMap<String, String>,
    defined_by_commit: &str,
    read_current: &dyn Fn(&str, &str) -> Result<String, String>,
) -> Result<Resolved, String> {
    if let Some(extra) = requested.keys().find(|key| !spec.params.contains_key(*key)) {
        return Err(format!(
            "`{name}` declares no param `{extra}`; it takes [{}]",
            spec.params.keys().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    let mut values = BTreeMap::new();
    let mut params = Vec::new();
    for (param, declared) in &spec.params {
        let asked = requested
            .get(param)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("`{name}` requires param `{param}`"))?;
        let (resolved, current) = match declared {
            ApprovedCommandParam::ReleaseVersion { current } => {
                let value = read_current(&current.path, &current.json_key)?;
                let resolved = ReleaseVersion::parse(&value)
                    .map_err(|error| format!("the current version in {}: {error}", current.path))?
                    .resolve(asked)
                    .map_err(|error| format!("param `{param}`: {error}"))?;
                (
                    resolved.to_string(),
                    Some(DeclaredVersionRead {
                        path: current.path.clone(),
                        json_key: current.json_key.clone(),
                        value,
                    }),
                )
            }
            ApprovedCommandParam::Choice { values } => {
                if !values.iter().any(|value| value == asked) {
                    return Err(format!(
                        "param `{param}` takes one of [{}], not `{asked}`",
                        values.join(", ")
                    ));
                }
                (asked.to_string(), None)
            }
        };
        values.insert(param.as_str(), resolved.clone());
        params.push(DeclaredParam {
            name: param.clone(),
            requested: asked.to_string(),
            resolved,
            current,
        });
    }
    let timeout_ms = spec
        .timeout
        .map_or(u64::from(RUN_BATCH_CEILING_MS), |seconds| {
            seconds.saturating_mul(1_000)
        });
    Ok(Resolved {
        command: expand(&spec.command, &values),
        timeout_ms,
        declaration: DeclaredAct {
            name: name.to_string(),
            label: spec.label.clone(),
            consequence: expand(&spec.consequence, &values),
            params,
            defined_by_commit: defined_by_commit.to_string(),
            declaration_digest: declaration_digest(spec)?,
            lands: spec.lands.as_ref().map(|lands| DeclaredLands {
                paths: lands.paths.clone(),
                message: expand(&lands.message, &values),
            }),
            then: spec.then.as_ref().map(|then| match then {
                ApprovedCommandThen::Rebuild => RequestKind::Rebuild,
            }),
        },
    })
}

/// Every release version still reads, through `read_now`, the value the act
/// resolved from. The installed app is built from the default branch, so this
/// is at least as strong as comparing with the running version.
pub(crate) fn current_unchanged(
    declared: &DeclaredAct,
    read_now: &dyn Fn(&str, &str) -> Result<String, String>,
) -> Result<(), String> {
    for param in &declared.params {
        if let Some(current) = &param.current {
            let now = read_now(&current.path, &current.json_key)?;
            if now != current.value {
                return Err(format!(
                    "target {} is stale against main's {now}; no implicit extra bump is authorized",
                    param.resolved
                ));
            }
        }
    }
    Ok(())
}

/// The one executor a declared act runs on: the named one, or else the only
/// online executor whose credential floor is measured refused and which
/// advertises every required toolchain.
fn choose_executor(
    orch: &Orchestrator,
    name: &str,
    named: Option<&str>,
    toolchains: &[String],
) -> Result<ExecutorFloor, String> {
    let inspections = orch.fleet.inspect_executors(crate::fleet::unix_time_ms());
    let fits = |inspection: &ExecutorInspection| -> Result<ExecutorFloor, String> {
        let floor = super::floor_of(inspection)?;
        let advertised = &inspection.health.advertisement.capabilities.toolchains;
        let missing: Vec<&str> = toolchains
            .iter()
            .filter(|toolchain| !advertised.contains(toolchain))
            .map(String::as_str)
            .collect();
        if !missing.is_empty() {
            return Err(format!(
                "{} cannot run `{name}`: it does not advertise {}",
                floor.name,
                missing.join(", ")
            ));
        }
        Ok(floor)
    };
    if let Some(reference) = named {
        let executor_id = crate::fleet::management::resolve_executor_reference(orch, reference)?;
        let inspection = inspections
            .iter()
            .find(|inspection| inspection.health.identity.executor_id == executor_id)
            .ok_or_else(|| {
                format!("fenced_command is unavailable: executor {reference} is not connected, so its credential floor has no current measurement")
            })?;
        return fits(inspection);
    }
    let candidates: Vec<ExecutorFloor> = inspections
        .iter()
        .filter_map(|inspection| fits(inspection).ok())
        .filter(|floor| floor.online)
        .collect();
    match candidates.as_slice() {
        [one] => Ok(one.clone()),
        [] => Err(format!(
            "no online executor can run `{name}`: it needs a credential floor measured refused{}. Name one with `executor`",
            if toolchains.is_empty() { String::new() } else { format!(" and the toolchains {}", toolchains.join(", ")) }
        )),
        several => Err(format!(
            "several executors can run `{name}` ({}); name one with `executor`",
            several
                .iter()
                .map(|floor| floor.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

/// Freeze a declared filing: the declaration and the versions it reads are
/// taken at the default branch's tip, never the filer's branch, and a `lands`
/// act is refused here first when the project's base-merge policy forbids it.
pub(crate) async fn freeze(
    orch: &Orchestrator,
    request: &CallbackRequest,
    target: DeclaredCommandTarget,
) -> Result<FencedCommandAct, String> {
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request).await?;
    let project_id = crate::mcp::handlers::branch::resolve_current_for_read(orch, request)
        .await?
        .project_id;
    let tip = default_tip(orch, &project_id).await?;
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let spec = declaration_at(&jj, &tip.store, &tip.tip, &target.declared)?;
    let resolved = resolve(
        &target.declared,
        &spec,
        &target.params,
        &tip.tip,
        &|path, key| json_key_at(&jj, &tip.store, &tip.tip, path, key),
    )?;
    let toolchains = spec
        .executor
        .as_ref()
        .map(|executor| executor.required_toolchains.clone())
        .unwrap_or_default();
    let floor = choose_executor(
        orch,
        &target.declared,
        target.executor.as_deref(),
        &toolchains,
    )?;
    if resolved.declaration.lands.is_some() {
        landing_policy(orch, &project_id, &target.declared).await?;
    }
    Ok(FencedCommandAct {
        executor_id: floor.executor_id,
        device_id: floor.device_id,
        project_id,
        branch: tip.branch,
        commit: tip.tip,
        cwd: String::new(),
        command: resolved.command,
        timeout_ms: resolved.timeout_ms,
        node: agent.home,
        declaration: Some(resolved.declaration),
    })
}

/// What the declared act's card says it does, beside the command.
pub(crate) fn card_consequence(
    act: &FencedCommandAct,
    declared: &DeclaredAct,
    executor: &str,
) -> String {
    let at = if declared.lands.is_some() {
        format!(
            "at {}'s tip when it is claimed (declared at {}@{})",
            act.branch,
            act.branch,
            short(&declared.defined_by_commit)
        )
    } else {
        format!("at {}@{}", act.branch, short(&act.commit))
    };
    format!(
        "{} Runs {}'s own `{}` once on {executor} {at}, with your account's access except Cairn's credentials. Network is not restricted.",
        declared.consequence, act.branch, declared.name
    )
}

/// SHA-256 of the act's canonical encoding: what its sealed commit's
/// `Cairn-Request-Act` trailer names.
pub(crate) fn act_sha(act: &FencedCommandAct) -> Result<String, String> {
    Ok(sha256_hex(super::encode_act(act)?.as_bytes()))
}

fn act_trailer(act_sha: &str) -> String {
    format!("{REQUEST_ACT_TRAILER}: {act_sha}")
}

/// The sealed commit's message: the declared subject, then the act's and the
/// attempt's trailers.
pub(crate) fn sealed_message(subject: &str, act_sha: &str, attempt_id: &str) -> String {
    format!(
        "{subject}\n\n{}\n{}\n",
        act_trailer(act_sha),
        crate::jj::request_attempt_trailer(attempt_id)
    )
}

fn store_git(store: &Path, args: &[&str]) -> Result<std::process::Output, String> {
    let git_dir = crate::jj::store_git_dir(store)
        .ok_or_else(|| format!("resolve the backing repository of {}", store.display()))?;
    crate::env::git()
        .arg(format!("--git-dir={}", git_dir.display()))
        .args(args)
        .output()
        .map_err(|error| format!("git {}: {error}", args.join(" ")))
}

fn store_git_text(store: &Path, args: &[&str]) -> Result<String, String> {
    let output = store_git(store, args)?;
    if !output.status.success() {
        return Err(format!(
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Why `commit` is not the commit this act lands, or `None` when it is: one
/// parent, authored by Cairn's managed identity, carrying the act's trailer
/// as a line of its own, and changing only paths inside `lands.paths`.
fn not_the_landing(
    store: &Path,
    commit: &str,
    act_sha: &str,
    lands: &DeclaredLands,
) -> Result<Option<String>, String> {
    let trailer = act_trailer(act_sha);
    let message = store_git_text(store, &["show", "-s", "--format=%B", commit])?;
    if !message.lines().any(|line| line.trim() == trailer) {
        return Ok(Some(format!("{commit} does not carry `{trailer}`")));
    }
    let parents = store_git_text(store, &["rev-list", "--parents", "-n", "1", commit])?;
    if parents.split_whitespace().count() != 2 {
        return Ok(Some(format!("{commit} is not a single-parent commit")));
    }
    let author = store_git_text(store, &["show", "-s", "--format=%an", commit])?;
    if author.trim() != cairn_vcs::MANAGED_IDENTITY_NAME {
        return Ok(Some(format!(
            "{commit} was authored by {}, not Cairn",
            author.trim()
        )));
    }
    let changed = store_git_text(
        store,
        &[
            "diff-tree",
            "--no-commit-id",
            "-r",
            "--name-only",
            "-z",
            commit,
        ],
    )?;
    let changed: Vec<&str> = changed
        .split('\0')
        .filter(|path| !path.is_empty())
        .collect();
    if changed.is_empty() {
        return Ok(Some(format!("{commit} changes nothing")));
    }
    if let Some(outside) = changed
        .iter()
        .find(|path| !crate::orchestrator::landing::path_matches(&lands.paths, path))
    {
        return Ok(Some(format!(
            "{commit} changes {outside}, outside the declared lands.paths"
        )));
    }
    Ok(None)
}

/// The commit on the default branch's first-parent line after
/// `defined_by_commit` that is this act's landing, if one is.
pub(crate) fn adopted_commit(
    store: &Path,
    defined_by_commit: &str,
    tip: &str,
    act_sha: &str,
    lands: &DeclaredLands,
) -> Result<Option<String>, String> {
    let candidates = store_git_text(
        store,
        &[
            "log",
            "--first-parent",
            "--fixed-strings",
            &format!("--grep={}", act_trailer(act_sha)),
            "--format=%H",
            &format!("{defined_by_commit}..{tip}"),
        ],
    )?;
    for commit in candidates
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        if not_the_landing(store, commit, act_sha, lands)?.is_none() {
            return Ok(Some(commit.to_string()));
        }
    }
    Ok(None)
}

/// The sealed tree reads, for every release version, the value the card
/// resolved: the command did what the operator accepted.
pub(crate) fn sealed_reads_resolved(
    orch: &Orchestrator,
    store: &Path,
    sealed: &str,
    declared: &DeclaredAct,
) -> Result<(), String> {
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    for param in &declared.params {
        if let Some(current) = &param.current {
            let now = json_key_at(&jj, store, sealed, &current.path, &current.json_key)?;
            if now != param.resolved {
                return Err(format!(
                    "the sealed commit sets {} `{}` to {now}, not the {} the card shows; nothing lands",
                    current.path, current.json_key, param.resolved
                ));
            }
        }
    }
    Ok(())
}

/// A reported landed commit is on the default branch and is this act's
/// landing.
pub(crate) async fn verify_landed(
    orch: &Orchestrator,
    act: &FencedCommandAct,
    commit: &str,
) -> Result<(), String> {
    let lands = act
        .declaration
        .as_ref()
        .and_then(|declared| declared.lands.as_ref())
        .ok_or("only a declared `lands` act reports a landed commit")?;
    let tip = default_tip(orch, &act.project_id).await?;
    let on_tip = store_git(
        &tip.store,
        &["merge-base", "--is-ancestor", commit, &tip.tip],
    )?;
    if !on_tip.status.success() {
        return Err(format!("{commit} is not on {}", tip.branch));
    }
    match not_the_landing(&tip.store, commit, &act_sha(act)?, lands)? {
        None => Ok(()),
        Some(why) => Err(why),
    }
}

/// Where a declared act stands at a recheck.
#[derive(Debug, Clone)]
pub(crate) enum Standing {
    /// A `lands` act's commit is already on the default branch.
    Adopted(String),
    /// Nothing landed, and the act may run at this tip.
    Current(DefaultTip),
}

/// The declared half of every recheck (acceptance, before the claim, at the
/// claim, and before each run): an act that already landed is adopted;
/// otherwise the declaration at the tip must be the one bound, and for a
/// `lands` act every version it resolved from must still be the tip's and the
/// base-merge policy must still allow the landing.
pub(crate) async fn recheck(
    orch: &Orchestrator,
    act: &FencedCommandAct,
    declared: &DeclaredAct,
) -> Result<Standing, String> {
    let tip = default_tip(orch, &act.project_id).await?;
    if let Some(lands) = &declared.lands {
        if let Some(commit) = adopted_commit(
            &tip.store,
            &declared.defined_by_commit,
            &tip.tip,
            &act_sha(act)?,
            lands,
        )? {
            return Ok(Standing::Adopted(commit));
        }
    }
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let spec = declaration_at(&jj, &tip.store, &tip.tip, &declared.name)
        .map_err(|error| format!("{DECLARATION_CHANGED}: {error}"))?;
    if declaration_digest(&spec)? != declared.declaration_digest {
        return Err(DECLARATION_CHANGED.to_string());
    }
    if declared.lands.is_some() {
        current_unchanged(declared, &|path, key| {
            json_key_at(&jj, &tip.store, &tip.tip, path, key)
        })?;
        landing_policy(orch, &act.project_id, &declared.name).await?;
    }
    Ok(Standing::Current(tip))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiers_resolve_against_the_current_version() {
        let current = ReleaseVersion::parse("1.2.3").unwrap();
        assert_eq!(current.resolve("sm").unwrap().to_string(), "1.2.4");
        assert_eq!(current.resolve("md").unwrap().to_string(), "1.3.0");
        assert_eq!(current.resolve("lg").unwrap().to_string(), "2.0.0");
    }

    #[test]
    fn a_plain_version_must_be_strictly_greater_than_the_current_one() {
        let current = ReleaseVersion::parse("1.2.3").unwrap();
        for newer in ["1.2.4", "1.3.0", "2.1.0", "1.2.10"] {
            assert_eq!(current.resolve(newer).unwrap().to_string(), newer);
        }
        for not_newer in ["1.2.3", "1.2.2", "0.9.0"] {
            assert!(
                current
                    .resolve(not_newer)
                    .unwrap_err()
                    .contains("must be greater"),
                "{not_newer}"
            );
        }
    }

    #[test]
    fn only_plain_release_versions_parse() {
        let current = ReleaseVersion(1, 2, 3);
        for bad in [
            "1.2",
            "1.2.3.4",
            "01.2.3",
            "1.2.3-alpha",
            "1.2.4+build",
            "+1.2.4",
            " 1.2.4",
            "v1.2.4",
            "",
            "patch",
        ] {
            assert!(current.resolve(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn versions_order_numerically_not_lexically() {
        assert!(ReleaseVersion(1, 2, 10) > ReleaseVersion(1, 2, 9));
        assert!(ReleaseVersion(2, 0, 0) > ReleaseVersion(1, 99, 99));
    }

    #[test]
    fn tier_overflow_is_refused_instead_of_wrapping() {
        for (current, tier) in [
            (ReleaseVersion(1, 2, u64::MAX), "sm"),
            (ReleaseVersion(1, u64::MAX, 2), "md"),
            (ReleaseVersion(u64::MAX, 1, 2), "lg"),
        ] {
            assert!(current.resolve(tier).unwrap_err().contains("overflow"));
        }
    }

    /// Cairn's own declaration, as the repository's config holds it.
    fn cairn_bump() -> ApprovedCommandSpec {
        let config = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.cairn/config.yaml"),
        )
        .unwrap();
        let (settings, _) = parse_project_settings(&config).unwrap();
        settings.approved_commands.unwrap().remove("bump").unwrap()
    }

    fn at(version: &str) -> impl Fn(&str, &str) -> Result<String, String> + '_ {
        move |path, key| {
            assert_eq!((path, key), ("package.json", "version"));
            Ok(version.to_string())
        }
    }

    fn asked(version: &str) -> BTreeMap<String, String> {
        BTreeMap::from([("version".to_string(), version.to_string())])
    }

    #[test]
    fn cairns_bump_expands_to_the_files_only_bumper_against_a_fixture_at_0_13_8() {
        let spec = cairn_bump();
        for (tier, expected) in [
            ("sm", "0.13.9"),
            ("md", "0.14.0"),
            ("lg", "1.0.0"),
            ("0.13.12", "0.13.12"),
        ] {
            let resolved =
                resolve("bump", &spec, &asked(tier), &"a".repeat(40), &at("0.13.8")).unwrap();
            assert_eq!(
                resolved.command,
                format!("bun run bumpver --files-only {expected}")
            );
            let declared = &resolved.declaration;
            assert_eq!(declared.params[0].requested, tier);
            assert_eq!(declared.params[0].resolved, expected);
            assert_eq!(declared.params[0].current.as_ref().unwrap().value, "0.13.8");
            assert!(
                declared.consequence.contains(expected),
                "{}",
                declared.consequence
            );
            assert!(!declared.consequence.contains("{version}"));
            let lands = declared.lands.as_ref().unwrap();
            assert_eq!(lands.message, format!("Bump version to {expected}"));
            assert_eq!(declared.then, Some(RequestKind::Rebuild));
            assert_eq!(
                declared.declaration_digest,
                declaration_digest(&spec).unwrap()
            );
        }
        assert_eq!(
            resolve("bump", &spec, &asked("sm"), "c", &at("0.13.8"))
                .unwrap()
                .timeout_ms,
            spec.timeout.unwrap() * 1_000
        );
    }

    #[test]
    fn a_filing_supplies_exactly_the_declared_params_with_values_they_accept() {
        let spec = cairn_bump();
        let commit = "a".repeat(40);
        for (params, refusal) in [
            (BTreeMap::new(), "requires param `version`"),
            (asked(" "), "requires param `version`"),
            (asked("0.13.8"), "must be greater"),
            (asked("0.13.9; rm -rf ~"), "Invalid release version"),
            (
                BTreeMap::from([
                    ("version".to_string(), "sm".to_string()),
                    ("tag".to_string(), "yes".to_string()),
                ]),
                "declares no param `tag`",
            ),
        ] {
            let error = resolve("bump", &spec, &params, &commit, &at("0.13.8")).unwrap_err();
            assert!(error.contains(refusal), "{error}");
        }
        let unreadable = resolve("bump", &spec, &asked("sm"), &commit, &at("next")).unwrap_err();
        assert!(
            unreadable.contains("current version in package.json"),
            "{unreadable}"
        );

        let mut choice = spec.clone();
        choice.params = BTreeMap::from([(
            "version".to_string(),
            ApprovedCommandParam::Choice {
                values: vec!["beta".into(), "stable".into()],
            },
        )]);
        let none =
            |_: &str, _: &str| -> Result<String, String> { unreachable!("a choice reads nothing") };
        assert_eq!(
            resolve("bump", &choice, &asked("stable"), &commit, &none)
                .unwrap()
                .command,
            "bun run bumpver --files-only stable"
        );
        assert!(resolve("bump", &choice, &asked("nightly"), &commit, &none)
            .unwrap_err()
            .contains("one of [beta, stable]"));
    }

    #[test]
    fn a_lands_act_is_stale_once_the_tip_reads_another_starting_version() {
        let declared = resolve("bump", &cairn_bump(), &asked("sm"), "c", &at("0.13.8"))
            .unwrap()
            .declaration;
        current_unchanged(&declared, &at("0.13.8")).unwrap();
        let stale = current_unchanged(&declared, &at("0.13.9")).unwrap_err();
        assert!(
            stale.contains("stale against main's 0.13.9")
                && stale.contains("no implicit extra bump is authorized"),
            "{stale}"
        );
    }

    #[test]
    fn the_sealed_message_carries_the_act_and_attempt_trailers_on_lines_of_their_own() {
        let message = sealed_message("Bump version to 0.13.9", "abc", "att-1");
        assert_eq!(
            message,
            "Bump version to 0.13.9\n\nCairn-Request-Act: abc\nCairn-Request-Attempt: att-1\n"
        );
    }
}
