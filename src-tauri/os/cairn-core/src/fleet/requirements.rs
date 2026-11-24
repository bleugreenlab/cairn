use super::selection::{
    candidate_rejection, declared_project_requirements_at, environment_identity, waits_on_machine,
    PlacementScope,
};
use super::{
    executor_public_name, learned_requirements, preparation, unix_time_ms, ExecutorConnectionState,
    Fleet,
};
use cairn_common::executor_protocol::{
    executor_names_match, CellOutcome, CellUnavailableReason, ExecutorCapabilities,
    ExecutorSelector, PlacementDecision, PlacementRejection, PlacementRejectionReason,
    ProjectCapability, ProjectPlacementRequirements, RepositoryLocator, SandboxCapabilityEvidence,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

impl Fleet {
    /// Give preparation refusals a durable store, and adopt what it holds.
    ///
    /// One task owns the table: it reads the rows, hydrates memory, then
    /// applies every later write in the order memory made it. A repair can
    /// therefore never be overtaken by an older recording, and hydration never
    /// restores a row memory has recorded or forgotten since. Optional by
    /// construction, like the queue journal.
    ///
    /// Lock order for everything below: `connections`, then
    /// `preparation_refusals`, both held across a transition, so the held
    /// state and each connection's copy of it change together.
    pub(crate) fn attach_preparation_store(&self, db: Arc<crate::storage::LocalDb>) {
        let (writer, writes) = mpsc::unbounded_channel();
        self.preparation_refusals.lock().unwrap().attach(writer);
        // Weak on purpose: the fleet owns the writer's sender, so a task that
        // held the fleet would keep its own channel open, and the database
        // with it, for as long as the process lived.
        let connections = Arc::downgrade(&self.connections);
        let refusals = Arc::downgrade(&self.preparation_refusals);
        // Off the caller's runtime when it has one, and on a thread of its own
        // when it does not (an orchestrator built outside a runtime). The task
        // spawns nothing, so a runtime of its own is enough.
        crate::orchestrator::spawn_onto(
            tokio::runtime::Handle::try_current().ok().as_ref(),
            async move {
                match preparation::load_all(&db).await {
                    Ok(rows) => {
                        if let (Some(connections), Some(refusals)) =
                            (connections.upgrade(), refusals.upgrade())
                        {
                            hydrate_preparation_refusals(&connections, &refusals, rows);
                        }
                    }
                    Err(error) => log::warn!("{error}"),
                }
                preparation::apply_writes(&db, writes).await;
            },
            || {
                log::error!(
                    "preparation refusal store could not start; refusals stay in memory, and a repair reports that it is not durable"
                )
            },
        );
    }

    /// Give learned project requirements a durable store, and adopt what it
    /// holds. Like the preparation store, optional by construction: a bare
    /// `Fleet::default()` learns in memory only.
    pub(crate) fn attach_learned_requirements_store(&self, db: Arc<crate::storage::LocalDb>) {
        let learned = self.learned_requirements.clone();
        crate::orchestrator::spawn_onto(
            tokio::runtime::Handle::try_current().ok().as_ref(),
            async move {
                if let Err(error) = learned.attach(db).await {
                    log::warn!("{error}");
                }
            },
            || {
                log::error!(
                    "learned project requirement store could not start; requirements learned \
                     from now on hold until this runner restarts"
                )
            },
        );
    }

    /// Record that `project_id`'s work needs `capability`, as
    /// `observation_id` on `executor_id` showed, so placement requires it of
    /// every machine from now on.
    pub(crate) async fn learn_project_requirement(
        &self,
        project_id: &str,
        capability: ProjectCapability,
        evidence_repository: Option<String>,
        executor_id: &str,
        observation_id: &str,
    ) {
        let is_new = !self
            .learned_requirements
            .capabilities(project_id)
            .contains(&capability);
        if let Err(error) = self
            .learned_requirements
            .learn(
                project_id,
                capability,
                evidence_repository,
                executor_id,
                observation_id,
                unix_time_ms(),
            )
            .await
        {
            log::warn!("{error}");
        }
        if is_new {
            log::info!(
                "project {project_id} now requires {capability}: learned from check observation \
                 {observation_id} on executor {executor_id}"
            );
        }
    }

    /// A project's placement requirements for a caller asking the eligibility
    /// question before a request exists: what its checkout at `project_path`
    /// declares, with the capabilities this runner learned it needs, which is
    /// the set preparation stamps on its requests. So the machines a check
    /// lane's scopes, occupancy and wait count are the ones it can be placed on.
    pub(crate) fn project_requirements_at(
        &self,
        project_id: &str,
        project_path: &Path,
    ) -> ProjectPlacementRequirements {
        let mut requirements = declared_project_requirements_at(project_path);
        requirements
            .capabilities
            .extend(self.learned_requirements.capabilities(project_id));
        requirements.capabilities.sort();
        requirements.capabilities.dedup();
        requirements
    }

    /// Every requirement this runner has learned, by project.
    pub fn learned_project_requirements(&self) -> Vec<learned_requirements::LearnedRequirement> {
        self.learned_requirements.all()
    }

    /// The capabilities each project's config declares under `requires`, as
    /// its most recent preparation on this runner read them.
    pub fn declared_project_capabilities(&self) -> Vec<(String, Vec<ProjectCapability>)> {
        let learned = self.learned_requirements.all();
        let mut declared: Vec<(String, Vec<ProjectCapability>)> = self
            .known_project_requirements
            .lock()
            .unwrap()
            .iter()
            .map(|(project_id, requirements)| {
                // What preparation stamped is declared and learned together;
                // a learned capability is rendered with its evidence instead.
                let only_declared = requirements
                    .capabilities
                    .iter()
                    .copied()
                    .filter(|capability| {
                        !learned.iter().any(|requirement| {
                            &requirement.project_id == project_id
                                && requirement.capability == *capability
                        })
                    })
                    .collect::<Vec<_>>();
                (project_id.clone(), only_declared)
            })
            .filter(|(_, capabilities)| !capabilities.is_empty())
            .collect();
        declared.sort();
        declared
    }

    /// The private GitHub repositories a machine's GitHub credential probe
    /// checks its credential against: the repository each failing fetch
    /// named, for the projects the machine serves that were learned to need
    /// a credential. At most [`GITHUB_PROBE_REPOSITORY_LIMIT`], since each is
    /// a network round trip on every probe.
    pub fn github_probe_repositories(&self, executor_id: &str) -> Vec<String> {
        let served = self
            .lock_connections()
            .get(executor_id)
            .filter(|entry| !entry.colocated)
            .map(|entry| entry.advertisement.capabilities.projects_served.clone())
            .unwrap_or_default();
        github_probe_repositories(&self.learned_requirements.all(), &served)
    }

    /// Resolves once every preparation write made so far has been applied,
    /// failing if any of them failed or the writer has stopped.
    #[cfg(test)]
    pub(crate) async fn flush_preparation_store(&self) -> Result<(), String> {
        let barrier = self.preparation_refusals.lock().unwrap().barrier()?;
        match barrier {
            Some(barrier) => barrier
                .await
                .map_err(|_| "the preparation refusal store's writer has stopped".to_string())?,
            None => Ok(()),
        }
    }

    /// Adopt the durable set of preparation refusals, restamping every live
    /// connection, since a machine can attach before the store is read.
    /// Stamp a connection that is not (yet) in the table.
    #[cfg(test)]
    pub(super) fn stamp_preparation_refusals(
        &self,
        entry: &mut ExecutorConnectionState,
    ) -> Vec<String> {
        stamp_preparation_refusals(&mut self.preparation_refusals.lock().unwrap(), entry)
    }

    /// Forget every machine's refusal for `project_id` that was recorded under
    /// a setup the project no longer runs: it describes commands nothing will
    /// run again, so it must not stand.
    pub(super) fn forget_superseded_setup_refusals(&self, project_id: &str, setup_identity: &str) {
        let mut connections = self.lock_connections();
        let mut refusals = self.preparation_refusals.lock().unwrap();
        let stale: Vec<String> = refusals
            .held
            .iter()
            .filter(|(_, held)| {
                held.get(project_id)
                    .is_some_and(|refusal| refusal.setup_identity != setup_identity)
            })
            .map(|(executor_id, _)| executor_id.clone())
            .collect();
        for executor_id in stale {
            refusals.forget(&executor_id, Some(vec![project_id.to_string()]));
            if let Some(entry) = connections.get_mut(&executor_id) {
                entry.preparation_refusals.remove(project_id);
            }
        }
    }

    /// Record that `executor_id` cannot prepare cells for `project_id` under
    /// the setup identified by `setup_identity`, because the host program
    /// `program` is not on its PATH, against the environment its live
    /// connection advertises. Returns whether a refusal was recorded (a
    /// machine no longer attached has no environment to record it against).
    pub(super) fn record_preparation_refusal(
        &self,
        executor_id: &str,
        project_id: &str,
        setup_identity: &str,
        command: &str,
        program: &str,
        diagnostic: &str,
    ) -> bool {
        {
            let mut connections = self.lock_connections();
            let Some(entry) = connections.get_mut(executor_id) else {
                return false;
            };
            let mut refusals = self.preparation_refusals.lock().unwrap();
            refusals.record(
                executor_id,
                preparation::PreparationRefusal {
                    project_id: project_id.to_string(),
                    environment_identity: preparation_environment_identity(entry),
                    setup_identity: setup_identity.to_string(),
                    command: command.to_string(),
                    program: program.to_string(),
                    diagnostic: diagnostic.to_string(),
                    recorded_at_unix_ms: unix_time_ms(),
                },
            );
            entry.preparation_refusals =
                refusals.held.get(executor_id).cloned().unwrap_or_default();
        }
        log::warn!(
            "executor {executor_id} cannot prepare cells for project {project_id}: `{program}` (run by `{command}`) is not on its PATH; excluded for that project until its evidence or the project's setup changes, or it is declared repaired"
        );
        true
    }

    /// An operator's `repaired`: re-admit the machine for every project it
    /// failed to prepare, and answer only once that is durable — an error if
    /// the delete failed or the store's writer has stopped, since a repair
    /// that a restart would undo has not happened. Does not lift a MISSING
    /// build-prerequisite verdict, which the executor's own re-check refreshes
    /// (at each attach and every 30 minutes).
    pub(crate) async fn clear_preparation_refusals(&self, executor_id: &str) -> Result<(), String> {
        let barrier = {
            let mut connections = self.lock_connections();
            let mut refusals = self.preparation_refusals.lock().unwrap();
            refusals.forget(executor_id, None);
            if let Some(entry) = connections.get_mut(executor_id) {
                entry.preparation_refusals.clear();
            }
            refusals.barrier()?
        };
        if let Some(barrier) = barrier {
            barrier.await.map_err(|_| {
                "the preparation refusal store stopped before the repair was written".to_string()
            })??;
        }
        Ok(())
    }
}

impl Fleet {
    /// Record a setup-program-not-found outcome against (machine, project), and
    /// say so in the result: which machine, that the cause is the machine's
    /// and not the branch's or the fleet's, and what happens next.
    pub(super) fn note_preparation_refusal(
        &self,
        outcome: CellOutcome,
        executor_id: &str,
        project_id: &str,
        setup_identity: &str,
        runs_in_home: bool,
    ) -> CellOutcome {
        let CellOutcome::Unavailable {
            reason:
                reason @ CellUnavailableReason::PreparationCommandNotFound {
                    command,
                    program,
                    exit_code,
                },
            diagnostic,
        } = &outcome
        else {
            return outcome;
        };
        // Work that ran no setup cannot have failed it; never record a
        // refusal that no request's setup identity would ever match.
        if setup_identity.is_empty()
            || !self.record_preparation_refusal(
                executor_id,
                project_id,
                setup_identity,
                command,
                program,
                diagnostic,
            )
        {
            return outcome;
        }
        let name = self
            .lock_connections()
            .get(executor_id)
            .map(executor_public_name)
            .unwrap_or_else(|| executor_id.to_string());
        let next = if runs_in_home {
            "; this job's environment there will be rebuilt on another machine on the next batch"
        } else {
            ""
        };
        CellOutcome::Unavailable {
            reason: reason.clone(),
            diagnostic: format!(
                "on {name}: cell preparation `{command}` failed because `{program}` is not on {name}'s PATH (exit {exit_code}). No commands ran. This is structural for {name} with project {project_id}, not for your branch or the fleet: {name} is now excluded from placement for {project_id} until it re-attaches with changed toolchains, the project's setup changes, or it is declared `repaired`{next}. Read cairn://executors/{name} for what it advertises.\n{diagnostic}"
            ),
        }
    }

    /// Copy an entry's binding chain onto the decision that produced it, so the
    /// placement record explains what happened to the machine it chose and not
    /// merely which machine that was.
    pub(super) fn record_binding_chain(&self, decision: &mut PlacementDecision, entry_id: &str) {
        (decision.binding, decision.binding_elided) = self
            .queue
            .with_entry(entry_id, |entry| {
                (entry.chain.clone(), entry.chain_elided())
            })
            .unwrap_or_default();
    }
}

// Colocated and unrestricted enrolled executors share the runner's project routing authority.
pub(super) fn serves_project(entry: &ExecutorConnectionState, project_id: &str) -> bool {
    entry.colocated
        || projects_serve(
            &entry.advertisement.capabilities.projects_served,
            project_id,
        )
}

/// The most repositories one GitHub credential probe checks.
pub const GITHUB_PROBE_REPOSITORY_LIMIT: usize = 4;

pub(super) fn github_probe_repositories(
    learned: &[learned_requirements::LearnedRequirement],
    projects_served: &[String],
) -> Vec<String> {
    let mut repositories: Vec<String> = Vec::new();
    for requirement in learned {
        let Some(repository) = requirement.evidence_repository.as_ref() else {
            continue;
        };
        if requirement.capability == ProjectCapability::GithubCredential
            && projects_serve(projects_served, &requirement.project_id)
            && cairn_common::git_credential::url_host(repository).as_deref() == Some("github.com")
            && !repositories.contains(repository)
        {
            repositories.push(repository.clone());
        }
    }
    repositories.truncate(GITHUB_PROBE_REPOSITORY_LIMIT);
    repositories
}

pub(super) fn projects_serve(projects_served: &[String], project_id: &str) -> bool {
    projects_served.is_empty() || projects_served.iter().any(|project| project == project_id)
}

/// The toolchains a project's setup invokes that this machine does not
/// advertise, as a rejection, or `None` when it advertises them all.
///
/// A toolchain no probe has finished measuring is not missing: setup is what
/// needs it, and a setup that then fails for want of it is learned as a
/// [`PlacementRejectionReason::PreparationRefused`] with its evidence (tenet
/// 951). Refusing on a probe that timed out would turn a slow host into an
/// absent tool (tenet 302).
pub(super) fn setup_toolchain_deficiency(
    capabilities: &ExecutorCapabilities,
    project_id: &str,
    requirements: &ProjectPlacementRequirements,
) -> Option<PlacementRejectionReason> {
    let mut missing = Vec::new();
    let mut evidence = Vec::new();
    for required in &requirements.setup_toolchains {
        let Some(gap) = capabilities.unadvertised_toolchain(required) else {
            continue;
        };
        if matches!(
            gap,
            cairn_common::executor_protocol::UnadvertisedToolchain::NotMeasured(_)
        ) {
            continue;
        }
        missing.push(required.clone());
        evidence.extend(gap.describe(required));
    }
    (!missing.is_empty()).then(|| PlacementRejectionReason::PreparationToolchainMissing {
        project_id: project_id.to_string(),
        missing,
        advertised: capabilities.toolchains.clone(),
        evidence,
    })
}

/// Work refused on a machine that measured itself without a usable GitHub
/// credential, when the project requires one. A machine that has not measured
/// (an older executor, or a probe that could not tell) is not refused: the
/// gate turns on for each machine as it measures, and the check classifier
/// stores an unmeasured machine's credential-less fetch as infrastructure.
pub(super) fn github_credential_deficiency(
    capabilities: &ExecutorCapabilities,
    project_id: &str,
    requirements: &ProjectPlacementRequirements,
) -> Option<PlacementRejectionReason> {
    if !requirements
        .capabilities
        .contains(&ProjectCapability::GithubCredential)
    {
        return None;
    }
    let reason = capabilities.github_credential.as_ref()?.absent_reason()?;
    Some(PlacementRejectionReason::GithubCredentialMissing {
        project_id: project_id.to_string(),
        reason: reason.to_string(),
    })
}

/// Rust work refused on a machine whose attach-time prerequisite verdict reads
/// MISSING, when the project declares the script that verdict came from.
///
/// Rust work is whatever needs `rust`, by the work's own selector or by the
/// project's setup, or `compiles_rust` work ([`PlacementScope::compiles_rust`]).
/// `Unchecked` and an absent verdict gate nothing, since
/// nothing is known, and that includes a `Missing` from a probe that could not
/// run its script (`BuildPrerequisiteCheck::effective_status`); a project that
/// does not declare the script is described by nothing the probe measured.
pub(super) fn build_prerequisite_deficiency(
    capabilities: &ExecutorCapabilities,
    project_id: &str,
    requirements: &ProjectPlacementRequirements,
    selector: Option<&ExecutorSelector>,
    compiles_rust: bool,
) -> Option<PlacementRejectionReason> {
    const RUST: &str = "rust";
    let needs_rust = compiles_rust
        || requirements
            .setup_toolchains
            .iter()
            .any(|toolchain| toolchain == RUST)
        || selector.is_some_and(|selector| {
            selector
                .required_toolchains
                .iter()
                .any(|toolchain| toolchain == RUST)
        });
    if !needs_rust {
        return None;
    }
    let check = capabilities.build_prerequisites.as_ref()?;
    if check.effective_status() != cairn_common::executor_protocol::BuildPrerequisiteStatus::Missing
    {
        return None;
    }
    let script = cairn_common::toolchain_requirements::prerequisite_script_path(&check.script);
    requirements
        .prerequisite_scripts
        .iter()
        .any(|declared| declared == script)
        .then(|| PlacementRejectionReason::BuildPrerequisitesMissing {
            project_id: project_id.to_string(),
            script: script.to_string(),
            detail: check.detail.clone(),
        })
}

/// For every project this runner knows (by a preparation, or by a refusal it
/// recorded), whether this machine could take the project's work, Rust work
/// included. Built from the same clauses [`candidate_rejection`] applies, so
/// the executors page shows exactly what placement enforces. A recorded setup
/// refusal [`refusal_applies`] rejects is carried apart, as recorded and not
/// applied, so the page can explain why placement still sends work here.
pub(super) fn project_eligibility(
    entry: &ExecutorConnectionState,
    known: &HashMap<String, ProjectPlacementRequirements>,
) -> Vec<cairn_common::executor_protocol::ProjectEligibility> {
    let capabilities = &entry.advertisement.capabilities;
    let mut projects: Vec<&String> = known
        .keys()
        .chain(entry.preparation_refusals.keys())
        .collect();
    projects.sort();
    projects.dedup();
    projects
        .into_iter()
        .filter(|project_id| serves_project(entry, project_id))
        .map(|project_id| {
            let requirements = known.get(project_id).cloned().unwrap_or_default();
            let mut refusals: Vec<PlacementRejectionReason> =
                setup_toolchain_deficiency(capabilities, project_id, &requirements)
                    .into_iter()
                    .chain(github_credential_deficiency(
                        capabilities,
                        project_id,
                        &requirements,
                    ))
                    .collect();
            let mut unapplied_refusals = Vec::new();
            if let Some(refusal) = entry.preparation_refusals.get(project_id) {
                if refusal_applies(refusal, &requirements) {
                    refusals.push(refused(refusal));
                } else {
                    unapplied_refusals.push(refused(refusal));
                }
            }
            refusals.extend(build_prerequisite_deficiency(
                capabilities,
                project_id,
                &requirements,
                None,
                true,
            ));
            cairn_common::executor_protocol::ProjectEligibility {
                project_id: project_id.clone(),
                refusals,
                unapplied_refusals,
            }
        })
        .collect()
}

/// Whether a cell over this repository runs the project's setup commands. An
/// existing checkout is a live tree the executor never prepares, and scratch
/// work has no project; neither is held to what preparation needs.
pub(super) fn runs_project_setup(repository: &RepositoryLocator) -> bool {
    matches!(
        repository,
        RepositoryLocator::ColocatedPath { .. } | RepositoryLocator::ManagedObjects { .. }
    )
}

/// The machine's recorded refusal for this project, if it applies to work
/// over `repository` running the setup `requirements` were derived from.
pub(super) fn applicable_refusal<'e>(
    entry: &'e ExecutorConnectionState,
    project_id: &str,
    repository: &RepositoryLocator,
    requirements: &ProjectPlacementRequirements,
) -> Option<&'e preparation::PreparationRefusal> {
    if !runs_project_setup(repository) {
        return None;
    }
    entry
        .preparation_refusals
        .get(project_id)
        .filter(|refusal| refusal_applies(refusal, requirements))
}

/// Whether a machine's recorded preparation refusal applies to work running
/// the setup `requirements` were derived from: the one definition placement
/// and the executors page both read. A refusal speaks only for the setup it
/// was recorded under, so one this runner cannot match does not exclude the
/// machine, whether the setup changed or this runner has not resolved the
/// project's setup since it started (an empty identity). Placement proceeds
/// there, and a fault still present refuses again and is recorded under the
/// identity then known; an unmatched refusal that excluded would need an
/// operator's `repaired` to lift.
pub(super) fn refusal_applies(
    refusal: &preparation::PreparationRefusal,
    requirements: &ProjectPlacementRequirements,
) -> bool {
    !requirements.setup_identity.is_empty() && refusal.setup_identity == requirements.setup_identity
}

pub(super) fn refused(refusal: &preparation::PreparationRefusal) -> PlacementRejectionReason {
    PlacementRejectionReason::PreparationRefused {
        project_id: refusal.project_id.clone(),
        command: refusal.command.clone(),
        program: refusal.program.clone(),
        recorded_at_unix_ms: refusal.recorded_at_unix_ms,
    }
}

/// Whether a rejection is about the machine being unable to prepare or build
/// the project's work: structural for that machine and project, and not
/// relieved by waiting.
pub(super) fn is_preparation_rejection(reason: &PlacementRejectionReason) -> bool {
    matches!(
        reason,
        PlacementRejectionReason::PreparationToolchainMissing { .. }
            | PlacementRejectionReason::PreparationRefused { .. }
            | PlacementRejectionReason::BuildPrerequisitesMissing { .. }
    )
}

/// The machines that cannot prepare this project, as one trailing sentence,
/// or nothing. Leads with a space so a caller can splice it in.
pub(super) fn describe_preparation_rejections(rejected: &[PlacementRejection]) -> String {
    let rows: Vec<String> = rejected
        .iter()
        .filter(|rejection| is_preparation_rejection(&rejection.reason))
        .map(|rejection| {
            format!(
                "{}: {}",
                rejection.executor_name,
                rejection.reason.describe()
            )
        })
        .collect();
    if rows.is_empty() {
        String::new()
    } else {
        format!(" Cannot prepare this project: {}.", rows.join("; "))
    }
}

/// The environment a preparation refusal is recorded against. Probe-derived
/// only: an operator's `repaired` clears refusals outright rather than by
/// moving an epoch, so the epoch component is held fixed.
/// Adopt the durable set of preparation refusals, restamping every live
/// connection, since a machine can attach before the store is read. Takes the
/// two tables rather than the fleet so the store's task need not own it.
pub(super) fn hydrate_preparation_refusals(
    connections: &Mutex<HashMap<String, ExecutorConnectionState>>,
    refusals: &Mutex<preparation::PreparationRefusals>,
    rows: Vec<(String, preparation::PreparationRefusal)>,
) {
    let mut connections = connections.lock().unwrap_or_else(|poisoned| {
        connections.clear_poison();
        poisoned.into_inner()
    });
    let mut refusals = refusals.lock().unwrap();
    refusals.hydrate(rows);
    for entry in connections.values_mut() {
        stamp_preparation_refusals(&mut refusals, entry);
    }
}

/// Stamp a connection with the refusals recorded against the environment it
/// advertises, forgetting (and returning the projects of) any recorded against
/// a different one. The caller holds `preparation_refusals`, and `connections`
/// too when the entry is in the table.
pub(super) fn stamp_preparation_refusals(
    refusals: &mut preparation::PreparationRefusals,
    entry: &mut ExecutorConnectionState,
) -> Vec<String> {
    let identity = preparation_environment_identity(entry);
    let executor_id = entry.identity.executor_id.clone();
    let superseded: Vec<String> = refusals
        .held
        .get(&executor_id)
        .map(|held| {
            held.values()
                .filter(|refusal| refusal.environment_identity != identity)
                .map(|refusal| refusal.project_id.clone())
                .collect()
        })
        .unwrap_or_default();
    refusals.forget(&executor_id, Some(superseded.clone()));
    entry.preparation_refusals = refusals.held.get(&executor_id).cloned().unwrap_or_default();
    superseded
}

pub(super) fn preparation_environment_identity(entry: &ExecutorConnectionState) -> String {
    environment_identity(entry, 0)
}

pub(super) fn matches_selector(
    entry: &ExecutorConnectionState,
    selector: &ExecutorSelector,
) -> bool {
    selector_shortfall(entry, selector).is_none()
}

/// The part of a selector one machine fails.
pub(super) enum SelectorShortfall {
    Name {
        wanted: String,
    },
    Platform {
        wanted: String,
    },
    /// Required toolchains the machine does not advertise, and the probe state
    /// of each where the machine reported one. A lane that names a toolchain
    /// needs it verified, so a probe that never completed refuses too, but
    /// says so rather than reading as an absence.
    Toolchains {
        missing: Vec<String>,
        states: Vec<String>,
    },
    /// The sentence [`nested_sandbox_shortfall`] states.
    NestedSandbox(String),
    /// The edge [`containment_deficiency`] names.
    Containment(String),
    /// A program a page-call batch runs that this machine's page-call probe
    /// did not run, in the sentence
    /// [`ExecutorCapabilities::page_call_program_shortfall`] states.
    PageCallProgram(String),
    /// The machine has sent no evidence at all for a capability the selector
    /// needs measured. Distinct from evidence that reads absent: the colocated
    /// executor runs its host probes after its first Ready, so for the first
    /// seconds after every attach this is a wait, not a deficiency. An
    /// executor too old to send the evidence reads the same way, which is why
    /// the age is named: seconds is a probe in flight, hours is not.
    Unreported {
        capability: &'static str,
        advertised_at_unix_ms: u64,
    },
}

impl SelectorShortfall {
    pub(super) fn describe(&self) -> String {
        match self {
            Self::Name { wanted } | Self::Platform { wanted } => format!("not {wanted}"),
            Self::Toolchains { missing, states } if states.is_empty() => {
                format!("does not advertise {}", missing.join(", "))
            }
            Self::Toolchains { missing, states } => format!(
                "does not advertise {} ({})",
                missing.join(", "),
                states.join("; ")
            ),
            Self::NestedSandbox(sentence) => sentence.clone(),
            Self::Containment(edge) => format!("containment: {edge}"),
            Self::PageCallProgram(sentence) => sentence.clone(),
            Self::Unreported {
                capability,
                advertised_at_unix_ms,
            } => format!(
                "{capability} not yet reported since it advertised {}s ago",
                unix_time_ms().saturating_sub(*advertised_at_unix_ms) / 1_000
            ),
        }
    }
}

/// The first part of `selector` this machine fails, or `None` when it
/// satisfies all of it. The one definition of a selector match:
/// [`matches_selector`] is this being `None`, so a refusal's explanation of
/// why a machine was passed over cannot disagree with the decision that
/// passed it over.
pub(super) fn selector_shortfall(
    entry: &ExecutorConnectionState,
    selector: &ExecutorSelector,
) -> Option<SelectorShortfall> {
    let capabilities = &entry.advertisement.capabilities;
    if let Some(wanted) = selector
        .name
        .as_deref()
        .filter(|wanted| !executor_names_match(wanted, &executor_public_name(entry)))
    {
        return Some(SelectorShortfall::Name {
            wanted: wanted.to_string(),
        });
    }
    if let Some(wanted) = selector
        .os
        .as_deref()
        .filter(|wanted| !wanted.eq_ignore_ascii_case(&capabilities.os))
    {
        return Some(SelectorShortfall::Platform {
            wanted: wanted.to_string(),
        });
    }
    let mut missing = Vec::new();
    let mut states = Vec::new();
    for required in &selector.required_toolchains {
        if let Some(gap) = capabilities.unadvertised_toolchain(required) {
            missing.push(required.clone());
            states.extend(gap.describe(required));
        }
    }
    if !missing.is_empty() {
        return Some(SelectorShortfall::Toolchains { missing, states });
    }
    let unreported = |capability| SelectorShortfall::Unreported {
        capability,
        advertised_at_unix_ms: entry.advertisement.observed_at_unix_ms,
    };
    if nested_sandbox_fit(capabilities, selector) == NestedSandboxFit::Refused {
        return Some(match capabilities.sandbox.as_ref() {
            None => unreported("check-lane nested Seatbelt probe"),
            Some(evidence) => SelectorShortfall::NestedSandbox(nested_sandbox_shortfall(evidence)),
        });
    }
    if selector.containment.is_some() && capabilities.containment.is_none() {
        return Some(unreported("containment probes"));
    }
    if let Some(deficiency) = containment_deficiency(capabilities, selector) {
        return Some(SelectorShortfall::Containment(deficiency));
    }
    // Asked after containment, because the program evidence is current only
    // while the page-call edges are: their fingerprint covers every program's
    // real path and digest.
    selector
        .page_call_programs
        .iter()
        .find_map(|program| capabilities.page_call_program_shortfall(program))
        .map(SelectorShortfall::PageCallProgram)
}

/// Why a macOS machine whose sandbox evidence is in hand cannot take a check
/// lane that applies its own nested Seatbelt profile, as one clause a refusal
/// can name the machine with.
pub(super) fn nested_sandbox_shortfall(evidence: &SandboxCapabilityEvidence) -> String {
    match evidence.nested_sandbox_write_context.as_ref() {
        None => "check lanes need a nested Seatbelt write measured on the check-lane path, and this machine measured it only from the executor's own process (refused until the executor updates)".to_string(),
        Some(context) => format!(
            "check lanes need a nested Seatbelt write and this machine cannot apply one ({}{})",
            evidence.nested_sandbox_write_detail.trim(),
            if context.credential_floor_sha256.is_some() {
                "; operator credential floor"
            } else {
                ""
            }
        ),
    }
}

/// Why this machine cannot take a workload envelope, or `None` if it can.
///
/// Deliberately separate from [`matches_sandbox_capability`]: that answers
/// whether a machine can *run sandbox-exercising tests*, which is a different
/// question from whether a workload is contained while it runs. Folding them
/// would read as consolidation and behave as a regression.
///
/// A machine that reports no matrix at all is refused with its own reason. "This
/// executor predates containment reporting" and "this machine measured the edge
/// and it is ambient" send an operator to entirely different places, and a
/// single "not contained" verdict would erase that difference.
pub(super) fn containment_deficiency(
    capabilities: &ExecutorCapabilities,
    selector: &ExecutorSelector,
) -> Option<String> {
    let requested = selector.containment.as_ref()?;
    match &capabilities.containment {
        None => Some(
            "this executor predates containment reporting, so no edge has evidence".to_string(),
        ),
        Some(observations) => observations
            .satisfies(requested)
            .err()
            .map(|refusal| refusal.describe()),
    }
}

/// Whether a machine satisfies the nested-Seatbelt part of a selector.
pub(super) fn matches_sandbox_capability(
    capabilities: &ExecutorCapabilities,
    selector: &ExecutorSelector,
) -> bool {
    nested_sandbox_fit(capabilities, selector) == NestedSandboxFit::Satisfied
}

/// How one machine meets a selector's nested-Seatbelt requirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NestedSandboxFit {
    /// The selector asks nothing of it, the machine is not a Mac, or its
    /// check lanes measured a nested write on the check-lane path.
    Satisfied,
    /// A Mac whose check lanes are refused nesting by the operator credential
    /// floor alone. Eligible, but taken only when nothing that satisfies the
    /// requirement could take the work ([`nested_sandbox_preference`]), with
    /// the waiver on the record. Retired by cairn/6339.
    Waivable,
    /// Any other Mac: no evidence, evidence measured from the executor's own
    /// process, or a refusal the floor does not explain.
    Refused,
}

/// Only a measurement taken on the check-lane path counts: one taken from the
/// executor's own process is how a machine whose lanes all exited 71 kept
/// receiving them (cairn/5316).
pub(super) fn nested_sandbox_fit(
    capabilities: &ExecutorCapabilities,
    selector: &ExecutorSelector,
) -> NestedSandboxFit {
    if !selector.requires_macos_nested_sandbox_write
        || !capabilities.os.eq_ignore_ascii_case("macos")
    {
        return NestedSandboxFit::Satisfied;
    }
    match capabilities.sandbox.as_ref() {
        Some(evidence) if evidence.check_lane_can_nest() => NestedSandboxFit::Satisfied,
        Some(evidence) if evidence.check_lane_floor_refuses_nesting() => NestedSandboxFit::Waivable,
        _ => NestedSandboxFit::Refused,
    }
}

/// The machines among `ordered` that satisfy the scope's nested-Seatbelt
/// requirement outright and could take the work: structurally eligible, or
/// away only for something that ends on its own (a decline, an update, a
/// provisioning turn). Empty when the scope requires nothing of the kind.
///
/// While any is named, a Mac that is merely [`NestedSandboxFit::Waivable`]
/// takes none of this work: a lane never runs without the capability while a
/// machine with it could have run it. A capable machine an operator drained,
/// or one the work's own selector excludes, does not hold the fallback back.
pub(super) fn nested_sandbox_preference(
    ordered: &[&ExecutorConnectionState],
    scope: PlacementScope<'_>,
    now_unix_ms: u64,
) -> Vec<String> {
    let Some(selector) = scope
        .selector
        .filter(|selector| selector.requires_macos_nested_sandbox_write)
    else {
        return Vec::new();
    };
    ordered
        .iter()
        .filter(|entry| matches_sandbox_capability(&entry.advertisement.capabilities, selector))
        .filter(|entry| {
            candidate_rejection(entry, scope, now_unix_ms)
                .is_none_or(|reason| waits_on_machine(&reason))
        })
        .map(|entry| executor_public_name(entry))
        .collect()
}

/// The nested-Seatbelt fact as a refusal inventory states it.
pub(super) fn nested_sandbox_summary(evidence: &SandboxCapabilityEvidence) -> String {
    let state = if evidence.nested_sandbox_write_context.is_none() {
        "unverified (measured from the executor's own process, which no check lane runs in; refused until the executor updates)"
    } else if evidence.nested_sandbox_write_available {
        "available"
    } else {
        "unavailable"
    };
    format!(
        "CAIRN_SANDBOXED={}, check-lane nested Seatbelt write={state} ({})",
        if evidence.cairn_sandboxed_marker {
            "set"
        } else {
            "unset"
        },
        evidence.nested_sandbox_write_detail
    )
}
