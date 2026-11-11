//! Who holds this host's pipe descriptors when the floor engages: the kernel's
//! descriptor census ([`cairn_procmem::fd_census`]) attributed to the runner's
//! own sessions first and to process families after.
//!
//! Session attribution runs first because the registry knows things the
//! kernel's names do not: a Claude CLI whose process name is its version
//! string is still found through its run's leader identity.

use std::collections::{BTreeMap, HashMap, HashSet};

use cairn_procmem::{FdCensus, FdCensusRow, ProcessIdentity};
use serde::{Deserialize, Serialize};

/// Families kept in the stored summary, by pipe descriptors held.
pub(crate) const TOP_FAMILIES: usize = 12;
/// Sessions kept in the stored summary, by pipe descriptors held.
pub(crate) const TOP_SESSIONS: usize = 10;

/// The runner's own process.
pub(crate) const RUNNER_FAMILY: &str = "cairn-orch";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SessionState {
    Active,
    Idle,
    /// A thread on a shared Codex app-server: no process of its own, so no
    /// descriptors are attributed to it.
    Pooled,
}

/// One registered agent process as the census attributes descriptors to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SessionProcess {
    pub run_id: String,
    pub job_id: Option<String>,
    pub session_id: Option<String>,
    /// The backend family its tree rolls up into (`claude-cli`, `codex`).
    pub family: String,
    pub state: SessionState,
    pub leader: Option<ProcessIdentity>,
}

/// The family a registered backend's process tree rolls up into.
pub(crate) fn backend_family(backend: Option<&str>) -> String {
    match backend.unwrap_or("claude") {
        "claude" | "claude-cli" | "" => "claude-cli".into(),
        other => other.into(),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FamilyCount {
    pub family: String,
    pub processes: u32,
    pub pipe_fds: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SessionCount {
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    pub family: String,
    pub state: SessionState,
    pub processes: u32,
    pub pipe_fds: u64,
}

/// Session admission's view of the runner's agent processes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AdmissionCounts {
    pub active: usize,
    pub idle: usize,
    /// Idle processes the warm collector protects.
    pub protected: usize,
    pub pooled_codex_threads: usize,
}

/// What the engagement row stores and the executor page renders.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CensusSummary {
    pub supported: bool,
    pub partial: bool,
    pub unreadable: u32,
    pub took_ms: u64,
    pub processes: u32,
    pub pipe_fds: u64,
    pub families: Vec<FamilyCount>,
    pub sessions: Vec<SessionCount>,
    pub admission: AdmissionCounts,
}

/// One counted process, for deltas and for following a released session's
/// descendants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CountedProcess {
    pub family: String,
    pub pipe_fds: u32,
}

/// A full attribution, kept in memory for the release experiment.
#[derive(Clone, Debug, Default)]
pub(crate) struct Attribution {
    pub supported: bool,
    pub partial: bool,
    pub unreadable: u32,
    pub took_ms: u64,
    pub families: BTreeMap<String, FamilyCount>,
    pub sessions: Vec<SessionCount>,
    /// Each session's tree as the census found it: the leader first, then its
    /// descendants.
    pub trees: HashMap<String, Vec<ProcessIdentity>>,
    pub processes: HashMap<ProcessIdentity, CountedProcess>,
}

/// The family of a process no session owns, by its executable's file name
/// (or its kernel name when the path did not read).
pub(crate) fn family_of(row: &FdCensusRow) -> String {
    let name = row
        .path_basename
        .as_deref()
        .filter(|name| !name.is_empty())
        .unwrap_or(&row.comm);
    match name {
        name if name.starts_with("python") => "python".into(),
        "bash" | "zsh" | "fish" | "sh" | "dash" => "shell".into(),
        name => name.into(),
    }
}

/// Attribute `census` to the runner (`runner_pid`), to each session's process
/// tree, and to families for everything else.
pub(crate) fn attribute(
    census: &FdCensus,
    sessions: &[SessionProcess],
    runner_pid: u32,
) -> Attribution {
    let mut children: HashMap<u32, Vec<&FdCensusRow>> = HashMap::new();
    for row in &census.processes {
        children.entry(row.parent_pid).or_default().push(row);
    }
    let mut attribution = Attribution {
        supported: census.supported,
        partial: census.partial,
        unreadable: census.unreadable,
        took_ms: census.took_ms,
        ..Attribution::default()
    };
    let mut claimed: HashSet<u32> = HashSet::new();
    let count = |attribution: &mut Attribution, row: &FdCensusRow, family: &str| {
        let entry = attribution
            .families
            .entry(family.to_string())
            .or_insert_with(|| FamilyCount {
                family: family.to_string(),
                ..FamilyCount::default()
            });
        entry.processes += 1;
        entry.pipe_fds += u64::from(row.pipe_fds);
        attribution.processes.insert(
            row.identity(),
            CountedProcess {
                family: family.to_string(),
                pipe_fds: row.pipe_fds,
            },
        );
    };
    if let Some(runner) = census.row(runner_pid) {
        claimed.insert(runner.pid);
        count(&mut attribution, runner, RUNNER_FAMILY);
    }
    for session in sessions {
        let mut tree = Vec::new();
        let mut pipe_fds = 0u64;
        let leader = session.leader.and_then(|leader| {
            census
                .row(leader.pid)
                .filter(|row| row.start == leader.start_time)
        });
        if let Some(leader) = leader {
            let mut frontier = vec![leader];
            while let Some(row) = frontier.pop() {
                if !claimed.insert(row.pid) {
                    continue;
                }
                tree.push(row.identity());
                pipe_fds += u64::from(row.pipe_fds);
                count(&mut attribution, row, &session.family);
                if let Some(next) = children.get(&row.pid) {
                    frontier.extend(next.iter().copied());
                }
            }
        }
        attribution.sessions.push(SessionCount {
            run_id: session.run_id.clone(),
            session_id: session.session_id.clone(),
            job_id: session.job_id.clone(),
            family: session.family.clone(),
            state: session.state,
            processes: tree.len() as u32,
            pipe_fds,
        });
        attribution.trees.insert(session.run_id.clone(), tree);
    }
    for row in &census.processes {
        if !claimed.contains(&row.pid) {
            count(&mut attribution, row, &family_of(row));
        }
    }
    attribution
}

impl Attribution {
    pub(crate) fn pipe_fds(&self) -> u64 {
        self.families.values().map(|family| family.pipe_fds).sum()
    }

    pub(crate) fn summary(&self, admission: AdmissionCounts) -> CensusSummary {
        let mut families: Vec<FamilyCount> = self.families.values().cloned().collect();
        families.sort_by(|a, b| b.pipe_fds.cmp(&a.pipe_fds).then(a.family.cmp(&b.family)));
        families.truncate(TOP_FAMILIES);
        let mut sessions = self.sessions.clone();
        sessions.sort_by(|a, b| b.pipe_fds.cmp(&a.pipe_fds).then(a.run_id.cmp(&b.run_id)));
        sessions.truncate(TOP_SESSIONS);
        CensusSummary {
            supported: self.supported,
            partial: self.partial,
            unreadable: self.unreadable,
            took_ms: self.took_ms,
            processes: self.processes.len() as u32,
            pipe_fds: self.pipe_fds(),
            families,
            sessions,
            admission,
        }
    }
}

/// How one family's pipe descriptors moved between two censuses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FamilyDelta {
    pub family: String,
    pub before: u64,
    pub after: u64,
}

impl FamilyDelta {
    pub(crate) fn change(&self) -> i64 {
        self.after as i64 - self.before as i64
    }
}

/// The families whose pipe descriptors changed, largest move first.
pub(crate) fn family_delta(before: &Attribution, after: &Attribution) -> Vec<FamilyDelta> {
    let names: HashSet<&String> = before
        .families
        .keys()
        .chain(after.families.keys())
        .collect();
    let fds = |attribution: &Attribution, name: &String| {
        attribution
            .families
            .get(name)
            .map_or(0, |family| family.pipe_fds)
    };
    let mut deltas: Vec<FamilyDelta> = names
        .into_iter()
        .map(|name| FamilyDelta {
            family: name.clone(),
            before: fds(before, name),
            after: fds(after, name),
        })
        .filter(|delta| delta.before != delta.after)
        .collect();
    deltas.sort_by(|a, b| {
        b.change()
            .abs()
            .cmp(&a.change().abs())
            .then(a.family.cmp(&b.family))
    });
    deltas.truncate(TOP_FAMILIES);
    deltas
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_procmem::identity::StartKey;

    fn row(pid: u32, parent: u32, name: &str, pipes: u32) -> FdCensusRow {
        FdCensusRow {
            pid,
            start: StartKey(pid as u64, 0),
            parent_pid: parent,
            comm: name.into(),
            path_basename: Some(name.into()),
            pipe_fds: pipes,
            total_fds: pipes + 3,
        }
    }

    fn census(rows: Vec<FdCensusRow>) -> FdCensus {
        FdCensus {
            processes: rows,
            supported: true,
            ..FdCensus::default()
        }
    }

    #[test]
    fn sessions_claim_their_trees_before_families_and_a_versioned_cli_is_found_by_its_leader() {
        let census = census(vec![
            row(1, 0, "launchd", 0),
            row(10, 1, "cairn-orch", 99),
            // A Claude CLI whose kernel name is its version string.
            FdCensusRow {
                path_basename: None,
                ..row(20, 10, "2.1.3", 12)
            },
            row(21, 20, "node", 4),
            row(30, 10, "cairn-exec", 22),
            row(31, 30, "bash", 6),
            row(40, 1, "python3.12", 8),
        ]);
        let session = SessionProcess {
            run_id: "run".into(),
            job_id: Some("job".into()),
            session_id: Some("session".into()),
            family: backend_family(None),
            state: SessionState::Idle,
            leader: Some(ProcessIdentity {
                pid: 20,
                start_time: StartKey(20, 0),
            }),
        };
        let attribution = attribute(&census, &[session], 10);
        let family = |name: &str| {
            attribution
                .families
                .get(name)
                .map(|f| (f.processes, f.pipe_fds))
        };
        assert_eq!(family(RUNNER_FAMILY), Some((1, 99)));
        assert_eq!(family("claude-cli"), Some((2, 16)), "the CLI and its child");
        assert_eq!(family("node"), None, "the session's node child rolls up");
        assert_eq!(family("cairn-exec"), Some((1, 22)));
        assert_eq!(family("shell"), Some((1, 6)));
        assert_eq!(family("python"), Some((1, 8)));
        assert_eq!(attribution.sessions[0].pipe_fds, 16);
        assert_eq!(attribution.trees["run"].len(), 2);
        let summary = attribution.summary(AdmissionCounts::default());
        assert_eq!(summary.pipe_fds, 99 + 16 + 22 + 6 + 8);
        assert_eq!(summary.families[0].family, RUNNER_FAMILY);
    }

    #[test]
    fn a_recycled_leader_pid_claims_nothing() {
        let census = census(vec![row(20, 1, "other", 5)]);
        let session = SessionProcess {
            run_id: "run".into(),
            job_id: None,
            session_id: None,
            family: "codex".into(),
            state: SessionState::Idle,
            leader: Some(ProcessIdentity {
                pid: 20,
                start_time: StartKey(1, 0),
            }),
        };
        let attribution = attribute(&census, &[session], 99);
        assert_eq!(attribution.sessions[0].processes, 0);
        assert_eq!(attribution.families["other"].pipe_fds, 5);
    }

    #[test]
    fn the_delta_names_the_largest_moves_first() {
        let before = attribute(
            &census(vec![row(1, 0, "claude", 40), row(2, 0, "node", 10)]),
            &[],
            0,
        );
        let after = attribute(&census(vec![row(2, 0, "node", 12)]), &[], 0);
        let delta = family_delta(&before, &after);
        assert_eq!(
            delta,
            vec![
                FamilyDelta {
                    family: "claude".into(),
                    before: 40,
                    after: 0
                },
                FamilyDelta {
                    family: "node".into(),
                    before: 10,
                    after: 12
                },
            ]
        );
    }
}
