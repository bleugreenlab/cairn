use crate::{Census, ProcessIdentity, StartKey};
use std::collections::BTreeMap;

/// Only verified identities enter this set; an unreadable PID never creates one.
pub(crate) struct Membership {
    leader: ProcessIdentity,
    verified: bool,
    pub(crate) members: BTreeMap<u32, StartKey>,
}

#[derive(Default)]
pub(crate) struct Changes {
    pub joined: Vec<ProcessIdentity>,
    pub departed: Vec<ProcessIdentity>,
    pub unreadable: Vec<ProcessIdentity>,
}

impl Membership {
    pub(crate) fn new(leader: ProcessIdentity) -> Self {
        Self {
            leader,
            verified: false,
            members: BTreeMap::new(),
        }
    }

    pub(crate) fn verified(&self) -> bool {
        self.verified
    }

    pub(crate) fn unverified_candidates(&self, census: &Census) -> Vec<u32> {
        let mut candidates = std::collections::BTreeSet::new();
        loop {
            let before = candidates.len();
            for (pid, parent) in &census.unverified_parents {
                if !self.members.contains_key(pid)
                    && (self.members.contains_key(parent) || candidates.contains(parent))
                {
                    candidates.insert(*pid);
                }
            }
            if candidates.len() == before {
                break;
            }
        }
        candidates.into_iter().collect()
    }

    #[cfg(any(not(windows), test))]
    pub(crate) fn fold(&mut self, census: &Census) -> Changes {
        self.fold_scoped(census, &[])
    }

    #[cfg(any(not(windows), test))]
    pub(crate) fn fold_scoped(&mut self, census: &Census, scoped: &[ProcessIdentity]) -> Changes {
        self.fold_with_scope(census, scoped, false)
    }

    pub(crate) fn fold_with_scope(
        &mut self,
        census: &Census,
        scoped: &[ProcessIdentity],
        scope_incomplete: bool,
    ) -> Changes {
        let mut changes = Changes::default();
        if !self.verified {
            if census
                .row(self.leader.pid)
                .is_some_and(|row| row.process.start == self.leader.start_time)
            {
                self.verified = true;
                self.members.insert(self.leader.pid, self.leader.start_time);
                changes.joined.push(self.leader);
            } else {
                return changes;
            }
        }
        self.members.retain(|pid, start| {
            let identity = ProcessIdentity {
                pid: *pid,
                start_time: *start,
            };
            let kept = match census.row(*pid) {
                Some(row) => row.process.start == *start,
                None if census.enumeration_incomplete
                    || scope_incomplete
                    || census.unreadable.binary_search(pid).is_ok() =>
                {
                    changes.unreadable.push(identity);
                    true
                }
                None => false,
            };
            if !kept {
                changes.departed.push(identity);
            }
            kept
        });
        for identity in scoped {
            if census
                .row(identity.pid)
                .is_some_and(|row| row.process.start == identity.start_time)
                && self.members.insert(identity.pid, identity.start_time)
                    != Some(identity.start_time)
            {
                changes.joined.push(*identity);
            }
        }
        loop {
            let mut joined = false;
            for row in &census.rows {
                let p = &row.process;
                if self.members.contains_key(&p.pid) {
                    continue;
                }
                let leads = |id: u32, field: fn(&crate::KernelProcess) -> Option<u32>| {
                    self.members.contains_key(&id)
                        && census
                            .row(id)
                            .is_some_and(|leader| field(&leader.process) == Some(id))
                };
                let parent_attached = self.members.get(&p.parent_pid).is_some_and(|start| {
                    // Windows snapshots carry parent PIDs, not parent handles.
                    // A parent newer than its alleged child is a recycled PID.
                    !cfg!(windows)
                        || (p.start >= *start
                            && census
                                .row(p.parent_pid)
                                .is_some_and(|parent| parent.process.start == *start))
                });
                let attached = parent_attached
                    || leads(p.process_group_id, |p| Some(p.process_group_id))
                    || p.session_id
                        .is_some_and(|session| leads(session, |p| p.session_id));
                if attached {
                    self.members.insert(p.pid, p.start);
                    changes.joined.push(ProcessIdentity {
                        pid: p.pid,
                        start_time: p.start,
                    });
                    joined = true;
                }
            }
            if !joined {
                break;
            }
        }
        changes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CensusRow, KernelProcess};
    fn row(pid: u32, parent_pid: u32, start: u64) -> CensusRow {
        CensusRow {
            process: KernelProcess {
                pid,
                parent_pid,
                process_group_id: pid,
                session_id: None,
                start: StartKey(start, 0),
            },
            resident_bytes: Some(10),
            physical_footprint_bytes: None,
            cpu_ms: Some(0),
            storage_io_bytes: None,
            uninterruptible: false,
            scheduling: None,
        }
    }
    fn census(rows: Vec<CensusRow>) -> Census {
        let mut c = Census {
            rows,
            ..Census::default()
        };
        c.rows.sort_by_key(|r| r.process.pid);
        c
    }
    fn tree() -> Membership {
        Membership::new(ProcessIdentity {
            pid: 10,
            start_time: StartKey(1, 0),
        })
    }

    #[test]
    fn wrong_or_unreadable_initial_leader_confers_nothing() {
        let mut m = tree();
        m.fold(&census(vec![row(10, 1, 2), row(11, 10, 3)]));
        assert!(m.members.is_empty());
        let mut c = census(vec![row(11, 10, 3)]);
        c.unreadable.push(10);
        m.fold(&c);
        assert!(m.members.is_empty());
        assert!(!m.verified());
    }
    #[test]
    fn detached_child_survives_leader_exit_but_not_pid_reuse() {
        let mut m = tree();
        assert_eq!(
            m.fold(&census(vec![row(10, 1, 1), row(11, 10, 2)]))
                .joined
                .len(),
            2
        );
        let changes = m.fold(&census(vec![row(11, 1, 2)]));
        assert_eq!(changes.departed.len(), 1);
        assert_eq!(m.members.len(), 1);
        m.fold(&census(vec![row(11, 1, 3)]));
        assert!(m.members.is_empty());
    }
    #[test]
    fn known_unreadable_and_enumeration_gaps_retain_without_inventing_identity() {
        let mut m = tree();
        m.fold(&census(vec![row(10, 1, 1)]));
        let mut c = Census {
            unreadable: vec![10, 999],
            ..Census::default()
        };
        assert_eq!(m.fold(&c).unreadable.len(), 1);
        assert_eq!(m.members.len(), 1);
        c.unreadable.clear();
        c.enumeration_incomplete = true;
        assert_eq!(m.fold(&c).unreadable.len(), 1);
        c.enumeration_incomplete = false;
        assert_eq!(m.fold(&c).departed.len(), 1);
    }
    #[test]
    fn closure_uses_groups_and_sessions_led_by_verified_members_only() {
        let mut root = row(10, 1, 1);
        root.process.session_id = Some(10);
        let mut group = row(12, 1, 3);
        group.process.process_group_id = 10;
        let mut session = row(13, 1, 4);
        session.process.session_id = Some(10);
        let mut outsider = row(14, 1, 5);
        outsider.process.session_id = Some(99);
        let mut m = tree();
        m.fold(&census(vec![root, group, session, outsider]));
        assert_eq!(
            m.members.keys().copied().collect::<Vec<_>>(),
            vec![10, 12, 13]
        );
    }
}

#[cfg(test)]
mod scope_tests {
    use super::*;
    use crate::{CensusRow, KernelProcess};
    fn row(pid: u32, parent_pid: u32, start: u64) -> CensusRow {
        CensusRow {
            process: KernelProcess {
                pid,
                parent_pid,
                process_group_id: pid,
                session_id: None,
                start: StartKey(start, 0),
            },
            resident_bytes: Some(1),
            physical_footprint_bytes: None,
            cpu_ms: Some(1),
            storage_io_bytes: None,
            uninterruptible: false,
            scheduling: None,
        }
    }
    fn identity(pid: u32, start: u64) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            start_time: StartKey(start, 0),
        }
    }

    #[test]
    fn unknown_candidate_hints_prove_uncertainty_not_identity_or_membership() {
        let mut membership = Membership::new(identity(10, 1));
        let census = Census {
            rows: vec![row(10, 1, 1)],
            unverified_parents: vec![(11, 10), (12, 11), (99, 1)],
            ..Census::default()
        };
        membership.fold(&census);
        assert_eq!(membership.unverified_candidates(&census), vec![11, 12]);
        assert_eq!(
            membership.members.keys().copied().collect::<Vec<_>>(),
            vec![10]
        );
    }
    #[test]
    fn only_verified_scoped_rows_can_join_and_wrong_initial_leader_conveys_nothing() {
        let census = Census {
            rows: vec![row(10, 1, 1), row(20, 1, 2)],
            ..Census::default()
        };
        let mut membership = Membership::new(identity(10, 1));
        membership.fold_scoped(&census, &[identity(20, 2), identity(30, 3)]);
        assert_eq!(
            membership.members.keys().copied().collect::<Vec<_>>(),
            vec![10, 20]
        );
        let mut wrong = Membership::new(identity(10, 99));
        wrong.fold_scoped(&census, &[identity(20, 2)]);
        assert!(wrong.members.is_empty());
    }
    #[test]
    fn uncertain_scope_retains_known_members_but_verified_reuse_still_departs() {
        let mut membership = Membership::new(identity(10, 1));
        let initial = Census {
            rows: vec![row(10, 1, 1), row(20, 1, 2)],
            ..Census::default()
        };
        membership.fold_with_scope(&initial, &[identity(20, 2)], false);
        let missing = Census {
            rows: vec![row(10, 1, 1)],
            ..Census::default()
        };
        let changes = membership.fold_with_scope(&missing, &[], true);
        assert_eq!(changes.unreadable, vec![identity(20, 2)]);
        assert!(changes.departed.is_empty());
        let reused = Census {
            rows: vec![row(10, 1, 1), row(20, 1, 99)],
            ..Census::default()
        };
        let changes = membership.fold_with_scope(&reused, &[], true);
        assert_eq!(changes.departed, vec![identity(20, 2)]);
        assert!(!membership.members.contains_key(&20));
    }
    #[cfg(windows)]
    #[test]
    fn a_new_child_requires_a_currently_verified_parent_and_creation_ordering() {
        let mut membership = Membership::new(identity(10, 10));
        membership.fold(&Census {
            rows: vec![row(10, 1, 10)],
            ..Census::default()
        });
        membership.fold(&Census {
            rows: vec![row(11, 10, 11)],
            unreadable: vec![10],
            ..Census::default()
        });
        assert_eq!(
            membership.members.keys().copied().collect::<Vec<_>>(),
            vec![10]
        );
        membership.fold(&Census {
            rows: vec![row(10, 1, 10), row(11, 10, 9)],
            ..Census::default()
        });
        assert_eq!(
            membership.members.keys().copied().collect::<Vec<_>>(),
            vec![10]
        );
    }
}
