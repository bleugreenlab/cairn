//! Known Cairn reporters and their stable attention identities.

use crate::messages::queued::DeliveryUrgency;

pub(crate) struct Reporter {
    pub name: &'static str,
    pub uri: &'static str,
    pub reports: &'static str,
    pub urgency: DeliveryUrgency,
    pub urgency_reason: &'static str,
}

pub(crate) const MAIN_CADENCE: Reporter = Reporter {
    name: "main cadence",
    uri: "cairn://reporters/main-cadence",
    reports: "News from default-branch check lanes, including new red results, infrastructure failures, restart streaks, and recovery.",
    urgency: DeliveryUrgency::Steer,
    urgency_reason: "Check news needs prompt triage; a new red must not wait behind the thread's current work.",
};

pub(crate) const JJ_BRANCH_RECOVERY: Reporter = Reporter {
    name: "jj branch recovery",
    uri: "cairn://reporters/jj-branch-recovery",
    reports: "Branch-store recovery and fencing notices for the threads responsible for the fleet.",
    urgency: DeliveryUrgency::Steer,
    urgency_reason: "A store fence or an unowned recovery is an outage that can block other work.",
};

pub(crate) const DEFAULT_BRANCH_WATCH: Reporter = Reporter {
    name: "default-branch watch",
    uri: "cairn://reporters/default-branch-watch",
    reports: "Incidents where the store's default branch holds commits beyond origin that no Cairn landing produced.",
    urgency: DeliveryUrgency::Steer,
    urgency_reason: "The incident blocks landings and risks carrying unreviewed commits into other work.",
};

pub(crate) const ALL: &[Reporter] = &[MAIN_CADENCE, JJ_BRANCH_RECOVERY, DEFAULT_BRANCH_WATCH];
