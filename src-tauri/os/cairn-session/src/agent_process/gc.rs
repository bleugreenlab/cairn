//! Memory-budget selection for already-scored, unprotected warm processes.
//! The host resolves job relevance and protection before supplying candidates.
use super::memory::SystemMemory;
pub const MIB: u64 = 1024 * 1024;
pub const GIB: u64 = 1024 * MIB;

/// Floor on the memory the GC keeps free: the larger of this and 10% of total.
pub const MIN_RESERVE: u64 = 2 * GIB;
/// Assumed RSS for a warm process we cannot measure and have no measured
/// siblings to borrow a median from.
pub const DEFAULT_UNMEASURED_RSS: u64 = 512 * MIB;
/// Assumed RSS of a process about to spawn when no warm sibling was measurable.
pub const DEFAULT_SPAWN_RSS: u64 = GIB;

/// The memory this host keeps free: the larger of [`MIN_RESERVE`] and 10% of
/// total. One number shared by the warm GC and session admission, so admitting
/// a session never plans to spend memory the next GC sweep would reclaim by
/// evicting a warm process.
pub fn memory_reserve(total: u64) -> u64 {
    std::cmp::max(MIN_RESERVE, total / 10)
}

/// Room a collection frees beyond the reserve for work about to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnRoom {
    /// The periodic sweep: only the reserve.
    None,
    /// A spawn of unknown size: the largest measured warm RSS, else
    /// [`DEFAULT_SPAWN_RSS`].
    NextProcess,
    /// A queued admission whose claim declares its own cost.
    Bytes(u64),
}

/// A warm process scored for possible eviction, with its measured RSS (if any).
pub struct ScoredCandidate {
    pub run_id: String,
    pub score: i32,
    pub rss: Option<u64>,
}
/// Median of a slice (upper-middle for even lengths), or `None` when empty.
pub fn median(values: &[u64]) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    Some(sorted[sorted.len() / 2])
}
pub fn choose_evictions(
    mut candidates: Vec<ScoredCandidate>,
    warm_count: usize,
    system: Option<SystemMemory>,
    spawn_room: SpawnRoom,
    max_warm: usize,
) -> Vec<String> {
    // Impute unmeasured RSS from the median of measured siblings so an
    // un-probeable process still counts toward the budget.
    let measured: Vec<u64> = candidates.iter().filter_map(|c| c.rss).collect();
    let imputed_rss = median(&measured).unwrap_or(DEFAULT_UNMEASURED_RSS);
    let effective_rss = |c: &ScoredCandidate| c.rss.unwrap_or(imputed_rss);

    // Lowest score first — least relevant evicted first.
    candidates.sort_by_key(|candidate| candidate.score);

    let aggregate_rss: u64 = candidates.iter().map(effective_rss).sum();

    let evicted: Vec<String> = match system {
        Some(SystemMemory { total, available }) => {
            let reserve = memory_reserve(total);
            let spawn_room = match spawn_room {
                SpawnRoom::None => 0,
                SpawnRoom::NextProcess => {
                    measured.iter().copied().max().unwrap_or(DEFAULT_SPAWN_RSS)
                }
                SpawnRoom::Bytes(bytes) => bytes,
            };
            let needed = reserve.saturating_add(spawn_room);
            let mut reclaimed = 0u64;
            let mut set = Vec::new();
            for candidate in &candidates {
                if available.saturating_add(reclaimed) >= needed {
                    break;
                }
                reclaimed = reclaimed.saturating_add(effective_rss(candidate));
                set.push(candidate.run_id.clone());
            }
            set
        }
        None => {
            // Fallback: trim to the count cap in one pass. Uses total warm
            // count (protected included) so the cap reflects real pool size,
            // but only unprotected candidates are evicted.
            if warm_count <= max_warm {
                Vec::new()
            } else {
                let to_evict = warm_count - max_warm;
                candidates
                    .iter()
                    .take(to_evict)
                    .map(|c| c.run_id.clone())
                    .collect()
            }
        }
    };

    log::info!(
            "GC: warm={} candidates={} agg_rss={}MiB available={}MiB measurable={} evicting {} run(s): {:?}",
            warm_count,
            candidates.len(),
            aggregate_rss / MIB,
            system.map(|s| s.available / MIB).unwrap_or(0),
            system.is_some(),
            evicted.len(),
            evicted
                .iter()
                .map(|r| r[..r.len().min(8)].to_string())
                .collect::<Vec<_>>(),
        );

    evicted
}

/// The single least relevant unprotected warm process: what a full explicit
/// unit ceiling gives up so a queued session can take its place. One, not a
/// set, because each eviction frees exactly one unit and its release
/// re-triggers promotion. Callers pass only unprotected candidates.
pub fn choose_unit_eviction(candidates: Vec<ScoredCandidate>) -> Option<String> {
    candidates
        .into_iter()
        .min_by_key(|candidate| candidate.score)
        .map(|candidate| candidate.run_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(run_id: &str, score: i32, rss: Option<u64>) -> ScoredCandidate {
        ScoredCandidate {
            run_id: run_id.into(),
            score,
            rss,
        }
    }

    #[test]
    fn memory_reserve_is_the_floor_or_a_tenth_of_total() {
        assert_eq!(memory_reserve(8 * GIB), MIN_RESERVE);
        assert_eq!(memory_reserve(128 * GIB), 128 * GIB / 10);
    }

    #[test]
    fn byte_spawn_room_reclaims_exactly_what_the_head_needs() {
        // 64 GiB total keeps a 6.4 GiB reserve; 6.5 GiB is available. A 1 GiB
        // head needs 0.9 GiB more: the lowest-scoring 600 MiB process is not
        // enough alone, so two go, and the third stays.
        let evicted = choose_evictions(
            vec![
                candidate("keep", 50, Some(600 * MIB)),
                candidate("first", -30, Some(600 * MIB)),
                candidate("second", -10, Some(600 * MIB)),
            ],
            3,
            Some(SystemMemory {
                total: 64 * GIB,
                available: memory_reserve(64 * GIB) + 100 * MIB,
            }),
            SpawnRoom::Bytes(GIB),
            6,
        );
        assert_eq!(evicted, vec!["first".to_string(), "second".to_string()]);
        assert!(choose_evictions(
            vec![candidate("idle", -30, Some(600 * MIB))],
            1,
            Some(SystemMemory {
                total: 64 * GIB,
                available: memory_reserve(64 * GIB) + 100 * MIB,
            }),
            SpawnRoom::None,
            6,
        )
        .is_empty());
    }

    #[test]
    fn unit_eviction_picks_the_single_lowest_score() {
        assert_eq!(
            choose_unit_eviction(vec![
                candidate("viewed", 50, None),
                candidate("stale", -90, None),
                candidate("recent", -10, None),
            ]),
            Some("stale".to_string())
        );
        assert_eq!(choose_unit_eviction(Vec::new()), None);
    }
}
