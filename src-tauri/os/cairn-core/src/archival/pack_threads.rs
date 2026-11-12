//! The thread source of the shared pack walk, not a second maintenance loop.
use super::*;

pub(super) async fn walk(
    db: &LocalDb,
    store: &dyn ContentStore,
    now: i64,
    cutoff: i64,
    budget: PackBudget,
    started: Instant,
    report: &mut PackReport,
) -> Result<bool, Stop> {
    // Reopened tasks must release their unevicted mapping before eligibility
    // is built. Each probe is one gate unit; eviction is never undone.
    if !probe_staleness(db, budget, started, report).await? {
        return Ok(false);
    }
    let saved = progress::load(db, progress::PACK_THREAD)
        .await
        .map_err(|e| Stop::halt(format!("loading thread pack cursor: {e}")))?;
    let mut after = saved.cursor.unwrap_or_default();
    loop {
        if started.elapsed() >= budget.pass_time {
            return Ok(false);
        }
        let owners = thread_range::owners(db, &after, CANDIDATE_PAGE as usize)
            .await
            .map_err(|e| Stop::halt(format!("loading thread pack owners: {e}")))?;
        if owners.is_empty() {
            save_cursor(db, None).await?;
            return Ok(true);
        }
        for owner in owners {
            if started.elapsed() >= budget.pass_time {
                return Ok(false);
            }
            let built = thread_range::build_ranges(
                db,
                &owner,
                cutoff,
                report.bytes_per_row,
                budget.max_pack_bytes,
            )
            .await
            .map_err(|e| Stop::halt(format!("building thread ranges: {e}")))?;
            for skip in built.skips {
                report.threads.skipped += 1;
                report.skipped.push(Passed {
                    scope_id: skip.scope_id,
                    reason: skip.reason,
                });
            }
            for candidate in built.candidates {
                if started.elapsed() >= budget.pass_time {
                    // Do not advance past this owner. Published ranges will be
                    // excluded when the next pass builds the remainder.
                    return Ok(false);
                }
                check_archive(store, budget).await?;
                let label = format!(
                    "thread {} sessions {}-{} ({})",
                    candidate.thread_name,
                    candidate.range.first_sequence,
                    candidate.range.last_sequence,
                    candidate.range.owner_job_id
                );
                let work = WorkUnit {
                    unit: PackUnit::ThreadRange(candidate.range.clone()),
                    resolved_at: candidate.resolved_at,
                    thread: Some(candidate),
                    label,
                };
                let before = (report.packed, report.skipped.len(), report.failed.len());
                pack_unit_candidate(db, store, now, budget, &work, report).await?;
                report.threads.packed += report.packed - before.0;
                report.threads.skipped += report.skipped.len() - before.1;
                report.threads.failed += report.failed.len() - before.2;
                tokio::task::yield_now().await;
                fold_if_due(
                    db,
                    budget.fold_at_log_bytes,
                    &mut report.fold,
                    "history pack",
                )
                .await;
                if report.packed >= budget.max_executions {
                    return Err(Stop::budget(format!(
                        "the limit of {} scopes is reached",
                        budget.max_executions
                    )));
                }
            }
            save_cursor(db, Some(&owner.job_id)).await?;
            after = owner.job_id;
        }
    }
}

async fn save_cursor(db: &LocalDb, job_id: Option<&str>) -> Result<(), Stop> {
    progress::save(
        db,
        progress::PACK_THREAD,
        &Progress {
            watermark: None,
            cursor: job_id.map(str::to_string),
        },
    )
    .await
    .map_err(|e| Stop::halt(format!("saving thread pack cursor: {e}")))
}

async fn probe_staleness(
    db: &LocalDb,
    budget: PackBudget,
    started: Instant,
    report: &mut PackReport,
) -> Result<bool, Stop> {
    let mut after = String::new();
    loop {
        if started.elapsed() >= budget.pass_time {
            return Ok(false);
        }
        let scopes = thread_range::unevicted_ranges(db, &after, CANDIDATE_PAGE as usize)
            .await
            .map_err(|e| Stop::halt(format!("loading thread staleness probes: {e}")))?;
        if scopes.is_empty() {
            return Ok(true);
        }
        for scope in scopes {
            if started.elapsed() >= budget.pass_time {
                return Ok(false);
            }
            let mut waited = Duration::ZERO;
            let permit = acquire_gate(&mut waited).await.map_err(Stop::halt)?;
            let held = Instant::now();
            report.staleness_probes += 1;
            let result = async {
                if thread_range::is_stale(db, &scope).await? {
                    return thread_range::invalidate_unevicted(db, &scope).await;
                }
                Ok(false)
            }
            .await;
            drop(permit);
            let held = held.elapsed();
            report.gate_wait += waited;
            report.gate_wait_max = report.gate_wait_max.max(waited);
            if held > report.hold_max {
                report.hold_max = held;
                report.hold_max_scope = Some(scope.clone());
            }
            if result.map_err(|e: crate::storage::DbError| {
                Stop::halt(format!("probing thread range {scope}: {e}"))
            })? {
                report.stale_ranges += 1;
            }
            after = scope;
            tokio::task::yield_now().await;
            fold_if_due(
                db,
                budget.fold_at_log_bytes,
                &mut report.fold,
                "history pack",
            )
            .await;
        }
    }
}

/// Bounded diagnostics: unknown is preferable to extending the maintenance
/// budget by walking every thread's history merely to count what remains.
pub(super) async fn probe_remaining(
    db: &LocalDb,
    cutoff: i64,
    budget: PackBudget,
    bytes_per_row: u64,
    allow_build: bool,
) -> Option<usize> {
    tokio::time::timeout(Duration::from_secs(1), async {
        let started = Instant::now();
        let mut after = String::new();
        let mut count = 0;
        loop {
            let owners = thread_range::owners(db, &after, CANDIDATE_PAGE as usize)
                .await
                .ok()?;
            if owners.is_empty() {
                return Some(count);
            }
            for owner in owners {
                if !allow_build || started.elapsed() > Duration::from_secs(1) {
                    return None;
                }
                let built = thread_range::build_ranges(
                    db,
                    &owner,
                    cutoff,
                    bytes_per_row.max(1),
                    budget.max_pack_bytes,
                )
                .await
                .ok()?;
                count += built.candidates.len() + built.skips.len();
                if count > REMAINING_PROBE {
                    return Some(REMAINING_PROBE + 1);
                }
                after = owner.job_id;
            }
        }
    })
    .await
    .ok()
    .flatten()
}
