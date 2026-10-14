use crate::orchestrator::Orchestrator;
pub use cairn_attention::wakes::schedules::*;
pub async fn fire_wake_schedule(
    orch: &Orchestrator,
    schedule: &WakeSchedule,
    now: i64,
) -> Result<bool, String> {
    cairn_attention::wakes::schedules::fire_wake_schedule(
        &crate::attention_host::runtime(orch),
        schedule,
        now,
    )
    .await
}
