use std::path::Path;

use cairn_common::executor_protocol::{
    CellOccupant, LifetimeLeaseAcquireRequest, LifetimeLeaseFence, LifetimeLeaseOperation,
    LifetimeLeaseOwner, LifetimeLeaseResult,
};

use crate::orchestrator::Orchestrator;

pub(crate) async fn resolve_logical_commit(
    orch: &Orchestrator,
    repository_path: &Path,
    branch: &str,
) -> Result<String, String> {
    let jj_binary_path = orch.jj_binary_path.clone();
    let config_dir = orch.config_dir.clone();
    let repository_path = repository_path.to_path_buf();
    let branch = branch.to_string();
    tokio::task::spawn_blocking(move || {
        let jj = crate::jj::JjEnv::resolve(&jj_binary_path, &config_dir);
        let store = crate::jj::project_store_dir(&config_dir, &repository_path);
        crate::jj::bookmark_commit(&jj, &store, &branch).ok_or_else(|| {
            format!("logical branch `{branch}` does not resolve to a committed head")
        })
    })
    .await
    .map_err(|error| format!("logical branch resolution task failed: {error}"))?
}

pub(crate) async fn acquire(
    orch: &Orchestrator,
    request: LifetimeLeaseAcquireRequest,
) -> Result<LifetimeLeaseFence, String> {
    let lease_id = request.declaration.lease_id.clone();
    let owner = request.declaration.owner.clone();
    let result = orch
        .fleet
        .operate_lifetime_lease(orch, LifetimeLeaseOperation::Acquire { request })
        .await;
    let LifetimeLeaseResult::State { cell } = result else {
        return Err(format!("lifetime lease acquisition failed: {result:?}"));
    };
    let lease = cell
        .occupant
        .as_ref()
        .and_then(CellOccupant::lifetime)
        .ok_or_else(|| "lifetime lease acquisition returned no lifetime occupant".to_string())?;
    Ok(LifetimeLeaseFence {
        lease_id,
        owner,
        incarnation_id: lease.incarnation_id.clone(),
        lease_epoch: cell.lease_epoch,
    })
}

async fn operation(
    orch: &Orchestrator,
    operation: LifetimeLeaseOperation,
    action: &str,
) -> Result<LifetimeLeaseResult, String> {
    let result = orch.fleet.operate_lifetime_lease(orch, operation).await;
    match result {
        LifetimeLeaseResult::State { .. } | LifetimeLeaseResult::Released { .. } => Ok(result),
        other => Err(format!("lifetime lease {action} failed: {other:?}")),
    }
}

pub(crate) async fn refresh(
    orch: &Orchestrator,
    fence: &LifetimeLeaseFence,
    commit: &str,
) -> Result<(), String> {
    operation(
        orch,
        LifetimeLeaseOperation::RefreshCheckout {
            fence: fence.clone(),
            base_commit: commit.to_string(),
        },
        "checkout refresh",
    )
    .await?;
    Ok(())
}

pub(crate) async fn renew(orch: &Orchestrator, fence: &LifetimeLeaseFence) -> Result<(), String> {
    operation(
        orch,
        LifetimeLeaseOperation::Renew {
            fence: fence.clone(),
        },
        "renewal",
    )
    .await?;
    Ok(())
}

pub(crate) async fn stop(
    orch: &Orchestrator,
    fence: &LifetimeLeaseFence,
    process_key: &str,
) -> Result<(), String> {
    operation(
        orch,
        LifetimeLeaseOperation::StopProcess {
            fence: fence.clone(),
            process_key: process_key.to_string(),
        },
        "process stop",
    )
    .await?;
    Ok(())
}

pub(crate) async fn release(orch: &Orchestrator, fence: &LifetimeLeaseFence) -> Result<(), String> {
    operation(
        orch,
        LifetimeLeaseOperation::Release {
            fence: fence.clone(),
        },
        "release",
    )
    .await?;
    Ok(())
}

pub(crate) async fn rollback(orch: &Orchestrator, fence: &LifetimeLeaseFence, process_key: &str) {
    let _ = stop(orch, fence, process_key).await;
    let _ = release(orch, fence).await;
}

pub(crate) fn owner(
    kind: cairn_common::executor_protocol::LifetimeLeaseOwnerKind,
    id: impl Into<String>,
) -> LifetimeLeaseOwner {
    LifetimeLeaseOwner {
        kind,
        owner_id: id.into(),
    }
}
