//! Node wake subscription resource mutation dispatch, relocated from dispatch.rs.

use super::super::{build_failure, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_uri::CairnResource;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (
            CairnResource::NodeWakes {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Append,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "wakes append requires payload"))?;
            let job_id = crate::resources::node::resolve_node_or_task_job_id(
                orch.db.for_project(project).await.as_ref(),
                project,
                *number,
                *exec_seq,
                node_id,
                None,
            )
            .await
            .map_err(|error| build_failure(index, item, error))?;
            let created_by = if request.run_id.is_some() {
                "agent"
            } else {
                "user"
            };
            if let Some(value) = payload.get("condition") {
                let condition = parse_condition_create(index, item, value)?;
                let db = orch.db.for_project(project).await;
                let thread_id =
                    crate::orchestrator::wakes::thread_home_for_job(db.as_ref(), &job_id)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                if dry_run {
                    format!("Would arm SQL condition: {}", condition.reason)
                } else {
                    let created = crate::orchestrator::wakes::conditions::create_wake_condition(
                        db.as_ref(),
                        &thread_id,
                        &condition.sql,
                        &condition.reason,
                        condition.once,
                        created_by,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    let results =
                        crate::orchestrator::wakes::conditions::evaluate_thread_conditions(
                            orch, &thread_id,
                        )
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    let (_, outcome, fired) = results
                        .into_iter()
                        .find(|(id, _, _)| id == &created.id)
                        .ok_or_else(|| {
                            build_failure(
                                index,
                                item,
                                "condition disappeared before its initial evaluation",
                            )
                        })?;
                    match outcome {
                        crate::orchestrator::wakes::conditions::ConditionOutcome::Matched {
                            ..
                        } => format!(
                            "Armed condition {} — currently matched, {}",
                            created.id,
                            if fired {
                                "fired now"
                            } else {
                                "delivery pending"
                            }
                        ),
                        crate::orchestrator::wakes::conditions::ConditionOutcome::Empty => {
                            format!("Armed condition {} — currently empty, waiting", created.id)
                        }
                        crate::orchestrator::wakes::conditions::ConditionOutcome::Error(error) => {
                            format!(
                                "Armed condition {} — evaluation failed and {}: {}",
                                created.id,
                                if fired {
                                    "reported now"
                                } else {
                                    "delivery pending"
                                },
                                error
                            )
                        }
                    }
                }
            } else if let Some(value) = payload.get("schedule") {
                let schedule = parse_schedule_create(index, item, value)?;
                let thread_id = crate::orchestrator::wakes::thread_home_for_job(
                    orch.db.for_project(project).await.as_ref(),
                    &job_id,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                if dry_run {
                    let first_wake =
                        chrono::Utc::now().timestamp() + (schedule.every_ms + 999) / 1_000;
                    let first_wake =
                        crate::clock::stamp(first_wake).unwrap_or_else(|| first_wake.to_string());
                    format!(
                        "Would schedule wake every {}: {} — first wake {}",
                        schedule.every, schedule.reason, first_wake
                    )
                } else {
                    let created = crate::orchestrator::wakes::create_wake_schedule(
                        orch.db.for_project(project).await.as_ref(),
                        &thread_id,
                        schedule.every_ms,
                        &schedule.reason,
                        created_by,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    let first_wake = crate::clock::stamp(created.anchor_at)
                        .unwrap_or_else(|| created.anchor_at.to_string());
                    format!(
                        "Scheduled wake every {}: {} — first wake {} (id {})",
                        schedule.every, schedule.reason, first_wake, created.id
                    )
                }
            } else if let Some(value) = payload.get("subscribe") {
                let filter = normalize_posts_filter(
                    index,
                    item,
                    parse_wake_filter(index, item, value, "subscribe")?,
                )?;
                if filter.kind == "terminal" {
                    subscribe_terminal_wake(
                        orch, index, item, &job_id, &filter, created_by, dry_run, project, *number,
                        *exec_seq, node_id,
                    )
                    .await?
                } else if filter.kind == "checks" {
                    subscribe_checks_wake(
                        orch, index, item, &job_id, &filter, created_by, dry_run, project, *number,
                        *exec_seq, node_id,
                    )
                    .await?
                } else if filter.kind == "rebuild" {
                    subscribe_rebuild_wake(
                        orch, index, item, &job_id, &filter, created_by, dry_run, project,
                    )
                    .await?
                } else if dry_run {
                    format!(
                        "Would subscribe wake: {} {:?}",
                        filter.kind, filter.reference
                    )
                } else {
                    let sub = crate::orchestrator::wakes::subscribe(
                        orch.db.for_project(project).await.as_ref(),
                        &job_id,
                        &filter.kind,
                        filter.reference.as_deref(),
                        filter.fact_kinds.as_deref(),
                        created_by,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    format!(
                        "Subscribed wake {} {}",
                        sub.source_kind,
                        sub.source_ref.unwrap_or_else(|| "*".to_string())
                    )
                }
            } else if let Some(value) = payload.get("mute") {
                if let Some((kind, wake_id)) =
                    parse_home_wake_reference(index, item, value, "mute")?
                {
                    let thread_id = crate::orchestrator::wakes::thread_home_for_job(
                        orch.db.for_project(project).await.as_ref(),
                        &job_id,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    if dry_run {
                        format!("Would mute {} {wake_id}", kind.label())
                    } else {
                        let db = orch.db.for_project(project).await;
                        let changed = match kind {
                            HomeWakeKind::Schedule => {
                                crate::orchestrator::wakes::mute_wake_schedule(
                                    db.as_ref(),
                                    &thread_id,
                                    &wake_id,
                                )
                                .await
                            }
                            HomeWakeKind::Condition => {
                                crate::orchestrator::wakes::conditions::mute_wake_condition(
                                    db.as_ref(),
                                    &thread_id,
                                    &wake_id,
                                )
                                .await
                            }
                        }
                        .map_err(|error| build_failure(index, item, error))?;
                        format!("Muted {} wake {}(s)", usize::from(changed), kind.label())
                    }
                } else {
                    let filter = normalize_checks_filter(
                        orch,
                        index,
                        item,
                        normalize_posts_filter(
                            index,
                            item,
                            normalize_rebuild_filter(
                                index,
                                item,
                                parse_wake_filter(index, item, value, "mute")?,
                                project,
                            )?,
                        )?,
                        project,
                        *number,
                        *exec_seq,
                        node_id,
                    )
                    .await?;
                    let until = payload
                        .get("until")
                        .map(|value| {
                            normalize_posts_filter(
                                index,
                                item,
                                parse_wake_filter(index, item, value, "until")?,
                            )
                        })
                        .transpose()?;
                    let until = match until {
                        Some(filter) => Some(
                            normalize_checks_filter(
                                orch, index, item, filter, project, *number, *exec_seq, node_id,
                            )
                            .await?,
                        ),
                        None => None,
                    };
                    if dry_run {
                        format!("Would mute wake: {} {:?}", filter.kind, filter.reference)
                    } else {
                        let sub = crate::orchestrator::wakes::mute(
                            orch.db.for_project(project).await.as_ref(),
                            &job_id,
                            &filter.kind,
                            filter.reference.as_deref(),
                            filter.fact_kinds.as_deref(),
                            until.as_ref().map(|until| until.kind.as_str()),
                            until.as_ref().and_then(|until| until.reference.as_deref()),
                            created_by,
                        )
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                        format!(
                            "Muted wake {} {}",
                            sub.source_kind,
                            sub.source_ref.unwrap_or_else(|| "*".to_string())
                        )
                    }
                }
            } else {
                return Err(build_failure(
                    index,
                    item,
                    "payload.condition, payload.schedule, payload.subscribe, or payload.mute is required",
                ));
            }
        }
        (
            CairnResource::NodeWakes {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "wakes patch requires payload"))?;
            let value = payload
                .get("unmute")
                .ok_or_else(|| build_failure(index, item, "payload.unmute is required"))?;
            let job_id = crate::resources::node::resolve_node_or_task_job_id(
                orch.db.for_project(project).await.as_ref(),
                project,
                *number,
                *exec_seq,
                node_id,
                None,
            )
            .await
            .map_err(|error| build_failure(index, item, error))?;
            if let Some((kind, wake_id)) = parse_home_wake_reference(index, item, value, "unmute")?
            {
                let thread_id = crate::orchestrator::wakes::thread_home_for_job(
                    orch.db.for_project(project).await.as_ref(),
                    &job_id,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                if dry_run {
                    format!("Would unmute {} {wake_id}", kind.label())
                } else {
                    let db = orch.db.for_project(project).await;
                    let changed = match kind {
                        HomeWakeKind::Schedule => {
                            crate::orchestrator::wakes::unmute_wake_schedule(
                                db.as_ref(),
                                &thread_id,
                                &wake_id,
                            )
                            .await
                        }
                        HomeWakeKind::Condition => {
                            crate::orchestrator::wakes::conditions::unmute_wake_condition(
                                db.as_ref(),
                                &thread_id,
                                &wake_id,
                            )
                            .await
                        }
                    }
                    .map_err(|error| build_failure(index, item, error))?;
                    if matches!(kind, HomeWakeKind::Condition) && changed {
                        crate::orchestrator::wakes::conditions::evaluate_thread_conditions(
                            orch, &thread_id,
                        )
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    }
                    format!("Unmuted {} wake {}(s)", usize::from(changed), kind.label())
                }
            } else {
                let filter = normalize_checks_filter(
                    orch,
                    index,
                    item,
                    normalize_posts_filter(
                        index,
                        item,
                        normalize_rebuild_filter(
                            index,
                            item,
                            parse_wake_filter(index, item, value, "unmute")?,
                            project,
                        )?,
                    )?,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                )
                .await?;
                if dry_run {
                    format!("Would unmute wake: {} {:?}", filter.kind, filter.reference)
                } else {
                    let count = crate::orchestrator::wakes::unmute_matching(
                        orch.db.for_project(project).await.as_ref(),
                        &job_id,
                        &filter.kind,
                        filter.reference.as_deref(),
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    format!("Unmuted {count} wake subscription(s)")
                }
            }
        }
        (
            CairnResource::NodeWakes {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Delete,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "wakes delete requires payload"))?;
            let value = payload
                .get("unsubscribe")
                .ok_or_else(|| build_failure(index, item, "payload.unsubscribe is required"))?;
            let created_by = if request.run_id.is_some() {
                "agent"
            } else {
                "user"
            };
            let job_id = crate::resources::node::resolve_node_or_task_job_id(
                orch.db.for_project(project).await.as_ref(),
                project,
                *number,
                *exec_seq,
                node_id,
                None,
            )
            .await
            .map_err(|error| build_failure(index, item, error))?;
            if let Some((kind, wake_id)) =
                parse_home_wake_reference(index, item, value, "unsubscribe")?
            {
                let thread_id = crate::orchestrator::wakes::thread_home_for_job(
                    orch.db.for_project(project).await.as_ref(),
                    &job_id,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                if dry_run {
                    format!("Would remove {} {wake_id}", kind.label())
                } else {
                    let db = orch.db.for_project(project).await;
                    let changed = match kind {
                        HomeWakeKind::Schedule => {
                            crate::orchestrator::wakes::delete_wake_schedule(
                                db.as_ref(),
                                &thread_id,
                                &wake_id,
                            )
                            .await
                        }
                        HomeWakeKind::Condition => {
                            crate::orchestrator::wakes::conditions::delete_wake_condition(
                                db.as_ref(),
                                &thread_id,
                                &wake_id,
                            )
                            .await
                        }
                    }
                    .map_err(|error| build_failure(index, item, error))?;
                    format!("Removed {} wake {}(s)", usize::from(changed), kind.label())
                }
            } else {
                let filter = normalize_posts_filter(
                    index,
                    item,
                    normalize_rebuild_filter(
                        index,
                        item,
                        parse_wake_filter(index, item, value, "unsubscribe")?,
                        project,
                    )?,
                )?;
                if dry_run {
                    format!(
                        "Would unsubscribe wake: {} {:?}",
                        filter.kind, filter.reference
                    )
                } else {
                    let count = crate::orchestrator::wakes::unsubscribe_matching(
                        orch.db.for_project(project).await.as_ref(),
                        &job_id,
                        &filter.kind,
                        filter.reference.as_deref(),
                        created_by,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    format!("Unsubscribed {count} wake subscription(s)")
                }
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

#[cfg(test)]
pub(super) fn parse_schedule_reference(
    index: usize,
    item: &ChangeItem,
    value: &serde_json::Value,
    field: &str,
) -> ResourceMutationResult<Option<String>> {
    Ok(parse_home_wake_reference(index, item, value, field)?
        .and_then(|(kind, id)| matches!(kind, HomeWakeKind::Schedule).then_some(id)))
}

#[allow(clippy::too_many_arguments)]
async fn normalize_checks_filter(
    orch: &Orchestrator,
    index: usize,
    item: &ChangeItem,
    mut filter: WakeFilterPayload,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
) -> ResourceMutationResult<WakeFilterPayload> {
    if filter.kind != "checks" {
        return Ok(filter);
    }
    if filter.suite.is_some() {
        return Err(build_failure(
            index,
            item,
            "a checks wake watches a whole node and does not accept suite",
        ));
    }
    let home = cairn_uri::build_node_uri(project, number, exec_seq, node_id);
    let reference = filter.reference.as_deref().unwrap_or("cairn:~/checks");
    let target =
        crate::execution::checks_settlement::resolve_checks_target(orch, reference, Some(&home))
            .await
            .map_err(|error| build_failure(index, item, error))?;
    filter.kind = "condition".to_string();
    filter.reference = Some(target.uri);
    Ok(filter)
}

/// Compile the agent-facing `kind:"posts"` watch to the fact clause it is:
/// `{fact: post_created}`, persisted as a `fact` subscription on that clause's
/// canonical reference.
///
/// A Posts watch is then an ordinary consumer of the fact log, matched by the
/// index lane with the one jurisdiction rule every consumer gets (a workspace
/// post reaches any home, a project post only homes in its project), so there
/// is no second scope rule for posts. Subscribe, mute, unmute, and unsubscribe
/// all compile to the same kind, reference, and fact kinds, so they address one
/// row. A `ref` is refused: the watch covers the whole corpus, and accepting a
/// caller-supplied reference would either do nothing or look like a way to name
/// a destination the subscription does not govern. Comments are not feed
/// events, so a fact kind other than `post_created` is refused too.
pub(super) fn normalize_posts_filter(
    index: usize,
    item: &ChangeItem,
    mut filter: WakeFilterPayload,
) -> ResourceMutationResult<WakeFilterPayload> {
    use crate::orchestrator::wakes::{fact_clause_ref, POST_CREATED, SOURCE_KIND_FACT};
    if !matches!(filter.kind.as_str(), "posts" | "post") {
        return Ok(filter);
    }
    if filter.reference.is_some() {
        return Err(build_failure(
            index,
            item,
            "a posts wake watches the whole Posts corpus and does not accept ref",
        ));
    }
    if filter
        .fact_kinds
        .as_deref()
        .is_some_and(|kinds| kinds.iter().any(|kind| kind != POST_CREATED))
    {
        return Err(build_failure(
            index,
            item,
            "a posts wake watches new posts (post_created) only; a comment reaches its post's author and whoever it names",
        ));
    }
    let clause = std::collections::BTreeMap::from([(
        "fact".to_string(),
        serde_json::Value::String(POST_CREATED.to_string()),
    )]);
    filter.kind = SOURCE_KIND_FACT.to_string();
    filter.reference =
        Some(fact_clause_ref(&clause).map_err(|error| build_failure(index, item, error))?);
    filter.fact_kinds = Some(vec![POST_CREATED.to_string()]);
    Ok(filter)
}

/// Compile the agent-facing `kind:"rebuild"` watch to the `fact` subscription
/// it is stored as, `{fact: runner_booted, contains, project}`, so mute,
/// unmute, and unsubscribe address the row subscribe wrote. The target rides in
/// `contains`; a `ref` is refused rather than read as one.
fn normalize_rebuild_filter(
    index: usize,
    item: &ChangeItem,
    mut filter: WakeFilterPayload,
    project: &str,
) -> ResourceMutationResult<WakeFilterPayload> {
    if filter.kind != "rebuild" {
        return Ok(filter);
    }
    let clause = rebuild_clause(index, item, &filter, project)?;
    filter.kind = crate::orchestrator::wakes::SOURCE_KIND_FACT.to_string();
    filter.reference = Some(
        clause
            .clause_ref()
            .map_err(|error| build_failure(index, item, error))?,
    );
    filter.fact_kinds = Some(vec![crate::orchestrator::wakes::RUNNER_BOOTED.to_string()]);
    Ok(filter)
}

fn rebuild_clause(
    index: usize,
    item: &ChangeItem,
    filter: &WakeFilterPayload,
    project: &str,
) -> ResourceMutationResult<crate::orchestrator::wakes::RebuildClause> {
    if filter.reference.is_some() {
        return Err(build_failure(
            index,
            item,
            "a rebuild wake names its target with contains, not ref: {subscribe:{kind:\"rebuild\",contains:\"<40-hex commit> | cairn://p/<project>/<issue>\"}}",
        ));
    }
    let raw = filter.contains.as_deref().ok_or_else(|| {
        build_failure(
            index,
            item,
            "a rebuild wake requires contains: a full 40-character commit id or an issue URI (cairn://p/<project>/<issue>)",
        )
    })?;
    let target = crate::orchestrator::wakes::RebuildTarget::parse(raw)
        .map_err(|error| build_failure(index, item, error))?;
    Ok(crate::orchestrator::wakes::RebuildClause::new(
        target, project,
    ))
}

/// Subscribe a one-shot `kind:"rebuild"` wake: resume this node at the first
/// runner boot whose source commit contains the target, or now when the
/// current boot already does.
#[allow(clippy::too_many_arguments)]
async fn subscribe_rebuild_wake(
    orch: &Orchestrator,
    index: usize,
    item: &ChangeItem,
    job_id: &str,
    filter: &WakeFilterPayload,
    created_by: &str,
    dry_run: bool,
    project: &str,
) -> ResourceMutationResult<String> {
    let clause = rebuild_clause(index, item, filter, project)?;
    if dry_run {
        return Ok(format!(
            "Would subscribe rebuild wake: fires once when the runner boots with a build containing {}",
            clause.target
        ));
    }
    let outcome = crate::orchestrator::wakes::subscribe_rebuild_once(
        orch,
        job_id,
        &clause,
        created_by,
        crate::orchestrator::wakes::current_boot(),
        &crate::orchestrator::wakes::StoreContainment::new(orch),
    )
    .await
    .map_err(|error| build_failure(index, item, error))?;
    Ok(outcome.describe(&clause))
}

#[derive(Clone, Debug)]
pub(super) struct WakeFilterPayload {
    pub(super) kind: String,
    pub(super) reference: Option<String>,
    pub(super) fact_kinds: Option<Vec<String>>,
    /// Terminal subscriptions only: `"exit"` (default) or `"output"`.
    pub(super) on: Option<String>,
    /// Terminal `on:"output"` subscriptions only: the literal phrase to watch for.
    pub(super) phrase: Option<String>,
    /// Only ever set to be REFUSED: a checks wake watches a whole node, and
    /// accepting a suite silently would subscribe to something other than what
    /// was asked for.
    pub(super) suite: Option<String>,
    /// Rebuild subscriptions only: the commit or issue a boot must contain.
    pub(super) contains: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ScheduleCreatePayload {
    pub(super) every: String,
    pub(super) every_ms: i64,
    pub(super) reason: String,
}

pub(super) fn parse_schedule_create(
    index: usize,
    item: &ChangeItem,
    value: &serde_json::Value,
) -> ResourceMutationResult<ScheduleCreatePayload> {
    let obj = value
        .as_object()
        .ok_or_else(|| build_failure(index, item, "payload.schedule must be an object"))?;
    let every = obj
        .get("every")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| build_failure(index, item, "payload.schedule.every is required"))?;
    let every_ms = crate::duration::parse_duration_ms(every)
        .and_then(|value| i64::try_from(value).map_err(|_| "duration too large".to_string()))
        .map_err(|error| build_failure(index, item, format!("invalid schedule every: {error}")))?;
    let reason = obj
        .get("reason")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| build_failure(index, item, "payload.schedule.reason is required"))?
        .trim()
        .to_string();
    Ok(ScheduleCreatePayload {
        every: every.to_string(),
        every_ms,
        reason,
    })
}

#[derive(Clone, Copy)]
enum HomeWakeKind {
    Schedule,
    Condition,
}

impl HomeWakeKind {
    fn label(self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::Condition => "condition",
        }
    }
}

struct ConditionCreate {
    sql: String,
    reason: String,
    once: bool,
}

fn parse_condition_create(
    index: usize,
    item: &ChangeItem,
    value: &serde_json::Value,
) -> ResourceMutationResult<ConditionCreate> {
    let obj = value
        .as_object()
        .ok_or_else(|| build_failure(index, item, "payload.condition must be an object"))?;
    let sql = obj
        .get("sql")
        .and_then(serde_json::Value::as_str)
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| build_failure(index, item, "payload.condition.sql is required"))?;
    crate::resources::sql_policy::refuse_retired_rulings(sql)
        .map_err(|refusal| build_failure(index, item, refusal))?;
    crate::resources::sql_policy::validate_read_only_sql(sql).map_err(|error| {
        build_failure(
            index,
            item,
            format!("{error}; write data through cairn:~/db, then query it from the condition"),
        )
    })?;
    let reason = obj
        .get("reason")
        .and_then(serde_json::Value::as_str)
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| build_failure(index, item, "payload.condition.reason is required"))?;
    let once = match obj.get("once") {
        Some(value) => value.as_bool().ok_or_else(|| {
            build_failure(index, item, "payload.condition.once must be a boolean")
        })?,
        None => false,
    };
    Ok(ConditionCreate {
        sql: sql.trim().into(),
        reason: reason.trim().into(),
        once,
    })
}

fn parse_home_wake_reference(
    index: usize,
    item: &ChangeItem,
    value: &serde_json::Value,
    field: &str,
) -> ResourceMutationResult<Option<(HomeWakeKind, String)>> {
    let Some(obj) = value.as_object() else {
        return Ok(None);
    };
    let kind = match obj.get("kind").and_then(serde_json::Value::as_str) {
        Some("schedule") => HomeWakeKind::Schedule,
        Some("condition") => HomeWakeKind::Condition,
        _ => return Ok(None),
    };
    let id = obj
        .get("ref")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            build_failure(
                index,
                item,
                format!("payload.{field}.ref is required for kind {}", kind.label()),
            )
        })?;
    Ok(Some((kind, id.trim().to_string())))
}

pub(super) fn parse_wake_filter(
    index: usize,
    item: &ChangeItem,
    value: &serde_json::Value,
    field: &str,
) -> ResourceMutationResult<WakeFilterPayload> {
    let obj = value
        .as_object()
        .ok_or_else(|| build_failure(index, item, format!("payload.{field} must be an object")))?;
    let kind = obj
        .get("kind")
        .or_else(|| obj.get("sourceKind"))
        .or_else(|| obj.get("source_kind"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| build_failure(index, item, format!("payload.{field}.kind is required")))?
        .to_string();
    let reference = obj
        .get("ref")
        .or_else(|| obj.get("sourceRef"))
        .or_else(|| obj.get("source_ref"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let fact_kinds = obj
        .get("factKinds")
        .or_else(|| obj.get("fact_kinds"))
        .or_else(|| obj.get("kinds"))
        .map(|value| {
            let values = value.as_array().ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("payload.{field}.factKinds must be an array"),
                )
            })?;
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .filter(|value| !value.is_empty())
                        .map(ToString::to_string)
                        .ok_or_else(|| {
                            build_failure(
                                index,
                                item,
                                format!("payload.{field}.factKinds entries must be strings"),
                            )
                        })
                })
                .collect::<ResourceMutationResult<Vec<_>>>()
        })
        .transpose()?;
    let on = obj
        .get("on")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let phrase = obj
        .get("phrase")
        .and_then(|value| value.as_str())
        .map(ToString::to_string);
    let suite = obj
        .get("suite")
        .and_then(|value| value.as_str())
        .map(ToString::to_string);
    let contains = obj
        .get("contains")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .map(ToString::to_string);
    Ok(WakeFilterPayload {
        kind,
        reference,
        fact_kinds,
        on,
        phrase,
        suite,
        contains,
    })
}

/// Subscribe an agent-facing `kind:"checks"` wake: resume this node when the
/// referenced node's project check lanes stop moving.
///
/// The elective, turn-boundary half of `run.waitFor {kind:"checks"}`. It is the
/// only one of the two that can watch the subscriber's OWN node, and that is the
/// point: a node's turn-end wave is armed by its turn ENDING, so "wake me when
/// my own lanes settle" is expressible here and structurally impossible as an
/// inline wait.
#[allow(clippy::too_many_arguments)]
async fn subscribe_checks_wake(
    orch: &Orchestrator,
    index: usize,
    item: &ChangeItem,
    job_id: &str,
    filter: &WakeFilterPayload,
    created_by: &str,
    dry_run: bool,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
) -> ResourceMutationResult<String> {
    let filter = normalize_checks_filter(
        orch,
        index,
        item,
        filter.clone(),
        project,
        number,
        exec_seq,
        node_id,
    )
    .await?;
    let reference = filter.reference.as_deref().expect("checks alias has a URI");
    let target = crate::execution::checks_settlement::resolve_checks_target(orch, reference, None)
        .await
        .map_err(|error| build_failure(index, item, error))?;

    if dry_run {
        return Ok(format!("Would subscribe checks wake: {}", target.uri));
    }
    use crate::orchestrator::wakes::ChecksSubscribeOutcome;
    match crate::orchestrator::wakes::subscribe_checks_settled_once(
        orch,
        job_id,
        &target.job_id,
        &target.label,
        &target.uri,
        created_by,
    )
    .await
    .map_err(|error| build_failure(index, item, error))?
    {
        ChecksSubscribeOutcome::AlreadySettled => Ok(format!(
            "Checks on '{}' have already settled; resume queued for turn end ({})",
            target.label, target.uri
        )),
        ChecksSubscribeOutcome::ConfirmingVerdictless => Ok(format!(
            "Subscribed to '{}' checks settling. Its lanes currently hold no verdicts, which also describes the moment before a wave starts, so that is being confirmed before you are woken — end your turn either way ({})",
            target.label, target.uri
        )),
        ChecksSubscribeOutcome::Subscribed => Ok(format!(
            "Subscribed to '{}' checks settling; end your turn to resume when its lanes stop moving ({})",
            target.label, target.uri
        )),
    }
}

/// Subscribe an agent-facing `kind:"terminal"` wake. Normalizes to the internal
/// `process` source with a `terminal_exit` fact kind and one-shot semantics,
/// validates the terminal exists in the scope its ref names (see
/// `mcp::handlers::terminal_ref`), and fires immediately when the terminal has
/// already exited.
#[allow(clippy::too_many_arguments)]
async fn subscribe_terminal_wake(
    orch: &Orchestrator,
    index: usize,
    item: &ChangeItem,
    job_id: &str,
    filter: &WakeFilterPayload,
    created_by: &str,
    dry_run: bool,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
) -> ResourceMutationResult<String> {
    use crate::mcp::handlers::terminal_ref::{
        resolve_terminal_ref, terminal_not_found, TerminalCaller, TERMINAL_REF_FORMS,
    };

    let reference = filter.reference.as_deref().ok_or_else(|| {
        build_failure(
            index,
            item,
            format!("terminal subscribe requires ref: {TERMINAL_REF_FORMS}"),
        )
    })?;
    let db = orch.db.for_project(project).await;
    let home = cairn_uri::build_node_uri(project, number, exec_seq, node_id);
    let caller = TerminalCaller {
        job_id,
        home: &home,
        project_key: project,
    };
    let target = resolve_terminal_ref(db.as_ref(), &caller, reference)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    let row = crate::mcp::handlers::terminal::lookup_terminal_for_wake(
        db.as_ref(),
        &target.owner,
        &target.slug,
    )
    .await
    .map_err(|error| build_failure(index, item, error.to_string()))?;
    let Some(row) = row else {
        return Err(build_failure(
            index,
            item,
            terminal_not_found(db.as_ref(), &caller, &target).await,
        ));
    };
    let (slug, uri) = (target.slug, target.uri);

    match filter.on.as_deref().unwrap_or("exit") {
        "exit" => {
            subscribe_terminal_exit_wake(
                orch, index, item, job_id, created_by, dry_run, &slug, &uri, &row,
            )
            .await
        }
        "output" => {
            subscribe_terminal_output_wake(
                orch, index, item, job_id, filter, created_by, dry_run, &slug, &uri, &row,
            )
            .await
        }
        other => Err(build_failure(
            index,
            item,
            format!("terminal subscribe `on` must be \"exit\" or \"output\" (got \"{other}\")"),
        )),
    }
}

/// Subscribe a one-shot terminal-**exit** wake: resume the node when the terminal
/// finishes (fires immediately if it already exited).
#[allow(clippy::too_many_arguments)]
async fn subscribe_terminal_exit_wake(
    orch: &Orchestrator,
    index: usize,
    item: &ChangeItem,
    job_id: &str,
    created_by: &str,
    dry_run: bool,
    slug: &str,
    uri: &str,
    row: &crate::mcp::handlers::terminal::TerminalWakeRow,
) -> ResourceMutationResult<String> {
    if dry_run {
        return Ok(format!("Would subscribe terminal-exit wake: {slug}"));
    }

    match crate::mcp::handlers::terminal::subscribe_terminal_exit_wake_once(
        orch,
        job_id,
        slug,
        uri,
        Some(row),
        row.session_id.as_deref(),
        created_by,
    )
    .await
    .map_err(|error| build_failure(index, item, error))?
    {
        crate::mcp::handlers::terminal::TerminalWakeSubscriptionOutcome::ExitAlreadyQueued => Ok(
            format!("Terminal '{slug}' already exited; resume queued for turn end ({uri})"),
        ),
        _ => Ok(format!(
            "Subscribed to terminal '{slug}' exit; end your turn to resume when it finishes ({uri})"
        )),
    }
}

/// Subscribe a one-shot terminal-**output** phrase wake: resume the node when a
/// literal phrase appears in the running terminal's output. The subscription
/// also fires on terminal exit, so a process that dies before printing the
/// phrase still wakes the waiting agent. Only meaningful on a running terminal.
#[allow(clippy::too_many_arguments)]
async fn subscribe_terminal_output_wake(
    orch: &Orchestrator,
    index: usize,
    item: &ChangeItem,
    job_id: &str,
    filter: &WakeFilterPayload,
    created_by: &str,
    dry_run: bool,
    slug: &str,
    uri: &str,
    row: &crate::mcp::handlers::terminal::TerminalWakeRow,
) -> ResourceMutationResult<String> {
    let phrase = filter
        .phrase
        .as_deref()
        .map(str::trim)
        .filter(|phrase| !phrase.is_empty())
        .ok_or_else(|| {
            build_failure(
                index,
                item,
                "terminal output subscribe requires a non-empty phrase, e.g. {subscribe:{kind:\"terminal\",ref:\"cairn:~/terminal/dev\",on:\"output\",phrase:\"ready\"}}",
            )
        })?;

    if dry_run {
        return Ok(format!(
            "Would subscribe terminal-output wake on '{slug}' for phrase \"{phrase}\""
        ));
    }

    match crate::mcp::handlers::terminal::subscribe_terminal_output_wake_once(
        orch,
        job_id,
        slug,
        uri,
        phrase,
        Some(row),
        row.session_id.as_deref(),
        created_by,
    )
    .await
    .map_err(|error| build_failure(index, item, error))?
    {
        crate::mcp::handlers::terminal::TerminalWakeSubscriptionOutcome::OutputAlreadyQueued => {
            Ok(format!(
                "Terminal '{slug}' already printed \"{phrase}\"; resume queued for turn end ({uri})"
            ))
        }
        crate::mcp::handlers::terminal::TerminalWakeSubscriptionOutcome::OutputRegistered => {
            Ok(format!(
                "Subscribed to terminal '{slug}' output for phrase \"{phrase}\"; end your turn to resume when it appears ({uri})"
            ))
        }
        crate::mcp::handlers::terminal::TerminalWakeSubscriptionOutcome::ExitAlreadyQueued => Ok(
            format!(
                "Terminal '{slug}' already exited before \"{phrase}\" appeared; resume queued for turn end ({uri})"
            ),
        ),
        _ => Ok(format!(
            "Subscribed to terminal '{slug}' output for phrase \"{phrase}\"; end your turn to resume when it next prints, across restarts ({uri})"
        )),
    }
}
