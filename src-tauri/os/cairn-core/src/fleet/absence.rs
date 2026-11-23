//! Durable absence incidents. Detection is evidence about a runner link, never
//! proof that a host is off and never authority to settle its work.
//!
//! One hour allows over nine times the slowest observed sub-hour first attach
//! in 212 samples across 27 runner starts (398,077 ms). Nonreturns are excluded.
//! The journal is retained for only fourteen days; this projection keeps the
//! last facts independently.

use std::collections::HashMap;

use cairn_db::turso::params;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::mcp::handlers::run::detached;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};

const HOUR: u64 = 3_600_000;
const DAY: u64 = 24 * HOUR;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Fact {
    at_ms: u64,
    source: String,
    value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Episode {
    key: String,
    since_ms: u64,
    start_known: bool,
    opened: bool,
    day_announced: bool,
    credential_announced: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LinkIdentity {
    incarnation: String,
    generation: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct State {
    name: String,
    cursor: String,
    episode: Option<Episode>,
    last_attach: Option<Fact>,
    last_detach: Option<Fact>,
    link_path: Option<Fact>,
    build: Option<Fact>,
    update: Option<Fact>,
    lane: Option<Fact>,
    credential_expiry_ms: Option<u64>,
    blocked: Vec<String>,
    observed_ms: u64,
    // A current attachment is stronger than journal edges that arrive late.
    // Stamp while holding the connection-table lock, not at sweep start.
    #[serde(default)]
    attached_observed_ms: u64,
    #[serde(default)]
    attached_link: Option<LinkIdentity>,
    #[serde(default)]
    detached_link: Option<LinkIdentity>,
    cleared: Option<String>,
}

#[derive(Debug)]
struct Event {
    link: Option<LinkIdentity>,
    id: String,
    kind: String,
    at_ms: u64,
    cause: Option<String>,
    detail: Value,
}

#[derive(Clone, Debug)]
struct Notice {
    key: String,
    content: String,
}

fn at(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms.min(i64::MAX as u64) as i64)
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| ms.to_string())
}

fn duration(ms: u64) -> String {
    super::compact_duration(ms)
}

fn journal_path(value: &Value) -> String {
    let executor = value
        .get("executor")
        .and_then(Value::as_str)
        .unwrap_or("unknown executor address");
    let runner = value
        .get("runner")
        .and_then(Value::as_str)
        .unwrap_or("unknown runner address");
    let route = match value.get("tunnel").and_then(Value::as_bool) {
        Some(true) => "tunnel",
        Some(false) => "direct; no tunnel",
        None => "tunnel status unknown",
    };
    format!("{executor} -> {runner} ({route})")
}

fn new_episode(key: String, since_ms: u64, start_known: bool) -> Episode {
    Episode {
        key,
        since_ms,
        start_known,
        opened: false,
        day_announced: false,
        credential_announced: false,
    }
}

impl State {
    fn notice(&self, id: &str, step: &str, content: String) -> Notice {
        Notice {
            key: format!(
                "executor-absence:{id}:{}:{step}",
                self.episode.as_ref().expect("episode").key
            ),
            content,
        }
    }

    fn clear(&mut self, id: &str, now: u64, notices: &mut Vec<Notice>) {
        if let Some(episode) = &self.episode {
            let text = format!(
                "**{}** absence cleared at {} after {}{}; returned build: {}; link: {}.",
                self.name,
                at(now),
                duration(now.saturating_sub(episode.since_ms)),
                if episode.start_known {
                    ""
                } else {
                    " since first observed absent (actual start unknown)"
                },
                self.build
                    .as_ref()
                    .map(|f| f.value.as_str())
                    .unwrap_or("unknown"),
                self.link_path
                    .as_ref()
                    .map(|f| f.value.as_str())
                    .unwrap_or("unknown"),
            );
            if episode.opened {
                notices.push(self.notice(id, "clear", text.clone()));
                self.cleared = Some(text);
            }
        }
        self.episode = None;
    }

    fn event(&mut self, id: &str, event: Event, notices: &mut Vec<Notice>) {
        let fact = |value: String| Fact {
            at_ms: event.at_ms,
            source: format!("runner_events/{}", event.id),
            value,
        };
        match event.kind.as_str() {
            "executor_attached" => {
                if self
                    .last_attach
                    .as_ref()
                    .is_none_or(|old| event.at_ms >= old.at_ms)
                {
                    self.last_attach = Some(fact("attached".into()));
                }
                if let Some(report) = event.detail.get("last_update").and_then(|value| {
                    serde_json::from_value::<cairn_common::executor_protocol::UpdateReport>(
                        value.clone(),
                    )
                    .ok()
                }) {
                    if self
                        .update
                        .as_ref()
                        .is_none_or(|old| report.at_unix_ms >= old.at_ms)
                    {
                        self.update = Some(Fact {
                            at_ms: report.at_unix_ms,
                            source: format!("runner_events/{} Hello.last_update", event.id),
                            value: serde_json::to_string(&report)
                                .expect("update report serializes"),
                        });
                    }
                }
                if event.at_ms > self.attached_observed_ms
                    && self
                        .last_attach
                        .as_ref()
                        .is_none_or(|attach| event.at_ms >= attach.at_ms)
                {
                    // Missing facts on a newer generation are unknown, not the
                    // previous generation's build or route.
                    self.build = event
                        .detail
                        .get("executor_build_id")
                        .and_then(Value::as_str)
                        .map(|v| fact(v.into()));
                    self.link_path = event
                        .detail
                        .get("link_path")
                        .filter(|v| !v.is_null())
                        .map(|v| fact(journal_path(v)));
                    if self
                        .episode
                        .as_ref()
                        .is_none_or(|episode| event.at_ms >= episode.since_ms)
                    {
                        self.clear(id, event.at_ms, notices);
                    }
                }
            }
            "executor_detached" => {
                if self
                    .last_detach
                    .as_ref()
                    .is_none_or(|old| event.at_ms >= old.at_ms)
                {
                    self.detached_link = event.link.clone();
                    self.last_detach = Some(fact(
                        event
                            .cause
                            .unwrap_or_else(|| "detach cause not recorded".into()),
                    ));
                }
                // Transport records detach before waiting for ingest teardown.
                // Presence of that SAME generation is not evidence of recovery.
                let same_observed_link = event.link.is_some() && event.link == self.attached_link;
                let superseded_by_attach = self
                    .last_attach
                    .as_ref()
                    .is_some_and(|attach| attach.at_ms > event.at_ms);
                if !superseded_by_attach
                    && (event.at_ms > self.attached_observed_ms || same_observed_link)
                {
                    if let Some(episode) = &mut self.episode {
                        // The writer can lag the first absent observation.
                        if !episode.start_known {
                            episode.since_ms = event.at_ms;
                            episode.start_known = true;
                        }
                    } else {
                        self.episode = Some(new_episode(event.id.clone(), event.at_ms, true));
                    }
                }
            }
            "executor_self_update"
                if self
                    .update
                    .as_ref()
                    .is_none_or(|old| event.at_ms >= old.at_ms) =>
            {
                self.update = Some(fact(event.detail.to_string()));
            }
            _ => {}
        }
        self.cursor = event.id;
    }

    fn observe(&mut self, id: &str, absent: bool, now: u64, notices: &mut Vec<Notice>) {
        self.observed_ms = now;
        if !absent {
            self.attached_observed_ms = self.attached_observed_ms.max(now);
            self.clear(id, now, notices);
            return;
        }
        if self.episode.is_none() {
            self.episode = Some(new_episode(uuid::Uuid::now_v7().to_string(), now, false));
        }
        let episode = self.episode.as_mut().expect("created above");
        let age = now.saturating_sub(episode.since_ms);
        let credential_due = self
            .credential_expiry_ms
            .is_some_and(|expiry| expiry <= now.saturating_add(HOUR));
        let mut steps = Vec::new();
        if !episode.opened && age >= HOUR {
            episode.opened = true;
            steps.push(("open", "ABSENCE INCIDENT"));
        }
        if episode.opened && age >= DAY && !episode.day_announced {
            episode.day_announced = true;
            steps.push(("24h", "ABSENCE ESCALATED: 24 hours"));
        }
        if episode.opened && credential_due && !episode.credential_announced {
            episode.credential_announced = true;
            steps.push((
                "credential",
                "ABSENCE ESCALATED: credential in its last hour or expired",
            ));
        }
        for (step, heading) in steps {
            notices.push(self.notice(id, step, format!("{heading}\n{}", self.render(now))));
        }
    }

    fn link_has_detached(&self, link: Option<&LinkIdentity>) -> bool {
        link.is_some() && link == self.detached_link.as_ref()
    }

    fn incident_line(&self, now: u64, current: Option<&LinkIdentity>) -> Option<String> {
        let opened = self.episode.as_ref().is_some_and(|episode| episode.opened);
        (opened && (current.is_none() || self.link_has_detached(current))).then(|| self.render(now))
    }

    fn render(&self, now: u64) -> String {
        let mut text = if let Some(episode) = &self.episode {
            format!(
                "**{}** — {} for {} (since {}{}). Opening bound: 1h; escalation: 24h or credential's last hour.\n",
                self.name,
                if episode.day_announced || episode.credential_announced { "absence escalated" }
                else if episode.opened { "absence incident" } else { "enrolled, not attached; below incident bound" },
                duration(now.saturating_sub(episode.since_ms)), at(episode.since_ms),
                if episode.start_known { "; durable detach" } else { "; first observation, actual start unknown" },
            )
        } else {
            self.cleared.clone().unwrap_or_default()
        };
        if text.is_empty() {
            return text;
        }
        for (label, fact) in [
            ("Last attach", &self.last_attach),
            ("Last detach", &self.last_detach),
            ("Last link path", &self.link_path),
            ("Last executor build", &self.build),
            ("Last self-update", &self.update),
            ("Last lane verdict", &self.lane),
        ] {
            match fact {
                Some(f) => text.push_str(&format!("- {label}: {} at {} ({}).\n", f.value, at(f.at_ms), f.source)),
                None => text.push_str(&format!("- {label}: unknown; no captured durable fact (journal retention or recording gap possible).\n")),
            }
        }
        text.push_str(&match self.credential_expiry_ms {
            Some(expiry) => format!(
                "- Credential expires: {} (runner enrollment record).\n",
                at(expiry)
            ),
            None => "- Credential expiry: unknown; no active runner enrollment record.\n".into(),
        });
        text.push_str(&format!(
            "- Blocked restart-detached batches: {} as of {}.{}\n",
            self.blocked.len(),
            at(self.observed_ms),
            if self.blocked.is_empty() {
                String::new()
            } else {
                format!(" Requests: {}.", self.blocked.join(", "))
            }
        ));
        text
    }
}

async fn load(db: &LocalDb) -> Result<HashMap<String, State>, String> {
    let rows = db
        .query_all(
            "SELECT executor_id,state_json FROM executor_absence_incidents",
            (),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|e| e.to_string())?;
    rows.into_iter()
        .map(|(id, json)| {
            serde_json::from_str(&json)
                .map(|state| (id, state))
                .map_err(|e| format!("decode absence state: {e}"))
        })
        .collect()
}

async fn events(db: &LocalDb, id: &str, cursor: &str) -> Result<Vec<Event>, String> {
    db.query_all(
        "SELECT id,kind,occurred_at_ms,cause,detail_json,runner_incarnation,generation FROM runner_events
         WHERE executor_id=?1 AND id>?2 AND kind IN ('executor_attached','executor_detached','executor_self_update')
         ORDER BY id",
        (id.to_string(), cursor.to_string()),
        |row| Ok(Event {
            link: row.opt_i64(6)?.map(|generation| Ok::<_, DbError>(LinkIdentity {
                incarnation: row.text(5)?, generation: generation.max(0) as u64,
            })).transpose()?,
            id: row.text(0)?, kind: row.text(1)?, at_ms: row.i64(2)?.max(0) as u64,
            cause: row.opt_text(3)?,
            detail: serde_json::from_str(&row.text(4)?).map_err(|e| DbError::Row(e.to_string()))?,
        }),
    ).await.map_err(|e| e.to_string())
}

async fn save(db: &LocalDb, id: &str, state: &State, notices: &[Notice]) -> Result<(), String> {
    let id = id.to_string();
    let json = serde_json::to_string(state).map_err(|e| e.to_string())?;
    let notices = notices.to_vec();
    db.write(move |conn| {
        let (id, json, notices) = (id.clone(), json.clone(), notices.clone());
        Box::pin(async move {
            conn.execute(
                "INSERT INTO executor_absence_incidents(executor_id,state_json) VALUES(?1,?2)
                 ON CONFLICT(executor_id) DO UPDATE SET state_json=excluded.state_json",
                params![id.clone(), json],
            ).await?;
            for notice in notices {
                conn.execute(
                    "INSERT OR IGNORE INTO executor_absence_notices(idempotency_key,executor_id,content)
                     VALUES(?1,?2,?3)", params![notice.key, id.clone(), notice.content],
                ).await?;
            }
            Ok(())
        })
    }).await.map_err(|e| e.to_string())
}

async fn lane_in_db(db: &LocalDb, id: &str) -> Result<Option<Fact>, String> {
    db.query_opt(
        "SELECT id,check_name,verdict,ran_at,commit_sha,failure_kind FROM check_result_observations
             WHERE executor_id=?1 ORDER BY ran_at DESC,id DESC LIMIT 1",
        (id.to_string(),),
        |row| {
            Ok(Fact {
                source: format!("check_result_observations/{}", row.text(0)?),
                at_ms: row.i64(3)?.max(0) as u64,
                value: format!(
                    "{}: {} on {}{}",
                    row.text(1)?,
                    row.text(2)?,
                    row.text(4)?,
                    row.opt_text(5)?
                        .map(|kind| format!(" ({kind})"))
                        .unwrap_or_default()
                ),
            })
        },
    )
    .await
    .map_err(|e| e.to_string())
}

async fn last_lane(orch: &Orchestrator, id: &str) -> Result<Option<Fact>, String> {
    let mut latest: Option<Fact> = None;
    for db in orch.db.all_dbs().await {
        if let Some(fact) = lane_in_db(&db, id).await? {
            if latest.as_ref().is_none_or(|old| fact.at_ms > old.at_ms) {
                latest = Some(fact);
            }
        }
    }
    Ok(latest)
}

async fn blocked(orch: &Orchestrator) -> Result<HashMap<String, Vec<String>>, String> {
    let mut by_name: HashMap<String, Vec<String>> = HashMap::new();
    for db in orch.db.all_dbs().await {
        for batch in detached::all_running(&db).await? {
            if batch.state != detached::DetachedState::Running
                || detached::has_live_resolver(&batch.request_id)
                || detached::batch_is_live(orch, &batch.request_id)
            {
                continue;
            }
            if detached::placed_here(&batch.request_id)
                || crate::mcp::handlers::run::adopted::has_open_adoption(&db, &batch.request_id)
                    .await
                    .map_err(|e| e.to_string())?
            {
                continue;
            }
            let placed = batch.settlement.as_ref().and_then(|s| s.placed_on.as_ref());
            let holder = detached::holder_of(orch, &batch.request_id, placed).await?;
            // A closed runner's never-claimed entry cannot be on any machine.
            if matches!(holder, detached::Holder::NeverClaimed { .. }) {
                continue;
            }
            if let super::RequestReport::Unknown { awaiting } = orch
                .fleet
                .request_report(&batch.request_id, holder.executor_id())
            {
                for name in awaiting {
                    by_name
                        .entry(name)
                        .or_default()
                        .push(format!("{} (job {})", batch.request_id, batch.job_id));
                }
            }
        }
    }
    for requests in by_name.values_mut() {
        requests.sort();
        requests.dedup();
    }
    Ok(by_name)
}

async fn deliver(orch: &Orchestrator) -> Result<(), String> {
    let local = &orch.db.local;
    let pending = local.query_all(
        "SELECT idempotency_key,content FROM executor_absence_notices WHERE delivered_at_ms IS NULL ORDER BY rowid",
        (), |row| Ok((row.text(0)?, row.text(1)?)),
    ).await.map_err(|e| e.to_string())?;
    if pending.is_empty() {
        return Ok(());
    }
    let db = orch.db.for_project("cairn").await;
    let thread = db
        .query_opt(
            "SELECT t.id FROM threads t JOIN projects p ON p.id=t.project_id
         WHERE lower(p.key)='cairn' AND t.name='execution' AND t.status='active'",
            (),
            |row| row.text(0),
        )
        .await
        .map_err(|e| e.to_string())?
        .ok_or("active cairn execution thread not found; notices remain pending")?;
    let job = crate::threads::ensure_thread_session(&db, &thread).await?;
    for (key, content) in pending {
        crate::messages::delivery::queue_system_direct_once_to_job(
            orch,
            &job,
            &content,
            crate::messages::queued::DeliveryUrgency::Steer,
            &key,
        )?;
        local.execute(
            "UPDATE executor_absence_notices SET delivered_at_ms=?2 WHERE idempotency_key=?1 AND delivered_at_ms IS NULL",
            (key, super::unix_time_ms() as i64),
        ).await.map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Called serially by the runner's monitor. All notice transitions are committed
/// with their outbox row before delivery; retries use the same system-direct key.
pub(crate) async fn sweep(orch: &Orchestrator) -> Result<(), String> {
    let now = super::unix_time_ms();
    let local = &orch.db.local;
    let mut states = load(local).await?;
    let away = orch.fleet.unattached_enrolled_remotes();
    let attached = orch.fleet.inspect_executors(now);
    let blocked = blocked(orch).await?;
    let mut machines: HashMap<String, (String, bool)> = away
        .iter()
        .map(|remote| (remote.executor_id.clone(), (remote.name.clone(), true)))
        .collect();
    for machine in &attached {
        let id = &machine.health.identity.executor_id;
        if orch.fleet.is_enrolled_remote(id) {
            machines.insert(id.clone(), (machine.name.clone(), false));
        }
    }
    for (id, (name, _)) in machines {
        let state = states.entry(id.clone()).or_default();
        state.name = name.clone();
        let mut notices = Vec::new();
        for event in events(local, &id, &state.cursor).await? {
            state.event(&id, event, &mut notices);
        }
        if let Some(lane) = last_lane(orch, &id).await? {
            if state
                .lane
                .as_ref()
                .is_none_or(|old| lane.at_ms >= old.at_ms)
            {
                state.lane = Some(lane);
            }
        }
        state.credential_expiry_ms = local
            .query_opt(
                "SELECT expires_at FROM executor_enrollments WHERE device_kind='executor'
             AND enrolled_endpoint_id=?1 AND revoked_at IS NULL",
                (id.clone(),),
                |row| row.i64(0),
            )
            .await
            .map_err(|e| e.to_string())?
            .map(|seconds| (seconds.max(0) as u64).saturating_mul(1000));
        state.blocked = blocked.get(&name).cloned().unwrap_or_default();
        // No I/O between this atomic connection observation and the transition.
        // The early fleet snapshots enumerate identities only.
        let (observed_ms, absent) = {
            let connections = orch.fleet.lock_connections();
            let observed_ms = super::unix_time_ms();
            let present = connections.get(&id);
            if let Some(machine) = present {
                let fact = |value| Fact {
                    at_ms: observed_ms,
                    source: "current executor inspection".into(),
                    value,
                };
                state.build = machine.executor_build_id.clone().map(fact);
                state.link_path = machine.link_path.as_ref().map(|path| {
                    fact(format!(
                        "{} -> {} ({})",
                        path.executor,
                        path.runner,
                        if path.through_tunnel() {
                            "tunnel"
                        } else {
                            "direct; no tunnel"
                        },
                    ))
                });
                if let Some(update) = &machine.last_update {
                    if state
                        .update
                        .as_ref()
                        .is_none_or(|old| update.at_unix_ms >= old.at_ms)
                    {
                        state.update = Some(Fact {
                            at_ms: update.at_unix_ms,
                            source: "executor UpdateReport (current inspection)".into(),
                            value: serde_json::to_string(update).map_err(|e| e.to_string())?,
                        });
                    }
                }
            }
            let link = present.and_then(|machine| {
                orch.fleet
                    .runner_incarnation()
                    .map(|incarnation| LinkIdentity {
                        incarnation: incarnation.to_string(),
                        generation: machine.generation,
                    })
            });
            let detached_generation_still_tearing_down = state.link_has_detached(link.as_ref());
            if present.is_some() {
                state.attached_link = link;
            }
            (
                observed_ms,
                present.is_none() || detached_generation_still_tearing_down,
            )
        };
        state.observe(&id, absent, observed_ms, &mut notices);
        save(local, &id, state, &notices).await?;
    }
    deliver(orch).await
}

/// Read-only projection: resource reads neither advance the clock nor deliver.
pub(crate) async fn incident_lines(orch: &Orchestrator) -> Result<HashMap<String, String>, String> {
    let states = load(&orch.db.local).await?;
    let (now, current) = {
        let connections = orch.fleet.lock_connections();
        let now = super::unix_time_ms();
        let current: HashMap<String, LinkIdentity> = connections
            .iter()
            .filter_map(|(id, machine)| {
                orch.fleet.runner_incarnation().map(|incarnation| {
                    (
                        id.clone(),
                        LinkIdentity {
                            incarnation: incarnation.to_string(),
                            generation: machine.generation,
                        },
                    )
                })
            })
            .collect();
        (now, current)
    };
    Ok(states
        .into_iter()
        .filter_map(|(id, state)| {
            state
                .incident_line(now, current.get(&id))
                .map(|line| (id, line))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edge(id: &str, kind: &str, time: u64) -> Event {
        Event {
            link: Some(LinkIdentity {
                incarnation: "runner-a".into(),
                generation: 1,
            }),
            id: id.into(),
            kind: kind.into(),
            at_ms: time,
            cause: Some("read timed out".into()),
            detail: serde_json::json!({"executor_build_id":"returned-build","link_path":{"tunnel":true}}),
        }
    }

    #[test]
    fn independent_incidents_survive_restart_escalate_once_and_clear_with_return_facts() {
        let mut a = State {
            name: "first".into(),
            ..State::default()
        };
        let mut b = State {
            name: "second".into(),
            ..State::default()
        };
        let mut notices = Vec::new();
        let mut attach = edge("quick-attach", "executor_attached", 80);
        attach.detail["last_update"] = serde_json::json!({
            "state": "succeeded", "fromBuild": "old", "toDistributionId": "distribution",
            "toBuild": "returned-build", "atUnixMs": 55
        });
        a.event("a", attach, &mut notices);
        assert_eq!(a.update.as_ref().unwrap().at_ms, 55);
        // A delayed Hello report cannot roll back a newer rollout fact.
        a.update.as_mut().unwrap().at_ms = 60;
        let mut delayed = edge("delayed-hello", "executor_attached", 90);
        delayed.detail["last_update"] = serde_json::json!({
            "state": "succeeded", "fromBuild": "old", "toDistributionId": "distribution",
            "toBuild": "returned-build", "atUnixMs": 55
        });
        a.event("a", delayed, &mut notices);
        assert_eq!(a.update.as_ref().unwrap().at_ms, 60);
        a.event(
            "a",
            edge("detach-a", "executor_detached", 100),
            &mut notices,
        );
        a.observe("a", true, HOUR, &mut notices);
        assert!(notices.is_empty());
        a.observe("a", true, HOUR + 100, &mut notices);
        assert!(a.episode.as_ref().unwrap().opened);
        assert!(!b.episode.as_ref().is_some_and(|e| e.opened));
        let open = notices.pop().unwrap();
        a = serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
        a.observe("a", true, HOUR + 200, &mut notices);
        assert!(notices.is_empty(), "restart must not repeat opening");
        assert_eq!(a.episode.as_ref().unwrap().since_ms, 100);
        b.observe("b", true, HOUR + 100, &mut notices);
        b.observe("b", true, 2 * HOUR + 100, &mut notices);
        assert_ne!(open.key, notices.pop().unwrap().key);
        assert!(!b.episode.as_ref().unwrap().start_known);
        let fact = |value: &str| Fact {
            at_ms: 50,
            source: "durable-test-fact".into(),
            value: value.into(),
        };
        a.lane = Some(fact("rust-tests: failed (infrastructure)"));
        a.update = Some(fact("self-update returned old build"));
        a.blocked = vec!["request-a (job owner)".into()];
        a.credential_expiry_ms = Some(DAY + 500);
        a.observe("a", true, DAY + 100, &mut notices);
        a = serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
        assert_eq!(a.lane.as_ref().unwrap().at_ms, 50);
        assert_eq!(a.update.as_ref().unwrap().source, "durable-test-fact");
        assert_eq!(a.credential_expiry_ms, Some(DAY + 500));
        assert_eq!(a.blocked, ["request-a (job owner)"]);
        assert!(notices
            .iter()
            .all(|notice| notice.content.contains("request-a")));
        assert!(a.episode.as_ref().unwrap().day_announced);
        assert!(a.episode.as_ref().unwrap().credential_announced);
        let keys = notices.iter().map(|n| n.key.clone()).collect::<Vec<_>>();
        assert!(keys.iter().any(|k| k.ends_with(":24h")));
        assert!(keys.iter().any(|k| k.ends_with(":credential")));
        notices.clear();
        a = serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
        a.observe("a", true, DAY + 200, &mut notices);
        assert!(notices.is_empty());
        a.event(
            "a",
            edge("attach-a", "executor_attached", DAY + 300),
            &mut notices,
        );
        assert!(a.episode.is_none());
        assert_eq!(a.build.as_ref().unwrap().value, "returned-build");
        assert!(notices[0].key.ends_with(":clear"));
        let clear = a.cleared.as_ref().unwrap();
        assert!(clear.contains(&duration(DAY + 200)));
        assert!(clear.contains("returned-build"));
        assert!(clear.contains("tunnel"));
        assert!(b.episode.as_ref().unwrap().opened);
        notices.clear();
        // A brief attach between monitor ticks still ends the old episode.
        a.event(
            "a",
            edge("detach-again", "executor_detached", DAY + 400),
            &mut notices,
        );
        a.observe("a", true, DAY + 500, &mut notices);
        assert!(!a.episode.as_ref().unwrap().opened);
        assert!(notices.is_empty());
        assert_eq!(a.last_detach.as_ref().unwrap().value, "read timed out");
        let key = b.episode.as_ref().unwrap().key.clone();
        b.event(
            "b",
            edge("delayed-detach", "executor_detached", 50),
            &mut notices,
        );
        let episode = b.episode.as_ref().unwrap();
        assert_eq!(episode.key, key, "late evidence must not re-arm delivery");
        assert_eq!(episode.since_ms, 50);
        assert!(episode.start_known);

        // The initial inventory is only enumeration. A later current presence
        // reading wins over a detach seen during the asynchronous reads.
        a.attached_link = Some(LinkIdentity {
            incarnation: "runner-a".into(),
            generation: 2,
        });
        a.observe("a", false, DAY + 600, &mut notices);
        assert!(a.episode.is_none());
        assert_eq!(a.attached_observed_ms, DAY + 600);
        a.build = Some(fact("new-inspection-build"));
        a.build.as_mut().unwrap().at_ms = DAY + 600;
        a.observe("a", true, DAY + 700, &mut notices);
        let key = a.episode.as_ref().unwrap().key.clone();
        notices.clear();
        // Neither late edge can resurrect the old absence or clear the new one.
        a.event(
            "a",
            edge("late-detach", "executor_detached", DAY + 550),
            &mut notices,
        );
        a.event(
            "a",
            edge("late-attach", "executor_attached", DAY + 560),
            &mut notices,
        );
        assert_eq!(a.episode.as_ref().unwrap().key, key);
        assert_eq!(a.build.as_ref().unwrap().value, "new-inspection-build");
        assert!(notices.is_empty());
        // A genuinely newer brief return must still clear an open episode.
        a.observe("a", true, DAY + HOUR + 700, &mut notices);
        notices.clear();
        a.event(
            "a",
            edge("new-attach", "executor_attached", DAY + HOUR + 800),
            &mut notices,
        );
        a.event(
            "a",
            edge("new-detach", "executor_detached", DAY + HOUR + 900),
            &mut notices,
        );
        assert!(notices.iter().any(|notice| notice.key.ends_with(":clear")));
        assert!(!a.episode.as_ref().unwrap().opened);
        assert_eq!(a.episode.as_ref().unwrap().since_ms, DAY + HOUR + 900);
        assert!(a.incident_line(DAY + HOUR + 900, None).is_none());

        // A detach may precede removal from the connection table while ingest
        // drains. A timestamp alone must not misclassify that as a return.
        let link = LinkIdentity {
            incarnation: "runner-a".into(),
            generation: 1,
        };
        let mut tearing_down = State {
            name: "teardown".into(),
            attached_link: Some(link.clone()),
            ..State::default()
        };
        tearing_down.observe("t", false, 1000, &mut notices);
        tearing_down.event(
            "t",
            edge("lagged-edge", "executor_detached", 900),
            &mut notices,
        );
        assert_eq!(tearing_down.episode.as_ref().unwrap().since_ms, 900);
        assert!(tearing_down.link_has_detached(Some(&link)));
        tearing_down.observe("t", true, HOUR + 1000, &mut notices);
        assert!(tearing_down
            .incident_line(HOUR + 1000, Some(&link))
            .is_some());
        let next_runner_same_generation = LinkIdentity {
            incarnation: "runner-b".into(),
            generation: 1,
        };
        assert!(tearing_down
            .incident_line(HOUR + 1000, Some(&next_runner_same_generation))
            .is_none());
        tearing_down.attached_link = Some(next_runner_same_generation);
        tearing_down.observe("t", false, HOUR + 1100, &mut notices);
        assert!(tearing_down.incident_line(HOUR + 1100, None).is_none());
        // A newer durable attach also supersedes old-generation teardown,
        // even if no current inspection ever sampled that brief return.
        tearing_down.event(
            "t",
            edge("new-durable-attach", "executor_attached", 2 * HOUR),
            &mut notices,
        );
        tearing_down.observe("t", true, 2 * HOUR + 100, &mut notices);
        let since = tearing_down.episode.as_ref().unwrap().since_ms;
        tearing_down.attached_link = Some(link);
        tearing_down.event(
            "t",
            edge("very-late-old-detach", "executor_detached", 900),
            &mut notices,
        );
        assert_eq!(tearing_down.episode.as_ref().unwrap().since_ms, since);
        assert!(!tearing_down.episode.as_ref().unwrap().start_known);
    }

    #[tokio::test]
    async fn incident_projection_and_outbox_commit_together_and_survive_journal_retention() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("absence.db");
        let schema = crate::storage::TURSO_MIGRATIONS
            .iter()
            .find(|migration| migration.name() == "executor_absence_incidents")
            .unwrap();
        let db = LocalDb::open_lineage(&path, &[*schema]).await.unwrap();
        cairn_db::storage::MigrationRunner::new(vec![*schema])
            .run(&db)
            .await
            .unwrap();
        db.execute_script(include_str!(
            "../../../../turso_migrations/0274_runner_events.sql"
        ))
        .await
        .unwrap();
        // Exercise only the observation columns read here, without a runner,
        // project fixture or full migration chain.
        db.execute_script(
            "CREATE TABLE check_result_observations(id TEXT PRIMARY KEY,check_name TEXT,verdict TEXT,ran_at INTEGER,commit_sha TEXT,failure_kind TEXT,executor_id TEXT);
             INSERT INTO check_result_observations VALUES('lane-old','unit','passed',10,'old',NULL,'a');
             INSERT INTO check_result_observations VALUES('lane-last','rust-tests','failed',20,'new','infrastructure','a');
             INSERT INTO check_result_observations VALUES('other-machine','unit','passed',30,'new',NULL,'b');
             INSERT INTO runner_events(id,runner_incarnation,kind,occurred_at_ms,executor_id,cause,detail_json)
             VALUES('edge-a','old-runner','executor_detached',100,'a','read timed out','{}');"
        ).await.unwrap();
        let mut state = State {
            name: "first".into(),
            ..State::default()
        };
        let mut notices = Vec::new();
        for event in events(&db, "a", "").await.unwrap() {
            state.event("a", event, &mut notices);
        }
        state.lane = lane_in_db(&db, "a").await.unwrap();
        assert_eq!(state.lane.as_ref().unwrap().at_ms, 20);
        assert!(state.lane.as_ref().unwrap().source.ends_with("lane-last"));
        state.observe("a", true, HOUR + 100, &mut notices);
        save(&db, "a", &state, &notices).await.unwrap();
        // Retrying an ambiguously delivered transition cannot insert it twice.
        save(&db, "a", &state, &notices).await.unwrap();
        db.execute("DELETE FROM runner_events", ()).await.unwrap();
        drop(db);
        let db = LocalDb::open_lineage(&path, &[*schema]).await.unwrap();
        let mut reloaded = load(&db).await.unwrap().remove("a").unwrap();
        notices.clear();
        reloaded.observe("a", true, HOUR + 200, &mut notices);
        assert!(notices.is_empty());
        assert_eq!(reloaded.episode.as_ref().unwrap().since_ms, 100);
        assert_eq!(reloaded.last_detach.unwrap().value, "read timed out");
        assert_eq!(reloaded.lane.unwrap().at_ms, 20);
        let pending = db
            .query_one(
                "SELECT COUNT(*) FROM executor_absence_notices WHERE delivered_at_ms IS NULL",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(
            pending, 1,
            "the undelivered transition survives restart exactly once"
        );
    }
}
