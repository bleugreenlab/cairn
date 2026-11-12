//! Install-local request transactions. Core owns target policy, operator proof,
//! registered consumers and content scrubbing; this module owns atomic state.
//! Every mutable decision reads and writes through the same exclusive transaction.
use super::authority;
use super::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};
use crate::turso::params;
use cairn_common::identity::{
    AppearanceSnapshot, AppearanceTransport, PrincipalRef, VerificationStatus,
};
use cairn_common::requests::*;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct NormalizedRequest {
    pub workspace_id: String,
    pub kind: RequestKind,
    pub target: Option<String>,
    pub target_identity: String,
    pub coalescing_key: Option<String>,
    pub contribution: RequestContribution,
}
#[derive(Debug, Clone)]
pub struct RequestGateState {
    pub state: RequestState,
    pub accepted: bool,
}

fn error(message: impl Into<String>) -> DbError {
    DbError::internal(message)
}
fn encode<T: Serialize + ?Sized>(v: &T) -> DbResult<String> {
    serde_json::to_string(v).map_err(|e| error(format!("request encoding: {e}")))
}
fn decode<T: DeserializeOwned>(s: &str) -> DbResult<T> {
    serde_json::from_str(s).map_err(|e| error(format!("unreadable request record: {e}")))
}
fn word<T: DeserializeOwned>(s: &str) -> DbResult<T> {
    decode(&encode(s)?)
}
fn id() -> String {
    uuid::Uuid::new_v4().to_string()
}
fn nonblank(s: &str) -> bool {
    !s.trim().is_empty()
}
fn version(s: &str) -> DbResult<(u64, u64, u64)> {
    let n: Vec<_> = s.split('.').collect();
    if n.len() != 3
        || n.iter().any(|s| {
            s.is_empty()
                || (s.len() > 1 && s.starts_with('0'))
                || !s.bytes().all(|b| b.is_ascii_digit())
        })
    {
        return Err(error("expected canonical X.Y.Z target"));
    }
    Ok((
        n[0].parse().map_err(|_| error("version overflow"))?,
        n[1].parse().map_err(|_| error("version overflow"))?,
        n[2].parse().map_err(|_| error("version overflow"))?,
    ))
}
async fn text<P: crate::turso::params::IntoParams + Send>(
    conn: &TrackedConnection,
    sql: &str,
    p: P,
) -> DbResult<Option<String>> {
    let mut rows = conn.query(sql, p).await?;
    rows.next().await?.map(|r| r.text(0)).transpose()
}

async fn members(conn: &TrackedConnection, cohort: &str) -> DbResult<Vec<String>> {
    let mut rows=conn.query("SELECT request_id FROM request_cohort_members WHERE cohort_id=?1 AND request_id IN (SELECT id FROM requests WHERE cohort_id=?1) ORDER BY request_id",params![cohort]).await?;
    let mut out = vec![];
    while let Some(r) = rows.next().await? {
        out.push(r.text(0)?);
    }
    Ok(out)
}
async fn contributions(
    conn: &TrackedConnection,
    request: &str,
) -> DbResult<Vec<RequestContribution>> {
    let mut rows=conn.query("SELECT id,request_id,origin_uri,run_id,principal_json,wake_destination,rationale,requirements_json,minimum_target,normalized_target_json,active,satisfied_by_attempt,created_at FROM request_requesters WHERE request_id=?1 ORDER BY created_at,id",params![request]).await?;
    let mut out = vec![];
    while let Some(r) = rows.next().await? {
        out.push(RequestContribution {
            id: r.text(0)?,
            request_id: r.text(1)?,
            origin_uri: r.text(2)?,
            run_id: r.text(3)?,
            principal: decode(&r.text(4)?)?,
            wake_destination: r.text(5)?,
            rationale: r.text(6)?,
            requirements: decode(&r.text(7)?)?,
            minimum_target: r.opt_text(8)?,
            normalized_target: decode(&r.text(9)?)?,
            active: r.i64(10)? != 0,
            satisfied_by_attempt: r.opt_text(11)?,
            created_at: r.i64(12)?,
            waiting: vec![],
        });
    }
    drop(rows);
    for c in &mut out {
        let mut rows = conn
            .query(
                "SELECT uri FROM request_waiting WHERE contribution_id=?1 ORDER BY uri",
                params![c.id.clone()],
            )
            .await?;
        while let Some(r) = rows.next().await? {
            c.waiting.push(r.text(0)?);
        }
    }
    Ok(out)
}
fn attempt_row(r: &crate::turso::Row) -> DbResult<RequestAttempt> {
    Ok(RequestAttempt {
        id: r.text(0)?,
        authorization_event_id: r.text(1)?,
        acceptance: decode(&r.text(2)?)?,
        state: word(&r.text(3)?)?,
        claim_id: r.opt_text(4)?,
        outcome: r.opt_text(5)?.map(|s| word(&s)).transpose()?,
        report: r.opt_text(6)?.map(|s| decode(&s)).transpose()?,
        updated_at: r.i64(7)?,
    })
}
const ATTEMPT_COLUMNS: &str="a.id,a.authorization_event_id,a.acceptance_json,a.state,a.claim_id,a.outcome,a.report_json,a.updated_at";
async fn attempt_conn(conn: &TrackedConnection, event: &str) -> DbResult<Option<RequestAttempt>> {
    let mut rows=conn.query(format!("SELECT {ATTEMPT_COLUMNS} FROM request_attempts a WHERE a.authorization_event_id=?1"),params![event]).await?;
    rows.next().await?.as_ref().map(attempt_row).transpose()
}
async fn get_conn(conn: &TrackedConnection, request: &str) -> DbResult<Option<OperatorRequest>> {
    let mut rows=conn.query("SELECT r.id,r.workspace_id,r.cohort_id,r.kind,r.target,r.target_identity,r.coalescing_key,c.revision,r.state,r.created_at,r.updated_at,r.current_attempt,r.fulfilled_by_attempt,r.report_json FROM requests r JOIN request_cohorts c ON c.id=r.cohort_id WHERE r.id=?1",params![request]).await?;
    let Some(r) = rows.next().await? else {
        return Ok(None);
    };
    let mut out = OperatorRequest {
        id: r.text(0)?,
        uri: format!("cairn://requests/{}", r.text(0)?),
        workspace_id: r.text(1)?,
        cohort_id: r.text(2)?,
        member_request_ids: vec![],
        kind: word(&r.text(3)?)?,
        target: r.opt_text(4)?,
        target_identity: r.text(5)?,
        coalescing_key: r.opt_text(6)?,
        revision: r.i64(7)?,
        state: word(&r.text(8)?)?,
        created_at: r.i64(9)?,
        updated_at: r.i64(10)?,
        current_attempt: r.opt_text(11)?,
        fulfilled_by_attempt: r.opt_text(12)?,
        report: r.opt_text(13)?.map(|s| decode(&s)).transpose()?,
        requesters: vec![],
        facts: None,
        attempts: vec![],
        available: false,
        unavailable_reason: Some(
            "consumer availability is supplied by the registered runner service".into(),
        ),
    };
    drop(rows);
    out.member_request_ids = members(conn, &out.cohort_id).await?;
    for m in &out.member_request_ids {
        out.requesters.extend(contributions(conn, m).await?);
    }
    let open = text(
        conn,
        "SELECT id FROM request_cohorts WHERE id=?1 AND state='open'",
        params![out.cohort_id.clone()],
    )
    .await?
    .is_some();
    if open {
        let mut rows = conn
            .query(
                "SELECT id FROM requests WHERE cohort_id=?1 AND state='open' ORDER BY id",
                params![out.cohort_id.clone()],
            )
            .await?;
        out.member_request_ids.clear();
        while let Some(row) = rows.next().await? {
            out.member_request_ids.push(row.text(0)?);
        }
    }
    let mut rows=conn.query("SELECT version,schema_version,revision,observed_at,document_json FROM request_facts WHERE cohort_id=?1 ORDER BY version DESC LIMIT 1",params![out.cohort_id.clone()]).await?;
    if let Some(r) = rows.next().await? {
        out.facts = Some(RequestFacts {
            version: r.i64(0)?,
            schema_version: r
                .i64(1)?
                .try_into()
                .map_err(|_| error("invalid facts schema"))?,
            revision: r.i64(2)?,
            observed_at: r.i64(3)?,
            document: decode(&r.text(4)?)?,
        });
    }
    drop(rows);
    let mut rows=conn.query(format!("SELECT DISTINCT {ATTEMPT_COLUMNS} FROM request_attempts a JOIN request_attempt_members m ON m.attempt_id=a.id WHERE m.request_id IN (SELECT request_id FROM request_cohort_members WHERE cohort_id=?1) ORDER BY a.updated_at,a.id"),params![out.cohort_id.clone()]).await?;
    while let Some(r) = rows.next().await? {
        out.attempts.push(attempt_row(&r)?);
    }
    Ok(Some(out))
}
pub async fn get_request(db: &LocalDb, request: &str) -> DbResult<Option<OperatorRequest>> {
    let request_owned = request.to_string();

    db.read(move |conn| {
        let request = request_owned.clone();
        Box::pin(async move {
            let request = request.as_str();
            get_conn(conn, request).await
        })
    })
    .await
}
pub async fn list_requests(
    db: &LocalDb,
    workspace: &str,
    filter: &RequestFilter,
) -> DbResult<Vec<OperatorRequest>> {
    let workspace_owned = workspace.to_string();
    let filter_owned = filter.clone();

    db.read(move |conn| { let workspace=workspace_owned.clone(); let filter=filter_owned.clone(); Box::pin(async move {let workspace=workspace.as_str(); let filter=&filter;
        let mut rows=conn.query("SELECT (SELECT MIN(rep.id) FROM requests rep WHERE rep.cohort_id=c.id AND rep.state=c.state) FROM requests r JOIN request_cohorts c ON c.id=r.cohort_id WHERE r.workspace_id=?1 AND (?2 IS NULL OR c.state=?2) AND (?3 IS NULL OR r.kind=?3) AND (?4 IS NULL OR EXISTS(SELECT 1 FROM request_requesters x WHERE x.request_id=r.id AND x.origin_uri=?4)) AND (?5 IS NULL OR EXISTS(SELECT 1 FROM request_requesters x JOIN request_waiting w ON w.contribution_id=x.id WHERE x.request_id=r.id AND w.uri=?5)) GROUP BY r.cohort_id ORDER BY MAX(r.updated_at) DESC,r.cohort_id LIMIT ?6 OFFSET ?7",
            params![workspace,filter.status.map(|s|s.as_str().to_string()),filter.kind.map(|k|k.as_str().to_string()),
                filter.requester.clone(),filter.waiting.clone(),filter.limit.unwrap_or(50).clamp(1,200),filter.offset.unwrap_or(0).max(0)]).await?;
        let mut ids=vec![]; while let Some(r)=rows.next().await? {ids.push(r.text(0)?);}
        drop(rows);
        let mut out=vec![]; for id in ids {if let Some(r)=get_conn(conn,&id).await? {out.push(r);}}
        Ok(out)
    }) }).await
}
async fn enqueue(
    conn: &TrackedConnection,
    request: &str,
    kind: &str,
    destination: Option<&str>,
    payload: Value,
    now: i64,
) -> DbResult<()> {
    conn.execute("INSERT INTO request_outbox(id,request_id,kind,destination,payload_json,created_at) VALUES(?1,?2,?3,?4,?5,?6)",params![id(),request,kind,destination,encode(&payload)?,now]).await?;
    Ok(())
}
async fn changed(conn: &TrackedConnection, request: &str, wake: bool, now: i64) -> DbResult<()> {
    changed_many(conn, &[request.to_string()], wake, now).await
}
/// One logical transition can touch several identities and cohorts. Changes
/// remain identity-addressed, but a routed destination receives one durable wake.
async fn changed_many(
    conn: &TrackedConnection,
    requests: &[String],
    wake: bool,
    now: i64,
) -> DbResult<()> {
    use std::collections::{BTreeMap, BTreeSet};
    let mut pending: BTreeSet<String> = requests.iter().cloned().collect();
    let mut records = BTreeMap::new();
    while let Some(request) = pending.pop_first() {
        if records.contains_key(&request) {
            continue;
        }
        let r = get_conn(conn, &request)
            .await?
            .ok_or_else(|| error("missing changed request"))?;
        for member in r
            .member_request_ids
            .iter()
            .chain(r.requesters.iter().map(|c| &c.request_id))
        {
            if !records.contains_key(member) {
                pending.insert(member.clone());
            }
        }
        records.insert(request, r);
    }
    for r in records.values() {
        enqueue(
            conn,
            &r.id,
            "changed",
            None,
            json!({"request_id":r.id,"state":r.state}),
            now,
        )
        .await?;
    }
    if wake {
        let mut destinations: BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)> =
            BTreeMap::new();
        for r in records.values() {
            for c in &r.requesters {
                let (origins, members) =
                    destinations.entry(c.wake_destination.clone()).or_default();
                origins.insert(c.origin_uri.clone());
                members.insert(r.id.clone());
            }
        }
        for (destination, (origins, members)) in destinations {
            let primary = requests
                .iter()
                .find(|id| members.contains(*id))
                .or_else(|| members.first())
                .ok_or_else(|| error("wake destination has no request identity"))?;
            let r = records
                .get(primary)
                .ok_or_else(|| error("wake request is missing"))?;
            let snapshots: Vec<_> = members
                .iter()
                .map(|id| {
                    let member = &records[id];
                    json!({"request_id":id,"request_uri":member.uri,"state":member.state})
                })
                .collect();
            let uris: Vec<_> = members.iter().map(|id| records[id].uri.clone()).collect();
            enqueue(
                conn,
                &r.id,
                "wake",
                Some(&destination),
                json!({
                    "request_id":r.id,"state":r.state,"request_uri":r.uri,
                    "request_ids":members,"request_uris":uris,"origin_uris":origins,
                    "requests":snapshots
                }),
                now,
            )
            .await?;
        }
    }
    Ok(())
}
/// Recomputes only mutable open targets from immutable filing minimums.
async fn recompute(conn: &TrackedConnection, cohort: &str, now: i64) -> DbResult<()> {
    let ids = members(conn, cohort).await?;
    let mut minimum: Option<(u64, u64, u64)> = None;
    let mut target = None;
    let mut any = false;
    let mut any_active = false;
    for m in &ids {
        let r = get_conn(conn, m)
            .await?
            .ok_or_else(|| error("missing cohort member"))?;
        let own = contributions(conn, m).await?;
        any_active |= own.iter().any(|c| c.active);
        let active: Vec<_> = own
            .iter()
            .filter(|c| c.active && c.satisfied_by_attempt.is_none())
            .collect();
        if !active.is_empty() {
            any = true;
            if r.kind == RequestKind::BumpVersion {
                for c in active {
                    let s = c
                        .minimum_target
                        .as_deref()
                        .ok_or_else(|| error("bump contribution missing minimum"))?;
                    let v = version(s)?;
                    if minimum.is_none_or(|n| v > n) {
                        minimum = Some(v);
                        target = Some(s.to_string());
                    }
                }
            } else {
                target = r.target;
            }
        }
        let state = if own.iter().any(|c| c.active) {
            if own
                .iter()
                .any(|c| c.active && c.satisfied_by_attempt.is_none())
            {
                "open"
            } else {
                "done"
            }
        } else {
            "withdrawn"
        };
        conn.execute(
            "UPDATE requests SET state=?2,revision=revision+1,updated_at=?3 WHERE id=?1",
            params![m.as_str(), state, now],
        )
        .await?;
    }
    conn.execute(
        "UPDATE request_cohorts SET revision=revision+1,state=?2,updated_at=?3 WHERE id=?1",
        params![
            cohort,
            if any {
                "open"
            } else if any_active {
                "done"
            } else {
                "withdrawn"
            },
            now
        ],
    )
    .await?;
    conn.execute(
        "UPDATE requests SET target=?2 WHERE cohort_id=?1 AND state='open'",
        params![cohort, target],
    )
    .await?;
    Ok(())
}
pub async fn file_request(
    db: &LocalDb,
    input: &NormalizedRequest,
    now: i64,
) -> DbResult<FiledOperatorRequest> {
    let input_owned = input.clone();

    if !nonblank(&input.workspace_id)
        || !nonblank(&input.target_identity)
        || !nonblank(&input.contribution.origin_uri)
        || !nonblank(&input.contribution.run_id)
        || !nonblank(&input.contribution.wake_destination)
        || !nonblank(&input.contribution.rationale)
    {
        return Err(error(
            "filing requires normalized identity and verified run attribution",
        ));
    }
    if input.kind == RequestKind::BumpVersion {
        version(
            input
                .contribution
                .minimum_target
                .as_deref()
                .ok_or_else(|| error("missing bump minimum"))?,
        )?;
    }
    db.exclusive(move |conn| { let input=input_owned.clone(); Box::pin(async move {let input=&input;
        let existing=text(conn,"SELECT id FROM request_cohorts WHERE workspace_id=?1 AND coalescing_key=?2 AND state='open'",params![input.workspace_id.clone(),input.coalescing_key.clone()]).await?;
        let created=existing.is_none();
        let cohort=existing.unwrap_or_else(id);
        let request=if created {
            let r=id();
            conn.execute("INSERT INTO request_cohorts(id,workspace_id,coalescing_key,kind,state,created_at,updated_at) VALUES(?1,?2,?3,?4,'open',?5,?5)",params![cohort.clone(),input.workspace_id.clone(),input.coalescing_key.clone(),input.kind.as_str(),now]).await?;
            conn.execute("INSERT INTO requests(id,workspace_id,kind,target,target_identity,coalescing_key,cohort_id,state,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,'open',?8,?8)",params![r.clone(),input.workspace_id.clone(),input.kind.as_str(),input.target.clone(),input.target_identity.clone(),input.coalescing_key.clone(),cohort.clone(),now]).await?;
            conn.execute("INSERT INTO request_cohort_members(cohort_id,request_id) VALUES(?1,?2)",params![cohort.clone(),r.clone()]).await?;
            r
        } else {
            let r=text(conn,"SELECT id FROM requests WHERE cohort_id=?1 AND state='open' ORDER BY id LIMIT 1",params![cohort.clone()]).await?.ok_or_else(||error("empty open cohort"))?;
            let card=get_conn(conn,&r).await?.ok_or_else(||error("missing request"))?;
            if card.kind!=input.kind || card.target_identity!=input.target_identity {return Err(error("coalescing key collides with another kind or target identity"));}
            r
        };
        let c=&input.contribution;
        let requirements=encode(&c.requirements)?;
        let normalized=encode(&c.normalized_target)?;
        let filing_key=encode(&c.normalized_target.get("filing").cloned().unwrap_or_else(||json!({"requirements":c.requirements,"normalized_target":c.normalized_target})))?;
        let duplicate=text(conn,"SELECT id FROM request_requesters WHERE request_id=?1 AND origin_uri=?2 AND rationale=?3 AND filing_key=?4",params![request.clone(),c.origin_uri.clone(),c.rationale.clone(),filing_key.clone()]).await?;
        let contribution=duplicate.clone().unwrap_or_else(id);
        let mut modified=duplicate.is_none();
        if duplicate.is_none() {
            conn.execute("INSERT INTO request_requesters(id,request_id,origin_uri,run_id,principal_json,wake_destination,rationale,requirements_json,minimum_target,normalized_target_json,filing_key,active,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,1,?12)",
                params![contribution.clone(),request.clone(),c.origin_uri.clone(),c.run_id.clone(),encode(&c.principal)?,c.wake_destination.clone(),c.rationale.clone(),requirements,c.minimum_target.clone(),normalized,filing_key,now]).await?;
            enqueue(conn,&request,"reference",Some(&c.wake_destination),json!({"request_id":request,"request_uri":format!("cairn://requests/{request}"),"run_id":c.run_id,"origin_uri":c.origin_uri}),now).await?;
        } else {
            let active=text(conn,"SELECT CAST(active AS TEXT) FROM request_requesters WHERE id=?1",params![contribution.clone()]).await?;
            if active.as_deref()==Some("0") {
                conn.execute("UPDATE request_requesters SET active=1 WHERE id=?1",params![contribution.clone()]).await?;modified=true;
            }
        }
        for uri in &c.waiting {
            let n=conn.execute("INSERT OR IGNORE INTO request_waiting(contribution_id,uri) VALUES(?1,?2)",params![contribution.clone(),uri.clone()]).await?;
            modified|=n>0;
        }
        if modified { recompute(conn,&cohort,now).await?;changed(conn,&request,false,now).await?; }
        Ok(FiledOperatorRequest{request:get_conn(conn,&request).await?.ok_or_else(||error("missing filed request"))?,created})
    }) }).await
}
pub async fn withdraw_request(
    db: &LocalDb,
    request: &str,
    origin: &str,
    now: i64,
) -> DbResult<OperatorRequest> {
    let request_owned = request.to_string();
    let origin_owned = origin.to_string();

    db.exclusive(move |conn| { let request=request_owned.clone(); let origin=origin_owned.clone(); Box::pin(async move {let request=request.as_str(); let origin=origin.as_str();
        let r=get_conn(conn,request).await?.ok_or_else(||error("request not found"))?;
        if r.state!=RequestState::Open {return Err(error("withdrawal cannot revoke an accepted or terminal act"));}
        let n=conn.execute("UPDATE request_requesters SET active=0 WHERE request_id=?1 AND origin_uri=?2 AND active=1",params![request,origin]).await?;
        if n==0 {return Err(error("no active contribution belongs to this origin"));}
        recompute(conn,&r.cohort_id,now).await?;changed(conn,request,true,now).await?;
        get_conn(conn,request).await?.ok_or_else(||error("request missing"))
    }) }).await
}
pub async fn refresh_facts(
    db: &LocalDb,
    request: &str,
    revision: i64,
    schema_version: u32,
    document: Value,
    now: i64,
) -> DbResult<RequestFacts> {
    let request_owned = request.to_string();

    db.exclusive(move |conn| {
        let document=document.clone();
        let request=request_owned.clone(); Box::pin(async move {let request=request.as_str();
            let r=get_conn(conn,request).await?.ok_or_else(||error("request not found"))?;
            if r.state!=RequestState::Open||r.revision!=revision {return Err(error("stale request card revision"));}
            let version=r.facts.map_or(1,|f|f.version+1);
            let f=RequestFacts {version,schema_version,revision,observed_at:now,document};
            conn.execute("INSERT INTO request_facts(cohort_id,version,revision,schema_version,observed_at,document_json) VALUES(?1,?2,?3,?4,?5,?6)",
                params![r.cohort_id,version,revision,i64::from(schema_version),now,encode(&f.document)?]).await?;
            changed(conn,request,false,now).await?;
            Ok(f)
        })
    }).await
}
fn operator(actor: &PrincipalRef, appearance: &AppearanceSnapshot) -> DbResult<()> {
    appearance
        .validate()
        .map_err(|e| error(format!("invalid operator evidence: {e}")))?;
    if appearance.principal() != actor
        || !(matches!(actor, PrincipalRef::Human { .. })
            && matches!(
                appearance.evidence().transport,
                AppearanceTransport::AuthenticatedDesktop
                    | AppearanceTransport::AuthenticatedOperator
            )
            || matches!(actor, PrincipalRef::Machine { .. })
                && appearance.evidence().transport == AppearanceTransport::AuthenticatedDesktop)
        || appearance.evidence().verification.status() != VerificationStatus::Verified
        || !appearance.delegation().is_empty()
        || appearance.terminal_represented().is_some()
    {
        return Err(error("operator-proven direct human appearance required"));
    }
    Ok(())
}
fn validate_acceptance(a: &RequestAcceptance) -> DbResult<()> {
    operator(&a.operator, &a.appearance)?;
    if a.schema_version != 1
        || !nonblank(&a.install_id)
        || !nonblank(&a.workspace_id)
        || !nonblank(&a.cohort_id)
        || !nonblank(&a.attempt_id)
        || !nonblank(&a.actor_id)
        || !nonblank(&a.target_identity)
        || a.request_ids.is_empty()
        || a.request_ids.iter().any(|s| !nonblank(s))
        || a.facts_version < 1
        || a.revision < 1
        || a.binding.algorithm != cairn_common::authorization::ACT_DIGEST_ALGORITHM
        || a.binding.encoding_version != cairn_common::authorization::ACT_DIGEST_ENCODING_VERSION
        || a.binding
            .salt
            .as_ref()
            .is_none_or(|s| s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()))
        || a.binding.digest.len() != 64
        || !a.binding.digest.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(error("missing or unsupported frozen acceptance fields"));
    }
    match a.kind {
        RequestKind::Rebuild => {
            if a.candidate_commit.as_ref().is_none_or(|s| !nonblank(s))
                || a.target.is_some()
                || a.drain.is_some()
            {
                return Err(error(
                    "rebuild acceptance requires frozen candidate and no target/drain",
                ));
            }
        }
        RequestKind::BumpVersion => {
            version(
                a.target
                    .as_deref()
                    .ok_or_else(|| error("missing bump target"))?,
            )?;
            if a.base_commit.as_ref().is_none_or(|s| !nonblank(s))
                || a.policy_project_id.as_ref().is_none_or(|s| !nonblank(s))
                || a.drain.is_some()
            {
                return Err(error("bump acceptance requires base and policy project"));
            }
        }
        RequestKind::LiftDrain => {
            let d = a
                .drain
                .as_ref()
                .ok_or_else(|| error("missing drain generation"))?;
            if !nonblank(&d.executor_id)
                || !nonblank(&d.drain_key)
                || !nonblank(&d.owner)
                || d.generation == 0
                || d.machine_revision == 0
                || a.target.as_ref().is_none_or(|s| !nonblank(s))
            {
                return Err(error(
                    "missing stable drain identity/owner/generation/machine revision",
                ));
            }
        }
    }
    Ok(())
}
pub async fn accept_request(db: &LocalDb, a: &RequestAcceptance) -> DbResult<RequestAttempt> {
    let a_owned = a.clone();

    validate_acceptance(a)?;
    db.exclusive(move |conn| { let a=a_owned.clone(); Box::pin(async move {let a=&a;
        let first=a.request_ids.first().ok_or_else(||error("empty acceptance membership"))?;
        let r=get_conn(conn,first).await?.ok_or_else(||error("request not found"))?;
        let mut expected=a.request_ids.clone();expected.sort();expected.dedup();
        if expected.len()!=a.request_ids.len()||r.member_request_ids!=expected||r.state!=RequestState::Open
            ||r.cohort_id!=a.cohort_id||r.revision!=a.revision||r.kind!=a.kind||r.target!=a.target
            ||r.target_identity!=a.target_identity||r.workspace_id!=a.workspace_id {
            return Err(error("stale or mismatched acceptance membership, kind, target or revision"));
        }
        let f=r.facts.as_ref().ok_or_else(||error("facts unavailable"))?;
        if f.version!=a.facts_version||f.revision!=a.revision {return Err(error("stale displayed facts"));}
        let unresolved=text(conn,"SELECT a.id FROM request_attempts a JOIN request_cohorts c ON c.id=a.cohort_id WHERE (a.state IN ('accepted','acting') OR (a.state='failed' AND a.settled=0)) AND c.workspace_id=?1 AND (c.coalescing_key=?2 OR c.id=?3) LIMIT 1",params![a.workspace_id.clone(),r.coalescing_key.clone(),a.cohort_id.clone()]).await?;
        if unresolved.is_some() {return Err(error("an earlier accepted consumer is unresolved; reconcile/fence it before another acceptance"));}
        let event=authority::append_request_acceptance_conn(conn,a).await?;
        conn.execute("INSERT INTO request_attempts(id,cohort_id,authorization_event_id,acceptance_json,state,updated_at) VALUES(?1,?2,?3,?4,'accepted',?5)",params![a.attempt_id.clone(),a.cohort_id.clone(),event.clone(),encode(a)?,a.accepted_at]).await?;
        for m in &a.request_ids {
            let snapshot=contributions(conn,m).await?;
            if !snapshot.iter().any(|c|c.active&&c.satisfied_by_attempt.is_none()) {return Err(error("acceptance includes a closed member"));}
            conn.execute("INSERT INTO request_attempt_members(attempt_id,request_id,contributions_json) VALUES(?1,?2,?3)",params![a.attempt_id.clone(),m.clone(),encode(&snapshot)?]).await?;
            conn.execute("UPDATE requests SET state='accepted',current_attempt=?2,updated_at=?3 WHERE id=?1 AND state='open'",params![m.clone(),a.attempt_id.clone(),a.accepted_at]).await?;
        }
        conn.execute("UPDATE request_cohorts SET state='accepted',updated_at=?2 WHERE id=?1",params![a.cohort_id.clone(),a.accepted_at]).await?;
        enqueue(conn,first,"dispatch",None,json!({"attempt_id":a.attempt_id,"authorization_event_id":event}),a.accepted_at).await?;
        changed_many(conn,&a.request_ids,true,a.accepted_at).await?;
        attempt_conn(conn,&event).await?.ok_or_else(||error("attempt missing"))
    }) }).await
}
pub async fn decline_request(
    db: &LocalDb,
    request: &str,
    revision: i64,
    reason: &str,
    actor: &PrincipalRef,
    appearance: &AppearanceSnapshot,
    now: i64,
) -> DbResult<OperatorRequest> {
    let request_owned = request.to_string();
    let reason_owned = reason.to_string();
    let actor_owned = actor.clone();
    let appearance_owned = appearance.clone();

    if !nonblank(reason) {
        return Err(error("decline requires a nonblank reason"));
    }
    operator(actor, appearance)?;
    db.exclusive(move |conn| { let request=request_owned.clone(); let reason=reason_owned.clone(); let actor=actor_owned.clone(); let appearance=appearance_owned.clone(); Box::pin(async move {let request=request.as_str(); let reason=reason.as_str(); let actor=&actor; let appearance=&appearance;
        let r=get_conn(conn,request).await?.ok_or_else(||error("request not found"))?;
        if r.state!=RequestState::Open||r.revision!=revision {return Err(error("stale or non-open request"));}
        let report=encode(&json!({"decision":"declined","reason":reason,"operator":actor,"appearance":appearance,"decided_at":now}))?;
        for m in &r.member_request_ids {
            conn.execute("UPDATE requests SET state='declined',report_json=?2,updated_at=?3 WHERE id=?1",params![m.clone(),report.clone(),now]).await?;
        }
        conn.execute("UPDATE request_cohorts SET state='declined',updated_at=?2 WHERE id=?1",params![r.cohort_id,now]).await?;
        changed_many(conn,&r.member_request_ids,true,now).await?;
        get_conn(conn,request).await?.ok_or_else(||error("request missing"))
    }) }).await
}
pub async fn get_attempt_by_event(db: &LocalDb, event: &str) -> DbResult<Option<RequestAttempt>> {
    let event_owned = event.to_string();

    db.read(move |conn| {
        let event = event_owned.clone();
        Box::pin(async move {
            let event = event.as_str();
            attempt_conn(conn, event).await
        })
    })
    .await
}
async fn checked(
    conn: &TrackedConnection,
    event: &str,
    expected: &RequestAcceptance,
) -> DbResult<RequestAttempt> {
    checked_record(conn, event, expected, true).await
}
async fn checked_record(
    conn: &TrackedConnection,
    event: &str,
    expected: &RequestAcceptance,
    current: bool,
) -> DbResult<RequestAttempt> {
    validate_acceptance(expected)?;
    let a = attempt_conn(conn, event)
        .await?
        .ok_or_else(|| error("no accepted request journal/attempt"))?;
    if a.acceptance != *expected {
        return Err(error("frozen acceptance fields differ from intended act"));
    }
    let mut rows=conn.query("SELECT outcome,request_acceptance_json,actor_principal_json,appearance_snapshot_json,workspace_id,grant_id,request_uri,scope_json,reason,principal_json,audience_json,mutation,decided_at FROM authorization_events WHERE id=?1",params![event]).await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| error("acceptance journal missing"))?;
    let envelope: RequestAcceptance = decode(&row.text(1)?)?;
    if row.text(0)? != "request_accepted"
        || envelope != *expected
        || decode::<PrincipalRef>(&row.text(2)?)? != expected.operator
        || decode::<AppearanceSnapshot>(&row.text(3)?)? != expected.appearance
        || row.text(4)? != expected.workspace_id
        || row.opt_text(5)?.is_some()
        || row.text(6)? != format!("cairn://requests/{}", expected.request_ids[0])
        || decode::<cairn_common::authorization::AuthorityScope>(&row.text(7)?)?
            != cairn_common::authorization::AuthorityScope::new(
                cairn_common::authorization::AuthorityPlace::Resource {
                    canonical_uri: format!("cairn://requests/{}", expected.request_ids[0]),
                },
                cairn_common::authorization::AuthorityAction::Run,
            )
        || row.text(8)? != "operator_request_acceptance"
        || decode::<cairn_common::authorization::AuthorityPrincipal>(&row.text(9)?)?
            != (cairn_common::authorization::AuthorityPrincipal {
                actor: Some(expected.operator.clone()),
                ..Default::default()
            })
        || decode::<cairn_common::authorization::AuthorityAudience>(&row.text(10)?)?
            != cairn_common::authorization::AuthorityAudience::workspace(&expected.workspace_id)
        || row.text(11)? != "update"
        || row.i64(12)? != expected.accepted_at
    {
        return Err(error("acceptance journal provenance mismatch"));
    }
    drop(rows);
    let mut ids = vec![];
    let mut rows=conn.query("SELECT m.request_id,r.current_attempt,r.state FROM request_attempt_members m JOIN requests r ON r.id=m.request_id WHERE m.attempt_id=?1 ORDER BY m.request_id",params![a.id.clone()]).await?;
    while let Some(r) = rows.next().await? {
        if current
            && (r.opt_text(1)?.as_deref() != Some(a.id.as_str())
                || !matches!(r.text(2)?.as_str(), "accepted" | "acting" | "failed"))
        {
            return Err(error("attempt no longer owns every member"));
        }
        ids.push(r.text(0)?);
    }
    let mut expected_ids = expected.request_ids.clone();
    expected_ids.sort();
    if ids != expected_ids {
        return Err(error("immutable attempt membership differs"));
    }
    Ok(a)
}
pub async fn claim_request(
    db: &LocalDb,
    event: &str,
    expected: &RequestAcceptance,
    claim: &str,
    now: i64,
) -> DbResult<RequestAttempt> {
    let event_owned = event.to_string();
    let expected_owned = expected.clone();
    let claim_owned = claim.to_string();

    if !nonblank(claim) {
        return Err(error("empty claim id"));
    }
    db.exclusive(move |conn| { let event=event_owned.clone(); let expected=expected_owned.clone(); let claim=claim_owned.clone(); Box::pin(async move {let event=event.as_str(); let expected=&expected; let claim=claim.as_str();
        let a=checked(conn,event,expected).await?;
        if a.state!=RequestState::Accepted||a.claim_id.is_some() {return Err(error("acceptance already claimed or spent"));}
        let n=conn.execute("UPDATE request_attempts SET state='acting',claim_id=?2,updated_at=?3 WHERE id=?1 AND state='accepted' AND claim_id IS NULL",params![a.id.clone(),claim,now]).await?;
        if n!=1 {return Err(error("claim lost compare-and-swap"));}
        conn.execute("UPDATE request_cohorts SET state='acting',updated_at=?2 WHERE id=?1",params![a.acceptance.cohort_id.clone(),now]).await?;
        for m in &a.acceptance.request_ids {
            conn.execute("UPDATE requests SET state='acting',updated_at=?2 WHERE id=?1",params![m.clone(),now]).await?;
        }
        changed_many(conn,&a.acceptance.request_ids,false,now).await?;
        attempt_conn(conn,event).await?.ok_or_else(||error("missing attempt"))
    }) }).await
}
pub async fn recheck_request(
    db: &LocalDb,
    event: &str,
    expected: &RequestAcceptance,
    claim: &str,
) -> DbResult<RequestAttempt> {
    let event_owned = event.to_string();
    let expected_owned = expected.clone();
    let claim_owned = claim.to_string();

    db.read(move |conn| {
        let event = event_owned.clone();
        let expected = expected_owned.clone();
        let claim = claim_owned.clone();
        Box::pin(async move {
            let event = event.as_str();
            let expected = &expected;
            let claim = claim.as_str();
            let a = checked(conn, event, expected).await?;
            if a.state != RequestState::Acting || a.claim_id.as_deref() != Some(claim) {
                return Err(error("stale or wrong consumer claim"));
            }
            Ok(a)
        })
    })
    .await
}
async fn reopen(conn: &TrackedConnection, a: &RequestAttempt, now: i64) -> DbResult<()> {
    let first = get_conn(conn, &a.acceptance.request_ids[0])
        .await?
        .ok_or_else(|| error("missing request"))?;
    conn.execute(
        "UPDATE request_cohorts SET state='failed',updated_at=?2 WHERE id=?1",
        params![a.acceptance.cohort_id.clone(), now],
    )
    .await?;
    let successor=text(conn,"SELECT id FROM request_cohorts WHERE workspace_id=?1 AND coalescing_key=?2 AND state='open'",params![first.workspace_id.clone(),first.coalescing_key.clone()]).await?;
    let cohort = if let Some(s) = successor {
        s
    } else {
        let s = id();
        conn.execute("INSERT INTO request_cohorts(id,workspace_id,coalescing_key,kind,state,created_at,updated_at) VALUES(?1,?2,?3,?4,'open',?5,?5)",params![s.clone(),first.workspace_id,first.coalescing_key,first.kind.as_str(),now]).await?;
        s
    };
    for m in &a.acceptance.request_ids {
        conn.execute(
            "INSERT INTO request_cohort_members(cohort_id,request_id) VALUES(?1,?2)",
            params![cohort.clone(), m.clone()],
        )
        .await?;
        conn.execute(
            "UPDATE requests SET cohort_id=?2,state='open',updated_at=?3 WHERE id=?1",
            params![m.clone(), cohort.clone(), now],
        )
        .await?;
    }
    recompute(conn, &cohort, now).await
}
/// A terminal consumer result reopens only after the caller has positively
/// settled/fenced the physical actor. Unknown liveness retains the acting fence.
#[allow(clippy::too_many_arguments)]
pub async fn report_request(
    db: &LocalDb,
    attempt: &str,
    claim: &str,
    receipt: &str,
    outcome: RequestAttemptOutcome,
    report: Value,
    settled: bool,
    now: i64,
) -> DbResult<RequestAttempt> {
    report_request_with_fulfillment(
        db,
        attempt,
        claim,
        receipt,
        outcome,
        report,
        settled,
        &[],
        now,
    )
    .await
}
#[allow(clippy::too_many_arguments)]
pub async fn report_request_with_fulfillment(
    db: &LocalDb,
    attempt: &str,
    claim: &str,
    receipt: &str,
    outcome: RequestAttemptOutcome,
    report: Value,
    settled: bool,
    covered: &[String],
    now: i64,
) -> DbResult<RequestAttempt> {
    let attempt_owned = attempt.to_string();
    let claim_owned = claim.to_string();
    let receipt_owned = receipt.to_string();
    let covered_owned = covered.to_vec();

    if !nonblank(receipt) {
        return Err(error("consumer report requires stable receipt id"));
    }
    let wire =
        encode(&json!({"outcome":outcome,"report":report,"settled":settled,"covered":covered}))?;
    db.exclusive(move |conn| {
        let wire=wire.clone();let outcome=outcome.clone();let report=report.clone();
        let attempt=attempt_owned.clone(); let claim=claim_owned.clone(); let receipt=receipt_owned.clone(); let covered=covered_owned.clone(); Box::pin(async move {let attempt=attempt.as_str(); let claim=claim.as_str(); let receipt=receipt.as_str(); let covered=covered.as_slice();
            let event=text(conn,"SELECT authorization_event_id FROM request_attempts WHERE id=?1",params![attempt]).await?.ok_or_else(||error("unknown attempt"))?;
            let a=attempt_conn(conn,&event).await?.ok_or_else(||error("missing attempt"))?;
            let mut rows=conn.query("SELECT attempt_id,claim_id,report_json FROM request_report_receipts WHERE id=?1",params![receipt]).await?;
            if let Some(r)=rows.next().await? {
                if r.text(0)?!=attempt||r.text(1)?!=claim {return Err(error("conflicting consumer receipt attribution"));}
                frozen_report_coverage(&r.text(2)?, &outcome, &report, settled)?;
                return Ok(a);
            }
            drop(rows);
            let a=checked(conn,&event,&a.acceptance).await?;
            if !matches!(a.state,RequestState::Acting|RequestState::Failed)||a.claim_id.as_deref()!=Some(claim) {return Err(error("report is not from the unresolved consumer claim"));}
            if a.state==RequestState::Failed && outcome==RequestAttemptOutcome::Done {
                return Err(error("a failed attempt is terminal and cannot be upgraded to success; settle it and obtain a new acceptance"));
            }
            if outcome==RequestAttemptOutcome::Done&&!settled {return Err(error("success requires settled decisive consumer evidence"));}
            conn.execute("INSERT INTO request_report_receipts(id,attempt_id,claim_id,report_json,created_at) VALUES(?1,?2,?3,?4,?5)",params![receipt,attempt,claim,wire,now]).await?;
            let state=if outcome==RequestAttemptOutcome::Done {"done"} else {"failed"};
            conn.execute("UPDATE request_attempts SET state=?2,outcome=?3,report_json=?4,updated_at=?5,settled=?6 WHERE id=?1",params![attempt,state,encode(&outcome)?.trim_matches('"').to_string(),encode(&report)?,now,i64::from(settled)]).await?;
            for m in &a.acceptance.request_ids {
                conn.execute("UPDATE requests SET state=?2,report_json=?3,updated_at=?4 WHERE id=?1",params![m.clone(),state,encode(&report)?,now]).await?;
                if outcome==RequestAttemptOutcome::Done {
                    let mut rows=conn.query("SELECT id FROM request_requesters WHERE request_id=?1 AND active=1 AND satisfied_by_attempt IS NULL ORDER BY id",params![m.clone()]).await?;
                    let mut open=vec![];while let Some(r)=rows.next().await? {open.push(r.text(0)?);}drop(rows);
                    record_fulfillment(conn,attempt,m,&open).await?;
                }
            }
            if settled&&outcome!=RequestAttemptOutcome::Done {reopen(conn,&a,now).await?;}
            else {conn.execute("UPDATE request_cohorts SET state=?2,updated_at=?3 WHERE id=?1",params![a.acceptance.cohort_id.clone(),state,now]).await?;}
            let mut changed_requests=a.acceptance.request_ids.clone();
            if !covered.is_empty() {
                changed_requests.extend(fulfill_conn(conn,attempt,covered,&report,now).await?);
            }
            if settled && outcome!=RequestAttemptOutcome::Done && a.acceptance.kind==RequestKind::BumpVersion {
                let mut rows=conn.query("SELECT id FROM requests WHERE workspace_id=?1 AND kind='rebuild' AND state='open'",params![a.acceptance.workspace_id.clone()]).await?;
                let mut ids=vec![];while let Some(r)=rows.next().await? {ids.push(r.text(0)?);}drop(rows);
                for r in ids {
                    conn.execute("UPDATE requests SET report_json=?2,updated_at=?3 WHERE id=?1",params![r.clone(),encode(&json!({"bump_failure_attempt":attempt,"report":report}))?,now]).await?;
                    changed_requests.push(r);
                }
            }
            changed_many(conn,&changed_requests,true,now).await?;
            attempt_conn(conn,&event).await?.ok_or_else(||error("missing attempt"))
        })
    }).await
}
/// Links proven coverage, never inventing acceptance on the fulfilled request.
pub async fn fulfill_rebuild_contributions(
    db: &LocalDb,
    bump_attempt: &str,
    covered: &[String],
    report: Value,
    now: i64,
) -> DbResult<()> {
    let bump_attempt_owned = bump_attempt.to_string();
    let covered_owned = covered.to_vec();

    db.exclusive(move |conn| {
        let report = report.clone();
        let bump_attempt = bump_attempt_owned.clone();
        let covered = covered_owned.clone();
        Box::pin(async move {
            let bump_attempt = bump_attempt.as_str();
            let covered = covered.as_slice();
            let requests = fulfill_conn(conn, bump_attempt, covered, &report, now).await?;
            changed_many(conn, &requests, true, now).await
        })
    })
    .await
}
async fn fulfill_conn(
    conn: &TrackedConnection,
    bump_attempt: &str,
    covered: &[String],
    report: &Value,
    now: i64,
) -> DbResult<Vec<String>> {
    let event = text(
        conn,
        "SELECT authorization_event_id FROM request_attempts WHERE id=?1",
        params![bump_attempt],
    )
    .await?
    .ok_or_else(|| error("unknown bump attempt"))?;
    let a = attempt_conn(conn, &event)
        .await?
        .ok_or_else(|| error("missing bump attempt"))?;
    if a.acceptance.kind != RequestKind::BumpVersion
        || a.state != RequestState::Done
        || a.outcome != Some(RequestAttemptOutcome::Done)
    {
        return Err(error(
            "linked rebuild fulfillment requires successful checked bump",
        ));
    }
    let mut satisfied: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
        std::collections::BTreeMap::new();
    for c in covered {
        let mut rows=conn.query("SELECT x.request_id,r.kind,r.state,x.satisfied_by_attempt FROM request_requesters x JOIN requests r ON r.id=x.request_id WHERE x.id=?1 AND x.active=1",params![c.clone()]).await?;
        let r = rows
            .next()
            .await?
            .ok_or_else(|| error("covered contribution is unknown/inactive"))?;
        let request = r.text(0)?;
        if r.text(1)? != "rebuild" {
            return Err(error("bump may fulfill rebuild contributions only"));
        }
        if r.opt_text(3)?.as_deref() == Some(bump_attempt) {
            continue;
        }
        if r.text(2)? != "open" || r.opt_text(3)?.is_some() {
            return Err(error("covered rebuild contribution is not open"));
        }
        drop(rows);
        satisfied.entry(request).or_default().insert(c.clone());
    }
    for (request, contributions) in &satisfied {
        let contributions: Vec<_> = contributions.iter().cloned().collect();
        record_fulfillment(conn, bump_attempt, request, &contributions).await?;
    }
    let requests: Vec<_> = satisfied.into_keys().collect();
    for request in &requests {
        let r = get_conn(conn, request)
            .await?
            .ok_or_else(|| error("missing rebuild"))?;
        let remaining=text(conn,"SELECT id FROM request_requesters WHERE request_id=?1 AND active=1 AND satisfied_by_attempt IS NULL LIMIT 1",params![request.clone()]).await?;
        conn.execute("UPDATE requests SET report_json=?2,updated_at=?3,state=?4 WHERE id=?1",params![request.clone(),encode(&json!({"fulfilled_by_attempt":bump_attempt,"authorization_event_id":event,"report":report}))?,now,if remaining.is_none(){"done"}else{"open"}]).await?;
        conn.execute(
            "UPDATE request_cohorts SET revision=revision+1,updated_at=?2 WHERE id=?1",
            params![r.cohort_id.clone(), now],
        )
        .await?;
        let open = text(
            conn,
            "SELECT id FROM requests WHERE cohort_id=?1 AND state='open' LIMIT 1",
            params![r.cohort_id.clone()],
        )
        .await?;
        if open.is_none() {
            conn.execute(
                "UPDATE request_cohorts SET state='done' WHERE id=?1",
                params![r.cohort_id],
            )
            .await?;
        }
    }
    Ok(requests)
}
/// Records `attempt` as what satisfied `contributions` of `request` and what
/// fulfilled the request. Both finishing paths (a request's own accepted
/// attempt, and a bump attempt's proven coverage) write through here, so
/// `recompute` reads a finished request's contributions as satisfied rather
/// than open.
async fn record_fulfillment(
    conn: &TrackedConnection,
    attempt: &str,
    request: &str,
    contributions: &[String],
) -> DbResult<()> {
    for c in contributions {
        conn.execute(
            "UPDATE request_requesters SET satisfied_by_attempt=?3 WHERE id=?1 AND request_id=?2",
            params![c.clone(), request, attempt],
        )
        .await?;
    }
    conn.execute(
        "UPDATE requests SET fulfilled_by_attempt=?2 WHERE id=?1",
        params![request, attempt],
    )
    .await?;
    Ok(())
}
pub async fn request_gate_state(db: &LocalDb, request: &str) -> DbResult<Option<RequestGateState>> {
    let request_owned = request.to_string();

    db.read(move |conn| { let request=request_owned.clone(); Box::pin(async move {let request=request.as_str();
        let mut rows=conn.query("SELECT state,EXISTS(SELECT 1 FROM request_attempt_members WHERE request_id=requests.id) FROM requests WHERE id=?1",params![request]).await?;
        rows.next().await?.map(|r|Ok(RequestGateState {state:word(&r.text(0)?)?,accepted:r.i64(1)?!=0})).transpose()
    }) }).await
}
pub async fn pending_outbox(db: &LocalDb, limit: i64) -> DbResult<Vec<RequestOutbox>> {
    db.query_all("SELECT id,request_id,kind,destination,payload_json,created_at FROM request_outbox WHERE delivered_at IS NULL ORDER BY COALESCE(last_attempt_at,0),created_at,id LIMIT ?1".to_string(),(limit.clamp(1,200),),|r|Ok(RequestOutbox {id:r.text(0)?,request_id:r.text(1)?,kind:r.text(2)?,destination:r.opt_text(3)?,payload:decode(&r.text(4)?)?,created_at:r.i64(5)?})).await
}
pub async fn acknowledge_outbox(db: &LocalDb, item: &str, now: i64) -> DbResult<()> {
    let item_owned = item.to_string();

    db.write(move |conn| { let item=item_owned.clone(); Box::pin(async move {let item=item.as_str();
        conn.execute("UPDATE request_outbox SET delivered_at=?2,last_error=NULL WHERE id=?1 AND delivered_at IS NULL",params![item,now]).await?;Ok(())
    }) }).await
}
pub async fn record_outbox_failure(
    db: &LocalDb,
    item: &str,
    message: &str,
    now: i64,
) -> DbResult<()> {
    let item_owned = item.to_string();
    let message_owned = message.to_string();

    db.write(move |conn| { let item=item_owned.clone(); let message=message_owned.clone(); Box::pin(async move {let item=item.as_str(); let message=message.as_str();
        conn.execute("UPDATE request_outbox SET last_attempt_at=?2,last_error=?3 WHERE id=?1 AND delivered_at IS NULL",params![item,now,message]).await?;Ok(())
    }) }).await
}
/// Small bounded maintenance pages; accepted facts and undelivered intents survive.
pub async fn prune(db: &LocalDb, now: i64) -> DbResult<()> {
    db.write(|conn|Box::pin(async move {
        let mut rows=conn.query("SELECT f.rowid FROM request_facts f WHERE NOT EXISTS(SELECT 1 FROM request_attempts a WHERE a.cohort_id=f.cohort_id AND json_extract(a.acceptance_json,'$.facts_version')=f.version) AND (SELECT COUNT(*) FROM request_facts newer WHERE newer.cohort_id=f.cohort_id AND newer.version>f.version)>=8 LIMIT 200",()).await?;
        let mut ids=vec![];while let Some(r)=rows.next().await? {ids.push(r.i64(0)?);}drop(rows);
        for id in ids {conn.execute("DELETE FROM request_facts WHERE rowid=?1",(id,)).await?;}
        let mut rows=conn.query("SELECT id FROM request_outbox WHERE delivered_at IS NOT NULL AND delivered_at<?1 LIMIT 200",params![now-7*24*60*60]).await?;
        let mut ids=vec![];while let Some(r)=rows.next().await? {ids.push(r.text(0)?);}drop(rows);
        for id in ids {conn.execute("DELETE FROM request_outbox WHERE id=?1",(id,)).await?;}
        Ok(())
    })).await
}

/// Consumes an acceptance refused by a live pre-dispatch check. This path is
/// legal only before a consumer claim exists, so no physical actor needs fencing.
pub async fn fail_unclaimed_request(
    db: &LocalDb,
    event: &str,
    expected: &RequestAcceptance,
    receipt: &str,
    report: Value,
    now: i64,
) -> DbResult<RequestAttempt> {
    let event_owned = event.to_string();
    let expected_owned = expected.clone();
    let receipt_owned = receipt.to_string();

    if !nonblank(receipt) {
        return Err(error("pre-dispatch refusal requires stable receipt id"));
    }
    let wire = encode(&report)?;
    db.exclusive(move |conn| {
        let wire=wire.clone();let report=report.clone();
        let event=event_owned.clone(); let expected=expected_owned.clone(); let receipt=receipt_owned.clone(); Box::pin(async move {let event=event.as_str(); let expected=&expected; let receipt=receipt.as_str();
            let existing=attempt_conn(conn,event).await?.ok_or_else(||error("acceptance missing"))?;
            let mut rows=conn.query("SELECT attempt_id,claim_id,report_json FROM request_report_receipts WHERE id=?1",params![receipt]).await?;
            if let Some(r)=rows.next().await? {
                if r.text(0)?!=expected.attempt_id||!r.text(1)?.is_empty()||r.text(2)?!=wire||existing.acceptance!=*expected {return Err(error("conflicting pre-dispatch receipt"));}
                return Ok(existing);
            }
            drop(rows);
            let a=checked(conn,event,expected).await?;
            if a.state!=RequestState::Accepted||a.claim_id.is_some() {return Err(error("claimed acceptance cannot use pre-dispatch refusal"));}
            conn.execute("INSERT INTO request_report_receipts(id,attempt_id,claim_id,report_json,created_at) VALUES(?1,?2,'',?3,?4)",params![receipt,a.id.clone(),wire,now]).await?;
            conn.execute("UPDATE request_attempts SET state='failed',settled=1,outcome='failed',report_json=?2,updated_at=?3 WHERE id=?1 AND state='accepted' AND claim_id IS NULL",params![a.id.clone(),encode(&report)?,now]).await?;
            for m in &a.acceptance.request_ids {
                conn.execute("UPDATE requests SET report_json=?2,updated_at=?3 WHERE id=?1",params![m.clone(),encode(&report)?,now]).await?;
            }
            reopen(conn,&a,now).await?;
            changed_many(conn,&a.acceptance.request_ids,true,now).await?;
            attempt_conn(conn,event).await?.ok_or_else(||error("attempt missing"))
        })
    }).await
}

/// Attempts of one kind whose consumer claim exists but whose effect is not
/// positively settled: acting, or failed without settlement. A consumer
/// reconciles these after a restart; it never replays them.
pub async fn unresolved_claims(db: &LocalDb, kind: RequestKind) -> DbResult<Vec<RequestAttempt>> {
    let kind = kind.as_str().to_string();
    db.read(move |conn| {
        let kind = kind.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    format!("SELECT {ATTEMPT_COLUMNS} FROM request_attempts a WHERE (a.state='acting' OR (a.state='failed' AND a.settled=0)) AND a.claim_id IS NOT NULL AND json_extract(a.acceptance_json,'$.kind')=?1 ORDER BY a.updated_at,a.id"),
                    params![kind.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(attempt_row(&row)?);
            }
            Ok(out)
        })
    })
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchDisposition {
    Dispatch,
    Complete,
    Unresolved,
}
/// Checked historical disposition permits outbox acknowledgement after a crash
/// between a terminal report and acknowledgement, but never retries an actor.
pub async fn dispatch_disposition(db: &LocalDb, event: &str) -> DbResult<DispatchDisposition> {
    let event_owned = event.to_string();

    db.read(move |conn| {
        let event = event_owned.clone();
        Box::pin(async move {
            let event = event.as_str();
            let a = attempt_conn(conn, event)
                .await?
                .ok_or_else(|| error("unknown dispatch acceptance"))?;
            let a = checked_record(conn, event, &a.acceptance, false).await?;
            let settled = text(
                conn,
                "SELECT CAST(settled AS TEXT) FROM request_attempts WHERE id=?1",
                params![a.id.clone()],
            )
            .await?
            .as_deref()
                == Some("1");
            match a.state {
                RequestState::Accepted if a.claim_id.is_none() => Ok(DispatchDisposition::Dispatch),
                RequestState::Done if settled && a.outcome == Some(RequestAttemptOutcome::Done) => {
                    Ok(DispatchDisposition::Complete)
                }
                RequestState::Failed if settled => Ok(DispatchDisposition::Complete),
                RequestState::Acting | RequestState::Failed => Ok(DispatchDisposition::Unresolved),
                _ => Err(error("inconsistent dispatch attempt state")),
            }
        })
    })
    .await
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ReportReceiptWire {
    outcome: RequestAttemptOutcome,
    report: Value,
    settled: bool,
    covered: Vec<String>,
}

fn frozen_report_coverage(
    wire: &str,
    outcome: &RequestAttemptOutcome,
    document: &Value,
    settled: bool,
) -> DbResult<Vec<String>> {
    let frozen: ReportReceiptWire = decode(wire)?;
    if frozen.outcome != *outcome || frozen.report != *document || frozen.settled != settled {
        return Err(error("conflicting consumer receipt bytes"));
    }
    if frozen.covered.iter().any(|id| !nonblank(id))
        || (frozen.outcome == RequestAttemptOutcome::Done && !frozen.settled)
    {
        return Err(error("consumer receipt evidence is malformed"));
    }
    // Coverage is a host-derived decision, not consumer input. Concurrent
    // redelivery may observe a different OPEN set after the first fulfillment.
    // The first committed receipt wins; replay cannot add any later coverage.
    Ok(frozen.covered)
}

/// Reuse the first receipt's derived coverage on an identical consumer report.
/// Recomputing it after fulfillment would depend on a different set of OPEN
/// requests and turn an ordinary redelivery into conflicting receipt bytes.
#[allow(clippy::too_many_arguments)]
pub async fn report_receipt_coverage(
    db: &LocalDb,
    attempt: &str,
    claim: &str,
    receipt: &str,
    outcome: &RequestAttemptOutcome,
    document: &Value,
    settled: bool,
) -> DbResult<Option<Vec<String>>> {
    let (attempt, claim, receipt) = (attempt.to_string(), claim.to_string(), receipt.to_string());
    let outcome = outcome.clone();
    let document = document.clone();
    db.read(move |conn| {
        let (attempt, claim, receipt, outcome, document) = (
            attempt.clone(),
            claim.clone(),
            receipt.clone(),
            outcome.clone(),
            document.clone(),
        );
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT attempt_id,claim_id,report_json FROM request_report_receipts WHERE id=?1",
                params![receipt.as_str()],
            ).await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            if row.text(0)? != attempt || row.text(1)? != claim {
                return Err(error("conflicting consumer receipt attribution"));
            }
            frozen_report_coverage(&row.text(2)?, &outcome, &document, settled).map(Some)
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::super::{MigrationRunner, TEAM_MIGRATIONS, TURSO_MIGRATIONS};
    use super::*;
    use cairn_common::authorization::{
        ActBinding, ActDigest, AuthorityAction, AuthorityAudience, AuthorityPlace,
        AuthorityPrincipal, AuthorityScope,
    };
    use cairn_common::identity::{
        Address, AppearanceEvidence, VerificationMethod, VerificationRecord, VerificationStrength,
    };
    async fn db() -> LocalDb {
        super::super::migrated_test_db("operator-requests.db").await
    }
    fn input(kind: RequestKind, origin: &str, minimum: Option<&str>) -> NormalizedRequest {
        NormalizedRequest {
            workspace_id: "default".into(),
            kind,
            target: minimum.map(str::to_owned),
            target_identity: format!("test:{}", kind.as_str()),
            coalescing_key: Some(format!("test:{}", kind.as_str())),
            contribution: RequestContribution {
                id: String::new(),
                request_id: String::new(),
                origin_uri: origin.into(),
                run_id: "run".into(),
                principal: PrincipalRef::Agent {
                    node: "cairn://p/cairn/1/1/builder".into(),
                    run_id: Some("run".into()),
                },
                wake_destination: "cairn://p/cairn/devops".into(),
                rationale: format!("please {origin}"),
                waiting: vec!["cairn://p/cairn/1".into()],
                requirements: RequestRequirements {
                    commits: vec!["a".repeat(40)],
                },
                minimum_target: minimum.map(str::to_owned),
                normalized_target: json!({"minimum":minimum}),
                active: true,
                satisfied_by_attempt: None,
                created_at: 0,
            },
        }
    }
    fn proof() -> (PrincipalRef, AppearanceSnapshot) {
        let p = PrincipalRef::Human {
            issuer: "https://example.test".into(),
            subject: "operator".into(),
            organization: None,
        };
        let verification = VerificationRecord::new(
            VerificationMethod::JwtOperator,
            VerificationStatus::Verified,
            Some("https://example.test".into()),
            Some("operator".into()),
            None,
            None,
            VerificationStrength::new("strong").unwrap(),
            1,
        )
        .unwrap();
        let evidence = AppearanceEvidence::new(
            AppearanceTransport::AuthenticatedOperator,
            Address::Invoke { origin: None },
            verification,
            1,
            None,
        )
        .unwrap();
        (
            p.clone(),
            AppearanceSnapshot::new(p, evidence, vec![], None).unwrap(),
        )
    }
    async fn envelope(db: &LocalDb, request: &str) -> RequestAcceptance {
        let r = get_request(db, request).await.unwrap().unwrap();
        let f = refresh_facts(
            db,
            request,
            r.revision,
            1,
            json!({"candidate":"a".repeat(40)}),
            2,
        )
        .await
        .unwrap();
        let (operator, appearance) = proof();
        RequestAcceptance {
            schema_version: 1,
            install_id: "test-install".into(),
            workspace_id: r.workspace_id,
            cohort_id: r.cohort_id,
            request_ids: r.member_request_ids,
            attempt_id: id(),
            actor_id: "test-actor".into(),
            kind: r.kind,
            target: r.target,
            target_identity: r.target_identity,
            binding: ActBinding::fresh(&ActDigest::of("request", |e| e.field("id", request))),
            facts_version: f.version,
            revision: r.revision,
            candidate_commit: Some("a".repeat(40)),
            base_commit: Some("b".repeat(40)),
            policy_project_id: Some("canonical".into()),
            drain: None,
            operator,
            appearance,
            accepted_at: 3,
        }
    }
    async fn count(db: &LocalDb, table: &str) -> i64 {
        db.query_one(format!("SELECT COUNT(*) FROM {table}"), (), |r| r.i64(0))
            .await
            .unwrap()
    }
    #[tokio::test]
    async fn filing_is_coalesced_idempotent_and_preserves_distinct_rationales() {
        let db = db().await;
        let a = input(RequestKind::Rebuild, "a", None);
        let first = file_request(&db, &a, 1).await.unwrap();
        let again = file_request(&db, &a, 2).await.unwrap();
        assert!(first.created);
        assert!(!again.created);
        assert_eq!(first.request.id, again.request.id);
        assert_eq!(first.request.revision, again.request.revision);
        assert_eq!(again.request.requesters.len(), 1);
        let mut other = a.clone();
        other.contribution.rationale = "another reason".into();
        let joined = file_request(&db, &other, 3).await.unwrap();
        assert_eq!(joined.request.requesters.len(), 2);
        assert!(joined.request.revision > again.request.revision);
        assert_eq!(count(&db, "request_cohorts").await, 1);
        assert_eq!(count(&db, "request_waiting").await, 2);
        assert_eq!(count(&db, "authorization_events").await, 0);
    }
    #[tokio::test]
    async fn joins_and_withdrawals_recompute_fixed_bump_minima() {
        let db = db().await;
        let a = file_request(&db, &input(RequestKind::BumpVersion, "a", Some("2.9.0")), 1)
            .await
            .unwrap();
        let b = file_request(
            &db,
            &input(RequestKind::BumpVersion, "b", Some("2.10.0")),
            2,
        )
        .await
        .unwrap();
        assert_eq!(b.request.id, a.request.id);
        assert_eq!(b.request.target.as_deref(), Some("2.10.0"));
        let r = withdraw_request(&db, &a.request.id, "b", 3).await.unwrap();
        assert_eq!(r.target.as_deref(), Some("2.9.0"));
        assert_eq!(r.state, RequestState::Open);
        let r = withdraw_request(&db, &a.request.id, "a", 4).await.unwrap();
        assert_eq!(r.state, RequestState::Withdrawn);
        assert_eq!(r.requesters.iter().filter(|c| c.active).count(), 0);
    }
    #[tokio::test]
    async fn acceptance_is_atomic_with_journal_members_and_dispatch() {
        let db = db().await;
        let filed = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let a = envelope(&db, &filed.request.id).await;
        db.exclusive(|c|Box::pin(async move{c.execute_batch("CREATE TRIGGER reject_dispatch BEFORE INSERT ON request_outbox WHEN NEW.kind='dispatch' BEGIN SELECT RAISE(ABORT,'injected dispatch failure'); END;").await?;Ok(())})).await.unwrap();
        assert!(accept_request(&db, &a).await.is_err());
        assert_eq!(count(&db, "authorization_events").await, 0);
        assert_eq!(count(&db, "request_attempts").await, 0);
        assert_eq!(
            get_request(&db, &filed.request.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            RequestState::Open
        );
        db.exclusive(|c| {
            Box::pin(async move {
                c.execute_batch("DROP TRIGGER reject_dispatch;").await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        let attempt = accept_request(&db, &a).await.unwrap();
        assert_eq!(attempt.acceptance, a);
        assert_eq!(count(&db, "request_attempt_members").await, 1);
        let journal = authority::list_events(&db, "default", 10).await.unwrap();
        assert_eq!(journal[0].outcome, "request_accepted");
        assert_eq!(journal[0].request_acceptance, Some(a));
        assert!(journal[0].grant_id.is_none());
        assert_eq!(count(&db, "authority_grants").await, 0);
        assert!(pending_outbox(&db, 200)
            .await
            .unwrap()
            .iter()
            .any(|o| o.kind == "dispatch"));
    }
    #[tokio::test]
    async fn stale_views_and_changed_facts_are_refused() {
        let db = db().await;
        let filed = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let a = envelope(&db, &filed.request.id).await;
        file_request(&db, &input(RequestKind::Rebuild, "b", None), 4)
            .await
            .unwrap();
        assert!(accept_request(&db, &a).await.is_err());
        let mut fresh = envelope(&db, &filed.request.id).await;
        refresh_facts(
            &db,
            &filed.request.id,
            fresh.revision,
            1,
            json!({"new":true}),
            5,
        )
        .await
        .unwrap();
        assert!(accept_request(&db, &fresh).await.is_err());
        fresh.facts_version += 1;
        assert!(accept_request(&db, &fresh).await.is_ok());
    }
    #[tokio::test]
    async fn claim_compares_every_frozen_field_and_rejects_duplicate_and_replay() {
        let db = db().await;
        let filed = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let a = envelope(&db, &filed.request.id).await;
        let attempt = accept_request(&db, &a).await.unwrap();
        let original = serde_json::to_value(&a).unwrap();
        let mut wrong_operator = original["operator"].clone();
        wrong_operator["subject"] = json!("other");
        for key in original.as_object().unwrap().keys() {
            let mut missing = original.clone();
            missing.as_object_mut().unwrap().remove(key);
            assert!(
                serde_json::from_value::<RequestAcceptance>(missing).is_err(),
                "missing required envelope field {key}"
            );
        }
        for (field, value) in [
            ("install_id", json!("other")),
            ("workspace_id", json!("other")),
            ("cohort_id", json!("other")),
            ("request_ids", json!(["other"])),
            ("attempt_id", json!("other")),
            ("actor_id", json!("other")),
            ("kind", json!("lift_drain")),
            (
                "binding",
                json!({"algorithm":"sha256","encoding_version":2,"salt":"00","digest":"00"}),
            ),
            (
                "drain",
                json!({"executor_id":"other","drain_key":"other","owner":"operator","generation":1,"machine_revision":1}),
            ),
            ("operator", wrong_operator),
            ("target", json!("other")),
            ("target_identity", json!("other")),
            ("facts_version", json!(999)),
            ("revision", json!(999)),
            ("candidate_commit", json!("other")),
            ("base_commit", json!("other")),
            ("policy_project_id", json!("other")),
            ("accepted_at", json!(999)),
            ("schema_version", json!(999)),
        ] {
            let mut altered = original.clone();
            altered[field] = value;
            let altered: RequestAcceptance = serde_json::from_value(altered).unwrap();
            assert!(
                claim_request(&db, &attempt.authorization_event_id, &altered, "claim", 4)
                    .await
                    .is_err(),
                "{field}"
            );
        }
        assert!(claim_request(&db, "missing", &a, "claim", 4).await.is_err());
        claim_request(&db, &attempt.authorization_event_id, &a, "claim", 4)
            .await
            .unwrap();
        assert!(
            claim_request(&db, &attempt.authorization_event_id, &a, "claim", 4)
                .await
                .is_err()
        );
        assert!(
            recheck_request(&db, &attempt.authorization_event_id, &a, "other")
                .await
                .is_err()
        );
        report_request(
            &db,
            &a.attempt_id,
            "claim",
            "done",
            RequestAttemptOutcome::Done,
            json!({"boot":"verified"}),
            true,
            5,
        )
        .await
        .unwrap();
        assert!(
            recheck_request(&db, &attempt.authorization_event_id, &a, "claim")
                .await
                .is_err()
        );
        assert!(
            request_gate_state(&db, &filed.request.id)
                .await
                .unwrap()
                .unwrap()
                .accepted
        );
    }
    #[tokio::test]
    async fn unsettled_failure_retains_fence_until_settlement_then_merges_successor() {
        let db = db().await;
        let old = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let a = envelope(&db, &old.request.id).await;
        let attempt = accept_request(&db, &a).await.unwrap();
        claim_request(&db, &attempt.authorization_event_id, &a, "claim", 4)
            .await
            .unwrap();
        let next = file_request(&db, &input(RequestKind::Rebuild, "b", None), 5)
            .await
            .unwrap();
        assert_ne!(next.request.id, old.request.id);
        let b = envelope(&db, &next.request.id).await;
        assert!(accept_request(&db, &b).await.is_err());
        report_request(
            &db,
            &a.attempt_id,
            "claim",
            "unknown",
            RequestAttemptOutcome::Inconclusive,
            json!({"liveness":"unknown"}),
            false,
            6,
        )
        .await
        .unwrap();
        assert_eq!(
            get_request(&db, &old.request.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            RequestState::Failed
        );
        assert!(
            recheck_request(&db, &attempt.authorization_event_id, &a, "claim")
                .await
                .is_err()
        );
        assert!(accept_request(&db, &b).await.is_err());
        report_request(
            &db,
            &a.attempt_id,
            "claim",
            "settled",
            RequestAttemptOutcome::Failed,
            json!({"fenced":true}),
            true,
            7,
        )
        .await
        .unwrap();
        let reopened = get_request(&db, &old.request.id).await.unwrap().unwrap();
        assert_eq!(reopened.state, RequestState::Open);
        assert_eq!(reopened.cohort_id, next.request.cohort_id);
        assert_eq!(reopened.member_request_ids.len(), 2);
        let fresh = envelope(&db, &old.request.id).await;
        assert_eq!(fresh.request_ids.len(), 2);
        accept_request(&db, &fresh).await.unwrap();
        assert_eq!(count(&db, "request_attempts").await, 2);
    }
    #[tokio::test]
    async fn receipts_are_idempotent_but_conflicting_bytes_are_refused() {
        let db = db().await;
        let r = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let a = envelope(&db, &r.request.id).await;
        let attempt = accept_request(&db, &a).await.unwrap();
        claim_request(&db, &attempt.authorization_event_id, &a, "claim", 4)
            .await
            .unwrap();
        for _ in 0..2 {
            report_request(
                &db,
                &a.attempt_id,
                "claim",
                "receipt",
                RequestAttemptOutcome::Done,
                json!({"ok":true}),
                true,
                5,
            )
            .await
            .unwrap();
        }
        assert_eq!(count(&db, "request_report_receipts").await, 1);
        let done = get_request(&db, &r.request.id).await.unwrap().unwrap();
        assert_eq!(done.state, RequestState::Done);
        assert_eq!(
            done.fulfilled_by_attempt.as_deref(),
            Some(a.attempt_id.as_str())
        );
        assert!(report_request(
            &db,
            &a.attempt_id,
            "claim",
            "receipt",
            RequestAttemptOutcome::Done,
            json!({"ok":false}),
            true,
            5
        )
        .await
        .is_err());
    }
    #[tokio::test]
    async fn recompute_keeps_a_request_done_by_its_own_attempt_done() {
        let db = db().await;
        let r = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let a = envelope(&db, &r.request.id).await;
        let attempt = accept_request(&db, &a).await.unwrap();
        claim_request(&db, &attempt.authorization_event_id, &a, "claim", 4)
            .await
            .unwrap();
        report_request(
            &db,
            &a.attempt_id,
            "claim",
            "done",
            RequestAttemptOutcome::Done,
            json!({"ok":true}),
            true,
            5,
        )
        .await
        .unwrap();
        let cohort = a.cohort_id.clone();
        db.exclusive(move |conn| {
            let cohort = cohort.clone();
            Box::pin(async move { recompute(conn, &cohort, 6).await })
        })
        .await
        .unwrap();
        let done = get_request(&db, &r.request.id).await.unwrap().unwrap();
        assert_eq!(done.state, RequestState::Done);
        assert!(done
            .requesters
            .iter()
            .all(|c| c.satisfied_by_attempt.as_deref() == Some(a.attempt_id.as_str())));
    }
    #[tokio::test]
    async fn bump_report_links_only_proven_contributions_atomically_without_acceptance() {
        let db = db().await;
        let rebuild = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        file_request(&db, &input(RequestKind::Rebuild, "b", None), 1)
            .await
            .unwrap();
        let ids = get_request(&db, &rebuild.request.id)
            .await
            .unwrap()
            .unwrap()
            .requesters
            .iter()
            .map(|c| c.id.clone())
            .collect::<Vec<_>>();
        let bump = file_request(&db, &input(RequestKind::BumpVersion, "c", Some("3.0.0")), 1)
            .await
            .unwrap();
        let a = envelope(&db, &bump.request.id).await;
        let attempt = accept_request(&db, &a).await.unwrap();
        claim_request(&db, &attempt.authorization_event_id, &a, "claim", 4)
            .await
            .unwrap();
        assert!(report_request_with_fulfillment(
            &db,
            &a.attempt_id,
            "claim",
            "bad",
            RequestAttemptOutcome::Done,
            json!({}),
            true,
            &["missing".into()],
            5
        )
        .await
        .is_err());
        assert_eq!(
            get_attempt_by_event(&db, &attempt.authorization_event_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            RequestState::Acting
        );
        report_request_with_fulfillment(
            &db,
            &a.attempt_id,
            "claim",
            "done",
            RequestAttemptOutcome::Done,
            json!({}),
            true,
            &ids[..1],
            5,
        )
        .await
        .unwrap();
        let r = get_request(&db, &rebuild.request.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.state, RequestState::Open);
        assert!(r.current_attempt.is_none());
        assert!(r.attempts.is_empty());
        assert!(
            !request_gate_state(&db, &r.id)
                .await
                .unwrap()
                .unwrap()
                .accepted
        );
        fulfill_rebuild_contributions(&db, &a.attempt_id, &ids[1..], json!({}), 6)
            .await
            .unwrap();
        assert_eq!(
            get_request(&db, &r.id).await.unwrap().unwrap().state,
            RequestState::Done
        );
        assert_eq!(count(&db, "authorization_events").await, 1);
    }
    #[tokio::test]
    async fn generic_journal_append_cannot_write_request_accepted() {
        let db = db().await;
        let event = authority::NewAuthorizationEvent {
            scope: AuthorityScope::new(
                AuthorityPlace::Resource {
                    canonical_uri: "cairn://requests".into(),
                },
                AuthorityAction::Run,
            ),
            mutation: "update".into(),
            summary: "forged".into(),
            outcome: "request_accepted".into(),
            reason: "forged".into(),
            principal: AuthorityPrincipal::default(),
            audience: AuthorityAudience::workspace("default"),
            run_id: None,
            request_uri: None,
            grant_id: None,
            decision_actor: None,
            appearance_snapshot: None,
            request_acceptance: None,
            decided_at: 1,
        };
        assert!(authority::append_event(&db, event)
            .await
            .unwrap_err()
            .to_string()
            .contains("transactional"));
        assert_eq!(count(&db, "authorization_events").await, 0);
    }
    #[tokio::test]
    async fn decline_requires_operator_reason_and_wakes_without_dispatch() {
        let db = db().await;
        let r = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let (p, s) = proof();
        assert!(
            decline_request(&db, &r.request.id, r.request.revision, " ", &p, &s, 2)
                .await
                .is_err()
        );
        let d = decline_request(&db, &r.request.id, r.request.revision, "not now", &p, &s, 2)
            .await
            .unwrap();
        assert_eq!(d.state, RequestState::Declined);
        assert!(db
            .write(|conn| Box::pin(async move {
                conn.execute(
                    "UPDATE requests SET report_json='{}' WHERE state='declined'",
                    (),
                )
                .await?;
                Ok(())
            }))
            .await
            .is_err());
        let items = pending_outbox(&db, 200).await.unwrap();
        assert!(items.iter().any(|i| i.kind == "wake"));
        assert!(!items.iter().any(|i| i.kind == "dispatch"));
    }
    #[tokio::test]
    async fn outbox_retry_rotation_and_retention_preserve_pending_and_accepted_facts() {
        let db = db().await;
        let r = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let before = pending_outbox(&db, 1).await.unwrap().remove(0);
        record_outbox_failure(&db, &before.id, "offline", 100)
            .await
            .unwrap();
        assert_ne!(pending_outbox(&db, 1).await.unwrap()[0].id, before.id);
        let a = envelope(&db, &r.request.id).await;
        accept_request(&db, &a).await.unwrap();
        assert!(db
            .write(|c| {
                let cohort = a.cohort_id.clone();
                Box::pin(async move {
                    c.execute(
                        "DELETE FROM request_facts WHERE cohort_id=?1",
                        params![cohort],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .is_err());
        let unaccepted = file_request(&db, &input(RequestKind::Rebuild, "b", None), 10)
            .await
            .unwrap();
        for n in 0..12 {
            refresh_facts(
                &db,
                &unaccepted.request.id,
                unaccepted.request.revision,
                1,
                json!({"n":n}),
                n,
            )
            .await
            .unwrap();
        }
        acknowledge_outbox(&db, &before.id, 1).await.unwrap();
        let recent = pending_outbox(&db, 1).await.unwrap().remove(0);
        acknowledge_outbox(&db, &recent.id, 3 * 24 * 60 * 60)
            .await
            .unwrap();
        let before_count = count(&db, "request_outbox").await;
        prune(&db, 9 * 24 * 60 * 60).await.unwrap();
        assert_eq!(
            count(&db, "request_outbox").await,
            before_count - 1,
            "eight-day-old delivered item pruned; six-day-old item and pending intents kept"
        );
        assert_eq!(count(&db, "request_facts").await, 9);
        assert!(!pending_outbox(&db, 200).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn closed_member_acceptance_rolls_back_without_journal_attempt_or_dispatch() {
        let db = db().await;
        let r = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let a = envelope(&db, &r.request.id).await;
        withdraw_request(&db, &r.request.id, "a", 4).await.unwrap();
        let before = count(&db, "request_outbox").await;
        assert!(accept_request(&db, &a).await.is_err());
        assert_eq!(count(&db, "request_outbox").await, before);
        assert_eq!(count(&db, "authorization_events").await, 0);
        assert_eq!(count(&db, "request_attempts").await, 0);
    }
    #[tokio::test]
    async fn accepted_inputs_and_facts_refuse_mutation_and_generic_envelopes_are_refused() {
        let db = db().await;
        let r = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let a = envelope(&db, &r.request.id).await;
        let accepted = accept_request(&db, &a).await.unwrap();
        for sql in [
            "UPDATE request_attempts SET acceptance_json='{}'",
            "UPDATE request_facts SET document_json='{}'",
            "DELETE FROM request_attempt_members",
            "UPDATE request_attempt_members SET contributions_json='[]'",
            "DELETE FROM authorization_events",
            "UPDATE authorization_events SET summary='not accepted'",
        ] {
            assert!(
                db.write(|c| Box::pin(async move {
                    c.execute(sql, ()).await?;
                    Ok(())
                }))
                .await
                .is_err(),
                "{sql}"
            );
        }
        let journal = authority::list_events(&db, "default", 1)
            .await
            .unwrap()
            .remove(0);
        let e = authority::NewAuthorizationEvent {
            scope: journal.scope,
            mutation: journal.mutation,
            summary: journal.summary,
            outcome: "direct".into(),
            reason: journal.reason,
            principal: journal.principal,
            audience: journal.audience,
            run_id: None,
            request_uri: journal.request_uri,
            grant_id: None,
            decision_actor: journal.decision_actor,
            appearance_snapshot: journal.appearance_snapshot,
            request_acceptance: Some(a.clone()),
            decided_at: 5,
        };
        assert!(authority::append_event(&db, e).await.is_err());
        assert_eq!(
            get_attempt_by_event(&db, &accepted.authorization_event_id)
                .await
                .unwrap()
                .unwrap()
                .acceptance,
            a
        );
        assert_eq!(count(&db, "authorization_events").await, 1);
    }
    #[tokio::test]
    async fn pre_dispatch_refusal_is_atomic_idempotent_and_never_claims() {
        let db = db().await;
        let r = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let a = envelope(&db, &r.request.id).await;
        let accepted = accept_request(&db, &a).await.unwrap();
        for _ in 0..2 {
            let failed = fail_unclaimed_request(
                &db,
                &accepted.authorization_event_id,
                &a,
                "refusal",
                json!({"policy":"enabled"}),
                4,
            )
            .await
            .unwrap();
            assert_eq!(failed.state, RequestState::Failed);
            assert!(failed.claim_id.is_none());
        }
        assert_eq!(count(&db, "request_report_receipts").await, 1);
        assert_eq!(
            get_request(&db, &r.request.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            RequestState::Open
        );
        assert!(
            claim_request(&db, &accepted.authorization_event_id, &a, "replay", 5)
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn concurrent_filing_and_claims_have_one_winner() {
        let db = db().await;
        let a = input(RequestKind::Rebuild, "a", None);
        let b = input(RequestKind::Rebuild, "b", None);
        let (first, second) = tokio::join!(file_request(&db, &a, 1), file_request(&db, &b, 1));
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first.request.id, second.request.id);
        assert_eq!(count(&db, "requests").await, 1);
        let a = envelope(&db, &first.request.id).await;
        let accepted = accept_request(&db, &a).await.unwrap();
        let (x, y) = tokio::join!(
            claim_request(&db, &accepted.authorization_event_id, &a, "x", 4),
            claim_request(&db, &accepted.authorization_event_id, &a, "y", 4)
        );
        assert_eq!(usize::from(x.is_ok()) + usize::from(y.is_ok()), 1);
    }

    #[tokio::test]
    async fn identical_raw_filing_preserves_original_fixed_minima_after_main_or_version_changes() {
        let db = db().await;
        let mut ask = input(RequestKind::BumpVersion, "origin", Some("2.0.0"));
        ask.contribution.normalized_target = json!({"filing":{"target":"sm","requirements":{"commits":[]}},"minimumVersion":"2.0.0"});
        let first = file_request(&db, &ask, 1).await.unwrap();
        ask.target = Some("3.0.0".into());
        ask.contribution.minimum_target = Some("3.0.0".into());
        ask.contribution.requirements.commits = vec!["b".repeat(40)];
        ask.contribution.normalized_target["minimumVersion"] = json!("3.0.0");
        let again = file_request(&db, &ask, 2).await.unwrap();
        assert_eq!(first.request.id, again.request.id);
        assert_eq!(first.request.revision, again.request.revision);
        assert_eq!(again.request.requesters.len(), 1);
        assert_eq!(again.request.target.as_deref(), Some("2.0.0"));
        assert_eq!(
            again.request.requesters[0].minimum_target.as_deref(),
            Some("2.0.0")
        );
    }
    #[tokio::test]
    async fn dispatch_recovery_validates_history_but_never_retries_unresolved_actor() {
        let db = db().await;
        let r = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let a = envelope(&db, &r.request.id).await;
        let accepted = accept_request(&db, &a).await.unwrap();
        let event = &accepted.authorization_event_id;
        assert_eq!(
            dispatch_disposition(&db, event).await.unwrap(),
            DispatchDisposition::Dispatch
        );
        claim_request(&db, event, &a, "claim", 4).await.unwrap();
        assert_eq!(
            dispatch_disposition(&db, event).await.unwrap(),
            DispatchDisposition::Unresolved
        );
        report_request(
            &db,
            &a.attempt_id,
            "claim",
            "unknown",
            RequestAttemptOutcome::Inconclusive,
            json!({}),
            false,
            5,
        )
        .await
        .unwrap();
        assert_eq!(
            dispatch_disposition(&db, event).await.unwrap(),
            DispatchDisposition::Unresolved
        );
        report_request(
            &db,
            &a.attempt_id,
            "claim",
            "settled",
            RequestAttemptOutcome::Failed,
            json!({}),
            true,
            6,
        )
        .await
        .unwrap();
        assert_eq!(
            dispatch_disposition(&db, event).await.unwrap(),
            DispatchDisposition::Complete
        );
        let next = envelope(&db, &r.request.id).await;
        accept_request(&db, &next).await.unwrap();
        assert_eq!(
            dispatch_disposition(&db, event).await.unwrap(),
            DispatchDisposition::Complete
        );
    }

    #[tokio::test]
    async fn claim_and_report_outbox_failures_roll_back_claim_receipt_and_state() {
        let db = db().await;
        let r = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let a = envelope(&db, &r.request.id).await;
        let accepted = accept_request(&db, &a).await.unwrap();
        let event = &accepted.authorization_event_id;
        db.exclusive(|conn|Box::pin(async move {
            conn.execute_batch("CREATE TRIGGER fail_changes BEFORE INSERT ON request_outbox WHEN NEW.kind='changed' BEGIN SELECT RAISE(ABORT,'injected changed failure'); END;").await?;Ok(())
        })).await.unwrap();
        assert!(claim_request(&db, event, &a, "claim", 4).await.is_err());
        let unchanged = get_attempt_by_event(&db, event).await.unwrap().unwrap();
        assert_eq!(unchanged.state, RequestState::Accepted);
        assert!(unchanged.claim_id.is_none());
        db.exclusive(|conn| {
            Box::pin(async move {
                conn.execute_batch("DROP TRIGGER fail_changes;").await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        claim_request(&db, event, &a, "claim", 4).await.unwrap();
        db.exclusive(|conn|Box::pin(async move {
            conn.execute_batch("CREATE TRIGGER fail_wakes BEFORE INSERT ON request_outbox WHEN NEW.kind='wake' BEGIN SELECT RAISE(ABORT,'injected wake failure'); END;").await?;Ok(())
        })).await.unwrap();
        assert!(report_request(
            &db,
            &a.attempt_id,
            "claim",
            "failed",
            RequestAttemptOutcome::Failed,
            json!({}),
            true,
            5
        )
        .await
        .is_err());
        assert_eq!(count(&db, "request_report_receipts").await, 0);
        assert_eq!(
            get_attempt_by_event(&db, event)
                .await
                .unwrap()
                .unwrap()
                .state,
            RequestState::Acting
        );
        assert_eq!(
            get_request(&db, &r.request.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            RequestState::Acting
        );
    }
    #[tokio::test]
    async fn drain_claim_requires_exact_executor_owner_key_and_generation_and_all_nested_fields() {
        let db = db().await;
        let r = file_request(
            &db,
            &input(
                RequestKind::LiftDrain,
                "a",
                Some("cairn://executors/machine"),
            ),
            1,
        )
        .await
        .unwrap();
        let mut a = envelope(&db, &r.request.id).await;
        a.drain = Some(DrainAcceptance {
            executor_id: "machine-id".into(),
            drain_key: "stable-key".into(),
            owner: "operator".into(),
            generation: 3,
            machine_revision: 5,
        });
        let accepted = accept_request(&db, &a).await.unwrap();
        let original = serde_json::to_value(&a).unwrap();
        for (field, value) in [
            ("executor_id", json!("other")),
            ("drain_key", json!("other")),
            ("owner", json!("other")),
            ("generation", json!(4)),
            ("machine_revision", json!(6)),
        ] {
            let mut changed = original.clone();
            changed["drain"][field] = value;
            let changed: RequestAcceptance = serde_json::from_value(changed).unwrap();
            assert!(
                claim_request(&db, &accepted.authorization_event_id, &changed, "claim", 4)
                    .await
                    .is_err(),
                "{field}"
            );
            let mut missing = original.clone();
            missing["drain"].as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<RequestAcceptance>(missing).is_err(),
                "missing drain {field}"
            );
        }
        claim_request(&db, &accepted.authorization_event_id, &a, "claim", 4)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn requester_filter_through_withdrawn_member_returns_the_active_cohort_card() {
        let db = db().await;
        let old = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let a = envelope(&db, &old.request.id).await;
        let accepted = accept_request(&db, &a).await.unwrap();
        claim_request(&db, &accepted.authorization_event_id, &a, "claim", 4)
            .await
            .unwrap();
        let newer = file_request(&db, &input(RequestKind::Rebuild, "b", None), 5)
            .await
            .unwrap();
        report_request(
            &db,
            &a.attempt_id,
            "claim",
            "failed",
            RequestAttemptOutcome::Failed,
            json!({}),
            true,
            6,
        )
        .await
        .unwrap();
        withdraw_request(&db, &old.request.id, "a", 7)
            .await
            .unwrap();
        let cards = list_requests(
            &db,
            "default",
            &RequestFilter {
                status: Some(RequestState::Open),
                requester: Some("a".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(cards.len(), 1);
        assert_eq!(cards[0].id, newer.request.id);
        assert_eq!(cards[0].state, RequestState::Open);
        assert!(cards[0]
            .requesters
            .iter()
            .any(|c| c.origin_uri == "a" && !c.active));
    }
    #[tokio::test]
    async fn withdrawing_last_uncovered_contribution_finishes_a_partially_fulfilled_card() {
        let db = db().await;
        let rebuild = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        file_request(&db, &input(RequestKind::Rebuild, "b", None), 2)
            .await
            .unwrap();
        let contribution = rebuild.request.requesters[0].id.clone();
        let bump = file_request(&db, &input(RequestKind::BumpVersion, "c", Some("3.0.0")), 3)
            .await
            .unwrap();
        let a = envelope(&db, &bump.request.id).await;
        let accepted = accept_request(&db, &a).await.unwrap();
        claim_request(&db, &accepted.authorization_event_id, &a, "claim", 4)
            .await
            .unwrap();
        report_request_with_fulfillment(
            &db,
            &a.attempt_id,
            "claim",
            "done",
            RequestAttemptOutcome::Done,
            json!({}),
            true,
            &[contribution],
            5,
        )
        .await
        .unwrap();
        let done = withdraw_request(&db, &rebuild.request.id, "b", 6)
            .await
            .unwrap();
        assert_eq!(done.state, RequestState::Done);
        let cards = list_requests(
            &db,
            "default",
            &RequestFilter {
                status: Some(RequestState::Done),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(cards.iter().any(|c| c.id == done.id));
        let gate = request_gate_state(&db, &done.id).await.unwrap().unwrap();
        assert_eq!(gate.state, RequestState::Done);
        assert!(!gate.accepted);
    }

    async fn wake_ids(db: &LocalDb) -> std::collections::BTreeSet<String> {
        pending_outbox(db, 200)
            .await
            .unwrap()
            .into_iter()
            .filter(|i| i.kind == "wake")
            .map(|i| i.id)
            .collect()
    }
    async fn assert_new_wakes(
        db: &LocalDb,
        before: &std::collections::BTreeSet<String>,
        expected: usize,
        members: &[String],
        origins: &[&str],
    ) {
        use std::collections::BTreeSet;
        let wakes: Vec<_> = pending_outbox(db, 200)
            .await
            .unwrap()
            .into_iter()
            .filter(|i| i.kind == "wake" && !before.contains(&i.id))
            .collect();
        assert_eq!(
            wakes.len(),
            expected,
            "one durable intent per routed destination"
        );
        for wake in wakes {
            let ids: BTreeSet<_> = wake.payload["request_ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect();
            assert_eq!(ids, members.iter().map(String::as_str).collect());
            let uris: BTreeSet<_> = wake.payload["request_uris"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect();
            assert_eq!(
                uris,
                members
                    .iter()
                    .map(|id| format!("cairn://requests/{id}"))
                    .collect()
            );
            let actual_origins: BTreeSet<_> = wake.payload["origin_uris"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect();
            assert_eq!(actual_origins, origins.iter().copied().collect());
            assert_eq!(
                wake.payload["requests"].as_array().unwrap().len(),
                members.len()
            );
        }
    }
    async fn two_member_cohort(db: &LocalDb) -> OperatorRequest {
        let old = file_request(db, &input(RequestKind::Rebuild, "a", None), 1)
            .await
            .unwrap();
        let a = envelope(db, &old.request.id).await;
        let accepted = accept_request(db, &a).await.unwrap();
        claim_request(db, &accepted.authorization_event_id, &a, "old-claim", 4)
            .await
            .unwrap();
        file_request(db, &input(RequestKind::Rebuild, "b", None), 5)
            .await
            .unwrap();
        report_request(
            db,
            &a.attempt_id,
            "old-claim",
            "old-failure",
            RequestAttemptOutcome::Failed,
            json!({}),
            true,
            6,
        )
        .await
        .unwrap();
        let card = get_request(db, &old.request.id).await.unwrap().unwrap();
        assert_eq!(card.member_request_ids.len(), 2);
        card
    }
    #[tokio::test]
    async fn multi_member_lifecycle_wakes_are_deduplicated_per_transition_not_per_identity() {
        for transition in [
            "accepted",
            "declined",
            "failed",
            "done",
            "unclaimed-refusal",
        ] {
            let db = db().await;
            let card = two_member_cohort(&db).await;
            let members = card.member_request_ids.clone();
            if transition == "declined" {
                let before = wake_ids(&db).await;
                let (operator, appearance) = proof();
                decline_request(
                    &db,
                    &card.id,
                    card.revision,
                    "not now",
                    &operator,
                    &appearance,
                    10,
                )
                .await
                .unwrap();
                assert_new_wakes(&db, &before, 1, &members, &["a", "b"]).await;
                continue;
            }
            let mut a = envelope(&db, &card.id).await;
            a.accepted_at = 10;
            let before = wake_ids(&db).await;
            let accepted = accept_request(&db, &a).await.unwrap();
            assert_new_wakes(&db, &before, 1, &members, &["a", "b"]).await;
            if transition == "accepted" {
                continue;
            }
            if transition == "unclaimed-refusal" {
                let before = wake_ids(&db).await;
                fail_unclaimed_request(
                    &db,
                    &accepted.authorization_event_id,
                    &a,
                    "refused",
                    json!({"policy":"changed"}),
                    11,
                )
                .await
                .unwrap();
                assert_new_wakes(&db, &before, 1, &members, &["a", "b"]).await;
                continue;
            }
            let before_claim = wake_ids(&db).await;
            let changed_before = db
                .query_one(
                    "SELECT COUNT(*) FROM request_outbox WHERE kind='changed'",
                    (),
                    |r| r.i64(0),
                )
                .await
                .unwrap();
            claim_request(&db, &accepted.authorization_event_id, &a, "claim", 11)
                .await
                .unwrap();
            assert_eq!(
                wake_ids(&db).await,
                before_claim,
                "acting is a changed event, not another lifecycle wake"
            );
            assert_eq!(
                db.query_one(
                    "SELECT COUNT(*) FROM request_outbox WHERE kind='changed'",
                    (),
                    |r| r.i64(0)
                )
                .await
                .unwrap(),
                changed_before + 2
            );
            let outcome = if transition == "done" {
                RequestAttemptOutcome::Done
            } else {
                RequestAttemptOutcome::Failed
            };
            report_request(
                &db,
                &a.attempt_id,
                "claim",
                "terminal",
                outcome.clone(),
                json!({"verified":true}),
                true,
                12,
            )
            .await
            .unwrap();
            assert_new_wakes(&db, &before_claim, 1, &members, &["a", "b"]).await;
            let after = wake_ids(&db).await;
            report_request(
                &db,
                &a.attempt_id,
                "claim",
                "terminal",
                outcome,
                json!({"verified":true}),
                true,
                12,
            )
            .await
            .unwrap();
            assert_eq!(
                wake_ids(&db).await,
                after,
                "receipt replay cannot add another wake"
            );
        }
    }
    #[tokio::test]
    async fn linked_fulfillment_and_failure_notes_share_one_wake_per_destination() {
        for success in [true, false] {
            let db = db().await;
            let rebuild = two_member_cohort(&db).await;
            let covered: Vec<_> = rebuild
                .requesters
                .iter()
                .filter(|c| c.active)
                .map(|c| c.id.clone())
                .collect();
            let bump = file_request(&db, &input(RequestKind::BumpVersion, "c", Some("3.0.0")), 7)
                .await
                .unwrap();
            let mut a = envelope(&db, &bump.request.id).await;
            a.accepted_at = 10;
            let accepted = accept_request(&db, &a).await.unwrap();
            claim_request(&db, &accepted.authorization_event_id, &a, "claim", 11)
                .await
                .unwrap();
            let before = wake_ids(&db).await;
            let outcome = if success {
                RequestAttemptOutcome::Done
            } else {
                RequestAttemptOutcome::Failed
            };
            report_request_with_fulfillment(
                &db,
                &a.attempt_id,
                "claim",
                "report",
                outcome,
                json!({}),
                true,
                if success { &covered } else { &[] },
                12,
            )
            .await
            .unwrap();
            let mut members = rebuild.member_request_ids.clone();
            members.push(bump.request.id);
            assert_new_wakes(&db, &before, 1, &members, &["a", "b", "c"]).await;
        }
    }
    #[tokio::test]
    async fn multi_member_wakes_preserve_distinct_routed_destinations() {
        let db = db().await;
        let card = two_member_cohort(&db).await;
        let mut third = input(RequestKind::Rebuild, "c", None);
        third.contribution.wake_destination = "cairn://p/cairn/execution".into();
        file_request(&db, &third, 7).await.unwrap();
        let mut a = envelope(&db, &card.id).await;
        a.accepted_at = 10;
        let before = wake_ids(&db).await;
        accept_request(&db, &a).await.unwrap();
        let wakes: Vec<_> = pending_outbox(&db, 200)
            .await
            .unwrap()
            .into_iter()
            .filter(|i| i.kind == "wake" && !before.contains(&i.id))
            .collect();
        assert_eq!(wakes.len(), 2);
        let destinations: std::collections::BTreeSet<_> = wakes
            .iter()
            .map(|w| w.destination.as_deref().unwrap())
            .collect();
        assert_eq!(
            destinations,
            ["cairn://p/cairn/devops", "cairn://p/cairn/execution"]
                .into_iter()
                .collect()
        );
        for wake in wakes {
            let origins: Vec<_> = wake.payload["origin_uris"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect();
            assert_eq!(
                origins,
                if wake.destination.as_deref() == Some("cairn://p/cairn/devops") {
                    vec!["a", "b"]
                } else {
                    vec!["c"]
                }
            );
            assert_eq!(wake.payload["request_ids"].as_array().unwrap().len(), 2);
        }
    }
    #[tokio::test]
    async fn terminal_failure_details_cannot_be_upgraded_to_success_or_linked_fulfillment() {
        for initial in [
            RequestAttemptOutcome::Failed,
            RequestAttemptOutcome::RolledBack,
            RequestAttemptOutcome::Cancelled,
            RequestAttemptOutcome::Inconclusive,
        ] {
            let db = db().await;
            let rebuild = file_request(&db, &input(RequestKind::Rebuild, "a", None), 1)
                .await
                .unwrap();
            let covered = vec![rebuild.request.requesters[0].id.clone()];
            let bump = file_request(&db, &input(RequestKind::BumpVersion, "b", Some("3.0.0")), 2)
                .await
                .unwrap();
            let a = envelope(&db, &bump.request.id).await;
            let accepted = accept_request(&db, &a).await.unwrap();
            claim_request(&db, &accepted.authorization_event_id, &a, "claim", 4)
                .await
                .unwrap();
            let failed = report_request(
                &db,
                &a.attempt_id,
                "claim",
                "initial",
                initial.clone(),
                json!({"liveness":"unknown"}),
                false,
                5,
            )
            .await
            .unwrap();
            assert_eq!(failed.state, RequestState::Failed);
            assert_eq!(failed.outcome, Some(initial.clone()));
            let wakes_before = wake_ids(&db).await;
            let receipts_before = count(&db, "request_report_receipts").await;
            let error = report_request_with_fulfillment(
                &db,
                &a.attempt_id,
                "claim",
                "upgrade",
                RequestAttemptOutcome::Done,
                json!({"boot":"late-success"}),
                true,
                &covered,
                6,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("terminal"));
            assert_eq!(
                get_attempt_by_event(&db, &accepted.authorization_event_id)
                    .await
                    .unwrap()
                    .unwrap(),
                failed
            );
            assert_eq!(count(&db, "request_report_receipts").await, receipts_before);
            assert_eq!(wake_ids(&db).await, wakes_before);
            let linked = get_request(&db, &rebuild.request.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(linked.state, RequestState::Open);
            assert!(linked.fulfilled_by_attempt.is_none());
            assert!(linked
                .requesters
                .iter()
                .all(|c| c.satisfied_by_attempt.is_none()));
            assert_eq!(
                request_gate_state(&db, &bump.request.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                RequestState::Failed
            );
            // Exact receipt replay precedes the guard and remains effect-free.
            let replay = report_request(
                &db,
                &a.attempt_id,
                "claim",
                "initial",
                initial.clone(),
                json!({"liveness":"unknown"}),
                false,
                5,
            )
            .await
            .unwrap();
            assert_eq!(replay, failed);
            assert_eq!(wake_ids(&db).await, wakes_before);
            let settled = report_request(
                &db,
                &a.attempt_id,
                "claim",
                "settled",
                initial.clone(),
                json!({"fenced":true}),
                true,
                7,
            )
            .await
            .unwrap();
            assert_eq!(settled.state, RequestState::Failed);
            assert_eq!(settled.outcome, Some(initial));
            assert_eq!(
                get_request(&db, &bump.request.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                RequestState::Open
            );
            assert_eq!(
                get_request(&db, &rebuild.request.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                RequestState::Open
            );
        }
    }

    #[tokio::test]
    async fn authoritative_report_replay_keeps_first_coverage_after_concurrent_prelookup_miss() {
        let db = db().await;
        let rebuild = file_request(&db, &input(RequestKind::Rebuild, "original", None), 1)
            .await
            .unwrap();
        let covered = vec![rebuild.request.requesters[0].id.clone()];
        let bump = file_request(
            &db,
            &input(RequestKind::BumpVersion, "bumper", Some("3.0.0")),
            2,
        )
        .await
        .unwrap();
        let a = envelope(&db, &bump.request.id).await;
        let accepted = accept_request(&db, &a).await.unwrap();
        claim_request(&db, &accepted.authorization_event_id, &a, "claim", 4)
            .await
            .unwrap();
        let document = json!({"evidence":{"verified_boot":"boot"},"builtCommit":"c".repeat(40)});
        // Both callers miss the advisory lookup before the first one commits.
        for _ in 0..2 {
            assert!(report_receipt_coverage(
                &db,
                &a.attempt_id,
                "claim",
                "stable",
                &RequestAttemptOutcome::Done,
                &document,
                true
            )
            .await
            .unwrap()
            .is_none());
        }
        let done = report_request_with_fulfillment(
            &db,
            &a.attempt_id,
            "claim",
            "stable",
            RequestAttemptOutcome::Done,
            document.clone(),
            true,
            &covered,
            5,
        )
        .await
        .unwrap();
        assert_eq!(
            get_request(&db, &rebuild.request.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            RequestState::Done
        );
        let mut late_input = input(RequestKind::Rebuild, "late", None);
        late_input.contribution.requirements.commits = vec!["d".repeat(40)];
        let late = file_request(&db, &late_input, 6).await.unwrap();
        let proposed = vec![late.request.requesters[0].id.clone()];
        assert_ne!(late.request.id, rebuild.request.id);

        let another = file_request(
            &db,
            &input(RequestKind::BumpVersion, "another", Some("4.0.0")),
            7,
        )
        .await
        .unwrap();
        let other_acceptance = envelope(&db, &another.request.id).await;
        let other = accept_request(&db, &other_acceptance).await.unwrap();
        let other = claim_request(
            &db,
            &other.authorization_event_id,
            &other_acceptance,
            "claim",
            8,
        )
        .await
        .unwrap();

        let wire_before = db
            .query_one(
                "SELECT report_json FROM request_report_receipts WHERE id='stable'",
                (),
                |r| r.text(0),
            )
            .await
            .unwrap();
        let outbox_before = serde_json::to_value(pending_outbox(&db, 200).await.unwrap()).unwrap();
        let original_before = get_request(&db, &rebuild.request.id)
            .await
            .unwrap()
            .unwrap();
        let late_before = get_request(&db, &late.request.id).await.unwrap().unwrap();
        // The second caller's derived coverage can be empty or include a new
        // ask. Neither is consumer evidence, and neither can replace the winner.
        for derived in [Vec::new(), proposed] {
            let replay = report_request_with_fulfillment(
                &db,
                &a.attempt_id,
                "claim",
                "stable",
                RequestAttemptOutcome::Done,
                document.clone(),
                true,
                &derived,
                9,
            )
            .await
            .unwrap();
            assert_eq!(replay, done);
            assert_eq!(
                report_receipt_coverage(
                    &db,
                    &a.attempt_id,
                    "claim",
                    "stable",
                    &RequestAttemptOutcome::Done,
                    &document,
                    true
                )
                .await
                .unwrap(),
                Some(covered.clone())
            );
        }
        let mut changed_report = document.clone();
        changed_report["evidence"]["verified_boot"] = json!("other-boot");
        let mut changed_commit = document.clone();
        changed_commit["builtCommit"] = json!("e".repeat(40));
        for changed in [changed_report, changed_commit] {
            assert!(report_request_with_fulfillment(
                &db,
                &a.attempt_id,
                "claim",
                "stable",
                RequestAttemptOutcome::Done,
                changed,
                true,
                &[],
                9
            )
            .await
            .is_err());
        }
        assert!(report_request_with_fulfillment(
            &db,
            &a.attempt_id,
            "claim",
            "stable",
            RequestAttemptOutcome::Failed,
            document.clone(),
            true,
            &[],
            9
        )
        .await
        .is_err());
        assert!(report_request_with_fulfillment(
            &db,
            &a.attempt_id,
            "claim",
            "stable",
            RequestAttemptOutcome::Done,
            document.clone(),
            false,
            &[],
            9
        )
        .await
        .is_err());
        assert!(report_request_with_fulfillment(
            &db,
            &a.attempt_id,
            "wrong-claim",
            "stable",
            RequestAttemptOutcome::Done,
            document.clone(),
            true,
            &[],
            9
        )
        .await
        .is_err());
        assert!(report_request_with_fulfillment(
            &db,
            &other.id,
            "claim",
            "stable",
            RequestAttemptOutcome::Done,
            document,
            true,
            &[],
            9
        )
        .await
        .is_err());
        assert_eq!(count(&db, "request_report_receipts").await, 1);
        assert_eq!(
            db.query_one(
                "SELECT report_json FROM request_report_receipts WHERE id='stable'",
                (),
                |r| r.text(0)
            )
            .await
            .unwrap(),
            wire_before
        );
        assert_eq!(
            get_attempt_by_event(&db, &accepted.authorization_event_id)
                .await
                .unwrap()
                .unwrap(),
            done
        );
        assert_eq!(
            get_attempt_by_event(&db, &other.authorization_event_id)
                .await
                .unwrap()
                .unwrap(),
            other
        );
        assert_eq!(
            get_request(&db, &rebuild.request.id)
                .await
                .unwrap()
                .unwrap(),
            original_before
        );
        assert_eq!(
            get_request(&db, &late.request.id).await.unwrap().unwrap(),
            late_before
        );
        assert_eq!(late_before.state, RequestState::Open);
        assert!(late_before
            .requesters
            .iter()
            .all(|c| c.satisfied_by_attempt.is_none()));
        assert_eq!(
            serde_json::to_value(pending_outbox(&db, 200).await.unwrap()).unwrap(),
            outbox_before
        );
    }

    async fn digest(db: &LocalDb, columns: &[String]) -> (usize, Vec<u8>) {
        let columns = columns.to_vec();
        use sha2::{Digest, Sha256};
        let sql = format!(
            "SELECT {} FROM authorization_events ORDER BY id",
            columns.join(",")
        );
        db.read(move |c| {
            Box::pin(async move {
                let mut rows = c.query(sql, ()).await?;
                let mut h = Sha256::new();
                let mut n = 0;
                while let Some(r) = rows.next().await? {
                    n += 1;
                    for (i, col) in columns.iter().enumerate() {
                        let v = format!("{:?}", r.get_value(i)?);
                        h.update((col.len() as u64).to_be_bytes());
                        h.update(col.as_bytes());
                        h.update((v.len() as u64).to_be_bytes());
                        h.update(v.as_bytes());
                    }
                }
                Ok((n, h.finalize().to_vec()))
            })
        })
        .await
        .unwrap()
    }
    #[tokio::test]
    async fn journal_check_rebuild_preserves_all_evidence_and_immutability() {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("migration.db"))
            .await
            .unwrap();
        let migration = *TURSO_MIGRATIONS
            .iter()
            .find(|m| m.name() == "operator_requests")
            .unwrap();
        assert!(!TEAM_MIGRATIONS
            .iter()
            .any(|m| m.name() == "operator_requests"));
        let prior = TURSO_MIGRATIONS
            .iter()
            .filter(|m| m.version() < migration.version())
            .copied()
            .collect::<Vec<_>>();
        MigrationRunner::new(prior).run(&db).await.unwrap();
        let columns = db
            .query_all(
                "PRAGMA table_info(authorization_events)".to_string(),
                (),
                |r| r.text(1),
            )
            .await
            .unwrap();
        assert_eq!(columns.len(), 18);
        db.exclusive(|c|Box::pin(async move {
            for (id,outcome,nulls) in [("one","direct",false),("two","forbidden",true)] {
                c.execute("INSERT INTO authorization_events(id,scope_key,place_kind,action,scope_json,mutation,summary,outcome,reason,principal_json,audience_json,workspace_id,run_id,request_uri,grant_id,decided_at,actor_principal_json,appearance_snapshot_json) VALUES(?1,'unique-scope','resource','run','{scope}','update','quoted rationale',?2,'unique-reason','{principal}','{audience}','workspace',?3,?4,?5,987654,?6,?7)",
                    params![id,outcome,if nulls{None}else{Some("run")},if nulls{None}else{Some("request")},if nulls{None}else{Some("grant")},if nulls{None}else{Some("{actor}")},if nulls{None}else{Some("{appearance}")}]).await?;
            }
            Ok(())
        })).await.unwrap();
        let before = digest(&db, &columns).await;
        let indexes=db.query_all("SELECT name,sql FROM sqlite_master WHERE type='index' AND tbl_name='authorization_events' AND sql IS NOT NULL ORDER BY name".to_string(),(),|r|Ok((r.text(0)?,r.text(1)?))).await.unwrap();
        MigrationRunner::new(vec![migration])
            .run(&db)
            .await
            .unwrap();
        assert_eq!(before, digest(&db, &columns).await);
        assert_eq!(before.0, 2);
        let after=db.query_all("SELECT name,sql FROM sqlite_master WHERE type='index' AND tbl_name='authorization_events' AND sql IS NOT NULL ORDER BY name".to_string(),(),|r|Ok((r.text(0)?,r.text(1)?))).await.unwrap();
        fn normalized(s: &str) -> String {
            s.chars()
                .filter(|c| !c.is_whitespace() && *c != '"')
                .collect()
        }
        assert_eq!(
            indexes
                .iter()
                .map(|(n, s)| (n, normalized(s)))
                .collect::<Vec<_>>(),
            after
                .iter()
                .map(|(n, s)| (n, normalized(s)))
                .collect::<Vec<_>>()
        );
        for sql in [
            "UPDATE authorization_events SET summary='mutated'",
            "DELETE FROM authorization_events",
        ] {
            assert!(db
                .write(|c| Box::pin(async move {
                    c.execute(sql, ()).await?;
                    Ok(())
                }))
                .await
                .is_err());
        }
        let added=db.query_one("SELECT COUNT(*) FROM authorization_events WHERE request_acceptance_json IS NOT NULL".to_string(),(),|r|r.i64(0)).await.unwrap();
        assert_eq!(added, 0);
    }
}
