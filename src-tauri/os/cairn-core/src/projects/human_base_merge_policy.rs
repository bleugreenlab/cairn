//! The operator act that enables, disables, or re-points a project's
//! provably-human base-merge policy.
//!
//! Enabling the policy and choosing the phone are one act: the stored policy
//! names exactly one approval enrollment, which must be live when chosen. Each
//! act writes the new policy and one append-only `human_base_merge_policy_events`
//! row, attributed to the operator who acted, in one transaction on the
//! project's own database. There is no other writer: project updates and agent
//! mutations refuse the policy field.

use cairn_common::identity::{AppearanceSnapshot, PrincipalPosition, PrincipalRef};
use cairn_db::models::{HumanBaseMergePolicy, HumanBaseMergePolicyReadState};
use cairn_db::turso::params;
use serde::Serialize;

use crate::approvals_client::{ApprovalsClient, EnrollmentState};
use crate::storage::LocalDb;

/// What the operator asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyRequest {
    Disable,
    Enable { approval_enrollment_id: String },
}

/// A policy as the settings surface shows it, with the selected phone's live
/// standing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyView {
    /// `enabled`, `disabled`, or `unavailable` (stored but unreadable).
    pub state: &'static str,
    pub approval_enrollment_id: Option<String>,
    /// The selected phone's standing: `live`, `revoked`, `reset`, `missing`
    /// (the organization has no such enrollment), or `unknown` (the service
    /// could not be asked). `None` when no phone is selected.
    pub enrollment_state: Option<&'static str>,
}

async fn stored_policy(db: &LocalDb, project_id: &str) -> Result<String, String> {
    db.query_opt_text(
        "SELECT human_base_merge_policy FROM projects WHERE id = ?1",
        (project_id.to_string(),),
    )
    .await
    .map_err(|error| format!("the project's policy is unreadable: {error}"))?
    .ok_or_else(|| format!("Unknown project: {project_id}"))
}

async fn enrollment_state(client: Option<&ApprovalsClient<'_>>, id: &str) -> &'static str {
    let Some(client) = client else {
        return "unknown";
    };
    match client.enrollment(id).await {
        Ok(Some(standing)) => match standing.state {
            EnrollmentState::Live => "live",
            EnrollmentState::Revoked => "revoked",
            EnrollmentState::Reset => "reset",
        },
        Ok(None) => "missing",
        Err(_) => "unknown",
    }
}

/// The project's policy and its selected phone's standing.
pub async fn read_policy(
    db: &LocalDb,
    client: Option<&ApprovalsClient<'_>>,
    project_id: &str,
) -> Result<PolicyView, String> {
    let stored = stored_policy(db, project_id).await?;
    Ok(match HumanBaseMergePolicyReadState::from_stored(&stored) {
        HumanBaseMergePolicyReadState::Available { policy } => {
            let enrollment = match policy.approval_enrollment_id.as_deref() {
                Some(id) => Some(enrollment_state(client, id).await),
                None => None,
            };
            PolicyView {
                state: if policy.is_enabled() {
                    "enabled"
                } else {
                    "disabled"
                },
                approval_enrollment_id: policy.approval_enrollment_id,
                enrollment_state: enrollment,
            }
        }
        HumanBaseMergePolicyReadState::Unavailable { .. } => PolicyView {
            state: "unavailable",
            approval_enrollment_id: None,
            enrollment_state: None,
        },
    })
}

/// Apply one operator act. `client` is the runner's approvals client, needed to
/// confirm a phone is live; `repository` is the project's GitHub `owner/name`
/// when it has one, because an approval intent must name a repository.
pub async fn set_policy(
    db: &LocalDb,
    client: Option<&ApprovalsClient<'_>>,
    project_id: &str,
    repository: Option<&str>,
    request: PolicyRequest,
    actor: &PrincipalRef,
    appearance: &AppearanceSnapshot,
) -> Result<PolicyView, String> {
    actor
        .validate_at(PrincipalPosition::DecisionActor)
        .map_err(|error| format!("the operator cannot be attributed: {error}"))?;
    appearance
        .validate()
        .map_err(|error| format!("the operator's appearance is invalid: {error}"))?;
    if appearance.principal() != actor {
        return Err("the operator's appearance names someone else".to_string());
    }
    stored_policy(db, project_id).await?;
    // Every network check finishes before the transaction opens.
    let (policy, installation_id) = match request {
        PolicyRequest::Disable => (HumanBaseMergePolicy::DISABLED_V1, None),
        PolicyRequest::Enable {
            approval_enrollment_id,
        } => {
            if repository.is_none() {
                return Err(
                    "Phone approval of base merges needs the project to be a GitHub repository, \
                     because the phone is shown which repository it is merging into."
                        .to_string(),
                );
            }
            let client = client.ok_or_else(|| {
                "Phone approval needs this runner to be registered with Cairn's cloud; it has \
                 no approvals credential."
                    .to_string()
            })?;
            let standing = client
                .enrollment(&approval_enrollment_id)
                .await
                .map_err(|error| format!("the approval phone could not be checked: {error}"))?;
            let standing = match standing {
                Some(standing) if standing.state == EnrollmentState::Live => standing,
                Some(_) => {
                    return Err(
                        "That phone's approval key was revoked or reset; choose a phone \
                                that is enrolled now."
                            .to_string(),
                    )
                }
                None => return Err("That phone is not enrolled in this organization.".to_string()),
            };
            (
                HumanBaseMergePolicy::enabled_v1(approval_enrollment_id),
                Some(standing.installation_id),
            )
        }
    };
    let encoded = policy.encode();
    let actor_json = serde_json::to_string(actor).map_err(|error| error.to_string())?;
    let appearance_json = serde_json::to_string(appearance).map_err(|error| error.to_string())?;
    let project = project_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(move |conn| {
        let (project, encoded, installation_id, actor_json, appearance_json) = (
            project.clone(),
            encoded.clone(),
            installation_id.clone(),
            actor_json.clone(),
            appearance_json.clone(),
        );
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT human_base_merge_policy FROM projects WHERE id = ?1",
                    (project.as_str(),),
                )
                .await?;
            let prior = crate::storage::next_text(&mut rows, 0)
                .await?
                .unwrap_or_default();
            drop(rows);
            conn.execute(
                "UPDATE projects SET human_base_merge_policy = ?1 WHERE id = ?2",
                params![encoded.as_str(), project.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO human_base_merge_policy_events (
                    event_id, project_id, prior_policy_json, policy_json, installation_id,
                    actor_principal_json, appearance_snapshot_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    uuid::Uuid::new_v4().to_string(),
                    project.as_str(),
                    prior.as_str(),
                    encoded.as_str(),
                    installation_id.as_deref(),
                    actor_json.as_str(),
                    appearance_json.as_str(),
                    now,
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("the policy was not changed: {error}"))?;
    read_policy(db, client, project_id).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{ApiConfig, RunnerCredential};
    use crate::services::testing::MockHttpClient;
    use crate::services::HttpResponse;
    use crate::storage::RowExt;
    use cairn_common::identity::{
        Address, AppearanceEvidence, AppearanceTransport, CredentialRef, VerificationMethod,
        VerificationRecord, VerificationStatus, VerificationStrength,
    };

    async fn db() -> LocalDb {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(dir.keep().join("policy.db"))
            .await
            .unwrap();
        db.execute(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'W', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project', 'w', 'P', 'project', '/tmp/repo', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db
    }

    fn operator() -> (PrincipalRef, AppearanceSnapshot) {
        let principal = PrincipalRef::Machine {
            device_id: "device-1".into(),
        };
        let verification = VerificationRecord::new(
            VerificationMethod::DesktopCredential,
            VerificationStatus::Verified,
            None,
            None,
            None,
            Some(CredentialRef::new("desktop_operator_credential").unwrap()),
            VerificationStrength::new("local_shared_secret").unwrap(),
            1,
        )
        .unwrap();
        let evidence = AppearanceEvidence::new(
            AppearanceTransport::AuthenticatedDesktop,
            Address::Desktop {
                device_id: "device-1".into(),
            },
            verification,
            1,
            None,
        )
        .unwrap();
        let snapshot =
            AppearanceSnapshot::new(principal.clone(), evidence, Vec::new(), None).unwrap();
        (principal, snapshot)
    }

    fn client(http: &MockHttpClient) -> ApprovalsClient<'_> {
        use base64::Engine as _;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"sub":"runner:runner-1","org_id":"org-1"}"#);
        ApprovalsClient::new(
            http,
            ApiConfig {
                base_url: "https://api.policy.test".into(),
            },
            RunnerCredential::parse(&format!("e30.{payload}.sig")).unwrap(),
        )
    }

    fn standing(state: &str) -> MockHttpClient {
        MockHttpClient::new().respond_to(
            "/runner/enrollments/",
            HttpResponse::new(
                200,
                serde_json::json!({
                    "enrollmentId": "cae_1", "installationId": "inst-1",
                    "userId": "user-1", "state": state,
                })
                .to_string()
                .into_bytes(),
            ),
        )
    }

    async fn events(db: &LocalDb) -> Vec<(String, String, Option<String>, String)> {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT prior_policy_json, policy_json, installation_id, actor_principal_json
                           FROM human_base_merge_policy_events ORDER BY created_at, rowid",
                        (),
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push((row.text(0)?, row.text(1)?, row.opt_text(2)?, row.text(3)?));
                }
                Ok(out)
            })
        })
        .await
        .unwrap()
    }

    fn enable() -> PolicyRequest {
        PolicyRequest::Enable {
            approval_enrollment_id: "cae_1".into(),
        }
    }

    #[tokio::test]
    async fn enabling_writes_the_policy_and_one_attributed_journal_row_together() {
        let db = db().await;
        let http = standing("live");
        let (actor, appearance) = operator();
        let view = set_policy(
            &db,
            Some(&client(&http)),
            "project",
            Some("bleugreen/cairn"),
            enable(),
            &actor,
            &appearance,
        )
        .await
        .unwrap();
        assert_eq!(view.state, "enabled");
        assert_eq!(view.enrollment_state, Some("live"));
        assert_eq!(
            stored_policy(&db, "project").await.unwrap(),
            HumanBaseMergePolicy::enabled_v1("cae_1").encode()
        );
        let rows = events(&db).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, HumanBaseMergePolicy::DISABLED_V1.encode());
        assert_eq!(rows[0].2.as_deref(), Some("inst-1"));
        assert_eq!(rows[0].3, serde_json::to_string(&actor).unwrap());

        // Disabling is its own journaled act, and the journal is immutable.
        set_policy(
            &db,
            None,
            "project",
            None,
            PolicyRequest::Disable,
            &actor,
            &appearance,
        )
        .await
        .unwrap();
        let rows = events(&db).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].2, None);
        assert!(db
            .execute(
                "UPDATE human_base_merge_policy_events SET installation_id = 'x'",
                ()
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_phone_that_is_not_live_is_refused_and_nothing_changes() {
        let db = db().await;
        let (actor, appearance) = operator();
        for state in ["revoked", "reset"] {
            let http = standing(state);
            let refusal = set_policy(
                &db,
                Some(&client(&http)),
                "project",
                Some("bleugreen/cairn"),
                enable(),
                &actor,
                &appearance,
            )
            .await
            .unwrap_err();
            assert!(refusal.contains("revoked or reset"), "{refusal}");
        }
        let missing = MockHttpClient::new().respond_to(
            "/runner/enrollments/",
            HttpResponse::new(404, b"{}".to_vec()),
        );
        assert!(set_policy(
            &db,
            Some(&client(&missing)),
            "project",
            Some("bleugreen/cairn"),
            enable(),
            &actor,
            &appearance,
        )
        .await
        .is_err());
        // No credential, and no GitHub repository, refuse too.
        assert!(set_policy(
            &db,
            None,
            "project",
            Some("o/r"),
            enable(),
            &actor,
            &appearance
        )
        .await
        .is_err());
        let live = standing("live");
        assert!(set_policy(
            &db,
            Some(&client(&live)),
            "project",
            None,
            enable(),
            &actor,
            &appearance
        )
        .await
        .unwrap_err()
        .contains("GitHub repository"));
        assert!(events(&db).await.is_empty());
        assert_eq!(
            stored_policy(&db, "project").await.unwrap(),
            HumanBaseMergePolicy::DISABLED_V1.encode()
        );
    }

    #[tokio::test]
    async fn the_view_reports_a_retired_selected_phone() {
        let db = db().await;
        let (actor, appearance) = operator();
        let live = standing("live");
        set_policy(
            &db,
            Some(&client(&live)),
            "project",
            Some("o/r"),
            enable(),
            &actor,
            &appearance,
        )
        .await
        .unwrap();
        let revoked = standing("revoked");
        let view = read_policy(&db, Some(&client(&revoked)), "project")
            .await
            .unwrap();
        assert_eq!(view.state, "enabled");
        assert_eq!(view.enrollment_state, Some("revoked"));
    }
}
