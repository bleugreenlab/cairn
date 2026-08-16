use cairn_common::uri::parse_uri;

use super::{Presence, RouteContext, RouteFact};
use crate::models::Label;
use crate::orchestrator::{AttentionEvent, Orchestrator};

const ATTENTION_QUEUE: usize = 256;

/// Every spelling by which a predicate can name one of the issue's labels.
///
/// A label reference is its slug id or its display name everywhere else a label
/// is named — the label sink resolves either (`labels::attach`), and so does
/// model routing (`config::model_routing`). Predicate matching, by contrast, is
/// plain value equality, so the fact has to carry both spellings for either to
/// match: the route editor picks display names out of the workspace vocabulary,
/// while a hand-written route tends to spell a label by its slug.
fn label_values(labels: &[Label]) -> serde_json::Value {
    let mut seen = std::collections::HashSet::new();
    serde_json::Value::Array(
        labels
            .iter()
            .flat_map(|label| [&label.id, &label.name])
            .filter(|spelling| seen.insert(spelling.as_str()))
            .map(|spelling| serde_json::Value::String(spelling.clone()))
            .collect(),
    )
}

fn attention_fact(
    event: AttentionEvent,
    project: &str,
    labels: &[Label],
    origin: crate::issues::crud::IssueAuthorship,
) -> RouteFact {
    let key = event.fact.key();
    let fact_json = serde_json::to_string(&event.fact).unwrap_or_default();
    let detail_uri = key
        .detail_uri
        .clone()
        .unwrap_or_else(|| event.issue_uri.clone());
    let summary = event.fact.summary();
    RouteFact {
        source: "attention".into(),
        // Attention is a state fact, not an emission event. Recomputing the same
        // review-ready state can mint a new `updated_at` on every poll, so time is
        // deliberately excluded from identity. The semantic state and detail URI
        // change when the fact itself changes. Labels are likewise excluded: they
        // describe the issue rather than its attention state, so relabelling must
        // not re-fire the routes that ignore labels entirely.
        identity: format!(
            "{}:{}:{}:{}:{}:{}",
            event.issue_uri, key.kind, event.attention, event.status, detail_uri, fact_json
        ),
        fields: std::collections::BTreeMap::from([
            ("project".into(), serde_json::Value::String(project.into())),
            (
                "attention".into(),
                serde_json::to_value(event.attention).unwrap_or(serde_json::Value::Null),
            ),
            (
                "status".into(),
                serde_json::to_value(event.status).unwrap_or(serde_json::Value::Null),
            ),
            ("label".into(), label_values(labels)),
            ("detailUri".into(), serde_json::Value::String(detail_uri)),
            ("text".into(), serde_json::Value::String(fact_json)),
        ]),
        origin: Some(origin),
        summary: Some(summary),
        route_provenance: event.route_provenance,
    }
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::models::{IssueAttention, IssueStatus};
    use crate::orchestrator::AttentionFact;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{migrated_test_db, SearchIndex};
    use std::sync::Arc;

    fn test_origin() -> crate::issues::crud::IssueAuthorship {
        crate::issues::crud::installation_machine_authorship("test-device", 1).unwrap()
    }

    #[test]
    fn repeated_attention_observations_share_one_fact_identity() {
        let event = |updated_at| AttentionEvent {
            issue_id: "i".into(),
            issue_uri: "cairn://p/cairn/42".into(),
            fact: AttentionFact::AgentIdleWithWork {
                detail_uri: "cairn://p/cairn/42/1/builder/pr".into(),
            },
            attention: IssueAttention::NeedsApproval,
            status: IssueStatus::Waiting,
            updated_at,
            route_provenance: None,
        };

        assert_eq!(
            attention_fact(event(1), "cairn", &[], test_origin()).identity,
            attention_fact(event(999), "cairn", &[], test_origin()).identity,
            "poll timestamps must not mint new identities for one review-ready fact"
        );
    }

    #[tokio::test]
    async fn attention_producer_carries_machine_origin_through_issue_sink() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("config");
        std::fs::create_dir_all(config.join("routes")).unwrap();
        std::fs::write(
            config.join("routes/attention-issue.yaml"),
            "name: attention issue\ndescription: test\nwhen:\n  - fact: attention\nto:\n  kind: issue\n  labels: [routed]\n",
        )
        .unwrap();
        let db = Arc::new(migrated_test_db("attention-routes-issue-origin.db").await);
        db.execute_batch(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','Cairn','cairn','/tmp/cairn',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i','p',42,'Target','active',1,1);",
        )
        .await
        .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config,
        )
        .build();
        let expected_device_id = orch.anon_device_manager.device_id();

        dispatch_attention(
            &orch,
            AttentionEvent {
                issue_id: "i".into(),
                issue_uri: "cairn://p/cairn/42".into(),
                fact: AttentionFact::AgentIdleWithWork {
                    detail_uri: "cairn://p/cairn/42".into(),
                },
                attention: IssueAttention::Idle,
                status: IssueStatus::Waiting,
                updated_at: 2,
                route_provenance: None,
            },
        )
        .await
        .unwrap();

        let (author, appearance) = db
            .query_opt(
                "SELECT author_principal_json, appearance_snapshot_json FROM issues WHERE id != 'i' ORDER BY created_at DESC LIMIT 1",
                (),
                |row| Ok((crate::storage::RowExt::text(row, 0)?, crate::storage::RowExt::text(row, 1)?)),
            )
            .await
            .unwrap()
            .expect("the real attention producer must reach the issue sink");
        let authorship =
            crate::issues::crud::decode_issue_authorship(Some(author), Some(appearance))
                .unwrap()
                .unwrap();
        assert_eq!(
            authorship.author,
            cairn_common::identity::PrincipalRef::Machine {
                device_id: expected_device_id,
            }
        );
        assert_eq!(authorship.appearance.evidence().at, 0);
    }

    #[test]
    fn content_change_at_one_uri_mints_a_new_fact_identity() {
        let event = |version| AttentionEvent {
            issue_id: "i".into(),
            issue_uri: "cairn://p/cairn/42".into(),
            fact: AttentionFact::ArtifactWritten {
                detail_uri: "cairn://p/cairn/42/1/builder/create-pr".into(),
                content: crate::orchestrator::attention::ArtifactSummary {
                    output_name: "create-pr".into(),
                    version,
                    confirmed: true,
                    title: Some("Review".into()),
                    summary: None,
                    artifact_type: "create-pr".into(),
                },
            },
            attention: IssueAttention::NeedsApproval,
            status: IssueStatus::Waiting,
            updated_at: i64::from(version),
            route_provenance: None,
        };

        assert_ne!(
            attention_fact(event(1), "cairn", &[], test_origin()).identity,
            attention_fact(event(2), "cairn", &[], test_origin()).identity,
            "content-bearing fact variants at one URI remain distinct"
        );
    }

    /// Both spellings of every attached label reach a predicate, and a clause
    /// naming any one of them matches — array-against-array is membership, per
    /// the predicate grammar.
    #[test]
    fn a_clause_naming_any_attached_label_matches_by_slug_or_display_name() {
        let label = |id: &str, name: &str| Label {
            id: id.into(),
            workspace_id: "default".into(),
            name: name.into(),
            color: "#000000".into(),
            created_at: 1,
            updated_at: 1,
        };
        let fact = attention_fact(
            AttentionEvent {
                issue_id: "i".into(),
                issue_uri: "cairn://p/cairn/42".into(),
                fact: AttentionFact::AgentIdleWithWork {
                    detail_uri: "cairn://p/cairn/42".into(),
                },
                attention: IssueAttention::Idle,
                status: IssueStatus::Waiting,
                updated_at: 1,
                route_provenance: None,
            },
            "cairn",
            &[label("needs-review", "Needs Review"), label("ui", "ui")],
            test_origin(),
        );

        // A label whose display name already is its slug contributes one value,
        // not a duplicated pair.
        assert_eq!(
            fact.fields["label"],
            serde_json::json!(["needs-review", "Needs Review", "ui"])
        );

        let clause = |yaml: &str| -> Vec<std::collections::BTreeMap<String, serde_json::Value>> {
            serde_yaml::from_str(yaml).unwrap()
        };
        let matches_clause = |yaml: &str| {
            crate::routes::matches(
                &clause(yaml),
                &crate::routes::Fact {
                    source: &fact.source,
                    fields: &fact.fields,
                    presence: Presence::Away,
                },
            )
        };

        // The slug a hand-written route spells, and the display name the editor
        // picks out of the workspace vocabulary, name the same label.
        assert!(matches_clause(
            "- { fact: attention, label: [needs-review] }"
        ));
        assert!(matches_clause(
            "- { fact: attention, label: [Needs Review] }"
        ));
        // Membership across a multi-label issue: one named label suffices.
        assert!(matches_clause("- { fact: attention, label: [ui, absent] }"));
        assert!(!matches_clause("- { fact: attention, label: [absent] }"));
    }

    #[tokio::test]
    async fn attention_message_sink_fires_without_channel_runtime() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("config");
        std::fs::create_dir_all(config.join("routes")).unwrap();
        std::fs::write(
            config.join("routes/attention-message.yaml"),
            "name: attention message\ndescription: test\nwhen:\n  - fact: attention\nto:\n  kind: message\n  target: cairn://p/cairn/42\n",
        )
        .unwrap();
        let db = Arc::new(migrated_test_db("attention-routes-no-channel.db").await);
        db.execute_batch(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','Cairn','cairn','/tmp/cairn',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i','p',42,'Target','active',1,1);",
        )
        .await
        .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config,
        )
        .build();

        dispatch_attention(
            &orch,
            AttentionEvent {
                issue_id: "i".into(),
                issue_uri: "cairn://p/cairn/42".into(),
                fact: AttentionFact::AgentIdleWithWork {
                    detail_uri: "cairn://p/cairn/42".into(),
                },
                attention: IssueAttention::Idle,
                status: IssueStatus::Waiting,
                updated_at: 2,
                route_provenance: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM messages WHERE channel_type='issue' AND channel_id='cairn/42'",
                (),
            )
            .await
            .unwrap(),
            Some(1)
        );
    }

    /// The end-to-end guarantee the `label` trigger field is offered on: a
    /// label-filtered attention route fires for an issue carrying the label and
    /// stays silent for one that does not.
    #[tokio::test]
    async fn label_filtered_attention_route_fires_only_for_the_labelled_issue() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("config");
        std::fs::create_dir_all(config.join("routes")).unwrap();
        std::fs::write(
            config.join("routes/signal-only.yaml"),
            "name: signal only\ndescription: test\nwhen:\n  - fact: attention\n    label: [signal]\nto:\n  kind: message\n  target: cairn://p/cairn/42\n",
        )
        .unwrap();
        let db = Arc::new(migrated_test_db("attention-routes-label-filter.db").await);
        db.execute_batch(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','Cairn','cairn','/tmp/cairn',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('labelled','p',42,'Labelled','active',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('bare','p',43,'Bare','active',1,1);
             INSERT INTO labels(id,workspace_id,name,color,created_at,updated_at) VALUES('signal','w','signal','#000000',1,1);
             INSERT INTO labels(id,workspace_id,name,color,created_at,updated_at) VALUES('noise','w','noise','#000000',1,1);
             INSERT INTO issue_labels(issue_id,label_id,created_at) VALUES('labelled','noise',1);
             INSERT INTO issue_labels(issue_id,label_id,created_at) VALUES('labelled','signal',1);",
        )
        .await
        .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config,
        )
        .build();

        let event = |issue_id: &str, number: u32| AttentionEvent {
            issue_id: issue_id.into(),
            issue_uri: format!("cairn://p/cairn/{number}"),
            fact: AttentionFact::AgentIdleWithWork {
                detail_uri: format!("cairn://p/cairn/{number}"),
            },
            attention: IssueAttention::Idle,
            status: IssueStatus::Waiting,
            updated_at: 2,
            route_provenance: None,
        };
        let fired = || async {
            db.query_opt_i64(
                "SELECT COUNT(*) FROM messages WHERE channel_type='issue' AND channel_id='cairn/42'",
                (),
            )
            .await
            .unwrap()
        };

        dispatch_attention(&orch, event("bare", 43)).await.unwrap();
        assert_eq!(fired().await, Some(0), "an unlabelled issue must not fire");

        dispatch_attention(&orch, event("labelled", 42))
            .await
            .unwrap();
        assert_eq!(
            fired().await,
            Some(1),
            "a route filtered on `signal` must fire for an issue carrying it"
        );
    }
}

pub async fn dispatch_attention(orch: &Orchestrator, event: AttentionEvent) -> Result<(), String> {
    let parsed = parse_uri(&event.issue_uri)
        .ok_or_else(|| format!("invalid attention issue URI: {}", event.issue_uri))?;
    let project = parsed
        .project()
        .ok_or_else(|| format!("attention issue has no project: {}", event.issue_uri))?
        .to_owned();
    let db = orch.db.for_project(&project).await;
    let (project_id, project_path) = db
        .query_opt(
            "SELECT p.id, p.repo_path FROM issues i JOIN projects p ON p.id=i.project_id WHERE i.id=?1",
            (event.issue_id.clone(),),
            |row| Ok((crate::storage::RowExt::text(row, 0)?, crate::storage::RowExt::text(row, 1)?)),
        )
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("could not resolve attention issue {}", event.issue_uri))?;
    // A failed label read propagates rather than degrading to "no labels": an
    // empty list is indistinguishable from an unlabelled issue, and silently
    // withholding a label is exactly the failure that made this field useless.
    let issue_id = event.issue_id.clone();
    let labels = db
        .read(move |conn| {
            Box::pin(
                async move { crate::labels::attach::list_labels_for_issue(conn, &issue_id).await },
            )
        })
        .await
        .map_err(|error| error.to_string())?;
    let presence = crate::channels::operator_presence_status().await.presence;
    let submissions = super::dispatch(
        orch,
        attention_fact(
            event,
            &project,
            &labels,
            super::installation_machine_origin(orch)?,
        ),
        if presence == crate::channels::OperatorPresence::Present {
            Presence::Active
        } else {
            Presence::Away
        },
        RouteContext {
            project_id: Some(&project_id),
            project_path: Some(std::path::Path::new(&project_path)),
        },
    )
    .await?;
    for submission in submissions {
        if let Err(submission) = crate::channels::submit_route(submission) {
            crate::routes::record_channel_outcome(
                orch,
                &submission,
                Err("channel runtime is unavailable".into()),
            )
            .await?;
        }
    }
    Ok(())
}

pub fn spawn_attention_routes(orch: Orchestrator) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(ATTENTION_QUEUE);
    let mut events = orch.attention_changed.subscribe();
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => match tx.try_send(event) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        log::warn!("attention route queue is full; dropping event")
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                },
                Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                    log::warn!("attention route subscriber lagged; skipped {count} events")
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if let Err(error) = dispatch_attention(&orch, event).await {
                log::warn!("attention route dispatch failed: {error}");
            }
        }
    });
}
