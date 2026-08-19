//! Rendering journaled authority grants and the authorization journal.
//!
//! A grant list has to answer two questions at a glance: what authority is
//! currently live in this workspace, and what did that authority actually
//! authorize. So a grant renders its normalized scope (never a paraphrase) and
//! its status, and drilling into one shows the decisions that cited it.

use cairn_common::authorization::{AuthorityGrant, AuthorityLifetime};
use cairn_db::storage::authority::AuthorizationEventRecord;

use crate::authorization;
use crate::storage::LocalDb;

fn render_uuid_id(id: &str) -> String {
    uuid::Uuid::parse_str(id)
        .map(|uuid| unigram::UnigramId::from_bytes(*uuid.as_bytes()).to_string())
        .unwrap_or_else(|_| id.to_string())
}

fn grant_uri_id(id: &str) -> String {
    render_uuid_id(id).replace(' ', "-")
}

#[cfg(test)]
mod tests {
    use super::{grant_uri_id, render_uuid_id, stored_grant_id};

    const UUID: &str = "123e4567-e89b-12d3-a456-426614174000";

    #[test]
    fn grant_uuid_renders_as_sixteen_unigram_words() {
        let rendered = render_uuid_id(UUID);
        assert_eq!(rendered.split_whitespace().count(), 16);
        assert_eq!(stored_grant_id(&rendered), UUID);
    }

    #[test]
    fn grant_member_segment_is_uri_safe_and_resolves_to_storage_id() {
        let segment = grant_uri_id(UUID);
        assert!(!segment.contains(char::is_whitespace));
        assert_eq!(segment.split('-').count(), 16);
        assert_eq!(stored_grant_id(&segment), UUID);
    }

    #[test]
    fn grant_lookup_accepts_recovered_words_and_legacy_uuid() {
        let rendered = render_uuid_id(UUID);
        assert_eq!(
            stored_grant_id(&rendered.to_uppercase().replace(' ', "-")),
            UUID
        );
        assert_eq!(stored_grant_id(UUID), UUID);
    }

    #[test]
    fn historical_non_uuid_ids_remain_addressable() {
        assert_eq!(render_uuid_id("legacy-grant"), "legacy-grant");
        assert_eq!(stored_grant_id("legacy-grant"), "legacy-grant");
    }
}

pub(crate) fn stored_grant_id(id: &str) -> String {
    if uuid::Uuid::parse_str(id).is_ok() {
        return id.to_string();
    }
    unigram::UnigramId::<16>::recover(id)
        .map(|words| uuid::Uuid::from_bytes(words.into_bytes()).to_string())
        .unwrap_or_else(|_| id.to_string())
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn lifetime_label(lifetime: &AuthorityLifetime) -> String {
    match lifetime {
        AuthorityLifetime::Once { .. } => "once".to_string(),
        AuthorityLifetime::Turn { turn_id } => format!("turn {turn_id}"),
        AuthorityLifetime::Session { session_id } => format!("session {session_id}"),
        AuthorityLifetime::Standing => "standing".to_string(),
    }
}

fn constraint_labels(grant: &AuthorityGrant) -> String {
    if grant.constraints.constraints.is_empty() {
        return "none (covers the whole scope)".to_string();
    }
    grant
        .constraints
        .constraints
        .iter()
        .map(|constraint| match constraint {
            cairn_common::authorization::AuthorityConstraint::SettingsSections { sections } => {
                format!("sections: {}", sections.join(", "))
            }
            cairn_common::authorization::AuthorityConstraint::MutationModes { modes } => format!(
                "modes: {}",
                modes
                    .iter()
                    .map(|mode| mode.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            // The digest, never the configuration. Rendering the command or the
            // env wiring here would put a server's credential references in a
            // listing anyone can read, and the digest is what matching uses
            // anyway.
            cairn_common::authorization::AuthorityConstraint::McpConfig { fingerprint } => {
                format!("exact MCP configuration {}", fingerprint.short())
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn render_grant_row(grant: &AuthorityGrant, at: i64) -> String {
    let id = render_uuid_id(&grant.id);
    let uri_id = grant_uri_id(&grant.id);
    format!(
        "- `{}` — {} ({}) · [{}](cairn://grants/{})",
        grant.scope.shorthand(),
        grant.status(at),
        lifetime_label(&grant.lifetime),
        id,
        uri_id
    )
}

fn render_decision(event: &AuthorizationEventRecord) -> String {
    let cited = match event.grant_id.as_deref() {
        Some(id) => format!(" · grant `{}`", render_uuid_id(id)),
        None => String::new(),
    };
    format!(
        "- `{}` — **{}** ({}) · {}{}",
        event.scope.shorthand(),
        event.outcome,
        event.reason,
        event.summary,
        cited
    )
}

/// `cairn://grants` — every grant in the workspace.
pub async fn read_grants(db: &LocalDb) -> String {
    let at = now();
    let grants = match authorization::list_grants(db, 200).await {
        Ok(grants) => grants,
        Err(error) => return format!("# Authority grants\n\nCould not read grants: {error}"),
    };

    let mut out = String::from("# Authority grants\n\n");
    if grants.is_empty() {
        out.push_str(
            "No authority grants. Ordinary project work needs none — a grant appears here only \
             when an operator approves a named authority boundary, such as installing a \
             workspace MCP server or writing a capability-bearing workspace setting.\n",
        );
        return out;
    }

    let (active, spent): (Vec<_>, Vec<_>) = grants.iter().partition(|grant| !grant.is_spent(at));

    out.push_str(&format!("## Active ({})\n\n", active.len()));
    if active.is_empty() {
        out.push_str("None.\n");
    } else {
        for grant in &active {
            out.push_str(&render_grant_row(grant, at));
            out.push('\n');
        }
    }

    if !spent.is_empty() {
        out.push_str(&format!("\n## Spent ({})\n\n", spent.len()));
        for grant in &spent {
            out.push_str(&render_grant_row(grant, at));
            out.push('\n');
        }
    }
    out
}

/// `cairn://grants?view=decisions` — the authorization journal.
pub async fn read_decisions(db: &LocalDb) -> String {
    let decisions = match authorization::recent_decisions(db, 100).await {
        Ok(decisions) => decisions,
        Err(error) => {
            return format!("# Authorization decisions\n\nCould not read decisions: {error}")
        }
    };
    let mut out = String::from("# Authorization decisions\n\n");
    if decisions.is_empty() {
        out.push_str(
            "No decisions recorded. Only approval-required outcomes are journaled; ordinary \
             direct work is deliberately absent so the boundary crossings stay visible.\n",
        );
        return out;
    }
    for decision in &decisions {
        out.push_str(&render_decision(decision));
        out.push('\n');
    }
    out
}

/// How long a lease has left, which is what an operator wants from an expiry
/// here: an absolute timestamp answers "when" but not "is this about to stop
/// working".
fn time_remaining(expires_at: i64, at: i64) -> String {
    let remaining = expires_at - at;
    if remaining <= 0 {
        return "elapsed".to_string();
    }
    if remaining < 60 {
        return format!("in {remaining}s");
    }
    if remaining < 3600 {
        return format!("in {}m", remaining / 60);
    }
    format!("in {}h{}m", remaining / 3600, (remaining % 3600) / 60)
}

/// `cairn://grants?view=leases` — the live credential leases.
///
/// Deliberately alongside the grants rather than on a URI of its own: a grant
/// says an authority was approved, and a lease says a credential exercising one
/// is currently out. Reading them in one place is how an operator answers "what
/// can act right now, and with what".
///
/// Process-local by construction, so this reports what *this* runner holds. It
/// is not persisted and a restart correctly shows an empty book.
pub fn read_leases() -> String {
    let at = now();
    let leases = crate::security::leases().inventory();
    let mut out = String::from("# Credential leases\n\n");
    if leases.is_empty() {
        out.push_str(
            "No live credential leases. A lease appears here when the broker hands a credential \
             to something that has to present it onward — a provider token bound to one host, a \
             backend key bound to an agent process. Credentials the broker uses without handing \
             out, like the GitHub App signing key, never appear.\n",
        );
        return out;
    }
    out.push_str(
        "Each lease names the authority it exercises, the one destination it may be presented \
         to, and when it stops working. Material is never carried here.\n\n",
    );
    out.push_str("| Lease | Authority | Audience | Expiry | Status |\n");
    out.push_str("| --- | --- | --- | --- | --- |\n");
    for lease in &leases {
        out.push_str(&format!(
            "| `{}` | `{}` | `{}` | {} | {} |\n",
            lease.id,
            lease.scope,
            lease.audience.label(),
            time_remaining(lease.expires_at, at),
            lease.status,
        ));
    }
    out
}

/// `cairn://grants?view=incidents` — disclosures and what was done about them.
///
/// Beside the grants and the leases because the three answer one question in
/// sequence: what authority was approved, what is currently out exercising it,
/// and what of that has leaked. An operator reading this is usually asking
/// "what do I still have to do myself", so the rotation column is the point of
/// the table and an unrotated incident stays visible however much was contained.
pub async fn read_incidents(db: &LocalDb) -> String {
    use crate::security::remediation::store;

    let incidents = match store::list_incidents(db, 100).await {
        Ok(incidents) => incidents,
        Err(error) => {
            return format!("# Disclosure incidents\n\nCould not read incidents: {error}")
        }
    };

    let mut out = String::from("# Disclosure incidents\n\n");
    if incidents.is_empty() {
        out.push_str(
            "No recorded disclosures. An incident appears here when a credential is known to \
             have reached a durable record \u{2014} caught by a crossing, or declared by an \
             operator. Recording one revokes the authority it carries, inventories every store \
             that holds it, and withholds those records the read path can gate.\n",
        );
        return out;
    }

    out.push_str(
        "Each incident names the credential by its registry id, never its value. Containment \
         is local: revoking stops this runner handing the credential out again, and rotation \
         at the provider is what actually ends the disclosure.\n\n\
         Read the disposition column closely. `quarantined` means a read gate now withholds \
         the record; `reported` means it was found in a store with no read gate and is \
         **still being served** until you deal with it by hand. An incident holding any \
         reported record stays at `action required` rather than `contained`.\n\n",
    );
    out.push_str("| Incident | Credential | Found | Status | Revoked | Rotation |\n");
    out.push_str("| --- | --- | --- | --- | --- | --- |\n");
    for incident in &incidents {
        out.push_str(&format!(
            "| `{}` | `{}` | {} | {} | {} lease(s), {} grant(s) | {} |\n",
            render_uuid_id(&incident.id),
            incident.secret_id,
            incident.discovered_via.as_str(),
            if incident.status == crate::security::remediation::IncidentStatus::ActionRequired {
                "**action required**".to_string()
            } else {
                incident.status.as_str().to_string()
            },
            incident.leases_revoked,
            incident.grants_revoked,
            if incident.rotation_required {
                "**required**"
            } else {
                "confirmed"
            },
        ));
    }

    for incident in &incidents {
        out.push_str(&format!(
            "\n## Incident `{}`\n\n",
            render_uuid_id(&incident.id)
        ));
        if let Some(crossing) = &incident.crossing {
            out.push_str(&format!("- Caught at the {crossing} crossing\n"));
        }
        if let Some(note) = &incident.note {
            out.push_str(&format!("- Operator note: {note}\n"));
        }

        match store::affected_for(db, &incident.id).await {
            Ok(records) if !records.is_empty() => {
                out.push_str("\n### Affected records\n\n");
                out.push_str("| Store | Record | Occurrences | Disposition |\n");
                out.push_str("| --- | --- | --- | --- |\n");
                let mut still_served: Vec<String> = Vec::new();
                for (sink, locator, occurrences, disposition) in records {
                    let flag = if disposition == "reported" {
                        still_served.push(format!("`{sink}` `{locator}`"));
                        " — **still served**"
                    } else {
                        ""
                    };
                    out.push_str(&format!(
                        "| `{sink}` | `{locator}` | {occurrences} | {disposition}{flag} |\n"
                    ));
                }
                out.push_str(
                    "\nOccurrence counts say how much, never what: an incident report that \
                     quoted the credential would be one more record carrying it.\n",
                );
                if !still_served.is_empty() {
                    out.push_str(&format!(
                        "\n**{} record(s) are still being served.** Cairn found them but has no \
                         read gate on their store, so nothing is withholding them. Edit or \
                         delete each one, and rotate the credential — rotation is what makes \
                         the residue harmless: {}\n",
                        still_served.len(),
                        still_served.join(", ")
                    ));
                }
            }
            Ok(_) => out.push_str("\nNo durable record on this host carried the credential.\n"),
            Err(error) => out.push_str(&format!("\nCould not read affected records: {error}\n")),
        }

        match store::actions_for(db, &incident.id).await {
            Ok(actions) if !actions.is_empty() => {
                out.push_str("\n### Response\n\n");
                for action in actions {
                    out.push_str(&format!(
                        "{}. **{}** ({}) \u{2014} {}\n",
                        action.seq,
                        action.action,
                        action.actor,
                        action.detail.as_deref().unwrap_or(""),
                    ));
                }
            }
            Ok(_) => {}
            Err(error) => {
                out.push_str(&format!("\nCould not read the response journal: {error}\n"))
            }
        }
    }

    out
}

/// `cairn://grants/{id}` — one grant and everything it authorized.
pub async fn read_grant(db: &LocalDb, id: &str) -> String {
    let at = now();
    let stored_id = stored_grant_id(id);
    let grant = match authorization::get_grant(db, &stored_id).await {
        Ok(Some(grant)) => grant,
        Ok(None) => return format!("# Authority grant\n\nNo grant with id `{id}`."),
        Err(error) => return format!("# Authority grant\n\nCould not read grant: {error}"),
    };

    let display_id = render_uuid_id(&grant.id);
    let mut out = format!("# Authority grant `{display_id}`\n\n");
    out.push_str(&format!("- Scope: `{}`\n", grant.scope.shorthand()));
    out.push_str(&format!("- Status: {}\n", grant.status(at)));
    out.push_str(&format!(
        "- Lifetime: {}\n",
        lifetime_label(&grant.lifetime)
    ));
    out.push_str(&format!("- Constraints: {}\n", constraint_labels(&grant)));
    out.push_str(&format!(
        "- Audience: workspace {}\n",
        grant.audience.workspace_id
    ));
    if let Some(node) = grant.principal.node_uri.as_deref() {
        out.push_str(&format!("- Principal: {node}\n"));
    }
    if let Some(agent) = grant.principal.agent_id.as_deref() {
        out.push_str(&format!("- Agent: {agent}\n"));
    }
    out.push_str(&format!("- Issued by: {}\n", grant.provenance.issuer));
    // The approver is the authenticated operator the mint recorded, and it is
    // rendered only when there is one. A grant with an issuer but no approver is
    // shown as exactly that rather than being attributed to whoever seems
    // likely: an audit trail that guesses is worse than one that admits a gap.
    match grant.provenance.approver.as_deref() {
        Some(approver) => out.push_str(&format!("- Approved by: {approver}\n")),
        None => out.push_str("- Approved by: not recorded\n"),
    }
    // An expiry is the case that makes a bare epoch worst: it is the one
    // timestamp a reader is deciding something against, and it reads forward.
    if let Some(expiry) = grant.expires_at {
        out.push_str(&format!("- Expires: {}\n", crate::clock::age(expiry)));
    }
    if let Some(revoked) = grant.revoked_at {
        out.push_str(&format!("- Revoked: {}\n", crate::clock::age(revoked)));
    }

    match authorization::events_citing(db, &stored_id).await {
        Ok(events) if !events.is_empty() => {
            out.push_str(&format!(
                "\n## Decisions citing this grant ({})\n\n",
                events.len()
            ));
            for event in &events {
                out.push_str(&render_decision(event));
                out.push('\n');
            }
        }
        Ok(_) => out.push_str("\nNo decision has cited this grant yet.\n"),
        Err(error) => out.push_str(&format!("\nCould not read decisions: {error}\n")),
    }
    out
}
