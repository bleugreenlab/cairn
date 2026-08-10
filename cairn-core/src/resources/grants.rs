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
    format!(
        "- `{}` — {} ({}) · [{}](cairn://grants/{})",
        grant.scope.shorthand(),
        grant.status(at),
        lifetime_label(&grant.lifetime),
        grant.id,
        grant.id
    )
}

fn render_decision(event: &AuthorizationEventRecord) -> String {
    let cited = match event.grant_id.as_deref() {
        Some(id) => format!(" · grant `{id}`"),
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

/// `cairn://grants/{id}` — one grant and everything it authorized.
pub async fn read_grant(db: &LocalDb, id: &str) -> String {
    let at = now();
    let grant = match authorization::get_grant(db, id).await {
        Ok(Some(grant)) => grant,
        Ok(None) => return format!("# Authority grant\n\nNo grant with id `{id}`."),
        Err(error) => return format!("# Authority grant\n\nCould not read grant: {error}"),
    };

    let mut out = format!("# Authority grant `{id}`\n\n");
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
    if let Some(expiry) = grant.expires_at {
        out.push_str(&format!("- Expires at: {expiry}\n"));
    }
    if let Some(revoked) = grant.revoked_at {
        out.push_str(&format!("- Revoked at: {revoked}\n"));
    }

    match authorization::events_citing(db, id).await {
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
