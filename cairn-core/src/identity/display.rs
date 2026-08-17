//! Where the alias registries behind [`PrincipalAliases`] are read, and how a
//! resolved display is attached to the rows on their way to a surface.
//!
//! The rules themselves live once, in [`cairn_common::identity::display`]. This
//! module owns only the other half: which registries exist, and where they are
//! read from.
//!
//! Both registries are PRIVATE tables (`anon_device`, `executor_enrollments`),
//! so every resolution reads the private database rather than whichever replica
//! the row came from. That is what makes a team issue this machine created still
//! resolve to this machine's name — and a teammate's device still resolve to
//! nothing, which is the honest answer rather than a wrong one.

use cairn_common::identity::display::{PrincipalAliases, PrincipalDisplay};
use cairn_common::identity::{AppearanceSnapshot, PrincipalRef};

use crate::models::{Issue, Post, PostComment};
use crate::storage::{LocalDb, RowExt};

/// Everything this installation can prove about who a device is.
///
/// Best-effort throughout: a database without these tables (a fresh install, a
/// replica) simply resolves nothing, and every principal renders as itself.
pub async fn principal_aliases(local: &LocalDb) -> PrincipalAliases {
    let mut aliases = PrincipalAliases::default();
    // Enrolled executors first. This installation's own presence name is the
    // more direct fact about its own device, so it is applied last and wins for
    // a machine that is both.
    for (device_id, name) in enrolled_device_names(local).await {
        aliases = aliases.with_device(device_id, name);
    }
    if let Some(device_id) = local_device_id(local).await {
        aliases = aliases.with_device(
            device_id,
            crate::account::anon_device::machine_device_name(),
        );
    }
    aliases
}

/// A row that carries authorship and can be told how that authorship reads.
///
/// One trait rather than one function per row type: every surface that renders
/// an author resolves it the same way, and a new attributed row joins by saying
/// what it holds.
pub trait Attributed {
    fn author(&self) -> Option<&PrincipalRef>;
    /// The evidence recorded with the author, when this projection kept it. It
    /// carries the only alias an external correspondent has.
    fn appearance(&self) -> Option<&AppearanceSnapshot> {
        None
    }
    fn set_author_display(&mut self, display: PrincipalDisplay);
}

impl Attributed for Issue {
    fn author(&self) -> Option<&PrincipalRef> {
        self.author.as_ref()
    }
    fn set_author_display(&mut self, display: PrincipalDisplay) {
        self.author_display = Some(display);
    }
}

impl Attributed for Post {
    fn author(&self) -> Option<&PrincipalRef> {
        Some(&self.author)
    }
    fn appearance(&self) -> Option<&AppearanceSnapshot> {
        Some(&self.appearance)
    }
    fn set_author_display(&mut self, display: PrincipalDisplay) {
        self.author_display = Some(display);
    }
}

impl Attributed for PostComment {
    fn author(&self) -> Option<&PrincipalRef> {
        Some(&self.author)
    }
    fn appearance(&self) -> Option<&AppearanceSnapshot> {
        Some(&self.appearance)
    }
    fn set_author_display(&mut self, display: PrincipalDisplay) {
        self.author_display = Some(display);
    }
}

/// Resolve how every row's author reads, on the way to a surface that renders
/// them. One registry read serves the whole batch; a row with no author is left
/// alone, since absence is not something to resolve.
pub async fn resolve_author_displays<T: Attributed>(local: &LocalDb, rows: &mut [T]) {
    if rows.iter().all(|row| row.author().is_none()) {
        return;
    }
    let aliases = principal_aliases(local).await;
    for row in rows.iter_mut() {
        if row.author().is_none() {
            continue;
        }
        let display = aliases.display(row.author(), row.appearance());
        row.set_author_display(display);
    }
}

/// The same resolution for a caller holding a single row.
pub async fn resolve_author_display<T: Attributed>(local: &LocalDb, row: &mut T) {
    resolve_author_displays(local, std::slice::from_mut(row)).await;
}

/// How one principal reads, for a caller holding a single row rather than a
/// batch.
pub async fn display_for(
    local: &LocalDb,
    principal: Option<&PrincipalRef>,
    appearance: Option<&AppearanceSnapshot>,
) -> PrincipalDisplay {
    principal_aliases(local)
        .await
        .display(principal, appearance)
}

/// Active enrollments that carry a chosen name, oldest first so the newest
/// claim on a device wins.
async fn enrolled_device_names(local: &LocalDb) -> Vec<(String, String)> {
    local
        .query_all(
            "SELECT device_id, executor_name FROM executor_enrollments \
             WHERE revoked_at IS NULL AND executor_name IS NOT NULL \
             ORDER BY updated_at ASC",
            (),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .unwrap_or_default()
}

/// This installation's own immutable machine id — the single `anon_device` row.
async fn local_device_id(local: &LocalDb) -> Option<String> {
    local
        .query_all("SELECT device_id FROM anon_device LIMIT 1", (), |row| {
            row.text(0)
        })
        .await
        .ok()?
        .into_iter()
        .next()
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCAL: &str = "77bcd7b1-b309-45c2-936b-df0fa904379c";
    const ENROLLED: &str = "c0ffee00-dead-4bee-9999-000000000001";

    fn machine(device_id: &str) -> PrincipalRef {
        PrincipalRef::Machine {
            device_id: device_id.into(),
        }
    }

    async fn seeded_db(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(&format!(
            "INSERT INTO anon_device(device_id, created_at, updated_at) VALUES('{LOCAL}', 1, 1);
             INSERT INTO executor_enrollments(executor_id, device_id, runner_device_id,
                 credential_hash, enrolled_at, updated_at, expires_at, executor_name)
             VALUES('e-1', '{ENROLLED}', '{LOCAL}', 'hash-1', 1, 1, 999, 'linux-box');"
        ))
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn this_installation_and_its_executors_resolve_to_names() {
        let db = seeded_db("principal-aliases-known.db").await;
        let aliases = principal_aliases(&db).await;

        assert_eq!(
            aliases.display(Some(&machine(LOCAL)), None).label,
            crate::account::anon_device::machine_device_name()
        );
        assert_eq!(
            aliases.display(Some(&machine(ENROLLED)), None).label,
            "linux-box"
        );
    }

    // Authorship replicates. A device this installation has never met has no
    // name here, and must not borrow one.
    #[tokio::test]
    async fn a_foreign_device_stays_its_own_id() {
        let db = seeded_db("principal-aliases-foreign.db").await;
        let foreign = "aaaaaaaa-1111-4222-8333-444444444444";
        let display = principal_aliases(&db)
            .await
            .display(Some(&machine(foreign)), None);

        assert_eq!(display.label, "aaaaaaaa…");
        assert_eq!(display.detail, foreign);
    }

    // A machine that is both this installation and an enrolled executor reads as
    // itself: its own presence name is the more direct fact.
    #[tokio::test]
    async fn the_local_presence_name_outranks_an_enrollment_name() {
        let db = crate::storage::migrated_test_db("principal-aliases-precedence.db").await;
        db.execute_script(&format!(
            "INSERT INTO anon_device(device_id, created_at, updated_at) VALUES('{LOCAL}', 1, 1);
             INSERT INTO executor_enrollments(executor_id, device_id, runner_device_id,
                 credential_hash, enrolled_at, updated_at, expires_at, executor_name)
             VALUES('e-1', '{LOCAL}', '{LOCAL}', 'hash-1', 1, 1, 999, 'self-enrolled');"
        ))
        .await
        .unwrap();

        assert_eq!(
            principal_aliases(&db)
                .await
                .display(Some(&machine(LOCAL)), None)
                .label,
            crate::account::anon_device::machine_device_name()
        );
    }

    // A database with neither registry (a replica, a fresh install) resolves
    // nothing rather than failing.
    #[tokio::test]
    async fn missing_registries_resolve_nothing() {
        let db = crate::storage::migrated_test_db("principal-aliases-empty.db").await;
        let display = principal_aliases(&db)
            .await
            .display(Some(&machine(LOCAL)), None);

        assert_eq!(display.detail, LOCAL);
        assert_eq!(display.label, "77bcd7b1…");
    }
}
