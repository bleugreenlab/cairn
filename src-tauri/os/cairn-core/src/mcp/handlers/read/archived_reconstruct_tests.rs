//! Byte-exact archived-read reconstruction tests.
//!
//! These live in the mcp read layer rather than beside `reconstruct` because they
//! assert that low-level reconstruction reproduces EXACTLY what the high-level mcp
//! renderer produced (`tool_result(reconstructed) == render_targets(targets)`),
//! and so must name the real renderer — which `storage` cannot, since it carries
//! no upward `crate::mcp` edge (it will descend into `cairn-db`). Each test
//! registers the renderer via [`super::register_archived_file_renderer`], then
//! drives reconstruction through public `crate::storage` APIs. The
//! renderer-agnostic reconstruction tests stay in
//! `crate::storage::events::reconstruct`.

use std::sync::Arc;

use super::register_archived_file_renderer;
use crate::storage::event_fixture::render_targets as expected_read;
use crate::storage::events::reconstruct::{
    reconstruct_events, reconstruct_events_with_routes, STUB_PREFIX,
};
use crate::storage::events::reconstruct_fixture::{
    build_fixture, migrated_db, read_event, seed_chain, seed_team_chain, tool_result,
};

#[tokio::test]
async fn gitcoord_reads_are_byte_identical() {
    register_archived_file_renderer();
    let fx = build_fixture();
    let db = migrated_db().await;
    seed_chain(
        &db,
        fx.origin.to_str().unwrap(),
        Some((fx.pack.clone(), fx.idx.clone())),
    )
    .await;

    // Tip read: full file, line window, tail, single-file grep (all pack-layer
    // a.txt) plus an unchanged file (pack-layer tree, repo-layer blob).
    let tip_a = b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n";
    let b_txt = b"unchanged-keep\n";
    let targets: Vec<(&str, &[u8])> = vec![
        ("file:a.txt", tip_a),
        ("file:a.txt?offset=2&limit=2", tip_a),
        ("file:a.txt?offset=-1", tip_a),
        ("file:a.txt?grep=beta", tip_a),
        ("file:dir/b.txt", b_txt),
    ];
    let paths: Vec<&str> = targets.iter().map(|(t, _)| *t).collect();
    let event = read_event(&fx.tip, &paths);
    let out = reconstruct_events(&db, vec![event]).await;
    assert_eq!(tool_result(&out[0]), expected_read(&targets));

    // Anchor read resolves entirely from the repo layer (anchor is not in the
    // range pack).
    let anchor_a = b"alpha\nbeta\ngamma\ndelta\n";
    let anchor_targets: Vec<(&str, &[u8])> = vec![("file:a.txt", anchor_a)];
    let anchor_event = read_event(&fx.anchor, &["file:a.txt"]);
    let anchor_out = reconstruct_events(&db, vec![anchor_event]).await;
    assert_eq!(tool_result(&anchor_out[0]), expected_read(&anchor_targets));
}

#[tokio::test]
async fn team_gitcoord_read_stubs_until_private_local_repo_path_is_set() {
    register_archived_file_renderer();
    let fx = build_fixture();
    let team_db = migrated_db().await;
    seed_team_chain(
        &team_db,
        "/creator/path/not/on/this/machine",
        Some((fx.pack.clone(), fx.idx.clone())),
    )
    .await;
    let private_db = Arc::new(migrated_db().await);
    private_db
        .execute(
            "INSERT INTO teams(id, name, sync_url, replica_path, created_at) VALUES ('teamABC123', 'Team', 'http://sync', '/tmp/team.db', 1)",
            (),
        )
        .await
        .unwrap();
    private_db
        .execute(
            "INSERT INTO project_routes(project_key, team_id, created_at) VALUES ('p', 'teamABC123', 1)",
            (),
        )
        .await
        .unwrap();

    let event = read_event(&fx.tip, &["file:a.txt"]);
    let without_clone =
        reconstruct_events_with_routes(&team_db, vec![event], None, Some(private_db.as_ref()))
            .await;
    assert!(tool_result(&without_clone[0]).contains(STUB_PREFIX));

    private_db
        .execute(
            "UPDATE project_routes SET local_repo_path = ?1 WHERE project_key = 'p'",
            (fx.origin.to_str().unwrap(),),
        )
        .await
        .unwrap();
    let event = read_event(&fx.tip, &["file:a.txt"]);
    let with_clone =
        reconstruct_events_with_routes(&team_db, vec![event], None, Some(private_db.as_ref()))
            .await;
    assert_eq!(
        tool_result(&with_clone[0]),
        expected_read(&[(
            "file:a.txt",
            b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n" as &[u8],
        )])
    );
}

#[tokio::test]
async fn null_pack_reconstructs_via_repo_layer() {
    register_archived_file_renderer();
    let fx = build_fixture();
    let db = migrated_db().await;
    // No range pack: an anchor read still resolves from the project repo ODB.
    seed_chain(&db, fx.origin.to_str().unwrap(), None).await;

    let anchor_a = b"alpha\nbeta\ngamma\ndelta\n";
    let event = read_event(&fx.anchor, &["file:a.txt"]);
    let out = reconstruct_events(&db, vec![event]).await;
    assert_eq!(
        tool_result(&out[0]),
        expected_read(&[("file:a.txt", anchor_a)])
    );
}

mod recorded_sha {
    //! The recorded render sha as the oracle for which read composition an
    //! archived read was rendered under (cairn/6792).

    use super::*;
    use crate::models::Event;
    use crate::storage::compress;
    use crate::storage::event_fixture::hybrid_read_stub;
    use crate::storage::events::encoding::ArchivedShape;
    use crate::storage::events::reconstruct::{
        reconstruct_events_reporting, section_placeholder, Reconstruction,
    };
    use sha2::{Digest, Sha256};

    const TIP_A: &[u8] = b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n";

    fn sha(text: &str) -> String {
        format!("{:x}", Sha256::digest(text.as_bytes()))
    }

    async fn reconstructed(event: Event) -> (String, Reconstruction) {
        let fx = build_fixture();
        let db = migrated_db().await;
        seed_chain(
            &db,
            fx.origin.to_str().unwrap(),
            Some((fx.pack.clone(), fx.idx.clone())),
        )
        .await;
        let mut event = event;
        event.content_commit = Some(fx.tip.clone());
        let reported = reconstruct_events_reporting(&db, vec![event]).await;
        let (event, outcome) = reported.into_iter().next().unwrap();
        (tool_result(&event), outcome)
    }

    async fn gitcoord(target: &str, recorded: Option<String>) -> (String, Reconstruction) {
        register_archived_file_renderer();
        let mut event = read_event("unset", &[target]);
        event.content_render_sha = recorded;
        reconstructed(event).await
    }

    /// The specimen's shape: one match in a named file. Under the current
    /// composition its path is a heading; the builds that archived it put it on
    /// the row.
    const GREP: &str = "file:a.txt?grep=beta&head_limit=5";
    const GREP_SUMMER_2026: &str =
        "=== file:a.txt?grep=beta&head_limit=5 [1 matches] ===\na.txt:2:beta";

    #[tokio::test]
    async fn a_read_matching_under_the_current_composition_is_restored() {
        register_archived_file_renderer();
        let current = expected_read(&[(GREP, TIP_A)]);
        assert_ne!(current, GREP_SUMMER_2026);
        assert_eq!(
            gitcoord(GREP, Some(sha(&current))).await,
            (current, Reconstruction::Restored)
        );
    }

    #[tokio::test]
    async fn a_read_matching_only_the_summer_2026_composition_is_restored_as_then() {
        assert_eq!(
            gitcoord(GREP, Some(sha(GREP_SUMMER_2026))).await,
            (GREP_SUMMER_2026.to_string(), Reconstruction::RestoredLegacy)
        );
    }

    #[tokio::test]
    async fn a_read_matching_no_composition_serves_the_current_render_unverified() {
        register_archived_file_renderer();
        let current = expected_read(&[(GREP, TIP_A)]);
        assert_eq!(
            gitcoord(GREP, Some("0".repeat(64))).await,
            (current, Reconstruction::Unverified)
        );
    }

    /// No recorded sha is nothing to check against, not a mismatch.
    #[tokio::test]
    async fn a_read_with_no_recorded_sha_is_restored() {
        assert_eq!(gitcoord(GREP, None).await.1, Reconstruction::Restored);
    }

    /// A stub legitimately differs from what was recorded and is never
    /// compared.
    #[tokio::test]
    async fn a_stubbed_read_is_degraded_without_a_compare() {
        let (text, outcome) = gitcoord("file:ghost.txt?grep=beta", Some("0".repeat(64))).await;
        assert!(text.contains(STUB_PREFIX), "{text}");
        assert_eq!(outcome, Reconstruction::Degraded);
    }

    /// A capped grep then counted output lines and said nothing of what it
    /// left out: `[2 matches]` with no window footer, where the current
    /// composition says `3 matches; 2 of 3 matches shown` and continues.
    #[tokio::test]
    async fn a_capped_grep_restores_the_line_capped_body_and_bare_count() {
        const CAPPED: &str = "file:a.txt?grep=a&head_limit=2";
        const CAPPED_SUMMER_2026: &str =
            "=== file:a.txt?grep=a&head_limit=2 [2 matches] ===\na.txt:2:beta\na.txt:3:gamma";
        register_archived_file_renderer();
        let current = expected_read(&[(CAPPED, TIP_A)]);
        assert!(current.contains("2 of 3 matches shown"), "{current}");
        assert_eq!(
            gitcoord(CAPPED, Some(sha(CAPPED_SUMMER_2026))).await,
            (
                CAPPED_SUMMER_2026.to_string(),
                Reconstruction::RestoredLegacy
            )
        );
        // With context, `head_limit` counted the context rows too.
        const CONTEXT: &str = "file:a.txt?grep=gamma&-C=1&head_limit=2";
        const CONTEXT_SUMMER_2026: &str =
            "=== file:a.txt?grep=gamma&-C=1&head_limit=2 [1 matches] ===\na.txt:2-beta\na.txt:3:gamma";
        assert_eq!(
            gitcoord(CONTEXT, Some(sha(CONTEXT_SUMMER_2026))).await,
            (
                CONTEXT_SUMMER_2026.to_string(),
                Reconstruction::RestoredLegacy
            )
        );
    }

    /// The stub prefix in a fully resolved render is content, not a stub, so
    /// the recorded sha is still checked: here it is the echoed pattern of a
    /// grep that matched nothing.
    #[tokio::test]
    async fn a_resolved_render_containing_the_stub_prefix_is_still_verified() {
        let target = format!("file:a.txt?grep={STUB_PREFIX}");
        let (text, outcome) = gitcoord(&target, Some("0".repeat(64))).await;
        assert!(text.contains(STUB_PREFIX), "{text}");
        assert_eq!(outcome, Reconstruction::Unverified);
    }

    /// A hybrid read whose kept resource section quotes the stub prefix still
    /// verifies, and still restores the Summer 2026 bytes its sha names.
    #[tokio::test]
    async fn a_hybrid_read_quoting_the_stub_prefix_is_verified_and_repaired() {
        register_archived_file_renderer();
        let resource = format!("\n=== cairn://p/P/1 ===\nwhy: {STUB_PREFIX}: abc:x.rs");
        let skeleton = format!("{}{resource}", section_placeholder(0));
        let summer = format!("=== file:a.txt?grep=beta [1 matches] ===\na.txt:2:beta{resource}");
        let hybrid = |recorded: &str| {
            let columns = ArchivedShape::HybridRead {
                commit: "unset".to_string(),
                render_sha: sha(recorded),
                data: hybrid_read_stub("t1", &["file:a.txt?grep=beta", "cairn://p/P/1"], &[0]),
                blob: compress(skeleton.as_bytes()).unwrap(),
            }
            .encode();
            let mut event = read_event("unset", &[]);
            event.storage_mode = columns.storage_mode;
            event.content_render_sha = columns.content_render_sha;
            event.data = columns.data;
            event.data_blob = columns.data_blob;
            event.codec = columns.codec;
            event
        };
        assert_eq!(
            reconstructed(hybrid(&summer)).await,
            (summer, Reconstruction::RestoredLegacy)
        );
        assert_eq!(
            reconstructed(hybrid("something else")).await.1,
            Reconstruction::Unverified
        );
    }

    /// A hybrid read verifies as a whole: its excised file section re-renders
    /// under each composition in turn and the spliced result is checked.
    #[tokio::test]
    async fn a_hybrid_read_re_splices_each_section_under_the_matching_composition() {
        register_archived_file_renderer();
        let resource = "\n=== cairn://p/P/1 ===\nthe resource body";
        let skeleton = format!("{}{resource}", section_placeholder(0));
        let current_section = expected_read(&[("file:a.txt?grep=beta", TIP_A)]);
        let summer_section = "=== file:a.txt?grep=beta [1 matches] ===\na.txt:2:beta";
        let hybrid = |recorded: &str| {
            let columns = ArchivedShape::HybridRead {
                commit: "unset".to_string(),
                render_sha: sha(recorded),
                data: hybrid_read_stub("t1", &["file:a.txt?grep=beta", "cairn://p/P/1"], &[0]),
                blob: compress(skeleton.as_bytes()).unwrap(),
            }
            .encode();
            let mut event = read_event("unset", &[]);
            event.storage_mode = columns.storage_mode;
            event.content_render_sha = columns.content_render_sha;
            event.data = columns.data;
            event.data_blob = columns.data_blob;
            event.codec = columns.codec;
            event
        };

        let current = format!("{current_section}{resource}");
        assert_eq!(
            reconstructed(hybrid(&current)).await,
            (current.clone(), Reconstruction::Restored)
        );
        let summer = format!("{summer_section}{resource}");
        assert_eq!(
            reconstructed(hybrid(&summer)).await,
            (summer, Reconstruction::RestoredLegacy)
        );
        assert_eq!(
            reconstructed(hybrid("something else")).await,
            (current, Reconstruction::Unverified)
        );
    }
}
