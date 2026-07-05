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
    reconstruct_events, reconstruct_events_with_conn_and_routes, STUB_PREFIX,
};
use crate::storage::events::reconstruct_fixture::{
    build_fixture, migrated_db, read_event, seed_chain, seed_team_chain, tool_result,
};
use crate::storage::DbResult;

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
            "INSERT INTO project_routes(project_key, team_id, created_at) VALUES ('P', 'teamABC123', 1)",
            (),
        )
        .await
        .unwrap();

    let event = read_event(&fx.tip, &["file:a.txt"]);
    let without_clone = team_db
        .read(|conn| {
            let event = event.clone();
            let private_db = private_db.clone();
            Box::pin(async move {
                DbResult::Ok(
                    reconstruct_events_with_conn_and_routes(
                        conn,
                        vec![event],
                        None,
                        Some(private_db.as_ref()),
                    )
                    .await,
                )
            })
        })
        .await
        .unwrap();
    assert!(tool_result(&without_clone[0]).contains(STUB_PREFIX));

    private_db
        .execute(
            "UPDATE project_routes SET local_repo_path = ?1 WHERE project_key = 'P'",
            (fx.origin.to_str().unwrap(),),
        )
        .await
        .unwrap();
    let event = read_event(&fx.tip, &["file:a.txt"]);
    let with_clone = team_db
        .read(|conn| {
            let event = event.clone();
            let private_db = private_db.clone();
            Box::pin(async move {
                DbResult::Ok(
                    reconstruct_events_with_conn_and_routes(
                        conn,
                        vec![event],
                        None,
                        Some(private_db.as_ref()),
                    )
                    .await,
                )
            })
        })
        .await
        .unwrap();
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
