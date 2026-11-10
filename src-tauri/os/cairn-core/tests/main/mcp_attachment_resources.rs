//! Reading a durable attachment, through the real resource read path.
//!
//! The unit tests in `attachments.rs` prove the record and the addressing. What
//! these prove is the half an agent actually experiences: that one address
//! routes on the media type MEASURED from the object's own bytes, and that each
//! route lands where a reader would expect — an image as a native block, a PDF
//! as extracted text, text as text, anything else as a described object with the
//! action that would give it a path.

use crate::common::{self, read_resource};
use cairn_core::attachments::{
    store_attachment_bytes, AttachmentAnchor, AttachmentProvenance, AttachmentScope,
};

const PNG: &[u8] = b"\x89PNG\r\n\x1a\nfixture-bytes";

async fn attach(
    db: &cairn_core::internal::storage::LocalDb,
    project_id: &str,
    bytes: &[u8],
    file_name: &str,
) -> String {
    let scope = AttachmentScope {
        project_id: project_id.to_string(),
        project_key: "att".to_string(),
        anchor: AttachmentAnchor::Issue(7),
    };
    store_attachment_bytes(
        db,
        &scope,
        bytes.to_vec(),
        file_name,
        None,
        &AttachmentProvenance::default(),
    )
    .await
    .unwrap_or_else(|error| panic!("attaching {file_name} failed: {error}"))
    .uri
}

/// A removed image reads as its tombstone through the agent's read path, and
/// the collection keeps it listed as removed rather than dropping its ordinal.
#[tokio::test]
async fn a_removed_image_reads_as_its_tombstone() {
    let (_temp, db, orch) = common::resource_orchestrator_fixture().await;
    let project_id = common::create_project(&db, "att").await;
    let uri = attach(&db, &project_id, PNG, "screen.png").await;

    cairn_core::attachments::remove_attachment(
        &db,
        &db,
        &project_id,
        "att",
        &cairn_uri::AttachmentScopeRef::Issue(7),
        1,
        &cairn_core::attachments::RemovalRequest {
            removed_by: "the desktop operator".into(),
            reason: "captured private data by mistake".into(),
            decision_id: None,
            include_shared: false,
        },
    )
    .await
    .unwrap();

    let rendered = read_resource(&orch, &uri).await;
    assert!(
        rendered.contains("was removed by the desktop operator"),
        "{rendered}"
    );
    assert!(
        rendered.contains("captured private data by mistake"),
        "{rendered}"
    );

    let images = read_resource(&orch, "cairn://p/att/7/images").await;
    assert!(images.contains("cairn://p/att/7/images/1"), "{images}");
    assert!(
        images.contains("REMOVED by the desktop operator"),
        "{images}"
    );
}
