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

/// A genuinely valid single-page PDF whose one text run is `PDF FIXTURE TEXT`,
/// assembled with a real cross-reference table so extraction runs for real
/// rather than against a `%PDF-` prefix the sniffer accepts and the extractor
/// rejects. The offsets are computed rather than written by hand, because a
/// hand-counted xref is exactly the kind of fixture that rots into a test
/// asserting its own failure.
fn pdf_fixture() -> Vec<u8> {
    let stream = b"BT /F1 24 Tf 20 100 Td (PDF FIXTURE TEXT) Tj ET";
    let objects: Vec<String> = vec![
        "<< /Type /Catalog /Pages 2 0 R >>".to_string(),
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_string(),
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 200] \
          /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>"
            .to_string(),
        format!(
            "<< /Length {} >>\nstream\n{}\nendstream",
            stream.len(),
            String::from_utf8_lossy(stream)
        ),
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".to_string(),
    ];

    let mut out = b"%PDF-1.4\n".to_vec();
    let mut offsets = Vec::with_capacity(objects.len());
    for (index, body) in objects.iter().enumerate() {
        offsets.push(out.len());
        out.extend_from_slice(format!("{} 0 obj\n{body}\nendobj\n", index + 1).as_bytes());
    }

    let xref_at = out.len();
    out.extend_from_slice(format!("xref\n0 {}\n", objects.len() + 1).as_bytes());
    out.extend_from_slice(b"0000000000 65535 f \n");
    for offset in &offsets {
        out.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
    }
    out.extend_from_slice(
        format!(
            "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{xref_at}\n%%EOF\n",
            objects.len() + 1
        )
        .as_bytes(),
    );
    out
}

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

#[tokio::test]
async fn an_attachment_reads_by_the_media_type_measured_from_its_bytes() {
    let (_temp, db, orch) = common::resource_orchestrator_fixture().await;
    let project_id = common::create_project(&db, "att").await;

    // Named `.txt`, but the bytes are a PNG. The measurement decides, so this
    // is an image and takes the image address.
    let mislabelled = attach(&db, &project_id, PNG, "screenshot.txt").await;
    assert_eq!(mislabelled, "cairn://p/att/7/images/1");

    let csv = attach(&db, &project_id, b"name,value\nada,1\nbob,2\n", "data.csv").await;
    assert_eq!(csv, "cairn://p/att/7/attachments/2");
    let pdf = attach(&db, &project_id, &pdf_fixture(), "report.pdf").await;
    assert_eq!(pdf, "cairn://p/att/7/attachments/3");
    let binary = attach(&db, &project_id, &[0u8, 1, 2, 3, 255, 254], "blob.bin").await;
    assert_eq!(binary, "cairn://p/att/7/attachments/4");

    // Text renders as its content, under a header that says it is content.
    let rendered = read_resource(&orch, &csv).await;
    assert!(rendered.contains("ada,1"), "{rendered}");
    assert!(rendered.contains("text/csv"), "{rendered}");
    assert!(
        rendered.contains("not \ninstruction") || rendered.contains("not instruction"),
        "attachment text must be marked as content rather than instruction: {rendered}"
    );

    // A PDF is extracted rather than described, and nothing about the temp file
    // it was staged into for extraction reaches the reader.
    let rendered = read_resource(&orch, &pdf).await;
    assert!(rendered.contains("application/pdf"), "{rendered}");
    assert!(
        rendered.contains("PDF FIXTURE TEXT"),
        "the PDF's own text must be extracted: {rendered}"
    );
    assert!(
        !rendered.contains("/tmp/"),
        "a staging path leaked: {rendered}"
    );

    // Bytes that are not text are described, with the action that would give
    // them a filesystem path.
    let rendered = read_resource(&orch, &binary).await;
    assert!(rendered.contains("application/octet-stream"), "{rendered}");
    assert!(rendered.contains("materialize"), "{rendered}");
    assert!(rendered.contains(&binary), "{rendered}");

    // The general address resolves the image too, and says so as an image.
    let general = read_resource(&orch, "cairn://p/att/7/attachments/1").await;
    assert!(
        !general.contains("is not an image"),
        "the general address resolves every kind: {general}"
    );
}

/// The `images/{n}` spelling asserts a kind. A row that does not hold an image
/// is refused by name, and the refusal carries the address that does resolve it
/// rather than leaving the reader to guess.
#[tokio::test]
async fn an_image_address_refuses_a_non_image_and_names_the_working_address() {
    let (_temp, db, orch) = common::resource_orchestrator_fixture().await;
    let project_id = common::create_project(&db, "att").await;
    attach(&db, &project_id, b"plain notes", "notes.txt").await;

    let rendered = read_resource(&orch, "cairn://p/att/7/images/1").await;
    assert!(rendered.contains("is not an image"), "{rendered}");
    assert!(
        rendered.contains("cairn://p/att/7/attachments/1"),
        "the refusal must name the address that works: {rendered}"
    );
}

/// A retired reference is an instructive refusal, not a 404 and not silence —
/// and the collection still lists it, because ordinals are never reused and
/// hiding one would make its neighbours lie about what they mean.
#[tokio::test]
async fn a_retired_reference_refuses_by_name_and_stays_enumerated() {
    let (_temp, db, orch) = common::resource_orchestrator_fixture().await;
    let project_id = common::create_project(&db, "att").await;
    let uri = attach(&db, &project_id, b"withdraw me", "draft.txt").await;

    cairn_core::attachments::retire_attachment(
        &db,
        &project_id,
        "att",
        &cairn_common::uri::AttachmentScopeRef::Issue(7),
        1,
    )
    .await
    .unwrap();

    let rendered = read_resource(&orch, &uri).await;
    assert!(rendered.contains("retired"), "{rendered}");

    let collection = read_resource(&orch, "cairn://p/att/7/attachments").await;
    assert!(collection.contains("draft.txt"), "{collection}");
    assert!(collection.contains("RETIRED"), "{collection}");
}

/// The collection is what makes an ordinal a navigable index rather than
/// decoration, and the image view is a filter over it: a gap in `images` is a
/// file, not a missing image, and the listing says where to find it.
#[tokio::test]
async fn the_collection_enumerates_every_kind_and_the_image_view_filters_it() {
    let (_temp, db, orch) = common::resource_orchestrator_fixture().await;
    let project_id = common::create_project(&db, "att").await;
    attach(&db, &project_id, PNG, "shot.png").await;
    attach(&db, &project_id, b"a,b\n", "data.csv").await;

    let collection = read_resource(&orch, "cairn://p/att/7/attachments").await;
    assert!(
        collection.contains("cairn://p/att/7/attachments/1"),
        "{collection}"
    );
    assert!(
        collection.contains("cairn://p/att/7/attachments/2"),
        "{collection}"
    );
    assert!(collection.contains("2 attachment(s)"), "{collection}");

    let images = read_resource(&orch, "cairn://p/att/7/images").await;
    assert!(images.contains("cairn://p/att/7/images/1"), "{images}");
    assert!(
        !images.contains("images/2"),
        "the csv is not an image: {images}"
    );
    assert!(
        images.contains("cairn://p/att/7/attachments"),
        "the image view must point at the collection that holds the rest: {images}"
    );
}
