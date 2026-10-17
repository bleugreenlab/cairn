//! The public cairn-models facet reaches neither the runtime nor the private crate.
//! The check lives with the facet; it runs here too so a manifest change to
//! this crate re-runs it.
#[path = "../../cairn-models/tests/support/facet_graph.rs"]
mod facet_graph;

#[test]
fn the_public_image_wire_keeps_its_boundary() {
    facet_graph::assert_public_facet(env!("CARGO_MANIFEST_DIR"));
}
