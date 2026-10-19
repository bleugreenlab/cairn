//! The public cairn-inference facet reaches neither the runtime nor the private crate.
#[path = "../../cairn-models/tests/support/facet_graph.rs"]
mod facet_graph;

#[test]
fn the_public_inference_facet_keeps_its_boundary() {
    facet_graph::assert_public_facet(env!("CARGO_MANIFEST_DIR"));
}
