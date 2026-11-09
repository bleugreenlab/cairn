//! Prints the JSON Schema of every type the module's exports carry, which
//! `bun run build:uri-wasm` renders as the frontend's TypeScript types
//! (`scripts/lib/json-schema-ts.ts`), so those types come from the Rust ones
//! rather than from a TypeScript mirror of them.
//!
//! `CairnResource` and `Surface` are what `parse` and `surface` serialize, so
//! their schemas take the serialize contract: every field, `None` included,
//! is present. `LabelContext` is what `label` deserializes, so its fields are
//! optional.

use cairn_uri::{CairnResource, LabelContext, Surface};
use schemars::generate::SchemaSettings;

fn main() {
    let output = SchemaSettings::draft2020_12()
        .for_serialize()
        .into_generator();
    let input = SchemaSettings::draft2020_12()
        .for_deserialize()
        .into_generator();
    let schemas = serde_json::json!({
        "CairnResource": output.clone().into_root_schema_for::<CairnResource>(),
        "Surface": output.into_root_schema_for::<Surface>(),
        "LabelContext": input.into_root_schema_for::<LabelContext>(),
    });
    println!("{}", serde_json::to_string_pretty(&schemas).unwrap());
}
