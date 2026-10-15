pub use cairn_workflow::output_schemas::*;

#[cfg(test)]
mod tests {
    //! Preset schemas are authored as the order they reach a model. Parsing
    //! sorts `properties`, and the wire restores the schema-order contract
    //! (`cairn_models::wire::schema_order`), so a file written in any other order
    //! misstates what a constrained decoder, the prompt's field list, and the
    //! examples all see.
    use super::*;
    use cairn_models::wire::schema_order::property_order;
    use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
    use serde_json::Value;

    /// JSON as written, object keys in source order.
    enum Authored {
        Object(Vec<(String, Authored)>),
        Array(Vec<Authored>),
        Scalar,
    }

    impl Authored {
        fn get(&self, key: &str) -> Option<&Authored> {
            match self {
                Authored::Object(entries) => entries
                    .iter()
                    .find_map(|(name, value)| (name == key).then_some(value)),
                _ => None,
            }
        }

        fn keys(&self) -> Vec<&str> {
            match self {
                Authored::Object(entries) => {
                    entries.iter().map(|(name, _)| name.as_str()).collect()
                }
                _ => Vec::new(),
            }
        }
    }

    impl<'de> Deserialize<'de> for Authored {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct AuthoredVisitor;
            impl<'de> Visitor<'de> for AuthoredVisitor {
                type Value = Authored;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("JSON")
                }
                fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Authored, A::Error> {
                    let mut entries = Vec::new();
                    while let Some(entry) = map.next_entry::<String, Authored>()? {
                        entries.push(entry);
                    }
                    Ok(Authored::Object(entries))
                }
                fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Authored, A::Error> {
                    let mut items = Vec::new();
                    while let Some(item) = seq.next_element::<Authored>()? {
                        items.push(item);
                    }
                    Ok(Authored::Array(items))
                }
                fn visit_bool<E>(self, _: bool) -> Result<Authored, E> {
                    Ok(Authored::Scalar)
                }
                fn visit_i64<E>(self, _: i64) -> Result<Authored, E> {
                    Ok(Authored::Scalar)
                }
                fn visit_u64<E>(self, _: u64) -> Result<Authored, E> {
                    Ok(Authored::Scalar)
                }
                fn visit_f64<E>(self, _: f64) -> Result<Authored, E> {
                    Ok(Authored::Scalar)
                }
                fn visit_str<E>(self, _: &str) -> Result<Authored, E> {
                    Ok(Authored::Scalar)
                }
                fn visit_unit<E>(self) -> Result<Authored, E> {
                    Ok(Authored::Scalar)
                }
            }
            deserializer.deserialize_any(AuthoredVisitor)
        }
    }

    /// Every object schema beneath `schema` whose authored property order
    /// differs from the contract, named by its path.
    fn misordered(schema: &Value, authored: &Authored, path: &str, found: &mut Vec<String>) {
        if let (Some(properties), Some(written)) = (
            schema.get("properties").and_then(Value::as_object),
            authored.get("properties"),
        ) {
            let contract = property_order(schema);
            if written.keys() != contract {
                found.push(format!(
                    "{path}: written {:?}, sent {contract:?}",
                    written.keys()
                ));
            }
            for (name, subschema) in properties {
                if let Some(child) = written.get(name) {
                    misordered(subschema, child, &format!("{path}.{name}"), found);
                }
            }
        }
        for key in ["items", "additionalProperties"] {
            if let (Some(sub), Some(written)) = (schema.get(key), authored.get(key)) {
                misordered(sub, written, &format!("{path}.{key}"), found);
            }
        }
        for key in ["anyOf", "oneOf", "allOf", "prefixItems"] {
            if let (Some(branches), Some(Authored::Array(written))) =
                (schema.get(key).and_then(Value::as_array), authored.get(key))
            {
                for (index, (branch, authored)) in branches.iter().zip(written).enumerate() {
                    misordered(branch, authored, &format!("{path}.{key}[{index}]"), found);
                }
            }
        }
    }

    #[test]
    fn every_preset_is_authored_in_the_order_it_reaches_the_model() {
        for name in PRESET_SCHEMAS {
            let path = format!(
                "{}/../../resources/schemas/{name}.json",
                env!("CARGO_MANIFEST_DIR")
            );
            let text = std::fs::read_to_string(&path).unwrap();
            let authored: Authored = serde_json::from_str(&text).unwrap();
            let schema = load_embedded_preset_schema(name).unwrap();
            let mut found = Vec::new();
            misordered(&schema, &authored, name, &mut found);
            assert!(
                found.is_empty(),
                "write each preset's properties required-first, in `required` order, then \
                 optional ones alphabetically: {found:#?}"
            );
        }
    }
}
