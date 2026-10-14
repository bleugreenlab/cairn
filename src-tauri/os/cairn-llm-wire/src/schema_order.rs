//! The one order in which Cairn lists a JSON Schema's `properties` wherever the
//! schema reaches a model: a provider's native structured-output constraint,
//! the prompt's field list, and few-shot example outputs.
//!
//! Order is part of the contract once a grammar enforces the schema.
//! Grammar-constrained decoders (vLLM's guided decoding, OpenAI strict mode,
//! and similar) walk an object's properties in the order the schema lists them.
//! Emitting a key forecloses every key listed before it, so an optional key the
//! model meant to write later is dropped with no error. The result is still
//! valid JSON and still valid against the schema, so no downstream validation
//! can detect the loss.
//!
//! An authored order does not survive parsing: without serde_json's
//! `preserve_order` feature (which Cairn does not enable, because it changes
//! map semantics workspace-wide) every `properties` object is sorted. The one
//! order-bearing channel that does survive is the `required` array, which
//! authors write in their natural order. The contract:
//!
//! 1. required properties first, in `required`-array order;
//! 2. then every optional property, lexically.
//!
//! applied to every object subschema, however deeply nested. Because every
//! optional key follows every required key, writing a required key can never
//! foreclose an optional one; an optional key can only be foreclosed by an
//! optional key listed after it. An object with at most one optional property
//! therefore cannot lose a key at all. Keeping the prompt's field list and the
//! examples in this same order means the model is never taught a sequence the
//! grammar forbids.
//!
//! `serde_json::Value` cannot hold an order, so the contract is applied when a
//! value is serialized: [`OrderedSchema`] for a schema, [`OrderedInstance`] for
//! a value shaped by one, and [`OrderedSchemaAt`] for a request body carrying a
//! schema at a known path.

use serde::ser::{Error as _, SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use serde_json::{Map, Value};

/// Keywords whose value is a subschema, or an array of subschemas.
const SUBSCHEMA_KEYWORDS: &[&str] = &[
    "additionalItems",
    "additionalProperties",
    "allOf",
    "anyOf",
    "contains",
    "else",
    "if",
    "items",
    "not",
    "oneOf",
    "prefixItems",
    "propertyNames",
    "then",
    "unevaluatedItems",
    "unevaluatedProperties",
];

/// Keywords whose value maps names to subschemas. Their names carry no
/// generation order, so they keep their sorted order; only the subschemas
/// beneath them are ordered.
const SUBSCHEMA_MAP_KEYWORDS: &[&str] = &[
    "$defs",
    "definitions",
    "dependentSchemas",
    "patternProperties",
];

/// The contract order of an object schema's properties: required names in
/// `required`-array order, then the optional ones lexically. Empty when the
/// schema declares no `properties`. A `required` name with no matching
/// property is skipped, since there is nothing to list.
pub fn property_order(schema: &Value) -> Vec<&str> {
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut order: Vec<&str> = Vec::with_capacity(properties.len());
    for name in schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        if let Some((key, _)) = properties.get_key_value(name) {
            if !order.contains(&key.as_str()) {
                order.push(key);
            }
        }
    }
    let optional: Vec<&str> = properties
        .keys()
        .map(String::as_str)
        .filter(|key| !order.contains(key))
        .collect();
    order.extend(optional);
    order
}

/// Serializes a JSON Schema with every `properties` object in contract order.
pub struct OrderedSchema<'a>(pub &'a Value);

impl Serialize for OrderedSchema<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::Object(map) => {
                let mut out = serializer.serialize_map(Some(map.len()))?;
                for (keyword, value) in map {
                    match (keyword.as_str(), value) {
                        ("properties", Value::Object(properties)) => out.serialize_entry(
                            keyword,
                            &OrderedProperties {
                                properties,
                                order: property_order(self.0),
                            },
                        )?,
                        (keyword_name, Value::Object(children))
                            if SUBSCHEMA_MAP_KEYWORDS.contains(&keyword_name) =>
                        {
                            out.serialize_entry(keyword, &SubschemaMap(children))?
                        }
                        (keyword_name, _) if SUBSCHEMA_KEYWORDS.contains(&keyword_name) => {
                            out.serialize_entry(keyword, &OrderedSchema(value))?
                        }
                        _ => out.serialize_entry(keyword, value)?,
                    }
                }
                out.end()
            }
            // Reached only through a keyword that takes a list of subschemas
            // (`anyOf`, `prefixItems`, tuple-form `items`).
            Value::Array(schemas) => {
                let mut out = serializer.serialize_seq(Some(schemas.len()))?;
                for schema in schemas {
                    out.serialize_element(&OrderedSchema(schema))?;
                }
                out.end()
            }
            other => other.serialize(serializer),
        }
    }
}

struct OrderedProperties<'a> {
    properties: &'a Map<String, Value>,
    order: Vec<&'a str>,
}

impl Serialize for OrderedProperties<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut out = serializer.serialize_map(Some(self.order.len()))?;
        for name in &self.order {
            out.serialize_entry(name, &OrderedSchema(&self.properties[*name]))?;
        }
        out.end()
    }
}

struct SubschemaMap<'a>(&'a Map<String, Value>);

impl Serialize for SubschemaMap<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut out = serializer.serialize_map(Some(self.0.len()))?;
        for (name, schema) in self.0 {
            out.serialize_entry(name, &OrderedSchema(schema))?;
        }
        out.end()
    }
}

/// Serializes a value shaped by a schema with each object's keys in the
/// schema's contract order, so an example shows the model the same sequence
/// the grammar enforces. Keys the schema does not declare follow, sorted.
///
/// Follows `properties`, `additionalProperties`, `items`, `prefixItems`, local
/// `$ref`s (`#/...`) resolved against the root schema, and the `allOf`,
/// `anyOf`, and `oneOf` branches that can apply to the value. [`OrderedSchema`]
/// orders every one of those branches on the wire, so an example nested under a
/// nullable object (`anyOf: [{object}, {type: null}]`) must follow its object
/// branch too. A branch is ruled out when the value's type, `const`, or `enum`
/// excludes it, when the value lacks one of its required keys, when it is
/// closed (`additionalProperties: false`) to a key the value has, or when a
/// declared field's own `type`/`const`/`enum` (a discriminator) excludes it.
///
/// When the remaining branches that declare properties list the value's keys
/// in different orders, no single order agrees with the grammar, and
/// serialization fails with an error naming the location rather than guessing.
pub struct OrderedInstance<'a> {
    value: &'a Value,
    schemas: Vec<&'a Value>,
    root: &'a Value,
    path: String,
}

impl<'a> OrderedInstance<'a> {
    pub fn new(value: &'a Value, schema: &'a Value) -> Self {
        Self {
            value,
            schemas: vec![schema],
            root: schema,
            path: String::new(),
        }
    }

    fn child(&self, value: &'a Value, schemas: Vec<&'a Value>, step: &str) -> Self {
        Self {
            value,
            schemas,
            root: self.root,
            path: format!("{}/{step}", self.path),
        }
    }

    /// Every schema that constrains this value: the candidates handed down,
    /// with references resolved and the composition branches that admit the
    /// value expanded.
    fn applicable(&self) -> Vec<&'a Value> {
        let mut out = Vec::new();
        for schema in &self.schemas {
            collect_applicable(schema, self.value, self.root, 0, &mut out);
        }
        out
    }

    fn location(&self) -> String {
        if self.path.is_empty() {
            "the top level".to_string()
        } else {
            format!("`{}`", self.path)
        }
    }
}

/// A value shaped by `schema` as compact JSON in contract order, or why no
/// single order can be derived for it (see [`OrderedInstance`]).
pub fn ordered_instance_json(value: &Value, schema: &Value) -> Result<String, String> {
    serde_json::to_string(&OrderedInstance::new(value, schema)).map_err(|e| e.to_string())
}

/// Bounds `$ref` chains and composition nesting so a self-referential schema
/// cannot loop.
const MAX_SCHEMA_DEPTH: usize = 16;

fn resolve_ref<'a>(mut schema: &'a Value, root: &'a Value) -> Option<&'a Value> {
    for _ in 0..MAX_SCHEMA_DEPTH {
        match schema
            .get("$ref")
            .and_then(Value::as_str)
            .and_then(|reference| reference.strip_prefix('#'))
        {
            Some(pointer) => schema = root.pointer(pointer)?,
            None => return Some(schema),
        }
    }
    None
}

fn collect_applicable<'a>(
    schema: &'a Value,
    value: &Value,
    root: &'a Value,
    depth: usize,
    out: &mut Vec<&'a Value>,
) {
    if depth > MAX_SCHEMA_DEPTH {
        return;
    }
    let Some(schema) = resolve_ref(schema, root) else {
        return;
    };
    if !admits(schema, value) {
        return;
    }
    out.push(schema);
    for keyword in ["allOf", "anyOf", "oneOf"] {
        for branch in schema
            .get(keyword)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            collect_applicable(branch, value, root, depth + 1, out);
        }
    }
}

/// Whether `schema` can describe `value`, judged on the cheap structural facts
/// that tell composition branches apart. Not full validation: the caller has
/// already validated the value against the whole schema.
fn admits(schema: &Value, value: &Value) -> bool {
    if !admits_shallow(schema, value) {
        return false;
    }
    let Value::Object(fields) = value else {
        return true;
    };
    let has_required = schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .all(|name| fields.contains_key(name));
    if !has_required {
        return false;
    }
    let properties = schema.get("properties").and_then(Value::as_object);
    let closed = schema.get("additionalProperties") == Some(&Value::Bool(false))
        && schema.get("patternProperties").is_none();
    if closed
        && !fields
            .keys()
            .all(|name| properties.is_some_and(|properties| properties.contains_key(name)))
    {
        return false;
    }
    properties
        .into_iter()
        .flatten()
        .filter_map(|(name, field_schema)| Some((field_schema, fields.get(name)?)))
        .all(|(field_schema, field)| admits_shallow(field_schema, field))
}

/// The schema's own `type`, `const`, and `enum` admit the value.
fn admits_shallow(schema: &Value, value: &Value) -> bool {
    if schema
        .get("const")
        .is_some_and(|expected| expected != value)
    {
        return false;
    }
    if let Some(Value::Array(options)) = schema.get("enum") {
        if !options.contains(value) {
            return false;
        }
    }
    match schema.get("type") {
        Some(Value::String(name)) => type_matches(name, value),
        Some(Value::Array(names)) => names
            .iter()
            .filter_map(Value::as_str)
            .any(|name| type_matches(name, value)),
        _ => true,
    }
}

fn type_matches(name: &str, value: &Value) -> bool {
    match (name, value) {
        ("null", Value::Null)
        | ("boolean", Value::Bool(_))
        | ("string", Value::String(_))
        | ("array", Value::Array(_))
        | ("object", Value::Object(_))
        | ("number", Value::Number(_)) => true,
        ("integer", Value::Number(number)) => {
            number.is_i64()
                || number.is_u64()
                || number.as_f64().is_some_and(|float| float.fract() == 0.0)
        }
        _ => false,
    }
}

impl Serialize for OrderedInstance<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.value {
            Value::Object(fields) => {
                let applicable = self.applicable();
                let shaping: Vec<&Value> = applicable
                    .iter()
                    .copied()
                    .filter(|schema| schema.get("properties").is_some_and(Value::is_object))
                    .collect();
                let mut declared: Vec<&str> = Vec::new();
                for schema in &shaping {
                    let order: Vec<&str> = property_order(schema)
                        .into_iter()
                        .filter(|name| fields.contains_key(*name))
                        .collect();
                    if order.is_empty() || order == declared {
                        continue;
                    }
                    if !declared.is_empty() {
                        return Err(S::Error::custom(format!(
                            "the schema branches that apply at {} list its keys in different orders ({} versus {}), so no one order agrees with what a grammar-constrained provider generates",
                            self.location(),
                            declared.join(", "),
                            order.join(", "),
                        )));
                    }
                    declared = order;
                }
                let mut out = serializer.serialize_map(Some(fields.len()))?;
                for name in &declared {
                    let subschemas = shaping
                        .iter()
                        .filter_map(|schema| schema["properties"].get(*name))
                        .collect();
                    out.serialize_entry(name, &self.child(&fields[*name], subschemas, name))?;
                }
                let extra_schemas: Vec<&Value> = applicable
                    .iter()
                    .filter_map(|schema| schema.get("additionalProperties"))
                    .filter(|value| value.is_object())
                    .collect();
                for (name, value) in fields {
                    if !declared.contains(&name.as_str()) {
                        out.serialize_entry(name, &self.child(value, extra_schemas.clone(), name))?;
                    }
                }
                out.end()
            }
            Value::Array(elements) => {
                let applicable = self.applicable();
                let mut out = serializer.serialize_seq(Some(elements.len()))?;
                for (index, element) in elements.iter().enumerate() {
                    let subschemas = applicable
                        .iter()
                        .filter_map(|schema| {
                            schema
                                .get("prefixItems")
                                .and_then(Value::as_array)
                                .and_then(|prefix| prefix.get(index))
                                .or_else(|| match schema.get("items") {
                                    Some(Value::Array(tuple)) => tuple.get(index),
                                    Some(items @ Value::Object(_)) => Some(items),
                                    _ => None,
                                })
                        })
                        .collect();
                    out.serialize_element(&self.child(element, subschemas, &index.to_string()))?;
                }
                out.end()
            }
            value => value.serialize(serializer),
        }
    }
}

/// Serializes a request body whose schema lives at `path` (for example
/// `response_format.json_schema.schema`) with that schema in contract order and
/// everything else as-is. A body without the path serializes unchanged.
pub struct OrderedSchemaAt<'a> {
    body: &'a Value,
    path: &'a [&'a str],
}

impl<'a> OrderedSchemaAt<'a> {
    pub fn new(body: &'a Value, path: &'a [&'a str]) -> Self {
        Self { body, path }
    }
}

impl Serialize for OrderedSchemaAt<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Some((head, rest)) = self.path.split_first() else {
            return OrderedSchema(self.body).serialize(serializer);
        };
        let Value::Object(map) = self.body else {
            return self.body.serialize(serializer);
        };
        let mut out = serializer.serialize_map(Some(map.len()))?;
        for (key, value) in map {
            if key == head {
                out.serialize_entry(key, &OrderedSchemaAt::new(value, rest))?;
            } else {
                out.serialize_entry(key, value)?;
            }
        }
        out.end()
    }
}

/// A schema as compact JSON in contract order, for a transport that takes the
/// schema as text (a CLI flag, say) rather than inside a serialized body.
pub fn ordered_schema_json(schema: &Value) -> String {
    serde_json::to_string(&OrderedSchema(schema)).expect("a JSON value always serializes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn position(text: &str, key: &str) -> usize {
        text.find(&format!("\"{key}\":"))
            .unwrap_or_else(|| panic!("`{key}` missing from {text}"))
    }

    fn assert_listed_in_order(text: &str, keys: &[&str]) {
        for pair in keys.windows(2) {
            assert!(
                position(text, pair[0]) < position(text, pair[1]),
                "`{}` must precede `{}` in {text}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn required_properties_lead_in_their_declared_order_then_optional_ones_sort() {
        let schema = json!({
            "type": "object",
            "properties": {
                "zeta": {}, "alpha": {}, "title": {}, "body": {}, "mid": {}
            },
            "required": ["title", "body", "title", "ghost"]
        });
        // A repeated required name is listed once, and one that names no
        // property is skipped rather than invented.
        assert_eq!(
            property_order(&schema),
            ["title", "body", "alpha", "mid", "zeta"]
        );
        assert!(property_order(&json!({ "type": "string" })).is_empty());
    }

    #[test]
    fn every_nested_subschema_is_ordered() {
        let object = |a: &str, b: &str| {
            json!({
                "type": "object",
                "properties": { a: {}, b: {} },
                "required": [b]
            })
        };
        // Each pair is sorted `a1 < b1`, and requiring `b1` must flip it.
        let schema = json!({
            "type": "object",
            "properties": {
                "list": { "type": "array", "items": object("a1", "b1") },
                "choice": { "anyOf": [object("a2", "b2"), { "type": "null" }] },
                "tuple": { "prefixItems": [object("a3", "b3")] },
                "map": { "additionalProperties": object("a4", "b4") },
                "link": { "$ref": "#/$defs/linked" }
            },
            "$defs": { "linked": object("a5", "b5") }
        });
        let text = ordered_schema_json(&schema);
        for level in 1..=5 {
            assert_listed_in_order(&text, &[&format!("b{level}"), &format!("a{level}")]);
        }
        // Only order changes: the schema means exactly what it did.
        assert_eq!(serde_json::from_str::<Value>(&text).unwrap(), schema);
    }

    #[test]
    fn keyword_values_that_are_not_schemas_are_left_alone() {
        // `enum`, `const`, `default`, and `examples` hold instances, and an
        // instance can have a key named `properties` without being a schema.
        let schema = json!({
            "type": "object",
            "properties": { "b": {}, "a": {} },
            "required": ["b"],
            "default": { "properties": { "z": 1, "y": 2 }, "required": ["z"] }
        });
        let text = ordered_schema_json(&schema);
        assert!(
            text.contains(r#""default":{"properties":{"y":2,"z":1}"#),
            "{text}"
        );
        assert_listed_in_order(&text, &["b", "a"]);
    }

    #[test]
    fn an_instance_is_written_in_its_schemas_order() {
        let schema = json!({
            "type": "object",
            "properties": {
                "title": {}, "content": {}, "summary": {},
                "steps": {
                    "type": "array",
                    "items": { "$ref": "#/$defs/step" }
                }
            },
            "required": ["title", "content", "steps"],
            "$defs": {
                "step": {
                    "type": "object",
                    "properties": { "id": {}, "note": {}, "agent": {} },
                    "required": ["id", "agent"]
                }
            }
        });
        let value = json!({
            "summary": "s", "content": "c", "title": "t", "extra": true,
            "steps": [{ "note": "n", "agent": "a", "id": "1" }]
        });
        let text = serde_json::to_string(&OrderedInstance::new(&value, &schema)).unwrap();
        assert_eq!(
            text,
            r#"{"title":"t","content":"c","steps":[{"id":"1","agent":"a","note":"n"}],"summary":"s","extra":true}"#
        );
    }

    /// `result` is a nullable object whose sorted key order (`summary`,
    /// `title`) is the reverse of its contract order.
    fn nullable_result_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "result": {
                    "anyOf": [
                        {
                            "type": "object",
                            "properties": {
                                "title": { "type": "string" },
                                "summary": { "type": "string" }
                            },
                            "required": ["title"]
                        },
                        { "type": "null" }
                    ]
                }
            },
            "required": ["result"]
        })
    }

    #[test]
    fn an_instance_under_a_nullable_object_follows_its_object_branch() {
        let schema = nullable_result_schema();
        let value = json!({ "result": { "summary": "s", "title": "t" } });
        let shown = ordered_instance_json(&value, &schema).unwrap();
        assert_eq!(shown, r#"{"result":{"title":"t","summary":"s"}}"#);
        // The same branch, as the wire lists it.
        assert_listed_in_order(&ordered_schema_json(&schema), &["title", "summary"]);
        assert_eq!(
            ordered_instance_json(&json!({ "result": null }), &schema).unwrap(),
            r#"{"result":null}"#
        );
    }

    #[test]
    fn a_discriminated_branch_is_the_one_that_orders_the_instance() {
        // Each branch reverses the other's order; `kind` rules one out.
        let branch = |kind: &str, first: &str, second: &str| {
            json!({
                "type": "object",
                "properties": {
                    "kind": { "const": kind },
                    "alpha": { "type": "string" },
                    "beta": { "type": "string" }
                },
                "required": ["kind", first, second]
            })
        };
        let schema = json!({
            "oneOf": [branch("ab", "alpha", "beta"), branch("ba", "beta", "alpha")]
        });
        let value = json!({ "kind": "ba", "alpha": "a", "beta": "b" });
        assert_eq!(
            ordered_instance_json(&value, &schema).unwrap(),
            r#"{"kind":"ba","beta":"b","alpha":"a"}"#
        );
    }

    #[test]
    fn branches_that_disagree_on_order_are_refused_rather_than_guessed() {
        let branch = |required: &str| {
            json!({
                "type": "object",
                "properties": { "alpha": {}, "beta": {} },
                "required": [required]
            })
        };
        let schema = json!({
            "type": "object",
            "properties": { "pick": { "anyOf": [branch("alpha"), branch("beta")] } }
        });
        let value = json!({ "pick": { "alpha": "a", "beta": "b" } });
        let refused = ordered_instance_json(&value, &schema).unwrap_err();
        assert!(
            refused.contains("`/pick`") && refused.contains("different orders"),
            "{refused}"
        );
    }

    #[test]
    fn only_the_schema_at_the_path_is_reordered() {
        let body = json!({
            "tools": { "properties": { "b": {}, "a": {} }, "required": ["b"] },
            "format": { "schema": { "properties": { "d": {}, "c": {} }, "required": ["d"] } }
        });
        let text =
            serde_json::to_string(&OrderedSchemaAt::new(&body, &["format", "schema"])).unwrap();
        assert_listed_in_order(&text, &["a", "b"]);
        assert_listed_in_order(&text, &["d", "c"]);
        // A body that does not carry the path is byte-identical to plain
        // serialization.
        let plain = json!({ "model": "m", "format": "text" });
        assert_eq!(
            serde_json::to_string(&OrderedSchemaAt::new(&plain, &["format", "schema"])).unwrap(),
            plain.to_string()
        );
    }
}
