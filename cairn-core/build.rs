use std::{env, fs, path::PathBuf};

fn main() {
    let ui_actions = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("../../../packages/ui/src/actions");
    let manifest = ui_actions.join("definitions.ts");
    let types = ui_actions.join("types.ts");
    println!("cargo:rerun-if-changed={}", manifest.display());
    println!("cargo:rerun-if-changed={}", types.display());
    let source = fs::read_to_string(&manifest).expect("read frontend action manifest");
    let types_source = fs::read_to_string(&types).expect("read frontend action types");
    let key_aliases = key_aliases(&types_source);
    let mut actions = Vec::new();
    for block in source.split("  {").skip(1) {
        let Some(id) = string_field(block, "id:") else {
            continue;
        };
        let Some(contexts) = array_field(block, "contexts:") else {
            continue;
        };
        let default = sequence_field(block, "defaultSequence:")
            .unwrap_or_else(|| panic!("action {id} has no default sequence"));
        let alternatives = sequence_list_field(block, "alternativeSequences:");
        actions.push((id, contexts, default, alternatives));
    }
    assert!(
        !actions.is_empty(),
        "frontend action manifest yielded no actions"
    );
    let generated = actions
        .into_iter()
        .map(|(id, contexts, default, alternatives)| {
            let contexts = contexts
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", ");
            let default = rust_sequence(&default, &key_aliases);
            let alternatives = alternatives
                .iter()
                .map(|sequence| rust_sequence(sequence, &key_aliases))
                .collect::<Vec<_>>()
                .join(", ");
            format!("    ActionMetadata {{ id: \"{id}\", contexts: &[{contexts}], default_sequence: {default}, alternative_sequences: &[{alternatives}] }},\n")
        })
        .collect::<String>();
    let generated_aliases = key_aliases
        .iter()
        .map(|(alias, canonical)| format!("    ({alias:?}, {canonical:?}),\n"))
        .collect::<String>();
    fs::write(
        PathBuf::from(env::var("OUT_DIR").unwrap()).join("keybind_actions.rs"),
        format!(
            "pub(crate) const GENERATED_KEY_ALIASES: &[(&str, &str)] = &[\n{generated_aliases}];\n\
             pub(crate) const GENERATED_ACTIONS: &[ActionMetadata] = &[\n{generated}];\n"
        ),
    )
    .expect("write generated action metadata");
}

fn normalize_key(key: &str, aliases: &[(String, String)]) -> String {
    let lower = key.to_lowercase();
    aliases
        .iter()
        .find_map(|(alias, canonical)| (alias == &lower).then(|| canonical.clone()))
        .unwrap_or_else(|| {
            if key.chars().count() == 1 {
                lower
            } else {
                key.to_string()
            }
        })
}

fn key_aliases(source: &str) -> Vec<(String, String)> {
    let body = source
        .split_once("const special: Record<string, string> = {")
        .expect("normalizeKey special alias table")
        .1
        .split_once("};")
        .expect("normalizeKey special alias table terminator")
        .0;
    body.lines()
        .flat_map(|line| line.split(','))
        .filter_map(|entry| {
            let (alias, canonical) = entry.split_once(':')?;
            Some((
                alias.trim().trim_matches('"').to_string(),
                canonical.trim().trim_matches('"').to_string(),
            ))
        })
        .collect()
}

fn sequence_field(block: &str, field: &str) -> Option<Vec<String>> {
    let rest = block.split_once(field)?.1.trim_start();
    let expression = balanced(rest, '(', ')')?;
    Some(parse_sequence(expression))
}

fn sequence_list_field(block: &str, field: &str) -> Vec<Vec<String>> {
    let Some(rest) = block.split_once(field).map(|(_, rest)| rest.trim_start()) else {
        return Vec::new();
    };
    let Some(list) = balanced(rest, '[', ']') else {
        return Vec::new();
    };
    let mut sequences = Vec::new();
    let mut remaining = list;
    while let Some(index) = remaining.find("keySequence") {
        remaining = &remaining[index..];
        let Some(expression) = balanced(remaining, '(', ')') else {
            break;
        };
        sequences.push(parse_sequence(expression));
        remaining = &remaining[expression.len()..];
    }
    sequences
}

fn balanced(value: &str, open: char, close: char) -> Option<&str> {
    let start = value.find(open)?;
    let mut depth = 0;
    for (offset, character) in value[start..].char_indices() {
        if character == open {
            depth += 1;
        }
        if character == close {
            depth -= 1;
            if depth == 0 {
                return Some(&value[..start + offset + character.len_utf8()]);
            }
        }
    }
    None
}

fn parse_sequence(expression: &str) -> Vec<String> {
    let mut strokes = Vec::new();
    let mut remaining = expression;
    while let Some(index) = remaining.find("keybind(") {
        remaining = &remaining[index + "keybind".len()..];
        let call = balanced(remaining, '(', ')').expect("balanced keybind call");
        let arguments = call
            .trim_start_matches('(')
            .trim_end_matches(')')
            .split(',')
            .map(|value| value.trim().trim_matches('"'))
            .collect::<Vec<_>>();
        let key = arguments[0].to_string();
        let mut modifiers = arguments[1..].to_vec();
        modifiers.sort_by_key(|modifier| match *modifier {
            "meta" => 0,
            "ctrl" => 1,
            "alt" => 2,
            "shift" => 3,
            other => panic!("unknown modifier {other}"),
        });
        strokes.push(format!("{}:{key}", modifiers.join("+")));
        remaining = &remaining[call.len()..];
    }
    strokes
}

fn rust_sequence(sequence: &[String], aliases: &[(String, String)]) -> String {
    format!(
        "&[{}]",
        sequence
            .iter()
            .map(|stroke| {
                let (modifiers, key) = stroke.split_once(':').expect("stroke signature");
                let key = normalize_key(key, aliases).to_lowercase();
                format!("{:?}", format!("{modifiers}:{key}"))
            })
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn string_field(block: &str, field: &str) -> Option<String> {
    let rest = block.split_once(field)?.1.trim_start();
    let rest = rest.strip_prefix('"')?;
    Some(rest.split_once('"')?.0.to_string())
}

fn array_field(block: &str, field: &str) -> Option<Vec<String>> {
    let rest = block.split_once(field)?.1;
    let body = rest.split_once('[')?.1.split_once(']')?.0;
    Some(
        body.split(',')
            .filter_map(|part| {
                let value = part.trim().trim_matches('"');
                (!value.is_empty()).then(|| value.to_string())
            })
            .collect(),
    )
}
