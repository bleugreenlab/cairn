//! The public executor name a `cairn://executors/<name>` segment carries.

/// Fold an operator-supplied label into the public name that addresses it.
///
/// Names are the address space agents type, so they are normalized on the way
/// in rather than validated and refused: `"BG Lab (Ubuntu)"` and `"bg-lab-ubuntu"`
/// are the same machine, and an agent that reads one in `cairn://executors` and
/// writes the other in a placement request must reach it. `None` means the label
/// contains nothing that can be part of an address.
pub fn normalize_executor_name(raw: &str) -> Option<String> {
    let mut name = String::with_capacity(raw.len());
    for character in raw.chars() {
        let mapped = if character.is_ascii_alphanumeric() {
            character.to_ascii_lowercase()
        } else {
            '-'
        };
        if mapped == '-' && (name.is_empty() || name.ends_with('-')) {
            continue;
        }
        name.push(mapped);
    }
    while name.ends_with('-') {
        name.pop();
    }
    (!name.is_empty()).then_some(name)
}
