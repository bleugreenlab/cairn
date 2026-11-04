//! `cairn-library-index`: derive a library source's index.
//!
//! Run by the library repository's CI on every push to `main`:
//!
//! ```text
//! cairn-library-index derive --root . \
//!     --url https://github.com/bleugreenlab/cairn-library --ref main \
//!     --commit "$GITHUB_SHA" --serial "$GITHUB_RUN_NUMBER" > index.json
//! ```
//!
//! The output is canonical JSON, exactly the bytes CI then signs with
//! minisign. Upstream repositories the marketplace references are fetched at
//! their pinned commits only to hash them; nothing from them is written out.

use cairn_packs::library::derive::{derive_index, PolicyFetch};
use cairn_packs::library::git::GitPolicy;
use cairn_packs::library::IndexSource;

const USAGE: &str =
    "usage: cairn-library-index derive --root <dir> --url <https url> --ref <branch> \
                     --commit <40-hex> --serial <n> [--generated-at <rfc3339>]";

fn main() {
    match run(std::env::args().skip(1).collect()) {
        Ok(json) => print!("{json}"),
        Err(error) => {
            eprintln!("cairn-library-index: {error}");
            std::process::exit(1);
        }
    }
}

fn run(args: Vec<String>) -> Result<String, String> {
    let mut args = args.into_iter();
    if args.next().as_deref() != Some("derive") {
        return Err(USAGE.to_string());
    }
    let (mut root, mut url, mut git_ref, mut commit, mut serial, mut generated_at) =
        (None, None, None, None, None, None);
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))?;
        match flag.as_str() {
            "--root" => root = Some(value),
            "--url" => url = Some(value),
            "--ref" => git_ref = Some(value),
            "--commit" => commit = Some(value),
            "--serial" => {
                serial = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| format!("--serial `{value}` is not a number"))?,
                )
            }
            "--generated-at" => generated_at = Some(value),
            other => return Err(format!("unknown flag {other}\n{USAGE}")),
        }
    }
    let missing = |name: &str| format!("{name} is required\n{USAGE}");
    let index = derive_index(
        std::path::Path::new(&root.ok_or_else(|| missing("--root"))?),
        IndexSource {
            url: url.ok_or_else(|| missing("--url"))?,
            git_ref: git_ref.ok_or_else(|| missing("--ref"))?,
            commit: commit.ok_or_else(|| missing("--commit"))?,
        },
        serial.ok_or_else(|| missing("--serial"))?,
        generated_at.unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
        &PolicyFetch(GitPolicy::production()),
    )?;
    index.to_canonical_json()
}
