//! Exercise Cargo's actual directory-watching behavior, not a mock of it.
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Command;

struct Workspace {
    _temp: tempfile::TempDir,
    root: PathBuf,
}

struct Build {
    identity: String,
    fresh: bool,
}

impl Workspace {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("src-tauri");
        write(
            &root.join("Cargo.toml"),
            "[workspace]\nmembers = [\"os/probe\"]\nresolver = \"2\"\n",
        );
        write(
            &root.join("os/probe/Cargo.toml"),
            "[package]\nname = \"probe\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        );
        write(
            &root.join("os/probe/src/lib.rs"),
            "pub const ID: &str = env!(\"CAIRN_CHECK_IMPLEMENTATION_ID\");\n",
        );
        write(&root.join("os/probe/build.rs"), include_str!("../build.rs"));
        write(&root.join("os/subject/src/lib.rs"), "// implementation\n");
        Self { _temp: temp, root }
    }

    fn build(&self) -> Build {
        let output = Command::new(env!("CARGO"))
            .args(["build", "--offline", "--message-format=json"])
            .current_dir(&self.root)
            // The fixture is a standalone, dependency-free workspace. It must
            // not borrow the enclosing build's target directory or wrapper.
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("RUSTC_WRAPPER")
            .env_remove("RUSTC_WORKSPACE_WRAPPER")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let messages: Vec<Value> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
        let identity = messages
            .iter()
            .find(|message| message["reason"] == "build-script-executed")
            .unwrap()["env"]
            .as_array()
            .unwrap()
            .iter()
            .find(|pair| pair[0] == "CAIRN_CHECK_IMPLEMENTATION_ID")
            .unwrap()[1]
            .as_str()
            .unwrap()
            .to_string();
        let fresh = messages
            .iter()
            .find(|message| {
                message["reason"] == "compiler-artifact" && message["target"]["kind"][0] == "lib"
            })
            .unwrap()["fresh"]
            .as_bool()
            .unwrap();
        Build { identity, fresh }
    }
}

fn write(path: &Path, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

#[test]
fn cargo_outputs_cannot_invalidate_an_unchanged_implementation() {
    let workspace = Workspace::new();
    let first = workspace.build();
    let second = workspace.build();
    assert_eq!(first.identity, second.identity);
    assert!(
        second.fresh,
        "an unchanged build recompiled the identity crate"
    );

    for output in ["target/generated.rs", "binaries/generated.rs"] {
        write(&workspace.root.join(output), "// generated build output\n");
    }
    let third = workspace.build();
    assert_eq!(first.identity, third.identity);
    assert!(third.fresh, "build outputs triggered a source rebuild");
}

#[test]
fn source_edits_additions_and_removals_change_the_identity() {
    let workspace = Workspace::new();
    let initial = workspace.build().identity;
    let source = workspace.root.join("os/subject/src/lib.rs");
    write(&source, "// changed implementation\n");
    let edited = workspace.build().identity;
    assert_ne!(initial, edited);

    let added = workspace.root.join("os/subject/src/new/module.rs");
    write(&added, "// new module\n");
    let extended = workspace.build().identity;
    assert_ne!(edited, extended);
    std::fs::remove_file(added).unwrap();
    assert_eq!(edited, workspace.build().identity);

    write(&source, "// implementation\n");
    assert_eq!(initial, workspace.build().identity);
}

#[test]
fn identical_checkouts_share_identity_but_manifest_changes_do_not() {
    let first = Workspace::new();
    let second = Workspace::new();
    let identity = first.build().identity;
    assert_eq!(identity, second.build().identity);
    let manifest = second.root.join("os/probe/Cargo.toml");
    let original = std::fs::read_to_string(&manifest).unwrap();
    write(&manifest, &format!("{original}\n# changed manifest\n"));
    assert_ne!(identity, second.build().identity);
}
