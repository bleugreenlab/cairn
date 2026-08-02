//! What the lane running this suite actually grants, asserted rather than assumed.
//!
//! Every conditional skip in this repository is a claim about the environment,
//! and those claims were wrong in both directions: `CAIRN_SANDBOXED` arrived by
//! inheritance where no sandbox existed, and a run-tool envelope was read as
//! confinement in an ordinary terminal (CAIRN-3164). A comment cannot hold that
//! ground because nothing re-checks it. This test does: it probes each capability
//! the suite asks of its environment, requires the ones the suite genuinely
//! depends on, and prints the whole report — fence markers included — when any
//! required one is missing.
//!
//! So it is two things at once. It is the enforcement: a lane that quietly loses
//! `tursodb` or `jj` goes red here instead of silently skipping dozens of tests
//! elsewhere. And it is the standing diagnostic: the answer to "what does the
//! check lane grant?" is measured on every run rather than remembered, so it
//! cannot rot.
//!
//! A nested `sandbox-exec` is deliberately REPORTED and not required. It is the
//! one capability the fence genuinely withholds, and the sandbox suites' own
//! probe (`os/cairn-sandbox/src/macos.rs`) skips on it with a declared reason.

use std::net::TcpListener;
use std::process::Command;

use crate::common;
use crate::common::sync_server;

/// One capability the suite may ask of its environment.
struct Capability {
    name: &'static str,
    /// Whether a test running here is entitled to expect it.
    required: bool,
    present: bool,
    /// How that was determined — the part that makes a failure actionable.
    detail: String,
    /// The one command that provisions this, where a command can. `None` for a
    /// capability the environment either simply has (temp files, loopback) or
    /// structurally cannot have (a nested sandbox).
    remedy: Option<&'static str>,
}

fn first_line(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .to_string()
}

fn temp_write() -> Capability {
    let probe = tempfile::tempdir().and_then(|dir| {
        let path = dir.path().join("probe");
        std::fs::write(&path, b"ok")?;
        std::fs::read(&path).map(|_| dir.path().to_path_buf())
    });
    Capability {
        name: "temp-write",
        required: true,
        present: probe.is_ok(),
        detail: match &probe {
            Ok(dir) => format!("wrote and read back a file under {}", dir.display()),
            Err(error) => format!("{error}"),
        },
        remedy: None,
    }
}

fn loopback_bind() -> Capability {
    let bound = TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr());
    Capability {
        name: "loopback-bind",
        required: true,
        present: bound.is_ok(),
        detail: match &bound {
            Ok(addr) => format!("bound {addr}"),
            Err(error) => format!("{error}"),
        },
        remedy: None,
    }
}

/// A child process that runs to completion and reports its own exit code. Suites
/// asserting timeout kills and terminal lifecycles need this to be undisturbed.
fn spawn_child() -> Capability {
    let status = Command::new("/bin/sh").args(["-c", "exit 7"]).status();
    let code = status.as_ref().ok().and_then(|s| s.code());
    Capability {
        name: "spawn-child",
        required: true,
        present: code == Some(7),
        detail: match (&status, code) {
            (Ok(_), Some(code)) => format!("/bin/sh exited {code}, asked for 7"),
            (Ok(_), None) => "child was signalled instead of exiting".to_string(),
            (Err(error), _) => format!("{error}"),
        },
        remedy: None,
    }
}

/// The sync server the 22 team-sync tests need. Required, because
/// `scripts/ensure-tursodb.ts` provisions it for every lane (`test:rust` runs it)
/// and the fence permits spawning it; a machine that genuinely cannot build it
/// declares so for the run with `CAIRN_SYNC_TESTS_OPTIONAL=1`.
///
/// The detail reports WHERE it resolved from, because "which tursodb" is the
/// question behind a silent ABI mismatch (CAIRN-2147) — a cached binary keyed to
/// the pinned rev and a stray PATH one look identical until sync misbehaves.
fn tursodb() -> Capability {
    let bin = sync_server::tursodb_bin();
    let version = bin
        .and_then(|bin| Command::new(bin).arg("--version").output().ok())
        .filter(|out| out.status.success())
        .map(|out| first_line(&out.stdout));
    let external = std::env::var("CAIRN_TEST_SYNC_URL").unwrap_or_default();
    Capability {
        name: "sync-server",
        required: std::env::var_os("CAIRN_SYNC_TESTS_OPTIONAL").is_none(),
        present: bin.is_some() || !external.is_empty(),
        detail: match (bin, &version, external.is_empty()) {
            (Some(bin), Some(version), _) => format!("{version} at {}", bin.display()),
            (Some(bin), None, _) => {
                format!("resolved {} but could not read its version", bin.display())
            }
            (None, _, false) => format!("external CAIRN_TEST_SYNC_URL={external}"),
            (None, _, true) => "no tursodb resolvable and no CAIRN_TEST_SYNC_URL".to_string(),
        },
        remedy: Some("bun run ensure-tursodb"),
    }
}

/// jj gates roughly 65 tests in `cairn-core`'s jj module, none of which record a
/// skip — so its absence would be invisible everywhere except here.
fn jj() -> Capability {
    let bin = common::jj_bin();
    let version = bin.as_ref().and_then(|bin| {
        Command::new(bin)
            .arg("--version")
            .output()
            .ok()
            .map(|out| first_line(&out.stdout))
    });
    Capability {
        name: "jj",
        required: true,
        present: bin.is_some(),
        detail: match (bin, version) {
            (Some(bin), Some(version)) => format!("{version} at {bin}"),
            (Some(bin), None) => format!("resolved {bin} but could not read its version"),
            (None, _) => "no build-slot, explicit, or PATH jj resolvable".to_string(),
        },
        // Deliberately no remedy: the suite bootstrap already attempted to
        // provision the exact host artifact that `jj_bin` prefers, and the two
        // remaining answers (an explicit path or PATH install) are host-specific.
        remedy: None,
    }
}

/// Reported, never required: the capability the fence legitimately withholds.
/// Probed the same way the sandbox suites probe it — an in-bounds write under a
/// nested profile — because macOS sandboxes do not nest.
#[cfg(target_os = "macos")]
fn nested_sandbox_exec() -> Capability {
    let outcome = tempfile::tempdir().map(|dir| {
        let root = dir.path().canonicalize().unwrap_or_else(|_| dir.path().into());
        let probe = root.join("probe");
        let profile = format!(
            "(version 1)(allow default)(deny file-write* (subpath \"/\"))(allow file-write* (subpath \"{}\"))",
            root.display()
        );
        let ran = Command::new("sandbox-exec")
            .args([
                "-p",
                &profile,
                "/bin/sh",
                "-c",
                &format!("echo ok > {}", probe.display()),
            ])
            .output();
        match ran {
            Ok(out) if out.status.success() && probe.exists() => {
                (true, "an in-bounds write succeeded under a nested profile".to_string())
            }
            Ok(out) => (
                false,
                format!(
                    "in-bounds write refused (exit {:?}): {}",
                    out.status.code(),
                    first_line(&out.stderr)
                ),
            ),
            Err(error) => (false, format!("{error}")),
        }
    });
    let (present, detail) = outcome.unwrap_or_else(|error| (false, format!("{error}")));
    Capability {
        name: "nested-sandbox-exec",
        required: false,
        present,
        detail,
        remedy: None,
    }
}

#[cfg(not(target_os = "macos"))]
fn nested_sandbox_exec() -> Capability {
    Capability {
        name: "nested-sandbox-exec",
        required: false,
        present: false,
        detail: format!("not applicable on {}", std::env::consts::OS),
        remedy: None,
    }
}

/// The fence markers themselves, which is what makes this the permanent answer to
/// "was this process confined, and did anything ACT on that?" A marker present
/// beside a working nested `sandbox-exec` means it was inherited rather than
/// decided — the report says so instead of a test guessing.
fn fence_markers() -> String {
    [
        "CAIRN_SANDBOXED",
        "CAIRN_WORKTREE",
        "CAIRN_RUN_ID",
        "CAIRN_CALLBACK_URL",
    ]
    .iter()
    .map(|name| match std::env::var(name) {
        Ok(value) if !value.is_empty() => format!("{name}=set"),
        _ => format!("{name}=unset"),
    })
    .collect::<Vec<_>>()
    .join(" ")
}

fn report(capabilities: &[Capability]) -> String {
    let mut out = String::from("lane capability report\n");
    for capability in capabilities {
        out.push_str(&format!(
            "  {} {:<20} {:<8} {}\n",
            if capability.present {
                '\u{2713}'
            } else {
                '\u{2717}'
            },
            capability.name,
            if capability.required {
                "required"
            } else {
                "reported"
            },
            capability.detail
        ));
    }
    out.push_str(&format!("  fence markers: {}\n", fence_markers()));
    // A report that only names what is missing leaves the reader to go find the
    // fix; the whole point of this test going red is that someone can clear it.
    for remedy in capabilities
        .iter()
        .filter(|capability| capability.required && !capability.present)
        .filter_map(|capability| capability.remedy)
    {
        out.push_str(&format!("  fix: {remedy}\n"));
    }
    out
}

#[test]
fn the_lane_grants_every_capability_this_suite_requires() {
    let capabilities = vec![
        temp_write(),
        loopback_bind(),
        spawn_child(),
        tursodb(),
        jj(),
        nested_sandbox_exec(),
    ];
    let missing: Vec<&str> = capabilities
        .iter()
        .filter(|capability| capability.required && !capability.present)
        .map(|capability| capability.name)
        .collect();
    assert!(
        missing.is_empty(),
        "this lane is missing a capability the suite depends on: {}.\n\n{}\nTests needing these \
         must NOT be skipped past a missing capability — that is how a coverage hole earns a green \
         checkmark (CAIRN-3164). Run the fix above, or route the suite to a lane that has it.",
        missing.join(", "),
        report(&capabilities)
    );
}
