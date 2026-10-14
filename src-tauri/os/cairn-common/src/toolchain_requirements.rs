//! The one vocabulary for what a machine must carry before it can prepare a
//! cell for a project: the toolchains an executor advertises, the programs
//! that need them, and the provisioning scripts whose verdict says whether the
//! workspace can be compiled there.
//!
//! The executor's attach probe and the runner's placement gate both read these
//! tables, so the name a machine advertises is, by construction, the name a
//! project's setup is found to require. A drift between the two would turn
//! every gate into a silent refusal (or a silent pass).

/// One toolchain an executor probes for and placement can require.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Toolchain {
    /// The advertised name, as it appears in `ExecutorCapabilities::toolchains`
    /// and in a selector's `required_toolchains`.
    pub name: &'static str,
    /// The program the executor runs (`<probe> --version`) to decide whether
    /// the toolchain is present.
    pub probe: &'static str,
    /// Every program whose invocation means the toolchain is needed. The probe
    /// program is always one of them.
    pub programs: &'static [&'static str],
}

/// The toolchain the `cairn` command line belongs to.
pub const CAIRN_CLI_TOOLCHAIN: &str = "cairn-cli";

/// Every toolchain the fleet knows how to probe for and require, in the order
/// the executor probes them.
pub const TOOLCHAINS: &[Toolchain] = &[
    Toolchain {
        name: "rust",
        probe: "cargo",
        programs: &["cargo", "rustc", "rustup"],
    },
    Toolchain {
        name: "bun",
        probe: "bun",
        programs: &["bun", "bunx"],
    },
    Toolchain {
        name: "node",
        probe: "node",
        programs: &["node"],
    },
    Toolchain {
        name: "npm",
        probe: "npm",
        programs: &["npm", "npx"],
    },
    Toolchain {
        name: CAIRN_CLI_TOOLCHAIN,
        probe: "cairn",
        programs: &["cairn"],
    },
];

/// The toolchain a program belongs to, when the fleet knows one.
pub fn toolchain_for_program(program: &str) -> Option<&'static str> {
    TOOLCHAINS
        .iter()
        .find(|toolchain| toolchain.programs.contains(&program))
        .map(|toolchain| toolchain.name)
}

/// The toolchains a project's setup commands certainly invoke, sorted and
/// deduplicated.
///
/// Every cell for a project runs its setup commands before its first command,
/// whatever the batch asked for, so what they invoke is a requirement of the
/// project on every machine that would host it. This feeds hard admission and
/// eviction, so it claims only what it can be sure of: a line contributes only
/// when it is a [`claimable_chain`] — a plain `&&` chain made only of command
/// forms whose effect on the shell is known, so that every one of them must
/// run, and succeed, for the line to. Any other line contributes nothing,
/// rather than a guess about which commands the shell will actually run.
pub fn setup_toolchain_requirements(commands: &[String]) -> Vec<String> {
    // A setup that composes its own PATH resolves programs the checkout may
    // provide (`PATH=./tools bun install`), so nothing it runs is a host
    // requirement we can be sure of.
    if commands.iter().any(|command| modifies_path(command)) {
        return Vec::new();
    }
    let mut required: Vec<String> = commands
        .iter()
        .filter_map(|command| claimable_chain(command))
        .flatten()
        .filter_map(|simple| toolchain_for_program(&simple.program))
        .map(str::to_string)
        .collect();
    required.sort();
    required.dedup();
    required
}

/// A digest of a project's setup commands. A preparation refusal recorded
/// under one setup says nothing about a different one, so it is keyed by this.
pub fn setup_identity(commands: &[String]) -> String {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    for command in commands {
        hasher.update(command.as_bytes());
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

/// Which host program a setup command that exited "command not found" could
/// not find, when that is provably a fact about the host.
///
/// Exit 127 alone proves nothing about the machine: a missing branch-owned
/// script, or a script that propagates 127 from inside, fails the same way on
/// an equipped host. So this answers only when the failed line is a
/// [`claimable_chain`], every command before the first non-builtin is a builtin the
/// executor's actual `shell` is guaranteed to have (so cannot exit 127), and
/// that first command is a bare-named known host
/// toolchain that `is_on_host_path` cannot resolve — so it is the command
/// that produced the 127. It also declines when any setup command touches
/// PATH, since resolution is then the checkout's. Anything less certain is
/// `None`, and the failure stays the branch's.
pub fn missing_host_program(
    setup_commands: &[String],
    failed_command: &str,
    shell: SetupShell,
    is_on_host_path: impl Fn(&str) -> bool,
) -> Option<String> {
    if setup_commands.iter().any(|command| modifies_path(command)) {
        return None;
    }
    let skippable = shell.builtins_that_cannot_exit_127();
    let first = claimable_chain(failed_command)?
        .into_iter()
        .find(|simple| simple.qualified || !skippable.contains(&simple.program.as_str()))?;
    (!first.qualified
        && toolchain_for_program(&first.program).is_some()
        && !is_on_host_path(&first.program))
    .then_some(first.program)
}

/// The shell an executor runs setup commands under, which decides which
/// leading commands are guaranteed builtins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupShell {
    /// `/bin/sh`: whatever POSIX shell the platform provides (dash, ash,
    /// bash), every one of which has `cd` as a builtin.
    PosixSh,
    /// `cmd.exe`, which shares no builtin vocabulary this can rely on.
    Cmd,
}

impl SetupShell {
    /// The shell this platform's executor runs setup under.
    pub fn current() -> Self {
        if cfg!(windows) {
            Self::Cmd
        } else {
            Self::PosixSh
        }
    }

    /// Leading commands that are builtins of every such shell and so cannot
    /// exit 127: a chain that did failed in a later command. Only `cd`, the
    /// one builtin [`claimable_chain`] admits; `pushd` is not a builtin of
    /// every POSIX `/bin/sh` (dash has none), and `cmd.exe` is relied on for
    /// nothing.
    fn builtins_that_cannot_exit_127(self) -> &'static [&'static str] {
        match self {
            Self::PosixSh => &["cd"],
            Self::Cmd => &[],
        }
    }
}

fn modifies_path(command: &str) -> bool {
    command
        .split(|character: char| !(character == '_' || character.is_ascii_alphanumeric()))
        .any(|word| word == "PATH")
}

/// One simple command of a setup line, as far as placement needs to know it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleCommand {
    /// The program it runs (file name, without `.exe`/`.cmd`), after leading
    /// assignments and a plain `env NAME=value` prefix.
    pub program: String,
    /// Whether the program was named with a path rather than looked up on PATH.
    pub qualified: bool,
    /// The words after the program.
    pub arguments: Vec<String>,
}

/// The commands of a setup line whose effect on which commands run is fully
/// known, or `None`.
///
/// An [`and_chain`] alone is not enough: a command that changes the shell's
/// own control flow (`exit 0 && bun install`, `set -n && bun install`) makes a
/// later command not run while the line still succeeds. So every command must
/// be one of the forms this supports — `cd` into one directory, or a
/// bare-named known host toolchain, which runs as its own process and can
/// affect the line only by succeeding or failing. Anything else (another
/// builtin, a script, a program no toolchain owns) declines the whole line.
pub fn claimable_chain(line: &str) -> Option<Vec<SimpleCommand>> {
    let chain = and_chain(line)?;
    chain
        .iter()
        .all(|simple| {
            !simple.qualified
                && match simple.program.as_str() {
                    "cd" => {
                        simple.arguments.len() <= 1
                            && simple
                                .arguments
                                .iter()
                                .all(|argument| !argument.starts_with('-'))
                    }
                    program => toolchain_for_program(program).is_some(),
                }
        })
        .then_some(chain)
}

/// The simple commands of a line that is nothing but simple commands joined
/// by `&&`, in order, or `None` for any other line.
///
/// Deliberately not a shell parser. Quotes and escapes are honoured so a
/// separator inside quotes is data; outside them, any construct whose effect
/// on which commands run is not simply "each must succeed" — `;`, newline,
/// `||`, `|`, a lone `&`, `#`, `<`, `$(`, backticks, parentheses, braces —
/// makes the whole line unclaimable, as does a wrapper (`exec`, `command`,
/// `env` with a flag, `sudo`, and the like). Output redirections (`>`, `2>&1`) are
/// kept as arguments. A segment that runs nothing (`command -v x`, only
/// assignments) also makes the line unclaimable.
pub fn and_chain(line: &str) -> Option<Vec<SimpleCommand>> {
    let mut segments: Vec<Vec<String>> = Vec::new();
    let mut words: Vec<String> = Vec::new();
    let mut word = String::new();
    let mut in_word = false;
    let mut previous: Option<char> = None;
    let mut chars = line.trim().chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '\\' => {
                let escaped = chars.next()?;
                if escaped == '\n' {
                    return None;
                }
                word.push(escaped);
                in_word = true;
            }
            '\'' => {
                in_word = true;
                loop {
                    match chars.next()? {
                        '\'' => break,
                        quoted => word.push(quoted),
                    }
                }
            }
            '"' => {
                in_word = true;
                loop {
                    match chars.next()? {
                        '"' => break,
                        '\\' => word.push(chars.next()?),
                        '$' | '`' => return None,
                        quoted => word.push(quoted),
                    }
                }
            }
            '&' if chars.peek() == Some(&'&') => {
                chars.next();
                if in_word {
                    words.push(std::mem::take(&mut word));
                    in_word = false;
                }
                segments.push(std::mem::take(&mut words));
            }
            // `2>&1`, `&>file`: redirections, kept as an argument.
            '&' if previous == Some('>') || chars.peek() == Some(&'>') => {
                word.push(character);
                in_word = true;
            }
            '$' if chars.peek() == Some(&'(') => return None,
            ';' | '\n' | '|' | '&' | '#' | '<' | '`' | '(' | ')' | '{' | '}' => return None,
            other if other.is_whitespace() => {
                if in_word {
                    words.push(std::mem::take(&mut word));
                    in_word = false;
                }
            }
            other => {
                word.push(other);
                in_word = true;
            }
        }
        previous = Some(character);
    }
    if in_word {
        words.push(word);
    }
    segments.push(words);
    segments
        .into_iter()
        .map(|words| {
            let program = program_word(&words)?;
            let position = words
                .iter()
                .position(|word| std::ptr::eq(word.as_str(), program))?;
            Some(SimpleCommand {
                program: program_name(program).to_string(),
                qualified: program.contains(['/', '\\']),
                arguments: words[position + 1..].to_vec(),
            })
        })
        .collect()
}

/// The word naming the program a simple command runs, past assignments and
/// the wrappers that run their argument.
fn program_word(words: &[String]) -> Option<&str> {
    let mut rest = words
        .iter()
        .map(String::as_str)
        .skip_while(|word| is_assignment(word));
    match rest.next()? {
        // `env NAME=value... program` runs the program; any flag (`env -i`,
        // `env --help`) changes what it does, so the segment is unclaimable.
        "env" => {
            let program = rest.find(|word| !is_assignment(word))?;
            (!program.starts_with('-')).then_some(program)
        }
        // Wrappers whose effect on control flow or on what runs this does not
        // model: `exec` replaces the shell (`exec true && cargo build` never
        // runs cargo), `command -v` only asks, `sudo` changes the account.
        "exec" | "command" | "time" | "builtin" | "eval" | "source" | "." | "nohup" | "sudo" => {
            None
        }
        program => Some(program),
    }
}

fn program_name(word: &str) -> &str {
    let name = word.rsplit(['/', '\\']).next().unwrap_or(word);
    [".exe", ".cmd"]
        .iter()
        .find_map(|suffix| name.strip_suffix(suffix))
        .unwrap_or(name)
}

fn is_assignment(word: &str) -> bool {
    word.split_once('=').is_some_and(|(name, _)| {
        !name.is_empty()
            && name
                .chars()
                .all(|character| character == '_' || character.is_ascii_alphanumeric())
            && !name.starts_with(|character: char| character.is_ascii_digit())
    })
}

/// The repository scripts that declare what each platform needs in order to
/// build the workspace, keyed by `std::env::consts::OS`.
///
/// The executor runs the script for its own platform at attach and advertises
/// the verdict (`BuildPrerequisiteCheck`). That verdict speaks only for a
/// repository that declares the same script: a project whose checkout holds no
/// such file is described by nothing the probe measured.
pub const BUILD_PREREQUISITE_SCRIPTS: &[(&str, &str)] = &[
    ("linux", "scripts/setup-self-hosted-runner-linux.sh"),
    ("macos", "scripts/setup-self-hosted-runner-macos.sh"),
    ("windows", "scripts/setup-self-hosted-runner-windows.ps1"),
];

/// The prerequisite script declared for one platform.
pub fn build_prerequisite_script(os: &str) -> Option<&'static str> {
    BUILD_PREREQUISITE_SCRIPTS
        .iter()
        .find(|(platform, _)| *platform == os)
        .map(|(_, path)| *path)
}

/// A project's placement requirements: the toolchains its setup commands
/// invoke, and the prerequisite scripts its checkout at `project_root`
/// declares.
pub fn project_placement_requirements(
    setup_commands: &[String],
    project_root: &std::path::Path,
) -> crate::executor_protocol::ProjectPlacementRequirements {
    crate::executor_protocol::ProjectPlacementRequirements {
        setup_toolchains: setup_toolchain_requirements(setup_commands),
        setup_identity: setup_identity(setup_commands),
        prerequisite_scripts: BUILD_PREREQUISITE_SCRIPTS
            .iter()
            .filter(|(_, path)| project_root.join(path).is_file())
            .map(|(_, path)| path.to_string())
            .collect(),
    }
}

/// The repository path a `BuildPrerequisiteCheck::script` names. The verdict
/// records the invocation (`<path> --check`), so the path is its first word.
pub fn prerequisite_script_path(invocation: &str) -> &str {
    invocation.split_whitespace().next().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn required(commands: &[&str]) -> Vec<String> {
        setup_toolchain_requirements(
            &commands
                .iter()
                .map(|command| command.to_string())
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn a_setup_command_requires_the_toolchain_of_the_program_it_runs() {
        assert_eq!(required(&["bun install --frozen-lockfile"]), ["bun"]);
        assert_eq!(required(&["cd api && bun install"]), ["bun"]);
        assert_eq!(required(&["FOO=1 cargo build"]), ["rust"]);
        // A program named by path is not looked up on PATH, so it says nothing
        // about the toolchains the machine advertises.
        assert!(required(&["/usr/local/bin/bun.exe install"]).is_empty());
        assert_eq!(
            required(&["npx prettier --check . || cargo fetch", "bun install"]),
            // The `||` line is not claimed at all; the plain one is.
            ["bun"]
        );
    }

    /// Only a plain `&&` chain is claimed, so nothing the shell might not run
    /// — quoted data, a comment, a here-document, a fallback, a recoverable
    /// failure, a wrapper that changes control flow, a program the checkout
    /// puts on PATH — becomes a hard requirement.
    #[test]
    fn only_a_plain_and_chain_contributes_requirements() {
        for unclaimable in [
            "printf '%s\\n' 'setup instructions; cargo build --workspace'",
            "echo \"a && $(cargo build)\"",
            "# comment; cargo build\ntrue",
            "cat <<'EOF'\ncargo build\nEOF",
            "true || cargo build --workspace",
            "bun install || true",
            "cargo fetch; true",
            "bun install | tee log",
            "(cd api && bun install)",
            "bun install &",
            "command -v bun && bun install",
            "exec true && cargo build",
            "env --help bun",
            "env -i FOO=1 bun install",
            "exec bun install",
            "sudo bun install",
            "PATH=./tools bun install",
            // A prefix that changes the shell's control flow: the line succeeds
            // and bun never runs.
            "exit 0 && bun install",
            "set -n && bun install",
            // Opaque prefixes prove nothing about what runs after them.
            "make && bun install",
            "./scripts/prepare.sh && bun install",
            "export CI=1 && bun install",
            "cd -P api && bun install",
        ] {
            assert!(required(&[unclaimable]).is_empty(), "{unclaimable:?}");
        }
        assert!(required(&["export PATH=./tools:$PATH", "bun install"]).is_empty());
        assert_eq!(required(&["\"bun\" install"]), ["bun"]);
        assert_eq!(required(&["env FOO=1 bun install"]), ["bun"]);
        assert_eq!(required(&["bun install 2>&1 > log"]), ["bun"]);
        assert_eq!(
            required(&["cd api && bun install && cargo fetch"]),
            ["bun", "rust"]
        );
    }

    /// Exit 127 is the host's fault only when the command that produced it is
    /// provably a bare-named host toolchain the host does not have — judged
    /// against the builtins the executor's actual shell guarantees, not bash's.
    #[test]
    fn only_a_provably_executed_missing_host_toolchain_is_the_hosts_fault() {
        let setup = |commands: &[&str]| commands.iter().map(|c| c.to_string()).collect::<Vec<_>>();
        let nothing_installed = |_: &str| false;
        let blame = |line: &str, installed: &dyn Fn(&str) -> bool| {
            missing_host_program(&setup(&[line]), line, SetupShell::PosixSh, installed)
        };
        assert_eq!(blame("bun install", &nothing_installed), Some("bun".into()));
        assert_eq!(
            blame("cd api && bun install", &nothing_installed),
            Some("bun".into())
        );
        assert_eq!(
            blame("bun install && cargo fetch", &nothing_installed),
            Some("bun".into())
        );
        // Only `cd` may precede the blamed command.
        assert_eq!(
            blame("export CI=1 && bun install", &nothing_installed),
            None
        );
        assert_eq!(blame("exit 0 && bun install", &nothing_installed), None);
        assert_eq!(blame("set -n && bun install", &nothing_installed), None);
        // dash has no `pushd`, so under /bin/sh it is what exits 127, and
        // bun is never shown to have run. Likewise for builtins a POSIX sh
        // need not provide.
        assert_eq!(blame("pushd . && bun install", &nothing_installed), None);
        assert_eq!(blame("echo setup && bun install", &nothing_installed), None);
        // bun never runs here; the 127 is the branch script's.
        assert_eq!(
            blame(
                "true || bun install; ./missing-branch-script",
                &nothing_installed
            ),
            None
        );
        assert_eq!(
            blame("./missing-branch-script && bun install", &nothing_installed),
            None
        );
        assert_eq!(blame("./scripts/setup.sh", &nothing_installed), None);
        // A program no host toolchain owns may be the checkout's own.
        assert_eq!(blame("make setup", &nothing_installed), None);
        // Present on the host: the 127 came from somewhere inside it.
        assert_eq!(
            blame("bun run setup", &|program: &str| program == "bun"),
            None
        );
        // Setup composes its own PATH, so resolution is the checkout's.
        assert_eq!(
            missing_host_program(
                &setup(&["export PATH=$PWD/node_modules/.bin:$PATH", "bun install"]),
                "bun install",
                SetupShell::PosixSh,
                nothing_installed
            ),
            None
        );
        assert_eq!(blame("PATH=./tools bun install", &nothing_installed), None);
        // cmd.exe guarantees none of the POSIX builtins.
        let line = "cd api && bun install";
        assert_eq!(
            missing_host_program(&setup(&[line]), line, SetupShell::Cmd, nothing_installed),
            None
        );
        assert_eq!(
            missing_host_program(
                &setup(&["bun install"]),
                "bun install",
                SetupShell::Cmd,
                nothing_installed
            ),
            Some("bun".into())
        );
    }

    #[test]
    fn the_setup_identity_moves_with_the_setup() {
        let a = setup_identity(&["bun install".into()]);
        assert_eq!(a, setup_identity(&["bun install".into()]));
        assert_ne!(a, setup_identity(&["bun install --frozen-lockfile".into()]));
    }

    #[test]
    fn a_program_the_fleet_has_no_toolchain_for_requires_nothing() {
        assert!(required(&["make"]).is_empty());
        assert!(required(&["cd api; ls -la"]).is_empty());
        assert!(required(&[]).is_empty());
    }

    #[test]
    fn every_probe_program_names_its_own_toolchain() {
        for toolchain in TOOLCHAINS {
            assert!(
                toolchain.programs.contains(&toolchain.probe),
                "{} probes {}, which does not require it",
                toolchain.name,
                toolchain.probe
            );
            assert_eq!(toolchain_for_program(toolchain.probe), Some(toolchain.name));
        }
    }

    #[test]
    fn a_verdicts_invocation_names_the_script_path() {
        assert_eq!(
            prerequisite_script_path("scripts/setup-self-hosted-runner-linux.sh --check"),
            "scripts/setup-self-hosted-runner-linux.sh"
        );
        assert_eq!(prerequisite_script_path(""), "");
        assert_eq!(
            build_prerequisite_script("linux"),
            Some("scripts/setup-self-hosted-runner-linux.sh")
        );
        assert_eq!(build_prerequisite_script("plan9"), None);
    }
}
