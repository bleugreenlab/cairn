//! The one way library code runs `git` against a remote.
//!
//! Every invocation pins the transport policy on the command line
//! (`protocol.allow=never` with only `https` re-allowed), ignores system and
//! global configuration, disables hooks and prompts, separates the URL from
//! options with `--`, and is killed after [`FETCH_TIMEOUT`]. URLs are checked
//! before git sees them: https only, nothing starting with `-`, and no
//! `<transport>::<address>` helper syntax (`ext::sh -c ...`).

use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::is_full_commit;

pub const FETCH_TIMEOUT: Duration = Duration::from_secs(120);

/// Which transports a fetch may use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GitPolicy {
    allow_file: bool,
}

impl GitPolicy {
    /// https only. The only policy production code can construct.
    pub const fn production() -> Self {
        GitPolicy { allow_file: false }
    }

    /// Additionally allows `file://` fixture repositories.
    #[cfg(any(test, feature = "test-support"))]
    pub const fn allowing_file_urls_for_tests() -> Self {
        GitPolicy { allow_file: true }
    }
}

/// Refuse a URL git must never be handed.
pub fn check_url(url: &str, policy: GitPolicy) -> Result<(), String> {
    if url.starts_with('-') {
        return Err(format!(
            "refusing git URL `{url}`: it begins with `-`, which git would read as an option"
        ));
    }
    if url.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(format!("refusing git URL `{url}`: it contains whitespace"));
    }
    if url.contains("::") {
        return Err(format!(
            "refusing git URL `{url}`: transport-helper syntax (`<transport>::<address>`) is \
             not allowed"
        ));
    }
    let parsed = url::Url::parse(url).map_err(|e| format!("refusing git URL `{url}`: {e}"))?;
    match parsed.scheme() {
        "https" if parsed.host_str().is_some_and(|host| !host.is_empty()) => Ok(()),
        "file" if policy.allow_file => Ok(()),
        _ => Err(format!(
            "refusing git URL `{url}`: only https:// sources are fetched"
        )),
    }
}

fn check_rev(rev: &str) -> Result<(), String> {
    let valid = !rev.is_empty()
        && !rev.starts_with('-')
        && !rev.contains("..")
        && rev
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'));
    if valid {
        Ok(())
    } else {
        Err(format!("refusing git revision `{rev}`"))
    }
}

/// Fetch `rev` of `url` into the new directory `dest` (depth 1, no tags, no
/// submodules) and check it out. Returns the resolved commit, which must equal
/// `rev` when `rev` is itself a commit.
pub fn fetch_tree(url: &str, rev: &str, dest: &Path, policy: GitPolicy) -> Result<String, String> {
    check_url(url, policy)?;
    check_rev(rev)?;
    if dest.exists() {
        return Err(format!(
            "refusing to fetch into existing directory {dest:?}"
        ));
    }
    std::fs::create_dir_all(dest).map_err(|e| format!("Failed to create {dest:?}: {e}"))?;
    let dest_arg = dest.to_string_lossy().to_string();
    // The checkout below is detached at FETCH_HEAD; the branch is named only so
    // no host default is ever consulted.
    run_git(
        policy,
        None,
        &["init", "-q", "-b", "main", "--template=", "--", &dest_arg],
    )?;
    run_git(
        policy,
        Some(dest),
        &[
            "fetch",
            "--depth",
            "1",
            "--no-tags",
            "--no-recurse-submodules",
            "--",
            url,
            rev,
        ],
    )
    .map_err(|e| format!("Failed to fetch {rev} from {url}: {e}"))?;
    run_git(
        policy,
        Some(dest),
        &["checkout", "-q", "--detach", "FETCH_HEAD"],
    )?;
    let commit = run_git(policy, Some(dest), &["rev-parse", "FETCH_HEAD"])?
        .trim()
        .to_string();
    if !is_full_commit(&commit) {
        return Err(format!("git resolved {rev} of {url} to `{commit}`"));
    }
    if is_full_commit(rev) && commit != rev {
        return Err(format!(
            "fetching {url} at {rev} produced commit {commit} instead; refusing"
        ));
    }
    Ok(commit)
}

/// The commit `refs/heads/<branch>` of `url` points at, or `None` when the
/// branch does not exist.
pub fn ls_remote(url: &str, branch: &str, policy: GitPolicy) -> Result<Option<String>, String> {
    check_url(url, policy)?;
    check_rev(branch)?;
    let refname = format!("refs/heads/{branch}");
    let output = run_git(policy, None, &["ls-remote", "--", url, &refname])?;
    Ok(output.lines().find_map(|line| {
        let (commit, name) = line.split_once('\t')?;
        (name.trim() == refname && is_full_commit(commit)).then(|| commit.to_string())
    }))
}

fn null_device() -> &'static str {
    if cfg!(windows) {
        "NUL"
    } else {
        "/dev/null"
    }
}

fn run_git(policy: GitPolicy, cwd: Option<&Path>, args: &[&str]) -> Result<String, String> {
    let mut command = Command::new("git");
    if let Some(cwd) = cwd {
        command.arg("-C").arg(cwd);
    }
    command
        .args([
            "-c",
            "protocol.allow=never",
            "-c",
            "protocol.https.allow=always",
        ])
        .args(["-c", &format!("core.hooksPath={}", null_device())])
        .args(["-c", "core.fsmonitor=false", "-c", "credential.helper="]);
    if policy.allow_file {
        command.args(["-c", "protocol.file.allow=always"]);
    }
    command
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", null_device())
        .env(
            "GIT_ALLOW_PROTOCOL",
            if policy.allow_file {
                "https:file"
            } else {
                "https"
            },
        )
        .env_remove("GIT_ASKPASS")
        .env_remove("SSH_ASKPASS")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|e| format!("Failed to run git: {e}"))?;
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let out_reader = std::thread::spawn(move || {
        let mut buffer = String::new();
        let _ = stdout.read_to_string(&mut buffer);
        buffer
    });
    let err_reader = std::thread::spawn(move || {
        let mut buffer = String::new();
        let _ = stderr.read_to_string(&mut buffer);
        buffer
    });
    let started = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if started.elapsed() >= FETCH_TIMEOUT => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "git {} timed out after {}s",
                    args.first().unwrap_or(&""),
                    FETCH_TIMEOUT.as_secs()
                ));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("Failed to wait for git: {e}")),
        }
    };
    let stdout = out_reader.join().unwrap_or_default();
    let stderr = err_reader.join().unwrap_or_default();
    if status.success() {
        Ok(stdout)
    } else {
        Err(stderr.trim().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::FixtureRepo;
    use super::*;

    #[test]
    fn production_refuses_everything_but_https() {
        let policy = GitPolicy::production();
        assert!(check_url("https://github.com/bleugreenlab/cairn-library", policy).is_ok());
        for (url, needle) in [
            ("--upload-pack=touch /tmp/x", "begins with `-`"),
            ("-https://github.com/x/y", "begins with `-`"),
            ("ext::sh -c touch% /tmp/pwned", "whitespace"),
            ("ext::sh%20-c%20id", "transport-helper syntax"),
            ("file:///tmp/repo", "only https://"),
            ("http://github.com/x/y", "only https://"),
            ("ssh://git@github.com/x/y", "only https://"),
            ("git@github.com:x/y.git", "refusing git URL"),
        ] {
            let error = check_url(url, policy).unwrap_err();
            assert!(error.contains(needle), "{url}: {error}");
        }
    }

    #[test]
    fn the_test_policy_allows_file_urls_and_nothing_else_new() {
        let policy = GitPolicy::allowing_file_urls_for_tests();
        assert!(check_url("file:///tmp/repo", policy).is_ok());
        assert!(check_url("ext::sh%20-c%20id", policy).is_err());
        assert!(check_url("ssh://git@github.com/x/y", policy).is_err());
    }

    #[test]
    fn the_production_policy_refuses_a_file_url_before_running_git() {
        let temp = tempfile::tempdir().unwrap();
        let repo = FixtureRepo::new(temp.path().join("origin"));
        repo.write("README.md", "hi\n");
        let commit = repo.commit("init");
        let dest = temp.path().join("dest");
        let error = fetch_tree(&repo.url(), &commit, &dest, GitPolicy::production()).unwrap_err();
        assert!(error.contains("only https://"), "{error}");
        assert!(!dest.exists(), "nothing is created for a refused URL");
    }

    #[test]
    fn a_fetch_pins_the_commit_and_checks_out_the_tree() {
        let temp = tempfile::tempdir().unwrap();
        let repo = FixtureRepo::new(temp.path().join("origin"));
        repo.write("plugins/a/SKILL.md", "one\n");
        let first = repo.commit("one");
        repo.write("plugins/a/SKILL.md", "two\n");
        let second = repo.commit("two");
        let policy = GitPolicy::allowing_file_urls_for_tests();

        let dest = temp.path().join("at-first");
        assert_eq!(
            fetch_tree(&repo.url(), &first, &dest, policy).unwrap(),
            first
        );
        assert_eq!(
            std::fs::read_to_string(dest.join("plugins/a/SKILL.md")).unwrap(),
            "one\n"
        );

        let dest = temp.path().join("at-main");
        assert_eq!(
            fetch_tree(&repo.url(), "main", &dest, policy).unwrap(),
            second
        );
        assert_eq!(
            ls_remote(&repo.url(), "main", policy).unwrap().as_deref(),
            Some(second.as_str())
        );
        assert_eq!(ls_remote(&repo.url(), "absent", policy).unwrap(), None);
    }
}
