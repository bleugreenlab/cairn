//! What a git credential failure looks like, in git's own words.
//!
//! A git dependency fetched over HTTPS or SSH fails the same way wherever it is
//! fetched from: git, asked for a credential it cannot get, prints one of a
//! handful of `fatal:` lines and exits 128. Cargo then wraps that exit in a
//! chain (`failed to load source for dependency`, `unable to update <url>`,
//! `revision … not found`) that reads as a bad pin, though the pin was never
//! checked. The wrappers are not evidence of anything on their own: a
//! genuinely missing revision prints them too. Only git's auth line is.
//!
//! Two readers share this one matcher so they cannot disagree. The runner's
//! check classifier reads a failed check's output with it to store a
//! credential-less fetch as infrastructure rather than as a verdict on the
//! tree. The executor's GitHub credential probe reads `git ls-remote`'s stderr
//! with it to tell "the credential was refused" from "the network was down".

/// A git credential failure found in some output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialFailure {
    /// Index, among the output's lines, of the first line that names the
    /// failure.
    pub line_index: usize,
    /// That line, trimmed.
    pub line: String,
    /// The host that refused or could not be asked, lowercased, when the
    /// output names one.
    pub host: Option<String>,
    /// The repository being fetched, normalised to `scheme://host/owner/repo`
    /// (no query, fragment, trailing slash or `.git`), when the output names
    /// one.
    pub repository: Option<String>,
}

impl CredentialFailure {
    /// Whether the refusing host is github.com.
    pub fn is_github(&self) -> bool {
        self.host.as_deref() == Some("github.com")
    }
}

/// The first git credential failure in `text`, if any.
pub fn credential_failure(text: &str) -> Option<CredentialFailure> {
    credential_failure_in(text.lines())
}

/// [`credential_failure`] over lines a caller has already split, so a caller
/// that blanks some lines (a test runner's report rows) keeps its indices.
pub fn credential_failure_in<'a>(
    lines: impl IntoIterator<Item = &'a str>,
) -> Option<CredentialFailure> {
    let lines: Vec<&str> = lines.into_iter().collect();
    // `Permission denied (publickey)` is ssh's, and ssh prints it for refusals
    // that have nothing to do with git. Git's own line beside it is what makes
    // it a fetch that failed for want of a key.
    let remote_unreadable = lines.iter().any(|line| {
        line.trim()
            .to_ascii_lowercase()
            .starts_with("fatal: could not read from remote repository")
    });
    let (line_index, host) = lines.iter().enumerate().find_map(|(index, line)| {
        auth_line_host(line, remote_unreadable).map(|host| (index, host))
    })?;
    let line = lines[line_index].trim().to_string();
    let repository = lines.iter().find_map(|line| repository_named_by(line));
    let host = host.or_else(|| repository.as_deref().and_then(url_host));
    Some(CredentialFailure {
        line_index,
        line,
        host,
        repository,
    })
}

/// Whether `line` is one of git's credential failures; if so, the host it
/// names (`Some(None)` when it names none).
///
/// Each form must open the (trimmed) line, as git, ssh and cargo print it.
/// Text that merely quotes one is not evidence: rustc and clippy echo source
/// lines in their diagnostics (`6287 |  fatal: could not read Username …`),
/// and a compile error that quotes a fixture must stay the tree's red.
fn auth_line_host(line: &str, remote_unreadable: bool) -> Option<Option<String>> {
    let line = line.trim();
    let lower = line.to_ascii_lowercase();
    for prefix in [
        "fatal: could not read username for '",
        "fatal: could not read password for '",
        "fatal: authentication failed for '",
    ] {
        if lower.starts_with(prefix) {
            let url = line[prefix.len()..].split('\'').next().unwrap_or_default();
            return Some(url_host(url));
        }
    }
    if lower.starts_with("fatal: ") && lower.contains("terminal prompts disabled") {
        return Some(quoted_url(line).and_then(url_host));
    }
    if remote_unreadable {
        if let Some((who, said)) = lower.split_once(": ") {
            if who.contains('@')
                && !who.contains(char::is_whitespace)
                && said.starts_with("permission denied (publickey")
            {
                return Some(scp_host(line));
            }
        }
    }
    const LIBGIT2: &str = "failed to authenticate when downloading repository";
    if lower.starts_with(LIBGIT2) {
        let url = line[LIBGIT2.len()..]
            .trim_start_matches(':')
            .trim()
            .split_whitespace()
            .next();
        return Some(url.and_then(url_host));
    }
    None
}

/// The repository a cargo or git line names: cargo's `unable to update <url>`,
/// git's quoted remote in a `git fetch …` or `git ls-remote …` command line,
/// cargo's libgit2 `failed to authenticate when downloading repository: <url>`,
/// or git's own `Authentication failed for '<url>'` when it carries a path.
fn repository_named_by(line: &str) -> Option<String> {
    let trimmed = line.trim();
    let lower = trimmed.to_ascii_lowercase();
    if let Some(rest) = trimmed.strip_prefix("unable to update ") {
        return rest
            .split_whitespace()
            .next()
            .and_then(normalise_repository);
    }
    if lower.contains("git fetch ") || lower.contains("git ls-remote ") {
        return trimmed
            .split('\'')
            .skip(1)
            .step_by(2)
            .find_map(normalise_repository);
    }
    if let Some(at) = lower.find("failed to authenticate when downloading repository: ") {
        let rest = &trimmed[at + "failed to authenticate when downloading repository: ".len()..];
        return rest
            .split_whitespace()
            .next()
            .and_then(normalise_repository);
    }
    if let Some(at) = lower.find("authentication failed for '") {
        let quoted = &trimmed[at + "authentication failed for '".len()..];
        return quoted.split('\'').next().and_then(normalise_repository);
    }
    None
}

/// `url` as a repository: a remote with an owner and a name, without the query,
/// fragment, trailing slash or `.git` cargo and git decorate it with.
fn normalise_repository(url: &str) -> Option<String> {
    let url = url.split(['?', '#']).next()?.trim_end_matches('/');
    let url = url.strip_suffix(".git").unwrap_or(url);
    let path = if let Some((_, rest)) = url.split_once("://") {
        rest.split_once('/')?.1
    } else if url.contains('@') {
        url.split_once(':')?.1
    } else {
        return None;
    };
    if path.trim_matches('/').is_empty() {
        return None;
    }
    Some(url.to_string())
}

/// The first `'…'`-quoted span of `line` that looks like a remote URL.
fn quoted_url(line: &str) -> Option<&str> {
    line.split('\'')
        .skip(1)
        .step_by(2)
        .find(|span| span.contains("://") || span.contains('@'))
}

/// The host of `https://user@host/path`, `ssh://git@host:22/path`, or
/// `git@host:owner/repo`, lowercased.
pub fn url_host(url: &str) -> Option<String> {
    let url = url.trim();
    let authority = match url.split_once("://") {
        Some((_, rest)) => rest.split('/').next()?,
        None => url.split_once(':')?.0,
    };
    let host = authority.rsplit('@').next()?;
    let host = host.split(':').next()?;
    (!host.is_empty()).then(|| host.to_ascii_lowercase())
}

/// The host of ssh's `git@github.com: Permission denied (publickey).`
fn scp_host(line: &str) -> Option<String> {
    let (prefix, _) = line.split_once(": ")?;
    let host = prefix.rsplit('@').next()?;
    (!host.is_empty() && !host.contains(' ')).then(|| host.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Observation 96e4e69d's tail (tm/230, bginf-sp1): cargo fetching the
    /// private `tm-cards` dependency on a machine with no credential helper.
    const TM_230_TAIL: &str = "stderr:\n\
        fatal: could not read Username for 'https://github.com': No such device or address\n\
        fatal: could not read Username for 'https://github.com': No such device or address\n\
        error: failed to get `tm-cards` as a dependency of package `tm-board v0.1.0`\n\
        \n\
        Caused by:\n\
        \x20 unable to update https://github.com/bleugreen/tm-cards?rev=3ef623eea69d3605f15c4b75a46f83a2922138ed#3ef623ee\n\
        \n\
        Caused by:\n\
        \x20 revision 3ef623eea69d3605f15c4b75a46f83a2922138ed not found\n\
        \n\
        Caused by:\n\
        \x20 process didn't exit successfully: `git fetch --no-tags --quiet --force --update-head-ok 'https://github.com/bleugreen/tm-cards' '+3ef623ee:refs/commit/3ef623ee'` (exit status: 128)";

    #[test]
    fn each_git_credential_failure_names_its_line_host_and_repository() {
        let tm_cards = Some("https://github.com/bleugreen/tm-cards".to_string());
        let github = Some("github.com".to_string());
        for (text, line_index, host, repository) in [
            (TM_230_TAIL, 1, github.clone(), tm_cards.clone()),
            (
                "fatal: could not read Password for 'https://u@github.com': terminal prompts disabled",
                0,
                github.clone(),
                None,
            ),
            (
                "remote: Invalid username or token.\nfatal: Authentication failed for 'https://github.com/bleugreen/tm-cards.git/'",
                1,
                github.clone(),
                tm_cards.clone(),
            ),
            (
                "fatal: could not read Username for 'https://gitlab.example.com': terminal prompts disabled",
                0,
                Some("gitlab.example.com".to_string()),
                None,
            ),
            (
                "git@github.com: Permission denied (publickey).\nfatal: Could not read from remote repository.\n\nPlease make sure you have the correct access rights",
                0,
                github.clone(),
                None,
            ),
            (
                "Caused by:\n  failed to authenticate when downloading repository: ssh://git@github.com/bleugreen/tm-cards.git\n\n  * attempted ssh-agent authentication, but no usernames succeeded: `git`",
                1,
                github.clone(),
                Some("ssh://git@github.com/bleugreen/tm-cards".to_string()),
            ),
            (
                "Caused by:\n  unable to update https://github.com/bleugreen/tm-cards?rev=3ef6#3ef6\n\nCaused by:\n  failed to authenticate when downloading repository\n",
                4,
                github.clone(),
                tm_cards.clone(),
            ),
        ] {
            let failure = credential_failure(text).unwrap_or_else(|| panic!("{text}"));
            assert_eq!(failure.line_index, line_index, "{text}");
            assert_eq!(failure.line, text.lines().nth(line_index).unwrap().trim());
            assert_eq!(failure.host, host, "{text}");
            assert_eq!(failure.repository, repository, "{text}");
        }
    }

    #[test]
    fn cargos_wrappers_and_lookalikes_are_not_a_credential_failure() {
        for text in [
            // A genuinely bad pin: cargo's whole chain, and no auth line.
            "error: failed to get `tm-cards` as a dependency\n\nCaused by:\n  failed to load source for dependency `tm-cards`\n\nCaused by:\n  revision 3ef623ee not found",
            // ssh refusing something that is not a git remote.
            "dev@bginf-sp1: Permission denied (publickey).",
            // A compile error.
            "error[E0425]: cannot find value `turn_id` in this scope",
            // The words without git's `fatal:` framing.
            "could not read Username for 'https://github.com'",
            // A rustc diagnostic quoting a fixture line of this very matcher.
            "error[E0308]: mismatched types\n    --> os/cairn-core/src/execution/checks.rs:6287:9\n     |\n6287 |         fatal: could not read Username for 'https://github.com': No such device or address\n     |         ^^^^^ expected `&str`, found `()`\nerror: could not compile `cairn-core` (lib test) due to 1 previous error",
            // A clippy diagnostic quoting the other forms.
            "warning: this could be simplified\n   --> src/git_credential.rs:12:5\n   |\n12 |     \"git@github.com: Permission denied (publickey).\\nfatal: Could not read from remote repository.\",\n13 |     \"failed to authenticate when downloading repository\",\n14 |     \"fatal: could not read Password for 'https://github.com': terminal prompts disabled\",",
            "",
        ] {
            assert_eq!(credential_failure(text), None, "{text}");
        }
    }

    #[test]
    fn hosts_are_read_from_every_remote_form() {
        for (url, host) in [
            ("https://github.com", "github.com"),
            ("https://user@GitHub.com/owner/repo", "github.com"),
            ("ssh://git@github.com:22/owner/repo", "github.com"),
            ("git@github.com:owner/repo.git", "github.com"),
        ] {
            assert_eq!(url_host(url).as_deref(), Some(host), "{url}");
        }
        assert!(credential_failure(TM_230_TAIL).unwrap().is_github());
    }
}
