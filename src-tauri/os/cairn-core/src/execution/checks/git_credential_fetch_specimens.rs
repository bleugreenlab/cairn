/// Observation 96e4e69d-a1b9-44d3-86b6-cc6e08412e57's `output_tail`
/// verbatim (tm/230, rust-test on bginf-sp1 at `b7ec03c0`): cargo fetching
/// the private `tm-cards` dependency on a machine with no credential
/// helper, which cargo words as a missing revision.
pub(crate) const TM_230_TAIL: &str = "stderr:\n\
        fatal: could not read Username for 'https://github.com': No such device or address\n\
        fatal: could not read Username for 'https://github.com': No such device or address\n\
        fatal: could not read Username for 'https://github.com': No such device or address\n\
        fatal: could not read Username for 'https://github.com': No such device or address\n\
        error: failed to get `tm-cards` as a dependency of package `tm-board v0.1.0 (/home/dev/.cairn-exec/build-slots/tm/slot-3/crates/tm-board)`\n\
        \n\
        Caused by:\n\
        \x20 failed to load source for dependency `tm-cards`\n\
        \n\
        Caused by:\n\
        \x20 unable to update https://github.com/bleugreen/tm-cards?rev=3ef623eea69d3605f15c4b75a46f83a2922138ed#3ef623ee\n\
        \n\
        Caused by:\n\
        \x20 failed to fetch into: /home/dev/.cargo/git/db/tm-cards-979544f55ddc4498\n\
        \n\
        Caused by:\n\
        \x20 revision 3ef623eea69d3605f15c4b75a46f83a2922138ed not found\n\
        \n\
        Caused by:\n\
        \x20 process didn't exit successfully: `git fetch --no-tags --quiet --force --update-head-ok 'https://github.com/bleugreen/tm-cards' '+3ef623eea69d3605f15c4b75a46f83a2922138ed:refs/commit/3ef623eea69d3605f15c4b75a46f83a2922138ed'` (exit status: 128)\n\
        \n\
        \n\
        Exit code: 101";

/// Cargo's chain around a git failure, with `{auth}` where git's own lines
/// go. Without an auth line it is a genuinely missing revision.
const CARGO_CHAIN: &str = "{auth}\n\
        error: failed to get `tm-cards` as a dependency of package `tm-board v0.1.0`\n\
        \n\
        Caused by:\n\
        \x20 failed to load source for dependency `tm-cards`\n\
        \n\
        Caused by:\n\
        \x20 unable to update https://github.com/bleugreen/tm-cards?rev=3ef623ee#3ef623ee\n\
        \n\
        Caused by:\n\
        \x20 revision 3ef623ee not found\n\
        \n\
        Caused by:\n\
        \x20 process didn't exit successfully: `git fetch --no-tags --quiet --force --update-head-ok 'https://github.com/bleugreen/tm-cards' '+3ef623ee:refs/commit/3ef623ee'` (exit status: 128)";

pub(crate) fn wrapped(auth: &str) -> String {
    CARGO_CHAIN.replace("{auth}", auth)
}

/// git's auth lines, each to be wrapped in [`wrapped`].
pub(crate) const AUTH_LINES: &[&str] = &[
    "remote: Invalid username or token.\nfatal: Authentication failed for 'https://github.com/bleugreen/tm-cards/'",
    "fatal: could not read Username for 'https://github.com': terminal prompts disabled",
    "git@github.com: Permission denied (publickey).\nfatal: Could not read from remote repository.\n\nPlease make sure you have the correct access rights\nand the repository exists.",
];

/// Cargo's libgit2 fetcher, which reports the refusal itself.
pub(crate) const LIBGIT2_TAIL: &str = "error: failed to get `tm-cards` as a dependency of package `tm-board v0.1.0`\n\
        \n\
        Caused by:\n\
        \x20 failed to load source for dependency `tm-cards`\n\
        \n\
        Caused by:\n\
        \x20 unable to update https://github.com/bleugreen/tm-cards?rev=3ef623ee#3ef623ee\n\
        \n\
        Caused by:\n\
        \x20 failed to authenticate when downloading repository\n\
        \n\
        \x20 * attempted to find username/password via git's `credential.helper` support, but failed";

/// The same failure against a host that is not GitHub.
pub(crate) const GITLAB_TAIL: &str =
    "fatal: could not read Username for 'https://gitlab.example.com': No such device or address\n\
        error: failed to get `widgets` as a dependency of package `tm-board v0.1.0`\n\
        \n\
        Caused by:\n\
        \x20 unable to update https://gitlab.example.com/acme/widgets?rev=9a1b#9a1b";
