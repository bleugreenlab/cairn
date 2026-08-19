//! The cursor: what a typed line means at the current location.
//!
//! This is the whole shell minus its terminal. A line becomes an [`Intent`] with
//! no input, no output, and no network, so the behaviour the operator's sketch
//! describes is testable as data. The loop in the parent module supplies the
//! terminal and the client and does as it is told.
//!
//! A failed read must not move the cursor, so navigation is two steps:
//! [`Shell::dispatch`] proposes a target, and [`Shell::arrive`] commits it only
//! once the read succeeded.

use cairn_common::read::AffordanceSpec;
use cairn_common::uri::{build_issue_uri, parse_uri, resolve_relative, ROOT_URI};
use serde_json::Value;

use super::actions;

/// What the loop should do with one understood line.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Intent {
    /// Read `target`; on success move the cursor there. `remember` is false when
    /// the move is itself a step backwards, so retreating does not re-record the
    /// place being retreated from.
    Navigate {
        target: String,
        remember: bool,
    },
    /// Read `target` and print it, leaving the cursor where it is. Files, web
    /// pages, and `:read` are lookups, not places.
    Peek {
        target: String,
    },
    /// Send one resource mutation. `confirm` carries the question to ask first,
    /// for the modes a keystroke cannot take back.
    Mutate {
        target: String,
        mode: String,
        payload: Value,
        confirm: Option<String>,
    },
    /// Print text at the prompt: help, `:pwd`, and every refusal.
    Print(String),
    /// Stream attention for the issue containing the cursor until interrupted.
    Watch {
        uri: String,
    },
    Quit,
    /// A blank line with no continuation to follow.
    Noop,
}

pub(crate) struct Shell {
    cwd: String,
    /// Locations left behind, most recent last. `-` walks it.
    back: Vec<String>,
    /// The affordances of the last resource rendered — that is, the command set
    /// in force at the prompt.
    spec: Option<AffordanceSpec>,
    /// The last text rendered, which is where completion candidates and the
    /// bare-enter continuation come from.
    last_text: String,
}

impl Shell {
    pub(crate) fn new() -> Self {
        Self {
            cwd: ROOT_URI.to_string(),
            back: Vec::new(),
            spec: None,
            last_text: String::new(),
        }
    }

    pub(crate) fn cwd(&self) -> &str {
        &self.cwd
    }

    pub(crate) fn prompt(&self) -> String {
        format!("{} > ", self.cwd)
    }

    pub(crate) fn spec(&self) -> Option<&AffordanceSpec> {
        self.spec.as_ref()
    }

    pub(crate) fn last_text(&self) -> &str {
        &self.last_text
    }

    /// The target a read at the root actually resolves to.
    ///
    /// No resource lives at `cairn://`; the workspace's projects are what is
    /// there to see. Keeping this a display-versus-read distinction is what lets
    /// the root be the empty segment path rather than a new resource.
    pub(crate) fn read_target(&self, target: &str) -> String {
        if target == ROOT_URI {
            "cairn://projects".to_string()
        } else {
            target.to_string()
        }
    }

    /// Record what a successful read rendered.
    pub(crate) fn record(&mut self, text: String, spec: Option<AffordanceSpec>) {
        self.last_text = text;
        self.spec = spec;
    }

    /// Record a read that did not move the cursor.
    ///
    /// The command set belongs to where the cursor stands, so looking at a file
    /// or paging further into a listing must not silently disarm `:comment`.
    pub(crate) fn record_peek(&mut self, text: String) {
        self.last_text = text;
    }

    /// Commit a navigation once its read succeeded. Any `?query` is scoping for
    /// that one read, not part of the location, so the cursor keeps the identity.
    pub(crate) fn arrive(&mut self, target: &str, remember: bool) {
        let identity = target.split('?').next().unwrap_or(target).to_string();
        if identity == self.cwd {
            return;
        }
        if remember {
            self.back.push(std::mem::replace(&mut self.cwd, identity));
        } else {
            self.cwd = identity;
        }
    }

    /// The continuation target the last render footered, if it offered one.
    ///
    /// Pressing enter on a paged read is the obvious gesture, and the footer
    /// already carries a complete, valid next target — so following it needs no
    /// paging state of the shell's own.
    fn continuation(&self) -> Option<String> {
        let (_, tail) = self.last_text.rsplit_once("continue: ")?;
        let end = tail.find(']')?;
        let target = tail[..end].trim();
        (!target.is_empty()).then(|| target.to_string())
    }

    pub(crate) fn dispatch(&mut self, line: &str) -> Intent {
        let line = line.trim();

        if line.is_empty() {
            return match self.continuation() {
                Some(target) => Intent::Peek { target },
                None => Intent::Noop,
            };
        }
        if line == "?" {
            return self.help();
        }
        if line == "-" {
            return match self.back.pop() {
                Some(previous) => Intent::Navigate {
                    target: previous,
                    remember: false,
                },
                None => Intent::Print("nowhere back to go".to_string()),
            };
        }
        if let Some(rest) = line.strip_prefix(':') {
            return self.colon(rest.trim());
        }
        // A file or a web page is something to look at from here, not a place to
        // stand: the cursor navigates the resource graph only.
        if line.starts_with("file:") || line.starts_with("http://") || line.starts_with("https://")
        {
            return Intent::Peek {
                target: line.to_string(),
            };
        }

        match resolve_relative(&self.cwd, line) {
            Ok(target) => Intent::Navigate {
                target,
                remember: true,
            },
            Err(message) => Intent::Print(message),
        }
    }

    fn colon(&self, rest: &str) -> Intent {
        let (verb, args) = match rest.split_once(char::is_whitespace) {
            Some((verb, args)) => (verb, args.trim()),
            None => (rest, ""),
        };
        match verb {
            "" => Intent::Print("expected a command after `:` — try `?`".to_string()),
            "help" => self.help(),
            "pwd" => Intent::Print(self.cwd.clone()),
            "quit" | "exit" => Intent::Quit,
            "read" => {
                if args.is_empty() {
                    Intent::Print("usage: :read <target>".to_string())
                } else {
                    Intent::Peek {
                        target: args.to_string(),
                    }
                }
            }
            "watch" => self.watch(),
            "ls" => Intent::Print(self.listing(args)),
            other => self.action(other, args),
        }
    }

    /// What is reachable from here, named the way it would be typed.
    ///
    /// `prefix` narrows exactly as a half-typed word does, and a leading `:`
    /// lists commands instead of places — the same call a completer makes.
    fn listing(&self, prefix: &str) -> String {
        let candidates =
            super::complete::candidates(&self.cwd, &self.last_text, self.spec.as_ref(), prefix);
        if candidates.is_empty() {
            format!("nothing here starts with `{prefix}`")
        } else {
            candidates.join("\n")
        }
    }

    /// Watching is issue-scoped, so a cursor anywhere inside an issue — a node, a
    /// task, an artifact — watches the issue it belongs to.
    fn watch(&self) -> Intent {
        let issue = parse_uri(&self.cwd).and_then(|resource| {
            let project = resource.project()?;
            let number = resource.issue_number()?;
            Some(build_issue_uri(project, number))
        });
        match issue {
            Some(uri) => Intent::Watch { uri },
            None => Intent::Print(format!(
                "nothing to watch at {} — :watch follows an issue, so stand inside one",
                self.cwd
            )),
        }
    }

    fn action(&self, verb: &str, args: &str) -> Intent {
        let Some(spec) = self.spec.as_ref() else {
            return Intent::Print(
                "no resource is rendered here, so there are no actions yet".to_string(),
            );
        };
        let action = match actions::resolve_verb(spec, verb) {
            Ok(action) => action,
            Err(message) => return Intent::Print(message),
        };
        // An action whose template names a resource this location cannot bind is
        // advertised as a pointer, not as something to do from here.
        let Some(target) = action.uri.clone() else {
            return Intent::Print(format!(
                "`{}` acts on {}, which this location does not name",
                actions::verb_slug(&action.label),
                action.uri_template
            ));
        };
        let payload = match actions::build_payload(action, args) {
            Ok(payload) => payload,
            Err(message) => return Intent::Print(message),
        };
        Intent::Mutate {
            target: target.clone(),
            mode: action.mode.clone(),
            payload,
            confirm: (action.mode == "delete").then(|| format!("delete {target}?")),
        }
    }

    /// The current resource's affordances, printed as the commands they are.
    fn help(&self) -> Intent {
        let mut out = String::new();
        match self.spec.as_ref() {
            None => out.push_str("nothing rendered here yet.\n"),
            Some(spec) => {
                out.push_str(&format!("{} — {}\n", self.cwd, spec.name));
                if !spec.actions.is_empty() {
                    out.push_str("\nactions\n");
                    for action in &spec.actions {
                        let keys = [
                            describe_keys("required", &action.required),
                            describe_keys("optional", &action.optional),
                        ]
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>()
                        .join("; ");
                        out.push_str(&format!(
                            "  :{:<24} {}{}\n",
                            actions::verb_slug(&action.label),
                            action.mode,
                            if keys.is_empty() {
                                String::new()
                            } else {
                                format!(" · {keys}")
                            }
                        ));
                    }
                }
                if !spec.links.is_empty() {
                    out.push_str("\nlinks\n");
                    for link in &spec.links {
                        out.push_str(&format!(
                            "  {:<24} {}\n",
                            link.label,
                            link.uri.as_deref().unwrap_or(&link.uri_template)
                        ));
                    }
                }
                if !spec.filters.is_empty() {
                    out.push_str("\nfilters (append as ?key=value)\n");
                    for filter in &spec.filters {
                        out.push_str(&format!("  {}={}\n", filter.key, filter.values));
                    }
                }
            }
        }
        out.push_str(
            "\nalways\n  <segment> navigate   .. up   . re-read   - back   <enter> next page\n  \
             :ls [prefix]  :pwd  :read <target>  :watch  :help  :quit\n",
        );
        Intent::Print(out)
    }
}

fn describe_keys(lead: &str, keys: &[cairn_common::read::KeyInfo]) -> Option<String> {
    if keys.is_empty() {
        return None;
    }
    Some(format!(
        "{lead} {}",
        keys.iter()
            .map(|key| format!("{}({})", key.key, key.ty))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::read::{ActionSpec, KeyInfo};

    fn issue_spec() -> AffordanceSpec {
        AffordanceSpec {
            kind: "issue".to_string(),
            name: "Issue".to_string(),
            links: Vec::new(),
            filters: Vec::new(),
            actions: vec![ActionSpec {
                label: "append comment".to_string(),
                mode: "append".to_string(),
                uri_template: "cairn://p/{project}/{number}".to_string(),
                uri: Some("cairn://p/cairn/4279".to_string()),
                required: vec![KeyInfo {
                    key: "content".to_string(),
                    ty: "str".to_string(),
                    note: String::new(),
                    aliases: Vec::new(),
                }],
                optional: Vec::new(),
                example: String::new(),
                guidance: None,
            }],
        }
    }

    fn navigate(shell: &mut Shell, line: &str) -> String {
        match shell.dispatch(line) {
            Intent::Navigate { target, remember } => {
                shell.arrive(&target, remember);
                target
            }
            other => panic!("expected navigation for {line:?}, got {other:?}"),
        }
    }

    #[test]
    fn the_operators_sketch_walks_and_climbs() {
        let mut shell = Shell::new();
        assert_eq!(shell.cwd(), "cairn://");
        assert_eq!(navigate(&mut shell, "p/cairn"), "cairn://p/cairn");
        assert_eq!(navigate(&mut shell, "4279"), "cairn://p/cairn/4279");
        assert_eq!(
            navigate(&mut shell, "1/builder"),
            "cairn://p/cairn/4279/1/builder"
        );
        assert_eq!(navigate(&mut shell, ".."), "cairn://p/cairn/4279");
    }

    #[test]
    fn back_undoes_each_move_in_turn_then_says_there_is_no_more() {
        let mut shell = Shell::new();
        navigate(&mut shell, "p/cairn");
        navigate(&mut shell, "4279");
        // Climbing is a move like any other, so the first `-` undoes it.
        navigate(&mut shell, "..");
        assert_eq!(navigate(&mut shell, "-"), "cairn://p/cairn/4279");
        assert_eq!(navigate(&mut shell, "-"), "cairn://p/cairn");
        assert_eq!(navigate(&mut shell, "-"), "cairn://");
        assert!(matches!(shell.dispatch("-"), Intent::Print(_)));
    }

    #[test]
    fn the_root_is_displayed_bare_and_read_as_the_project_list() {
        let shell = Shell::new();
        assert_eq!(shell.prompt(), "cairn:// > ");
        assert_eq!(shell.read_target("cairn://"), "cairn://projects");
        assert_eq!(shell.read_target("cairn://p/cairn"), "cairn://p/cairn");
    }

    #[test]
    fn a_query_scopes_the_read_without_becoming_the_location() {
        let mut shell = Shell::new();
        navigate(&mut shell, "p/cairn");
        assert_eq!(
            navigate(&mut shell, "4279?grep=Status"),
            "cairn://p/cairn/4279?grep=Status"
        );
        assert_eq!(shell.cwd(), "cairn://p/cairn/4279");
    }

    #[test]
    fn an_unresolvable_line_prints_and_leaves_the_cursor_alone() {
        let mut shell = Shell::new();
        navigate(&mut shell, "p/cairn");
        assert!(matches!(shell.dispatch("zz/builder"), Intent::Print(_)));
        assert_eq!(shell.cwd(), "cairn://p/cairn");
    }

    #[test]
    fn files_and_web_pages_are_looked_at_not_stood_on() {
        let mut shell = Shell::new();
        assert_eq!(
            shell.dispatch("file:src/lib.rs"),
            Intent::Peek {
                target: "file:src/lib.rs".to_string()
            }
        );
        assert_eq!(
            shell.dispatch(":read https://example.com"),
            Intent::Peek {
                target: "https://example.com".to_string()
            }
        );
        assert_eq!(shell.cwd(), "cairn://");
    }

    #[test]
    fn a_bare_enter_follows_the_continuation_footer_and_nothing_else() {
        let mut shell = Shell::new();
        assert_eq!(shell.dispatch(""), Intent::Noop);
        shell.record(
            "body\n[lines 1–5 of 90 — continue: cairn://p/cairn/4279?offset=5&limit=5]".to_string(),
            None,
        );
        assert_eq!(
            shell.dispatch(""),
            Intent::Peek {
                target: "cairn://p/cairn/4279?offset=5&limit=5".to_string()
            }
        );
    }

    #[test]
    fn a_comment_is_written_with_no_issue_specific_code_in_the_shell() {
        let mut shell = Shell::new();
        navigate(&mut shell, "p/cairn");
        navigate(&mut shell, "4279");
        shell.record(String::new(), Some(issue_spec()));
        assert_eq!(
            shell.dispatch(":comment ship it"),
            Intent::Mutate {
                target: "cairn://p/cairn/4279".to_string(),
                mode: "append".to_string(),
                payload: serde_json::json!({"content": "ship it"}),
                confirm: None,
            }
        );
    }

    #[test]
    fn an_action_before_anything_is_rendered_says_so() {
        let mut shell = Shell::new();
        assert!(matches!(shell.dispatch(":comment hi"), Intent::Print(_)));
    }

    #[test]
    fn watching_resolves_the_issue_containing_the_cursor() {
        let mut shell = Shell::new();
        navigate(&mut shell, "p/cairn");
        navigate(&mut shell, "4279");
        navigate(&mut shell, "1/builder");
        assert_eq!(
            shell.dispatch(":watch"),
            Intent::Watch {
                uri: "cairn://p/cairn/4279".to_string()
            }
        );
        let mut root = Shell::new();
        assert!(matches!(root.dispatch(":watch"), Intent::Print(_)));
    }

    #[test]
    fn help_lists_the_current_resources_actions_as_commands() {
        let mut shell = Shell::new();
        navigate(&mut shell, "p/cairn");
        navigate(&mut shell, "4279");
        shell.record(String::new(), Some(issue_spec()));
        let Intent::Print(text) = shell.dispatch("?") else {
            panic!("expected help")
        };
        assert!(text.contains(":append-comment"), "{text}");
        assert!(text.contains("content(str)"), "{text}");
        assert!(text.contains(":quit"), "{text}");
    }

    #[test]
    fn listing_narrows_by_prefix_and_switches_to_commands_at_a_colon() {
        let mut shell = Shell::new();
        navigate(&mut shell, "p/cairn");
        navigate(&mut shell, "4279");
        shell.record(
            "cairn://p/cairn/4279/1/builder\ncairn://p/cairn/4279/1/planner".to_string(),
            Some(issue_spec()),
        );
        let Intent::Print(places) = shell.dispatch(":ls 1/") else {
            panic!("expected a listing")
        };
        assert_eq!(places, "1/builder\n1/planner");
        let Intent::Print(commands) = shell.dispatch(":ls :") else {
            panic!("expected a listing")
        };
        assert!(commands.contains(":append-comment"), "{commands}");
    }

    #[test]
    fn quitting_is_recognized_by_both_spellings() {
        let mut shell = Shell::new();
        assert_eq!(shell.dispatch(":quit"), Intent::Quit);
        assert_eq!(shell.dispatch(":exit"), Intent::Quit);
    }
}
