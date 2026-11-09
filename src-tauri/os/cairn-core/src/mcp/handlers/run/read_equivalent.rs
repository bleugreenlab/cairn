//! Read-shaped `run` items, served as the reads they are (tenet 1246).
//!
//! A `run` item that only reads or searches files, and has a `read`
//! equivalent, is served as that read: from the store, through the same
//! composition `read` runs ([`read::compose_read`]), with no lease, no slot
//! admission and no cell. Each target renders under a `↳ read <uri> [suffix]`
//! label, and the item keeps the exit status the shell would have produced, so
//! an agent's reading of success survives the translation. The aim is the
//! answer, not the program's bytes: a read renders matches its own way, and
//! lists what the store tracks rather than what a checkout holds.
//!
//! Translation is a pure function of the command ([`route_item`]). A batch is
//! served only when every item is a read, because a batch runs on one
//! substrate: `sequential` and `stop_on_error` order its items, and two
//! substrates cannot share an order. A read-shaped item that runs on a machine
//! instead ends with a `↳ machine:` note saying why, and the reason is logged
//! under a stable code ([`LOG_TARGET`]) so coverage work can be sized by how
//! often each one occurs. An item that is not read-shaped never carries a note.

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_common::read::{SegmentKind, SegmentMeta};

use super::hygiene::{expand_tilde, normalize_path};
use super::process::append_exit_code;
use super::redact::redact_command;
use super::types::{ItemOutcome, RunSpec};
use crate::mcp::handlers::branch::BranchResolution;
use crate::mcp::handlers::read::{self, view, Composed, FenceAsk, ReadOptions};
use crate::mcp::handlers::search_translate::{
    translate_search_invocation, SearchTranslationError, TranslatedSearch,
};
use crate::mcp::handlers::shell_words::{
    command_for_classification, command_line, Element, Operator, Piece, Word,
};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;

/// The log target every machine reason is recorded under, as
/// `machine reason=<code> command=<redacted>`.
pub(super) const LOG_TARGET: &str = "cairn::run::read_equivalent";

/// The programs whose invocation makes an item read-shaped: after any launcher
/// or leading `cd`, the command's first program is one of these.
const READ_PROGRAMS: &[&str] = &["rg", "grep", "find", "cat", "head", "tail", "sed"];

/// Where one `run` item belongs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ItemRoute {
    /// Not a read at all (`cargo test`, `echo`). It runs on a machine and its
    /// result never mentions this module.
    NotReadShaped,
    /// A read: these targets, in order, and how the shell's status follows
    /// from what they found.
    Read {
        targets: Vec<String>,
        exit: ExitRule,
    },
    /// Read-shaped, but with no read equivalent.
    Machine(MachineReason),
}

/// How a served item's exit status follows from its targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExitRule {
    /// A search: 0 when anything matched, 1 when nothing did, 2 when a target
    /// failed.
    Search,
    /// A pipeline's status is its last stage's (there is no `pipefail`), and
    /// `head` and `wc` succeed whatever they were fed: 0.
    Pipeline,
    /// 0, or this status when any target failed.
    FailsWith(i32),
}

impl ExitRule {
    fn status(self, metas: &[SegmentMeta]) -> i32 {
        let failed = metas.iter().any(|meta| meta.kind == SegmentKind::Error);
        match self {
            Self::Search if failed => 2,
            Self::Search if metas.iter().any(matched) => 0,
            Self::Search => 1,
            Self::Pipeline => 0,
            Self::FailsWith(code) if failed => code,
            Self::FailsWith(_) => 0,
        }
    }
}

/// A grep segment found something. A duplicate stands for a result the caller
/// already holds unchanged, so it is not a miss.
fn matched(meta: &SegmentMeta) -> bool {
    meta.kind == SegmentKind::Duplicate
        || meta.match_count.unwrap_or(0) > 0
        || meta.file_count.unwrap_or(0) > 0
}

/// Why a read-shaped item runs on a machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MachineReason {
    /// Stable, countable: `flag:-m`, `stage:sort`, `sibling`, `branch`, `fence`.
    pub(super) code: String,
    /// What the item's trailing note says.
    pub(super) detail: String,
}

impl MachineReason {
    fn new(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
        }
    }

    /// The line appended to the item's result.
    pub(super) fn note(&self) -> String {
        format!("↳ machine: {}", self.detail)
    }
}

/// What [`route_item`] needs besides the command: where relative paths land.
pub(super) struct RouteContext<'a> {
    pub(super) cwd: &'a str,
    /// The job's head coordinate. Its absence marks an ambient caller, whose
    /// relative paths are host paths under `cwd`.
    pub(super) logical: Option<&'a BranchResolution>,
    pub(super) roots: &'a ProjectRoots,
}

/// Route one shell command.
pub(super) fn route_item(command: &str, ctx: &RouteContext<'_>) -> ItemRoute {
    let classified = command_for_classification(command);
    let line = match read_line(&classified) {
        LineShape::NotReadShaped => return ItemRoute::NotReadShaped,
        LineShape::Machine(reason) => return ItemRoute::Machine(reason),
        LineShape::Read(line) => line,
    };
    match translate_line(&line, ctx) {
        Ok((targets, exit)) => ItemRoute::Read { targets, exit },
        Err(reason) => ItemRoute::Machine(reason),
    }
}

/// A read-shaped command line: an optional leading `cd DIR &&`, then the read
/// program and any pipeline stages after it, each as its words.
struct ReadLine {
    cd: Option<String>,
    stages: Vec<Vec<String>>,
}

enum LineShape {
    NotReadShaped,
    Machine(MachineReason),
    Read(ReadLine),
}

fn program_name(word: &str) -> &str {
    Path::new(word)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(word)
}

fn is_read_program(word: &Word) -> bool {
    word.literal && READ_PROGRAMS.contains(&program_name(&word.text))
}

fn operator_text(operator: Operator) -> &'static str {
    match operator {
        Operator::Pipe => "|",
        Operator::PipeAll => "|&",
        Operator::And => "&&",
        Operator::Or => "||",
        Operator::Sequence => ";",
        Operator::Background => "&",
    }
}

fn read_line(command: &str) -> LineShape {
    let Some(elements) = command_line(command) else {
        // Syntax the splitter cannot read: a heredoc, process substitution,
        // unbalanced quoting.
        let first = command.split_whitespace().next().unwrap_or_default();
        return if READ_PROGRAMS.contains(&program_name(first)) {
            LineShape::Machine(MachineReason::new(
                "shell:syntax",
                "this shell syntax has no read equivalent",
            ))
        } else {
            LineShape::NotReadShaped
        };
    };

    // Each simple command with the operator that follows it.
    let mut commands: Vec<(Vec<Word>, Option<Operator>)> = Vec::new();
    let mut subshell = false;
    let mut malformed = false;
    for element in elements {
        match element {
            Element::Piece(Piece::Command(words)) => commands.push((words, None)),
            Element::Piece(Piece::Open | Piece::Close) => subshell = true,
            Element::Operator(operator) => match commands.last_mut() {
                Some((_, slot @ None)) => *slot = Some(operator),
                _ => malformed = true,
            },
        }
    }

    let main = commands
        .iter()
        .take_while(|(words, _)| {
            words
                .first()
                .is_some_and(|word| word.plain && word.text == "cd")
        })
        .count();
    let read_shaped = commands
        .get(main)
        .and_then(|(words, _)| words.first())
        .is_some_and(is_read_program);
    if !read_shaped {
        return LineShape::NotReadShaped;
    }

    let machine = |code: &str, detail: String| LineShape::Machine(MachineReason::new(code, detail));
    if subshell {
        return machine("shell:subshell", "a subshell has no read equivalent".into());
    }
    if malformed {
        return machine(
            "shell:syntax",
            "this shell syntax has no read equivalent".into(),
        );
    }
    if let Some(word) = commands
        .iter()
        .flat_map(|(words, _)| words)
        .find(|word| !word.literal)
    {
        return machine(
            "shell:expansion",
            format!(
                "the shell would rewrite `{}` before the program saw it",
                word.text
            ),
        );
    }

    // Leading `cd`s compose: each relative one descends from the last.
    let mut cd: Option<String> = None;
    for (words, operator) in &commands[..main] {
        if words.len() != 2 || words[1].text == "-" {
            return machine("shell:cd", "this `cd` has no read equivalent".into());
        }
        if *operator != Some(Operator::And) {
            let shown = operator.map_or("", operator_text);
            return machine(
                "shell:operator",
                format!("`cd …{shown}` has no read equivalent"),
            );
        }
        let dir = words[1].text.as_str();
        cd = Some(match cd {
            Some(base) if !dir.starts_with('/') => {
                format!("{}/{dir}", base.trim_end_matches('/'))
            }
            _ => dir.to_string(),
        });
    }

    let rest = &commands[main..];
    for (index, (_, operator)) in rest.iter().enumerate() {
        let last = index + 1 == rest.len();
        match (operator, last) {
            (None, true) | (Some(Operator::Pipe), false) => {}
            (Some(operator), _) => {
                return machine(
                    "shell:operator",
                    format!("`{}` has no read equivalent", operator_text(*operator)),
                )
            }
            (None, false) => {
                return machine(
                    "shell:syntax",
                    "this shell syntax has no read equivalent".into(),
                )
            }
        }
    }

    LineShape::Read(ReadLine {
        cd,
        stages: rest
            .iter()
            .map(|(words, _)| words.iter().map(|word| word.text.clone()).collect())
            .collect(),
    })
}

type Translated = Result<(Vec<String>, ExitRule), MachineReason>;

fn translate_line(line: &ReadLine, ctx: &RouteContext<'_>) -> Translated {
    let program = program_name(&line.stages[0][0]);
    let args = &line.stages[0][1..];
    let post = &line.stages[1..];
    let cd = line.cd.as_deref();
    match program {
        "rg" | "grep" => search_targets(program, args, post, cd, ctx),
        "find" => find_targets(args, post, cd, ctx),
        "cat" => {
            no_stages(post)?;
            cat_targets(args, cd, ctx)
        }
        "head" | "tail" => {
            no_stages(post)?;
            window_targets(program, args, cd, ctx)
        }
        "sed" => {
            no_stages(post)?;
            sed_targets(args, cd, ctx)
        }
        _ => unreachable!("read_line admits only READ_PROGRAMS"),
    }
}

/// A pipeline stage after a search or listing that has a read equivalent.
#[derive(Debug, Clone, Copy)]
enum PostStage {
    /// `| head [-n] N`: the first N results.
    Head(usize),
    /// `| wc -l`: how many there are.
    CountLines,
}

fn stage_reason(stage: &[String]) -> MachineReason {
    MachineReason::new(
        format!("stage:{}", program_name(&stage[0])),
        format!("| {} has no read equivalent", stage.join(" ")),
    )
}

fn no_stages(post: &[Vec<String>]) -> Result<(), MachineReason> {
    post.first()
        .map_or(Ok(()), |stage| Err(stage_reason(stage)))
}

fn post_stage(post: &[Vec<String>]) -> Result<Option<PostStage>, MachineReason> {
    let mut parsed = None;
    for stage in post {
        let this = match program_name(&stage[0]) {
            "head" => match line_count(&stage[1..]) {
                Ok((Some(Count::First(count)), files)) if files.is_empty() => {
                    Some(PostStage::Head(count))
                }
                Ok((None, files)) if files.is_empty() => Some(PostStage::Head(10)),
                _ => None,
            },
            "wc" if stage.len() == 2 && stage[1] == "-l" => Some(PostStage::CountLines),
            _ => None,
        };
        let Some(this) = this else {
            return Err(stage_reason(stage));
        };
        if parsed.is_some() {
            return Err(MachineReason::new(
                "stage:chain",
                "a chain of pipeline stages has no read equivalent",
            ));
        }
        parsed = Some(this);
    }
    Ok(parsed)
}

fn translation_reason(program: &str, error: SearchTranslationError) -> MachineReason {
    match error {
        SearchTranslationError::StdinInput => MachineReason::new(
            "stdin",
            format!("{program} reading stdin has no read equivalent"),
        ),
        SearchTranslationError::UnsupportedFlag(flag) => {
            let name = flag.split([' ', '=']).next().unwrap_or(&flag);
            MachineReason::new(
                format!("flag:{name}"),
                format!("{program} {flag} has no read equivalent"),
            )
        }
        SearchTranslationError::UnsupportedPattern => MachineReason::new(
            "pattern:bre",
            "this basic-regex pattern has no read equivalent",
        ),
        SearchTranslationError::UnsupportedProgram
        | SearchTranslationError::UnsupportedInvocation => MachineReason::new(
            "invocation",
            format!("this {program} invocation has no read equivalent"),
        ),
    }
}

/// The one glob a read target takes, from the globs a search named.
fn single_glob(globs: Vec<String>) -> Result<Option<String>, MachineReason> {
    if globs.len() > 1 {
        return Err(MachineReason::new(
            "glob:multiple",
            "more than one glob has no read equivalent (a read takes one `glob=`)",
        ));
    }
    match globs.into_iter().next() {
        Some(glob) if glob.starts_with('!') => Err(MachineReason::new(
            "glob:negated",
            format!("the negated glob `{glob}` has no read equivalent"),
        )),
        glob => Ok(glob),
    }
}

/// A listing glob that matches at any depth, as `rg -g` and `find -name` do.
fn anywhere(glob: &str) -> String {
    if glob.contains('/') {
        glob.to_string()
    } else {
        format!("**/{glob}")
    }
}

/// A query value as the read grammar carries it. Only `%` (an escape) and `&`
/// (a parameter separator) need escaping; leaving the rest readable keeps the
/// `↳ read` label something an agent can copy.
fn query_value(value: &str) -> String {
    value.replace('%', "%25").replace('&', "%26")
}

fn query(params: &[(&str, String)]) -> String {
    params
        .iter()
        .map(|(key, value)| {
            if value.is_empty() {
                (*key).to_string()
            } else {
                format!("{key}={}", query_value(value))
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn search_targets(
    program: &str,
    args: &[String],
    post: &[Vec<String>],
    cd: Option<&str>,
    ctx: &RouteContext<'_>,
) -> Translated {
    let search = translate_search_invocation(program, args)
        .map_err(|error| translation_reason(program, error))?;
    let post = post_stage(post)?;
    let exit = |own: ExitRule| {
        if post.is_some() {
            ExitRule::Pipeline
        } else {
            own
        }
    };
    match search {
        TranslatedSearch::Grep {
            pattern,
            globs,
            mut output_mode,
            case_insensitive,
            before_context,
            after_context,
            max_per_file,
            paths,
            ..
        } => {
            if max_per_file.is_some() {
                return Err(MachineReason::new(
                    "flag:-m",
                    format!("{program} -m has no read equivalent"),
                ));
            }
            let glob = single_glob(globs)?;
            let mut params = vec![("grep", pattern)];
            if case_insensitive == Some(true) {
                params.push(("-i", String::new()));
            }
            if before_context == after_context && before_context > 0 {
                params.push(("-C", before_context.to_string()));
            } else {
                if after_context > 0 {
                    params.push(("-A", after_context.to_string()));
                }
                if before_context > 0 {
                    params.push(("-B", before_context.to_string()));
                }
            }
            if let Some(glob) = glob {
                params.push(("glob", glob));
            }
            let mut head_limit = None;
            match post {
                Some(PostStage::Head(count)) => head_limit = Some(count),
                // Counting matched lines is counting matches; counting `-l`
                // or `-c` output lines is counting files, which their own
                // headers already say.
                Some(PostStage::CountLines) if output_mode == "content" => {
                    output_mode = "count".to_string()
                }
                Some(PostStage::CountLines) | None => {}
            }
            params.push(("output_mode", output_mode));
            if let Some(count) = head_limit {
                params.push(("head_limit", count.to_string()));
            }
            let query = query(&params);
            let roots: Vec<Option<&str>> = if paths.is_empty() {
                vec![None]
            } else {
                paths.iter().map(|path| Some(path.as_str())).collect()
            };
            let targets = roots
                .into_iter()
                .map(|path| Ok(format!("{}?{query}", identity(ctx, cd, path)?)))
                .collect::<Result<_, MachineReason>>()?;
            Ok((targets, exit(ExitRule::Search)))
        }
        TranslatedSearch::Files { globs, path } => {
            let glob = single_glob(globs)?
                .map(|glob| anywhere(&glob))
                .unwrap_or_else(|| "**/*".to_string());
            let target = listing_target(identity(ctx, cd, path.as_deref())?, glob, post);
            Ok((vec![target], exit(ExitRule::FailsWith(2))))
        }
    }
}

fn listing_target(identity: String, glob: String, post: Option<PostStage>) -> String {
    let mut params = vec![("glob", glob)];
    if let Some(PostStage::Head(count)) = post {
        params.push(("limit", count.to_string()));
    }
    format!("{identity}?{}", query(&params))
}

fn find_targets(
    args: &[String],
    post: &[Vec<String>],
    cd: Option<&str>,
    ctx: &RouteContext<'_>,
) -> Translated {
    let unsupported = |flag: &str, shown: String| {
        Err(MachineReason::new(
            format!("find:{flag}"),
            format!("find {shown} has no read equivalent"),
        ))
    };
    let starts = args.iter().take_while(|arg| !arg.starts_with('-')).count();
    let (paths, expression) = args.split_at(starts);
    let mut name = None;
    let mut files_only = false;
    let mut index = 0;
    while index < expression.len() {
        let flag = expression[index].as_str();
        let value = expression.get(index + 1);
        match (flag, value.map(String::as_str)) {
            ("-name", Some(pattern)) if !pattern.contains('/') => name = Some(pattern.to_string()),
            ("-type", Some("f")) => files_only = true,
            ("-type", Some(kind)) => return unsupported("-type", format!("-type {kind}")),
            _ => return unsupported(flag, flag.to_string()),
        }
        index += 2;
    }
    if name.is_none() && !files_only {
        return Err(MachineReason::new(
            "find:bare",
            "find without -name or -type f lists directories too, which a read glob does not",
        ));
    }
    let post = post_stage(post)?;
    let glob = name.map_or_else(|| "**/*".to_string(), |name| anywhere(&name));
    let roots: Vec<Option<&str>> = if paths.is_empty() {
        vec![None]
    } else {
        paths.iter().map(|path| Some(path.as_str())).collect()
    };
    let targets = roots
        .into_iter()
        .map(|path| Ok(listing_target(identity(ctx, cd, path)?, glob.clone(), post)))
        .collect::<Result<_, MachineReason>>()?;
    let exit = if post.is_some() {
        ExitRule::Pipeline
    } else {
        ExitRule::FailsWith(1)
    };
    Ok((targets, exit))
}

fn stdin_reason(program: &str) -> MachineReason {
    MachineReason::new(
        "stdin",
        format!("{program} reading stdin has no read equivalent"),
    )
}

fn flag_reason(program: &str, flag: &str) -> MachineReason {
    MachineReason::new(
        format!("flag:{flag}"),
        format!("{program} {flag} has no read equivalent"),
    )
}

fn file_identities(
    program: &str,
    files: &[String],
    cd: Option<&str>,
    ctx: &RouteContext<'_>,
) -> Result<Vec<String>, MachineReason> {
    if files.is_empty() || files.iter().any(|file| file == "-") {
        return Err(stdin_reason(program));
    }
    files
        .iter()
        .map(|file| identity(ctx, cd, Some(file)))
        .collect()
}

fn cat_targets(args: &[String], cd: Option<&str>, ctx: &RouteContext<'_>) -> Translated {
    if let Some(flag) = args.iter().find(|arg| arg.starts_with('-') && *arg != "-") {
        return Err(flag_reason("cat", flag));
    }
    Ok((
        file_identities("cat", args, cd, ctx)?,
        ExitRule::FailsWith(1),
    ))
}

/// A `head`/`tail` line count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Count {
    /// `-n N`: the first (head) or last (tail) N lines.
    First(usize),
    /// `tail -n +K`: from line K on.
    From(usize),
}

/// Split `head`/`tail` arguments into the line count and the files. `Err`
/// names a flag with no read equivalent.
fn line_count(args: &[String]) -> Result<(Option<Count>, Vec<String>), String> {
    let parse = |value: &str| -> Result<Count, String> {
        match value.strip_prefix('+') {
            Some(from) => from.parse().map(Count::From),
            None => value.parse().map(Count::First),
        }
        .map_err(|_| format!("-n {value}"))
    };
    let mut count = None;
    let mut files = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let arg = args[index].as_str();
        if arg == "-n" || arg == "--lines" {
            let value = args.get(index + 1).ok_or_else(|| arg.to_string())?;
            count = Some(parse(value)?);
            index += 2;
            continue;
        }
        if let Some(value) = arg
            .strip_prefix("--lines=")
            .or_else(|| arg.strip_prefix("-n").filter(|value| !value.is_empty()))
        {
            count = Some(parse(value)?);
        } else if arg.len() > 1
            && arg.starts_with('-')
            && arg[1..].bytes().all(|byte| byte.is_ascii_digit())
        {
            count = Some(parse(&arg[1..])?);
        } else if arg.starts_with('-') && arg != "-" {
            return Err(arg.to_string());
        } else {
            files.push(arg.to_string());
        }
        index += 1;
    }
    Ok((count, files))
}

fn window_targets(
    program: &str,
    args: &[String],
    cd: Option<&str>,
    ctx: &RouteContext<'_>,
) -> Translated {
    let (count, files) = line_count(args).map_err(|flag| flag_reason(program, &flag))?;
    let window = match (program, count.unwrap_or(Count::First(10))) {
        (_, Count::First(0)) => return Err(flag_reason(program, "-n 0")),
        ("head", Count::First(count)) => format!("limit={count}"),
        ("tail", Count::First(count)) => format!("offset=-{count}"),
        ("tail", Count::From(line)) => format!("offset={}", line.saturating_sub(1)),
        (_, Count::From(line)) => return Err(flag_reason(program, &format!("-n +{line}"))),
        _ => unreachable!("window_targets serves only head and tail"),
    };
    let targets = file_identities(program, &files, cd, ctx)?
        .into_iter()
        .map(|identity| format!("{identity}?{window}"))
        .collect();
    Ok((targets, ExitRule::FailsWith(1)))
}

fn sed_targets(args: &[String], cd: Option<&str>, ctx: &RouteContext<'_>) -> Translated {
    let lines = match args {
        [quiet, script, _file] if quiet == "-n" => script.strip_suffix('p').and_then(|range| {
            let (start, end) = range.split_once(',').unwrap_or((range, range));
            let (start, end) = (start.parse::<usize>().ok()?, end.parse::<usize>().ok()?);
            (start >= 1 && end >= start).then_some((start, end))
        }),
        _ => None,
    };
    let Some((start, end)) = lines else {
        return Err(MachineReason::new(
            "sed:script",
            format!("sed {} has no read equivalent", args.join(" ")),
        ));
    };
    let identity = identity(ctx, cd, Some(&args[2]))?;
    Ok((
        vec![format!(
            "{identity}?offset={}&limit={}",
            start - 1,
            end - start + 1
        )],
        ExitRule::FailsWith(1),
    ))
}

/// The `file:` identity a path argument names, re-rooted under a leading `cd`.
fn identity(
    ctx: &RouteContext<'_>,
    cd: Option<&str>,
    path: Option<&str>,
) -> Result<String, MachineReason> {
    let effective = match (cd, path) {
        (None, path) => path.map(str::to_string),
        (Some(dir), None) => Some(dir.to_string()),
        (Some(_), Some(path)) if path.starts_with('/') => Some(path.to_string()),
        (Some(dir), Some(path)) => Some(format!("{}/{path}", dir.trim_end_matches('/'))),
    };
    if let Some(path) = effective.as_deref() {
        // An unquoted `~` is a shell expansion and never reaches here; a quoted
        // one names a directory literally called `~`.
        if path.starts_with('~') {
            return Err(MachineReason::new(
                "path:tilde",
                format!("the literal path `{path}` has no read equivalent"),
            ));
        }
        if path.contains('?') {
            return Err(MachineReason::new(
                "path:query",
                format!("`{path}` contains `?`, which a read target cannot carry"),
            ));
        }
    }
    match classify_target(ctx.logical, ctx.roots, ctx.cwd, effective.as_deref())? {
        Target::Project { repo_path } => Ok(format!("file:{repo_path}")),
        Target::Host { root } => Ok(format!("file:{}", root.display())),
    }
}

/// Which authority owns one target's content.
#[derive(Debug)]
enum Target {
    /// Project content, addressed at the job's head coordinate.
    Project { repo_path: String },
    /// A path outside the project: the host filesystem is its only authority.
    Host { root: PathBuf },
}

/// The absolute locations that hold this project's content rather than the
/// host's. Project content is served from the coordinate and never from a
/// filesystem copy of it, so an absolute path landing in one of these is not
/// host content however it was spelled.
pub(super) struct ProjectRoots {
    /// The project's own repository. An absolute path under it names project
    /// content directly and maps onto a repo-relative target.
    repository: Vec<PathBuf>,
    /// Places that hold a *copy* of project content: the fleet's materialized
    /// checkouts, and the runner-owned operation store. Neither can be proven
    /// to match the job's coordinate (a materialization trails the head
    /// between refreshes), so a read there runs on a machine rather than risk
    /// answering from stale bytes.
    copies: Vec<PathBuf>,
}

impl ProjectRoots {
    pub(super) fn new(config_dir: &Path, logical: Option<&BranchResolution>) -> Self {
        let mut repository = Vec::new();
        let mut copies = Vec::new();
        // Both the home's inventory and a build root's: cells move to the
        // latter, and what stays behind in the former is no fresher.
        push_root(&mut copies, &config_dir.join("build-slots"));
        let inventory = crate::config::settings::colocated_cell_inventory(config_dir);
        if inventory != config_dir.join("build-slots") {
            push_root(&mut copies, &inventory);
        }
        if let Some(resolution) = logical {
            push_root(&mut repository, &resolution.object_repository_path);
            if resolution.repository_path != resolution.object_repository_path {
                push_root(&mut copies, &resolution.repository_path);
            }
        }
        Self { repository, copies }
    }
}

/// Record a root under both its literal and its symlink-resolved form. On macOS
/// `/var` and `/private/var` name the same directory, and a prefix test that
/// knows only one of them silently misclassifies paths spelled the other way.
fn push_root(roots: &mut Vec<PathBuf>, path: &Path) {
    roots.push(path.to_path_buf());
    if let Ok(canonical) = std::fs::canonicalize(path) {
        if canonical != path {
            roots.push(canonical);
        }
    }
}

/// Resolve `path` through symlinks as far as the filesystem allows, keeping any
/// trailing components that do not exist yet.
///
/// Plain `canonicalize` gives up entirely on a missing leaf, which would leave
/// `/alias/src/absent.rs` looking like host content even when `/alias` points
/// straight into the project.
fn resolve_through_symlinks(path: &Path) -> Option<PathBuf> {
    let mut trailing = Vec::new();
    let mut current = path;
    loop {
        if let Ok(resolved) = std::fs::canonicalize(current) {
            let mut full = resolved;
            for component in trailing.iter().rev() {
                full.push(component);
            }
            return Some(full);
        }
        trailing.push(current.file_name()?);
        current = current.parent()?;
    }
}

/// Decide which authority owns one target.
///
/// A managed job's process residence is scratch, so a relative path always names
/// project content. An absolute path is host content only when it lands outside
/// every copy of the project: inside the repository it is project content at
/// the coordinate, and inside a materialization or the operation store it runs
/// on a machine. Spelling a project path absolutely must not be a way to read
/// bytes the coordinate does not vouch for.
///
/// Ambient callers have no logical coordinate, so every target they name is
/// host content rooted at their cwd.
fn classify_target(
    logical: Option<&BranchResolution>,
    roots: &ProjectRoots,
    cwd: &str,
    path_arg: Option<&str>,
) -> Result<Target, MachineReason> {
    if logical.is_none() {
        let root = match path_arg {
            Some(path) if Path::new(&expand_tilde(path)).is_absolute() => {
                PathBuf::from(expand_tilde(path))
            }
            Some(path) => Path::new(cwd).join(path),
            None => PathBuf::from(cwd),
        };
        return Ok(Target::Host { root });
    }
    let Some(path) = path_arg else {
        return Ok(Target::Project {
            repo_path: String::new(),
        });
    };

    let expanded = expand_tilde(path);
    if !Path::new(&expanded).is_absolute() {
        let components: Vec<&str> = path
            .split('/')
            .filter(|component| !component.is_empty() && *component != ".")
            .collect();
        // A path that climbs out of the project has no coordinate to be
        // addressed at, and guessing where it lands would not be faithful.
        if components.contains(&"..") {
            return Err(MachineReason::new(
                "path:climbs",
                format!("`{path}` climbs out of the project"),
            ));
        }
        return Ok(Target::Project {
            repo_path: components.join("/"),
        });
    }

    let absolute = PathBuf::from(normalize_path(&expanded, cwd));
    // A symlink can alias project content from anywhere on the filesystem, so
    // the target is tested under both the spelling it was given and the path it
    // actually resolves to. Only classification consults the resolved form; a
    // host target keeps the original spelling.
    let resolved = resolve_through_symlinks(&absolute).filter(|path| *path != absolute);
    let spellings: Vec<&Path> = std::iter::once(absolute.as_path())
        .chain(resolved.as_deref())
        .collect();

    for spelling in &spellings {
        for root in &roots.repository {
            if let Ok(relative) = spelling.strip_prefix(root) {
                let Some(relative) = relative.to_str() else {
                    break;
                };
                return Ok(Target::Project {
                    repo_path: relative.trim_matches('/').to_string(),
                });
            }
        }
    }
    if spellings.iter().any(|spelling| {
        roots
            .copies
            .iter()
            .any(|root| spelling.starts_with(root) || root.starts_with(spelling))
    }) {
        return Err(MachineReason::new(
            "path:checkout-copy",
            format!("`{path}` is inside a checkout copy whose content the store cannot vouch for"),
        ));
    }
    Ok(Target::Host { root: absolute })
}

/// What a batch's routing decided.
pub(super) enum BatchRoute {
    /// Every item was a read, and these are their outcomes.
    Served(Vec<ItemOutcome>),
    /// The batch runs on a machine. Each read-shaped item's index, with why;
    /// an item that is not read-shaped has no entry.
    Machine(Vec<(usize, MachineReason)>),
}

fn route_spec(spec: &Result<RunSpec, String>, ctx: &RouteContext<'_>) -> ItemRoute {
    match spec {
        Ok(RunSpec::Shell { command, args, .. }) => match route_item(command, ctx) {
            route @ ItemRoute::NotReadShaped => route,
            _ if !args.is_empty() => ItemRoute::Machine(MachineReason::new(
                "shell:args",
                "a procedure's arguments have no read equivalent",
            )),
            route => route,
        },
        _ => ItemRoute::NotReadShaped,
    }
}

/// Serve a batch whose items are all reads, or say why each read-shaped item
/// runs on a machine.
///
/// Every item's targets are composed in **one** read, so read's single budget
/// water-fills across the whole batch, and the composition never prompts: a
/// target only a fence permission could open sends the batch to a machine,
/// with nothing recorded. `deadline` is the batch's own bound; past it every
/// item fails as a timed-out command would.
#[allow(clippy::too_many_arguments)]
pub(super) async fn try_serve_batch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    logical: Option<&BranchResolution>,
    cwd: &str,
    resolved: &[(String, Result<RunSpec, String>)],
    sequential: bool,
    stop_on_error: bool,
    branch_scoped: bool,
    deadline: Duration,
) -> BatchRoute {
    let roots = ProjectRoots::new(&orch.config_dir, logical);
    let ctx = RouteContext {
        cwd,
        logical,
        roots: &roots,
    };
    let routes: Vec<ItemRoute> = resolved
        .iter()
        .map(|(_, spec)| route_spec(spec, &ctx))
        .collect();

    if branch_scoped {
        let branch = MachineReason::new("branch", "branch-scoped run");
        return machine(resolved, &routes, None, Some(branch));
    }
    let mut plans = Vec::with_capacity(routes.len());
    for route in &routes {
        match route {
            ItemRoute::Read { targets, exit } => plans.push((targets.as_slice(), *exit)),
            _ => return machine(resolved, &routes, None, None),
        }
    }

    let mut targets = Vec::new();
    let mut spans: Vec<Range<usize>> = Vec::with_capacity(plans.len());
    for (item_targets, _) in &plans {
        let start = targets.len();
        targets.extend(item_targets.iter().cloned());
        spans.push(start..targets.len());
    }
    let owner = |target: usize| {
        spans
            .iter()
            .position(|span| span.contains(&target))
            .unwrap_or(0)
    };

    let options = ReadOptions {
        budget: served_read_budget(request, resolved, targets.len()),
        include_bodies: true,
        fence_ask: FenceAsk::Decline,
    };
    let cursors = read::ReadCursorState::default();
    let composed = tokio::time::timeout(
        deadline,
        read::compose_read(orch, request, &cursors, &targets, options),
    )
    .await;
    let envelope = match composed {
        Ok(Composed::Envelope(envelope)) => envelope,
        Ok(Composed::Declined { target, reason }) => {
            let index = targets.iter().position(|each| *each == target).unwrap_or(0);
            let cause = (owner(index), MachineReason::new("fence", reason));
            return machine(resolved, &routes, Some(cause), None);
        }
        Ok(Composed::Suspended(_)) => {
            let cause = MachineReason::new("fence", "a read target would need a fence permission");
            return machine(resolved, &routes, Some((0, cause)), None);
        }
        Err(_) => {
            let outcomes = resolved
                .iter()
                .map(|(header, _)| {
                    ItemOutcome::failed(
                        header.clone(),
                        format!("Command timed out after {}ms", deadline.as_millis()),
                    )
                })
                .collect();
            return BatchRoute::Served(stop_after_failure(outcomes, sequential, stop_on_error));
        }
    };

    // Read guarantees one segment per target, in order; anything else is not
    // a composition this module can map back onto its items.
    let bodies = match envelope.bodies.as_ref() {
        Some(bodies)
            if bodies.len() == targets.len() && envelope.segments.len() == targets.len() =>
        {
            bodies
        }
        _ => {
            let cause = MachineReason::new(
                "decline:composition",
                "the read could not be mapped back onto the batch's items",
            );
            return machine(resolved, &routes, Some((0, cause)), None);
        }
    };

    // A failed project target that the repository ignores is not a missing
    // path: only a checkout holds it. Read can fetch one ignored file from the
    // live materialization, but has nothing to enumerate an ignored directory
    // with, so that target is a machine's.
    if let Some(resolution) = logical {
        for (index, meta) in envelope.segments.iter().enumerate() {
            if meta.kind != SegmentKind::Error {
                continue;
            }
            if let Some(repo_path) =
                repo_path_of(&targets[index]).filter(|path| is_ignored(resolution, path))
            {
                let cause = MachineReason::new(
                    "decline:ignored",
                    format!(
                        "`{repo_path}` is ignored by the repository, so only a checkout holds it"
                    ),
                );
                return machine(resolved, &routes, Some((owner(index), cause)), None);
            }
        }
    }

    let mut outcomes: Vec<ItemOutcome> = spans
        .iter()
        .zip(&plans)
        .zip(resolved)
        .map(|((span, (_, exit)), (header, _))| {
            served_outcome(
                header.clone(),
                &envelope.segments[span.clone()],
                &bodies[span.clone()],
                *exit,
            )
        })
        .collect();
    if let Some(first) = outcomes.first_mut() {
        first.images = envelope.images;
    }
    for (header, _) in resolved {
        log::debug!(
            target: LOG_TARGET,
            "served without placement: {}",
            redact_command(header)
        );
    }
    BatchRoute::Served(stop_after_failure(outcomes, sequential, stop_on_error))
}

/// The budget a served batch's read composes under: the one `read` itself
/// would use for this caller ([`read::read_budget_for`]), with a model-facing
/// read's context budget framed for the run result's own text. A code caller
/// reads every target whole, as its `read` would.
fn served_read_budget(
    request: &McpCallbackRequest,
    resolved: &[(String, Result<RunSpec, String>)],
    targets: usize,
) -> view::ReadBudget {
    match read::read_budget_for(request) {
        view::ReadBudget::Context => view::ReadBudget::FramedContext {
            frame_bytes: framing_bytes(resolved, targets),
        },
        budget => budget,
    }
}

/// Bytes the run result adds around the read's segments: each item's
/// `=== header ===` line when the batch has several, a label one byte longer
/// than the read frame it replaces (plus its join), and an exit-code line.
/// Reserving them keeps the framed result under the same client ceiling the
/// read itself is held to, so a served item can be carried whole.
fn framing_bytes(resolved: &[(String, Result<RunSpec, String>)], targets: usize) -> usize {
    let headers = if resolved.len() > 1 {
        resolved
            .iter()
            .map(|(header, _)| header.len() + "=== ".len() + " ===\n".len() + 1)
            .sum()
    } else {
        0
    };
    let labels = targets * 2;
    let exits = resolved.len() * "\n\nExit code: -2147483648".len();
    headers + labels + exits
}

fn served_outcome(
    header: String,
    metas: &[SegmentMeta],
    bodies: &[String],
    exit: ExitRule,
) -> ItemOutcome {
    let mut body = String::new();
    for (meta, segment_body) in metas.iter().zip(bodies) {
        if !body.is_empty() {
            body.push('\n');
        }
        body.push_str("↳ read ");
        body.push_str(&view::segment_label(meta));
        if !segment_body.is_empty() {
            body.push('\n');
            body.push_str(segment_body);
        }
    }
    let status = exit.status(metas);
    append_exit_code(&mut body, status);
    ItemOutcome {
        header,
        body,
        succeeded: status == 0,
        suspended: false,
        images: Vec::new(),
        tracked_modifications: None,
        data: None,
        // Read already budgeted this body and its `continue:` footer is only
        // valid uncut, so the run composer must not tail-cap it again.
        whole: true,
    }
}

/// Reads have no side effects, so a sequential stop-on-error batch can be
/// composed whole and then cut after its first failure: the result is the one
/// running the items in order would give.
fn stop_after_failure(
    mut outcomes: Vec<ItemOutcome>,
    sequential: bool,
    stop_on_error: bool,
) -> Vec<ItemOutcome> {
    if sequential && stop_on_error {
        if let Some(failed) = outcomes.iter().position(|outcome| !outcome.succeeded) {
            outcomes.truncate(failed + 1);
        }
    }
    outcomes
}

/// The repository path a project target names; `None` for a host target.
fn repo_path_of(target: &str) -> Option<&str> {
    let identity = target
        .split_once('?')
        .map_or(target, |(identity, _)| identity);
    identity
        .strip_prefix("file:")
        .filter(|path| !path.is_empty() && !path.starts_with('/'))
}

fn is_ignored(resolution: &BranchResolution, repo_path: &str) -> bool {
    read::object_read::ObjectReadService::new(
        resolution.object_repository_path.clone(),
        resolution.commit_id.clone(),
        repo_path.to_string(),
    )
    .and_then(|service| {
        service
            .is_ignored_path(repo_path)
            .map_err(|error| error.to_string())
    })
    .unwrap_or(false)
}

/// The machine route, with a reason for every read-shaped item.
///
/// `cause` is a read item that turned out to need a machine while being
/// served. `all_reads` is a reason that applies to every read item at once (a
/// branch-scoped run). Otherwise a read item runs on a machine because of a
/// sibling, and its note names the first item that needed one.
fn machine(
    resolved: &[(String, Result<RunSpec, String>)],
    routes: &[ItemRoute],
    cause: Option<(usize, MachineReason)>,
    all_reads: Option<MachineReason>,
) -> BatchRoute {
    let blocker = cause.as_ref().map(|(index, _)| *index).or_else(|| {
        routes
            .iter()
            .position(|route| !matches!(route, ItemRoute::Read { .. }))
    });
    let sibling = blocker.map(|index| {
        MachineReason::new(
            "sibling",
            format!(
                "item {} ({}) needs one; a batch runs on one substrate",
                index + 1,
                short_header(&resolved[index].0)
            ),
        )
    });
    let mut notes = Vec::new();
    for (index, route) in routes.iter().enumerate() {
        let reason = match (&cause, route) {
            (Some((at, reason)), _) if *at == index => Some(reason.clone()),
            (_, ItemRoute::Machine(reason)) => Some(reason.clone()),
            (_, ItemRoute::Read { .. }) => all_reads.clone().or_else(|| sibling.clone()),
            (_, ItemRoute::NotReadShaped) => None,
        };
        if let Some(reason) = reason {
            log::info!(
                target: LOG_TARGET,
                "machine reason={} command={}",
                reason.code,
                redact_command(&resolved[index].0)
            );
            notes.push((index, reason));
        }
    }
    BatchRoute::Machine(notes)
}

fn short_header(header: &str) -> String {
    const MAX: usize = 60;
    let redacted = redact_command(header);
    match redacted.char_indices().nth(MAX) {
        Some((end, _)) => format!("{}…", &redacted[..end]),
        None => redacted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolution() -> BranchResolution {
        BranchResolution {
            project_id: "project".to_string(),
            repository_path: PathBuf::from("/repo"),
            object_repository_path: PathBuf::from("/repo"),
            rev: "branch".to_string(),
            commit_id: "head".to_string(),
            default_commit_id: "base".to_string(),
            default_branch: "main".to_string(),
        }
    }

    fn roots(logical: Option<&BranchResolution>) -> ProjectRoots {
        ProjectRoots::new(Path::new("/cairn-home"), logical)
    }

    fn managed(command: &str) -> ItemRoute {
        let resolution = resolution();
        let roots = roots(Some(&resolution));
        let ctx = RouteContext {
            cwd: "/scratch",
            logical: Some(&resolution),
            roots: &roots,
        };
        route_item(command, &ctx)
    }

    fn read(targets: &[&str], exit: ExitRule) -> ItemRoute {
        ItemRoute::Read {
            targets: targets.iter().map(|target| (*target).to_string()).collect(),
            exit,
        }
    }

    #[test]
    fn every_read_shape_translates_to_its_targets_and_exit_rule() {
        use ExitRule::{FailsWith, Pipeline, Search};
        for (command, expected) in [
            (
                "rg needle src",
                read(&["file:src?grep=needle&output_mode=content"], Search),
            ),
            (
                "rg needle",
                read(&["file:?grep=needle&output_mode=content"], Search),
            ),
            (
                "rg -i -C 2 -g '*.rs' needle src tests",
                read(
                    &[
                        "file:src?grep=needle&-i&-C=2&glob=*.rs&output_mode=content",
                        "file:tests?grep=needle&-i&-C=2&glob=*.rs&output_mode=content",
                    ],
                    Search,
                ),
            ),
            (
                "rg -A 1 -B 3 -n -w x src",
                read(
                    &[r"file:src?grep=\b(?:x)\b&-A=1&-B=3&output_mode=content"],
                    Search,
                ),
            ),
            (
                "grep -rln needle .",
                read(
                    &["file:?grep=needle&output_mode=files_with_matches"],
                    Search,
                ),
            ),
            (
                "grep -n needle README.md",
                read(&["file:README.md?grep=needle&output_mode=content"], Search),
            ),
            (
                "rg -c needle -t rust",
                read(&["file:?grep=needle&glob=*.rs&output_mode=count"], Search),
            ),
            (
                "rg 'a&b%' src",
                read(&["file:src?grep=a%26b%25&output_mode=content"], Search),
            ),
            (
                "cd src && rg -n foo",
                read(&["file:src?grep=foo&output_mode=content"], Search),
            ),
            (
                "cd src && cd lib && rg foo",
                read(&["file:src/lib?grep=foo&output_mode=content"], Search),
            ),
            (
                "cd src && rg foo lib.rs ./tests/",
                read(
                    &[
                        "file:src/lib.rs?grep=foo&output_mode=content",
                        "file:src/tests?grep=foo&output_mode=content",
                    ],
                    Search,
                ),
            ),
            (
                "rg needle | head -5",
                read(
                    &["file:?grep=needle&output_mode=content&head_limit=5"],
                    Pipeline,
                ),
            ),
            (
                "rg needle | head",
                read(
                    &["file:?grep=needle&output_mode=content&head_limit=10"],
                    Pipeline,
                ),
            ),
            (
                "rg needle | wc -l",
                read(&["file:?grep=needle&output_mode=count"], Pipeline),
            ),
            (
                "rg -l needle | wc -l",
                read(
                    &["file:?grep=needle&output_mode=files_with_matches"],
                    Pipeline,
                ),
            ),
            ("rg --files", read(&["file:?glob=**/*"], FailsWith(2))),
            (
                "rg --files src -g '*.rs'",
                read(&["file:src?glob=**/*.rs"], FailsWith(2)),
            ),
            (
                "find src -name '*.test.tsx'",
                read(&["file:src?glob=**/*.test.tsx"], FailsWith(1)),
            ),
            (
                "find src -name '*.test.tsx' | wc -l",
                read(&["file:src?glob=**/*.test.tsx"], Pipeline),
            ),
            (
                "find . -type f -name '*.md' | head -3",
                read(&["file:?glob=**/*.md&limit=3"], Pipeline),
            ),
            ("find . -type f", read(&["file:?glob=**/*"], FailsWith(1))),
            (
                "cat a.rs src/b.rs",
                read(&["file:a.rs", "file:src/b.rs"], FailsWith(1)),
            ),
            (
                "head -n 20 a.rs",
                read(&["file:a.rs?limit=20"], FailsWith(1)),
            ),
            ("head a.rs", read(&["file:a.rs?limit=10"], FailsWith(1))),
            ("head -5 a.rs", read(&["file:a.rs?limit=5"], FailsWith(1))),
            (
                "tail -n 20 a.rs",
                read(&["file:a.rs?offset=-20"], FailsWith(1)),
            ),
            (
                "tail -n +5 a.rs",
                read(&["file:a.rs?offset=4"], FailsWith(1)),
            ),
            (
                "sed -n '10,20p' a.rs",
                read(&["file:a.rs?offset=9&limit=11"], FailsWith(1)),
            ),
            (
                "sed -n 7p a.rs",
                read(&["file:a.rs?offset=6&limit=1"], FailsWith(1)),
            ),
            // A whole-line launcher runs exactly its script.
            (
                "bash -lc 'rg foo src'",
                read(&["file:src?grep=foo&output_mode=content"], Search),
            ),
            // An absolute path into the repository addresses the coordinate.
            (
                "rg foo /repo/src",
                read(&["file:src?grep=foo&output_mode=content"], Search),
            ),
            (
                "cat /repo/src/../README.md",
                read(&["file:README.md"], FailsWith(1)),
            ),
            (
                "cd /repo/src && cat lib.rs",
                read(&["file:src/lib.rs"], FailsWith(1)),
            ),
            // Host content stays host content.
            (
                "rg ERROR /tmp/logs",
                read(&["file:/tmp/logs?grep=ERROR&output_mode=content"], Search),
            ),
        ] {
            assert_eq!(managed(command), expected, "{command}");
        }
    }

    #[test]
    fn ambient_relative_paths_become_absolute_host_targets() {
        let roots = roots(None);
        let ctx = RouteContext {
            cwd: "/work",
            logical: None,
            roots: &roots,
        };
        assert_eq!(
            route_item("rg x src", &ctx),
            read(
                &["file:/work/src?grep=x&output_mode=content"],
                ExitRule::Search
            )
        );
        assert_eq!(
            route_item("cd sub && cat a.txt", &ctx),
            read(&["file:/work/sub/a.txt"], ExitRule::FailsWith(1))
        );
        assert_eq!(
            route_item("rg x", &ctx),
            read(&["file:/work?grep=x&output_mode=content"], ExitRule::Search)
        );
    }

    #[test]
    fn commands_that_are_not_reads_are_not_read_shaped() {
        for command in [
            "cargo build",
            "echo hi",
            "git grep needle",
            "FOO=1 rg needle",
            // A chained launcher is not one launcher invocation.
            "bash -c 'rg x' && echo y",
            "cd src && cargo test",
            "ls | grep x",
        ] {
            assert_eq!(managed(command), ItemRoute::NotReadShaped, "{command}");
        }
    }

    #[test]
    fn every_shape_without_a_read_equivalent_names_a_stable_reason() {
        for (command, code) in [
            ("rg -m1 foo", "flag:-m"),
            ("rg --max-count 3 foo", "flag:-m"),
            ("rg --json foo", "flag:--json"),
            ("grep -r --exclude='*.rs' foo src", "flag:--exclude"),
            ("grep -rv foo src", "flag:-v"),
            (r"grep -r '\(a\)\1' src", "pattern:bre"),
            ("grep foo", "stdin"),
            ("cat", "stdin"),
            ("head -5", "stdin"),
            ("rg foo | sort", "stage:sort"),
            ("rg foo | uniq", "stage:uniq"),
            ("rg foo | grep -v test", "stage:grep"),
            ("rg foo | tail -5", "stage:tail"),
            ("rg foo | sed -n 1,5p", "stage:sed"),
            ("rg foo | head -c 10", "stage:head"),
            ("rg foo | head -5 | wc -l", "stage:chain"),
            ("cat a.rs | head -5", "stage:head"),
            ("rg -g '*.rs' -g '*.ts' foo", "glob:multiple"),
            ("rg -g '!target/**' foo", "glob:negated"),
            ("rg foo > out.txt", "shell:expansion"),
            ("rg $PATTERN src", "shell:expansion"),
            ("rg foo src/*.rs", "shell:expansion"),
            ("rg foo ~/notes", "shell:expansion"),
            ("rg foo 2>&1", "shell:expansion"),
            ("rg foo && echo found", "shell:operator"),
            ("rg foo; echo done", "shell:operator"),
            ("rg foo || true", "shell:operator"),
            ("rg foo &", "shell:operator"),
            ("rg foo |& head", "shell:operator"),
            ("cd src; rg foo", "shell:operator"),
            ("(rg foo)", "shell:subshell"),
            ("rg foo <(ls)", "shell:syntax"),
            ("find . -maxdepth 1 -name x", "find:-maxdepth"),
            ("find . -iname x", "find:-iname"),
            ("find . -type d", "find:-type"),
            ("find . -name x -exec cat ';'", "find:-exec"),
            ("find src", "find:bare"),
            ("tail -f app.log", "flag:-f"),
            ("head -c 5 a.rs", "flag:-c"),
            ("cat -n a.rs", "flag:-n"),
            ("head -n 0 a.rs", "flag:-n 0"),
            ("sed -i s/a/b/ a.rs", "sed:script"),
            ("sed -n '$p' a.rs", "sed:script"),
            ("rg foo ../elsewhere", "path:climbs"),
            ("cat src/../../etc/passwd", "path:climbs"),
            (
                "rg foo /cairn-home/build-slots/cairn/slot-3/src",
                "path:checkout-copy",
            ),
            ("rg foo 'odd?name'", "path:query"),
            ("cat '~/notes'", "path:tilde"),
        ] {
            match managed(command) {
                ItemRoute::Machine(reason) => {
                    assert_eq!(reason.code, code, "{command}: {}", reason.detail);
                    assert!(
                        reason.note().starts_with("↳ machine: "),
                        "{command}: {}",
                        reason.note()
                    );
                }
                other => panic!("{command} should need a machine, got {other:?}"),
            }
        }
    }

    #[test]
    fn machine_notes_read_as_the_reason() {
        let ItemRoute::Machine(reason) = managed("rg -m1 foo") else {
            panic!("-m has no read equivalent");
        };
        assert_eq!(reason.note(), "↳ machine: rg -m has no read equivalent");
        let ItemRoute::Machine(reason) = managed("rg foo | sort") else {
            panic!("| sort has no read equivalent");
        };
        assert_eq!(reason.note(), "↳ machine: | sort has no read equivalent");
    }

    fn shell(command: &str) -> (String, Result<RunSpec, String>) {
        (
            command.to_string(),
            Ok(RunSpec::Shell {
                command: command.to_string(),
                timeout: Some(30_000),
                args: Vec::new(),
            }),
        )
    }

    fn notes(batch: &[(String, Result<RunSpec, String>)]) -> Vec<(usize, String)> {
        let resolution = resolution();
        let roots = roots(Some(&resolution));
        let ctx = RouteContext {
            cwd: "/scratch",
            logical: Some(&resolution),
            roots: &roots,
        };
        let routes: Vec<ItemRoute> = batch
            .iter()
            .map(|(_, spec)| route_spec(spec, &ctx))
            .collect();
        match machine(batch, &routes, None, None) {
            BatchRoute::Machine(notes) => notes
                .into_iter()
                .map(|(index, reason)| (index, reason.note()))
                .collect(),
            BatchRoute::Served(_) => unreachable!(),
        }
    }

    #[test]
    fn a_read_forced_onto_a_machine_by_a_sibling_names_the_sibling() {
        assert_eq!(
            notes(&[shell("rg needle src"), shell("cargo test")]),
            vec![(
                0,
                "↳ machine: item 2 (cargo test) needs one; a batch runs on one substrate"
                    .to_string()
            )]
        );
        // A read-shaped sibling without an equivalent carries its own reason,
        // and the read is forced by it.
        assert_eq!(
            notes(&[shell("rg -m1 x"), shell("rg y")]),
            vec![
                (0, "↳ machine: rg -m has no read equivalent".to_string()),
                (
                    1,
                    "↳ machine: item 1 (rg -m1 x) needs one; a batch runs on one substrate"
                        .to_string()
                ),
            ]
        );
        // Nothing read-shaped: nothing to say.
        assert!(notes(&[shell("cargo build"), shell("echo hi")]).is_empty());
    }

    #[test]
    fn a_procedure_argument_keeps_a_read_on_a_machine() {
        let mut item = shell("rg '$1'");
        let Ok(RunSpec::Shell { args, .. }) = &mut item.1 else {
            unreachable!()
        };
        *args = vec!["needle".to_string()];
        let resolution = resolution();
        let roots = roots(Some(&resolution));
        let ctx = RouteContext {
            cwd: "/scratch",
            logical: Some(&resolution),
            roots: &roots,
        };
        assert!(matches!(
            route_spec(&item.1, &ctx),
            ItemRoute::Machine(reason) if reason.code == "shell:args"
        ));
    }

    fn meta(kind: SegmentKind, matches: Option<usize>, files: Option<usize>) -> SegmentMeta {
        let mut meta = SegmentMeta::new(
            "file:src?grep=x",
            kind,
            cairn_common::read::NaturalUnit::Match,
        );
        meta.match_count = matches;
        meta.file_count = files;
        meta
    }

    #[test]
    fn exit_status_follows_the_shell() {
        let hit = meta(SegmentKind::Grep, Some(3), Some(1));
        let miss = meta(SegmentKind::Grep, None, Some(0));
        let error = meta(SegmentKind::Error, None, None);
        assert_eq!(ExitRule::Search.status(std::slice::from_ref(&hit)), 0);
        assert_eq!(ExitRule::Search.status(std::slice::from_ref(&miss)), 1);
        assert_eq!(ExitRule::Search.status(&[miss.clone(), hit.clone()]), 0);
        assert_eq!(ExitRule::Search.status(&[hit.clone(), error.clone()]), 2);
        assert_eq!(ExitRule::Pipeline.status(&[miss.clone(), error.clone()]), 0);
        assert_eq!(ExitRule::FailsWith(1).status(std::slice::from_ref(&hit)), 0);
        assert_eq!(ExitRule::FailsWith(1).status(&[hit, error]), 1);
    }

    #[test]
    fn a_failed_served_item_ends_with_the_shared_exit_code_line() {
        let outcome = served_outcome(
            "rg x".to_string(),
            &[meta(SegmentKind::Grep, None, Some(0))],
            &["No matches found for pattern 'x'".to_string()],
            ExitRule::Search,
        );
        assert!(!outcome.succeeded);
        assert!(outcome.whole);
        assert_eq!(
            outcome.body,
            "↳ read file:src?grep=x [0 files]\nNo matches found for pattern 'x'\n\nExit code: 1"
        );
    }

    #[test]
    fn a_sequential_stop_on_error_batch_stops_after_its_first_failure() {
        let ok = |header: &str| ItemOutcome {
            succeeded: true,
            ..ItemOutcome::failed(header.to_string(), "")
        };
        let outcomes = vec![
            ok("a"),
            ItemOutcome::failed("b".to_string(), "Exit code: 1"),
            ok("c"),
        ];
        assert_eq!(stop_after_failure(outcomes.clone(), true, true).len(), 2);
        assert_eq!(stop_after_failure(outcomes.clone(), true, false).len(), 3);
        assert_eq!(stop_after_failure(outcomes, false, true).len(), 3);
    }

    #[test]
    fn only_relative_file_targets_name_repository_paths() {
        assert_eq!(repo_path_of("file:generated?grep=x"), Some("generated"));
        assert_eq!(repo_path_of("file:src/lib.rs"), Some("src/lib.rs"));
        assert_eq!(repo_path_of("file:?grep=x"), None);
        assert_eq!(repo_path_of("file:/tmp/x"), None);
    }

    /// A symlink can alias project content from anywhere on the filesystem, so
    /// resolving only the known roots is not enough: with `/tmp/link -> /repo`,
    /// `/tmp/link/src` prefix-matches no root and would be read straight off the
    /// working tree. Classification has to resolve the target too.
    #[cfg(unix)]
    #[test]
    fn an_aliased_absolute_path_still_addresses_the_coordinate() {
        let project = tempfile::tempdir().unwrap();
        std::fs::create_dir(project.path().join("src")).unwrap();
        let resolution = BranchResolution {
            repository_path: project.path().to_path_buf(),
            object_repository_path: project.path().to_path_buf(),
            ..resolution()
        };
        let elsewhere = tempfile::tempdir().unwrap();
        let alias = elsewhere.path().join("project-link");
        std::os::unix::fs::symlink(project.path(), &alias).unwrap();

        let roots = roots(Some(&resolution));
        let classify = |path: PathBuf| {
            classify_target(
                Some(&resolution),
                &roots,
                "/scratch",
                Some(&path.display().to_string()),
            )
        };
        match classify(alias.join("src/lib.rs")) {
            Ok(Target::Project { repo_path }) => assert_eq!(repo_path, "src/lib.rs"),
            other => {
                panic!("a symlink into the project must address the coordinate, got {other:?}")
            }
        }
        // A leaf that does not exist yet resolves through its parent.
        match classify(alias.join("src/absent.rs")) {
            Ok(Target::Project { repo_path }) => assert_eq!(repo_path, "src/absent.rs"),
            other => {
                panic!("a missing leaf under an alias must stay project content, got {other:?}")
            }
        }
        // An alias that genuinely points outside the project stays host content.
        let outside = elsewhere.path().join("outside-link");
        std::os::unix::fs::symlink(elsewhere.path(), &outside).unwrap();
        match classify(outside.join("notes.txt")) {
            Ok(Target::Host { root }) => assert!(root.ends_with("outside-link/notes.txt")),
            other => panic!("an alias outside the project must stay host content, got {other:?}"),
        }
    }

    #[test]
    fn the_operation_store_is_a_copy_when_distinct_from_the_repository() {
        let jj = BranchResolution {
            repository_path: PathBuf::from("/cairn-home/stores/project"),
            ..resolution()
        };
        let roots = roots(Some(&jj));
        assert!(matches!(
            classify_target(Some(&jj), &roots, "/scratch", Some("/cairn-home/stores/project/x")),
            Err(reason) if reason.code == "path:checkout-copy"
        ));
        // A sibling whose name merely starts with a root's name is not inside it.
        assert!(matches!(
            classify_target(Some(&jj), &roots, "/scratch", Some("/repository-notes")),
            Ok(Target::Host { root }) if root == Path::new("/repository-notes")
        ));
    }
}
