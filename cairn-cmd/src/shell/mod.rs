//! The interactive Cairn shell: a cursor over the URI grammar.
//!
//! Bare `cairn` on a terminal opens this. The prompt holds a current URI and a
//! typed line resolves against it the way a path resolves against a working
//! directory — except that arriving somewhere *is* reading it, so `cd` and `cat`
//! are the same gesture.
//!
//! Nothing here is a new interface. Resolution is [`cairn_common::uri`], reads go
//! through the same `read_batch` callback `cairn read` uses and print its exact
//! output, writes go through `cairn write`'s change path, and the commands
//! available at any location are whatever that resource's affordance block
//! advertises. The shell adds a cursor and nothing else, which is why
//! `src/shell/` contains no resource name anywhere in it.
//!
//! The split: [`nav`] turns a line into an [`Intent`] with no I/O at all,
//! [`actions`] derives commands from affordances, [`complete`] names what is
//! reachable from here, and this module supplies the terminal and the client.

mod actions;
mod complete;
mod nav;

use std::io::Write;

use cairn_common::read::SegmentKind;

use crate::cli::{
    build_cli_client, cli_callback_url, ensure_callback_reachable, print_unreachable_callback,
    run_cli_change, run_cli_watch,
};
use crate::schemas::{ChangeInput, ChangeItemInput};
use crate::server::CairnCmd;
use nav::{Intent, Shell};

const BANNER: &str =
    "cairn shell — type a segment to go there, `?` for what is here, `:quit` to leave";

/// Run the prompt until end of input or `:quit`. `false` means the shell could
/// not start, which is the process exit code.
pub(crate) async fn run() -> bool {
    let callback_url = cli_callback_url();
    if !ensure_callback_reachable(&callback_url).await {
        print_unreachable_callback(&callback_url);
        return false;
    }
    let client = build_cli_client(callback_url);
    let mut shell = Shell::new();

    println!("{BANNER}");
    let start = shell.cwd().to_string();
    navigate(&client, &mut shell, &start, true).await;

    loop {
        print!("{}", shell.prompt());
        let _ = std::io::stdout().flush();
        let Some(line) = read_line().await else {
            // End of input is how a person leaves a prompt.
            println!();
            return true;
        };

        match shell.dispatch(&line) {
            Intent::Noop => {}
            Intent::Quit => return true,
            Intent::Print(text) => println!("{}", text.trim_end()),
            Intent::Peek { target } => peek(&client, &mut shell, &target).await,
            Intent::Navigate { target, remember } => {
                navigate(&client, &mut shell, &target, remember).await
            }
            Intent::Watch { uri } => watch(uri).await,
            Intent::Mutate {
                target,
                mode,
                payload,
                confirm,
            } => mutate(target, mode, payload, confirm).await,
        }
    }
}

/// Read one line without holding a runtime thread hostage.
async fn read_line() -> Option<String> {
    tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        match std::io::stdin().read_line(&mut line) {
            Ok(0) | Err(_) => None,
            Ok(_) => Some(line),
        }
    })
    .await
    .ok()
    .flatten()
}

/// Read a target, print it, and move the cursor there — but only if the read
/// actually produced the resource.
async fn navigate(client: &CairnCmd, shell: &mut Shell, target: &str, remember: bool) {
    let Some((text, spec, kind)) = render(client, &shell.read_target(target)).await else {
        return;
    };
    shell.record(text, spec);
    // A parseable URI can still name nothing that exists. The segment's own
    // metadata says so, and standing somewhere unreadable is worse than
    // refusing to move.
    if kind != Some(SegmentKind::Error) {
        shell.arrive(target, remember);
    }
}

/// Read a target and print it without moving: a file, a web page, or the next
/// page of what is already on screen.
async fn peek(client: &CairnCmd, shell: &mut Shell, target: &str) {
    if let Some((text, _, _)) = render(client, target).await {
        // The command set belongs to where the cursor stands, not to whatever was
        // just looked at, so the spec survives a peek.
        shell.record_peek(text);
    }
}

/// Issue the read and print the composed text, byte for byte as `cairn read`
/// prints it. `None` when the callback itself failed.
async fn render(
    client: &CairnCmd,
    target: &str,
) -> Option<(
    String,
    Option<cairn_common::read::AffordanceSpec>,
    Option<SegmentKind>,
)> {
    match client.read_envelope(target).await {
        Ok(envelope) => {
            let text = envelope.text;
            println!("{}", text.trim_end());
            let kind = envelope.segments.first().map(|segment| segment.kind);
            let spec = envelope
                .affordances
                .into_iter()
                .find_map(|affordance| affordance.spec);
            Some((text, spec, kind))
        }
        Err(raw) => {
            eprintln!("{}", raw.trim_end());
            None
        }
    }
}

/// Stream the issue's attention until it arrives or the operator interrupts.
///
/// Racing the interrupt is what lets Ctrl-C end the watch and return to the
/// prompt rather than ending the session.
async fn watch(uri: String) {
    tokio::select! {
        _ = run_cli_watch(uri, None) => {}
        _ = tokio::signal::ctrl_c() => println!("\n(watch interrupted)"),
    }
}

/// Send one mutation through the same change path `cairn write` uses, so the
/// shell reports a write exactly the way the CLI does.
async fn mutate(target: String, mode: String, payload: serde_json::Value, confirm: Option<String>) {
    if let Some(question) = confirm {
        if !ask(&question).await {
            println!("cancelled");
            return;
        }
    }
    let input = ChangeInput {
        changes: Some(vec![ChangeItemInput {
            target: Some(target),
            mode: Some(mode),
            payload: Some(payload),
        }]),
        commit_msg: None,
        preview: None,
        atomic: None,
        conflict_markers_reason: None,
    };
    match serde_json::to_string(&input) {
        Ok(json) => {
            run_cli_change(Some(json), None).await;
        }
        Err(e) => eprintln!("cairn: could not encode the change: {e}"),
    }
}

/// Ask before something a keystroke cannot take back.
async fn ask(question: &str) -> bool {
    print!("{question} [y/N] ");
    let _ = std::io::stdout().flush();
    matches!(
        read_line().await.map(|line| line.trim().to_lowercase()),
        Some(answer) if answer == "y" || answer == "yes"
    )
}
