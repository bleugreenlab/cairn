//! `cairn auth login`: sign a provider subscription into the runner.
//!
//! The sign-in state machine lives entirely on the runner (the Claude CLI's
//! pseudo-terminal, pasted-code delivery, `claude auth status` confirmation,
//! account metadata and routing). The desktop's sign-in dialog is one client of
//! it; this verb is another, for an operator with only a shell — typically on
//! a headless box, where the CLI's own attempt to open a browser goes nowhere
//! and the paste-code path is the only way through.

use crate::invoke_client::InvokeClient;
use serde::Deserialize;
use serde_json::json;
use std::future::Future;
use std::io::Write;
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(clap::Subcommand, Clone, Debug)]
pub(crate) enum AuthCommand {
    /// Sign a subscription into a runner-managed provider profile.
    ///
    /// Prints a URL to open on any device; sign in there and paste the code it
    /// shows. Works over SSH and on hosts with no browser. Provider account ids
    /// for `--account` are listed by `cairn read cairn://settings`.
    Login {
        /// Provider to sign in to.
        provider: AuthProvider,
        /// Re-authenticate this existing managed account in place instead of
        /// adding a new one. Its id, profile, and routing position are kept.
        #[arg(long)]
        account: Option<String>,
        /// Make a new account private to this project id instead of shared.
        #[arg(long)]
        project: Option<String>,
    },
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AuthProvider {
    /// A Claude subscription (Pro/Max/Team), through the Claude CLI.
    Claude,
}

/// The runner's account view, reduced to what the report names.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Account {
    id: String,
    label: String,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    plan: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
enum ConnectResponse {
    Connected {
        account: Account,
    },
    #[serde(rename_all = "camelCase")]
    Pending {
        login_id: String,
        #[serde(default)]
        manual_url: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
enum PollResponse {
    Pending,
    Connected { account: Account },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
enum SubmitResponse {
    Connected { account: Account },
    InvalidCode { message: String },
}

/// The four runner commands one sign-in is made of. The desktop dialog drives
/// the same four; this trait exists so the loop below can be exercised against
/// scripted answers.
trait SignInApi {
    async fn connect(
        &self,
        project: Option<&str>,
        account: Option<&str>,
    ) -> Result<ConnectResponse, String>;
    async fn poll(&self, login_id: &str) -> Result<PollResponse, String>;
    async fn submit(&self, login_id: &str, code: &str) -> Result<SubmitResponse, String>;
    async fn cancel(&self, login_id: &str) -> Result<(), String>;
}

impl SignInApi for InvokeClient {
    async fn connect(
        &self,
        project: Option<&str>,
        account: Option<&str>,
    ) -> Result<ConnectResponse, String> {
        self.invoke(
            "connect_provider_oauth",
            json!({ "apiProvider": "anthropic", "projectId": project, "accountId": account }),
        )
        .await
    }

    async fn poll(&self, login_id: &str) -> Result<PollResponse, String> {
        self.invoke("poll_provider_oauth", json!({ "loginId": login_id }))
            .await
    }

    async fn submit(&self, login_id: &str, code: &str) -> Result<SubmitResponse, String> {
        self.invoke(
            "submit_provider_oauth_code",
            json!({ "loginId": login_id, "code": code }),
        )
        .await
    }

    async fn cancel(&self, login_id: &str) -> Result<(), String> {
        self.invoke("cancel_provider_oauth", json!({ "loginId": login_id }))
            .await
    }
}

#[derive(Debug, PartialEq)]
enum SignInOutcome {
    Connected(Account),
    Failed(String),
    /// The operator stopped waiting: Ctrl-C (`interrupted`) or end of input.
    Cancelled {
        interrupted: bool,
    },
}

impl SignInOutcome {
    fn exit_code(&self) -> i32 {
        match self {
            Self::Connected(_) => 0,
            Self::Cancelled { interrupted: true } => 130,
            Self::Failed(_) | Self::Cancelled { interrupted: false } => 1,
        }
    }
}

const PROMPT: &str = "Code: ";

/// Drive one sign-in to its end.
///
/// While waiting for a pasted code this also polls the session, because the
/// runner may settle it without one (the CLI's own browser flow finished, or the
/// session timed out) and an operator staring at `Code: ` must hear about that.
/// Polling stops while a code is being checked: the runner answers that
/// submission itself.
async fn sign_in(
    api: &impl SignInApi,
    request: &LoginRequest,
    mut lines: mpsc::Receiver<String>,
    interrupt: impl Future<Output = ()>,
    poll_interval: Duration,
    out: &mut impl Write,
    err: &mut impl Write,
) -> SignInOutcome {
    tokio::pin!(interrupt);

    let started = tokio::select! {
        started = api.connect(request.project.as_deref(), request.account.as_deref()) => started,
        () = &mut interrupt => return SignInOutcome::Cancelled { interrupted: true },
    };
    let (login_id, manual_url) = match started {
        Err(error) => return SignInOutcome::Failed(error),
        // The profile was already signed in; nothing was asked of anyone.
        Ok(ConnectResponse::Connected { account }) => return SignInOutcome::Connected(account),
        Ok(ConnectResponse::Pending {
            login_id,
            manual_url,
        }) => (login_id, manual_url),
    };
    let Some(manual_url) = manual_url else {
        let _ = api.cancel(&login_id).await;
        return SignInOutcome::Failed(
            "the Claude CLI started signing in but printed no paste-code URL, so there is no way to finish from here. Check the runner log for its output.".into(),
        );
    };

    let _ = writeln!(
        err,
        "Open this URL on any device, sign in, then paste the code it shows:"
    );
    let _ = writeln!(out, "{manual_url}");
    let _ = out.flush();
    let _ = write!(err, "{PROMPT}");
    let _ = err.flush();

    let mut ticker = tokio::time::interval(poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick is immediate; the session was just confirmed pending.
    ticker.tick().await;

    loop {
        let line = tokio::select! {
            line = lines.recv() => line,
            _ = ticker.tick() => {
                match api.poll(&login_id).await {
                    Ok(PollResponse::Pending) => continue,
                    Ok(PollResponse::Connected { account }) => {
                        let _ = writeln!(err);
                        return SignInOutcome::Connected(account);
                    }
                    // The session is over on the runner (expired or failed);
                    // its message says which.
                    Err(error) => {
                        let _ = writeln!(err);
                        return SignInOutcome::Failed(error);
                    }
                }
            }
            () = &mut interrupt => {
                let _ = writeln!(err);
                let _ = api.cancel(&login_id).await;
                return SignInOutcome::Cancelled { interrupted: true };
            }
        };
        let Some(line) = line else {
            let _ = writeln!(err);
            let _ = api.cancel(&login_id).await;
            return SignInOutcome::Cancelled { interrupted: false };
        };
        let code = line.trim();
        if code.is_empty() {
            let _ = write!(err, "{PROMPT}");
            let _ = err.flush();
            continue;
        }

        let submitted = tokio::select! {
            submitted = api.submit(&login_id, code) => submitted,
            () = &mut interrupt => {
                let _ = writeln!(err);
                let _ = api.cancel(&login_id).await;
                return SignInOutcome::Cancelled { interrupted: true };
            }
        };
        match submitted {
            Ok(SubmitResponse::Connected { account }) => return SignInOutcome::Connected(account),
            // The CLI rejected the paste locally and is still prompting: the
            // same session takes another try.
            Ok(SubmitResponse::InvalidCode { message }) => {
                let _ = writeln!(err, "{message}");
                let _ = writeln!(err, "That wasn't a complete code; paste it again.");
                let _ = write!(err, "{PROMPT}");
                let _ = err.flush();
                // Whatever the poll timer owes accrued while the submit held
                // the session; start its interval over.
                ticker.reset();
            }
            // The token exchange itself failed and the runner has already
            // retired the session, so there is nothing left to cancel.
            Err(error) => {
                return SignInOutcome::Failed(format!(
                    "{error}\nSign-in codes are single-use; run `{}` again.",
                    request.command_line()
                ));
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct LoginRequest {
    account: Option<String>,
    project: Option<String>,
}

impl LoginRequest {
    /// The command that would start this same sign-in again.
    fn command_line(&self) -> String {
        let mut command = "cairn auth login claude".to_string();
        if let Some(account) = &self.account {
            command.push_str(&format!(" --account {account}"));
        }
        if let Some(project) = &self.project {
            command.push_str(&format!(" --project {project}"));
        }
        command
    }
}

fn format_connected(account: &Account, request: &LoginRequest) -> String {
    let kind = if request.account.is_some() {
        "re-authenticated"
    } else {
        "new account"
    };
    format!(
        "Signed in to Claude: {}\n  email:   {}\n  plan:    {}\n  account: {} ({kind})\nNew Claude sessions on this runner can now route to this profile.",
        account.label,
        account.email.as_deref().unwrap_or("-"),
        account.plan.as_deref().unwrap_or("-"),
        account.id,
    )
}

/// Stdin lines, read on a blocking thread so the sign-in loop can wait on a
/// line, the poll timer, and Ctrl-C at once. The channel closes at EOF.
fn stdin_lines() -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel(1);
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut line = String::new();
        loop {
            line.clear();
            match stdin.read_line(&mut line) {
                Ok(0) | Err(_) => return,
                Ok(_) => {
                    if tx.blocking_send(line.clone()).is_err() {
                        return;
                    }
                }
            }
        }
    });
    rx
}

/// Run the verb and return the process exit code.
pub(crate) async fn run(command: AuthCommand) -> i32 {
    let AuthCommand::Login {
        provider: AuthProvider::Claude,
        account,
        project,
    } = command;
    let request = LoginRequest { account, project };
    let client = InvokeClient::from_environment();
    let interrupt = async {
        if tokio::signal::ctrl_c().await.is_err() {
            // No signal handler: never report an interrupt that did not happen.
            std::future::pending::<()>().await;
        }
    };
    let outcome = sign_in(
        &client,
        &request,
        stdin_lines(),
        interrupt,
        Duration::from_secs(2),
        &mut std::io::stdout(),
        &mut std::io::stderr(),
    )
    .await;
    match &outcome {
        SignInOutcome::Connected(account) => println!("{}", format_connected(account, &request)),
        SignInOutcome::Failed(error) => eprintln!("cairn auth login: {error}"),
        SignInOutcome::Cancelled { .. } => {
            eprintln!("cairn auth login: cancelled; nothing was saved.")
        }
    }
    outcome.exit_code()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// Scripted runner answers, recording every call made against them.
    #[derive(Default)]
    struct ScriptedApi {
        connect: Mutex<Option<Result<ConnectResponse, String>>>,
        polls: Mutex<VecDeque<Result<PollResponse, String>>>,
        submits: Mutex<VecDeque<Result<SubmitResponse, String>>>,
        calls: Mutex<Vec<String>>,
    }

    impl ScriptedApi {
        fn new(connect: Result<ConnectResponse, String>) -> Self {
            Self {
                connect: Mutex::new(Some(connect)),
                ..Default::default()
            }
        }

        fn submits(self, submits: Vec<Result<SubmitResponse, String>>) -> Self {
            *self.submits.lock().unwrap() = submits.into();
            self
        }

        fn polls(self, polls: Vec<Result<PollResponse, String>>) -> Self {
            *self.polls.lock().unwrap() = polls.into();
            self
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }

        fn record(&self, call: String) {
            self.calls.lock().unwrap().push(call);
        }
    }

    impl SignInApi for ScriptedApi {
        async fn connect(
            &self,
            project: Option<&str>,
            account: Option<&str>,
        ) -> Result<ConnectResponse, String> {
            self.record(format!("connect {project:?} {account:?}"));
            self.connect.lock().unwrap().take().expect("one connect")
        }

        async fn poll(&self, login_id: &str) -> Result<PollResponse, String> {
            self.record(format!("poll {login_id}"));
            self.polls
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(PollResponse::Pending))
        }

        async fn submit(&self, login_id: &str, code: &str) -> Result<SubmitResponse, String> {
            self.record(format!("submit {login_id} {code}"));
            self.submits
                .lock()
                .unwrap()
                .pop_front()
                .expect("a scripted submit answer")
        }

        async fn cancel(&self, login_id: &str) -> Result<(), String> {
            self.record(format!("cancel {login_id}"));
            Ok(())
        }
    }

    fn account(id: &str) -> Account {
        Account {
            id: id.into(),
            label: "op@example.com".into(),
            email: Some("op@example.com".into()),
            plan: Some("max".into()),
        }
    }

    fn pending(manual_url: Option<&str>) -> Result<ConnectResponse, String> {
        Ok(ConnectResponse::Pending {
            login_id: "login-1".into(),
            manual_url: manual_url.map(Into::into),
        })
    }

    const URL: &str = "https://claude.com/cai/oauth/authorize?code=true";

    fn new_account() -> LoginRequest {
        LoginRequest {
            account: None,
            project: None,
        }
    }

    /// Lines the operator types, then EOF unless `hold_open`.
    fn typed(lines: &[&str], hold_open: bool) -> mpsc::Receiver<String> {
        let (tx, rx) = mpsc::channel(lines.len().max(1));
        for line in lines {
            tx.try_send(format!("{line}\n")).unwrap();
        }
        if hold_open {
            std::mem::forget(tx);
        }
        rx
    }

    struct Run {
        outcome: SignInOutcome,
        out: String,
        err: String,
    }

    async fn drive(
        api: &ScriptedApi,
        request: &LoginRequest,
        lines: mpsc::Receiver<String>,
        interrupt: impl Future<Output = ()>,
    ) -> Run {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let outcome = sign_in(
            api,
            request,
            lines,
            interrupt,
            Duration::from_millis(5),
            &mut out,
            &mut err,
        )
        .await;
        Run {
            outcome,
            out: String::from_utf8(out).unwrap(),
            err: String::from_utf8(err).unwrap(),
        }
    }

    #[tokio::test]
    async fn an_already_signed_in_profile_connects_without_a_prompt() {
        let api = ScriptedApi::new(Ok(ConnectResponse::Connected {
            account: account("acc_1"),
        }));
        let run = drive(
            &api,
            &new_account(),
            typed(&[], true),
            std::future::pending(),
        )
        .await;
        assert_eq!(run.outcome, SignInOutcome::Connected(account("acc_1")));
        assert_eq!(run.outcome.exit_code(), 0);
        assert!(run.out.is_empty(), "{}", run.out);
        assert_eq!(api.calls(), ["connect None None"]);
    }

    #[tokio::test]
    async fn an_incomplete_code_reprompts_under_the_same_session() {
        let api = ScriptedApi::new(pending(Some(URL))).submits(vec![
            Ok(SubmitResponse::InvalidCode {
                message: "Invalid code. Please make sure the full code was copied.".into(),
            }),
            Ok(SubmitResponse::Connected {
                account: account("acc_1"),
            }),
        ]);
        let run = drive(
            &api,
            &new_account(),
            typed(&["garbage", "", "good#state"], true),
            std::future::pending(),
        )
        .await;

        assert_eq!(run.outcome, SignInOutcome::Connected(account("acc_1")));
        // The URL alone is on stdout so it copies cleanly.
        assert_eq!(run.out, format!("{URL}\n"));
        assert!(run.err.contains("Invalid code."), "{}", run.err);
        assert!(run.err.contains("paste it again"), "{}", run.err);
        let submits: Vec<_> = api
            .calls()
            .into_iter()
            .filter(|call| call.starts_with("submit"))
            .collect();
        assert_eq!(
            submits,
            ["submit login-1 garbage", "submit login-1 good#state"],
            "the empty line is re-prompted locally, never submitted"
        );
        assert!(!api.calls().iter().any(|call| call.starts_with("cancel")));
    }

    #[tokio::test]
    async fn a_failed_exchange_reports_the_runner_and_does_not_cancel_a_dead_session() {
        let api = ScriptedApi::new(pending(Some(URL))).submits(vec![Err(
            "Login failed: Request failed with status code 400".into(),
        )]);
        let request = LoginRequest {
            account: Some("acc_9".into()),
            project: None,
        };
        let run = drive(
            &api,
            &request,
            typed(&["spent#code"], true),
            std::future::pending(),
        )
        .await;

        let SignInOutcome::Failed(message) = &run.outcome else {
            panic!("expected failure, got {:?}", run.outcome);
        };
        assert!(
            message.starts_with("Login failed: Request failed with status code 400\n"),
            "{message}"
        );
        assert!(
            message.contains("single-use; run `cairn auth login claude --account acc_9` again"),
            "{message}"
        );
        assert_eq!(run.outcome.exit_code(), 1);
        assert!(!api.calls().iter().any(|call| call.starts_with("cancel")));
    }

    #[tokio::test]
    async fn a_session_without_a_paste_url_is_cancelled() {
        let api = ScriptedApi::new(pending(None));
        let run = drive(
            &api,
            &new_account(),
            typed(&[], true),
            std::future::pending(),
        )
        .await;
        assert!(
            matches!(&run.outcome, SignInOutcome::Failed(message) if message.contains("no paste-code URL")),
            "{:?}",
            run.outcome
        );
        assert_eq!(api.calls(), ["connect None None", "cancel login-1"]);
    }

    #[tokio::test]
    async fn end_of_input_cancels_the_session() {
        let api = ScriptedApi::new(pending(Some(URL)));
        let run = drive(
            &api,
            &new_account(),
            typed(&[], false),
            std::future::pending(),
        )
        .await;
        assert_eq!(run.outcome, SignInOutcome::Cancelled { interrupted: false });
        assert_eq!(run.outcome.exit_code(), 1);
        assert_eq!(
            api.calls().last().map(String::as_str),
            Some("cancel login-1")
        );
    }

    #[tokio::test]
    async fn ctrl_c_cancels_the_session() {
        let api = ScriptedApi::new(pending(Some(URL)));
        let run = drive(
            &api,
            &new_account(),
            typed(&[], true),
            tokio::time::sleep(Duration::from_millis(20)),
        )
        .await;
        assert_eq!(run.outcome, SignInOutcome::Cancelled { interrupted: true });
        assert_eq!(run.outcome.exit_code(), 130);
        assert_eq!(
            api.calls().last().map(String::as_str),
            Some("cancel login-1")
        );
    }

    #[tokio::test]
    async fn a_session_that_settles_by_itself_ends_the_wait() {
        let api = ScriptedApi::new(pending(Some(URL))).polls(vec![
            Ok(PollResponse::Pending),
            Ok(PollResponse::Connected {
                account: account("acc_1"),
            }),
        ]);
        let run = drive(
            &api,
            &new_account(),
            typed(&[], true),
            std::future::pending(),
        )
        .await;
        assert_eq!(run.outcome, SignInOutcome::Connected(account("acc_1")));
        assert!(!api.calls().iter().any(|call| call.starts_with("submit")));
    }

    #[tokio::test]
    async fn an_expired_session_reports_the_runners_reason() {
        let api = ScriptedApi::new(pending(Some(URL))).polls(vec![Err(
            "Claude sign-in timed out before it finished.".into(),
        )]);
        let run = drive(
            &api,
            &new_account(),
            typed(&[], true),
            std::future::pending(),
        )
        .await;
        assert_eq!(
            run.outcome,
            SignInOutcome::Failed("Claude sign-in timed out before it finished.".into())
        );
    }

    #[test]
    fn connect_parses_the_runner_contract() {
        let pending: ConnectResponse = serde_json::from_value(json!({
            "status": "pending", "loginId": "abc", "manualUrl": URL,
        }))
        .unwrap();
        assert!(matches!(
            pending,
            ConnectResponse::Pending { login_id, manual_url: Some(url) } if login_id == "abc" && url == URL
        ));
        let invalid: SubmitResponse =
            serde_json::from_value(json!({ "status": "invalidCode", "message": "Invalid code." }))
                .unwrap();
        assert!(
            matches!(invalid, SubmitResponse::InvalidCode { message } if message == "Invalid code.")
        );
        let connected: PollResponse = serde_json::from_value(json!({
            "status": "connected",
            "account": {"id": "acc_1", "label": "op@example.com", "apiProvider": "anthropic",
                        "source": "configured", "authType": "claude_profile",
                        "email": "op@example.com", "plan": "max"}
        }))
        .unwrap();
        assert!(
            matches!(connected, PollResponse::Connected { account: a } if a == account("acc_1"))
        );
    }

    #[test]
    fn the_report_names_the_account_and_whether_it_was_new() {
        let report = format_connected(&account("acc_1"), &new_account());
        assert!(report.contains("account: acc_1 (new account)"), "{report}");
        assert!(report.contains("plan:    max"), "{report}");
        assert!(report.contains("can now route to this profile"), "{report}");
        let reauth = LoginRequest {
            account: Some("acc_1".into()),
            project: None,
        };
        assert!(format_connected(&account("acc_1"), &reauth).contains("(re-authenticated)"));
    }
}
