//! The operator's approval of a generation's draft, before verification runs
//! it.
//!
//! Verifying a draft runs its runnable surface ([`runnable_surface`]) on the
//! pool's machines, so the operator approves exactly that surface first. The
//! question asking them is the server's, never the agent's: [`request_approval`]
//! renders it from the draft as the inventory stores it, with the surface in
//! full and its sha256, raises it as an ordinary question on the generation's
//! own run, and binds that prompt and run to the generation's current draft
//! revision in the private `inference_generations` row. The agent can ask for
//! it to be raised (`patch cairn://inference/generations/{id} {approve:{}}`)
//! and nothing more: no text, URI or answer it supplies reaches the question
//! or the decision. Neither Approve nor Reject is recommended, so the form
//! preselects neither. Asking again returns the same pending question, and
//! once it is answered, records the answer.
//!
//! Only the operator approves. The answer is read from the canonical answer
//! ledger in the private database, whose first claim names the transport it
//! arrived through, and counts only when that is an authenticated operator
//! transport (the shared [`is_operator_transport`] predicate). An answer an agent gives through the
//! question's resource (the generating agent's own, an ancestor's, any
//! other's) is refused even though ordinary question authority lets it
//! answer, as is a channel reply, a remote intent or an unauthenticated call.
//!
//! An approval holds for one draft revision and one surface digest
//! ([`approval_holds`]). A new revision, a changed surface (a newer surface
//! schema included), a stored question that no longer matches what the server
//! would render, or a withdrawn question all need a freshly minted question;
//! the superseded one, if still pending, is withdrawn. Nothing compares a
//! digest inside question text or resolves a question by its `q-N` suffix:
//! the bound prompt id is the question's identity.

use cairn_common::identity::AppearanceTransport;
use cairn_db::turso::params;
use cairn_inference::{InferenceProfileId, Inventory, OperatorAnswer};
use serde_json::Value;

use super::{get_generation, runnable_surface, select_generations, surface_digest, GenerationRow};
use crate::channels::ledger::{self, AskResolution};
use crate::mcp::handlers::permission::{answerer_label, is_operator_transport, WithdrawalCause};
use crate::mcp::types::{AskUserPayload, McpCallbackRequest};
use crate::models::question::{PromptAnswers, Question, QuestionOption};
use crate::orchestrator::inference::load_inventory;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// The option that approves a draft's runnable surface.
pub const APPROVE: &str = "Approve";
/// The option that keeps it off the machines.
pub const REJECT: &str = "Reject";
/// The approval question's header, which keeps it out of the generation's
/// preference answers.
const HEADER: &str = "Approve draft";

/// A generation's current draft, as an approval names it.
#[derive(Debug, Clone, PartialEq)]
pub struct Draft {
    pub profile: String,
    pub pool: String,
    pub revision: i64,
    pub surface: Value,
    pub digest: String,
}

/// The draft `generation` would have approved, from the inventory as it
/// stands.
pub fn current_draft(generation: &GenerationRow, inventory: &Inventory) -> Result<Draft, String> {
    let profile = generation
        .draft_profile
        .as_deref()
        .ok_or_else(|| format!("generation {} has no draft to approve", generation.id))?;
    let surface = inventory
        .profiles
        .get(&InferenceProfileId::from(profile))
        .and_then(runnable_surface)
        .ok_or_else(|| {
            format!(
                "the draft {profile} is no longer a container or native profile in the inventory"
            )
        })?;
    let digest = surface_digest(&surface);
    Ok(Draft {
        profile: profile.to_string(),
        pool: generation.pool.clone(),
        revision: generation.draft_revision,
        surface,
        digest,
    })
}

/// The one approval question for `draft`: its full runnable surface and
/// sha256, Approve or Reject, neither recommended.
pub fn approval_question(generation: &GenerationRow, draft: &Draft) -> Question {
    let surface =
        serde_json::to_string_pretty(&draft.surface).unwrap_or_else(|_| draft.surface.to_string());
    Question {
        question: format!(
            "Generation {id} asks to verify its draft profile {profile} (revision {revision}) of {model} on pool {pool}. Verifying runs exactly this runnable surface on the pool's machines.\n\nsha256 {digest}\n\n```json\n{surface}\n```\n\n{APPROVE} lets verification run this surface as it stands; any change to it needs approving again. {REJECT} keeps it off the machines.",
            id = generation.id,
            profile = draft.profile,
            revision = draft.revision,
            model = generation.model,
            pool = draft.pool,
            digest = draft.digest,
        ),
        header: Some(HEADER.to_string()),
        options: vec![
            QuestionOption {
                label: APPROVE.to_string(),
                description: format!(
                    "Verify exactly this surface (sha256 {}) on {}",
                    draft.digest, draft.pool
                ),
                recommended: false,
            },
            QuestionOption {
                label: REJECT.to_string(),
                description: "Do not run it; the agent revises the draft or ends the generation"
                    .to_string(),
                recommended: false,
            },
        ],
        multi_select: false,
    }
}

/// Whether `generation` holds the operator's approval of a draft whose
/// runnable surface has sha256 `digest`: approved for this very revision and
/// this very surface.
pub fn approval_holds(generation: &GenerationRow, digest: &str) -> bool {
    generation.approved_surface.as_deref() == Some(digest)
        && generation.approval_revision == Some(generation.draft_revision)
}

/// Why `profile` may not be verified yet: it is an open generation's draft
/// whose runnable surface the operator has not approved as it stands.
pub async fn unapproved_draft(db: &LocalDb, profile: &str) -> Result<Option<String>, String> {
    let open = select_generations(
        db,
        "WHERE draft_profile = ?1 AND state = 'running'",
        Some(profile.to_string()),
    )
    .await?;
    let Some(generation) = open.into_iter().next() else {
        return Ok(None);
    };
    let inventory = load_inventory(db).await?;
    let holds = current_draft(&generation, &inventory)
        .is_ok_and(|draft| approval_holds(&generation, &draft.digest));
    Ok((!holds).then(|| {
        format!(
            "profile {profile} is generation {id}'s draft, and the operator has not approved what it runs as it stands; its agent asks with patch cairn://inference/generations/{id} {{approve:{{}}}}",
            id = generation.id
        )
    }))
}

/// Refuse an approval request that is anything but `{approve:{}}` alone.
pub fn approve_payload(payload: &Value) -> Result<(), String> {
    let object = payload
        .as_object()
        .ok_or("payload is an object: {approve:{}}")?;
    if object.len() != 1 {
        return Err("approve is an operation of its own: send {approve:{}} alone, and an outcome in a patch of its own".into());
    }
    match object.get("approve") {
        Some(Value::Object(inner)) if inner.is_empty() => Ok(()),
        _ => Err("payload.approve is {}: Cairn raises the approval question itself from the draft as stored, so it takes no question, text or URI".into()),
    }
}

/// Who gave the answer the ledger recorded first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answerer {
    /// An authenticated operator transport: the only answer that approves.
    Operator(AppearanceTransport),
    /// Anyone else, in words that name them.
    Other(String),
}

impl Answerer {
    pub fn of(winner: &AskResolution) -> Self {
        let present = |value: &str| (!value.is_empty()).then(|| value.to_string());
        let actor = present(&winner.winner_actor);
        let provider = present(&winner.winner_provider);
        match serde_json::from_value::<AppearanceTransport>(Value::String(
            winner.winner_surface.clone(),
        )) {
            Ok(transport) if is_operator_transport(transport) => Self::Operator(transport),
            Ok(transport) => Self::Other(answerer_label(
                transport,
                actor.as_deref(),
                provider.as_deref(),
            )),
            Err(_) => Self::Other(format!(
                "an answerer recorded as '{}'",
                winner.winner_surface
            )),
        }
    }
}

/// What an approval request found or did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalOutcome {
    /// A fresh canonical question was raised, at `question`, because of `why`.
    Asked { question: String, why: String },
    /// The question at `question` waits on the operator.
    Pending { question: String },
    /// The operator approved the draft as it stands.
    Approved,
    /// The operator answered `answer`, not Approve.
    Rejected { answer: String },
    /// The operator answered `answer`, not Approve, to the last candidate the
    /// generation may submit, which ended it exhausted.
    Exhausted { answer: String },
}

impl ApprovalOutcome {
    /// What the requesting agent reads, and the request's structured result.
    pub fn report(&self, generation: &str, draft: &Draft) -> (String, Value) {
        let named = format!(
            "generation {generation}'s draft {} (revision {}, surface sha256 {})",
            draft.profile, draft.revision, draft.digest
        );
        let (said, state, question) = match self {
            Self::Asked { question, why } => (
                format!("Raised the operator's approval question for {named} at {question}: {why}. Only the operator's answer approves it; keep working, and patch again with {{approve:{{}}}} once they have answered."),
                "asked",
                Some(question),
            ),
            Self::Pending { question } => (
                format!("The operator has not yet answered the approval question for {named} at {question}; patch again with {{approve:{{}}}} once they have."),
                "pending",
                Some(question),
            ),
            Self::Approved => (
                format!("The operator approved {named} as it stands; verify it now: patch cairn://inference/profiles/{} {{verify:{{pool:\"{}\"}}}}.", draft.profile, draft.pool),
                "approved",
                None,
            ),
            Self::Rejected { answer } => (
                format!("The operator answered {answer:?}, not {APPROVE}, to the approval of {named}; it will not be verified. Revise the draft (a new revision is asked about afresh) or end the generation."),
                "rejected",
                None,
            ),
            Self::Exhausted { answer } => (
                format!("The operator answered {answer:?}, not {APPROVE}, to the approval of {named}, the last candidate this generation may submit; it has ended exhausted and its draft is removed."),
                "exhausted",
                None,
            ),
        };
        let mut data = serde_json::json!({
            "generation": generation,
            "approval": state,
            "profile": draft.profile,
            "revision": draft.revision,
            "surfaceDigest": draft.digest,
        });
        if let Some(question) = question {
            data["question"] = Value::String(question.clone());
        }
        (said, data)
    }
}

/// Raise, return or record the operator's approval of generation `id`'s
/// current draft, for its own agent (`caller_job`, whose home is
/// `caller_home`) on the authenticated `request`.
pub(crate) async fn request_approval(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    caller_home: &str,
    caller_job: &str,
    id: &str,
) -> Result<(ApprovalOutcome, Draft), String> {
    let generation = get_generation(&orch.db.local, id)
        .await?
        .ok_or_else(|| format!("there is no generation {id}"))?;
    if generation.job_id != caller_job {
        return Err(
            "only the generation's own agent requests the operator's approval of its draft".into(),
        );
    }
    if !generation.is_open() {
        return Err(format!(
            "generation {id} has already ended {}",
            generation.state
        ));
    }
    let draft = current_draft(&generation, &load_inventory(&orch.db.local).await?)?;
    let owning = crate::execution::routing::owning_db_for_job(&orch.db, &generation.job_id)
        .await
        .map_err(|error| error.to_string())?;
    let question_uri = |segment: &str| format!("{caller_home}/questions/{segment}");
    let outcome = match standing(&orch.db.local, &owning, &generation, &draft).await? {
        Standing::Pending { segment } => ApprovalOutcome::Pending {
            question: question_uri(&segment),
        },
        Standing::Approved => {
            record(&orch.db.local, &generation, &draft).await?;
            ApprovalOutcome::Approved
        }
        // The rejected candidate's attempt is spent; on the last one there is
        // nothing left to submit, so the generation ends now rather than when
        // its agent next writes or exits. `standing` has just rechecked the
        // bound prompt, revision, surface and ledger winner.
        Standing::Rejected { answer } if generation.attempts >= LAST_ATTEMPT => {
            exhaust(orch, &generation, &draft, &answer).await?;
            ApprovalOutcome::Exhausted { answer }
        }
        Standing::Rejected { answer } => ApprovalOutcome::Rejected { answer },
        Standing::Unasked { why, stale } => {
            let segment = mint(orch, request, &owning, &generation, &draft).await?;
            if let Some(stale) = stale {
                withdraw(orch, &owning, &stale).await;
            }
            ApprovalOutcome::Asked {
                question: question_uri(&segment),
                why,
            }
        }
        Standing::NotOperator { answered_by } => {
            let segment = mint(orch, request, &owning, &generation, &draft).await?;
            ApprovalOutcome::Asked {
                question: question_uri(&segment),
                why: format!("{answered_by} answered the last one, and only the operator's answer approves a draft"),
            }
        }
    };
    Ok((outcome, draft))
}

/// Withdraw `generation`'s bound approval question if it is still pending,
/// for a draft about to be replaced: what it asked about is going away.
pub(crate) async fn withdraw_approval_question(orch: &Orchestrator, generation: &GenerationRow) {
    let Some(prompt_id) = generation.approval_prompt_id.as_deref() else {
        return;
    };
    match crate::execution::routing::owning_db_for_job(&orch.db, &generation.job_id).await {
        Ok(owning) => withdraw(orch, &owning, prompt_id).await,
        Err(error) => log::warn!(
            "inference: generation {}'s approval question cannot be withdrawn: {error}",
            generation.id
        ),
    }
}

/// The attempt that is a generation's last candidate (the generated door
/// reserves at most three).
const LAST_ATTEMPT: i64 = 3;

/// End `generation` exhausted: the operator rejected its last candidate.
async fn exhaust(
    orch: &Orchestrator,
    generation: &GenerationRow,
    draft: &Draft,
    answer: &str,
) -> Result<(), String> {
    let outcome = serde_json::json!({
        "outcome": super::lifecycle::EXHAUSTED,
        "why": format!(
            "the operator answered {answer:?}, not {APPROVE}, to its third and last candidate {} (revision {})",
            draft.profile, draft.revision
        ),
    });
    super::lifecycle::end(
        orch,
        &generation.id,
        super::lifecycle::EXHAUSTED,
        Some(&outcome),
    )
    .await
    .map(|_| ())
}

/// After any question is answered: if it is the bound approval question of
/// an open generation's last candidate and the operator rejected it, end the
/// generation now, without waiting for its agent to ask again. The same
/// checks [`request_approval`] makes decide (bound prompt, revision, surface,
/// ledger winner), so an old or tampered question ends nothing.
pub(crate) async fn approval_answered(orch: &Orchestrator, prompt_id: &str) {
    let open = match select_generations(
        &orch.db.local,
        "WHERE approval_prompt_id = ?1 AND state = 'running'",
        Some(prompt_id.to_string()),
    )
    .await
    {
        Ok(open) => open,
        Err(error) => {
            log::warn!(
                "inference: reading the generation question {prompt_id} approves failed: {error}"
            );
            return;
        }
    };
    for generation in open
        .into_iter()
        .filter(|generation| generation.attempts >= LAST_ATTEMPT)
    {
        let settled = async {
            let draft = current_draft(&generation, &load_inventory(&orch.db.local).await?)?;
            let owning = crate::execution::routing::owning_db_for_job(&orch.db, &generation.job_id)
                .await
                .map_err(|error| error.to_string())?;
            if let Standing::Rejected { answer } =
                standing(&orch.db.local, &owning, &generation, &draft).await?
            {
                exhaust(orch, &generation, &draft, &answer).await?;
            }
            Ok::<(), String>(())
        }
        .await;
        if let Err(error) = settled {
            log::warn!(
                "inference: generation {} could not settle its approval answer: {error}",
                generation.id
            );
        }
    }
}

/// A `generated` payload as the door passes it on: its `answers` replaced
/// by the operator's own answers to the caller's open generation
/// ([`operator_answers`]), whatever the agent submitted.
pub(crate) async fn stamp_operator_answers(
    orch: &Orchestrator,
    caller_job: &str,
    generated: &Value,
) -> Result<Value, String> {
    let mut stamped = generated.clone();
    let Some(object) = stamped.as_object_mut() else {
        return Ok(stamped);
    };
    let Some(generation) = super::open_generation_for_job(&orch.db.local, caller_job).await? else {
        // The door refuses a caller with no open generation by name.
        return Ok(stamped);
    };
    let answers = operator_answers(orch, &generation).await?;
    object.insert(
        "answers".to_string(),
        serde_json::to_value(answers).map_err(|error| error.to_string())?,
    );
    Ok(stamped)
}

/// After generation `id` accepted a new draft: withdraw the approval question
/// it still points at, which asked about a revision that is gone. A refused
/// candidate leaves the row and its question as they were, so this runs only
/// after a successful write.
pub(crate) async fn draft_replaced(orch: &Orchestrator, id: &str) {
    match get_generation(&orch.db.local, id).await {
        Ok(Some(generation)) if generation.approval_revision != Some(generation.draft_revision) => {
            withdraw_approval_question(orch, &generation).await
        }
        Ok(_) => {}
        Err(error) => log::warn!(
            "inference: generation {id} cannot be read after its draft was replaced: {error}"
        ),
    }
}

/// The operator's answers to the ordinary questions `generation`'s agent
/// asked, in the order it asked them: what a generated draft's origin stamps
/// as its answers. Only an answer the private ledger records as the
/// operator's counts, so an agent's answer, or one the agent quotes, never
/// does; approval questions are not preferences and are left out.
pub(crate) async fn operator_answers(
    orch: &Orchestrator,
    generation: &GenerationRow,
) -> Result<Vec<OperatorAnswer>, String> {
    let owning = crate::execution::routing::owning_db_for_job(&orch.db, &generation.job_id)
        .await
        .map_err(|error| error.to_string())?;
    let job_id = generation.job_id.clone();
    let prompts = owning
        .read(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT p.id, p.questions FROM prompts p JOIN runs r ON r.id = p.run_id \
                         WHERE r.job_id = ?1 AND p.response IS NOT NULL AND p.withdrawn_at IS NULL \
                         ORDER BY p.created_at, p.rowid",
                        (job_id.as_str(),),
                    )
                    .await?;
                let mut prompts = Vec::new();
                while let Some(row) = rows.next().await? {
                    prompts.push((row.text(0)?, row.text(1)?));
                }
                Ok(prompts)
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    let mut answers = Vec::new();
    for (prompt_id, questions) in prompts {
        let Ok(questions) = serde_json::from_str::<Vec<Question>>(&questions) else {
            continue;
        };
        if questions
            .iter()
            .any(|question| question.header.as_deref() == Some(HEADER))
        {
            continue;
        }
        let action = cairn_uri::canonicalize_uri_identity(&prompt_id);
        let Some(winner) = ledger::resolution_for_action(&orch.db.local, &action)
            .await?
            .filter(|winner| {
                winner.kind == "question"
                    && winner.action_ref == action
                    && winner.binding_ref == action
            })
        else {
            continue;
        };
        if !matches!(Answerer::of(&winner), Answerer::Operator(_)) {
            continue;
        }
        let Ok(given) = PromptAnswers::decode_stored(&winner.answer).into_answers(questions.len())
        else {
            continue;
        };
        for answer in given.answers() {
            if let Some(question) = questions.get(answer.index) {
                answers.push(OperatorAnswer {
                    question: question.question.clone(),
                    answer: answer.answer.clone(),
                });
            }
        }
    }
    Ok(answers)
}

/// Where the bound approval question of `generation`'s current draft stands.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Standing {
    /// No question stands for the draft as it is, because of `why`; `stale`
    /// is a superseded one still pending, to withdraw.
    Unasked {
        why: String,
        stale: Option<String>,
    },
    Pending {
        segment: String,
    },
    Approved,
    Rejected {
        answer: String,
    },
    /// Answered, but not by the operator.
    NotOperator {
        answered_by: String,
    },
}

/// A prompt as its owning database stores it.
struct StoredPrompt {
    questions: String,
    job_id: Option<String>,
    run_job_id: Option<String>,
    run_id: String,
    segment: Option<String>,
    withdrawn: bool,
    answered: bool,
}

async fn stored_prompt(db: &LocalDb, prompt_id: &str) -> Result<Option<StoredPrompt>, String> {
    db.query_opt(
        "SELECT p.questions, p.job_id, r.job_id, p.run_id, p.uri_segment, \
                p.withdrawn_at IS NOT NULL, p.response IS NOT NULL \
         FROM prompts p JOIN runs r ON r.id = p.run_id WHERE p.id = ?1",
        (prompt_id.to_string(),),
        |row| {
            Ok(StoredPrompt {
                questions: row.text(0)?,
                job_id: row.opt_text(1)?,
                run_job_id: row.opt_text(2)?,
                run_id: row.text(3)?,
                segment: row.opt_text(4)?,
                withdrawn: row.i64(5)? != 0,
                answered: row.i64(6)? != 0,
            })
        },
    )
    .await
    .map_err(|error| error.to_string())
}

/// The option the ledger's winning answer chose.
fn chosen(winner: &AskResolution) -> Option<String> {
    PromptAnswers::decode_stored(&winner.answer)
        .into_answers(1)
        .ok()?
        .answers()
        .first()
        .map(|answer| answer.answer.clone())
}

/// Judge the bound question from the generation's run (`owning`, the database
/// its prompts live in) and the private answer ledger (`private`).
async fn standing(
    private: &LocalDb,
    owning: &LocalDb,
    generation: &GenerationRow,
    draft: &Draft,
) -> Result<Standing, String> {
    let unasked = |why: &str, stale: Option<&str>| Standing::Unasked {
        why: why.to_string(),
        stale: stale.map(str::to_string),
    };
    let Some(prompt_id) = generation.approval_prompt_id.as_deref() else {
        return Ok(unasked(
            "no approval question had been raised for this draft",
            None,
        ));
    };
    let Some(prompt) = stored_prompt(owning, prompt_id).await? else {
        return Ok(unasked("its approval question no longer exists", None));
    };
    let pending = (!prompt.answered && !prompt.withdrawn).then_some(prompt_id);
    if generation.approval_revision != Some(draft.revision) {
        return Ok(unasked(
            &format!(
                "the draft is now revision {}, and the last approval question asked about another",
                draft.revision
            ),
            pending,
        ));
    }
    if generation.approval_run_id.as_deref() != Some(prompt.run_id.as_str())
        || prompt.run_job_id.as_deref() != Some(generation.job_id.as_str())
        || prompt
            .job_id
            .as_deref()
            .is_some_and(|job| job != generation.job_id)
    {
        return Ok(unasked(
            "its approval question is not the one raised on this generation's run",
            None,
        ));
    }
    let canonical = serde_json::to_value(vec![approval_question(generation, draft)])
        .map_err(|error| error.to_string())?;
    let stored = serde_json::from_str::<Value>(&prompt.questions).unwrap_or(Value::Null);
    if stored != canonical {
        return Ok(unasked(
            "the last approval question no longer shows the draft's runnable surface as it stands",
            pending,
        ));
    }
    if prompt.withdrawn {
        return Ok(unasked("the last approval question was withdrawn", None));
    }
    let action = cairn_uri::canonicalize_uri_identity(prompt_id);
    let winner = ledger::resolution_for_action(private, &action)
        .await?
        .filter(|winner| {
            winner.kind == "question" && winner.action_ref == action && winner.binding_ref == action
        });
    let Some(winner) = winner else {
        return Ok(if prompt.answered {
            Standing::NotOperator {
                answered_by: "an answer with no record in the answer ledger of who gave it"
                    .to_string(),
            }
        } else {
            Standing::Pending {
                segment: prompt.segment.unwrap_or_else(|| prompt_id.to_string()),
            }
        });
    };
    Ok(match Answerer::of(&winner) {
        Answerer::Other(answered_by) => Standing::NotOperator { answered_by },
        Answerer::Operator(_) => match chosen(&winner) {
            Some(answer) if answer == APPROVE => Standing::Approved,
            Some(answer) => Standing::Rejected { answer },
            None => Standing::NotOperator {
                answered_by: "an answer the ledger cannot read".to_string(),
            },
        },
    })
}

/// Raise the canonical question on the caller's run and bind it to the
/// draft's revision. Answers the new question's segment.
async fn mint(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    owning: &LocalDb,
    generation: &GenerationRow,
    draft: &Draft,
) -> Result<String, String> {
    let asked = crate::mcp::handlers::planning::ask_questions(
        orch,
        request,
        AskUserPayload {
            questions: vec![approval_question(generation, draft)],
        },
        true,
        None,
    )
    .await;
    let prompt_id = asked
        .prompt_id
        .clone()
        .ok_or_else(|| format!("the approval question could not be raised: {}", asked.text))?;
    let prompt = stored_prompt(owning, &prompt_id)
        .await?
        .ok_or("the approval question it raised is missing")?;
    if prompt.run_job_id.as_deref() != Some(generation.job_id.as_str()) {
        withdraw(orch, owning, &prompt_id).await;
        return Err("the approval question was raised on another job's run".into());
    }
    if !bind(
        &orch.db.local,
        generation,
        draft,
        &prompt_id,
        &prompt.run_id,
    )
    .await?
    {
        withdraw(orch, owning, &prompt_id).await;
        return Err(
            "the draft or its approval question changed while this one was raised; request approval again"
                .into(),
        );
    }
    Ok(prompt.segment.unwrap_or(prompt_id))
}

/// Bind `prompt_id` on `run_id` as the approval question of `draft`'s
/// revision, clearing any earlier approval, only while the row still holds
/// the draft and the binding `generation` was read with.
async fn bind(
    db: &LocalDb,
    generation: &GenerationRow,
    draft: &Draft,
    prompt_id: &str,
    run_id: &str,
) -> Result<bool, String> {
    let previous = generation.approval_prompt_id.clone().unwrap_or_default();
    db.execute(
        "UPDATE inference_generations \
         SET approval_prompt_id = ?2, approval_run_id = ?3, approval_revision = ?4, approved_surface = NULL \
         WHERE id = ?1 AND state = 'running' AND draft_revision = ?4 AND draft_profile = ?5 \
           AND COALESCE(approval_prompt_id, '') = ?6",
        params![
            generation.id.as_str(),
            prompt_id,
            run_id,
            draft.revision,
            draft.profile.as_str(),
            previous.as_str()
        ],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

/// Record the operator's approval of `draft`'s surface, only while the row
/// still holds that revision and its bound question.
async fn record(db: &LocalDb, generation: &GenerationRow, draft: &Draft) -> Result<(), String> {
    let prompt_id = generation.approval_prompt_id.clone().unwrap_or_default();
    let changed = db
        .execute(
            "UPDATE inference_generations SET approved_surface = ?2 \
             WHERE id = ?1 AND state = 'running' AND draft_revision = ?3 AND approval_revision = ?3 \
               AND approval_prompt_id = ?4 AND draft_profile = ?5",
            params![
                generation.id.as_str(),
                draft.digest.as_str(),
                draft.revision,
                prompt_id.as_str(),
                draft.profile.as_str()
            ],
        )
        .await
        .map_err(|error| error.to_string())?;
    if changed == 0 {
        return Err(
            "the draft changed while its approval was being recorded; request approval again"
                .into(),
        );
    }
    Ok(())
}

/// Withdraw a superseded approval question that is still pending.
async fn withdraw(orch: &Orchestrator, owning: &LocalDb, prompt_id: &str) {
    let now = chrono::Utc::now().timestamp();
    let id = prompt_id.to_string();
    let result = owning
        .write(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                crate::mcp::handlers::planning::withdraw_prompt_conn(
                    conn,
                    &id,
                    WithdrawalCause::Superseded,
                    now,
                )
                .await
            })
        })
        .await;
    match result {
        Ok(true) => {
            let change = crate::notify::prompt_db_change_for_id(owning, prompt_id, "update").await;
            let _ = orch.services.emitter.emit("db-change", change);
        }
        Ok(false) => {}
        Err(error) => {
            log::warn!("inference: withdrawing a superseded approval question failed: {error}")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cairn_inference::InferenceProfile;

    use super::super::{insert_generation, RUNNING};
    use super::*;
    use crate::mcp::handlers::planning::{answer_prompt_payload, ask_questions};
    use crate::orchestrator::inference::{write_inventory, Authoring, Removals};
    use crate::orchestrator::inference_models::tests::FakeHub;
    use crate::orchestrator::inference_models::{register, HUGGING_FACE};
    use crate::turns::queries::ResolutionProvenance;

    const HOME: &str = "cairn://p/prj/7/1/generator";
    const ANCESTOR: &str = "cairn://p/prj/7/1/builder";
    const SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn profile_yaml(args: &str) -> String {
        profile_yaml_for("a", args)
    }

    fn profile_yaml_for(id: &str, args: &str) -> String {
        format!(
            r#"
id: {id}
model:
  name: Qwen/Qwen3-Embedding-4B
  weights:
    - {{repo: Qwen/Qwen3-Embedding-4B-GGUF, revision: abc, path: Qwen3-Embedding-4B-Q8_0.gguf, sha256: {SHA}}}
servedName: Qwen/Qwen3-Embedding-4B
engine: {{kind: container, image: "ghcr.io/ggml-org/llama.cpp@sha256:{SHA}", server: llamaCpp, args: [{args}]}}
requirements:
  machines: 1
  memory: {{kind: discrete, vram: 8000000000, host: 0}}
"#
        )
    }

    async fn put_profile(db: &LocalDb, args: &str) {
        let profile: InferenceProfile = serde_yaml::from_str(&profile_yaml(args)).unwrap();
        let mut records = Inventory::default();
        records.insert_profile(profile);
        write_inventory(
            db,
            &Authoring::Patch {
                upsert: Some(records.to_yaml()),
                remove: Removals::default(),
            },
        )
        .await
        .unwrap();
    }

    fn generation_row() -> GenerationRow {
        GenerationRow {
            id: "g1".into(),
            project_id: "p".into(),
            issue_id: Some("i".into()),
            job_id: "j-gen".into(),
            mode: "agent".into(),
            execution_id: Some("e".into()),
            attempts: 0,
            draft_revision: 0,
            draft_digest: None,
            approval_prompt_id: None,
            approval_run_id: None,
            approval_revision: None,
            verification_id: None,
            cleanup_pending: false,
            node_uri: HOME.into(),
            model: "Qwen/Qwen3-Embedding-4B".into(),
            pool: "bbs".into(),
            state: RUNNING.into(),
            draft_profile: None,
            draft_written_at: None,
            approved_surface: None,
            hardware: BTreeMap::new(),
            outcome: None,
            started_at: 1,
            finished_at: None,
        }
    }

    /// An orchestrator whose issue 7 has the generation's agent `generator`
    /// (job `j-gen`, run `r-gen`) and its ancestor `builder`, and generation
    /// `g1` whose draft `a` is at revision 1.
    async fn seeded() -> Orchestrator {
        let orch = crate::mcp::handlers::permission::tests::test_orchestrator().await;
        let db = &orch.db.local;
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',7,'Generate profile: Qwen/Qwen3-Embedding-4B on bbs','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment) VALUES ('j-anc','e','i','p','Builder','running',1,1,'builder')",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment) VALUES ('j-gen','e','i','p','Generator','running',1,1,'generator')",
            "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at) VALUES ('r-gen','i','p','j-gen','live',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        let mut hub = FakeHub::fixtures();
        hub.bodies.insert(
            format!("{HUGGING_FACE}/api/models?filter=base_model:quantized:Qwen/Qwen3-Embedding-4B&limit=100"),
            "[]".into(),
        );
        register(db, &hub, "Qwen/Qwen3-Embedding-4B", None)
            .await
            .unwrap();
        // The draft is written through the generated door, as the agent does,
        // so it carries the door's origin and ownership: revision 1, attempt 1.
        insert_generation(db, &generation_row()).await.unwrap();
        super::super::write_generated(
            db,
            HOME,
            "j-gen",
            &serde_json::json!({"profile": profile_yaml(r#"-m, "{weights}""#)}),
        )
        .await
        .unwrap();
        orch
    }

    fn agent_request() -> McpCallbackRequest {
        McpCallbackRequest {
            run_id: Some("r-gen".into()),
            ..Default::default()
        }
    }

    async fn ask(orch: &Orchestrator) -> ApprovalOutcome {
        request_approval(orch, &agent_request(), HOME, "j-gen", "g1")
            .await
            .unwrap()
            .0
    }

    async fn row(orch: &Orchestrator) -> GenerationRow {
        get_generation(&orch.db.local, "g1").await.unwrap().unwrap()
    }

    async fn bound(orch: &Orchestrator) -> String {
        row(orch).await.approval_prompt_id.unwrap()
    }

    /// Answer `prompt` with `choice` the way the answer surface `surface`
    /// does, `actor` being who it authenticated.
    async fn answer(
        orch: &Orchestrator,
        prompt: &str,
        choice: &str,
        surface: AppearanceTransport,
        actor: Option<&str>,
    ) {
        answer_prompt_payload(
            orch,
            prompt,
            &serde_json::json!({"answers": [{"index": 0, "selection": choice}]}),
            provenance(surface, actor),
        )
        .await
        .unwrap();
    }

    /// What `surface` records of an answer: a channel reply also names its
    /// provider and conversation, as the ledger requires.
    fn provenance(surface: AppearanceTransport, actor: Option<&str>) -> ResolutionProvenance {
        let mut provenance = ResolutionProvenance::new(surface, actor.map(str::to_string));
        if surface == AppearanceTransport::ChannelReply {
            provenance.provider = Some("imessage".into());
            provenance.conversation = Some(format!("imessage:{}", actor.unwrap_or_default()));
        }
        provenance
    }

    async fn unapproved(orch: &Orchestrator) -> bool {
        unapproved_draft(&orch.db.local, "a")
            .await
            .unwrap()
            .is_some()
    }

    async fn prompt_count(orch: &Orchestrator) -> i64 {
        orch.db
            .local
            .query_one("SELECT COUNT(*) FROM prompts", (), |row| row.i64(0))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn the_server_raises_one_canonical_unrecommended_question_and_repeats_it() {
        let orch = seeded().await;
        let ApprovalOutcome::Asked { question, .. } = ask(&orch).await else {
            panic!("the first request raises the question");
        };
        assert_eq!(question, format!("{HOME}/questions/q-1"));
        let generation = row(&orch).await;
        assert_eq!(generation.approval_revision, Some(1));
        assert_eq!(generation.approval_run_id.as_deref(), Some("r-gen"));
        let prompt = generation.approval_prompt_id.clone().unwrap();

        let draft =
            current_draft(&generation, &load_inventory(&orch.db.local).await.unwrap()).unwrap();
        let stored: Value = serde_json::from_str(
            &orch
                .db
                .local
                .query_one(
                    "SELECT questions FROM prompts WHERE id = ?1",
                    (prompt.clone(),),
                    |row| row.text(0),
                )
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            stored,
            serde_json::to_value(vec![approval_question(&generation, &draft)]).unwrap()
        );
        let text = stored[0]["question"].as_str().unwrap();
        assert!(text.contains(&draft.digest), "{text}");
        assert!(
            text.contains(&serde_json::to_string_pretty(&draft.surface).unwrap()),
            "the question shows the surface in full: {text}"
        );
        let options = stored[0]["options"].as_array().unwrap();
        assert_eq!(options.len(), 2);
        assert!(
            options
                .iter()
                .all(|option| option.get("recommended").is_none()),
            "neither Approve nor Reject is preselected: {options:?}"
        );
        assert_eq!(stored[0]["multiSelect"], false);

        // Asking again returns the very same pending question.
        assert_eq!(
            ask(&orch).await,
            ApprovalOutcome::Pending {
                question: question.clone()
            }
        );
        assert_eq!(bound(&orch).await, prompt);
        assert_eq!(prompt_count(&orch).await, 1);
        assert!(unapproved(&orch).await);
    }

    #[tokio::test]
    async fn only_the_operators_ledger_answer_approves_never_an_agents_or_ancestors() {
        let orch = seeded().await;
        for (surface, actor) in [
            // The generating agent answering its own question through its resource.
            (AppearanceTransport::ResourcePatch, Some(HOME)),
            // An ancestor, which ordinary question authority lets answer.
            (AppearanceTransport::ResourcePatch, Some(ANCESTOR)),
            (AppearanceTransport::ChannelReply, Some("+15551234567")),
            (AppearanceTransport::RemoteIntent, None),
            (AppearanceTransport::NonOperatorInvoke, None),
            (AppearanceTransport::LocalInvoke, None),
        ] {
            ask(&orch).await;
            let prompt = bound(&orch).await;
            answer(&orch, &prompt, APPROVE, surface, actor).await;
            let ApprovalOutcome::Asked { why, .. } = ask(&orch).await else {
                panic!("{surface:?} does not approve; a fresh question is raised");
            };
            assert!(why.contains("only the operator's answer"), "{why}");
            if let Some(actor) = actor.filter(|_| surface == AppearanceTransport::ResourcePatch) {
                assert!(why.contains(actor), "the refusal names who answered: {why}");
            }
            assert_ne!(bound(&orch).await, prompt, "{surface:?}");
            assert!(unapproved(&orch).await, "{surface:?}");
            assert_eq!(row(&orch).await.approved_surface, None);
        }

        let prompt = bound(&orch).await;
        answer(
            &orch,
            &prompt,
            APPROVE,
            AppearanceTransport::AuthenticatedDesktop,
            None,
        )
        .await;
        assert_eq!(ask(&orch).await, ApprovalOutcome::Approved);
        assert!(!unapproved(&orch).await);
        let generation = row(&orch).await;
        let draft =
            current_draft(&generation, &load_inventory(&orch.db.local).await.unwrap()).unwrap();
        assert!(approval_holds(&generation, &draft.digest));
        // Recording is idempotent.
        assert_eq!(ask(&orch).await, ApprovalOutcome::Approved);
    }

    /// The answerer is judged by the one operator predicate permissions use.
    /// The question ledger does not accept a companion-app winner today, so
    /// this judges a winner as it would be stored.
    #[test]
    fn the_answerer_is_the_operator_exactly_when_permissions_say_so() {
        let winner = |surface: &str, actor: &str| AskResolution {
            resolution_id: "r".into(),
            binding_ref: "p".into(),
            action_ref: "p".into(),
            kind: "question".into(),
            answer: "[]".into(),
            winner_provider: String::new(),
            winner_conversation: String::new(),
            winner_surface: surface.into(),
            winner_actor: actor.into(),
            domain_resolved: true,
        };
        for surface in [
            AppearanceTransport::AuthenticatedOperator,
            AppearanceTransport::AuthenticatedDesktop,
            AppearanceTransport::CompanionApp,
        ] {
            let stored = serde_json::to_value(surface).unwrap();
            assert_eq!(
                Answerer::of(&winner(stored.as_str().unwrap(), "")),
                Answerer::Operator(surface)
            );
        }
        assert!(matches!(
            Answerer::of(&winner("resource_patch", ANCESTOR)),
            Answerer::Other(label) if label.contains(ANCESTOR)
        ));
        assert!(matches!(
            Answerer::of(&winner("legacy_unknown", "")),
            Answerer::Other(_)
        ));
    }

    #[tokio::test]
    async fn the_operators_reject_of_the_last_candidate_ends_the_generation_exhausted() {
        let orch = seeded().await;
        orch.db
            .local
            .execute(
                "UPDATE inference_generations SET attempts = 3 WHERE id = 'g1'",
                (),
            )
            .await
            .unwrap();
        ask(&orch).await;
        let prompt = bound(&orch).await;
        answer(
            &orch,
            &prompt,
            REJECT,
            AppearanceTransport::AuthenticatedDesktop,
            None,
        )
        .await;
        // The answer alone ends it: the agent never asks again.
        let ended = row(&orch).await;
        assert_eq!(ended.state, super::super::lifecycle::EXHAUSTED);
        let inventory = load_inventory(&orch.db.local).await.unwrap();
        assert!(
            !inventory
                .profiles
                .contains_key(&InferenceProfileId::from("a"))
                || ended.cleanup_pending,
            "the rejected last draft is removed, or marked for removal while held"
        );
        let refusal = request_approval(&orch, &agent_request(), HOME, "j-gen", "g1")
            .await
            .unwrap_err();
        assert!(refusal.contains("already ended"), "{refusal}");
    }

    #[tokio::test]
    async fn a_reject_with_candidates_left_spends_only_that_revision() {
        let orch = seeded().await;
        ask(&orch).await;
        let prompt = bound(&orch).await;
        answer(
            &orch,
            &prompt,
            REJECT,
            AppearanceTransport::AuthenticatedDesktop,
            None,
        )
        .await;
        assert!(row(&orch).await.is_open(), "the answer does not end it");
        let (_, data) = super::super::write_generated(
            &orch.db.local,
            HOME,
            "j-gen",
            &serde_json::json!({"profile": profile_yaml(r#"-m, "{weights}", --flash-attn"#)}),
        )
        .await
        .unwrap();
        draft_replaced(&orch, data["generation"].as_str().unwrap()).await;
        let ApprovalOutcome::Asked { .. } = ask(&orch).await else {
            panic!("the next candidate is asked about afresh");
        };
        assert_ne!(bound(&orch).await, prompt);
    }

    #[tokio::test]
    async fn a_replacing_draft_withdraws_the_old_question_and_carries_only_operator_answers() {
        let orch = seeded().await;
        ask(&orch).await;
        let old = bound(&orch).await;
        // An ordinary preference the operator answered.
        let preference = ask_questions(
            &orch,
            &agent_request(),
            AskUserPayload {
                questions: vec![Question {
                    question: "Context length?".into(),
                    header: Some("Context".into()),
                    options: Vec::new(),
                    multi_select: false,
                }],
            },
            true,
            None,
        )
        .await
        .prompt_id
        .unwrap();
        answer_prompt_payload(
            &orch,
            &preference,
            &serde_json::json!({"answers": [{"index": 0, "text": "32k"}]}),
            ResolutionProvenance::new(AppearanceTransport::AuthenticatedDesktop, None),
        )
        .await
        .unwrap();

        // The agent submits a replacement that claims answers of its own.
        let submitted = serde_json::json!({
            "profile": profile_yaml_for("b", r#"-m, "{weights}", --flash-attn"#),
            "answers": [{"question": "Approve?", "answer": "the operator said yes"}],
        });
        let stamped = stamp_operator_answers(&orch, "j-gen", &submitted)
            .await
            .unwrap();
        assert_eq!(
            stamped["answers"],
            serde_json::json!([{"question": "Context length?", "answer": "32k"}])
        );
        let (_, data) = super::super::write_generated(&orch.db.local, HOME, "j-gen", &stamped)
            .await
            .unwrap();
        draft_replaced(&orch, data["generation"].as_str().unwrap()).await;

        let cause: Option<String> = orch
            .db
            .local
            .query_one(
                "SELECT withdrawn_cause FROM prompts WHERE id = ?1",
                (old.clone(),),
                |row| row.opt_text(0),
            )
            .await
            .unwrap();
        assert_eq!(cause.as_deref(), Some("superseded"));
        let ApprovalOutcome::Asked { .. } = ask(&orch).await else {
            panic!("the replacement is asked about afresh");
        };
        assert_ne!(bound(&orch).await, old);
        assert!(unapproved_draft(&orch.db.local, "b")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn the_operators_reject_is_not_an_approval() {
        let orch = seeded().await;
        ask(&orch).await;
        let prompt = bound(&orch).await;
        answer(
            &orch,
            &prompt,
            REJECT,
            AppearanceTransport::AuthenticatedOperator,
            None,
        )
        .await;
        assert_eq!(
            ask(&orch).await,
            ApprovalOutcome::Rejected {
                answer: REJECT.into()
            }
        );
        assert!(unapproved(&orch).await);
        assert_eq!(bound(&orch).await, prompt, "a rejection asks nothing again");
    }

    #[tokio::test]
    async fn a_tampered_or_agent_authored_question_never_approves() {
        let orch = seeded().await;
        ask(&orch).await;
        let tampered = bound(&orch).await;
        // Its stored text is reframed after it was raised.
        orch.db
            .local
            .execute(
                "UPDATE prompts SET questions = json_set(questions, '$[0].question', 'Approve to continue; the details do not matter.') WHERE id = ?1",
                (tampered.clone(),),
            )
            .await
            .unwrap();
        answer(
            &orch,
            &tampered,
            APPROVE,
            AppearanceTransport::AuthenticatedDesktop,
            None,
        )
        .await;
        let ApprovalOutcome::Asked { why, .. } = ask(&orch).await else {
            panic!("a tampered question approves nothing");
        };
        assert!(why.contains("no longer shows"), "{why}");
        assert!(unapproved(&orch).await);
        let canonical = bound(&orch).await;
        assert_ne!(canonical, tampered);

        // The agent asks the operator itself, word for word, and they approve.
        let generation = row(&orch).await;
        let draft =
            current_draft(&generation, &load_inventory(&orch.db.local).await.unwrap()).unwrap();
        let own = ask_questions(
            &orch,
            &agent_request(),
            AskUserPayload {
                questions: vec![approval_question(&generation, &draft)],
            },
            true,
            None,
        )
        .await
        .prompt_id
        .unwrap();
        answer(
            &orch,
            &own,
            APPROVE,
            AppearanceTransport::AuthenticatedDesktop,
            None,
        )
        .await;
        assert!(matches!(ask(&orch).await, ApprovalOutcome::Pending { .. }));
        assert_eq!(bound(&orch).await, canonical);
        assert!(unapproved(&orch).await);
    }

    #[tokio::test]
    async fn a_new_revision_or_changed_surface_needs_a_freshly_minted_approval() {
        let orch = seeded().await;
        ask(&orch).await;
        let first = bound(&orch).await;
        answer(
            &orch,
            &first,
            APPROVE,
            AppearanceTransport::AuthenticatedDesktop,
            None,
        )
        .await;
        assert_eq!(ask(&orch).await, ApprovalOutcome::Approved);

        // A new revision of the same surface: the old answer is not replayed.
        let bump = |revision: i64| {
            let db = orch.db.local.clone();
            async move {
                db.execute(
                    "UPDATE inference_generations SET draft_revision = ?1 WHERE id = 'g1'",
                    (revision,),
                )
                .await
                .unwrap();
            }
        };
        bump(2).await;
        assert!(unapproved(&orch).await);
        let ApprovalOutcome::Asked { why, .. } = ask(&orch).await else {
            panic!("a new revision is asked about afresh");
        };
        assert!(why.contains("revision 2"), "{why}");
        let second = bound(&orch).await;
        assert_ne!(second, first);
        assert_eq!(row(&orch).await.approved_surface, None);

        // A revision that lands while its question is pending supersedes it.
        bump(3).await;
        ask(&orch).await;
        let third = bound(&orch).await;
        assert_ne!(third, second);
        let withdrawn: Option<String> = orch
            .db
            .local
            .query_one(
                "SELECT withdrawn_cause FROM prompts WHERE id = ?1",
                (second.clone(),),
                |row| row.opt_text(0),
            )
            .await
            .unwrap();
        assert_eq!(withdrawn.as_deref(), Some("superseded"));
        answer(
            &orch,
            &third,
            APPROVE,
            AppearanceTransport::AuthenticatedDesktop,
            None,
        )
        .await;
        assert_eq!(ask(&orch).await, ApprovalOutcome::Approved);
        assert!(!unapproved(&orch).await);

        // The surface changes under the same revision: the approval no longer
        // holds, and the answered question does not approve the new surface.
        put_profile(&orch.db.local, r#"-m, "{weights}", --flash-attn"#).await;
        assert!(unapproved(&orch).await);
        let ApprovalOutcome::Asked { .. } = ask(&orch).await else {
            panic!("a changed surface is asked about afresh");
        };
        assert_ne!(bound(&orch).await, third);
        assert!(unapproved(&orch).await);
    }

    #[tokio::test]
    async fn only_the_open_generations_own_agent_requests_approval() {
        let orch = seeded().await;
        let refusal = request_approval(&orch, &agent_request(), ANCESTOR, "j-anc", "g1")
            .await
            .unwrap_err();
        assert!(refusal.contains("own agent"), "{refusal}");
        orch.db
            .local
            .execute(
                "UPDATE inference_generations SET state = 'gaveUp' WHERE id = 'g1'",
                (),
            )
            .await
            .unwrap();
        let refusal = request_approval(&orch, &agent_request(), HOME, "j-gen", "g1")
            .await
            .unwrap_err();
        assert!(refusal.contains("already ended"), "{refusal}");
        assert_eq!(prompt_count(&orch).await, 0);
    }

    #[tokio::test]
    async fn a_replaced_drafts_pending_question_is_withdrawn() {
        let orch = seeded().await;
        ask(&orch).await;
        let generation = row(&orch).await;
        withdraw_approval_question(&orch, &generation).await;
        let cause: Option<String> = orch
            .db
            .local
            .query_one(
                "SELECT withdrawn_cause FROM prompts WHERE id = ?1",
                (generation.approval_prompt_id.clone().unwrap(),),
                |row| row.opt_text(0),
            )
            .await
            .unwrap();
        assert_eq!(cause.as_deref(), Some("superseded"));
        let ApprovalOutcome::Asked { why, .. } = ask(&orch).await else {
            panic!("a withdrawn question is asked afresh");
        };
        assert!(why.contains("withdrawn"), "{why}");
    }

    #[tokio::test]
    async fn only_the_operators_answers_are_the_generations_answers() {
        let orch = seeded().await;
        let preference = |text: &str| AskUserPayload {
            questions: vec![Question {
                question: text.to_string(),
                header: Some("Quantization".into()),
                options: vec![
                    QuestionOption {
                        label: "q8_0".into(),
                        description: String::new(),
                        recommended: true,
                    },
                    QuestionOption {
                        label: "q4_k_m".into(),
                        description: String::new(),
                        recommended: false,
                    },
                ],
                multi_select: false,
            }],
        };
        for (text, choice, surface, actor) in [
            (
                "Which quantization?",
                "q8_0",
                AppearanceTransport::AuthenticatedDesktop,
                None,
            ),
            (
                "Which quantization, really?",
                "q4_k_m",
                AppearanceTransport::ResourcePatch,
                Some(ANCESTOR),
            ),
        ] {
            let prompt = ask_questions(&orch, &agent_request(), preference(text), true, None)
                .await
                .prompt_id
                .unwrap();
            answer(&orch, &prompt, choice, surface, actor).await;
        }
        // The approval question, answered by the operator, is no preference.
        ask(&orch).await;
        let approval = bound(&orch).await;
        answer(
            &orch,
            &approval,
            APPROVE,
            AppearanceTransport::AuthenticatedDesktop,
            None,
        )
        .await;
        assert_eq!(
            operator_answers(&orch, &row(&orch).await).await.unwrap(),
            vec![OperatorAnswer {
                question: "Which quantization?".into(),
                answer: "q8_0".into(),
            }]
        );
    }

    #[test]
    fn every_runnable_field_and_a_newer_surface_schema_needs_approving_again() {
        let baseline: InferenceProfile =
            serde_yaml::from_str(&profile_yaml(r#"-m, "{weights}""#)).unwrap();
        let baseline = serde_json::to_value(baseline).unwrap();
        let surface = |profile: &Value| {
            runnable_surface(&serde_json::from_value(profile.clone()).unwrap()).unwrap()
        };
        let approved = surface_digest(&surface(&baseline));
        let mut generation = generation_row();
        generation.approved_surface = Some(approved.clone());
        generation.approval_revision = Some(generation.draft_revision);
        assert!(approval_holds(&generation, &approved));

        let mut changes = Vec::new();
        for field in ["repo", "revision", "path", "sha256"] {
            let mut changed = baseline.clone();
            changed["model"]["weights"][0][field] = match field {
                "repo" => "other/weights".into(),
                "revision" => "def".into(),
                "path" => "other.gguf".into(),
                _ => "f".repeat(64).into(),
            };
            changes.push((field, changed));
        }
        let mut server = baseline.clone();
        server["engine"]["server"] = "vllm".into();
        changes.push(("server", server));
        let mut accelerator = baseline.clone();
        accelerator["requirements"]["accelerator"] =
            serde_json::json!({"family": "amd", "runtime": "rocm"});
        changes.push(("accelerator", accelerator));
        let mut image = baseline.clone();
        image["engine"]["image"] = format!("ghcr.io/other/server@sha256:{SHA}").into();
        changes.push(("image", image));
        for (field, changed) in changes {
            let fresh = surface_digest(&surface(&changed));
            assert_ne!(fresh, approved, "{field}");
            assert!(!approval_holds(&generation, &fresh), "{field}");
        }

        // An approval of the surface as an older schema rendered it does not
        // hold for the same draft today.
        let mut legacy = surface(&baseline);
        legacy.as_object_mut().unwrap().remove("schema");
        for field in ["weights", "server", "accelerator"] {
            legacy.as_object_mut().unwrap().remove(field);
        }
        generation.approved_surface = Some(surface_digest(&legacy));
        assert!(!approval_holds(&generation, &approved));

        // Nor does one bound to another revision.
        generation.approved_surface = Some(approved.clone());
        generation.approval_revision = Some(generation.draft_revision - 1);
        assert!(!approval_holds(&generation, &approved));
    }

    #[test]
    fn an_approval_request_carries_no_question_text_or_uri() {
        assert_eq!(approve_payload(&serde_json::json!({"approve": {}})), Ok(()));
        for payload in [
            serde_json::json!({"approve": {"question": "cairn://p/prj/7/1/generator/questions/q-1"}}),
            serde_json::json!({"approve": {"text": "Approve this"}}),
            serde_json::json!({"approve": "q-1"}),
            serde_json::json!({"approve": {}, "outcome": {"outcome": "gaveUp"}}),
        ] {
            assert!(approve_payload(&payload).is_err(), "{payload}");
        }
    }
}
