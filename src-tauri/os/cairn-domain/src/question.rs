//! Question values shared by callback payloads and attention facts, and the
//! answers a prompt's questions receive.
use serde::{Deserialize, Serialize};

/// A single option for a question
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuestionOption {
    pub label: String,
    pub description: String,
    /// The asker's recommendation, which an answer form preselects so that
    /// accepting it is one click. At most one per single-select question.
    #[serde(default, skip_serializing_if = "is_false")]
    pub recommended: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Refuse questions no form can show coherently: a single-select question
/// recommending more than one option.
pub fn validate_questions(questions: &[Question]) -> Result<(), String> {
    for (index, question) in questions.iter().enumerate() {
        let recommended = question.options.iter().filter(|o| o.recommended).count();
        if !question.multi_select && recommended > 1 {
            return Err(format!(
                "question index {index} ({}) is single-select but recommends {recommended} options; recommend at most one, or set multiSelect",
                question_label(index)
            ));
        }
    }
    Ok(())
}

/// A single question to ask the user
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Question {
    pub question: String,
    pub header: Option<String>,
    /// Empty (or omitted) makes the question free-form: the answer is typed
    /// text rather than a selection.
    #[serde(default)]
    pub options: Vec<QuestionOption>,
    #[serde(default)]
    pub multi_select: bool,
}

/// One question's answer, keyed by the question's zero-based index in its
/// prompt. The answer is opaque text: a newline in it is just a character.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuestionAnswer {
    pub index: usize,
    pub answer: String,
}

/// Every question in one prompt answered exactly once, sorted by index.
///
/// The only constructors validate against the prompt's question count, so a
/// value of this type cannot hold a missing, duplicate, out-of-range, or empty
/// answer. It is stored as a JSON array ([`PromptAnswers::to_stored`]) and
/// shown to people through [`PromptAnswers::render`]; rendered text is never
/// parsed back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptAnswers {
    question_count: usize,
    answers: Vec<QuestionAnswer>,
}

/// The condition whose coming true closed a question as moot by itself: the
/// `ask_conditions` row and its rendered clauses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MootCondition {
    pub id: String,
    pub reason: String,
}

/// A question resolved without an answer: the operator did the thing outside
/// Cairn (`handled`), or it no longer needs one (`moot`), by the operator's
/// hand or by its `moot_when` coming true (`condition`).
///
/// Stored in `prompts.response` as an object tagged by `form`, beside the
/// answer array, so `response IS NULL` still means exactly "unresolved".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "form", rename_all = "snake_case")]
pub enum Closure {
    Handled {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
    Moot {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        condition: Option<MootCondition>,
    },
}

impl Closure {
    pub fn form(&self) -> &'static str {
        match self {
            Closure::Handled { .. } => "handled",
            Closure::Moot { .. } => "moot",
        }
    }

    pub fn note(&self) -> Option<&str> {
        match self {
            Closure::Handled { note } | Closure::Moot { note, .. } => note.as_deref(),
        }
    }

    pub fn condition(&self) -> Option<&MootCondition> {
        match self {
            Closure::Moot { condition, .. } => condition.as_ref(),
            Closure::Handled { .. } => None,
        }
    }

    /// One line a reader sees in place of an answer.
    pub fn render(&self) -> String {
        let head = match self {
            Closure::Handled { .. } => "Handled".to_string(),
            Closure::Moot {
                condition: Some(condition),
                ..
            } => format!("Moot: closed itself because {}", condition.reason),
            Closure::Moot { .. } => "Moot".to_string(),
        };
        match self.note() {
            Some(note) => format!("{head}: {note}"),
            None => head,
        }
    }
}

/// How a question was resolved. Every resolution is recorded in
/// `prompts.response` and delivered to the asker through one path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptResolution {
    Answered(PromptAnswers),
    Closed(Closure),
}

impl PromptResolution {
    pub fn to_stored(&self) -> String {
        match self {
            PromptResolution::Answered(answers) => answers.to_stored(),
            PromptResolution::Closed(closure) => {
                serde_json::to_string(closure).expect("a closure always serializes")
            }
        }
    }

    /// `answered`, `handled` or `moot`: the question's state once resolved.
    pub fn form(&self) -> &'static str {
        match self {
            PromptResolution::Answered(_) => "answered",
            PromptResolution::Closed(closure) => closure.form(),
        }
    }

    pub fn render(&self, questions: &[Question]) -> String {
        match self {
            PromptResolution::Answered(answers) => answers.render(questions),
            PromptResolution::Closed(closure) => closure.render(),
        }
    }

    pub fn answers(&self) -> Option<&PromptAnswers> {
        match self {
            PromptResolution::Answered(answers) => Some(answers),
            PromptResolution::Closed(_) => None,
        }
    }
}

/// A stored `prompts.response` (or claim-ledger answer) as read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredAnswer {
    /// The canonical indexed form.
    Answers(PromptAnswers),
    /// Resolved without an answer: handled or moot.
    Closed(Closure),
    /// A value written before answers were stored by index: bare text, the
    /// old `Question N: answer` lines, or a desktop JSON object keyed by
    /// question text. It is shown verbatim and never reinterpreted, except
    /// where an unresolved claim from before the change still has to run.
    Legacy(String),
}

/// `Question 2` for index 1: the numbering people see.
fn question_label(index: usize) -> String {
    format!("Question {}", index + 1)
}

fn prompt_shape(question_count: usize) -> String {
    match question_count {
        0 => "this prompt has no questions".to_string(),
        1 => "this prompt has 1 question (index 0)".to_string(),
        n => format!("this prompt has {n} questions (indices 0-{})", n - 1),
    }
}

fn excerpt(line: &str) -> String {
    const LIMIT: usize = 40;
    match line.char_indices().nth(LIMIT) {
        Some((cut, _)) => format!("{}...", &line[..cut]),
        None => line.to_string(),
    }
}

impl PromptAnswers {
    /// Validate `answers` against a prompt of `question_count` questions:
    /// every index in range, answered once, none missing, none empty. Answer
    /// text is kept byte-for-byte; `trim` only decides emptiness.
    pub fn new(question_count: usize, mut answers: Vec<QuestionAnswer>) -> Result<Self, String> {
        if question_count == 0 {
            return Err("this prompt has no questions to answer".to_string());
        }
        let mut seen = vec![false; question_count];
        for answer in &answers {
            let index = answer.index;
            if index >= question_count {
                return Err(format!(
                    "answer index {index} ({}) is out of range: {}",
                    question_label(index),
                    prompt_shape(question_count)
                ));
            }
            if std::mem::replace(&mut seen[index], true) {
                return Err(format!(
                    "question index {index} ({}) is answered more than once; give each question one answer",
                    question_label(index)
                ));
            }
            if answer.answer.trim().is_empty() {
                return Err(format!(
                    "question index {index} ({}) has an empty answer",
                    question_label(index)
                ));
            }
        }
        if let Some(missing) = seen.iter().position(|seen| !seen) {
            return Err(format!(
                "question index {missing} ({}) has no answer; every question in this prompt needs one",
                question_label(missing)
            ));
        }
        answers.sort_by_key(|answer| answer.index);
        Ok(Self {
            question_count,
            answers,
        })
    }

    /// Parse a human-typed answer against a prompt of `question_count`
    /// questions. This is the only place the `Question N: ` line format is
    /// read.
    ///
    /// With one question the whole text, byte for byte, is the answer, even a
    /// line that looks like a header. With several, line 1 must begin
    /// `Question 1: `, and only a line beginning `Question k: ` with `k` the
    /// next expected number opens the next answer; every other line belongs to
    /// the answer it follows. Bodies are sliced from the original text, so the
    /// only bytes dropped are the headers and the newline before each one.
    pub fn parse_typed(question_count: usize, text: &str) -> Result<Self, String> {
        if question_count == 1 {
            return Self::new(
                1,
                vec![QuestionAnswer {
                    index: 0,
                    answer: text.to_string(),
                }],
            );
        }
        if question_count == 0 {
            return Self::new(0, Vec::new());
        }
        let first = "Question 1: ";
        let Some(mut body_start) = text.starts_with(first).then_some(first.len()) else {
            let line = text.lines().next().unwrap_or("");
            return Err(format!(
                "line 1 must begin \"{first}\" because {}; found \"{}\"",
                prompt_shape(question_count),
                excerpt(line)
            ));
        };
        let mut answers = Vec::with_capacity(question_count);
        for number in 2..=question_count {
            let header = format!("\nQuestion {number}: ");
            let Some(offset) = text[body_start..].find(&header) else {
                return Err(format!(
                    "expected a line beginning \"Question {number}: \" to start the answer to question {number}; none found"
                ));
            };
            let boundary = body_start + offset;
            answers.push(QuestionAnswer {
                index: number - 2,
                answer: text[body_start..boundary].to_string(),
            });
            body_start = boundary + header.len();
        }
        answers.push(QuestionAnswer {
            index: question_count - 1,
            answer: text[body_start..].to_string(),
        });
        Self::new(question_count, answers)
    }

    /// The stored encoding: a JSON array of `{"index", "answer"}` objects.
    pub fn to_stored(&self) -> String {
        serde_json::to_string(&self.answers).expect("question answers always serialize")
    }

    /// Read a stored value back: the canonical array, a [`Closure`] object,
    /// or anything else as [`StoredAnswer::Legacy`], kept verbatim.
    pub fn decode_stored(stored: &str) -> StoredAnswer {
        if let Some(answers) = serde_json::from_str::<Vec<QuestionAnswer>>(stored)
            .ok()
            .and_then(|answers| Self::new(answers.len(), answers).ok())
        {
            return StoredAnswer::Answers(answers);
        }
        match serde_json::from_str::<Closure>(stored) {
            Ok(closure) => StoredAnswer::Closed(closure),
            Err(_) => StoredAnswer::Legacy(stored.to_string()),
        }
    }

    /// What a person reads: one answer verbatim, or for several questions a
    /// `Q{n}. {question}` block per answer with the answer on the next line,
    /// blocks separated by a blank line.
    pub fn render(&self, questions: &[Question]) -> String {
        if self.question_count == 1 {
            return self.answers[0].answer.clone();
        }
        self.answers
            .iter()
            .map(|answer| match questions.get(answer.index) {
                Some(question) => format!(
                    "Q{}. {}\n{}",
                    answer.index + 1,
                    question.question,
                    answer.answer
                ),
                None => format!("Q{}.\n{}", answer.index + 1, answer.answer),
            })
            .collect::<Vec<_>>()
            .join("\n\n")
    }

    pub fn answers(&self) -> &[QuestionAnswer] {
        &self.answers
    }

    pub fn into_answers(self) -> Vec<QuestionAnswer> {
        self.answers
    }

    pub fn question_count(&self) -> usize {
        self.question_count
    }
}

impl StoredAnswer {
    /// Recover answers that still have to be acted on: the canonical form as
    /// stored, or a legacy value through the typed-input parser, which is how
    /// every legacy value was written.
    pub fn into_answers(self, question_count: usize) -> Result<PromptAnswers, String> {
        match self {
            StoredAnswer::Answers(answers) if answers.question_count() == question_count => {
                Ok(answers)
            }
            StoredAnswer::Answers(answers) => Err(format!(
                "stored answers cover {} question(s) but {}",
                answers.question_count(),
                prompt_shape(question_count)
            )),
            StoredAnswer::Closed(closure) => Err(format!(
                "this question was closed as {}, not answered",
                closure.form()
            )),
            StoredAnswer::Legacy(text) => PromptAnswers::parse_typed(question_count, &text),
        }
    }

    /// The resolution a stored value records, recovering a legacy answer
    /// through the typed-input parser as [`StoredAnswer::into_answers`] does.
    pub fn into_resolution(self, question_count: usize) -> Result<PromptResolution, String> {
        match self {
            StoredAnswer::Closed(closure) => Ok(PromptResolution::Closed(closure)),
            other => other
                .into_answers(question_count)
                .map(PromptResolution::Answered),
        }
    }

    /// `answered`, `handled` or `moot`.
    pub fn form(&self) -> &'static str {
        match self {
            StoredAnswer::Closed(closure) => closure.form(),
            StoredAnswer::Answers(_) | StoredAnswer::Legacy(_) => "answered",
        }
    }

    /// What a person reads: the canonical form rendered, a closure as its
    /// line, a legacy value verbatim.
    pub fn render(&self, questions: &[Question]) -> String {
        match self {
            StoredAnswer::Answers(answers) => answers.render(questions),
            StoredAnswer::Closed(closure) => closure.render(),
            StoredAnswer::Legacy(text) => text.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = "First paragraph.\n\nSecond paragraph:\n\n1. a\n2. b\n\n- one\n- two\n\n```rust\nfn main() {\n    println!(\"Question 2: x\");\n}\n```\n";
    const OTHER: &str = "Another\nmulti-line\n\nanswer.";

    fn answer(index: usize, answer: &str) -> QuestionAnswer {
        QuestionAnswer {
            index,
            answer: answer.to_string(),
        }
    }

    fn question(text: &str) -> Question {
        Question {
            question: text.to_string(),
            header: None,
            options: Vec::new(),
            multi_select: false,
        }
    }

    fn option(label: &str, recommended: bool) -> QuestionOption {
        QuestionOption {
            label: label.to_string(),
            description: String::new(),
            recommended,
        }
    }

    #[test]
    fn a_recommendation_round_trips_and_an_unrecommended_option_omits_it() {
        let json = serde_json::json!({
            "question": "Quantization?",
            "header": null,
            "options": [
                {"label": "q8_0", "description": "", "recommended": true},
                {"label": "bf16", "description": ""}
            ]
        });
        let question: Question = serde_json::from_value(json).unwrap();
        assert!(question.options[0].recommended);
        assert!(!question.options[1].recommended);
        let back = serde_json::to_value(&question).unwrap();
        assert_eq!(back["options"][0]["recommended"], true);
        assert!(back["options"][1].get("recommended").is_none());
    }

    #[test]
    fn a_single_select_question_recommends_at_most_one_option() {
        let mut q = question("Engine?");
        q.options = vec![option("vLLM", true), option("llama.cpp", false)];
        assert_eq!(validate_questions(std::slice::from_ref(&q)), Ok(()));
        q.options[1].recommended = true;
        assert_eq!(
            validate_questions(std::slice::from_ref(&q)).unwrap_err(),
            "question index 0 (Question 1) is single-select but recommends 2 options; recommend at most one, or set multiSelect"
        );
        q.multi_select = true;
        assert_eq!(validate_questions(&[q]), Ok(()));
    }

    #[test]
    fn a_question_without_options_is_free_form() {
        let question: Question =
            serde_json::from_value(serde_json::json!({"question": "What should change?"})).unwrap();
        assert_eq!(question.question, "What should change?");
        assert!(question.options.is_empty());
        assert!(!question.multi_select);
        assert_eq!(question.header, None);
    }

    #[test]
    fn stored_answers_round_trip_byte_for_byte() {
        for answers in [
            PromptAnswers::new(1, vec![answer(0, BODY)]).unwrap(),
            PromptAnswers::new(2, vec![answer(1, OTHER), answer(0, BODY)]).unwrap(),
        ] {
            let stored = answers.to_stored();
            assert_eq!(
                PromptAnswers::decode_stored(&stored),
                StoredAnswer::Answers(answers.clone())
            );
        }
        let two = PromptAnswers::new(2, vec![answer(1, OTHER), answer(0, BODY)]).unwrap();
        assert_eq!(two.answers(), &[answer(0, BODY), answer(1, OTHER)]);
    }

    #[test]
    fn an_extra_answer_on_a_one_question_prompt_names_its_index() {
        let error = PromptAnswers::new(1, vec![answer(0, "a"), answer(1, "b")]).unwrap_err();
        assert_eq!(
            error,
            "answer index 1 (Question 2) is out of range: this prompt has 1 question (index 0)"
        );
    }

    #[test]
    fn missing_duplicate_and_empty_answers_name_the_index() {
        assert_eq!(
            PromptAnswers::new(2, vec![answer(0, "a")]).unwrap_err(),
            "question index 1 (Question 2) has no answer; every question in this prompt needs one"
        );
        assert!(PromptAnswers::new(2, vec![answer(0, "a"), answer(0, "b")])
            .unwrap_err()
            .starts_with("question index 0 (Question 1) is answered more than once"));
        assert_eq!(
            PromptAnswers::new(1, vec![answer(0, " \n")]).unwrap_err(),
            "question index 0 (Question 1) has an empty answer"
        );
    }

    #[test]
    fn one_question_typed_text_is_the_answer_verbatim() {
        let text = "Question 2: looks like a header\n\n1. a\n2. b";
        let parsed = PromptAnswers::parse_typed(1, text).unwrap();
        assert_eq!(parsed.answers(), &[answer(0, text)]);
    }

    #[test]
    fn several_questions_typed_with_multi_line_bodies_parse() {
        let text = format!("Question 1: {BODY}\nQuestion 2: {OTHER}");
        let parsed = PromptAnswers::parse_typed(2, &text).unwrap();
        assert_eq!(parsed.answers(), &[answer(0, BODY), answer(1, OTHER)]);
    }

    #[test]
    fn only_the_next_expected_header_is_a_boundary() {
        let text = "Question 1: first\nQuestion 1: still first\nQuestion 3: still first\nQuestion 2: second";
        let parsed = PromptAnswers::parse_typed(2, text).unwrap();
        assert_eq!(
            parsed.answers(),
            &[
                answer(0, "first\nQuestion 1: still first\nQuestion 3: still first"),
                answer(1, "second"),
            ]
        );
    }

    #[test]
    fn malformed_typed_answers_name_the_line_and_what_was_expected() {
        assert_eq!(
            PromptAnswers::parse_typed(2, "Sure, go ahead\nand more").unwrap_err(),
            "line 1 must begin \"Question 1: \" because this prompt has 2 questions (indices 0-1); found \"Sure, go ahead\""
        );
        assert_eq!(
            PromptAnswers::parse_typed(2, "Question 1: a\nb").unwrap_err(),
            "expected a line beginning \"Question 2: \" to start the answer to question 2; none found"
        );
    }

    #[test]
    fn render_shows_questions_never_bare_indices() {
        let questions = [question("Which path?"), question("What risks?")];
        let two = PromptAnswers::new(2, vec![answer(0, "Legacy"), answer(1, "a\nb")]).unwrap();
        assert_eq!(
            two.render(&questions),
            "Q1. Which path?\nLegacy\n\nQ2. What risks?\na\nb"
        );
        let one = PromptAnswers::new(1, vec![answer(0, BODY)]).unwrap();
        assert_eq!(one.render(&questions[..1]), BODY);
    }

    #[test]
    fn old_flat_values_decode_as_legacy_and_display_verbatim() {
        let questions = [question("Which path?"), question("What risks?")];
        for old in [
            "Question 1: a\nQuestion 2: b",
            r#"{"Which path?":"Legacy"}"#,
            "plain text",
        ] {
            let decoded = PromptAnswers::decode_stored(old);
            assert_eq!(decoded, StoredAnswer::Legacy(old.to_string()));
            assert_eq!(decoded.render(&questions), old);
        }
    }

    #[test]
    fn closures_round_trip_and_never_read_as_an_answer() {
        let questions = [question("Merge #4382?")];
        for (resolution, stored, rendered) in [
            (
                PromptResolution::Closed(Closure::Handled { note: None }),
                r#"{"form":"handled"}"#,
                "Handled",
            ),
            (
                PromptResolution::Closed(Closure::Handled {
                    note: Some("merged it from the console".into()),
                }),
                r#"{"form":"handled","note":"merged it from the console"}"#,
                "Handled: merged it from the console",
            ),
            (
                PromptResolution::Closed(Closure::Moot {
                    note: None,
                    condition: Some(MootCondition {
                        id: "c-1".into(),
                        reason: "cairn://p/cairn/4382 merged".into(),
                    }),
                }),
                r#"{"form":"moot","condition":{"id":"c-1","reason":"cairn://p/cairn/4382 merged"}}"#,
                "Moot: closed itself because cairn://p/cairn/4382 merged",
            ),
        ] {
            assert_eq!(resolution.to_stored(), stored);
            let decoded = PromptAnswers::decode_stored(stored);
            assert_eq!(decoded.render(&questions), rendered);
            assert_eq!(decoded.form(), resolution.form());
            assert!(decoded.clone().into_answers(1).is_err());
            assert_eq!(decoded.into_resolution(1).unwrap(), resolution);
        }
        let answered = PromptAnswers::new(1, vec![answer(0, "Yes")]).unwrap();
        assert_eq!(
            PromptAnswers::decode_stored(&answered.to_stored()).into_resolution(1),
            Ok(PromptResolution::Answered(answered))
        );
    }

    #[test]
    fn a_legacy_winner_still_to_run_resolves_through_the_typed_parser() {
        let legacy = PromptAnswers::decode_stored("Question 1: a\nQuestion 2: b");
        let answers = legacy.into_answers(2).unwrap();
        assert_eq!(answers.answers(), &[answer(0, "a"), answer(1, "b")]);
        let plain = PromptAnswers::decode_stored("Yes").into_answers(1).unwrap();
        assert_eq!(plain.answers(), &[answer(0, "Yes")]);
    }
}
