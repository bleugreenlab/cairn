//! The four typed crossings.
//!
//! A crossing is a place where a value changes trust domain. Cairn has four that
//! matter for credential disclosure, and they do not share one seam: an
//! asynchronous backend event reaches the transcript without ever passing
//! through tool dispatch, and a tool result reaches the model without ever
//! becoming a transcript event. Each therefore gets its own type, and the type
//! is what makes the check unforgettable:
//!
//! - [`CheckedInvocation`] gates model-originated input *into* a handler.
//! - [`ObservedSafe`] gates observed output *out* to a model, a transcript row,
//!   or a frontend subscriber.
//!
//! Neither wrapper can be built from raw data by any other means, so a new
//! dispatch entry or transcript writer cannot silently opt out: it will not
//! compile without naming the constructor.

use std::ops::Deref;

use serde::Serialize;
use serde_json::Value;

use super::registry::Detections;
use super::sanitize::Sanitizer;
use super::secret::{MatchRule, SecretCategory, SecretId};

/// Where a value was inspected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Crossing {
    /// Model-originated tool input, before handler dispatch.
    Invocation,
    /// A dispatch result, before it reaches a backend or model.
    FinalResponse,
    /// A backend event, before it becomes a persisted transcript row.
    Transcript,
    /// A transcript or model event, before it reaches frontend subscribers.
    LiveEvent,
    /// Output read back from a process — a batch item, a terminal, a REPL, a
    /// workflow, or a remote executor's relay — before it reaches a buffer, a
    /// subscriber, or a result.
    ///
    /// Distinct from [`Self::FinalResponse`] because it is not a dispatch
    /// result: it crosses on its own schedule, often long after the call that
    /// started the process returned, and a detection here names a *process* that
    /// echoed a credential rather than a tool that returned one.
    ProcessOutput,
    /// A result, catalog, or error coming back from an external MCP server,
    /// before it reaches any Cairn code.
    ExternalTool,
}

impl Crossing {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Invocation => "invocation",
            Self::FinalResponse => "final-response",
            Self::Transcript => "transcript",
            Self::LiveEvent => "live-event",
            Self::ProcessOutput => "process-output",
            Self::ExternalTool => "external-tool",
        }
    }
}

/// A non-secret record that a registered credential was seen at a crossing.
///
/// Deliberately carries no value and no surrounding context: context around a
/// match is the value's immediate neighbourhood, and reporting it would leak
/// exactly what the report exists to protect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DetectionReport {
    pub secret_id: Option<SecretId>,
    pub category: Option<SecretCategory>,
    pub crossing: Crossing,
    pub rule: MatchRule,
    pub count: usize,
    pub run_id: Option<String>,
    /// Identity of the event or call the detection belongs to, when the crossing
    /// has one.
    pub event_ref: Option<String>,
}

impl DetectionReport {
    /// Build reports for one scrub pass.
    pub fn from_detections(
        detections: &Detections,
        crossing: Crossing,
        run_id: Option<&str>,
        event_ref: Option<&str>,
    ) -> Vec<Self> {
        detections
            .entries()
            .iter()
            .map(|entry| Self {
                secret_id: entry.secret_id.clone(),
                category: entry.category,
                crossing,
                rule: entry.rule,
                count: entry.count,
                run_id: run_id.map(str::to_string),
                event_ref: event_ref.map(str::to_string),
            })
            .collect()
    }

    /// Emit the operator-visible signal for a detection.
    ///
    /// Structural matches are routine on untrusted payloads and stay quiet; an
    /// exact match on a registered Cairn credential is an incident signal and is
    /// logged at warn.
    pub fn log(&self) {
        if !self.rule.is_exact() {
            return;
        }
        log::warn!(
            "secret_detection: crossing={} secret={} category={} rule={:?} count={} run={}",
            self.crossing.as_str(),
            self.secret_id
                .as_ref()
                .map(SecretId::as_str)
                .unwrap_or("<structural>"),
            self.category
                .map(SecretCategory::as_str)
                .unwrap_or("<none>"),
            self.rule,
            self.count,
            self.run_id.as_deref().unwrap_or("<none>"),
        );
    }
}

// ── Inbound invocation crossing ────────────────────────────────────────────

/// What a dispatch entry needs from a request to check it.
pub trait ModelInvocation {
    fn tool_name(&self) -> &str;
    fn tool_input(&self) -> &Value;
    fn run_identity(&self) -> Option<&str>;
}

/// A model-originated invocation that carried a registered credential.
#[derive(Debug, Clone)]
pub struct RejectedInvocation {
    pub tool: String,
    pub reports: Vec<DetectionReport>,
}

impl RejectedInvocation {
    /// What the caller sees instead of a tool result.
    ///
    /// Generic on purpose. Naming which secret matched, where in the payload it
    /// sat, or how much of it was recognized would hand an agent an oracle for
    /// probing the registry one byte at a time. The non-secret detail goes to
    /// the operator's log through [`DetectionReport::log`], not to the caller.
    pub fn refusal(&self) -> String {
        format!(
            "This `{}` call was refused: its input contains a credential this session is not \
             permitted to send. The call was rejected before any side effect — nothing was \
             written, previewed, or committed. Remove the credential from the request. Cairn \
             uses its own credentials on your behalf; a tool that needs one does not need you \
             to carry its value.",
            self.tool
        )
    }
}

/// A tool invocation that has passed the inbound crossing.
///
/// Handlers are reached only through this wrapper, so "was this input checked?"
/// is answered by the signature rather than by reviewer memory.
pub struct CheckedInvocation<'a, T> {
    request: &'a T,
}

impl<'a, T: ModelInvocation> CheckedInvocation<'a, T> {
    /// Check a model-originated invocation.
    ///
    /// There is no second constructor, and deliberately no origin parameter.
    /// Every caller of authenticated tool dispatch is an agent process, and the
    /// `_cairn_origin` marker a standalone CLI sets is not evidence of a human:
    /// an agent's own shell can invoke the `cairn` CLI, so honouring that marker
    /// would make this gate bypassable from inside a `run` item.
    /// Operator-authored content reaches the system through the desktop
    /// resource-mutation paths, which do not pass through tool dispatch at all.
    /// When the broker gives typed secret-store operations a genuine second
    /// origin (CAIRN-3825), it arrives as its own constructor — never as a flag
    /// on this one, which a caller could set to skip the check.
    ///
    /// Recursively inspects the parsed tool input for exact registered values.
    /// A match rejects; it never rewrites. Rewriting a model-authored write to
    /// make it safe would commit corrupted content under the agent's name, so
    /// the only honest answers are "run it" and "refuse it".
    ///
    /// Structural heuristics deliberately do not reject: a false positive here
    /// would block a legitimate write, which is a far worse failure than the
    /// speculative leak it would prevent.
    pub fn from_model(request: &'a T) -> Result<Self, RejectedInvocation> {
        let mut sanitizer = Sanitizer::exact();
        if !sanitizer.is_noop() {
            let mut probe = request.tool_input().clone();
            sanitizer.json(&mut probe);
            let detections = sanitizer.into_detections();
            if detections.has_exact() {
                let reports = DetectionReport::from_detections(
                    &detections,
                    Crossing::Invocation,
                    request.run_identity(),
                    Some(request.tool_name()),
                );
                for report in &reports {
                    report.log();
                }
                return Err(RejectedInvocation {
                    tool: request.tool_name().to_string(),
                    reports,
                });
            }
        }
        Ok(Self { request })
    }

    pub fn request(&self) -> &'a T {
        self.request
    }
}

// ── Observed-output crossings ──────────────────────────────────────────────

/// A value that has been sanitized for a specific crossing.
///
/// `Deref` gives read access, because reading an already-sanitized value is
/// safe by construction. The gate is on *making* one.
pub struct ObservedSafe<T> {
    value: T,
    crossing: Crossing,
    detections: Detections,
}

impl<T: Sanitize> ObservedSafe<T> {
    /// Sanitize a value for a crossing.
    ///
    /// Infallible by construction: the sanitizer replaces bytes in place and has
    /// no failure mode that could leave a partially-scrubbed value. Callers that
    /// serialize afterwards must fail closed on their own serialization errors.
    pub fn observe(mut value: T, crossing: Crossing) -> Self {
        let mut sanitizer = Sanitizer::exact();
        if !sanitizer.is_noop() {
            value.sanitize_observed(&mut sanitizer);
        }
        Self {
            value,
            crossing,
            detections: sanitizer.into_detections(),
        }
    }
}

impl<T> ObservedSafe<T> {
    pub fn crossing(&self) -> Crossing {
        self.crossing
    }

    pub fn detections(&self) -> &Detections {
        &self.detections
    }

    pub fn into_inner(self) -> T {
        self.value
    }

    /// Re-stamp an already-sanitized value for a downstream crossing.
    ///
    /// The live-event crossing must emit *the same* value persistence used;
    /// forwarding the wrapper rather than re-deriving from raw data is what
    /// prevents a raw/sanitized fork.
    pub fn forwarded(self, crossing: Crossing) -> Self {
        Self { crossing, ..self }
    }

    /// Non-secret reports for what this pass matched.
    pub fn reports(&self, run_id: Option<&str>, event_ref: Option<&str>) -> Vec<DetectionReport> {
        DetectionReport::from_detections(&self.detections, self.crossing, run_id, event_ref)
    }
}

impl<T> Deref for ObservedSafe<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for ObservedSafe<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObservedSafe")
            .field("crossing", &self.crossing)
            .field("value", &self.value)
            .finish()
    }
}

impl<T: Clone> Clone for ObservedSafe<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            crossing: self.crossing,
            detections: self.detections.clone(),
        }
    }
}

/// Types that know how to scrub themselves for an observed-output crossing.
///
/// Implementations live in this module and in the two modules that own a guarded
/// payload type (`dispatch`, `agent_process::stream`). A source-level check keeps
/// that list closed, so a new type cannot become "safe" by implementing a no-op.
pub trait Sanitize {
    fn sanitize_observed(&mut self, sanitizer: &mut Sanitizer<'_>);
}

impl Sanitize for String {
    fn sanitize_observed(&mut self, sanitizer: &mut Sanitizer<'_>) {
        sanitizer.text_in_place(self);
    }
}

impl Sanitize for Value {
    fn sanitize_observed(&mut self, sanitizer: &mut Sanitizer<'_>) {
        sanitizer.json(self);
    }
}

impl<T: Sanitize> Sanitize for Option<T> {
    fn sanitize_observed(&mut self, sanitizer: &mut Sanitizer<'_>) {
        if let Some(value) = self.as_mut() {
            value.sanitize_observed(sanitizer);
        }
    }
}

impl<T: Sanitize> Sanitize for Vec<T> {
    fn sanitize_observed(&mut self, sanitizer: &mut Sanitizer<'_>) {
        for value in self.iter_mut() {
            value.sanitize_observed(sanitizer);
        }
    }
}
