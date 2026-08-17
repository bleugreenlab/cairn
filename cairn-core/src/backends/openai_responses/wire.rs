//! OpenAI Responses wire types: input/output items, the typed lifecycle SSE
//! events, and the aggregator that rebuilds a response from them. Pure data plus
//! (de)serialization; no HTTP or orchestrator state.
//!
//! Shapes are taken from recorded OpenCode Go `/zen/go/v1/responses` traffic.
//! One recorded behaviour drives the aggregator's design: the terminal
//! `response.completed` envelope carried ONLY the function-call item, dropping
//! the assistant message that had streamed beside it. Rebuilding from the item
//! and delta events — rather than trusting the terminal envelope's `output` — is
//! therefore what keeps the assistant's text from vanishing.

use crate::backends::http_loop::TurnUsage;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One turn's worth of items, as the neutral turn loop's `Message`.
///
/// Responses has no single assistant message: one generation is a reasoning
/// item, a message, and an item per tool call. The loop pushes exactly one
/// `Message` per generation, so a `Message` here is a GROUP of items, flattened
/// back into the protocol's flat input list when the next request is built.
#[derive(Debug, Clone)]
pub(crate) struct ResponsesTurn {
    pub(crate) items: Vec<ResponsesItem>,
}

impl ResponsesTurn {
    pub(crate) fn one(item: ResponsesItem) -> Self {
        Self { items: vec![item] }
    }

    pub(crate) fn estimated_chars(&self) -> usize {
        self.items.iter().map(ResponsesItem::estimated_chars).sum()
    }
}

/// One item in a Responses conversation.
///
/// Responses has no message array with roles at the top level — it has a flat
/// ordered list of items, where a tool call and its output are siblings of the
/// messages around them. Cairn's system prompt is not an item at all: it is the
/// request's `instructions` field, carried here as [`ResponsesItem::Instructions`]
/// so the neutral loop can still hand the adapter one flat `Vec<Message>`, and
/// lifted back out by [`super::http::build_body`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ResponsesItem {
    Instructions {
        text: String,
    },
    Message {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        role: String,
        /// The gateway sends `"content": null` on `output_item.added` and fills
        /// it through deltas, so absence and null both have to read as empty.
        #[serde(default, deserialize_with = "null_as_empty")]
        content: Vec<ContentPart>,
    },
    FunctionCall {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        /// The id a `function_call_output` must quote. Distinct from `id`, which
        /// names the item; quoting the wrong one orphans the result.
        call_id: String,
        name: String,
        #[serde(default)]
        arguments: String,
    },
    FunctionCallOutput {
        call_id: String,
        output: String,
    },
    /// A reasoning item. `encrypted_content` is opaque and must be replayed
    /// verbatim for a continuation to stay valid.
    Reasoning {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        encrypted_content: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        summary: Vec<ReasoningSummary>,
    },
    /// An item type this build has no mapping for. Kept so an unrecognized item
    /// cannot fail the whole parse, and dropped before anything is sent back.
    #[serde(other)]
    Unknown,
}

fn null_as_empty<'de, D>(deserializer: D) -> Result<Vec<ContentPart>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<Vec<ContentPart>>::deserialize(deserializer)?.unwrap_or_default())
}

impl ResponsesItem {
    pub(crate) fn is_replayable(&self) -> bool {
        !matches!(self, ResponsesItem::Unknown)
    }

    pub(crate) fn assistant_text(text: String) -> Self {
        ResponsesItem::Message {
            id: None,
            role: "assistant".to_string(),
            content: vec![ContentPart::OutputText { text }],
        }
    }

    pub(crate) fn estimated_chars(&self) -> usize {
        match self {
            ResponsesItem::Instructions { text } => text.len(),
            ResponsesItem::Message { content, .. } => {
                content.iter().map(ContentPart::estimated_chars).sum()
            }
            ResponsesItem::FunctionCall {
                name, arguments, ..
            } => name.len() + arguments.len(),
            ResponsesItem::FunctionCallOutput { output, .. } => output.len(),
            ResponsesItem::Reasoning {
                encrypted_content,
                summary,
                ..
            } => {
                encrypted_content.as_ref().map_or(0, String::len)
                    + summary.iter().map(|part| part.text.len()).sum::<usize>()
            }
            ResponsesItem::Unknown => 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ContentPart {
    /// User-authored text. The input and output spellings are distinct on this
    /// protocol and are not interchangeable.
    InputText {
        text: String,
    },
    InputImage {
        image_url: String,
    },
    OutputText {
        text: String,
    },
    #[serde(other)]
    Unknown,
}

impl ContentPart {
    fn estimated_chars(&self) -> usize {
        match self {
            ContentPart::InputText { text } | ContentPart::OutputText { text } => text.len(),
            ContentPart::InputImage { image_url } => image_url.len(),
            ContentPart::Unknown => 0,
        }
    }

    pub(crate) fn text(&self) -> Option<&str> {
        match self {
            ContentPart::InputText { text } | ContentPart::OutputText { text } => Some(text),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReasoningSummary {
    #[serde(rename = "type", default = "summary_text_type")]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) text: String,
}

fn summary_text_type() -> String {
    "summary_text".to_string()
}

// === Usage ===

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ResponsesUsage {
    #[serde(default)]
    pub(crate) input_tokens: Option<i32>,
    #[serde(default)]
    pub(crate) output_tokens: Option<i32>,
    #[serde(default)]
    pub(crate) total_tokens: Option<i32>,
    #[serde(default)]
    pub(crate) input_tokens_details: Option<Value>,
    #[serde(default)]
    pub(crate) output_tokens_details: Option<Value>,
}

impl ResponsesUsage {
    pub(crate) fn into_turn_usage(self, cost: Option<f64>) -> TurnUsage {
        TurnUsage::from_responses(
            self.input_tokens,
            self.output_tokens,
            self.total_tokens,
            self.input_tokens_details,
            self.output_tokens_details,
            cost,
        )
    }
}

// === Response envelope ===

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ResponseEnvelope {
    #[serde(default)]
    pub(crate) id: Option<String>,
    #[serde(default)]
    pub(crate) model: Option<String>,
    #[serde(default)]
    pub(crate) status: Option<String>,
    #[serde(default)]
    pub(crate) output: Vec<ResponsesItem>,
    #[serde(default)]
    pub(crate) usage: Option<ResponsesUsage>,
    #[serde(default)]
    pub(crate) error: Option<ResponseError>,
    #[serde(default)]
    pub(crate) incomplete_details: Option<IncompleteDetails>,
    #[serde(default)]
    pub(crate) cost: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ResponseError {
    #[serde(default)]
    pub(crate) code: Option<String>,
    #[serde(default)]
    pub(crate) message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct IncompleteDetails {
    #[serde(default)]
    pub(crate) reason: Option<String>,
}

/// The finished response the adapter maps into a neutral generation.
#[derive(Debug, Clone, Default)]
pub(crate) struct ResponsesResponse {
    pub(crate) id: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) incomplete_reason: Option<String>,
    pub(crate) output: Vec<ResponsesItem>,
    pub(crate) usage: Option<ResponsesUsage>,
    pub(crate) cost: Option<f64>,
    pub(crate) streamed_text: bool,
}

impl ResponsesResponse {
    pub(crate) fn from_envelope(envelope: ResponseEnvelope) -> Self {
        let cost = super::wire::parse_cost(envelope.cost.as_ref());
        Self {
            id: envelope.id,
            model: envelope.model,
            status: envelope.status,
            incomplete_reason: envelope
                .incomplete_details
                .and_then(|details| details.reason),
            output: envelope.output,
            usage: envelope.usage,
            cost,
            streamed_text: false,
        }
    }
}

/// The gateway reports cost as a JSON string (`"cost":"0"`) where the rest of
/// the wire uses numbers, so both spellings are accepted.
pub(crate) fn parse_cost(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

// === Streaming events ===

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum ResponseStreamEvent {
    #[serde(rename = "response.created")]
    Created { response: ResponseEnvelope },
    #[serde(rename = "response.output_item.added")]
    OutputItemAdded {
        output_index: usize,
        item: ResponsesItem,
    },
    #[serde(rename = "response.output_item.done")]
    OutputItemDone {
        output_index: usize,
        item: ResponsesItem,
    },
    #[serde(rename = "response.output_text.delta")]
    OutputTextDelta { output_index: usize, delta: String },
    #[serde(rename = "response.function_call_arguments.delta")]
    FunctionArgumentsDelta { output_index: usize, delta: String },
    #[serde(rename = "response.reasoning_summary_text.delta")]
    ReasoningSummaryDelta { output_index: usize, delta: String },
    #[serde(rename = "response.completed")]
    Completed { response: ResponseEnvelope },
    #[serde(rename = "response.failed")]
    Failed { response: ResponseEnvelope },
    #[serde(rename = "response.incomplete")]
    Incomplete { response: ResponseEnvelope },
    #[serde(other)]
    Unknown,
}

// === Streaming aggregate ===

/// Rebuilds one response from its item and delta events.
#[derive(Debug, Default)]
pub(crate) struct StreamingResponse {
    pub(crate) id: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) incomplete_reason: Option<String>,
    pub(crate) usage: Option<ResponsesUsage>,
    pub(crate) cost: Option<f64>,
    /// Whether a terminal lifecycle event was actually observed.
    ///
    /// A chunked HTTP response can be closed by a proxy or upstream without
    /// producing a read error, so "the stream ended" is NOT evidence that the
    /// response finished. Without this, a truncated generation would be stored
    /// as a success and a half-assembled function call could be dispatched with
    /// no trustworthy status behind it.
    terminal: bool,
    /// Terminal `output`, kept only as a fallback for a stream that emitted no
    /// item events at all.
    terminal_output: Vec<ResponsesItem>,
    items: Vec<Option<ResponsesItem>>,
}

impl StreamingResponse {
    pub(crate) fn apply(&mut self, event: &ResponseStreamEvent) {
        match event {
            ResponseStreamEvent::Created { response } => self.absorb(response),
            ResponseStreamEvent::OutputItemAdded { output_index, item }
            | ResponseStreamEvent::OutputItemDone { output_index, item } => {
                self.set_item(*output_index, item.clone())
            }
            ResponseStreamEvent::OutputTextDelta {
                output_index,
                delta,
            } => {
                let slot = self.slot(*output_index);
                let item = slot.get_or_insert_with(|| ResponsesItem::Message {
                    id: None,
                    role: "assistant".to_string(),
                    content: Vec::new(),
                });
                if let ResponsesItem::Message { content, .. } = item {
                    match content.last_mut() {
                        Some(ContentPart::OutputText { text }) => text.push_str(delta),
                        _ => content.push(ContentPart::OutputText {
                            text: delta.clone(),
                        }),
                    }
                }
            }
            ResponseStreamEvent::FunctionArgumentsDelta {
                output_index,
                delta,
            } => {
                if let Some(ResponsesItem::FunctionCall { arguments, .. }) =
                    self.slot(*output_index).as_mut()
                {
                    arguments.push_str(delta);
                }
            }
            ResponseStreamEvent::ReasoningSummaryDelta {
                output_index,
                delta,
            } => {
                if let Some(ResponsesItem::Reasoning { summary, .. }) =
                    self.slot(*output_index).as_mut()
                {
                    match summary.last_mut() {
                        Some(part) => part.text.push_str(delta),
                        None => summary.push(ReasoningSummary {
                            kind: summary_text_type(),
                            text: delta.clone(),
                        }),
                    }
                }
            }
            ResponseStreamEvent::Completed { response }
            | ResponseStreamEvent::Failed { response }
            | ResponseStreamEvent::Incomplete { response } => {
                self.terminal = true;
                self.absorb(response);
            }
            ResponseStreamEvent::Unknown => {}
        }
    }

    /// Whether the response reported an outcome of its own. `incomplete` counts:
    /// it is a real, storable result (the output cap was reached), unlike a
    /// connection that simply stopped talking.
    pub(crate) fn saw_terminal(&self) -> bool {
        self.terminal
    }

    fn absorb(&mut self, response: &ResponseEnvelope) {
        if self.id.is_none() {
            self.id = response.id.clone();
        }
        if self.model.is_none() {
            self.model = response.model.clone();
        }
        if let Some(status) = &response.status {
            self.status = Some(status.clone());
        }
        if let Some(details) = &response.incomplete_details {
            self.incomplete_reason = details.reason.clone();
        }
        if response.usage.is_some() {
            self.usage = response.usage.clone();
        }
        if let Some(cost) = parse_cost(response.cost.as_ref()) {
            self.cost = Some(cost);
        }
        if !response.output.is_empty() {
            self.terminal_output = response.output.clone();
        }
    }

    fn slot(&mut self, index: usize) -> &mut Option<ResponsesItem> {
        while self.items.len() <= index {
            self.items.push(None);
        }
        &mut self.items[index]
    }

    /// Replace a slot, preserving text or arguments already accumulated for it.
    ///
    /// `output_item.done` re-sends the whole item, and a gateway that streamed
    /// the text through deltas may send the finished item with empty content;
    /// taking it verbatim would erase what was just streamed.
    fn set_item(&mut self, index: usize, incoming: ResponsesItem) {
        let slot = self.slot(index);
        let merged = match (slot.take(), incoming) {
            (
                Some(ResponsesItem::Message {
                    content: existing, ..
                }),
                ResponsesItem::Message { id, role, content },
            ) if content.is_empty() => ResponsesItem::Message {
                id,
                role,
                content: existing,
            },
            (
                Some(ResponsesItem::FunctionCall {
                    arguments: existing,
                    ..
                }),
                ResponsesItem::FunctionCall {
                    id,
                    call_id,
                    name,
                    arguments,
                },
            ) if arguments.is_empty() => ResponsesItem::FunctionCall {
                id,
                call_id,
                name,
                arguments: existing,
            },
            (_, incoming) => incoming,
        };
        *slot = Some(merged);
    }

    pub(crate) fn text(&self) -> String {
        self.output_items()
            .iter()
            .filter_map(|item| match item {
                ResponsesItem::Message { content, .. } => Some(
                    content
                        .iter()
                        .filter_map(ContentPart::text)
                        .collect::<String>(),
                ),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn reasoning_text(&self) -> String {
        self.output_items()
            .iter()
            .filter_map(|item| match item {
                ResponsesItem::Reasoning { summary, .. } => Some(
                    summary
                        .iter()
                        .map(|part| part.text.as_str())
                        .collect::<String>(),
                ),
                _ => None,
            })
            .collect()
    }

    /// The response's items, preferring what the stream built.
    fn output_items(&self) -> Vec<ResponsesItem> {
        let streamed: Vec<ResponsesItem> = self.items.iter().flatten().cloned().collect();
        if streamed.is_empty() {
            return self.terminal_output.clone();
        }
        streamed
    }

    pub(crate) fn into_response(self, streamed_text: bool) -> ResponsesResponse {
        let output = self.output_items();
        ResponsesResponse {
            id: self.id,
            model: self.model,
            status: self.status,
            incomplete_reason: self.incomplete_reason,
            output,
            usage: self.usage,
            cost: self.cost,
            streamed_text,
        }
    }
}
