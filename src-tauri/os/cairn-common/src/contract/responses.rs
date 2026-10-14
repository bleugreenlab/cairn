use super::specs::{DELETE_REASON, DESCRIPTION, NO_CROSS_ACTIONS, NO_PROJECTIONS, NO_RELATED};
use super::types::*;

const NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "display name; slugified into the response id",
);
const PROMPT: KeySpec = KeySpec::new(
    "prompt",
    KeyType::Str,
    "prompt template using declared {{variables}}",
);
const TIER: KeySpec = KeySpec::new("tier", KeyType::Str, "model tier; defaults to sm");
const MODEL: KeySpec = KeySpec::new("model", KeyType::Str, "exact model id, overriding tier");
const BACKEND: KeySpec = KeySpec::new("backend", KeyType::Str, "backend to complete through");
const OPTIONS: KeySpec = KeySpec::new("options", KeyType::Object, "per-call completion options");
const VARIABLES: KeySpec = KeySpec::new("variables", KeyType::Array, "declared template variables");
const OUTPUT: KeySpec = KeySpec::new(
    "output",
    KeyType::Any,
    "text, a preset schema name, or an inline JSON Schema",
);
const TIMEOUT: KeySpec = KeySpec::new("timeout", KeyType::Str, "completion timeout");
const EXAMPLES: KeySpec =
    KeySpec::new("examples", KeyType::Array, "few-shot input/output examples");
const NONE: &[MutationSpec] = &[];

const fn collection(kind: ResourceKind, uri: &'static str, name: &'static str) -> ResourceContract {
    ResourceContract { kind, uri_template: uri, name, description: "Named one-shot model completion definitions", read_projections: NO_PROJECTIONS, related: NO_RELATED, cross_actions: NO_CROSS_ACTIONS, mutations: &[MutationSpec { mode: ChangeMode::Create, required: &[NAME, DESCRIPTION, PROMPT], optional: &[TIER, MODEL, BACKEND, OPTIONS, VARIABLES, OUTPUT, TIMEOUT, EXAMPLES], label: "create response", example: "write({changes:[{target:\"cairn://responses\",mode:\"create\",payload:{name:\"...\",description:\"...\",prompt:\"...\"}}]})" }] }
}
const fn member(kind: ResourceKind, uri: &'static str, name: &'static str) -> ResourceContract {
    ResourceContract { kind, uri_template: uri, name, description: "A named one-shot model completion definition", read_projections: NO_PROJECTIONS, related: NO_RELATED, cross_actions: NO_CROSS_ACTIONS, mutations: &[MutationSpec { mode: ChangeMode::Patch, required: &[], optional: &[NAME, DESCRIPTION, PROMPT, TIER, MODEL, BACKEND, OPTIONS, VARIABLES, OUTPUT, TIMEOUT, EXAMPLES], label: "patch response", example: "write({changes:[{target:\"cairn://responses/ID\",mode:\"patch\",payload:{prompt:\"...\"}}]})" }, MutationSpec { mode: ChangeMode::Delete, required: &[], optional: &[DELETE_REASON], label: "delete response", example: "write({changes:[{target:\"cairn://responses/ID\",mode:\"delete\"}]})" }] }
}
const fn readonly(
    kind: ResourceKind,
    uri: &'static str,
    name: &'static str,
    description: &'static str,
) -> ResourceContract {
    ResourceContract {
        kind,
        uri_template: uri,
        name,
        description,
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NONE,
    }
}
pub(crate) const RESPONSES_CONTRACT: ResourceContract =
    collection(ResourceKind::Responses, "cairn://responses", "Responses");
pub(crate) const RESPONSE_CONTRACT: ResourceContract = member(
    ResourceKind::Response,
    "cairn://responses/{response_id}",
    "Response",
);
pub(crate) const RESPONSE_HISTORY_CONTRACT: ResourceContract = readonly(
    ResourceKind::ResponseHistory,
    "cairn://responses/{response_id}/history",
    "Response history",
    "Recent durable invocations of a response",
);
pub(crate) const RESPONSE_HISTORY_ENTRY_CONTRACT: ResourceContract = readonly(
    ResourceKind::ResponseHistoryEntry,
    "cairn://responses/{response_id}/history/{seq}",
    "Response history entry",
    "One durable response history entry, including its rendered prompt and outcome",
);
pub(crate) const PROJECT_RESPONSES_CONTRACT: ResourceContract = collection(
    ResourceKind::ProjectResponses,
    "cairn://p/{project}/responses",
    "Project responses",
);
pub(crate) const PROJECT_RESPONSE_CONTRACT: ResourceContract = member(
    ResourceKind::ProjectResponse,
    "cairn://p/{project}/responses/{response_id}",
    "Project response",
);
pub(crate) const PROJECT_RESPONSE_HISTORY_CONTRACT: ResourceContract = readonly(
    ResourceKind::ProjectResponseHistory,
    "cairn://p/{project}/responses/{response_id}/history",
    "Project response history",
    "Recent durable invocations of a project response",
);
pub(crate) const PROJECT_RESPONSE_HISTORY_ENTRY_CONTRACT: ResourceContract = readonly(
    ResourceKind::ProjectResponseHistoryEntry,
    "cairn://p/{project}/responses/{response_id}/history/{seq}",
    "Project response history entry",
    "One durable project response history entry",
);
