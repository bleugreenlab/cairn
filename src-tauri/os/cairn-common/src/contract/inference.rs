//! The cairn-inference facet's resources: `cairn://inference/*`.
//!
//! Read-only. Every record today mirrors bginf's control service, which Cairn
//! reads with GET requests only; nothing here starts, stops, pins or powers.

use super::specs::*;
use super::types::*;

const INFERENCE_RELATED: &[RelatedSpec] = &[
    RelatedSpec {
        label: "recipes: how each model is run",
        kind: ResourceKind::InferenceRecipes,
        actions: false,
    },
    RelatedSpec {
        label: "deployments: recipes placed on machines",
        kind: ResourceKind::InferenceDeployments,
        actions: false,
    },
    RelatedSpec {
        label: "pools: Cairn's resolution beside bginf's",
        kind: ResourceKind::InferencePools,
        actions: false,
    },
];

const INFERENCE_INDEX_RELATED: &[RelatedSpec] = INFERENCE_RELATED;

const INFERENCE_BACK: &[RelatedSpec] = &[
    RelatedSpec {
        label: "the inference index and the bginf link's status",
        kind: ResourceKind::Inference,
        actions: false,
    },
    RelatedSpec {
        label: "recipes",
        kind: ResourceKind::InferenceRecipes,
        actions: false,
    },
    RelatedSpec {
        label: "deployments",
        kind: ResourceKind::InferenceDeployments,
        actions: false,
    },
    RelatedSpec {
        label: "pools",
        kind: ResourceKind::InferencePools,
        actions: false,
    },
];

pub(crate) const INFERENCE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Inference,
    uri_template: "cairn://inference",
    name: "Inference facet",
    description: "The cairn-inference facet: which model each inference machine should run. The bginf link's status (configured or not, when bginf was last read and how long ago, whether it is unreachable now, the last change an import made and what it refused), the record counts, and one line per collection. Every record today mirrors bginf and is read-only; Cairn sends bginf only GET /profiles and GET /state and starts, stops, and powers nothing. Link bginf with `cairn://settings` `inference.bginf` and a `bginf` account holding ctl's token.",
    read_projections: &[ProjectionSpec { key: "format", values: "yaml — the whole inventory in Cairn's own record format" }],
    related: INFERENCE_INDEX_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_RECIPES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceRecipes,
    uri_template: "cairn://inference/recipes",
    name: "Inference recipes",
    description: "Every recipe: the model and its pinned weight files, the served name, the upstream id catalog matching uses, the verbs it serves (unknown on imports), the engine (a digest-pinned container, a pinned native artifact, or an operator's start and stop commands, shown in full), what the hardware must provide (machine count, accelerator, interconnect, memory per machine by kind, each stated as unknown when it is), and whose record it is.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_DEPLOYMENTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceDeployments,
    uri_template: "cairn://inference/deployments",
    name: "Inference deployments",
    description: "Every deployment: its recipe and pool, the machines it occupies as a gang (each marked as an enrolled executor or not), the head, and the API base and health URL derived from the head machine's address and the port (or that the head's address is unknown).",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_POOLS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferencePools,
    uri_template: "cairn://inference/pools",
    name: "Inference pools",
    description: "Every pool: its machines (enrolment, address, and power as bginf reports it), the pin and default mirrored from bginf, Cairn's resolved desired set and reason beside bginf's desired, reason, running and converged, and a verdict: agree, or DISAGREE with each difference. Both sides are computed from one bginf read, whose time and age are shown. A disagreement is shown, never hidden or corrected; when bginf is unreachable the page says since when and why, and shows the last successful read with its age.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};
