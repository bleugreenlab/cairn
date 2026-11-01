//! The cairn-inference facet's resources: `cairn://inference/*`.
//!
//! Every record is Cairn's own and the operator writes it at the index: the
//! whole inventory replaced, or records added, replaced and removed by a
//! patch, each validated as a whole. Records mirrored from bginf before it
//! retired are adopted there too. The pool controller writes desired state: a
//! pool patch takes a pool under Cairn's control, sets its windows, and pins
//! or releases it; a requests append asks for a model. One deployment's own
//! resource is the act side: patching it starts or stops the deployment
//! through its head executor. Nothing here powers a machine.

use super::specs::*;
use super::types::*;

const DEPLOYMENT_ACTION: KeySpec = KeySpec::new("action", KeyType::Str, "start|stop");

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
        label: "pools: each pool's records and the controller's state",
        kind: ResourceKind::InferencePools,
        actions: true,
    },
    RelatedSpec {
        label: "requests: ask for a model and read the queue",
        kind: ResourceKind::InferenceRequests,
        actions: true,
    },
];

const INFERENCE_INDEX_RELATED: &[RelatedSpec] = INFERENCE_RELATED;

const INFERENCE_BACK: &[RelatedSpec] = &[
    RelatedSpec {
        label: "the inference index, where the records are written",
        kind: ResourceKind::Inference,
        actions: true,
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
    RelatedSpec {
        label: "requests",
        kind: ResourceKind::InferenceRequests,
        actions: false,
    },
];

const INFERENCE_CONTENT: KeySpec = KeySpec::new(
    "content",
    KeyType::Str,
    "the whole inventory in Cairn's record format, as cairn://inference?format=yaml reads it",
);

const INFERENCE_FORMAT: KeySpec = KeySpec::new("format", KeyType::Str, "yaml, the only format");

const INFERENCE_ADOPT: KeySpec = KeySpec::new(
    "adopt",
    KeyType::Str,
    "bginf: every record still mirrored from bginf becomes Cairn's own, keeping its id; alone in its patch",
);

const INFERENCE_UPSERT: KeySpec = KeySpec::new(
    "upsert",
    KeyType::Str,
    "records in Cairn's record format (any part of the ?format=yaml document), each added or replacing the record with its id",
);

const INFERENCE_REMOVE: KeySpec = KeySpec::new(
    "remove",
    KeyType::Object,
    "{recipes?, deployments?, pools?}: the ids of records to take out",
);

const INFERENCE_POOL: KeySpec = KeySpec::new("pool", KeyType::Str, "");

const INFERENCE_CONTROLLED: KeySpec = KeySpec::new(
    "controlled",
    KeyType::Bool,
    "true: the controller decides the pool's desired set, adopting what is healthy now; false: hands it back and cancels its waiting requests",
);

const INFERENCE_SETTINGS: KeySpec = KeySpec::new(
    "settings",
    KeyType::Object,
    "{modelIdleSecs, poolIdleSecs, minResidencySecs, loadBoundSecs}; null derives that setting from measured loads again",
);

const INFERENCE_PIN: KeySpec = KeySpec::new(
    "pin",
    KeyType::Array,
    "deployment ids held resident against idle unload ([] pins nothing); refused while another holder's pin stands, naming them",
);

const INFERENCE_RELEASE: KeySpec = KeySpec::new(
    "release",
    KeyType::Bool,
    "true releases your own pin; its set carries over to the controller",
);

const INFERENCE_REVOKE: KeySpec = KeySpec::new(
    "revoke",
    KeyType::Bool,
    "true releases the pin whoever holds it, naming them",
);

const INFERENCE_MODEL: KeySpec = KeySpec::new(
    "model",
    KeyType::Str,
    "a deployment id, or a recipe id, served name, model name or alias naming exactly one deployment",
);

const INFERENCE_REQUESTER: KeySpec = KeySpec::new(
    "requester",
    KeyType::Str,
    "who is asking, for a caller that is not an agent run; an agent run is always named by its home",
);

const INFERENCE_REQUEST_ID: KeySpec = KeySpec::new("id", KeyType::Int, "");

const INFERENCE_REQUEST_ACTION: KeySpec = KeySpec::new("action", KeyType::Str, "cancel");

pub(crate) const INFERENCE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Inference,
    uri_template: "cairn://inference",
    name: "Inference facet",
    description: "The cairn-inference facet: which model each inference machine should run. The record counts, one line per collection, and, while any is left, what remains of bginf: records still mirrored from it (read-only until adopted) and the retired link setting. Every record is Cairn's own and is written here: replace with the whole inventory (the inverse of ?format=yaml), or patch to add, replace and remove records. A write is validated as a whole and refused naming every problem, and may not remove or move a deployment Cairn is running or remove a pool the controller drives. Writing records needs the same authority as the `inference` settings section, since a recipe's start and stop commands run as shell on its head machine; adopting does not. Cairn starts and stops a deployment only when its own resource (`cairn://inference/deployments/{id}`) is patched or the controller asks, and powers nothing.",
    read_projections: &[ProjectionSpec { key: "format", values: "yaml — the whole inventory in Cairn's own record format" }],
    related: INFERENCE_INDEX_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Replace,
            required: &[INFERENCE_CONTENT],
            optional: &[INFERENCE_FORMAT],
            label: "replace the whole inventory with Cairn's own records, validated as a whole",
            example: r#"write({changes:[{target:"cairn://inference",mode:"replace",payload:{format:"yaml",content:"recipes: []\ndeployments: []\npools: []\n"}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[INFERENCE_ADOPT, INFERENCE_UPSERT, INFERENCE_REMOVE],
            label: "adopt the records mirrored from bginf, or add, replace and remove records",
            example: r#"write({changes:[{target:"cairn://inference",mode:"patch",payload:{adopt:"bginf"}}]})"#,
        },
    ],
};

pub(crate) const INFERENCE_RECIPES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceRecipes,
    uri_template: "cairn://inference/recipes",
    name: "Inference recipes",
    description: "Every recipe: the model and its pinned weight files, the served name, the upstream id catalog matching uses, the verbs it serves (unknown unless declared), the engine (a digest-pinned container, a pinned native artifact, or an operator's start and stop commands, shown in full), what the hardware must provide (machine count, accelerator, interconnect, memory per machine by kind, each stated as unknown when it is), and whose record it is. Recipes are written at cairn://inference.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_DEPLOYMENTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceDeployments,
    uri_template: "cairn://inference/deployments",
    name: "Inference deployments",
    description: "Every deployment: its recipe and pool, the machines it occupies as a gang (each marked as an enrolled executor or not), the head, the API base and health URL derived from the head machine's address and the port (or that the head's address is unknown), and its phase as Cairn controls it with a link to its own page, where it is started and stopped. Deployments are written at cairn://inference.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_DEPLOYMENT_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceDeployment,
    uri_template: "cairn://inference/deployments/{id}",
    name: "Inference deployment",
    description: "One deployment and what Cairn has done to it: its phase (never acted on, reserving with what it waits on, starting, healthy with how long the load took, stopping, stopped, or failed with the reason), each machine of its gang with whether Cairn holds it away from batch work and the GPU memory it holds now, the health URL, its last measured load and settled footprint, and its recent loads. A patch with action start reserves every machine of the gang at once, sends the recipe's start command to the head once each machine has the memory the deployment last needed, and watches the health URL; action stop runs the recipe's stop command on the head and waits for health to stop answering. Nothing but Cairn starts or stops it: a machine that still runs another controller's agent must have that agent stopped first.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[DEPLOYMENT_ACTION],
        optional: &[],
        label: "start or stop the deployment on its head machine",
        example: "write({changes:[{target:\"cairn://inference/deployments/dsv4f-tp2\",mode:\"patch\",payload:{action:\"start\"}}]})",
    }],
};

pub(crate) const INFERENCE_POOLS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferencePools,
    uri_template: "cairn://inference/pools",
    name: "Inference pools",
    description: "Every pool: whose record it is, its machines (enrolment and address), the pin its record holds and its default. A pool under Cairn's control also shows the pool controller's state: the desired set it writes, which slot won (pin, requested, default or nothing) and why, Cairn's pin and its holder, each member's phase and residency, the batches running on and waiting for the pool's machines, and the idle windows, minimum residency and load bound in force with where each came from. The controller loads a requested model, unloads an idle one when batches wait for the pool, and loads the default when the pool has had no batch work for its idle window; it writes desired state and never starts or stops an engine itself. A pool not under control is decided by nothing, and its deployments start and stop only from their own pages. The pool records themselves are written at cairn://inference.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[INFERENCE_POOL],
        optional: &[
            INFERENCE_CONTROLLED,
            INFERENCE_SETTINGS,
            INFERENCE_PIN,
            INFERENCE_RELEASE,
            INFERENCE_REVOKE,
        ],
        label: "control a pool: take or hand back control, set its windows, pin or release it",
        example: r#"write({changes:[{target:"cairn://inference/pools",mode:"patch",payload:{pool:"sparks",controlled:true,settings:{modelIdleSecs:1800}}}]})"#,
    }],
};

pub(crate) const INFERENCE_REQUESTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceRequests,
    uri_template: "cairn://inference/requests",
    name: "Inference requests",
    description: "The pool controller's queue of model requests. Append a model to ask for it: the request names exactly one deployment, which must be in a pool under Cairn's control, and waits until that deployment is healthy, fails to load, or is cancelled; a named model is never served by another. The page lists every waiting request with the model, the pool, the requester and what it waits on (builds draining from the pool's machines, an agent turn that is never preempted, another model unloading, the load itself, a pin, a loaded model inside its idle window or minimum residency, or an earlier request for the same machines), then the requests answered in the last week.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Append,
            required: &[INFERENCE_MODEL],
            optional: &[INFERENCE_REQUESTER],
            label: "request a model",
            example: r#"write({changes:[{target:"cairn://inference/requests",mode:"append",payload:{model:"dsv4f-tp2"}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[INFERENCE_REQUEST_ID, INFERENCE_REQUEST_ACTION],
            optional: &[],
            label: "cancel a waiting request",
            example: r#"write({changes:[{target:"cairn://inference/requests",mode:"patch",payload:{id:3,action:"cancel"}}]})"#,
        },
    ],
};
