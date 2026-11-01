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
        label: "models: registered by their Hugging Face name",
        kind: ResourceKind::InferenceModels,
        actions: true,
    },
    RelatedSpec {
        label: "verifications: what loading each recipe on a pool measured",
        kind: ResourceKind::InferenceVerifications,
        actions: false,
    },
    RelatedSpec {
        label: "recipes: how each model is run; import or generate one",
        kind: ResourceKind::InferenceRecipes,
        actions: true,
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
        label: "models",
        kind: ResourceKind::InferenceModels,
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
    "{models?, recipes?, deployments?, pools?}: the ids of records to take out",
);

const INFERENCE_POOL: KeySpec = KeySpec::new("pool", KeyType::Str, "");

const INFERENCE_WARM: KeySpec = KeySpec::new(
    "warm",
    KeyType::Str,
    "the model (real name) the pool loads when idle, held as the pool record's default; null warms nothing",
);

const INFERENCE_REQUEST_POOL: KeySpec = KeySpec::new(
    "pool",
    KeyType::Str,
    "the pool to load it on, when the model is deployed in more than one",
);

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
    "the model's real name, its Hugging Face repository id (deepseek-ai/DeepSeek-V4-Flash-Vision-Exp); a deployment id, recipe id or served name names no model",
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

const INFERENCE_IMPORT: KeySpec = KeySpec::new(
    "import",
    KeyType::Object,
    "{url, commit, compose, service, model, envFiles?, env?, ranks?, hostPaths?, weights?, image?, id?}: compile a GitHub compose service at a pinned commit; a refusal lists every key the files cannot supply",
);

const INFERENCE_GENERATE: KeySpec = KeySpec::new(
    "generate",
    KeyType::Object,
    "{model, pool, hardware?}: candidate recipes for a registered model on a pool, from its machines' reported hardware (hardware: {<machine>: {family, memoryGib, unified, arch}} for one that reports none)",
);

const INFERENCE_VERIFY: KeySpec = KeySpec::new(
    "verify",
    KeyType::Object,
    "{pool}: load it on the pool, check health, benchmark, and record memory per machine, load time and speed; answers a verification id at once",
);

const INFERENCE_MODEL_VERIFY: KeySpec = KeySpec::new(
    "verify",
    KeyType::Object,
    "{pool, all?}: verify the model's recipes that suit the pool, imported first, stopping at the first pass unless all",
);

const INFERENCE_REFRESH: KeySpec = KeySpec::new(
    "refresh",
    KeyType::Bool,
    "true reads the model's config again at its current revision",
);

const INFERENCE_REPO: KeySpec = KeySpec::new(
    "repo",
    KeyType::Str,
    "a Hugging Face repository id; a quantized repository registers the model it quantizes",
);

const INFERENCE_REVISION: KeySpec =
    KeySpec::new("revision", KeyType::Str, "a sha or ref; main when absent");

pub(crate) const INFERENCE_RECIPES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceRecipes,
    uri_template: "cairn://inference/recipes",
    name: "Inference recipes",
    description: "Every recipe: the model and its pinned weight files, the quantization, where it came from (authored, imported from a compose file at a commit, or generated from a template), the served name, the verbs it serves (unknown unless declared), the engine (a digest-pinned container with its server, arguments, env per rank, mounts and host options; a pinned native artifact; or an operator's start and stop commands, shown in full), and what the hardware must provide. Append an import to compile a published compose service into a recipe, or a generate request to have Cairn write candidates for a registered model on a pool; both are verified before they are trusted. Recipes are also written as records at cairn://inference.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[],
        optional: &[INFERENCE_IMPORT, INFERENCE_GENERATE],
        label: "import a recipe from a compose file, or generate candidates for a model on a pool",
        example: r#"write({changes:[{target:"cairn://inference/recipes",mode:"append",payload:{generate:{model:"Qwen/Qwen3-Embedding-4B",pool:"bbs"}}}]})"#,
    }],
};

pub(crate) const INFERENCE_RECIPE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceRecipe,
    uri_template: "cairn://inference/recipes/{id}",
    name: "Inference recipe",
    description: "One recipe in full, with the scripts its head would run rendered for each deployment of it, and every verification of it per pool (stale once the recipe has changed). Patch it with verify to load it on a pool as deployment verify-<recipe>-<pool>, check its health, benchmark it and record what it measured; the pool's own model is stopped first and restored after, and a pool pinned by anyone else is refused.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[INFERENCE_VERIFY],
        optional: &[],
        label: "verify the recipe on a pool",
        example: r#"write({changes:[{target:"cairn://inference/recipes/qwen3-embedding-4b-q8_0-llamacpp-x1",mode:"patch",payload:{verify:{pool:"halo"}}}]})"#,
    }],
};

pub(crate) const INFERENCE_MODELS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceModels,
    uri_template: "cairn://inference/models",
    name: "Inference models",
    description: "Every model, by its real Hugging Face name: architecture, parameters, dtype or quantization, context, modalities and verbs as its config states them (unknown where it does not), the revision they were read at, and its recipes. Append a repository id to register a model; a quantized repository registers the model it quantizes, since its files are the weights of one of that model's recipes.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[INFERENCE_REPO],
        optional: &[INFERENCE_REVISION],
        label: "register a model by its Hugging Face name",
        example: r#"write({changes:[{target:"cairn://inference/models",mode:"append",payload:{repo:"Qwen/Qwen3.6-35B-A3B"}}]})"#,
    }],
};

pub(crate) const INFERENCE_MODEL_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceModel,
    uri_template: "cairn://inference/models/{owner}/{name}",
    name: "Inference model",
    description: "One model: its facts and the revision they were read at, and each recipe that serves it with its quantization, server, machine count, origin and latest verification per pool. Patch it to read its config again or to verify its recipes on a pool; delete it once no recipe serves it.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[INFERENCE_REFRESH, INFERENCE_MODEL_VERIFY],
            label: "read the model's config again, or verify its recipes on a pool",
            example: r#"write({changes:[{target:"cairn://inference/models/Qwen/Qwen3.6-35B-A3B",mode:"patch",payload:{verify:{pool:"sparks"}}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Delete,
            required: &[],
            optional: &[],
            label: "remove the model; refused while a recipe serves it",
            example: r#"write({changes:[{target:"cairn://inference/models/Qwen/Qwen3.6-35B-A3B",mode:"delete"}]})"#,
        },
    ],
};

pub(crate) const INFERENCE_VERIFICATIONS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceVerifications,
    uri_template: "cairn://inference/verifications",
    name: "Inference verifications",
    description: "Every verification of a recipe on a pool, newest first: the outcome, the stage it reached and why it failed, its load time, the memory each machine held, the context and KV cache the engine reported, time to first token, decode, prefill and embedding speeds, and whether its recipe has changed since. Verifications are started by patching a recipe or a model.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_VERIFICATION_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceVerification,
    uri_template: "cairn://inference/verifications/{id}",
    name: "Inference verification",
    description: "One verification: its recipe, pool and machines, who asked, when it started and finished, the stage it reached, its outcome and reason, and everything it measured.",
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
            INFERENCE_WARM,
            INFERENCE_CONTROLLED,
            INFERENCE_SETTINGS,
            INFERENCE_PIN,
            INFERENCE_RELEASE,
            INFERENCE_REVOKE,
        ],
        label: "set a pool's warm model, or control it: take or hand back control, set its windows, pin or release it",
        example: r#"write({changes:[{target:"cairn://inference/pools",mode:"patch",payload:{pool:"sparks",controlled:true,settings:{modelIdleSecs:1800}}}]})"#,
    }],
};

pub(crate) const INFERENCE_REQUESTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceRequests,
    uri_template: "cairn://inference/requests",
    name: "Inference requests",
    description: "The pool controller's queue of model requests. Append a model, by its real name, to ask for it: the name (and the pool, when the model is deployed in several) must mean exactly one deployment, which must be in a pool under Cairn's control, and waits until that deployment is healthy, fails to load, or is cancelled; a named model is never served by another. The page lists every waiting request with the model, the pool, the requester and what it waits on (builds draining from the pool's machines, an agent turn that is never preempted, another model unloading, the load itself, a pin, a loaded model inside its idle window or minimum residency, or an earlier request for the same machines), then the requests answered in the last week.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Append,
            required: &[INFERENCE_MODEL],
            optional: &[INFERENCE_REQUEST_POOL, INFERENCE_REQUESTER],
            label: "request a model",
            example: r#"write({changes:[{target:"cairn://inference/requests",mode:"append",payload:{model:"deepseek-ai/DeepSeek-V4-Flash-Vision-Exp"}}]})"#,
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
