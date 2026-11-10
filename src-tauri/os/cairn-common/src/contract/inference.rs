//! The cairn-inference facet's resources: `cairn://inference/*`.
//!
//! Every record is Cairn's own and the operator writes it at the index: the
//! whole inventory replaced, or records added, replaced and removed by a
//! patch, each validated as a whole. Records mirrored from bginf before it
//! retired are adopted there too. Which model runs is placement's: a pool
//! patch sets a detected pool's warm model and idle window and pins or
//! releases a model there; a requests append asks for a model. One
//! deployment's own resource starts or stops it by hand. Nothing here powers a
//! machine.

use super::specs::*;
use super::types::*;
use cairn_uri::ResourceKind;

const DEPLOYMENT_ACTION: KeySpec = KeySpec::new("action", KeyType::Str, "start|stop");

const INFERENCE_RELATED: &[RelatedSpec] = &[
    RelatedSpec {
        label: "models: registered by their Hugging Face name",
        kind: ResourceKind::InferenceModels,
        actions: true,
    },
    RelatedSpec {
        label: "verifications: what loading each profile on a pool measured",
        kind: ResourceKind::InferenceVerifications,
        actions: false,
    },
    RelatedSpec {
        label: "profiles: how each model is run; import or generate one",
        kind: ResourceKind::InferenceProfiles,
        actions: true,
    },
    RelatedSpec {
        label: "deployments: profiles placed on machines",
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
        label: "profiles",
        kind: ResourceKind::InferenceProfiles,
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
    "{models?, profiles?, deployments?, machines?}: the ids of records to take out",
);

const INFERENCE_POOL: KeySpec = KeySpec::new(
    "pool",
    KeyType::Str,
    "a detected pool's key: its machine names, sorted, joined with +",
);

const INFERENCE_WARM: KeySpec = KeySpec::new(
    "warm",
    KeyType::Str,
    "the model, by its real name, placed when the pool has had no batch work for its idle window; null clears it",
);

const INFERENCE_IDLE_SECS: KeySpec = KeySpec::new(
    "idleSecs",
    KeyType::Int,
    "the pool's idle window in seconds, for eviction and the warm model alike; null derives it from the pool's measured load again",
);

const INFERENCE_PIN: KeySpec = KeySpec::new(
    "pin",
    KeyType::Str,
    "a model, by its real name, to keep resident on the pool: a standing lease, acquired at once and never evicted for room; refused while another holder's pin holds its machines, naming them",
);

const INFERENCE_RELEASE: KeySpec = KeySpec::new(
    "release",
    KeyType::Str,
    "true releases your pins on the pool, or name the model; the model stays, its residency counted from the pin",
);

const INFERENCE_REVOKE: KeySpec =
    KeySpec::new("revoke", KeyType::Str, "as release, for anyone's pin");

const INFERENCE_MODEL: KeySpec = KeySpec::new(
    "model",
    KeyType::Str,
    "the model's real name, its Hugging Face repository id (deepseek-ai/DeepSeek-V4-Flash-Vision-Exp); a deployment id, profile id or served name names no model; never served by another model",
);

const INFERENCE_REQUEST_POOL: KeySpec = KeySpec::new(
    "pool",
    KeyType::Str,
    "narrows which pool's deployment serves it, by the pool's key",
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
    description: "The cairn-inference facet: how each local model is run and where; which runs when is placement's (cairn://inference/pools). The record counts, one line per collection, and, while any is left, what remains of bginf: records still mirrored from it (read-only until adopted) and the retired link setting. Every record is Cairn's own and is written here: replace with the whole inventory (the inverse of ?format=yaml), or patch to add, replace and remove records. A write is validated as a whole and refused naming every problem, and may not remove or move a deployment whose model occupies its machines. Writing records needs the same authority as the `inference` settings section, since a profile's start and stop commands run as shell on its head machine; adopting does not. Placement starts and stops deployments as models are requested, evicted, pinned or warmed, and by hand on a deployment's own resource (`cairn://inference/deployments/{id}`); Cairn powers nothing.",
    read_projections: &[ProjectionSpec { key: "format", values: "yaml — the whole inventory in Cairn's own record format" }],
    related: INFERENCE_INDEX_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Replace,
            required: &[INFERENCE_CONTENT],
            optional: &[INFERENCE_FORMAT],
            label: "replace the whole inventory with Cairn's own records, validated as a whole",
            example: r#"write({changes:[{target:"cairn://inference",mode:"replace",payload:{format:"yaml",content:"profiles: []\ndeployments: []\nmachines: []\n"}}]})"#,
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
    "{url, commit, compose, service, model, envFiles?, env?, ranks?, hostPaths?, weights?, image?, id?, preview?}: compile a GitHub compose service at a pinned commit; a refusal lists every key the files cannot supply. preview:true writes nothing and answers the candidate profile, its source commit and notes, with secret-named variables kept as ${NAME} references",
);

const INFERENCE_GENERATED: KeySpec = KeySpec::new(
    "generated",
    KeyType::Object,
    "{profile, sources?[{url, title}], answers?[{question, answer}], startingTemplate?}: a profile generation's draft, written only by that generation's bound ordinary agent; a digest-pinned container profile of its model, its origin stamped from the writer",
);

const INFERENCE_VERIFY: KeySpec = KeySpec::new(
    "verify",
    KeyType::Object,
    "{pool}: load it on the pool, check health, benchmark, and record memory per machine, load time and speed; answers the verification id at once, as data {verification}",
);

const INFERENCE_OUTCOME: KeySpec = KeySpec::new(
    "outcome",
    KeyType::Object,
    "{outcome: verified|cannotRun|gaveUp, verification?, why?, measured?, sources?, attempts?}: how the generation ended, from its bound ordinary agent; verified requires the server's exact revision-bound verification pass on its pool, regardless of caller-supplied proof, and any other ending removes the draft",
);

const INFERENCE_APPROVE: KeySpec = KeySpec::new(
    "approve",
    KeyType::Object,
    "{} and nothing else: Cairn raises the operator's approval question itself, showing the draft's runnable surface and its sha256, and returns the same pending question when asked again; once the operator has answered, asking again records an Approve, which verifying that revision requires. Only the operator's own answer approves.",
);

const FORMAT_JSON: ProjectionSpec = ProjectionSpec {
    key: "format",
    values: "json — the same answer as data",
};

const GENERATION_HARDWARE: ProjectionSpec = ProjectionSpec {
    key: "generation",
    values: "a generation's id: reads its pool with the hardware its operator stated for machines that report none",
};

const FIT_MODEL: ProjectionSpec = ProjectionSpec {
    key: "model",
    values: "a registered model's real name",
};

const FIT_POOL: ProjectionSpec = ProjectionSpec {
    key: "pool",
    values: "a detected pool's key, or its machines joined with + in any order",
};

const FIT_SOURCE: ProjectionSpec = ProjectionSpec {
    key: "source",
    values: "a weight source's key (<repo>@<quantization>) as models/<name>/files lists it; every source when absent",
};

const TEMPLATE_PROJECTOR: ProjectionSpec = ProjectionSpec {
    key: "projector",
    values: "a projector's file path, for llama.cpp; the one at the weights' precision when the model takes images and none is named",
};

const INFERENCE_MODEL_VERIFY: KeySpec = KeySpec::new(
    "verify",
    KeyType::Object,
    "{pool, all?}: verify the model's profiles that suit the pool, imported first, stopping at the first pass unless all",
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

pub(crate) const INFERENCE_PROFILES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceProfiles,
    uri_template: "cairn://inference/profiles",
    name: "Inference profiles",
    description: "Every profile: the model and its pinned weight files, the quantization, where it came from (authored, imported from a compose file at a commit, or generated by an agent session with its run, the sources it cited and the operator's answers), the served name, the verbs it serves (unknown unless declared), the engine (a digest-pinned container with its server, arguments, env per rank, mounts and host options; a pinned native artifact; or an operator's start and stop commands, shown in full), and what the hardware must provide. Append an import to compile a published compose service into a profile; a profile generation (started from Settings > Inference, cairn://inference/generations) appends its draft as generated. Either is verified before it is trusted. Profiles are also written as records at cairn://inference.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[],
        optional: &[INFERENCE_IMPORT, INFERENCE_GENERATED],
        label: "import a profile from a compose file, or write a generation's draft",
        example: r#"write({changes:[{target:"cairn://inference/profiles",mode:"append",payload:{import:{url:"https://github.com/o/r",commit:"<40-hex>",compose:"compose.yaml",service:"vllm",model:"Qwen/Qwen3-Embedding-4B"}}}]})"#,
    }],
};

pub(crate) const INFERENCE_PROFILE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceProfile,
    uri_template: "cairn://inference/profiles/{id}",
    name: "Inference profile",
    description: "One profile in full, with the scripts its head would run rendered for each deployment of it, and every verification of it per pool (stale once the profile has changed). Patch it with verify to load it on a pool as deployment verify-<profile>-<pool>, check its health, benchmark it and record what it measured; the pool's own model is stopped first and restored after, and a pool pinned by anyone else is refused.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[INFERENCE_VERIFY],
        optional: &[],
        label: "verify the profile on a pool",
        example: r#"write({changes:[{target:"cairn://inference/profiles/qwen3-embedding-4b-q8_0-llamacpp-x1",mode:"patch",payload:{verify:{pool:"halo"}}}]})"#,
    }],
};

pub(crate) const INFERENCE_MODELS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceModels,
    uri_template: "cairn://inference/models",
    name: "Inference models",
    description: "Every model, by its real Hugging Face name: architecture, parameters, dtype or quantization, context, modalities and verbs as its config states them (unknown where it does not), the revision they were read at, and its profiles. Append a repository id to register a model; a quantized repository registers the model it quantizes, since its files are the weights of one of that model's profiles.",
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
    description: "One model: its facts and the revision they were read at, and each profile that serves it with its quantization, server, machine count, origin and latest verification per pool. Patch it to read its config again or to verify its profiles on a pool; delete it once no profile serves it.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[INFERENCE_REFRESH, INFERENCE_MODEL_VERIFY],
            label: "read the model's config again, or verify its profiles on a pool",
            example: r#"write({changes:[{target:"cairn://inference/models/Qwen/Qwen3.6-35B-A3B",mode:"patch",payload:{verify:{pool:"sparks"}}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Delete,
            required: &[],
            optional: &[],
            label: "remove the model; refused while a profile serves it",
            example: r#"write({changes:[{target:"cairn://inference/models/Qwen/Qwen3.6-35B-A3B",mode:"delete"}]})"#,
        },
    ],
};

pub(crate) const INFERENCE_VERIFICATIONS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceVerifications,
    uri_template: "cairn://inference/verifications",
    name: "Inference verifications",
    description: "Every verification of a profile on a pool, newest first: the outcome, the stage it reached and why it failed, its load time, the memory each machine held, the context and KV cache the engine reported, time to first token, decode, prefill and embedding speeds, and whether its profile has changed since. Verifications are started by patching a profile or a model.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_VERIFICATION_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceVerification,
    uri_template: "cairn://inference/verifications/{id}",
    name: "Inference verification",
    description: "One verification: its profile, pool and machines, who asked, when it started and finished, the stage it reached, its outcome and reason, and everything it measured.",
    read_projections: &[FORMAT_JSON],
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_MODEL_FILES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceModelFiles,
    uri_template: "cairn://inference/models/{owner}/{name}/files",
    name: "Inference model files",
    description: "A registered model's repositories (its own and the quantized derivatives its organisation or a trusted quantizer publishes), their files sorted by what they are to it: weight sources keyed <repo>@<quantization> (every safetensors file of a repository; each GGUF quantization, its split shards grouped and their sizes summed), vision projectors (mmproj-*, which llama.cpp loads with --mmproj), speculative-decoding draft heads (mtp-*, dflash-*, *draft*; never a source, and deliberately unused), and anything else.",
    read_projections: &[FORMAT_JSON],
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_POOL_HARDWARE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferencePoolHardware,
    uri_template: "cairn://inference/pools/{pool}/hardware",
    name: "Inference pool hardware",
    description: "A detected pool's hardware exactly as the fit arithmetic reads it: each machine's accelerator family and GPU, whether its memory is unified or discrete and how much, what an engine may take of it, its CPU architecture, whether its GPU is Blackwell, its address and fast-link address, and the link joining the pool. Executors do not report driver or CUDA versions.",
    read_projections: &[FORMAT_JSON, GENERATION_HARDWARE],
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_FIT_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceFit,
    uri_template: "cairn://inference/fit",
    name: "Inference fit",
    description: "Whether a model's weight source fits a pool: the machines it needs (each carrying weights/k x 1.05 plus 2 GiB, in 0.90 of a discrete card's VRAM or 0.85 of unified memory; more than one needs a linked pool and k dividing the attention heads), each machine's share, and the context the memory left over holds in KV cache, or why it does not fit. A prediction: verification decides.",
    read_projections: &[FIT_MODEL, FIT_POOL, FIT_SOURCE, GENERATION_HARDWARE, FORMAT_JSON],
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_TEMPLATES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceTemplates,
    uri_template: "cairn://inference/templates",
    name: "Inference templates",
    description: "The engines Cairn ships as known-good starting points for a profile: each image pinned by digest, the server it runs, the accelerator (nvidia or amd, on cuda, rocm or vulkan) and CPU it serves, the weight formats it reads, its command and what its research found. With a pool, only those that serve the pool's accelerator and CPU; a pool whose accelerator is unknown is refused naming why. A starting point, not a rule.",
    read_projections: &[FIT_POOL, GENERATION_HARDWARE, FORMAT_JSON],
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_TEMPLATE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceTemplate,
    uri_template: "cairn://inference/templates/{id}",
    name: "Inference template",
    description: "One template, and with model, pool and source, that template made into a whole profile as Cairn's record YAML: every shard pinned, the context and machine count the fit predicts, tensor parallelism across a linked pool, a projector wired with --mmproj for a model that takes images, and the memory it needs per machine. A starting point to keep or change.",
    read_projections: &[FIT_MODEL, FIT_POOL, FIT_SOURCE, TEMPLATE_PROJECTOR, GENERATION_HARDWARE],
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_GENERATIONS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceGenerations,
    uri_template: "cairn://inference/generations",
    name: "Inference profile generations",
    description: "Every profile generation, newest first: its ordinary issue, model and pool, bound agent, authorization state, candidate budget, draft revision, approval, terminal outcome and pending cleanup. Conversation, questions and execution progress belong to the ordinary issue. Generation starts from Settings > Inference; verification decides whether its draft is promoted.",
    read_projections: &[FORMAT_JSON],
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_GENERATION_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceGeneration,
    uri_template: "cairn://inference/generations/{id}",
    name: "Inference profile generation",
    description: "One generation's domain ledger: its ordinary issue and bound agent, state, model and pool, candidate budget, current draft revision and runnable surface, approval, outcome and pending cleanup. Its bound agent requests a server-rendered operator approval question or reports an outcome; conversation and execution progress remain on the ordinary issue.",
    read_projections: &[FORMAT_JSON],
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[],
        optional: &[INFERENCE_APPROVE, INFERENCE_OUTCOME],
        label: "end the generation with its outcome, or raise and then record the operator's approval of its draft (its own agent only)",
        example: r#"write({changes:[{target:"cairn://inference/generations/ID",mode:"patch",payload:{outcome:{outcome:"cannotRun",why:"..."}}}]})"#,
    }],
};

pub(crate) const INFERENCE_DEPLOYMENTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceDeployments,
    uri_template: "cairn://inference/deployments",
    name: "Inference deployments",
    description: "Every deployment: its profile and pool, the machines it occupies as a gang (each marked as an enrolled executor or not), the head, the API base and health URL derived from the head machine's address and the port (or that the head's address is unknown), and its phase as Cairn controls it with a link to its own page, where it is started and stopped. Deployments are written at cairn://inference.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const INFERENCE_DEPLOYMENT_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceDeployment,
    uri_template: "cairn://inference/deployments/{id}",
    name: "Inference deployment",
    description: "One deployment and what Cairn has done to it: its phase (never acted on, reserving with what it waits on, starting, healthy with how long the load took, stopping, stopped, or failed with the reason), each machine of its gang with whether Cairn holds it away from batch work and the GPU memory it holds now, the health URL, its last measured load and settled footprint, and its recent loads. A patch with action start reserves every machine of the gang at once, sends the profile's start command to the head once each machine has the memory the deployment last needed, and watches the health URL; action stop runs the profile's stop command on the head and waits for health to stop answering. Nothing but Cairn starts or stops it: a machine that still runs another controller's agent must have that agent stopped first.",
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
    description: "The pools placement detects from the links executors report (machines joined by a fast link; a GPU machine with none is a pool of its own): each pool's machines and link, the models resident on it with their phase, footprint, why each is there and why it stays or that it is idle, its warm model, its idle window, minimum residency and load bound with where each came from, and the deployments placement cannot run and why. A loading model keeps its unmet footprint off batches; a healthy one runs beside them; an idle unpinned one is evicted when work is refused its room; an idle pool places its warm model. Patch a pool to set its warm model and idle window, or to pin, release or revoke a model there.",
    read_projections: NO_PROJECTIONS,
    related: INFERENCE_BACK,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[INFERENCE_POOL],
        optional: &[
            INFERENCE_WARM,
            INFERENCE_IDLE_SECS,
            INFERENCE_PIN,
            INFERENCE_RELEASE,
            INFERENCE_REVOKE,
        ],
        label: "set a pool's warm model and idle window, or pin, release or revoke a model there",
        example: r#"write({changes:[{target:"cairn://inference/pools",mode:"patch",payload:{pool:"bginf-sp1+bginf-sp2",warm:"deepseek-ai/DeepSeek-V4-Flash",idleSecs:1800}}]})"#,
    }],
};

pub(crate) const INFERENCE_REQUESTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::InferenceRequests,
    uri_template: "cairn://inference/requests",
    name: "Inference requests",
    description: "Placement's model requests. Append a model, by its real name, to ask for it: the request leases one deployment of it, loading it if it is absent, and waits until it is healthy, fails to load, or is cancelled; a model is never served by another. The page lists every waiting request with the model, the pool, the requester and what it waits on (the gang fitting while the work on its machines finishes, the load itself, another model stopping, a model pinned or inside its idle window or minimum residency, or an earlier request for the same machines), then the requests answered in the last week.",
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
