//! Service resource contracts.
//!
//! A service is a supervised process. A project declares one under
//! `.cairn/services/<name>/` on its base branch; a thread writes one at
//! `cairn:~/services/<name>` the way it writes a page. Either way its resource
//! is also the acting home of its runs, and appending to it calls it, the way
//! appending to a terminal sends it input.
use super::specs::*;
use super::types::*;
use cairn_uri::ResourceKind;

const SERVICE_ACTION: KeySpec = KeySpec::new("action", KeyType::Str, "start|stop|restart");
const SERVICE_METHOD: KeySpec =
    KeySpec::new("method", KeyType::Str, "a method the service implements");
const SERVICE_PARAMS: KeySpec =
    KeySpec::new("params", KeyType::Any, "passed to the method as given");
const SERVICE_TIMEOUT_SECONDS: KeySpec = KeySpec::new(
    "timeoutSeconds",
    KeyType::Int,
    "the call's deadline in seconds",
);

const SERVICE_RELATED: &[RelatedSpec] = &[
    RelatedSpec {
        label: "status: state, generation, executor, pending calls",
        kind: ResourceKind::ProjectServiceStatus,
        actions: false,
    },
    RelatedSpec {
        label: "log: lifecycle transitions, stderr, and calls",
        kind: ResourceKind::ProjectServiceLog,
        actions: false,
    },
];

const SERVICE_MEMBER_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "the service: declaration, actions, and recent calls",
    kind: ResourceKind::ProjectService,
    actions: true,
}];

pub(crate) const PROJECT_SERVICES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectServices,
    uri_template: "cairn://p/{project}/services",
    name: "Project services",
    description: "The project's services: supervised processes declared under .cairn/services/<name>/ on the base branch, each with its state, start policy, executor, generation, source hash, uptime, restarts, and last error",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const PROJECT_SERVICE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectService,
    uri_template: "cairn://p/{project}/services/{service}",
    name: "Project service",
    description: "One service: its declaration at the base-branch tip, its status, and its most recent calls. Patch an action to start, stop, or restart it; append a method call to call it and receive the result in the change report",
    read_projections: NO_PROJECTIONS,
    related: SERVICE_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[SERVICE_ACTION],
            optional: &[],
            label: "start, stop, or restart the service",
            example: "write({changes:[{target:\"cairn:@/services/echo\",mode:\"patch\",payload:{action:\"restart\"}}]})",
        },
        MutationSpec {
            mode: ChangeMode::Append,
            required: &[SERVICE_METHOD],
            optional: &[SERVICE_PARAMS, SERVICE_TIMEOUT_SECONDS],
            label: "call the service",
            example: "write({changes:[{target:\"cairn:@/services/echo\",mode:\"append\",payload:{method:\"echo\",params:{hi:1}}}]})",
        },
    ],
};

pub(crate) const PROJECT_SERVICE_STATUS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectServiceStatus,
    uri_template: "cairn://p/{project}/services/{service}/status",
    name: "Service status",
    description: "A service's compact live status: state, generation, run, executor, health, pending calls, and any backoff deadline",
    read_projections: NO_PROJECTIONS,
    related: SERVICE_MEMBER_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const PROJECT_SERVICE_LOG_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectServiceLog,
    uri_template: "cairn://p/{project}/services/{service}/log",
    name: "Service log",
    description: "A service's log, newest last: lifecycle transitions, the process's stderr, and every call with its caller and outcome",
    read_projections: &[
        ProjectionSpec { key: "since", values: "only rows after this point" },
        ProjectionSpec { key: "limit", values: "N rows" },
        ProjectionSpec { key: "kind", values: "lifecycle|stderr|call" },
    ],
    related: SERVICE_MEMBER_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

const THREAD_SERVICE_TITLE: KeySpec = KeySpec::new(
    "title",
    KeyType::Str,
    "what a person calls it; default the name",
);
const THREAD_SERVICE_DESCRIPTION: KeySpec = KeySpec::new("description", KeyType::Str, "");
const THREAD_SERVICE_FILES: KeySpec = KeySpec::new(
    "files",
    KeyType::Object,
    "path -> UTF-8 text; a Bun package whose entry calls serve({methods}) from @cairn/sdk/service; at most 128 files and 8 MiB; on patch each key merges and null deletes that file",
);
const THREAD_SERVICE_ENTRY: KeySpec = KeySpec::new(
    "entry",
    KeyType::Str,
    "the .ts/.js/.mjs file the process imports; default main.ts",
);
const THREAD_SERVICE_MANIFEST: KeySpec = KeySpec::new(
    "manifest",
    KeyType::Object,
    "{start, env, limits:{memoryMb, concurrency, callTimeoutSeconds}, executor:{requiredToolchains}}: a service.yaml's keys beyond name, description, and entry; none required",
);

const THREAD_SERVICE_RELATED: &[RelatedSpec] = &[
    RelatedSpec {
        label: "status: state, generation, executor, pending calls",
        kind: ResourceKind::ThreadServiceStatus,
        actions: false,
    },
    RelatedSpec {
        label: "log: boot errors, protocol violations, stderr, and calls",
        kind: ResourceKind::ThreadServiceLog,
        actions: false,
    },
];

const THREAD_SERVICE_MEMBER_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "the service: its package, actions, and recent calls",
    kind: ResourceKind::ThreadService,
    actions: true,
}];

pub(crate) const THREAD_SERVICES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadServices,
    uri_template: "cairn://p/{project}/{name}/services",
    name: "Thread services",
    description: "The services this thread owns: processes the runner supervises from packages the thread wrote, each with its state, version, generation, uptime, restarts, and last error. Write one at .../services/<name>",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const THREAD_SERVICE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadService,
    uri_template: "cairn://p/{project}/{name}/services/{service}",
    name: "Thread service",
    description: "One thread-owned service: the package its thread wrote, its status, and its most recent calls. Every create or patch appends a version and deploys it: the first starts the service, and each later one is a rolling replace in which the old generation serves until the new one is ready (a boot failure leaves it serving, state update_failed). Only the owning thread writes it, and only that thread, its tasks, and its pages call it. Patch an action to start, stop, or restart it; append a method call to call it",
    read_projections: &[ProjectionSpec { key: "format", values: "json for the exact stored package" }],
    related: THREAD_SERVICE_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Create,
            required: &[THREAD_SERVICE_FILES],
            optional: &[THREAD_SERVICE_TITLE, THREAD_SERVICE_DESCRIPTION, THREAD_SERVICE_ENTRY, THREAD_SERVICE_MANIFEST],
            label: "write a service (replaces every field; appends a version and deploys it)",
            example: "write({changes:[{target:\"cairn:~/services/tutor\",mode:\"create\",payload:{title:\"Tutor\",files:{\"main.ts\":\"import {serve} from '@cairn/sdk/service';\\nserve({methods: {hello: (params) => ({hi: params})}});\"}}}]})",
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[THREAD_SERVICE_FILES, THREAD_SERVICE_TITLE, THREAD_SERVICE_DESCRIPTION, THREAD_SERVICE_ENTRY, THREAD_SERVICE_MANIFEST, SERVICE_ACTION],
            label: "revise and redeploy a service (files merge per path, null deletes one; other keys replace), or {action} alone to start, stop, or restart it",
            example: "write({changes:[{target:\"cairn:~/services/tutor\",mode:\"patch\",payload:{files:{\"main.ts\":\"...\"}}}]})",
        },
        MutationSpec {
            mode: ChangeMode::Append,
            required: &[SERVICE_METHOD],
            optional: &[SERVICE_PARAMS, SERVICE_TIMEOUT_SECONDS],
            label: "call the service",
            example: "write({changes:[{target:\"cairn:~/services/tutor\",mode:\"append\",payload:{method:\"hello\",params:{hi:1}}}]})",
        },
    ],
};

pub(crate) const THREAD_SERVICE_STATUS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadServiceStatus,
    uri_template: "cairn://p/{project}/{name}/services/{service}/status",
    name: "Thread service status",
    description: "A thread-owned service's compact live status: state, the version it serves, generation, run, executor, pending calls, any backoff deadline, and the last error",
    read_projections: NO_PROJECTIONS,
    related: THREAD_SERVICE_MEMBER_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const THREAD_SERVICE_LOG_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadServiceLog,
    uri_template: "cairn://p/{project}/{name}/services/{service}/log",
    name: "Thread service log",
    description: "A thread-owned service's log, newest last: lifecycle transitions (boot errors among them), protocol violations, the process's stderr, and every call with its caller and outcome",
    read_projections: &[
        ProjectionSpec { key: "since", values: "only rows after this point" },
        ProjectionSpec { key: "limit", values: "N rows" },
        ProjectionSpec { key: "kind", values: "lifecycle|stderr|call" },
    ],
    related: THREAD_SERVICE_MEMBER_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

#[cfg(test)]
mod tests {
    use super::super::{contract_for, mutation_spec};
    use super::*;

    #[test]
    fn a_thread_service_is_written_like_a_page_and_called_like_a_service() {
        for kind in [
            ResourceKind::ThreadServices,
            ResourceKind::ThreadServiceStatus,
            ResourceKind::ThreadServiceLog,
        ] {
            let contract = contract_for(kind).expect("registered");
            assert!(contract.mutations.is_empty(), "{kind:?} is read-only");
        }
        for mode in ChangeMode::ALL {
            assert_eq!(
                mutation_spec(ResourceKind::ThreadService, *mode).is_some(),
                matches!(
                    mode,
                    ChangeMode::Create | ChangeMode::Patch | ChangeMode::Append
                ),
                "{mode:?}"
            );
        }
        let create = mutation_spec(ResourceKind::ThreadService, ChangeMode::Create).unwrap();
        assert_eq!(
            create.required.iter().map(|k| k.key).collect::<Vec<_>>(),
            ["files"]
        );
        let patch = mutation_spec(ResourceKind::ThreadService, ChangeMode::Patch).unwrap();
        assert!(patch.required.is_empty());
        assert!(patch.optional.iter().any(|k| k.key == "action"));
        assert!(patch.optional.iter().any(|k| k.key == "files"));
    }

    #[test]
    fn only_the_service_itself_is_writable_and_only_by_patch_and_append() {
        for kind in [
            ResourceKind::ProjectServices,
            ResourceKind::ProjectServiceStatus,
            ResourceKind::ProjectServiceLog,
        ] {
            let contract = contract_for(kind).expect("registered");
            assert!(contract.mutations.is_empty(), "{kind:?} is read-only");
        }
        for mode in ChangeMode::ALL {
            let spec = mutation_spec(ResourceKind::ProjectService, *mode);
            assert_eq!(
                spec.is_some(),
                matches!(mode, ChangeMode::Patch | ChangeMode::Append),
                "{mode:?}"
            );
        }
        let call = mutation_spec(ResourceKind::ProjectService, ChangeMode::Append).unwrap();
        assert_eq!(
            call.required.iter().map(|k| k.key).collect::<Vec<_>>(),
            ["method"]
        );
        assert_eq!(
            call.optional.iter().map(|k| k.key).collect::<Vec<_>>(),
            ["params", "timeoutSeconds"]
        );
        let control = mutation_spec(ResourceKind::ProjectService, ChangeMode::Patch).unwrap();
        assert_eq!(
            control.required.iter().map(|k| k.key).collect::<Vec<_>>(),
            ["action"]
        );
        assert!(control.optional.is_empty());
    }
}
