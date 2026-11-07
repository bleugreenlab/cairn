//! Page resource contracts.
//!
//! A page is a small sandboxed app a thread owns: a versioned `page` artifact
//! stored as `pages/<name>`, rendered as a facet of its thread, whose only I/O
//! is the bridge its frame talks to. A page written on an issue node is a draft
//! that renders on that node and never reaches the project index.
use super::specs::*;
use super::types::*;
use cairn_uri::ResourceKind;

const PAGE_TITLE: KeySpec = KeySpec::new("title", KeyType::Str, "");
const PAGE_FILES: KeySpec = KeySpec::new(
    "files",
    KeyType::Object,
    "path -> UTF-8 text; paths match [A-Za-z0-9._-] segments joined by '/', 1 MiB in total; on patch each key merges and null deletes that file",
);
const PAGE_DESCRIPTION: KeySpec = KeySpec::new("description", KeyType::Str, "");
const PAGE_ENTRY: KeySpec = KeySpec::new(
    "entry",
    KeyType::Str,
    "the .js/.mjs file whose default export is the page's React component; default index.js",
);
const PAGE_MANIFEST: KeySpec = KeySpec::new(
    "manifest",
    KeyType::Object,
    "{network:{hosts:[\"api.example.com\", \"*.example.com\"]}, services:[\"judge\", \"cairn://p/P/THREAD/services/NAME\"]}: the https hosts the page may read and the services it may call, a project service by id or its own thread's service by canonical URI; none by default",
);
const PAGE_PROMOTED: KeySpec = KeySpec::new(
    "promoted",
    KeyType::Bool,
    "true lists a thread's page in the project's Pages index; a draft on an issue node is never listed",
);

pub(crate) const PROJECT_PAGES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectPages,
    uri_template: "cairn://p/{project}/pages",
    name: "Project pages",
    description: "The project's Pages index: the latest version of every thread-owned page whose owning thread promoted it, newest first. Drafts written on issue nodes are never listed",
    read_projections: &[
        ProjectionSpec { key: "thread", values: "a thread name: only that thread's pages" },
        ProjectionSpec { key: "search", values: "case-insensitive name, title, or description substring" },
        ProjectionSpec { key: "limit", values: "N (default 50, max 200)" },
        ProjectionSpec { key: "format", values: "json for the full projection" },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const NODE_PAGES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::NodePages,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/pages",
    name: "Pages",
    description: "The pages this thread (or, as drafts, this issue node) owns: each one's name, title, latest version, and whether it is promoted. Write one at .../pages/<name>",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const NODE_PAGE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::NodePage,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/pages/{name}",
    name: "Page",
    description: "One page: a sandboxed app whose entry module default-exports a React component built from @cairn/page-kit, rendered as a facet of the thread that owns it. A read returns the page rendered as text (what the operator sees), with a footer naming its version and how the render went; ?format=json is the stored source. Every write appends a version; an open facet remounts on it. Its reads run with the owning thread's authority, narrowed to what the reading agent may read, and are logged at .../log",
    read_projections: &[
        ProjectionSpec { key: "format", values: "text (default): the page rendered as Markdown | json: the exact stored page data" },
        ProjectionSpec { key: "version", values: "N: render (or, with format=json, return) that version (default latest)" },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Create,
            required: &[PAGE_TITLE, PAGE_FILES],
            optional: &[PAGE_DESCRIPTION, PAGE_ENTRY, PAGE_MANIFEST, PAGE_PROMOTED],
            label: "write a page (replaces every field; appends a version)",
            example: "write({changes:[{target:\"cairn:~/pages/release-board\",mode:\"create\",payload:{title:\"Release board\",files:{\"index.js\":\"import {Page, html} from '@cairn/page-kit';\\nexport default () => html`<${Page} title=\\\"Releases\\\" />`;\"}}}]})",
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[PAGE_FILES, PAGE_TITLE, PAGE_DESCRIPTION, PAGE_ENTRY, PAGE_MANIFEST, PAGE_PROMOTED],
            label: "revise a page (files merge per path, null deletes one; other keys replace)",
            example: "write({changes:[{target:\"cairn:~/pages/release-board\",mode:\"patch\",payload:{files:{\"chart.js\":\"...\",\"old.js\":null},promoted:true}}]})",
        },
    ],
};

pub(crate) const NODE_PAGE_LOG_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::NodePageLog,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/pages/{name}/log",
    name: "Page call log",
    description: "What open frames and rendered reads of this page did, newest first: each bridged read and subscription with its targets and outcome, refusals, runtime errors the page reported, and blocked navigations. The viewer (a person, or the agent that read the page) is recorded but is never the source of a call's authority",
    read_projections: &[
        ProjectionSpec { key: "limit", values: "N (default 50, max 500)" },
        ProjectionSpec { key: "verb", values: "read|subscribe|diag|navigation|write" },
        ProjectionSpec { key: "outcome", values: "ok|refused|error" },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};
