//! Store-served source-control contracts: `cairn://p/{project}/vcs/{op}`.
//!
//! Every operation answers from the project's jj store and Git object
//! database with no checkout, no subprocess, and no store lock. Revision keys
//! accept what `?branch=` accepts: a branch name, a commit or change id, a node
//! URI, or `main` for the project's default branch.

use super::specs::*;
use super::types::*;

const FROM: ProjectionSpec = ProjectionSpec {
    key: "from",
    values: "older revision: branch, commit or change id, node URI, or main (default: the project's default branch)",
};

const TO: ProjectionSpec = ProjectionSpec {
    key: "to",
    values: "newer revision, same forms as from (default: the calling node's branch; required without one)",
};

const FORMAT_JSON: ProjectionSpec = ProjectionSpec {
    key: "format",
    values: "json for the structured answer (absent = Markdown)",
};

const VCS_RELATED: &[RelatedSpec] = &[
    RelatedSpec {
        label: "diff",
        kind: ResourceKind::ProjectVcsDiff,
        actions: false,
    },
    RelatedSpec {
        label: "ancestry",
        kind: ResourceKind::ProjectVcsAncestry,
        actions: false,
    },
    RelatedSpec {
        label: "log",
        kind: ResourceKind::ProjectVcsLog,
        actions: false,
    },
    RelatedSpec {
        label: "blame",
        kind: ResourceKind::ProjectVcsBlame,
        actions: false,
    },
];

pub(crate) const PROJECT_VCS_DIFF_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectVcsDiff,
    uri_template: "cairn://p/{project}/vcs/diff",
    name: "Store diff",
    description: "What changed between two revisions, read from the store with no checkout (read-only). A bare read from a node answers what its branch changed since it forked from the default branch.",
    read_projections: &[
        FROM,
        TO,
        ProjectionSpec {
            key: "commit",
            values: "one revision diffed against its first parent, instead of from/to",
        },
        ProjectionSpec {
            key: "base",
            values: "fork (default: from the merge-base, git's from...to) | from (tree to tree, git's from to)",
        },
        ProjectionSpec {
            key: "view",
            values: "summary (default) | patch | commits",
        },
        ProjectionSpec {
            key: "file",
            values: "one path's patch (implies view=patch)",
        },
        ProjectionSpec {
            key: "glob",
            values: "restrict the summary or patch to matching paths",
        },
        FORMAT_JSON,
    ],
    related: VCS_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const PROJECT_VCS_ANCESTRY_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectVcsAncestry,
    uri_template: "cairn://p/{project}/vcs/ancestry",
    name: "Store ancestry",
    description: "How two revisions relate: their merge-base, whether either contains the other, and how many commits each has that the other lacks (read-only)",
    read_projections: &[FROM, TO, FORMAT_JSON],
    related: VCS_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const PROJECT_VCS_LOG_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectVcsLog,
    uri_template: "cairn://p/{project}/vcs/log",
    name: "Store log",
    description: "Commits reachable from `to` and not from `from`, newest first, following every parent (read-only)",
    read_projections: &[
        TO,
        ProjectionSpec {
            key: "from",
            values: "exclude this revision and its ancestors (default: none, so the whole history of to)",
        },
        ProjectionSpec {
            key: "path",
            values: "only commits that change this file or directory",
        },
        ProjectionSpec {
            key: "limit",
            values: "commits per page (default 30, maximum 500)",
        },
        ProjectionSpec {
            key: "offset",
            values: "commits to skip (default 0)",
        },
        FORMAT_JSON,
    ],
    related: VCS_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const PROJECT_VCS_BLAME_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectVcsBlame,
    uri_template: "cairn://p/{project}/vcs/blame",
    name: "Store blame",
    description: "Each line of one text file at a revision, attributed to the commit that introduced it (read-only; files up to 1 MB)",
    read_projections: &[
        ProjectionSpec {
            key: "path",
            values: "required repository-relative file path",
        },
        ProjectionSpec {
            key: "rev",
            values: "revision, same forms as ?branch= (default: the calling node's branch)",
        },
        ProjectionSpec {
            key: "lines",
            values: "A-B to render only that 1-based inclusive line range",
        },
        FORMAT_JSON,
    ],
    related: VCS_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};
