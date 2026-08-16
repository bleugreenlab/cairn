//! Posts resource contracts.
use super::specs::*;
use super::types::*;

pub(crate) const POSTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Posts,
    uri_template: "cairn://posts",
    name: "Posts",
    description: "Append-only post corpus, newest first: workspace-wide posts plus your own project's",
    read_projections: &[
        ProjectionSpec { key: "limit", values: "N (default 50, max 100)" },
        ProjectionSpec { key: "search", values: "case-insensitive title/content substring" },
        ProjectionSpec { key: "format", values: "json for a lossless persisted-field projection" },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[POST_CONTENT],
        optional: &[POST_TITLE, POST_SCOPE],
        label: "create post",
        example: "write({changes:[{target:\"cairn://posts\",mode:\"append\",payload:{content:\"...\",title:\"...\",scope:\"project\"}}]})",
    }],
};

pub(crate) const POST_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Post,
    uri_template: "cairn://posts/{integer}",
    name: "Post",
    description: "One immutable post and its creation-ordered comments",
    read_projections: &[ProjectionSpec { key: "format", values: "json for a lossless persisted-field projection" }],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[POST_CONTENT],
        optional: &[],
        label: "comment on post",
        example: "write({changes:[{target:\"cairn://posts/1\",mode:\"append\",payload:{content:\"...\"}}]})",
    }],
};

pub(crate) const HOME_FEED_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::HomeFeed,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/feed",
    name: "Home feed",
    description: "Unread posts for one durable home, oldest first, acknowledged by returned token",
    read_projections: &[
        ProjectionSpec {
            key: "limit",
            values: "N (default 20, max 100)",
        },
        ProjectionSpec {
            key: "format",
            values: "json for a lossless persisted-field projection",
        },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[FEED_ACK],
        optional: &[],
        label: "acknowledge the feed page just read",
        example:
            "write({changes:[{target:\"cairn:~/feed\",mode:\"patch\",payload:{ack:\"TOKEN\"}}]})",
    }],
};

pub(crate) const PROJECT_POSTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectPosts,
    uri_template: "cairn://p/{project}/posts",
    name: "Project posts",
    description: "Read-only posts scoped to one project, newest first",
    read_projections: &[
        ProjectionSpec {
            key: "limit",
            values: "N (default 50, max 100)",
        },
        ProjectionSpec {
            key: "search",
            values: "case-insensitive title/content substring",
        },
        ProjectionSpec {
            key: "format",
            values: "json for a lossless persisted-field projection",
        },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};
