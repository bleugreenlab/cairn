//! Camp resource contracts (cairn/6255, cut down by cairn/7002).
use super::specs::*;
use super::types::*;
use cairn_uri::ResourceKind;

const CAMP_SLUG: KeySpec = KeySpec::new(
    "slug",
    KeyType::Str,
    "the camp's address segment; lowercase letters, digits and single hyphens, starting with a letter; `base` is reserved; stable once created",
);
const CAMP_TITLE: KeySpec = KeySpec::new("title", KeyType::Str, "");
const CAMP_MEMBER: KeySpec = KeySpec::new(
    "member",
    KeyType::Str,
    "a canonical thread URI of this project, or `operator`",
);
const CAMP_MEMBERS: KeySpec = KeySpec::new(
    "members",
    KeyType::Array,
    "the camp's other first members: canonical thread URIs of this project, or `operator`",
);
const CAMP_STATUS: KeySpec = KeySpec::new(
    "status",
    KeyType::Str,
    "active or archived: an archived camp stays readable and delivers nothing",
);

pub(crate) const PROJECT_CAMPS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectCamps,
    uri_template: "cairn://p/{project}/camps",
    name: "Project camps",
    description: "Base camp first, then the project's camps by title, then archived ones. Base camp is a named, passive channel every open thread is in: a post or notice there rides along at each thread's next turn and never wakes one. A camp is a group conversation: each post wakes every other member",
    read_projections: &[ProjectionSpec { key: "format", values: "json for the full projection" }],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[CAMP_SLUG, CAMP_TITLE],
        optional: &[CAMP_MEMBERS],
        label: "create a camp; you are its first member, and `members` adds others (threads and `operator`)",
        example: "write({changes:[{target:\"cairn:@/camps\",mode:\"append\",payload:{slug:\"rebuild\",title:\"Rebuild as an action\",members:[\"cairn://p/PROJECT/execution\",\"operator\"]}}]})",
    }],
};

pub(crate) const PROJECT_CAMP_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectCamp,
    uri_template: "cairn://p/{project}/camps/{slug}",
    name: "Camp",
    description: "One camp (`base` is the project's base camp): its members and its conversation, the posts and notices newest first",
    read_projections: &[ProjectionSpec { key: "format", values: "json for the full projection" }],
    related: &[
        RelatedSpec {
            label: "members",
            kind: ResourceKind::ProjectCampMembers,
            actions: true,
        },
        RelatedSpec {
            label: "posts",
            kind: ResourceKind::ProjectCampPosts,
            actions: true,
        },
    ],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[],
        optional: &[CAMP_TITLE, CAMP_STATUS],
        label: "rename (`title`) or archive and unarchive (`status`); any member, not base camp",
        example: "write({changes:[{target:\"cairn:@/camps/rebuild\",mode:\"patch\",payload:{status:\"archived\"}}]})",
    }],
};

pub(crate) const PROJECT_CAMP_MEMBERS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectCampMembers,
    uri_template: "cairn://p/{project}/camps/{slug}/members",
    name: "Camp members",
    description: "Every member of a camp, longest standing first. Base camp's members are every open thread of the project and are not managed",
    read_projections: &[ProjectionSpec { key: "format", values: "json for the full projection" }],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Append,
            required: &[CAMP_MEMBER],
            optional: &[],
            label: "add a member (a member adds anyone; a thread may add itself)",
            example: "write({changes:[{target:\"cairn:@/camps/rebuild/members\",mode:\"append\",payload:{member:\"cairn://p/PROJECT/harness\"}}]})",
        },
        MutationSpec {
            mode: ChangeMode::Delete,
            required: &[],
            optional: &[],
            label: "leave (your own thread; not base camp)",
            example: "write({changes:[{target:\"cairn:@/camps/rebuild/members\",mode:\"delete\"}]})",
        },
    ],
};

pub(crate) const PROJECT_CAMP_POSTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectCampPosts,
    uri_template: "cairn://p/{project}/camps/{slug}/posts",
    name: "Camp posts",
    description: "A camp's posts, newest first; each is also one of its project's posts. Posting to base camp reaches every open thread passively; posting to a camp wakes every other member. Posting is open to threads and the operator; if base camp becomes noisy, the lever is to restrict its posting to the operator and the `general` thread",
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
        optional: &[POST_TITLE],
        label: "post to the camp (any thread to base camp; a member to a camp)",
        example: "write({changes:[{target:\"cairn:@/camps/rebuild/posts\",mode:\"append\",payload:{title:\"...\",content:\"...\"}}]})",
    }],
};
