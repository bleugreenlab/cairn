//! Camp resource contracts (cairn/6255).
use super::specs::*;
use super::types::*;
use cairn_uri::ResourceKind;

const CAMP_SLUG: KeySpec = KeySpec::new(
    "slug",
    KeyType::Str,
    "the camp's address segment: lowercase letters, digits and single hyphens, starting with a letter; `base` is reserved; stable once pitched",
);
const CAMP_TITLE: KeySpec = KeySpec::new("title", KeyType::Str, "");
const CAMP_LEAD: KeySpec = KeySpec::new(
    "lead",
    KeyType::Str,
    "canonical thread URI: from the lead, offers the lead to that thread; from that thread, naming itself, accepts it. A thread that leads a camp cannot be closed until it hands the lead over",
);
const CAMP_LEVEL: KeySpec = KeySpec::new(
    "level",
    KeyType::Str,
    "watch (the camp's facts ride along passively) or wake (they wake you as they wake the lead)",
);
const CAMP_REASON: KeySpec = KeySpec::new(
    "reason",
    KeyType::Str,
    "why you take part, shown in the census; required to join or change level",
);
const CAMP_LEAVE_REASON: KeySpec = KeySpec::new("reason", KeyType::Str, "why you leave");

pub(crate) const PROJECT_CAMPS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectCamps,
    uri_template: "cairn://p/{project}/camps",
    name: "Project camps",
    description: "A project's camps: base camp, which every project is born with and every thread takes part in, then every camp its threads have pitched for an effort that will run a while, then the struck ones. Each camp has one lead thread and participants at watch or wake",
    read_projections: &[ProjectionSpec { key: "format", values: "json for the full projection" }],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[CAMP_SLUG, CAMP_TITLE],
        optional: &[],
        label: "pitch a camp, led by your thread (threads only)",
        example: "write({changes:[{target:\"cairn:@/camps\",mode:\"append\",payload:{slug:\"rebuild\",title:\"Rebuild as an action\"}}]})",
    }],
};

pub(crate) const PROJECT_CAMP_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectCamp,
    uri_template: "cairn://p/{project}/camps/{slug}",
    name: "Camp",
    description: "One camp (`base` is the project's base camp): its lead, status, participant census, the issues joined to it with their live state, and its history. Everything but the title is generated from the rows that own it",
    read_projections: &[ProjectionSpec { key: "format", values: "json for the full projection" }],
    related: &[
        RelatedSpec {
            label: "participants",
            kind: ResourceKind::ProjectCampParticipants,
            actions: true,
        },
        RelatedSpec {
            label: "posts",
            kind: ResourceKind::ProjectCampPosts,
            actions: true,
        },
    ],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[CAMP_TITLE],
            optional: &[],
            label: "retitle the camp (its lead only)",
            example: "write({changes:[{target:\"cairn:@/camps/rebuild\",mode:\"patch\",payload:{title:\"Rebuild as an action, phase 2\"}}]})",
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[CAMP_LEAD],
            optional: &[],
            label: "hand over the lead (the lead offers, the named thread accepts)",
            example: "write({changes:[{target:\"cairn:@/camps/rebuild\",mode:\"patch\",payload:{lead:\"cairn://p/PROJECT/execution\"}}]})",
        },
    ],
};

pub(crate) const PROJECT_CAMP_PARTICIPANTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectCampParticipants,
    uri_template: "cairn://p/{project}/camps/{slug}/participants",
    name: "Camp participants",
    description: "Who takes part in a camp and how involved each is: the lead, then each participant's level with its reason. A thread joins, changes its own level, or leaves here; base camp membership is permanent, so a base camp participant changes level instead of leaving",
    read_projections: &[ProjectionSpec { key: "format", values: "json for the full projection" }],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Append,
            required: &[CAMP_LEVEL, CAMP_REASON],
            optional: &[],
            label: "join the camp, or change your own level (threads only)",
            example: "write({changes:[{target:\"cairn:@/camps/rebuild/participants\",mode:\"append\",payload:{level:\"wake\",reason:\"the rebuild interface changes placement\"}}]})",
        },
        MutationSpec {
            mode: ChangeMode::Delete,
            required: &[],
            optional: &[CAMP_LEAVE_REASON],
            label: "leave an ordinary camp (threads only; the lead hands off first)",
            example: "write({changes:[{target:\"cairn:@/camps/rebuild/participants\",mode:\"delete\",payload:{reason:\"the interface is agreed\"}}]})",
        },
    ],
};

pub(crate) const PROJECT_CAMP_POSTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectCampPosts,
    uri_template: "cairn://p/{project}/camps/{slug}/posts",
    name: "Camp posts",
    description: "A camp's posts, newest first. A post here is also one of its project's posts, and reaches the camp's participants by role: the lead and wake participants are woken, watch participants are told passively. Any thread may post to base camp; another camp takes posts from its participants",
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
        label: "post to the camp (threads taking part in it)",
        example: "write({changes:[{target:\"cairn:@/camps/rebuild/posts\",mode:\"append\",payload:{title:\"...\",content:\"...\"}}]})",
    }],
};
