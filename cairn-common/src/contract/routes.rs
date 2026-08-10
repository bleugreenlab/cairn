use super::specs::{DELETE_REASON, NO_CROSS_ACTIONS, NO_RELATED};
use super::types::*;
const DEFINITION: KeySpec = KeySpec::new("definition", KeyType::Any, "complete route definition");
const ROUTE_PROJECTIONS: &[ProjectionSpec] = &[ProjectionSpec {
    key: "projection",
    values: "graph",
}];
const NONE: &[MutationSpec] = &[];
const fn collection(kind: ResourceKind, uri: &'static str, name: &'static str) -> ResourceContract {
    ResourceContract{kind,uri_template:uri,name,description:"Named routes from a fact to its sinks, as a node+edge graph",read_projections:ROUTE_PROJECTIONS,related:NO_RELATED,cross_actions:NO_CROSS_ACTIONS,mutations:&[MutationSpec{mode:ChangeMode::Create,required:&[DEFINITION],optional:&[],label:"create route",example:"write({changes:[{target:\"cairn://routes\",mode:\"create\",payload:{definition:{name:\"...\",enabled:true,nodes:[{id:\"t1\",type:\"trigger\",when:{fact:\"attention\"}},{id:\"s1\",type:\"sink\",sink:{kind:\"channel\",register:\"notify\"}}],edges:[{from:\"t1\",to:\"s1\"}]}}}]})"}]}
}
const fn member(kind: ResourceKind, uri: &'static str, name: &'static str) -> ResourceContract {
    ResourceContract{kind,uri_template:uri,name,description:"A named declarative route: trigger, response, and sink nodes joined by edges",read_projections:&[],related:NO_RELATED,cross_actions:NO_CROSS_ACTIONS,mutations:&[MutationSpec{mode:ChangeMode::Patch,required:&[],optional:&[DEFINITION],label:"patch route",example:"write({changes:[{target:\"cairn://routes/ID\",mode:\"patch\",payload:{definition:{...}}}]})"},MutationSpec{mode:ChangeMode::Delete,required:&[],optional:&[DELETE_REASON],label:"delete route",example:"write({changes:[{target:\"cairn://routes/ID\",mode:\"delete\"}]})"}]}
}
const fn ro(kind: ResourceKind, uri: &'static str, name: &'static str) -> ResourceContract {
    ResourceContract {
        kind,
        uri_template: uri,
        name,
        description: "Durable route firing history",
        read_projections: &[],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NONE,
    }
}
pub(crate) const ROUTES_CONTRACT: ResourceContract =
    collection(ResourceKind::Routes, "cairn://routes", "Routes");
pub(crate) const ROUTE_CONTRACT: ResourceContract =
    member(ResourceKind::Route, "cairn://routes/{route_id}", "Route");
pub(crate) const ROUTE_HISTORY_CONTRACT: ResourceContract = ro(
    ResourceKind::RouteHistory,
    "cairn://routes/{route_id}/history",
    "Route history",
);
pub(crate) const ROUTE_HISTORY_ENTRY_CONTRACT: ResourceContract = ro(
    ResourceKind::RouteHistoryEntry,
    "cairn://routes/{route_id}/history/{seq}",
    "Route history entry",
);
pub(crate) const PROJECT_ROUTES_CONTRACT: ResourceContract = collection(
    ResourceKind::ProjectRoutes,
    "cairn://p/{project}/routes",
    "Project routes",
);
pub(crate) const PROJECT_ROUTE_CONTRACT: ResourceContract = member(
    ResourceKind::ProjectRoute,
    "cairn://p/{project}/routes/{route_id}",
    "Project route",
);
pub(crate) const PROJECT_ROUTE_HISTORY_CONTRACT: ResourceContract = ro(
    ResourceKind::ProjectRouteHistory,
    "cairn://p/{project}/routes/{route_id}/history",
    "Project route history",
);
pub(crate) const PROJECT_ROUTE_HISTORY_ENTRY_CONTRACT: ResourceContract = ro(
    ResourceKind::ProjectRouteHistoryEntry,
    "cairn://p/{project}/routes/{route_id}/history/{seq}",
    "Project route history entry",
);
