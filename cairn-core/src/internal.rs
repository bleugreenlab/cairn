//! Unstable host-facing runtime surface for Cairn's first-party apps.
//!
//! This module is gated behind the non-default `internal-api` feature and is not
//! part of the intended semver contract for third-party consumers.

pub mod agent_process {
    pub mod gc {
        pub use crate::agent_process::gc::*;
    }

    pub mod process {
        pub use crate::agent_process::process::*;
    }

    pub mod stream {
        pub use crate::agent_process::stream::*;
    }
}

pub mod api {
    pub use crate::api::*;
}

pub mod backends {
    pub use crate::backends::{
        backend_for_model, backend_for_name, AgentBackend, AgentPermissions, ProviderModelCatalog,
        ResolvedTools, SessionConfig,
    };

    pub mod codex {
        pub mod app_server {
            pub use crate::backends::codex::app_server::*;
        }
    }

    pub mod stdin {
        pub use crate::backends::stdin::*;
    }
}

pub mod db {
    pub use crate::db::*;
}

pub mod diesel_models {
    pub use crate::diesel_models::*;
}

pub mod effects {
    pub mod executor {
        pub use crate::effects::executor::*;
    }

    pub mod outbox {
        pub use crate::effects::outbox::*;
    }

    pub mod run {
        pub use crate::effects::run::*;
    }

    pub mod types {
        pub use crate::effects::types::*;
    }
}

pub mod embeddings {
    pub use crate::embeddings::{
        backfill_session, extract_embeddable_text, init_blocking, EmbeddingEngine, VibeState,
    };

    pub mod queries {
        pub use crate::embeddings::queries::*;
    }

    pub mod vibes {
        pub use crate::embeddings::vibes::*;
    }
}

pub mod env {
    pub use crate::env::*;
}

pub mod execution {
    pub use crate::execution::Initiator;

    pub mod actions {
        pub use crate::execution::actions::*;
    }

    pub mod advancement {
        pub use crate::execution::advancement::*;
    }

    pub mod cache {
        pub use crate::execution::cache::*;
    }

    pub mod checkpoints {
        pub use crate::execution::checkpoints::*;
    }

    pub mod conditions {
        pub use crate::execution::conditions::*;
    }

    pub mod creation {
        pub use crate::execution::creation::*;
    }

    pub mod dag {
        pub use crate::execution::dag::*;
    }

    pub mod dispatch {
        pub use crate::execution::dispatch::*;
    }

    pub mod jobs {
        pub use crate::execution::jobs::*;
    }

    pub mod queries {
        pub use crate::execution::queries::*;
    }

    pub mod recipe {
        pub use crate::execution::recipe::*;
    }
}

pub mod git {
    pub mod worktree {
        pub use crate::git::worktree::*;
    }
}

pub mod identity {
    pub use crate::identity::*;

    pub mod local {
        pub use crate::identity::local::*;
    }
}

pub mod mcp {
    pub use crate::mcp::McpAuthState;

    pub mod auth {
        pub use crate::mcp::auth::*;
    }

    pub mod git {
        pub use crate::mcp::git::*;
    }

    pub mod handlers {
        pub mod agents {
            pub use crate::mcp::handlers::agents::*;
        }

        pub mod bash {
            pub use crate::mcp::handlers::bash::*;
        }

        pub mod bug_report {
            pub use crate::mcp::handlers::bug_report::*;
        }

        pub mod custom_tool {
            pub use crate::mcp::handlers::custom_tool::*;
        }

        pub mod execute {
            pub use crate::mcp::handlers::execute::*;
        }

        pub mod external {
            pub use crate::mcp::handlers::external::*;
        }

        pub mod files {
            pub use crate::mcp::handlers::files::*;
        }

        pub mod implementation {
            pub use crate::mcp::handlers::implementation::*;
        }

        pub mod issue_resources {
            pub use crate::mcp::handlers::issue_resources::*;
        }

        pub mod issues {
            pub use crate::mcp::handlers::issues::*;
        }

        pub mod memories {
            pub use crate::mcp::handlers::memories::*;
        }

        pub mod messages {
            pub use crate::mcp::handlers::messages::*;
        }

        pub mod permission {
            pub use crate::mcp::handlers::permission::*;
        }

        pub mod planning {
            pub use crate::mcp::handlers::planning::*;
        }

        pub mod resources {
            pub use crate::mcp::handlers::resources::*;
        }

        pub mod search {
            pub use crate::mcp::handlers::search::*;
        }

        pub mod skills {
            pub use crate::mcp::handlers::skills::*;
        }

        pub mod slug {
            pub use crate::mcp::handlers::slug::*;
        }

        pub mod todos {
            pub use crate::mcp::handlers::todos::*;
        }
    }

    pub mod types {
        pub use crate::mcp::types::*;
    }
}

pub mod notify {
    pub use crate::notify::*;
}

pub mod orchestrator {
    pub use crate::orchestrator::{AccountManager, Orchestrator, OrchestratorBuilder};

    pub mod conflict_resolution {
        pub use crate::orchestrator::conflict_resolution::*;
    }

    pub mod lifecycle {
        pub use crate::orchestrator::lifecycle::*;
    }

    pub mod session {
        pub use crate::orchestrator::session::*;
    }
}

pub mod schema {
    pub use crate::schema::*;
}

pub mod services {
    pub use crate::services::*;

    #[cfg(any(test, feature = "test-utils"))]
    pub mod testing {
        pub use crate::services::testing::*;
    }
}

pub mod sync {
    pub use crate::sync::*;

    pub mod initial {
        pub use crate::sync::initial::*;
    }
}
