pub mod coresdk {
    include!("coresdk.rs");

    pub type HistoryEventId = i64;
}

// No need to lint these
#[allow(clippy::all)]
// This is disgusting, but unclear to me how to avoid it. TODO: Discuss w/ prost maintainer
pub mod temporal {
    pub mod api {
        pub mod command {
            pub mod v1 {
                include!("temporal.api.command.v1.rs");
            }
        }
        pub mod enums {
            pub mod v1 {
                include!("temporal.api.enums.v1.rs");
            }
        }
        pub mod failure {
            pub mod v1 {
                include!("temporal.api.failure.v1.rs");
            }
        }
        pub mod filter {
            pub mod v1 {
                include!("temporal.api.filter.v1.rs");
            }
        }
        pub mod common {
            pub mod v1 {
                include!("temporal.api.common.v1.rs");
            }
        }
        pub mod history {
            pub mod v1 {
                include!("temporal.api.history.v1.rs");
            }
        }
        pub mod namespace {
            pub mod v1 {
                include!("temporal.api.namespace.v1.rs");
            }
        }
        pub mod query {
            pub mod v1 {
                include!("temporal.api.query.v1.rs");
            }
        }
        pub mod replication {
            pub mod v1 {
                include!("temporal.api.replication.v1.rs");
            }
        }
        pub mod taskqueue {
            pub mod v1 {
                include!("temporal.api.taskqueue.v1.rs");
            }
        }
        pub mod version {
            pub mod v1 {
                include!("temporal.api.version.v1.rs");
            }
        }
        pub mod workflow {
            pub mod v1 {
                include!("temporal.api.workflow.v1.rs");
            }
        }
        pub mod workflowservice {
            pub mod v1 {
                include!("temporal.api.workflowservice.v1.rs");
            }
        }
    }
}
