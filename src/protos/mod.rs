pub mod coresdk {
    tonic::include_proto!("coresdk");
}

// No need to lint these
#[allow(clippy::all)]
// This is disgusting, but unclear to me how to avoid it. TODO: Discuss w/ prost maintainer
pub mod temporal {
    pub mod api {
        pub mod command {
            pub mod v1 {
                tonic::include_proto!("temporal.api.command.v1");
            }
        }
        pub mod enums {
            pub mod v1 {
                tonic::include_proto!("temporal.api.enums.v1");
            }
        }
        pub mod failure {
            pub mod v1 {
                tonic::include_proto!("temporal.api.failure.v1");
            }
        }
        pub mod filter {
            pub mod v1 {
                tonic::include_proto!("temporal.api.filter.v1");
            }
        }
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("temporal.api.common.v1");
            }
        }
        pub mod history {
            pub mod v1 {
                tonic::include_proto!("temporal.api.history.v1");
            }
        }
        pub mod namespace {
            pub mod v1 {
                tonic::include_proto!("temporal.api.namespace.v1");
            }
        }
        pub mod query {
            pub mod v1 {
                tonic::include_proto!("temporal.api.query.v1");
            }
        }
        pub mod replication {
            pub mod v1 {
                tonic::include_proto!("temporal.api.replication.v1");
            }
        }
        pub mod taskqueue {
            pub mod v1 {
                tonic::include_proto!("temporal.api.taskqueue.v1");
            }
        }
        pub mod version {
            pub mod v1 {
                tonic::include_proto!("temporal.api.version.v1");
            }
        }
        pub mod workflow {
            pub mod v1 {
                tonic::include_proto!("temporal.api.workflow.v1");
            }
        }
        pub mod workflowservice {
            pub mod v1 {
                tonic::include_proto!("temporal.api.workflowservice.v1");
            }
        }
    }
}
