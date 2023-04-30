//! Prelude for easy importing of required types when defining workflow and activity definitions

/// Registry prelude
#[allow(unused_imports)]
pub mod registry {
    pub use crate::workflow::{
        into_activity_0_args, into_activity_0_args_without_ctx, into_activity_1_args,
        into_activity_1_args_exit_value, into_activity_1_args_with_errors, into_activity_2_args,
        into_workflow_0_args, into_workflow_1_args, into_workflow_1_args_exit_value,
        into_workflow_1_args_with_errors, into_workflow_2_args,
    };
}

/// Activity prelude
#[allow(unused_imports)]
pub mod activity {
    pub use crate::{ActContext, ActExitValue, ActivityCancelledError, NonRetryableActivityError};
    pub use serde::{Deserialize, Serialize};
    pub use temporal_sdk_core_protos::{
        coresdk::FromFailureExt, temporal::api::failure::v1::Failure,
    };
}

/// Workflow prelude
#[allow(unused_imports)]
pub mod workflow {
    pub use crate::workflow::{
        execute_activity_0_args, execute_activity_0_args_without_ctx, execute_activity_1_args,
        execute_activity_1_args_exit_value, execute_activity_1_args_with_errors,
        execute_activity_2_args, execute_child_workflow_0_args_exit_value,
        execute_child_workflow_1_args, execute_child_workflow_2_args,
        execute_child_workflow_2_args_exit_value,
    };
    pub use crate::workflow::{AsyncFn0, AsyncFn1, AsyncFn2, AsyncFn3};
    pub use crate::{
        ActivityOptions, ChildWorkflowOptions, LocalActivityOptions, Signal, SignalData,
        SignalWorkflowOptions, WfContext, WfExitValue, WorkflowResult,
    };
    use futures::FutureExt;
    pub use serde::{Deserialize, Serialize};
    pub use std::{
        fmt::{Debug, Display},
        future::Future,
        time::Duration,
    };
    pub use temporal_sdk_core_protos::{
        coresdk::{
            activity_result::{self, activity_resolution},
            child_workflow::{child_workflow_result, Failure, Success},
            workflow_commands::ActivityCancellationType,
            AsJsonPayloadExt, FromFailureExt, FromJsonPayloadExt,
        },
        temporal::api::common::v1::Payload,
    };
}
