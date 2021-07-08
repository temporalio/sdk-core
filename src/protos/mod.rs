//! Contains the protobuf definitions used as arguments to and return values from interactions with
//! [super::Core]. Language SDK authors can generate structs using the proto definitions that will
//! match the generated structs in this module.

#[allow(clippy::large_enum_variant)]
// I'd prefer not to do this, but there are some generated things that just don't need it.
#[allow(missing_docs)]
pub mod coresdk {
    //! Contains all protobufs relating to communication between core and lang-specific SDKs

    tonic::include_proto!("coresdk");

    use crate::protos::temporal::api::{
        common::v1::{Payloads, WorkflowExecution},
        failure::v1::{failure::FailureInfo, ApplicationFailureInfo, Failure},
        workflowservice::v1::PollActivityTaskQueueResponse,
    };
    use activity_result::ActivityResult;
    use activity_task::ActivityTask;
    use common::{Payload, UserCodeFailure};
    use std::{
        convert::TryFrom,
        fmt::{Display, Formatter},
    };
    use workflow_activation::{wf_activation_job, WfActivationJob};
    use workflow_commands::{workflow_command, workflow_command::Variant, WorkflowCommand};
    use workflow_completion::{wf_activation_completion, WfActivationCompletion};

    #[allow(clippy::module_inception)]
    pub mod activity_task {
        use crate::task_token::TaskToken;

        tonic::include_proto!("coresdk.activity_task");

        impl ActivityTask {
            pub fn cancel_from_ids(task_token: TaskToken, activity_id: String) -> Self {
                ActivityTask {
                    task_token: task_token.0,
                    activity_id,
                    variant: Some(activity_task::Variant::Cancel(Cancel {})),
                }
            }
        }
    }
    #[allow(clippy::module_inception)]
    pub mod activity_result {
        tonic::include_proto!("coresdk.activity_result");
        use super::common::Payload;

        impl ActivityResult {
            pub fn cancel_from_details(payload: Option<Payload>) -> Self {
                Self {
                    status: Some(activity_result::Status::Canceled(Cancelation {
                        details: payload,
                    })),
                }
            }
        }
    }
    pub mod common {
        tonic::include_proto!("coresdk.common");

        impl<T> From<T> for Payload
        where
            T: AsRef<[u8]>,
        {
            fn from(v: T) -> Self {
                Self {
                    metadata: Default::default(),
                    data: v.as_ref().to_vec(),
                }
            }
        }

        impl Payload {
            // Is it's own function b/c asref causes implementation conflicts
            pub fn as_slice(&self) -> &[u8] {
                self.data.as_slice()
            }
        }
    }
    pub mod workflow_activation {
        use crate::{
            core_tracing::VecDisplayer,
            protos::coresdk::PayloadsExt,
            protos::temporal::api::history::v1::{
                WorkflowExecutionCancelRequestedEventAttributes,
                WorkflowExecutionSignaledEventAttributes,
            },
            workflow::LEGACY_QUERY_ID,
        };
        use std::fmt::{Display, Formatter};

        tonic::include_proto!("coresdk.workflow_activation");
        pub fn create_evict_activation(run_id: String) -> WfActivation {
            WfActivation {
                timestamp: None,
                run_id,
                is_replaying: false,
                jobs: vec![WfActivationJob::from(
                    wf_activation_job::Variant::RemoveFromCache(true),
                )],
            }
        }

        pub fn create_query_activation(
            run_id: String,
            queries: impl IntoIterator<Item = QueryWorkflow>,
        ) -> WfActivation {
            WfActivation {
                timestamp: None,
                run_id,
                is_replaying: false,
                jobs: queries
                    .into_iter()
                    .map(|qr| wf_activation_job::Variant::QueryWorkflow(qr).into())
                    .collect(),
            }
        }

        impl WfActivation {
            /// Returns true if this activation has one and only one job to perform a legacy query
            pub(crate) fn is_legacy_query(&self) -> bool {
                matches!(&self.jobs.as_slice(), &[WfActivationJob {
                    variant: Some(wf_activation_job::Variant::QueryWorkflow(qr))
                }] if qr.query_id == LEGACY_QUERY_ID)
            }
        }

        impl Display for WfActivation {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "WfActivation(")?;
                write!(f, "run_id: {}, ", self.run_id)?;
                write!(f, "is_replaying: {}, ", self.is_replaying)?;
                write!(f, "jobs: {})", self.jobs.display())
            }
        }

        impl Display for WfActivationJob {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                match &self.variant {
                    None => write!(f, "Empty"),
                    Some(v) => match v {
                        wf_activation_job::Variant::StartWorkflow(_) => write!(f, "StartWorkflow"),
                        wf_activation_job::Variant::FireTimer(_) => write!(f, "FireTimer"),
                        wf_activation_job::Variant::UpdateRandomSeed(_) => {
                            write!(f, "UpdateRandomSeed")
                        }
                        wf_activation_job::Variant::QueryWorkflow(_) => write!(f, "QueryWorkflow"),
                        wf_activation_job::Variant::CancelWorkflow(_) => {
                            write!(f, "CancelWorkflow")
                        }
                        wf_activation_job::Variant::SignalWorkflow(_) => {
                            write!(f, "SignalWorkflow")
                        }
                        wf_activation_job::Variant::ResolveActivity(_) => {
                            write!(f, "ResolveActivity")
                        }
                        wf_activation_job::Variant::RemoveFromCache(_) => {
                            write!(f, "RemoveFromCache")
                        }
                    },
                }
            }
        }

        impl From<WorkflowExecutionSignaledEventAttributes> for SignalWorkflow {
            fn from(a: WorkflowExecutionSignaledEventAttributes) -> Self {
                Self {
                    signal_name: a.signal_name,
                    input: Vec::from_payloads(a.input),
                    identity: a.identity,
                }
            }
        }

        impl From<WorkflowExecutionCancelRequestedEventAttributes> for CancelWorkflow {
            fn from(_a: WorkflowExecutionCancelRequestedEventAttributes) -> Self {
                Self { details: vec![] }
            }
        }
    }

    pub mod workflow_completion {
        use crate::protos::coresdk::workflow_completion::wf_activation_completion::Status;
        tonic::include_proto!("coresdk.workflow_completion");

        impl wf_activation_completion::Status {
            pub fn is_success(&self) -> bool {
                match &self {
                    Status::Successful(_) => true,
                    Status::Failed(_) => false,
                }
            }
        }
    }
    pub mod workflow_commands {
        tonic::include_proto!("coresdk.workflow_commands");

        use super::workflow_completion;
        use crate::protos::temporal::api::common::v1::Payloads;
        use crate::protos::temporal::api::enums::v1::QueryResultType;
        use crate::workflow::LEGACY_QUERY_ID;
        use std::fmt::{Display, Formatter};

        impl Display for WorkflowCommand {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                match &self.variant {
                    None => write!(f, "Empty"),
                    Some(v) => match v {
                        workflow_command::Variant::StartTimer(_) => write!(f, "StartTimer"),
                        workflow_command::Variant::ScheduleActivity(_) => {
                            write!(f, "ScheduleActivity")
                        }
                        workflow_command::Variant::RespondToQuery(_) => write!(f, "RespondToQuery"),
                        workflow_command::Variant::RequestCancelActivity(_) => {
                            write!(f, "RequestCancelActivity")
                        }
                        workflow_command::Variant::CancelTimer(_) => write!(f, "CancelTimer"),
                        workflow_command::Variant::CompleteWorkflowExecution(_) => {
                            write!(f, "CompleteWorkflowExecution")
                        }
                        workflow_command::Variant::FailWorkflowExecution(_) => {
                            write!(f, "FailWorkflowExecution")
                        }
                        workflow_command::Variant::ContinueAsNewWorkflowExecution(_) => {
                            write!(f, "ContinueAsNewWorkflowExecution")
                        }
                        workflow_command::Variant::AckExecutionCancelled(_) => {
                            write!(f, "AckExecutionCancelled")
                        }
                    },
                }
            }
        }

        impl QueryResult {
            /// Helper to construct the Temporal API query result types.
            pub fn into_components(self) -> (String, QueryResultType, Option<Payloads>, String) {
                match self {
                    QueryResult {
                        variant: Some(query_result::Variant::Succeeded(qs)),
                        query_id,
                    } => (
                        query_id,
                        QueryResultType::Answered,
                        qs.response.map(Into::into),
                        "".to_string(),
                    ),
                    QueryResult {
                        variant: Some(query_result::Variant::Failed(err)),
                        query_id,
                    } => (query_id, QueryResultType::Failed, None, err.message),
                    QueryResult {
                        variant: None,
                        query_id,
                    } => (
                        query_id,
                        QueryResultType::Failed,
                        None,
                        "Query response was empty".to_string(),
                    ),
                }
            }

            pub fn legacy_failure(fail: workflow_completion::Failure) -> Self {
                QueryResult {
                    query_id: LEGACY_QUERY_ID.to_string(),
                    variant: Some(query_result::Variant::Failed(
                        fail.failure.unwrap_or_default(),
                    )),
                }
            }
        }
    }

    pub type HistoryEventId = i64;

    impl From<wf_activation_job::Variant> for WfActivationJob {
        fn from(a: wf_activation_job::Variant) -> Self {
            WfActivationJob { variant: Some(a) }
        }
    }

    impl From<Vec<WorkflowCommand>> for workflow_completion::Success {
        fn from(v: Vec<WorkflowCommand>) -> Self {
            Self { commands: v }
        }
    }

    impl From<workflow_command::Variant> for WorkflowCommand {
        fn from(v: workflow_command::Variant) -> Self {
            WorkflowCommand { variant: Some(v) }
        }
    }

    impl workflow_completion::Success {
        pub fn from_variants(cmds: Vec<Variant>) -> Self {
            let cmds: Vec<_> = cmds
                .into_iter()
                .map(|c| WorkflowCommand { variant: Some(c) })
                .collect();
            cmds.into()
        }
    }

    impl WfActivationCompletion {
        /// Create a successful activation from a list of commands
        pub fn from_cmds(cmds: Vec<workflow_command::Variant>, run_id: String) -> Self {
            let success = workflow_completion::Success::from_variants(cmds);
            Self {
                run_id,
                status: Some(wf_activation_completion::Status::Successful(success)),
            }
        }

        /// Create a successful activation from just one command
        pub fn from_cmd(cmd: workflow_command::Variant, run_id: String) -> Self {
            let success = workflow_completion::Success::from_variants(vec![cmd]);
            Self {
                run_id,
                status: Some(wf_activation_completion::Status::Successful(success)),
            }
        }

        pub fn fail(run_id: String, failure: UserCodeFailure) -> Self {
            Self {
                run_id,
                status: Some(wf_activation_completion::Status::Failed(
                    workflow_completion::Failure {
                        failure: Some(failure),
                    },
                )),
            }
        }

        pub fn from_status(run_id: String, status: wf_activation_completion::Status) -> Self {
            Self {
                run_id,
                status: Some(status),
            }
        }

        /// Returns true if the activation contains a continue as new workflow execution command
        pub fn has_continue_as_new(&self) -> bool {
            if let Some(wf_activation_completion::Status::Successful(s)) = &self.status {
                return s.commands.iter().any(|wfc| {
                    matches!(
                        wfc,
                        WorkflowCommand {
                            variant: Some(
                                workflow_command::Variant::ContinueAsNewWorkflowExecution(_)
                            ),
                        }
                    )
                });
            }
            false
        }

        /// Returns true if the activation contains a complete workflow execution command
        pub fn has_complete_workflow_execution(&self) -> bool {
            if let Some(wf_activation_completion::Status::Successful(s)) = &self.status {
                return s.commands.iter().any(|wfc| {
                    matches!(
                        wfc,
                        WorkflowCommand {
                            variant: Some(workflow_command::Variant::CompleteWorkflowExecution(_)),
                        }
                    )
                });
            }
            false
        }
    }

    impl Display for WfActivationCompletion {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "WfActivationCompletion(run_id: {}, status: ",
                &self.run_id
            )?;
            match &self.status {
                None => write!(f, "empty")?,
                Some(s) => write!(f, "{}", s)?,
            };
            write!(f, ")")
        }
    }

    impl Display for wf_activation_completion::Status {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                wf_activation_completion::Status::Successful(workflow_completion::Success {
                    commands,
                }) => {
                    write!(f, "Success(")?;
                    for c in commands {
                        write!(f, " {} ", c)?;
                    }
                    write!(f, ")")
                }
                wf_activation_completion::Status::Failed(_) => {
                    write!(f, "Failed")
                }
            }
        }
    }

    impl ActivityResult {
        pub fn ok(result: Payload) -> Self {
            Self {
                status: Some(activity_result::activity_result::Status::Completed(
                    activity_result::Success {
                        result: Some(result),
                    },
                )),
            }
        }

        pub fn unwrap_ok_payload(self) -> Payload {
            match self.status.unwrap() {
                activity_result::activity_result::Status::Completed(c) => c.result.unwrap(),
                _ => panic!("Activity was not successful"),
            }
        }
    }

    impl ActivityTask {
        pub fn start_from_poll_resp(r: PollActivityTaskQueueResponse) -> Self {
            ActivityTask {
                task_token: r.task_token,
                activity_id: r.activity_id,
                variant: Some(activity_task::activity_task::Variant::Start(
                    activity_task::Start {
                        workflow_namespace: r.workflow_namespace,
                        workflow_type: r
                            .workflow_type
                            .map(|wt| wt.name)
                            .unwrap_or_else(|| "".to_string()),
                        workflow_execution: r.workflow_execution.map(Into::into),
                        activity_type: r
                            .activity_type
                            .map(|at| at.name)
                            .unwrap_or_else(|| "".to_string()),
                        header_fields: r.header.map(Into::into).unwrap_or_default(),
                        input: Vec::from_payloads(r.input),
                        heartbeat_details: Vec::from_payloads(r.heartbeat_details),
                        scheduled_time: r.scheduled_time,
                        current_attempt_scheduled_time: r.current_attempt_scheduled_time,
                        started_time: r.started_time,
                        attempt: r.attempt,
                        schedule_to_close_timeout: r.schedule_to_close_timeout,
                        start_to_close_timeout: r.start_to_close_timeout,
                        heartbeat_timeout: r.heartbeat_timeout,
                        retry_policy: r.retry_policy.map(Into::into),
                    },
                )),
            }
        }
    }

    impl From<UserCodeFailure> for Failure {
        fn from(f: UserCodeFailure) -> Self {
            Self {
                message: f.message,
                source: f.source,
                stack_trace: f.stack_trace,
                cause: f.cause.map(|b| Box::new((*b).into())),
                failure_info: Some(FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo {
                        r#type: f.r#type,
                        non_retryable: f.non_retryable,
                        details: None,
                    },
                )),
            }
        }
    }

    impl From<Failure> for UserCodeFailure {
        fn from(f: Failure) -> Self {
            let mut r#type = "".to_string();
            let mut non_retryable = false;
            if let Some(FailureInfo::ApplicationFailureInfo(fi)) = f.failure_info {
                r#type = fi.r#type;
                non_retryable = fi.non_retryable;
            }
            Self {
                message: f.message,
                r#type,
                source: f.source,
                stack_trace: f.stack_trace,
                non_retryable,
                cause: f.cause.map(|b| Box::new((*b).into())),
            }
        }
    }

    impl From<common::Payload> for super::temporal::api::common::v1::Payload {
        fn from(p: Payload) -> Self {
            Self {
                metadata: p.metadata,
                data: p.data,
            }
        }
    }

    impl From<super::temporal::api::common::v1::Payload> for common::Payload {
        fn from(p: super::temporal::api::common::v1::Payload) -> Self {
            Self {
                metadata: p.metadata,
                data: p.data,
            }
        }
    }

    pub trait PayloadsExt {
        fn into_payloads(self) -> Option<Payloads>;
        fn from_payloads(p: Option<Payloads>) -> Self;
    }

    impl PayloadsExt for Vec<common::Payload> {
        fn into_payloads(self) -> Option<Payloads> {
            if self.is_empty() {
                None
            } else {
                Some(Payloads {
                    payloads: self.into_iter().map(Into::into).collect(),
                })
            }
        }

        fn from_payloads(p: Option<Payloads>) -> Self {
            match p {
                None => vec![],
                Some(p) => p.payloads.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl From<common::Payload> for Payloads {
        fn from(p: Payload) -> Self {
            Self {
                payloads: vec![p.into()],
            }
        }
    }

    impl<T> From<T> for super::temporal::api::common::v1::Payload
    where
        T: AsRef<[u8]>,
    {
        fn from(v: T) -> Self {
            Self {
                metadata: Default::default(),
                data: v.as_ref().to_vec(),
            }
        }
    }

    impl<T> From<T> for Payloads
    where
        T: AsRef<[u8]>,
    {
        fn from(v: T) -> Self {
            Payloads {
                payloads: vec![v.into()],
            }
        }
    }

    /// Errors when converting from a [Payloads] api proto to our internal [common::Payload]
    #[derive(derive_more::Display, Debug)]
    pub enum PayloadsToPayloadError {
        MoreThanOnePayload,
        NoPayload,
    }
    impl TryFrom<Payloads> for common::Payload {
        type Error = PayloadsToPayloadError;

        fn try_from(mut v: Payloads) -> Result<Self, Self::Error> {
            match v.payloads.pop() {
                None => Err(PayloadsToPayloadError::NoPayload),
                Some(p) => {
                    if !v.payloads.is_empty() {
                        Err(PayloadsToPayloadError::MoreThanOnePayload)
                    } else {
                        Ok(p.into())
                    }
                }
            }
        }
    }

    impl From<WorkflowExecution> for common::WorkflowExecution {
        fn from(w: WorkflowExecution) -> Self {
            Self {
                workflow_id: w.workflow_id,
                run_id: w.run_id,
            }
        }
    }
}

// No need to lint these
#[allow(clippy::all)]
#[allow(missing_docs)]
// This is disgusting, but unclear to me how to avoid it. TODO: Discuss w/ prost maintainer
pub mod temporal {
    pub mod api {
        pub mod command {
            pub mod v1 {
                tonic::include_proto!("temporal.api.command.v1");

                use crate::protos::{
                    coresdk::{workflow_commands, PayloadsExt},
                    temporal::api::common::v1::ActivityType,
                    temporal::api::enums::v1::CommandType,
                };
                use command::Attributes;
                use std::fmt::{Display, Formatter};

                impl From<command::Attributes> for Command {
                    fn from(c: command::Attributes) -> Self {
                        match c {
                            a @ Attributes::StartTimerCommandAttributes(_) => Self {
                                command_type: CommandType::StartTimer as i32,
                                attributes: Some(a),
                            },
                            a @ Attributes::CancelTimerCommandAttributes(_) => Self {
                                command_type: CommandType::CancelTimer as i32,
                                attributes: Some(a),
                            },
                            a @ Attributes::CompleteWorkflowExecutionCommandAttributes(_) => Self {
                                command_type: CommandType::CompleteWorkflowExecution as i32,
                                attributes: Some(a),
                            },
                            a @ Attributes::FailWorkflowExecutionCommandAttributes(_) => Self {
                                command_type: CommandType::FailWorkflowExecution as i32,
                                attributes: Some(a),
                            },
                            a @ Attributes::ScheduleActivityTaskCommandAttributes(_) => Self {
                                command_type: CommandType::ScheduleActivityTask as i32,
                                attributes: Some(a),
                            },
                            a @ Attributes::RequestCancelActivityTaskCommandAttributes(_) => Self {
                                command_type: CommandType::RequestCancelActivityTask as i32,
                                attributes: Some(a),
                            },
                            a @ Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(_) => {
                                Self {
                                    command_type: CommandType::ContinueAsNewWorkflowExecution
                                        as i32,
                                    attributes: Some(a),
                                }
                            }
                            a @ Attributes::CancelWorkflowExecutionCommandAttributes(_) => Self {
                                command_type: CommandType::CancelWorkflowExecution as i32,
                                attributes: Some(a),
                            },
                            _ => unimplemented!(),
                        }
                    }
                }

                impl Display for Command {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        let ct = CommandType::from_i32(self.command_type)
                            .unwrap_or(CommandType::Unspecified);
                        write!(f, "{:?}", ct)
                    }
                }

                impl From<workflow_commands::StartTimer> for command::Attributes {
                    fn from(s: workflow_commands::StartTimer) -> Self {
                        Self::StartTimerCommandAttributes(StartTimerCommandAttributes {
                            timer_id: s.timer_id,
                            start_to_fire_timeout: s.start_to_fire_timeout,
                        })
                    }
                }

                impl From<workflow_commands::CancelTimer> for command::Attributes {
                    fn from(s: workflow_commands::CancelTimer) -> Self {
                        Self::CancelTimerCommandAttributes(CancelTimerCommandAttributes {
                            timer_id: s.timer_id,
                        })
                    }
                }

                impl From<workflow_commands::RequestCancelActivity> for command::Attributes {
                    fn from(c: workflow_commands::RequestCancelActivity) -> Self {
                        Self::RequestCancelActivityTaskCommandAttributes(
                            RequestCancelActivityTaskCommandAttributes {
                                scheduled_event_id: c.scheduled_event_id,
                            },
                        )
                    }
                }

                impl From<workflow_commands::ScheduleActivity> for command::Attributes {
                    fn from(s: workflow_commands::ScheduleActivity) -> Self {
                        Self::ScheduleActivityTaskCommandAttributes(
                            ScheduleActivityTaskCommandAttributes {
                                activity_id: s.activity_id,
                                activity_type: Some(ActivityType {
                                    name: s.activity_type,
                                }),
                                namespace: s.namespace,
                                task_queue: Some(s.task_queue.into()),
                                header: Some(s.header_fields.into()),
                                input: s.arguments.into_payloads(),
                                schedule_to_close_timeout: s.schedule_to_close_timeout,
                                schedule_to_start_timeout: s.schedule_to_start_timeout,
                                start_to_close_timeout: s.start_to_close_timeout,
                                heartbeat_timeout: s.heartbeat_timeout,
                                retry_policy: s.retry_policy.map(Into::into),
                            },
                        )
                    }
                }

                impl From<workflow_commands::CompleteWorkflowExecution> for command::Attributes {
                    fn from(c: workflow_commands::CompleteWorkflowExecution) -> Self {
                        Self::CompleteWorkflowExecutionCommandAttributes(
                            CompleteWorkflowExecutionCommandAttributes {
                                result: c.result.map(Into::into),
                            },
                        )
                    }
                }

                impl From<workflow_commands::FailWorkflowExecution> for command::Attributes {
                    fn from(c: workflow_commands::FailWorkflowExecution) -> Self {
                        Self::FailWorkflowExecutionCommandAttributes(
                            FailWorkflowExecutionCommandAttributes {
                                failure: c.failure.map(Into::into),
                            },
                        )
                    }
                }

                impl From<workflow_commands::ContinueAsNewWorkflowExecution> for command::Attributes {
                    fn from(c: workflow_commands::ContinueAsNewWorkflowExecution) -> Self {
                        Self::ContinueAsNewWorkflowExecutionCommandAttributes(
                            ContinueAsNewWorkflowExecutionCommandAttributes {
                                workflow_type: Some(c.workflow_type.into()),
                                task_queue: Some(c.task_queue.into()),
                                input: c.arguments.into_payloads(),
                                workflow_run_timeout: c.workflow_run_timeout,
                                workflow_task_timeout: c.workflow_task_timeout,
                                memo: Some(c.memo.into()),
                                header: Some(c.header.into()),
                                search_attributes: Some(c.search_attributes.into()),
                                ..Default::default()
                            },
                        )
                    }
                }

                impl From<workflow_commands::CancelWorkflowExecution> for command::Attributes {
                    fn from(_c: workflow_commands::CancelWorkflowExecution) -> Self {
                        Self::CancelWorkflowExecutionCommandAttributes(
                            CancelWorkflowExecutionCommandAttributes { details: None },
                        )
                    }
                }
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
                use crate::protos::coresdk::common;
                use std::collections::HashMap;
                tonic::include_proto!("temporal.api.common.v1");

                impl From<HashMap<String, common::Payload>> for Header {
                    fn from(h: HashMap<String, common::Payload>) -> Self {
                        Self {
                            fields: h.into_iter().map(|(k, v)| (k, v.into())).collect(),
                        }
                    }
                }

                impl From<Header> for HashMap<String, common::Payload> {
                    fn from(h: Header) -> Self {
                        h.fields.into_iter().map(|(k, v)| (k, v.into())).collect()
                    }
                }

                impl From<HashMap<String, common::Payload>> for Memo {
                    fn from(h: HashMap<String, common::Payload>) -> Self {
                        Self {
                            fields: h.into_iter().map(|(k, v)| (k, v.into())).collect(),
                        }
                    }
                }

                impl From<HashMap<String, common::Payload>> for SearchAttributes {
                    fn from(h: HashMap<String, common::Payload>) -> Self {
                        Self {
                            indexed_fields: h.into_iter().map(|(k, v)| (k, v.into())).collect(),
                        }
                    }
                }

                impl From<common::RetryPolicy> for RetryPolicy {
                    fn from(r: common::RetryPolicy) -> Self {
                        Self {
                            initial_interval: r.initial_interval,
                            backoff_coefficient: r.backoff_coefficient,
                            maximum_interval: r.maximum_interval,
                            maximum_attempts: r.maximum_attempts,
                            non_retryable_error_types: r.non_retryable_error_types,
                        }
                    }
                }

                impl From<RetryPolicy> for common::RetryPolicy {
                    fn from(r: RetryPolicy) -> Self {
                        Self {
                            initial_interval: r.initial_interval,
                            backoff_coefficient: r.backoff_coefficient,
                            maximum_interval: r.maximum_interval,
                            maximum_attempts: r.maximum_attempts,
                            non_retryable_error_types: r.non_retryable_error_types,
                        }
                    }
                }
            }
        }
        pub mod history {
            pub mod v1 {
                use crate::protos::temporal::api::{
                    enums::v1::EventType, history::v1::history_event::Attributes,
                };
                use prost::alloc::fmt::Formatter;
                use std::fmt::Display;

                tonic::include_proto!("temporal.api.history.v1");

                impl HistoryEvent {
                    /// Returns true if this is an event created to mirror a command
                    pub fn is_command_event(&self) -> bool {
                        if let Some(et) = EventType::from_i32(self.event_type) {
                            match et {
                                EventType::ActivityTaskScheduled
                                | EventType::ActivityTaskCancelRequested
                                | EventType::MarkerRecorded
                                | EventType::RequestCancelExternalWorkflowExecutionInitiated
                                | EventType::SignalExternalWorkflowExecutionInitiated
                                | EventType::StartChildWorkflowExecutionInitiated
                                | EventType::TimerCanceled
                                | EventType::TimerStarted
                                | EventType::UpsertWorkflowSearchAttributes
                                | EventType::WorkflowExecutionCanceled
                                | EventType::WorkflowExecutionCompleted
                                | EventType::WorkflowExecutionContinuedAsNew
                                | EventType::WorkflowExecutionFailed => true,
                                _ => false,
                            }
                        } else {
                            debug!(
                                "Could not determine type of event with enum index {}",
                                self.event_type
                            );
                            false
                        }
                    }

                    /// Returns the command's initiating event id, if present. This is the id of the
                    /// event which "started" the command. Usually, the "scheduled" event for the
                    /// command.
                    pub fn get_initial_command_event_id(&self) -> Option<i64> {
                        self.attributes.as_ref().and_then(|a| {
                            // Fun! Not really any way to make this better w/o incompatibly changing
                            // protos.
                            match a {
                                Attributes::ActivityTaskStartedEventAttributes(a) =>
                                    Some(a.scheduled_event_id),
                                Attributes::ActivityTaskCompletedEventAttributes(a) =>
                                    Some(a.scheduled_event_id),
                                Attributes::ActivityTaskFailedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::ActivityTaskTimedOutEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::ActivityTaskCancelRequestedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::ActivityTaskCanceledEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::TimerFiredEventAttributes(a) => Some(a.started_event_id),
                                Attributes::TimerCanceledEventAttributes(a) => Some(a.started_event_id),
                                Attributes::RequestCancelExternalWorkflowExecutionFailedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ExternalWorkflowExecutionCancelRequestedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::StartChildWorkflowExecutionFailedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionStartedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionCompletedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionFailedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionCanceledEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionTimedOutEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionTerminatedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::SignalExternalWorkflowExecutionFailedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ExternalWorkflowExecutionSignaledEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::WorkflowTaskStartedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::WorkflowTaskCompletedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::WorkflowTaskTimedOutEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::WorkflowTaskFailedEventAttributes(a) => Some(a.scheduled_event_id),
                                _ => None
                            }
                        })
                    }

                    /// Returns true if the event is one which would end a workflow
                    pub fn is_final_wf_execution_event(&self) -> bool {
                        match EventType::from_i32(self.event_type) {
                            Some(EventType::WorkflowExecutionCompleted) => true,
                            Some(EventType::WorkflowExecutionCanceled) => true,
                            Some(EventType::WorkflowExecutionFailed) => true,
                            Some(EventType::WorkflowExecutionTimedOut) => true,
                            Some(EventType::WorkflowExecutionContinuedAsNew) => true,
                            Some(EventType::WorkflowExecutionTerminated) => true,
                            _ => false,
                        }
                    }
                }

                impl Display for HistoryEvent {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        write!(
                            f,
                            "HistoryEvent(id: {}, {:?})",
                            self.event_id,
                            EventType::from_i32(self.event_type)
                        )
                    }
                }
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
                use crate::protos::temporal::api::enums::v1::TaskQueueKind;
                tonic::include_proto!("temporal.api.taskqueue.v1");

                impl From<String> for TaskQueue {
                    fn from(name: String) -> Self {
                        Self {
                            name,
                            kind: TaskQueueKind::Normal as i32,
                        }
                    }
                }
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
                use std::fmt::{Display, Formatter};

                tonic::include_proto!("temporal.api.workflowservice.v1");

                impl Display for PollWorkflowTaskQueueResponse {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        let last_event = self
                            .history
                            .as_ref()
                            .map(|h| h.events.last().map(|he| he.event_id))
                            .flatten()
                            .unwrap_or(0);
                        write!(
                            f,
                            "PollWFTQResp(run_id: {}, attempt: {}, last_event: {})",
                            self.workflow_execution
                                .as_ref()
                                .map(|we| we.run_id.as_str())
                                .unwrap_or(""),
                            self.attempt,
                            last_event
                        )
                    }
                }

                /// Can be used while debugging to avoid filling up a whole screen with poll resps
                pub struct CompactHist<'a>(pub &'a PollWorkflowTaskQueueResponse);
                impl Display for CompactHist<'_> {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        writeln!(
                            f,
                            "PollWorkflowTaskQueueResponse (prev_started: {}, started: {})",
                            self.0.previous_started_event_id, self.0.started_event_id
                        )?;
                        if let Some(h) = self.0.history.as_ref() {
                            for event in &h.events {
                                writeln!(f, "{}", event)?;
                            }
                        }
                        writeln!(f, "query: {:#?}", self.0.query)?;
                        writeln!(f, "queries: {:#?}", self.0.queries)
                    }
                }

                impl QueryWorkflowResponse {
                    /// Unwrap a successful response as vec of payloads
                    pub fn unwrap(self) -> Vec<crate::protos::temporal::api::common::v1::Payload> {
                        self.query_result.unwrap().payloads
                    }
                }
            }
        }
    }
}
