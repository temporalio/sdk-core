//! Contains the protobuf definitions used as arguments to and return values from interactions with
//! [super::Core]. Language SDK authors can generate structs using the proto definitions that will match
//! the generated structs in this module.

#[allow(clippy::large_enum_variant)]
// I'd prefer not to do this, but there are some generated things that just don't need it.
#[allow(missing_docs)]
pub mod coresdk {
    //! Contains all protobufs relating to communication between core and lang-specific SDKs

    include!("coresdk.rs");
    pub mod activity_task {
        include!("coresdk.activity_task.rs");
    }
    pub mod activity_result {
        include!("coresdk.activity_result.rs");
    }
    pub mod common {
        include!("coresdk.common.rs");
    }
    pub mod workflow_activation {
        include!("coresdk.workflow_activation.rs");
    }
    pub mod workflow_completion {
        include!("coresdk.workflow_completion.rs");
    }
    pub mod workflow_commands {
        include!("coresdk.workflow_commands.rs");
    }

    use super::temporal::api::command::v1 as api_command;
    use super::temporal::api::command::v1::Command as ApiCommand;
    use super::temporal::api::failure::v1::Failure;
    use crate::protos::coresdk::common::{Payload, UserCodeFailure};
    use crate::protos::coresdk::workflow_activation::SignalWorkflow;
    use crate::protos::temporal::api::common::v1::Payloads;
    use crate::protos::temporal::api::failure::v1::failure::FailureInfo;
    use crate::protos::temporal::api::failure::v1::ApplicationFailureInfo;
    use crate::protos::temporal::api::history::v1::WorkflowExecutionSignaledEventAttributes;
    use workflow_activation::{wf_activation_job, WfActivation, WfActivationJob};
    use workflow_commands::{workflow_command, WorkflowCommand};
    use workflow_completion::{wf_activation_completion, WfActivationCompletion};

    pub type HistoryEventId = i64;

    impl Task {
        pub fn from_wf_task(task_token: Vec<u8>, t: WfActivation) -> Self {
            Task {
                task_token,
                variant: Some(t.into()),
            }
        }

        /// Returns any contained jobs if this task was a wf activation and it had some
        pub fn get_wf_jobs(&self) -> Vec<WfActivationJob> {
            if let Some(task::Variant::Workflow(a)) = &self.variant {
                a.jobs.clone()
            } else {
                vec![]
            }
        }

        /// Returns the workflow run id if the task was a workflow
        pub fn get_run_id(&self) -> Option<&str> {
            if let Some(task::Variant::Workflow(a)) = &self.variant {
                Some(&a.run_id)
            } else {
                None
            }
        }
    }

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

    impl TaskCompletion {
        /// Build a successful completion from some api command attributes and a task token
        pub fn ok_from_api_attrs(
            cmds: Vec<workflow_command::Variant>,
            task_token: Vec<u8>,
        ) -> Self {
            let cmds: Vec<_> = cmds
                .into_iter()
                .map(|c| WorkflowCommand { variant: Some(c) })
                .collect();
            let success: workflow_completion::Success = cmds.into();
            TaskCompletion {
                task_token,
                variant: Some(task_completion::Variant::Workflow(WfActivationCompletion {
                    status: Some(wf_activation_completion::Status::Successful(success)),
                })),
            }
        }

        pub fn fail(task_token: Vec<u8>, failure: UserCodeFailure) -> Self {
            Self {
                task_token,
                variant: Some(task_completion::Variant::Workflow(WfActivationCompletion {
                    status: Some(wf_activation_completion::Status::Failed(
                        workflow_completion::Failure {
                            failure: Some(failure),
                        },
                    )),
                })),
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

    impl From<WorkflowExecutionSignaledEventAttributes> for SignalWorkflow {
        fn from(a: WorkflowExecutionSignaledEventAttributes) -> Self {
            Self {
                signal_name: a.signal_name,
                input: Vec::from_payloads(a.input),
                identity: a.identity,
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
                include!("temporal.api.command.v1.rs");
                use crate::protos::coresdk::workflow_commands::{
                    CompleteWorkflowExecution, ScheduleActivity,
                };
                use crate::protos::coresdk::{workflow_commands, PayloadsExt};
                use crate::protos::temporal::api::common::v1::ActivityType;
                use crate::protos::temporal::api::enums::v1::CommandType;
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
                                result: c.result.into_payloads(),
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
                use crate::protos::coresdk::common;
                use std::collections::HashMap;
                include!("temporal.api.common.v1.rs");

                impl From<HashMap<String, common::Payload>> for Header {
                    fn from(h: HashMap<String, common::Payload>) -> Self {
                        Self {
                            fields: h.into_iter().map(|(k, v)| (k, v.into())).collect(),
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
            }
        }
        pub mod history {
            pub mod v1 {
                use crate::protos::temporal::api::{
                    enums::v1::EventType, history::v1::history_event::Attributes,
                };
                use crate::protosext::HistoryInfoError;
                use prost::alloc::fmt::Formatter;
                use std::fmt::Display;

                include!("temporal.api.history.v1.rs");

                impl History {
                    /// Counts the number of whole workflow tasks. Looks for WFTaskStarted followed
                    /// by WFTaskCompleted, adding one to the count for every match. It will
                    /// additionally count a WFTaskStarted at the end of the event list.
                    ///
                    /// If `up_to_event_id` is provided, the count will be returned as soon as
                    /// processing advances past that id.
                    pub(crate) fn get_workflow_task_count(
                        &self,
                        up_to_event_id: Option<i64>,
                    ) -> Result<usize, HistoryInfoError> {
                        let mut last_wf_started_id = 0;
                        let mut count = 0;
                        let mut history = self.events.iter().peekable();
                        while let Some(event) = history.next() {
                            let next_event = history.peek();

                            if event.is_final_wf_execution_event() {
                                // If the workflow is complete, we're done.
                                return Ok(count);
                            }

                            if let Some(upto) = up_to_event_id {
                                if event.event_id > upto {
                                    return Ok(count);
                                }
                            }

                            let next_is_completed = next_event.map_or(false, |ne| {
                                ne.event_type == EventType::WorkflowTaskCompleted as i32
                            });

                            if event.event_type == EventType::WorkflowTaskStarted as i32
                                && (next_event.is_none() || next_is_completed)
                            {
                                last_wf_started_id = event.event_id;
                                count += 1;
                            }

                            if next_event.is_none() {
                                if last_wf_started_id != event.event_id {
                                    return Err(HistoryInfoError::HistoryEndsUnexpectedly);
                                }
                                return Ok(count);
                            }
                        }
                        Ok(count)
                    }
                }

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
                use crate::protos::temporal::api::enums::v1::TaskQueueKind;
                include!("temporal.api.taskqueue.v1.rs");

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
