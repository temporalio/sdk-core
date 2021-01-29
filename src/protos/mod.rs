#[allow(clippy::large_enum_variant)]
pub mod coresdk {
    include!("coresdk.rs");
    use super::temporal::api::command::v1 as api_command;
    use super::temporal::api::command::v1::Command as ApiCommand;
    use crate::protos::coresdk::complete_task_req::Completion;
    use command::Variant;

    pub type HistoryEventId = i64;

    impl Task {
        pub fn from_wf_task(task_token: Vec<u8>, t: WfActivation) -> Self {
            Task {
                task_token,
                variant: Some(t.into()),
            }
        }
    }

    impl From<Vec<ApiCommand>> for WfActivationSuccess {
        fn from(v: Vec<ApiCommand>) -> Self {
            WfActivationSuccess {
                commands: v
                    .into_iter()
                    .map(|cmd| Command {
                        variant: Some(Variant::Api(cmd)),
                    })
                    .collect(),
            }
        }
    }

    impl CompleteTaskReq {
        /// Build a successful completion from some api command attributes and a task token
        pub fn ok_from_api_attrs(
            cmd: api_command::command::Attributes,
            task_token: Vec<u8>,
        ) -> Self {
            let cmd: ApiCommand = cmd.into();
            let success: WfActivationSuccess = vec![cmd].into();
            CompleteTaskReq {
                task_token,
                completion: Some(Completion::Workflow(WfActivationCompletion {
                    status: Some(wf_activation_completion::Status::Successful(success)),
                })),
            }
        }
    }
}

// No need to lint these
#[allow(clippy::all)]
// This is disgusting, but unclear to me how to avoid it. TODO: Discuss w/ prost maintainer
pub mod temporal {
    pub mod api {
        pub mod command {
            pub mod v1 {
                include!("temporal.api.command.v1.rs");
                use crate::protos::temporal::api::enums::v1::CommandType;
                use command::Attributes;

                impl From<command::Attributes> for Command {
                    fn from(c: command::Attributes) -> Self {
                        match c {
                            a @ Attributes::StartTimerCommandAttributes(_) => Self {
                                command_type: CommandType::StartTimer as i32,
                                attributes: Some(a),
                            },
                            a @ Attributes::CompleteWorkflowExecutionCommandAttributes(_) => Self {
                                command_type: CommandType::CompleteWorkflowExecution as i32,
                                attributes: Some(a),
                            },
                            _ => unimplemented!(),
                        }
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
                include!("temporal.api.common.v1.rs");
            }
        }
        pub mod history {
            pub mod v1 {
                use crate::protos::temporal::api::{
                    enums::v1::EventType, history::v1::history_event::Attributes,
                };
                use prost::alloc::fmt::Formatter;
                use std::fmt::Display;

                include!("temporal.api.history.v1.rs");

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
