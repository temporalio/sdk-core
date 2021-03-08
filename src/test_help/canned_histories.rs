use crate::machines::test_help::TestHistoryBuilder;
use crate::protos::temporal::api::common::v1::Payload;
use crate::protos::temporal::api::enums::v1::{EventType, WorkflowTaskFailedCause};
use crate::protos::temporal::api::failure::v1::Failure;
use crate::protos::temporal::api::history::v1::{
    history_event, ActivityTaskCompletedEventAttributes, ActivityTaskScheduledEventAttributes,
    ActivityTaskStartedEventAttributes, TimerCanceledEventAttributes, TimerFiredEventAttributes,
};

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_TIMER_STARTED
///  6: EVENT_TYPE_TIMER_FIRED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn single_timer(timer_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    t.add(
        EventType::TimerFired,
        history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
            started_event_id: timer_started_event_id,
            timer_id: timer_id.to_string(),
        }),
    );
    t.add_workflow_task_scheduled_and_started();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_TIMER_STARTED (cancel)
///  6: EVENT_TYPE_TIMER_STARTED (wait)
///  7: EVENT_TYPE_TIMER_FIRED (wait)
///  8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  9: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 10: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 11: EVENT_TYPE_TIMER_CANCELED (cancel)
/// 12: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn cancel_timer(wait_timer_id: &str, cancel_timer_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let cancel_timer_started_id = t.add_get_event_id(EventType::TimerStarted, None);
    let wait_timer_started_id = t.add_get_event_id(EventType::TimerStarted, None);
    t.add(
        EventType::TimerFired,
        history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
            started_event_id: wait_timer_started_id,
            timer_id: wait_timer_id.to_string(),
        }),
    );
    // 8
    t.add_full_wf_task();
    // 11
    t.add(
        EventType::TimerCanceled,
        history_event::Attributes::TimerCanceledEventAttributes(TimerCanceledEventAttributes {
            started_event_id: cancel_timer_started_id,
            timer_id: cancel_timer_id.to_string(),
            ..Default::default()
        }),
    );
    // 12
    t.add_workflow_execution_completed();
    t
}

/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_TIMER_STARTED
/// 6: EVENT_TYPE_TIMER_STARTED
/// 7: EVENT_TYPE_TIMER_FIRED
/// 8: EVENT_TYPE_TIMER_FIRED
/// 9: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 10: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn parallel_timer(timer1: &str, timer2: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    let timer_2_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    t.add(
        EventType::TimerFired,
        history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
            started_event_id: timer_started_event_id,
            timer_id: timer1.to_string(),
        }),
    );
    t.add(
        EventType::TimerFired,
        history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
            started_event_id: timer_2_started_event_id,
            timer_id: timer2.to_string(),
        }),
    );
    t.add_workflow_task_scheduled_and_started();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_TIMER_STARTED
///  6: EVENT_TYPE_TIMER_FIRED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_FAILED
/// 10: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 11: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn workflow_fails_with_reset_after_timer(
    timer_id: &str,
    original_run_id: &str,
) -> TestHistoryBuilder {
    let mut t = single_timer(timer_id);
    t.add_workflow_task_failed_new_id(WorkflowTaskFailedCause::ResetWorkflow, original_run_id);

    t.add_workflow_task_scheduled_and_started();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_TIMER_STARTED
///  6: EVENT_TYPE_TIMER_FIRED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_FAILED
/// 10: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 11: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn workflow_fails_with_failure_after_timer(timer_id: &str) -> TestHistoryBuilder {
    let mut t = single_timer(timer_id);
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
        Failure {
            message: "boom".to_string(),
            ..Default::default()
        },
    );

    t.add_workflow_task_scheduled_and_started();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
///  6: EVENT_TYPE_ACTIVITY_TASK_STARTED
///  7: EVENT_TYPE_ACTIVITY_TASK_COMPLETED
///  8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  9: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn single_activity(activity_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_get_event_id(
        EventType::ActivityTaskScheduled,
        Some(
            history_event::Attributes::ActivityTaskScheduledEventAttributes(
                ActivityTaskScheduledEventAttributes {
                    activity_id: activity_id.to_string(),
                    ..Default::default()
                },
            ),
        ),
    );
    let started_event_id = t.add_get_event_id(
        EventType::ActivityTaskStarted,
        Some(
            history_event::Attributes::ActivityTaskStartedEventAttributes(
                ActivityTaskStartedEventAttributes {
                    scheduled_event_id,
                    ..Default::default()
                },
            ),
        ),
    );
    t.add(
        EventType::ActivityTaskCompleted,
        history_event::Attributes::ActivityTaskCompletedEventAttributes(
            ActivityTaskCompletedEventAttributes {
                scheduled_event_id,
                started_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_workflow_task_scheduled_and_started();
    t
}

/// First signal's payload is "hello " and second is "world" (no metadata for either)
/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 6: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 8: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn two_signals(sig_1_id: &str, sig_2_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled(
        sig_1_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_we_signaled(
        sig_2_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"world".to_vec(),
        }],
    );
    t.add_workflow_task_scheduled_and_started();
    t
}
