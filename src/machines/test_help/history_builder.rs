use super::Result;
use crate::protosext::HistoryInfo;
use crate::{machines::{workflow_machines::WorkflowMachines, MachineCommand}, protos::temporal::api::{
    enums::v1::EventType,
    history::v1::{
        history_event::Attributes, HistoryEvent, TimerStartedEventAttributes,
        WorkflowExecutionStartedEventAttributes, WorkflowTaskCompletedEventAttributes,
        WorkflowTaskScheduledEventAttributes, WorkflowTaskStartedEventAttributes,
    },
}, HistoryInfoError};
use anyhow::bail;
use std::time::SystemTime;

#[derive(Default, Debug)]
pub struct TestHistoryBuilder {
    events: Vec<HistoryEvent>,
    /// Is incremented every time a new event is added, and that *new* value is used as that event's
    /// id
    current_event_id: i64,
    workflow_task_scheduled_event_id: i64,
    previous_started_event_id: i64,
}

impl TestHistoryBuilder {
    /// Add an event by type with attributes. Bundles both into a [HistoryEvent] with an id that is
    /// incremented on each call to add.
    pub fn add(&mut self, event_type: EventType, attribs: Attributes) {
        self.build_and_push_event(event_type, attribs);
    }

    /// Adds an event to the history by type, with default attributes.
    pub fn add_by_type(&mut self, event_type: EventType) {
        let attribs =
            default_attribs(event_type).expect("Couldn't make default attributes in test builder");
        self.build_and_push_event(event_type.clone(), attribs);
    }

    /// Adds an event, returning the ID that was assigned to it
    pub fn add_get_event_id(&mut self, event_type: EventType, attrs: Option<Attributes>) -> i64 {
        if let Some(a) = attrs {
            self.build_and_push_event(event_type, a);
        } else {
            self.add_by_type(event_type);
        }
        self.current_event_id
    }

    /// Adds the following events:
    /// ```text
    /// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    /// EVENT_TYPE_WORKFLOW_TASK_STARTED
    /// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    /// ```
    pub fn add_workflow_task(&mut self) {
        self.add_workflow_task_scheduled_and_started();
        self.add_workflow_task_completed();
    }

    pub fn add_workflow_task_scheduled_and_started(&mut self) {
        self.add_workflow_task_scheduled();
        self.add_workflow_task_started();
    }

    pub fn add_workflow_task_scheduled(&mut self) {
        // WFStarted always immediately follows WFScheduled
        self.previous_started_event_id = self.workflow_task_scheduled_event_id + 1;
        self.workflow_task_scheduled_event_id =
            self.add_get_event_id(EventType::WorkflowTaskScheduled, None);
    }

    pub fn add_workflow_task_started(&mut self) {
        let attrs = WorkflowTaskStartedEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowTaskStarted, attrs.into());
    }

    pub fn add_workflow_task_completed(&mut self) {
        let attrs = WorkflowTaskCompletedEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowTaskCompleted, attrs.into());
    }

    /// Counts the number of whole workflow tasks. Looks for WFTaskStarted followed by
    /// WFTaskCompleted, adding one to the count for every match. It will additionally count
    /// a WFTaskStarted at the end of the event list.
    ///
    /// If `up_to_event_id` is provided, the count will be returned as soon as processing advances
    /// past that id.
    pub fn get_workflow_task_count(&self, up_to_event_id: Option<i64>) -> Result<usize> {
        let mut last_wf_started_id = 0;
        let mut count = 0;
        let mut history = self.events.iter().peekable();
        while let Some(event) = history.next() {
            let next_event = history.peek();
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
                    bail!("Last item in history wasn't WorkflowTaskStarted")
                }
                return Ok(count);
            }
        }
        Ok(count)
    }

    /// Handle workflow task(s) using the provided [WorkflowMachines]. Will process as many workflow
    /// tasks as the provided `to_wf_task_num` parameter..
    ///
    /// # Panics
    /// * Can panic if the passed in machines have been manipulated outside of this builder
    pub(crate) fn handle_workflow_task_take_cmds(
        &self,
        wf_machines: &mut WorkflowMachines,
        to_wf_task_num: Option<usize>,
    ) -> Result<Vec<MachineCommand>> {
        self.handle_workflow_task(wf_machines, to_wf_task_num)?;
        Ok(wf_machines.get_commands())
    }

    fn handle_workflow_task(
        &self,
        wf_machines: &mut WorkflowMachines,
        to_wf_task_num: Option<usize>,
    ) -> Result<()> {
        let to_wf_task_num = to_wf_task_num.unwrap_or(usize::MAX);
        let (_, events) = self
            .events
            .split_at(wf_machines.get_last_started_event_id() as usize);
        let mut history = events.iter().peekable();

        let hist_info = self.get_history_info(to_wf_task_num)?;
        wf_machines.set_started_ids(
            hist_info.previous_started_event_id,
            hist_info.workflow_task_started_event_id,
        );
        let mut started_id = hist_info.previous_started_event_id;
        let mut num_seen_wf_tasks = if wf_machines.get_last_started_event_id() > 0 {
            self.get_workflow_task_count(history.peek().map(|e| e.event_id - 1))?
        } else {
            0
        };

        while let Some(event) = history.next() {
            let next_event = history.peek();

            if event.event_type == EventType::WorkflowTaskStarted as i32 {
                let next_is_completed = next_event.map_or(false, |ne| {
                    ne.event_type == EventType::WorkflowTaskCompleted as i32
                });
                let next_is_failed_or_timeout = next_event.map_or(false, |ne| {
                    ne.event_type == EventType::WorkflowTaskFailed as i32
                        || ne.event_type == EventType::WorkflowTaskTimedOut as i32
                });

                if next_event.is_none() || next_is_completed {
                    started_id = event.event_id;
                    num_seen_wf_tasks += 1;
                    if num_seen_wf_tasks == to_wf_task_num || next_event.is_none() {
                        wf_machines.handle_event(event, false)?;
                        return Ok(());
                    }
                } else if next_event.is_some() && !next_is_failed_or_timeout {
                    bail!(
                        "Invalid history! Event {:?} should be WF task completed, \
                         failed, or timed out.",
                        &event
                    );
                }
            }

            wf_machines.handle_event(event, next_event.is_some())?;

            if next_event.is_none() {
                if event.is_final_wf_execution_event() {
                    return Ok(());
                }
                if started_id != event.event_id {
                    bail!(
                        "The last event in the history (id {}) isn't the last WF task \
                           started (id {})",
                        event.event_id,
                        started_id
                    );
                }
                unreachable!()
            }
        }

        Ok(())
    }

    /// Iterates over the events in this builder to return a [HistoryInfo] of the n-th workflow task.
    pub(crate) fn get_history_info(
        &self,
        to_wf_task_num: usize,
    ) -> Result<HistoryInfo, HistoryInfoError> {
        HistoryInfo::new_from_events(self.events.clone(), to_wf_task_num)
    }

    fn build_and_push_event(&mut self, event_type: EventType, attribs: Attributes) {
        self.current_event_id += 1;
        let evt = HistoryEvent {
            event_type: event_type as i32,
            event_id: self.current_event_id,
            event_time: Some(SystemTime::now().into()),
            attributes: Some(attribs),
            ..Default::default()
        };
        self.events.push(evt);
    }
}

fn default_attribs(et: EventType) -> Result<Attributes> {
    Ok(match et {
        EventType::WorkflowExecutionStarted => {
            WorkflowExecutionStartedEventAttributes::default().into()
        }
        EventType::WorkflowTaskScheduled => WorkflowTaskScheduledEventAttributes::default().into(),
        EventType::TimerStarted => TimerStartedEventAttributes::default().into(),
        _ => bail!("Don't know how to construct default attrs for {:?}", et),
    })
}
