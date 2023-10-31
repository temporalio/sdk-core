//! This module implements traits defined in the client to dispatch a
//! WFT to a worker bypassing the server.
//! This enables latency optimizations such as Eager Workflow Start.

use anyhow::Context;
use std::fmt::{Debug, Formatter};

use crate::{
    abstractions::{MeteredSemaphore, OwnedMeteredSemPermit},
    protosext::ValidPollWFTQResponse,
    worker::workflow::wft_poller::validate_wft,
};

use std::sync::Arc;
use temporal_client::{Slot as SlotTrait, SlotProvider as SlotProviderTrait};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;
use tokio::sync::mpsc::UnboundedSender;
use tonic::Status;

type WFTStreamSender =
    UnboundedSender<Result<(ValidPollWFTQResponse, OwnedMeteredSemPermit), Status>>;

pub struct Slot {
    permit: Option<OwnedMeteredSemPermit>,
    external_wft_tx: WFTStreamSender,
}

impl Slot {
    fn new(permit: OwnedMeteredSemPermit, external_wft_tx: WFTStreamSender) -> Self {
        Self {
            permit: Some(permit),
            external_wft_tx,
        }
    }
}

impl SlotTrait for Slot {
    fn schedule_wft(&mut self, task: PollWorkflowTaskQueueResponse) -> Result<(), anyhow::Error> {
        let wft = validate_wft(task)?;
        let permit = self
            .permit
            .take()
            .context("Calling 'schedule_wft()' multiple times")?;
        self.external_wft_tx.send(Ok((wft, permit)))?;
        Ok(())
    }
}

pub struct SlotProvider {
    uuid: String,
    namespace: String,
    task_queue: String,
    wft_semaphore: Arc<MeteredSemaphore>,
    external_wft_tx: WFTStreamSender,
}

impl Debug for SlotProvider {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "SlotProvider: uuid={} namespace={} task_queue={}",
            self.uuid, self.namespace, self.task_queue,
        )
    }
}

impl SlotProvider {
    pub(crate) fn new(
        uuid: String,
        namespace: String,
        task_queue: String,
        wft_semaphore: Arc<MeteredSemaphore>,
        external_wft_tx: WFTStreamSender,
    ) -> Self {
        Self {
            uuid,
            namespace,
            task_queue,
            wft_semaphore,
            external_wft_tx,
        }
    }
}

impl SlotProviderTrait for SlotProvider {
    fn uuid(&self) -> String {
        self.uuid.clone()
    }
    fn namespace(&self) -> String {
        self.namespace.clone()
    }
    fn task_queue(&self) -> String {
        self.task_queue.clone()
    }
    fn try_reserve_wft_slot(&self) -> Option<Box<dyn SlotTrait>> {
        match self.wft_semaphore.try_acquire_owned().ok() {
            Some(permit) => Some(Box::new(Slot::new(permit, self.external_wft_tx.clone()))),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use temporal_sdk_core_protos::temporal::api::{
        common::v1::{WorkflowExecution, WorkflowType},
        history::v1::History,
        taskqueue::v1::TaskQueue,
    };
    use tokio::sync::mpsc::unbounded_channel;

    // make validate_wft() happy
    fn new_validatable_response() -> PollWorkflowTaskQueueResponse {
        PollWorkflowTaskQueueResponse {
            workflow_execution_task_queue: Some(TaskQueue::default()),
            workflow_execution: Some(WorkflowExecution::default()),
            workflow_type: Some(WorkflowType::default()),
            history: Some(History::default()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn slot_can_only_be_used_once() {
        let wft_semaphore = Arc::new(MeteredSemaphore::new(
            2,
            crate::MetricsContext::no_op(),
            |_, _| {},
        ));
        let (external_wft_tx, mut external_wft_rx) = unbounded_channel();

        let provider = SlotProvider::new(
            "my_id".to_string(),
            "my_namespace".to_string(),
            "my_queue".to_string(),
            wft_semaphore,
            external_wft_tx,
        );

        if let Some(mut slot) = provider.try_reserve_wft_slot() {
            let p = slot.schedule_wft(new_validatable_response());
            assert!(p.is_ok());

            let p = slot.schedule_wft(new_validatable_response());
            assert!(p.is_err());

            assert!(external_wft_rx.recv().await.is_some());
        }
    }

    #[tokio::test]
    async fn channel_closes_when_provider_drops() {
        let (external_wft_tx, mut external_wft_rx) = unbounded_channel();
        {
            let external_wft_tx = external_wft_tx;
            let wft_semaphore = Arc::new(MeteredSemaphore::new(
                2,
                crate::MetricsContext::no_op(),
                |_, _| {},
            ));
            let provider = SlotProvider::new(
                "my_id".to_string(),
                "my_namespace".to_string(),
                "my_queue".to_string(),
                wft_semaphore,
                external_wft_tx,
            );
            assert!(provider.try_reserve_wft_slot().is_some());
        }
        assert!(external_wft_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn unused_slots_reclaimed() {
        let wft_semaphore = Arc::new(MeteredSemaphore::new(
            2,
            crate::MetricsContext::no_op(),
            |_, _| {},
        ));
        {
            let wft_semaphore = wft_semaphore.clone();
            let (external_wft_tx, _) = unbounded_channel();
            let provider = SlotProvider::new(
                "my_id".to_string(),
                "my_namespace".to_string(),
                "my_queue".to_string(),
                wft_semaphore.clone(),
                external_wft_tx,
            );
            let slot = provider.try_reserve_wft_slot();
            assert!(slot.is_some());
            assert_eq!(wft_semaphore.available_permits(), 1);
            // drop slot without using it
        }
        assert_eq!(wft_semaphore.available_permits(), 2);
    }
}
