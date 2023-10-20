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
