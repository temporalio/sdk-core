use crate::{InterceptedMetricsSvc, RawClientLike};
use anyhow::anyhow;
use std::marker::PhantomData;
use temporal_sdk_core_protos::{
    coresdk::{common::Payload, FromPayloadsExt},
    temporal::api::{
        common::v1::WorkflowExecution, enums::v1::HistoryEventFilterType, failure::v1::Failure,
        history::v1::history_event::Attributes,
        workflowservice::v1::GetWorkflowExecutionHistoryRequest,
    },
};

/// Enumerates terminal states for a particular workflow execution
// TODO: Add non-proto failure types, flesh out details, etc.
#[derive(Debug)]
pub enum WorkflowExecutionResult<T> {
    /// The workflow finished successfully
    Succeeded(T),
    /// The workflow finished in failure
    Failed(Failure),
    /// The workflow was cancelled
    Cancelled,
    /// The workflow was terminated
    Terminated,
    /// The workflow timed out
    TimedOut,
    /// The workflow continued as new
    ContinuedAsNew,
}

/// A workflow handle which can refer to a specific workflow run, or a chain of workflow runs with
/// the same workflow id.
pub struct WorkflowHandle<ClientT, ResultT> {
    client: ClientT,
    namespace: String,
    workflow_id: String,
    run_id: Option<String>,

    _res_type: PhantomData<ResultT>,
}

/// A workflow handle to a workflow with unknown types. Uses raw payloads.
pub type UntypedWorkflowHandle<CT> = WorkflowHandle<CT, Vec<Payload>>;

impl<CT, RT> WorkflowHandle<CT, RT>
where
    CT: RawClientLike<SvcType = InterceptedMetricsSvc> + Clone,
    // TODO: Make more generic, capable of (de)serialization w/ serde
    RT: FromPayloadsExt,
{
    pub(crate) fn new(
        client: CT,
        namespace: String,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Self {
        Self {
            client,
            namespace,
            workflow_id,
            run_id,
            _res_type: PhantomData::<RT>,
        }
    }

    pub async fn get_workflow_result(&self) -> Result<WorkflowExecutionResult<RT>, anyhow::Error> {
        let mut next_page_tok = vec![];
        loop {
            let server_res = self
                .client
                .clone()
                .client()
                .get_workflow_execution_history(GetWorkflowExecutionHistoryRequest {
                    namespace: self.namespace.to_string(),
                    execution: Some(WorkflowExecution {
                        workflow_id: self.workflow_id.clone(),
                        run_id: self.run_id.clone().unwrap_or_default(),
                    }),
                    skip_archival: true,
                    wait_new_event: true,
                    history_event_filter_type: HistoryEventFilterType::CloseEvent as i32,
                    next_page_token: next_page_tok.clone(),
                    ..Default::default()
                })
                .await?
                .into_inner();

            let mut history = server_res
                .history
                .ok_or(anyhow!("Server returned an empty history!"))?;

            if history.events.is_empty() {
                next_page_tok = server_res.next_page_token;
                continue;
            }

            let event_attrs = history.events.pop().and_then(|ev| ev.attributes);

            break match event_attrs {
                Some(Attributes::WorkflowExecutionCompletedEventAttributes(attrs)) => Ok(
                    WorkflowExecutionResult::Succeeded(RT::from_payloads(attrs.result)),
                ),
                Some(Attributes::WorkflowExecutionFailedEventAttributes(attrs)) => Ok(
                    WorkflowExecutionResult::Failed(attrs.failure.unwrap_or_default()),
                ),
                Some(Attributes::WorkflowExecutionCanceledEventAttributes(_attrs)) => {
                    Ok(WorkflowExecutionResult::Cancelled)
                }
                Some(Attributes::WorkflowExecutionTimedOutEventAttributes(_attrs)) => {
                    Ok(WorkflowExecutionResult::TimedOut)
                }
                Some(Attributes::WorkflowExecutionTerminatedEventAttributes(_attrs)) => {
                    Ok(WorkflowExecutionResult::Terminated)
                }
                Some(Attributes::WorkflowExecutionContinuedAsNewEventAttributes(_attrs)) => {
                    // TODO: Follow chains
                    Ok(WorkflowExecutionResult::ContinuedAsNew)
                }
                o => Err(anyhow!(
                    "Server returned an event that didn't match the CloseEvent filter. \
                     This is either a server bug or a new event the SDK does not understand. \
                     Event details: {:?}",
                    o
                )),
            };
        }
    }
}
