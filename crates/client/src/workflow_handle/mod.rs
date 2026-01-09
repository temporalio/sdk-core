use crate::WorkflowService;
use anyhow::{anyhow, bail};
use std::{fmt::Debug, marker::PhantomData};
use temporalio_common::{
    WorkflowDefinition,
    data_converters::{GenericPayloadConverter, PayloadConverter, RawValue, SerializationContext},
    protos::{
        coresdk::FromPayloadsExt,
        temporal::api::{
            common::v1::{Payload, WorkflowExecution},
            enums::v1::HistoryEventFilterType,
            failure::v1::Failure,
            history::v1::history_event::Attributes,
            workflowservice::v1::GetWorkflowExecutionHistoryRequest,
        },
    },
};
use tonic::IntoRequest;

/// Enumerates terminal states for a particular workflow execution
// TODO: Add non-proto failure types, flesh out details, etc.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum WorkflowExecutionResult<T> {
    /// The workflow finished successfully
    Succeeded(T),
    /// The workflow finished in failure
    Failed(Failure),
    /// The workflow was cancelled
    Cancelled(Vec<Payload>),
    /// The workflow was terminated
    Terminated(Vec<Payload>),
    /// The workflow timed out
    TimedOut,
    /// The workflow continued as new
    ContinuedAsNew,
}

impl<T> WorkflowExecutionResult<T>
where
    T: Debug,
{
    /// Unwrap the result, panicking if it was not a success
    pub fn unwrap_success(self) -> T {
        match self {
            Self::Succeeded(t) => t,
            o => panic!("Expected success, got {o:?}"),
        }
    }
}

/// Options for fetching workflow results
#[derive(Debug, Clone, Copy)]
pub struct GetWorkflowResultOptions {
    /// If true (the default), follows to the next workflow run in the execution chain while
    /// retrieving results.
    pub follow_runs: bool,
}
impl Default for GetWorkflowResultOptions {
    fn default() -> Self {
        Self { follow_runs: true }
    }
}

/// A workflow handle which can refer to a specific workflow run, or a chain of workflow runs with
/// the same workflow id.
pub struct WorkflowHandle<ClientT, W> {
    client: ClientT,
    info: WorkflowExecutionInfo,

    _wf_type: PhantomData<W>,
}

/// Holds needed information to refer to a specific workflow run, or workflow execution chain
#[derive(Debug)]
pub struct WorkflowExecutionInfo {
    /// Namespace the workflow lives in
    pub namespace: String,
    /// The workflow's id
    pub workflow_id: String,
    /// If set, target this specific run of the workflow
    pub run_id: Option<String>,
}

impl WorkflowExecutionInfo {
    /// Bind the workflow info to a specific client, turning it into a workflow handle
    pub fn bind_untyped<CT>(self, client: CT) -> UntypedWorkflowHandle<CT>
    where
        CT: WorkflowService + Clone,
    {
        UntypedWorkflowHandle::new(client, self)
    }
}

/// A workflow handle to a workflow with unknown types. Uses raw payloads.
pub type UntypedWorkflowHandle<CT> = WorkflowHandle<CT, UntypedWorkflow>;

/// Marker type for untyped workflow handles
pub struct UntypedWorkflow;
impl WorkflowDefinition for UntypedWorkflow {
    // TODO: Need to handle multiargs
    type Input = RawValue;
    type Output = RawValue;
    fn name() -> &'static str {
        ""
    }
}

impl<CT, W> WorkflowHandle<CT, W>
where
    CT: WorkflowService + Clone,
    W: WorkflowDefinition,
{
    pub(crate) fn new(client: CT, info: WorkflowExecutionInfo) -> Self {
        Self {
            client,
            info,
            _wf_type: PhantomData::<W>,
        }
    }

    /// Get the workflow execution info
    pub fn info(&self) -> &WorkflowExecutionInfo {
        &self.info
    }

    /// Get the client attached to this handle
    pub fn client(&self) -> &CT {
        &self.client
    }

    /// Await the result of the workflow execution
    pub async fn get_workflow_result(
        &self,
        opts: GetWorkflowResultOptions,
    ) -> Result<WorkflowExecutionResult<W::Output>, anyhow::Error> {
        let mut next_page_tok = vec![];
        let mut run_id = self.info.run_id.clone().unwrap_or_default();
        loop {
            let server_res = self
                .client
                .clone()
                .get_workflow_execution_history(
                    GetWorkflowExecutionHistoryRequest {
                        namespace: self.info.namespace.to_string(),
                        execution: Some(WorkflowExecution {
                            workflow_id: self.info.workflow_id.clone(),
                            run_id: run_id.clone(),
                        }),
                        skip_archival: true,
                        wait_new_event: true,
                        history_event_filter_type: HistoryEventFilterType::CloseEvent as i32,
                        next_page_token: next_page_tok.clone(),
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await?
                .into_inner();

            let mut history = server_res
                .history
                .ok_or_else(|| anyhow!("Server returned an empty history!"))?;

            if history.events.is_empty() {
                next_page_tok = server_res.next_page_token;
                continue;
            }
            // If page token was previously set, clear it.
            next_page_tok = vec![];

            let event_attrs = history.events.pop().and_then(|ev| ev.attributes);

            macro_rules! follow {
                ($attrs:ident) => {
                    if opts.follow_runs && $attrs.new_execution_run_id != "" {
                        run_id = $attrs.new_execution_run_id;
                        continue;
                    }
                };
            }

            // TODO: Get payload converter from client
            let pc = PayloadConverter::default();

            break match event_attrs {
                Some(Attributes::WorkflowExecutionCompletedEventAttributes(attrs)) => {
                    follow!(attrs);
                    let payload = attrs
                        .result
                        .and_then(|p| p.payloads.into_iter().next())
                        .unwrap_or_default();
                    let result: W::Output =
                        pc.from_payload(&SerializationContext::Workflow, payload)?;
                    Ok(WorkflowExecutionResult::Succeeded(result))
                }
                Some(Attributes::WorkflowExecutionFailedEventAttributes(attrs)) => {
                    follow!(attrs);
                    Ok(WorkflowExecutionResult::Failed(
                        attrs.failure.unwrap_or_default(),
                    ))
                }
                Some(Attributes::WorkflowExecutionCanceledEventAttributes(attrs)) => Ok(
                    WorkflowExecutionResult::Cancelled(Vec::from_payloads(attrs.details)),
                ),
                Some(Attributes::WorkflowExecutionTimedOutEventAttributes(attrs)) => {
                    follow!(attrs);
                    Ok(WorkflowExecutionResult::TimedOut)
                }
                Some(Attributes::WorkflowExecutionTerminatedEventAttributes(attrs)) => Ok(
                    WorkflowExecutionResult::Terminated(Vec::from_payloads(attrs.details)),
                ),
                Some(Attributes::WorkflowExecutionContinuedAsNewEventAttributes(attrs)) => {
                    if opts.follow_runs {
                        if !attrs.new_execution_run_id.is_empty() {
                            run_id = attrs.new_execution_run_id;
                            continue;
                        } else {
                            bail!("New execution run id was empty in continue as new event!");
                        }
                    } else {
                        Ok(WorkflowExecutionResult::ContinuedAsNew)
                    }
                }
                o => Err(anyhow!(
                    "Server returned an event that didn't match the CloseEvent filter. \
                     This is either a server bug or a new event the SDK does not understand. \
                     Event details: {o:?}"
                )),
            };
        }
    }
}
