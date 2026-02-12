use crate::{
    NamespacedClient, WorkflowCancelOptions, WorkflowDescribeOptions, WorkflowExecuteUpdateOptions,
    WorkflowFetchHistoryOptions, WorkflowGetResultOptions, WorkflowQueryOptions,
    WorkflowSignalOptions, WorkflowStartUpdateOptions, WorkflowTerminateOptions,
    WorkflowUpdateWaitStage,
    grpc::WorkflowService,
    errors::{
        WorkflowGetResultError, WorkflowInteractionError, WorkflowQueryError, WorkflowUpdateError,
    },
};
use std::{fmt::Debug, marker::PhantomData};
use temporalio_common::{
    QueryDefinition, SignalDefinition, UpdateDefinition, WorkflowDefinition,
    data_converters::{RawValue, SerializationContextData},
    protos::{
        coresdk::FromPayloadsExt,
        temporal::api::{
            common::v1::{Payload, Payloads, WorkflowExecution as ProtoWorkflowExecution},
            enums::v1::{HistoryEventFilterType, UpdateWorkflowExecutionLifecycleStage},
            failure::v1::Failure,
            history::{
                self,
                v1::{HistoryEvent, history_event::Attributes},
            },
            query::v1::WorkflowQuery,
            update::{self, v1::WaitPolicy},
            workflowservice::v1::{
                DescribeWorkflowExecutionRequest, DescribeWorkflowExecutionResponse,
                GetWorkflowExecutionHistoryRequest, PollWorkflowExecutionUpdateRequest,
                QueryWorkflowRequest, RequestCancelWorkflowExecutionRequest,
                SignalWorkflowExecutionRequest, TerminateWorkflowExecutionRequest,
                UpdateWorkflowExecutionRequest,
            },
        },
    },
};
use tonic::IntoRequest;
use uuid::Uuid;

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
    Cancelled {
        /// Details provided at cancellation time
        details: Vec<Payload>,
    },
    /// The workflow was terminated
    Terminated {
        /// Details provided at termination time
        details: Vec<Payload>,
    },
    /// The workflow timed out
    TimedOut,
    /// The workflow continued as new
    ContinuedAsNew,
}

/// Description of a workflow execution returned by `WorkflowHandle::describe`.
#[derive(Debug, Clone)]
pub struct WorkflowExecutionDescription {
    /// The raw proto response from the server.
    pub raw_description: DescribeWorkflowExecutionResponse,
}

impl WorkflowExecutionDescription {
    fn new(raw_description: DescribeWorkflowExecutionResponse) -> Self {
        Self { raw_description }
    }
}

// TODO [rust-sdk-branch]: Could implment stream a-la ListWorkflowsStream
/// Workflow execution history returned by `WorkflowHandle::fetch_history`.
#[derive(Debug, Clone)]
pub struct WorkflowHistory {
    events: Vec<HistoryEvent>,
}
impl From<WorkflowHistory> for history::v1::History {
    fn from(h: WorkflowHistory) -> Self {
        Self { events: h.events }
    }
}

impl WorkflowHistory {
    fn new(events: Vec<HistoryEvent>) -> Self {
        Self { events }
    }

    /// The history events.
    pub fn events(&self) -> &[HistoryEvent] {
        &self.events
    }

    /// Consume the history and return the events.
    pub fn into_events(self) -> Vec<HistoryEvent> {
        self.events
    }
}

/// A workflow handle which can refer to a specific workflow run, or a chain of workflow runs with
/// the same workflow id.
pub struct WorkflowHandle<ClientT, W> {
    client: ClientT,
    info: WorkflowExecutionInfo,

    _wf_type: PhantomData<W>,
}

impl<CT, W> WorkflowHandle<CT, W> {
    /// Return the run id of the Workflow Execution pointed at by this handle, if there is one.
    pub fn run_id(&self) -> Option<&str> {
        self.info.run_id.as_deref()
    }
}

/// Holds needed information to refer to a specific workflow run, or workflow execution chain
#[derive(Debug, Clone)]
pub struct WorkflowExecutionInfo {
    /// Namespace the workflow lives in.
    pub namespace: String,
    /// The workflow's id.
    pub workflow_id: String,
    /// If set, target this specific run of the workflow.
    pub run_id: Option<String>,
    /// Run ID used for cancellation and termination to ensure they happen on a workflow starting
    /// with this run ID. This can be set when getting a workflow handle. When starting a workflow,
    /// this is set as the resulting run ID if no start signal was provided.
    pub first_execution_run_id: Option<String>,
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

/// A workflow handle to a workflow with unknown types. Uses single argument raw payloads for input
/// and output.
pub type UntypedWorkflowHandle<CT> = WorkflowHandle<CT, UntypedWorkflow>;

/// Marker type for untyped workflow handles. Stores the workflow type name.
pub struct UntypedWorkflow {
    name: String,
}
impl UntypedWorkflow {
    /// Create a new `UntypedWorkflow` with the given workflow type name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}
impl WorkflowDefinition for UntypedWorkflow {
    type Input = RawValue;
    type Output = RawValue;
    fn name(&self) -> &str {
        &self.name
    }
}

/// Marker type for sending untyped signals. Stores the signal name for runtime lookup.
///
/// Use with `handle.signal(UntypedSignal::new("signal_name"), raw_payload)`.
pub struct UntypedSignal<W> {
    name: String,
    _wf: PhantomData<W>,
}

impl<W> UntypedSignal<W> {
    /// Create a new `UntypedSignal` with the given signal name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _wf: PhantomData,
        }
    }
}

impl<W: WorkflowDefinition> SignalDefinition for UntypedSignal<W> {
    type Workflow = W;
    type Input = RawValue;

    fn name(&self) -> &str {
        &self.name
    }
}

/// Marker type for sending untyped queries. Stores the query name for runtime lookup.
///
/// Use with `handle.query(UntypedQuery::new("query_name"), raw_payload)`.
pub struct UntypedQuery<W> {
    name: String,
    _wf: PhantomData<W>,
}

impl<W> UntypedQuery<W> {
    /// Create a new `UntypedQuery` with the given query name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _wf: PhantomData,
        }
    }
}

impl<W: WorkflowDefinition> QueryDefinition for UntypedQuery<W> {
    type Workflow = W;
    type Input = RawValue;
    type Output = RawValue;

    fn name(&self) -> &str {
        &self.name
    }
}

/// Marker type for sending untyped updates. Stores the update name for runtime lookup.
///
/// Use with `handle.update(UntypedUpdate::new("update_name"), raw_payload)`.
pub struct UntypedUpdate<W> {
    name: String,
    _wf: PhantomData<W>,
}

impl<W> UntypedUpdate<W> {
    /// Create a new `UntypedUpdate` with the given update name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _wf: PhantomData,
        }
    }
}

impl<W: WorkflowDefinition> UpdateDefinition for UntypedUpdate<W> {
    type Workflow = W;
    type Input = RawValue;
    type Output = RawValue;

    fn name(&self) -> &str {
        &self.name
    }
}

impl<CT, W> WorkflowHandle<CT, W>
where
    CT: WorkflowService + Clone,
    W: WorkflowDefinition,
{
    /// Create a workflow handle from a client and identifying information.
    pub fn new(client: CT, info: WorkflowExecutionInfo) -> Self {
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

    /// Await the result of the workflow execution, returning the output on success or an error
    /// describing the non-success outcome (failed, cancelled, terminated, etc.).
    pub async fn get_result(
        &self,
        opts: WorkflowGetResultOptions,
    ) -> Result<W::Output, WorkflowGetResultError>
    where
        CT: WorkflowService + NamespacedClient + Clone,
    {
        let raw = self.get_result_raw(opts).await?;
        match raw {
            WorkflowExecutionResult::Succeeded(v) => Ok(v),
            WorkflowExecutionResult::Failed(f) => Err(WorkflowGetResultError::Failed(f)),
            WorkflowExecutionResult::Cancelled { details } => {
                Err(WorkflowGetResultError::Cancelled { details })
            }
            WorkflowExecutionResult::Terminated { details } => {
                Err(WorkflowGetResultError::Terminated { details })
            }
            WorkflowExecutionResult::TimedOut => Err(WorkflowGetResultError::TimedOut),
            WorkflowExecutionResult::ContinuedAsNew => Err(WorkflowGetResultError::ContinuedAsNew),
        }
    }

    /// Await the result of the workflow execution, returning the full
    /// [`WorkflowExecutionResult`] enum for callers that need to inspect non-success outcomes
    /// directly.
    pub async fn get_result_raw(
        &self,
        opts: WorkflowGetResultOptions,
    ) -> Result<WorkflowExecutionResult<W::Output>, WorkflowInteractionError>
    where
        CT: WorkflowService + NamespacedClient + Clone,
    {
        let mut run_id = self.info.run_id.clone().unwrap_or_default();
        let fetch_opts = WorkflowFetchHistoryOptions::builder()
            .skip_archival(true)
            .wait_new_event(true)
            .event_filter_type(HistoryEventFilterType::CloseEvent)
            .build();

        loop {
            let history = self.fetch_history_for_run(&run_id, &fetch_opts).await?;
            let mut events = history.into_events();

            if events.is_empty() {
                continue;
            }

            let event_attrs = events.pop().and_then(|ev| ev.attributes);

            macro_rules! follow {
                ($attrs:ident) => {
                    if opts.follow_runs && $attrs.new_execution_run_id != "" {
                        run_id = $attrs.new_execution_run_id;
                        continue;
                    }
                };
            }

            let dc = self.client.data_converter();

            break match event_attrs {
                Some(Attributes::WorkflowExecutionCompletedEventAttributes(attrs)) => {
                    follow!(attrs);
                    let payload = attrs
                        .result
                        .and_then(|p| p.payloads.into_iter().next())
                        .unwrap_or_default();
                    let result: W::Output = dc
                        .from_payload(&SerializationContextData::Workflow, payload)
                        .await?;
                    Ok(WorkflowExecutionResult::Succeeded(result))
                }
                Some(Attributes::WorkflowExecutionFailedEventAttributes(attrs)) => {
                    follow!(attrs);
                    Ok(WorkflowExecutionResult::Failed(
                        attrs.failure.unwrap_or_default(),
                    ))
                }
                Some(Attributes::WorkflowExecutionCanceledEventAttributes(attrs)) => {
                    Ok(WorkflowExecutionResult::Cancelled {
                        details: Vec::from_payloads(attrs.details),
                    })
                }
                Some(Attributes::WorkflowExecutionTimedOutEventAttributes(attrs)) => {
                    follow!(attrs);
                    Ok(WorkflowExecutionResult::TimedOut)
                }
                Some(Attributes::WorkflowExecutionTerminatedEventAttributes(attrs)) => {
                    Ok(WorkflowExecutionResult::Terminated {
                        details: Vec::from_payloads(attrs.details),
                    })
                }
                Some(Attributes::WorkflowExecutionContinuedAsNewEventAttributes(attrs)) => {
                    if opts.follow_runs {
                        if !attrs.new_execution_run_id.is_empty() {
                            run_id = attrs.new_execution_run_id;
                            continue;
                        } else {
                            return Err(WorkflowInteractionError::Other(
                                "New execution run id was empty in continue as new event!".into(),
                            ));
                        }
                    } else {
                        Ok(WorkflowExecutionResult::ContinuedAsNew)
                    }
                }
                o => Err(WorkflowInteractionError::Other(
                    format!(
                        "Server returned an event that didn't match the CloseEvent filter. \
                         This is either a server bug or a new event the SDK does not understand. \
                         Event details: {o:?}"
                    )
                    .into(),
                )),
            };
        }
    }

    /// Send a signal to the workflow
    pub async fn signal<S>(
        &self,
        signal: S,
        input: S::Input,
        opts: WorkflowSignalOptions,
    ) -> Result<(), WorkflowInteractionError>
    where
        CT: WorkflowService + NamespacedClient + Clone,
        S: SignalDefinition<Workflow = W>,
        S::Input: Send,
    {
        let payloads = self
            .client
            .data_converter()
            .to_payloads(&SerializationContextData::Workflow, &input)
            .await?;
        WorkflowService::signal_workflow_execution(
            &mut self.client.clone(),
            SignalWorkflowExecutionRequest {
                namespace: self.client.namespace(),
                workflow_execution: Some(ProtoWorkflowExecution {
                    workflow_id: self.info.workflow_id.clone(),
                    run_id: self.info.run_id.clone().unwrap_or_default(),
                }),
                signal_name: signal.name().to_string(),
                input: Some(Payloads { payloads }),
                identity: self.client.identity(),
                request_id: opts
                    .request_id
                    .unwrap_or_else(|| Uuid::new_v4().to_string()),
                header: opts.header,
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .map_err(WorkflowInteractionError::from_status)?;
        Ok(())
    }

    /// Query the workflow
    pub async fn query<Q>(
        &self,
        query: Q,
        input: Q::Input,
        opts: WorkflowQueryOptions,
    ) -> Result<Q::Output, WorkflowQueryError>
    where
        CT: WorkflowService + NamespacedClient + Clone,
        Q: QueryDefinition<Workflow = W>,
        Q::Input: Send,
    {
        let dc = self.client.data_converter();
        let payloads = dc
            .to_payloads(&SerializationContextData::Workflow, &input)
            .await?;
        let response = self
            .client
            .clone()
            .query_workflow(
                QueryWorkflowRequest {
                    namespace: self.client.namespace(),
                    execution: Some(ProtoWorkflowExecution {
                        workflow_id: self.info.workflow_id.clone(),
                        run_id: self.info.run_id.clone().unwrap_or_default(),
                    }),
                    query: Some(WorkflowQuery {
                        query_type: query.name().to_string(),
                        query_args: Some(Payloads { payloads }),
                        header: opts.header,
                    }),
                    // Default to None (1) which means don't reject
                    query_reject_condition: opts.reject_condition.map(|c| c as i32).unwrap_or(1),
                }
                .into_request(),
            )
            .await
            .map_err(WorkflowQueryError::from_status)?
            .into_inner();

        if let Some(rejected) = response.query_rejected {
            return Err(WorkflowQueryError::Rejected(rejected));
        }

        let result_payloads = response
            .query_result
            .map(|p| p.payloads)
            .unwrap_or_default();

        dc.from_payloads(&SerializationContextData::Workflow, result_payloads)
            .await
            .map_err(WorkflowQueryError::from)
    }

    /// Send an update to the workflow and wait for it to complete, returning the result.
    pub async fn execute_update<U>(
        &self,
        update: U,
        input: U::Input,
        options: WorkflowExecuteUpdateOptions,
    ) -> Result<U::Output, WorkflowUpdateError>
    where
        CT: WorkflowService + NamespacedClient + Clone,
        U: UpdateDefinition<Workflow = W>,
        U::Input: Send,
        U::Output: 'static,
    {
        let handle = self
            .start_update(
                update,
                input,
                WorkflowStartUpdateOptions::builder()
                    .maybe_update_id(options.update_id)
                    .maybe_header(options.header)
                    .wait_for_stage(WorkflowUpdateWaitStage::Completed)
                    .build(),
            )
            .await?;
        handle.get_result().await
    }

    /// Start an update and return a handle without waiting for completion.
    /// Use `execute_update()` if you want to wait for the result immediately.
    pub async fn start_update<U>(
        &self,
        update: U,
        input: U::Input,
        options: WorkflowStartUpdateOptions,
    ) -> Result<WorkflowUpdateHandle<CT, U::Output>, WorkflowUpdateError>
    where
        CT: WorkflowService + NamespacedClient + Clone,
        U: UpdateDefinition<Workflow = W>,
        U::Input: Send,
    {
        let dc = self.client.data_converter();
        let payloads = dc
            .to_payloads(&SerializationContextData::Workflow, &input)
            .await?;

        let lifecycle_stage = match options.wait_for_stage {
            WorkflowUpdateWaitStage::Admitted => UpdateWorkflowExecutionLifecycleStage::Admitted,
            WorkflowUpdateWaitStage::Accepted => UpdateWorkflowExecutionLifecycleStage::Accepted,
            WorkflowUpdateWaitStage::Completed => UpdateWorkflowExecutionLifecycleStage::Completed,
        };

        let update_id = options
            .update_id
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let response = WorkflowService::update_workflow_execution(
            &mut self.client.clone(),
            UpdateWorkflowExecutionRequest {
                namespace: self.client.namespace(),
                workflow_execution: Some(ProtoWorkflowExecution {
                    workflow_id: self.info().workflow_id.clone(),
                    run_id: self.info().run_id.clone().unwrap_or_default(),
                }),
                wait_policy: Some(WaitPolicy {
                    lifecycle_stage: lifecycle_stage.into(),
                }),
                request: Some(update::v1::Request {
                    meta: Some(update::v1::Meta {
                        update_id: update_id.clone(),
                        identity: self.client.identity(),
                    }),
                    input: Some(update::v1::Input {
                        header: options.header,
                        name: update.name().to_string(),
                        args: Some(Payloads { payloads }),
                    }),
                }),
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .map_err(WorkflowUpdateError::from_status)?
        .into_inner();

        // Extract run_id from response if available
        let run_id = response
            .update_ref
            .as_ref()
            .and_then(|r| r.workflow_execution.as_ref())
            .map(|e| e.run_id.clone())
            .filter(|s| !s.is_empty())
            .or_else(|| self.info().run_id.clone());

        Ok(WorkflowUpdateHandle {
            client: self.client.clone(),
            update_id,
            workflow_id: self.info().workflow_id.clone(),
            run_id,
            known_outcome: response.outcome,
            _output: PhantomData,
        })
    }

    /// Request cancellation of this workflow.
    pub async fn cancel(&self, opts: WorkflowCancelOptions) -> Result<(), WorkflowInteractionError>
    where
        CT: NamespacedClient,
    {
        WorkflowService::request_cancel_workflow_execution(
            &mut self.client.clone(),
            RequestCancelWorkflowExecutionRequest {
                namespace: self.client.namespace(),
                workflow_execution: Some(ProtoWorkflowExecution {
                    workflow_id: self.info.workflow_id.clone(),
                    run_id: self.info.run_id.clone().unwrap_or_default(),
                }),
                identity: self.client.identity(),
                request_id: opts
                    .request_id
                    .unwrap_or_else(|| Uuid::new_v4().to_string()),
                first_execution_run_id: self
                    .info
                    .first_execution_run_id
                    .clone()
                    .unwrap_or_default(),
                reason: opts.reason,
                links: vec![],
            }
            .into_request(),
        )
        .await
        .map_err(WorkflowInteractionError::from_status)?;
        Ok(())
    }

    /// Terminate this workflow.
    pub async fn terminate(
        &self,
        opts: WorkflowTerminateOptions,
    ) -> Result<(), WorkflowInteractionError>
    where
        CT: NamespacedClient,
    {
        WorkflowService::terminate_workflow_execution(
            &mut self.client.clone(),
            TerminateWorkflowExecutionRequest {
                namespace: self.client.namespace(),
                workflow_execution: Some(ProtoWorkflowExecution {
                    workflow_id: self.info.workflow_id.clone(),
                    run_id: self.info.run_id.clone().unwrap_or_default(),
                }),
                reason: opts.reason,
                details: opts.details,
                identity: self.client.identity(),
                first_execution_run_id: self
                    .info
                    .first_execution_run_id
                    .clone()
                    .unwrap_or_default(),
                links: vec![],
            }
            .into_request(),
        )
        .await
        .map_err(WorkflowInteractionError::from_status)?;
        Ok(())
    }

    /// Get workflow execution description/metadata.
    pub async fn describe(
        &self,
        _opts: WorkflowDescribeOptions,
    ) -> Result<WorkflowExecutionDescription, WorkflowInteractionError>
    where
        CT: NamespacedClient,
    {
        let response = WorkflowService::describe_workflow_execution(
            &mut self.client.clone(),
            DescribeWorkflowExecutionRequest {
                namespace: self.client.namespace(),
                execution: Some(ProtoWorkflowExecution {
                    workflow_id: self.info.workflow_id.clone(),
                    run_id: self.info.run_id.clone().unwrap_or_default(),
                }),
            }
            .into_request(),
        )
        .await
        .map_err(WorkflowInteractionError::from_status)?
        .into_inner();
        Ok(WorkflowExecutionDescription::new(response))
    }
    /// Fetch workflow execution history.
    pub async fn fetch_history(
        &self,
        opts: WorkflowFetchHistoryOptions,
    ) -> Result<WorkflowHistory, WorkflowInteractionError>
    where
        CT: NamespacedClient,
    {
        let run_id = self.info.run_id.clone().unwrap_or_default();
        self.fetch_history_for_run(&run_id, &opts).await
    }

    /// Fetch history for a specific run_id, handling pagination.
    async fn fetch_history_for_run(
        &self,
        run_id: &str,
        opts: &WorkflowFetchHistoryOptions,
    ) -> Result<WorkflowHistory, WorkflowInteractionError>
    where
        CT: NamespacedClient,
    {
        let mut all_events = Vec::new();
        let mut next_page_token = vec![];

        loop {
            let response = WorkflowService::get_workflow_execution_history(
                &mut self.client.clone(),
                GetWorkflowExecutionHistoryRequest {
                    namespace: self.client.namespace(),
                    execution: Some(ProtoWorkflowExecution {
                        workflow_id: self.info.workflow_id.clone(),
                        run_id: run_id.to_string(),
                    }),
                    next_page_token: next_page_token.clone(),
                    skip_archival: opts.skip_archival,
                    wait_new_event: opts.wait_new_event,
                    history_event_filter_type: opts.event_filter_type as i32,
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .map_err(WorkflowInteractionError::from_status)?
            .into_inner();

            if let Some(history) = response.history {
                all_events.extend(history.events);
            }

            if response.next_page_token.is_empty() {
                break;
            }
            next_page_token = response.next_page_token;
        }

        Ok(WorkflowHistory::new(all_events))
    }
}

/// Handle to a workflow update that has been started but may not be complete.
///
/// Use `get_result()` to wait for the update to complete and retrieve its result.
pub struct WorkflowUpdateHandle<CT, T> {
    client: CT,
    update_id: String,
    workflow_id: String,
    run_id: Option<String>,
    /// If the update was started with `Completed` wait stage, the outcome is already available.
    known_outcome: Option<update::v1::Outcome>,
    _output: PhantomData<T>,
}

impl<CT, T> WorkflowUpdateHandle<CT, T> {
    /// Get the update ID.
    pub fn id(&self) -> &str {
        &self.update_id
    }

    /// Get the workflow ID.
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// Get the workflow run ID, if available.
    pub fn workflow_run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }
}

impl<CT, T: 'static> WorkflowUpdateHandle<CT, T>
where
    CT: WorkflowService + NamespacedClient + Clone,
{
    /// Wait for the update to complete and return the result.
    pub async fn get_result(&self) -> Result<T, WorkflowUpdateError>
    where
        T: temporalio_common::data_converters::TemporalDeserializable,
    {
        let outcome = if let Some(known) = &self.known_outcome {
            known.clone()
        } else {
            let response = WorkflowService::poll_workflow_execution_update(
                &mut self.client.clone(),
                PollWorkflowExecutionUpdateRequest {
                    namespace: self.client.namespace(),
                    update_ref: Some(update::v1::UpdateRef {
                        workflow_execution: Some(ProtoWorkflowExecution {
                            workflow_id: self.workflow_id.clone(),
                            run_id: self.run_id.clone().unwrap_or_default(),
                        }),
                        update_id: self.update_id.clone(),
                    }),
                    identity: self.client.identity(),
                    wait_policy: Some(WaitPolicy {
                        lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed.into(),
                    }),
                }
                .into_request(),
            )
            .await
            .map_err(WorkflowUpdateError::from_status)?
            .into_inner();

            response.outcome.ok_or_else(|| {
                WorkflowUpdateError::Other("Update poll returned no outcome".into())
            })?
        };

        match outcome.value {
            Some(update::v1::outcome::Value::Success(success)) => self
                .client
                .data_converter()
                .from_payloads(&SerializationContextData::Workflow, success.payloads)
                .await
                .map_err(WorkflowUpdateError::from),
            Some(update::v1::outcome::Value::Failure(failure)) => {
                Err(WorkflowUpdateError::Failed(Box::new(failure)))
            }
            None => Err(WorkflowUpdateError::Other(
                "Update returned no outcome value".into(),
            )),
        }
    }
}
