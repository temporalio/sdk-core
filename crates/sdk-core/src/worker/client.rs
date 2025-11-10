//! Worker-specific client needs

pub(crate) mod mocks;
use crate::protosext::legacy_query_failure;
use parking_lot::Mutex;
use prost_types::Duration as PbDuration;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use temporalio_client::{
    Client, ClientWorkerSet, IsWorkerTaskLongPoll, Namespace, NamespacedClient, NoRetryOnMatching,
    RetryClient, RetryConfig, RetryConfigForCall, SharedReplaceableClient, WorkflowService,
};
use temporalio_common::{
    protos::{
        TaskToken,
        coresdk::{workflow_commands::QueryResult, workflow_completion},
        temporal::api::{
            command::v1::Command,
            common::v1::{
                MeteringMetadata, Payloads, WorkerVersionCapabilities, WorkerVersionStamp,
                WorkflowExecution,
            },
            deployment,
            enums::v1::{
                TaskQueueKind, VersioningBehavior, WorkerStatus, WorkerVersioningMode,
                WorkflowTaskFailedCause,
            },
            failure::v1::Failure,
            nexus,
            protocol::v1::Message as ProtocolMessage,
            query::v1::WorkflowQueryResult,
            sdk::v1::WorkflowTaskCompletedMetadata,
            taskqueue::v1::{StickyExecutionAttributes, TaskQueue, TaskQueueMetadata},
            worker::v1::{WorkerHeartbeat, WorkerSlotsInfo},
            workflowservice::v1::{get_system_info_response::Capabilities, *},
        },
    },
    worker::WorkerVersioningStrategy,
};
use tonic::IntoRequest;
use uuid::Uuid;

type Result<T, E = tonic::Status> = std::result::Result<T, E>;

pub enum LegacyQueryResult {
    Succeeded(QueryResult),
    Failed(workflow_completion::Failure),
}

/// Contains everything a worker needs to interact with the server
pub(crate) struct WorkerClientBag {
    client: RetryClient<SharedReplaceableClient<Client>>,
    namespace: String,
    identity: String,
    worker_versioning_strategy: WorkerVersioningStrategy,
    worker_heartbeat_map: Arc<Mutex<HashMap<String, ClientHeartbeatData>>>,
}

impl WorkerClientBag {
    pub(crate) fn new(
        client: RetryClient<SharedReplaceableClient<Client>>,
        namespace: String,
        identity: String,
        worker_versioning_strategy: WorkerVersioningStrategy,
    ) -> Self {
        Self {
            client,
            namespace,
            identity,
            worker_versioning_strategy,
            worker_heartbeat_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn default_capabilities(&self) -> Capabilities {
        self.capabilities().unwrap_or_default()
    }

    fn binary_checksum(&self) -> String {
        if self.default_capabilities().build_id_based_versioning {
            "".to_string()
        } else {
            self.worker_versioning_strategy.build_id().to_owned()
        }
    }

    fn deployment_options(&self) -> Option<deployment::v1::WorkerDeploymentOptions> {
        match &self.worker_versioning_strategy {
            WorkerVersioningStrategy::WorkerDeploymentBased(dopts) => {
                Some(deployment::v1::WorkerDeploymentOptions {
                    deployment_name: dopts.version.deployment_name.clone(),
                    build_id: dopts.version.build_id.clone(),
                    worker_versioning_mode: if dopts.use_worker_versioning {
                        WorkerVersioningMode::Versioned.into()
                    } else {
                        WorkerVersioningMode::Unversioned.into()
                    },
                })
            }
            _ => None,
        }
    }

    fn worker_version_capabilities(&self) -> Option<WorkerVersionCapabilities> {
        if self.default_capabilities().build_id_based_versioning {
            Some(WorkerVersionCapabilities {
                build_id: self.worker_versioning_strategy.build_id().to_owned(),
                use_versioning: self.worker_versioning_strategy.uses_build_id_based(),
                // This will never be used, as it is the v3 version that we never supported in
                // Core SDKs.
                deployment_series_name: "".to_string(),
            })
        } else {
            None
        }
    }

    fn worker_version_stamp(&self) -> Option<WorkerVersionStamp> {
        if self.default_capabilities().build_id_based_versioning {
            Some(WorkerVersionStamp {
                build_id: self.worker_versioning_strategy.build_id().to_owned(),
                use_versioning: self.worker_versioning_strategy.uses_build_id_based(),
            })
        } else {
            None
        }
    }
}

/// This trait contains everything workers need to interact with Temporal, and hence provides a
/// minimal mocking surface. Delegates to [WorkflowClientTrait] so see that for details.
#[cfg_attr(any(feature = "test-utilities", test), mockall::automock)]
#[async_trait::async_trait]
pub trait WorkerClient: Sync + Send {
    /// Poll workflow tasks
    async fn poll_workflow_task(
        &self,
        poll_options: PollOptions,
        wf_options: PollWorkflowOptions,
    ) -> Result<PollWorkflowTaskQueueResponse>;
    /// Poll activity tasks
    async fn poll_activity_task(
        &self,
        poll_options: PollOptions,
        act_options: PollActivityOptions,
    ) -> Result<PollActivityTaskQueueResponse>;
    /// Poll Nexus tasks
    async fn poll_nexus_task(
        &self,
        poll_options: PollOptions,
        send_heartbeat: bool,
    ) -> Result<PollNexusTaskQueueResponse>;
    /// Complete a workflow task
    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse>;
    /// Complete an activity task
    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse>;
    /// Complete a Nexus task
    async fn complete_nexus_task(
        &self,
        task_token: TaskToken,
        response: nexus::v1::Response,
    ) -> Result<RespondNexusTaskCompletedResponse>;
    /// Record an activity heartbeat
    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse>;
    /// Cancel an activity task
    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse>;
    /// Fail an activity task
    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse>;
    /// Fail a workflow task
    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse>;
    /// Fail a Nexus task
    async fn fail_nexus_task(
        &self,
        task_token: TaskToken,
        error: nexus::v1::HandlerError,
    ) -> Result<RespondNexusTaskFailedResponse>;
    /// Get the workflow execution history
    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse>;
    /// Respond to a legacy query
    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: LegacyQueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse>;
    /// Describe the namespace
    async fn describe_namespace(&self) -> Result<DescribeNamespaceResponse>;
    /// Shutdown the worker
    async fn shutdown_worker(
        &self,
        sticky_task_queue: String,
        final_heartbeat: Option<WorkerHeartbeat>,
    ) -> Result<ShutdownWorkerResponse>;
    /// Record a worker heartbeat
    async fn record_worker_heartbeat(
        &self,
        namespace: String,
        worker_heartbeat: Vec<WorkerHeartbeat>,
    ) -> Result<RecordWorkerHeartbeatResponse>;

    /// Replace the underlying client
    fn replace_client(&self, new_client: Client);
    /// Return server capabilities
    fn capabilities(&self) -> Option<Capabilities>;
    /// Return workers using this client
    fn workers(&self) -> Arc<ClientWorkerSet>;
    /// Indicates if this is a mock client
    fn is_mock(&self) -> bool;
    /// Return name and version of the SDK
    fn sdk_name_and_version(&self) -> (String, String);
    /// Get worker identity
    fn identity(&self) -> String;
    /// Get worker grouping key
    fn worker_grouping_key(&self) -> Uuid;
    /// Sets the client-reliant fields for WorkerHeartbeat. This also updates client-level tracking
    /// of heartbeat fields, like last heartbeat timestamp.
    fn set_heartbeat_client_fields(&self, heartbeat: &mut WorkerHeartbeat);
}

/// Configuration options shared by workflow, activity, and Nexus polling calls
#[derive(Debug, Clone)]
pub struct PollOptions {
    /// The name of the task queue to poll
    pub task_queue: String,
    /// Prevents retrying on specific gRPC statuses
    pub no_retry: Option<NoRetryOnMatching>,
    /// Overrides the default RPC timeout for the poll request
    pub timeout_override: Option<Duration>,
}
/// Additional options specific to workflow task polling
#[derive(Debug, Clone)]
pub struct PollWorkflowOptions {
    /// Optional sticky queue name for session‐based workflow polling
    pub sticky_queue_name: Option<String>,
}
/// Additional options specific to activity task polling
#[derive(Debug, Clone)]
pub struct PollActivityOptions {
    /// Optional rate limit (tasks per second) for activity polling
    pub max_tasks_per_sec: Option<f64>,
}

#[async_trait::async_trait]
impl WorkerClient for WorkerClientBag {
    async fn poll_workflow_task(
        &self,
        poll_options: PollOptions,
        wf_options: PollWorkflowOptions,
    ) -> Result<PollWorkflowTaskQueueResponse> {
        let task_queue = if let Some(sticky) = wf_options.sticky_queue_name {
            TaskQueue {
                name: sticky,
                kind: TaskQueueKind::Sticky.into(),
                normal_name: poll_options.task_queue,
            }
        } else {
            TaskQueue {
                name: poll_options.task_queue,
                kind: TaskQueueKind::Normal.into(),
                normal_name: "".to_string(),
            }
        };
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = PollWorkflowTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(task_queue),
            identity: self.identity.clone(),
            binary_checksum: self.binary_checksum(),
            worker_version_capabilities: self.worker_version_capabilities(),
            deployment_options: self.deployment_options(),
            worker_heartbeat: None,
        }
        .into_request();
        request.extensions_mut().insert(IsWorkerTaskLongPoll);
        if let Some(nr) = poll_options.no_retry {
            request.extensions_mut().insert(nr);
        }
        if let Some(to) = poll_options.timeout_override {
            request.set_timeout(to);
        }

        Ok(self
            .client
            .clone()
            .poll_workflow_task_queue(request)
            .await?
            .into_inner())
    }

    async fn poll_activity_task(
        &self,
        poll_options: PollOptions,
        act_options: PollActivityOptions,
    ) -> Result<PollActivityTaskQueueResponse> {
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = PollActivityTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: poll_options.task_queue,
                kind: TaskQueueKind::Normal as i32,
                normal_name: "".to_string(),
            }),
            identity: self.identity.clone(),
            task_queue_metadata: act_options.max_tasks_per_sec.map(|tps| TaskQueueMetadata {
                max_tasks_per_second: Some(tps),
            }),
            worker_version_capabilities: self.worker_version_capabilities(),
            deployment_options: self.deployment_options(),
            worker_heartbeat: None,
        }
        .into_request();
        request.extensions_mut().insert(IsWorkerTaskLongPoll);
        if let Some(nr) = poll_options.no_retry {
            request.extensions_mut().insert(nr);
        }
        if let Some(to) = poll_options.timeout_override {
            request.set_timeout(to);
        }

        Ok(self
            .client
            .clone()
            .poll_activity_task_queue(request)
            .await?
            .into_inner())
    }

    async fn poll_nexus_task(
        &self,
        poll_options: PollOptions,
        _send_heartbeat: bool,
    ) -> Result<PollNexusTaskQueueResponse> {
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = PollNexusTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: poll_options.task_queue,
                kind: TaskQueueKind::Normal as i32,
                normal_name: "".to_string(),
            }),
            identity: self.identity.clone(),
            worker_version_capabilities: self.worker_version_capabilities(),
            deployment_options: self.deployment_options(),
            worker_heartbeat: Vec::new(),
        }
        .into_request();
        request.extensions_mut().insert(IsWorkerTaskLongPoll);
        if let Some(nr) = poll_options.no_retry {
            request.extensions_mut().insert(nr);
        }
        if let Some(to) = poll_options.timeout_override {
            request.set_timeout(to);
        }

        Ok(self
            .client
            .clone()
            .poll_nexus_task_queue(request)
            .await?
            .into_inner())
    }

    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse> {
        #[allow(deprecated)] // want to list all fields explicitly
        let request = RespondWorkflowTaskCompletedRequest {
            task_token: request.task_token.into(),
            commands: request.commands,
            messages: request.messages,
            identity: self.identity.clone(),
            sticky_attributes: request.sticky_attributes,
            return_new_workflow_task: request.return_new_workflow_task,
            force_create_new_workflow_task: request.force_create_new_workflow_task,
            worker_version_stamp: self.worker_version_stamp(),
            binary_checksum: self.binary_checksum(),
            query_results: request
                .query_responses
                .into_iter()
                .map(|qr| {
                    let (id, completed_type, query_result, error_message) = qr.into_components();
                    (
                        id,
                        WorkflowQueryResult {
                            result_type: completed_type as i32,
                            answer: query_result,
                            error_message,
                            // TODO: https://github.com/temporalio/sdk-core/issues/867
                            failure: None,
                        },
                    )
                })
                .collect(),
            namespace: self.namespace.clone(),
            sdk_metadata: Some(request.sdk_metadata),
            metering_metadata: Some(request.metering_metadata),
            capabilities: Some(respond_workflow_task_completed_request::Capabilities {
                discard_speculative_workflow_task_with_events: true,
            }),
            // Will never be set, deprecated.
            deployment: None,
            versioning_behavior: request.versioning_behavior.into(),
            deployment_options: self.deployment_options(),
        };
        Ok(self
            .client
            .clone()
            .respond_workflow_task_completed(request.into_request())
            .await?
            .into_inner())
    }

    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        Ok(self
            .client
            .clone()
            .respond_activity_task_completed(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskCompletedRequest {
                    task_token: task_token.0,
                    result,
                    identity: self.identity.clone(),
                    namespace: self.namespace.clone(),
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn complete_nexus_task(
        &self,
        task_token: TaskToken,
        response: nexus::v1::Response,
    ) -> Result<RespondNexusTaskCompletedResponse> {
        Ok(self
            .client
            .clone()
            .respond_nexus_task_completed(
                RespondNexusTaskCompletedRequest {
                    namespace: self.namespace.clone(),
                    identity: self.identity.clone(),
                    task_token: task_token.0,
                    response: Some(response),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        Ok(self
            .client
            .clone()
            .record_activity_task_heartbeat(
                RecordActivityTaskHeartbeatRequest {
                    task_token: task_token.0,
                    details,
                    identity: self.identity.clone(),
                    namespace: self.namespace.clone(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        Ok(self
            .client
            .clone()
            .respond_activity_task_canceled(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskCanceledRequest {
                    task_token: task_token.0,
                    details,
                    identity: self.identity.clone(),
                    namespace: self.namespace.clone(),
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse> {
        Ok(self
            .client
            .clone()
            .respond_activity_task_failed(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskFailedRequest {
                    task_token: task_token.0,
                    failure,
                    identity: self.identity.clone(),
                    namespace: self.namespace.clone(),
                    // TODO: Implement - https://github.com/temporalio/sdk-core/issues/293
                    last_heartbeat_details: None,
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse> {
        #[allow(deprecated)] // want to list all fields explicitly
        let request = RespondWorkflowTaskFailedRequest {
            task_token: task_token.0,
            cause: cause as i32,
            failure,
            identity: self.identity.clone(),
            binary_checksum: self.binary_checksum(),
            namespace: self.namespace.clone(),
            messages: vec![],
            worker_version: self.worker_version_stamp(),
            // Will never be set, deprecated.
            deployment: None,
            deployment_options: self.deployment_options(),
        };
        Ok(self
            .client
            .clone()
            .respond_workflow_task_failed(request.into_request())
            .await?
            .into_inner())
    }

    async fn fail_nexus_task(
        &self,
        task_token: TaskToken,
        error: nexus::v1::HandlerError,
    ) -> Result<RespondNexusTaskFailedResponse> {
        Ok(self
            .client
            .clone()
            .respond_nexus_task_failed(
                RespondNexusTaskFailedRequest {
                    namespace: self.namespace.clone(),
                    identity: self.identity.clone(),
                    task_token: task_token.0,
                    error: Some(error),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse> {
        Ok(self
            .client
            .clone()
            .get_workflow_execution_history(
                GetWorkflowExecutionHistoryRequest {
                    namespace: self.namespace.clone(),
                    execution: Some(WorkflowExecution {
                        workflow_id,
                        run_id: run_id.unwrap_or_default(),
                    }),
                    next_page_token: page_token,
                    ..Default::default()
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: LegacyQueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse> {
        let mut failure = None;
        let (query_result, cause) = match query_result {
            LegacyQueryResult::Succeeded(s) => (s, WorkflowTaskFailedCause::Unspecified),
            #[allow(deprecated)]
            LegacyQueryResult::Failed(f) => {
                let cause = f.force_cause();
                failure = f.failure.clone();
                (legacy_query_failure(f), cause)
            }
        };
        let (_, completed_type, query_result, error_message) = query_result.into_components();

        Ok(self
            .client
            .clone()
            .respond_query_task_completed(
                RespondQueryTaskCompletedRequest {
                    task_token: task_token.into(),
                    completed_type: completed_type as i32,
                    query_result,
                    error_message,
                    namespace: self.namespace.clone(),
                    failure,
                    cause: cause.into(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn describe_namespace(&self) -> Result<DescribeNamespaceResponse> {
        Ok(self
            .client
            .clone()
            .describe_namespace(
                Namespace::Name(self.namespace.clone())
                    .into_describe_namespace_request()
                    .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn shutdown_worker(
        &self,
        sticky_task_queue: String,
        final_heartbeat: Option<WorkerHeartbeat>,
    ) -> Result<ShutdownWorkerResponse> {
        let mut final_heartbeat = final_heartbeat;
        if let Some(w) = final_heartbeat.as_mut() {
            w.status = WorkerStatus::Shutdown.into();
            self.set_heartbeat_client_fields(w);
        }
        let mut request = ShutdownWorkerRequest {
            namespace: self.namespace.clone(),
            identity: self.identity.clone(),
            sticky_task_queue,
            reason: "graceful shutdown".to_string(),
            worker_heartbeat: final_heartbeat,
        }
        .into_request();
        request
            .extensions_mut()
            .insert(RetryConfigForCall(RetryConfig::no_retries()));

        Ok(
            WorkflowService::shutdown_worker(&mut self.client.clone(), request)
                .await?
                .into_inner(),
        )
    }

    async fn record_worker_heartbeat(
        &self,
        namespace: String,
        worker_heartbeat: Vec<WorkerHeartbeat>,
    ) -> Result<RecordWorkerHeartbeatResponse> {
        let request = RecordWorkerHeartbeatRequest {
            namespace,
            identity: self.identity.clone(),
            worker_heartbeat,
        };
        Ok(self
            .client
            .clone()
            .record_worker_heartbeat(request.into_request())
            .await?
            .into_inner())
    }

    fn replace_client(&self, new_client: Client) {
        self.client.get_client().replace_client(new_client);
    }

    fn capabilities(&self) -> Option<Capabilities> {
        self.client
            .get_client()
            .inner_cow()
            .inner()
            .capabilities()
            .cloned()
    }

    fn workers(&self) -> Arc<ClientWorkerSet> {
        self.client.get_client().inner_cow().inner().workers()
    }

    fn is_mock(&self) -> bool {
        false
    }

    fn sdk_name_and_version(&self) -> (String, String) {
        let inner = self.client.get_client().inner_cow();
        let opts = inner.options();
        (opts.client_name.clone(), opts.client_version.clone())
    }

    fn identity(&self) -> String {
        self.identity.clone()
    }

    fn worker_grouping_key(&self) -> Uuid {
        self.client.get_client().inner_cow().worker_grouping_key()
    }

    fn set_heartbeat_client_fields(&self, heartbeat: &mut WorkerHeartbeat) {
        if let Some(host_info) = heartbeat.host_info.as_mut() {
            host_info.process_key = self.worker_grouping_key().to_string();
        }
        heartbeat.worker_identity = WorkerClient::identity(self);
        let sdk_name_and_ver = self.sdk_name_and_version();
        heartbeat.sdk_name = sdk_name_and_ver.0;
        heartbeat.sdk_version = sdk_name_and_ver.1;

        let now = SystemTime::now();
        heartbeat.heartbeat_time = Some(now.into());
        let mut heartbeat_map = self.worker_heartbeat_map.lock();
        let client_heartbeat_data = heartbeat_map
            .entry(heartbeat.worker_instance_key.clone())
            .or_default();
        let elapsed_since_last_heartbeat =
            client_heartbeat_data.last_heartbeat_time.map(|hb_time| {
                let dur = now.duration_since(hb_time).unwrap_or(Duration::ZERO);
                PbDuration {
                    seconds: dur.as_secs() as i64,
                    nanos: dur.subsec_nanos() as i32,
                }
            });
        heartbeat.elapsed_since_last_heartbeat = elapsed_since_last_heartbeat;
        client_heartbeat_data.last_heartbeat_time = Some(now);

        update_slots(
            &mut heartbeat.workflow_task_slots_info,
            &mut client_heartbeat_data.workflow_task_slots_info,
        );
        update_slots(
            &mut heartbeat.activity_task_slots_info,
            &mut client_heartbeat_data.activity_task_slots_info,
        );
        update_slots(
            &mut heartbeat.nexus_task_slots_info,
            &mut client_heartbeat_data.nexus_task_slots_info,
        );
        update_slots(
            &mut heartbeat.local_activity_slots_info,
            &mut client_heartbeat_data.local_activity_slots_info,
        );
    }
}

impl NamespacedClient for WorkerClientBag {
    fn namespace(&self) -> String {
        self.namespace.clone()
    }

    fn identity(&self) -> String {
        self.identity.clone()
    }
}

/// A version of [RespondWorkflowTaskCompletedRequest] that will finish being filled out by the
/// server client
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowTaskCompletion {
    /// The task token that would've been received from polling for a workflow activation
    pub task_token: TaskToken,
    /// A list of new commands to send to the server, such as starting a timer.
    pub commands: Vec<Command>,
    /// A list of protocol messages to send to the server.
    pub messages: Vec<ProtocolMessage>,
    /// If set, indicate that next task should be queued on sticky queue with given attributes.
    pub sticky_attributes: Option<StickyExecutionAttributes>,
    /// Responses to queries in the `queries` field of the workflow task.
    pub query_responses: Vec<QueryResult>,
    /// Indicate that the task completion should return a new WFT if one is available
    pub return_new_workflow_task: bool,
    /// Force a new WFT to be created after this completion
    pub force_create_new_workflow_task: bool,
    /// SDK-specific metadata to send
    pub sdk_metadata: WorkflowTaskCompletedMetadata,
    /// Metering info
    pub metering_metadata: MeteringMetadata,
    /// Versioning behavior of the workflow, if any.
    pub versioning_behavior: VersioningBehavior,
}

#[derive(Clone, Default)]
struct SlotsInfo {
    total_processed_tasks: i32,
    total_failed_tasks: i32,
}

#[derive(Clone, Default)]
struct ClientHeartbeatData {
    last_heartbeat_time: Option<SystemTime>,

    workflow_task_slots_info: SlotsInfo,
    activity_task_slots_info: SlotsInfo,
    nexus_task_slots_info: SlotsInfo,
    local_activity_slots_info: SlotsInfo,
}

fn update_slots(slots_info: &mut Option<WorkerSlotsInfo>, client_heartbeat_data: &mut SlotsInfo) {
    if let Some(wft_slot_info) = slots_info.as_mut() {
        wft_slot_info.last_interval_processed_tasks =
            wft_slot_info.total_processed_tasks - client_heartbeat_data.total_processed_tasks;
        wft_slot_info.last_interval_failure_tasks =
            wft_slot_info.total_failed_tasks - client_heartbeat_data.total_failed_tasks;

        client_heartbeat_data.total_processed_tasks = wft_slot_info.total_processed_tasks;
        client_heartbeat_data.total_failed_tasks = wft_slot_info.total_failed_tasks;
    }
}
