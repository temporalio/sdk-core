//! Worker-specific client needs

pub(crate) mod mocks;
use parking_lot::{Mutex, RwLock};
use std::{sync::Arc, time::Duration};
use std::time::SystemTime;
use opentelemetry_sdk::metrics::data::{Aggregation, Gauge, Sum};
use prost_types::Timestamp;
use prost_types::Duration as PbDuration;
use temporal_client::{
    Client, IsWorkerTaskLongPoll, Namespace, NamespacedClient, NoRetryOnMatching, RetryClient,
    SlotManager, WorkflowService,
};
use temporal_sdk_core_api::worker::{SlotKind, WorkerConfig, WorkerVersioningStrategy};
use temporal_sdk_core_protos::{
    TaskToken,
    coresdk::workflow_commands::QueryResult,
    temporal::api::{
        command::v1::Command,
        common::v1::{
            MeteringMetadata, Payloads, WorkerVersionCapabilities, WorkerVersionStamp,
            WorkflowExecution,
        },
        deployment,
        enums::v1::{
            TaskQueueKind, VersioningBehavior, WorkerVersioningMode, WorkflowTaskFailedCause,
        },
        failure::v1::Failure,
        nexus,
        protocol::v1::Message as ProtocolMessage,
        query::v1::WorkflowQueryResult,
        sdk::v1::WorkflowTaskCompletedMetadata,
        taskqueue::v1::{StickyExecutionAttributes, TaskQueue, TaskQueueMetadata},
        workflowservice::v1::{get_system_info_response::Capabilities, *},
    },
};
use tonic::IntoRequest;
use uuid::Uuid;
use temporal_sdk_core_protos::temporal::api::worker::v1::WorkerHeartbeat;
use crate::SlotSupplierOptions;
use crate::telemetry::InMemoryMeter;
use crate::telemetry::metrics::STICKY_CACHE_SIZE_NAME;

type Result<T, E = tonic::Status> = std::result::Result<T, E>;

/// Contains everything a worker needs to interact with the server
pub(crate) struct WorkerClientBag {
    replaceable_client: RwLock<RetryClient<Client>>,
    namespace: String,
    identity: String,
    worker_versioning_strategy: WorkerVersioningStrategy,
    heartbeat_info: Arc<Mutex<WorkerHeartbeatInfo>>,
}

impl WorkerClientBag {
    pub(crate) fn new(
        client: RetryClient<Client>,
        namespace: String,
        identity: String,
        worker_versioning_strategy: WorkerVersioningStrategy,
        heartbeat_info: Arc<Mutex<WorkerHeartbeatInfo>>,
    ) -> Self {
        Self {
            replaceable_client: RwLock::new(client),
            namespace,
            identity,
            worker_versioning_strategy,
            heartbeat_info,
        }
    }

    fn cloned_client(&self) -> RetryClient<Client> {
        self.replaceable_client.read().clone()
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
#[cfg_attr(test, mockall::automock)]
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
        query_result: QueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse>;
    /// Describe the namespace
    async fn describe_namespace(&self) -> Result<DescribeNamespaceResponse>;
    /// Shutdown the worker
    async fn shutdown_worker(&self, sticky_task_queue: String) -> Result<ShutdownWorkerResponse>;
    // TODO: params and return type?
    //  Probably whatever metrics itself can't update?
    /// Record a worker heartbeat
    async fn record_worker_heartbeat(&self) -> Result<()>;

    /// Replace the underlying client
    fn replace_client(&self, new_client: RetryClient<Client>);
    /// Return server capabilities
    fn capabilities(&self) -> Option<Capabilities>;
    /// Return workers using this client
    fn workers(&self) -> Arc<SlotManager>;
    /// Indicates if this is a mock client
    fn is_mock(&self) -> bool;
    /// Return name and version of the SDK
    fn sdk_name_and_version(&self) -> (String, String);
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
            worker_heartbeat: None, // TODO
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
            .cloned_client()
            .poll_workflow_task_queue(request)
            .await?
            .into_inner())
    }

    async fn poll_activity_task(
        &self,
        poll_options: PollOptions,
        act_options: PollActivityOptions,
    ) -> Result<PollActivityTaskQueueResponse> {
        let mut heartbeat_info = self.heartbeat_info.lock();
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
            worker_heartbeat: Some(heartbeat_info.capture_heartbeat()), // TODO:
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
            .cloned_client()
            .poll_activity_task_queue(request)
            .await?
            .into_inner())
    }

    async fn poll_nexus_task(
        &self,
        poll_options: PollOptions,
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
            worker_heartbeat: None, // TODO:
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
            .cloned_client()
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
            .cloned_client()
            .respond_workflow_task_completed(request)
            .await?
            .into_inner())
    }

    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        Ok(self
            .cloned_client()
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
                },
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
            .cloned_client()
            .respond_nexus_task_completed(RespondNexusTaskCompletedRequest {
                namespace: self.namespace.clone(),
                identity: self.identity.clone(),
                task_token: task_token.0,
                response: Some(response),
            })
            .await?
            .into_inner())
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        Ok(self
            .cloned_client()
            .record_activity_task_heartbeat(RecordActivityTaskHeartbeatRequest {
                task_token: task_token.0,
                details,
                identity: self.identity.clone(),
                namespace: self.namespace.clone(),
            })
            .await?
            .into_inner())
    }

    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        Ok(self
            .cloned_client()
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
                },
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
            .cloned_client()
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
                },
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
            .cloned_client()
            .respond_workflow_task_failed(request)
            .await?
            .into_inner())
    }

    async fn fail_nexus_task(
        &self,
        task_token: TaskToken,
        error: nexus::v1::HandlerError,
    ) -> Result<RespondNexusTaskFailedResponse> {
        Ok(self
            .cloned_client()
            .respond_nexus_task_failed(RespondNexusTaskFailedRequest {
                namespace: self.namespace.clone(),
                identity: self.identity.clone(),
                task_token: task_token.0,
                error: Some(error),
            })
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
            .cloned_client()
            .get_workflow_execution_history(GetWorkflowExecutionHistoryRequest {
                namespace: self.namespace.clone(),
                execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id: run_id.unwrap_or_default(),
                }),
                next_page_token: page_token,
                ..Default::default()
            })
            .await?
            .into_inner())
    }

    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: QueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse> {
        let (_, completed_type, query_result, error_message) = query_result.into_components();
        Ok(self
            .cloned_client()
            .respond_query_task_completed(RespondQueryTaskCompletedRequest {
                task_token: task_token.into(),
                completed_type: completed_type as i32,
                query_result,
                error_message,
                namespace: self.namespace.clone(),
                // TODO: https://github.com/temporalio/sdk-core/issues/867
                failure: None,
            })
            .await?
            .into_inner())
    }

    async fn describe_namespace(&self) -> Result<DescribeNamespaceResponse> {
        Ok(self
            .cloned_client()
            .describe_namespace(
                Namespace::Name(self.namespace.clone()).into_describe_namespace_request(),
            )
            .await?
            .into_inner())
    }

    async fn shutdown_worker(&self, sticky_task_queue: String) -> Result<ShutdownWorkerResponse> {
        let request = ShutdownWorkerRequest {
            namespace: self.namespace.clone(),
            identity: self.identity.clone(),
            sticky_task_queue,
            reason: "graceful shutdown".to_string(),
            worker_heartbeat: None, // TODO:
        };

        Ok(
            WorkflowService::shutdown_worker(&mut self.cloned_client(), request)
                .await?
                .into_inner(),
        )
    }
    
    async fn record_worker_heartbeat(&self) -> Result<()> {
        let worker_heartbeat = self.heartbeat_info.lock().capture_heartbeat();
        self
        .cloned_client()
        .record_worker_heartbeat(RecordWorkerHeartbeatRequest {
            namespace: self.namespace.clone(),
            identity: self.identity.clone(),
            worker_heartbeat: Some(worker_heartbeat),
        })
        .await?
        .map(|_| ()) // TODO: Do we want response?
        .into_inner();
        Ok(())
    }

    fn replace_client(&self, new_client: RetryClient<Client>) {
        let mut replaceable_client = self.replaceable_client.write();
        *replaceable_client = new_client;
    }

    fn capabilities(&self) -> Option<Capabilities> {
        let client = self.replaceable_client.read();
        client.get_client().inner().capabilities().cloned()
    }

    fn workers(&self) -> Arc<SlotManager> {
        let client = self.replaceable_client.read();
        client.get_client().inner().workers()
    }

    fn is_mock(&self) -> bool {
        false
    }

    fn sdk_name_and_version(&self) -> (String, String) {
        let lock = self.replaceable_client.read();
        let opts = lock.get_client().inner().options();
        (opts.client_name.clone(), opts.client_version.clone())
    }
}

impl NamespacedClient for WorkerClientBag {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn get_identity(&self) -> &str {
        &self.identity
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

#[derive(Debug, Clone)]
struct WorkerPollerInfo {
    /// Number of polling RPCs that are currently in flight.
    current_pollers: u32,
    last_successful_poll_time: Timestamp,
    is_autoscaling: bool,
}

struct HostInfo {
    host_name: String,
    process_id: u32,
    /// System used CPU as a float in the range [0.0, 1.0] where 1.0 is
    /// defined as all cores on the host pegged.
    current_host_cpu_usage: f32,
    /// System used memory as a float in the range [0.0, 1.0] where 1.0
    /// is defined as all available memory on the host is used.
    current_host_mem_usage: f32,
}

struct WorkerSlotsInfo<SK: SlotKind> {
    /// Number of slots available to the worker.
    available_slots: u32,
    /// Number of slots currently in use.
    used_slots: u32,
    /// Kind of the slot supplier, which is used to determine how the slots are allocated.
    /// Possible values: "Fixed | ResourceBased | Custom String"
    /// TODO: Use SlotSupplierOptions and figure out the whole generic thing
    slot_supplier_kind: SlotSupplierOptions<SK>,
    total_processed_tasks: u32,
    total_failed_tasks: u32,

    /// Number of tasks processed in since the last heartbeat from the worker.
    /// This is a cumulative counter, and it is reset to 0 each time the worker sends a heartbeat.
    /// Contains both successful and failed tasks.
    last_interval_processed_tasks: u32,
    /// Number of failed tasks processed since the last heartbeat from the worker.
    last_interval_failure_tasks: u32,
}
/// Heartbeat information
///
/// Note: Experimental
#[derive(Debug)]
pub(crate) struct WorkerHeartbeatInfo {
    in_memory_meter: Option<Arc<InMemoryMeter>>,
    pub(crate) data: WorkerHeartbeatData,
}

impl WorkerHeartbeatInfo {
    pub(crate) fn new(in_memory_meter: Option<Arc<InMemoryMeter>>, worker_config: WorkerConfig) -> Self {
        Self {
            in_memory_meter,
            data: WorkerHeartbeatData::new(worker_config),
        }
    }

    /// Transform heartbeat data into `WorkerHeartbeat` we can send in gRPC request. Some
    /// metrics are also cached into `self` for future calls of this function
    fn capture_heartbeat(&mut self) -> WorkerHeartbeat {
        println!("capture_heartbeat_data");
        println!("\tsdk_name {:?}", self.data.sdk_name);
        println!("\tsdk_version {:?}", self.data.sdk_version);
        let mut sticky_cache_hit = 0;
        if let Some(in_memory_meter) = &self.in_memory_meter {
            // TODO: propagate error
            let metrics = in_memory_meter.get_metrics().unwrap();
            for metric in metrics {
                for sm in metric.scope_metrics {
                    for m in sm.metrics {
                        println!("m.name {:?}", m.name);

                        // sticky cache name
                        if m.name == STICKY_CACHE_SIZE_NAME {
                            // println!("sticky_cache_size: {:#?}", m);
                            let agg: &dyn Aggregation = &*m.data;
                            let any = agg.as_any();
                            if let Some(gauge) = any.downcast_ref::<Gauge<u64>>() {
                                // TODO: Defensive program against if size > 1?
                                for data_point in &gauge.data_points {
                                    sticky_cache_hit = data_point.value;
                                }
                            }
                        } else if m.name == "sticky_cache_hit" {
                            println!("sticky_cache_hit: {:#?}", m);
                            let agg: &dyn Aggregation = &*m.data;
                            let any = agg.as_any();
                            if let Some(counter) = any.downcast_ref::<Sum<u64>>() {
                                // TODO: Defensive program against if size > 1?
                                for data_point in &counter.data_points {
                                    self.data.total_sticky_cache_hit = data_point.value as i32;
                                }
                            }

                        } else if m.name == "sticky_cache_miss" {
                            println!("sticky_cache_miss: {:#?}", m);
                            let agg: &dyn Aggregation = &*m.data;
                            let any = agg.as_any();
                            if let Some(counter) = any.downcast_ref::<Sum<u64>>() {
                                // TODO: Defensive program against if size > 1?
                                for data_point in &counter.data_points {
                                    self.data.total_sticky_cache_hit = data_point.value as i32;
                                }
                            }

                        }
                    }
                }
            }
        }

        let now = SystemTime::now();
        let elapsed_since_last_heartbeat = if let Some(heartbeat_time) = self.data.heartbeat_time {
            let dur = now.duration_since(heartbeat_time).unwrap(); // TODO: map_err
            Some(PbDuration {
                seconds: dur.as_secs() as i64,
                nanos: dur.subsec_nanos() as i32,
            })
        } else {
            None
        };

        self.data.heartbeat_time = Some(now.into());

        let heartbeat = WorkerHeartbeat {
            worker_instance_key: self.data.worker_instance_key.clone(),
            // TODO: get host name and process ID
            worker_identity: "".to_string(),
            host_info: None,
            task_queue: self.data.task_queue.clone(),
            deployment_version: None,
            sdk_name: self.data.sdk_name.clone(),
            sdk_version: self.data.sdk_version.clone(),
            status: 0,
            start_time: Some(self.data.start_time.into()),
            heartbeat_time: Some(SystemTime::now().into()),
            elapsed_since_last_heartbeat,
            workflow_task_slots_info: None,
            activity_task_slots_info: None,
            nexus_task_slots_info: None,
            local_activity_slots_info: None,
            workflow_poller_info: None,
            workflow_sticky_poller_info: None,
            activity_poller_info: None,
            nexus_poller_info: None,
            total_sticky_cache_hit: self.data.total_sticky_cache_hit,
            total_sticky_cache_miss: self.data.total_sticky_cache_miss,
            current_sticky_cache_size: sticky_cache_hit as i32,
        };
        println!("[hb]: {:#?}", heartbeat);
        heartbeat
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WorkerHeartbeatData {
    worker_instance_key: String,
    // worker_identity: String,
    // TODO: Customer metrics
    // host_info: HostInfo,
    // Time of the last heartbeat. This is used to both for heartbeat_time and last_heartbeat_time
    pub(crate) heartbeat_time: Option<SystemTime>, // TODO: Why is this option? Because the first heartbeat will be blank?
    pub(crate) task_queue: String,
    // worker_deployment_version: Option<temporal_sdk_core_api::worker::WorkerDeploymentVersion>,
    /// SDK name
    pub(crate) sdk_name: String,
    /// SDK version
    pub(crate) sdk_version: String,
    // status: WorkerStatus,
    /// Worker start time
    pub(crate) start_time: SystemTime,
    // // workflow_task_slots_info: WorkerSlotsInfo<WorkflowSlotKind>,
    // // ///
    // // activity_task_slots_info: WorkerSlotsInfo<ActivitySlotKind>,
    // // ///
    // // nexus_task_slots_info: WorkerSlotsInfo<NexusSlotKind>,
    // // local_activity_slots_info: WorkerSlotsInfo<LocalActivitySlotKind>,
    //
    // ///
    // workflow_poller_info: WorkerPollerInfo,
    // ///
    // activity_poller_info: WorkerPollerInfo,
    // ///
    // nexus_poller_info: WorkerPollerInfo,
    // ///
    // workflow_sticky_poller_info: WorkerPollerInfo,
    //
    // // TODO: Need to plumb this from metrics to here
    /// A Workflow Task found a cached Workflow Execution to run against.
    total_sticky_cache_hit: i32,
    /// A Workflow Task did not find a cached Workflow execution to run against.
    total_sticky_cache_miss: i32,
    // /// Current cache size, expressed in number of Workflow Executions.
    // current_sticky_cache_size: u32,
}

impl WorkerHeartbeatData {
    fn new(worker_config: WorkerConfig) -> Self {
        // let workflow_poller_info = WorkerPollerInfo {
        //     current_pollers: 0, // TODO
        //     last_successful_poll_time: Timestamp::default(), // TODO
        //     is_autoscaling: worker_config.workflow_task_poller_behavior.is_autoscaling(),
        // };
        // let activity_poller_info = WorkerPollerInfo {
        //     current_pollers: 0,
        //     last_successful_poll_time: Timestamp::default(),
        //     is_autoscaling: worker_config.activity_task_poller_behavior.is_autoscaling(),
        // };
        // // TODO: make_wft_poller seems important for this?
        // // TODO: I think WFTPollerShared will give the list
        // //  new_workflow_task
        // //  make_wft_poller
        // let nexus_poller_info = WorkerPollerInfo {
        //     current_pollers: 0,
        //     last_successful_poll_time: Timestamp::default(),
        //     is_autoscaling: worker_config.nexus_task_poller_behavior.is_autoscaling(),
        // };
        // // TODO: none of these values are right
        // let workflow_sticky_poller_info = WorkerPollerInfo {
        //     current_pollers: 0,
        //     last_successful_poll_time: Timestamp::default(),
        //     is_autoscaling: worker_config.activity_task_poller_behavior.is_autoscaling(),
        // };
        Self {
            sdk_name: String::new(),
            sdk_version: String::new(),
            task_queue: String::new(),
            start_time: SystemTime::now(),
            heartbeat_time: None,
            worker_instance_key: Uuid::new_v4().to_string(),
            total_sticky_cache_hit: 0,
            total_sticky_cache_miss: 0,
            // workflow_poller_info,
            // activity_poller_info,
            // nexus_poller_info,
            // workflow_sticky_poller_info
        }
    }
}
