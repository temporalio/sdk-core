use std::sync::Arc;

use temporal_sdk_core::legacy_query_failure;
use temporal_sdk_core_protos::temporal::api::deployment::v1::WorkerDeploymentOptions;
use temporal_sdk_core_protos::temporal::api::nexus;
use temporal_sdk_core_protos::temporal::api::query::v1::WorkflowQueryResult;
use temporal_sdk_core_protos::temporal::api::taskqueue::v1::{TaskQueue, TaskQueueMetadata};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
    get_system_info_response, respond_workflow_task_completed_request,
    GetWorkflowExecutionHistoryRequest, PollActivityTaskQueueRequest, PollNexusTaskQueueRequest,
    PollWorkflowTaskQueueRequest, RecordActivityTaskHeartbeatRequest, RespondActivityTaskCanceledRequest,
    RespondActivityTaskCompletedRequest, RespondActivityTaskFailedRequest, RespondNexusTaskCompletedRequest,
    RespondNexusTaskFailedRequest, RespondQueryTaskCompletedRequest, RespondWorkflowTaskCompletedRequest,
    RespondWorkflowTaskFailedRequest, ShutdownWorkerRequest,
    PollWorkflowTaskQueueResponse, PollActivityTaskQueueResponse, PollNexusTaskQueueResponse,
    RespondWorkflowTaskCompletedResponse, RespondActivityTaskCompletedResponse, RespondNexusTaskCompletedResponse,
    RecordActivityTaskHeartbeatResponse, RespondActivityTaskCanceledResponse, RespondActivityTaskFailedResponse,
    RespondWorkflowTaskFailedResponse, RespondNexusTaskFailedResponse, GetWorkflowExecutionHistoryResponse,
    RespondQueryTaskCompletedResponse, DescribeNamespaceResponse, ShutdownWorkerResponse,
    get_system_info_response::Capabilities,
};
use temporal_sdk_core_protos::temporal::api::common::v1::{Payloads, WorkflowExecution};
use temporal_sdk_core_protos::temporal::api::failure::v1::Failure;
use temporal_sdk_core_protos::temporal::api::enums::v1::{TaskQueueKind, WorkerVersioningMode, WorkflowTaskFailedCause};
use tokio::sync::oneshot;
use prost::Message;
use tonic::Status;

use temporal_sdk_core::{
    LegacyQueryResult,
    PollActivityOptions,
    PollOptions,
    PollWorkflowOptions,
    TaskToken,
    WorkerClient,
    WorkerConfig,
    WorkflowTaskCompletion
};

use temporal_client::{Client, Namespace, RetryClient, SlotManager};

use crate::{
    ByteArrayRef,
    ByteArray
};

use crate::callback_based_worker_client::interop::CallbackBasedWorkerClientRPCs;
use crate::callback_based_worker_client::interop_types::CallbackBasedWorkerClientPollOptions;
use crate::callback_based_worker_client::trampoline::worker_client_callback_trampoline;

/// Wrapper that implements the [WorkerClient] trait by using the closures from the [CallbackBasedWorkerClientRPCs]
pub(crate) struct CallbackBasedWorkerClient {
    /// Function pointer table implementing the RPC logic of the worker client.
    pub(crate) inner: CallbackBasedWorkerClientRPCs,
    /// Config of the worker.
    pub(crate) worker_config: WorkerConfig,
    /// Config of the worker client.
    pub(crate) client_config: WorkerClientConfig,

    /// Capabilities as read from the `get_system_info` RPC call made on client startup
    pub(crate) capabilities: Option<get_system_info_response::Capabilities>,
    /// Registry with workers using this client instance
    pub(crate) workers: Arc<SlotManager>
}

/// Configuration of the worker client.
pub(crate) struct WorkerClientConfig {
    pub(crate) client_name: String,
    pub(crate) client_version: String,
    pub(crate) identity: String
}

impl CallbackBasedWorkerClient {
    /// Takes the two ByteArray callbacks from the trampoline (success / failure) and
    /// either returns a decoded `Res` or a `Status` error.
    fn decode_callback_response<Res>(
        succ: ByteArray,
        fail: ByteArray,
    ) -> Result<Res, Status>
    where
        Res: Message + Default,
    {
        // Success path
        if !succ.data.is_null() {
            let buf = unsafe { Vec::from_raw_parts(succ.data as *mut u8, succ.size, succ.cap) };
            return Res::decode(&*buf)
                .map_err(|e| Status::internal(format!("decode error: {}", e)));
        }

        // Failure path: error string
        if !fail.data.is_null() {
            let msg = unsafe {
                String::from_utf8_unchecked(
                    Vec::from_raw_parts(fail.data as *mut u8, fail.size, fail.cap)
                )
            };
            return Err(Status::internal(msg));
        }

        // Both empty → cancelled
        Err(Status::cancelled("Cancelled by interop"))
    }

    /// Helper to get the worker deployment options.
    fn get_deployment_opts(&self) -> WorkerDeploymentOptions {
        let deployment_version = self.worker_config.computed_deployment_version();

        WorkerDeploymentOptions {
            deployment_name: deployment_version
                .as_ref()
                .map(|v| v.deployment_name.clone())
                .unwrap_or_default(),
            build_id: deployment_version
                .as_ref()
                .map(|v| v.build_id.clone())
                .unwrap_or_default(),
            worker_versioning_mode: match deployment_version {
                Some(_) => WorkerVersioningMode::Versioned as i32,
                None => WorkerVersioningMode::Unversioned as i32,
            },
        }
    }
}

/// Implementation of the [WorkerClient] trait, delegating the RPC logic to the interop implementation in [CallbackBasedWorkerClientRPCs]
#[async_trait::async_trait]
impl WorkerClient for CallbackBasedWorkerClient {
    async fn poll_workflow_task(
        &self,
        poll_options: PollOptions,
        wf_options:   PollWorkflowOptions,
    ) -> Result<PollWorkflowTaskQueueResponse, Status> {
        let task_queue = if let Some(sticky) = wf_options.sticky_queue_name {
            TaskQueue {
                name: sticky,
                kind: TaskQueueKind::Sticky.into(),
                normal_name: poll_options.clone().task_queue,
            }
        } else {
            TaskQueue {
                name: poll_options.clone().task_queue,
                kind: TaskQueueKind::Normal.into(),
                normal_name: "".to_string(),
            }
        };

        // Build request
        #[allow(deprecated)] // want to list all fields explicitly
        let req = PollWorkflowTaskQueueRequest {
            namespace: self.worker_config.namespace.to_string(),
            task_queue: Some(task_queue),
            identity: self.client_config.identity.clone(),
            binary_checksum: "".to_string(),        // deprecated
            worker_version_capabilities: None,      // deprecated
            deployment_options: Some(self.get_deployment_opts()),
            worker_heartbeat: None,
        };

        // Encode request
        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let interop_poll = CallbackBasedWorkerClientPollOptions::from(&poll_options);

        // Set up oneshot channel to interop layer
        let (tx, rx) = oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        // Fire the callback with trampoline callback
        unsafe {
            (self.inner.poll_workflow_task)(
                self.inner.user_data,
                req_ref,
                &interop_poll as *const _,
                worker_client_callback_trampoline,
                cb_sender,
            )
        }

        // Await trampoline's callback pair
        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        // Decode callback response
        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    async fn poll_activity_task(
        &self,
        poll_options: PollOptions,
        act_options:  PollActivityOptions,
    ) -> Result<PollActivityTaskQueueResponse> {
        let interop_poll_options = CallbackBasedWorkerClientPollOptions::from(&poll_options);

        #[allow(deprecated)]
        let req = PollActivityTaskQueueRequest {
            namespace: self.worker_config.namespace.to_string(),
            task_queue: Some(TaskQueue {
                name: poll_options.task_queue,
                kind: TaskQueueKind::Normal as i32,
                normal_name: "".to_string(),
            }),
            identity: self.client_config.identity.clone(),
            task_queue_metadata: act_options.max_tasks_per_sec.map(|tps| TaskQueueMetadata {
                max_tasks_per_second: Some(tps),
            }),
            worker_version_capabilities: None,      // deprecated
            deployment_options: Some(self.get_deployment_opts()),
            worker_heartbeat: None,
        };

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = tokio::sync::oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.poll_activity_task)(
                self.inner.user_data,
                req_ref,
                &interop_poll_options as *const _,
                worker_client_callback_trampoline,
                cb_sender,
            )
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    async fn poll_nexus_task(
        &self,
        poll_options: PollOptions,
    ) -> Result<PollNexusTaskQueueResponse> {
        let interop_poll_options = CallbackBasedWorkerClientPollOptions::from(&poll_options);

        #[allow(deprecated)]
        let req = PollNexusTaskQueueRequest {
            namespace: self.worker_config.namespace.to_string(),
            task_queue: Some(TaskQueue {
                name: poll_options.task_queue,
                kind: TaskQueueKind::Normal as i32,
                normal_name: "".to_string(),
            }),
            identity: self.client_config.identity.clone(),
            worker_version_capabilities: None,      // deprecated
            deployment_options: Some(self.get_deployment_opts()),
            worker_heartbeat: None,
        };

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = tokio::sync::oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.poll_nexus_task)(
                self.inner.user_data,
                req_ref,
                &interop_poll_options as *const _,
                worker_client_callback_trampoline,
                cb_sender,
            )
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse> {
        #[allow(deprecated)]
        let req = RespondWorkflowTaskCompletedRequest {
            task_token: request.task_token.into(),
            commands: request.commands,
            messages: request.messages,
            identity: self.client_config.identity.clone(),
            sticky_attributes: request.sticky_attributes,
            return_new_workflow_task: request.return_new_workflow_task,
            force_create_new_workflow_task: request.force_create_new_workflow_task,
            worker_version_stamp: None,     // deprecated
            binary_checksum: "".to_string(),     // deprecated
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
                            failure: None,      // TODO: https://github.com/temporalio/sdk-core/issues/867
                        },
                    )
                })
                .collect(),
            namespace: self.worker_config.namespace.to_string(),
            sdk_metadata: Some(request.sdk_metadata),
            metering_metadata: Some(request.metering_metadata),
            capabilities: Some(respond_workflow_task_completed_request::Capabilities {
                discard_speculative_workflow_task_with_events: true,
            }),
            deployment: None,       // deprecated
            versioning_behavior: request.versioning_behavior.into(),
            deployment_options: Some(self.get_deployment_opts()),
        };

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = tokio::sync::oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.complete_workflow_task)(
                self.inner.user_data,
                req_ref,
                worker_client_callback_trampoline,
                cb_sender,
            )
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        #[allow(deprecated)]
        let req = RespondActivityTaskCompletedRequest {
            task_token: task_token.0,
            result,
            identity: self.client_config.identity.clone(),
            namespace: self.worker_config.namespace.to_string(),
            worker_version: None,     // deprecated
            deployment: None,     // deprecated
            deployment_options: Some(self.get_deployment_opts()),
        };

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = tokio::sync::oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.complete_activity_task)(
                self.inner.user_data,
                req_ref,
                worker_client_callback_trampoline,
                cb_sender,
            );
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    async fn complete_nexus_task(
        &self,
        task_token: TaskToken,
        response:   nexus::v1::Response,
    ) -> Result<RespondNexusTaskCompletedResponse> {
        let req = RespondNexusTaskCompletedRequest {
            namespace: self.worker_config.namespace.to_string(),
            identity: self.client_config.identity.clone(),
            task_token: task_token.0,
            response: Some(response),
        };

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = tokio::sync::oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.complete_nexus_task)(
                self.inner.user_data,
                req_ref,
                worker_client_callback_trampoline,
                cb_sender,
            );
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        let req = RecordActivityTaskHeartbeatRequest {
            task_token: task_token.0,
            details,
            identity: self.client_config.identity.clone(),
            namespace: self.worker_config.namespace.to_string(),
        };

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = tokio::sync::oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.record_activity_heartbeat)(
                self.inner.user_data,
                req_ref,
                worker_client_callback_trampoline,
                cb_sender,
            );
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }


    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details:    Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        #[allow(deprecated)]
        let req = RespondActivityTaskCanceledRequest {
            task_token: task_token.0,
            details,
            identity: self.client_config.identity.clone(),
            namespace: self.worker_config.namespace.to_string(),
            worker_version: None,   // deprecated
            deployment: None,   // deprecated
            deployment_options: Some(self.get_deployment_opts()),
        };

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = tokio::sync::oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.cancel_activity_task)(
                self.inner.user_data,
                req_ref,
                worker_client_callback_trampoline,
                cb_sender,
            );
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure:    Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse> {
        #[allow(deprecated)]
        let req = RespondActivityTaskFailedRequest {
            task_token: task_token.0,
            failure,
            identity: self.client_config.identity.clone(),
            namespace: self.worker_config.namespace.to_string(),
            last_heartbeat_details: None,   // TODO: https://github.com/temporalio/sdk-core/issues/293
            worker_version: None,       // deprecated
            deployment: None,       // deprecated
            deployment_options: Some(self.get_deployment_opts()),
        };

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.fail_activity_task)(
                self.inner.user_data,
                req_ref,
                worker_client_callback_trampoline,
                cb_sender,
            )
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| tonic::Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause:      WorkflowTaskFailedCause,
        failure:    Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse> {
        #[allow(deprecated)]
        let req = RespondWorkflowTaskFailedRequest {
            task_token: task_token.0,
            cause: cause as i32,
            failure,
            identity: self.client_config.identity.clone(),
            binary_checksum: "".to_string(),        // deprecated
            namespace: self.worker_config.namespace.to_string(),
            messages: vec![],
            worker_version: None,       // deprecated
            deployment: None,        // deprecated
            deployment_options: Some(self.get_deployment_opts()),
        };

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.fail_workflow_task)(
                self.inner.user_data,
                req_ref,
                worker_client_callback_trampoline,
                cb_sender,
            )
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    async fn fail_nexus_task(
        &self,
        task_token: TaskToken,
        error:      nexus::v1::HandlerError,
    ) -> Result<RespondNexusTaskFailedResponse> {
        let req = RespondNexusTaskFailedRequest {
            namespace: self.worker_config.namespace.to_string(),
            identity: self.client_config.identity.clone(),
            task_token: task_token.0,
            error: Some(error),
        };

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = tokio::sync::oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.fail_nexus_task)(
                self.inner.user_data,
                req_ref,
                worker_client_callback_trampoline,
                cb_sender,
            );
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id:      Option<String>,
        page_token:  Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse> {
        let req = GetWorkflowExecutionHistoryRequest {
            namespace: self.worker_config.namespace.to_string(),
            execution: Some(WorkflowExecution {
                workflow_id,
                run_id: run_id.unwrap_or_default(),
            }),
            next_page_token: page_token,
            ..Default::default()
        };

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = tokio::sync::oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.get_workflow_execution_history)(
                self.inner.user_data,
                req_ref,
                worker_client_callback_trampoline,
                cb_sender,
            );
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    async fn respond_legacy_query(
        &self,
        task_token:  TaskToken,
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
        
        let req = RespondQueryTaskCompletedRequest {
            task_token: task_token.into(),
            completed_type: completed_type as i32,
            query_result,
            error_message,
            namespace: self.worker_config.namespace.to_string(),
            failure,
            cause: cause.into(),
        };

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = tokio::sync::oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.respond_legacy_query)(
                self.inner.user_data,
                req_ref,
                worker_client_callback_trampoline,
                cb_sender,
            );
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    async fn describe_namespace(&self) -> Result<DescribeNamespaceResponse> {
        let req = Namespace::Name(self.worker_config.namespace.to_string()).into_describe_namespace_request();

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.describe_namespace)(
                self.inner.user_data,
                req_ref,
                worker_client_callback_trampoline,
                cb_sender,
            );
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    async fn shutdown_worker(
        &self,
        sticky_task_queue: String,
    ) -> Result<ShutdownWorkerResponse> {
        let req = ShutdownWorkerRequest {
            namespace: self.worker_config.namespace.to_string(),
            identity: self.client_config.identity.clone(),
            sticky_task_queue,
            reason: "graceful shutdown".to_string(),
            worker_heartbeat: None,
        };

        let req_vec = req.encode_to_vec();
        let req_ref = ByteArrayRef { data: req_vec.as_ptr(), size: req_vec.len() };

        let (tx, rx) = tokio::sync::oneshot::channel::<(ByteArray, ByteArray)>();
        let cb_sender = Box::into_raw(Box::new(tx)) as *mut libc::c_void;

        unsafe {
            (self.inner.shutdown_worker)(
                self.inner.user_data,
                req_ref,
                worker_client_callback_trampoline,
                cb_sender,
            );
        }

        let (succ, fail) = rx
            .await
            .map_err(|_| Status::internal("Interop trampoline never called back"))?;

        CallbackBasedWorkerClient::decode_callback_response(succ, fail)
    }

    fn replace_client(&self, _: RetryClient<Client>) {
        unimplemented!("Replacing client not supported for CallbackBasedWorkerClient");
    }

    fn capabilities(&self) -> Option<Capabilities> {
        self.capabilities
    }

    fn workers(&self) -> Arc<SlotManager> {
        self.workers.clone()
    }

    fn is_mock(&self) -> bool {
        // Call sync closure
        unsafe { (self.inner.is_mock)(self.inner.user_data) }
    }

    fn sdk_name_and_version(&self) -> (String, String) {
        (self.client_config.client_name.clone(), self.client_config.client_version.clone())
    }
}

type Result<T, E = Status> = std::result::Result<T, E>;