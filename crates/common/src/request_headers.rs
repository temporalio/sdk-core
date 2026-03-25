//! Request header extraction functionality.
//!
//! This module provides functionality to automatically extract field values from request messages
//! and convert them to HTTP headers based on protobuf annotations.

use std::any::Any;
use tonic::metadata::MetadataMap;

// Include the generated implementation
include!(concat!(env!("OUT_DIR"), "/request_header_impl.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protos::temporal::api::workflowservice::v1::*;

    /// Helper to extract the temporal-resource-id header value from a request.
    fn extract_resource_id(request: &dyn Any) -> Option<String> {
        extract_temporal_request_headers(request, None)
            .into_iter()
            .find(|(k, _)| k == "temporal-resource-id")
            .map(|(_, v)| v)
    }

    #[test]
    fn existing_metadata_prevents_duplicate_header() {
        let request = StartWorkflowExecutionRequest {
            workflow_id: "wf-1".to_string(),
            ..Default::default()
        };
        let mut metadata = MetadataMap::new();
        metadata.insert("temporal-resource-id", "existing".parse().unwrap());

        let headers =
            extract_temporal_request_headers(&request as &dyn Any, Some(&metadata));
        assert!(headers.is_empty());
    }

    #[test]
    fn empty_field_produces_no_header() {
        let request = StartWorkflowExecutionRequest::default();
        assert_eq!(extract_resource_id(&request), None);
    }

    // --- Workflow request headers (annotation extracts from workflow_id or resource_id) ---

    #[test]
    fn start_workflow_execution() {
        let request = StartWorkflowExecutionRequest {
            workflow_id: "my-wf".to_string(),
            ..Default::default()
        };
        assert_eq!(extract_resource_id(&request), Some("workflow:my-wf".into()));
    }

    #[test]
    fn signal_workflow_execution() {
        let request = SignalWorkflowExecutionRequest {
            workflow_execution: Some(
                crate::protos::temporal::api::common::v1::WorkflowExecution {
                    workflow_id: "sig-wf".to_string(),
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("workflow:sig-wf".into())
        );
    }

    // --- Workflow task requests (resource_id field, annotation: "{resource_id}") ---
    // The SDK sets resource_id with prefix already applied (e.g., "workflow:wf-id"),
    // and the annotation passes it through as-is.

    #[test]
    fn respond_workflow_task_completed() {
        let request = RespondWorkflowTaskCompletedRequest {
            resource_id: "workflow:wf-completed".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("workflow:wf-completed".into())
        );
    }

    #[test]
    fn respond_workflow_task_failed() {
        let request = RespondWorkflowTaskFailedRequest {
            resource_id: "workflow:wf-456".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("workflow:wf-456".into())
        );
    }

    // --- Activity task requests (resource_id field, annotation: "{resource_id}") ---

    #[test]
    fn respond_activity_task_completed() {
        let request = RespondActivityTaskCompletedRequest {
            resource_id: "workflow:act-wf".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("workflow:act-wf".into())
        );
    }

    #[test]
    fn respond_activity_task_completed_standalone() {
        let request = RespondActivityTaskCompletedRequest {
            resource_id: "activity:standalone-act".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("activity:standalone-act".into())
        );
    }

    #[test]
    fn respond_activity_task_failed() {
        let request = RespondActivityTaskFailedRequest {
            resource_id: "workflow:fail-wf".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("workflow:fail-wf".into())
        );
    }

    #[test]
    fn respond_activity_task_canceled() {
        let request = RespondActivityTaskCanceledRequest {
            resource_id: "workflow:cancel-wf".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("workflow:cancel-wf".into())
        );
    }

    #[test]
    fn respond_activity_task_completed_by_id() {
        let request = RespondActivityTaskCompletedByIdRequest {
            resource_id: "workflow:byid-wf".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("workflow:byid-wf".into())
        );
    }

    #[test]
    fn respond_activity_task_failed_by_id() {
        let request = RespondActivityTaskFailedByIdRequest {
            resource_id: "activity:byid-act".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("activity:byid-act".into())
        );
    }

    #[test]
    fn respond_activity_task_canceled_by_id() {
        let request = RespondActivityTaskCanceledByIdRequest {
            resource_id: "workflow:cancel-byid-wf".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("workflow:cancel-byid-wf".into())
        );
    }

    // --- Heartbeat requests ---

    #[test]
    fn record_activity_task_heartbeat() {
        let request = RecordActivityTaskHeartbeatRequest {
            resource_id: "workflow:hb-wf".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("workflow:hb-wf".into())
        );
    }

    #[test]
    fn record_activity_task_heartbeat_by_id() {
        let request = RecordActivityTaskHeartbeatByIdRequest {
            resource_id: "activity:hb-act".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("activity:hb-act".into())
        );
    }

    // --- Worker heartbeat (annotation: "{resource_id}", SDK prefixes with "worker:") ---

    #[test]
    fn record_worker_heartbeat() {
        let request = RecordWorkerHeartbeatRequest {
            resource_id: "worker:some-grouping-key".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("worker:some-grouping-key".into())
        );
    }

    // --- Batch operation (annotation: "{resource_id}", SDK prefixes with "workflow:") ---

    #[test]
    fn execute_multi_operation() {
        let request = ExecuteMultiOperationRequest {
            resource_id: "workflow:multi-op-wf".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_resource_id(&request),
            Some("workflow:multi-op-wf".into())
        );
    }

    // --- Empty resource_id produces no header ---

    #[test]
    fn empty_resource_id_produces_no_header() {
        let request = RespondActivityTaskCompletedRequest::default();
        assert_eq!(extract_resource_id(&request), None);
    }
}
