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
    use crate::protos::temporal::api::workflowservice::v1::StartWorkflowExecutionRequest;

    #[test]
    fn test_extract_headers_with_existing_metadata() {
        let request = StartWorkflowExecutionRequest {
            namespace: "test-namespace".to_string(),
            workflow_id: "test-workflow-id".to_string(),
            ..Default::default()
        };

        let mut metadata = MetadataMap::new();
        metadata.insert("temporal-resource-id", "existing-value".parse().unwrap());

        let headers = extract_temporal_request_headers(&request as &dyn Any, Some(&metadata));

        // Should return empty since the header already exists in metadata
        assert_eq!(headers.len(), 0);
    }

    #[test]
    fn test_extract_headers_from_annotated_request() {
        // StartWorkflowExecutionRequest should have a request_header annotation
        // that generates "temporal-resource-id" header with value "workflow:{workflow_id}"
        let request = StartWorkflowExecutionRequest {
            namespace: "test-namespace".to_string(),
            workflow_id: "my-test-workflow".to_string(),
            workflow_type: Some(crate::protos::temporal::api::common::v1::WorkflowType {
                name: "TestWorkflow".to_string(),
            }),
            task_queue: Some(crate::protos::temporal::api::taskqueue::v1::TaskQueue {
                name: "test-task-queue".to_string(),
                kind: 0,
                normal_name: "test-task-queue".to_string(),
            }),
            ..Default::default()
        };

        let headers = extract_temporal_request_headers(&request as &dyn Any, None);

        let resource_header = headers
            .iter()
            .find(|(key, _)| key == "temporal-resource-id");

        assert!(
            resource_header.is_some(),
            "Expected to find temporal-resource-id header from proto annotation"
        );

        let (_, value) = resource_header.unwrap();
        assert_eq!(
            value, "workflow:my-test-workflow",
            "Expected header value to be interpolated template 'workflow:my-test-workflow'"
        );
    }
}
