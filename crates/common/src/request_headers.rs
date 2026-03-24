//! Request header extraction functionality.
//!
//! This module provides functionality to automatically extract field values from request messages
//! and convert them to HTTP headers based on protobuf annotations.

use std::any::Any;
use tonic::metadata::MetadataMap;

/// Options for extracting headers from request messages.
pub struct HeaderExtractionOptions<'a> {
    /// Existing metadata to check for duplicates.
    /// If provided, headers that already exist will not be added again.
    /// If None, no duplicate checking is performed.
    pub existing_metadata: Option<&'a MetadataMap>,
    /// Whether to include the temporal-namespace header if present.
    pub include_namespace: bool,
}

impl<'a> Default for HeaderExtractionOptions<'a> {
    fn default() -> Self {
        Self {
            existing_metadata: None,
            include_namespace: true,
        }
    }
}

/// Extract headers from request messages based on proto annotations.
/// Returns a vector of (header_name, header_value) pairs.
pub fn extract_request_headers(
    request: &dyn Any,
    opts: HeaderExtractionOptions<'_>,
) -> Vec<(String, String)> {
    extract_temporal_request_headers(request, opts.existing_metadata)
}

// Include the generated implementation
include!(concat!(env!("OUT_DIR"), "/request_header_impl.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protos::temporal::api::workflowservice::v1::StartWorkflowExecutionRequest;

    #[test]
    fn test_extract_headers_with_no_annotations() {
        let request = StartWorkflowExecutionRequest {
            namespace: "test-namespace".to_string(),
            workflow_id: "test-workflow-id".to_string(),
            ..Default::default()
        };

        let headers =
            extract_request_headers(&request as &dyn Any, HeaderExtractionOptions::default());

        // Since we don't have any actual annotations parsed yet, this should return empty
        assert_eq!(headers.len(), 0);
    }

    #[test]
    fn test_extract_headers_with_existing_metadata() {
        let request = StartWorkflowExecutionRequest {
            namespace: "test-namespace".to_string(),
            workflow_id: "test-workflow-id".to_string(),
            ..Default::default()
        };

        let mut metadata = MetadataMap::new();
        metadata.insert("existing-header", "existing-value".parse().unwrap());

        let headers = extract_request_headers(
            &request as &dyn Any,
            HeaderExtractionOptions {
                existing_metadata: Some(&metadata),
                include_namespace: false,
            },
        );

        // Should still return empty since no annotations are parsed
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

        let headers =
            extract_request_headers(&request as &dyn Any, HeaderExtractionOptions::default());

        // Should find the temporal-resource-id header from the proto annotation
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
