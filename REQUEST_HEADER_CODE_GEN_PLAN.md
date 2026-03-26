# Rust Implementation Plan: Request Header Code Generation

## Overview

This plan outlines the implementation of a Rust equivalent to the Go code generator added in [temporalio/api-go#236](https://github.com/temporalio/api-go/pull/236) that automatically extracts field values from request messages and propagates them as HTTP headers based on protobuf annotations defined in [temporalio/api#728](https://github.com/temporalio/api/pull/728).

## Background

The Go implementation adds:
1. Proto annotations (`temporal.api.protometa.v1.request_header`) that specify which fields should be extracted from request messages and set as headers
2. A code generator that parses these annotations and generates header extraction logic
3. Runtime functions to extract headers from request messages and add them to outgoing gRPC metadata

The annotations support template interpolation (e.g., `"workflow:{workflow_id}"`) where field paths in braces are replaced with actual field values.

## Current State Analysis

### Existing Rust Infrastructure
- **Protobuf Generation**: Uses `tonic_prost_build` in `crates/common/build.rs` to generate Rust types from proto files
- **Descriptor Access**: Already generates and saves `descriptors.bin` for use by existing code generators
- **Code Generation Pattern**: Existing `PayloadVisitorGenerator` shows the pattern for parsing descriptors and generating Rust code
- **gRPC Client**: `crates/client/src/grpc.rs` handles gRPC service calls with existing header support
- **Request Extensions**: `crates/client/src/request_extensions.rs` provides mechanisms for request modification

### Proto Annotations Available
The `temporal.api.protometa.v1.annotations.proto` file defines:
- `RequestHeaderAnnotation` message with `header` and `value` fields
- `request_header` extension for `google.protobuf.MethodOptions`
- Support for template interpolation with field paths in braces

## Implementation Plan

### Phase 1: Core Generator Infrastructure

#### 1.1 Request Header Generator Module
**Location**: `crates/common/build.rs` (extend existing build script)

**Components**:
- `RequestHeaderGenerator` struct to parse proto descriptors and extract annotation information
- Logic to identify methods with `request_header` annotations
- Template parsing to extract field paths from value templates (e.g., `"{workflow_execution.workflow_id}"`)
- Field accessor generation for nested proto field navigation

**Key Functions**:
```rust
struct RequestHeaderGenerator {
    // Maps method full names to their header extraction info
    method_headers: HashMap<String, Vec<MethodHeaderInfo>>,
}

struct MethodHeaderInfo {
    service_name: String,
    method_name: String,
    request_type: String,
    headers: Vec<HeaderInfo>,
}

struct HeaderInfo {
    header_name: String,
    value_template: String,
    field_paths: Vec<String>, // Extracted from template
}
```

#### 1.2 Code Generation Templates
Generate Rust code similar to Go's approach but idiomatic to Rust:

```rust
// Generated function signature
pub fn extract_temporal_request_headers<T>(
    request: &T,
    existing_metadata: Option<&tonic::metadata::MetadataMap>,
) -> Vec<(String, String)>
where
    T: std::any::Any,
{
    // Type-based dispatch and header extraction
}
```

#### 1.3 Integration with Build Process
Extend `crates/common/build.rs` to:
- Call the header generator after proto compilation
- Generate `request_header_impl.rs` alongside existing generated files
- Include the generated file in the build output

### Phase 2: Runtime Header Extraction

#### 2.1 Header Extraction API
**Location**: `crates/client/src/request_headers.rs` (new module)

**Core API**:
```rust
pub struct HeaderExtractionOptions<'a> {
    pub existing_metadata: Option<&'a tonic::metadata::MetadataMap>,
    pub include_namespace: bool, // Whether to add temporal-namespace header
}

pub fn extract_request_headers<T>(
    request: &T,
    opts: HeaderExtractionOptions<'_>,
) -> Vec<(String, String)>
where
    T: std::any::Any + Send + Sync,
```

#### 2.2 Field Accessor Utilities
Generate helper functions for navigating proto field paths:
```rust
// Example generated accessor for "{workflow_execution.workflow_id}"
fn get_workflow_execution_workflow_id(request: &StartWorkflowExecutionRequest) -> Option<&str> {
    request.workflow_execution.as_ref()?.workflow_id.as_deref()
}
```

#### 2.3 Template Interpolation
Handle template strings with field path substitution:
```rust
fn interpolate_template(template: &str, field_values: &[(&str, &str)]) -> String {
    // Replace "{field_path}" with actual values
}
```

### Phase 3: gRPC Client Integration

#### 3.1 Request Interceptor
**Location**: `crates/client/src/grpc.rs` (extend existing)

Add header extraction to the gRPC call pipeline:
```rust
impl<T> RawGrpcCaller for HeaderExtractingClient<T>
where
    T: RawGrpcCaller,
{
    async fn call<F, Req, Resp>(
        &mut self,
        call_name: &'static str,
        mut callfn: F,
        mut req: Request<Req>,
    ) -> Result<Response<Resp>, Status>
    where
        // ... trait bounds
    {
        // Extract headers from request body
        let headers = extract_request_headers(
            req.get_ref(),
            HeaderExtractionOptions {
                existing_metadata: Some(req.metadata()),
                include_namespace: true,
            },
        );
        
        // Add headers to request metadata
        for (key, value) in headers {
            if !req.metadata().contains_key(&key) {
                req.metadata_mut().insert(
                    key.try_into()?,
                    value.try_into()?,
                );
            }
        }
        
        self.inner.call(call_name, callfn, req).await
    }
}
```

### Phase 4: Error Handling & Edge Cases

#### 4.1 Error Handling Strategy
- **Build-time errors**: Invalid annotations, missing fields, type mismatches
- **Runtime errors**: Invalid header values, metadata insertion failures
- **Graceful degradation**: Continue operation if header extraction fails

#### 4.2 Edge Cases
- **Empty/missing fields**: Skip header if field value is empty
- **Nested message navigation**: Handle Option<> types in field chains
- **Duplicate headers**: Respect existing metadata, don't override
- **Invalid template syntax**: Compile-time validation of template strings

### Phase 5: Testing & Validation

#### 5.1 Unit Tests
- Template parsing and field path extraction
- Header generation for various request types
- Edge cases and error conditions

#### 5.2 Integration Tests
- End-to-end header propagation in gRPC calls
- Compatibility with existing client functionality
- Performance impact measurement

#### 5.3 Compatibility Testing
- Verify generated headers match Go implementation
- Test with actual Temporal server routing

## Implementation Questions & Considerations

### 1. Code Generation Approach
**Question**: Should we generate a single function that handles all request types, or individual functions per request type?

**Recommendation**: Single function with type-based dispatch (using `std::any::Any`) for better maintainability and smaller generated code size.

### 2. Performance Considerations
**Question**: What's the performance impact of reflection-based type dispatch?

**Considerations**: 
- Use compile-time type information where possible
- Consider caching field accessors
- Benchmark against Go implementation

### 3. Integration Point
**Question**: Where should header extraction be integrated in the client pipeline?

**Recommendation**: In `RawGrpcCaller::call` to ensure all requests go through header extraction automatically.

### 4. Namespace Header Handling
**Question**: Should we always include the namespace header like the Go implementation?

**Recommendation**: Yes, maintain compatibility by always checking for namespace field and adding the header if present.

### 5. Template Syntax Validation
**Question**: Should we validate template syntax at compile time or runtime?

**Recommendation**: Compile-time validation during code generation to catch errors early.

## Dependencies

- No new external dependencies required
- Uses existing `prost-types`, `tonic`, and `std::any` functionality
- Builds on existing code generation patterns in the codebase

## Future Considerations

1. **Performance optimization**: Consider compile-time code generation for better performance
2. **Additional header types**: Support for other header patterns beyond resource-id
3. **Dynamic header configuration**: Runtime configuration of header extraction rules
4. **Metrics**: Add telemetry for header extraction success/failure rates

## Conclusion

This implementation will provide Rust SDK users with automatic request header propagation equivalent to the Go SDK, enabling proper request routing in multi-cluster Temporal deployments while maintaining the existing client API and performance characteristics.
