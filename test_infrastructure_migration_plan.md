# TestWorker Migration Plan

## Overview
This plan addresses moving TestWorker-dependent tests from core unit tests to integration tests, while exposing necessary test infrastructure from core behind the `test-utilities` feature flag.

## Files Using mock_sdk/TestWorker Infrastructure

### Files to Analyze:
1. `/core/src/test_help/mod.rs` - Test infrastructure (needs partial exposure)
2. `/core/src/core_tests/determinism.rs` - 4 unit tests to move
3. `/core/src/core_tests/workflow_tasks.rs` - Very large file with many unit tests
4. `/core/src/core_tests/local_activities.rs` - 25+ unit tests to move
5. `/core/src/core_tests/child_workflows.rs` - Child workflow unit tests to move
6. `/core/src/worker/workflow/history_update.rs` - 2 unit tests to move

## Infrastructure Analysis

### Current TestWorker Dependencies
The `mock_sdk` and `TestWorker` infrastructure creates complex circular dependencies:

```rust
// In test_help/mod.rs
TestWorker {
    inner: temporal_sdk::Worker,  // <- This creates the circular dependency
    core_worker: Arc<dyn CoreWorker>,
}
```

The TestWorker provides:
- `register_wf()` - Register workflow functions
- `register_activity()` - Register activity functions
- `submit_wf()` - Submit workflows for execution
- `run_until_done()` - Execute until completion

### Infrastructure to Expose Publicly

From `/core/src/test_help/mod.rs`, these items need `pub` visibility behind `#[cfg(feature = "test-utilities")]`:

#### Core Mock Infrastructure:
```rust
pub struct MockPollCfg {
    pub workflow_responses: VecDeque<ValidPollWFTQResponse>,
    pub activity_responses: VecDeque<PollActivityTaskQResponse>,
    pub num_expected_fails: usize,
    pub num_expected_completions: Option<BTreeSet<u32>>,
    // ... other fields
}

impl MockPollCfg {
    pub fn from_resp_batches() -> Self
    pub fn from_single_resp() -> Self
    // ... other methods
}

pub fn build_mock_pollers(cfg: MockPollCfg) -> MocksHolder
pub fn mock_worker(mocks: MocksHolder) -> Arc<dyn CoreWorker>
pub fn single_hist_mock_sg() -> MocksHolder
pub fn hist_to_poll_resp() -> ValidPollWFTQResponse
```

#### Response Types and Utilities:
```rust
pub enum ResponseType {
    AllHistory,
    ToTaskNum(u32),
    OneTask(u32),
    UntilResolved(BoxFuture<'static, ()>, u32),
}

pub struct MocksHolder {
    pub workflow_machines: MockWorkerClient,
    pub outstanding_task_map: Option<OutstandingTaskMap>,
    // ... other fields
}
```

#### Helper Functions:
```rust
pub fn mock_worker_client() -> MockWorkerClient
```

## Migration Strategy

### Phase 1: Expose Core Infrastructure
1. Add `#[cfg(feature = "test-utilities")]` guards to mock infrastructure
2. Make key structs and functions `pub` in `test_help/mod.rs`:
   - `MockPollCfg` and its methods
   - `build_mock_pollers()`
   - `mock_worker()`
   - `single_hist_mock_sg()`
   - `hist_to_poll_resp()`
   - `ResponseType` enum
   - `MocksHolder` struct
   - `mock_worker_client()`
3. Add `#[allow(missing_docs)]` to the module, we don't need to add docs for these

### Phase 2: Port unit test code using the TestWorker to tests/common
1. Uses the exposed core infrastructure from Phase 1
2. Copy the `mock_sdk()`, `mock_sdk_cfg()`, and `build_fake_sdk()` functions from `core/src/test_help/mod.rs`

### Phase 3: Move Unit Tests to Integration Tests

#### Files to Move Entirely:
1. **`/core/src/core_tests/determinism.rs`** → `/tests/determinism_tests.rs`
   - 4 tests: `test_panic_wf_task_rejected_properly`, `test_wf_task_rejected_properly_due_to_nondeterminism`, `activity_id_or_type_change_is_nondeterministic`, `child_wf_id_or_type_change_is_nondeterministic`, `repro_channel_missing_because_nondeterminism`

2. **`/core/src/core_tests/child_workflows.rs`** → `/tests/child_workflow_tests.rs`
   - All child workflow tests that use `mock_sdk`

3. **`/core/src/core_tests/local_activities.rs`** → `/tests/local_activity_tests.rs`
   - 25+ local activity tests that use `mock_sdk` or `mock_sdk_cfg`

#### Files with Partial Migration:
4. **`/core/src/core_tests/activity_tasks.rs`** - Move only tests using the SDK
5. **`/core/src/core_tests/workflow_tasks.rs`** - Very large (37k+ tokens)
   - Needs analysis to identify which specific test functions use `mock_sdk`
   - Only move tests that require TestWorker/mock_sdk
   - Leave pure unit tests that test core logic without SDK

6. **`/core/src/worker/workflow/history_update.rs`**
   - Has 2 tests using `mock_sdk_cfg`: `test_incremental_with_new_task_in_cache`, `test_incremental_with_task_already_in_cache`
   - Move these 2 tests to `/tests/workflow_history_tests.rs`

7. Various tests located within implementation files, some of which are using the SDK. These are scattered throughout

### Phase 4: Update Test Infrastructure Access
1. Update moved tests to use:
   - `temporal_sdk_core::test_help::*` for exposed core infrastructure
   - `crate::common::TestWorker` and `crate::common::{mock_sdk, mock_sdk_cfg}` for the new integration test functions
2. Ensure all tests maintain the same behavior and assertions

### Phase 5: Remove TestWorker Infrastructure from Core
1. Remove `mock_sdk()`, `mock_sdk_cfg()`, and `build_fake_sdk()` functions from core
3. Remove SDK-related registration and execution logic from core
4. Keep only the core mock infrastructure that was exposed in Phase 1

## Expected Benefits
1. **Eliminates Circular Dependencies**: Core no longer depends on temporal-sdk
2. **Cleaner Architecture**: Clear separation between unit tests (core logic) and integration tests (full workflow execution)
3. **IDE Compatibility**: rust-analyzer will work properly without circular dependency confusion
4. **Maintainability**: Test infrastructure has clear boundaries and responsibilities

## Risk Mitigation
1. **Preserve Test Coverage**: All existing tests maintain same assertions and behavior
2. **Incremental Migration**: Move files one at a time with verification
3. **Backward Compatibility**: Exposed infrastructure maintains stable API
4. **Documentation**: Clear documentation of what infrastructure is available where

## Implementation Order
1. Expose core infrastructure with feature flags
2. Create integration TestWorker in tests/common
3. Move determinism.rs (smallest, simple tests)
4. Move child_workflows.rs (medium complexity)
5. Move local_activities.rs (many tests, well-contained)
6. Analyze and partially move workflow_tasks.rs (largest file)
7. Move history_update.rs tests (smallest number of tests)
8. Clean up removed infrastructure from core

This approach maintains test functionality while eliminating the problematic circular dependency between core and SDK.
