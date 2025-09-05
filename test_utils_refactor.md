# Test Utils Crate Refactoring Analysis

This document provides a comprehensive inventory of all public exports from the `temporal-sdk-core-test-utils` crate and their usage patterns across the codebase.

## Overview

The `test-utils` crate creates a circular dependency issue with `core`:
- `core` dev-depends on `test-utils`
- `test-utils` depends on `core`

Analysis shows most utilities only need `sdk-core-protos` rather than full `core`, making refactoring feasible.

## Public Exports Inventory

### From `lib.rs`

#### Constants & Static Values
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `NAMESPACE` | ✅ (test_help) | ✅ (many) | ❌ | **Move to core** with test-utilities feature |
| `TEST_Q` | ✅ (test_help) | ✅ (many) | ❌ | **Move to core** with test-utilities feature |
| `INTEG_SERVER_TARGET_ENV_VAR` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |
| `INTEG_NAMESPACE_ENV_VAR` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |
| `INTEG_USE_TLS_ENV_VAR` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |
| `INTEG_API_KEY` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |
| `INTEG_TEMPORAL_DEV_SERVER_USED_ENV_VAR` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |
| `INTEG_TEST_SERVER_USED_ENV_VAR` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |
| `SEARCH_ATTR_TXT` | ❌ | ✅ (visibility tests) | ❌ | **Move to tests/common** |
| `SEARCH_ATTR_INT` | ❌ | ✅ (visibility tests) | ❌ | **Move to tests/common** |
| `OTEL_URL_ENV_VAR` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |
| `PROM_ENABLE_ENV_VAR` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |
| `PROMETHEUS_QUERY_API` | ❌ | ✅ (metrics tests) | ❌ | **Move to tests/common** |
| `ANY_PORT` | ❌ | ✅ (various tests) | ❌ | **Move to tests/common** |

#### Macros
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `prost_dur!` | ❌ | ✅ (many) | ❌ | **Move to sdk-core-protos** with test-utilities feature |

#### Core Workflow Command Helpers
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `start_timer_cmd` | ✅ (core_tests) | ✅ (many) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `schedule_activity_cmd` | ❌ | ✅ (few tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `schedule_local_activity_cmd` | ❌ | ✅ (few tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `query_ok` | ✅ (queries tests in core) | ✅ (query tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |

#### Integration Test Infrastructure (Heavy Dependencies on Core)
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `init_core_and_create_wf` | ❌ | ✅ (many integ tests) | ❌ | **Move to tests/common** |
| `CoreWfStarter` | ❌ | ✅ (many integ tests) | ❌ | **Move to tests/common** |
| `TestWorker` | ✅ (test_help, core_tests) | ✅ (many integ tests) | ❌ | **Move to tests/common** |
| `TestWorkerSubmitterHandle` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |
| `TestWorkerShutdownCond` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |
| `TestWorkerCompletionIceptor` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |
| `BoxDynActivationHook` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |

#### Replay Infrastructure
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `init_core_replay_preloaded` | ❌ | ✅ (replay tests) | ❌ | **Move to tests/common** |
| `init_core_replay_stream` | ❌ | ✅ (replay tests) | ❌ | **Move to tests/common** |
| `replay_sdk_worker` | ❌ | ✅ (replay tests) | ❌ | **Move to tests/common** |
| `replay_sdk_worker_stream` | ❌ | ✅ (replay tests) | ❌ | **Move to tests/common** |

#### Configuration & Client Setup
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `integ_worker_config` | ❌ | ✅ (many integ tests) | ❌ | **Move to tests/common** |
| `get_integ_server_options` | ❌ | ✅ (many integ tests) | ✅ (histfetch binary) | **Move to tests/common** |
| `get_integ_tls_config` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |
| `get_integ_telem_options` | ❌ | ✅ (integ tests) | ❌ | **Move to tests/common** |
| `get_cloud_client` | ❌ | ✅ (cloud tests) | ❌ | **Move to tests/common** |

#### Initialization & Runtime
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `init_integ_telem` | ✅ (core_tests/mod.rs) | ✅ (many integ tests) | ❌ | **Move to tests/common** |
| `DONT_AUTO_INIT_INTEG_TELEM` | ❌ | ✅ (some tests) | ❌ | **Move to tests/common** |

#### Utility Functions
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `fanout_tasks` | ✅ (core_tests) | ❌ | ❌ | **Move to core/src/core_tests_mod.rs** |
| `history_from_proto_binary` | ❌ | ✅ (few tests) | ❌ | **Move to tests/common** |
| `rand_6_chars` | ❌ | ✅ (some tests) | ❌ | **Move to tests/common** |
| `prom_metrics` | ❌ | ✅ (metrics tests) | ❌ | **Move to tests/common** |
| `eventually` | ❌ | ✅ (some tests) | ❌ | **Move to tests/common** |
| `drain_pollers_and_shutdown` | ✅ (test_help) | ✅ (some tests) | ❌ | **Move to core** with test-utilities feature |
| `AbortOnDrop` | ❌ | ✅ (metrics tests) | ❌ | **Move to tests/common** |

#### Traits
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `WorkerTestHelpers` | ✅ (core_tests) | ✅ (some tests) | ❌ | **Move to core** with test-utilities feature |
| `WorkflowHandleExt` | ❌ | ✅ (replay tests) | ❌ | **Move to tests/common** |

#### Ephemeral Server Support
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `default_cached_download` | ❌ | ✅ (ephemeral tests) | ❌ | **Move to tests/common** |

#### Re-exports
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `HttpProxy` | ❌ | ✅ (proxy tests) | ❌ | **Move to tests/common** |
| `HistoryForReplay` | ❌ | ✅ (replay tests) | ❌ | **Already in core, remove re-export** |

### From `canned_histories` module

All canned_histories functions return `TestHistoryBuilder` from `sdk-core-protos` and have minimal dependencies.

#### Timer Histories
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `single_timer` | ✅ (workflow_cancels) | ✅ (replay tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `single_timer_wf_completes` | ❌ | ✅ (few tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `cancel_timer` | ❌ | ✅ (timer tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `parallel_timer` | ❌ | ✅ (timer tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `long_sequential_timers` | ✅ (benches) | ✅ (replay tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |

#### Workflow Cancellation Histories
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `timer_wf_cancel_req_cancelled` | ✅ (workflow_cancels) | ❌ | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `timer_wf_cancel_req_completed` | ✅ (workflow_cancels) | ❌ | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `timer_wf_cancel_req_failed` | ✅ (workflow_cancels) | ❌ | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `timer_wf_cancel_req_do_another_timer_then_cancelled` | ✅ (workflow_cancels) | ❌ | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `immediate_wf_cancel` | ✅ (workflow_cancels) | ❌ | ❌ | **Move to sdk-core-protos** with test-utilities feature |

#### Activity Histories
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `single_activity` | ❌ | ✅ (activity tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `single_local_activity` | ❌ | ✅ (local activity tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `single_failed_activity` | ❌ | ✅ (activity tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `two_local_activities_one_wft` | ❌ | ✅ (local activity tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| Various activity cancellation histories | ❌ | ✅ (activity tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| Various activity timeout histories | ❌ | ✅ (activity tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |

#### Child Workflow Histories
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `single_child_workflow` | ✅ (core_tests/child_workflows) | ✅ (child workflow tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `single_child_workflow_fail` | ✅ (core_tests/child_workflows) | ✅ (child workflow tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `single_child_workflow_signaled` | ✅ (core_tests/child_workflows) | ✅ (child workflow tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| Various child workflow cancellation histories | ✅ (core_tests/child_workflows) | ✅ (child workflow tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |

#### Signal & Other Histories
| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `two_signals` | ❌ | ✅ (signal tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `lots_of_big_signals` | ✅ (benches) | ❌ | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `timer_then_continue_as_new` | ❌ | ✅ (continue as new tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| Various workflow failure histories | ❌ | ✅ (determinism tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| Various repro histories | ❌ | ✅ (regression tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |
| `write_hist_to_binfile` | ❌ | ✅ (few tests) | ❌ | **Move to sdk-core-protos** with test-utilities feature |

### From `interceptors` module

| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `ActivationAssertionsInterceptor` | ✅ (state machine tests) | ✅ (some integ tests) | ❌ | **Move to tests/common** |

### From `workflows` module

| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `la_problem_workflow` | ❌ | ✅ (local activity tests) | ❌ | **Move to tests/common** |

### From `http_proxy` module

| Export | Used in `core/` | Used in `tests/` | Used elsewhere | Recommendation |
|--------|-----------------|------------------|-----------------|----------------|
| `HttpProxy` (re-exported in lib.rs) | ❌ | ✅ (proxy tests) | ❌ | **Move to tests/common** |

## Binaries

| Binary | Dependencies | Recommendation |
|--------|--------------|----------------|
| `histfetch` | Uses `get_integ_server_options` | **Move to tests/bin/** |

## Summary

### Items to move to `core` with `test-utilities` feature:
- `NAMESPACE`, `TEST_Q`
- `start_timer_cmd`, `schedule_activity_cmd`, `schedule_local_activity_cmd`, `query_ok`
- `drain_pollers_and_shutdown`
- `WorkerTestHelpers` trait

### Items to move to `core/src/core_tests/mod.rs`:
- `fanout_tasks`

### Items to move to `sdk-core-protos` with `test-utilities` feature:
- `prost_dur!` macro
- All `canned_histories` functions (40+ functions)

### Items to move to `tests/common/`:
- All integration test constants
- All integration test infrastructure (`CoreWfStarter`, `TestWorker`, etc.)
- All replay infrastructure
- Configuration & client setup functions
- Telemetry initialization
- Utility functions specific to integration tests
- `ActivationAssertionsInterceptor`
- `la_problem_workflow`
- `HttpProxy`
- `histfetch` binary

This refactoring will:
1. ✅ Eliminate the circular dependency
2. ✅ Keep commonly used helpers accessible to both unit and integration tests
3. ✅ Move integration-specific code to `tests/common`
4. ✅ Maintain clean separation of concerns
