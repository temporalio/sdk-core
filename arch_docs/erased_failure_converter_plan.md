# Erased Failure Converter Plan

This document describes a lower-churn plan for failure encoding starting from the current
`master` state, not from the experimental branch state.

The goal is to keep the outbound failure-converter API type-erased while making its behavior
explicit, testable, and robust against wrapper chains.

Decoding remains out of scope. Inbound failures will continue to be surfaced as raw failure protos
or SDK error wrappers around raw failure protos.

## Why This Direction

The typed outbound-failure approach has some real advantages, but it also has meaningful costs:

- it broadens the change significantly
- it requires reshaping public SDK error surfaces
- it makes some common `?`-based user flows worse
- it does not fully eliminate erased errors anyway, because query/update/signal handlers still
  fundamentally produce erased user errors

Given that, a simpler direction is to keep the failure-converter boundary erased and make the SDK's
classification logic much more deliberate.

## Changes That Still Make Sense From This Branch

Keeping an erased failure-converter boundary does not mean all of the type cleanup from the
experimental branch should be discarded.

Two changes are still worth carrying forward explicitly because they improve the user-facing error
model independently of the converter signature.

### 1. Introduce `ApplicationFailure`

The project should still define a first-class `ApplicationFailure` type for user-authored
application errors:

```rust
pub struct ApplicationFailure {
    source: anyhow::Error,
    type_name: Option<String>,
    non_retryable: bool,
    next_retry_delay: Option<Duration>,
    category: ApplicationErrorCategory,
    details: Option<Payloads>,
}
```

This type should live in `temporalio_common::error` and be re-exported from `temporalio_sdk`.
It should remain the canonical way to attach retry metadata and explicit failure type information
to user errors.

Recommended API shape:

- `ApplicationFailure::new(err)`
- `ApplicationFailure::non_retryable(err)`
- builder-style configuration for the optional fields
- accessors for the underlying error and metadata

`ApplicationFailure` should also continue to implement `std::error::Error`. That keeps it usable
with `anyhow`, enables normal error chaining, and preserves good ergonomics for activity/workflow
authors.

The builder should use `bon`, matching the style already used elsewhere in the repo. The
recommended shape is:

- provide simple default constructors for the two common cases:
  - retryable application failure with no extra metadata
  - non-retryable application failure with no extra metadata
- provide builder methods for the optional metadata:
  - `type_name`
  - `next_retry_delay`
  - `category`
  - raw `details`

This plan does not include typed-details helpers. If callers want to attach details in this round,
they must provide already-encoded payloads.

### Application failure details policy

`ApplicationFailure` details should be modeled as raw payloads, not typed Rust values.

That means:

- store `details` as `Payloads` or `Vec<Payload>` in `ApplicationFailure`
- store `category` explicitly as `ApplicationErrorCategory`
- treat `category` as a normal round-trippable field
- do not promise typed round-tripping for `details`

This is a direct consequence of keeping the outbound failure-converter boundary erased:

- on the encode side, the converter can faithfully write payloads
- on the decode side, the SDK cannot recover the original Rust type identity in a principled way
- returning some erased dynamic value would create a weak and confusing contract

So the contract should stay honest:

- categories round-trip cleanly
- details are transport payloads
- typed details helpers, if added later, are outbound conveniences only and not a round-trip
  guarantee

### 2. Reshape `ActivityError`

`ActivityError` should still move to the cleaner shape developed on this branch:

```rust
pub enum ActivityError {
    Application(ApplicationFailure),
    Cancelled { details: Option<Payload> },
    WillCompleteAsync,
}
```

This is still worthwhile even if the converter remains erased because it separates:

- application failure
- cancellation-as-completion
- explicit async control flow

and removes the older "arbitrary boxed error plus side-band flags" shape.

With this design:

- normal user errors can still convert into `ActivityError::Application(...)`
- richer failure metadata stays attached through `ApplicationFailure`
- `WillCompleteAsync` remains explicit control flow, not an error-encoding concern

This should be part of the initial implementation work, not a later cleanup, because the new
`ActivityError` shape establishes the application-failure surface the converter is meant to encode.

### 3. Keep workflow-facing application failures explicit where practical

Even if `WorkflowTermination` remains closer to the current `master` model, the branch direction is
still informative:

- application-style workflow failures should ideally be represented as `ApplicationFailure`
- special workflow failure categories such as activity/child-workflow/signal propagation will still
  need classifier support

The key point is that `ApplicationFailure` is still the canonical user-authored application-error
type even if the converter accepts erased errors.

## Core Idea

Keep the converter input erased:

```rust
fn to_failure(
    &self,
    error: Box<dyn std::error::Error>,
    payload_converter: &PayloadConverter,
    context: &SerializationContextData,
) -> Result<Failure, PayloadConversionError>;
```

or, if implementation ergonomics are better, normalize to `anyhow::Error` internally before
classification.

The important change is not the public converter input type. The important change is that outbound
failure encoding must go through a single classification step before producing a `Failure` proto.

## Key Risks With An Erased Boundary

An erased boundary has two main failure modes:

1. A new top-level SDK error type is introduced and the classifier is not updated to recognize it.
2. A recognized SDK error is wrapped one or more levels deep and classification stops too early.

Both are real concerns. This design is only acceptable if the implementation and tests are built to
detect both.

## Important Distinction: Top-Level Classification vs Nested Causes

Walking the error chain must be done carefully.

It is valid for a failure to be genuinely nested. For example:

- an application failure caused by an activity timeout
- an application failure caused by a child workflow failure

Those cases should remain application failures with nested causes. The classifier must not blindly
promote any recognized inner error to the top-level failure kind.

The intended rule is:

- classify based on the first recognized SDK failure category encountered in the chain when the
  outer wrappers are only transport wrappers such as `anyhow` context
- do not discard a user-authored `ApplicationFailure` just because its cause chain contains a
  special SDK failure type

That means the implementation needs to distinguish:

- wrappers that only add context
- wrappers that are themselves semantically meaningful failure categories

## Required Structural Changes

The current implementation shape in `master` is not a good fit for this work:

- `temporalio_common::data_converters` is still a single large file
- `DefaultFailureConverter` lives inline in that file and is still `todo!()`
- several SDK error types that the converter must recognize are still defined in
  `temporalio_sdk`

Before implementing classification logic, reshape the modules so the ownership boundaries are
clear.

### 1. Create a shared error module in `common`

Add a new `temporalio_common::error` module and move the SDK error types that must be understood by
the failure converter into it.

The minimum move set is:

- `ApplicationFailure`
- `ActivityExecutionError`
- `ChildWorkflowExecutionError`
- `ChildWorkflowSignalError`

These types are currently defined in `crates/sdk/src/workflow_context.rs`, but they are really
cross-cutting transport/error boundary types:

- workflow-facing APIs return them from `sdk`
- outbound failure classification needs to recognize them from `common`
- they only depend on shared types (`Failure`, `PayloadConversionError`, and proto enums)

Putting them in `temporalio_common::error` avoids making the converter depend back on `sdk`, which
would otherwise force an unhealthy crate boundary.

This module should be the long-term home for shared SDK/common error definitions, not just a
temporary staging area for this project.

### 2. Re-export moved errors from `sdk`

The move to `common` should not force users to rewrite imports immediately.

`temporalio_sdk` should re-export the moved types:

- from a dedicated `temporalio_sdk::error` module
- from the existing top-level public surface where those types are already exported today

That keeps the public SDK ergonomics stable while still making `common` the canonical
implementation home.

Concretely, the compatibility target is:

- existing imports such as `temporalio_sdk::ApplicationFailure` continue to work
- existing imports such as `temporalio_sdk::ActivityExecutionError` continue to work
- new imports such as `temporalio_sdk::error::ApplicationFailure` also work
- new imports such as `temporalio_sdk::error::ActivityExecutionError` also work
- the actual type definition lives in `temporalio_common::error`

### 3. Split the failure converter into its own data-converter submodule

The failure-converter implementation will be large enough that it should not stay inline in
`crates/common/src/data_converters.rs`.

Reshape `data_converters` into a module tree:

- `crates/common/src/data_converters/mod.rs`
- `crates/common/src/data_converters/failure_converter.rs`

`mod.rs` should continue to expose the current public `data_converters` API surface, while
`failure_converter.rs` owns:

- `FailureConverter`
- `DefaultFailureConverter`
- classifier-specific helper enums/structs
- failure encoding helpers
- targeted unit tests for classification and encoding

This split is important because the failure converter has different complexity and test needs than
the payload converter path. Keeping it isolated will make it much easier to review and extend.

## Proposed Implementation Shape

## 1. Centralize classification

Add one internal classification function that all outbound encoding paths use:

```rust
enum ClassifiedFailure {
    Application(ApplicationFailure),
    ActivityExecution(ActivityExecutionError),
    ChildWorkflowExecution(ChildWorkflowExecutionError),
    ChildWorkflowSignal(ChildWorkflowSignalError),
}

fn classify_error(err: &(dyn std::error::Error + 'static)) -> ClassifiedFailure
```

If `anyhow::Error` is the actual working type, the same idea applies:

```rust
fn classify_anyhow(err: &anyhow::Error) -> ClassifiedFailure
```

The failure converter should then be:

1. classify
2. encode according to the classified variant

That keeps all recognition rules in one place.

## 1a. Start with an explicit transparent-wrapper allowlist

The classifier should not treat "anything with a `source()`" as transparent.

Instead, start with a narrow set of chain-walking rules that are known to add context without
changing the semantic failure category:

- walk the erased `source()` chain looking for recognized SDK failure categories
- stop relying on wrapper-type recovery that would require `temporalio_common` to depend on SDK
  wrapper types directly

Because `temporalio_common` cannot depend back on `temporalio_sdk`, SDK-local wrappers such as
`WorkflowError::Execution(anyhow::Error)` and `WorkflowTermination::Failed(anyhow::Error)` should
be unwrapped at the SDK call sites before invoking the shared failure converter.

Initial non-goals for wrapper transparency:

- `WorkflowError::PayloadConversion`
- `ActivityExecutionError::Serialization`
- `ChildWorkflowExecutionError::Serialization`
- `ChildWorkflowSignalError::Serialization`
- `ActivityError::Application(ApplicationFailure)` after the `ActivityError` reshape

Those are semantically meaningful categories or local-conversion failures, not transport wrappers,
and should not be treated as transparent by default.

The allowlist can expand later, but the first implementation should keep it explicit and small.

## 2. Walk the full error chain

Classification should inspect the full chain, not just the outermost error.

It should recognize:

- `ApplicationFailure`
- `ActivityExecutionError`
- `ChildWorkflowExecutionError`
- `ChildWorkflowSignalError`

and any other special SDK top-level error categories that are expected to be encoded.

However, it must preserve nesting semantics:

- if the outer error is an `ApplicationFailure`, it stays an application failure
- recognized inner SDK errors should become nested causes, not a replacement top-level category

In practice, this likely means:

- first check whether the outer error itself is a semantically meaningful SDK failure type
- then inspect inner sources only to unwrap transport/context layers
- avoid "deepest recognized type wins"

## 3. Handle conversion failures explicitly at the call site

Failure encoding happens on an error path, but the converter itself cannot be made infallible.

For some failure categories, the converter necessarily needs the payload converter:

- `ApplicationFailure` details
- encoded attributes / encoded common failure fields
- any nested cause path that recursively needs payload encoding

That means `FailureConverter::to_failure` must remain fallible:

```rust
fn to_failure(
    &self,
    error: Box<dyn std::error::Error>,
    payload_converter: &PayloadConverter,
    context: &SerializationContextData,
) -> Result<Failure, PayloadConversionError>;
```

The plan should therefore focus on which call sites locally recover from conversion failure and how
they do it.

### Workflow scope for this project

For workflow activation failure completion, add an explicit local fallback around
`failure_converter.to_failure(...)`.

If conversion of the intended workflow failure fails, emit a synthetic plain application failure
without re-entering the configured failure converter. The fallback message should follow this shape:

`Failed converting error to failure: <converter error>, original error message: <original error>`

Important properties of this fallback:

- it must not call the configured failure converter again
- it must not require payload conversion
- it must be built from fixed proto construction only
- it should be used for top-level workflow failure / panic style completion paths where the worker
  would otherwise fail the activation instead of producing a workflow failure

### Activity scope for this project

For activity failure completion, also add an explicit local fallback around
`failure_converter.to_failure(...)`.

The first implementation should use the same non-recursive synthetic application-failure fallback
as the workflow path, rather than Ruby's "try converting the converter error again" behavior.

That is a better initial policy because it is:

- simpler to reason about
- guaranteed not to recurse through a broken converter
- guaranteed to produce either a real failure completion or a single well-defined fallback failure

### Deferred scope

Query failure, update rejection/failure, and client-side async activity failure completion should be
handled in a follow-up once the workflow and activity policies are working and tested.

Those paths are more nuanced because today a thrown conversion error often escapes into broader task
failure machinery instead of being turned into the originally intended response.

## 4. Continue routing user-originated failure creation through the converter

Long term, all user-originated outbound failure paths should still go through the configured
failure converter.

In scope for this project:

- activity execution failure
- activity panic
- top-level workflow failure
- workflow panic

Deferred follow-up scope:

- explicit async activity failure completion
- query failure
- update rejection
- update failure

SDK-generated failures such as missing handlers can remain inline if they are intentionally not part
of user failure conversion policy.

## Concrete Work Breakdown

## Phase 1: Establish the final public error surface

Goal: make the shared error types and activity-facing application-error model match the converter
design before the classifier work begins.

Changes:

- add `crates/common/src/error.rs`
- export it from `crates/common/src/lib.rs`
- add `ApplicationFailure` to `temporalio_common::error`
- re-export `ApplicationFailure` from `temporalio_sdk`
- add `category` and raw-payload `details` to `ApplicationFailure`
- implement `ApplicationFailure` as a `bon` builder-style type
- provide simple retryable/non-retryable constructors that do not require configuring any optional
  fields
- reshape `ActivityError` to:
  - `Application(ApplicationFailure)`
  - `Cancelled { details: Option<Payload> }`
  - `WillCompleteAsync`
- remove the older retryable/non-retryable split in favor of `ApplicationFailure` metadata
- keep normal `From<E>` conversions ergonomic for user code
- do not add typed detail helpers in this phase

Verification:

- activity examples and macros still compile against the new `ActivityError` shape
- existing SDK-level imports can be preserved via re-exports where appropriate
- application-failure APIs make the payload-only details contract explicit

## Phase 2: Module and crate reshaping

Goal: create the ownership boundaries needed for the real implementation work.

Changes:

- move `ActivityExecutionError`, `ChildWorkflowExecutionError`, and `ChildWorkflowSignalError`
  into `temporalio_common::error`
- update `crates/sdk/src/workflow_context.rs` to import those types instead of defining them
- add `temporalio_sdk::error` as a re-exporting module
- preserve existing top-level `pub use` re-exports from `crates/sdk/src/lib.rs`
- split `crates/common/src/data_converters.rs` into `mod.rs` plus
  `data_converters/failure_converter.rs`

Verification:

- `cargo test` still passes for the crates that compile these public surfaces
- no public import path regressions inside the workspace

## Phase 3: Build the classifier in the new failure-converter module

Goal: replace the current inline stub with a real, reviewable classifier/encoder boundary.

Changes:

- implement a private classifier enum and chain-walking helper
- implement the initial transparent-wrapper allowlist explicitly:
  - source-chain walking inside `temporalio_common`
  - SDK-local wrappers are unwrapped before calling into `temporalio_common`
- define what counts as a semantic failure category versus a transparent wrapper
- keep the recognition list explicit rather than relying on ad hoc downcasts at each call site
- keep all conversion from classified variants to `Failure` in one module

Verification:

- direct unit tests cover all recognized categories
- wrapped errors classify identically to unwrapped equivalents

## Phase 4: Add explicit workflow/activity conversion-failure handling

Goal: define what happens when `to_failure(...)` itself fails in the first in-scope call sites.

Changes:

- wrap workflow failure-completion call sites in explicit recovery
- wrap activity failure-completion call sites in explicit recovery
- build the fallback failure directly, without recursively invoking the configured converter
- use a fixed fallback message that includes both converter error and original error text
- document query/update/client-side async activity failure handling as deferred follow-up work

Verification:

- unit tests cover direct fallback-failure construction
- integration coverage proves workflow and activity failure paths still emit a `Failure` when
  detail encoding breaks

## Phase 5: Route every intended outbound seam through the shared converter

Goal: eliminate divergence between "special" failure-producing paths.

Changes:

- inventory every user-originated failure emission seam
- update each seam to call the configured `DataConverter` failure converter instead of open-coding
  `Failure` construction
- keep intentionally inline SDK-generated failures documented as exclusions

The initial seam inventory should explicitly include:

- activity function error return
- activity panic path
- top-level workflow return error path
- workflow panic path

Verification:

- a single documentable list exists for future reviewers
- integration tests cover every listed in-scope seam
- deferred query/update/client-side paths are called out explicitly instead of being left ambiguous

## Phase 6: Public API cleanup and documentation

Goal: leave the new structure understandable to users and maintainers.

Changes:

- document `temporalio_common::error` as the canonical home for shared error types
- document `temporalio_sdk::error` as the preferred SDK-facing import module
- update any README or rustdoc references that point to the old implementation locations
- add a short module-level note in `failure_converter.rs` explaining why failure conversion is
  isolated from the rest of `data_converters`

Verification:

- rustdoc paths and examples refer to the new module layout
- the moved error types are discoverable from the SDK public surface

## Testing Strategy

The testing burden is the main tradeoff of keeping the converter erased. The tests need to prove the
classifier is robust.

## 1. Classifier unit tests

Where these tests live:

- `crates/common/src/data_converters/failure_converter.rs`
- in a dedicated `#[cfg(test)]` module next to the classifier implementation

Execution mode:

- pure unit tests
- no worker
- no replay

Purpose:

- prove that the classifier chooses the expected top-level failure kind before any worker plumbing
  is involved

Test matrix:

- plain user error
  - construct a custom error type with no special SDK wrapper
  - direct form, one `anyhow` wrapper, and multiple `anyhow` wrappers
  - assert the classifier chooses application failure
  - assert the encoded proto has `ApplicationFailureInfo`
  - assert the innermost custom error type name becomes the application failure type when no
    explicit `ApplicationFailure.type_name` is provided
- `ApplicationFailure`
  - direct form, one `anyhow` wrapper, and multiple `anyhow` wrappers
  - assert the classifier chooses application failure
  - assert explicit `type_name`, retryability, retry delay, category, and raw details survive
    unchanged
- `ActivityExecutionError::Failed`
  - direct form, one `anyhow` wrapper, and multiple `anyhow` wrappers
  - assert the classifier chooses activity execution failure
  - assert the emitted top-level proto is the same activity failure shape that was wrapped
- `ActivityExecutionError::Cancelled`
  - same forms as above
  - assert the top-level proto remains a canceled activity failure, not an application failure
- `ActivityExecutionError::Serialization`
  - same forms as above
  - assert this degrades to application failure because it is a local serialization problem, not a
    server-produced activity failure proto
- `ChildWorkflowExecutionError::Failed`
  - same forms as above
  - assert the classifier chooses child-workflow failure
  - assert workflow id, run id, initiated event id, started event id, and retry state are preserved
- `ChildWorkflowExecutionError::Cancelled`
  - same forms as above
  - assert the top-level proto remains child-workflow-canceled
- `ChildWorkflowExecutionError::StartFailed`
  - same forms as above
  - assert the emitted proto follows the intended fallback policy for start-failed cases
- `ChildWorkflowExecutionError::Serialization`
  - same forms as above
  - assert application-failure fallback
- `ChildWorkflowSignalError::Failed`
  - same forms as above
  - assert the classifier chooses child-workflow-signal failure
- `ChildWorkflowSignalError::Serialization`
  - same forms as above
  - assert application-failure fallback

These tests are the main protection against accidentally forgetting to walk through `anyhow`
context wrappers.

## 2. Nested-failure tests

Where these tests live:

- primarily `crates/common/src/data_converters/failure_converter.rs`
- one representative end-to-end check should also live in
  `crates/sdk-core/tests/integ_tests/data_converter_tests.rs`

Execution mode:

- unit tests for classification and proto-shape assertions
- one live-worker integration test for the path that matters most in practice
- replay is not sufficient here because these tests are validating outbound encoding behavior, not
  inbound history interpretation

Purpose:

- prove that walking the error chain does not incorrectly flatten valid nested failures
- distinguish "context wrapper around a special error" from "application failure caused by a
  special error"

Required unit tests:

- `ApplicationFailure` caused by `ActivityExecutionError::Failed`
  - assert the top-level proto is still `ApplicationFailureInfo`
  - assert the top-level message/type/retry metadata come from the outer `ApplicationFailure`
  - assert category and raw details are preserved on the outer application failure
  - assert the nested `cause` is the activity failure proto
- `ApplicationFailure` caused by `ChildWorkflowExecutionError::Cancelled`
  - assert top-level application failure
  - assert nested `cause` is child-workflow-canceled
- `ApplicationFailure` caused by `ChildWorkflowSignalError::Failed`
  - assert top-level application failure
  - assert nested `cause` is the child-workflow-signal failure
- wrapper-chain control case
  - take `ActivityExecutionError::Failed` and wrap it only in `anyhow` context
  - assert this does become a top-level activity failure
  - this is the contrast case that proves the classifier is distinguishing wrappers from true
    nesting

Ruby-matching nested-failure test:

- add a unit test modeled after
  `/Users/olszewski/code/temporal/sdk/ruby/temporalio/test/converters/failure_converter_test.rb`
  `test_failure_with_causes`
- intended Rust shape:
  - top-level error is `ChildWorkflowExecutionError`
  - its cause is an `ApplicationFailure` with details
  - that cause's cause is another `ApplicationFailure` without details
  - that cause's cause is a plain custom error type
- assertions:
  - top-level emitted proto is child-workflow-execution failure
  - child-workflow metadata is preserved
- first nested cause is application failure with details intact
  - details here are asserted as raw payloads, not typed values
- second nested cause is application failure with no details
  - third nested cause is plain application failure derived from the custom error type
  - each level has the expected message

This test is important enough that it may justify adding helper constructors or source-plumbing to
the shared inbound error types after they move to `temporalio_common::error`. If the test cannot be
expressed naturally, that is a sign the error types are not exposing the nesting semantics the
converter needs.

## 3. Workflow/activity conversion-failure tests

Where these tests live:

- `crates/common/src/data_converters/failure_converter.rs`
- workflow/activity integration coverage in `crates/sdk-core/tests/integ_tests`

Execution mode:

- unit tests for fallback-failure construction
- live worker tests for workflow/activity call-site recovery

Purpose:

- prove that a fallible converter does not turn workflow/activity failure handling into an
  uncaught internal error

Required tests:

- fallback builder unit test
  - build the synthetic fallback from a converter error plus original error
  - assert it produces a plain application failure without needing payload conversion
  - assert the message includes both error strings
- workflow failure path
  - use a converter/payload setup that makes `to_failure(...)` fail while encoding raw application
    details or encoded attributes
  - assert the workflow path still completes with a fallback failure instead of surfacing an
    internal activation failure
- workflow panic path
  - same as above, but through the panic handling path
  - assert the same fallback policy is used
- activity failure path
  - use a converter/payload setup that makes `to_failure(...)` fail for an activity error
  - assert the activity still completes with a fallback failure
- activity panic path
  - same as above, through the panic handling path
  - assert the worker does not recursively invoke the converter on the converter error

## 4. Boundary integration tests

Where these tests live:

- `crates/sdk-core/tests/integ_tests/data_converter_tests.rs`
- add workflow-focused coverage in the existing workflow integration files if that gives cleaner
  setup

Execution mode:

- live worker tests
- not replay-only tests

Reason:

- these tests are validating that the actual SDK call sites invoke the configured failure converter
- replay cannot prove that the original outbound encoding path used the converter

Required tests and assertions:

- activity returns a normal user error
  - run with a custom/default converter configuration that makes converter usage visible
  - assert the activity failure written to history matches converter output
- activity returns a non-retryable `ApplicationFailure`
  - assert `ApplicationFailureInfo.non_retryable = true`
  - assert explicit type name, retry delay, category, and raw details survive
- activity panic
  - assert panic path also goes through the converter
  - assert the emitted failure is application-style with the panic text in the message chain
- workflow returns a normal user error
  - assert workflow-execution-failed event contains converter-produced application failure
- workflow propagates activity failure with `?`
  - assert top-level workflow failure is an activity failure, not rewrapped as application failure
- workflow propagates child workflow failure with `?`
  - assert top-level workflow failure is child-workflow-execution failure
- workflow panic
  - assert workflow panic path uses the converter

For all of these tests, assert the raw emitted `Failure` proto shape directly because failure
decoding is not part of this project.

Deferred follow-up:

- query failure
- update rejection
- update failure after acceptance
- client-side async activity explicit failure completion

## 5. Wrapped-special-error integration tests

Where these tests live:

- `crates/sdk-core/tests/integ_tests/data_converter_tests.rs`
- or in the workflow-focused integration file that already exercises the relevant surface if that
  leads to simpler setup

Execution mode:

- live worker tests
- not replay-only tests

Purpose:

- prove that real workflow code using `?` plus `anyhow` context still preserves special failure
  kinds at the top level

Required tests:

- workflow receives `ActivityExecutionError::Failed(...).context("outer")`
  - assert top-level workflow failure is still activity failure
  - assert outer context appears as a nested application-style cause only if the chosen encoding
    policy preserves it
- workflow receives `ChildWorkflowExecutionError::Failed(...).context("outer")`
  - assert top-level workflow failure is still child-workflow-execution failure
- workflow receives `ChildWorkflowSignalError::Failed(...).context("outer")`
  - assert top-level workflow failure is still child-workflow-signal failure

These tests should be paired with the nested-failure unit tests so both sides of the rule are
covered:

- pure context wrapping should preserve the special top-level category
- a real outer `ApplicationFailure` should remain the top-level category

## 6. Top-level source inventory

Maintain a documented list of the SDK call sites that intentionally invoke failure conversion.

That list should be reviewed whenever a new outbound failure-producing API surface is added. This is
the main protection against "new top-level error type added, classifier never updated."

## Practical Guidance For The Classifier

The classifier should follow these rules:

1. Prefer explicit SDK failure categories over generic erased errors.
2. Preserve `ApplicationFailure` when it is the top-level semantic failure.
3. Only unwrap through the explicit transparent-wrapper allowlist.
4. Do not let a recognized inner failure unexpectedly replace an outer application failure.
5. Let `to_failure(...)` return an error when payload conversion fails; recovery happens at the
   workflow/activity call sites defined above, not by recursively retrying conversion.
6. Keep the set of recognized top-level categories centralized in one module.

If this proves too subtle to implement reliably with today's error shapes, that is a stronger signal
for future decode work than it is a signal to force a large typed outbound refactor now.

## Recommended Execution Plan

1. Create `temporalio_common::error`, add `ApplicationFailure` there, re-export it from
   `temporalio_sdk`, implement it as a `bon` builder-style type with category/raw-details support,
   and reshape `ActivityError` around it.
2. Move the remaining shared execution/signal failure types into `temporalio_common::error` and
   re-export those types from `temporalio_sdk`.
3. Split `data_converters` so the failure converter lives in its own submodule.
4. Implement one internal classifier with an explicit transparent-wrapper allowlist and route all
   outbound failure encoding through it.
5. Add explicit local fallback handling for workflow and activity failure-completion paths when
   `to_failure(...)` returns an error.
6. Add exhaustive classifier tests and workflow/activity conversion-failure coverage for the
   in-scope seams.
7. Revisit query/update/client-side async failure-conversion recovery as follow-up work.
8. Revisit a typed outbound API only if the erased classifier proves too subtle or too fragile in
   practice.
