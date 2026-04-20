# Richer Failure Error Shapes

This document sketches a first-pass richer public error surface for the Rust SDK errors that are
currently mostly wrappers around raw Temporal `Failure` protos.

It should be read alongside [`failure_converter_design.md`](./failure_converter_design.md).

## Goal

The current public workflow-side error types still expose too much proto shape directly.

For example, `ActivityExecutionError::is_timeout()` is a sign that callers are being asked to
recover semantic meaning from a generic failure wrapper after decode.

The goal of this note is to define a conservative first pass where:

- top-level public error enums reflect semantic outcome categories directly
- decoded errors retain the original `Failure` proto
- decoded errors preserve normalized causes
- the SDK can remove helper APIs like `is_timeout()` in favor of more honest type structure

This note is intentionally limited to the public inbound error surface. It does not propose a new
converter architecture; it builds on the converter design already captured in
[`failure_converter_design.md`](./failure_converter_design.md).

## Design Principles

### 1. Public enums should distinguish meaningful outcome classes

If callers routinely need to inspect `failure()` or ask boolean helper questions like
`is_timeout()`, that usually means the public enum is still too raw.

A richer public error should distinguish categories such as:

- failure
- timeout
- cancellation
- termination
- serialization failure

at the type level where that distinction is already available from decode context plus normalized
cause.

### 2. Retained proto remains mandatory

Every decoded error type proposed here must still retain the original `Failure` proto so callers
can inspect fields the first-pass Rust shape does not yet model directly.

This applies to:

- `ApplicationFailure`
- thin failure wrapper structs such as `ActivityFailureError`
- timeout/cancelled/terminated wrappers

### 3. Normalized cause remains part of the public shape

Every decoded wrapper struct should preserve:

- `failure: Failure`
- `cause: Option<Box<IncomingError>>`

The richer public enums should therefore be understood as the outer semantic surface, not as a
replacement for normalized cause access.

Where a decoded public error type implements `std::error::Error`, this preserved normalized
`cause` should also be the natural basis for `source()`.

That means the richer public shape should aim to align:

- retained normalized cause for structured inspection
- `std::error::Error::source()` for ordinary Rust error-chain traversal

This is important because one of the main benefits of making the error surface richer is letting
callers use normal Rust error handling patterns instead of immediately dropping down to raw proto
inspection.

### 4. First pass should focus on semantic outcomes, not full proto field accessor coverage

The first pass does not need to expose every field from `ActivityFailureInfo`,
`ChildWorkflowExecutionFailureInfo`, etc. as first-class accessors.

It is sufficient for the initial richer shapes to provide:

- semantic outcome variants
- retained original proto
- preserved normalized cause

Callers can still inspect wrapper-specific proto fields through `failure()` until the need for more
typed accessors is clearer.

## Shared Method Convention

Decoded wrappers should expose a consistent concrete method shape for proto/cause access:

```rust
impl SomeDecodedError {
    fn failure(&self) -> &Failure;
    fn cause(&self) -> Option<&IncomingError>;
}
```

Wrapper-like public enums whose top-level shape represents the operation rather than the underlying
reason should also expose a convenience reason accessor:

```rust
impl SomeOperationError {
    fn reason(&self) -> Option<&IncomingError>;
}
```

For the first pass, `reason()` should be understood as a convenience wrapper over the preserved
normalized cause chain:

- `ActivityExecutionError::Failed(...)` returns the nested reason from the wrapped
  `ActivityFailureError`
- `ChildWorkflowExecutionError::Failed(...)` returns the nested reason from the wrapped
  `ChildWorkflowFailureError`
- top-level `Cancelled(...)` or `StartFailed { .. }` cases may return `None`, because their meaning
  is already expressed by the top-level variant

This keeps the public shape cross-SDK-aligned while still avoiding clumsy repeated `cause()`
inspection in normal Rust code.

This convention is intended for decoded error wrapper structs such as:

- `ApplicationFailure`
- `ActivityFailureError`
- `TimeoutError`
- `CancelledError`
- `TerminatedError`
- `ServerError`
- `ResetWorkflowError`
- `ChildWorkflowFailureError`
- `ChildWorkflowSignalFailureError`
- Nexus wrapper types

An explicit shared public trait is not required for the first pass.

The trait only becomes worthwhile if the SDK later grows a real generic consumer, such as:

- shared logging/formatting helpers over multiple decoded wrapper types
- public APIs that intentionally accept any decoded failure-backed type

Until such a use case exists, consistent concrete methods are a simpler and more idiomatic choice.

## Proposed First-Pass Shapes

### Remote activity execution

Current state:

- `ActivityExecutionError::Failed(Box<Failure>)`
- `ActivityExecutionError::Cancelled(Box<Failure>)`
- `ActivityExecutionError::Serialization(PayloadConversionError)`
- helper `is_timeout()` inspects the wrapped proto

Proposed first pass:

```rust
pub enum ActivityExecutionError {
    Failed(ActivityFailureError),
    Cancelled(CancelledError),
    Serialization(PayloadConversionError),
}

pub struct ActivityFailureError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
}
```

Intended behavior:

- cancelled activity resolution maps directly to `ActivityExecutionError::Cancelled(...)`
- failed activity resolution maps to `ActivityExecutionError::Failed(...)`
- the actual reason for the failed activity remains in `ActivityFailureError::cause()`, e.g.:
  - `IncomingError::Application(...)`
  - `IncomingError::Timeout(...)`
  - `IncomingError::Cancelled(...)`

This keeps Rust aligned with the cross-SDK rule that remote activity waits first identify that the
failed thing was an activity, while `cause()` identifies whether the reason was application
failure, timeout, or cancellation. To keep that shape ergonomic, `ActivityExecutionError` should
also expose `reason()` as a convenience accessor for the nested failure reason.

### Local activity execution

Local activities are different because there is no remote wrapper layer to preserve.

The first-pass richer local-activity rule should therefore remain:

- application failure -> `ApplicationFailure`
- timeout -> `TimeoutError`
- cancellation -> `CancelledError`

This document does not propose a separate local-activity wrapper type.

### Child workflow execution

Current state:

- `ChildWorkflowExecutionError::Failed(Box<Failure>)`
- `ChildWorkflowExecutionError::Cancelled(Box<Failure>)`
- `StartFailed { ... }`
- `Serialization(PayloadConversionError)`

Proposed first pass:

```rust
pub enum ChildWorkflowExecutionError {
    Failed(ChildWorkflowFailureError),
    Cancelled(CancelledError),
    StartFailed {
        workflow_id: String,
        workflow_type: String,
        cause: StartChildWorkflowExecutionFailedCause,
    },
    Serialization(PayloadConversionError),
}

pub struct ChildWorkflowFailureError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
}
```

This keeps `StartFailed` as-is because it is already a semantic SDK-level error shape, while making
other child-workflow outcomes more explicit.

Intended behavior:

- cancelled child workflow resolution maps to `Cancelled(...)`
- failed child workflow resolution maps to `Failed(...)`
- the actual reason for the failed child workflow remains in `ChildWorkflowFailureError::cause()`,
  e.g.:
  - `IncomingError::Application(...)`
  - `IncomingError::Timeout(...)`
  - `IncomingError::Cancelled(...)`
  - `IncomingError::Terminated(...)`

This keeps Rust aligned with the cross-SDK rule that the child-workflow wrapper identifies what
operation failed, while the nested cause identifies why it failed. To keep that shape ergonomic,
`ChildWorkflowExecutionError` should also expose `reason()` as a convenience accessor for the
nested failure reason.

### Child workflow signal

Current state:

- `ChildWorkflowSignalError::Failed(Box<Failure>)`
- `Serialization(PayloadConversionError)`

Proposed first pass:

```rust
pub enum ChildWorkflowSignalError {
    Failed(ChildWorkflowSignalFailureError),
    Serialization(PayloadConversionError),
}

pub struct ChildWorkflowSignalFailureError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
}
```

This is less urgent than activity or child-workflow execution, but it follows the same general
shape.

## Decode Mapping Principle

The intended mapping rule is:

- call-site context still determines the outer error family
- normalized cause determines the nested reason within that family

For example, activity decode should behave as:

1. resolve status says cancelled:
   return `ActivityExecutionError::Cancelled(...)`
2. resolve status says failed:
   return `ActivityExecutionError::Failed(...)`

In that failed case, the nested reason remains visible through `cause()` / `source()`.

For caller ergonomics, wrapper-like public enums should expose that same nested reason through a
`reason()` convenience accessor, so ordinary code does not need to walk the full cause chain just
to answer common questions such as "was this timeout-shaped?".

This means:

- `FailureConverter::to_error(...)` still owns proto normalization
- `DataConverter::to_error(..., hint)` still owns caller-surface adaptation
- richer public enums are an adaptation-layer change, not a converter-architecture change

## First-Pass Adaptation Tables

This section is intended to remove ambiguity during implementation by stating the expected
caller-surface adaptation rules explicitly.

### Activity execution adaptation

Decode input:

- workflow-side activity resolution status
- normalized decoded value from `FailureConverter::to_error(...)`

Expected first-pass mapping:

- cancelled resolution + any normalized value
  -> `ActivityExecutionError::Cancelled(...)`
- failed resolution + normalized activity wrapper whose cause is anything else
  -> `ActivityExecutionError::Failed(...)`
- failed resolution + normalized value that is not `IncomingError::Activity(...)`
  -> `ActivityExecutionError::Failed(...)` using retained proto fallback

This mapping removes the need for `ActivityExecutionError::is_timeout()` without inventing a
timeout-specific top-level wrapper variant for remote activities.

More specifically, a failed activity resolution whose normalized cause is
`IncomingError::Timeout(...)`, `IncomingError::Cancelled(...)`, or `IncomingError::Application(...)`
should all remain `ActivityExecutionError::Failed(...)` in the first pass.

The intended rule is:

- resolution status determines the top-level public variant
- failed remote activity resolutions remain wrapper-shaped
- nested cause remains visible through `cause()` / `source()`
- nested cause does not by itself promote a failed resolution into a different top-level variant

### Child workflow execution adaptation

Decode input:

- workflow-side child-workflow resolution status
- normalized decoded value from `FailureConverter::to_error(...)`

Expected first-pass mapping:

- cancelled resolution + any normalized value
  -> `ChildWorkflowExecutionError::Cancelled(...)`
- failed resolution + normalized child-workflow wrapper whose cause is anything else
  -> `ChildWorkflowExecutionError::Failed(...)`
- failed resolution + normalized value that is not `IncomingError::ChildWorkflowExecution(...)`
  -> `ChildWorkflowExecutionError::Failed(...)` using retained proto fallback
- start-failed resolution remains `StartFailed { ... }` and does not participate in `IncomingError`
  adaptation

More specifically, a failed child-workflow resolution whose normalized cause is
`IncomingError::Cancelled(...)` should still remain `ChildWorkflowExecutionError::Failed(...)` in
the first pass.

The intended rule is:

- resolution status determines the top-level public variant
- failed child-workflow resolutions remain wrapper-shaped
- nested cause remains visible through `cause()` / `source()`
- nested cancelled cause does not by itself promote a failed resolution into `Cancelled(...)`

This keeps the richer Rust shape aligned with the cross-SDK behavior where a top-level
child-workflow failure with a nested cancelled cause is still treated as a child-workflow failure,
while higher-level cancellation-sensitive logic can inspect the cause chain when needed.

### Child workflow signal adaptation

Decode input:

- workflow-side child-workflow signal result
- normalized decoded value from `FailureConverter::to_error(...)`

Expected first-pass mapping:

- all other decoded failures
  -> `ChildWorkflowSignalError::Failed(...)`

For the first pass, child-workflow signal should remain:

- `Failed(...)`
- `Serialization(...)`

That is, signal failure does not need a separate public `Cancelled(...)` variant in the first pass.

## Source Behavior

Where these richer public types implement `std::error::Error`, `source()` should follow the same
semantic structure as the public variant/cause model rather than falling back to raw proto-only
inspection.

Expected first-pass rule:

- wrapper structs such as `ActivityFailureError`, `ChildWorkflowFailureError`, and
  `ChildWorkflowSignalFailureError` should return their preserved normalized `cause()` from
  `source()` when a Rust `Error` object can be exposed for that cause
- `TimeoutError`, `CancelledError`, `TerminatedError`, `ServerError`, and `ResetWorkflowError`
  should likewise expose their normalized cause through `source()` where applicable
- enum types such as `ActivityExecutionError` and `ChildWorkflowExecutionError` should delegate
  `source()` to the contained richer wrapper or serialization error as appropriate
- those same enums should expose `reason()` as the convenient structured accessor for the nested
  `IncomingError` reason when the top-level variant is wrapper-shaped

`IncomingError` itself should implement `std::error::Error`.

That keeps cause chaining straightforward because richer public wrapper types can expose their
normalized `cause()` through `source()` without inventing a second parallel trait hierarchy just for
error-chain traversal.

## Explicit Non-Goals For This Pass

This note does not propose:

- full typed accessor coverage for all wrapper-specific proto fields
- immediate redesign of every existing public SDK error type
- changing Nexus public caller-facing error types yet
- removing retained proto access

The first pass is specifically about replacing generic raw-failure wrapper enums with richer,
semantic public shapes where the SDK already has enough information to do so honestly.

## Testing Expectations

Changing these public error shapes will require both unit-test and integration-test updates.

### 1. Existing integration assertions will need to become more semantic

Many current tests assert against raw `Failure` wrappers or helper methods such as `is_timeout()`.

Those assertions should be updated to assert against the richer public variants directly.

For example:

- remote activity timeout tests should assert `ActivityExecutionError::Failed(...)` with
  `reason()`/`source()` exposing `TimeoutError`
- activity cancellation tests should assert `ActivityExecutionError::Cancelled(...)`
- generic activity failure tests should assert `ActivityExecutionError::Failed(...)`
- child workflow timeout / terminated tests should assert `ChildWorkflowExecutionError::Failed(...)`
  with the corresponding nested `reason()`

The important point is that integration tests should stop re-deriving semantics from the retained
proto where the richer public type now exposes those semantics directly.

### 2. Retained proto access should still be verified

Even after integration tests move to richer semantic assertions, at least one test per richer error
family should still verify:

- the decoded public error retains the original `Failure` proto
- the decoded public error preserves the normalized cause

This matters because the richer type shape is not intended to replace retained proto inspection;
it is intended to make the common semantic cases easier while still preserving the transport object.

### 3. Round-trip support for standard SDK error types should be verified

The richer public shapes should not be decode-only conveniences.

Where the SDK already has a standard error type such as:

- `ActivityExecutionError`
- `ChildWorkflowExecutionError`
- `ChildWorkflowSignalError`

there should be integration coverage proving that these errors still round-trip through workflow
execution in the expected standard form.

For example, when one of these standard SDK errors is used as a workflow failure:

- outbound encoding should preserve the corresponding Temporal failure semantics
- inbound decoding should reconstruct the richer public error variant rather than collapsing back to
  a generic raw `Failure` wrapper

This does not mean arbitrary custom Rust type identity should round-trip. The point is narrower:
the SDK's own standard error types should continue to round-trip in a stable and ergonomic way.

### 4. First-pass recommended coverage

The minimum useful first-pass coverage should include:

- unit tests for decode hint adaptation into the richer variants
- unit tests for `source()` / preserved cause behavior on the richer wrappers
- integration tests for activity timeout / cancellation / generic failure outcomes
- integration tests for child workflow timeout / termination / cancellation / generic failure
  outcomes
- integration tests showing that workflow query/update failure assertions continue to observe the
  expected failure semantics after the richer public shapes land
- integration tests showing that standard SDK errors used in workflow code still round-trip through
  encode/decode as the expected richer public variants

## Compatibility And Migration

The first implementation of richer public error shapes should assume that some existing tests and
call sites will need straightforward migration.

Expected compatibility changes:

- `ActivityExecutionError::is_timeout()` should be removed or deprecated once timeout becomes a
  first-class nested `cause()`/`source()` inspection path rather than a raw-proto helper
- existing pattern matches on raw `Box<Failure>` enum payloads will need to move to semantic
  variant matches first, with `failure()` only used when retained proto inspection is still needed
- integration assertions that currently inspect `failure_info` directly for common semantic cases
  should prefer the richer public variant shape instead
- retained `failure()` access remains available, so tests that need exact proto inspection can
  still do so without losing coverage

The intended migration style is:

1. first match on the richer semantic variant
2. then inspect `failure()` only for fields that are not yet modeled directly
3. use `reason()` for ordinary failure-reason inspection on wrapper-like public enums
4. use `cause()` / `source()` for ordinary Rust error-chain traversal rather than re-parsing raw
   nested proto causes whenever possible

## Recommended Implementation Order

The most conservative rollout order is:

1. `ActivityExecutionError`
2. `ChildWorkflowExecutionError`
3. `ChildWorkflowSignalError`
4. any further specialized decode surfaces that need similar treatment

This order is recommended because:

- activity timeout is the clearest current ergonomics gap
- child-workflow execution already has meaningful semantic distinctions such as `StartFailed`
- signal failure is a smaller follow-up once the pattern is proven
