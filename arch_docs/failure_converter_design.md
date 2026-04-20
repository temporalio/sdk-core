# Failure Converter Design

This document consolidates the current encoding and decoding plans for the Rust SDK failure
converter.

It should be read alongside
[`rust_failure_conversion_constraints.md`](./rust_failure_conversion_constraints.md), which explains
why this problem is structurally harder in Rust than in the other SDKs.

## Scope

This document describes the intended architectural boundary for:

- outbound failure encoding
- inbound failure decoding
- SDK-owned versus user-extensible behavior
- normalized failure representation
- fallback policy

It starts from the current state of `olszewski/failure_converter_v3`, where:

- shared error types already live in `temporalio_common::error`
- `ApplicationFailure` already exists as a first-class shared type
- `DefaultFailureConverter` owns outbound encoding behavior in
  `crates/common/src/data_converters/failure_converter.rs`
- inbound decode now normalizes into `IncomingError`
- ordinary decode/encode call sites now go through `DataConverter::to_error(..., hint)` and
  `DataConverter::to_failure(...)`

## Current Implementation Status

The core converter boundary described in this document is now largely implemented.

What has landed:

- `FailureConverter::to_error(...) -> IncomingError`
- `DataConverter::to_error(..., hint)` for SDK-owned adaptation
- `DataConverter::to_failure(...)` as the ordinary encode entry point
- typed outbound encoding through `OutgoingError`
- richer inbound public error surfaces for:
  - `ActivityExecutionError`
  - `ChildWorkflowExecutionError`
  - `ChildWorkflowSignalError`
- retained proto + normalized-cause preservation on decoded error wrappers
- `WorkflowTermination` conversions that preserve typed SDK errors instead of erasing them through
  `anyhow`

What is still incomplete:

- broader integration coverage for the newer richer error shapes, especially child-workflow signal
  remote failure
- any additional public-field exposure on other incoming wrapper types beyond the first-cut
  activity/child-workflow work
- final doc cleanup once the richer error pass is fully finished

## Naming Convention In This Document

This document uses `Error` as the suffix for Rust-side error types and reserves `Failure` for the
Temporal protobuf transport object.

That means some of the design language here intentionally differs from current branch code. The
main intentional difference that remains is:

- current code still uses `ApplicationFailure`
- this document uses `ApplicationError` as the target conceptual name

Likewise:

- older plan terms like `NormalizedFailure` become `IncomingError`
- older plan terms like `OutboundFailure` become `OutgoingError`

This naming is preferred because it makes the Rust-side / proto-side boundary much easier to reason
about during implementation.

## Core Architecture

The converter should not be treated as a direct bridge between:

- arbitrary public SDK error/wrapper types
- raw proto `Failure`

The durable boundary should instead be:

- SDK-owned adaptation between caller-facing surfaces and a shared normalized failure model
- converter-owned translation between that normalized model and proto `Failure`

Conceptually:

- encode: caller-facing Rust surface -> normalized failure -> proto `Failure`
- decode: proto `Failure` -> normalized failure -> caller-facing Rust surface

This is the key design decision that makes the encode and decode sides coherent.

## Shared Principles

### 1. Normalization is the core step

The important architectural step is normalization, not the erased converter trait boundary.

If encode/decode skip normalization and go straight to ad hoc wrapper conversion, the SDK will keep
accumulating call-site-specific error massaging.

### 2. SDK wrapper semantics are SDK-owned

The SDK, not user-provided converter logic, should own:

- how `ActivityExecutionError`, `ChildWorkflowExecutionError`, and
  `ChildWorkflowSignalError` are constructed
- how cancellation-vs-failure wrapper semantics are interpreted
- which caller-facing surface is valid at a given call site
- mismatch fallback policy for SDK wrapper adaptation

### 3. User extensibility should narrow to normalization/application behavior

User-extensible behavior should primarily live at the normalization / application-failure layer:

- application failure metadata mapping
- details payload behavior
- failure attributes encoding/decoding where customization is allowed
- normalization behavior for user-originated errors into supported extension points of the
  normalized model

That last point should be read narrowly.

It does **not** mean users can define arbitrary new SDK-owned top-level failure kinds.

It means things such as:

- a user mapping their custom Rust error into `ApplicationError` through normal Rust conversion
  patterns, with specific retryability/type/category semantics
- decoding an application-style failure back into a richer user-controlled application error when a
  generic/application-facing decode path allows it
- using an explicit extension slot if the normalized model eventually defines one for
  user-originated failures

It does **not** mean:

- overriding what counts as `ActivityExecutionError`, `ChildWorkflowExecutionError`, etc.
- changing how SDK wrapper surfaces are adapted at ordinary call sites
- introducing arbitrary new top-level normalized variants that the SDK itself does not understand

User-provided converters should not be the authority over SDK wrapper adaptation at every call
site.

### 4. Decoded errors must retain the original proto

All decoded error types should retain the original `Failure` proto so callers can inspect fields
that the structured Rust surface does not model directly.

### 5. Custom Rust type round-tripping is an explicit non-goal

The SDK should not promise that arbitrary user-defined Rust error types round-trip as the same
concrete Rust type on decode.

The honest contract is:

- failure semantics may round-trip
- retry metadata/details may round-trip
- original proto data remains inspectable
- arbitrary concrete Rust type identity does not round-trip as a supported API guarantee

## Shared Types

### ApplicationError

`ApplicationError` remains the canonical user-authored application error type.

It should continue to carry:

- underlying source error
- type name
- non-retryable flag
- next retry delay
- category
- raw details payloads

For decode compatibility, it should also retain the original `Failure` proto.

### IncomingError

The converter logic should converge on a shared normalized failure model in `common`.

Conceptually:

```rust
pub enum IncomingError {
    Application(ApplicationError),
    Timeout(TimeoutError),
    Cancelled(CancelledError),
    Terminated(TerminatedError),
    Server(ServerError),
    ResetWorkflow(ResetWorkflowError),
    Activity(IncomingActivityError),
    ChildWorkflowExecution(IncomingChildWorkflowExecutionError),
    NexusOperationExecution(IncomingNexusOperationExecutionError),
    NexusHandler(IncomingNexusHandlerError),
}
```

The exact names and public exposure can change, but the architectural role should be stable:

- normalization by failure kind happens once in shared code
- encode and decode both build on that normalization boundary

### Typed outbound error surface

On the encode side, the SDK should expose an explicit typed outbound adaptation layer rather than
rely on arbitrary erased `Error` values forever.

Conceptually, this should look much closer to the `failure_encoder_v2` branch direction:

```rust
pub enum OutgoingError {
    Activity(OutgoingActivityError),
    Workflow(OutgoingWorkflowError),
    Query(ApplicationError),
    UpdateRejection(ApplicationError),
    UpdateFailure(ApplicationError),
}

pub enum OutgoingActivityError {
    Application(ApplicationError),
    Cancelled { details: Option<Payload> },
}
```

The exact shape can still evolve, but the important design point is that encode should operate on
typed outbound SDK surfaces that reflect real failure-producing seams.

This is preferable for the 1.0 design to a permanent `Box<dyn Error>` boundary because it:

- makes caller intent explicit
- moves wrapper adaptation into SDK-owned code
- reduces erased-chain guessing inside the converter
- aligns outbound seams with the same normalization boundary decode is moving toward

### Consistent wrapper access

Decoded failure types should expose raw proto access consistently through concrete methods such as:

```rust
impl SomeDecodedError {
    fn failure(&self) -> &Failure;
    fn cause(&self) -> Option<&IncomingError>;
}
```

An explicit shared public trait is not required unless the SDK later grows a real generic consumer
for these wrapper types.

## Execution-Critical Clarifications

The sections below describe the intended architecture. This section makes a few execution-critical
 choices explicit so implementation can proceed without repeatedly reopening the design.

### 1. Public call sites should go through `DataConverter`

Ordinary SDK call sites should not reach into `FailureConverter` directly except where they are
implementing `DataConverter` itself.

The intended public/internal call-site split is:

- encode call sites use `DataConverter::to_failure(..., OutgoingError)`
- decode call sites use `DataConverter::to_error(..., hint)`
- `FailureConverter` remains the customization hook behind `DataConverter`

This is true even though encode does not have the same hint/adaptation concerns as decode. The
top-level `DataConverter::to_failure(...)` entry point is still desirable for symmetry and for
keeping ordinary SDK call sites attached to the full data-converter surface rather than reaching
through to the failure converter component directly.

This matters because it keeps:

- hint selection SDK-owned
- wrapper adaptation SDK-owned
- end-user customization concentrated at the normalization/proto-translation boundary

### 2. Fallback behavior is split across three layers

To avoid ambiguity, fallback should be owned in three distinct places:

- normalization fallback:
  - `FailureConverter::to_error(...)` falls back to `IncomingError::Application(...)` when the
    inbound proto shape is malformed or otherwise cannot be normalized into a more specific shape
- adaptation fallback:
  - `FailureDecodeHint::mismatch_fallback(...)` handles the case where a normalized value does not
    fit the caller-facing surface requested by the hint
- encode-conversion fallback:
  - SDK call sites build the synthetic application failure directly if `to_failure(...)` itself
    returns `PayloadConversionError`

These must not be collapsed together. In particular:

- decode normalization fallback should not be delegated to caller code
- hint mismatch fallback should not require local wrapper repair work
- encode fallback should not recurse back through the configured converter

### 3. First-cut variant scope should follow existing SDK wrapper pressure

The first implementation does not need every normalized variant to be rich at once, but it does
need to cover the seams that already create the most wrapper pressure in the SDK.

The intended first-cut order is:

1. `ApplicationError`
2. activity-related failures
3. child-workflow-related failures
4. remaining specialized variants

Concretely, this means the first implementation should be able to support:

- `OutgoingError::Activity(...)`
- `OutgoingError::Workflow(...)`
- `ActivityExecutionDecodeHint`
- child-workflow decode/encode paths soon after activity paths

That keeps the design focused on the seams where the current SDK most obviously performs local
error reshaping.

### 4. Thin variants are acceptable, but the seam contract is not optional

For initial execution, a normalized non-application variant can be thin, but the seam itself must
still be explicit.

For example, an initial `IncomingActivityError` normalized type does not need a large accessor surface as
long as it at least provides:

- retained original `Failure`
- preserved normalized cause
- enough top-level identity for hints/adaptation to match on it

What is not acceptable is silently skipping the normalized seam and rebuilding wrapper semantics
from raw `Failure` values directly at call sites.

## Decode Design

### Decode flow

The inbound flow should be:

1. decode payload-bearing failure fields
2. use the configured `FailureConverter` to normalize the proto into `IncomingError`
3. have `DataConverter` adapt the normalized value to the caller-facing surface using a decode hint

### Decode hint

Decode adaptation should be driven by an explicit hint contract.

For the moment, the most useful way to describe that is as a trait:

```rust
trait FailureDecodeHint {
    type Output;

    fn adapt(
        self,
        normalized: IncomingError,
    ) -> Result<Self::Output, FailureAdaptError>;

    fn mismatch_fallback(
        self,
        failure: Failure,
        cause: Option<anyhow::Error>,
    ) -> Self::Output;
}
```

Required functionality:

- define the top-level output surface the caller expects
- adapt from `IncomingError`
- define mismatch fallback behavior
- never override proto classification

### Proposed decode signatures

The old shape is:

```rust
fn to_error(
    &self,
    failure: Failure,
    payload_converter: &PayloadConverter,
    context: &SerializationContextData,
) -> Result<Box<dyn std::error::Error>, PayloadConversionError>;
```

The trait-level normalization hook should become:

```rust
fn to_error(
    &self,
    failure: Failure,
    payload_converter: &PayloadConverter,
    context: &SerializationContextData,
) -> Result<IncomingError, PayloadConversionError>;
```

Then `DataConverter` should expose the caller-facing hinted adaptation entry point:

```rust
fn to_error<H: FailureDecodeHint>(
    &self,
    context: &SerializationContextData,
    failure: Failure,
    hint: H,
) -> Result<H::Output, PayloadConversionError>;
```

This is the intended split:

- `FailureConverter::to_error(...)` is the end-user configuration hook for failure normalization
- `DataConverter::to_error(..., hint)` is the SDK-facing adaptation entry point

That keeps the trait object-safe while still making the ordinary decode API hint-driven rather than
context-free erased decode.

### Decode fallback policy

If an inbound failure does not map cleanly onto an expected normalized special-case shape, decode
should fall back to `ApplicationError`, not to a dedicated raw-proto wrapper type.

That includes:

- missing `failure_info`
- malformed known failure kind
- future failure shapes the SDK does not yet understand structurally

The fallback must still retain the original `Failure` proto.

### Concrete decode usefulness test

The hint only helps if it eliminates post-`to_error(...)` massaging.

For example, an activity-result call site that knows it needs `ActivityExecutionError` should be
able to get that surface directly from the decode API, rather than:

1. calling `DataConverter::to_error(...)`
2. downcasting / inspecting the decoded value
3. rebuilding `ActivityExecutionError` locally

If that massaging still exists, the hint is just ceremony.

### Concrete activity decode example

`ActivityExecutionError` is the clearest example of why the explicit hint should remain a concrete
value rather than being replaced by an output-type-only API.

The workflow-side activity resolution path already knows whether it is handling:

- a failed activity resolution
- a cancelled activity resolution

That information does not come from the desired output type alone.

So the intended call shape should be explicit at the call site:

```rust
match status {
    activity_resolution::Status::Failed(f) => Err(
        data_converter.to_error(
            &SerializationContextData::Workflow,
            f.failure.unwrap_or_default(),
            ActivityExecutionDecodeHint { cancelled: false },
        )?
    ),
    activity_resolution::Status::Cancelled(c) => Err(
        data_converter.to_error(
            &SerializationContextData::Workflow,
            c.failure.unwrap_or_default(),
            ActivityExecutionDecodeHint { cancelled: true },
        )?
    ),
    // ...
}
```

The contract for this call pattern is:

- `FailureConverter::to_error(...)` normalizes the proto into `IncomingError`
- `DataConverter::to_error(..., ActivityExecutionDecodeHint { ... })` adapts that normalized value
  into `ActivityExecutionError`
- the call site does not manually inspect the decoded value and rebuild
  `ActivityExecutionError::{Failed, Cancelled}`

This example is important because it shows why the decode API should stay hint-driven.

If the API instead became:

```rust
data_converter.to_error::<ActivityExecutionError>(...)
```

the output type alone would not capture the required adaptation policy. The call site still knows
something important that must be passed explicitly: whether the resolution is in the failed or
cancelled path.

## Encode Design

### Encode flow

For a coherent 1.0 design, the outbound flow should be:

1. adapt caller-facing SDK error/wrapper surfaces into a typed outbound failure surface
2. normalize that outbound surface into normalized outbound failure categories
3. encode that normalized form into proto `Failure`

Conceptually:

- caller-facing SDK surface -> `OutgoingError` -> normalized outgoing error -> proto `Failure`

This is the place where the strongest `failure_encoder_v2` idea fits: the converter should not be
asked to infer the entire outbound seam from arbitrary erased errors when the SDK already knows
whether it is encoding:

- an activity failure
- a workflow failure
- a query failure
- an update rejection
- an update failure

That typed outbound surface is the encode-side counterpart to the decode hint: it makes call-site
adaptation explicit before proto translation happens.

If an erased-input classifier is still used during transition, it should be treated as a temporary
implementation detail rather than the intended steady-state architecture.

### Proposed `to_failure` signature

The old shape is:

```rust
fn to_failure(
    &self,
    error: Box<dyn std::error::Error>,
    payload_converter: &PayloadConverter,
    context: &SerializationContextData,
) -> Result<Failure, PayloadConversionError>;
```

The target shape should become:

```rust
fn to_failure(
    &self,
    failure: OutgoingError,
    payload_converter: &PayloadConverter,
    context: &SerializationContextData,
) -> Result<Failure, PayloadConversionError>;
```

That makes the encode-side API honest in the same way the decode-side hint makes decode honest:

- the caller identifies the outbound seam explicitly
- the SDK owns adaptation into `OutgoingError`
- the converter owns translation from `OutgoingError` / normalized outgoing error into proto
  `Failure`

If the implementation wants a convenience entry point on `DataConverter`, it should also expose a
typed helper in the same direction, conceptually:

```rust
fn to_failure(
    &self,
    context: &SerializationContextData,
    failure: OutgoingError,
) -> Result<Failure, PayloadConversionError>;
```

This `DataConverter::to_failure(...)` entry point should be treated as part of the intended public
shape, not just as an optional convenience wrapper. Unlike decode, it does not carry extra
adaptation policy beyond `OutgoingError`, but it still gives encode-side call sites the same
"go through `DataConverter`" structure as decode-side call sites.

### Concrete activity encode example

`ActivityError` is the clearest encode-side example of why the typed outbound surface is useful.

The activity worker path already knows whether an activity is finishing with:

- an application failure
- a cancellation failure
- explicit async completion control flow

So the intended call shape should be explicit there too:

```rust
match err {
    ActivityError::Application(app) => {
        let failure = data_converter.to_failure(
            &SerializationContextData::Activity,
            OutgoingError::Activity(OutgoingActivityError::Application(app)),
        )?;
        ActivityExecutionResult::fail(failure)
    }
    ActivityError::Cancelled { details } => {
        let failure = data_converter.to_failure(
            &SerializationContextData::Activity,
            OutgoingError::Activity(OutgoingActivityError::Cancelled { details }),
        )?;
        ActivityExecutionResult::cancel(failure)
    }
    ActivityError::WillCompleteAsync => ActivityExecutionResult::will_complete_async(),
}
```

The contract for this call pattern is:

- the activity worker adapts `ActivityError` into `OutgoingError::Activity(...)`
- the converter is not asked to infer the outbound seam from an arbitrary erased error
- `WillCompleteAsync` remains outside the failure converter because it is control flow, not failure
  encoding

### Centralized classification

All outbound encoding should go through one shared classification step before proto construction.

During transition, the first internal representation may still look like:

```rust
enum ClassifiedFailure {
    Application(ApplicationError),
    ActivityExecution(ActivityExecutionError),
    ChildWorkflowExecution(ChildWorkflowExecutionError),
    ChildWorkflowSignal(ChildWorkflowSignalError),
}
```

But it should be designed so it can evolve into the outbound counterpart of `IncomingError`
rather than remain a permanent encode-only artifact for the 1.0 design.

For 1.0, the preferred boundary is:

- call site constructs or reaches `OutgoingError`
- shared code normalizes `OutgoingError`
- converter encodes the normalized outbound form

### Wrapper-chain handling

The encode side still needs explicit chain-walking rules for erased errors.

However, that should not remain the primary 1.0 boundary. The 1.0 target should be to reduce how
much wrapper semantics the converter has to infer from arbitrary erased chains by pushing SDK-known
wrapper adaptation earlier.

### Encode fallback policy

Encoding remains fallible because details/attributes may require payload conversion.

When converter encoding fails, SDK call sites should use an explicit SDK-owned fallback policy:

- do not recurse back through the configured failure converter
- build a plain synthetic application failure directly
- include both converter error text and original error text

That keeps fallback behavior predictable and prevents broken custom converters from consuming the
original failure path entirely.

## Context-Aware Behavior

Both encode and decode should continue to receive `SerializationContextData` so behavior can vary by
workflow/activity/nexus context where needed.

This remains compatible with:

- failure attribute encoding/decoding
- confidentiality via `encoded_attributes`
- context-aware payload conversion or codec behavior

## Encoded Attributes

The design still needs first-class support for `encoded_attributes`.

The intended contract remains:

- common fields such as message/stack trace may be moved into `encoded_attributes`
- payload codecs can then encrypt/compress them
- decode should restore them before final normalization when possible

This is one of the clearest reasons failure conversion remains a distinct concern from ordinary
payload conversion.

## User Extensibility Boundaries

The intended split across the full design is:

- user-extensible:
  - application-failure-specific encode/decode behavior
  - normalization behavior through `FailureConverter::to_error(...) -> IncomingError`
  - failure attributes encode/decode behavior where customization is allowed
- SDK-owned:
  - caller-facing wrapper adaptation
  - decode hint selection at each call site
  - `DataConverter::to_error(..., hint)` as the ordinary adaptation API
  - mismatch fallback for caller-facing surfaces
  - interpretation of SDK wrapper semantics

Because the SDK controls all ordinary converter call sites, it also controls the adaptation
context. That is deliberate and necessary if SDK wrapper invariants are meant to remain trustworthy.

## Implementation Priorities

Before starting, it is worth being explicit about dependency order. The work should not begin by
editing random call sites. It should begin by establishing the shared seam types and only then
migrating callers.

## Implementation Readiness

The high-level design is sufficiently settled that additional broad architecture work is unlikely
to pay off. The remaining useful planning work is the work that removes execution ambiguity.

Before implementation begins in earnest, the following TODOs should be completed:

1. Write a current API inventory.

List every encode/decode call site that:

- constructs proto `Failure` values
- consumes proto `Failure` values
- manually wraps or reshapes decoded errors after converter calls

This should become the migration checklist and rollout order. The main goal is to avoid missing a
seam or reintroducing local error-massaging logic while the new boundary is being adopted.

### Initial API Inventory: Failure Construction

The encode-side seams are easier to enumerate first. The current construction inventory is:

#### 1. Converter-backed SDK construction seams

These are already the most natural first migration targets because they centralize outbound failure
construction in one helper.

- `crates/sdk/src/lib.rs`
  - activity task execution converts panics and `ActivityError::Application(...)` through
    `convert_failure_with_fallback(...)`
- `crates/sdk/src/workflow_future.rs`
  - workflow panic handling converts the synthetic panic `ApplicationError` through
    `convert_failure_with_fallback(...)`
  - workflow completion converts `WorkflowTermination::Failed(...)` through
    `convert_failure_with_fallback(...)`

These paths are already close to the intended `DataConverter::to_failure(..., OutgoingError)`
shape. They should be the first encode call sites migrated away from erased `Box<dyn Error>`
classification.

#### 2. Direct proto construction in SDK-facing crates

These paths still synthesize `Failure` directly and should be reviewed explicitly during encode
migration, because they currently bypass the intended typed outbound seam.

- `crates/common/src/error.rs`
  - `impl From<ApplicationFailure> for Failure` directly builds an application `Failure` proto and
    its cause chain
- `crates/sdk/src/workflow_future.rs`
  - query failure responses construct `Failure { message, ..Default::default() }` directly for
    missing handlers and handler errors
- `crates/sdk/src/workflows.rs`
  - `impl From<WorkflowError> for Failure` constructs a plain message-only `Failure`
- `crates/sdk/src/lib.rs`
  - workflow start/setup failures call `WorkflowActivationCompletion::fail(..., String::into(), ...)`
    and therefore still rely on direct `Failure` construction instead of the configured converter

These are not all equally urgent. The first implementation should prioritize the activity/workflow
execution paths above, then decide whether each remaining direct-construction case should:

- migrate onto `OutgoingError`
- remain an SDK-owned synthetic system failure outside the customizable converter boundary
- or be removed as obsolete compatibility glue

#### 3. Public async activity failure boundary

One public API should be reshaped as part of this work:

- `crates/client/src/async_activity_handle.rs`
  - `AsyncActivityHandle::fail(failure: Failure, ...)` should become an application-error-facing
    API instead of a raw-proto API

This should accept `ApplicationError` rather than `Failure`. That matches the intended model for
activity failure completion: async activities are expected to fail with application semantics, and
the SDK should remain responsible for turning that Rust-side error into proto `Failure` through the
configured converter path.

#### 4. Internal core-generated failures

There are many direct `Failure` constructions under `crates/sdk-core`, especially in workflow state
machines, validation paths, and test helpers.

These failures are not expected to go through the failure converter and should not be touched by
this migration. At the `sdk-core` layer, they are generally internal protocol/system failures, not
"user errors" in the language-SDK sense.

The first encode migration checklist should therefore focus on the SDK-owned user-error paths in
`crates/sdk`, `crates/common`, and the async-activity client surface, not on every internal
`Failure { ... }` literal in core.

2. Freeze the first-cut variant set and type sketches.

Be explicit about the initial shapes for:

- `IncomingError`
- `OutgoingError`
- `FailureDecodeHint`
- activity-related incoming/outgoing variants
- child-workflow-related incoming/outgoing variants
- `DataConverter::to_error(...)`
- `DataConverter::to_failure(...)`

This does not require every future variant to be designed up front. It does require the first-cut
variant and field boundaries to be fixed before implementation starts, so engineers are not
deciding struct layout and ownership boundaries ad hoc while changing call sites.

### First-cut variant set and type sketches

The first implementation does not need to model every Temporal failure kind as a rich dedicated
Rust type. It does need to freeze a stable first-cut set that covers the activity and child
workflow seams the rollout is centered around.

The proposed first-cut shapes are:

#### IncomingError

```rust
pub enum IncomingError {
    Application(ApplicationError),
    Timeout(TimeoutError),
    Cancelled(CancelledError),
    Terminated(TerminatedError),
    Server(ServerError),
    ResetWorkflow(ResetWorkflowError),
    Activity(IncomingActivityError),
    ChildWorkflowExecution(IncomingChildWorkflowExecutionError),
}
```

This first cut intentionally does **not** freeze dedicated variants yet for:

- child-workflow signal failures
- nexus failures
- any other specialized future SDK wrapper type not needed for the first migration pass

If one of those shapes is encountered before its richer variant exists, normalization should fall
back to `IncomingError::Application(...)` rather than inventing an unstable stopgap variant.

#### Shared incoming error structs

All first-cut incoming error structs should retain the original proto `Failure` and, where
applicable, the normalized decoded cause.

Conceptually:

```rust
pub struct TimeoutError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
}

pub struct CancelledError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
}

pub struct TerminatedError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
}

pub struct ServerError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
}

pub struct ResetWorkflowError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
}

pub struct IncomingActivityError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
}

pub struct IncomingChildWorkflowExecutionError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
}
```

The point of these first-cut structs is not to expose a rich accessor surface immediately. The
first implementation only needs them to provide:

- top-level normalized identity
- retained original proto access
- preserved normalized cause chains

Any additional typed field extraction can be added later without changing the architectural seam.

#### ApplicationError

`ApplicationError` should be the one first-cut type with a richer field surface from the start.

Conceptually:

```rust
pub struct ApplicationError {
    source: anyhow::Error,
    type_name: Option<String>,
    non_retryable: bool,
    next_retry_delay: Option<Duration>,
    category: ApplicationErrorCategory,
    details: Option<Payloads>,
    failure: Failure,
}
```

This keeps the existing application-specific retry/details semantics while making decode-side proto
retention explicit.

#### OutgoingError

```rust
pub enum OutgoingError {
    Activity(OutgoingActivityError),
    Workflow(OutgoingWorkflowError),
    Query(ApplicationError),
    UpdateRejection(ApplicationError),
    UpdateFailure(ApplicationError),
}
```

The first-cut outbound surface should be scoped to the real encode call sites that already exist
in `sdk`.

#### Shared outgoing error structs

```rust
pub enum OutgoingActivityError {
    Application(ApplicationError),
    Cancelled { details: Option<Payload> },
}

pub enum OutgoingWorkflowError {
    Application(ApplicationError),
    Cancelled,
    ContinueAsNew(Box<ContinueAsNewWorkflowExecution>),
}
```

The first cut does not need distinct outbound child-workflow variants immediately because child
workflow failures are primarily a decode/migration pressure first. If encode-side child-workflow
adaptation needs a dedicated outbound shape during implementation, it should be added explicitly
rather than inferred from erased errors.

#### FailureDecodeHint

The hint contract should remain explicit and value-carrying:

```rust
pub trait FailureDecodeHint {
    type Output;

    fn adapt(self, normalized: IncomingError) -> Result<Self::Output, FailureAdaptError>;

    fn mismatch_fallback(
        self,
        failure: Failure,
        cause: Option<anyhow::Error>,
    ) -> Self::Output;
}
```

The first implementation should freeze concrete hint types for the seams being migrated first.

#### ActivityExecutionDecodeHint

```rust
pub struct ActivityExecutionDecodeHint {
    pub cancelled: bool,
}

impl FailureDecodeHint for ActivityExecutionDecodeHint {
    type Output = ActivityExecutionError;
}
```

Adaptation rules:

- `cancelled: false` expects to produce `ActivityExecutionError::Failed(...)`
- `cancelled: true` expects to produce `ActivityExecutionError::Cancelled(...)`
- both cases should preserve the normalized/retained proto data inside the returned top-level error
- mismatch fallback should remain SDK-owned and should not require local caller repair work
- mismatch will produce `ActivityExecutionError::Failed(...)`

#### ChildWorkflowExecutionDecodeHint

```rust
pub enum ChildWorkflowExecutionDecodeHint {
    CompletionFailed,
    CompletionCancelled,
    StartCancelled,
}

impl FailureDecodeHint for ChildWorkflowExecutionDecodeHint {
    type Output = ChildWorkflowExecutionError;
}
```

This shape is slightly more explicit than a bool because the child-workflow call sites already have
multiple distinct resolution paths, and collapsing them into a single flag would make the hint
harder to read at the call site.

Adaptation rules:

- `CompletionFailed` produces `ChildWorkflowExecutionError::Failed(...)`
- `CompletionCancelled` produces `ChildWorkflowExecutionError::Cancelled(...)`
- `StartCancelled` also produces `ChildWorkflowExecutionError::Cancelled(...)`, but exists as a
  separate hint value because it comes from a different call-site seam and may need different
  fallback behavior later

#### FailureConverter signatures

The trait-level customizable boundary should freeze to:

```rust
fn to_error(
    &self,
    failure: Failure,
    payload_converter: &PayloadConverter,
    context: &SerializationContextData,
) -> Result<IncomingError, PayloadConversionError>;

fn to_failure(
    &self,
    failure: OutgoingError,
    payload_converter: &PayloadConverter,
    context: &SerializationContextData,
) -> Result<Failure, PayloadConversionError>;
```

This is the user-extensible normalization/proto-translation boundary.

#### DataConverter signatures

The ordinary SDK call-site boundary should freeze to:

```rust
fn to_error<H: FailureDecodeHint>(
    &self,
    context: &SerializationContextData,
    failure: Failure,
    hint: H,
) -> Result<H::Output, PayloadConversionError>;

fn to_failure(
    &self,
    context: &SerializationContextData,
    failure: OutgoingError,
) -> Result<Failure, PayloadConversionError>;
```

This split keeps:

- user customization on `FailureConverter`
- call-site adaptation on `DataConverter`
- encode/decode symmetry for ordinary SDK callers

#### Explicit first-pass deferrals

The following items should remain intentionally unfrozen in the first implementation pass:

- dedicated incoming child-workflow-signal variants
- dedicated incoming/outgoing nexus variants
- richer typed field accessors for non-application incoming structs
- typed helper convenience methods on `DataConverter` beyond the generic hint-based decode entry
  point

Those can be added later once the shared seam is proven on application, activity, and child
workflow execution paths.

3. Define the first-pass test matrix up front.

Before implementation, write down the required initial coverage for:

- normalization fallback when inbound proto decoding/classification fails
- hint mismatch fallback when `DataConverter::to_error(..., hint)` cannot adapt a normalized value
- activity failed vs cancelled decode paths
- child workflow failed vs cancelled/terminated decode paths
- custom failure converter behavior at the normalization boundary
- failure-converter encode/decode interaction with retained proto data and encoded attributes

The goal is to make the first implementation prove the architectural boundaries deliberately,
rather than only accumulating tests opportunistically while the code is changing.

### First-pass test matrix

The first-pass test matrix should prove the new seams in three layers:

- unit tests for normalization and adaptation behavior in `common`
- SDK/unit-style tests for call-site adaptation into wrapper types
- integration tests for end-to-end converter behavior on real workflow/activity paths

The initial matrix should be:

#### 1. Inbound normalization tests

These tests should primarily live near `FailureConverter` / `IncomingError` in `common`.

- `ApplicationFailureInfo` normalizes to `IncomingError::Application(...)`
  - preserves message, type name, non-retryable, retry delay, category, details, and raw proto
- malformed or missing `ApplicationFailureInfo` fields fall back to `IncomingError::Application(...)`
  - raw proto is still retained
- known non-application failure infos normalize to the expected first-cut incoming variant
  - timeout -> `IncomingError::Timeout(...)`
  - cancelled -> `IncomingError::Cancelled(...)`
  - terminated -> `IncomingError::Terminated(...)`
  - server -> `IncomingError::Server(...)`
  - reset-workflow -> `IncomingError::ResetWorkflow(...)`
  - activity -> `IncomingError::Activity(...)`
  - child-workflow -> `IncomingError::ChildWorkflowExecution(...)`
- unimplemented or future failure infos fall back to `IncomingError::Application(...)`
- nested causes normalize recursively
  - the top-level normalized variant is correct
  - nested `cause: Option<Box<IncomingError>>` chain is preserved
  - each normalized node retains its original proto

#### 2. Decode-hint adaptation tests

These tests should primarily live around `DataConverter::to_error(..., hint)`.

- `ActivityExecutionDecodeHint { cancelled: false }`
  - adapts `IncomingError::Activity(...)` into `ActivityExecutionError::Failed(...)`
  - preserves raw proto data through the returned wrapper
- `ActivityExecutionDecodeHint { cancelled: true }`
  - adapts `IncomingError::Activity(...)` into `ActivityExecutionError::Cancelled(...)`
  - preserves raw proto data through the returned wrapper
- `ChildWorkflowExecutionDecodeHint::CompletionFailed`
  - adapts `IncomingError::ChildWorkflowExecution(...)` into `ChildWorkflowExecutionError::Failed(...)`
- `ChildWorkflowExecutionDecodeHint::CompletionCancelled`
  - adapts `IncomingError::ChildWorkflowExecution(...)` into `ChildWorkflowExecutionError::Cancelled(...)`
- `ChildWorkflowExecutionDecodeHint::StartCancelled`
  - adapts the incoming child-workflow shape into `ChildWorkflowExecutionError::Cancelled(...)`

These tests should also explicitly cover mismatch fallback:

- activity hint applied to a non-activity normalized value
  - returns the SDK-owned fallback surface without local caller repair work
- child-workflow hint applied to a non-child-workflow normalized value
  - returns the SDK-owned fallback surface without local caller repair work
- mismatch fallback preserves the original proto

#### 3. Outbound normalization / encoding tests

These tests should primarily live near `FailureConverter::to_failure(...)` and `OutgoingError`.

- `OutgoingError::Activity(OutgoingActivityError::Application(...))`
  - encodes to an activity/application proto shape with preserved application semantics
- `OutgoingError::Activity(OutgoingActivityError::Cancelled { details })`
  - encodes to the expected cancelled activity failure shape
- `OutgoingError::Workflow(OutgoingWorkflowError::Application(...))`
  - encodes to the expected workflow-failure proto shape
- `OutgoingError::Query(...)`
  - encodes to an application-style failure with the expected message/details semantics
- `OutgoingError::UpdateRejection(...)`
  - encodes to an application-style failure with the expected message/details semantics
- `OutgoingError::UpdateFailure(...)`
  - encodes to an application-style failure with the expected message/details semantics
- nested outgoing application causes are preserved in the encoded proto chain

#### 4. Encode fallback tests

These tests should prove the explicit SDK-owned fallback path when encoding itself fails.

- if `FailureConverter::to_failure(...)` returns `PayloadConversionError`, the SDK path that called
  `DataConverter::to_failure(...)` emits the synthetic application fallback failure
- the fallback contains both:
  - the converter failure message
  - the original error message/context that the SDK was attempting to encode
- fallback does not recurse back through the configured failure converter

These cases should include at least:

- workflow failure completion path
- activity failure completion path
- activity panic path

#### 5. Retained-proto tests

These tests should prove the retained-proto contract directly.

- every first-cut `IncomingError` variant exposes the original `Failure`
- `ApplicationError` exposes the original `Failure`
- top-level decoded wrapper types produced by hints still allow access to the original underlying
  proto data they were adapted from
- retained proto data is preserved across nested causes

#### 6. Encoded-attributes / payload-codec tests

These tests should verify that the new design still supports failure-attribute confidentiality.

- message/stack-trace encoded-attributes handling still runs through the expected converter path
- retained proto access remains valid even when common failure attributes were moved into
  `encoded_attributes`
- decode reconstructs the normalized/application shape correctly after encoded-attribute decoding

#### 7. Custom failure converter boundary tests

These tests should prove the new user-extensibility boundary explicitly.

- a custom `FailureConverter::to_error(...) -> IncomingError` implementation can influence
  normalization
- the SDK still owns hint selection and caller-facing adaptation
- a custom converter cannot replace an activity/child-workflow call site's top-level wrapper shape
- a custom `FailureConverter::to_failure(...)` implementation can influence outbound proto
  translation from `OutgoingError`

#### 8. End-to-end integration tests

These tests should live in the integration suite and exercise real workflow/activity paths.

- workflow application failure round-trips through the new encode/decode path
- activity application failure round-trips through the new encode/decode path
- activity cancellation path decodes through `ActivityExecutionDecodeHint { cancelled: true }`
- child workflow failure path decodes through `ChildWorkflowExecutionDecodeHint::CompletionFailed`
- child workflow cancellation path decodes through
  `ChildWorkflowExecutionDecodeHint::CompletionCancelled`
- failing custom failure converter still triggers the SDK fallback behavior already covered by the
  current integration tests

The first implementation does not need every one of these as a separate test function, but it
should not be considered complete until each matrix row is deliberately covered by either a unit
test or an integration test.

### Observed workflow-side error expectations from current integration tests

The current workflow integration tests are a useful guide for the first structured error shapes,
because they show what workflow code already expects to inspect on decode.

#### Activity execution

Observed expectations from `crates/sdk-core/tests/integ_tests/workflow_tests/activities.rs`,
`crates/sdk-core/tests/integ_tests/workflow_tests/local_activities.rs`,
`crates/sdk-core/tests/integ_tests/async_activity_client_tests.rs`, and
`crates/sdk-core/tests/integ_tests/heartbeat_tests.rs`:

- workflow code first distinguishes `ActivityExecutionError::Failed(...)` vs
  `ActivityExecutionError::Cancelled(...)`
- timeout cases are expected to be discoverable from the decoded activity failure
  - today this is often done with `is_timeout()`
- tests frequently inspect the top-level wrapped proto kind
  - often `ActivityFailureInfo`
- tests frequently inspect the cause proto kind
  - for example timeout or cancelled causes
- some tests inspect a small amount of nested message data on the cause
  - for example async activity failure message text

This suggests the first structured activity decode shape should preserve:

- the top-level failed/cancelled wrapper distinction
- easy access to the normalized cause
- easy access to timeout classification
- raw proto access for the top-level activity failure and its cause

It does **not** suggest that the first cut needs a rich typed accessor for every field inside
`ActivityFailureInfo`. Most current workflow-side expectations are about wrapper kind, cause kind,
and timeout classification rather than every transport field.

#### Child workflow execution

Observed expectations from `crates/sdk-core/tests/integ_tests/workflow_tests/child_workflows.rs`:

- workflow code first distinguishes:
  - `ChildWorkflowExecutionError::Failed(...)`
  - `ChildWorkflowExecutionError::Cancelled(...)`
  - `ChildWorkflowExecutionError::StartFailed { .. }`
  - `ChildWorkflowExecutionError::Serialization(...)`
- the `StartFailed` variant already carries structured fields that workflow code cares about
  directly:
  - `workflow_id`
  - `workflow_type`
  - `cause`
- ordinary completion failure/cancellation tests mostly care about wrapper discrimination rather
  than deep proto inspection

This suggests the first structured child-workflow decode shape should preserve:

- the top-level failed/cancelled/start-failed distinction
- dedicated structured data for start failure
- raw proto access for completion failure/cancellation cases

It does **not** suggest that the first cut needs a rich dedicated field surface for every
child-workflow failure-info field beyond what the existing wrappers already expose.

#### Child workflow signaling

Observed expectations from `crates/sdk-core/tests/integ_tests/workflow_tests/child_workflows.rs`:

- current workflow-side tests appear to exercise `ChildWorkflowSignalError::Serialization(...)`
  explicitly
- there is not yet comparable workflow-side pressure for a richer decoded
  `ChildWorkflowSignalError::Failed(...)` shape

This supports the current first-pass deferral:

- child-workflow signal failure should not drive the initial normalized variant set
- if needed later, it can be added once there is real wrapper pressure beyond serialization

#### Design implication

Taken together, the workflow integration tests support a conservative first structured-error shape:

- keep wrapper discrimination first-class
- preserve normalized cause chains
- provide a small number of high-value convenience queries where workflow code already relies on
  them
  - activity timeout classification is the clearest example
- retain raw proto access everywhere so existing low-level inspections remain possible during the
  migration period

This is a strong signal against trying to fully model every failure-info field up front. The
current workflow-side expectations are real, but narrower than the full proto surface.

### Execution order

The implementation sequence should be:

1. finalize shared types and signatures
2. implement shared normalization/proto translation
3. wire `DataConverter` entry points
4. migrate activity paths
5. migrate child-workflow paths
6. migrate remaining seams

That order matters because activity and child workflow paths are the places where the new
boundaries will be validated most quickly.

### Phase 1

Establish the shared public error surface:

- keep `ApplicationError` as the canonical user-authored application type
- keep shared SDK error definitions in `common`
- keep public imports stable through `sdk` re-exports
- shape activity/workflow-facing failure types so they can feed a typed outbound failure surface

### Phase 2

Establish the normalization boundary:

- add `OutgoingError` or an equivalent typed outbound adaptation layer
- add `IncomingError`
- add retained raw-proto access
- make the existing classifier compatible with normalized outbound/inbound flow
- keep outbound typed seams aligned with real SDK failure-producing paths
- decide the first-cut normalized variants needed for activity and child-workflow execution paths

### Phase 3

Implement decode on top of normalization:

- implement `FailureConverter::to_error(...) -> IncomingError`
- implement `DataConverter::to_error(..., hint)`
- add typed helper entry points on `DataConverter` where that improves call-site ergonomics
- prove the design on `ActivityExecutionError` before broadening to other wrappers

### Phase 4

Make encode explicitly match the same boundary:

- adapt SDK wrapper surfaces into `OutgoingError`
- normalize `OutgoingError` into normalized outgoing error categories
- keep proto translation centralized in the converter
- keep fallback policy SDK-owned
- do not treat direct erased-error classification as the desired end state for 1.0
- migrate the activity worker path first, then child-workflow-related encode seams

### Phase 5

Extend coverage and ergonomics:

- route all intended failure-producing/consuming seams through the shared design
- expand tests for mismatch fallback, nested causes, and encoded attributes
- expand normalized variant coverage once the shared boundary is in place

## Resolved Decisions

1. `IncomingError` needs to be public from `common` so end users can implement their own
   `FailureConverter` implementations against the shared normalization model.
2. Non-application normalized variants may be implemented incrementally, one variant at a time, as
   long as the shared normalization boundary, retained-proto contract, and fallback rules are fixed
   first.
3. The most straightforward encode-side wrapper adaptation to move out of erased classification
   first is activity failure handling, followed by child workflow handling.

## Incremental Variant Rollout

Implementing non-application normalized variants one by one is a good fit for this design.

The important constraint is that incremental rollout should happen on top of already-settled shared
invariants:

- `FailureConverter::to_error(...)` returns `IncomingError`
- decoded failures retain the original proto
- `DataConverter::to_error(..., hint)` owns caller-facing adaptation
- unmatched or malformed shapes fall back consistently

With those invariants fixed, the SDK can add richer normalized variants incrementally without
reopening the architecture each time.

That means a sensible order is:

1. `ApplicationError`
2. activity-related failures
3. child-workflow-related failures
4. remaining specialized variants
