# Failure Payloads

## Summary

Payload-backed metadata on Rust error surfaces is currently exposed as raw proto
`Payload` / `Payloads` values. That leaks transport concerns into the public API:

- users must manually encode application failure details before constructing an error
- users must manually decode raw payloads when reading failure details back
- the API shape differs across otherwise similar failure surfaces

This document proposes a shared decoded-payload abstraction together with a failure-specific
outbound wrapper that:

- accepts any `TemporalSerializable` for outbound error payload fields
- stores inbound payloads together with the `PayloadConverter` needed to decode them later
- keeps raw access available through an escape hatch
- preserves the existing codec boundary: codecs run at SDK/Core or client/RPC boundaries, while
  typed deserialization of failure payloads uses only the payload converter

## Goals

- Make `ApplicationFailure.details` ergonomic to set from ordinary Rust values.
- Apply the same model to every payload-backed field currently exposed through error types.
- Support lazy typed decode of inbound payload-backed fields with a serde-like generic accessor.
- Keep failure payloads codec-decoded before they are stored on Rust error wrappers.
- Preserve an explicit raw-payload escape hatch for interop and advanced users.

## Non-goals

- Inferring the concrete Rust type of decoded details automatically.
- Running codecs from workflow code or from error accessor methods.
- Changing Temporal's failure protobuf schema.
- Solving every payload-bearing API in the SDK in one pass. This document focuses on failure and
  error surfaces first.

## Current State

The current API exposes raw payloads in several places:

- `ApplicationFailure.details: Option<Payloads>`
- `CancelledError.details: Option<Payloads>`
- `TimeoutError.last_heartbeat_details: Option<Payloads>`
- `ActivityError::Cancelled { details: Option<Payload> }`
- client close-outcome errors such as `WorkflowGetResultError::Cancelled { details: Vec<Payload> }`
  and `WorkflowGetResultError::Terminated { details: Vec<Payload> }`

`DefaultFailureConverter` currently copies these payloads through unchanged. The payload converter
is not involved in failure-payload encode or decode.

There is also an important transport split today:

- worker inbound paths already run `payload_visitor::decode_payloads` before
  `DataConverter::to_error` is called
- client history/result paths do not consistently decode close-event payloads before surfacing them

That means the worker path already satisfies the "codec before error wrapper" rule, but some
client-side paths do not.

## Proposed Model

### Introduce `DecodablePayloads`

Add a new public wrapper for payload-backed error fields:

```rust
pub struct DecodablePayloads {
    payloads: Vec<Payload>,
    payload_converter: PayloadConverter,
    context: SerializationContextData,
}
```

This shared wrapper is the reusable decoded-side representation: already-materialized payloads plus
the `PayloadConverter` required for lazy typed access later.

`DecodablePayloads` should provide:

- `DecodablePayloads::new(payloads: Vec<Payload>, payload_converter: PayloadConverter, context:
  SerializationContextData) -> Self`
- `deserialize<T: TemporalDeserializable + 'static>(&self) -> Result<T, PayloadConversionError>`
- `raw(&self) -> Option<&[Payload]>`
- `into_raw(self) -> Option<RawValue>`

`deserialize` only uses the stored `PayloadConverter`. It does not invoke the codec.

Although this document focuses on failure details, the type name should stay broad because the same
representation is useful anywhere the SDK wants to retain codec-decoded payloads and offer lazy
typed access later.

### Failure-specific outbound wrapper

Failures need an additional layer because they must support both:

- outbound construction from direct user code
- inbound reconstruction from decoded protos

That should be modeled separately from the reusable decoded-side holder:

```rust
pub enum FailurePayloads {
    Serializable(Box<dyn TemporalSerializable + Send + Sync>),
    Decoded(DecodablePayloads),
}
```

This keeps the `Serializable` case limited to the failure APIs that actually need it, instead of
imposing that extra representation on every other decoded payload surface in the SDK.

### Normalize on payload collections

User-facing failure payload fields should all use the same conceptual type: zero or more payloads.
This avoids preserving the current accidental split where some surfaces use `Payloads` and others
use a single `Payload`.

Concretely:

- `ApplicationFailure.details` becomes `Option<FailurePayloads>`
- `CancelledError.details` becomes `Option<DecodablePayloads>`
- `TimeoutError.last_heartbeat_details` becomes `Option<DecodablePayloads>`
- `ActivityError::Cancelled.details` becomes `Option<FailurePayloads>`

For single-payload transport fields elsewhere, the abstraction should still serialize through
`to_payloads` and reject multi-payload outputs only if the wire format truly requires a single
payload. The failure payload fields above already map naturally to `Payloads`.

### Accessor shape

To mirror `serde_json`'s typed decode style, payload-backed accessors on decoded errors should
become generic:

```rust
impl ApplicationFailure {
    pub fn details<T: TemporalDeserializable + 'static>(
        &self,
    ) -> Result<Option<T>, PayloadConversionError>;

    pub fn raw_details(&self) -> Option<&[Payload]>;
}

impl CancelledError {
    pub fn details<T: TemporalDeserializable + 'static>(
        &self,
    ) -> Result<Option<T>, PayloadConversionError>;

    pub fn raw_details(&self) -> Option<&[Payload]>;
}

impl TimeoutError {
    pub fn last_heartbeat_details<T: TemporalDeserializable + 'static>(
        &self,
    ) -> Result<Option<T>, PayloadConversionError>;

    pub fn raw_last_heartbeat_details(&self) -> Option<&[Payload]>;
}
```

Example:

```rust
let details: MyDetails = app_err.details()?.expect("details present");
let progress: Progress = timeout.last_heartbeat_details()?.expect("heartbeat details present");
```

This is a breaking API change from the current raw getters, but it produces the intended ergonomic
shape and is reasonable for the SDK's current maturity level. The raw getters remain available as
escape hatches.

## Encode Path

### Outbound construction

The public API should accept serializable values directly:

```rust
ApplicationFailure::builder(anyhow!("boom"))
    .details(MyDetails { ... })
    .build()
```

Internally, that stores `FailurePayloads::Serializable`.

For advanced users who already have payloads, `RawValue` should remain the escape hatch:

```rust
ApplicationFailure::builder(anyhow!("boom"))
    .details(RawValue::new(payloads))
    .build()
```

`RawValue` already models "these payloads are already chosen; do not reinterpret them."

### Failure conversion

`DefaultFailureConverter::to_failure` should become responsible for materializing every
`FailurePayloads` field through the active payload converter:

1. Build a `SerializationContext` from the supplied `payload_converter` and `context`.
2. For each payload-backed field:
   - if the field is `FailurePayloads::Serializable`, call `payload_converter.to_payloads(...)`
   - if the field is `FailurePayloads::Decoded`, reuse the stored raw payloads directly
3. Attach the resulting `Payloads` to the failure proto.
4. Let existing payload visitors apply the codec to the enclosing proto later.

This keeps failure encode aligned with normal typed value encode: payload converter first, codec at
the transport boundary.

## Decode Path

### Failure decode

`DefaultFailureConverter::to_error` should populate `DecodablePayloads` when it sees
payload-backed fields in a failure:

1. Assume the incoming proto has already had its payloads codec-decoded.
2. Clone the active `PayloadConverter`.
3. Store the raw payload vector plus the `PayloadConverter` and `SerializationContextData` in a
   `DecodablePayloads`.
4. Delay typed deserialization until the user calls a generic accessor.

This lets decoded errors remain type-erased without losing the ability to recover a typed value.

### Why the codec must not run here

Failure payload accessors may be called from workflow code. Workflow code can safely use the
payload converter, but it must not invoke the codec. The codec belongs at the transport boundary
and may depend on capabilities that are not valid or deterministic inside workflow execution.

The invariant therefore becomes:

> Every proto that may later feed `DefaultFailureConverter::to_error` must have already gone
> through `decode_payloads`.

## Required Boundary Fixes

The worker path already mostly satisfies the invariant above. The implementation work should ensure
the same is true everywhere failure payloads can enter user-facing APIs.

### Worker paths

Keep the existing behavior where activations and activity tasks are passed through
`payload_visitor::decode_payloads` before error wrappers are created.

Add regression tests that specifically assert typed failure detail decode works after codec decode
and does not attempt to run the codec from the accessor.

### Client paths

The current client close-event path extracts raw cancellation and termination details directly from
history events in `WorkflowHandle::get_result_raw` without first decoding the history with the
codec. That must change.

Implementation options:

1. Decode fetched history pages before storing or interpreting them.
2. Decode only the close event before constructing the result enum.

Option 1 is simpler to reason about because it keeps the existing rule intact: all incoming history
visible to higher layers has already been codec-decoded.

The same audit should be applied to any other client path that surfaces raw failures or payload
details directly.

## Reuse Beyond Failures

Failures are not the only place where this abstraction fits.

The more general pattern is:

- a field originates as Temporal proto payloads
- codec decode happens at a transport boundary
- the SDK wants to preserve raw payloads after that point
- callers should be able to request typed decode later using the active payload converter

That pattern also appears in other surfaces in this repository:

- workflow execution memo on `WorkflowExecutionDescription`
- workflow/user metadata fields that are currently eagerly decoded into strings
- schedule memo and similar client-side decoded views
- workflow close-outcome details surfaced from history

Failures are still a special consumer because they need both directions in one representation:

- outbound construction from direct user code
- inbound reconstruction from decoded protos

But that does not justify naming the shared representation after failures. It is better to use a
general type name and let error APIs provide failure-specific accessor names on top of it.

### Memo and map-shaped fields

The same underlying `DecodablePayloads` representation can be reused for values inside map-like
containers such as memo entries. That does not require every higher-level API to have the same
shape as error details.

For example:

- failure details naturally expose `Option<FailurePayloads>` while decoded failure accessors work
  through `DecodablePayloads`
- memo naturally exposes `HashMap<String, DecodablePayloads>` or a small wrapper around that map

The reuse target is therefore the payload-value layer, not necessarily the exact outer container
type of every API.

## API Changes

### Common error types

Change payload-backed fields on common error types to use `FailurePayloads` or
`DecodablePayloads`, depending on whether the surface needs outbound construction or only decoded
read access.

Add typed accessors and raw escape hatches:

- `ApplicationFailure::details<T>()` and `ApplicationFailure::raw_details()`
- `CancelledError::details<T>()` and `CancelledError::raw_details()`
- `TimeoutError::last_heartbeat_details<T>()` and
  `TimeoutError::raw_last_heartbeat_details()`

### Outbound constructors

Add convenience constructors for the new typed model:

- `ApplicationFailureBuilder::details<T: TemporalSerializable + Send + Sync + 'static>(...)`
- `ApplicationFailure::with_details(...)`
- `ActivityError::cancelled_with_details(...)`

The exact API surface can be adjusted, but callers should never need to manually build payload
protos for these cases.

### Raw escape hatch

`RawValue` should be the supported way to explicitly opt back into raw payload handling.

This avoids teaching users to construct `Payload` / `Payloads` directly while still preserving full
control for advanced integrations.

### `From<ApplicationFailure> for Failure`

This conversion is no longer a good fit for outbound failures once details may be deferred typed
values. Converting to `Failure` now requires a `PayloadConverter`.

The recommended change is:

- keep `ApplicationFailure::failure()` for decoded failures that already retain their source proto
- route all outbound conversion through `FailureConverter::to_failure`
- deprecate or remove `From<ApplicationFailure> for Failure` for newly-constructed failures

Without this change, outbound typed details would either be silently dropped or encoded with the
wrong converter.

### Where to define the shared types

The broader decoded-payload type should live in `temporalio_common`, alongside the existing data
converter abstractions, not inside the failure-converter module. The failure-only wrapper should
remain close to the failure APIs that consume it.

The recommended placement is:

- define `DecodablePayloads` in [crates/common/src/data_converters.rs](/Users/olszewski/code/temporal/sdk/core/crates/common/src/data_converters.rs)
- re-export it from the existing `temporalio_common::data_converters` module surface
- define `FailurePayloads` in [crates/common/src/error.rs](/Users/olszewski/code/temporal/sdk/core/crates/common/src/error.rs) or in a small adjacent module re-exported from there
- keep failure-specific adaptation logic in
  [crates/common/src/data_converters/failure_converter.rs](/Users/olszewski/code/temporal/sdk/core/crates/common/src/data_converters/failure_converter.rs)

That placement is preferable because:

- `DecodablePayloads` fundamentally depends on `PayloadConverter`, `SerializationContextData`,
  `TemporalSerializable`, `TemporalDeserializable`, and `RawValue`
- it should be reusable by client-side decoded views such as workflow descriptions and schedules
- it avoids forcing non-failure code to depend on a failure-specific module
- it keeps the failure-only outbound representation near the failure API surfaces that need it

If the implementation later grows a family of related types, it would also be reasonable to split
them into a dedicated `crates/common/src/data_converters/decodable_payloads.rs` submodule and
re-export them from `data_converters.rs`. The important boundary is that the shared representation
belongs with data conversion, while failure conversion only consumes it.

## Migration Strategy

Implement this in phases.

### Phase 1

- Introduce `DecodablePayloads`.
- Introduce `FailurePayloads` as the failure-only outbound-or-inbound wrapper.
- Update `ApplicationFailure`, `CancelledError`, and `TimeoutError`.
- Teach `DefaultFailureConverter` to encode `FailurePayloads` and decode to
  `DecodablePayloads`.
- Add typed accessors and raw escape hatches.
- Update worker-side tests.

### Phase 2

- Update `ActivityError::Cancelled` to accept typed payloads.
- Add convenience constructors so activity authors no longer build raw payloads.
- Audit local-activity and activity-cancellation paths for consistency.

### Phase 3

- Update client close-outcome error surfaces to use the same typed model or an equivalent typed
  accessor shape.
- Ensure history/close-event payloads are codec-decoded before result construction.
- Audit update- and query-related failure surfaces for similar raw-payload leaks.

## Testing

The implementation should add coverage for:

- encoding `ApplicationFailure.details` from an ordinary serializable Rust value
- decoding `ApplicationFailure.details::<T>()` through the configured payload converter
- decoding cancellation details and timeout heartbeat details through typed accessors
- preserving raw access via `raw_details` / `into_raw`
- verifying that codec decode happens before failure payload access on worker paths
- verifying that client `get_result` close-event payloads are codec-decoded before exposure
- ensuring `RawValue` round-trips without extra interpretation
- ensuring converter failures are surfaced as `PayloadConversionError`

An especially important regression test is a codec that visibly mutates payload metadata or data,
combined with a typed failure-details accessor. That test proves the accessor is consuming the
already-codec-decoded payloads rather than bypassing the codec boundary.

## Open Questions

### Naming

The shared type should have a broad name because it is reusable beyond failures.

`DecodablePayloads` is the recommended name in this document because it describes what the value
actually provides: payloads that are not themselves typed, but that can be decoded into a caller-
requested type later.

This is a better fit than `TypedPayloads`, which sounds like the struct already carries concrete
type information.

### Raw getter return type

Returning `&[Payload]` is simple and avoids exposing a bespoke wrapper in the raw path. Returning
`&RawValue` is also viable if we want the escape hatch to align more directly with the typed
conversion fallback type.

### Scope of client APIs

This document recommends auditing client close-outcome errors in the same effort because they hit
the same codec-boundary issue. If that broadens the first implementation too much, they can follow
immediately after Phase 1 as a short second PR.

## Recommendation

Implement the design around a dedicated `DecodablePayloads` shared type plus a failure-specific
`FailurePayloads` wrapper, rather than storing bare `Box<dyn TemporalSerializable>` directly on
error types.

That refinement is what makes the full design coherent:

- outbound failures can defer payload materialization until `DefaultFailureConverter`
- inbound failures and other decoded payload surfaces can share one reusable decoded-side holder
- codec application remains outside workflow code
- raw-payload escape hatches remain available
