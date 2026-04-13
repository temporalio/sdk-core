# Rust Failure Conversion Constraints

This document captures why failure conversion in the Rust SDK is materially harder than in the
other Temporal SDKs, and why seemingly simple converter work keeps turning into broader error-model
design work.

The short version is:

- other SDKs mostly translate between a dynamic wire format and dynamic language error objects
- Rust is translating between a dynamic wire format and a mostly static, type-driven error model
- that means the hard problem is not just conversion, but defining the canonical Rust failure model

## Why This Needs A Separate Note

When compared against TypeScript, Ruby, or other SDKs, the Rust failure-converter work can look
overly complicated.

That is misleading unless the architectural differences are made explicit.

The Rust SDK is not struggling because the team is overthinking protobuf decoding. It is struggling
because the converter sits on top of unresolved questions about:

- what the Rust SDK's canonical failure types should be
- where type erasure is acceptable
- how much raw proto data must remain inspectable
- how much normalization should happen in shared code vs SDK-local wrapper code

Those questions are mostly absent, or much less severe, in dynamic-language SDKs.

## Fundamental Difference: Dynamic Errors vs Static Errors

The Temporal `Failure` proto is a dynamic transport object:

- it has runtime-selected failure kinds
- it has recursively nested causes
- it carries optional metadata depending on the failure kind
- it carries transport-only fields such as `source`, `stack_trace`, and `encoded_attributes`

Dynamic-language SDKs already work with dynamic error objects.

For example:

- TypeScript converts failures into subclasses of `TemporalFailure`
- Ruby converts failures into subclasses of `Temporalio::Error::Failure`

That is a natural translation:

- proto kind -> runtime error class
- nested proto cause -> nested runtime error cause

Rust does not start from the same place.

Idiomatic Rust error handling is largely compile-time and type-directed:

- `Result<T, E>`
- concrete error enums
- `thiserror`
- `std::error::Error`
- `?`-driven propagation

But at the boundary where the converter operates, the type information is often already erased:

- `Box<dyn Error>`
- `anyhow::Error`
- source chains made of arbitrary user and SDK errors

So Rust pays both costs at once:

- the language pushes toward typed error modeling
- the converter boundary often receives erased values

That tension is the root of most of the complexity.

## The Converter Is Not Just Translating, It Is Recovering Structure

In the other SDKs, the converter largely performs direct mapping.

In Rust, the converter often has to answer harder questions:

- is this erased error chain really an application failure?
- is this SDK wrapper semantically meaningful, or just context transport?
- if a special SDK error appears in a nested cause, should it become the top-level failure kind?
- when decoding, what should the Rust type be for a failure kind that has no settled Rust error
  type yet?

That means the converter is not merely encoding and decoding. It is reconstructing semantic
structure from partially erased inputs.

## The Rust SDK Does Not Yet Have A Single Canonical Failure Hierarchy

Dynamic SDKs can define a family of SDK failure base classes and route nearly all failure
conversion through them.

The Rust SDK currently has a more mixed surface:

- shared error structs and enums
- SDK wrapper enums around raw `Failure` protos
- `anyhow::Error` as a general escape hatch
- user-authored arbitrary error types

This mixed model is workable, but it means the converter inherits architectural ambiguity.

Specifically, the converter cannot be simple until the SDK is clear about:

- which failures should have first-class Rust types
- which ones should remain wrapper/context types
- which ones should normalize into shared types vs SDK-local types

Until that model settles, converter work naturally turns into API design work.

## Rust Users Expect Typed Surfaces To Mean Something Real

Another major difference is user expectation.

In Rust, if the SDK exposes a structured error type, users reasonably expect:

- it represents a real semantic category
- it has a stable contract
- it composes correctly with `source()`
- it supports normal inspection and matching patterns

That makes "invent a nice type now and clean it up later" a poor strategy.

The bar is higher than in a runtime-class-based model, because adding a Rust error type is not just
adding a more convenient exception class. It is defining part of the public semantic model.

This is why the Rust SDK has to think harder about normalization boundaries before exposing new
decoded types.

## The Proto Carries More Than Rust Error Types Naturally Want To Model

A Temporal `Failure` can contain:

- semantic failure kind
- nested cause
- retryability metadata
- activity / child workflow / nexus metadata
- details payloads
- `source`
- `stack_trace`
- `encoded_attributes`

Dynamic-language SDKs can attach those fields to error objects with relatively little friction.

In Rust, every field raises design questions:

- should this be a first-class typed field?
- should this remain available only through the original proto?
- if it is not modeled directly, how does the user inspect it?
- does the SDK promise round-tripping through the Rust error type?

This is why retained access to the original `Failure` proto matters so much in the Rust design.

## Call-Site Context Matters More In Rust Than It First Appears

Some Rust SDK error distinctions are not derivable from the proto alone.

For example:

- activity failed vs activity cancelled
- child workflow failed vs child workflow cancelled

Those distinctions can come from activation/result status in addition to the inner `Failure`.

That means a function like:

```rust
fn to_error(failure: Failure) -> Box<dyn Error>
```

cannot fully recover the whole SDK-level error story by itself.

The Rust SDK therefore has to separate:

- failure normalization from the proto
- higher-level wrapper construction that also depends on call-site context

That separation exists conceptually in other SDKs too, but it is more important in Rust because
the public error surface is more explicit and more varied.

## Encode And Decode Have Different Failure Modes

Encoding and decoding are not symmetric problems in Rust.

On encode:

- user code starts from arbitrary Rust errors
- the SDK must classify erased or semi-erased error chains
- wrapper transparency matters a lot

On decode:

- the SDK starts from a proto with explicit failure kind information
- the hard question is what Rust type to normalize into
- the ergonomic API question becomes central immediately

This is why "just implement `to_error(...)`" is misleading.

If `to_error(...)` is built as a one-off erased adapter, it will likely need to be redesigned as
soon as the SDK wants a better inbound error surface. The durable design step is normalization.

## Why Other SDKs Look Simpler

TypeScript and Ruby are simpler here for real reasons:

- they already have SDK-local failure class hierarchies
- they can attach original failure data to error instances naturally
- their runtime type systems make class-based normalization cheap
- they do not need to balance the same compile-time type expectations Rust users have

So their converter code can look more direct without being less correct.

Rust is paying extra complexity because it is trying to preserve:

- idiomatic Rust error handling
- honest cross-language Temporal failure semantics
- future ergonomic typed APIs
- retained access to the original proto

Those are valid goals, but they force more up-front architectural decisions.

## Consequence For Design Work

The practical consequence is that failure-converter work in Rust should be approached in this
order:

1. define the normalized Rust failure model
2. decide which fields are first-class vs proto-backed
3. decide which higher-level SDK wrappers are built from normalized failures plus call-site context
4. implement erased converter adapters on top of that model

Doing it in the opposite order leads to churn:

- ad hoc decode helpers that do not compose into a real API
- encode classification logic tied to temporary wrappers
- repeated redesign of public error types

## Non-Goals For This Note

This document does not prescribe the final normalized error hierarchy.

It only explains why the Rust SDK cannot safely treat failure conversion as a narrow
serialization-only concern.

That constraint should inform all future design documents in this area, especially:

- failure encoding plans
- failure decoding plans
- public error surface redesigns
