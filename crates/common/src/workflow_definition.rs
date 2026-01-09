use crate::data_converters::{TemporalDeserializable, TemporalSerializable};

/// Implement on a marker struct to define a workflow.
///
/// Typically, you will want to use the `#[workflow]` and `#[workflow_methods]` macros to define
/// workflows. However, this trait may be implemented manually if desired.
pub trait WorkflowDefinition {
    /// Type of the input argument to the workflow
    type Input: TemporalDeserializable + TemporalSerializable + 'static;
    /// Type of the output of the workflow
    type Output: TemporalDeserializable + TemporalSerializable + 'static;
    /// The workflow type name
    fn name() -> &'static str
    where
        Self: Sized;
}

/// Implement on a marker struct to define a query.
///
/// Typically, you will want to use the `#[query]` attribute inside a `#[workflow_methods]` macro
/// to define updates. However, this trait may be implemented manually if desired.
pub trait QueryDefinition {
    type Input: TemporalDeserializable + TemporalSerializable + 'static;
    type Output: TemporalSerializable + 'static;

    fn name() -> &'static str
    where
        Self: Sized;
}

/// Implement on a marker struct to define a signal.
///
/// Typically, you will want to use the `#[signal]` attribute inside a `#[workflow_methods]` macro
/// to define signals. However, this trait may be implemented manually if desired.
pub trait SignalDefinition {
    type Input: TemporalDeserializable + TemporalSerializable + 'static;

    fn name() -> &'static str
    where
        Self: Sized;
}

/// Implement on a marker struct to define an update.
///
/// Typically, you will want to use the `#[update]` attribute inside a `#[workflow_methods]` macro
/// to define updates. However, this trait may be implemented manually if desired.
pub trait UpdateDefinition {
    type Input: TemporalDeserializable + TemporalSerializable + 'static;
    type Output: TemporalSerializable + 'static;

    fn name() -> &'static str
    where
        Self: Sized;
}
