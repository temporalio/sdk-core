use crate::data_converters::{TemporalDeserializable, TemporalSerializable};

/// Defines the input and output types and registered name for a workflow.
///
/// User-defined workflow structs (annotated with `#[workflow]` and `#[workflow_methods]`)
/// implement this trait automatically. It may also be implemented manually if desired.
///
/// This trait is the type parameter for [`WorkflowHandle`], enabling typed access to
/// workflow results, signals, queries, and updates.
pub trait WorkflowDefinition {
    /// Type of the input argument to the workflow
    type Input: TemporalDeserializable + TemporalSerializable + 'static;
    /// Type of the output of the workflow
    type Output: TemporalDeserializable + TemporalSerializable + 'static;
    /// The workflow type name
    fn name() -> &'static str;
}

/// Implement on a marker struct to define a query.
///
/// Typically, you will want to use the `#[query]` attribute inside a `#[workflow_methods]` macro
/// to define queries. However, this trait may be implemented manually if desired.
pub trait QueryDefinition {
    /// The workflow type this query belongs to
    type Workflow: WorkflowDefinition;
    /// Type of the input argument to the query.
    type Input: TemporalDeserializable + TemporalSerializable + 'static;
    /// Type of the output of the query.
    type Output: TemporalDeserializable + TemporalSerializable + 'static;

    /// The query handler name.
    fn name(&self) -> &str;
}

/// Implement on a marker struct to define a signal.
///
/// Typically, you will want to use the `#[signal]` attribute inside a `#[workflow_methods]` macro
/// to define signals. However, this trait may be implemented manually if desired.
pub trait SignalDefinition {
    /// The workflow type this signal belongs to
    type Workflow: WorkflowDefinition;
    /// Type of the input argument to the signal.
    type Input: TemporalDeserializable + TemporalSerializable + 'static;

    /// The signal handler name.
    fn name(&self) -> &str;
}

/// Implement on a marker struct to define an update.
///
/// Typically, you will want to use the `#[update]` attribute inside a `#[workflow_methods]` macro
/// to define updates. However, this trait may be implemented manually if desired.
pub trait UpdateDefinition {
    /// The workflow type this update belongs to
    type Workflow: WorkflowDefinition;
    /// Type of the input argument to the update.
    type Input: TemporalDeserializable + TemporalSerializable + 'static;
    /// Type of the output of the update.
    type Output: TemporalDeserializable + TemporalSerializable + 'static;

    /// The update handler name.
    fn name(&self) -> &str;
}
