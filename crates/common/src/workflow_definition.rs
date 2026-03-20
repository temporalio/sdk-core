use crate::data_converters::{TemporalDeserializable, TemporalSerializable};

/// Defines the input and output types and registered name for a workflow.
///
/// Typically, you will want to use the `#[workflow]` and `#[workflow_methods]` macros to define
/// workflows. However, this trait may be implemented manually if desired.
pub trait WorkflowDefinition {
    /// Type of the input argument to the workflow
    type Input: TemporalDeserializable + TemporalSerializable + 'static;
    /// Type of the output of the workflow
    type Output: TemporalDeserializable + TemporalSerializable + 'static;
    /// The workflow type name
    fn name() -> &'static str;
}

/// Marker trait for workflow types that can be started via `Client::start_workflow`.
///
/// All `#[workflow]` types implement this automatically. `UntypedWorkflow` intentionally
/// does not — use `Client::start_untyped_workflow` instead.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be used with `start_workflow`",
    label = "use `start_untyped_workflow` for workflows without compile-time type information",
    note = "`UntypedWorkflow` does not implement `StartableWorkflow`; use `Client::start_untyped_workflow` instead"
)]
pub trait StartableWorkflow: WorkflowDefinition {}

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

    /// The query name.
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

    /// The signal name.
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

    /// The update name.
    fn name(&self) -> &str;
}
