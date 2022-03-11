//! User-definable interceptors are defined in this module

use temporal_sdk_core_protos::coresdk::workflow_completion::WorkflowActivationCompletion;

/// Implementors can intercept certain actions that happen within the Worker.
///
/// Advanced usage only.
pub trait WorkerInterceptor {
    /// Called every time a workflow activation completes
    fn on_workflow_activation_completion(&self, completion: &WorkflowActivationCompletion);
}
