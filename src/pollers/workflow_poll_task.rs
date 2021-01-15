use crate::protos::temporal::api::workflowservice::v1;

struct WorkflowPollTask {
    namespace: String,
    task_queue: String,
    identity: String,
    binary_checksum: String,
}

trait PollTask<T> {
    fn poll() -> T;
}

impl PollTask<v1::PollWorkflowTaskQueueResponse> for WorkflowPollTask {
    fn poll() -> v1::PollWorkflowTaskQueueResponse {
        unimplemented!()
    }
}
