use crate::{
    machines::WFCommand, protos::coresdk::workflow_completion::wf_activation_completion::Status,
    test_workflow_driver::WFFutureDriver, workflow::WorkflowFetcher,
};

use std::convert::TryInto;

#[async_trait::async_trait]
impl WorkflowFetcher for WFFutureDriver {
    async fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand> {
        if let Some(completion) = self.completions_rx.recv().await {
            completion
                .status
                .map(|s| match s {
                    Status::Successful(s) => s
                        .commands
                        .into_iter()
                        .map(|cmd| cmd.try_into().unwrap())
                        .collect(),
                    Status::Failed(_) => panic!("Ahh failed"),
                })
                .unwrap_or_default()
        } else {
            // Sender went away so nothing to do here. End of wf/test.
            vec![]
        }
    }
}
