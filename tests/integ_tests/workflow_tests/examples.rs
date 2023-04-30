use serde_json::json;
use temporal_client::WorkflowOptions;
use temporal_sdk::prelude::registry::*;
use temporal_sdk_core_protos::coresdk::AsJsonPayloadExt;
use temporal_sdk_core_test_utils::CoreWfStarter;

mod activity {
    use temporal_sdk::prelude::activity::*;

    #[derive(Debug, thiserror::Error)]
    #[non_exhaustive]
    pub enum Error {
        #[error(transparent)]
        Io(#[from] std::io::Error),
        #[error(transparent)]
        Any(anyhow::Error),
    }

    impl FromFailureExt for Error {
        fn from_failure(failure: Failure) -> Error {
            Error::Any(anyhow::anyhow!("{:?}", failure.message))
        }
    }

    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub struct ActivityOutput0 {
        pub echo: String,
        pub name: Option<String>,
    }
    pub async fn activity_0_args(
        _ctx: ActContext,
    ) -> Result<(String, ActivityOutput0), anyhow::Error> {
        Ok((
            "Temporal".to_string(),
            ActivityOutput0 {
                echo: "Hello".to_string(),
                name: Some("Rust".to_string()),
            },
        ))
    }
    pub async fn activity_0_args_exit_value(
        _ctx: ActContext,
    ) -> Result<ActExitValue<ActivityOutput0>, anyhow::Error> {
        Ok(ActExitValue::Normal(ActivityOutput0 {
            echo: "Hello".to_string(),
            name: Some("Rust".to_string()),
        }))
    }
    pub async fn activity_0_args_without_ctx() -> Result<(String, ActivityOutput0), anyhow::Error> {
        Ok((
            "test_tuple".to_string(),
            ActivityOutput0 {
                echo: "Hello".to_string(),
                name: Some("Rust".to_string()),
            },
        ))
    }
    pub async fn activity_0_args_without_ctx_exit_value(
    ) -> Result<ActExitValue<ActivityOutput0>, anyhow::Error> {
        Ok(ActExitValue::Normal(ActivityOutput0 {
            echo: "Hello".to_string(),
            name: Some("Rust".to_string()),
        }))
    }

    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub struct ActivityInput1(pub String, pub i32);
    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub struct ActivityOutput1(pub String, pub i32);
    pub async fn activity_1_args(
        _ctx: ActContext,
        input: ActivityInput1,
    ) -> Result<ActivityOutput1, anyhow::Error> {
        Ok(ActivityOutput1(input.0, input.1))
    }
    pub async fn activity_1_args_exit_value(
        _ctx: ActContext,
        input: ActivityInput1,
    ) -> Result<ActExitValue<ActivityOutput1>, anyhow::Error> {
        Ok(ActExitValue::Normal(ActivityOutput1(input.0, input.1)))
    }
    pub async fn activity_1_args_with_errors(
        _ctx: ActContext,
        input: ActivityInput1,
    ) -> Result<ActivityOutput1, Error> {
        Ok(ActivityOutput1(input.0, input.1))
    }

    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub struct ActivityOutput2(pub Vec<i32>, pub String);
    pub async fn activity_2_args(
        _ctx: ActContext,
        name: String,
        size: i32,
    ) -> Result<ActivityOutput2, anyhow::Error> {
        Ok(ActivityOutput2(vec![42; size as usize], name))
    }
}

mod workflow {
    use crate::integ_tests::workflow_tests::examples::activity::{self, *};
    use std::collections::HashMap;
    use temporal_sdk::prelude::workflow::*;
    use temporal_sdk::workflow;

    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub struct ChildInput1 {
        pub echo: (String,),
    }
    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub struct ChildOutput1 {
        pub result: (String,),
    }
    pub async fn child_workflow_0_args(_ctx: WfContext) -> Result<ChildOutput1, anyhow::Error> {
        Ok(ChildOutput1 {
            result: ("success".to_string(),),
        })
    }
    pub async fn child_workflow_0_args_exit_value(
        _ctx: WfContext,
    ) -> Result<WfExitValue<String>, anyhow::Error> {
        Ok(WfExitValue::Normal("success".to_string()))
    }

    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub struct ChildInput2 {
        pub echo_again: (String,),
    }
    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub struct ChildOutput2 {
        pub result: HashMap<String, i32>,
    }
    pub async fn child_workflow_1_args(
        _ctx: WfContext,
        _input: ChildInput1,
    ) -> Result<ChildOutput1, anyhow::Error> {
        Ok(ChildOutput1 {
            result: ("success".to_string(),),
        })
    }
    pub async fn child_workflow_1_args_exit_value(
        _ctx: WfContext,
        _input: ChildInput1,
    ) -> Result<WfExitValue<ChildOutput1>, anyhow::Error> {
        Ok(WfExitValue::Normal(ChildOutput1 {
            result: ("success".to_string(),),
        }))
    }
    pub async fn child_workflow_1_args_with_errors(
        ctx: WfContext,
        _input: (String,),
    ) -> Result<activity::ActivityOutput1, Error> {
        let activity_timeout = Duration::from_secs(5);
        let output = execute_activity_1_args_with_errors(
            &ctx,
            ActivityOptions {
                activity_id: Some("activity_1_args_with_errors".to_string()),
                schedule_to_close_timeout: Some(activity_timeout),
                ..Default::default()
            },
            activity::activity_1_args_with_errors,
            activity::ActivityInput1("hello".to_string(), 7),
        )
        .await;
        match output {
            Ok(output) => Ok(output),
            Err(e) => Err(e),
        }
    }
    pub async fn child_workflow_2_args(
        _ctx: WfContext,
        _input: ChildInput1,
        _input2: ChildInput2,
    ) -> Result<ChildOutput2, anyhow::Error> {
        let mut result = HashMap::new();
        result.insert("one".to_string(), 1);
        result.insert("two".to_string(), 2);
        Ok(ChildOutput2 { result })
    }

    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub struct Input {
        pub one: String,
        pub two: i64,
    }
    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub struct Output {
        pub result: HashMap<String, i32>,
    }
    pub async fn examples_workflow(ctx: WfContext) -> Result<Output, anyhow::Error> {
        let activity_timeout = Duration::from_secs(5);

        let _output = workflow::execute_activity_0_args(
            &ctx,
            ActivityOptions {
                activity_id: Some("activity_0_args".to_string()),
                schedule_to_close_timeout: Some(activity_timeout),
                ..Default::default()
            },
            activity::activity_0_args,
        )
        .await;

        let _output = workflow::execute_activity_0_args_exit_value(
            &ctx,
            ActivityOptions {
                activity_id: Some("activity_0_args_exit_value".to_string()),
                schedule_to_close_timeout: Some(activity_timeout),
                ..Default::default()
            },
            activity::activity_0_args_exit_value,
        )
        .await;

        let _output = workflow::execute_activity_0_args_without_ctx(
            &ctx,
            ActivityOptions {
                activity_id: Some("activity_0_args_without_ctx".to_string()),
                schedule_to_close_timeout: Some(activity_timeout),
                ..Default::default()
            },
            activity::activity_0_args_without_ctx,
        )
        .await;

        let _output = workflow::execute_activity_0_args_without_ctx_exit_value(
            &ctx,
            ActivityOptions {
                activity_id: Some("activity_0_args_without_ctx_exit_value".to_string()),
                schedule_to_close_timeout: Some(activity_timeout),
                ..Default::default()
            },
            activity::activity_0_args_without_ctx_exit_value,
        )
        .await;

        let _stop = workflow::execute_activity_1_args(
            &ctx,
            ActivityOptions {
                activity_id: Some("activity_1_args".to_string()),
                schedule_to_close_timeout: Some(activity_timeout),
                ..Default::default()
            },
            activity::activity_1_args,
            ActivityInput1("hello".to_string(), 7),
        )
        .await;

        let _stop = workflow::execute_activity_1_args_exit_value(
            &ctx,
            ActivityOptions {
                activity_id: Some("stop_activity".to_string()),
                schedule_to_close_timeout: Some(activity_timeout),
                ..Default::default()
            },
            activity::activity_1_args_exit_value,
            ActivityInput1("hello".to_string(), 7),
        )
        .await;

        let _output = workflow::execute_activity_2_args(
            &ctx,
            ActivityOptions {
                activity_id: Some("activity_2_args".to_string()),
                schedule_to_close_timeout: Some(activity_timeout),
                ..Default::default()
            },
            activity::activity_2_args,
            "hello".to_string(),
            7,
        )
        .await;

        let _child_output0 = workflow::execute_child_workflow_0_args(
            &ctx,
            ChildWorkflowOptions {
                workflow_id: "child_workflow_0_args".to_owned(),
                ..Default::default()
            },
            child_workflow_0_args,
            //"test".to_string(),
        )
        .await;

        let _child_output0 = workflow::execute_child_workflow_0_args_exit_value(
            &ctx,
            ChildWorkflowOptions {
                workflow_id: "child_workflow_0_args_exit_value".to_owned(),
                ..Default::default()
            },
            child_workflow_0_args_exit_value,
            //"test".to_string(),
        )
        .await;

        let _child_output3 = workflow::execute_child_workflow_1_args(
            &ctx,
            ChildWorkflowOptions {
                workflow_id: "child_workflow_1_args".to_owned(),
                ..Default::default()
            },
            child_workflow_1_args,
            ChildInput1 {
                echo: ("child_workflow_1_args".to_string(),),
            },
        )
        .await
        .unwrap();

        let _child_output3 = workflow::execute_child_workflow_1_args_exit_value(
            &ctx,
            ChildWorkflowOptions {
                workflow_id: "child_workflow_1_args_exit_value".to_owned(),
                ..Default::default()
            },
            child_workflow_1_args_exit_value,
            ChildInput1 {
                echo: ("child_workflow_1_args_exit_value".to_string(),),
            },
        )
        .await
        .unwrap();

        let _child_output4 = workflow::execute_child_workflow_1_args_with_errors(
            &ctx,
            ChildWorkflowOptions {
                workflow_id: "child_workflow_1_args_with_errors".to_owned(),
                ..Default::default()
            },
            child_workflow_1_args_with_errors,
            ("hello".to_string(),),
        )
        .await
        .unwrap();

        let child_output5 = workflow::execute_child_workflow_2_args(
            &ctx,
            ChildWorkflowOptions {
                workflow_id: "child_workflow_2_args".to_owned(),
                ..Default::default()
            },
            child_workflow_2_args,
            ChildInput1 {
                echo: ("hello".to_string(),),
            },
            ChildInput2 {
                echo_again: ("hello".to_string(),),
            },
        )
        .await;

        match child_output5 {
            Ok(r) => Ok(Output { result: r.result }),
            Err(e) => Err(e),
        }
    }
}

#[tokio::test]
async fn example_workflows_test() {
    use crate::integ_tests::workflow_tests::examples::activity;
    use crate::integ_tests::workflow_tests::examples::workflow;

    let mut starter = CoreWfStarter::new("sdk-example-workflows");
    let mut worker = starter.worker().await;

    // Register zero argument activities

    worker.register_activity(
        "integ_tests::integ_tests::workflow_tests::examples::activity::activity_0_args",
        into_activity_0_args(activity::activity_0_args),
    );
    worker.register_activity(
        "integ_tests::integ_tests::workflow_tests::examples::activity::activity_0_args_exit_value",
        into_activity_0_args(activity::activity_0_args_exit_value),
    );
    worker.register_activity(
        "integ_tests::integ_tests::workflow_tests::examples::activity::activity_0_args_without_ctx",
        into_activity_0_args_without_ctx(activity::activity_0_args_without_ctx),
    );
    worker.register_activity(
        "integ_tests::integ_tests::workflow_tests::examples::activity::activity_0_args_without_ctx_exit_value",
        into_activity_0_args_without_ctx(activity::activity_0_args_without_ctx_exit_value),
    );

    // Register one argument activities

    worker.register_activity(
        "integ_tests::integ_tests::workflow_tests::examples::activity::activity_1_args",
        into_activity_1_args(activity::activity_1_args),
    );

    worker.register_activity(
        "integ_tests::integ_tests::workflow_tests::examples::activity::activity_1_args_exit_value",
        into_activity_1_args_exit_value(activity::activity_1_args_exit_value),
    );

    worker.register_activity(
        "integ_tests::integ_tests::workflow_tests::examples::activity::activity_1_args_with_errors",
        into_activity_1_args_with_errors(activity::activity_1_args_with_errors),
    );

    // Register two argument activities

    worker.register_activity(
        "integ_tests::integ_tests::workflow_tests::examples::activity::activity_2_args",
        into_activity_2_args(activity::activity_2_args),
    );

    // Register zero argument workflows

    worker.register_wf(
        "integ_tests::integ_tests::workflow_tests::examples::workflow::child_workflow_0_args",
        into_workflow_0_args(workflow::child_workflow_0_args),
    );

    worker.register_wf(
        "integ_tests::integ_tests::workflow_tests::examples::workflow::child_workflow_0_args_exit_value",
        into_workflow_0_args(workflow::child_workflow_0_args_exit_value),
    );

    // Register one argument workflows

    worker.register_wf(
        "integ_tests::integ_tests::workflow_tests::examples::workflow::child_workflow_1_args",
        into_workflow_1_args(workflow::child_workflow_1_args),
    );

    worker.register_wf(
        "integ_tests::integ_tests::workflow_tests::examples::workflow::child_workflow_1_args_exit_value",
        into_workflow_1_args_exit_value(workflow::child_workflow_1_args_exit_value),
    );

    worker.register_wf(
        "integ_tests::integ_tests::workflow_tests::examples::workflow::child_workflow_1_args_with_errors",
        into_workflow_1_args_with_errors(workflow::child_workflow_1_args_with_errors),
    );

    // Register two argument workflows

    worker.register_wf(
        "integ_tests::integ_tests::workflow_tests::examples::workflow::child_workflow_2_args",
        into_workflow_2_args(workflow::child_workflow_2_args),
    );

    worker.register_wf(
        "integ_tests::integ_tests::workflow_tests::examples::workflow::examples_workflow",
        into_workflow_0_args(workflow::examples_workflow),
    );

    // Run parent workflow

    worker
        .submit_wf(
            "examples_workflow".to_string(),
            "integ_tests::integ_tests::workflow_tests::examples::workflow::examples_workflow"
                .to_string(),
            vec![json!({
                "one":"one",
                "two":42
            })
            .as_json_payload()
            .expect("serialize fine")],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}
