use crate::{
    pollers::{MockServerGatewayApis, RetryGateway, RETRYABLE_ERROR_CODES},
    ServerGatewayApis,
};
use tonic::{Code, Status};

#[tokio::test]
async fn non_retryable_errors() {
    for code in [
        Code::InvalidArgument,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::FailedPrecondition,
        Code::Cancelled,
        Code::DeadlineExceeded,
        Code::Unauthenticated,
        Code::Unimplemented,
    ] {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_cancel_activity_task()
            .returning(move |_, _| Err(Status::new(code, "non-retryable failure")))
            .times(1);
        let retry_gateway = RetryGateway::new(mock_gateway, Default::default());
        let result = retry_gateway
            .cancel_activity_task(vec![1].into(), None)
            .await;
        // Expecting an error after a single attempt, since there was a non-retryable error.
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn long_poll_non_retryable_errors() {
    for code in [
        Code::InvalidArgument,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::FailedPrecondition,
        Code::Unauthenticated,
        Code::Unimplemented,
    ] {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_poll_workflow_task()
            .returning(move |_, _| Err(Status::new(code, "non-retryable failure")))
            .times(1);
        mock_gateway
            .expect_poll_activity_task()
            .returning(move |_| Err(Status::new(code, "non-retryable failure")))
            .times(1);
        let retry_gateway = RetryGateway::new(mock_gateway, Default::default());
        let result = retry_gateway
            .poll_workflow_task("tq".to_string(), false)
            .await;
        assert!(result.is_err());
        let result = retry_gateway.poll_activity_task("tq".to_string()).await;
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn retryable_errors() {
    for code in RETRYABLE_ERROR_CODES {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_cancel_activity_task()
            .returning(move |_, _| Err(Status::new(code, "retryable failure")))
            .times(3);
        mock_gateway
            .expect_cancel_activity_task()
            .returning(|_, _| Ok(Default::default()))
            .times(1);

        let retry_gateway = RetryGateway::new(mock_gateway, Default::default());
        let result = retry_gateway
            .cancel_activity_task(vec![1].into(), None)
            .await;
        // Expecting successful response after retries
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn long_poll_retries_forever() {
    let mut mock_gateway = MockServerGatewayApis::new();
    mock_gateway
        .expect_poll_workflow_task()
        .returning(move |_, _| Err(Status::new(Code::Unknown, "retryable failure")))
        .times(50);
    mock_gateway
        .expect_poll_workflow_task()
        .returning(|_, _| Ok(Default::default()))
        .times(1);
    mock_gateway
        .expect_poll_activity_task()
        .returning(move |_| Err(Status::new(Code::Unknown, "retryable failure")))
        .times(50);
    mock_gateway
        .expect_poll_activity_task()
        .returning(|_| Ok(Default::default()))
        .times(1);

    let retry_gateway = RetryGateway::new(mock_gateway, Default::default());

    let result = retry_gateway
        .poll_workflow_task("tq".to_string(), false)
        .await;
    assert!(result.is_ok());
    let result = retry_gateway.poll_activity_task("tq".to_string()).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn long_poll_retries_deadline_exceeded() {
    // For some reason we will get cancelled in these situations occasionally (always?) too
    for code in [Code::Cancelled, Code::DeadlineExceeded] {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_poll_workflow_task()
            .returning(move |_, _| Err(Status::new(code, "retryable failure")))
            .times(5);
        mock_gateway
            .expect_poll_workflow_task()
            .returning(|_, _| Ok(Default::default()))
            .times(1);
        mock_gateway
            .expect_poll_activity_task()
            .returning(move |_| Err(Status::new(code, "retryable failure")))
            .times(5);
        mock_gateway
            .expect_poll_activity_task()
            .returning(|_| Ok(Default::default()))
            .times(1);

        let retry_gateway = RetryGateway::new(mock_gateway, Default::default());

        let result = retry_gateway
            .poll_workflow_task("tq".to_string(), false)
            .await;
        assert!(result.is_ok());
        let result = retry_gateway.poll_activity_task("tq".to_string()).await;
        assert!(result.is_ok());
    }
}
