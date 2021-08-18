use crate::pollers::{MockServerGatewayApis, RetryGateway, RETRYABLE_ERROR_CODES};
use crate::ServerGatewayApis;
use tonic::{Code, Status};

#[tokio::test]
async fn non_retryable_errors() {
    for code in [
        Code::InvalidArgument,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::FailedPrecondition,
    ] {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_cancel_activity_task()
            .returning(move |_, _| Err(Status::new(code, "non-retryable failure")))
            .times(1);
        let retry_gateway = RetryGateway::new(mock_gateway, None);
        let result = retry_gateway
            .cancel_activity_task(vec![1].into(), None)
            .await;
        // Expecting an error after a single attempt, since there was a non-retryable error.
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
            .times(1);
        mock_gateway
            .expect_cancel_activity_task()
            .returning(|_, _| Ok(Default::default()))
            .times(1);

        let retry_gateway = RetryGateway::new(mock_gateway, None);
        let result = retry_gateway
            .cancel_activity_task(vec![1].into(), None)
            .await;
        // Expecting successful response after one retry.
        assert!(result.is_ok());
    }
}
