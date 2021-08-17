use crate::pollers::{MockServerGatewayApis, RetryGateway};
use crate::ServerGatewayApis;
use tonic::{Code, Status};

#[tokio::test]
async fn non_retryable_errors() {
    let non_retryable_codes = vec![
        Code::InvalidArgument,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::FailedPrecondition,
    ];

    for code in non_retryable_codes {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_cancel_activity_task()
            .returning(move |_, _| Err(Status::new(code, "non-retryable failure")))
            .times(1);
        let retry_gateway = RetryGateway::new(mock_gateway);
        let result = retry_gateway
            .cancel_activity_task(vec![1].into(), None)
            .await;
        // Expecting an error after a single attempt, since there was a non-retryable error.
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn retryable_errors() {
    let retryable_codes = vec![
        Code::Cancelled,
        Code::DataLoss,
        Code::DeadlineExceeded,
        Code::Internal,
        Code::Unknown,
        Code::ResourceExhausted,
        Code::Aborted,
        Code::OutOfRange,
        Code::Unimplemented,
        Code::Unavailable,
        Code::Unauthenticated,
    ];

    for code in retryable_codes {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_cancel_activity_task()
            .returning(move |_, _| Err(Status::new(code, "retryable failure")))
            .times(1);
        mock_gateway
            .expect_cancel_activity_task()
            .returning(|_, _| Ok(Default::default()))
            .times(1);

        let retry_gateway = RetryGateway::new(mock_gateway);
        let result = retry_gateway
            .cancel_activity_task(vec![1].into(), None)
            .await;
        // Expecting successful response after one retry.
        assert!(result.is_ok());
    }
}
