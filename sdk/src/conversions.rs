use temporal_sdk_core_protos::temporal::api::failure::v1::Failure;

pub(crate) fn anyhow_to_fail(e: anyhow::Error) -> Failure {
    e.chain()
        .rfold(None, |cause, e| {
            Some(Failure {
                message: e.to_string(),
                cause: cause.map(Box::new),
                ..Default::default()
            })
        })
        .expect("there is always at least one element in the error chain")
}
