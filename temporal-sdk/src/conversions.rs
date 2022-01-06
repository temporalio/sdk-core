use temporal_sdk_core_protos::temporal::api::failure::v1::Failure;

pub(crate) fn anyhow_to_fail(e: anyhow::Error) -> Failure {
    Failure {
        message: e.to_string(),
        ..Default::default()
    }
}
