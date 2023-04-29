use temporal_sdk::{ActContext, ActExitValue};
use temporal_sdk_core_protos::temporal::api::common::v1::Payload;

pub async fn echo(ctx: ActContext) -> anyhow::Result<ActExitValue<Payload>> {
    Ok(ActExitValue::Normal(ctx.get_args()[0].clone()))
}
