use temporal_sdk::ActContext;

pub(crate) async fn echo(_ctx: ActContext, e: String) -> anyhow::Result<String> {
    Ok(e)
}
