use temporal_sdk::ActContext;

pub async fn echo(_ctx: ActContext, e: String) -> anyhow::Result<String> {
    Ok(e)
}
