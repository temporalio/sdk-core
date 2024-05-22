use temporal_sdk::{ActContext, ActivityError};

pub(crate) async fn echo(_ctx: ActContext, e: String) -> Result<String, ActivityError> {
    Ok(e)
}
