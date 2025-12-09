use temporalio_sdk::{ActivityContext, ActivityError};

pub(crate) async fn echo(_ctx: ActivityContext, e: String) -> Result<String, ActivityError> {
    Ok(e)
}
