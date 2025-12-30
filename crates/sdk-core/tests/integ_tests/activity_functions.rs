use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

pub(crate) async fn echo(_ctx: ActivityContext, e: String) -> Result<String, ActivityError> {
    Ok(e)
}

pub(crate) struct StdActivities {}

#[activities]
impl StdActivities {
    #[activity]
    pub(crate) async fn echo(_ctx: ActivityContext, e: String) -> Result<String, ActivityError> {
        Ok(e)
    }
}
