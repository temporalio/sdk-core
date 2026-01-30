use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

pub struct SimpleActivities;

#[activities]
impl SimpleActivities {
    #[activity]
    pub async fn no_input_activity(_ctx: ActivityContext) -> Result<String, ActivityError> {
        Ok("No input needed".to_string())
    }
}

fn main() {}
