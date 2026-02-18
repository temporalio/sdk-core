use std::sync::Arc;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

pub struct MultiArgActivities;

#[activities]
impl MultiArgActivities {
    #[activity]
    pub async fn two_args(
        _ctx: ActivityContext,
        _a: String,
        _b: i32,
    ) -> Result<String, ActivityError> {
        Ok("done".to_string())
    }

    #[activity]
    pub async fn three_args(
        _ctx: ActivityContext,
        _a: String,
        _b: i32,
        _c: bool,
    ) -> Result<String, ActivityError> {
        Ok("done".to_string())
    }

    #[activity]
    pub async fn instance_two_args(
        self: Arc<Self>,
        _ctx: ActivityContext,
        _a: String,
        _b: i32,
    ) -> Result<String, ActivityError> {
        Ok("done".to_string())
    }

    #[activity]
    pub fn sync_two_args(
        _ctx: ActivityContext,
        _a: String,
        _b: i32,
    ) -> Result<String, ActivityError> {
        Ok("done".to_string())
    }
}

fn main() {}
