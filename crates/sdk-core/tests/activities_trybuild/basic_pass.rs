use std::sync::Arc;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

pub struct MyActivities;

#[activities]
impl MyActivities {
    #[activity]
    pub async fn static_activity(
        _ctx: ActivityContext,
        _in: String,
    ) -> Result<&'static str, ActivityError> {
        Ok("Can be static")
    }

    #[activity]
    pub async fn activity(
        self: Arc<Self>,
        _ctx: ActivityContext,
        _in: bool,
    ) -> Result<&'static str, ActivityError> {
        Ok("I'm done!")
    }

    #[activity]
    pub fn sync_activity(_ctx: ActivityContext, _in: bool) -> Result<&'static str, ActivityError> {
        Ok("Sync activities are supported too")
    }
}

pub struct MyActivitiesStatic;

#[activities]
impl MyActivitiesStatic {
    #[activity]
    pub async fn static_activity(
        _ctx: ActivityContext,
        _in: String,
    ) -> Result<&'static str, ActivityError> {
        Ok("Can be static")
    }
}

fn main() {}
