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
    ) -> Result<String, ActivityError> {
        Ok("Can be static".to_string())
    }

    #[activity]
    pub async fn activity(
        self: Arc<Self>,
        _ctx: ActivityContext,
        _in: bool,
    ) -> Result<String, ActivityError> {
        Ok("I'm done!".to_string())
    }

    #[activity]
    pub async fn activity_arc_fully_qualified(
        self: std::sync::Arc<Self>,
        _ctx: ActivityContext,
        _in: bool,
    ) -> Result<String, ActivityError> {
        Ok("I'm done!".to_string())
    }

    #[activity]
    pub fn sync_activity(_ctx: ActivityContext, _in: bool) -> Result<String, ActivityError> {
        Ok("Sync activities are supported too".to_string())
    }
}

pub struct MyActivitiesStatic;

#[activities]
impl MyActivitiesStatic {
    #[activity]
    pub async fn static_activity(
        _ctx: ActivityContext,
        _in: String,
    ) -> Result<String, ActivityError> {
        Ok("Can be static".to_string())
    }
}

fn main() {}
