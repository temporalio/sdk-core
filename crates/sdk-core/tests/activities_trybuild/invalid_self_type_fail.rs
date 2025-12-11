use temporalio_macros::activities;

pub struct BadActivities;

#[activities]
impl BadActivities {
    #[activity]
    pub async fn no_context(
        &self,
        _ctx: ActivityContext,
        _in: String,
    ) -> Result<String, ActivityError> {
        // This should fail because the self type is invalid.
        Ok("bad".to_string())
    }
}

fn main() {}
