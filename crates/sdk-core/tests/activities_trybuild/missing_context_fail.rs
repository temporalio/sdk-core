use temporalio_macros::activities;

pub struct BadActivities;

#[activities]
impl BadActivities {
    #[activity]
    pub async fn no_context(_in: String) -> Result<String, ActivityError> {
        // This should fail because there's no ActivityContext parameter
        Ok("bad".to_string())
    }
}

fn main() {}
