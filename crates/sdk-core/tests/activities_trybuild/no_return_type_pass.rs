use temporalio_macros::activities;
use temporalio_sdk::activities::ActivityContext;

pub struct VoidActivities;

#[activities]
impl VoidActivities {
    #[activity]
    pub async fn no_return(_ctx: ActivityContext, _in: String) {
        println!("Doing work...");
    }

    #[activity]
    pub fn sync_no_return(_ctx: ActivityContext) {
        println!("Sync work...");
    }
}

fn main() {}
