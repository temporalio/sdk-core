use tonic::{Code, Status};

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(remote = "Status")]
pub(super) struct SerdeStatus {
    #[serde(getter = "get_code")]
    code: i32,
    #[serde(getter = "get_owned_msg")]
    message: String,
}

fn get_owned_msg(v: &Status) -> String {
    v.message().to_string()
}

fn get_code(v: &Status) -> i32 {
    v.code() as i32
}

impl From<SerdeStatus> for Status {
    fn from(v: SerdeStatus) -> Self {
        Status::new(Code::from(v.code), v.message)
    }
}
