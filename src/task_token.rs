use std::fmt::{Debug, Display, Formatter};

#[derive(Hash, Eq, PartialEq, Clone, derive_more::From)]
pub struct TaskToken(pub Vec<u8>);

impl Display for TaskToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&fmt_tt(&self.0))
    }
}

impl Debug for TaskToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("TaskToken({})", fmt_tt(&self.0)))
    }
}

pub fn fmt_tt(tt: &[u8]) -> String {
    base64::encode(tt)
}
