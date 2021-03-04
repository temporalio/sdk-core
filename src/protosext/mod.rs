mod history_info;
pub(crate) use history_info::HistoryInfo;
pub(crate) use history_info::HistoryInfoError;

pub(crate) fn fmt_task_token(token: &[u8]) -> String {
    base64::encode(token)
}
