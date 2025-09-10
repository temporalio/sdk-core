use std::collections::HashMap;

use prost::{EncodeError, Message};

pub trait TryIntoOrNone<F, T> {
    /// Turn an option of something into an option of another thing, trying to convert along the way
    /// and returning `None` if that conversion fails
    fn try_into_or_none(self) -> Option<T>;
}

impl<F, T> TryIntoOrNone<F, T> for Option<F>
where
    F: TryInto<T>,
{
    fn try_into_or_none(self) -> Option<T> {
        self.map(TryInto::try_into).transpose().ok().flatten()
    }
}

/// Use to encode an message into a proto `Any`.
///
/// Delete this once `prost_wkt_types` supports `prost` `0.12.x` which has built-in any packing.
pub fn pack_any<T: Message>(
    type_url: String,
    msg: &T,
) -> Result<prost_wkt_types::Any, EncodeError> {
    let mut value = Vec::new();
    Message::encode(msg, &mut value)?;
    Ok(prost_wkt_types::Any { type_url, value })
}

/// Given a header map, lowercase all the keys and return it as a new map.
/// Any keys that are duplicated after lowercasing will clobber each other in undefined ordering.
pub fn normalize_http_headers(headers: HashMap<String, String>) -> HashMap<String, String> {
    let mut new_headers = HashMap::new();
    for (header_key, val) in headers.into_iter() {
        new_headers.insert(header_key.to_lowercase(), val);
    }
    new_headers
}
