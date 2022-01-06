//! Very much subject to change. Defines traits for converting inputs/outputs to/from payloads

use serde::{Deserialize, Serialize};

/// Something that can be deserialized from a payload. Currently just a pass-through to
/// [Deserialize] which may actually be the best long-run choice.
pub trait FromPayload<'de>: Deserialize<'de> {}

/// Something that can be serialized into a payload. Currently just a pass-through to
/// [Serialize] which may actually be the best long-run choice.
pub trait IntoPayload: Serialize {}
