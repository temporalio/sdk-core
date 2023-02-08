//! Utilities for and tracking of internal versions which alter history in incompatible ways
//! so that we can use older code paths for workflows executed on older core versions.

use std::collections::{BTreeSet, HashSet};
use temporal_sdk_core_protos::temporal::api::{
    history::v1::WorkflowTaskCompletedEventAttributes, sdk::v1::WorkflowTaskCompletedMetadata,
};

/// This enumeration contains internal flags that may result in incompatible history changes with
/// older workflows, or other breaking changes.
///
/// When a flag has existed long enough the version it was introduced in is no longer supported, it
/// may be removed from the enum. *Importantly*, all variants must be given explicit values, such
/// that removing older variants does not create any change in existing values. Removed flag
/// variants must be reserved forever (a-la protobuf), and should be called out in a comment.
#[repr(u32)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Debug)]
pub(crate) enum CoreInternalFlags {
    /// In this flag, additional checks were added to a number of state machines to ensure that
    /// the ID and type of activities, local activities, and child workflows match during replay.
    IdAndTypeDeterminismChecks = 0,
    /// We received a value higher than this code can understand.
    TooHigh = u32::MAX,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct InternalFlags {
    core: BTreeSet<CoreInternalFlags>,
    lang: BTreeSet<u32>,
    core_since_last_complete: HashSet<CoreInternalFlags>,
    lang_since_last_complete: HashSet<u32>,
}

impl InternalFlags {
    pub fn add_from_complete(&mut self, e: &WorkflowTaskCompletedEventAttributes) {
        if let Some(metadata) = e.sdk_metadata.as_ref() {
            self.core.extend(
                metadata
                    .core_used_flags
                    .iter()
                    .map(|u| CoreInternalFlags::from_u32(*u)),
            );
            self.lang.extend(metadata.lang_used_flags.iter());
        }
    }

    pub fn add_lang_used(&mut self, flags: impl IntoIterator<Item = u32>) {
        self.lang_since_last_complete.extend(flags.into_iter());
    }

    /// Returns true if this flag may currently be used. If `replaying` is false, always returns
    /// true and records the flag as being used, for taking later via
    /// [Self::gather_for_wft_complete].
    pub fn try_use(&mut self, core_patch: CoreInternalFlags, replaying: bool) -> bool {
        if !replaying {
            self.core_since_last_complete.insert(core_patch);
            true
        } else {
            self.core.contains(&core_patch)
        }
    }

    /// Wipes the recorded flags used during the current WFT and returns a partially filled
    /// sdk metadata message that can be combined with any existing data before sending the WFT
    /// complete
    pub fn gather_for_wft_complete(&mut self) -> WorkflowTaskCompletedMetadata {
        WorkflowTaskCompletedMetadata {
            core_used_flags: self
                .core_since_last_complete
                .drain()
                .map(|p| p as u32)
                .collect(),
            lang_used_flags: self.lang_since_last_complete.drain().collect(),
        }
    }

    pub fn all_lang(&self) -> &BTreeSet<u32> {
        &self.lang
    }
}

impl CoreInternalFlags {
    fn from_u32(v: u32) -> Self {
        match v {
            0 => Self::IdAndTypeDeterminismChecks,
            _ => Self::TooHigh,
        }
    }
}
