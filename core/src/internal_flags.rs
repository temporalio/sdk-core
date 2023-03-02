//! Utilities for and tracking of internal versions which alter history in incompatible ways
//! so that we can use older code paths for workflows executed on older core versions.

use std::collections::{BTreeSet, HashSet};
use temporal_sdk_core_protos::temporal::api::{
    history::v1::WorkflowTaskCompletedEventAttributes, sdk::v1::WorkflowTaskCompletedMetadata,
    workflowservice::v1::get_system_info_response,
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
    /// In this flag additional checks were added to a number of state machines to ensure that
    /// the ID and type of activities, local activities, and child workflows match during replay.
    IdAndTypeDeterminismChecks = 1,
    /// Introduced automatically upserting search attributes for each patched call, and
    /// nondeterminism checks for upserts.
    UpsertSearchAttributeOnPatch = 2,
    /// We received a value higher than this code can understand.
    TooHigh = u32::MAX,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InternalFlags {
    enabled: bool,
    core: BTreeSet<CoreInternalFlags>,
    lang: BTreeSet<u32>,
    core_since_last_complete: HashSet<CoreInternalFlags>,
    lang_since_last_complete: HashSet<u32>,
}

impl InternalFlags {
    pub fn new(server_capabilities: &get_system_info_response::Capabilities) -> Self {
        Self {
            enabled: server_capabilities.sdk_metadata,
            core: Default::default(),
            lang: Default::default(),
            core_since_last_complete: Default::default(),
            lang_since_last_complete: Default::default(),
        }
    }

    #[cfg(test)]
    pub fn all_core_enabled() -> Self {
        Self {
            enabled: true,
            core: BTreeSet::from([
                CoreInternalFlags::IdAndTypeDeterminismChecks,
                CoreInternalFlags::UpsertSearchAttributeOnPatch,
            ]),
            lang: Default::default(),
            core_since_last_complete: Default::default(),
            lang_since_last_complete: Default::default(),
        }
    }

    pub fn add_from_complete(&mut self, e: &WorkflowTaskCompletedEventAttributes) {
        if !self.enabled {
            return;
        }

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
        if !self.enabled {
            return;
        }

        self.lang_since_last_complete.extend(flags.into_iter());
    }

    /// Returns true if this flag may currently be used. If `should_record` is true, always returns
    /// true and records the flag as being used, for taking later via
    /// [Self::gather_for_wft_complete].
    pub fn try_use(&mut self, core_patch: CoreInternalFlags, should_record: bool) -> bool {
        if !self.enabled {
            // If the server does not support the metadata field, we must assume we can never use
            // any internal flags since they can't be recorded for future use
            return false;
        }

        if should_record {
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
            1 => Self::IdAndTypeDeterminismChecks,
            2 => Self::UpsertSearchAttributeOnPatch,
            _ => Self::TooHigh,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::get_system_info_response::Capabilities;

    #[test]
    fn disabled_in_capabilities_disables() {
        let mut f = InternalFlags::new(&Capabilities::default());
        f.add_lang_used([1]);
        f.add_from_complete(&WorkflowTaskCompletedEventAttributes {
            sdk_metadata: Some(WorkflowTaskCompletedMetadata {
                core_used_flags: vec![1],
                lang_used_flags: vec![],
            }),
            ..Default::default()
        });
        let gathered = f.gather_for_wft_complete();
        assert_matches!(gathered.core_used_flags.as_slice(), &[]);
        assert_matches!(gathered.lang_used_flags.as_slice(), &[]);
    }
}
