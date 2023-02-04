//! Utilities for and tracking of internal versions which alter history in incompatible ways
//! so that we can use older code paths for workflows executed on older core versions.

use std::collections::HashSet;
use temporal_sdk_core_protos::temporal::api::{
    history::v1::WorkflowTaskCompletedEventAttributes,
    sdk::v1::InternalPatches as InternalPatchesProto,
};

/// This enumeration contains internal patches that may result in incompatible history changes with
/// older workflows, or other breaking changes.
///
/// When a patch has existed long enough the version it was introduced in is no longer supported, it
/// may be removed from the enum. *Importantly*, all variants must be given explicit values, such
/// that removing older variants does not create any change in existing values. Removed patch
/// variants must be reserved forever (a-la protobuf), and should be called out in a comment.
#[repr(u32)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug)]
pub(crate) enum CoreInternalPatches {
    /// In this patch, additional checks were added to a number of state machines to ensure that
    /// the ID and type of activities, local activities, and child workflows match during replay.
    IdAndTypeDeterminismChecks = 0,
    /// We received a value higher than this code can understand.
    TooHigh = u32::MAX,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct InternalPatches {
    core: HashSet<CoreInternalPatches>,
    lang: HashSet<u32>,
    core_since_last_complete: HashSet<CoreInternalPatches>,
}

impl InternalPatches {
    pub fn add_from_complete(&mut self, e: &WorkflowTaskCompletedEventAttributes) {
        if let Some(patches) = e
            .sdk_data
            .as_ref()
            .and_then(|d| d.internal_patches.as_ref())
        {
            self.core.extend(
                patches
                    .core_used_patches
                    .iter()
                    .map(|u| CoreInternalPatches::from_u32(*u)),
            );
            self.lang.extend(patches.lang_used_patches.iter());
        }
    }

    /// Returns true if this patch may currently be used. If `replaying` is false, always returns
    /// true and records the patch as being used, for taking later via
    /// [Self::gather_for_wft_complete].
    pub fn try_use(&mut self, core_patch: CoreInternalPatches, replaying: bool) -> bool {
        if !replaying {
            self.core_since_last_complete.insert(core_patch);
            true
        } else if self.core.contains(&core_patch) {
            true
        } else {
            false
        }
    }

    /// Wipes the recorded patches used during the current WFT and returns them, if any.
    pub fn gather_for_wft_complete(&mut self) -> InternalPatchesProto {
        InternalPatchesProto {
            core_used_patches: self
                .core_since_last_complete
                .drain()
                .map(|p| p as u32)
                .collect(),
            lang_used_patches: vec![],
        }
    }
}

impl CoreInternalPatches {
    fn from_u32(v: u32) -> Self {
        match v {
            0 => Self::IdAndTypeDeterminismChecks,
            _ => Self::TooHigh,
        }
    }
}

impl From<InternalPatches> for InternalPatchesProto {
    fn from(value: InternalPatches) -> Self {
        Self {
            core_used_patches: value.core.into_iter().map(|v| v as u32).collect(),
            lang_used_patches: value.lang.into_iter().collect(),
        }
    }
}
