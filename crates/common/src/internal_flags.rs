//! Core internal flags that may result in incompatible history changes with older workflows.
//!
//! This enum is defined in the `common` crate so that test utilities (e.g. `TestHistoryBuilder`)
//! can reference flag values directly. The runtime flag tracking machinery (`InternalFlags`) remains
//! in `sdk-core`.

/// This enumeration contains internal flags that may result in incompatible history changes with
/// older workflows, or other breaking changes.
///
/// When a flag has existed long enough that the version it was introduced in is no longer supported, it
/// may be removed from the enum. *Importantly*, all variants must be given explicit values, such
/// that removing older variants does not create any change in existing values. Removed flag
/// variants must be reserved forever (a-la protobuf), and should be called out in a comment.
#[repr(u32)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Debug, enum_iterator::Sequence)]
pub enum CoreInternalFlags {
    /// In this flag additional checks were added to a number of state machines to ensure that
    /// the ID and type of activities, local activities, and child workflows match during replay.
    IdAndTypeDeterminismChecks = 1,

    /// Introduced automatically upserting search attributes for each patched call, and
    /// nondeterminism checks for upserts.
    UpsertSearchAttributeOnPatch = 2,

    /// Prior to this flag, we truncated commands received from lang at the
    /// first terminal (i.e. workflow-terminating) command. With this flag, we
    /// reorder commands such that all non-terminal commands come first,
    /// followed by the first terminal command, if any (it's possible that
    /// multiple workflow coroutines generated a terminal command). This has the
    /// consequence that all non-terminal commands are sent to the server, even
    /// if in the sequence delivered by lang they came after a terminal command.
    /// See <https://github.com/temporalio/features/issues/481>.
    MoveTerminalCommands = 3,

    /// Indicates that this workflow uses the improved WFT heartbeat heuristic that
    /// is more conservative when deciding whether to collapse consecutive empty WFTs.
    /// The improved heuristic avoids incorrectly collapsing WFTs that carry updates,
    /// and requires more look-ahead before committing to a chunking decision when
    /// history is paginated.
    ///
    /// Only set on workflows started after this flag was introduced (first-WFT-only).
    /// Existing workflows continue to use the legacy heuristic.
    ImprovedHeartbeatHeuristic = 4,

    /// We received a value higher than this code can understand.
    UnknownFlag = u32::MAX,
}

impl CoreInternalFlags {
    /// Returns the CoreInternalFlags enum value for the given raw u32 flag value.
    pub fn from_u32(v: u32) -> Self {
        match v {
            1 => Self::IdAndTypeDeterminismChecks,
            2 => Self::UpsertSearchAttributeOnPatch,
            3 => Self::MoveTerminalCommands,
            4 => Self::ImprovedHeartbeatHeuristic,
            _ => Self::UnknownFlag,
        }
    }

    /// Returns all cumulative flags that should be enabled by default on every WFT completion.
    pub fn all_cumulative_default_enabled() -> impl Iterator<Item = CoreInternalFlags> {
        [
            CoreInternalFlags::IdAndTypeDeterminismChecks,
            CoreInternalFlags::UpsertSearchAttributeOnPatch,
            CoreInternalFlags::MoveTerminalCommands,
        ]
        .iter()
        .copied()
    }

    /// Returns cumulative flags that should only be enabled on the first WFT of new workflows.
    /// These are not written on subsequent WFTs to avoid changing behavior of existing workflows.
    pub fn all_first_wft_only_default_enabled() -> impl Iterator<Item = CoreInternalFlags> {
        [CoreInternalFlags::ImprovedHeartbeatHeuristic]
            .iter()
            .copied()
    }

    /// Returns all known flag variants (excluding the sentinel).
    pub fn all_except_unknown() -> impl Iterator<Item = CoreInternalFlags> {
        enum_iterator::all::<CoreInternalFlags>()
            .filter(|f| !matches!(f, CoreInternalFlags::UnknownFlag))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_have_u32_from_impl() {
        let all_known = CoreInternalFlags::all_except_unknown();
        for flag in all_known {
            let as_u32 = flag as u32;
            assert_eq!(CoreInternalFlags::from_u32(as_u32), flag);
        }
    }
}
