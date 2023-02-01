//! Utilities for and tracking of internal versions which alter history in incompatible ways
//! so that we can use older code paths for workflows executed on older core versions.

/// This enumeration contains internal patches that may result in incompatible history changes with
/// older workflows, or other breaking changes.
///
/// The largest discriminant value is meant to be used as the "current" patch level. When a patch
/// has existed long enough the version it was introduced in is no longer supported, it may be
/// removed from the enum. *Importantly*, all variants must be given explicit values, such that
/// removing older variants does not create any change in existing values.
pub(crate) enum InternalPatchLevel {
    /// The current patch level is pre-introduction of this concept. The workflow cannot support
    /// any new internal patches at this level, and existing code must be processed with the old
    /// paths. This variant can never be removed and will always have the `0` discriminant.
    Primordial = 0,
    /// In this patch, additional checks were added to a number of state machines to ensure that
    /// the ID and type of activities, local activities, and child workflows match during replay.
    IdAndTypeDeterminismChecks = 1,
}

impl InternalPatchLevel {
    /// The current highest patch level. Must be updated when a new variant is added.
    pub const HIGHEST: InternalPatchLevel = InternalPatchLevel::IdAndTypeDeterminismChecks;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// This test exists to fail at compile time if a new variant is added. It's a reminder to
    /// update the highest level.
    #[test]
    fn make_sure_you_checked() {
        let highest = InternalPatchLevel::HIGHEST;
        match highest {
            InternalPatchLevel::Primordial => {}
            InternalPatchLevel::IdAndTypeDeterminismChecks => {}
        }
    }
}
