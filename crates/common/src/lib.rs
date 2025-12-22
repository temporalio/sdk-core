// TODO [rust-sdk-branch]: Enable
//#![warn(missing_docs)] // error if there are missing docs

//! This crate contains base-level functionality needed by the other crates in the Temporal Core and
//! Rust SDK.

#[allow(unused_imports)] // Not used by all flag combinations, which is fine.
#[macro_use]
extern crate tracing;

mod activity_definition;
pub mod data_converters;
#[cfg(feature = "envconfig")]
pub mod envconfig;
#[doc(hidden)]
pub mod fsm_trait;
pub mod protos;
pub mod telemetry;
pub mod worker;

pub use activity_definition::ActivityDefinition;

macro_rules! dbg_panic {
  ($($arg:tt)*) => {
      use tracing::error;
      error!($($arg)*);
      debug_assert!(false, $($arg)*);
  };
}
pub(crate) use dbg_panic;
