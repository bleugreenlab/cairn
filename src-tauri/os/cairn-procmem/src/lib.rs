//! One kernel census and sticky identity-fenced membership engine per host cadence.
//!
//! Consumers own policy, never kernel walks. Registration and cached reads do no
//! kernel work. Batch observers execute on the sampling thread after the census.
pub mod census;
pub mod fd_census;
pub mod identity;
mod membership;
pub mod platform;
mod sampler;

pub use census::{Census, CensusFn, CensusRow, SchedulingTimes};
pub use fd_census::{fd_census, FdCensus, FdCensusRow};
pub use identity::{KernelProcess, KernelRead, ProcessIdentity, StartKey};
pub use sampler::{
    monotonic_stamp, BatchObserver, Clock, ProcessTreeSampler, ProcessTreeWatch, SamplingStatus,
    TickBatch, TickClocks, TreeReading, WatchId, WatchOptions, WatchTick,
};

#[cfg(any(test, feature = "test-utils"))]
pub mod test_support;
