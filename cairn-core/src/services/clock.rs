//! Clock service for time-related operations.
//!
//! Abstracts time to enable deterministic testing.

use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(any(test, feature = "test-utils"))]
use mockall::automock;

/// Trait for getting current time.
///
/// This abstraction allows tests to inject a fixed time,
/// making time-dependent logic deterministic.
#[cfg_attr(any(test, feature = "test-utils"), automock)]
pub trait Clock: Send + Sync {
    /// Get the current Unix timestamp in seconds.
    fn now(&self) -> i64;

    /// Get the current Unix timestamp in seconds as u64.
    fn now_u64(&self) -> u64;
}

/// Production clock implementation using system time.
pub struct RealClock;

impl Clock for RealClock {
    fn now(&self) -> i64 {
        chrono::Utc::now().timestamp()
    }

    fn now_u64(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_clock_returns_reasonable_timestamp() {
        let clock = RealClock;
        let now = clock.now();
        // Should be after 2024-01-01 (1704067200)
        assert!(now > 1704067200);
    }

    #[test]
    fn real_clock_now_u64_returns_reasonable_timestamp() {
        let clock = RealClock;
        let now = clock.now_u64();
        // Should be after 2024-01-01
        assert!(now > 1704067200);
    }

    #[test]
    fn mock_clock_returns_fixed_time() {
        let mut mock = MockClock::new();
        mock.expect_now().returning(|| 1704067200);
        mock.expect_now_u64().returning(|| 1704067200);

        assert_eq!(mock.now(), 1704067200);
        assert_eq!(mock.now_u64(), 1704067200);
    }
}
