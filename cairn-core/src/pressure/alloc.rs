//! Process-wide allocation counters behind a `#[global_allocator]` wrapper.
//!
//! [`CountingAllocator`] wraps the [`System`] allocator and maintains three
//! relaxed atomic counters (one atomic op per alloc/dealloc — negligible). The
//! wrapper TYPE and its counters live here in shared code; each daemon binary
//! (`cairn-runner`, `cairn-server`) installs it as its `#[global_allocator]`.
//! The desktop app and cairn-core tests do NOT install it, so the counters
//! simply stay at zero there.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative bytes handed out by the allocator (monotonic).
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
/// Cumulative bytes returned to the allocator (monotonic).
static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
/// Cumulative number of allocation calls (monotonic).
static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);

/// A `#[global_allocator]`-compatible wrapper around [`System`] that counts the
/// bytes allocated/freed and the number of allocations. Zero-sized.
pub struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            // Model a realloc as free-old + alloc-new so live-bytes stays
            // accurate across in-place and moved reallocations alike.
            DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        new_ptr
    }
}

/// A point-in-time read of the allocation counters.
#[derive(Debug, Clone, Copy)]
pub struct AllocSnapshot {
    pub(crate) total_allocated_bytes: u64,
    pub(crate) total_deallocated_bytes: u64,
    pub(crate) alloc_count: u64,
}

impl AllocSnapshot {
    /// Read the current counters.
    pub(crate) fn read() -> Self {
        Self {
            total_allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
            total_deallocated_bytes: DEALLOCATED_BYTES.load(Ordering::Relaxed),
            alloc_count: ALLOC_COUNT.load(Ordering::Relaxed),
        }
    }

    /// Live (not-yet-freed) bytes = allocated - deallocated. Saturating because
    /// the two counters are independent atomics read without a lock, so a
    /// concurrent dealloc can momentarily make deallocated exceed the allocated
    /// value we read.
    pub(crate) fn live_bytes(&self) -> u64 {
        self.total_allocated_bytes
            .saturating_sub(self.total_deallocated_bytes)
    }

    /// Bytes allocated since an earlier snapshot.
    pub(crate) fn allocated_since(&self, prev: &AllocSnapshot) -> u64 {
        self.total_allocated_bytes
            .saturating_sub(prev.total_allocated_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The process's real `#[global_allocator]` in the cairn-core test binary is
    // the default System allocator, so nothing else touches these static
    // counters. Driving `CountingAllocator`'s methods directly here exercises
    // the counting logic and the System delegation without a second global
    // allocator; before/after deltas stay robust to any parallel test.
    #[test]
    fn counting_allocator_tracks_alloc_and_dealloc() {
        let before = AllocSnapshot::read();
        let layout = Layout::from_size_align(4096, 8).unwrap();
        // SAFETY: layout is valid and non-zero; the pointer is freed below with
        // the same layout it was allocated with.
        unsafe {
            let ptr = CountingAllocator.alloc(layout);
            assert!(!ptr.is_null());
            let after_alloc = AllocSnapshot::read();
            assert!(
                after_alloc.total_allocated_bytes >= before.total_allocated_bytes + 4096,
                "allocated bytes should grow by at least the layout size"
            );
            assert!(after_alloc.alloc_count > before.alloc_count);
            CountingAllocator.dealloc(ptr, layout);
        }
        let after = AllocSnapshot::read();
        assert!(after.total_deallocated_bytes >= before.total_deallocated_bytes + 4096);
    }

    #[test]
    fn live_and_since_are_saturating_deltas() {
        let prev = AllocSnapshot {
            total_allocated_bytes: 1_000,
            total_deallocated_bytes: 400,
            alloc_count: 10,
        };
        let now = AllocSnapshot {
            total_allocated_bytes: 3_000,
            total_deallocated_bytes: 1_000,
            alloc_count: 25,
        };
        assert_eq!(now.live_bytes(), 2_000);
        assert_eq!(now.allocated_since(&prev), 2_000);
        // Saturating: a stale prev never yields a negative/underflowed delta.
        assert_eq!(prev.allocated_since(&now), 0);
    }
}
