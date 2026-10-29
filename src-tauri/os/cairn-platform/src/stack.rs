//! Keeping large child futures out of an awaiting function's stack frame.
//!
//! An unoptimized build gives every temporary in a function its own stack slot
//! and never lets two temporaries share one. At an `.await`, the child future is
//! such a temporary: it is built on the awaiting function's stack and only then
//! moved into the parent's state. So the poll frame of an `async fn` in a debug
//! build is about twice the *sum* of every child future it awaits, not the
//! largest one, and each layer of a chain of 40–60 KB futures pays that again.
//! A tool call crosses a dozen such layers between `dispatch_tool` and a resource
//! handler, which is how a single served `write` came to need more than the
//! 2 MiB a libtest thread or a tokio worker gets.
//!
//! `Box::pin(child()).await` does not help. The argument is still built in the
//! caller's frame before it is moved to the heap, so it shrinks the caller's
//! state but not its frame. [`off_frame`] builds the child inside its own frame,
//! which has returned before the caller polls, and leaves the caller holding a
//! pointer in both its frame and its state.
//!
//! Use it on the spine of a deep call chain where a child future is large, on
//! every arm of a dispatcher that awaits children in sequence, whose frame
//! otherwise grows with each arm added, and on a leaf that nearly every path
//! awaits many times over, as `cairn-db`'s `LocalDb` entry points do. The cost
//! is one allocation per call.
//!
//! `cairn-core`'s `tests/main/stack_headroom.rs` holds the tool path to a stack
//! budget, so a regression fails there by name instead of as an overflow in
//! whichever unrelated test happens to run deepest.

use std::future::Future;
use std::pin::Pin;

/// Build `make`'s future in this function's own stack frame and return it
/// boxed.
///
/// The body is an ordinary `Box::pin`; `#[inline(never)]` is the mechanism.
/// It guarantees the separate frame that keeps `make()`'s temporary out of the
/// caller's. Inlined, this would be exactly the `Box::pin(child()).await` that
/// does not help, under whatever optimization level a profile happens to set.
///
/// A closure that only needs shared borrows can capture them directly. One
/// that passes `&mut` must reborrow outside and `move` the reference in,
/// because a closure capturing by mutable reference cannot let a borrow of it
/// escape through its result.
#[inline(never)]
pub fn off_frame<Fut: Future>(make: impl FnOnce() -> Fut) -> Pin<Box<Fut>> {
    Box::pin(make())
}
