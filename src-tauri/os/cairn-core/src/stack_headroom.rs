//! Holding a deep async path to a stack budget, by name.
//!
//! In an unoptimized build an async poll frame costs about twice the sum of
//! every child future it awaits (see `cairn_platform::stack`), so a path that
//! crosses a dozen such layers can outgrow the 2 MiB a libtest thread and a
//! tokio worker get. When it does, the overflow aborts whichever process runs
//! it deepest: an unrelated test that reads as flake, or a running instance.
//! A guard runs one such path on a thread with half that stack, so lost
//! headroom fails the guard, by name, while there is still room to find what
//! grew.
//!
//! Every guard shares [`BUDGET`] and [`within_budget`], so they cannot drift
//! apart and a budget is never raised for one path alone.

use std::future::Future;

/// Half of the 2 MiB that std gives a spawned thread and tokio gives a worker.
pub const BUDGET: usize = 1024 * 1024;

/// Drive `make`'s future to completion on a current-thread runtime, pinned on
/// the stack of a thread that has only [`BUDGET`] bytes: the shape
/// `#[tokio::test]` gives a test body, with less room. Tasks the body spawns
/// run on that same thread, from the runtime's own loop, as a worker would run
/// them. An overflow aborts the process with a message naming `path`.
pub fn within_budget<F: Future<Output = ()> + 'static>(
    path: &str,
    make: impl FnOnce() -> F + Send + 'static,
) {
    let joined = std::thread::Builder::new()
        .name(format!("{path} within a {BUDGET}-byte stack"))
        .stack_size(BUDGET)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let body = std::pin::pin!(make());
            runtime.block_on(body);
        })
        .unwrap()
        .join();
    if let Err(panic) = joined {
        std::panic::resume_unwind(panic);
    }
}
