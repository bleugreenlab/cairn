//! Scheduling priority for the threads that orchestrate work, relative to the
//! work they place on their own host.
//!
//! A runner shares its machine with the builds and test binaries its colocated
//! executor runs. Those are lowered (nice 10, a utility QoS clamp and throttled
//! disk I/O on macOS, the idle-adjacent I/O class on Linux; see the executor's
//! child spawn), and the runner's own runtime threads are raised here, so under
//! a saturated host the scheduler keeps choosing the process every agent's tool
//! call is waiting on. On 10-16 the runner competed on equal terms with the
//! rustc processes it placed and lost for minutes at a time.
//!
//! Raising a thread's QoS needs no privilege on macOS. Linux has no
//! unprivileged way to raise a thread above the default, so there the lowering
//! of bulk work is the whole mechanism.

/// macOS `QOS_CLASS_USER_INITIATED`: work a person is waiting on.
#[cfg(target_os = "macos")]
const QOS_CLASS_USER_INITIATED: u32 = 0x19;

#[cfg(target_os = "macos")]
extern "C" {
    fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: libc::c_int)
        -> libc::c_int;
    #[cfg(test)]
    fn qos_class_self() -> u32;
}

/// Run the calling thread ahead of bulk work on the same host: user-initiated
/// QoS on macOS, nothing elsewhere. Best effort; a refusal leaves the thread
/// at the default and is not worth failing a runtime over.
pub fn raise_current_thread_for_orchestration() {
    #[cfg(target_os = "macos")]
    // SAFETY: sets only the calling thread's own QoS; no pointers.
    unsafe {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INITIATED, 0);
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "macos")]
    #[test]
    fn an_orchestration_thread_runs_at_user_initiated_qos() {
        let qos = std::thread::spawn(|| {
            super::raise_current_thread_for_orchestration();
            // SAFETY: reads the calling thread's own QoS.
            unsafe { super::qos_class_self() }
        })
        .join()
        .unwrap();
        assert_eq!(qos, super::QOS_CLASS_USER_INITIATED);
    }
}
