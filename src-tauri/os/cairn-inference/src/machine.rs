//! Machines, named the way Cairn names executors.

crate::string_id!(
    /// A machine by its Cairn executor name, whether or not that executor is
    /// enrolled yet. bginf's `halo` is the executor `fedora`; the Sparks keep
    /// their bginf names (`sp1`, `sp2`) and are not enrolled executors yet.
    MachineName
);
