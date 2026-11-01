//! Machines, named the way Cairn names executors.

crate::string_id!(
    /// A machine by its Cairn executor name, whether or not that executor is
    /// enrolled yet: the Sparks are `bginf-sp1` and `bginf-sp2`, and the halo
    /// pool's one machine is `fedora`.
    MachineName
);
