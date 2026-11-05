/// A process's kernel start time, kept numeric so a census of the whole machine
/// allocates nothing per process. [`StartKey::token`] is the one place the
/// start token every ownership record and every identity check compares is
/// rendered, so an identity the sampler records and one the reaper reads are
/// the same string by construction.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StartKey(pub u64, pub u64);

impl StartKey {
    /// Linux reports `starttime` in clock ticks since boot (field 22 of
    /// `stat`); macOS reports seconds and microseconds of the wall clock.
    pub fn token(self) -> String {
        if cfg!(target_os = "macos") {
            format!("{}:{}", self.0, self.1)
        } else {
            self.0.to_string()
        }
    }

    /// The inverse of [`StartKey::token`], for an identity captured as a token.
    pub fn parse(token: &str) -> Option<Self> {
        match token.split_once(':') {
            Some((seconds, micros)) => Some(Self(seconds.parse().ok()?, micros.parse().ok()?)),
            None => Some(Self(token.parse().ok()?, 0)),
        }
    }
}

/// One process's identity facts as the kernel reports them, without an
/// allocation. Live readers exclude zombies; child capture includes them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KernelProcess {
    pub pid: u32,
    pub parent_pid: u32,
    pub process_group_id: u32,
    pub session_id: Option<u32>,
    pub start: StartKey,
}

impl KernelProcess {
    pub fn identity(&self) -> ProcessIdentity {
        ProcessIdentity {
            pid: self.pid,
            start_time: self.start,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub start_time: StartKey,
}
/// What reading one pid from the kernel found. `Unreadable` is not `Gone`: a
/// process that exists but could not be read is still there, and a measurement
/// that treats it as departed loses it for good.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KernelRead {
    Live(KernelProcess),
    /// No such process, or a zombie awaiting its parent's wait.
    Gone,
    Unreadable,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn start_key_round_trip() {
        let key = if cfg!(target_os = "macos") {
            StartKey(1700000000, 123456)
        } else {
            StartKey(1700000000, 0)
        };
        assert_eq!(StartKey::parse(&key.token()), Some(key));
        assert_eq!(StartKey::parse("not-a-time"), None);
        assert_eq!(StartKey::parse("1:2:3"), None);
        assert_eq!(StartKey::parse(""), None);
    }
}
