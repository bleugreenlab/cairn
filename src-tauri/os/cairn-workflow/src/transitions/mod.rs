pub mod outcome;
pub mod projection;
pub mod run;
pub mod session;
pub mod status;
pub mod turn;
/// Error type for invalid state transitions.
#[derive(Debug, Clone)]
pub struct TransitionError {
    pub entity: &'static str,
    pub id: String,
    pub from: String,
    pub to: String,
    pub reason: String,
}

impl std::fmt::Display for TransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Invalid {} transition for {}: {} → {} ({})",
            self.entity, self.id, self.from, self.to, self.reason
        )
    }
}

impl std::error::Error for TransitionError {}
