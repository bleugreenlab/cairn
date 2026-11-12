//! The wire shape a thread's headline takes when it rides on something else
//! the operator acts on: a notification-center entry, an operator request.

use serde::{Deserialize, Serialize};

/// The line a thread wrote about one address, shown above whatever else
/// points there so the operator reads why before he presses the button.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadlineAbout {
    pub line: String,
    /// The headline row's canonical address, `cairn://p/P/THREAD/db/headlines/ID`.
    pub uri: String,
}
