//! The operator as a recipient: `cairn://operator`.
//!
//! A message to the operator is one `operator_messages` row in the main
//! database. It holds the facts of the message itself: what it claims, the
//! channel its sender chose (`via`), the default the sender acts on without a
//! reply, the headline it is linked to, and whether and how he has seen and
//! answered it. Delivery is the transport's, so a phone `via` writes one
//! `channel_outbound` row of kind `operator` (see
//! [`crate::channels::operator_message`]) and its state is derived from that
//! row rather than copied: a message is *delivered* when the row is sent, and
//! sent through `app` the moment it exists.
//!
//! Every caller moves a message through the same two transitions,
//! [`messages::record_seen`] and [`messages::record_answered`], so the notice
//! its sender gets is the same whichever source saw it: the iMessage read
//! receipt, the operator's headline cursor passing its headline, or the
//! message opened in the app.

pub mod messages;

pub use messages::{
    message_uri, record_answered, record_seen, AnsweredVia, Message, MessageFilter, SeenVia, State,
    Via,
};
