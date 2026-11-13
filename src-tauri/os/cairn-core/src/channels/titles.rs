//! The name a provider gives a conversation, fetched from the provider and
//! cached in this process.
//!
//! A conversation's label is derived when it is read and never stored on a
//! row. Telegram's chat title lives only on Telegram, so it is fetched through
//! the running provider (`getChat`) and held here for [`TITLE_TTL`]; a rename
//! shows within that window. A provider that names nothing, fails, or times
//! out is cached too, so a failing lookup is not retried on every read.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use super::{ChannelProvider, ConversationProvider};

/// How long a fetched title stands before it is fetched again.
const TITLE_TTL: Duration = Duration::from_secs(10 * 60);

/// Bounds the provider call, which runs inside a resource read.
const FETCH_TIMEOUT: Duration = Duration::from_secs(2);

/// A title is one line of at most this many characters.
const MAX_TITLE_CHARS: usize = 64;

/// A fetched title (or its absence) and when it was fetched.
type Fetched = (Option<String>, Instant);

/// Fetched titles by `(provider, conversation)`.
pub(crate) struct ConversationTitles {
    ttl: Duration,
    seen: Mutex<HashMap<(String, String), Fetched>>,
}

impl Default for ConversationTitles {
    fn default() -> Self {
        Self::with_ttl(TITLE_TTL)
    }
}

impl ConversationTitles {
    pub(crate) fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// `conversation`'s title on `provider`: the cached one while it stands,
    /// otherwise asked of `runtime` and cached. `None` without a runtime, and
    /// that absence is not cached, so the title appears once the provider runs.
    pub(crate) async fn get(
        &self,
        provider: &str,
        conversation: &str,
        runtime: Option<Arc<dyn ChannelProvider>>,
    ) -> Option<String> {
        let key = (provider.to_string(), conversation.to_string());
        if let Some((title, at)) = self
            .seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
        {
            if at.elapsed() < self.ttl {
                return title.clone();
            }
        }
        let runtime = runtime?;
        let title =
            match tokio::time::timeout(FETCH_TIMEOUT, runtime.conversation_title(conversation))
                .await
            {
                Ok(Ok(title)) => title.as_deref().and_then(sanitize),
                Ok(Err(error)) => {
                    log::debug!("no title for {conversation}: {error}");
                    None
                }
                Err(_) => {
                    log::debug!("no title for {conversation}: the provider did not answer in time");
                    None
                }
            };
        self.seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, (title.clone(), Instant::now()));
        title
    }
}

/// The first line, trimmed, cut to [`MAX_TITLE_CHARS`]; `None` when empty.
fn sanitize(title: &str) -> Option<String> {
    let line = title.lines().next().unwrap_or_default().trim();
    (!line.is_empty()).then(|| line.chars().take(MAX_TITLE_CHARS).collect())
}

static TITLES: OnceLock<ConversationTitles> = OnceLock::new();

/// The title the running provider gives `conversation`, for its label. Only a
/// Telegram label carries one: a Discord channel is named by its Cairn binding
/// and an iMessage conversation by its handle.
pub async fn conversation_title(
    provider: ConversationProvider,
    conversation: &str,
) -> Option<String> {
    if provider != ConversationProvider::Telegram {
        return None;
    }
    let runtime = super::runtime_slot()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(provider.id())
        .cloned();
    TITLES
        .get_or_init(ConversationTitles::default)
        .get(provider.id(), conversation, runtime)
        .await
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::channels::{
        ChannelCapabilities, ChannelHealth, InboundEvent, OutboundMessage, SentIds,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    /// A running provider that names every conversation `title`, counts the
    /// asks, and replies natively when `native_reply` says so.
    pub(crate) struct TitledProvider {
        pub title: Option<String>,
        pub asks: AtomicUsize,
        pub native_reply: bool,
    }

    impl TitledProvider {
        pub(crate) fn new(title: Option<&str>) -> Arc<Self> {
            Arc::new(Self {
                title: title.map(str::to_string),
                asks: AtomicUsize::new(0),
                native_reply: false,
            })
        }

        pub(crate) fn replying(native_reply: bool) -> Arc<Self> {
            Arc::new(Self {
                title: None,
                asks: AtomicUsize::new(0),
                native_reply,
            })
        }
    }

    #[async_trait::async_trait]
    impl ChannelProvider for TitledProvider {
        fn capabilities(&self) -> ChannelCapabilities {
            ChannelCapabilities {
                native_reply: self.native_reply,
                ..ChannelCapabilities::default()
            }
        }
        async fn send(&self, _message: &OutboundMessage) -> Result<SentIds, String> {
            Err("not a sending provider".into())
        }
        fn subscribe(&self) -> mpsc::Receiver<InboundEvent> {
            mpsc::channel(1).1
        }
        fn health(&self) -> ChannelHealth {
            ChannelHealth::Ready
        }
        async fn conversation_title(&self, _conversation: &str) -> Result<Option<String>, String> {
            self.asks.fetch_add(1, Ordering::SeqCst);
            Ok(self.title.clone())
        }
    }

    #[tokio::test]
    async fn a_fresh_title_is_served_without_asking_the_provider() {
        let titles = ConversationTitles::default();
        let provider = TitledProvider::new(Some("Ops\nsecond line"));
        for _ in 0..3 {
            assert_eq!(
                titles
                    .get("telegram", "telegram:42", Some(provider.clone()))
                    .await
                    .as_deref(),
                Some("Ops")
            );
        }
        assert_eq!(provider.asks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_expired_title_is_fetched_again() {
        let titles = ConversationTitles::with_ttl(Duration::ZERO);
        let provider = TitledProvider::new(Some("Ops"));
        titles
            .get("telegram", "telegram:42", Some(provider.clone()))
            .await;
        titles
            .get("telegram", "telegram:42", Some(provider.clone()))
            .await;
        assert_eq!(provider.asks.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn no_title_is_cached_too_but_no_runtime_is_not() {
        let titles = ConversationTitles::default();
        assert_eq!(titles.get("telegram", "telegram:42", None).await, None);
        let provider = TitledProvider::new(None);
        for _ in 0..2 {
            assert_eq!(
                titles
                    .get("telegram", "telegram:42", Some(provider.clone()))
                    .await,
                None
            );
        }
        assert_eq!(provider.asks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_title_is_one_trimmed_bounded_line() {
        assert_eq!(sanitize("  Ops \nmore").as_deref(), Some("Ops"));
        assert_eq!(sanitize(" \n"), None);
        assert_eq!(sanitize(&"x".repeat(100)).unwrap().chars().count(), 64);
    }
}
