//! Live engine metrics: every healthy deployment's own `/metrics`, scraped
//! every [`SCRAPE_EVERY`] and kept in memory for [`KEPT_MS`], so the Inference
//! page can show what each running model is doing now.
//!
//! Nothing is persisted. A deployment's window starts empty at each load and
//! is dropped when it stops being healthy, so rates never span a restart.
//! Measured speed per recipe and pool is a verification benchmark's to
//! record, not a live observation's.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use cairn_inference::control::Phase;
use cairn_inference::metrics::{self, EngineSample, EngineSummary};
use cairn_inference::DeploymentId;

use super::inference::load_inventory;
use super::pool_control::deployment_phases;
use super::Orchestrator;

/// How often each healthy deployment is scraped.
pub const SCRAPE_EVERY: Duration = Duration::from_secs(10);
/// How much history a deployment keeps.
pub const KEPT_MS: u64 = 10 * 60_000;
/// The span a rate is taken over.
pub const RATE_WINDOW_MS: u64 = 60_000;
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(3);

/// One deployment's scrapes, oldest first, from the endpoint they came from.
#[derive(Debug, Default)]
struct Window {
    url: String,
    samples: VecDeque<(u64, EngineSample)>,
}

impl Window {
    /// Add a scrape taken at `at` from `url`. A different endpoint (the
    /// deployment moved) starts the window again; scrapes older than
    /// [`KEPT_MS`] fall off.
    fn record(&mut self, url: &str, at: u64, sample: EngineSample) {
        if self.url != url {
            self.url = url.to_string();
            self.samples.clear();
        }
        self.samples.push_back((at, sample));
        while self
            .samples
            .front()
            .is_some_and(|(first, _)| at.saturating_sub(*first) > KEPT_MS)
        {
            self.samples.pop_front();
        }
    }

    fn summary(&self) -> EngineSummary {
        let samples: Vec<_> = self.samples.iter().cloned().collect();
        metrics::summarize(&samples, RATE_WINDOW_MS)
    }
}

static WINDOWS: Mutex<BTreeMap<DeploymentId, Window>> = Mutex::new(BTreeMap::new());

fn windows() -> MutexGuard<'static, BTreeMap<DeploymentId, Window>> {
    WINDOWS.lock().unwrap_or_else(|error| error.into_inner())
}

/// What each scraped deployment's engine says now, over its window.
pub fn summaries() -> BTreeMap<DeploymentId, EngineSummary> {
    windows()
        .iter()
        .filter(|(_, window)| !window.samples.is_empty())
        .map(|(id, window)| (id.clone(), window.summary()))
        .collect()
}

/// Forget every deployment not in `healthy`.
fn retain(healthy: &BTreeSet<DeploymentId>) {
    windows().retain(|id, _| healthy.contains(id));
}

async fn scrape_once(orch: &Orchestrator, client: &reqwest::Client) -> Result<(), String> {
    let db = &orch.db.local;
    let inventory = load_inventory(db).await?;
    let healthy: BTreeSet<DeploymentId> = deployment_phases(db)
        .await?
        .into_iter()
        .filter(|(_, phase)| matches!(phase, Phase::Healthy { .. }))
        .map(|(id, _)| id)
        .collect();
    retain(&healthy);
    for id in &healthy {
        let Some(deployment) = inventory.deployments.get(id) else {
            continue;
        };
        let Some(address) = inventory.address_of(&deployment.head) else {
            continue;
        };
        let url = deployment.metrics_url(address);
        let body = match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => response.text().await.ok(),
            _ => None,
        };
        // An engine with no metrics (llama.cpp without `--metrics`) says
        // nothing and keeps no window.
        let Some(sample) = body
            .map(|body| metrics::parse(&body))
            .filter(|s| s.dialect.is_some())
        else {
            continue;
        };
        windows()
            .entry(id.clone())
            .or_default()
            .record(&url, crate::fleet::unix_time_ms(), sample);
    }
    Ok(())
}

pub fn spawn_engine_metrics(orch: Orchestrator) {
    tokio::spawn(async move {
        let Ok(client) = reqwest::Client::builder().timeout(SCRAPE_TIMEOUT).build() else {
            log::warn!("inference: no HTTP client for engine metrics");
            return;
        };
        loop {
            if let Err(error) = scrape_once(&orch, &client).await {
                log::debug!("inference: engine metrics scrape failed: {error}");
            }
            tokio::time::sleep(SCRAPE_EVERY).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(prompt: f64) -> EngineSample {
        metrics::parse(&format!("llamacpp:prompt_tokens_total {prompt}\n"))
    }

    #[test]
    fn a_window_keeps_ten_minutes_and_restarts_when_the_endpoint_moves() {
        let mut window = Window::default();
        window.record("http://a/metrics", 0, sample(0.0));
        window.record("http://a/metrics", 300_000, sample(300.0));
        window.record("http://a/metrics", 660_000, sample(660.0));
        let kept: Vec<u64> = window.samples.iter().map(|(at, _)| *at).collect();
        assert_eq!(kept, [300_000, 660_000]);
        assert_eq!(window.summary().prompt_tokens_per_sec, Some(1.0));

        window.record("http://b/metrics", 670_000, sample(5.0));
        assert_eq!(window.samples.len(), 1);
        assert_eq!(window.summary().prompt_tokens_per_sec, None);
    }
}
