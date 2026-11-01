//! What an engine says about itself in its Prometheus metrics (`/metrics`).
//!
//! One reader for both engines Cairn runs. vLLM names its series `vllm:*` and
//! llama.cpp's server `llamacpp:*` (and serves them only when started with
//! `--metrics`), so the dialect is read from the series themselves, never
//! from a recipe. [`parse`] reads one scrape into an [`EngineSample`];
//! [`summarize`] turns a window of scrapes into the rates and latencies a
//! person reads: counters become rates by the difference between two scrapes
//! (a counter that went down means the engine restarted, and says nothing),
//! and histograms become p50 and p95 over the observations between them.
//!
//! A series with several label sets (one per engine, per model, per finish
//! reason) is summed, except a usage fraction, which is the fullest one.

use serde::Serialize;

/// Whose metric names a scrape uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Dialect {
    Vllm,
    LlamaCpp,
}

/// A cumulative histogram: each bucket's upper bound and how many
/// observations fell at or below it, in ascending order, the last `+Inf`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Histogram {
    pub buckets: Vec<(f64, f64)>,
    pub count: f64,
    pub sum: f64,
}

impl Histogram {
    /// The observations made after `earlier`: `None` when the engine restarted
    /// between the two (a count went down) or the buckets differ.
    pub fn since(&self, earlier: &Histogram) -> Option<Histogram> {
        if self.buckets.len() != earlier.buckets.len() || self.count < earlier.count {
            return None;
        }
        let mut buckets = Vec::with_capacity(self.buckets.len());
        for ((le, now), (was_le, was)) in self.buckets.iter().zip(&earlier.buckets) {
            if le != was_le || now < was {
                return None;
            }
            buckets.push((*le, now - was));
        }
        Some(Histogram {
            buckets,
            count: self.count - earlier.count,
            sum: self.sum - earlier.sum,
        })
    }

    /// The `q` quantile (0 to 1), interpolated within its bucket as
    /// Prometheus's `histogram_quantile` does; `None` with no observations.
    /// One that falls in the `+Inf` bucket is the highest finite bound.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        let total = self.buckets.last()?.1;
        if total <= 0.0 {
            return None;
        }
        let rank = q.clamp(0.0, 1.0) * total;
        let mut lower = (0.0, 0.0);
        for &(le, cumulative) in &self.buckets {
            if cumulative >= rank {
                if le.is_infinite() {
                    return Some(lower.0);
                }
                let within = cumulative - lower.1;
                if within <= 0.0 {
                    return Some(le);
                }
                return Some(lower.0 + (le - lower.0) * (rank - lower.1) / within);
            }
            lower = (le, cumulative);
        }
        Some(lower.0)
    }

    /// The mean observation, `None` with none.
    pub fn mean(&self) -> Option<f64> {
        (self.count > 0.0).then(|| self.sum / self.count)
    }
}

/// One scrape of an engine's metrics. Every field is `None` when the engine
/// does not report it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EngineSample {
    pub dialect: Option<Dialect>,
    pub requests_running: Option<f64>,
    pub requests_waiting: Option<f64>,
    /// Requests finished since the engine started (vLLM only).
    pub requests_finished: Option<f64>,
    pub prompt_tokens: Option<f64>,
    pub generated_tokens: Option<f64>,
    /// How full the KV cache is, 0 to 1.
    pub kv_cache_usage: Option<f64>,
    /// How many tokens the KV cache holds (vLLM's `kv_cache_size_tokens`).
    pub kv_cache_tokens: Option<u64>,
    pub time_to_first_token: Option<Histogram>,
    pub request_latency: Option<Histogram>,
    pub time_per_output_token: Option<Histogram>,
}

/// One series line: its name, its raw label set, and its value.
struct Series<'a> {
    name: &'a str,
    labels: &'a str,
    value: f64,
}

fn series(text: &str) -> Vec<Series<'_>> {
    text.lines().filter_map(series_line).collect()
}

fn series_line(line: &str) -> Option<Series<'_>> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let name_end = line.find(|c: char| c == '{' || c.is_whitespace())?;
    let name = &line[..name_end];
    let (labels, rest) = if line[name_end..].starts_with('{') {
        let close = closing_brace(&line[name_end + 1..])?;
        (
            &line[name_end + 1..name_end + 1 + close],
            &line[name_end + 2 + close..],
        )
    } else {
        ("", &line[name_end..])
    };
    let value = rest.split_whitespace().next()?;
    let value = match value {
        "+Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        other => other.parse().ok()?,
    };
    Some(Series {
        name,
        labels,
        value,
    })
}

/// The index of the brace closing a label set, quotes and escapes respected.
fn closing_brace(labels: &str) -> Option<usize> {
    let mut quoted = false;
    let mut chars = labels.char_indices();
    while let Some((index, c)) = chars.next() {
        match c {
            '\\' if quoted => {
                chars.next();
            }
            '"' => quoted = !quoted,
            '}' if !quoted => return Some(index),
            _ => {}
        }
    }
    None
}

/// The value of `key` in a raw label set, quotes and escapes respected.
fn label(labels: &str, key: &str) -> Option<String> {
    let mut rest = labels;
    loop {
        rest = rest.trim_start_matches([',', ' ']);
        if rest.is_empty() {
            return None;
        }
        let (name, after) = rest.split_once('=')?;
        let after = after.strip_prefix('"')?;
        let mut value = String::new();
        let mut chars = after.char_indices();
        let end = loop {
            match chars.next()? {
                (_, '\\') => value.push(chars.next()?.1),
                (index, '"') => break index,
                (_, c) => value.push(c),
            }
        };
        if name.trim() == key {
            return Some(value);
        }
        rest = &after[end + 1..];
    }
}

fn finite(series: &[Series<'_>], name: &str) -> impl Iterator<Item = f64> + Clone {
    let name = name.to_string();
    series
        .iter()
        .filter(move |s| s.name == name)
        .map(|s| s.value)
        .filter(|v| v.is_finite())
        .collect::<Vec<_>>()
        .into_iter()
}

fn summed(series: &[Series<'_>], name: &str) -> Option<f64> {
    let mut values = finite(series, name).peekable();
    values.peek()?;
    Some(values.sum())
}

fn fullest(series: &[Series<'_>], name: &str) -> Option<f64> {
    finite(series, name).reduce(f64::max)
}

fn histogram(series: &[Series<'_>], base: &str) -> Option<Histogram> {
    let bucket = format!("{base}_bucket");
    let mut buckets: Vec<(f64, f64)> = Vec::new();
    for s in series.iter().filter(|s| s.name == bucket) {
        let le = match label(s.labels, "le")?.as_str() {
            "+Inf" => f64::INFINITY,
            bound => bound.parse().ok()?,
        };
        match buckets.iter_mut().find(|(known, _)| *known == le) {
            Some((_, count)) => *count += s.value,
            None => buckets.push((le, s.value)),
        }
    }
    if buckets.is_empty() {
        return None;
    }
    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    Some(Histogram {
        count: summed(series, &format!("{base}_count")).unwrap_or(buckets.last()?.1),
        sum: summed(series, &format!("{base}_sum")).unwrap_or(0.0),
        buckets,
    })
}

/// The KV cache size an engine reports, which vLLM gives as the
/// `kv_cache_size_tokens` label of its `vllm:cache_config_info` gauge. With
/// several engines (one series per `engine` label) it is the smallest, since a
/// request lands on one of them. `None` when no series reports it.
///
/// Nothing is derived in its absence. `num_gpu_blocks` × `block_size` beside
/// it on the DeepSeek V4 Flash build gives 64,824 tokens against a reported
/// 1,447,093, so the product does not measure the cache on every engine.
pub fn kv_cache_tokens(metrics: &str) -> Option<u64> {
    series(metrics)
        .iter()
        .filter(|s| s.name == "vllm:cache_config_info")
        .filter_map(|s| label(s.labels, "kv_cache_size_tokens")?.parse::<u64>().ok())
        .min()
}

/// Read one scrape.
pub fn parse(text: &str) -> EngineSample {
    let all = series(text);
    let dialect = if all.iter().any(|s| s.name.starts_with("vllm:")) {
        Some(Dialect::Vllm)
    } else if all.iter().any(|s| s.name.starts_with("llamacpp:")) {
        Some(Dialect::LlamaCpp)
    } else {
        None
    };
    let mut sample = EngineSample {
        dialect,
        kv_cache_tokens: kv_cache_tokens(text),
        ..EngineSample::default()
    };
    match dialect {
        Some(Dialect::Vllm) => {
            sample.requests_running = summed(&all, "vllm:num_requests_running");
            sample.requests_waiting = summed(&all, "vllm:num_requests_waiting");
            sample.requests_finished = summed(&all, "vllm:request_success_total");
            sample.prompt_tokens = summed(&all, "vllm:prompt_tokens_total");
            sample.generated_tokens = summed(&all, "vllm:generation_tokens_total");
            sample.kv_cache_usage = fullest(&all, "vllm:kv_cache_usage_perc")
                .or_else(|| fullest(&all, "vllm:gpu_cache_usage_perc"));
            sample.time_to_first_token = histogram(&all, "vllm:time_to_first_token_seconds");
            sample.request_latency = histogram(&all, "vllm:e2e_request_latency_seconds");
            sample.time_per_output_token = [
                "vllm:time_per_output_token_seconds",
                "vllm:inter_token_latency_seconds",
                "vllm:request_time_per_output_token_seconds",
            ]
            .iter()
            .find_map(|base| histogram(&all, base));
        }
        Some(Dialect::LlamaCpp) => {
            sample.requests_running = summed(&all, "llamacpp:requests_processing");
            sample.requests_waiting = summed(&all, "llamacpp:requests_deferred");
            sample.prompt_tokens = summed(&all, "llamacpp:prompt_tokens_total");
            sample.generated_tokens = summed(&all, "llamacpp:tokens_predicted_total");
            sample.kv_cache_usage = fullest(&all, "llamacpp:kv_cache_usage_ratio");
        }
        None => {}
    }
    sample
}

/// What a window of scrapes says, as a person reads it.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineSummary {
    pub requests_running: Option<f64>,
    pub requests_waiting: Option<f64>,
    pub kv_cache_usage: Option<f64>,
    pub requests_per_min: Option<f64>,
    pub prompt_tokens_per_sec: Option<f64>,
    pub generated_tokens_per_sec: Option<f64>,
    pub time_to_first_token_p50: Option<f64>,
    pub time_to_first_token_p95: Option<f64>,
    pub latency_p50: Option<f64>,
    pub latency_p95: Option<f64>,
    /// One over the mean time per output token across the window: how fast it
    /// decoded while it was decoding.
    pub decode_tokens_per_sec: Option<f64>,
}

fn per_sec(earlier: Option<f64>, later: Option<f64>, secs: f64) -> Option<f64> {
    let (earlier, later) = (earlier?, later?);
    (later >= earlier && secs > 0.0).then(|| (later - earlier) / secs)
}

/// Summarize `samples` (scrape time in ms, oldest first). Rates come from the
/// newest scrape and the newest one at least `rate_window_ms` older (else the
/// oldest); latencies from every observation since the oldest.
pub fn summarize(samples: &[(u64, EngineSample)], rate_window_ms: u64) -> EngineSummary {
    let Some((latest_at, latest)) = samples.last() else {
        return EngineSummary::default();
    };
    let mut summary = EngineSummary {
        requests_running: latest.requests_running,
        requests_waiting: latest.requests_waiting,
        kv_cache_usage: latest.kv_cache_usage,
        ..EngineSummary::default()
    };
    let earlier = &samples[..samples.len() - 1];
    let base = earlier
        .iter()
        .rev()
        .find(|(at, _)| latest_at.saturating_sub(*at) >= rate_window_ms)
        .or_else(|| earlier.first());
    if let Some((base_at, base)) = base {
        let secs = latest_at.saturating_sub(*base_at) as f64 / 1000.0;
        summary.requests_per_min =
            per_sec(base.requests_finished, latest.requests_finished, secs).map(|r| r * 60.0);
        summary.prompt_tokens_per_sec = per_sec(base.prompt_tokens, latest.prompt_tokens, secs);
        summary.generated_tokens_per_sec =
            per_sec(base.generated_tokens, latest.generated_tokens, secs);
    }
    if let Some((_, oldest)) = earlier.first() {
        let window = |of: fn(&EngineSample) -> Option<&Histogram>| of(latest)?.since(of(oldest)?);
        if let Some(ttft) = window(|s| s.time_to_first_token.as_ref()) {
            summary.time_to_first_token_p50 = ttft.quantile(0.5);
            summary.time_to_first_token_p95 = ttft.quantile(0.95);
        }
        if let Some(latency) = window(|s| s.request_latency.as_ref()) {
            summary.latency_p50 = latency.quantile(0.5);
            summary.latency_p95 = latency.quantile(0.95);
        }
        summary.decode_tokens_per_sec = window(|s| s.time_per_output_token.as_ref())
            .and_then(|tpot| tpot.mean())
            .filter(|mean| *mean > 0.0)
            .map(|mean| 1.0 / mean);
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The live `vllm:cache_config_info` series of DeepSeek V4 Flash on the
    /// Sparks, read 2026-10-31.
    const DSV4_CACHE_CONFIG: &str = r#"# HELP vllm:cache_config_info Information of the LLMEngine CacheConfig
# TYPE vllm:cache_config_info gauge
vllm:cache_config_info{_block_size_resolved="True",block_size="4",cache_dtype="nvfp4_ds_mla",calculate_kv_scales="False",enable_prefix_caching="True",engine="0",gpu_memory_utilization="0.835",hash_block_size="None",is_attention_free="False",kv_cache_dtype_skip_layers="[]",kv_cache_max_concurrency="1.3800562036958188",kv_cache_memory_bytes="None",kv_cache_size_tokens="1447093",kv_offloading_backend="native",kv_offloading_size="None",kv_sharing_fast_prefill="False",mamba_block_size="None",mamba_cache_dtype="auto",mamba_cache_mode="none",mamba_page_size_padded="None",mamba_ssm_cache_dtype="auto",num_cpu_blocks="None",num_gpu_blocks="16206",num_gpu_blocks_override="None",prefix_caching_hash_algo="sha256",skip_page_size_padded="None",sliding_window="128",user_specified_block_size="True",user_specified_mamba_block_size="False"} 1.0
vllm:num_requests_running{engine="0",model_name="deepseek-v4-flash-vision-exp"} 0.0
"#;

    /// vLLM's series as its v1 engine exposes them, shaped from its metric
    /// definitions: not yet a capture, since DeepSeek was stopped when this was
    /// written. Two scrapes a minute apart.
    fn vllm(finished: f64, prompt: f64, generated: f64, ttft: [f64; 4], tpot: [f64; 3]) -> String {
        format!(
            r#"# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{{engine="0",model_name="deepseek-v4-flash-vision-exp"}} 2.0
vllm:num_requests_waiting{{engine="0",model_name="deepseek-v4-flash-vision-exp"}} 1.0
vllm:kv_cache_usage_perc{{engine="0",model_name="deepseek-v4-flash-vision-exp"}} 0.25
vllm:request_success_total{{engine="0",finished_reason="stop",model_name="m"}} {finished}
vllm:request_success_total{{engine="0",finished_reason="length",model_name="m"}} 1.0
vllm:prompt_tokens_total{{engine="0",model_name="m"}} {prompt}
vllm:prompt_tokens_created{{engine="0",model_name="m"}} 1.7934706e+09
vllm:generation_tokens_total{{engine="0",model_name="m"}} {generated}
vllm:time_to_first_token_seconds_bucket{{engine="0",le="0.1",model_name="m"}} {}
vllm:time_to_first_token_seconds_bucket{{engine="0",le="0.5",model_name="m"}} {}
vllm:time_to_first_token_seconds_bucket{{engine="0",le="1.0",model_name="m"}} {}
vllm:time_to_first_token_seconds_bucket{{engine="0",le="+Inf",model_name="m"}} {}
vllm:time_to_first_token_seconds_count{{engine="0",model_name="m"}} {}
vllm:time_to_first_token_seconds_sum{{engine="0",model_name="m"}} 0.0
vllm:time_per_output_token_seconds_bucket{{engine="0",le="0.02",model_name="m"}} {}
vllm:time_per_output_token_seconds_bucket{{engine="0",le="+Inf",model_name="m"}} {}
vllm:time_per_output_token_seconds_count{{engine="0",model_name="m"}} {}
vllm:time_per_output_token_seconds_sum{{engine="0",model_name="m"}} {}
"#,
            ttft[0], ttft[1], ttft[2], ttft[3], ttft[3], tpot[0], tpot[1], tpot[1], tpot[2]
        )
    }

    /// llama.cpp's server series (`--metrics`), shaped from its server source:
    /// it counts tokens but not requests.
    fn llamacpp(prompt: f64, predicted: f64) -> String {
        format!(
            "# HELP llamacpp:prompt_tokens_total Number of prompt tokens processed.\n\
             # TYPE llamacpp:prompt_tokens_total counter\n\
             llamacpp:prompt_tokens_total {prompt}\n\
             llamacpp:prompt_seconds_total 2.5\n\
             llamacpp:tokens_predicted_total {predicted}\n\
             llamacpp:requests_processing 1\n\
             llamacpp:requests_deferred 0\n\
             llamacpp:kv_cache_usage_ratio 0.125\n"
        )
    }

    #[test]
    fn the_kv_cache_is_the_reported_label_and_nothing_derived() {
        assert_eq!(kv_cache_tokens(DSV4_CACHE_CONFIG), Some(1_447_093));
        let derivable_only =
            "vllm:cache_config_info{block_size=\"4\",num_gpu_blocks=\"16206\"} 1.0\n";
        assert_eq!(kv_cache_tokens(derivable_only), None);
        assert_eq!(kv_cache_tokens(""), None);
        assert_eq!(kv_cache_tokens("not found"), None);
    }

    #[test]
    fn of_several_engines_the_smallest_cache_counts() {
        let two = "vllm:cache_config_info{engine=\"0\",kv_cache_size_tokens=\"900\"} 1.0\n\
                   vllm:cache_config_info{engine=\"1\",kv_cache_size_tokens=\"700\"} 1.0\n";
        assert_eq!(kv_cache_tokens(two), Some(700));
    }

    #[test]
    fn a_quoted_value_may_hold_separators_and_escapes() {
        let tricky =
            "vllm:cache_config_info{note=\"a,b}=c \\\"q\\\"\",kv_cache_size_tokens=\"42\"} 1.0";
        assert_eq!(kv_cache_tokens(tricky), Some(42));
    }

    #[test]
    fn a_vllm_scrape_reads_its_gauges_counters_and_histograms() {
        let sample = parse(&vllm(
            9.0,
            1000.0,
            500.0,
            [1.0, 3.0, 4.0, 4.0],
            [10.0, 10.0, 0.2],
        ));
        assert_eq!(sample.dialect, Some(Dialect::Vllm));
        assert_eq!(sample.requests_running, Some(2.0));
        assert_eq!(sample.requests_waiting, Some(1.0));
        // Every finish reason counts.
        assert_eq!(sample.requests_finished, Some(10.0));
        assert_eq!(sample.prompt_tokens, Some(1000.0));
        assert_eq!(sample.generated_tokens, Some(500.0));
        assert_eq!(sample.kv_cache_usage, Some(0.25));
        let ttft = sample.time_to_first_token.unwrap();
        assert_eq!(
            ttft.buckets,
            [(0.1, 1.0), (0.5, 3.0), (1.0, 4.0), (f64::INFINITY, 4.0)]
        );
        assert_eq!(ttft.count, 4.0);
        assert!(sample.request_latency.is_none());
        assert!(sample.time_per_output_token.is_some());
    }

    #[test]
    fn an_older_vllm_names_its_cache_usage_gpu_cache() {
        let sample = parse("vllm:gpu_cache_usage_perc{engine=\"0\"} 0.5\n");
        assert_eq!(sample.kv_cache_usage, Some(0.5));
    }

    #[test]
    fn a_llamacpp_scrape_reads_tokens_and_slots_but_no_requests() {
        let sample = parse(&llamacpp(120.0, 40.0));
        assert_eq!(sample.dialect, Some(Dialect::LlamaCpp));
        assert_eq!(sample.prompt_tokens, Some(120.0));
        assert_eq!(sample.generated_tokens, Some(40.0));
        assert_eq!(sample.requests_running, Some(1.0));
        assert_eq!(sample.requests_waiting, Some(0.0));
        assert_eq!(sample.kv_cache_usage, Some(0.125));
        assert_eq!(sample.requests_finished, None);
    }

    #[test]
    fn something_else_is_no_dialect_and_says_nothing() {
        let sample = parse("process_cpu_seconds_total 3\nhttp_requests_total{code=\"200\"} 5\n");
        assert_eq!(sample, EngineSample::default());
    }

    #[test]
    fn quantiles_interpolate_within_their_bucket() {
        let histogram = Histogram {
            buckets: vec![(0.1, 1.0), (0.5, 3.0), (1.0, 4.0), (f64::INFINITY, 4.0)],
            count: 4.0,
            sum: 1.6,
        };
        // Rank 2 of 4 lies halfway through the (0.1, 0.5] bucket.
        assert!((histogram.quantile(0.5).unwrap() - 0.3).abs() < 1e-9);
        // Rank 3.8 lies 80% through the (0.5, 1.0] bucket.
        assert!((histogram.quantile(0.95).unwrap() - 0.9).abs() < 1e-9);
        assert!((histogram.mean().unwrap() - 0.4).abs() < 1e-9);
        let overflowing = Histogram {
            buckets: vec![(0.1, 0.0), (f64::INFINITY, 2.0)],
            count: 2.0,
            sum: 9.0,
        };
        assert_eq!(overflowing.quantile(0.5), Some(0.1));
        assert_eq!(Histogram::default().quantile(0.5), None);
    }

    #[test]
    fn a_window_summarizes_rates_quantiles_and_decode_speed() {
        let first = parse(&vllm(
            9.0,
            1000.0,
            500.0,
            [1.0, 1.0, 2.0, 2.0],
            [0.0, 0.0, 0.0],
        ));
        let middle = parse(&vllm(
            10.0,
            1300.0,
            900.0,
            [1.0, 2.0, 3.0, 3.0],
            [5.0, 5.0, 0.1],
        ));
        let last = parse(&vllm(
            12.0,
            1600.0,
            1400.0,
            [2.0, 4.0, 5.0, 5.0],
            [40.0, 40.0, 0.8],
        ));
        let samples = vec![(0, first), (30_000, middle), (90_000, last)];
        let summary = summarize(&samples, 60_000);
        // The rate base is the newest scrape a minute or more older: 60 s.
        assert_eq!(summary.requests_per_min, Some(2.0));
        assert_eq!(summary.prompt_tokens_per_sec, Some(5.0));
        assert!((summary.generated_tokens_per_sec.unwrap() - 500.0 / 60.0).abs() < 1e-9);
        assert_eq!(summary.requests_running, Some(2.0));
        assert_eq!(summary.kv_cache_usage, Some(0.25));
        // Latencies span the whole window: three first tokens, one under
        // 0.1 s and two between 0.1 and 0.5 s. Rank 1.5 of 3 is a quarter of
        // the way through that bucket, rank 2.85 most of the way.
        assert!((summary.time_to_first_token_p50.unwrap() - 0.2).abs() < 1e-9);
        assert!((summary.time_to_first_token_p95.unwrap() - 0.47).abs() < 1e-9);
        assert_eq!(summary.latency_p50, None);
        // 40 output tokens in 0.8 s: 50 tokens per second.
        assert!((summary.decode_tokens_per_sec.unwrap() - 50.0).abs() < 1e-9);
    }

    #[test]
    fn a_restarted_engine_and_a_lone_scrape_give_no_rates() {
        let before = parse(&llamacpp(1000.0, 400.0));
        let after = parse(&llamacpp(10.0, 4.0));
        let restarted = summarize(&[(0, before), (60_000, after.clone())], 60_000);
        assert_eq!(restarted.prompt_tokens_per_sec, None);
        assert_eq!(restarted.generated_tokens_per_sec, None);
        let lone = summarize(&[(0, after)], 60_000);
        assert_eq!(lone.prompt_tokens_per_sec, None);
        assert_eq!(lone.requests_running, Some(1.0));
        // llama.cpp counts no requests, so it has no request rate.
        let steady = summarize(
            &[
                (0, parse(&llamacpp(0.0, 0.0))),
                (10_000, parse(&llamacpp(50.0, 20.0))),
            ],
            60_000,
        );
        assert_eq!(steady.requests_per_min, None);
        assert_eq!(steady.prompt_tokens_per_sec, Some(5.0));
        assert_eq!(steady.generated_tokens_per_sec, Some(2.0));
    }
}
