//! The pure half of verifying a profile on a pool: what a benchmark's samples
//! measure, what a failed start says about the KV cache, how a profile's
//! context is clamped to it, and the canonical form a verification pins a
//! profile by. The host runs the load, the health check and the requests.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::profile::{Engine, InferenceProfile};

/// How far a verification got.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stage {
    Render,
    Start,
    Health,
    Benchmark,
    Done,
}

impl Stage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Render => "render",
            Self::Start => "start",
            Self::Health => "health",
            Self::Benchmark => "benchmark",
            Self::Done => "done",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        [
            Self::Render,
            Self::Start,
            Self::Health,
            Self::Benchmark,
            Self::Done,
        ]
        .into_iter()
        .find(|stage| stage.as_str() == value)
    }
}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One streamed chat request, timed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChatSample {
    /// Until the first generated token arrived.
    pub ttft_ms: f64,
    /// Until the response ended.
    pub total_ms: f64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

/// One embeddings request, timed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmbedSample {
    pub total_ms: f64,
    pub tokens: u64,
}

/// What a benchmark measured: medians over its samples.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Speeds {
    pub ttft_ms: Option<f64>,
    pub decode_tps: Option<f64>,
    pub prefill_tps: Option<f64>,
    pub embed_tps: Option<f64>,
}

pub fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let mid = values.len() / 2;
    Some(if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2.0
    })
}

/// The chat speeds, or why the samples measure nothing: an engine that
/// generated no tokens has not passed.
pub fn chat_speeds(samples: &[ChatSample]) -> Result<Speeds, String> {
    if samples.is_empty() {
        return Err("no chat request completed".into());
    }
    if let Some(empty) = samples.iter().find(|s| s.completion_tokens == 0) {
        return Err(format!(
            "a chat request generated no tokens ({:.0} ms, {} prompt tokens)",
            empty.total_ms, empty.prompt_tokens
        ));
    }
    let mut ttft: Vec<f64> = samples.iter().map(|s| s.ttft_ms).collect();
    let mut decode: Vec<f64> = samples
        .iter()
        .filter(|s| s.completion_tokens > 1 && s.total_ms > s.ttft_ms)
        .map(|s| (s.completion_tokens - 1) as f64 / ((s.total_ms - s.ttft_ms) / 1000.0))
        .collect();
    let mut prefill: Vec<f64> = samples
        .iter()
        .filter(|s| s.ttft_ms > 0.0)
        .map(|s| s.prompt_tokens as f64 / (s.ttft_ms / 1000.0))
        .collect();
    Ok(Speeds {
        ttft_ms: median(&mut ttft),
        decode_tps: median(&mut decode),
        prefill_tps: median(&mut prefill),
        embed_tps: None,
    })
}

pub fn embed_speeds(samples: &[EmbedSample]) -> Result<Speeds, String> {
    if samples.is_empty() {
        return Err("no embeddings request completed".into());
    }
    let mut tps: Vec<f64> = samples
        .iter()
        .filter(|s| s.total_ms > 0.0)
        .map(|s| s.tokens as f64 / (s.total_ms / 1000.0))
        .collect();
    Ok(Speeds {
        embed_tps: median(&mut tps),
        ..Speeds::default()
    })
}

/// The KV cache size a failed vLLM start names: "... is larger than the
/// maximum number of tokens that can be stored in KV cache (N) ...".
pub fn kv_limit_from_failure(reason: &str) -> Option<u64> {
    let at = reason.find("KV cache (")? + "KV cache (".len();
    let digits: String = reason[at..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

const CONTEXT_FLAGS: &[&str] = &["--max-model-len", "-c", "--ctx-size"];

/// The context a profile's engine is told to hold.
pub fn context_of(profile: &InferenceProfile) -> Option<u64> {
    let Engine::Container(container) = &profile.engine else {
        return None;
    };
    container.args.iter().enumerate().find_map(|(i, arg)| {
        if CONTEXT_FLAGS.contains(&arg.as_str()) {
            return container.args.get(i + 1)?.parse().ok();
        }
        CONTEXT_FLAGS.iter().find_map(|flag| {
            let words: Vec<&str> = arg.split_whitespace().collect();
            let at = words.iter().position(|w| w.trim_matches('\'') == *flag)?;
            words.get(at + 1)?.trim_matches('\'').parse().ok()
        })
    })
}

/// `profile` with its context clamped to `tokens`, floored to a multiple of
/// 1024, or `None` when it states no context to clamp or already fits.
pub fn clamp_context(profile: &InferenceProfile, tokens: u64) -> Option<InferenceProfile> {
    let current = context_of(profile)?;
    let clamped = tokens / 1024 * 1024;
    if clamped >= current || clamped == 0 {
        return None;
    }
    let mut revised = profile.clone();
    let Engine::Container(container) = &mut revised.engine else {
        return None;
    };
    let from = current.to_string();
    let to = clamped.to_string();
    let mut changed = false;
    for i in 0..container.args.len() {
        if CONTEXT_FLAGS.contains(&container.args[i].as_str())
            && container.args.get(i + 1) == Some(&from)
        {
            container.args[i + 1] = to.clone();
            changed = true;
        }
        for flag in CONTEXT_FLAGS {
            for (old, new) in [
                (format!("{flag} {from}"), format!("{flag} {to}")),
                (format!("'{flag}' '{from}'"), format!("'{flag}' '{to}'")),
            ] {
                if container.args[i].contains(&old) {
                    container.args[i] = container.args[i].replace(&old, &new);
                    changed = true;
                }
            }
        }
    }
    changed.then_some(revised)
}

/// The profile as a verification pins it: its canonical JSON, whose sha256 a
/// verification row records. A row whose digest no longer matches its profile
/// measured a profile that has since changed.
pub fn canonical(profile: &InferenceProfile) -> String {
    serde_json::to_string(profile).expect("profiles serialize")
}

/// The fixed prompt of the chat benchmark: about a thousand tokens of plain
/// prose, the same every run so runs compare.
pub fn chat_prompt() -> String {
    let paragraph = "A cairn is a pile of stones raised to mark a path across open ground, where nothing else would show the way. Each traveller who passes adds a stone, so the marks that are used the most grow the tallest and are the easiest to find in fog. ";
    let mut prompt = String::from("Read the passage, then summarise it in one sentence.\n\n");
    for _ in 0..20 {
        prompt.push_str(paragraph);
    }
    prompt
}

/// The fixed texts of the embeddings benchmark: one batch.
pub fn embed_batch() -> Vec<String> {
    (0..32)
        .map(|i| {
            format!("Benchmark sentence {i}: stones mark the path across the moor, one for every traveller who passed this way before.")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::{Container, HostOptions, ModelRef, ProfileOrigin, Requirements, Server};
    use std::collections::BTreeMap;

    fn profile(args: Vec<&str>) -> InferenceProfile {
        InferenceProfile {
            id: "r".into(),
            source: Default::default(),
            model: ModelRef {
                name: "m".into(),
                weights: vec![],
            },
            served_name: "m".into(),
            upstream_id: None,
            verbs: None,
            modalities: None,
            quantization: None,
            origin: ProfileOrigin::Authored,
            engine: Engine::Container(Container {
                image: "x".into(),
                server: Server::Vllm,
                entrypoint: None,
                args: args.into_iter().map(String::from).collect(),
                env: BTreeMap::new(),
                mounts: vec![],
                host: HostOptions::default(),
                files: None,
                ranks: vec![],
            }),
            requirements: Requirements {
                machines: 1,
                accelerator: None,
                interconnect: None,
                memory: None,
            },
            notes: None,
        }
    }

    #[test]
    fn chat_speeds_are_medians_and_empty_output_fails() {
        let sample = |ttft, total, completion| ChatSample {
            ttft_ms: ttft,
            total_ms: total,
            prompt_tokens: 1000,
            completion_tokens: completion,
        };
        let speeds = chat_speeds(&[
            sample(200.0, 5_300.0, 256),
            sample(100.0, 2_650.0, 256),
            sample(400.0, 10_600.0, 256),
        ])
        .unwrap();
        assert_eq!(speeds.ttft_ms, Some(200.0));
        assert_eq!(speeds.decode_tps, Some(50.0));
        assert_eq!(speeds.prefill_tps, Some(5_000.0));
        assert!(chat_speeds(&[sample(100.0, 200.0, 0)])
            .unwrap_err()
            .contains("generated no tokens"));
        assert!(chat_speeds(&[]).is_err());
        assert_eq!(median(&mut [4.0, 1.0, 3.0, 2.0]), Some(2.5));
        let embed = embed_speeds(&[EmbedSample {
            total_ms: 500.0,
            tokens: 1_000,
        }])
        .unwrap();
        assert_eq!(embed.embed_tps, Some(2_000.0));
    }

    #[test]
    fn the_kv_limit_is_read_from_vllms_refusal() {
        let reason = "container cairn-x-r0 exited before it was healthy; its last errors:\nValueError: The model's max seq len (1048576) is larger than the maximum number of tokens that can be stored in KV cache (712448). Try increasing `gpu_memory_utilization` or decreasing `max_model_len` when initializing the engine.";
        assert_eq!(kv_limit_from_failure(reason), Some(712_448));
        assert_eq!(kv_limit_from_failure("CUDA out of memory"), None);
    }

    #[test]
    fn the_context_clamps_in_plain_args_and_inside_a_shell_command() {
        let plain = profile(vec!["vllm", "serve", "m", "--max-model-len", "262144"]);
        assert_eq!(context_of(&plain), Some(262_144));
        let clamped = clamp_context(&plain, 100_000).unwrap();
        assert_eq!(context_of(&clamped), Some(99_328));
        assert_eq!(clamp_context(&plain, 300_000), None, "it already fits");

        let shell = profile(vec![
            "bash",
            "-lc",
            "exec vllm serve {model} --max-model-len 1048576 --block-size 256",
        ]);
        assert_eq!(context_of(&shell), Some(1_048_576));
        let clamped = clamp_context(&shell, 712_448).unwrap();
        let Engine::Container(container) = &clamped.engine else {
            unreachable!()
        };
        assert!(container.args[2].contains("--max-model-len 711680 --block-size"));

        let quoted = profile(vec!["exec 'vllm' '--max-model-len' '40960' --nnodes 2"]);
        let clamped = clamp_context(&quoted, 20_000).unwrap();
        assert_eq!(context_of(&clamped), Some(19_456));

        let llama = profile(vec!["/app/llama-server", "-c", "40960"]);
        assert_eq!(
            context_of(&clamp_context(&llama, 8_192).unwrap()),
            Some(8_192)
        );
        assert_eq!(clamp_context(&profile(vec!["serve"]), 1_000), None);
    }

    #[test]
    fn a_changed_profile_has_a_different_canonical_form() {
        let a = profile(vec!["--max-model-len", "1"]);
        let mut b = a.clone();
        assert_eq!(canonical(&a), canonical(&b));
        b.notes = Some("changed".into());
        assert_ne!(canonical(&a), canonical(&b));
        assert!(chat_prompt().len() > 4_000);
        assert_eq!(embed_batch().len(), 32);
    }
}
