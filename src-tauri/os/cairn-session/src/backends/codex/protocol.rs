use crate::transcripts::stream::{ActiveMessageStream, StreamAccumulator};

pub struct StreamingState {
    pub stream_id: String,
    pub run_id: String,
    pub version: i32,
    pub acc: StreamAccumulator,
}

impl StreamingState {
    pub fn new(stream: &ActiveMessageStream, run_id: &str) -> Self {
        Self {
            stream_id: stream.stream.id.clone(),
            run_id: run_id.to_string(),
            version: stream.stream.version,
            acc: StreamAccumulator::new(),
        }
    }
}
