use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<T> {
    pub v: u16,
    #[serde(rename = "type")]
    pub kind: String,
    pub id: String,
    pub payload: T,
}

impl<T> Envelope<T> {
    pub fn new(kind: impl Into<String>, id: impl Into<String>, payload: T) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            kind: kind.into(),
            id: id.into(),
            payload,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WorkerPayload {
    Register(WorkerRegister),
    Telemetry(WorkerTelemetry),
    JobStart(InferenceJob),
    JobDelta(JobDelta),
    JobFinish(JobFinish),
    JobError(JobError),
    Empty {},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegister {
    pub worker_id: String,
    pub models: Vec<ModelCapability>,
    pub max_inflight: usize,
    pub engine: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelCapability {
    pub id: String,
    pub aliases: Vec<String>,
    pub max_context_tokens: usize,
    pub supports_streaming: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerTelemetry {
    pub active_jobs: usize,
    pub queued_jobs: usize,
    pub tokens_per_second: Option<f64>,
    pub healthy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceJob {
    pub job_id: String,
    pub model: String,
    pub messages: Vec<ChatMessage>,
    pub sampling: Sampling,
    pub stream: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sampling {
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub max_tokens: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDelta {
    pub job_id: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobFinish {
    pub job_id: String,
    pub finish_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobError {
    pub job_id: String,
    pub message: String,
}
