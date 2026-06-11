use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use axum::{
    Json, Router,
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use epoll_router::protocol::{
    CapabilityDelta, CapabilityDescriptor, CapabilityError, CapabilityFinish, CapabilityJob,
    ChatMessage, Envelope, InferenceJob, JobDelta, JobError, JobFinish, ModelCapability, Sampling,
    WorkerPayload, WorkerRegister, WorkerTelemetry,
};
use futures_util::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    inner: Arc<Mutex<RouterState>>,
    client_keys: Arc<HashMap<String, String>>,
    worker_keys: Arc<HashMap<String, String>>,
}

#[derive(Default)]
struct RouterState {
    workers: HashMap<String, ConnectedWorker>,
    pending_jobs: HashMap<String, PendingJob>,
}

struct ConnectedWorker {
    worker_id: String,
    models: Vec<ModelCapability>,
    capabilities: Vec<CapabilityDescriptor>,
    max_inflight: usize,
    active_jobs: usize,
    healthy: bool,
    tx: mpsc::Sender<Envelope<WorkerPayload>>,
}

struct PendingJob {
    worker_id: String,
    tx: mpsc::Sender<RouterEvent>,
}

#[derive(Debug)]
enum RouterEvent {
    Delta(String),
    Data(Value),
    Done,
    Output(Value),
    Error(String),
}

#[derive(Clone, Copy)]
enum SseFormat {
    Chat,
    Capability,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    #[serde(default)]
    temperature: Option<f32>,
    #[serde(default)]
    top_p: Option<f32>,
    #[serde(default)]
    max_tokens: Option<u32>,
    #[serde(default)]
    stream: bool,
}

#[derive(Debug, Serialize)]
struct ChatCompletionResponse {
    id: String,
    object: String,
    model: String,
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Serialize)]
struct ChatChoice {
    index: usize,
    message: ChatMessage,
    finish_reason: String,
}

#[derive(Debug, Deserialize)]
struct CapabilityRequest {
    #[serde(default)]
    input: Value,
    #[serde(default)]
    stream: bool,
}

#[derive(Debug, Serialize)]
struct CapabilityResponse {
    id: String,
    capability: String,
    output: Value,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "epoll_router=info,tower_http=info".into()),
        )
        .init();

    let state = AppState {
        inner: Arc::new(Mutex::new(RouterState::default())),
        client_keys: Arc::new(parse_keys("CLIENT_KEYS", "local-client:dev-client-token")),
        worker_keys: Arc::new(parse_keys("WORKER_KEYS", "mock-worker:dev-worker-token")),
    };

    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/workers/socket", get(worker_socket))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/capabilities/{capability}", post(capability_request))
        .with_state(state);

    let addr: SocketAddr = std::env::var("ROUTER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3000".to_string())
        .parse()
        .expect("ROUTER_ADDR must be host:port");

    info!(%addr, "router listening");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind router");
    axum::serve(listener, app).await.expect("router failed");
}

async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ChatCompletionRequest>,
) -> impl IntoResponse {
    let Some(client_id) = authenticate(&headers, &state.client_keys) else {
        return (
            StatusCode::UNAUTHORIZED,
            "missing or invalid client bearer token",
        )
            .into_response();
    };

    if request.messages.is_empty() {
        return (StatusCode::BAD_REQUEST, "messages must not be empty").into_response();
    }

    let job_id = format!("job_{}", Uuid::new_v4());
    let job = InferenceJob {
        job_id: job_id.clone(),
        model: request.model.clone(),
        messages: request.messages,
        sampling: Sampling {
            temperature: request.temperature,
            top_p: request.top_p,
            max_tokens: request.max_tokens,
        },
        stream: request.stream,
    };

    let (stream_tx, stream_rx) = mpsc::channel::<RouterEvent>(64);
    match dispatch_job(state.clone(), &job_id, job, stream_tx).await {
        Ok(()) => info!(%client_id, %job_id, "job dispatched"),
        Err(err) => return (StatusCode::SERVICE_UNAVAILABLE, err).into_response(),
    }

    if request.stream {
        Sse::new(SseStream {
            rx: stream_rx,
            format: SseFormat::Chat,
        })
        .keep_alive(KeepAlive::default())
        .into_response()
    } else {
        let content = collect_non_streaming(stream_rx).await;
        Json(ChatCompletionResponse {
            id: job_id,
            object: "chat.completion".to_string(),
            model: request.model,
            choices: vec![ChatChoice {
                index: 0,
                message: ChatMessage {
                    role: "assistant".to_string(),
                    content,
                },
                finish_reason: "stop".to_string(),
            }],
        })
        .into_response()
    }
}

async fn capability_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(capability): Path<String>,
    Json(request): Json<CapabilityRequest>,
) -> impl IntoResponse {
    let Some(client_id) = authenticate(&headers, &state.client_keys) else {
        return (
            StatusCode::UNAUTHORIZED,
            "missing or invalid client bearer token",
        )
            .into_response();
    };

    let job_id = format!("cap_{}", Uuid::new_v4());
    let job = CapabilityJob {
        job_id: job_id.clone(),
        capability: capability.clone(),
        input: request.input,
        stream: request.stream,
    };

    let (stream_tx, stream_rx) = mpsc::channel::<RouterEvent>(64);
    match dispatch_capability(state.clone(), &job_id, job, stream_tx).await {
        Ok(()) => info!(%client_id, %job_id, %capability, "capability job dispatched"),
        Err(err) => return (StatusCode::SERVICE_UNAVAILABLE, err).into_response(),
    }

    if request.stream {
        Sse::new(SseStream {
            rx: stream_rx,
            format: SseFormat::Capability,
        })
        .keep_alive(KeepAlive::default())
        .into_response()
    } else {
        let output = collect_capability_output(stream_rx).await;
        Json(CapabilityResponse {
            id: job_id,
            capability,
            output,
        })
        .into_response()
    }
}

async fn dispatch_job(
    state: AppState,
    job_id: &str,
    job: InferenceJob,
    stream_tx: mpsc::Sender<RouterEvent>,
) -> Result<(), String> {
    let mut guard = state.inner.lock().await;
    let Some(worker) = guard.workers.values_mut().find(|worker| {
        worker.healthy
            && worker.active_jobs < worker.max_inflight
            && worker.models.iter().any(|model| {
                model.id == job.model || model.aliases.iter().any(|alias| alias == &job.model)
            })
    }) else {
        return Err("no healthy worker has capacity for requested model".to_string());
    };

    worker.active_jobs += 1;
    let worker_id = worker.worker_id.clone();
    let tx = worker.tx.clone();
    guard.pending_jobs.insert(
        job_id.to_string(),
        PendingJob {
            worker_id,
            tx: stream_tx,
        },
    );
    drop(guard);

    let envelope = Envelope::new(
        "job.start",
        job_id.to_string(),
        WorkerPayload::JobStart(job),
    );
    if tx.send(envelope).await.is_err() {
        let mut guard = state.inner.lock().await;
        guard.pending_jobs.remove(job_id);
        return Err("selected worker disconnected before dispatch".to_string());
    }

    Ok(())
}

async fn dispatch_capability(
    state: AppState,
    job_id: &str,
    job: CapabilityJob,
    stream_tx: mpsc::Sender<RouterEvent>,
) -> Result<(), String> {
    let mut guard = state.inner.lock().await;
    let Some(worker) = guard.workers.values_mut().find(|worker| {
        worker.healthy
            && worker.active_jobs < worker.max_inflight
            && worker
                .capabilities
                .iter()
                .any(|capability| capability.name == job.capability)
    }) else {
        return Err("no healthy worker has capacity for requested capability".to_string());
    };

    worker.active_jobs += 1;
    let worker_id = worker.worker_id.clone();
    let tx = worker.tx.clone();
    guard.pending_jobs.insert(
        job_id.to_string(),
        PendingJob {
            worker_id,
            tx: stream_tx,
        },
    );
    drop(guard);

    let envelope = Envelope::new(
        "capability.start",
        job_id.to_string(),
        WorkerPayload::CapabilityStart(job),
    );
    if tx.send(envelope).await.is_err() {
        let mut guard = state.inner.lock().await;
        guard.pending_jobs.remove(job_id);
        return Err("selected worker disconnected before dispatch".to_string());
    }

    Ok(())
}

async fn collect_non_streaming(mut rx: mpsc::Receiver<RouterEvent>) -> String {
    let mut content = String::new();
    while let Some(event) = rx.recv().await {
        match event {
            RouterEvent::Delta(delta) => content.push_str(&delta),
            RouterEvent::Data(data) => content.push_str(&data.to_string()),
            RouterEvent::Done => break,
            RouterEvent::Output(output) => content.push_str(&output.to_string()),
            RouterEvent::Error(_) => break,
        }
    }
    content
}

async fn collect_capability_output(mut rx: mpsc::Receiver<RouterEvent>) -> Value {
    let mut deltas = Vec::new();
    while let Some(event) = rx.recv().await {
        match event {
            RouterEvent::Data(data) => deltas.push(data),
            RouterEvent::Output(output) => return output,
            RouterEvent::Done => return Value::Array(deltas),
            RouterEvent::Error(message) => {
                return serde_json::json!({ "error": { "message": message } });
            }
            RouterEvent::Delta(delta) => deltas.push(Value::String(delta)),
        }
    }
    Value::Array(deltas)
}

async fn worker_socket(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let Some(authorized_worker_id) = authenticate(&headers, &state.worker_keys) else {
        return (
            StatusCode::UNAUTHORIZED,
            "missing or invalid worker bearer token",
        )
            .into_response();
    };

    ws.on_upgrade(move |socket| handle_worker_socket(state, authorized_worker_id, socket))
        .into_response()
}

async fn handle_worker_socket(state: AppState, authorized_worker_id: String, socket: WebSocket) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<Envelope<WorkerPayload>>(128);

    let writer = tokio::spawn(async move {
        while let Some(envelope) = out_rx.recv().await {
            match serde_json::to_string(&envelope) {
                Ok(text) => {
                    if ws_tx.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                Err(err) => error!(%err, "failed to serialize worker envelope"),
            }
        }
    });

    let mut registered_worker_id: Option<String> = None;

    while let Some(message) = ws_rx.next().await {
        let Ok(message) = message else {
            break;
        };
        let Message::Text(text) = message else {
            continue;
        };

        let envelope = match serde_json::from_str::<Envelope<WorkerPayload>>(&text) {
            Ok(envelope) => envelope,
            Err(err) => {
                warn!(%err, "invalid worker message");
                continue;
            }
        };

        match (envelope.kind.as_str(), envelope.payload) {
            ("worker.register", WorkerPayload::Register(register)) => {
                if register.worker_id != authorized_worker_id {
                    warn!(
                        worker_id = %register.worker_id,
                        authorized_worker_id = %authorized_worker_id,
                        "worker tried to register as a different identity"
                    );
                    break;
                }
                register_worker(&state, register, out_tx.clone()).await;
                registered_worker_id = Some(authorized_worker_id.clone());
            }
            ("worker.telemetry", WorkerPayload::Telemetry(telemetry)) => {
                if let Some(worker_id) = &registered_worker_id {
                    update_worker_telemetry(&state, worker_id, telemetry).await;
                }
            }
            ("job.delta", WorkerPayload::JobDelta(delta)) => {
                forward_delta(&state, delta).await;
            }
            ("job.finish", WorkerPayload::JobFinish(finish)) => {
                finish_job(&state, finish).await;
            }
            ("job.error", WorkerPayload::JobError(err)) => {
                fail_job(&state, err).await;
            }
            ("capability.delta", WorkerPayload::CapabilityDelta(delta)) => {
                forward_capability_delta(&state, delta).await;
            }
            ("capability.finish", WorkerPayload::CapabilityFinish(finish)) => {
                finish_capability(&state, finish).await;
            }
            ("capability.error", WorkerPayload::CapabilityError(err)) => {
                fail_capability(&state, err).await;
            }
            _ => warn!(kind = %envelope.kind, "unexpected worker message"),
        }
    }

    writer.abort();
    if let Some(worker_id) = registered_worker_id {
        remove_worker(&state, &worker_id).await;
    }
}

async fn register_worker(
    state: &AppState,
    register: WorkerRegister,
    tx: mpsc::Sender<Envelope<WorkerPayload>>,
) {
    let mut guard = state.inner.lock().await;
    info!(
        worker_id = %register.worker_id,
        models = register.models.len(),
        capabilities = register.capabilities.len(),
        max_inflight = register.max_inflight,
        "worker registered"
    );
    guard.workers.insert(
        register.worker_id.clone(),
        ConnectedWorker {
            worker_id: register.worker_id,
            models: register.models,
            capabilities: register.capabilities,
            max_inflight: register.max_inflight.max(1),
            active_jobs: 0,
            healthy: true,
            tx,
        },
    );
}

async fn forward_capability_delta(state: &AppState, delta: CapabilityDelta) {
    let tx = {
        let guard = state.inner.lock().await;
        guard
            .pending_jobs
            .get(&delta.job_id)
            .map(|pending| pending.tx.clone())
    };
    if let Some(tx) = tx {
        let _ = tx.send(RouterEvent::Data(delta.data)).await;
    }
}

async fn finish_capability(state: &AppState, finish: CapabilityFinish) {
    let pending = {
        let mut guard = state.inner.lock().await;
        guard.pending_jobs.remove(&finish.job_id)
    };
    if let Some(pending) = pending {
        decrement_worker_active(state, &pending.worker_id).await;
        let _ = pending.tx.send(RouterEvent::Output(finish.output)).await;
        let _ = pending.tx.send(RouterEvent::Done).await;
    }
}

async fn fail_capability(state: &AppState, err: CapabilityError) {
    let pending = {
        let mut guard = state.inner.lock().await;
        guard.pending_jobs.remove(&err.job_id)
    };
    if let Some(pending) = pending {
        decrement_worker_active(state, &pending.worker_id).await;
        let _ = pending.tx.send(RouterEvent::Error(err.message)).await;
    }
}

async fn update_worker_telemetry(state: &AppState, worker_id: &str, telemetry: WorkerTelemetry) {
    let mut guard = state.inner.lock().await;
    if let Some(worker) = guard.workers.get_mut(worker_id) {
        worker.active_jobs = telemetry.active_jobs;
        worker.healthy = telemetry.healthy;
    }
}

async fn forward_delta(state: &AppState, delta: JobDelta) {
    let tx = {
        let guard = state.inner.lock().await;
        guard
            .pending_jobs
            .get(&delta.job_id)
            .map(|pending| pending.tx.clone())
    };
    if let Some(tx) = tx {
        let _ = tx.send(RouterEvent::Delta(delta.content)).await;
    }
}

async fn finish_job(state: &AppState, finish: JobFinish) {
    let pending = {
        let mut guard = state.inner.lock().await;
        guard.pending_jobs.remove(&finish.job_id)
    };
    if let Some(pending) = pending {
        decrement_worker_active(state, &pending.worker_id).await;
        let _ = pending.tx.send(RouterEvent::Done).await;
    }
}

async fn fail_job(state: &AppState, err: JobError) {
    let pending = {
        let mut guard = state.inner.lock().await;
        guard.pending_jobs.remove(&err.job_id)
    };
    if let Some(pending) = pending {
        decrement_worker_active(state, &pending.worker_id).await;
        let _ = pending.tx.send(RouterEvent::Error(err.message)).await;
    }
}

async fn decrement_worker_active(state: &AppState, worker_id: &str) {
    let mut guard = state.inner.lock().await;
    if let Some(worker) = guard.workers.get_mut(worker_id) {
        worker.active_jobs = worker.active_jobs.saturating_sub(1);
    }
}

async fn remove_worker(state: &AppState, worker_id: &str) {
    let mut guard = state.inner.lock().await;
    if let Some(worker) = guard.workers.remove(worker_id) {
        warn!(worker_id = %worker.worker_id, "worker disconnected");
    }
}

fn authenticate(headers: &HeaderMap, keys: &HashMap<String, String>) -> Option<String> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    let token = auth.strip_prefix("Bearer ")?;
    keys.iter()
        .find_map(|(id, configured_token)| (configured_token == token).then(|| id.clone()))
}

fn parse_keys(var: &str, default: &str) -> HashMap<String, String> {
    std::env::var(var)
        .unwrap_or_else(|_| default.to_string())
        .split(',')
        .filter_map(|entry| {
            let (id, token) = entry.split_once(':')?;
            Some((id.trim().to_string(), token.trim().to_string()))
        })
        .collect()
}

struct SseStream {
    rx: mpsc::Receiver<RouterEvent>,
    format: SseFormat,
}

impl Stream for SseStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(RouterEvent::Delta(content))) => {
                let data = match self.format {
                    SseFormat::Chat => serde_json::json!({
                        "choices": [{
                            "delta": { "content": content }
                        }]
                    }),
                    SseFormat::Capability => serde_json::json!({
                        "delta": content
                    }),
                };
                Poll::Ready(Some(Ok(Event::default().data(data.to_string()))))
            }
            Poll::Ready(Some(RouterEvent::Data(data))) => {
                Poll::Ready(Some(Ok(Event::default().data(data.to_string()))))
            }
            Poll::Ready(Some(RouterEvent::Done)) => {
                Poll::Ready(Some(Ok(Event::default().data("[DONE]"))))
            }
            Poll::Ready(Some(RouterEvent::Output(output))) => {
                Poll::Ready(Some(Ok(Event::default()
                    .event("output")
                    .data(output.to_string()))))
            }
            Poll::Ready(Some(RouterEvent::Error(message))) => {
                let data = serde_json::json!({ "error": { "message": message } });
                Poll::Ready(Some(Ok(Event::default()
                    .event("error")
                    .data(data.to_string()))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
