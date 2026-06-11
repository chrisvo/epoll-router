use std::time::Duration;

use epoll_router::protocol::{
    CapabilityDelta, CapabilityDescriptor, CapabilityError, CapabilityFinish, Envelope, JobDelta,
    JobError, JobFinish, ModelCapability, WorkerPayload, WorkerRegister, WorkerTelemetry,
};
use futures_util::{SinkExt, StreamExt};
use tokio::time;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};
use tracing::{error, info, warn};
use uuid::Uuid;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "epoll_router=info".into()),
        )
        .init();

    let worker_id = std::env::var("WORKER_ID").unwrap_or_else(|_| "mock-worker".to_string());
    let token = std::env::var("WORKER_TOKEN").unwrap_or_else(|_| "dev-worker-token".to_string());
    let url = std::env::var("ROUTER_WS_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:3000/workers/socket".to_string());

    loop {
        if let Err(err) = run_worker(&url, &worker_id, &token).await {
            error!(%err, "worker connection failed");
        }
        time::sleep(Duration::from_secs(2)).await;
    }
}

async fn run_worker(
    url: &str,
    worker_id: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut request = url.into_client_request()?;
    request.headers_mut().insert(
        "authorization",
        HeaderValue::from_str(&format!("Bearer {token}"))?,
    );

    let (socket, _) = connect_async(request).await?;
    info!(%url, %worker_id, "connected to router");
    let (mut write, mut read) = socket.split();

    let register = WorkerRegister {
        worker_id: worker_id.to_string(),
        engine: "mock".to_string(),
        max_inflight: 4,
        models: vec![ModelCapability {
            id: "mock-llm".to_string(),
            aliases: vec!["local-default".to_string(), "fast".to_string()],
            max_context_tokens: 8192,
            supports_streaming: true,
        }],
        capabilities: vec![CapabilityDescriptor {
            name: "run_echo".to_string(),
            description: "Echoes the provided input from inside the private worker.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "message": { "type": "string" }
                },
                "required": ["message"]
            }),
            streams: true,
        }],
    };
    send(
        &mut write,
        Envelope::new(
            "worker.register",
            format!("msg_{}", Uuid::new_v4()),
            WorkerPayload::Register(register),
        ),
    )
    .await?;

    let mut telemetry = time::interval(Duration::from_secs(5));
    let mut active_jobs = 0usize;

    loop {
        tokio::select! {
            _ = telemetry.tick() => {
                send(
                    &mut write,
                    Envelope::new(
                        "worker.telemetry",
                        format!("msg_{}", Uuid::new_v4()),
                        WorkerPayload::Telemetry(WorkerTelemetry {
                            active_jobs,
                            queued_jobs: 0,
                            tokens_per_second: Some(42.0),
                            healthy: true,
                        }),
                    ),
                ).await?;
            }
            Some(message) = read.next() => {
                let message = message?;
                let Message::Text(text) = message else {
                    continue;
                };
                let envelope: Envelope<WorkerPayload> = serde_json::from_str(&text)?;
                match (envelope.kind.as_str(), envelope.payload) {
                    ("job.start", WorkerPayload::JobStart(job)) => {
                        active_jobs += 1;
                        info!(job_id = %job.job_id, model = %job.model, "mock job started");
                        let prompt = job
                            .messages
                            .last()
                            .map(|message| message.content.as_str())
                            .unwrap_or("");
                        let response = format!("Mock response to: {prompt}");

                        for token in response.split_inclusive(' ') {
                            send(
                                &mut write,
                                Envelope::new(
                                    "job.delta",
                                    format!("msg_{}", Uuid::new_v4()),
                                    WorkerPayload::JobDelta(JobDelta {
                                        job_id: job.job_id.clone(),
                                        content: token.to_string(),
                                    }),
                                ),
                            ).await?;
                            time::sleep(Duration::from_millis(120)).await;
                        }

                        send(
                            &mut write,
                            Envelope::new(
                                "job.finish",
                                format!("msg_{}", Uuid::new_v4()),
                                WorkerPayload::JobFinish(JobFinish {
                                    job_id: job.job_id,
                                    finish_reason: "stop".to_string(),
                                }),
                            ),
                        ).await?;
                        active_jobs = active_jobs.saturating_sub(1);
                    }
                    ("capability.start", WorkerPayload::CapabilityStart(job)) => {
                        active_jobs += 1;
                        info!(
                            job_id = %job.job_id,
                            capability = %job.capability,
                            "mock capability started"
                        );

                        if job.capability != "run_echo" {
                            send(
                                &mut write,
                                Envelope::new(
                                    "capability.error",
                                    format!("msg_{}", Uuid::new_v4()),
                                    WorkerPayload::CapabilityError(CapabilityError {
                                        job_id: job.job_id,
                                        message: "unsupported capability".to_string(),
                                    }),
                                ),
                            ).await?;
                            active_jobs = active_jobs.saturating_sub(1);
                            continue;
                        }

                        let message = job
                            .input
                            .get("message")
                            .and_then(|value| value.as_str())
                            .unwrap_or("");
                        let response = format!("private worker echo: {message}");

                        for token in response.split_inclusive(' ') {
                            send(
                                &mut write,
                                Envelope::new(
                                    "capability.delta",
                                    format!("msg_{}", Uuid::new_v4()),
                                    WorkerPayload::CapabilityDelta(CapabilityDelta {
                                        job_id: job.job_id.clone(),
                                        data: serde_json::json!({ "chunk": token }),
                                    }),
                                ),
                            ).await?;
                            time::sleep(Duration::from_millis(120)).await;
                        }

                        send(
                            &mut write,
                            Envelope::new(
                                "capability.finish",
                                format!("msg_{}", Uuid::new_v4()),
                                WorkerPayload::CapabilityFinish(CapabilityFinish {
                                    job_id: job.job_id,
                                    output: serde_json::json!({
                                        "message": response,
                                        "worker": worker_id
                                    }),
                                }),
                            ),
                        ).await?;
                        active_jobs = active_jobs.saturating_sub(1);
                    }
                    (kind, WorkerPayload::JobStart(job)) => {
                        warn!(%kind, job_id = %job.job_id, "unexpected job envelope");
                        send(
                            &mut write,
                            Envelope::new(
                                "job.error",
                                format!("msg_{}", Uuid::new_v4()),
                                WorkerPayload::JobError(JobError {
                                    job_id: job.job_id,
                                    message: "unexpected job envelope".to_string(),
                                }),
                            ),
                        ).await?;
                    }
                    _ => warn!(kind = %envelope.kind, "unexpected router message"),
                }
            }
            else => break,
        }
    }

    Ok(())
}

async fn send<S>(
    write: &mut S,
    envelope: Envelope<WorkerPayload>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: SinkExt<Message> + Unpin,
    <S as futures_util::Sink<Message>>::Error: std::error::Error + Send + Sync + 'static,
{
    let text = serde_json::to_string(&envelope)?;
    write.send(Message::Text(text.into())).await?;
    Ok(())
}
