# epoll-router

`epoll-router` is a prototype LLM inference router for self-hosted workers that connect outbound over a persistent WebSocket, similar in spirit to Slack Socket Mode.

The goal is simple: keep inference workers behind NAT or private networks, let them dial out to a central router, and expose a normal OpenAI-compatible-ish HTTP API to clients.

## Why This Exists

Most LLM gateways assume the gateway can reach every upstream inference endpoint over normal HTTP. That works well for cloud providers, Kubernetes services, and public or VPN-accessible vLLM/TGI deployments.

`epoll-router` is for a different shape:

```text
private GPU machine behind NAT/firewall
  |
  | outbound WebSocket
  v
public router with OpenAI-compatible API
```

The real use case is a reverse-connected inference mesh: run LLM workers anywhere without opening inbound ports, then route inference to them through one public API.

This is useful for:

- Internal GPU workstations that should not expose public HTTP servers.
- On-prem or regulated environments where inference must happen inside a private network.
- Edge inference nodes in stores, labs, factories, clinics, or appliances.
- Ephemeral GPU capacity from spot instances, rented machines, homelab boxes, or developer laptops.
- Bring-your-own-compute products where a SaaS control plane dispatches jobs to customer-owned workers.

This project is not trying to replace vLLM, TGI, llama.cpp, or LiteLLM. A more realistic production role is:

```text
client / OpenAI SDK
  |
  v
LLM gateway or app backend
  |
  v
epoll-router reverse worker fabric
  |
  v
private vLLM / TGI / llama.cpp / MLX workers
```

The killer demo is: run the router on a VPS, run a worker on a laptop behind home NAT, wrap a local inference backend, and call one OpenAI-compatible endpoint from anywhere without opening an inbound port on the laptop.

## What Works

- Router HTTP API at `/v1/chat/completions`
- SSE streaming responses for `stream: true`
- Non-streaming JSON responses for `stream: false`
- Worker outbound WebSocket at `/workers/socket`
- JSON message envelope with protocol version and message type
- Bearer-token auth for clients and workers
- Worker registration with model capabilities and aliases
- Worker telemetry updates
- In-memory routing to one healthy, available worker
- Mock worker that streams fake token deltas

## Architecture

```text
client
  |
  | HTTP POST /v1/chat/completions
  v
router
  |
  | outbound worker WebSocket, already connected
  v
self-hosted worker
```

Clients use a conventional HTTP interface. Workers maintain persistent outbound WebSocket connections to the router, register their capabilities, receive jobs, and stream deltas back.

This lets workers run on machines that do not expose public inbound ports.

## Run Locally

Start the router:

```sh
cargo run --bin router
```

Start the mock worker in another terminal:

```sh
cargo run --bin mock-worker
```

Send a streaming chat request:

```sh
curl -N http://127.0.0.1:3000/v1/chat/completions \
  -H 'Authorization: Bearer dev-client-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "local-default",
    "stream": true,
    "messages": [
      { "role": "user", "content": "hello router" }
    ]
  }'
```

Expected response shape:

```text
data: {"choices":[{"delta":{"content":"Mock "}}]}

data: {"choices":[{"delta":{"content":"response "}}]}

data: [DONE]
```

Send a non-streaming request:

```sh
curl http://127.0.0.1:3000/v1/chat/completions \
  -H 'Authorization: Bearer dev-client-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "mock-llm",
    "stream": false,
    "messages": [
      { "role": "user", "content": "non stream check" }
    ]
  }'
```

## Configuration

Defaults:

- Client API key: `dev-client-token`
- Worker API key: `dev-worker-token`
- Worker id: `mock-worker`
- Router address: `127.0.0.1:3000`
- Worker socket URL: `ws://127.0.0.1:3000/workers/socket`

Environment variables:

```sh
CLIENT_KEYS=client-a:token-a,client-b:token-b
WORKER_KEYS=worker-a:token-a,worker-b:token-b
ROUTER_ADDR=127.0.0.1:3000
ROUTER_WS_URL=ws://127.0.0.1:3000/workers/socket
WORKER_ID=mock-worker
WORKER_TOKEN=dev-worker-token
```

## Worker Protocol

Messages use a versioned JSON envelope:

```json
{
  "v": 1,
  "type": "job.start",
  "id": "msg_...",
  "payload": {}
}
```

Current message types:

- `worker.register`
- `worker.telemetry`
- `job.start`
- `job.delta`
- `job.finish`
- `job.error`

## Model Routing

Workers advertise concrete model ids and aliases. The router currently accepts either:

- a concrete model id, such as `mock-llm`
- an alias, such as `local-default` or `fast`

The first healthy worker with matching capacity receives the job.

## Current Limitations

This is a prototype, not production infrastructure.

- Active jobs are in memory only.
- There is no durable queue.
- There is no retry after a stream has started.
- There is no real inference backend yet.
- There is no cancellation propagation yet.
- The OpenAI-compatible API is intentionally small.
- Routing is first-match, not latency/cost optimized.

## Next Milestones

- Add cancellation when the client disconnects.
- Add request and protocol validation tests.
- Add a real worker adapter for `llama.cpp`, vLLM, or TGI.
- Add queue limits and per-client in-flight limits.
- Add Prometheus/OpenTelemetry metrics.
- Add heartbeat timeout handling.
- Add a documented worker SDK boundary.

## Development

Format and check:

```sh
cargo fmt --check
cargo check
```
