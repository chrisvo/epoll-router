# epoll-router

`epoll-router` is a prototype reverse-connected capability router for hosted AI agents.

Think: a self-hosted runner for AI agents. Private workers connect outbound over a persistent WebSocket, advertise approved capabilities, and perform agent-requested work without exposing inbound network endpoints.

## Why This Exists

Hosted AI agents are most useful when they can inspect, test, and operate against real private environments. But giving a hosted agent broad SSH, VPN, database, or VPC access is usually the wrong trust boundary.

`epoll-router` is for trust-bounded delegation:

```text
hosted AI agent
  |
  | request approved capability
  v
public router
  |
  | already-open outbound WebSocket
  v
private agent worker
  |
  | performs work locally
  v
private repo / service / logs / model
```

The hosted agent does not get general network reachability. It can only request named capabilities that the private worker advertised.

This is useful for:

- Running tests in a private repo without uploading the whole environment.
- Reading redacted logs from inside a VPC.
- Querying internal deployment status through a bounded tool.
- Calling private APIs without giving the hosted agent raw credentials.
- Running local inference as one capability among many.
- Building bring-your-own-compute agent products where work executes inside customer infrastructure.

This project is not trying to replace SSH, VPNs, CI runners, vLLM, TGI, llama.cpp, or LiteLLM. The more specific role is:

```text
hosted AI agent
  |
  v
epoll-router
  |
  v
private agent worker
  |
  v
approved local capabilities
```

The first runnable prototype still uses an OpenAI-compatible-ish chat endpoint and a mock streaming worker. That is the first vertical slice of the routing path, not the full product boundary.

## Related Approaches

This sits near several existing categories, but the intended boundary is different.

**Network tunnels and VPNs** such as Tailscale, WireGuard, and Cloudflare Tunnel provide private network reachability. `epoll-router` is not trying to be a better VPN. The distinction is capability access instead of network access: a hosted agent can request `run_tests`, but it does not receive broad access to the private network.

**Self-hosted CI runners** such as GitHub Actions runners, Buildkite agents, and Jenkins agents execute predefined workflows from repo or pipeline events. `epoll-router` is agent-facing and interactive: a hosted agent requests a bounded capability during its reasoning loop and receives structured context back.

**Self-hosted agent environments** run more of the AI agent stack inside customer infrastructure. That can be the right answer for enterprises that want full local execution. `epoll-router` is smaller: it lets a hosted agent delegate specific approved work to private workers without moving the whole agent runtime.

**MCP servers and tool connectors** expose tools to agents. `epoll-router` is complementary: it can act as the reverse-connected private worker layer underneath agent tool protocols when the tool server cannot or should not be publicly reachable.

The target niche is:

```text
open-source, vendor-neutral, capability-scoped private workers
for hosted AI agents
```

## What Works

- Router HTTP API at `/v1/chat/completions`
- General capability API at `/v1/capabilities/{capability}`
- SSE streaming responses for `stream: true`
- Non-streaming JSON responses for `stream: false`
- Worker outbound WebSocket at `/workers/socket`
- JSON message envelope with protocol version and message type
- Bearer-token auth for clients and workers
- Worker registration with model-like capabilities and aliases
- Worker telemetry updates
- In-memory routing to one healthy, available worker
- Mock `run_echo` capability
- Mock worker that streams fake token deltas

## Architecture

```text
hosted agent or app
  |
  | request capability
  v
router
  |
  | outbound worker WebSocket, already connected
  v
private agent worker
```

Clients use a conventional HTTP interface. Workers maintain persistent outbound WebSocket connections to the router, register their capabilities, receive jobs, and stream results back.

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

Send a streaming capability request:

```sh
curl -N http://127.0.0.1:3000/v1/capabilities/run_echo \
  -H 'Authorization: Bearer dev-client-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "stream": true,
    "input": {
      "message": "hello private worker"
    }
  }'
```

Expected response shape:

```text
data: {"chunk":"private "}

data: {"chunk":"worker "}

event: output
data: {"message":"private worker echo: hello private worker","worker":"mock-worker"}

data: [DONE]
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
- `capability.start`
- `capability.delta`
- `capability.finish`
- `capability.error`

## Capability Routing

Workers advertise named capabilities with bounded input/output contracts. The mock worker currently advertises:

- `run_echo`

The chat-completion prototype still models local inference as model routing: workers advertise concrete model ids and aliases, and the router accepts either:

- a concrete model id, such as `mock-llm`
- an alias, such as `local-default` or `fast`

The first healthy worker with matching capacity receives the job. In the intended product shape, `run_tests`, `read_logs`, `query_deploy_status`, and `run_local_model` are all capabilities.

## Current Limitations

This is a prototype, not production infrastructure.

- Active jobs are in memory only.
- There is no durable queue.
- There is no retry after a stream has started.
- There is no real inference backend yet.
- There is only one mock capability.
- There is no cancellation propagation yet.
- The OpenAI-compatible API is intentionally small.
- Routing is first-match, not latency/cost optimized.

## Next Milestones

- Add cancellation when the client disconnects.
- Add request and protocol validation tests.
- Add real capabilities such as `run_tests`, `read_logs`, and `query_deploy_status`.
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
