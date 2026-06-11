# epoll-router

This context describes the domain language for routing AI-initiated work into private compute environments over outbound-only connections.

## Language

**Private Agent Worker**:
A compute process running inside a private environment that can perform AI-delegated work without exposing an inbound network endpoint. A public router may have many **Private Agent Workers** connected at once.
_Avoid_: inference worker, private EC2, socket client

**Trust-Bounded Delegation**:
An arrangement where an AI system may request specific approved work inside a private environment without receiving broad network access or unrestricted credentials. **Trust-Bounded Delegation** is the reason to use a **Private Agent Worker** instead of a general-purpose tunnel.
_Avoid_: tunneling, remote access, private curl

**Capability**:
A named kind of work that a **Private Agent Worker** is allowed to perform for a hosted AI system. A **Private Agent Worker** advertises many **Capabilities**, and each **Capability** has a bounded input/output contract.
_Avoid_: shell command, arbitrary command, raw execution

**Capability Catalog**:
The set of **Capabilities** advertised by a **Private Agent Worker**. The private environment owns the **Capability Catalog**; the hosted system may request advertised **Capabilities** but does not define new ones at dispatch time.
_Avoid_: hosted tool list, remote command menu, generated capability

**Self-Hosted Runner for AI Agents**:
The product metaphor for epoll-router: a private worker that performs approved work for a hosted AI agent, similar to how a self-hosted CI runner performs jobs for a hosted CI system. The metaphor emphasizes delegated work in a private environment, not general network access.
_Avoid_: tunnel, VPN, remote shell, inference gateway

**Agent-Requested Work**:
A bounded work request initiated by a hosted AI agent during its reasoning loop. **Agent-Requested Work** differs from CI work because it is interactive, capability-scoped, and intended to return context for the agent's next step.
_Avoid_: CI job, workflow run, pipeline step

**Agent Capability Routing**:
The product category for routing **Agent-Requested Work** from hosted AI systems to private workers that advertise approved **Capabilities**. LLM inference is one possible **Capability**, not the defining purpose of the system.
_Avoid_: LLM inference routing, model gateway, generic API proxy

## Example Dialogue

Developer: "Can the hosted agent inspect logs inside the customer's VPC?"

Domain expert: "Yes, if the customer runs a Private Agent Worker in that VPC. The hosted system uses Trust-Bounded Delegation to request approved work; the worker performs it locally."

Developer: "Can the agent run `cat /etc/passwd`?"

Domain expert: "No. The agent can request an approved Capability, such as `read_service_logs`, and the Private Agent Worker decides how to perform that safely."

Developer: "Can the hosted agent ask for a new `dump_database` tool?"

Domain expert: "No. It can only request a Capability already present in the worker's Capability Catalog."

Developer: "How should we explain this to a platform team?"

Domain expert: "Call it a Self-Hosted Runner for AI Agents: the agent requests approved work, and the private worker performs it locally."

Developer: "Isn't that just GitHub Actions?"

Domain expert: "No. CI runs predefined workflows from repo events; Agent-Requested Work is initiated by an AI agent and limited to advertised Capabilities."

Developer: "Is this mainly for LLM inference?"

Domain expert: "No. Local inference can be a Capability, but the broader category is Agent Capability Routing."
