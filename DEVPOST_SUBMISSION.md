# Devpost Submission — OpsPulse for Alexa+

## 🚀 Elevator Pitch
**OpsPulse for Alexa+** transforms Alexa into an autonomous, voice-directed **Site Reliability Engineering (SRE) Principal Copilot**. Powered by a self-hosted **Model Context Protocol (MCP)** server over Streamable HTTP/SSE and **Amazon Bedrock Claude 3.5 Sonnet**, OpsPulse allows engineers and on-call leads to inspect microservice fleet health, diagnose complex distributed stack traces, and trigger automated canary rollbacks across AWS cloud infrastructure — entirely through natural voice commands or an executive cyberpunk glassmorphic cockpit.

---

## 💡 Inspiration
During high-severity production incidents (SEV-1 outages), every second of Mean-Time-To-Resolution (MTTR) impacts revenue, customer trust, and team sanity. Yet, on-call engineers are forced into "context-switching hell": digging through fragmented CloudWatch dashboards, deciphering massive Java/Python stack traces across Datadog and OpenTelemetry, and navigating multi-step deployment pipelines in AWS ECS or Kubernetes to execute emergency canary rollbacks.

With the release of **Alexa+** and the emergence of the open **Model Context Protocol (MCP)**, we saw an opportunity to rethink Incident Management from the ground up:
*What if an on-call engineer or VP of Engineering could simply ask their Alexa device or desk cockpit: "Alexa, what's wrong with the payments service and how do we fix it?" and have a foundation model immediately triage the logs, isolate the database connection starvation, and execute a verified canary traffic drain with a single verbal confirmation?*

OpsPulse was born to prove how voice-driven AI copilot interfaces can bridge human intuition and autonomous cloud orchestration.

---

## 🛠️ What It Does
OpsPulse for Alexa+ exposes a suite of three high-leverage Site Reliability Engineering domain tools via the Model Context Protocol (spec 2025-11-25):

1. **Fleet Telemetry Inspection (`get_service_health`)**:
   - Inspects real-time operational metrics across microservices: p99 and p50 latency, CPU/memory utilization, error rates, target group health distribution, and active task replicas.
   - Evaluates services such as `payments-service`, `payment-gateway`, `auth-service`, `order-api`, and `inventory-service` — or dynamically synthesizes deterministic telemetry for any arbitrary microservice.

2. **AI Incident Log Diagnostics (`analyze_incident_log`)**:
   - Streams downstream error logs directly to **Amazon Bedrock**, leveraging **Claude 3.5 Sonnet** (`anthropic.claude-3-5-sonnet-20240620-v1:0`) via `boto3`.
   - Generates an instant root-cause analysis, severity classification (SEV-1 through SEV-4), customer impact assessment, and a step-by-step actionable remediation playbook.
   - Includes graceful, zero-downtime mock fallback if AWS credentials are not yet configured in local test environments.

3. **Emergency Canary Rollback (`trigger_rollback`)**:
   - Executes an emergency rollback mutation: shifts ALB weighted routing from canary (10% $\to$ 0%) back to stable (90% $\to$ 100%), updates ECS task definition revisions, drains degraded tasks, runs CloudWatch synthetic health verification probes, and emits an audit event to AWS EventBridge.
   - Live-mutates the service telemetry catalog from `DEGRADED` to `HEALTHY`, dropping error rates to 0.01% and p99 latency to sub-60ms.

4. **Executive Multimodal SRE Cockpit (`index.html`)**:
   - A dark-mode obsidian glassmorphic interface (`#0b0f19`) featuring a dynamic, multi-state animated **Alexa Voice Orb** (IDLE, LISTENING, THINKING, SPEAKING, and an XKCD `import antigravity` easter egg).
   - Real-time MCP Protocol Inspector displaying bidirectional JSON-RPC 2.0 frames with syntax highlighting.
   - Full Web Speech API voice dictation, synthesized speech responses, synthesized sound effects engine, tactile quick-prompt chips (Ctrl+1..4), and a Raycast-style Command Palette (⌘K).

---

## 🏗️ How We Built It
OpsPulse is architected around clean, decoupled systems:

```
[ Alexa+ Voice Assistant ]        [ Executive Web Cockpit ]
           │                                 │
           ▼                                 ▼
   SSE: /mcp/sse                      JSON-RPC: /mcp/message
   ─────────────────────────────────────────────────────────
             OpsPulse MCP Server (FastAPI / Python 3.12)
   ─────────────────────────────────────────────────────────
           │                     │                     │
           ▼                     ▼                     ▼
  get_service_health     trigger_rollback     analyze_incident_log
   (Service Catalog)    (State Mutation &       (Amazon Bedrock
                         Audit Logging)        Claude 3.5 Sonnet)
```

- **Backend Protocol Engine**: Written in Python 3.12 with **FastAPI** and **Pydantic v2**. Implements a non-blocking, streamable HTTP and Server-Sent Events (SSE) server following the MCP specification (2025-11-25).
- **Deadlock-Free Session Decoupling**: Designed an async `McpSessionManager` that isolates per-client event queues, preventing known ASGI transport hijacking bugs and ensuring rock-solid multi-client delivery.
- **Amazon Bedrock AI Engine**: Native `boto3` runtime integration with Claude 3.5 Sonnet, engineered with high-fidelity local fallback emulation for offline evaluation.
- **Frontend Architecture**: Pure Vanilla HTML5, CSS3, and modern JavaScript. Uses CSS custom properties, backdrop filters, SVG acoustics, and Web Audio API synthesizer oscillators for zero-dependency sound design.
- **Containerization**: Multi-stage `Dockerfile` (Python 3.12-slim) with built-in health probes and `docker-compose.yml` for single-command production deployment.

---

## ⚡ Challenges We Faced & How We Solved Them
1. **MCP Python SDK ASGI Transport Deadlock**:
   - *Problem*: Attempting to route `/mcp/sse` through standard SDK ASGI handlers locked up FastAPI when multiple requests or concurrent `/mcp/message` calls arrived.
   - *Solution*: Developed a clean, native async SSE session dispatcher (`McpSessionManager`) with per-session queues and automatic ping heartbeats.

2. **Multimodal Information Density (Voice vs Screen)**:
   - *Problem*: Complex JSON telemetry is overwhelming when read aloud by Alexa, but essential on a dashboard.
   - *Solution*: Implemented a dual-channel payload design: tools return structured JSON containing complete telemetry, while the client extracts a punchy 15-word conversational verbal synthesis ("*Payments-service is degraded with 1,420ms latency; Bedrock recommends rollback to v2.4.1*").

3. **Evaluation Portability for Hackathon Judges**:
   - *Problem*: Hackathon judges might test the project without active AWS credentials or on restricted networks.
   - *Solution*: Created an automatic fallback layer that detects AWS credentials and seamlessly provides high-fidelity simulated Claude 3.5 Sonnet analysis, paired with an offline demo toggle in the UI.

---

## 🏆 Accomplishments That We're Proud Of
- **100% Pass Rate on Automated Verification Suite**: `test_client.py` validates REST health probes, SSE initial handshakes, JSON-RPC tool discovery, and live service rollback state mutations (`payments-service` DEGRADED $\to$ HEALTHY).
- **Full MCP Protocol Compliance**: Verified compliance with the official Model Context Protocol 2025-11-25 specification over Streamable HTTP and SSE.
- **Zero-Dependency Executive Cockpit**: Built a cockpit UI featuring voice orb animations, sound effects, JSON syntax highlighting, and voice recognition with zero heavy frontend frameworks.
- **Enterprise SRE Playbooks**: Integrated Amazon Bedrock Claude 3.5 Sonnet to produce actionable, 4-step remediation playbooks for production outages.

---

## 💬 Amazon Developer Tools & DevRel Product Feedback
*(Extracted from our comprehensive [`FRICTION_LOG.md`](./FRICTION_LOG.md))*

1. **Publish an Official AWS / Alexa MCP FastAPI Package**:
   Developers building self-hosted MCP servers for Alexa+ would greatly benefit from an official `aws-mcp-fastapi` package that handles SSE streaming, session IDs, and CORS out of the box.
2. **Pydantic Support in Boto3 for Bedrock**:
   Adding typed Pydantic responses to `boto3.client('bedrock-runtime')` would eliminate boilerplate `StreamingBody` reading and manual JSON deserialization.
3. **Speculative Audio Streaming for Alexa+ MCP**:
   Allowing MCP servers to stream dual channels (an early verbal acknowledgment chunk followed by the structured data payload) would cut perceived latency in half for complex AI workflows.
4. **Local Bedrock Emulation Container**:
   An official Amazon Bedrock Mock Container would streamline CI/CD testing and hackathon evaluations without incurring AWS billing during development.
