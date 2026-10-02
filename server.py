"""OpsPulse for Alexa+ MCP Server.

A production-grade Model Context Protocol (MCP) server running over Streamable HTTP /
Server-Sent Events (SSE, spec version 2025-11-25) built for the Alexa+ Hackathon.

Architected for:
  1. Alexa+ Track: Self-hosted MCP server with SSE transport at /mcp/sse and message receiver at /mcp/message.
  2. AWS Builder Mini-Challenge: Active code-level integration with Amazon Bedrock
     (anthropic.claude-3-5-sonnet-20240620-v1:0 via boto3) with intelligent fallback handling.
  3. Open Source Mini-Challenge: Fully documented, permissive, clean architecture with
     structured Pydantic schemas, CORS support for Alexa+ web UI, and direct REST testing endpoints.

Author: Principal Cloud & AI Systems Engineer
License: Apache 2.0
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import sys
import uuid
from typing import Any, AsyncGenerator, Dict, List, Literal, Optional

# -----------------------------------------------------------------------------
# Optional third-party imports with resilient defensive boundaries
# -----------------------------------------------------------------------------

# FastAPI & Starlette
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

# Boto3 / Amazon Bedrock Runtime
try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:  # pragma: no cover
    BOTO3_AVAILABLE = False
    boto3 = None
    BotoCoreError = ClientError = NoCredentialsError = Exception

# Official MCP SDK
try:
    from mcp.server import Server
    from mcp.server.sse import SseServerTransport
    import mcp.types as mcp_types
    MCP_SDK_AVAILABLE = True
except ImportError:  # pragma: no cover
    MCP_SDK_AVAILABLE = False
    Server = None
    SseServerTransport = None
    mcp_types = None


# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("opspulse.server")

# -----------------------------------------------------------------------------
# Configuration & Environment Variables
# -----------------------------------------------------------------------------
PORT = int(os.getenv("PORT", "8000"))
HOST = os.getenv("HOST", "0.0.0.0")
AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-5-sonnet-20240620-v1:0")

# -----------------------------------------------------------------------------
# Pydantic Schemas
# -----------------------------------------------------------------------------
class ServiceHealthResponse(BaseModel):
    """Structured telemetry response for microservice health inspections."""
    service_name: str = Field(..., description="Name of the inspected microservice")
    status: Literal["HEALTHY", "DEGRADED", "CRITICAL"] = Field(..., description="Operational status")
    latency_p99_ms: float = Field(..., description="99th percentile response latency in milliseconds")
    latency_p50_ms: float = Field(..., description="Median response latency in milliseconds")
    cpu_utilization_pct: float = Field(..., description="Average container CPU utilization percentage")
    memory_utilization_pct: float = Field(..., description="Average container Memory utilization percentage")
    error_rate_pct: float = Field(..., description="HTTP 5xx / application error rate percentage")
    active_replicas: int = Field(..., description="Currently active healthy task replicas")
    desired_replicas: int = Field(..., description="Target desired task replicas")
    target_group_health: str = Field(..., description="AWS ALB Target Group health distribution")
    timestamp: str = Field(..., description="ISO8601 observation timestamp")
    environment: str = Field(default="production", description="Deployment environment")
    details: str = Field(..., description="Diagnostic observation summary")


class RollbackRequest(BaseModel):
    """Payload to trigger an emergency canary rollback."""
    service_name: str = Field(..., description="Name of the target service to revert")
    target_version: str = Field(..., description="Target revision or semver tag (e.g. v2.4.1)")
    reason: Optional[str] = Field(default="Triggered by Alexa+ OpsPulse automated remediation", description="Reason for rollback")


class RollbackResponse(BaseModel):
    """Execution timeline and verification logs for canary rollback."""
    status: Literal["SUCCESS", "FAILED"] = Field(..., description="Final outcome of rollback")
    service_name: str = Field(..., description="Target service")
    target_version: str = Field(..., description="Reverted to stable version")
    previous_version: str = Field(..., description="Decommissioned unhealthy version")
    rollback_id: str = Field(..., description="Unique idempotency and tracking ID")
    duration_seconds: float = Field(..., description="Total execution time in seconds")
    execution_logs: List[str] = Field(..., description="Ordered timestamped operational logs")
    timestamp: str = Field(..., description="Completion timestamp")


class AnalyzeIncidentRequest(BaseModel):
    """Incident log payload for AI diagnostic analysis."""
    log_trace: str = Field(..., description="Raw log trace or stack trace from CloudWatch / Datadog")
    service_name: Optional[str] = Field(default="unknown-service", description="Optional associated service name")


class AnalyzeIncidentResponse(BaseModel):
    """Root cause diagnostic and remediation playbook generated via AWS Bedrock."""
    provider: str = Field(..., description="AI inference provider")
    model_id: str = Field(..., description="Underlying Foundation Model identifier")
    severity: Literal["SEV-1 (Critical)", "SEV-2 (High)", "SEV-3 (Moderate)", "SEV-4 (Low)"] = Field(..., description="Incident severity level")
    root_cause_summary: str = Field(..., description="Concise explanation of the underlying failure")
    impact_assessment: str = Field(..., description="Impact on user traffic and SLAs")
    remediation_playbook: List[str] = Field(..., description="Step-by-step actionable remediation instructions")
    suggested_action: str = Field(..., description="Immediate primary recommendation (e.g. trigger_rollback)")
    raw_analysis: str = Field(..., description="Full text analysis generated by Claude 3.5 Sonnet")
    timestamp: str = Field(..., description="Analysis generation timestamp")


# -----------------------------------------------------------------------------
# Mock Telemetry Datastore & Baseline State
# -----------------------------------------------------------------------------
SERVICE_CATALOG: Dict[str, Dict[str, Any]] = {
    "payment-gateway": {
        "status": "DEGRADED",
        "latency_p99_ms": 1420.5,
        "latency_p50_ms": 118.4,
        "cpu_utilization_pct": 89.2,
        "memory_utilization_pct": 76.5,
        "error_rate_pct": 14.8,
        "active_replicas": 12,
        "desired_replicas": 16,
        "target_group_health": "4_UNHEALTHY_12_HEALTHY",
        "current_version": "v2.5.0-canary",
        "stable_version": "v2.4.1",
        "details": "High HTTP 504 Gateway Timeout rate detected on /v1/charges downstream endpoint.",
    },
    "payments-service": {
        "status": "DEGRADED",
        "latency_p99_ms": 1420.5,
        "latency_p50_ms": 118.4,
        "cpu_utilization_pct": 89.2,
        "memory_utilization_pct": 76.5,
        "error_rate_pct": 14.8,
        "active_replicas": 12,
        "desired_replicas": 16,
        "target_group_health": "4_UNHEALTHY_12_HEALTHY",
        "current_version": "v2.5.0-canary",
        "stable_version": "v2.4.1",
        "details": "High HTTP 504 Gateway Timeout rate detected on /v1/charges downstream endpoint.",
    },
    "auth-service": {
        "status": "HEALTHY",
        "latency_p99_ms": 48.2,
        "latency_p50_ms": 14.1,
        "cpu_utilization_pct": 32.1,
        "memory_utilization_pct": 41.0,
        "error_rate_pct": 0.02,
        "active_replicas": 8,
        "desired_replicas": 8,
        "target_group_health": "8_HEALTHY_0_UNHEALTHY",
        "current_version": "v3.1.2",
        "stable_version": "v3.1.2",
        "details": "JWT verification and session cache latency well within SLO thresholds.",
    },
    "order-api": {
        "status": "HEALTHY",
        "latency_p99_ms": 94.6,
        "latency_p50_ms": 32.8,
        "cpu_utilization_pct": 44.5,
        "memory_utilization_pct": 52.8,
        "error_rate_pct": 0.08,
        "active_replicas": 10,
        "desired_replicas": 10,
        "target_group_health": "10_HEALTHY_0_UNHEALTHY",
        "current_version": "v1.9.0",
        "stable_version": "v1.9.0",
        "details": "Order placement queue depth normal; DynamoDB latency < 5ms.",
    },
    "inventory-service": {
        "status": "DEGRADED",
        "latency_p99_ms": 860.0,
        "latency_p50_ms": 82.0,
        "cpu_utilization_pct": 78.4,
        "memory_utilization_pct": 84.1,
        "error_rate_pct": 6.5,
        "active_replicas": 6,
        "desired_replicas": 8,
        "target_group_health": "2_UNHEALTHY_6_HEALTHY",
        "current_version": "v2.0.4-rc2",
        "stable_version": "v2.0.3",
        "details": "PostgreSQL connection pool near exhaustion during stock reservation bursts.",
    },
    "notification-worker": {
        "status": "HEALTHY",
        "latency_p99_ms": 15.0,
        "latency_p50_ms": 4.2,
        "cpu_utilization_pct": 18.0,
        "memory_utilization_pct": 29.3,
        "error_rate_pct": 0.0,
        "active_replicas": 4,
        "desired_replicas": 4,
        "target_group_health": "4_HEALTHY_0_UNHEALTHY",
        "current_version": "v1.4.0",
        "stable_version": "v1.4.0",
        "details": "SQS message consumption rate balanced with push notification outbound queue.",
    },
}


# -----------------------------------------------------------------------------
# Core Domain Tools
# -----------------------------------------------------------------------------

async def tool_get_service_health(service_name: str) -> Dict[str, Any]:
    """Inspects latency (p99), CPU utilization, error rate percentage, and returns structured JSON."""
    clean_name = service_name.strip().lower()
    logger.info("Executing tool: get_service_health(service_name='%s')", clean_name)

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if clean_name in SERVICE_CATALOG:
        entry = SERVICE_CATALOG[clean_name]
        response = ServiceHealthResponse(
            service_name=clean_name,
            status=entry["status"],
            latency_p99_ms=entry["latency_p99_ms"],
            latency_p50_ms=entry["latency_p50_ms"],
            cpu_utilization_pct=entry["cpu_utilization_pct"],
            memory_utilization_pct=entry["memory_utilization_pct"],
            error_rate_pct=entry["error_rate_pct"],
            active_replicas=entry["active_replicas"],
            desired_replicas=entry["desired_replicas"],
            target_group_health=entry["target_group_health"],
            timestamp=now_iso,
            environment="production",
            details=entry["details"],
        )
        return response.model_dump()

    # Dynamic deterministic generation for any unknown service name (allows judges to query arbitrary microservices)
    name_hash = sum(ord(c) for c in clean_name)
    is_degraded = (name_hash % 3 == 0)
    status_val: Literal["HEALTHY", "DEGRADED", "CRITICAL"] = "DEGRADED" if is_degraded else "HEALTHY"
    p99 = 1200.0 + (name_hash % 400) if is_degraded else 45.0 + (name_hash % 50)
    err_rate = round(8.5 + (name_hash % 10) * 0.5, 2) if is_degraded else 0.01

    response = ServiceHealthResponse(
        service_name=clean_name,
        status=status_val,
        latency_p99_ms=round(p99, 1),
        latency_p50_ms=round(p99 * 0.15, 1),
        cpu_utilization_pct=round(82.0 if is_degraded else 38.0, 1),
        memory_utilization_pct=round(74.0 if is_degraded else 42.0, 1),
        error_rate_pct=err_rate,
        active_replicas=6 if is_degraded else 8,
        desired_replicas=8,
        target_group_health="2_UNHEALTHY_6_HEALTHY" if is_degraded else "8_HEALTHY_0_UNHEALTHY",
        timestamp=now_iso,
        environment="production",
        details=f"Synthesized telemetry for dynamically registered service '{clean_name}'.",
    )
    return response.model_dump()


async def tool_trigger_rollback(service_name: str, target_version: str, reason: Optional[str] = None) -> Dict[str, Any]:
    """Simulates initiating an emergency canary rollback and returning execution logs."""
    clean_name = service_name.strip().lower()
    target_ver = target_version.strip()
    reason_text = reason or "Alexa+ OpsPulse triggered emergency rollback"
    rollback_id = f"rb-{uuid.uuid4().hex[:8]}"

    logger.info("Executing tool: trigger_rollback(service_name='%s', target_version='%s', reason='%s')",
                clean_name, target_ver, reason_text)

    # Validate target version syntax (e.g., v1.2.3 or 1.2.3)
    if not target_ver or len(target_ver) < 2:
        raise ValueError(f"Invalid target_version format: '{target_version}'. Expected format like 'v2.4.1'.")

    prev_version = "v2.5.0-canary"
    if clean_name in SERVICE_CATALOG:
        prev_version = SERVICE_CATALOG[clean_name].get("current_version", "v2.5.0-canary")

    now = datetime.datetime.now(datetime.timezone.utc)
    t0 = now.strftime("%H:%M:%S")

    # High-fidelity progressive canary rollback simulation logs
    execution_logs = [
        f"[{t0}.102] [AWS::ECS::DeploymentController] Received emergency rollback request for service '{clean_name}' (Rollback ID: {rollback_id}). Reason: {reason_text}",
        f"[{t0}.450] [AWS::ApplicationLoadBalancer] Shifting weighted routing for target group 'tg-{clean_name}': Canary weight 10% -> 0%, Stable weight 90% -> 100%.",
        f"[{t0}.920] [AWS::ECS] Reverting active task definition from '{clean_name}:{prev_version}' to stable revision '{clean_name}:{target_ver}'.",
        f"[{t0}.580] [AWS::ECS] Initiated deregistration and graceful SIGTERM drain of 2 unhealthy canary container instances.",
        f"[{t0}.980] [AWS::CloudWatch::SyntheticProbes] Dispatching 20 canary verification probes to stable revision '{target_ver}'.",
        f"[{t0}.210] [AWS::CloudWatch::SyntheticProbes] Verification probes PASS: HTTP 200 OK (p99 latency 38.2ms, error rate 0.00%).",
        f"[{t0}.540] [AWS::EventBridge] Emitted audit event 'ServiceRollbackCompleted' to EventBridge event bus 'OpsPulseAuditBus'.",
        f"[{t0}.790] [AWS::Route53] Healthcheck status GREEN. Rollback transaction finalized successfully.",
    ]

    # Update local mock catalog if present to reflect restored health
    if clean_name in SERVICE_CATALOG:
        SERVICE_CATALOG[clean_name]["status"] = "HEALTHY"
        SERVICE_CATALOG[clean_name]["latency_p99_ms"] = 52.0
        SERVICE_CATALOG[clean_name]["error_rate_pct"] = 0.01
        SERVICE_CATALOG[clean_name]["current_version"] = target_ver
        SERVICE_CATALOG[clean_name]["target_group_health"] = f"{SERVICE_CATALOG[clean_name]['desired_replicas']}_HEALTHY_0_UNHEALTHY"
        SERVICE_CATALOG[clean_name]["details"] = f"Restored to stable version {target_ver} following rollback {rollback_id}."

    response = RollbackResponse(
        status="SUCCESS",
        service_name=clean_name,
        target_version=target_ver,
        previous_version=prev_version,
        rollback_id=rollback_id,
        duration_seconds=3.8,
        execution_logs=execution_logs,
        timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    )
    return response.model_dump()


def _generate_bedrock_mock_analysis(log_trace: str, service_name: str) -> Dict[str, Any]:
    """Generates a high-fidelity SRE incident analysis when AWS Bedrock credentials are unavailable."""
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    trace_lower = log_trace.lower()

    if "timeout" in trace_lower or "504" in trace_lower or "gateway" in trace_lower:
        severity = "SEV-1 (Critical)"
        root_cause = "Downstream HTTP 504 Gateway Timeout caused by connection saturation and thread starvation in the upstream connection pool."
        impact = "Customer checkout and authorization requests timing out; p99 latency spiked over 1,400ms."
        playbook = [
            f"1. Emergency Traffic Shedding: Trigger immediate rollback of '{service_name}' to previous stable revision (e.g. v2.4.1).",
            "2. Connection Pool Hardening: Increase maximum pool capacity and adjust idle connection TTL in HikariCP/asyncpg.",
            "3. Upstream Circuit Breaking: Verify Envoy / ALB circuit breaker settings to fail fast rather than hanging threads.",
            "4. CloudWatch Metric Validation: Monitor 'HTTPCode_Target_5XX_Count' until metric drops to baseline < 0.05%."
        ]
        suggested_action = f"trigger_rollback(service_name='{service_name}', target_version='v2.4.1')"
    elif "connection refused" in trace_lower or "postgres" in trace_lower or "database" in trace_lower:
        severity = "SEV-1 (Critical)"
        root_cause = "Database Connection Exhaustion: The service pool exceeded PostgreSQL max_connections limit, leading to dropped worker threads."
        impact = "Persistent transaction failures across all read/write database requests."
        playbook = [
            "1. Activate AWS RDS Proxy to queue connection surges and prevent direct backend exhaustion.",
            "2. Identify and terminate long-running idle transactions via 'pg_terminate_backend()'.",
            f"3. Revert deployment of '{service_name}' if connection leak was introduced in the latest release.",
            "4. Increase ECS read-replica autoscaling threshold."
        ]
        suggested_action = f"trigger_rollback(service_name='{service_name}', target_version='v2.4.1')"
    elif "out of memory" in trace_lower or "oom" in trace_lower or "heap" in trace_lower:
        severity = "SEV-1 (Critical)"
        root_cause = "Container OOMKilled: Native heap exhaustion in service worker tasks under high concurrent load."
        impact = "Cascading container restart loops (CrashLoopBackOff) and ALB 502 Bad Gateway responses."
        playbook = [
            f"1. Initiate canary rollback to previous stable tag for '{service_name}'.",
            "2. Bump ECS task memory reservation by 50% via AWS CloudFormation / Terraform.",
            "3. Capture memory heap dump from crashed container in S3 debug bucket.",
            "4. Confirm container stability across all availability zones."
        ]
        suggested_action = f"trigger_rollback(service_name='{service_name}', target_version='v2.4.1')"
    else:
        severity = "SEV-2 (High)"
        root_cause = "Unhandled runtime exception during request serialization / external integration processing."
        impact = "Degraded request success rate with elevated HTTP 500 error responses on affected endpoints."
        playbook = [
            f"1. Isolate blast radius by draining canary traffic for '{service_name}'.",
            "2. Roll back service to previous stable release.",
            "3. Inspect stack trace line numbers and commit diff against main branch.",
            "4. Verify synthetic integration tests in staging environment."
        ]
        suggested_action = f"trigger_rollback(service_name='{service_name}', target_version='v2.4.1')"

    raw_text = (
        f"### SRE Incident Analysis & Remediation Playbook (Bedrock Claude 3.5 Sonnet Engine)\n\n"
        f"**Service:** `{service_name}`\n"
        f"**Severity:** {severity}\n"
        f"**Observation:** [AWS Bedrock Mock Fallback - Local Environment Mode active]\n\n"
        f"#### 1. Root Cause Summary\n{root_cause}\n\n"
        f"#### 2. Impact Assessment\n{impact}\n\n"
        f"#### 3. Immediate Remediation Playbook\n"
        + "\n".join(playbook) + "\n\n"
        f"#### 4. Primary Recommended Action\n`{suggested_action}`"
    )

    return AnalyzeIncidentResponse(
        provider="AWS Bedrock (Local Fallback - Configure AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY for live Bedrock)",
        model_id=BEDROCK_MODEL_ID,
        severity=severity,
        root_cause_summary=root_cause,
        impact_assessment=impact,
        remediation_playbook=playbook,
        suggested_action=suggested_action,
        raw_analysis=raw_text,
        timestamp=now_iso,
    ).model_dump()


async def tool_analyze_incident_log(log_trace: str, service_name: Optional[str] = None) -> Dict[str, Any]:
    """Directly invokes Amazon Bedrock (anthropic.claude-3-5-sonnet-20240620-v1:0 via boto3)

    to summarize the error and provide an immediate remediation playbook. Includes fallback
    handling if AWS credentials are not yet configured in local environment variables.
    """
    clean_svc = (service_name or "unknown-service").strip().lower()
    clean_trace = log_trace.strip()

    logger.info("Executing tool: analyze_incident_log(service_name='%s', log_len=%d)",
                clean_svc, len(clean_trace))

    if not clean_trace:
        raise ValueError("Parameter 'log_trace' must not be empty.")

    # Check if AWS boto3 and credentials are available
    has_aws_creds = bool(os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY")) or \
                    bool(os.getenv("AWS_PROFILE")) or \
                    bool(os.path.exists(os.path.expanduser("~/.aws/credentials")))

    if BOTO3_AVAILABLE and has_aws_creds:
        try:
            logger.info("Connecting to Amazon Bedrock Runtime in region '%s'...", AWS_REGION)
            client = boto3.client("bedrock-runtime", region_name=AWS_REGION)

            system_prompt = (
                "You are OpsPulse AI, an automated Principal Site Reliability Engineer (SRE) copilot "
                "integrated into Alexa+ and AWS cloud systems. Analyze the provided microservice log trace or "
                "stack trace. You must output a structured, crisp analysis with:\n"
                "1. SEVERITY: One of [SEV-1 (Critical), SEV-2 (High), SEV-3 (Moderate), SEV-4 (Low)]\n"
                "2. ROOT_CAUSE: Concise technical root cause explanation (1-2 sentences)\n"
                "3. IMPACT: User and system impact\n"
                "4. PLAYBOOK: Numbered step-by-step immediate remediation instructions\n"
                "5. RECOMMENDED_ACTION: Primary actionable command, e.g. trigger_rollback(service_name=..., target_version=...)"
            )

            user_message = f"Analyze this incident log trace for service '{clean_svc}':\n\n```\n{clean_trace}\n```"

            body = json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1024,
                "temperature": 0.2,
                "system": system_prompt,
                "messages": [
                    {"role": "user", "content": user_message}
                ],
            })

            # Offload blocking boto3 call to thread pool
            loop = asyncio.get_running_loop()
            bedrock_response = await loop.run_in_executor(
                None,
                lambda: client.invoke_model(
                    modelId=BEDROCK_MODEL_ID,
                    contentType="application/json",
                    accept="application/json",
                    body=body,
                )
            )

            raw_payload = json.loads(bedrock_response["body"].read().decode("utf-8"))
            claude_text = raw_payload.get("content", [{}])[0].get("text", "")

            # Parse severity and recommendations from Claude's response
            severity_val: Literal["SEV-1 (Critical)", "SEV-2 (High)", "SEV-3 (Moderate)", "SEV-4 (Low)"] = "SEV-1 (Critical)"
            if "SEV-2" in claude_text:
                severity_val = "SEV-2 (High)"
            elif "SEV-3" in claude_text:
                severity_val = "SEV-3 (Moderate)"
            elif "SEV-4" in claude_text:
                severity_val = "SEV-4 (Low)"

            playbook_lines = [
                line.strip() for line in claude_text.split("\n")
                if line.strip().startswith(("1.", "2.", "3.", "4.", "5.", "-"))
            ]
            if not playbook_lines:
                playbook_lines = [
                    f"1. Trigger canary rollback for '{clean_svc}'.",
                    "2. Verify ALB target group health metrics in AWS CloudWatch.",
                    "3. Inspect container runtime logs in CloudWatch Logs Insights."
                ]

            return AnalyzeIncidentResponse(
                provider="AWS Bedrock (Amazon Bedrock Runtime via boto3)",
                model_id=BEDROCK_MODEL_ID,
                severity=severity_val,
                root_cause_summary=f"Incident identified in {clean_svc}: {claude_text[:200].strip()}...",
                impact_assessment=f"Microservice '{clean_svc}' experiencing elevated errors or latency degradation.",
                remediation_playbook=playbook_lines[:5],
                suggested_action=f"trigger_rollback(service_name='{clean_svc}', target_version='v2.4.1')",
                raw_analysis=claude_text,
                timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            ).model_dump()

        except (BotoCoreError, ClientError, NoCredentialsError) as aws_err:
            logger.warning("AWS Bedrock API call failed (%s). Activating SRE mock fallback.", str(aws_err))
            return _generate_bedrock_mock_analysis(clean_trace, clean_svc)
        except Exception as exc:
            logger.warning("Unexpected error invoking Bedrock (%s). Activating SRE mock fallback.", str(exc))
            return _generate_bedrock_mock_analysis(clean_trace, clean_svc)

    logger.info("AWS credentials not configured. Utilizing SRE fallback engine.")
    return _generate_bedrock_mock_analysis(clean_trace, clean_svc)


# -----------------------------------------------------------------------------
# MCP Schema Declarations (Tools & Capabilities)
# -----------------------------------------------------------------------------
MCP_TOOLS_DEFINITIONS = [
    {
        "name": "get_service_health",
        "description": "Inspects mock latency (p99/p50), CPU/memory utilization, error rate percentage, and active replicas for a given microservice.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "service_name": {
                    "type": "string",
                    "description": "The identifier of the microservice (e.g. payment-gateway, auth-service, order-api)",
                }
            },
            "required": ["service_name"],
        },
    },
    {
        "name": "trigger_rollback",
        "description": "Simulates initiating an emergency canary rollback for a microservice and returns progressive execution verification logs.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "service_name": {
                    "type": "string",
                    "description": "Name of the target service (e.g. payment-gateway)",
                },
                "target_version": {
                    "type": "string",
                    "description": "Target stable revision to roll back to (e.g. v2.4.1)",
                },
                "reason": {
                    "type": "string",
                    "description": "Optional audit explanation for the emergency rollback",
                },
            },
            "required": ["service_name", "target_version"],
        },
    },
    {
        "name": "analyze_incident_log",
        "description": "Directly invokes Amazon Bedrock (anthropic.claude-3-5-sonnet-20240620-v1:0 via boto3) to diagnose stack traces and produce an immediate remediation playbook.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "log_trace": {
                    "type": "string",
                    "description": "Raw log trace or stack trace text from CloudWatch Logs",
                },
                "service_name": {
                    "type": "string",
                    "description": "Optional microservice identifier associated with the log trace",
                },
            },
            "required": ["log_trace"],
        },
    },
]


async def dispatch_mcp_tool_call(name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Internal tool dispatcher routing tool requests to implementations."""
    if name == "get_service_health":
        svc = arguments.get("service_name")
        if not svc:
            raise ValueError("Missing required argument 'service_name'")
        return await tool_get_service_health(svc)

    elif name == "trigger_rollback":
        svc = arguments.get("service_name")
        ver = arguments.get("target_version")
        reason = arguments.get("reason")
        if not svc or not ver:
            raise ValueError("Missing required arguments 'service_name' and 'target_version'")
        return await tool_trigger_rollback(svc, ver, reason)

    elif name == "analyze_incident_log":
        trace = arguments.get("log_trace")
        svc = arguments.get("service_name")
        if not trace:
            raise ValueError("Missing required argument 'log_trace'")
        return await tool_analyze_incident_log(trace, svc)

    else:
        raise ValueError(f"Unknown MCP tool: '{name}'")


# -----------------------------------------------------------------------------
# MCP SSE Session Manager (Streamable HTTP / SSE Protocol 2025-11-25)
# -----------------------------------------------------------------------------
class McpSseSession:
    """Represents an active Server-Sent Events MCP client connection."""

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.created_at = datetime.datetime.now(datetime.timezone.utc)

    async def send_event(self, event: str, data: Any) -> None:
        """Pushes an SSE event frame into the client's outbound queue."""
        data_str = json.dumps(data) if not isinstance(data, str) else data
        frame = f"event: {event}\ndata: {data_str}\n\n"
        await self.queue.put(frame)


class McpSessionManager:
    """Thread-safe registry for connected MCP SSE clients."""

    def __init__(self):
        self._sessions: Dict[str, McpSseSession] = {}
        self._lock = asyncio.Lock()

    async def create_session(self) -> McpSseSession:
        async with self._lock:
            sid = uuid.uuid4().hex
            session = McpSseSession(sid)
            self._sessions[sid] = session
            logger.info("New MCP SSE session created: %s (Total active: %d)", sid, len(self._sessions))
            return session

    async def get_session(self, session_id: str) -> Optional[McpSseSession]:
        async with self._lock:
            return self._sessions.get(session_id)

    async def remove_session(self, session_id: str) -> None:
        async with self._lock:
            if session_id in self._sessions:
                del self._sessions[session_id]
                logger.info("MCP SSE session closed: %s (Remaining active: %d)", session_id, len(self._sessions))


mcp_session_manager = McpSessionManager()


# -----------------------------------------------------------------------------
# Official MCP SDK Integration (if SDK installed in runtime)
# -----------------------------------------------------------------------------
sdk_mcp_server: Optional[Any] = None
sdk_sse_transport: Optional[Any] = None

if MCP_SDK_AVAILABLE and Server is not None and SseServerTransport is not None and mcp_types is not None:
    try:
        sdk_mcp_server = Server("opspulse-alexa-mcp")
        sdk_sse_transport = SseServerTransport("/mcp/message")

        @sdk_mcp_server.list_tools()
        async def sdk_handle_list_tools() -> list[mcp_types.Tool]:
            tools = []
            for t in MCP_TOOLS_DEFINITIONS:
                tools.append(mcp_types.Tool(
                    name=t["name"],
                    description=t["description"],
                    inputSchema=t["inputSchema"],
                ))
            return tools

        @sdk_mcp_server.call_tool()
        async def sdk_handle_call_tool(name: str, arguments: dict | None) -> list[mcp_types.TextContent]:
            res = await dispatch_mcp_tool_call(name, arguments or {})
            return [mcp_types.TextContent(type="text", text=json.dumps(res, indent=2))]

        logger.info("Official MCP Python SDK initialized and tools registered successfully.")
    except Exception as exc:
        logger.warning("Could not initialize official MCP SDK hooks: %s. Using native SSE engine.", exc)
        sdk_mcp_server = None
        sdk_sse_transport = None


# -----------------------------------------------------------------------------
# FastAPI Application & Middlewares
# -----------------------------------------------------------------------------
app = FastAPI(
    title="OpsPulse for Alexa+ MCP Server",
    description=(
        "Production-ready Model Context Protocol (MCP) server over SSE and Streamable HTTP (spec 2025-11-25) "
        "with Amazon Bedrock Claude 3.5 Sonnet SRE automation."
    ),
    version="1.0.0",
)

# CORS configuration for Alexa+ web UI and developer simulators
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "*",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------------------------------------------------------
# Root & Health Check Endpoints
# -----------------------------------------------------------------------------
@app.get("/", tags=["System"])
async def root():
    """Server metadata and hackathon compliance information."""
    return {
        "project": "OpsPulse for Alexa+",
        "status": "ONLINE",
        "version": "1.0.0",
        "hackathon_tracks": [
            "Alexa+ Track (Self-hosted MCP Server)",
            "AWS Builder Mini-Challenge (Amazon Bedrock boto3 integration)",
            "Open Source Mini-Challenge (Production-grade MCP SSE server)"
        ],
        "mcp_protocol": {
            "spec_version": "2025-11-25",
            "transport": "Server-Sent Events (SSE) / Streamable HTTP",
            "sse_endpoint": "/mcp/sse",
            "message_endpoint": "/mcp/message",
        },
        "aws_integration": {
            "bedrock_model_id": BEDROCK_MODEL_ID,
            "region": AWS_REGION,
            "boto3_installed": BOTO3_AVAILABLE,
        },
        "available_tools": [t["name"] for t in MCP_TOOLS_DEFINITIONS],
        "documentation": "/docs",
    }


@app.get("/health", tags=["System"])
async def health_check():
    """Liveness probe returning server status and service mesh overview."""
    return {
        "status": "HEALTHY",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "services_monitored": len(SERVICE_CATALOG),
        "bedrock_configured": BOTO3_AVAILABLE,
        "mcp_sdk_available": MCP_SDK_AVAILABLE,
    }


@app.get("/ui", tags=["System"], response_class=FileResponse)
async def serve_ui():
    """Serves the interactive OpsPulse for Alexa+ SRE Cockpit web interface."""
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)
    return JSONResponse(status_code=404, content={"detail": "index.html not found"})


# -----------------------------------------------------------------------------
# Streamable HTTP / MCP Server-Sent Events Endpoints (spec 2025-11-25)
# -----------------------------------------------------------------------------
@app.get("/mcp/sse", tags=["Model Context Protocol"])
async def mcp_sse_endpoint(request: Request):
    """Establishes an SSE stream connection with the MCP client (Alexa+ or simulator).

    Per the MCP SSE transport specification:
    1. Server yields an initial 'endpoint' event with the POST message receiver URL.
    2. Client sends JSON-RPC 2.0 requests to that URL.
    3. Server pushes responses and notifications back over this event stream.
    """
    # If official MCP SDK is available and initialized, route through official SDK transport
    if sdk_mcp_server is not None and sdk_sse_transport is not None:
        try:
            logger.info("Connecting SSE client using official MCP SDK SseServerTransport...")
            async with sdk_sse_transport.connect_sse(
                request.scope, request.receive, request._send
            ) as (read_stream, write_stream):
                await sdk_mcp_server.run(
                    read_stream, write_stream, sdk_mcp_server.create_initialization_options()
                )
            return Response(status_code=status.HTTP_200_OK)
        except Exception as sdk_err:
            logger.warning("SDK SSE connection error (%s). Falling back to native SSE engine.", sdk_err)

    # Native High-Performance MCP SSE Protocol Handler
    session = await mcp_session_manager.create_session()

    async def sse_event_generator() -> AsyncGenerator[str, None]:
        try:
            # Step 1: Send the mandatory MCP 'endpoint' event to announce message receiver
            message_url = f"/mcp/message?sessionId={session.session_id}"
            yield f"event: endpoint\ndata: {message_url}\n\n"

            # Step 2: Stream queued events to the client
            while True:
                if await request.is_disconnected():
                    logger.info("SSE client disconnected for session: %s", session.session_id)
                    break
                try:
                    # Timeout poll allows periodic disconnect verification
                    frame = await asyncio.wait_for(session.queue.get(), timeout=2.0)
                    yield frame
                    session.queue.task_done()
                except asyncio.TimeoutError:
                    # Send keep-alive comment
                    yield ": ping\n\n"
        finally:
            await mcp_session_manager.remove_session(session.session_id)

    return StreamingResponse(
        sse_event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/mcp/message", tags=["Model Context Protocol"])
async def mcp_message_endpoint(request: Request):
    """Processes incoming JSON-RPC 2.0 messages from the MCP client.

    Handles:
    - 'initialize': Protocol negotiation and capability exchange
    - 'notifications/initialized': Client readiness notification
    - 'tools/list': Discovers OpsPulse diagnostic and remediation tools
    - 'tools/call': Invokes get_service_health, trigger_rollback, or analyze_incident_log
    """
    # If official SDK transport is available, let it process the message
    if sdk_sse_transport is not None:
        try:
            # Check if this request is handled by SseServerTransport
            session_id = request.query_params.get("sessionId") or request.query_params.get("session_id")
            if session_id:
                await sdk_sse_transport.handle_post_message(request.scope, request.receive, request._send)
                return Response(status_code=status.HTTP_202_ACCEPTED)
        except Exception:
            pass  # Fall through to native handler

    # Parse incoming JSON-RPC payload
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {exc}")

    session_id = request.query_params.get("sessionId") or request.query_params.get("session_id")
    logger.info("MCP Message received [session=%s]: method=%s id=%s",
                session_id, payload.get("method"), payload.get("id"))

    # Validate JSON-RPC 2.0 structure
    msg_id = payload.get("id")
    method = payload.get("method")
    params = payload.get("params", {})

    # Handle notifications (no response needed)
    if method == "notifications/initialized":
        logger.info("Client reported initialization complete.")
        return Response(status_code=status.HTTP_202_ACCEPTED)

    # Prepare response envelope
    result_data: Any = None
    error_data: Optional[Dict[str, Any]] = None

    try:
        if method == "initialize":
            result_data = {
                "protocolVersion": "2025-11-25",
                "capabilities": {
                    "tools": {
                        "listChanged": False
                    },
                    "logging": {}
                },
                "serverInfo": {
                    "name": "opspulse-alexa-mcp",
                    "version": "1.0.0"
                }
            }

        elif method == "tools/list":
            result_data = {
                "tools": MCP_TOOLS_DEFINITIONS
            }

        elif method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            call_result = await dispatch_mcp_tool_call(tool_name, arguments)
            result_data = {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(call_result, indent=2)
                    }
                ],
                "isError": False
            }

        elif method == "ping":
            result_data = {}

        else:
            error_data = {
                "code": -32601,
                "message": f"Method not found: '{method}'"
            }

    except Exception as err:
        logger.exception("Error executing MCP method %s: %s", method, err)
        error_data = {
            "code": -32603,
            "message": str(err)
        }

    # Construct JSON-RPC response
    jsonrpc_response: Dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": msg_id,
    }
    if error_data:
        jsonrpc_response["error"] = error_data
    else:
        jsonrpc_response["result"] = result_data

    # If this request is bound to an active SSE session, push through the SSE stream
    if session_id:
        target_session = await mcp_session_manager.get_session(session_id)
        if target_session:
            await target_session.send_event("message", jsonrpc_response)
            return Response(status_code=status.HTTP_202_ACCEPTED)

    # Otherwise return synchronously as standard HTTP response (Streamable HTTP fallback)
    return JSONResponse(content=jsonrpc_response)


# -----------------------------------------------------------------------------
# Direct REST Endpoints (For Hackathon Judges & Browser / Curl Testing)
# -----------------------------------------------------------------------------
@app.get("/api/health/{service_name}", response_model=ServiceHealthResponse, tags=["Domain Tools REST API"])
async def api_get_service_health(service_name: str):
    """Direct REST inspection endpoint for microservice health."""
    try:
        return await tool_get_service_health(service_name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/rollback", response_model=RollbackResponse, tags=["Domain Tools REST API"])
async def api_trigger_rollback(body: RollbackRequest):
    """Direct REST invocation to trigger emergency canary rollback."""
    try:
        return await tool_trigger_rollback(body.service_name, body.target_version, body.reason)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/analyze-log", response_model=AnalyzeIncidentResponse, tags=["Domain Tools REST API"])
async def api_analyze_incident_log(body: AnalyzeIncidentRequest):
    """Direct REST endpoint invoking Amazon Bedrock Claude 3.5 Sonnet incident diagnostics."""
    try:
        return await tool_analyze_incident_log(body.log_trace, body.service_name)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# -----------------------------------------------------------------------------
# CLI Entry Point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting OpsPulse for Alexa+ MCP Server on %s:%d ...", HOST, PORT)
    logger.info("MCP Server-Sent Events Endpoint: http://%s:%d/mcp/sse", HOST, PORT)
    logger.info("MCP Message Receiver:          http://%s:%d/mcp/message", HOST, PORT)
    logger.info("Interactive API Documentation:   http://%s:%d/docs", HOST, PORT)
    uvicorn.run("server:app", host=HOST, port=PORT, reload=False, log_level="info")
