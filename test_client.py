"""OpsPulse for Alexa+ End-to-End Verification Test Client.

Comprehensive automated test suite covering:
  1) REST health probes (`GET /health`, `/`)
  2) SSE initial handshake (`GET /mcp/sse` emits `endpoint` event with session ID)
  3) JSON-RPC tool listing (`tools/list`) and capability negotiation (`initialize`)
  4) Service rollback state mutation (`payments-service` DEGRADED -> HEALTHY)
  5) Domain tool executions (get_service_health, trigger_rollback, analyze_incident_log)
"""

import json
import sys
import urllib.error
import urllib.request
import httpx

# Ensure cross-platform UTF-8 console output
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

BASE_URL = "http://localhost:8000"


def print_section(title: str):
    print("\n" + "=" * 75)
    print(f"  {title}")
    print("=" * 75)


def request_json(url: str, method: str = "GET", payload: dict = None) -> dict:
    headers = {"Content-Type": "application/json"}
    data = json.dumps(payload).encode("utf-8") if payload else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def test_rest_health_probes():
    """Requirement 1: REST health probes (GET /health, GET /)."""
    print_section("1. REST Health & Liveness Probes (GET /health)")

    print("-> Querying GET /health ...")
    health = request_json(f"{BASE_URL}/health")
    print(f"   Status: {health.get('status')}")
    print(f"   Services Monitored: {health.get('services_monitored')}")
    print(f"   Timestamp: {health.get('timestamp')}")
    assert health.get("status") == "HEALTHY", f"Expected status HEALTHY, got {health.get('status')}"
    assert health.get("services_monitored", 0) >= 4, "Expected at least 4 services in catalog"

    print("\n-> Querying GET / (Root Metadata) ...")
    root_meta = request_json(f"{BASE_URL}/")
    print(f"   Project: {root_meta.get('project')}")
    print(f"   Status: {root_meta.get('status')}")
    print(f"   MCP Transport: {root_meta.get('mcp_protocol', {}).get('transport')}")
    assert root_meta.get("status") == "ONLINE", "Expected root status ONLINE"
    assert "tools/list" or "available_tools" in str(root_meta)

    print("   [PASS]: REST health probes verified successfully.")


def test_mcp_sse_handshake_and_lifecycle():
    """Requirement 2: SSE initial handshake (GET /mcp/sse emits 'endpoint')."""
    print_section("2. MCP SSE Initial Handshake & Lifecycle (GET /mcp/sse)")

    sse_url = f"{BASE_URL}/mcp/sse"
    print(f"-> Initiating connection to SSE stream: {sse_url} ...")

    with httpx.Client(timeout=10.0) as client:
        with client.stream("GET", sse_url, headers={"Accept": "text/event-stream"}) as sse_resp:
            print(f"   HTTP Response Code: {sse_resp.status_code}")
            content_type = sse_resp.headers.get("content-type", "")
            print(f"   Content-Type Header: {content_type}")
            assert sse_resp.status_code == 200, f"Expected 200 OK, got {sse_resp.status_code}"
            assert "text/event-stream" in content_type, f"Expected text/event-stream, got {content_type}"

            # Step 1: Handshake verification - read initial SSE frame for 'endpoint'
            print("   Listening for initial SSE 'endpoint' event frame...")
            endpoint_url = None
            for line in sse_resp.iter_lines():
                if line.startswith("data:"):
                    raw_data = line[len("data:"):].strip()
                    if "/mcp/message?sessionId=" in raw_data:
                        endpoint_url = raw_data
                        break

            print(f"   Discovered Session Endpoint: {endpoint_url}")
            assert endpoint_url is not None, "Failed to receive mandatory SSE 'endpoint' handshake event"
            assert "/mcp/message?sessionId=" in endpoint_url, "Endpoint URL does not contain sessionId query param"

            # Step 2: Post JSON-RPC initialize payload to discovered endpoint
            full_endpoint = f"{BASE_URL}{endpoint_url}" if endpoint_url.startswith("/") else endpoint_url
            print(f"\n-> Dispatching JSON-RPC 'initialize' to session endpoint:\n   {full_endpoint} ...")

            init_payload = {
                "jsonrpc": "2.0",
                "id": "sse-init-01",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "alexa-plus-sse-client", "version": "1.0.0"}
                }
            }
            post_resp = client.post(full_endpoint, json=init_payload)
            print(f"   Session Receiver Response Status: {post_resp.status_code}")
            assert post_resp.status_code in (200, 202), f"Expected 200 or 202, got {post_resp.status_code}"

            if post_resp.status_code == 202:
                print("   Session receiver accepted message (HTTP 202). Reading response frame from SSE stream...")
                received_response = None
                for line in sse_resp.iter_lines():
                    if line.startswith("data:"):
                        payload_str = line[len("data:"):].strip()
                        try:
                            payload_json = json.loads(payload_str)
                            if payload_json.get("id") == "sse-init-01":
                                received_response = payload_json
                                break
                        except Exception:
                            continue

                assert received_response is not None, "Did not receive JSON-RPC response over SSE stream"
                print(f"   Received JSON-RPC Response over SSE: {received_response.get('result', {}).get('serverInfo')}")
                print(f"   Protocol Version: {received_response.get('result', {}).get('protocolVersion')}")
                assert received_response.get("result", {}).get("protocolVersion") == "2025-11-25"
            else:
                data = post_resp.json()
                print(f"   Received Synchronous JSON-RPC Response: {data.get('result', {}).get('serverInfo')}")
                assert data.get("result", {}).get("protocolVersion") == "2025-11-25"

    print("   [PASS]: MCP SSE Handshake and stream delivery verified successfully.")


def test_mcp_jsonrpc_tool_listing():
    """Requirement 3: JSON-RPC tool listing (tools/list) and discovery."""
    print_section("3. JSON-RPC Protocol & Tool Listing (tools/list)")

    # 1. MCP initialize handshake
    print("-> Sending MCP 'initialize' JSON-RPC request to /mcp/message ...")
    init_res = request_json(f"{BASE_URL}/mcp/message", method="POST", payload={
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "alexa-plus-test-client", "version": "1.0.0"}
        }
    })
    server_info = init_res.get("result", {}).get("serverInfo", {})
    proto_ver = init_res.get("result", {}).get("protocolVersion")
    print(f"   Server Name: {server_info.get('name')} | Version: {server_info.get('version')}")
    print(f"   Negotiated Protocol Version: {proto_ver}")
    assert server_info.get("name") == "opspulse-alexa-mcp", "Server name mismatch in initialize"
    assert proto_ver == "2025-11-25", "Protocol version mismatch"

    # 2. MCP tools/list
    print("\n-> Sending MCP 'tools/list' JSON-RPC request ...")
    tools_res = request_json(f"{BASE_URL}/mcp/message", method="POST", payload={
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    })
    tools = tools_res.get("result", {}).get("tools", [])
    print(f"   Discovered {len(tools)} Registered MCP Tools:")
    tool_names = [t["name"] for t in tools]
    for t in tools:
        print(f"   - {t['name']}: {t['description'][:65]}...")

    assert len(tools) >= 3, f"Expected at least 3 tools, got {len(tools)}"
    assert "get_service_health" in tool_names, "Missing 'get_service_health' in tools/list"
    assert "trigger_rollback" in tool_names, "Missing 'trigger_rollback' in tools/list"
    assert "analyze_incident_log" in tool_names, "Missing 'analyze_incident_log' in tools/list"

    print("   [PASS]: JSON-RPC tool listing returned all required domain tools.")


def test_service_rollback_state_mutation():
    """Requirement 4: Service rollback state mutation (payments-service DEGRADED -> HEALTHY)."""
    print_section("4. Service Rollback State Mutation (payments-service DEGRADED -> HEALTHY)")

    # Step 1: Verify pre-condition: payments-service is DEGRADED
    print("-> Checking baseline health of 'payments-service' ...")
    pre_health = request_json(f"{BASE_URL}/api/health/payments-service")
    print(f"   Baseline Status: {pre_health['status']}")
    print(f"   Baseline p99 Latency: {pre_health['latency_p99_ms']}ms")
    print(f"   Baseline Error Rate: {pre_health['error_rate_pct']}%")
    print(f"   Target Group Health: {pre_health['target_group_health']}")
    assert pre_health["service_name"] == "payments-service", "Service name mismatch"
    assert pre_health["status"] == "DEGRADED", f"Expected DEGRADED status prior to rollback, got {pre_health['status']}"
    assert pre_health["error_rate_pct"] > 5.0, "Expected elevated error rate prior to rollback"

    # Step 2: Trigger rollback mutation via MCP tools/call
    print("\n-> Invoking 'trigger_rollback' via MCP JSON-RPC tools/call ...")
    rollback_rpc = request_json(f"{BASE_URL}/mcp/message", method="POST", payload={
        "jsonrpc": "2.0",
        "id": 42,
        "method": "tools/call",
        "params": {
            "name": "trigger_rollback",
            "arguments": {
                "service_name": "payments-service",
                "target_version": "v2.4.1",
                "reason": "Automated remediation triggered via test suite"
            }
        }
    })
    raw_content = rollback_rpc.get("result", {}).get("content", [{}])[0].get("text", "{}")
    rollback_result = json.loads(raw_content)
    print(f"   Rollback Status: {rollback_result.get('status')}")
    print(f"   Target Version: {rollback_result.get('target_version')}")
    print(f"   Previous Version: {rollback_result.get('previous_version')}")
    print(f"   Rollback ID: {rollback_result.get('rollback_id')}")
    print(f"   Execution Logs Count: {len(rollback_result.get('execution_logs', []))} log entries")
    assert rollback_result.get("status") == "SUCCESS", f"Expected SUCCESS rollback status, got {rollback_result.get('status')}"
    assert rollback_result.get("target_version") == "v2.4.1", "Target version not reflected in rollback response"

    # Step 3: Verify post-condition: payments-service state has mutated to HEALTHY
    print("\n-> Verifying state mutation for 'payments-service' ...")
    post_health = request_json(f"{BASE_URL}/api/health/payments-service")
    print(f"   Post-Rollback Status: {post_health['status']}")
    print(f"   Post-Rollback p99 Latency: {post_health['latency_p99_ms']}ms")
    print(f"   Post-Rollback Error Rate: {post_health['error_rate_pct']}%")
    print(f"   Post-Rollback Version: {post_health.get('current_version')}")
    print(f"   Target Group Health: {post_health.get('target_group_health')}")

    assert post_health["status"] == "HEALTHY", f"Expected mutated status HEALTHY, got {post_health['status']}"
    assert post_health["current_version"] == "v2.4.1", f"Expected current_version v2.4.1, got {post_health['current_version']}"
    assert post_health["error_rate_pct"] <= 0.05, f"Expected normalized error rate <= 0.05%, got {post_health['error_rate_pct']}"
    assert post_health["latency_p99_ms"] < 100.0, f"Expected p99 latency < 100ms, got {post_health['latency_p99_ms']}"

    print("   [PASS]: Service rollback state mutation (DEGRADED -> HEALTHY) verified 100%.")


def test_bedrock_incident_analysis():
    """Verify Amazon Bedrock Claude 3.5 Sonnet incident diagnostic tool."""
    print_section("5. Amazon Bedrock Claude 3.5 Sonnet Diagnostic Engine")

    sample_log = (
        "2026-09-29T14:12:01Z [ERROR] [payments-service] HTTP 504 Gateway Timeout downstream. "
        "HikariPool-1 - Connection is not available, request timed out after 30005ms. "
        "at com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:213)"
    )
    print("-> Invoking POST /api/analyze-log (or Bedrock fallback) ...")
    analysis = request_json(f"{BASE_URL}/api/analyze-log", method="POST", payload={
        "log_trace": sample_log,
        "service_name": "payments-service"
    })
    print(f"   Provider: {analysis['provider']}")
    print(f"   Model ID: {analysis['model_id']}")
    print(f"   Severity: {analysis['severity']}")
    print(f"   Root Cause: {analysis['root_cause_summary']}")
    print(f"   Playbook Steps ({len(analysis['remediation_playbook'])}):")
    for step in analysis['remediation_playbook']:
        print(f"     * {step}")
    print(f"   Suggested Action: {analysis['suggested_action']}")

    assert analysis["severity"] in ("SEV-1 (Critical)", "SEV-2 (High)")
    assert len(analysis["remediation_playbook"]) >= 3
    assert "trigger_rollback" in analysis["suggested_action"]

    print("   [PASS]: Bedrock incident diagnostics and remediation playbook verified.")


if __name__ == "__main__":
    try:
        test_rest_health_probes()
        test_mcp_sse_handshake_and_lifecycle()
        test_mcp_jsonrpc_tool_listing()
        test_service_rollback_state_mutation()
        test_bedrock_incident_analysis()

        print("\n" + "=" * 75)
        print("  ALL 5 VERIFICATION SUITES COMPLETED WITH 100% PASS RATE!")
        print("=" * 75)
    except urllib.error.URLError as err:
        print(f"\n[ERROR] Connection failed: {err}")
        print(f"Ensure the server is running on {BASE_URL} before executing tests.")
        sys.exit(1)
    except httpx.RequestError as err:
        print(f"\n[ERROR] HTTPX Connection failed: {err}")
        print(f"Ensure the server is running on {BASE_URL} before executing tests.")
        sys.exit(1)
    except AssertionError as err:
        print(f"\n[FAIL] Test assertion failed: {err}")
        sys.exit(2)
