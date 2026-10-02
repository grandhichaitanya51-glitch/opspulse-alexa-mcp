"""OpsPulse for Alexa+ End-to-End Verification Test Client.

Exercises:
  1. Server health probe and metadata discovery
  2. Direct REST endpoints (get_service_health, trigger_rollback, analyze_incident_log)
  3. MCP JSON-RPC 2.0 protocol over HTTP (/mcp/message)
  4. MCP Streamable HTTP / Server-Sent Events (SSE) lifecycle and session dispatch (/mcp/sse)
"""

import json
import sys
import urllib.error
import urllib.request
import httpx

BASE_URL = "http://localhost:8000"


def print_section(title: str):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def request_json(url: str, method: str = "GET", payload: dict = None) -> dict:
    headers = {"Content-Type": "application/json"}
    data = json.dumps(payload).encode("utf-8") if payload else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def test_rest_endpoints():
    print_section("1. Testing REST Endpoints")

    # 1. Root / Health
    print("-> Querying GET /health ...")
    health = request_json(f"{BASE_URL}/health")
    print(f"   Response Status: {health.get('status')}")

    # 2. Get Service Health
    print("\n-> Querying GET /api/health/payment-gateway ...")
    svc_health = request_json(f"{BASE_URL}/api/health/payment-gateway")
    print(f"   Service: {svc_health['service_name']} | Status: {svc_health['status']} | p99: {svc_health['latency_p99_ms']}ms | Error Rate: {svc_health['error_rate_pct']}%")

    # 3. Analyze Incident Log (Bedrock)
    print("\n-> Querying POST /api/analyze-log (AWS Bedrock Claude 3.5 Sonnet) ...")
    sample_log = (
        "2026-09-29T14:12:01Z [ERROR] [payment-gateway] HTTP 504 Gateway Timeout downstream. "
        "HikariPool-1 - Connection is not available, request timed out after 30005ms. "
        "at com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:213)"
    )
    analysis = request_json(f"{BASE_URL}/api/analyze-log", method="POST", payload={
        "log_trace": sample_log,
        "service_name": "payment-gateway"
    })
    print(f"   Provider: {analysis['provider']}")
    print(f"   Severity: {analysis['severity']}")
    print(f"   Root Cause: {analysis['root_cause_summary']}")
    print(f"   Playbook Steps: {len(analysis['remediation_playbook'])} steps returned")
    print(f"   Primary Suggestion: {analysis['suggested_action']}")

    # 4. Trigger Canary Rollback
    print("\n-> Querying POST /api/rollback ...")
    rollback = request_json(f"{BASE_URL}/api/rollback", method="POST", payload={
        "service_name": "payment-gateway",
        "target_version": "v2.4.1",
        "reason": "Automated remediation following p99 latency breach"
    })
    print(f"   Rollback Status: {rollback['status']} | Reverted to: {rollback['target_version']}")
    print(f"   Execution Logs Count: {len(rollback['execution_logs'])} entries")


def test_mcp_jsonrpc():
    print_section("2. Testing MCP Protocol Endpoints (/mcp/message)")

    # 1. MCP initialize
    print("-> Sending MCP 'initialize' JSON-RPC request ...")
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
    print(f"   Server Info: {init_res.get('result', {}).get('serverInfo')}")
    print(f"   Protocol Version: {init_res.get('result', {}).get('protocolVersion')}")

    # 2. MCP tools/list
    print("\n-> Sending MCP 'tools/list' request ...")
    tools_res = request_json(f"{BASE_URL}/mcp/message", method="POST", payload={
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    })
    tools = tools_res.get("result", {}).get("tools", [])
    print(f"   Discovered {len(tools)} MCP Tools:")
    for t in tools:
        print(f"   - {t['name']}: {t['description'][:65]}...")

    # 3. MCP tools/call -> get_service_health
    print("\n-> Sending MCP 'tools/call' (get_service_health) ...")
    call_res = request_json(f"{BASE_URL}/mcp/message", method="POST", payload={
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "get_service_health",
            "arguments": {"service_name": "auth-service"}
        }
    })
    raw_content = call_res.get("result", {}).get("content", [{}])[0].get("text", "{}")
    tool_output = json.loads(raw_content)
    print(f"   Tool Response: Service '{tool_output.get('service_name')}' Status = {tool_output.get('status')}")


def test_mcp_sse_lifecycle():
    print_section("3. Testing MCP SSE Stream Lifecycle & Session Dispatch")

    sse_url = f"{BASE_URL}/mcp/sse"
    print(f"-> Connecting to SSE stream: {sse_url} ...")

    with httpx.Client(timeout=10.0) as client:
        with client.stream("GET", sse_url, headers={"Accept": "text/event-stream"}) as sse_resp:
            print(f"   HTTP Status: {sse_resp.status_code}")
            content_type = sse_resp.headers.get("content-type", "")
            print(f"   Content-Type: {content_type}")
            assert sse_resp.status_code == 200, f"Expected 200, got {sse_resp.status_code}"
            assert "text/event-stream" in content_type, f"Expected text/event-stream, got {content_type}"

            # Read initial SSE events to extract endpoint URL
            endpoint_url = None
            for line in sse_resp.iter_lines():
                if line.startswith("data:"):
                    raw_data = line[len("data:"):].strip()
                    if "/mcp/message?sessionId=" in raw_data:
                        endpoint_url = raw_data
                        break

            print(f"   Discovered Session Endpoint: {endpoint_url}")
            assert endpoint_url is not None, "Failed to receive endpoint event with sessionId"

            # Dispatch JSON-RPC 'initialize' to the session endpoint
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

            # If 202 Accepted, read the response frame from the active SSE stream
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
                print(f"   Protocol Version: {data.get('result', {}).get('protocolVersion')}")
                assert data.get("result", {}).get("protocolVersion") == "2025-11-25"

    print("\n   SSE Stream lifecycle & bidirectional event delivery verified successfully!")


if __name__ == "__main__":
    try:
        test_rest_endpoints()
        test_mcp_jsonrpc()
        test_mcp_sse_lifecycle()
        print("\n" + "=" * 70)
        print("  ALL VERIFICATION TESTS COMPLETED SUCCESSFULLY!")
        print("=" * 70)
    except urllib.error.URLError as err:
        print(f"\n[ERROR] Connection failed: {err}")
        print(f"Ensure the server is running on {BASE_URL} before executing tests.")
        sys.exit(1)
    except httpx.RequestError as err:
        print(f"\n[ERROR] HTTPX Connection failed: {err}")
        print(f"Ensure the server is running on {BASE_URL} before executing tests.")
        sys.exit(1)
