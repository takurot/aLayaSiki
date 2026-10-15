"""Exercise an installed native wheel executable, including its real stdio database."""
import argparse
import json
import subprocess
import tempfile


def run(command, *, messages=None):
    result = subprocess.run(command, input=messages, text=True, capture_output=True,
                            timeout=120, check=False)
    if result.returncode:
        raise RuntimeError(f"MCP executable failed ({result.returncode}): {result.stderr}")
    return result.stdout


def request(identifier, method, params):
    return {"jsonrpc": "2.0", "id": identifier, "method": method, "params": params}


def tool(identifier, name, arguments):
    return request(identifier, "tools/call", {"name": name, "arguments": arguments})


def tool_value(reply):
    result = reply["result"]
    assert not result.get("isError", False), result
    assert result["content"][0]["type"] == "text", result
    return json.loads(result["content"][0]["text"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", default="0.3.0")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("provide executable command after --")
    assert run(command + ["--version"]).strip() == f"alayasiki-mcp {args.version}"
    assert "stdio" in run(command + ["--help"])
    frames = [
        request(1, "initialize", {
            "protocolVersion": "2024-11-05", "capabilities": {},
            "clientInfo": {"name": "wheel-smoke", "version": "1"},
        }),
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        request(2, "tools/list", {}),
        tool(3, "alayasiki_ingest", {
            "content": "Rust startup acquired a company for graph technology.",
            "metadata": {"source": "wheel-smoke.md"}, "idempotency_key": "wheel-smoke",
        }),
        tool(4, "alayasiki_query", {"query": "Rust startup", "mode": "evidence"}),
        tool(5, "alayasiki_session_write", {
            "session_id": "wheel-session", "content": "Temporary agent memory about Rust.",
        }),
        tool(6, "alayasiki_session_read", {"session_id": "wheel-session"}),
    ]
    with tempfile.TemporaryDirectory(prefix="alayasiki-wheel-") as data:
        stdout = run(command + ["--data-dir", data], messages="".join(
            json.dumps(frame) + "\n" for frame in frames
        ))
    replies = [json.loads(line) for line in stdout.splitlines()]
    assert [reply["id"] for reply in replies] == [1, 2, 3, 4, 5, 6], replies
    assert all(reply["jsonrpc"] == "2.0" and "error" not in reply for reply in replies)
    assert replies[0]["result"]["protocolVersion"] == "2024-11-05"
    names = {item["name"] for item in replies[1]["result"]["tools"]}
    assert {"alayasiki_query", "alayasiki_ingest", "alayasiki_session_write", "alayasiki_session_read"} <= names
    assert tool_value(replies[2])["node_ids"]
    assert any("Rust startup" in node["data"] for node in tool_value(replies[3])["evidence"]["nodes"])
    assert tool_value(replies[4])["node_ids"]
    assert tool_value(replies[5])["session_id"] == "wheel-session"
    assert any("Temporary agent memory" in node["data"] for node in tool_value(replies[5])["nodes"])
    print("Wheel smoke passed: version/help, initialize, tools/list, ingest/query, session write/read")


if __name__ == "__main__":
    main()
