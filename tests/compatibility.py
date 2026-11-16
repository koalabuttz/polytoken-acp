#!/usr/bin/env python3
"""Exercise an installed Polytoken through the shim with a local fake provider.

No API keys, external model calls, or changes to the user's configuration.
Usage: python3 tests/compatibility.py --shim target/debug/polytoken-acp
       --polytoken /path/to/polytoken
"""

import argparse
import json
import os
from pathlib import Path
import queue
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Provider(BaseHTTPRequestHandler):
    def handle(self):
        try:
            super().handle()
        except (BrokenPipeError, ConnectionResetError):
            pass  # Expected when the cancellation test closes an active stream.

    def log_message(self, *_):
        pass

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"object": "list", "data": [
            {"id": "fixture", "object": "model"},
        ]}).encode())

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/mcp":
            self.server.mcp_requests.append((body, self.headers.get("Authorization")))
            if body.get("method") == "tools/list":
                self.server.mcp_discovered.set()
            if "id" not in body:
                self.send_response(202)
                self.end_headers()
                return
            result = ({"protocolVersion": body["params"]["protocolVersion"],
                       "capabilities": {"tools": {}}, "serverInfo": {"name": "fixture", "version": "1"}}
                      if body["method"] == "initialize" else {"tools": []})
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"jsonrpc": "2.0", "id": body["id"], "result": result}).encode())
            return
        self.server.requests.append(body)
        if body.get("stream") and "exercise-cancel" in json.dumps(body.get("input")):
            self.server.slow_started.set()
            self.server.release.wait(timeout=10)
        self.send_response(200)
        message = {"id": "msg-fixture", "type": "message", "role": "assistant", "status": "completed",
                   "content": [{"type": "output_text", "text": "compat-ok", "annotations": []}]}
        response = {"id": "resp-fixture", "object": "response", "status": "completed", "model": "fixture",
                    "output": [message], "usage": {"input_tokens": 10, "output_tokens": 3,
                    "input_tokens_details": {"cached_tokens": 0}}}
        tool_turn = (body.get("stream") and "exercise-tool" in json.dumps(body.get("input"))
                     and not self.server.tool_fired)
        question_turn = (body.get("stream") and "exercise-question" in json.dumps(body.get("input"))
                         and not self.server.question_fired)
        if tool_turn:
            self.server.tool_fired = True
            message = {"id": "fc-fixture", "type": "function_call", "status": "completed",
                       "call_id": "call-fixture", "name": "shell_exec",
                       "arguments": json.dumps({"command": "printf compat-tool"})}
            response["output"] = [message]
        if question_turn:
            self.server.question_fired = True
            message = {"id": "fc-question", "type": "function_call", "status": "completed",
                       "call_id": "call-question", "name": "ask_user_question",
                       "arguments": json.dumps({"questions": [{"id": "q", "question": "Pick one", "mode": "single_select",
                           "allow_free_text": True, "options": [{"id": "one", "label": "One", "description": "First option"}]}]})}
            response["output"] = [message]
        background_cancel = (body.get("stream") and "exercise-background-cancel" in json.dumps(body.get("input"))
                             and not getattr(self.server, "background_cancel_fired", False))
        background_turn = (body.get("stream") and "exercise-background" in json.dumps(body.get("input"))
                           and not getattr(self.server, "background_fired", False))
        if background_cancel or background_turn:
            if background_cancel:
                self.server.background_cancel_fired = True
            else:
                self.server.background_fired = True
            call = "fc-background-cancel" if background_cancel else "fc-background"
            command = "sleep 30; printf cancelled-too-late" if background_cancel else "sleep 2; printf background-done"
            message = {"id":call,"type":"function_call","status":"completed","call_id":call,
                       "name":"shell_exec","arguments":json.dumps({"command":command,"background":True})}
            response["output"] = [message]
        if body.get("stream"):
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            events = [
                ("response.created", {"response": {**response, "status": "in_progress", "output": []}}),
                ("response.output_item.added", {"output_index": 0, "item": {**message, "content": []}}),
                ("response.content_part.added", {"output_index": 0, "content_index": 0, "item_id": "msg-fixture",
                    "part": {"type": "output_text", "text": "", "annotations": []}}),
                ("response.output_text.delta", {"output_index": 0, "content_index": 0, "item_id": "msg-fixture", "delta": "compat-ok"}),
                ("response.output_text.done", {"output_index": 0, "content_index": 0, "item_id": "msg-fixture", "text": "compat-ok"}),
                ("response.output_item.done", {"output_index": 0, "item": message}),
                ("response.completed", {"response": response}),
            ]
            if tool_turn or question_turn:
                events = [
                    ("response.created", {"response": {**response, "status": "in_progress", "output": []}}),
                    ("response.output_item.added", {"output_index": 0, "item": {**message, "arguments": ""}}),
                    ("response.function_call_arguments.delta", {"output_index": 0, "item_id": "fc-fixture", "delta": message["arguments"]}),
                    ("response.function_call_arguments.done", {"output_index": 0, "item_id": "fc-fixture", "arguments": message["arguments"]}),
                    ("response.output_item.done", {"output_index": 0, "item": message}),
                    ("response.completed", {"response": response}),
                ]
            for kind, data in events:
                self.wfile.write(("event: " + kind + "\ndata: " + json.dumps({"type": kind, **data}) + "\n\n").encode())
        else:
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())


class Client:
    def __init__(self, shim, env, cwd, log):
        self.process = subprocess.Popen([shim], env=env, cwd=cwd, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=log, text=True)
        self.messages = queue.Queue()
        self.protocol = 1
        self.elicitation = False
        self.counter = 0
        self.notifications = []
        threading.Thread(target=self.read, daemon=True).start()

    def read(self):
        for line in self.process.stdout:
            self.messages.put(json.loads(line))
        self.messages.put(None)

    def request(self, method, params):
        original_method = method
        if self.protocol == 2:
            if method == "initialize":
                params = {"protocolVersion": 2, "info": {"name": "compat-test", "version": "1"}, "capabilities": {"elicitation":{"form":{}}} if self.elicitation else {}}
            elif method == "session/load":
                method = "session/resume"
                params = {**params, "replayFrom": {"type": "start"}}
            elif method == "session/set_mode":
                method = "session/set_config_option"
                params = {"sessionId": params["sessionId"], "configId": "permission_mode", "type":"select", "value": params["modeId"]}
        if self.protocol == 2 and method == "session/set_config_option" and "type" not in params:
            params = {**params,"type":"select"}
        self.counter += 1
        self.process.stdin.write(json.dumps({"jsonrpc": "2.0", "id": self.counter,
                                            "method": method, "params": params}) + "\n")
        self.process.stdin.flush()
        accepted = None
        while True:
            msg = self.messages.get(timeout=45)
            assert msg is not None, f"Shim exited during {method}"
            if "method" in msg:
                self.notifications.append(msg)
                update = msg.get("params", {}).get("update", {})
                if self.protocol == 2 and original_method == "session/prompt" and update.get("sessionUpdate") == "state_update" and update.get("state") == "idle":
                    assert accepted is not None, "v2 completion arrived before prompt insertion acknowledgement"
                    return {"stopReason": update.get("stopReason"), "messageId": accepted["messageId"]}
                if "id" in msg:
                    if msg["method"] == "session/request_permission":
                        result = {"outcome": {"outcome": "selected", "optionId": "allow_once"}}
                    elif msg["method"] == "elicitation/create":
                        result = {"action":"accept","content":{"q":"one"}}
                    else:
                        assert msg["method"] == "_polytoken/ask_user_question", msg
                        result = {"answers": [{"question_id": "q", "selected_option_ids": ["one"]}]}
                    self.process.stdin.write(json.dumps({"jsonrpc": "2.0", "id": msg["id"],
                        "result": result}) + "\n")
                    self.process.stdin.flush()
            elif msg.get("id") == self.counter:
                assert "error" not in msg, (method, msg)
                result = msg["result"]
                if self.protocol == 2:
                    if original_method == "session/prompt":
                        assert "messageId" in result and "stopReason" not in result, result
                        accepted = result
                        continue
                    def legacy_options(value):
                        if isinstance(value, dict):
                            if "configId" in value: value["id"] = value["configId"]
                            for v in value.values(): legacy_options(v)
                        elif isinstance(value, list):
                            for v in value: legacy_options(v)
                    legacy_options(result)
                return result

    def close(self):
        self.process.stdin.close()
        try:
            self.process.wait(timeout=10)
        finally:
            if self.process.poll() is None:
                self.process.kill()
                self.process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shim", required=True)
    parser.add_argument("--polytoken", required=True)
    parser.add_argument("--protocol", type=int, choices=[1, 2], default=1)
    parser.add_argument("--elicitation", action="store_true")
    args = parser.parse_args()
    shim, polytoken = str(Path(args.shim).resolve()), str(Path(args.polytoken).resolve())
    print(subprocess.check_output([polytoken, "--version"], text=True).strip())
    server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
    server.requests = []
    server.mcp_requests = []
    server.mcp_discovered = threading.Event()
    server.tool_fired = False
    server.question_fired = False
    server.slow_started = threading.Event()
    server.release = threading.Event()
    threading.Thread(target=server.serve_forever, daemon=True).start()
    with tempfile.TemporaryDirectory(prefix="polytoken-acp-compat-") as temp:
        root = Path(temp)
        config = root / "config" / "polytoken"
        config.mkdir(parents=True)
        (config / "permissions.yaml").write_text("version: 2\nask:\n  - tool: shell_exec\n")
        bin_dir = root / "bin"
        bin_dir.mkdir()
        (bin_dir / "polytoken").symlink_to(polytoken)
        model = {"provider": "fixture", "provider_name": "fixture", "class": "full",
                 "context_window": 128000, "max_tokens": 4096,
                 "reasoning": {"type": "effort", "effort_set": "gpt_6_sol", "levels": ["low", "high"],
                               "default_level": "low", "can_disable": True}}
        (config / "config.json").write_text(json.dumps({"version": 4,
            "providers": {"fixture": {"kind": {"type": "custom_open_ai_responses"},
                "url": f"http://127.0.0.1:{server.server_port}/v1", "auth": {"type": "no_auth"}}},
            "models": {"fixture": model, "fixture-two": model},
            "modelgroups": {"polytoken:default_model_full": "fixture"},
            "updates": {"automatic_check": False}, "data": {"automatic_crash_reporting": False}}))
        env = dict(os.environ, HOME=str(root), XDG_CONFIG_HOME=str(root / "config"),
                   XDG_DATA_HOME=str(root / "data"), TMPDIR=str(root),
                   PATH=str(bin_dir) + os.pathsep + os.environ["PATH"], RUST_LOG="polytoken_acp=debug")
        client = None
        with (root / "shim.log").open("w+") as log:
            try:
                client = Client(shim, env, root, log)
                client.protocol = args.protocol
                client.elicitation = args.elicitation
                client.request("initialize", {"protocolVersion": 1, "clientCapabilities": {}})
                mcp_servers = [{"type": "http", "name": "fixture_mcp", "url": f"http://127.0.0.1:{server.server_port}/mcp",
                                "headers": [{"name": "Authorization", "value": "Bearer fixture-mcp-token"}]}]
                session = client.request("session/new", {"cwd": str(root), "mcpServers": mcp_servers})
                sid = session["sessionId"]
                startup = json.loads(next(root.rglob("startup.json")).read_text())
                base_url = f"http://127.0.0.1:{startup['port']}"
                try:
                    urllib.request.urlopen(base_url + "/health", timeout=5)
                except urllib.error.HTTPError as error:
                    assert error.code == 401, error
                else:
                    raise AssertionError("Daemon accepted an unauthenticated health request")
                assert any(o["id"] == "model" for o in session["configOptions"]), session
                print("PASS: startup, credential validation, model/mode discovery")
                assert any(o["id"] == "mcp:fixture_mcp" for o in session["configOptions"]), session
                client.request("session/set_config_option", {"sessionId": sid, "configId": "mcp:fixture_mcp", "type": "boolean", "value": True})
                assert server.mcp_discovered.wait(timeout=5), server.mcp_requests
                assert any(b["method"] == "tools/list" and auth == "Bearer fixture-mcp-token" for b, auth in server.mcp_requests), server.mcp_requests
                print("PASS: HTTP MCP config forwarding, authentication and discovery")
                changed = client.request("session/set_config_option", {"sessionId": sid, "configId": "thought_level", "value": "high"})
                assert next(o for o in changed["configOptions"] if o["id"] == "thought_level")["currentValue"] == "high", changed
                client.request("session/set_config_option", {"sessionId": sid, "configId": "model", "value": "fixture-two"})
                client.request("session/set_mode", {"sessionId": sid, "modeId": "bypass"})
                print("PASS: reasoning effort, model switching, permission mode")
                prompt = client.request("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "Reply compat-ok"}]})
                assert prompt["stopReason"] == "end_turn", prompt
                assert any(n.get("params", {}).get("update", {}).get("sessionUpdate") in ("agent_message_chunk", "agent_message")
                           and "compat-ok" in json.dumps(n["params"]["update"]["content"])
                           for n in client.notifications), client.notifications
                print("PASS: prompt submission and streamed response")
                client.request("session/set_mode", {"sessionId": sid, "modeId": "standard"})
                before_tool = len(client.notifications)
                client.request("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "exercise-tool"}]})
                tool_notifications = client.notifications[before_tool:]
                results = [n["params"]["update"] for n in tool_notifications
                           if n.get("params", {}).get("update", {}).get("sessionUpdate") == "tool_call_update"]
                assert any(r.get("status") == "completed" and "compat-tool" in json.dumps(r.get("content")) for r in results), results
                assert any(n.get("method") == "session/request_permission" for n in tool_notifications), tool_notifications
                print("PASS: tool execution, permission round-trip, structured tool result")
                before_question = len(client.notifications)
                client.request("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "exercise-question"}]})
                assert any(n.get("method") in ("_polytoken/ask_user_question","elicitation/create") for n in client.notifications[before_question:])
                print("PASS: structured question extension and answer submission")
                if args.protocol == 2:
                    before_resync = len(client.notifications)
                    resynced = client.request("session/resume", {"sessionId":sid,"cwd":str(root),"mcpServers":mcp_servers})
                    assert resynced.get("availableCommands"), resynced
                    assert not any(n.get("params",{}).get("update",{}).get("sessionUpdate") in ("agent_message", "agent_message_chunk") for n in client.notifications[before_resync:])
                    replay = client.request("session/load", {"sessionId":sid,"cwd":str(root),"mcpServers":mcp_servers})
                    assert replay.get("configOptions"), replay
                    assert any(n.get("params",{}).get("update",{}).get("sessionUpdate") == "replay_complete" for n in client.notifications[before_resync:])
                    print("PASS: active resume, optional replay and complete setup lists")
                client.request("session/close", {"sessionId": sid})
                before = len(client.notifications)
                loaded = client.request("session/load", {"sessionId": sid, "cwd": str(root), "mcpServers": mcp_servers})
                startup = json.loads(next(root.rglob("startup.json")).read_text())
                base_url = f"http://127.0.0.1:{startup['port']}"
                assert loaded.get("configOptions"), loaded
                assert any(n.get("params", {}).get("update", {}).get("sessionUpdate") in ("agent_message_chunk", "agent_message")
                           for n in client.notifications[before:]), client.notifications[before:]
                print("PASS: close, resume, saved transcript replay")
                def cancel_when_started():
                    if server.slow_started.wait(timeout=10):
                        client.process.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "session/cancel",
                                                               "params": {"sessionId": sid}}) + "\n")
                        client.process.stdin.flush()
                threading.Thread(target=cancel_when_started, daemon=True).start()
                cancelled = client.request("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "exercise-cancel"}]})
                assert cancelled["stopReason"] == "cancelled", cancelled
                server.release.set()
                credential = json.loads(next(root.glob("polytoken-acp-*/credential.json")).read_text())
                deadline = time.monotonic() + 5
                while True:
                    request = urllib.request.Request(base_url + "/state", headers={"Authorization": "Bearer " + credential["token"]})
                    with urllib.request.urlopen(request, timeout=5) as response:
                        busy = json.load(response).get("turn_in_flight", False)
                    if not busy:
                        break
                    assert time.monotonic() < deadline, "Cancellation did not stop the daemon turn"
                    time.sleep(0.05)
                print("PASS: in-flight cancellation")
                client.request("session/close", {"sessionId": sid})
            except BaseException:
                log.flush()
                log.seek(0)
                print(log.read()[-16000:])
                print("Session files:", [str(p.relative_to(root)) for p in root.rglob("startup.json")])
                raise
            finally:
                server.release.set()
                if client:
                    client.close()
                server.shutdown()


if __name__ == "__main__":
    main()
