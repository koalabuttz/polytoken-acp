#!/usr/bin/env python3
"""Controlled real-daemon worker audit and native ACP child-session test.

All model replies are served locally. No hosted provider or credentials.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from v2_protocol import Wire


class WorkerWire(Wire):
    """Hold real permission requests so replay/cancellation can run while blocked."""
    def __init__(self,*args):
        super().__init__(*args)
        self.permissions=[]
        self.cancel_during_replay=None
    def pump(self,timeout=25):
        message=self.messages.get(timeout=timeout)
        assert message is not None,"Shim exited unexpectedly"
        if "method" not in message:self.responses[message["id"]]=message
        else:
            self.notifications.append(message)
            update=message.get("params",{}).get("update",{})
            if self.cancel_during_replay and update.get("sessionUpdate")=="subagent_update" and update.get("sessionId")==self.cancel_during_replay and update.get("capabilities")=={}:
                self.process.stdin.write(json.dumps({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":self.cancel_during_replay}})+"\n")
                self.process.stdin.flush();self.cancel_during_replay=None
            if "id" in message:
                assert message["method"]=="session/request_permission",message
                self.permissions.append(message)
        return message
    def approve(self):
        for message in self.permissions:
            self.process.stdin.write(json.dumps({"jsonrpc":"2.0","id":message["id"],"result":{"outcome":{"outcome":"selected","optionId":"allow_once"}}})+"\n")
        self.process.stdin.flush();self.permissions.clear()


def function(call, name, arguments):
    return {"id":call,"type":"function_call","status":"completed","call_id":call,
            "name":name,"arguments":json.dumps(arguments)}


class Provider(BaseHTTPRequestHandler):
    def log_message(self,*_):pass
    def handle(self):
        try:super().handle()
        except (BrokenPipeError,ConnectionResetError):pass
    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append(body)
        text=json.dumps(body.get("input",[]))
        returned='"type": "function_call_output"' in text
        if body["model"]=="root" and not returned:
            output=[function("spawn-parent","subagent",{"subagent_type":"fixture-worker","prompt":"parent-worker","model_override":"fixture-child","cwd":str(self.server.worktree)}),
                    function("spawn-slow","subagent",{"subagent_type":"fixture-worker","prompt":"slow-worker","model_override":"fixture-child","cwd":str(self.server.worktree)})]
        elif body["model"]=="root" and self.server.root_wait and '"name": "job_block"' not in text:
            outputs=[x.get("output","") for x in body.get("input",[]) if x.get("type")=="function_call_output"]
            handle=next(json.loads(x)["handles"][0] for x in outputs if '"handles"' in x)
            output=[function("root-wait","job_block",{"job_id":handle,"wait_seconds":15,"timeout_seconds":20})]
        elif body["model"]=="worker":
            if "slow-worker" in text:
                self.server.slow.set();self.server.release.wait(25)
            if "parent-worker" in text and not returned:
                output=[function("spawn-leaf","subagent",{"subagent_type":"fixture-worker","prompt":"leaf-worker","model_override":"fixture-child","cwd":str(self.server.worktree)})]
            elif "parent-worker" in text and '"name": "job_block"' not in text:
                outputs=[x.get("output","") for x in body.get("input",[]) if x.get("type")=="function_call_output"]
                handle=next(json.loads(x)["handles"][0] for x in outputs if '"handles"' in x)
                output=[function("wait-leaf","job_block",{"job_id":handle,"wait_seconds":15,"timeout_seconds":20})]
            elif "leaf-worker" in text and '"name": "pushd"' not in text:
                output=[function("leaf-pushd","pushd",{"path":str(self.server.worktree/"leaf")})]
            elif "leaf-worker" in text and '"name": "shell_exec"' not in text:
                output=[function("leaf-pwd","shell_exec",{"command":"pwd"})]
            else:output=[function("worker-exit","exit_tool",{"summary":"worker-result"})]
        else:
            output=[{"id":"root-answer","type":"message","role":"assistant","status":"completed",
                     "content":[{"type":"output_text","text":"root-done","annotations":[]}]}]
        response={"id":"fixture-response","object":"response","status":"completed","model":body["model"],"output":output,
                  "usage":{"input_tokens":10,"output_tokens":3,"input_tokens_details":{"cached_tokens":0}}}
        self.send_response(200);self.send_header("Content-Type","text/event-stream" if body.get("stream") else "application/json");self.end_headers()
        if not body.get("stream"):
            self.wfile.write(json.dumps(response).encode());return
        events=[("response.created",{"response":{**response,"status":"in_progress","output":[]}})]
        for index,item in enumerate(output):
            if item["type"]=="function_call":
                events.extend([("response.output_item.added",{"output_index":index,"item":{**item,"arguments":""}}),
                               ("response.function_call_arguments.delta",{"output_index":index,"item_id":item["id"],"delta":item["arguments"]}),
                               ("response.function_call_arguments.done",{"output_index":index,"item_id":item["id"],"arguments":item["arguments"]})])
            else:
                events.extend([("response.output_item.added",{"output_index":index,"item":{**item,"content":[]}}),
                               ("response.output_text.delta",{"output_index":index,"content_index":0,"item_id":item["id"],"delta":"root-done"})])
            events.append(("response.output_item.done",{"output_index":index,"item":item}))
        events.append(("response.completed",{"response":response}))
        for event,data in events:self.wfile.write(f"event: {event}\ndata: {json.dumps({'type':event,**data})}\n\n".encode())
        self.wfile.flush()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shim",required=True);parser.add_argument("--polytoken",required=True)
    parser.add_argument("--audit",type=Path)
    parser.add_argument("--protocol",type=int,choices=[1,2],default=2)
    parser.add_argument("--legacy",action="store_true")
    parser.add_argument("--cancel-root",action="store_true")
    args=parser.parse_args()
    binary=str(Path(args.polytoken).resolve());shim=str(Path(args.shim).resolve())
    server=ThreadingHTTPServer(("127.0.0.1",0),Provider)
    server.requests=[];server.slow=threading.Event();server.release=threading.Event()
    server.root_wait=args.cancel_root
    threading.Thread(target=server.serve_forever,daemon=True).start()
    with tempfile.TemporaryDirectory(prefix="polytoken-acp-workers-") as directory:
        root=Path(directory);config=root/"config/polytoken";config.mkdir(parents=True)
        server.worktree=root/"worktree";server.worktree.mkdir()
        (server.worktree/"leaf").mkdir()
        bin_dir=root/"bin";bin_dir.mkdir();(bin_dir/"polytoken").symlink_to(binary)
        models={name:{"provider":"fixture","provider_name":provider,"class":"full","context_window":128000,"max_tokens":4096}
                for name,provider in [("fixture-root","root"),("fixture-child","worker")]}
        (config/"config.json").write_text(json.dumps({"version":4,"providers":{"fixture":{"kind":{"type":"custom_open_ai_responses"},
            "url":f"http://127.0.0.1:{server.server_port}/v1","auth":{"type":"no_auth"}}},"models":models,
            "modelgroups":{"polytoken:default_model_full":"fixture-root"},"updates":{"automatic_check":False},"data":{"automatic_crash_reporting":False}}))
        (config/"permissions.yaml").write_text("version: 2\nask:\n  - tool: shell_exec\n")
        profiles=root/".polytoken/subagents";profiles.mkdir(parents=True)
        (profiles/"fixture-worker.md").write_text('''---
name: fixture-worker
description: Controlled fixture worker
polytoken:
  model: fixture-child
  inherit_tools: true
  allow_subagent_spawn: true
  exit_tool_schema:
    type: object
    required: [summary]
    properties:
      summary:
        type: string
---
{{ prompt }}
''')
        env=dict(os.environ,HOME=str(root),XDG_CONFIG_HOME=str(root/"config"),XDG_DATA_HOME=str(root/"data"),TMPDIR=str(root),
                 PATH=str(bin_dir)+os.pathsep+os.environ["PATH"],RUST_LOG="polytoken_acp=debug")
        client=None
        with (root/"shim.log").open("w+") as log:
            try:
                client=WorkerWire(shim,env,root,log)
                if args.protocol==2:
                    client.call("initialize",{"protocolVersion":2,"info":{"name":"worker-fixture","version":"1"},"capabilities":{}})
                else:
                    client.call("initialize",{"protocolVersion":1,"clientInfo":{"name":"worker-fixture","version":"1"},"clientCapabilities":{} if args.legacy else {"subagents":{}}})
                sid=client.call("session/new",{"cwd":str(root),"mcpServers":[]})["sessionId"]
                client.call("session/prompt",{"sessionId":sid,"prompt":[{"type":"text","text":"controlled-workers"}]})
                startup=json.loads((root/"data/polytoken/sessions-acp-v1"/sid/"startup.json").read_text())
                credential=json.loads(next(root.glob("polytoken-acp-*/credential.json")).read_text())
                base=f"http://127.0.0.1:{startup['port']}"
                def get(path,method="GET"):
                    request=urllib.request.Request(base+path,method=method,data=b'{}' if method=="POST" else None,
                        headers={"Authorization":"Bearer "+credential["token"],"Content-Type":"application/json"})
                    with urllib.request.urlopen(request,timeout=5) as response:
                        data=response.read();return json.loads(data) if data else {}
                deadline=time.monotonic()+12;observed=[]
                while time.monotonic()<deadline:
                    jobs=get("/jobs");state=get("/state")
                    observed.append({"jobs":jobs,"pending":state.get("pending_interrogatives",[])})
                    if len(jobs)>=3 and state.get("pending_interrogatives"):break
                    time.sleep(.1)
                histories={}
                for job in jobs:
                    handle=job.get("subagent_handle")
                    if handle:
                        try:histories[handle]=get("/subagent/"+handle+"/history?offset=0&limit=200")
                        except Exception as error:histories[handle]={"error":str(error)}
                root_history=get("/history?checkpoint=fresh")
                root_raw=get("/history")
                print("Observed jobs:",len(jobs),"pending:",len(state.get("pending_interrogatives",[])))
                if args.audit:
                    args.audit.parent.mkdir(parents=True,exist_ok=True)
                    args.audit.write_text(json.dumps({"observed":observed,"histories":histories,"root_history":root_history,"root_raw":root_raw,"provider_requests":server.requests},indent=2))
                assert len(jobs)>=3,(jobs,server.requests)
                if args.audit:
                    print("PASS: real concurrent and nested workers with retained history")
                    return
                def update(message):return message.get("params",{}).get("update",{})
                if args.legacy:
                    client.until(lambda n:update(n).get("toolCallId")==jobs[0]["handle"])
                    assert not any(update(n).get("sessionUpdate")=="subagent_update" for n in client.notifications)
                    server.release.set()
                    print("PASS: ACP v1 ordinary-tool fallback")
                    return
                client.until(lambda _:len({update(n).get("sessionId") for n in client.notifications if update(n).get("sessionUpdate")=="subagent_update"})==3)
                associations={update(n)["sessionId"]:n["params"]["sessionId"] for n in client.notifications if update(n).get("sessionUpdate")=="subagent_update"}
                def incoming(n):return update(n).get("sessionUpdate")=="session_message" and update(n).get("recipientSessionId")==n["params"]["sessionId"]
                client.until(lambda _: {"parent-worker","leaf-worker","slow-worker"}.issubset({update(n).get("content",[{}])[0].get("text") for n in client.notifications if incoming(n)}))
                messages={update(n).get("content",[{}])[0].get("text"):n["params"]["sessionId"] for n in client.notifications if incoming(n)}
                parent=messages["parent-worker"];leaf=messages["leaf-worker"];slow=messages["slow-worker"]
                assert associations[parent]==sid and associations[slow]==sid and associations[leaf]==parent,associations
                client.until(lambda n:n.get("method")=="session/request_permission")
                assert client.permissions[-1]["params"]["sessionId"]==leaf,client.permissions
                leaf_updates=[update(n) for n in client.notifications if update(n).get("sessionId")==leaf]
                assert leaf_updates[-1]["cwd"]==str(server.worktree/"leaf"),leaf_updates
                announced={sid}
                for n in client.notifications:
                    if n.get("method") not in ["session/update","session/request_permission"]:continue
                    p=n["params"];assert p["sessionId"] in announced,n
                    if update(n).get("sessionUpdate")=="subagent_update":announced.add(update(n)["sessionId"])
                    for key in ["senderSessionId","recipientSessionId"]:
                        if key in update(n):assert update(n)[key] in announced,n
                assert not any(n.get("method","").startswith("_polytoken/subagent_") for n in client.notifications)
                # No legacy worker card alongside a standard child session.
                handles={j["handle"] for j in jobs}
                assert not any(update(n).get("toolCallId") in handles for n in client.notifications)
                if args.cancel_root:
                    assert args.protocol==2,"Root cancellation fixture uses v2's immediate prompt admission"
                    client.until(lambda n:update(n).get("toolCallId")=="root-wait")
                    client.process.stdin.write(json.dumps({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":sid}})+"\n");client.process.stdin.flush()
                    for child in [parent,leaf]:
                        client.until(lambda n:update(n).get("sessionUpdate")=="subagent_update" and update(n).get("sessionId")==child and update(n).get("state",{}).get("stopReason")=="cancelled")
                    assert next(j for j in get("/jobs") if j["handle"]==next(h for h,v in histories.items() if isinstance(v,list) and any(i.get("content")=="slow-worker" for i in v)))["status"]["status"]=="running"
                    print("PASS: root cancellation cascades through waited descendants and preserves unrelated background work")
                    return
                if args.protocol==2:
                    client.call("session/prompt",{"sessionId":leaf,"prompt":[{"type":"text","text":"unavailable"}]},error=-32602)
                    start=len(client.notifications)
                    client.cancel_during_replay=slow
                    client.call("session/resume",{"sessionId":sid,"cwd":str(root),"mcpServers":[],"replayFrom":{"type":"start"}})
                    replay=client.notifications[start:]
                    # Already-enqueued live traffic can precede request handling.
                    first=next(i for i,n in enumerate(replay) if update(n).get("sessionUpdate")=="subagent_update" and update(n).get("capabilities")=={})
                    replay=replay[first:]
                    assert {update(n)["sessionId"] for n in replay if update(n).get("sessionUpdate")=="subagent_update"}==set(associations)
                    complete=[n["params"]["sessionId"] for n in replay if update(n).get("sessionUpdate")=="replay_complete"]
                    assert complete[-1]==sid and set(complete)=={sid,*associations},complete
                    assert all(update(n).get("capabilities")=={} for n in replay if update(n).get("sessionUpdate")=="subagent_update")
                    slow_handle=next(h for h,v in histories.items() if isinstance(v,list) and any(i.get("content")=="slow-worker" for i in v))
                    assert next(j for j in get("/jobs") if j["handle"]==slow_handle)["status"]["status"]=="running","Historical cancellation must not authorize live controls"
                client.until(lambda n:update(n).get("sessionUpdate")=="subagent_update" and update(n).get("sessionId")==slow and "cancel" in update(n).get("capabilities",{}),start if args.protocol==2 else 0)
                client.process.stdin.write(json.dumps({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":slow}})+"\n");client.process.stdin.flush()
                client.until(lambda n:update(n).get("sessionUpdate")=="subagent_update" and update(n).get("sessionId")==slow and update(n).get("state",{}).get("stopReason")=="cancelled")
                remaining=get("/jobs")
                parent_handle=next(h for h,v in histories.items() if isinstance(v,list) and any(i.get("content")=="parent-worker" for i in v))
                assert next(j for j in remaining if j["handle"]==parent_handle)["status"]["status"]=="running",remaining
                client.approve();server.release.set()
                client.until(lambda n:update(n).get("sessionUpdate")=="subagent_update" and update(n).get("sessionId")==leaf and update(n).get("state",{}).get("stopReason")=="end_turn")
                client.close()
                try:get("/health")
                except (OSError,urllib.error.URLError):pass
                else:raise AssertionError("Closing the connection left the owned daemon running")
                client=WorkerWire(shim,env,root,log)
                if args.protocol==2:
                    client.call("initialize",{"protocolVersion":2,"info":{"name":"worker-reconnect","version":"1"},"capabilities":{}})
                    client.call("session/resume",{"sessionId":sid,"cwd":str(root),"mcpServers":[]})
                    assert not any(update(n).get("sessionUpdate") in ["session_message","agent_message","tool_call_update"] for n in client.notifications)
                    client.call("session/resume",{"sessionId":sid,"cwd":str(root),"mcpServers":[],"replayFrom":{"type":"start"}})
                else:
                    client.call("initialize",{"protocolVersion":1,"clientCapabilities":{"subagents":{}}})
                    client.call("session/load",{"sessionId":sid,"cwd":str(root),"mcpServers":[]})
                restored=[update(n) for n in client.notifications if update(n).get("sessionUpdate")=="subagent_update"]
                assert {u["sessionId"] for u in restored}==set(associations),restored
                assert all(u.get("capabilities")=={} for u in restored),restored
                assert all(u.get("state",{}).get("state") in ["unknown","idle"] for u in restored),restored
                assert all("cancel" not in u.get("capabilities",{}) for u in restored),restored
                assert len(client.call("session/list",{})["sessions"])==1
                print(f"PASS: ACP v{args.protocol} concurrent/nested children, attribution, permissions, cwd, targeted cancellation, replay/reconnect, fallback suppression")
            except BaseException:
                log.flush();log.seek(0);print(log.read()[-12000:]);raise
            finally:
                server.release.set()
                if client:client.close()
                server.shutdown()


if __name__=="__main__":main()
