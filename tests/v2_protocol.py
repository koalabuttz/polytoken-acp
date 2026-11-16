#!/usr/bin/env python3
"""Real Polytoken, isolated configuration, no credentials or remote model calls.

Exercise v2 concurrency, replay, content, discovery, deletion and recovery.
python3 tests/v2_protocol.py --shim /path/to/polytoken-acp --polytoken /path/to/polytoken
"""
import argparse
import base64
import json
import os
from pathlib import Path
import queue
import subprocess
import tempfile
import threading
import time
import urllib.request
from http.server import ThreadingHTTPServer
from compatibility import Client, Provider


class Wire(Client):
    def __init__(self, *args):
        super().__init__(*args)
        self.responses = {}

    def send(self, method, params):
        self.counter += 1
        self.process.stdin.write(json.dumps({"jsonrpc":"2.0","id":self.counter,"method":method,"params":params})+"\n")
        self.process.stdin.flush()
        return self.counter

    def pump(self, timeout=25):
        message = self.messages.get(timeout=timeout)
        assert message is not None, "Shim exited unexpectedly"
        if "method" not in message:
            self.responses[message["id"]] = message
        else:
            self.notifications.append(message)
            if "id" in message:
                if message["method"] == "session/request_permission":
                    result={"outcome":{"outcome":"selected","optionId":"allow_once"}}
                elif message["method"] == "elicitation/create":
                    result={"action":"accept","content":{"q":"one"}}
                else:
                    result={"answers":[{"question_id":"q","selected_option_ids":["one"]}]}
                self.process.stdin.write(json.dumps({"jsonrpc":"2.0","id":message["id"],"result":result})+"\n")
                self.process.stdin.flush()
        return message

    def result(self, request, error=None):
        deadline=time.monotonic()+30
        while request not in self.responses:
            self.pump(max(.1,deadline-time.monotonic()))
            assert time.monotonic()<deadline
        response=self.responses.pop(request)
        if error is not None:
            assert response.get("error",{}).get("code")==error,response
            return response
        assert "error" not in response,response
        return response["result"]

    def call(self, method, params, error=None):
        return self.result(self.send(method,params),error)

    def until(self, predicate, start=0, timeout=30):
        deadline=time.monotonic()+timeout
        while True:
            found=next((n for n in self.notifications[start:] if predicate(n)),None)
            if found is not None:return found
            assert time.monotonic()<deadline, self.notifications[start:]
            self.pump(max(.1,deadline-time.monotonic()))

    def idle(self, start=0):
        return self.until(lambda n:n.get("params",{}).get("update",{}).get("sessionUpdate")=="state_update"
                          and n["params"]["update"].get("state")=="idle"
                          and "stopReason" in n["params"]["update"],start)


def updates(client,start=0):
    return [n["params"]["update"] for n in client.notifications[start:] if n.get("method")=="session/update"]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shim",required=True)
    parser.add_argument("--polytoken",required=True)
    args=parser.parse_args()
    shim=str(Path(args.shim).resolve());daemon=str(Path(args.polytoken).resolve())
    server=ThreadingHTTPServer(("127.0.0.1",0),Provider)
    server.requests=[];server.mcp_requests=[];server.mcp_discovered=threading.Event()
    server.tool_fired=False;server.question_fired=False
    server.slow_started=threading.Event();server.release=threading.Event()
    threading.Thread(target=server.serve_forever,daemon=True).start()
    client=None
    with tempfile.TemporaryDirectory(prefix="polytoken-acp-v2-") as directory:
        root=Path(directory);config=root/"config"/"polytoken";config.mkdir(parents=True)
        (config/"permissions.yaml").write_text("version: 2\nask:\n  - tool: shell_exec\n")
        bin_dir=root/"bin";bin_dir.mkdir();(bin_dir/"polytoken").symlink_to(daemon)
        model={"provider":"fixture","provider_name":"fixture","class":"full","context_window":128000,
               "max_tokens":4096,"supports_vision":True,"reasoning":{"type":"effort","effort_set":"gpt_6_sol",
               "levels":["low","high"],"default_level":"low","can_disable":True}}
        (config/"config.json").write_text(json.dumps({"version":4,"providers":{"fixture":{
            "kind":{"type":"custom_open_ai_responses"},"url":f"http://127.0.0.1:{server.server_port}/v1","auth":{"type":"no_auth"}}},
            "models":{"fixture":model},"modelgroups":{"polytoken:default_model_full":"fixture"},
            "updates":{"automatic_check":False},"data":{"automatic_crash_reporting":False}}))
        env=dict(os.environ,HOME=str(root),XDG_CONFIG_HOME=str(root/"config"),XDG_DATA_HOME=str(root/"data"),
                 TMPDIR=str(root),PATH=str(bin_dir)+os.pathsep+os.environ["PATH"],RUST_LOG="polytoken_acp=debug")
        with (root/"shim.log").open("w+") as log:
            try:
                def launch():
                    nonlocal client
                    client=Wire(shim,env,root,log)
                    initialized=client.call("initialize",{"protocolVersion":2,"info":{"name":"v2-contract","version":"1"},"capabilities":{"elicitation":{"form":{}}}})
                    assert initialized["capabilities"]["session"]["prompt"]["embeddedContext"]=={},initialized
                    assert initialized["capabilities"]["session"]["prompt"]["image"]=={},initialized
                    assert initialized["capabilities"]["session"]["delete"]=={},initialized
                launch()
                sid=client.call("session/new",{"cwd":str(root),"mcpServers":[]})["sessionId"]
                # The official read-only CLI also sees the daemon session.
                listed=json.loads(subprocess.check_output([daemon,"sessions","--all","--format","json","--sessions-dir",str(root/"data/polytoken/sessions-acp")],env=env,text=True))
                assert listed[0]["session_id"]==sid
                index=root/"data/polytoken/acp-shim-metadata"/f"{sid}.json"
                saved=index.read_bytes();index.unlink()
                assert any(s["sessionId"]==sid for s in client.call("session/list",{})["sessions"])
                index.write_bytes(saved)
                print("PASS: pre-index daemon sessions are discoverable through the read-only CLI")

                marker="embedded-content-survives-2718"
                start=len(client.notifications)
                first=client.call("session/prompt",{"sessionId":sid,"prompt":[{"type":"resource","resource":{"uri":"file:///fixture.txt","mimeType":"text/plain","text":marker}}]})
                client.idle(start)
                assert any(marker in json.dumps(r.get("input")) for r in server.requests),server.requests
                print("PASS: advertised embedded context reaches the provider without loss")

                png="iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a5Z0AAAAASUVORK5CYII="
                start=len(client.notifications)
                client.call("session/prompt",{"sessionId":sid,"prompt":[{"type":"image","mimeType":"image/png","data":png}]})
                client.idle(start)
                assert any("input_image" in json.dumps(r.get("input")) for r in server.requests),server.requests
                print("PASS: native inline image reaches the provider")
                client.call("session/prompt",{"sessionId":sid,"prompt":[{"type":"audio","mimeType":"audio/wav","data":"AA=="}]},error=-32602)
                start=len(client.notifications)
                command=client.call("session/prompt",{"sessionId":sid,"prompt":[{"type":"text","text":"/title v2-protocol-fixture"}]})
                assert "_polytoken_idle" not in command,command
                client.idle(start)
                print("PASS: locally handled slash commands acknowledge insertion and finish without a model turn")

                # Admit another message and replay while the first is still running.
                start=len(client.notifications)
                running=client.call("session/prompt",{"sessionId":sid,"prompt":[{"type":"text","text":"exercise-cancel first-overlap"}]})
                assert server.slow_started.wait(5)
                second=client.call("session/prompt",{"sessionId":sid,"prompt":[{"type":"text","text":"queued-second-overlap"}]})
                assert running["messageId"]!=second["messageId"]
                resumed=client.call("session/resume",{"sessionId":sid,"cwd":str(root),"mcpServers":[],"replayFrom":{"type":"start"}})
                assert resumed.get("configOptions")
                assert any(u["sessionUpdate"]=="replay_complete" for u in updates(client,start))
                assert not any(u.get("state")=="idle" for u in updates(client,start))
                server.release.set()
                client.idle(start)
                before=len(client.notifications)
                client.call("session/resume",{"sessionId":sid,"cwd":str(root),"mcpServers":[],"replayFrom":{"type":"start"}})
                replay=updates(client,before)
                users=[u for u in replay if u["sessionUpdate"]=="user_message"]
                assert any(u["messageId"]==first["messageId"] for u in users),users
                assert any(u["messageId"]==second["messageId"] for u in users),users
                assert all(u["content"][0].get("text")!="compat-okcompat-ok" for u in replay if u["sessionUpdate"]=="agent_message"),replay
                print("PASS: overlapping admissions, replay during work, stable replay IDs and no duplicate text")

                start=len(client.notifications)
                client.call("session/prompt",{"sessionId":sid,"prompt":[{"type":"text","text":"exercise-background"}]})
                client.idle(start)
                terminal=client.until(lambda n:n.get("params",{}).get("update",{}).get("sessionUpdate")=="terminal_update"
                                      and "exitStatus" not in n["params"]["update"],start)["params"]["update"]
                terminal_id=terminal["terminalId"]
                completed=client.until(lambda n:n.get("params",{}).get("update",{}).get("terminalId")==terminal_id
                                       and "exitStatus" in n["params"]["update"],start)["params"]["update"]
                assert "background-done" in base64.b64decode(completed["output"]["data"]).decode(),completed
                assert any(any(w.get("terminalId")==terminal_id for w in u.get("background",[])) for u in updates(client,start)),updates(client,start)
                print("PASS: background terminal output and completion continue after foreground idle")

                start=len(client.notifications)
                client.call("session/prompt",{"sessionId":sid,"prompt":[{"type":"text","text":"exercise-background-cancel"}]})
                terminal=client.until(lambda n:n.get("params",{}).get("update",{}).get("sessionUpdate")=="terminal_update"
                                      and "sleep 30" in n["params"]["update"].get("command","") and "exitStatus" not in n["params"]["update"],start)["params"]["update"]
                client.call("terminal/cancel",{"sessionId":sid,"terminalId":terminal["terminalId"]})
                client.until(lambda n:n.get("params",{}).get("update",{}).get("terminalId")==terminal["terminalId"]
                             and "exitStatus" in n["params"]["update"],start)
                client.idle(start)
                print("PASS: client cancellation targets the declared background terminal")

                # Replace MCP settings on an active session, then its workspace.
                mcp=[{"type":"http","name":"replacement_mcp","url":f"http://127.0.0.1:{server.server_port}/mcp","headers":[]}]
                new_cwd=root/"workspace";new_cwd.mkdir()
                server.release.clear();server.slow_started.clear()
                client.call("session/prompt",{"sessionId":sid,"prompt":[{"type":"text","text":"exercise-cancel reconfigure-without-cancelling"}]})
                assert server.slow_started.wait(5)
                before=len(client.notifications)
                setup=client.send("session/resume",{"sessionId":sid,"cwd":str(new_cwd),"mcpServers":mcp})
                client.until(lambda n:n.get("params",{}).get("update",{}).get("title")=="Applying workspace settings",before)
                assert setup not in client.responses
                assert not server.release.is_set()
                server.release.set()
                client.result(setup)
                assert server.mcp_discovered.wait(5)
                client.call("session/resume",{"sessionId":sid,"cwd":str(new_cwd),"mcpServers":[]})
                startup=json.loads((root/"data/polytoken/sessions-acp-v1"/sid/"startup.json").read_text())
                credential=json.loads(next(root.glob("polytoken-acp-*/credential.json")).read_text())
                request=urllib.request.Request(f"http://127.0.0.1:{startup['port']}/local-shell",data=json.dumps({"command":"pwd","shared_with_agent":False}).encode(),
                    headers={"Authorization":"Bearer "+credential["token"],"Content-Type":"application/json"})
                with urllib.request.urlopen(request,timeout=5) as response:assert json.load(response)["stdout"].strip()==str(new_cwd)
                listed=client.call("session/list",{"cwd":str(new_cwd)})["sessions"]
                assert any(s["sessionId"]==sid and s["cwd"]==str(new_cwd) for s in listed),listed
                assert client.call("session/list",{"cwd":str(root)})["sessions"]==[]
                client.call("session/list",{"cwd":"relative"},error=-32602)
                client.call("session/list",{"cursor":"invalid"},error=-32602)
                client.call("session/resume",{"sessionId":"missing-session","cwd":str(root)},error=-32002)
                print("PASS: active settings replacement and listing/filter/error contracts")

                # Public discovery after close and a shim process restart.
                client.call("session/close",{"sessionId":sid})
                assert any(s["sessionId"]==sid for s in client.call("session/list",{})["sessions"])
                client.close();client=None;launch()
                assert any(s["sessionId"]==sid for s in client.call("session/list",{})["sessions"])
                client.call("session/resume",{"sessionId":sid,"cwd":str(new_cwd),"replayFrom":{"type":"start"}})
                print("PASS: saved session discovery and resume after process restart")

                # Recover a dead owned daemon without a new prompt submission.
                startup=json.loads((root/"data/polytoken/sessions-acp-v1"/sid/"startup.json").read_text())
                credential=json.loads(next(root.glob("polytoken-acp-*/credential.json")).read_text())
                before=len(client.notifications);requests=len(server.requests)
                request=urllib.request.Request(f"http://127.0.0.1:{startup['port']}/terminate",data=b"{}",headers={"Authorization":"Bearer "+credential["token"],"Content-Type":"application/json"})
                with urllib.request.urlopen(request,timeout=5):pass
                client.until(lambda n:n.get("params",{}).get("update",{}).get("title")=="Daemon connection recovered",before)
                assert len(server.requests)==requests,"Recovery resubmitted a prompt"
                print("PASS: daemon restart recovers history/state without resubmission")

                # Pagination captures are stable and filter-bound.
                metadata=root/"data/polytoken/acp-shim-metadata"
                for i in range(103):
                    (metadata/f"fixture-{i:03}.json").write_text(json.dumps({"sessionId":f"fixture-{i:03}","cwd":str(new_cwd)}))
                page=client.call("session/list",{"cwd":str(new_cwd)})
                assert len(page["sessions"])==100 and page.get("nextCursor"),page
                tail=client.call("session/list",{"cwd":str(new_cwd),"cursor":page["nextCursor"]})
                assert len(tail["sessions"])==4 and "nextCursor" not in tail,tail
                assert client.call("session/list",{"cwd":str(new_cwd),"cursor":page["nextCursor"]})==tail
                print("PASS: stable paginated discovery and repeatable opaque cursors")

                server.release.clear();server.slow_started.clear()
                client.call("session/prompt",{"sessionId":sid,"prompt":[{"type":"text","text":"exercise-cancel delete-active"}]})
                assert server.slow_started.wait(5)
                client.call("session/delete",{"sessionId":sid})
                assert not server.release.is_set(),"Deletion waited for the provider to finish"
                client.call("session/list",{"cwd":str(new_cwd),"cursor":page["nextCursor"]},error=-32602)
                client.call("session/delete",{"sessionId":sid})
                assert all(s["sessionId"]!=sid for s in client.call("session/list",{"cwd":str(new_cwd)})["sessions"])
                client.call("session/resume",{"sessionId":sid,"cwd":str(new_cwd)},error=-32002)
                client.call("session/prompt",{"sessionId":sid,"prompt":[{"type":"text","text":"no"}]},error=-32002)
                print("PASS: idempotent session deletion and deleted-session rejection")
            except BaseException:
                log.flush();log.seek(0);print(log.read()[-14000:])
                if client:print("Recent notifications:",json.dumps(client.notifications[-18:]))
                try:
                    startup=json.loads(next(root.rglob("startup.json")).read_text())
                    credential=json.loads(next(root.glob("polytoken-acp-*/credential.json")).read_text())
                    request=urllib.request.Request(f"http://127.0.0.1:{startup['port']}/sync",headers={"Authorization":"Bearer "+credential["token"]})
                    with urllib.request.urlopen(request,timeout=3) as response:snapshot=json.load(response)
                    print("Daemon foreground:",{key:snapshot.get(key) for key in ("turn","queue")})
                except Exception:pass
                raise
            finally:
                server.release.set()
                if client:client.close()
                server.shutdown()


if __name__=="__main__":main()
