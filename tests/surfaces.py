#!/usr/bin/env python3
"""Surface transport contract test. Mock daemon routes are explicitly synthetic.

--polytoken additionally exercises discovery against a real isolated daemon.
All fixture state and executables live in a temporary home; no plugin approval
or installed daemon is changed.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import queue
import shlex
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer

CAP="io.github.koalabuttz/polytoken-surfaces"
PREFIX="_"+CAP+"/"
CHUNK=16384


class MockDaemon(BaseHTTPRequestHandler):
    protocol_version="HTTP/1.1"
    def log_message(self,*_):pass
    def handle(self):
        try:super().handle()
        except (BrokenPipeError,ConnectionResetError):pass
    def respond(self,value,status=200,headers=()):
        body=json.dumps(value).encode() if not isinstance(value,bytes) else value
        self.send_response(status)
        for key,val in headers:self.send_header(key,val)
        self.send_header("Content-Length",str(len(body)))
        self.end_headers();self.wfile.write(body)
    def do_GET(self):self.dispatch()
    def do_POST(self):self.dispatch()
    def dispatch(self):
        server=self.server
        if self.headers.get("Authorization")!="Bearer "+server.token:
            self.respond({},401);return
        path=urllib.parse.urlsplit(self.path).path
        body=self.rfile.read(int(self.headers.get("Content-Length",0)))
        if path=="/health":self.respond({});return
        if path=="/sync":
            snapshot=json.dumps({"version":1,"daemon_epoch":server.epoch,"cursor":"opaque-snapshot","catalog_revision":"1",
                "features":["session.sync.v1","session.history.v1","session.attachments.v1","session.targeted-mutations.v1","daemon.catalog.v1"],
                "selections":{"model":"fixture","facet":"execute"},"queue":{"items":[],"revision":"0"},"jobs":[],"todos":[],"interrogatives":[],"context":{},
                "surfaces":server.surfaces,"actions":{"can_submit_prompt":True},"title":"fixture"}).encode()
            with server.condition:
                paused=server.pause_sync
                if paused:server.pause_sync=False;server.sync_entered=True
            if paused:assert server.sync_release.wait(5),"Test did not release paused snapshot"
            self.respond(snapshot);return
        if path=="/daemon-state":self.respond({"daemon_epoch":server.epoch,"catalog_revision":"1","models":[],"facets":[],"skills":[],"tools":[],"mcp":[]});return
        if path=="/state":self.respond({"pending_interrogatives":[]});return
        if path=="/permission-monitor":self.respond({"monitor":{"type":"standard"}});return
        if path=="/jobs":self.respond([]);return
        if path=="/history":
            self.respond({"checkpoint_id":"fixture-history","items":[],"next_page":None,"session_id":server.sid,"offset":0,"total_projected_items":0});return
        if path=="/events":
            if "after=" not in self.path and server.block_surface_events:
                self.respond({},503);return
            self.send_response(200);self.send_header("Content-Type","text/event-stream");self.send_header("Connection","close");self.end_headers()
            ordered="after=" in self.path
            index=len(server.events)
            while True:
                with server.condition:
                    server.condition.wait(.15)
                    events=server.events[index:];index=len(server.events)
                if not events:events=[{"type":"heartbeat"}]
                for event in events:
                    if event.get("disconnect"):return
                    if ordered:event={"revision":str(index),"record":{"type":"fixture_ping"}}
                    envelope={"session_id":server.sid,"event":event}
                    self.wfile.write(b"data: "+json.dumps(envelope).encode()+b"\n\n");self.wfile.flush()
        elif path.startswith("/fixture/"):
            action=path.rsplit("/",1)[1]
            if action=="stats":self.respond({"actions":server.actions,"syncEntered":server.sync_entered});return
            if action=="pause-sync":
                with server.condition:
                    server.sync_release.clear();server.pause_sync=True;server.sync_entered=False
                self.respond({});return
            if action=="release-sync":server.sync_release.set();self.respond({});return
            if action=="block-events":server.block_surface_events=True;self.respond({});return
            if action=="unblock-events":server.block_surface_events=False;self.respond({});return
            if action=="replace":
                server.generation+=1;server.surfaces[0]["generation"]=f"00000000-0000-7000-8000-{server.generation:012x}"
            elif action=="remove":server.surfaces=[]
            elif action=="epoch":server.epoch+="-new"
            elif action=="gap":pass
            elif action=="disconnect":pass
            elif action=="revise":server.surfaces[0]["revision"]=str(int(server.surfaces[0]["revision"])+1)
            else:self.respond({},404);return
            with server.condition:
                server.events.append({"disconnect":True} if action=="disconnect" else {"type":"stream_discontinuity"} if action=="gap" else {"type":"plugin_event"})
                server.condition.notify_all()
            self.respond({});return
        elif path=="/plugins/demo/http/surface":
            self.respond(b"<!doctype html><h1>fixture \xe2\x98\x83</h1>",headers=[("Content-Type","text/html; charset=utf-8")]);return
        elif path=="/plugins/demo/http/echo":
            server.actions+=1
            self.respond({"path":self.path,"bodyBase64":base64.b64encode(body).decode(),"headers":self.headers.get_all("x-test"),"datastar":self.headers.get("datastar-request")},headers=[("Content-Type","application/json")]);return
        elif path=="/plugins/demo/http/bytes":
            self.respond(b"\x00\xff\xfe\xe2\x98\x83",status=418,headers=[("Content-Type","application/octet-stream"),("X-Repeat","one"),("X-Repeat","two"),("Set-Cookie","must-not-forward=1")]);return
        elif path=="/plugins/demo/http/stream":
            self.respond(b"x"*(CHUNK*8),headers=[("Content-Type","text/event-stream")]);return
        elif path=="/plugins/demo/http/quiet":
            self.send_response(200);self.send_header("Content-Type","text/event-stream");self.send_header("Connection","close");self.end_headers()
            for _ in range(4):self.wfile.write(b"x");self.wfile.flush();time.sleep(.25)
            time.sleep(45);return
        elif path=="/plugins/demo/http/hold":
            self.send_response(200);self.send_header("Content-Type","text/event-stream");self.send_header("Connection","close");self.end_headers()
            while True:self.wfile.write(b"data: {}\n\n");self.wfile.flush();time.sleep(.05)
        elif path=="/plugins/demo/http/redirect":
            self.respond(b"",status=302,headers=[("Location","http://127.0.0.1:1/forbidden")]);return
        elif path=="/terminate":self.respond({});return
        else:self.respond({},404)


def mock_daemon():
    args=sys.argv[2:]
    if not args or args[0]!="daemon":print("[]");return
    def option(name):return args[args.index(name)+1]
    server=ThreadingHTTPServer(("127.0.0.1",0),MockDaemon)
    server.token=json.loads(Path(option("--credential-file")).read_text())["token"]
    server.sid=option("--session-id");server.epoch="epoch-"+server.sid
    server.generation=1;server.actions=0;server.events=[];server.condition=threading.Condition()
    server.pause_sync=False;server.sync_entered=False;server.sync_release=threading.Event();server.sync_release.set()
    server.block_surface_events=False
    server.surfaces=[{"plugin_name":"demo","surface_id":"page","generation":"00000000-0000-7000-8000-000000000001","revision":"18446744073709551610","source":{"type":"plugin_http","path":"/surface"}}]
    startup=Path(option("--sessions-dir"));startup=startup.with_name(startup.name+"-v1")/server.sid/"startup.json"
    startup.parent.mkdir(parents=True,exist_ok=True);startup.write_text(json.dumps({"state":"ready","port":server.server_port}))
    server.serve_forever()


def main():
    from v2_protocol import Wire
    import jsonschema
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shim",required=True);parser.add_argument("--protocol",type=int,choices=[1,2],default=2)
    parser.add_argument("--polytoken");args=parser.parse_args()
    project=Path(__file__).resolve().parent.parent
    schema=json.loads((project/"docs/surfaces.schema.json").read_text())
    jsonschema.Draft202012Validator.check_schema(schema)
    examples=json.loads((project/"docs/surfaces.examples.json").read_text())
    validators={name:jsonschema.Draft202012Validator({"$defs":schema["$defs"],"$ref":"#/$defs/"+name}) for name in schema["$defs"]}
    for message in examples["htmlExchange"]+[examples["sseRequest"],examples["cancel"],examples["error"]]:jsonschema.validate(message,schema)
    class SurfaceWire(Wire):
        auto_ack=True
        def __init__(self,*a):super().__init__(*a);self.operations={}
        def send(self,method,params):
            if method.startswith(PREFIX):validators[method[len(PREFIX):]+"Params"].validate(params)
            request=super().send(method,params)
            if method.startswith(PREFIX):self.operations[request]=method[len(PREFIX):]
            return request
        def pump(self,*a,**kw):
            message=super().pump(*a,**kw)
            if message.get("method","").startswith(PREFIX):validators[message["method"][len(PREFIX):]+"Notification"].validate(message)
            if "method" not in message and message.get("id") in self.operations:
                name=self.operations.pop(message["id"])
                validators["ErrorResponse" if "error" in message else name+"Response"].validate(message)
            p=message.get("params",{})
            if self.auto_ack and message.get("method")==PREFIX+"httpEvent" and p["event"]["kind"]=="body":
                self.send(PREFIX+"ack",{k:p[k] for k in ["sessionId","subscriptionId","requestId"]}|{"throughSequence":p["sequence"]})
            return message
    with tempfile.TemporaryDirectory(prefix="polytoken-acp-surfaces-") as directory:
        root=Path(directory);binary=root/"bin";binary.mkdir()
        if args.polytoken:(binary/"polytoken").symlink_to(Path(args.polytoken).resolve())
        else:
            wrapper=binary/"polytoken"
            wrapper.write_text("#!/bin/sh\nexec python3 "+shlex.quote(str(Path(__file__).resolve()))+' --mock-daemon "$@"\n');wrapper.chmod(0o755)
        env=dict(os.environ,HOME=str(root),XDG_DATA_HOME=str(root/"data"),XDG_CONFIG_HOME=str(root/"config"),TMPDIR=str(root),PATH=str(binary)+os.pathsep+os.environ["PATH"],RUST_LOG="polytoken_acp=debug")
        (root/"config/polytoken").mkdir(parents=True)
        (root/"config/polytoken/config.json").write_text(json.dumps({"version":4,"providers":{"fixture":{"kind":{"type":"custom_open_ai_responses"},"url":"http://127.0.0.1:1/v1","auth":{"type":"no_auth"}}},"models":{"fixture":{"provider":"fixture","provider_name":"fixture","class":"full","context_window":128000,"max_tokens":4096}},"modelgroups":{"polytoken:default_model_full":"fixture"},"updates":{"automatic_check":False},"data":{"automatic_crash_reporting":False}}))
        with (root/"shim.log").open("w+") as log:
            for offered in [None,{"versions":[2]}]:
                probe=SurfaceWire(str(Path(args.shim).resolve()),env,root,log)
                try:
                    capabilities={} if offered is None else {"_meta":{CAP:offered}}
                    init={"protocolVersion":args.protocol,"info":{"name":"unsupported","version":"1"},"capabilities":capabilities} if args.protocol==2 else {"protocolVersion":1,"clientCapabilities":capabilities}
                    probe.call("initialize",init)
                    probe.call(PREFIX+"subscribe",{"sessionId":"not-open"},error=-32601)
                finally:probe.close()
            client=SurfaceWire(str(Path(args.shim).resolve()),env,root,log)
            try:
                # No opt-in must not enable private methods, including on v2.
                caps={"_meta":{CAP:{"versions":[1]}}}
                init={"protocolVersion":args.protocol,"info":{"name":"surface-fixture","version":"1"},"capabilities":caps} if args.protocol==2 else {"protocolVersion":1,"clientInfo":{"name":"surface-fixture","version":"1"},"clientCapabilities":caps}
                response=client.call("initialize",init)
                advertised=response["capabilities" if args.protocol==2 else "agentCapabilities"]["_meta"][CAP]
                assert advertised==examples["initialization"][0]["agentCapabilityFragment"]["agentCapabilities"]["_meta"][CAP]
                assert advertised["version"]==1 and advertised["generationAtomicDispatch"] is False
                sid=client.call("session/new",{"cwd":str(root),"mcpServers":[]})["sessionId"]
                sub=client.call(PREFIX+"subscribe",{"sessionId":sid})
                scope={"sessionId":sid,"subscriptionId":sub["subscriptionId"]}
                if args.polytoken:
                    assert sub["surfaces"]==[],sub
                    client.call(PREFIX+"unsubscribe",scope)
                    print(f"PASS: real-daemon ACP v{args.protocol} negotiated empty discovery and subscription cleanup");return
                surface=sub["surfaces"][0]
                target=scope|{"daemonEpoch":sub["daemonEpoch"],"surface":{k:surface[k] for k in ["pluginName","surfaceId","generation","revision"]}}
                startup=json.loads((root/"data/polytoken/sessions-acp-v1"/sid/"startup.json").read_text())
                token=json.loads(next(root.glob("polytoken-acp-*/credential.json")).read_text())["token"]
                base=f"http://127.0.0.1:{startup['port']}"
                def daemon(path):
                    request=urllib.request.Request(base+path,method="POST",data=b"{}",headers={"Authorization":"Bearer "+token,"Content-Type":"application/json"})
                    with urllib.request.urlopen(request,timeout=5) as r:return json.load(r)
                def collect(request,start=0,timeout=40):
                    client.until(lambda m:m.get("method")==PREFIX+"httpEvent" and m["params"]["requestId"]==request and m["params"]["event"]["kind"]=="end",start,timeout=timeout)
                    frames=[m["params"] for m in client.notifications[start:] if m.get("method")==PREFIX+"httpEvent" and m["params"]["requestId"]==request]
                    assert [int(f["sequence"]) for f in frames]==list(range(len(frames))),frames
                    return frames,b"".join(base64.b64decode(f["event"]["bodyBase64"]) for f in frames if f["event"]["kind"]=="body")
                reply=client.call(PREFIX+"getHtml",target|{"requestId":"html"})
                assert reply["bodyRevisionVerified"] is False and reply["observedSurface"]==surface
                frames,html=collect("html");assert b"fixture" in html and "\u2603" in html.decode()
                # Queue observer invalidation and a second preflight behind a
                # paused snapshot, with event reconnection unavailable.
                daemon("/fixture/pause-sync")
                first=client.send(PREFIX+"getHtml",target|{"requestId":"pending-first"})
                deadline=time.monotonic()+3
                while not daemon("/fixture/stats")["syncEntered"]:
                    assert time.monotonic()<deadline,"Preflight did not reach snapshot barrier"
                    time.sleep(.01)
                start=len(client.notifications)
                daemon("/fixture/block-events");daemon("/fixture/disconnect")
                time.sleep(.15)
                second=client.send(PREFIX+"getHtml",target|{"requestId":"pending-second"})
                time.sleep(.15)
                daemon("/fixture/release-sync")
                client.result(second,error=-32071)
                client.until(lambda m:m.get("method")==PREFIX+"surfaceEvent" and m["params"]["event"].get("reason")=="sync_unavailable",start)
                resets=[m["params"]["event"] for m in client.notifications[start:] if m.get("method")==PREFIX+"surfaceEvent" and m["params"]["event"]["kind"]=="reset"]
                assert all(e["surfaces"] is None for e in resets),resets
                client.call(PREFIX+"getHtml",target|{"requestId":"still-unavailable"},error=-32071)
                while first not in client.responses:client.pump()
                first_response=client.responses.pop(first)
                if "result" in first_response:collect("pending-first")
                else:assert first_response["error"]["code"]==-32071,first_response
                daemon("/fixture/unblock-events")
                client.until(lambda m:m.get("method")==PREFIX+"surfaceEvent" and m["params"]["event"].get("reason")=="reconnected",start)
                client.call(PREFIX+"getHtml",target|{"requestId":"after-reconnect"});collect("after-reconnect")
                data=b"\x00\xffbinary"
                client.call(PREFIX+"request",target|{"requestId":"echo","method":"POST","path":"/echo","query":"x=1&x=2&a=%2f+%20","headers":[["x-test","one"],["x-test","two"],["datastar-request","true"]],"bodyBase64":base64.b64encode(data).decode()})
                frames,echo=collect("echo");echo=json.loads(echo)
                assert echo["path"].endswith("?x=1&x=2&a=%2f+%20") and echo["headers"]==["one","two"] and echo["datastar"]=="true",echo
                assert base64.b64decode(echo["bodyBase64"])==data
                client.call(PREFIX+"request",target|{"requestId":"bytes","method":"GET","path":"/bytes"})
                frames,data=collect("bytes");assert data==b"\x00\xff\xfe\xe2\x98\x83" and frames[0]["event"]["status"]==418
                headers=frames[0]["event"]["headers"]
                assert [base64.b64decode(v) for k,v in headers if k=="x-repeat"]==[b"one",b"two"]
                assert not any(k=="set-cookie" for k,v in headers)
                client.call(PREFIX+"request",target|{"requestId":"redirect","method":"GET","path":"/redirect"})
                frames,data=collect("redirect");assert not data and frames[0]["event"]["status"]==302
                assert frames[-1]["event"]["result"]["outcome"]=="complete"
                for path in ["/../sync","/%2e%2e/sync","//external/path","/x%2Fy","/x%252fy"]:
                    client.call(PREFIX+"request",target|{"requestId":"bad","method":"GET","path":path},error=-32602)
                for name in ["Authorization","Cookie","Host","X-Polytoken-Session","Connection"]:
                    client.call(PREFIX+"request",target|{"requestId":"bad","method":"GET","path":"/echo","headers":[[name,"forbidden"]]},error=-32602)
                client.call(PREFIX+"request",target|{"requestId":"echo","method":"POST","path":"/echo"},error=-32073)
                assert daemon("/fixture/stats")["actions"]==1
                client.auto_ack=False
                client.call(PREFIX+"request",target|{"requestId":"flow","method":"GET","path":"/stream"})
                client.until(lambda _:sum(m.get("params",{}).get("requestId")=="flow" and m["params"]["event"]["kind"]=="body" for m in client.notifications if m.get("method")==PREFIX+"httpEvent")==4)
                try:
                    while True:
                        m=client.pump(.2)
                        assert not(m.get("method")==PREFIX+"httpEvent" and m["params"]["requestId"]=="flow" and m["params"]["event"]["kind"]=="body"),m
                except queue.Empty:pass
                client.auto_ack=True;client.call(PREFIX+"ack",scope|{"requestId":"flow","throughSequence":"4"})
                frames,data=collect("flow");assert len(data)==CHUNK*8
                client.call(PREFIX+"request",target|{"requestId":"cancel","method":"GET","path":"/hold"})
                client.until(lambda m:m.get("method")==PREFIX+"httpEvent" and m["params"]["requestId"]=="cancel")
                client.call(PREFIX+"cancel",scope|{"requestId":"cancel"})
                frames,_=collect("cancel");assert frames[-1]["event"]["result"]["outcome"]=="cancelled"
                client.call(PREFIX+"cancel",scope|{"requestId":"cancel"})
                client.call(PREFIX+"request",target|{"requestId":"replace","method":"GET","path":"/hold"})
                # Hold an older preflight snapshot while the observer receives
                # a new generation. It must never publish new -> old -> new.
                daemon("/fixture/pause-sync")
                overlap=client.send(PREFIX+"getHtml",target|{"requestId":"snapshot-race"})
                deadline=time.monotonic()+3
                while not daemon("/fixture/stats")["syncEntered"]:
                    assert time.monotonic()<deadline,"Preflight did not reach snapshot barrier"
                    time.sleep(.01)
                race_start=len(client.notifications)
                daemon("/fixture/replace")
                deadline=time.monotonic()+.5
                while time.monotonic()<deadline:
                    try:client.pump(.05)
                    except queue.Empty:pass
                daemon("/fixture/release-sync")
                while overlap not in client.responses:client.pump()
                overlap_response=client.responses.pop(overlap)
                if "error" in overlap_response:assert overlap_response["error"]["code"]==-32071,overlap_response
                else:collect("snapshot-race")
                client.until(lambda m:m.get("method")==PREFIX+"surfaceEvent" and m["params"]["event"]["kind"]=="upsert" and m["params"]["event"]["surface"]["generation"]!=surface["generation"],race_start)
                generations=[m["params"]["event"]["surface"]["generation"] for m in client.notifications[race_start:] if m.get("method")==PREFIX+"surfaceEvent" and m["params"]["event"]["kind"]=="upsert"]
                assert surface["generation"] not in generations,generations
                frames,_=collect("replace");assert frames[-1]["event"]["result"]["outcome"]=="cancelled"
                client.call(PREFIX+"request",target|{"requestId":"stale","method":"POST","path":"/echo"},error=-32071)
                assert daemon("/fixture/stats")["actions"]==1
                start=len(client.notifications);daemon("/fixture/gap")
                client.until(lambda m:m.get("method")==PREFIX+"surfaceEvent" and m["params"]["event"].get("reason")=="event_gap",start)
                start=len(client.notifications);daemon("/fixture/disconnect")
                client.until(lambda m:m.get("method")==PREFIX+"surfaceEvent" and m["params"]["event"].get("reason")=="sync_unavailable",start)
                client.until(lambda m:m.get("method")==PREFIX+"surfaceEvent" and m["params"]["event"].get("reason")=="reconnected",start)
                start=len(client.notifications);daemon("/fixture/remove")
                client.until(lambda m:m.get("method")==PREFIX+"surfaceEvent" and m["params"]["event"]["kind"]=="removed",start)
                client.call(PREFIX+"unsubscribe",scope)
                client.call(PREFIX+"getHtml",target|{"requestId":"gone"},error=-32070)
                client.call(PREFIX+"subscribe",{"sessionId":"pt-child:unowned"},error=-32002)
                second=client.call("session/new",{"cwd":str(root),"mcpServers":[]})["sessionId"]
                second_sub=client.call(PREFIX+"subscribe",{"sessionId":second})
                second_target={"sessionId":second,"subscriptionId":second_sub["subscriptionId"],"daemonEpoch":second_sub["daemonEpoch"],"surface":{k:second_sub["surfaces"][0][k] for k in ["pluginName","surfaceId","generation","revision"]}}
                # Fill all slots with streams that send four chunks and remain
                # open but quiet. One is ACKed; the others must expire without
                # further upstream bytes or EOF and release their slots.
                client.auto_ack=False
                quiet_ids=[f"quiet-{n}" for n in range(8)]
                for request in quiet_ids:
                    client.call(PREFIX+"request",second_target|{"requestId":request,"method":"GET","path":"/quiet"})
                client.until(lambda _:all(sum(m.get("params",{}).get("requestId")==request and m["params"]["event"]["kind"]=="body" for m in client.notifications if m.get("method")==PREFIX+"httpEvent")==4 for request in quiet_ids))
                second_scope={k:second_target[k] for k in ["sessionId","subscriptionId"]}
                client.call(PREFIX+"ack",second_scope|{"requestId":quiet_ids[0],"throughSequence":"4"})
                client.call(PREFIX+"request",second_target|{"requestId":"full","method":"GET","path":"/surface"},error=-32075)
                started=time.monotonic();client.call("session/list",{});assert time.monotonic()-started<5,"Flow control blocked unrelated RPCs"
                deadline=time.monotonic()+31
                for request in quiet_ids[1:]:
                    frames,_=collect(request,timeout=max(.1,deadline-time.monotonic()))
                    assert frames[-1]["event"]["result"]["outcome"]=="error"
                    assert sum(f["event"]["kind"]=="body" for f in frames)==4
                assert not any(m.get("method")==PREFIX+"httpEvent" and m["params"]["requestId"]==quiet_ids[0] and m["params"]["event"]["kind"]=="end" for m in client.notifications),"ACKed quiet stream expired"
                client.auto_ack=True
                client.call(PREFIX+"getHtml",second_target|{"requestId":"slot-freed"});collect("slot-freed")
                client.call(PREFIX+"cancel",second_scope|{"requestId":quiet_ids[0]})
                frames,_=collect(quiet_ids[0]);assert frames[-1]["event"]["result"]["outcome"]=="cancelled"
                client.call(PREFIX+"request",second_target|{"requestId":"owner-close","method":"GET","path":"/hold"})
                client.call("session/close",{"sessionId":second})
                frames,_=collect("owner-close");assert frames[-1]["event"]["result"]=={"outcome":"cancelled","reason":"session_closed"}
                client.call(PREFIX+"subscribe",{"sessionId":second},error=-32002)
                surviving=client.call(PREFIX+"subscribe",{"sessionId":sid})
                client.call(PREFIX+"unsubscribe",{"sessionId":sid,"subscriptionId":surviving["subscriptionId"]})
                print(f"PASS: ACP v{args.protocol} surfaces including queued preflight rejection, quiet-upstream ACK deadlines, acknowledged quiet streams, and slot release")
            except BaseException:
                log.flush();log.seek(0);print(log.read()[-10000:]);raise
            finally:client.close()


if __name__=="__main__":
    if len(sys.argv)>1 and sys.argv[1]=="--mock-daemon":mock_daemon()
    else:main()
