//! Versioned ACP frontend. The daemon/session implementation is shared; this
//! adapter translates ACP v2's insertion acknowledgements and ID-addressed
//! updates to the internal v1 presentation types. V2 session updates are
//! validated against the current SDK schema before they reach the client.
use agent_client_protocol::v2::schema as v2;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

#[derive(Default)]
struct Session {
    config: Vec<Value>,
    commands: Vec<Value>,
    mode: String,
    prompt: String,
    state: Value,
    replay_content: HashMap<String, String>,
    setup: Value,
}

struct Pending {
    method: String,
    session: String,
    replay: bool,
    accepted: bool,
    mode: Option<String>,
    setup: Value,
}

#[derive(Default)]
struct Frontend {
    version: Option<u64>,
    initialized: bool,
    used: bool,
    pending: HashMap<String, Pending>,
    sessions: HashMap<String, Session>,
    replaying: HashSet<String>,
    actions: HashMap<String, String>,
}

fn error(id: Value, code: i64, message: impl ToString) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message.to_string()}})
}
fn notification(sid: &str, update: Value) -> Result<Value> {
    let typed: v2::SessionUpdate =
        serde_json::from_value(update).context("Invalid ACP v2 session update")?;
    Ok(json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":sid,"update":typed}}))
}
fn config_options(options: &Value) -> Vec<Value> {
    options
        .as_array()
        .into_iter()
        .flatten()
        .map(|o| {
            let mut o = o.clone();
            if let Some(id) = o.as_object_mut().and_then(|o| o.remove("id")) {
                o["configId"] = id;
            }
            o
        })
        .collect()
}
fn commands(commands: &Value) -> Vec<Value> {
    commands
        .as_array()
        .into_iter()
        .flatten()
        .map(|c| {
            let mut c = c.clone();
            c["type"] = json!("command");
            c
        })
        .collect()
}
impl Session {
    fn options(&self) -> Vec<Value> {
        let mut options = self.config.clone();
        options.push(json!({"configId":"permission_mode","name":"Permissions","type":"select",
            "currentValue":if self.mode.is_empty() {"standard"} else {&self.mode},
            "options":[{"value":"standard","name":"Standard"},{"value":"bypass","name":"Bypass"},
                {"value":"bypass_plus","name":"Bypass+"},{"value":"autonomous","name":"Autonomous"}]}));
        options
    }
}

impl Frontend {
    /// Returns (to backend, to client). Responses to agent requests retain their
    /// original IDs and go directly to the SDK's response router.
    fn incoming(&mut self, mut msg: Value) -> Result<(Vec<Value>, Vec<Value>)> {
        let method = match msg["method"].as_str() {
            Some(m) => m.to_string(),
            None => {
                let mut out = Vec::new();
                if self.version == Some(2)
                    && let Some(sid) = self.actions.remove(&msg["id"].to_string())
                    && !self.actions.values().any(|s| s == &sid)
                {
                    let state = json!({"sessionUpdate":"state_update","state":"running"});
                    self.sessions.entry(sid.clone()).or_default().state = state.clone();
                    out.push(notification(&sid, state)?);
                }
                return Ok((vec![msg], out));
            }
        };
        let id = msg.get("id").cloned();
        if method == "initialize" {
            let requested = msg["params"]["protocolVersion"].as_u64().unwrap_or(1);
            let version = if requested >= 2 { 2 } else { 1 };
            if self.used || (self.initialized && self.version != Some(version)) {
                return Ok((
                    vec![],
                    vec![error(
                        id.unwrap_or(Value::Null),
                        -32600,
                        "Connection already initialized",
                    )],
                ));
            }
            if version == 2 {
                if id.as_ref().is_none_or(Value::is_null) {
                    return Ok((
                        vec![],
                        vec![error(
                            Value::Null,
                            -32600,
                            "initialize requires a non-null request id",
                        )],
                    ));
                }
                if let Err(err) =
                    serde_json::from_value::<v2::InitializeRequest>(msg["params"].clone())
                {
                    return Ok((vec![], vec![error(id.unwrap_or(Value::Null), -32602, err)]));
                }
                let original = msg["params"].clone();
                msg["params"] = json!({"protocolVersion":1,"clientInfo":original["info"],
                    "clientCapabilities":{"elicitation":original["capabilities"]["elicitation"]},"_meta":original["_meta"]});
            }
            self.version = Some(version);
        } else if !self.initialized {
            return Ok((
                vec![],
                id.map(|id| vec![error(id, -32002, "Initialize the connection first")])
                    .unwrap_or_default(),
            ));
        } else {
            self.used = true;
        }
        if self.version == Some(1) {
            if let Some(id) = id {
                self.pending.insert(
                    id.to_string(),
                    Pending {
                        method,
                        session: String::new(),
                        replay: false,
                        accepted: false,
                        mode: None,
                        setup: msg["params"].clone(),
                    },
                );
            }
            return Ok((vec![msg], vec![]));
        }
        let sid = msg["params"]["sessionId"]
            .as_str()
            .unwrap_or("")
            .to_string();
        let mut pending = Pending {
            method: method.clone(),
            session: sid.clone(),
            replay: false,
            accepted: false,
            mode: None,
            setup: msg["params"].clone(),
        };
        let invalid = match method.as_str() {
            "session/new" => {
                serde_json::from_value::<v2::NewSessionRequest>(msg["params"].clone()).err()
            }
            "session/resume" => {
                serde_json::from_value::<v2::ResumeSessionRequest>(msg["params"].clone()).err()
            }
            "session/prompt" => {
                serde_json::from_value::<v2::PromptRequest>(msg["params"].clone()).err()
            }
            "session/set_config_option" => {
                serde_json::from_value::<v2::SetSessionConfigOptionRequest>(msg["params"].clone())
                    .err()
            }
            _ => None,
        };
        if let Some(err) = invalid {
            return Ok((vec![], vec![error(id.unwrap_or(Value::Null), -32602, err)]));
        }
        if method.starts_with("session/")
            && !matches!(
                method.as_str(),
                "session/new" | "session/list" | "session/resume"
            )
            && !self.sessions.contains_key(&sid)
        {
            return Ok((
                vec![],
                id.map(|id| vec![error(id, -32002, "Unknown session")])
                    .unwrap_or_default(),
            ));
        }
        match method.as_str() {
            "session/resume" => {
                let replay = &msg["params"]["replayFrom"];
                if !replay.is_null() && replay["type"] != "start" {
                    return Ok((
                        vec![],
                        vec![error(
                            id.unwrap_or(Value::Null),
                            -32602,
                            "Only replayFrom start is supported",
                        )],
                    ));
                }
                pending.replay = !replay.is_null();
                if pending.replay
                    && self
                        .pending
                        .values()
                        .any(|p| p.method == "session/prompt" && p.session == sid)
                {
                    return Ok((
                        vec![],
                        vec![error(
                            id.unwrap_or(Value::Null),
                            -32000,
                            "Wait for foreground work to finish before replaying history; resume without replayFrom to refresh current state",
                        )],
                    ));
                }
                if let Some(session) = self.sessions.get_mut(&sid) {
                    if session.setup["cwd"] != msg["params"]["cwd"]
                        || session.setup["mcpServers"] != msg["params"]["mcpServers"]
                    {
                        return Ok((
                            vec![],
                            vec![error(
                                id.unwrap_or(Value::Null),
                                -32602,
                                "Close the session before replacing its working directory or MCP servers",
                            )],
                        ));
                    }
                    msg["params"]["_meta"] = json!({"_polytoken_resync":true});
                    session.replay_content.clear();
                }
                if pending.replay {
                    msg["method"] = json!("session/load");
                    self.replaying.insert(sid.clone());
                }
            }
            "session/prompt" => {
                if msg["params"]["prompt"].as_array().is_none_or(Vec::is_empty) {
                    return Ok((
                        vec![],
                        vec![error(
                            id.unwrap_or(Value::Null),
                            -32602,
                            "Prompt must not be empty",
                        )],
                    ));
                }
                if msg["params"]["prompt"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|block| {
                        !matches!(
                            block["type"].as_str(),
                            Some("text" | "resource_link" | "resource")
                        )
                    })
                {
                    return Ok((
                        vec![],
                        vec![error(
                            id.unwrap_or(Value::Null),
                            -32602,
                            "Prompt contains a content type that the agent did not advertise",
                        )],
                    ));
                }
                if self
                    .pending
                    .values()
                    .any(|p| p.method == "session/prompt" && p.session == sid)
                {
                    return Ok((
                        vec![],
                        vec![error(
                            id.unwrap_or(Value::Null),
                            -32000,
                            "Session already has foreground work",
                        )],
                    ));
                }
            }
            "session/set_config_option" if msg["params"]["configId"] == "permission_mode" => {
                msg["method"] = json!("session/set_mode");
                msg["params"] = json!({"sessionId":sid,"modeId":msg["params"]["value"]});
                pending.mode = msg["params"]["modeId"].as_str().map(str::to_string);
                pending.method = "permission_mode".into();
            }
            "session/set_mode" | "session/load" | "authenticate" => {
                return Ok((
                    vec![],
                    vec![error(
                        id.unwrap_or(Value::Null),
                        -32601,
                        "Method is not part of ACP v2",
                    )],
                ));
            }
            _ => {}
        }
        if let Some(id) = id {
            self.pending.insert(id.to_string(), pending);
        }
        Ok((vec![msg], vec![]))
    }

    fn outgoing(&mut self, mut msg: Value) -> Result<Vec<Value>> {
        if msg["method"] == "_polytoken/prompt_accepted" {
            if self.version != Some(2) {
                return Ok(vec![]);
            }
            let sid = msg["params"]["session_id"]
                .as_str()
                .context("Missing accepted session")?;
            let pid = msg["params"]["prompt_id"]
                .as_str()
                .context("Missing accepted prompt")?;
            let entry = self
                .pending
                .iter_mut()
                .find(|(_, p)| p.method == "session/prompt" && p.session == sid && !p.accepted);
            if let Some((key, pending)) = entry {
                pending.accepted = true;
                let id: Value = serde_json::from_str(key)?;
                self.sessions.entry(sid.into()).or_default().prompt = pid.into();
                let state = json!({"sessionUpdate":"state_update","state":"running"});
                self.sessions.get_mut(sid).unwrap().state = state.clone();
                return Ok(vec![
                    notification(
                        sid,
                        json!({"sessionUpdate":"user_message","messageId":pid,"content":[{"type":"text","text":msg["params"]["content"]}]}),
                    )?,
                    json!({"jsonrpc":"2.0","id":id,"result":v2::PromptResponse::new(pid.to_string())}),
                    notification(sid, state)?,
                ]);
            }
            bail!("Accepted prompt has no pending ACP request");
        }
        if self.version != Some(2) {
            if msg.get("method").is_none()
                && let Some(id) = msg.get("id")
                && let Some(p) = self.pending.remove(&id.to_string())
                && p.method == "initialize"
                && msg.get("result").is_some()
            {
                self.initialized = true;
            }
            return Ok(vec![msg]);
        }
        if let Some(method) = msg["method"].as_str().map(str::to_string) {
            if method == "session/update" {
                return self.update(msg);
            }
            if method == "session/request_permission" {
                let params = &msg["params"];
                let value = json!({"sessionId":params["sessionId"],"title":params["toolCall"]["title"].as_str().unwrap_or("Permission required"),"options":params["options"]});
                let typed: v2::RequestPermissionRequest = serde_json::from_value(value)?;
                msg["params"] = serde_json::to_value(typed)?;
            }
            if matches!(
                method.as_str(),
                "session/request_permission"
                    | "elicitation/create"
                    | "_polytoken/ask_user_question"
            ) {
                let sid = msg["params"]["sessionId"]
                    .as_str()
                    .or_else(|| msg["params"]["session_id"].as_str())
                    .or_else(|| msg["params"]["scope"]["sessionId"].as_str())
                    .unwrap_or("")
                    .to_string();
                if !sid.is_empty() {
                    self.actions.insert(msg["id"].to_string(), sid.clone());
                    let state = json!({"sessionUpdate":"state_update","state":"requires_action"});
                    self.sessions.entry(sid.clone()).or_default().state = state.clone();
                    return Ok(vec![notification(&sid, state)?, msg]);
                }
            }
            return Ok(vec![msg]);
        }
        let id = msg["id"].clone();
        let Some(pending) = self.pending.remove(&id.to_string()) else {
            return Ok(vec![msg]);
        };
        if msg.get("error").is_some() {
            if pending.method == "session/prompt" && pending.accepted {
                let state = json!({"sessionUpdate":"state_update","state":"idle","stopReason":"error","error":msg["error"]});
                self.sessions
                    .entry(pending.session.clone())
                    .or_default()
                    .state = state.clone();
                return Ok(vec![notification(&pending.session, state)?]);
            }
            self.replaying.remove(&pending.session);
            return Ok(vec![msg]);
        }
        let result = &mut msg["result"];
        match pending.method.as_str() {
            "initialize" => {
                self.initialized = true;
                let value = json!({"protocolVersion":2,"info":result["agentInfo"],
                    "capabilities":{"session":{"prompt":{"embeddedContext":true},"mcp":{"http":true}}},
                    "authMethods":[],"_meta":result["agentCapabilities"]["_meta"]});
                let typed: v2::InitializeResponse = serde_json::from_value(value)?;
                *result = serde_json::to_value(typed)?;
            }
            "session/new" | "session/resume" => {
                let sid = result["sessionId"]
                    .as_str()
                    .unwrap_or(&pending.session)
                    .to_string();
                let session = self.sessions.entry(sid.clone()).or_default();
                session.config = config_options(&result["configOptions"]);
                session.setup = pending.setup.clone();
                session.mode = result["modes"]["currentModeId"]
                    .as_str()
                    .unwrap_or("standard")
                    .into();
                session.commands = commands(&result["_meta"]["_polytoken_available_commands"]);
                let mut value =
                    json!({"configOptions":session.options(),"availableCommands":session.commands});
                if pending.method == "session/new" {
                    value["sessionId"] = json!(sid);
                }
                let foreground = result["_meta"]["_polytoken_foreground"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_string();
                session.state = json!({"sessionUpdate":"state_update","state":foreground});
                *result = value;
                let mut out = Vec::new();
                if pending.replay {
                    out.push(notification(
                        &sid,
                        json!({"sessionUpdate":"replay_complete"}),
                    )?);
                    self.replaying.remove(&sid);
                }
                if pending.method == "session/resume" {
                    out.push(notification(&sid, session.state.clone())?);
                }
                out.push(msg);
                return Ok(out);
            }
            "session/prompt" => {
                if !pending.accepted {
                    let mut out = vec![error(id, -32000, "Prompt cancelled before insertion")];
                    if result["stopReason"] == "cancelled" {
                        out.push(notification(&pending.session,json!({"sessionUpdate":"state_update","state":"idle","stopReason":"cancelled"}))?);
                    }
                    return Ok(out);
                }
                let state = json!({"sessionUpdate":"state_update","state":"idle","stopReason":result["stopReason"]});
                self.sessions
                    .entry(pending.session.clone())
                    .or_default()
                    .state = state.clone();
                return Ok(vec![notification(&pending.session, state)?]);
            }
            "session/set_config_option" => {
                let session = self.sessions.entry(pending.session).or_default();
                session.config = config_options(&result["configOptions"]);
                *result = json!({"configOptions":session.options()});
            }
            "permission_mode" => {
                let session = self.sessions.entry(pending.session).or_default();
                if let Some(mode) = pending.mode {
                    session.mode = mode;
                }
                *result = json!({"configOptions":session.options()});
            }
            "session/close" => {
                self.sessions.remove(&pending.session);
            }
            _ => {}
        }
        Ok(vec![msg])
    }

    fn update(&mut self, mut msg: Value) -> Result<Vec<Value>> {
        let sid = msg["params"]["sessionId"]
            .as_str()
            .context("Missing session id")?
            .to_string();
        let replay = self.replaying.contains(&sid);
        let session = self.sessions.entry(sid.clone()).or_default();
        let update = &mut msg["params"]["update"];
        let kind = update["sessionUpdate"].as_str().unwrap_or("").to_string();
        match kind.as_str() {
            "agent_message_chunk" | "agent_thought_chunk" | "user_message_chunk" => {
                let pid = update["_meta"]["_polytoken_message_id"]
                    .as_str()
                    .unwrap_or(&session.prompt);
                let pid = if pid.is_empty() {
                    uuid::Uuid::new_v4().to_string()
                } else {
                    pid.to_string()
                };
                update["messageId"] = json!(pid);
                if replay {
                    let key = format!("{kind}:{pid}");
                    let full = session.replay_content.entry(key).or_default();
                    full.push_str(update["content"]["text"].as_str().unwrap_or(""));
                    update["sessionUpdate"] = json!(kind.trim_end_matches("_chunk"));
                    update["content"] = json!([{"type":"text","text":full}]);
                }
            }
            "tool_call" => update["sessionUpdate"] = json!("tool_call_update"),
            "plan" => {
                let entries = update["entries"].clone();
                *update = json!({"sessionUpdate":"plan_update","plan":{"type":"items","planId":"todos","entries":entries}});
            }
            "available_commands_update" => {
                session.commands = commands(&update["availableCommands"]);
                update["availableCommands"] = json!(session.commands);
            }
            "config_option_update" => {
                session.config = config_options(&update["configOptions"]);
                update["configOptions"] = json!(session.options());
            }
            "current_mode_update" => {
                session.mode = update["currentModeId"]
                    .as_str()
                    .unwrap_or("standard")
                    .into();
                *update = json!({"sessionUpdate":"config_option_update","configOptions":session.options()});
            }
            _ => {}
        }
        notification(&sid, update.clone()).map(|n| vec![n])
    }
}

async fn write(writer: &mut (impl tokio::io::AsyncWrite + Unpin), value: &Value) -> Result<()> {
    writer.write_all(&serde_json::to_vec(value)?).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

pub async fn run() {
    if let Err(error) = serve().await {
        tracing::error!(%error,"ACP frontend failed");
    }
}
async fn serve() -> Result<()> {
    let (frontend, backend) = tokio::io::duplex(1024 * 1024);
    let (read, mut writer) = tokio::io::split(frontend);
    let (input, output) = tokio::io::split(backend);
    let engine = tokio::spawn(crate::agent::run(agent_client_protocol::ByteStreams::new(
        output.compat_write(),
        input.compat(),
    )));
    let mut client = BufReader::new(tokio::io::stdin()).lines();
    let mut agent = BufReader::new(read).lines();
    let mut stdout = tokio::io::stdout();
    let mut frontend = Frontend::default();
    loop {
        tokio::select! {
            line=client.next_line()=> {
                let Some(line)=line? else {break};
                let msg:Value=match serde_json::from_str(&line){Ok(v)=>v,Err(_)=>{write(&mut stdout,&error(Value::Null,-32700,"Invalid JSON")).await?;continue;}};
                let (to_agent,to_client)=frontend.incoming(msg)?;
                for msg in to_client {write(&mut stdout,&msg).await?;}
                for msg in to_agent {write(&mut writer,&msg).await?;}
            }
            line=agent.next_line()=> {
                let Some(line)=line? else {break};
                let msg:Value=serde_json::from_str(&line)?;
                for msg in frontend.outgoing(msg)? {write(&mut stdout,&msg).await?;}
            }
        }
    }
    writer.shutdown().await?;
    drop(writer);
    // Closing the private transport invokes the same daemon cleanup as stdio EOF.
    engine.await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn v2_initialize_uses_v2_schema_and_v1_keeps_its_wire_shape() {
        let mut front = Frontend::default();
        let (to_backend,_)=front.incoming(json!({"id":1,"method":"initialize","params":{"protocolVersion":2,"info":{"name":"test","version":"1"}}})).unwrap();
        assert_eq!(to_backend[0]["params"]["protocolVersion"], 1);
        let out=front.outgoing(json!({"jsonrpc":"2.0","id":1,"result":{"agentInfo":{"name":"polytoken","version":"1"},"agentCapabilities":{}}})).unwrap();
        let parsed: v2::InitializeResponse =
            serde_json::from_value(out[0]["result"].clone()).unwrap();
        assert_eq!(
            parsed.protocol_version,
            agent_client_protocol::schema::ProtocolVersion::V2
        );
        assert!(front.initialized);
    }

    #[test]
    fn v2_acknowledges_insertion_then_reports_late_errors_as_idle_state() {
        let mut front = Frontend {
            version: Some(2),
            initialized: true,
            ..Default::default()
        };
        front.sessions.insert("s".into(), Session::default());
        front.incoming(json!({"id":7,"method":"session/prompt","params":{"sessionId":"s","prompt":[{"type":"text","text":"hi"}]}})).unwrap();
        let inserted=front.outgoing(json!({"method":"_polytoken/prompt_accepted","params":{"session_id":"s","prompt_id":"p","content":"hi"}})).unwrap();
        assert_eq!(inserted[0]["params"]["update"]["messageId"], "p");
        assert_eq!(inserted[1]["result"]["messageId"], "p");
        assert!(inserted[1]["result"].get("stopReason").is_none());
        let failed = front
            .outgoing(json!({"id":7,"error":{"code":-32603,"message":"lost daemon"}}))
            .unwrap();
        assert_eq!(failed.len(), 1);
        assert!(failed[0].get("id").is_none());
        assert_eq!(failed[0]["params"]["update"]["state"], "idle");
        assert_eq!(failed[0]["params"]["update"]["stopReason"], "error");
    }

    #[test]
    fn v2_permissions_expose_requires_action_and_resume_running_after_reply() {
        let mut front = Frontend {
            version: Some(2),
            initialized: true,
            ..Default::default()
        };
        front.sessions.insert("s".into(), Session::default());
        let request=front.outgoing(json!({"id":4,"method":"session/request_permission","params":{"sessionId":"s","toolCall":{"toolCallId":"c","title":"Allow?"},"options":[{"optionId":"allow_once","name":"Allow","kind":"allow_once"}]}})).unwrap();
        assert_eq!(request[0]["params"]["update"]["state"], "requires_action");
        assert_eq!(request[1]["params"]["title"], "Allow?");
        let (reply, state) = front
            .incoming(
                json!({"id":4,"result":{"outcome":{"outcome":"selected","optionId":"allow_once"}}}),
            )
            .unwrap();
        assert_eq!(reply.len(), 1);
        assert_eq!(state[0]["params"]["update"]["state"], "running");
    }

    #[test]
    fn v2_replay_uses_stable_entity_ids_and_complete_content_upserts() {
        let mut front = Frontend::default();
        front.replaying.insert("s".into());
        for (text, expected) in [("hel", "hel"), ("lo", "hello")] {
            let updates = front
                .update(json!({"params":{"sessionId":"s","update":{
                "sessionUpdate":"agent_message_chunk","content":{"type":"text","text":text},
                "_meta":{"_polytoken_message_id":"p"}}}}))
                .unwrap();
            let update = &updates[0]["params"]["update"];
            assert_eq!(update["messageId"], "p");
            assert_eq!(update["sessionUpdate"], "agent_message");
            assert_eq!(update["content"][0]["text"], expected);
        }
    }

    #[test]
    fn v2_rejects_unadvertised_prompt_media_and_replay_during_foreground_work() {
        let mut front = Frontend {
            version: Some(2),
            initialized: true,
            ..Default::default()
        };
        front.sessions.insert("s".into(), Session::default());
        let (sent, rejected) = front
            .incoming(json!({"id":1,"method":"session/prompt","params":{
            "sessionId":"s","prompt":[{"type":"image","mimeType":"image/png","data":"AA=="}]}}))
            .unwrap();
        assert!(sent.is_empty());
        assert_eq!(rejected[0]["error"]["code"], -32602);
        front
            .incoming(json!({"id":2,"method":"session/prompt","params":{
            "sessionId":"s","prompt":[{"type":"text","text":"hello"}]}}))
            .unwrap();
        let (sent, rejected) = front
            .incoming(json!({"id":3,"method":"session/resume","params":{
            "sessionId":"s","cwd":"/project","mcpServers":[],"replayFrom":{"type":"start"}}}))
            .unwrap();
        assert!(sent.is_empty());
        assert_eq!(rejected[0]["error"]["code"], -32000);
    }
}
