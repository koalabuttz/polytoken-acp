//! Version negotiation and framing. ACP v2 uses its own persistent session
//! engine; v1 keeps its existing SDK handlers and delayed prompt responses.
use agent_client_protocol::v2::schema as v2;
use anyhow::{Result, bail};
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

#[derive(Default)]
struct Frontend {
    version: Option<u64>,
    initialized: bool,
    initialize_id: Option<String>,
    requests: HashMap<String, String>,
}

fn error(id: Value, code: i64, message: impl ToString) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message.to_string()}})
}

impl Frontend {
    fn incoming(&mut self, mut msg: Value) -> Result<(Vec<Value>, Vec<Value>)> {
        if !msg.is_object() || msg["jsonrpc"] != "2.0" {
            return Ok((
                vec![],
                vec![error(Value::Null, -32600, "Invalid JSON-RPC message")],
            ));
        }
        let Some(method) = msg["method"].as_str().map(str::to_string) else {
            // Replies travel in the independent agent-to-client ID namespace.
            return Ok((vec![msg], vec![]));
        };
        let id = msg.get("id").cloned();
        if let Some(id) = &id
            && !(id.is_string() || id.as_i64().is_some() || id.as_u64().is_some())
        {
            return Ok((
                vec![],
                vec![error(
                    Value::Null,
                    -32600,
                    "Request IDs must be strings or integers",
                )],
            ));
        }
        if let Some(id) = &id
            && (self.requests.contains_key(&id.to_string())
                || self.initialize_id.as_ref() == Some(&id.to_string()))
        {
            return Ok((
                vec![],
                vec![error(id.clone(), -32600, "Request ID is already in use")],
            ));
        }
        if method == "initialize" {
            if self.version.is_some() {
                return Ok((
                    vec![],
                    vec![error(
                        id.unwrap_or(Value::Null),
                        -32600,
                        "Connection already initialized",
                    )],
                ));
            }
            let Some(id) = id else {
                return Ok((
                    vec![],
                    vec![error(
                        Value::Null,
                        -32600,
                        "initialize requires a request ID",
                    )],
                ));
            };
            let requested = msg["params"]["protocolVersion"].as_u64().unwrap_or(1);
            let version = if requested >= 2 { 2 } else { 1 };
            if version == 2 {
                if let Err(e) =
                    serde_json::from_value::<v2::InitializeRequest>(msg["params"].clone())
                {
                    return Ok((vec![], vec![error(id, -32602, e)]));
                }
                let params = msg["params"].clone();
                msg["params"] = json!({"protocolVersion":1,"clientInfo":params["info"],
                    "clientCapabilities":{"elicitation":params["capabilities"]["elicitation"]},"_meta":params["_meta"]});
            }
            self.version = Some(version);
            self.initialize_id = Some(id.to_string());
            return Ok((vec![msg], vec![]));
        }
        if !self.initialized {
            return Ok((
                vec![],
                id.map(|id| vec![error(id, -32002, "Initialize the connection first")])
                    .unwrap_or_default(),
            ));
        }
        if self.version == Some(1) {
            return Ok((vec![msg], vec![]));
        }
        if method == "session/cancel" {
            if let Some(id) = id {
                return Ok((
                    vec![],
                    vec![error(id, -32600, "session/cancel is a notification")],
                ));
            }
            if !msg["params"]["sessionId"].is_string() {
                return Ok((vec![], vec![]));
            }
            msg["method"] = json!("_polytoken/v2_cancel");
            return Ok((vec![msg], vec![]));
        }
        if method == "$/cancel_request" {
            return Ok((vec![], vec![]));
        }
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
            "session/list" => {
                serde_json::from_value::<v2::ListSessionsRequest>(msg["params"].clone()).err()
            }
            "session/close" => {
                serde_json::from_value::<v2::CloseSessionRequest>(msg["params"].clone()).err()
            }
            "session/delete" => {
                serde_json::from_value::<v2::DeleteSessionRequest>(msg["params"].clone()).err()
            }
            "session/set_config_option" => {
                serde_json::from_value::<v2::SetSessionConfigOptionRequest>(msg["params"].clone())
                    .err()
            }
            "tool_call/cancel" => {
                serde_json::from_value::<v2::CancelToolCallRequest>(msg["params"].clone()).err()
            }
            "terminal/cancel" => {
                serde_json::from_value::<v2::CancelTerminalRequest>(msg["params"].clone()).err()
            }
            _ => {
                return Ok((
                    vec![],
                    id.map(|id| vec![error(id, -32601, "Unknown ACP v2 method")])
                        .unwrap_or_default(),
                ));
            }
        };
        let Some(id) = id else {
            return Ok((vec![], vec![]));
        };
        if let Some(e) = invalid {
            return Ok((vec![], vec![error(id, -32602, e)]));
        }
        self.requests.insert(id.to_string(), method.clone());
        let params = msg["params"].clone();
        msg["method"] = json!("_polytoken/v2");
        msg["params"] = json!({"method":method,"params":params});
        Ok((vec![msg], vec![]))
    }

    fn outgoing(&mut self, mut msg: Value) -> Result<Vec<Value>> {
        if msg["method"] == "_polytoken/prompt_accepted" {
            return Ok(vec![]);
        }
        if let Some(method) = msg["method"].as_str().map(str::to_string) {
            if self.version != Some(2) {
                return Ok(vec![msg]);
            }
            if method == "_polytoken/v2_update" {
                let update: v2::SessionUpdate =
                    serde_json::from_value(msg["params"]["update"].clone())?;
                msg["method"] = json!("session/update");
                msg["params"]["update"] = serde_json::to_value(update)?;
            } else if method == "session/request_permission" {
                let params = &msg["params"];
                let typed: v2::RequestPermissionRequest = serde_json::from_value(
                    json!({"sessionId":params["sessionId"],
                    "title":params["toolCall"]["title"].as_str().unwrap_or("Permission required"),"options":params["options"]}),
                )?;
                msg["params"] = serde_json::to_value(typed)?;
            } else if method == "session/update" {
                // Question fallbacks in the shared permission layer may emit
                // legacy chunks. Give them IDs without changing native updates.
                let update = &mut msg["params"]["update"];
                if matches!(
                    update["sessionUpdate"].as_str(),
                    Some("agent_message_chunk" | "agent_thought_chunk" | "user_message_chunk")
                ) {
                    update["messageId"] = update["_meta"]["_polytoken_message_id"]
                        .as_str()
                        .map(|s| json!(s))
                        .unwrap_or_else(|| json!(uuid::Uuid::new_v4().to_string()));
                }
            }
            return Ok(vec![msg]);
        }
        let key = msg["id"].to_string();
        if self.initialize_id.as_ref() == Some(&key) {
            self.initialize_id = None;
            if msg.get("error").is_some() {
                self.version = None;
                return Ok(vec![msg]);
            }
            self.initialized = true;
            if self.version == Some(2) {
                let value = json!({"protocolVersion":2,"info":msg["result"]["agentInfo"],
                    "capabilities":{"session":{"prompt":{"embeddedContext":{},"image":{}},"mcp":{"http":{}},"delete":{}}},"authMethods":[]});
                let typed: v2::InitializeResponse = serde_json::from_value(value)?;
                msg["result"] = serde_json::to_value(typed)?;
            }
        } else if self.version == Some(2)
            && let Some(method) = self.requests.remove(&key)
            && msg.get("result").is_some()
        {
            // Validate the complete response, including capability-gated fields.
            let value = msg["result"].clone();
            let valid = match method.as_str() {
                "session/new" => {
                    serde_json::from_value::<v2::NewSessionResponse>(value).map(|_| ())
                }
                "session/resume" => {
                    serde_json::from_value::<v2::ResumeSessionResponse>(value).map(|_| ())
                }
                "session/prompt" => serde_json::from_value::<v2::PromptResponse>(value).map(|_| ()),
                "session/list" => {
                    serde_json::from_value::<v2::ListSessionsResponse>(value).map(|_| ())
                }
                "session/set_config_option" => {
                    serde_json::from_value::<v2::SetSessionConfigOptionResponse>(value).map(|_| ())
                }
                _ => Ok(()),
            };
            if let Err(e) = valid {
                bail!("Invalid {method} response: {e}");
            }
        }
        let idle = msg
            .get_mut("result")
            .and_then(Value::as_object_mut)
            .and_then(|o| o.remove("_polytoken_idle"));
        let mut output = vec![msg];
        if let Some(params) = idle {
            output.push(json!({"jsonrpc":"2.0","method":"session/update","params":params}));
        }
        Ok(output)
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
    let result: Result<()> = async {
        loop { tokio::select! {
            line=client.next_line()=> {
                let Some(line)=line? else {break};
                let msg:Value=match serde_json::from_str(&line){Ok(v)=>v,Err(_)=>{write(&mut stdout,&error(Value::Null,-32700,"Invalid JSON")).await?;continue;}};
                let (to_agent,to_client)=frontend.incoming(msg)?;
                for msg in to_client {write(&mut stdout,&msg).await?;}
                for msg in to_agent {write(&mut writer,&msg).await?;}
            }
            line=agent.next_line()=> {
                let Some(line)=line? else {break};
                for msg in frontend.outgoing(serde_json::from_str(&line)?)? {write(&mut stdout,&msg).await?;}
            }
        }}
        Ok(())
    }.await;
    // Even malformed output or a client write error must close the engine and
    // release all child processes, rather than detaching the cleanup task.
    let closing = writer.shutdown().await;
    drop(writer);
    engine.await?;
    closing?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    fn initialized() -> Frontend {
        Frontend {
            version: Some(2),
            initialized: true,
            ..Default::default()
        }
    }
    #[test]
    fn v2_requests_use_native_engine_and_acknowledge_only_once() {
        let mut front = initialized();
        let (requests,_) = front.incoming(json!({"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"s","prompt":[{"type":"text","text":"hi"}]}})).unwrap();
        assert_eq!(requests[0]["method"], "_polytoken/v2");
        assert_eq!(requests[0]["params"]["method"], "session/prompt");
        let response = front
            .outgoing(json!({"jsonrpc":"2.0","id":1,"result":{"messageId":"p"}}))
            .unwrap();
        assert_eq!(response[0]["result"]["messageId"], "p");
        let update=front.outgoing(json!({"method":"_polytoken/v2_update","params":{"sessionId":"s","update":{"sessionUpdate":"state_update","state":"idle","stopReason":"end_turn"}}})).unwrap();
        assert!(update[0].get("id").is_none());
        assert_eq!(update[0]["method"], "session/update");
    }
    #[test]
    fn request_ids_and_agent_replies_have_independent_namespaces() {
        let mut front = initialized();
        front.requests.insert("7".into(), "session/prompt".into());
        let (reply, _) = front
            .incoming(json!({"jsonrpc":"2.0","id":7,"result":{}}))
            .unwrap();
        assert_eq!(reply.len(), 1);
        assert!(front.requests.contains_key("7"));
        let (sent, errors) = front
            .incoming(json!({"jsonrpc":"2.0","id":7,"method":"session/list","params":{}}))
            .unwrap();
        assert!(sent.is_empty());
        assert_eq!(errors[0]["error"]["code"], -32600);
    }
    #[test]
    fn local_command_completion_follows_the_acknowledgement_and_stays_internal() {
        let mut front = initialized();
        front.requests.insert("1".into(), "session/prompt".into());
        let response=front.outgoing(json!({"jsonrpc":"2.0","id":1,"result":{"messageId":"p","_polytoken_idle":{
            "sessionId":"s","update":{"sessionUpdate":"state_update","state":"idle","stopReason":"end_turn"}}}})).unwrap();
        assert_eq!(response.len(), 2);
        assert_eq!(response[0]["id"], 1);
        assert!(response[0]["result"].get("_polytoken_idle").is_none());
        assert_eq!(response[1]["params"]["update"]["state"], "idle");
    }
    #[test]
    fn initialization_negotiates_v2_and_v1_keeps_its_shape() {
        for version in [1, 2] {
            let mut front = Frontend::default();
            let params = if version == 2 {
                json!({"protocolVersion":2,"info":{"name":"test","version":"1"}})
            } else {
                json!({"protocolVersion":1,"clientInfo":{"name":"test","version":"1"}})
            };
            front
                .incoming(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":params}))
                .unwrap();
            let out=front.outgoing(json!({"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentInfo":{"name":"test","version":"1"},"agentCapabilities":{}}})).unwrap();
            assert_eq!(out[0]["result"]["protocolVersion"], version);
            if version == 2 {
                assert!(out[0]["result"]["capabilities"]["session"]["prompt"]["image"].is_object());
            }
        }
    }
}
