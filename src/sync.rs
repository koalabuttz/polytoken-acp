//! Polytoken's authenticated v1 client protocol (independent of ACP's version).
use anyhow::{Context, Result, bail};
use base64::Engine;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

pub async fn get(base: &str, token: &str, path: &str) -> Result<Value> {
    let response = reqwest::Client::new()
        .get(format!("{base}{path}"))
        .bearer_auth(token)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await?;
    let status = response.status();
    let value: Value = response
        .json()
        .await
        .context("Invalid daemon JSON response")?;
    if !status.is_success() {
        bail!("GET {path}: HTTP {status}: {value}");
    }
    Ok(value)
}

pub async fn snapshot(base: &str, token: &str) -> Result<Value> {
    let value = get(base, token, "/sync").await?;
    for feature in [
        "session.sync.v1",
        "session.history.v1",
        "session.attachments.v1",
        "session.targeted-mutations.v1",
        "daemon.catalog.v1",
    ] {
        if !value["features"]
            .as_array()
            .is_some_and(|f| f.iter().any(|v| v == feature))
        {
            bail!("Daemon does not advertise required feature {feature}; upgrade Polytoken");
        }
    }
    if value["version"] != 1 || !value["cursor"].is_string() || !value["daemon_epoch"].is_string() {
        bail!("Malformed daemon synchronization snapshot");
    }
    Ok(value)
}

/// Retry the *same* operation and body after ambiguous transport/server errors.
/// Never renew an expired operation window or retarget a mutation automatically.
pub async fn mutate(
    base: &str,
    token: &str,
    path: &str,
    mut body: Value,
    snap: &Value,
) -> Result<Value> {
    let window = snap["operation_window"]["token"]
        .as_str()
        .context("Daemon has no operation window")?;
    body["operation_context"] = json!({"daemon_epoch":snap["daemon_epoch"], "window":window,
        "operation":uuid::Uuid::new_v4().to_string()});
    let client = reqwest::Client::new();
    for attempt in 0..3 {
        let response = client
            .post(format!("{base}{path}"))
            .bearer_auth(token)
            .json(&body)
            .timeout(std::time::Duration::from_secs(20))
            .send()
            .await;
        match response {
            Ok(response) => {
                let status = response.status();
                if status.is_server_error() && attempt < 2 {
                    continue;
                }
                let value: Value = match response.json().await {
                    Ok(value) => value,
                    Err(_) if attempt < 2 => continue,
                    Err(error) => return Err(error).context("Invalid operation result"),
                };
                if !status.is_success() || value["operation_result"]["status"] == "failed" {
                    bail!("POST {path}: HTTP {status}: {value}");
                }
                let result = value
                    .get("operation_result")
                    .context("Correlated command returned no operation result")?;
                if result["daemon_epoch"] != snap["daemon_epoch"]
                    || result["operation"] != body["operation_context"]["operation"]
                {
                    bail!("Daemon returned an operation result for a different operation or epoch");
                }
                return Ok(value);
            }
            Err(error) if attempt == 2 => return Err(error.into()),
            Err(_) => {}
        }
    }
    unreachable!()
}

pub async fn respond(base: &str, token: &str, id: &str, body: Value) -> Result<()> {
    let snap = snapshot(base, token).await?;
    mutate(
        base,
        token,
        &format!("/interrogative/{id}/respond"),
        body,
        &snap,
    )
    .await?;
    Ok(())
}

/// Combine authoritative sync state and the catalog into the shim's internal
/// presentation model. Do not cache a catalog from another revision or epoch.
pub async fn state(base: &str, token: &str) -> Result<Value> {
    for _ in 0..3 {
        let snap = snapshot(base, token).await?;
        let catalog = get(base, token, "/daemon-state").await?;
        if snap["daemon_epoch"] != catalog["daemon_epoch"]
            || snap["catalog_revision"] != catalog["catalog_revision"]
        {
            continue;
        }
        let models: Vec<Value> = catalog["models"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|m| {
                let mut m = m.clone();
                m["reasoning"] = m["reasoning_capability"].clone();
                m
            })
            .collect();
        let skills: Vec<Value> = catalog["skills"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|s| s["user_invocable"].as_bool().unwrap_or(true))
            .map(|s| s["name"].clone())
            .collect();
        let mcp: Vec<Value> = catalog["mcp"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|m| {
                json!({
            "server_name":m["name"],"status":m["state"],"tool_count":m["tool_count"]})
            })
            .collect();
        // 0.9.8 retains the model's default effort in /sync after explicit
        // effort changes. Read this one runtime field until its projection is fixed.
        let runtime = get(base, token, "/state").await?;
        return Ok(
            json!({"active_model":snap["selections"]["model"], "active_reasoning_effort":runtime["active_reasoning_effort"],
            "active_facet":snap["selections"]["facet"], "available_models":models,"available_skills":skills,
            "mcp_servers":mcp,"session_title":snap["title"],"todos":snap["todos"],"current_goal":snap["goal"],
            "context_usage":snap["context"]["usage"],"_catalog":catalog,"_foreground":foreground(&snap)}),
        );
    }
    bail!("Daemon catalog changed during synchronization; retry the request")
}

pub fn foreground(snapshot: &Value) -> &'static str {
    if snapshot["interrogatives"]
        .as_array()
        .is_some_and(|a| !a.is_empty())
    {
        "requires_action"
    } else if snapshot["turn"].is_object() {
        "running"
    } else {
        "idle"
    }
}

/// Read immutable, signed history pages. Retry expired captures from scratch;
/// never splice pages from two checkpoints or turn corrupt history into empty history.
pub async fn history(base: &str, token: &str) -> Result<Vec<Value>> {
    let client = reqwest::Client::new();
    for attempt in 0..3 {
        let mut next = "fresh".to_string();
        let mut checkpoint: Option<Value> = None;
        let mut seen = HashSet::new();
        let mut items = Vec::new();
        loop {
            if !seen.insert(next.clone()) {
                bail!("Daemon repeated a history page token");
            }
            let url = reqwest::Url::parse_with_params(
                &format!("{base}/history"),
                &[("checkpoint", next.as_str())],
            )?;
            let response = client
                .get(url)
                .bearer_auth(token)
                .timeout(std::time::Duration::from_secs(20))
                .send()
                .await?;
            if response.status().as_u16() == 410 && attempt < 2 {
                break;
            }
            let status = response.status();
            let page: Value = response.json().await?;
            if !status.is_success() {
                bail!("Public history: HTTP {status}: {page}");
            }
            let id = page
                .get("checkpoint_id")
                .context("History page has no checkpoint")?;
            if let Some(ref expected) = checkpoint
                && expected != id
            {
                bail!("History checkpoint changed between pages");
            }
            checkpoint = Some(id.clone());
            let entries = page["items"]
                .as_array()
                .context("History page has no items array")?;
            for entry in entries {
                items.push(hydrate(base, token, entry.clone()).await?);
            }
            match page["next_page"].as_str() {
                Some(value) => next = value.to_string(),
                None => return Ok(items),
            }
        }
    }
    bail!("Public history checkpoint repeatedly expired")
}

/// Resolve bounded public text/JSON through the session attachment API.
async fn hydrate(base: &str, token: &str, mut value: Value) -> Result<Value> {
    let mut stack = vec![&mut value];
    while let Some(node) = stack.pop() {
        if node["truncated"] == true && node["handle"]["availability"] == "available" {
            let id = node["handle"]["artifact_id"]
                .as_str()
                .context("Attachment has no id")?
                .to_string();
            let mut bytes = Vec::new();
            let mut offset = 0u64;
            loop {
                let chunk = get(base, token, &format!("/attachments/{id}?offset={offset}")).await?;
                if chunk["artifact_id"] != id || chunk["offset"].as_u64() != Some(offset) {
                    bail!("Attachment range mismatch");
                }
                let data = base64::engine::general_purpose::STANDARD.decode(
                    chunk["data_base64"]
                        .as_str()
                        .context("Missing attachment bytes")?,
                )?;
                if data.is_empty() {
                    bail!("Attachment read made no progress");
                }
                offset += data.len() as u64;
                bytes.extend(data);
                let total = chunk["total_length"]
                    .as_u64()
                    .context("Missing attachment length")?;
                if offset > total {
                    bail!("Attachment exceeded captured length");
                }
                if offset == total {
                    break;
                }
            }
            if node.get("text").is_some() {
                node["text"] = Value::String(String::from_utf8(bytes)?);
            } else {
                node["value"] = serde_json::from_slice(&bytes)?;
            }
            node["truncated"] = json!(false);
        }
        match node {
            Value::Object(map) => stack.extend(map.values_mut()),
            Value::Array(array) => stack.extend(array.iter_mut()),
            _ => {}
        }
    }
    Ok(value)
}

/// Convert public projection items to the internal transcript representation.
pub fn normalize(item: &Value) -> Vec<Value> {
    let body = &item["body"];
    let pid = item["prompt_id"].as_str().unwrap_or("");
    match body["type"].as_str().unwrap_or("") {
        "user_message" => vec![json!({"type":"user","prompt_id":pid,"content":body["content"]["text"]})],
        "assistant_message" => {
            let blocks: Vec<Value> = body["blocks"].as_array().into_iter().flatten().map(|b| match b["type"].as_str() {
                Some("text" | "thinking") => json!({"type":b["type"],"text":b["text"]["text"]}),
                Some("tool_use") => json!({"type":"tool_use","id":b["id"],"name":b["name"],"input":b["input"]["value"]}),
                _ => b.clone(),
            }).collect();
            vec![json!({"type":"assistant","prompt_id":pid,"blocks":blocks})]
        }
        "tool_result" => body["results"].as_array().into_iter().flatten().map(|r| {
            let text = r["content"].as_array().into_iter().flatten().filter_map(|c| c["text"]["text"].as_str()).collect::<Vec<_>>().join("\n");
            json!({"type":"tool_result","prompt_id":pid,"call_id":r["tool_use_id"],"is_error":r["is_error"],"content":{"text":text}})
        }).collect(),
        _ => vec![body.clone()],
    }
}

/// Per-consumer projection retained across reconnects. Revisions are opaque;
/// deduplicate by equality, never parse or increment them or signed cursors.
pub struct Projection {
    pub snapshot: Value,
    pub seen: HashSet<String>,
    items: HashSet<String>,
    blocks: HashMap<String, String>,
    streamed: HashMap<String, String>,
    pub handled_questions: HashSet<String>,
    pub prompt_id: String,
    pub terminal: Option<Value>,
    pub recovering: bool,
    jobs: HashMap<String, String>,
}

impl Projection {
    pub fn new(snapshot: Value, prompt_id: String) -> Self {
        let jobs = snapshot["jobs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|j| {
                Some((
                    j["handle"].as_str()?.to_string(),
                    j["status"].as_str()?.to_string(),
                ))
            })
            .collect();
        Self {
            snapshot,
            prompt_id,
            seen: HashSet::new(),
            items: HashSet::new(),
            blocks: HashMap::new(),
            streamed: HashMap::new(),
            handled_questions: HashSet::new(),
            terminal: None,
            recovering: false,
            jobs,
        }
    }

    pub async fn job_events(&mut self, base: &str, token: &str) -> Result<Vec<Value>> {
        let jobs = get(base, token, "/jobs").await?;
        let mut events = Vec::new();
        for job in jobs.as_array().context("Job catalog must be an array")? {
            let id = job["handle"].as_str().context("Job has no handle")?;
            let status = job["status"]["status"]
                .as_str()
                .context("Job has no status")?;
            let previous = self.jobs.insert(id.to_string(), status.to_string());
            if previous.as_deref() == Some(status) {
                continue;
            }
            if job["kind"] == "subagent" {
                let handle = job["subagent_handle"].as_str().unwrap_or(id);
                if previous.is_none() {
                    events.push(json!({"type":"subagent_started","handle":handle,"subagent_type":job["subagent_type"].as_str().unwrap_or("subagent"),"model":job["model"].as_str().unwrap_or("")}));
                }
                if matches!(status, "completed" | "failed" | "cancelled") {
                    let retained = get(base, token, &format!("/job/{id}/result")).await;
                    let result_available = retained.is_ok();
                    let result = match retained {
                        Ok(value) => value,
                        Err(error) => {
                            json!({"content":{"text":format!("Result unavailable: {error}")},"is_error":status!="completed"})
                        }
                    };
                    let summary = crate::tool_output::text(&result["content"]).unwrap_or_default();
                    let kind = if status == "cancelled" {
                        "cancelled"
                    } else if status == "failed" || result["is_error"] == true {
                        "failure"
                    } else if result_available {
                        "success"
                    } else {
                        "unknown"
                    };
                    events.push(json!({"type":"subagent_completed","handle":handle,"outcome":{"kind":kind},"result_summary":summary}));
                }
            } else if matches!(status, "completed" | "failed" | "cancelled") {
                let result = match get(base, token, &format!("/job/{id}/result")).await {
                    Ok(value) => value,
                    Err(error) => {
                        json!({"content":{"text":format!("Result unavailable: {error}")},"is_error":status!="completed"})
                    }
                };
                events.push(json!({"type":"tool_result","prompt_id":self.prompt_id,"call_id":id,"content_full":result["content"],"is_error":result["is_error"]}));
            } else if previous.is_none() {
                events.push(json!({"type":"job_promoted","job_id":id}));
            }
        }
        Ok(events)
    }

    pub fn pending_record(&self, data: &str, session: &str) -> Result<Option<(String, Value)>> {
        let envelope: Value =
            serde_json::from_str(data).context("Malformed ordered sync envelope")?;
        if envelope["session_id"] != session {
            bail!("Ordered stream belongs to a different session");
        }
        let event = &envelope["event"];
        let revision = event["revision"]
            .as_str()
            .context("Sync record has no revision")?;
        if self.seen.contains(revision) {
            return Ok(None);
        }
        if !event["record"]["type"].is_string() {
            bail!("Sync record has no kind");
        }
        Ok(Some((revision.to_string(), event["record"].clone())))
    }

    pub async fn history_events(
        &mut self,
        base: &str,
        token: &str,
        reconcile_text: bool,
    ) -> Result<Vec<Value>> {
        let items = history(base, token).await?;
        let mut events = Vec::new();
        let mut text = HashMap::<String, String>::new();
        for item in items.iter().filter(|i| i["prompt_id"] == self.prompt_id) {
            for normalized in normalize(item) {
                let key = format!("{}:{}", item["raw_index"], normalized);
                let fresh = self.items.insert(key);
                match normalized["type"].as_str() {
                    Some("assistant") => for block in normalized["blocks"].as_array().into_iter().flatten() {
                        match block["type"].as_str() {
                            Some("text" | "thinking") => text.entry(block["type"].as_str().unwrap().into()).or_default().push_str(block["text"].as_str().unwrap_or("")),
                            Some("tool_use") if fresh => events.push(json!({"type":"tool_call","prompt_id":self.prompt_id,"call_id":block["id"],"name":block["name"],"input":block["input"]})),
                            _ => {}
                        }
                    },
                    Some("tool_result") if fresh => events.push(json!({"type":"tool_result","prompt_id":self.prompt_id,"call_id":normalized["call_id"],"content_full":normalized["content"],"is_error":normalized["is_error"]})),
                    _ => {}
                }
            }
        }
        // Durable text can precede or replace stream deltas after a reconnect.
        // Only append an unseen suffix; rewriting an already-displayed prefix
        // cannot be represented faithfully by ACP v1 and is surfaced explicitly.
        if !reconcile_text {
            return Ok(events);
        }
        for (kind, full) in text {
            let sent = self.streamed.entry(kind.clone()).or_default();
            if full.starts_with(sent.as_str()) {
                let suffix = &full[sent.len()..];
                if !suffix.is_empty() {
                    events.push(json!({"type":"content_block_delta","prompt_id":self.prompt_id,"block_index":0,"delta":{"type":kind,"text":suffix}}));
                }
                *sent = full;
            } else {
                bail!(
                    "Durable history differs from streamed output; reload the session to reconcile it"
                );
            }
        }
        Ok(events)
    }

    pub fn record_events(&mut self, record: &Value) -> Vec<Value> {
        let pid = &self.prompt_id;
        match record["type"].as_str().unwrap_or("") {
            "stream_block_started" => {
                if let (Some(id), Some(kind)) =
                    (record["block_id"].as_str(), record["kind"].as_str())
                {
                    self.blocks.insert(id.to_string(), kind.to_string());
                }
                vec![]
            }
            "stream_block_delta" => {
                let id = record["block_id"].as_str().unwrap_or("");
                let kind = self.blocks.get(id).map(String::as_str).unwrap_or("");
                if self.recovering
                    || !id.starts_with(&format!("{pid}:"))
                    || !matches!(kind, "text" | "thinking")
                {
                    return vec![];
                }
                let delta = record["delta"].as_str().unwrap_or("");
                self.streamed
                    .entry(kind.into())
                    .or_default()
                    .push_str(delta);
                vec![
                    json!({"type":"content_block_delta","prompt_id":pid,"block_index":0,"delta":{"type":kind,"text":delta}}),
                ]
            }
            "turn_completed" if record["prompt_id"] == *pid => {
                self.terminal = Some(json!({"type":"message_complete","prompt_id":pid}));
                vec![]
            }
            "turn_failed" if record["prompt_id"] == *pid => {
                self.terminal =
                    Some(json!({"type":"harness_error","prompt_id":pid,"reason":record["reason"]}));
                vec![]
            }
            "slot_released" => self.terminal.take().into_iter().collect(),
            "title_changed" => vec![
                json!({"type":"session_title_changed","title":record["title"].as_str().unwrap_or("")}),
            ],
            "model_changed" | "catalog_published" | "facet_changed" => {
                vec![json!({"type":"model_options_changed"})]
            }
            "monitor_changed" => vec![
                json!({"type":"permission_monitor_switch","from_monitor":{"type":"standard"},"to_monitor":{"type":record["mode"]}}),
            ],
            "todos_changed" => vec![json!({"type":"session_state_changed","domains":["todos"]})],
            _ => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read_request(socket: &mut tokio::net::TcpStream) -> (String, Value) {
        use tokio::io::AsyncReadExt;
        let mut bytes = Vec::new();
        let (end, length) = loop {
            let mut buf = [0u8; 4096];
            let read = socket.read(&mut buf).await.unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buf[..read]);
            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let header = String::from_utf8_lossy(&bytes[..end]);
                let length = header
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                break (end + 4, length);
            }
        };
        while bytes.len() < end + length {
            let mut buf = [0u8; 4096];
            let read = socket.read(&mut buf).await.unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buf[..read]);
        }
        (
            String::from_utf8_lossy(&bytes[..end]).to_string(),
            if length == 0 {
                Value::Null
            } else {
                serde_json::from_slice(&bytes[end..end + length]).unwrap()
            },
        )
    }
    async fn reply(socket: &mut tokio::net::TcpStream, status: u16, value: Value) {
        use tokio::io::AsyncWriteExt;
        let body = value.to_string();
        socket.write_all(format!("HTTP/1.1 {status} result\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
    }

    #[test]
    fn replay_deduplicates_opaque_revisions_and_checks_session_binding() {
        let mut projection = Projection::new(json!({}), "p".into());
        let frame=json!({"session_id":"s","event":{"revision":"9007199254740993123456789","record":{"type":"future_event","extra":true}}}).to_string();
        let (revision, record) = projection.pending_record(&frame, "s").unwrap().unwrap();
        assert_eq!(record["extra"], true);
        projection.seen.insert(revision);
        assert!(projection.pending_record(&frame, "s").unwrap().is_none());
        assert!(projection.pending_record(&frame, "other").is_err());
    }

    #[tokio::test]
    async fn ambiguous_mutation_retries_the_identical_operation() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (_, first) = read_request(&mut socket).await;
            drop(socket); // The daemon accepted the command but the response was lost.
            let (mut socket, _) = listener.accept().await.unwrap();
            let (_, second) = read_request(&mut socket).await;
            assert_eq!(first, second);
            reply(&mut socket,202,json!({"prompt_id":"p","operation_result":{"daemon_epoch":"epoch","operation":second["operation_context"]["operation"],"status":"completed"}})).await;
        });
        let result = mutate(
            &format!("http://{address}"),
            "token",
            "/prompt",
            json!({"content":"hello"}),
            &json!({"daemon_epoch":"epoch","operation_window":{"token":"opaque"}}),
        )
        .await
        .unwrap();
        assert_eq!(result["prompt_id"], "p");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn public_history_keeps_checkpoint_and_opaque_page_token() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (request, _) = read_request(&mut socket).await;
            assert!(request.starts_with("GET /history?checkpoint=fresh "));
            reply(&mut socket,200,json!({"checkpoint_id":"cp","items":[{"body":{"type":"user_message","content":{"text":"a","truncated":false}}}],"next_page":"opaque+/token"})).await;
            let (mut socket, _) = listener.accept().await.unwrap();
            let (request, _) = read_request(&mut socket).await;
            assert!(request.contains("checkpoint=opaque%2B%2Ftoken"));
            reply(
                &mut socket,
                200,
                json!({"checkpoint_id":"cp","items":[],"next_page":null}),
            )
            .await;
        });
        let items = history(&format!("http://{address}"), "token")
            .await
            .unwrap();
        assert_eq!(items.len(), 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bounded_history_text_is_hydrated_from_attachment_chunks() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for (offset, data) in [(0, "caf".as_bytes()), (3, "é".as_bytes())] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (request, _) = read_request(&mut socket).await;
                assert!(
                    request.starts_with(&format!("GET /attachments/artifact?offset={offset} "))
                );
                reply(&mut socket, 200, json!({"artifact_id":"artifact","offset":offset,
                    "total_length":5,"data_base64":base64::engine::general_purpose::STANDARD.encode(data)})).await;
            }
        });
        let value = hydrate(&format!("http://{address}"), "token",
            json!({"text":"ca","truncated":true,"handle":{"availability":"available","artifact_id":"artifact"}}))
            .await.unwrap();
        assert_eq!(value["text"], "café");
        assert_eq!(value["truncated"], false);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn recovery_reconciles_only_the_unsent_suffix_and_rejects_rewrites() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for text in ["hello world", "rewritten"] {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_request(&mut socket).await;
                reply(
                    &mut socket,
                    200,
                    json!({"checkpoint_id":"cp","items":[{
                    "raw_index":1,"prompt_id":"p","body":{"type":"assistant_message","blocks":[{
                        "type":"text","text":{"text":text,"truncated":false}}]}}]}),
                )
                .await;
            }
        });
        let base = format!("http://{address}");
        let mut projection = Projection::new(json!({}), "p".into());
        projection.streamed.insert("text".into(), "hello".into());
        projection.recovering = true;
        let events = projection
            .history_events(&base, "token", true)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["delta"]["text"], " world");
        assert!(
            projection
                .history_events(&base, "token", true)
                .await
                .is_err()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn correlated_command_rejects_a_result_for_another_operation() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            reply(
                &mut socket,
                200,
                json!({"operation_result":{
                "daemon_epoch":"epoch","operation":"some-other-operation","status":"completed"}}),
            )
            .await;
        });
        assert!(
            mutate(
                &format!("http://{address}"),
                "token",
                "/prompt",
                json!({"content":"hello"}),
                &json!({"daemon_epoch":"epoch","operation_window":{"token":"opaque"}})
            )
            .await
            .is_err()
        );
        server.await.unwrap();
    }
    #[test]
    fn public_history_and_ordered_stream_use_wire_shapes() {
        let item = json!({"prompt_id":"p","raw_index":42,"body":{"type":"assistant_message","blocks":[{"type":"text","text":{"text":"hello","truncated":false}},{"type":"tool_use","id":"c","name":"read","input":{"value":{"path":"x"},"truncated":false}}]}});
        let normalized = normalize(&item);
        assert_eq!(normalized[0]["blocks"][0]["text"], "hello");
        assert_eq!(normalized[0]["blocks"][1]["input"]["path"], "x");
        let mut projection = Projection::new(json!({}), "p".into());
        projection
            .record_events(&json!({"type":"stream_block_started","block_id":"p:0","kind":"text"}));
        assert_eq!(
            projection.record_events(
                &json!({"type":"stream_block_delta","block_id":"p:0","delta":"hello"})
            )[0]["delta"]["text"],
            "hello"
        );
        assert!(
            projection
                .record_events(&json!({"type":"turn_completed","prompt_id":"p"}))
                .is_empty()
        );
        assert_eq!(
            projection.record_events(&json!({"type":"slot_released"}))[0]["type"],
            "message_complete"
        );
    }

    #[tokio::test]
    async fn expired_history_capture_restarts_without_mixing_pages() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for (status, page) in [
                (
                    200,
                    json!({"checkpoint_id":"old","items":[{"body":{"type":"user_message","content":{"text":"stale","truncated":false}}}],"next_page":"expired"}),
                ),
                (410, json!({"code":"checkpoint_expired"})),
                (
                    200,
                    json!({"checkpoint_id":"new","items":[{"body":{"type":"user_message","content":{"text":"fresh","truncated":false}}}]}),
                ),
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_request(&mut socket).await;
                reply(&mut socket, status, page).await;
            }
        });
        let items = history(&format!("http://{address}"), "token")
            .await
            .unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["body"]["content"]["text"], "fresh");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn job_projection_reads_tagged_status_and_retained_subagent_result() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            reply(&mut socket,200,json!([{"handle":"job-1","kind":"subagent","subagent_handle":"child-1","subagent_type":"researcher","model":"fixture","status":{"status":"completed","exit_code":0}}])).await;
            let (mut socket, _) = listener.accept().await.unwrap();
            let (request, _) = read_request(&mut socket).await;
            assert!(request.starts_with("GET /job/job-1/result "));
            reply(
                &mut socket,
                200,
                json!({"content":{"text":"Finished"},"is_error":false}),
            )
            .await;
        });
        let mut projection = Projection::new(json!({}), "p".into());
        let events = projection
            .job_events(&format!("http://{address}"), "token")
            .await
            .unwrap();
        assert_eq!(events[0]["type"], "subagent_started");
        assert_eq!(events[1]["handle"], "child-1");
        assert_eq!(events[1]["result_summary"], "Finished");
        server.await.unwrap();
    }
}
