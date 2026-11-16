//! ACP v2 sessions have a persistent observer, independent of prompt requests.
//! One session lock serializes admissions, replay and projection publication.
use agent_client_protocol::v1::{ConnectionTo, schema as v1};
use agent_client_protocol::{Client, Error};
use anyhow::{Context, Result, bail};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

use crate::{agent, daemon::DaemonHandle, store, sync};

fn invalid(e: impl ToString) -> Error {
    Error::invalid_params().data(e.to_string())
}
fn internal(e: impl ToString) -> Error {
    Error::internal_error().data(e.to_string())
}
fn unknown() -> Error {
    Error::new(-32002, "Unknown session")
}

#[derive(Clone)]
struct ListPage {
    filter: Option<String>,
    items: Vec<Value>,
}

#[derive(Default)]
pub struct Manager {
    closed: std::sync::atomic::AtomicBool,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    /// A list cursor names an immutable page capture, not an offset in a list
    /// that may change between requests. Captures are local to a connection.
    pages: Mutex<HashMap<String, ListPage>>,
    lifecycle: tokio::sync::Mutex<()>,
}

struct Session {
    id: String,
    conn: ConnectionTo<Client>,
    features: agent::ClientFeatureSupport,
    inner: tokio::sync::Mutex<Inner>,
    stop: CancellationToken,
    observing: std::sync::atomic::AtomicBool,
    changed: tokio::sync::Notify,
    refresh_children: tokio::sync::Notify,
}

struct Inner {
    daemon: DaemonHandle,
    temp: tempfile::TempDir,
    config_dir: std::path::PathBuf,
    cwd: std::path::PathBuf,
    servers: Value,
    restart: bool,
    reload_pending: bool,
    closed: bool,
    snapshot: Value,
    revision_generation: u64,
    seen: HashSet<String>,
    entities: BTreeMap<String, Value>,
    live_text: HashMap<String, String>,
    blocks: HashMap<String, String>,
    handled: HashSet<String>,
    recover_text: bool,
    terminal: Option<Value>,
    foreground: bool,
    state: Value,
    record: Value,
    children_paused: bool,
}

impl Manager {
    pub fn owns(&self, id: &str) -> bool {
        self.sessions.lock().unwrap().contains_key(id)
    }
    pub async fn endpoint(&self, id: &str) -> Option<crate::surfaces::Endpoint> {
        let session = self.sessions.lock().unwrap().get(id).cloned()?;
        let inner = session.inner.lock().await;
        (!inner.closed).then(|| {
            crate::surfaces::Endpoint::new(inner.daemon.base_url(), inner.daemon.bearer_token())
        })
    }
    pub async fn request(
        self: &Arc<Self>,
        method: &str,
        params: Value,
        conn: ConnectionTo<Client>,
        features: agent::ClientFeatureSupport,
    ) -> std::result::Result<Value, Error> {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::new(-32000, "Connection is closing"));
        }
        if let Some(id) = params["sessionId"]
            .as_str()
            .filter(|id| crate::children::Hub::child_id(id))
        {
            if method != "session/cancel" {
                return Err(invalid("This operation is unavailable on worker sessions"));
            }
            let (root, grant) = features.children.permit(id).await.ok_or_else(unknown)?;
            let session = self
                .sessions
                .lock()
                .unwrap()
                .get(&root)
                .cloned()
                .ok_or_else(unknown)?;
            let inner = session.inner.lock().await;
            features
                .children
                .cancel(
                    id,
                    inner.daemon.base_url(),
                    inner.daemon.bearer_token(),
                    grant,
                )
                .await
                .map_err(internal)?;
            return Ok(json!({}));
        }
        match method {
            "session/new" | "session/resume" => self.setup(method, params, conn, features).await,
            "session/list" => self.list(params).await,
            "session/delete" => {
                let id = params["sessionId"]
                    .as_str()
                    .ok_or_else(|| invalid("Missing session ID"))?;
                if !store::valid_id(id) {
                    return Err(invalid("Invalid session ID"));
                }
                self.close(id).await;
                let mut record = store::read(id)
                    .map_err(internal)?
                    .unwrap_or(json!({"sessionId":id}));
                record["deleted"] = json!(true);
                store::write(id, &record).map_err(internal)?;
                // A continuation capture must not reintroduce a deleted row.
                self.pages.lock().unwrap().clear();
                Ok(json!({}))
            }
            "session/close" => {
                let id = params["sessionId"]
                    .as_str()
                    .ok_or_else(|| invalid("Missing session ID"))?;
                self.close(id).await;
                Ok(json!({}))
            }
            _ => {
                let id = params["sessionId"]
                    .as_str()
                    .ok_or_else(|| invalid("Missing session ID"))?;
                let session = self.sessions.lock().unwrap().get(id).cloned();
                let Some(session) = session else {
                    return if method == "session/cancel" {
                        Ok(json!({}))
                    } else {
                        Err(unknown())
                    };
                };
                let mut inner = session.inner.lock().await;
                if inner.closed {
                    return Err(unknown());
                }
                match method {
                    "session/prompt" => session.prompt(&mut inner, params).await,
                    "session/cancel" => {
                        let snapshot =
                            sync::snapshot(inner.daemon.base_url(), inner.daemon.bearer_token())
                                .await
                                .map_err(internal)?;
                        if snapshot["turn"].is_object() {
                            session
                                .features
                                .children
                                .cancel_waited(
                                    &session.id,
                                    inner.daemon.base_url(),
                                    inner.daemon.bearer_token(),
                                    &snapshot,
                                )
                                .await
                                .map_err(internal)?;
                            sync::mutate(inner.daemon.base_url(), inner.daemon.bearer_token(), "/turn/cancel",
                                json!({"prompt_id":snapshot["turn"]["prompt_id"],"generation":snapshot["turn"]["generation"]}), &snapshot).await.map_err(internal)?;
                        }
                        Ok(json!({}))
                    }
                    "session/set_config_option" => session.configure(&mut inner, params).await,
                    "tool_call/cancel" | "terminal/cancel" => {
                        let id = params[if method == "terminal/cancel" {
                            "terminalId"
                        } else {
                            "toolCallId"
                        }]
                        .as_str()
                        .ok_or_else(|| invalid("Missing work ID"))?;
                        let jobs = sync::get(
                            inner.daemon.base_url(),
                            inner.daemon.bearer_token(),
                            "/jobs",
                        )
                        .await
                        .map_err(internal)?;
                        let job = jobs
                            .as_array()
                            .into_iter()
                            .flatten()
                            .find(|j| j["handle"] == id)
                            .ok_or_else(|| invalid("Unknown work ID"))?;
                        if !matches!(
                            job["status"]["status"].as_str(),
                            Some("completed" | "cancelled" | "failed")
                        ) {
                            let mut url = reqwest::Url::parse("http://localhost/job/").unwrap();
                            url.path_segments_mut()
                                .unwrap()
                                .pop_if_empty()
                                .push(id)
                                .push("cancel");
                            post(&inner, url.path(), json!({}))
                                .await
                                .map_err(internal)?;
                        }
                        Ok(json!({}))
                    }
                    _ => Err(Error::method_not_found()),
                }
            }
        }
    }

    async fn setup(
        self: &Arc<Self>,
        method: &str,
        params: Value,
        conn: ConnectionTo<Client>,
        features: agent::ClientFeatureSupport,
    ) -> std::result::Result<Value, Error> {
        let _lifecycle = self.lifecycle.lock().await;
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::new(-32000, "Connection is closing"));
        }
        let cwd = store::absolute_directory(
            params["cwd"]
                .as_str()
                .ok_or_else(|| invalid("Missing cwd"))?,
        )
        .map_err(invalid)?;
        let servers = params.get("mcpServers").cloned().unwrap_or(json!([]));
        let parsed: Vec<v1::McpServer> =
            serde_json::from_value(servers.clone()).map_err(invalid)?;
        if parsed.iter().any(|s| matches!(s, v1::McpServer::Sse(_))) {
            return Err(invalid("SSE MCP transport is not supported by Polytoken"));
        }
        let resume = method == "session/resume";
        let replay = !params["replayFrom"].is_null();
        if replay && params["replayFrom"]["type"] != "start" {
            return Err(invalid("Unsupported replay cursor type"));
        }
        let requested = params["sessionId"].as_str();
        if resume && requested.is_none_or(|id| !store::valid_id(id)) {
            return Err(invalid("Invalid session ID"));
        }
        let existing = requested.and_then(|id| self.sessions.lock().unwrap().get(id).cloned());
        let session = if let Some(session) = existing {
            let mut inner = session.inner.lock().await;
            if inner.servers != servers {
                let config_path = inner.config_dir.join("config.yaml");
                let previous = std::fs::read(&config_path).map_err(internal)?;
                update_mcp(&inner.temp, &parsed).map_err(internal)?;
                match post(&inner, "/reload", json!({})).await {
                    Ok(value) => {
                        if value["failed"].as_array().is_some_and(|v| !v.is_empty()) {
                            std::fs::write(config_path, previous).map_err(internal)?;
                            return Err(internal(format!(
                                "Daemon configuration reload failed: {value}"
                            )));
                        }
                        inner.reload_pending = value["queued"] == true;
                    }
                    Err(error) => {
                        std::fs::write(config_path, previous).map_err(internal)?;
                        return Err(internal(error));
                    }
                }
                inner.servers = servers.clone();
            }
            if inner.cwd != cwd {
                inner.cwd = cwd.clone();
                inner.restart = true;
            }
            inner.record["cwd"] = json!(cwd);
            store::write(&session.id, &inner.record).map_err(internal)?;
            drop(inner);
            session
        } else {
            if resume {
                let id = requested.unwrap();
                let metadata = store::read(id).map_err(internal)?;
                if metadata.as_ref().is_some_and(|v| v["deleted"] == true) {
                    return Err(unknown());
                }
                if metadata.is_none()
                    && !store::discover()
                        .await
                        .map_err(internal)?
                        .iter()
                        .any(|v| v["sessionId"] == id)
                {
                    return Err(unknown());
                }
            }
            let temp = tempfile::Builder::new()
                .prefix("polytoken-acp-config-")
                .tempdir()
                .map_err(internal)?;
            let config_dir = update_mcp(&temp, &parsed).map_err(internal)?;
            let mut daemon = DaemonHandle::spawn_with_session_id(
                &cwd,
                requested.filter(|_| resume),
                Some(&config_dir),
            )
            .await
            .map_err(internal)?;
            let snapshot = match sync::snapshot(daemon.base_url(), daemon.bearer_token()).await {
                Ok(s) => s,
                Err(e) => {
                    daemon.terminate().await;
                    return Err(internal(e));
                }
            };
            let id = daemon.session_id().to_string();
            if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
                daemon.terminate().await;
                return Err(Error::new(-32000, "Connection is closing"));
            }
            let mut record = store::read(&id)
                .map_err(internal)?
                .unwrap_or(json!({"sessionId":id,"cwd":cwd}));
            if record["legacy_users"].is_null() {
                // Establish the identities predating shim-managed admissions.
                // Public history can redact text, so queue identity must not
                // depend on matching the original prompt's bytes.
                let retained = sync::history(daemon.base_url(), daemon.bearer_token())
                    .await
                    .map_err(internal)?;
                seed_legacy_users(&retained, &mut record);
                if record["entity_order"].is_null() {
                    record["entity_order"] = json!(
                        history_updates(&retained, &mut record)
                            .iter()
                            .filter_map(entity_key)
                            .collect::<Vec<_>>()
                    );
                }
            }
            let session = Arc::new(Session {
                id: id.clone(),
                conn,
                features,
                stop: CancellationToken::new(),
                observing: std::sync::atomic::AtomicBool::new(false),
                changed: tokio::sync::Notify::new(),
                refresh_children: tokio::sync::Notify::new(),
                inner: tokio::sync::Mutex::new(Inner {
                    daemon,
                    temp,
                    config_dir,
                    cwd: cwd.clone(),
                    servers: servers.clone(),
                    restart: false,
                    reload_pending: false,
                    closed: false,
                    snapshot,
                    revision_generation: 0,
                    seen: HashSet::new(),
                    entities: BTreeMap::new(),
                    live_text: HashMap::new(),
                    blocks: HashMap::new(),
                    handled: HashSet::new(),
                    recover_text: false,
                    terminal: None,
                    foreground: false,
                    state: Value::Null,
                    record,
                    children_paused: true,
                }),
            });
            self.sessions.lock().unwrap().insert(id, session.clone());
            session
        };
        let mut inner = session.inner.lock().await;
        if session.features.supports_surfaces {
            session.features.surfaces.reopen(&session.id);
        }
        session
            .restart_if_idle(&mut inner)
            .await
            .map_err(internal)?;
        if inner.restart || inner.reload_pending {
            session.notice("Applying workspace settings", "Existing foreground and background work continues. Resume will complete after it finishes and the new workspace is ready.").map_err(internal)?;
            while inner.restart || inner.reload_pending {
                let changed = session.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                drop(inner);
                tokio::select! {
                    _=session.stop.cancelled()=>return Err(unknown()),
                    _=changed=>{},
                }
                inner = session.inner.lock().await;
                if inner.closed {
                    return Err(unknown());
                }
                session
                    .restart_if_idle(&mut inner)
                    .await
                    .map_err(internal)?;
            }
        }
        inner.children_paused = true;
        if resume {
            if !replay && !session.observing.load(std::sync::atomic::Ordering::SeqCst) {
                session
                    .features
                    .children
                    .baseline(
                        &session.id,
                        inner.daemon.base_url(),
                        inner.daemon.bearer_token(),
                        &session.conn,
                        true,
                    )
                    .await
                    .map_err(internal)?;
            } else {
                session.children(&inner, replay, false).await;
            }
        }
        if replay {
            session.history(&mut inner, true).await.map_err(internal)?;
            session
                .emit(json!({"sessionUpdate":"replay_complete"}))
                .map_err(internal)?;
        }
        session
            .current(&mut inner, resume)
            .await
            .map_err(internal)?;
        let mut result = session.options(&inner).await.map_err(internal)?;
        if !resume {
            result["sessionId"] = json!(session.id);
        }
        inner.children_paused = false;
        drop(inner);
        session.refresh_children.notify_one();
        // Each session has exactly one observer, even across active resumes.
        if !session.stop.is_cancelled() && !existing_observer(&session) {
            let observer = session.clone();
            tokio::spawn(async move {
                observer.observe().await;
            });
        }
        Ok(result)
    }

    async fn list(&self, params: Value) -> std::result::Result<Value, Error> {
        let filter = match params.get("cwd") {
            Some(Value::String(s)) => {
                if !std::path::Path::new(s).is_absolute() {
                    return Err(invalid("cwd must be absolute"));
                }
                Some(s.clone())
            }
            None | Some(Value::Null) => None,
            _ => return Err(invalid("Invalid cwd filter")),
        };
        let mut values = if let Some(cursor) = params.get("cursor").filter(|v| !v.is_null()) {
            let cursor = cursor.as_str().ok_or_else(|| invalid("Invalid cursor"))?;
            let page = self
                .pages
                .lock()
                .unwrap()
                .get(cursor)
                .cloned()
                .ok_or_else(|| invalid("Invalid or expired session cursor"))?;
            if page.filter != filter {
                return Err(invalid("Cursor belongs to another directory filter"));
            }
            page.items
        } else {
            store::discover()
                .await
                .map_err(internal)?
                .into_iter()
                .filter(|v| {
                    filter.as_ref().is_none_or(|f| {
                        std::path::Path::new(f)
                            == std::path::Path::new(v["cwd"].as_str().unwrap_or(""))
                    })
                })
                .map(|v| {
                    let mut info = json!({"sessionId":v["sessionId"],"cwd":v["cwd"]});
                    if v["title"].is_string() {
                        info["title"] = v["title"].clone();
                    }
                    info
                })
                .collect::<Vec<_>>()
        };
        let remaining = values.split_off(values.len().min(100));
        let mut result = json!({"sessions":values});
        if !remaining.is_empty() {
            let cursor = uuid::Uuid::new_v4().to_string();
            let mut pages = self.pages.lock().unwrap();
            if pages.len() >= 16 {
                pages.clear();
            }
            pages.insert(
                cursor.clone(),
                ListPage {
                    filter,
                    items: remaining,
                },
            );
            result["nextCursor"] = json!(cursor);
        }
        Ok(result)
    }

    async fn close(&self, id: &str) {
        let session = self.sessions.lock().unwrap().remove(id);
        if let Some(session) = session {
            session.stop.cancel();
            session.features.children.close(id);
            session.features.surfaces.close(id);
            let mut inner = session.inner.lock().await;
            inner.closed = true;
            inner.daemon.terminate().await;
        }
    }

    pub async fn shutdown(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        let ids: Vec<_> = self.sessions.lock().unwrap().keys().cloned().collect();
        for id in ids {
            self.close(&id).await;
        }
        // Wait for a setup that was spawning before EOF, then catch any
        // session it inserted before observing the closed flag.
        let _lifecycle = self.lifecycle.lock().await;
        let ids: Vec<_> = self.sessions.lock().unwrap().keys().cloned().collect();
        for id in ids {
            self.close(&id).await;
        }
    }
}

fn update_mcp(temp: &tempfile::TempDir, servers: &[v1::McpServer]) -> Result<std::path::PathBuf> {
    let path = temp.path().join("mcp-config");
    std::fs::create_dir_all(&path)?;
    if servers.is_empty() {
        std::fs::write(path.join("config.yaml"), "mcp_servers: {}\n")?;
    } else {
        agent::write_mcp_servers_config(servers, temp.path())
            .context("Failed to write MCP configuration")?;
    }
    Ok(path)
}

async fn post(inner: &Inner, path: &str, payload: Value) -> Result<Value> {
    let response = reqwest::Client::new()
        .post(format!("{}{path}", inner.daemon.base_url()))
        .bearer_auth(inner.daemon.bearer_token())
        .json(&payload)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        bail!("POST {path}: HTTP {status}: {text}");
    }
    if text.is_empty() {
        Ok(json!({}))
    } else {
        Ok(serde_json::from_str(&text)?)
    }
}

// The observer flag is set synchronously while holding the session lock.
fn existing_observer(session: &Session) -> bool {
    use std::sync::atomic::Ordering;
    session.observing.swap(true, Ordering::SeqCst)
}

impl Session {
    async fn children(&self, inner: &Inner, replay: bool, live: bool) {
        if let Err(error) = self
            .features
            .children
            .refresh(
                &self.id,
                inner.daemon.base_url(),
                inner.daemon.bearer_token(),
                &self.conn,
                true,
                replay,
                live,
            )
            .await
        {
            tracing::warn!(%error, "Worker projection unavailable; preserving ordinary activity");
            self.features
                .children
                .unobservable(&self.id, &self.conn, true)
                .await;
        }
    }
    fn emit(&self, update: Value) -> Result<()> {
        let typed: agent_client_protocol::v2::schema::SessionUpdate =
            serde_json::from_value(update)?;
        agent::send_ext_notification(
            &self.conn,
            "_polytoken/v2_update",
            &json!({"sessionId":self.id,"update":typed}),
        );
        Ok(())
    }

    fn notice(&self, title: &str, detail: &str) -> Result<()> {
        self.emit(json!({"sessionUpdate":"notice","severity":"warning","title":title,"description":detail}))
    }

    fn publish(&self, inner: &mut Inner, update: Value, force: bool) -> Result<()> {
        let key = entity_key(&update).context("Update is not an entity")?;
        if inner.record["entity_order"].is_null() {
            inner.record["entity_order"] = json!([]);
        }
        let order = inner.record["entity_order"]
            .as_array_mut()
            .context("Invalid stored entity order")?;
        if !order.iter().any(|v| v == &key) {
            order.push(json!(key));
        }
        if !force
            && matches!(
                update["sessionUpdate"].as_str(),
                Some("agent_message" | "agent_thought")
            )
        {
            let previous = inner
                .entities
                .get(&key)
                .and_then(|v| v["content"][0]["text"].as_str());
            let text = update["content"][0]["text"].as_str().unwrap_or("");
            if previous.is_some_and(|p| p.len() > text.len() && p.starts_with(text)) {
                return Ok(());
            }
        }
        let value = if update["sessionUpdate"] == "tool_call_update" {
            let mut value = inner.entities.get(&key).cloned().unwrap_or(json!({}));
            value
                .as_object_mut()
                .unwrap()
                .extend(update.as_object().unwrap().clone());
            value
        } else {
            update
        };
        if force || inner.entities.get(&key) != Some(&value) {
            self.emit(value.clone())?;
        }
        inner.entities.insert(key, value);
        Ok(())
    }

    async fn options(&self, inner: &Inner) -> Result<Value> {
        let data = sync::state(inner.daemon.base_url(), inner.daemon.bearer_token()).await?;
        let monitor = sync::get(
            inner.daemon.base_url(),
            inner.daemon.bearer_token(),
            "/permission-monitor",
        )
        .await?;
        let mut options: Vec<Value> =
            agent::build_config_options(&Ok(data.clone()), &None, &Ok(monitor.clone()))
                .iter()
                .map(|o| {
                    let mut value = serde_json::to_value(o).unwrap();
                    let id = value.as_object_mut().unwrap().remove("id").unwrap();
                    value["configId"] = id;
                    value
                })
                .collect();
        options.push(json!({"configId":"permission_mode","name":"Permissions","type":"select",
            "currentValue":monitor["monitor"]["type"].as_str().unwrap_or("standard"),"options":[
                {"value":"standard","name":"Standard"},{"value":"bypass","name":"Bypass"},
                {"value":"bypass_plus","name":"Bypass+"},{"value":"autonomous","name":"Autonomous"}]}));
        let mut commands = agent::build_available_commands(&inner.cwd).unwrap_or_default();
        commands.extend(agent::build_skill_commands(&Ok(data)));
        let commands: Vec<_> = commands
            .iter()
            .map(|c| {
                let mut value = serde_json::to_value(c).unwrap();
                value["type"] = json!("command");
                value
            })
            .collect();
        Ok(json!({"configOptions":options,"availableCommands":commands}))
    }

    async fn configure(
        &self,
        inner: &mut Inner,
        params: Value,
    ) -> std::result::Result<Value, Error> {
        let options = self.options(inner).await.map_err(internal)?;
        let id = params["configId"]
            .as_str()
            .ok_or_else(|| invalid("Missing config ID"))?;
        let value = &params["value"];
        let option = options["configOptions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|o| o["configId"] == id)
            .ok_or_else(|| invalid("Unknown config option"))?;
        if option["type"] != params["type"] {
            return Err(invalid(
                "Configuration value type does not match the option",
            ));
        }
        if option["type"] == "select"
            && !option["options"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|o| o["value"] == *value)
        {
            return Err(invalid("Unknown configuration choice"));
        }
        let (path, body) = match id {
            "model" => ("/model".to_string(), json!({"model":value})),
            "thought_level" => {
                let state = sync::state(inner.daemon.base_url(), inner.daemon.bearer_token())
                    .await
                    .map_err(internal)?;
                (
                    "/model".into(),
                    agent::build_reasoning_request(&state, value.as_str().unwrap_or(""))
                        .ok_or_else(|| invalid("Unsupported reasoning effort"))?,
                )
            }
            "permission_mode" => ("/permission-monitor".into(), json!({"mode":value})),
            name if name.starts_with("mcp:") => {
                let enabled = value.as_bool().ok_or_else(|| invalid("Expected boolean"))?;
                let name = reqwest::Url::parse("http://localhost/")
                    .unwrap()
                    .join("mcp/")
                    .unwrap();
                let mut url = name;
                url.path_segments_mut()
                    .unwrap()
                    .pop_if_empty()
                    .push(&id[4..])
                    .push(if enabled { "enable" } else { "disable" });
                (url.path().to_string(), json!({}))
            }
            _ => return Err(invalid("Unknown config option")),
        };
        post(inner, &path, body).await.map_err(internal)?;
        let options = self.options(inner).await.map_err(internal)?;
        self.emit(json!({"sessionUpdate":"config_option_update","configOptions":options["configOptions"]})).map_err(internal)?;
        Ok(json!({"configOptions":options["configOptions"]}))
    }

    async fn prompt(&self, inner: &mut Inner, params: Value) -> std::result::Result<Value, Error> {
        self.restart_if_idle(inner).await.map_err(internal)?;
        if inner.restart {
            return Err(Error::new(
                -32000,
                "Workspace transition is waiting for existing work to finish",
            ));
        }
        let blocks = params["prompt"]
            .as_array()
            .ok_or_else(|| invalid("Missing prompt"))?;
        let mut payload = crate::prompt::convert(blocks).map_err(invalid)?;
        let text = payload["content"].as_str().unwrap_or("").to_string();
        let command = text.split_whitespace().next().unwrap_or("");
        let local_command = matches!(
            command,
            "/clear" | "/title" | "/facet" | "/mcp" | "/daemon-reload" | "/reset-shell" | "/goal"
        );
        if !local_command
            && command != "/compact"
            && let Some(name) = command.strip_prefix('/')
        {
            let catalog = sync::get(
                inner.daemon.base_url(),
                inner.daemon.bearer_token(),
                "/daemon-state",
            )
            .await
            .map_err(internal)?;
            if catalog["skills"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|s| s["name"] == name && s["user_invocable"].as_bool().unwrap_or(true))
            {
                payload["content"] = json!(format!("@skill:{name}{}", &text[command.len()..]));
            }
        }
        let snapshot = sync::snapshot(inner.daemon.base_url(), inner.daemon.bearer_token())
            .await
            .map_err(internal)?;
        let accepted = sync::mutate(
            inner.daemon.base_url(),
            inner.daemon.bearer_token(),
            "/prompt",
            payload.clone(),
            &snapshot,
        )
        .await
        .map_err(internal)?;
        let id = accepted["prompt_id"]
            .as_str()
            .ok_or_else(|| internal("Prompt admission returned no ID"))?;
        inner.foreground = true;
        inner.terminal = None;
        inner.record["users"][id] = json!({"content":blocks,"text":payload["content"],"queued":accepted["queued_item"].is_object()});
        if accepted["queued_item"].is_object() {
            if inner.record["queued_ids"].is_null() {
                inner.record["queued_ids"] = json!([]);
            }
            inner.record["queued_ids"]
                .as_array_mut()
                .unwrap()
                .push(json!(id));
        }
        if let Err(error) = store::write(&self.id, &inner.record) {
            // The daemon has already accepted this operation. Returning a
            // rejection here could cause the client to duplicate its action.
            self.notice("Session metadata could not be saved", &error.to_string())
                .map_err(internal)?;
        }
        self.publish(
            inner,
            json!({"sessionUpdate":"user_message","messageId":id,"content":blocks}),
            false,
        )
        .map_err(internal)?;
        // The observer cannot publish terminal work before this lock is released.
        self.emit(json!({"sessionUpdate":"state_update","state":"running","background":background(&inner.state)})).map_err(internal)?;
        let mut result = json!({"messageId":id});
        if local_command && accepted["queued_item"].is_null() {
            let current = match sync::snapshot(inner.daemon.base_url(), inner.daemon.bearer_token())
                .await
            {
                Ok(current) => current,
                Err(error) => {
                    self.notice("Waiting for command state", &error.to_string())
                        .map_err(internal)?;
                    inner.foreground = false;
                    inner.terminal = Some(json!({"stopReason":"end_turn"}));
                    result["_polytoken_idle"] = json!({"sessionId":self.id,"update":{"sessionUpdate":"state_update","state":"unknown","background":background(&inner.state)}});
                    return Ok(result);
                }
            };
            if !current["turn"].is_object() {
                inner.foreground = false;
                inner.terminal = Some(json!({"stopReason":"end_turn"}));
                let state = json!({"sessionUpdate":"state_update","state":"idle","stopReason":"end_turn","background":background(&inner.state)});
                inner.state = state.clone();
                // The frontend emits this only AFTER the insertion response.
                result["_polytoken_idle"] = json!({"sessionId":self.id,"update":state});
            }
        }
        Ok(result)
    }

    async fn restart_if_idle(&self, inner: &mut Inner) -> Result<()> {
        if !inner.restart {
            return Ok(());
        }
        let snapshot = sync::snapshot(inner.daemon.base_url(), inner.daemon.bearer_token()).await?;
        let live_jobs = snapshot["jobs"].as_array().into_iter().flatten().any(|j| {
            !matches!(
                j["status"].as_str(),
                Some("completed" | "failed" | "cancelled")
            )
        });
        // 0.9.8 can retain a drained item in /sync.queue. The ordered slot
        // lifecycle and active turn, rather than that preview, gate restart.
        if inner.foreground || snapshot["turn"].is_object() || live_jobs {
            return Ok(());
        }
        self.restart(inner).await
    }

    async fn restart(&self, inner: &mut Inner) -> Result<()> {
        // Capture any durable output not yet delivered by the old connection.
        // A failed daemon may be unavailable, so recovery can continue from its
        // saved session after relaunch in that case.
        let _ = self.history(inner, false).await;
        inner.daemon.terminate().await;
        inner.daemon = DaemonHandle::spawn_with_session_id(
            &inner.cwd,
            Some(&self.id),
            Some(&inner.config_dir),
        )
        .await?;
        inner.snapshot =
            sync::snapshot(inner.daemon.base_url(), inner.daemon.bearer_token()).await?;
        inner.revision_generation += 1;
        inner.seen.clear();
        inner.live_text.clear();
        inner.blocks.clear();
        inner.handled.clear();
        inner.recover_text = false;
        inner.restart = false;
        inner.reload_pending = false;
        self.changed.notify_waiters();
        Ok(())
    }

    async fn history(&self, inner: &mut Inner, force: bool) -> Result<()> {
        if !inner.children_paused {
            self.children(inner, false, true).await;
        }
        let items = sync::history(inner.daemon.base_url(), inner.daemon.bearer_token()).await?;
        let updates = history_updates(&items, &mut inner.record);
        for update in updates {
            self.publish(inner, update, force)?;
        }
        store::write(&self.id, &inner.record)?;
        Ok(())
    }

    async fn current(&self, inner: &mut Inner, force: bool) -> Result<()> {
        if self.features.supports_surfaces {
            self.features
                .surfaces
                .bind(
                    &self.id,
                    crate::surfaces::Endpoint::new(
                        inner.daemon.base_url(),
                        inner.daemon.bearer_token(),
                    ),
                    &self.conn,
                )
                .await;
        }
        if !inner.children_paused {
            self.children(inner, false, true).await;
        }
        let snapshot = sync::snapshot(inner.daemon.base_url(), inner.daemon.bearer_token()).await?;
        let jobs = sync::get(
            inner.daemon.base_url(),
            inner.daemon.bearer_token(),
            "/jobs",
        )
        .await?;
        let mut background = Vec::new();
        for job in jobs.as_array().context("Invalid jobs catalog")? {
            let id = job["handle"].as_str().context("Job has no ID")?;
            if job["kind"] == "subagent" {
                if !self.features.children.represented(&self.id, id).await {
                    // Work discovered after the replay capture belongs to the
                    // fresh observation after resume, not historical traffic.
                    if inner.children_paused {
                        continue;
                    }
                    self.children(inner, false, !inner.children_paused).await;
                }
                if !self.features.children.fallback(&self.id, id).await {
                    continue;
                }
            }
            let status = job["status"]["status"]
                .as_str()
                .context("Invalid job status")?;
            let final_status = matches!(status, "completed" | "failed" | "cancelled");
            let process_backed = matches!(job["kind"].as_str(), Some("shell" | "service"));
            let mut update = json!({"sessionUpdate":"tool_call_update","toolCallId":id,
                "title":job["command_line"].as_str().or_else(|| job["subagent_activity"].as_str()).unwrap_or(job["tool_name"].as_str().unwrap_or("Background work")),
                "status":match status {"completed"=>"completed","failed"=>"failed","cancelled"=>"cancelled",_=>"in_progress"},"capabilities":if final_status {json!({})} else {json!({"cancel":{}})}});
            if process_backed {
                let mut terminal = json!({"sessionUpdate":"terminal_update","terminalId":id,
                    "command":job["command_line"].as_str().unwrap_or("Background process"),
                    "cwd":inner.cwd,"capabilities":if final_status {json!({})}else{json!({"cancel":{}})}});
                let mut output = String::new();
                for channel in ["stdout", "stderr"] {
                    let mut offset = 0;
                    loop {
                        let path = format!(
                            "/job/{id}/output?channel={channel}&offset={offset}&limit=65536"
                        );
                        let page =
                            sync::get(inner.daemon.base_url(), inner.daemon.bearer_token(), &path)
                                .await;
                        let Ok(page) = page else { break };
                        if page["offset"].as_u64() != Some(offset) {
                            bail!("Job output offset mismatch");
                        }
                        let content = page["content"].as_str().context("Invalid job output")?;
                        output.push_str(content);
                        offset += content.len() as u64;
                        if offset >= page["total"].as_u64().unwrap_or(offset)
                            || content.is_empty()
                            || output.len() >= 2 * 1024 * 1024
                        {
                            break;
                        }
                    }
                }
                use base64::Engine;
                terminal["output"] = json!({"data":base64::engine::general_purpose::STANDARD.encode(output.as_bytes())});
                if final_status {
                    terminal["exitStatus"] = json!({});
                    if let Some(code) = job["status"]["exit_code"].as_i64() {
                        terminal["exitStatus"]["exitCode"] = json!(code);
                    }
                    if let Some(signal) = job["status"]["signal"].as_str() {
                        terminal["exitStatus"]["signal"] = json!(signal);
                    }
                }
                self.publish(inner, terminal, force)?;
                update["content"] = json!([{"type":"terminal","terminalId":id}]);
            }
            if final_status {
                let key = format!("tool_call_update:{id}");
                if !inner
                    .entities
                    .get(&key)
                    .is_some_and(|v| v["status"] == update["status"])
                {
                    match sync::get(
                        inner.daemon.base_url(),
                        inner.daemon.bearer_token(),
                        &format!("/job/{id}/result"),
                    )
                    .await
                    {
                        Ok(result) => {
                            let text =
                                crate::tool_output::text(&result["content"]).unwrap_or_default();
                            update["content"] =
                                json!([{"type":"content","content":{"type":"text","text":text}}]);
                            update["rawOutput"] = result;
                        }
                        Err(e) => {
                            update["content"] = json!([{"type":"content","content":{"type":"text","text":format!("Retained result unavailable: {e}")}}])
                        }
                    }
                }
            } else {
                background.push(if process_backed {
                    json!({"type":"terminal","terminalId":id})
                } else {
                    json!({"type":"tool_call","toolCallId":id})
                });
            }
            self.publish(inner, update, force)?;
        }
        let mut state = json!({"sessionUpdate":"state_update","state":sync::foreground(&snapshot),"background":background});
        // State snapshots can be ahead of the ordered stream. A fast model may
        // already be idle while we are applying turn_started. Completion must
        // wait until that turn's slot_released record has been applied.
        if state["state"] == "idle" && inner.foreground {
            state["state"] = json!("running");
        }
        if let Some(terminal) = &inner.terminal
            && state["state"] == "idle"
        {
            state["stopReason"] = terminal["stopReason"].clone();
            if !terminal["error"].is_null() {
                state["error"] = terminal["error"].clone();
            }
        }
        if force || state != inner.state {
            self.emit(state.clone())?;
        }
        inner.state = state;
        if let Some(title) = snapshot["title"].as_str() {
            if force || inner.record["title"] != title {
                self.emit(json!({"sessionUpdate":"session_info_update","title":title}))?;
            }
            inner.record["title"] = json!(title);
        }
        inner.record["cwd"] = json!(inner.cwd);
        store::write(&self.id, &inner.record)?;
        // Preserve the stream baseline: this fresh snapshot is a state read,
        // not evidence that records before its cursor have been applied.
        self.pending_questions(inner, &snapshot).await?;
        Ok(())
    }

    async fn pending_questions(&self, inner: &mut Inner, snapshot: &Value) -> Result<()> {
        if inner.children_paused {
            return Ok(());
        }
        if snapshot["interrogatives"]
            .as_array()
            .is_none_or(Vec::is_empty)
        {
            return Ok(());
        }
        let state = sync::get(
            inner.daemon.base_url(),
            inner.daemon.bearer_token(),
            "/state",
        )
        .await?;
        for event in state["pending_interrogatives"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let Some(id) = event["interrogative_id"].as_str() else {
                continue;
            };
            if inner.handled.contains(id) {
                continue;
            }
            let mut event = event.clone();
            let (sid, cwd) = if let Some(handle) = event["subagent_handle"].as_str() {
                // The question read can be ahead of the earlier jobs/history
                // capture. Establish its child before emitting an RPC to it.
                self.children(inner, false, true).await;
                match self.features.children.route(&self.id, handle).await {
                    Some(route) => route,
                    None if self.features.children.represented(&self.id, handle).await => continue,
                    None => {
                        event["question"] = json!(format!(
                            "Worker {handle}: {}",
                            event["question"].as_str().unwrap_or("Permission required")
                        ));
                        (self.id.clone(), inner.cwd.clone())
                    }
                }
            } else {
                (self.id.clone(), inner.cwd.clone())
            };
            if inner.handled.insert(id.to_string()) {
                agent::forward_v2_interrogative(
                    &self.conn,
                    &event,
                    &sid,
                    inner.daemon.base_url(),
                    inner.daemon.bearer_token(),
                    &cwd,
                    &self.features,
                )
                .await;
            }
        }
        Ok(())
    }

    async fn apply(&self, inner: &mut Inner, record: Value) -> Result<()> {
        tracing::debug!(session_id=%self.id,record_type=%record["type"],"Applying ordered ACP v2 record");
        match record["type"].as_str().unwrap_or("") {
            "stream_block_started" => {
                if let (Some(id), Some(kind)) =
                    (record["block_id"].as_str(), record["kind"].as_str())
                {
                    inner.blocks.insert(id.to_string(), kind.to_string());
                }
            }
            "stream_block_delta" if !inner.recover_text => {
                let id = record["block_id"].as_str().unwrap_or("");
                let kind = inner.blocks.get(id).map(String::as_str).unwrap_or("");
                if matches!(kind, "text" | "thinking") {
                    let pid = id.rsplit_once(':').map(|(pid, _)| pid).unwrap_or(id);
                    let message_type = if kind == "text" {
                        "agent_message"
                    } else {
                        "agent_thought"
                    };
                    let key = format!("{message_type}:{pid}");
                    let text = inner.live_text.entry(key.clone()).or_default();
                    text.push_str(record["delta"].as_str().unwrap_or(""));
                    let text = text.clone();
                    // A replay capture can be ahead of this retained stream.
                    // Do not regress or append duplicate bytes while catching up.
                    let displayed = inner
                        .entities
                        .get(&key)
                        .and_then(|v| v["content"][0]["text"].as_str())
                        .unwrap_or("");
                    if !displayed.starts_with(&text) || text.len() >= displayed.len() {
                        self.publish(inner, json!({"sessionUpdate":message_type,"messageId":pid,"content":[{"type":"text","text":text}]}), false)?;
                    }
                }
            }
            "history_appended" | "stream_block_completed" => self.history(inner, false).await?,
            "turn_started" => {
                inner.foreground = true;
                inner.terminal = None;
                inner.recover_text = false;
                self.current(inner, true).await?;
            }
            "turn_cancel_requested" => inner.terminal = Some(json!({"stopReason":"cancelled"})),
            "turn_completed" => {
                if inner.terminal.is_none() {
                    inner.terminal = Some(json!({"stopReason":"end_turn"}));
                }
                self.history(inner, false).await?;
            }
            "turn_failed" => {
                if inner
                    .terminal
                    .as_ref()
                    .is_none_or(|t| t["stopReason"] != "cancelled")
                {
                    inner.terminal = Some(
                        json!({"stopReason":"error","error":{"code":-32603,"message":record["reason"].as_str().unwrap_or("Turn failed")}}),
                    );
                }
                self.history(inner, false).await?;
            }
            "slot_released"
            | "jobs_changed"
            | "subagent_changed"
            | "interrogative_registered"
            | "interrogative_resolved"
            | "interrogative_withdrawn"
            | "title_changed" => {
                if record["type"] == "slot_released" {
                    inner.foreground = false;
                }
                self.current(inner, false).await?;
                self.restart_if_idle(inner).await?;
                self.changed.notify_waiters();
            }
            "catalog_published" | "model_changed" | "facet_changed" | "monitor_changed" => {
                if record["type"] == "catalog_published" {
                    inner.reload_pending = false;
                    self.changed.notify_waiters();
                }
                let options = self.options(inner).await?;
                self.emit(json!({"sessionUpdate":"config_option_update","configOptions":options["configOptions"]}))?;
                self.emit(json!({"sessionUpdate":"available_commands_update","availableCommands":options["availableCommands"]}))?;
            }
            "todos_changed" => {
                let data =
                    sync::state(inner.daemon.base_url(), inner.daemon.bearer_token()).await?;
                if let Some(plan) = crate::events::build_plan_from_state(&data) {
                    self.publish(inner, json!({"sessionUpdate":"plan_update","plan":{"type":"items","planId":"todos","entries":plan.entries}}), false)?;
                }
            }
            "context_changed" => {
                if let Some(size) = record["usage"]["limit_tokens"].as_u64().filter(|n| *n > 0) {
                    self.emit(json!({"sessionUpdate":"usage_update","used":record["usage"]["used_tokens"].as_u64().unwrap_or(0),"size":size}))?;
                }
            }
            "compaction_started" => {
                inner.foreground = true;
                self.publish(inner,json!({"sessionUpdate":"compaction_update","compactionId":record["compaction_id"],"status":"in_progress"}),false)?;
                self.current(inner, false).await?;
            }
            "compaction_completed" | "compaction_failed" => {
                let mut update = json!({"sessionUpdate":"compaction_update","compactionId":record["compaction_id"],"status":if record["type"]=="compaction_completed" {"completed"}else{"failed"}});
                if record["type"] == "compaction_failed" {
                    update["error"] = record["reason"].clone();
                }
                self.publish(inner, update, false)?;
                let snapshot =
                    sync::snapshot(inner.daemon.base_url(), inner.daemon.bearer_token()).await?;
                if !snapshot["turn"].is_object() {
                    inner.foreground = false;
                    inner.terminal = Some(
                        json!({"stopReason":if record["type"]=="compaction_completed" {"end_turn"}else{"error"}}),
                    );
                }
                self.history(inner, false).await?;
                self.current(inner, false).await?;
            }
            "history_rewritten" => {
                self.features.children.rescan(&self.id).await;
                self.history(inner, true).await?;
                self.notice("Conversation history changed", "The retained conversation was synchronized after a clear, rewind or compaction.")?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn observe(self: Arc<Self>) {
        let mut failures = 0;
        loop {
            if self.stop.is_cancelled() {
                return;
            }
            let result = self.stream().await;
            if self.stop.is_cancelled() {
                return;
            }
            if let Err(error) = result {
                tracing::warn!(session_id=%self.id,%error,"ACP v2 observer reconnecting");
                failures += 1;
                if failures >= 3 {
                    let mut inner = self.inner.lock().await;
                    let healthy = matches!(
                        tokio::time::timeout(
                            std::time::Duration::from_secs(2),
                            inner.daemon.health()
                        )
                        .await,
                        Ok(Ok(200))
                    );
                    if healthy {
                        // A malformed record or a temporarily unavailable
                        // projection is not grounds to kill healthy work.
                        failures = 0;
                        let state = json!({"sessionUpdate":"state_update","state":"unknown","background":background(&inner.state)});
                        let _ = self.emit(state.clone());
                        inner.state = state;
                        let _ =
                            self.notice("Waiting for session synchronization", &error.to_string());
                        drop(inner);
                        tokio::select! { _=self.stop.cancelled()=>return, _=tokio::time::sleep(std::time::Duration::from_secs(1))=>{} }
                        continue;
                    }
                    let interrupted = inner.state["state"] != "idle";
                    match self.restart(&mut inner).await {
                        Ok(()) => {
                            failures = 0;
                            inner.foreground = false;
                            if interrupted {
                                inner.terminal = Some(
                                    json!({"stopReason":"error","error":{"code":-32603,"message":"Daemon restarted; retained output was recovered. The interrupted prompt was not resubmitted."}}),
                                );
                            }
                            let _ = self.history(&mut inner, false).await;
                            let _ = self.current(&mut inner, true).await;
                            let _ = self.notice("Daemon connection recovered", "Retained history and current state were synchronized; no prompt was resubmitted.");
                        }
                        Err(error) => {
                            let _ = self.notice("Daemon unavailable", &error.to_string());
                        }
                    }
                }
            } else {
                failures = 0;
            }
            tokio::select! { _=self.stop.cancelled()=>return, _=tokio::time::sleep(std::time::Duration::from_millis(200))=>{} }
        }
    }

    async fn stream(&self) -> Result<()> {
        let (base, token, cursor, generation) = {
            let inner = self.inner.lock().await;
            (
                inner.daemon.base_url().to_string(),
                inner.daemon.bearer_token().to_string(),
                inner.snapshot["cursor"]
                    .as_str()
                    .context("Missing sync cursor")?
                    .to_string(),
                inner.revision_generation,
            )
        };
        let response = tokio::select! {
            _=self.stop.cancelled()=>return Ok(()),
            response=reqwest::Client::new().get(format!("{base}/events")).query(&[("after",cursor)]).bearer_auth(&token).send()=>response?,
        };
        if matches!(response.status().as_u16(), 400 | 409 | 410) {
            return self.reset().await;
        }
        if !response.status().is_success() {
            bail!("Ordered stream rejected: {}", response.status());
        }
        let mut stream = response.bytes_stream();
        let mut parser = agent::SseParser::new();
        loop {
            let chunk = tokio::select! {
                _=self.stop.cancelled()=>return Ok(()),
                _=self.refresh_children.notified()=> {
                    let mut inner=self.inner.lock().await;
                    self.current(&mut inner,false).await?;
                    continue;
                },
                chunk=stream.next()=>match chunk {Some(chunk)=>chunk?,None=>bail!("Ordered stream closed")},
                _=tokio::time::sleep(std::time::Duration::from_secs(20))=> {
                    // Quiet streams still refresh background state and detect a
                    // dead child; a reconnect replays retained records safely.
                    let mut inner=self.inner.lock().await;
                    self.current(&mut inner,false).await?;
                    return Ok(());
                }
            };
            for data in parser.feed(&chunk) {
                let envelope: Value = serde_json::from_str(&data)?;
                if envelope["session_id"] != self.id {
                    bail!("Wrong session in ordered stream");
                }
                let event = &envelope["event"];
                let revision = event["revision"]
                    .as_str()
                    .context("Missing ordered revision")?;
                if matches!(
                    event["record"]["type"].as_str(),
                    Some("reset_required" | "epoch_started")
                ) {
                    return self.reset().await;
                }
                let mut inner = self.inner.lock().await;
                if inner.closed {
                    return Ok(());
                }
                if generation != inner.revision_generation {
                    return Ok(());
                }
                if inner.seen.contains(revision) {
                    continue;
                }
                tokio::select! {
                    _=self.stop.cancelled()=>return Ok(()),
                    result=self.apply(&mut inner,event["record"].clone())=>result?,
                }
                inner.seen.insert(revision.to_string());
            }
        }
    }

    async fn reset(&self) -> Result<()> {
        self.features.children.rescan(&self.id).await;
        let mut inner = self.inner.lock().await;
        let snapshot = sync::snapshot(inner.daemon.base_url(), inner.daemon.bearer_token()).await?;
        if inner.foreground && !snapshot["turn"].is_object() {
            inner.terminal = Some(
                json!({"stopReason":"error","error":{"code":-32603,"message":"The turn ended outside the retained event stream. Retained history was recovered; its exact terminal outcome is unavailable."}}),
            );
        }
        inner.foreground = snapshot["turn"].is_object();
        inner.snapshot = snapshot;
        inner.revision_generation += 1;
        inner.seen.clear();
        inner.recover_text = true;
        inner.live_text.clear();
        inner.blocks.clear();
        self.history(&mut inner, false).await?;
        self.current(&mut inner, true).await?;
        self.notice(
            "Session synchronization recovered",
            "Retained conversation and current activity were synchronized after a stream reset.",
        )?;
        Ok(())
    }
}

fn entity_key(update: &Value) -> Option<String> {
    let kind = update["sessionUpdate"].as_str()?;
    let id = match kind {
        "user_message" | "agent_message" | "agent_thought" => update["messageId"].as_str()?,
        "tool_call_update" => update["toolCallId"].as_str()?,
        "plan_update" => update["plan"]["planId"].as_str()?,
        "compaction_update" => update["compactionId"].as_str()?,
        "terminal_update" => update["terminalId"].as_str()?,
        _ => return None,
    };
    Some(format!("{kind}:{id}"))
}

fn background(state: &Value) -> Value {
    state.get("background").cloned().unwrap_or(json!([]))
}

fn seed_legacy_users(items: &[Value], metadata: &mut Value) {
    metadata["legacy_users"] = json!({});
    for item in items
        .iter()
        .filter(|item| item["body"]["type"] == "user_message")
    {
        let id = item["prompt_id"]
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| format!("{}:{}", item["lineage"], item["raw_index"]));
        metadata["legacy_users"][&id] = json!(true);
    }
}

fn history_updates(items: &[Value], metadata: &mut Value) -> Vec<Value> {
    let mut updates = Vec::<Value>::new();
    let mut indexes = HashMap::<String, usize>::new();
    for item in items {
        let pid = item["prompt_id"]
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| format!("{}:{}", item["lineage"], item["raw_index"]));
        for normalized in sync::normalize(item) {
            let mut add = |update: Value| {
                let key = entity_key(&update).unwrap();
                if let Some(index) = indexes.get(&key) {
                    if matches!(
                        update["sessionUpdate"].as_str(),
                        Some("agent_message" | "agent_thought")
                    ) {
                        let old = updates[*index]["content"][0]["text"].as_str().unwrap_or("");
                        let new = update["content"][0]["text"].as_str().unwrap_or("");
                        updates[*index]["content"][0]["text"] = json!(format!("{old}{new}"));
                    } else {
                        updates[*index]
                            .as_object_mut()
                            .unwrap()
                            .extend(update.as_object().unwrap().clone());
                    }
                } else {
                    indexes.insert(key, updates.len());
                    updates.push(update);
                }
            };
            match normalized["type"].as_str().unwrap_or("") {
                "user" => {
                    // A queued admission has its own stable prompt ID, while
                    // the daemon may mint another ID when draining it. Bind
                    // the retained user message to the original ACP ID once.
                    if metadata["users"][&pid].is_null()
                        && metadata["aliases"][&pid].is_null()
                        && metadata["legacy_users"][&pid] != true
                    {
                        let text = normalized["content"].as_str().unwrap_or("");
                        let ordered = metadata["queued_ids"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_str)
                            .find(|id| metadata["users"][*id]["queued"] == true)
                            .map(str::to_string);
                        let matched = ordered.or_else(|| {
                            metadata["users"]
                                .as_object()
                                .into_iter()
                                .flatten()
                                .find_map(|(id, user)| {
                                    let used = metadata["aliases"]
                                        .as_object()
                                        .is_some_and(|a| a.values().any(|v| v == id));
                                    (user["queued"] == true && user["text"] == text && !used)
                                        .then(|| id.clone())
                                })
                        });
                        if let Some(id) = matched {
                            metadata["aliases"][&pid] = json!(id);
                            metadata["users"][&id]["queued"] = json!(false);
                        }
                    } else if metadata["users"][&pid].is_object() {
                        metadata["users"][&pid]["queued"] = json!(false);
                    }
                    let id = metadata["aliases"][&pid]
                        .as_str()
                        .unwrap_or(&pid)
                        .to_string();
                    let content = metadata["users"][&id]["content"].as_array().cloned().unwrap_or_else(|| vec![json!({"type":"text","text":normalized["content"].as_str().unwrap_or("")})]);
                    add(json!({"sessionUpdate":"user_message","messageId":id,"content":content}));
                }
                "assistant" => {
                    for block in normalized["blocks"].as_array().into_iter().flatten() {
                        match block["type"].as_str().unwrap_or("") {
                            "text" | "thinking" => add(
                                json!({"sessionUpdate":if block["type"]=="text" {"agent_message"}else{"agent_thought"},"messageId":pid,"content":[{"type":"text","text":block["text"].as_str().unwrap_or("")}]}),
                            ),
                            "tool_use" => add(
                                json!({"sessionUpdate":"tool_call_update","toolCallId":block["id"],"title":block["name"],"status":"in_progress","rawInput":block["input"]}),
                            ),
                            _ => {}
                        }
                    }
                }
                "tool_result" => {
                    let text = crate::tool_output::text(&normalized["content"]).unwrap_or_default();
                    add(
                        json!({"sessionUpdate":"tool_call_update","toolCallId":normalized["call_id"],"status":if normalized["is_error"]==true {"failed"}else{"completed"},"rawOutput":normalized["content"],"content":[{"type":"content","content":{"type":"text","text":text}}]}),
                    );
                }
                "native_compaction_boundary_v1" | "compaction_fencepost" => add(
                    json!({"sessionUpdate":"compaction_update","compactionId":format!("{}:{}",item["lineage"],item["raw_index"]),"status":"completed"}),
                ),
                _ => {}
            }
        }
    }
    // Admissions retained by the shim also survive replay while still queued,
    // and locally handled commands need not appear in the daemon transcript.
    for (id, user) in metadata["users"].as_object().into_iter().flatten() {
        let key = format!("user_message:{id}");
        if !indexes.contains_key(&key) && user["content"].is_array() {
            updates.push(
                json!({"sessionUpdate":"user_message","messageId":id,"content":user["content"]}),
            );
        }
    }
    let order: HashMap<String, usize> = metadata["entity_order"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .filter_map(|(index, key)| key.as_str().map(|key| (key.to_string(), index)))
        .collect();
    updates.sort_by_key(|update| {
        entity_key(update)
            .and_then(|key| order.get(&key).copied())
            .unwrap_or(usize::MAX)
    });
    updates
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn queued_history_preserves_the_insertion_id_across_daemon_drain() {
        let mut metadata = json!({"users":{"accepted-id":{"queued":true,"text":"hello","content":[{"type":"text","text":"hello"}]}}});
        let items = vec![
            json!({"prompt_id":"drained-id","lineage":"lineage","raw_index":1,
            "body":{"type":"user_message","content":{"text":"hello","truncated":false}}}),
        ];
        let updates = history_updates(&items, &mut metadata);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0]["messageId"], "accepted-id");
        assert_eq!(metadata["aliases"]["drained-id"], "accepted-id");
        assert_eq!(history_updates(&items, &mut metadata), updates);
    }
    #[test]
    fn replay_keeps_history_before_a_compaction_boundary() {
        let items = vec![
            json!({"prompt_id":"before","raw_index":1,"lineage":"l","body":{"type":"user_message","content":{"text":"before","truncated":false}}}),
            json!({"raw_index":2,"lineage":"l","body":{"type":"native_compaction_boundary_v1"}}),
            json!({"prompt_id":"after","raw_index":3,"lineage":"l","body":{"type":"user_message","content":{"text":"after","truncated":false}}}),
        ];
        let updates = history_updates(&items, &mut json!({}));
        assert_eq!(updates.len(), 3);
        assert_eq!(updates[0]["messageId"], "before");
        assert_eq!(updates[1]["sessionUpdate"], "compaction_update");
        assert_eq!(updates[2]["messageId"], "after");
    }
    #[test]
    fn pending_admissions_are_replayed_before_the_daemon_persists_them() {
        let mut metadata = json!({"users":{"accepted-id":{"queued":true,"content":[{"type":"text","text":"pending"}]}}});
        let updates = history_updates(&[], &mut metadata);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0]["messageId"], "accepted-id");
    }
    #[test]
    fn queue_identity_survives_redaction_without_rebinding_legacy_messages() {
        let mut metadata = json!({"queued_ids":["accepted"],
            "users":{"accepted":{"queued":true,"text":"original secret","content":[{"type":"text","text":"original secret"}]}}});
        let items = vec![
            json!({"lineage":"legacy","raw_index":1,"body":{"type":"user_message","content":{"text":"old history","truncated":false}}}),
            json!({"prompt_id":"drained","body":{"type":"user_message","content":{"text":"[redacted]","truncated":false}}}),
        ];
        seed_legacy_users(&items[..1], &mut metadata);
        let updates = history_updates(&items, &mut metadata);
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0]["messageId"], "\"legacy\":1");
        assert_eq!(updates[1]["messageId"], "accepted");
        assert_eq!(updates[1]["content"][0]["text"], "original secret");
    }
}
