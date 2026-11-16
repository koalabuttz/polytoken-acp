//! Read-only ACP views of daemon-owned workers. No child daemon or scheduler.
//!
//! The public checkpoint transcript omits launch directories and child history.
//! We therefore allowlist launch records from authenticated /history and page
//! /subagent/{handle}/history only after ordered invalidations. Never publish
//! rendered definitions, provider state, opaque reasoning, or runtime deltas.
use crate::{agent, store, sync};
use agent_client_protocol::{
    Client,
    v1::{ConnectionTo, schema as v1},
};
use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;

#[derive(Default, Debug)]
pub(crate) struct Hub {
    roots: Mutex<HashMap<String, Arc<Tracker>>>,
}

#[derive(Debug)]
struct Tracker {
    projection: tokio::sync::Mutex<Projection>,
    stop: CancellationToken,
}

#[derive(Default, Debug, Serialize, Deserialize)]
struct Projection {
    root: String,
    nodes: BTreeMap<String, Node>,
    calls: BTreeMap<String, Call>,
    legacy: HashSet<String>,
    #[serde(skip)]
    offset: usize,
    #[serde(skip)]
    sent: HashSet<String>,
    #[serde(skip)]
    current: HashMap<String, Value>,
    #[serde(skip)]
    announced: HashSet<String>,
    #[serde(skip)]
    grants: HashSet<String>,
    #[serde(skip)]
    questions: HashSet<String>,
    #[serde(skip)]
    epoch: Option<String>,
    #[serde(skip)]
    suppress_history: bool,
    #[serde(skip)]
    grant_epoch: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Node {
    id: String,
    birth: String,
    /// Empty string is the proven root; None is unobserved parentage.
    parent: Option<String>,
    title: String,
    model: Option<String>,
    cwd: Option<PathBuf>,
    launch_cwd: PathBuf,
    directories: Vec<PathBuf>,
    history: Vec<Value>,
    state: Value,
    last_stop: Option<Value>,
    outcome: Option<Value>,
    incomplete: bool,
    /// Reused daemon handles without a conversation identity cannot be routed
    /// safely. Preserve the old view, and use ordinary activity for new work.
    ambiguous: bool,
    fallback: bool,
    todos: BTreeMap<String, Value>,
    initial_message: Option<Value>,
    #[serde(skip)]
    cursor: usize,
    #[serde(skip)]
    anchor: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Call {
    sender: String,
    id: String,
    name: String,
    input: Value,
    recipients: Vec<String>,
    delivered: bool,
    prompt: String,
    finished: bool,
}

fn path(kind: &str, handle: &str, tail: &str) -> String {
    let mut url = reqwest::Url::parse(&format!("http://localhost/{kind}/")).unwrap();
    url.path_segments_mut()
        .unwrap()
        .pop_if_empty()
        .push(handle)
        .push(tail);
    url.path().into()
}

fn active(job: &Value) -> bool {
    matches!(
        job["status"]["status"].as_str(),
        Some("running" | "queued" | "reserved")
    )
}

fn descendants(jobs: &[Value], mut targets: HashSet<String>) -> HashSet<String> {
    loop {
        let before = targets.len();
        for job in jobs {
            if job["parent"]["type"] == "subagent"
                && job["parent"]["handle"]
                    .as_str()
                    .is_some_and(|h| targets.contains(h))
                && let Some(h) = job["handle"].as_str()
            {
                targets.insert(h.into());
            }
        }
        if before == targets.len() {
            return targets;
        }
    }
}

async fn cancel_jobs(base: &str, token: &str, targets: HashSet<String>) -> Result<()> {
    let jobs = sync::get(base, token, "/jobs").await?;
    let jobs = jobs.as_array().context("Invalid jobs")?;
    let targets = descendants(jobs, targets);
    for job in jobs {
        if let Some(h) = job["handle"].as_str()
            && targets.contains(h)
            && active(job)
        {
            let response = reqwest::Client::new()
                .post(format!("{base}{}", path("job", h, "cancel")))
                .bearer_auth(token)
                .json(&json!({}))
                .send()
                .await?;
            if !response.status().is_success() && !matches!(response.status().as_u16(), 404 | 409) {
                bail!("Worker cancellation rejected: {}", response.status());
            }
        }
    }
    Ok(())
}

impl Hub {
    /// Establish a reconnect baseline without replaying old transcript views.
    pub async fn baseline(
        &self,
        root: &str,
        base: &str,
        token: &str,
        conn: &ConnectionTo<Client>,
        v2: bool,
    ) -> Result<()> {
        let tracker = self.tracker(root)?;
        tracker.projection.lock().await.suppress_history = true;
        let result = self
            .refresh(root, base, token, conn, v2, false, false)
            .await;
        tracker.projection.lock().await.suppress_history = false;
        result
    }
    pub async fn cancel_waited(
        &self,
        root: &str,
        base: &str,
        token: &str,
        snapshot: &Value,
    ) -> Result<()> {
        let tracker = self.tracker(root)?;
        let mut p = tracker.projection.lock().await;
        p.read_root(base, token).await?;
        let waited = p
            .calls
            .values()
            .filter(|c| {
                c.sender.is_empty()
                    && c.name == "job_block"
                    && !c.finished
                    && snapshot["turn"]["prompt_id"] == c.prompt
            })
            .filter_map(|c| c.input["job_id"].as_str().map(Into::into))
            .collect::<HashSet<String>>();
        drop(p);
        if !waited.is_empty() {
            let current = sync::snapshot(base, token).await?;
            if current["daemon_epoch"] == snapshot["daemon_epoch"]
                && current["turn"]["prompt_id"] == snapshot["turn"]["prompt_id"]
            {
                cancel_jobs(base, token, waited).await?;
            }
        }
        Ok(())
    }
    fn tracker(&self, root: &str) -> Result<Arc<Tracker>> {
        let mut roots = self.roots.lock().unwrap();
        if let Some(tracker) = roots.get(root) {
            return Ok(tracker.clone());
        }
        let saved = store::root().join("children").join(format!("{root}.json"));
        let mut projection: Projection = match std::fs::read(saved) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Projection::default(),
            Err(e) => return Err(e.into()),
        };
        projection.root = root.into();
        projection.grant_epoch = rand::random();
        for node in projection.nodes.values_mut() {
            // A previous connection's observation never grants live authority.
            if node.state["state"] != "idle" {
                node.state = json!({"state":"unknown"});
            }
        }
        let tracker = Arc::new(Tracker {
            projection: tokio::sync::Mutex::new(projection),
            stop: CancellationToken::new(),
        });
        roots.insert(root.into(), tracker.clone());
        Ok(tracker)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn refresh(
        &self,
        root: &str,
        base: &str,
        token: &str,
        conn: &ConnectionTo<Client>,
        v2: bool,
        replay: bool,
        live: bool,
    ) -> Result<()> {
        let tracker = self.tracker(root)?;
        let mut p = tracker.projection.lock().await;
        if replay {
            p.sent.clear();
            p.current.clear();
            p.announced.clear();
        }
        p.grants.clear();
        if !live {
            p.grant_epoch = p.grant_epoch.wrapping_add(1);
        }
        p.epoch = sync::snapshot(base, token).await?["daemon_epoch"]
            .as_str()
            .map(Into::into);
        let jobs = sync::get(base, token, "/jobs").await?;
        p.read_root(base, token).await?;
        // Parent edges from the live job catalog are authoritative. Absence of
        // an edge is NOT evidence that a worker belongs directly to the root.
        for job in jobs.as_array().context("Invalid worker catalog")? {
            if job["parent"]["type"] == "subagent"
                && let (Some(handle), Some(parent)) =
                    (job["handle"].as_str(), job["parent"]["handle"].as_str())
                && let Some(node) = p
                    .nodes
                    .get_mut(handle)
                    .filter(|n| !n.ambiguous && n.parent.is_none())
            {
                node.parent = Some(parent.into());
            }
        }
        let handles = p.nodes.keys().cloned().collect::<Vec<_>>();
        for handle in &handles {
            if p.nodes[handle].ambiguous {
                continue;
            }
            if let Err(error) = p.read_child(base, token, handle).await {
                let node = p.nodes.get_mut(handle).unwrap();
                node.incomplete = true;
                tracing::debug!(%handle, %error, "Child history is incomplete");
            }
        }
        p.bind();
        let spawning = p
            .calls
            .values()
            .any(|c| c.name == "subagent" && !c.finished);
        if !spawning {
            for n in p.nodes.values_mut() {
                if n.parent.is_none() {
                    n.fallback = true;
                    n.incomplete = true;
                }
            }
        }
        loop {
            let fallbacks = p.legacy.clone();
            let mut added = false;
            let mut reserve = vec![];
            for (handle, n) in &mut p.nodes {
                if n.parent
                    .as_ref()
                    .is_some_and(|parent| fallbacks.contains(parent))
                    && !n.fallback
                {
                    n.fallback = true;
                    added = true;
                }
                if n.fallback {
                    reserve.push(handle.clone());
                }
            }
            p.legacy.extend(reserve);
            if !added {
                break;
            }
        }
        p.grants.clear();
        for handle in &handles {
            let job = jobs
                .as_array()
                .unwrap()
                .iter()
                .find(|j| j["handle"] == *handle && j["kind"] == "subagent");
            let node = p.nodes.get_mut(handle).unwrap();
            if node.ambiguous {
                node.state = json!({"state":"unknown"});
                continue;
            }
            let result = if job.is_some_and(|j| j["status"]["status"] == "completed") {
                sync::get(base, token, &path("job", handle, "result"))
                    .await
                    .ok()
            } else {
                None
            };
            node.state = work_state(job, result.as_ref(), node.outcome.as_ref());
            if node.state["state"] == "idle" {
                node.last_stop = Some(node.state.clone());
            }
            if job.is_some_and(|j| j["status"]["status"] == "completed") && result.is_none() {
                node.incomplete = true;
            }
            if let Some(model) = job.and_then(|j| j["model"].as_str()) {
                node.model = Some(model.into());
            }
            if live && !node.fallback && node.cwd.is_some() && job.is_some_and(active) {
                p.grants.insert(handle.clone());
            }
        }
        // This is a narrowly scoped legacy endpoint: only pending questions,
        // including their origin, are consumed. Historical questions are never
        // reconstructed or approved from transcript data.
        let pending = if live {
            sync::get(base, token, "/state").await.ok()
        } else {
            None
        };
        if let Some(pending) = &pending {
            for event in pending["pending_interrogatives"]
                .as_array()
                .into_iter()
                .flatten()
            {
                if let Some(node) = event["subagent_handle"]
                    .as_str()
                    .and_then(|h| p.nodes.get_mut(h))
                    .filter(|n| !n.ambiguous && !n.fallback)
                {
                    node.state = json!({"state":"requires_action"});
                }
            }
        }
        p.publish(conn, v2, replay, live)?;
        p.save()?;
        Ok(())
    }

    async fn questions_v1(
        &self,
        root: &str,
        base: &str,
        token: &str,
        conn: &ConnectionTo<Client>,
        features: &agent::ClientFeatureSupport,
    ) -> Result<()> {
        let state = sync::get(base, token, "/state").await?;
        for event in state["pending_interrogatives"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let Some(h) = event["subagent_handle"].as_str() else {
                continue;
            };
            let Some(qid) = event["interrogative_id"].as_str() else {
                continue;
            };
            let tracker = self.tracker(root)?;
            if tracker.projection.lock().await.questions.contains(qid) {
                continue;
            }
            // A question can be newer than our preceding history capture.
            // Refresh its workspace before sending child-addressed controls.
            self.refresh(root, base, token, conn, false, false, true)
                .await?;
            let Some((sid, cwd)) = self.route(root, h).await else {
                continue;
            };
            if tracker.projection.lock().await.questions.insert(qid.into()) {
                agent::forward_v2_interrogative(conn, event, &sid, base, token, &cwd, features)
                    .await;
            }
        }
        Ok(())
    }

    pub async fn represented(&self, root: &str, handle: &str) -> bool {
        let tracker = self.roots.lock().unwrap().get(root).cloned();
        if let Some(t) = tracker {
            let p = t.projection.lock().await;
            p.nodes
                .get(handle)
                .is_some_and(|n| !n.ambiguous && !n.fallback)
        } else {
            false
        }
    }

    /// Once ordinary activity was exposed, preserve that choice. ACP has no
    /// removal operation that could safely replace its existing worker card.
    pub async fn fallback(&self, root: &str, handle: &str) -> bool {
        let Ok(t) = self.tracker(root) else {
            return true;
        };
        let mut p = t.projection.lock().await;
        if p.nodes
            .get(handle)
            .is_some_and(|n| !n.ambiguous && !n.fallback)
        {
            return false;
        }
        if p.legacy.insert(handle.into())
            && let Err(error) = p.save()
        {
            tracing::warn!(%error,"Unable to retain worker fallback choice");
        }
        true
    }

    pub async fn rescan(&self, root: &str) {
        if let Ok(t) = self.tracker(root) {
            let mut p = t.projection.lock().await;
            p.offset = 0;
            p.grants.clear();
            p.grant_epoch = p.grant_epoch.wrapping_add(1);
        }
    }

    pub async fn unobservable(&self, root: &str, conn: &ConnectionTo<Client>, v2: bool) {
        if let Ok(t) = self.tracker(root) {
            let mut p = t.projection.lock().await;
            p.grants.clear();
            p.grant_epoch = p.grant_epoch.wrapping_add(1);
            for n in p.nodes.values_mut() {
                n.state = json!({"state":"unknown"});
                n.incomplete = true;
            }
            let _ = p.publish(conn, v2, false, false);
        }
    }

    pub async fn route(&self, root: &str, handle: &str) -> Option<(String, PathBuf)> {
        let tracker = self.roots.lock().unwrap().get(root).cloned()?;
        let p = tracker.projection.lock().await;
        let n = p.nodes.get(handle)?;
        if n.ambiguous || n.fallback {
            return None;
        }
        p.announced
            .contains(&n.id)
            .then(|| Some((n.id.clone(), n.cwd.clone()?)))?
    }

    pub fn child_id(id: &str) -> bool {
        id.starts_with("pt-child:")
    }

    pub async fn cancel(&self, id: &str, base: &str, token: &str, grant: u64) -> Result<bool> {
        let trackers = self
            .roots
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for tracker in trackers {
            let p = tracker.projection.lock().await;
            let Some((handle, _)) = p.nodes.iter().find(|(_, n)| n.id == id) else {
                continue;
            };
            if !p.grants.contains(handle) || p.grant_epoch != grant {
                bail!("Child cancellation is not currently available");
            }
            let current = sync::snapshot(base, token).await?;
            if current["daemon_epoch"].as_str() != p.epoch.as_deref() {
                bail!("Worker cancellation grant has expired");
            }
            cancel_jobs(base, token, HashSet::from([handle.clone()])).await?;
            // Acknowledgement alone never changes displayed state.
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn permit(&self, id: &str) -> Option<(String, u64)> {
        let trackers = self
            .roots
            .lock()
            .unwrap()
            .iter()
            .map(|(r, t)| (r.clone(), t.clone()))
            .collect::<Vec<_>>();
        for (root, t) in trackers {
            let p = t.projection.lock().await;
            if p.nodes
                .iter()
                .any(|(h, n)| n.id == id && p.grants.contains(h))
            {
                return Some((root, p.grant_epoch));
            }
        }
        None
    }

    pub fn close(&self, root: &str) {
        if let Some(t) = self.roots.lock().unwrap().remove(root) {
            t.stop.cancel();
        }
    }

    /// V1 otherwise stops observing when its foreground prompt finishes.
    pub fn observe_v1(
        self: &Arc<Self>,
        root: String,
        base: String,
        token: String,
        conn: ConnectionTo<Client>,
        features: agent::ClientFeatureSupport,
    ) {
        let hub = self.clone();
        let Ok(tracker) = self.tracker(&root) else {
            return;
        };
        let stop = tracker.stop.clone();
        tokio::spawn(async move {
            let observe = async {
                loop {
                    let result: Result<()> = async {
                        let snapshot = sync::snapshot(&base, &token).await?;
                        hub.refresh(&root, &base, &token, &conn, false, false, true)
                            .await?;
                        hub.questions_v1(&root, &base, &token, &conn, &features)
                            .await?;
                        let response = reqwest::Client::new()
                            .get(format!("{base}/events"))
                            .query(&[(
                                "after",
                                snapshot["cursor"].as_str().context("Missing cursor")?,
                            )])
                            .bearer_auth(&token)
                            .send()
                            .await?
                            .error_for_status()?;
                        let mut stream = response.bytes_stream();
                        let mut parser = agent::SseParser::new();
                        while let Some(chunk) = stream.next().await {
                            let mut dirty = false;
                            for line in parser.feed(&chunk?) {
                                let e: Value = serde_json::from_str(&line)?;
                                if e["session_id"] != root {
                                    bail!("Worker stream session mismatch");
                                }
                                if matches!(
                                    e["event"]["record"]["type"].as_str(),
                                    Some("history_rewritten" | "reset_required" | "epoch_started")
                                ) {
                                    hub.rescan(&root).await;
                                    dirty = true;
                                }
                                dirty |= matches!(
                                    e["event"]["record"]["type"].as_str(),
                                    Some(
                                        "history_appended"
                                            | "jobs_changed"
                                            | "subagent_changed"
                                            | "interrogative_registered"
                                            | "interrogative_resolved"
                                            | "reset_required"
                                            | "epoch_started"
                                    )
                                );
                            }
                            if dirty {
                                hub.refresh(&root, &base, &token, &conn, false, false, true)
                                    .await?;
                                hub.questions_v1(&root, &base, &token, &conn, &features)
                                    .await?;
                            }
                        }
                        bail!("Worker event stream ended")
                    }
                    .await;
                    if let Err(error) = result {
                        tracing::debug!(%error, "Reconnecting worker observer");
                        let mut p = tracker.projection.lock().await;
                        p.grants.clear();
                        p.grant_epoch = p.grant_epoch.wrapping_add(1);
                        for n in p.nodes.values_mut() {
                            n.state = json!({"state":"unknown"});
                        }
                        let _ = p.publish(&conn, false, false, false);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            };
            tokio::select! { _=stop.cancelled()=>{}, _=observe=>{} }
        });
    }
}

impl Projection {
    fn save(&self) -> Result<()> {
        let directory = store::root().join("children");
        std::fs::create_dir_all(&directory)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
        }
        let mut file = tempfile::NamedTempFile::new_in(&directory)?;
        serde_json::to_writer(&mut file, self)?;
        file.persist(directory.join(format!("{}.json", self.root)))?;
        Ok(())
    }

    async fn read_root(&mut self, base: &str, token: &str) -> Result<()> {
        loop {
            let page = sync::get(
                base,
                token,
                &format!("/history?offset={}&limit=256", self.offset),
            )
            .await?;
            if page["session_id"] != self.root {
                bail!("Worker history belongs to another session");
            }
            if page["offset"].as_u64() != Some(self.offset as u64) {
                bail!("Worker launch history offset mismatch");
            }
            let items = page["items"].as_array().context("Invalid launch history")?;
            for item in items {
                if item["type"] == "subagent_spawn_v2"
                    && let (Some(h), Some(birth), Some(cwd)) = (
                        item["handle"].as_str(),
                        item["emitted_at"].as_str(),
                        item["launch"]["cwd"].as_str(),
                    )
                    && Path::new(cwd).is_absolute()
                    && !self.nodes.contains_key(h)
                {
                    let id = format!(
                        "pt-child:{}",
                        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&(&self.root, h, birth))?)
                    );
                    self.nodes.insert(
                        h.into(),
                        Node {
                            id,
                            birth: birth.into(),
                            parent: None,
                            title: item["subagent_type"].as_str().unwrap_or("Worker").into(),
                            model: item["model"].as_str().map(Into::into),
                            cwd: Some(cwd.into()),
                            launch_cwd: cwd.into(),
                            directories: vec![],
                            history: vec![],
                            state: json!({"state":"unknown"}),
                            last_stop: None,
                            outcome: None,
                            incomplete: false,
                            ambiguous: false,
                            fallback: self.legacy.contains(h),
                            todos: BTreeMap::new(),
                            initial_message: None,
                            cursor: 0,
                            anchor: None,
                        },
                    );
                }
                if item["type"] == "subagent_spawn_v2"
                    && let Some(n) = item["handle"].as_str().and_then(|h| self.nodes.get_mut(h))
                    && item["emitted_at"]
                        .as_str()
                        .is_some_and(|birth| birth != n.birth)
                {
                    n.ambiguous = true;
                    n.incomplete = true;
                }
                self.observe_call("", item);
                if item["type"] == "subagent_complete_v1"
                    && let Some(n) = item["handle"]
                        .as_str()
                        .and_then(|h| self.nodes.get_mut(h))
                        .filter(|n| !n.ambiguous)
                {
                    n.outcome = Some(
                        json!({"kind":item["outcome"]["kind"],"message":item["outcome"]["message"]}),
                    );
                    // This is a retained stop event, not a fresh job snapshot.
                    // Preserve it for replay even on the first shim attachment.
                    n.last_stop = Some(work_state(
                        Some(&json!({"status":{"status":"completed"}})),
                        None,
                        n.outcome.as_ref(),
                    ));
                }
            }
            self.offset += items.len();
            if items.is_empty()
                || self.offset
                    >= page["total_projected_items"]
                        .as_u64()
                        .context("Missing history total")? as usize
            {
                break;
            }
        }
        Ok(())
    }

    fn observe_call(&mut self, sender: &str, item: &Value) {
        if item["type"] == "assistant" {
            for block in item["blocks"].as_array().into_iter().flatten() {
                if block["type"] == "tool_use"
                    && matches!(
                        block["name"].as_str(),
                        Some("subagent" | "message_subagent" | "job_block")
                    )
                    && let Some(id) = block["id"].as_str()
                {
                    let key = format!("{sender}:{}:{id}", item["prompt_id"]);
                    self.calls.entry(key).or_insert(Call {
                        sender: sender.into(),
                        id: id.into(),
                        name: block["name"].as_str().unwrap().into(),
                        input: block["input"].clone(),
                        recipients: vec![],
                        delivered: false,
                        prompt: item["prompt_id"].as_str().unwrap_or("").into(),
                        finished: false,
                    });
                }
            }
        }
        if item["type"] == "tool_result" {
            let results = item["results"]
                .as_array()
                .cloned()
                .unwrap_or_else(|| vec![item.clone()]);
            for r in results {
                let id = r["tool_use_id"]
                    .as_str()
                    .or_else(|| r["call_id"].as_str())
                    .unwrap_or("");
                let key = format!("{sender}:{}:{id}", item["prompt_id"]);
                if let Some(call) = self.calls.get_mut(&key) {
                    call.finished = true;
                    if r["is_error"] != false {
                        continue;
                    }
                    if call.name == "subagent" {
                        if let Some(text) = crate::tool_output::text(&r["content"])
                            && let Ok(result) = serde_json::from_str::<Value>(&text)
                        {
                            call.recipients = result["handles"]
                                .as_array()
                                .into_iter()
                                .flatten()
                                .filter_map(|h| h.as_str().map(Into::into))
                                .collect();
                            call.delivered = !call.recipients.is_empty();
                        }
                    } else if call.name == "message_subagent"
                        && let Some(h) = call.input["job_id"].as_str()
                    {
                        call.recipients = vec![h.into()];
                        call.delivered = true;
                    }
                }
            }
        }
    }

    async fn read_child(&mut self, base: &str, token: &str, handle: &str) -> Result<()> {
        loop {
            let n = self.nodes.get(handle).unwrap();
            let offset = n.cursor.saturating_sub(1);
            let url = format!(
                "{}?offset={offset}&limit=128",
                path("subagent", handle, "history")
            );
            let page = sync::get(base, token, &url).await?;
            let items = page.as_array().context("Invalid child history")?;
            if n.cursor > 0 && items.first() != n.anchor.as_ref() {
                bail!("Child history changed or was truncated");
            }
            if n.cursor == 0
                && n.initial_message.is_some()
                && n.initial_message.as_ref() != items.first().map(initial_message).as_ref()
            {
                bail!("Retained child history belongs to a different conversation");
            }
            let skip = usize::from(n.cursor > 0);
            if n.cursor == 0 {
                let n = self.nodes.get_mut(handle).unwrap();
                n.history.clear();
                n.directories.clear();
                n.todos.clear();
                n.cwd = Some(n.launch_cwd.clone());
                n.initial_message = items.first().map(initial_message);
            }
            for (i, item) in items.iter().enumerate().skip(skip) {
                self.observe_call(handle, item);
                let n = self.nodes.get_mut(handle).unwrap();
                n.absorb(item, offset + i);
            }
            let n = self.nodes.get_mut(handle).unwrap();
            n.cursor = offset + items.len();
            n.anchor = items.last().cloned();
            if items.len() < 128 {
                break;
            }
        }
        Ok(())
    }

    fn bind(&mut self) {
        for call in self
            .calls
            .values()
            .filter(|c| c.delivered && c.name == "subagent")
        {
            for h in &call.recipients {
                if let Some(n) = self
                    .nodes
                    .get_mut(h)
                    .filter(|n| !n.ambiguous && n.parent.is_none())
                {
                    n.parent = Some(call.sender.clone());
                }
            }
        }
    }

    fn order(&self) -> Vec<String> {
        let mut ordered = vec![];
        let mut ready = HashSet::from([String::new()]);
        loop {
            let before = ready.len();
            for (h, n) in &self.nodes {
                if !ready.contains(h)
                    && !n.fallback
                    && (n.cwd.is_some() || self.announced.contains(&n.id))
                    && n.parent.as_ref().is_some_and(|p| ready.contains(p))
                {
                    ready.insert(h.clone());
                    ordered.push(h.clone());
                }
            }
            if before == ready.len() {
                break;
            }
        }
        ordered
    }

    fn publish(
        &mut self,
        conn: &ConnectionTo<Client>,
        v2: bool,
        replay: bool,
        live: bool,
    ) -> Result<()> {
        let order = self.order();
        for h in &order {
            let n = &self.nodes[h];
            let parent = n.parent.as_ref().unwrap();
            let parent = if parent.is_empty() {
                &self.root
            } else {
                &self.nodes[parent].id
            };
            let state = if replay && n.state["state"] == "unknown" && n.last_stop.is_some() {
                n.last_stop.clone().unwrap()
            } else if n.cwd.is_some() {
                n.state.clone()
            } else {
                json!({"state":"unknown"})
            };
            let title = if n.incomplete {
                format!("{} (history incomplete)", n.title)
            } else {
                n.title.clone()
            };
            let mut update = json!({"sessionUpdate":"subagent_update","sessionId":n.id,"title":title,"additionalDirectories":[],"capabilities":if live && self.grants.contains(h) {json!({"cancel":{}})}else{json!({})},"state":state,"_meta":{"polytoken":{"historyIncomplete":n.incomplete}}});
            if let Some(cwd) = &n.cwd {
                update["cwd"] = json!(cwd);
            }
            emit_current(&mut self.current, conn, v2, parent, update)?;
            self.announced.insert(n.id.clone());
            if v2 {
                let mut state = state;
                state["sessionUpdate"] = json!("state_update");
                emit_current(&mut self.current, conn, v2, &n.id, state)?;
            }
        }
        // All referenced participants now exist, even for cross-worker messages.
        for h in &order {
            let n = &self.nodes[h];
            if n.incomplete && v2 {
                emit_once(
                    &mut self.sent,
                    conn,
                    v2,
                    &n.id,
                    json!({"sessionUpdate":"notice","severity":"warning","title":"Worker history is incomplete","description":"Only observed, retained activity is shown. Missing activity and outcomes have not been reconstructed."}),
                )?;
            }
            if let Some(model) = &n.model {
                emit_current(
                    &mut self.current,
                    conn,
                    v2,
                    &n.id,
                    json!({"sessionUpdate":"config_option_update","configOptions":[{"id":"model","name":"Model","category":"model","type":"select","currentValue":model,"options":[{"value":model,"name":model}]}]}),
                )?;
            }
            for u in &n.history {
                let mut u = u.clone();
                if u["sessionUpdate"] == "session_message" {
                    u["recipientSessionId"] = json!(n.id);
                    // Initial assignment sender follows the proven spawn edge.
                    // Later user-shaped entries lack sender identity in this API.
                    if u["_meta"]["polytoken"]["historyIndex"] == 0 {
                        u["senderSessionId"] = if n.parent.as_deref() == Some("") {
                            json!(self.root)
                        } else {
                            json!(self.nodes[n.parent.as_ref().unwrap()].id)
                        };
                    }
                }
                emit_history(
                    &mut self.sent,
                    conn,
                    v2,
                    &n.id,
                    versioned(u, v2),
                    self.suppress_history,
                )?;
            }
        }
        for call in self
            .calls
            .values()
            .filter(|c| c.delivered && matches!(c.name.as_str(), "subagent" | "message_subagent"))
        {
            let sender = if call.sender.is_empty() {
                &self.root
            } else if let Some(n) = self
                .nodes
                .get(&call.sender)
                .filter(|n| !n.ambiguous && !n.fallback && self.announced.contains(&n.id))
            {
                &n.id
            } else {
                continue;
            };
            for h in &call.recipients {
                let Some(n) = self
                    .nodes
                    .get(h)
                    .filter(|n| !n.ambiguous && !n.fallback && self.announced.contains(&n.id))
                else {
                    continue;
                };
                let text = call.input[if call.name == "subagent" {
                    "prompt"
                } else {
                    "message"
                }]
                .as_str()
                .unwrap_or("");
                emit_history(
                    &mut self.sent,
                    conn,
                    v2,
                    sender,
                    json!({"sessionUpdate":"session_message","messageId":format!("{}:to:{}",call.id,n.id),"senderSessionId":sender,"recipientSessionId":n.id,"content":[{"type":"text","text":text}]}),
                    self.suppress_history,
                )?;
            }
        }
        if replay && v2 {
            for h in order.into_iter().rev() {
                emit(
                    conn,
                    true,
                    &self.nodes[&h].id,
                    json!({"sessionUpdate":"replay_complete"}),
                )?;
            }
        }
        Ok(())
    }
}

impl Node {
    fn absorb(&mut self, item: &Value, index: usize) {
        if item["type"] == "context_cleared" {
            self.cwd = Some(self.launch_cwd.clone());
            self.directories.clear();
        }
        if item["type"] == "state_update" {
            let delta = &item["delta"];
            match delta["type"].as_str() {
                Some("pushd") => {
                    if let Some(path) = delta["path"]
                        .as_str()
                        .filter(|p| Path::new(p).is_absolute())
                    {
                        if let Some(old) = self.cwd.replace(path.into()) {
                            self.directories.push(old);
                        }
                    } else {
                        self.cwd = None;
                        self.incomplete = true;
                    }
                }
                Some("popd") => {
                    self.cwd = self.directories.pop();
                    if self.cwd.is_none() {
                        self.incomplete = true;
                    }
                }
                Some("todo_create" | "todo_update" | "todo_complete" | "todo_deleted") => {
                    let id = delta["id"].to_string();
                    match delta["type"].as_str().unwrap() {
                        "todo_create" => {
                            self.todos.insert(id,json!({"content":delta["title"],"status":"pending","priority":"medium"}));
                        }
                        "todo_deleted" => {
                            self.todos.remove(&id);
                        }
                        kind => {
                            if let Some(todo) = self.todos.get_mut(&id) {
                                if delta["title"].is_string() {
                                    todo["content"] = delta["title"].clone();
                                }
                                if kind == "todo_complete" {
                                    todo["status"] = json!("completed");
                                } else if let Some(status) = delta["status"].as_str() {
                                    todo["status"] = json!(match status {
                                        "done" => "completed",
                                        "in_progress" => "in_progress",
                                        _ => "pending",
                                    });
                                }
                            } else {
                                self.incomplete = true;
                            }
                        }
                    }
                    self.history.push(json!({"sessionUpdate":"plan_update","plan":{"type":"items","planId":"todos","entries":self.todos.values().collect::<Vec<_>>()},"_meta":{"polytoken":{"historyIndex":index}}}));
                }
                _ => {}
            }
        }
        self.history.extend(
            transcript(item, index)
                .into_iter()
                .filter(|u| self.cwd.is_some() || u["sessionUpdate"] != "tool_call_update"),
        );
    }
}

fn transcript(item: &Value, index: usize) -> Vec<Value> {
    let mut out = vec![];
    let pid = item["prompt_id"].as_str().unwrap_or("");
    match item["type"].as_str() {
        Some("user") => {
            if let Some(text) = item["content"].as_str() {
                out.push(json!({"sessionUpdate":"session_message","messageId":format!("{pid}:in:{index}"),"content":[{"type":"text","text":text}],"_meta":{"polytoken":{"historyIndex":index}}}));
            }
        }
        Some("assistant") => {
            for (b, block) in item["blocks"].as_array().into_iter().flatten().enumerate() {
                match block["type"].as_str() {
                    Some("text") => if let Some(text) = block["text"].as_str() { out.push(json!({"sessionUpdate":"agent_message","messageId":format!("{pid}:{index}:{b}"),"content":[{"type":"text","text":text}]})); },
                    Some("tool_use") if block["id"].is_string() && block["name"].is_string() => out.push(json!({"sessionUpdate":"tool_call_update","toolCallId":block["id"],"title":block["name"],"kind":"other","status":"in_progress","rawInput":block["input"]})),
                    _ => {},
                }
            }
        }
        Some("tool_result") => {
            for r in item["results"].as_array().into_iter().flatten() {
                let Some(id) = r["tool_use_id"].as_str() else {
                    continue;
                };
                let text = crate::tool_output::text(&r["content"]).unwrap_or_default();
                out.push(json!({"sessionUpdate":"tool_call_update","toolCallId":id,"status":if r["is_error"] == true {"failed"}else{"completed"},"content":[{"type":"content","content":{"type":"text","text":text}}]}));
            }
        }
        _ => {}
    }
    out
}

fn initial_message(item: &Value) -> Value {
    json!({"type":item["type"],"prompt_id":item["prompt_id"],"content":item["content"],"emitted_at":item["emitted_at"]})
}

fn work_state(job: Option<&Value>, result: Option<&Value>, outcome: Option<&Value>) -> Value {
    let Some(job) = job else {
        return json!({"state":"unknown"});
    };
    match job["status"]["status"].as_str() {
        Some("running" | "queued" | "reserved") => json!({"state":"running"}),
        Some("cancelled") => json!({"state":"idle","stopReason":"cancelled"}),
        Some("failed") => {
            json!({"state":"idle","stopReason":"error","error":{"code":-32603,"message":job["status"]["error"].as_str().unwrap_or("Worker failed")}})
        }
        Some("completed") => match outcome.and_then(|o| o["kind"].as_str()) {
            Some("success") => json!({"state":"idle","stopReason":"end_turn"}),
            Some("cancelled") => json!({"state":"idle","stopReason":"cancelled"}),
            Some("failure") => {
                json!({"state":"idle","stopReason":"error","error":{"code":-32603,"message":outcome.and_then(|o|o["message"].as_str()).unwrap_or("Worker failed")}})
            }
            Some(kind) => json!({"state":"idle","_meta":{"polytoken":{"outcomeKind":kind}}}),
            None if result.is_some_and(|r| r["is_error"] == true) => {
                json!({"state":"idle","stopReason":"error","error":{"code":-32603,"message":"Worker returned an error"}})
            }
            None => json!({"state":"idle"}),
        },
        _ => json!({"state":"unknown"}),
    }
}

fn versioned(mut update: Value, v2: bool) -> Value {
    if !v2 && update["sessionUpdate"] == "plan_update" {
        update = json!({"sessionUpdate":"plan","entries":update["plan"]["entries"],"_meta":update["_meta"]});
    }
    if !v2 && update["sessionUpdate"] == "agent_message" {
        update["sessionUpdate"] = json!("agent_message_chunk");
        update["content"] = update["content"][0].clone();
    }
    if !v2 && update["sessionUpdate"] == "tool_call_update" && update["title"].is_string() {
        update["sessionUpdate"] = json!("tool_call");
    }
    update
}

fn emit_once(
    sent: &mut HashSet<String>,
    conn: &ConnectionTo<Client>,
    v2: bool,
    sid: &str,
    update: Value,
) -> Result<()> {
    let key = format!("{sid}:{update}");
    if !sent.contains(&key) {
        emit(conn, v2, sid, update)?;
        sent.insert(key);
    }
    Ok(())
}

fn emit_history(
    sent: &mut HashSet<String>,
    conn: &ConnectionTo<Client>,
    v2: bool,
    sid: &str,
    update: Value,
    suppress: bool,
) -> Result<()> {
    if suppress {
        sent.insert(format!("{sid}:{update}"));
        Ok(())
    } else {
        emit_once(sent, conn, v2, sid, update)
    }
}

fn emit_current(
    current: &mut HashMap<String, Value>,
    conn: &ConnectionTo<Client>,
    v2: bool,
    sid: &str,
    update: Value,
) -> Result<()> {
    let key = format!("{sid}:{}:{}", update["sessionUpdate"], update["sessionId"]);
    if current.get(&key) != Some(&update) {
        emit(conn, v2, sid, update.clone())?;
        current.insert(key, update);
    }
    Ok(())
}

fn emit(conn: &ConnectionTo<Client>, v2: bool, sid: &str, update: Value) -> Result<()> {
    if v2 {
        let typed: agent_client_protocol::v2::schema::SessionUpdate =
            serde_json::from_value(update)?;
        agent::send_ext_notification(
            conn,
            "_polytoken/v2_update",
            &json!({"sessionId":sid,"update":typed}),
        );
    } else {
        let typed: v1::SessionUpdate = serde_json::from_value(update)?;
        conn.send_notification(v1::SessionNotification::new(sid.to_string(), typed))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, parent: Option<&str>) -> Node {
        Node {
            id: id.into(),
            birth: "launch-one".into(),
            parent: parent.map(Into::into),
            title: "worker".into(),
            model: Some("child-model".into()),
            cwd: Some(std::env::current_dir().unwrap()),
            launch_cwd: std::env::current_dir().unwrap(),
            directories: vec![],
            history: vec![],
            state: json!({"state":"running"}),
            last_stop: None,
            outcome: None,
            incomplete: false,
            ambiguous: false,
            fallback: false,
            todos: BTreeMap::new(),
            initial_message: None,
            cursor: 0,
            anchor: None,
        }
    }

    #[test]
    fn descendants_wait_for_proven_ancestors_and_cycles_never_become_roots() {
        let mut p = Projection::default();
        p.nodes
            .insert("leaf".into(), node("child-leaf", Some("parent")));
        assert!(p.order().is_empty());
        p.nodes.insert("parent".into(), node("child-parent", None));
        assert!(p.order().is_empty());
        p.nodes.get_mut("parent").unwrap().parent = Some("".into());
        assert_eq!(p.order(), ["parent", "leaf"]);
        p.nodes.get_mut("parent").unwrap().fallback = true;
        assert!(p.order().is_empty());
        p.nodes.get_mut("parent").unwrap().fallback = false;
        p.nodes.get_mut("parent").unwrap().parent = Some("leaf".into());
        assert!(p.order().is_empty());
    }

    #[test]
    fn cancellation_subtrees_exclude_siblings_and_include_owned_resources() {
        let jobs = vec![
            json!({"handle":"parent"}),
            json!({"handle":"sibling"}),
            json!({"handle":"child","parent":{"type":"subagent","handle":"parent"}}),
            json!({"handle":"shell","parent":{"type":"subagent","handle":"child"}}),
        ];
        assert_eq!(
            descendants(&jobs, HashSet::from(["parent".into()])),
            HashSet::from(["parent".into(), "child".into(), "shell".into()])
        );
    }

    #[test]
    fn only_successful_observed_spawn_results_establish_parentage() {
        let mut p = Projection::default();
        p.nodes.insert("leaf".into(), node("child-leaf", None));
        p.observe_call("parent",&json!({"type":"assistant","prompt_id":"p","blocks":[{"type":"tool_use","id":"spawn","name":"subagent","input":{"prompt":"assignment"}}]}));
        p.bind();
        assert!(p.nodes["leaf"].parent.is_none());
        p.observe_call("parent",&json!({"type":"tool_result","prompt_id":"p","results":[{"tool_use_id":"spawn","is_error":true,"content":{"text":"{\"handles\":[\"leaf\"]}"}}]}));
        p.bind();
        assert!(p.nodes["leaf"].parent.is_none());
        p.observe_call("parent",&json!({"type":"tool_result","prompt_id":"p","results":[{"tool_use_id":"spawn","is_error":false,"content":{"text":"{\"handles\":[\"leaf\"]}"}}]}));
        p.bind();
        assert_eq!(p.nodes["leaf"].parent.as_deref(), Some("parent"));
    }

    #[test]
    fn lost_observability_and_missing_results_do_not_invent_outcomes() {
        assert_eq!(work_state(None, None, None), json!({"state":"unknown"}));
        let completed = json!({"status":{"status":"completed"}});
        assert_eq!(
            work_state(Some(&completed), None, None),
            json!({"state":"idle"})
        );
        assert_eq!(
            work_state(Some(&completed), Some(&json!({"is_error":false})), None),
            json!({"state":"idle"})
        );
        assert_eq!(
            work_state(
                Some(&completed),
                Some(&json!({"is_error":false})),
                Some(&json!({"kind":"success"}))
            )["stopReason"],
            "end_turn"
        );
        assert_eq!(
            work_state(Some(&completed), Some(&json!({"is_error":true})), None)["stopReason"],
            "error"
        );
        for (status, reason) in [("failed", "error"), ("cancelled", "cancelled")] {
            let state = work_state(Some(&json!({"status":{"status":status}})), None, None);
            assert_eq!(state["state"], "idle");
            assert_eq!(state["stopReason"], reason);
            let mut update = state;
            update["sessionUpdate"] = json!("state_update");
            serde_json::from_value::<agent_client_protocol::v2::schema::SessionUpdate>(update)
                .unwrap();
        }
        for kind in ["salvaged", "unknown"] {
            let state = work_state(
                Some(&completed),
                Some(&json!({"is_error":false})),
                Some(&json!({"kind":kind})),
            );
            assert!(state["stopReason"].is_null());
            assert_eq!(state["_meta"]["polytoken"]["outcomeKind"], kind);
        }
    }

    #[test]
    fn private_metadata_restores_identity_without_live_authority() {
        let mut p = Projection::default();
        p.nodes.insert("worker".into(), node("stable-id", Some("")));
        p.grants.insert("worker".into());
        p.announced.insert("stable-id".into());
        p.epoch = Some("previous-daemon".into());
        p.legacy.insert("fallback-worker".into());
        p.questions.insert("old-approval".into());
        let restored: Projection =
            serde_json::from_slice(&serde_json::to_vec(&p).unwrap()).unwrap();
        assert_eq!(restored.nodes["worker"].id, "stable-id");
        assert!(restored.legacy.contains("fallback-worker"));
        assert!(
            restored.grants.is_empty()
                && restored.announced.is_empty()
                && restored.questions.is_empty()
                && restored.epoch.is_none()
        );
    }

    #[test]
    fn raw_child_history_is_allowlisted_and_assignments_are_session_messages() {
        let assignment = transcript(
            &json!({"type":"user","prompt_id":"p","content":"assignment","kind":"human","provider_secret":"secret"}),
            0,
        );
        assert_eq!(assignment[0]["sessionUpdate"], "session_message");
        let visible = transcript(
            &json!({"type":"assistant","prompt_id":"p","blocks":[{"type":"thinking","text":"private-thought"},{"type":"open_ai_reasoning_opaque","data":"secret"},{"type":"text","text":"visible"}]}),
            1,
        );
        assert_eq!(visible.len(), 1);
        assert!(!serde_json::to_string(&visible).unwrap().contains("secret"));
        assert!(transcript(&json!({"type":"state_update","delta":{"type":"env_update","env_vars":{"TOKEN":"secret"}}}),2).is_empty());
        for update in assignment.into_iter().chain(visible) {
            serde_json::from_value::<v1::SessionUpdate>(versioned(update.clone(), false)).unwrap();
            serde_json::from_value::<agent_client_protocol::v2::schema::SessionUpdate>(update)
                .unwrap();
        }
    }

    #[test]
    fn workspace_and_plan_deltas_preserve_child_scope() {
        let mut n = node("child", Some(""));
        let original = n.cwd.clone();
        let nested = n.launch_cwd.join("nested");
        n.absorb(
            &json!({"type":"state_update","delta":{"type":"pushd","path":nested}}),
            1,
        );
        assert_eq!(n.cwd, Some(nested));
        n.absorb(&json!({"type":"state_update","delta":{"type":"popd"}}), 2);
        assert_eq!(n.cwd, original);
        n.absorb(
            &json!({"type":"state_update","delta":{"type":"todo_create","id":1,"title":"Review"}}),
            3,
        );
        n.absorb(
            &json!({"type":"state_update","delta":{"type":"todo_complete","id":1}}),
            4,
        );
        assert_eq!(n.todos["1"]["status"], "completed");
        for update in &n.history {
            serde_json::from_value::<v1::SessionUpdate>(versioned(update.clone(), false)).unwrap();
            serde_json::from_value::<agent_client_protocol::v2::schema::SessionUpdate>(
                update.clone(),
            )
            .unwrap();
        }
        n.absorb(
            &json!({"type":"state_update","delta":{"type":"pushd","path":"relative/unknown"}}),
            5,
        );
        assert!(n.cwd.is_none() && n.incomplete);
        n.absorb(&json!({"type":"context_cleared"}), 6);
        assert_eq!(n.cwd, original);
    }

    #[tokio::test]
    async fn incremental_history_deduplicates_overlap_and_preserves_cache_on_gaps() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let first = json!({"type":"user","prompt_id":"child-prompt","content":"assignment","emitted_at":"birth"});
        let text = json!({"type":"assistant","prompt_id":"child-prompt","blocks":[{"type":"text","text":"observed answer"}]});
        let tool = json!({"type":"assistant","prompt_id":"child-prompt","blocks":[{"type":"tool_use","id":"tool","name":"read_file","input":{"path":"file"}}]});
        let pages = vec![
            json!([first, text]),
            json!([text, tool]),
            json!([{"type":"context_cleared"}]),
            json!([{"type":"user","prompt_id":"another-conversation","content":"different"}]),
        ];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut requests = vec![];
            for page in pages {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = [0; 8192];
                let size = socket.read(&mut buffer).await.unwrap();
                requests.push(String::from_utf8_lossy(&buffer[..size]).to_string());
                let body = page.to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
            requests
        });
        let mut p = Projection::default();
        p.nodes.insert("worker".into(), node("stable", Some("")));
        p.read_child(&base, "test-token", "worker").await.unwrap();
        assert_eq!(p.nodes["worker"].history.len(), 2);
        p.read_child(&base, "test-token", "worker").await.unwrap();
        assert_eq!(p.nodes["worker"].history.len(), 3);
        let retained = p.nodes["worker"].history.clone();
        assert!(p.read_child(&base, "test-token", "worker").await.is_err());
        assert_eq!(p.nodes["worker"].history, retained);
        let mut restored: Projection =
            serde_json::from_value(serde_json::to_value(&p).unwrap()).unwrap();
        assert!(
            restored
                .read_child(&base, "test-token", "worker")
                .await
                .is_err()
        );
        assert_eq!(restored.nodes["worker"].history, retained);
        let requests = server.await.unwrap();
        assert!(requests[0].contains("offset=0&limit=128"));
        assert!(requests[1].contains("offset=1&limit=128"));
        assert!(requests.iter().all(|r| {
            r.to_lowercase()
                .contains("authorization: bearer test-token")
        }));
    }
}
