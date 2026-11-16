//! Optional, session-owned plugin-surface transport. HTML never enters prompts.
use crate::{agent, sync};
use agent_client_protocol::{Client, Error, v1::ConnectionTo};
use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

pub const CAPABILITY: &str = "io.github.koalabuttz/polytoken-surfaces";
pub const PREFIX: &str = "_io.github.koalabuttz/polytoken-surfaces/";
pub const MAX_BODY: usize = 1024 * 1024;
pub const MAX_HTML: usize = 256 * 1024;
pub const CHUNK: usize = 16384;
const WINDOW: usize = 4;
const MAX_REQUESTS: usize = 4096;
const ACK_TIMEOUT: Duration = Duration::from_secs(30);

pub fn offered(meta: Option<&Value>) -> bool {
    meta.and_then(|m| m[CAPABILITY]["versions"].as_array())
        .is_some_and(|a| a.iter().any(|v| v.as_u64() == Some(1)))
}
pub fn capability() -> Value {
    json!({"version":1,"limits":{"requestBytes":MAX_BODY,"htmlBytes":MAX_HTML,"chunkBytes":CHUNK,"initialCredits":WINDOW,"activePerSession":8,"activePerConnection":32,"requestIdsPerSubscription":MAX_REQUESTS,"ackTimeoutMs":30000},
        "generationAtomicDispatch":false,"bodyRevisionVerified":false,"httpStreamReplay":false})
}
pub fn method(name: &str) -> bool {
    matches!(
        name.strip_prefix(PREFIX),
        Some("subscribe" | "unsubscribe" | "getHtml" | "request" | "ack" | "cancel")
    )
}
fn invalid(e: impl ToString) -> Error {
    Error::invalid_params().data(e.to_string())
}
fn fault(code: i32, message: &str) -> Error {
    Error::new(code, message)
}
fn failed(e: impl ToString) -> Error {
    tracing::debug!(error=%e.to_string(),"Surface transport failed");
    fault(-32074, "Surface transport failed")
}
fn keys(params: &Value, allowed: &[&str]) -> std::result::Result<(), Error> {
    let map = params
        .as_object()
        .ok_or_else(|| invalid("Expected object parameters"))?;
    if map.keys().any(|k| !allowed.contains(&k.as_str())) {
        return Err(invalid("Unknown operation parameter"));
    }
    Ok(())
}
fn string<'a>(v: &'a Value, key: &str) -> std::result::Result<&'a str, Error> {
    v[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| invalid(format!("Missing {key}")))
}
fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.' | b':'))
}

#[derive(Clone)]
pub struct Endpoint {
    pub base: String,
    pub token: String,
}
impl Endpoint {
    pub fn new(base: &str, token: &str) -> Self {
        Self {
            base: base.into(),
            token: token.into(),
        }
    }
}

pub struct Hub {
    roots: Mutex<HashMap<String, Arc<Root>>>,
    active: Arc<Semaphore>,
    closed: Mutex<HashSet<String>>,
    shutting_down: AtomicBool,
}
impl Default for Hub {
    fn default() -> Self {
        Self {
            roots: Mutex::new(HashMap::new()),
            active: Arc::new(Semaphore::new(32)),
            closed: Mutex::new(HashSet::new()),
            shutting_down: AtomicBool::new(false),
        }
    }
}
impl std::fmt::Debug for Hub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SurfaceHub").finish_non_exhaustive()
    }
}
struct Root {
    id: String,
    endpoint: Mutex<Endpoint>,
    conn: ConnectionTo<Client>,
    state: tokio::sync::Mutex<State>,
    stop: CancellationToken,
    changed: tokio::sync::Notify,
    slots: Arc<Semaphore>,
    subscription_setup: tokio::sync::Mutex<()>,
    catalog_refresh: tokio::sync::Mutex<()>,
}
#[derive(Default)]
struct State {
    subscription: Option<Subscription>,
    epoch: String,
    surfaces: BTreeMap<String, Surface>,
    ready: bool,
    sequence: u64,
    active: HashMap<String, Arc<Control>>,
    used: HashSet<String>,
}
struct Subscription {
    id: String,
    stop: CancellationToken,
}
struct Control {
    stop: CancellationToken,
    credits: Semaphore,
    delivery: Mutex<Delivery>,
    reason: Mutex<&'static str>,
    surface: SurfaceRef,
    ack_changed: tokio::sync::Notify,
}
#[derive(Default)]
struct Delivery {
    sent: u64,
    acked: u64,
    pending: VecDeque<(u64, tokio::time::Instant)>,
}
impl Control {
    fn cancel(&self, reason: &'static str) {
        let mut original = self.reason.lock().unwrap();
        if !self.stop.is_cancelled() {
            *original = reason;
            self.stop.cancel();
        }
    }
    fn ack(&self, n: u64) -> std::result::Result<(), Error> {
        let mut delivery = self.delivery.lock().unwrap();
        if n > delivery.sent {
            return Err(invalid("Acknowledgement exceeds last body sequence"));
        }
        if n > delivery.acked {
            self.credits.add_permits((n - delivery.acked) as usize);
            delivery.acked = n;
            while delivery.pending.front().is_some_and(|(seq, _)| *seq <= n) {
                delivery.pending.pop_front();
            }
            self.ack_changed.notify_waiters();
        }
        Ok(())
    }
    fn sent(&self, sequence: u64) {
        let mut delivery = self.delivery.lock().unwrap();
        delivery.sent = sequence;
        delivery
            .pending
            .push_back((sequence, tokio::time::Instant::now()));
        self.ack_changed.notify_waiters();
    }
    async fn ack_timeout(&self) {
        loop {
            let changed = self.ack_changed.notified();
            tokio::pin!(changed);
            // Register before inspecting delivery so sends/ACKs cannot be
            // missed, including when EOF also awaits acknowledgements.
            changed.as_mut().enable();
            let deadline = {
                let delivery = self.delivery.lock().unwrap();
                let deadline = delivery
                    .pending
                    .front()
                    .map(|(_, sent)| *sent + ACK_TIMEOUT);
                // Decide expiry under the ACK lock too: an ACK received just
                // before the deadline must not lose to a cached older view.
                if deadline.is_some_and(|d| d <= tokio::time::Instant::now()) {
                    return;
                }
                deadline
            };
            if let Some(deadline) = deadline {
                tokio::select! {
                    _ = &mut changed => {},
                    _ = tokio::time::sleep_until(deadline) => {},
                }
            } else {
                changed.await;
            }
        }
    }
    async fn drain_acks(&self) {
        loop {
            let changed = self.ack_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.delivery.lock().unwrap().pending.is_empty() {
                return;
            }
            changed.await;
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SurfaceRef {
    plugin_name: String,
    surface_id: String,
    generation: String,
    revision: String,
}
impl SurfaceRef {
    fn validate(&self) -> Result<()> {
        if self.plugin_name.len() > 128
            || !self
                .plugin_name
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_lowercase)
            || !self
                .plugin_name
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        {
            bail!("Invalid plugin name");
        }
        if self.surface_id.len() > 64
            || !self
                .surface_id
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_lowercase)
            || !self
                .surface_id
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'_' | b'-'))
        {
            bail!("Invalid surface ID");
        }
        if self.generation.is_empty() || self.generation.len() > 128 {
            bail!("Invalid generation");
        }
        decimal(&self.revision)?;
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct Surface {
    #[serde(flatten)]
    reference: SurfaceRef,
    source: Value,
}
impl Surface {
    fn key(&self) -> String {
        format!(
            "{}\0{}",
            self.reference.plugin_name, self.reference.surface_id
        )
    }
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Target {
    session_id: String,
    subscription_id: String,
    daemon_epoch: String,
    surface: SurfaceRef,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Http {
    #[serde(flatten)]
    target: Target,
    request_id: String,
    method: String,
    path: String,
    query: Option<String>,
    #[serde(default)]
    headers: Vec<(String, String)>,
    body_base64: Option<String>,
    #[serde(skip)]
    html: bool,
}
pub struct Reply {
    pub result: Value,
    pub after: Option<Box<dyn FnOnce() + Send>>,
}
impl Reply {
    fn value(v: Value) -> Self {
        Self {
            result: v,
            after: None,
        }
    }
}

impl Hub {
    pub fn reopen(&self, id: &str) {
        self.closed.lock().unwrap().remove(id);
    }
    pub fn bound(&self, id: &str) -> bool {
        self.roots.lock().unwrap().contains_key(id)
    }
    pub async fn bind(&self, id: &str, endpoint: Endpoint, conn: &ConnectionTo<Client>) {
        if self.shutting_down.load(Ordering::SeqCst) {
            return;
        }
        let root = {
            let closed = self.closed.lock().unwrap();
            if closed.contains(id) {
                return;
            }
            let mut roots = self.roots.lock().unwrap();
            roots
                .entry(id.into())
                .or_insert_with(|| {
                    Arc::new(Root {
                        id: id.into(),
                        endpoint: Mutex::new(endpoint.clone()),
                        conn: conn.clone(),
                        state: tokio::sync::Mutex::new(State::default()),
                        stop: CancellationToken::new(),
                        changed: tokio::sync::Notify::new(),
                        slots: Arc::new(Semaphore::new(8)),
                        subscription_setup: tokio::sync::Mutex::new(()),
                        catalog_refresh: tokio::sync::Mutex::new(()),
                    })
                })
                .clone()
        };
        let different = {
            let old = root.endpoint.lock().unwrap();
            old.base != endpoint.base || old.token != endpoint.token
        };
        if different {
            // A snapshot from the previous endpoint must not overwrite the
            // unavailable baseline after the owned daemon is replaced.
            let _refresh = root.catalog_refresh.lock().await;
            let replaced = {
                let mut old = root.endpoint.lock().unwrap();
                let changed = old.base != endpoint.base || old.token != endpoint.token;
                *old = endpoint;
                changed
            };
            if replaced {
                root.invalidate("daemon_replaced", None).await;
                root.changed.notify_one();
            }
        }
    }
    pub fn close(&self, id: &str) {
        self.closed.lock().unwrap().insert(id.into());
        if let Some(root) = self.roots.lock().unwrap().remove(id) {
            root.stop.cancel();
        }
    }
    pub fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        for (_, root) in self.roots.lock().unwrap().drain() {
            root.stop.cancel();
        }
    }
    pub async fn request(
        self: &Arc<Self>,
        name: &str,
        params: Value,
    ) -> std::result::Result<Reply, Error> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(fault(-32002, "Connection closing"));
        }
        let id = string(&params, "sessionId")?;
        let root = self
            .roots
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| fault(-32002, "Unknown active owning session"))?;
        if root.stop.is_cancelled() {
            return Err(fault(-32002, "Session closed"));
        }
        match name.strip_prefix(PREFIX).unwrap_or("") {
            "subscribe" => {
                keys(&params, &["sessionId"])?;
                let _setup = root
                    .subscription_setup
                    .try_lock()
                    .map_err(|_| fault(-32070, "Subscription setup is already pending"))?;
                if root.state.lock().await.subscription.is_some() {
                    return Err(fault(-32070, "Already subscribed"));
                }
                // The ordinary event stream is ephemeral: subscribe BEFORE
                // taking a snapshot, buffer concurrent events, then reconcile.
                let refresh = root.catalog_refresh.lock().await;
                let response = root.events().await.map_err(failed)?;
                let snap = root.snapshot().await.map_err(failed)?;
                let (epoch, surfaces) = catalog(&snap).map_err(failed)?;
                let mut state = root.state.lock().await;
                if root.stop.is_cancelled() {
                    return Err(fault(-32002, "Session closed"));
                }
                if state.subscription.is_some() {
                    return Err(fault(-32070, "Already subscribed"));
                }
                let subscription = uuid::Uuid::new_v4().to_string();
                let stop = root.stop.child_token();
                state.subscription = Some(Subscription {
                    id: subscription.clone(),
                    stop: stop.clone(),
                });
                state.epoch = epoch;
                state.surfaces = surfaces;
                state.ready = true;
                state.sequence = 0;
                state.used.clear();
                let result = json!({"subscriptionId":subscription,"daemonEpoch":state.epoch,"sequence":"0","surfaces":state.surfaces.values().collect::<Vec<_>>()});
                drop(state);
                drop(refresh);
                drop(_setup);
                Ok(Reply {
                    result,
                    after: Some(Box::new(move || {
                        tokio::spawn(async move {
                            root.observe(subscription, stop, response).await;
                        });
                    })),
                })
            }
            "unsubscribe" => {
                keys(&params, &["sessionId", "subscriptionId"])?;
                let sub = string(&params, "subscriptionId")?;
                let mut s = root.state.lock().await;
                check_subscription(&s, sub)?;
                if let Some(subscription) = s.subscription.take() {
                    subscription.stop.cancel();
                }
                for c in s.active.values() {
                    c.cancel("unsubscribed");
                }
                s.ready = false;
                Ok(Reply::value(json!({})))
            }
            "getHtml" | "request" => {
                let permit = self
                    .active
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| fault(-32075, "Connection request limit reached"))?;
                let slot = root
                    .slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| fault(-32075, "Session request limit reached"))?;
                let html = name.ends_with("/getHtml");
                let mut params = params;
                if html {
                    keys(
                        &params,
                        &[
                            "sessionId",
                            "subscriptionId",
                            "daemonEpoch",
                            "surface",
                            "requestId",
                        ],
                    )?;
                    let target: Target = serde_json::from_value(params.clone()).map_err(invalid)?;
                    let surface = root.validate(&target).await?;
                    params["method"] = json!("GET");
                    params["path"] = surface.source["path"].clone();
                    params["headers"] = json!([["accept", "text/html"]]);
                }

                keys(
                    &params,
                    &[
                        "sessionId",
                        "subscriptionId",
                        "daemonEpoch",
                        "surface",
                        "requestId",
                        "method",
                        "path",
                        "query",
                        "headers",
                        "bodyBase64",
                    ],
                )?;
                let mut http: Http = serde_json::from_value(params).map_err(invalid)?;
                http.html = html;
                if !identifier(&http.request_id) {
                    return Err(invalid("Invalid request ID"));
                }
                if !matches!(
                    http.method.as_str(),
                    "GET" | "HEAD" | "POST" | "PUT" | "PATCH" | "DELETE" | "OPTIONS"
                ) {
                    return Err(invalid("Unsupported HTTP method"));
                }
                let surface = root.validate(&http.target).await?;
                let endpoint = root.endpoint();
                let url = plugin_url(
                    &endpoint,
                    &http.target.surface.plugin_name,
                    &http.path,
                    http.query.as_deref(),
                )
                .map_err(invalid)?;
                let headers = request_headers(&http.headers).map_err(invalid)?;
                let body = decode_body(http.body_base64.as_deref()).map_err(invalid)?;
                let mut s = root.state.lock().await;
                check_target(&s, &http.target)?;
                if s.active.len() >= 8 || s.used.len() >= MAX_REQUESTS {
                    return Err(fault(
                        -32075,
                        "Session request limit reached; renew the subscription",
                    ));
                }
                if !s.used.insert(http.request_id.clone()) {
                    return Err(fault(
                        -32073,
                        "Request ID already used in this subscription",
                    ));
                }
                let control = Arc::new(Control {
                    stop: root.stop.child_token(),
                    credits: Semaphore::new(WINDOW),
                    delivery: Mutex::new(Delivery::default()),
                    reason: Mutex::new("cancelled"),
                    surface: http.target.surface.clone(),
                    ack_changed: tokio::sync::Notify::new(),
                });
                s.active.insert(
                    format!("{}\0{}", http.target.subscription_id, http.request_id),
                    control.clone(),
                );
                drop(s);
                let result = json!({"requestId":http.request_id,"observedSurface":surface,"generationAtomicDispatch":false,"bodyRevisionVerified":false});
                Ok(Reply {
                    result,
                    after: Some(Box::new(move || {
                        tokio::spawn(async move {
                            let (_permit, _slot) = (permit, slot);
                            root.http(http, endpoint, url, headers, body, control).await;
                        });
                    })),
                })
            }
            "cancel" | "ack" => {
                let ack = name.ends_with("/ack");
                keys(
                    &params,
                    if ack {
                        &[
                            "sessionId",
                            "subscriptionId",
                            "requestId",
                            "throughSequence",
                        ]
                    } else {
                        &["sessionId", "subscriptionId", "requestId"]
                    },
                )?;
                let sub = string(&params, "subscriptionId")?;
                let id = string(&params, "requestId")?;
                let s = root.state.lock().await;
                check_subscription(&s, sub)?;
                if let Some(c) = s.active.get(&format!("{sub}\0{id}")) {
                    if ack {
                        c.ack(decimal(string(&params, "throughSequence")?).map_err(invalid)?)?;
                    } else {
                        c.cancel("cancelled");
                    }
                } else if !s.used.contains(id) {
                    return Err(fault(-32076, "Unknown request ID"));
                }
                Ok(Reply::value(json!({})))
            }
            _ => Err(Error::method_not_found()),
        }
    }
}

impl Root {
    fn endpoint(&self) -> Endpoint {
        self.endpoint.lock().unwrap().clone()
    }
    async fn snapshot(&self) -> Result<Value> {
        let ep = self.endpoint();
        sync::snapshot(&ep.base, &ep.token).await
    }
    async fn events(&self) -> Result<reqwest::Response> {
        let ep = self.endpoint();
        Ok(tokio::time::timeout(
            Duration::from_secs(15),
            http_client()
                .get(format!("{}/events", ep.base))
                .bearer_auth(&ep.token)
                .send(),
        )
        .await??
        .error_for_status()?)
    }
    async fn refresh(&self, sub: &str, reset: Option<&str>) -> Result<()> {
        // Snapshot revisions are opaque. Serialize read + application rather
        // than attempting to order concurrent responses by their revision.
        let _refresh = self.catalog_refresh.lock().await;
        let snapshot = self.snapshot().await?;
        self.update(sub, &snapshot, reset).await
    }
    async fn validate(&self, target: &Target) -> std::result::Result<Surface, Error> {
        target.surface.validate().map_err(invalid)?;
        if target.session_id != self.id {
            return Err(fault(-32002, "Wrong owning session"));
        }
        // An observer invalidation may have queued before this preflight.
        // Recheck readiness after taking the same lock as invalidation; only
        // the observer may establish a new baseline after losing its stream.
        let _refresh = self.catalog_refresh.lock().await;
        {
            let s = self.state.lock().await;
            check_subscription(&s, &target.subscription_id)?;
            if self.stop.is_cancelled() {
                return Err(fault(-32002, "Session closed"));
            }
            if !s.ready {
                return Err(fault(-32071, "Surface observation unavailable"));
            }
        }
        let snapshot = self.snapshot().await.map_err(failed)?;
        self.update(&target.subscription_id, &snapshot, None)
            .await
            .map_err(failed)?;
        if self.stop.is_cancelled() {
            return Err(fault(-32002, "Session closed"));
        }
        let s = self.state.lock().await;
        check_target(&s, target)
    }
    async fn update(&self, sub: &str, snapshot: &Value, reset: Option<&str>) -> Result<()> {
        let (epoch, surfaces) = catalog(snapshot)?;
        let mut s = self.state.lock().await;
        if s.subscription.as_ref().map(|s| s.id.as_str()) != Some(sub) {
            return Ok(());
        }
        let replaced = s.epoch != epoch;
        let recovering = !s.ready;
        for c in s.active.values() {
            let current = surfaces.get(&format!(
                "{}\0{}",
                c.surface.plugin_name, c.surface.surface_id
            ));
            if replaced || reset.is_some() || current.is_none_or(|v| v.reference != c.surface) {
                c.cancel("surface_invalidated");
            }
        }
        let old = std::mem::replace(&mut s.surfaces, surfaces);
        s.epoch = epoch;
        s.ready = true;
        if replaced || reset.is_some() || recovering {
            let event = json!({"kind":"reset","reason":reset.unwrap_or(if replaced {"daemon_epoch_changed"}else{"resynchronized"}),"surfaces":s.surfaces.values().collect::<Vec<_>>() });
            notify_catalog(self, &mut s, event);
        } else {
            let changed = s
                .surfaces
                .iter()
                .filter(|(k, v)| old.get(*k) != Some(*v))
                .map(|(_, v)| v.clone())
                .collect::<Vec<_>>();
            for (key, previous) in old {
                if !s.surfaces.contains_key(&key) {
                    notify_catalog(self, &mut s, json!({"kind":"removed","surface":previous}));
                }
            }
            for surface in changed {
                notify_catalog(self, &mut s, json!({"kind":"upsert","surface":surface}));
            }
        }
        Ok(())
    }
    async fn invalidate(&self, reason: &'static str, subscription: Option<&str>) {
        let mut s = self.state.lock().await;
        if subscription.is_some_and(|sub| s.subscription.as_ref().is_none_or(|v| v.id != sub)) {
            return;
        }
        for c in s.active.values() {
            c.cancel(reason);
        }
        s.ready = false;
        if s.subscription.is_some() {
            notify_catalog(
                self,
                &mut s,
                json!({"kind":"reset","reason":reason,"surfaces":null}),
            );
        }
    }
    async fn observe(
        self: Arc<Self>,
        sub: String,
        stop: CancellationToken,
        first: reqwest::Response,
    ) {
        let mut next = Some(first);
        let mut delay = 250;
        loop {
            let observe = async {
                let response = if let Some(response) = next.take() {
                    response
                } else {
                    let _refresh = self.catalog_refresh.lock().await;
                    let response = self.events().await?;
                    let snap = self.snapshot().await?;
                    self.update(&sub, &snap, Some("reconnected")).await?;
                    response
                };
                let mut stream = response.bytes_stream();
                let mut parser = agent::SseParser::new();
                loop {
                    let chunk = tokio::select! {_=self.changed.notified()=>bail!("Endpoint changed"),next=stream.next()=>next.context("Surface event stream ended")??};
                    let mut dirty = false;
                    let mut reset = false;
                    for data in parser.feed(&chunk) {
                        let envelope: Value = serde_json::from_str(&data)?;
                        if envelope.get("session_id").is_some_and(|id| id != &self.id) {
                            bail!("Wrong session in surface event stream");
                        }
                        let event = envelope.get("event").unwrap_or(&envelope);
                        let kind = event["type"].as_str().context("Invalid daemon event")?;
                        reset |= kind == "stream_discontinuity";
                        dirty |= !matches!(
                            kind,
                            "heartbeat"
                                | "content_block_delta"
                                | "content_block_start"
                                | "content_block_stop"
                                | "usage_throttle"
                        );
                    }
                    if dirty {
                        self.refresh(&sub, if reset { Some("event_gap") } else { None })
                            .await?;
                    }
                }
            };
            let result: Result<()> =
                tokio::select! {_=stop.cancelled()=>return,result=observe=>result};
            if let Err(error) = result {
                tracing::debug!(%error,"Surface observation interrupted");
                let _refresh = self.catalog_refresh.lock().await;
                self.invalidate("sync_unavailable", Some(&sub)).await;
            }
            tokio::select! {_=stop.cancelled()=>return,_=tokio::time::sleep(Duration::from_millis(delay))=>{}}
            delay = (delay * 2).min(4000);
        }
    }
    #[allow(clippy::too_many_arguments)]
    async fn http(
        self: Arc<Self>,
        http: Http,
        endpoint: Endpoint,
        url: reqwest::Url,
        headers: reqwest::header::HeaderMap,
        body: Vec<u8>,
        control: Arc<Control>,
    ) {
        let mut sequence = 0u64;
        let mut headers_sent = false;
        let work = async {
            self.validate(&http.target)
                .await
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let request = http_client()
                .request(reqwest::Method::from_bytes(http.method.as_bytes())?, url)
                .headers(headers)
                .bearer_auth(&endpoint.token)
                .body(body);
            let response = tokio::time::timeout(Duration::from_secs(20), request.send()).await??;
            self.validate(&http.target)
                .await
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            self.emit_http(&http,sequence,json!({"kind":"headers","status":response.status().as_u16(),"headers":response_headers(response.headers())}));
            headers_sent = true;
            let mut stream = response.bytes_stream();
            let mut total = 0usize;
            while let Some(bytes) = stream.next().await {
                let bytes = bytes?;
                total = total.saturating_add(bytes.len());
                if http.html && total > MAX_HTML {
                    bail!("HTML response exceeds limit");
                }
                if bytes.len() > MAX_BODY {
                    bail!("Upstream chunk exceeds buffer limit");
                }
                for chunk in bytes.chunks(CHUNK) {
                    let permit = control.credits.acquire().await?;
                    permit.forget();
                    sequence += 1;
                    control.sent(sequence);
                    self.emit_http(
                        &http,
                        sequence,
                        json!({"kind":"body","bodyBase64":STANDARD.encode(chunk)}),
                    );
                }
            }
            // Keep the concurrency slot until the consumer has accepted the
            // final body bytes, not merely until the upstream socket hit EOF.
            control.drain_acks().await;
            Ok::<_, anyhow::Error>(())
        };
        let outcome = tokio::select! {
            _=control.stop.cancelled()=>json!({"outcome":"cancelled","reason":*control.reason.lock().unwrap()}),
            _=control.ack_timeout()=>json!({"outcome":"error","code":"transport_error","message":"Plugin HTTP acknowledgement timeout"}),
            result=work=>match result {Ok(())=>json!({"outcome":"complete"}),Err(error)=>{tracing::debug!(%error,"Plugin HTTP relay failed");json!({"outcome":"error","code":"transport_error","message":"Plugin HTTP transport or flow-control limit failed"})}}
        };
        let outcome = if control.stop.is_cancelled() {
            json!({"outcome":"cancelled","reason":if self.stop.is_cancelled(){"session_closed"}else{*control.reason.lock().unwrap()}})
        } else {
            outcome
        };
        self.emit_http(
            &http,
            sequence + u64::from(headers_sent),
            json!({"kind":"end","result":outcome}),
        );
        self.state.lock().await.active.remove(&format!(
            "{}\0{}",
            http.target.subscription_id, http.request_id
        ));
    }
    fn emit_http(&self, http: &Http, sequence: u64, event: Value) {
        agent::send_ext_notification(
            &self.conn,
            &format!("{PREFIX}httpEvent"),
            &json!({"sessionId":self.id,"subscriptionId":http.target.subscription_id,"daemonEpoch":http.target.daemon_epoch,"surface":http.target.surface,"requestId":http.request_id,"sequence":sequence.to_string(),"event":event}),
        );
    }
}

fn check_subscription(s: &State, id: &str) -> std::result::Result<(), Error> {
    if s.subscription.as_ref().map(|s| s.id.as_str()) != Some(id) {
        Err(fault(-32070, "Unknown subscription"))
    } else {
        Ok(())
    }
}
fn check_target(s: &State, t: &Target) -> std::result::Result<Surface, Error> {
    check_subscription(s, &t.subscription_id)?;
    if !s.ready || s.epoch != t.daemon_epoch {
        return Err(fault(-32071, "Stale or unavailable daemon epoch"));
    }
    let surface = s
        .surfaces
        .get(&format!(
            "{}\0{}",
            t.surface.plugin_name, t.surface.surface_id
        ))
        .ok_or_else(|| fault(-32072, "Surface removed"))?;
    if surface.reference != t.surface {
        return Err(fault(-32071, "Stale surface generation or revision"));
    }
    Ok(surface.clone())
}
fn notify_catalog(root: &Root, s: &mut State, event: Value) {
    let Some(sub) = &s.subscription else { return };
    s.sequence += 1;
    agent::send_ext_notification(
        &root.conn,
        &format!("{PREFIX}surfaceEvent"),
        &json!({"sessionId":root.id,"subscriptionId":sub.id,"daemonEpoch":s.epoch,"sequence":s.sequence.to_string(),"event":event}),
    );
}
fn catalog(v: &Value) -> Result<(String, BTreeMap<String, Surface>)> {
    let epoch = v["daemon_epoch"]
        .as_str()
        .context("Missing daemon epoch")?
        .to_string();
    let mut surfaces = BTreeMap::new();
    // 0.9.8 omits this optional/default-empty field when no plugin has output.
    // A present malformed value remains an error, not an empty catalog.
    let entries = v
        .get("surfaces")
        .map(|v| v.as_array().context("Invalid surface snapshots"))
        .transpose()?;
    for value in entries.map(Vec::as_slice).unwrap_or(&[]) {
        let surface = Surface {
            reference: SurfaceRef {
                plugin_name: value["plugin_name"]
                    .as_str()
                    .context("Missing plugin name")?
                    .into(),
                surface_id: value["surface_id"]
                    .as_str()
                    .context("Missing surface ID")?
                    .into(),
                generation: value["generation"]
                    .as_str()
                    .context("Missing generation")?
                    .into(),
                revision: value["revision"]
                    .as_str()
                    .context("Missing revision")?
                    .into(),
            },
            // Keep the public extension's source shape stable even if the
            // daemon later adds internal metadata to this descriptor.
            source: json!({"type":value["source"]["type"],"path":value["source"]["path"]}),
        };
        surface.reference.validate()?;
        if !identifier(&surface.reference.plugin_name)
            || surface.reference.surface_id.is_empty()
            || surface.source["type"] != "plugin_http"
        {
            bail!("Unsupported surface descriptor");
        }
        local_path(
            surface.source["path"]
                .as_str()
                .context("Missing surface path")?,
        )?;
        decimal(&surface.reference.revision)?;
        if surfaces.insert(surface.key(), surface).is_some() {
            bail!("Duplicate surface identity");
        }
    }
    Ok((epoch, surfaces))
}
fn decimal(s: &str) -> Result<u64> {
    if s.is_empty() || (s.len() > 1 && s.starts_with('0')) || !s.bytes().all(|c| c.is_ascii_digit())
    {
        bail!("Expected canonical unsigned decimal string");
    }
    Ok(s.parse()?)
}
fn local_path(path: &str) -> Result<()> {
    if path.len() > 4096
        || !path.starts_with('/')
        || path.starts_with("//")
        || path.contains(['\\', '?', '#'])
        || path.chars().any(char::is_control)
    {
        bail!("Expected plugin-local absolute path");
    }
    let mut decoded = path.to_string();
    for _ in 0..8 {
        if decoded.split('/').any(|s| s == "." || s == "..") || decoded.contains('\\') {
            bail!("Path traversal is forbidden");
        }
        let mut bytes = Vec::new();
        let input = decoded.as_bytes();
        let mut i = 0;
        let mut changed = false;
        while i < input.len() {
            if input[i] == b'%' {
                if i + 2 >= input.len() {
                    bail!("Invalid path encoding");
                }
                let a = (input[i + 1] as char)
                    .to_digit(16)
                    .context("Invalid path encoding")?;
                let b = (input[i + 2] as char)
                    .to_digit(16)
                    .context("Invalid path encoding")?;
                let b = (a * 16 + b) as u8;
                if matches!(b, b'/' | b'\\' | b'?' | b'#' | 0..=31 | 127) {
                    bail!("Encoded path separator is forbidden");
                }
                bytes.push(b);
                i += 3;
                changed = true;
            } else {
                bytes.push(input[i]);
                i += 1;
            }
        }
        if !changed {
            return Ok(());
        }
        decoded = String::from_utf8(bytes)?;
    }
    bail!("Excessive nested path encoding")
}
fn plugin_url(
    endpoint: &Endpoint,
    plugin: &str,
    path: &str,
    query: Option<&str>,
) -> Result<reqwest::Url> {
    if !identifier(plugin) || plugin.contains(':') {
        bail!("Invalid plugin name");
    }
    local_path(path)?;
    if query.is_some_and(|q| {
        q.len() > 8192 || q.contains('#') || !q.is_ascii() || q.chars().any(char::is_control)
    }) {
        bail!("Invalid query");
    }
    let mut url = reqwest::Url::parse(&format!("{}/plugins/{plugin}/http{path}", endpoint.base))?;
    if !url.path().starts_with(&format!("/plugins/{plugin}/http/")) {
        bail!("Path escaped plugin namespace");
    }
    url.set_query(query);
    Ok(url)
}
fn forbidden(name: &str) -> bool {
    matches!(
        name,
        "authorization"
            | "proxy-authorization"
            | "cookie"
            | "cookie2"
            | "set-cookie"
            | "host"
            | "connection"
            | "proxy-connection"
            | "transfer-encoding"
            | "content-length"
            | "expect"
            | "upgrade"
            | "te"
            | "trailer"
            | "forwarded"
            | "origin"
            | "referer"
            | "x-real-ip"
    ) || name.starts_with("x-forwarded-")
        || name.starts_with("x-polytoken-")
        || name.starts_with("sec-")
}
fn request_headers(headers: &[(String, String)]) -> Result<reqwest::header::HeaderMap> {
    if headers.len() > 64
        || headers
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>()
            > 32768
    {
        bail!("Header limit exceeded");
    }
    let mut map = reqwest::header::HeaderMap::new();
    for (name, value) in headers {
        let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())?;
        if forbidden(name.as_str()) {
            bail!("Header {} is reserved", name);
        }
        map.append(name, reqwest::header::HeaderValue::from_str(value)?);
    }
    Ok(map)
}
fn response_headers(headers: &reqwest::header::HeaderMap) -> Vec<(String, String)> {
    let nominated = headers
        .get_all("connection")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|v| v.trim().to_ascii_lowercase())
        .collect::<HashSet<_>>();
    headers
        .iter()
        .filter(|(n, _)| !forbidden(n.as_str()) && !nominated.contains(n.as_str()))
        .map(|(n, v)| (n.as_str().into(), STANDARD.encode(v.as_bytes())))
        .collect()
}
fn decode_body(s: Option<&str>) -> Result<Vec<u8>> {
    let Some(s) = s else { return Ok(vec![]) };
    if s.len() > MAX_BODY.div_ceil(3) * 4 {
        bail!("Request body exceeds limit");
    }
    let body = STANDARD.decode(s)?;
    if body.len() > MAX_BODY {
        bail!("Request body exceeds limit");
    }
    Ok(body)
}
fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .retry(reqwest::retry::never())
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .build()
        .expect("HTTP client configuration")
}

#[cfg(test)]
mod tests {
    use super::*;
    fn surface() -> SurfaceRef {
        SurfaceRef {
            plugin_name: "demo".into(),
            surface_id: "page".into(),
            generation: "generation-one".into(),
            revision: "1".into(),
        }
    }
    fn control() -> Control {
        Control {
            stop: CancellationToken::new(),
            credits: Semaphore::new(WINDOW),
            delivery: Mutex::new(Delivery::default()),
            reason: Mutex::new("cancelled"),
            surface: surface(),
            ack_changed: tokio::sync::Notify::new(),
        }
    }
    fn deliver(control: &Control, sequence: u64) {
        control.credits.try_acquire().unwrap().forget();
        control.sent(sequence);
    }
    #[test]
    fn request_urls_cannot_escape_a_plugin_and_queries_keep_their_bytes() {
        let endpoint = Endpoint::new("http://127.0.0.1:1234", "private-token");
        let url = plugin_url(
            &endpoint,
            "demo",
            "/actions/a%20b",
            Some("x=1&x=2&data=%2f+%20"),
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "http://127.0.0.1:1234/plugins/demo/http/actions/a%20b?x=1&x=2&data=%2f+%20"
        );
        for path in [
            "https://other/",
            "//other/",
            "/../sync",
            "/%2e%2e/sync",
            "/a%2fb",
            "/a%255cb",
            "/a\\b",
            "/a?x=1",
            "/a#b",
            "/%é",
        ] {
            assert!(plugin_url(&endpoint, "demo", path, None).is_err(), "{path}");
        }
        assert!(plugin_url(&endpoint, "../other", "/a", None).is_err());
    }
    #[test]
    fn headers_preserve_duplicates_without_credentials_or_hop_headers() {
        let request = request_headers(&[
            ("x-test".into(), "one".into()),
            ("x-test".into(), "two".into()),
            ("datastar-request".into(), "true".into()),
        ])
        .unwrap();
        assert_eq!(request.get_all("x-test").iter().count(), 2);
        for name in [
            "Authorization",
            "Cookie",
            "Host",
            "Connection",
            "X-Polytoken-Session",
            "Transfer-Encoding",
        ] {
            assert!(request_headers(&[(name.into(), "forbidden".into())]).is_err());
        }
        let mut response = request.clone();
        response.insert("set-cookie", "secret=1".parse().unwrap());
        response.insert("connection", "x-test".parse().unwrap());
        let exposed = response_headers(&response);
        assert_eq!(
            exposed,
            [("datastar-request".into(), STANDARD.encode("true"))]
        );
    }
    #[test]
    fn credit_acknowledgements_are_monotonic_bounded_and_idempotent() {
        let c = control();
        for sequence in 1..=4 {
            deliver(&c, sequence);
        }
        assert!(c.ack(5).is_err());
        assert_eq!(c.credits.available_permits(), 0);
        c.ack(3).unwrap();
        assert_eq!(c.credits.available_permits(), 3);
        c.ack(3).unwrap();
        c.ack(1).unwrap();
        assert_eq!(c.credits.available_permits(), 3);
        c.ack(4).unwrap();
        assert_eq!(c.credits.available_permits(), 4);
        c.cancel("surface_invalidated");
        assert!(c.stop.is_cancelled());
    }
    #[tokio::test(start_paused = true)]
    async fn quiet_upstream_and_new_bytes_do_not_extend_an_unacked_deadline() {
        let c = control();
        deliver(&c, 1);
        let timeout = c.ack_timeout();
        tokio::pin!(timeout);
        assert!(futures::poll!(&mut timeout).is_pending());
        tokio::time::advance(Duration::from_secs(10)).await;
        deliver(&c, 2);
        c.ack(0).unwrap();
        assert!(futures::poll!(&mut timeout).is_pending());
        tokio::time::advance(Duration::from_secs(20)).await;
        assert!(futures::poll!(&mut timeout).is_ready());
    }
    #[tokio::test(start_paused = true)]
    async fn partial_ack_keeps_the_remaining_chunks_original_deadline() {
        let c = control();
        deliver(&c, 1);
        tokio::time::advance(Duration::from_secs(10)).await;
        deliver(&c, 2);
        c.ack(1).unwrap();
        let timeout = c.ack_timeout();
        tokio::pin!(timeout);
        tokio::time::advance(Duration::from_secs(20)).await;
        assert!(futures::poll!(&mut timeout).is_pending());
        c.ack(1).unwrap();
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(futures::poll!(&mut timeout).is_ready());
    }
    #[tokio::test(start_paused = true)]
    async fn quiet_acknowledged_streams_remain_valid_and_later_bytes_start_a_new_deadline() {
        let c = control();
        let timeout = c.ack_timeout();
        tokio::pin!(timeout);
        assert!(futures::poll!(&mut timeout).is_pending());
        tokio::time::advance(ACK_TIMEOUT * 2).await;
        assert!(futures::poll!(&mut timeout).is_pending());
        deliver(&c, 1);
        assert!(futures::poll!(&mut timeout).is_pending());
        tokio::time::advance(ACK_TIMEOUT - Duration::from_secs(1)).await;
        c.ack(1).unwrap();
        tokio::time::advance(ACK_TIMEOUT * 2).await;
        assert!(futures::poll!(&mut timeout).is_pending());
        deliver(&c, 2);
        assert!(futures::poll!(&mut timeout).is_pending());
        tokio::time::advance(ACK_TIMEOUT).await;
        assert!(futures::poll!(&mut timeout).is_ready());
    }
    #[tokio::test(start_paused = true)]
    async fn eof_and_deadline_waiters_both_observe_the_final_ack() {
        let c = control();
        deliver(&c, 1);
        let timeout = c.ack_timeout();
        let drain = c.drain_acks();
        tokio::pin!(timeout, drain);
        assert!(futures::poll!(&mut timeout).is_pending());
        assert!(futures::poll!(&mut drain).is_pending());
        c.ack(1).unwrap();
        assert!(futures::poll!(&mut drain).is_ready());
        tokio::time::advance(ACK_TIMEOUT * 2).await;
        assert!(futures::poll!(&mut timeout).is_pending());
    }
    #[tokio::test]
    async fn queued_preflight_cannot_restore_an_invalidated_observer_baseline() {
        let (transport, _peer) = agent_client_protocol::Channel::duplex();
        agent_client_protocol::Agent
            .v1()
            .connect_with(transport, async |conn| {
                let hub = Arc::new(Hub::default());
                // No server: a rejected preflight must not attempt a snapshot.
                hub.bind(
                    "root",
                    Endpoint::new("http://127.0.0.1:1", "private"),
                    &conn,
                )
                .await;
                let root = hub.roots.lock().unwrap()["root"].clone();
                {
                    let mut state = root.state.lock().await;
                    state.subscription = Some(Subscription {
                        id: "subscription".into(),
                        stop: CancellationToken::new(),
                    });
                    state.ready = true;
                    state.epoch = "epoch".into();
                }
                let target = Target {
                    session_id: "root".into(),
                    subscription_id: "subscription".into(),
                    daemon_epoch: "epoch".into(),
                    surface: surface(),
                };
                let gate = root.catalog_refresh.lock().await;
                let invalidate = async {
                    let _gate = root.catalog_refresh.lock().await;
                    root.invalidate("sync_unavailable", Some("subscription"))
                        .await;
                };
                let preflight = root.validate(&target);
                tokio::pin!(invalidate, preflight);
                // Queue invalidation first, then the preflight, behind a held
                // snapshot lock. Tokio's mutex preserves this ordering.
                assert!(futures::poll!(&mut invalidate).is_pending());
                assert!(futures::poll!(&mut preflight).is_pending());
                drop(gate);
                invalidate.await;
                let error = preflight.await.unwrap_err();
                assert_eq!(serde_json::to_value(error).unwrap()["code"], -32071);
                let state = root.state.lock().await;
                assert!(!state.ready);
                assert_eq!(state.sequence, 1); // Only reset-null, no resynchronized.
                Ok(())
            })
            .await
            .unwrap();
    }
    #[test]
    fn catalog_preserves_large_revisions_and_never_substitutes_missing_state() {
        let snapshot = json!({"daemon_epoch":"epoch","surfaces":[{"plugin_name":"demo","surface_id":"page","generation":"gen","revision":"18446744073709551615","source":{"type":"plugin_http","path":"/surface"}}]});
        let (_, surfaces) = catalog(&snapshot).unwrap();
        assert_eq!(
            surfaces.values().next().unwrap().reference.revision,
            "18446744073709551615"
        );
        assert!(
            catalog(&json!({"daemon_epoch":"epoch"}))
                .unwrap()
                .1
                .is_empty()
        );
        assert!(catalog(&json!({"daemon_epoch":"epoch","surfaces":null})).is_err());
        let mut bad = snapshot.clone();
        bad["surfaces"][0]["revision"] = json!("18446744073709551616");
        assert!(catalog(&bad).is_err());
        bad = snapshot.clone();
        bad["surfaces"][0]["source"]["path"] = json!("https://other/");
        assert!(catalog(&bad).is_err());
    }
    #[test]
    fn active_requests_require_the_current_subscription_epoch_and_descriptor() {
        let reference = surface();
        let descriptor = Surface {
            reference: reference.clone(),
            source: json!({"type":"plugin_http","path":"/surface"}),
        };
        let mut state = State {
            subscription: Some(Subscription {
                id: "subscription".into(),
                stop: CancellationToken::new(),
            }),
            epoch: "epoch".into(),
            ready: true,
            ..Default::default()
        };
        state.surfaces.insert(descriptor.key(), descriptor);
        let mut t = Target {
            session_id: "root".into(),
            subscription_id: "subscription".into(),
            daemon_epoch: "epoch".into(),
            surface: reference,
        };
        assert!(check_target(&state, &t).is_ok());
        t.surface.generation = "obsolete".into();
        assert_eq!(
            serde_json::to_value(check_target(&state, &t).unwrap_err()).unwrap()["code"],
            -32071
        );
        t.surface = surface();
        t.daemon_epoch = "old-epoch".into();
        assert!(check_target(&state, &t).is_err());
        t.daemon_epoch = "epoch".into();
        t.subscription_id = "old-subscription".into();
        assert!(check_target(&state, &t).is_err());
        t.subscription_id = "subscription".into();
        state.ready = false;
        assert!(check_target(&state, &t).is_err());
        state.ready = true;
        state.surfaces.clear();
        assert_eq!(
            serde_json::to_value(check_target(&state, &t).unwrap_err()).unwrap()["code"],
            -32072
        );
    }
    #[test]
    fn request_bodies_and_capability_versions_have_explicit_bounds() {
        assert!(!offered(None));
        assert!(!offered(Some(&json!({CAPABILITY:{"versions":[2]}}))));
        assert!(offered(Some(&json!({CAPABILITY:{"versions":[1]}}))));
        assert_eq!(
            decode_body(Some(&STANDARD.encode([0, 255, 128]))).unwrap(),
            [0, 255, 128]
        );
        assert!(decode_body(Some("not-base64")).is_err());
        assert!(decode_body(Some(&"A".repeat(MAX_BODY * 2))).is_err());
        assert!(decimal("01").is_err());
        assert!(decimal("-1").is_err());
    }
}
