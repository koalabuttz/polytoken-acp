//! Daemon lifecycle management and HTTP API helpers.
//!
//! This module spawns a `polytoken daemon` child process, waits for it to be ready,
//! and provides thin HTTP wrapper methods for the daemon's REST API.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine;
use rand::Rng;
use serde::Deserialize;
use tokio::process::{Child, Command};
use tracing::{debug, info};

/// How long to wait for the daemon to become ready (polling startup.json).
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// Handle to a running polytoken daemon process.
pub struct DaemonHandle {
    base_url: String,
    bearer_token: String,
    session_id: String,
    cwd: std::path::PathBuf,
    child: Option<Child>,
    #[allow(dead_code)]
    sessions_dir: PathBuf,
    /// Temp directory for the daemon process: contains credential file and
    /// logs. Cleaned up on terminate.
    temp_dir: Option<tempfile::TempDir>,
    /// Temp directory containing MCP server config forwarded from the ACP
    /// client. Cleaned up on terminate.
    mcp_config_dir: Option<PathBuf>,
    lease: Option<std::fs::File>,
}

#[derive(Deserialize)]
struct StartupJson {
    state: String,
    port: Option<u16>,
    #[serde(default)]
    message: Option<String>,
}

/// Subset of the daemon's `GET /state` response (`SessionStateSnapshot`)
/// that we need to populate the ACP model list.
#[allow(dead_code)]
#[derive(Deserialize)]
pub(crate) struct SessionStateSnapshot {
    #[serde(default)]
    pub active_model: Option<String>,
    #[serde(default)]
    pub available_models: Vec<AvailableModelEntry>,
}

/// One entry in the daemon's available-model list.
#[allow(dead_code)]
#[derive(Deserialize)]
pub(crate) struct AvailableModelEntry {
    pub name: String,
    pub label: String,
}

impl DaemonHandle {
    /// Spawn a new polytoken daemon for the given working directory.
    #[allow(dead_code)]
    pub async fn spawn(cwd: &Path) -> Result<DaemonHandle> {
        Self::spawn_with_session_id(cwd, None, None).await
    }

    /// Spawn a polytoken daemon, optionally resuming an existing session by ID.
    ///
    /// When `resume_session_id` is `Some`, the daemon is started with the given
    /// session ID and will load the corresponding session history from its
    /// internal store. A fresh temp directory and credential are always
    /// created for the daemon process itself.
    ///
    /// When `project_config_dir` is `Some`, it is passed as
    /// `--project-config-dir` so the daemon loads an additional project-level
    /// config layer (used for forwarding ACP-provided MCP servers).
    pub async fn spawn_with_session_id(
        cwd: &Path,
        resume_session_id: Option<&str>,
        project_config_dir: Option<&Path>,
    ) -> Result<DaemonHandle> {
        let session_id = resume_session_id
            .map(|s| s.to_string())
            .unwrap_or_else(generate_session_id);
        // Acquire ownership before touching startup.json or launching a child.
        let lease = crate::store::lease(&session_id)?;
        // A fresh private directory also prevents a resumed session from
        // reusing a credential path or following an existing file symlink.
        let temp_dir = tempfile::Builder::new()
            .prefix("polytoken-acp-")
            .tempdir()
            .context("Failed to create private daemon directory")?;
        let log_dir = temp_dir.path().join("logs");
        let cred_path = temp_dir.path().join("credential.json");

        // Use a dedicated ACP sessions directory (~/.local/share/polytoken/sessions-acp/)
        // instead of the default `~/.local/share/polytoken/sessions/`.
        //
        // WHY: The polytoken TUI (`polytoken attach`, `polytoken sessions`,
        // `polytoken continue`) discovers live and resumable sessions by scanning
        // the default sessions directory. If the ACP-managed daemon were registered
        // there, a user could attach a TUI to the same daemon that this shim is
        // driving. When the ACP connection ends (Paseo window closed, stdin EOF,
        // or process exit), the cleanup code in `run()` terminates the daemon —
        // which would kill the TUI's SSE stream with confusing "SSE errors"
        // instead of a clear "session not found."
        //
        // Using a separate directory makes ACP sessions invisible to TUI
        // tooling: `polytoken sessions` won't list them, `polytoken attach
        // <id>` won't find them, and `polytoken continue` will resume from its
        // own separate history (spawning its own daemon if needed). ACP resume
        // still works because `session/load` always passes `--sessions-dir` to
        // point at this directory, so `--resume` finds the saved `log.jsonl`.
        let sessions_dir = acp_sessions_dir();

        // Create temp dir with 0700 permissions (polytoken requires it)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(temp_dir.path(), std::fs::Permissions::from_mode(0o700))
                .context("Failed to set temp dir permissions")?;
        }
        std::fs::create_dir_all(&log_dir).context("Failed to create log dir")?;

        let bearer_token = generate_token();
        let cred_json = serde_json::json!({
            "version": 1,
            "kind": "polytoken-daemon-credential",
            "token": bearer_token,
        });
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        // Set permissions at creation, before any token bytes are written.
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let credential = options
            .open(&cred_path)
            .context("Failed to create credential file")?;
        serde_json::to_writer(credential, &cred_json).context("Failed to write credential file")?;

        // Remove any stale startup.json from a previous daemon run. Without this,
        // poll_startup would read the old file (state: "ready", old port) on its
        // very first iteration and return the old port — before the new daemon
        // has had a chance to overwrite it. This happens when polytoken-acp
        // restarts (e.g. Paseo bounce) and resumes a session whose daemon was
        // killed or orphaned, leaving a stale startup.json behind.
        // Polytoken 0.9.x stores the v1 session tree beside the supplied
        // legacy sessions directory, with a "-v1" suffix. Resume may migrate
        // an older tree, so clear stale startup records in both locations.
        let startup_paths = startup_paths(&sessions_dir, &session_id);
        for startup_path in &startup_paths {
            match std::fs::remove_file(startup_path) {
                Ok(()) => debug!(path = ?startup_path, "Removed stale startup.json"),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).context("Failed to remove stale startup.json"),
            }
        }

        info!(session_id = %session_id, cwd = ?cwd, "Spawning polytoken daemon");

        let mut cmd = Command::new("polytoken");
        cmd.arg("daemon")
            .arg("--harness-type")
            .arg("acp")
            .arg("--project-dir")
            .arg(cwd)
            .arg("--credential-file")
            .arg(&cred_path)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--sessions-dir")
            .arg(&sessions_dir)
            .arg("--log-dir")
            .arg(&log_dir)
            .arg("--session-id")
            .arg(&session_id);

        // When resuming, pass --resume so the daemon loads saved history.
        if resume_session_id.is_some() {
            cmd.arg("--resume");
        }

        // When the client provided MCP servers, pass a project-level config dir
        // so the daemon merges those servers on top of the user's global config.
        if let Some(config_dir) = project_config_dir {
            cmd.arg("--project-config-dir").arg(config_dir);
        }

        let mut child = cmd
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to spawn polytoken daemon")?;

        // Drain child stdout to prevent pipe blocking; log stderr.
        if let Some(stdout) = child.stdout.take() {
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut buf = vec![0u8; 1024];
                let mut stdout = stdout;
                loop {
                    match stdout.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            });
        }
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut buf = vec![0u8; 4096];
                let mut stderr = stderr;
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            let s = String::from_utf8_lossy(&buf[..n]);
                            for line in s.lines() {
                                debug!(target: "polytoken_daemon", "{}", line);
                            }
                        }
                    }
                }
            });
        }

        // Poll startup.json until state == "ready"
        let timeout_result =
            tokio::time::timeout(STARTUP_TIMEOUT, poll_startup(&startup_paths, &mut child)).await;

        let port = match timeout_result {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => {
                let _ = child.kill().await;
                return Err(e);
            }
            Err(_) => {
                // Check if the process is still alive
                match child.try_wait() {
                    Ok(Some(status)) => bail!(
                        "Polytoken daemon exited during startup with status: {}. Enable RUST_LOG=debug for daemon stderr.",
                        status
                    ),
                    Ok(None) => {
                        let _ = child.kill().await;
                        bail!(
                            "Polytoken daemon failed to become ready within {} seconds. Enable RUST_LOG=debug for daemon stderr.",
                            STARTUP_TIMEOUT.as_secs()
                        );
                    }
                    Err(e) => bail!(
                        "Daemon startup timed out and process status check failed: {}",
                        e
                    ),
                }
            }
        };

        // Quick health check
        let base_url = format!("http://127.0.0.1:{}", port);
        let _port = port; // suppress unused warning in some paths

        let mut handle = DaemonHandle {
            base_url: base_url.clone(),
            bearer_token,
            session_id,
            cwd: cwd.to_path_buf(),
            child: Some(child),
            sessions_dir,
            temp_dir: Some(temp_dir),
            mcp_config_dir: None,
            lease: Some(lease),
        };

        // Verify the daemon is responding
        let mut last_error: Option<String> = None;
        let mut last_status: Option<u16> = None;
        for attempt in 0..20 {
            match handle.health().await {
                Ok(200) => {
                    info!(session_id = %handle.session_id, "Daemon is healthy");
                    return Ok(handle);
                }
                Ok(status) => {
                    last_status = Some(status);
                    last_error = None;
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    last_status = None;
                }
            }
            if attempt < 19 {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        // All 20 attempts failed — kill the child and report diagnostics
        let _ = handle.child.as_mut().unwrap().kill().await;
        let child_status = match handle.child.as_mut().unwrap().try_wait() {
            Ok(Some(status)) => format!("exited with status {}", status),
            Ok(None) => "still running".to_string(),
            Err(e) => format!("status check failed: {}", e),
        };
        match (&last_error, &last_status) {
            (Some(err), None) => bail!(
                "Daemon startup.json says ready but /health is not responding after 20 attempts. \
                 Last transport error: {}. Child process: {}. \
                 Enable RUST_LOG=debug for daemon stderr.",
                err,
                child_status
            ),
            (None, Some(status)) => bail!(
                "Daemon startup.json says ready but /health returned HTTP {} after 20 attempts. \
                 Child process: {}. Enable RUST_LOG=debug for daemon stderr.",
                status,
                child_status
            ),
            _ => bail!(
                "Daemon startup.json says ready but /health is not responding after 20 attempts. \
                 Child process: {}. Enable RUST_LOG=debug for daemon stderr.",
                child_status
            ),
        }
    }

    /// Send a prompt to the daemon using explicit connection params (no borrow needed).
    pub async fn prompt_with(base_url: &str, bearer_token: &str, content: &str) -> Result<String> {
        let snapshot = crate::sync::snapshot(base_url, bearer_token).await?;
        let response = crate::sync::mutate(
            base_url,
            bearer_token,
            "/prompt",
            serde_json::json!({"content":content}),
            &snapshot,
        )
        .await?;
        Ok(response["prompt_id"]
            .as_str()
            .context("Prompt response has no id")?
            .to_string())
    }

    /// Send a prompt to the daemon. Returns the prompt_id.
    #[allow(dead_code)]
    pub async fn prompt(&self, content: &str) -> Result<String> {
        Self::prompt_with(&self.base_url, &self.bearer_token, content).await
    }

    /// Cancel the current turn.
    #[allow(dead_code)]
    pub async fn cancel_turn(&self) -> Result<()> {
        let snapshot = crate::sync::snapshot(&self.base_url, &self.bearer_token).await?;
        if snapshot["turn"]["prompt_id"].is_string() {
            crate::sync::mutate(&self.base_url,&self.bearer_token,"/turn/cancel",serde_json::json!({"prompt_id":snapshot["turn"]["prompt_id"],"generation":snapshot["turn"]["generation"]}),&snapshot).await?;
        }
        Ok(())
    }

    /// Respond to an interrogative (permission request).
    #[allow(dead_code)]
    pub async fn respond_interrogative(&self, interrogative_id: &str, granted: bool) -> Result<()> {
        crate::sync::respond(
            &self.base_url,
            &self.bearer_token,
            interrogative_id,
            serde_json::json!({"kind":"permission_answer","granted":granted}),
        )
        .await
    }

    /// Check daemon health by GET /health. Returns the HTTP status code.
    pub async fn health(&self) -> Result<u16> {
        let client = reqwest::Client::new();
        let url = format!("{}/health", self.base_url);
        let resp = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .send()
            .await
            .with_context(|| format!("Failed to connect to {} (transport error)", url))?;
        Ok(resp.status().as_u16())
    }

    /// Fetch the daemon's session state snapshot (the subset we need).
    #[allow(dead_code)]
    pub async fn fetch_session_state(&self) -> Result<SessionStateSnapshot> {
        serde_json::from_value(crate::sync::state(&self.base_url, &self.bearer_token).await?)
            .context("Invalid model state")
    }

    /// Switch the active model by POSTing to `/model`.
    #[allow(dead_code)]
    pub async fn set_model(&self, model_name: &str) -> Result<()> {
        let client = reqwest::Client::new();
        let url = format!("{}/model", self.base_url);
        let resp = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .json(&serde_json::json!({ "model": model_name }))
            .send()
            .await
            .context("Failed to POST /model")?;
        if !resp.status().is_success() {
            bail!("POST /model returned status {}", resp.status());
        }
        Ok(())
    }

    /// The SSE events URL for this daemon.
    #[allow(dead_code)]
    pub fn events_url(&self) -> String {
        format!("{}/events", self.base_url)
    }

    /// The bearer token used for auth.
    pub fn bearer_token(&self) -> &str {
        &self.bearer_token
    }

    /// The base URL of the daemon.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The session ID of this daemon.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// The working directory of this daemon.
    pub fn cwd(&self) -> &std::path::Path {
        &self.cwd
    }

    /// Set the MCP config dir to clean up on terminate.
    pub fn set_mcp_config_dir(&mut self, dir: PathBuf) {
        self.mcp_config_dir = Some(dir);
    }

    /// Switch the active facet by POSTing to `/facet`.
    #[allow(dead_code)]
    pub async fn set_facet(&self, facet: &str) -> Result<()> {
        let client = reqwest::Client::new();
        let url = format!("{}/facet", self.base_url);
        let resp = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .json(&serde_json::json!({ "facet": facet }))
            .send()
            .await
            .context("Failed to POST /facet")?;
        if !resp.status().is_success() {
            bail!("POST /facet returned status {}", resp.status());
        }
        Ok(())
    }

    /// Fetch the full daemon state snapshot.
    pub async fn fetch_daemon_state(&self) -> Result<serde_json::Value> {
        crate::sync::state(&self.base_url, &self.bearer_token).await
    }

    /// Terminate the daemon gracefully.
    pub async fn terminate(&mut self) {
        let client = reqwest::Client::new();
        let url = format!("{}/terminate", self.base_url);
        let _ = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .timeout(Duration::from_secs(2))
            .send()
            .await;

        // Give it a moment then kill
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(child) = &mut self.child {
            let _ = child.kill().await;
        }

        // Clean up the temp MCP config directory (may contain auth tokens).
        if let Some(ref mcp_dir) = self.mcp_config_dir {
            if let Err(e) = std::fs::remove_dir_all(mcp_dir) {
                debug!(path = ?mcp_dir, error = %e, "Failed to clean up MCP config dir");
            } else {
                debug!(path = ?mcp_dir, "Cleaned up MCP config dir");
            }
        }

        // Clean up the daemon's temp directory (credential file and logs).
        // The credential file contains a bearer token, so remove it even if
        // the daemon process was already killed.
        if let Some(dir) = self.temp_dir.take()
            && let Err(e) = dir.close()
        {
            debug!(error = %e, "Failed to clean up daemon temp dir");
        }

        info!(session_id = %self.session_id, "Daemon terminated");
        self.lease.take();
    }
}

impl Drop for DaemonHandle {
    fn drop(&mut self) {
        // Best-effort synchronous kill if async terminate wasn't called
        if let Some(child) = &mut self.child {
            let _ = child.start_kill();
        }
    }
}

async fn poll_startup(startup_paths: &[PathBuf], child: &mut Child) -> Result<u16> {
    loop {
        if let Some(status) = child.try_wait().context("Failed to check daemon process")? {
            bail!(
                "Polytoken daemon exited during startup with status: {}. Enable RUST_LOG=debug for daemon stderr.",
                status
            );
        }
        for startup_path in startup_paths {
            if let Ok(content) = std::fs::read_to_string(startup_path)
                && let Ok(startup) = serde_json::from_str::<StartupJson>(&content)
            {
                match startup.state.as_str() {
                    "ready" => {
                        if let Some(port) = startup.port {
                            return Ok(port);
                        }
                        bail!("startup.json has no port field");
                    }
                    "failed" => {
                        let msg = startup
                            .message
                            .unwrap_or_else(|| "unknown error".to_string());
                        bail!("Polytoken daemon failed to start: {}", msg);
                    }
                    _ => {
                        // Still starting up
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Both layouts are daemon-owned; keep passing the legacy base to --sessions-dir
/// so the daemon can locate and migrate old ACP sessions during resume.
fn startup_paths(sessions_dir: &Path, session_id: &str) -> Vec<PathBuf> {
    let name = sessions_dir
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    vec![
        sessions_dir
            .with_file_name(format!("{name}-v1"))
            .join(session_id)
            .join("startup.json"),
        sessions_dir.join(session_id).join("startup.json"),
    ]
}

/// Returns the ACP-specific persistent sessions directory.
///
/// This is `~/.local/share/polytoken/sessions-acp/` (XDG_DATA_HOME fallback).
///
/// This is deliberately **separate** from the default `sessions/` directory
/// used by the polytoken TUI, so that ACP-managed daemons are invisible to
/// `polytoken attach`, `polytoken sessions`, and `polytoken continue`. See the
/// comment at the call site in `spawn_with_session_id` for the full rationale.
///
/// Session history still survives across polytoken-acp process restarts here
/// so that `--resume` can find the saved `log.jsonl`.
pub(crate) fn acp_sessions_dir() -> PathBuf {
    let data_dir = std::env::var("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::var("HOME")
                .map(|h| PathBuf::from(h).join(".local").join("share"))
                .unwrap_or_else(|_| PathBuf::from("~/.local/share"))
        });
    data_dir.join("polytoken").join("sessions-acp")
}

/// Generate a session ID in polytoken's required format:
/// `{6 Crockford base32 chars}-{word}`
/// Crockford base32 excludes I, L, O, U to avoid confusion.
fn generate_session_id() -> String {
    const CROCKFORD: &[u8] = b"0123456789abcdefghjkmnpqrstvwxyz";
    const WORDS: &[&str] = &[
        "acp", "junky", "river", "delta", "alpha", "forge", "nova", "echo", "flux", "gamma",
        "halo", "iris", "jolt", "keen", "lunar", "mega", "nimbus", "opal", "pulse", "quill",
    ];
    let mut rng = rand::thread_rng();
    let prefix: String = (0..6)
        .map(|_| {
            let idx = rng.gen_range(0..CROCKFORD.len());
            CROCKFORD[idx] as char
        })
        .collect();
    let word = WORDS[rng.gen_range(0..WORDS.len())];
    format!("{}-{}", prefix, word)
}

fn generate_token() -> String {
    // Polytoken requires the credential token to be valid base64url.
    // Generate 48 random bytes and encode as base64url (no padding).
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..48).map(|_| rng.r#gen()).collect();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes)
}
