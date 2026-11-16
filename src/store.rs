//! Shim-owned session metadata, separate from daemon-owned history.
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

pub fn root() -> PathBuf {
    crate::daemon::acp_sessions_dir().with_file_name("acp-shim-metadata")
}

pub fn lease(id: &str) -> Result<std::fs::File> {
    lease_in(&root(), id)
}

fn lease_in(directory: &Path, id: &str) -> Result<std::fs::File> {
    if !valid_id(id) {
        bail!("Invalid session ID");
    }
    std::fs::create_dir_all(directory)?;
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
        options.mode(0o600);
    }
    let file = options.open(directory.join(format!("{id}.lock")))?;
    file.try_lock()
        .context("Session is active in another shim connection")?;
    Ok(file)
}

pub fn read(id: &str) -> Result<Option<Value>> {
    if !valid_id(id) {
        bail!("Invalid session ID");
    }
    let path = root().join(format!("{id}.json"));
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn write(id: &str, value: &Value) -> Result<()> {
    if !valid_id(id) {
        bail!("Invalid session ID");
    }
    let directory = root();
    std::fs::create_dir_all(&directory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
    }
    let mut file = tempfile::NamedTempFile::new_in(&directory)?;
    serde_json::to_writer(&mut file, value)?;
    file.persist(directory.join(format!("{id}.json")))?;
    Ok(())
}

/// `sessions --all` is the daemon's read-only discovery surface. Do not parse
/// its private log files, or use plain `sessions` (which cleans stale entries).
pub async fn discover() -> Result<Vec<Value>> {
    let mut entries = std::collections::BTreeMap::new();
    let output = tokio::process::Command::new("polytoken")
        .args(["sessions", "--all", "--format", "json", "--sessions-dir"])
        .arg(crate::daemon::acp_sessions_dir())
        .output()
        .await?;
    if !output.status.success() {
        bail!("Read-only daemon session discovery failed");
    }
    let discovered: Value = serde_json::from_slice(&output.stdout)?;
    for item in discovered
        .as_array()
        .context("Invalid daemon session list")?
    {
        if let Some(id) = item["session_id"].as_str().or_else(|| item["id"].as_str()) {
            let cwd = item["project_path"]
                .as_str()
                .or_else(|| item["project_dir"].as_str())
                .or_else(|| item["cwd"].as_str());
            if valid_id(id)
                && let Some(cwd) = cwd
            {
                let mut entry = json!({"sessionId":id,"cwd":cwd,"title":item["effective_title"].as_str().or_else(|| item["title"].as_str())});
                if item["last_activity_at"].is_string() {
                    entry["updatedAt"] = item["last_activity_at"].clone();
                }
                entries.insert(id.to_string(), entry);
            }
        }
    }
    match std::fs::read_dir(root()) {
        Ok(files) => {
            for file in files {
                let path = file?.path();
                if path.extension().is_some_and(|e| e == "json") {
                    let value: Value = serde_json::from_slice(&std::fs::read(path)?)?;
                    if let Some(id) = value["sessionId"].as_str() {
                        entries.insert(id.to_string(), value.clone());
                    }
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    Ok(entries
        .into_values()
        .filter(|v| v["deleted"] != true)
        .collect())
}

pub fn absolute_directory(value: &str) -> Result<PathBuf> {
    let path = Path::new(value);
    if !path.is_absolute() || !path.is_dir() {
        bail!("Working directory must be an existing absolute directory");
    }
    Ok(path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn session_ids_cannot_escape_the_metadata_directory() {
        assert!(valid_id("abc123-acp"));
        for value in ["", "..", "../other", "x/y", "x\\y", "C:other"] {
            assert!(!valid_id(value));
        }
    }
    #[test]
    fn a_session_has_one_live_owner_and_the_lease_is_released_on_drop() {
        let directory = tempfile::tempdir().unwrap();
        let first = lease_in(directory.path(), "abc123-acp").unwrap();
        assert!(lease_in(directory.path(), "abc123-acp").is_err());
        drop(first);
        assert!(lease_in(directory.path(), "abc123-acp").is_ok());
    }
}
