//! Shared rendering for live `ToolLiveDisplayContent` and saved tool results.

use agent_client_protocol::schema::v1 as acp;
use serde_json::Value;

/// Accept legacy strings as well as the current text, blocks, image and diff
/// objects. A diff preview is bounded; do not invent complete old/new files.
pub fn text(value: &Value) -> Option<String> {
    if let Some(s) = value.as_str().or_else(|| value.get("text")?.as_str()) {
        return Some(s.to_string());
    }
    if let Some(blocks) = value.get("blocks").and_then(Value::as_array) {
        return Some(
            blocks
                .iter()
                .filter_map(|b| b.get("text")?.as_str())
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }
    if let Some(image) = value.get("image") {
        return image
            .get("text_fallback")
            .and_then(Value::as_str)
            .map(str::to_string);
    }
    let diff = value.get("diff_preview")?;
    let mut rendered = format!(
        "{}\n{}\n",
        diff["summary"].as_str().unwrap_or("Diff preview"),
        diff["new_path"].as_str().unwrap_or("")
    );
    for line in diff.get("lines")?.as_array()? {
        for (key, prefix) in [("added", "+"), ("removed", "-"), ("context", " ")] {
            if let Some(s) = line.get(key).and_then(Value::as_str) {
                rendered.push_str(&format!("{prefix}{s}\n"));
            }
        }
        if let Some(h) = line.get("hunk") {
            rendered.push_str(&format!(
                "@@ -{},{} +{},{} @@\n",
                h["old_start"], h["old_len"], h["new_start"], h["new_len"]
            ));
        }
    }
    if let Some(n) = diff["omitted_line_count"].as_u64().filter(|n| *n > 0) {
        rendered.push_str(&format!("[{n} lines omitted]\n"));
    }
    Some(rendered)
}

pub fn fields(value: &Value, status: acp::ToolCallStatus) -> acp::ToolCallUpdateFields {
    let mut fields = acp::ToolCallUpdateFields::new().status(status);
    let raw = value
        .as_str()
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .unwrap_or_else(|| value.clone());
    fields = fields.raw_output(raw);
    if let Some(s) = text(value) {
        let block: acp::ContentBlock = s.into();
        fields = fields.content(vec![acp::ToolCallContent::from(block)]);
    }
    fields
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn renders_current_and_legacy_results() {
        assert_eq!(text(&json!("old")), Some("old".into()));
        assert_eq!(text(&json!({"text":"full"})), Some("full".into()));
        assert_eq!(
            text(&json!({"blocks":[{"type":"text","text":"a"},{"type":"text","text":"b"}]})),
            Some("a\nb".into())
        );
        assert_eq!(
            text(&json!({"image":{"text_fallback":"an image"}})),
            Some("an image".into())
        );
        let diff = json!({"diff_preview":{"summary":"Edited file","new_path":"file.rs","lines":[{"removed":"old"},{"added":"new"}],"omitted_line_count":2}});
        assert!(
            text(&diff)
                .unwrap()
                .contains("-old\n+new\n[2 lines omitted]")
        );
        assert_eq!(
            serde_json::to_value(fields(&diff, acp::ToolCallStatus::Completed)).unwrap()["rawOutput"],
            diff
        );
    }
}
