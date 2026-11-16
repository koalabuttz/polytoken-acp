//! Lossless ACP prompt conversion to Polytoken's text and inline image parts.
use anyhow::{Result, bail};
use serde_json::{Value, json};

pub fn convert(blocks: &[Value]) -> Result<Value> {
    if blocks.is_empty() {
        bail!("Prompt must not be empty");
    }
    let mut text = Vec::new();
    let mut images = Vec::new();
    for block in blocks {
        match block["type"].as_str() {
            Some("text") => text.push(
                block["text"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing text"))?
                    .to_string(),
            ),
            Some("resource_link") => {
                let uri = block["uri"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing resource URI"))?;
                text.push(format!(
                    "[Resource: {}]\n{uri}",
                    block["title"]
                        .as_str()
                        .or_else(|| block["name"].as_str())
                        .unwrap_or(uri)
                ));
            }
            Some("resource") => {
                let resource = &block["resource"];
                let uri = resource["uri"].as_str().unwrap_or("embedded");
                let mime = resource["mimeType"]
                    .as_str()
                    .unwrap_or("application/octet-stream");
                if let Some(content) = resource["text"].as_str() {
                    text.push(format!("[Embedded resource: {uri}; {mime}]\n{content}"));
                } else if let Some(blob) = resource["blob"].as_str() {
                    use base64::Engine;
                    let bytes = base64::engine::general_purpose::STANDARD.decode(blob)?;
                    if mime.starts_with("image/") {
                        images.push(json!({"media_type":mime,"data":blob}));
                        text.push(format!("[Embedded image: {uri}]"));
                    } else if let Ok(content) = std::str::from_utf8(&bytes) {
                        text.push(format!("[Embedded resource: {uri}; {mime}]\n{content}"));
                    } else {
                        text.push(format!(
                            "[Embedded resource: {uri}; {mime}; base64]\n{blob}"
                        ));
                    }
                } else {
                    bail!("Embedded resource has neither text nor blob");
                }
            }
            Some("image") => {
                let mime = block["mimeType"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing image media type"))?;
                let data = block["data"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing image data"))?;
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.decode(data)?;
                images.push(json!({"media_type":mime,"data":data}));
            }
            _ => bail!("Unsupported prompt content type"),
        }
    }
    Ok(json!({"content":text.join("\n"),"images":images}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn embedded_resources_keep_their_content_and_images_are_native() {
        let payload = convert(&[
            json!({"type":"resource","resource":{"uri":"file:///a","text":"actual file contents"}}),
            json!({"type":"resource","resource":{"uri":"file:///b","blob":"AP8=","mimeType":"application/octet-stream"}}),
            json!({"type":"image","mimeType":"image/png","data":"AA=="})]).unwrap();
        assert!(
            payload["content"]
                .as_str()
                .unwrap()
                .contains("actual file contents")
        );
        assert!(payload["content"].as_str().unwrap().contains("AP8="));
        assert_eq!(payload["images"][0]["data"], "AA==");
        assert!(convert(&[json!({"type":"audio"})]).is_err());
    }
}
