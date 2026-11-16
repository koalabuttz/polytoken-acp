# Protocol implementation

Validated on 2026-11-16 with Polytoken **0.9.8** and Rust `agent-client-protocol` **5.3.0**. Polytoken's client protocol v1 and ACP's wire version are independent. This shim requires the current Polytoken sync/history/attachment/targeted-mutation/catalog features and negotiates ACP v1 or v2 over stdio. ACP v2 remains a [draft](https://agentclientprotocol.com/protocol/v2/overview); its SDK dependency is pinned exactly.

## Daemon synchronization

`GET /sync` establishes the daemon epoch, opaque signed stream cursor, selections, foreground state, catalog revision, and operation window. `GET /daemon-state` supplies the published model, reasoning, skill, tool, and MCP metadata. The shim accepts a catalog only when its epoch and revision match the sync snapshot. It does not expose credential-bearing runtime configuration to the ACP client.

A prompt takes its checkpoint **before** submission, so output produced during the HTTP request is replayable. The shim subscribes to `GET /events?after=<cursor>` and applies ordered records. A projection retained across reconnects deduplicates opaque revision strings by equality. Revisions are never parsed as numbers or incremented.

In 0.9.8, SSE `id` values are bare revisions, while the `after` parameter requires a signed cursor. The tested daemon rejects bare revisions with HTTP 400. Reconnects therefore replay from the retained signed snapshot cursor and suppress already-applied revisions. The implementation does not manufacture signed continuation cursors from SSE IDs.

HTTP 400/409/410 or a reset record triggers a fresh snapshot. If the same epoch still owns the same prompt, the shim resumes from the new cursor, suppresses potentially overlapping transient text, and reconciles the unsent suffix from durable public history at completion. A changed epoch, rewritten history, divergent durable text, or a turn that ended outside the retained stream produces an explicit error requiring reload. It never resubmits that prompt or guesses a successful outcome. Completion waits for slot release, keeping successive prompts serialized.

Prompt insertion, turn cancellation, and interrogative answers carry `operation_context` with a UUID, daemon epoch, and server-issued window. Ambiguous network errors, malformed response bodies, and server errors retry the **identical body and context**, up to three attempts. A retry does not acquire a new window or operation ID. Responses must identify the same operation and epoch. Expired windows, conflicts, and failed operations remain errors. Cancellation is bound to the accepted prompt and checks the current epoch/prompt/generation before acting.

See the authoritative [Polytoken client protocol](https://docs.polytoken.dev/reference/client-protocol/) and [daemon authentication](https://docs.polytoken.dev/reference/daemon-auth/) contracts.

## History and attachments

Transcript replay uses `GET /history?checkpoint=fresh`, then passes each opaque `next_page` token as the next checkpoint parameter. All pages must have the same capture ID. A 410 discards the entire partial capture and restarts, up to three attempts; pages from separate captures are never combined. Repeated page tokens or invalid responses fail explicitly.

Bounded text and JSON with an available attachment handle are retrieved through `/attachments/{artifact_id}` in chunks. The shim validates identity, byte offsets, lengths, base64, and UTF-8/JSON before replacing the preview. Public history supplies tool inputs/results and the final text used for stream reconciliation. Resume replay retains the existing policy of showing the active context after the last compaction/clear boundary.

Two narrow compatibility reads remain necessary with 0.9.8:

- Ordered interrogative previews omit structured question/plan payloads. The shim reads only the matching pending interrogative from `/state` when its registration arrives.
- After an explicit reasoning-effort change, `/sync.selections.effort` can remain at the model default. The shim reads `/state.active_reasoning_effort` for the current selection; reasoning choices still come from `/daemon-state`.

## ACP v2 behavior

The versioned frontend validates v2 requests and emitted session updates with SDK 5.3.0's v2 schemas, sharing the same daemon/session engine with v1 clients.

- Initialization selects v2 when the client requests version 2 or later, otherwise v1. The v2 response uses `info` and `capabilities`.
- `session/prompt` echoes the accepted user message and returns its canonical `messageId` at insertion. It does not wait for completion or return `stopReason`. Foreground updates report `running`, `requires_action` while waiting for client input, and terminal `idle` with a stop reason. A failure after insertion becomes an idle error update, without a second response to the prompt request.
- Text/thought chunks carry stable IDs derived from the daemon prompt. Entity type and ID identify separate user/agent/thought messages. History replay sends complete content upserts under those IDs, allowing repeated replay to replace content instead of appending duplicate messages.
- Permissions retain approval choices and use the v2 title/options shape. Clients advertising form elicitation receive `elicitation/create` for structured questions; other clients can use `_polytoken/ask_user_question`.
- Session setup returns complete configuration and available-command lists. Permission modes are a `permission_mode` configuration option. Plans and tool calls use v2 upserts.
- `session/resume` optionally accepts `replayFrom: {"type":"start"}`. Replay ends with `replay_complete`, followed by the current foreground state before the response. Omitting `replayFrom` skips history. Resuming an already-active session refreshes its state and can replay history.
- `session/list` lists this process's active sessions. Known saved IDs remain resumable after close or process restart, but saved-session directory discovery is not implemented.
- ACP v1 retains its delayed prompt response, modes, `session/load`, and fallback `session/resume` behavior.

This implements the prompt insertion/state lifecycle used by T3 Code's ACP v2 adapter. Validation exercises the wire contract directly; it does not claim a T3 Code UI run.

## Remaining boundaries

- ACP v2 replay supports only the start marker, not arbitrary replay cursors. Only one foreground prompt is accepted at a time. Replay during foreground work is rejected to prevent overlapping transcript updates; resume without replay can still refresh current state. Active resume requires unchanged working directory and MCP settings; close and resume to replace them.
- Native image/audio input forwarding is not implemented or advertised. V2 rejects unadvertised media; v1 retains descriptive text fallback. Resource content is represented as descriptive text. Full text/JSON attachment recovery does not imply native image rendering.
- Job/subagent results are translated to timeline tools while a foreground consumer is running. A persistent idle background observer, Outpost fleet discovery, plugin surfaces, child-session ownership, and remote terminal control are not advertised or implemented.
- A queued daemon response without an attributable prompt ID fails explicitly. The shim never guesses the identity of a drained queue item.
- Structured plan/goal interaction still depends on client elicitation or extension support. No Paseo, T3 Code, or daemon change is required or included.
- Hosted provider/OAuth flows, macOS runtime behavior, old-session migration, and actual editor UI rendering were not tested. Future daemon and draft ACP revisions require renewed contract validation.

See [compatibility validation](compatibility.md#validation) for reproducible checks.
