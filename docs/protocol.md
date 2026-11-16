# Protocol implementation

Validated on 2026-11-16 with Polytoken **0.9.8**, Rust `agent-client-protocol` **5.3.0**, and protocol schema **1.18.0**. Polytoken's client protocol v1 and ACP's wire version are independent. The SDK and draft schema are pinned exactly. This shim requires the current Polytoken sync/history/attachment/targeted-mutation/catalog features and negotiates ACP v1 or v2 over stdio. ACP v2 remains a [draft](https://agentclientprotocol.com/protocol/v2/overview).

## Daemon synchronization

`GET /sync` establishes the daemon epoch, opaque signed stream cursor, selections, foreground state, catalog revision, and operation window. `GET /daemon-state` supplies the published model, reasoning, skill, tool, and MCP metadata. The shim accepts a catalog only when its epoch and revision match the sync snapshot. It does not expose credential-bearing runtime configuration to the ACP client.

A prompt takes its checkpoint **before** submission, so output produced during the HTTP request is replayable. The shim subscribes to `GET /events?after=<cursor>` and applies ordered records. A projection retained across reconnects deduplicates opaque revision strings by equality. Revisions are never parsed as numbers or incremented.

In 0.9.8, SSE `id` values are bare revisions, while the `after` parameter requires a signed cursor. The tested daemon rejects bare revisions with HTTP 400. Reconnects therefore replay from the retained signed snapshot cursor and suppress already-applied revisions. The implementation does not manufacture signed continuation cursors from SSE IDs.

ACP v2 keeps an observer connected for each active session, including while foreground work is idle. HTTP 400/409/410 or a reset record triggers a fresh snapshot and durable entity upserts. Old revisions are discarded at an epoch boundary. Missing transient deltas are reconciled from durable history; new turns resume token updates. Rewrites synchronize retained history rather than failing the session. After repeated connection failures, the shim checks health before relaunching its unavailable owned daemon with the saved session. A healthy daemon with a projection error stays running while synchronization retries. Recovery never resubmits the interrupted prompt or guesses a successful outcome: an unobservable terminal outcome is reported as an error state. ACP v1 retains its more conservative reload-required recovery behavior.

Prompt insertion, turn cancellation, and interrogative answers carry `operation_context` with a UUID, daemon epoch, and server-issued window. Ambiguous network errors, malformed response bodies, and server errors retry the **identical body and context**, up to three attempts. A retry does not acquire a new window or operation ID. Responses must identify the same operation and epoch. Expired windows, conflicts, and failed operations remain errors. Cancellation is bound to the accepted prompt and checks the current epoch/prompt/generation before acting.

See the authoritative [Polytoken client protocol](https://docs.polytoken.dev/reference/client-protocol/) and [daemon authentication](https://docs.polytoken.dev/reference/daemon-auth/) contracts.

## History and attachments

Transcript replay uses `GET /history?checkpoint=fresh`, then passes each opaque `next_page` token as the next checkpoint parameter. All pages must have the same capture ID. A 410 discards the entire partial capture and restarts, up to three attempts; pages from separate captures are never combined. Repeated page tokens or invalid responses fail explicitly.

Native worker projection additionally allowlists authenticated launch metadata and incremental child-history pages, which public checkpoint history does not supply. Their weaker retention/identity guarantees and fallback policy are detailed in [subagent sessions](subagent-sessions.md); ordinary transcript replay continues to use the immutable public surface.

Bounded text and JSON with an available attachment handle are retrieved through `/attachments/{artifact_id}` in chunks. The shim validates identity, byte offsets, lengths, base64, and UTF-8/JSON before replacing the preview. Public history supplies tool inputs/results and final text used for stream reconciliation. ACP v2 replays **all retained history**, including content before a compaction boundary. ACP v1 retains its existing active-context replay policy.

Two narrow compatibility reads remain necessary with 0.9.8:

- Ordered interrogative previews omit structured question/plan payloads. The shim reads only the matching pending interrogative from `/state` when its registration arrives.
- After an explicit reasoning-effort change, `/sync.selections.effort` can remain at the model default. The shim reads `/state.active_reasoning_effort` for the current selection; reasoning choices still come from `/daemon-state`.

## ACP v2 behavior

The versioned frontend validates v2 requests, responses, and emitted session updates with schema 1.18.0. `src/v2.rs` owns the persistent v2 session engine; v1 retains its existing handlers. Both share daemon authentication/lifecycle, client-protocol helpers, configuration builders, and question translation. An internal SDK transport connects the frontend to these handlers; its extension methods never become requirements on ACP clients. Capability presence uses objects (`{}`), as required by v2, rather than v1 booleans.

- Initialization selects v2 when the client requests version 2 or later, otherwise v1. The v2 response uses `info` and `capabilities`.
- `session/prompt` echoes the accepted user message and returns its canonical `messageId` at insertion. It does not wait for completion or return `stopReason`. Foreground updates report `running`, `requires_action` while waiting for client input, and terminal `idle` with a stop reason. A failure after insertion becomes an idle error update, without a second response to the prompt request.
- Text/thought output uses complete message upserts with stable IDs derived from the daemon prompt. Entity type and ID identify separate user/agent/thought messages. Live and replay updates share one ordered projection, preventing duplicate text or regression when replay is ahead of the live stream.
- Multiple prompts can be admitted while work runs. Polytoken serializes model turns and queues later inputs. A drained input can acquire another daemon prompt ID; the shim persists its mapping to the original ACP insertion ID, using admission order even when public history redacts the prompt. Stored entity order preserves live placement during replay. The 0.9.8 sync queue preview can retain drained inputs, so ordered slot-release records determine foreground completion and safe workspace transitions.
- Permissions retain approval choices and use the v2 title/options shape. Clients advertising form elicitation receive `elicitation/create` for structured questions; other clients can use `_polytoken/ask_user_question`.
- Session setup returns complete configuration and available-command lists. Permission modes are a `permission_mode` configuration option. Plans and tool calls use v2 upserts.
- `session/resume` optionally accepts `replayFrom: {"type":"start"}`, including during foreground work. Replay ends with `replay_complete`, followed by current foreground state before the response. Omitting `replayFrom` skips historical replay. Unsupported future cursor types are rejected with `-32602`, as specified; arbitrary custom cursors are not required by the standard.
- Active resume replaces MCP settings through an isolated config and daemon reload. A working-directory change waits for foreground/background work to finish, then resumes the saved daemon in the new directory. Work is not cancelled to apply settings. A notice explains the pending transition, and **resume succeeds only after the new workspace is ready**. Other RPCs, permission replies and cancellation continue to be processed while setup waits.
- `session/list` discovers current and saved ACP sessions through the daemon's read-only `sessions --all` CLI and shim metadata. It supports exact directory filters, rejects relative filters and invalid cursors, and returns 100-entry pages from immutable, filter-bound captures. A connection retains up to 16 continuation captures. Unknown/expired tokens return `-32602`.
- `session/delete` closes active resources, stores a durable tombstone, and invalidates list captures so old pages cannot reintroduce a deleted session. Repeated deletion succeeds; deleted sessions are excluded from discovery and cannot be resumed through v1 or v2. Daemon history is retained on disk (soft delete).
- Embedded text and blob resources retain their contents; binary blobs are represented as base64 with provenance. Native images are forwarded through Polytoken's inline image parts and advertised in v2. Original rich user messages are retained for replay.
- The observer publishes job results and background state after foreground completion. Shell/service jobs have terminal declarations, paged output snapshots, exit status, and per-entity cancellation capabilities. `terminal/cancel` and `tool_call/cancel` target declared daemon job handles. Native subagents use the standard child-session draft when supported; see [worker-session support and limitations](subagent-sessions.md).
- ACP v1 retains its delayed prompt response, modes, `session/load`, and fallback `session/resume` behavior.

Session metadata and canonical queued-message mappings are stored privately in the sibling `acp-shim-metadata` directory. An OS file lease prevents two shim connections from simultaneously launching the same saved session; ownership is acquired before touching startup records. MCP credentials remain in private temporary config, not the index. The implementation follows the insertion/state lifecycle in T3 Code's local ACP registry adapter tests. Validation exercises the wire contract directly; it does not claim a T3 Code UI run or exhaustive ACP conformance certification.

## Remaining boundaries

- Audio input is not supported by Polytoken's prompt endpoint and is not advertised. V2 rejects unsupported media. V1 retains its image/audio text fallback; native image input is a v2 feature.
- Working-directory changes cannot move a running daemon in place; setup waits for a safe boundary. Long-running background work can delay that resume until it finishes or the client cancels it. MCP settings still merge with the user's global daemon configuration.
- Terminal display output is bounded (approximately 2 MiB per current snapshot); complete job results use the daemon's retained result surface. The shim exposes cancellation, not interactive terminal input/resizing or arbitrary process creation.
- Plugin surfaces use an optional, explicitly negotiated [extension](surfaces.md) on owning sessions in either wire version. An ordinary event subscription precedes the surface snapshot; invalidations refresh metadata and reconnects establish a new baseline. This transient catalog and its HTTP/SSE streams are separate from conversation replay. The daemon does not establish atomic generation-bound HTTP dispatch or a body-revision validator. Outpost fleet discovery is not advertised. Native worker-session support and its daemon/client limits are documented in [subagent sessions](subagent-sessions.md).
- A lost terminal outcome cannot be reconstructed from a transcript alone. Recovery preserves available output and reports uncertainty rather than inventing success. Process-local pagination cursors expire on reconnect or capture eviction.
- Structured plan/goal interaction still depends on client elicitation or extension support. No Paseo, T3 Code, or daemon change is required or included.
- Hosted provider/OAuth flows, macOS runtime behavior, old-session migration, and actual editor UI rendering were not tested. Future daemon and draft ACP revisions require renewed contract validation.

See [compatibility validation](compatibility.md#validation) for reproducible checks.
