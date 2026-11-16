# polytoken-acp

An [Agent Client Protocol (ACP)](https://agentclientprotocol.com/) server shim for the [Polytoken](https://docs.polytoken.dev/introduction/) daemon.

> I have to be clear whenever this comes up: 0% guarantee on any API stability for things like this. > the polytoken API is a private surface until 1.0

## Current compatibility

Updated for **Polytoken 0.9.8's versioned client protocol** on 2026-11-16. Supports **ACP v2 (draft)** when requested by the client, including T3 Code's prompt/state lifecycle, and retains ACP v1. The Rust ACP SDK is pinned to 5.3.0 because v2 is still evolving. See the [protocol implementation](docs/protocol.md) and [compatibility audit](docs/compatibility.md) for validation and limits. Older daemon versions are not supported by this implementation.

Clients can opt into the [plugin-surface extension](docs/surfaces.md) for session-bound discovery, HTML retrieval, and cancellable HTTP/SSE streaming. Its [JSON Schema](docs/surfaces.schema.json) and [example exchanges](docs/surfaces.examples.json) define the client contract. Rendering, sandboxing, and Datastar injection belong to the client; actual T3 rendering and HTTP callbacks against a loaded plugin remain unverified.

## What It Does

`polytoken-acp` bridges ACP (JSON-RPC over stdio) to the Polytoken daemon's HTTP/SSE API. This allows Paseo — or any ACP-compatible editor — to drive Polytoken as an agent without the TUI.

```
Paseo  ──stdio JSON-RPC──>  polytoken-acp (Rust binary, ACP Agent)
                               │
                               ├── spawns polytoken daemon (one per ACP session)
                               ├── GET /sync + /daemon-state → state and catalog
                               ├── POST /prompt + operation_context → {prompt_id}
                               ├── GET /events?after=<signed cursor> → ordered ACP updates
                               ├── GET /history?checkpoint=... + /attachments/{id} → replay
                               └── correlated question answers and targeted cancellation
```

## Building & Installing

```bash
cargo build --release          # build to target/release/polytoken-acp
cargo install --path . --force # install to ~/.cargo/bin/polytoken-acp
```

`cargo build` only writes to `target/release/`. The ACP client (e.g. Paseo) spawns the **installed** binary — typically `~/.cargo/bin/polytoken-acp` or a symlink to it — so you must run `cargo install --path . --force` after pulling changes or you'll keep running the old binary. Starting a new session is not enough; the client re-spawns the same stale shim each time.

### macOS: do not `cp` to install

On macOS, copying the release binary directly with `cp` (e.g. `cp target/release/polytoken-acp ~/.cargo/bin/`) results in a process that is immediately killed (exit code 137 / SIGKILL). This is caused by the `com.apple.provenance` extended attribute that macOS attaches to manually-copied binaries. Use `cargo install --path . --force` instead, which handles code signing correctly.

### Prerequisites

- A recent stable Rust toolchain (edition 2024; verified with Rust 1.93 on Windows and 1.96 in WSL)
- The `polytoken` binary on your `PATH` (validated against 0.9.8)

## Paseo Configuration

Add the following to `~/.paseo/config.json`:

```json
{
  "agents": {
    "providers": {
      "polytoken": {
        "extends": "acp",
        "label": "Polytoken",
        "command": ["polytoken-acp"]
      }
    }
  }
}
```

Then launch Paseo, select "Polytoken" as the provider, and start a session.

## How It Works

1. **Paseo spawns `polytoken-acp`** as a subprocess and communicates over stdio using JSON-RPC (ACP).
2. **On `session/new`**, the shim spawns a `polytoken daemon` process for the session's working directory, using a random port and credential file. The daemon uses a dedicated sessions directory (`~/.local/share/polytoken/sessions-acp-v1/` in 0.9.8), separate from the default `sessions/` used by the polytoken TUI — see [Session Directory Isolation](#session-directory-isolation) below.
3. **On `session/prompt`**, the shim takes an authenticated sync checkpoint, submits the text with a correlated operation ID, and consumes ordered records from that checkpoint. ACP v1 holds the prompt response until completion. ACP v2 returns the inserted user message's `messageId` immediately, then reports `running`, `requires_action`, and terminal `idle` states through notifications.
4. **Permission requests** are forwarded through `session/request_permission`. Structured questions use native `elicitation/create` when the client advertises form support, otherwise `_polytoken/ask_user_question`. Answers carry daemon operation correlation.
5. **On `session/cancel`**, the shim targets the accepted prompt's epoch, prompt ID, and observed generation, so a delayed cancellation cannot stop a later turn.
6. **On disconnect** (stdin EOF), the shim terminates all daemon processes.

### Session Directory Isolation

ACP-managed daemon sessions are registered in a **separate** directory from the polytoken TUI:

| | Directory |
|---|---|
| **ACP shim** (`polytoken-acp`) | `~/.local/share/polytoken/sessions-acp-v1/` |
| **TUI** (`polytoken new` / `attach` / `continue`) | `~/.local/share/polytoken/sessions-v1/` (0.9.8 default) |

**Why this matters:** The polytoken TUI discovers live and resumable sessions by scanning `sessions/`. If an ACP-managed daemon were registered there, a user could attach a TUI to the same daemon the ACP shim is driving. When the ACP connection ends (Paseo window closed, stdin EOF, or process exit), the shim's cleanup code terminates its daemon — which would silently kill the TUI's SSE stream with confusing "SSE errors."

By using a separate directory, ACP sessions are invisible to `polytoken sessions`, `polytoken attach <id>`, and `polytoken continue`. ACP resume (`session/load`) still works because the shim always passes `--sessions-dir sessions-acp` so `--resume` finds the saved history.

The shim passes the legacy `sessions-acp` base to `--sessions-dir`; 0.9.8 derives the sibling `sessions-acp-v1` tree and controls migration on resume. Inspect current startup/state/history files under `sessions-acp-v1/<session_id>/`. The shim also checks the legacy startup path. ACP v1 resumes with `session/load` or `session/resume`; ACP v2 uses `session/resume`, with optional `replayFrom: {"type":"start"}`.

### Event Translation

The shim consumes the ordered sync stream, then adapts its public records to the negotiated ACP version:

| Ordered daemon record | ACP action |
|---|---|
| `stream_block_started`, `stream_block_delta` | Text/thought output; v2 publishes stable message upserts |
| `history_appended` | Checkpointed public history supplies tool calls and structured results |
| `interrogative_registered` | Permission, native elicitation, or Polytoken extension request |
| `jobs_changed`, `subagent_changed` | Native child updates when supported; ordinary job tools/results otherwise |
| `title_changed` | `session_info_update` |
| `model_changed`, `catalog_published`, `facet_changed` | Refresh configuration from matching sync/catalog snapshots |
| `monitor_changed` | v1 mode update; v2 permission configuration update |
| `todos_changed` | v1 plan; v2 plan upsert |
| `turn_completed`, `turn_failed`, `slot_released` | Reconcile durable output, then completion or error |
| Reset or expired cursor | Same-epoch resnapshot/reconciliation; ambiguous outcomes require reload |
| `history_rewritten`, epoch change | Explicit reload-required error |
| Unknown future record | Ignore without interpreting opaque revisions |

## Testing

### Unit Tests

```bash
cargo test
```

Unit tests cover:
- SSE event deserialization (all handled event types + `#[serde(other)]` catch-all)
- Event-to-ACP translation (text delta, thinking/reasoning deltas, tool call, tool result, message complete, turn cancelled, interrogative)
- `message_start`, `model_changed`, `facet_changed`, `session_title_changed` deserialization and translation
- `tool_result` with `content_full` fallback
- Non-permission interrogative types (confirmation, etc.)
- Permission translation (options construction, outcome→granted mapping)
- `ask_user_question` event deserialization (payload with questions, options, modes) and translation
- `ask_user_question` payload serialization for ext_method forwarding
- Content block text extraction
- MCP server config conversion (Stdio, Http, Sse-skipped, multiple servers, empty list)

### Integration Tests

Integration tests require the `polytoken` binary on PATH and LLM credentials:

```bash
cargo test -- --ignored
```

For a reproducible real-daemon test without API credentials, run [tests/compatibility.py](tests/compatibility.py) as described in the [audit](docs/compatibility.md#validation).

[tests/surfaces.py](tests/surfaces.py) validates the optional surface extension against its published JSON Schema using a mock daemon, including HTTP bytes, flow control, cancellation, and reconnect. It also supports real-daemon empty-catalog discovery. See the [surface contract](docs/surfaces.md#errors-and-verification) for commands and verification limits.

## Troubleshooting

### Debug logging

The shim logs structured JSON to stderr (stdout is reserved for JSON-RPC). Set the `RUST_LOG` environment variable:

```bash
# Default: conversation-level logging (prompts, tool calls, permissions, turn events)
RUST_LOG=info polytoken-acp

# Include per-event detail: every daemon SSE event and ACP notification
RUST_LOG=debug polytoken-acp

# Shim logs only (suppress daemon stderr)
RUST_LOG=polytoken_acp=info polytoken-acp

# Verbose conversation logging only
RUST_LOG=polytoken_acp::conv=debug polytoken-acp
```

Each log line is a JSON object with fields like `timestamp`, `level`, `target`, `fields`, and `message`. The conversation target (`polytoken_acp::conv`) emits structured events:

| `message` field | Description | Key structured fields |
|---|---|---|
| `prompt_start` | User prompt forwarded to daemon | `session_id`, `prompt_len`, `prompt_preview` |
| `daemon_event` | Daemon SSE event received and translated | `event_type`, `summary` |
| `acp_notification` | ACP session update sent to editor | `update_type` |
| `turn_end` | Assistant turn completed | `prompt_id` |
| `turn_cancelled` | Turn was cancelled | `prompt_id` |
| `permission_request` | Permission interrogative forwarded to client | `interrogative_id`, `question` |
| `permission_response` | Client's permission answer relayed to daemon | `interrogative_id`, `granted` |
| `interrogative_request` | Non-permission interrogative forwarded | `interrogative_id`, `interrogative_type`, `question` |
| `interrogative_response` | Client's interrogative answer relayed | `interrogative_id`, `interrogative_type`, `granted` |
| `ask_user_question` | `ask_user_question` event forwarded via ext_method | `interrogative_id`, `question_count` |
| `cancel` | Client cancelled the turn | `session_id` |

At `debug` level, the `daemon_event` lines include an `event_type` (e.g. `tool_call`, `content_block_delta`) and a `summary` with key fields and content previews (up to 80–120 chars). Example:

```json
{"timestamp":"2025-01-01T12:00:00.123Z","level":"DEBUG","target":"polytoken_acp::conv","fields":{"event_type":"tool_call","summary":"prompt_id=abc call_id=call_1 name=read_file input={\"path\":\"/tmp/test\"}"},"message":"daemon_event"}
```

### Daemon process issues

Each ACP session spawns a `polytoken daemon` process. If the daemon fails to start:

1. Check that `polytoken` is on `PATH` and `polytoken --version` works.
2. Check the shim's stderr output for error messages.
3. The daemon's temp directory is under `$TMPDIR/polytoken-acp-<random>/`. It is removed when startup fails or the session closes; set `RUST_LOG=debug` to capture daemon stderr during startup.

### Still seeing old modes (e.g. plan/execute) after pulling changes

The ACP client spawns the **installed** binary, not `target/release/`. If you only ran `cargo build`, the installed shim at `~/.cargo/bin/polytoken-acp` (or wherever the client resolves it on `PATH`) is still the old one. Reinstall:

```bash
cargo install --path . --force
```

Then fully quit and relaunch the ACP client (don't just open a new session tab) so it re-spawns the updated shim.

### stdout pollution

The shim's own logging goes to stderr only. The daemon child process's stdout is piped and drained. If you see JSON-RPC errors, ensure nothing else writes to stdout.

## Limitations

- **MCP server forwarding**: Client servers are forwarded through a private temporary `--project-config-dir`, merged with the user's global daemon configuration. Stdio and streamable HTTP are supported. The daemon has no SSE transport: v2 rejects SSE configuration explicitly; v1 skips it with a warning. See [MCP Server Forwarding](docs/mcp-servers.md).
- **Prompt content**: ACP v2 forwards native images and preserves embedded text/blob resources. Binary resources retain their base64 data and provenance. Audio is unsupported by the daemon and is not advertised. V1 retains image/audio placeholders; embedded resource contents are preserved in its text prompt.
- **`ask_user_question` via ext_method**: The daemon's `ask_user_question` events are forwarded to the ACP client via the `_polytoken/ask_user_question` extension method. The client must implement `ext_method` and return a JSON object with an `answers` array (each answer has `question_id`, `selected_option_ids`, and/or `free_text`). If the client does not support the extension or returns no answers, the interrogative is cancelled on the daemon so the agent can proceed.
- **Non-permission interrogatives**: `confirmation`, `capability`, and `goal_proposal` interrogatives are mapped to ACP `session/request_permission` (allow/reject) and translated to the appropriate daemon response kind. `clarification` and `plan_handoff` types cannot be fully answered via ACP's permission mechanism (they need structured input) and are cancelled if rejected.
- **ACP v2 sessions**: Supports queued prompt admission, replay during work, saved-session discovery/pagination, soft deletion, settings replacement, and persistent background updates. Changing the working directory waits for existing work to finish before resume succeeds. Background shell/service jobs expose output and cancellation through ACP terminals. Native subagents use the standard child-session draft when supported; see [worker-session support and limitations](docs/subagent-sessions.md).
- **Plugin surfaces**: Optional capability negotiation enables discovery and plugin-local HTTP/SSE transport on owning sessions in v1 and v2. Streams are not replayable, and observed surface metadata cannot guarantee atomic generation-bound HTTP execution. See the [contract and client checklist](docs/surfaces.md).
- **Recovery**: Signed cursors, epoch checks, checkpointed history, attachment reads, and correlated retries are implemented. V2 resynchronizes retained history after gaps/rewrites and relaunches an unavailable owned daemon without resubmitting prompts. An unobservable terminal outcome is reported as an error. V1 retains conservative reload-required handling. See [protocol details and remaining optional capabilities](docs/protocol.md).
