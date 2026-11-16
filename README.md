# polytoken-acp

An [Agent Client Protocol (ACP)](https://agentclientprotocol.com/) server shim for the [Polytoken](https://docs.polytoken.dev/introduction/) daemon.

> I have to be clear whenever this comes up: 0% guarantee on any API stability for things like this. > the polytoken API is a private surface until 1.0

## Current compatibility

Audited against **Polytoken 0.9.8** on 2026-11-16, including exported schemas and a real-daemon test with a local mock provider. See the [compatibility audit](docs/compatibility.md) for fixes, validation, and protocol limits. Older versions are not guaranteed.

## What It Does

`polytoken-acp` bridges ACP (JSON-RPC over stdio) to the Polytoken daemon's HTTP/SSE API. This allows Paseo — or any ACP-compatible editor — to drive Polytoken as an agent without the TUI.

```
Paseo  ──stdio JSON-RPC──>  polytoken-acp (Rust binary, ACP Agent)
                               │
                               ├── spawns polytoken daemon (one per ACP session)
                               ├── POST /prompt → 202 {prompt_id}
                               ├── GET /events (SSE) → ACP session/update notifications
                               ├── POST /interrogative/{id}/respond ← ACP session/request_permission
                               └── POST /turn/cancel ← ACP session/cancel
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
3. **On `session/prompt`**, the shim forwards the prompt text to the daemon's `POST /prompt` endpoint, then connects to the daemon's SSE event stream (`GET /events`) and translates daemon events into ACP `session/update` notifications.
4. **Permission requests** (interrogative events) are forwarded to the ACP client via `session/request_permission`, and responses are relayed back to the daemon via `POST /interrogative/{id}/respond`. **`ask_user_question` events** are forwarded via the `_polytoken/ask_user_question` extension method, and answers are relayed back.
5. **On `session/cancel`**, the shim calls `POST /turn/cancel` on the daemon.
6. **On disconnect** (stdin EOF), the shim terminates all daemon processes.

### Session Directory Isolation

ACP-managed daemon sessions are registered in a **separate** directory from the polytoken TUI:

| | Directory |
|---|---|
| **ACP shim** (`polytoken-acp`) | `~/.local/share/polytoken/sessions-acp-v1/` |
| **TUI** (`polytoken new` / `attach` / `continue`) | `~/.local/share/polytoken/sessions-v1/` (0.9.8 default) |

**Why this matters:** The polytoken TUI discovers live and resumable sessions by scanning `sessions/`. If an ACP-managed daemon were registered there, a user could attach a TUI to the same daemon the ACP shim is driving. When the ACP connection ends (Paseo window closed, stdin EOF, or process exit), the shim's cleanup code terminates its daemon — which would silently kill the TUI's SSE stream with confusing "SSE errors."

By using a separate directory, ACP sessions are invisible to `polytoken sessions`, `polytoken attach <id>`, and `polytoken continue`. ACP resume (`session/load`) still works because the shim always passes `--sessions-dir sessions-acp` so `--resume` finds the saved history.

The shim passes the legacy `sessions-acp` base to `--sessions-dir`; 0.9.8 derives the sibling `sessions-acp-v1` tree and controls migration on resume. Inspect current startup/state/history files under `sessions-acp-v1/<session_id>/`. The shim also checks the legacy startup path.

### Event Translation

| Daemon SSE Event | ACP Action |
|---|---|
| `content_block_delta` (text) | `session/update` → `agent_message_chunk` |
| `content_block_delta` (thinking) | `session/update` → `agent_thought_chunk` |
| `content_block_delta` (redacted_thinking) | `session/update` → `agent_thought_chunk` (placeholder) |
| `content_block_delta` (open_ai_reasoning_summary) | `session/update` → `agent_thought_chunk` |
| `content_block_delta` (open_ai_reasoning_opaque) | Ignored (encrypted provider data) |
| `content_block_delta` (signature_delta) | Ignored (no ACP equivalent) |
| `message_start` | Ignored (no ACP equivalent needed) |
| `tool_call` | `session/update` → `tool_call` (status: pending) |
| `tool_result` | `session/update` → `tool_call_update` (status: completed/failed). Uses `content_full` when available, falls back to `content`. |
| `interrogative` (permission) | `session/request_permission` → `POST /interrogative/{id}/respond` |
| `interrogative` (confirmation, capability, goal_proposal) | `session/request_permission` → `POST /interrogative/{id}/respond` (mapped to appropriate response kind) |
| `interrogative` (clarification, plan_handoff) | `session/request_permission` → `POST /interrogative/{id}/respond` (best-effort; cancelled if unsupported) |
| `ask_user_question` | `ext_method` (`_polytoken/ask_user_question`) → `POST /interrogative/{id}/respond` |
| `session_title_changed` | `session/update` → `session_info_update` (title) |
| `facet_switch` (legacy: `facet_changed`) | Ignored (facets are slash commands, not ACP modes; mode maps to the permission monitor) |
| `permission_monitor_switch` | `session/update` → `current_mode_update` |
| `model_switch`, `model_options_changed`, `daemon_catalog_changed` | Refresh `config_option_update` from `/state` |
| `message_complete` | `session/prompt` response with `stop_reason: end_turn` |
| `turn_cancelled` | `session/prompt` response with `stop_reason: cancelled` |
| `model_error` | `session/prompt` response with `stop_reason: end_turn` |
| `harness_error`, `stream_discontinuity` | ACP prompt error |
| heartbeat / unknown | Ignored |

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

## Limitations (v1)

- **MCP server forwarding**: Paseo's `mcpServers` are forwarded to the polytoken daemon. When a client sends MCP servers in `session/new`, `session/resume`, or `session/load`, the shim converts them to polytoken's YAML config format and passes a temporary `--project-config-dir` to the daemon. The daemon merges this project-level config on top of the user's global config, so ACP-provided servers are available alongside any configured in `~/.config/polytoken/`. ACP `Stdio` servers map to polytoken's stdio transport; `Http` servers map to polytoken's streamable-HTTP transport. The `Sse` variant is **not supported** — polytoken has no SSE transport, so SSE servers are skipped with a warning. See [MCP Server Forwarding](docs/mcp-servers.md) for details.
- **Image/file/audio prompts**: The shim forwards plain text to `POST /prompt`. The current daemon also accepts inline images, but native image forwarding is not implemented in this shim. ACP prompt content blocks of type `Image`, `ResourceLink`, `Resource`, and `Audio` are converted to descriptive text placeholders (e.g. `[image: image/png, 1234 bytes]`, `[resource: file:///src/main.rs]`) so the agent knows something was attached. A warning is logged each time a non-text block is converted.
- **`ask_user_question` via ext_method**: The daemon's `ask_user_question` events are forwarded to the ACP client via the `_polytoken/ask_user_question` extension method. The client must implement `ext_method` and return a JSON object with an `answers` array (each answer has `question_id`, `selected_option_ids`, and/or `free_text`). If the client does not support the extension or returns no answers, the interrogative is cancelled on the daemon so the agent can proceed.
- **Non-permission interrogatives**: `confirmation`, `capability`, and `goal_proposal` interrogatives are mapped to ACP `session/request_permission` (allow/reject) and translated to the appropriate daemon response kind. `clarification` and `plan_handoff` types cannot be fully answered via ACP's permission mechanism (they need structured input) and are cancelled if rejected.
- **Stream recovery**: The shim uses the supported legacy `/events` and `/history` routes, without the new synchronization cursors, epochs, checkpoints, or operation correlation. See the [compatibility boundary](docs/compatibility.md#compatibility-boundary).
