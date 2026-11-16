# Polytoken compatibility audit

Checked on 2026-11-16 against **Polytoken 0.9.8**, the stable release listed on the official [downloads page](https://docs.polytoken.dev/installation/downloads/). The locally installed WSL binary was 0.4.0-unstable.1; validation used a separate 0.9.8 Linux amd64 download, without replacing that installation or changing user configuration.

## Contracts reviewed

- [Client protocol](https://docs.polytoken.dev/reference/client-protocol/): ordered synchronization, epochs, operation correlation, history and attachment checkpoints, catalogs, and legacy compatibility.
- [Daemon authentication](https://docs.polytoken.dev/reference/daemon-auth/): credential JSON, token encoding and minimum length, private file/directory modes, and bearer authentication.
- [CLI reference](https://docs.polytoken.dev/reference/cli/): required daemon arguments and the reorganized `polytoken print` commands.
- [Application configuration](https://docs.polytoken.dev/reference/configuration/): reasoning capabilities and MCP transports/authentication.
- The release binary's `daemon --help`, `print openapi --format json`, `print event-schema --format json`, `print schema app-config --format json`, `print schema permissions-config --format json`, `print tools --format json`, and `print slash-commands --format json` exports.

The downloaded archive's SHA-256 matched the [published Linux checksum](https://dl.polytoken.dev/0.9.8/SHA256SUMS.linux): `c45524a6f5c2ab782975366f486b559806303179beb43d94ff74a596848d151a`.

## Fixes

- Pass the required `--harness-type acp` when launching a daemon.
- Discover `startup.json` in the new `sessions-acp-v1/<id>/` tree, as well as the legacy `sessions-acp/<id>/` tree. Continue passing the legacy base to `--sessions-dir` so the daemon controls migration on resume. Remove stale startup files in both trees before launching, and detect early child exit without waiting for the full startup timeout.
- Generate credentials in a fresh private temporary directory. Create the credential file exclusively with Unix mode `0600` before writing its token. The directory uses `0700`; the JSON uses version 1, the required kind, and 48 random bytes encoded as unpadded base64url. Cleanup also runs when startup fails.
- Decode `tool_result.content_full` as structured `ToolLiveDisplayContent`, retaining compatibility with old strings. Render text, blocks, image fallback text, and bounded diff previews, and preserve structured raw output. Use the same renderer for persisted `live_display` during history replay.
- Recognize `model_switch`, `facet_switch`, `model_options_changed`, and `daemon_catalog_changed`. Refresh the model/reasoning controls on model or catalog changes. A facet switch does not change the ACP permission mode, including during history replay.
- Send `reasoning_effort` explicitly to `POST /model` and validate it against the advertised reasoning choices. Offer disabling reasoning only when supported. Display `open_ai_reasoning_summary` text and skip encrypted `open_ai_reasoning_opaque` deltas.
- Recognize native compaction boundaries during history replay.
- Surface harness failures, stream discontinuities, and exhausted stream reconnects as errors instead of reporting successful completion.
- Use `polytoken print slash-commands --format json` for command discovery.
- Remove the catch-all ACP dispatch handler, which intercepted successful client replies and replaced them with errors. This broke permission approvals and extension replies with the resolved ACP SDK. Let the SDK reject unknown requests and route responses normally.
- Advertise HTTP MCP forwarding in the ACP capabilities.

## Validation

`cargo test --all-targets`, `cargo fmt --check`, and `cargo clippy --all-targets -- -D warnings` cover the Rust implementation. Regression cases include current event shapes, structured output, reasoning validation, opaque versus readable reasoning, and facet/mode separation.

The credential-free test [tests/compatibility.py](../tests/compatibility.py) launches the real daemon and shim with an isolated home/config/data directory, a local mock Responses API, and a local HTTP MCP server. It exercises:

- Startup with strict credential validation and authenticated state/model discovery.
- HTTP MCP config forwarding, Authorization-header conversion, discovery, and enablement.
- Explicit reasoning-effort and model changes, and permission-mode changes.
- Prompt acceptance, streamed output, and normal turn completion.
- A real `shell_exec` call (`printf compat-tool`), a forced permission approval through ACP, and the structured result.
- An `ask_user_question` tool call and its extension request/answer round-trip.
- Close followed by `session/load`, daemon resume, and saved transcript replay.
- Cancellation while a provider request is in flight.

The final Windows run passed 149 unit tests and two non-ignored integration/smoke tests; eight existing account-dependent tests remain ignored. Formatting and strict Clippy checks passed. The isolated 0.9.8 test passed all the flows above, including rejecting unauthenticated HTTP access and checking that cancellation releases the daemon's turn.

Run on Linux or macOS, with Python 3 and built shim/daemon binaries:

```sh
cargo build
python3 tests/compatibility.py \
  --shim target/debug/polytoken-acp \
  --polytoken /absolute/path/to/polytoken
```

The Rust suite was run on Windows; the real-daemon test was run in Ubuntu WSL with Polytoken 0.9.8. No hosted model/provider, real account OAuth, macOS runtime, old-session migration fixture, or Paseo UI was exercised. The local provider proves the shim/daemon path without making claims about every provider backend.

## Compatibility boundary

0.9.8 still exports the legacy `GET /state`, `GET /history`, and `GET /events` interfaces used by this shim. Requests without `operation_context` retain legacy behavior; empty-body turn cancellation is explicitly documented in `TurnCancelRequest`. Interrogative responses keep the flattened `{"kind": ...}` body. A wholesale protocol migration is not required for basic ACP operation.

The shim does **not** implement `GET /sync`, `GET /events?after=...`, checkpointed public-history pagination, attachment retrieval, or operation-window retry correlation. It therefore has no cursor/epoch recovery or replay-safe mutation retry guarantees. A broken legacy stream can lose output or replay updates on reconnect; an explicit discontinuity or exhausted reconnect is reported as an error. New catalog events trigger refreshes through `/state`; the shim does not use `/daemon-state` as its authoritative metadata source.

ACP image/audio/file content still becomes text placeholders. Polytoken 0.9.8 itself accepts inline prompt images, but the shim does not advertise or forward native image input. Structured question/plan/goal presentation still depends on the ACP client's elicitation or Polytoken extension support. Native Polytoken subagents remain timeline tool calls; no Paseo-side track integration is introduced.

Older daemon versions and future releases are not guaranteed. Re-run the contract exports and the real-daemon test after an upgrade. The daemon's reported OpenAPI `info.version` is `0.1.0` even in release 0.9.8; use `polytoken --version` to identify the tested release.
