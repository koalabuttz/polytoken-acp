# Polytoken compatibility audit

Checked on 2026-11-16 against **Polytoken 0.9.8**, the stable release listed on the official [downloads page](https://docs.polytoken.dev/installation/downloads/). The locally installed WSL binary was 0.4.0-unstable.1; validation used a separate 0.9.8 Linux amd64 download, without replacing that installation or changing user configuration.

## Contracts reviewed

- [Client protocol](https://docs.polytoken.dev/reference/client-protocol/): ordered synchronization, epochs, operation correlation, history and attachment checkpoints, catalogs, and legacy compatibility.
- [Daemon authentication](https://docs.polytoken.dev/reference/daemon-auth/): credential JSON, token encoding and minimum length, private file/directory modes, and bearer authentication.
- [CLI reference](https://docs.polytoken.dev/reference/cli/): required daemon arguments and the reorganized `polytoken print` commands.
- [Application configuration](https://docs.polytoken.dev/reference/configuration/): reasoning capabilities and MCP transports/authentication.
- The release binary's `daemon --help`, `print openapi --format json`, `print event-schema --format json`, `print schema app-config --format json`, `print schema permissions-config --format json`, `print tools --format json`, and `print slash-commands --format json` exports.

The downloaded archive's SHA-256 matched the [published Linux checksum](https://dl.polytoken.dev/0.9.8/SHA256SUMS.linux): `c45524a6f5c2ab782975366f486b559806303179beb43d94ff74a596848d151a`.

## Initial compatibility fixes

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

## Current protocol migration

The shim now requires and implements the versioned client surface rather than relying on legacy event/history behavior:

- Authenticated `/sync` checkpoints and matching `/daemon-state` catalogs.
- Ordered replay from signed snapshot cursors, opaque revision deduplication, epoch checks, and explicit handling of reset/retention boundaries.
- Correlated prompt, cancellation, and interrogative operations. Ambiguous retries keep the same operation ID, window, and request; cancellation stays bound to its accepted prompt.
- Immutable checkpointed public-history pagination, capture-expiry restarts, and chunked text/JSON attachment retrieval.
- Durable tool/result translation, job/subagent results, and terminal text reconciliation.
- SDK 5.3.0 and negotiated ACP v2 draft support alongside v1. V2 implements insertion acknowledgements, stable message IDs, foreground lifecycle notifications, configuration modes, complete setup lists, optional history replay, and replay completion markers.

See [protocol implementation](protocol.md) for recovery semantics, remaining restrictions, and the two narrow 0.9.8 `/state` fallbacks.

## Validation

`cargo test --all-targets`, `cargo fmt --check`, and `cargo clippy --all-targets -- -D warnings` cover the Rust implementation. Regression cases include current event shapes, structured output, reasoning validation, opaque versus readable reasoning, and facet/mode separation.

The credential-free test [tests/compatibility.py](../tests/compatibility.py) launches the real daemon and shim with an isolated home/config/data directory, a local mock Responses API, and a local HTTP MCP server. It exercises:

- Startup with strict credential validation and authenticated state/model discovery.
- HTTP MCP config forwarding, Authorization-header conversion, discovery, and enablement.
- Explicit reasoning-effort and model changes, and permission-mode changes.
- Prompt acceptance, streamed output, and normal turn completion.
- A real `shell_exec` call (`printf compat-tool`), a forced permission approval through ACP, and the structured result.
- An `ask_user_question` tool call through both the extension and native form elicitation request/answer paths.
- Close followed by `session/load`, daemon resume, and saved transcript replay.
- Cancellation while a provider request is in flight.
- ACP v2 prompt insertion acknowledgements followed by terminal state, active-session resume with/without replay, complete setup lists, and replay completion boundaries.

The Rust suite also covers a lost mutation response followed by an identical correlated retry, mismatched operation rejection, opaque revision deduplication, history capture expiry without page mixing, chunked UTF-8 attachment hydration, recovery suffix reconciliation, divergent history rejection, and stable v2 replay upserts. These controlled fault tests do not substitute for an end-to-end daemon restart/network outage test.

The final Windows run passed **164 unit tests and two non-ignored integration/smoke tests**, plus formatting and strict Clippy checks. The isolated 0.9.8 test passed with ACP v1, ACP v2 with extension questions, and ACP v2 with native form elicitation, including rejection of unauthenticated HTTP access and confirmation that cancellation releases the daemon's turn. Eight existing account-dependent Rust tests remain ignored.

Run on Linux or macOS, with Python 3 and built shim/daemon binaries:

```sh
cargo build
python3 tests/compatibility.py \
  --shim target/debug/polytoken-acp \
  --polytoken /absolute/path/to/polytoken --protocol 1
python3 tests/compatibility.py \
  --shim target/debug/polytoken-acp \
  --polytoken /absolute/path/to/polytoken --protocol 2
python3 tests/compatibility.py \
  --shim target/debug/polytoken-acp \
  --polytoken /absolute/path/to/polytoken --protocol 2 --elicitation
```

The Rust suite was run on Windows; the real-daemon tests were run in Ubuntu WSL with Polytoken 0.9.8. T3 Code's local ACP v2 adapter tests were inspected for its initialization and insertion/state lifecycle expectations. No hosted model/provider, real account OAuth, macOS runtime, old-session migration fixture, Paseo UI, or T3 Code UI was exercised. The local provider proves the shim/daemon path without making claims about every provider backend. The installed daemon was not upgraded because repository scope excludes changes to it; all current-daemon testing used the isolated download.

## Compatibility boundary

The initial compatibility commit retained legacy event/history behavior. The subsequent migration replaces that path with the current client protocol. Reconnects retain a signed snapshot cursor and deduplicate ordered revisions; a same-epoch reset can recover durable text when the original prompt is still active. Epoch changes, history rewrites, conflicting output, and an outcome lost beyond retention fail explicitly and require reload. See [the implementation report](protocol.md) for the precise guarantees and limits.

ACP image/audio/file content still becomes text placeholders. Polytoken 0.9.8 itself accepts inline prompt images, but the shim does not advertise or forward native image input. Structured question/plan/goal presentation still depends on the ACP client's elicitation or Polytoken extension support. Native Polytoken subagents remain timeline tool calls; no Paseo-side track integration is introduced.

Older daemon versions and future releases are not guaranteed. Re-run the contract exports and the real-daemon test after an upgrade. The daemon's reported OpenAPI `info.version` is `0.1.0` even in release 0.9.8; use `polytoken --version` to identify the tested release.
