# Polytoken ACP plugin surfaces, extension version 1

This optional transport exposes an existing daemon's plugin surfaces through ACP. It creates no daemon, plugin, approval, scheduler, browser, or renderer. Only daemon-owning ACP session IDs are accepted. Native ACP child-session representations are not independent plugin hosts.

The shim owns daemon discovery, credentials, observation, and HTTP lifetimes. T3 owns the iframe/webview sandbox, HTML rendering, Datastar injection, virtual `/backend/` routing, and user-facing policy. HTML, SSE, and surface metadata never enter prompts, model requests, ordinary conversation updates, or saved conversation history.

The normative machine-readable contract is [surfaces.schema.json](surfaces.schema.json), with exact initialization fragments and exchanges in [surfaces.examples.json](surfaces.examples.json). HTTP framing, limits, and lifecycle rules below are also normative. The namespace belongs to this fork and is independent of both ACP wire versions and Polytoken's protocol versions.

## Negotiation

Capability key: `io.github.koalabuttz/polytoken-surfaces`.

Client capability fragment:

```json
{
  "_meta": {
    "io.github.koalabuttz/polytoken-surfaces": { "versions": [1] }
  }
}
```

Agent capability fragment:

```json
{
  "_meta": {
    "io.github.koalabuttz/polytoken-surfaces": {
      "version": 1,
      "limits": {
        "requestBytes": 1048576,
        "htmlBytes": 262144,
        "chunkBytes": 16384,
        "initialCredits": 4,
        "activePerSession": 8,
        "activePerConnection": 32,
        "requestIdsPerSubscription": 4096,
        "ackTimeoutMs": 30000
      },
      "generationAtomicDispatch": false,
      "bodyRevisionVerified": false,
      "httpStreamReplay": false
    }
  }
}
```

On ACP v1 these fragments belong in `initialize.params.clientCapabilities` and `initialize.result.agentCapabilities`. On v2 both belong in the corresponding `capabilities` object. Existing ACP capabilities still accompany the fragment. The agent advertises its implemented version; use is enabled only when the client explicitly offers version 1. Missing markers or lists without 1 disable all extension operations. Unsupported calls return `-32601`; unsupported notifications are not emitted. Legacy optimistic `_polytoken` extension negotiation does not enable surfaces.

All methods below use this exact prefix:

```text
_io.github.koalabuttz/polytoken-surfaces/
```

## Requests and responses

These are JSON-RPC **requests**, including cancellation and acknowledgement. They require a normal JSON-RPC `id`. Their IDs are separate from plugin HTTP `requestId` values.

| Suffix | Parameters beyond `sessionId` | Result |
|---|---|---|
| `subscribe` | None | `subscriptionId`, `daemonEpoch`, `sequence: "0"`, complete current `surfaces` array |
| `unsubscribe` | `subscriptionId` | `{}`; stops observation and cancels that subscription's requests |
| `getHtml` | `subscriptionId`, `daemonEpoch`, `surface`, `requestId` | Accepted response; HTTP events subsequently carry the HTML bytes |
| `request` | Same target/ID as `getHtml`, plus `method`, `path`, optional `query`, `headers`, `bodyBase64` | Accepted response; HTTP events subsequently carry status, headers and bytes |
| `ack` | `subscriptionId`, `requestId`, `throughSequence` | `{}` |
| `cancel` | `subscriptionId`, `requestId` | `{}`; await the stream's terminal event for the relay outcome |

A target `surface` is exactly `{pluginName, surfaceId, generation, revision}` copied from the current descriptor. A descriptor also has `source: {type: "plugin_http", path: "/surface"}`. Revisions and event sequence numbers are canonical unsigned decimal **strings**, including values larger than JavaScript's safe integer range. Daemon epochs, generations, subscription IDs and request IDs are identities, not numeric counters for a client to increment.

The accepted response is exactly:

```json
{
  "requestId": "html-1",
  "observedSurface": {
    "pluginName": "demo",
    "surfaceId": "page",
    "generation": "00000000-0000-7000-8000-000000000001",
    "revision": "1",
    "source": { "type": "plugin_http", "path": "/surface" }
  },
  "generationAtomicDispatch": false,
  "bodyRevisionVerified": false
}
```

It acknowledges admission, **not successful HTTP execution**. It is queued before any HTTP event for that request. `getHtml` makes a GET of the advertised source path with `Accept: text/html`. It uses the same cancellable streaming protocol as other HTTP requests, with a smaller response limit. The shim does not execute, rewrite, inject into, or persist the HTML. T3 should check HTTP status/media type and decode/assemble the HTML before rendering.

`request` accepts GET, HEAD, POST, PUT, PATCH, DELETE and OPTIONS. Paths are plugin-local absolute paths such as `/action`; the shim constructs the owning plugin's `/plugins/{name}/http/...` destination. External URLs, authorities, traversal, encoded separators, query/fragment data in paths and excessive recursive escaping are rejected. Path limits are 4096 UTF-8 bytes; query limits are 8192 ASCII bytes. `query` excludes the leading `?` and preserves repeated parameters, order, percent-escape case and `+` bytes. Callers must encode non-ASCII query data themselves.

Request headers are `[name, value]` pairs, preserving duplicate values. There are at most 64 pairs and 32 KiB of combined names/values. The shim rejects credential, cookie, routing, browser-origin and hop-by-hop request headers: `authorization`, `proxy-authorization`, `cookie`, `cookie2`, `set-cookie`, `host`, `connection`, `proxy-connection`, `transfer-encoding`, `content-length`, `expect`, `upgrade`, `te`, `trailer`, `forwarded`, `origin`, `referer`, `x-real-ip`, and names beginning `x-forwarded-`, `x-polytoken-` or `sec-`. Other valid HTTP headers, including `datastar-request`, are permitted. The daemon bearer is supplied internally and never returned.

Response headers are `[name, valueBase64]` pairs. Values encode raw header bytes, including non-UTF-8 values. Repeated names retain value order. The same reserved headers and headers nominated by `Connection` are filtered. No cookie jar, redirect following, automatic action retry, decompression, SSE parsing or SSE reframing is performed. A received 302/404/418/500 is an ordinary HTTP response, not a JSON-RPC error. T3 must not automatically follow response locations outside its bound plugin transport.

Bodies are base64 bytes. The finite request body limit is 1 MiB. Streaming HTTP responses have no aggregate size limit; `getHtml` responses are limited to 256 KiB. Individual body events contain at most 16 KiB of decoded data. Initial response headers must arrive within 20 seconds. Quiet persistent streams are allowed; the 30-second timeout applies to consumer acknowledgements, not to ordinary periods without SSE events.

## Notifications

`surfaceEvent` params are `{sessionId, subscriptionId, daemonEpoch, sequence, event}`. Sequence starts at `"1"` after subscription's snapshot baseline. The event is one of:

```json
{ "kind": "upsert", "surface": { "pluginName": "demo", "surfaceId": "page", "generation": "00000000-0000-7000-8000-000000000001", "revision": "1", "source": { "type": "plugin_http", "path": "/surface" } } }
```

```json
{ "kind": "removed", "surface": { "pluginName": "demo", "surfaceId": "page", "generation": "00000000-0000-7000-8000-000000000001", "revision": "1", "source": { "type": "plugin_http", "path": "/surface" } } }
```

```json
{ "kind": "reset", "reason": "sync_unavailable", "surfaces": null }
```

An upsert replaces that plugin/surface pair's descriptor; a new generation replaces the old instance. A removal identifies the previously observed descriptor. A reset with an array **replaces** the full catalog. `[]` confirms no surfaces. `null` means unavailable observation, not an empty catalog. Reasons include `daemon_replaced`, `daemon_epoch_changed`, `sync_unavailable`, `reconnected`, `event_gap`, and `resynchronized`.

`httpEvent` params are `{sessionId, subscriptionId, daemonEpoch, surface, requestId, sequence, event}`. Events within each HTTP request are ordered independently:

```json
{ "kind": "headers", "status": 200, "headers": [["content-type", "dGV4dC9ldmVudC1zdHJlYW0="]] }
```

```json
{ "kind": "body", "bodyBase64": "ZGF0YToge30KCg==" }
```

```json
{ "kind": "end", "result": { "outcome": "complete" } }
```

Other terminal results are `{outcome: "cancelled", reason: "..."}` and `{outcome: "error", code: "transport_error", message: "..."}`. Error diagnostics sent to clients omit daemon addresses and credentials; internal logs retain diagnostic context. Exactly one end is emitted per admitted request. Normally headers have sequence `"0"`, body events follow, and end is last. Cancellation/failure before headers produces only end at sequence `"0"`. Completion means transport EOF and acknowledgement of body delivery; it does not mean an HTTP success status or a successful plugin action.

Body chunk boundaries have no relationship to UTF-8 characters, SSE frames, JSON objects, Datastar events or HTTP transfer chunks. Feed decoded bytes to an incremental parser/ReadableStream. Never parse each body notification as a complete SSE event.

## Flow control and cancellation

Each request starts with credit for four body events. After consuming body bytes into a **bounded** renderer stream, T3 sends `ack` with the last consumed body sequence. This cumulatively releases credit. Duplicate/older acknowledgements do nothing; acknowledging beyond the latest body event is invalid. Do not acknowledge headers or end. The sender retains its concurrency slot until the final bytes are acknowledged or a cancellation/error terminates the relay. Slow consumers pause upstream reads. Each body event must be acknowledged within 30 seconds of delivery; an independent deadline watcher enforces this even when upstream remains open and quiet. New bytes and duplicate ACKs do not extend an older chunk's deadline. With no outstanding body events, a quiet stream has no ACK deadline. Timeout produces one terminal error and releases the request's slot.

Request IDs must be unique within a subscription, including completed requests. Up to 4096 are retained, after which unsubscribe/resubscribe establishes a fresh ID namespace. The limit is not silently evicted, so an ambiguous POST cannot be accidentally resubmitted under an old ID. Cancellation of an already finished, known request is an idempotent no-op. Unknown request IDs return an error.

Cancellation aborts the transport and releases its resources; it does **not** undo plugin side effects. A completion may win a cancellation race. A transport error after dispatch leaves execution ambiguous. Neither the shim nor T3 should automatically replay mutating requests after a reset, timeout, connection loss, or plugin reload.

## Recovery and generation boundaries

Surface metadata is transient, not conversation history. The observer opens the daemon's ordinary event stream **before** taking the `/sync` surface snapshot, buffers concurrent events, and reconciles current snapshots after event invalidations. This uses the authenticated connection's existing session binding and shared snapshot/SSE helpers. It does not manufacture a replay cursor from legacy event sequence numbers. Metadata may coalesce to the latest observed catalog; this is not an archival event feed.

Snapshot reads and application are serialized across observation, HTTP preflight, and daemon replacement. A delayed older response cannot restore an obsolete catalog after a newer refresh. A preflight rechecks readiness under the refresh lock, including when it queued before observer invalidation. Requests cannot restore availability while the metadata observer is disconnected; observation must establish a fresh baseline first.

Polytoken 0.9.8 omits the optional `surfaces` field from a valid full sync snapshot when empty. That omission is decoded as `[]`. A failed snapshot read or a present malformed field is not decoded as an empty catalog.

On a stream gap/disconnect, the shim marks observation unavailable, cancels affected HTTP streams, opens a new event stream and takes a fresh snapshot. Plugin HTTP streams are independent of metadata observation and are not replayable from daemon cursors. Epoch, generation, revision changes and removals invalidate old request targets. An ordinary session close ends all owned subscriptions/requests; unsubscribe ends only that subscription. A later ACP connection must resume its owning session and subscribe again.

Before dispatch and after receiving HTTP headers, the shim checks the latest advertised descriptor. This rejects known-stale requests and responses. The inspected public API does not establish a generation precondition or body-version validator on plugin HTTP dispatch. A plugin may change between metadata reads and HTTP handling. Therefore the advertised guarantees are explicitly false: `observedSurface` is observed metadata, **not proof that those bytes or side effects belong atomically to that revision**. Do not strengthen this guarantee without a supported daemon interface and a real-plugin test.

## T3 implementation checklist

1. Offer version 1 at initialize, check the returned capability, and use only owning session IDs.
2. Subscribe and maintain the current epoch and descriptor map. Replace full snapshots on reset; treat `null` as unavailable. A lost catalog notification sequence requires unsubscribe/resubscribe and a new rendering baseline.
3. Fetch HTML with a unique request ID. Decode response header values, reconstruct bytes, check status/media type, and render under T3's sandbox policy.
4. Bind the renderer's virtual `/backend/...` operations to the selected plugin's `/...` paths. The host supplies session/plugin identities; page content must not choose a different session, plugin, daemon or URL.
5. Adapt HTTP events to a bounded response ReadableStream, propagate acknowledgements and cancellations, and let the renderer/Datastar parse the resulting bytes.
6. On invalidation, discard late frames with old subscription/epoch/generation/revision identities, cancel their requests, and remount/refetch as needed. Keep mutation errors ambiguous rather than silently retrying actions.

## Errors and verification

| JSON-RPC code | Meaning |
|---|---|
| `-32601` | Extension not negotiated or method unknown |
| `-32602` | Invalid parameters, path, method, header, body encoding, or acknowledgement |
| `-32002` | No active daemon-owning session |
| `-32070` | Unknown subscription or another subscription/setup is active |
| `-32071` | Stale or unavailable epoch/generation/revision |
| `-32072` | Surface removed |
| `-32073` | Request ID already used |
| `-32074` | Daemon observation/transport unavailable |
| `-32075` | Admission or retained-ID limit exceeded |
| `-32076` | Unknown request ID |

After admission, transport/body/flow-control failures are terminal HTTP events, not a second RPC response. HTTP error statuses remain HTTP events. Unknown/unsupported surface source types invalidate observation rather than being interpreted as external URLs.

Validated with Polytoken 0.9.8 metadata exports, Rust ACP SDK 5.3.0 and schema 1.18.0. [tests/surfaces.py](../tests/surfaces.py) provides a clearly marked **mock daemon** for HTTP, binary/header/query preservation, flow control, cancellation, stale generations, removals and reset/reconnect. It validates exchanged messages against the JSON Schema and also supports a real isolated daemon's empty-catalog exercise with `--polytoken`.

The installed environment lacks `esbuild`, `extism-js`, `wasm-opt` and `wasm-merge`, as reported by `polytoken plugin-dev doctor`. No plugin was built, installed, or approved for these tests. Dynamic HTTP callbacks against a real loaded plugin, its reload behavior, and T3/Paseo rendering remain **unverified**. The [current plugin-surface documentation](https://docs.polytoken.dev/extending-polytoken/plugin-surfaces/) defines the upstream routes and lifecycle, and [ACP extensibility](https://agentclientprotocol.com/protocol/v2/extensibility) defines capability placement and custom-method naming.

```sh
python3 -B tests/surfaces.py --shim /path/to/polytoken-acp
python3 -B tests/surfaces.py --protocol 1 --shim /path/to/polytoken-acp
python3 -B tests/surfaces.py --shim /path/to/polytoken-acp --polytoken /path/to/polytoken
```

The Python contract tests require `jsonschema` (Draft 2020-12 support).
