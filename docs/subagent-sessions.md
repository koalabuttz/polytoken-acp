# Native worker sessions

Implemented against the [ACP Subagent Sessions preview](https://agentclientprotocol.com/rfds/subagents), Rust SDK **5.3.0**, schema **1.18.0**, and Polytoken **0.9.8** on 2026-11-16. The [Codex ACP implementation notes](https://github.com/agentclientprotocol/codex-acp/blob/main/docs/subagent-sessions.md) were also reviewed. These are draft contracts and remain pinned.

ACP v1 clients opt in with `clientCapabilities.subagents: {}`. ACP v2 includes the contract without this marker. Clients without v1 support keep ordinary worker tool activity and the existing negotiated extensions. Standard child sessions suppress the corresponding worker-handle cards and private subagent notifications; the actual collaboration tool invocation remains normal tool activity.

## Supported behavior

- A child represents an existing daemon worker. It has no separate daemon, prompt queue, scheduler, session-list row, or session lease.
- IDs derive from the owning root session, daemon handle, and retained launch timestamp. Finishing an assignment preserves the association. Nested associations use proven immediate parents, with ancestors announced before any child traffic or participant references. Unknown parents are never attached to the root.
- Observed child text, tool calls/results, and todo plans appear on the child. Assignment and subsequent-message views use `session_message`. Incoming views come from the child's actual history; outgoing views require an observed successful collaboration tool result. Role titles do not contain assignment text. Unproven message links and sender identities remain unset.
- Absolute launch directories and observed `pushd`/`popd` changes are reported. The child's observed model is a singleton configuration selector; it grants no configuration mutation. No parent model or directory is substituted for missing child metadata.
- Current work reports `running`, `requires_action`, `idle`, or `unknown`. V2 child state and its parent association carry the same snapshot. Failure and cancellation remain distinct stop reasons. A completed job without a confirmed outcome reports idle without guessing success. V1 fallback maps failures/cancellation to its supported `failed` tool status and includes a reason; unknown/salvaged outcomes remain unconfirmed.
- Live children can advertise `session/cancel`. Cancellation targets that worker and its owned descendants, including their jobs, and leaves siblings alone. Root cancellation also cancels observed work awaited through `job_block`. HTTP acceptance does not mark work cancelled; subsequent observed job state does. Root close/disconnect terminates the owning daemon and all its resources.
- Child permissions retain their origin and existing daemon approval/correlation rules. The projection refreshes child history before forwarding a new permission, so a directory change cannot be hidden by an older capture. Historical approvals are never replayed as live controls.
- Root replay reconstructs known ancestors and available child history. Each v2 child receives `replay_complete` before the root's completion marker. Historical associations have empty capabilities; fresh cancellation grants follow successful resume. Reconnect preserves IDs and a sanitized private cache. Missing or contradictory history is labeled incomplete rather than invented.
- Direct child prompting, configuration, load/resume, close/delete, and independent terminal/tool mutations are unavailable. Ordinary parent sessions and MCP forwarding retain their existing behavior.

## Daemon surfaces and limits

| Information | Observed interface and restriction |
|---|---|
| Worker identity, status, model, immediate live parent | Authenticated `/jobs`; a missing parent field alone does not prove a root child |
| Retained launch identity and absolute cwd | Allowlisted `subagent_spawn_v2` records from authenticated `/history?offset=…&limit=…`; immutable public checkpoint history strips these launch fields |
| Assignments, text, tools, messages, directory/todo deltas | Explicitly paged `/subagent/{handle}/history`; incremental offsets plus overlapping anchors detect changes; first-message identity protects restored cached conversations |
| Parentage after reconnect | Observed spawn calls paired with their successful returned handles, plus previously observed associations in shim metadata |
| Current activity and outcomes | Ordered `subagent_changed`/`jobs_changed` invalidations, fresh job status, retained `/job/{handle}/result`, and actual `subagent_complete_v1.outcome` records; unknown/salvaged kinds are not success |
| Cancellation | `/job/{handle}/cancel`; does not offer correlated generation-bound job cancellation, unlike root turn mutation; grants expire across daemon epochs |
| Permission origin | Narrow read of `/state.pending_interrogatives.subagent_handle`; sync previews omit structured origin/answer details |

The child-history surface is not an immutable public checkpoint and does not guarantee complete retention. Recovery uses available observed history and the shim cache, validates conversation identity, and reports gaps. Histories are fetched incrementally after existing event invalidations, not continuously polled or refetched in full on each activity update. Opaque reasoning, rendered definitions, environment deltas, and provider/runtime data are excluded from the cache and protocol projection.

Older launch records without an authoritative directory cannot establish a native child view and retain the ordinary integration. Unresolved descendants wait for their ancestor information. A reused daemon handle with a different birth record cannot safely identify the current conversation: the old view becomes unobservable and new work uses ordinary activity. The shim cannot eliminate the daemon's small read/cancel race because job cancellation has no generation precondition.

When a worker must first be shown through ordinary activity, that choice is retained in shim metadata. Later metadata cannot safely replace an already displayed worker card because ACP supplies no card-removal operation. This prevents duplicate native and fallback presentation, including after reconnect.

Child text is delivered when retained in child history; per-token child streaming is not claimed. A parent's observed result tool remains its actual result view; no child reply is fabricated from a parent's result alone. Child-specific model settings beyond observable model selection, missing message links, and unavailable history are not reconstructed. Additional workspace roots are not supplied by the shim.

## Verification

The isolated [worker fixture](../tests/subagents.py) serves model responses locally, uses no hosted credentials, and runs three real workers: two concurrent roots and one nested worker. It checks native v1/v2 capability behavior, announcement ordering, attribution, a different directory/model, child permissions, targeted cancellation with a sibling surviving, replay/restart, stable IDs, and suppression of legacy worker cards. A separate mode checks cancellation of a root waiting on nested work. Rust tests cover unknown parents/cycles, successful spawn evidence, unknown/failed/cancelled outcomes, replay authority, transcript redaction, workspace/todo changes, and cancellation subtrees.

```sh
python3 -B tests/subagents.py --shim /path/to/polytoken-acp --polytoken /path/to/polytoken
python3 -B tests/subagents.py --protocol 1 --shim /path/to/polytoken-acp --polytoken /path/to/polytoken
python3 -B tests/subagents.py --protocol 1 --legacy --shim /path/to/polytoken-acp --polytoken /path/to/polytoken
python3 -B tests/subagents.py --cancel-root --shim /path/to/polytoken-acp --polytoken /path/to/polytoken
```

T3 Code and Paseo rendering/control behavior has **not** been verified. Passing protocol tests does not establish that either client displays this draft or integrates children into its own agent-management UI. No client or daemon changes are included.
