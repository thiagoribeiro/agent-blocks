# Agent Blocks

Agent Blocks is the reference implementation of [BRIAN](https://github.com/thiagoribeiro/brian),
the Block-based Rich Interface Notation: stateful, auditable UI blocks (approvals,
checklists, polls, forms, workflows, agent status, monthly bills) carried as ordinary
chat events.

It runs next to a [Nimlet](https://github.com/thiagoribeiro/nimlet) chat server as an
AppService. Clients such as [NimChat](https://github.com/thiagoribeiro/nimchat) render the
blocks and send user interactions back as `m.plugin.command` events.

```text
 NimChat ──m.plugin.command──▶ Nimlet ──AppService webhook──▶ router ──▶ plugin service
    ▲                            ▲                              │            │
    └──── m.plugin.state.updated ┴──────── publish ◀────────────┴── Trama ◀──┘
```

- **router** (`cmd/router`) is stateless. It authenticates Nimlet's webhook, forwards
  commands to the owning plugin, serves the runtime catalog, and publishes results. It
  never stores instances or runs domain actions.
- **plugin-server** (`cmd/plugin-server`) runs one plugin per process, with its own
  PostgreSQL schema, token and pool. It validates commands, applies transitions and keeps
  an operation journal. Results are published durably through a
  [Trama](https://github.com/thiagoribeiro/trama) workflow.
- **room-admin** (`cmd/room-admin`) is an optional internal adapter that runs a fixed
  allowlist of `nimlet` admin CLI operations. Shared surfaces and membership checks use it.

Plugins: `approval`, `workflow`, `form_stateful`, `checklist`, `poll`, `agent_status` and
`monthly_bills` (experimental). `workflow` tracks real Trama/RiteSmith executions and only
supports `refresh`.

## Quick start

Requirements: Go 1.26+, PostgreSQL, a running Nimlet with an AppService registered for the
router, and Trama for result publication.

```sh
go build ./...
go test ./...                                     # domain, catalog, HTTP boundaries
BRIAN_TEST_DSN=postgres://... go test -race ./...   # adds persistence, restart, concurrency
```

Use a dedicated test database for `BRIAN_TEST_DSN`: the tests clean their own fixtures.

Container images: `Dockerfile` (router), `Plugin.Dockerfile` (one image for every plugin)
and `RoomAdmin.Dockerfile` (needs a Nimlet image for the `nimlet` binary).
`deploy/compose.brian.yaml` is a base for adding the router and plugins to an existing
stack on a private network. It publishes no ports.

## Configuration

### Router

| Variable | Description |
|---|---|
| `PORT` | Listen port (default `4000`). |
| `NIMLET_URL` | Nimlet base URL. |
| `HS_TOKEN` | Token Nimlet sends on AppService webhooks. |
| `AS_TOKEN` | AppService token used to publish events. |
| `AGENT_BLOCKS_INTERNAL_TOKEN` | Token for the internal API (catalog, instances, status updates). |
| `PLUGINS_FILE` | JSON list of `{name,url,token}`; see `plugins.example.json`. Each plugin needs a distinct token. |

`scripts/render-plugin-config.py` writes `deploy/plugins.json` with mode 0600 from
`BRIAN_<PLUGIN>_TOKEN` (and optional `BRIAN_<PLUGIN>_URL`) without printing secrets.
Never commit tokens or the generated file.

### Plugin service

| Variable | Description |
|---|---|
| `BRIAN_PLUGIN` | Plugin name this process runs. |
| `PLUGIN_TOKEN` | Token shared with the router for this plugin only. |
| `PLUGIN_DB_DSN` | Plugin database (schema `brian_<plugin>`). |
| `PLUGIN_PUBLIC_URL`, `PLUGIN_HTTP_ADDR` | URL the workflow engine calls back on; listen address (default `:4010`). |
| `AGENT_BLOCKS_URL`, `TRAMA_URL` | Router and Trama URLs. |
| `RITESMITH_URL` | Required by `workflow` for RiteSmith executions. |
| `NIMLET_URL`, `NIMLET_CONSUMER_TOKEN` | Normal (non-AppService) session of a service user that is a member of the tracked rooms. Used for `/sync`, history replay and delegation checks. |
| `BRIAN_DELEGATING_AGENT_IDS` | Comma-separated user IDs of agents allowed to delegate commands (empty by default). |
| `BRIAN_PUBLISHER_IDS` | Trusted publisher user IDs (status and sharing). |
| `PLUGIN_START_CURSOR` | Optional opaque sync cursor for the first start; see below. |
| `BRIAN_SHARING_ENABLED`, `BRIAN_SURFACE_NAMES_ENABLED` | Feature switches for shared surfaces and renaming. |
| `ROOM_ADMIN_URL`, `ROOM_ADMIN_TOKEN` | room-admin endpoint, required for sharing and status membership checks. |

Without `NIMLET_CONSUMER_TOKEN`, the webhook fast path works, but there is no sync, replay
or delegation. The plugin logs a warning at startup. The AppService publisher must also be
a member of the rooms.

### room-admin

See `deploy/room-admin.env.example`. The CLI subprocess only receives
`NIMLET_ADMIN_DB_DSN`, `NIMLET_ADMIN_OPERATOR` and `PATH`.

## Internal API

Every route except `/health` requires a token.

| Route | Auth | Purpose |
|---|---|---|
| `POST /events`, `PUT /transactions/{txn}` | `HS_TOKEN` | Nimlet AppService webhook. |
| `GET /catalog` | internal | Installed catalog with resolved schemas. |
| `POST /plugins/{plugin}/instances` | internal | Idempotent creation. |
| `GET /plugins/{plugin}/instances/{id}` | internal | Authoritative state and view. |
| `POST /plugins/{plugin}/instances/{id}/updates` | internal | Runtime-owned `agent_status` updates. |
| `GET /plugins/{plugin}/operations/{id}` | internal | Authoritative journal entry. |
| `POST /plugins/{plugin}/operations/{id}/publish` | per-operation token | Workflow step. Fetches the result from the plugin and never accepts caller-supplied views. |

The router only acknowledges a command after the plugin has persisted the transition and
its result. Every publication uses a stable transaction ID. A command can arrive through
the webhook, sync or replay, and the event ID deduplicates all three.

### Result publication

Each operation runs a two-task Trama workflow: publish the result, then mark the operation
complete. The workflow definition carries an operation token derived as
`HMAC-SHA256(PLUGIN_TOKEN, "operation:" + id)`, which only authorizes those two calls for
that operation, so the plugin token is never stored in Trama. For workflows created by
older versions, the router and plugin also accept the plugin token. Workflows whose retries
are exhausted are re-admitted by the plugin's recovery cycle. Only the eight most recent
execution IDs are retained.

## Delegated commands

An agent publishes as itself, with `delegation` pointing to the human message
(`request_event_id`, `request_room_seq`) and a required `expected_revision`. Before the
transaction, the runtime checks:
- the agent allowlist;
- the command;
- the origin message, through Nimlet history: room, precedence, and that its author owns
  the card.

The revision is checked inside the per-instance transaction. The journal records the agent,
the requester and the origin. A replay returns the previous result. History outages stay
retryable, while an invalid origin or a conflict is a final rejection. Webhook, sync and
reconciliation apply the same rules.

Checklist, poll and form accept the limited command set from the catalog. Workflow only
accepts `refresh`, and approval never accepts delegated decisions. See
[BRIAN spec 11](https://github.com/thiagoribeiro/brian/blob/main/spec/11-agent-delegation.md).

Only one instance is serialized per transition. There are no table locks or distributed
transactions.

## Operations

- `brian_<plugin>.operations WHERE NOT done` lists pending results.
- `executions` points at Trama executions.
- `brian_<plugin>.cursors` holds the sync cursor and per-room replay positions.
- Logs include plugin, operation and instance, never tokens.
- Restarting the router changes nothing. Restarting a plugin resumes its journal.
- Admission and reconciliation limits: 50 pending operations, and 500 events per room per
  30 s cycle. Rooms whose instances can no longer change leave the sweep.

**`agent_status` publication queue.**
- A failed publication stays pending and is retried with a durable backoff of 1–30 s. One
  example is a room where the AppService is not a member.
- It only blocks later operations of the same instance.
- Each attempt keeps the operation ID, payload and idempotent transaction, and must get an
  HTTP confirmation before it is marked `done`.
- Never clear pending entries to unblock the queue.
- See [docs/resource-recovery.md](docs/resource-recovery.md) for suspension, resume and
  explicit history replay.

**First start and cursors.**
- `PLUGIN_START_CURSOR` can set the sync cutoff so old commands are not reprocessed. It is
  an opaque `next_batch` issued to the consumer account; empty starts from the beginning.
- Once a durable cursor exists, it always wins over this variable.
- Instances replay from their own `source_pos`, the `room_seq` of their origin message.

**Upgrading stores created with numeric cursors.**
- The store converts the cursor column to text and drops replay cursors.
- The runtime refuses to use a numeric sync cursor until you convert it with
  `nimlet user sync-cursor <account> --at <value>`, because restarting from zero would
  re-run old commands.
- `rehash-journal` and `rewind-replay` are one-shot `plugin-server` subcommands for
  upgrades and recovery.

## Shared surfaces

Shared surfaces, transfers and monthly bills are described in
[docs/surfaces-v3.md](docs/surfaces-v3.md).

## License

[Apache License 2.0](LICENSE).
