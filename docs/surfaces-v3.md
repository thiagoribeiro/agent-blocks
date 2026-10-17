# Surface evolution v3

Implemented across agent-blocks, [NimChat](https://github.com/thiagoribeiro/nimchat) and the agent connector. Nimlet, the BRIAN specification and the copied canonical schemas/catalog are unchanged.

## Contract gate and behavior

The implementation uses the existing plugin envelope, `reference`, `m.plugin.command`, state updates and rejection events. Plugin-specific management commands are runtime catalog additions. `monthly_bills` has application-owned schemas in `internal/catalog/runtime_assets`, not spec schemas. There is no new Nimlet endpoint, table, room type, event or sync protocol.

The isolated contract gate exercises rename/ACL, schema-valid references (client tests), a transfer interrupted before preparation, concurrent recovery, permanent source fencing, and three actual Nimlet accounts. The local browser suite covers real auth/invites/DM/sync/pagination/media plus rendered surface interactions.

Origins must already be conversations integrated with Brian: the AppService publisher and normal bot consumer are joined. The adapter checks this before provisioning; it never silently adds service access to a human-only conversation.

Each shared surface occupies exactly one ordinary private room named `#shared-surface:<UUID>#`. The AS owns its transport administration; humans have business owner/editor roles in plugin state. Existing humans with `join` in the source and selected additional humans are captured in `sharing_participants` before provisioning; retries reuse that durable selection. They are invited; the initiator is joined. Membership changes in the source are not mirrored later. Additional invitations in an existing shared surface are one person per operation.

Checklist and monthly bills are the transfer pilots. The exporter copies supported business state, creation time and attribution, not private transport context or source messages. Other plugin instances have no transfer control. Approval remains human controlled; running workflows are not moved.

`share` is durable preparation, not a client-side clone. The source row is fenced before external provisioning. The new instance and permanent source tombstone commit in one plugin DB transaction. Both publication and redirect use the existing operation journal and stable transaction IDs. `transfers.phase` reaches `completed` after destination/source publications finish. A transient failure leaves the source fenced and is retried. An invalid request is rejected before freezing where possible; a definitive pre-cut preparation failure restores the source. Cancellation is permitted only before the freeze. After the cut, recover the destination; never unlock the source.

Creation time in the destination comes from the original trusted UI event. The plugin searches only the authorized source stream and exports its timestamp. Without an original UI event the clients show creation unavailable instead of using the latest state update. Shared creation goes straight to the destination and does not leave an editable personal instance.

Editors can rename and edit checklist items or monthly records. Only the business owner shares, invites, removes, archives or cancels. Transport membership is checked live by the adapter before shared commands and scoped agent reads. Removal first drops the business ACL, then completes transport removal idempotently. All CLI mutations retain Nimlet's existing administrative audit.

## Interfaces

Human clients send the existing `m.plugin.command` envelope. Runtime commands include `rename`, `share`, `invite_participant`, `remove_participant`, `cancel_transfer`, `create_personal` and `create_shared` for eligible plugins. Creations use an opaque `create:<uuid>` instance placeholder and `{title,data,invitees?}` payload; they produce the actual instance/card or a navigation reference. The owner comes from the authenticated sender or validated same-room delegation, never payload.

The room-admin process exposes internal `POST /operation` with a dedicated token and a fixed operation allowlist. It runs the unchanged CLI without a shell. Its child environment contains only the operator DSN, operator identity and PATH. Its credentials are not present in plugin workers. No Docker socket is mounted. Namespace-scoped user classification uses the existing batched CLI query, and a process gate serializes CLI children to bound their pools and memory.

The router's existing internal instance GET optionally forwards `actor` and `room_id`; the plugin verifies business and current room access before returning an agent-facing view. Anonymous browser access to this internal API is not introduced.

Monthly bills use safe integer BRL cents, a surface timezone (default `America/Sao_Paulo`), immutable historical period entries, last confirmed preceding-period suggestions and last-day clamping. Access/commands materialize months idempotently; there is no background generation without activity. `record_payment` requires `confirmed:true` from a human control and is excluded from agent delegation. It records a payment, not a bank operation.

## Rollout

1. Back up the plugin and connector databases, current configs/images and cursors. Record protected-repo baselines. Do not reset cursors.
2. Build router/plugin images and `RoomAdmin.Dockerfile` for the host architecture; use the already deployed unchanged Nimlet image as its binary source. The plugin image includes timezone data.
3. Provision the private env files from `deploy/*.env.example`; use an existing administrative operator credential. Give room-admin an exclusive token. Do not put its DSN in plugin/common/client configuration.
4. Add the new monthly plugin entry with its own token to the installed runtime registry. Keep other plugin entries and tokens intact. Add the monthly bills plugin and room-admin services to the existing compose project and operate only affected services with `up -d --no-deps`. Do not recreate Nimlet, Trama or shared data services.
5. Update all plugin processes to the compatible image for rename support; enable collaboration only for checklist/monthly. Supply `BRIAN_PUBLISHER_IDS` and the normal bot consumer identity. The sidecar needs the same bot token rotation as plugin consumption; keep it running until journals are settled.
6. Deploy the connector before clients. It persists dynamically admitted shared-room bindings in its own schema. Human messages never admit a room by name; only a configured trusted publisher plus authoritative plugin binding does. No restart is needed per shared surface.
7. Enable client build flags `NEXT_PUBLIC_BRIAN_SHARING_ENABLED=true` / `EXPO_PUBLIC_BRIAN_SHARING_ENABLED=true` and matching `*_BRIAN_MONTHLY_BILLS_ENABLED=true` after services/catalog are ready. Existing public agent/publisher IDs remain required. Namespace cache scope uses `*_NIMLET_NAMESPACE` (default `default`).
8. Roll out optimistic reconciliation/creation dates, names, sharing/transfer, then monthly bills. Existing shared navigation remains available when new-sharing controls are disabled. Test with three isolated accounts before using real conversations.

`BRIAN_SURFACE_NAMES_ENABLED=false` disables runtime rename; use matching client `*_BRIAN_SURFACE_NAMES_ENABLED=false` for the control. `BRIAN_SHARING_ENABLED=false` stops new sharing while the adapter and upgraded runtime must remain available to finish admitted work. Monthly visibility is controlled by the installed registry and client monthly flag.

## Recovery and verification

Inspect `brian_<plugin>.operations WHERE NOT done`, `transfers`, `shared_creations`, `sharing_participants` and the instance's transfer/access-operation fields. A client HTTP 2xx is only transport admission. Recover the journal instead of resending with a new transaction ID. A changed publisher configuration must be coordinated across runtime, connector and both client builds.

Rollback new controls first. Retain the new runtime/adapter while any transfer or access operation is pending. Never deploy an old worker that does not enforce source tombstones over a database with moved instances. Restore whole plugin/connector backups only as a coordinated operator recovery; do not mix pre-cut source rows with post-cut destination state.

Tests: `go test ./...`; `BRIAN_TEST_DSN=<isolated-test-db> go test -race ./...`. Client tests validate canonical schemas without editing them, optimistic races, rendered sharing, mentions, payment confirmation, revocation and responsive focus views.

Sharing confirmation: personal `share` now requires an unexpired `preview_token` returned by an owner-only `preview_share` command. The preview publishes the selected human identities (all joined origin humans plus requested usernames) in `view.sharing_preview`, without provisioning a room or sending invitations. A self-only selection is rejected. Confirmation snapshots those exact identities in the same transaction that starts the transfer; subsequent origin memberships cannot expand the approved audience. Client-supplied internal preview fields are discarded. Legacy `share` without a preview is rejected without freezing the source.
