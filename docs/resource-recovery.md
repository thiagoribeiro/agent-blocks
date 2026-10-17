# Publication and history recovery

Publication errors retain their upstream HTTP status and return a secret-free JSON
object with `upstream_status`, `code` and `retryable`. Network errors, HTTP 408,
429 and 5xx remain retryable. Other HTTP failures suspend activity telemetry.
Suspension does not acknowledge delivery or delete any journal entry.

Before creating an activity card, the status plugin asks the existing room-admin
adapter for membership. The human owner and an explicitly configured publisher
must already be joined. This operation never changes membership. The status
service needs `ROOM_ADMIN_URL`, `ROOM_ADMIN_TOKEN`, and `BRIAN_PUBLISHER_IDS`;
supply them through a private env file.

## Resume a suspended room

After fixing access deliberately, POST `/status-publication/resume` directly to
the agent_status plugin on the private network, authenticated with its plugin
token, with `{"room_id":"...","owner_id":"..."}`. The endpoint rechecks access
before clearing suspension and wakes publication. It preserves the original
operation IDs, payloads, ordering and transaction IDs. Connector-owned reports
rejected before card creation can then be resumed with `ReportStore.resume(room)`;
never clear their suspension before the plugin access check succeeds.

## Explicit history recovery

Normal reconciliation advances a durable cursor after each confirmed page and
does not rewind when it reaches the end. Recovery is bounded to five pages of
100 events per cycle. A failed page is retried from its previous cursor.

To replay an older interval, stop only the affected plugin worker, run its
configured image with `rewind-replay --room ROOM_ID --from ROOM_SEQ`, then start
the worker. Use the normal service environment and database credentials. The
room must already be tracked. Stopping the worker prevents a concurrent cursor
write from undoing the rewind. No live sync cursor is reset. The journal still
deduplicates commands; this is recovery, not permission to repeat business effects.

Transient publication retries use capped exponential backoff with jitter (maximum
five minutes). Repeated warnings are sampled at powers of two. An idle status
worker sleeps until its next retry, or at most 30 seconds; new work wakes it
immediately.
