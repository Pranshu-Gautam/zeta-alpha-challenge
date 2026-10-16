# Part 3 — Production discussion

The submitted connector is a sequential baseline crawl followed by the assignment's
fixed-window delta. It validates all required input before mutation, uses stable
client-scoped UUIDs, reconciles batch outcomes by identity, and reads back the final
state. The following describes how I would evolve its operation, not infrastructure
implemented by this submission. Source identity stability and non-reuse, exclusive
target ownership, and no outstanding earlier writes remain correctness assumptions.

## 1. Scaling to millions of documents

Batch ingestion and deletion are available today. I would tune batch sizes using
measured request limits, payload bytes, latency, and per-item errors, rather than
document count alone. The current script holds enumeration results, bodies, and
expected records in memory; its page budget and memory footprint are not suitable
for millions of documents. A production implementation could spool validated input
and content to durable staging storage, preserving the preparation barrier without
keeping everything in RAM. Partial preparation must not trigger target writes.

Content reads could use bounded concurrency and connection pooling. A bounded queue
between fetching and writing would apply backpressure when ingestion slows; memory,
in-flight bytes, and worker counts need hard limits. Target write concurrency would
require additional care: keep operations for one identity serialized, stop scheduling
on ambiguity, and account for requests already in flight. The submitted sequential
writer avoids that extra uncertainty. Partitioning by a stable source-ID hash is
useful only with exclusive partition ownership and reliable enumeration; it cannot
create an ordering guarantee absent from the source API.

Offset pagination and stable total counts do not establish snapshot consistency.
For reliable large crawls I would request snapshot-bound cursors or a documented
snapshot-to-change-log handoff, bulk content export, and explicit rate limits.
The target's unscoped listing plus individual detail GETs is also expensive: scoped,
paginated listings with full verification fields or bulk reads would help. Periodic
reconciliation should compare identities and fields, repair confirmed divergence,
and never delete records merely because an incomplete crawl omitted them.

An external durable ledger could checkpoint validated staging segments and fully
acknowledged outcomes. The existing APIs do not provide durable checkpoints, a
resumable snapshot, or a transaction spanning source progress and target writes.
Saving an offset locally is not proof of safe continuation after source drift.
Resume only when the same prepared input and resolved target outcomes are available;
otherwise reconcile and rebuild under stable-source/exclusive-writer conditions.

## 2. Synchronizing changes within approximately one minute

I would first measure end-to-end freshness: source publication delay, polling wait,
enumeration/content time, queue age, and target acknowledgement/read-back latency.
Choose a polling interval well below a minute only if documented rate limits,
change volume, and measured processing capacity leave enough headroom. Prioritize
incremental work over background reconciliation, bound queues, and alert when the
oldest pending change threatens the freshness objective. The assignment's fixed
`since` value is not a production incremental checkpoint strategy.

The existing delta endpoint can be polled, but timestamps alone do not establish a
safe high-water mark: tied events, retention, boundary inclusivity, late publication,
and pagination consistency need documented semantics. Overlapping polling windows
can reduce some gaps but do not prove complete coverage or resolve conflicting
histories. The connector deliberately rejects conflicting events for one identity
rather than inventing an order.

For a defensible sub-minute objective I would request a durable change feed with
resumable cursors, retention guarantees, per-item versions/order, deletion tombstones,
and a consistent baseline handoff. Webhooks could wake workers quickly, but should
be treated as hints unless their delivery and replay guarantees are explicit; a
replayable feed and reconciliation remain necessary. Persist progress only after
all relevant outcomes are resolved. Neither the present API nor more workers alone
guarantees sub-minute synchronization or exactly-once delivery.

## 3. Handling ingestion failures

HTTP 200 is not sufficient: reconcile every submitted identity against the batch's
success and failure arrays. Explicitly successful siblings may already be committed.
Separate permanent problems (invalid identity, empty content, schema errors) from
documented transient failures. Quarantine permanent failures with their source ID,
input version/digest, reason, and attempt history in a durable dead-letter ledger;
provide an owner, alert, repair, and controlled replay path. Restrict access and
avoid retaining sensitive document bodies unnecessarily. A dead-letter entry is
unresolved work, not a successfully synchronized document.

Reads can use bounded retries with exponential backoff, jitter, and applicable
`Retry-After` guidance in production. The current implementation bounds GET attempts
at three but does not add backoff. For writes, do not blindly retry timeouts, 5xx,
unreadable outcomes, or malformed accounting: the first request may have committed
or may still commit later. This connector stops without automatic write retries.
Even a current read-back does not establish that an outstanding request has finished.

Safe automatic write recovery would need documented non-commit semantics or stronger
capabilities such as durable idempotency keys with outcome lookup, operation status,
and version/fencing checks. Deterministic UUIDs reduce duplicate identities but do
not prevent a delayed old upsert from overwriting newer data. Without those features,
resolve outstanding operations out of band, inspect acknowledged partial state,
and explicitly authorize a rerun only after old writes cannot still commit. The
present strict Part 1 also requires content for a baseline item later deleted;
an unavailable body blocks preparation even though that item is absent at the end.

## 4. Handling deletion failures

Deletion has the same per-identity accounting and write-ambiguity rules as ingestion.
Track the preflight target snapshot and acknowledged current-run upserts. An explicit
successful deletion is an actual deletion; a `NOT_FOUND` is not. If a known-present
ID returns `NOT_FOUND`, even GET 404 contradicts the single-writer/stable-target
assumptions and execution must stop. Only an ID never known present in that run may
be treated as already absent, after an explicit GET 404. A present record or failed
confirmation leaves the deletion unresolved. Report real deletions and already-absent
intents separately, then verify final absence.

For production, keep durable deletion intents/tombstones until confirmed resolution;
route explicit unrecoverable failures to a monitored dead-letter workflow, not silent
abandonment. Retry an explicitly rejected deletion only when non-commit and current
intent are established; never automatically replay an ambiguous request with the
present API. A delayed older upsert can resurrect a deleted document, so lasting
deletion requires version-aware writes/tombstones or fencing, not just a successful
delete response. Periodic reconciliation can detect resurrection but cannot prevent it.

Across both failure paths, monitor source lag, queue depth/age, read and write latency,
throughput, per-error counts, retries, ambiguous operations, dead-letter age,
confirmed deletions, already-absent intents, and reconciliation mismatches. Alert on
stalled progress and unexpected state loss. Recovery should establish exclusive
ownership, resolve outstanding writes, repair/replay known inputs, and verify before
advancing progress. The supplied in-memory target is not durable across restart;
production recovery needs durable target storage and an external operational ledger.
Verification establishes observed state at that time, not permanent quiescence.
