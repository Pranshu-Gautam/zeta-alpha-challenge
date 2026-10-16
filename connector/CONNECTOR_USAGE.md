# ClientDocs connector

`clientdocs_connector.py` implements the public assignment's baseline crawl followed
by the delta at exactly `2024-06-01T00:00:00`. It uses explicit client-scoped UUIDv5
IDs, derived from the opaque `item_id`, with namespace
`35fee07c-74dd-52bf-8906-24d26b0047e3`.

The public assignment's four Part 3 discussion prompts are answered in
[PART3_DISCUSSION.md](PART3_DISCUSSION.md).

## Run

Use a Python environment with `connector/requirements.txt` installed. For example,
from the repository root:

```bash
python3 -m venv connector/.venv
connector/.venv/bin/python -m pip install -r connector/requirements.txt
connector/.venv/bin/python connector/clientdocs_connector.py \
  --source-url http://127.0.0.1:8002 \
  --target-url http://127.0.0.1:8001 \
  --page-size 10 --batch-size 10 --max-pages 1000 --timeout 10
```

The source and target must already be running. Use only a dedicated target with
one writer and no unresolved writes from a previous interrupted run. The default
URLs are those above. The timeout is per request; the page budget applies separately
to baseline and delta traversals. No mock document counts or URI patterns are built in.

Validation passed 64 synthetic tests and fresh-target, warm-rerun, and seeded
partial-state integration against the permitted mock. A separate, independently
frozen golden-fixture verifier checked 25 complete documents with 330 assertions
in each of the three integration runs. These results do not establish
production-scale performance or exactly-once delivery.

## Behavior and recovery

- Enumeration, metadata validation, history resolution, every required content read,
  and target ownership/identity preflight finish before the first target mutation.
- A delta item may have one event, or a group of completely identical payloads.
  Other repeated-item histories fail before writes; there is no ordering inference
  or timestamp bisection. An empty delta is valid.
- Baseline bodies are required even for items subsequently deleted. Bodies are
  preserved verbatim and must be nonempty after an emptiness-only strip check.
  An unavailable body therefore stops the entire run before writes even when the
  final state would omit that item; this preserves the assignment's strict Part 1.
- Preflight reads complete target records. Known items awaiting deletion are allowed;
  foreign records, incompatible UUIDs, missing metadata, and duplicate source IDs fail.
  Existing field values may be stale. No automatic migration or sweep occurs.
  The snapshot is retained, and preflight precedes all source content fetching,
  after complete metadata validation and delta ambiguity resolution.
- Batches are sequential. Every outcome must identify exactly one submitted UUID.
  HTTP 200 alone is insufficient. A delete `NOT_FOUND` for an ID present at preflight
  or explicitly acknowledged by a current-run upsert is contradictory and fails
  the run, even if a GET confirms HTTP 404. Only an ID absent from both sets may
  be accepted as already absent, and only after GET confirms HTTP 404. HTTP 200
  (including JSON null) or a failed confirmation cannot establish absence.
  Deletion events never fetch source content.
- GET transport failures, HTTP 429 and HTTP 5xx receive at most three attempts.
  Malformed responses and other read failures stop without repeated data retries.
  Requests do not follow redirects. The CLI ignores proxy environment variables.
- Mutations are never automatically retried. A lost response, unexpected write
  status, unreadable response or unaccounted outcome stops all further mutation.
  A read-back cannot prove that a timed-out operation has finished. Resolve previous
  writes out of band before rerunning; the script never restarts services or rolls back.
- A received HTTP 422 and explicit per-item rejection are reported distinctly.
  Confirmed successful siblings may remain after a rejected batch. Completion is
  not reported for such a run.
- Final verification compares exact UUID and source-ID membership, URI, title,
  complete content, metadata, and deletion absence against prepared source payloads.
  This runtime read-back check is distinct from the separate read-only
  golden-fixture verification that independently established mock integration
  correctness. The external verification evidence archive is not included in
  this repository.

Success prints a JSON summary with `status: verified` and exits 0. Synchronization
failure exits 1; interruption exits 130; invalid CLI syntax exits 2. Failure does
not roll back earlier accepted writes.
`verified_deletions` counts explicitly acknowledged successful deletions whose
absence was finally verified; `already_absent` counts separately confirmed
`NOT_FOUND` deletion intents. Neither counter is emitted as success on an
incomplete run. A mixed batch may have performed successful deletions before a
contradictory or rejected sibling stops execution.

A completed rerun converges under unchanged source data and exclusive execution.
It can temporarily restore baseline metadata and recreate a subsequently deleted
record. Recovery after acknowledged partial completion is supported; recovery after
ambiguous writes requires establishing that old writes cannot still commit.

Correctness assumes stable, non-reused source identities, complete baseline/delta
coverage, sufficiently stable source reads, and a stable, dedicated target process.
Historical metadata may be combined with newer content. Neither a historical
snapshot, durable target state, nor exactly-once processing is promised.

## Synthetic tests

From the repository root, in the same dependency environment:

```bash
connector/.venv/bin/python -m unittest discover \
  -s connector -p test_clientdocs_connector.py -v
```

The tests use `httpx.MockTransport` and the standard-library `unittest` module.
They import neither supplied server implementation and open no network sockets.
They do not read source scenarios, prior solutions, or grading validators.
