# ClientDocs Connector — Zeta Alpha Engineering Challenge

*A Python implementation of full document ingestion, incremental synchronization,
and robust failure handling.*

## Overview

My solution to the **ClientDocs connector track** of the
[Zeta Alpha engineering challenge](https://github.com/zetaalphavector/zeta-alpha-challenge).
The connector reads metadata, content, and changes from the ClientDocs HTTP API
and synchronizes them into the supplied Pipeline service: a full baseline followed
by a delta, with conservative failure handling and field-by-field verification.

## What is implemented

- Full paginated baseline crawl and content retrieval.
- Paginated delta processing for `created`, `updated`, and `deleted` events, using
  the assignment's fixed `since=2024-06-01T00:00:00` boundary.
- Deterministic, client-scoped UUIDv5 identifiers derived from source `item_id`.
- All required input validated before mutation; sequential batch upserts and
  deletions with exact per-document accounting.
- Final verification of membership, URI, title, content, metadata, and deletion
  absence. Incomplete or ambiguous execution exits unsuccessfully.

## Architecture

```text
ClientDocs HTTP API → Pagination and validation → Document transformation
                   → Pipeline batch ingestion → Final-state verification
```

A move or rename can change a URI without creating a new logical document.
UUIDv5 combines the source `item_id` with a fixed client namespace, keeping target
identity stable across updates, deletions, and reruns. This assumes source IDs
are stable and never reused.

## Quick start

From the repository root, with Python 3 and `venv` available:

```bash
python3 -m venv connector/.venv
connector/.venv/bin/python -m pip install -r connector/requirements.txt
```

Start the supplied mocks in **separate terminals**, each from the repository root.
The scripts create their environments and install requirements.

```bash
# Terminal 1: Pipeline — http://127.0.0.1:8001/docs
bash connector/run_pipeline.sh
```

```bash
# Terminal 2: ClientDocs mock — http://127.0.0.1:8002/docs
bash connector/connector_client_servers/run_client_mock.sh
```

Use a dedicated Pipeline with no other writers or unresolved earlier writes.
From the repository root in a third terminal:

```bash
connector/.venv/bin/python connector/clientdocs_connector.py \
  --source-url http://127.0.0.1:8002 \
  --target-url http://127.0.0.1:8001 \
  --page-size 10 --batch-size 10 --max-pages 1000 --timeout 10
```

Run the synthetic tests without services:

```bash
PYTHONDONTWRITEBYTECODE=1 connector/.venv/bin/python -m unittest discover \
  -s connector -p test_clientdocs_connector.py -v
```

See [usage and recovery](connector/CONNECTOR_USAGE.md) for CLI behavior and
operating assumptions.

## Verification results

Verified against the **provided mock environment**, not production benchmarks:

- **64 synthetic tests passed**, including failure paths.
- **Three real-mock integration scenarios passed:** fresh target, warm rerun,
  and seeded partial-state recovery.
- Each scenario independently verified **25 complete final documents with 330
  assertions**, including **25 matching content SHA-256 fingerprints** and deletion
  absence.
- The warm rerun converged to an **identical terminal-state digest**.
- The seeded partial-state recovery reached the same correct final state; this
  was not a real operating-system crash or delayed-write recovery test.

The independently frozen acceptance oracle and evidence archive are external,
not bundled here. They are separate from the connector's runtime read-back checks.

## Engineering decisions and limitations

- **Stable identity:** item-derived UUIDs survive URI changes.
- **Conservative delta handling:** only completely identical repeated events
  collapse; conflicting histories are rejected without inventing event order.
- **Exact batch checks:** every UUID needs one identifiable outcome; HTTP 200 or
  matching counts alone are insufficient.
- **Write safety:** no automatic retry of ambiguous writes. Execution stops until
  outstanding writes are resolved; a delayed write may still commit.
- **API boundaries:** no consistent source snapshot; metadata may pair with newer
  content. The in-memory target offers no durable checkpoints, write fencing, or
  exactly-once guarantee.
- **Scope:** fixed delta boundary and in-memory preparation, not a production
  scheduler or a production-scale performance claim.

The [Part 3 discussion](connector/PART3_DISCUSSION.md) covers scaling, fast sync,
ingestion failures, deletion failures, and additional API capabilities needed.

## Repository navigation

- [Connector implementation](connector/clientdocs_connector.py)
- [Synthetic tests](connector/test_clientdocs_connector.py)
- [Usage and recovery](connector/CONNECTOR_USAGE.md)
- [Production discussion](connector/PART3_DISCUSSION.md)
- [Original ClientDocs assignment and API contracts](connector/README.md)

## Upstream attribution

Zeta Alpha provided the [public challenge](https://github.com/zetaalphavector/zeta-alpha-challenge),
mock services, and API contracts. This fork adds my implementation, tests, and
documentation: a personal solution, **not an official Zeta Alpha product**.
The original general search challenge README is preserved below; this solution
addresses the separate ClientDocs exercise.

<details>
<summary>Original upstream README — general search challenge</summary>

# Zeta Alpha Challenge

At Zeta Alpha we’re building the [next generation neural discovery platform](https://search.zeta-alpha.com). Our platform allows you to find documents, stay up-to-date on relevant developments, and organize your knowledge discovery work. Our search system is the part that ingests, understands, and retrieves documents from multiple sources.

## Assignment: Implement a search system MVP

In this challenge you will be creating an MVP of the search system. You are provided with a source folder containing multiple PDF documents. Your objective is to implement a solution capable of:
1. Ingesting, parsing, and storing the provided documents
1. Retrieving a list of documents by matching the title, authors, and/or content.

**You can find the source documents [here](https://zeta-alpha-challenge.s3.eu-central-1.amazonaws.com/index.html).**

**Note**: You may not have the knowledge to make an exceptional implementation of all parts of this system. Therefore it's recommended that you try to shine in your areas of expertise, while picking pre-built solutions for the others.

### Deliverables

The result should be in the form of functional code along with documentation for running and using it.

## [Optional] Bonus points

A typical search system includes the following stages:
- An offline stage that periodically polls multiple data sources (external APIs, websites, etc.), processes them, and populates a search database.
- A retrieval stage that allows users to search, rank, and sort the documents from the database.

Some of the challenges we face, when designing a search system, revolve around providing a good experience for both our internal and external users. For example:

1. Allowing data and content managers to easily manage data sources, as well as individual documents within the sources. 
1. Allowing NLP/ML engineers to easily experiment and develop processing workflows.
1. Allowing search engineers to tweak and develop the retrieval process.
1. Allowing end-users to interact with the platform in a fast, reliable, and secure way.

How would you address (some of) these challenges when designing a search system?

## Tips and remarks

- Don't hesitate to contact us with any clarification questions.
- We expect about half a day to build your solution.
- The coding assignment should be written in **python**.
- You can choose any existing libraries and packages that you deem necessary.
- Expect to be questioned about your technology and architectural decisions, as well as implementation details.
- Work in an agile way. You might not be able to completely solve everything in the assignment, so pick wisely.

</details>
