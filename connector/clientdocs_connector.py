"""Full baseline plus fixed-window delta ingestion through the public HTTP APIs."""

import argparse
from dataclasses import dataclass
import json
import math
import sys
from urllib.parse import quote
import uuid

import httpx


# Seed: urn:zeta-alpha:clientdocs:myclient. Changing this migrates the ID space.
CLIENTDOCS_NAMESPACE = uuid.UUID("35fee07c-74dd-52bf-8906-24d26b0047e3")
SINCE = "2024-06-01T00:00:00"
DOCUMENTS = "/ingestion/documents"
GET_ATTEMPTS = 3
ABSENT = object()


class ConnectorError(Exception):
    """Synchronization is incomplete; a partially written target may remain."""


class ContractError(ConnectorError):
    """A response, source history, or existing identity is incompatible."""


class BatchError(ConnectorError):
    """A batch has rejected or incorrectly accounted-for operations."""


class AmbiguousWriteError(ConnectorError):
    """A mutation may still commit; further mutations must stop."""


def document_id(item_id: str) -> str:
    """Use the opaque source ID, never the URI, as the client-scoped identity."""
    return str(uuid.uuid5(CLIENTDOCS_NAMESPACE, item_id))


@dataclass(frozen=True)
class Config:
    source_url: str = "http://127.0.0.1:8002"
    target_url: str = "http://127.0.0.1:8001"
    page_size: int = 10
    batch_size: int = 10
    max_pages: int = 1000
    timeout: float = 10.0

    def __post_init__(self):
        for name in ("page_size", "batch_size", "max_pages"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ConnectorError(f"{name} must be a positive integer")
        if (type(self.timeout) not in (int, float)
                or not math.isfinite(self.timeout) or self.timeout <= 0):
            raise ConnectorError("timeout must be positive and finite")
        for name in ("source_url", "target_url"):
            if not isinstance(getattr(self, name), str):
                raise ConnectorError(f"{name} must be a string")
            try:
                url = httpx.URL(getattr(self, name))
            except (TypeError, httpx.InvalidURL) as error:
                raise ConnectorError(f"{name} must be an absolute HTTP(S) base URL") from error
            if (url.scheme not in ("http", "https") or not url.host
                    or url.username or url.password
                    or "?" in getattr(self, name) or "#" in getattr(self, name)):
                raise ConnectorError(f"{name} must be HTTP(S), without credentials, query, or fragment")


def require_object(value, label):
    if not isinstance(value, dict):
        raise ContractError(f"{label} must be an object")
    return value


def require_string(row, key, label, *, usable=True):
    value = row.get(key)
    if not isinstance(value, str) or (usable and not value.strip()):
        raise ContractError(f"{label}.{key} must be {'a usable' if usable else 'a'} string")
    return value


def validate_source_document(row, label, *, metadata_required=True):
    require_object(row, label)
    require_string(row, "item_id", label)
    require_string(row, "uri", label)
    metadata = row.get("metadata")
    if metadata_required or metadata is not None:
        require_object(metadata, f"{label}.metadata")
        require_string(metadata, "title", f"{label}.metadata")
        require_string(metadata, "item_sensitivity", f"{label}.metadata")


def resolve_delta(events):
    """Collapse only wholly identical groups; never deduplicate mixed histories."""
    resolved = {}
    signatures = {}
    for event in events:
        require_object(event, "delta event")
        kind = event.get("event_type")
        if kind not in ("created", "updated", "deleted"):
            raise ContractError("delta event has an unsupported event_type")
        validate_source_document(event, "delta event", metadata_required=kind != "deleted")
        item_id = event["item_id"]
        try:
            signature = json.dumps(event, sort_keys=True, allow_nan=False)
        except ValueError as error:
            raise ContractError(f"invalid delta payload for {item_id!r}") from error
        if item_id in signatures and signatures[item_id] != signature:
            raise ContractError(f"conflicting delta history for {item_id!r}")
        signatures[item_id] = signature
        resolved[item_id] = event
    return list(resolved.values())


class Connector:
    """One sequential run. The injected client permits entirely synthetic tests."""

    def __init__(self, config: Config, client: httpx.Client):
        self.config = config
        self.client = client

    def _get(self, url, *, params=None, absent_ok=False):
        for attempt in range(GET_ATTEMPTS):
            try:
                response = self.client.get(
                    url, params=params, timeout=self.config.timeout, follow_redirects=False,
                )
            except httpx.RequestError as error:
                if attempt + 1 < GET_ATTEMPTS:
                    continue
                raise ConnectorError(f"GET {url} failed after {GET_ATTEMPTS} attempts") from error
            if response.status_code == 429 or response.status_code >= 500:
                if attempt + 1 < GET_ATTEMPTS:
                    continue
            if response.status_code == 404 and absent_ok:
                return ABSENT
            if response.status_code != 200:
                raise ConnectorError(f"GET {url} returned HTTP {response.status_code}")
            try:
                return response.json()
            except ValueError as error:
                raise ContractError(f"GET {url} returned invalid JSON") from error
        raise AssertionError("unreachable GET retry state")

    def _post(self, path, payload):
        url = self.config.target_url.rstrip("/") + path
        try:
            response = self.client.post(
                url, json=payload, timeout=self.config.timeout, follow_redirects=False,
            )
        except httpx.RequestError as error:
            raise AmbiguousWriteError(
                f"POST {path} lost its response; do not retry or continue without resolving prior writes"
            ) from error
        if response.status_code == 422:
            raise ConnectorError(f"POST {path} rejected with HTTP {response.status_code}; no retry")
        if response.status_code != 200:
            raise AmbiguousWriteError(f"POST {path} returned HTTP {response.status_code}; outcome unresolved")
        try:
            return response.json()
        except ValueError as error:
            raise AmbiguousWriteError(f"POST {path} returned unreadable outcome JSON") from error

    def _pages(self, path, key, *, params=None):
        rows = []
        total = None
        for page in range(1, self.config.max_pages + 1):
            query = dict(params or {}, page=page, page_size=self.config.page_size)
            body = require_object(self._get(self.config.source_url.rstrip("/") + path, params=query), path)
            for field, expected in (("page", page), ("page_size", self.config.page_size)):
                if type(body.get(field)) is not int or body[field] != expected:
                    raise ContractError(f"{path} echoed an unexpected {field}")
            advertised = body.get("total")
            if type(advertised) is not int or advertised < 0:
                raise ContractError(f"{path}.total must be a nonnegative integer")
            if total is None:
                total = advertised
            elif total != advertised:
                raise ContractError(f"{path}.total changed during traversal")
            batch = body.get(key)
            expected_count = min(self.config.page_size, max(0, total - len(rows)))
            if not isinstance(batch, list) or len(batch) != expected_count:
                raise ContractError(f"{path} has an unexpected {key} page length")
            rows.extend(batch)
            if page * self.config.page_size >= total:
                if len(rows) != total:
                    raise ContractError(f"{path} did not cover its advertised total")
                return rows
        raise ContractError(f"{path} exceeded max_pages={self.config.max_pages}")

    def _payload(self, row):
        item_id = row["item_id"]
        segment = quote(item_id, safe="")
        if item_id in (".", ".."):
            segment = "%2E" * len(item_id)
        body = require_object(self._get(
            self.config.source_url.rstrip("/") + f"/documents/{segment}/content",
        ), "content response")
        if body.get("item_id") != item_id:
            raise ContractError(f"content item_id does not match {item_id!r}")
        content = require_string(body, "content", "content response")
        return {
            "document_id": document_id(item_id),
            "document_uri": row["uri"],
            "title": row["metadata"]["title"],
            "document_content": content,
            "client_specific_metadata": {
                "item_id": item_id,
                "item_sensitivity": row["metadata"]["item_sensitivity"],
            },
        }

    def _target_records(self):
        url = self.config.target_url.rstrip("/") + DOCUMENTS
        summaries = self._get(url)
        if not isinstance(summaries, list):
            raise ContractError("target listing must be an array")
        records = {}
        source_ids = set()
        for summary in summaries:
            require_object(summary, "target summary")
            target_id = require_string(summary, "document_id", "target summary")
            try:
                uuid.UUID(target_id)
            except ValueError as error:
                raise ContractError("target summary document_id is not a UUID") from error
            if target_id in records:
                raise ContractError("duplicate target document_id in listing")
            row = require_object(self._get(url + "/" + quote(target_id, safe="")), "target document")
            for field in ("document_id", "document_uri", "title"):
                require_string(summary, field, "target summary", usable=False)
                require_string(row, field, "target document", usable=False)
                if row[field] != summary[field]:
                    raise ContractError(f"target summary/detail disagree on {field}")
            require_string(row, "document_content", "target document", usable=False)
            metadata = require_object(row.get("client_specific_metadata"), "target metadata")
            item_id = require_string(metadata, "item_id", "target metadata")
            require_string(metadata, "item_sensitivity", "target metadata", usable=False)
            if item_id in source_ids:
                raise ContractError(f"duplicate target source identity {item_id!r}")
            source_ids.add(item_id)
            records[target_id] = row
        return records

    def _preflight(self, known_items):
        records = self._target_records()
        for target_id, row in records.items():
            item_id = row["client_specific_metadata"]["item_id"]
            if item_id not in known_items:
                raise ContractError(f"foreign or out-of-scope target item {item_id!r}")
            if target_id != document_id(item_id):
                raise ContractError(f"incompatible target identity for {item_id!r}; no migration")
        return records

    def _confirm_absent(self, target_id):
        url = self.config.target_url.rstrip("/") + DOCUMENTS + "/" + quote(target_id, safe="")
        if self._get(url, absent_ok=True) is not ABSENT:
            raise ContractError(f"expected deleted document {target_id} is still present")

    def _write_batches(self, rows, *, deleting=False, known_present=frozenset()):
        acknowledged = set()
        already_absent = set()
        for start in range(0, len(rows), self.config.batch_size):
            batch = rows[start:start + self.config.batch_size]
            submitted = batch if deleting else [row["document_id"] for row in batch]
            expected = set(submitted)
            if len(expected) != len(submitted):
                raise BatchError("duplicate submitted target identities")
            path = DOCUMENTS + ("/delete-document-batches" if deleting else "/document-batches")
            result = self._post(path, {
                "document_ids" if deleting else "documents": batch,
            })
            try:
                require_object(result, "batch response")
                succeeded, failed = result.get("succeeded"), result.get("failed")
                if not isinstance(succeeded, list) or not isinstance(failed, list):
                    raise ContractError("batch response requires succeeded and failed arrays")
                accounted = set()
                missing = []
                rejected = []
                for accepted, outcomes in ((True, succeeded), (False, failed)):
                    for outcome in outcomes:
                        require_object(outcome, "batch outcome")
                        target_id = require_string(outcome, "document_id", "batch outcome")
                        if target_id not in expected or target_id in accounted:
                            raise ContractError("duplicate or unexpected batch outcome identity")
                        accounted.add(target_id)
                        if not accepted:
                            code = require_string(outcome, "error_code", "batch failure")
                            require_string(outcome, "error_message", "batch failure", usable=False)
                            if deleting and code == "NOT_FOUND":
                                missing.append(target_id)
                            else:
                                rejected.append({"document_id": target_id, "error_code": code})
                if accounted != expected or len(succeeded) + len(failed) != len(batch):
                    raise ContractError("missing batch outcomes")
            except ContractError as error:
                raise AmbiguousWriteError(f"POST {path} has unresolved outcomes: {error}") from error
            acknowledged.update(row["document_id"] for row in succeeded)
            if rejected:
                confirmed = [row["document_id"] for row in succeeded]
                raise BatchError(f"batch rejected required records: {rejected}; succeeded={confirmed}")
            for target_id in missing:
                self._confirm_absent(target_id)
                if target_id in known_present:
                    raise ContractError(
                        f"incomplete execution: delete NOT_FOUND for known-present document {target_id}; "
                        "single-writer/stable-target assumptions violated"
                    )
                already_absent.add(target_id)
        return acknowledged, already_absent

    def _verify(self, expected, deleted):
        actual = self._target_records()
        if set(actual) != set(expected):
            raise ContractError("verification failed: target UUID membership differs")
        for target_id, wanted in expected.items():
            for field, value in wanted.items():
                if actual[target_id][field] != value:
                    raise ContractError(f"verification failed: {target_id} has wrong {field}")
        for target_id in deleted:
            self._confirm_absent(target_id)

    def run(self):
        # Hard preparation barrier: no target POST occurs before preflight completes.
        baseline = self._pages("/documents", "documents")
        events = self._pages("/documents/delta", "events", params={"since": SINCE})
        baseline_ids = set()
        for row in baseline:
            validate_source_document(row, "baseline document")
            if row["item_id"] in baseline_ids:
                raise ContractError(f"duplicate baseline item_id {row['item_id']!r}")
            baseline_ids.add(row["item_id"])
        delta = resolve_delta(events)
        known_items = baseline_ids | {event["item_id"] for event in delta}
        if len({document_id(item_id) for item_id in known_items}) != len(known_items):
            raise ContractError("target UUID collision between distinct source items")
        preflight_records = self._preflight(known_items)
        baseline_payloads = [self._payload(row) for row in baseline]
        delta_payloads = [self._payload(event) for event in delta if event["event_type"] != "deleted"]
        deleted = [document_id(event["item_id"]) for event in delta if event["event_type"] == "deleted"]
        expected = {row["document_id"]: row for row in baseline_payloads + delta_payloads}
        for target_id in deleted:
            expected.pop(target_id, None)
        baseline_upserts, _ = self._write_batches(baseline_payloads)
        delta_upserts, _ = self._write_batches(delta_payloads)
        confirmed_deletions, already_absent = self._write_batches(
            deleted, deleting=True,
            known_present=set(preflight_records) | baseline_upserts | delta_upserts,
        )
        self._verify(expected, deleted)
        return {
            "status": "verified",
            "baseline_documents": len(baseline),
            "delta_events": len(events),
            "delta_items": len(delta),
            "verified_deletions": len(confirmed_deletions),
            "already_absent": len(already_absent),
            "verified_documents": len(expected),
        }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-url", default=Config.source_url)
    parser.add_argument("--target-url", default=Config.target_url)
    parser.add_argument("--page-size", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--max-pages", type=int, default=1000)
    parser.add_argument("--timeout", type=float, default=10.0, help="Per-request timeout in seconds")
    args = parser.parse_args(argv)
    try:
        config = Config(**vars(args))
        with httpx.Client(trust_env=False, follow_redirects=False) as client:
            result = Connector(config, client).run()
    except ConnectorError as error:
        print(f"ERROR [{type(error).__name__}]: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("ERROR: interrupted; state may be partial. Resolve outstanding writes before rerunning.", file=sys.stderr)
        return 130
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
