"""Synthetic public-contract tests. No supplied server or scenario is imported."""

from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import io
import json
import unittest
from unittest.mock import patch
import uuid

import httpx

import clientdocs_connector as connector


SOURCE = "http://source.test"
TARGET = "http://target.test"
A = "synthetic-alpha"
B = "DOC-0005"  # Regression: a known baseline item awaiting its delta deletion.
C = "synthetic-charlie"


def source_document(item_id, *, uri=None, title=None):
    return {
        "item_id": item_id,
        "uri": uri if uri is not None else "synthetic://documents/" + item_id,
        "metadata": {"title": title or "Synthetic " + item_id, "item_sensitivity": "internal"},
    }


def event(kind, item_id, **changes):
    row = source_document(item_id, **changes)
    row["event_type"] = kind
    if kind == "deleted":
        row["metadata"] = None
    return row


def stored_document(row, content="Previously stored synthetic content"):
    return {
        "document_id": connector.document_id(row["item_id"]),
        "document_uri": row["uri"],
        "title": row["metadata"]["title"],
        "document_content": content,
        "client_specific_metadata": {
            "item_id": row["item_id"],
            "item_sensitivity": row["metadata"]["item_sensitivity"],
        },
    }


class SyntheticServices:
    """Tiny in-process HTTP double, written only from the published contracts."""

    def __init__(self, baseline=None, events=None):
        self.baseline = deepcopy(baseline if baseline is not None else [source_document(A), source_document(B)])
        self.events = deepcopy(events if events is not None else [
            event("created", C), event("updated", A, title="Synthetic alpha revised"), event("deleted", B),
        ])
        self.contents = {row["item_id"]: "  Synthetic body for " + str(row["item_id"]) + "\n"
                         for row in self.baseline + self.events}
        self.store = {}
        self.requests = []
        self.intercept = None

    @property
    def posts(self):
        return [request for request in self.requests if request.method == "POST"]

    def __call__(self, request):
        self.requests.append(request)
        if self.intercept is not None:
            response = self.intercept(request)
            if response is not None:
                return response
        return self.respond(request)

    def respond(self, request):
        path = request.url.path
        if request.url.host == "source.test":
            if request.method != "GET":
                raise AssertionError("The source must never receive a mutation")
            if path in ("/documents", "/documents/delta"):
                key = "documents" if path == "/documents" else "events"
                rows = self.baseline if key == "documents" else self.events
                if key == "events":
                    assert request.url.params["since"] == "2024-06-01T00:00:00"
                page, size = int(request.url.params["page"]), int(request.url.params["page_size"])
                return httpx.Response(200, json={
                    key: rows[(page - 1) * size:page * size], "page": page,
                    "page_size": size, "total": len(rows),
                })
            if path.startswith("/documents/") and path.endswith("/content"):
                item_id = path[len("/documents/"):-len("/content")]
                if item_id not in self.contents:
                    return httpx.Response(404, json={"detail": "document not found"})
                return httpx.Response(200, json={"item_id": item_id, "content": self.contents[item_id]})
        elif request.url.host == "target.test":
            if request.method == "GET" and path == connector.DOCUMENTS:
                return httpx.Response(200, json=[
                    {key: row[key] for key in ("document_id", "document_uri", "title")}
                    for row in self.store.values()
                ])
            if request.method == "GET" and path.startswith(connector.DOCUMENTS + "/"):
                row = self.store.get(path.rsplit("/", 1)[1])
                return httpx.Response(200, json=row) if row else httpx.Response(404, json={"detail": "document not found"})
            if request.method == "POST" and path == connector.DOCUMENTS + "/document-batches":
                docs = json.loads(request.content)["documents"]
                for row in docs:
                    assert set(row["client_specific_metadata"]) == {"item_id", "item_sensitivity"}
                    self.store[row["document_id"]] = deepcopy(row)
                return httpx.Response(200, json={
                    "succeeded": [{"document_id": row["document_id"]} for row in docs], "failed": [],
                })
            if request.method == "POST" and path == connector.DOCUMENTS + "/delete-document-batches":
                succeeded, failed = [], []
                for target_id in json.loads(request.content)["document_ids"]:
                    if target_id in self.store:
                        del self.store[target_id]
                        succeeded.append({"document_id": target_id})
                    else:
                        failed.append({"document_id": target_id, "document_uri": None,
                                       "error_code": "NOT_FOUND", "error_message": "no such target"})
                return httpx.Response(200, json={"succeeded": succeeded, "failed": failed})
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")


class ConnectorTests(unittest.TestCase):
    def setUp(self):
        self.service = SyntheticServices()

    def make_connector(self, service=None, **options):
        service = service if service is not None else self.service
        client = httpx.Client(transport=httpx.MockTransport(service), trust_env=False)
        self.addCleanup(client.close)
        return connector.Connector(connector.Config(source_url=SOURCE, target_url=TARGET, **options), client)

    def assert_preparation_failure(self, service=None, **options):
        service = service if service is not None else self.service
        with self.assertRaises(connector.ConnectorError):
            self.make_connector(service, **options).run()
        self.assertEqual(service.posts, [])

    def test_namespace_matches_seed_independently(self):
        expected = uuid.uuid5(uuid.NAMESPACE_URL, "urn:zeta-alpha:clientdocs:myclient")
        self.assertEqual(str(expected), "35fee07c-74dd-52bf-8906-24d26b0047e3")
        self.assertEqual(connector.CLIENTDOCS_NAMESPACE, expected)

    def test_frozen_uuid_vectors(self):
        for item_id, expected in {
            "DOC-0001": "fa31c9e6-8fd1-5c9b-b627-097a9aa9107c",
            "DOC-0003": "388b84cd-b023-557c-96ec-9c9a89f2e1c4",
            "DOC-0005": "4671bf5d-9ea4-5395-8441-1c90148790ff",
            "DOC-0025": "a4591667-a6aa-51c9-9975-dd7b42e75734",
        }.items():
            with self.subTest(item_id=item_id):
                self.assertEqual(connector.document_id(item_id), expected)

    def test_identity_preserves_case_whitespace_and_unicode(self):
        values = ["Item", "item", " Item ", "\u00e9", "e\u0301"]
        self.assertEqual(len({connector.document_id(value) for value in values}), len(values))

    def test_doc_0026_independently_pinned_uuid(self):
        self.assertEqual(connector.document_id("DOC-0026"), "0bf1c44d-d8be-56fa-929f-89188729e436")

    def test_bad_configuration_rejected(self):
        for options in ({"page_size": 0}, {"batch_size": -1}, {"max_pages": True},
                        {"timeout": 0}, {"timeout": float("nan")}, {"timeout": float("inf")},
                        {"source_url": "relative"}, {"target_url": "ftp://target.test"},
                        {"source_url": httpx.URL(SOURCE)},
                        {"source_url": "http://user:secret@source.test"},
                        {"target_url": "http://target.test?x=1"},
                        {"source_url": "http://source.test?"}, {"target_url": "http://target.test#"}):
            with self.subTest(options=options), self.assertRaises(connector.ConnectorError):
                connector.Config(**options)

    def test_complete_synthetic_run_and_phase_barrier(self):
        result = self.make_connector().run()
        self.assertEqual(result["status"], "verified")
        self.assertEqual(result["verified_documents"], 2)
        self.assertEqual(result["verified_deletions"], 1)
        self.assertEqual(result["already_absent"], 0)
        self.assertEqual(set(self.service.store), {connector.document_id(A), connector.document_id(C)})
        first_post = next(i for i, r in enumerate(self.service.requests) if r.method == "POST")
        self.assertTrue(all(r.url.host == "target.test" for r in self.service.requests[first_post:]))
        self.assertEqual(Counter(r.url.path for r in self.service.requests if r.url.host == "source.test"), {
            "/documents": 1, "/documents/delta": 1,
            f"/documents/{A}/content": 2, f"/documents/{B}/content": 1, f"/documents/{C}/content": 1,
        })
        writes = [json.loads(r.content) for r in self.service.posts]
        self.assertEqual(writes[0]["documents"][0]["title"], "Synthetic " + A)
        self.assertEqual(writes[1]["documents"][1]["title"], "Synthetic alpha revised")

    def test_existing_item_moves_without_changing_identity(self):
        old = source_document(A, uri="synthetic://old")
        new = source_document(A, uri="synthetic://new")
        self.service = SyntheticServices([new], [])
        self.service.store[connector.document_id(A)] = stored_document(old)
        self.make_connector().run()
        self.assertEqual(len(self.service.store), 1)
        self.assertEqual(self.service.store[connector.document_id(A)]["document_uri"], "synthetic://new")

    def test_shared_uri_does_not_collapse_distinct_items(self):
        self.service = SyntheticServices([source_document(A, uri="synthetic://shared"),
                                          source_document(B, uri="synthetic://shared")], [])
        self.make_connector().run()
        self.assertEqual(len(self.service.store), 2)
        self.assertEqual({row["document_uri"] for row in self.service.store.values()}, {"synthetic://shared"})

    def test_exact_and_partial_pagination_boundaries(self):
        for count, expected_pages in ((0, 1), (3, 2), (4, 2)):
            with self.subTest(count=count):
                service = SyntheticServices([source_document(f"synthetic-{i}") for i in range(count)], [])
                self.make_connector(service, page_size=2).run()
                pages = [r for r in service.requests if r.url.path == "/documents"]
                self.assertEqual(len(pages), expected_pages)
                self.assertEqual([r.url.params["page"] for r in pages], [str(i) for i in range(1, expected_pages + 1)])

    def test_invalid_pagination_envelopes_rejected(self):
        invalid = [[], {}, {"page": True, "page_size": 10, "total": 0, "documents": []},
                   {"page": 2, "page_size": 10, "total": 0, "documents": []},
                   {"page": 1, "page_size": 9, "total": 0, "documents": []},
                   {"page": 1, "page_size": 10, "total": -1, "documents": []},
                   {"page": 1, "page_size": 10, "total": True, "documents": []},
                   {"page": 1, "page_size": 10, "total": 2, "documents": []},
                   {"page": 1, "page_size": 10, "total": 0, "documents": [source_document(A)]}]
        for body in invalid:
            with self.subTest(body=body):
                service = SyntheticServices()
                service.intercept = lambda r, body=body: httpx.Response(200, json=body) if r.url.path == "/documents" else None
                self.assert_preparation_failure(service)

    def test_total_drift_rejected(self):
        def drift(request):
            if request.url.path == "/documents" and request.url.params["page"] == "2":
                response = self.service.respond(request).json()
                response["total"] += 1
                return httpx.Response(200, json=response)
        self.service.intercept = drift
        self.assert_preparation_failure(page_size=1)

    def test_page_budget_is_failure_not_truncation(self):
        self.assert_preparation_failure(page_size=1, max_pages=1)

    def test_duplicate_baseline_ids_rejected_across_pages(self):
        self.service = SyntheticServices([source_document(A), source_document(A)], [])
        self.assert_preparation_failure(page_size=1)

    def test_empty_delta_is_valid_and_uses_no_timestamp_probes(self):
        self.service.events = []
        self.make_connector().run()
        delta_reads = [r for r in self.service.requests if r.url.path == "/documents/delta"]
        self.assertEqual(len(delta_reads), 1)
        self.assertEqual(delta_reads[0].url.params["since"], "2024-06-01T00:00:00")
        self.assertEqual(len(self.service.store), 2)

    def test_identical_delta_group_collapses_across_pages(self):
        change = event("updated", A)
        self.service.events = [change, deepcopy(change)]
        result = self.make_connector(page_size=1).run()
        self.assertEqual(result["delta_events"], 2)
        self.assertEqual(result["delta_items"], 1)

    def test_mixed_delta_history_never_deduplicated_or_ordered(self):
        histories = [
            [event("updated", A, title="one"), event("updated", A, title="two")],
            [event("deleted", A), event("created", A)],
            [event("deleted", A), event("created", A), event("deleted", A)],
        ]
        for history in histories:
            with self.subTest(history=history):
                service = SyntheticServices(events=history)
                self.assert_preparation_failure(service, page_size=1)
                self.assertFalse(any(r.url.path.endswith("/content") for r in service.requests))

    def test_identical_required_fields_but_different_payloads_rejected(self):
        first = dict(event("updated", A), extra=True)
        second = dict(first, extra=1)
        self.service.events = [first, second]
        self.assert_preparation_failure()

    def test_invalid_source_metadata_and_event_kind(self):
        invalid = [dict(source_document(A), metadata=None),
                   dict(source_document(A), metadata={"title": "x"}),
                   dict(source_document(A), metadata={"title": " ", "item_sensitivity": "internal"}),
                   dict(source_document(A), item_id=17)]
        for row in invalid:
            with self.subTest(row=row):
                self.assert_preparation_failure(SyntheticServices([row], []))
        self.service.events = [dict(event("updated", A), event_type="renamed")]
        self.assert_preparation_failure()

    def test_created_or_updated_null_metadata_rejected(self):
        for kind in ("created", "updated"):
            with self.subTest(kind=kind):
                self.assert_preparation_failure(SyntheticServices(events=[dict(event(kind, C), metadata=None)]))

    def test_pending_baseline_deletion_is_allowed_by_preflight(self):
        for row in self.service.baseline:
            self.service.store[connector.document_id(row["item_id"])] = stored_document(row)
        self.make_connector().run()
        self.assertNotIn(connector.document_id(B), self.service.store)

    def test_all_preflight_details_read_before_mutation(self):
        for row in self.service.baseline:
            self.service.store[connector.document_id(row["item_id"])] = stored_document(row)
        self.make_connector().run()
        first_post = next(i for i, r in enumerate(self.service.requests) if r.method == "POST")
        details = {r.url.path for r in self.service.requests[:first_post] if r.url.host == "target.test"}
        self.assertIn(connector.DOCUMENTS + "/" + connector.document_id(A), details)
        self.assertIn(connector.DOCUMENTS + "/" + connector.document_id(B), details)

    def test_preflight_allows_stale_values_for_compatible_identity(self):
        row = stored_document(source_document(A, uri="synthetic://old", title="Old title"))
        row["client_specific_metadata"]["item_sensitivity"] = "old classification"
        self.service.store[row["document_id"]] = row
        self.make_connector().run()
        self.assertEqual(self.service.store[row["document_id"]]["title"], "Synthetic alpha revised")
        self.assertEqual(self.service.store[row["document_id"]]["client_specific_metadata"]["item_sensitivity"], "internal")

    def test_duplicate_source_identities_in_target_are_rejected(self):
        first = stored_document(source_document(A))
        second = dict(first, document_id=str(uuid.UUID(int=4321)))
        self.service.store = {first["document_id"]: first, second["document_id"]: second}
        self.assert_preparation_failure()

    def test_malformed_target_listing_or_record_is_rejected(self):
        for mode in ("not_list", "missing_content", "invalid_uuid", "duplicate_summary"):
            with self.subTest(mode=mode):
                service = SyntheticServices()
                row = stored_document(source_document(A))
                service.store[row["document_id"]] = row
                def malformed(request):
                    if request.url.host != "target.test":
                        return None
                    if mode == "not_list" and request.url.path == connector.DOCUMENTS:
                        return httpx.Response(200, json={})
                    if mode == "invalid_uuid" and request.url.path == connector.DOCUMENTS:
                        return httpx.Response(200, json=[{"document_id": "..", "document_uri": "x", "title": "x"}])
                    if mode == "duplicate_summary" and request.url.path == connector.DOCUMENTS:
                        body = service.respond(request).json()
                        return httpx.Response(200, json=body + body)
                    if mode == "missing_content" and request.url.path.endswith("/" + row["document_id"]):
                        body = dict(row)
                        del body["document_content"]
                        return httpx.Response(200, json=body)
                service.intercept = malformed
                self.assert_preparation_failure(service)

    def test_foreign_or_legacy_target_identity_rejected(self):
        for row in (stored_document(source_document("foreign")),
                    dict(stored_document(source_document(A)), document_id=str(uuid.UUID(int=123)))):
            with self.subTest(row=row):
                service = SyntheticServices()
                service.store[row["document_id"]] = row
                self.assert_preparation_failure(service)

    def test_expected_id_does_not_license_conflicting_or_missing_metadata(self):
        for metadata in (None, {}, {"item_id": C, "item_sensitivity": "internal"}):
            with self.subTest(metadata=metadata):
                service = SyntheticServices()
                row = dict(stored_document(source_document(A)), client_specific_metadata=metadata)
                service.store[row["document_id"]] = row
                self.assert_preparation_failure(service)

    def test_target_summary_detail_identity_mismatch_rejected(self):
        row = stored_document(source_document(A))
        self.service.store[row["document_id"]] = row
        def mismatch(request):
            if request.url.host == "target.test" and request.url.path.endswith("/" + row["document_id"]):
                return httpx.Response(200, json=dict(row, document_id=connector.document_id(C)))
        self.service.intercept = mismatch
        self.assert_preparation_failure()

    def test_missing_or_empty_content_rejected_before_writes(self):
        for content in (None, "", " \t\n", 123):
            with self.subTest(content=content):
                service = SyntheticServices()
                if content is None:
                    del service.contents[B]
                else:
                    service.contents[B] = content
                self.assert_preparation_failure(service)

    def test_content_envelope_or_echo_mismatch_rejected(self):
        for body in ([], {}, {"item_id": "wrong", "content": "x"}, {"item_id": A}):
            with self.subTest(body=body):
                service = SyntheticServices()
                service.intercept = lambda r, body=body: httpx.Response(200, json=body) if r.url.path.endswith("/content") else None
                self.assert_preparation_failure(service)

    def test_content_preserved_verbatim(self):
        self.service.contents[A] = " \tUnicode \u00e9\nsecond line\r\n "
        self.make_connector().run()
        self.assertEqual(self.service.store[connector.document_id(A)]["document_content"], self.service.contents[A])

    def test_opaque_ids_are_encoded_as_path_segments(self):
        for item_id in ("a/b?\u00e9", " spaced ", ".", ".."):
            with self.subTest(item_id=item_id):
                service = SyntheticServices([source_document(item_id)], [])
                self.make_connector(service).run()
                self.assertEqual(service.store[connector.document_id(item_id)]["client_specific_metadata"]["item_id"], item_id)

    def test_partial_batch_failure_commits_sibling_but_stops_run(self):
        def partial(request):
            if request.method == "POST":
                docs = json.loads(request.content)["documents"]
                self.service.store[docs[0]["document_id"]] = docs[0]
                return httpx.Response(200, json={
                    "succeeded": [{"document_id": docs[0]["document_id"]}],
                    "failed": [{"document_id": docs[1]["document_id"], "error_code": "EMPTY_CONTENT", "error_message": "rejected"}],
                })
        self.service.intercept = partial
        with self.assertRaises(connector.BatchError):
            self.make_connector().run()
        self.assertEqual(len(self.service.posts), 1)
        self.assertEqual(len(self.service.store), 1)

    def test_duplicate_missing_or_unexpected_batch_outcomes_stop_writes(self):
        for mode in ("duplicate", "missing", "unexpected", "overlap", "missing_arrays", "not_object"):
            with self.subTest(mode=mode):
                service = SyntheticServices()
                def malformed(request):
                    if request.method != "POST":
                        return None
                    docs = json.loads(request.content)["documents"]
                    good = [{"document_id": row["document_id"]} for row in docs]
                    bodies = {
                        "duplicate": {"succeeded": [good[0], good[0]], "failed": []},
                        "missing": {"succeeded": good[:1], "failed": []},
                        "unexpected": {"succeeded": good + [{"document_id": str(uuid.UUID(int=1))}], "failed": []},
                        "overlap": {"succeeded": good, "failed": [dict(good[0], error_code="X", error_message="x")]},
                        "missing_arrays": {"succeeded": good}, "not_object": [],
                    }
                    return httpx.Response(200, json=bodies[mode])
                service.intercept = malformed
                with self.assertRaises(connector.AmbiguousWriteError):
                    self.make_connector(service).run()
                self.assertEqual(len(service.posts), 1)

    def test_reordered_success_outcomes_are_valid(self):
        def reverse(request):
            if request.method == "POST":
                body = self.service.respond(request).json()
                body["succeeded"].reverse()
                return httpx.Response(200, json=body)
        self.service.intercept = reverse
        self.make_connector().run()

    def test_unidentifiable_failure_outcome_is_ambiguous(self):
        def unidentifiable(request):
            if request.method == "POST":
                return httpx.Response(200, json={"succeeded": [], "failed": [{
                    "document_id": None, "error_code": "UNRECOGNIZED", "error_message": "cannot correlate",
                }]})
        self.service.intercept = unidentifiable
        with self.assertRaises(connector.AmbiguousWriteError):
            self.make_connector().run()
        self.assertEqual(len(self.service.posts), 1)

    def test_http_422_is_not_retried_or_reported_as_success(self):
        self.service.intercept = lambda r: httpx.Response(422, json={"detail": []}) if r.method == "POST" else None
        with self.assertRaisesRegex(connector.ConnectorError, "HTTP 422"):
            self.make_connector().run()
        self.assertEqual(len(self.service.posts), 1)

    def test_ambiguous_write_after_commit_stops_all_subsequent_writes(self):
        def timeout(request):
            if request.method == "POST":
                self.service.respond(request)
                raise httpx.ReadTimeout("synthetic lost response", request=request)
        self.service.intercept = timeout
        with self.assertRaises(connector.AmbiguousWriteError):
            self.make_connector().run()
        self.assertEqual(len(self.service.posts), 1)
        self.assertEqual(len(self.service.store), 2)
        self.assertEqual(self.service.store[connector.document_id(A)]["title"], "Synthetic " + A)

    def test_write_connection_loss_is_never_retried(self):
        def disconnect(request):
            if request.method == "POST":
                raise httpx.ConnectError("synthetic connection loss", request=request)
        self.service.intercept = disconnect
        with self.assertRaises(connector.AmbiguousWriteError):
            self.make_connector().run()
        self.assertEqual(len(self.service.posts), 1)

    def test_ambiguous_write_status_or_unreadable_response_stops(self):
        for status, raw in ((500, None), (503, None), (307, None), (200, b"not JSON")):
            with self.subTest(status=status, raw=raw):
                service = SyntheticServices()
                service.intercept = lambda r: httpx.Response(status, content=raw or b"") if r.method == "POST" else None
                with self.assertRaises(connector.AmbiguousWriteError):
                    self.make_connector(service).run()
                self.assertEqual(len(service.posts), 1)

    def test_read_retries_are_bounded(self):
        calls = 0
        def timeout(request):
            nonlocal calls
            calls += 1
            raise httpx.ConnectError("synthetic read failure", request=request)
        self.service.intercept = timeout
        self.assert_preparation_failure()
        self.assertEqual(calls, 3)

    def test_transient_read_failure_can_recover(self):
        calls = 0
        def transient(request):
            nonlocal calls
            if request.url.path == "/documents":
                calls += 1
                if calls < 3:
                    return httpx.Response(503)
        self.service.intercept = transient
        self.make_connector().run()
        self.assertEqual(calls, 3)

    def test_malformed_read_json_not_retried(self):
        self.service.intercept = lambda r: httpx.Response(200, content=b"invalid JSON")
        self.assert_preparation_failure()
        self.assertEqual(len(self.service.requests), 1)

    def test_not_found_deletion_confirms_absence_without_fetching_body(self):
        self.service = SyntheticServices([source_document(A)], [event("deleted", B)])
        del self.service.contents[B]
        result = self.make_connector().run()
        self.assertEqual(result["verified_deletions"], 0)
        self.assertEqual(result["already_absent"], 1)
        content_paths = [r.url.path for r in self.service.requests if r.url.host == "source.test"]
        self.assertNotIn(f"/documents/{B}/content", content_paths)
        missing_reads = [r for r in self.service.requests if r.url.host == "target.test" and r.url.path.endswith("/" + connector.document_id(B))]
        self.assertEqual(len(missing_reads), 2)  # NOT_FOUND confirmation and final deletion verification.

    def test_repeated_successful_deletion_accepts_verified_not_found(self):
        row = stored_document(source_document(B))
        self.service.store[row["document_id"]] = row
        instance = self.make_connector()
        self.assertEqual(instance._write_batches([row["document_id"]], deleting=True),
                         ({row["document_id"]}, set()))
        self.assertEqual(instance._write_batches([row["document_id"]], deleting=True),
                         (set(), {row["document_id"]}))
        self.assertEqual(self.service.store, {})
        self.assertEqual(len(self.service.posts), 2)
        self.assertFalse(any(r.url.host == "source.test" for r in self.service.requests))

    def disappearing_deletion_service(self, presence):
        baseline = [] if presence == "preflight" else [source_document(B)]
        events = [event("deleted", B)]
        if presence == "mixed":
            baseline.insert(0, source_document(A))
            events.insert(0, event("deleted", A))
        service = SyntheticServices(baseline, events)
        if presence == "preflight":
            row = stored_document(source_document(B))
            service.store[row["document_id"]] = row

        def disappear(request):
            if request.method == "POST" and request.url.path.endswith("/delete-document-batches"):
                # Simulate state loss by another writer/restart after the preparation barrier.
                service.store.pop(connector.document_id(B), None)
                return service.respond(request)
        service.intercept = disappear
        return service

    def test_not_found_after_acknowledged_baseline_upsert_fails(self):
        service = self.disappearing_deletion_service("upsert")
        with self.assertRaisesRegex(connector.ContractError, "incomplete execution.*known-present"):
            self.make_connector(service).run()
        self.assertEqual(len(service.posts), 2)
        self.assertNotIn(connector.document_id(B), service.store)

    def test_not_found_after_preflight_presence_without_upsert_fails(self):
        service = self.disappearing_deletion_service("preflight")
        with self.assertRaisesRegex(connector.ContractError, "incomplete execution.*known-present"):
            self.make_connector(service).run()
        self.assertEqual(len(service.posts), 1)
        self.assertNotIn(connector.document_id(B), service.store)

    def test_mixed_deletions_cannot_hide_contradictory_not_found(self):
        service = self.disappearing_deletion_service("mixed")
        service.events.append(event("deleted", C))
        with self.assertRaisesRegex(connector.ContractError, "incomplete execution.*known-present"):
            self.make_connector(service, batch_size=2).run()
        self.assertEqual(len(service.posts), 2)
        self.assertEqual(service.store, {})  # A really deleted, B unexpectedly disappeared.

    def test_mixed_success_and_already_absent_have_distinct_counts(self):
        service = SyntheticServices([source_document(A)], [event("deleted", A), event("deleted", B)])
        result = self.make_connector(service).run()
        self.assertEqual(result["verified_deletions"], 1)
        self.assertEqual(result["already_absent"], 1)
        self.assertEqual(result["verified_documents"], 0)

    def test_foreign_preflight_fails_before_content_reads(self):
        row = stored_document(source_document("foreign"))
        self.service.store[row["document_id"]] = row
        self.assert_preparation_failure()
        self.assertFalse(any(r.url.path.endswith("/content") for r in self.service.requests))

    def test_confirm_absent_encodes_target_path_component(self):
        instance = self.make_connector()
        with patch.object(instance, "_get", return_value=connector.ABSENT) as get:
            instance._confirm_absent("a/b?é")
        get.assert_called_once_with(TARGET + connector.DOCUMENTS + "/a%2Fb%3F%C3%A9", absent_ok=True)

    def test_not_found_is_rejected_when_target_still_exists(self):
        def false_absence(request):
            if request.method == "POST" and request.url.path.endswith("/delete-document-batches"):
                target_id = json.loads(request.content)["document_ids"][0]
                return httpx.Response(200, json={"succeeded": [], "failed": [{
                    "document_id": target_id, "error_code": "NOT_FOUND", "error_message": "claimed absent",
                }]})
        self.service.intercept = false_absence
        with self.assertRaisesRegex(connector.ConnectorError, "still present"):
            self.make_connector().run()

    def test_http_200_null_does_not_confirm_absence(self):
        self.service = SyntheticServices([source_document(A)], [event("deleted", B)])
        def invalid_absence(request):
            if request.method == "GET" and request.url.host == "target.test" and request.url.path.endswith("/" + connector.document_id(B)):
                return httpx.Response(200, content=b"null", headers={"Content-Type": "application/json"})
        self.service.intercept = invalid_absence
        with self.assertRaises(connector.ConnectorError):
            self.make_connector().run()

    def test_unknown_item_rejection_stops(self):
        def rejected(request):
            if request.method == "POST":
                docs = json.loads(request.content)["documents"]
                return httpx.Response(200, json={"succeeded": [], "failed": [
                    {"document_id": row["document_id"], "error_code": "UNRECOGNIZED", "error_message": "unknown"}
                    for row in docs
                ]})
        self.service.intercept = rejected
        with self.assertRaises(connector.BatchError):
            self.make_connector().run()
        self.assertEqual(len(self.service.posts), 1)

    def test_verification_catches_same_count_wrong_fields(self):
        for field in ("document_content", "title", "document_uri", "item_sensitivity", "item_id"):
            with self.subTest(field=field):
                service = SyntheticServices()
                def corrupt(request):
                    if request.method == "POST" and request.url.path.endswith("/delete-document-batches"):
                        response = service.respond(request)
                        row = service.store[connector.document_id(A)]
                        if field in ("item_id", "item_sensitivity"):
                            row["client_specific_metadata"][field] = "wrong"
                        else:
                            row[field] = "wrong"
                        return response
                service.intercept = corrupt
                with self.assertRaises(connector.ConnectorError):
                    self.make_connector(service).run()
                self.assertEqual(len(service.store), 2)

    def test_completed_rerun_converges(self):
        self.make_connector().run()
        first = deepcopy(self.service.store)
        self.make_connector().run()
        self.assertEqual(self.service.store, first)

    def test_target_restart_detected_when_unchanged_baseline_record_is_lost(self):
        self.service.baseline.append(source_document("synthetic-unchanged"))
        self.service.contents["synthetic-unchanged"] = "Unchanged synthetic body"
        count = 0
        def restart(request):
            nonlocal count
            if request.method == "POST":
                count += 1
                if count == 2:
                    self.service.store.clear()
        self.service.intercept = restart
        with self.assertRaisesRegex(connector.ConnectorError, "incomplete execution.*known-present"):
            self.make_connector().run()

    def test_observed_identity_collision_stops_before_writes(self):
        with patch.object(connector, "document_id", return_value=str(uuid.UUID(int=1))):
            self.assert_preparation_failure()

    def run_cli(self, service=None, extra=None):
        service = service if service is not None else self.service
        client = httpx.Client(transport=httpx.MockTransport(service), trust_env=False)
        self.addCleanup(client.close)
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.object(connector.httpx, "Client", return_value=client), redirect_stdout(stdout), redirect_stderr(stderr):
            code = connector.main(["--source-url", SOURCE, "--target-url", TARGET] + (extra or []))
        return code, stdout.getvalue(), stderr.getvalue()

    def test_cli_reports_success_only_after_verification(self):
        code, stdout, stderr = self.run_cli()
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout)["status"], "verified")
        self.assertEqual(stderr, "")

    def test_cli_failed_execution_is_nonzero(self):
        self.service.contents[B] = ""
        code, stdout, stderr = self.run_cli()
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        self.assertIn("ERROR", stderr)
        self.assertEqual(self.service.posts, [])

    def test_deletion_state_failures_never_report_cli_success(self):
        for presence in ("upsert", "preflight", "mixed"):
            with self.subTest(presence=presence):
                service = self.disappearing_deletion_service(presence)
                code, stdout, stderr = self.run_cli(service)
                self.assertEqual(code, 1)
                self.assertEqual(stdout, "")
                self.assertIn("incomplete execution", stderr)
        for read_status in (200, 503):
            with self.subTest(read_status=read_status):
                service = SyntheticServices([source_document(A)], [event("deleted", B)])
                def failed_confirmation(request):
                    if request.method == "GET" and request.url.host == "target.test" and request.url.path.endswith("/" + connector.document_id(B)):
                        return httpx.Response(read_status, json=stored_document(source_document(B)))
                service.intercept = failed_confirmation
                code, stdout, stderr = self.run_cli(service)
                self.assertEqual(code, 1)
                self.assertEqual(stdout, "")
                self.assertIn("ERROR", stderr)

    def test_cli_already_absent_success_reports_no_actual_deletion(self):
        service = SyntheticServices([], [event("deleted", B)])
        code, stdout, stderr = self.run_cli(service)
        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(json.loads(stdout)["verified_deletions"], 0)
        self.assertEqual(json.loads(stdout)["already_absent"], 1)

    def test_cli_invalid_config_is_nonzero(self):
        code, stdout, stderr = self.run_cli(extra=["--page-size", "0"])
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        self.assertIn("page_size", stderr)
        self.assertEqual(self.service.requests, [])

    def test_cli_interruption_is_nonzero(self):
        def interrupt(request):
            raise KeyboardInterrupt
        self.service.intercept = interrupt
        code, stdout, stderr = self.run_cli()
        self.assertEqual(code, 130)
        self.assertEqual(stdout, "")
        self.assertIn("interrupted", stderr)
        self.assertEqual(self.service.posts, [])

    def test_interrupted_after_baseline_commit_can_rerun_after_write_finished(self):
        def interrupt_after_commit(request):
            if request.method == "POST":
                self.service.respond(request)
                raise KeyboardInterrupt
        self.service.intercept = interrupt_after_commit
        code, stdout, stderr = self.run_cli()
        self.assertEqual(code, 130)
        self.assertEqual(stdout, "")
        self.assertIn("interrupted", stderr)
        self.assertEqual(len(self.service.posts), 1)
        self.assertIn(connector.document_id(B), self.service.store)
        # This double completed its write synchronously; there is no pending request.
        self.service.intercept = None
        result = self.make_connector().run()
        self.assertEqual(result["status"], "verified")
        self.assertEqual(result["verified_deletions"], 1)
        self.assertEqual(result["already_absent"], 0)
        self.assertNotIn(connector.document_id(B), self.service.store)


if __name__ == "__main__":
    unittest.main()
