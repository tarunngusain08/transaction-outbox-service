#!/usr/bin/env python3
"""Send deterministic mixed traffic and verify the PostgreSQL-to-Kafka pipeline."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CREATE_PATH = "/api/v2/transactions"
NORMALIZE_PATH = "/api/v2/transactions/normalize"
RETIRED_CREATE_PATH = "/api/v1/transactions"
TOPIC = "payments.transactions.created"
TRANSACTION_FIELDS = {
    "transactionId",
    "sourceSystem",
    "externalReference",
    "amount",
    "currency",
    "type",
    "status",
    "sourceAccount",
    "destinationAccount",
    "channel",
    "createdAt",
    "receivedAt",
    "metadata",
}
NORMALIZED_FIELDS = {
    "sourceSystem",
    "externalReference",
    "amount",
    "currency",
    "type",
    "sourceAccount",
    "destinationAccount",
    "channel",
    "createdAt",
    "metadata",
}
OUTBOX_FIELDS = {
    "outboxId",
    "aggregateType",
    "aggregateId",
    "eventType",
    "payload",
    "status",
    "createdAt",
    "publishedAt",
    "nextAttemptAt",
    "retryCount",
    "lastError",
    "claimToken",
    "claimedAt",
}
EVENT_FIELDS = {
    "schemaVersion",
    "producer",
    "eventId",
    "eventType",
    "occurredAt",
    "transaction",
}


@dataclass(frozen=True)
class RequestCase:
    name: str
    method: str
    path: str
    expected_status: int
    payload: dict[str, Any] | None = None


@dataclass(frozen=True)
class RequestResult:
    case: RequestCase
    actual_status: int
    latency_ms: float
    response_body: str

    @property
    def passed(self) -> bool:
        return self.actual_status == self.case.expected_status


def canonical_payload(external_reference: str, amount: int = 150_000) -> dict[str, Any]:
    return {
        "sourceSystem": "DIRECT_API",
        "externalReference": external_reference,
        "amount": amount,
        "currency": "INR",
        "type": "DEBIT",
        "sourceAccount": "1234567890",
        "destinationAccount": "9876543210",
        "channel": "UPI",
        "metadata": {"generator": "traffic-simulator"},
    }


def legacy_payload(external_reference: str, transaction_type: str = "DR") -> dict[str, Any]:
    return {
        "txn_ref": external_reference,
        "txn_amount": "1500.00",
        "ccy": "INR",
        "txn_type": transaction_type,
        "payer": {"acct_no": "1234567890", "ifsc": "HDFC0001234"},
        "payee": {"acct_no": "9876543210", "ifsc": "ICIC0005678"},
        "mode": "UPI",
        "txn_date": "08-08-2026 14:32:11",
        "remarks": "Generated pipeline traffic",
    }


def historical_v1_payload(external_reference: str) -> dict[str, Any]:
    return {
        "transactionId": str(uuid.uuid4()),
        "externalReference": external_reference,
        "amount": 150_000,
        "currency": "INR",
        "type": "DEBIT",
        "status": "PENDING",
        "sourceAccount": "1234567890",
        "destinationAccount": "9876543210",
        "channel": "UPI",
        "metadata": {"generator": "traffic-simulator-v1-probe"},
    }


def send_request(base_url: str, case: RequestCase) -> RequestResult:
    body = None if case.payload is None else json.dumps(case.payload).encode("utf-8")
    request = urllib.request.Request(
        base_url + case.path,
        data=body,
        method=case.method,
        headers={"Content-Type": "application/json"},
    )
    started = time.perf_counter()

    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            status = response.status
            response_body = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        status = error.code
        response_body = error.read().decode("utf-8")
    except (OSError, urllib.error.URLError) as error:
        status = 0
        response_body = str(error)

    return RequestResult(
        case=case,
        actual_status=status,
        latency_ms=(time.perf_counter() - started) * 1000,
        response_body=response_body,
    )


def wait_for_service(base_url: str, timeout_seconds: int = 30) -> None:
    deadline = time.monotonic() + timeout_seconds
    health_url = base_url + "/actuator/health"

    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=2) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.5)

    raise RuntimeError(f"service did not become healthy at {health_url}")


def run_smoke(base_url: str, run_prefix: str) -> tuple[list[RequestResult], list[str], float]:
    valid_references = [f"{run_prefix}SUCCESS-{index:03d}" for index in range(1, 5)]
    results: list[RequestResult] = []
    started = time.perf_counter()

    for reference in valid_references:
        results.append(send_request(base_url, RequestCase(
            name=f"create {reference}",
            method="POST",
            path=CREATE_PATH,
            expected_status=201,
            payload=canonical_payload(reference),
        )))

    first_response = json.loads(results[0].response_body) if results[0].actual_status == 201 else {}
    if not isinstance(first_response, dict):
        first_response = {}
    transaction_id = first_response.get("transactionId", "00000000-0000-0000-0000-000000000000")
    results.extend([
        send_request(base_url, RequestCase(
            name="accept idempotent replay",
            method="POST",
            path=CREATE_PATH,
            expected_status=200,
            payload=canonical_payload(valid_references[0]),
        )),
        send_request(base_url, RequestCase(
            name="reject conflicting replay",
            method="POST",
            path=CREATE_PATH,
            expected_status=409,
            payload=canonical_payload(valid_references[0], amount=999),
        )),
        send_request(base_url, RequestCase(
            name="reject non-positive amount",
            method="POST",
            path=CREATE_PATH,
            expected_status=400,
            payload=canonical_payload(f"{run_prefix}INVALID-AMOUNT", amount=0),
        )),
        send_request(base_url, RequestCase(
            name="reject retired V1 without durable state",
            method="POST",
            path=RETIRED_CREATE_PATH,
            expected_status=410,
            payload=historical_v1_payload(f"{run_prefix}RETIRED-V1"),
        )),
    ])

    normalized_reference = f"{run_prefix}NORMALIZE"
    normalization = send_request(base_url, RequestCase(
        name="normalize legacy record",
        method="POST",
        path=NORMALIZE_PATH,
        expected_status=200,
        payload=legacy_payload(normalized_reference),
    ))
    results.append(normalization)
    if normalization.actual_status == 200:
        normalized_payload = json.loads(normalization.response_body)
        if not isinstance(normalized_payload, dict):
            raise RuntimeError("normalization response was not a JSON object")
        results.append(send_request(base_url, RequestCase(
            name="create normalized record unchanged",
            method="POST",
            path=CREATE_PATH,
            expected_status=201,
            payload=normalized_payload,
        )))
        valid_references.append(normalized_reference)

    results.extend([
        send_request(base_url, RequestCase(
            name="reject unsupported legacy type",
            method="POST",
            path=NORMALIZE_PATH,
            expected_status=422,
            payload=legacy_payload(f"{run_prefix}INVALID-TYPE", transaction_type="SIDEWAYS"),
        )),
        send_request(base_url, RequestCase(
            name="fetch created transaction",
            method="GET",
            path=f"{CREATE_PATH}/{transaction_id}",
            expected_status=200,
        )),
    ])

    return results, valid_references, time.perf_counter() - started


def run_load(
        base_url: str,
        run_prefix: str,
        request_count: int,
        concurrency: int,
) -> tuple[list[RequestResult], list[str], float]:
    cases: list[RequestCase] = []
    valid_references: list[str] = []

    for index in range(1, request_count + 1):
        if index % 5 == 0:
            cases.append(RequestCase(
                name=f"reject invalid load request {index}",
                method="POST",
                path=CREATE_PATH,
                expected_status=400,
                payload=canonical_payload(f"{run_prefix}INVALID-{index:05d}", amount=0),
            ))
        else:
            reference = f"{run_prefix}SUCCESS-{index:05d}"
            valid_references.append(reference)
            cases.append(RequestCase(
                name=f"create load transaction {index}",
                method="POST",
                path=CREATE_PATH,
                expected_status=201,
                payload=canonical_payload(reference, amount=10_000 + index),
            ))

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        results = list(executor.map(lambda case: send_request(base_url, case), cases))
    primary_elapsed = time.perf_counter() - started

    if valid_references:
        duplicate_reference = valid_references[0]
        results.append(send_request(base_url, RequestCase(
            name="accept idempotent replay after load",
            method="POST",
            path=CREATE_PATH,
            expected_status=200,
            payload=canonical_payload(duplicate_reference, amount=10_001),
        )))
        results.append(send_request(base_url, RequestCase(
            name="reject conflicting replay after load",
            method="POST",
            path=CREATE_PATH,
            expected_status=409,
            payload=canonical_payload(duplicate_reference, amount=999),
        )))

    normalized_reference = f"{run_prefix}NORMALIZE"
    normalization = send_request(base_url, RequestCase(
        name="normalize after load",
        method="POST",
        path=NORMALIZE_PATH,
        expected_status=200,
        payload=legacy_payload(normalized_reference),
    ))
    results.append(normalization)
    if normalization.actual_status == 200:
        normalized_payload = json.loads(normalization.response_body)
        if not isinstance(normalized_payload, dict):
            raise RuntimeError("normalization response was not a JSON object")
        results.append(send_request(base_url, RequestCase(
            name="create normalized record unchanged after load",
            method="POST",
            path=CREATE_PATH,
            expected_status=201,
            payload=normalized_payload,
        )))
        valid_references.append(normalized_reference)

    return results, valid_references, primary_elapsed


def database_pipeline_records(run_prefix: str) -> list[dict[str, Any]]:
    if not re.fullmatch(r"SIM-(?:SMOKE|LOAD)-[A-F0-9]{12}-", run_prefix):
        raise RuntimeError("refusing to interpolate an invalid simulator run prefix")

    query = f"""
        SELECT json_build_object(
            'transaction', json_build_object(
                'transactionId', t.id::text,
                'sourceSystem', t.source_system,
                'externalReference', t.external_reference,
                'amount', t.amount_minor,
                'currency', t.currency,
                'type', t.type,
                'status', t.status,
                'sourceAccount', t.source_account,
                'destinationAccount', t.destination_account,
                'channel', t.channel,
                'createdAt', TO_CHAR(
                    t.created_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ),
                'receivedAt', TO_CHAR(
                    t.received_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ),
                'metadata', t.metadata
            ),
            'requestFingerprint', t.request_fingerprint,
            'requestFingerprintVersion', t.request_fingerprint_version,
            'outbox', CASE WHEN o.id IS NULL THEN NULL ELSE json_build_object(
                'outboxId', o.id::text,
                'aggregateType', o.aggregate_type,
                'aggregateId', o.aggregate_id::text,
                'eventType', o.event_type,
                'payload', o.payload,
                'status', o.status,
                'createdAt', TO_CHAR(
                    o.created_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ),
                'publishedAt', CASE WHEN o.published_at IS NULL THEN NULL ELSE TO_CHAR(
                    o.published_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ) END,
                'nextAttemptAt', TO_CHAR(
                    o.next_attempt_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ),
                'retryCount', o.retry_count,
                'lastError', o.last_error,
                'claimToken', o.claim_token::text,
                'claimedAt', CASE WHEN o.claimed_at IS NULL THEN NULL ELSE TO_CHAR(
                    o.claimed_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ) END
            ) END
        )::text
        FROM transactions t
        LEFT JOIN outbox_events o ON o.aggregate_id = t.id
        WHERE t.external_reference LIKE '{run_prefix}%'
        ORDER BY t.external_reference, o.created_at, o.id;
    """
    command = [
        "docker", "compose", "exec", "-T", "postgres",
        "psql", "-U", "payments", "-d", "payments", "-At", "-c", query,
    ]
    process = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        check=False,
        text=True,
        timeout=10,
    )
    if process.returncode != 0:
        raise RuntimeError(f"PostgreSQL verification failed: {process.stderr.strip()}")

    records: list[dict[str, Any]] = []
    for raw_record in process.stdout.splitlines():
        if not raw_record.strip():
            continue
        try:
            record = json.loads(raw_record)
        except json.JSONDecodeError as error:
            raise RuntimeError(
                f"unexpected PostgreSQL verification output: {raw_record!r}"
            ) from error
        if not isinstance(record, dict):
            raise RuntimeError("PostgreSQL verification returned a non-object record")
        records.append(record)
    return records


def wait_for_database_records(
        run_prefix: str,
        expected_references: set[str],
        timeout_seconds: int = 30,
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout_seconds
    latest: list[dict[str, Any]] = []

    while time.monotonic() < deadline:
        latest = database_pipeline_records(run_prefix)
        references = [
            record.get("transaction", {}).get("externalReference")
            for record in latest
            if isinstance(record.get("transaction"), dict)
        ]
        unexpected = set(references) - expected_references
        if unexpected:
            raise RuntimeError(
                "PostgreSQL contains unexpected simulator references: "
                + ", ".join(sorted(str(reference) for reference in unexpected)[:5])
            )
        duplicates = [
            reference for reference, count in Counter(references).items() if count > 1
        ]
        if duplicates:
            raise RuntimeError(
                "PostgreSQL contains multiple outbox rows for transaction references: "
                + ", ".join(sorted(str(reference) for reference in duplicates)[:5])
            )
        all_published = all(
            isinstance(record.get("outbox"), dict)
            and record["outbox"].get("status") == "PUBLISHED"
            for record in latest
        )
        if set(references) == expected_references and all_published:
            return latest
        time.sleep(0.5)

    latest_states = {
        record.get("transaction", {}).get("externalReference"): (
            record.get("outbox", {}).get("status")
            if isinstance(record.get("outbox"), dict)
            else "MISSING"
        )
        for record in latest
        if isinstance(record.get("transaction"), dict)
    }
    raise RuntimeError(
        "pipeline did not settle in PostgreSQL: "
        f"expected={len(expected_references)}, observed={len(latest)}, states={latest_states}"
    )


def parse_instant(value: Any, context: str) -> datetime:
    if not isinstance(value, str):
        raise RuntimeError(f"{context} is not an ISO-8601 string")
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise RuntimeError(f"{context} is not a valid ISO-8601 instant") from error
    if instant.tzinfo is None:
        raise RuntimeError(f"{context} has no UTC offset")
    return instant


def assert_transaction_matches(
        actual: Any,
        expected: dict[str, Any],
        context: str,
) -> None:
    if not isinstance(actual, dict):
        raise RuntimeError(f"{context} is not a JSON object")
    if set(actual) != TRANSACTION_FIELDS:
        raise RuntimeError(
            f"{context} fields differ: expected={sorted(TRANSACTION_FIELDS)}, "
            f"actual={sorted(actual)}"
        )
    for field in TRANSACTION_FIELDS - {"createdAt", "receivedAt"}:
        if actual[field] != expected[field]:
            raise RuntimeError(
                f"{context}.{field} differs: expected={expected[field]!r}, actual={actual[field]!r}"
            )
    for field in ("createdAt", "receivedAt"):
        if parse_instant(actual[field], f"{context}.{field}") != parse_instant(
                expected[field],
                f"expected transaction.{field}",
        ):
            raise RuntimeError(f"{context}.{field} differs from the HTTP response")


def expected_transactions(results: list[RequestResult], run_prefix: str) -> dict[str, dict[str, Any]]:
    expected: dict[str, dict[str, Any]] = {}
    for result in results:
        if result.case.path != CREATE_PATH or result.actual_status != 201:
            continue
        transaction = json.loads(result.response_body)
        if not isinstance(transaction, dict):
            raise RuntimeError("a successful create response was not a JSON object")
        if set(transaction) != TRANSACTION_FIELDS:
            raise RuntimeError(
                f"successful create response fields differ for {result.case.name}"
            )
        reference = transaction.get("externalReference", "")
        if reference.startswith(run_prefix):
            if reference in expected:
                raise RuntimeError(f"multiple 201 responses created {reference}")
            try:
                canonical_id = str(uuid.UUID(str(transaction.get("transactionId"))))
            except (TypeError, ValueError) as error:
                raise RuntimeError(f"HTTP transactionId is invalid for {reference}") from error
            if transaction["transactionId"] != canonical_id:
                raise RuntimeError(f"HTTP transactionId is not canonical for {reference}")
            if transaction["status"] != "PENDING":
                raise RuntimeError(f"HTTP transaction status is not PENDING for {reference}")
            parse_instant(transaction["createdAt"], f"HTTP transaction createdAt for {reference}")
            parse_instant(transaction["receivedAt"], f"HTTP transaction receivedAt for {reference}")
            expected[reference] = transaction
    return expected


def validate_http_results(
        results: list[RequestResult],
        expected: dict[str, dict[str, Any]],
        run_prefix: str,
) -> None:
    expected_by_id = {
        transaction["transactionId"]: transaction for transaction in expected.values()
    }

    for result in results:
        if result.actual_status != result.case.expected_status:
            continue
        if result.case.path == CREATE_PATH and result.actual_status == 200:
            replay = json.loads(result.response_body)
            reference = replay.get("externalReference") if isinstance(replay, dict) else None
            if reference not in expected:
                raise RuntimeError(f"replay response has unknown reference {reference!r}")
            assert_transaction_matches(replay, expected[reference], "HTTP replay response")
        elif result.case.method == "GET" and result.actual_status == 200:
            fetched = json.loads(result.response_body)
            transaction_id = fetched.get("transactionId") if isinstance(fetched, dict) else None
            if transaction_id not in expected_by_id:
                raise RuntimeError(f"GET response has unknown transactionId {transaction_id!r}")
            assert_transaction_matches(fetched, expected_by_id[transaction_id], "HTTP GET response")
        elif result.case.path == NORMALIZE_PATH and result.actual_status == 200:
            normalized = json.loads(result.response_body)
            if not isinstance(normalized, dict) or set(normalized) != NORMALIZED_FIELDS:
                raise RuntimeError("normalization response is not the exact create-request schema")
            source_reference = (result.case.payload or {}).get("txn_ref")
            if normalized["sourceSystem"] != "LEGACY_BANK_FEED":
                raise RuntimeError("normalization response has the wrong sourceSystem")
            if normalized["externalReference"] != source_reference:
                raise RuntimeError("normalization response changed the source reference")
            if not normalized["externalReference"].startswith(run_prefix):
                raise RuntimeError("normalization response escaped the simulator run prefix")
            if normalized["amount"] != 150_000 or normalized["currency"] != "INR":
                raise RuntimeError("normalization response has the wrong canonical amount or currency")
            parse_instant(normalized["createdAt"], "normalization response createdAt")
        elif result.case.path == RETIRED_CREATE_PATH and result.actual_status == 410:
            problem = json.loads(result.response_body)
            if not isinstance(problem, dict):
                raise RuntimeError("retired V1 response was not a problem object")
            if problem.get("type") != "urn:transaction-outbox-service:api-version-retired":
                raise RuntimeError("retired V1 response has the wrong problem type")
            if problem.get("successor") != CREATE_PATH:
                raise RuntimeError("retired V1 response has the wrong V2 successor")


def validate_database_records(
        records: list[dict[str, Any]],
        expected: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    records_by_reference: dict[str, dict[str, Any]] = {}

    for record in records:
        transaction = record.get("transaction")
        if not isinstance(transaction, dict):
            raise RuntimeError("PostgreSQL record is missing its transaction object")
        reference = transaction.get("externalReference")
        if reference not in expected:
            raise RuntimeError(f"PostgreSQL record has unexpected reference {reference!r}")
        if reference in records_by_reference:
            raise RuntimeError(f"PostgreSQL returned duplicate pipeline record for {reference}")
        assert_transaction_matches(
            transaction,
            expected[reference],
            f"PostgreSQL transaction for {reference}",
        )

        fingerprint = record.get("requestFingerprint")
        if not isinstance(fingerprint, str) or not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
            raise RuntimeError(f"PostgreSQL fingerprint is invalid for {reference}")
        if record.get("requestFingerprintVersion") != 1:
            raise RuntimeError(f"PostgreSQL fingerprint version is not 1 for {reference}")

        outbox = record.get("outbox")
        if not isinstance(outbox, dict) or set(outbox) != OUTBOX_FIELDS:
            raise RuntimeError(f"PostgreSQL outbox row is missing or malformed for {reference}")
        try:
            outbox_id = str(uuid.UUID(str(outbox["outboxId"])))
        except (TypeError, ValueError) as error:
            raise RuntimeError(f"outbox ID is invalid for {reference}") from error
        if outbox["outboxId"] != outbox_id:
            raise RuntimeError(f"outbox ID is not canonical for {reference}")
        if outbox["aggregateType"] != "TRANSACTION":
            raise RuntimeError(f"outbox aggregateType is invalid for {reference}")
        if outbox["aggregateId"] != transaction["transactionId"]:
            raise RuntimeError(f"outbox aggregateId differs from transactionId for {reference}")
        if outbox["eventType"] != "TRANSACTION_CREATED":
            raise RuntimeError(f"outbox eventType is invalid for {reference}")
        if outbox["status"] != "PUBLISHED" or outbox["publishedAt"] is None:
            raise RuntimeError(f"outbox row is not durably published for {reference}")
        if outbox["retryCount"] != 0:
            raise RuntimeError(f"unexpected outbox retry occurred for {reference}")
        if any(outbox[field] is not None for field in ("lastError", "claimToken", "claimedAt")):
            raise RuntimeError(f"published outbox ownership/error fields are not clear for {reference}")

        payload = outbox["payload"]
        if not isinstance(payload, dict) or set(payload) != EVENT_FIELDS:
            raise RuntimeError(f"stored outbox payload schema is invalid for {reference}")
        if payload["schemaVersion"] != 2:
            raise RuntimeError(f"stored event schemaVersion is not 2 for {reference}")
        if payload["producer"] != "transaction-outbox-service":
            raise RuntimeError(f"stored event producer is invalid for {reference}")
        if payload["eventId"] != outbox_id:
            raise RuntimeError(f"stored eventId differs from outbox ID for {reference}")
        if payload["eventType"] != outbox["eventType"]:
            raise RuntimeError(f"stored payload eventType differs from its outbox row for {reference}")
        assert_transaction_matches(
            payload["transaction"],
            expected[reference],
            f"stored outbox transaction for {reference}",
        )

        received_at = parse_instant(transaction["receivedAt"], f"transaction receivedAt for {reference}")
        occurred_at = parse_instant(payload["occurredAt"], f"stored event occurredAt for {reference}")
        outbox_created_at = parse_instant(outbox["createdAt"], f"outbox createdAt for {reference}")
        next_attempt_at = parse_instant(
            outbox["nextAttemptAt"],
            f"outbox nextAttemptAt for {reference}",
        )
        published_at = parse_instant(outbox["publishedAt"], f"outbox publishedAt for {reference}")
        if not received_at == occurred_at == outbox_created_at == next_attempt_at:
            raise RuntimeError(
                f"transaction/outbox/event creation timestamps do not correlate for {reference}"
            )
        if published_at < outbox_created_at:
            raise RuntimeError(f"outbox publishedAt precedes createdAt for {reference}")

        records_by_reference[reference] = record

    if set(records_by_reference) != set(expected):
        raise RuntimeError("PostgreSQL pipeline records do not exactly match successful HTTP creates")
    return records_by_reference


def kafka_events(
        run_prefix: str,
        database_records: dict[str, dict[str, Any]],
) -> tuple[dict[str, set[str]], int]:
    command = [
        "docker", "compose", "exec", "-T", "kafka",
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "kafka:29092",
        "--topic", TOPIC,
        "--from-beginning",
        "--timeout-ms", "5000",
        "--property", "print.key=true",
        "--property", "key.separator=\t",
    ]
    process = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    event_ids_by_reference: dict[str, set[str]] = {}
    matching_occurrences = 0

    for line in process.stdout.splitlines():
        key, separator, raw_event = line.partition("\t")
        if not separator:
            continue
        try:
            event = json.loads(raw_event)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        transaction = event.get("transaction")
        reference = transaction.get("externalReference", "") if isinstance(transaction, dict) else ""
        if not reference.startswith(run_prefix):
            continue

        matching_occurrences += 1
        if reference not in database_records:
            raise RuntimeError(f"Kafka contains an unexpected event for {reference}")
        database_record = database_records[reference]
        outbox = database_record["outbox"]
        if event != outbox["payload"]:
            raise RuntimeError(f"Kafka body differs from the stored outbox payload for {reference}")
        if key != outbox["aggregateId"]:
            raise RuntimeError(f"Kafka key differs from the stored aggregateId for {reference}")
        event_id = event["eventId"]
        if event_id != outbox["outboxId"]:
            raise RuntimeError(f"Kafka eventId differs from the stored outbox ID for {reference}")
        event_ids_by_reference.setdefault(reference, set()).add(event_id)

    if not event_ids_by_reference and process.returncode != 0:
        raise RuntimeError(f"Kafka verification failed: {process.stderr.strip()}")
    return event_ids_by_reference, matching_occurrences


def percentile(values: list[float], percentage: int) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil((percentage / 100) * len(ordered)) - 1)
    return ordered[index]


def print_report(results: list[RequestResult], elapsed_seconds: float, primary_requests: int) -> None:
    status_counts = Counter(result.actual_status for result in results)
    primary_latencies = [result.latency_ms for result in results[:primary_requests]]
    expected_failures = sum(result.case.expected_status >= 400 for result in results)

    print("\nHTTP results")
    print(f"  Requests: {len(results)} ({primary_requests} primary, {expected_failures} expected error responses)")
    print("  Statuses: " + ", ".join(
        f"{status}={count}" for status, count in sorted(status_counts.items())
    ))
    print(f"  Throughput: {primary_requests / elapsed_seconds:.2f} requests/second")
    print(
        "  Latency: "
        f"p50={percentile(primary_latencies, 50):.1f}ms "
        f"p95={percentile(primary_latencies, 95):.1f}ms "
        f"p99={percentile(primary_latencies, 99):.1f}ms (primary requests only)"
    )

    failures = [result for result in results if not result.passed]
    if failures:
        print("\nUnexpected responses", file=sys.stderr)
        for result in failures:
            response = result.response_body.replace("\n", " ")[:200]
            print(
                f"  {result.case.name}: expected {result.case.expected_status}, "
                f"got {result.actual_status}; {response}",
                file=sys.stderr,
            )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("smoke", "load"), default="smoke")
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--min-throughput", type=float, default=0)
    parser.add_argument("--max-p95-ms", type=float, default=0)
    parser.add_argument("--skip-pipeline-verification", action="store_true")
    arguments = parser.parse_args()

    if arguments.requests < 1:
        parser.error("--requests must be at least 1")
    if arguments.concurrency < 1:
        parser.error("--concurrency must be at least 1")
    if arguments.min_throughput < 0:
        parser.error("--min-throughput cannot be negative")
    if arguments.max_p95_ms < 0:
        parser.error("--max-p95-ms cannot be negative")
    return arguments


def main() -> int:
    arguments = parse_arguments()
    run_prefix = f"SIM-{arguments.mode.upper()}-{uuid.uuid4().hex[:12].upper()}-"

    try:
        wait_for_service(arguments.base_url)
        print(f"Traffic run: mode={arguments.mode}, prefix={run_prefix}")

        if arguments.mode == "smoke":
            results, expected_references, elapsed = run_smoke(arguments.base_url, run_prefix)
            primary_requests = len(results)
        else:
            results, expected_references, elapsed = run_load(
                arguments.base_url,
                run_prefix,
                arguments.requests,
                arguments.concurrency,
            )
            primary_requests = arguments.requests

        print_report(results, elapsed, primary_requests)
        if any(not result.passed for result in results):
            return 1

        if arguments.mode == "load":
            throughput = primary_requests / elapsed
            p95_ms = percentile(
                [result.latency_ms for result in results[:primary_requests]],
                95,
            )
            if arguments.min_throughput and throughput < arguments.min_throughput:
                raise RuntimeError(
                    f"throughput {throughput:.2f} req/s is below "
                    f"{arguments.min_throughput:.2f} req/s"
                )
            if arguments.max_p95_ms and p95_ms > arguments.max_p95_ms:
                raise RuntimeError(
                    f"p95 latency {p95_ms:.1f}ms exceeds {arguments.max_p95_ms:.1f}ms"
                )

        if not arguments.skip_pipeline_verification:
            expected = expected_transactions(results, run_prefix)
            if set(expected) != set(expected_references):
                raise RuntimeError("successful HTTP responses do not match the expected references")
            validate_http_results(results, expected, run_prefix)
            database_records = wait_for_database_records(run_prefix, set(expected))
            records_by_reference = validate_database_records(database_records, expected)
            kafka_seen, kafka_occurrences = kafka_events(run_prefix, records_by_reference)
            missing_references = set(expected) - set(kafka_seen)
            if missing_references:
                sample = ", ".join(sorted(missing_references)[:5])
                raise RuntimeError(
                    f"Kafka is missing {len(missing_references)} expected transaction event(s): {sample}"
                )
            contradictory_references = [
                reference for reference, event_ids in kafka_seen.items() if len(event_ids) != 1
            ]
            if contradictory_references:
                raise RuntimeError(
                    "Kafka contains multiple creation event IDs for: "
                    + ", ".join(sorted(contradictory_references)[:5])
                )

            print("\nPipeline verification")
            print(f"  Exact PostgreSQL transaction rows: {len(database_records)}/{len(expected)}")
            print(f"  Exact published outbox rows/payloads: {len(records_by_reference)}/{len(expected)}")
            print(f"  Exact Kafka contracts: {len(kafka_seen)}/{len(expected)}")
            print(f"  Kafka occurrences (at-least-once): {kafka_occurrences}")

        print("\nPASS: all expected HTTP outcomes and pipeline checks succeeded")
        return 0
    except (RuntimeError, subprocess.SubprocessError, ValueError) as error:
        print(f"\nFAIL: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
