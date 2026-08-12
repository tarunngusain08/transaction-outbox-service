#!/usr/bin/env python3
"""Send deterministic mixed traffic and verify the PostgreSQL-to-Kafka pipeline."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CREATE_PATH = "/api/v1/transactions"
NORMALIZE_PATH = "/api/v1/transactions/normalize"
TOPIC = "payments.transactions.created"


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
    transaction_id = first_response.get("transactionId", "00000000-0000-0000-0000-000000000000")
    results.extend([
        send_request(base_url, RequestCase(
            name="reject duplicate reference",
            method="POST",
            path=CREATE_PATH,
            expected_status=409,
            payload=canonical_payload(valid_references[0]),
        )),
        send_request(base_url, RequestCase(
            name="reject non-positive amount",
            method="POST",
            path=CREATE_PATH,
            expected_status=400,
            payload=canonical_payload(f"{run_prefix}INVALID-AMOUNT", amount=0),
        )),
        send_request(base_url, RequestCase(
            name="normalize legacy record",
            method="POST",
            path=NORMALIZE_PATH,
            expected_status=200,
            payload=legacy_payload(f"{run_prefix}NORMALIZE"),
        )),
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

    if valid_references:
        duplicate_reference = valid_references[0]
        results.append(send_request(base_url, RequestCase(
            name="reject duplicate after load",
            method="POST",
            path=CREATE_PATH,
            expected_status=409,
            payload=canonical_payload(duplicate_reference, amount=10_001),
        )))

    results.append(send_request(base_url, RequestCase(
        name="normalize after load",
        method="POST",
        path=NORMALIZE_PATH,
        expected_status=200,
        payload=legacy_payload(f"{run_prefix}NORMALIZE"),
    )))

    return results, valid_references, time.perf_counter() - started


def database_counts(run_prefix: str) -> tuple[int, int]:
    query = (
        "SELECT "
        "(SELECT COUNT(*) FROM transactions "
        f"WHERE external_reference LIKE '{run_prefix}%'), "
        "(SELECT COUNT(*) FROM outbox_events o "
        "JOIN transactions t ON t.id = o.aggregate_id "
        f"WHERE t.external_reference LIKE '{run_prefix}%' AND o.status = 'PUBLISHED');"
    )
    command = [
        "docker", "compose", "exec", "-T", "postgres",
        "psql", "-U", "payments", "-d", "payments", "-At", "-F", "|", "-c", query,
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

    fields = process.stdout.strip().split("|")
    if len(fields) != 2:
        raise RuntimeError(f"unexpected PostgreSQL verification output: {process.stdout!r}")
    return int(fields[0]), int(fields[1])


def wait_for_database(run_prefix: str, expected_count: int, timeout_seconds: int = 30) -> tuple[int, int]:
    deadline = time.monotonic() + timeout_seconds
    latest = (0, 0)

    while time.monotonic() < deadline:
        latest = database_counts(run_prefix)
        if latest == (expected_count, expected_count):
            return latest
        time.sleep(0.5)

    raise RuntimeError(
        "pipeline did not settle in PostgreSQL: "
        f"expected records/published={expected_count}/{expected_count}, got={latest[0]}/{latest[1]}"
    )


def kafka_references(run_prefix: str) -> set[str]:
    command = [
        "docker", "compose", "exec", "-T", "kafka",
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "kafka:29092",
        "--topic", TOPIC,
        "--from-beginning",
        "--timeout-ms", "5000",
    ]
    process = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    references: set[str] = set()

    for line in process.stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        reference = event.get("transaction", {}).get("externalReference", "")
        if reference.startswith(run_prefix):
            references.add(reference)

    if not references and process.returncode != 0:
        raise RuntimeError(f"Kafka verification failed: {process.stderr.strip()}")
    return references


def percentile(values: list[float], percentage: int) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil((percentage / 100) * len(ordered)) - 1)
    return ordered[index]


def print_report(results: list[RequestResult], elapsed_seconds: float, primary_requests: int) -> None:
    status_counts = Counter(result.actual_status for result in results)
    latencies = [result.latency_ms for result in results]
    expected_failures = sum(result.case.expected_status >= 400 for result in results)

    print("\nHTTP results")
    print(f"  Requests: {len(results)} ({primary_requests} primary, {expected_failures} expected error responses)")
    print("  Statuses: " + ", ".join(
        f"{status}={count}" for status, count in sorted(status_counts.items())
    ))
    print(f"  Throughput: {primary_requests / elapsed_seconds:.2f} requests/second")
    print(
        "  Latency: "
        f"p50={percentile(latencies, 50):.1f}ms "
        f"p95={percentile(latencies, 95):.1f}ms "
        f"p99={percentile(latencies, 99):.1f}ms"
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
    parser.add_argument("--skip-pipeline-verification", action="store_true")
    arguments = parser.parse_args()

    if arguments.requests < 1:
        parser.error("--requests must be at least 1")
    if arguments.concurrency < 1:
        parser.error("--concurrency must be at least 1")
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

        if not arguments.skip_pipeline_verification:
            stored, published = wait_for_database(run_prefix, len(expected_references))
            kafka_seen = kafka_references(run_prefix)
            missing_references = set(expected_references) - kafka_seen
            if missing_references:
                sample = ", ".join(sorted(missing_references)[:5])
                raise RuntimeError(
                    f"Kafka is missing {len(missing_references)} expected transaction event(s): {sample}"
                )

            print("\nPipeline verification")
            print(f"  PostgreSQL transactions: {stored}/{len(expected_references)}")
            print(f"  Published outbox rows: {published}/{len(expected_references)}")
            print(f"  Distinct Kafka events: {len(kafka_seen)}/{len(expected_references)}")

        print("\nPASS: all expected HTTP outcomes and pipeline checks succeeded")
        return 0
    except (RuntimeError, subprocess.SubprocessError, ValueError) as error:
        print(f"\nFAIL: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
