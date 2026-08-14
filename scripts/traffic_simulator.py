#!/usr/bin/env python3
"""Send a small mixed request set and verify PostgreSQL-to-Kafka delivery."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

TOPIC = "payments.transactions.created"


def send(base_url: str, path: str, payload: dict[str, Any] | None = None) -> tuple[int, str]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        base_url + path,
        data=body,
        headers={"Content-Type": "application/json"} if body else {},
        method="POST" if body else "GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8")


def wait_for_service(base_url: str) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            status, _ = send(base_url, "/actuator/health")
            if status == 200:
                return
        except urllib.error.URLError:
            pass
        time.sleep(1)
    raise RuntimeError("service did not become healthy within 30 seconds")


def canonical_payload(reference: str, amount: int = 150_000) -> dict[str, Any]:
    return {
        "externalReference": reference,
        "amount": amount,
        "currency": "INR",
        "type": "DEBIT",
        "sourceAccount": "1234567890",
        "destinationAccount": "9876543210",
        "channel": "UPI",
        "metadata": {"source": "traffic-simulator"},
    }


def legacy_payload(reference: str) -> dict[str, Any]:
    return {
        "txn_ref": reference,
        "txn_amount": "1500.00",
        "ccy": "INR",
        "txn_type": "DR",
        "payer": {"acct_no": "1234567890", "ifsc": "HDFC0001234"},
        "payee": {"acct_no": "9876543210", "ifsc": "ICIC0005678"},
        "mode": "UPI",
        "txn_date": "08-08-2026 14:32:11",
        "remarks": "Traffic simulator",
    }


def published_references(references: list[str]) -> set[str]:
    quoted = ",".join("'" + reference + "'" for reference in references)
    sql = (
        "SELECT t.external_reference FROM transactions t "
        "JOIN outbox_events o ON o.aggregate_id = t.id "
        f"WHERE t.external_reference IN ({quoted}) AND o.status = 'PUBLISHED' "
        "ORDER BY t.external_reference;"
    )
    process = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "postgres", "psql", "-X", "-A", "-t",
            "-U", "payments", "-d", "payments", "-c", sql,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return {line.strip() for line in process.stdout.splitlines() if line.strip()}


def wait_for_publication(references: list[str]) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if published_references(references) == set(references):
            return
        time.sleep(1)
    raise RuntimeError("expected transaction outbox rows were not published")


def kafka_references() -> set[str]:
    process = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "kafka",
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server", "kafka:29092",
            "--topic", TOPIC,
            "--from-beginning",
            "--timeout-ms", "10000",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    references: set[str] = set()
    for line in process.stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        reference = event.get("transaction", {}).get("externalReference")
        if reference:
            references.add(reference)
    return references


def expect(actual: int, expected: int, label: str) -> None:
    if actual != expected:
        raise RuntimeError(f"{label}: expected HTTP {expected}, got {actual}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8080")
    arguments = parser.parse_args()

    wait_for_service(arguments.base_url)
    prefix = "SIM-" + uuid.uuid4().hex[:12].upper()
    direct_reference = prefix + "-DIRECT"
    invalid_reference = prefix + "-INVALID"
    normalized_reference = prefix + "-NORMALIZED"

    status, _ = send(arguments.base_url, "/api/v1/transactions", canonical_payload(direct_reference))
    expect(status, 201, "valid create")

    status, _ = send(
        arguments.base_url,
        "/api/v1/transactions",
        canonical_payload(invalid_reference, amount=0),
    )
    expect(status, 400, "invalid create")

    status, normalized_body = send(
        arguments.base_url,
        "/api/v1/transactions/normalize",
        legacy_payload(normalized_reference),
    )
    expect(status, 200, "normalization")

    status, _ = send(
        arguments.base_url,
        "/api/v1/transactions",
        json.loads(normalized_body),
    )
    expect(status, 201, "create normalized transaction")

    expected = [direct_reference, normalized_reference]
    wait_for_publication(expected)
    missing = set(expected) - kafka_references()
    if missing:
        raise RuntimeError(f"Kafka is missing expected references: {sorted(missing)}")

    print("Traffic verification passed")
    print("  HTTP: valid create=201, invalid create=400, normalize=200, normalized create=201")
    print("  PostgreSQL/outbox: 2/2 published")
    print("  Kafka: 2/2 transaction-created events found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
