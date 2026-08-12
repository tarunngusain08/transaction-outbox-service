# ADR 0003: Preserve applied Flyway migrations and move forward

- Status: accepted
- Date: 2026-08-10
- Scope: Flyway V1-V7 and databases created by earlier local artifacts

## Context

An earlier remediation edited V3 and replaced the already-existing V6 file.
Flyway records a checksum for every applied version, so those edits made a
database created by artifact `1d9d97c` fail validation before startup. A clean
database was unaffected, but that did not make the upgrade path safe.

## Decision

1. V1 through V6 are restored byte-for-byte from `1d9d97c` and are immutable.
2. Their SHA-256 digests are pinned by an executable test.
3. Quarantine, the numeric V2 discriminator constraint, the quarantine index,
   and the corrected audit-trigger wording are introduced only by V7.
4. Integration tests first migrate a populated database to canonical V6, then
   apply V7 and exercise the real publisher against PostgreSQL and Kafka.
5. Production rollout is stop-the-world and follows the V7 runbook. A zero
   historical-unpublished count permits release; a nonzero count must be drained
   first or explicitly remains a release blocker pending UC-P08.
6. `flyway repair`, silent data deletion, reference merging, and arbitrary
   status flips are not reconciliation mechanisms.

## Consequences

Clean installations and databases that already applied canonical V1-V6 share
one tested migration chain. Quarantining incompatible payloads is fail-safe
containment, not proof that their events were delivered.

A disposable local database that ran the temporary rewritten V3/V6 chain must
be reset. A non-disposable database with those noncanonical checksums needs its
own captured, reviewed, forward-only transition; this repository does not
pretend the two divergent histories are interchangeable.
