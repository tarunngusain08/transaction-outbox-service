# Use-case model

This is the concise behavioral traceability for the transaction outbox service.
The [sequence-diagram catalog](sequence-diagrams.md) is the canonical home for
all diagrams. **Planned** means documented future work, not an available
endpoint, job, or operational control.

## Status vocabulary

| Status | Meaning |
| --- | --- |
| Implemented | Executable code exists in this repository |
| Implemented contract | The service side exists; another system owns the complementary behavior |
| Planned | A next-phase capability with no current implementation |

## Implemented product and operational use cases

See the [implemented scope map](sequence-diagrams.md#implemented-use-cases).

| ID | Status | Primary actor and trigger | Successful outcome | Alternate or failure outcome |
| --- | --- | --- | --- | --- |
| UC-01 | Implemented | API client sends strict `POST /api/v2/transactions` JSON | A database-native decision on `(sourceSystem, externalReference)` returns `201` for one new canonical transaction and atomically commits one `PENDING` outbox event; a matching durable-fingerprint replay returns the original record with `200` | Invalid/coerced/oversized input returns `400`; a differing or unverifiable historical fingerprint returns `409`; serialization/database failure rolls back the create |
| UC-02 | Implemented | API client sends `GET /api/v2/transactions/{transactionId}` | Returns the stored canonical transaction | Unknown UUID returns `404`; malformed UUID returns `400` |
| UC-03 | Implemented | API client sends `POST /api/v2/transactions/normalize` with the legacy schema | Returns deterministic, create-compatible JSON with no server-owned fields; that body can be submitted unchanged to UC-01 | Shape validation returns `400`; unsupported type, inexact/invalid amount, invalid date, or non-canonical output returns `422`; normalization has no database or Kafka side effect |
| UC-04 | Implemented | Scheduled poller claims a due `PENDING` or expired `PROCESSING` row | A short `SKIP LOCKED` transaction commits one claim token; Kafka I/O runs without a DB transaction; a guarded short transaction records `PUBLISHED` | Concurrent workers receive disjoint claims; abandoned leases become claimable; stale workers cannot finalize newer claims |
| UC-05 | Implemented | Kafka send throws, times out, or is interrupted | An ordinary V2 failure increments `retry_count`, stores a bounded error, releases the claim, and schedules capped exponential backoff | Interruption preserves the interrupt, consumes no retry, leaves `PROCESSING` ownership for lease recovery, and stops the batch |
| UC-06 | Implemented contract | UC-04 sends `TRANSACTION_CREATED` to `payments.transactions.created` | Kafka receives transaction ID as key and a schema-version-2 envelope with producer, event identity/time, and canonical V2 body | Delivery is at least once; consumers validate the version and deduplicate `eventId`; already-published V1 events may remain on the topic |
| UC-07 | Implemented | Probe calls `/actuator/health` or operator calls `/actuator/outbox` | Basic health remains independent; one aggregate reports pending, processing, and quarantined counts plus ages | Compose can be `UP` during broker backlog or quarantine; alert routing and thresholds are planned |
| UC-08 | Implemented | Local database operator queries PostgreSQL | Retained rows expose publishable, published, and quarantined state plus retry/error/claim/quarantine evidence | Operational history is not tamper-evident audit storage; no mutation API, quarantine release, or archival job exists |
| UC-09 | Implemented | Client calls a retired V1 create, normalize, or GET route | Returns `410 Gone` and the route-specific V2 successor without binding V1 input or touching durable state | Valid and malformed IDs create no transaction/outbox row; V1 is never silently interpreted as V2 |
| UC-10 | Implemented | Deployment operator runs V7 preflight before Flyway | Its documented V1-V6 recorded-history condition with zero historical unpublished rows permits the V6-to-V7 upgrade; V7 adds quarantine evidence/constraints/index without rewriting V1-V6 | Changed checksums, ordinary extra versioned migrations, already-applied V7, or unresolved count exit nonzero; the gate is not a complete lineage audit; containment does not complete historical delivery and requires UC-P08 |

## Implemented engineering use cases

| ID | Interface | Verified outcome |
| --- | --- | --- |
| ENG-01 | `make build` | Executable Spring Boot JAR and local container image build |
| ENG-02 | `make run`, `make stop`, `make reset` | Stack starts in dependency order; ordinary stop retains PostgreSQL/Kafka volumes; reset deletes both |
| ENG-03 | `make unit-test`, `make integration-test`, `make test`, `make lint`, `make docs-lint`, `make coverage`, `make check` | Tests, Checkstyle, Python compilation, Markdown/Mermaid/link checks, coverage threshold, and toolchain enforcement run |
| ENG-04 | `make traffic` | Expected `2xx`, `400`, `409`, `410`, and `422` responses occur; exact HTTP/DB/outbox/Kafka contracts correlate for successful V2 creates |
| ENG-05 | `make load-test` | Concurrent traffic satisfies local configurable bounds and exact pipeline checks; results are local acceptance evidence, not capacity |
| ENG-06 | `.github/workflows/ci.yml` | Static/docs, test/coverage, build, and E2E/load gates run for configured branches/PRs |
| ENG-07 | `make migration-preflight`, `make migration-v7-preflight` | Read-only worklists expose identity/currency collisions and historical unpublished events; the V7 command rejects its documented noncanonical/nonzero states |

## Planned next-phase use cases

See the [planned scope map](sequence-diagrams.md#planned-not-implemented-use-cases).

| ID | Status | Intended actor and trigger | Required outcome before the use case is complete |
| --- | --- | --- | --- |
| UC-P01 | Planned | API client presents credentials; security policy is administered externally | Reject unauthenticated/unauthorized calls and attach an auditable principal to allowed operations |
| UC-P02 | Planned | API client exceeds a configured request budget | Enforce documented per-principal/client limits and return deterministic throttling without partial writes |
| UC-P03 | Planned | An allowed request contains account identifiers | Tokenize or encrypt stored identifiers, redact logs/errors, and define controlled detokenization access |
| UC-P04 | Planned | Authorized operator selects a persistent `PENDING` retry after resolving the dependency | Make the existing event due immediately without changing the business transaction or `eventId`, and audit who expedited it and why |
| UC-P05 | Planned | Retention scheduler finds old `PUBLISHED` events past policy | Copy to approved audit storage, verify the archive, then remove/partition source rows without touching `PENDING` or `PROCESSING` events |
| UC-P06 | Planned | Request/outbox activity or an SLO breach occurs | Export correlated traces and broker/database/outbox metrics; provide dashboards and actionable alerts |
| UC-P07 | Planned | CI proposes an event-schema change | Check chosen compatibility against a registry and block incompatible producer deployment |
| UC-P08 | Planned | Authorized operator selects a `QUARANTINED` legacy event after compatibility analysis | Preserve the original, audit the decision, explicitly validate/transform to V2, and create a causally linked replacement only under an approved event-ID/deduplication policy |

## Traceability to sequence diagrams

| Use cases | Sequence-diagram section |
| --- | --- |
| UC-01 | Create and atomically stage an event; reject invalid or duplicate create |
| UC-02 | Retrieve a transaction |
| UC-03 | Normalize a legacy transaction |
| UC-04, UC-05, UC-06 | Claim, deliver, retry, or recover an outbox event; claim disjoint concurrent work |
| UC-07, UC-08 | Report application/delivery/quarantine status; inspect retained operational history |
| UC-09 | Reject retired API V1 explicitly |
| UC-10, ENG-07 | Gate and quarantine pre-V2 unpublished events |
| ENG-01, ENG-02 | Build and run the local stack |
| ENG-03, ENG-06 | Execute local and CI quality gates |
| ENG-04, ENG-05 | Verify smoke and concurrent load traffic |
| UC-P01, UC-P02, UC-P03 | Planned secured request path |
| UC-P04 | Planned operator expedite of a persistent retry |
| UC-P05 | Planned archival of old `PUBLISHED` events |
| UC-P06 | Planned telemetry and SLO alerting |
| UC-P07 | Planned schema-compatibility gate |
| UC-P08 | Planned quarantined-payload compatibility review |
