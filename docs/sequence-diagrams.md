# Sequence-diagram catalog

These diagrams describe observable ordering and state transitions. Sections
labelled **implemented** correspond to executable repository behavior. Sections
labelled **planned** are next-phase designs and are not available today.

The use-case IDs and actor definitions are maintained in
[the use-case model](use-cases.md).

## Implemented runtime sequences

### UC-01 — Create and atomically stage an event

```mermaid
sequenceDiagram
    autonumber
    actor Client as API client
    participant MVC as Spring MVC validation
    participant Controller as TransactionController
    participant Service as TransactionService
    participant TxRepo as Transaction repository
    participant OutboxRepo as Outbox repository
    participant DB as PostgreSQL

    Client->>MVC: POST /api/v1/transactions
    MVC->>Controller: Valid CreateTransactionRequest
    Controller->>Service: create(request)
    Service->>TxRepo: existsByExternalReference(reference)
    TxRepo->>DB: SELECT existence
    DB-->>TxRepo: false
    Service->>Service: Apply UUID, PENDING status, and UTC-time defaults
    Note over Service,DB: One Spring database transaction starts
    Service->>TxRepo: save(transaction)
    TxRepo->>DB: INSERT transactions
    Service->>Service: Serialize TRANSACTION_CREATED with a new eventId
    Service->>OutboxRepo: save(PENDING event)
    OutboxRepo->>DB: INSERT outbox_events
    DB-->>Service: Commit both inserts atomically
    Service-->>Controller: Canonical TransactionResponse
    Controller-->>Client: 201 Created + Location header
    Note over Client,DB: No Kafka call occurs on the request thread
```

### UC-01 — Reject an invalid or duplicate create

```mermaid
sequenceDiagram
    autonumber
    actor Client as API client
    participant MVC as Spring MVC validation
    participant Controller as TransactionController
    participant Service as TransactionService
    participant DB as PostgreSQL
    participant Handler as ApiExceptionHandler

    Client->>MVC: POST /api/v1/transactions
    alt Bean validation fails
        MVC->>Handler: MethodArgumentNotValidException
        Handler-->>Client: 400 ProblemDetail + field errors
        Note over MVC,DB: Controller is not called and no rows are written
    else Request shape is valid
        MVC->>Controller: CreateTransactionRequest
        Controller->>Service: create(request)
        Service->>DB: Check external_reference
        alt Reference already exists
            DB-->>Service: true
            Service->>Handler: DuplicateTransactionException
            Handler-->>Client: 409 ProblemDetail
        else Concurrent request wins unique constraint
            Service->>DB: Attempt transaction + outbox inserts
            DB-->>Service: DataIntegrityViolationException
            Note over Service,DB: Spring rolls back the whole database transaction
            Service->>Handler: DataIntegrityViolationException
            Handler-->>Client: 409 ProblemDetail
        end
    end
```

### UC-02 — Retrieve a transaction

```mermaid
sequenceDiagram
    autonumber
    actor Client as API client
    participant MVC as Spring MVC UUID binding
    participant Controller as TransactionController
    participant Service as TransactionService
    participant Repo as Transaction repository
    participant DB as PostgreSQL
    participant Handler as ApiExceptionHandler

    Client->>MVC: GET /api/v1/transactions/{transactionId}
    alt Path value is not a UUID
        MVC-->>Client: 400 Bad Request
    else UUID is valid
        MVC->>Controller: findById(UUID)
        Controller->>Service: findById(UUID)
        Service->>Repo: findById(UUID)
        Repo->>DB: SELECT transaction
        alt Record exists
            DB-->>Repo: PaymentTransaction
            Repo-->>Service: PaymentTransaction
            Service-->>Controller: Canonical TransactionResponse
            Controller-->>Client: 200 OK
        else Record does not exist
            DB-->>Repo: empty
            Repo-->>Service: empty
            Service->>Handler: TransactionNotFoundException
            Handler-->>Client: 404 ProblemDetail
        end
    end
```

### UC-03 — Normalize a legacy transaction

```mermaid
sequenceDiagram
    autonumber
    actor Client as API client
    participant MVC as Spring MVC validation
    participant Controller as TransactionController
    participant Normalizer as TransactionNormalizer
    participant Handler as ApiExceptionHandler

    Client->>MVC: POST /api/v1/transactions/normalize
    alt Required legacy field/IFSC shape is invalid
        MVC->>Handler: MethodArgumentNotValidException
        Handler-->>Client: 400 ProblemDetail + field errors
    else Request shape is valid
        MVC->>Controller: LegacyTransactionRequest
        Controller->>Normalizer: normalize(request)
        Normalizer->>Normalizer: Parse exact currency minor units
        Normalizer->>Normalizer: Map DR/CR and payment channel
        Normalizer->>Normalizer: Convert Asia/Kolkata source time to Instant
        Normalizer->>Normalizer: Flatten accounts and preserve source metadata
        alt Currency, amount, type, or date is semantically invalid
            Normalizer->>Handler: NormalizationException
            Handler-->>Client: 422 ProblemDetail
        else Normalization succeeds
            Normalizer-->>Controller: Canonical TransactionResponse
            Controller-->>Client: 200 OK
            Note over Client,Normalizer: Unknown channel maps to OTHER and original mode is metadata
            Note over Client,Normalizer: No PostgreSQL write and no Kafka publish occur
        end
    end
```

### UC-04, UC-05, UC-06 — Deliver, retry, or terminally fail an outbox event

```mermaid
sequenceDiagram
    autonumber
    participant Scheduler as OutboxPoller
    participant Publisher as OutboxPublisher
    participant Delivery as OutboxEventDelivery
    participant DB as PostgreSQL
    participant Kafka as Kafka broker
    participant Consumer as External consumer

    Scheduler->>Publisher: publishPendingBatch() on fixed delay
    Publisher->>DB: SELECT due PENDING IDs ordered by created_at LIMIT batchSize
    DB-->>Publisher: Candidate event IDs
    loop Each candidate ID
        Publisher->>Delivery: deliver(eventId)
        Note over Delivery,DB: Start independent REQUIRES_NEW transaction
        Delivery->>DB: SELECT event FOR UPDATE
        alt Missing, not PENDING, or not yet due
            Delivery->>DB: Commit without state change
            Delivery-->>Publisher: SKIPPED
        else Eligible PENDING event
            Delivery->>Kafka: Send key=transactionId and value=stored JSON, then await acknowledgement
            alt Broker acknowledges before timeout
                Kafka-->>Delivery: Send result
                Delivery->>DB: Set PUBLISHED and published_at, then clear last_error
                Delivery->>DB: Commit
                Kafka-->>Consumer: TRANSACTION_CREATED (at least once)
                Consumer->>Consumer: Deduplicate by eventId
                Delivery-->>Publisher: PUBLISHED
            else Send fails, times out, or is interrupted
                Delivery->>DB: Increment retry_count and store bounded root error
                alt retry_count is below maxRetries
                    Delivery->>DB: Keep PENDING and schedule next_attempt_at with exponential backoff
                    Delivery->>DB: Commit
                    Delivery-->>Publisher: RETRY_SCHEDULED
                else retry_count reaches maxRetries
                    Delivery->>DB: Set FAILED and commit
                    Delivery-->>Publisher: PERMANENTLY_FAILED
                end
            end
        end
    end
    Note over Delivery,Kafka: Broker acknowledgement followed by DB commit failure can cause a later duplicate publish
```

### UC-04 — Serialize concurrent delivery of the same event

```mermaid
sequenceDiagram
    autonumber
    participant WorkerA as Service instance A
    participant WorkerB as Service instance B
    participant DB as PostgreSQL
    participant Kafka as Kafka broker

    WorkerA->>DB: Discover the same due event ID
    WorkerB->>DB: Discover the same due event ID
    WorkerA->>DB: SELECT event FOR UPDATE
    DB-->>WorkerA: Lock granted with status PENDING
    WorkerB->>DB: SELECT event FOR UPDATE
    Note over WorkerB,DB: Instance B waits for the row lock
    WorkerA->>Kafka: Publish stored payload
    Kafka-->>WorkerA: Acknowledged
    WorkerA->>DB: Set PUBLISHED and commit, releasing the lock
    DB-->>WorkerB: Lock granted with current status PUBLISHED
    WorkerB->>WorkerB: Re-check status and skip
    WorkerB->>DB: Commit without publishing
```

### UC-07 — Report health and readiness

```mermaid
sequenceDiagram
    autonumber
    actor Probe as Operator / Compose health check
    participant Actuator as Spring Boot Actuator
    participant Indicators as Registered health contributors

    Probe->>Actuator: GET /actuator/health
    Actuator->>Indicators: Evaluate registered health state
    Indicators-->>Actuator: Component statuses
    alt Response is healthy
        Actuator-->>Probe: 200 aggregate UP response
        Probe->>Probe: Operator sees healthy and Compose health command exits 0
    else Response is unhealthy or times out
        Actuator-->>Probe: Non-healthy response or timeout
        Probe->>Probe: Operator investigates while Compose retries or fails --wait
    end
```

### UC-08 — Inspect retained outbox audit state

```mermaid
sequenceDiagram
    autonumber
    actor Operator as Authorized database operator
    participant PSQL as psql / approved SQL client
    participant DB as PostgreSQL

    Operator->>PSQL: Query outbox_events
    PSQL->>DB: SELECT id, aggregate_id, status, retry_count, last_error, published_at
    DB-->>PSQL: Retained PENDING / PUBLISHED / FAILED rows
    PSQL-->>Operator: Audit and diagnostic view
    Note over Operator,DB: Current scope is read-only SQL with no operator HTTP API or replay command
```

## Implemented engineering sequences

### ENG-01, ENG-02 — Build and run the local stack

```mermaid
sequenceDiagram
    autonumber
    actor Developer
    participant Make as Makefile
    participant Maven as Maven Wrapper
    participant Docker as Docker / Compose
    participant Postgres as PostgreSQL
    participant Kafka as Kafka
    participant Init as kafka-init
    participant App as Spring Boot app

    Developer->>Make: make build
    Make->>Maven: clean package with unit execution skipped
    Maven-->>Make: Executable JAR
    Make->>Docker: Build transaction-outbox-service:local
    Docker-->>Developer: Local image available
    Developer->>Make: make run
    Make->>Docker: compose up --detach --build --wait
    Docker->>Postgres: Start and wait for pg_isready
    Docker->>Kafka: Start KRaft broker and wait for topic command readiness
    Docker->>Init: Create payments.transactions.created if absent
    Init-->>Docker: Exit 0
    Docker->>App: Start after PostgreSQL healthy and topic initialization complete
    Docker->>App: Poll /actuator/health
    App-->>Docker: UP
    Docker-->>Developer: Complete stack ready on localhost:8080
    Developer->>Make: make stop
    Make->>Docker: compose down
    Note over Docker,Postgres: Containers and network stop while the named PostgreSQL volume is retained
```

### ENG-03, ENG-06 — Execute local and CI quality gates

```mermaid
sequenceDiagram
    autonumber
    actor Initiator as Developer / GitHub Actions
    participant Make as Makefile
    participant Maven as Maven Wrapper
    participant Python as Python compiler
    participant TC as Testcontainers
    participant Docker as Docker
    participant Reports as Test and coverage reports

    Initiator->>Make: Select one target, make check, or a CI job
    loop Each gate selected by that workflow
        alt Lint gate
            Make->>Maven: Checkstyle Java sources
            Make->>Python: Compile-check traffic_simulator.py
        else Unit-only gate
            Make->>Maven: Run Surefire while excluding *IT
            Maven->>Reports: Write unit reports
        else Integration-only or full test/coverage gate
            Make->>Maven: Run the selected unit phase when included
            Maven->>TC: Start ephemeral PostgreSQL and Kafka
            TC->>Docker: Create isolated containers
            Docker-->>TC: Healthy dependencies
            TC-->>Maven: Integration scenarios complete
            Maven->>Reports: Write Surefire/Failsafe reports and JaCoCo report when enabled
            Maven->>Maven: Enforce 70% line threshold when coverage is enabled
        else Build gate
            Make->>Maven: Package executable JAR with unit execution skipped
            Make->>Docker: Build application image
        end
    end
    Make-->>Initiator: Selected gate(s) pass or fail independently
```

### ENG-04, ENG-05 — Verify mixed smoke and concurrent load traffic

```mermaid
sequenceDiagram
    autonumber
    actor Initiator as Developer / CI
    participant Make as Makefile
    participant Compose as Docker Compose
    participant Simulator as traffic_simulator.py
    participant API as Transaction API
    participant DB as PostgreSQL
    participant Poller as Outbox poller
    participant Kafka as Kafka

    Initiator->>Make: make traffic OR make load-test
    Make->>Compose: Start/build stack and wait for health
    Compose-->>Make: Stack ready
    Make->>Simulator: Start unique run prefix
    alt Smoke mode
        Simulator->>API: Sequential valid creates, duplicate, invalid create, normalize success/failure, GET
    else Load mode
        Simulator->>API: Concurrent unique valid creates and intentional invalid creates
        Simulator->>API: Duplicate probe and normalization probe
    end
    API-->>Simulator: Expected 2xx and intentional 4xx responses
    Poller->>Kafka: Publish successful create events asynchronously
    Simulator->>DB: Poll exact prefix counts and PUBLISHED outbox counts
    DB-->>Simulator: Expected records and events settled
    Simulator->>Kafka: Consume topic from beginning and filter unique prefix
    Kafka-->>Simulator: Matching TRANSACTION_CREATED events
    Simulator->>Simulator: Assert statuses and end-to-end counts, then calculate throughput and percentiles
    Simulator-->>Initiator: Exit 0 on complete match and non-zero on any mismatch
```

## Planned next-phase sequences

### UC-P01, UC-P02, UC-P03 — Planned secured request path

```mermaid
sequenceDiagram
    autonumber
    actor Client as API client
    participant Security as Authentication / authorization layer
    participant IdP as Identity provider
    participant Limiter as Rate limiter
    participant Privacy as Tokenization / encryption boundary
    participant API as Transaction API
    participant DB as PostgreSQL
    participant Telemetry as Telemetry collector

    Client->>Security: Request + credentials
    Security->>IdP: Validate identity and permissions
    alt Identity or permission denied
        IdP-->>Security: Denied
        Security-->>Client: 401 or 403 with no business call
    else Authorized principal
        IdP-->>Security: Principal + permitted scopes
        Security->>Limiter: Consume principal/client request budget
        alt Budget exhausted
            Limiter-->>Client: 429 with no database write
        else Budget available
            Limiter->>Privacy: Protect account identifiers and produce redacted log context
            Privacy->>API: Allowed request with protected values
            API->>DB: Execute existing atomic transaction/outbox write
            DB-->>API: Commit
            API-->>Client: Existing success response under finalized disclosure policy
            API->>Telemetry: Trace with principal correlation and no raw account data
        end
    end
    Note over Security,Telemetry: Planned design where credential format, policy engine, and key/token service remain unselected
```

### UC-P04 — Planned operator replay of a FAILED event

```mermaid
sequenceDiagram
    autonumber
    actor Operator as Authorized operator
    participant Ops as Planned operator API / CLI
    participant Auth as Authorization policy
    participant Replay as Planned replay service
    participant DB as PostgreSQL
    participant Poller as Existing outbox poller
    participant Kafka as Kafka

    Operator->>Ops: Replay FAILED event + reason
    Ops->>Auth: Verify replay permission
    alt Permission denied
        Auth-->>Operator: Reject with no state change
    else Permission granted
        Ops->>Replay: replay(eventId, principal, reason)
        Replay->>DB: Lock event and require status=FAILED
        alt Event is not FAILED or already replayed concurrently
            DB-->>Replay: Not eligible
            Replay-->>Operator: Conflict with no state change
        else FAILED event is eligible
            Replay->>DB: Audit replay decision and reset same eventId to due PENDING
            DB-->>Replay: Commit
            Replay-->>Operator: Replay accepted
            Poller->>DB: Claim due PENDING event
            Poller->>Kafka: Publish using existing at-least-once flow
        end
    end
    Note over Replay,Kafka: Keeping eventId preserves the existing consumer idempotency contract
```

### UC-P05 — Planned archival of old PUBLISHED events

```mermaid
sequenceDiagram
    autonumber
    participant Scheduler as Retention scheduler
    participant ArchiveJob as Planned archive job
    participant DB as PostgreSQL
    participant Archive as Approved audit storage

    Scheduler->>ArchiveJob: Run with retention cutoff and bounded batch size
    ArchiveJob->>DB: Select PUBLISHED rows older than cutoff
    DB-->>ArchiveJob: Batch containing no PENDING or FAILED rows
    ArchiveJob->>Archive: Write immutable records + integrity metadata
    alt Archive write or verification fails
        Archive-->>ArchiveJob: Failure / mismatch
        ArchiveJob-->>Scheduler: Fail batch and delete nothing
    else Archive acknowledges verified copy
        Archive-->>ArchiveJob: Verified receipt
        ArchiveJob->>DB: Delete or detach exactly the archived IDs in one transaction
        DB-->>ArchiveJob: Commit
        ArchiveJob-->>Scheduler: Archived count + audit receipt
    end
```

### UC-P06 — Planned telemetry and SLO alerting

```mermaid
sequenceDiagram
    autonumber
    participant API as Request path
    participant Poller as Outbox delivery path
    participant DB as PostgreSQL
    participant Kafka as Kafka
    participant Collector as Telemetry collector
    participant Backend as Metrics / tracing backend
    actor Operator as Platform / SRE

    API->>DB: Business transaction
    DB-->>API: Commit or database error
    API->>Collector: Correlated request/database timing and error metrics without sensitive values
    Poller->>Collector: Backlog age, batch, retry, and FAILED metrics
    Poller->>Kafka: Publish event
    Kafka-->>Poller: Broker acknowledgement or failure
    Poller->>Collector: Publish outcome and latency metric
    Collector->>Backend: Export traces and metrics
    Backend->>Backend: Evaluate documented SLOs and alert thresholds
    alt Threshold is breached
        Backend-->>Operator: Actionable alert with trace/event correlation
    else Within objective
        Backend-->>Operator: Dashboard remains healthy
    end
```

### UC-P07 — Planned schema-compatibility gate

```mermaid
sequenceDiagram
    autonumber
    actor Developer
    participant CI as GitHub Actions
    participant Schema as Event schema artifact
    participant Registry as Schema registry
    participant Deploy as Deployment gate

    Developer->>CI: Propose transaction-event schema change
    CI->>Schema: Validate schema syntax and generated contract tests
    CI->>Registry: Check configured compatibility against current subject/version
    alt Change is incompatible
        Registry-->>CI: Compatibility failure + reason
        CI-->>Developer: Fail check and do not deploy producer
    else Change is compatible
        Registry-->>CI: Compatible
        CI->>Deploy: Allow versioned producer deployment
        Deploy-->>Developer: Gate passed
    end
```

### UC-P08 — Planned scalable outbox claiming

```mermaid
sequenceDiagram
    autonumber
    participant WorkerA as Delivery worker A
    participant WorkerB as Delivery worker B
    participant DB as PostgreSQL
    participant Kafka as Kafka

    WorkerA->>DB: Atomically claim bounded due batch using SKIP LOCKED or claim token
    DB-->>WorkerA: Batch A
    WorkerB->>DB: Atomically claim bounded due batch using same protocol
    DB-->>WorkerB: Disjoint Batch B
    Note over WorkerA,WorkerB: No event is actively owned by both workers
    WorkerA->>Kafka: Publish Batch A with existing acknowledgement/retry semantics
    WorkerB->>Kafka: Publish Batch B with existing acknowledgement/retry semantics
    WorkerA->>DB: Commit PUBLISHED/retry states for Batch A
    WorkerB->>DB: Commit PUBLISHED/retry states for Batch B
    Note over DB,Kafka: At-least-once delivery and eventId deduplication remain unchanged
```
