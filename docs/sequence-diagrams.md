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
    Note over Service,DB: Spring opens the transaction before create method logic executes
    Service->>Service: Trim reference and apply source defaults
    Service->>TxRepo: findByExternalReference(canonical reference)
    TxRepo->>DB: SELECT transaction
    DB-->>TxRepo: empty
    Service->>Service: Apply UUID, PENDING status, and UTC-time defaults
    Service->>TxRepo: persist(transaction) and flush
    TxRepo->>DB: Execute insert-only INSERT transactions
    Service->>Service: Serialize version-1 TRANSACTION_CREATED with a new eventId
    Service->>OutboxRepo: save(PENDING event)
    OutboxRepo-->>Service: Managed event (SQL may be deferred)
    Service->>DB: Flush pending outbox INSERT and commit
    DB-->>Service: Both durable rows committed atomically
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
        Note over Service,DB: Transaction starts before service method logic
        Service->>DB: Read by trimmed external_reference
        alt Reference already exists
            DB-->>Service: Stored transaction
            alt Canonical request matches stored fields
                Service-->>Controller: Original transaction, replayed=true
                Controller-->>Client: 200 OK + original Location
            else Same reference has different fields
                Service->>Handler: DuplicateTransactionException
                Handler-->>Client: 409 ProblemDetail
            end
        else New reference
            Service->>DB: Insert-only transaction flush and stage outbox
            alt Known primary/external unique constraint loses a race
                DB-->>Service: DataIntegrityViolationException
                Note over Service,DB: Spring rolls back the whole database transaction
                Service->>Handler: Known unique constraint
                Handler-->>Client: 409 ProblemDetail
            else Unrelated integrity constraint fails
                DB-->>Service: DataIntegrityViolationException
                Note over Service,DB: Spring rolls back the whole database transaction
                Service->>Handler: Unexpected integrity failure
                Handler-->>Client: Sanitized 500 ProblemDetail
            end
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
    alt Required field/IFSC shape or currency is invalid/unsupported
        MVC->>Handler: MethodArgumentNotValidException
        Handler-->>Client: 400 ProblemDetail + field errors
    else Request shape is valid
        MVC->>Controller: LegacyTransactionRequest
        Controller->>Normalizer: normalize(request)
        Normalizer->>Normalizer: Parse exact currency minor units
        Normalizer->>Normalizer: Map DR/CR and payment channel
        Normalizer->>Normalizer: Convert Asia/Kolkata source time to Instant
        Normalizer->>Normalizer: Flatten accounts and preserve source metadata
        alt Amount, type, or date is semantically invalid
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
    participant Claim as OutboxEventClaimService
    participant Delivery as OutboxEventDelivery
    participant Finalizer as OutboxEventFinalizer
    participant DB as PostgreSQL
    participant Kafka as Kafka broker
    participant Consumer as External consumer

    Scheduler->>Publisher: publishPendingBatch() on fixed delay
    Publisher->>Claim: claimBatch()
    Note over Claim,DB: Short REQUIRES_NEW claim transaction
    Claim->>DB: SELECT due PENDING or expired PROCESSING rows FOR UPDATE SKIP LOCKED
    DB-->>Claim: Disjoint bounded event IDs
    Claim->>DB: Set PROCESSING, claim_token, claimed_at
    Claim->>DB: Commit and release every row lock
    Claim-->>Publisher: Immutable claimed event snapshots
    loop Each claimed event
        Publisher->>Delivery: deliver(claimed event)
        Note over Delivery,Kafka: No database transaction or row lock is open
        Delivery->>Kafka: Send key=transactionId and stored versioned JSON, then await acknowledgement
        Kafka-->>Consumer: Record may be observable before DB finalize commit
        Consumer->>Consumer: Validate schemaVersion and deduplicate by eventId
        alt Broker acknowledges before timeout
            Kafka-->>Delivery: Send result
            Delivery->>Finalizer: markPublished(eventId, claimToken)
            Note over Finalizer,DB: Short REQUIRES_NEW finalize transaction
            Finalizer->>DB: Lock row and require current PROCESSING claim token
            alt Claim token is still current
                Finalizer->>DB: Set PUBLISHED and published_at, clear claim/error, then commit
                Finalizer-->>Delivery: PUBLISHED
            else Lease was reclaimed or row changed
                Finalizer->>DB: Commit without mutation
                Finalizer-->>Delivery: SKIPPED
            end
        else Send fails, times out, or is interrupted
            Delivery->>Finalizer: recordFailure(eventId, claimToken, bounded error)
            Finalizer->>DB: Lock row and require current PROCESSING claim token
            alt Current claim and retry_count below maxRetries
                Finalizer->>DB: Set PENDING and next_attempt_at, clear claim, then commit
                Finalizer-->>Delivery: RETRY_SCHEDULED
            else Current claim reaches maxRetries
                Finalizer->>DB: Set FAILED, clear claim, then commit
                Finalizer-->>Delivery: PERMANENTLY_FAILED
            else Claim token is stale
                Finalizer->>DB: Commit without mutation
                Finalizer-->>Delivery: SKIPPED
            end
            opt Thread was interrupted
                Delivery-->>Publisher: INTERRUPTED and preserve interrupt flag
                Publisher->>Publisher: Stop batch before any later send
            end
        end
    end
    Note over Delivery,Kafka: Acknowledgement plus finalize failure or lease expiry can produce a duplicate publish
```

### UC-04 — Claim disjoint work across concurrent pollers

```mermaid
sequenceDiagram
    autonumber
    participant WorkerA as Service instance A
    participant WorkerB as Service instance B
    participant DB as PostgreSQL
    participant Kafka as Kafka broker

    par Claim transaction A
        WorkerA->>DB: SELECT bounded due rows FOR UPDATE SKIP LOCKED
        DB-->>WorkerA: Batch A
        WorkerA->>DB: Mark Batch A PROCESSING with claim tokens, then commit
    and Claim transaction B
        WorkerB->>DB: SELECT bounded due rows FOR UPDATE SKIP LOCKED
        DB-->>WorkerB: Disjoint Batch B (locked rows skipped)
        WorkerB->>DB: Mark Batch B PROCESSING with claim tokens, then commit
    end
    Note over WorkerA,WorkerB: Database locks are released before broker I/O
    par Publish Batch A outside a DB transaction
        WorkerA->>Kafka: Publish claimed payloads
        Kafka-->>WorkerA: Acknowledgements
        WorkerA->>DB: Guarded short finalize transactions
    and Publish Batch B outside a DB transaction
        WorkerB->>Kafka: Publish claimed payloads
        Kafka-->>WorkerB: Acknowledgements
        WorkerB->>DB: Guarded short finalize transactions
    end
    Note over WorkerA,DB: An abandoned PROCESSING lease becomes claimable after claimLease
```

### UC-07 — Report application and delivery status

```mermaid
sequenceDiagram
    autonumber
    actor Probe as Operator / Compose health check
    participant Actuator as Spring Boot Actuator
    participant Indicators as Registered health contributors
    participant Outbox as OutboxDeliveryEndpoint
    participant DB as PostgreSQL

    alt Basic application / Compose readiness check
        Probe->>Actuator: GET /actuator/health
        Actuator->>Indicators: Evaluate registered application contributors
        Indicators-->>Actuator: Component statuses
        Actuator-->>Probe: Aggregate health response
        Note over Probe,Actuator: Kafka backlog does not intentionally make the ingest API unavailable
    else Separate asynchronous-delivery inspection
        Probe->>Actuator: GET /actuator/outbox
        Actuator->>Outbox: snapshot()
        Outbox->>DB: Count PENDING / PROCESSING / FAILED and find oldest unpublished
        DB-->>Outbox: Delivery state
        Outbox-->>Probe: Counts, age in seconds, and checkedAt
    end
```

### UC-08 — Inspect retained outbox operational history

```mermaid
sequenceDiagram
    autonumber
    actor Operator as Authorized database operator
    participant PSQL as psql / approved SQL client
    participant DB as PostgreSQL

    Operator->>PSQL: Query outbox_events
    PSQL->>DB: SELECT id, aggregate_id, status, retry_count, last_error, published_at
    DB-->>PSQL: Retained PENDING / PROCESSING / PUBLISHED / FAILED rows
    PSQL-->>Operator: Mutable operational and diagnostic view
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
    Note over Docker,Kafka: Containers stop while named PostgreSQL and Kafka volumes are retained
```

### ENG-03, ENG-06 — Execute local and CI quality gates

```mermaid
sequenceDiagram
    autonumber
    actor Initiator as Developer / GitHub Actions
    participant Make as Makefile
    participant Maven as Maven Wrapper
    participant Python as Python compiler
    participant Node as Pinned documentation tools
    participant Chrome as Headless Chrome
    participant TC as Testcontainers
    participant Docker as Docker
    participant Reports as Test and coverage reports

    Initiator->>Make: Select one target, make check, or a CI job
    loop Each gate selected by that workflow
        alt Lint gate
            Make->>Maven: Checkstyle Java sources
            Make->>Python: Compile-check repository Python scripts
            Make->>Node: npm ci from package-lock and Markdown lint
            Node->>Python: Run local link / use-case traceability checks
            Python->>Chrome: Render every Mermaid block through pinned CLI
            Chrome-->>Python: Parsed SVG artefacts in a temporary directory
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
            Maven->>Maven: Enforce 70% line and 65% branch thresholds when coverage is enabled
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
    Simulator->>Kafka: Consume keys and values from beginning, then filter unique prefix
    Kafka-->>Simulator: Matching versioned TRANSACTION_CREATED occurrences
    Simulator->>Simulator: Validate key, envelope, event ID, and exact canonical body
    Simulator->>Simulator: Permit same-event repeats and reject contradictory creation IDs
    Simulator->>Simulator: Enforce expected statuses and configurable load bounds
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
