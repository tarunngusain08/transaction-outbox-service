# Sequence diagrams

## Create and publish a transaction

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as TransactionController
    participant Service as TransactionService
    participant DB as PostgreSQL
    participant Poller as OutboxPublisher
    participant Kafka

    Client->>API: POST /api/v1/transactions
    API->>API: Validate JSON
    API->>Service: create(request)
    Service->>DB: INSERT transaction
    Service->>DB: INSERT PENDING outbox event
    DB-->>Service: Commit both rows
    Service-->>API: Transaction response
    API-->>Client: 201 Created

    loop Scheduled polling
        Poller->>DB: Lock ready PENDING rows
        Poller->>Kafka: Send stored event
        Kafka-->>Poller: Acknowledgement
        Poller->>DB: Mark event PUBLISHED
    end
```

The database commit happens before asynchronous Kafka delivery. This keeps the
request independent of temporary Kafka outages while preserving event intent.

## Reject invalid or duplicate input

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as TransactionController
    participant Validation
    participant DB as PostgreSQL

    Client->>API: POST transaction JSON
    API->>Validation: Validate required fields
    alt Invalid input
        Validation-->>API: Constraint errors
        API-->>Client: 400 Bad Request
    else Valid input with duplicate externalReference
        API->>DB: INSERT transaction
        DB-->>API: Unique constraint violation
        API-->>Client: 409 Conflict
    end
```

Neither failure creates a committed outbox event.

## Normalize legacy JSON

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as TransactionController
    participant Normalizer as TransactionNormalizer

    Client->>API: POST /api/v1/transactions/normalize
    API->>API: Validate legacy fields
    API->>Normalizer: normalize(legacyRequest)
    Normalizer->>Normalizer: Convert amount to minor units
    Normalizer->>Normalizer: Map DR/CR and payment channel
    Normalizer->>Normalizer: Convert Asia/Kolkata time to UTC
    Normalizer->>Normalizer: Flatten accounts and metadata
    Normalizer-->>API: Canonical create request
    API-->>Client: 200 OK
```

The client may send the returned JSON unchanged to the create endpoint.
Normalization has no database or Kafka side effects.

## Retry a Kafka failure

```mermaid
sequenceDiagram
    autonumber
    participant Poller as OutboxPublisher
    participant DB as PostgreSQL
    participant Kafka

    Poller->>DB: Lock ready PENDING event
    Poller->>Kafka: Send event
    Kafka--xPoller: Failure or timeout
    Poller->>DB: Increment retry count and set next attempt
    Note over DB: Status remains PENDING
    Poller->>DB: Fetch again after backoff
    Poller->>Kafka: Retry event
    Kafka-->>Poller: Acknowledgement
    Poller->>DB: Mark PUBLISHED
```
