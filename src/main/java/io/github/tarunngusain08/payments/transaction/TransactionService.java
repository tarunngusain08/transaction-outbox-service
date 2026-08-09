package io.github.tarunngusain08.payments.transaction;

import io.github.tarunngusain08.payments.outbox.OutboxEvent;
import io.github.tarunngusain08.payments.outbox.OutboxEventRepository;
import io.github.tarunngusain08.payments.transaction.api.CreateTransactionRequest;
import io.github.tarunngusain08.payments.transaction.api.TransactionResponse;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Service
public class TransactionService {

    private static final String AGGREGATE_TYPE = "TRANSACTION";
    private static final Instant EARLIEST_CREATED_AT = Instant.parse("1970-01-01T00:00:00Z");
    private static final Duration MAX_FUTURE_CLOCK_SKEW = Duration.ofMinutes(5);

    private final PaymentTransactionRepository transactionRepository;
    private final PaymentTransactionInserter transactionInserter;
    private final OutboxEventRepository outboxRepository;
    private final TransactionEventSerializer eventSerializer;
    private final Clock clock;

    public TransactionService(
            PaymentTransactionRepository transactionRepository,
            PaymentTransactionInserter transactionInserter,
            OutboxEventRepository outboxRepository,
            TransactionEventSerializer eventSerializer,
            Clock clock
    ) {
        this.transactionRepository = transactionRepository;
        this.transactionInserter = transactionInserter;
        this.outboxRepository = outboxRepository;
        this.eventSerializer = eventSerializer;
        this.clock = clock;
    }

    @Transactional
    public TransactionCreationResult create(CreateTransactionRequest request) {
        String externalReference = request.externalReference().trim();
        var existing = transactionRepository.findByExternalReference(externalReference);
        if (existing.isPresent()) {
            if (matches(existing.get(), request)) {
                return TransactionCreationResult.replayed(TransactionResponse.from(existing.get()));
            }
            throw new DuplicateTransactionException(externalReference);
        }

        Instant now = Instant.now(clock);
        Instant createdAt = request.createdAt() == null ? now : request.createdAt();
        validateCreatedAt(createdAt, now);
        var transaction = new PaymentTransaction(
                request.transactionId() == null ? UUID.randomUUID() : request.transactionId(),
                externalReference,
                request.amount(),
                request.currency(),
                request.type(),
                request.status() == null ? TransactionStatus.PENDING : request.status(),
                request.sourceAccount().trim(),
                request.destinationAccount().trim(),
                request.channel(),
                createdAt,
                request.metadata() == null ? Map.of() : request.metadata()
        );

        transactionInserter.insert(transaction);
        var response = TransactionResponse.from(transaction);
        var eventId = UUID.randomUUID();
        var event = new TransactionCreatedEvent(
                TransactionCreatedEvent.SCHEMA_VERSION,
                TransactionCreatedEvent.PRODUCER,
                eventId,
                TransactionCreatedEvent.EVENT_TYPE,
                now,
                response
        );

        outboxRepository.save(OutboxEvent.pending(
                eventId,
                AGGREGATE_TYPE,
                transaction.getId(),
                TransactionCreatedEvent.EVENT_TYPE,
                eventSerializer.serialize(event),
                now
        ));

        return TransactionCreationResult.created(response);
    }

    @Transactional(readOnly = true)
    public TransactionResponse findById(UUID transactionId) {
        return transactionRepository.findById(transactionId)
                .map(TransactionResponse::from)
                .orElseThrow(() -> new TransactionNotFoundException(transactionId));
    }

    private void validateCreatedAt(Instant createdAt, Instant now) {
        if (createdAt.isBefore(EARLIEST_CREATED_AT)
                || createdAt.isAfter(now.plus(MAX_FUTURE_CLOCK_SKEW))) {
            throw new InvalidTransactionException(
                    "createdAt must be between 1970-01-01T00:00:00Z and five minutes in the future"
            );
        }
    }

    private boolean matches(PaymentTransaction existing, CreateTransactionRequest request) {
        var expectedStatus = request.status() == null ? TransactionStatus.PENDING : request.status();
        var expectedMetadata = request.metadata() == null ? Map.<String, Object>of() : request.metadata();

        return (request.transactionId() == null || request.transactionId().equals(existing.getId()))
                && request.amount() == existing.getAmountMinor()
                && request.currency().equals(existing.getCurrency())
                && request.type() == existing.getType()
                && expectedStatus == existing.getStatus()
                && request.sourceAccount().trim().equals(existing.getSourceAccount())
                && request.destinationAccount().trim().equals(existing.getDestinationAccount())
                && request.channel() == existing.getChannel()
                && (request.createdAt() == null || request.createdAt().equals(existing.getCreatedAt()))
                && Objects.equals(expectedMetadata, existing.getMetadata());
    }
}
