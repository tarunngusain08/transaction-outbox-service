package io.github.tarunngusain08.payments.transaction;

import io.github.tarunngusain08.payments.outbox.OutboxEvent;
import io.github.tarunngusain08.payments.outbox.OutboxEventRepository;
import io.github.tarunngusain08.payments.transaction.api.CreateTransactionRequest;
import io.github.tarunngusain08.payments.transaction.api.TransactionResponse;
import io.github.tarunngusain08.payments.validation.MetadataCanonicalizer;
import jakarta.validation.Validator;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Service
public class TransactionService {

    private static final String AGGREGATE_TYPE = "TRANSACTION";
    private final PaymentTransactionRepository transactionRepository;
    private final PaymentTransactionInserter transactionInserter;
    private final OutboxEventRepository outboxRepository;
    private final TransactionEventSerializer eventSerializer;
    private final MetadataCanonicalizer metadataCanonicalizer;
    private final Validator validator;
    private final Clock clock;

    public TransactionService(
            PaymentTransactionRepository transactionRepository,
            PaymentTransactionInserter transactionInserter,
            OutboxEventRepository outboxRepository,
            TransactionEventSerializer eventSerializer,
            MetadataCanonicalizer metadataCanonicalizer,
            Validator validator,
            Clock clock
    ) {
        this.transactionRepository = transactionRepository;
        this.transactionInserter = transactionInserter;
        this.outboxRepository = outboxRepository;
        this.eventSerializer = eventSerializer;
        this.metadataCanonicalizer = metadataCanonicalizer;
        this.validator = validator;
        this.clock = clock;
    }

    @Transactional
    public TransactionCreationResult create(CreateTransactionRequest request) {
        validateRequest(request);
        Instant receivedAt = TransactionContract.canonicalTimestamp(Instant.now(clock));
        Instant requestedCreatedAt = request.createdAt() == null
                ? null
                : TransactionContract.canonicalTimestamp(request.createdAt());
        Instant createdAt = requestedCreatedAt == null ? receivedAt : requestedCreatedAt;
        TransactionContract.validateCreatedAt(createdAt, receivedAt);
        Map<String, Object> metadata = metadataCanonicalizer.canonicalize(request.metadata());

        var existing = transactionRepository.findBySourceSystemAndExternalReference(
                request.sourceSystem(),
                request.externalReference()
        );
        if (existing.isPresent()) {
            if (matches(existing.get(), request, requestedCreatedAt, metadata)) {
                return TransactionCreationResult.replayed(TransactionResponse.from(existing.get()));
            }
            throw new DuplicateTransactionException(request.externalReference());
        }

        var transaction = new PaymentTransaction(
                UUID.randomUUID(),
                request.sourceSystem(),
                request.externalReference(),
                request.amount(),
                request.currency(),
                request.type(),
                TransactionStatus.PENDING,
                request.sourceAccount(),
                request.destinationAccount(),
                request.channel(),
                createdAt,
                receivedAt,
                metadata,
                null,
                null
        );

        transactionInserter.insert(transaction);
        var response = TransactionResponse.from(transaction);
        var eventId = UUID.randomUUID();
        var event = new TransactionCreatedEvent(
                TransactionCreatedEvent.SCHEMA_VERSION,
                TransactionCreatedEvent.PRODUCER,
                eventId,
                TransactionCreatedEvent.EVENT_TYPE,
                receivedAt,
                response
        );

        outboxRepository.save(OutboxEvent.pending(
                eventId,
                AGGREGATE_TYPE,
                transaction.getId(),
                TransactionCreatedEvent.EVENT_TYPE,
                eventSerializer.serialize(event),
                receivedAt
        ));

        return TransactionCreationResult.created(response);
    }

    @Transactional(readOnly = true)
    public TransactionResponse findById(UUID transactionId) {
        return transactionRepository.findById(transactionId)
                .map(TransactionResponse::from)
                .orElseThrow(() -> new TransactionNotFoundException(transactionId));
    }

    private void validateRequest(CreateTransactionRequest request) {
        if (request == null) {
            throw new InvalidTransactionException("request is required");
        }
        validator.validate(request).stream()
                .min(Comparator.comparing(violation -> violation.getPropertyPath().toString()))
                .ifPresent(violation -> {
                    throw new InvalidTransactionException(
                            violation.getPropertyPath() + " " + violation.getMessage()
                    );
                });
    }

    private boolean matches(
            PaymentTransaction existing,
            CreateTransactionRequest request,
            Instant requestedCreatedAt,
            Map<String, Object> metadata
    ) {
        return request.amount() == existing.getAmountMinor()
                && request.currency().equals(existing.getCurrency())
                && request.type() == existing.getType()
                && request.sourceAccount().equals(existing.getSourceAccount())
                && request.destinationAccount().equals(existing.getDestinationAccount())
                && request.channel() == existing.getChannel()
                && (requestedCreatedAt == null || requestedCreatedAt.equals(existing.getCreatedAt()))
                && Objects.equals(metadata, existing.getMetadata());
    }
}
