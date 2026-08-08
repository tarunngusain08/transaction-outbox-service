package io.github.tarunngusain08.payments.transaction;

import io.github.tarunngusain08.payments.outbox.OutboxEvent;
import io.github.tarunngusain08.payments.outbox.OutboxEventRepository;
import io.github.tarunngusain08.payments.transaction.api.CreateTransactionRequest;
import io.github.tarunngusain08.payments.transaction.api.TransactionResponse;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
public class TransactionService {

    private static final String AGGREGATE_TYPE = "TRANSACTION";

    private final PaymentTransactionRepository transactionRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public TransactionService(
            PaymentTransactionRepository transactionRepository,
            OutboxEventRepository outboxRepository,
            ObjectMapper objectMapper,
            Clock clock
    ) {
        this.transactionRepository = transactionRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @Transactional
    public TransactionResponse create(CreateTransactionRequest request) {
        if (transactionRepository.existsByExternalReference(request.externalReference())) {
            throw new DuplicateTransactionException(request.externalReference());
        }

        Instant now = Instant.now(clock);
        var transaction = new PaymentTransaction(
                request.transactionId() == null ? UUID.randomUUID() : request.transactionId(),
                request.externalReference(),
                request.amount(),
                request.currency(),
                request.type(),
                request.status() == null ? TransactionStatus.PENDING : request.status(),
                request.sourceAccount(),
                request.destinationAccount(),
                request.channel(),
                request.createdAt() == null ? now : request.createdAt(),
                request.metadata() == null ? Map.of() : request.metadata()
        );

        transactionRepository.save(transaction);
        var response = TransactionResponse.from(transaction);
        var eventId = UUID.randomUUID();
        var event = new TransactionCreatedEvent(
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
                serialize(event),
                now
        ));

        return response;
    }

    @Transactional(readOnly = true)
    public TransactionResponse findById(UUID transactionId) {
        return transactionRepository.findById(transactionId)
                .map(TransactionResponse::from)
                .orElseThrow(() -> new TransactionNotFoundException(transactionId));
    }

    private String serialize(TransactionCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception exception) {
            throw new IllegalStateException("Could not serialize transaction-created event", exception);
        }
    }
}
