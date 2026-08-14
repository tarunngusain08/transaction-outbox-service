package io.github.tarunngusain08.payments.transaction.application;

import io.github.tarunngusain08.payments.outbox.domain.OutboxEvent;
import io.github.tarunngusain08.payments.outbox.repository.OutboxEventRepository;
import io.github.tarunngusain08.payments.transaction.api.CreateTransactionRequest;
import io.github.tarunngusain08.payments.transaction.api.TransactionResponse;
import io.github.tarunngusain08.payments.transaction.domain.PaymentTransaction;
import io.github.tarunngusain08.payments.transaction.domain.TransactionStatus;
import io.github.tarunngusain08.payments.transaction.event.TransactionCreatedEvent;
import io.github.tarunngusain08.payments.transaction.repository.PaymentTransactionRepository;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
public class TransactionService {

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
        Instant now = Instant.now(clock);
        var transaction = new PaymentTransaction(
                UUID.randomUUID(),
                request.externalReference(),
                request.amount(),
                request.currency(),
                request.type(),
                TransactionStatus.PENDING,
                request.sourceAccount(),
                request.destinationAccount(),
                request.channel(),
                request.createdAt() == null ? now : request.createdAt(),
                request.metadata() == null ? Map.of() : request.metadata()
        );

        transactionRepository.saveAndFlush(transaction);
        var response = TransactionResponse.from(transaction);
        var event = new TransactionCreatedEvent(
                UUID.randomUUID(),
                TransactionCreatedEvent.EVENT_TYPE,
                now,
                response
        );

        outboxRepository.save(OutboxEvent.pending(
                event.eventId(),
                transaction.getId(),
                event.eventType(),
                serialize(event),
                now
        ));

        return response;
    }

    private String serialize(TransactionCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception exception) {
            throw new IllegalStateException("Could not serialize transaction-created event", exception);
        }
    }
}
