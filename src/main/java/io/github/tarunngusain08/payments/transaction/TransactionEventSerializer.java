package io.github.tarunngusain08.payments.transaction;

import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class TransactionEventSerializer {

    private final ObjectMapper objectMapper;

    public TransactionEventSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String serialize(TransactionCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception exception) {
            throw new IllegalStateException("Could not serialize transaction-created event", exception);
        }
    }
}
