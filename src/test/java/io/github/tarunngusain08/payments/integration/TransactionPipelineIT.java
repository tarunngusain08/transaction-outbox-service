package io.github.tarunngusain08.payments.integration;

import io.github.tarunngusain08.payments.outbox.OutboxEventRepository;
import io.github.tarunngusain08.payments.outbox.OutboxStatus;
import io.github.tarunngusain08.payments.transaction.PaymentTransactionRepository;
import io.github.tarunngusain08.payments.transaction.api.TransactionResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "payments.outbox.poll-delay=100ms"
)
class TransactionPipelineIT {

    private static final String TOPIC = "payments.transactions.created";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            DockerImageName.parse("postgres:17.10-alpine")
    )
            .withDatabaseName("payments")
            .withUsername("payments")
            .withPassword("payments");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka:4.1.2")
    );

    @DynamicPropertySource
    static void configureInfrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("payments.outbox.topic", () -> TOPIC);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private PaymentTransactionRepository transactionRepository;

    @Autowired
    private OutboxEventRepository outboxRepository;

    private final HttpClient httpClient = HttpClient.newHttpClient();

    @BeforeEach
    void cleanDatabase() {
        outboxRepository.deleteAllInBatch();
        transactionRepository.deleteAllInBatch();
    }

    @Test
    void createsTransactionAndPublishesStoredOutboxPayloadToKafka() throws Exception {
        String externalReference = "IT-SUCCESS-" + UUID.randomUUID();

        var response = post("/api/v1/transactions", canonicalRequest(externalReference));

        assertThat(response.statusCode()).isEqualTo(HttpStatus.CREATED.value());
        var transaction = objectMapper.readValue(response.body(), TransactionResponse.class);
        assertThat(transaction.externalReference()).isEqualTo(externalReference);
        assertThat(transaction.amount()).isEqualTo(150_000L);
        assertThat(transactionRepository.findByExternalReference(externalReference)).isPresent();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var outboxEvents = outboxRepository.findAll();
            assertThat(outboxEvents).hasSize(1);
            assertThat(outboxEvents.getFirst().getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
            assertThat(outboxEvents.getFirst().getPublishedAt()).isNotNull();
        });

        String kafkaPayload = consumeEvent(externalReference);
        var event = objectMapper.readTree(kafkaPayload);
        assertThat(event.path("eventType").asString()).isEqualTo("TRANSACTION_CREATED");
        assertThat(event.path("transaction").path("transactionId").asString())
                .isEqualTo(transaction.transactionId().toString());
    }

    @Test
    void returnsExpectedErrorsWithoutAddingExtraRows() throws Exception {
        String externalReference = "IT-FAILURE-" + UUID.randomUUID();

        assertThat(post("/api/v1/transactions", canonicalRequest(externalReference)).statusCode())
                .isEqualTo(HttpStatus.CREATED.value());
        assertThat(post("/api/v1/transactions", canonicalRequest(externalReference)).statusCode())
                .isEqualTo(HttpStatus.CONFLICT.value());
        assertThat(post("/api/v1/transactions", invalidCanonicalRequest()).statusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST.value());

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(outboxRepository.findAll())
                        .singleElement()
                        .extracting(event -> event.getStatus())
                        .isEqualTo(OutboxStatus.PUBLISHED));
        assertThat(transactionRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
    }

    @Test
    void normalizesLegacyPayloadWithoutPersistingIt() throws Exception {
        var response = post("/api/v1/transactions/normalize", legacyRequest());

        assertThat(response.statusCode()).isEqualTo(HttpStatus.OK.value());
        var normalized = objectMapper.readValue(response.body(), TransactionResponse.class);
        assertThat(normalized.amount()).isEqualTo(150_000L);
        assertThat(normalized.createdAt()).isEqualTo(Instant.parse("2026-08-08T09:02:11Z"));
        assertThat(normalized.metadata()).containsEntry("payerIfsc", "HDFC0001234");
        assertThat(transactionRepository.count()).isZero();
        assertThat(outboxRepository.count()).isZero();
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        var request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private String consumeEvent(String externalReference) {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "pipeline-it-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            consumer.subscribe(List.of(TOPIC));
            Instant deadline = Instant.now().plusSeconds(15);

            while (Instant.now().isBefore(deadline)) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.value().contains(externalReference)) {
                        return record.value();
                    }
                }
            }
        }

        throw new AssertionError("Kafka event not received for " + externalReference);
    }

    private String canonicalRequest(String externalReference) {
        return """
                {
                  "externalReference": "%s",
                  "amount": 150000,
                  "currency": "INR",
                  "type": "DEBIT",
                  "sourceAccount": "1234567890",
                  "destinationAccount": "9876543210",
                  "channel": "UPI",
                  "metadata": {"testRun": "integration"}
                }
                """.formatted(externalReference);
    }

    private String invalidCanonicalRequest() {
        return """
                {
                  "externalReference": "IT-INVALID",
                  "amount": 0,
                  "currency": "INR",
                  "type": "DEBIT",
                  "sourceAccount": "1234567890",
                  "destinationAccount": "9876543210",
                  "channel": "UPI"
                }
                """;
    }

    private String legacyRequest() {
        return """
                {
                  "txn_ref": "IT-LEGACY",
                  "txn_amount": "1500.00",
                  "ccy": "INR",
                  "txn_type": "DR",
                  "payer": {"acct_no": "1234567890", "ifsc": "HDFC0001234"},
                  "payee": {"acct_no": "9876543210", "ifsc": "ICIC0005678"},
                  "mode": "UPI",
                  "txn_date": "08-08-2026 14:32:11",
                  "remarks": "Integration test"
                }
                """;
    }
}
