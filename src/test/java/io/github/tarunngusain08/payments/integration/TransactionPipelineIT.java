package io.github.tarunngusain08.payments.integration;

import io.github.tarunngusain08.payments.outbox.OutboxEventRepository;
import io.github.tarunngusain08.payments.outbox.OutboxStatus;
import io.github.tarunngusain08.payments.transaction.PaymentTransactionRepository;
import io.github.tarunngusain08.payments.transaction.TransactionCreatedEvent;
import io.github.tarunngusain08.payments.transaction.TransactionEventSerializer;
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
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

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
            DockerImageName.parse(
                    "postgres:17.10-alpine@sha256:"
                            + "742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193"
            ).asCompatibleSubstituteFor("postgres")
    )
            .withDatabaseName("payments")
            .withUsername("payments")
            .withPassword("payments");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse(
                    "apache/kafka:4.1.2@sha256:"
                            + "5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e"
            ).asCompatibleSubstituteFor("apache/kafka")
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

    @MockitoSpyBean
    private TransactionEventSerializer eventSerializer;

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
        assertThat(event.path("schemaVersion").asInt()).isEqualTo(1);
        assertThat(event.path("producer").asString()).isEqualTo("transaction-outbox-service");
        assertThat(event.path("transaction").path("transactionId").asString())
                .isEqualTo(transaction.transactionId().toString());

        var outboxSnapshot = get("/actuator/outbox");
        assertThat(outboxSnapshot.statusCode()).isEqualTo(HttpStatus.OK.value());
        var snapshot = objectMapper.readTree(outboxSnapshot.body());
        assertThat(snapshot.path("pending").asLong()).isZero();
        assertThat(snapshot.path("processing").asLong()).isZero();
        assertThat(snapshot.path("failed").asLong()).isZero();
    }

    @Test
    void returnsExpectedErrorsWithoutAddingExtraRows() throws Exception {
        String externalReference = "IT-FAILURE-" + UUID.randomUUID();

        assertThat(post("/api/v1/transactions", canonicalRequest(externalReference)).statusCode())
                .isEqualTo(HttpStatus.CREATED.value());
        assertThat(post("/api/v1/transactions", canonicalRequest(externalReference)).statusCode())
                .isEqualTo(HttpStatus.OK.value());
        assertThat(post(
                "/api/v1/transactions",
                canonicalRequest(externalReference).replace("150000", "999")
        ).statusCode()).isEqualTo(HttpStatus.CONFLICT.value());
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

    @Test
    void rollsBackTheTransactionInsertWhenEventSerializationFails() throws Exception {
        doThrow(new IllegalStateException("forced serialization failure"))
                .when(eventSerializer)
                .serialize(any(TransactionCreatedEvent.class));

        var response = post(
                "/api/v1/transactions",
                canonicalRequest("IT-ROLLBACK-" + UUID.randomUUID())
        );

        assertThat(response.statusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        assertThat(transactionRepository.count()).isZero();
        assertThat(outboxRepository.count()).isZero();
    }

    @Test
    void rejectsUuidCollisionWithoutChangingTheOriginalTransaction() throws Exception {
        UUID transactionId = UUID.randomUUID();
        String originalReference = "IT-ID-ORIGINAL-" + UUID.randomUUID();
        String conflictingReference = "IT-ID-CONFLICT-" + UUID.randomUUID();

        assertThat(post(
                "/api/v1/transactions",
                canonicalRequest(transactionId, originalReference, 150_000L, "INR", "DEBIT")
        ).statusCode()).isEqualTo(HttpStatus.CREATED.value());

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(outboxRepository.findAll())
                        .singleElement()
                        .extracting(event -> event.getStatus())
                        .isEqualTo(OutboxStatus.PUBLISHED));

        var collision = post(
                "/api/v1/transactions",
                canonicalRequest(transactionId, conflictingReference, 999L, "USD", "CREDIT")
        );

        assertThat(collision.statusCode()).isEqualTo(HttpStatus.CONFLICT.value());
        var stored = transactionRepository.findById(transactionId).orElseThrow();
        assertThat(stored.getExternalReference()).isEqualTo(originalReference);
        assertThat(stored.getAmountMinor()).isEqualTo(150_000L);
        assertThat(stored.getCurrency()).isEqualTo("INR");
        assertThat(stored.getType().name()).isEqualTo("DEBIT");
        assertThat(outboxRepository.count()).isEqualTo(1);
    }

    @Test
    void rejectsLossyOrAmbiguousCanonicalJsonWithoutDurableWrites() throws Exception {
        String referencePrefix = "IT-STRICT-" + UUID.randomUUID();
        List<String> invalidRequests = List.of(
                canonicalRequest(referencePrefix + "-DECIMAL").replace("150000", "100.99"),
                canonicalRequest(referencePrefix + "-STRING").replace("150000", "\"100\""),
                canonicalRequest(referencePrefix + "-EXPONENT").replace("150000", "1e2"),
                canonicalRequest(referencePrefix + "-OVERFLOW")
                        .replace("150000", "9223372036854775808"),
                canonicalRequest(referencePrefix + "-ENUM").replace("\"DEBIT\"", "0"),
                canonicalRequest(referencePrefix + "-UNKNOWN")
                        .replace("\"metadata\"", "\"statuz\": \"SUCCESS\", \"metadata\""),
                canonicalRequest(referencePrefix + "-CURRENCY").replace("\"INR\"", "\"XYZ\""),
                canonicalRequest(referencePrefix + "-TIME").replace(
                        "\"metadata\"",
                        "\"createdAt\": \"+300000-01-01T00:00:00Z\", \"metadata\""
                ),
                canonicalRequest(referencePrefix + "-METADATA").replace(
                        "\"integration\"",
                        "\"" + "x".repeat(17_000) + "\""
                ),
                canonicalRequest(referencePrefix + "-DOMAIN-METADATA").replace(
                        "\"integration\"",
                        "\"" + "x".repeat(1_100) + "\""
                )
        );

        for (String request : invalidRequests) {
            assertThat(post("/api/v1/transactions", request).statusCode())
                    .as("request must be rejected: %s", request.substring(0, Math.min(120, request.length())))
                    .isEqualTo(HttpStatus.BAD_REQUEST.value());
        }

        assertThat(transactionRepository.count()).isZero();
        assertThat(outboxRepository.count()).isZero();
    }

    @Test
    void rejectsExtremeLegacyScalarsBeforeNormalization() throws Exception {
        var response = post(
                "/api/v1/transactions/normalize",
                legacyRequest().replace("1500.00", "1e2147483647")
        );

        assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(transactionRepository.count()).isZero();
        assertThat(outboxRepository.count()).isZero();
    }

    @Test
    void canonicalizesExternalReferenceBeforeDuplicateDetectionAndPersistence() throws Exception {
        String canonicalReference = "IT-SPACE-" + UUID.randomUUID();

        var created = post("/api/v1/transactions", canonicalRequest("  " + canonicalReference + "  "));
        assertThat(created.statusCode()).isEqualTo(HttpStatus.CREATED.value());
        var body = objectMapper.readValue(created.body(), TransactionResponse.class);
        assertThat(body.externalReference()).isEqualTo(canonicalReference);

        var replay = post("/api/v1/transactions", canonicalRequest(canonicalReference));
        assertThat(replay.statusCode()).isEqualTo(HttpStatus.OK.value());
        assertThat(objectMapper.readValue(replay.body(), TransactionResponse.class).transactionId())
                .isEqualTo(body.transactionId());
        assertThat(transactionRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        var request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        var request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET()
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

    private String canonicalRequest(
            UUID transactionId,
            String externalReference,
            long amount,
            String currency,
            String type
    ) {
        return """
                {
                  "transactionId": "%s",
                  "externalReference": "%s",
                  "amount": %d,
                  "currency": "%s",
                  "type": "%s",
                  "sourceAccount": "1234567890",
                  "destinationAccount": "9876543210",
                  "channel": "UPI",
                  "metadata": {"testRun": "integration"}
                }
                """.formatted(transactionId, externalReference, amount, currency, type);
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
