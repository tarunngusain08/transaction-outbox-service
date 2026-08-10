package io.github.tarunngusain08.payments.integration;

import io.github.tarunngusain08.payments.outbox.OutboxEventRepository;
import io.github.tarunngusain08.payments.outbox.OutboxStatus;
import io.github.tarunngusain08.payments.transaction.PaymentTransactionRepository;
import io.github.tarunngusain08.payments.transaction.TransactionCreatedEvent;
import io.github.tarunngusain08.payments.transaction.TransactionEventSerializer;
import io.github.tarunngusain08.payments.transaction.api.CreateTransactionRequest;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        assertThat(transaction.sourceSystem()).isEqualTo("DIRECT_API");
        assertThat(transaction.externalReference()).isEqualTo(externalReference);
        assertThat(transaction.amount()).isEqualTo(150_000L);
        assertThat(transaction.receivedAt()).isNotNull();
        assertThat(transactionRepository.findBySourceSystemAndExternalReference(
                "DIRECT_API",
                externalReference
        )).isPresent();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var outboxEvents = outboxRepository.findAll();
            assertThat(outboxEvents).hasSize(1);
            assertThat(outboxEvents.getFirst().getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
            assertThat(outboxEvents.getFirst().getPublishedAt()).isNotNull();
        });

        var outboxEvent = outboxRepository.findAll().getFirst();
        assertThat(outboxEvent.getAggregateType()).isEqualTo("TRANSACTION");
        assertThat(outboxEvent.getAggregateId()).isEqualTo(transaction.transactionId());
        assertThat(outboxEvent.getEventType()).isEqualTo(TransactionCreatedEvent.EVENT_TYPE);
        assertThat(outboxEvent.getCreatedAt()).isEqualTo(transaction.receivedAt());
        assertThat(outboxEvent.getNextAttemptAt()).isEqualTo(outboxEvent.getCreatedAt());
        assertThat(outboxEvent.getRetryCount()).isZero();
        assertThat(outboxEvent.getLastError()).isNull();
        assertThat(outboxEvent.getClaimToken()).isNull();
        assertThat(outboxEvent.getClaimedAt()).isNull();

        var storedEvent = objectMapper.readValue(
                outboxEvent.getPayload(),
                TransactionCreatedEvent.class
        );
        assertThat(storedEvent.schemaVersion()).isEqualTo(TransactionCreatedEvent.SCHEMA_VERSION);
        assertThat(storedEvent.producer()).isEqualTo(TransactionCreatedEvent.PRODUCER);
        assertThat(storedEvent.eventId()).isEqualTo(outboxEvent.getId());
        assertThat(storedEvent.eventType()).isEqualTo(outboxEvent.getEventType());
        assertThat(storedEvent.occurredAt()).isEqualTo(outboxEvent.getCreatedAt());
        assertThat(storedEvent.transaction()).isEqualTo(transaction);

        var kafkaRecord = consumeEvent(externalReference);
        assertThat(kafkaRecord.key()).isEqualTo(transaction.transactionId().toString());
        assertThat(objectMapper.readTree(kafkaRecord.value()))
                .isEqualTo(objectMapper.readTree(outboxEvent.getPayload()));

        var outboxSnapshot = get("/actuator/outbox");
        assertThat(outboxSnapshot.statusCode()).isEqualTo(HttpStatus.OK.value());
        var snapshot = objectMapper.readTree(outboxSnapshot.body());
        assertThat(snapshot.path("pending").asLong()).isZero();
        assertThat(snapshot.path("processing").asLong()).isZero();
        assertThat(snapshot.has("failed")).isFalse();
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
    void atomicallyCreatesOrReplaysConcurrentIdenticalRequests() throws Exception {
        int workers = 12;
        String request = canonicalRequest("IT-CONCURRENT-" + UUID.randomUUID());
        var ready = new CountDownLatch(workers);
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(workers);

        try {
            var futures = new ArrayList<java.util.concurrent.Future<HttpResponse<String>>>();
            for (int index = 0; index < workers; index++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("concurrent create start gate timed out");
                    }
                    return post("/api/v1/transactions", request);
                }));
            }

            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            var responses = new ArrayList<HttpResponse<String>>();
            for (var future : futures) {
                responses.add(future.get(15, TimeUnit.SECONDS));
            }

            assertThat(responses).filteredOn(response -> response.statusCode() == 201).hasSize(1);
            assertThat(responses).filteredOn(response -> response.statusCode() == 200)
                    .hasSize(workers - 1);
            assertThat(responses)
                    .extracting(response -> objectMapper.readTree(response.body())
                            .path("transactionId").asString())
                    .containsOnly(objectMapper.readTree(responses.getFirst().body())
                            .path("transactionId").asString());
        } finally {
            executor.shutdownNow();
        }

        assertThat(transactionRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
    }

    @Test
    void atomicallyCreatesOneOfTwoConcurrentConflictingRequests() throws Exception {
        String externalReference = "IT-CONFLICT-" + UUID.randomUUID();
        String firstRequest = canonicalRequest(externalReference);
        String secondRequest = firstRequest.replace("150000", "999");
        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);

        try {
            var first = executor.submit(() -> gatedPost(ready, start, firstRequest));
            var second = executor.submit(() -> gatedPost(ready, start, secondRequest));

            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            var responses = List.of(
                    first.get(15, TimeUnit.SECONDS),
                    second.get(15, TimeUnit.SECONDS)
            );
            assertThat(responses)
                    .extracting(HttpResponse::statusCode)
                    .containsExactlyInAnyOrder(
                            HttpStatus.CREATED.value(),
                            HttpStatus.CONFLICT.value()
                    );

            var createdResponse = responses.stream()
                    .filter(response -> response.statusCode() == HttpStatus.CREATED.value())
                    .findFirst()
                    .orElseThrow();
            var created = objectMapper.readValue(createdResponse.body(), TransactionResponse.class);
            var stored = transactionRepository.findBySourceSystemAndExternalReference(
                    "DIRECT_API",
                    externalReference
            ).orElseThrow();
            assertThat(stored.getId()).isEqualTo(created.transactionId());
            assertThat(stored.getAmountMinor()).isEqualTo(created.amount());
        } finally {
            executor.shutdownNow();
        }

        assertThat(transactionRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
    }

    @Test
    void scopesExternalReferenceIdentityBySourceSystem() throws Exception {
        String externalReference = "IT-NAMESPACE-" + UUID.randomUUID();
        String directRequest = canonicalRequest("DIRECT_API", externalReference);
        String partnerRequest = canonicalRequest("PARTNER_BANK", externalReference);

        assertThat(post("/api/v1/transactions", directRequest).statusCode())
                .isEqualTo(HttpStatus.CREATED.value());
        assertThat(post("/api/v1/transactions", partnerRequest).statusCode())
                .isEqualTo(HttpStatus.CREATED.value());
        assertThat(transactionRepository.count()).isEqualTo(2);
        assertThat(outboxRepository.count()).isEqualTo(2);
    }

    @Test
    void normalizesLegacyPayloadAndSubmitsTheResponseUnchangedToCreate() throws Exception {
        var response = post("/api/v1/transactions/normalize", legacyRequest());

        assertThat(response.statusCode()).isEqualTo(HttpStatus.OK.value());
        var normalized = objectMapper.readValue(response.body(), CreateTransactionRequest.class);
        assertThat(normalized.sourceSystem()).isEqualTo("LEGACY_BANK_FEED");
        assertThat(normalized.amount()).isEqualTo(150_000L);
        assertThat(normalized.createdAt()).isEqualTo(Instant.parse("2026-08-08T09:02:11Z"));
        assertThat(normalized.metadata()).containsEntry("payerIfsc", "HDFC0001234");

        var created = post("/api/v1/transactions", response.body());
        assertThat(created.statusCode()).isEqualTo(HttpStatus.CREATED.value());
        var transaction = objectMapper.readValue(created.body(), TransactionResponse.class);
        assertThat(transaction.sourceSystem()).isEqualTo(normalized.sourceSystem());
        assertThat(transaction.externalReference()).isEqualTo(normalized.externalReference());
        assertThat(transaction.amount()).isEqualTo(normalized.amount());
        assertThat(transaction.createdAt()).isEqualTo(normalized.createdAt());
        assertThat(transaction.metadata()).isEqualTo(normalized.metadata());
        assertThat(transactionRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
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
    void rejectsCallerOwnedServerFieldsWithoutDurableWrites() throws Exception {
        String base = canonicalRequest("IT-OWNERSHIP-" + UUID.randomUUID());
        List<String> invalidRequests = List.of(
                base.replace(
                        "\"externalReference\"",
                        "\"transactionId\": \"%s\", \"externalReference\""
                                .formatted(UUID.randomUUID())
                ),
                base.replace(
                        "\"externalReference\"",
                        "\"status\": \"SUCCESS\", \"externalReference\""
                ),
                base.replace(
                        "\"externalReference\"",
                        "\"receivedAt\": \"2026-08-08T10:15:30Z\", \"externalReference\""
                )
        );

        for (String request : invalidRequests) {
            assertThat(post("/api/v1/transactions", request).statusCode())
                    .isEqualTo(HttpStatus.BAD_REQUEST.value());
        }
        assertThat(transactionRepository.count()).isZero();
        assertThat(outboxRepository.count()).isZero();
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
                canonicalRequest(referencePrefix + "-DUPLICATE").replace(
                        "\"amount\": 150000",
                        "\"amount\": 1, \"amount\": 150000"
                ),
                canonicalRequest(referencePrefix + "-UNKNOWN")
                        .replace("\"metadata\"", "\"statuz\": \"SUCCESS\", \"metadata\""),
                canonicalRequest(referencePrefix + "-CURRENCY").replace("\"INR\"", "\"XYZ\""),
                canonicalRequest(referencePrefix + "-SOURCE")
                        .replace("\"DIRECT_API\"", "\"direct_api\""),
                canonicalRequest("\u00a0" + referencePrefix + "-UNICODE"),
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
                ),
                canonicalRequest(referencePrefix + "-METADATA-DECIMAL").replace(
                        "{\"testRun\": \"integration\"}",
                        "{\"fraction\": 1.0}"
                ),
                canonicalRequest(referencePrefix + "-METADATA-EXPONENT").replace(
                        "{\"testRun\": \"integration\"}",
                        "{\"count\": 1e2}"
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
    void rejectsNonCanonicalIdentifiersBeforePersistence() throws Exception {
        String canonicalReference = "IT-SPACE-" + UUID.randomUUID();

        assertThat(post(
                "/api/v1/transactions",
                canonicalRequest("  " + canonicalReference + "  ")
        ).statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(post(
                "/api/v1/transactions",
                canonicalRequest(canonicalReference).replace("1234567890", " account ")
        ).statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(transactionRepository.count()).isZero();
        assertThat(outboxRepository.count()).isZero();
    }

    @Test
    void canonicalizesTimestampToDatabasePrecisionForCreateReplayAndRead() throws Exception {
        String externalReference = "IT-PRECISION-" + UUID.randomUUID();
        String request = canonicalRequest(externalReference).replace(
                "\"metadata\"",
                "\"createdAt\": \"2026-08-08T09:02:11.123456789Z\", \"metadata\""
        );

        var created = post("/api/v1/transactions", request);
        assertThat(created.statusCode()).isEqualTo(HttpStatus.CREATED.value());
        var createdBody = objectMapper.readValue(created.body(), TransactionResponse.class);
        assertThat(createdBody.createdAt())
                .isEqualTo(Instant.parse("2026-08-08T09:02:11.123456Z"));

        var replay = post("/api/v1/transactions", request);
        assertThat(replay.statusCode()).isEqualTo(HttpStatus.OK.value());
        var replayBody = objectMapper.readValue(replay.body(), TransactionResponse.class);
        assertThat(replayBody).isEqualTo(createdBody);

        var read = get("/api/v1/transactions/" + createdBody.transactionId());
        assertThat(read.statusCode()).isEqualTo(HttpStatus.OK.value());
        assertThat(objectMapper.readValue(read.body(), TransactionResponse.class))
                .isEqualTo(createdBody);
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        var request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> gatedPost(
            CountDownLatch ready,
            CountDownLatch start,
            String body
    ) throws Exception {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("concurrent create start gate timed out");
        }
        return post("/api/v1/transactions", body);
    }

    private HttpResponse<String> get(String path) throws Exception {
        var request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private ConsumedKafkaRecord consumeEvent(String externalReference) {
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
                    var event = objectMapper.readTree(record.value());
                    if (event.path("transaction").path("externalReference")
                            .asString().equals(externalReference)) {
                        return new ConsumedKafkaRecord(record.key(), record.value());
                    }
                }
            }
        }

        throw new AssertionError("Kafka event not received for " + externalReference);
    }

    private String canonicalRequest(String externalReference) {
        return canonicalRequest("DIRECT_API", externalReference);
    }

    private String canonicalRequest(String sourceSystem, String externalReference) {
        return """
                {
                  "sourceSystem": "%s",
                  "externalReference": "%s",
                  "amount": 150000,
                  "currency": "INR",
                  "type": "DEBIT",
                  "sourceAccount": "1234567890",
                  "destinationAccount": "9876543210",
                  "channel": "UPI",
                  "metadata": {"testRun": "integration"}
                }
                """.formatted(sourceSystem, externalReference);
    }

    private String invalidCanonicalRequest() {
        return """
                {
                  "sourceSystem": "DIRECT_API",
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

    private record ConsumedKafkaRecord(String key, String value) {
    }
}
