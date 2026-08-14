package io.github.tarunngusain08.payments.integration;

import io.github.tarunngusain08.payments.outbox.domain.OutboxStatus;
import io.github.tarunngusain08.payments.outbox.repository.OutboxEventRepository;
import io.github.tarunngusain08.payments.transaction.event.TransactionCreatedEvent;
import io.github.tarunngusain08.payments.transaction.repository.PaymentTransactionRepository;
import io.github.tarunngusain08.payments.transaction.domain.TransactionType;
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
            DockerImageName.parse("postgres:17.10-alpine").asCompatibleSubstituteFor("postgres")
    )
            .withDatabaseName("payments")
            .withUsername("payments")
            .withPassword("payments");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka:4.1.2").asCompatibleSubstituteFor("apache/kafka")
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
    void createsTransactionAndPublishesItsEventToKafka() throws Exception {
        String reference = "IT-CREATE-" + UUID.randomUUID();

        HttpResponse<String> response = post("/api/v1/transactions", canonicalRequest(reference));

        assertThat(response.statusCode()).isEqualTo(HttpStatus.CREATED.value());
        TransactionResponse transaction = objectMapper.readValue(response.body(), TransactionResponse.class);
        assertThat(transaction.externalReference()).isEqualTo(reference);
        assertThat(transactionRepository.findById(transaction.transactionId())).isPresent();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(outboxRepository.findAll())
                    .singleElement()
                    .extracting(event -> event.getStatus())
                    .isEqualTo(OutboxStatus.PUBLISHED);
        });

        KafkaRecord event = consumeEvent(reference);
        assertThat(event.key()).isEqualTo(transaction.transactionId().toString());
        var body = objectMapper.readValue(event.value(), TransactionCreatedEvent.class);
        assertThat(body.eventType()).isEqualTo(TransactionCreatedEvent.EVENT_TYPE);
        assertThat(body.transaction()).isEqualTo(transaction);
    }

    @Test
    void normalizesLegacyJsonIntoTheCreateSchema() throws Exception {
        String reference = "IT-NORMALIZE-" + UUID.randomUUID();

        HttpResponse<String> normalizedResponse = post(
                "/api/v1/transactions/normalize",
                legacyRequest(reference)
        );

        assertThat(normalizedResponse.statusCode()).isEqualTo(HttpStatus.OK.value());
        CreateTransactionRequest normalized = objectMapper.readValue(
                normalizedResponse.body(),
                CreateTransactionRequest.class
        );
        assertThat(normalized.externalReference()).isEqualTo(reference);
        assertThat(normalized.amount()).isEqualTo(150_000L);
        assertThat(normalized.type()).isEqualTo(TransactionType.DEBIT);
        assertThat(normalized.createdAt()).isEqualTo(Instant.parse("2026-08-08T09:02:11Z"));

        HttpResponse<String> created = post("/api/v1/transactions", normalizedResponse.body());
        assertThat(created.statusCode()).isEqualTo(HttpStatus.CREATED.value());
        assertThat(transactionRepository.count()).isEqualTo(1);
    }

    @Test
    void rejectsDuplicateExternalReferences() throws Exception {
        String request = canonicalRequest("IT-DUPLICATE-" + UUID.randomUUID());

        assertThat(post("/api/v1/transactions", request).statusCode())
                .isEqualTo(HttpStatus.CREATED.value());
        assertThat(post("/api/v1/transactions", request).statusCode())
                .isEqualTo(HttpStatus.CONFLICT.value());
        assertThat(transactionRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
    }

    @Test
    void rejectsInvalidTransactionsWithoutWritingRows() throws Exception {
        String invalid = canonicalRequest("IT-INVALID-" + UUID.randomUUID())
                .replace("150000", "0");

        HttpResponse<String> response = post("/api/v1/transactions", invalid);

        assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
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

    private KafkaRecord consumeEvent(String externalReference) {
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
                    if (externalReference.equals(
                            event.path("transaction").path("externalReference").asString()
                    )) {
                        return new KafkaRecord(record.key(), record.value());
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

    private String legacyRequest(String externalReference) {
        return """
                {
                  "txn_ref": "%s",
                  "txn_amount": "1500.00",
                  "ccy": "INR",
                  "txn_type": "DR",
                  "payer": {"acct_no": "1234567890", "ifsc": "HDFC0001234"},
                  "payee": {"acct_no": "9876543210", "ifsc": "ICIC0005678"},
                  "mode": "UPI",
                  "txn_date": "08-08-2026 14:32:11",
                  "remarks": "Integration test"
                }
                """.formatted(externalReference);
    }

    private record KafkaRecord(String key, String value) {
    }
}
