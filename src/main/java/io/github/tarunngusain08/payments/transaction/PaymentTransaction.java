package io.github.tarunngusain08.payments.transaction;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(
        name = "transactions",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_transactions_source_reference",
                columnNames = {"source_system", "external_reference"}
        )
)
public class PaymentTransaction {

    @Id
    private UUID id;

    @Column(name = "source_system", nullable = false, length = 32)
    private String sourceSystem;

    @Column(name = "external_reference", nullable = false, length = 100)
    private String externalReference;

    @Column(name = "amount_minor", nullable = false)
    private long amountMinor;

    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private TransactionType type;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private TransactionStatus status;

    @Column(name = "source_account", nullable = false, length = 64)
    private String sourceAccount;

    @Column(name = "destination_account", nullable = false, length = 64)
    private String destinationAccount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 24)
    private PaymentChannel channel;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "received_at", nullable = false, updatable = false)
    private Instant receivedAt;

    @Column(name = "request_fingerprint", length = 64)
    private String requestFingerprint;

    @Column(name = "request_fingerprint_version")
    private Short requestFingerprintVersion;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private Map<String, Object> metadata = new LinkedHashMap<>();

    protected PaymentTransaction() {
    }

    public PaymentTransaction(
            UUID id,
            String sourceSystem,
            String externalReference,
            long amountMinor,
            String currency,
            TransactionType type,
            TransactionStatus status,
            String sourceAccount,
            String destinationAccount,
            PaymentChannel channel,
            Instant createdAt,
            Instant receivedAt,
            Map<String, Object> metadata,
            String requestFingerprint,
            Short requestFingerprintVersion
    ) {
        this.id = id;
        this.sourceSystem = sourceSystem;
        this.externalReference = externalReference;
        this.amountMinor = amountMinor;
        this.currency = currency;
        this.type = type;
        this.status = status;
        this.sourceAccount = sourceAccount;
        this.destinationAccount = destinationAccount;
        this.channel = channel;
        this.createdAt = createdAt;
        this.receivedAt = receivedAt;
        this.metadata = metadata == null ? new LinkedHashMap<>() : new LinkedHashMap<>(metadata);
        this.requestFingerprint = requestFingerprint;
        this.requestFingerprintVersion = requestFingerprintVersion;
    }

    public UUID getId() {
        return id;
    }

    public String getSourceSystem() {
        return sourceSystem;
    }

    public String getExternalReference() {
        return externalReference;
    }

    public long getAmountMinor() {
        return amountMinor;
    }

    public String getCurrency() {
        return currency;
    }

    public TransactionType getType() {
        return type;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public String getSourceAccount() {
        return sourceAccount;
    }

    public String getDestinationAccount() {
        return destinationAccount;
    }

    public PaymentChannel getChannel() {
        return channel;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public String getRequestFingerprint() {
        return requestFingerprint;
    }

    public Short getRequestFingerprintVersion() {
        return requestFingerprintVersion;
    }

    public Map<String, Object> getMetadata() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
    }
}
