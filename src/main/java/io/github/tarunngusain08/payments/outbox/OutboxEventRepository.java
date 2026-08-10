package io.github.tarunngusain08.payments.outbox;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    @Query(value = """
            SELECT id
            FROM outbox_events
            WHERE (status = 'PENDING' AND next_attempt_at <= :now)
               OR (status = 'PROCESSING' AND claimed_at <= :expiredBefore)
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    Optional<UUID> findNextClaimableEventId(
            @Param("now") Instant now,
            @Param("expiredBefore") Instant expiredBefore
    );

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT event FROM OutboxEvent event WHERE event.id = :id")
    Optional<OutboxEvent> findByIdForUpdate(@Param("id") UUID id);

    @Query(value = """
            SELECT
                COUNT(*) FILTER (WHERE status = 'PENDING') AS pending,
                COUNT(*) FILTER (WHERE status = 'PROCESSING') AS processing,
                COUNT(*) FILTER (WHERE status = 'QUARANTINED') AS quarantined,
                MIN(created_at) FILTER (
                    WHERE status IN ('PENDING', 'PROCESSING')
                ) AS "oldestUnpublishedAt",
                MIN(quarantined_at) FILTER (
                    WHERE status = 'QUARANTINED'
                ) AS "oldestQuarantinedAt"
            FROM outbox_events
            WHERE status IN ('PENDING', 'PROCESSING', 'QUARANTINED')
            """, nativeQuery = true)
    OutboxDeliveryState summarizeDeliveryState();
}
