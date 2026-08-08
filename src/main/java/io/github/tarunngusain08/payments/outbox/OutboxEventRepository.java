package io.github.tarunngusain08.payments.outbox;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    @Query(value = """
            SELECT id
            FROM outbox_events
            WHERE status = 'PENDING'
              AND next_attempt_at <= :now
            ORDER BY created_at
            LIMIT :batchSize
            """, nativeQuery = true)
    List<UUID> findReadyEventIds(@Param("now") Instant now, @Param("batchSize") int batchSize);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT event FROM OutboxEvent event WHERE event.id = :id")
    Optional<OutboxEvent> findByIdForUpdate(@Param("id") UUID id);
}
