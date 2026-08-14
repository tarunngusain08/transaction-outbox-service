package io.github.tarunngusain08.payments.outbox.repository;

import io.github.tarunngusain08.payments.outbox.domain.OutboxEvent;
import io.github.tarunngusain08.payments.outbox.domain.OutboxStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT event
            FROM OutboxEvent event
            WHERE event.status = :status
              AND event.nextAttemptAt <= :now
            ORDER BY event.createdAt
            """)
    List<OutboxEvent> findReady(
            @Param("status") OutboxStatus status,
            @Param("now") Instant now,
            Pageable pageable
    );
}
