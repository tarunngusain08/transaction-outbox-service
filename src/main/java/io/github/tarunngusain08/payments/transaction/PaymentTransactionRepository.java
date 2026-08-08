package io.github.tarunngusain08.payments.transaction;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface PaymentTransactionRepository extends JpaRepository<PaymentTransaction, UUID> {

    Optional<PaymentTransaction> findByExternalReference(String externalReference);

    boolean existsByExternalReference(String externalReference);
}
