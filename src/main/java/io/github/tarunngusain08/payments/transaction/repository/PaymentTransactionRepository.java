package io.github.tarunngusain08.payments.transaction.repository;

import io.github.tarunngusain08.payments.transaction.domain.PaymentTransaction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface PaymentTransactionRepository extends JpaRepository<PaymentTransaction, UUID> {
}
