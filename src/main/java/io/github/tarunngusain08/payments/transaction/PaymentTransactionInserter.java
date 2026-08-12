package io.github.tarunngusain08.payments.transaction;

import jakarta.persistence.EntityManager;
import org.springframework.stereotype.Repository;

@Repository
public class PaymentTransactionInserter {

    private final EntityManager entityManager;

    public PaymentTransactionInserter(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    public void insert(PaymentTransaction transaction) {
        entityManager.persist(transaction);
        entityManager.flush();
    }
}
