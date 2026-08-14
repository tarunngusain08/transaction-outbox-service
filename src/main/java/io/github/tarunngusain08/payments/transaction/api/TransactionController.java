package io.github.tarunngusain08.payments.transaction.api;

import io.github.tarunngusain08.payments.normalization.LegacyTransactionRequest;
import io.github.tarunngusain08.payments.normalization.TransactionNormalizer;
import io.github.tarunngusain08.payments.transaction.application.TransactionService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {

    private final TransactionService transactionService;
    private final TransactionNormalizer transactionNormalizer;

    public TransactionController(
            TransactionService transactionService,
            TransactionNormalizer transactionNormalizer
    ) {
        this.transactionService = transactionService;
        this.transactionNormalizer = transactionNormalizer;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public TransactionResponse create(@Valid @RequestBody CreateTransactionRequest request) {
        return transactionService.create(request);
    }

    @PostMapping("/normalize")
    public CreateTransactionRequest normalize(@Valid @RequestBody LegacyTransactionRequest request) {
        return transactionNormalizer.normalize(request);
    }
}
