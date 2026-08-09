package io.github.tarunngusain08.payments.transaction.api;

import io.github.tarunngusain08.payments.normalization.TransactionNormalizer;
import io.github.tarunngusain08.payments.normalization.api.LegacyTransactionRequest;
import io.github.tarunngusain08.payments.transaction.TransactionService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

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
    public ResponseEntity<TransactionResponse> create(
            @Valid @RequestBody CreateTransactionRequest request
    ) {
        var result = transactionService.create(request);
        var transaction = result.transaction();
        var location = URI.create("/api/v1/transactions/" + transaction.transactionId());
        return result.created()
                ? ResponseEntity.created(location).body(transaction)
                : ResponseEntity.ok().location(location).body(transaction);
    }

    @GetMapping("/{transactionId}")
    public TransactionResponse findById(@PathVariable UUID transactionId) {
        return transactionService.findById(transactionId);
    }

    @PostMapping("/normalize")
    public TransactionResponse normalize(
            @Valid @RequestBody LegacyTransactionRequest request
    ) {
        return transactionNormalizer.normalize(request);
    }
}
