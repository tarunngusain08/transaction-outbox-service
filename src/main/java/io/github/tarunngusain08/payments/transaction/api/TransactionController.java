package io.github.tarunngusain08.payments.transaction.api;

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

    public TransactionController(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @PostMapping
    public ResponseEntity<TransactionResponse> create(
            @Valid @RequestBody CreateTransactionRequest request
    ) {
        var transaction = transactionService.create(request);
        return ResponseEntity
                .created(URI.create("/api/v1/transactions/" + transaction.transactionId()))
                .body(transaction);
    }

    @GetMapping("/{transactionId}")
    public TransactionResponse findById(@PathVariable UUID transactionId) {
        return transactionService.findById(transactionId);
    }
}
