package io.github.tarunngusain08.payments.transaction.api;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/transactions")
public class RetiredV1TransactionController {

    private static final String V2_TRANSACTIONS = "/api/v2/transactions";

    @PostMapping
    public ResponseEntity<ProblemDetail> retiredCreate() {
        return retired(V2_TRANSACTIONS);
    }

    @PostMapping("/normalize")
    public ResponseEntity<ProblemDetail> retiredNormalize() {
        return retired(V2_TRANSACTIONS + "/normalize");
    }

    @GetMapping("/{transactionId}")
    public ResponseEntity<ProblemDetail> retiredGetById(
            @PathVariable UUID transactionId
    ) {
        return retired(V2_TRANSACTIONS + "/" + transactionId);
    }

    private ResponseEntity<ProblemDetail> retired(String successor) {
        var problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.GONE,
                "API version 1 is retired because version 2 has a different ownership and identity contract"
        );
        problem.setTitle("Transaction API version 1 retired");
        problem.setType(URI.create("urn:transaction-outbox-service:api-version-retired"));
        problem.setProperty("successor", successor);
        return ResponseEntity.status(HttpStatus.GONE)
                .header(HttpHeaders.LINK, "<" + successor + ">; rel=\"successor-version\"")
                .body(problem);
    }
}
