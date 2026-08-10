package io.github.tarunngusain08.payments.transaction.api;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

@RestController
@RequestMapping("/api/v1/transactions")
public class RetiredV1TransactionController {

    private static final String SUCCESSOR = "/api/v2/transactions";

    @RequestMapping(
            path = {"", "/normalize", "/{transactionId}"},
            method = {RequestMethod.GET, RequestMethod.POST}
    )
    public ResponseEntity<ProblemDetail> retired() {
        var problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.GONE,
                "API version 1 is retired because version 2 has a different ownership and identity contract"
        );
        problem.setTitle("Transaction API version 1 retired");
        problem.setType(URI.create("urn:transaction-outbox-service:api-version-retired"));
        problem.setProperty("successor", SUCCESSOR);
        return ResponseEntity.status(HttpStatus.GONE)
                .header(HttpHeaders.LINK, "<" + SUCCESSOR + ">; rel=\"successor-version\"")
                .body(problem);
    }
}
