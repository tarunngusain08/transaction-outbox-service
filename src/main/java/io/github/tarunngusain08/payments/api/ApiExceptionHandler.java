package io.github.tarunngusain08.payments.api;

import io.github.tarunngusain08.payments.normalization.NormalizationException;
import io.github.tarunngusain08.payments.transaction.DuplicateTransactionException;
import io.github.tarunngusain08.payments.transaction.TransactionNotFoundException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.util.LinkedHashMap;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler({DuplicateTransactionException.class, DataIntegrityViolationException.class})
    ProblemDetail handleConflict(Exception exception) {
        String detail = exception instanceof DuplicateTransactionException
                ? exception.getMessage()
                : "A transaction with the same identifier already exists";
        return problem(HttpStatus.CONFLICT, "Transaction conflict", detail);
    }

    @ExceptionHandler(TransactionNotFoundException.class)
    ProblemDetail handleNotFound(TransactionNotFoundException exception) {
        return problem(HttpStatus.NOT_FOUND, "Transaction not found", exception.getMessage());
    }

    @ExceptionHandler(NormalizationException.class)
    ProblemDetail handleNormalization(NormalizationException exception) {
        return problem(
                HttpStatus.UNPROCESSABLE_CONTENT,
                "Transaction could not be normalized",
                exception.getMessage()
        );
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail handleValidation(MethodArgumentNotValidException exception) {
        var errors = new LinkedHashMap<String, String>();
        exception.getBindingResult().getFieldErrors().forEach(error ->
                errors.putIfAbsent(error.getField(), error.getDefaultMessage()));

        var problem = problem(
                HttpStatus.BAD_REQUEST,
                "Invalid request",
                "One or more request fields failed validation"
        );
        problem.setProperty("errors", errors);
        return problem;
    }

    private ProblemDetail problem(HttpStatus status, String title, String detail) {
        var problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        problem.setType(URI.create("https://github.com/tarunngusain08/transaction-outbox-service/problems/"
                + status.value()));
        return problem;
    }
}
