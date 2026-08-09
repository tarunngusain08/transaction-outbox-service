package io.github.tarunngusain08.payments.api;

import io.github.tarunngusain08.payments.normalization.NormalizationException;
import io.github.tarunngusain08.payments.transaction.DuplicateTransactionException;
import io.github.tarunngusain08.payments.transaction.InvalidTransactionException;
import io.github.tarunngusain08.payments.transaction.TransactionNotFoundException;
import org.hibernate.exception.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Set;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);
    private static final Set<String> TRANSACTION_UNIQUE_CONSTRAINTS = Set.of(
            "transactions_pkey",
            "uk_transactions_external_reference"
    );

    @ExceptionHandler(DuplicateTransactionException.class)
    ProblemDetail handleConflict(DuplicateTransactionException exception) {
        return problem(HttpStatus.CONFLICT, "Transaction conflict", exception.getMessage());
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    ProblemDetail handleDataIntegrity(DataIntegrityViolationException exception) {
        String constraintName = findConstraintName(exception);
        if (constraintName != null && TRANSACTION_UNIQUE_CONSTRAINTS.contains(constraintName)) {
            return problem(
                    HttpStatus.CONFLICT,
                    "Transaction conflict",
                    "A transaction with the same identifier already exists"
            );
        }

        log.error("Unexpected database integrity violation", exception);
        return problem(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Database operation failed",
                "The transaction could not be stored"
        );
    }

    @ExceptionHandler(InvalidTransactionException.class)
    ProblemDetail handleInvalidTransaction(InvalidTransactionException exception) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid transaction", exception.getMessage());
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

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ProblemDetail handleUnreadableMessage(HttpMessageNotReadableException exception) {
        log.debug("Rejected unreadable request body", exception);
        return problem(
                HttpStatus.BAD_REQUEST,
                "Invalid request",
                "The JSON body is malformed or contains unsupported value types"
        );
    }

    private ProblemDetail problem(HttpStatus status, String title, String detail) {
        var problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setType(URI.create("about:blank"));
        problem.setTitle(title);
        return problem;
    }

    private String findConstraintName(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof ConstraintViolationException violation) {
                return violation.getConstraintName();
            }
            current = current.getCause();
        }
        return null;
    }
}
