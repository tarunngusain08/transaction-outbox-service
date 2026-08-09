package io.github.tarunngusain08.payments.api;

import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.converter.HttpMessageNotReadableException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ApiExceptionHandlerTest {

    private final ApiExceptionHandler handler = new ApiExceptionHandler();

    @Test
    void mapsOnlyKnownTransactionUniqueConstraintsToConflict() {
        var violation = mock(ConstraintViolationException.class);
        when(violation.getConstraintName()).thenReturn("transactions_pkey");

        var problem = handler.handleDataIntegrity(
                new DataIntegrityViolationException("duplicate", violation)
        );

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.CONFLICT.value());
    }

    @Test
    void mapsUnrelatedIntegrityFailuresToServerError() {
        var problem = handler.handleDataIntegrity(
                new DataIntegrityViolationException("unrelated check failure")
        );

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        assertThat(problem.getType().toString()).isEqualTo("about:blank");
    }

    @Test
    void sanitizesStrictJsonParsingFailures() {
        var problem = handler.handleUnreadableMessage(
                new HttpMessageNotReadableException("internal parser detail", mock(HttpInputMessage.class))
        );

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(problem.getDetail()).doesNotContain("internal parser detail");
        assertThat(problem.getType().toString()).isEqualTo("about:blank");
    }
}
