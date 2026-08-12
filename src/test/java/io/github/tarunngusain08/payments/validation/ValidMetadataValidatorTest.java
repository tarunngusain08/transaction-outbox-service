package io.github.tarunngusain08.payments.validation;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ValidMetadataValidatorTest {

    private final ValidMetadataValidator validator = new ValidMetadataValidator();

    @Test
    void acceptsBoundedJsonMetadata() {
        assertThat(validator.isValid(Map.of(
                "orderId", "ORDER-99",
                "attempt", 2,
                "tags", List.of("grocery", "upi"),
                "routing", Map.of("priority", true)
        ), null)).isTrue();
    }

    @Test
    void rejectsExcessiveDepthEntriesStringsAndEncodedBytes() {
        Map<String, Object> tooDeep = Map.of(
                "one", Map.of("two", Map.of("three", Map.of("four", Map.of("five", "value"))))
        );
        var tooManyEntries = new LinkedHashMap<String, Object>();
        for (int index = 0; index <= ValidMetadataValidator.MAX_ENTRIES; index++) {
            tooManyEntries.put("key-" + index, index);
        }
        var oversizedEncodedValue = Map.<String, Object>of(
                "values",
                Collections.nCopies(17, "x".repeat(ValidMetadataValidator.MAX_STRING_BYTES))
        );

        assertThat(validator.isValid(tooDeep, null)).isFalse();
        assertThat(validator.isValid(tooManyEntries, null)).isFalse();
        assertThat(validator.isValid(Map.of(
                "note",
                "x".repeat(ValidMetadataValidator.MAX_STRING_BYTES + 1)
        ), null)).isFalse();
        assertThat(validator.isValid(oversizedEncodedValue, null)).isFalse();
    }

    @Test
    void acceptsOnlySignedLongIntegerNumbersAndNonblankKeys() {
        assertThat(validator.isValid(Map.of("minimum", Long.MIN_VALUE), null)).isTrue();
        assertThat(validator.isValid(Map.of("maximum", BigInteger.valueOf(Long.MAX_VALUE)), null))
                .isTrue();
        assertThat(validator.isValid(Map.of("decimal", new BigDecimal("1.0")), null)).isFalse();
        assertThat(validator.isValid(Map.of("floating", 1.0D), null)).isFalse();
        assertThat(validator.isValid(Map.of(
                "overflow",
                BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)
        ), null)).isFalse();
        assertThat(validator.isValid(Map.of(" ", "value"), null)).isFalse();
    }
}
