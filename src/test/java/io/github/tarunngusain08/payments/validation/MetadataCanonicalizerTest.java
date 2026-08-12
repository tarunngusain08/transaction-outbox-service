package io.github.tarunngusain08.payments.validation;

import io.github.tarunngusain08.payments.transaction.InvalidTransactionException;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataCanonicalizerTest {

    private final MetadataCanonicalizer canonicalizer =
            new MetadataCanonicalizer(new ObjectMapper());

    @Test
    void sortsObjectKeysAndCanonicalizesEveryIntegerToLong() {
        var metadata = new LinkedHashMap<String, Object>();
        metadata.put("zeta", 2);
        metadata.put("alpha", Map.of("large", BigInteger.valueOf(2_147_483_648L)));
        var values = new ArrayList<>();
        values.add((short) 3);
        values.add(null);
        metadata.put("values", values);

        var canonical = canonicalizer.canonicalize(metadata);

        assertThat(canonical.keySet()).containsExactly("alpha", "values", "zeta");
        assertThat(canonical.get("zeta")).isEqualTo(2L);
        assertThat(canonical).extractingByKey("alpha")
                .isEqualTo(Map.of("large", 2_147_483_648L));
        assertThat(canonical).extractingByKey("values")
                .isEqualTo(java.util.Arrays.asList(3L, null));
    }

    @Test
    void rejectsDecimalFloatingAndOutOfRangeNumbers() {
        List<Object> rejected = List.of(
                new BigDecimal("1.0"),
                1.0D,
                BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)
        );

        for (Object value : rejected) {
            assertThatThrownBy(() -> canonicalizer.canonicalize(Map.of("value", value)))
                    .isInstanceOf(InvalidTransactionException.class)
                    .hasMessageContaining("metadata");
        }
    }
}
