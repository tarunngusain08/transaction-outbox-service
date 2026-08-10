package io.github.tarunngusain08.payments.validation;

import io.github.tarunngusain08.payments.transaction.InvalidTransactionException;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Component
public class MetadataCanonicalizer {

    public static final int MAX_DEPTH = 5;
    public static final int MAX_NODES = 200;
    public static final int MAX_ENTRIES = 100;
    public static final int MAX_KEY_BYTES = 128;
    public static final int MAX_STRING_BYTES = 1_024;
    public static final int MAX_ENCODED_BYTES = 16_384;

    private final ObjectMapper objectMapper;

    public MetadataCanonicalizer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Map<String, Object> canonicalize(Map<String, Object> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return Map.of();
        }

        var state = new TraversalState();
        Object canonical = canonicalizeValue(metadata, 1, state);
        try {
            if (objectMapper.writeValueAsBytes(canonical).length > MAX_ENCODED_BYTES) {
                throw invalidMetadata();
            }
        } catch (InvalidTransactionException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new InvalidTransactionException("metadata could not be encoded as canonical JSON");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> canonicalMap = (Map<String, Object>) canonical;
        return canonicalMap;
    }

    private Object canonicalizeValue(Object value, int depth, TraversalState state) {
        if (++state.nodes > MAX_NODES || depth > MAX_DEPTH) {
            throw invalidMetadata();
        }

        if (value instanceof Map<?, ?> map) {
            registerContainer(map, state);
            state.entries += map.size();
            if (state.entries > MAX_ENTRIES) {
                throw invalidMetadata();
            }

            var canonical = new TreeMap<String, Object>();
            for (var entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key)
                        || key.isBlank()
                        || utf8Length(key) > MAX_KEY_BYTES) {
                    throw invalidMetadata();
                }
                canonical.put(key, canonicalizeValue(entry.getValue(), depth + 1, state));
            }
            return Collections.unmodifiableMap(canonical);
        }

        if (value instanceof List<?> list) {
            registerContainer(list, state);
            state.entries += list.size();
            if (state.entries > MAX_ENTRIES) {
                throw invalidMetadata();
            }

            var canonical = new ArrayList<>(list.size());
            for (Object item : list) {
                canonical.add(canonicalizeValue(item, depth + 1, state));
            }
            return Collections.unmodifiableList(canonical);
        }

        if (value instanceof String text) {
            if (utf8Length(text) > MAX_STRING_BYTES) {
                throw invalidMetadata();
            }
            return text;
        }

        if (value == null || value instanceof Boolean) {
            return value;
        }

        if (value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long) {
            return ((Number) value).longValue();
        }

        if (value instanceof BigInteger integer) {
            try {
                return integer.longValueExact();
            } catch (ArithmeticException exception) {
                throw invalidMetadata();
            }
        }

        throw invalidMetadata();
    }

    private void registerContainer(Object container, TraversalState state) {
        if (state.visitedContainers.put(container, Boolean.TRUE) != null) {
            throw invalidMetadata();
        }
    }

    private int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private InvalidTransactionException invalidMetadata() {
        return new InvalidTransactionException(
                "metadata exceeds the permitted shape, integer, depth, or UTF-8 size policy"
        );
    }

    private static final class TraversalState {
        private final IdentityHashMap<Object, Boolean> visitedContainers = new IdentityHashMap<>();
        private int nodes;
        private int entries;
    }
}
