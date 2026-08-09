package io.github.tarunngusain08.payments.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class ValidMetadataValidator implements ConstraintValidator<ValidMetadata, Map<String, Object>> {

    static final int MAX_DEPTH = 5;
    static final int MAX_NODES = 200;
    static final int MAX_ENTRIES = 100;
    static final int MAX_KEY_BYTES = 64;
    static final int MAX_STRING_BYTES = 1_024;
    static final int MAX_ENCODED_BYTES = 16_384;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public boolean isValid(Map<String, Object> metadata, ConstraintValidatorContext context) {
        if (metadata == null) {
            return true;
        }

        var pending = new ArrayDeque<Node>();
        var visitedContainers = new IdentityHashMap<Object, Boolean>();
        pending.push(new Node(metadata, 1));
        int nodes = 0;
        int entries = 0;

        while (!pending.isEmpty()) {
            var node = pending.pop();
            if (++nodes > MAX_NODES || node.depth() > MAX_DEPTH) {
                return false;
            }

            Object value = node.value();
            if (value instanceof Map<?, ?> map) {
                if (visitedContainers.put(map, Boolean.TRUE) != null) {
                    return false;
                }
                entries += map.size();
                if (entries > MAX_ENTRIES) {
                    return false;
                }
                for (var entry : map.entrySet()) {
                    if (!(entry.getKey() instanceof String key)
                            || utf8Length(key) > MAX_KEY_BYTES) {
                        return false;
                    }
                    pending.push(new Node(entry.getValue(), node.depth() + 1));
                }
            } else if (value instanceof List<?> list) {
                if (visitedContainers.put(list, Boolean.TRUE) != null) {
                    return false;
                }
                entries += list.size();
                if (entries > MAX_ENTRIES) {
                    return false;
                }
                for (Object item : list) {
                    pending.push(new Node(item, node.depth() + 1));
                }
            } else if (value instanceof String text) {
                if (utf8Length(text) > MAX_STRING_BYTES) {
                    return false;
                }
            } else if (value != null
                    && !(value instanceof Number)
                    && !(value instanceof Boolean)) {
                return false;
            }
        }

        try {
            return OBJECT_MAPPER.writeValueAsBytes(metadata).length <= MAX_ENCODED_BYTES;
        } catch (RuntimeException exception) {
            return false;
        }
    }

    private int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private record Node(Object value, int depth) {
    }
}
