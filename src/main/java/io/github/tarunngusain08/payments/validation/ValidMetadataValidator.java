package io.github.tarunngusain08.payments.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import java.util.Map;

public class ValidMetadataValidator implements ConstraintValidator<ValidMetadata, Map<String, Object>> {

    static final int MAX_ENTRIES = MetadataCanonicalizer.MAX_ENTRIES;
    static final int MAX_STRING_BYTES = MetadataCanonicalizer.MAX_STRING_BYTES;
    private static final MetadataCanonicalizer CANONICALIZER =
            new MetadataCanonicalizer(new tools.jackson.databind.ObjectMapper());

    @Override
    public boolean isValid(Map<String, Object> metadata, ConstraintValidatorContext context) {
        if (metadata == null) {
            return true;
        }

        try {
            CANONICALIZER.canonicalize(metadata);
            return true;
        } catch (RuntimeException exception) {
            return false;
        }
    }
}
