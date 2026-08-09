package io.github.tarunngusain08.payments.config;

import org.springframework.boot.jackson.autoconfigure.JsonFactoryBuilderCustomizer;
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.databind.cfg.CoercionAction;
import tools.jackson.databind.cfg.CoercionInputShape;
import tools.jackson.databind.type.LogicalType;

@Configuration
public class JsonConfiguration {

    @Bean
    JsonMapperBuilderCustomizer strictEnumInput() {
        return builder -> builder.withCoercionConfig(LogicalType.Enum, coercion -> {
            coercion.setCoercion(CoercionInputShape.Integer, CoercionAction.Fail);
            coercion.setCoercion(CoercionInputShape.Float, CoercionAction.Fail);
        });
    }

    @Bean
    JsonFactoryBuilderCustomizer boundedJsonInput() {
        return builder -> builder.streamReadConstraints(StreamReadConstraints.builder()
                .maxNestingDepth(16)
                .maxDocumentLength(65_536)
                .maxTokenCount(1_000)
                .maxNumberLength(64)
                .maxStringLength(16_384)
                .maxNameLength(128)
                .build());
    }
}
