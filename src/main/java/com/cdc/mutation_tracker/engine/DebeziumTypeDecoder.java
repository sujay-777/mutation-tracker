package com.cdc.mutation_tracker.engine;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;

@Component
@Slf4j
public class DebeziumTypeDecoder {

    // called before every field comparison in DiffEngine
    public Object decode(String fieldName, JsonNode node) {

        // null field — happens for INSERT (before) and DELETE (after)
        if (node == null || node.isNull()) return null;

        // normal string — email, name, status
        if (node.isTextual()) return node.asText();

        // normal number — id field
        if (node.isNumber()) return node.numberValue();

        // boolean field
        if (node.isBoolean()) return node.booleanValue();

        // Debezium NUMERIC type — { "scale": 0, "value": "E4g=" }
        // must decode Base64 otherwise false alerts fire constantly
        if (node.isObject() && node.has("scale") && node.has("value")) {
            return decodeNumeric(fieldName, node);
        }

        // anything else — return as raw string, never crash pipeline
        return node.toString();
    }

    private Object decodeNumeric(String fieldName, JsonNode node) {
        try {
            // Base64 string → raw bytes → BigInteger → BigDecimal
            byte[] bytes = Base64.getDecoder()
                    .decode(node.get("value").asText());
            BigInteger unscaled = new BigInteger(bytes);
            int scale = node.get("scale").asInt();
            return new BigDecimal(unscaled, scale);

        } catch (Exception e) {
            // decoding failed — log and return raw string
            // pipeline must never stop over a type issue
            log.warn("Could not decode NUMERIC field '{}': {}",
                    fieldName, e.getMessage());
            return node.toString();
        }
    }
}
