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

    public Object decode(String fieldName, JsonNode node) {

        // CASE 1: null field — return null
        // Happens for INSERT (before is null)
        // and DELETE (after is null)
        if (node == null || node.isNull()) {
            return null;
        }

        // CASE 2: Normal string — return as is
        // Example: email, name, status fields
        if (node.isTextual()) {
            return node.asText();
        }

        // CASE 3: Normal number — return as is
        // Example: id field (integer)
        if (node.isNumber()) {
            return node.numberValue();
        }

        // CASE 4: Boolean — return as is
        if (node.isBoolean()) {
            return node.booleanValue();
        }

        // CASE 5: Debezium VariableScaleDecimal
        // This is how Debezium encodes PostgreSQL NUMERIC columns
        // Structure: { "scale": 0, "value": "E4g=" }
        // value is Base64 encoded bytes of the number
        if (node.isObject()
                && node.has("scale")
                && node.has("value")) {

            return decodeVariableScaleDecimal(fieldName, node);
        }

        // CASE 6: Any other object — convert to string
        // Handles edge cases we haven't seen yet
        // Don't crash the pipeline over unknown types
        return node.toString();
    }

    private Object decodeVariableScaleDecimal(
            String fieldName,
            JsonNode node) {
        try {
            // Step 1: Get Base64 string from "value" field
            String base64Value = node.get("value").asText();

            // Step 2: Decode Base64 → raw bytes
            byte[] bytes = Base64.getDecoder().decode(base64Value);

            // Step 3: Convert bytes → BigInteger (unscaled number)
            BigInteger unscaled = new BigInteger(bytes);

            // Step 4: Apply scale to get actual decimal
            // Example: unscaled=5000, scale=0 → 5000
            // Example: unscaled=5000, scale=2 → 50.00
            int scale = node.get("scale").asInt();

            return new BigDecimal(unscaled, scale);

        } catch (Exception e) {
            // Decoding failed — return raw string
            // Log warning but never crash the pipeline
            log.warn("Failed to decode VariableScaleDecimal " +
                    "for field '{}': {}", fieldName, e.getMessage());
            return node.toString();
        }
    }
}
