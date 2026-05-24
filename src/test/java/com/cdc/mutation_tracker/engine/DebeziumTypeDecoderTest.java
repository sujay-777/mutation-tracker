package com.cdc.mutation_tracker.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class DebeziumTypeDecoderTest {

    private DebeziumTypeDecoder decoder;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        decoder = new DebeziumTypeDecoder();
        objectMapper = new ObjectMapper();
    }

    @Test
    void nullNode_shouldReturnNull() throws Exception {
        JsonNode node = objectMapper.readTree("null");
        assertNull(decoder.decode("field", node));
    }

    @Test
    void nullReference_shouldReturnNull() {
        assertNull(decoder.decode("field", null));
    }

    @Test
    void stringNode_shouldReturnString() throws Exception {
        JsonNode node = objectMapper.readTree("\"arjun@gmail.com\"");
        assertEquals("arjun@gmail.com", decoder.decode("email", node));
    }

    @Test
    void integerNode_shouldReturnNumber() throws Exception {
        JsonNode node = objectMapper.readTree("42");
        assertEquals(42, ((Number) decoder.decode("id", node)).intValue());
    }

    @Test
    void booleanNode_shouldReturnBoolean() throws Exception {
        JsonNode node = objectMapper.readTree("true");
        assertEquals(true, decoder.decode("active", node));
    }

    @Test
    void variableScaleDecimal_shouldDecodeToBigDecimal() throws Exception {
        // This is how Debezium encodes PostgreSQL NUMERIC columns
        // scale=0, value=Base64 encoded bytes of the number 5000
        JsonNode node = objectMapper.readTree(
                "{\"scale\": 0, \"value\": \"E4g=\"}"
        );

        Object result = decoder.decode("balance", node);

        assertNotNull(result);
        assertInstanceOf(BigDecimal.class, result);
    }

    @Test
    void variableScaleDecimal_sameValue_shouldBeEqual() throws Exception {
        // Two identical balances must decode to equal values
        // This is critical — without this diff engine fires false alerts
        JsonNode node1 = objectMapper.readTree(
                "{\"scale\": 0, \"value\": \"E4g=\"}"
        );
        JsonNode node2 = objectMapper.readTree(
                "{\"scale\": 0, \"value\": \"E4g=\"}"
        );

        Object result1 = decoder.decode("balance", node1);
        Object result2 = decoder.decode("balance", node2);

        assertEquals(result1, result2);
    }

    @Test
    void invalidBase64_shouldReturnRawString() throws Exception {
        // If Base64 decoding fails, return raw string
        // Never crash the pipeline over a type issue
        JsonNode node = objectMapper.readTree(
                "{\"scale\": 0, \"value\": \"!!!invalid!!!\"}"
        );

        Object result = decoder.decode("balance", node);

        assertNotNull(result);
        // should not throw, should return something
        assertInstanceOf(String.class, result);
    }

    @Test
    void unknownObjectNode_shouldReturnRawString() throws Exception {
        // Any unknown object type returns toString
        JsonNode node = objectMapper.readTree(
                "{\"unknown\": \"structure\"}"
        );

        Object result = decoder.decode("field", node);

        assertNotNull(result);
        assertInstanceOf(String.class, result);
    }
}