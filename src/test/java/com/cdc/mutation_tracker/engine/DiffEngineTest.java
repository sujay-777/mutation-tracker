package com.cdc.mutation_tracker.engine;

import com.cdc.mutation_tracker.config.SchemaTagConfig;
import com.cdc.mutation_tracker.exception.MalformedEventException;
import com.cdc.mutation_tracker.model.DebeziumEvent;
import com.cdc.mutation_tracker.model.DiffResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@org.mockito.junit.jupiter.MockitoSettings(strictness = org.mockito.quality.Strictness.LENIENT)
class DiffEngineTest {

    @Mock
    private SchemaTagConfig schemaTagConfig;

    @Mock
    private DebeziumTypeDecoder typeDecoder;

    @InjectMocks
    private DiffEngine diffEngine;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        // default tag for any field
        when(schemaTagConfig.getTag(anyString(), anyString()))
                .thenReturn("untagged");
    }

    // ── INSERT ──────────────────────────────────────────────

    @Test
    void insert_shouldCaptureAllFieldsAsNew() throws Exception {
        // before is null for INSERT
        // after has all field values
        String raw = """
            {
              "payload": {
                "before": null,
                "after": {"id": 1, "email": "arjun@gmail.com", "balance": 5000},
                "op": "c",
                "source": {"table": "users", "db": "testdb", "schema": "public"},
                "ts_ms": 1000
              }
            }
            """;

        DebeziumEvent event = objectMapper.readValue(raw, DebeziumEvent.class);

        when(typeDecoder.decode(eq("id"), any())).thenReturn(1);
        when(typeDecoder.decode(eq("email"), any())).thenReturn("arjun@gmail.com");
        when(typeDecoder.decode(eq("balance"), any())).thenReturn(5000);

        DiffResult result = diffEngine.compute(event);

        assertEquals("INSERT", result.getOperation());
        assertEquals("users", result.getTableName());
        assertEquals("1", result.getRowId());
        assertFalse(result.isEmpty());
        assertEquals(3, result.getChanges().size());

        // for INSERT oldValue must be null for every field
        result.getChanges().forEach(change ->
                assertNull(change.getOldValue())
        );
    }

    // ── UPDATE ──────────────────────────────────────────────

    @Test
    void update_shouldOnlyCaptureChangedFields() throws Exception {
        // only email changed, name and balance stayed the same
        String raw = """
            {
              "payload": {
                "before": {"id": 1, "name": "Arjun", "email": "old@gmail.com", "balance": 5000},
                "after":  {"id": 1, "name": "Arjun", "email": "new@gmail.com", "balance": 5000},
                "op": "u",
                "source": {"table": "users", "db": "testdb", "schema": "public"},
                "ts_ms": 1000
              }
            }
            """;

        DebeziumEvent event = objectMapper.readValue(raw, DebeziumEvent.class);

        when(typeDecoder.decode(eq("id"), any())).thenReturn(1);
        when(typeDecoder.decode(eq("name"), any())).thenReturn("Arjun");
        when(typeDecoder.decode(eq("email"), any()))
                .thenReturn("old@gmail.com")  // before
                .thenReturn("new@gmail.com"); // after
        when(typeDecoder.decode(eq("balance"), any())).thenReturn(5000);

        DiffResult result = diffEngine.compute(event);

        assertEquals("UPDATE", result.getOperation());
        // only email changed — id, name, balance skipped
        assertEquals(1, result.getChanges().size());
        assertEquals("email", result.getChanges().get(0).getFieldName());
        assertEquals("old@gmail.com", result.getChanges().get(0).getOldValue());
        assertEquals("new@gmail.com", result.getChanges().get(0).getNewValue());
    }

    @Test
    void update_sameValue_shouldReturnEmptyDiff() throws Exception {
        // nothing actually changed — no-op update
        // PostgreSQL still fires WAL, we must skip it
        String raw = """
            {
              "payload": {
                "before": {"id": 1, "email": "same@gmail.com"},
                "after":  {"id": 1, "email": "same@gmail.com"},
                "op": "u",
                "source": {"table": "users", "db": "testdb", "schema": "public"},
                "ts_ms": 1000
              }
            }
            """;

        DebeziumEvent event = objectMapper.readValue(raw, DebeziumEvent.class);

        when(typeDecoder.decode(anyString(), any())).thenReturn("same@gmail.com");

        DiffResult result = diffEngine.compute(event);

        // diff must be empty — same value update should be ignored
        assertTrue(result.isEmpty());
    }

    // ── DELETE ──────────────────────────────────────────────

    @Test
    void delete_shouldCaptureAllFieldsAsRemoved() throws Exception {
        // after is null for DELETE
        // before has the deleted row values
        String raw = """
            {
              "payload": {
                "before": {"id": 1, "email": "arjun@gmail.com", "balance": 5000},
                "after": null,
                "op": "d",
                "source": {"table": "users", "db": "testdb", "schema": "public"},
                "ts_ms": 1000
              }
            }
            """;

        DebeziumEvent event = objectMapper.readValue(raw, DebeziumEvent.class);

        when(typeDecoder.decode(eq("id"), any())).thenReturn(1);
        when(typeDecoder.decode(eq("email"), any())).thenReturn("arjun@gmail.com");
        when(typeDecoder.decode(eq("balance"), any())).thenReturn(5000);

        DiffResult result = diffEngine.compute(event);

        assertEquals("DELETE", result.getOperation());
        assertFalse(result.isEmpty());

        // for DELETE newValue must be null for every field
        result.getChanges().forEach(change ->
                assertNull(change.getNewValue())
        );
    }

    // ── SCHEMA TAGS ─────────────────────────────────────────

    @Test
    void update_shouldTagFieldsCorrectly() throws Exception {
        String raw = """
            {
              "payload": {
                "before": {"id": 1, "email": "old@gmail.com", "balance": 1000},
                "after":  {"id": 1, "email": "new@gmail.com", "balance": 9999},
                "op": "u",
                "source": {"table": "users", "db": "testdb", "schema": "public"},
                "ts_ms": 1000
              }
            }
            """;

        DebeziumEvent event = objectMapper.readValue(raw, DebeziumEvent.class);

        when(typeDecoder.decode(eq("id"), any())).thenReturn(1);
        when(typeDecoder.decode(eq("email"), any()))
                .thenReturn("old@gmail.com")
                .thenReturn("new@gmail.com");
        when(typeDecoder.decode(eq("balance"), any()))
                .thenReturn(1000)
                .thenReturn(9999);

        // email is pii, balance is financial
        when(schemaTagConfig.getTag("users", "email")).thenReturn("pii");
        when(schemaTagConfig.getTag("users", "balance")).thenReturn("financial");
        when(schemaTagConfig.getTag("users", "id")).thenReturn("untagged");

        DiffResult result = diffEngine.compute(event);

        assertTrue(result.hasPiiChanges());
        assertTrue(result.hasFinancialChanges());

        result.getChanges().forEach(change -> {
            if ("email".equals(change.getFieldName())) {
                assertEquals("pii", change.getTag());
            }
            if ("balance".equals(change.getFieldName())) {
                assertEquals("financial", change.getTag());
            }
        });
    }

    // ── EDGE CASES ───────────────────────────────────────────

    @Test
    void nullPayload_shouldThrowMalformedEventException() {
        DebeziumEvent event = new DebeziumEvent();
        event.setPayload(null);

        assertThrows(MalformedEventException.class,
                () -> diffEngine.compute(event));
    }

    @Test
    void insert_nullAfter_shouldThrowMalformedEventException() throws Exception {
        String raw = """
            {
              "payload": {
                "before": null,
                "after": null,
                "op": "c",
                "source": {"table": "users", "db": "testdb", "schema": "public"},
                "ts_ms": 1000
              }
            }
            """;

        DebeziumEvent event = objectMapper.readValue(raw, DebeziumEvent.class);

        assertThrows(MalformedEventException.class,
                () -> diffEngine.compute(event));
    }

    @Test
    void delete_nullBefore_shouldThrowMalformedEventException() throws Exception {
        String raw = """
            {
              "payload": {
                "before": null,
                "after": null,
                "op": "d",
                "source": {"table": "users", "db": "testdb", "schema": "public"},
                "ts_ms": 1000
              }
            }
            """;

        DebeziumEvent event = objectMapper.readValue(raw, DebeziumEvent.class);

        assertThrows(MalformedEventException.class,
                () -> diffEngine.compute(event));
    }

    @Test
    void snapshotRead_shouldReturnEmptyChanges() throws Exception {
        // op=r is snapshot read on Debezium startup
        // should be ignored in CDCConsumer but DiffEngine
        // returns empty list for unknown ops
        String raw = """
            {
              "payload": {
                "before": null,
                "after": {"id": 1, "email": "arjun@gmail.com"},
                "op": "r",
                "source": {"table": "users", "db": "testdb", "schema": "public"},
                "ts_ms": 1000
              }
            }
            """;

        DebeziumEvent event = objectMapper.readValue(raw, DebeziumEvent.class);

        DiffResult result = diffEngine.compute(event);

        assertTrue(result.isEmpty());
    }
}