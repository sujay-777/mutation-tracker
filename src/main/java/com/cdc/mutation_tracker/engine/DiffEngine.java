package com.cdc.mutation_tracker.engine;

import com.cdc.mutation_tracker.config.SchemaTagConfig;
import com.cdc.mutation_tracker.exception.MalformedEventException;
import com.cdc.mutation_tracker.model.DebeziumEvent;
import com.cdc.mutation_tracker.model.DiffResult;
import com.cdc.mutation_tracker.model.FieldChange;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Component
@RequiredArgsConstructor
@Slf4j
public class DiffEngine {

    private final SchemaTagConfig schemaTagConfig;
    private final DebeziumTypeDecoder typeDecoder;

    public DiffResult compute(DebeziumEvent event) {
        if (event.getPayload() == null) {
            throw new MalformedEventException("Event has no payload");
        }

        DebeziumEvent.Payload payload = event.getPayload();
        String op = payload.getOp();
        String tableName = payload.getSource().getTable();

        if (op == null) {
            throw new MalformedEventException("op field missing");
        }

        List<FieldChange> changes = switch (op) {
            case "c" -> handleInsert(payload.getAfter(), tableName);
            case "u" -> handleUpdate(payload.getBefore(), payload.getAfter(), tableName);
            case "d" -> handleDelete(payload.getBefore(), tableName);
            default  -> { log.warn("Unknown op: {}", op); yield new ArrayList<>(); }
        };

        String rowId = extractRowId(payload.getAfter(), payload.getBefore());

        return DiffResult.builder()
                .tableName(tableName)
                .operation(mapOperation(op))
                .rowId(rowId)
                .changes(changes)
                .eventTimestamp(payload.getTs_ms())
                .build();
    }

    private List<FieldChange> handleInsert(JsonNode after, String tableName) {
        if (after == null || after.isNull()) {  // add isNull() check
            throw new MalformedEventException("INSERT has null after");
        }
        List<FieldChange> changes = new ArrayList<>();
        after.fieldNames().forEachRemaining(field -> {
            if (field.startsWith("__")) return;
            Object newValue = typeDecoder.decode(field, after.get(field));
            String tag = schemaTagConfig.getTag(tableName, field);
            changes.add(new FieldChange(field, null, newValue, tag));
        });
        return changes;
    }

    private List<FieldChange> handleUpdate(
            JsonNode before, JsonNode after, String tableName) {
        if (before == null || before.isNull() || after == null || after.isNull()) {
            throw new MalformedEventException(
                    "UPDATE missing before/after. " +
                            "Run: ALTER TABLE users REPLICA IDENTITY FULL"
            );
        }
        List<FieldChange> changes = new ArrayList<>();
        after.fieldNames().forEachRemaining(field -> {
            if (field.startsWith("__")) return;
            Object oldValue = typeDecoder.decode(field, before.get(field));
            Object newValue = typeDecoder.decode(field, after.get(field));
            if (Objects.equals(oldValue, newValue)) return;
            String tag = schemaTagConfig.getTag(tableName, field);
            changes.add(new FieldChange(field, oldValue, newValue, tag));
        });
        return changes;
    }

    private List<FieldChange> handleDelete(JsonNode before, String tableName) {
        if (before == null || before.isNull()) {  // add isNull() check
            throw new MalformedEventException("DELETE has null before");
        }
        List<FieldChange> changes = new ArrayList<>();
        before.fieldNames().forEachRemaining(field -> {
            if (field.startsWith("__")) return;
            Object oldValue = typeDecoder.decode(field, before.get(field));
            String tag = schemaTagConfig.getTag(tableName, field);
            changes.add(new FieldChange(field, oldValue, null, tag));
        });
        return changes;
    }

    private String extractRowId(JsonNode after, JsonNode before) {
        if (after != null && after.has("id")) return after.get("id").asText();
        if (before != null && before.has("id")) return before.get("id").asText();
        return "unknown";
    }

    private String mapOperation(String op) {
        return switch (op) {
            case "c" -> "INSERT";
            case "u" -> "UPDATE";
            case "d" -> "DELETE";
            default  -> op;
        };
    }
}