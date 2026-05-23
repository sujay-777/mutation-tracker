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

    // entry point — called by CDCConsumer for every event
    public DiffResult compute(DebeziumEvent event) {

        if (event.getPayload() == null) {
            throw new MalformedEventException("Event has no payload");
        }

        DebeziumEvent.Payload payload = event.getPayload();
        String op = payload.getOp();
        String tableName = payload.getSource().getTable();

        if (op == null) {
            throw new MalformedEventException("op field is missing");
        }

        // route to correct handler based on operation type
        List<FieldChange> changes = switch (op) {
            case "c" -> handleInsert(payload.getAfter(), tableName);
            case "u" -> handleUpdate(payload.getBefore(), payload.getAfter(), tableName);
            case "d" -> handleDelete(payload.getBefore(), tableName);
            default  -> {
                log.warn("Unknown op: {}", op);
                yield new ArrayList<>();
            }
        };

        // extract row ID — try after first, then before
        String rowId = extractRowId(payload.getAfter(), payload.getBefore());

        return DiffResult.builder()
                .tableName(tableName)
                .operation(mapOperation(op))
                .rowId(rowId)
                .changes(changes)
                .eventTimestamp(payload.getTs_ms())
                .build();
    }

    // INSERT — before is null, every field in after is new
    private List<FieldChange> handleInsert(JsonNode after, String tableName) {
        if (after == null) {
            throw new MalformedEventException("INSERT has null after");
        }

        List<FieldChange> changes = new ArrayList<>();
        after.fieldNames().forEachRemaining(field -> {
            if (field.startsWith("__")) return; // skip debezium internals

            Object newValue = typeDecoder.decode(field, after.get(field));
            String tag = schemaTagConfig.getTag(tableName, field);

            changes.add(new FieldChange(field, null, newValue, tag));
        });
        return changes;
    }

    // UPDATE — compare every field, skip unchanged ones
    private List<FieldChange> handleUpdate(
            JsonNode before, JsonNode after, String tableName) {

        // before being null here means REPLICA IDENTITY FULL is missing
        if (before == null || after == null) {
            throw new MalformedEventException(
                    "UPDATE missing before/after. " +
                            "Run: ALTER TABLE x REPLICA IDENTITY FULL"
            );
        }

        List<FieldChange> changes = new ArrayList<>();
        after.fieldNames().forEachRemaining(field -> {
            if (field.startsWith("__")) return;

            Object oldValue = typeDecoder.decode(field, before.get(field));
            Object newValue = typeDecoder.decode(field, after.get(field));

            // CRITICAL — skip if same value, avoids false alerts
            if (Objects.equals(oldValue, newValue)) return;

            String tag = schemaTagConfig.getTag(tableName, field);
            changes.add(new FieldChange(field, oldValue, newValue, tag));
        });
        return changes;
    }

    // DELETE — after is null, everything in before is gone
    private List<FieldChange> handleDelete(JsonNode before, String tableName) {
        if (before == null) {
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