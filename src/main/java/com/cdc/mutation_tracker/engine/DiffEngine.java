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

    /**
     * ENTRY POINT — called by CDCConsumer for every event.
     *
     * Takes a full Debezium event.
         * Returns a DiffResult with exactly what changed and why it matters.
     */
    public DiffResult compute(DebeziumEvent event) {

        // Validate event has payload
        if (event.getPayload() == null) {
            throw new MalformedEventException(
                    "Event has no payload"
            );
        }

        DebeziumEvent.Payload payload = event.getPayload();
        String op = payload.getOp();
        String tableName = payload.getSource().getTable();

        // Validate op exists
        if (op == null) {
            throw new MalformedEventException(
                    "Event has no operation type (op field missing)"
            );
        }

        // Find what changed based on operation type
        List<FieldChange> changes = switch (op) {
            case "c" -> handleInsert(payload.getAfter(), tableName);
            case "u" -> handleUpdate(
                    payload.getBefore(),
                    payload.getAfter(),
                    tableName
            );
            case "d" -> handleDelete(payload.getBefore(), tableName);
            default  -> {
                log.warn("Unknown op type: {}", op);
                yield new ArrayList<>();
            }
        };

        // Find row ID for audit log
        // Try after first, then before (DELETE has no after)
        String rowId = extractRowId(payload.getAfter(), payload.getBefore());

        return DiffResult.builder()
                .tableName(tableName)
                .operation(mapOperation(op))
                .rowId(rowId)
                .changes(changes)
                .eventTimestamp(payload.getTs_ms())
                .build();
    }

    /**
     * INSERT LOGIC
     * before = null, after has all new values
     * Every field in after is a "new" value
     */
    private List<FieldChange> handleInsert(
            JsonNode after,
            String tableName) {

        if (after == null) {
            throw new MalformedEventException(
                    "INSERT event has null after payload"
            );
        }

        List<FieldChange> changes = new ArrayList<>();

        after.fieldNames().forEachRemaining(fieldName -> {

            // Skip Debezium internal fields
            if (fieldName.startsWith("__")) return;

            Object newValue = typeDecoder.decode(
                    fieldName,
                    after.get(fieldName)
            );

            String tag = schemaTagConfig.getTag(tableName, fieldName);

            // before = null because this is an INSERT
            changes.add(new FieldChange(fieldName, null, newValue, tag));
        });

        return changes;
    }

    /**
     * UPDATE LOGIC
     * Both before and after exist
     * Compare field by field — only return fields that actually changed
     *
     * EDGE CASE: PostgreSQL sometimes fires WAL even when value
     * didn't change. Objects.equals() handles this —
     * if old == new, skip this field entirely.
     */
    private List<FieldChange> handleUpdate(
            JsonNode before,
            JsonNode after,
            String tableName) {

        if (before == null || after == null) {
            throw new MalformedEventException(
                    "UPDATE event missing before or after. " +
                            "Did you run ALTER TABLE x REPLICA IDENTITY FULL?"
            );
        }

        List<FieldChange> changes = new ArrayList<>();

        after.fieldNames().forEachRemaining(fieldName -> {

            if (fieldName.startsWith("__")) return;

            // Decode both values to comparable Java objects
            Object oldValue = typeDecoder.decode(
                    fieldName,
                    before.get(fieldName)
            );
            Object newValue = typeDecoder.decode(
                    fieldName,
                    after.get(fieldName)
            );

            // CRITICAL: Skip if values are the same
            // Without this you get false alerts on every update
            if (Objects.equals(oldValue, newValue)) return;

            String tag = schemaTagConfig.getTag(tableName, fieldName);

            changes.add(new FieldChange(
                    fieldName,
                    oldValue,
                    newValue,
                    tag
            ));
        });

        return changes;
    }

    /**
     * DELETE LOGIC
     * after = null, before has the deleted values
     * Every field in before is "gone"
     */
    private List<FieldChange> handleDelete(
            JsonNode before,
            String tableName) {

        if (before == null) {
            throw new MalformedEventException(
                    "DELETE event has null before payload"
            );
        }

        List<FieldChange> changes = new ArrayList<>();

        before.fieldNames().forEachRemaining(fieldName -> {

            if (fieldName.startsWith("__")) return;

            Object oldValue = typeDecoder.decode(
                    fieldName,
                    before.get(fieldName)
            );

            String tag = schemaTagConfig.getTag(tableName, fieldName);

            // after = null because this is a DELETE
            changes.add(new FieldChange(fieldName, oldValue, null, tag));
        });

        return changes;
    }

    // Extract primary key value for audit log
    private String extractRowId(JsonNode after, JsonNode before) {
        if (after != null && after.has("id")) {
            return after.get("id").asText();
        }
        if (before != null && before.has("id")) {
            return before.get("id").asText();
        }
        return "unknown";
    }

    // Convert single char op to readable string
    private String mapOperation(String op) {
        return switch (op) {
            case "c" -> "INSERT";
            case "u" -> "UPDATE";
            case "d" -> "DELETE";
            default  -> op;
        };
    }
}