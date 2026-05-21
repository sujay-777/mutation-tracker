package com.cdc.mutation_tracker.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.stream.Collectors;

@Data
@Builder
public class DiffResult {

    private String tableName;
    private String operation;      // INSERT / UPDATE / DELETE
    private String rowId;          // primary key value
    private List<FieldChange> changes;
    private Long eventTimestamp;

    // True if nothing actually changed
    // Happens with same-value updates in PostgreSQL
    public boolean isEmpty() {
        return changes == null || changes.isEmpty();
    }

    // Get changes filtered by tag
    public List<FieldChange> getChangesByTag(String tag) {
        if (changes == null) return List.of();
        return changes.stream()
                .filter(c -> tag.equals(c.getTag()))
                .collect(Collectors.toList());
    }

    public boolean hasPiiChanges() {
        return !getChangesByTag("pii").isEmpty();
    }

    public boolean hasFinancialChanges() {
        return !getChangesByTag("financial").isEmpty();
    }
}