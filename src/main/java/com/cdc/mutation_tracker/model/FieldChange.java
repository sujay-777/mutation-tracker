package com.cdc.mutation_tracker.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FieldChange {

    // Which field changed
    private String fieldName;

    // What it was before (null for INSERT)
    private Object oldValue;

    // What it is now (null for DELETE)
    private Object newValue;

    // Tag from schema-tags.yml
    // pii / financial / operational / untagged
    private String tag;

    // Used by AuditLogService to build the prompt for Groq
    public String toHumanReadable() {
        if (oldValue == null) {
            return String.format(
                    "'%s' was set to '%s'",
                    fieldName, newValue
            );
        }
        if (newValue == null) {
            return String.format(
                    "'%s' was removed (was '%s')",
                    fieldName, oldValue
            );
        }
        return String.format(
                "'%s' changed from '%s' to '%s'",
                fieldName, oldValue, newValue
        );
    }
}