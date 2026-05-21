package com.cdc.mutation_tracker.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "schema-tags")
@Data
public class SchemaTagConfig {

    // Spring automatically maps schema-tags.yml into this
    // structure: tableName → columnName → tag
    // Example: tables["users"]["email"] = "pii"
    private Map<String, TableConfig> tables;

    @Data
    public static class TableConfig {
        // columnName → tag
        // Example: columns["email"] = "pii"
        private Map<String, String> columns;
    }

    /**
     * MAIN METHOD your diff engine calls.
     *
     * Example calls:
     * getTag("users", "email")   → "pii"
     * getTag("users", "balance") → "financial"
     * getTag("users", "id")      → "untagged"
     */
    public String getTag(String tableName, String fieldName) {

        // Table not in config at all
        if (tables == null) return "untagged";

        TableConfig tableConfig = tables.get(tableName);
        if (tableConfig == null) return "untagged";

        // Field not tagged — still process it, just mark untagged
        return tableConfig.getColumns()
                .getOrDefault(fieldName, "untagged");
    }
}