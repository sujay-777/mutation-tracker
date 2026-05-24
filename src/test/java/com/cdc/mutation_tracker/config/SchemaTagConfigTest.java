package com.cdc.mutation_tracker.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SchemaTagConfigTest {

    private SchemaTagConfig schemaTagConfig;

    @BeforeEach
    void setUp() {
        schemaTagConfig = new SchemaTagConfig();

        // build the same structure as schema-tags.yml
        Map<String, String> userColumns = new HashMap<>();
        userColumns.put("email", "pii");
        userColumns.put("name", "pii");
        userColumns.put("balance", "financial");

        SchemaTagConfig.TableConfig userTableConfig =
                new SchemaTagConfig.TableConfig();
        userTableConfig.setColumns(userColumns);

        Map<String, SchemaTagConfig.TableConfig> tables = new HashMap<>();
        tables.put("users", userTableConfig);

        schemaTagConfig.setTables(tables);
    }

    @Test
    void knownTable_knownPiiField_shouldReturnPii() {
        assertEquals("pii", schemaTagConfig.getTag("users", "email"));
    }

    @Test
    void knownTable_knownFinancialField_shouldReturnFinancial() {
        assertEquals("financial", schemaTagConfig.getTag("users", "balance"));
    }

    @Test
    void knownTable_unknownField_shouldReturnUntagged() {
        // id is not in schema-tags.yml
        // should return untagged not throw exception
        assertEquals("untagged", schemaTagConfig.getTag("users", "id"));
    }

    @Test
    void unknownTable_shouldReturnUntagged() {
        // payments table not configured
        // should return untagged not throw exception
        assertEquals("untagged", schemaTagConfig.getTag("payments", "amount"));
    }

    @Test
    void nullTables_shouldReturnUntagged() {
        // tables not loaded at all
        SchemaTagConfig emptyConfig = new SchemaTagConfig();
        emptyConfig.setTables(null);

        assertEquals("untagged", emptyConfig.getTag("users", "email"));
    }

    @Test
    void multipleFields_shouldReturnCorrectTagForEach() {
        assertEquals("pii",       schemaTagConfig.getTag("users", "email"));
        assertEquals("pii",       schemaTagConfig.getTag("users", "name"));
        assertEquals("financial", schemaTagConfig.getTag("users", "balance"));
        assertEquals("untagged",  schemaTagConfig.getTag("users", "id"));
    }
}