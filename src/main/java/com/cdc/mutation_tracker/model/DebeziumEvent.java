package com.cdc.mutation_tracker.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumEvent {

    private Payload payload;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Payload {

        // Row state BEFORE the change
        // NULL for INSERT — nothing existed before
        private JsonNode before;

        // Row state AFTER the change
        // NULL for DELETE — nothing exists after
        private JsonNode after;

        // c = insert, u = update, d = delete, r = snapshot
        private String op;

        // Which table and database this came from
        private Source source;

        // When this change happened (milliseconds)
        private Long ts_ms;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Source {
        private String table;
        private String db;
        private String schema;
        // WAL position when this change happened
        private Long lsn;
    }
}