package com.cdc.mutation_tracker.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "audit_log")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "table_name")
    private String tableName;

    @Column(name = "row_id")
    private String rowId;

    // INSERT / UPDATE / DELETE
    @Column(name = "operation")
    private String operation;

    // JSON of what changed — stored as jsonb in PostgreSQL
    @Column(name = "changed_fields", columnDefinition = "jsonb")
    private String changedFields;

    // Groq generated human readable description
    @Column(name = "human_readable_log", columnDefinition = "TEXT")
    private String humanReadableLog;

    // Comma separated tags: pii,financial
    @Column(name = "tags")
    private String tags;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    // Automatically set timestamp before saving
    @PrePersist
    public void prePersist() {
        this.createdAt = LocalDateTime.now();
    }
}
