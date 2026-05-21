package com.cdc.mutation_tracker.audit;

import com.cdc.mutation_tracker.model.AuditLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AuditLogRepository extends JpaRepository<AuditLog, Long> {

    // Get all audit logs for a specific table
    // Used by the impact graph endpoint later
    List<AuditLog> findByTableNameOrderByCreatedAtDesc(String tableName);

    // Get all audit logs for a specific row
    List<AuditLog> findByTableNameAndRowIdOrderByCreatedAtDesc(
            String tableName,
            String rowId
    );

    // Get all PII related changes
    List<AuditLog> findByTagsContainingOrderByCreatedAtDesc(String tag);
}