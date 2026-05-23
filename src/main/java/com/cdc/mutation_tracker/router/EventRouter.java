package com.cdc.mutation_tracker.router;

import com.cdc.mutation_tracker.audit.AuditLogService;
import com.cdc.mutation_tracker.cache.CacheInvalidator;
import com.cdc.mutation_tracker.model.DiffResult;
import com.cdc.mutation_tracker.model.FieldChange;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Slf4j
public class EventRouter {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AuditLogService auditLogService;
    private final CacheInvalidator cacheInvalidator;
    private final ObjectMapper objectMapper;

    // Prometheus counters — visible in Grafana
    private final Counter piiAlertsCounter;
    private final Counter financialAlertsCounter;

    public EventRouter(
            KafkaTemplate<String, String> kafkaTemplate,
            AuditLogService auditLogService,
            CacheInvalidator cacheInvalidator,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {

        this.kafkaTemplate = kafkaTemplate;
        this.auditLogService = auditLogService;
        this.cacheInvalidator = cacheInvalidator;
        this.objectMapper = objectMapper;

        this.piiAlertsCounter = Counter.builder("router.pii.alerts")
                .description("total PII change alerts fired")
                .register(meterRegistry);

        this.financialAlertsCounter = Counter.builder("router.financial.alerts")
                .description("total financial change alerts fired")
                .register(meterRegistry);
    }

    // entry point — called by CDCConsumer after diff is computed
    public void route(DiffResult diff) {

        // ROUTE 1: PII changes → privacy-alerts topic
        // example: email, name, address changed
        if (diff.hasPiiChanges()) {
            publish("privacy-alerts", diff, "pii");
            piiAlertsCounter.increment();
        }

        // ROUTE 2: financial changes → financial-audit topic
        // example: balance, amount changed
        if (diff.hasFinancialChanges()) {
            publish("financial-audit", diff, "financial");
            financialAlertsCounter.increment();
        }

        // ROUTE 3: every single change → audit log
        // always runs regardless of what tag the change has
        // saves human readable log to PostgreSQL via Groq API
        auditLogService.createAuditLog(diff);

        // ROUTE 4: every single change → Redis cache invalidation
        // deletes cached row so next read gets fresh data from DB
        cacheInvalidator.invalidate(diff.getTableName(), diff.getRowId());
    }

    private void publish(String topic, DiffResult diff, String tagFilter) {
        try {
            // filter only relevant changes for this topic
            // e.g. privacy-alerts only gets pii tagged changes
            List<String> relevantChanges = diff.getChangesByTag(tagFilter)
                    .stream()
                    .map(FieldChange::toHumanReadable)
                    .collect(Collectors.toList());

            // build payload to send to Kafka topic
            Map<String, Object> payload = Map.of(
                    "tableName",  diff.getTableName(),
                    "rowId",      diff.getRowId(),
                    "operation",  diff.getOperation(),
                    "timestamp",  diff.getEventTimestamp(),
                    "changes",    relevantChanges
            );

            String json = objectMapper.writeValueAsString(payload);

            // rowId as Kafka key = same row always goes to same partition
            // this guarantees ordering of changes for the same row
            kafkaTemplate.send(topic, diff.getRowId(), json);

            log.info("Published to {}: table={} rowId={}",
                    topic, diff.getTableName(), diff.getRowId());

        } catch (Exception e) {
            // publishing failed — log but never stop the pipeline
            // audit log still gets saved even if Kafka publish fails
            log.error("Failed to publish to topic {}: {}",
                    topic, e.getMessage());
        }
    }
}
