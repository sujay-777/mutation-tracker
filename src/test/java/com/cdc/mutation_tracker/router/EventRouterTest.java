package com.cdc.mutation_tracker.router;

import com.cdc.mutation_tracker.audit.AuditLogService;
import com.cdc.mutation_tracker.cache.CacheInvalidator;
import com.cdc.mutation_tracker.model.DiffResult;
import com.cdc.mutation_tracker.model.FieldChange;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventRouterTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private AuditLogService auditLogService;

    @Mock
    private CacheInvalidator cacheInvalidator;

    private EventRouter eventRouter;

    @BeforeEach
    void setUp() {
        eventRouter = new EventRouter(
                kafkaTemplate,
                auditLogService,
                cacheInvalidator,
                new ObjectMapper(),
                new SimpleMeterRegistry()
        );
    }

    @Test
    void piiChange_shouldPublishToPrivacyAlerts() {
        DiffResult diff = DiffResult.builder()
                .tableName("users")
                .rowId("1")
                .operation("UPDATE")
                .eventTimestamp(1000L)
                .changes(List.of(
                        new FieldChange("email", "old@gmail.com", "new@gmail.com", "pii")
                ))
                .build();

        eventRouter.route(diff);

        // privacy-alerts topic must be called
        verify(kafkaTemplate).send(eq("privacy-alerts"), eq("1"), anyString());
        // financial-audit must NOT be called
        verify(kafkaTemplate, never()).send(eq("financial-audit"), anyString(), anyString());
    }

    @Test
    void financialChange_shouldPublishToFinancialAudit() {
        DiffResult diff = DiffResult.builder()
                .tableName("users")
                .rowId("2")
                .operation("UPDATE")
                .eventTimestamp(1000L)
                .changes(List.of(
                        new FieldChange("balance", 1000, 9999, "financial")
                ))
                .build();

        eventRouter.route(diff);

        // financial-audit topic must be called
        verify(kafkaTemplate).send(eq("financial-audit"), eq("2"), anyString());
        // privacy-alerts must NOT be called
        verify(kafkaTemplate, never()).send(eq("privacy-alerts"), anyString(), anyString());
    }

    @Test
    void bothPiiAndFinancialChange_shouldPublishToBothTopics() {
        DiffResult diff = DiffResult.builder()
                .tableName("users")
                .rowId("3")
                .operation("UPDATE")
                .eventTimestamp(1000L)
                .changes(List.of(
                        new FieldChange("email", "old@gmail.com", "new@gmail.com", "pii"),
                        new FieldChange("balance", 1000, 9999, "financial")
                ))
                .build();

        eventRouter.route(diff);

        verify(kafkaTemplate).send(eq("privacy-alerts"), eq("3"), anyString());
        verify(kafkaTemplate).send(eq("financial-audit"), eq("3"), anyString());
    }

    @Test
    void operationalChange_shouldNotPublishToEitherTopic() {
        DiffResult diff = DiffResult.builder()
                .tableName("orders")
                .rowId("4")
                .operation("UPDATE")
                .eventTimestamp(1000L)
                .changes(List.of(
                        new FieldChange("status", "pending", "shipped", "operational")
                ))
                .build();

        eventRouter.route(diff);

        // no Kafka publish for operational changes
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void anyChange_shouldAlwaysCreateAuditLog() {
        DiffResult diff = DiffResult.builder()
                .tableName("users")
                .rowId("5")
                .operation("INSERT")
                .eventTimestamp(1000L)
                .changes(List.of(
                        new FieldChange("email", null, "new@gmail.com", "pii")
                ))
                .build();

        eventRouter.route(diff);

        // audit log always created regardless of tag
        verify(auditLogService).createAuditLog(diff);
    }

    @Test
    void anyChange_shouldAlwaysInvalidateCache() {
        DiffResult diff = DiffResult.builder()
                .tableName("users")
                .rowId("6")
                .operation("UPDATE")
                .eventTimestamp(1000L)
                .changes(List.of(
                        new FieldChange("status", "old", "new", "operational")
                ))
                .build();

        eventRouter.route(diff);

        // cache always invalidated regardless of tag
        verify(cacheInvalidator).invalidate("users", "6");
    }

    @Test
    void kafkaPublishFails_shouldNotStopAuditLogOrCacheInvalidation() {
        // if Kafka publish fails, audit log and cache must still happen
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenThrow(new RuntimeException("Kafka down"));

        DiffResult diff = DiffResult.builder()
                .tableName("users")
                .rowId("7")
                .operation("UPDATE")
                .eventTimestamp(1000L)
                .changes(List.of(
                        new FieldChange("email", "old@gmail.com", "new@gmail.com", "pii")
                ))
                .build();

        // should not throw
        eventRouter.route(diff);

        // audit log and cache still happen even when Kafka is down
        verify(auditLogService).createAuditLog(diff);
        verify(cacheInvalidator).invalidate("users", "7");
    }
}