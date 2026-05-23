package com.cdc.mutation_tracker.audit;

import com.cdc.mutation_tracker.model.AuditLog;
import com.cdc.mutation_tracker.model.DiffResult;
import com.cdc.mutation_tracker.model.FieldChange;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AuditLogService {

    private final AuditLogRepository auditLogRepository;
    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final Counter auditLogCounter;

    @Value("${groq.api.key}")
    private String groqApiKey;

    @Value("${groq.api.url}")
    private String groqApiUrl;

    @Value("${groq.api.model}")
    private String groqModel;

    public AuditLogService(
            AuditLogRepository auditLogRepository,
            WebClient.Builder webClientBuilder,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {

        this.auditLogRepository = auditLogRepository;
        this.webClient = webClientBuilder.build();
        this.objectMapper = objectMapper;

        // counts how many audit logs saved — visible in Grafana
        this.auditLogCounter = Counter.builder("audit.logs.saved")
                .description("total audit logs saved")
                .register(meterRegistry);
    }

    // called by EventRouter for every change event
    public void createAuditLog(DiffResult diff) {

        // Step 1: generate human readable log via Groq
        // falls back to basic log if Groq is unavailable
        String humanReadableLog = generateHumanReadableLog(diff);

        // Step 2: collect all tags involved in this change
        String tags = diff.getChanges().stream()
                .map(FieldChange::getTag)
                .distinct()
                .collect(Collectors.joining(","));

        // Step 3: serialize changed fields to JSON string
        String changedFieldsJson = serializeChanges(diff.getChanges());

        // Step 4: build and save audit log entity
        AuditLog auditLog = AuditLog.builder()
                .tableName(diff.getTableName())
                .rowId(diff.getRowId())
                .operation(diff.getOperation())
                .changedFields(changedFieldsJson)
                .humanReadableLog(humanReadableLog)
                .tags(tags)
                .build();

        auditLogRepository.save(auditLog);
        auditLogCounter.increment();

        log.info("Audit log saved for {}.{} — {}",
                diff.getTableName(), diff.getRowId(), diff.getOperation());
    }

    private String generateHumanReadableLog(DiffResult diff) {
        try {
            return callGroqApi(diff);
        } catch (Exception e) {
            // Groq is down — use basic fallback
            // audit log still gets saved, just less readable
            log.warn("Groq API failed, using fallback: {}", e.getMessage());
            return generateFallbackLog(diff);
        }
    }

    private String callGroqApi(DiffResult diff) throws Exception {

        // build human readable description of changes
        String changesDescription = diff.getChanges().stream()
                .map(FieldChange::toHumanReadable)
                .collect(Collectors.joining(", "));

        // prompt sent to Groq
        String prompt = String.format(
                "Generate one clear audit log sentence for this database change. " +
                        "Be concise and professional. " +
                        "Table: %s, Operation: %s, Row ID: %s, Changes: %s.",
                diff.getTableName(),
                diff.getOperation(),
                diff.getRowId(),
                changesDescription
        );

        // build Groq API request body
        Map<String, Object> requestBody = Map.of(
                "model", groqModel,
                "max_tokens", 150,
                "messages", List.of(
                        Map.of("role", "user", "content", prompt)
                )
        );

        // call Groq API
        String response = webClient.post()
                .uri(groqApiUrl)
                .header("Authorization", "Bearer " + groqApiKey)
                .header("Content-Type", "application/json")
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(String.class)
                .block();

        // parse response and extract text
        var responseNode = objectMapper.readTree(response);
        return responseNode
                .path("choices")
                .get(0)
                .path("message")
                .path("content")
                .asText();
    }

    // used when Groq API is unavailable
    private String generateFallbackLog(DiffResult diff) {
        String changes = diff.getChanges().stream()
                .map(FieldChange::toHumanReadable)
                .collect(Collectors.joining(", "));

        return String.format("%s on %s (ID: %s): %s",
                diff.getOperation(),
                diff.getTableName(),
                diff.getRowId(),
                changes);
    }

    private String serializeChanges(List<FieldChange> changes) {
        try {
            return objectMapper.writeValueAsString(changes);
        } catch (Exception e) {
            return "[]";
        }
    }
}
