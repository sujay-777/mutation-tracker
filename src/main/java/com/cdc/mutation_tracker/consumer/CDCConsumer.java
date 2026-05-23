package com.cdc.mutation_tracker.consumer;

import com.cdc.mutation_tracker.engine.DiffEngine;
import com.cdc.mutation_tracker.exception.MalformedEventException;
import com.cdc.mutation_tracker.model.DebeziumEvent;
import com.cdc.mutation_tracker.model.DiffResult;
import com.cdc.mutation_tracker.router.EventRouter;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class CDCConsumer {

    private final ObjectMapper objectMapper;
    private final DiffEngine diffEngine;
    private final EventRouter eventRouter;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(
            topics = {
                    "cdc.public.users",
                    "cdc.public.orders",
                    "cdc.public.payments"
            },
            groupId = "cdc-consumer-group"
    )
    public void consume(String rawMessage, Acknowledgment acknowledgment) {
        try {

            // Step 1: raw JSON string → DebeziumEvent Java object
            // if JSON is broken this throws exception → caught below
            DebeziumEvent event = objectMapper
                    .readValue(rawMessage, DebeziumEvent.class);

            // Step 2: validate payload exists
            if (event.getPayload() == null) {
                throw new MalformedEventException("Payload is null");
            }

            // Step 3: skip snapshot reads
            // op="r" means Debezium is snapshotting existing rows on startup
            // these are not real changes — ignore them
            if ("r".equals(event.getPayload().getOp())) {
                log.debug("Skipping snapshot read for table: {}",
                        event.getPayload().getSource().getTable());
                acknowledgment.acknowledge();
                return;
            }

            // Step 4: send to DiffEngine
            // DiffEngine compares before vs after field by field
            // returns DiffResult with list of FieldChange objects
            DiffResult diff = diffEngine.compute(event);

            // Step 5: skip if nothing actually changed
            // PostgreSQL sometimes fires WAL for no-op updates
            // e.g. UPDATE users SET email=email WHERE id=1
            if (diff.isEmpty()) {
                log.debug("No changes detected, skipping");
                acknowledgment.acknowledge();
                return;
            }

            // Step 6: send DiffResult to EventRouter
            // router decides where to send what based on tags
            eventRouter.route(diff);

            // Step 7: commit offset ONLY after everything succeeds
            // this is the manual commit — tells Kafka "I'm done with this message"
            acknowledgment.acknowledge();

            log.info("Successfully processed: op={} table={} rowId={}",
                    diff.getOperation(),
                    diff.getTableName(),
                    diff.getRowId());

        } catch (MalformedEventException e) {
            // message is broken and will never be processable
            // send to dead letter queue so pipeline doesn't get stuck
            // still commit offset — move on to next message
            log.error("Malformed event, routing to DLQ: {}", e.getMessage());
            kafkaTemplate.send("audit.dead-letter", rawMessage);
            acknowledgment.acknowledge();

        } catch (Exception e) {
            // something failed during processing
            // do NOT commit offset
            // Kafka will replay this message when app restarts
            // gives you a chance to fix the bug and reprocess
            log.error("Processing failed, will retry on restart: {}",
                    e.getMessage());
        }
    }
}