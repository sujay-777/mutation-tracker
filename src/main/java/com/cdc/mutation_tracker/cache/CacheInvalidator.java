package com.cdc.mutation_tracker.cache;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@Slf4j
public class CacheInvalidator {

    private final RedisTemplate<String, String> redisTemplate;
    private final Counter successCounter;
    private final Counter failureCounter;

    public CacheInvalidator(
            RedisTemplate<String, String> redisTemplate,
            MeterRegistry meterRegistry) {

        this.redisTemplate = redisTemplate;

        // Prometheus counters — visible in Grafana dashboard later
        this.successCounter = Counter.builder("cache.invalidation.success")
                .description("successful cache invalidations")
                .register(meterRegistry);

        this.failureCounter = Counter.builder("cache.invalidation.failure")
                .description("failed cache invalidations")
                .register(meterRegistry);
    }

    public void invalidate(String tableName, String rowId) {
        try {
            // delete exact row cache — e.g. "users:5"
            String exactKey = tableName + ":" + rowId;
            redisTemplate.delete(exactKey);

            // also delete any list caches for this table
            // e.g. "users:all", "users:page:1"
            // because those lists now contain stale data too
            String patternKey = tableName + ":*";
            Set<String> keys = redisTemplate.keys(patternKey);
            if (keys != null && !keys.isEmpty()) {
                redisTemplate.delete(keys);
            }

            successCounter.increment();
            log.debug("Cache invalidated: {}", exactKey);

        } catch (Exception e) {
            // Redis is down or slow — log and move on
            // never stop the CDC pipeline for cache issues
            failureCounter.increment();
            log.error("Cache invalidation failed for {}:{} — {}",
                    tableName, rowId, e.getMessage());
        }
    }
}