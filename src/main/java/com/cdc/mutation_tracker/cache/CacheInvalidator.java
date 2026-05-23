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

    public CacheInvalidator(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void invalidate(String tableName, String rowId) {
        try {
            String exactKey = tableName + ":" + rowId;
            redisTemplate.delete(exactKey);

            Set<String> keys = redisTemplate.keys(tableName + ":*");
            if (keys != null && !keys.isEmpty()) {
                redisTemplate.delete(keys);
            }
            log.debug("Cache invalidated: {}", exactKey);
        } catch (Exception e) {
            log.error("Cache invalidation failed for {}:{} — {}",
                    tableName, rowId, e.getMessage());
        }
    }
}