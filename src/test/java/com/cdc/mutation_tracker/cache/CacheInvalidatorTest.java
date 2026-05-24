package com.cdc.mutation_tracker.cache;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.Set;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CacheInvalidatorTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    private CacheInvalidator cacheInvalidator;

    @BeforeEach
    void setUp() {
        cacheInvalidator = new CacheInvalidator(
                redisTemplate,
                new SimpleMeterRegistry()
        );
    }

    @Test
    void invalidate_shouldDeleteExactKey() {
        // when users:5 changes, delete that exact Redis key
        when(redisTemplate.keys("users:*")).thenReturn(Set.of());

        cacheInvalidator.invalidate("users", "5");

        verify(redisTemplate).delete("users:5");
    }

    @Test
    void invalidate_shouldDeletePatternKeys() {
        // also delete list caches like users:all, users:page:1
        Set<String> listKeys = Set.of("users:all", "users:page:1");
        when(redisTemplate.keys("users:*")).thenReturn(listKeys);

        cacheInvalidator.invalidate("users", "5");

        verify(redisTemplate).delete(listKeys);
    }

    @Test
    void invalidate_noPatternKeys_shouldNotCallDeleteForEmptySet() {
        // if no list caches exist, don't call delete with empty set
        when(redisTemplate.keys("users:*")).thenReturn(Set.of());

        cacheInvalidator.invalidate("users", "5");

        // delete called once for exact key only
        verify(redisTemplate, times(1)).delete(anyString());
        verify(redisTemplate, never()).delete(anySet());
    }

    @Test
    void invalidate_redisThrowsException_shouldNotPropagate() {
        // Redis failure must never stop the CDC pipeline
        when(redisTemplate.keys(anyString()))
                .thenThrow(new RuntimeException("Redis down"));

        // should not throw — pipeline must continue
        cacheInvalidator.invalidate("users", "5");
    }

    @Test
    void invalidate_nullKeys_shouldHandleGracefully() {
        // redisTemplate.keys() can return null
        when(redisTemplate.keys("users:*")).thenReturn(null);

        // should not throw NullPointerException
        cacheInvalidator.invalidate("users", "5");

        verify(redisTemplate).delete("users:5");
    }
}