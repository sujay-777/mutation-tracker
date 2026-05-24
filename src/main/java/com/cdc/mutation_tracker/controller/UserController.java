package com.cdc.mutation_tracker.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/users")
@RequiredArgsConstructor
@Slf4j
public class UserController {

    private final RedisTemplate<String, String> redisTemplate;
    private final JdbcTemplate jdbcTemplate;

    // GET /users/{id}
    // First checks Redis cache, then falls back to PostgreSQL
    @GetMapping("/{id}")
    public ResponseEntity<String> getUser(@PathVariable String id) {

        String cacheKey = "users:" + id;

        // Step 1: check Redis cache
        String cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            log.info("Cache HIT for users:{}", id);
            return ResponseEntity.ok("CACHE HIT: " + cached);
        }

        // Step 2: cache miss — fetch from PostgreSQL
        log.info("Cache MISS for users:{} — fetching from DB", id);
        try {
            Map<String, Object> user = jdbcTemplate.queryForMap(
                    "SELECT * FROM users WHERE id = ?", id
            );

            String userJson = user.toString();

            // Step 3: store in Redis with 5 minute TTL
            redisTemplate.opsForValue().set(cacheKey, userJson, 5, TimeUnit.MINUTES);
            log.info("Cached users:{} in Redis", id);

            return ResponseEntity.ok("DB HIT (now cached): " + userJson);

        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }
}