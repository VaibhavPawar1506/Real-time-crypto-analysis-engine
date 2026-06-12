package com.crypto.analytics.alerting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import io.micrometer.core.instrument.Counter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import java.time.Duration;

@Service
public class MarketAlertEngine {

    private static final Logger log = LoggerFactory.getLogger(MarketAlertEngine.class);
    private final WebClient webClient;
    private final Counter alertsFiredCounter;
    private final ReactiveStringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private static final String WEBHOOK_URL = "https://httpbin.org/post";

    public MarketAlertEngine(WebClient.Builder webClientBuilder, Counter alertsFiredCounter, ReactiveStringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.webClient = webClientBuilder.build();
        this.alertsFiredCounter = alertsFiredCounter;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    public void fireAlertAsync(AlertPayload payload) {
        alertsFiredCounter.increment();
        log.info("[ALERT TRIGGERED] -> Sending payload to webhook...");
        webClient.post()
                .uri(WEBHOOK_URL)
                .bodyValue(payload)
                .exchangeToMono(response -> {
                    log.info("Webhook Response Status: {}", response.statusCode());
                    if (response.statusCode().isError()) {
                        return response.createException().flatMap(Mono::error);
                    }
                    return response.releaseBody();
                })
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                .onErrorResume(error -> {
                    log.error("[DLQ TRIGGERED] Webhook failed after retries: {}", error.getMessage());
                    try {
                        String json = objectMapper.writeValueAsString(payload);
                        return redisTemplate.opsForList().rightPush("alerts:dlq", json).then();
                    } catch (Exception e) {
                        log.error("Failed to serialize and push payload to DLQ", e);
                        return Mono.empty();
                    }
                })
                // Subscribe handles the request asynchronously on Netty's event loop, without blocking parallel-1
                .subscribe(
                        null,
                        error -> log.error("Failed to send webhook alert", error)
                );
    }
}
