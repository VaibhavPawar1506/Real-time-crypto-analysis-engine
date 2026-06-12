package com.crypto.analytics.alerting;

import com.crypto.analytics.config.CryptoEngineConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@Service
public class MarketAlertEngine {

    private static final Logger log = LoggerFactory.getLogger(MarketAlertEngine.class);
    private static final String WEBHOOK_URL = "https://httpbin.org/post";
    private static final String SECRET_KEY = "crypto-analytics-hmac-secret-key-2026";

    private final WebClient webClient;
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final CryptoEngineConfig engineConfig;
    private final ObjectMapper objectMapper;
    private final Counter alertsFiredCounter;

    public MarketAlertEngine(WebClient.Builder webClientBuilder,
                             ReactiveRedisTemplate<String, String> redisTemplate,
                             CryptoEngineConfig engineConfig,
                             ObjectMapper objectMapper,
                             Counter alertsFiredCounter) {
        this.webClient = webClientBuilder.build();
        this.redisTemplate = redisTemplate;
        this.engineConfig = engineConfig;
        this.objectMapper = objectMapper;
        this.alertsFiredCounter = alertsFiredCounter;
    }

    public Mono<Boolean> isDuplicateEvent(String tradeId) {
        String key = "processed:trade:" + tradeId;
        return redisTemplate.opsForValue()
                .setIfAbsent(key, "1", Duration.ofMinutes(1))
                .map(isNew -> !isNew); // Return true if duplicate (i.e. not new)
    }

    private String calculateHMAC(String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKeySpec = new SecretKeySpec(SECRET_KEY.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac.init(secretKeySpec);
            byte[] hmacData = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hmacData);
        } catch (Exception e) {
            log.error("Failed to calculate HMAC signature", e);
            throw new RuntimeException("HMAC calculation failed", e);
        }
    }

    public void triggerSecureAlert(String symbol, double priceChange) {
        Map<String, Object> payloadMap = new HashMap<>();
        payloadMap.put("symbol", symbol);
        payloadMap.put("priceChange", priceChange);
        payloadMap.put("thresholdUsed", engineConfig.getAlertThreshold());
        payloadMap.put("timestamp", System.currentTimeMillis());

        String jsonPayload;
        try {
            jsonPayload = objectMapper.writeValueAsString(payloadMap);
        } catch (Exception e) {
            log.error("Failed to serialize alert payload", e);
            return;
        }

        String signature = calculateHMAC(jsonPayload);
        alertsFiredCounter.increment();

        webClient.post()
                .uri(WEBHOOK_URL)
                .header("X-Hub-Signature", signature)
                .bodyValue(jsonPayload)
                .retrieve()
                .bodyToMono(String.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                .onErrorResume(error -> {
                    log.error("[DLQ TRIGGERED] Secure alert failed after retries for {}: {}", symbol, error.getMessage());
                    return redisTemplate.opsForList()
                            .leftPush("alerts:dlq", jsonPayload)
                            .then(Mono.empty());
                })
                .subscribe(
                        response -> log.info("Secure alert sent successfully: {}", response),
                        error -> log.error("Error in triggerSecureAlert subscription", error)
                );
    }

    // Retaining this for backwards compatibility with existing usages (e.g. LiveMarketAnalyticsEngine)
    public void fireAlertAsync(AlertPayload payload) {
        triggerSecureAlert("UNKNOWN", payload.currentPrice());
    }
}
