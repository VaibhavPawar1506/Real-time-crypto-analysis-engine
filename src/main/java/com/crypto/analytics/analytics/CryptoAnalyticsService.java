package com.crypto.analytics.analytics;

import com.crypto.analytics.ingestion.MarketTick;
import com.crypto.analytics.ingestion.MarketTickEventBus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import jakarta.annotation.PreDestroy;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

@Service
public class CryptoAnalyticsService {

    private static final Logger log = LoggerFactory.getLogger(CryptoAnalyticsService.class);

    private final MarketTickEventBus eventBus;
    private final ReactiveStringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private Disposable streamDisposable;

    public CryptoAnalyticsService(MarketTickEventBus eventBus, ReactiveStringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.eventBus = eventBus;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    // Thread-safe sliding window (5 minutes)
    private final ConcurrentLinkedDeque<MarketTick> slidingWindow = new ConcurrentLinkedDeque<>();
    private static final long WINDOW_SIZE_MS = 5 * 60 * 1000;

    // Latest snapshots for fast polling
    private final ConcurrentHashMap<String, AnalyticsSnapshot> snapshotMap = new ConcurrentHashMap<>();

    public Collection<AnalyticsSnapshot> getLatestSnapshots() {
        return snapshotMap.values();
    }

    @PostConstruct
    public void startAnalyticsPipeline() {
        log.info("Starting Crypto Analytics Service pipeline...");

        this.streamDisposable = eventBus.getTickStream()
                .doOnNext(this::processSlidingWindow)
                .flatMap(tick ->
                        updateRedis(tick)
                                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                                        .maxBackoff(Duration.ofSeconds(10))
                                        .doBeforeRetry(rs -> log.warn("Retrying Redis connection due to drop/failure...")))
                                .onErrorResume(e -> {
                                    log.error("Irrecoverable error updating Redis", e);
                                    return Mono.empty();
                                })
                )
                .subscribe(
                        success -> log.trace("Successfully pushed tick to Redis"),
                        error -> log.error("Error in analytics pipeline: ", error)
                );
    }

    private void processSlidingWindow(MarketTick tick) {
        long threshold = tick.timestamp() - WINDOW_SIZE_MS;

        // Evict ticks older than the 5-minute window threshold
        while (!slidingWindow.isEmpty() && slidingWindow.peekFirst().timestamp() < threshold) {
            slidingWindow.pollFirst();
        }

        // Add the new tick
        slidingWindow.addLast(tick);

        if (slidingWindow.isEmpty()) {
            return;
        }

        // Calculate Open, High, and Low for the sliding window
        BigDecimal open = slidingWindow.peekFirst().price();
        BigDecimal high = slidingWindow.stream()
                .map(MarketTick::price)
                .max(BigDecimal::compareTo)
                .orElse(tick.price());
        BigDecimal low = slidingWindow.stream()
                .map(MarketTick::price)
                .min(BigDecimal::compareTo)
                .orElse(tick.price());

        log.debug("[Sliding Window - 5M] Symbol: {} | Open: {} | High: {} | Low: {} | Ticks in Window: {}",
                tick.symbol(), open, high, low, slidingWindow.size());

        // Update the snapshot map
        snapshotMap.put(tick.symbol().toUpperCase(), 
                new AnalyticsSnapshot(tick.symbol().toUpperCase(), tick, open, high, low));
    }

    private Mono<Boolean> updateRedis(MarketTick tick) {
        try {
            String jsonValue = objectMapper.writeValueAsString(tick);
            String redisKey = "ticker:" + tick.symbol().toUpperCase();
            return redisTemplate.opsForValue().set(redisKey, jsonValue);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize MarketTick to JSON", e);
            return Mono.just(false);
        }
    }

    @PreDestroy
    public void cleanup() {
        if (streamDisposable != null && !streamDisposable.isDisposed()) {
            log.info("[SHUTDOWN] Programmatically disposing active reactive stream pipelines before thread pool termination...");
            streamDisposable.dispose();
        }
    }
}
