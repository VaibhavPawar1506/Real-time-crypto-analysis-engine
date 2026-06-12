package com.crypto.analytics.analytics;

import com.crypto.analytics.alerting.AlertPayload;
import com.crypto.analytics.alerting.MarketAlertEngine;
import com.crypto.analytics.config.DynamicRuleConfig;
import com.crypto.analytics.ingestion.MarketTick;
import com.crypto.analytics.ingestion.MarketTickEventBus;
import com.crypto.analytics.persistence.CryptoMarketHistory;
import com.crypto.analytics.persistence.CryptoMarketHistoryRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.reactor.ratelimiter.operator.RateLimiterOperator;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class LiveMarketAnalyticsEngine {

    private static final Logger log = LoggerFactory.getLogger(LiveMarketAnalyticsEngine.class);

    private final MarketTickEventBus eventBus;
    private final ConcurrentLinkedQueue<MarketTick> slidingWindow = new ConcurrentLinkedQueue<>();
    private static final long WINDOW_SIZE_MS = 60 * 1000;
    private final AtomicReference<AnalyticsMetrics> latestMetrics = new AtomicReference<>();
    
    // Sink to broadcast metrics to other components (like MarketAlertEngine)
    private final Sinks.Many<AnalyticsMetrics> metricsSink = Sinks.many().multicast().onBackpressureBuffer();

    private final RateLimiter rateLimiter;
    private final CircuitBreaker circuitBreaker;
    
    private final ReactiveStringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final MarketAlertEngine alertEngine;
    private final CryptoMarketHistoryRepository repository;
    private final DynamicRuleConfig dynamicRuleConfig;
    private long lastSavedMinute = 0;

    public LiveMarketAnalyticsEngine(MarketTickEventBus eventBus, ReactiveStringRedisTemplate redisTemplate, ObjectMapper objectMapper, MarketAlertEngine alertEngine, CryptoMarketHistoryRepository repository, DynamicRuleConfig dynamicRuleConfig) {
        this.eventBus = eventBus;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.alertEngine = alertEngine;
        this.repository = repository;
        this.dynamicRuleConfig = dynamicRuleConfig;

        // Setup bulk persistence stream for historical DB
        this.metricsSink.asFlux()
            .map(m -> new CryptoMarketHistory(
                    m.symbol(),
                    m.open().doubleValue(),
                    m.high().doubleValue(),
                    m.low().doubleValue(),
                    m.close().doubleValue(),
                    m.vwap().doubleValue(),
                    m.count(),
                    java.time.LocalDateTime.now()
            ))
            .bufferTimeout(10, Duration.ofSeconds(3))
            .flatMap(batch -> repository.saveAll(batch))
            .subscribe(
                    saved -> log.debug("Bulk saved history record: {}", saved.getId()),
                    error -> log.error("Failed to persist analytics batch", error)
            );

        // Rate Limiter: max 500 requests per second
        RateLimiterConfig rateLimiterConfig = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .limitForPeriod(500)
                .timeoutDuration(Duration.ZERO)
                .build();
        this.rateLimiter = RateLimiter.of("analyticsRateLimiter", rateLimiterConfig);

        // Circuit Breaker: trips if too many RequestNotPermitted exceptions occur (from Rate Limiter)
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Trip if 50% of the window calls fail
                .slidingWindowSize(100) // Evaluate over the last 100 ticks
                .minimumNumberOfCalls(100)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(10)
                .recordExceptions(RequestNotPermitted.class)
                .build();
        this.circuitBreaker = CircuitBreaker.of("analyticsCircuitBreaker", circuitBreakerConfig);

        // Listen for Circuit Breaker state changes to log the required warning
        this.circuitBreaker.getEventPublisher().onStateTransition(event -> {
            if (event.getStateTransition() == CircuitBreaker.StateTransition.CLOSED_TO_OPEN) {
                log.warn("[CIRCUIT BREAKER OPEN - STREAM THROTTLED]");
            }
        });
    }

    public Flux<AnalyticsMetrics> getMetricsStream() {
        return metricsSink.asFlux();
    }

    @PostConstruct
    public void startEngine() {
        // Subscribe to the real-time event bus
        eventBus.getTickStream()
                .concatMap(tick ->
                        Mono.just(tick)
                            .transformDeferred(RateLimiterOperator.of(rateLimiter))
                            .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                            .doOnNext(this::processTickFull)
                            .onErrorResume(Exception.class, e -> {
                                // Degraded processing when rate limit exceeded or circuit breaker open
                                processTickDegraded(tick);
                                return Mono.empty();
                            })
                )
                .subscribe(
                        null,
                        error -> log.error("LiveMarketAnalyticsEngine stream error", error)
                );

        Flux.interval(Duration.ofSeconds(2))
                .subscribe(i -> {
                    AnalyticsMetrics metrics = latestMetrics.get();
                    if (metrics != null) {
                        log.info("[1M Window] Symbol: {} | Open: {} | High: {} | Low: {} | Close: {} | VWAP: {} | Ticks: {}",
                                metrics.symbol(), metrics.open(), metrics.high(), metrics.low(),
                                metrics.close(), metrics.vwap(), metrics.count());
                    }
                });
    }

    private void processTickFull(MarketTick tick) {
        // Detect minute boundary rollover based on event-time timestamp
        long currentMinute = tick.timestamp() / 60000;
        if (lastSavedMinute == 0) {
            lastSavedMinute = currentMinute;
        } else if (currentMinute > lastSavedMinute) {
            AnalyticsMetrics finalSnapshot = latestMetrics.get();
            if (finalSnapshot != null) {
                saveToRedis(finalSnapshot);
            }
            lastSavedMinute = currentMinute;
        }

        long threshold = tick.timestamp() - WINDOW_SIZE_MS;

        while (!slidingWindow.isEmpty() && slidingWindow.peek().timestamp() < threshold) {
            slidingWindow.poll();
        }

        slidingWindow.offer(tick);

        if (slidingWindow.isEmpty()) {
            return;
        }

        MarketTick first = slidingWindow.peek();
        BigDecimal open = first.price();
        BigDecimal close = tick.price();

        BigDecimal high = tick.price();
        BigDecimal low = tick.price();

        BigDecimal sumPriceVol = BigDecimal.ZERO;
        BigDecimal sumVol = BigDecimal.ZERO;

        int count = 0;
        
        for (MarketTick t : slidingWindow) {
            BigDecimal p = t.price();
            BigDecimal v = t.volume();

            if (p.compareTo(high) > 0) high = p;
            if (p.compareTo(low) < 0) low = p;

            BigDecimal priceVol = p.multiply(v);
            sumPriceVol = sumPriceVol.add(priceVol);
            sumVol = sumVol.add(v);
            
            count++;
        }

        BigDecimal vwap = BigDecimal.ZERO;
        if (sumVol.compareTo(BigDecimal.ZERO) > 0) {
            vwap = sumPriceVol.divide(sumVol, 8, RoundingMode.HALF_UP);
        }

        AnalyticsMetrics m = new AnalyticsMetrics(tick.symbol(), open, high, low, close, vwap, count, tick.timestamp());
        latestMetrics.set(m);
        metricsSink.tryEmitNext(m);

        // Alert logic: inside the sliding window execution
        if (open.compareTo(BigDecimal.ZERO) != 0) {
            double openPrice = open.doubleValue();
            double closePrice = close.doubleValue();
            double percentChange = Math.abs((closePrice - openPrice) / openPrice) * 100.0;

            if (percentChange > dynamicRuleConfig.getVolatilityThreshold()) {
                alertEngine.fireAlertAsync(new AlertPayload(
                        tick.symbol(),
                        String.format("Price Volatility Exceeded %.2f%% Window Threshold", dynamicRuleConfig.getVolatilityThreshold()),
                        "PRICE_MOVEMENT",
                        close.doubleValue(),
                        String.valueOf(tick.timestamp())
                ));
            }
        }

        if (count > 0 && count % 10 == 0) {
            alertEngine.fireAlertAsync(new AlertPayload(
                    tick.symbol(),
                    "System Throughput Milestone reached: " + count + " ticks in window.",
                    "SYSTEM_THROUGHPUT",
                    close.doubleValue(),
                    String.valueOf(tick.timestamp())
            ));
        }
    }

    private void processTickDegraded(MarketTick tick) {
        // Drop non-essential metrics (High, Low, Open, VWAP), keeping only the Close price active
        BigDecimal close = tick.price();
        AnalyticsMetrics m = new AnalyticsMetrics(tick.symbol(), BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, close, BigDecimal.ZERO, 0, tick.timestamp());
        latestMetrics.set(m);
        metricsSink.tryEmitNext(m);
    }

    private void saveToRedis(AnalyticsMetrics snapshot) {
        try {
            String json = objectMapper.writeValueAsString(snapshot);
            String key = "snapshot:" + snapshot.symbol() + ":" + snapshot.timestamp();
            redisTemplate.opsForValue().set(key, json, Duration.ofHours(1))
                    .subscribe(
                            null,
                            error -> log.error("Failed to save snapshot to Redis", error)
                    );
        } catch (Exception e) {
            log.error("Failed to serialize snapshot for Redis", e);
        }
    }

    public record AnalyticsMetrics(
            String symbol,
            BigDecimal open,
            BigDecimal high,
            BigDecimal low,
            BigDecimal close,
            BigDecimal vwap,
            int count,
            long timestamp
    ) {}
}
