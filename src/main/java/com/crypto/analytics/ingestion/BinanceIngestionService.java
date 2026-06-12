package com.crypto.analytics.ingestion;

import com.crypto.analytics.config.CryptoEngineConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import io.micrometer.core.instrument.Counter;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.Disposable;
import jakarta.annotation.PreDestroy;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.net.URI;
import java.time.Duration;
import java.util.stream.Collectors;

@Service
public class BinanceIngestionService {

    private static final Logger log = LoggerFactory.getLogger(BinanceIngestionService.class);

    private final MarketTickEventBus eventBus;
    private final ObjectMapper objectMapper;
    private final ReactiveStringRedisTemplate redisTemplate;
    private final Counter ticksReceivedCounter;
    private final CryptoEngineConfig engineConfig;
    private Disposable streamDisposable;

    public BinanceIngestionService(MarketTickEventBus eventBus, ObjectMapper objectMapper, ReactiveStringRedisTemplate redisTemplate, Counter ticksReceivedCounter, CryptoEngineConfig engineConfig) {
        this.eventBus = eventBus;
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
        this.ticksReceivedCounter = ticksReceivedCounter;
        this.engineConfig = engineConfig;
    }

    @PostConstruct
    public void init() {
        startMultiplexStream();
    }

    public void startMultiplexStream() {
        if (streamDisposable != null && !streamDisposable.isDisposed()) {
            log.info("Disposing previous WebSocket connection to re-establish multiplex stream...");
            streamDisposable.dispose();
        }

        String combinedStreams = engineConfig.getActiveSymbols().stream()
                .map(symbol -> symbol.toLowerCase() + "@ticker")
                .collect(Collectors.joining("/"));

        if (combinedStreams.isEmpty()) {
            log.warn("No active symbols to stream. WebSocket connection aborted.");
            return;
        }

        String wsUrl = "wss://stream.binance.com:9443/stream?streams=" + combinedStreams;
        log.info("Starting Binance WebSocket Multiplex Stream: {}", wsUrl);
        
        ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();

        Mono<Void> connectionMono = client.execute(URI.create(wsUrl), session -> 
            session.receive()
                   .map(WebSocketMessage::getPayloadAsText)
                   .onBackpressureBuffer(1000, dropped -> log.warn("[BACKPRESSURE DETECTED] Dropping messages to prevent memory overflow"), BufferOverflowStrategy.DROP_LATEST)
                   .flatMap(json -> Mono.justOrEmpty(parseTick(json)))
                   .filterWhen(tick -> {
                       String key = "idem:" + tick.symbol() + ":" + tick.price() + ":" + tick.volume() + ":" + tick.timestamp();
                       return redisTemplate.opsForValue()
                               .setIfAbsent(key, "1", Duration.ofSeconds(5))
                               .doOnNext(isNew -> {
                                   if (Boolean.FALSE.equals(isNew)) {
                                       log.debug("[DUPLICATE TICK DROPPED] {}", key);
                                   }
                               });
                   })
                   .doOnNext(tick -> {
                       ticksReceivedCounter.increment();
                       log.debug("Received unique tick: {}", tick.price());
                       eventBus.publishTick(tick);
                   })
                   .then()
        );

        this.streamDisposable = connectionMono
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(5)))
                .subscribe(
                        null,
                        error -> log.error("WebSocket connection failed: ", error),
                        () -> log.info("WebSocket connection completed")
                );
    }

    private MarketTick parseTick(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            if (root.has("data")) {
                return objectMapper.treeToValue(root.get("data"), MarketTick.class);
            } else {
                return objectMapper.readValue(json, MarketTick.class);
            }
        } catch (Exception e) {
            log.error("Failed to parse market tick JSON: {}", json, e);
            return null;
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
