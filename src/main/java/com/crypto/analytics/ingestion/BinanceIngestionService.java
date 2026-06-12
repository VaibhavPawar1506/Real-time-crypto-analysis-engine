package com.crypto.analytics.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import io.micrometer.core.instrument.Counter;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.net.URI;
import java.time.Duration;

@Service
public class BinanceIngestionService {

    private static final Logger log = LoggerFactory.getLogger(BinanceIngestionService.class);

    private final MarketTickEventBus eventBus;
    private final ObjectMapper objectMapper;
    private final ReactiveStringRedisTemplate redisTemplate;
    private final Counter ticksReceivedCounter;
    private static final String BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@ticker";

    public BinanceIngestionService(MarketTickEventBus eventBus, ObjectMapper objectMapper, ReactiveStringRedisTemplate redisTemplate, Counter ticksReceivedCounter) {
        this.eventBus = eventBus;
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
        this.ticksReceivedCounter = ticksReceivedCounter;
    }

    @PostConstruct
    public void startIngestion() {
        log.info("Starting Binance WebSocket Ingestion Service...");
        ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();

        Mono<Void> connectionMono = client.execute(URI.create(BINANCE_WS_URL), session -> 
            session.receive()
                   .map(WebSocketMessage::getPayloadAsText)
                   .flatMap(json -> Mono.justOrEmpty(parseTick(json)))
                   .filterWhen(tick -> {
                       // Create unique key from Symbol + Price + Volume + Timestamp
                       String key = "idem:" + tick.symbol() + ":" + tick.price() + ":" + tick.volume() + ":" + tick.timestamp();
                       
                       // Redis SETNX with 5 Seconds TTL
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

        // Subscribe asynchronously. Retry logic ensures it stays up if disconnected.
        connectionMono
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(5)))
                .subscribe(
                        null,
                        error -> log.error("WebSocket connection failed: ", error),
                        () -> log.info("WebSocket connection completed")
                );
    }

    private MarketTick parseTick(String json) {
        try {
            return objectMapper.readValue(json, MarketTick.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to parse market tick JSON: {}", json, e);
            return null;
        }
    }
}
