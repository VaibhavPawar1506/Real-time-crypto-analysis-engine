package com.crypto.analytics.ingestion;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Component
public class MarketTickEventBus {

    // Multicast sink acting as an internal event bus/system memory for parsed ticks
    private final Sinks.Many<MarketTick> sink = Sinks.many().multicast().onBackpressureBuffer();

    public void publishTick(MarketTick tick) {
        // Emit tick to the bus, dropping it if buffer overflows
        sink.tryEmitNext(tick);
    }

    public Flux<MarketTick> getTickStream() {
        return sink.asFlux();
    }
}
