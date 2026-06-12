package com.crypto.analytics.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    public Counter ticksReceivedCounter(MeterRegistry registry) {
        return Counter.builder("crypto.ticks.received")
                .description("Total number of incoming crypto market ticks received")
                .register(registry);
    }

    @Bean
    public Counter alertsFiredCounter(MeterRegistry registry) {
        return Counter.builder("crypto.alerts.fired")
                .description("Total number of market alerts fired")
                .register(registry);
    }
}
