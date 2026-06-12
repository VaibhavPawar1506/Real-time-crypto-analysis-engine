package com.crypto.analytics.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class DynamicRuleConfig {
    private static final Logger log = LoggerFactory.getLogger(DynamicRuleConfig.class);
    
    private final AtomicReference<Double> volatilityThreshold = new AtomicReference<>(2.0);

    public double getVolatilityThreshold() {
        return volatilityThreshold.get();
    }

    public void setVolatilityThreshold(double newThreshold) {
        volatilityThreshold.set(newThreshold);
        log.info("[RULE UPDATED] New alert threshold set to {}%", newThreshold);
    }
}
