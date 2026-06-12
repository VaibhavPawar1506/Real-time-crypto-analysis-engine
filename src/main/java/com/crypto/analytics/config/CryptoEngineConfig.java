package com.crypto.analytics.config;

import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Configuration
public class CryptoEngineConfig {

    private final AtomicReference<Double> alertThreshold = new AtomicReference<>(2.0);
    private final ConcurrentHashMap.KeySetView<String, Boolean> activeSymbols = ConcurrentHashMap.newKeySet();

    public CryptoEngineConfig() {
        activeSymbols.add("btcusdt");
        activeSymbols.add("ethusdt");
    }

    public Double getAlertThreshold() {
        return alertThreshold.get();
    }

    public void setAlertThreshold(Double threshold) {
        this.alertThreshold.set(threshold);
    }

    public void subscribeSymbol(String symbol) {
        if (symbol != null && !symbol.trim().isEmpty()) {
            activeSymbols.add(symbol.toLowerCase());
        }
    }

    public void unsubscribeSymbol(String symbol) {
        if (symbol != null) {
            activeSymbols.remove(symbol.toLowerCase());
        }
    }

    public ConcurrentHashMap.KeySetView<String, Boolean> getActiveSymbols() {
        return activeSymbols;
    }
}
