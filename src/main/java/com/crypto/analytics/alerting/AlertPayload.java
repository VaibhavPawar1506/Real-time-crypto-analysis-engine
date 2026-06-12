package com.crypto.analytics.alerting;

public record AlertPayload(
        String symbol,
        String message,
        String alertType,
        double currentPrice,
        String timestamp
) {}
