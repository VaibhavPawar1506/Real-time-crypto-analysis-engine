package com.crypto.analytics.analytics;

import com.crypto.analytics.ingestion.MarketTick;
import java.math.BigDecimal;

public record AnalyticsSnapshot(
    String symbol,
    MarketTick latestTick,
    BigDecimal open,
    BigDecimal high,
    BigDecimal low
) {}
