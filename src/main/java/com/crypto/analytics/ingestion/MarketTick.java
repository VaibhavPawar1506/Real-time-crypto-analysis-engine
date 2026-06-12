package com.crypto.analytics.ingestion;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MarketTick(
        @JsonProperty("s") String symbol,
        @JsonProperty("c") BigDecimal price,
        @JsonProperty("v") BigDecimal volume,
        @JsonProperty("E") long timestamp
) {
}
