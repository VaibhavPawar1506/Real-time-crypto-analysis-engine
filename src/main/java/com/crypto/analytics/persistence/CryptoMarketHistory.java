package com.crypto.analytics.persistence;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import java.time.LocalDateTime;

@Table("crypto_market_history")
public class CryptoMarketHistory {

    @Id
    private Long id;
    private String symbol;
    private Double openPrice;
    private Double highPrice;
    private Double lowPrice;
    private Double closePrice;
    private Double vwap;
    private Integer ticksCount;
    private LocalDateTime persistedAt;

    public CryptoMarketHistory() {}

    public CryptoMarketHistory(String symbol, Double openPrice, Double highPrice, Double lowPrice, Double closePrice, Double vwap, Integer ticksCount, LocalDateTime persistedAt) {
        this.symbol = symbol;
        this.openPrice = openPrice;
        this.highPrice = highPrice;
        this.lowPrice = lowPrice;
        this.closePrice = closePrice;
        this.vwap = vwap;
        this.ticksCount = ticksCount;
        this.persistedAt = persistedAt;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }
    public Double getOpenPrice() { return openPrice; }
    public void setOpenPrice(Double openPrice) { this.openPrice = openPrice; }
    public Double getHighPrice() { return highPrice; }
    public void setHighPrice(Double highPrice) { this.highPrice = highPrice; }
    public Double getLowPrice() { return lowPrice; }
    public void setLowPrice(Double lowPrice) { this.lowPrice = lowPrice; }
    public Double getClosePrice() { return closePrice; }
    public void setClosePrice(Double closePrice) { this.closePrice = closePrice; }
    public Double getVwap() { return vwap; }
    public void setVwap(Double vwap) { this.vwap = vwap; }
    public Integer getTicksCount() { return ticksCount; }
    public void setTicksCount(Integer ticksCount) { this.ticksCount = ticksCount; }
    public LocalDateTime getPersistedAt() { return persistedAt; }
    public void setPersistedAt(LocalDateTime persistedAt) { this.persistedAt = persistedAt; }
}
