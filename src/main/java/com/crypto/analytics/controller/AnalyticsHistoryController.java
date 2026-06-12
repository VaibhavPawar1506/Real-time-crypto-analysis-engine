package com.crypto.analytics.controller;

import com.crypto.analytics.persistence.CryptoMarketHistory;
import com.crypto.analytics.persistence.CryptoMarketHistoryRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/v1/analytics")
public class AnalyticsHistoryController {

    private final CryptoMarketHistoryRepository repository;

    public AnalyticsHistoryController(CryptoMarketHistoryRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/history/{symbol}")
    public Flux<CryptoMarketHistory> getHistory(@PathVariable String symbol) {
        return repository.findBySymbolOrderByPersistedAtDesc(symbol);
    }
}
