package com.crypto.analytics.persistence;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface CryptoMarketHistoryRepository extends ReactiveCrudRepository<CryptoMarketHistory, Long> {
    Flux<CryptoMarketHistory> findBySymbolOrderByPersistedAtDesc(String symbol);
}
