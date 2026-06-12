package com.crypto.analytics.websocket;

import com.crypto.analytics.analytics.AnalyticsSnapshot;
import com.crypto.analytics.analytics.CryptoAnalyticsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
@EnableScheduling
public class CryptoBroadcastWorker {

    private static final Logger log = LoggerFactory.getLogger(CryptoBroadcastWorker.class);

    private final CryptoAnalyticsService analyticsService;
    private final SimpMessagingTemplate messagingTemplate;

    public CryptoBroadcastWorker(CryptoAnalyticsService analyticsService, SimpMessagingTemplate messagingTemplate) {
        this.analyticsService = analyticsService;
        this.messagingTemplate = messagingTemplate;
    }

    /**
     * Polls the in-memory latest stats snapshot every 200ms and broadcasts
     * it directly to the STOMP topics for UI subscribers.
     */
    @Scheduled(fixedRate = 200)
    public void broadcastAnalytics() {
        Collection<AnalyticsSnapshot> snapshots = analyticsService.getLatestSnapshots();
        
        for (AnalyticsSnapshot snapshot : snapshots) {
            String destination = "/topic/live-crypto/" + snapshot.symbol();
            messagingTemplate.convertAndSend(destination, snapshot);
            log.trace("Broadcasted snapshot to {}", destination);
        }
    }
}
