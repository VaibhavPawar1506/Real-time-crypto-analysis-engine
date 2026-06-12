# Real-Time Crypto Analytics Engine

A robust, reactive, and non-blocking real-time cryptocurrency analytics engine built with **Java 21**, **Spring Boot 3.3**, and **Project Reactor**. This system connects directly to the Binance WebSocket API to ingest live market ticks, calculate real-time analytics over a sliding window, trigger dynamic alerts, and broadcast aggregated metrics to frontend clients.

## Key Features

- **Live Data Ingestion**: Maintains a persistent WebSocket connection to Binance to consume real-time market ticks (e.g., BTC/USDT, ETH/USDT).
- **Reactive & Non-Blocking**: Built from the ground up using Spring WebFlux and Java 21 Virtual Threads (`spring.threads.virtual.enabled=true`) for maximum throughput and minimal resource usage.
- **Deduplication**: Ensures data integrity by performing idempotency checks via Redis before processing market ticks.
- **Sliding Window Analytics**: Calculates moving metrics (Open, High, Low, Close, VWAP) over a configurable 60-second sliding window.
- **Resilience**: Integrated with Resilience4j to provide Rate Limiting and Circuit Breaking to prevent system overloads.
- **Dynamic Alerting**: Evaluates price volatility and system throughput dynamically, firing HTTP Webhook alerts. Implements a Dead Letter Queue (DLQ) in Redis for failed alerts.
- **Historical Persistence**: Periodically batches and saves aggregated 1-minute snapshots into an H2 database (running in PostgreSQL mode) using R2DBC.
- **Real-Time Broadcasting**: Broadcasts live analytics updates to frontend clients over STOMP/WebSockets.

## Technology Stack

- **Java 21** (Virtual Threads)
- **Spring Boot 3.3.0**
- **Spring WebFlux** (Project Reactor)
- **Spring Data Redis Reactive**
- **Spring Data R2DBC & H2 Database**
- **Spring WebSocket / STOMP**
- **Resilience4j** (Rate Limiter, Circuit Breaker)
- **Micrometer & Prometheus** (Metrics & Observability)

## Getting Started

### Prerequisites

- Java 21 or later
- Maven 3.8+
- Redis (running locally on port 6379, or configured in `application.properties`)

### Running the Application

1. Make sure your local Redis server is up and running.
2. Clone the repository and navigate to the project directory:
   ```bash
   git clone https://github.com/VaibhavPawar1506/Real-time-crypto-analysis-engine.git
   cd Real-time-crypto-analysis-engine
   ```
3. Build the project using Maven:
   ```bash
   mvn clean install
   ```
4. Run the Spring Boot application:
   ```bash
   mvn spring-boot:run
   ```

### Application Configuration

The core runtime configurations, such as the `alertThreshold` and `activeSymbols` (e.g., `btcusdt`, `ethusdt`), are managed dynamically in memory via the `CryptoEngineConfig` class. The system uses a thread-safe implementation to allow runtime updates to these configurations without requiring a reboot.

### Metrics & Monitoring

Prometheus metrics are exposed via the Actuator endpoint. Ensure you have the endpoint enabled:
```
http://localhost:8080/actuator/prometheus
```

## Contributing
Contributions and PRs are welcome. Please adhere to reactive programming patterns and write tests for new components.
