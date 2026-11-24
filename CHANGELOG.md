# Changelog

All notable changes to `distributed-event-pipeline` are documented here.

## [1.1.0] - 2026-11-24
### Added
- **HTTP Ingress Idempotency**: Support for client-supplied `Idempotency-Key` and `X-Correlation-ID` headers in `OrderController`. Duplicate requests are recognized and returned with HTTP 200 OK and `X-Idempotency-Replay: true` header without duplicate Kafka message emission.
- **Native Exponential Backoff**: Upgraded Kafka consumer error handler from fixed backoff to `ExponentialBackOffWithMaxRetries(3)` with exponential progression (1s, 2s, 4s) up to 8s before poison pill routing to DLT.
- **Comprehensive Controller & Consumer Test Suite**: Added dedicated unit tests for `OrderController` (idempotency replay, custom headers) and `OrderConsumer` (Redis atomic deduplication, error handling lock release).

## [1.0.0] - 2026-11-04
### Added
- Native KRaft Apache Kafka cluster integration in Docker Compose.
- Redis-backed atomic idempotency checking (`SETNX`) with configurable TTL.
- Dead Letter Topic (DLT) error handling with exponential backoff retries.
- PostgreSQL 16 schema with Spring Data JPA and correlation ID indexing.
- Global RFC-7807 ProblemDetail exception handler for unified error schemas.
- Unit test suite verifying order state transitions and negative amount rejections.
- GitHub Actions CI workflow for automated build and test validation.
