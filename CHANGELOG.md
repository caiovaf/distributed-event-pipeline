# Changelog

All notable changes to `distributed-event-pipeline` are documented here.

## [1.0.0] - 2026-11-04
### Added
- Native KRaft Apache Kafka cluster integration in Docker Compose.
- Redis-backed atomic idempotency checking (`SETNX`) with configurable TTL.
- Dead Letter Topic (DLT) error handling with exponential backoff retries.
- PostgreSQL 16 schema with Spring Data JPA and correlation ID indexing.
- Global RFC-7807 ProblemDetail exception handler for unified error schemas.
- Unit test suite verifying order state transitions and negative amount rejections.
- GitHub Actions CI workflow for automated build and test validation.
