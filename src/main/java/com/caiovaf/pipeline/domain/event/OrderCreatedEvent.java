package com.caiovaf.pipeline.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(
    UUID orderId,
    String correlationId,
    String customerId,
    BigDecimal amount,
    Instant timestamp
) {}
