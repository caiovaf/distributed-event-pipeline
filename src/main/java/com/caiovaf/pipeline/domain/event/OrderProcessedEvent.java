package com.caiovaf.pipeline.domain.event;

import com.caiovaf.pipeline.domain.model.OrderStatus;

import java.time.Instant;
import java.util.UUID;

public record OrderProcessedEvent(
    UUID orderId,
    String correlationId,
    OrderStatus status,
    String message,
    Instant processedAt
) {}
