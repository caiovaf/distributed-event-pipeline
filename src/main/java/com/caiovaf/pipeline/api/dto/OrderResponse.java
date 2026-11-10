package com.caiovaf.pipeline.api.dto;

import com.caiovaf.pipeline.domain.model.Order;
import com.caiovaf.pipeline.domain.model.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
    UUID id,
    String correlationId,
    String customerId,
    BigDecimal amount,
    OrderStatus status,
    String failureReason,
    Instant createdAt,
    Instant updatedAt
) {
    public static OrderResponse fromEntity(Order order) {
        return new OrderResponse(
            order.getId(),
            order.getCorrelationId(),
            order.getCustomerId(),
            order.getAmount(),
            order.getStatus(),
            order.getFailureReason(),
            order.getCreatedAt(),
            order.getUpdatedAt()
        );
    }
}
