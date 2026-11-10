package com.caiovaf.pipeline.application.service;

import com.caiovaf.pipeline.domain.event.OrderCreatedEvent;
import com.caiovaf.pipeline.domain.model.Order;
import com.caiovaf.pipeline.domain.model.OrderStatus;
import com.caiovaf.pipeline.infrastructure.kafka.OrderProducer;
import com.caiovaf.pipeline.infrastructure.persistence.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderProcessingServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OrderProducer orderProducer;

    @InjectMocks
    private OrderProcessingService orderProcessingService;

    private UUID orderId;
    private Order pendingOrder;

    @BeforeEach
    void setUp() {
        orderId = UUID.randomUUID();
        pendingOrder = Order.builder()
            .id(orderId)
            .correlationId(UUID.randomUUID().toString())
            .customerId("cust_123")
            .amount(new BigDecimal("150.00"))
            .status(OrderStatus.PENDING)
            .build();
    }

    @Test
    @DisplayName("Should successfully process valid order and emit processed event")
    void shouldProcessValidOrder() {
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(pendingOrder));
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> invocation.getArgument(0));

        OrderCreatedEvent event = new OrderCreatedEvent(
            orderId, pendingOrder.getCorrelationId(), "cust_123", new BigDecimal("150.00"), Instant.now()
        );

        orderProcessingService.processOrder(event);

        assertEquals(OrderStatus.PROCESSED, pendingOrder.getStatus());
        verify(orderRepository).save(pendingOrder);
        verify(orderProducer).publishOrderProcessed(any());
    }

    @Test
    @DisplayName("Should reject order when amount is zero or negative")
    void shouldRejectInvalidAmount() {
        pendingOrder.setAmount(BigDecimal.ZERO);
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(pendingOrder));

        OrderCreatedEvent event = new OrderCreatedEvent(
            orderId, pendingOrder.getCorrelationId(), "cust_123", BigDecimal.ZERO, Instant.now()
        );

        orderProcessingService.processOrder(event);

        assertEquals(OrderStatus.FAILED, pendingOrder.getStatus());
        verify(orderProducer).publishOrderProcessed(argThat(e -> e.status() == OrderStatus.FAILED));
    }
}
