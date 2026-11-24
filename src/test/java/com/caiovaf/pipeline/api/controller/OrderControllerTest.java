package com.caiovaf.pipeline.api.controller;

import com.caiovaf.pipeline.api.dto.CreateOrderRequest;
import com.caiovaf.pipeline.api.dto.OrderResponse;
import com.caiovaf.pipeline.domain.event.OrderCreatedEvent;
import com.caiovaf.pipeline.domain.model.Order;
import com.caiovaf.pipeline.domain.model.OrderStatus;
import com.caiovaf.pipeline.infrastructure.kafka.OrderProducer;
import com.caiovaf.pipeline.infrastructure.persistence.OrderRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderControllerTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OrderProducer orderProducer;

    @InjectMocks
    private OrderController orderController;

    @Test
    @DisplayName("Should create new order and publish event when no idempotency key is supplied")
    void shouldCreateOrderWithoutIdempotencyKey() {
        CreateOrderRequest request = new CreateOrderRequest("cust_999", new BigDecimal("120.00"));
        UUID generatedId = UUID.randomUUID();

        when(orderRepository.findByCorrelationId(any())).thenReturn(Optional.empty());
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> {
            Order o = inv.getArgument(0);
            o.setId(generatedId);
            return o;
        });

        ResponseEntity<OrderResponse> response = orderController.createOrder(null, null, request);

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("cust_999", response.getBody().customerId());
        assertEquals(new BigDecimal("120.00"), response.getBody().amount());
        assertEquals(OrderStatus.PENDING, response.getBody().status());

        verify(orderRepository).save(any(Order.class));
        verify(orderProducer).publishOrderCreated(any(OrderCreatedEvent.class));
    }

    @Test
    @DisplayName("Should use client-provided Idempotency-Key and publish event on first delivery")
    void shouldCreateOrderWithClientProvidedIdempotencyKey() {
        String customKey = "client-req-uuid-456";
        CreateOrderRequest request = new CreateOrderRequest("cust_777", new BigDecimal("85.50"));

        when(orderRepository.findByCorrelationId(customKey)).thenReturn(Optional.empty());
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> {
            Order o = inv.getArgument(0);
            o.setId(UUID.randomUUID());
            return o;
        });

        ResponseEntity<OrderResponse> response = orderController.createOrder(customKey, null, request);

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        assertEquals(customKey, response.getHeaders().getFirst("X-Correlation-ID"));

        ArgumentCaptor<OrderCreatedEvent> eventCaptor = ArgumentCaptor.forClass(OrderCreatedEvent.class);
        verify(orderProducer).publishOrderCreated(eventCaptor.capture());
        assertEquals(customKey, eventCaptor.getValue().correlationId());
    }

    @Test
    @DisplayName("Should return existing order without publishing new event on idempotent request replay")
    void shouldReturnExistingOrderOnIdempotentReplay() {
        String existingKey = "idemp-existing-999";
        CreateOrderRequest request = new CreateOrderRequest("cust_777", new BigDecimal("85.50"));

        Order existingOrder = Order.builder()
            .id(UUID.randomUUID())
            .correlationId(existingKey)
            .customerId("cust_777")
            .amount(new BigDecimal("85.50"))
            .status(OrderStatus.PROCESSED)
            .createdAt(Instant.now())
            .build();

        when(orderRepository.findByCorrelationId(existingKey)).thenReturn(Optional.of(existingOrder));

        ResponseEntity<OrderResponse> response = orderController.createOrder(existingKey, null, request);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("true", response.getHeaders().getFirst("X-Idempotency-Replay"));
        assertNotNull(response.getBody());
        assertEquals(existingKey, response.getBody().correlationId());
        assertEquals(OrderStatus.PROCESSED, response.getBody().status());

        // Crucial: ensure no duplicate side-effects
        verify(orderRepository, never()).save(any());
        verify(orderProducer, never()).publishOrderCreated(any());
    }

    @Test
    @DisplayName("Should find order by ID when exists")
    void shouldFindOrderById() {
        UUID id = UUID.randomUUID();
        Order order = Order.builder()
            .id(id)
            .correlationId("cor_123")
            .customerId("cust_1")
            .amount(new BigDecimal("50.00"))
            .status(OrderStatus.PENDING)
            .build();

        when(orderRepository.findById(id)).thenReturn(Optional.of(order));

        ResponseEntity<OrderResponse> response = orderController.getOrderById(id);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(id, response.getBody().id());
    }
}
