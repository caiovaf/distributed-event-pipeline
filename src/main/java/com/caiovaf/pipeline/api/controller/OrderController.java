package com.caiovaf.pipeline.api.controller;

import com.caiovaf.pipeline.api.dto.CreateOrderRequest;
import com.caiovaf.pipeline.api.dto.OrderResponse;
import com.caiovaf.pipeline.domain.event.OrderCreatedEvent;
import com.caiovaf.pipeline.domain.model.Order;
import com.caiovaf.pipeline.domain.model.OrderStatus;
import com.caiovaf.pipeline.infrastructure.kafka.OrderProducer;
import com.caiovaf.pipeline.infrastructure.persistence.OrderRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
@Tag(name = "Orders API", description = "Asynchronous, event-driven order processing endpoints")
public class OrderController {

    private final OrderRepository orderRepository;
    private final OrderProducer orderProducer;

    @PostMapping
    @Transactional
    @Operation(summary = "Submit a new order into the event pipeline")
    public ResponseEntity<OrderResponse> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        String correlationId = UUID.randomUUID().toString();

        log.info("Received HTTP order creation: customerId={}, amount={}, correlationId={}",
            request.customerId(), request.amount(), correlationId);

        // Persist initial state
        Order order = Order.builder()
            .correlationId(correlationId)
            .customerId(request.customerId())
            .amount(request.amount())
            .status(OrderStatus.PENDING)
            .build();

        Order saved = orderRepository.save(order);

        // Emit asynchronous event to Kafka
        orderProducer.publishOrderCreated(new OrderCreatedEvent(
            saved.getId(),
            saved.getCorrelationId(),
            saved.getCustomerId(),
            saved.getAmount(),
            Instant.now()
        ));

        return ResponseEntity.status(HttpStatus.ACCEPTED)
            .body(OrderResponse.fromEntity(saved));
    }

    @GetMapping("/{id}")
    @Operation(summary = "Inspect order status and details by UUID")
    public ResponseEntity<OrderResponse> getOrderById(@PathVariable UUID id) {
        return orderRepository.findById(id)
            .map(OrderResponse::fromEntity)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/correlation/{correlationId}")
    @Operation(summary = "Lookup order by unique correlation ID")
    public ResponseEntity<OrderResponse> getOrderByCorrelationId(@PathVariable String correlationId) {
        return orderRepository.findByCorrelationId(correlationId)
            .map(OrderResponse::fromEntity)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
