package com.caiovaf.pipeline.api.controller;

import com.caiovaf.pipeline.api.dto.CreateOrderRequest;
import com.caiovaf.pipeline.api.dto.OrderResponse;
import com.caiovaf.pipeline.domain.event.OrderCreatedEvent;
import com.caiovaf.pipeline.domain.model.Order;
import com.caiovaf.pipeline.domain.model.OrderStatus;
import com.caiovaf.pipeline.infrastructure.kafka.OrderProducer;
import com.caiovaf.pipeline.infrastructure.persistence.OrderRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Optional;
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
    @Operation(
        summary = "Submit a new order into the event pipeline",
        parameters = {
            @Parameter(name = "Idempotency-Key", in = ParameterIn.HEADER, description = "Unique client-supplied idempotency key", required = false),
            @Parameter(name = "X-Correlation-ID", in = ParameterIn.HEADER, description = "Alternative correlation identifier", required = false)
        }
    )
    public ResponseEntity<OrderResponse> createOrder(
        @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
        @RequestHeader(value = "X-Correlation-ID", required = false) String headerCorrelationId,
        @Valid @RequestBody CreateOrderRequest request
    ) {
        String correlationId = resolveCorrelationId(idempotencyKey, headerCorrelationId);

        // Check for idempotent request replay
        Optional<Order> existingOrder = orderRepository.findByCorrelationId(correlationId);
        if (existingOrder.isPresent()) {
            Order order = existingOrder.get();
            log.info("Idempotent request replay detected for correlationId=[{}]. Returning existing order without duplicate emission.", correlationId);

            HttpHeaders headers = new HttpHeaders();
            headers.add("X-Idempotency-Replay", "true");
            headers.add("X-Correlation-ID", correlationId);

            return ResponseEntity.status(HttpStatus.OK)
                .headers(headers)
                .body(OrderResponse.fromEntity(order));
        }

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

        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Correlation-ID", correlationId);

        return ResponseEntity.status(HttpStatus.ACCEPTED)
            .headers(headers)
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

    private String resolveCorrelationId(String idempotencyKey, String headerCorrelationId) {
        if (StringUtils.hasText(idempotencyKey)) {
            return idempotencyKey.trim();
        }
        if (StringUtils.hasText(headerCorrelationId)) {
            return headerCorrelationId.trim();
        }
        return UUID.randomUUID().toString();
    }
}
