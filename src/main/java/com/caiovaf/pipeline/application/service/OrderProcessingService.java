package com.caiovaf.pipeline.application.service;

import com.caiovaf.pipeline.domain.event.OrderCreatedEvent;
import com.caiovaf.pipeline.domain.event.OrderProcessedEvent;
import com.caiovaf.pipeline.domain.model.Order;
import com.caiovaf.pipeline.domain.model.OrderStatus;
import com.caiovaf.pipeline.infrastructure.kafka.OrderProducer;
import com.caiovaf.pipeline.infrastructure.persistence.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProcessingService {

    private final OrderRepository orderRepository;
    private final OrderProducer orderProducer;

    @Transactional
    public void processOrder(OrderCreatedEvent event) {
        log.info("Executing transactional processing for orderId={}", event.orderId());

        Order order = orderRepository.findById(event.orderId())
            .orElseThrow(() -> new IllegalStateException("Order not found with id: " + event.orderId()));

        // Business Validation & Processing Simulation
        if (event.amount().compareTo(BigDecimal.ZERO) <= 0) {
            order.setStatus(OrderStatus.FAILED);
            order.setFailureReason("Amount must be greater than zero");
            orderRepository.save(order);

            orderProducer.publishOrderProcessed(new OrderProcessedEvent(
                order.getId(),
                order.getCorrelationId(),
                OrderStatus.FAILED,
                "Order rejected: invalid amount",
                Instant.now()
            ));
            return;
        }

        // Successfully processed
        order.setStatus(OrderStatus.PROCESSED);
        orderRepository.save(order);

        orderProducer.publishOrderProcessed(new OrderProcessedEvent(
            order.getId(),
            order.getCorrelationId(),
            OrderStatus.PROCESSED,
            "Order cleared and verified by distributed pipeline",
            Instant.now()
        ));
    }
}
