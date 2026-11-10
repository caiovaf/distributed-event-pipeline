package com.caiovaf.pipeline.infrastructure.kafka;

import com.caiovaf.pipeline.domain.event.OrderCreatedEvent;
import com.caiovaf.pipeline.domain.event.OrderProcessedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${pipeline.kafka.topics.orders-created:orders.created.v1}")
    private String ordersCreatedTopic;

    @Value("${pipeline.kafka.topics.orders-processed:orders.processed.v1}")
    private String ordersProcessedTopic;

    public void publishOrderCreated(OrderCreatedEvent event) {
        log.info("Publishing OrderCreatedEvent to topic [{}]: correlationId={}, orderId={}",
            ordersCreatedTopic, event.correlationId(), event.orderId());

        kafkaTemplate.send(ordersCreatedTopic, event.correlationId(), event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish OrderCreatedEvent [correlationId={}]: {}",
                        event.correlationId(), ex.getMessage(), ex);
                } else {
                    log.info("OrderCreatedEvent successfully published [offset={}, partition={}]",
                        result.getRecordMetadata().offset(), result.getRecordMetadata().partition());
                }
            });
    }

    public void publishOrderProcessed(OrderProcessedEvent event) {
        log.info("Publishing OrderProcessedEvent to topic [{}]: correlationId={}, status={}",
            ordersProcessedTopic, event.correlationId(), event.status());

        kafkaTemplate.send(ordersProcessedTopic, event.correlationId(), event);
    }
}
