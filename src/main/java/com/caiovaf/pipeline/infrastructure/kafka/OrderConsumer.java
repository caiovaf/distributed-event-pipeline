package com.caiovaf.pipeline.infrastructure.kafka;

import com.caiovaf.pipeline.application.service.OrderProcessingService;
import com.caiovaf.pipeline.domain.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderConsumer {

    private final OrderProcessingService processingService;
    private final StringRedisTemplate redisTemplate;

    private static final String IDEMPOTENCY_KEY_PREFIX = "idempotency:order:";
    private static final Duration DEDUPLICATION_TTL = Duration.ofHours(24);

    @KafkaListener(
        topics = "${pipeline.kafka.topics.orders-created:orders.created.v1}",
        groupId = "${spring.kafka.consumer.group-id:order-processing-group}"
    )
    public void consumeOrderCreated(
        @Payload OrderCreatedEvent event,
        @Header(KafkaHeaders.RECEIVED_KEY) String key,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("Received OrderCreatedEvent: key={}, correlationId={}, partition={}, offset={}",
            key, event.correlationId(), partition, offset);

        // Idempotency check with Redis setIfAbsent (NX)
        String idempotencyKey = IDEMPOTENCY_KEY_PREFIX + event.correlationId();
        Boolean isFirstDelivery = redisTemplate.opsForValue()
            .setIfAbsent(idempotencyKey, "PROCESSING", DEDUPLICATION_TTL);

        if (Boolean.FALSE.equals(isFirstDelivery)) {
            log.warn("Duplicate message detected for correlationId [{}]. Skipping processing.", event.correlationId());
            return;
        }

        try {
            processingService.processOrder(event);
            redisTemplate.opsForValue().set(idempotencyKey, "COMPLETED", DEDUPLICATION_TTL);
            log.info("Order [{}] processed successfully with idempotent guarantee.", event.correlationId());
        } catch (Exception ex) {
            log.error("Error processing order [{}]: {}", event.correlationId(), ex.getMessage(), ex);
            redisTemplate.delete(idempotencyKey); // Clear lock so retry or DLT can proceed
            throw ex;
        }
    }

    @KafkaListener(topics = "${pipeline.kafka.topics.orders-dlt:orders.created.v1.DLT}", groupId = "order-dlt-audit-group")
    public void consumeDeadLetterTopic(@Payload Object payload, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("CRITICAL: Message received on Dead Letter Topic [{}]: {}", topic, payload);
    }
}
