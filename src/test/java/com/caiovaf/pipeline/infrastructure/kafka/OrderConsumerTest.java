package com.caiovaf.pipeline.infrastructure.kafka;

import com.caiovaf.pipeline.application.service.OrderProcessingService;
import com.caiovaf.pipeline.domain.event.OrderCreatedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderConsumerTest {

    @Mock
    private OrderProcessingService processingService;

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @InjectMocks
    private OrderConsumer orderConsumer;

    private OrderCreatedEvent event;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);

        event = new OrderCreatedEvent(
            UUID.randomUUID(),
            "cor-test-12345",
            "cust_abc",
            new BigDecimal("200.00"),
            Instant.now()
        );
    }

    @Test
    @DisplayName("Should process order and mark completed in Redis on first-time delivery")
    void shouldProcessOrderOnFirstDelivery() {
        when(valueOperations.setIfAbsent(eq("idempotency:order:cor-test-12345"), eq("PROCESSING"), any(Duration.class)))
            .thenReturn(Boolean.TRUE);

        orderConsumer.consumeOrderCreated(event, "cor-test-12345", 0, 100L);

        verify(processingService, times(1)).processOrder(event);
        verify(valueOperations, times(1)).set(eq("idempotency:order:cor-test-12345"), eq("COMPLETED"), any(Duration.class));
    }

    @Test
    @DisplayName("Should discard message and skip processing on duplicate delivery")
    void shouldSkipProcessingOnDuplicateDelivery() {
        when(valueOperations.setIfAbsent(eq("idempotency:order:cor-test-12345"), eq("PROCESSING"), any(Duration.class)))
            .thenReturn(Boolean.FALSE);

        orderConsumer.consumeOrderCreated(event, "cor-test-12345", 0, 101L);

        verify(processingService, never()).processOrder(any());
        verify(valueOperations, never()).set(anyString(), eq("COMPLETED"), any(Duration.class));
    }

    @Test
    @DisplayName("Should delete Redis lock and rethrow exception on processing failure")
    void shouldReleaseLockOnProcessingFailure() {
        when(valueOperations.setIfAbsent(eq("idempotency:order:cor-test-12345"), eq("PROCESSING"), any(Duration.class)))
            .thenReturn(Boolean.TRUE);
        doThrow(new RuntimeException("Simulated DB timeout"))
            .when(processingService).processOrder(event);

        assertThrows(RuntimeException.class, () ->
            orderConsumer.consumeOrderCreated(event, "cor-test-12345", 0, 102L)
        );

        verify(redisTemplate, times(1)).delete("idempotency:order:cor-test-12345");
        verify(valueOperations, never()).set(anyString(), eq("COMPLETED"), any(Duration.class));
    }
}
