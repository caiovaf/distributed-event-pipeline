package com.caiovaf.pipeline.infrastructure.kafka;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public CommonErrorHandler errorHandler(KafkaOperations<Object, Object> operations) {
        // Dead Letter Topic Recoverer: routes failed messages to topic.DLT
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            operations,
            (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition())
        );

        // Exponential backoff: 3 retries, starting at 1000ms with multiplier 2.0 (1s -> 2s -> 4s) up to 8s
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(8000L);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}
