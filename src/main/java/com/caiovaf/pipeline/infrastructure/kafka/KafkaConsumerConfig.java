package com.caiovaf.pipeline.infrastructure.kafka;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public CommonErrorHandler errorHandler(KafkaOperations<Object, Object> operations) {
        // Dead Letter Topic Recoverer: routes failed messages to topic.DLT
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            operations,
            (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition())
        );

        // 3 retry attempts with a 2-second interval before publishing to DLT
        FixedBackOff backOff = new FixedBackOff(2000L, 3);
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
