package com.messagebrokers.message_broker_lab.infrastructure.gateways.kafka;

import com.messagebrokers.message_broker_lab.application.settings.KafkaTopics;
import com.messagebrokers.message_broker_lab.domain.interfaces.KafkaGateway;
import com.messagebrokers.message_broker_lab.infrastructure.gateways.kafka.dtos.requests.TestTopicTwoRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaGatewayImpl implements KafkaGateway {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaGatewayImpl(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void testTopicOne(String message) {
        kafkaTemplate.send(KafkaTopics.TOPIC_ONE, message);
    }

    @Override
    public void testTopicTwo(String attributeOne, String attributeTwo) {
        var request = new TestTopicTwoRequest(attributeOne, attributeTwo);
        kafkaTemplate.send(KafkaTopics.TOPIC_TWO, request);
    }
}
