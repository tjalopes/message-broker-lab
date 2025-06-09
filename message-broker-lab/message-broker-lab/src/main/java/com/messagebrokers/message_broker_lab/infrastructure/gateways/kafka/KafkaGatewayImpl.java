package com.messagebrokers.message_broker_lab.infrastructure.gateways.kafka;

import com.messagebrokers.message_broker_lab.application.settings.KafkaTopics;
import com.messagebrokers.message_broker_lab.domain.interfaces.KafkaGateway;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.util.Map;

import java.util.HashMap;

@Component
public class KafkaGatewayImpl implements KafkaGateway {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaGatewayImpl(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void testTopicOne(String attributeOne) {

        Map<String, String> payload = new HashMap<>();
        payload.put("attributeOne", attributeOne);

        kafkaTemplate.send(KafkaTopics.TOPIC_ONE, payload);
    }

    @Override
    public void testTopicTwo(String attributeOne, String attributeTwo) {

        Map<String, String> payload = new HashMap<>();
        payload.put("attributeOne", attributeOne);
        payload.put("attributeTwo", attributeTwo);

        kafkaTemplate.send(KafkaTopics.TOPIC_TWO, payload);
    }
}
