package com.messagebrokers.message_broker_lab.domain.interfaces;

public interface KafkaGateway {
    void testTopicOne(String attributeOne);
    void testTopicTwo(String attributeOne, String attributeTwo);
}
