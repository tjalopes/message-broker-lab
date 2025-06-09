package com.messagebrokers.message_broker_lab.application.services.kafka;

import com.messagebrokers.message_broker_lab.application.dtos.requests.TopicOneRequest;
import com.messagebrokers.message_broker_lab.application.dtos.requests.TopicTwoRequest;
import com.messagebrokers.message_broker_lab.domain.interfaces.KafkaGateway;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaGateway kafkaGateway;

    public KafkaProducerService(KafkaGateway kafkaGateway) {
        this.kafkaGateway = kafkaGateway;
    }

    public void testTopicOne(TopicOneRequest topicOneRequest) {
        kafkaGateway.testTopicOne(topicOneRequest.attributeOne());
    }

    public void testTopicTwo(TopicTwoRequest topicTwoRequest) {
        kafkaGateway.testTopicTwo(topicTwoRequest.attributeOne(),
                topicTwoRequest.attributeTwo());
    }
}
