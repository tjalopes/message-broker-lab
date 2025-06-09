package com.messagebrokers.message_broker_lab.application.services.kafka;

import com.messagebrokers.message_broker_lab.application.settings.KafkaAttributes;
import com.messagebrokers.message_broker_lab.application.settings.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.util.Map;
@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    @KafkaListener(topics = KafkaTopics.TOPIC_ONE, groupId = "test")
    public void consumeTopicOne(Map<String, String> response) {

        String attributeOne = response.get(KafkaAttributes.ATTRIBUTE_ONE) != null ? response.get(KafkaAttributes.ATTRIBUTE_ONE) : null;

        logger.info("attributeOne: {}", attributeOne);
    }

    @KafkaListener(topics = KafkaTopics.TOPIC_TWO, groupId = "test")
    public void consumeTopicTwo(Map<String, String> response) {

        String attributeOne = response.get(KafkaAttributes.ATTRIBUTE_ONE) != null ? response.get(KafkaAttributes.ATTRIBUTE_ONE) : null;
        String attributeTwo = response.get(KafkaAttributes.ATTRIBUTE_TWO) != null ? response.get(KafkaAttributes.ATTRIBUTE_TWO) : null;

        logger.info("attributeOne: {}", attributeOne);
        logger.info("attributeTwo: {}", attributeTwo);
    }
}
