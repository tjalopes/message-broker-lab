package com.messagebrokers.message_broker_lab.kafka.integration;

import com.messagebrokers.message_broker_lab.application.dtos.requests.TopicOneRequest;
import com.messagebrokers.message_broker_lab.application.dtos.requests.TopicTwoRequest;
import com.messagebrokers.message_broker_lab.application.services.kafka.KafkaProducerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"topic-one", "topic-two"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaIntegrationTest {

    @Autowired
    private KafkaProducerService producer;

    /**
     * Queues to capture consumed messages inside the tests.
     */
    private BlockingQueue<Map<String, String>> topicOneMessages;
    private BlockingQueue<Map<String, String>> topicTwoMessages;

    /**
     * Clears/initializes the queues before every test run, to make sure old messages don't affect the test.
     */
    @BeforeEach
    void setup() {
        topicOneMessages = new LinkedBlockingQueue<>();
        topicTwoMessages = new LinkedBlockingQueue<>();
    }

    /**
     * Kafka listener for the "topic-one" topic.
     * <p>
     * This method is triggered automatically when a message is published to the topic.
     *
     * @param message the kafka message received from the topic, deserialized into a map.
     */
    @KafkaListener(topics = "topic-one", groupId = "test-group")
    public void listenTopicOne(Map<String, String> message) {
        if(!topicOneMessages.offer(message)) fail("Failed to add message to queue (topic-one)");
    }

    @KafkaListener(topics = "topic-two", groupId = "test-group")
    public void listenTopicTwo(Map<String, String> message) {
        if(!topicTwoMessages.offer(message)) fail("Failed to add message to queue (topic-two)");
    }

    @Test
    void testTopicOne() throws InterruptedException {
        // given
        TopicOneRequest topicOneRequest = new TopicOneRequest(
                "Hello Kafka!");
        String expectedMessage = topicOneRequest.attributeOne();

        // when
        producer.testTopicOne(topicOneRequest);
        Map<String, String> received = topicOneMessages.poll(5, TimeUnit.SECONDS);

        // then
        assert received != null;
        String actualMessage = received.get("attributeOne");

        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void testTopicTwo() throws InterruptedException {
        // given
        TopicTwoRequest topicTwoRequest = new TopicTwoRequest(
                "Hello Kafka!",
                "Bye Kafka!");

        String expectedAttributeOne = topicTwoRequest.attributeOne();
        String expectedAttributeTwo = topicTwoRequest.attributeTwo();

        // when
        producer.testTopicTwo(topicTwoRequest);
        Map<String, String> received = topicTwoMessages.poll(5, TimeUnit.SECONDS);

        // then
        assert received != null;
        String actualAttributeOne = received.get("attributeOne");
        String actualAttributeTwo = received.get("attributeTwo");

        assertEquals(expectedAttributeOne, actualAttributeOne);
        assertEquals(expectedAttributeTwo, actualAttributeTwo);
    }
}